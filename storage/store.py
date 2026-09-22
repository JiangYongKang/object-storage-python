"""本地对象存储引擎：分块上传、版本管理、配额与保留约束。

一致性规则：
- 所有状态变更在同一把可重入锁内完成；读取永远返回某一完整版本，绝不撕裂。
- 版本单调递增，覆盖/删除追加新版本（删除为 tombstone），历史版本不可变。
- 同一 idempotency_key 的重复提交返回首次结论，不重复占用配额或产生版本。
"""

from __future__ import annotations

import hashlib
import threading
import time
import uuid
from collections.abc import Callable

from .errors import RejectReason, StorageRejected
from .models import (
    MultipartUpload,
    ObjectVersion,
    Part,
    QuotaPolicy,
    RetentionPolicy,
)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ObjectStore:
    def __init__(
        self,
        quota: QuotaPolicy | None = None,
        retention: RetentionPolicy | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._quota = quota or QuotaPolicy()
        self._retention = retention or RetentionPolicy()
        self._clock = clock
        self._lock = threading.RLock()
        # key -> 版本列表（version_id 升序，最后一个为当前版本）
        self._versions: dict[str, list[ObjectVersion]] = {}
        self._uploads: dict[str, MultipartUpload] = {}
        # 幂等键 -> 首次提交产生的版本（重复提交直接返回它）
        self._committed: dict[str, ObjectVersion] = {}
        self._used_bytes = 0

    # ---- 内部辅助（调用方须已持锁） ----

    def _current(self, key: str) -> ObjectVersion | None:
        versions = self._versions.get(key)
        if not versions:
            return None
        latest = versions[-1]
        return None if latest.deleted else latest

    def _check_retention(self, key: str) -> None:
        current = self._current(key)
        if current is None or self._retention.retain_seconds <= 0:
            return
        age = self._clock() - current.created_at
        if age < self._retention.retain_seconds:
            raise StorageRejected(
                RejectReason.RETENTION_VIOLATION,
                f"key={key!r} 仍在保留期内（已存 {age:.3f}s < "
                f"{self._retention.retain_seconds}s），禁止覆盖或删除",
            )

    def _check_quota(self, key: str, size: int) -> None:
        current = self._current(key)
        old_size = current.size if current else 0
        new_used = self._used_bytes - old_size + size
        if self._quota.max_bytes is not None and new_used > self._quota.max_bytes:
            raise StorageRejected(
                RejectReason.QUOTA_BYTES_EXCEEDED,
                f"写入 {size}B 后容量 {new_used}B 将超过上限 "
                f"{self._quota.max_bytes}B（当前 {self._used_bytes}B）",
            )
        if current is None and self._quota.max_objects is not None:
            live = sum(1 for v in self._versions.values() if v and not v[-1].deleted)
            if live + 1 > self._quota.max_objects:
                raise StorageRejected(
                    RejectReason.QUOTA_OBJECTS_EXCEEDED,
                    f"对象数量将达到 {live + 1}，超过上限 {self._quota.max_objects}",
                )

    def _commit(self, key: str, data: bytes, idempotency_key: str | None) -> ObjectVersion:
        if idempotency_key is not None and idempotency_key in self._committed:
            return self._committed[idempotency_key]  # 幂等重放：返回首次结论
        self._check_retention(key)
        self._check_quota(key, len(data))
        versions = self._versions.setdefault(key, [])
        current = self._current(key)
        self._used_bytes += len(data) - (current.size if current else 0)
        version = ObjectVersion(
            key=key,
            version_id=len(versions) + 1,
            data=data,
            checksum=_sha256(data),
            size=len(data),
            created_at=self._clock(),
        )
        versions.append(version)
        if idempotency_key is not None:
            self._committed[idempotency_key] = version
        return version

    # ---- 简单写入 / 读取 / 删除 ----

    def put_object(
        self, key: str, data: bytes, idempotency_key: str | None = None
    ) -> ObjectVersion:
        with self._lock:
            return self._commit(key, data, idempotency_key)

    def get_object(self, key: str, version_id: int | None = None) -> ObjectVersion:
        with self._lock:
            versions = self._versions.get(key)
            if not versions:
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_FOUND, f"key={key!r} 不存在"
                )
            if version_id is None:
                version = versions[-1]
            else:
                version = next(
                    (v for v in versions if v.version_id == version_id), None
                )
                if version is None:
                    raise StorageRejected(
                        RejectReason.OBJECT_NOT_FOUND,
                        f"key={key!r} 没有 version_id={version_id}",
                    )
            if version.deleted:
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_FOUND,
                    f"key={key!r} version={version.version_id} 是删除标记",
                )
            return version

    def delete_object(self, key: str) -> ObjectVersion:
        with self._lock:
            current = self._current(key)
            if current is None:
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_FOUND, f"key={key!r} 不存在，无法删除"
                )
            self._check_retention(key)
            versions = self._versions[key]
            self._used_bytes -= current.size
            tombstone = ObjectVersion(
                key=key,
                version_id=len(versions) + 1,
                data=b"",
                checksum=_sha256(b""),
                size=0,
                created_at=self._clock(),
                deleted=True,
            )
            versions.append(tombstone)
            return tombstone

    def list_versions(self, key: str) -> list[ObjectVersion]:
        with self._lock:
            return list(self._versions.get(key, []))

    # ---- 分块上传 ----

    def create_upload(
        self, key: str, part_count: int, idempotency_key: str | None = None
    ) -> MultipartUpload:
        with self._lock:
            if part_count < 1:
                raise StorageRejected(
                    RejectReason.PART_OUT_OF_RANGE,
                    f"part_count={part_count} 必须 >= 1",
                )
            if idempotency_key is not None:
                for up in self._uploads.values():
                    if up.idempotency_key == idempotency_key and up.state == "active":
                        return up  # 重复创建返回同一会话
            upload = MultipartUpload(
                upload_id=uuid.uuid4().hex,
                key=key,
                part_count=part_count,
                idempotency_key=idempotency_key,
                created_at=self._clock(),
            )
            self._uploads[upload.upload_id] = upload
            return upload

    def _get_upload(self, upload_id: str) -> MultipartUpload:
        upload = self._uploads.get(upload_id)
        if upload is None:
            raise StorageRejected(
                RejectReason.UPLOAD_NOT_FOUND, f"upload_id={upload_id!r} 不存在"
            )
        return upload

    @staticmethod
    def _require_active(upload: MultipartUpload) -> None:
        if upload.state == "aborted":
            raise StorageRejected(
                RejectReason.UPLOAD_ABORTED,
                f"upload_id={upload.upload_id!r} 已中止，拒绝后续操作",
            )
        if upload.state == "completed":
            raise StorageRejected(
                RejectReason.UPLOAD_COMPLETED,
                f"upload_id={upload.upload_id!r} 已完成，拒绝后续操作",
            )

    def upload_part(
        self, upload_id: str, number: int, data: bytes, checksum: str
    ) -> None:
        with self._lock:
            upload = self._get_upload(upload_id)
            self._require_active(upload)
            if not 1 <= number <= upload.part_count:
                raise StorageRejected(
                    RejectReason.PART_OUT_OF_RANGE,
                    f"分片号 {number} 越界（有效范围 1..{upload.part_count}）",
                )
            if number in upload.parts:
                raise StorageRejected(
                    RejectReason.PART_DUPLICATE,
                    f"分片 {number} 已上传过，拒绝重复分片",
                )
            actual = _sha256(data)
            if actual != checksum:
                raise StorageRejected(
                    RejectReason.CHECKSUM_MISMATCH,
                    f"分片 {number} 校验和不匹配：声明 {checksum}，实际 {actual}",
                )
            upload.parts[number] = Part(number=number, data=data, checksum=actual)

    def complete_upload(
        self, upload_id: str, idempotency_key: str | None = None
    ) -> ObjectVersion:
        with self._lock:
            # 幂等：同一提交键已完成过，直接返回首次结论
            if idempotency_key is not None and idempotency_key in self._committed:
                return self._committed[idempotency_key]
            upload = self._get_upload(upload_id)
            self._require_active(upload)
            missing = [
                n for n in range(1, upload.part_count + 1) if n not in upload.parts
            ]
            if missing:
                raise StorageRejected(
                    RejectReason.PART_MISSING,
                    f"缺少分片 {missing}，无法完成合并",
                )
            data = b"".join(upload.parts[n].data for n in sorted(upload.parts))
            version = self._commit(upload.key, data, idempotency_key)
            upload.state = "completed"
            upload.parts.clear()  # 合并后释放半成品
            return version

    def abort_upload(self, upload_id: str) -> None:
        with self._lock:
            upload = self._get_upload(upload_id)
            self._require_active(upload)
            upload.state = "aborted"
            upload.parts.clear()  # 中止即清理半成品，不产生可见对象

    # ---- 统计 ----

    @property
    def used_bytes(self) -> int:
        with self._lock:
            return self._used_bytes
