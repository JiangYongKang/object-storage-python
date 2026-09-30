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
    RecoveryPolicy,
    RetentionPolicy,
)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ObjectStore:
    def __init__(
        self,
        quota: QuotaPolicy | None = None,
        retention: RetentionPolicy | None = None,
        recovery: RecoveryPolicy | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._quota = quota or QuotaPolicy()
        self._retention = retention or RetentionPolicy()
        self._recovery = recovery or RecoveryPolicy()
        self._clock = clock
        self._lock = threading.RLock()
        # key -> 版本列表（version_id 升序，最后一个为当前版本）
        self._versions: dict[str, list[ObjectVersion]] = {}
        self._uploads: dict[str, MultipartUpload] = {}
        # 幂等记录：键为「操作作用域 + 重试标识」的复合键，保证同一重试标识
        # 用在不同对象 / 不同上传 / 不同操作类型之间互不串味。
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

    @staticmethod
    def _idem_scope(kind: str, *parts: str) -> str:
        return "\u0000".join((kind, *parts))

    def _append_version(self, key: str, data: bytes) -> ObjectVersion:
        """追加一个新版本。调用方须已完成保留/配额校验并持锁。"""
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
        return version

    def _commit(
        self,
        key: str,
        data: bytes,
        idempotency_key: str | None,
        scope: str = "put",
    ) -> ObjectVersion:
        scope_key = (
            None
            if idempotency_key is None
            else self._idem_scope(scope, key, idempotency_key)
        )
        if scope_key is not None and scope_key in self._committed:
            return self._committed[scope_key]  # 幂等重放：返回首次结论
        self._check_retention(key)
        self._check_quota(key, len(data))
        version = self._append_version(key, data)
        if scope_key is not None:
            self._committed[scope_key] = version
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

    def delete_object(
        self, key: str, idempotency_key: str | None = None
    ) -> ObjectVersion:
        with self._lock:
            versions = self._versions.get(key)
            scope_key = (
                self._idem_scope("delete", key, idempotency_key)
                if idempotency_key is not None
                else None
            )
            latest = versions[-1] if versions else None
            # 幂等重放：对象当前仍处于删除态时，首次墓碑就是确定结论。
            # 若期间已被恢复（当前为存活版本），旧结论失效，重新执行删除。
            if (
                scope_key is not None
                and scope_key in self._committed
                and latest is not None
                and latest.deleted
            ):
                return self._committed[scope_key]
            if latest is None:
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_FOUND, f"key={key!r} 不存在，无法删除"
                )
            if latest.deleted:
                return latest  # 重复删除：结论一致，不新增墓碑、不重复释放配额
            self._check_retention(key)
            self._used_bytes -= latest.size
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
            if scope_key is not None:
                self._committed[scope_key] = tombstone
            return tombstone

    def rollback_object(
        self, key: str, version_id: int, idempotency_key: str | None = None
    ) -> ObjectVersion:
        """把存活对象回滚到历史上的完整版本，作为一次新版本提交。"""
        with self._lock:
            versions = self._versions.get(key)
            scope_key = (
                self._idem_scope("rollback", key, idempotency_key)
                if idempotency_key is not None
                else None
            )
            latest = versions[-1] if versions else None
            # 幂等重放：自首次回滚后对象没有新的提交时，结论仍然有效。
            if (
                scope_key is not None
                and scope_key in self._committed
                and latest is self._committed[scope_key]
            ):
                return self._committed[scope_key]
            if latest is None:
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_FOUND,
                    f"key={key!r} 不存在，无法回滚",
                )
            if latest.deleted:
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_FOUND,
                    f"key={key!r} 当前为删除状态；恢复窗口内请使用 restore_object",
                )
            target = next(
                (v for v in versions if v.version_id == version_id), None
            )
            if target is None:
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_FOUND,
                    f"key={key!r} 没有 version_id={version_id}",
                )
            if target.deleted:
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_FOUND,
                    f"key={key!r} version={version_id} 是删除标记，不能作为回滚目标",
                )
            # 与普通写入相同：保留期内的当前版本禁止覆盖；按目标内容检查配额。
            self._check_retention(key)
            self._check_quota(key, target.size)
            version = self._append_version(key, target.data)
            if scope_key is not None:
                self._committed[scope_key] = version
            return version

    def restore_object(
        self, key: str, idempotency_key: str | None = None
    ) -> ObjectVersion:
        """在删除恢复窗口内，把已删除对象恢复到删除前的完整版本。"""
        with self._lock:
            versions = self._versions.get(key)
            scope_key = (
                self._idem_scope("restore", key, idempotency_key)
                if idempotency_key is not None
                else None
            )
            latest = versions[-1] if versions else None
            # 幂等重放：自首次恢复后对象没有被再次删除时，结论仍然有效。
            if (
                scope_key is not None
                and scope_key in self._committed
                and latest is self._committed[scope_key]
            ):
                return self._committed[scope_key]
            if latest is None:
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_FOUND,
                    f"key={key!r} 不存在，无法恢复",
                )
            if not latest.deleted:
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_DELETED,
                    f"key={key!r} 当前存活（version={latest.version_id}），无需恢复",
                )
            window = self._recovery.recover_seconds
            if window <= 0:
                raise StorageRejected(
                    RejectReason.RECOVERY_WINDOW_EXPIRED,
                    f"key={key!r} 未配置删除恢复窗口（recover_seconds={window}），"
                    "删除为终态，无法恢复",
                )
            age = self._clock() - latest.created_at
            if age > window:
                raise StorageRejected(
                    RejectReason.RECOVERY_WINDOW_EXPIRED,
                    f"key={key!r} 删除已过 {age:.3f}s，超过恢复窗口 {window}s，"
                    "无法恢复",
                )
            # 删除前最近的一份完整版本即为恢复目标。
            target = next(
                (v for v in reversed(versions) if not v.deleted), None
            )
            if target is None:
                # 理论上不可达：墓碑前必有存活版本；保留为明确拒绝而非崩溃。
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_FOUND,
                    f"key={key!r} 历史中没有可恢复的完整版本",
                )
            # 恢复等于重新写入：历史版本仍占元数据空间，容量/数量按新生计配额。
            self._check_quota(key, target.size)
            version = self._append_version(key, target.data)
            if scope_key is not None:
                self._committed[scope_key] = version
            return version

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
            upload = self._get_upload(upload_id)
            # 幂等：同一「上传会话 + 提交键」已完成过，直接返回首次结论；
            # 作用域含 upload_id，同一重试标识用于不同上传互不影响。
            scope_key = (
                self._idem_scope("complete", upload_id, idempotency_key)
                if idempotency_key is not None
                else None
            )
            if scope_key is not None and scope_key in self._committed:
                return self._committed[scope_key]
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
            self._check_retention(upload.key)
            self._check_quota(upload.key, len(data))
            version = self._append_version(upload.key, data)
            if scope_key is not None:
                self._committed[scope_key] = version
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

    @property
    def recovery_policy(self) -> RecoveryPolicy:
        return self._recovery
