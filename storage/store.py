"""本地对象存储引擎：分块上传、版本管理、配额/保留约束、回滚与可恢复删除。

一致性规则：
- 所有状态变更在同一把可重入锁内完成；读取永远返回某一完整版本，绝不撕裂。
- 版本单调递增；覆盖/删除/回滚/恢复一律追加新版本（删除为 tombstone），历史版本不可变。
- 幂等记录的作用域精确到“这一笔提交”：
  * 直接写/删/回滚/恢复按（对象 key, 幂等键）判重；
  * 分块上传的 complete 按（upload_id, 幂等键）判重——同一对象的两个不同上传会话
    即使共用同一条重试标识，也各自得到自己的结果，互不顶替。
- 带状态的操作（删除/回滚/恢复）重放时先核对“对象当前状态是否仍停在这笔提交的结论上”：
  * 状态没变：稳定重放首次结论，不重复占配额、不多出版本；
  * 状态已被别的提交改动（如删除后被恢复、回滚后又被写入）：绝不返回与当前可读状态
    对不上的旧结论，而是按当前状态重新执行一遍——能成功就真的生效（成功即可读到对应
    状态/数据），受保留期/恢复窗口/配额阻拦则明确拒绝且对象状态原样不动。
- 同一作用域换操作以 idempotency_conflict 拒绝。
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
        # 作用域 -> (操作标签, 首笔结论)：
        #   ("obj", key, 幂等键)           直接写/删/回滚/恢复——同一对象内判重
        #   ("upload", upload_id, 幂等键)  分块 complete——同一笔提交（会话）判重
        self._committed: dict[tuple, tuple[str, ObjectVersion]] = {}
        self._used_bytes = 0

    # ---- 内部辅助（调用方须已持锁） ----

    @staticmethod
    def _obj_scope(key: str, idempotency_key: str) -> tuple:
        return ("obj", key, idempotency_key)

    @staticmethod
    def _upload_scope(upload_id: str, idempotency_key: str) -> tuple:
        return ("upload", upload_id, idempotency_key)

    def _replay(self, scope: tuple | None, op: str) -> ObjectVersion | None:
        """返回首笔结论；同一作用域换了操作则明确冲突；未记录/无幂等键返回 None。"""
        if scope is None:
            return None
        record = self._committed.get(scope)
        if record is None:
            return None
        first_op, version = record
        if first_op != op:
            raise StorageRejected(
                RejectReason.IDEMPOTENCY_CONFLICT,
                f"该重试标识已用于 {first_op}，不能再用于 {op}"
                f"（作用域={scope[0]}:{scope[1]!r}）",
            )
        return version

    def _replay_object(
        self, key: str, idempotency_key: str | None, op: str
    ) -> ObjectVersion | None:
        """对象级带状态操作（删除/回滚/恢复）的重放判定。

        - 无幂等键或从未提交：返回 None，调用方按首次提交执行；
        - 同作用域换了操作：idempotency_conflict；
        - 对象当前版本仍是这笔提交的结论版本：重放该结论（状态没变，结论稳定）；
        - 当前版本已是后来别的提交产生的版本：返回 None，调用方必须按当前状态重新执行，
          绝不能把与现状对不上的旧结论当作成功返回。
        """
        if idempotency_key is None:
            return None
        version = self._replay(self._obj_scope(key, idempotency_key), op)
        if version is None:
            return None
        versions = self._versions.get(key)
        if versions and versions[-1].version_id == version.version_id:
            return version
        return None

    def _record(self, scope: tuple | None, op: str, version: ObjectVersion) -> None:
        if scope is not None:
            self._committed[scope] = (op, version)

    # ---- 回滚与可恢复删除 ----

    def _restore_as_new_version(
        self,
        key: str,
        data: bytes,
        scope: tuple | None,
        op: str,
        versions: list[ObjectVersion],
        current_size: int,
    ) -> ObjectVersion:
        """把一份历史内容以新版本方式恢复（锁内）：受配额约束、留痕、可幂等重放。

        恢复不检查保留期（保留期约束的是“覆盖/删除”旧版本，恢复本身不销毁数据）；
        新恢复版本自身的 created_at 重新计时，之后对它的覆盖/删除仍受保留期保护。
        调用方须已完成幂等重放与参数校验。
        """
        self._check_quota(key, len(data))
        self._used_bytes += len(data) - current_size
        version = self._append(versions, key, data)
        self._record(scope, op, version)
        return version

    def rollback_object(
        self, key: str, version_id: int, idempotency_key: str | None = None
    ) -> ObjectVersion:
        """把存活对象回滚为某个历史完整版本的内容；回滚本身追加一个新版本。

        - 目标版本必须存在且不是墓碑；对象当前必须存活（已删除请走 recover）。
        - 受保留期（当前版本仍在保留期则拒绝）与配额（恢复后的容量）约束。
        - 幂等：同一（对象, 提交键）在对象状态未变时稳定重放首次结论，不重复出版本、
          不重复占配额；若两次调用之间对象已被别的提交改动（如又写入了新内容），则按
          当前状态重新回滚——成功后紧接着读到的内容必为目标版本数据；若此时被保留期或
          配额拦住则拒绝且对象保持原样。
        """
        with self._lock:
            replay = self._replay_object(key, idempotency_key, "rollback")
            if replay is not None:
                return replay
            scope = (
                self._obj_scope(key, idempotency_key)
                if idempotency_key is not None
                else None
            )
            versions = self._versions.get(key)
            if not versions:
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_FOUND, f"key={key!r} 不存在"
                )
            target = next(
                (v for v in versions if v.version_id == version_id), None
            )
            if target is None:
                raise StorageRejected(
                    RejectReason.VERSION_NOT_FOUND,
                    f"key={key!r} 没有 version_id={version_id}",
                )
            if target.deleted:
                raise StorageRejected(
                    RejectReason.VERSION_NOT_RESTORABLE,
                    f"key={key!r} version={version_id} 是删除标记，不能回滚到它",
                )
            current = versions[-1]
            if current.deleted:
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_FOUND,
                    f"key={key!r} 当前已删除，请使用 recover 恢复后再回滚",
                )
            self._check_retention(key)
            return self._restore_as_new_version(
                key, target.data, scope, "rollback",
                versions, current.size,
            )

    def recover_object(
        self, key: str, idempotency_key: str | None = None
    ) -> ObjectVersion:
        """把软删除中的对象恢复为删除前最后一个完整版本的内容（新版本）。

        - 仅在删除后的 recover_seconds 窗口内允许；窗口为 0 表示删除即不可恢复。
        - 对象当前必须是删除状态；历史版本在恢复期间一直可查。
        - 恢复内容为墓碑之前最后一个非墓碑版本；受配额约束、留痕。
        - 不检查保留期：恢复不覆盖或删除任何已有数据。
        - 幂等：对象状态未变（仍是本笔恢复结论）时稳定重放；若恢复后又被删除（状态已
          分歧），同键重试按“最近一次删除”重新恢复：仍在窗口内就再次恢复，超出窗口或
          超配额则拒绝、状态原样不动。
        """
        with self._lock:
            replay = self._replay_object(key, idempotency_key, "recover")
            if replay is not None:
                return replay
            scope = (
                self._obj_scope(key, idempotency_key)
                if idempotency_key is not None
                else None
            )
            versions = self._versions.get(key)
            if not versions:
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_FOUND, f"key={key!r} 不存在"
                )
            tombstone = versions[-1]
            if not tombstone.deleted:
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_DELETED,
                    f"key={key!r} 未处于删除状态，无需恢复",
                )
            window = self._recovery.recover_seconds
            if window <= 0:
                raise StorageRejected(
                    RejectReason.RECOVERY_WINDOW_EXPIRED,
                    f"key={key!r} 未配置可恢复窗口（recover_seconds={window}），无法恢复",
                )
            elapsed = self._clock() - tombstone.created_at
            if elapsed > window:
                raise StorageRejected(
                    RejectReason.RECOVERY_WINDOW_EXPIRED,
                    f"key={key!r} 已删除 {elapsed:.3f}s，超过可恢复窗口 {window}s",
                )
            prior = next(
                (v for v in reversed(versions[:-1]) if not v.deleted), None
            )
            if prior is None:  # 理论上不可达：墓碑前必有完整版本
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_FOUND,
                    f"key={key!r} 的墓碑之前没有可恢复的完整版本",
                )
            return self._restore_as_new_version(
                key, prior.data, scope, "recover",
                versions, current_size=0,  # 墓碑 size=0，不占容量
            )

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

    def _append(
        self,
        versions: list[ObjectVersion],
        key: str,
        data: bytes,
        *,
        deleted: bool = False,
    ) -> ObjectVersion:
        """在锁内追加一个新版本（version_id 单调递增）。"""
        version = ObjectVersion(
            key=key,
            version_id=len(versions) + 1,
            data=data,
            checksum=_sha256(data),
            size=0 if deleted else len(data),
            created_at=self._clock(),
            deleted=deleted,
        )
        versions.append(version)
        return version

    def _commit(
        self,
        key: str,
        data: bytes,
        idempotency_key: str | None,
        op: str = "write",
        scope: tuple | None = None,
    ) -> ObjectVersion:
        if scope is None and idempotency_key is not None:
            scope = self._obj_scope(key, idempotency_key)
        replay = self._replay(scope, op)
        if replay is not None:
            # 重放语义：写入的结论是“这一笔提交产生过这个版本”。重试时即使对象后来被
            # 别的提交改动，也只返回本笔版本、绝不再追加/顶替当前内容（不丢后来的写入）。
            return replay
        self._check_retention(key)
        self._check_quota(key, len(data))
        versions = self._versions.setdefault(key, [])
        current = self._current(key)
        self._used_bytes += len(data) - (current.size if current else 0)
        version = self._append(versions, key, data)
        self._record(scope, op, version)
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
        """软删除：追加墓碑版本，历史版本保留。

        - 对象存活：保留期内拒绝（retention_violation，状态原样）；否则追加墓碑并释放容量。
        - 幂等重试（同一对象、同一提交键）：
          * 对象当前仍是本笔删除的墓碑：稳定重放该墓碑，不多出版本、不重复扣容量；
          * 对象在两次调用之间被恢复（当前又存活）：按当前状态重新删除——成功必真删掉
            （随后读不到），并把该提交的结论更新为新墓碑；此时若仍在保留期则拒绝、
            状态原样不动；
          * 对象已被另一笔提交删除（当前是别的墓碑）：object_not_found，状态不变。
        - 无幂等键删除已删除对象：一律 object_not_found（结论稳定）。
        """
        with self._lock:
            replay = self._replay_object(key, idempotency_key, "delete")
            if replay is not None:
                return replay
            versions = self._versions.get(key)
            if not versions or versions[-1].deleted:
                raise StorageRejected(
                    RejectReason.OBJECT_NOT_FOUND,
                    f"key={key!r} 不存在或已删除，无法删除",
                )
            current = versions[-1]
            self._check_retention(key)
            self._used_bytes -= current.size
            tombstone = self._append(versions, key, b"", deleted=True)
            if idempotency_key is not None:
                self._record(
                    self._obj_scope(key, idempotency_key), "delete", tombstone
                )
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
                    if (
                        up.key == key
                        and up.idempotency_key == idempotency_key
                        and up.state == "active"
                    ):
                        return up  # 重复创建返回同一会话（仅同一对象内）
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
            # 幂等作用域精确到“这一笔提交（本上传会话）”：同一（upload_id, 提交键）
            # 已完成过就稳定返回本会话的首次结论（即使会话已 completed）；不同上传会话
            # 即使属于同一对象、共用同一条重试标识，也互不重放、互不顶替。
            scope = (
                self._upload_scope(upload_id, idempotency_key)
                if idempotency_key is not None
                else None
            )
            replay = self._replay(scope, "write")
            if replay is not None:
                return replay
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
            version = self._commit(
                upload.key, data, idempotency_key=None, scope=scope
            )
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

    def reset(self) -> None:
        """清空全部状态（仅供测试在共享存储实例上重置用例）。"""
        with self._lock:
            self._versions.clear()
            self._uploads.clear()
            self._committed.clear()
            self._used_bytes = 0
