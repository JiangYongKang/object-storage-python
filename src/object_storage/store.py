"""本地对象存储核心引擎。

线程安全的纯内存实现，是所有入口共享的唯一判定中心：
分块上传、版本可见性、配额与保留策略全部在此集中决策。

可见性规则
----------
* 每个 key 维护一条不可变版本链，链顶即 ``current``（最新版本）。
* 写入（put / 完成分块 / 回滚）与删除（追加 delete marker）都是
  “追加新版本 + 原子切换链顶”，读取要么看到旧版本、要么看到新版本，
  绝不会读到撕裂状态。
* 删除幂等：key 已经是删除态时，重复 delete 不产生新的 delete marker。
* 回滚是普通提交：把目标历史版本的数据作为一个新版本追加到链顶，
  因此回滚结果本身也可再次被回滚/覆盖/删除。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, replace
from threading import RLock
from typing import Callable

from .errors import (
    ChecksumMismatchError,
    IdempotencyConflict,
    InvalidPartError,
    NoSuchUploadError,
    NoSuchVersionError,
    NotFoundError,
    PartDuplicateError,
    PartMissingError,
    QuotaExceededError,
    RetentionPolicyViolation,
    UploadAbortedError,
    UploadAlreadyCompletedError,
)
from .models import (
    CommitResult,
    MultipartUpload,
    ObjectVersion,
    PartSpec,
    StoredPart,
    checksum_of,
    now_ts,
)

# 合法分片号范围（与常见对象存储语义保持一致）
MIN_PART_NUMBER = 1
MAX_PART_NUMBER = 10_000


@dataclass(frozen=True, slots=True)
class QuotaConfig:
    """容量与数量配额；None 表示不限制。"""

    max_bytes: int | None = None
    max_objects: int | None = None


@dataclass(frozen=True, slots=True)
class StoreConfig:
    quota: QuotaConfig = field(default_factory=QuotaConfig)
    default_retention_seconds: float | None = None
    clock: Callable[[], float] = now_ts


@dataclass(slots=True)
class _KeyState:
    # 不可变版本链（旧 -> 新）与版本索引
    versions: list[ObjectVersion] = field(default_factory=list)
    index: dict[str, int] = field(default_factory=dict)
    # 版本级写操作（put/delete/rollback/complete）的幂等缓存：
    # token -> (操作类型, 首次成功结果)
    idem: dict[str, tuple[str, CommitResult]] = field(default_factory=dict)
    # 分块 create 的幂等：token -> upload_id（映射在会话终结后仍保留，
    # 使同 token 重放得到确定的“已中止/已完成”结论；新上传需新 token）
    create_tokens: dict[str, str] = field(default_factory=dict)
    # 已终结（完成/中止）的 upload_id -> 终态，供后续操作给出可区分原因
    terminal: dict[str, str] = field(default_factory=dict)

    @property
    def current(self) -> ObjectVersion | None:
        return self.versions[-1] if self.versions else None


class ObjectStore:
    """本地对象存储（内存后端）。"""

    def __init__(self, config: StoreConfig | None = None) -> None:
        self._config = config or StoreConfig()
        self._lock = RLock()
        # (key, upload_id) -> 会话；会话仅在“进行中”存在，
        # 完成或中止都会被移除，因此不会残留半成品。
        self._uploads: dict[tuple[str, str], MultipartUpload] = {}
        self._keys: dict[str, _KeyState] = {}

    # ==================================================================
    # 简单对象写入 / 读取 / 删除 / 版本
    # ==================================================================

    def put_object(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        client_token: str | None = None,
        retain_seconds: float | None = None,
        checksum: str | None = None,
    ) -> CommitResult:
        """整体写入（覆盖）。

        checksum 非空时必须等于 sha256(data)，否则 checksum_mismatch；
        client_token 非空时整个提交幂等，重复提交回放首次结果。
        """
        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")
        digest = checksum_of(data)
        if checksum is not None and checksum != digest:
            raise ChecksumMismatchError(
                f"put checksum mismatch key={key!r}: "
                f"declared={checksum!r} actual={digest!r}"
            )
        with self._lock:
            state = self._state(key)
            if client_token is not None:
                replay = self._take_replay(state, client_token, "put")
                if replay is not None:
                    self._assert_same_payload(replay, digest, len(data), key)
                    return replace(replay, replayed=True)
            self._enforce_not_retained(key, "overwrite")
            # 覆盖写：新版本占用 size，旧版本腾出 old.size
            old = state.current
            delta = len(data) - (old.size if old is not None and old.is_live else 0)
            self._enforce_quota(delta, new_key=old is None or not old.is_live)

            version = self._commit_version(
                state,
                key=key,
                data=data,
                checksum=digest,
                content_type=content_type,
                retain_seconds=retain_seconds,
                client_token=client_token,
            )
            return self._result_of(version, replayed=False)

    def get_object(self, key: str, *, version_id: str | None = None) -> ObjectVersion:
        """读取对象。

        * 不带 version_id：读当前版本；当前为 delete marker / key 不存在 -> NotFound。
        * 带 version_id：读指定历史版本；delete marker 版本同样 NotFound。
        """
        with self._lock:
            state = self._keys.get(key)
            if state is None:
                raise NotFoundError(f"key not found: {key!r}")
            version = self._resolve_version(state, version_id)
            if version.delete_marker:
                raise NotFoundError(
                    f"version {version.version_id!r} of {key!r} is a delete marker"
                )
            return version

    def delete_object(self, key: str, *, client_token: str | None = None) -> CommitResult:
        """删除（追加 delete marker；历史版本保留）。

        * key 不存在或已处删除态：幂等成功（replayed=True），不产生新版本；
        * 当前版本仍在保留期：retention_policy_violation。
        """
        with self._lock:
            state = self._state(key)
            if client_token is not None:
                replay = self._take_replay(state, client_token, "delete")
                if replay is not None:
                    return replace(replay, replayed=True)

            current = state.current
            if current is None or current.delete_marker:
                result = CommitResult(
                    key=key,
                    version_id=current.version_id if current is not None else "",
                    checksum="",
                    size=0,
                    replayed=True,
                    delete_marker=True,
                )
                if client_token is not None:
                    state.idem[client_token] = ("delete", result)
                return result

            self._enforce_not_retained(key, "delete")
            version = self._commit_version(
                state,
                key=key,
                data=None,
                checksum="",
                content_type=None,
                retain_seconds=None,
                client_token=client_token,
                op="delete",
            )
            return self._result_of(version, replayed=False)

    def list_versions(self, key: str) -> list[ObjectVersion]:
        """按提交时间从旧到新返回不可变版本链（含 delete marker）。"""
        with self._lock:
            state = self._keys.get(key)
            if state is None:
                raise NotFoundError(f"key not found: {key!r}")
            return list(state.versions)

    def rollback(
        self,
        key: str,
        version_id: str,
        *,
        client_token: str | None = None,
    ) -> CommitResult:
        """把指定历史版本内容作为新版本追加到链顶。

        目标为 delete marker 版本 -> no_such_version；
        当前版本处于保留期 -> retention_policy_violation。
        """
        with self._lock:
            state = self._state(key)
            if client_token is not None:
                replay = self._take_replay(state, client_token, "rollback")
                if replay is not None:
                    if replay.version_id_target != version_id:
                        raise IdempotencyConflict(
                            f"token {client_token!r} already rolled back to "
                            f"{replay.version_id_target!r}, retry targets {version_id!r}"
                        )
                    return replace(replay, replayed=True)
            target = self._resolve_version(state, version_id)
            if target.delete_marker:
                raise NoSuchVersionError(
                    f"cannot roll back to delete marker {version_id!r} of {key!r}"
                )
            self._enforce_not_retained(key, "rollback")
            old = state.current
            delta = target.size - (old.size if old is not None and old.is_live else 0)
            self._enforce_quota(delta, new_key=old is None or not old.is_live)

            version = self._commit_version(
                state,
                key=key,
                data=target.data,
                checksum=target.checksum,
                content_type=target.content_type,
                retain_seconds=None,
                client_token=client_token,
                op="rollback",
                version_id_target=version_id,
            )
            return self._result_of(version, replayed=False)

    # ==================================================================
    # 分块上传
    # ==================================================================

    def create_multipart_upload(
        self,
        key: str,
        *,
        content_type: str | None = None,
        client_token: str | None = None,
    ) -> MultipartUpload:
        """创建分块上传会话（client_token 幂等）。"""
        with self._lock:
            state = self._state(key)
            if client_token is not None and client_token in state.create_tokens:
                upload_id = state.create_tokens[client_token]
                existing = self._uploads.get((key, upload_id))
                if existing is not None:
                    return existing
                # 会话已终结：重放同一 create 请求给出确定原因
                terminal = state.terminal.get(upload_id)
                if terminal == "aborted":
                    raise UploadAbortedError(
                        f"upload created by token {client_token!r} was aborted; "
                        f"a new upload requires a new client_token"
                    )
                raise UploadAlreadyCompletedError(
                    f"upload created by token {client_token!r} was already completed"
                )

            upload = MultipartUpload(
                upload_id=uuid.uuid4().hex,
                key=key,
                content_type=content_type,
                created_at=self._now(),
            )
            self._uploads[(key, upload.upload_id)] = upload
            if client_token is not None:
                state.create_tokens[client_token] = upload.upload_id
            return upload

    def upload_part(
        self, key: str, upload_id: str, part_number: int, data: bytes
    ) -> StoredPart:
        """上传/重传单个分片，返回带 sha256 的 StoredPart。

        同一分片号重传整体覆盖旧分片（用于重试），不会产生拼接半成品。
        """
        if not MIN_PART_NUMBER <= part_number <= MAX_PART_NUMBER:
            raise InvalidPartError(
                f"part_number out of range [{MIN_PART_NUMBER},{MAX_PART_NUMBER}]: "
                f"{part_number}"
            )
        if not isinstance(data, bytes) or not data:
            raise InvalidPartError(
                f"part {part_number} of upload {upload_id!r} must be non-empty bytes"
            )
        with self._lock:
            upload = self._require_live_upload(state_key=key, upload_id=upload_id)
            part = StoredPart(
                number=part_number,
                checksum=checksum_of(data),
                size=len(data),
                data=data,
            )
            upload.parts[part_number] = part
            return StoredPart(number=part.number, checksum=part.checksum, size=part.size)

    def complete_multipart_upload(
        self,
        key: str,
        upload_id: str,
        parts: list[PartSpec] | tuple[PartSpec, ...],
        *,
        client_token: str | None = None,
        retain_seconds: float | None = None,
        checksum: str | None = None,
    ) -> CommitResult:
        """校验分片清单并原子合并提交。

        可区分的拒绝原因：
        * 会话不存在/已中止/已完成 -> 对应专用错误；
        * 清单为空、分片号重复、声明分片未上传 -> part_missing / part_duplicate；
        * 声明校验和与实际分片不一致 -> checksum_mismatch；
        * 合并整对象 sha256 与可选 checksum 不一致 -> checksum_mismatch。
        任何失败都不产生可见对象；成功后会话立即移除。
        """
        specs = list(parts)
        with self._lock:
            state = self._state(key)

            # 幂等重放优先：即使进行中的会话已被清理，重复完成也回放首次结论。
            if client_token is not None:
                replay = self._take_replay(state, client_token, "complete")
                if replay is not None:
                    return replace(replay, replayed=True)

            upload = self._uploads.get((key, upload_id))
            if upload is None:
                self._raise_terminal_or_missing(state, upload_id)
            if upload.aborted:  # 理论不可达（中止即移除），防御性处理
                raise UploadAbortedError(
                    f"upload {upload_id!r} for {key!r} is aborted"
                )
            if upload.completed:
                raise UploadAlreadyCompletedError(
                    f"upload {upload_id!r} for {key!r} is already completed"
                )

            self._validate_part_manifest(specs, upload)
            ordered = sorted(specs, key=lambda p: p.number)
            merged = b"".join(upload.parts[s.number].data for s in ordered)
            digest = checksum_of(merged)
            if checksum is not None and checksum != digest:
                raise ChecksumMismatchError(
                    f"merged checksum mismatch key={key!r}: "
                    f"declared={checksum!r} actual={digest!r}"
                )

            self._enforce_not_retained(key, "overwrite")
            old = state.current
            delta = len(merged) - (old.size if old is not None and old.is_live else 0)
            self._enforce_quota(delta, new_key=old is None or not old.is_live)

            upload.completed = True
            version = self._commit_version(
                state,
                key=key,
                data=merged,
                checksum=digest,
                content_type=upload.content_type,
                retain_seconds=retain_seconds,
                client_token=client_token,
                upload_id=upload_id,
                op="complete",
            )
            self._uploads.pop((key, upload_id), None)
            state.terminal[upload_id] = "completed"
            return self._result_of(version, replayed=False)

    def abort_multipart_upload(
        self, key: str, upload_id: str, *, client_token: str | None = None
    ) -> None:
        """中止：丢弃全部分片并移除会话。

        对已中止会话再次中止（含相同 client_token）幂等成功；
        对已完成会话中止 -> upload_already_completed。
        """
        with self._lock:
            state = self._state(key)
            upload = self._uploads.get((key, upload_id))
            if upload is None:
                terminal = state.terminal.get(upload_id)
                if terminal == "aborted":
                    return
                if terminal == "completed":
                    raise UploadAlreadyCompletedError(
                        f"cannot abort completed upload {upload_id!r} for {key!r}"
                    )
                raise NoSuchUploadError(
                    f"no such upload {upload_id!r} for key {key!r}"
                )
            upload.aborted = True
            upload.parts.clear()
            self._uploads.pop((key, upload_id), None)
            state.terminal[upload_id] = "aborted"
            # 注意：不删除 create_tokens 映射——同一 create token 永远代表
            # 同一逻辑上传；重放必须得到确定的“已中止”结论。开启新上传
            # 需要调用方使用新的 client_token。

    # ==================================================================
    # 观测
    # ==================================================================

    def quota_usage(self) -> tuple[int, int]:
        """返回 (当前活动对象字节数, 当前活动对象数量)。

        仅统计“可见的当前版本”；历史版本与 delete marker 不计费，
        使配额口径简单、确定、可解释。
        """
        with self._lock:
            return self._usage_locked()

    # ==================================================================
    # 内部辅助（除特别说明，调用时持有 self._lock）
    # ==================================================================

    def _usage_locked(self) -> tuple[int, int]:
        total_bytes = 0
        total_objects = 0
        for state in self._keys.values():
            cur = state.current
            if cur is not None and cur.is_live:
                total_bytes += cur.size
                total_objects += 1
        return total_bytes, total_objects

    def _now(self) -> float:
        return self._config.clock()

    def _state(self, key: str) -> _KeyState:
        st = self._keys.get(key)
        if st is None:
            st = _KeyState()
            self._keys[key] = st
        return st

    @staticmethod
    def _resolve_version(state: _KeyState, version_id: str | None) -> ObjectVersion:
        if version_id is None:
            version = state.current
            if version is None:
                raise NotFoundError("no current version")
            return version
        idx = state.index.get(version_id)
        if idx is None:
            raise NoSuchVersionError(f"version not found: {version_id!r}")
        return state.versions[idx]

    def _commit_version(
        self,
        state: _KeyState,
        *,
        key: str,
        data: bytes | None,
        checksum: str,
        content_type: str | None,
        retain_seconds: float | None,
        client_token: str | None,
        upload_id: str | None = None,
        op: str = "put",
        version_id_target: str | None = None,
    ) -> ObjectVersion:
        """追加一个新版本并原子切换链顶，同时登记幂等记录。"""
        ts = self._now()
        if retain_seconds is None:
            retain_seconds = self._config.default_retention_seconds
        is_delete = data is None
        version = ObjectVersion(
            key=key,
            version_id=uuid.uuid4().hex,
            data=data,
            size=0 if is_delete else len(data),
            checksum=checksum,
            content_type=None if is_delete else content_type,
            created_at=ts,
            upload_id=upload_id,
            client_token=client_token,
            delete_marker=is_delete,
            retain_until=(
                None
                if is_delete or retain_seconds is None
                else ts + retain_seconds
            ),
        )
        state.index[version.version_id] = len(state.versions)
        state.versions.append(version)
        result = self._result_of(
            version, replayed=False, version_id_target=version_id_target
        )
        if client_token is not None:
            state.idem[client_token] = (op, result)
        return version

    @staticmethod
    def _result_of(
        version: ObjectVersion,
        *,
        replayed: bool,
        version_id_target: str | None = None,
    ) -> CommitResult:
        return CommitResult(
            key=version.key,
            version_id=version.version_id,
            checksum=version.checksum,
            size=version.size,
            replayed=replayed,
            upload_id=version.upload_id,
            delete_marker=version.delete_marker,
            version_id_target=version_id_target,
        )

    @staticmethod
    def _take_replay(
        state: _KeyState, token: str, op: str
    ) -> CommitResult | None:
        """命中幂等缓存时返回首次结果；操作类型不符则拒绝。"""
        entry = state.idem.get(token)
        if entry is None:
            return None
        prior_op, result = entry
        if prior_op != op:
            raise IdempotencyConflict(
                f"client_token {token!r} already used for {prior_op!r}, "
                f"cannot reuse for {op!r}"
            )
        return result

    @staticmethod
    def _assert_same_payload(
        cached: CommitResult, digest: str, size: int, key: str
    ) -> None:
        if cached.checksum != digest or cached.size != size:
            raise IdempotencyConflict(
                f"replay of token on key={key!r} carries different payload: "
                f"first checksum={cached.checksum!r},size={cached.size}; "
                f"retry checksum={digest!r},size={size}"
            )

    def _enforce_quota(self, byte_delta: int, *, new_key: bool) -> None:
        used_bytes, used_objects = self._usage_locked()
        quota = self._config.quota
        if quota.max_bytes is not None and used_bytes + byte_delta > quota.max_bytes:
            raise QuotaExceededError(
                f"byte quota exceeded: used={used_bytes} delta={byte_delta:+d} "
                f"> max_bytes={quota.max_bytes}"
            )
        if new_key and quota.max_objects is not None:
            if used_objects + 1 > quota.max_objects:
                raise QuotaExceededError(
                    f"object-count quota exceeded: used={used_objects} + 1 "
                    f"> max_objects={quota.max_objects}"
                )

    def _enforce_not_retained(self, key: str, action: str) -> None:
        state = self._keys.get(key)
        cur = state.current if state else None
        if cur is not None and cur.is_live and cur.retain_until is not None:
            remaining = cur.retain_until - self._now()
            if remaining > 0:
                raise RetentionPolicyViolation(
                    f"cannot {action} key={key!r}: current version "
                    f"{cur.version_id!r} retained for another {remaining:.3f}s"
                )

    def _require_live_upload(self, *, state_key: str, upload_id: str) -> MultipartUpload:
        state = self._keys.get(state_key)
        upload = self._uploads.get((state_key, upload_id))
        if upload is None:
            if state is not None:
                self._raise_terminal_or_missing(state, upload_id)
            raise NoSuchUploadError(
                f"no such upload {upload_id!r} for key {state_key!r}"
            )
        if upload.aborted:
            raise UploadAbortedError(
                f"upload {upload_id!r} for {state_key!r} is aborted"
            )
        if upload.completed:
            raise UploadAlreadyCompletedError(
                f"upload {upload_id!r} for {state_key!r} is already completed"
            )
        return upload

    @staticmethod
    def _raise_terminal_or_missing(state: _KeyState, upload_id: str) -> None:
        terminal = state.terminal.get(upload_id)
        if terminal == "aborted":
            raise UploadAbortedError(
                f"upload {upload_id!r} was aborted and its parts were discarded"
            )
        if terminal == "completed":
            raise UploadAlreadyCompletedError(
                f"upload {upload_id!r} was already completed"
            )
        raise NoSuchUploadError(f"no such upload {upload_id!r}")

    @staticmethod
    def _validate_part_manifest(
        specs: list[PartSpec], upload: MultipartUpload
    ) -> None:
        if not specs:
            raise PartMissingError(
                f"empty part manifest for upload {upload.upload_id!r}"
            )
        seen: set[int] = set()
        for spec in specs:
            if spec.number in seen:
                raise PartDuplicateError(
                    f"part {spec.number} listed twice in manifest "
                    f"for upload {upload.upload_id!r}"
                )
            seen.add(spec.number)
            part = upload.parts.get(spec.number)
            if part is None:
                raise PartMissingError(
                    f"part {spec.number} declared but never uploaded "
                    f"to {upload.upload_id!r}"
                )
            if spec.checksum != part.checksum:
                raise ChecksumMismatchError(
                    f"part {spec.number} checksum mismatch: "
                    f"declared={spec.checksum!r} uploaded={part.checksum!r}"
                )
