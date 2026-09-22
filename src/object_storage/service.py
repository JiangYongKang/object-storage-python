"""服务门面：在核心引擎之上统一记录“输入 + 判定依据”。

Python 直接调用方与 HTTP 入口共享同一个 ObjectStore 实例，
因此两个入口对同一输入的判定结果完全一致；所有判定均委托给
ObjectStore，门面自身不做任何业务决策，只补充结构化日志。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .errors import NotFoundError, StorageError
from .models import (
    CommitResult,
    MultipartUpload,
    ObjectVersion,
    PartSpec,
    StoredPart,
    checksum_of,
)
from .store import ObjectStore, StoreConfig

_logger = logging.getLogger("object_storage.service")


@dataclass(frozen=True, slots=True)
class RetrievedObject:
    key: str
    version_id: str
    data: bytes
    content_type: str | None
    size: int
    checksum: str
    created_at: float


class ObjectStorageService:
    """带判定日志的服务门面，线程安全（委托给加锁的 ObjectStore）。"""

    def __init__(
        self,
        store_or_config: ObjectStore | StoreConfig | None = None,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        if isinstance(store_or_config, ObjectStore):
            self.store = store_or_config
        else:
            self.store = ObjectStore(store_or_config or StoreConfig())
        self._log = logger or _logger

    # ------------------------------------------------------------------
    # 简单对象
    # ------------------------------------------------------------------

    def put(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        client_token: str | None = None,
        retain_seconds: float | None = None,
        checksum: str | None = None,
    ) -> CommitResult:
        digest = checksum_of(data)
        self._log.info(
            "put input key=%r size=%d sha256=%s declared_checksum=%r "
            "token=%r retain_seconds=%r",
            key, len(data), digest, checksum, client_token, retain_seconds,
        )
        try:
            result = self.store.put_object(
                key,
                data,
                content_type=content_type,
                client_token=client_token,
                retain_seconds=retain_seconds,
                checksum=checksum,
            )
        except StorageError as exc:
            self._log.warning("put REJECTED key=%r reason=%s detail=%s",
                              key, exc.code, exc.message)
            raise
        self._log.info(
            "put ACCEPTED key=%r version=%s replayed=%s usage_after=%s",
            key, result.version_id, result.replayed, self.store.quota_usage(),
        )
        return result

    def get(self, key: str, *, version_id: str | None = None) -> RetrievedObject:
        self._log.info("get input key=%r version_id=%r", key, version_id)
        try:
            version = self.store.get_object(key, version_id=version_id)
        except NotFoundError as exc:
            self._log.info("get NOT_FOUND key=%r reason=%s detail=%s",
                           key, exc.code, exc.message)
            raise
        self._log.info(
            "get HIT key=%r version=%s size=%d sha256=%s",
            key, version.version_id, version.size, version.checksum,
        )
        return RetrievedObject(
            key=key,
            version_id=version.version_id,
            data=version.data or b"",
            content_type=version.content_type,
            size=version.size,
            checksum=version.checksum,
            created_at=version.created_at,
        )

    def delete(self, key: str, *, client_token: str | None = None) -> CommitResult:
        self._log.info("delete input key=%r token=%r", key, client_token)
        try:
            result = self.store.delete_object(key, client_token=client_token)
        except StorageError as exc:
            self._log.warning("delete REJECTED key=%r reason=%s detail=%s",
                              key, exc.code, exc.message)
            raise
        self._log.info(
            "delete COMMITTED key=%r marker_version=%s replayed=%s usage_after=%s",
            key, result.version_id, result.replayed, self.store.quota_usage(),
        )
        return result

    def versions(self, key: str) -> list[ObjectVersion]:
        versions = self.store.list_versions(key)
        chain = [
            ("delete-marker" if v.delete_marker else f"v:{v.version_id[:8]}")
            for v in versions
        ]
        self._log.info("versions key=%r chain_old_to_new=%s", key, chain)
        return versions

    def rollback(
        self,
        key: str,
        version_id: str,
        *,
        client_token: str | None = None,
    ) -> CommitResult:
        self._log.info("rollback input key=%r target=%s token=%r",
                       key, version_id, client_token)
        try:
            result = self.store.rollback(
                key, version_id, client_token=client_token
            )
        except StorageError as exc:
            self._log.warning("rollback REJECTED key=%r target=%s reason=%s detail=%s",
                              key, version_id, exc.code, exc.message)
            raise
        self._log.info(
            "rollback COMMITTED key=%r target=%s new_version=%s replayed=%s",
            key, version_id, result.version_id, result.replayed,
        )
        return result

    # ------------------------------------------------------------------
    # 分块上传
    # ------------------------------------------------------------------

    def create_multipart(
        self,
        key: str,
        *,
        content_type: str | None = None,
        client_token: str | None = None,
    ) -> MultipartUpload:
        self._log.info("multipart.create input key=%r token=%r", key, client_token)
        try:
            upload = self.store.create_multipart_upload(
                key, content_type=content_type, client_token=client_token
            )
        except StorageError as exc:
            self._log.warning("multipart.create REJECTED key=%r reason=%s detail=%s",
                              key, exc.code, exc.message)
            raise
        self._log.info("multipart.create OPEN key=%r upload_id=%s",
                       key, upload.upload_id)
        return upload

    def upload_part(
        self, key: str, upload_id: str, part_number: int, data: bytes
    ) -> StoredPart:
        digest = checksum_of(data)
        self._log.info(
            "multipart.part input key=%r upload=%s part=%d size=%d sha256=%s",
            key, upload_id, part_number, len(data), digest,
        )
        try:
            part = self.store.upload_part(key, upload_id, part_number, data)
        except StorageError as exc:
            self._log.warning(
                "multipart.part REJECTED key=%r upload=%s part=%d reason=%s detail=%s",
                key, upload_id, part_number, exc.code, exc.message,
            )
            raise
        self._log.info(
            "multipart.part STORED key=%r upload=%s part=%d checksum=%s",
            key, upload_id, part.number, part.checksum,
        )
        return part

    def complete_multipart(
        self,
        key: str,
        upload_id: str,
        parts: list[PartSpec] | tuple[PartSpec, ...],
        *,
        client_token: str | None = None,
        retain_seconds: float | None = None,
        checksum: str | None = None,
    ) -> CommitResult:
        manifest = [(p.number, p.checksum[:12]) for p in parts]
        self._log.info(
            "multipart.complete input key=%r upload=%s manifest=%s token=%r "
            "declared_checksum=%r",
            key, upload_id, manifest, client_token, checksum,
        )
        try:
            result = self.store.complete_multipart_upload(
                key,
                upload_id,
                parts,
                client_token=client_token,
                retain_seconds=retain_seconds,
                checksum=checksum,
            )
        except StorageError as exc:
            self._log.warning(
                "multipart.complete REJECTED key=%r upload=%s reason=%s detail=%s",
                key, upload_id, exc.code, exc.message,
            )
            raise
        self._log.info(
            "multipart.complete MERGED key=%r upload=%s version=%s size=%d "
            "sha256=%s replayed=%s usage_after=%s",
            key, upload_id, result.version_id, result.size, result.checksum,
            result.replayed, self.store.quota_usage(),
        )
        return result

    def abort_multipart(
        self, key: str, upload_id: str, *, client_token: str | None = None
    ) -> None:
        self._log.info("multipart.abort input key=%r upload=%s token=%r",
                       key, upload_id, client_token)
        try:
            self.store.abort_multipart_upload(
                key, upload_id, client_token=client_token
            )
        except StorageError as exc:
            self._log.warning(
                "multipart.abort REJECTED key=%r upload=%s reason=%s detail=%s",
                key, upload_id, exc.code, exc.message,
            )
            raise
        self._log.info(
            "multipart.abort DISCARDED key=%r upload=%s (parts removed, no object)",
            key, upload_id,
        )

    # ------------------------------------------------------------------
    # 观测
    # ------------------------------------------------------------------

    def quota_usage(self) -> tuple[int, int]:
        used = self.store.quota_usage()
        self._log.info("quota.usage used_bytes=%d used_objects=%d", used[0], used[1])
        return used
