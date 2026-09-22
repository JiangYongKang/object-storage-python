"""本地对象存储能力：分块上传、版本管理、配额与保留。

典型用法::

    from object_storage import ObjectStorageService, StoreConfig, QuotaConfig

    svc = ObjectStorageService(
        StoreConfig(quota=QuotaConfig(max_bytes=10_000, max_objects=100))
    )
    svc.put("key", b"hello")
    svc.get("key")
"""

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
    StorageError,
    UploadAbortedError,
    UploadAlreadyCompletedError,
)
from .models import (
    CommitResult,
    MultipartUpload,
    ObjectVersion,
    PartSpec,
    StoredPart,
)
from .service import ObjectStorageService, RetrievedObject
from .store import ObjectStore, QuotaConfig, StoreConfig

__all__ = [
    # 核心
    "ObjectStore",
    "StoreConfig",
    "QuotaConfig",
    "ObjectStorageService",
    "RetrievedObject",
    # 模型
    "ObjectVersion",
    "MultipartUpload",
    "PartSpec",
    "StoredPart",
    "CommitResult",
    # 错误（可区分的拒绝原因）
    "StorageError",
    "NotFoundError",
    "NoSuchUploadError",
    "NoSuchVersionError",
    "UploadAbortedError",
    "UploadAlreadyCompletedError",
    "PartMissingError",
    "PartDuplicateError",
    "InvalidPartError",
    "ChecksumMismatchError",
    "QuotaExceededError",
    "RetentionPolicyViolation",
    "IdempotencyConflict",
]
