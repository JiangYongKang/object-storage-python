"""可区分的错误类型。

所有对象存储拒绝原因都有独立异常类型，各入口（Python API / HTTP）
可以据此给出确定且可解释的结果。
"""

from __future__ import annotations


class StorageError(Exception):
    """所有存储层错误的基类。"""

    code: str = "storage_error"

    def __init__(self, message: str, *, reason: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.reason = reason or self.code


class NotFoundError(StorageError):
    code = "not_found"


class NoSuchUploadError(NotFoundError):
    """分片上传会话不存在（从未创建或已清理）。"""

    code = "no_such_upload"


class NoSuchVersionError(NotFoundError):
    code = "no_such_version"


class UploadAbortedError(StorageError):
    """操作作用于一个已中止的上传会话。"""

    code = "upload_aborted"


class UploadAlreadyCompletedError(StorageError):
    """操作作用于一个已完成的上传会话。"""

    code = "upload_already_completed"


class PartMissingError(StorageError):
    """完成上传时缺少分片。"""

    code = "part_missing"


class PartDuplicateError(StorageError):
    """同一次完成请求中出现重复分片号。"""

    code = "part_duplicate"


class ChecksumMismatchError(StorageError):
    """分片校验和或整对象合并校验和不匹配。"""

    code = "checksum_mismatch"


class QuotaExceededError(StorageError):
    """超出容量或数量配额。"""

    code = "quota_exceeded"


class RetentionPolicyViolation(StorageError):
    """写入被保留策略拒绝（覆盖/删除/回滚仍在保留期内的对象）。"""

    code = "retention_policy_violation"


class IdempotencyConflict(StorageError):
    """同一 client_token 被用于参数不一致的另一写请求。"""

    code = "idempotency_conflict"


class InvalidPartError(StorageError):
    """分片本身非法（编号越界、空负载等）。"""

    code = "invalid_part"
