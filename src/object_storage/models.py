"""核心数据模型（不可变值对象 + 少量可变记录）。"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from hashlib import sha256


def now_ts() -> float:
    """单调递增的时间戳来源（便于测试注入）。"""
    return time.time()


@dataclass(frozen=True, slots=True)
class PartSpec:
    """完成合并时调用方声明的一个分片（编号 + 期望校验和）。"""

    number: int
    checksum: str  # 上传该分片时返回的 sha256


@dataclass(slots=True)
class StoredPart:
    """已经上传到某个会话中的分片。"""

    number: int
    checksum: str
    size: int
    data: bytes = b""


@dataclass(slots=True)
class MultipartUpload:
    """一次分块上传会话。"""

    upload_id: str
    key: str
    content_type: str | None
    created_at: float
    parts: dict[int, StoredPart] = field(default_factory=dict)
    aborted: bool = False
    completed: bool = False


@dataclass(frozen=True, slots=True)
class ObjectVersion:
    """对象的一个不可变版本；value 为 None 表示删除标记（delete marker）。"""

    key: str
    version_id: str
    data: bytes | None
    size: int
    checksum: str
    content_type: str | None
    created_at: float
    upload_id: str | None = None
    client_token: str | None = None
    delete_marker: bool = False
    retain_until: float | None = None  # None 表示无保留期

    @property
    def is_live(self) -> bool:
        return not self.delete_marker


def checksum_of(data: bytes) -> str:
    return sha256(data).hexdigest()


@dataclass(frozen=True, slots=True)
class CommitResult:
    """一次写入提交（put / complete_multipart / rollback / delete）的结果。"""

    key: str
    version_id: str
    checksum: str
    size: int
    replayed: bool  # True 表示幂等重放，未产生新版本
    upload_id: str | None = None
    delete_marker: bool = False
    # 仅回滚操作内部使用：首次提交所指向的历史版本，用于重放一致性校验
    version_id_target: str | None = None
