"""存储模型：对象版本、分块上传会话、配额与保留策略。"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ObjectVersion:
    """一个不可变的对象版本。读取结果永远对应某一完整版本。"""

    key: str
    version_id: int
    data: bytes
    checksum: str          # 内容 sha256
    size: int
    created_at: float
    deleted: bool = False  # 删除标记（tombstone）


@dataclass
class Part:
    number: int
    data: bytes
    checksum: str


@dataclass
class MultipartUpload:
    """分块上传会话。未完成或已中止的会话不产生可见对象。"""

    upload_id: str
    key: str
    part_count: int                 # 期望的分片总数
    idempotency_key: str | None     # 创建上传时的幂等键
    parts: dict[int, Part] = field(default_factory=dict)
    state: str = "active"           # active | completed | aborted
    created_at: float = field(default_factory=time.monotonic)


@dataclass(frozen=True)
class QuotaPolicy:
    """配额策略：容量字节数与对象数量上限；None 表示不限制。"""

    max_bytes: int | None = None
    max_objects: int | None = None


@dataclass(frozen=True)
class RetentionPolicy:
    """保留策略：版本创建后 retain_seconds 内禁止被覆盖或删除。"""

    retain_seconds: float = 0.0
