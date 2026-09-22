"""可区分的拒绝原因：所有存储层拒绝都携带 reason 码，便于各入口一致解释。"""

from __future__ import annotations

from enum import Enum


class RejectReason(str, Enum):
    # 分块上传相关
    UPLOAD_NOT_FOUND = "upload_not_found"          # 上传会话不存在
    UPLOAD_ABORTED = "upload_aborted"              # 上传已中止，拒绝后续操作
    UPLOAD_COMPLETED = "upload_completed"          # 上传已完成，拒绝重复完成/续传
    PART_MISSING = "part_missing"                  # 完成合并时缺少分片
    PART_DUPLICATE = "part_duplicate"              # 同一分片号重复上传且内容不一致
    CHECKSUM_MISMATCH = "checksum_mismatch"        # 分片或整体校验和不匹配
    PART_OUT_OF_RANGE = "part_out_of_range"        # 分片编号越界
    # 配额与保留
    QUOTA_BYTES_EXCEEDED = "quota_bytes_exceeded"  # 超出容量配额
    QUOTA_OBJECTS_EXCEEDED = "quota_objects_exceeded"  # 超出对象数量配额
    RETENTION_VIOLATION = "retention_violation"    # 违反保留策略（保留期内禁止覆盖/删除）
    # 读取
    OBJECT_NOT_FOUND = "object_not_found"          # 对象不存在或已删除


class StorageRejected(Exception):
    """存储层统一拒绝异常。reason 可区分，message 面向人读。"""

    def __init__(self, reason: RejectReason, message: str):
        self.reason = reason
        self.message = message
        super().__init__(f"[{reason.value}] {message}")
