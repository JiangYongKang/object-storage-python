"""HTTP 入口：与存储层一致的拒绝原因以结构化错误返回。"""

from __future__ import annotations

import base64
import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from storage import (
    ObjectStore,
    QuotaPolicy,
    RecoveryPolicy,
    RejectReason,
    RetentionPolicy,
    StorageRejected,
)

# 配额、保留与删除恢复策略可通过环境变量配置，默认不限制 / 删除即终态
store = ObjectStore(
    quota=QuotaPolicy(
        max_bytes=int(os.environ["QUOTA_MAX_BYTES"]) if "QUOTA_MAX_BYTES" in os.environ else None,
        max_objects=int(os.environ["QUOTA_MAX_OBJECTS"]) if "QUOTA_MAX_OBJECTS" in os.environ else None,
    ),
    retention=RetentionPolicy(
        retain_seconds=float(os.environ.get("RETAIN_SECONDS", "0"))
    ),
    recovery=RecoveryPolicy(
        recover_seconds=float(os.environ.get("RECOVER_SECONDS", "0"))
    ),
)

app = FastAPI(title="local-object-storage")

# 仅“资源不存在”类原因映射为 404；状态冲突（含恢复窗口过期、对象未删除、
# 配额超限、保留期冲突等）统一 409，响应体结构一致。
_NOT_FOUND = {
    RejectReason.OBJECT_NOT_FOUND,
    RejectReason.UPLOAD_NOT_FOUND,
}


@app.exception_handler(StorageRejected)
async def on_rejected(_: Request, exc: StorageRejected) -> JSONResponse:
    status = 404 if exc.reason in _NOT_FOUND else 409
    return JSONResponse(
        status_code=status,
        content={"error": {"reason": exc.reason.value, "message": exc.message}},
    )


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def _unb64(text: str) -> bytes:
    return base64.b64decode(text.encode())


class PutBody(BaseModel):
    data_b64: str
    idempotency_key: str | None = None


class DeleteBody(BaseModel):
    idempotency_key: str | None = None


class RollbackBody(BaseModel):
    version_id: int
    idempotency_key: str | None = None


class RestoreBody(BaseModel):
    idempotency_key: str | None = None


class CreateUploadBody(BaseModel):
    part_count: int = Field(ge=1)
    idempotency_key: str | None = None


class PartBody(BaseModel):
    data_b64: str
    checksum: str


class CompleteBody(BaseModel):
    idempotency_key: str | None = None


@app.put("/objects/{key}")
def put_object(key: str, body: PutBody):
    v = store.put_object(key, _unb64(body.data_b64), body.idempotency_key)
    return {"key": v.key, "version_id": v.version_id, "checksum": v.checksum, "size": v.size}


@app.get("/objects/{key}")
def get_object(key: str, version_id: int | None = None):
    v = store.get_object(key, version_id)
    return {
        "key": v.key,
        "version_id": v.version_id,
        "checksum": v.checksum,
        "size": v.size,
        "data_b64": _b64(v.data),
    }


@app.delete("/objects/{key}")
def delete_object(key: str, body: DeleteBody | None = None):
    idem = body.idempotency_key if body is not None else None
    v = store.delete_object(key, idem)
    return {"key": v.key, "version_id": v.version_id, "deleted": True}


@app.post("/objects/{key}/rollback")
def rollback_object(key: str, body: RollbackBody):
    v = store.rollback_object(key, body.version_id, body.idempotency_key)
    return {"key": v.key, "version_id": v.version_id, "checksum": v.checksum, "size": v.size}


@app.post("/objects/{key}/restore")
def restore_object(key: str, body: RestoreBody | None = None):
    idem = body.idempotency_key if body is not None else None
    v = store.restore_object(key, idem)
    return {"key": v.key, "version_id": v.version_id, "checksum": v.checksum, "size": v.size}


@app.get("/objects/{key}/versions")
def list_versions(key: str):
    recover_seconds = store.recovery_policy.recover_seconds

    def item(v) -> dict:
        d = {"version_id": v.version_id, "size": v.size, "deleted": v.deleted}
        if v.deleted and recover_seconds > 0:
            d["restorable_until"] = v.created_at + recover_seconds
        return d

    return {"versions": [item(v) for v in store.list_versions(key)]}


@app.post("/uploads/{key}")
def create_upload(key: str, body: CreateUploadBody):
    up = store.create_upload(key, body.part_count, body.idempotency_key)
    return {"upload_id": up.upload_id, "key": up.key, "part_count": up.part_count}


@app.put("/uploads/{upload_id}/parts/{number}")
def upload_part(upload_id: str, number: int, body: PartBody):
    store.upload_part(upload_id, number, _unb64(body.data_b64), body.checksum)
    return {"upload_id": upload_id, "number": number, "accepted": True}


@app.post("/uploads/{upload_id}/complete")
def complete_upload(upload_id: str, body: CompleteBody):
    v = store.complete_upload(upload_id, body.idempotency_key)
    return {"key": v.key, "version_id": v.version_id, "checksum": v.checksum, "size": v.size}


@app.post("/uploads/{upload_id}/abort")
def abort_upload(upload_id: str):
    store.abort_upload(upload_id)
    return {"upload_id": upload_id, "aborted": True}


@app.get("/stats")
def stats():
    return {"used_bytes": store.used_bytes}
