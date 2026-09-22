"""HTTP 入口（FastAPI router）。

与 Python 门面共享同一个 ObjectStore 实例，保证判定一致；
每种拒绝原因映射到确定的 HTTP 状态码与错误码。

路由布局::

    PUT/GET    /storage/v1/o/{key}
    POST       /storage/v1/o/{key}/delete
    GET        /storage/v1/o/{key}/versions
    POST       /storage/v1/o/{key}/rollback
    POST       /storage/v1/u/{key}                            创建会话
    PUT        /storage/v1/u/{key}/{upload_id}/parts/{part}   上传分片
    POST       /storage/v1/u/{key}/{upload_id}/complete
    POST       /storage/v1/u/{key}/{upload_id}/abort

key 支持斜杠分层（如 ``logs/2026/a.log``）。为避免 ``{key:path}``
贪婪匹配，对象与分块使用互不重叠的静态前缀 ``/o``、``/u``，并且
具体后缀路由（versions/delete/rollback/complete/abort）的注册顺序
先于同方法的贪婪路由（Starlette 按注册顺序匹配）。

二进制负载用 base64 承载在 JSON 中，测试通过 TestClient 纯本地执行，
不依赖任何外部服务或网络。
"""

from __future__ import annotations

import base64

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field

from .errors import NotFoundError, StorageError
from .models import PartSpec
from .service import ObjectStorageService

router = APIRouter(prefix="/storage/v1", tags=["object-storage"])

# 单一事实来源：错误原因 -> HTTP 状态码（reason 直接取异常 code）。
_STATUS_BY_CODE: dict[str, int] = {
    "not_found": 404,
    "no_such_upload": 404,
    "no_such_version": 404,
    "upload_aborted": 409,
    "upload_already_completed": 409,
    "part_missing": 400,
    "part_duplicate": 400,
    "invalid_part": 400,
    "checksum_mismatch": 422,
    "quota_exceeded": 413,
    "retention_policy_violation": 423,  # Locked
    "idempotency_conflict": 409,
}


def http_status_for(exc: StorageError) -> int:
    return _STATUS_BY_CODE.get(exc.code, 500)


_default_service: ObjectStorageService | None = None


def set_default_service(service: ObjectStorageService) -> None:
    global _default_service
    _default_service = service


def get_service() -> ObjectStorageService:
    if _default_service is None:
        raise RuntimeError(
            "ObjectStorageService not configured; call set_default_service() or "
            "override the get_service dependency"
        )
    return _default_service


def _raise(exc: StorageError) -> None:
    raise HTTPException(
        status_code=http_status_for(exc),
        detail={"reason": exc.code, "message": exc.message},
    )


def _b64_decode(value: str, *, what: str) -> bytes:
    try:
        return base64.b64decode(value, validate=True)
    except Exception as exc:  # noqa: BLE001 - 归一化为 400
        raise HTTPException(
            status_code=400,
            detail={"reason": "invalid_base64", "message": f"{what} is not valid base64"},
        ) from exc


def _b64_encode(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


# ---------------------------------------------------------------------
# 请求 / 响应模型
# ---------------------------------------------------------------------

class PutBody(BaseModel):
    data_b64: str
    content_type: str | None = None
    retain_seconds: float | None = None
    checksum: str | None = None


class CreateMultipartBody(BaseModel):
    content_type: str | None = None


class PartBody(BaseModel):
    data_b64: str


class PartItem(BaseModel):
    number: int = Field(ge=1, le=10_000)
    checksum: str


class CompleteBody(BaseModel):
    parts: list[PartItem]
    retain_seconds: float | None = None
    checksum: str | None = None


class RollbackBody(BaseModel):
    version_id: str


def _commit_payload(result) -> dict:
    return {
        "key": result.key,
        "version_id": result.version_id,
        "checksum": result.checksum,
        "size": result.size,
        "replayed": result.replayed,
        "delete_marker": result.delete_marker,
    }


# =====================================================================
# 路由：顺序敏感——同一方法下，带静态后缀的具体路由必须先注册，
# {key:path} 贪婪路由最后注册。
# =====================================================================

@router.get("/health")
def health(svc: ObjectStorageService = Depends(get_service)) -> dict:
    used_bytes, used_objects = svc.quota_usage()
    return {"status": "ok", "used_bytes": used_bytes, "used_objects": used_objects}


# ---------- 分块：具体路由（parts / complete / abort）先注册 ----------

@router.put("/u/{key:path}/{upload_id}/parts/{part_number}")
def upload_part(
    key: str,
    upload_id: str,
    part_number: int,
    body: PartBody,
    svc: ObjectStorageService = Depends(get_service),
):
    data = _b64_decode(body.data_b64, what="data_b64")
    try:
        part = svc.upload_part(key, upload_id, part_number, data)
    except StorageError as exc:
        _raise(exc)
    return {
        "key": key,
        "upload_id": upload_id,
        "part_number": part.number,
        "size": part.size,
        "checksum": part.checksum,
    }


@router.post("/u/{key:path}/{upload_id}/complete")
def complete_multipart(
    key: str,
    upload_id: str,
    body: CompleteBody,
    svc: ObjectStorageService = Depends(get_service),
    x_idempotency_token: str | None = Header(default=None),
):
    specs = [PartSpec(number=p.number, checksum=p.checksum) for p in body.parts]
    try:
        result = svc.complete_multipart(
            key,
            upload_id,
            specs,
            client_token=x_idempotency_token,
            retain_seconds=body.retain_seconds,
            checksum=body.checksum,
        )
    except StorageError as exc:
        _raise(exc)
    return _commit_payload(result)


@router.post("/u/{key:path}/{upload_id}/abort")
def abort_multipart(
    key: str,
    upload_id: str,
    svc: ObjectStorageService = Depends(get_service),
    x_idempotency_token: str | None = Header(default=None),
):
    try:
        svc.abort_multipart(key, upload_id, client_token=x_idempotency_token)
    except StorageError as exc:
        _raise(exc)
    return {"key": key, "upload_id": upload_id, "aborted": True}


# ---------- 分块：创建会话（贪婪 POST，置于最后） ----------

@router.post("/u/{key:path}")
def create_multipart(
    key: str,
    body: CreateMultipartBody,
    svc: ObjectStorageService = Depends(get_service),
    x_idempotency_token: str | None = Header(default=None),
):
    try:
        upload = svc.create_multipart(
            key,
            content_type=body.content_type,
            client_token=x_idempotency_token,
        )
    except StorageError as exc:
        _raise(exc)
    return {"key": key, "upload_id": upload.upload_id, "created_at": upload.created_at}


# ---------- 对象：带后缀的具体路由先注册 ----------

@router.get("/o/{key:path}/versions")
def list_versions(key: str, svc: ObjectStorageService = Depends(get_service)):
    try:
        versions = svc.versions(key)
    except NotFoundError as exc:
        _raise(exc)
    return {
        "key": key,
        "versions": [
            {
                "version_id": v.version_id,
                "delete_marker": v.delete_marker,
                "size": v.size,
                "checksum": v.checksum,
                "content_type": v.content_type,
                "created_at": v.created_at,
                "retain_until": v.retain_until,
            }
            for v in versions
        ],
    }


@router.post("/o/{key:path}/delete")
def delete_object(
    key: str,
    svc: ObjectStorageService = Depends(get_service),
    x_idempotency_token: str | None = Header(default=None),
):
    try:
        result = svc.delete(key, client_token=x_idempotency_token)
    except StorageError as exc:
        _raise(exc)
    return _commit_payload(result)


@router.post("/o/{key:path}/rollback")
def rollback(
    key: str,
    body: RollbackBody,
    svc: ObjectStorageService = Depends(get_service),
    x_idempotency_token: str | None = Header(default=None),
):
    try:
        result = svc.rollback(key, body.version_id, client_token=x_idempotency_token)
    except StorageError as exc:
        _raise(exc)
    return _commit_payload(result)


# ---------- 对象：贪婪读写路由最后注册 ----------

@router.put("/o/{key:path}")
def put_object(
    key: str,
    body: PutBody,
    svc: ObjectStorageService = Depends(get_service),
    x_idempotency_token: str | None = Header(default=None),
):
    data = _b64_decode(body.data_b64, what="data_b64")
    try:
        result = svc.put(
            key,
            data,
            content_type=body.content_type,
            client_token=x_idempotency_token,
            retain_seconds=body.retain_seconds,
            checksum=body.checksum,
        )
    except StorageError as exc:
        _raise(exc)
    return _commit_payload(result)


@router.get("/o/{key:path}")
def get_object(
    key: str,
    version_id: str | None = Query(default=None),
    svc: ObjectStorageService = Depends(get_service),
):
    try:
        obj = svc.get(key, version_id=version_id)
    except NotFoundError as exc:
        _raise(exc)
    return {
        "key": obj.key,
        "version_id": obj.version_id,
        "content_type": obj.content_type,
        "size": obj.size,
        "checksum": obj.checksum,
        "created_at": obj.created_at,
        "data_b64": _b64_encode(obj.data),
    }
