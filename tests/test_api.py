"""HTTP 入口测试：验证各入口的拒绝原因与存储层一致、结构可解释。"""

from __future__ import annotations

import base64
import hashlib

from fastapi.testclient import TestClient

from main import app, store
from storage import ObjectStore, RecoveryPolicy


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def log(case: str, inputs: str, basis: str) -> None:
    print(f"\n[CASE] {case}\n  输入: {inputs}\n  判定依据: {basis}")


def test_http_multipart_and_error_shape():
    client = TestClient(app)
    key = "http-doc"
    log("HTTP 分块流程与错误结构",
        f"PUT 2 个分片后 complete；再以错误校验和上传分片",
        "complete 返回合并对象；校验和错误返回 409 且 error.reason=checksum_mismatch")

    r = client.post(f"/uploads/{key}", json={"part_count": 2})
    assert r.status_code == 200, r.text
    upload_id = r.json()["upload_id"]

    for i, p in enumerate([b"hello-", b"world"], start=1):
        r = client.put(f"/uploads/{upload_id}/parts/{i}",
                       json={"data_b64": b64(p), "checksum": sha(p)})
        assert r.status_code == 200, r.text

    r = client.post(f"/uploads/{upload_id}/complete", json={})
    assert r.status_code == 200, r.text
    assert r.json()["checksum"] == sha(b"hello-world")

    r = client.get(f"/objects/{key}")
    assert r.status_code == 200
    assert base64.b64decode(r.json()["data_b64"]) == b"hello-world"

    # 校验和错误：结构化拒绝
    r = client.post("/uploads/bad-sum", json={"part_count": 1})
    bad_id = r.json()["upload_id"]
    r = client.put(f"/uploads/{bad_id}/parts/1",
                   json={"data_b64": b64(b"x"), "checksum": sha(b"y")})
    assert r.status_code == 409
    assert r.json()["error"]["reason"] == "checksum_mismatch"

    # 已中止上传的后续操作
    r = client.post("/uploads/abort-me", json={"part_count": 1})
    abort_id = r.json()["upload_id"]
    assert client.post(f"/uploads/{abort_id}/abort").status_code == 200
    r = client.post(f"/uploads/{abort_id}/complete", json={})
    assert r.status_code == 409
    assert r.json()["error"]["reason"] == "upload_aborted"

    # 不存在的对象
    r = client.get("/objects/no-such-key")
    assert r.status_code == 404
    assert r.json()["error"]["reason"] == "object_not_found"

    store.reset()  # 清理，保证用例可重复执行


def test_http_rollback_recoverable_delete_and_idempotency():
    client = TestClient(app)
    key = "http-recover-doc"
    log("HTTP 回滚与可恢复删除",
        "写 v1/v2→回滚 v1→幂等删除两次→恢复→重复恢复/超窗错误",
        "回滚/恢复返回新版本；同一删除键两次结论一致；重复恢复 409 object_not_deleted")

    # 为该用例临时打开恢复窗口（纯内存，用例结束后恢复默认）
    saved_recovery = store._recovery
    store._recovery = RecoveryPolicy(recover_seconds=3600)
    try:
        assert client.put(f"/objects/{key}", json={"data_b64": b64(b"one")}).status_code == 200
        assert client.put(f"/objects/{key}", json={"data_b64": b64(b"two")}).status_code == 200

        r = client.post(f"/objects/{key}/rollback", json={"version_id": 1})
        assert r.status_code == 200, r.text
        assert r.json()["version_id"] == 3
        assert base64.b64decode(client.get(f"/objects/{key}").json()["data_b64"]) == b"one"

        # 幂等删除：同一键两次结论一致（同一 version_id）
        r1 = client.delete(f"/objects/{key}?idempotency_key=del1")
        r2 = client.delete(f"/objects/{key}?idempotency_key=del1")
        assert r1.status_code == r2.status_code == 200
        assert r1.json()["version_id"] == r2.json()["version_id"] == 4
        assert client.get(f"/objects/{key}").status_code == 404

        # 恢复
        r = client.post(f"/objects/{key}/recover", json={"idempotency_key": "rec1"})
        assert r.status_code == 200, r.text
        assert r.json()["version_id"] == 5
        assert base64.b64decode(client.get(f"/objects/{key}").json()["data_b64"]) == b"one"

        # 重复恢复：对象已存活 → 409 object_not_deleted
        r = client.post(f"/objects/{key}/recover", json={})
        assert r.status_code == 409
        assert r.json()["error"]["reason"] == "object_not_deleted"

        # 回滚到不存在版本 → 404 version_not_found
        r = client.post(f"/objects/{key}/rollback", json={"version_id": 99})
        assert r.status_code == 404
        assert r.json()["error"]["reason"] == "version_not_found"
    finally:
        store._recovery = saved_recovery

    store.reset()  # 清理，保证用例可重复执行


def test_http_retry_after_state_change_and_upload_isolation():
    client = TestClient(app)
    key = "http-retry-doc"
    log("HTTP 重试语义：恢复后重删、新写后重回滚、两会话共用标识",
        "删除(del)->恢复->同键重删(须真404)；回滚(rb)->新写->同键重回滚(须读到目标)；"
        "两个上传会话 complete 共用 c 各自生效",
        "成功结论与随后读到的状态始终一致；第二个会话内容不丢")
    saved_recovery = store._recovery
    store._recovery = RecoveryPolicy(recover_seconds=3600)
    try:
        client.put(f"/objects/{key}", json={"data_b64": b64(b"data")})

        # 删除 -> 恢复 -> 同一重试标识再删除：必须真删掉
        assert client.delete(f"/objects/{key}?idempotency_key=del").status_code == 200
        assert client.post(f"/objects/{key}/recover", json={}).status_code == 200
        r = client.delete(f"/objects/{key}?idempotency_key=del")
        assert r.status_code == 200
        assert client.get(f"/objects/{key}").status_code == 404

        # 再恢复 -> 写 b'two' -> 回滚到最早存活版本(rb) -> 写 b'later' -> 同键重回滚
        client.post(f"/objects/{key}/recover", json={})
        client.put(f"/objects/{key}", json={"data_b64": b64(b"two")})
        versions = client.get(f"/objects/{key}/versions").json()["versions"]
        target = next(v["version_id"] for v in versions if not v["deleted"])
        client.post(f"/objects/{key}/rollback",
                    json={"version_id": target, "idempotency_key": "rb"})
        client.put(f"/objects/{key}", json={"data_b64": b64(b"later")})
        r = client.post(f"/objects/{key}/rollback",
                        json={"version_id": target, "idempotency_key": "rb"})
        assert r.status_code == 200
        assert base64.b64decode(
            client.get(f"/objects/{key}").json()["data_b64"]) == b"data"

        # 同一对象两个上传会话共用同一条 complete 重试标识：各得各的结果
        up1 = client.post(f"/uploads/{key}", json={"part_count": 1}).json()["upload_id"]
        client.put(f"/uploads/{up1}/parts/1",
                   json={"data_b64": b64(b"AAA"), "checksum": sha(b"AAA")})
        assert client.post(f"/uploads/{up1}/complete",
                           json={"idempotency_key": "c"}).status_code == 200

        up2 = client.post(f"/uploads/{key}", json={"part_count": 1}).json()["upload_id"]
        assert up2 != up1
        client.put(f"/uploads/{up2}/parts/1",
                   json={"data_b64": b64(b"BBB"), "checksum": sha(b"BBB")})
        r = client.post(f"/uploads/{up2}/complete", json={"idempotency_key": "c"})
        assert r.status_code == 200
        assert base64.b64decode(
            client.get(f"/objects/{key}").json()["data_b64"]) == b"BBB"
    finally:
        store._recovery = saved_recovery

    store.reset()  # 清理，保证用例可重复执行
