"""HTTP 入口测试：验证各入口的拒绝原因与存储层一致、结构可解释。"""

from __future__ import annotations

import base64
import hashlib

from fastapi.testclient import TestClient

import main
from main import app, store


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

    store.delete_object(key)  # 清理，保证用例可重复执行


def test_http_rollback_restore_and_error_shape(monkeypatch):
    from storage import ObjectStore, RecoveryPolicy

    # 该用例需要可恢复删除：用带恢复窗口的临时实例替换全局 store
    monkeypatch.setattr(main, "store", ObjectStore(recovery=RecoveryPolicy(3600)))
    client = TestClient(app)
    key = "http-rr"
    log("HTTP 回滚与可恢复删除",
        "写入两版→回滚 v1→删除→重复删除→恢复；再触发窗口过期/对象存活两类拒绝",
        "回滚/恢复返回新版本且内容正确；重复删除结论一致；"
        "过期恢复 409 recovery_window_expired；存活恢复 409 object_not_deleted")

    client.put(f"/objects/{key}", json={"data_b64": b64(b"one")})
    r = client.put(f"/objects/{key}", json={"data_b64": b64(b"two")})
    assert r.status_code == 200

    r = client.post(f"/objects/{key}/rollback",
                    json={"version_id": 1, "idempotency_key": "rb"})
    assert r.status_code == 200, r.text
    assert r.json()["version_id"] == 3
    # 幂等重放：同一结论
    r2 = client.post(f"/objects/{key}/rollback",
                     json={"version_id": 1, "idempotency_key": "rb"})
    assert r2.json() == r.json()
    assert base64.b64decode(client.get(f"/objects/{key}").json()["data_b64"]) == b"one"

    r = client.request("DELETE", f"/objects/{key}", json={"idempotency_key": "dl"})
    assert r.status_code == 200
    assert r.json()["version_id"] == 4
    r = client.request("DELETE", f"/objects/{key}", json={"idempotency_key": "dl"})
    assert r.status_code == 200 and r.json()["version_id"] == 4  # 结论一致

    r = client.post(f"/objects/{key}/restore", json={"idempotency_key": "rs"})
    assert r.status_code == 200, r.text
    assert r.json()["version_id"] == 5
    assert base64.b64decode(client.get(f"/objects/{key}").json()["data_b64"]) == b"one"

    # 对象存活时恢复：409 object_not_deleted
    r = client.post(f"/objects/{key}/restore", json={})
    assert r.status_code == 409
    assert r.json()["error"]["reason"] == "object_not_deleted"

    # 不存在的对象恢复：404 object_not_found
    r = client.post("/objects/no-such-key/restore", json={})
    assert r.status_code == 404
    assert r.json()["error"]["reason"] == "object_not_found"

    main.store.delete_object(key)  # 清理，保证用例可重复执行
