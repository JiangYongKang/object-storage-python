"""HTTP 入口测试：错误码映射、幂等头、分块流程、与 Python 门面一致性。

全部使用 FastAPI TestClient 纯本地执行，无网络与外部服务。
"""

from __future__ import annotations

import base64

import pytest
from fastapi.testclient import TestClient

from object_storage import ObjectStorageService, QuotaConfig, StoreConfig
from object_storage.app import create_app

b64 = base64.b64encode
d64 = base64.b64decode


@pytest.fixture
def client():
    app = create_app(StoreConfig(quota=QuotaConfig(max_bytes=1000, max_objects=20)))
    return TestClient(app)


def test_health_and_root(client):
    assert client.get("/").json()["service"] == "local-object-storage"
    h = client.get("/storage/v1/health")
    assert h.status_code == 200
    assert h.json() == {"status": "ok", "used_bytes": 0, "used_objects": 0}


def test_put_get_idempotency_header_and_version_read(client, log_case):
    payload = b"hello-object"
    r1 = client.put(
        "/storage/v1/o/docsa.txt",
        json={"data_b64": b64(payload).decode()},
        headers={"X-Idempotency-Token": "tok-1"},
    )
    assert r1.status_code == 200
    body = r1.json()
    log_case("api-put", key="docsa.txt", size=body["size"], replayed=body["replayed"])

    r2 = client.put(
        "/storage/v1/o/docsa.txt",
        json={"data_b64": b64(payload).decode()},
        headers={"X-Idempotency-Token": "tok-1"},
    )
    assert r2.json()["version_id"] == body["version_id"]
    assert r2.json()["replayed"] is True

    g = client.get("/storage/v1/o/docsa.txt")
    assert d64(g.json()["data_b64"]) == payload
    old = client.get(
        f"/storage/v1/o/docsa.txt?version_id={body['version_id']}"
    )
    assert old.status_code == 200


def test_error_status_mapping(client, log_case):
    # 422 checksum
    r = client.put("/storage/v1/o/k", json={"data_b64": b64(b"x").decode(),
                                            "checksum": "0" * 64})
    assert r.status_code == 422
    assert r.json()["detail"]["reason"] == "checksum_mismatch"

    # 404 not found
    assert client.get("/storage/v1/o/nope").status_code == 404

    # 413 quota
    big = client.put("/storage/v1/o/big",
                     json={"data_b64": b64(b"0" * 2000).decode()})
    assert big.status_code == 413
    assert big.json()["detail"]["reason"] == "quota_exceeded"

    # 400 invalid base64
    bad_b64 = client.put("/storage/v1/o/k2", json={"data_b64": "@@@not-base64"})
    assert bad_b64.status_code == 400
    assert bad_b64.json()["detail"]["reason"] == "invalid_base64"

    # 423 retention
    ret = client.put("/storage/v1/o/locked",
                     json={"data_b64": b64(b"v1").decode(), "retain_seconds": 60})
    assert ret.status_code == 200
    over = client.put("/storage/v1/o/locked",
                      json={"data_b64": b64(b"v2").decode()})
    log_case("api-retention", status=over.status_code,
             reason=over.json()["detail"]["reason"])
    assert over.status_code == 423
    assert over.json()["detail"]["reason"] == "retention_policy_violation"


def test_multipart_lifecycle_over_http_with_all_rejections(client, log_case):
    uid = client.post("/storage/v1/u/day.log", json={}).json()["upload_id"]
    p1 = client.put(
        f"/storage/v1/u/day.log/{uid}/parts/1",
        json={"data_b64": b64(b"alpha-").decode()},
    ).json()
    p2 = client.put(
        f"/storage/v1/u/day.log/{uid}/parts/2",
        json={"data_b64": b64(b"beta").decode()},
    ).json()

    base = f"/storage/v1/u/day.log/{uid}"
    # 缺分片
    miss = client.post(f"{base}/complete", json={"parts": [
        {"number": 1, "checksum": p1["checksum"]},
        {"number": 9, "checksum": "0" * 64},
    ]})
    assert miss.status_code == 400
    assert miss.json()["detail"]["reason"] == "part_missing"
    # 重复分片
    dup = client.post(f"{base}/complete", json={"parts": [
        {"number": 1, "checksum": p1["checksum"]},
        {"number": 1, "checksum": p1["checksum"]},
    ]})
    assert dup.status_code == 400 and dup.json()["detail"]["reason"] == "part_duplicate"
    # 校验和不匹配
    wcs = client.post(f"{base}/complete", json={"parts": [
        {"number": 1, "checksum": "0" * 64},
        {"number": 2, "checksum": p2["checksum"]},
    ]})
    assert wcs.status_code == 422
    assert wcs.json()["detail"]["reason"] == "checksum_mismatch"
    # 整体校验和不匹配
    whole = client.post(f"{base}/complete", json={
        "parts": [
            {"number": 1, "checksum": p1["checksum"]},
            {"number": 2, "checksum": p2["checksum"]},
        ],
        "checksum": "f" * 64,
    })
    assert whole.status_code == 422

    # 正确完成（乱序清单）
    ok = client.post(f"{base}/complete", json={"parts": [
        {"number": 2, "checksum": p2["checksum"]},
        {"number": 1, "checksum": p1["checksum"]},
    ]})
    assert ok.status_code == 200
    g = client.get("/storage/v1/o/day.log")
    assert d64(g.json()["data_b64"]) == b"alpha-beta"
    log_case("api-mp", merged="alpha-beta", version=ok.json()["version_id"][:8])

    # 完成后继续操作
    assert client.put(f"{base}/parts/1",
                      json={"data_b64": b64(b"z").decode()}).status_code == 409
    assert client.post(f"{base}/abort").status_code == 409

    # 完成请求带 token 重放：会话已不存在仍幂等
    ok2 = client.post(
        f"{base}/complete",
        json={"parts": [
            {"number": 2, "checksum": p2["checksum"]},
            {"number": 1, "checksum": p1["checksum"]},
        ]},
        headers={"X-Idempotency-Token": "finish-1"},
    )
    # 首次完成未带 token，因此这是新 token 的二次完成 -> 已完成冲突
    assert ok2.status_code == 409
    assert ok2.json()["detail"]["reason"] == "upload_already_completed"


def test_abort_over_http_leaves_no_object_and_is_idempotent(client):
    uid = client.post("/storage/v1/u/ghost", json={}).json()["upload_id"]
    client.put(f"/storage/v1/u/ghost/{uid}/parts/1",
               json={"data_b64": b64(b"data").decode()})
    assert client.post(f"/storage/v1/u/ghost/{uid}/abort").status_code == 200
    # 重复中止成功
    assert client.post(f"/storage/v1/u/ghost/{uid}/abort").status_code == 200
    # 后续分片 / 完成均 409 upload_aborted
    p = client.put(f"/storage/v1/u/ghost/{uid}/parts/1",
                   json={"data_b64": b64(b"data").decode()})
    assert p.status_code == 409 and p.json()["detail"]["reason"] == "upload_aborted"
    c = client.post(f"/storage/v1/u/ghost/{uid}/complete", json={"parts": []})
    assert c.status_code == 409 and c.json()["detail"]["reason"] == "upload_aborted"
    # 无可见对象
    assert client.get("/storage/v1/o/ghost").status_code == 404


def test_delete_versions_rollback_over_http(client):
    r = client.put("/storage/v1/o/hist", json={"data_b64": b64(b"v1").decode()})
    v1 = r.json()["version_id"]
    client.put("/storage/v1/o/hist", json={"data_b64": b64(b"v2").decode()})

    lv = client.get("/storage/v1/o/hist/versions").json()["versions"]
    assert [v["version_id"] for v in lv] != []
    d = client.post("/storage/v1/o/hist/delete")
    assert d.json()["delete_marker"] is True
    assert client.get("/storage/v1/o/hist").status_code == 404
    assert client.get(f"/storage/v1/o/hist?version_id={v1}").status_code == 200

    rb = client.post("/storage/v1/o/hist/rollback", json={"version_id": v1})
    assert rb.status_code == 200
    assert d64(client.get("/storage/v1/o/hist").json()["data_b64"]) == b"v1"
    # 回滚到不存在的版本 404 no_such_version
    bad = client.post("/storage/v1/o/hist/rollback",
                      json={"version_id": "deadbeef"})
    assert bad.status_code == 404
    assert bad.json()["detail"]["reason"] == "no_such_version"


def test_http_and_python_entrypoints_share_decisions(client, log_case):
    """同一存储实例上，HTTP 与直接门面调用的判定必须一致。"""
    svc: ObjectStorageService = client.app.state.service
    # HTTP 写入
    client.put("/storage/v1/o/shared",
               json={"data_b64": b64(b"via-http").decode()},
               headers={"X-Idempotency-Token": "shared-token"})
    # Python 门面用同 token 重放 -> 同一版本、replayed
    result = svc.put("shared", b"via-http", client_token="shared-token")
    log_case("entry-consistency", version=result.version_id, replayed=result.replayed)
    assert result.replayed is True
    assert d64(client.get("/storage/v1/o/shared").json()["data_b64"]) == b"via-http"

    # Python 侧的保留拒绝在 HTTP 侧同样可见（同 reason 同状态码语义）
    svc.put("locked2", b"x", retain_seconds=60)
    over = client.put("/storage/v1/o/locked2",
                      json={"data_b64": b64(b"y").decode()})
    assert over.status_code == 423


def test_nested_keys_with_slashes_work(client, log_case):
    nested = "logs/2026/09/day.log"
    r = client.put(f"/storage/v1/o/{nested}",
                   json={"data_b64": b64(b"line").decode()})
    assert r.status_code == 200, r.text
    uid = client.post(f"/storage/v1/u/{nested}", json={}).json()["upload_id"]
    p = client.put(f"/storage/v1/u/{nested}/{uid}/parts/1",
                   json={"data_b64": b64(b"chunk").decode()}).json()
    ok = client.post(f"/storage/v1/u/{nested}/{uid}/complete",
                     json={"parts": [{"number": 1, "checksum": p["checksum"]}]})
    assert ok.status_code == 200, ok.text
    assert client.post(f"/storage/v1/u/{nested}/{uid}/abort").status_code == 409
    assert d64(client.get(f"/storage/v1/o/{nested}").json()["data_b64"]) == b"chunk"
    lv = client.get(f"/storage/v1/o/{nested}/versions")
    assert lv.status_code == 200 and len(lv.json()["versions"]) == 2
    assert client.post(f"/storage/v1/o/{nested}/delete").status_code == 200
    log_case("nested-key", key=nested, decision="all sub-routes resolve correctly")
