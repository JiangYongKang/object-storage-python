"""服务门面：日志必须包含输入与判定依据，且结论与引擎一致。"""

from __future__ import annotations

import logging

import pytest

from object_storage import ObjectStorageService, StoreConfig
from object_storage.errors import ChecksumMismatchError


@pytest.fixture
def svc():
    return ObjectStorageService(StoreConfig())


def test_service_logs_inputs_and_decisions(svc, caplog, log_case):
    caplog.set_level(logging.INFO, logger="object_storage.service")
    log_case("svc-log", action="put then checksum-reject")

    svc.put("k", b"hello", client_token="tok")
    with pytest.raises(ChecksumMismatchError):
        svc.put("k", b"x", checksum="0" * 64)

    messages = [r.getMessage() for r in caplog.records]
    joined = "\n".join(messages)
    # 输入被打印
    assert "put input" in joined and "sha256=" in joined
    # 成功判定
    assert "put ACCEPTED" in joined
    # 拒绝原因被打印
    assert "put REJECTED" in joined and "checksum_mismatch" in joined
    log_case("svc-log", records=len(messages),
             decision="input + ACCEPTED + REJECTED(reason) all logged")


def test_service_facade_matches_store_semantics(svc):
    r1 = svc.put("k", b"a")
    r2 = svc.put("k", b"a", client_token="t")
    r3 = svc.put("k", b"a", client_token="t")
    assert r2.version_id == r3.version_id and r3.replayed
    got = svc.get("k")
    assert got.data == b"a" and got.checksum == r2.checksum
    chain = svc.versions("k")
    assert len(chain) == 2  # r1 + r2；r3 是回放
