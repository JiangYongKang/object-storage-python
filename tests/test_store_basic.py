"""核心引擎基础行为：整体写、版本可见性、回滚、删除。

每个用例通过 log_case 打印输入与判定依据，结果可复现、纯内存。
"""

from __future__ import annotations

import pytest

from object_storage import ObjectStore, StoreConfig
from object_storage.errors import (
    NoSuchVersionError,
    NotFoundError,
    RetentionPolicyViolation,
)


@pytest.fixture
def store():
    return ObjectStore(StoreConfig())


def test_put_creates_immutable_version_chain(store, log_case):
    log_case("put-chain", inputs={"keys": ["k"], "payloads": ["hello", "world"]})
    r1 = store.put_object("k", b"hello")
    r2 = store.put_object("k", b"world")

    current = store.get_object("k")
    log_case(
        "put-chain",
        decision="current must equal newest version",
        current_version=current.version_id,
        expect=r2.version_id,
    )
    assert current.data == b"world"
    assert current.version_id == r2.version_id

    # 历史版本不可变，仍可精确读取
    old = store.get_object("k", version_id=r1.version_id)
    assert old.data == b"hello"
    versions = store.list_versions("k")
    assert [v.version_id for v in versions] == [r1.version_id, r2.version_id]
    assert versions[0].data == b"hello"


def test_get_missing_key_and_delete_marker(store, log_case):
    log_case("get-missing", key="ghost", version_id=None)
    with pytest.raises(NotFoundError) as ei:
        store.get_object("ghost")
    log_case("get-missing", decision="not_found", reason=ei.value.code)

    store.put_object("k", b"v")
    store.delete_object("k")
    with pytest.raises(NotFoundError):
        store.get_object("k")
    # delete marker 版本同样不可作为对象读取，但版本链里可见
    marker = store.list_versions("k")[-1]
    assert marker.delete_marker is True
    with pytest.raises(NotFoundError):
        store.get_object("k", version_id=marker.version_id)


def test_delete_is_idempotent_without_extra_markers(store, log_case):
    log_case("delete-idempotent", key="k")
    store.put_object("k", b"v")
    d1 = store.delete_object("k", client_token="tok-del")
    d2 = store.delete_object("k", client_token="tok-del")
    # 对已删除对象再删（无 token）也是无额外副作用的成功
    d3 = store.delete_object("k")

    log_case(
        "delete-idempotent",
        decision="replays return first marker; chain length stays 2",
        v1=d1.version_id,
        v2=d2.version_id,
        replayed=d2.replayed,
    )
    assert d1.version_id == d2.version_id
    assert d2.replayed is True
    assert d3.replayed is True
    assert len(store.list_versions("k")) == 2


def test_rollback_appends_new_version_with_target_content(store, log_case):
    v1 = store.put_object("k", b"v1")
    store.put_object("k", b"v2")
    log_case("rollback", target_version=v1.version_id)
    rb = store.rollback("k", v1.version_id)

    assert store.get_object("k").data == b"v1"
    # 回滚是“追加新版本”，不是修改或删除历史
    assert rb.version_id != v1.version_id
    assert len(store.list_versions("k")) == 3
    assert store.get_object("k", version_id=v1.version_id).data == b"v1"


def test_rollback_to_delete_marker_rejected(store, log_case):
    store.put_object("k", b"v1")
    marker = store.delete_object("k")
    log_case("rollback-marker", target=marker.version_id)
    with pytest.raises(NoSuchVersionError) as ei:
        store.rollback("k", marker.version_id)
    log_case("rollback-marker", decision="reject", reason=ei.value.code)


def test_retention_blocks_overwrite_delete_rollback(log_case):
    store = ObjectStore(StoreConfig(default_retention_seconds=3600))
    store.put_object("k", b"v1")
    target = store.list_versions("k")[0].version_id

    for action, fn in [
        ("overwrite", lambda: store.put_object("k", b"v2")),
        ("delete", lambda: store.delete_object("k")),
        ("rollback", lambda: store.rollback("k", target)),
    ]:
        log_case("retention", action=action)
        with pytest.raises(RetentionPolicyViolation) as ei:
            fn()
        log_case("retention", action=action, decision="reject", reason=ei.value.code)

    # 被保留的对象仍可读
    assert store.get_object("k").data == b"v1"


def test_retention_expires_according_to_clock(log_case):
    ticks = {"t": 1000.0}
    store = ObjectStore(
        StoreConfig(default_retention_seconds=10, clock=lambda: ticks["t"])
    )
    store.put_object("k", b"v1")
    ticks["t"] = 1005.0
    log_case("retention-expiry", now=ticks["t"], decision="still locked")
    with pytest.raises(RetentionPolicyViolation):
        store.put_object("k", b"v2")

    ticks["t"] = 1011.0
    log_case("retention-expiry", now=ticks["t"], decision="lock released")
    result = store.put_object("k", b"v2")
    assert result.replayed is False
    assert store.get_object("k").data == b"v2"
