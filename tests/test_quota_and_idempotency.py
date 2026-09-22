"""配额约束、保留策略与写请求幂等。"""

from __future__ import annotations

import pytest

from object_storage import ObjectStore, QuotaConfig, StoreConfig
from object_storage.errors import (
    IdempotencyConflict,
    QuotaExceededError,
    RetentionPolicyViolation,
)
from object_storage.models import PartSpec


def make_store(**quota):
    return ObjectStore(StoreConfig(quota=QuotaConfig(**quota)))


# ---------------------------------------------------------------- quota

def test_byte_quota_rejected_with_explainable_reason(log_case):
    store = make_store(max_bytes=10)
    log_case("quota-bytes", max_bytes=10, incoming=11, used_before=0)
    with pytest.raises(QuotaExceededError) as ei:
        store.put_object("k", b"x" * 11)
    log_case("quota-bytes", decision="reject", reason=ei.value.code,
             message=ei.value.message)
    assert "max_bytes=10" in ei.value.message
    assert store.quota_usage() == (0, 0)  # 被拒请求不占用配额


def test_object_count_quota(log_case):
    store = make_store(max_objects=2)
    store.put_object("a", b"1")
    store.put_object("b", b"1")
    log_case("quota-count", max_objects=2, used_objects=2)
    with pytest.raises(QuotaExceededError) as ei:
        store.put_object("c", b"1")
    assert "max_objects=2" in ei.value.message
    log_case("quota-count", decision="reject", reason=ei.value.code)


def test_overwrite_charges_only_size_delta_and_delete_releases(log_case):
    store = make_store(max_bytes=10, max_objects=1)
    store.put_object("k", b"aaaa")  # 4/10
    log_case("quota-delta", action="overwrite 4 -> 8 bytes")
    store.put_object("k", b"a" * 8)  # 覆盖，旧 4 字节腾出 -> 8/10
    assert store.quota_usage() == (8, 1)

    log_case("quota-delta", action="overwrite beyond cap must reject")
    with pytest.raises(QuotaExceededError):
        store.put_object("k", b"a" * 11)

    store.delete_object("k")
    log_case("quota-delta", action="delete releases", usage=store.quota_usage())
    assert store.quota_usage() == (0, 0)
    # 删除后容量与数量都腾出来了
    store.put_object("k2", b"a" * 10)
    assert store.quota_usage() == (10, 1)


def test_failed_commit_does_not_change_usage(log_case):
    store = make_store(max_bytes=10)
    store.put_object("k", b"aaaa")
    log_case("quota-atomic", usage_before=store.quota_usage())
    with pytest.raises(QuotaExceededError):
        store.put_object("k", b"a" * 20)
    # 仍然是原来的对象与用量
    assert store.quota_usage() == (4, 1)


def test_quota_counts_only_live_current_versions(log_case):
    # 历史版本不计费：多次覆盖不会累积占用
    store = make_store(max_bytes=5)
    for i, payload in enumerate([b"a", b"bb", b"ccc", b"dddd", b"eeeee"]):
        store.put_object("k", payload)
    log_case("quota-versions", versions=len(store.list_versions("k")),
             usage=store.quota_usage())
    assert store.quota_usage() == (5, 1)
    store.delete_object("k")
    assert store.quota_usage() == (0, 0)


def test_multipart_quota_checked_at_complete_only(log_case):
    # 上传分片期间不占配额，仅完成提交时检查
    store = make_store(max_bytes=5)
    up = store.create_multipart_upload("k")
    store.upload_part("k", up.upload_id, 1, b"aaa")
    store.upload_part("k", up.upload_id, 2, b"bbb")
    log_case("mp-quota", usage_after_parts=store.quota_usage())
    assert store.quota_usage() == (0, 0)
    with pytest.raises(QuotaExceededError):
        store.complete_multipart_upload(
            "k", up.upload_id, _specs(store, up.upload_id, [1, 2])
        )
    # 配额拒绝后会话仍未提交、无对象
    assert store.quota_usage() == (0, 0)
    with pytest.raises(Exception):
        store.get_object("k")


def _specs(store, upload_id, numbers):
    upload = store._uploads[("k", upload_id)]
    return [PartSpec(n, upload.parts[n].checksum) for n in numbers]


# ---------------------------------------------------------- idempotency

def test_put_replay_returns_same_version_and_keeps_quota(log_case):
    store = make_store(max_bytes=100)
    r1 = store.put_object("k", b"hello", client_token="t1")
    r2 = store.put_object("k", b"hello", client_token="t1")
    r3 = store.put_object("k", b"hello", client_token="t1")
    log_case(
        "idem-put",
        versions=[r1.version_id, r2.version_id, r3.version_id],
        usage=store.quota_usage(),
    )
    assert r1.version_id == r2.version_id == r3.version_id
    assert r1.replayed is False and r2.replayed and r3.replayed
    assert len(store.list_versions("k")) == 1
    assert store.quota_usage() == (5, 1)  # 只计费一次


def test_put_replay_with_different_payload_conflicts(log_case):
    store = make_store(max_bytes=100)
    store.put_object("k", b"hello", client_token="t1")
    log_case("idem-conflict", first=b"hello", retry=b"world")
    with pytest.raises(IdempotencyConflict) as ei:
        store.put_object("k", b"world", client_token="t1")
    log_case("idem-conflict", decision="reject", reason=ei.value.code)
    # 冲突的重放不得改变对象内容
    assert store.get_object("k").data == b"hello"
    assert len(store.list_versions("k")) == 1


def test_delete_and_rollback_replays_are_stable(log_case):
    store = make_store(max_bytes=100)
    v1 = store.put_object("k", b"v1")
    store.put_object("k", b"v2")

    d1 = store.delete_object("k", client_token="d")
    d2 = store.delete_object("k", client_token="d")
    assert d1.version_id == d2.version_id and d2.replayed

    rb1 = store.rollback("k", v1.version_id, client_token="r")
    rb2 = store.rollback("k", v1.version_id, client_token="r")
    log_case(
        "idem-del-rollback",
        delete_marker=d1.version_id[:8],
        rollback_versions=[rb1.version_id, rb2.version_id],
        chain_len=len(store.list_versions("k")),
    )
    assert rb1.version_id == rb2.version_id and rb2.replayed
    assert store.get_object("k").data == b"v1"
    # v1,v2,marker,rollback = 4；重放不增加版本
    assert len(store.list_versions("k")) == 4

    # 同一 token 指向不同回滚目标 -> 冲突
    with pytest.raises(IdempotencyConflict):
        store.rollback(
            "k", store.list_versions("k")[1].version_id, client_token="r"
        )


def test_multipart_complete_replay_after_session_gone(log_case):
    store = make_store(max_bytes=100)
    up = store.create_multipart_upload("k")
    p1 = store.upload_part("k", up.upload_id, 1, b"abc")
    specs = [PartSpec(1, p1.checksum)]
    r1 = store.complete_multipart_upload("k", up.upload_id, specs,
                                         client_token="c")
    r2 = store.complete_multipart_upload("k", up.upload_id, specs,
                                         client_token="c")
    log_case("idem-mp", same_version=r1.version_id == r2.version_id,
             replayed=r2.replayed, usage=store.quota_usage())
    assert r1.version_id == r2.version_id and r2.replayed
    assert store.quota_usage() == (3, 1)


# ------------------------------------------------------------ retention

def test_retained_multipart_overwrite_rejected_at_complete(log_case):
    store = ObjectStore(StoreConfig(default_retention_seconds=3600))
    store.put_object("k", b"old")
    up = store.create_multipart_upload("k")
    p1 = store.upload_part("k", up.upload_id, 1, b"new")
    log_case("retained-mp", action="complete multipart over locked key")
    with pytest.raises(RetentionPolicyViolation):
        store.complete_multipart_upload(
            "k", up.upload_id, [PartSpec(1, p1.checksum)]
        )
    # 对象仍是旧内容，分片会话未产生可见对象
    assert store.get_object("k").data == b"old"
