"""分块上传：完整流程、分片缺失/重复/校验和、中止语义、幂等。"""

from __future__ import annotations

import pytest

from object_storage import ObjectStore, StoreConfig
from object_storage.errors import (
    ChecksumMismatchError,
    IdempotencyConflict,
    NotFoundError,
    NoSuchUploadError,
    PartDuplicateError,
    PartMissingError,
    UploadAbortedError,
    UploadAlreadyCompletedError,
)
from object_storage.models import PartSpec


@pytest.fixture
def store():
    return ObjectStore(StoreConfig())


def _open(store, key="k", token=None):
    return store.create_multipart_upload(key, client_token=token)


def test_full_multipart_flow_merges_in_part_number_order(store, log_case):
    upload = _open(store)
    p1 = store.upload_part("k", upload.upload_id, 1, b"foo")
    p2 = store.upload_part("k", upload.upload_id, 2, b"bar")
    log_case(
        "mp-merge",
        parts_uploaded={1: p1.checksum[:8], 2: p2.checksum[:8]},
        manifest_order=[2, 1],
        decision="merge ordered by part number regardless of manifest order",
    )
    # 清单乱序不影响结果：必须按分片号确定顺序拼接
    result = store.complete_multipart_upload(
        "k",
        upload.upload_id,
        [PartSpec(2, p2.checksum), PartSpec(1, p1.checksum)],
    )
    obj = store.get_object("k")
    assert obj.data == b"foobar"
    assert result.size == 6
    assert result.upload_id == upload.upload_id


def test_missing_part_rejected(store, log_case):
    upload = _open(store)
    p1 = store.upload_part("k", upload.upload_id, 1, b"foo")
    log_case("mp-missing", uploaded=[1], manifest=[1, 3])
    with pytest.raises(PartMissingError) as ei:
        store.complete_multipart_upload(
            "k",
            upload.upload_id,
            [PartSpec(1, p1.checksum), PartSpec(3, "0" * 64)],
        )
    log_case("mp-missing", decision="reject", reason=ei.value.code)
    # 失败后会话仍存活、无可见对象
    assert store._uploads.get(("k", upload.upload_id)) is not None
    with pytest.raises(NotFoundError):
        store.get_object("k")


def test_empty_manifest_rejected(store, log_case):
    upload = _open(store)
    with pytest.raises(PartMissingError):
        store.complete_multipart_upload("k", upload.upload_id, [])


def test_duplicate_part_in_manifest_rejected(store, log_case):
    upload = _open(store)
    p1 = store.upload_part("k", upload.upload_id, 1, b"foo")
    store.upload_part("k", upload.upload_id, 2, b"bar")
    log_case("mp-duplicate", manifest=[1, 1])
    with pytest.raises(PartDuplicateError) as ei:
        store.complete_multipart_upload(
            "k",
            upload.upload_id,
            [PartSpec(1, p1.checksum), PartSpec(1, p1.checksum)],
        )
    log_case("mp-duplicate", decision="reject", reason=ei.value.code)


def test_part_checksum_mismatch_and_merged_checksum_mismatch(store, log_case):
    upload = _open(store)
    p1 = store.upload_part("k", upload.upload_id, 1, b"foo")
    p2 = store.upload_part("k", upload.upload_id, 2, b"bar")

    log_case("mp-part-checksum", declared_for_part_1="0" * 64,
             actual=p1.checksum[:12])
    with pytest.raises(ChecksumMismatchError) as ei:
        store.complete_multipart_upload(
            "k",
            upload.upload_id,
            [PartSpec(1, "0" * 64), PartSpec(2, p2.checksum)],
        )
    assert ei.value.code == "checksum_mismatch"

    log_case("mp-whole-checksum", declared="f" * 64,
             actual="sha256(foobar)", decision="reject")
    with pytest.raises(ChecksumMismatchError):
        store.complete_multipart_upload(
            "k",
            upload.upload_id,
            [PartSpec(1, p1.checksum), PartSpec(2, p2.checksum)],
            checksum="f" * 64,
        )

    # 失败不产生对象
    with pytest.raises(NotFoundError):
        store.get_object("k")


def test_reupload_same_part_number_overwrites(store, log_case):
    upload = _open(store)
    store.upload_part("k", upload.upload_id, 1, b"foo")
    p1b = store.upload_part("k", upload.upload_id, 1, b"new")
    log_case("mp-reupload", part=1, decision="last upload wins")
    result = store.complete_multipart_upload(
        "k", upload.upload_id, [PartSpec(1, p1b.checksum)]
    )
    assert store.get_object("k").data == b"new"
    assert result.size == 3


def test_abort_discards_everything_and_blocks_followups(store, log_case):
    upload = _open(store)
    store.upload_part("k", upload.upload_id, 1, b"foo")
    store.abort_multipart_upload("k", upload.upload_id)
    log_case(
        "mp-abort",
        upload_id=upload.upload_id,
        decision="parts discarded; session removed; no visible object",
        sessions_left=list(store._uploads),
    )
    assert store._uploads == {}

    with pytest.raises(UploadAbortedError):
        store.upload_part("k", upload.upload_id, 1, b"x")
    with pytest.raises(UploadAbortedError):
        store.complete_multipart_upload("k", upload.upload_id, [])
    # 重复中止幂等成功（不报错、不产生对象）
    store.abort_multipart_upload("k", upload.upload_id)
    with pytest.raises(NotFoundError):
        store.get_object("k")


def test_operations_after_completion_are_rejected(store, log_case):
    upload = _open(store)
    p1 = store.upload_part("k", upload.upload_id, 1, b"foo")
    store.complete_multipart_upload(
        "k", upload.upload_id, [PartSpec(1, p1.checksum)]
    )
    log_case("mp-terminal", upload_id=upload.upload_id, terminal="completed")
    with pytest.raises(UploadAlreadyCompletedError):
        store.upload_part("k", upload.upload_id, 1, b"x")
    with pytest.raises(UploadAlreadyCompletedError):
        store.complete_multipart_upload(
            "k", upload.upload_id, [PartSpec(1, p1.checksum)]
        )
    with pytest.raises(UploadAlreadyCompletedError):
        store.abort_multipart_upload("k", upload.upload_id)


def test_unknown_upload_id_is_no_such_upload(store, log_case):
    _open(store, key="other")
    log_case("mp-unknown", upload_id="deadbeef")
    with pytest.raises(NoSuchUploadError):
        store.upload_part("k", "deadbeef", 1, b"x")
    with pytest.raises(NoSuchUploadError):
        store.complete_multipart_upload("k", "deadbeef", [])
    with pytest.raises(NoSuchUploadError):
        store.abort_multipart_upload("k", "deadbeef")


def test_create_upload_is_idempotent_by_token(store, log_case):
    u1 = store.create_multipart_upload("k", client_token="ct")
    u2 = store.create_multipart_upload("k", client_token="ct")
    log_case("mp-create-idem", upload_1=u1.upload_id, upload_2=u2.upload_id)
    assert u1.upload_id == u2.upload_id

    # 中止后同 token 重放给出确定的“已中止”结论
    store.abort_multipart_upload("k", u1.upload_id, client_token="ab")
    with pytest.raises(UploadAbortedError):
        store.create_multipart_upload("k", client_token="ct")
    # 中止释放 token 后可用新 token 重新开启新会话
    u3 = store.create_multipart_upload("k", client_token="ct2")
    assert u3.upload_id != u1.upload_id


def test_complete_is_idempotent_and_does_not_double_charge(store, log_case):
    upload = _open(store)
    p1 = store.upload_part("k", upload.upload_id, 1, b"foo")
    specs = [PartSpec(1, p1.checksum)]
    r1 = store.complete_multipart_upload(
        "k", upload.upload_id, specs, client_token="cmp"
    )
    # 会话已移除后，重复 complete 仍回放首次结论，不产生第二个版本
    r2 = store.complete_multipart_upload(
        "k", upload.upload_id, specs, client_token="cmp"
    )
    log_case(
        "mp-complete-idem",
        v1=r1.version_id,
        v2=r2.version_id,
        replayed=r2.replayed,
        usage=store.quota_usage(),
    )
    assert r2.replayed is True
    assert r1.version_id == r2.version_id
    assert len(store.list_versions("k")) == 1
    assert store.quota_usage() == (3, 1)


def test_same_token_different_operation_conflicts(store, log_case):
    store.put_object("k", b"v1", client_token="shared")
    log_case("mp-token-conflict", token="shared", first_op="put",
             second_op="delete")
    with pytest.raises(IdempotencyConflict):
        store.delete_object("k", client_token="shared")
