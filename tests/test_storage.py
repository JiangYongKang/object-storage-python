"""对象存储单元测试：每个用例打印输入与判定依据，结论可复现、不依赖外部服务。"""

from __future__ import annotations

import hashlib
import threading

import pytest

from storage import ObjectStore, QuotaPolicy, RejectReason, RetentionPolicy, StorageRejected


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def log(case: str, inputs: str, basis: str) -> None:
    print(f"\n[CASE] {case}\n  输入: {inputs}\n  判定依据: {basis}")


# ---------- 分块上传 ----------

def test_multipart_happy_path():
    store = ObjectStore()
    parts = [b"aaa", b"bbb", b"ccc"]
    log("分块上传完整流程", f"key=doc, parts={parts}",
        "完成后应得到合并对象 b'aaabbbccc'，版本 1，校验和等于整体 sha256")
    up = store.create_upload("doc", part_count=3)
    for i, p in enumerate(parts, start=1):
        store.upload_part(up.upload_id, i, p, sha(p))
    v = store.complete_upload(up.upload_id)
    assert v.data == b"aaabbbccc"
    assert v.version_id == 1
    assert v.checksum == sha(b"aaabbbccc")
    assert store.get_object("doc").data == b"aaabbbccc"


def test_complete_with_missing_part_rejected():
    store = ObjectStore()
    log("缺失分片", "part_count=2，仅上传分片 1",
        "完成时必须以 part_missing 拒绝，且不产生可见对象")
    up = store.create_upload("doc", part_count=2)
    store.upload_part(up.upload_id, 1, b"aaa", sha(b"aaa"))
    with pytest.raises(StorageRejected) as ei:
        store.complete_upload(up.upload_id)
    assert ei.value.reason == RejectReason.PART_MISSING
    with pytest.raises(StorageRejected) as ei2:
        store.get_object("doc")
    assert ei2.value.reason == RejectReason.OBJECT_NOT_FOUND


def test_duplicate_part_rejected():
    store = ObjectStore()
    log("重复分片", "分片 1 上传两次（内容相同也算重复）",
        "第二次必须以 part_duplicate 拒绝")
    up = store.create_upload("doc", part_count=2)
    store.upload_part(up.upload_id, 1, b"aaa", sha(b"aaa"))
    with pytest.raises(StorageRejected) as ei:
        store.upload_part(up.upload_id, 1, b"aaa", sha(b"aaa"))
    assert ei.value.reason == RejectReason.PART_DUPLICATE


def test_checksum_mismatch_rejected():
    store = ObjectStore()
    log("校验和不匹配", "分片数据 b'aaa'，声明校验和为 b'bbb' 的 sha256",
        "必须以 checksum_mismatch 拒绝，且分片不被记录")
    up = store.create_upload("doc", part_count=1)
    with pytest.raises(StorageRejected) as ei:
        store.upload_part(up.upload_id, 1, b"aaa", sha(b"bbb"))
    assert ei.value.reason == RejectReason.CHECKSUM_MISMATCH
    with pytest.raises(StorageRejected) as ei2:
        store.complete_upload(up.upload_id)
    assert ei2.value.reason == RejectReason.PART_MISSING  # 分片未被记录


def test_part_out_of_range_rejected():
    store = ObjectStore()
    log("分片号越界", "part_count=2，上传分片号 3 与 0",
        "必须以 part_out_of_range 拒绝")
    up = store.create_upload("doc", part_count=2)
    for bad in (0, 3):
        with pytest.raises(StorageRejected) as ei:
            store.upload_part(up.upload_id, bad, b"x", sha(b"x"))
        assert ei.value.reason == RejectReason.PART_OUT_OF_RANGE


def test_abort_blocks_further_ops_and_leaves_nothing():
    store = ObjectStore()
    log("中止上传", "上传 1 个分片后中止，再尝试续传/完成/重复中止",
        "后续操作必须以 upload_aborted 拒绝；对象不可见；used_bytes 为 0")
    up = store.create_upload("doc", part_count=2)
    store.upload_part(up.upload_id, 1, b"aaa", sha(b"aaa"))
    store.abort_upload(up.upload_id)
    for op in (
        lambda: store.upload_part(up.upload_id, 2, b"bbb", sha(b"bbb")),
        lambda: store.complete_upload(up.upload_id),
        lambda: store.abort_upload(up.upload_id),
    ):
        with pytest.raises(StorageRejected) as ei:
            op()
        assert ei.value.reason == RejectReason.UPLOAD_ABORTED
    with pytest.raises(StorageRejected):
        store.get_object("doc")
    assert store.used_bytes == 0


def test_completed_upload_rejects_further_ops():
    store = ObjectStore()
    log("已完成上传的后续操作", "完成后再次上传分片/中止",
        "必须以 upload_completed 拒绝")
    up = store.create_upload("doc", part_count=1)
    store.upload_part(up.upload_id, 1, b"aaa", sha(b"aaa"))
    store.complete_upload(up.upload_id)
    with pytest.raises(StorageRejected) as ei:
        store.upload_part(up.upload_id, 1, b"aaa", sha(b"aaa"))
    assert ei.value.reason == RejectReason.UPLOAD_COMPLETED
    with pytest.raises(StorageRejected) as ei2:
        store.abort_upload(up.upload_id)
    assert ei2.value.reason == RejectReason.UPLOAD_COMPLETED


def test_unknown_upload_rejected():
    store = ObjectStore()
    log("未知上传会话", "upload_id='nope'",
        "必须以 upload_not_found 拒绝")
    with pytest.raises(StorageRejected) as ei:
        store.upload_part("nope", 1, b"x", sha(b"x"))
    assert ei.value.reason == RejectReason.UPLOAD_NOT_FOUND


# ---------- 版本与并发一致性 ----------

def test_overwrite_and_version_visibility():
    store = ObjectStore()
    log("覆盖与版本可见性", "依次写入 v1=b'one'、v2=b'two'，再读取 v1",
        "当前版本为 v2；显式读取 version_id=1 必须返回完整旧版本 b'one'")
    v1 = store.put_object("k", b"one")
    v2 = store.put_object("k", b"two")
    assert (v1.version_id, v2.version_id) == (1, 2)
    assert store.get_object("k").data == b"two"
    assert store.get_object("k", version_id=1).data == b"one"
    assert [v.version_id for v in store.list_versions("k")] == [1, 2]


def test_delete_tombstone_and_history():
    store = ObjectStore()
    log("删除可见性", "写入后删除，再读取",
        "读取必须以 object_not_found 拒绝；历史版本保留且含删除标记")
    store.put_object("k", b"data")
    tomb = store.delete_object("k")
    assert tomb.deleted and tomb.version_id == 2
    with pytest.raises(StorageRejected) as ei:
        store.get_object("k")
    assert ei.value.reason == RejectReason.OBJECT_NOT_FOUND
    history = store.list_versions("k")
    assert len(history) == 2 and history[-1].deleted
    assert store.used_bytes == 0


def test_concurrent_writes_no_pollution():
    store = ObjectStore()
    n = 32
    log("并发写入", f"{n} 个线程对同一 key 写入互不相同的值",
        "全部成功后版本数必须为 n；当前版本必须完整等于某一次写入（校验和自洽）")
    barrier = threading.Barrier(n)

    def write(i: int) -> None:
        barrier.wait()
        store.put_object("k", f"value-{i}".encode())

    threads = [threading.Thread(target=write, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    versions = store.list_versions("k")
    assert len(versions) == n
    final = store.get_object("k")
    assert final.data in {f"value-{i}".encode() for i in range(n)}
    assert final.checksum == sha(final.data)


def test_concurrent_reads_never_torn():
    store = ObjectStore()
    store.put_object("k", b"initial")
    stop = threading.Event()
    failures: list[str] = []
    log("并发读写不撕裂", "1 个写线程反复覆盖，4 个读线程持续读取并自校验",
        "每次读到的 data 的 sha256 必须等于该版本记录的 checksum")

    def writer() -> None:
        i = 0
        while not stop.is_set():
            store.put_object("k", f"payload-{i}".encode())
            i += 1

    def reader() -> None:
        while not stop.is_set():
            try:
                v = store.get_object("k")
            except StorageRejected:
                continue
            if v.checksum != sha(v.data):
                failures.append(f"torn read: {v!r}")

    wt = threading.Thread(target=writer)
    rts = [threading.Thread(target=reader) for _ in range(4)]
    wt.start()
    for t in rts:
        t.start()
    threading.Event().wait(0.3)  # 让读写并发交织一段时间
    stop.set()
    wt.join()
    for t in rts:
        t.join()
    assert failures == []


# ---------- 配额与保留 ----------

def test_quota_bytes_exceeded():
    store = ObjectStore(quota=QuotaPolicy(max_bytes=10))
    log("容量配额", "max_bytes=10，先写 6B 再写 6B",
        "第二次写入必须以 quota_bytes_exceeded 拒绝，且已用量保持 6B")
    store.put_object("a", b"123456")
    with pytest.raises(StorageRejected) as ei:
        store.put_object("b", b"123456")
    assert ei.value.reason == RejectReason.QUOTA_BYTES_EXCEEDED
    assert store.used_bytes == 6


def test_quota_objects_exceeded():
    store = ObjectStore(quota=QuotaPolicy(max_objects=1))
    log("数量配额", "max_objects=1，写入两个不同 key",
        "第二个 key 必须以 quota_objects_exceeded 拒绝；覆盖已有 key 不受数量限制")
    store.put_object("a", b"x")
    with pytest.raises(StorageRejected) as ei:
        store.put_object("b", b"y")
    assert ei.value.reason == RejectReason.QUOTA_OBJECTS_EXCEEDED
    store.put_object("a", b"xx")  # 覆盖同 key 允许
    assert store.get_object("a").data == b"xx"


def test_retention_blocks_overwrite_and_delete():
    now = [1000.0]
    store = ObjectStore(
        retention=RetentionPolicy(retain_seconds=60), clock=lambda: now[0]
    )
    log("保留策略", "retain_seconds=60，写入后立即覆盖/删除，再推进时钟 61s 后覆盖",
        "保留期内覆盖与删除必须以 retention_violation 拒绝；期满后允许")
    store.put_object("k", b"v1")
    with pytest.raises(StorageRejected) as ei:
        store.put_object("k", b"v2")
    assert ei.value.reason == RejectReason.RETENTION_VIOLATION
    with pytest.raises(StorageRejected) as ei2:
        store.delete_object("k")
    assert ei2.value.reason == RejectReason.RETENTION_VIOLATION
    now[0] += 61
    store.put_object("k", b"v2")
    assert store.get_object("k").data == b"v2"


# ---------- 幂等重复提交 ----------

def test_idempotent_put_replay():
    store = ObjectStore()
    log("写入幂等", "同一 idempotency_key='r1' 提交两次相同数据",
        "两次返回同一 version_id；版本数与已用量不重复增长")
    v1 = store.put_object("k", b"data", idempotency_key="r1")
    v2 = store.put_object("k", b"data", idempotency_key="r1")
    assert v1 is v2
    assert len(store.list_versions("k")) == 1
    assert store.used_bytes == 4


def test_idempotent_complete_replay():
    store = ObjectStore()
    log("合并提交幂等", "分块上传完成后以同一 idempotency_key 重复 complete",
        "重复提交返回首次版本，不产生多余版本、不重复占用配额")
    up = store.create_upload("doc", part_count=1)
    store.upload_part(up.upload_id, 1, b"aaa", sha(b"aaa"))
    v1 = store.complete_upload(up.upload_id, idempotency_key="c1")
    v2 = store.complete_upload(up.upload_id, idempotency_key="c1")
    assert v1 is v2
    assert len(store.list_versions("doc")) == 1
    assert store.used_bytes == 3


def test_idempotent_create_upload():
    store = ObjectStore()
    log("创建上传幂等", "同一 idempotency_key 创建两次上传",
        "第二次返回同一会话（相同 upload_id）")
    u1 = store.create_upload("doc", part_count=2, idempotency_key="u1")
    u2 = store.create_upload("doc", part_count=2, idempotency_key="u1")
    assert u1.upload_id == u2.upload_id
