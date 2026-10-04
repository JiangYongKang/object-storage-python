"""重试标识语义：状态变化后的重复删除/重复回滚、上传会话间隔离、拒绝时状态不变。

纯内存、可复现；每个用例打印输入与判定依据（pytest -s 可见）。
"""

from __future__ import annotations

import hashlib
import threading

import pytest

from storage import (
    ObjectStore,
    QuotaPolicy,
    RecoveryPolicy,
    RejectReason,
    RetentionPolicy,
    StorageRejected,
)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def log(case: str, inputs: str, basis: str) -> None:
    print(f"\n[CASE] {case}\n  输入: {inputs}\n  判定依据: {basis}")


# ---------- 删除 → 恢复 → 同一重试标识重复删除 ----------

def test_retry_delete_after_recover_actually_deletes():
    now = [1000.0]
    store = ObjectStore(
        recovery=RecoveryPolicy(recover_seconds=30), clock=lambda: now[0]
    )
    log("删除-恢复后用同一重试标识再删除",
        "idem='d1' 删除(v2 墓碑)→窗口内恢复(v3)→同 'd1' 重试删除",
        "重试必须真删：返回新墓碑 v4，之后读取 object_not_found；"
        "版本链 [1,2(墓碑),3,4(墓碑)] 完整不乱；容量正确释放；"
        "状态稳定后再用 'd1' 重试才重放 v4 结论")
    store.put_object("k", b"data")
    d1 = store.delete_object("k", idempotency_key="d1")
    assert d1.deleted and d1.version_id == 2
    now[0] += 5
    store.recover_object("k")  # v3，对象复活
    assert store.get_object("k").data == b"data"

    # 同一重试标识重试删除：必须真删，不能重放旧墓碑
    d2 = store.delete_object("k", idempotency_key="d1")
    assert d2.deleted and d2.version_id == 4
    with pytest.raises(StorageRejected) as ei:
        store.get_object("k")
    assert ei.value.reason == RejectReason.OBJECT_NOT_FOUND
    # 版本历史不乱：追加而非篡改
    versions = store.list_versions("k")
    assert [v.version_id for v in versions] == [1, 2, 3, 4]
    assert [v.deleted for v in versions] == [False, True, False, True]
    assert store.get_object("k", 3).data == b"data"  # 历史完整版本仍可读
    assert store.used_bytes == 0
    # 状态稳定后同一笔提交重放同一结论
    assert store.delete_object("k", idempotency_key="d1") is d2
    assert len(store.list_versions("k")) == 4


def test_retry_delete_rejected_by_retention_keeps_state():
    now = [1000.0]
    store = ObjectStore(
        retention=RetentionPolicy(retain_seconds=60),
        recovery=RecoveryPolicy(recover_seconds=300),
        clock=lambda: now[0],
    )
    log("恢复后重试删除撞上保留期",
        "retain=60s：@1000 写入，@1061 删除('d1')→@1062 恢复→@1063 同 'd1' 重试删除",
        "恢复出的新版本仍在保留期内，重试删除必须以 retention_violation 明确拒绝；"
        "拒绝后对象状态保持原样：仍存活、内容不变、版本数不变、容量不变；"
        "保留期满后同一重试标识可真正删除成功")
    store.put_object("k", b"data")
    now[0] += 61  # 出保留期才允许第一次删除
    store.delete_object("k", idempotency_key="d1")
    now[0] += 1
    store.recover_object("k")  # v3，created_at 重新计时
    now[0] += 1
    with pytest.raises(StorageRejected) as ei:
        store.delete_object("k", idempotency_key="d1")
    assert ei.value.reason == RejectReason.RETENTION_VIOLATION
    # 状态原样：对象活着、内容可读、版本与容量未动
    assert store.get_object("k").data == b"data"
    assert [v.version_id for v in store.list_versions("k")] == [1, 2, 3]
    assert store.used_bytes == 4
    # 保留期满后同一重试标识真正删除成功
    now[0] += 61
    d = store.delete_object("k", idempotency_key="d1")
    assert d.deleted
    with pytest.raises(StorageRejected):
        store.get_object("k")


# ---------- 回滚 → 又写入新内容 → 同一重试标识重复回滚 ----------

def test_retry_rollback_after_new_write_converges_to_target():
    store = ObjectStore()
    log("回滚后被写入新内容，再用同一重试标识重复回滚",
        "v1=b'one'、v2=b'two'；idem='rb1' 回滚到 v1(v3)；写入 v4=b'new'；同 'rb1' 重试回滚",
        "重试必须按新一笔执行：追加 v5=b'one'，紧接着读到的就是回滚目标 b'one'；"
        "中间写入的 v4 不丢（按 version_id 仍可读）；状态稳定后再重试才重放 v5")
    store.put_object("k", b"one")
    store.put_object("k", b"two")
    r1 = store.rollback_object("k", 1, idempotency_key="rb1")
    assert r1.version_id == 3 and r1.data == b"one"
    store.put_object("k", b"new")  # v4：两次重试之间对象被改动

    r2 = store.rollback_object("k", 1, idempotency_key="rb1")
    assert r2.version_id == 5 and r2.data == b"one"
    # 报成功的回滚：紧接着读到的必须等于回滚目标
    assert store.get_object("k").data == b"one"
    # 之前正常写入的内容不丢：历史版本完整可读
    assert store.get_object("k", 4).data == b"new"
    assert [v.version_id for v in store.list_versions("k")] == [1, 2, 3, 4, 5]
    # 状态稳定后同一笔提交结论稳定，不多出版本、不重复占配额
    assert store.rollback_object("k", 1, idempotency_key="rb1") is r2
    assert len(store.list_versions("k")) == 5
    assert store.used_bytes == 3


def test_retry_recover_rejected_by_quota_keeps_state():
    store = ObjectStore(
        quota=QuotaPolicy(max_bytes=10),
        recovery=RecoveryPolicy(recover_seconds=300),
    )
    log("重试恢复超配额被拒",
        "max_bytes=10：k 写 8B→删除('del1')→j 写 8B 占满→同 'rc' 重试恢复 k",
        "恢复 8B 将使总量达 16B 超上限：必须 quota_bytes_exceeded 拒绝；"
        "拒绝后 k 仍是删除状态、j 内容不变、已用量保持 8B")
    store.put_object("k", b"12345678")
    store.delete_object("k", idempotency_key="del1")
    assert store.used_bytes == 0
    store.put_object("j", b"abcdefgh")  # 别人把配额占满
    assert store.used_bytes == 8
    with pytest.raises(StorageRejected) as ei:
        store.recover_object("k", idempotency_key="rc")
    assert ei.value.reason == RejectReason.QUOTA_BYTES_EXCEEDED
    # 状态原样
    with pytest.raises(StorageRejected) as ei2:
        store.get_object("k")
    assert ei2.value.reason == RejectReason.OBJECT_NOT_FOUND
    assert store.get_object("j").data == b"abcdefgh"
    assert store.used_bytes == 8
    assert [v.version_id for v in store.list_versions("k")] == [1, 2]


def test_retry_recover_after_window_expired_keeps_state():
    now = [1000.0]
    store = ObjectStore(
        recovery=RecoveryPolicy(recover_seconds=30), clock=lambda: now[0]
    )
    log("恢复窗口外的重试恢复",
        "@1000 删除；@1005 同 'rc' 恢复成功；@1006 再删除；@1040 同 'rc' 重试恢复",
        "第二次墓碑已超窗：必须 recovery_window_expired 拒绝；"
        "拒绝后仍是删除状态、历史可查、容量为 0")
    store.put_object("k", b"data")
    store.delete_object("k")
    now[0] += 5
    store.recover_object("k", idempotency_key="rc")  # v3
    now[0] += 1
    store.delete_object("k")  # v4 墓碑 @1006
    now[0] += 34              # @1040，距 v4 已 34s > 30s
    with pytest.raises(StorageRejected) as ei:
        store.recover_object("k", idempotency_key="rc")
    assert ei.value.reason == RejectReason.RECOVERY_WINDOW_EXPIRED
    with pytest.raises(StorageRejected) as ei2:
        store.get_object("k")
    assert ei2.value.reason == RejectReason.OBJECT_NOT_FOUND
    assert store.get_object("k", 3).data == b"data"
    assert store.used_bytes == 0


# ---------- 同一对象、两个上传会话共用同一重试标识 ----------

def test_two_upload_sessions_same_idempotency_key_isolated():
    store = ObjectStore()
    log("同一对象两个上传会话共用同一重试标识",
        "key=k：会话A、会话B 都以 idem='u' 创建、各自传不同内容、都以 idem='c' 完成",
        "两个会话各自得到自己的结果：A→v1=b'from-A'，B→v2=b'from-B'；"
        "B 的内容不被丢弃也不被顶替；重复 complete 同一会话仍重放自己的结论")
    up_a = store.create_upload("k", part_count=1, idempotency_key="u")
    store.upload_part(up_a.upload_id, 1, b"from-A", sha(b"from-A"))
    va = store.complete_upload(up_a.upload_id, idempotency_key="c")
    assert va.data == b"from-A" and va.version_id == 1

    up_b = store.create_upload("k", part_count=1, idempotency_key="u")
    assert up_b.upload_id != up_a.upload_id  # A 已完成，B 是新会话
    store.upload_part(up_b.upload_id, 1, b"from-B", sha(b"from-B"))
    vb = store.complete_upload(up_b.upload_id, idempotency_key="c")
    # B 的结果必须是他自己的内容，不能被 A 的结论顶替
    assert vb.data == b"from-B" and vb.version_id == 2
    assert store.get_object("k").data == b"from-B"
    assert store.get_object("k", 1).data == b"from-A"  # A 的成果也不丢
    assert store.used_bytes == len(b"from-B")

    # 同一会话、状态没变的重复 complete：结论稳定，不多出版本、不重复占配额
    assert store.complete_upload(up_b.upload_id, idempotency_key="c") is vb
    assert store.complete_upload(up_a.upload_id, idempotency_key="c") is va
    assert len(store.list_versions("k")) == 2
    assert store.used_bytes == len(b"from-B")


def test_interleaved_upload_sessions_same_key_no_mixing():
    store = ObjectStore()
    log("两个会话交错进行、共用完成键",
        "key=k：A、B 各自创建（不同创建键）→交错传分片→同 idem='c2' 先后完成",
        "先完成者占 v1，后完成者占 v2，内容各自正确、互不串味")
    up_a = store.create_upload("k", part_count=2, idempotency_key="uA")
    up_b = store.create_upload("k", part_count=2, idempotency_key="uB")
    store.upload_part(up_a.upload_id, 1, b"A1", sha(b"A1"))
    store.upload_part(up_b.upload_id, 1, b"B1", sha(b"B1"))
    store.upload_part(up_a.upload_id, 2, b"A2", sha(b"A2"))
    store.upload_part(up_b.upload_id, 2, b"B2", sha(b"B2"))
    vb = store.complete_upload(up_b.upload_id, idempotency_key="c2")
    va = store.complete_upload(up_a.upload_id, idempotency_key="c2")
    assert vb.data == b"B1B2" and va.data == b"A1A2"
    assert (vb.version_id, va.version_id) == (1, 2)
    assert store.get_object("k").data == b"A1A2"
    assert store.get_object("k", 1).data == b"B1B2"


# ---------- 并发：回滚/删除/写入交织时读取永远对应某一完整版本 ----------

def test_concurrent_retry_ops_reads_always_consistent():
    store = ObjectStore(
        recovery=RecoveryPolicy(recover_seconds=3600),
        clock=lambda: 1000.0,
    )
    log("并发带重试标识的回滚/删除/恢复/写入",
        "4 写线程(同 idem='w1') + 3 回滚线程(同 idem='rb') + 2 删除线程(同 idem='d')"
        " + 1 恢复线程(同 idem='rc')；6 个读线程全程自校验",
        "同一幂等键并发提交只生效一笔或安全重放；每次读取都对应某一完整版本"
        "（校验和自洽）；版本链无空洞；最终账实相符")
    store.put_object("k", b"v0")
    n_workers = 10
    barrier = threading.Barrier(n_workers)
    stop = threading.Event()
    errors: list[str] = []
    allowed = {
        RejectReason.OBJECT_NOT_FOUND,
        RejectReason.OBJECT_NOT_DELETED,
        RejectReason.RETENTION_VIOLATION,
        RejectReason.RECOVERY_WINDOW_EXPIRED,
    }

    def run(op, *args):
        barrier.wait()
        try:
            op(*args)
        except StorageRejected as exc:
            if exc.reason not in allowed:
                errors.append(f"unexpected: {exc.reason}")

    def reader():
        while not stop.is_set():
            try:
                v = store.get_object("k")
                if v.checksum != sha(v.data):
                    errors.append(f"torn read: {v!r}")
            except StorageRejected as exc:
                if exc.reason != RejectReason.OBJECT_NOT_FOUND:
                    errors.append(f"read unexpected: {exc.reason}")

    workers = [
        *(threading.Thread(target=run, args=(store.put_object, "k", b"w", "w1"))
          for _ in range(4)),
        *(threading.Thread(target=run, args=(store.rollback_object, "k", 1, "rb"))
          for _ in range(3)),
        *(threading.Thread(target=run, args=(store.delete_object, "k", "d"))
          for _ in range(2)),
        threading.Thread(target=run, args=(store.recover_object, "k", "rc")),
    ]
    readers = [threading.Thread(target=reader) for _ in range(6)]
    for t in readers:
        t.start()
    for t in workers:
        t.start()
    for t in workers:
        t.join()
    stop.set()
    for t in readers:
        t.join()

    assert errors == []
    versions = store.list_versions("k")
    assert [v.version_id for v in versions] == list(range(1, len(versions) + 1))
    for v in versions:
        if not v.deleted:
            assert v.checksum == sha(v.data)
    live = versions[-1] if versions and not versions[-1].deleted else None
    assert store.used_bytes == (live.size if live else 0)
