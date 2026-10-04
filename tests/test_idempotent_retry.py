"""重试语义专项单测（纯内存、假时钟、可复现）。

覆盖业务（业务清单与结论由 tests/conftest.py 在运行日志开头/结尾统一打印）：
A. 删除-恢复后用同一条重试标识重复删除：成功必真删、历史不乱；状态没变时结论稳定。
B. 重复删除被拒绝的边界：恢复后仍在保留期 -> retention_violation 且状态原样不动。
C. 回滚后又写入新内容、同键重复回滚：成功后读到的必为目标数据；中间写入不丢。
D. 重复回滚的边界：对象已被删除 -> 拒绝且状态不变；重放时超配额 -> 拒绝且状态不变。
E. 同一对象两个上传会话共用同一条重试标识：各得各的结果，会话内重试稳定、不顶替。
F. 重复恢复的边界：超恢复窗口拒绝、对象已存活拒绝，均状态不变。
G. 并发回滚/删除/恢复/写入：读者每次读到的都是某一完整版本（校验和自洽），账实相符。
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


# ---------- A. 删除 -> 恢复 -> 同键重复删除 ----------

def test_delete_retry_after_recover_actually_deletes():
    now = [1000.0]
    store = ObjectStore(
        recovery=RecoveryPolicy(recover_seconds=30), clock=lambda: now[0]
    )
    log("A 删除后窗口内恢复，再用同一重试标识重复删除",
        "v1@1000 删除(del)@1000 -> 恢复@1005 -> 同键 del 重试删除",
        "重试必须真删掉（随后 object_not_found）；版本链 [1,2墓碑,3,4墓碑] 不乱、"
        "历史可读、容量归零；再次重试稳定返回同一墓碑、不多出版本")
    store.put_object("k", b"data")
    t1 = store.delete_object("k", idempotency_key="del")
    assert t1.deleted and t1.version_id == 2
    now[0] += 5
    store.recover_object("k")  # v3 恢复版本
    t2 = store.delete_object("k", idempotency_key="del")
    assert t2.deleted and t2.version_id == 4
    assert t2 is not t1  # 不是旧结论：重新执行并产生了新墓碑

    # 报成功就一定真删掉
    with pytest.raises(StorageRejected) as ei:
        store.get_object("k")
    assert ei.value.reason == RejectReason.OBJECT_NOT_FOUND

    # 版本历史不乱：连续、墓碑在尾、历史内容都还能读
    hist = store.list_versions("k")
    assert [v.version_id for v in hist] == [1, 2, 3, 4]
    assert hist[1].deleted and hist[3].deleted
    assert store.get_object("k", 1).data == b"data"
    assert store.get_object("k", 3).data == b"data"
    assert store.used_bytes == 0

    # 状态没变的同一笔提交再重试：结论稳定，不多版本、不重复扣容量
    t3 = store.delete_object("k", idempotency_key="del")
    assert t3 is t2
    assert len(store.list_versions("k")) == 4
    assert store.used_bytes == 0


# ---------- B. 恢复后仍在保留期，重复删除被拒，状态不变 ----------

def test_delete_retry_within_retention_rejected_state_unchanged():
    now = [1000.0]
    store = ObjectStore(
        retention=RetentionPolicy(retain_seconds=60),
        recovery=RecoveryPolicy(recover_seconds=300),
        clock=lambda: now[0],
    )
    log("B 恢复后的新版本仍在保留期，同一重试标识重复删除",
        "v1@1000；@1061 删除(del) -> @1071 恢复(v3) -> @1076 同键重试删除 -> @1136 再重试",
        "@1076 必须 retention_violation 且对象原样存活、版本不增、容量不变；"
        "保留期满后重试成功且真删掉")
    store.put_object("k", b"data")
    now[0] += 61  # 让首次删除越过保留期
    store.delete_object("k", idempotency_key="del")
    now[0] += 10
    store.recover_object("k")  # 恢复版本 @1071，重新计时
    before = store.list_versions("k")
    assert len(before) == 3 and not before[-1].deleted

    now[0] += 5
    with pytest.raises(StorageRejected) as ei:
        store.delete_object("k", idempotency_key="del")
    assert ei.value.reason == RejectReason.RETENTION_VIOLATION

    # 拒绝后状态原样：对象还活着、读得到、版本不增、容量不变
    assert store.get_object("k").data == b"data"
    after = store.list_versions("k")
    assert [v.version_id for v in after] == [1, 2, 3]
    assert store.used_bytes == 4

    # 保留期满后同键重试：成功并真删掉
    now[0] += 60
    t = store.delete_object("k", idempotency_key="del")
    assert t.deleted and t.version_id == 4
    with pytest.raises(StorageRejected) as ei2:
        store.get_object("k")
    assert ei2.value.reason == RejectReason.OBJECT_NOT_FOUND
    assert store.used_bytes == 0


# ---------- C. 回滚 -> 新写入 -> 同键重复回滚 ----------

def test_rollback_retry_after_new_write_matches_target():
    store = ObjectStore()
    log("C 回滚后对象又被写入新内容，同一重试标识重复回滚",
        "v1=b'one'、v2=b'two' -> 回滚v1(rb)得v3 -> 写入 b'three' 得v4 -> 同键 rb 重试",
        "重试必须重新生效：紧接着读到 b'one'；正常写入的 v4 不丢（按版本可读）；"
        "版本链 [1..5] 完整；再重试结论稳定、不增版本、不重复占配额")
    v1 = store.put_object("k", b"one")
    store.put_object("k", b"two")
    r1 = store.rollback_object("k", v1.version_id, idempotency_key="rb")
    assert r1.version_id == 3 and r1.data == b"one"
    store.put_object("k", b"three")  # 回滚之后的正常写入，不能丢

    r2 = store.rollback_object("k", v1.version_id, idempotency_key="rb")
    assert r2.version_id == 5 and r2.data == b"one"
    assert r2 is not r1  # 不能停在与当前可读内容对不上的旧结论上

    # 报成功则紧接着读到的必为回滚目标数据
    assert store.get_object("k").data == b"one"
    # 中间历史与正常写入一律保留
    assert [v.version_id for v in store.list_versions("k")] == [1, 2, 3, 4, 5]
    assert store.get_object("k", 4).data == b"three"
    assert store.get_object("k", 2).data == b"two"
    assert store.used_bytes == 3

    # 状态没变的同笔提交：稳定重放
    r3 = store.rollback_object("k", v1.version_id, idempotency_key="rb")
    assert r3 is r2
    assert len(store.list_versions("k")) == 5
    assert store.used_bytes == 3


# ---------- D. 重复回滚被拒的边界 ----------

def test_rollback_retry_when_deleted_rejected_state_unchanged():
    store = ObjectStore(recovery=RecoveryPolicy(recover_seconds=30))
    log("D1 回滚后对象被另一笔提交删除，同键重试回滚",
        "v1/v2 -> 回滚v1(rb)v3 -> 无键删除 v4墓碑 -> 同键 rb 重试",
        "必须 object_not_found（已删除请先 recover）；墓碑不变、版本不增、仍读不到")
    v1 = store.put_object("k", b"one")
    store.put_object("k", b"two")
    store.rollback_object("k", v1.version_id, idempotency_key="rb")
    store.delete_object("k")  # 另一笔提交
    with pytest.raises(StorageRejected) as ei:
        store.rollback_object("k", v1.version_id, idempotency_key="rb")
    assert ei.value.reason == RejectReason.OBJECT_NOT_FOUND
    hist = store.list_versions("k")
    assert [v.version_id for v in hist] == [1, 2, 3, 4]
    assert hist[-1].deleted
    with pytest.raises(StorageRejected) as ei2:
        store.get_object("k")
    assert ei2.value.reason == RejectReason.OBJECT_NOT_FOUND


def test_rollback_retry_quota_exceeded_state_unchanged():
    store = ObjectStore(quota=QuotaPolicy(max_bytes=20))
    log("D2 回滚后别的对象占满容量、本对象又改小，同键重试回滚",
        "max_bytes=20：k=10B 回滚v1(rb)；g=10B；k 改写为 3B；g 增大到 17B；重试 rb",
        "重试回滚将使容量 20-3+10=27B > 20B：quota_bytes_exceeded；k 仍为 3B、"
        "版本不增、已用量不变；腾出容量后同键重试可成功")
    target = b"k" * 10
    store.put_object("k", target)                                   # used 10
    store.rollback_object("k", 1, idempotency_key="rb")            # v2，仍 10B
    store.put_object("g", b"g" * 10)                               # used 20
    store.put_object("k", b"abc")                                  # used 13
    store.put_object("g", b"g" * 17)                               # used 20

    with pytest.raises(StorageRejected) as ei:
        store.rollback_object("k", 1, idempotency_key="rb")
    assert ei.value.reason == RejectReason.QUOTA_BYTES_EXCEEDED

    # 拒绝后状态原样
    assert store.get_object("k").data == b"abc"
    assert [v.version_id for v in store.list_versions("k")] == [1, 2, 3]
    assert store.used_bytes == 20

    # 腾出容量后同键重试成功，读到的即回滚目标
    store.delete_object("g")  # used 3
    r = store.rollback_object("k", 1, idempotency_key="rb")
    assert r.data == target
    assert store.get_object("k").data == target
    assert store.used_bytes == 10


# ---------- E. 同一对象两个上传会话共用同一条重试标识 ----------

def test_two_upload_sessions_same_idem_key_isolated():
    store = ObjectStore()
    log("E 同一对象两个不同上传会话共用 idempotency_key='c'",
        "会话1 传 AAA 完成(c)；会话2 传 BBB 完成(c)；再分别重试 complete",
        "会话2 不得被重放成会话1的结论（BBB 不丢不被顶替）；各自重试只返回本会话的"
        "版本、不顶替当前内容；仅 2 个版本、容量 3B")

    u1 = store.create_upload("k", part_count=1)
    store.upload_part(u1.upload_id, 1, b"AAA", sha(b"AAA"))
    v1 = store.complete_upload(u1.upload_id, idempotency_key="c")
    assert v1.data == b"AAA" and v1.version_id == 1

    u2 = store.create_upload("k", part_count=1)
    assert u2.upload_id != u1.upload_id
    store.upload_part(u2.upload_id, 1, b"BBB", sha(b"BBB"))
    v2 = store.complete_upload(u2.upload_id, idempotency_key="c")
    assert v2.data == b"BBB" and v2.version_id == 2  # 新内容真正生效
    assert store.get_object("k").data == b"BBB"

    # 会话1的网络重试：只拿到会话1自己的结论，不把当前内容顶回 AAA
    r1 = store.complete_upload(u1.upload_id, idempotency_key="c")
    assert r1 is v1
    assert store.get_object("k").data == b"BBB"
    # 会话2重试同理
    r2 = store.complete_upload(u2.upload_id, idempotency_key="c")
    assert r2 is v2
    assert store.get_object("k").data == b"BBB"

    assert [v.version_id for v in store.list_versions("k")] == [1, 2]
    assert store.used_bytes == 3  # 不重复占配额


# ---------- F. 重复恢复被拒的边界 ----------

def test_recover_retry_after_window_expired_state_unchanged():
    now = [1000.0]
    store = ObjectStore(
        recovery=RecoveryPolicy(recover_seconds=30), clock=lambda: now[0]
    )
    log("F1 删除-恢复-再删除后超出恢复窗口，同一重试标识重复恢复",
        "删除 -> 恢复(rc) -> 再删除 -> 时钟推进 31s -> 同键 rc 重试",
        "必须 recovery_window_expired；对象仍是第二次的墓碑、版本不增、容量为 0、"
        "历史仍可查")
    store.put_object("k", b"data")
    store.delete_object("k")
    now[0] += 5
    store.recover_object("k", idempotency_key="rc")  # v3
    store.delete_object("k")                         # v4 墓碑（另一笔提交）
    before = store.list_versions("k")
    assert len(before) == 4 and before[-1].deleted

    now[0] += 31
    with pytest.raises(StorageRejected) as ei:
        store.recover_object("k", idempotency_key="rc")
    assert ei.value.reason == RejectReason.RECOVERY_WINDOW_EXPIRED

    # 状态原样
    hist = store.list_versions("k")
    assert [v.version_id for v in hist] == [1, 2, 3, 4]
    assert hist[-1].deleted
    with pytest.raises(StorageRejected) as ei2:
        store.get_object("k")
    assert ei2.value.reason == RejectReason.OBJECT_NOT_FOUND
    assert store.get_object("k", 1).data == b"data"
    assert store.used_bytes == 0


def test_recover_retry_when_live_state_unchanged():
    now = [1000.0]
    store = ObjectStore(
        recovery=RecoveryPolicy(recover_seconds=300), clock=lambda: now[0]
    )
    log("F2 恢复后对象又写入了新内容，同一重试标识重复恢复",
        "删除 -> 恢复(rc)v3 -> 写入 b'new' v4 -> 同键 rc 重试",
        "对象当前存活：object_not_deleted；新写入不丢、版本不增、容量不变")
    store.put_object("k", b"data")
    store.delete_object("k")
    now[0] += 5
    r1 = store.recover_object("k", idempotency_key="rc")
    assert r1.version_id == 3
    store.put_object("k", b"new")  # 恢复之后的正常写入
    with pytest.raises(StorageRejected) as ei:
        store.recover_object("k", idempotency_key="rc")
    assert ei.value.reason == RejectReason.OBJECT_NOT_DELETED
    assert store.get_object("k").data == b"new"
    assert [v.version_id for v in store.list_versions("k")] == [1, 2, 3, 4]
    assert store.used_bytes == 3


# ---------- G. 并发混合操作，每次读取都对应完整版本 ----------

def test_concurrent_mixed_ops_reads_always_complete_version():
    store = ObjectStore(
        recovery=RecoveryPolicy(recover_seconds=3600),
        clock=lambda: 1000.0,  # 固定时钟：无保留期压力、恢复窗口始终有效
    )
    log("G 并发回滚/删除/恢复/写入 + 4 个持续读者",
        "8 写线程 + 4 回滚线程 + 2 删除线程 + 2 恢复线程，读者持续读取并自校验",
        "每次成功读取的 checksum 必须等于其 data 的 sha256（即某一完整版本）；"
        "版本号连续无空洞；已用量与当前存活版本账实相符")
    store.put_object("k", b"v0")
    n_total = 16 + 4 + 2 + 2
    barrier = threading.Barrier(n_total)
    errors: list[str] = []
    stop = threading.Event()
    torn: list[str] = []

    def writer(i: int) -> None:
        barrier.wait()
        try:
            store.put_object("k", f"w{i}".encode())
        except StorageRejected as exc:
            if exc.reason != RejectReason.OBJECT_NOT_FOUND:
                errors.append(f"writer unexpected: {exc.reason}")

    def roller(i: int) -> None:
        barrier.wait()
        try:
            store.rollback_object("k", 1)  # 回滚到 v0
        except StorageRejected as exc:
            if exc.reason not in (
                RejectReason.OBJECT_NOT_FOUND,
                RejectReason.RETENTION_VIOLATION,
            ):
                errors.append(f"roller unexpected: {exc.reason}")

    def deleter() -> None:
        barrier.wait()
        try:
            store.delete_object("k")
        except StorageRejected as exc:
            if exc.reason != RejectReason.OBJECT_NOT_FOUND:
                errors.append(f"deleter unexpected: {exc.reason}")

    def recoverer() -> None:
        barrier.wait()
        try:
            store.recover_object("k")
        except StorageRejected as exc:
            if exc.reason not in (
                RejectReason.OBJECT_NOT_DELETED,
                RejectReason.RECOVERY_WINDOW_EXPIRED,
            ):
                errors.append(f"recoverer unexpected: {exc.reason}")

    def reader() -> None:
        while not stop.is_set():
            try:
                v = store.get_object("k")
            except StorageRejected as exc:
                if exc.reason != RejectReason.OBJECT_NOT_FOUND:
                    torn.append(f"unexpected read error: {exc.reason}")
                continue
            if v.checksum != sha(v.data) or v.size != len(v.data):
                torn.append(f"torn/inconsistent read: {v!r}")

    mutators = (
        [threading.Thread(target=writer, args=(i,)) for i in range(16)]
        + [threading.Thread(target=roller, args=(i,)) for i in range(4)]
        + [threading.Thread(target=deleter), threading.Thread(target=deleter)]
        + [threading.Thread(target=recoverer), threading.Thread(target=recoverer)]
    )
    readers = [threading.Thread(target=reader) for _ in range(4)]

    for t in readers:
        t.start()
    threading.Event().wait(0.02)  # 让读者先进入读取循环
    for t in mutators:
        t.start()
    for t in mutators:
        t.join()
    stop.set()
    for t in readers:
        t.join()

    assert errors == []
    assert torn == []

    # 版本链连续无空洞；每个版本自洽
    versions = store.list_versions("k")
    assert [v.version_id for v in versions] == list(range(1, len(versions) + 1))
    for v in versions:
        if not v.deleted:
            assert v.checksum == sha(v.data)
            assert v.size == len(v.data)
    # 账实相符：已用量等于当前存活版本大小
    live = versions[-1] if versions and not versions[-1].deleted else None
    assert store.used_bytes == (live.size if live else 0)
