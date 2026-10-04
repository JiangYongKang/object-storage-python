"""回滚与可恢复删除：纯内存、可复现，每个用例打印输入与判定依据。"""

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


# ---------- 按历史版本回滚 ----------

def test_rollback_to_historical_version_appends_new_version():
    store = ObjectStore()
    log("回滚到历史版本", "v1=b'one'、v2=b'two'，回滚到 v1",
        "回滚追加新版本 v3，内容为 b'one'；当前读到 b'one'；"
        "v1/v2/v3 都能按 version_id 稳定读出，历史不被抹掉")
    v1 = store.put_object("k", b"one")
    v2 = store.put_object("k", b"two")
    v3 = store.rollback_object("k", v1.version_id)
    assert v3.version_id == 3
    assert v3.data == b"one"
    assert store.get_object("k").data == b"one"
    assert store.get_object("k", 1).data == b"one"
    assert store.get_object("k", 2).data == b"two"
    assert store.get_object("k", 3).data == b"one"
    assert [v.version_id for v in store.list_versions("k")] == [1, 2, 3]


def test_rollback_unknown_version_rejected():
    store = ObjectStore()
    log("回滚到不存在的版本", "只有 v1，回滚 version_id=9",
        "必须以 version_not_found 拒绝，且不产生新版本")
    store.put_object("k", b"one")
    with pytest.raises(StorageRejected) as ei:
        store.rollback_object("k", 9)
    assert ei.value.reason == RejectReason.VERSION_NOT_FOUND
    assert len(store.list_versions("k")) == 1


def test_rollback_to_tombstone_rejected():
    store = ObjectStore()
    log("回滚目标是墓碑", "v1 后删除（v2 墓碑），再写 v3，尝试回滚到 v2",
        "必须以 version_not_restorable 拒绝；回滚已删除对象需先 recover")
    store.put_object("k", b"one")
    store.delete_object("k")
    store.put_object("k", b"three")
    with pytest.raises(StorageRejected) as ei:
        store.rollback_object("k", 2)
    assert ei.value.reason == RejectReason.VERSION_NOT_RESTORABLE


def test_rollback_blocked_by_retention():
    now = [1000.0]
    store = ObjectStore(
        retention=RetentionPolicy(retain_seconds=60), clock=lambda: now[0]
    )
    log("回滚受保留期约束", "v1@1000、v2@1061，立即回滚到 v1 被拒；时钟再推进 61s 后回滚",
        "保留期内以 retention_violation 拒绝；期满后允许")
    store.put_object("k", b"one")
    now[0] += 61
    store.put_object("k", b"two")
    with pytest.raises(StorageRejected) as ei:
        store.rollback_object("k", 1)
    assert ei.value.reason == RejectReason.RETENTION_VIOLATION
    now[0] += 61
    v3 = store.rollback_object("k", 1)
    assert v3.data == b"one"


def test_rollback_respects_quota():
    store = ObjectStore(quota=QuotaPolicy(max_bytes=10))
    log("回滚受配额约束", "max_bytes=10：写 2B 再写 10B（已用 10B），回滚到 2B 允许",
        "回滚后已用量变为 2B；若回滚目标放大到超配额则拒绝")
    store.put_object("k", b"12")
    store.put_object("k", b"x" * 10)
    assert store.used_bytes == 10
    v3 = store.rollback_object("k", 1)
    assert v3.data == b"12" and store.used_bytes == 2
    # 当前 2B，配额 10B，回滚/写入超过 10B 的内容不允许
    with pytest.raises(StorageRejected) as ei:
        store.put_object("k", b"y" * 11)
    assert ei.value.reason == RejectReason.QUOTA_BYTES_EXCEEDED


def test_rollback_idempotent_replay():
    store = ObjectStore()
    log("重复回滚幂等", "同一 idempotency_key='rb1' 对同一目标版本回滚两次",
        "两次返回同一版本对象；版本数、已用量不重复增长")
    store.put_object("k", b"one")
    store.put_object("k", b"two")
    r1 = store.rollback_object("k", 1, idempotency_key="rb1")
    r2 = store.rollback_object("k", 1, idempotency_key="rb1")
    assert r1 is r2
    assert len(store.list_versions("k")) == 3
    assert store.used_bytes == 3


# ---------- 可恢复删除 ----------

def test_recover_within_window_restores_content():
    now = [1000.0]
    store = ObjectStore(
        recovery=RecoveryPolicy(recover_seconds=30), clock=lambda: now[0]
    )
    log("窗口内恢复", "v1=b'one'、v2=b'two'，@1000 删除；@1010 恢复",
        "删除后读取 object_not_found 但历史可查；恢复追加新版本 v4=b'two'；"
        "当前读 b'two'；版本序列为 [1,2,3(墓碑),4]；容量恢复为 3B")
    store.put_object("k", b"one")
    store.put_object("k", b"two")
    tomb = store.delete_object("k")
    assert tomb.version_id == 3 and tomb.deleted
    with pytest.raises(StorageRejected) as ei:
        store.get_object("k")
    assert ei.value.reason == RejectReason.OBJECT_NOT_FOUND
    # 删除窗口内历史版本仍可查
    assert store.get_object("k", 1).data == b"one"
    assert store.get_object("k", 2).data == b"two"
    assert store.used_bytes == 0
    now[0] += 10
    restored = store.recover_object("k")
    assert restored.version_id == 4
    assert restored.data == b"two"
    assert store.get_object("k").data == b"two"
    assert [v.version_id for v in store.list_versions("k")] == [1, 2, 3, 4]
    assert store.used_bytes == 3


def test_recover_after_window_expired_rejected():
    now = [1000.0]
    store = ObjectStore(
        recovery=RecoveryPolicy(recover_seconds=30), clock=lambda: now[0]
    )
    log("超过恢复窗口", "@1000 删除，@1031 尝试恢复",
        "必须以 recovery_window_expired 拒绝；对象仍处于删除状态、历史仍可查")
    store.put_object("k", b"data")
    store.delete_object("k")
    now[0] += 31
    with pytest.raises(StorageRejected) as ei:
        store.recover_object("k")
    assert ei.value.reason == RejectReason.RECOVERY_WINDOW_EXPIRED
    with pytest.raises(StorageRejected) as ei2:
        store.get_object("k")
    assert ei2.value.reason == RejectReason.OBJECT_NOT_FOUND
    assert store.get_object("k", 1).data == b"data"


def test_recover_disabled_when_window_zero():
    store = ObjectStore()  # 默认 recover_seconds=0，兼容旧语义
    log("未配置恢复窗口", "默认策略下删除后恢复",
        "必须以 recovery_window_expired 拒绝（删除即不可恢复）")
    store.put_object("k", b"data")
    store.delete_object("k")
    with pytest.raises(StorageRejected) as ei:
        store.recover_object("k")
    assert ei.value.reason == RejectReason.RECOVERY_WINDOW_EXPIRED


def test_recover_live_object_rejected():
    store = ObjectStore(recovery=RecoveryPolicy(recover_seconds=30))
    log("恢复未删除对象", "对象存活时调用 recover",
        "必须以 object_not_deleted 拒绝，不产生版本")
    store.put_object("k", b"data")
    with pytest.raises(StorageRejected) as ei:
        store.recover_object("k")
    assert ei.value.reason == RejectReason.OBJECT_NOT_DELETED
    assert len(store.list_versions("k")) == 1


def test_recover_idempotent_replay():
    now = [1000.0]
    store = ObjectStore(
        recovery=RecoveryPolicy(recover_seconds=30), clock=lambda: now[0]
    )
    log("重复恢复幂等", "删除后以同一 idempotency_key='rc1' 恢复两次",
        "两次返回同一版本；不重复占配额、不多出版本")
    store.put_object("k", b"data")
    store.delete_object("k")
    now[0] += 5
    r1 = store.recover_object("k", idempotency_key="rc1")
    r2 = store.recover_object("k", idempotency_key="rc1")
    assert r1 is r2
    assert len(store.list_versions("k")) == 3  # v1, 墓碑, 恢复版本
    assert store.used_bytes == 4


def test_recover_after_redelete_uses_latest_content():
    now = [1000.0]
    store = ObjectStore(
        recovery=RecoveryPolicy(recover_seconds=30), clock=lambda: now[0]
    )
    log("删除-恢复-再删除-再恢复", "v1→删除→恢复(v3)→再删除(v4)→再恢复(v5)",
        "第二次恢复内容必须是第二次删除前的存活版本，版本链完整不丢不乱")
    store.put_object("k", b"first")
    store.delete_object("k")
    now[0] += 5
    store.recover_object("k")  # v3=b'first'
    store.put_object("k", b"second")  # v4
    store.delete_object("k")  # v5 墓碑
    now[0] += 5
    v6 = store.recover_object("k")
    assert v6.data == b"second"
    assert [v.version_id for v in store.list_versions("k")] == [1, 2, 3, 4, 5, 6]
    assert store.get_object("k").data == b"second"


# ---------- 重复删除与幂等隔离 ----------

def test_duplicate_delete_conclusions_are_stable():
    store = ObjectStore(recovery=RecoveryPolicy(recover_seconds=30))
    log("重复删除结论一致", "带幂等键删除两次；再用另一个幂等键/无键重复删除",
        "同一笔提交两次返回同一墓碑；已删除后新的删除尝试一律 object_not_found，"
        "墓碑只有一个、容量不重复扣减")
    store.put_object("k", b"data")
    d1 = store.delete_object("k", idempotency_key="d1")
    d2 = store.delete_object("k", idempotency_key="d1")
    assert d1 is d2
    assert len(store.list_versions("k")) == 2  # v1 + 单个墓碑
    assert store.used_bytes == 0
    with pytest.raises(StorageRejected) as ei:
        store.delete_object("k")  # 无幂等键的重复删除
    assert ei.value.reason == RejectReason.OBJECT_NOT_FOUND
    with pytest.raises(StorageRejected) as ei2:
        store.delete_object("k", idempotency_key="d-other")  # 另一笔提交
    assert ei2.value.reason == RejectReason.OBJECT_NOT_FOUND
    assert len(store.list_versions("k")) == 2


def test_idempotency_key_isolated_between_objects():
    store = ObjectStore(recovery=RecoveryPolicy(recover_seconds=30))
    log("重试标识跨对象隔离", "key=a 与 key=b 复用同一 idempotency_key='same'",
        "两边各自独立提交、各自产生自己的版本，互不串味、不互相重放")
    va = store.put_object("a", b"aaa", idempotency_key="same")
    vb = store.put_object("b", b"bbb", idempotency_key="same")
    assert va is not vb and va.key == "a" and vb.key == "b"
    assert store.get_object("a").data == b"aaa"
    assert store.get_object("b").data == b"bbb"
    # 删除/回滚/恢复同样隔离
    ta = store.delete_object("a", idempotency_key="op")
    tb = store.delete_object("b", idempotency_key="op")
    assert ta is not tb and ta.key == "a" and tb.key == "b"
    ra = store.recover_object("a", idempotency_key="op2")
    rb = store.recover_object("b", idempotency_key="op2")
    assert ra is not rb
    assert store.get_object("a").data == b"aaa"
    assert store.get_object("b").data == b"bbb"
    # 同一键在同一对象内：两次重试之间对象已被改动（删除+恢复）时，
    # 旧结论与当前状态对不上，按新一笔重新执行（追加新版本、内容不变）；
    # 重新执行后状态稳定，再次重试才重放该次结论
    v_re = store.put_object("a", b"aaa", idempotency_key="same")
    assert v_re is not va and v_re.data == b"aaa"
    assert store.get_object("a").data == b"aaa"
    assert store.put_object("a", b"aaa", idempotency_key="same") is v_re


def test_idempotency_key_isolated_between_uploads():
    store = ObjectStore()
    log("重试标识跨上传会话隔离", "key=a 与 key=b 用同一键创建上传并完成",
        "得到两个不同会话与两个对象版本，不互相返回对方结果")

    def finish(key: str) -> str:
        up = store.create_upload(key, part_count=1, idempotency_key="u")
        store.upload_part(up.upload_id, 1, b"x", sha(b"x"))
        store.complete_upload(up.upload_id, idempotency_key="c")
        return up.upload_id

    id_a = finish("a")
    id_b = finish("b")
    assert id_a != id_b
    assert store.get_object("a").data == b"x"
    assert store.get_object("b").data == b"x"
    assert store.used_bytes == 2


def test_same_key_different_operation_conflicts():
    store = ObjectStore(recovery=RecoveryPolicy(recover_seconds=30))
    log("同一对象同一幂等键换操作", "'k1' 先用于 write，再用于 rollback",
        "必须以 idempotency_conflict 明确拒绝，避免错误重放成别的操作")
    store.put_object("k", b"one", idempotency_key="k1")
    store.put_object("k", b"two")
    with pytest.raises(StorageRejected) as ei:
        store.rollback_object("k", 1, idempotency_key="k1")
    assert ei.value.reason == RejectReason.IDEMPOTENCY_CONFLICT


# ---------- 并发 ----------

def test_concurrent_rollback_delete_write_no_pollution():
    store = ObjectStore(
        recovery=RecoveryPolicy(recover_seconds=3600),
        clock=lambda: 1000.0,  # 固定时钟：保留期 0，恢复窗口始终有效
    )
    log("并发回滚/删除/恢复/写入", "8 个写线程 + 4 个回滚线程 + 2 个删除/恢复线程",
        "所有操作串行化；每次读取都等于某一完整版本（校验和自洽）；"
        "版本链无空洞、不重复；最终状态确定一致")
    store.put_object("k", b"v0")
    n_writers = 8
    barrier = threading.Barrier(n_writers + 6)
    errors: list[str] = []

    def writer(i: int) -> None:
        barrier.wait()
        try:
            store.put_object("k", f"w{i}".encode())
        except StorageRejected as exc:  # 撞上删除状态时允许失败
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

    threads = (
        [threading.Thread(target=writer, args=(i,)) for i in range(n_writers)]
        + [threading.Thread(target=roller, args=(i,)) for i in range(4)]
        + [threading.Thread(target=deleter), threading.Thread(target=recoverer)]
    )
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 版本链单调、无空洞；每个存活版本内容与校验和自洽
    versions = store.list_versions("k")
    assert [v.version_id for v in versions] == list(range(1, len(versions) + 1))
    for v in versions:
        if not v.deleted:
            assert v.checksum == sha(v.data)
            assert v.size == len(v.data)
    # 已用量统计必须与版本账实相符
    live = versions[-1] if versions and not versions[-1].deleted else None
    assert store.used_bytes == (live.size if live else 0)
    # 当前读取必然是某一完整版本
    try:
        cur = store.get_object("k")
        assert cur.checksum == sha(cur.data)
    except StorageRejected as exc:
        assert exc.reason == RejectReason.OBJECT_NOT_FOUND
    assert errors == []
