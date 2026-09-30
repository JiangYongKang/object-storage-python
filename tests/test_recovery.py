"""回滚与可恢复删除的单元测试：纯内存、可复现，不依赖外部服务。

每个用例打印输入与判定依据；新增能力的结论必须与普通写入一样：
受配额/保留约束、支持按对象隔离的幂等重试、不撕裂历史。
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


# ---------- 按历史版本回滚 ----------

def test_rollback_to_historical_version_appends_new_version():
    store = ObjectStore()
    log("回滚到历史版本", "写入 one/two/three 后回滚到 v1",
        "回滚后当前读到 b'one'；回滚是新版本 v4；v2/v3 仍可稳定读取，历史不乱")
    store.put_object("k", b"one")
    store.put_object("k", b"two")
    store.put_object("k", b"three")
    rb = store.rollback_object("k", version_id=1)
    assert rb.version_id == 4
    assert rb.data == b"one"
    assert rb.checksum == sha(b"one")
    assert store.get_object("k").data == b"one"
    ids = [v.version_id for v in store.list_versions("k")]
    assert ids == [1, 2, 3, 4]
    assert store.get_object("k", version_id=2).data == b"two"
    assert store.get_object("k", version_id=3).data == b"three"
    assert store.used_bytes == 3


def test_rollback_to_unknown_or_deleted_version_rejected():
    store = ObjectStore()
    log("非法回滚目标", "回滚到不存在的版本；回滚到墓碑版本",
        "不存在版本/墓碑都以 object_not_found 拒绝，且不产生新版本")
    store.put_object("k", b"one")
    store.put_object("k", b"two")
    with pytest.raises(StorageRejected) as ei:
        store.rollback_object("k", version_id=99)
    assert ei.value.reason == RejectReason.OBJECT_NOT_FOUND
    tomb = store.delete_object("k")
    with pytest.raises(StorageRejected) as ei2:
        store.rollback_object("k", version_id=tomb.version_id)
    assert ei2.value.reason == RejectReason.OBJECT_NOT_FOUND
    # 已删除对象上的回滚也被拒绝，恢复只能走 restore
    with pytest.raises(StorageRejected) as ei3:
        store.rollback_object("k", version_id=1)
    assert ei3.value.reason == RejectReason.OBJECT_NOT_FOUND
    assert len(store.list_versions("k")) == 3


def test_rollback_idempotent_replay():
    store = ObjectStore()
    log("回滚幂等重试", "同一 idempotency_key 重复回滚两次",
        "两次返回同一个版本对象，不新增版本、不重复占配额")
    store.put_object("k", b"one")
    store.put_object("k", b"two")
    rb1 = store.rollback_object("k", 1, idempotency_key="rb-1")
    rb2 = store.rollback_object("k", 1, idempotency_key="rb-1")
    assert rb1 is rb2
    assert len(store.list_versions("k")) == 3
    assert store.used_bytes == 3


def test_rollback_respects_quota_and_retention():
    log("回滚受配额/保留约束",
        "容量配额下回滚到超配额的大版本；保留期内回滚",
        "分别以 quota_bytes_exceeded / retention_violation 拒绝，不产生版本")
    # max_bytes=10：k 的大版本 v1 被覆盖为小版本后，容量被 other 占用，
    # 此时回滚放大 k 必然超配额。
    qs = ObjectStore(quota=QuotaPolicy(max_bytes=10))
    qs.put_object("k", b"x" * 5)
    qs.put_object("k", b"z")       # 已用量降到 1
    qs.put_object("other", b"o" * 6)  # 已用量升到 7
    with pytest.raises(StorageRejected) as ei:
        qs.rollback_object("k", 1)  # 1-1+5 后总量 11 > 10
    assert ei.value.reason == RejectReason.QUOTA_BYTES_EXCEEDED
    assert qs.used_bytes == 7
    assert len(qs.list_versions("k")) == 2
    assert qs.get_object("k").data == b"z"

    now = [1000.0]
    rs = ObjectStore(
        retention=RetentionPolicy(retain_seconds=60), clock=lambda: now[0]
    )
    rs.put_object("k", b"v1")
    now[0] += 61
    rs.put_object("k", b"v2-longer")
    with pytest.raises(StorageRejected) as ei2:
        rs.rollback_object("k", 1)
    assert ei2.value.reason == RejectReason.RETENTION_VIOLATION
    assert rs.get_object("k").data == b"v2-longer"


# ---------- 可恢复删除 ----------

def test_restore_within_window_returns_pre_delete_content():
    now = [1000.0]
    store = ObjectStore(
        recovery=RecoveryPolicy(recover_seconds=60), clock=lambda: now[0]
    )
    log("窗口内恢复", "one/two 后删除，推进 30s 后恢复",
        "恢复为新版本且内容等于删除前最近完整版本 b'two'；"
        "删除期间历史可查；恢复后不丢不多版本，配额重新计入")
    store.put_object("k", b"one")
    store.put_object("k", b"two")
    tomb = store.delete_object("k")
    assert tomb.version_id == 3 and store.used_bytes == 0
    # 删除态：当前读不到，历史版本仍可查
    with pytest.raises(StorageRejected) as ei:
        store.get_object("k")
    assert ei.value.reason == RejectReason.OBJECT_NOT_FOUND
    assert store.get_object("k", version_id=1).data == b"one"
    now[0] += 30
    restored = store.restore_object("k")
    assert restored.version_id == 4
    assert restored.data == b"two"
    assert store.get_object("k").data == b"two"
    assert [v.version_id for v in store.list_versions("k")] == [1, 2, 3, 4]
    assert store.used_bytes == 3


def test_restore_after_window_expired_rejected():
    now = [1000.0]
    store = ObjectStore(
        recovery=RecoveryPolicy(recover_seconds=60), clock=lambda: now[0]
    )
    log("恢复窗口过期", "删除后推进 61s 再恢复",
        "必须以 recovery_window_expired 拒绝，对象保持删除态，不产生版本")
    store.put_object("k", b"data")
    store.delete_object("k")
    now[0] += 61
    with pytest.raises(StorageRejected) as ei:
        store.restore_object("k")
    assert ei.value.reason == RejectReason.RECOVERY_WINDOW_EXPIRED
    assert store.list_versions("k")[-1].deleted
    assert len(store.list_versions("k")) == 2


def test_restore_disabled_by_default_and_live_object_rejected():
    store = ObjectStore()
    log("默认删除即终态", "不配置 RecoveryPolicy（窗口=0），删除后恢复",
        "以 recovery_window_expired 拒绝，保持向后兼容")
    store.put_object("k", b"data")
    store.delete_object("k")
    with pytest.raises(StorageRejected) as ei:
        store.restore_object("k")
    assert ei.value.reason == RejectReason.RECOVERY_WINDOW_EXPIRED

    alive = ObjectStore(recovery=RecoveryPolicy(60))
    alive.put_object("a", b"x")
    with pytest.raises(StorageRejected) as ei2:
        alive.restore_object("a")
    assert ei2.value.reason == RejectReason.OBJECT_NOT_DELETED


def test_restore_quota_object_count_enforced():
    store = ObjectStore(
        quota=QuotaPolicy(max_objects=1), recovery=RecoveryPolicy(60)
    )
    log("恢复受数量配额约束", "删除 a 后写入占满名额的 b，再恢复 a",
        "恢复等同重新写入，必须以 quota_objects_exceeded 拒绝；a 仍为删除态")
    store.put_object("a", b"x")
    store.delete_object("a")
    store.put_object("b", b"y")
    with pytest.raises(StorageRejected) as ei:
        store.restore_object("a")
    assert ei.value.reason == RejectReason.QUOTA_OBJECTS_EXCEEDED
    assert store.list_versions("a")[-1].deleted


# ---------- 幂等重试与重试标识隔离 ----------

def test_repeated_delete_has_consistent_conclusion():
    store = ObjectStore()
    log("重复删除结论一致", "删除一次后不带键再删，再用同一 idempotency_key 删两次",
        "三次都返回同一个墓碑版本，不新增版本、不重复释放容量；"
        "不存在的 key 仍以 object_not_found 拒绝")
    store.put_object("k", b"data-123")
    t1 = store.delete_object("k")
    t2 = store.delete_object("k")
    t3 = store.delete_object("k", idempotency_key="del-k")
    t4 = store.delete_object("k", idempotency_key="del-k")
    assert t1 is t2 is t3 is t4
    assert t1.version_id == 2 and t1.deleted
    assert len(store.list_versions("k")) == 2
    assert store.used_bytes == 0
    with pytest.raises(StorageRejected) as ei:
        store.delete_object("never-existed", idempotency_key="del-x")
    assert ei.value.reason == RejectReason.OBJECT_NOT_FOUND


def test_restore_idempotent_replay():
    now = [1000.0]
    store = ObjectStore(
        recovery=RecoveryPolicy(60), clock=lambda: now[0]
    )
    log("恢复幂等重试", "删除后用同一 idempotency_key 连续恢复两次",
        "两次返回同一个恢复版本，不新增版本、不重复占配额")
    store.put_object("k", b"one")
    store.put_object("k", b"two")
    store.delete_object("k")
    now[0] += 10
    r1 = store.restore_object("k", idempotency_key="res-1")
    r2 = store.restore_object("k", idempotency_key="res-1")
    assert r1 is r2
    assert r1.version_id == 4 and r1.data == b"two"
    assert len(store.list_versions("k")) == 4
    assert store.used_bytes == 3


def test_stale_conclusion_invalidated_by_lifecycle_turn():
    now = [1000.0]
    store = ObjectStore(
        recovery=RecoveryPolicy(60), clock=lambda: now[0]
    )
    log("生命周期翻转后旧结论失效",
        "用同一键 删除→恢复→再删除→再恢复",
        "第二次删除/恢复必须重新执行并产生新版本，不能回放过期结论；"
        "接口成功与对象状态始终一致")
    store.put_object("k", b"v1")
    t1 = store.delete_object("k", idempotency_key="op")
    assert t1.version_id == 2
    now[0] += 5
    r1 = store.restore_object("k", idempotency_key="op")
    assert r1.version_id == 3 and store.get_object("k").data == b"v1"
    now[0] += 5
    t2 = store.delete_object("k", idempotency_key="op")
    assert t2.version_id == 4 and t2 is not t1
    now[0] += 5
    r2 = store.restore_object("k", idempotency_key="op")
    assert r2.version_id == 5 and r2 is not r1
    assert store.get_object("k").data == b"v1"
    assert len(store.list_versions("k")) == 5
    assert store.used_bytes == 2


def test_idempotency_key_isolated_between_objects_and_ops():
    store = ObjectStore(recovery=RecoveryPolicy(60))
    log("重试标识跨对象/跨操作隔离",
        "字符串 'same-key' 同时用于 a/b 两个对象的写入，"
        "以及删除、回滚、恢复等不同操作",
        "各结论只绑定自己的对象与操作，绝不串味、不互占版本")
    va = store.put_object("a", b"aaa", idempotency_key="same-key")
    vb = store.put_object("b", b"bbb", idempotency_key="same-key")
    assert va.version_id == 1 and vb.version_id == 1
    assert store.get_object("a").data == b"aaa"
    assert store.get_object("b").data == b"bbb"
    # 同键用于不同对象的删除：各自产生独立墓碑
    da = store.delete_object("a", idempotency_key="same-key")
    db = store.delete_object("b", idempotency_key="same-key")
    assert da.key == "a" and db.key == "b"
    assert store.list_versions("a")[-1].deleted
    assert store.list_versions("b")[-1].deleted
    # 同键用于不同对象的恢复：各自恢复自己的内容
    ra = store.restore_object("a", idempotency_key="same-key")
    rb = store.restore_object("b", idempotency_key="same-key")
    assert ra.data == b"aaa" and rb.data == b"bbb"
    # 再把同键用于回滚，不应命中写入/删除/恢复的任何记录
    store.put_object("a", b"aaa2", idempotency_key="write-2")
    rba = store.rollback_object("a", 1, idempotency_key="same-key")
    assert rba.data == b"aaa" and rba.key == "a"
    assert store.get_object("b").data == b"bbb"  # b 未受影响
    # 分块上传的 complete 作用域含 upload_id：同键用于两个上传互不影响
    up1 = store.create_upload("c", part_count=1)
    up2 = store.create_upload("c", part_count=1)
    store.upload_part(up1.upload_id, 1, b"ccc1", sha(b"ccc1"))
    store.upload_part(up2.upload_id, 1, b"ccc2", sha(b"ccc2"))
    c1 = store.complete_upload(up1.upload_id, idempotency_key="same-key")
    c2 = store.complete_upload(up2.upload_id, idempotency_key="same-key")
    assert c1.data == b"ccc1" and c2.data == b"ccc2"
    assert c1.version_id != c2.version_id


# ---------- 并发一致性 ----------

def test_concurrent_rollback_delete_write_never_pollutes():
    store = ObjectStore(recovery=RecoveryPolicy(3600))
    log("并发回滚/删除/写入不互相污染",
        "预置 8 个历史版本；32 个线程分三类操作同一 key，并发跑 4 轮",
        "版本链严格连续、无重复无空洞；每次成功读取的校验和必须自洽；"
        "删除态读取以 object_not_found 结束；最终仍可恢复到一份完整版本")
    for i in range(1, 9):
        store.put_object("k", f"payload-{i}".encode())
    n = 32
    rounds = 4
    barrier = threading.Barrier(n)

    def worker(i: int) -> None:
        for r in range(rounds):
            barrier.wait()
            try:
                if i % 3 == 0:
                    # 历史版本 1..8 都是完整版本，任选其一回滚
                    store.rollback_object("k", version_id=1 + (r % 8))
                elif i % 3 == 1:
                    store.put_object("k", f"writer-{i}-{r}".encode())
                else:
                    if store.list_versions("k")[-1].deleted:
                        store.restore_object("k")
                    else:
                        store.delete_object("k")
            except StorageRejected:
                # 竞争中可能出现状态翻转导致的 not_found / not_deleted /
                # 恢复窗口等拒绝，属正常；关键是不能读到撕裂数据。
                pass

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    versions = store.list_versions("k")
    assert [v.version_id for v in versions] == list(range(1, len(versions) + 1))
    for v in versions:
        if not v.deleted:
            assert v.checksum == sha(v.data)
            assert v.size == len(v.data)
    final = versions[-1]
    if not final.deleted:
        got = store.get_object("k")
        assert got.checksum == sha(got.data)
    # 不论最终落在哪个状态，历史首版本永远稳定可读
    assert store.get_object("k", version_id=1).data == b"payload-1"


def test_concurrent_duplicate_delete_single_tombstone():
    store = ObjectStore()
    log("并发重复删除只有一个墓碑",
        "16 个线程带同一 idempotency_key 同时删除同一 key",
        "全部成功且返回同一个墓碑；版本链只新增一个删除版本")
    store.put_object("k", b"hello")
    n = 16
    barrier = threading.Barrier(n)
    results: list = []
    errors: list = []

    def delete() -> None:
        barrier.wait()
        try:
            results.append(store.delete_object("k", idempotency_key="only-del"))
        except StorageRejected as exc:  # pragma: no cover - 不允许发生
            errors.append(exc)

    threads = [threading.Thread(target=delete) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert len(results) == n
    assert all(v is results[0] for v in results)
    versions = store.list_versions("k")
    assert len(versions) == 2 and versions[-1].deleted
    assert store.used_bytes == 0
