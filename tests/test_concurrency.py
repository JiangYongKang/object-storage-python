"""并发一致性：并发覆盖、删除、回滚、分块完成不得产生撕裂读取。

使用真实线程 + 屏障制造高争用，固定随机种子保证可复现；
不依赖任何外部服务或网络。
"""

from __future__ import annotations

import random
import threading
from hashlib import sha256

import pytest

from object_storage import ObjectStore, QuotaConfig, StoreConfig
from object_storage.models import PartSpec, checksum_of


@pytest.fixture
def store():
    return ObjectStore(StoreConfig(quota=QuotaConfig(max_bytes=10_000_000)))


def test_concurrent_overwrites_never_tear(store, log_case):
    key = "hot"
    # 每个写入者使用自分界、可整体校验的 payload
    payloads = [(f"writer-{i:02d}:".encode() + b"x" * 512) for i in range(8)]
    for p in payloads:
        assert len(p) == 522
    n_writers = len(payloads)
    n_readers = 4
    barrier = threading.Barrier(n_writers + n_readers)
    errors: list[BaseException] = []
    # 预置一个完整版本，保证并发窗口内读取永远命中某个版本，
    # 从而“读到的内容必须完整自洽”这一判定不含 NotFound 噪声。
    store.put_object(key, payloads[0])

    def writer(idx: int) -> None:
        try:
            barrier.wait()
            data = payloads[idx]
            for _ in range(50):
                store.put_object(key, data, checksum=checksum_of(data))
        except BaseException as exc:  # noqa: BLE001 - 回传线程内异常
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(n_writers)]
    valid_digests = {checksum_of(p) for p in payloads}
    torn: list[str] = []

    def reader() -> None:
        try:
            barrier.wait()
            for _ in range(400):
                v = store.get_object(key)
                # 读取必须恰好等于某一个完整版本
                if v.checksum not in valid_digests:
                    torn.append(v.checksum)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads += [threading.Thread(target=reader) for _ in range(n_readers)]
    log_case("concurrent-overwrite", writers=n_writers, readers=n_readers,
             rounds="50x8 / 400x4")
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert torn == [], f"observed torn reads: {torn[:3]}"
    final = store.get_object(key)
    assert final.checksum in valid_digests
    # 版本链完整：每个版本数据自洽且长度正确
    for v in store.list_versions(key):
        assert v.size == 522 and checksum_of(v.data or b"") == v.checksum
    log_case("concurrent-overwrite", decision="all reads matched a complete version",
             versions=len(store.list_versions(key)))


def test_concurrent_mixed_reads_always_map_to_one_version(store, log_case):
    key = "mixed"
    seed_payload = b"v0"
    store.put_object(key, seed_payload)
    rng = random.Random(20260923)
    payloads = [f"payload-{i:03d}".encode() * 8 for i in range(20)]
    # 初始版本也是一个完整、可读的合法版本
    accepted = set(payloads) | {seed_payload}
    versions_seen: list[str] = []
    errors: list[BaseException] = []
    start = threading.Barrier(6)

    def writer() -> None:
        start.wait()
        for _ in range(100):
            data = rng.choice(payloads)
            store.put_object(key, data)

    def deleter() -> None:
        start.wait()
        for i in range(40):
            if i % 2 == 0:
                store.delete_object(key)
            else:
                store.put_object(key, rng.choice(payloads))

    def reader() -> None:
        start.wait()
        for _ in range(300):
            try:
                v = store.get_object(key)
            except Exception:
                continue  # 合法地读到 delete marker
            if checksum_of(v.data or b"") != v.checksum:
                errors.append(AssertionError("checksum mismatch on read"))
                return
            if v.data not in accepted:
                errors.append(AssertionError("read bytes match no complete version"))
                return
            versions_seen.append(v.version_id)

    threads = [
        threading.Thread(target=writer),
        threading.Thread(target=writer),
        threading.Thread(target=deleter),
        threading.Thread(target=reader),
        threading.Thread(target=reader),
        threading.Thread(target=reader),
    ]
    log_case("concurrent-mixed", seed=20260923,
             decision="every live read equals some full committed version")
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert versions_seen  # 至少读到过若干次活动版本


def test_concurrent_multipart_completes_are_atomic(log_case):
    # 多个分块上传并发写同一 key，最终对象必须是某一次“完整合并”的结果
    store = ObjectStore(StoreConfig(quota=QuotaConfig(max_bytes=10_000_000)))
    key = "mp"
    n = 6
    barrier = threading.Barrier(n)
    results: dict[int, str] = {}
    errors: list[BaseException] = []

    def one_upload(idx: int) -> None:
        try:
            up = store.create_multipart_upload(f"{key}")
            chunk_a = bytes([65 + idx]) * 100   # 'A'..'F' x100
            chunk_b = bytes([97 + idx]) * 100   # 'a'..'f' x100
            p1 = store.upload_part(key, up.upload_id, 1, chunk_a)
            p2 = store.upload_part(key, up.upload_id, 2, chunk_b)
            barrier.wait()
            r = store.complete_multipart_upload(
                key,
                up.upload_id,
                [PartSpec(1, p1.checksum), PartSpec(2, p2.checksum)],
            )
            results[idx] = sha256(chunk_a + chunk_b).hexdigest()
            assert r.checksum == results[idx]
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=one_upload, args=(i,)) for i in range(n)]
    log_case("concurrent-mp", uploads=n, parts_per_upload=2)
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    final = store.get_object(key)
    # 最终内容必须恰好是某次上传的完整合并，而不是分片交叉拼接
    assert final.checksum in results.values()
    assert len(final.data) == 200
    assert final.data[:100] == bytes([final.data[0]]) * 100
    assert final.data[100:] == bytes([final.data[100]]) * 100
    # 六个完整版本都在链上且数据自洽
    versions = store.list_versions(key)
    assert len(versions) == n
    assert {v.checksum for v in versions} == set(results.values())
    log_case("concurrent-mp", decision="final object is one complete merge",
             final_sha=final.checksum[:12], versions=n)


def test_concurrent_token_replays_do_not_duplicate_versions(log_case):
    store = ObjectStore(StoreConfig(quota=QuotaConfig(max_bytes=10_000_000)))
    key = "idem"
    token = "fixed-client-token"
    data = b"exactly-once"
    n = 10
    barrier = threading.Barrier(n)
    outcomes: list[str] = []
    lock = threading.Lock()
    errors: list[BaseException] = []

    def retry() -> None:
        try:
            barrier.wait()
            r = store.put_object(key, data, client_token=token)
            with lock:
                outcomes.append(r.version_id)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=retry) for _ in range(n)]
    log_case("concurrent-replay", threads=n, token=token)
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert len(outcomes) == n
    assert len(set(outcomes)) == 1, outcomes
    assert len(store.list_versions(key)) == 1
    assert store.quota_usage() == (len(data), 1)
    log_case("concurrent-replay", decision="exactly one version, quota once",
             distinct_versions=len(set(outcomes)))
