# 设计决策与拒绝原因速查

本文件记录本地对象存储的关键设计取舍，供审查与后续演进参考。

## 1. 单一判定中心

所有业务规则只存在于 `ObjectStore`（`store.py`）：

- `ObjectStorageService`（Python 门面）只加日志并透传，不做决策；
- FastAPI 路由只做序列化、错误码映射，不做决策。

两个入口共享同一个 `ObjectStore` 实例，因此“同一输入 → 同一结论”，
不会出现入口间语义漂移。

## 2. 并发模型：单把可重入锁 + 原子追加

- 所有改变状态的操作在 `RLock` 内完成；
- 一次提交 = 构造不可变版本 → 追加链尾 → 切换链顶，全部在临界区内；
- 读取也在同一把锁内取快照引用，返回不可变 `ObjectVersion`。

因此并发写入不会互相污染，读取只会对应某个完整版本。

## 3. 版本可见性

| 当前链顶 | 普通 get | 指定 version get |
|---|---|---|
| 普通版本 | 返回该版本 | 返回该历史版本 |
| delete marker | 404 not_found | marker 版本也 404（marker 非对象） |
| key 不存在 | 404 not_found | 404 no_such_version |

回滚 = “复制历史内容为新版本追加”，不是指针回退；这样回滚本身
也服从统一的覆盖/删除/配额/保留规则，语义只有一套。

## 4. 删除与回滚的幂等细节

- 对不存在/已删除 key 的 delete：成功、`replayed=True`、不新增 marker；
- rollback 到 delete marker：`no_such_version`（没有可恢复的内容）；
- rollback 重放必须指向同一目标版本，否则 `idempotency_conflict`。

## 5. 分块会话生命周期

- 会话仅存于进行中的内存表 `(key, upload_id) -> upload`；
- `complete` 成功 / `abort` 后立即从进行中表移除，并在该 key 的
  `terminal` 记录终态（completed/aborted），用于对“迟到的后续操作”
  返回**可区分且确定**的原因；
- 合并采用两阶段判定：先纯校验（清单、分片存在性、各级校验和），
  全部通过后才检查保留与配额、再提交；任何前置失败都不产生副作用，
  会话保持可用，客户端可修正后重试；
- 合并顺序只由分片号决定，清单乱序不影响结果。

## 6. 幂等键的作用域与生命周期

- 幂等记录按 key 隔离，存“首次成功结果”；失败不占用幂等记录，允许重试；
- token 绑定操作类型（put/delete/rollback/complete/create-upload）；
- create token 代表一次逻辑上传，会话终结后仍保留映射，使迟到的 create
  重放得到确定终态；需要新上传请用新 token；
- complete 重放不依赖会话仍存在（完成时会话已移除），直接回放首次结果。

## 7. 配额口径

- 只对“当前可见版本”计费，历史版本与 marker 不计费；
- 覆盖按增量 `new.size - old.size`，新 key 才占一个对象计数；
- 分块仅在完成时计费；
- 判定在提交前完成，拒绝绝不改变用量。

这一口径牺牲了“历史版本也计费”的精细度，换取确定、可解释、易复算的规则。

## 8. 保留策略

- `retain_until` 为绝对时刻，基于可注入时钟，测试可精确控制；
- 保留期内禁止任何“取代当前版本”的操作，统一返回 423，消息含剩余秒数；
- 不阻止读取，也不阻止对其他 key 的操作。

## 9. 拒绝原因全表

| code | 含义 | HTTP |
|---|---|---|
| `not_found` | key/当前版本不存在 | 404 |
| `no_such_upload` | 上传会话不存在 | 404 |
| `no_such_version` | 指定版本不存在/不可作为回滚目标 | 404 |
| `upload_aborted` | 会话已中止 | 409 |
| `upload_already_completed` | 会话已完成 | 409 |
| `part_missing` | 清单空或声明分片缺失 | 400 |
| `part_duplicate` | 清单分片号重复 | 400 |
| `invalid_part` | 分片号越界/空负载 | 400 |
| `checksum_mismatch` | 分片或整体 sha256 不符 | 422 |
| `quota_exceeded` | 超容量/数量 | 413 |
| `retention_policy_violation` | 保留期内禁止取代 | 423 |
| `idempotency_conflict` | token 复用冲突/重放载荷不一致 | 409 |
| `invalid_base64`（仅 HTTP） | 请求体 base64 非法 | 400 |
