# 本地对象存储（object-storage-python）

在 FastAPI 骨架之上实现的本地对象存储能力：分块上传、对象版本与并发一致性、
配额与保留约束，以及**按历史版本回滚**与**可恢复删除**。所有拒绝均携带可区分的
`reason` 码，各入口（存储引擎 / HTTP API）给出一致且可解释的结果。纯内存实现，
不依赖任何外部服务或网络。

## 存储模型

- **对象（Object）**：以 `key` 标识，内容由一串单调递增的**版本（ObjectVersion）**构成。
  版本不可变，包含 `version_id`、`data`、`checksum`（内容 sha256）、`size`、`created_at`。
- **删除即墓碑（tombstone）**：删除追加一个 `deleted=True` 的版本，历史版本保留可查，
  当前版本为墓碑时读取返回 `object_not_found`。
- **分块上传会话（MultipartUpload）**：`create_upload → upload_part* → complete_upload`
  或 `abort_upload`。会话状态机为 `active → completed | aborted`。
  **未完成或已中止的会话不产生任何可见对象，也不占用配额**；完成/中止时分片数据即被清理。
- **配额（QuotaPolicy）**：`max_bytes`（存活对象总字节）与 `max_objects`（存活对象数），
  `None` 表示不限制。覆盖同 key 只按差额计量，删除立即释放容量。
- **保留（RetentionPolicy）**：`retain_seconds` 内当前版本禁止被覆盖、删除或回滚。
- **删除恢复（RecoveryPolicy）**：`recover_seconds` 为删除后的恢复窗口。
  `<=0`（默认）表示删除即终态、不可恢复，**与旧版本行为完全兼容**。

## 回滚与可恢复删除

两条能力都以「追加一个新版本」的方式生效，中间历史一律不抹除、不乱序，
恢复/回滚完成后读到的就是目标那份完整数据，并作为一次新的可见变更计入版本链。

### 按历史版本回滚：`rollback_object(key, version_id)`

- 目标必须是该 key 上**曾经存在过的完整版本**（不能是墓碑、不能不存在）。
- 仅对**当前存活**的对象生效；对象处于删除态时，回滚以 `object_not_found` 拒绝，
  窗口内请改用恢复。回滚后追加一份内容与目标版本逐字节相同的新版本（新 `version_id`、
  新 `created_at`），历史上的每个版本之后仍可按 `version_id` 稳定读出。
- 与普通写入一致，受**保留策略**（当前版本仍在保留期则拒绝）和**配额**
  （按目标版本大小重新计量）约束。

### 可恢复删除：`delete_object` + `restore_object(key)`

- 删除后对象进入可恢复的删除态：当前读返回 `object_not_found`，但**历史版本依然可查**。
- 恢复窗口内 `restore_object` 把对象恢复为**删除前最近的一份完整版本**，
  以新版本形式追加；窗口外（含默认 `recover_seconds=0`）以
  `recovery_window_expired` 明确拒绝，对象保持删除态。
- 对存活对象调用恢复以 `object_not_deleted` 拒绝。
- 恢复等同于一次重新写入：删除时已释放的容量/名额会**重新计入配额**
  （容量按目标大小、对象数按新增存活计），配额不足则拒绝。
- 恢复不受保留策略拦截（墓碑没有“存活年龄”）；恢复出的新版本从恢复时刻起
  重新获得完整保留窗口。
- 窗口只约束“能否恢复”，**不物理清理历史**；窗口过后历史版本仍可按 `version_id` 查询。

### 重复删除与幂等重试

- **重复删除结论一致**：对已处于删除态的对象再次删除，直接返回同一个墓碑，
  不新增版本、不重复释放容量（不存在的 key 仍返回 `object_not_found`）。
- 回滚、删除、恢复、写入、合并完成均可携带 `idempotency_key`；同一笔提交的重复调用
  永远返回首次结论，不重复占配额、不多出版本，也不会出现“接口成功但对象没变化”。
- 重试结论的有效期与对象状态绑定：
  - 回滚/恢复：自首次提交后对象又发生过新的提交（写入、再次回滚、或删除↔恢复翻转），
    旧结论作废，重试按当前状态重新执行并产生新版本；
  - 删除：只要对象当前仍是删除态，首次墓碑就是确定结论（即使中间经历过
    删除→恢复→再删除，重试也不会再制造墓碑）。
- **作用域隔离**：幂等记录按「操作类型 + 对象 key（分块合并还含 upload_id）+ 重试标识」
  建立复合索引。同一字符串用在不同对象、不同上传或不同操作之间绝不串味。

## 版本与一致性规则

- 所有状态变更在同一把可重入锁内完成：并发写入/覆盖/删除/回滚/恢复相互串行化，
  **读取永远对应某一完整版本，绝不撕裂**（每个版本自带校验和可自证）。
- 覆盖、删除、回滚、恢复均以追加新版本的方式生效，可见性确定：提交成功即对后续读取可见；
  显式指定 `version_id` 可稳定读取任一历史完整版本。
- **幂等**：写入/合并/回滚/删除/恢复可携带 `idempotency_key`，按上述作用域与状态规则
  返回首次结论；同一幂等键重复 `create_upload` 返回同一会话。

## 拒绝原因（reason 码）

| reason | 含义 |
| --- | --- |
| `upload_not_found` | 上传会话不存在 |
| `upload_aborted` | 上传已中止，拒绝后续操作 |
| `upload_completed` | 上传已完成，拒绝续传/中止 |
| `part_missing` | 完成合并时缺少分片（消息中列出缺失编号） |
| `part_duplicate` | 同一分片号重复上传 |
| `checksum_mismatch` | 分片校验和不匹配 |
| `part_out_of_range` | 分片编号越界 |
| `quota_bytes_exceeded` | 超出容量配额 |
| `quota_objects_exceeded` | 超出对象数量配额 |
| `retention_violation` | 保留期内禁止覆盖/删除/回滚 |
| `recovery_window_expired` | 恢复窗口已过（或未配置窗口），无法恢复 |
| `object_not_deleted` | 对象当前存活，无需恢复 |
| `object_not_found` | 对象不存在、当前版本为删除标记，或回滚目标版本不可用 |

HTTP 入口下，`object_not_found` / `upload_not_found` 返回 404，其余（含
`recovery_window_expired`、`object_not_deleted` 等状态冲突）返回 409，
响应体统一为 `{"error": {"reason": ..., "message": ...}}`。

## HTTP API 摘要

| 方法与路径 | 说明 |
| --- | --- |
| `PUT /objects/{key}` | 直接写入（body: `data_b64`, 可选 `idempotency_key`） |
| `GET /objects/{key}?version_id=` | 读取当前或指定版本 |
| `DELETE /objects/{key}` | 删除（body 可选 `idempotency_key`；重复删除结论一致，追加墓碑版本） |
| `POST /objects/{key}/rollback` | 回滚到历史完整版本（body: `version_id`, 可选 `idempotency_key`） |
| `POST /objects/{key}/restore` | 窗口内恢复删除（body 可选 `idempotency_key`） |
| `GET /objects/{key}/versions` | 列出全部版本；墓碑版本在配置了窗口时附 `restorable_until` |
| `POST /uploads/{key}` | 创建分块上传（`part_count`, 可选 `idempotency_key`） |
| `PUT /uploads/{upload_id}/parts/{n}` | 上传分片（`data_b64` + `checksum`） |
| `POST /uploads/{upload_id}/complete` | 完成合并（可选 `idempotency_key`） |
| `POST /uploads/{upload_id}/abort` | 中止上传 |
| `GET /stats` | 当前已用字节数 |

配额/保留/恢复窗口可通过环境变量配置：`QUOTA_MAX_BYTES`、`QUOTA_MAX_OBJECTS`、
`RETAIN_SECONDS`、`RECOVER_SECONDS`（默认 `0`，即删除即终态）。

## 兼容范围

- 纯内存引擎，无持久化、无后台清理；窗口判定基于注入时钟（默认 `time.monotonic`），
  便于测试复现。历史版本永久保留在内存中，恢复窗口不影响历史查询。
- API 向后兼容：不配置 `RecoveryPolicy` / `RECOVER_SECONDS` 时，删除行为与旧版一致；
  `delete_object` 新增的 `idempotency_key`、DELETE 请求体均为可选。

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest -q -s     # 运行全部测试，-s 可看到每个用例打印的输入与判定依据
uv run uvicorn main:app # 启动 HTTP 服务（默认 127.0.0.1:8000）
RECOVER_SECONDS=3600 uv run uvicorn main:app  # 带 1 小时删除恢复窗口启动
```

测试覆盖：分块上传完整流程、缺失/重复/越界分片、校验和失败、中止与已完成会话的
后续操作、并发写入无污染、并发读写不撕裂、覆盖/删除的可见性、容量与数量配额、
保留期约束、写入与合并提交的幂等重放；以及**按历史版本回滚、窗口内/窗口外恢复、
重复删除与重复回滚的幂等重试、生命周期翻转后旧结论失效、重试标识跨对象/跨上传/
跨操作隔离、回滚/删除/写入并发不互相污染、恢复的配额约束**，外加 HTTP 入口错误
结构的一致性。全部用例纯内存执行，可复现，不依赖外部服务或网络。
