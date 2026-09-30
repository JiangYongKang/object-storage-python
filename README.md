# 本地对象存储（object-storage-python）

在 FastAPI 骨架之上实现的本地对象存储能力：分块上传、对象版本与并发一致性、
配额与保留约束、**按历史版本回滚**与**可恢复删除**。所有拒绝均携带可区分的
`reason` 码，各入口（存储引擎 / HTTP API）给出一致且可解释的结果。纯内存实现，
不依赖任何外部服务或网络。

## 存储模型

- **对象（Object）**：以 `key` 标识，内容由一串单调递增的**版本（ObjectVersion）**构成。
  版本不可变，包含 `version_id`、`data`、`checksum`（内容 sha256）、`size`、`created_at`。
- **删除即墓碑（tombstone）**：删除追加一个 `deleted=True` 的版本，历史版本保留可查，
  当前版本为墓碑时读取返回 `object_not_found`。墓碑不是数据，不能回滚到它。
- **回滚（rollback）**：`rollback_object(key, version_id)` 把对象内容恢复为某个
  **曾经存在过的完整版本**，恢复动作本身**追加一个新版本**（内容即目标版本的数据）。
  中间历史一律保留，任一历史完整版本之后仍能按 `version_id` 稳定读出。
  对象当前处于删除状态时不能直接回滚，请先恢复。
- **可恢复删除（recoverable delete）**：配置 `RecoveryPolicy(recover_seconds=…)` 后，
  删除在窗口内是“软删除”：读取返回 `object_not_found`，但历史版本可查，
  `recover_object(key)` 可恢复为**删除前最后一个完整版本的内容**（同样追加新版本、留痕）。
  超过窗口（`now - 删除时刻 > recover_seconds`）拒绝恢复，返回 `recovery_window_expired`；
  墓碑与历史不会被物理清理，只是不能再恢复。`recover_seconds=0`（默认）表示删除即不可恢复，
  与旧版本语义完全兼容。可反复“删除→恢复→再删除→再恢复”，每次恢复都以
  最近一次删除前的存活版本为准。
- **分块上传会话（MultipartUpload）**：`create_upload → upload_part* → complete_upload`
  或 `abort_upload`。会话状态机为 `active → completed | aborted`。
  **未完成或已中止的会话不产生任何可见对象，也不占用配额**；完成/中止时分片数据即被清理。
- **配额（QuotaPolicy）**：`max_bytes`（存活对象总字节）与 `max_objects`（存活对象数），
  `None` 表示不限制。覆盖/回滚/恢复同 key 只按差额计量，删除立即释放容量。
- **保留（RetentionPolicy）**：`retain_seconds` 内当前版本禁止被覆盖或删除。
- **恢复窗口（RecoveryPolicy）**：`recover_seconds` 控制删除后可恢复的时长，
  `0`（默认）表示不可恢复。

## 版本与一致性规则

- 所有状态变更在同一把可重入锁内完成：并发写入/覆盖/删除/回滚/恢复相互串行化，
  **读取永远对应某一完整版本，绝不撕裂**（每个版本自带校验和可自证）。
- 覆盖、删除、回滚、恢复均以**追加新版本**的方式生效，可见性确定：提交成功即对后续
  读取可见；显式指定 `version_id` 可稳定读取任一历史完整版本，回滚不会抹掉中间历史。
- **回滚与配额/保留的交互**：回滚前若当前版本仍在保留期内，按 `retention_violation`
  拒绝（保留期约束的是“销毁/替换当前版本”）；回滚产生的新内容按配额做差额计量，
  超容量返回 `quota_bytes_exceeded`。回滚不检查数量配额（同一 key 复活，不新增对象数）。
- **恢复与配额/保留的交互**：删除已立即释放容量，恢复相当于重新写入删除前的内容，
  因此受容量（及数量）配额约束，超限拒绝且状态不变；恢复不覆盖/删除任何已有版本，
  **不受保留期限制**，恢复出的新版本从恢复时刻重新计时，之后对它的覆盖/删除仍受保留期保护。
- **幂等**：写入/合并/删除/回滚/恢复均可携带 `idempotency_key`。幂等记录按
  **（对象 key, 幂等键）作用域**保存首次结论：同一笔提交的重复调用永远返回同一结果，
  不重复占用配额、不多出版本；同一个重试标识用在不同对象或不同上传之间完全隔离、
  互不重放。同一（对象, 幂等键）若先用于一种操作（如写入）又用于另一种操作（如回滚），
  返回 `idempotency_conflict` 明确拒绝，绝不“接口说成功、对象却没变”。
  不带幂等键的重复删除结论同样稳定：对象已删除后一律返回 `object_not_found`。

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
| `quota_bytes_exceeded` | 超出容量配额（含回滚/恢复后的计量） |
| `quota_objects_exceeded` | 超出对象数量配额（含恢复一个已删除 key） |
| `retention_violation` | 保留期内禁止覆盖/删除/回滚 |
| `object_not_found` | 对象不存在或当前版本为删除标记（回滚已删除对象亦返回此码） |
| `version_not_found` | 回滚目标 `version_id` 不存在 |
| `version_not_restorable` | 回滚目标是墓碑版本，不能回滚到它 |
| `object_not_deleted` | 对象未删除就调用恢复 |
| `recovery_window_expired` | 超过可恢复窗口，或未配置恢复窗口 |
| `idempotency_conflict` | 同一（对象, 幂等键）被用于不同操作 |

HTTP 入口下，`object_not_found` / `upload_not_found` / `version_not_found` 返回 404，
其余返回 409，响应体统一为 `{"error": {"reason": ..., "message": ...}}`。

## HTTP API 摘要

| 方法与路径 | 说明 |
| --- | --- |
| `PUT /objects/{key}` | 直接写入（body: `data_b64`, 可选 `idempotency_key`） |
| `GET /objects/{key}?version_id=` | 读取当前或指定版本 |
| `DELETE /objects/{key}?idempotency_key=` | 删除（追加墓碑版本；幂等键可选，同一提交可安全重试） |
| `POST /objects/{key}/rollback` | 回滚到历史完整版本（body: `version_id`, 可选 `idempotency_key`） |
| `POST /objects/{key}/recover` | 恢复窗口内恢复删除前内容（body: 可选 `idempotency_key`） |
| `GET /objects/{key}/versions` | 列出全部版本（含墓碑，`deleted` 字段区分） |
| `POST /uploads/{key}` | 创建分块上传（`part_count`, 可选 `idempotency_key`） |
| `PUT /uploads/{upload_id}/parts/{n}` | 上传分片（`data_b64` + `checksum`） |
| `POST /uploads/{upload_id}/complete` | 完成合并（可选 `idempotency_key`） |
| `POST /uploads/{upload_id}/abort` | 中止上传 |
| `GET /stats` | 当前已用字节数 |

配额/保留/恢复窗口可通过环境变量配置：`QUOTA_MAX_BYTES`、`QUOTA_MAX_OBJECTS`、
`RETAIN_SECONDS`、`RECOVER_SECONDS`（默认 0，即删除不可恢复，保持旧行为）。

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest -q -s     # 运行全部测试，-s 可看到每个用例打印的输入与判定依据
uv run uvicorn main:app # 启动 HTTP 服务（默认 127.0.0.1:8000）
```

存储层可直接注入假时钟，精确复现“保留期/恢复窗口”边界：

```python
from storage import ObjectStore, RecoveryPolicy

now = [1000.0]
store = ObjectStore(recovery=RecoveryPolicy(recover_seconds=30),
                    clock=lambda: now[0])
store.put_object("k", b"v1")
store.delete_object("k")          # @1000 软删除
now[0] += 31                       # 越过窗口
store.recover_object("k")          # -> recovery_window_expired
```

测试覆盖：分块上传完整流程、缺失/重复/越界分片、校验和失败、中止与已完成会话的
后续操作、并发写入无污染、并发读写不撕裂、覆盖/删除的可见性、容量与数量配额、
保留期约束、写入与合并提交的幂等重放，以及 HTTP 入口错误结构的一致性；
新增覆盖**按历史版本回滚（留痕/历史稳定可读/保留与配额约束）、窗口内删除恢复、
超窗与未配置窗口拒绝、删除-恢复-再删除链路、重复删除与重复回滚/恢复的幂等重试、
重试标识在不同对象与不同上传之间的隔离、同一键跨操作冲突，以及回滚/删除/恢复/
写入的并发一致性**。全部用例纯内存执行，可复现，不依赖外部服务或网络。

## 兼容范围

- 未配置 `RecoveryPolicy`（或 `recover_seconds=0`）时，删除行为与旧版本一致：
  追加墓碑、立即释放容量、不可恢复。
- 回滚与恢复均为纯追加语义，旧版本号与按 `version_id` 的读取结果保持稳定不变。
- `delete_object` 新增的 `idempotency_key` 为可选参数；HTTP `DELETE` 的
  `idempotency_key` 为可选查询参数，旧调用方无需改动。
