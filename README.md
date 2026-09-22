# 本地对象存储（object-storage-python）

在 FastAPI 骨架之上实现的本地对象存储能力：分块上传、对象版本与并发一致性、
配额与保留约束。所有拒绝均携带可区分的 `reason` 码，各入口（存储引擎 / HTTP API）
给出一致且可解释的结果。纯内存实现，不依赖任何外部服务或网络。

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
- **保留（RetentionPolicy）**：`retain_seconds` 内当前版本禁止被覆盖或删除。

## 版本与一致性规则

- 所有状态变更在同一把可重入锁内完成：并发写入/覆盖/删除相互串行化，
  **读取永远对应某一完整版本，绝不撕裂**（每个版本自带校验和可自证）。
- 覆盖、删除以追加新版本的方式生效，可见性确定：提交成功即对后续读取可见；
  显式指定 `version_id` 可稳定读取任一历史完整版本（回滚语义）。
- **幂等**：写入/合并提交可携带 `idempotency_key`，同一键的重复提交返回首次结论，
  不重复占用配额、不产生多余版本；同一幂等键重复 `create_upload` 返回同一会话。

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
| `retention_violation` | 保留期内禁止覆盖/删除 |
| `object_not_found` | 对象不存在或当前版本为删除标记 |

HTTP 入口下，`object_not_found` / `upload_not_found` 返回 404，其余返回 409，
响应体统一为 `{"error": {"reason": ..., "message": ...}}`。

## HTTP API 摘要

| 方法与路径 | 说明 |
| --- | --- |
| `PUT /objects/{key}` | 直接写入（body: `data_b64`, 可选 `idempotency_key`） |
| `GET /objects/{key}?version_id=` | 读取当前或指定版本 |
| `DELETE /objects/{key}` | 删除（追加墓碑版本） |
| `GET /objects/{key}/versions` | 列出全部版本 |
| `POST /uploads/{key}` | 创建分块上传（`part_count`, 可选 `idempotency_key`） |
| `PUT /uploads/{upload_id}/parts/{n}` | 上传分片（`data_b64` + `checksum`） |
| `POST /uploads/{upload_id}/complete` | 完成合并（可选 `idempotency_key`） |
| `POST /uploads/{upload_id}/abort` | 中止上传 |
| `GET /stats` | 当前已用字节数 |

配额/保留可通过环境变量配置：`QUOTA_MAX_BYTES`、`QUOTA_MAX_OBJECTS`、`RETAIN_SECONDS`。

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest -q -s     # 运行全部测试，-s 可看到每个用例打印的输入与判定依据
uv run uvicorn main:app # 启动 HTTP 服务（默认 127.0.0.1:8000）
```

测试覆盖：分块上传完整流程、缺失/重复/越界分片、校验和失败、中止与已完成会话的
后续操作、并发写入无污染、并发读写不撕裂、覆盖/删除的可见性、容量与数量配额、
保留期约束、写入与合并提交的幂等重放，以及 HTTP 入口错误结构的一致性。
全部用例纯内存执行，可复现，不依赖外部服务或网络。
