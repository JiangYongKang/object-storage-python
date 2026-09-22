# 本地对象存储（object-storage-python）

在既有 FastAPI 服务之上实现的一套**纯本地、内存后端**对象存储能力，提供统一的
判定中心，使分块上传、版本管理、配额与保留约束在 **Python 门面**与 **HTTP 入口**
两个入口下给出一致且可解释的结果。

- 零外部依赖：不使用数据库、磁盘、消息队列或网络服务；测试用 FastAPI
  `TestClient` 在进程内完成。
- 线程安全：所有判定在单把可重入锁内原子完成。
- 结果可解释：每类拒绝原因都有独立错误码，并映射到确定的 HTTP 状态码。
- 判定可观测：服务门面为每次操作打印 **输入** 与 **判定依据/结果**。

## 目录结构

```
src/object_storage/
  errors.py    # 可区分的拒绝原因（异常类型 + 稳定 code）
  models.py    # 值对象：ObjectVersion / MultipartUpload / PartSpec / CommitResult
  store.py     # 核心引擎：唯一判定中心（版本链、分块、配额、保留、幂等）
  service.py   # 服务门面：结构化日志 + 透传，不做业务决策
  api.py       # FastAPI 路由：错误码 -> HTTP 状态码
  app.py       # 应用装配
main.py        # uvicorn/fastapi 启动入口
tests/         # 42 个纯本地单测（含真实多线程并发用例）
```

两个入口共享同一个 `ObjectStore` 实例，业务规则只存在于 `store.py` 一处，
因此 Python 调用与 HTTP 调用对相同输入必然得到相同结论。

## 快速开始

```bash
uv sync --dev

# 运行全部测试（日志会打印每个用例的输入与判定依据）
uv run pytest

# 启动 HTTP 服务（本地）
uv run fastapi dev main.py
# 打开 http://127.0.0.1:8000/docs
```

直接使用 Python 门面：

```python
from object_storage import ObjectStorageService, StoreConfig, QuotaConfig

svc = ObjectStorageService(
    StoreConfig(quota=QuotaConfig(max_bytes=10_000, max_objects=100))
)
svc.put("k", b"hello")
svc.get("k")                 # 当前版本
svc.versions("k")            # 版本链
svc.delete("k")
```

## 存储模型

- **Key**：字符串，允许斜杠分层（例如 `logs/2026/a.log`）。
- **ObjectVersion（不可变版本）**：`{key, version_id, data, size, checksum(sha256),
  content_type, created_at, retain_until, delete_marker, upload_id, client_token}`。
  版本一旦产生就不再修改。
- **版本链**：每个 key 维护一条从旧到新的不可变版本链，链顶即“当前版本（current）”。
- **Delete Marker**：删除不是抹除数据，而是向链顶追加一个 `delete_marker=True`
  的特殊版本；历史版本仍可按 `version_id` 读取。
- **分块会话（MultipartUpload）**：`{upload_id, key, parts:{number -> StoredPart},
  aborted, completed}`。会话只在“进行中”存在于内存；**完成或中止都会立即移除**，
  因此不会残留半成品，也不会对未完成/已中止的上传产生任何可见对象。

所有写入都是“**构造新版本 → 追加到链尾 → 原子切换链顶**”，在单把锁内完成；
读取要么看到完整旧版本，要么看到完整新版本，不存在中间/撕裂状态。

## 分块上传流程

1. `create_multipart(key, client_token=...)` → 返回 `upload_id`。
2. `upload_part(key, upload_id, part_number, data)` → 返回该分片的 `sha256`。
   分片号范围 `1..10000`；同号重传整体覆盖（用于重试）。
3. `complete_multipart_upload(key, upload_id, [PartSpec(number, checksum), ...],
   checksum=可选)`：
   - 按**分片号**（而非清单顺序）确定地排序拼接，清单允许乱序；
   - 校验通过后原子合并为一个新版本，随后移除会话。
4. `abort_multipart_upload(key, upload_id)`：丢弃全部分片并移除会话。

### 拒绝规则（原因可区分）

| 场景 | 错误 code | HTTP |
|---|---|---|
| 分片清单为空 | `part_missing` | 400 |
| 清单中分片号重复 | `part_duplicate` | 400 |
| 声明的分片从未上传 | `part_missing` | 400 |
| 分片号越界 / 空负载 | `invalid_part` | 400 |
| 分片声明校验和与实际不符 | `checksum_mismatch` | 422 |
| 合并后整体 sha256 与可选 checksum 不符 | `checksum_mismatch` | 422 |
| 整体 put 声明 checksum 不符 | `checksum_mismatch` | 422 |
| 对已中止会话再上传分片/完成 | `upload_aborted` | 409 |
| 对已完成会话再上传分片/完成/中止 | `upload_already_completed` | 409 |
| 会话 id 不存在 | `no_such_upload` | 404 |
| 重复中止同一会话 | **幂等成功**（不报错） | 200 |

- 任何合并失败都**不会**产生对象、不会移除会话，可用修正后的清单重试完成。
- 中止后：分片被丢弃、会话被移除，`upload_part/complete` 一律以
  `upload_aborted` 拒绝，且该 key 无可见对象。

## 版本与一致性规则

- **覆盖写**：追加新版本并切换链顶；旧版本保留、可读、不可变。
- **删除**：追加 delete marker；当前版本变为 marker 时，普通 `get` 返回
  `not_found`，但按 `version_id` 仍能读到被删的历史版本。
- **删除幂等**：key 不存在或已是删除态时，重复 delete 返回成功
  （`replayed=True`），且**不**追加额外 marker。
- **回滚（rollback）**：把指定历史版本的内容复制为一个**新版本**追加到链顶。
  回滚不修改历史，因此回滚结果本身也可再次被覆盖、删除或回滚。
  - 不允许回滚到 delete marker（`no_such_version`）。
- **读取一致性**：读操作在锁内取链顶或指定版本，返回的是某个完整版本；
  并发覆盖/删除/回滚不会产生相互污染或撕裂读取（见并发测试）。
- **重复消费同一提交结论稳定**：见下节幂等规则。

## 幂等规则（重复提交不重复占用配额/不多版本）

写操作（put / complete / rollback / delete，以及 create_multipart）支持
`client_token`（HTTP 头 `X-Idempotency-Token`）：

- 相同 token 的重复请求返回**首次结果**（`CommitResult.replayed=True`），
  不产生新版本、不重复计配额。
- token 与首次请求的**操作类型**必须一致；跨操作复用同一 token 以
  `idempotency_conflict`（409）拒绝。
- put 重放携带的内容（sha256、size）必须与首次一致，否则
  `idempotency_conflict`，且不改变现有对象。
- rollback 重放的目标版本必须一致，否则 `idempotency_conflict`。
- create_multipart 的 token 唯一标识一次逻辑上传：会话终结后同 token 重放会
  得到确定的 `upload_aborted` / `upload_already_completed`；要发起全新上传
  必须使用新 token。
- complete 的幂等重放在会话已被移除后依然有效（回放首次合并结论）。

> 说明：未携带 token 的请求每次都是一次新的独立提交（会产生新版本）；
> 需要恰好一次语义的客户端应在所有重试中复用同一 token。

## 配额与保留策略

### 配额（`QuotaConfig`）

- `max_bytes`：所有“当前可见对象”的字节总量上限；`None` 不限。
- `max_objects`：当前可见对象数量上限；`None` 不限。
- 计费口径**只统计当前可见版本**：被覆盖的历史版本与 delete marker 不计费。
  - 覆盖写按 **size 增量** 计费（旧版本腾出空间）；
  - 删除释放该对象占用的字节与数量；
  - 分块上传在**完成时**才计费，上传分片期间不占配额；
  - 超配额以 `quota_exceeded`（413）拒绝，错误信息含 used/delta/上限，
    且被拒请求不改变任何用量。

### 保留（retention）

- 整体写或完成分块可传 `retain_seconds`（也可配置
  `default_retention_seconds`），版本携带 `retain_until` 绝对时刻。
- 当前版本仍在保留期内时，**覆盖 / 删除 / 回滚 / 分块覆盖完成**一律以
  `retention_policy_violation`（HTTP 423 Locked）拒绝，错误信息含剩余时间。
- 保留判定基于可注入的时钟（`StoreConfig.clock`），便于确定性测试。
- 保留只锁定“当前版本不被取代”，不影响读取。

## HTTP 接口

对象与分块使用互不重叠的静态前缀 `/o`、`/u`；带静态后缀的具体路由
（versions/delete/rollback/parts/complete/abort）优先于贪婪的 `{key:path}` 注册。
二进制数据以 base64 放在 JSON 中。

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/storage/v1/health` | 健康与当前配额用量 |
| PUT | `/storage/v1/o/{key}` | 整体写入（头 `X-Idempotency-Token`） |
| GET | `/storage/v1/o/{key}?version_id=` | 读当前/指定版本 |
| GET | `/storage/v1/o/{key}/versions` | 版本链 |
| POST | `/storage/v1/o/{key}/delete` | 删除（追加 marker） |
| POST | `/storage/v1/o/{key}/rollback` | 回滚到指定版本 |
| POST | `/storage/v1/u/{key}` | 创建分块上传 |
| PUT | `/storage/v1/u/{key}/{uid}/parts/{n}` | 上传分片 |
| POST | `/storage/v1/u/{key}/{uid}/complete` | 完成合并 |
| POST | `/storage/v1/u/{key}/{uid}/abort` | 中止 |

错误响应统一为：`{"detail": {"reason": "<code>", "message": "<可解释细节>"}}`。

### curl 示例

```bash
# 整体写（幂等 token）
curl -s -X PUT "http://127.0.0.1:8000/storage/v1/o/hello.txt" \
  -H "X-Idempotency-Token: t-1" -H "Content-Type: application/json" \
  -d "{\"data_b64\":\"$(printf 'hello' | base64)\"}"

# 读
curl -s "http://127.0.0.1:8000/storage/v1/o/hello.txt"
```

## 本地验证

```bash
uv sync --dev
uv run pytest                      # 全部用例，打印输入与判定依据
uv run pytest -k concurrent        # 只看并发一致性用例（真实多线程）
uv run pytest -k multipart         # 分块上传各拒绝路径
uv run pytest -k quota             # 配额/保留/幂等
uv run pytest -k api               # HTTP 入口（TestClient，无网络）
```

测试覆盖：并发覆盖/删除/回滚/分块合并的无撕裂读取与原子可见性、
校验和失败（分片级与整体级）、缺失/重复分片、中止后拒绝与无残留、
容量与数量配额、保留期拒绝、以及各写操作的重复提交幂等。
并发与随机用例使用固定种子/屏障，结论可复现。

## 范围与取舍

- 后端为进程内内存实现，进程退出数据不保留；设计上将判定逻辑集中在
  `ObjectStore`，后续可替换为持久后端而保持接口与语义不变。
- 配额按“当前可见版本”计费，历史版本不计费，以换取口径的简单与确定性。
- delete marker 是版本而非对象，按 marker 版本 `get` 返回 `not_found`，
  但它出现在版本链中，可用于审计与回滚判断。
