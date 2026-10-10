# Career.ai 数字人 AI 面试 · 生产部署检查清单

> 适用版本：本次「生产化优化」之后（新增 `backend/services/interview_production.py`、
> 实时通道接线、`/api/health` 新增 `metrics` 键）。
> 目标读者：负责把本服务部署到生产环境的人。
>
> **本次改动的一条铁律：不改变核心业务流程。**
> 所有新增保护都是「**不配 = 默认 = 行为不变**」；唯一例外是**空闲超时**（默认 300s 生效），
> 它只影响「已经死了但没发 FIN」的连接，正常连接由前端心跳续期，不受影响。

---

## 0. 本次改动总览

### 0.1 新增 / 修改的文件

| 文件 | 性质 | 说明 |
| --- | --- | --- |
| `backend/services/interview_production.py` | **新增** | 生产化支撑层：配置 / 会话清理 / 并发闸门 / 每会话互斥 / 限流 / 结构化日志 / 脱敏 / 指标 |
| `backend/api/interview_ws.py` | 修改 | 接线上述能力；新增关闭码 `4408`/`4409`、错误码 `busy` |
| `backend/main.py` | 修改 | 新增 `production_status()` / `_report_production_status()`；`/api/health` 新增 `metrics` 键 |
| `backend/services/avatar_provider_iflytek.py` | 修改 | **修复鉴权签名泄漏**；`408`/`429` 归入可重试 |
| `backend/.env.example` | 修改 | 新增第 6 节「实时通道生产化保护」 |
| `backend/tests/test_interview_production.py` | **新增** | 回归套件（129 项），基线 **65 → 66 套** |
| `frontend/src/api/interviewSocket.js` | 修改 | 镜像 `4408`/`4409`/`busy`；新增心跳；两者**不重连** |
| `frontend/src/components/InterviewRoom.vue` | 修改 | `4408`/`4409` 的中文提示文案 |

### 0.2 新增环境变量（全部可选）

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `INTERVIEW_WS_IDLE_TIMEOUT` | `300` | WS 空闲超时（秒），超时由服务端主动关闭（`4408`） |
| `INTERVIEW_STATE_TTL` | `21600` | 进程内会话活跃记录的 TTL（秒），过期即清理编排状态 |
| `INTERVIEW_MAX_TRACKED_SESSIONS` | `2000` | 进程内最多跟踪多少条会话（超出淘汰最久未活动） |
| `INTERVIEW_MAX_WS_CONNECTIONS` | `4` | 每用户同时打开的 WS 连接数上限（超出拒绝，`4409`） |
| `INTERVIEW_MAX_SESSIONS_PER_USER` | `3` | 每用户同时进行的面试会话数上限（超出拒绝，`4409`） |
| `INTERVIEW_ANSWER_MIN_INTERVAL` | `0` | 两次作答最小间隔（秒）；**`0` = 不限制**，需显式开启 |

> 非法值（非数字 / 负数 / 上限为 0）**启动即报错**，不会静默回落到默认值。
> 运行期若配置非法，服务会降级为默认值并打 `[production][ERROR]`，**不中断面试**。

### 0.3 新增协议常量（前后端已逐字对齐）

| 常量 | 值 | 含义 | 前端是否重连 |
| --- | --- | --- | --- |
| `WS_CLOSE_IDLE` | `4408` | 空闲超时，服务端释放资源 | **否** |
| `WS_CLOSE_BUSY` | `4409` | 并发超限 | **否** |
| `ERROR_BUSY` | `"busy"` | 并发闸门 / 上一条还在处理 | — |

> 既有 `4401` / `4403` / `4404` 与 9 个事件、6 个客户端消息**逐字未变**（有守卫测试）。

---

## 1. 部署前：配置与凭据

- [ ] **`DATABASE_URL` 指向 MySQL 8（`mysql+aiomysql://…`）**。
      生产**只允许 MySQL**；SQLite 仅用于 `backend/tests/`（内存 / 临时文件）。
      *验证*：`python -c "import database; print(database.engine.url)"` 输出 `mysql+aiomysql`。
- [ ] **`SECRET`/凭据类变量只放在 `.env`，不提交仓库**（`.env` 已在 `.gitignore` 中）。
- [ ] **`EMBEDDING_PROVIDER=spark` 且凭据齐全**（否则 RAG 会静默退化为词面哈希，检索恒 0 命中）。
      *验证*：`GET /api/health` → `embedding.semantic_enabled == true`。
- [ ] **数字人**：要接真实讯飞虚拟人需 `AVATAR_PROVIDER=iflytek` + `AVATAR_IFLYTEK_APP_ID/API_KEY/API_SECRET`；
      不配则走 **Mock**（离线、无真实音视频，但面试流程照常跑完）。
      *验证*：`GET /api/health` → `avatar.provider` 与 `avatar.configured`。
- [ ] **检索阈值仍与当前模型 + 语料匹配**（换过 Embedding 模型或改过知识库语料就必须重标定）。
      *验证*：`GET /api/health` → `threshold.status` 不是 `unknown`；启动日志无 `[threshold][WARN]`。
- [ ] **`ADMIN_PASSWORD` 已设置**（`scripts/create_admin.py` 的默认口令仅供本地开发）。
- [ ] 生产化旋钮按容量规划设置（见 0.2）；**建议**：`INTERVIEW_ANSWER_MIN_INTERVAL=0.4`。

---

## 2. Session 管理：超时 / 重连 / 清理

### 2.1 超时（本次新增）

- [ ] 理解机制：服务端在 `asyncio.wait_for(websocket.receive_json(), timeout=空闲超时)` 上等待；
      **任何**客户端消息（含 `ping` / `snapshot`）都续期。
- [ ] **确认前端心跳在跑**：`interviewSocket.js` 的 `HEARTBEAT_INTERVAL_MS = 25000`，
      即 25s 一次 `ping` ⇒ 与默认 300s 超时相比有 12 倍余量。
      若把 `INTERVIEW_WS_IDLE_TIMEOUT` 调到 **< 60s**，必须同步调小前端心跳间隔，
      否则「用户在看题」会被误判为空闲。
- [ ] 反向代理 / 负载均衡的 **idle timeout 必须大于** 本值（Nginx `proxy_read_timeout`、
      ALB `idle_timeout` 等），否则代理会先断，应用侧看不到任何日志。

### 2.2 重连

- [ ] 前端已有自动重连（指数退避，最多 5 次）；**不重连**的四种情况：
      用户主动关闭、`4401`（未登录）、`4404`（会话不存在）、**`4408` / `4409`**（本次新增）。
- [ ] 重连后的界面恢复依赖服务端在**每次**连接建立时推快照
      （`hello → state → question → avatar`），无需额外调 `snapshot`。
- [ ] ★ **多副本部署必须做会话亲和（sticky session）**：编排状态是**进程内**的，
      同一 `session_id` 的 WS 若落到不同副本，`snapshot` 拿不到进程内状态，
      只能按库中 `status` **保守登记**（`created→waiting_question` / `ongoing→waiting_answer`）。
      表现为「重连后状态倒退」。**方案**：LB 按 `session_id`（或按用户）做一致性哈希 / 会话保持。

### 2.3 清理（本次新增）

- [ ] 理解机制：进程内有两处会长大的表——编排层的 `_STATES`、通道层的会话活跃记录。
      清理由 `SessionTracker` 负责，**机会式**触发：**每次有新 WS 连接建立时**顺手
      `sweep()` 一次（淘汰「超过 TTL」+「超出容量上限」的会话，并回调清理编排状态）。
      **没有后台定时任务**（不引入新的启动顺序依赖）。
- [ ] 容量核算：`max_tracked_sessions`（默认 2000）应 ≥ 峰值同时活跃会话数。
      单条记录约几十字节，2000 条可忽略不计。
- [ ] `INTERVIEW_STATE_TTL`（默认 6h）应 ≥ 单场面试最长时长 × 3。
- [ ] **注意**：长时间没有新连接 ⇒ 不会触发清理。若部署形态是「长时间零流量」，
      可接受（零流量时也没有内存增长）；若担心，把 TTL 调小并保证有流量即可。
- [ ] ★ **进程重启会丢失全部编排状态**（本来就不落库）。这是**设计如此**：
      重启后 `snapshot` 按库中 `status` 保守恢复，不会把已结束的会话显示成「准备中」。

---

## 3. 讯飞 SDK 异常处理

### 3.1 错误分档（既有 + 本次修正）

| 上游情况 | 抛出 | 通道层 `status` | 可重试 | 处置 |
| --- | --- | --- | --- | --- |
| 网络异常 / DNS / 连接超时 | `AvatarProviderUnavailableError` | `unavailable` | **是** | 提示「稍后重试」 |
| HTTP `5xx` | `AvatarProviderUnavailableError` | `unavailable` | **是** | 同上 |
| **HTTP `408` / `429`（本次修正）** | `AvatarProviderUnavailableError` | `unavailable` | **是** | 转述上游 `Retry-After` |
| HTTP 其它 `4xx` | `AvatarProviderResponseError` | `failed` | 否 | 配置 / 参数问题 |
| 业务 `header.code != 0` | `AvatarProviderResponseError` | `failed` | 否 | 查上游错误码 |
| 会话失效（`SESSION_EXPIRED_CODES` 命中） | `AvatarProviderSessionExpiredError` | `expired` | 否（需重建会话） | 见 3.3 |
| 响应缺会话 id / 非 JSON | `AvatarProviderResponseError` | `failed` | 否 | 上游契约变化 |

- [ ] 确认上述分类符合预期；`429` 之前被误判为「业务拒绝」（不可重试），本次已修正。
- [ ] **通道层永远不把 provider 异常抛给调用方**：任何故障都降级为「纯文本交付」，
      **绝不阻断面试**（既有约定，本次未变）。

### 3.2 超时

- [ ] `AVATAR_TIMEOUT`（默认 15s）应小于上游网关超时，避免「我们还在等、上游已断」。
- [ ] 该超时**逐请求**生效（含 ASR 取文本），不是整场面试的总预算。

### 3.3 ★ 已知占位项（**部署前必须与官方文档核对**）

- [ ] **`SESSION_EXPIRED_CODES` 仍是空元组 `()`**（`services/avatar_provider_iflytek.py`）。
      含义：**目前没有任何上游错误码会被识别为「会话失效」**，这类情况会落到 `failed`。
      影响：前端不会提示「需要重建数字人会话」，只会提示「数字人异常」。
      **处置**：从讯飞官方文档取得会话失效错误码后填入该元组（一处常量，改完重启即可）。
- [ ] **讯飞端点路径 / 字段名全是占位**（`CREATE_PATH` 等 6 个 `*_PATH`、
      `SESSION_ID_PATHS` / `STREAM_URL_PATHS`）。**接真实讯飞前必须逐个核对**。
      在占位状态下 `AVATAR_PROVIDER=iflytek` 会**连得上但解析不出会话 id** ⇒ 走降级路径。
      建议：先在预发环境用真实凭据跑通 `create_session → initialize → speak → fetch_answer`。

---

## 4. API Key 安全管理

- [ ] ★ **本次修复的泄漏点**：讯飞鉴权把 `authorization`（HMAC 签名）放在 **URL query** 里，
      而 `httpx` 的网络异常消息**内嵌完整 URL**。旧代码 `f"…失败：{exc}"` 会把签名写进
      日志 / `avatar` 事件的 `error` 字段 / 前端界面。
      现已统一经 `sanitize_endpoint()` / `redact_url_in()` 处理：**只保留 scheme+host+path**。
      *验证*：`python backend/tests/test_interview_production.py` 的 `[2]` 段（用桩 httpx 真实走了一遍）。
- [ ] **日志脱敏过滤器已挂在 `careerai.interview` 日志器上**：`k=v` 形式的
      `api_key` / `api_secret` / `token` / `authorization` / `password` / `signature` 等
      一律替换为 `****1234`（保留末 4 位便于核对「是哪一把」）。
- [ ] **确认生产日志采集不会绕过该过滤器**：应用内 `logging` 输出已被脱敏；
      但**反向代理 / 网关的访问日志**会记录**完整 URL**（含 `?token=…`）——
      这是 WS 鉴权走 query 的固有代价。**必须**：
      - [ ] 网关访问日志对 `/api/interview/*/ws` **不记录 query string**；
      - [ ] 或改为在子协议头 / 首帧消息里传 token（属协议变更，需另行评审）。
- [ ] 凭据**不进**健康检查明文：`/api/health` → `avatar.masked_api_key` 只给末 4 位。
- [ ] 凭据轮换预案：改 `.env` 后重启即可；**无需**改代码。

---

## 5. 并发控制

- [ ] **每用户连接数上限** `INTERVIEW_MAX_WS_CONNECTIONS`（默认 4）。
      一个面试间 1 条连接；4 条容纳「多标签页 + 重连窗口期」。超限新连接 → `error(busy)` + `4409`。
- [ ] **每用户并发会话数上限** `INTERVIEW_MAX_SESSIONS_PER_USER`（默认 3）。
      **幂等**：同一 `session_id` 重复登记不重复计数（重连不会被自己的旧记录挤掉）。
- [ ] **每会话非阻塞互斥**：两个标签页同时点「提交作答」，**只放行先到的那个**，
      另一个收到 `error(busy)`。刻意**不排队**——排队会让用户看到「点了没反应」。
- [ ] **额度在 `finally` 释放**（鉴权失败 / 超时 / 异常 / 正常断开都会还回去），
      不会随连接次数单调泄漏。
- [ ] **进程级并发**：以上都是**单副本内**的限制。多副本时总并发 = 单副本上限 × 副本数；
      需要全局配额请在网关层做（本服务不引入 Redis，避免新增依赖）。
- [ ] **上游配额**：讯飞侧有 QPS / 并发限制，请确认 `副本数 × 每用户上限 × 峰值用户数`
      不超过已购配额；不足时优先调小 `INTERVIEW_MAX_WS_CONNECTIONS`。

---

## 6. 日志完善

- [ ] 统一日志器名 **`careerai.interview`**（`services/interview_production.get_logger()`）。
      生产日志配置按此前缀分流 / 采样。
- [ ] 结构化事件（`event` + `k=v` 字段，**已脱敏**）：

      | 事件 | 级别 | 何时 |
      | --- | --- | --- |
      | `ws_connect` | INFO | 连接建立（含 `session_id` / `user_id` / `interview_mode` / `idle_timeout`） |
      | `ws_disconnect` | INFO | 连接关闭 |
      | `ws_unauthorized` | WARNING | token 无效 |
      | `ws_idle_timeout` | WARNING | 空闲超时关闭 |
      | `ws_rejected_busy` | WARNING | 并发闸门拒绝（含 `reason` / `limit`） |
      | `answer_submitted` | INFO | 作答提交成功（含 `mode` / `source` / `chars`） |
      | `answer_rejected` | WARNING | 作答被拒（含 `status` / `error`） |
      | `answer_rate_limited` | WARNING | 触发限流（含 `retry_after`） |
      | `ws_message_failed` | ERROR | 单条消息处理异常（不拖垮连接） |
      | `sessions_evicted` | INFO | 会话淘汰（含 `count` / `session_ids`） |
      | `session_evict_hook_failed` | WARNING | 清理回调自身异常 |

- [ ] 确认日志级别配置为 `INFO`（默认 WARNING 会看不到 `ws_connect` 这类关键行）。
- [ ] 建议告警规则：
      `ws_idle_timeout` 突增 → 前端心跳失效 / 网络问题；
      `ws_rejected_busy` 突增 → 有人重复开窗或配额太小；
      `ws_message_failed` 出现 → 立刻看栈。

---

## 7. 权限控制

- [ ] WS 鉴权口径与 `deps.get_current_user` **完全一致**：token 命中 `user_session` 表即视为登录。
      **先 `accept()` 再校验** ⇒ 未登录能拿到明确的 `4401`，而不是一个含糊的「握手失败」。
- [ ] **归属校验**：`session_id` 不属于当前用户 → 与「不存在」**同样**返回 `4404`，
      **不泄露他人会话是否存在**。
- [ ] `interview_service` 是**唯一接触 DB 的层**，所有读写都带 `user_id` 归属过滤（既有约定）。
- [ ] **限流**：`INTERVIEW_ANSWER_MIN_INTERVAL`（默认 `0` = 关闭）。
      建议生产开启 `0.4`，用于挡「误触连点」与「脚本重放」。触发时返回
      `answer(rejected)` 并计入 `answer_rate_limited_total`。
- [ ] **网关层限流建议**（本服务不内置全局限流）：对 `/api/interview/*/ws` 的连接建立速率、
      对 `/api/auth/*` 的登录尝试做 IP 级限流。
- [ ] ★ **既有风险（本次未改，需在评审中确认）**：
      `main._hash_password` 为**无盐 sha256**、`_generate_token` 为 `md5 + time.time()`。
      这两处属于**既有实现**，改动会影响已注册用户的登录，故本次**刻意未动**。
      **建议**：上线前评估是否迁移到 bcrypt / `secrets.token_urlsafe`（需要一次口令迁移方案）。

---

## 8. 监控指标

- [ ] **`GET /api/health` → `metrics`（本次新增，固定 12 键）**。
      既有 6 键（`status` / `service` / `embedding` / `vector` / `threshold` / `avatar`）**逐字未变**。

      ```jsonc
      {
        "idle_timeout_seconds": 300.0,
        "state_ttl_seconds": 21600.0,
        "max_tracked_sessions": 2000,
        "max_sessions_per_user": 3,
        "max_connections_per_user": 4,
        "min_answer_interval_seconds": 0.0,
        "tracked_sessions": 0,
        "active_connections": 0,
        "active_sessions": 0,
        "busy_sessions": 0,
        "metrics": {
          "counters": { "ws_connections_total": 0, "answer_submitted_total": 0, ... },
          "gauges":   { "ws_connections_active": 0 },
          "uptime_seconds": 12.3
        },
        "error": null
      }
      ```

- [ ] 采集建议（Prometheus / 自研均可）：
      - **Gauge**：`active_connections` / `active_sessions` / `tracked_sessions` / `busy_sessions`
        → 容量水位；`tracked_sessions` 逼近 `max_tracked_sessions` 说明清理没跟上。
      - **Counter**：`ws_connections_total` / `answer_submitted_total` / `answer_rejected_total`
        → 业务量；`ws_idle_timeouts_total` / `sessions_evicted_total`
        → 连接质量；`orchestrator_state_errors_total` / `service_errors_total`
        → 前端状态机 / 业务异常。
      - **告警**：`answer_rejected_total / answer_submitted_total > 20%`（作答体验劣化）；
        `sessions_evicted_total` 突增（TTL 太短或有人刷会话）。
- [ ] **指标是进程内的**：多副本各自计数，需由采集端按 `instance` 标签聚合。
- [ ] 启动日志应出现一行 `[production] 实时通道：…`；配置非法时为 `[production][ERROR] …`。

---

## 9. 部署后验收（冒烟）

```bash
# ① 后端起来、状态可见
curl -s http://127.0.0.1:5000/api/health | python -m json.tool
#   → 期望：status=ok；embedding.semantic_enabled=true（生产）；
#           avatar.provider 符合预期；metrics.error 为 null

# ② 全量回归（66 套，含新增的生产化套件）
cd backend
for t in tests/test_*.py; do
  python "$t" > /dev/null 2>&1 && echo "PASS $(basename $t)" || echo "FAIL $(basename $t)"
done

# ③ 前端自检（77 项）
cd ../frontend && npm test

# ④ 真机端到端（真实 WebSocket ↔ 真实 uvicorn；离线可跑）
cd ../backend && python tests/interview_ws_integration_run.js 2>/dev/null || \
  node ../frontend/tests/interview_ws_integration_run.js
```

- [ ] ① 全部键存在且 `metrics.error` 为 `null`。
- [ ] ② 66/66 PASS。
- [ ] ③ 77/77 PASS。
- [ ] ④ 端到端 53 项 PASS。
- [ ] 手工验证：开两个标签页进同一个面试间 → 同时点「提交作答」→ 一个成功、一个收到
      「上一条操作还在处理中」；关掉一个标签页 → 另一个**不受影响**。
- [ ] 手工验证：断开网络 6 分钟 → 服务端日志出现 `ws_idle_timeout`（默认 300s）。
- [ ] 手工验证：`/api/health` 的 `active_connections` 随连接建立 / 断开来回变化（**不泄漏**）。

---

## 10. 回滚方案

改动是**追加式**的，回滚成本极低：

- [ ] **只回滚行为、不回滚代码**（推荐）：
      把 `INTERVIEW_WS_IDLE_TIMEOUT` 调到极大（如 `86400`）⇒ 空闲超时事实上失效；
      其它旋钮保持默认即「与改造前完全一致」。
- [ ] **完整回滚**：还原 `backend/api/interview_ws.py`、`backend/main.py`、
      `backend/services/avatar_provider_iflytek.py`、
      `frontend/src/api/interviewSocket.js`、`frontend/src/components/InterviewRoom.vue`，
      删除 `backend/services/interview_production.py` 与
      `backend/tests/test_interview_production.py`。
      **无需**改数据库（本次**没有**任何模型 / 列变更，`schema_sync.DESIRED_COLUMNS` 未动）。

---

## 11. 已知限制（诚实清单）

| # | 限制 | 影响 | 建议 |
| --- | --- | --- | --- |
| 1 | 编排状态是**进程内**的 | 多副本时重连可能落到别的副本 ⇒ 状态倒退 | 网关做会话亲和 |
| 2 | 指标是**进程内**计数 | 多副本需采集端聚合 | 按 `instance` 聚合 |
| 3 | 并发闸门是**单副本内**配额 | 全局并发 = 上限 × 副本数 | 需要全局配额请在网关做 |
| 4 | `SESSION_EXPIRED_CODES` 仍是空占位 | 「会话失效」被归为 `failed` 而非 `expired` | 核对官方文档后填入 |
| 5 | 讯飞端点 / 字段名是占位 | 接真实讯飞前必须逐个核对 | 预发环境先跑通 |
| 6 | 无后台定时清理（机会式） | 零流量期间不清理（但也不增长） | 可接受 |
| 7 | 作答限流默认关闭 | 默认不防连点 | 生产设 `0.4` |
| 8 | 登录口令无盐 sha256 / token 用 md5+time | 既有实现，属安全加固项 | 单独排期迁移 |
| 9 | CORS 白名单硬编码 `localhost:5173` | 生产前端域名不同源时会被拒 | 部署前改 `main.py` 白名单 |
| 10 | WS 鉴权走 URL query | 网关访问日志会记到 token | 网关侧屏蔽 query 记录 |

---

## 12. 命令速查

```bash
# 后端（注意端口是 5000，不是 8000）
cd backend && python -m uvicorn main:app --reload --port 5000

# 前端（vite 5173；代理必须开 ws:true，否则 WS 升级请求会被直接丢弃）
cd frontend && npm run dev

# 单套自检
cd backend && python tests/test_interview_production.py

# 全量回归（66 套）
cd backend && for t in tests/test_*.py; do python "$t" >/dev/null 2>&1 \
  && echo "PASS $(basename $t)" || echo "FAIL $(basename $t)"; done

# 前端自检（77 项）
cd frontend && npm test
```
