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
| `INTERVIEW_WS_TOKEN_AUTH` | 开 | `?token=<长期登录 token>` 的**历史兼容**路径。`0/false/no/off` = 显式关闭；**新部署建议设 `0`**（前端已改走一次性 ticket） |
| `INTERVIEW_WS_REVOCATION_WATCH` | 开 | 撤销广播的每实例轮询器。`0` = 关闭 ⇒ 撤销退化为 60s 周期复核 |
| `INTERVIEW_WS_REVOCATION_WATCH_INTERVAL` | `2.0` | 轮询周期（秒），`<0.1` 会被 clamp 到 `0.1`。实测：周期 0.2s ⇒ 撤销 **0.31s**；周期 2.0s（默认）⇒ **2.05s** |
| `AUTH_LOGIN_STORE` | `memory` | 登录失败限流后端。`memory` = 进程内（单 worker 够用）；**`db` = 共享表 `login_failure`，跨实例统一计数**（多 worker 必设） |

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
      `api_key` / `api_secret` / `token` / `ticket` / `authorization` / `password` / `signature` 等
      一律替换为 `****1234`（保留末 4 位便于核对「是哪一把」）。
- [ ] **应用内访问日志也已脱敏**：根日志器与 `uvicorn.access` 上安装了
      `main._LogRedactingFilter`（**保留 `record.args` 结构**，不破坏 uvicorn
      `AccessFormatter` 的位置解包）。`?token=…` / `?ticket=…` 会打印成
      `?ticket=********3456`。
- [ ] **WS 已改为一次性 ticket**：前端先 `POST /api/interview/{id}/ws-ticket`
      （Bearer 头），拿到 30 秒有效、**单次使用**的 ticket 再握手；
      长期 token 不再出现在 WS URL 中。仅当 ticket 接口因**非鉴权原因**失败
      （网络 / 5xx）时才回退到 `?token=`（兼容回退，建议在网关侧观察是否仍有命中）。
- [ ] ★ **ticket 已改为 MySQL 共享存储**（`ws_ticket` 表，`services/ws_ticket_store.py`）：
      **不是**进程内字典 ⇒ 「A 实例签发、B 实例校验」可工作；「单次使用」由
      `UPDATE … WHERE used_at IS NULL` 的 `rowcount` 保证全局唯一（真实 MySQL 上
      8 路并发消费只成功 1 次）。库里只存 `sha256(ticket)`，**不存明文**。
- [ ] ★ **部署时重启后端**：`ws_ticket` 是**新表**，由 `lifespan` 的
      `Base.metadata.create_all` 自动创建（`create_all` 只建新表、不给既有表补约束，
      而本表是新表）⇒ **无需人工 DDL、无需数据迁移、无需数据清理**。
      上线后可用 `python tests/ws_ticket_mysql_concurrency_run.py` 复核（生产库只读检查）。
- [ ] ★ **存储不可用时安全失败**：签发 → **503**；握手 → **1011**（标准码，前端会重连重试）。
      **不会**降级回进程内字典，也**不会**被误报成「登录过期」。
- [ ] 登出会作废该用户所有**未使用**的 ticket（`main.logout` → `revoke_for_user`）；
      清理失败只记 WARNING，**不阻断登出**（主效应 token 撤销已落库）。
- [ ] **网关侧仍建议**：对 `/api/interview/*/ws` 的访问日志关闭 query 记录，
      作为纵深防御（应用内脱敏无法覆盖网关自己的日志）。
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

- [ ] WS 鉴权：**ticket 优先**（`?ticket=`，30 秒、单次使用、绑定 session），
      `?token=` 仅作兼容回退；两者最终都归一到 `user_session` 的有效性口径。
      **先 `accept()` 再校验** ⇒ 未登录能拿到明确的 `4401`，而不是一个含糊的「握手失败」。
- [ ] ★ ticket 的**消费是原子的**（数据库条件更新，`rowcount == 1` 才算成功），
      两个 worker 并发验证同一个 ticket 时**最多一个**成功；绑定不符
      （拿 A 会话的 ticket 连 B 会话）**不会消耗**该 ticket。
- [ ] **Origin 校验**：浏览器带 `Origin` 时必须命中
      `WS_ALLOWED_ORIGINS`（优先）/ `CORS_ALLOW_ORIGINS` / 同源（Origin 的 netloc
      等于请求 `Host`）；不带 `Origin` 的非浏览器客户端仍要过鉴权。跨站来源 → `4403`。
- [ ] **连接存续期间的登录态复核**：每 `AUTH_RECHECK_SECONDS`（默认 60 秒）至少复核一次，
      撤销 / 过期最多延迟 60 秒生效，而不是等到空闲超时（默认 300 秒）。
      token 路径覆盖「撤销 + 过期」；ticket 路径退化为「用户仍存在」（见剩余风险）。
- [ ] **归属校验**：`session_id` 不属于当前用户 → 与「不存在」**同样**返回 `4404`，
      **不泄露他人会话是否存在**。
- [ ] `interview_service` 是**唯一接触 DB 的层**，所有读写都带 `user_id` 归属过滤（既有约定）。
- [ ] ★ **简历归属**：`resume.user_id` 才是归属，`interview_session.resume_id` 只是外键。
      创建会话已按 `resume.user_id == user_id` 校验，挂他人简历 → 400（文案与「不存在」一致，
      不构成存在性预言机）；`_load_resume_skills` 与上下文包读路径再过滤一次。
      **上线前必须跑** `python scripts/audit_resume_ownership.py`（只读），
      输出「0 条」才放行；有脏行时先 `--json` 导出评审，再 `--fix --yes` 解除引用
      （简历行本身不删除）。未清理的历史脏行仍会被 `interview_core` / `interview_planner`
      两条冻结路径读到。
- [ ] **限流**：`INTERVIEW_ANSWER_MIN_INTERVAL`（默认 `0` = 关闭）。
      建议生产开启 `0.4`，用于挡「误触连点」与「脚本重放」。触发时返回
      `answer(rejected)` 并计入 `answer_rate_limited_total`。
- [ ] **网关层限流建议**（本服务不内置全局限流）：对 `/api/interview/*/ws` 的连接建立速率、
      对 `/api/auth/*` 的登录尝试做 IP 级限流。
- [ ] ★ **管理员权限实时查库**：`deps.get_current_admin` 不再信任
      `user_session.role` 的登录时快照，而是按 `user_id` 查 `user.role`。
      降级后旧 token 立即失去管理员权限（最多多一次主键查询，仅 admin 接口承担）。
- [ ] ★ **密码与 token 已完成迁移**：
      `security.hash_password` = bcrypt（`bcrypt$` 前缀，rounds 由 `AUTH_BCRYPT_ROUNDS`
      控制，默认 12）；旧无盐 sha256 在**下次成功登录时透明升级**（`needs_upgrade`）。
      token = `secrets.token_urlsafe(32)`（256-bit），`user_session` 带 `expires_at`
      与 `revoked_at`，`POST /api/auth/logout` 立即撤销当前会话。
- [ ] **登录限流**：`AUTH_LOGIN_MAX_FAILURES`（默认 5）/ `AUTH_LOGIN_WINDOW_SECONDS`
      （默认 900）；超限返回 429 + `Retry-After`。未知账号也跑 dummy bcrypt，
      消除账号存在性的时间侧信道。
- [ ] **422 不回显提交值**：`RequestValidationError` 处理器只保留 `type/loc/msg`，
      注册/登录参数非法时响应体不含密码原文。

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

# ② 全量回归（74 套，含新增的生产化 / 认证 / 并发 / 撤销 / 限流套件）
cd backend
for t in tests/test_*.py; do
  python "$t" > /dev/null 2>&1 && echo "PASS $(basename $t)" || echo "FAIL $(basename $t)"
done

# ③ 前端自检（104 项）
cd ../frontend && npm test

# ④ 真机端到端（真实 WebSocket ↔ 真实 uvicorn；离线可跑）
cd ../backend && python tests/interview_ws_integration_run.js 2>/dev/null || \
  node ../frontend/tests/interview_ws_integration_run.js
```

- [ ] ① 全部键存在且 `metrics.error` 为 `null`。
- [ ] ② 74/74 PASS。
- [ ] ③ 104/104 PASS。
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
| 8 | 登录口令无盐 sha256 / token 用 md5+time | ~~既有实现，属安全加固项~~ | **已迁移**：bcrypt + secrets.token_urlsafe + TTL + logout |
| 9 | CORS 白名单曾硬编码 `localhost:5173` | 生产前端域名不同源时会被拒 | **已改为** `CORS_ALLOW_ORIGINS`（逗号分隔，默认值即本地） |
| 10 | WS 长期 token 曾走 URL query | 网关访问日志会记到 token | **已改为一次性 ticket**；应用内访问日志已脱敏，网关侧仍建议屏蔽 query |
| 11 | ~~ticket 存在**进程内**字典~~ | ~~多副本时 ticket 落到别的副本 ⇒ 握手 4401~~ | **已修复**：迁到 MySQL `ws_ticket` 表，`rowcount` 保证原子消费；真实 MySQL 8 路并发只成功 1 次 |
| 12 | ~~ticket 路径无法复核**会话撤销**~~ | ~~撤销后已建立的连接最多存活到空闲超时~~ | **已修复**：ticket 绑定 `auth_session_id` + `ws_revocation` 广播 + 每实例轮询器。实测同实例 **0.313s** / 跨实例 **0.375s**（测试周期 0.2s）；生产周期 2.0s ⇒ 目标 ≤3s |
| 13 | `ws_ticket` / `ws_revocation` 在**生产库尚未创建**（本次改动前） | 多 worker 前必须先有一次启动 | 重启后端即由 `create_all` 建表；`ws_ticket.auth_session_id` 由 `schema_sync` 启动时补列。`tests/ws_revocation_mysql_run.py [1][1b]` 会打印生产库现状 |
| 14 | ~~`login_throttle` 仍是**进程内**~~ | ~~多 worker 时登录爆破限流被放大 N 倍（安全项）~~ | **已提供共享实现**：设 `AUTH_LOGIN_STORE=db` 即跨实例统一计数（实测跨实例拦截、突发后第二波 0 放行）。默认仍 `memory` |
| 15 | 历史提交含真实 `SPARK_API_SECRET` | 拿到仓库历史即可复用凭据 | **必须轮换密钥**并清理历史（见安全报告） |
| 16 | 撤销广播依赖 MySQL 可用 | 库不可用时退化为 60s 复核（不会"永不失效"） | 监控 `/api/health` 与 `ws_revocation_*` 日志 |

---

## 12. 命令速查

```bash
# 后端（注意端口是 5000，不是 8000）
cd backend && python -m uvicorn main:app --reload --port 5000

# 前端（vite 5173；代理必须开 ws:true，否则 WS 升级请求会被直接丢弃）
cd frontend && npm run dev

# 单套自检
cd backend && python tests/test_interview_production.py

# 全量回归（74 套：68 原有 + auth_security + interview_concurrency + ws_ticket
#           + authz_ownership + ws_revocation + login_throttle_store）
cd backend && for t in tests/test_*.py; do python "$t" >/dev/null 2>&1 \
  && echo "PASS $(basename $t)" || echo "FAIL $(basename $t)"; done

# 前端自检（104 项）
cd frontend && npm test
```

---

## 13. 安全加固与并发正确性改造（本次新增）

### 13.1 认证硬化

| 改造点 | 说明 | 回滚 |
|---|---|---|
| 登录自动建号已删除 | 未知邮箱登录统一返回 401「邮箱或密码错误」 | 代码回退即可 |
| 密码哈希迁 bcrypt | 旧 sha256 账号登录时**透明升级**为新格式 | 回退后旧哈希仍可用，但新哈希不可逆 |
| token 改 `secrets.token_urlsafe(32)` | 高熵、URL-safe、43 字符 | 回退后旧 token 仍有效（无撤销机制） |
| `UserSession` 新增 `expires_at` / `revoked_at` | 空串默认 TTL = 7 天；logout 即撤销 | 代码回退即可；两列留在库里无害 |
| 登录限流 | 进程内滑动窗口：默认 5 次/15 分钟，超限 429 + Retry-After | 代码回退即可 |
| `POST /api/auth/logout` / `GET /api/auth/me` | 前端登出必须调后端；启动时校验 token | 代码回退即可 |

新增配置项（均已写入 `regression_env.PINNED`，空串 = 默认）：
- `AUTH_SESSION_TTL_SECONDS`：默认 604800（7 天）
- `AUTH_LOGIN_MAX_FAILURES`：默认 5
- `AUTH_LOGIN_WINDOW_SECONDS`：默认 900（15 分钟）

### 13.2 并发正确性

| 改造点 | 说明 | 回滚 |
|---|---|---|
| `_load_session_for_update` | 4 个写入口（start / begin / submit / end）加 `FOR UPDATE` | 代码回退即可 |
| `InterviewQuestion` 唯一约束 | `(session_id, question_no)` 数据库层兜底 | 代码回退；索引需 `--drop` 才消失（留着无害） |
| `IntegrityError` 收敛 | `submit_answer` / `_activate_session` / `generate_and_persist_next` / `end_session` 捕获冲突 → 400/409 | 代码回退即可 |

### 13.3 单 Worker 断言

- `lifespan` 首行调用 `assert_single_worker()`：检测 `WEB_CONCURRENCY` / `UVICORN_WORKERS` / `GUNICORN_WORKERS`。
- 任一 >1 则启动即 `RuntimeError`。
- 需要水平扩展时，先实现 Redis 状态外置，再移除本断言。
  ★ ticket 共享状态与撤销广播已在第 4/5 轮下沉到 MySQL（§10），但
  `login_throttle` / `ConcurrencyGate` / `RateLimiter` **仍是进程内**，
  所以本断言**必须保留**。解除条件见 `CONCURRENCY_CONSISTENCY_REPORT.md` §9.6。

### 13.4 前端清理

- `App.vue` `onMounted` 恢复登录态（调 `/api/auth/me` 校验）。
- `handleLogout` 先调后端 `logout` 再清本地。
- `fetchUserLogs` 删除硬编码 Mock 数据兜底。
- 管理页容器加 `currentUser.role === 'admin'` 守卫。
- `interviewSocket.js` 4403 加入重连排除。

### 13.5 发布前必做

1. **运行迁移脚本**（若有生产数据）：
   ```bash
   python backend/scripts/migrate_interview_question_unique.py --dry-run
   python backend/scripts/migrate_interview_question_unique.py --yes
   ```
2. **MySQL 真并发验证**（发布前手工跑一次）：
   ```bash
   # 配置 DATABASE_URL 指向 MySQL 测试库（CI 已自动执行同一批，见 §14）
   cd backend && python tests/run_mysql_suites.py
   ```
3. **历史会话失效提醒**：上线瞬间所有旧 token 按 `created_at + 7 天` 判定，过期的会 401，需引导用户重新登录。
4. **套件基线**：后端 74 套、前端 104 项、真机端到端 53 项、真实 MySQL 运行器 5 套 / 163 项。

---

## 14. CI：真实 MySQL 纳入自动门禁（第 5 轮新增）

### 14.1 新增文件

| 文件 | 作用 |
|---|---|
| `.github/workflows/ci.yml` | 3 个 job：`backend-regression` / `frontend-regression` / `mysql-integration` |
| `backend/tests/run_mysql_suites.py` | ★ **唯一判绿处**：只有退出码 `0` 是通过 |
| `backend/tests/mysql_guard.py` | 目标库守卫：`DATABASE_URL` 指向非测试库 ⇒ 直接拒绝（退出码 2） |
| `backend/tests/engine_pool_mysql_run.py` | 驱动 / 连接池 / 事务边界（25 项） |
| `backend/tests/ws_revocation_mysql_run.py` | 撤销广播的真实 MySQL 验证（54 项） |
| `backend/tests/login_throttle_mysql_run.py` | 登录限流的真实 MySQL 验证（30 项） |
| `backend/scripts/setup_test_mysql.py` | 建**专用测试库 + 最小权限账号**（幂等，自带 4 项权限自验） |
| `backend/scripts/ci_bootstrap_db.py` | 在真实 MySQL 上跑 `create_all` + `schema_sync`（`--twice` 验幂等） |

### 14.2 为什么需要 `run_mysql_suites.py` 这一层

`tests/*_run.py` 的退出码是 `0` 通过 / `1` 失败 / **`2` 环境不满足**。

`2` 对**人**是友好的（"这台机器没配 MySQL，跳过吧"），对 **CI 是致命的**：
若 workflow 直接 `python tests/xxx_run.py`，容器没起来会返回 `2`，
有人加一句 `|| true` 就变绿了 —— **一个用例都没跑，CI 却是绿的**。

所以判据只在 CI 侧收紧（**不改运行器本身的退出码语义**，本地手工跑行为不变）：

| 子进程退出码 | 包装脚本判定 | 最终退出码 |
|---|---|---|
| `0` | 通过 | `0` |
| `1` 且 `failed > 0` | 断言失败 | `1` |
| `1` 且 `failed == 0` | **没跑到断言就崩了** | `3` |
| `2` | **环境不满足（= CI 失败）** | `2` |

### 14.3 CI 里的执行顺序

```bash
# 1) 等待 MySQL 就绪（150s 超时 ⇒ exit 1，不是跳过）
# 2) 建专用测试库 + 最小权限账号（幂等；自带 4 项权限自验）
cd backend && python scripts/setup_test_mysql.py --password "$CI_TEST_PW"
# 3) 切换到专用账号（DATABASE_URL 改为 career_ci@career_ci_test，不再用 root）
# 4) 初始化 / 迁移（与 main.py lifespan 同一条路径，连跑两遍验幂等）
cd backend && python scripts/ci_bootstrap_db.py --twice
# 5) 真实 MySQL 运行器（5 套 / 163 项）
cd backend && python tests/run_mysql_suites.py
# 6) 清理遗留 probe 库（if: always()；正则含 ticket/conc/revoke/throttle/pool）
```

★ 初始化**不写进 YAML 的 run 里**，而是调脚本：否则 CI 验证的是
`create_all` + `ensure_columns` 的**平行实现**，它绿了不代表应用启动时能绿。

### 14.4 本地等价命令（发版前手工跑）

```bash
# ① 起一个本机 MySQL（8.0+），并用**管理员**账号建测试库 + 专用账号
cd backend
python scripts/setup_test_mysql.py --password '<本地测试口令>' --show-url
#   → 它会打印一行 export DATABASE_URL='mysql+aiomysql://career_ci:***@localhost:3306/career_ci_test?...'
#   → 复制执行（★ 含口令，别提交进仓库）

# ② 在测试库里建表（与生产启动同一条路径）
python scripts/ci_bootstrap_db.py --twice

# ③ 全部真实 MySQL 运行器
#    pool 25 + ticket 22 + 并发 32 + 撤销 54 + 限流 30 = 163 项
python tests/run_mysql_suites.py

# 只跑一个 / 看清单
python tests/run_mysql_suites.py --only pool
python tests/run_mysql_suites.py --list
```

★ `setup_test_mysql.py` 的 admin 连接串取 `--admin-url`，否则 `MYSQL_ADMIN_URL`，
再否则 `DATABASE_URL`，都没有则读 `backend/.env`（与 `database.py` 同一手法）。
它默认**不打印口令**，`--show-url` 才输出完整串。

★ 专用账号 `career_ci` 只有两条授权：
`career_ci_test`（测试主库）与 `career_%_probe_%`（各运行器的临时库）。
它**连不上**开发库 / 生产库（MySQL 直接 1044）。这一点由脚本的 `[1.4]` 每次实测。

### 14.5 凭据卫生

- CI 用**一次性容器口令**，与生产无关；声明后立刻 `::add-mask::`。
- **没有任何步骤 echo** `$DATABASE_URL` / `$*PASSWORD`（已用脚本校验）。
- `setup_test_mysql.py` **默认不打印口令**（`_redact()` → `***`），`--show-url` 才输出。
- 运行器打印连接串前一律 `_redact()`，只留 `host/db`；不打印 ticket / token 明文。
- `mysql_guard` 的拒绝理由只说库名，**不回显连接串**。
- **生产凭据永远不要写进 workflow 文件**（它会被提交进 Git）。

### 14.6 排查：集成测试红了怎么看

| 现象 | 退出码 | 原因 | 处置 |
|---|---|---|---|
| `[FAIL] 未设置 DATABASE_URL` | `2` | 环境变量没传 | `export DATABASE_URL=...` |
| `[FAIL] DATABASE_URL 不是 MySQL` | `2` | 指到 SQLite 了 | 换成 `mysql+aiomysql://...` |
| `[FAIL] 拒绝运行：目标库 `career` 看起来不是测试库` | `2` | ★ **守卫生效**，你指到开发/生产库了 | 改用 `career_ci_test`；**不要**去改守卫 |
| `[SKIP] 无法创建 probe 库（OperationalError）` + `Can't connect to MySQL server` | `2` | 库没起来 / 端口错 | 先起 MySQL；确认 `host:port` |
| `[FAIL] 没跑到断言就异常退出` + traceback | `3` | 驱动异常 / 崩溃 / 被信号杀掉 | 看末 15 行 |
| 某套件 `失败 N 项` | `1` | 真的断言失败 | 看该套件的 `[FAIL]` 行 |
| 运行器里 `[1.1]/[1.2]` 表不存在 | `1` | 测试库是空的，没初始化 | 先跑 `python scripts/ci_bootstrap_db.py --twice` |
| `setup_test_mysql.py` 报 1044 / 1045 | `1` | admin 账号无权建用户，或口令错 | 换有 `CREATE USER` / `GRANT` 权限的账号 |

★ **`2` 在 CI 里必须判失败**：它意味着"一个用例都没跑"。绝不要加 `|| true`。

### 14.7 复现并发问题的固定套路

并发问题最怕"跑一次是绿的"。四条固定手法：

1. **同一套件连跑 10 次**：`for i in $(seq 10); do python tests/xxx_run.py || break; done`
   —— 偶发问题（死锁、间隙锁、`Retry-After` 越界）只在重复时暴露。
2. **加规模**：把运行器里的连接数从 8 调到 32 再跑一遍，看结论是否还成立。
3. **留现场**：`python tests/engine_pool_mysql_run.py --keep`
   —— 保留 `career_pool_probe_*` 临时库，用客户端直接 `SHOW ENGINE INNODB STATUS`
   看 `LATEST DETECTED DEADLOCK`。
4. **开 traceback**：`POOL_DEBUG=1 python tests/engine_pool_mysql_run.py`
   —— 让被 `except` 吞掉的栈打出来（默认吞掉是为了输出可读）。

★ 排查完记得手工删残留库：
`DROP DATABASE IF EXISTS \`career_pool_probe_<pid>_<ts>\`;`
（`--keep` 的库不会被自动清理，CI 里由 `if: always()` 那一步兜底。）

### 14.8 诚实说明

- **本地无法执行 GitHub Actions**。已验证：YAML 可解析、内嵌 Python 可
  `compile()`、所有 `run:` 命令能在本地原样执行并通过。**没有**在 GitHub 上真跑过。
- ★ **CI 侧「专用账号」改动已落地**：`.github/workflows/ci.yml` 的 `mysql-integration`
  job 现在会先调 `setup_test_mysql.py` 建 `career_ci_test` + `career_ci@'%'` 最小权限
  账号，再 `echo DATABASE_URL >> $GITHUB_ENV` 切到专用账号跑 5 套运行器，清理正则已含
  `pool`。本地侧 5 套 / 163 项全部实测通过；CI 仅验证过 YAML 解析与命令可原样执行。
- 只覆盖 MySQL 8.0；8.4 / MariaDB 未验证。
- 运行器用**独立 engine + 独立物理连接**模拟多实例（InnoDB 侧等价于多进程），
  但**不覆盖**进程内状态的跨实例问题 —— 那正是 `login_throttle` 的待办。
- `engine_pool_mysql_run.py` 的 `[4]` 需要 `KILL CONNECTION <自己的连接>`，
  所以要求账号能杀自己的线程（MySQL 默认允许），只读账号跑不了这一节。

---

## 15. AI 能力接入层（`services/ai`）

> 详细设计、接口与测试见仓库根 `AI_LAYER_REFACTOR_REPORT.md`。本节只列**部署必做项**。

### 15.1 新增环境变量

| 变量 | 空串 / 未配置 | 可选值 | 说明 |
|---|---|---|---|
| `AI_RUN_MODE` | **按凭据自动判定** | `mock` / `offline` / `production` | 生产**必须**显式设 `production` |
| `AI_LLM_PROVIDER` | `spark` | `spark` / `fake` | `fake` 仅非生产模式可用 |
| `AI_LLM_TIMEOUT` | 90（秒） | ≥ 1 的数字 | 必须**大于**传输层自身超时（旧 websocket 60s） |
| `AI_LLM_MAX_ATTEMPTS` | 1（不重试） | ≥ 1 的整数 | 重试是显式 opt-in |
| `AI_LLM_BACKOFF_BASE` | 0.5（秒） | ≥ 0 的数字 | equal jitter，封顶 8s |

### 15.2 三种模式

| 模式 | 联网 | 凭据 | 行为 |
|---|---|---|---|
| `mock` | ❌ | ❌ | 返回**固定占位文本**，仅用于联调 / 演示 |
| `offline` | ❌ | ❌ | 任何调用**确定失败**，走各自降级路径 |
| `production` | ✅ | ✅ | 调真实模型 |

★ **自动判定永远不会落到 `mock`**（静默返回假数据比失败严重得多）。

### 15.3 发布前必做

- [ ] 生产环境设 `AI_RUN_MODE=production`，且 `SPARK_APP_ID` / `SPARK_API_KEY` / `SPARK_API_SECRET` 齐全。
      缺任一项 ⇒ **进程起不来**（`AiModeError`），这是有意的 fail-closed。
- [ ] **绝对不要**在生产设 `AI_RUN_MODE=mock`：`mock` 下所有 AI 输出都是固定占位文本。
      该模式在启动日志里会打一行明确警告。
- [ ] 确认启动日志里出现 `[ai] 运行模式=production provider=spark ... —— 调用真实模型`。
- [ ] 抽查 `POST /api/resume/analyze` 的响应体：成功时 `ai_error` 为 `null`，
      降级时 `ai_error.kind` 能区分 `timeout` / `auth` / `rate_limit` / `unavailable` / `empty`。
- [ ] 回归套件**必须**跑在 `AI_RUN_MODE=offline` 上（`tests/regression_env.py` 已钉扎），
      否则本机 `.env` 里的真实凭据会让套件真打星火。

### 15.4 排障入口

| 想知道 | 怎么查 |
|---|---|
| 当前用的哪个 Provider / 哪个模式 | `services.ai.describe_llm()`（**不联网、不抛异常**） |
| 四类能力（LLM / Embedding / 数字人 / ASR）的状态 | `services.ai.describe_capabilities()` |
| 累计调用数 / 错误数 / 重试数 / 耗时 / Token | `services.ai.telemetry.snapshot()`（进程内，多 worker 各自独立） |
| 某次调用为什么失败 | 结构化日志里的 `ai_call_failed`（带 `request_id` / `error_kind` / `prompt_id`） |

### 15.5 诚实说明

- 指标是**进程内**的，多 worker 不聚合（与 `utils/login_throttle` 同一取舍）。
- Token 用量恒为 0：旧星火 transport 不返回用量，本层**不猜**（`LlmUsage.estimated=False`）。
- 外层超时取消的是 `await`，**杀不掉** `run_in_executor` 里的阻塞线程；
  线程会一直阻塞到它自己的 60s websocket 超时。这是旧协议层的固有限制。
- 真实供应商的**联网**验证不在回归循环里（`tests/*_run.py` 才做）。
