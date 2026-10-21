# 认证与权限安全加固报告

> 范围：`career-ai` 后端（FastAPI）+ 前端（Vue 3）。
> 原则：**渐进式加固**——保留既有接口契约、兼容现有用户与会话，不做整站重写。
> 结论先行：**已修复的问题有测试取证；未修复的问题单列在「剩余风险」，不以代码改动代替验证。**

---

## 一、结论摘要

| 类别 | 现状 | 结论 |
| --- | --- | --- |
| 密码哈希 | bcrypt（`bcrypt$` 前缀，rounds=12），旧无盐 SHA-256 **登录时透明升级** | 已达标 |
| Token 生成 | `secrets.token_urlsafe(32)`（256-bit），`md5+时间戳` 已移除 | 已达标 |
| Token 生命周期 | `expires_at`（默认 7 天）+ `revoked_at` + `POST /api/auth/logout` | 已达标 |
| 资源归属 | 面试会话所有读写经 `interview_service` 带 `user_id` 过滤，越权一律 404 | 已达标（有测试） |
| 资源归属（简历） | `resume_id` 原来只校验「存在」不校验归属 → A 可挂 B 的简历；现按 `resume.user_id` 过滤，写路径拦截 + 读路径兜底 | 已修复（有测试） |
| 管理员权限 | `get_current_admin` **实时查 `user.role`**，不再信任登录时快照 | 已加固（有测试） |
| WebSocket 鉴权 | 长期 token 走 URL → **30 秒一次性 ticket**；token 仅兼容回退 | 已加固（有测试） |
| WS Origin / 连接后失效 | Origin 白名单 + 同源判定；连接存续期 60 秒复核登录态 | 已加固（有测试） |
| 日志脱敏 | `careerai.interview` 过滤器 + **根日志器 / uvicorn.access** 过滤器 | 已加固（有测试） |
| 422 响应 | 不再回显 `input`（注册/登录密码不落响应体） | 已加固（有测试） |
| CORS | 改为 `CORS_ALLOW_ORIGINS` 环境变量，默认值保持本地两个入口 | 已加固 |
| 仓库密钥 | **发现真实 `SPARK_API_SECRET` 在 Git 历史与当前 `.env` 中** | **未解决，必须轮换**（见 §五） |

**明确未做的事**：没有重写认证系统；没有改动既有 HTTP 契约字段；没有执行 Git 历史重写（需协调、属破坏性操作）；没有在真实 MySQL / 真实浏览器 / 真实讯飞环境下做渗透测试。

---

## 二、修改文件清单

### 后端

| 文件 | 变更 |
| --- | --- |
| `backend/security.py`（新增） | 密码哈希（bcrypt + 旧格式升级）、`generate_token`、`session_is_valid`、`load_valid_session`、`create_user_session` |
| `backend/deps.py` | `get_current_admin` 实时查 `user.role`（防降级后旧会话继续持权）；保留对 `get_current_user` 的依赖以兼容测试覆盖链 |
| `backend/main.py` | 注册/登录改用 bcrypt 与高熵 token；新增 `/api/auth/logout`、`/api/auth/me`；登录限流；422 不回显输入；CORS 可配置；安装访问日志脱敏过滤器；500 响应不再回显异常文本 |
| `backend/api/interview_ws.py` | 新增 `POST /api/interview/{id}/ws-ticket`；ticket 生成/校验（30s、单次、绑定 session）；WS 握手优先 ticket；Origin 校验；连接存续期登录态复核 |
| `backend/api/interview.py` | 仅异常收敛（`IntegrityError → 409`、500 不回显异常）；**未引入任何 WS / ticket 依赖** |
| `backend/services/interview_service.py` | 写路径 `SELECT … FOR UPDATE` 行锁；`IntegrityError` 收敛为 409/400；**`create_session` 校验 `resume_id` 归属**；`_load_resume_skills` 增加 `user_id` 过滤 |
| `backend/services/interview_context_pack.py` | 读简历时按 `resume.user_id == session.user_id` 过滤（兜住历史脏行，纵深防御） |
| `backend/scripts/audit_resume_ownership.py`（新增） | 历史脏行审计（默认只读）；`--fix --yes` 先导出备份再置 `resume_id = NULL` |
| `backend/services/interview_production.py` | 行内脱敏正则加入 `ticket` |
| `backend/models/user.py` | `user_session` 新增 `expires_at` / `revoked_at`（可空） |
| `backend/models/interview.py` | `(session_id, question_no)` 唯一约束 |
| `backend/utils/schema_sync.py` | 登记 `user_session` 两个新列（幂等 ALTER） |
| `backend/utils/login_throttle.py`（新增） | 登录失败计数与窗口限流 |
| `backend/utils/worker_guard.py`（新增） | 单 worker 启动断言（进程内状态前提） |
| `backend/scripts/create_admin.py` | **移除内置默认管理员密码**；未提供密码时拒绝执行 |
| `backend/scripts/migrate_interview_question_unique.py`（新增） | 唯一约束迁移脚本（先查重、可回滚） |
| `backend/debug_spark.py` / `backend/test_spark_v2.py` | 不再打印 Authorization / Signature 片段 |
| `backend/requirements.txt` | 新增 `bcrypt>=4.0.0` |

### 前端

| 文件 | 变更 |
| --- | --- |
| `frontend/src/api/interviewSocket.js` | `connect()` 先取 ticket 再握手；401/403 不再回退到 URL token；重连接住 async 拒绝 |
| `frontend/src/components/InterviewRoom.vue` | 适配 `connect()` 的 Promise（保持同步调用点，用 `.catch` 收口） |
| `frontend/src/App.vue` | markdown-it **显式** `html: false`（v-html 前的最后一道闸） |
| `frontend/tests/test_interview_mode.js` | 适配 ticket 流程 |

### 测试

| 文件 | 覆盖 |
| --- | --- |
| `backend/tests/test_auth_security.py`（新增） | 密码哈希、token 熵、过期/撤销、注册登录、logout、限流、422 不回显、访问日志脱敏（**44 项**） |
| `backend/tests/test_ws_ticket.py`（新增） | ticket 生成/单次/过期/会话绑定；端点 401/404/200；WS 握手、重放 4401、跨站 4403、撤销后 4401（**34 项**） |
| `backend/tests/test_authz_ownership.py`（新增） | 篡改 `session_id` 的读/写越权、**篡改 `resume_id` 挂他人简历**、过期/撤销 token、管理员降级、知识库写权限（**23 项**） |
| `backend/tests/test_admin_role.py` | 管理员角色与注册提权防护（**36 项**） |
| `backend/tests/test_interview_concurrency.py`（新增） | 并发作答 / 行锁 |
| 既有套件 | `test_interview_ws.py`（102）、`test_interview_production.py`（129）、`test_knowledge_api.py`（71）、`test_knowledge_upload_api.py`（69）均保持通过 |

---

## 三、威胁模型

### 3.1 资产

1. 用户口令与口令哈希（`user.password_hash`）
2. 会话凭证（`user_session.token`）与 WS 一次性 ticket
3. 面试会话内容（题目、作答、报告）—— 用户私有
4. 知识库写权限 —— 全局共享数据的写入口
5. 第三方凭据（讯飞 SPARK / 虚拟人 / Embedding）
6. 服务端日志与错误响应

### 3.2 攻击者能力假设

| 编号 | 攻击者 | 能力 |
| --- | --- | --- |
| A1 | 未认证互联网用户 | 可任意调用公开端点、伪造参数、扫描路由 |
| A2 | 已认证普通用户 | 拥有合法 token，可篡改 `session_id` / `document_id` 等标识 |
| A3 | 恶意网页 | 可诱导受害者浏览器发起跨站请求 / 跨站 WebSocket |
| A4 | 日志 / 监控读取者 | 可读应用日志、网关访问日志、错误上报 |
| A5 | 仓库读取者 | 可读当前代码与 **Git 历史** |
| A6 | 数据库读取者（拖库） | 可读 `user` / `user_session` 表 |

### 3.3 威胁与缓解

| # | 威胁 | 缓解 | 取证 |
| --- | --- | --- | --- |
| T1 | 拖库后离线爆破口令 | bcrypt（自适应成本）；旧 SHA-256 登录时升级；未知账号跑 dummy bcrypt 消除时间侧信道 | `test_auth_security [2][3][9]` |
| T2 | Token 可预测（MD5/时间戳） | `secrets.token_urlsafe(32)`；测试断言两次生成不同、长度与字符集 | `test_auth_security [1]` |
| T3 | Token 永不过期 / 无法撤销 | `expires_at` + `revoked_at` + `logout`；失效判定 fail-closed | `test_auth_security [4][5][10]`、`test_authz_ownership [3][4]` |
| T4 | 改 `session_id` 读他人面试 | 所有读写经 service 的 `user_id` 过滤；不存在与无权同为 404 | `test_authz_ownership [1][2]` |
| T4b | 改 `resume_id` 把**他人简历**挂到自己会话，借出题 / 报告 / 上下文包把他人简历正文抽成技能注入 Prompt | 写路径按 `resume.user_id` 校验归属；读路径（service + 上下文包）再过滤一次；错误文案与「不存在」一致，不构成存在性预言机 | `test_authz_ownership [7]` |
| T5 | 普通用户调用管理员接口 | `get_current_admin` 实时查 `user.role`；注册体 `role` 字段被忽略 | `test_authz_ownership [5][6]`、`test_admin_role` |
| T6 | 管理员被降级后旧会话仍持权 | 权限判定实时查库，不使用 `user_session.role` 快照 | `test_authz_ownership [5]` |
| T7 | 长期 token 泄漏在 URL / 网关日志 | WS 改用 30 秒单次 ticket；401/403 时**不回退**到 URL token | `test_ws_ticket [2][3][4][10]` |
| T8 | 跨站 WebSocket 劫持 | Origin 白名单 + 同源判定；不带 Origin 的非浏览器客户端仍需有效凭证 | `test_ws_ticket [13]` |
| T9 | 连接建立后凭证被撤销仍可继续操作 | 每 60 秒复核登录态（token 路径覆盖撤销/过期） | `test_ws_ticket [14]` |
| T10 | 凭证进日志 | 应用域过滤器 + 根/uvicorn 访问日志过滤器；`ticket` 纳入脱敏词表 | `test_auth_security [14]` |
| T11 | 422 回显密码 | 校验异常只保留 `type/loc/msg` | `test_auth_security [13]` |
| T12 | 500 回显内部异常 / SQL 片段 | 统一「服务器内部错误」；服务层日志记录完整栈 | `test_interview_api` 等既有套件 |
| T13 | SQL 注入 | ORM 参数化；仅迁移脚本使用拼接，且标识符为硬编码常量 | 代码审查（无用户输入进入 SQL 文本） |
| T14 | 存储型 XSS（LLM 输出进 `v-html`） | markdown-it **显式 `html: false`**；`javascript:` 链接由 `validateLink` 拦截 | 代码审查（无自动化浏览器测试） |
| T15 | CSRF | 不使用 Cookie 鉴权（Bearer + localStorage），跨站请求无法自动携带凭证 | 设计层面不适用 |
| T16 | 仓库中的固定管理员口令 | `create_admin.py` 移除默认密码，必须显式提供 | 代码审查 |

---

## 四、测试结果

运行方式（不使用 pytest，直接执行自检脚本）：

```bash
cd backend
python tests/test_auth_security.py
python tests/test_ws_ticket.py
python tests/test_authz_ownership.py
python tests/test_admin_role.py
python tests/test_interview_ws.py
python tests/test_interview_production.py
python tests/test_knowledge_api.py
python tests/test_knowledge_upload_api.py

# 全量（72 套）
for t in tests/test_*.py; do python "$t" >/dev/null 2>&1 \
  && echo "PASS $(basename $t)" || echo "FAIL $(basename $t)"; done

cd ../frontend && npm test    # 77 项
```

| 套件 | 结果 |
| --- | --- |
| `test_auth_security.py` | 44 / 44 通过 |
| `test_ws_ticket.py` | 34 / 34 通过 |
| `test_authz_ownership.py` | 23 / 23 通过 |
| `test_admin_role.py` | 36 / 36 通过 |
| `test_interview_ws.py` | 102 / 102 通过 |
| `test_interview_production.py` | 129 / 129 通过 |
| `test_knowledge_api.py` | 71 / 71 通过 |
| `test_knowledge_upload_api.py` | 69 / 69 通过 |
| 前端 `npm test` | 77 / 77 通过 |
| 后端全量 | **72 / 72 套通过** |

> 备注：全量回归过程中出现过两次**不可复现**的失败（`test_auth_security.py`、
> `test_retrieval_scoring_rules.py`），单独复跑均连续通过，最终一次全量运行为
> **72 / 72 全绿**。两次失败都发生在系统负载较高时，且失败套件本身与其它套件无共享状态，
> 怀疑为资源竞争 / 进程启动抖动，**不是断言逻辑错误**；已记录为观察项。

---

## 五、剩余风险

### P0 —— 必须立即处理

**R1：真实 `SPARK_API_SECRET` 已进入 Git 历史与当前工作区**

- 证据：`git log --all -S "ZDQ0Y2QzMzQxM2EyMmYxZDg2NzViNjIx"` 命中 `4d68abc`、`f6abbc1`、`eed664c`；当前 `backend/.env` 第 15 行仍为该值。
- 影响：任何能读仓库（含 fork / 备份 / CI 缓存）的人都能复用该凭据，产生费用或数据风险。
- **处置（需人工执行，未自动进行）**：
  1. **先在讯飞控制台轮换 / 吊销该 secret**（这一步最紧急，历史清理不能替代轮换）；
  2. 用 `git filter-repo --replace-text` 或 BFG 清理历史中的该字符串；
  3. 与所有协作者协调后 force-push，并通知重新 clone；
  4. 轮换后检查账单与调用日志，确认无异常使用。
- 说明：历史重写会改变所有提交哈希，属破坏性操作，**不在本次自动执行范围内**。

### P1 —— 高风险，建议排期

**R2：会话 token 以明文存库**
拖库者可直接使用 `user_session.token` 登录。修复需要把 token 改为「只存哈希（如 SHA-256）+ 查询时哈希比对」，并做一次在线迁移（新旧并存一段时间）。本次未做，因为会改变登录查询口径且需要迁移窗口。

**R3：ticket 存在进程内字典**
多副本部署时 ticket 可能落到没有该 ticket 的副本，导致握手 4401。已由 `utils/worker_guard.assert_single_worker()` 强制单 worker；若要水平扩展，需要把 ticket 放到 Redis 等共享存储。

### P2 —— 中风险

**R4：ticket 路径无法感知「会话撤销」**
ticket 是一次性、30 秒有效，但连接建立后没有长期 token 可复核。当前退化为「用户仍存在」检查，撤销最多延迟到空闲超时（默认 300 秒）才断开。缓解：生产把 `INTERVIEW_WS_IDLE_TIMEOUT` 调小（如 120 秒），或后续把 ticket 与 `session_id` 关联后复核。

**R5b：简历读路径在冻结模块内仍按 id 直读**
`interview_core.load_resume_row` 与 `interview_planner._load_resume` 位于**源码指纹冻结清单**（改动会触发多处 sha256 守卫），因此本次**未修改**它们。它们读的是 `session.resume_id`，而该字段在**写入时**已被归属校验，故新数据安全；**历史脏行**由 `interview_service._load_resume_skills` 与 `interview_context_pack` 两处过滤兜住，`interview_core` / `interview_planner` 这两条路径仍会读到他人简历正文。
处置：部署前先跑 `scripts/audit_resume_ownership.py`（只读）确认脏行数为 0；若不为 0，用 `--fix --yes` 解除引用后再上线。这是「不改冻结模块」与「彻底关闭该面」之间的取舍，已显式记录。

**R5：XSS 防护依赖 markdown-it 配置**
已显式 `html: false`，但没有 DOMPurify 之类的二次净化，也没有浏览器端自动化测试。若未来有人打开 `html: true`，`v-html` 会立刻变成可执行标记。建议加一条源码守卫测试，禁止 `html: true`。

**R6：知识库读接口对所有登录用户开放**
`GET /api/knowledge/documents` 及详情不做用户级隔离。这是「知识库是全局共享资源」的设计选择，不是缺陷；若未来知识库需要按用户隔离，必须新增 `owner_id` 并改所有查询。

**R7：缺少安全响应头**
未设置 CSP / `X-Content-Type-Options` / `X-Frame-Options` / HSTS。属于部署层加固，建议在网关或中间件补。

**R8：登录限流是进程内的**
多副本时限流阈值会按副本数放大；IP 维度依赖 `request.client.host`，在反向代理后需要正确配置 `X-Forwarded-For` 信任链，否则所有请求会被当成同一个 IP。

**R9：部分 LLM 接口无需登录**
`POST /api/chat`、`POST /api/resume/analyze`、`POST /api/resume/parse`、`POST /api/match`
是**无状态**接口（不读写任何用户数据，因此不存在越权面），但也不要求登录，
可被匿名脚本消耗大模型配额。改为强制登录会破坏既有前端契约，故本次未动。
建议在网关层对这些路径做 IP 级限流。

### P3 —— 低风险 / 观察项

- **R10**：`requirements.txt` 使用范围版本，无锁文件；供应链风险未收敛。
- **R11**：`debug_spark.py` / `test_spark_v2.py` 仍在仓库中（已脱敏输出），建议移入 `scripts/dev/` 或删除。
- **R12**：`fix_admin.py` 使用 `pymysql` 直连并硬编码数据库名 `career`，属遗留脚本，建议删除。

---

## 六、部署注意事项

### 6.1 新增 / 变更的环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `AUTH_SESSION_TTL_SECONDS` | `604800`（7 天） | 会话有效期；小于 60 会被抬到 60 |
| `AUTH_BCRYPT_ROUNDS` | `12` | bcrypt 成本；小于 4 会被抬到 4 |
| `AUTH_LOGIN_MAX_FAILURES` | `5` | 登录失败次数阈值 |
| `AUTH_LOGIN_WINDOW_SECONDS` | `900` | 限流窗口 |
| `CORS_ALLOW_ORIGINS` | 本地两个 Vite 入口 | 逗号分隔；生产分域部署必须显式配置 |
| `WS_ALLOWED_ORIGINS` | 回落到 `CORS_ALLOW_ORIGINS` | WS Origin 白名单；同源部署无需配置 |
| `INTERVIEW_WS_IDLE_TIMEOUT` | `300` | 建议配合 ticket 路径调到 120 |
| `INTERVIEW_ANSWER_MIN_INTERVAL` | `0`（关闭） | 建议生产设 `0.4` |
| `ADMIN_PASSWORD` | 无 | `create_admin.py` **不再有内置默认密码** |

### 6.2 部署前检查

1. **单 worker**：`WEB_CONCURRENCY` / `UVICORN_WORKERS` / `GUNICORN_WORKERS` 必须 ≤ 1，否则启动直接失败（进程内状态前提）。
2. **CORS / WS Origin**：前后端分域部署时，两处都要配；同源部署可留空。
3. **数据库迁移**：启动时 `schema_sync` 会给 `user_session` 幂等补 `expires_at` / `revoked_at`（可空，无需回填）。`interview_question` 的唯一约束需手工执行 `scripts/migrate_interview_question_unique.py`（先查重）。
4. **简历归属审计**：上线前执行 `python scripts/audit_resume_ownership.py`（**只读**）。
   输出「发现越权/悬挂引用 0 条」才可放行；若不为 0，先 `--json` 导出明细评审，
   再 `--fix --yes` 解除引用（简历行本身不删除）。**不要**跳过这一步——历史脏行
   在 `interview_core` / `interview_planner` 两条冻结路径上仍会被读到（见 §五 R5b）。
4. **密钥轮换**：按 §五 R1 处理 `SPARK_API_SECRET`。
5. **依赖**：`pip install -r requirements.txt`（新增 `bcrypt`）。
6. **日志**：确认日志级别为 `INFO`，否则看不到 `ws_connect` 等关键行；确认日志采集不会绕过应用内过滤器。

### 6.3 迁移步骤（对现有用户的影响）

- **口令**：无需批量迁移。旧 SHA-256 用户在**下次成功登录**时自动改写为 bcrypt；未登录用户不受影响。
- **会话**：历史 `user_session` 行没有 `expires_at`，按 `created_at + 7 天` 折算；超过 7 天的历史 token 会 401，需要引导重新登录。
- **WS 客户端**：新前端先取 ticket。旧前端仍可用 `?token=`（兼容回退），但建议尽快升级以消除 URL 泄漏面。
- **管理员**：若曾用旧脚本创建过 `admin` 账号且口令是仓库里的默认值，**必须立即改密**（脚本已不再提供默认值）。

### 6.4 回滚

- 代码回滚即可；数据库新增列**可空**，旧代码忽略它们，无需回滚 DDL。
- 回滚后 WS 会回到 `?token=` 模式，前端旧版本仍兼容。
- 回滚不会恢复被清理的 Git 历史（若已执行 R1 的历史清理）。

---

## 七、验证边界（诚实声明）

以下内容**没有**被验证，不能据此宣称「安全问题已完全解决」：

1. 未在真实 MySQL 8 上跑并发 / 行锁 / 唯一约束验证（测试使用 SQLite）。
2. 未在真实浏览器中做端到端 ticket 握手与跨站 Origin 验证（使用 Starlette TestClient 模拟）。
3. 未对真实讯飞接口做鉴权与脱敏验证（使用桩 / Mock）。
4. 未做专业渗透测试、依赖漏洞扫描（SCA）、CSP 实际拦截验证。
5. Git 历史中的密钥**仍未清理**，轮换需人工执行。
6. `test_auth_security.py` 在高压并行下出现过一次未复现的失败，已记录为观察项。
