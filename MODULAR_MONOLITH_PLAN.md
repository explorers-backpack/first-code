# 模块化单体改造 · 目标目录树与依赖规则

> 目标：**边界清晰、依赖方向明确的模块化单体**。不拆微服务、不拆库、不改部署方式。
>
> 本文是设计契约；执行记录见 `MODULAR_MONOLITH_REPORT.md`。

---

## 1. 盘点结果（`tools/arch_inventory.py`）

工具只做 AST 静态解析、不 import 任何项目模块。产物可复现：

```bash
python tools/arch_inventory.py                       # 人类可读
python tools/arch_inventory.py --json out.json       # 机器可读
python tools/arch_inventory.py --fail-on-cycles      # 硬环守卫
```

| 指标 | 实测 |
|------|------|
| 后端业务模块 | **78** 个 / **30 963** 行 |
| 依赖边 | **168**（顶层 import 构成 **155**） |
| 硬环（顶层互相 import） | **0** |
| 软环（环上有函数内延迟导入） | **1**：`embedding_provider ↔ embedding_provider_spark` |
| fan-in 最高 | `models`(19) / `database`(11) / `services`(10) / `services.ai.errors`(8) |
| fan-out 最高 | `main`(19) / `api.interview_ws`(11) / `services.ai`(10) |
| 最大文件 | `main.py` 1691 / `interview_service.py` 1485 / `interview_ws.py` 1394 |

**软环为什么不是缺陷**：`embedding_provider.py` 的顶层依赖被 AST 断言锁死（只许标准库 +
`services`），而 `spark` 实现必须继承它的基类 ⇒ 唯一解是在工厂函数里延迟导入。
把它当缺陷去「消环」，只会逼人去改冻结文件。工具的 `--fail-on-cycles` 只对**硬环**报警。

**分层方向违规：0 条**（已逐条核对）——`api` 没被反向依赖、`services` 不依赖 `api`、
`models`/`schemas` 不依赖 `services`/`api`、`services.ai` 不依赖路由。
⇒ 这一轮**不是修方向，而是把已经成立的方向变成可执行的守卫**，并拆掉入口文件。

**真正的缺口**（本轮的改造对象）：

1. `main.py` 1691 行，装着 **4 个业务域**的路由 + 内联业务逻辑 + 内联 SQL；
2. 没有 `common/`：配置/DB/日志/错误/监控散在 `database.py` / `deps.py` / `security.py` / `utils/`；
3. `services/` 是 **40 个模块的平铺袋子**，4 个业务域混在一起，靠命名前缀区分；
4. 规则**没有被强制执行**——今天的 0 违规是 9 轮守卫的副产品，不是架构保证。

---

## 2. 目标目录树

```text
backend/
├── main.py                        # 只做装配：app / 中间件 / 异常处理 / lifespan / include_router
│
├── modules/                       # 业务模块（按领域切）
│   ├── auth/                      # ★ 身份认证、权限、会话安全
│   │   ├── __init__.py
│   │   ├── api.py                 #   /api/auth/* 四个端点（只碰 HTTP）
│   │   ├── service.py             #   注册 / 登录 / 登出 / 当前用户用例 + 事务边界
│   │   └── schemas.py             #   UserRegister / UserLogin
│   ├── career/                    # 简历、岗位、匹配与求职业务
│   │   ├── api.py                 #   /api/jobs /api/match /api/resume/*
│   │   └── service.py             #   resume_scoring / job_matching 的用例编排
│   ├── interview/                 # 面试会话、题目、答案、评分与报告
│   │   ├── api.py                 #   已有 api/interview.py 平移
│   │   ├── ws.py                  #   已有 api/interview_ws.py 平移
│   │   └── service.py             #   已有 interview_service 平移
│   ├── knowledge/                 # 文档、切片、Embedding、检索与 RAG
│   │   ├── api.py                 #   已有 api/knowledge.py 平移
│   │   └── service.py             #   文档服务 / 导入管线 / 维护动作
│   └── ai/                        # Provider 接口、模型客户端、Prompt 版本、调用策略
│       └── …                      #   ← services/ai/ 11 模块**平迁**，已成型
│
├── common/                        # 共享基础设施
│   ├── config.py                  #   环境变量读取与校验（新增，逐步收口）
│   ├── database.py                #   ← database.py
│   ├── errors.py                  #   错误模型（统一 HTTPException 映射）
│   ├── security.py                #   ← security.py（口令 / token / 会话）
│   ├── logging.py                 #   日志与脱敏过滤器
│   └── health.py                  #   embedding / vector / threshold / avatar / ai 状态上报
│
├── api/                           # 过渡期：未迁移的路由（迁完即空）
├── services/                      # 过渡期：冻结文件 + 未迁移的领域实现
├── schemas/  models/  utils/      # 过渡期保留
└── prompts/                       # 保持（被 FROZEN 指纹锁定）
```

### 为什么不能一步到位

`tests/test_agent_full_input.py` 的 `FROZEN_SOURCES` 用 sha256 锁死 **13 个源文件 + 2 个
prompt 模板**（`interview_core` / `interview_agent` / `interview_planner` / `interview_context` /
`question_validator` / `knowledge_retriever` / `vector_knowledge_retriever` / `vector_store*` /
`embedding_service` / `embedding_provider`）。另有 **约 150 处测试硬编码了
`"services/xxx.py"` 形式的路径字面量**。

⇒ 冻结文件的**内容**不能动；搬它们的**位置**要同时改 `FROZEN_SOURCES` 的键与所有路径字面量，
一次改动会同时影响 20+ 个套件，**无法定位回归**。因此 `interview` / `knowledge` 两个模块
**最后**迁移，且只在「其他模块都稳定」之后做。

---

## 3. 依赖规则（可执行）

| 编号 | 规则 | 判据 |
|------|------|------|
| **R1** | 单向依赖：`entry(main) → api → service → (domain, data, infra)`；同层不互相依赖 | AST 模块图 |
| **R2** | `api/*` 不得被 `services/` `models/` `schemas/` `utils/` `modules/*/{service,schemas}.py` import | AST 反向边 |
| **R3** | `services/` 与 `modules/*/service.py` 不得 import `api.*` | AST |
| **R4** | `models/` `schemas/` 不得 import `services/` `api/` `modules/` | AST |
| **R5** | AI 适配层（`services/ai/`、未来 `modules/ai/`）不得 import 任何路由模块 | AST |
| **R6** | 顶层 import 构成的环 = 0（软环必须写明理由） | `arch_inventory --fail-on-cycles` |
| **R7** | 路由层不得直接执行 SQL（`select`/`insert`/`update`/`delete`/`db.execute`） | AST 调用节点 |
| **R8** | 领域/服务层不得 import `fastapi.Request` / `Response` | AST |

### R7 / R8 采用「棘轮」而不是「一刀切」

现状有两处**已存在**的违规（`api/interview_ws.py` 的 token 校验内联 SQL、
`main.py` 的各业务路由内联查询）。要求「一次清零」只能靠大规模重写，违反「渐进式」。

因此 `tests/test_module_boundaries.py` 用**棘轮**（ratchet）：

- 每条规则带一个 `KNOWN_ALLOWED` 白名单，写清**为什么**允许、**什么时候**清掉；
- 出现**新**违规 ⇒ 红；
- 白名单里某条**已经不再违规**却没删 ⇒ 也红（强制名单只能变小）。

这样「存量」不阻塞前进，「增量」一个都进不来。

---

## 4. 迁移顺序（按风险从低到高）

| 阶段 | 模块 | 内容 | 风险 | 依据 |
|------|------|------|------|------|
| **P0** | — | 依赖盘点工具 + 边界守卫（棘轮） | 极低 | 纯新增，零行为改动 |
| **P1** | `auth` | `main.py` 的 4 个 `/api/auth/*` + 2 个 schema + 口令/token 助手 → `modules/auth/` | 低 | 无冻结文件；测试符号引用仅 3 处 |
| **P2** | `career` | `/api/jobs` `/api/match` `/api/resume/*` `/api/chat` + `resume_scoring` `job_matching` → `modules/career/` | 中 | 无冻结文件；但 `analyze_resume` 有 4 处测试符号引用 |
| **P3** | `ai` | `services/ai/` → `modules/ai/`（平迁，只改 import 路径） | 中 | 11 模块、无冻结文件、无测试路径字面量 |
| **P4** | `common` | `database` `security` `deps` `utils/*` + `main.py` 的 7 个 status 上报 → `common/` | 高 | `database` fan-in 11，且被 20+ 套件直接 import |
| **P5** | `interview` | `api/interview.py` `api/interview_ws.py` `interview_service` + 冻结核心 | 很高 | 13 个冻结文件 + 最多路径字面量 |
| **P6** | `knowledge` | `api/knowledge.py` + 导入/维护/检索实现 + 冻结向量层 | 很高 | 同上 |

每个阶段都必须：① 只动一个模块；② 跑对应套件 + 全量；③ 记录回滚点。

---

## 5. 兼容性与回滚原则

- **不改任何 HTTP 契约**：路由路径、方法、请求体、响应体、状态码、错误文案逐字不变。
  P1 的验收方式 = 全量套件（含 `test_auth_security` / `test_admin_role` /
  `test_login_throttle_store` / `test_authz_ownership`）全绿 + `/openapi.json` 路由数不变。
- **不改前端**：前端只认 HTTP，路由搬家对它是透明的。
- **回滚 = 单次 git revert**：每个阶段一个独立提交，改动只落在该模块的新增目录 +
  该模块在 `main.py` 的那一段。禁止跨阶段混合提交。
- **临时兼容别名是显式的**：若某符号必须留在旧位置，写成一行 `X = new.X  # 过渡别名`，
  并在报告中登记「谁在引用、什么时候删」。不做隐式的 `import *`。
