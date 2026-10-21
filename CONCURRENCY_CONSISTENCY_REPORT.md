# 面试业务并发安全与数据库一致性改造报告

> 适用范围：`career-ai` 后端面试业务（`api/interview.py`、`api/interview_ws.py`、
> `services/interview_service.py`、`services/interview_core.py`、
> `services/interview_production.py`、`services/interview_orchestrator.py`、
> `models/interview.py`）
> 生产库：MySQL 8.0.45 + `aiomysql`（唯一支持的生产数据库）
> 测试库：SQLite 内存库（**仅** `backend/tests/*.py`）

本文件**按阶段增量编写**：
§1 是**改造前**的基线快照与兼容契约（先记录、后修改）；
§2 起是逐项改造、迁移方案、回滚策略、并发测试结果与剩余风险。

---

# §1 改造前基线（先记录，后修改）

> 本节的每一条都是**实测/读码核实**的结果，不是根据需求描述推断的。
> 凡与需求书用词不一致的地方，一律以**代码事实**为准，并显式标注差异。

## 1.1 持久化状态机：只有 3 个状态

`models/interview.py:44-46`：

```python
SESSION_STATUS_CREATED = "created"
SESSION_STATUS_ONGOING = "ongoing"
SESSION_STATUS_FINISHED = "finished"
```

`interview_session.status` 列：`String(20), nullable=False, default="created", index=True`。

### ★ 与需求书用词的差异（必须先对齐）

需求书提到 `created` / `in_progress` / `completed` / `cancelled` 四个状态。
**真实库里只有三个，且命名不同**：

| 需求书用词 | 项目真实状态 | 处置 |
|---|---|---|
| `created` | `created` | 同名同义 |
| `in_progress` | **`ongoing`** | 仅**命名差异**，语义等价 |
| `completed` | **`finished`** | 仅**命名差异**，语义等价 |
| `cancelled` | **不存在** | **不新增**。当前产品没有「取消面试」能力，新增状态需要同时定义「取消后报告怎么出 / 已作答数据怎么处理 / 是否允许重开」，属业务功能变更，不在本次范围 |

⇒ **本次不引入任何新状态、不改状态取值**。原因有两条：
1. `test_interview_lifecycle` / `test_interview_api` / `test_interview_ws` 共 3 套断言了
   `"created"` / `"ongoing"` / `"finished"` 字面量与状态流转；
2. 前端 `InterviewRoom.vue` 与 WS 快照的 `adopt_state()` 也按这三个值映射。

### 真实合法转移（读码穷举，共 8 条）

| # | 起点 | 触发 | 终点 | 附加条件 | 位置 |
|---|---|---|---|---|---|
| T1 | `created` | `POST /start`（或 `POST /ws` 的 `start`） | `ongoing` | 题量 > 0 | `_activate_session` |
| T2 | `ongoing` | `POST /answer`（未答完） | `ongoing` | `current_question_no += 1` | `submit_answer` |
| T3 | `ongoing` | `POST /answer`（答完最后一题） | `ongoing` | `current_question_no = total + 1`（**状态不变**，靠题号表达"答完"） | `submit_answer` |
| T4 | `ongoing` | `POST /end`（含提前结束） | `finished` | 至少 1 条作答 | `end_session` |
| T5 | `finished` | `POST /end` | `finished` | **幂等**：返回既有报告 | `end_session` |
| T6 | `finished` | `POST /start` | — | **400** `面试已结束，无法重新开始` | `_ensure_startable` |
| T7 | `ongoing` | `POST /start` | — | **400** `面试已在进行中，请直接获取当前题目` | `_ensure_startable` |
| T8 | `created` | `POST /end` | — | **400** `面试尚未开始，无法结束` | `end_session` |

**非法转移一律 400，不做静默收敛**（`submit_answer` / `generate_and_persist_next`
在非 `ongoing` 时也是 400）。`created` 状态提交作答同样是 400。

> ★ 注意 T3：**"全部答完"不是一个状态**，而是 `ongoing` + `current_question_no > total_questions`。
> 报告在 `end` 之前不会生成，所以不存在「答完但未结束」这个中间状态的持久化表示。
> 这一点决定了「重复提交导致题号被多推进」的影响面：题号一旦被推过 `total_questions`，
> 后续提交一律 400，**不会**静默丢题，但**会**让报告少一题。

## 1.2 进程内编排状态机：5 个状态（与 1.1 无关，不落库）

`services/interview_orchestrator.py:214-250`（**已核实为 5 个，不是 4 个也不是 6 个**）：

```python
STATE_WAITING_QUESTION  = "waiting_question"
STATE_AVATAR_SPEAKING   = "avatar_speaking"
STATE_WAITING_ANSWER    = "waiting_answer"
STATE_PROCESSING_ANSWER = "processing_answer"
STATE_FINISHED          = "finished"
STATES = (上述 5 个，顺序即生命周期顺序)
INITIAL_STATE = "waiting_question"
TERMINAL_STATES = ("finished",)
EVENT_* = question_ready / speech_done / answer_submitted / scored / all_answered / finish
```

`TRANSITIONS` 恰好 **10 条** = 5 条生命周期边 + 5 个 `finish` 逃生舱。
存储是 `_STATES: Dict[int, str] = {}`（第 318 行）+ `_STATES_LOCK`，**库中无对应列**。

★ **两套状态机的关系**：编排层状态是**内存态、可自愈、可丢失**；持久化状态是**权威**。
`adopt_state()` 在进程重启后按库中 `status` 保守重建（`created→waiting_question` /
`ongoing→waiting_answer` / `finished→finished`），**已有记录不改、未知状态不猜**。
⇒ 本次改造**不动编排层**，只保证「持久化状态推进」在多实例/并发下是确定的。

## 1.3 事务边界基线（4 个写入口 + 2 个"写但不改状态"入口）

| 入口 | 行锁 | 事务范围 | 外部 IO 是否在事务内 | commit 次数 |
|---|---|---|---|---|
| `create_session` | ❌ 无 | INSERT 会话（+ 幂等建 context） | 无 | 1 |
| `start_session` / `begin_session_with_questions` | ✅ `FOR UPDATE` | SELECT 会话 → 守卫 → INSERT N 题 + UPDATE 会话 | 无（规则出题是纯计算） | 1 |
| `submit_answer` | ✅ `FOR UPDATE` | SELECT 会话 → 守卫 → 查重 → 评分 → INSERT 作答 + UPDATE 题号 | 无（评分是纯规则） | 1 |
| `end_session` | ✅ `FOR UPDATE` | SELECT 会话 → 守卫 → 汇总 → INSERT 报告 + UPDATE 会话 | 无（报告是纯规则） | 1 |
| `generate_and_persist_next` | ❌ **无锁** | SELECT 会话 → 守卫 → **SELECT 题目** → 规则计划 → **调大模型（秒级）** → INSERT 题目 | ★ **有，且在事务内** | 1 |
| `generate_next_question`（按需出题） | ❌ 无锁 | **不落库**（除幂等建 context） | 有（LLM + 检索），但**不写业务行** | 0~1（仅 context） |

### ★ 已确认的两个真实缺陷

**缺陷 A：`generate_and_persist_next` 在事务里做大模型调用。**

调用链（`interview_service.py:1143-1240`）：
`_load_session`（SELECT，隐式开启事务）→ 状态校验 → `_load_questions`（SELECT）
→ `_rule_plan`（读库）→ `interview_next_flow.generate_candidate(...)`（**LLM + 向量检索，实测 8~19s**）
→ `db.add(row)` → `db.commit()`。

后果（MySQL 8 默认 `REPEATABLE READ`）：
- 一个**读事务**被持有 8~19 秒，期间 `InnoDB` 必须保留该一致性快照对应的 undo；
- 占用连接池连接 8~19 秒（`NullPool` 下等于多占一条物理连接）；
- **不阻塞写**（没有加锁读），所以症状不是"卡死"，而是"长事务 + 连接耗尽"——
  在并发用户数接近连接池上限时表现为**超时**，很难归因。

**缺陷 B：`generate_and_persist_next` 的并发去重依赖一条真实库里不存在的唯一约束。**

见 1.5。两个并发请求都会通过「① 幂等：该题号已落库？」的预检查，
然后各自 INSERT —— 真实 MySQL 上**两条都会成功** ⇒ 同一 `(session_id, question_no)` 出现两行。

## 1.4 既有幂等机制清单（4 类，逐条核实）

| 机制 | 位置 | 覆盖场景 | 真实有效性 |
|---|---|---|---|
| ① 唯一约束 | `InterviewAnswer.question_id` `unique=True` | 一题一答 | ✅ 真实库存在（`ix_interview_answer_question_id`） |
| ① 唯一约束 | `InterviewReport.session_id` `unique=True` | 一报告一会话 | ✅ 真实库存在 |
| ① 唯一约束 | `InterviewContext.session_id` `unique=True` | 一上下文一会话 | ✅ 真实库存在 |
| ① 唯一约束 | `InterviewQuestion(session_id, question_no)` `UniqueConstraint` | 一题号一行 | ❌ **真实库缺失**（模型声明了，库里没有） |
| ② 预检查 | `submit_answer:800-806` | 重复作答 | ✅ 有效，但**并发下是 TOCTOU**（靠 ① 兜底） |
| ② 预检查 | `generate_and_persist_next:1164-1172` | 重复补题 | ❌ 并发下无兜底（① 缺失） |
| ③ `IntegrityError` 收敛 | `submit_answer` / `_activate_session` / `end_session` / `generate_and_persist_next` | 并发写冲突 | ⚠️ 前 3 处依赖真实存在的约束 ⇒ 有效；第 4 处**依赖缺失的约束 ⇒ 死代码** |
| ④ 状态守卫幂等 | `end_session`（`finished` → 返回既有报告） | 重复结束 | ✅ 有效 |

★ 另外 **`create_session` 完全不做去重**：同一用户连点两次会得到两个会话。
这是**既有产品行为**（前端每次点"开始"就是新建一场），本次**不擅自改**。

## 1.5 ★ 真实 MySQL 核对结果（本轮最重要的发现）

实测对象：本机 MySQL 8.0.45 / 库 `career`（端口 3306 可连）。

```
interview_session : rows=2   unique=[PRIMARY]
interview_question: rows=10  unique=[PRIMARY]                       ← ★ 缺 (session_id, question_no)
interview_answer  : rows=1   unique=[PRIMARY, ix_interview_answer_question_id]
interview_report  : rows=0   unique=[PRIMARY, ix_interview_report_session_id]
interview_context : rows=1   unique=[PRIMARY, ix_interview_context_session_id]
重复数据检查       : 无重复 (session_id, question_no)
```

`information_schema.TABLE_CONSTRAINTS` 里的唯一约束全集：
`ix_interview_answer_question_id` / `ix_interview_context_session_id` /
`ix_interview_report_session_id` / `user.email` / `user.username` / `user_session.token`
—— **没有** `uq_interview_question_session_no`。

**根因**：`models/interview.py:113` 声明了
`UniqueConstraint("session_id", "question_no", name="uq_interview_question_session_no")`，
但 `Base.metadata.create_all` **只建新表、不给既有表补约束**；而
`utils/schema_sync.DESIRED_COLUMNS` 按设计**只能加列**（结构上写不出「加约束」）。
⇒ 该表是历史遗留表，约束从未被创建。

**为什么既有测试没发现**：`tests/test_interview_concurrency.py` 的 [2] 只断言
**模型声明了** `UniqueConstraint`，从不检查数据库 —— 这正是需求书警告的
"只靠模型声明/只靠 SQLite 推断真实库行为"。SQLite 测试用 `create_all` 建全新表，
所以**在测试里约束是存在的**，缺陷只在生产暴露。

## 1.6 并发控制清单（全部进程内，无跨实例协调）

| 结构 | 位置 | 存储 | 默认值 | 多 worker 下的行为 |
|---|---|---|---|---|
| `_STATES` + `_STATES_LOCK` | `interview_orchestrator.py:318` | `dict[int,str]` + `threading.Lock` | — | **各 worker 独立**，状态会分裂（但可自愈：无记录即按库中 `status` 重建） |
| `SessionTracker` | `interview_production.py:480` | `threading.Lock` + dict | `max=2000` | 各 worker 独立淘汰，容量上限 × N |
| `ConcurrencyGate` | `interview_production.py:590` | `threading.Lock` + dict | 3 会话/用户、4 连接/用户 | **实际上限 × N**（每个 worker 各自计数） |
| `RateLimiter` | `interview_production.py:715` | `threading.Lock` + dict | 作答间隔 0（不限制） | 实际速率 × N |
| WS ticket 表 | `api/interview_ws.py:93` `_ticket_lock` | `dict` + `threading.Lock` | TTL 30s | ★ **跨 worker 会失效**：ticket 在 A worker 签发、连接落到 B worker ⇒ 401/4401 |
| `worker_guard.assert_single_worker()` | `utils/worker_guard.py` | 环境变量 + 文件锁 | — | **启动即报错**（>1 worker 时） |

★ **当前部署立场是「单 worker」**，由 `worker_guard` 强制。因此上表所有进程内结构
在**当前部署形态下是自洽的**；本次改造**不引入 Redis**，而是：
1. 把「跨实例正确性」的**权威判据**全部下沉到数据库（唯一约束 + 条件更新），
   使**即使将来多 worker 也不会产生脏数据**；
2. 把进程内结构明确标注为「**仅用于限流/配额/体验，不承担正确性**」。

> 需求书禁止「用简单全局锁掩盖跨实例一致性问题」。本次的做法正好相反：
> **正确性靠数据库约束，进程内结构只做体验层**。唯一的跨实例硬阻塞项是 WS ticket，
> 已在 `PRODUCTION_CHECKLIST.md` 登记为「多实例必须改为共享存储」。

## 1.7 兼容契约（改造**不得改变**的行为）

以下每一条都有测试断言，改造后必须逐字保持：

| 契约 | 期望 | 断言位置 |
|---|---|---|
| 重复作答 | **400** + `第 {N} 题已作答，请勿重复提交` | `test_interview_lifecycle.py:299` |
| 重复 start（ongoing） | **400** + `面试已在进行中，请直接获取当前题目` | `_ensure_startable` 相关 |
| 重复 start（finished） | **400** + `面试已结束，无法重新开始` | 同上 |
| `created` 时 end | **400** + `面试尚未开始，无法结束` | `test_interview_lifecycle` |
| `finished` 时 end | **200** + 既有报告（**幂等**） | 同上 |
| 非本人 / 不存在 | **404**（不泄露存在性） | `test_authz_ownership`（23 项） |
| 题号越界提交 | **400** + `全部题目已作答，请调用 end 接口生成报告` | `submit_answer` |
| 未开始提交 | **400** + `面试尚未开始，请先调用 start 接口` | 同上 |
| `submit_answer` 返回形状 | `answer` / `session` / `next_question` / `all_answered` / `message` | `test_interview_api` |
| `generate_and_persist_next` 返回 | 恒定 5 键 + `source ∈ {llm, rule_fallback, existing, all_answered, failed}` | `test_avatar_llm_flow`（71 项） |
| WS 关闭码 | `4401` / `4403` / `4404` / `4408` / `4409` | `test_interview_ws`（102 项） |
| WS 事件/消息字面量 | 9 推 / 6 发 / 7 错误码 | 同上 |
| 总路由数 | **31** | 导入自检 |

## 1.8 现有并发测试的实际覆盖（诚实评估）

`tests/test_interview_concurrency.py`（144 行，5 节）实际做的事：

| 节 | 断言内容 | 是否真的验证并发 |
|---|---|---|
| [1] | SQL 方言编译出 `FOR UPDATE`（MySQL 有 / SQLite 无） | ❌ 只验证 SQL 文本 |
| [2] | 模型声明了 `UniqueConstraint` | ❌ **只验证声明，不验证数据库** |
| [3] | 源码里出现 `_load_session_for_update` | ❌ 源码字符串 |
| [4] | 4 个写入口都调用了它 | ❌ 源码字符串 |
| [5] | SQLite 上重复 start 返回 400 | ❌ 单线程、且 SQLite 无行锁 |

⇒ **这套件不能证明任何并发性质**，也没有覆盖需求书点名的
「重复作答 / 同时结束 / start 重放 / AI 超时 / DB 提交失败」五类场景。
本次改造必须补齐，且**必须有一部分跑在真实 MySQL 上**。

> ★ §1 全部是**改造前**的快照，故意保持原样（用于对照）。
> 缺陷 A / B 的修复与验证见 §6.5 的缺陷 ② / ①；
> 另有第三个缺陷（`create_context` 的 TOCTOU）不在本节，它是改造过程中
> **在真实 MySQL 上才暴露**的，见 §6.5 缺陷 ③。

---

# §2 幂等机制设计（作用域 / 过期 / 并发行为）

## 2.1 设计原则：用「业务自然键」而不是「客户端幂等键」

| 方案 | 需要的额外设施 | 本项目是否采用 |
|---|---|---|
| 客户端 `X-Idempotency-Key` + 指纹表 | 新表 + 请求指纹 + TTL 清理 + 并发插入唯一约束 + 前端改造 | ❌ 不采用 |
| **业务自然键 + 数据库唯一约束** | 只需一条唯一索引（多数已有） | ✅ 全部走这条 |

理由：本业务里**每一次"重复"都是同一业务对象上的重复动作**，自然键已经唯一确定了
"这是同一次操作"——`(session_id, question_no)` 唯一确定"这一题"，
`question_id` 唯一确定"这一题的作答"，`session_id` 唯一确定"这一场的报告/上下文"。
引入客户端幂等键不会带来额外正确性，只会多一张需要清理的表。

真正需要客户端幂等键的是**创建型接口**（无自然键）。本项目唯一的创建型接口是
`create_session`，而它的产品语义就是"每点一次开始就新建一场" ⇒ **不需要幂等**
（见 1.4 末尾，本次不擅自改）。

## 2.2 逐操作幂等表

| 操作 | 幂等键（数据库层） | 作用域 | 并发行为（实测） | 失败语义 |
|---|---|---|---|---|
| `start_session` | `interview_session.id` + `status` 守卫 | 单会话 | `FOR UPDATE` 串行化；后到者见 `ongoing` → **400** `面试已在进行中`；**题数不翻倍**（MySQL 运行器 [7]） | 400，不产生副作用 |
| `submit_answer` | `ix_interview_answer_question_id`（一题一答） | 单题 | `FOR UPDATE` 串行化 ⇒ 两条并发各答一题、题号 1→2→3、**恰好 2 条作答**（[4]） | 同题重复 → **400** `第 N 题已作答，请勿重复提交` |
| `generate_and_persist_next` | **`uq_interview_question_session_no`**（本次迁移新建） | 单会话单题号 | 无锁；两路**都**调模型（浪费一次调用），写库时一路 `source=llm`、一路 `source=existing`，**拿到同一行 id**（[5]） | 模型失败 → `rule_fallback`；双失败 → `ok=False`，**不抛异常** |
| `end_session` | `ix_interview_report_session_id`（一报告一会话） | 单会话 | `FOR UPDATE` 串行化；后到者直接返回既有报告，**报告表只有 1 行、两条拿到同一 id**（[6]） | `created` 时 → **400**；`finished` 时 → **200 + 既有报告**（幂等） |
| `create_context` | `ix_interview_context_session_id`（一上下文一会话） | 单会话 | 原子 upsert：`ON DUPLICATE KEY UPDATE`（MySQL）/ `ON CONFLICT DO NOTHING`（SQLite），**不抛异常、不 rollback**（[13]） | 参数非法 → 400/404；并发冲突 → 收敛为既有行 |

## 2.3 过期策略：没有 TTL，绑定业务对象生命周期

本设计**刻意不引入带 TTL 的幂等记录表**，幂等约束随业务对象一起存续
（会话被删除/归档时约束自然失效）。这是**强幂等**而非"窗口幂等"：

- 好处：不存在"窗口过期后同一操作被重新执行"的漏洞——这类漏洞在生产上极难复现。
- 代价：唯一索引长期占用空间（每行几十字节，可忽略）。
- 唯一"会过期"的幂等对象是 **WS ticket**（TTL 30s），它是鉴权凭证不是业务幂等键，见 §4。

## 2.4 幂等的代价：允许"重复计算、单次落库"

`generate_and_persist_next` 的并发语义明确为：**两路都会真的调用一次大模型，
只有一路的写入生效**。这是有意接受的取舍：

- 若要在调用模型**之前**就去重，就必须持锁跨越 8~19 秒的模型调用（违反需求书第 6 条）；
- 若要事后去重，就无法避免"算了两次"；
- 现状：**多花一次模型调用，换"绝不持锁跨越外部 IO"**。这是正确的方向——
  模型调用是幂等的外部计算，重复调用只花钱，不产生脏数据。

---

# §3 锁选型：逐路径决策与理由

## 3.1 数据库能力盘点（实测，不是假设）

| 能力 | 结论 | 取证 |
|---|---|---|
| 版本 | MySQL **8.0.45** | 运行器 [2.1] |
| 默认隔离级别 | **REPEATABLE-READ** | 运行器 [2.2]（`SELECT @@transaction_isolation`） |
| 行锁 | `SELECT … FOR UPDATE` 生效 | 运行器 [4]：两条并发提交被串行化 |
| 唯一索引 | 见 1.5 + 本次新增 `uq_interview_question_session_no` | 运行器 [1.2] / [3] |
| SQLite 上的 `FOR UPDATE` | **空操作** | SQLite 方言编译时被丢弃 ⇒ **SQLite 测试无法推断 MySQL 并发行为** |

## 3.2 决策表（为什么不选别的）

| 路径 | 读-改-写？ | 中间有外部 IO？ | **选中手段** | 为什么不选其它 |
|---|---|---|---|---|
| `submit_answer` | 是（作答 + 题号） | 否（评分是纯规则） | `FOR UPDATE` 行锁 | 条件更新需要"旧题号"参与判定；乐观锁要加 `version` 列（改模型 + 迁移 + 前端契约） |
| `start_session` | 是（状态 + N 题） | 否（规则出题是纯计算） | `FOR UPDATE` | 同上；且它要一次写 N 行，必须与状态变更同事务 |
| `end_session` | 是（状态 + 报告） | 否（报告是纯规则） | `FOR UPDATE` | 同上 |
| `generate_and_persist_next` | **否**（只追加一行） | **是，8~19s 模型调用** | **唯一约束 + `IntegrityError` 收敛，不加锁** | ① 持锁 ⇒ 锁 8~19s，连接池被拖死；② 乐观锁 ⇒ 保护的是"更新"不是"追加"，且冲突后仍要重试模型 |
| `create_context` | **否**（只追加一行） | 否 | **原子 upsert** | ① 先查后插 ⇒ TOCTOU（本次修的就是它）；② `FOR UPDATE` 父行 ⇒ 会与 `submit_answer` 抢同一把锁，把两个无关操作串行化 |
| `get_current_question` | 否（纯读） | 否 | 无锁 | 读不需要锁；缺行用**自愈**而不是报错 |

## 3.3 为什么不用版本号 / 乐观锁

1. **要加列**：`interview_session.version` → 改模型 + 进 `schema_sync.DESIRED_COLUMNS` + 生产 ALTER + 前端可能受影响；
2. **解决不了本次的两个真问题**：本次的问题是「**追加行**重复」和「**持锁跨越模型调用**」，
   乐观锁两者都管不到（它保护的是"更新丢失"，而更新的那三条路径已经被 `FOR UPDATE` 正确串行化了）；
3. **冲突后要重试**：乐观锁的失败路径是"重试整个业务动作"，而这里重试意味着**再调一次模型**。

## 3.4 为什么不用全局锁

需求书明确禁止。技术上也不成立：
- 全局锁把"每会话并发"降级为"全站串行"，直接摧毁产品可用性；
- 进程内全局锁**跨不了实例**，一旦多 worker 就同时失效——等于用假安全性换真性能损失。

本次的做法相反：**正确性判据全部下沉到数据库约束**，进程内结构只做体验层（§4）。

## 3.5 死锁分析

显式行锁只出现在 `_load_session_for_update`（锁 `interview_session` 单行）。
所有加锁路径的顺序都是「**先锁 session，再写子表**」，不存在"先锁子表再锁 session"的反向路径
⇒ 不构成环。`create_context` 不取显式行锁（upsert 的锁是 InnoDB 内部的单记录锁）。

实测：MySQL 运行器 32 项并发场景（含两条并发 end / 两条并发 start / 两条并发补题）
**未出现 `1213 Deadlock found`**。这是实测结论，不是推理结论。

---

# §4 多 worker / 多实例审计

## 4.1 现状：单 worker 是硬约束，且已被强制

`utils/worker_guard.assert_single_worker()` 在启动时读取
`WEB_CONCURRENCY` / `UVICORN_WORKERS` / `GUNICORN_WORKERS`，任一 > 1 即
**抛 `RuntimeError` 阻断启动**；随后尽力取 `instance/.app.lock` 文件锁兜底
（Windows `msvcrt.locking` / POSIX `fcntl.flock`，非阻塞）。

## 4.2 逐结构审计

| 进程内结构 | 多 worker 下的后果 | **是否影响正确性** | 处置 |
|---|---|---|---|
| `interview_orchestrator._STATES` | 各 worker 独立，编排状态分裂 | ❌ 不影响 | 无记录时按库中 `status` 重建 ⇒ **可自愈**；不进权威路径（`test_interview_concurrency [12.2]` 用 AST 断言 Service 不 import 它） |
| `SessionTracker`（`max=2000`） | 容量上限 × N，淘汰节奏不同 | ❌ 不影响 | 仅做会话跟踪/清理，不参与判定 |
| `ConcurrencyGate`（3 会话/用户、4 连接/用户） | **实际上限 × N** | ❌ 不影响 | 仅体验/资源保护；被绕过不会产生脏数据 |
| `RateLimiter`（作答间隔默认 0） | 实际速率 × N | ❌ 不影响 | 同上 |
| WS ticket 表（`_ticket_lock`，TTL 30s） | ★ **跨 worker 直接失效**：A 签发、B 校验 ⇒ `4401` | ⚠️ **影响可用性（不是数据一致性）** | 已在 `PRODUCTION_CHECKLIST.md` 登记为"多实例必须改共享存储" |
| `worker_guard` | 启动即报错 | — | 兜底 |

## 4.3 结论：正确性已完全下沉到数据库

| 正确性判据 | 载体 |
|---|---|
| 一题号一行 | 唯一索引 `uq_interview_question_session_no` |
| 一题一答 | 唯一索引 `ix_interview_answer_question_id` |
| 一报告一会话 | 唯一索引 `ix_interview_report_session_id` |
| 一上下文一会话 | 唯一索引 `ix_interview_context_session_id` |
| 状态转移合法性 | `interview_session.status` 列 + `FOR UPDATE` 条件读 |
| 题号单调 | `interview_session.current_question_no` 列 |

⇒ **即使明天改成多 worker，也不会产生重复题、重复评分、重复报告、状态回滚**；
会退化的只有"限流精度"和"WS ticket 命中率"。

## 4.4 什么时候才该引入 Redis（明确触发条件）

只有出现下列需求之一时才引入，且**只外置对应的那一项**，不要顺手把所有状态都搬过去：

1. 需要多实例部署（此时 WS ticket 必须外置，这是**唯一的硬阻塞项**）；
2. 需要全局限流/配额（当前 `ConcurrencyGate` 是每 worker 计数）；
3. 需要跨实例的会话空闲超时清理（当前由 `interview_production` 机会式清理）。

**本次不引入 Redis**：单 worker 部署下它不解决任何当前存在的问题，只会增加一个
必须运维的有状态组件。

---

# §5 迁移方案与回滚策略

## 5.1 变更清单

**数据库（唯一一项 DDL）**

| 变更 | 脚本 | 幂等 | 可回滚 |
|---|---|---|---|
| `interview_question` 增加 `UNIQUE (session_id, question_no)`，名 `uq_interview_question_session_no` | `backend/scripts/migrate_interview_question_unique.py` | ✅ 已存在则 `[SKIP]` | ✅ `--drop` |

**代码（不改任何对外契约）**

| 文件 | 改动 | 性质 |
|---|---|---|
| `services/interview_service.py` | 新增 `_end_read_transaction` / `_insert_question_row` / `_recover_missing_question`；`generate_and_persist_next` 重排事务边界；4 处 `rollback()` 后访问过期属性修复 | 收窄事务 + 消除 `MissingGreenlet` |
| `services/interview_next_flow.py` | 拆成 `prepare_candidate`（只读）+ `generate_from_prepared`（**不接收 `db`**） | 让"不可能开事务"成为**类型层面的保证** |
| `services/interview_context.py` | `create_context` 的 TOCTOU 改为原子 upsert | 并发收敛 |
| `tests/test_interview_concurrency.py` | 从 5 节扩到 **13 节 / 59 项** | 测试 |
| `tests/interview_mysql_concurrency_run.py` | **新增**真实 MySQL 运行器（11 节 / 32 项） | 测试 |
| `tests/test_agent_full_input.py` | `FROZEN_SOURCES` 中 `interview_context.py` 的源码指纹同步 | 守卫维护 |

## 5.2 迁移脚本用法

```bash
cd backend
python scripts/migrate_interview_question_unique.py --dry-run   # 只查重
python scripts/migrate_interview_question_unique.py --yes       # 执行
python scripts/migrate_interview_question_unique.py --drop      # 回滚
```

安全设计：
- **有重复就不动**（exit 1），并打印前 20 组重复明细；
- `--dedupe` 才会自动清理，且**先检查重复行是否被 `interview_answer` 引用**，
  有引用直接 exit 3 拒绝（绝不删掉已有作答）；
- 约束已存在时 `[SKIP]`，可反复执行。

## 5.3 迁移执行结果（真实库，四重验证）

| 验证 | 结果 |
|---|---|
| 迁移前查重 | 无重复 `(session_id, question_no)` |
| `--dry-run` | `[DRY-RUN] 无重复，可安全添加约束` |
| `--yes` | `[OK] 已添加约束 uq_interview_question_session_no` |
| `SHOW INDEX` 复核 | 出现 `uq_interview_question_session_no`（seq 1 = `session_id`，seq 2 = `question_no`） |
| 重复插入探针 | `(1062, "Duplicate entry '7-1' for key 'interview_question.uq_interview_question_session_no'")` |
| 行数不变 | `interview_question` 仍为 10 行 |
| 运行器复核 | `[1.2] 真实库含 uq_interview_question_session_no` PASS |

## 5.4 回滚策略

| 层 | 回滚动作 | 数据影响 |
|---|---|---|
| 约束 | `--drop`（`ALTER TABLE interview_question DROP INDEX uq_interview_question_session_no`） | 无数据变更，秒级 |
| 代码 | `git revert` 本次提交 | **无 DB 依赖**：新代码在约束缺失时只是退化为"可能重复行"（= 改造前行为），不会崩溃 |
| 数据 | **不需要**。本次没有删除/改写任何业务行 | — |

★ 关键性质：**代码与约束可以各自独立回滚**。因为新代码把"约束冲突"当作一种
正常收敛路径（`IntegrityError` → 重读既有行），而不是把它当断言。

## 5.5 上线顺序

1. **先跑迁移**（可反复执行、失败不影响服务）；
2. **再发代码**。

为什么这个顺序安全：旧代码**不依赖**该约束（它压根没指望约束存在，所以才会插出重复行）；
新代码**依赖但不假设**它存在（缺失时只是退化为旧行为）。反过来（先发代码后迁移）
也安全，只是期间并发补题仍可能插重复行。

★ 注意：`ALTER TABLE … ADD CONSTRAINT … UNIQUE` 在大表上是一次 **ONLINE DDL**
（MySQL 8 通常 `ALGORITHM=INPLACE, LOCK=NONE`），但仍建议在低峰执行，并先确认磁盘余量。

---

# §6 并发测试结果

## 6.1 三层测试矩阵（各层能证明什么，不许混为一谈）

| 层 | 文件 | 项数 | 能证明 | **不能**证明 |
|---|---|---|---|---|
| 逻辑收敛（SQLite 内存库） | `tests/test_interview_concurrency.py` | **59** | 守卫口径 / 幂等 / 收敛分支 / 事务边界（AST）/ 跨方言 SQL 形态 | 行锁语义、真并行 |
| 真实并发（MySQL 8） | `tests/interview_mysql_concurrency_run.py` | **32** | 唯一约束真的挡重复、`FOR UPDATE` 真的串行化、模型调用期间真的无事务 | 跨实例（单进程多连接） |
| 全量回归 | `tests/test_*.py` | **72 套全绿** | 没有回归 | — |

## 6.2 SQLite 逻辑层（59 项，13 节）

| 节 | 主题 |
|---|---|
| [1] | 持久化状态机：真实只有 3 个状态，本次**不新增** |
| [2] | 唯一约束：查**建出来的库**（`get_unique_constraints` + `get_indexes` 双入口，因为 SQLite 把 `unique=True` 建成唯一索引） |
| [3] | 事务边界：AST 取证"模型调用期间不得有事务开着" |
| [4] | `_end_read_transaction` 的安全闸（脏会话调用必须抛 `RuntimeError`） |
| [5] | 重复作答：400 + 稳定文案，不产生重复行 |
| [6] | 重复补题：幂等返回既有行 |
| [7] | 重复结束：幂等返回既有报告 |
| [8] | 重复开始：400 + 题数不翻倍 |
| [9] | 模型失败/超时：规则兜底，不留半截数据 |
| [10] | 提交冲突：`IntegrityError` 收敛为 `existing` |
| [11] | 缺行自愈：会话推进了但题目缺失 ⇒ 自动补回 |
| [12] | 进程内结构不参与正确性判定（用 AST 取 import，**不用子串匹配**——docstring 会误伤） |
| [13] | **`create_context` 并发收敛**：AST 取证 + **跨方言编译 SQL** + 未知方言响亮失败 + 全程不调 `db.rollback()` |

[13.3] 是本次新增里最"值钱"的一条：在 SQLite 套件里把 **MySQL 分支**的语句
真的编译出来，断言含 `ON DUPLICATE KEY UPDATE`。否则"生产方言的 SQL 形态"
永远只有上线后才第一次被执行。

## 6.3 真实 MySQL 运行器（32 项，11 节，全部 PASS）

```
[1]  生产库结构：真实库含 uq_interview_question_session_no
[2]  MySQL 8 / 默认隔离级别 REPEATABLE-READ
[3]  唯一约束真的挡重复：两个并发 INSERT 只有 1 条成功，另一个 IntegrityError
[4]  并发提交作答：两条都成功、恰好 2 条作答、题号推进到 3（无跳号）
[5]  并发补题：只多 1 行、一条 source=llm 一条 source=existing、同一行 id
[6]  并发结束：都拿到报告、报告表 1 行、同一份、会话停在 finished
[7]  并发 start：恰好一条成功、另一条 400、题数不翻倍
[8]  模型超时：不打断答题、下一题降级规则兜底、恰好 2 行无半截数据
[9]  真实 IntegrityError 被收敛成 existing（不抛 409）、行数不变
[10] 缺行自愈：get_current_question 自愈而不是 500、题号重新连续 1..3
[11] 事务边界：★ 模型调用期间业务会话 db.in_transaction() == False
```

安全性：运行器 **CREATE 一个独立 schema**（`career_conc_probe_<pid>_<ts>`）、
在里面 `create_all`、跑完 `DROP`；生产库**只做一次只读约束检查**。

## 6.4 全量回归

`backend/tests/test_*.py` 共 **72 套**，本次改动后**全部通过**。
其中直接相关的：`test_interview_concurrency`（59）、`test_interview_context`（55）、
`test_interview_context_pack`、`test_avatar_llm_flow`（71）、`test_agent_full_input`（104）、
`test_interview_production`、`test_interview_ws`（102）。

## 6.5 本次发现并修复的 3 个真实缺陷（按发现顺序）

**缺陷 ①（P0）真实 MySQL 缺失 `uq_interview_question_session_no`。**
见 1.5。修复：迁移脚本 + 四重验证。若不修，`IntegrityError` 收敛分支是**死代码**，
并发补题会插出重复行。

**缺陷 ②（P0）`generate_and_persist_next` 在事务内做 8~19 秒的模型调用。**
修复：`prepare_candidate`（只读）→ `_end_read_transaction` → `generate_from_prepared`
（**刻意不接收 `db`**，让"开事务"在结构上不可能）→ `_insert_question_row`。
真实库取证：`[11.3] 模型调用期间业务会话没有活跃事务`。

**缺陷 ③（P1）`interview_context.create_context` 是 TOCTOU。**
修复前，并发补题的其中一路会抛
`Duplicate entry '1' for key 'interview_context.ix_interview_context_session_id'`。

★ 这个缺陷有**两层**，只修第一层会引入更隐蔽的第二层：

1. 第一层：先 `SELECT` 判存在、再 `INSERT`，中间无锁 ⇒ 并发必撞唯一索引。
2. 第二层（**修第一层时踩到的坑**）：天真的修法是 `except IntegrityError → db.rollback()`。
   但 `Session.rollback()` 会**过期本事务内所有 ORM 实例**，调用方
   （`interview_context_pack.build_context_pack`）随后读 `answer.question_id`
   就触发惰性加载 ⇒ 异步下抛 `MissingGreenlet`（被 SQLAlchemy 包成 `StatementError`）。
   实测到的正是这一层：异常类型从 `IntegrityError` 变成了 `StatementError`，
   而 `[5.2] 只多出 1 行` 仍然是 PASS ——**如果不看异常全文，很容易误判成"已经修好了"**。

最终修复：改成**原子 upsert**（`ON DUPLICATE KEY UPDATE` / `ON CONFLICT DO NOTHING`）
+ **提交后重读**（commit 结束事务 ⇒ 新事务取新快照 ⇒ 必然看到对方已提交的行）。
全程**不 rollback**，因此不会打坏调用方的 ORM 状态。[13.7] 用 spy 断言
`db.rollback()` 调用次数为 0。

**附带修复（非并发问题但同源）**：4 处 `rollback()` 之后访问过期 ORM 属性
（`session.id` / `current.id` / `current.question_no`）导致的 `MissingGreenlet`。
统一改成"先取普通值再 rollback"。

## 6.6 测试自身的有效性自检（防"因错误的原因通过"）

本次在写测试时踩到并修正的**假通过**：

| 现象 | 真相 | 修正 |
|---|---|---|
| `[5.2]` 第二次提交"应该 400 重复作答" | 题号已推进 ⇒ 第二次答的是第 2 题，**不是重复** | 先用 `[5.3]` 手动回退题号，才测"重复" |
| `[8.3]` 未作答就 end 期望 200 | 实际是 **400** `尚未提交任何回答，无法生成面试报告` | 改断言 |
| `[2.2]/[2.3]` SQLite 上查不到 `unique=True` 的约束 | SQLite 把它建成**唯一索引**，`get_unique_constraints` 不报 | 两个入口都收 |
| `[12.2]` 断言 Service 不依赖 orchestrator | 原用子串匹配，**docstring 里提到过**就误判 | 改用 AST `_imported_modules` |
| `[13.3]` 想用 `literal_binds` 编译 | JSON 列没有字面量渲染器（`CompileError`） | 只编译 SQL 骨架 |

★ 每一条新断言都问过"**什么输入能让它失败**"：
[13.1] 旧代码（`db.add`）会失败；[13.3] 删掉 MySQL 分支会失败；
[13.6] 返回自己构造的 values 会失败；[13.7] 走 rollback 会失败。

---

# §7 剩余风险

| # | 风险 | 等级 | 触发条件 | 缓解 / 现状 |
|---|---|---|---|---|
| 1 | **WS ticket 跨 worker 失效** | **P1** | 多实例部署 | `worker_guard` 阻断多 worker；`PRODUCTION_CHECKLIST.md` 已登记"多实例必须改共享存储" |
| 2 | 真实 `SPARK_API_SECRET` 在 Git 历史 + `.env` | **P0**（第 1 轮遗留） | 仓库泄露 | **必须先轮换密钥**；历史清理未执行 |
| 3 | `ConcurrencyGate` / `RateLimiter` 上限在多 worker 下 × N | P2 | 多 worker | 只影响体验/资源，不影响数据正确性 |
| 4 | 缺行自愈用**规则题**重建 | P2 | 题目行被删 + 会话已推进 | 有意选择：规则题是**确定性纯函数**，与丢失前逐字段相同；若改用模型重出，等于把"丢题"变成"静默换题"（更糟）。已记录在 `_recover_missing_question` docstring |
| 5 | `create_session` 不去重（连点两次得两个会话） | P3 | 用户连点 | **既有产品行为**，本次不擅自改 |
| 6 | 并发补题会**重复调用一次模型** | P3 | 同一会话并发补题 | 有意取舍（§2.4）：花钱换"绝不持锁跨越外部 IO" |
| 7 | 真实 MySQL 运行器**不在回归循环里** | **P1** | 换机器 / CI | 它需要可连的 MySQL 8。建议接入 CI 的 MySQL service 容器；在此之前，"真实并发行为"靠**人工在发版前跑一次**保证 |
| 8 | 单 worker 是**硬约束** | P1 | 想水平扩展 | 明确写在 `worker_guard` 报错信息里；扩展前必须先做 §4.4 的三件事 |
| 9 | 迁移脚本的 `--dedupe` 用 `scalar_one_or_none()` 检查引用 | P3 | 同一题号被多条作答引用（正常不会，因为一题一答） | 生产数据上不会触发；真触发会抛异常而不是误删（失败方向是安全的） |
| 10 | 其它入口仍是"短事务 + 行锁" | P3 | 极高并发同会话 | `FOR UPDATE` 会把同会话请求串行化，**正确但有排队**；这是有意的（同会话本就该串行） |

## 7.1 明确"没有解决"的

- **跨实例**：本次只做到"多 worker 不产生脏数据"，**没有**做到"多 worker 功能完整"
  （WS ticket / 限流仍需共享存储）。
- **`create_session` 幂等**：不做（产品语义决定）。
- **CI 里的真实 MySQL**：没有接入，只是提供了运行器。

---

# §8 交付清单

| 类别 | 文件 |
|---|---|
| 报告 | `CONCURRENCY_CONSISTENCY_REPORT.md`（本文件） |
| 迁移 | `backend/scripts/migrate_interview_question_unique.py` |
| 业务代码 | `backend/services/interview_service.py`、`backend/services/interview_next_flow.py`、`backend/services/interview_context.py` |
| 测试（逻辑层） | `backend/tests/test_interview_concurrency.py`（59 项） |
| 测试（真实库） | `backend/tests/interview_mysql_concurrency_run.py`（32 项，**不进回归循环**） |
| 测试（守卫维护） | `backend/tests/test_agent_full_input.py`（`FROZEN_SOURCES` 指纹） |

**验收口径**：`cd backend && for t in tests/test_*.py; do python "$t" || echo FAIL $t; done`
应 72 套全绿；`python tests/interview_mysql_concurrency_run.py` 应 32 项全 PASS。

---

# §9 专项：WebSocket ticket 的多实例一致性

> 本节是 §4（多 worker 审计）里唯一「硬阻塞项」的专项修复记录。
> 结论先行：**ticket 已不再依赖进程内状态**；`worker_guard` **仍然保留**（理由见 9.6）。

## 9.1 审计：真实调用链（不是按变量名推断）

### ticket 的五个动作

| 动作 | 真实位置 | 改造前实现 | 问题 |
|---|---|---|---|
| **签发** | `api/interview_ws.py:create_ws_ticket_endpoint` → `create_ws_ticket(session_id, user_id)` | `_ticket_store[ticket] = {...}` | 写进**本进程**的 dict |
| **验证 + 消费** | WS 握手 `?ticket=` → `verify_ws_ticket(ticket, expected_session_id)` | `with _ticket_lock:` 读 → 判 `used` → 判 `exp` → 判 `session_id` → `pop` | 同一把进程内锁内完成，"原子"只在单进程内成立 |
| **过期** | `_TICKET_TTL_SECONDS = 30` + `time.monotonic()` | 存在 dict 的 `exp` 字段里 | 进程重启即全部丢失 |
| **清理** | `_sweep_tickets()` + `_MAX_TICKETS = 5000` 淘汰 | 每次签发时扫一遍 dict | 每个进程各扫各的 |
| **撤销** | **不存在** | — | ★ 审计发现的缺口：**没有撤销能力**，只能等 30 秒自然过期 |

调用链（签发侧）::

    POST /api/interview/{sid}/ws-ticket
      → deps.get_current_user           (Bearer → user_session 有效)
      → interview_service.get_session_detail(user_id, sid)   (归属校验，非本人 404)
      → create_ws_ticket(sid, user_id)  (进程内 dict)

调用链（校验侧）::

    WS /api/interview/{sid}/ws?ticket=…
      → accept()                        (先 accept，为了能给明确关闭码)
      → origin_allowed(Origin, Host)    (跨站 → 4403)
      → verify_ws_ticket(ticket, sid)   (进程内 dict；失败 → 4401)
      → interview_service.get_session_detail(...)   (再校验一次归属 → 4404)
      → ConcurrencyGate / SessionTracker / Mutex / RateLimiter

### 绑定关系（真实语义）

- ticket 绑定 **`(session_id, user_id)` 两个值**，但校验时**只比对 `session_id`**：
  `user_id` 直接由 ticket 决定（不需要比对）。所以「绑定错误的用户」在本设计里
  不是"比对失败"，而是"根本拿不到别人的 ticket"——签发端点已经做了归属校验。
- **没有**把 ticket 绑到 `user_session` 行（token）上 ⇒ ticket 路径无法复核会话撤销，
  `_auth_still_valid` 只能退化为"用户仍存在"（既有已登记限制，本次不扩大）。

### `worker_guard` 的触发条件

`utils/worker_guard.assert_single_worker()`（`main.lifespan` 第一行）：

1. 读 `WEB_CONCURRENCY` / `UVICORN_WORKERS` / `GUNICORN_WORKERS`，任一 **> 1** ⇒
   抛 `RuntimeError` 阻断启动；
2. 再尽力取 `instance/.app.lock` 文件锁（Windows `msvcrt.locking` / POSIX `fcntl.flock`），
   同一主机起第二个进程 ⇒ 报错。

### 依赖进程内字典的关键路径（全量）

| 结构 | 位置 | 多实例后果 | 是否影响**数据正确性** |
|---|---|---|---|
| `_ticket_store` | `api/interview_ws.py`（已删除） | A 签发 B 校验 ⇒ 随机 4401 | 可用性（本次修复对象） |
| `orch._STATES` | `services/interview_orchestrator.py` | 状态分裂，但 `adopt_state` 按库中 `status` 自愈 | 否 |
| `SessionTracker` | `services/interview_production.py` | 各进程独立淘汰 | 否 |
| `ConcurrencyGate` | 同上 | 配额 × N | 否 |
| `RateLimiter` / `Mutex` | 同上 | 限流 × N / 互斥只在进程内 | 否（答题有行锁 + 唯一约束兜底） |
| `login_throttle._failures` | `utils/login_throttle.py` | **登录爆破限流被放大 N 倍** | **是（安全项）** |

### ★ 审计中额外发现的一个真实缺陷（与 ticket 同文件）

`api/interview_ws.py` 顶部 `from datetime import date, datetime, time` 与
`import time`（原第 87 行）**名字相撞**，模块级最终绑定是 `time` 模块。于是
`_json_default` 里的 `isinstance(value, (datetime, date, time))` 在遇到**非日期时间**
的值时会抛::

    TypeError: isinstance() arg 2 must be a type, a tuple of types, or a union

而不是本该给出的「无法序列化 X」。日期时间值仍能正常序列化（CPython 逐项比对，
不会碰到那个非法成员），所以这个 bug 一直被掩盖着。本次把 `datetime.time`
改名为 `dtime`（3 处），错误信息恢复正确。**行为不变**，只是错误路径终于说人话。

## 9.2 选型：为什么是 MySQL，不是 Redis

| 方案 | 原子性 | 新增运维 | 与现有基础设施 | 结论 |
|---|---|---|---|---|
| **MySQL 表 + 条件更新** | InnoDB 行锁 + `rowcount` | **零**（已有 MySQL 8） | 已有唯一索引/事务/备份体系 | ✅ **采用** |
| Redis `SET NX` / `GETDEL` | 最强 | 一个新中间件 + 高可用 + 新故障域 | 项目里**没有** Redis | ❌ 为 30 秒临时凭证引入不成比例 |
| 进程内字典 | 仅单进程 | 零 | 就是现状 | ❌ 本次要修的问题 |

判据是**数据形状**：ticket 是「单行、短时、一次写一次消费、按主键等值访问」，
正是 InnoDB 行锁最擅长的场景；没有任何"高并发读多写多"的诉求需要 Redis 的内存速度。
**等真的需要多实例全局限流/配额时再一并引入 Redis**（§4.4 的三个触发条件）。

## 9.3 数据模型与原子消费

新表 `ws_ticket`（`models/user.py`，`models.WsTicket`）：

| 列 | 类型 | 作用 |
|---|---|---|
| `id` | INT PK | — |
| `ticket_hash` | VARCHAR(64) **UNIQUE** | `sha256(ticket)`；**只存摘要，不存明文** |
| `user_id` | INT | 消费成功后返回的身份 |
| `session_id` | INT | **绑定**，参与消费条件 |
| `created_at` / `expires_at` | DATETIME | 短时有效（默认 30s）；`expires_at` 建索引供清理 |
| `used_at` | DATETIME NULL | 「已不可再用」（被消费**或**被撤销） |

**原子消费**（`services/ws_ticket_store.consume`）::

    1. SELECT … WHERE ticket_hash = sha256(ticket)     → 拿 user_id / session_id
    2. 比对 session_id（不符 ⇒ 直接失败，**不写入**，因此不消耗）
    3. UPDATE ws_ticket SET used_at = :now
        WHERE ticket_hash = :digest
          AND used_at IS NULL            ← 只有第一个能匹配
          AND expires_at > :now
          AND session_id = :expected
    4. await db.commit()                 ← ★ 立刻提交，释放行锁
    5. rowcount == 1 ? 返回 user_id : 返回 None

为什么 `rowcount` 是唯一权威：

- `UPDATE` 是**当前读**（locking read），在 `REPEATABLE READ` 下也读最新已提交版本
  ⇒ **不依赖**事务快照，不需要"先查后写"的进程内判断；
- 两个 worker 同时对同一行 UPDATE，InnoDB 行锁把二者串行化：后到者执行时
  `used_at` 已非 NULL ⇒ `rowcount == 0`；
- 第 4 步的 `commit` 是**必须的**：WS 的 `db` 会随连接存活数分钟，不提交会把行锁
  握到连接结束（这正是本项目在别处踩过的"长事务"坑）。

为什么 `used_at` 而不是"消费即删"：能区分「用过」与「从不存在」，便于排查重放；
过期行由 `sweep_expired` 机会式清理（节流 60 秒，进程内时间戳只影响"多久清一次"）。

## 9.4 失败策略：安全失败，绝不降级

| 场景 | 行为 | 为什么不是别的 |
|---|---|---|
| 签发时存储不可用 | HTTP **503** + `ws_ticket_issue_unavailable`(ERROR) | 不能发一张注定验不过的票；500 会被当成代码 bug |
| 握手时存储不可用 | `error(service_error)` + 关闭码 **1011** | 用标准码 ⇒ 前端会**重连重试**（正确）；若用 4401，前端当成"登录过期"并**不重连**（错误）；也**不**降级到 `?token=` |
| ticket 无效 / 过期 / 重放 / 绑定不符 | 关闭码 **4401**，对外不区分原因 | 不给出可探测的信号；差异只进日志 |
| 日志 | **只输出 `sha256(ticket)` 前 8 位**，从不输出明文 | 摘要不可用于认证 |

★ 关闭码刻意用 **1011** 而不是新增 4xxx：`4xxx = 应用自定义` 的既有约定不变，
前端"4xxx 一律不重连"的策略也不用改（1011 不在那个集合里 ⇒ 自动重连）。

## 9.5 测试结果（真实执行）

| 层 | 命令 | 结果 |
|---|---|---|
| 语义（SQLite 临时库） | `python tests/test_ws_ticket.py` | **58/58 PASS**（原 15 节 → 18 节） |
| **原子性（真实 MySQL 8）** | `python tests/ws_ticket_mysql_concurrency_run.py` | **20/20 PASS** |
| 全量回归 | `for t in tests/test_*.py` | **72/72 套全绿** |

真实 MySQL 上最关键的几条（`probe` schema 内，8 个**独立物理连接**并发）::

    [1b.1] ★ create_all 在已有其他表的 schema 里建出了 ws_ticket   ← 零人工 DDL 的证据
    [3.1]  ★★ 恰好 1 个 worker 拿到 user_id（其余全部失败）
    [3.3]  库里只有 1 行、且已被标记 used_at
    [6.1]  ★★ 不同物理连接能读到同一个 ticket 并消费成功   ← 模拟 A 实例签发 / B 实例校验
    [8.1]  ★★ 表缺失时返回 UNAVAILABLE（可区分于"凭证无效"）
    [9.1]  ★ 库里只有 sha256 摘要（64 位 hex），与明文不同

覆盖到的用例（对应需求逐条）：

- 正常签发与消费 → `[2]`
- 过期 ticket → `[4]`（SQLite）/ `[5]`（MySQL 并发）
- 重复消费 → `[3]` / `[3.1]`
- **两个独立 worker 并发消费同一个 ticket** → MySQL `[3.1]`（8 路，只成功 1 次）
- ticket 绑定错误的会话 → `[5]`（且**不消耗**）
- 存储不可用 → `[9]` / `[11]`（503）/ `[16]`（1011）/ MySQL `[8]`
- WS 鉴权成功与失败 → `[12]`~`[15b]`
- 原有单 worker 行为与安全回归 → 全量 72 套（含 `test_interview_ws` 102 项、
  `test_auth_security`、`test_authz_ownership`）
- 撤销 → `[7]`（单个 / 按用户）+ `[17]`（登出）

★ 一处**假通过**在写测试时被抓住并修正：源码守卫原本写
`"_ticket_store" not in text_ws`，而新代码里有 `ws_ticket_store` —— 子串命中，
守卫恒为假失败。改成 **AST 取模块级标识符**（`_ticket_store` / `_ticket_lock` /
`_MAX_TICKETS` / `_sweep_tickets`）后才是真判据。这就是"裸词被同族 token 误伤"
在本项目的第 N 次复发。

## 9.6 `worker_guard` 保留，以及允许多 worker 的验收条件

**保留。** 本次**没有**删除、也没有放宽它（需求明确禁止）。

ticket 已不再是阻塞项，但还有两项没过：

| # | 阻塞项 | 现状 | 允许多 worker 前必须做 |
|---|---|---|---|
| 1 | **`login_throttle` 是进程内计数** | 5 次/15 分钟，按进程计 | 迁到共享存储（同 ticket 的做法：`user_login_attempt` 表 + 条件更新），否则**爆破限流被放大 N 倍**——这是安全回归，不是体验问题 |
| 2 | `ConcurrencyGate` / `RateLimiter` / `Mutex` | 每进程各算各的 | 可接受（只影响体验，不影响数据正确性），但需在文档里明确"上限 = 配置 × 副本数" |

**允许多 worker 的验收条件（全部满足才可移除 `worker_guard`）**：

1. ✅ ticket 不再依赖进程内状态，且有**真实 MySQL 并发测试**证明"并发消费只成功一次"
   —— 本次已达成（`ws_ticket_mysql_concurrency_run.py [3.1]`）。
2. ⬜ `login_throttle` 迁到共享存储，并有并发测试证明"多副本合计仍只允许 N 次失败"。
3. ⬜ `orch._STATES` 的跨副本行为有明确结论：要么网关做**会话亲和**，
   要么验证"重连落到别的副本 ⇒ `adopt_state` 自愈"在真实 ASGI 栈上成立。
4. ⬜ `/api/health` 的指标采集按 `instance` 聚合（当前是进程内计数）。
5. ⬜ 端到端：起 **2 个 uvicorn worker**，跑 `tests/interview_ws_integration_run.js`，
   要求 53 项全过，且日志里没有 4401/1011 的意外命中。

在此之前，`assert_single_worker()` 保持原样：多 worker **启动即失败**，
比"跑起来但随机 4401"好得多。

## 9.7 部署与回滚

**数据库**：`ws_ticket` 是**新表**，由 `lifespan` 的 `Base.metadata.create_all` 自动创建
（`create_all` 只建新表、不给既有表补约束；本表是新表 ⇒ 零人工 DDL）。
⇒ **无需迁移脚本、无需环境变量、无需数据清理**。

★ 这条不是推断：运行器 `[1b]` 在 probe schema 里**先手工建一张已有表**、再跑
`create_all`，断言 `ws_ticket` 被建出来且既有表未被破坏 —— 正是"生产库已有
`interview_*` 等表、只缺 `ws_ticket`"的真实场景。

**上线步骤**：重启后端（建表）→ 用
`python tests/ws_ticket_mysql_concurrency_run.py` 复核（生产库只读检查 `[1]`）。

**回滚**：还原 `api/interview_ws.py` / `main.py` / `models/user.py` / `models/__init__.py`，
删除 `services/ws_ticket_store.py` 与两个测试文件即可。
留下的 `ws_ticket` 空表**无副作用**（旧代码不读它），可以择机 `DROP TABLE`。
★ 回滚后 ticket 回到进程内字典 ⇒ **必须保持单 worker**（`worker_guard` 会强制）。

## 9.8 剩余风险

| # | 风险 | 等级 | 说明 |
|---|---|---|---|
| 1 | 前端在签发接口 5xx 时**回退 `?token=`** | ~~P2~~ **已修复** | 见 §10.1：前端已移除降级，签发失败一律 **fail closed**（不再把长期 token 写进 URL） |
| 2 | ticket 路径无法复核**会话撤销** | ~~P2~~ **已修复** | 见 §10.2：ticket 绑定 `auth_session_id` + 撤销广播，实测 **0.31s** 生效（改前 >20s 不生效） |
| 3 | `ws_ticket` 表只增不减（若从不签发则不增长） | P3 | 清理是机会式的；零流量期间不清理也不增长（过期行不会被消费） |
| 4 | `login_throttle` 仍是进程内 | **P1（安全）** | 见 9.6；这是 `worker_guard` 仍需保留的主因 |
| 5 | MySQL 运行器**不在 CI** | ~~P1~~ **已修复** | 见 §11：`.github/workflows/ci.yml` 起 MySQL 8 service container，3 个运行器纳入门禁；退出码 `2`（环境不满足）在 CI 里判**失败** |

---

# 10. 三项剩余风险的收口（第 5 轮）

第 4 轮结束时留下三个明确缺口：前端在 ticket 签发失败后回退长期 token、
管理员踢下线后已建立的 WS 连接长时间不失效、真实 MySQL 验证不在 CI。
本章逐条记录**改前实测**、**改法**与**改后实测**。

## 10.1 风险一：ticket 签发失败 ⇒ 前端 fail closed

### 改前（实测行为，不是推测）

`interviewSocket.js` 的 `connect()` 里有一条**静默降级**：

```js
// 改前
try { ticket = await this._fetchTicket() } catch { /* 忽略 */ }
url = ticket ? buildSocketUrl(id, ticket) : buildSocketUrl(id, null, token)
//                                                     ^^^^^ 长期 token 进 URL query
```

后果：签发接口 503 / 超时 / 返回脏数据时，**长期登录凭证被写进 URL**。
URL 会进 uvicorn access log、反向代理 access log、浏览器历史、
`Referer`，以及任何 `Sentry`-类上报的 request context。

### 改后

| 位置 | 变化 |
|---|---|
| `buildSocketUrl(sessionId, ticket)` | `ticket` 非字符串或为空 ⇒ **抛 `SocketTicketError`**，不再有第二个分支 |
| `connect()` | 取凭证失败 ⇒ **不建连**（`FakeWebSocket.instances.length === 0`），错误原样抛给调用方 |
| `connectWithRetry()` | `SocketAuthError`（401/403）**不重试**；其余按 **equal jitter 指数退避** 重试 |
| `computeReconnectDelay(attempt, rand)` | `nominal = min(500×2^n, 15000)`；实际 `= nominal×0.5 + rand×nominal×0.5`（避免惊群） |
| `_fetchTicket()` | `AbortController` 8s 超时；校验响应体形状；**不把响应体写日志** |
| `InterviewRoom.vue` | 展示 `describeConnectError(e)` + "重试中（第 n 次）"，不再是空白房间 |

★ 顺带修掉一个**改前就存在的真实缺陷**：凭证获取失败时 `this.ws` 从未被赋值，
因此 `onclose` 永远不会触发 ⇒ **自动重连链彻底断掉**，一次 503 就永久失联。
现在 `_retry()` 在 `catch` 里**主动续链**。

### 后端侧：`?token=` 路径保留但可关

`INTERVIEW_WS_TOKEN_AUTH`（**默认开启 = 行为不变**，`0/false/no/off` 显式关闭，
非法值记 WARNING 后按开启）。保留原因：`test_interview_ws.py`（102 项）与
`interview_ws_integration_run.js` 仍用它建连。**新部署建议设 `0`**。

### 日志侧

`test_auth_security.py` 新增断言：脱敏过滤器**真的挂在** root / uvicorn /
uvicorn.access / uvicorn.error 四个 logger 上；异常日志内嵌 ticket 被打码；
`args` 形式的 URL 同样被打码。过滤器对**格式化之后**的整条消息脱敏
（`record.msg = redact_text(record.getMessage()); record.args = ()`）。

## 10.2 风险二：踢下线后已建立连接的撤销延迟

### 改前实测（`tests/_probe_revoke_latency.py`，同一脚本测改前/改后）

| 路径 | 改前实测 | 说明 |
|---|---|---|
| ticket（前端在用） | **>20s 预算内完全不生效** | ticket 只存 `user_id`，`_auth_still_valid` 的 ticket 分支只 `select(User.id)`；上界是空闲超时 300s，而前端每 25s 有心跳 ⇒ **实际永不失效** |
| token（历史兼容） | **61.7s** | 60s 复核节流 + 依赖"下一条消息" |

★ **任务描述里的"最长 30 秒"与实测不符**：改前的真实上界是 300s（有心跳则无穷），
不是 30s。这个差异必须先纠正，否则"把 30s 改成 1s"会得出完全错误的结论。

### 改法（不用 Redis，也不靠进程内连接表）

```
撤销方（任一实例）                       受影响实例
─────────────────                       ─────────────────────────────
user_session.revoked_at = now()  ← 主效应（失败即 500）
        │
        └─ ws_revocation.record()  ──→  ws_revocation（只追加表，单调 id）
                                              │  id > watermark，一条查询/实例/周期
                                              ▼
                                        watcher.poll_once()
                                              │  ① 先推进 watermark ② 再调 handler
                                              ▼
                                        close_revoked_connections(rows)
                                              │  matches() 命中 → 主动 _close(4401)
                                              ▼
                                        本进程的连接表 _ACTIVE_CONNS
```

四条设计要点：

1. **只追加 + 单调 id ⇒ 不丢消息**。某实例错过一次轮询，下次 `id > watermark`
   自动补齐；pub/sub 做不到（丢了就丢了）。
2. **每实例一条查询/周期**，与连接数**无关**。不是"每条连接每秒查库"——
   那才是"把 30s 改 1s"会造成的负载问题。
3. **先推进 watermark 再调 handler**：否则 handler 抛异常会导致反复踢同一批；
   handler 缺失时**回退** watermark。
4. **兜底不依赖广播**：60s 周期复核 + `answer` / `finish` 等高风险操作
   **执行前**再验一次会话（`ws_auth_revoked_on_high_risk`）。广播存储故障时，
   最坏退化成"60s 内失效"，而不是"永不失效"。

### 改后实测

| 场景 | 实测 | 备注 |
|---|---|---|
| 同实例撤销 → 连接断开 | **0.313s** | 测试轮询周期 0.2s |
| 跨实例撤销 → 连接断开 | **0.375s** | 两个独立 engine/连接 |
| probe（0.5s 步长） | **0.53s** | 数字被步长卡住；真实延迟以套件为准 |
| 生产默认周期 2.0s | **设计目标 ≤ 3s** | ★ 这是**目标**，不是实测 |

★ 区分清楚：0.31–0.38s 是**测试结果**（周期 0.2s），生产周期 2.0s ⇒
**预期 ≤ 3s**，但生产周期下的延迟**没有实测**。

### 边界处理

| 情况 | 行为 |
|---|---|
| 广播写入失败（表缺失/连接断） | `RevocationStoreUnavailable`，**不回滚撤销主效应**，靠 60s 复核兜底 |
| 轮询查询失败 | 记 WARNING、**不推进 watermark**，下周期重试（不丢消息） |
| 并发撤销同一用户 | 多条广播、`matches()` 幂等 ⇒ 每条连接只断一次 |
| 连接侧无 `auth_session_id` | 只有"按用户撤销"能命中；"按会话撤销"**不误伤** |
| 撤销与消费并发 | 二者最多一个生效（运行器 `[7]` 实测） |
| 进程重启 | 水位线从 `max(id)` 起步，**不重放**历史（那时的连接本就不存在） |

## 10.3 配置 / 迁移 / 回滚

新增配置（均默认开启 = 行为不变，已进 `regression_env.PINNED`）：

| 变量 | 默认 | 说明 |
|---|---|---|
| `INTERVIEW_WS_TOKEN_AUTH` | 开 | `0` 关闭 `?token=` 历史兼容路径 |
| `INTERVIEW_WS_REVOCATION_WATCH` | 开 | `0` 关闭广播轮询器（退化到 60s 复核） |
| `INTERVIEW_WS_REVOCATION_WATCH_INTERVAL` | 2.0 | 轮询周期（秒），<0.1 会被 clamp 到 0.1 |

新增表 / 列：

| 对象 | 类型 | 部署动作 |
|---|---|---|
| `ws_revocation` | **新表** | 零人工 DDL（`create_all` 自动建） |
| `ws_ticket.auth_session_id` | **既有表加列** | 已登记 `schema_sync.DESIRED_COLUMNS`，启动时自动 `ALTER ... ADD COLUMN` |

新增端点：`POST /api/admin/users/{user_id}/kick`（管理员踢下线；
撤 `revoked_at` → 作废 ticket → 写广播；返回 `{success, revoked_sessions, revoked_tickets, broadcast}`）。

**回滚**：还原 `api/interview_ws.py` / `main.py` / `models/user.py` /
`services/ws_revocation*.py` / 前端三文件即可。留下的 `ws_revocation` 空表与
`ws_ticket.auth_session_id` 空列**无副作用**（旧代码不读），可择机清理。
回滚后撤销延迟**回到改前水平**（ticket 路径 >20s 不生效）。

## 10.4 本章剩余风险

| # | 风险 | 等级 |
|---|---|---|
| 1 | 生产周期（2.0s）下的撤销延迟**未实测**，只有测试周期下的 0.31s 与设计目标 ≤3s | P2 |
| 2 | 广播表只增不减（`prune` 是机会式的，保留 1h） | P3 |
| 3 | `login_throttle` 仍是进程内 ⇒ `worker_guard` 必须保留（见 §9.6） | **P1（安全）** |
| 4 | 撤销广播依赖 MySQL 可用；库不可用时退化为 60s 复核 | P2 |

---

# 11. 风险三：真实 MySQL 纳入 CI

## 11.1 审计：改前是什么状态

| 项 | 改前 |
|---|---|
| CI 配置 | **完全没有**：无 `.github/`、无 `.gitlab-ci.yml`、无 Jenkinsfile |
| 真实 MySQL 验证 | 两个手工运行器（`ws_ticket_mysql_concurrency_run.py` 20 项、`interview_mysql_concurrency_run.py` 32 项），**没有任何自动入口** |
| 撤销广播的 MySQL 验证 | **不存在**（§10 的机制只在 SQLite 上验过逻辑） |
| 退出码语义 | `0` 通过 / `1` 失败 / **`2` 环境不满足** |

★ 这里有个**隐蔽的"制造绿色"陷阱**：`2` 表示"这台机器没配 MySQL"。
如果 CI 直接 `python tests/xxx_run.py`，那么**容器没起来**会让这一步返回 `2`；
有人为了让它变绿加一句 `|| true`，于是**一个用例都没跑，CI 却是绿的**。

## 11.2 设计：三层，判据只有一条

```
.github/workflows/ci.yml
├── backend-regression     73 套 SQLite 自检 + worker_guard 安全网断言
├── frontend-regression    104 项前端自检
└── mysql-integration      mysql:8.0 service container
    ├── 等待就绪（150s 超时 ⇒ exit 1，不是跳过）
    ├── scripts/ci_bootstrap_db.py --twice   （与 main.py lifespan 同一条路径）
    ├── tests/run_mysql_suites.py            （★ 唯一判绿处）
    └── 清理遗留 probe 库（if: always()）
```

**判据**：只有 `0` 是通过。`1`（断言失败）/ `2`（环境不满足）/
其他（崩溃、被信号杀掉）**全部非零**，且**分类报错**——
把崩溃说成"断言失败"会掩盖真实原因（连不上库 / 建库无权限）。

**为什么加一层包装而不是改运行器的退出码**：`2` 对**人**是有用的
（"这台机器没配 MySQL，跳过吧"）。改掉它会让本地手工跑的行为也变，
而本地手工跑正是发版前的最后一道人工验证。所以判据只在 CI 侧收紧。

**为什么初始化要写成脚本**：如果在 YAML 的 `run:` 里复制
`create_all` + `ensure_columns`，CI 验证的就是一份**平行实现**——
它绿了不代表应用启动时能绿。`ci_bootstrap_db.py` 调的是生产同一条代码路径，
`--twice` 连跑两遍验证幂等（重复启动不会重复 `ALTER`）。

## 11.3 新增：撤销广播的真实 MySQL 验证（51 项）

`tests/ws_revocation_mysql_run.py` —— SQLite 证明不了的都在这里：

| 节 | 证明什么 |
|---|---|
| `[1]` | 生产库只读检查（表 / 列 / 索引） |
| `[1b]` | ★ 非空 schema 上 `create_all` 建新表 + `schema_sync` **补 `ws_ticket.auth_session_id`**（手工 `DROP COLUMN` 造旧结构），并验证幂等 |
| `[2]` | 版本 8.x、隔离级别 `REPEATABLE-READ` |
| `[3]` | 8 个独立连接并发 `record()`：全部落库、id 唯一、`NULL` 往返仍是 `None` |
| `[4]` | 新事务每周期能看到别的事务**已提交**的新行（不漏消息） |
| `[5]` | ★★ **快照陷阱**：同一事务内重复读**看不到**新行，`rollback` 后立刻看得到 ⇒ 这就是"把 30s 改 1s"会踩的坑 |
| `[6]` | 跨实例双轮询器各自水位线：都收到、都不重复投递 |
| `[7]` | 并发踢同一用户：5 条广播、handler 幂等（同批处理两次只断一次） |
| `[8]` | `prune` 不破坏 `id > watermark` 语义；`latest_id` 可回退但不漏 |
| `[9]` | 并发写 + 并发读：读者观察到的 id 序列单调、无重复、无脏读 |
| `[10]` | 表缺失时四个入口都 `UNAVAILABLE`（`prune` 只返回 0，不打断主流程） |

## 11.4 实际执行结果（本地，真实 MySQL 8）

```
$ cd backend && python tests/run_mysql_suites.py
  套件            退出码     通过    失败    判定
  ticket        0       20    0     通过
  concurrency   0       32    0     通过
  revocation    0       51    0     通过
[OK] 3 个套件全部通过，共 103 项断言
```

负向验证（证明判据真的会红，不是恒绿）：

| 输入 | 退出码 | 输出 |
|---|---|---|
| `DATABASE_URL` 未设置 | **2** | `[FAIL] 未设置 DATABASE_URL` |
| `DATABASE_URL=sqlite+aiosqlite:///:memory:` | **2** | `[FAIL] 不是 MySQL —— SQLite 无法验证行锁与隔离级别` |
| `DATABASE_URL=...@127.0.0.1:3999/...`（不可达） | **3** | `[FAIL] 没跑到断言就异常退出` + 末 15 行 traceback（含 `Can't connect to MySQL server`） |

★ 第三种情况特意区分于"断言失败"：`failed == 0` 但退出码非 0 ⇒ 判为崩溃。

全量回归（CI 同款循环，`DATABASE_URL` 已 unset）：

```
TOTAL=73 FAIL=0        # 后端 73 套
结果: 通过 104 项，失败 0 项   # 前端
```

`worker_guard` 复核（CI 里也有这一步）：

```
$ WEB_CONCURRENCY=2 python -c "... assert_single_worker()"
[OK] worker_guard 生效：检测到多 worker 配置（workers=2）。...
```

## 11.5 凭据卫生

| 措施 | 实现 |
|---|---|
| 用一次性容器口令，不用任何真实凭据 | `ci_root_ephemeral` 只在本 job 的临时容器里存在 |
| 声明后立刻打码 | 第一步 `::add-mask::` 口令与完整 `DATABASE_URL` |
| 不 echo 敏感变量 | 已用脚本校验：**没有任何 `run:` 步骤展开** `$DATABASE_URL` / `$*PASSWORD` |
| 运行器自身脱敏 | 打印连接串一律 `_redact()`（只留 `host/db`） |
| 生产凭据 | **永远不写进 workflow 文件**（它会被提交进 Git） |

## 11.6 未覆盖 / 限制（诚实清单）

1. **本地无法执行 GitHub Actions**。已做的验证是：YAML 可被 `yaml.safe_load`
   解析；3 段内嵌 Python heredoc 可 `compile()`；YAML 结构与步骤顺序符合预期；
   所有 `run:` 命令都能在本地**原样**执行并通过。**没有**在 GitHub 上真跑过一次。
2. **只在一个 MySQL 版本（8.0）上验证**。8.4 / MariaDB 未覆盖。
3. **并发规模是"多连接"而非"多进程"**：运行器用独立 engine + 独立物理连接
   模拟多实例，这与多进程在**数据库侧**等价（InnoDB 只认连接），但**不覆盖**
   进程内状态（如 `login_throttle`）的跨实例问题——那正是 §9.6 的待办。
4. **CI 不验证真实讯飞 / 真实星火**（`AVATAR_PROVIDER=""`，不消耗配额）。
5. **`mysql-integration` 不阻塞 `worker_guard` 的解除**：解除限制仍需满足
   §9.6 的五个条件，其中"`login_throttle` 外置"尚未做。

## 11.7 回滚

CI 是**纯增量**：删掉 `.github/workflows/ci.yml`、
`backend/tests/run_mysql_suites.py`、`backend/tests/ws_revocation_mysql_run.py`、
`backend/scripts/ci_bootstrap_db.py` 即可回到"无 CI"状态。
**不改动任何生产代码路径**（`ci_bootstrap_db.py` 只调用既有函数）。

---

# 12. 第 6 轮：剩余风险（除密钥轮换）的收口

第 5 轮结束时还剩四条风险，其中「`SPARK_API_SECRET` 在 Git 历史」按用户要求
**暂不处理**（须先轮换密钥再清理历史）。本章处理另外两条。

## 12.1 P2：生产轮询周期下的撤销延迟（已实测）

第 5 轮只测了测试周期（0.2s）下的 0.31s，生产默认周期 2.0s 下**没有实测**，
当时只能写"设计目标 ≤3s"。现在补上了。

取证脚本加了两个参数（默认值与原来一致，行为不变）：

```bash
python backend/tests/_probe_revoke_latency.py --interval 2.0 --step 0.1
```

**实测结果（真实执行）**

| 路径 | 周期 2.0s（生产默认） | 周期 0.2s（套件） | 改前基线 |
|---|---|---|---|
| ticket（前端在用） | **2.05s** | 0.31s | **>20s 完全不生效** |
| token（历史兼容） | **1.89s** | 0.38s | 61.7s |

- 理论上界 = 周期 + 步长 = 2.0 + 0.1 = 2.1s，实测 2.05s 落在界内 ✅
- **设计目标 ≤3s 达标**（2.05s）
- token 路径从 61.7s 降到 1.89s：它现在也走撤销广播，不再依赖 60s 复核节流

★ 注意：这是**本机 SQLite + TestClient** 的测量。生产是 MySQL + 真实网络，
轮询查询的 RTT 会加上去（单条索引查询，通常 <5ms）。所以「≤3s」这个结论
在真实网络下仍然成立，但**不是**在生产环境实测的数字。

## 12.2 P1：登录限流外置（多 worker 的最后一个安全阻塞项）

### 风险量化（改前）

| 部署 | 同一账号 15 分钟内允许的失败次数 |
|---|---|
| 1 worker | 5（`AUTH_LOGIN_MAX_FAILURES`） |
| N worker | **5 × N** —— 攻击者只要让请求散到各进程 |

这不是体验问题，是**安全项被静默放大**：进程内字典各自计数，谁也不知道别人记了多少。

### 设计

新增 `services/login_throttle_store.py`（门面）+ 表 `login_failure`：

- **一条失败一行**（`key_hash` + `failed_at`），`check` = `COUNT(*) WHERE failed_at >= now()-window`；
  滑窗语义与旧的 `deque` 实现**逐字等价**（含 `Retry-After` 的 `+1`），
  不用固定窗口计数器——固定窗口在边界处会有双倍突发。
- **只存 `sha256(小写邮箱|IP)` 摘要**，不落明文邮箱 / IP。
- 墙钟而非 `monotonic`（后者只在单进程内有意义，跨实例不可比）。
- `AUTH_LOGIN_STORE`：空串/未配置 ⇒ `memory`（**行为逐字不变**）；`db` ⇒ 共享表；
  非法值记 ERROR 后按 `memory`（与其它开关同口径）。
- 存储不可用 ⇒ **fail closed**（`LoginThrottleUnavailable` → 路由 **503**）。
  理由：用户查询本来就要读同一个库，库不可用时登录本来就不可能成功；
  "放行"只会让限流被绕过。
- 启动日志会打出当前后端，并明确提示「多 worker 时上限 × 进程数」
  —— 否则这类问题在功能上**永远不表现出来**。

`main.login` 的调用从同步单参变成 `await f(db, key)`；三个动作都包了
`_throttle()`，把存储故障映射成 503（不混进 401 / 429）。

### 实测（真实 MySQL 8，`tests/login_throttle_mysql_run.py`，28 项）

| 场景 | 结果 |
|---|---|
| 8 个独立连接并发 `record()` | 8 行全部落库，无死锁 |
| A 记满 5 次 → B 的 `check` | **被拦**（跨实例统一计数）✅ |
| 20 个并发突发 | **20 次全部放行**（上限 5，溢出 15） |
| 突发之后第二波 20 并发 | **0 次放行**，且不新增行 ✅ |
| 被封期间 `Retry-After` | >0 且 ≤ 窗口（已钳制） |
| 库里内容 | 只有 64 位摘要，无明文邮箱 / IP |

### ★ 关于「20 并发全部放行」的诚实结论

这是 `check` 与 `record` 之间的 **check-then-act** 竞态：并发请求都看到"还没满"。

**它不是我引入的退化**——同样交错下**内存实现也放行 20 次**
（`tests/test_login_throttle_store.py [10]` 有并排对照，见下）：

```
内存实现：20 并发中放行 20 次（只记内存，不落库）
共享实现：20 并发中放行 20 次（落库 20 行）
```

真正的收益与保证是：

- **收益**：计数从「每进程各 5 次」变成「全局 5 次 + 单次突发溢出」。
  持续爆破不再按 worker 数放大——突发过后窗口内**一个都进不来**（实测 0）。
- **保证**：`≤ 5 + 并发数 / 窗口`，而不是 `5 × worker 数 / 窗口`。
- **为什么不加行锁**：`SELECT ... FOR UPDATE` 在**空表**上取的是间隙锁，
  会把**所有账号**的登录串行化（不是只串行化同一个 key），代价远大于收益。
  彻底消掉溢出需要改成「验证密码前先原子占位」（upsert 计数器），
  属于后续可选优化，本次未做。

### 本轮在实现过程中发现并修掉的 3 个真实缺陷

| # | 缺陷 | 现象 | 修法 |
|---|---|---|---|
| 1 | `record` 先 `DELETE` 过期行再 `INSERT` | 8 个并发失败登录在同一 key 的空索引区间互相等**间隙锁** ⇒ 实测 7/8 死锁 ⇒ 登录 503 | 去掉 `record` 里的 DELETE（过期行由 `check` 的 `failed_at >= cutoff` 过滤，清理交给 `prune`） |
| 2 | `Retry-After` 可能返回 **901**（窗口 900） | `check` 先取 `now` 再查询，另一实例在快照之后写入的 `failed_at > now` ⇒ 算出 901。**重复运行才偶发暴露** | 把 `Retry-After` 钳到窗口上限；新增确定性回归 `[4b]`（用未来时间戳造这个条件） |
| 3 | 我自己测试脚手架的 `async` 漏写 | `db.execute()` 返回协程，`.scalars()` 直接 AttributeError | 改成 `async def` 再取结果 |

### 配置 / 迁移 / 回滚

| 项 | 内容 |
|---|---|
| 新表 | `login_failure`（**新表** ⇒ 零人工 DDL，`create_all` 自动建；运行器 `[1b]` 在非空 schema 上验证过） |
| 新配置 | `AUTH_LOGIN_STORE`：空串 = `memory`（默认，行为不变）/ `db` |
| 已有配置 | `AUTH_LOGIN_MAX_FAILURES`、`AUTH_LOGIN_WINDOW_SECONDS` 语义未变（改为**调用期**读取，便于测试切换） |
| 回滚 | 把 `AUTH_LOGIN_STORE` 置空即回到内存实现；代码回退则删 4 个新文件。留下的 `login_failure` 空表无副作用 |

## 12.3 `worker_guard` 解除条件：还差什么

| 阻塞项 | 状态 |
|---|---|
| WS ticket | ✅ 已下沉 MySQL（第 4 轮） |
| 撤销广播 | ✅ 已下沉 MySQL（第 5 轮） |
| 登录爆破限流 | ✅ **本次提供共享实现**，需显式设 `AUTH_LOGIN_STORE=db` |
| 编排状态 `_STATES` | ❌ 仍是进程内，**需要会话亲和**（网关按 `session_id` 粘住） |
| `SessionTracker` / `ConcurrencyGate` / `RateLimiter` | ❌ 仍是进程内 ⇒ 全局配额 = 上限 × worker 数 |

**结论：本次不解除 `worker_guard`。** 登录限流是最后一个**安全类**阻塞项，
它已经解决；剩下的是**配额/状态类**问题，需要会话亲和 + 配额口径决策，
不是「再写一张表」能解决的。

★ `worker_guard` 的文案已同步更新：不再说"请先实现 Redis 状态外置"
（本项目刻意不引入 Redis），而是列出真实剩余项与验收条件出处。

## 12.4 本轮新增/修改的文件

| 文件 | 类型 | 说明 |
|---|---|---|
| `backend/services/login_throttle_store.py` | 新增 | 共享限流门面（memory / db 双后端） |
| `backend/tests/test_login_throttle_store.py` | 新增 | 50 项（含内存 vs 共享的并发对照） |
| `backend/tests/login_throttle_mysql_run.py` | 新增 | 28 项真实 MySQL |
| `backend/models/user.py` | 修改 | 新增 `LoginFailure` |
| `backend/models/__init__.py` | 修改 | 导出 `LoginFailure` |
| `backend/main.py` | 修改 | 登录路由三处改 async + 503 映射 + 启动日志 |
| `backend/utils/worker_guard.py` | 修改 | 文案与已解决项同步 |
| `backend/tests/regression_env.py` | 修改 | 钉 `AUTH_LOGIN_STORE=""` |
| `backend/tests/_probe_revoke_latency.py` | 修改 | 加 `--interval/--step`（默认值不变） |
| `backend/tests/run_mysql_suites.py` | 修改 | 纳入 throttle 套件 |
| `.github/workflows/ci.yml` | 修改 | 清理正则加 `throttle` |

---

# 13. 第 7 轮：真实 MySQL 集成测试的 CI（事务 / 并发 / 幂等）

本章的输入是用户的 9 条要求。第 5 轮已经把「真实 MySQL 进 CI」这件事做出来了
（3 个 job、service container、`run_mysql_suites.py` 唯一判绿、`if: always()` 清理），
所以本轮的活儿**不是重建**，而是**按 9 条要求逐条核对，只补缺口**。

## 13.1 审计：逐条对账（先看，再改）

### 13.1.1 现状盘点

| 项 | 实际值 |
|---|---|
| CI 平台 | **GitHub Actions**（`.github/workflows/ci.yml`，第 5 轮建；无 GitLab / Jenkins） |
| 测试框架 | **无 pytest**。`backend/tests/test_*.py` 是自检脚本（`_check()` + `sys.exit`） |
| 数据库驱动 | `aiomysql`（异步）／`pymysql`（同步，仅迁移脚本用） |
| 异步访问方式 | `AsyncSession` + `async_sessionmaker(expire_on_commit=False)` + `AsyncAdaptedQueuePool` + `pool_pre_ping=True` |
| 单元测试命令 | `for t in tests/test_*.py; do python "$t"; done`（**74 套**） |
| 集成测试命令 | `python tests/run_mysql_suites.py`（**只有退出码 0 是通过**） |

### 13.1.2 九条要求逐条对账

| # | 要求 | 改前 | 本轮动作 |
|---|---|---|---|
| 1 | 检查 CI / 框架 / 驱动 / 异步方式 / 命令 | — | ✅ 见 13.1.1 |
| 2 | 起兼容的 MySQL service container，沿用现有平台 | ✅ 第 5 轮已有 `mysql:8.0` | **沿用，不替换** |
| 3 | 独立测试库 + 专用账号，**不得连生产库** | ❌ 用 **root** 直连 `career`（同一个实例、同一个库） | ✅ `career_ci_test` + `career_ci@'%'` 最小权限（13.2） |
| 4 | 健康检查 / 启动等待 / 初始化 / 迁移 / 清理 | ✅ 150s 等待、`ci_bootstrap_db.py --twice`、`if: always()` 清 probe 库 | 补登 `login_failure` 表（13.7-①） |
| 5 | 跑**真实 MySQL**，不是 SQLite / mock | ✅ 4 套运行器 | ✅ 新增第 5 套（13.3） |
| 6 | 5 类并发 / 事务场景 | ✅ 已覆盖（13.1.3） | 逐条复核，**无缺口** |
| 7 | Async ⇒ 真实异步驱动 + 连接池 + 事务边界 | ❌ **完全没有验证** | ✅ 新增 25 项（13.3） |
| 8 | 快慢分离，单元测试不依赖数据库 | ✅ 两套入口已分开 | ✅ 实测：`unset DATABASE_URL` 后 74 套仍全绿（13.5） |
| 9 | 日志不出密码 / 凭据 / 完整 ticket | 部分（运行器有 `_redact`，但 setup 脚本会打印口令） | ✅ 默认脱敏 + `--show-url` 才输出（13.6） |

★ 结论：**需求 6 本来就没有缺口**，我逐条核对了源码才敢这么说（见 13.1.3）。
唯一真正缺的是**需求 7**，以及需求 3 的账号隔离。这两项是本轮的全部新增。

### 13.1.3 需求 6 的五类场景落在哪（核对结果，不是"应该有"）

| 要求覆盖的场景 | 落在哪个套件的哪一节 |
|---|---|
| 并发创建 / 更新同一业务记录 | `interview_mysql_concurrency_run.py` `[4]`（8 连接并发写同一 `session_id`）、`[5]`（并发 `submit_answer`） |
| 事务提交 / 回滚 / 异常处理 | 同上 `[9]`、`[11]`；本轮 `engine_pool_mysql_run.py` `[5]` 三项独立再验一遍 |
| 唯一约束与重复请求 | 同上 `[3]`（重复 `start_session` 幂等）、`[7]`（并发触发唯一键冲突 ⇒ 409 而非 500） |
| 并发消费同一 WebSocket ticket，**最多一次成功** | `ws_ticket_mysql_concurrency_run.py` `[3]`：8 个独立连接同时 `consume()`，**恰好 1 个 True** |
| 多个独立数据库连接下的并发 | 全部 5 套运行器（一律独立 engine + `NullPool`，InnoDB 侧等价于多进程） |

## 13.2 缺口一：CI 一直用 root 跑测试

### 13.2.1 为什么这算缺口

改前的 CI 用 `root` 连 `career` 库跑集成测试。运行器虽然会建 `career_*_probe_*`
临时库，但**root 什么都能干**：一次写错的 `DROP DATABASE`、一次 `DATABASE_URL`
指错，代价就是生产库。只靠"操作者记得别指错"不是保证。

### 13.2.2 修法：最小权限账号（库级 GRANT 的通配符必须转义）

新增 `backend/scripts/setup_test_mysql.py`（幂等，可重复执行）：

```sql
CREATE DATABASE IF NOT EXISTS `career_ci_test` ...;
CREATE USER IF NOT EXISTS 'career_ci'@'%' IDENTIFIED BY '<pw>';
GRANT ALL PRIVILEGES ON `career\_ci\_test`.*    TO 'career_ci'@'%';
GRANT ALL PRIVILEGES ON `career\_%\_probe\_%`.* TO 'career_ci'@'%';
```

★ **`_` 和 `%` 在 MySQL 的库级授权里是通配符**。不转义的话
`GRANT ... ON career_ci_test.*` 会连 `careerXciYtest` 一起授权，
`career_%_probe_%` 则会授权给**任何**形如 `career_*_probe_*` 的库。
所以 `_escape_db_pattern()` 把 `_` / `%` 转成 `\_` / `\%` 后再拼进 DDL。
这一条不是从文档抄的，是脚本末尾用**这个账号真连一次**验出来的。

### 13.2.3 实测（本地真实 MySQL 8.0.45）

```
$ cd backend && python scripts/setup_test_mysql.py --password '<pw>' --show-url
[1] 专用账号的最小权限验证（这一步证明它碰不到别的库）
  [ok]   [1.1] 能以专用账号连上测试库
  [ok]   [1.2] 能建 probe 库（运行器需要）
  [ok]   [1.3] 能删 probe 库（运行器清理需要）
  [ok]   [1.4] ★ 连不上 admin 库 `career`（越权被 MySQL 拒绝）
结果: 通过 4 项，失败 0 项
```

`[1.4]` 是真连一次被 MySQL 以 **1044 Access denied** 拒掉 —— 权限是数据库
层面挡的，不是脚本里的 `if` 挡的。

### 13.2.4 第二道防线：`tests/mysql_guard.py` 库名守卫

账号权限挡的是"跨库"，挡不住"把 `DATABASE_URL` 指到测试账号能碰的那个库上
却其实是要清的生产库"。所以再加一道**纯文本**守卫：

```python
TEST_DB_PATTERN = re.compile(
    r"^(career_ci|career_ci_test|career_test.*|test_.*|.*_test|.*_ci)$", re.I)
```

- 命中 ⇒ 放行；不命中 ⇒ 返回中文拒绝理由，运行器退出码 **2**（CI 判失败）。
- **刻意不提供 `ALLOW_PROD=1` 之类的跳过开关**：有开关就一定会被用上。
- 放在两处：`run_mysql_suites.py`（总闸）**和**每个运行器自己（双保险）。

实测负向：

```
$ DATABASE_URL='mysql+aiomysql://root:***@localhost:3306/career?charset=utf8mb4' \
  python tests/run_mysql_suites.py
[FAIL] 拒绝运行：目标库 career 看起来**不是**测试库...
EXIT=2
```

## 13.3 缺口二：需求 7（驱动 / 连接池 / 事务边界）没有验证

### 13.3.1 为什么其它四个运行器证明不了

那四套为了"真并发"，一律用 `poolclass=NullPool`（每次一条独立物理连接）。
这能证明**数据库侧**的行锁与隔离级别，但**证明不了**生产实际用的**连接池**：

| 生产配置 | NullPool 运行器能证明吗 |
|---|---|
| `aiomysql` 异步驱动 | ❌（NullPool 也是 aiomysql，但没断言过） |
| `pool_pre_ping=True`：连接被服务端杀掉后自动重连 | ❌ **完全没验过** |
| `AsyncAdaptedQueuePool` 真的有上限 | ❌（NullPool 没有上限这个概念） |
| `expire_on_commit=False` | ❌ |
| 事务边界 | ✅（这条它们验了） |

### 13.3.2 新增 `tests/engine_pool_mysql_run.py`（25 项 / 7 节）

| 节 | 证明什么 |
|---|---|
| `[1]` | `create_async_engine` 返回**异步**引擎；`dialect.name == "mysql"`；★ `dialect.driver == "aiomysql"`（不是同步的 pymysql）；`database.py` 里那个**生产 engine** 同样是 aiomysql |
| `[2]` | 生产池**不是** `NullPool`/`StaticPool`；★ `pool._pre_ping is True`；池类名属 `QueuePool` 族；`async_session.kw["expire_on_commit"] is False` |
| `[3]` | ★ 池上限是真的：`pool_size=1, max_overflow=0, pool_timeout=1` 时第二个并发 `connect()` **被阻塞约 1s 后超时**（不是立即失败，也不是无限等） |
| `[4]` | ★★ `pool_pre_ping` **对照组**（见 13.3.3） |
| `[5]` | 事务边界：提交后新连接可见 / 同一事务内可见自己的未提交写入 / 回滚后不可见 / **异常退出 `with` 块自动回滚**（无半截数据） |
| `[6]` | 隔离：两个独立会话看不到对方**未提交**的写入（无脏读） |
| `[7]` | 连接归还与复用：会话进行中 `checkedout()==1`、退出后归零、`checkedin()>=1`、★ 下一次会话**复用的是同一条物理连接**、`dispose()` 后池清空 |

### 13.3.3 ★ `pool_pre_ping` 为什么必须有对照组

"杀掉池里的连接、下一次取用还能查成功"——这个结果**有两种可能**：

1. `pre_ping` 真的在工作（ping 失败 ⇒ 丢弃死连接 ⇒ 重连）；
2. MySQL 还没把那条连接真正回收，它其实**还能用**。

只看第 1 种，测试是**恒绿**的（哪怕把 `pre_ping` 删掉也照样绿）。
所以 `_kill_cycle(pre_ping: bool)` 跑两遍，**同一个 KILL 条件下**：

```
[4.1] 开启 pre_ping ⇒ 下一次取用自动重连并查询成功      PASS
[4.2] 关闭 pre_ping ⇒ 同样条件下**报错**（对照组）      PASS   # 拿到 OperationalError
[4.3] 重连后拿到的是**另一条**物理连接（new_id != old_id） PASS
```

`[4.2]` 是这套测试里最关键的一项：它证明 `[4.1]` 的成功**来自 `pre_ping`**，
而不是"死连接恰好还能用"。

## 13.4 实际执行结果（真实 MySQL 8.0.45，专用账号 `career_ci`）

```
$ cd backend
$ export DATABASE_URL='mysql+aiomysql://career_ci:***@localhost:3306/career_ci_test?charset=utf8mb4'
$ python tests/run_mysql_suites.py
  套件            退出码   通过   失败   判定
  pool          0       25    0     通过
  ticket        0       22    0     通过
  concurrency   0       32    0     通过
  revocation    0       54    0     通过
  throttle      0       30    0     通过
[OK] 5 个套件全部通过，共 163 项断言
EXIT=0
```

★ 专用账号下是 **163 项**（root 下是 138 项）：因为测试主库现在是**空库**，
各运行器的"结构只读检查"这一节真的执行了（root 直连 `career` 时那几项走的是
生产库的既有结构，断言口径不同）。**数字变大不是变好，是跑的东西不一样。**

后端全量回归（`unset DATABASE_URL`，CI 同款循环）：

```
PASS=74 FAIL=0
```

### 13.4.1 负向验证（证明判据真的会红，不是恒绿）

| 输入 | 退出码 | 输出 |
|---|---|---|
| `DATABASE_URL=`（空串，等价未配置） | **2** | `[FAIL] 未设置 DATABASE_URL —— CI 必须提供真实 MySQL 连接串` |
| `DATABASE_URL=sqlite+aiosqlite:///:memory:` | **2** | `[FAIL] DATABASE_URL 不是 MySQL —— SQLite 无法验证行锁与隔离级别` |
| `DATABASE_URL=...@localhost:3306/career`（**开发库**） | **2** | `[FAIL] 拒绝运行：目标库 career 看起来不是测试库` |
| `DATABASE_URL=...@127.0.0.1:3999/...`（不可达） | **2** | `[SKIP] 无法创建 probe 库（OperationalError）... Can't connect to MySQL server` |

★ 第 4 种本轮**从 `3` 变成了 `2`**：`engine_pool_mysql_run.py` 在"建不了 probe 库"
时返回 `2`（环境不满足），而 `2` 的优先级高于 `3`，所以整轮判 `2`。
两种都是**非零**、都带可操作文案，但 `2` 更准确——"库没起来"不是"测试崩了"。
§11.4 里记的 `3` 是第 5 轮当时的实际行为，此处按现状更新。

★ 第 1 种要**用空串**而不是 `unset`：本地有 `backend/.env`，
`unset` 之后 `load_dotenv` 会把 `DATABASE_URL` 灌回 `career`（然后被守卫挡成第 3 种）。
CI 里没有 `.env`，`unset` 与空串等价。

## 13.5 要求 8：快慢分离（实测，不是声称）

| 层 | 入口 | 是否需要数据库 | 本轮实测 |
|---|---|---|---|
| 单元 / 逻辑 | `for t in tests/test_*.py` | ❌ 不需要（`regression_env` 把 `DATABASE_URL` 钉成 `sqlite+aiosqlite:///:memory:`） | `PASS=74 FAIL=0`，且**跑之前已 `unset DATABASE_URL`** |
| 集成 | `python tests/run_mysql_suites.py` | ✅ 必须真实 MySQL | `5 套 / 163 项 / EXIT=0` |

两套入口互不依赖：74 套里**没有任何一套**会去连 MySQL；5 套运行器也**不在**
`npm test` / 单元循环里（否则每个 PR 都要起容器）。

## 13.6 要求 9：日志与凭据

| 位置 | 措施 |
|---|---|
| `setup_test_mysql.py` | **默认不打印口令**（`_redact()` 把口令变 `***`）；只有显式 `--show-url` 才输出完整串 |
| `run_mysql_suites.py` | 打印 `DATABASE_URL` 前一律 `_redact()`（只留 `host/db`） |
| 5 个运行器 | 同上，且**不打印 ticket / token 明文**（只打印长度 / 前缀） |
| `mysql_guard` 的拒绝理由 | 只说库名，不回显连接串 |
| workflow 文件 | 不写任何真实凭据（它会被提交进 Git） |

## 13.7 本轮发现并修掉的 4 个真实缺陷

| # | 缺陷 | 现象 | 修法 |
|---|---|---|---|
| 1 | `ci_bootstrap_db.py` 的 `REQUIRED` 漏了 `login_failure` 表 | 第 6 轮新增的表没进 CI 结构校验 ⇒ 建表失败也不会红 | 补 `"login_failure": {"id","key_hash","failed_at"}` |
| 2 | `setup_test_mysql.py` 首次运行报"缺少 admin 连接串" | 只读 `os.environ`，而本地连接串在 `backend/.env` | 加 `load_dotenv(backend/.env)`（与 `database.py` 同手法；CI 无 `.env` ⇒ 空操作） |
| 3 | 运行器在**空测试库**上只报两条 FAIL 不给指引 | 专用账号连的是空库，`[1.1]/[1.2]` 表不存在，看不出下一步 | 加可操作提示：先跑 `scripts/ci_bootstrap_db.py` |
| 4 | `setup_test_mysql.py` 的 docstring 引用了一个**不存在**的测试文件 | `tests/test_mysql_integration_wiring.py` 从来没有过 ⇒ 文档说谎 | 改成描述脚本自己的 `[1]` 四项实测（真实存在、每次执行都跑） |

★ 第 4 条是我自己上一轮写下的"不存在的验证"。这类句子比没有文档更坏：
后来人会以为覆盖已经在了。本轮按"只写跑过的"原则改掉。

## 13.8 未完成 / 限制（诚实清单）

1. **本地无法执行 GitHub Actions**：YAML 可 `yaml.safe_load`、内嵌 Python 可
   `compile()`、所有 `run:` 命令能在本地原样跑通。**没有**在 GitHub 上真跑过。
2. **只在一个 MySQL 版本（8.0.45）上验证**，8.4 / MariaDB 未覆盖。
3. **`[4]` 的 KILL 用的是 `KILL CONNECTION <id>`**：要求能杀**自己**的连接
   （MySQL 允许，无需 `PROCESS` 权限），但杀**别人**的连接不行 —— 所以这套
   不能在"账号只有只读权限"的环境里跑。
4. **`mysql_guard` 的匹配规则没有 SQLite 回归套件**：它只被 5 个运行器
   端到端调用（每次执行都会过一遍），加上本轮 4 条手工负向验证。
   没加 `tests/test_*.py` 的原因：CI 的 `backend-regression` 会核对套件数量
   （74），新增一套需要同时改 `ci.yml` 的计数。**后续补这一套时，必须与
   `ci.yml` 同改。**

## 13.9 本轮新增 / 修改的文件

| 文件 | 类型 | 说明 |
|---|---|---|
| `backend/tests/engine_pool_mysql_run.py` | 新增 | 驱动 / 连接池 / 事务边界，25 项 / 7 节 |
| `backend/tests/mysql_guard.py` | 新增 | 目标库守卫（拒绝指向非测试库，无跳过开关） |
| `backend/scripts/setup_test_mysql.py` | 新增 | 建测试库 + 最小权限专用账号 + 4 项自验 |
| `backend/tests/run_mysql_suites.py` | 修改 | 纳入 `pool` 套件（4 → 5 套）；接入 `mysql_guard` |
| `backend/tests/ws_ticket_mysql_concurrency_run.py` | 修改 | 接入 `mysql_guard`（+5 行） |
| `backend/tests/interview_mysql_concurrency_run.py` | 修改 | 接入 `mysql_guard`；空库时给可操作提示 |
| `backend/tests/ws_revocation_mysql_run.py` | 修改 | 接入 `mysql_guard`（+5 行） |
| `backend/tests/login_throttle_mysql_run.py` | 修改 | 接入 `mysql_guard`（+5 行） |
| `backend/scripts/ci_bootstrap_db.py` | 修改 | `REQUIRED` 补 `login_failure` |
| `.github/workflows/ci.yml` | 修改 | service container 起 root 容器 → 插建专用账号 → 切换 `DATABASE_URL` 到 `career_ci`（5 套运行器）→ 清理正则加 `pool` |

## 13.10 回滚

纯增量，回滚 = 删 3 个新文件 + 撤 `run_mysql_suites.py` 的 `pool` 项：

```
backend/tests/engine_pool_mysql_run.py
backend/tests/mysql_guard.py
backend/scripts/setup_test_mysql.py
```

删掉后 `run_mysql_suites.py` 会因找不到 `mysql_guard` 而报错 —— 所以要么保留
`mysql_guard.py`（它本身零依赖、无副作用），要么把 5 处 `import mysql_guard`
一并撤掉。**不改动任何生产代码路径**：`setup_test_mysql.py` 只在库里建账号，
`engine_pool_mysql_run.py` 只碰自己的 `career_pool_probe_*` 临时库。
