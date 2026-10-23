# AI 面试问题评估基准 · 评价字段说明

> 数据文件：`backend/scripts/interview_eval_benchmark.json`
> 自检脚本：`backend/tests/test_interview_eval_benchmark.py`
>
> 本文件是**说明**，字段与场景的权威定义在 JSON 里。两者的关键事实由自检脚本交叉校验
> （文档必须覆盖 5 个字段名与 2 个臂 id），避免说明与实际漂移。

## 1. 这份基准解决什么问题

为「**无 RAG** vs **有 RAG**」的问题生成对比，固定全部输入条件，并把两臂的差异
收敛到**唯一一个处理变量**：是否把检索到的知识注入 Agent。

因此本基准的产出是两件事——**固定的测试条件** + **可自动判定的输出检查字段**。
它**不**执行生成质量对比（LLM 非确定性），也**不**修改 Agent / Prompt / RAG。

## 2. 两个臂（对比条件）

| 臂 | 标签 | 调用方式 | 期望 `knowledge_context` | 期望 Prompt 模板 |
|---|---|---|---|---|
| `rag_off` | 无 RAG | `generate_next_question(db, sid, retriever=None, use_rag=False, spark=<Mock>)` | 恰好 `[]` | `prompts/interview/question.txt` |
| `rag_on` | 有 RAG | `generate_next_question(db, sid, retriever=VectorKnowledgeRetriever(..., top_k=<场景>, min_score=<场景>), use_rag=False, spark=<同一个 Mock>)` | 非空，条数 == 场景声明 | `prompts/interview/question_knowledge.txt` |

**为什么有 RAG 臂要显式注入 retriever 而不是 `use_rag=True`**：
`generate_next_question` 里的 `use_rag=True` 会走 `resolve_retriever` →
`knowledge_rag.build_vector_retriever(db)`，**不传任何 `retriever_kwargs`**，
只能用默认 `top_k=5` / `min_score=None`——那样就钉不住检索参数，场景失效。
`resolve_retriever` 的优先级本身也是「显式注入 > `use_rag`」，所以显式注入是最强口径。

**两臂之间必须完全相同的条件**：同一套 DB 行（Job / Resume / InterviewSession /
InterviewContext）、同一个 plan（Planner 走 `use_llm=False`）、同一个 context、
同一份模板集合、**同一条 Mock 回复**、同一个闸门状态（题号恒为 1）。

⇒ 观测到的输出差异**不能**归因于「模板不同」「计划不同」「上下文不同」，
只能是**知识注入**。自检脚本会实测这条：有 RAG 臂的 Prompt 删掉「参考知识」小节后
与无 RAG 臂**逐字节相同**。

## 3. 固定面试场景（三个）

| id | 岗位类型 | 面试阶段 | 难度 | 技能方向 | RAG 注入的文档 |
|---|---|---|---|---|---|
| `s1-mysql-index-mid` | 后端开发工程师 | `project` | `mid` | MySQL 索引 / Spring Boot / 性能优化 | `handbook://mysql/index`（2 片） |
| `s2-spring-boot-senior` | 后端开发工程师（Java） | `technical` | `senior` | Spring Boot 自动配置 / Spring 事务 / Java 并发 | `handbook://spring/auto-configuration`（2 片） |
| `s3-cache-consistency-senior` | 后端架构师 | `scenario` | `senior` | 分布式缓存一致性 / Redis / MySQL | `handbook://architecture/cache-consistency`（1 片） |

每个场景声明：`job_type`（岗位类型）、`context.current_stage`（面试阶段）、
`candidate`（候选人信息：年限 / 简历正文 / 亮点）、`skill_direction`（技能方向）、
`session.difficulty`（难度），以及 `job` / `session` / `context` / `retrieval` /
`mock_reply` / `frozen_conditions_sha256`。

`frozen_conditions_sha256` 是「测试条件」的指纹（场景声明 + 派生 plan 载荷 +
10 个基础变量）。条件漂移会**立刻失败**，而不是静默地比出「假差异」。

## 4. 输出检查字段（5 个）

### 4.1 `question`

- **来源**：模型输出（回复 JSON 的 `question` 键）。经 Validator 标准化后由 Core
  原样返回，**不做 `strip` 写回**（空白只在判空时忽略）。
- **硬闸门**：
  - `question_non_empty` — 空问题 ⇒ `ERROR_QUESTION_EMPTY`（`question_empty`），**阻断**。
  - `question_not_duplicate_of_history` — 与 `context.asked_questions` 完全相同 ⇒
    `ERROR_DUPLICATE_QUESTION`（`duplicate_question`），**阻断**。
- **观察指标**：字符数；是否引用简历中的具体项目名（`plan.resume_focus_points`）；
  与注入知识的最长公共子串长度（度量「照抄程度」）。
- **两臂期望**：有 RAG 臂应更贴合具体场景与追问点，同时**不得整段照抄**参考知识
  （模板明写此约束，建议阈值 40 字）。

### 4.2 `question_type`

- **来源**：模型输出。Validator **明确不做枚举校验**——缺失 → `""`，**不阻断**、
  不产生 error（键缺失属正常补全路径）。
- **硬闸门**：无。
- **观察指标**：是否属于模板声明的枚举
  （`introduction` / `resume` / `technical` / `project` / `scenario` / `hr` / `closing`）；
  **是否等于当前阶段**（`type_matches_stage`）——这是「是否按阶段出题」的直接度量。
- **两臂期望**：**软指标，不是闸门**。不匹配**只记录、不判失败**，因为本项目刻意
  不校验该枚举（题型推进属业务层职责）。

### 4.3 `difficulty`

- **来源**：模型输出；缺失时由 `resolve_difficulty` 用**本场已知难度**（来自 plan）补全。
- **硬闸门**：
  - `difficulty_matches_scenario` — 必须等于 `session.difficulty`（模板第 4 条要求）。
  - `difficulty_in_enum` — 必须属于 `junior` / `mid` / `senior`；非法值 ⇒
    `ERROR_INVALID_DIFFICULTY`（`invalid_difficulty`），**阻断**。
- **谁先拦**（实测结论）：非法值**先被 Agent 的内联规则拦下**（`interview_agent.validate_question`
  返回散文式错误 → 触发一次修复 → 修复仍不合法 ⇒ 流程 `ok=False`、`errors=['agent_failed']`）。
  Validator 的 `invalid_difficulty` 是**同一规则的第二道**（架构化演进），
  只在**直接调用 Validator** 时可见。迁移完成前两者不得同时作为权威。
- **两臂期望**：两臂都必须等于场景难度——**这是闸门**。某臂出现非法难度说明该次运行
  **流程失败**，应作废重跑，**不得**当成「有/无 RAG 的质量差异」。

### 4.4 `reason`

- **来源**：模型输出。缺失 → `""`，**不阻断**、不产生 error。
- **硬闸门**：无。
- **观察指标**：字符数；非空率。
- **两臂期望**：软指标，**本轮不设阈值、不判失败**。

### 4.5 `stage`

- **来源**：**不是模型输出**。Core 从 `context.current_stage` 读出后原样回填
  （`interview_core._question_result(stage=stage)`）。实测：在模型回复里塞
  `"stage": "closing"` 也**不生效**（未知键在结构归一时被丢弃）。
- **硬闸门**：`stage_matches_context` — 必须等于场景的 `context.current_stage`；
  不等即说明上下文没正确送达 Core。
- **观察指标**：无。
- **两臂期望**：两臂必须相同。⚠️ 它是**上下文回显**，不是生成质量指标——
  **不能**用它评判模型优劣，只能用它验证「上下文是否正确送达」。

### 字段一览

| 字段 | 来源 | 硬闸门 | 观察指标 | 两臂差异可否归因于 RAG |
|---|---|---|---|---|
| `question` | 模型 | 2 个 | 3 个 | 可以（核心观测量） |
| `question_type` | 模型 | 0 个 | 2 个 | 可以（软指标） |
| `difficulty` | 模型（缺失时补全） | 2 个 | 0 个 | 不可以（必须两臂相同） |
| `reason` | 模型 | 0 个 | 2 个 | 可以（软指标） |
| `stage` | **Core 回显** | 1 个 | 0 个 | 不可以（必须两臂相同） |

## 5. 判定规则

- **有效运行**：某次运行的**全部硬闸门通过** ⇒ 可进入两臂对比。
- **无效运行**：任一硬闸门失败 ⇒ 该次运行**作废**，重跑，**不计入**对比
  （避免把流程失败误读为质量差异）。
- **观察指标**只记录原始值与指标值，**不做阈值判定**（阈值留给后续对比任务）。
- **两臂差值指标**：`question_length` 均值差、`cites_resume_focus` 命中率差、
  `max_verbatim_run_vs_knowledge` 均值差、`type_matches_stage` 命中率差、
  `reason_non_empty` 命中率差。
- **本轮不评价**：语义正确性 / 教学质量打分（无人工标注，不引入 LLM 评委）、
  两臂绝对分数（LLM 非确定性）、延迟与 token 消耗（属性能基准）。

## 6. 有效性前提（★ 最重要的一条）

默认 Embedding 是 `HashEmbeddingService`（`semantic_enabled=False`，**词面哈希**），
`VectorStore` 只按相似度排序、**不做相关性判断**。由此推出两条硬性要求：

1. **每个场景必须声明 `min_score`**。不设阈值时**全部 `top_k` 候选都会被注入**，
   含不相关文档——那测的是「RAG 噪声」而不是「RAG 知识」。
2. **每个场景的 query topic 必须实测筛选**。「语义上相关」**不等于**「检索得到」。
   实测 12 个候选里只有 6 个可用：

   | 可用 | query | 目标文档名次 | 与次高分的间隔 |
   |---|---|---|---|
   | ✅ | MySQL 索引 | 1 | +0.391 |
   | ✅ | Spring Boot 自动配置 | 1 | +0.405 |
   | ✅ | 分布式缓存一致性 | 1 | +0.063 |
   | ✅ | Spring 事务与传播行为 | 1 | +0.061 |
   | ✅ | MySQL 事务隔离级别与锁 | 1 | +0.032 |
   | ✅ | Redis 持久化 | 1 | +0.027 |
   | ❌ | Redis 数据结构 | **2** | −0.107 |
   | ❌ | JVM 内存结构与垃圾回收 | 1 | +0.017（间隔过窄） |
   | ❌ | JVM 线上问题诊断 | **2** | −0.041 |
   | ❌ | Java 并发线程池与锁 | **2** | −0.052 |
   | ❌ | Kafka 消息可靠性与幂等 | 1 | +0.008（间隔过窄） |
   | ❌ | HTTP 与 HTTPS | **4** | −0.085 |

   判据：`usable = (目标文档 rank == 1) and (间隔 > 0.02)`。
   完整的 12 行（含精确分数）在 JSON 的 `topic_screening` 里，由自检脚本**逐行复算**。

   ⇒ **新增场景必须先跑这张筛选表**，不能凭「看起来相关」就加进来。

## 7. 怎么用（后续任务）

1. 改任何业务文件后先跑 `python backend/tests/test_interview_eval_benchmark.py`
   ——它会告诉你基准是否还成立（指纹、筛选表、armed、条件摘要）。
2. 真正做对比时：对每个场景 × 每个臂，用**同一条 Mock 回复**跑一次，收集 5 个字段；
   先过硬闸门筛掉无效运行，再算差值指标。
3. 若要比较**生成质量**（而不是输入条件），需要另设方案：同参数多次采样比较分布，
   或引入人工 / LLM 评委。**不在本基准范围内**。
