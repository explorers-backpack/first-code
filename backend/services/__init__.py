# -*- coding: utf-8 -*-
"""backend.services —— 业务服务层。

当前包含：
- ``resume_scoring``：简历 8 维可取证评分（纯规则、确定性、每维附原文证据）
- ``job_matching``：岗位技能匹配（``keyword_match`` / ``match_job``，
  纯规则、确定性；原 ``main.py`` 中重复定义两份的 ``keyword_match``
  已合并至此，作为全项目唯一实现）

AI 模拟面试（四层分层 + 生成策略层）
------------------------------------
::

    api/interview.py              ← HTTP 契约（参数校验 / 响应模型 / 鉴权）
        └── interview_service     ← ① Service：API 流程 / Session 管理 / 前端交互
              └── interview_core  ← ② Core：流程控制 / Context / Plan / Agent / Validator
                    ├── interview_context    上下文获取
                    ├── interview_planner    计划获取
                    ├── interview_agent      ③ Agent：LLM 调用 / Prompt 构造 / 候选问题
                    └── question_validator   ④ Validator：问题校验 / 字段标准化
                    └── knowledge_retriever  知识检索（RAG 扩展点，**仅 agent 出题路径**）

    生成策略层（可选，供调用方显式选择出题方式）：
    question_generator ──┬── RuleQuestionGenerator  → interview_core.build_question_plan
                         └── AgentQuestionGenerator → interview_core.generate_next_question
                                                          └── KnowledgeRetriever（透传 retriever）

    交互通道层（按 interview_mode 分流「怎么交付」，不参与「面什么」）：
    interview_service._deliver ──┬── text   → 原样返回（既有流程）
                                 └── avatar → avatar_interview.enter（空实现）

    RAG 接线（唯一一处，只服务 agent 出题路径）：
    generate_next_question → retrieve_knowledge → retriever.retrieve(job, topic, context)
                           → Agent(knowledge_context=...) → Validator

- ``avatar_interview``：**数字人视频面试通道**（``interview_mode == "avatar"`` 的入口）。
  **预留入口，当前为空实现**：``enter(db, session, payload)`` 原样透传交付内容，
  因此 avatar 会话与 text 会话拿到完全一样的结果，**不会阻塞任何流程**。
  ``STATUS = "pending"``、``INTEGRATIONS = {iflytek: False, asr: False, tts: False}``
  明确标注**未接讯飞数字人 / ASR / TTS**。接入时**只改 ``enter`` 函数体**
  （签名已预留 ``db`` / ``session`` 供登记数字人会话），Service 与 Core 都不用动。
  **不参与出题 / 评分 / 报告**，且**不反向 import** ``interview_core`` /
  ``interview_service`` / ``main``（避免环与拉入 Spark）。

- ``knowledge_retriever``：**知识检索接口（RAG 扩展点，不实现真实 RAG）**。
  ``KnowledgeRetriever.retrieve(job_info, topic, context) -> List[KnowledgeChunk]``
  —— 基类即**默认空实现**，**无知识时返回 ``[]``**（绝不返回 ``None``）。
  ``KnowledgeChunk`` 是 ``@dataclass(frozen=True)`` 值对象，
  ``to_dict()`` 恒为三键 ``{content, source, metadata}``；``from_dict()``
  供上游适配器归一（未知键忽略、类型不符抛 ``InvalidChunkError``）。
  ``MockKnowledgeRetriever`` 返回固定片段 ``MOCK_CHUNKS``（忽略入参、记录 ``calls``）。
  **零依赖**：只 import 标准库，不碰数据库 / 向量库 / LLM / HTTP；
  ``retrieve`` 签名**刻意不含 ``db``**，且**提前异步化**（真实检索需访问外部服务，
  反过来改会破坏所有调用点）。
  **接线点全项目唯一**：``interview_core.retrieve_knowledge``——由
  ``generate_next_question`` 在 plan 之后、Agent 之前调用，**仅 agent 出题路径**
  （规则出题走 ``build_question_plan``，根本不经过，故不可能触发检索）。
  **默认不接真实知识库**：``retriever`` 缺省时用 ``KnowledgeRetriever()`` 空实现
  → 无知识 → 出题行为与引入本能力之前完全一致；真实检索器由调用方**显式注入**，
  **不做全局单例**（避免「悄悄接上 RAG」）。

- ``question_generator``：**题目生成策略层**。把「用哪种方式出题」收敛为可替换的
  生成器对象，让规则与 Agent **共存且互不影响**：
  ``await generate_questions(db, session_id, mode, context=None, plan=None, *,
  retriever=None, spark=None)``，
  ``mode`` ∈ ``("rule", "agent")``（默认 ``rule``）。
  两种模式返回**同一形状**的结果信封（``GENERATED_SET_FIELDS``：
  ``mode`` / ``ok`` / ``questions`` / ``question`` / ``count`` / ``reason`` /
  ``stage`` / ``errors`` / ``error``），题目项恒为 6 字段
  （``QUESTION_ITEM_FIELDS``，与 ``build_question_plan`` 产出一致），
  故 rule 模式结果可与 ``build_question_plan`` **逐字段比对**。
  **知识检索只走 agent 路径**：``retriever`` 是 keyword-only 可选参数，
  只有 ``AgentQuestionGenerator`` 会把它透传给 Core；``RuleQuestionGenerator``
  **接收但绝不使用**（方法体内无任何检索调用），且规则出题不经 Core 的
  ``generate_next_question``——两道保险叠加，「rule 不触发检索」是结构性保证。
  ``mode`` 非法抛 ``UnknownModeError``（继承 ``QuestionGeneratorError`` + ``ValueError``）。
  **不改动** ``start_session`` 默认行为，**不删除** ``build_question_plan``；
  依赖方向为「上层策略 → Core 原语」，**Core 不反向 import 本模块**（否则成环）。

- ``interview_service``：① **Service（会话与 API 门面）**。
  只做三件事：Session 管理（创建 / 开始 / 查询 / 作答 / 结束 / 读报告，
  含状态机守卫与归属校验）、数据访问（``_load_*``，**全项目唯一接触 DB 的地方**）、
  前端交互契约（``*_to_dict`` 序列化）。
  另有 ``generate_next_question``（按需出题的**大模型通道**）：Service 只做归属校验，
  其余全部委托 ``interview_core``——**Service 永不直接调用 Agent / Validator**。
  **交互模式分流**只在 ``_deliver``（交付边界）一处：``text`` 原样返回（默认路径
  零变化），``avatar`` 交给 ``avatar_interview.enter``。**面试逻辑一行都不分流**——
  出题 / 评分 / 报告 / 状态机对两种模式完全共享。
  **不再自行实现**出题计划 / 评分 / 报告——这些已迁至 ``interview_core``；
  为兼容既有调用方，旧名字（``score_answer`` / ``_plan_question_types`` 等）
  以**别名再导出**，与 Core 是同一对象（非复制实现）。
- ``interview_core``：② **Core（面试流程控制层）**。
  三件事：(1) **业务规则**（纯函数、确定性）——出题计划 ``build_question_plan``、
  作答评分 ``score_answer``、报告汇总 ``build_report``；
  (2) **编排接缝**（全项目唯一一处）——``load_context`` / ``build_interview_plan``
  / ``build_interview_plan_for`` / ``generate_candidate_question``
  / ``validate_candidate_question``，把 Context / Plan / Agent / Validator
  四个协作者收口，Service 不再直接依赖它们；
  (2.5) **知识检索接缝**——``current_topic(plan, context)``（纯函数，推导「当前待考察
  知识点」）与 ``retrieve_knowledge(job, topic, context, *, retriever=None)``
  （**全项目对 ``knowledge_retriever`` 的唯一接线点**，缺省用空实现 → 无知识）。
  检索是**可选增强**：抛异常时静默降级为「无知识」并在结果 ``warnings`` 记
  ``WARNING_KNOWLEDGE_FAILED``，**绝不让出题中断**（与 Planner 的 Spark 增强同约定）；
  (3) **流程入口 ``generate_next_question(db, session_id, *, retriever=None)``**
  ——「文字面试」与「数字人面试」共享的**同一个面试核心**：``session → context → plan
  → KnowledgeRetriever（可选）→ Agent（生成）→ Validator（校验）→ InterviewQuestion``。
  四道闸门（不存在 / 非本人 / 已答完 /
  已结束），失败返回稳定错误码（``session_not_found`` / ``session_finished`` /
  ``all_answered`` / ``agent_failed`` / ``validation_failed``），
  结果字段集恒定（``QUESTION_RESULT_FIELDS``），**不写库**（除幂等创建上下文外）。
  **零数据库耦合**（不 import ``models`` / ``database``，``AsyncSession`` 仅用于
  类型标注）、不 import FastAPI 请求上下文；需读库的接缝由调用方注入 ``db``，
  故业务规则可脱离 DB 与 HTTP 单测。
  注意：``start_session`` 仍走**纯规则** ``build_question_plan``，
  **未**切到 ``generate_next_question``——切换会让出题变为依赖大模型，
  属业务效果变更，需显式授权。
- ``interview_context``：AI 模拟面试的**上下文管理**（每场面试一份独立上下文，
  记录阶段 / 已问问题 / 已覆盖与薄弱知识点 / 追问次数 / 题量上限；
  上下文存于独立表 ``interview_context``，与 ``interview_session`` 1:1）
- ``interview_planner``：AI 模拟面试的**计划制定**（由「简历 + 岗位 + 面试配置」
  产出结构化 InterviewPlan：阶段权重 / 各阶段目标题量 / 目标知识点 /
  优先考察点 / 简历重点；**只制定计划、不出题**。纯规则确定性实现，
  可选调用既有 Spark 服务增强知识点抽取，失败自动回退）
- ``interview_agent``：AI 模拟面试的**问题生成**（按「上下文 + 计划 + 简历 + 岗位」
  生成**一道**问题，经解析与校验后返回结构化 InterviewQuestion）。
  **只做出题**：不做回答评分、动态追问、下一题决策、面试报告；
  不读库不写库（持久化由调用方负责）。  Prompt 取自 ``prompts/interview/``，
  失败最多修复一次，绝不伪造问题。
  **外部知识上下文（可选）**：``generate_question(..., *, knowledge_context=None)``
  —— ``knowledge_context`` 是 **keyword-only**（既有 4 个位置参数顺序不变，
  老调用方零影响），形状宽松（str / dict / ``KnowledgeChunk`` 风格对象 / 混合列表 /
  集合），由 ``normalize_knowledge_context`` 归一为**去重保序的非空文本行**；
  数字 / 布尔等非知识标量**直接丢弃**（不把 repr 塞进 Prompt）。
  **Agent 不调用任何 Retriever**（不 import ``knowledge_retriever``）——知识由**调用方
  注入**（生产链路上就是 Core 的 ``retrieve_knowledge`` 接缝），
  是否检索、检索什么 topic 都由上层决定，Agent 只负责「把拿到的知识写进 Prompt」。
  **空知识 → 走原模板 ``question.txt``，渲染结果与引入知识前逐字节相同**
  （要求「行为完全一致」）；非空 → 走变体模板 ``question_knowledge.txt``
  （原 10 变量 + ``knowledge_context``）。用**两个模板**而非单模板条件块，是因为
  ``prompts`` 严格模式双向校验变量，单模板在空知识时也会渲染出「参考知识：（无）」。
  两模板一致性由 ``tests/test_agent_knowledge.py`` 的漂移守卫锁死。
  **修复路径不含知识**（``question_repair.txt`` 未引入该变量）。
- ``question_validator``：面试问题的**校验器 + 标准化器**。
  ``QuestionValidator.validate(question_data, context, plan) -> ValidationResult``，
  ``errors`` 中为**稳定错误码**（``question_empty`` / ``topic_empty`` /
  ``invalid_difficulty`` / ``duplicate_question``）。
  校验通过后按固定六字段契约（``NORMALIZED_FIELDS``）产出
  ``normalized_question``：``difficulty`` 缺失时回落到 ``plan.difficulty``，
  ``expected_points`` 补 ``[]``，``reason`` / ``question_type`` 补 ``""``。
  重复检测对照 ``context.asked_questions``：**完全相同**拒绝，
  **高度相似**只记 warning（纯字符规则，不用 embedding / 向量库 / RAG）。
  零第三方依赖（不 import fastapi / sqlalchemy / main / models），纯内存、可脱离
  HTTP 与数据库单测，**纯函数**（不改入参、同输入同输出）。
  当前**未实现**：Agent 委托（``interview_agent.validate_question`` 的内联规则
  仍在，rule-4 待移除；在此之前两套校验并存，但 Core 的流程以 Validator 为准）。

设计约定
--------
- service 层不依赖 FastAPI 的请求上下文，函数以 ``db: AsyncSession`` 为第一参数
  （依赖注入），返回普通 dict；HTTP 相关职责由 ``api/`` 层承担。
- 分数一律由确定性规则产出，保证同输入同输出、可复现、可申诉。
- 依赖方向严格单向：``api → service → core → (context | planner | agent | validator)``，
  另有两条**旁支**边：策略层 ``question_generator → core``、
  通道层 ``service → avatar_interview``（两者都**不得反向**被 core 依赖），
  再加一条**可选**依赖 ``core → knowledge_retriever``（Core 在
  ``retrieve_knowledge`` 内**延迟导入**；``knowledge_retriever`` 零依赖、不反向），
  **不允许反向或跨层直连**（``interview_core`` 不 import ``interview_service``）。
- 判断「A 是否 import B」**必须用 AST 解析 import 语句**，不要用 ``"B" in source``
  子串匹配——本文件的 docstring 就提到大量模块名，子串匹配必然误判。
"""
