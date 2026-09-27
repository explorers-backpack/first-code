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
                        └── knowledge_rag    组装器：检索器由谁拼、用哪个 Embedding / 向量后端
                                             （``resolve_retriever`` 延迟导入，默认不启用）

    生成策略层（可选，供调用方显式选择出题方式）：
    question_generator ──┬── RuleQuestionGenerator  → interview_core.build_question_plan
                         └── AgentQuestionGenerator → interview_core.generate_next_question
                                                          └── retriever / use_rag（透传）

    交互通道层（按 interview_mode 分流「怎么交付」，不参与「面什么」）：
    interview_service._deliver ──┬── text   → 原样返回（既有流程）
                                 └── avatar → avatar_interview.enter（空实现）

    RAG 接线（唯一一处，只服务 agent 出题路径；默认关闭）：
    generate_next_question(use_rag=True)
      → resolve_retriever（唯一开关：显式 retriever > use_rag=True > 不接）
      → retrieve_knowledge → retriever.retrieve(job, topic, context)
      → Agent(knowledge_context=...) → Validator

    知识库入库（写侧，与面试链路**完全解耦**，面试侧不 import 它）：
    knowledge_document_service → models.knowledge.KnowledgeDocument（只写文档主表，**不切片**）
    document_chunker → [{content, metadata}, …]（纯内存，不落库）
    embedding_service → [float]（纯内存，不落库）
    vector_store → VectorStore 接口（add / search / count）+ InMemoryVectorStore（零依赖、不接 DB）
    vector_store_sql → SqlAlchemyVectorStore（把向量写进 knowledge_chunk 的 embedding 三列）
    vector_store_chroma → ChromaVectorStore（真实 HNSW **ANN** 索引；组合 vector_store_sql 作权威行读写器）
    vector_knowledge_retriever → KnowledgeRetriever 的**真实实现**（读侧：query → 向量 → 相似切片）
    knowledge_import_pipeline → **唯一编排者**：把上面四环串成
        「文档 → 切片 → 向量 → 落库 → 向量库」一条流程（幂等 + 失败有明确状态）
    knowledge_rag → **唯一组装器**：写侧 build_vector_store / 读侧 build_vector_retriever
        （检索器与入库 Pipeline 都只靠注入拿协作者，都不认识具体后端类；
        用哪个后端由 ``VECTOR_STORE`` 决定，不设 = vector_store_sql）

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

- ``knowledge_document_service``：**知识库文档导入**（只落库，不做 RAG）。
  ``create_document(db, payload)`` 把 ``{title, content, category, source}`` 写成一条
  ``KnowledgeDocument``，返回整篇 dict（契约 ``DOCUMENT_FIELDS`` 恒 6 键）；
  ``get_document(db, id)`` 单篇读回（含正文，不存在 404）；
  ``list_documents(db, *, category=None, keyword=None, limit, offset)`` 分页读**元信息**
  （契约 ``DOCUMENT_SUMMARY_FIELDS`` 恒 5 键，**刻意不含 ``content``**，避免整库正文回传），
  返回 ``{total, items, limit, offset}``；非法 ``category`` 一律 400（不静默返回空）。
  入参用 ``_read_field`` 归一（**映射取键 / 对象取属性**），故 ``dict`` /
  ``SimpleNamespace`` / Pydantic 模型均可直接传入。字段口径：``title`` / ``source``
  属标签，落库前 ``strip()``；``content`` 是正文，**原样保存**（保真、不静默改写），
  但**空判定**用 ``content.strip()``（纯空白视为空 → 400）。业务错误抛
  ``HTTPException``（400 参数非法 / 404 不存在），与 ``interview_service`` 同口径，
  将来加路由可原样透传。
  **本阶段刻意不做**：文档解析（PDF/Word/MD → 文本）、**切片（``KnowledgeChunk``
  一行都不写）**、Embedding、向量库、真实 Retriever、HTTP 路由（先要定「谁能导入」，
  属权限设计）、去重（同一 ``title`` 可多次导入）。与面试链路**完全解耦**：
  不 import Retriever / 任何 Interview 模块，面试侧也不 import 它。

- ``document_chunker``：**文档切片**（正文 → 带上下文的切片列表，**只切片、不落库**）。
  ``DocumentChunker(chunk_size=500, chunk_overlap=80).split(content, *, document_id,
  category, source)`` → ``[{content, metadata}, …]``（契约 ``CHUNK_FIELDS`` 恒两键）；
  ``split_document(document)`` 直接吃 ``KnowledgeDocument``（ORM 行 / dict / 任意带同名
  属性的对象），自动把 ``id`` / ``category`` / ``source`` 搬进 metadata。
  三步策略与需求逐条对应：①**按长度切分**（每片 ≤ ``chunk_size``）；
  ②**保留上下文**——相邻片**重叠** ``chunk_overlap`` 字，且切点回退到窗口后半段内的
  **最粗**自然边界（段落空行 → 换行 → 句末标点 → 句内停顿 → 空白），
  两条合起来给出可验证的强性质：**每句话都完整出现在某一片里**（测试逐句取证）；
  ③**保存 metadata**——``{document_id, category, source, chunk_index}``
  （前三键是需求点名，``chunk_index`` 为追加）。
  空正文 → ``[]``（不产占位片）；``len <= chunk_size`` → **整篇 1 片**（短文本保持完整）。
  参数 keyword-only 且校验严格（``0 <= overlap < size``，否则 ``ChunkConfigError``，
  同时是 ``ValueError``）；``overlap`` 极大时退化为不重叠，**保证必然终止**。
  **零第三方依赖**（只 import ``re`` / ``collections.abc`` / ``typing``，
  不 import ``models`` / ``database`` / ``fastapi`` / Retriever），
  纯内存、纯函数（同输入同输出）；**不提供全局单例**。
  **本阶段刻意不做**：Embedding、向量库、Retriever、**落库**（写 ``knowledge_chunk``
  是下一步；模块文档给了 3 行映射示例，注意属性名是 ``chunk_metadata``）。

- ``embedding_service``：**文本向量化**（``text → vector``，**只算向量、不落库**）。
  抽象基类 ``EmbeddingService`` 只规定一个钩子 ``_embed_one``，而
  ``embed(text)`` / ``embed_batch(texts)`` 是**带统一校验的模板方法**——
  于是「换模型」= 写一个子类、实现一个方法、声明 ``name`` / ``dimension``，其余零改动；
  ``name`` 会被写进 ``KnowledgeChunk.embedding_model``（换模型后据此找出旧向量重算）。
  两个自带实现都**零第三方依赖**：``HashEmbeddingService``（``hashlib.blake2b`` 哈希散列，
  离线、确定性、跨进程稳定——**不是语义向量**，只用于打通链路与单测）与
  ``MockEmbeddingService``（固定向量 + ``calls`` 记录，测试替身）。
  **异常四分类**（入参错 vs 服务错必须能分流，真实模型接进来后处理方式完全不同）：
  ``EmbeddingError``（基类）／``EmbeddingInputError``（+``ValueError``，不该重试）／
  ``EmbeddingUnavailableError``（+``RuntimeError``，可重试或降级）／
  ``EmbeddingDimensionError``（+``ValueError``，实现写错，必须暴露而不是放过脏向量）。
  基类统一挡掉：空文本 / 纯空白 / 非字符串 / **把单个字符串传给批量接口**
  （``str`` 可迭代，不拦会被静默逐字符拆开）／向量非数值序列 / 空向量 / 维度不符。
  ``embed_batch`` 是**可覆盖点**（厂商原生批量接口是真实性能来源），默认逐条复用 ``embed``。
  **零依赖**：顶层 import 恰为 ``{__future__, abc, collections, hashlib, re, typing}``，
  不 import 任何厂商 SDK / HTTP 客户端 / 向量库，也不 import ``models`` / Retriever /
  Interview；**不做全局单例**，由调用方显式构造并注入。
  另有**运行状态标识**（供日志 / 调试）：``EmbeddingInfo(provider, dimension,
  semantic_enabled)`` + ``describe_embedding(embedder)``——**纯读取、零 IO、不编码文本**。
  「是否语义」由**各实现自行声明** ``semantic_enabled``（Hash / Mock 为 ``False``，
  真实 Provider 为 ``True``）——**基类一行未改**（接口面保持原样），未声明的实现按
  「非语义」**保守上报**。组装器侧入口是 ``knowledge_rag.describe_default_embedding()``，
  一句话回答「本进程默认会用什么 Embedding」。
  **本阶段刻意不做**：向量库、相似度检索、Retriever、**落库**。
  （真实厂商实现**不在本模块**——见下面 ``embedding_provider``。）

- ``embedding_provider``：**真实 Embedding 实现 + 配置 + 工厂**（OpenAI 兼容 ``/embeddings``）。
  单独一个模块的理由与 ``vector_knowledge_retriever`` 同源：``embedding_service`` 的
  「零第三方依赖」被守卫锁死，真实实现必然要发 HTTP，塞进去会当场破坏它。
  - ``EmbeddingProvider`` **只实现 ``_embed_one`` 钩子 + 覆盖 ``embed_batch``**，
    ``embed(text)`` 一行没重写 ⇒ 接口与校验口径完全沿用基类（**接口不变**）。
  - ``embed_batch`` 是**原生批量**：按 ``EMBEDDING_BATCH_SIZE`` 分批，顺序与入参一致，
    上游 ``data`` 带 ``index`` 时按 ``index`` 还原；入参校验在**任何 HTTP 之前**完成。
  - ``EmbeddingTransport`` 只有一个 ``post_json``；默认实现 ``HttpxEmbeddingTransport``
    **函数体内才 import httpx** ⇒ 本模块顶层依然只有标准库，测试注入假 transport 即可完全离线。
  - **配置全走环境变量**（``EMBEDDING_PROVIDER`` / ``_API_KEY`` / ``_BASE_URL`` /
    ``_MODEL`` / ``_DIMENSION`` / ``_TIMEOUT`` / ``_BATCH_SIZE``）；**一个都不配 → 离线占位**，
    配了密钥 → 真实模型。密钥在 ``config`` 里是 ``repr=False``，异常消息一律打码
    （上游回显也要抹）。
  - ``EmbeddingProviderConfigError``（+``ValueError``）留给**配置**问题，**构造期就报**——
    不该把「部署配错了」伪装成「检索不到知识」。
  - **消费者收口**：``knowledge_rag.default_embedder()`` 委托
    ``build_embedding_service()``，于是「用哪个模型」只有**一处**说法；
    读侧检索与写侧入库都从这里拿，绝不会出现「入库用 A、检索用 B」。

- ``vector_store``：**向量存储接口 + 内存实现**（``chunk + vector`` 的存取，**不绑定任何数据库**）。
  ``VectorStore`` 抽象接口三个方法：``add(records) -> int``（upsert 语义）、
  ``search(query_vector, *, top_k, model, document_id, category, min_score) -> List[VectorMatch]``
  （**余弦相似度**降序，``[-1, 1]``）、``count()``（**已向量化**条数）。
  接口签名里**刻意没有任何数据库概念**（无 ``db`` / session / engine / 表名）——
  需要 DB 的后端在**构造函数**里接依赖，于是「内存 / MySQL / 真实向量库」一视同仁。
  两个值对象：``VectorRecord``（写入单位，``__post_init__`` 自校验 → 造出来的一定合法；
  ``chunk_id`` 空=新建、有值=给既有切片补向量）与 ``VectorMatch``（命中结果，
  ``to_dict()`` **不含向量本体**——不把上千个浮点带回上层）。
  排序 / 同分兜底 / 维度策略收在**唯一函数** ``select_matches``，两个后端共用 →
  「换后端不换语义」是可测的（测试用同一数据在两个实现上比对结果）。
  **维度不符的处理是刻意设计的**：跳过维度不同的候选，但若「候选非空、却一条维度
  都对不上」，抛 ``VectorStoreDimensionError`` 而**不是安静返回空列表**——
  空列表只表示「确实没有候选」，把「拿错模型的向量在查」误判成「库里没数据」
  会浪费大量排查时间。
  相似度数学（``cosine_similarity`` / ``dot_product`` / ``l2_norm``）是纯函数：
  零向量返回 ``0.0``（不做除零）、长度不符**抛错而非静默截断**。
  ``InMemoryVectorStore`` 是**进程内**实现（不落库、退出即消失），只用于单测与本地打通链路。
  **零依赖**：顶层 import 恰为 ``{__future__, abc, collections, dataclasses, typing}``，
  不 import ``models`` / ``database`` / ``sqlalchemy`` / 任何向量库 / 任何 Interview 模块。
  ``add`` 的 upsert 契约（两后端一致）：``chunk_id`` 已存在时**只更新 ``vector`` / ``model``**，
  **不动** ``content`` / ``metadata`` / ``document_id``——向量层的职责是「给切片写向量」，
  批量补向量的调用方通常只传 ``chunk_id + vector``，整体覆盖会把切片正文清空。
  （唯一刻意差异：``chunk_id`` 有值但不存在时，内存实现视为新建、SQL 实现报错——
  因为后者指向的是真实切片行。）
  **本阶段刻意不做**：文本 → 向量（``embedding_service`` 的职责，本层只认**算好的向量**）、
  切片、Retriever、**接 InterviewAgent / Core / Service**。
  （真实 ANN 索引已在 ``vector_store_chroma``，由 ``knowledge_rag`` 按 ``VECTOR_STORE`` 选择。）

- ``vector_store_sql``：**向量存储的 SQLAlchemy 后端**（复用既有表 ``knowledge_chunk`` 的
  ``embedding`` / ``embedding_model`` / ``embedding_dim`` **扩展字段**，**不建新表**）。
  选扩展字段而非独立表：三列在任务 42 已预留（结构已就位、无需动 ``schema_sync``）、
  切片与向量 **1:1**（独立表只会多一次 JOIN 与一套孤儿行治理）、
  ``embedding IS NULL`` 天然表达「待向量化」。
  ``SqlAlchemyVectorStore(db, *, model=None)``：``db`` 由调用方注入（**按请求构造**，
  **不做全局单例**——session 有生命周期）；新建切片要求 ``document_id`` 与非空 ``content``，
  补向量时若 ``chunk_id`` 不存在**明确报错**（不悄悄新建）。
  ``embedding`` 是 **JSON 列，必须整体重新赋值**（原地改不落库，见项目约定）。
  **规模边界（别当成向量数据库）**：没有 ANN 索引，``search`` 是
  ``WHERE embedding IS NOT NULL`` 全表扫描 + Python 侧算余弦；``category`` 也在 Python 侧过滤
  （``metadata`` 是 JSON，写进 SQL 会失去 MySQL/SQLite 可移植性），
  只有 ``document_id`` 是**真列**、在 SQL 里过滤。适合几千条以内；上万或高 QPS 时
  改用下面的 ``vector_store_chroma``（``VECTOR_STORE=chroma``），**调用方一行不用改**
  （接口没变）。本模块同时扮演「**切片行的权威写入方**」：chroma 后端会组合它。
  容错：库里存在脏向量（人为改库 / 半截写入）时**跳过该行**，不让一条坏数据打挂整次检索。
  **本阶段刻意不做**：建表 / 改 ``schema_sync``、批量入库脚本、真实 Retriever、
  **接 InterviewAgent / Core / Service**。

- ``vector_store_chroma``：**向量存储的真实 ANN 后端**（chromadb / HNSW）。
  为什么另起模块：``vector_store`` 的**零第三方依赖**被守卫用 AST **相等**断言锁死，
  真实 ANN 库必然带依赖，只能放在接口模块之外（同 ``embedding_provider`` 的理由）。
  **架构：ANN 索引是派生数据，权威副本仍在 MySQL**——它**组合** ``SqlAlchemyVectorStore``：
  ``add`` 先落权威行（校验与字段覆盖范围沿用 SQL 后端，只更新向量三列），
  再把**从库里读回来的行**镜像进索引（索引内容不取调用方入参）。
  这样 ``knowledge_import_pipeline`` 的幂等判据（``row.embedding IS NULL``）仍然成立，
  索引也能随时从权威行重建。
  **检索 = ANN 召回 + 精确重排**：HNSW 取回 ``top_k × DEFAULT_OVERSAMPLE`` 个候选，
  再交给 ``vector_store.select_matches`` 精确排序，因此三个后端的
  **排序口径 / ``score`` 口径 / 同分兜底完全一致**（差别只在召回：ANN 是近似的）。
  **维度契约更严**：一个 HNSW 索引只支持单一维度，批次内混维度 / 与索引维度不符 /
  查询维度不符都抛 ``VectorStoreDimensionError``，且**在写库之前**抛（不留半截状态）。
  元数据：标量扁平化供 ``where`` 过滤（``category`` 因此在**索引层**过滤，比 SQL 后端更早），
  ``_meta`` 存完整 JSON 保真还原（chroma 元数据只接受标量）。
  ``chromadb`` 是**可选依赖**（模块内函数体延迟导入），不装也能跑默认后端；
  客户端本地且同步（**不丢线程池**：本地操作微秒级，而 chroma 的 SQLite 连接不保证可跨线程）。
  **删/重建的边界**：提供两个**索引维护原语** ``delete(chunk_ids)`` / ``reset()``
  ——它们**只动派生 ANN 索引、绝不动权威行**，且**刻意不是 ``VectorStore`` 接口方法**
  （同 ``vector_store_sql.add_returning_ids`` / ``load_records`` 的先例，
  接口仍是 ``add`` / ``search`` / ``count``）。**不提供** ``rebuild``：
  从权威行重建要读 ``knowledge_chunk``，那是**编排**，归
  ``knowledge_maintenance.rebuild_index``。
  **仍刻意不做**：``clear`` 全局单例、改 ``VectorStore`` 接口、
  **接 InterviewAgent / Core / Service**。

- ``vector_knowledge_retriever``：**真实知识检索器**（``KnowledgeRetriever`` 接口的
  向量检索实现，**取代 Mock 的生产位置**）。
  ``VectorKnowledgeRetriever(embedder, store, *, top_k, min_score, category,
  document_id, model, dedup)``——两个协作者**全部注入**，因此本模块**只依赖接口**：
  ``query → build_query → embedder.embed → store.search → KnowledgeChunk``。
  流程要点：
  ① **``build_query`` 是纯函数**，按 ``topic → context.current_stage → 岗位名`` 的
  优先级取**一个**来源。刻意**不**把 topic + 岗位 + 上下文拼成长 query——
  拼接会**稀释**语义向量（长 query 的向量是各 token 的平均方向），
  检索精度反而下降；真正的加权应作用于 **score**（排序层），不是 query。
  ② **``model`` 默认取 ``embedder.name``**（不同模型的向量不可比）→ 只跟同模型算分。
  ③ 结果 ``VectorMatch → KnowledgeChunk``：``metadata["source"]`` **提升**为顶层
  ``source``（并从中移除，避免两份），``chunk_id`` / ``document_id`` /
  ``embedding_model`` / ``score`` 写进 ``metadata``；**空正文跳过、重复正文去重**；
  ``content`` **原样保留**（可取证，不悄悄改写正文）。
  ④ **接口与值对象一行都没改**：``retrieve(job_info, topic, context)`` 签名逐字一致，
  返回仍是三键的 ``KnowledgeChunk``（该契约由守卫测试锁死，Agent 也只读
  ``content`` / ``source``）。相似度落在 ``metadata["score"]``，
  因此**不会渗进 Agent 的 Prompt**；要摊平的 ``{content, source, score}`` 视图用
  ``chunk_to_result()``。
  ⑤ **异常按类型分流**（``[]`` 只表示「没知识」）：embedder 入参错 / 维度错、
  向量库维度不符（多半是**换了模型没重算向量**）→ ``RetrieverConfigError``(+``ValueError``，
  重试没用)；embedder / 向量库不可用或其他故障 → ``RetrieverUnavailableError``
  (+``RuntimeError``，可重试或降级)。原始异常一律挂 ``__cause__``。
  **没有查询线索 → 直接返回 ``[]`` 且不调用 embedder / 向量库**（省一次外部调用）。
  **配置错误在构造期就报**（缺 ``embed`` / 缺 ``search`` / ``top_k`` 非法 …），
  不把「部署配错了」伪装成「检索不到知识」。
  **零第三方依赖**（顶层只有 ``__future__`` / ``collections`` / ``typing`` / ``services``），
  尤其**不 import** ``vector_store_sql``（那会把 SQLAlchemy 拉进来）、不 import
  ``models`` / ``database`` / 任何 Interview 模块。
  **本阶段刻意不做**：真实向量库（ANN）、
  **把本检索器接进 ``start_session``**（那会把出题从确定性规则变成依赖大模型 + 向量库，
  属业务效果变更，需显式授权）。**不提供全局单例**，由调用方显式注入
  （生产链路上是 ``interview_core.retrieve_knowledge`` / ``generate_next_question``
  的 ``retriever`` 参数；**默认由 ``knowledge_rag.build_vector_retriever`` 组装**，
  仅在显式 ``use_rag=True`` 时）。

- ``knowledge_rag``：**RAG 组装器（全项目唯一一处「知道用哪个 Embedding、
  哪个向量后端」的地方）**。两个方向各一个入口：

  - **读侧** ``build_vector_retriever(db, *, embedder=None, model=None,
    **retriever_kwargs) -> VectorKnowledgeRetriever``——把 ``embedding_service``
    与 ``vector_store_sql`` 拼成检索器（生产调用方＝``interview_core.resolve_retriever``）；
  - **写侧** ``build_vector_store(db, *, model=None) -> VectorStore``——入库 Pipeline 用
    （生产调用方＝``knowledge_import_pipeline``）。**写侧也经这里拿后端**，
    所以「换向量库只改本模块」对读写两侧都成立。
  - ``default_embedder()`` **按 ``EMBEDDING_*`` 配置选实现**（委托
    ``embedding_provider.build_embedding_service()``）：一个变量都不配 → **离线占位**
    ``HashEmbeddingService``（无语义，只为跑通链路，**默认行为不变**）；配了
    ``EMBEDDING_API_KEY`` → 真实模型。**读写两侧共用它**，因此「用哪个模型」只有一处
    说法——否则入库用 A、检索用 B，会表现为「明明入库了却检索不到」。
    也可以直接给 ``build_vector_retriever(embedder=...)`` 覆盖。

  存在的理由：**检索器与入库 Pipeline 都只认注入**，若不收口，每个调用方都要自己挑
  Embedding 与向量后端，迟早出现「有人用 hash 占位、有人用真实模型」而向量不可比。
  ``db`` 缺失 / kwargs 非法 → ``RetrieverConfigError``（构造期就报，不伪装成「检索不到」）。
  **三个协作者全部延迟导入**（``vector_store_sql`` 会经 ``models`` 拉 ``database``，
  而 ``database`` 缺 ``DATABASE_URL`` 即 ``RuntimeError``）——因此本模块**模块顶层**
  只有标准库，**无 ``DATABASE_URL`` 也能 import**（子进程守卫证明）。
  **``model`` 默认 ``None`` → 检索器按 ``embedder.name`` 过滤**：拿 hash 占位去查
  真实模型建的库，只会得到 ``[]``（**不污染**），不会返回乱序结果；
  要造噪声必须写、查都用 ``hash-local``，属于刻意为之。
  **消费者收口为已知闭集**（守卫断言，精确相等）：
  ``["services/interview_core.py", "services/knowledge_embedding_migration.py",
  "services/knowledge_import_pipeline.py", "services/knowledge_maintenance.py"]``
  ——读侧 Core / 迁移 / 写侧入库 / 维护，四类各一处，没有第五个。
  **本阶段刻意不做**：把 RAG 打开进 ``start_session``（需显式授权）。

- ``knowledge_import_pipeline``：**批量知识入库（写侧唯一编排者）**。
  ``KnowledgeImportPipeline(db, *, chunker=None, embedder=None, store=None, model=None)``
  ＋ ``await import_document(document)``；把上面四环串成
  ``文档 → 切片 → 向量 → 落库切片 → 写向量库`` 一条流程。
  三个协作者**全部可注入**，默认值取 ``DocumentChunker()`` /
  ``knowledge_rag.default_embedder()`` / ``knowledge_rag.build_vector_store(db)``
  ——**本模块不认识 ``SqlAlchemyVectorStore``**，换向量库只改组装器。
  **顺序是刻意的：先编码、后落库**，编码失败时该切片行根本不产生，
  「半截数据」最少化，重跑即从那一处继续。
  **三层幂等（支持重复执行）**：① 文档按自然键 ``(title, source, category)`` 复用，
  不重复建；② 切片按 ``(document_id, chunk_index)`` upsert，不重复堆积；
  ③ 向量已存在**且同模型、正文未变**才跳过——**换模型**（``embedding_model`` 不符）
  或**改正文**（切点漂移）都必须重算，否则会留下指向旧内容的向量。
  全部跳过时 ``status="skipped"`` 且 ``ok=True``（**成功的空操作**，不是失败）。
  **失败不抛异常，返回同形状报告**：``ok/status/stage/document_id/chunk_count/
  saved_chunks/embedded_chunks/skipped_chunks/reused_document/failed_index/
  errors/error``（12 键恒定，``IMPORT_RESULT_FIELDS``）。``stage`` 指出失败在哪一步
  （``document``/``chunk``/``embedding``/``persist``/``vector``），``failed_index``
  指出第几片，``errors`` 是稳定错误码（``chunk_empty``/``embedding_failed``/
  ``persist_failed``/``vector_failed``），已完成计数**保留**，于是重跑能续做。
  **唯一抛异常的是入参契约**（空标题 / 空正文 / 非法 ``category``）——那不是
  「中途失败」而是「还没开始」，由 ``knowledge_document_service.create_document``
  抛 ``HTTPException(400)``（校验口径与消息复用同一处，本模块不重写）。
  **逐片编码**（不是 ``embed_batch``）：为了能给出 ``failed_index``；换性能可改。
  **刻意不做**：文件上传 / PDF 解析（入参已是纯文本）、HTTP 路由、**删过期切片**
  （改短文档后多出的旧切片会保留——静默删数据风险更大）、不做全局单例。

- ``knowledge_embedding_migration``：**换 Embedding 模型后的向量迁移（不改切片）**。
  ``EmbeddingMigration(db, *, embedder=None, store=None, target_model=None,
  target_dim=None, batch_size=8, pace_seconds=0, retries=0, retry_delay=1.0,
  max_failures=None, dry_run=False, progress=None)`` ＋
  ``await run(*, document_id=None, chunk_ids=None, limit=None)``。
  与 ``knowledge_import_pipeline`` 的**关键区别**：本模块**不切片、不新建行**，
  只把 ``embedding`` / ``embedding_model`` / ``embedding_dim`` 三列按当前模型重写
  ——``chunk_index`` / 正文 / ``metadata`` / ``document_id`` **逐字节不变**。
  之所以必须这样：旧库的切片是按旧切点切好的，用 chunker 重跑会让「切片 7」
  变成另一段文字，而向量是**按行**存的，错位后全部指向错内容。
  **写回走 ``VectorStore`` 接口**（``store.add`` 对「``chunk_id`` 有值」的契约
  恰好就是「只更新向量三列」），因此 ANN 后端的派生索引会同步镜像；
  **一行都没改 ``VectorStore`` / ``Retriever`` 接口**（守卫断言签名未变）。
  **幂等**：``embedding`` 非空 **且** ``embedding_model == target_model`` **且**
  ``embedding_dim == target_dim`` 才跳过 ⇒ 复跑是**成功的空操作**
  （``status="skipped"``），且**一次编码都不会发生**。
  **失败恢复**：逐片编码、逐批写回（一批一次 commit）；单片编码失败记进
  ``failures`` 且**同批其它片照常迁移**；整批写回失败先 ``rollback`` 再记该批全失败，
  后续批次继续；失败明细带 ``chunk_id``，脚本可 ``--retry-failed`` **精确补漏**。
  **报告 16 键恒定**（``MIGRATION_RESULT_FIELDS``）：
  ``ok/status/stage/scanned_chunks/pending_chunks/migrated_chunks/skipped_chunks/
  failed_chunks/batches/target_model/target_dim/dry_run/stopped_early/errors/
  failures/error``；状态 ``ok``/``skipped``/``partial``/``failed``。
  **刻意不做**：自动触发（只由迁移脚本显式调用）、删旧向量（是覆盖写不是追加）、
  认识任何具体向量后端（经组装器拿）、用离线占位跑迁移（脚本侧安全闸拦下）。

- ``question_generator``：**题目生成策略层**。把「用哪种方式出题」收敛为可替换的
  生成器对象，让规则与 Agent **共存且互不影响**：
  ``await generate_questions(db, session_id, mode, context=None, plan=None, *,
  retriever=None, use_rag=False, spark=None)``，
  ``mode`` ∈ ``("rule", "agent")``（默认 ``rule``）。
  两种模式返回**同一形状**的结果信封（``GENERATED_SET_FIELDS``：
  ``mode`` / ``ok`` / ``questions`` / ``question`` / ``count`` / ``reason`` /
  ``stage`` / ``errors`` / ``error``），题目项恒为 6 字段
  （``QUESTION_ITEM_FIELDS``，与 ``build_question_plan`` 产出一致），
  故 rule 模式结果可与 ``build_question_plan`` **逐字段比对**。
  **知识检索只走 agent 路径**：``retriever`` / ``use_rag`` 都是 keyword-only 可选参数，
  只有 ``AgentQuestionGenerator`` 会把它们透传给 Core；``RuleQuestionGenerator``
  **接收但绝不使用**（方法体内无任何检索调用，也**不组装** RAG），且规则出题不经
  Core 的 ``generate_next_question``——两道保险叠加，「rule 不触发检索」是结构性保证。
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
  另有 ``resolve_retriever(db, retriever=None, use_rag=False) -> (retriever, warnings)``
  ——**全项目唯一的「RAG 开关」**（同步、不发起 IO）：显式 ``retriever`` 优先 >
  ``use_rag=True`` 时调用组装器 > 都不给则 ``None``。组装失败**不抛异常**，
  降级为 ``(None, [WARNING_KNOWLEDGE_FAILED])``（与检索失败同一约定）。
  ``use_rag`` **默认 False**：不显式要求就绝不打开 RAG，既有行为逐字节不变；
  (3) **流程入口 ``generate_next_question(db, session_id, *, user_id=None, spark=None,
  context=None, plan=None, retriever=None, use_rag=False)``**
  ——「文字面试」与「数字人面试」共享的**同一个面试核心**：``session → context → plan
  → resolve_retriever → KnowledgeRetriever（可选）→ Agent（生成）→ Validator（校验）
  → InterviewQuestion``。
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
  **进 Prompt 的组装口径**（任务 72）由 ``format_knowledge_context`` 负责：
  只取 ``content`` + ``source``（``metadata`` 从不进 Prompt）、**同源多片合并成一块**
  （来源标注每文档只写一次）、**去掉切片重叠**（``document_chunker`` 的 80 字重叠）、
  并按 ``KNOWLEDGE_CONTEXT_MAX_CHARS`` 截断。单条 / 每条来源仅一片时输出与
  ``normalize_knowledge_context`` **逐字节相同**。
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
  以及组装器边 ``core → knowledge_rag → (embedding_service | vector_store_sql
  | vector_knowledge_retriever)``（``resolve_retriever`` 内**延迟导入**，
  否则会把 SQLAlchemy 与 ``database`` 拉进 Core 的模块顶层），
  再加一条**写侧**边 ``knowledge_import_pipeline → knowledge_rag →
  vector_store_sql``（入库 Pipeline 经组装器拿向量后端，**不认识具体后端类**），
  **不允许反向或跨层直连**（``interview_core`` 不 import ``interview_service``；
  入库 Pipeline 不 import 任何 ``interview_*``，面试侧也不 import 它）。
- **知识库流水线与面试链路解耦**（面试侧只经「开关 + 组装器」两处间接接触）：
  ``knowledge_document_service``（导入）→ ``document_chunker``（切片）→
  ``embedding_service``（向量化）→ ``vector_store`` / ``vector_store_sql``（存取）→
  ``vector_knowledge_retriever``（检索）→ ``knowledge_rag``（组装）。
  前六环每一环都**只做自己那一步**；相邻环之间**没有**调用关系——
  把它们串起来的只有 ``knowledge_import_pipeline``（写侧）与
  ``interview_core``（读侧），一读一写各一处。
  唯一的**实现关系**是 ``vector_knowledge_retriever → knowledge_retriever``（实现接口）
  与 ``vector_knowledge_retriever → vector_store``（按注入消费接口）——
  **这两条是「实现 / 消费接口」，不是「接线」**。
  **真正把 RAG 接进业务路径的只有两处**：组装器 ``knowledge_rag``
  （选择用哪个 Embedding / 向量后端）与 Core 的 ``resolve_retriever``（决定要不要接）。
  且**默认关闭**（``use_rag=False``）、``start_session`` 仍走纯规则——
  所以「RAG 不影响既有业务」是结构事实，不是口头承诺。
  这样每一环都能脱离 DB 与 HTTP 单测。
- 判断「A 是否 import B」**必须用 AST 解析 import 语句**，不要用 ``"B" in source``
  子串匹配——本文件的 docstring 就提到大量模块名，子串匹配必然误判。
"""
