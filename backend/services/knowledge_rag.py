# -*- coding: utf-8 -*-
"""AI 模拟面试 · 真实 RAG 链路的**组装器**。

为什么需要一个组装器
--------------------
RAG 链路由三段拼成，每段都刻意「不认识别人」：

::

    embedding_service            text → vector        （零依赖，不知道谁在用它）
        └── vector_store_sql     vector → 相似切片    （只认识 DB 与模型）
              └── vector_knowledge_retriever
                                 检索器               （只依赖**接口**，后端靠注入）

于是**必须有且只有一处**知道「该配哪个 Embedding、该用哪个向量后端」——
就是本模块。它把三者拼成 :class:`~services.vector_knowledge_retriever.VectorKnowledgeRetriever`，
交给面试流程使用（生产链路上是 ``interview_core.resolve_retriever``）。

**收口的意义**：想换真实向量库（chromadb / milvus / pgvector）只改
``build_vector_store`` 里选后端的那一处；想换 Embedding 模型只改 ``default_embedder()``
或调用方传 ``embedder=``。检索器、Core、Agent、Prompt **全都不用动**。

当前有两个向量后端，由 ``VECTOR_STORE`` 选择（**不设 = 默认 = 行为不变**）：
``sql``（``vector_store_sql``，无 ANN、全表扫描 + Python 侧余弦，适合几千条以内）
与 ``chroma``（``vector_store_chroma``，真实 HNSW ANN 索引）。
两者的**排序口径 / ``score`` 口径完全一致**（共用 ``vector_store.select_matches``），
差别只在**召回**：ANN 是近似的。

.. warning::
   **默认 Embedding 是离线占位、不是语义向量**。一个 ``EMBEDDING_*`` 变量都不配时，
   :func:`default_embedder` 返回 ``HashEmbeddingService``（blake2b 哈希散列），
   它只能保证「同文本同向量」，**没有语义**——用它检索出来的「相似度」基本是噪声。
   配好 ``EMBEDDING_API_KEY``（或 ``EMBEDDING_PROVIDER=openai``）后本函数即返回真实模型
   （``services/embedding_provider.py``），也可以直接给 :func:`build_vector_retriever`
   传 ``embedder=`` 覆盖。
   好消息是**默认不会造成污染**：检索器按 ``embedding_model == embedder.name`` 过滤，
   若知识库是用**另一个模型**向量化的，``search`` 会一条都匹配不到 → 返回 ``[]``
   （「无知识」），而不是把不相关的文本塞进 Prompt。
   只有「用 ``hash-local`` 给知识库打过向量、又用 ``hash-local`` 去查」才会拿到噪声——
   那需要两次刻意的选择。

.. note::
   **为什么 import 写在函数体内**：``vector_store_sql`` 会连带 import
   ``models`` → ``database``，而 ``database`` **缺少 ``DATABASE_URL`` 时直接
   ``RuntimeError``**。若写在模块顶层，本模块就会变成「没有 DATABASE_URL 就无法 import」，
   连带把 ``interview_core`` 的零耦合性质也破坏掉（Core 是**延迟导入**本模块的）。
   因此三个协作者全部**函数内延迟导入**。

本模块提供两个方向各一个入口
------------------------------
- **读侧**：:func:`build_vector_retriever` → ``interview_core.resolve_retriever``（``use_rag=True``）
- **写侧**：:func:`build_vector_store` → ``knowledge_import_pipeline.KnowledgeImportPipeline``

两者都从这里拿「用哪个 Embedding / 哪个向量后端」，所以换真实向量库或真实模型
**只需改本模块**。入库 Pipeline 因此不需要认识 ``SqlAlchemyVectorStore``。

检索参数的配置注入
------------------
读侧还有一个**配置注入**入口：:func:`resolve_retriever_defaults` 读
``RAG_TOP_K`` / ``RAG_MIN_SCORE``，:func:`build_vector_retriever` 把它当**默认值**，
调用方显式传入的参数**覆盖**它。三个变量都遵循同一条约定
（``VECTOR_STORE`` / ``EMBEDDING_*`` / ``RAG_*``）：**不配 = 默认 = 行为不变**。

.. warning::
   **``min_score`` 是绝对分数下限，与 Embedding 模型 + 语料绑定，不能照抄别处的数字。**
   它过滤掉「相似度不够」的候选，从而避免低分 chunk 进入 ``knowledge_context``、
   白占 Prompt 预算。但**同一个数字在不同语料上后果完全相反**——实测：``0.25``
   在 22 篇的面试知识库上恰好切掉噪声，在 16 篇的检索测试集上却会把大多数 query
   的结果**全部过滤掉**。**必须先在自己的库上标定**，
   方法见 ``backend/scripts/rag_query_set.json`` 的 ``min_score_calibration``。

本阶段刻意不做
--------------
- **自动被调用**：本模块的两个入口都**不会被自动触发**——读侧由
  ``interview_core.generate_next_question(use_rag=True)`` 显式触发，
  写侧由调用方构造 ``KnowledgeImportPipeline`` 后调用 ``import_document``；
  **不做全局单例**，避免「悄悄接上 RAG」。
- **把 ANN 接进面试流程**：``VECTOR_STORE=chroma`` 只换向量后端，
  **不改变**「谁在什么时候检索」——那由 ``use_rag`` 决定，与后端无关。
- **索引的删/重建**：``vector_store_chroma`` 只写不删，见该模块「刻意不做」。
- **给阈值设「代码默认值」**：``RAG_MIN_SCORE`` 不设时仍是 ``None``（不过滤）。
  本项目不内置任何「推荐的魔法数字」——阈值必须由部署方按自己的库标定后显式配置。
（真实厂商 Embedding 实现已在 ``services/embedding_provider.py``，由
:func:`default_embedder` 按 ``EMBEDDING_*`` 配置选择；真实 ANN 后端已在
``services/vector_store_chroma.py``，由 :func:`resolve_vector_backend` 按
``VECTOR_STORE`` 选择。）
"""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from typing import Any, Dict, Optional

# ============================================================
# 默认值
# ============================================================
#: 默认 ``top_k``（检索条数）——与 ``VectorStore`` 的默认一致，此处**不重复声明**，
#: 由 ``VectorKnowledgeRetriever`` 的默认参数决定（避免两处魔法数字漂移）。

# ============================================================
# 向量后端选择（全项目唯一开关）
# ============================================================
#: 选择向量后端的**唯一**环境变量。不设 → 默认后端（行为与接入 ANN 之前逐字节一致）
ENV_VECTOR_STORE = "VECTOR_STORE"

#: 默认后端：无 ANN 索引、全表扫描 + Python 侧余弦（``vector_store_sql``）
BACKEND_SQL = "sql"
#: 真实 ANN 后端：HNSW 索引（``vector_store_chroma``，需要额外安装 chromadb）
BACKEND_CHROMA = "chroma"

#: 取值别名（大小写不敏感；``sqlalchemy`` / ``mysql`` 都指同一个后端）
_BACKEND_ALIASES = {
    "sql": BACKEND_SQL,
    "sqlalchemy": BACKEND_SQL,
    "mysql": BACKEND_SQL,
    "chroma": BACKEND_CHROMA,
    "chromadb": BACKEND_CHROMA,
}


def resolve_vector_backend(env: Optional[Mapping[str, str]] = None) -> str:
    """读 ``VECTOR_STORE`` 决定用哪个向量后端，返回 ``"sql"`` 或 ``"chroma"``。

    - **不设 / 空** → :data:`BACKEND_SQL`（默认；保持「未配置就不改变行为」的项目约定）
    - ``sql`` / ``sqlalchemy`` / ``mysql`` → :data:`BACKEND_SQL`
    - ``chroma`` / ``chromadb`` → :data:`BACKEND_CHROMA`

    单独抽成函数（而不是在 :func:`build_vector_store` 里内联读环境变量），
    是为了让「默认值是什么」**可被单测直接断言**——环境变量是最容易悄悄改变行为的地方。

    :param env: 环境变量映射（默认 ``os.environ``；测试可注入，避免污染真实环境）。
    :raises RetrieverConfigError: 取值无法识别（**不静默退回默认**——那会让
        「以为开了 ANN、其实还在全表扫描」这种问题藏起来）。
    """
    from services.vector_knowledge_retriever import RetrieverConfigError

    values = os.environ if env is None else env
    raw = str(values.get(ENV_VECTOR_STORE) or "").strip().lower()
    if not raw:
        return BACKEND_SQL
    resolved = _BACKEND_ALIASES.get(raw)
    if resolved is None:
        raise RetrieverConfigError(
            f"未知的 {ENV_VECTOR_STORE}={raw!r}；可选：{BACKEND_SQL}"
            f"（默认，无 ANN、全表扫描）/ {BACKEND_CHROMA}（真实 ANN，需安装 chromadb）"
        )
    return resolved


def _normalize_backend(backend: str) -> str:
    """把显式传入的 ``backend`` 归一成已知取值（未知 → ``RetrieverConfigError``）。"""
    from services.vector_knowledge_retriever import RetrieverConfigError

    resolved = _BACKEND_ALIASES.get(str(backend).strip().lower())
    if resolved is None:
        raise RetrieverConfigError(
            f"未知的向量后端 {backend!r}；可选：{BACKEND_SQL} / {BACKEND_CHROMA}"
        )
    return resolved


# ============================================================
# 检索默认参数（配置注入；全项目唯一入口）
# ============================================================
#: 默认 ``top_k`` 的环境变量。不设 → 用检索器自身的默认（``DEFAULT_TOP_K``）。
ENV_RAG_TOP_K = "RAG_TOP_K"

#: 默认 ``min_score`` 的环境变量。不设 → ``None``（**不过滤**，与接入本配置之前逐字节一致）。
#:
#: ⚠️ ``min_score`` 是**绝对分数下限**，与 Embedding 模型、语料绑定
#: （余弦分数只在**同一模型 + 同一语料**内可比）。**必须按自己的库标定**，
#: 不能照抄别处的数字。标定方法见 ``backend/scripts/rag_query_set.json`` 的
#: ``min_score_calibration``：``min_score = floor(0.5 × 该 query 的 top-1 实测分数, 2 位小数)``。
ENV_RAG_MIN_SCORE = "RAG_MIN_SCORE"

#: 默认**相对阈值** ``α`` 的环境变量（``min_score_ratio``）。不设 → ``None``（关闭）。
#:
#: 为什么需要它：``min_score`` 是**绝对**尺度，而余弦分数的绝对尺度**随 query 形态漂移**
#: ——实测同一个模型 + 同一份语料，长问句 top1 ≈ 0.84、岗位技能短词 top1 ≈ 0.77，
#: 于是「为长问句标定的 0.83」会把短词 query 整片滤掉。``α`` 是尺度无关的。
#:
#: 与 ``min_score`` 是 **AND**：``score >= max(min_score, α × top1)``。
#: 标定方法见 ``backend/tests/rag_production_query_calibration_spark_run.py``。
ENV_RAG_MIN_SCORE_RATIO = "RAG_MIN_SCORE_RATIO"

#: 余弦相似度的取值域（``min_score`` 的合法范围）。
_COSINE_MIN = -1.0
_COSINE_MAX = 1.0


def resolve_retriever_defaults(env: Optional[Mapping[str, str]] = None) -> Dict[str, Any]:
    """读 ``RAG_TOP_K`` / ``RAG_MIN_SCORE``，返回**要预置到组装参数里的检索默认值**。

    - **两个都不设 / 都是空串** → ``{}``
      ⇒ :func:`build_vector_retriever` 完全按调用方给的参数构造
      ⇒ 行为与引入本配置之前**逐字节一致**（「未配置就不改变行为」的项目约定）。
    - 设了 → ``{"top_k": int}`` / ``{"min_score": float}``（各自独立，可只设一个）。
    - 调用方**显式**传入的同名参数**优先**（见 :func:`build_vector_retriever` 的合并规则）。

    单独抽成函数（而不是在 :func:`build_vector_retriever` 里内联读环境变量），
    与 :func:`resolve_vector_backend` 同一取舍：让「默认值是什么」**可被单测直接断言**
    ——环境变量是最容易悄悄改变行为的地方。

    :param env: 环境变量映射（默认 ``os.environ``；测试可注入，避免污染真实环境）。
    :raises RetrieverConfigError: 取值非法。**刻意不静默退回默认**——
        配错了却「看起来正常地不过滤」，比直接报错更难排查。
        注意：面试流程里 ``interview_core.resolve_retriever`` 会把这个异常
        统一降级为「无知识 + warning」，因此**配错的表现是「检索不到」**，
        这与既有的 RAG 失败语义一致（有 warning 可查）。
    """
    from services.vector_knowledge_retriever import RetrieverConfigError

    values = os.environ if env is None else env
    defaults: Dict[str, Any] = {}

    raw_top_k = str(values.get(ENV_RAG_TOP_K) or "").strip()
    if raw_top_k:
        # 刻意不用 ``int()`` 直接吞：``int()`` 会把 ``1_0``（下划线分组）也收下。
        # 这里只认「十进制数字串」（允许一个前导 ``+``）。
        #
        # 用 ``isdecimal()`` 而**不是** ``isdigit()``：后者对 ``²`` 这类上标也返回
        # ``True``，而 ``int("²")`` 会抛 ``ValueError`` —— 那就从「配置非法」变成了
        # 「未捕获异常」，而本函数承诺的是前者。``isdecimal()`` 与 ``int()`` 的可接受
        # 集合严格对齐（含各国十进制数字，如 ``８``）。
        digits = raw_top_k[1:] if raw_top_k.startswith("+") else raw_top_k
        if not digits.isdecimal():
            raise RetrieverConfigError(
                f"{ENV_RAG_TOP_K} 必须是十进制正整数，收到 {raw_top_k!r}"
            )
        top_k = int(digits)
        if top_k < 1:
            raise RetrieverConfigError(f"{ENV_RAG_TOP_K} 必须 >= 1，收到 {top_k}")
        defaults["top_k"] = top_k

    raw_min_score = str(values.get(ENV_RAG_MIN_SCORE) or "").strip()
    if raw_min_score:
        if "_" in raw_min_score:
            raise RetrieverConfigError(
                f"{ENV_RAG_MIN_SCORE} 不接受下划线写法，收到 {raw_min_score!r}"
            )
        try:
            min_score = float(raw_min_score)
        except ValueError as exc:
            raise RetrieverConfigError(
                f"{ENV_RAG_MIN_SCORE} 必须是数值（余弦相似度下限），收到 {raw_min_score!r}"
            ) from exc
        if not math.isfinite(min_score):
            raise RetrieverConfigError(
                f"{ENV_RAG_MIN_SCORE} 必须是有限数值，收到 {raw_min_score!r}"
            )
        if not (_COSINE_MIN <= min_score <= _COSINE_MAX):
            # 最常见的真实错误：想写 0.25 却写成 25 ⇒ 全部结果被过滤掉，
            # 表现为「RAG 忽然一条都检索不到」。这里直接拦下。
            raise RetrieverConfigError(
                f"{ENV_RAG_MIN_SCORE}={min_score} 超出余弦相似度取值域 "
                f"[{_COSINE_MIN}, {_COSINE_MAX}]；若想「不过滤」请**不要设置**本变量"
            )
        defaults["min_score"] = min_score

    raw_ratio = str(values.get(ENV_RAG_MIN_SCORE_RATIO) or "").strip()
    if raw_ratio:
        if "_" in raw_ratio:
            raise RetrieverConfigError(
                f"{ENV_RAG_MIN_SCORE_RATIO} 不接受下划线写法，收到 {raw_ratio!r}"
            )
        try:
            ratio = float(raw_ratio)
        except ValueError as exc:
            raise RetrieverConfigError(
                f"{ENV_RAG_MIN_SCORE_RATIO} 必须是数值（相对阈值 α），收到 {raw_ratio!r}"
            ) from exc
        if not math.isfinite(ratio):
            raise RetrieverConfigError(
                f"{ENV_RAG_MIN_SCORE_RATIO} 必须是有限数值，收到 {raw_ratio!r}"
            )
        if not (0.0 < ratio <= 1.0):
            # 最常见错误：写成百分数（如 99.5）⇒ α > 1 ⇒ 连 top1 自己都被滤掉，
            # 表现为「RAG 忽然一条都检索不到」。这里直接拦下。
            raise RetrieverConfigError(
                f"{ENV_RAG_MIN_SCORE_RATIO}={ratio} 超出比例取值域 (0, 1]；"
                "请写小数（如 0.9947），不要写百分数"
            )
        defaults["min_score_ratio"] = ratio

    return defaults


# ============================================================
# 组装
# ============================================================
def default_embedder(*, role: Optional[str] = None) -> Any:
    """默认 Embedding 服务：**按环境变量选实现**，未配置时是离线占位（不是语义向量）。

    单独抽成函数，是为了让「用哪个模型」只有**一个**说法——读侧检索与写侧入库
    都从这里拿，因此绝不会出现「入库用 A、检索用 B」而检索不到的情况。

    - 一个 ``EMBEDDING_*`` 都不配 → ``HashEmbeddingService``（**离线、零依赖、默认行为不变**）
    - 配了 ``EMBEDDING_API_KEY``（或显式 ``EMBEDDING_PROVIDER=openai``）→
      ``embedding_provider.EmbeddingProvider``（真实模型）
    - 显式 ``EMBEDDING_PROVIDER=spark`` → 讯飞星火实现
      （``embedding_provider_spark.SparkEmbeddingProvider``）

    :param role: 与厂商无关的角色（``"document"`` / ``"query"``，缺省 ``document``）。
        只有**非对称**实现会用到它——讯飞把「知识原文」与「用户问题」分两个 ``domain``，
        配错不报错、只是掉召回。写侧（入库）用默认值 ``document``；
        **读侧（检索）由 :func:`build_vector_retriever` 传 ``query``**。
        对称实现（hash / mock / openai）忽略它，因此传了不会改变既有行为。

    :raises EmbeddingProviderConfigError: 配置非法（如 ``openai`` 却没给密钥）。
        读侧由 ``interview_core.resolve_retriever`` 捕获并**静默降级为「无知识」**；
        写侧在构造 ``KnowledgeImportPipeline`` 时即暴露（配置错误不该伪装成「检索不到」）。
    """
    from services.embedding_provider import build_embedding_service

    return build_embedding_service(role=role)


def describe_default_embedding() -> Any:
    """**当前默认 Embedding 的运行状态标识**（``EmbeddingInfo``，供日志 / 调试）。

    一句话回答「本进程默认会用什么 Embedding、它到底是不是语义模型」：

    - 一个 ``EMBEDDING_*`` 都不配 → ``provider="hash-local"``、``semantic_enabled=False``
      （**离线占位，不可用于评估检索质量**）
    - 配了 ``EMBEDDING_API_KEY``（或显式 ``EMBEDDING_PROVIDER=openai``）→
      ``provider=<模型名>``、``semantic_enabled=True``

    与 :func:`default_embedder` **同源**（就是描述它的返回值），因此「日志里报的模式」
    与「真正在用的实现」不可能不一致；且**不读库、不发网络请求、不编码任何文本**
    （构造 Provider 只解析配置），可安全地放进启动日志。

    :raises EmbeddingProviderConfigError: 配置非法（如 ``openai`` 却没给密钥）。
        **刻意不在这里吞掉**——「部署配错了」不该在日志里伪装成「离线占位」，
        那正是这个标识要防的事。
    """
    # 延迟导入（模块顶层保持只有标准库）：与 default_embedder 同一取舍。
    # 注意**不要**改成从 ``services.embedding_service`` 取 ``describe_embedding``——
    # 那会让组装器重新直接依赖接口模块，破坏「组装器不直接依赖具体实现」的守卫。
    from services.embedding_provider import describe_embedding

    return describe_embedding(default_embedder())


def build_vector_store(
    db: Any, *, model: Optional[str] = None, backend: Optional[str] = None
) -> Any:
    """组装向量存储后端（**写侧**入口：入库 Pipeline 用）。

    与 :func:`build_vector_retriever` 是**同一件事的两半**：

    - 写侧（本函数）：文档入库时把「切片 + 向量」写进去
    - 读侧（:func:`build_vector_retriever`）：面试出题时按向量检索

    两者共用本函数，于是「用哪个向量后端」仍然**只有本模块知道**——
    入库 Pipeline 不需要（也不应该）认识 ``SqlAlchemyVectorStore`` / ``ChromaVectorStore``。

    :param db: 数据库会话（由调用方注入）。**必填**，两个后端都要它：
        切片行的权威副本始终在 ``knowledge_chunk``（chroma 只是**派生**的 ANN 索引）。
    :param model: 默认模型标识（记录自带 ``model`` 时以记录为准）。
    :param backend: 显式指定后端（``"sql"`` / ``"chroma"``，见 :func:`resolve_vector_backend`）。
        ``None`` → 读环境变量 ``VECTOR_STORE``，不设则 :data:`BACKEND_SQL`。
    :raises RetrieverConfigError: ``db`` 为空 / 后端取值无法识别。

    .. note::
       ``chroma`` 的实现是**函数内延迟导入**的：它顶层会 import ``chromadb``，
       而那是可选依赖（不装也能跑默认后端）。
    """
    from services.vector_knowledge_retriever import RetrieverConfigError
    from services.vector_store_sql import SqlAlchemyVectorStore

    if db is None:
        raise RetrieverConfigError(
            "build_vector_store 需要 db（向量后端要读写知识切片表）"
        )

    selected = _normalize_backend(backend) if backend is not None else resolve_vector_backend()
    if selected == BACKEND_CHROMA:
        from services.vector_store_chroma import ChromaVectorStore

        return ChromaVectorStore(db, model=model)
    return SqlAlchemyVectorStore(db, model=model)


def build_vector_retriever(
    db: Any,
    *,
    embedder: Any = None,
    model: Optional[str] = None,
    **retriever_kwargs: Any,
) -> Any:
    """组装真实 RAG 检索器：``EmbeddingService`` + ``VectorStore`` → ``KnowledgeRetriever``。

    :param db: 数据库会话（由调用方注入，**本模块不建连接、不读配置**）。
    :param embedder: Embedding 服务。``None`` 时用 :func:`default_embedder`
        （按 ``EMBEDDING_*`` 配置选实现；未配置则是**离线占位**，生产请配好密钥
        或显式注入真实模型）。
    :param model: 传给向量库的模型标识。``None`` 时由检索器按 ``embedder.name`` 决定
        （推荐保持 ``None``：不同模型的向量不可比，检索器必须只跟同模型算分）。
    :param retriever_kwargs: 原样透传给
        :class:`~services.vector_knowledge_retriever.VectorKnowledgeRetriever`
        （``top_k`` / ``min_score`` / ``category`` / ``document_id`` / ``dedup``）。
        **不做白名单**——非法参数会在检索器构造函数里以
        ``RetrieverConfigError`` 报错，错误信息更精确。

    **检索默认参数（配置注入）**
    ----------------------------
    本函数会先读 :func:`resolve_retriever_defaults`（即 ``RAG_TOP_K`` /
    ``RAG_MIN_SCORE`` 两个环境变量），把它当作**默认值**，再让**调用方显式传入的参数覆盖它**：

    .. code-block:: text

        最终参数 = { **环境变量默认值, **调用方显式传入 }

    于是三条性质同时成立：

    1. **不配环境变量 ⇒ 行为逐字节不变**（默认值集合为空）；
    2. **配了 ⇒ 生产链路自动带上阈值**（``interview_core.resolve_retriever``
       在 ``use_rag=True`` 时调用的就是本函数，不传任何 ``retriever_kwargs``）；
    3. **显式传参始终优先** ⇒ 单测、基准、A/B 对比仍能逐场景钉死参数
       （任务 70 · R1 的 ``retriever_kwargs`` 透传链不受影响）。

    返回一个可直接交给 ``interview_core`` 的检索器（鸭子类型：
    有 ``async retrieve(job_info, topic, context)``）。

    异常：``db`` 为空 / 构造参数非法 / 环境变量非法 →
    :class:`~services.vector_knowledge_retriever.RetrieverConfigError`
    （``ValueError``）；由调用方决定是否降级（面试流程里是**静默降级为「无知识」**）。
    """
    from services.vector_knowledge_retriever import VectorKnowledgeRetriever

    if embedder is None:
        # **读侧 = 问题**：非对称实现（讯飞）要按 ``query`` 编码才能与入库时的
        # ``para``（原文）配对；配对错了不报错、只是掉召回。对称实现忽略该参数。
        from services.embedding_provider import ROLE_QUERY

        embedder = default_embedder(role=ROLE_QUERY)

    # 配置注入：环境变量只提供**默认值**，调用方显式传入的一律覆盖它。
    merged: Dict[str, Any] = {**resolve_retriever_defaults(), **retriever_kwargs}

    # 复用写侧的组装函数：「用哪个向量后端」只在 build_vector_store 里决定
    store = build_vector_store(db, model=model)
    return VectorKnowledgeRetriever(embedder, store, **merged)


__all__ = [
    "BACKEND_CHROMA",
    "BACKEND_SQL",
    "ENV_RAG_MIN_SCORE",
    "ENV_RAG_MIN_SCORE_RATIO",
    "ENV_RAG_TOP_K",
    "ENV_VECTOR_STORE",
    "build_vector_retriever",
    "build_vector_store",
    "default_embedder",
    "describe_default_embedding",
    "resolve_retriever_defaults",
    "resolve_vector_backend",
]
