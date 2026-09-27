# -*- coding: utf-8 -*-
"""AI 模拟面试 · **真实知识检索器**（向量检索链路）。

分层定位
--------
::

    KnowledgeRetriever（接口 + 值对象 + Mock，services/knowledge_retriever.py，零依赖）
        └── VectorKnowledgeRetriever          【本模块】真实实现
              ├── EmbeddingService            query 文本 → query 向量
              └── VectorStore                 query 向量 → 相似切片（接口，不绑后端）

**为什么单独一个模块，而不是写进 ``knowledge_retriever.py``**
``knowledge_retriever.py`` 的定位是「接口 + 值对象 + Mock」，它必须**零依赖**
（只 import 标准库），由 ``tests/test_knowledge_retriever.py`` 的 AST 与子进程守卫锁死。
真实实现必须 import ``embedding_service`` 与 ``vector_store``，写进去会破坏该性质。
``models/knowledge.py`` 的文档也早已写明：「ORM 行 → 值对象」的转换由
**真实 Retriever 在自己的模块里**完成。于是这里新建一个模块，
``knowledge_retriever.py`` **一行都不用改**（Mock 也原样保留给测试用）。

检索流程
--------
::

    job_info / topic / context
        │  build_query（纯函数：决定「查什么」）
        ▼
      query 文本 ──为空──▶ 返回 []（没有可检索的线索 ≠ 故障）
        │  embedder.embed(query)
        ▼
      query 向量
        │  store.search(vector, top_k, model=embedder.name, category, min_score)
        ▼
      List[VectorMatch]
        │  过滤空正文 → 去重 → 计分写进 metadata
        ▼
      List[KnowledgeChunk]（content / source / metadata{score, chunk_id, …}）

返回形状
--------
接口**没变**——仍然是 ``retrieve(job_info, topic, context) -> List[KnowledgeChunk]``，
``KnowledgeChunk`` 仍然是 ``{content, source, metadata}`` 三键（该契约由
``test_knowledge_retriever.py`` 锁死，且 Agent 只读 ``content`` / ``source``）。
相似度分数落在 ``metadata["score"]``（``metadata`` 正是为「上游结构的额外字段」
准备的扩展点），因此**不会**渗进 Agent 的 Prompt。
若需要一个摊平的检索视图 ``{content, source, score}``，用 :func:`chunk_to_result`。

异常策略（**「检索不到」不是异常**）
------------------------------------
沿用接口模块的约定：**``[]`` 表示「没知识」**，异常只用于「检索流程本身无法进行」。
本模块把上游两类异常收敛成两个**可按类型分流**的异常：

=========================================  ====================================================
情况                                        结果
=========================================  ====================================================
没有查询文本（topic / stage / 岗位名都空）    ``[]``
检索到 0 条 / 全部低于 ``min_score``          ``[]``
embedder 报「入参错 / 维度错」                :class:`RetrieverConfigError`（+``ValueError``）
向量库报「维度不符 / 入参错」                 :class:`RetrieverConfigError`（多半是**换模型没重算**）
embedder / 向量库「不可用」或其他故障         :class:`RetrieverUnavailableError`（+``RuntimeError``）
=========================================  ====================================================

两类的处理方式完全不同（配置错要改配置/重算向量、不可用可重试或降级），
所以必须**按类型**分开，而不是靠消息文本判断。原始异常一律挂在 ``__cause__`` 上。

调用方（``interview_core._gather_knowledge``）会把任何异常**静默降级为「无知识」**
并记一条 warning，所以这里的异常是给**日志与将来的可观测性**用的，
不会让出题中断——这正是「检索是可选增强」的落点。

.. warning::
   **库里的向量必须用同一个模型标识写入**。``search`` 默认按
   ``embedder.name`` 过滤（不同模型的向量**不可比**），因此若写入时用了别的
   ``name``（或干脆没打标），结果会是 **「检索不到」而不是报错**——这是刻意的：
   宁可查不到，也不能拿另一个模型的向量算相似度。排查「明明有数据却搜不到」时，
   先看 ``knowledge_chunk.embedding_model`` 是否等于 ``embedder.name``。
   需要关掉该过滤时显式传 ``model=""``。

本阶段刻意不做
--------------
真实向量库（ANN 索引）、把本检索器**接进** ``start_session`` 业务路径
（那会把出题从确定性规则变成依赖大模型 + 向量库，属业务效果变更，需显式授权）。
（真实 Embedding 实现已在 ``services/embedding_provider.py``，由组装器
``knowledge_rag.default_embedder()`` 按 ``EMBEDDING_*`` 配置选择。）
本模块**没有任何生产调用方**，由调用方显式注入——生产链路上是
``interview_core.retrieve_knowledge`` / ``generate_next_question`` 的 ``retriever`` 参数。
**不提供全局单例**（避免「悄悄接上 RAG」）。
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Tuple

from services.embedding_service import (
    EmbeddingDimensionError,
    EmbeddingError,
    EmbeddingInputError,
    EmbeddingUnavailableError,
)
from services.knowledge_retriever import (
    KnowledgeChunk,
    KnowledgeRetriever,
    KnowledgeRetrieverError,
)
from services.vector_store import (
    DEFAULT_TOP_K,
    VectorMatch,
    VectorStoreDimensionError,
    VectorStoreError,
    VectorStoreInputError,
    require_min_score,
    require_top_k,
    require_vector,
)

# ============================================================
# 契约常量
# ============================================================
#: 相似度落在 ``KnowledgeChunk.metadata`` 的这个键上。
#: 不放进 ``KnowledgeChunk`` 的顶层字段，是因为那个三键契约
#: （``content`` / ``source`` / ``metadata``）已被守卫测试锁死，
#: 且 Agent 只读 ``content`` / ``source``——分数留在 ``metadata`` 里既够用又不外泄。
SCORE_METADATA_KEY = "score"

#: :func:`chunk_to_result` 的键与顺序（「摊平的检索视图」）。
RETRIEVAL_RESULT_FIELDS: Tuple[str, ...] = ("content", "source", "score")

#: 查询文本的候选来源键，按优先级排列（``build_query`` 用）。
_TOPIC_KEYS: Tuple[str, ...] = ("topic", "name", "title")
_JOB_KEYS: Tuple[str, ...] = ("job_name", "title", "name", "position")

#: 异常信息里截断查询文本，避免把长正文写进日志。
_QUERY_PREVIEW_LEN = 40


# ============================================================
# 异常（项目规范：领域基类 + 最贴近的内建异常）
# ============================================================
class VectorRetrieverError(KnowledgeRetrieverError):
    """本模块的领域基类。

    继承接口模块的 :class:`KnowledgeRetrieverError`，于是调用方既能一把捕获
    「所有知识检索错误」，也能按下面的子类分流。
    """


class RetrieverConfigError(VectorRetrieverError, ValueError):
    """**配置 / 环境**问题：模型不匹配、维度不符、embedder 报了入参错。

    归为 ``ValueError``：改配置或重算向量就能解决，**重试没用**。
    """


class RetrieverUnavailableError(VectorRetrieverError, RuntimeError):
    """**服务不可用**：embedder 或向量库故障。

    归为 ``RuntimeError``：**可以重试或降级**（降级路径即「无知识」）。
    """


# ============================================================
# 一、查询构造（纯函数）
# ============================================================
def _read_field(source: Any, key: str) -> Any:
    """读字段：``Mapping`` 取键，其他对象取属性。读不到返回 ``None``。

    本模块**不假设入参形状**——``job_info`` 在生产链路上是 ``models.Job`` ORM 行，
    但测试与将来的 HTTP 层可能传 dict。故统一走这里。
    """
    if source is None:
        return None
    if isinstance(source, Mapping):
        return source.get(key)
    return getattr(source, key, None)


def _as_text(value: Any) -> str:
    """把任意值归一成**去首尾空白**的文本。

    - ``None`` / ``bool`` → ``""``（``True`` 当查询文本毫无意义）
    - ``str`` → 去首尾空白
    - ``int`` / ``float`` → ``str(...)``
    - 列表 / 元组 → 逐项归一后用空格连接（``skills`` 常是这种形状）

    刻意**不** ``str(dict)`` / ``str(obj)``——那会把 ``{'a': 1}`` 这种 repr
    当成查询文本，比空串更糟。
    """
    if value is None or isinstance(value, bool):
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, tuple)):
        parts = [_as_text(item) for item in value]
        return " ".join(part for part in parts if part)
    return ""


def _require_min_score_ratio(value: Any) -> Optional[float]:
    """校验相对阈值 ``α`` ⇒ ``None`` 或 ``(0, 1]`` 内的有限浮点。

    为什么**不复用** ``vector_store.require_min_score``：那个校验的是「余弦取值域
    ``[-1, 1]``」，会把 ``α = -0.5`` 当合法值放行；而 α 是**比例**，
    合法域是 ``(0, 1]``（``α > 1`` 会连 top1 自己都切掉，等价于「恒返回空」，
    是配置错误而不是「过滤严格」）。

    ``bool`` 是 ``int`` 子类 ⇒ 显式拒绝（``True`` 会被当成 ``1.0`` 静默放行）。
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RetrieverConfigError(
            f"min_score_ratio 必须是数值（比例），收到 {type(value).__name__}: {value!r}"
        )
    ratio = float(value)
    if not math.isfinite(ratio):
        raise RetrieverConfigError(f"min_score_ratio 必须是有限数值，收到 {value!r}")
    if not (0.0 < ratio <= 1.0):
        raise RetrieverConfigError(
            f"min_score_ratio 必须在 (0, 1] 内，收到 {value!r}"
            "（> 1 会把最佳命中自己都滤掉 ⇒ 恒返回空；0 或负数无意义）"
        )
    return ratio


def build_query(job_info: Any = None, topic: Any = None, context: Any = None) -> str:
    """构造查询文本（纯函数、确定性、不改入参）。

    优先级：

    1. ``topic``——「当前待考察的知识点」，最精确的检索线索
    2. ``context.current_stage``——topic 为空时的兜底
       （``interview_core.current_topic`` 也是这么兜底的）
    3. ``job_info`` 的 ``job_name`` / ``title`` / ``name`` / ``position``
    4. ``""``——连线索都没有

    **为什么只取一个来源，而不是把 topic + 岗位 + 上下文拼成一段长文本**：
    拼接会**稀释**语义向量（长 query 的向量是所有 token 的平均方向），
    检索精度反而下降。真正的加权（岗位相关性、阶段相关性）应当作用于
    **score**，而不是污染 query——那是排序层的职责，不是查询构造层的。
    """
    for candidate in (topic, _read_field(context, "current_stage")):
        text = _as_text(candidate)
        if text:
            return text

    for key in _JOB_KEYS:
        text = _as_text(_read_field(job_info, key))
        if text:
            return text

    return ""


def chunk_to_result(chunk: KnowledgeChunk) -> Dict[str, Any]:
    """把检索结果摊平成 ``{content, source, score}``（前端 / 日志用的检索视图）。

    ``score`` 取自 ``metadata``；缺省 ``0.0``（例如手工构造的片段没有分数）。
    这是**只读投影**，不改动原对象。
    """
    score = chunk.metadata.get(SCORE_METADATA_KEY, 0.0)
    try:
        score = float(score)
    except (TypeError, ValueError):
        score = 0.0
    return {"content": chunk.content, "source": chunk.source, "score": score}


# ============================================================
# 二、真实检索器
# ============================================================
class VectorKnowledgeRetriever(KnowledgeRetriever):
    """向量检索器：``query → EmbeddingService → VectorStore.search → KnowledgeChunk``。

    两个协作者都由**构造函数注入**，本模块**只依赖接口**、不依赖任何具体后端：

    :param embedder: 任何有 ``async embed(text) -> List[float]`` 的对象
        （通常是 :class:`~services.embedding_service.EmbeddingService` 的子类）。
        它的 ``name`` 会被用作向量库的 ``model`` 过滤条件。
    :param store: 任何有 ``async search(...)`` 的
        :class:`~services.vector_store.VectorStore`。生产上传
        ``SqlAlchemyVectorStore(db, model=embedder.name)``；
        单测传 ``InMemoryVectorStore()``。**本模块不 import 任何具体后端**
        （尤其是 ``vector_store_sql``，那会把 SQLAlchemy 拉进来）。
    :param top_k: 最多返回几条（必须正整数）。
    :param min_score: 相似度下限，低于它的结果丢弃（``None`` = 不过滤）。
        **语义是「绝对下限」**——回答的是「语料里到底有没有这个东西」。
    :param min_score_ratio: **相对阈值** ``α``（``None`` = 关闭，默认）。开启后额外要求
        ``score >= α × top1``（``top1`` = 本次检索的最高分），即**只保留与最佳命中
        同一档的结果**。回答的是「同一次检索里，谁比最好的那个差太多」。

        为什么需要它：余弦分数的**绝对尺度随 query 形态漂移**（实测同一个模型、
        同一份语料，长问句 top1 ≈ 0.84、岗位技能短词 top1 ≈ 0.77），
        单一绝对阈值不可能同时适配 ⇒ 短词 query 会被整片滤掉。
        ``α`` 是**尺度无关**的，因此两种形态可以共用一把尺子。

        与 ``min_score`` 是 **AND** 关系：``score >= max(min_score, α × top1)``。
        典型配法：``min_score`` 给一个**低**的绝对下限（挡住「语料完全没有这个主题」
        时凭相对值硬凑出来的噪声），``min_score_ratio`` 给 ``0.99`` 左右。
    :param category: 只检索该知识分类（取自切片 ``metadata["category"]``）。
        默认 ``None`` = 不分类过滤——知识点可能与「岗位 / 公司 / 项目」类知识都相关，
        由调用方按需收紧。
    :param document_id: 只在指定知识文档内检索（``None`` = 全库）。
    :param model: 覆盖向量库的 ``model`` 过滤条件。``None``（默认）用 ``embedder.name``；
        显式传 ``""`` 表示**关掉**模型过滤（只在你确定库里全是同一模型的向量时用）。
    :param dedup: 是否按正文去重（默认 ``True``）。重复正文会白白占用 Prompt 预算。

    **配置错误在构造期就报**（缺 ``embed`` / 缺 ``search`` / ``top_k`` 非法 …），
    而不是等第一次检索——那样会把「部署配错了」伪装成「检索不到知识」。
    """

    #: 检索器标识（基类是 ``"none"``、Mock 是 ``"mock"``）。
    source_name = "vector"

    def __init__(
        self,
        embedder: Any,
        store: Any,
        *,
        top_k: int = DEFAULT_TOP_K,
        min_score: Optional[float] = None,
        category: Optional[str] = None,
        document_id: Optional[int] = None,
        model: Optional[str] = None,
        dedup: bool = True,
        min_score_ratio: Optional[float] = None,
    ) -> None:
        if not callable(getattr(embedder, "embed", None)):
            raise RetrieverConfigError(
                "embedder 必须提供 async embed(text) -> List[float]，"
                f"收到 {type(embedder).__name__}"
            )
        if not callable(getattr(store, "search", None)):
            raise RetrieverConfigError(
                "store 必须提供 async search(query_vector, ...) -> List[VectorMatch]，"
                f"收到 {type(store).__name__}"
            )
        if not isinstance(dedup, bool):
            raise RetrieverConfigError(f"dedup 必须是 bool，收到 {dedup!r}")
        try:
            self.top_k = require_top_k(top_k)
            self.min_score = require_min_score(min_score)
        except VectorStoreInputError as exc:
            raise RetrieverConfigError(f"检索配置非法：{exc}") from exc
        #: 相对阈值 α（``None`` = 关闭）。校验刻意放在这里而不是复用
        #: ``vector_store.require_min_score``：那个的口径是「余弦取值域 [-1, 1]」，
        #: 而 α 的口径是「比例 (0, 1]」——两套约束混用会把非法值放行。
        self.min_score_ratio = _require_min_score_ratio(min_score_ratio)

        self.embedder = embedder
        self.store = store
        self.category = category
        self.document_id = document_id
        self.model = model
        self.dedup = dedup
        #: 每次调用**实际构造出的** query 文本（仅供观测，不影响输出）。
        self.queries: List[str] = []

    # --------------------------------------------------------
    # 只读属性
    # --------------------------------------------------------
    @property
    def model_name(self) -> Optional[str]:
        """实际传给向量库的 ``model`` 过滤条件（``None`` = 不过滤）。"""
        if self.model is not None:
            return _as_text(self.model) or None
        return _as_text(getattr(self.embedder, "name", "")) or None

    # --------------------------------------------------------
    # 接口实现（签名与基类完全一致）
    # --------------------------------------------------------
    async def retrieve(
        self,
        job_info: Any,
        topic: Any,
        context: Any,
    ) -> List[KnowledgeChunk]:
        """按「岗位 + 知识点 + 上下文」检索知识片段。

        与基类签名**逐字一致**，因此可以无感替换 ``MockKnowledgeRetriever`` /
        空实现。任何入参形状都接受（dict / ORM 行 / 任意对象）。

        返回 ``List[KnowledgeChunk]``；**无知识时返回 ``[]``（绝不返回 ``None``）**。
        失败时抛 :class:`RetrieverConfigError` / :class:`RetrieverUnavailableError`
        ——「检索不到」与「检索失败」是两件事，不能混（见模块文档的异常表）。
        """
        query = build_query(job_info, topic, context)
        self.queries.append(query)
        if not query:
            # 没有可检索的线索 —— 这不是故障，是「没东西可查」。
            return []

        vector = await self._embed(query)
        matches = await self._search(vector, query)
        matches = self._apply_ratio_filter(matches)
        return self._to_chunks(matches)

    def _apply_ratio_filter(self, matches: List[Any]) -> List[Any]:
        """相对阈值过滤：``score >= α × top1``（``α=None`` 或空结果 ⇒ 原样返回）。

        **为什么在 ``search`` 之后做、而不是把阈值塞进 ``search``**：
        ``α × top1`` 需要先知道 top1，而 top1 只有在拿到结果之后才存在。
        正确性依据是 ``select_matches`` 的顺序「排序 → ``min_score`` → ``top_k``」——
        返回的 ``matches`` 已是**降序前缀**，因此「在它上面按 α 过滤」与
        「先按 α 过滤再截 ``top_k``」**结果一致**（被保留的一定还是那个前缀）。
        """
        if self.min_score_ratio is None or not matches:
            return matches
        threshold = self.min_score_ratio * float(matches[0].score)
        return [match for match in matches if float(match.score) >= threshold]

    # --------------------------------------------------------
    # 内部：两段外部调用，各自收敛异常
    # --------------------------------------------------------
    async def _embed(self, query: str) -> List[float]:
        """调 embedder 把 query 变成向量，并把异常收敛成两类。"""
        try:
            vector = await self.embedder.embed(query)
        except EmbeddingInputError as exc:
            # query 由 build_query 保证非空，还报入参错 → 上游实现/配置有问题
            raise RetrieverConfigError(
                f"EmbeddingService 拒绝查询文本（{_preview(query)}）：{exc}"
            ) from exc
        except EmbeddingDimensionError as exc:
            raise RetrieverConfigError(
                f"EmbeddingService 维度配置错误（{_preview(query)}）：{exc}"
            ) from exc
        except EmbeddingUnavailableError as exc:
            raise RetrieverUnavailableError(
                f"EmbeddingService 不可用，无法向量化查询（{_preview(query)}）：{exc}"
            ) from exc
        except EmbeddingError as exc:                      # 兜底：其他 Embedding 错误
            raise RetrieverUnavailableError(
                f"向量化查询失败（{_preview(query)}）：{exc}"
            ) from exc
        except Exception as exc:  # noqa: BLE001 - 外部实现什么都可能抛
            raise RetrieverUnavailableError(
                f"向量化查询时发生未预期错误（{_preview(query)}）："
                f"{type(exc).__name__}: {exc}"
            ) from exc

        # 兜底校验：embedder 若是鸭子类型（未继承 EmbeddingService），
        # 基类的统一校验不会生效。这里挡一次，把「脏向量」变成清晰错误，
        # 而不是让它一路漂到余弦相似度那里再炸。
        try:
            return require_vector(vector, what="EmbeddingService 返回的向量")
        except VectorStoreInputError as exc:
            raise RetrieverConfigError(
                f"EmbeddingService 返回了非法向量（{_preview(query)}）：{exc}"
            ) from exc

    async def _search(self, vector: List[float], query: str) -> List[VectorMatch]:
        """调向量库取相似切片，并把异常收敛成两类。"""
        try:
            return await self.store.search(
                vector,
                top_k=self.top_k,
                model=self.model_name,
                document_id=self.document_id,
                category=self.category,
                min_score=self.min_score,
            )
        except VectorStoreDimensionError as exc:
            # 最常见的真实原因：**换了 Embedding 模型但没重算库里的向量**
            raise RetrieverConfigError(
                f"向量维度不符，多半是换了 Embedding 模型却没重算库中向量"
                f"（当前模型 {self.model_name!r}）：{exc}"
            ) from exc
        except VectorStoreInputError as exc:
            raise RetrieverConfigError(f"向量库拒绝了本次检索：{exc}") from exc
        except VectorStoreError as exc:                    # 兜底：其他向量库错误
            raise RetrieverUnavailableError(
                f"向量库检索失败（{_preview(query)}）：{exc}"
            ) from exc
        except Exception as exc:  # noqa: BLE001 - 外部后端什么都可能抛
            raise RetrieverUnavailableError(
                f"向量库检索时发生未预期错误（{_preview(query)}）："
                f"{type(exc).__name__}: {exc}"
            ) from exc

    # --------------------------------------------------------
    # 内部：结果归一
    # --------------------------------------------------------
    def _to_chunks(self, matches: List[VectorMatch]) -> List[KnowledgeChunk]:
        """``VectorMatch`` → ``KnowledgeChunk``（过滤空正文 + 去重，保序）。"""
        chunks: List[KnowledgeChunk] = []
        seen: set = set()
        for match in matches:
            content = match.content if isinstance(match.content, str) else ""
            if not content.strip():
                # 空正文在 Prompt 里毫无价值，直接丢（别让它占掉一条 top_k 名额的语义）
                continue
            if self.dedup:
                key = "".join(content.split())
                if key in seen:
                    continue
                seen.add(key)
            chunks.append(self._to_chunk(match, content))
        return chunks

    def _to_chunk(self, match: VectorMatch, content: str) -> KnowledgeChunk:
        """单条转换。

        - ``metadata["source"]``（切片阶段由 ``document_chunker`` 从文档搬过来的）
          **提升**为 :class:`KnowledgeChunk` 的 ``source`` 字段——那是它的正式位置，
          所以从 metadata 里移除，避免同一信息出现两份。
        - ``chunk_id`` / ``document_id`` 写进 metadata：面试场景下必须能溯源到具体行。
        - 相似度写进 ``metadata["score"]``（见 :data:`SCORE_METADATA_KEY`）。
        - ``content`` **原样保留**（不去首尾空白）：检索结果要能取证，
          不能悄悄改写原文。
        """
        metadata: Dict[str, Any] = dict(match.metadata or {})
        source = _as_text(metadata.pop("source", ""))

        if match.chunk_id is not None:
            metadata["chunk_id"] = match.chunk_id
        if match.document_id is not None:
            metadata["document_id"] = match.document_id

        metadata[SCORE_METADATA_KEY] = float(match.score)

        model = _as_text(match.model)
        if model:
            metadata["embedding_model"] = model

        return KnowledgeChunk(content=content, source=source, metadata=metadata)


def _preview(query: str) -> str:
    """把查询文本截断成日志友好的预览。"""
    text = _as_text(query)
    if len(text) <= _QUERY_PREVIEW_LEN:
        return text
    return text[:_QUERY_PREVIEW_LEN] + "…"


__all__ = [
    "RETRIEVAL_RESULT_FIELDS",
    "SCORE_METADATA_KEY",
    "RetrieverConfigError",
    "RetrieverUnavailableError",
    "VectorKnowledgeRetriever",
    "VectorRetrieverError",
    "build_query",
    "chunk_to_result",
]
