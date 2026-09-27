# -*- coding: utf-8 -*-
"""VectorStore **双后端一致性测试 · 统一测试环境**（测试辅助模块，**不是**自检脚本）。

存在的理由
----------
``SqlAlchemyVectorStore``（全表扫描 + Python 侧余弦）与 ``ChromaVectorStore``
（HNSW ANN 召回 + ``select_matches`` 精确重排）是**两个实现、一份排序口径**
（``vector_store.select_matches``）。要比较它们，最大的风险不是「比不对」，
而是**两边输入不一样**——那样比出来的差异全是噪声，排查方向会完全跑偏：

- corpus 不一样（一篇文档多/少、正文被改过）
- query 不一样（少跑一条、措辞不同）
- Embedding 配置不一样（换过模型、维度不同 ⇒ 向量本来就不可比）
- ``top_k`` / ``min_score`` 不一样（截断点与阈值不同 ⇒ 结果条数天生不同）

所以本模块把**这五项全部固定下来**，并给出一个**统一执行入口**
（:func:`run_case` / :func:`run_case_observed`），保证两个后端拿到的是
**逐字节相同的输入**；再由 :func:`compare_chunk_lists` 给出**容差感知**的比对契约
（跨后端比对不能要求分数逐位相同——ANN 索引按 float32 存取，详见下）。

本模块**只准备环境**：不写任何「哪个后端更准」的断言，也不修改任何生产代码
（``VectorStore`` / ``VectorKnowledgeRetriever`` / ``EmbeddingService`` 一行未改）。

固定的五项
----------
===================  ==========================================================
同一批数据             ``rag_query_set.json`` 的 corpus（16 篇 ⇒ 16 片，见下）
同一组 query          同一文件的 16 条 query 文本
同一 Embedding 配置   ``knowledge_rag.default_embedder()``，**同一个实例**被两个后端共用
同一 top_k            :data:`ENV_TOP_K`（对所有 query 统一施加）
同一 min_score        :data:`ENV_MIN_SCORE`（对所有 query 统一施加）
===================  ==========================================================

.. note::
   ``rag_query_set.json`` 里每条 query 自带**逐条标定**的 ``top_k`` / ``min_score``
   ——那是「检索准确性」测试用的（任务 52）。**一致性**测试**刻意不用**它们：
   统一成一组常量，才能保证「两个后端输入完全相同」。本模块的
   :func:`load_queries` 只取 query 文本，参数一律由本模块的常量填充。

.. warning::
   **跨后端比对必须容差感知**（任务 49 的实测结论）。ANN 索引按 **float32** 存取向量，
   回读重算余弦会带上 ~1e-8 的漂移，后果是：
   ① 分数在数值上等于 0 的近似并列项**顺序互换**；
   ② ``top_k`` 截断处的入选者**可能不同**。
   因此比较规则是「条数 + 分数序列近似相等 + **显著命中集合**一致」，
   **不能**写成「命中的 chunk_id 集合必须相同」。这些口径已固化在
   :data:`SCORE_TOL` / :func:`stable_hits` / :func:`compare_chunk_lists` 里。

用法
----
::

    async with open_env() as env:
        for case in env.queries:
            a = await run_case_observed(env, "sql", case)
            b = await run_case_observed(env, "chroma", case)
            assert a.query_vector == b.query_vector      # 输入完全相同
            assert a.search_kwargs == b.search_kwargs    # 参数完全相同
            cmp = compare_chunk_lists(a.chunks, b.chunks)
            assert cmp.count_equal and cmp.scores_within_tol and cmp.stable_equal

.. note::
   ``tests/`` 下没有 ``__init__.py``，所以消费方要先把本目录加进 ``sys.path``::

       sys.path.insert(0, str(Path(__file__).resolve().parent))
       from vector_backend_env import open_env, run_case, compare_chunk_lists
"""

from __future__ import annotations

import itertools
import json
import os
import sys
from collections.abc import Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Optional, Sequence, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
import regression_env  # noqa: E402,F401  钉住离线 Embedding + RAG 阈值（回归不受 .env 影响）
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import KnowledgeChunk as KnowledgeChunkRow  # noqa: E402
from services.embedding_service import (  # noqa: E402
    EmbeddingInfo,
    describe_embedding,
)
from services.knowledge_import_pipeline import (  # noqa: E402
    KnowledgeImportPipeline,
    STATUS_OK,
)
from services.knowledge_rag import default_embedder  # noqa: E402
from services.knowledge_retriever import KnowledgeChunk  # noqa: E402
from services.vector_knowledge_retriever import (  # noqa: E402
    SCORE_METADATA_KEY,
    VectorKnowledgeRetriever,
)
from services.vector_store import VectorMatch, VectorRecord  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

# ============================================================
# 一、固定配置（**所有后端必须拿到这些完全相同的值**）
# ============================================================
#: 数据集文件（corpus + query 的唯一来源）。
DATASET_PATH = BACKEND_DIR / "scripts" / "rag_query_set.json"

#: 固定的 ``top_k``：对所有 query 统一施加，两个后端相同。
ENV_TOP_K = 5

#: 固定的 ``min_score``：对所有 query 统一施加，两个后端相同。
#: 取 0.05 —— 低于数据集里**实测最低**的 top-1 分数（0.1184），
#: 于是每条 query 在两个后端上**都**能检出结果（否则「空 vs 非空」的差异
#: 会被误读成后端差异，其实是阈值卡在了噪声上）。
ENV_MIN_SCORE = 0.05

#: 跨后端分数容差（任务 49 实测：float32 回读漂移量级 ≤ 1e-6）。
SCORE_TOL = 1e-6

#: 后端标识（顺序即报告顺序）。
BACKEND_SQL = "sql"
BACKEND_CHROMA = "chroma"
BACKENDS: Tuple[str, ...] = (BACKEND_SQL, BACKEND_CHROMA)

#: 本环境要求「不要修改」的三个生产模块（供消费方的守卫断言引用）。
PROTECTED_MODULES: Tuple[str, ...] = (
    "services/vector_store.py",
    "services/vector_knowledge_retriever.py",
    "services/embedding_service.py",
)


class EnvUsageError(Exception):
    """使用本环境时的调用方错误（如后端名写错）。"""


class EnvUnavailableError(Exception):
    """环境无法构建（如 chromadb 未安装）。"""


# ============================================================
# 二、数据来源（固定输入）
# ============================================================
@dataclass(frozen=True)
class QueryCase:
    """一条固定 query —— ``top_k`` / ``min_score`` **来自环境常量**，不来自数据文件。

    ``index`` 只是给报告用的稳定编号（0 起）；``text`` 是唯一被送去检索的东西。
    """

    index: int
    text: str
    top_k: int
    min_score: float


def _read_dataset() -> Dict[str, Any]:
    """读取数据集文件（每次现读，避免模块级缓存掩盖「文件被改过」）。"""
    return json.loads(DATASET_PATH.read_text(encoding="utf-8"))


def load_corpus_documents() -> Tuple[Dict[str, str], ...]:
    """固定的 corpus：``{title, content, category, source}`` 四键字典，共 16 篇。

    与 ``knowledge_document_service.create_document`` 的入参契约一致，
    因此可以直接喂给 :class:`KnowledgeImportPipeline`。
    """
    docs = _read_dataset()["corpus"]["documents"]
    return tuple(dict(d) for d in docs)


def load_queries() -> Tuple[QueryCase, ...]:
    """固定的 query 集合：**只取 query 文本**，参数由环境常量统一填充。

    刻意**不读**数据集里逐条标定的 ``top_k`` / ``min_score``——那是准确性测试
    的参数；一致性测试要的是「两个后端输入完全相同」，必须用一组统一常量。
    """
    cases = []
    for i, item in enumerate(_read_dataset()["queries"]):
        cases.append(QueryCase(
            index=i, text=item["query"], top_k=ENV_TOP_K, min_score=ENV_MIN_SCORE,
        ))
    return tuple(cases)


# ============================================================
# 三、观测器（只观测，不改行为）
# ============================================================
class RecordingEmbedder:
    """包住真实 EmbeddingService，记录每次 ``embed`` 的**入参与返回向量**。

    刻意**不继承** ``EmbeddingService``：这样走的是 ``VectorKnowledgeRetriever._embed``
    里那条「鸭子类型兜底校验」（``require_vector``）。``name`` / ``dimension``
    原样透传，保证向量库的 ``model`` 过滤条件不受影响。
    """

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.name = getattr(inner, "name", "")
        self.dimension = getattr(inner, "dimension", 0)
        self.calls: List[str] = []
        self.vectors: List[List[float]] = []

    async def embed(self, text: str) -> List[float]:
        vector = await self.inner.embed(text)
        self.calls.append(text)
        self.vectors.append(list(vector))
        return vector


class RecordingStore:
    """包住真实 VectorStore，记录每次 ``search`` 的**完整入参**与返回。"""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.calls: List[Dict[str, Any]] = []
        self.results: List[List[VectorMatch]] = []

    async def search(self, query_vector: Any, **kwargs: Any) -> List[VectorMatch]:
        result = await self.inner.search(query_vector, **kwargs)
        self.calls.append({"query_vector": query_vector, **kwargs})
        self.results.append(list(result))
        return result


@dataclass(frozen=True)
class ObservedRun:
    """一次检索的**完整观测记录**：既含结果，也含「输入是什么」。"""

    backend: str
    case: QueryCase
    chunks: List[KnowledgeChunk]
    embed_input: str
    query_vector: Tuple[float, ...]
    search_kwargs: Dict[str, Any] = field(default_factory=dict)

    @property
    def scores(self) -> Tuple[float, ...]:
        return scores_of(self.chunks)

    @property
    def chunk_ids(self) -> Tuple[Any, ...]:
        return tuple(c.metadata.get("chunk_id") for c in self.chunks)


# ============================================================
# 四、环境对象
# ============================================================
class BackendEnv:
    """统一测试环境：**一份数据、一套配置、两个后端**。

    - ``session`` 是**唯一**的数据库会话，两个后端都绑定在它上面；
      ``chroma_store`` 内部还组合了一个 ``SqlAlchemyVectorStore``（权威副本），
      所以「索引内容 == 权威行内容」是结构上成立的，不是靠约定。
    - ``records`` 是入库后从**权威行**读回的快照（16 条，按主键升序）——
      它就是「同一批 KnowledgeChunk 数据」的准确定义，两个后端都必须服务这批数据。
    - 用完必须 :meth:`close`（推荐 ``async with open_env() as env``）。
    """

    def __init__(
        self,
        *,
        engine: Any,
        session: AsyncSession,
        embedder: Any,
        sql_store: Any,
        chroma_store: Optional[Any],
        records: Tuple[VectorRecord, ...],
        queries: Tuple[QueryCase, ...],
        top_k: int,
        min_score: float,
        collection_name: str,
        embedding_info: EmbeddingInfo,
        chroma_available: bool,
    ) -> None:
        self.engine = engine
        self.session = session
        self.embedder = embedder
        self.sql_store = sql_store
        self.chroma_store = chroma_store
        self.records = records
        self.queries = queries
        self.top_k = top_k
        self.min_score = min_score
        self.collection_name = collection_name
        self.embedding_info = embedding_info
        self.chroma_available = chroma_available
        self._closed = False

    # ---------------- 只读访问 ----------------
    @property
    def closed(self) -> bool:
        """环境是否已关闭（``close()`` 幂等，重复调用无副作用）。"""
        return self._closed

    @property
    def backends(self) -> Tuple[str, ...]:
        """本环境下**可用**的后端（chromadb 未装时只有 ``sql``）。"""
        if self.chroma_store is None:
            return (BACKEND_SQL,)
        return BACKENDS

    def store_for(self, backend: str) -> Any:
        """按后端名取 store；名字写错**立刻报错**，而不是悄悄回落到 sql。"""
        if backend == BACKEND_SQL:
            return self.sql_store
        if backend == BACKEND_CHROMA:
            if self.chroma_store is None:
                raise EnvUnavailableError(
                    "本环境没有 chroma 后端（chromadb 未安装或构建时 with_chroma=False）"
                )
            return self.chroma_store
        raise EnvUsageError(f"未知后端 {backend!r}，可选：{BACKENDS}")

    def describe(self) -> Dict[str, Any]:
        """环境摘要（用于报告 / 排查）。"""
        return {
            "dataset": DATASET_PATH.relative_to(BACKEND_DIR.parent).as_posix(),
            "corpus_documents": len(self.records),
            "queries": len(self.queries),
            "top_k": self.top_k,
            "min_score": self.min_score,
            "backends": list(self.backends),
            "collection_name": self.collection_name,
            "embedding": self.embedding_info.to_dict(),
            "score_tol": SCORE_TOL,
        }

    # ---------------- 生命周期 ----------------
    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self.session.close()
        finally:
            await self.engine.dispose()

    async def __aenter__(self) -> "BackendEnv":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()


# ============================================================
# 五、构建环境
# ============================================================
_COLLECTION_SEQ = itertools.count(1)


def _next_collection_name() -> str:
    """每个环境实例一个**独立集合名**。

    ``chromadb.EphemeralClient()`` **不是**「每次调用都得到一个空库」：
    chroma 按 settings 缓存 System，同进程内多次调用会**共享**内存数据。
    因此隔离只能靠**集合名**（这个坑任务 48 已踩过）。
    """
    return f"env_consistency_{next(_COLLECTION_SEQ)}"


def _ephemeral_client() -> Any:
    """进程内 chroma 客户端（不落盘、不启动服务、关掉遥测）。"""
    try:
        from services.vector_store_chroma import load_chromadb
    except Exception as exc:  # pragma: no cover - 取决于本机环境
        raise EnvUnavailableError(f"无法加载 chroma 后端模块：{exc}") from exc
    chromadb = load_chromadb()
    factory = getattr(chromadb, "EphemeralClient", None)
    if factory is not None:
        return factory()
    return chromadb.Client(settings=chromadb.Settings(
        is_persistent=False, anonymized_telemetry=False,
    ))


async def build_env(
    *,
    top_k: int = ENV_TOP_K,
    min_score: float = ENV_MIN_SCORE,
    collection_name: Optional[str] = None,
    with_chroma: bool = True,
    client: Any = None,
) -> BackendEnv:
    """构建统一测试环境。

    :param top_k: 固定 ``top_k``（两个后端相同）。
    :param min_score: 固定 ``min_score``（两个后端相同）。
    :param collection_name: chroma 集合名；``None`` → 自动生成**唯一**名字（保证隔离）。
    :param with_chroma: 是否构建 chroma 后端（``False`` → 只构建 sql，用于对照）。
    :param client: 注入 chroma 客户端（测试用）；``None`` → 进程内 ephemeral 客户端。
    :raises EnvUnavailableError: 要求 chroma 但 ``chromadb`` 不可用。
    """
    documents = load_corpus_documents()
    queries = load_queries()

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,               # 同一引擎的所有会话共享同一个内存库
        connect_args={"check_same_thread": False},
    )
    session: Optional[AsyncSession] = None
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        session = factory()

        # 两个后端都绑在**同一个会话**上 ⇒ 权威行只有一份
        sql_store = SqlAlchemyVectorStore(session)

        chroma_store = None
        resolved_name = collection_name or _next_collection_name()
        if with_chroma:
            try:
                from services.vector_store_chroma import ChromaVectorStore
            except Exception as exc:  # pragma: no cover
                raise EnvUnavailableError(f"无法加载 ChromaVectorStore：{exc}") from exc
            chroma_store = ChromaVectorStore(
                session,
                client=client if client is not None else _ephemeral_client(),
                collection_name=resolved_name,
            )

        # 用「会同时写权威行 + 镜像索引」的那个后端做入库 ⇒ 一次导入，两边都就位
        embedder = default_embedder()
        seeding_store = chroma_store if chroma_store is not None else sql_store
        pipeline = KnowledgeImportPipeline(session, embedder=embedder, store=seeding_store)
        for doc in documents:
            report = await pipeline.import_document(doc)
            if report["status"] != STATUS_OK:
                raise EnvUnavailableError(
                    f"corpus 入库失败（{doc.get('title')!r}）："
                    f"status={report['status']} stage={report['stage']} "
                    f"error={report.get('error')}"
                )

        # 快照：从**权威行**读回「同一批数据」的准确内容（两个后端都必须服务它）
        ids = (await session.execute(
            select(KnowledgeChunkRow.id).order_by(KnowledgeChunkRow.id))).scalars().all()
        records = tuple(await sql_store.load_records([int(i) for i in ids]))

        env = BackendEnv(
            engine=engine,
            session=session,
            embedder=embedder,
            sql_store=sql_store,
            chroma_store=chroma_store,
            records=records,
            queries=queries,
            top_k=top_k,
            min_score=min_score,
            collection_name=resolved_name,
            embedding_info=describe_embedding(embedder),
            chroma_available=chroma_store is not None,
        )
        return env
    except BaseException:
        if session is not None:
            await session.close()
        await engine.dispose()
        raise


@asynccontextmanager
async def open_env(**kwargs: Any) -> Iterator[BackendEnv]:
    """``async with open_env() as env:`` —— 用完自动关闭。"""
    env = await build_env(**kwargs)
    try:
        yield env
    finally:
        await env.close()


# ============================================================
# 六、统一执行入口（**两个后端拿到完全相同输入**的落点）
# ============================================================
def build_retriever(env: BackendEnv, backend: str, case: QueryCase) -> VectorKnowledgeRetriever:
    """为指定后端构造检索器，参数**一律取自** ``case``（而 case 取自环境常量）。"""
    return VectorKnowledgeRetriever(
        env.embedder,
        env.store_for(backend),
        top_k=case.top_k,
        min_score=case.min_score,
    )


async def run_case(env: BackendEnv, backend: str, case: QueryCase) -> List[KnowledgeChunk]:
    """在指定后端上跑一条 query，返回 ``List[KnowledgeChunk]``。"""
    return await build_retriever(env, backend, case).retrieve({}, case.text, {})


async def run_case_observed(
    env: BackendEnv, backend: str, case: QueryCase
) -> ObservedRun:
    """同 :func:`run_case`，但额外记录**每一跳的实际入参**。

    这是「两个后端输入完全相同」的取证手段：拿两个后端的
    ``ObservedRun`` 逐项比较 ``embed_input`` / ``query_vector`` / ``search_kwargs``，
    全部相等才说明输入真的相同——只看最终结果**证明不了**这一点。
    """
    rec_embedder = RecordingEmbedder(env.embedder)
    rec_store = RecordingStore(env.store_for(backend))
    retriever = VectorKnowledgeRetriever(
        rec_embedder, rec_store, top_k=case.top_k, min_score=case.min_score
    )
    chunks = await retriever.retrieve({}, case.text, {})
    return ObservedRun(
        backend=backend,
        case=case,
        chunks=chunks,
        embed_input=rec_embedder.calls[0] if rec_embedder.calls else "",
        query_vector=tuple(rec_embedder.vectors[0]) if rec_embedder.vectors else (),
        search_kwargs=dict(rec_store.calls[0]) if rec_store.calls else {},
    )


async def run_all_backends(
    env: BackendEnv, case: QueryCase
) -> Dict[str, ObservedRun]:
    """一条 query 跑遍**所有可用后端**，返回 ``{backend: ObservedRun}``。"""
    return {b: await run_case_observed(env, b, case) for b in env.backends}


# ============================================================
# 七、比对契约（**容差感知**，任务 49 的结论固化于此）
# ============================================================
def scores_of(chunks: Sequence[KnowledgeChunk]) -> Tuple[float, ...]:
    """取出分数序列（顺序即检索排序）。"""
    return tuple(float(c.metadata.get(SCORE_METADATA_KEY, 0.0)) for c in chunks)


def stable_hits(chunks: Sequence[KnowledgeChunk], *, tol: float = SCORE_TOL) -> FrozenSet[Any]:
    """**显著命中**：分数明显高于**末位**（即不与截断边界并列）的 ``chunk_id`` 集合。

    为什么需要它：ANN 索引按 float32 存取 ⇒ 分数有 ~1e-8 漂移，
    与 ``top_k`` 截断边界**并列**的项可能换人。这些项属于「模糊带」，
    跨后端不同是**允许**的；只有**严格高于**末位 + ``tol`` 的项才必须两边一致。
    """
    if not chunks:
        return frozenset()
    cut = min(scores_of(chunks))
    return frozenset(
        c.metadata.get("chunk_id") for c in chunks
        if float(c.metadata.get(SCORE_METADATA_KEY, 0.0)) > cut + tol
    )


def ambiguous_hits(chunks: Sequence[KnowledgeChunk], *, tol: float = SCORE_TOL) -> Tuple[Any, ...]:
    """**模糊带**：与末位并列（差 ≤ ``tol``）的 ``chunk_id``，跨后端允许不同。"""
    if not chunks:
        return ()
    cut = min(scores_of(chunks))
    return tuple(
        c.metadata.get("chunk_id") for c in chunks
        if float(c.metadata.get(SCORE_METADATA_KEY, 0.0)) <= cut + tol
    )


@dataclass(frozen=True)
class Comparison:
    """两个后端在同一输入下的比对结果（**不判优劣，只描述差异**）。"""

    left_backend: str
    right_backend: str
    left_count: int
    right_count: int
    count_equal: bool
    score_deltas: Tuple[float, ...]
    max_abs_delta: float
    scores_within_tol: bool
    stable_left: FrozenSet[Any]
    stable_right: FrozenSet[Any]
    stable_equal: bool
    ambiguous_left: Tuple[Any, ...]
    ambiguous_right: Tuple[Any, ...]

    @property
    def ok(self) -> bool:
        """按既定口径「可接受」：条数相同 + 分数近似 + 显著命中一致。"""
        return self.count_equal and self.scores_within_tol and self.stable_equal

    def describe(self) -> str:
        return (
            f"{self.left_backend} vs {self.right_backend}: "
            f"count={self.left_count}/{self.right_count}"
            f"（{'同' if self.count_equal else '异'}） "
            f"max|Δscore|={self.max_abs_delta:.3e}"
            f"（{'≤' if self.scores_within_tol else '>'} tol） "
            f"显著命中={len(self.stable_left)}/{len(self.stable_right)}"
            f"（{'同' if self.stable_equal else '异'}） "
            f"模糊带={len(self.ambiguous_left)}/{len(self.ambiguous_right)}"
        )


def compare_chunk_lists(
    left: Sequence[KnowledgeChunk],
    right: Sequence[KnowledgeChunk],
    *,
    left_backend: str = "left",
    right_backend: str = "right",
    tol: float = SCORE_TOL,
) -> Comparison:
    """容差感知地比较两个后端的结果。

    **比较口径**（刻意**不**要求「命中的 chunk_id 集合相同」）：

    1. 条数相同；
    2. 同位分数差 ≤ ``tol``（``zip`` 对齐，长度不等时按较短的比）；
    3. **显著命中集合**一致（见 :func:`stable_hits`）——
       与截断边界并列的「模糊带」成员允许不同。
    """
    ls, rs = scores_of(left), scores_of(right)
    deltas = tuple(r - l for l, r in zip(ls, rs))
    max_abs = max((abs(d) for d in deltas), default=0.0)
    sl, sr = stable_hits(left, tol=tol), stable_hits(right, tol=tol)
    return Comparison(
        left_backend=left_backend,
        right_backend=right_backend,
        left_count=len(left),
        right_count=len(right),
        count_equal=len(left) == len(right),
        score_deltas=deltas,
        max_abs_delta=max_abs,
        scores_within_tol=max_abs <= tol,
        stable_left=sl,
        stable_right=sr,
        stable_equal=sl == sr,
        ambiguous_left=ambiguous_hits(left, tol=tol),
        ambiguous_right=ambiguous_hits(right, tol=tol),
    )


__all__ = [
    "BACKENDS",
    "BACKEND_CHROMA",
    "BACKEND_SQL",
    "BackendEnv",
    "Comparison",
    "DATASET_PATH",
    "ENV_MIN_SCORE",
    "ENV_TOP_K",
    "EnvUnavailableError",
    "EnvUsageError",
    "ObservedRun",
    "PROTECTED_MODULES",
    "QueryCase",
    "RecordingEmbedder",
    "RecordingStore",
    "SCORE_TOL",
    "ambiguous_hits",
    "build_env",
    "build_retriever",
    "compare_chunk_lists",
    "load_corpus_documents",
    "load_queries",
    "open_env",
    "run_all_backends",
    "run_case",
    "run_case_observed",
    "scores_of",
    "stable_hits",
]
