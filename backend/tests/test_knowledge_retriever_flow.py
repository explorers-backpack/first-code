# -*- coding: utf-8 -*-
"""AI 面试 RAG · **基础检索流程**验证（脚本式，非 pytest）。

验证目标：确认下面这条链路是通的::

    query 文本
        │  ① build_query（纯函数：决定「查什么」）
        ▼
      EmbeddingService.embed(query)          → query 向量
        │  ②
        ▼
      VectorStore.search(vector, top_k, model, min_score, …)   → List[VectorMatch]
        │  ③
        ▼
      VectorKnowledgeRetriever.retrieve(...) → List[KnowledgeChunk]
        │  ④  过滤空正文 → 按正文去重 → 计分写进 metadata
        ▼
      KnowledgeChunk{content, source, metadata{score, chunk_id, …}}  ⑤

测试数据：``backend/scripts/rag_query_set.json``（自带受控 corpus：16 篇 / 16 条 query）

运行：``python backend/tests/test_knowledge_retriever_flow.py``

需求点名的 5 条验证（见 [4] 节）：

1. **query 可以正常检索**——每条 query 都不抛异常、都返回非空结果、条数 ≤ top_k
2. **返回 chunk 结构正确**——恒为 ``KnowledgeChunk``，``to_dict()`` 恰好三键
3. **content 存在**——非空字符串，且与库中该切片的正文**逐字节相等**（原样保留）
4. **metadata 存在**——非空 dict，含 ``score`` / ``chunk_id`` / ``document_id`` /
   ``category`` / ``embedding_model``；``source`` 已**提升**为顶层字段
5. **score 存在**——``metadata["score"]`` 是 float、落在余弦值域，且与
   ``VectorStore.search`` 返回的**同位置** ``VectorMatch.score`` 一致

.. warning::
   **本套件只验证「链路是否通」，不评价「检索是否准」。**
   当前默认 Embedding 是离线占位 ``HashEmbeddingService``（词面哈希，**非语义**），
   用它算出的分数**只能证明链路连通**，不能代表真实语义检索效果。
   因此本套件**刻意不读取**数据文件里的「期望类别 / 期望关键词」两个字段
   ——那两项属于「检索准确性」，由 ``tests/test_rag_query_set.py`` 负责；
   本文件甚至有一条守卫断言自己**没有**引用它们（见 [5] 节）。
   （注意：那两个字面键名**不能**出现在本文件里，否则守卫会自己命中自己。）

.. note::
   本套件**只调用**生产代码，不修改它：``VectorKnowledgeRetriever`` 与
   ``VectorStore`` 的接口签名、抽象方法集合都有断言锁死。
   全程使用内存 SQLite，**不读生产 MySQL**。
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import sys
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))
import regression_env  # noqa: E402,F401  钉住离线 Embedding + RAG 阈值（回归不受 .env 影响）
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from sqlalchemy import func, select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import KnowledgeChunk as KnowledgeChunkRow  # noqa: E402
from models import KnowledgeDocument  # noqa: E402
from services.embedding_service import EmbeddingService  # noqa: E402
from services.knowledge_import_pipeline import (  # noqa: E402
    KnowledgeImportPipeline,
    STATUS_OK,
)
from services.knowledge_rag import default_embedder  # noqa: E402
from services.knowledge_retriever import (  # noqa: E402
    KNOWLEDGE_CHUNK_FIELDS,
    KnowledgeChunk,
    KnowledgeRetriever,
)
from services.vector_knowledge_retriever import (  # noqa: E402
    SCORE_METADATA_KEY,
    RetrieverConfigError,
    VectorKnowledgeRetriever,
    build_query,
    chunk_to_result,
)
from services.vector_store import (  # noqa: E402
    VectorMatch,
    VectorStore,
    VectorStoreDimensionError,
)
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

SET_PATH = BACKEND_DIR / "scripts" / "rag_query_set.json"

#: 链路末端 ``KnowledgeChunk`` 的字段名与顺序（三键契约）。
CHUNK_KEYS = ("content", "source", "metadata")

#: ``KnowledgeChunk.metadata`` 里**必须**出现的键。
#: 前四个来自真实检索链路的溯源需求，最后一个来自切片阶段。
REQUIRED_METADATA_KEYS = ("score", "chunk_id", "document_id", "category", "embedding_model")

#: 本轮要求「不要修改」的两个接口模块。
PROTECTED_MODULES = (
    "services/vector_knowledge_retriever.py",
    "services/vector_store.py",
)

#: 余弦相似度的取值域（``score`` 必须落在这里）。
COSINE_MIN, COSINE_MAX = -1.0, 1.0

_PASSED = 0
_FAILED = 0


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name
          + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def _section(title: str) -> None:
    print("\n" + "-" * 74)
    print(title)
    print("-" * 74)


def _fmt_vector(vector: List[float], head: int = 3) -> str:
    preview = ", ".join(f"{x:.4f}" for x in vector[:head])
    nonzero = sum(1 for x in vector if x != 0.0)
    return f"dim={len(vector)} 前{head}位=[{preview}] 非零分量={nonzero}"


# ============================================================
# 记录型包装器（只观测，不改行为）
# ============================================================
class RecordingEmbedder:
    """包住真实 EmbeddingService，记录每次 ``embed`` 的**入参与返回向量**。

    刻意**不继承** ``EmbeddingService``：这样它走的是 ``VectorKnowledgeRetriever._embed``
    里那条「鸭子类型兜底校验」（``require_vector``），顺带把那条路径也验了。
    ``name`` / ``dimension`` 原样透传，保证向量库的 ``model`` 过滤条件不受影响。
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


class StubStore:
    """最小桩：``search`` 返回固定内容（用于边界用例，不碰数据库）。"""

    def __init__(self, matches: Any = None, error: BaseException | None = None) -> None:
        self._matches = matches if matches is not None else []
        self._error = error
        self.calls = 0

    async def search(self, query_vector: Any, **kwargs: Any) -> List[VectorMatch]:
        self.calls += 1
        if self._error is not None:
            raise self._error
        return list(self._matches)


# ============================================================
# [1] 链路契约（两个接口一行未改）
# ============================================================
def check_contract() -> None:
    _section("[1] 链路契约（Retriever / VectorStore 接口未改）")

    _check("VectorKnowledgeRetriever 继承 KnowledgeRetriever（可无感替换接口）",
           issubclass(VectorKnowledgeRetriever, KnowledgeRetriever))
    _check("VectorKnowledgeRetriever.retrieve 签名仍是 (self, job_info, topic, context)",
           list(inspect.signature(VectorKnowledgeRetriever.retrieve).parameters)
           == ["self", "job_info", "topic", "context"],
           str(list(inspect.signature(VectorKnowledgeRetriever.retrieve).parameters)))
    _check("retrieve 仍是协程函数（await 可直接调用）",
           inspect.iscoroutinefunction(VectorKnowledgeRetriever.retrieve))

    expected_search = ["self", "query_vector", "top_k", "model",
                       "document_id", "category", "min_score"]
    _check(f"VectorStore.search 签名仍是 {tuple(expected_search)}",
           list(inspect.signature(VectorStore.search).parameters) == expected_search,
           str(list(inspect.signature(VectorStore.search).parameters)))
    _check("VectorStore.search 的关键字参数全是 keyword-only",
           all(p.kind is inspect.Parameter.KEYWORD_ONLY
               for n, p in inspect.signature(VectorStore.search).parameters.items()
               if n not in ("self", "query_vector")))
    _check("VectorStore 的抽象方法仍是 add / search / count",
           getattr(VectorStore, "__abstractmethods__", None)
           == frozenset({"add", "search", "count"}),
           str(getattr(VectorStore, "__abstractmethods__", None)))
    _check("EmbeddingService 的抽象方法仍是 {_embed_one}",
           getattr(EmbeddingService, "__abstractmethods__", None) == frozenset({"_embed_one"}),
           str(getattr(EmbeddingService, "__abstractmethods__", None)))

    _check("KnowledgeChunk 仍是 frozen dataclass，字段恒为 (content, source, metadata)",
           is_dataclass(KnowledgeChunk) and KnowledgeChunk.__dataclass_params__.frozen
           and tuple(f.name for f in fields(KnowledgeChunk)) == CHUNK_KEYS,
           str(tuple(f.name for f in fields(KnowledgeChunk))))
    _check("KNOWLEDGE_CHUNK_FIELDS 常量与 CHUNK_KEYS 一致",
           tuple(KNOWLEDGE_CHUNK_FIELDS) == CHUNK_KEYS, str(KNOWLEDGE_CHUNK_FIELDS))

    offenders = [rel for rel in PROTECTED_MODULES
                 if "test_knowledge_retriever_flow" in
                 (BACKEND_DIR / rel).read_text(encoding="utf-8")]
    _check("★ 两个受保护接口模块都不引用本测试文件",
           not offenders, str(offenders))


# ============================================================
# [2] 测试数据准备（corpus 入库 → 向量库有东西可查）
# ============================================================
async def prepare_corpus(db: AsyncSession, embedder: Any,
                         docs: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    _section("[2] 测试数据准备（rag_query_set.json 的 corpus 入库）")

    pipeline = KnowledgeImportPipeline(db, embedder=embedder)
    bad: List[str] = []
    for doc in docs:
        report = await pipeline.import_document(doc)
        if report["status"] != STATUS_OK or report["chunk_count"] != 1:
            bad.append(f"{doc['title']}: status={report['status']} "
                       f"chunks={report['chunk_count']}")
    _check(f"corpus {len(docs)} 篇全部入库成功（status=ok，每篇 1 片）",
           not bad, str(bad))

    doc_rows = int((await db.execute(
        select(func.count()).select_from(KnowledgeDocument))).scalar_one())
    _check("knowledge_document 行数 == corpus 篇数",
           doc_rows == len(docs), f"{doc_rows} vs {len(docs)}")

    rows = (await db.execute(select(KnowledgeChunkRow))).scalars().all()
    _check("knowledge_chunk 行数 == corpus 篇数（每篇恰好 1 片）",
           len(rows) == len(docs), f"{len(rows)} vs {len(docs)}")

    # 提前把需要的列读出来（避免离开会话后再触发惰性加载 → MissingGreenlet）
    row_by_id = {
        int(r.id): {
            "content": r.content,
            "document_id": r.document_id,
            "embedding_model": r.embedding_model,
            "embedding_dim": r.embedding_dim,
        }
        for r in rows
    }
    not_embedded = [cid for cid, r in row_by_id.items() if r["embedding_dim"] is None]
    _check("每片都已向量化（embedding_dim 非空）", not not_embedded, str(not_embedded))

    wrong_model = sorted({r["embedding_model"] for r in row_by_id.values()} - {embedder.name})
    _check(f"每片的 embedding_model 都等于 embedder.name（{embedder.name!r}）",
           not wrong_model, str(wrong_model))
    wrong_dim = sorted({r["embedding_dim"] for r in row_by_id.values()} - {embedder.dimension})
    _check(f"每片的 embedding_dim 都等于 embedder.dimension（{embedder.dimension}）",
           not wrong_dim, str(wrong_dim))

    print(f"  embedder = {embedder.name} / dimension={embedder.dimension}"
          f"（离线占位，**非语义**——本套件不评价准确率）")
    return row_by_id


# ============================================================
# [3] 链路逐跳取证 + [4] 返回结构
# ============================================================
async def run_flow(db: AsyncSession, embedder: Any,
                   queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    _section("[3] 链路逐跳取证（每条 query 记录 ①~⑤ 五跳的实际入参/返回）")

    rows: List[Dict[str, Any]] = []
    for q in queries:
        # 每条 query 用**全新的**记录器，保证调用次数与入参互不串台
        rec_embedder = RecordingEmbedder(embedder)
        rec_store = RecordingStore(SqlAlchemyVectorStore(db))
        retriever = VectorKnowledgeRetriever(
            rec_embedder, rec_store, top_k=q["top_k"], min_score=q["min_score"]
        )
        chunks = await retriever.retrieve({}, q["query"], {})
        rows.append({
            "query": q["query"],
            "top_k": q["top_k"],
            "min_score": q["min_score"],
            "chunks": chunks,
            "built_query": build_query({}, q["query"], {}),
            "query_log": list(retriever.queries),
            "embed_calls": len(rec_embedder.calls),
            "embed_input": rec_embedder.calls[0] if rec_embedder.calls else None,
            "embed_vector": rec_embedder.vectors[0] if rec_embedder.vectors else None,
            "search_calls": len(rec_store.calls),
            "search_args": rec_store.calls[0] if rec_store.calls else None,
            "matches": rec_store.results[0] if rec_store.results else [],
        })

    # ---- ① build_query ----
    _check("① build_query 把 topic 原样作为查询文本（不拼接岗位/上下文）",
           all(r["built_query"] == r["query"] for r in rows),
           str([(r["query"], r["built_query"]) for r in rows
                if r["built_query"] != r["query"]]))
    _check("① 检索器把每次构造的 query 记进 self.queries（可观测）",
           all(r["query_log"] == [r["query"]] for r in rows),
           str([(r["query"], r["query_log"]) for r in rows if r["query_log"] != [r["query"]]]))

    # ---- ② Embedding ----
    _check("② 每条 query 恰好调用 EmbeddingService.embed 一次",
           all(r["embed_calls"] == 1 for r in rows),
           str([(r["query"], r["embed_calls"]) for r in rows if r["embed_calls"] != 1]))
    _check("② embed 的入参就是 query 原文（未被改写/截断）",
           all(r["embed_input"] == r["query"] for r in rows),
           str([(r["query"], r["embed_input"]) for r in rows
                if r["embed_input"] != r["query"]]))
    _check(f"② 返回向量维度 == embedder.dimension（{embedder.dimension}）",
           all(len(r["embed_vector"] or []) == embedder.dimension for r in rows),
           str(sorted({len(r["embed_vector"] or []) for r in rows})))
    _check("② 向量元素全是 float 且非全零（不是空壳向量）",
           all(isinstance(x, float) for r in rows for x in (r["embed_vector"] or []))
           and all(any(x != 0.0 for x in (r["embed_vector"] or [])) for r in rows))

    # ---- ③ VectorStore ----
    _check("③ 每条 query 恰好调用 VectorStore.search 一次",
           all(r["search_calls"] == 1 for r in rows),
           str([(r["query"], r["search_calls"]) for r in rows if r["search_calls"] != 1]))
    _check("③ search 收到的向量就是 ② 产出的那个向量（逐元素相等）",
           all(r["search_args"] is not None
               and list(r["search_args"]["query_vector"]) == list(r["embed_vector"])
               for r in rows))
    _check("③ search 的 top_k / min_score 与构造检索器时传的一致（参数透传）",
           all(r["search_args"]["top_k"] == r["top_k"]
               and r["search_args"]["min_score"] == r["min_score"] for r in rows),
           str([(r["query"], r["search_args"]["top_k"], r["search_args"]["min_score"])
                for r in rows if r["search_args"]["top_k"] != r["top_k"]
                or r["search_args"]["min_score"] != r["min_score"]]))
    _check(f"③ search 的 model 默认取 embedder.name（{embedder.name!r}，不同模型向量不可比）",
           all(r["search_args"]["model"] == embedder.name for r in rows),
           str(sorted({r["search_args"]["model"] for r in rows})))
    _check("③ search 的 category / document_id 默认不收紧（None = 全库）",
           all(r["search_args"]["category"] is None
               and r["search_args"]["document_id"] is None for r in rows))

    # ---- ④ 归一 ----
    _check("④ 返回的是 list，元素全是 KnowledgeChunk（不是 VectorMatch）",
           all(isinstance(r["chunks"], list)
               and all(isinstance(c, KnowledgeChunk) for c in r["chunks"]) for r in rows))
    _check("④ 过滤掉了空正文（返回结果里没有空白 content）",
           all(c.content.strip() for r in rows for c in r["chunks"]))
    _check("④ 按正文去重：同一 query 的结果里没有重复正文",
           all(len({c.content for c in r["chunks"]}) == len(r["chunks"]) for r in rows))

    return rows


def check_chunk_structure(rows: List[Dict[str, Any]],
                          row_by_id: Dict[int, Dict[str, Any]]) -> None:
    _section("[4] 返回结构（用户点名的 5 条：可检索 / 结构 / content / metadata / score）")

    all_chunks = [c for r in rows for c in r["chunks"]]

    # ---- 1. query 可以正常检索 ----
    _check("1. 每条 query 都返回了结果（检索链路连通，非空）",
           all(r["chunks"] for r in rows),
           str([r["query"] for r in rows if not r["chunks"]]))
    _check("1. 每条 query 的结果条数 <= top_k（截断生效）",
           all(len(r["chunks"]) <= r["top_k"] for r in rows),
           str([(r["query"], len(r["chunks"]), r["top_k"]) for r in rows
                if len(r["chunks"]) > r["top_k"]]))

    # ---- 2. 返回 chunk 结构正确 ----
    _check("2. KnowledgeChunk.to_dict() 恰好三键，且顺序为 (content, source, metadata)",
           all(tuple(c.to_dict()) == CHUNK_KEYS for c in all_chunks),
           str(sorted({tuple(c.to_dict()) for c in all_chunks})))
    _check("2. content / source 是 str，metadata 是 dict（三键类型正确）",
           all(isinstance(c.content, str) and isinstance(c.source, str)
               and isinstance(c.metadata, dict) for c in all_chunks))
    _check("2. KnowledgeChunk 是 frozen（下游改不动，检索结果可安全共享）",
           all(_is_frozen(c) for c in all_chunks[:1]))

    # ---- 3. content 存在 ----
    _check("3. content 存在且非空白（每条 chunk 都有正文）",
           all(isinstance(c.content, str) and c.content.strip() for c in all_chunks))
    _check("3. source 存在且非空（可溯源到知识来源）",
           all(c.source.strip() for c in all_chunks),
           str([c.source for c in all_chunks if not c.source.strip()]))
    mismatched = [
        (c.metadata.get("chunk_id"), len(c.content), len(row_by_id.get(
            c.metadata.get("chunk_id"), {}).get("content", "")))
        for c in all_chunks
        if c.metadata.get("chunk_id") in row_by_id
        and c.content != row_by_id[c.metadata["chunk_id"]]["content"]
    ]
    _check("3. content 与库中该切片的正文**逐字节相等**（原样保留，未改写）",
           not mismatched, str(mismatched[:3]))
    unknown = [c.metadata.get("chunk_id") for c in all_chunks
               if c.metadata.get("chunk_id") not in row_by_id]
    _check("3. 每条 chunk 的 chunk_id 都能在库里找到对应切片", not unknown, str(unknown[:3]))

    # ---- 4. metadata 存在 ----
    _check("4. metadata 存在且非空", all(c.metadata for c in all_chunks))
    missing_keys = sorted({k for c in all_chunks for k in REQUIRED_METADATA_KEYS
                           if k not in c.metadata})
    _check(f"4. metadata 含必需键 {REQUIRED_METADATA_KEYS}", not missing_keys, str(missing_keys))
    _check("4. source 已从 metadata **提升**为顶层字段（metadata 里不再重复）",
           all("source" not in c.metadata for c in all_chunks))
    doc_id_bad = [
        (c.metadata.get("chunk_id"), c.metadata.get("document_id"),
         row_by_id[c.metadata["chunk_id"]]["document_id"])
        for c in all_chunks
        if c.metadata.get("chunk_id") in row_by_id
        and c.metadata.get("document_id") != row_by_id[c.metadata["chunk_id"]]["document_id"]
    ]
    _check("4. metadata['document_id'] 与库中该切片的 document_id 一致",
           not doc_id_bad, str(doc_id_bad[:3]))
    model_bad = sorted({c.metadata.get("embedding_model") for c in all_chunks
                        if c.metadata.get("chunk_id") in row_by_id
                        and c.metadata.get("embedding_model")
                        != row_by_id[c.metadata["chunk_id"]]["embedding_model"]})
    _check("4. metadata['embedding_model'] 与库中该切片的 embedding_model 一致",
           not model_bad, str(model_bad))

    # ---- 5. score 存在 ----
    _check(f"5. metadata 含分数键 {SCORE_METADATA_KEY!r}", 
           all(SCORE_METADATA_KEY in c.metadata for c in all_chunks))
    _check("5. score 是 float（不是字符串/None）",
           all(isinstance(c.metadata[SCORE_METADATA_KEY], float) for c in all_chunks),
           str(sorted({type(c.metadata[SCORE_METADATA_KEY]).__name__ for c in all_chunks})))
    _check(f"5. score 落在余弦值域 [{COSINE_MIN}, {COSINE_MAX}]",
           all(COSINE_MIN <= c.metadata[SCORE_METADATA_KEY] <= COSINE_MAX
               for c in all_chunks),
           str(sorted({c.metadata[SCORE_METADATA_KEY] for c in all_chunks})[:5]))
    score_mismatch = [
        (r["query"], c.metadata[SCORE_METADATA_KEY], m.score)
        for r in rows
        for c, m in zip(r["chunks"], _scored_matches(r))
        if c.metadata[SCORE_METADATA_KEY] != float(m.score)
    ]
    _check("5. score 与 VectorStore 返回的同位置 VectorMatch.score 完全一致（未被改写）",
           not score_mismatch, str(score_mismatch[:3]))
    flat_bad = [c for c in all_chunks
                if chunk_to_result(c)["score"] != c.metadata[SCORE_METADATA_KEY]]
    _check("5. chunk_to_result 摊平视图的 score 与 metadata 一致（{content, source, score}）",
           not flat_bad)
    unsorted = [r["query"] for r in rows
                if [c.metadata[SCORE_METADATA_KEY] for c in r["chunks"]]
                != sorted((c.metadata[SCORE_METADATA_KEY] for c in r["chunks"]), reverse=True)]
    _check("5. 结果按 score 单调不增（排序口径生效）", not unsorted, str(unsorted))

    print(f"\n  返回 chunk 总数：{len(all_chunks)}"
          f"（{len(rows)} 条 query，平均 {len(all_chunks) / len(rows):.2f} 条/query）")


def _is_frozen(chunk: KnowledgeChunk) -> bool:
    """``frozen`` 只阻止属性重绑定 —— 直接试一次，能改就说明没冻结。"""
    try:
        chunk.content = "mutated"  # type: ignore[misc]
    except Exception:
        return True
    return False


def _scored_matches(row: Dict[str, Any]) -> List[VectorMatch]:
    """与 ``row["chunks"]`` 对齐的原始命中（跳过被过滤掉的空正文/重复正文）。

    对齐规则与 ``VectorKnowledgeRetriever._to_chunks`` 完全一致：
    空正文丢弃 → 按去空白后的正文去重，保序。
    """
    picked: List[VectorMatch] = []
    seen: set = set()
    for match in row["matches"]:
        content = match.content if isinstance(match.content, str) else ""
        if not content.strip():
            continue
        key = "".join(content.split())
        if key in seen:
            continue
        seen.add(key)
        picked.append(match)
    return picked


# ============================================================
# [5] 边界
# ============================================================
async def check_boundaries(db: AsyncSession, embedder: Any,
                           rows: List[Dict[str, Any]]) -> None:
    _section("[5] 边界（空 query / 过滤 / 去重 / 维度不符 / 不评价语义）")

    sample = rows[0]
    sample_query = sample["query"]

    # ---- 5.1 空 query：没有可检索的线索 ≠ 故障 ----
    probe_embedder = RecordingEmbedder(embedder)
    probe_store = RecordingStore(SqlAlchemyVectorStore(db))
    probe = VectorKnowledgeRetriever(probe_embedder, probe_store, top_k=3)
    empty = await probe.retrieve({}, "", {})
    _check("空 query → 返回 []（不抛异常）", empty == [], repr(empty))
    _check("空 query **不触发** Embedding / VectorStore（短路在第一步）",
           not probe_embedder.calls and not probe_store.calls,
           f"embed={len(probe_embedder.calls)} search={len(probe_store.calls)}")

    # ---- 5.2 min_score 真的在过滤 ----
    strict = VectorKnowledgeRetriever(embedder, SqlAlchemyVectorStore(db),
                                      top_k=3, min_score=1.01)
    strict_chunks = await strict.retrieve({}, sample_query, {})
    _check("min_score=1.01（余弦上限 1.0）→ 返回 []（阈值过滤生效）",
           strict_chunks == [], repr(strict_chunks))

    # ---- 5.3 去重 ----
    duplicated = sample["matches"][0]
    dup_retriever = VectorKnowledgeRetriever(
        embedder, StubStore([duplicated, duplicated]), top_k=5
    )
    dup_chunks = await dup_retriever.retrieve({}, sample_query, {})
    _check("同一正文命中两次 → 去重后只保留 1 条",
           len(dup_chunks) == 1, f"{len(dup_chunks)} 条")

    # ---- 5.4 维度不符：配置错要报「配置错」，而不是「检索不到」 ----
    dim_store = StubStore(error=VectorStoreDimensionError("查询向量 128 维，库里是 256 维"))
    dim_retriever = VectorKnowledgeRetriever(embedder, dim_store, top_k=3)
    try:
        await dim_retriever.retrieve({}, sample_query, {})
        raised = None
    except RetrieverConfigError as exc:
        raised = exc
    _check("向量维度不符 → RetrieverConfigError（改配置/重算向量，重试无用）",
           raised is not None, repr(raised))

    # ---- 5.5 「不评价语义准确率」写成可执行断言 ----
    # 反例 token 运行时拼出，避免出现在本文件源码里导致自引用恒假
    absent_category = "expected_" + "cate" + "gory"
    absent_keywords = "expected_" + "key" + "words"
    own_src = Path(__file__).read_text(encoding="utf-8")
    _check("★ 本套件刻意不读取「期望类别 / 期望关键词」字段（只验链路，不评价语义）",
           absent_category not in own_src and absent_keywords not in own_src,
           f"category={absent_category in own_src} keywords={absent_keywords in own_src}")
    _check("★ 本套件确实读到了数据文件里的 query / top_k / min_score（不是空跑）",
           "query" in own_src and "top_k" in own_src and "min_score" in own_src
           and bool(rows))

    # ---- 5.6 守卫自检 ----
    ghost = "test_knowledge" + "_" + "retriever_" + "FLOW_ZZZ"
    _check("守卫自检：子串检查能区分「有」与「无」",
           ("test_knowledge_retriever_flow" in own_src) is True
           and (ghost in own_src) is False
           and ("test_knowledge_retriever_flow"
                in (BACKEND_DIR / PROTECTED_MODULES[0]).read_text(encoding="utf-8")) is False)


# ============================================================
# 调用流程（打印实际观测到的五跳）
# ============================================================
def print_flow(rows: List[Dict[str, Any]], embedder: Any) -> None:
    print("\n" + "=" * 74)
    print("调用流程（实际观测值）")
    print("=" * 74)
    print("  query 文本")
    print("      │  ① build_query(job_info, topic, context)        纯函数，决定「查什么」")
    print("      ▼")
    print("    EmbeddingService.embed(query)                       query 文本 → query 向量")
    print("      │  ②")
    print("      ▼")
    print("    VectorStore.search(vector, top_k, model, min_score) query 向量 → 相似切片")
    print("      │  ③")
    print("      ▼")
    print("    VectorKnowledgeRetriever.retrieve(...)              过滤空正文 → 去重 → 计分")
    print("      │  ④")
    print("      ▼")
    print("    List[KnowledgeChunk]{content, source, metadata}      ⑤")
    print()

    sample = rows[0]
    print(f"  ── 以第 1 条 query 为例 ──")
    print(f"  输入        : topic={sample['query']!r}")
    print(f"  ① build_query → {sample['built_query']!r}")
    print(f"  ② embed      → {_fmt_vector(sample['embed_vector'] or [])}"
          f"   [embedder={embedder.name}]")
    args = sample["search_args"]
    print(f"  ③ search     → top_k={args['top_k']} model={args['model']!r} "
          f"min_score={args['min_score']} category={args['category']} "
          f"document_id={args['document_id']}")
    print(f"                 命中 {len(sample['matches'])} 条，"
          f"scores={[round(m.score, 4) for m in sample['matches']]}")
    print(f"  ④ 归一       → {len(sample['chunks'])} 条 KnowledgeChunk"
          f"（空正文已丢、重复正文已去）")
    for c in sample["chunks"]:
        print(f"  ⑤ chunk      → content({len(c.content)} 字) "
              f"source={c.source!r} "
              f"metadata={{score={c.metadata[SCORE_METADATA_KEY]:.4f}, "
              f"chunk_id={c.metadata.get('chunk_id')}, "
              f"document_id={c.metadata.get('document_id')}, "
              f"category={c.metadata.get('category')!r}, "
              f"embedding_model={c.metadata.get('embedding_model')!r}}}")

    print()
    print(f"  ── 全量 {len(rows)} 条 query 的链路计数 ──")
    print(f"  embed  调用次数 : {sum(r['embed_calls'] for r in rows)}"
          f"（= query 条数 {len(rows)}，无重复调用）")
    print(f"  search 调用次数 : {sum(r['search_calls'] for r in rows)}"
          f"（= query 条数 {len(rows)}）")
    print(f"  返回 chunk 总数 : {sum(len(r['chunks']) for r in rows)}")


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 74)
    print("KnowledgeRetriever 基础检索流程 · 自检")
    print(f"测试数据：{SET_PATH.relative_to(BACKEND_DIR.parent)}")
    print("链路：query → Embedding → VectorStore → VectorKnowledgeRetriever → KnowledgeChunk")
    print("=" * 74)

    data = json.loads(SET_PATH.read_text(encoding="utf-8"))
    docs = data["corpus"]["documents"]
    queries = data["queries"]

    check_contract()

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with factory() as db:
            embedder = default_embedder()
            row_by_id = await prepare_corpus(db, embedder, docs)
            rows = await run_flow(db, embedder, queries)
            check_chunk_structure(rows, row_by_id)
            await check_boundaries(db, embedder, rows)
            print_flow(rows, embedder)
    finally:
        await engine.dispose()

    print("\n" + "=" * 74)
    print("测试结果")
    print("=" * 74)
    print(f"  测试数据      : rag_query_set.json（corpus {len(docs)} 篇 / query {len(queries)} 条）")
    print(f"  Embedding     : {embedder.name} / {embedder.dimension} 维"
          f"（离线占位，**仅验证链路连通性，不代表语义检索效果**）")
    print(f"  链路          : query → Embedding → VectorStore → Retriever → KnowledgeChunk  全部连通")
    print(f"  断言          : 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
