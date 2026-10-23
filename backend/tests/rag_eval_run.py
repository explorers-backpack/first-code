# -*- coding: utf-8 -*-
"""RAG 端到端离线评估运行器（**不进回归循环**，手动/定期跑）。

用法::

    python backend/tests/rag_eval_run.py                 # 默认：质量 + 后端对比 + 规模曲线
    python backend/tests/rag_eval_run.py --skip-scale    # 只跑质量评估（快）
    python backend/tests/rag_eval_run.py --respect-env   # 用 .env 里配的真实 Embedding

产物：``backend/scripts/rag_eval_report.json`` + 控制台 markdown 表。

三条设计原则
------------
1. **不硬编码文档 / 查询 / 答案**。语料与标注全部来自既有文件：
   ``scripts/rag_query_set.json``（16 条带 ``expected_category`` /
   ``expected_keywords`` 的 query + 自带受控语料）与
   ``scripts/interview_knowledge.json``（22 篇真实风格知识文档，多段落）。
   金标准由 ``expected_keywords`` **运行时推导**：某篇正文包含该 query 的**全部**
   关键词 ⇒ 那篇是金标准文档。推导不唯一就直接报错退出，绝不猜。
2. **检索质量与生成质量分开评估**。[2]~[5] 只测检索（Recall / Precision / MRR /
   nDCG / 噪声注入率 / 延迟）；[6] 只测"检索结果是否真的进了 Prompt、来源是否可追"，
   用 Fake LLM 验证**管道**，不声称生成质量——生成质量由
   ``tests/test_interview_eval_benchmark.py``（真实模型，不进回归）负责。
3. **默认钉死离线环境**（``regression_env``）⇒ 零密钥、可复现、结果可比。
   要评估真实 Embedding 请显式传 ``--respect-env`` 并配好 ``EMBEDDING_*``。

为什么用 ``*_run.py`` 而不是 ``test_*.py``
------------------------------------------
它跑几十秒到几分钟、产出数据文件，不属于"每次提交都必须绿"的单元自检。
指标本身的正确性由 ``tests/test_rag_eval_metrics.py``（进回归）保证。
"""

import argparse
import asyncio
import json
import os
import pathlib
import random
import shutil
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

_BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND_DIR))
sys.path.insert(0, str(_BACKEND_DIR / "tests"))

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

# 默认钉死「离线 Embedding + 默认 RAG 参数」：保证跨机器、跨时间可比。
if "--respect-env" not in sys.argv:
    import regression_env  # noqa: E402,F401

import rag_eval_metrics as metrics  # noqa: E402

from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  注册全部模型
from database import Base  # noqa: E402
from models import KnowledgeChunk, KnowledgeDocument  # noqa: E402
from services.document_chunker import DocumentChunker  # noqa: E402
from services.knowledge_import_pipeline import KnowledgeImportPipeline  # noqa: E402
from services.knowledge_rag import (  # noqa: E402
    build_vector_retriever,
    build_vector_store,
    default_embedder,
)
from services.vector_store import VectorRecord  # noqa: E402

SCRIPTS = _BACKEND_DIR / "scripts"
QUERY_SET = SCRIPTS / "rag_query_set.json"
INTERVIEW_CORPUS = SCRIPTS / "interview_knowledge.json"
REPORT_PATH = SCRIPTS / "rag_eval_report.json"

#: 生产默认检索参数（与 ``VectorKnowledgeRetriever`` 的默认值一致）。
PRODUCTION_DEFAULTS = {"top_k": 5, "min_score": None, "min_score_ratio": None}


# ============================================================
# 基础设施
# ============================================================
def _say(message: str = "") -> None:
    print(message, flush=True)


def _section(title: str) -> None:
    _say("\n" + "-" * 74)
    _say(title)
    _say("-" * 74)


def _table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
    """打印 markdown 表（列宽自适应，中文按 2 宽度算）。"""
    def width(text: Any) -> int:
        value = "" if text is None else str(text)
        return sum(2 if ord(ch) > 0x2E80 else 1 for ch in value)

    cells = [[("" if value is None else str(value)) for value in row] for row in rows]
    widths = [max(width(header), *(width(row[i]) for row in cells)) if cells else width(header)
              for i, header in enumerate(headers)]

    def pad(text: str, target: int) -> str:
        return text + " " * max(0, target - width(text))

    _say("| " + " | ".join(pad(str(header), widths[i]) for i, header in enumerate(headers)) + " |")
    _say("|" + "|".join("-" * (widths[i] + 2) for i in range(len(headers))) + "|")
    for row in cells:
        _say("| " + " | ".join(pad(row[i], widths[i]) for i in range(len(headers))) + " |")


async def _make_session() -> Tuple[Any, AsyncSession]:
    """内存 SQLite（同一引擎共享同一个库）+ 建表。"""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return engine, factory()


def _load_json(path: pathlib.Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


# ============================================================
# [1] 语料与切片统计
# ============================================================
def _chunk_stats(texts: Sequence[str]) -> Dict[str, Any]:
    """切片统计：条数、长度分布、**重复率**、**重叠率**。

    重复率按"去掉所有空白后完全相同"计算（与检索器的 ``dedup`` 同一口径）。
    重叠率用相邻切片的**最长公共后缀/前缀**长度除以平均长度——近似但可解释：
    真实重叠是"后一片开头 = 前一片结尾"，用后缀/前缀重合度度量它。
    """
    if not texts:
        return {"count": 0}
    lengths = [len(text) for text in texts]
    normalized = ["".join(text.split()) for text in texts]
    unique = len(set(normalized))
    overlaps: List[int] = []
    for left, right in zip(texts, texts[1:]):
        limit = min(len(left), len(right), 200)
        best = 0
        for size in range(limit, 0, -1):
            if left[-size:] == right[:size]:
                best = size
                break
        overlaps.append(best)
    return {
        "count": len(texts),
        "unique": unique,
        "duplicate_rate": round(1 - unique / len(texts), 4),
        "avg_len": round(sum(lengths) / len(lengths), 1),
        "min_len": min(lengths),
        "max_len": max(lengths),
        "avg_overlap": round(sum(overlaps) / len(overlaps), 1) if overlaps else 0.0,
        "max_overlap": max(overlaps) if overlaps else 0,
    }


def _chunk_corpus(documents: Sequence[Dict[str, Any]]) -> List[str]:
    chunker = DocumentChunker()
    texts: List[str] = []
    for doc in documents:
        for chunk in chunker.split(
            doc["content"], document_id=0, category=doc["category"], source=doc.get("source", "")
        ):
            texts.append(chunk["content"])
    return texts


# ============================================================
# [2] 检索质量
# ============================================================
async def _import_corpus(session: AsyncSession, documents: Sequence[Dict[str, Any]],
                         *, store: Any = None, embedder: Any = None) -> List[int]:
    """用**真实写链路**（KnowledgeImportPipeline）导入，返回 document_id 列表。"""
    pipeline = KnowledgeImportPipeline(session, store=store, embedder=embedder)
    ids: List[int] = []
    for doc in documents:
        report = await pipeline.import_document(dict(doc))
        if report.get("status") == "failed":
            raise RuntimeError(f"导入失败：{doc.get('title')} → {report.get('error')}")
        ids.append(report["document_id"])
    return ids


async def _gold_document_ids(session: AsyncSession,
                            corpus: Sequence[Dict[str, Any]],
                            queries: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    """按 ``expected_keywords`` 在语料里推导金标准文档（**不唯一就报错**）。

    推导规则（与 ``rag_query_set.json`` 的 ``expectation_contract`` 同源）：
    正文包含该 query **全部** ``expected_keywords`` 的文档就是金标准。
    命中 0 篇或多篇 ⇒ 直接抛错，避免"金标准悄悄错了，指标却看起来很正常"。
    """
    rows = (await session.execute(select(KnowledgeDocument))).scalars().all()
    by_title = {row.title: row.id for row in rows}
    gold: Dict[str, int] = {}
    for item in queries:
        keywords = list(item["expected_keywords"])
        hits = [
            doc["title"] for doc in corpus
            if all(keyword in doc["content"] for keyword in keywords)
        ]
        if len(hits) != 1:
            raise RuntimeError(
                f"金标准推导失败：query={item['query']!r} 命中 {len(hits)} 篇 {hits}；"
                "评测集与语料不一致，拒绝继续（不猜）"
            )
        gold[item["query"]] = by_title[hits[0]]
    return gold


async def _chunk_ids_of(session: AsyncSession, document_id: int) -> List[int]:
    rows = (await session.execute(
        select(KnowledgeChunk.id).where(KnowledgeChunk.document_id == document_id)
    )).scalars().all()
    return [int(value) for value in rows]


async def _eval_config(session: AsyncSession, *, backend: str, top_k: int,
                       min_score: Optional[float], min_score_ratio: Optional[float],
                       queries: Sequence[Dict[str, Any]], gold_docs: Dict[str, int],
                       gold_chunks: Dict[str, List[int]], repeat: int,
                       chroma_path: Optional[str] = None,
                       store: Any = None) -> Dict[str, Any]:
    """跑一组参数，返回指标 + 延迟 + 每条 query 的明细。

    ``store`` 可复用**同一个**后端实例（保证 SQL / Chroma 比的是同一份数据）。
    直接构造 ``VectorKnowledgeRetriever`` 而不是走 ``build_vector_retriever``：
    后者固定按环境变量选后端，无法在一次运行里对比两个后端；
    生产装配本身由 [5] 的等价性断言覆盖。
    """
    from services.vector_knowledge_retriever import VectorKnowledgeRetriever

    embedder = default_embedder()
    if store is None:
        store = _make_store(session, backend, chroma_path)
    retriever = VectorKnowledgeRetriever(
        embedder, store,
        top_k=top_k, min_score=min_score, min_score_ratio=min_score_ratio,
    )

    recalls, precisions, rrs, ndcgs, noises = [], [], [], [], []
    latencies: List[float] = []
    top1_scores: List[float] = []
    empty_queries: List[str] = []
    details: List[Dict[str, Any]] = []

    for item in queries:
        query = item["query"]
        relevant = gold_chunks[query]
        # 延迟：跑 repeat 次取分位数（第 1 次作为"冷"结果，全部计入）
        elapsed = 0.0
        chunks: List[Any] = []
        for _ in range(max(1, repeat)):
            started = time.perf_counter()
            chunks = await retriever.retrieve(None, query, None)
            elapsed = (time.perf_counter() - started) * 1000.0
            latencies.append(elapsed)
        retrieved = [int(chunk.metadata["chunk_id"]) for chunk in chunks
                     if chunk.metadata.get("chunk_id") is not None]
        if not retrieved:
            empty_queries.append(query)
        if chunks:
            top1_scores.append(float(chunks[0].metadata.get("score", 0.0)))

        recalls.append(metrics.recall_at_k(retrieved, relevant, top_k))
        precisions.append(metrics.precision_at_k(retrieved, relevant, top_k))
        rrs.append(metrics.reciprocal_rank(retrieved, relevant))
        ndcgs.append(metrics.ndcg_at_k(retrieved, relevant, top_k))
        noises.append(metrics.noise_injection_rate(retrieved, relevant, top_k))
        details.append({
            "query": query,
            "returned": len(retrieved),
            "hit_gold": bool(set(retrieved) & set(relevant)),
            "top1_score": round(top1_scores[-1], 4) if chunks else None,
            "top1_is_gold": bool(retrieved and retrieved[0] in set(relevant)),
        })

    return {
        "backend": backend,
        "top_k": top_k,
        "min_score": min_score,
        "min_score_ratio": min_score_ratio,
        "recall": metrics.summarize(recalls),
        "precision": metrics.summarize(precisions),
        "mrr": metrics.summarize(rrs),
        "ndcg": metrics.summarize(ndcgs),
        "noise": metrics.summarize(noises),
        "latency_ms": {
            "p50": round(metrics.percentile(latencies, 0.5) or 0.0, 2),
            "p95": round(metrics.percentile(latencies, 0.95) or 0.0, 2),
            "max": round(max(latencies), 2) if latencies else None,
            "samples": len(latencies),
        },
        "top1_score_mean": round(sum(top1_scores) / len(top1_scores), 4) if top1_scores else None,
        "top1_score_min": round(min(top1_scores), 4) if top1_scores else None,
        "empty_queries": empty_queries,
        "details": details,
    }


def _make_store(session: AsyncSession, backend: str, chroma_path: Optional[str]) -> Any:
    if backend == "chroma":
        from services.vector_store_chroma import ChromaVectorStore

        return ChromaVectorStore(
            session,
            path=chroma_path,
            collection_name="rag_eval_%d" % (int(time.time() * 1000) % 100000),
        )
    return build_vector_store(session, backend="sql")


# ============================================================
# [3] 规模 - 延迟曲线
# ============================================================
async def _bulk_fill(session: AsyncSession, store: Any, embedder: Any,
                     base_chunks: Sequence[str], target: int, tag: str) -> int:
    """用**真实 store.add 路径**批量灌到 ``target`` 条切片，返回实际条数。

    为什么不用 import pipeline：那是逐片 commit（真实入库语义，但对"只测检索延迟"
    太慢）。这里仍然走 ``store.add``（SQL 落库 / chroma 镜像都是真的），
    只是把「切片」这一步用既有切片器的产物复制出来。
    """
    document = KnowledgeDocument(
        title=f"scale-{tag}", content="(bulk)", category="technical", source=f"scale://{tag}",
    )
    session.add(document)
    await session.flush()
    document_id = document.id

    written = 0
    batch: List[VectorRecord] = []
    index = 0
    while written < target:
        text = base_chunks[index % len(base_chunks)] + f"\n\n[{tag}-{index}]"
        vector = await embedder.embed(text)
        batch.append(VectorRecord(
            vector=vector,
            content=text,
            document_id=document_id,
            chunk_id=None,
            metadata={"document_id": document_id, "category": "technical",
                      "source": f"scale://{tag}", "chunk_index": index},
            model=getattr(embedder, "name", ""),
        ))
        index += 1
        if len(batch) >= 100 or written + len(batch) >= target:
            written += await store.add(batch)
            batch = []
    return written


class _RandomEmbedder:
    """**合成向量**（不联网、不读语料）：只用来放大**维度**，测后端成本。

    为什么需要它：向量后端的成本主要由「行数 × 维度」决定，而**与产生向量的
    模型无关**。生产用的是 2560 维的讯飞 Embedding，离线套件用的是 256 维哈希
    占位——只测 256 维会**系统性低估**全表扫描的代价（余弦计算量正比于维度）。
    这里用同分布的随机向量把维度拉到生产值，**专门测后端**，不测检索质量。
    """

    def __init__(self, dimension: int, name: str) -> None:
        self.dimension = dimension
        self.name = name
        self._rng = random.Random(20251022)  # 固定种子 ⇒ 可复现

    async def embed(self, text: str) -> List[float]:
        return [self._rng.random() for _ in range(self.dimension)]


async def _scale_curve(base_chunks: Sequence[str], sizes: Sequence[int],
                       repeat: int, dims: Sequence[Optional[int]] = (None,),
                       ) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for dim in dims:
        # dim=None ⇒ 用**真实** Embedder（与检索质量同一套）；否则合成向量
        embedder = default_embedder() if dim is None else _RandomEmbedder(dim, "synthetic-%d" % dim)
        model_name = getattr(embedder, "name", "")
        query_vector = await embedder.embed("Redis 持久化 RDB AOF 取舍")
        for backend in ("sql", "chroma"):
            chroma_path = tempfile.mkdtemp(prefix="rag_eval_chroma_") if backend == "chroma" else None
            try:
                for size in sizes:
                    engine, session = await _make_session()
                    try:
                        store = _make_store(session, backend, chroma_path)
                        written = await _bulk_fill(session, store, embedder,
                                                   base_chunks, size,
                                                   "%s%s%d" % (backend, dim or "real", size))
                        latencies = []
                        for _ in range(repeat):
                            started = time.perf_counter()
                            await store.search(query_vector, top_k=5, model=model_name)
                            latencies.append((time.perf_counter() - started) * 1000.0)
                        rows.append({
                            "backend": backend,
                            "dim": dim if dim is not None else getattr(embedder, "dimension", 0),
                            "chunks": written,
                            "p50_ms": round(metrics.percentile(latencies, 0.5) or 0.0, 2),
                            "p95_ms": round(metrics.percentile(latencies, 0.95) or 0.0, 2),
                            "max_ms": round(max(latencies), 2),
                            "samples": len(latencies),
                        })
                    finally:
                        await session.close()
                        await engine.dispose()
            finally:
                if chroma_path:
                    shutil.rmtree(chroma_path, ignore_errors=True)
    return rows


# ============================================================
# [6] 检索 → Prompt 注入（管道连通性 + 来源可追）
# ============================================================
async def _prompt_injection_check(session: AsyncSession, queries: Sequence[Dict[str, Any]],
                                  gold_docs: Dict[str, int]) -> Dict[str, Any]:
    """验证"检索结果真的进了 Prompt，且来源可追"。

    **不评估生成质量**（那需要真实模型）：这里只用真实
    ``interview_agent.render_question_prompt``（冻结的纯函数）+ Fake LLM，
    确认 (a) 检索片段进入 Prompt；(b) 每条片段都带 ``（来源：…）``；
    (c) Fake LLM 能收到 Prompt 并产出可解析的题目 ⇒ 管道是通的。
    """
    from services import interview_agent, interview_planner
    from modules.ai import CallPolicy, FakeLlmProvider, build_legacy_spark_facade

    job = {"job_name": "Java 后端开发工程师", "skills": "Java,MySQL,Redis,Kafka",
           "duty": "负责订单系统后端开发"}
    resume = "技能特长\nJava、Spring Boot、Redis、Kafka\n"
    plan = interview_planner.build_plan(
        interview_type="technical", difficulty="mid", duration=30, job=job, resume=resume,
    )
    context = {
        "session_id": 1, "current_question_no": 1, "current_stage": "technical",
        "asked_questions": [], "covered_topics": [], "weak_topics": [],
        "follow_up_count": 0, "total_questions": 10, "max_questions": 10,
    }
    question_json = (
        '{"question": "请说明 Redis 持久化中 RDB 与 AOF 的取舍。", "question_type": "technical",'
        ' "topic": "Redis 持久化", "difficulty": "mid",'
        ' "expected_points": ["RDB 快照", "AOF 追加"], "reason": "补强薄弱知识点"}'
    )

    retriever = build_vector_retriever(session, top_k=3)
    samples: List[Dict[str, Any]] = []
    for item in queries[:5]:
        chunks = await retriever.retrieve(None, item["query"], None)
        variables = interview_agent.build_question_variables(context, plan, resume, job)
        # ★ render_question_prompt 返回 (模板名, Prompt 正文) —— 顺序反了会把 18 字符的
        #   模板名当成 Prompt，于是「来源可追」恒为 False（假红）。这里必须按契约解包。
        template, prompt = interview_agent.render_question_prompt(variables, chunks)
        sources = [chunk.source for chunk in chunks if chunk.source]
        fake = FakeLlmProvider([question_json])
        facade = build_legacy_spark_facade(provider_getter=lambda: fake,
                                           policy=CallPolicy(max_attempts=1))
        result = await interview_agent.generate_question(
            context, plan, resume, job, spark=facade, knowledge_context=chunks,
        )
        samples.append({
            "query": item["query"],
            "chunks": len(chunks),
            "template": template,
            "prompt_chars": len(prompt),
            "source_marks": prompt.count("（来源："),
            "distinct_sources": len(set(sources)),
            "sources_in_prompt": all(source in prompt for source in sources) if sources else False,
            "question_ok": bool(result.get("ok")),
            "question_has_evidence_points": bool(result.get("expected_points")),
        })
    return {
        "samples": samples,
        "all_sources_traceable": all(sample["sources_in_prompt"] for sample in samples),
        "all_questions_ok": all(sample["question_ok"] for sample in samples),
        "avg_chunks": round(sum(s["chunks"] for s in samples) / len(samples), 2) if samples else 0,
        "avg_prompt_chars": round(sum(s["prompt_chars"] for s in samples) / len(samples), 1)
        if samples else 0,
    }


# ============================================================
# 主流程
# ============================================================
async def run(args: argparse.Namespace) -> int:
    report: Dict[str, Any] = {"generated_by": "backend/tests/rag_eval_run.py",
                              "respect_env": bool(args.respect_env)}
    query_set = _load_json(QUERY_SET)
    corpus = query_set["corpus"]["documents"]
    queries = query_set["queries"]
    interview_docs = _load_json(INTERVIEW_CORPUS)["documents"]

    _section("[1] 语料与切片统计")
    chunk_a = _chunk_corpus(corpus)
    chunk_b = _chunk_corpus(interview_docs)
    stats = {"eval_corpus": _chunk_stats(chunk_a), "interview_corpus": _chunk_stats(chunk_b)}
    report["chunk_stats"] = stats
    _table(
        ["语料", "文档", "切片", "去重后", "重复率", "平均长度", "平均重叠", "最大重叠"],
        [
            ["rag_query_set（受控）", len(corpus), stats["eval_corpus"]["count"],
             stats["eval_corpus"]["unique"], stats["eval_corpus"]["duplicate_rate"],
             stats["eval_corpus"]["avg_len"], stats["eval_corpus"]["avg_overlap"],
             stats["eval_corpus"]["max_overlap"]],
            ["interview_knowledge（真实风格）", len(interview_docs),
             stats["interview_corpus"]["count"], stats["interview_corpus"]["unique"],
             stats["interview_corpus"]["duplicate_rate"], stats["interview_corpus"]["avg_len"],
             stats["interview_corpus"]["avg_overlap"], stats["interview_corpus"]["max_overlap"]],
        ],
    )
    embedder = default_embedder()
    from services.embedding_service import describe_embedding

    info = describe_embedding(embedder)
    _say(f"Embedding: provider={info.provider} dim={info.dimension} "
         f"semantic_enabled={info.semantic_enabled} name={getattr(embedder, 'name', '')}")
    report["embedding"] = {"provider": info.provider, "dimension": info.dimension,
                           "semantic_enabled": info.semantic_enabled,
                           "name": getattr(embedder, "name", "")}

    engine, session = await _make_session()
    chroma_path = tempfile.mkdtemp(prefix="rag_eval_chroma_") if not args.skip_chroma else None
    chroma_store = _make_store(session, "chroma", chroma_path) if chroma_path else None
    try:
        # ★ 用 chroma store 导入：它会把权威行写进 SQL、并**镜像**进 ANN 索引，
        #   于是一次导入后两个后端都能检索（否则 ANN 集合是空的，见 [3b]）。
        doc_ids = await _import_corpus(session, corpus, store=chroma_store)
        gold_docs = await _gold_document_ids(session, corpus, queries)
        gold_chunks = {query: await _chunk_ids_of(session, doc) for query, doc in gold_docs.items()}
        _say(f"已导入 {len(doc_ids)} 篇文档；金标准推导成功（{len(gold_chunks)} 条 query）")
        report["corpus"] = {"documents": len(doc_ids), "queries": len(queries)}

        # ---- [2] 参数网格 ----
        _section("[2] 检索质量：参数网格（Top-K / 绝对阈值 / 相对阈值）")
        grid: List[Dict[str, Any]] = []
        for top_k in (1, 2, 3, 5, 8, 10):
            grid.append({"top_k": top_k, "min_score": None, "min_score_ratio": None})
        for min_score in (0.05, 0.10, 0.15, 0.20, 0.25, 0.30):
            grid.append({"top_k": 5, "min_score": min_score, "min_score_ratio": None})
        for ratio in (0.7, 0.8, 0.9, 0.95, 0.99):
            grid.append({"top_k": 5, "min_score": None, "min_score_ratio": ratio})

        results: List[Dict[str, Any]] = []
        for config in grid:
            result = await _eval_config(
                session, backend="sql", repeat=args.repeat, queries=queries,
                gold_docs=gold_docs, gold_chunks=gold_chunks, **config,
            )
            results.append(result)
            _say(f"  top_k={config['top_k']} min_score={config['min_score']} "
                 f"ratio={config['min_score_ratio']} → "
                 f"R@K={result['recall']['mean']} MRR={result['mrr']['mean']} "
                 f"噪声={result['noise']['mean']} 空结果={len(result['empty_queries'])}")

        _table(
            ["top_k", "min_score", "ratio", "Recall@K", "Precision", "MRR", "nDCG", "噪声率", "空结果", "p95(ms)"],
            [[r["top_k"], r["min_score"], r["min_score_ratio"], r["recall"]["mean"],
              r["precision"]["mean"], r["mrr"]["mean"], r["ndcg"]["mean"], r["noise"]["mean"],
              len(r["empty_queries"]), r["latency_ms"]["p95"]] for r in results],
        )
        report["grid"] = results

        # ---- [3] 后端对比 ----
        _section("[3] 后端对比：SqlAlchemy 全表扫描 vs Chroma HNSW（同一语料、同一参数）")
        baseline = await _eval_config(
            session, backend="sql", repeat=args.repeat, queries=queries,
            gold_docs=gold_docs, gold_chunks=gold_chunks, **PRODUCTION_DEFAULTS,
        )
        backend_rows = [baseline]
        if not args.skip_chroma:
            backend_rows.append(await _eval_config(
                session, backend="chroma", repeat=args.repeat, queries=queries,
                gold_docs=gold_docs, gold_chunks=gold_chunks, chroma_path=chroma_path,
                store=chroma_store, **PRODUCTION_DEFAULTS,
            ))
        _table(
            ["后端", "Recall@K", "MRR", "nDCG", "噪声率", "p50(ms)", "p95(ms)", "最大(ms)"],
            [[r["backend"], r["recall"]["mean"], r["mrr"]["mean"], r["ndcg"]["mean"],
              r["noise"]["mean"], r["latency_ms"]["p50"], r["latency_ms"]["p95"],
              r["latency_ms"]["max"]] for r in backend_rows],
        )
        if len(backend_rows) == 2:
            same = [d for d in backend_rows[0]["details"]]
            other = [d for d in backend_rows[1]["details"]]
            identical = all(
                left["query"] == right["query"] and left["returned"] == right["returned"]
                and left["top1_is_gold"] == right["top1_is_gold"]
                for left, right in zip(same, other)
            )
            _say(f"两后端 top-1/返回条数逐条一致：{identical}"
                 "（一致 = ANN 只是加速，没有改变排序语义；不一致 = 索引参数需要调）")
            report["backend_parity"] = identical
        report["backends"] = backend_rows

        # ---- [4] 规模 × 维度 - 延迟 ----
        if not args.skip_scale:
            _section("[4] 规模 × 维度 - 延迟曲线（同一台机器，单次查询，重复 %d 次）" % args.repeat)
            sizes = [int(value) for value in args.sizes.split(",") if value.strip()]
            dims: List[Optional[int]] = []
            for token in args.dims.split(","):
                token = token.strip()
                if not token:
                    continue
                dims.append(None if token.lower() == "real" else int(token))
            curve = await _scale_curve(chunk_b, sizes, args.repeat, dims)
            _table(
                ["后端", "维度", "切片数", "p50(ms)", "p95(ms)", "最大(ms)", "样本"],
                [[row["backend"], row["dim"], row["chunks"], row["p50_ms"],
                  row["p95_ms"], row["max_ms"], row["samples"]] for row in curve],
            )
            _say("说明：维度非 0 的行里，向量是**合成随机向量**（不联网）——只测后端成本，"
                 "不测检索质量；成本正比于「行数 × 维度」，生产是 2560 维。")
            report["scale"] = curve

        # ---- [5] 生产默认参数 ----
        _section("[5] 生产默认参数（top_k=5, 无阈值）的基线")
        prod = backend_rows[0]
        _say(f"Recall@K={prod['recall']['mean']}  MRR={prod['mrr']['mean']}  "
             f"nDCG={prod['ndcg']['mean']}  噪声率={prod['noise']['mean']}  "
             f"p95={prod['latency_ms']['p95']}ms  空结果={len(prod['empty_queries'])} 条")
        _say(f"top-1 分数：均值 {prod['top1_score_mean']}  最低 {prod['top1_score_min']}")
        report["production_default"] = {
            "recall": prod["recall"], "mrr": prod["mrr"], "ndcg": prod["ndcg"],
            "noise": prod["noise"], "latency_ms": prod["latency_ms"],
            "top1_score_mean": prod["top1_score_mean"], "top1_score_min": prod["top1_score_min"],
            "empty_queries": prod["empty_queries"],
        }

        # ---- [6] 检索 → Prompt → 出题 ----
        _section("[6] 检索 → Prompt 注入 → 出题（Fake LLM；只验管道，不评生成质量）")
        injection = await _prompt_injection_check(session, queries, gold_docs)
        _table(
            ["query", "模板", "切片", "Prompt 字符", "来源标记", "来源可追", "出题 ok"],
            [[sample["query"][:28], sample["template"], sample["chunks"],
              sample["prompt_chars"], sample["source_marks"],
              sample["sources_in_prompt"], sample["question_ok"]]
             for sample in injection["samples"]],
        )
        _say(f"来源全部可追：{injection['all_sources_traceable']}；"
             f"出题全部成功：{injection['all_questions_ok']}；"
             f"平均注入 {injection['avg_chunks']} 片 / {injection['avg_prompt_chars']} 字符")
        report["prompt_injection"] = injection
    finally:
        await session.close()
        await engine.dispose()
        if chroma_path:
            shutil.rmtree(chroma_path, ignore_errors=True)

    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    _section("产物")
    _say(f"已写入 {REPORT_PATH.relative_to(_BACKEND_DIR.parent)}")
    return 0


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RAG 端到端离线评估")
    parser.add_argument("--repeat", type=int, default=5, help="每条 query 重复次数（延迟分位）")
    parser.add_argument("--sizes", default="200,1000,3000", help="规模曲线的切片数（逗号分隔）")
    parser.add_argument("--dims", default="real,2560",
                        help="规模曲线的向量维度（逗号分隔；real = 用真实 Embedder，"
                             "数字 = 合成随机向量。生产是 2560 维）")
    parser.add_argument("--skip-scale", action="store_true", help="跳过规模-延迟曲线")
    parser.add_argument("--skip-chroma", action="store_true", help="跳过 Chroma 后端对比")
    parser.add_argument("--respect-env", action="store_true",
                        help="不钉扎离线环境（用 .env 里配置的真实 Embedding）")
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(asyncio.run(run(_parse_args(sys.argv[1:]))))
