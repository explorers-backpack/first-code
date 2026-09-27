# -*- coding: utf-8 -*-
"""知识库语料导入 CLI：把 ``interview_knowledge.json`` 灌进 RAG 知识库（真实 Embedding）。

用法
----
.. code-block:: bash

    # 1) 先看计划（不编码、不写库、不花配额）
    python scripts/import_knowledge.py --provider spark --dry-run

    # 2) 真跑（必须显式 --yes，避免手滑把生产库写脏）
    python scripts/import_knowledge.py --provider spark --yes --pace 3

    # 3) 只重跑上一次失败的文档（已完成的文档本来就会零调用跳过）
    python scripts/import_knowledge.py --provider spark --yes \\
        --only-failed scripts/knowledge_import_stats.json

    # 4) 只导某一类 / 先小样本试跑
    python scripts/import_knowledge.py --provider spark --yes --category technical
    python scripts/import_knowledge.py --provider spark --yes --limit 3

执行流程（严格对齐需求）
------------------------
.. code-block:: text

    JSON 语料
      └─ 解析（校验 title / content / category / source 四项契约）
           └─ document 落库（knowledge_document；自然键 (title, source, category) 幂等）
                └─ chunk 切片（**DocumentChunker 默认 500 / 80**，与既有结构一致）
                     └─ **真实 Embedding**（逐片编码，role=document ⇒ 讯飞 domain=para）
                          └─ 写 knowledge_chunk（content + metadata + 向量三列）
                               └─ 同步 VectorStore（store.add ⇒ sql 即权威行；chroma 另镜像 ANN）

**为什么走 ``KnowledgeImportPipeline`` 而不是自己写一遍**：这条链（落库 → 切片 →
编码 → 写向量库）就是该 Pipeline 的职责，它的**三层幂等**与**失败报告**正是本任务
「重复执行幂等」与「异常处理」两条要求的现成实现。本脚本只负责
「读语料 + 节流/重试 + 统计」，**不重写任何业务逻辑**。

幂等（支持重复执行）
--------------------
三层幂等（由 Pipeline 提供）：① 文档按自然键复用；② 切片按 ``(document_id,
chunk_index)`` upsert；③ 向量已存在**且同模型、正文未变**才跳过。
⇒ **重跑同一份语料是安全的**：已完成的文档 ``status="skipped"`` 且**一次上游调用都不发生**，
只有「没做完的」会被继续做。因此**断点续跑不需要特殊命令，直接重跑即可**。

安全闸：拒绝用「离线占位」导入
------------------------------
不配 ``EMBEDDING_*`` 时 ``default_embedder()`` 返回 ``HashEmbeddingService``
（``semantic_enabled=False``，**无语义**）。需求明确要求**不使用 HashEmbedding**，
因此脚本开工前校验 ``semantic_enabled``，为假直接拒绝（``--allow-offline`` 可越，
仅建议测试库）。

限流
----
讯飞 Embedding 上游有 license 限流（实测连续快速调用返回 ``code=11202 licc failed``）。
``--pace`` 控制**每条编码之间**的间隔（默认 3 秒，实测稳定），``--retries`` /
``--retry-delay`` 控制单条失败后的重试。三者都由脚本内的 ``PacedEmbedder``
包装实现——**不改 Pipeline、不改 Embedding 实现**。

统计（需求点名四项）
--------------------
落盘到 ``scripts/knowledge_import_stats.json``（**不含数据库口令**），含：

- **文档数量**：``knowledge_document`` 总数 + 本次新建 / 复用
- **chunk 数量**：``knowledge_chunk`` 总数 + 本次新建 / 已向量化
- **embedding 维度**：``embedding_dim`` 去重取值（并断言与目标模型声明维度一致）
- **VectorStore 数量**：``store.count()``（已向量化条数）+ 后端名
  以及「向量模型分布」（必须只有目标模型，不能混进 ``hash-local``）

退出码：``0`` = 无失败；``1`` = 有文档导入失败；``2`` = 参数问题；``3`` = 安全闸拦下。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit, urlunsplit

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from sqlalchemy import func, select  # noqa: E402

from database import DATABASE_URL, async_session, engine  # noqa: E402
from models import (  # noqa: E402
    KNOWLEDGE_CATEGORIES,
    KnowledgeChunk,
    KnowledgeDocument,
)
from services.document_chunker import (  # noqa: E402
    DEFAULT_CHUNK_OVERLAP,
    DEFAULT_CHUNK_SIZE,
    DocumentChunker,
)
from services.embedding_provider import (  # noqa: E402
    ENV_PROVIDER,
    ROLE_DOCUMENT,
    describe_embedding,
)
from services.embedding_service import EmbeddingService  # noqa: E402
from services.knowledge_import_pipeline import KnowledgeImportPipeline  # noqa: E402
from services.knowledge_rag import build_vector_store, default_embedder  # noqa: E402

#: 默认语料（需求指定的数据源）。
DEFAULT_CORPUS = os.path.join(BACKEND_DIR, "scripts", "interview_knowledge.json")

#: 统计文件默认落点（与既有 rag_*.json 同目录）。
DEFAULT_STATS_PATH = os.path.join(BACKEND_DIR, "scripts", "knowledge_import_stats.json")

EXIT_OK = 0
EXIT_HAS_FAILURES = 1
EXIT_USAGE = 2
EXIT_SAFETY = 3


# ============================================================
# 小工具
# ============================================================
def _redact(url: str) -> str:
    """抹掉连接串里的口令（统计文件会进版本库，**绝不能带凭据**）。"""
    try:
        parts = urlsplit(url)
    except Exception:  # noqa: BLE001
        return "<unparsable>"
    if not parts.password:
        return url
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    return urlunsplit(parts._replace(netloc=f"{parts.username}:***@{host}"))


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def load_corpus(path: str) -> List[Dict[str, str]]:
    """读语料并**校验入参契约**（非法直接抛 ``ValueError``，不带着坏数据往下跑）。

    校验口径与 ``knowledge_document_service.create_document`` 一致：
    ``title`` / ``content`` 非空、``category`` ∈ ``KNOWLEDGE_CATEGORIES``；
    ``source`` 可为空（自由文本来源标识）。
    """
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} 顶层必须是对象（含 documents 数组）")
    documents = payload.get("documents")
    if not isinstance(documents, list) or not documents:
        raise ValueError(f"{path} 里 documents 必须是非空数组")

    cleaned: List[Dict[str, str]] = []
    for index, item in enumerate(documents):
        if not isinstance(item, dict):
            raise ValueError(f"documents[{index}] 不是对象")
        title = str(item.get("title") or "").strip()
        content = item.get("content")
        category = str(item.get("category") or "").strip()
        source = str(item.get("source") or "").strip()
        if not title:
            raise ValueError(f"documents[{index}] 缺 title")
        if not isinstance(content, str) or not content.strip():
            raise ValueError(f"documents[{index}]（{title}）缺 content")
        if category not in KNOWLEDGE_CATEGORIES:
            raise ValueError(
                f"documents[{index}]（{title}）category={category!r} 非法；"
                f"可选 {list(KNOWLEDGE_CATEGORIES)}"
            )
        cleaned.append({
            "title": title, "content": content,
            "category": category, "source": source,
        })

    # 自然键 (title, source, category) 必须唯一，否则「第 2 篇」会被静默当成
    # 「复用第 1 篇」——那是最难查的一类问题（导入了但看不见）。
    keys = [(d["title"], d["source"], d["category"]) for d in cleaned]
    if len(set(keys)) != len(keys):
        seen: set = set()
        dupes = [k for k in keys if k in seen or seen.add(k)]
        raise ValueError(f"语料存在重复自然键 (title, source, category)：{dupes}")
    return cleaned


def _load_failed_sources(path: str) -> List[str]:
    """从上次统计文件里取出失败文档的 ``source``（``--only-failed`` 用）。"""
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    failed = payload.get("failed_sources") if isinstance(payload, dict) else None
    if failed is None:
        raise ValueError(f"{path} 里没有 failed_sources 段（不是本脚本产出的统计文件？）")
    return [str(item) for item in failed]


# ============================================================
# 节流 / 重试包装（不改 Pipeline、不改 Embedding 实现）
# ============================================================
class PacedEmbedder(EmbeddingService):
    """包住真实 embedder：**逐条间隔 + 失败重试 + 调用计数**，其余行为原样透传。

    为什么需要它：上游有 license 限流（实测 ``code=11202``），而
    ``KnowledgeImportPipeline`` 是**逐片**编码的（为了失败可定位）。
    把节流放在这里，就**不用改 Pipeline**（它的「先编码后落库 + 失败可定位」
    语义原样保留），也不用改任何 Embedding 实现。

    ``name`` / ``dimension`` / ``semantic_enabled`` 必须透传——
    Pipeline 用 ``embedder.name`` 写 ``embedding_model`` 并据此判断
    「这一片的旧向量是否可比」（幂等判据的第三个条件）。
    """

    def __init__(
        self,
        inner: EmbeddingService,
        *,
        pace_seconds: float = 0.0,
        retries: int = 0,
        retry_delay: float = 1.0,
        verbose: bool = True,
    ) -> None:
        self.inner = inner
        self.name = inner.name
        self.dimension = inner.dimension
        self.semantic_enabled = getattr(inner, "semantic_enabled", False)
        self.pace_seconds = float(pace_seconds)
        self.retries = int(retries)
        self.retry_delay = float(retry_delay)
        self.verbose = verbose
        self.calls: List[float] = []   # 每次**尝试**的耗时（含失败的那几次）
        self.retried = 0

    @property
    def domain(self) -> str:
        return getattr(self.inner, "domain", "")

    async def _embed_one(self, text: str) -> List[float]:
        for attempt in range(self.retries + 1):
            started = time.perf_counter()
            try:
                vector = await self.inner.embed(text)
            except Exception as exc:  # noqa: BLE001 - 重试耗尽后原样抛出，由 Pipeline 记状态
                self.calls.append(time.perf_counter() - started)
                if attempt >= self.retries:
                    raise
                self.retried += 1
                if self.verbose:
                    print(f"      [retry {attempt + 1}/{self.retries}] {str(exc)[:100]}")
                if self.retry_delay:
                    await asyncio.sleep(self.retry_delay)
                continue
            self.calls.append(time.perf_counter() - started)
            # 限流间隔放在**成功之后**：失败路径已有 retry_delay 退避，不必叠加
            if self.pace_seconds:
                await asyncio.sleep(self.pace_seconds)
            return vector
        raise AssertionError("unreachable")  # pragma: no cover - 循环必然 return/raise

    async def embed(self, text: str) -> List[float]:  # type: ignore[override]
        return await self._embed_one(text)

    async def embed_batch(self, texts):  # type: ignore[override]
        return [await self._embed_one(t) for t in texts]


# ============================================================
# 库内现状统计
# ============================================================
async def _db_stats(db: Any) -> Dict[str, Any]:
    """文档数 / 切片数 / 已向量化数 / 维度与模型分布。"""
    documents = (await db.execute(
        select(func.count()).select_from(KnowledgeDocument)
    )).scalar_one()
    chunks = (await db.execute(
        select(func.count()).select_from(KnowledgeChunk)
    )).scalar_one()
    vectorized = (await db.execute(
        select(func.count()).select_from(KnowledgeChunk)
        .where(KnowledgeChunk.embedding.is_not(None))
    )).scalar_one()

    dims = (await db.execute(
        select(KnowledgeChunk.embedding_dim).distinct()
        .where(KnowledgeChunk.embedding.is_not(None))
    )).scalars().all()
    models = (await db.execute(
        select(KnowledgeChunk.embedding_model, func.count())
        .group_by(KnowledgeChunk.embedding_model)
        .order_by(KnowledgeChunk.embedding_model)
    )).all()

    by_category = (await db.execute(
        select(KnowledgeDocument.category, func.count())
        .group_by(KnowledgeDocument.category)
        .order_by(KnowledgeDocument.category)
    )).all()

    return {
        "documents": int(documents),
        "chunks": int(chunks),
        "vectorized_chunks": int(vectorized),
        "embedding_dims": sorted(d for d in dims if d is not None),
        "embedding_models": [{"model": m or "", "count": int(c)} for m, c in models],
        "documents_by_category": [{"category": c, "count": int(n)} for c, n in by_category],
    }


# ============================================================
# CLI
# ============================================================
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="import_knowledge.py",
        description="把 JSON 语料导入 RAG 知识库（切片 → 真实 Embedding → knowledge_chunk → VectorStore）",
    )
    parser.add_argument("--corpus", default=DEFAULT_CORPUS,
                        help=f"语料文件，默认 {DEFAULT_CORPUS}")
    parser.add_argument("--dry-run", action="store_true",
                        help="只解析与预估（不编码、不写库）")
    parser.add_argument("--yes", action="store_true",
                        help="确认真跑（会写入生产库）")
    parser.add_argument("--provider", default=None,
                        help=f"显式指定 {ENV_PROVIDER}（如 spark）；缺省沿用环境变量")
    parser.add_argument("--category", default=None, choices=list(KNOWLEDGE_CATEGORIES),
                        help="只导入该类别")
    parser.add_argument("--limit", type=int, default=None,
                        help="最多导入前 N 篇（按语料顺序）")
    parser.add_argument("--only-failed", default=None, metavar="STATS_JSON",
                        help="只导入上次统计文件里失败的文档（按 source 匹配）")
    parser.add_argument("--pace", type=float, default=3.0,
                        help="每条编码之间的间隔秒数，默认 3.0（讯飞实测稳定值）")
    parser.add_argument("--retries", type=int, default=3,
                        help="单条编码的重试次数（不含首次），默认 3")
    parser.add_argument("--retry-delay", type=float, default=3.0,
                        help="重试间隔秒数，默认 3.0")
    parser.add_argument("--allow-offline", action="store_true",
                        help="允许用离线占位导入——**需求明确禁止**，仅测试库可越")
    parser.add_argument("--stats", default=DEFAULT_STATS_PATH,
                        help=f"统计输出路径，默认 {DEFAULT_STATS_PATH}")
    return parser


async def run(args: argparse.Namespace) -> int:
    # ---- 0. 参数自检：真跑与试跑必须二选一 ----
    if not args.yes and not args.dry_run:
        print("[!] 这是会把语料写进生产库的导入脚本。", file=sys.stderr)
        print("    先看计划：  --dry-run", file=sys.stderr)
        print("    确认真跑：  --yes", file=sys.stderr)
        return EXIT_USAGE
    if args.yes and args.dry_run:
        print("[!] --yes 与 --dry-run 不能同时给（一个写库、一个不写）。", file=sys.stderr)
        return EXIT_USAGE

    # ---- 1. 读语料（入参契约问题当场报，不带着坏数据往下跑）----
    try:
        documents = load_corpus(args.corpus)
    except (OSError, ValueError) as exc:
        print(f"[X] 语料读取/校验失败：{type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_USAGE

    if args.category:
        documents = [d for d in documents if d["category"] == args.category]
    if args.only_failed:
        try:
            wanted = set(_load_failed_sources(args.only_failed))
        except (OSError, ValueError) as exc:
            print(f"[X] --only-failed 读取失败：{exc}", file=sys.stderr)
            return EXIT_USAGE
        documents = [d for d in documents if d["source"] in wanted]
        print(f"只导入上次失败的 {len(wanted)} 篇 → 命中 {len(documents)} 篇")
    if args.limit is not None:
        if isinstance(args.limit, bool) or args.limit < 1:
            print("[X] --limit 必须是 >= 1 的整数", file=sys.stderr)
            return EXIT_USAGE
        documents = documents[:args.limit]
    if not documents:
        print("[X] 过滤后没有可导入的文档。", file=sys.stderr)
        return EXIT_USAGE

    # ---- 2. 选 Embedding 实现（显式 opt-in；本脚本就是那个显式动作）----
    if args.provider:
        os.environ[ENV_PROVIDER] = str(args.provider).strip()
    embedder = default_embedder(role=ROLE_DOCUMENT)
    info = describe_embedding(embedder)
    embedder_name = str(getattr(embedder, "name", "") or info.provider)
    embedder_dim = int(getattr(embedder, "dimension", 0) or 0)

    # 预估切片数（用与 Pipeline **同一个** chunker 默认值，不另设参数）
    chunker = DocumentChunker()
    planned_chunks = sum(
        len(chunker.split(d["content"], document_id=0,
                          category=d["category"], source=d["source"]))
        for d in documents
    )

    print("=" * 74)
    print("知识库语料导入（RAG · 真实 Embedding）")
    print("=" * 74)
    print(f"数据库      : {_redact(DATABASE_URL)}")
    print(f"语料        : {args.corpus}")
    print(f"待导入      : {len(documents)} 篇 → 预估 {planned_chunks} 片")
    print(f"切片参数    : chunk_size={DEFAULT_CHUNK_SIZE} chunk_overlap={DEFAULT_CHUNK_OVERLAP}"
          f"（DocumentChunker 默认，既有结构不变）")
    print(f"Embedding   : provider={info.provider} dim={info.dimension} "
          f"semantic={info.semantic_enabled}")
    print(f"角色(模式)  : {ROLE_DOCUMENT}（文档向量化 ⇒ 讯飞 domain=para）")
    print(f"节流        : 每条 {args.pace}s    重试 {args.retries} × {args.retry_delay}s")
    print(f"模式        : {'DRY-RUN（不写库）' if args.dry_run else '真跑（写库）'}")
    print("-" * 74)

    # ---- 3. 安全闸：需求明确禁止 HashEmbedding ----
    if not info.semantic_enabled and not args.allow_offline:
        print("[X] 当前 Embedding 是**离线占位**（HashEmbeddingService，无语义）。", file=sys.stderr)
        print("    需求要求「不使用 HashEmbedding」⇒ 拒绝执行。", file=sys.stderr)
        print("    请用 --provider spark（或先在 backend/.env 里配好 EMBEDDING_*）。", file=sys.stderr)
        return EXIT_SAFETY

    started = time.monotonic()
    paced = PacedEmbedder(
        embedder, pace_seconds=args.pace, retries=args.retries,
        retry_delay=args.retry_delay,
    )
    reports: List[Dict[str, Any]] = []
    failed_sources: List[str] = []

    async with async_session() as db:
        before = await _db_stats(db)
        store = build_vector_store(db)

        if args.dry_run:
            print("DRY-RUN：不编码、不写库。库内现状：")
            print(f"  文档 {before['documents']} / 切片 {before['chunks']} / "
                  f"已向量化 {before['vectorized_chunks']}")
            print(f"  维度分布 {before['embedding_dims']} / 模型分布 "
                  f"{[m['model'] for m in before['embedding_models']]}")
            after = before
        else:
            pipeline = KnowledgeImportPipeline(db, embedder=paced, store=store)
            for index, doc in enumerate(documents, start=1):
                print(f"[{index}/{len(documents)}] {doc['category']:9s} {doc['title']}")
                report = await pipeline.import_document(doc)
                reports.append(report)
                flag = "OK " if report["ok"] else "FAIL"
                print(f"      {flag} status={report['status']:8s} "
                      f"chunks={report['chunk_count']} new={report['saved_chunks']} "
                      f"embedded={report['embedded_chunks']} "
                      f"skipped={report['skipped_chunks']}"
                      + (f"  stage={report['stage']} error={report['error'][:90]}"
                         if not report["ok"] else ""))
                if not report["ok"]:
                    failed_sources.append(doc["source"])

            after = await _db_stats(db)

        store_count = await store.count()

    elapsed = time.monotonic() - started
    ok_reports = [r for r in reports if r["ok"]]
    payload = {
        "generated_at": _now_iso(),
        "database": _redact(DATABASE_URL),
        "corpus": args.corpus,
        "corpus_name": None,
        "embedding": {
            "provider": info.provider,
            "name": embedder_name,
            "dimension": info.dimension,
            "declared_dimension": embedder_dim,
            "semantic_enabled": info.semantic_enabled,
            "role": ROLE_DOCUMENT,
        },
        "chunking": {"chunk_size": DEFAULT_CHUNK_SIZE,
                     "chunk_overlap": DEFAULT_CHUNK_OVERLAP,
                     "chunker": "DocumentChunker(默认)"},
        "options": {
            "dry_run": args.dry_run,
            "category": args.category,
            "limit": args.limit,
            "only_failed_from": args.only_failed,
            "pace_seconds": args.pace,
            "retries": args.retries,
            "retry_delay": args.retry_delay,
        },
        # ---- 需求点名要记录的四项 ----
        "summary": {
            "documents_in_corpus": len(documents),
            "documents_imported": len(ok_reports),
            "documents_failed": len(failed_sources),
            "documents_created": sum(1 for r in reports if not r["reused_document"]),
            "documents_reused": sum(1 for r in reports if r["reused_document"]),
            "chunks_total_in_db": after["chunks"],
            "chunks_new_this_run": sum(r["saved_chunks"] for r in reports),
            "chunks_embedded_this_run": sum(r["embedded_chunks"] for r in reports),
            "chunks_skipped_this_run": sum(r["skipped_chunks"] for r in reports),
            "embedding_dimension": after["embedding_dims"],
            "embedding_models": after["embedding_models"],
            "vector_store_backend": getattr(store, "name", ""),
            "vector_store_count": store_count,
        },
        "db_before": before,
        "db_after": after,
        "embedding_calls": {
            "attempts": len(paced.calls),
            "retried": paced.retried,
            "worst_seconds": round(max(paced.calls), 4) if paced.calls else None,
            "avg_seconds": round(sum(paced.calls) / len(paced.calls), 4) if paced.calls else None,
        },
        "timing": {"seconds": round(elapsed, 3)},
        "failed_sources": failed_sources,
        "reports": reports,
    }
    try:
        with open(args.corpus, "r", encoding="utf-8") as handle:
            payload["corpus_name"] = json.load(handle).get("name")
    except Exception:  # noqa: BLE001 - 名字只是给人看的，读不到就算了
        pass

    with open(args.stats, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)

    # ---- 4. 一致性自检（写库后必须自证「没混进旧模型」）----
    problems: List[str] = []
    if not args.dry_run:
        models = {item["model"] for item in after["embedding_models"]}
        if models and models != {embedder_name}:
            problems.append(f"库内向量模型不是唯一目标模型：{sorted(models)}")
        if embedder_dim > 0 and after["embedding_dims"] not in ([], [embedder_dim]):
            problems.append(f"库内维度不是唯一目标维度：{after['embedding_dims']}")
        if after["vectorized_chunks"] != after["chunks"]:
            problems.append(
                f"仍有未向量化切片：{after['chunks'] - after['vectorized_chunks']} 片"
            )
        if store_count != after["vectorized_chunks"]:
            problems.append(
                f"VectorStore 条数({store_count}) 与已向量化切片数"
                f"({after['vectorized_chunks']}) 不一致"
            )

    s = payload["summary"]
    print("-" * 74)
    print(f"文档数量    : {after['documents']}"
          f"（本次导入 {s['documents_imported']} 篇 / 新建 {s['documents_created']} / "
          f"复用 {s['documents_reused']} / 失败 {s['documents_failed']}）")
    print(f"chunk 数量  : {s['chunks_total_in_db']}"
          f"（本次新建 {s['chunks_new_this_run']} / 已向量化 {s['chunks_embedded_this_run']} / "
          f"幂等跳过 {s['chunks_skipped_this_run']}）")
    print(f"embedding 维度: {s['embedding_dimension']}  （模型 {[m['model'] for m in s['embedding_models']]}）")
    print(f"VectorStore : {s['vector_store_backend']} → {s['vector_store_count']} 条")
    print(f"上游调用    : {payload['embedding_calls']['attempts']} 次"
          f"（重试 {payload['embedding_calls']['retried']}）"
          f"  最慢 {payload['embedding_calls']['worst_seconds']}s")
    print(f"耗时        : {elapsed:.2f}s")
    if problems:
        print("[!] 一致性自检未通过：")
        for item in problems:
            print(f"    - {item}")
    else:
        print("一致性自检  : 通过（向量模型唯一 / 维度唯一 / 无未向量化切片 / VectorStore 对齐）")
    print(f"统计        : {args.stats}")
    print("=" * 74)

    await engine.dispose()
    return EXIT_HAS_FAILURES if (failed_sources or problems) else EXIT_OK


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
