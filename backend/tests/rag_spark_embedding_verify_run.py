# -*- coding: utf-8 -*-
"""**真实 Embedding 全链路验收**（非回归套件；需要联网 + 真实凭据）

    python backend/tests/rag_spark_embedding_verify_run.py            # 内存 SQLite（默认，无副作用）
    python backend/tests/rag_spark_embedding_verify_run.py --mysql    # 走真实 MySQL（用完即删测试文档）

为什么单独一个运行器（而不是 ``test_*.py`` 套件）
-------------------------------------------------
它**必须联网**并消耗讯飞配额，而项目约定「回归套件全程不联网、不需要密钥」。
命名不带 ``test_`` 前缀，因此不会被当回归套件跑（同 ``rag_baseline_run.py`` 的做法）。

它验的是「接线是否真的通了」——单测只能证明「代码按契约调用」，
证明不了「真实上游 + 真实切片 + 真实向量库 + 真实检索」这条链子合得上：

1. 写入侧：``DocumentChunker`` → **真实讯飞 embedding（domain=para）** → ``knowledge_chunk`` + 向量库
2. 读侧：``build_vector_retriever`` → **真实讯飞 embedding（domain=query）** → 余弦召回
3. **role/domain 取证**：写侧实例 ``domain=="para"``、读侧实例 ``domain=="query"``
   （非对称编码配错**不报错、只掉召回**，所以必须显式取证）
4. 幂等：重复导入同一文档 → ``status="skipped"`` 且**不再调用上游**
5. 模型隔离：用 ``hash-local`` 的检索器去查同一批数据 → **0 条**
   （向量库按 ``embedding_model`` 过滤 ⇒ 不同模型的向量不可比，宁可查不到）
6. 时延：每次调用耗时必须远低于上游上限 **60s**（官方：全链路会话不超过 1 分钟）

报告写到 ``backend/scripts/rag_spark_embedding_verify.json``。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(BACKEND_DIR / ".env")

# 只在「内存 SQLite」模式下才兜底设 DATABASE_URL（真实 MySQL 模式必须用 .env 里的值）
_MYSQL_MODE = "--mysql" in sys.argv
if not _MYSQL_MODE:
    os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
# 本运行器的目的就是验真实 embedding ⇒ 强制打开（不依赖 .env 里是否取消注释）
os.environ["EMBEDDING_PROVIDER"] = "spark"

from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from database import Base  # noqa: E402
from models import KnowledgeChunk, KnowledgeDocument  # noqa: E402
from services.embedding_provider import build_embedding_service  # noqa: E402
from services.embedding_provider import ROLE_DOCUMENT, ROLE_QUERY  # noqa: E402
from services.embedding_service import (  # noqa: E402
    EmbeddingService,
    EmbeddingUnavailableError,
    HashEmbeddingService,
)
from services.knowledge_import_pipeline import KnowledgeImportPipeline  # noqa: E402
from services.knowledge_maintenance import delete_document  # noqa: E402
from services.knowledge_rag import build_vector_retriever  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

# ============================================================
# 固定语料（**逐字节固定**：期望值都由它推导）
# ============================================================
TITLE_PREFIX = "[验收] "
DOCS: List[Dict[str, str]] = [
    {
        "title": TITLE_PREFIX + "Redis 持久化机制",
        "category": "technical",
        "source": "verify://redis/persistence",
        "content": (
            "Redis 持久化提供 RDB 与 AOF 两种机制：RDB 是某一时刻的全量快照，通过 fork "
            "子进程写盘，文件紧凑、恢复快，但两次快照之间的写入会丢失；AOF 记录每条写命令，"
            "appendfsync 可取 always / everysec / no，everysec 是生产常见折中，最多丢 1 秒数据，"
            "重写时用 BGREWRITEAOF。"
        ),
    },
    {
        "title": TITLE_PREFIX + "MySQL 索引与最左前缀",
        "category": "technical",
        "source": "verify://mysql/index",
        "content": (
            "MySQL 联合索引遵循最左前缀原则：只有从索引最左列开始的连续前缀才能被用于定位，"
            "跳过中间列会导致后面的列无法走索引。索引选择性指不重复值与总行数的比值，"
            "选择性越高越适合建索引；范围查询会让其后的列失去索引能力，"
            "因此把等值条件放在范围条件之前。"
        ),
    },
    {
        "title": TITLE_PREFIX + "Kafka 消息不丢失",
        "category": "technical",
        "source": "verify://kafka/reliability",
        "content": (
            "Kafka 保证消息不丢失需要三段都到位：生产者设置 acks=all 并开启重试与幂等；"
            "broker 侧设置 replication.factor >= 3 且 min.insync.replicas >= 2，"
            "并关闭 unclean.leader.election；消费者关闭自动提交，处理完业务后再手动提交位点。"
        ),
    },
]

#: (查询, 期望命中的 source) —— 查询文本与原文**措辞不同**，才有「语义检索」的意义
QUERIES: List[Tuple[str, str]] = [
    ("RDB 和 AOF 有什么区别？", "verify://redis/persistence"),
    ("联合索引为什么要遵循最左前缀？", "verify://mysql/index"),
    ("怎么保证 Kafka 的消息不丢？", "verify://kafka/reliability"),
]

#: 上游限流实测：连续快速调用会得到 HTTP 500 + code=11202（licc failed）⇒ 串行 + 间隔 + 重试
CALL_INTERVAL_SECONDS = 3.0
RETRY_ATTEMPTS = 4

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


class CountingEmbedder(EmbeddingService):
    """包住真实 embedder：记录**调用次数 / 耗时 / domain**（其余行为原样透传）。

    ``name`` / ``dimension`` / ``semantic_enabled`` 必须透传——入库 Pipeline 用
    ``embedder.name`` 写 ``embedding_model`` 并据此判断「旧向量是否可比」。
    """

    def __init__(self, inner: EmbeddingService, *, role: str) -> None:
        self.inner = inner
        self.role = role
        self.name = inner.name
        self.dimension = inner.dimension
        self.semantic_enabled = getattr(inner, "semantic_enabled", False)
        self.calls: List[float] = []          # 每次调用的耗时（秒）

    @property
    def domain(self) -> str:
        return getattr(self.inner, "domain", "")

    async def _embed_one(self, text: str) -> List[float]:
        started = time.perf_counter()
        try:
            return await self.inner.embed(text)
        finally:
            self.calls.append(time.perf_counter() - started)

    async def embed(self, text: str) -> List[float]:  # type: ignore[override]
        # 显式覆盖：把「一次逻辑调用」记成一次，而不是让基类的 _require_vector 再走一遍
        return await self._embed_one(text)

    async def embed_batch(self, texts):  # type: ignore[override]
        return [await self._embed_one(t) for t in texts]


async def _with_retry(embedder: CountingEmbedder, text: str) -> List[float]:
    """串行 + 重试（上游限流时 ``EmbeddingUnavailableError``，可重试）。"""
    last: Optional[BaseException] = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            return await embedder.embed(text)
        except EmbeddingUnavailableError as exc:
            last = exc
            print(f"    [retry {attempt + 1}/{RETRY_ATTEMPTS}] {str(exc)[:110]}")
            if attempt + 1 < RETRY_ATTEMPTS:
                await asyncio.sleep(CALL_INTERVAL_SECONDS)
    raise last  # type: ignore[misc]


async def _run_pipeline_with_pacing(
    pipeline: KnowledgeImportPipeline, doc: Dict[str, str]
) -> Dict[str, Any]:
    """用「串行 + 间隔 + 重试」的方式跑入库（真实上游有 license 限流）。"""
    report = await pipeline.import_document(doc)
    # 限流失败时报告是 ok=False / stage=embedding ⇒ 退避后整篇重跑（幂等，安全）
    for attempt in range(RETRY_ATTEMPTS - 1):
        if report.get("ok") or report.get("stage") != "embedding":
            return report
        print(f"    [retry {attempt + 1}/{RETRY_ATTEMPTS - 1}] 入库在 embedding 阶段失败，"
              f"退避后重跑：{str(report.get('error'))[:110]}")
        await asyncio.sleep(CALL_INTERVAL_SECONDS)
        report = await pipeline.import_document(doc)
    return report


async def _main() -> int:
    print("=" * 74)
    print("真实 Embedding 全链路验收（讯飞星火）")
    print("=" * 74)

    backend_label = "MySQL（真实库）" if _MYSQL_MODE else "内存 SQLite（无副作用）"
    print(f"目标库: {backend_label}")
    print(f"DATABASE_URL 驱动: {os.environ.get('DATABASE_URL', '').split('://')[0]}")

    # ------------------------------------------------------------
    # [1] 配置与实例（两侧 role 必须不同）
    # ------------------------------------------------------------
    print("\n[1] 构造真实 Embedding（写入侧 para / 读取侧 query）")
    write_inner = build_embedding_service(role=ROLE_DOCUMENT)
    read_inner = build_embedding_service(role=ROLE_QUERY)
    _check("★ 写入侧实例 domain=para（知识原文）", write_inner.domain == "para",
           getattr(write_inner, "domain", "<无 domain 属性>"))
    _check("★ 读取侧实例 domain=query（用户问题）", read_inner.domain == "query",
           getattr(read_inner, "domain", "<无 domain 属性>"))
    _check("  └ 模型标识与离线占位区分开（入库据此判断旧向量是否可比）",
           write_inner.name not in ("", HashEmbeddingService.name), write_inner.name)
    _check("  └ 维度已声明（实测 2560）", write_inner.dimension == 2560,
           str(write_inner.dimension))
    print(f"  model={write_inner.name!r} dimension={write_inner.dimension} "
          f"endpoint={write_inner.endpoint}")

    write_embedder = CountingEmbedder(write_inner, role=ROLE_DOCUMENT)
    read_embedder = CountingEmbedder(read_inner, role=ROLE_QUERY)

    # ------------------------------------------------------------
    # [2] 建库
    # ------------------------------------------------------------
    print("\n[2] 准备数据库")
    if _MYSQL_MODE:
        engine = create_async_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)
    else:
        engine = create_async_engine(
            os.environ["DATABASE_URL"], poolclass=StaticPool,
            connect_args={"check_same_thread": False},
        )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print(f"  建表完成（create_all 幂等；knowledge_document / knowledge_chunk 已就绪）")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    report: Dict[str, Any] = {"backend": backend_label, "model": write_inner.name,
                             "dimension": write_inner.dimension}
    created_ids: List[int] = []
    try:
        async with Session() as db:
            store = SqlAlchemyVectorStore(db)
            pipeline = KnowledgeImportPipeline(db, embedder=write_embedder, store=store)

            # ------------------------------------------------------------
            # [3] 写入侧：真实 embedding + 落库 + 写向量库
            # ------------------------------------------------------------
            print("\n[3] 写入侧：切片 → 真实 embedding(para) → 落库 + 写向量库")
            import_reports: List[Dict[str, Any]] = []
            for index, doc in enumerate(DOCS):
                if index:
                    await asyncio.sleep(CALL_INTERVAL_SECONDS)
                rep = await _run_pipeline_with_pacing(pipeline, doc)
                import_reports.append({k: rep.get(k) for k in
                                       ("ok", "status", "stage", "document_id",
                                        "chunk_count", "embedded_chunks",
                                        "skipped_chunks", "errors")})
                print(f"  {doc['title']}: ok={rep.get('ok')} status={rep.get('status')} "
                      f"chunks={rep.get('chunk_count')} embedded={rep.get('embedded_chunks')} "
                      f"errors={rep.get('errors')}")
                if rep.get("document_id"):
                    created_ids.append(int(rep["document_id"]))
                _check(f"★ 入库成功：{doc['source']}", bool(rep.get("ok")),
                       str(rep.get("errors") or rep.get("error")))
            report["imports"] = import_reports

            calls_after_write = len(write_embedder.calls)
            _check("★ 写入侧确实调用了真实上游（不是离线占位）",
                   calls_after_write > 0, str(calls_after_write))
            _check("  └ 写入侧只用了 para（不会拿 query 编码原文）",
                   write_embedder.domain == "para", write_embedder.domain)

            # 落库口径：embedding 非空、model / dim 都写对
            rows = (await db.execute(select(KnowledgeChunk))).scalars().all()
            _check("★ 切片行已落库", len(rows) >= len(DOCS), str(len(rows)))
            _check("★ 每行都有向量（embedding IS NOT NULL）",
                   all(r.embedding for r in rows),
                   str([len(r.embedding or []) for r in rows]))
            _check(f"★ embedding_model 记录为 {write_inner.name!r}",
                   all(r.embedding_model == write_inner.name for r in rows),
                   str(sorted({r.embedding_model for r in rows})))
            _check("★ embedding_dim 记录为 2560",
                   all(r.embedding_dim == write_inner.dimension for r in rows),
                   str(sorted({r.embedding_dim for r in rows})))
            _check("★ 向量长度与声明维度一致",
                   all(len(r.embedding or []) == write_inner.dimension for r in rows))

            # ------------------------------------------------------------
            # [4] 幂等：重复导入不再调用上游
            # ------------------------------------------------------------
            print("\n[4] 幂等：重复导入同一文档")
            await asyncio.sleep(CALL_INTERVAL_SECONDS)
            again = await pipeline.import_document(DOCS[0])
            _check("★ 重复导入 → status=skipped（成功的空操作）",
                   again.get("status") == "skipped", str(again.get("status")))
            _check("  └ embedded_chunks=0（没有重算）",
                   again.get("embedded_chunks") == 0, str(again.get("embedded_chunks")))
            _check("  └ 上游调用次数没增加（真的跳过了编码）",
                   len(write_embedder.calls) == calls_after_write,
                   f"{calls_after_write} → {len(write_embedder.calls)}")
            report["idempotent"] = {k: again.get(k) for k in
                                    ("ok", "status", "embedded_chunks", "skipped_chunks")}

            # ------------------------------------------------------------
            # [5] 读取侧：真实 embedding(query) → 余弦召回
            # ------------------------------------------------------------
            print("\n[5] 读取侧：真实 embedding(query) → 向量检索")
            retriever = build_vector_retriever(db, embedder=read_embedder, top_k=3)
            _check("★ 读取侧实例 domain=query", read_embedder.domain == "query",
                   read_embedder.domain)

            hits: List[Dict[str, Any]] = []
            for query, expected in QUERIES:
                await asyncio.sleep(CALL_INTERVAL_SECONDS)
                chunks = await _with_retry_retrieve(retriever, query)
                ranked = [
                    {
                        # 值对象的三键是 content / source / metadata（score 在 metadata 里）
                        "source": c.source,
                        "score": round(float((c.metadata or {}).get("score", 0.0)), 6),
                        "preview": (c.content or "")[:34],
                    }
                    for c in chunks
                ]
                hits.append({"query": query, "expected": expected, "ranked": ranked})
                print(f"  Q: {query}")
                for pos, item in enumerate(ranked, 1):
                    print(f"     #{pos} score={item['score']:.6f} {item['source']}")
                _check(f"★ 命中期望文档且排第 1：{expected}",
                       bool(ranked) and ranked[0]["source"] == expected,
                       str(ranked[:1]))
                _check("  └ 分数落在余弦合法区间 (-1, 1]",
                       bool(ranked) and all(-1.0 < r["score"] <= 1.0 for r in ranked),
                       str([r["score"] for r in ranked]))
                _check("  └ 返回条数不超过 top_k=3", len(ranked) <= 3, str(len(ranked)))
            report["retrieval"] = hits

            _check("★ 读取侧确实调用了真实上游", len(read_embedder.calls) == len(QUERIES),
                   str(len(read_embedder.calls)))

            # ------------------------------------------------------------
            # [6] 模型隔离：换模型的检索器必须查不到（而不是算出错误的相似度）
            # ------------------------------------------------------------
            print("\n[6] 模型隔离：用 hash-local 检索同一批数据")
            hash_retriever = build_vector_retriever(
                db, embedder=HashEmbeddingService(dimension=2560), top_k=3)
            hash_chunks = await hash_retriever.retrieve(
                {"job_title": "后端工程师"}, "Redis 持久化", "")
            _check("★ 不同模型的向量不可比 ⇒ 检索结果为 0 条（宁可查不到，不算错分）",
                   hash_chunks == [], str(len(hash_chunks)))

            # ------------------------------------------------------------
            # [7] 默认路径取证：不注入 embedder 时，读侧自动用 query
            # ------------------------------------------------------------
            print("\n[7] 默认路径：build_vector_retriever 不注入 embedder")
            default_retriever = build_vector_retriever(db, top_k=3)
            _check("★ 默认读侧 embedder 的 domain=query（组装器传对了 role）",
                   getattr(default_retriever.embedder, "domain", None) == "query",
                   getattr(default_retriever.embedder, "domain", None))
            _check("  └ 默认读侧的模型标识与写入侧一致（否则会因模型过滤而查不到）",
                   default_retriever.embedder.name == write_inner.name,
                   f"{default_retriever.embedder.name} vs {write_inner.name}")
            await asyncio.sleep(CALL_INTERVAL_SECONDS)
            default_hits = await default_retriever.retrieve(
                {"job_title": "后端工程师"}, "Redis 持久化", "")
            _check("★ 默认路径也能召回（读侧/写侧同模型同口径）", len(default_hits) > 0,
                   str(len(default_hits)))

            # ------------------------------------------------------------
            # [8] 时延（上游要求：全链路会话 <= 60s）
            # ------------------------------------------------------------
            print("\n[8] 时延")
            all_calls = write_embedder.calls + read_embedder.calls
            worst = max(all_calls) if all_calls else 0.0
            print(f"  调用 {len(all_calls)} 次 | 最慢 {worst:.3f}s | "
                  f"平均 {sum(all_calls) / len(all_calls):.3f}s")
            _check("★ 单次调用远低于上游上限 60s（本实现一条文本 = 一次短请求）",
                   worst < 60.0, f"worst={worst:.3f}s")
            report["latency"] = {
                "calls": len(all_calls), "worst_seconds": round(worst, 3),
                "avg_seconds": round(sum(all_calls) / len(all_calls), 3),
                "limit_seconds": 60.0,
            }
    finally:
        # ------------------------------------------------------------
        # [9] 收尾：MySQL 模式下删掉本次验收写入的文档（不留垃圾）
        # ------------------------------------------------------------
        if _MYSQL_MODE and created_ids:
            print("\n[9] 收尾：删除本次验收写入的文档")
            async with Session() as db:
                for doc_id in created_ids:
                    try:
                        out = await delete_document(db, doc_id)
                        print(f"  deleted document_id={doc_id} "
                              f"chunks={out.get('deleted_chunks')}")
                    except Exception as exc:  # noqa: BLE001
                        print(f"  [WARN] 删除 document_id={doc_id} 失败：{exc}")
        await engine.dispose()

    report["checks"] = {"passed": _PASSED, "failed": _FAILED}
    out_path = BACKEND_DIR / "scripts" / "rag_spark_embedding_verify.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n报告已写出：{out_path}")

    print("\n" + "=" * 74)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return 0 if _FAILED == 0 else 1


async def _with_retry_retrieve(retriever, query: str):
    """检索也带重试：query 侧同样会撞上游限流。"""
    last: Optional[BaseException] = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            return await retriever.retrieve({"job_title": "后端工程师"}, query, "")
        except Exception as exc:  # noqa: BLE001 - RetrieverUnavailableError 等
            last = exc
            if attempt + 1 < RETRY_ATTEMPTS:
                print(f"    [retry {attempt + 1}/{RETRY_ATTEMPTS}] 检索失败：{str(exc)[:110]}")
                await asyncio.sleep(CALL_INTERVAL_SECONDS)
    raise last  # type: ignore[misc]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="真实 Embedding 全链路验收")
    parser.add_argument("--mysql", action="store_true", help="走真实 MySQL（用完即删测试文档）")
    parser.parse_args()
    sys.exit(asyncio.run(_main()))
