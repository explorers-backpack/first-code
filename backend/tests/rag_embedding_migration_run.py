# -*- coding: utf-8 -*-
"""**真实 Embedding 向量迁移实跑**（非回归套件；需要联网 + 真实凭据）

    python backend/tests/rag_embedding_migration_run.py

为什么单独一个运行器（而不是 ``test_*.py`` 套件）
-------------------------------------------------
它**必须联网**并消耗讯飞配额，而项目约定「回归套件全程不联网、不需要密钥」。
命名不带 ``test_`` 前缀，因此不会被当回归套件跑（同 ``rag_*_run.py`` 的做法）。

离线套件（``test_knowledge_embedding_migration.py``）只能证明「代码按契约调用」；
本运行器复现**真实迁移场景**并给出**真实执行统计**：

1. **造出「旧知识库」**：用 ``HashEmbeddingService`` 经**正式入库 Pipeline**
   建库（这正是「旧向量由 HashEmbedding 生成」的成因），取证全库是
   ``hash-local`` / 256 维；
2. **迁移**：``EmbeddingMigration`` + **真实讯飞 Embedding（文档模式 domain=para）**
   把每一片的向量就地重算，**切片正文一字不动**；
3. **幂等**：立刻复跑 → ``status="skipped"`` 且**上游调用次数增量为 0**；
4. **迁移后可用**：读侧（``domain=query``）检索，期望来源排在第一位；
5. **旧模型隔离**：拿 ``hash-local`` 的检索器去查 → **0 条**（不同模型向量不可比）；
6. **时延**：单次调用最远低于上游上限 **60s**（官方：全链路会话不超过 1 分钟）。

报告写到 ``backend/scripts/rag_embedding_migration_stats.json``。
**只跑内存 SQLite**，不碰生产库（生产库迁移用 ``scripts/migrate_embeddings.py``）。
"""

from __future__ import annotations

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

# 本运行器只在内存 SQLite 上跑（**不碰生产库**），且必须验真实 embedding
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
os.environ["EMBEDDING_PROVIDER"] = "spark"

from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from database import Base  # noqa: E402
from models import KnowledgeChunk, KnowledgeDocument  # noqa: E402
from services.embedding_provider import (  # noqa: E402
    ROLE_DOCUMENT,
    ROLE_QUERY,
    build_embedding_service,
)
from services.embedding_service import (  # noqa: E402
    EmbeddingService,
    EmbeddingUnavailableError,
    HashEmbeddingService,
)
from services.knowledge_embedding_migration import (  # noqa: E402
    MIGRATION_RESULT_FIELDS,
    EmbeddingMigration,
)
from services.knowledge_import_pipeline import KnowledgeImportPipeline  # noqa: E402
from services.knowledge_rag import build_vector_retriever  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

# ============================================================
# 固定语料（**逐字节固定**：期望值都由它推导）
# ============================================================
TITLE_PREFIX = "[迁移验收] "
DOCS: List[Dict[str, str]] = [
    {
        "title": TITLE_PREFIX + "Redis 持久化机制",
        "category": "technical",
        "source": "migrate://redis/persistence",
        "content": (
            "Redis 持久化提供 RDB 与 AOF 两种机制：RDB 是某一时刻的全量快照，通过 fork "
            "子进程写盘，文件紧凑、恢复快，但两次快照之间的写入会丢失；AOF 记录每条写命令，"
            "appendfsync 可取 always / everysec / no，everysec 是生产常见折中，最多丢 1 秒数据。"
        ),
    },
    {
        "title": TITLE_PREFIX + "MySQL 索引与最左前缀",
        "category": "technical",
        "source": "migrate://mysql/index",
        "content": (
            "MySQL 联合索引遵循最左前缀原则：只有从索引最左列开始的连续前缀才能被用于定位，"
            "跳过中间列会导致后面的列无法走索引。索引选择性指不重复值与总行数的比值，"
            "选择性越高越适合建索引；范围查询会让其后的列失去索引能力。"
        ),
    },
    {
        "title": TITLE_PREFIX + "Kafka 消息不丢失",
        "category": "technical",
        "source": "migrate://kafka/reliability",
        "content": (
            "Kafka 保证消息不丢失需要三段都到位：生产者设置 acks=all 并开启重试与幂等；"
            "broker 侧设置 replication.factor >= 3 且 min.insync.replicas >= 2；"
            "消费者关闭自动提交，处理完业务后再手动提交位点。"
        ),
    },
    {
        "title": TITLE_PREFIX + "HTTP 缓存与协商缓存",
        "category": "technical",
        "source": "migrate://http/cache",
        "content": (
            "HTTP 缓存分强缓存与协商缓存：强缓存用 Cache-Control 的 max-age 或 "
            "Expires，命中时不发请求；协商缓存用 ETag / If-None-Match 或 "
            "Last-Modified / If-Modified-Since，服务端返回 304 表示可继续用本地副本。"
        ),
    },
    {
        "title": TITLE_PREFIX + "数据库事务隔离级别",
        "category": "technical",
        "source": "migrate://db/isolation",
        "content": (
            "数据库事务隔离级别有四种：读未提交会脏读；读已提交避免脏读但会不可重复读；"
            "可重复读避免不可重复读，MySQL InnoDB 默认这一级并用间隙锁在很大程度上避免幻读；"
            "串行化最严格但并发最差。"
        ),
    },
    {
        "title": TITLE_PREFIX + "TCP 三次握手与四次挥手",
        "category": "technical",
        "source": "migrate://tcp/handshake",
        "content": (
            "TCP 建立连接需要三次握手：客户端发 SYN，服务端回 SYN+ACK，客户端再发 ACK，"
            "目的是同步双方初始序列号并确认收发能力；断开连接需要四次挥手，"
            "因为 TCP 是全双工，两个方向要各自关闭，服务端收到 FIN 后可能还有数据要发。"
        ),
    },
]

#: (查询, 期望命中的 source) —— 查询措辞与原文**不同**，才有「语义检索」的意义
QUERIES: List[Tuple[str, str]] = [
    ("RDB 和 AOF 有什么区别？", "migrate://redis/persistence"),
    ("联合索引为什么要遵循最左前缀？", "migrate://mysql/index"),
    ("怎么保证 Kafka 的消息不丢？", "migrate://kafka/reliability"),
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

    ``name`` / ``dimension`` 必须透传——迁移器用 ``embedder.name`` 写
    ``embedding_model`` 并据此判断「这一片要不要重算」。
    """

    def __init__(self, inner: EmbeddingService) -> None:
        self.inner = inner
        self.name = inner.name
        self.dimension = inner.dimension
        self.semantic_enabled = getattr(inner, "semantic_enabled", False)
        self.calls: List[float] = []

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
        return await self._embed_one(text)

    async def embed_batch(self, texts):  # type: ignore[override]
        return [await self._embed_one(t) for t in texts]


async def _snapshot(db: Any) -> List[Tuple[Any, ...]]:
    """切片行「除向量三列以外」的全部字段快照（用于断言**没被改**）。"""
    rows = (await db.execute(
        select(KnowledgeChunk).order_by(KnowledgeChunk.id)
    )).scalars().all()
    return [(r.id, r.document_id, r.content, r.chunk_metadata) for r in rows]


async def _vectors(db: Any) -> Dict[int, Tuple[Optional[int], str, Optional[int]]]:
    rows = (await db.execute(
        select(KnowledgeChunk).order_by(KnowledgeChunk.id)
    )).scalars().all()
    return {
        r.id: (len(r.embedding) if isinstance(r.embedding, list) else None,
               r.embedding_model, r.embedding_dim)
        for r in rows
    }


async def _with_retry_retrieve(retriever: Any, query: str) -> List[Any]:
    """检索也带重试：query 侧同样会撞上游限流。"""
    last: Optional[BaseException] = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            return await retriever.retrieve({"job_title": "后端工程师"}, query, "")
        except Exception as exc:  # noqa: BLE001 - 上游限流统一按可重试处理
            last = exc
            if attempt + 1 < RETRY_ATTEMPTS:
                print(f"    [retry {attempt + 1}/{RETRY_ATTEMPTS}] 检索失败：{str(exc)[:110]}")
                await asyncio.sleep(CALL_INTERVAL_SECONDS)
    raise last  # type: ignore[misc]


async def _main() -> int:
    print("=" * 74)
    print("真实 Embedding 向量迁移实跑（讯飞星火 · 文档模式）")
    print("=" * 74)
    print("目标库: 内存 SQLite（**不碰生产库**）")

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as db:
        # ------------------------------------------------------------
        # [1] 造出「旧知识库」：HashEmbedding 经正式入库 Pipeline 建库
        # ------------------------------------------------------------
        print("\n[1] 造旧知识库（HashEmbedding 占位 → 就是「旧向量不可用」的成因）")
        legacy = HashEmbeddingService()
        pipeline = KnowledgeImportPipeline(
            db, embedder=legacy, store=SqlAlchemyVectorStore(db)
        )
        for doc in DOCS:
            report = await pipeline.import_document(doc)
            _check(f"  └ 入库 {doc['source']}",
                   report["ok"] and report["embedded_chunks"] >= 1, str(report))

        legacy_vectors = await _vectors(db)
        _check("★ 旧库全部是 hash-local 向量（这正是要迁移掉的东西）",
               bool(legacy_vectors)
               and all(v[1] == HashEmbeddingService.name for v in legacy_vectors.values()),
               str(set(v[1] for v in legacy_vectors.values())))
        _check(f"  └ 旧维度 = {legacy.dimension}（与新模型 2560 不同 ⇒ 必须重算）",
               all(v[2] == legacy.dimension for v in legacy_vectors.values()),
               str(set(v[2] for v in legacy_vectors.values())))
        before_rows = await _snapshot(db)
        chunk_total = len(before_rows)
        print(f"  旧库规模: {len(DOCS)} 篇文档 / {chunk_total} 片 / 维度 {legacy.dimension}")

        # ------------------------------------------------------------
        # [2] 迁移（真实讯飞 Embedding · 文档模式）
        # ------------------------------------------------------------
        print("\n[2] 迁移：真实讯飞 Embedding（role=document ⇒ domain=para）")
        write_inner = build_embedding_service(role=ROLE_DOCUMENT)
        _check("★ 迁移用的是**文档模式**（domain=para）",
               getattr(write_inner, "domain", "") == "para",
               getattr(write_inner, "domain", "<无 domain>"))
        _check("  └ 模型标识与旧库区分开", write_inner.name != HashEmbeddingService.name,
               write_inner.name)
        _check("  └ 维度已声明（实测 2560）", write_inner.dimension == 2560,
               str(write_inner.dimension))

        counting = CountingEmbedder(write_inner)
        migration = EmbeddingMigration(
            db, embedder=counting, store=SqlAlchemyVectorStore(db),
            batch_size=3, pace_seconds=CALL_INTERVAL_SECONDS,
            retries=RETRY_ATTEMPTS - 1, retry_delay=CALL_INTERVAL_SECONDS,
        )
        started = time.perf_counter()
        result = await migration.run()
        elapsed = time.perf_counter() - started

        _check("★ 报告键集合与 MIGRATION_RESULT_FIELDS 一致（16 键恒定）",
               tuple(result.keys()) == MIGRATION_RESULT_FIELDS, str(list(result.keys())))
        _check("★ 迁移成功：status=ok / ok=True / 失败 0",
               result["status"] == "ok" and result["ok"] is True
               and result["failed_chunks"] == 0, str(result))
        _check(f"★ 全部 {chunk_total} 片都重算了（migrated == 切片总数）",
               result["migrated_chunks"] == chunk_total,
               f"migrated={result['migrated_chunks']} total={chunk_total}")
        _check("  └ 跳过 0（旧向量与目标模型/维度都不符）",
               result["skipped_chunks"] == 0, str(result["skipped_chunks"]))
        _check(f"  └ 上游调用 ≥ 切片数（{chunk_total} 片，一次一片；含重试共 "
               f"{len(counting.calls)} 次）",
               len(counting.calls) >= chunk_total, str(len(counting.calls)))
        _check("  └ target_model / target_dim 如实上报",
               (result["target_model"], result["target_dim"])
               == (write_inner.name, write_inner.dimension), str(result))

        # ------------------------------------------------------------
        # [3] 切片本体逐字节不变（本次迁移的硬约束）
        # ------------------------------------------------------------
        print("\n[3] 切片本体取证：正文 / 元数据 / 归属 / 行数 全部不变")
        after_rows = await _snapshot(db)
        _check("★ 切片正文、元数据、文档归属、行数**逐字节不变**",
               after_rows == before_rows,
               f"before={len(before_rows)} after={len(after_rows)}")
        _check("★ 行数不变（是覆盖写，不是新增行）",
               len(after_rows) == chunk_total, str(len(after_rows)))

        new_vectors = await _vectors(db)
        _check("★ 全部向量已换成新模型",
               all(v[1] == write_inner.name for v in new_vectors.values()),
               str(set(v[1] for v in new_vectors.values())))
        _check(f"★ 全部向量维度已换成 {write_inner.dimension}",
               all(v[2] == write_inner.dimension for v in new_vectors.values()),
               str(set(v[2] for v in new_vectors.values())))
        _check("  └ 向量长度确实变了（256 → 2560，不是「只改了标记」）",
               all(v[0] == write_inner.dimension for v in new_vectors.values()),
               str(set(v[0] for v in new_vectors.values())))

        # ------------------------------------------------------------
        # [4] 幂等：复跑是空操作且**不再调用上游**
        # ------------------------------------------------------------
        print("\n[4] 幂等：复跑（期望 status=skipped 且上游调用增量为 0）")
        calls_before_rerun = len(counting.calls)
        again = await migration.run()
        _check("★ 复跑 status=skipped / ok=True（成功的空操作）",
               again["status"] == "skipped" and again["ok"] is True, str(again))
        _check("  └ 待迁移 0 / 已迁移 0 / 跳过 = 切片总数",
               (again["pending_chunks"], again["migrated_chunks"],
                again["skipped_chunks"]) == (0, 0, chunk_total), str(again))
        _check("★ 复跑**没有新增任何上游调用**（幂等判据在编码之前生效）",
               len(counting.calls) == calls_before_rerun,
               f"{calls_before_rerun} -> {len(counting.calls)}")
        _check("  └ 向量未被重写（与迁移后一致）",
               await _vectors(db) == new_vectors)

        # ------------------------------------------------------------
        # [5] 迁移后检索可用（读侧 query 模式）
        # ------------------------------------------------------------
        print("\n[5] 迁移后可用性：读侧（domain=query）真实检索")
        read_inner = build_embedding_service(role=ROLE_QUERY)
        _check("★ 读侧是 query 模式（domain=query）",
               getattr(read_inner, "domain", "") == "query",
               getattr(read_inner, "domain", "<无 domain>"))
        retriever = build_vector_retriever(db, embedder=CountingEmbedder(read_inner), top_k=3)
        hits_report: List[Dict[str, Any]] = []
        for query, expected in QUERIES:
            await asyncio.sleep(CALL_INTERVAL_SECONDS)
            hits = await _with_retry_retrieve(retriever, query)
            ranked = [(h.source, round(h.metadata.get("score", 0.0), 6)) for h in hits]
            hits_report.append({"query": query, "expected": expected, "ranked": ranked})
            _check(f"  └ 「{query}」命中期望来源",
                   bool(hits) and hits[0].source == expected,
                   str(ranked))
        _check("★ 迁移后的库**能被真实检索到**（不是「迁完查不到」）",
               all(item["ranked"] for item in hits_report), str(hits_report))

        # ------------------------------------------------------------
        # [6] 旧模型隔离：拿 hash-local 去查 → 0 条
        # ------------------------------------------------------------
        print("\n[6] 模型隔离：旧模型检索器查不到新向量（不同模型不可比）")
        legacy_retriever = build_vector_retriever(
            db, embedder=HashEmbeddingService(dimension=write_inner.dimension), top_k=3
        )
        legacy_hits = await _with_retry_retrieve(legacy_retriever, QUERIES[0][0])
        _check("★ 用 hash-local 检索 → 0 条（向量库按 embedding_model 过滤）",
               legacy_hits == [], str(len(legacy_hits)))

        # ------------------------------------------------------------
        # [7] 时延（官方：全链路会话不超过 1 分钟）
        # ------------------------------------------------------------
        print("\n[7] 时延（官方硬约束：全链路请求会话 ≤ 60s）")
        worst = max(counting.calls) if counting.calls else 0.0
        avg = sum(counting.calls) / len(counting.calls) if counting.calls else 0.0
        _check(f"★ 单次编码最慢 {worst:.3f}s ≪ 60s",
               worst < 60.0, f"{worst:.3f}")
        print(f"  编码 {len(counting.calls)} 次：最慢 {worst:.3f}s / 均值 {avg:.3f}s")
        print(f"  迁移总耗时 {elapsed:.2f}s（含每条 {CALL_INTERVAL_SECONDS}s 限流间隔）")

    await engine.dispose()

    # ------------------------------------------------------------
    # 报告落盘
    # ------------------------------------------------------------
    payload = {
        "backend": "内存 SQLite（无副作用）",
        "legacy": {"model": HashEmbeddingService.name, "dimension": legacy.dimension,
                   "documents": len(DOCS), "chunks": chunk_total},
        "target": {"model": write_inner.name, "dimension": write_inner.dimension,
                   "role": ROLE_DOCUMENT},
        "result": result,
        "rerun": again,
        "retrieval": hits_report,
        "legacy_retriever_hits": len(legacy_hits),
        "latency": {
            "calls": len(counting.calls),
            "worst_seconds": round(worst, 4),
            "avg_seconds": round(avg, 4),
            "limit_seconds": 60.0,
            "total_seconds": round(elapsed, 3),
            "pace_seconds": CALL_INTERVAL_SECONDS,
        },
        "checks": {"passed": _PASSED, "failed": _FAILED},
    }
    out = BACKEND_DIR / "scripts" / "rag_embedding_migration_stats.json"
    with open(out, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    print(f"\n报告: {out}")

    print("\n" + "=" * 74)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(_main()) else 1)
