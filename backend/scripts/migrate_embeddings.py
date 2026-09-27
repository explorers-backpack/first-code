# -*- coding: utf-8 -*-
"""知识库向量迁移 CLI：把既有 ``KnowledgeChunk`` 的向量重算成**当前真实 Embedding**。

用法
----
.. code-block:: bash

    # 1) 先看要迁移多少条（不编码、不写库、不花配额）
    python scripts/migrate_embeddings.py --provider spark --dry-run

    # 2) 真跑（必须显式 --yes，避免手滑把生产库跑掉）
    python scripts/migrate_embeddings.py --provider spark --yes --batch-size 8 --pace 3

    # 3) 只重跑上一次失败的切片（精确恢复，不重跑已成功的）
    python scripts/migrate_embeddings.py --provider spark --yes \\
        --retry-failed scripts/embedding_migration_stats.json

    # 4) 只迁一个文档 / 先小样本试跑
    python scripts/migrate_embeddings.py --provider spark --yes --document-id 3 --limit 20

    # 5) 换了 Embedding **维度**且在用 chroma 后端时：先丢掉派生 ANN 索引
    #    （索引是派生数据，会按新维度重建；不丢会因 HNSW 单维度限制直接报错）
    python scripts/migrate_embeddings.py --provider spark --yes --reset-index

为什么必须有 ``--yes``
----------------------
本脚本**直接改写生产库的向量列**。项目约定「绝不静默清理 / 静默改写」，
因此真跑必须显式确认；只想看看就加 ``--dry-run``。两者必须二选一。

安全闸：拒绝用「离线占位」跑迁移
--------------------------------
一个 ``EMBEDDING_*`` 都不配时 ``default_embedder()`` 返回 ``HashEmbeddingService``
（``semantic_enabled=False``，**无语义**）。用它跑迁移会把整库向量写成哈希散列、
却把 ``embedding_model`` 记成真模型名——**比不迁移更糟**（检索会返回噪声）。
因此脚本在开工前校验 ``semantic_enabled``，不是语义模型就直接拒绝
（真要跑请显式 ``--allow-offline``，只建议在测试库上）。

执行统计
--------
统计落盘到 ``scripts/embedding_migration_stats.json``（**不含数据库口令**），含：
迁移前后按 ``(embedding_model, embedding_dim)`` 的分布、耗时、完整失败明细。
退出码：``0`` = 无失败；``1`` = 有失败（可用 ``--retry-failed`` 续跑）；
``2`` = 参数问题（没给 ``--yes``/``--dry-run`` 等）；``3`` = 安全闸拦下。
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
from models import KnowledgeChunk  # noqa: E402
from services.embedding_provider import (  # noqa: E402
    ENV_PROVIDER,
    ROLE_DOCUMENT,
    describe_embedding,
)
from services.knowledge_embedding_migration import (  # noqa: E402
    MIGRATION_RESULT_FIELDS,
    EmbeddingMigration,
)
from services.knowledge_rag import default_embedder, resolve_vector_backend  # noqa: E402

#: 统计文件默认落点（与既有 rag_*.json 同目录）。
DEFAULT_STATS_PATH = os.path.join(BACKEND_DIR, "scripts", "embedding_migration_stats.json")

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
    except Exception:  # noqa: BLE001 - 连不上的字符串原样返回也比崩掉好
        return "<unparsable>"
    if not parts.password:
        return url
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    return urlunsplit(parts._replace(netloc=f"{parts.username}:***@{host}"))


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


async def _distribution(db: Any) -> List[Dict[str, Any]]:
    """按 ``(embedding_model, embedding_dim)`` 统计切片数（迁移前后各跑一次）。"""
    rows = (await db.execute(
        select(
            KnowledgeChunk.embedding_model,
            KnowledgeChunk.embedding_dim,
            func.count(),
        )
        .group_by(KnowledgeChunk.embedding_model, KnowledgeChunk.embedding_dim)
        .order_by(KnowledgeChunk.embedding_model, KnowledgeChunk.embedding_dim)
    )).all()
    return [
        {"model": model or "", "dim": dim, "count": int(count)}
        for model, dim, count in rows
    ]


def _load_failed_ids(path: str) -> List[int]:
    """从上次统计文件里取出失败切片的 id（``--retry-failed`` 用）。"""
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    result = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(result, dict):
        raise ValueError(f"{path} 里没有 result 段（不是本脚本产出的统计文件？）")
    failures = result.get("failures") or []
    ids: List[int] = []
    seen: set = set()
    for item in failures:
        chunk_id = (item or {}).get("chunk_id")
        if isinstance(chunk_id, int) and not isinstance(chunk_id, bool) and chunk_id not in seen:
            seen.add(chunk_id)
            ids.append(chunk_id)
    return ids


def _drop_chroma_index() -> Dict[str, Any]:
    """丢掉派生的 ANN 索引（**只对 chroma 后端有意义**）。

    为什么需要：HNSW 索引只支持单一维度。换了 Embedding 模型（维度 2560）后，
    旧索引若还是 256 维，``store.add`` 会在**写权威行之前**就抛
    ``VectorStoreDimensionError``（这是刻意的：宁可整体失败，也不要出现
    「权威行已写、索引没镜像」的半截状态）。因此必须先丢掉旧索引，
    迁移时按新维度重建——索引是**派生数据**，权威副本始终在 MySQL。
    """
    from services.vector_store_chroma import DEFAULT_COLLECTION, open_client

    client = open_client()
    try:
        client.delete_collection(DEFAULT_COLLECTION)
        dropped = True
    except Exception as exc:  # noqa: BLE001 - 集合本来就不存在是正常情况
        dropped = False
        note = f"{type(exc).__name__}: {exc}"
    else:
        note = ""
    return {"collection": DEFAULT_COLLECTION, "dropped": dropped, "note": note}


# ============================================================
# CLI
# ============================================================
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="migrate_embeddings.py",
        description="把知识库既有切片的向量重算成当前真实 Embedding（切片正文不变）",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="只扫描与判定，不编码、不写库")
    parser.add_argument("--yes", action="store_true",
                        help="确认真跑（会改写生产库向量列）")
    parser.add_argument("--provider", default=None,
                        help=f"显式指定 {ENV_PROVIDER}（如 spark）；缺省沿用环境变量")
    parser.add_argument("--document-id", type=int, default=None,
                        help="只迁移该文档的切片")
    parser.add_argument("--limit", type=int, default=None,
                        help="最多扫描多少片（按主键升序取前 N 片）")
    parser.add_argument("--batch-size", type=int, default=8,
                        help="写回批大小（一批 = 一次 commit = 一个失败面），默认 8")
    parser.add_argument("--pace", type=float, default=0.0,
                        help="每条编码之间的间隔秒数（上游限流时用，讯飞实测 3 秒稳定）")
    parser.add_argument("--retries", type=int, default=0,
                        help="单条编码的重试次数（不含首次），默认 0")
    parser.add_argument("--retry-delay", type=float, default=1.0,
                        help="重试间隔秒数，默认 1.0")
    parser.add_argument("--max-failures", type=int, default=None,
                        help="失败片数达到该值就停止后续批次（默认不限）")
    parser.add_argument("--retry-failed", default=None, metavar="STATS_JSON",
                        help="只重跑指定统计文件里失败的切片 id")
    parser.add_argument("--reset-index", action="store_true",
                        help="迁移前丢掉派生 ANN 索引（换维度且用 chroma 后端时必须）")
    parser.add_argument("--allow-offline", action="store_true",
                        help="允许用离线占位（无语义）跑迁移——仅建议测试库")
    parser.add_argument("--stats", default=DEFAULT_STATS_PATH,
                        help=f"统计输出路径，默认 {DEFAULT_STATS_PATH}")
    return parser


async def run(args: argparse.Namespace) -> int:
    # ---- 0. 参数自检：真跑与试跑必须二选一 ----
    if not args.yes and not args.dry_run:
        print("[!] 这是会改写生产库向量列的迁移脚本。", file=sys.stderr)
        print("    先看规模：  --dry-run", file=sys.stderr)
        print("    确认真跑：  --yes", file=sys.stderr)
        return EXIT_USAGE
    if args.yes and args.dry_run:
        print("[!] --yes 与 --dry-run 不能同时给（一个写库、一个不写）。", file=sys.stderr)
        return EXIT_USAGE

    # ---- 1. 选 Embedding 实现（显式 opt-in；本脚本就是那个显式动作）----
    if args.provider:
        os.environ[ENV_PROVIDER] = str(args.provider).strip()
    embedder = default_embedder(role=ROLE_DOCUMENT)
    info = describe_embedding(embedder)
    # ``EmbeddingInfo`` 只有三键（provider / dimension / semantic_enabled），
    # 其中 ``provider`` **就是** ``embedder.name``（写进 embedding_model 的那个值）。
    # 模型名单独取一次，是为了让日志读起来更直白。
    embedder_name = str(getattr(embedder, "name", "") or info.provider)
    embedder_dim = int(getattr(embedder, "dimension", 0) or 0)

    backend = resolve_vector_backend()

    print("=" * 72)
    print("知识库向量迁移")
    print("=" * 72)
    print(f"数据库      : {_redact(DATABASE_URL)}")
    print(f"向量后端    : {backend}")
    print(f"Embedding   : provider={info.provider} dim={info.dimension} "
          f"semantic={info.semantic_enabled}")
    print(f"角色(模式)  : {ROLE_DOCUMENT}（文档向量化模式）")
    print(f"目标模型    : {embedder_name} / 目标维度 {embedder_dim}")
    print(f"批大小      : {args.batch_size}    间隔 {args.pace}s    重试 {args.retries}")
    print(f"模式        : {'DRY-RUN（不写库）' if args.dry_run else '真跑（写库）'}")
    print("-" * 72)

    # ---- 2. 安全闸：拒绝用离线占位跑迁移 ----
    if not info.semantic_enabled and not args.allow_offline:
        print("[X] 当前 Embedding 是**离线占位**（无语义）。用它迁移会把整库向量写成", file=sys.stderr)
        print("    哈希散列、却把 embedding_model 记成模型名——比不迁移更糟。", file=sys.stderr)
        print("    请用 --provider spark（或先在 backend/.env 里配好 EMBEDDING_*）。", file=sys.stderr)
        return EXIT_SAFETY

    # ---- 3. 可选：丢掉派生 ANN 索引 ----
    index_reset: Optional[Dict[str, Any]] = None
    if args.reset_index:
        if backend == "chroma":
            index_reset = _drop_chroma_index()
            print(f"派生索引    : {index_reset}")
        else:
            index_reset = {"collection": None, "dropped": False,
                           "note": f"后端 {backend} 的索引就是权威行本身，无需重置"}
            print(f"派生索引    : {index_reset['note']}")

    # ---- 4. 指定「只重跑失败片」 ----
    chunk_ids: Optional[List[int]] = None
    if args.retry_failed:
        chunk_ids = _load_failed_ids(args.retry_failed)
        print(f"重跑失败片  : {len(chunk_ids)} 条（来自 {args.retry_failed}）")
        if not chunk_ids:
            print("  └ 上次统计里没有失败切片，无事可做。")

    started = time.monotonic()
    async with async_session() as db:
        before = await _distribution(db)

        def _progress(done: int, total: int) -> None:
            print(f"  ... 已处理 {done}/{total} 片", flush=True)

        migration = EmbeddingMigration(
            db,
            embedder=embedder,
            batch_size=args.batch_size,
            pace_seconds=args.pace,
            retries=args.retries,
            retry_delay=args.retry_delay,
            max_failures=args.max_failures,
            dry_run=args.dry_run,
            progress=None if args.dry_run else _progress,
        )
        result = await migration.run(
            document_id=args.document_id, chunk_ids=chunk_ids, limit=args.limit
        )
        after = await _distribution(db)

    elapsed = time.monotonic() - started
    attempted = result["migrated_chunks"] + result["failed_chunks"]
    payload = {
        "generated_at": _now_iso(),
        "database": _redact(DATABASE_URL),
        "vector_backend": backend,
        "embedding": {
            "provider": info.provider,
            "name": embedder_name,
            "dimension": info.dimension,
            "declared_dimension": embedder_dim,
            "semantic_enabled": info.semantic_enabled,
        },
        "role": ROLE_DOCUMENT,
        "options": {
            "dry_run": args.dry_run,
            "document_id": args.document_id,
            "limit": args.limit,
            "batch_size": args.batch_size,
            "pace_seconds": args.pace,
            "retries": args.retries,
            "retry_delay": args.retry_delay,
            "max_failures": args.max_failures,
            "retry_failed_from": args.retry_failed,
            "reset_index": index_reset,
        },
        "timing": {
            "seconds": round(elapsed, 3),
            "attempted_chunks": attempted,
            "per_chunk_seconds": round(elapsed / attempted, 4) if attempted else None,
        },
        "distribution_before": before,
        "distribution_after": after,
        "result": result,
    }

    # 契约自检：报告键集合必须恒定（写文件前先钉死，避免统计文件悄悄变形状）
    if tuple(result.keys()) != MIGRATION_RESULT_FIELDS:
        print("[X] 迁移报告键集合与 MIGRATION_RESULT_FIELDS 不一致，拒绝写统计文件。",
              file=sys.stderr)
        return EXIT_USAGE

    with open(args.stats, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)

    print("-" * 72)
    print(f"状态        : {result['status']}  (ok={result['ok']}, stage={result['stage']})")
    print(f"扫描 / 待迁移: {result['scanned_chunks']} / {result['pending_chunks']}")
    print(f"已迁移      : {result['migrated_chunks']}")
    print(f"跳过(幂等)  : {result['skipped_chunks']}")
    print(f"失败        : {result['failed_chunks']}"
          + ("（已提前停止）" if result["stopped_early"] else ""))
    print(f"批次        : {result['batches']}    耗时 {elapsed:.2f}s")
    if result["failures"]:
        print("失败样例    :")
        for item in result["failures"][:5]:
            print(f"  - chunk#{item['chunk_id']} [{item['stage']}] {item['error']}")
        if len(result["failures"]) > 5:
            print(f"  ... 其余 {len(result['failures']) - 5} 条见统计文件")
    print(f"统计        : {args.stats}")
    print("=" * 72)

    await engine.dispose()
    return EXIT_HAS_FAILURES if result["failed_chunks"] else EXIT_OK


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
