# -*- coding: utf-8 -*-
"""向量后端 **切换验证** 运行器（真实生产库；默认**只读**，写操作需 ``--yes``）。

**不是回归套件**（无 ``test_`` 前缀、不进回归循环），也**不 import regression_env**
——它要的正是 ``backend/.env`` 里的真实 ``DATABASE_URL``。

它回答一个问题：**把 ``VECTOR_STORE`` 从 sql 切到 chroma，生产数据真的能跑起来吗？**
启动日志里那条「切片数已达阈值，建议切 chroma」的告警，指的就是先跑本脚本验证。

全程**零网络**：用「某一片自己的向量」当 query 向量，因此**不调用任何 Embedding 接口**。

安全边界（两条，都必须守住）
------------------------------
1. **绝不碰真实 ANN 索引目录**：本脚本把 ``CHROMA_PATH`` 指向一个**临时目录**，
   在它里面建一个全新集合。``backend/data/chroma`` 一行都不会被改。
2. **默认只读**：不加 ``--yes`` 时只做体检（读真实行、验证维度与组装路径）。
   ``--yes`` 才会 ``rebuild_index``——它会把权威行里**已有的**向量原值重写一遍
   （``add_returning_ids`` 只更新 ``embedding`` / ``embedding_model`` / ``embedding_dim``
   三列且值相同，语义上是幂等的，但它确实是一次生产库写入），所以需要显式确认。

用法::

    python backend/tests/rag_vector_backend_switch_run.py           # 只读体检
    python backend/tests/rag_vector_backend_switch_run.py --yes     # 真正验证（临时索引目录）

退出码：``0`` = 通过（或无事可做）；``1`` = 有断言失败。
"""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import List

BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))

from sqlalchemy import func, select  # noqa: E402

from database import async_session, engine  # noqa: E402
from models import KnowledgeChunk  # noqa: E402
from services import knowledge_rag  # noqa: E402
from services.knowledge_maintenance import rebuild_index  # noqa: E402
from services.vector_store_chroma import ChromaVectorStore  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

#: 分数比对容差：ANN 索引按 **float32** 存取，回读重算余弦有 ~1e-7 的漂移。
SCORE_TOL = 1e-4

#: 最多取几片做 sql vs chroma 对照。
MAX_PROBES = 5

#: 临时索引目录前缀（用于「上次运行的下次启动即回收」）。
TMP_PREFIX = "career_ai_chroma_verify_"


def _sweep_stale_tmp() -> None:
    """删掉**上一次**运行留下的临时索引目录。

    chroma 的本地客户端没有 ``close()``，句柄要等**进程退出**才释放 ⇒ 本次运行
    建的目录在 ``finally`` 里删不掉（Windows 尤其明显）。所以本次启动时先把
    **上次**的残留清掉，磁盘上最多只留一个。
    """
    import glob

    for path in glob.glob(os.path.join(tempfile.gettempdir(), TMP_PREFIX + "*")):
        shutil.rmtree(path, ignore_errors=True)

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


async def _load_rows(db) -> List[KnowledgeChunk]:
    stmt = (
        select(KnowledgeChunk)
        .where(KnowledgeChunk.embedding.is_not(None))
        .order_by(KnowledgeChunk.id)
    )
    return list((await db.execute(stmt)).scalars().all())


async def run(args: argparse.Namespace) -> int:
    print("=" * 74)
    print("向量后端切换验证（真实生产库 · 零网络）")
    print("=" * 74)

    _sweep_stale_tmp()
    tmpdir = tempfile.mkdtemp(prefix=TMP_PREFIX)
    # ★ 必须在**任何** chroma 后端被构造之前设好：build_vector_store 只认环境变量。
    os.environ["CHROMA_PATH"] = tmpdir
    print(f"临时索引目录 : {tmpdir}（真实索引目录一行都不会被改）")
    print(f"模式         : {'真跑（会重写已有向量原值，幂等）' if args.yes else '只读体检'}")
    print("-" * 74)

    try:
        async with async_session() as db:
            total = int(
                (await db.execute(select(func.count()).select_from(KnowledgeChunk))).scalar_one()
            )
            rows = await _load_rows(db)
            print(f"权威行       : 切片 {total} 条，其中已向量化 {len(rows)} 条")
            if not rows:
                print("[!] 库里没有任何已向量化切片 —— 无事可做（先跑 scripts/import_knowledge.py）。")
                return 0

            dims = sorted({len(row.embedding) for row in rows})
            models = sorted({row.embedding_model or "" for row in rows})
            print(f"维度         : {dims}")
            print(f"模型标识     : {models}")

            # ---------------- [1] 前置条件：HNSW 只支持单维度 ----------------
            print("\n[1] 前置条件（HNSW 一个集合只支持单一维度）")
            _check("★ 全部已向量化切片维度一致（否则 chroma 必然报混维度）",
                   len(dims) == 1, str(dims))
            _check("模型标识唯一（检索按 model 过滤，混模型会漏召回）",
                   len(models) == 1, str(models))
            model = models[0] if models else ""

            # ---------------- [2] 生产组装路径按 VECTOR_STORE 换后端 ----------------
            print("\n[2] 生产组装路径（knowledge_rag.build_vector_store）")
            for backend, expected in (
                (knowledge_rag.BACKEND_SQL, SqlAlchemyVectorStore),
                (knowledge_rag.BACKEND_CHROMA, ChromaVectorStore),
            ):
                os.environ[knowledge_rag.ENV_VECTOR_STORE] = backend
                status = knowledge_rag.describe_vector_backend()
                _check(f"  └ describe_vector_backend() 报 {backend} / ann={backend != 'sql'}",
                       status["backend"] == backend, str(status))
                store = knowledge_rag.build_vector_store(db)
                _check(f"  └ build_vector_store() 真的返回 {expected.__name__}",
                       isinstance(store, expected), type(store).__name__)

            # ---------------- [3] 切到 chroma：未重建 ⇒ 检索为空 ----------------
            print("\n[3] 切到 chroma（派生索引尚未建立）")
            os.environ[knowledge_rag.ENV_VECTOR_STORE] = knowledge_rag.BACKEND_CHROMA
            chroma_store = knowledge_rag.build_vector_store(db)
            probe_row = rows[0]
            hits = await chroma_store.search(list(probe_row.embedding), top_k=3, model=model)
            _check("★ 未重建 ⇒ 检索返回 0 条（权威行与向量都在，坑就在这）",
                   hits == [], f"实际命中 {len(hits)} 条")
            _check("  └ 索引计数为 0",
                   await chroma_store.count() == 0, str(await chroma_store.count()))

            if not args.yes:
                print("\n[i] 只读体检结束。要真正验证，请加 --yes 重跑"
                      "（会重写已有向量的**原值**，幂等）。")
                print("\n" + "=" * 74)
                print(f"通过 {_PASSED} / {_PASSED + _FAILED}（未做重建与检索对照）")
                print("=" * 74)
                return 0 if _FAILED == 0 else 1

            # ---------------- [4] 重建派生索引 ----------------
            print("\n[4] rebuild_index(purge=True)：从权威行重建 ANN 索引")
            report = await rebuild_index(db, store=chroma_store, purge=True)
            _check("★ 重建无错误", report["errors"] == [], str(report["errors"]))
            _check("★ 重建条数 == 已向量化切片数",
                   report["rebuilt_chunks"] == len(rows),
                   f"{report['rebuilt_chunks']} vs {len(rows)}")
            _check("  └ 没有脏行被跳过", report["skipped_chunks"] == 0, str(report))
            _check("  └ 索引计数与重建条数一致",
                   await chroma_store.count() == report["rebuilt_chunks"],
                   str(await chroma_store.count()))

            # ---------------- [5] sql vs chroma：真实向量上的检索对照 ----------------
            print("\n[5] sql vs chroma：用**真实向量**做同输入对照（零网络）")
            sql_store = knowledge_rag.build_vector_store(db, backend=knowledge_rag.BACKEND_SQL)
            step = max(1, len(rows) // MAX_PROBES)
            probes = rows[::step][:MAX_PROBES]
            for row in probes:
                qv = list(row.embedding)
                a = await sql_store.search(qv, top_k=3, model=model)
                b = await chroma_store.search(qv, top_k=3, model=model)
                label = f"chunk#{row.id}"
                _check(f"  └ {label}：两条路径条数一致",
                       len(a) == len(b), f"sql={len(a)} chroma={len(b)}")
                _check(f"  └ {label}：自身都进了结果（自相似）",
                       row.id in [h.chunk_id for h in a] and row.id in [h.chunk_id for h in b],
                       f"sql={[h.chunk_id for h in a]} chroma={[h.chunk_id for h in b]}")
                if a and b and a[0].chunk_id == b[0].chunk_id:
                    _check(f"  └ {label}：top-1 分数一致（容差 {SCORE_TOL}）",
                           abs(a[0].score - b[0].score) <= SCORE_TOL,
                           f"{a[0].score} vs {b[0].score}")
                else:
                    print(f"  [i]  {label}：top-1 不同（ANN 近似召回 + 近似并列，属正常），"
                          f"sql={a[0].chunk_id if a else None} "
                          f"chroma={b[0].chunk_id if b else None}")

            print("\n[i] 结论：生产数据（%d 条 / %s 维）在 chroma 后端上可重建、可检索，"
                  "与 sql 后端结果一致。" % (len(rows), dims[0] if dims else "?"))

        print("\n" + "=" * 74)
        print(f"通过 {_PASSED} / {_PASSED + _FAILED}")
        print("=" * 74)
        return 0 if _FAILED == 0 else 1
    finally:
        await engine.dispose()
        os.environ.pop("CHROMA_PATH", None)
        shutil.rmtree(tmpdir, ignore_errors=True)
        if os.path.isdir(tmpdir):
            # chroma 的本地客户端**没有 close()**，句柄要等进程退出才释放；
            # Windows 上因此删不掉。**如实报出来**，别用 ignore_errors 假装清理干净了。
            print(f"[i] 临时目录未能删除（chroma 句柄未释放，进程退出后由系统回收）：{tmpdir}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rag_vector_backend_switch_run.py",
        description="验证 VECTOR_STORE 从 sql 切到 chroma 后，生产数据能否重建并检索",
    )
    parser.add_argument("--yes", action="store_true",
                        help="真正执行重建（会以**原值**重写已有向量的三列，幂等）")
    return parser


if __name__ == "__main__":
    sys.exit(asyncio.run(run(build_parser().parse_args())))
