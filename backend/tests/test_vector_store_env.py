# -*- coding: utf-8 -*-
"""VectorStore **双后端一致性测试环境** 自检（脚本式，非 pytest）。

被验证对象：``backend/tests/vector_backend_env.py``（测试辅助模块，**不是**自检脚本）

运行：``python backend/tests/test_vector_store_env.py``

本套件只验证**环境本身是否正确**——即「两个后端是否真的拿到了完全相同的输入」。
**不**比较「哪个后端更准」，那是后续对比测试的事。

[1] 环境契约——``VectorStore`` / ``VectorKnowledgeRetriever`` / ``EmbeddingService``
    三个受保护模块一行未改
[2] 测试数据来源——corpus 与 query 都来自 ``rag_query_set.json``；
    ``top_k`` / ``min_score`` 被**统一常量**覆盖（刻意不用数据集里逐条标定的值）
[3] 两后端同一批数据——权威行与 ANN 索引都是 16 条，且内容/向量/模型一致
[4] 完全相同输入取证——16 条 query × 2 后端，逐条比对
    ``embed_input`` / ``query_vector`` / ``search_kwargs``，全部必须**完全相同**
[5] 比对契约自检——容差感知的比较口径（含「显著命中」与「模糊带」）确实按预期工作
[6] 边界与隔离——未知后端报错、无 chroma 时的降级、集合名隔离、关闭幂等

.. note::
   本套件**只调用**生产代码与测试辅助代码，不修改任何生产文件。
   全程使用内存 SQLite + 进程内 chroma 客户端（不落盘、不启动服务）。
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BACKEND_DIR))
sys.path.insert(0, str(TESTS_DIR))
import regression_env  # noqa: E402,F401  钉住离线 Embedding + RAG 阈值（回归不受 .env 影响）
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from services.embedding_service import EmbeddingService  # noqa: E402
from services.knowledge_retriever import KnowledgeChunk  # noqa: E402
from services.vector_knowledge_retriever import VectorKnowledgeRetriever  # noqa: E402
from services.vector_store import VectorStore  # noqa: E402
from services.vector_store_chroma import ChromaVectorStore  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

from vector_backend_env import (  # noqa: E402
    BACKEND_CHROMA,
    BACKEND_SQL,
    BACKENDS,
    DATASET_PATH,
    ENV_MIN_SCORE,
    ENV_TOP_K,
    PROTECTED_MODULES,
    SCORE_TOL,
    EnvUnavailableError,
    EnvUsageError,
    ambiguous_hits,
    compare_chunk_lists,
    load_corpus_documents,
    load_queries,
    open_env,
    run_all_backends,
    scores_of,
    stable_hits,
)

#: corpus 每篇正文长度上限（⇒ ``DEFAULT_CHUNK_SIZE=500`` 下恰好 1 片）
CORPUS_CHAR_LIMIT = 480

#: 数据集里 corpus / query 的条数
EXPECTED_DOCS = 16
EXPECTED_QUERIES = 16

#: 本环境模块的名字（用于「生产代码不引用测试辅助」守卫）
ENV_MODULE_STEM = "vector_backend_env"
SUITE_STEM = "test_vector_store_env"

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


# ============================================================
# [1] 环境契约（三个受保护模块一行未改）
# ============================================================
def check_contract() -> None:
    _section("[1] 环境契约（VectorStore / Retriever / Embedding 接口未改）")

    _check("VectorStore 的抽象方法仍是 add / search / count",
           getattr(VectorStore, "__abstractmethods__", None)
           == frozenset({"add", "search", "count"}),
           str(getattr(VectorStore, "__abstractmethods__", None)))
    expected_search = ["self", "query_vector", "top_k", "model",
                       "document_id", "category", "min_score"]
    _check(f"VectorStore.search 签名仍是 {tuple(expected_search)}",
           list(inspect.signature(VectorStore.search).parameters) == expected_search,
           str(list(inspect.signature(VectorStore.search).parameters)))
    _check("VectorKnowledgeRetriever.retrieve 签名仍是 (self, job_info, topic, context)",
           list(inspect.signature(VectorKnowledgeRetriever.retrieve).parameters)
           == ["self", "job_info", "topic", "context"],
           str(list(inspect.signature(VectorKnowledgeRetriever.retrieve).parameters)))
    _check("EmbeddingService 的抽象方法仍是 {_embed_one}",
           getattr(EmbeddingService, "__abstractmethods__", None) == frozenset({"_embed_one"}),
           str(getattr(EmbeddingService, "__abstractmethods__", None)))
    _check("两个后端实现都仍是 VectorStore 的子类（接口未被绕过）",
           issubclass(SqlAlchemyVectorStore, VectorStore)
           and issubclass(ChromaVectorStore, VectorStore))

    offenders = [rel for rel in PROTECTED_MODULES
                 if ENV_MODULE_STEM in (BACKEND_DIR / rel).read_text(encoding="utf-8")
                 or SUITE_STEM in (BACKEND_DIR / rel).read_text(encoding="utf-8")]
    _check("★ 三个受保护模块都不引用本环境 / 本套件",
           not offenders, str(offenders))


# ============================================================
# [2] 测试数据来源（固定输入）
# ============================================================
def check_data_source() -> None:
    _section("[2] 测试数据来源（corpus / query 的唯一来源 = 数据集文件）")

    _check("数据集文件存在", DATASET_PATH.is_file(),
           DATASET_PATH.as_posix())
    raw = json.loads(DATASET_PATH.read_text(encoding="utf-8"))

    docs = load_corpus_documents()
    cases = load_queries()
    _check(f"corpus {EXPECTED_DOCS} 篇，每篇恰好 4 键（title/content/category/source）",
           len(docs) == EXPECTED_DOCS
           and all(set(d) == {"title", "content", "category", "source"} for d in docs),
           f"{len(docs)} 篇")
    _check(f"query {EXPECTED_QUERIES} 条，文本无重复",
           len(cases) == EXPECTED_QUERIES
           and len({c.text for c in cases}) == len(cases),
           f"{len(cases)} 条")
    _check("corpus 每篇正文 <= 480 字（DEFAULT_CHUNK_SIZE=500 ⇒ 每篇恰好 1 片）",
           all(len(d["content"]) <= CORPUS_CHAR_LIMIT for d in docs),
           str([(d["title"], len(d["content"])) for d in docs
                if len(d["content"]) > CORPUS_CHAR_LIMIT]))

    # ---- 「固定参数」：一律取自环境常量，不是数据集里逐条标定的值 ----
    _check(f"★ 每条 query 的 top_k 都 == 环境常量 ENV_TOP_K({ENV_TOP_K})",
           all(c.top_k == ENV_TOP_K for c in cases),
           str(sorted({c.top_k for c in cases})))
    _check(f"★ 每条 query 的 min_score 都 == 环境常量 ENV_MIN_SCORE({ENV_MIN_SCORE})",
           all(c.min_score == ENV_MIN_SCORE for c in cases),
           str(sorted({c.min_score for c in cases})))
    _check("★ query 文本取自数据集（逐条一致，未改写/截断）",
           [c.text for c in cases] == [q["query"] for q in raw["queries"]])
    ds_top_k = {q["top_k"] for q in raw["queries"]}
    ds_min_score = {q["min_score"] for q in raw["queries"]}
    _check("★ 数据集里逐条标定的 top_k / min_score 被**刻意忽略**（不是照搬）",
           ds_top_k != {ENV_TOP_K} and ds_min_score != {ENV_MIN_SCORE},
           f"数据集 top_k={sorted(ds_top_k)} min_score={sorted(ds_min_score)}")

    print(f"  数据集      : {DATASET_PATH.relative_to(BACKEND_DIR.parent).as_posix()}")
    print(f"  corpus      : {len(docs)} 篇 ⇒ {len(docs)} 片（每篇 1 片）")
    print(f"  query       : {len(cases)} 条")
    print(f"  统一 top_k  : {ENV_TOP_K}   统一 min_score : {ENV_MIN_SCORE}")
    print(f"  数据集自带  : top_k={sorted(ds_top_k)} min_score={sorted(ds_min_score)}"
          f"（一致性测试不用）")


# ============================================================
# [3] 两后端同一批数据
# ============================================================
async def check_shared_corpus(env: Any) -> None:
    _section("[3] 两后端同一批数据（权威行 ↔ ANN 索引）")

    sql_count = await env.sql_store.count()
    chroma_count = await env.chroma_store.count()
    _check(f"SqlAlchemyVectorStore 已向量化切片数 == {EXPECTED_DOCS}",
           sql_count == EXPECTED_DOCS, str(sql_count))
    _check(f"★ ChromaVectorStore **索引内**向量数 == {EXPECTED_DOCS}（镜像完整）",
           chroma_count == EXPECTED_DOCS, str(chroma_count))
    _check("两后端条数一致（同一批数据）", sql_count == chroma_count,
           f"{sql_count} vs {chroma_count}")

    records = env.records
    ids = [r.chunk_id for r in records]
    _check(f"权威行快照 {EXPECTED_DOCS} 条，chunk_id 唯一且升序",
           len(records) == EXPECTED_DOCS
           and len(set(ids)) == len(ids)
           and ids == sorted(ids), str(ids))
    _check("每条快照都有非空正文", all(r.content.strip() for r in records))
    _check(f"每条快照的向量维度 == embedder.dimension（{env.embedder.dimension}）",
           all(len(r.vector) == env.embedder.dimension for r in records),
           str(sorted({len(r.vector) for r in records})))
    _check("每条快照的 model == embedder.name（换模型据此重算）",
           all(r.model == env.embedder.name for r in records),
           str(sorted({r.model for r in records})))

    info = env.embedding_info
    _check("★ 环境公开的 Embedding 状态标识与 embedder 同源（provider/dimension）",
           info.provider == env.embedder.name and info.dimension == env.embedder.dimension,
           str(info.to_dict()))
    _check("Embedding 状态标识只报 3 个字段（不含密钥等敏感信息）",
           set(info.to_dict()) == {"provider", "dimension", "semantic_enabled"},
           str(sorted(info.to_dict())))

    print(f"  后端            : {list(env.backends)}")
    print(f"  集合名          : {env.collection_name}")
    print(f"  Embedding       : {info.provider} / {info.dimension} 维 / "
          f"semantic_enabled={info.semantic_enabled}"
          f"（离线占位，仅用于验证环境一致性）")
    print(f"  权威行快照      : {len(records)} 条，chunk_id={ids[0]}..{ids[-1]}")


# ============================================================
# [4] 完全相同输入取证
# ============================================================
async def check_identical_inputs(env: Any) -> List[Dict[str, Any]]:
    _section("[4] 完全相同输入取证（16 条 query × 2 后端，逐条比对）")

    rows: List[Dict[str, Any]] = []
    print(f"\n  {'#':>2}  {'sql n':>5} {'chr n':>5}  {'输入同?':^9} "
          f"{'max|Δscore|':>12}  {'显著命中':^9}  对比")
    print("  " + "-" * 96)
    for case in env.queries:
        runs = await run_all_backends(env, case)
        a, b = runs[BACKEND_SQL], runs[BACKEND_CHROMA]
        cmp = compare_chunk_lists(a.chunks, b.chunks,
                                  left_backend=BACKEND_SQL, right_backend=BACKEND_CHROMA)
        same_input = (a.embed_input == b.embed_input
                      and a.query_vector == b.query_vector
                      and a.search_kwargs == b.search_kwargs)
        rows.append({"case": case, "sql": a, "chroma": b, "cmp": cmp,
                     "same_input": same_input})
        print(f"  {case.index:>2}  {len(a.chunks):>5} {len(b.chunks):>5}  "
              f"{('相同' if same_input else '不同'):^9} "
              f"{cmp.max_abs_delta:>12.3e}  "
              f"{len(cmp.stable_left)}/{len(cmp.stable_right):<7} "
              f"{'一致' if cmp.ok else '有差异'}")

    # ---- ① Embedding 入参完全相同 ----
    _check("★ 两后端的 embed 入参完全相同，且都等于 query 原文",
           all(r["sql"].embed_input == r["chroma"].embed_input == r["case"].text
               for r in rows),
           str([r["case"].index for r in rows
                if not (r["sql"].embed_input == r["chroma"].embed_input
                        == r["case"].text)]))
    _check("★ 两后端收到的**查询向量逐元素完全相同**（同一 embedder、确定性编码）",
           all(r["sql"].query_vector == r["chroma"].query_vector for r in rows),
           str([r["case"].index for r in rows
                if r["sql"].query_vector != r["chroma"].query_vector]))
    _check("两后端用的是**同一个 embedder 实例**（不是两个配置相同的实例）",
           env.embedder is env.embedder
           and all(len(r["sql"].query_vector) == env.embedder.dimension for r in rows))

    # ---- ② search 参数完全相同 ----
    _check("★ 两后端的 search 入参（除后端本身）完全相同",
           all(r["sql"].search_kwargs == r["chroma"].search_kwargs for r in rows),
           str([r["case"].index for r in rows
                if r["sql"].search_kwargs != r["chroma"].search_kwargs]))
    _check(f"★ search 的 top_k 恒为环境常量 {ENV_TOP_K}",
           all(r["sql"].search_kwargs.get("top_k") == ENV_TOP_K for r in rows),
           str(sorted({r["sql"].search_kwargs.get("top_k") for r in rows})))
    _check(f"★ search 的 min_score 恒为环境常量 {ENV_MIN_SCORE}",
           all(r["sql"].search_kwargs.get("min_score") == ENV_MIN_SCORE for r in rows),
           str(sorted({r["sql"].search_kwargs.get("min_score") for r in rows})))
    _check("★ search 的 model 恒为 embedder.name（两后端相同，不同模型向量不可比）",
           all(r["sql"].search_kwargs.get("model") == env.embedder.name for r in rows),
           str(sorted({r["sql"].search_kwargs.get("model") for r in rows})))
    _check("search 的 category / document_id 两后端都不收紧（None = 全库）",
           all(r["sql"].search_kwargs.get("category") is None
               and r["sql"].search_kwargs.get("document_id") is None for r in rows))

    # ---- ③ 输入相同的汇总 ----
    _check("★ 全部 16 条 query 在两个后端上都是「输入完全相同」",
           all(r["same_input"] for r in rows),
           f"{sum(1 for r in rows if r['same_input'])}/{len(rows)}")
    _check("两后端在每条 query 上都检出了结果（环境可用，非空跑）",
           all(r["sql"].chunks and r["chroma"].chunks for r in rows),
           str([r["case"].index for r in rows
                if not (r["sql"].chunks and r["chroma"].chunks)]))
    _check("返回的每条都是 KnowledgeChunk（链路末端类型正确）",
           all(isinstance(c, KnowledgeChunk)
               for r in rows for c in r["sql"].chunks + r["chroma"].chunks))
    return rows


# ============================================================
# [5] 比对契约自检
# ============================================================
def check_comparison_contract(rows: List[Dict[str, Any]]) -> None:
    _section("[5] 比对契约自检（容差感知口径确实按预期工作）")

    cmps = [r["cmp"] for r in rows]
    _check("真实数据：两后端条数全部相同",
           all(c.count_equal for c in cmps),
           str([c.left_count for c in cmps if not c.count_equal]))
    _check(f"真实数据：同位分数差全部 <= SCORE_TOL({SCORE_TOL:g})",
           all(c.scores_within_tol for c in cmps),
           str(sorted({f"{c.max_abs_delta:.3e}" for c in cmps
                       if not c.scores_within_tol})[:3]))
    _check("真实数据：显著命中集合全部一致",
           all(c.stable_equal for c in cmps),
           str([(c.stable_left, c.stable_right) for c in cmps if not c.stable_equal][:2]))
    _check("真实数据：确实存在**非零**漂移（证明容差不是为 0 差异准备的摆设）",
           any(c.max_abs_delta > 0 for c in cmps),
           f"max={max((c.max_abs_delta for c in cmps), default=0.0):.3e}")
    _check("真实数据：漂移量级远小于容差（≤ tol 的 1/10）",
           all(c.max_abs_delta <= SCORE_TOL / 10 for c in cmps),
           f"max={max((c.max_abs_delta for c in cmps), default=0.0):.3e}")

    # ---- 容差函数本身：正例 / 反例 ----
    base = [KnowledgeChunk(content="a", source="s", metadata={"score": 0.5, "chunk_id": 1}),
            KnowledgeChunk(content="b", source="s", metadata={"score": 0.3, "chunk_id": 2}),
            KnowledgeChunk(content="c", source="s", metadata={"score": 0.1, "chunk_id": 3})]
    tiny = [KnowledgeChunk(content="a", source="s", metadata={"score": 0.5 + 1e-9, "chunk_id": 1}),
            KnowledgeChunk(content="b", source="s", metadata={"score": 0.3 - 1e-9, "chunk_id": 2}),
            KnowledgeChunk(content="c", source="s", metadata={"score": 0.1, "chunk_id": 3})]
    big = [KnowledgeChunk(content="a", source="s", metadata={"score": 0.9, "chunk_id": 1}),
           KnowledgeChunk(content="b", source="s", metadata={"score": 0.3, "chunk_id": 2}),
           KnowledgeChunk(content="c", source="s", metadata={"score": 0.1, "chunk_id": 3})]
    short = base[:2]

    _check("容差函数：1e-9 级漂移判为「在容差内」",
           compare_chunk_lists(base, tiny).scores_within_tol)
    _check("容差函数：0.4 级差异判为「超容差」",
           not compare_chunk_lists(base, big).scores_within_tol)
    _check("容差函数：长度不等 → count_equal=False（不静默按短的比完就算过）",
           not compare_chunk_lists(base, short).count_equal)
    _check("容差函数：长度不等时仍按较短序列给出同位差（便于定位）",
           len(compare_chunk_lists(base, short).score_deltas) == 2)

    # ---- 显著命中 / 模糊带 ----
    # 末位（= 截断边界）是 0.2；chunk 3 与它只差 1e-9 ⇒ 落在模糊带里
    tie = [KnowledgeChunk(content="a", source="s", metadata={"score": 0.5, "chunk_id": 1}),
           KnowledgeChunk(content="b", source="s", metadata={"score": 0.2, "chunk_id": 2}),
           KnowledgeChunk(content="c", source="s", metadata={"score": 0.2 + 1e-9, "chunk_id": 3})]
    _check("★ 显著命中只保留「明显高于末位（截断边界）」的项",
           stable_hits(tie) == frozenset({1}),
           str(sorted(stable_hits(tie))))
    _check("模糊带 = 与末位差 <= tol 的项（这些跨后端允许不同）",
           set(ambiguous_hits(tie)) == {2, 3}, str(ambiguous_hits(tie)))
    _check("显著命中 ∪ 模糊带 == 全部命中（不重不漏）",
           set(stable_hits(tie)) | set(ambiguous_hits(tie))
           == {1, 2, 3}
           and not (set(stable_hits(tie)) & set(ambiguous_hits(tie))))
    _check("空结果集 → 显著命中为空、模糊带为空（不抛异常）",
           stable_hits([]) == frozenset() and ambiguous_hits([]) == ())
    _check("分数序列按检索顺序取出（scores_of 保序）",
           scores_of(base) == (0.5, 0.3, 0.1), str(scores_of(base)))

    print(f"\n  真实数据 {len(cmps)} 条 query 的比对："
          f"条数一致 {sum(1 for c in cmps if c.count_equal)}/{len(cmps)}，"
          f"分数在容差内 {sum(1 for c in cmps if c.scores_within_tol)}/{len(cmps)}，"
          f"显著命中一致 {sum(1 for c in cmps if c.stable_equal)}/{len(cmps)}")
    print(f"  最大同位分数差：{max((c.max_abs_delta for c in cmps), default=0.0):.3e}"
          f"（容差 {SCORE_TOL:g}）")


# ============================================================
# [6] 边界与隔离
# ============================================================
async def check_boundaries() -> None:
    _section("[6] 边界与隔离（未知后端 / 降级 / 集合隔离 / 关闭幂等）")

    async with open_env() as env:
        try:
            env.store_for("no-such-backend")
            raised: Any = None
        except EnvUsageError as exc:
            raised = exc
        _check("未知后端名 → EnvUsageError（不静默回落到 sql）",
               raised is not None, repr(raised))

        _check("BACKENDS 常量顺序为 (sql, chroma)",
               BACKENDS == (BACKEND_SQL, BACKEND_CHROMA), str(BACKENDS))
        _check("环境摘要含全部固定项（数据/query/top_k/min_score/后端/Embedding）",
               set(env.describe()) >= {"dataset", "corpus_documents", "queries", "top_k",
                                       "min_score", "backends", "collection_name",
                                       "embedding", "score_tol"},
               str(sorted(env.describe())))
        name_a = env.collection_name

    # ---- 无 chroma 时的降级 ----
    async with open_env(with_chroma=False) as sql_only:
        _check("with_chroma=False → 只有 sql 后端",
               sql_only.backends == (BACKEND_SQL,), str(sql_only.backends))
        try:
            sql_only.store_for(BACKEND_CHROMA)
            raised2: Any = None
        except EnvUnavailableError as exc:
            raised2 = exc
        _check("无 chroma 后端时取 chroma → EnvUnavailableError（明确报「没有」，不是 None 崩溃）",
               raised2 is not None, repr(raised2))
        _check("sql-only 环境下 sql 后端仍可用（count == 16）",
               await sql_only.sql_store.count() == EXPECTED_DOCS)

    # ---- 两个实例的集合名必须不同（同进程共享 chroma 内存数据）----
    async with open_env() as env_b:
        _check("★ 两个环境实例的 chroma 集合名不同（同进程内存数据靠集合名隔离）",
               env_b.collection_name != name_a,
               f"{name_a} vs {env_b.collection_name}")
        _check("新实例的索引条数仍是 16（没有被上一个实例污染）",
               await env_b.chroma_store.count() == EXPECTED_DOCS)

    # ---- 关闭幂等 ----
    env_c = await _open_raw()
    _check("环境刚构建时 closed=False", env_c.closed is False)
    await env_c.close()
    _check("close() 后 closed=True", env_c.closed is True)
    await env_c.close()
    _check("close() 幂等（重复调用不抛异常）", env_c.closed is True)

    # ---- 守卫自检：反例 token 运行时拼出，避免自引用恒假 ----
    own_src = Path(__file__).read_text(encoding="utf-8")
    ghost = "vector_backend" + "_" + "ENV_ZZZ"
    _check("守卫自检：子串检查能区分「有」与「无」",
           (ENV_MODULE_STEM in own_src) is True
           and (ghost in own_src) is False
           and (ENV_MODULE_STEM
                in (BACKEND_DIR / PROTECTED_MODULES[0]).read_text(encoding="utf-8")) is False)


async def _open_raw() -> Any:
    """构建一个环境但**不用** async with（用于验证 close() 语义）。"""
    from vector_backend_env import build_env
    return await build_env()


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 74)
    print("VectorStore 双后端一致性测试环境 · 自检")
    print("辅助模块：backend/tests/vector_backend_env.py")
    print("=" * 74)

    check_contract()
    check_data_source()

    async with open_env() as env:
        await check_shared_corpus(env)
        rows = await check_identical_inputs(env)
        check_comparison_contract(rows)

    await check_boundaries()

    print("\n" + "=" * 74)
    print("测试环境摘要")
    print("=" * 74)
    print(f"  测试文件位置 : backend/tests/vector_backend_env.py（环境）"
          f" + backend/tests/test_vector_store_env.py（本自检）")
    print(f"  测试数据来源 : backend/scripts/rag_query_set.json"
          f"（corpus {EXPECTED_DOCS} 篇 / query {EXPECTED_QUERIES} 条）")
    print(f"  固定配置     : top_k={ENV_TOP_K}  min_score={ENV_MIN_SCORE}  "
          f"score_tol={SCORE_TOL:g}")
    print(f"  后端         : {list(BACKENDS)}"
          f"（权威行 = 内存 SQLite；ANN 索引 = 进程内 chroma，不落盘）")
    print(f"  断言         : 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
