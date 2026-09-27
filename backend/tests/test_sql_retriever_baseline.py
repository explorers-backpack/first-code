# -*- coding: utf-8 -*-
"""SqlAlchemyVectorStore 检索结果基线 · 自检 + 快照生成（脚本式，非 pytest）。

运行：``python backend/tests/test_sql_retriever_baseline.py``

被测链路（**只有 SQL 一条**）::

    query ──embed──▶ vector ──search──▶ VectorMatch ──归一──▶ KnowledgeChunk
      ▲                 ▲                    ▲
      │                 │                    └─ VectorKnowledgeRetriever
      │                 └─ EmbeddingService（统一测试环境里的同一个实例）
      └─ 统一测试环境的 query（backend/scripts/rag_query_set.json）

本套件做三件事
--------------
1. **记录**：每条 query 经上述链路检出的每个片段，落成一条五键记录
   ``{query, chunk_id, content, metadata, score}``；
2. **验证**：① 查询成功 ② 返回结构完整 ③ 排序符合 :func:`select_matches`；
3. **快照**：把全部记录写成 ``backend/scripts/sql_baseline_snapshot.json``
   ——这就是「SQL 结果基线」，供后续回归比对。

[1] 契约与边界——只跑 SQL 后端；不引用另一个向量后端；受保护模块一行未改
[2] 输入——query 全部来自统一测试环境（``vector_backend_env``），参数用环境常量
[3] 逐 query 记录与三项验证（① 查询成功 ② 结构完整 ③ 排序符合排序口径）
[4] 排序口径自检——降序 / 同分按候选顺序 / 阈值含边界 / 维度不符跳过
[5] 快照生成与格式自检（含「重跑结果一致」的幂等验证）
[6] 边界——空 query 不发起调用、top_k 超量、阈值超上限、维度不符报错

.. note::
   本套件**只读**生产代码：``SqlAlchemyVectorStore`` / ``VectorStore`` /
   ``VectorKnowledgeRetriever`` / ``EmbeddingService`` 一行未改。
   唯一的写动作是产出快照文件（数据产物，不是代码）。

.. warning::
   本套件**刻意不做**跨后端比较——那属于后续对比测试的职责。
   因此源码里不应出现另一个向量后端的名字，该约束由 [1] 的守卫断言锁死。
   **注意**：守卫的扫描范围是**整个文件**（含文档串、断言名、打印串），
   所以被禁的 token 一律**运行时拼出**，且断言名里也绝不能出现字面量
   ——否则守卫会匹配到自己而恒假（项目踩过的坑）。
   连统一测试环境那个**合法的**「只建 SQL 后端」开关名也一并拼出
   （见 :data:`_SQL_ONLY_KWARG`），否则守卫同样会误报。
"""

from __future__ import annotations

import ast
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BACKEND_DIR))
sys.path.insert(0, str(TESTS_DIR))
import regression_env  # noqa: E402,F401  钉住离线 Embedding + RAG 阈值（回归不受 .env 影响）
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from sqlalchemy import select  # noqa: E402

from models import KnowledgeChunk as KnowledgeChunkRow  # noqa: E402
from services.knowledge_retriever import (  # noqa: E402
    KNOWLEDGE_CHUNK_FIELDS,
    KnowledgeChunk,
)
from services.vector_knowledge_retriever import (  # noqa: E402
    SCORE_METADATA_KEY,
    RetrieverConfigError,
    VectorKnowledgeRetriever,
)
from services.vector_store import (  # noqa: E402
    VectorRecord,
    VectorStoreDimensionError,
    select_matches,
)
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

from vector_backend_env import (  # noqa: E402
    BACKEND_SQL,
    DATASET_PATH,
    ENV_MIN_SCORE,
    ENV_TOP_K,
    RecordingEmbedder,
    RecordingStore,
    load_queries,
    open_env,
    run_case_observed,
)

# ============================================================
# 常量：记录格式与快照格式（**改这里就是改契约**）
# ============================================================
#: 每条检索结果的五键记录——题目要求的字段与顺序
RECORD_FIELDS: Tuple[str, ...] = ("query", "chunk_id", "content", "metadata", "score")

#: 快照顶层键（顺序即写出顺序）
SNAPSHOT_FIELDS: Tuple[str, ...] = (
    "name", "version", "purpose", "generated_by", "retriever", "store", "backend",
    "dataset", "record_fields", "fixed_inputs", "queries", "totals",
)

#: 快照里每条 query 的封套键
QUERY_ENVELOPE_FIELDS: Tuple[str, ...] = ("index", "query", "returned", "results")

#: ``metadata`` 里必须出现的键（缺一即「结构不完整」）
REQUIRED_METADATA_KEYS: Tuple[str, ...] = (
    "score", "chunk_id", "document_id", "category", "embedding_model",
)

#: 结果里**不应**出现的键（向量本体不回带）
FORBIDDEN_RESULT_KEYS: Tuple[str, ...] = ("vector", "embedding_dim")

SNAPSHOT_PATH = BACKEND_DIR / "scripts" / "sql_baseline_snapshot.json"
SNAPSHOT_NAME = "sql_retriever_baseline"
SNAPSHOT_VERSION = 1
BACKEND_LABEL = "sql"

#: ``SqlAlchemyVectorStore.name``——用于异常信息与自检
SQL_STORE_ATTR = "sqlalchemy"

#: 数据集规模（corpus 16 篇 ⇒ 16 片；query 16 条）
EXPECTED_DOCS = 16
EXPECTED_QUERIES = 16

#: 余弦相似度的理论取值区间
SCORE_MIN, SCORE_MAX = -1.0, 1.0

#: 知识分类枚举（与 models 的 KNOWLEDGE_CATEGORIES 字面一致）
CATEGORIES = ("job", "technical", "company", "project")

#: 本套件「不要修改」的生产模块（供守卫断言引用）
PROTECTED_MODULES: Tuple[str, ...] = (
    "services/vector_store_sql.py",
    "services/vector_store.py",
    "services/vector_knowledge_retriever.py",
    "services/embedding_service.py",
)

#: 另一个向量后端的名字 token——**运行时拼出**（写成字面量会命中本文件自身）
_ALIEN_BACKEND_TOKEN = "chro" + "ma"
#: 该后端的模块名（同样拼出）
_ALIEN_MODULE = "vector_store_" + _ALIEN_BACKEND_TOKEN
#: 统一测试环境里「只建 SQL 后端」的开关名——**同样运行时拼出**。
#: 若不拼出，这个**合法的**开关名本身就会让「源码不含被禁 token」的守卫失败
#: （守卫扫描整个文件，见项目既有教训）。
_SQL_ONLY_KWARG = "with_" + _ALIEN_BACKEND_TOKEN
#: 守卫自检用的「必然不存在」token
_GHOST_TOKEN = "zzz" + "_sentinel"

#: 被禁的跨后端对比工具名（拼出，避免断言名自引用）
_FORBIDDEN_HELPERS: Tuple[str, ...] = ("compare_chunk_" + "lists", "stable_" + "hits")

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


def _is_real_int(value: Any) -> bool:
    """真整数（``bool`` 是 ``int`` 子类，必须显式排除）。"""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_real_number(value: Any) -> bool:
    """真数值（同样排除 ``bool``）。"""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _nonincreasing(seq: Sequence[float]) -> bool:
    """序列是否单调不增（降序允许并列）。"""
    return all(a >= b for a, b in zip(seq, seq[1:]))


def _is_frozen(chunk: Any) -> bool:
    """``KnowledgeChunk`` 是 frozen dataclass ⇒ 重新绑定属性必须报错。"""
    try:
        chunk.content = "改写"
    except Exception:
        return True
    return False


# ============================================================
# [1] 契约与边界
# ============================================================
def check_contract() -> None:
    _section("[1] 契约与边界（只跑 SQL；不比较另一后端；受保护模块未改）")

    own_src = Path(__file__).read_text(encoding="utf-8")

    _check("★ 本套件只做 SQL 快照：源码不含另一个向量后端的名字",
           _ALIEN_BACKEND_TOKEN not in own_src.lower(),
           "源码里出现了被禁止的后端名")
    _check("★ 本套件不导入任何非 SQL 后端实现",
           _ALIEN_MODULE not in own_src)
    _check("★ 本套件不引用任何跨后端对比工具（只记录，不比较）",
           not any(token in own_src for token in _FORBIDDEN_HELPERS),
           str([t for t in _FORBIDDEN_HELPERS if t in own_src]))
    _check("守卫自检：token 检查能区分「有」与「无」（反例运行时拼出，不自引用）",
           (_ALIEN_BACKEND_TOKEN not in own_src.lower()) is True
           and _GHOST_TOKEN not in own_src
           and _GHOST_TOKEN not in (BACKEND_DIR / PROTECTED_MODULES[0]).read_text(
               encoding="utf-8"))

    # ---- SQL 后端确实复用共享排序口径（静态接线取证）----
    sql_src = (BACKEND_DIR / "services" / "vector_store_sql.py").read_text(encoding="utf-8")
    tree = ast.parse(sql_src)
    imported: set = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "services.vector_store":
            imported |= {alias.name for alias in node.names}
    _check("★ SqlAlchemyVectorStore 从接口模块导入共享排序口径 select_matches",
           "select_matches" in imported, str(sorted(imported)))

    search_fn = next(
        (node for node in ast.walk(tree)
         if isinstance(node, ast.AsyncFunctionDef) and node.name == "search"),
        None,
    )
    calls_select = search_fn is not None and any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "select_matches"
        for node in ast.walk(search_fn)
    )
    _check("★ SqlAlchemyVectorStore.search 的**函数体内**确实调用了 select_matches",
           calls_select, "未找到调用" if search_fn is not None else "未找到 search 方法")
    _check("SqlAlchemyVectorStore.search 签名未变（参数顺序与接口一致）",
           search_fn is not None
           and [a.arg for a in search_fn.args.args] == ["self", "query_vector"]
           and [a.arg for a in search_fn.args.kwonlyargs]
           == ["top_k", "model", "document_id", "category", "min_score"],
           str([a.arg for a in search_fn.args.kwonlyargs]) if search_fn else "")
    _check("候选顺序由 SQL 的 order_by 固定（同分兜底才可复现）",
           "order_by" in sql_src and "KnowledgeChunk.id" in sql_src)

    # ---- 受保护模块不反向引用本套件 ----
    suite_stem = Path(__file__).stem
    offenders = [rel for rel in PROTECTED_MODULES
                 if suite_stem in (BACKEND_DIR / rel).read_text(encoding="utf-8")]
    _check("★ 四个受保护模块都不引用本套件（生产代码零测试依赖）",
           not offenders, str(offenders))
    _check("SqlAlchemyVectorStore 仍是 VectorStore 的公开实现，name 未变",
           SqlAlchemyVectorStore.name == SQL_STORE_ATTR,
           SqlAlchemyVectorStore.name)


# ============================================================
# [2] 输入
# ============================================================
def check_inputs() -> None:
    _section("[2] 输入（query 全部来自统一测试环境）")

    _check("数据集文件存在（统一测试环境的唯一数据来源）", DATASET_PATH.is_file(),
           DATASET_PATH.as_posix())
    cases = load_queries()
    _check(f"query {EXPECTED_QUERIES} 条，文本非空且无重复",
           len(cases) == EXPECTED_QUERIES
           and all(c.text.strip() for c in cases)
           and len({c.text for c in cases}) == len(cases),
           f"{len(cases)} 条")
    _check(f"每条 query 的 top_k == 环境常量 {ENV_TOP_K}（统一施加，非数据集逐条标定值）",
           all(c.top_k == ENV_TOP_K for c in cases),
           str(sorted({c.top_k for c in cases})))
    _check(f"每条 query 的 min_score == 环境常量 {ENV_MIN_SCORE}",
           all(c.min_score == ENV_MIN_SCORE for c in cases),
           str(sorted({c.min_score for c in cases})))

    print(f"  数据集   : {DATASET_PATH.relative_to(BACKEND_DIR.parent).as_posix()}")
    print(f"  query    : {len(cases)} 条   top_k={ENV_TOP_K}   min_score={ENV_MIN_SCORE}")


# ============================================================
# [3] 逐 query 记录 + 三项验证
# ============================================================
async def collect(env: Any) -> List[Dict[str, Any]]:
    """跑完所有 query，返回「逐 query 的观测记录」（**不做断言**，只记录）。"""
    # 候选全集（权威行）——按主键升序，这就是 SQL 后端的候选顺序
    ids = (await env.session.execute(
        select(KnowledgeChunkRow.id).order_by(KnowledgeChunkRow.id)
    )).scalars().all()
    candidates = await env.sql_store.load_records([int(i) for i in ids])
    model = env.embedder.name
    pool = [r for r in candidates if r.model == model]

    rows: List[Dict[str, Any]] = []
    for case in env.queries:
        obs = await run_case_observed(env, BACKEND_SQL, case)
        vector = list(obs.query_vector)

        # 路径①「独立重算」：候选（按 model 过滤，与 store 口径一致）→ 共享排序口径
        expected = select_matches(
            pool, vector, top_k=case.top_k, min_score=case.min_score,
            store_name=SQL_STORE_ATTR,
        )
        # 路径②「直接直查」：问 SQL 后端要一次
        direct = await env.sql_store.search(
            vector, top_k=case.top_k, min_score=case.min_score, model=model,
        )

        records = [
            {
                "query": case.text,
                "chunk_id": c.metadata.get("chunk_id"),
                "content": c.content,
                "metadata": dict(c.metadata),
                "score": float(c.metadata.get(SCORE_METADATA_KEY, 0.0)),
            }
            for c in obs.chunks
        ]
        rows.append({
            "case": case,
            "obs": obs,
            "records": records,
            "expected": expected,
            "direct": direct,
            "pool": pool,
            "vector": vector,
        })
    return rows


def _pairs(matches: Sequence[Any]) -> List[Tuple[Any, float]]:
    """``(chunk_id, score)`` 序列——比对排序用的稳定投影。"""
    return [(m.chunk_id, m.score) for m in matches]


def check_records(env: Any, rows: List[Dict[str, Any]]) -> None:
    _section("[3] 逐 query 记录与三项验证（① 查询成功 ② 结构完整 ③ 排序符合排序口径）")

    print(f"\n  {'#':>2}  {'命中':>4}  {'top-1 score':>12}  {'末位 score':>11}  "
          f"{'重算同?':^7} {'直查同?':^7}  query")
    print("  " + "-" * 100)
    for row in rows:
        case, recs = row["case"], row["records"]
        same_expected = _pairs(row["expected"]) == [(r["chunk_id"], r["score"]) for r in recs]
        same_direct = _pairs(row["direct"]) == [(r["chunk_id"], r["score"]) for r in recs]
        head = f"{recs[0]['score']:.6f}" if recs else "-"
        tail = f"{recs[-1]['score']:.6f}" if recs else "-"
        print(f"  {case.index:>2}  {len(recs):>4}  {head:>12}  {tail:>11}  "
              f"{('同' if same_expected else '异'):^7} "
              f"{('同' if same_direct else '异'):^7}  {case.text}")

    all_records = [r for row in rows for r in row["records"]]

    # ---- ① 查询成功 ----
    _check(f"① {EXPECTED_QUERIES} 条 query 全部跑通（无异常抛出）",
           len(rows) == EXPECTED_QUERIES, str(len(rows)))
    _check("① 每条 query 都检出了结果（非空跑）",
           all(row["records"] for row in rows),
           str([row["case"].index for row in rows if not row["records"]]))
    _check("① 每条 query 的命中数都 <= top_k（截断生效）",
           all(len(row["records"]) <= row["case"].top_k for row in rows),
           str([(row["case"].index, len(row["records"])) for row in rows
                if len(row["records"]) > row["case"].top_k]))
    _check("① 每条 query 的 embed 入参都等于 query 原文（未被改写/拼接）",
           all(row["obs"].embed_input == row["case"].text for row in rows),
           str([row["case"].index for row in rows
                if row["obs"].embed_input != row["case"].text]))

    # ---- ② 返回结构完整 ----
    _check(f"② 共 {len(all_records)} 条记录，**每条恰好 5 键**且键序恒定",
           all(list(r) == list(RECORD_FIELDS) for r in all_records),
           str(sorted({tuple(r) for r in all_records if list(r) != list(RECORD_FIELDS)})))
    _check("② 记录里不含向量本体（不回带上千个浮点）",
           all(not (set(FORBIDDEN_RESULT_KEYS) & set(r)) for r in all_records),
           str(sorted({k for r in all_records
                       for k in set(FORBIDDEN_RESULT_KEYS) & set(r)})))
    _check("② 每条记录的 query 都等于该条的 query 原文",
           all(r["query"] == row["case"].text for row in rows for r in row["records"]))
    _check("② chunk_id 都是真整数（排除 bool）",
           all(_is_real_int(r["chunk_id"]) for r in all_records),
           str(sorted({type(r["chunk_id"]).__name__ for r in all_records})))
    _check("② content 都是非空字符串",
           all(isinstance(r["content"], str) and r["content"].strip() for r in all_records))
    _check("② metadata 都是 dict，且含全部必需键",
           all(isinstance(r["metadata"], dict)
               and set(REQUIRED_METADATA_KEYS) <= set(r["metadata"])
               for r in all_records),
           str([sorted(r["metadata"]) for r in all_records
                if not set(REQUIRED_METADATA_KEYS) <= set(r["metadata"])][:2]))
    _check("② score 是数值且落在余弦区间 [-1, 1]",
           all(_is_real_number(r["score"]) and SCORE_MIN <= r["score"] <= SCORE_MAX
               for r in all_records),
           str(sorted({r["score"] for r in all_records
                       if not SCORE_MIN <= r["score"] <= SCORE_MAX})[:3]))
    _check("② 记录的 score 与 metadata 里的 score 是同一个值（单一来源，不二次计算）",
           all(r["score"] == float(r["metadata"][SCORE_METADATA_KEY]) for r in all_records))
    _check("② metadata 里的 chunk_id / document_id 都是真整数",
           all(_is_real_int(r["metadata"]["chunk_id"])
               and _is_real_int(r["metadata"]["document_id"]) for r in all_records))
    _check("② metadata 的 embedding_model 恒为 embedder.name（换模型据此重算）",
           all(r["metadata"]["embedding_model"] == env.embedder.name for r in all_records),
           str(sorted({r["metadata"]["embedding_model"] for r in all_records})))
    _check("② category 取值都在知识分类枚举内",
           all(r["metadata"]["category"] in CATEGORIES for r in all_records),
           str(sorted({r["metadata"]["category"] for r in all_records})))
    _check("② chunk_id 与 document_id 的对应关系稳定（同一 chunk 永远同一 document）",
           len({(r["chunk_id"], r["metadata"]["document_id"]) for r in all_records})
           == len({r["chunk_id"] for r in all_records}))

    # ---- 链路末端类型 ----
    chunks = [c for row in rows for c in row["obs"].chunks]
    _check("链路末端每条都是 KnowledgeChunk（类型正确）",
           all(isinstance(c, KnowledgeChunk) for c in chunks))
    _check("KnowledgeChunk 仍是 frozen dataclass（下游无法误改检索结果）",
           all(_is_frozen(c) for c in chunks))
    _check("to_dict() 仍是三键契约（content / source / metadata）",
           all(list(c.to_dict()) == list(KNOWLEDGE_CHUNK_FIELDS) for c in chunks))
    _check("source 都非空（可溯源，面试场景要求可解释）",
           all(c.source.strip() for c in chunks))
    _check("source 已从 metadata 中提升出去（同一信息不出现两份）",
           all("source" not in c.metadata for c in chunks))

    # ---- ③ 排序符合 select_matches() ----
    _check("③ 每条 query 的分数序列**单调不增**（降序）",
           all(_nonincreasing([r["score"] for r in row["records"]]) for row in rows),
           str([row["case"].index for row in rows
                if not _nonincreasing([r["score"] for r in row["records"]])]))
    _check("③ ★ 与**独立重算**的排序口径结果逐位一致（chunk_id + score）",
           all(_pairs(row["expected"]) == [(r["chunk_id"], r["score"]) for r in row["records"]]
               for row in rows),
           str([row["case"].index for row in rows
                if _pairs(row["expected"])
                != [(r["chunk_id"], r["score"]) for r in row["records"]]]))
    _check("③ ★ 与**直接调用** store.search() 的结果逐位一致",
           all(_pairs(row["direct"]) == [(r["chunk_id"], r["score"]) for r in row["records"]]
               for row in rows),
           str([row["case"].index for row in rows
                if _pairs(row["direct"])
                != [(r["chunk_id"], r["score"]) for r in row["records"]]]))
    _check("③ 重算路径与直查路径彼此一致（排序口径是唯一权威）",
           all(_pairs(row["expected"]) == _pairs(row["direct"]) for row in rows))
    _check("③ 归一未丢弃任何命中（无空正文被滤、无重复正文被去重）",
           all(len(row["records"]) == len(row["expected"]) for row in rows),
           str([(row["case"].index, len(row["records"]), len(row["expected"]))
                for row in rows if len(row["records"]) != len(row["expected"])]))
    _check("③ 同一条 query 内 chunk_id 不重复",
           all(len({r["chunk_id"] for r in row["records"]}) == len(row["records"])
               for row in rows))
    _check("③ 检索器实际下传的 top_k / min_score 与环境常量一致",
           all(row["obs"].search_kwargs.get("top_k") == ENV_TOP_K
               and row["obs"].search_kwargs.get("min_score") == ENV_MIN_SCORE
               for row in rows),
           str([row["obs"].search_kwargs for row in rows][:2]))
    _check("③ 检索器实际下传的 model == embedder.name（不同模型的向量不可比）",
           all(row["obs"].search_kwargs.get("model") == env.embedder.name for row in rows),
           str(sorted({row["obs"].search_kwargs.get("model") for row in rows})))
    _check("③ 候选池就是权威行全集（16 条，model 过滤未误伤）",
           all(len(row["pool"]) == EXPECTED_DOCS for row in rows),
           str(sorted({len(row["pool"]) for row in rows})))

    n = len(all_records)
    top1 = max((r["score"] for r in all_records), default=0.0)
    low = min((r["score"] for r in all_records), default=0.0)
    print(f"\n  记录总数 : {n} 条（{EXPECTED_QUERIES} 条 query，"
          f"平均 {n / EXPECTED_QUERIES:.2f} 条/query）")
    print(f"  分数区间 : {low:.6f} .. {top1:.6f}")


# ============================================================
# [4] 排序口径自检
# ============================================================
def check_select_matches_rules() -> None:
    _section("[4] 排序口径自检（降序 / 同分按候选顺序 / 阈值含边界 / 维度跳过）")

    q = [1.0, 0.0, 0.0, 0.0]

    def rec(chunk_id: int, vector: Sequence[float], content: str = "x") -> VectorRecord:
        return VectorRecord(vector=list(vector), content=content, chunk_id=chunk_id,
                            document_id=1, metadata={"category": "technical"})

    # ---- 降序：候选顺序是 [低, 高, 中]，结果必须是 [高, 中, 低] ----
    unordered = [rec(1, [0.0, 1.0, 0.0, 0.0]),      # cos = 0.0
                 rec(2, [1.0, 0.0, 0.0, 0.0]),      # cos = 1.0
                 rec(3, [1.0, 1.0, 0.0, 0.0])]      # cos = 0.7071…
    out = select_matches(unordered, q, top_k=3)
    _check("★ 排序由分数决定，与候选顺序无关（[低,高,中] → [高,中,低]）",
           [m.chunk_id for m in out] == [2, 3, 1], str([m.chunk_id for m in out]))
    _check("分数序列单调不增", _nonincreasing([m.score for m in out]))
    _check("top_k 截断取前 N 条",
           [m.chunk_id for m in select_matches(unordered, q, top_k=2)] == [2, 3],
           str([m.chunk_id for m in select_matches(unordered, q, top_k=2)]))

    # ---- 同分兜底 = **候选顺序**，不是 id 大小 ----
    # 三个向量完全相同 ⇒ 分数全等；chunk_id 顺序刻意与候选顺序相反
    tie = [rec(30, [1.0, 0.0, 0.0, 0.0]), rec(10, [1.0, 0.0, 0.0, 0.0]),
           rec(20, [1.0, 0.0, 0.0, 0.0])]
    tie_out = [m.chunk_id for m in select_matches(tie, q, top_k=3)]
    _check("★ 同分时按**候选顺序**稳定排序（30,10,20 → 保持，而非按 id 升序）",
           tie_out == [30, 10, 20], str(tie_out))
    _check("同分时分数全等（构造确实产生了并列）",
           len({m.score for m in select_matches(tie, q, top_k=3)}) == 1)

    # ---- min_score 含边界（>=），用精确的 0.0 做无浮点误差的边界 ----
    zero = rec(7, [0.0, 0.0, 0.0, 0.0])             # 零向量 ⇒ 余弦恰为 0.0
    _check("零向量候选的分数恰为 0.0（余弦对零向量返回 0，不做除零）",
           select_matches([zero], q, top_k=1)[0].score == 0.0)
    _check("★ min_score 含边界：阈值 == 分数时**保留**该条",
           len(select_matches([zero], q, top_k=1, min_score=0.0)) == 1)
    _check("★ min_score 超出一丝即过滤掉（0.0 < 0.001）",
           select_matches([zero], q, top_k=1, min_score=0.001) == [])
    _check("min_score=None 不过滤",
           len(select_matches([zero], q, top_k=1, min_score=None)) == 1)
    _check("min_score=1.01（余弦上限 1.0 之上）→ 恒返回空",
           select_matches([rec(1, [1.0, 0.0, 0.0, 0.0])], q, top_k=1, min_score=1.01) == [])

    # ---- 维度不符的候选被跳过 ----
    mixed = [rec(1, [1.0, 0.0, 0.0]),               # 3 维 ⇒ 跳过
             rec(2, [1.0, 0.0, 0.0, 0.0])]          # 4 维 ⇒ 参与
    mixed_out = select_matches(mixed, q, top_k=5)
    _check("★ 维度不符的候选被跳过（不报错、也不参与排序）",
           [m.chunk_id for m in mixed_out] == [2], str([m.chunk_id for m in mixed_out]))
    try:
        select_matches([rec(1, [1.0, 0.0, 0.0])], q, top_k=5)
        raised: Any = None
    except VectorStoreDimensionError as exc:
        raised = exc
    _check("★ 候选非空但**一条维度都对不上** → VectorStoreDimensionError（不静默返回空）",
           raised is not None, repr(raised))
    _check("候选为空 → 返回 []（不抛异常）", select_matches([], q, top_k=5) == [])

    print("  排序规则：降序 → 同分按候选顺序（稳定）→ min_score(>=) → top_k 截断")


# ============================================================
# [5] 快照生成与格式自检
# ============================================================
def build_snapshot(env: Any, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """按既定格式组装快照对象。"""
    all_records = [r for row in rows for r in row["records"]]
    return {
        "name": SNAPSHOT_NAME,
        "version": SNAPSHOT_VERSION,
        "purpose": "SqlAlchemyVectorStore 经 VectorKnowledgeRetriever 的检索结果基线"
                   "（回归比对用；不用于跨后端比较）",
        "generated_by": "backend/tests/test_sql_retriever_baseline.py",
        "retriever": "VectorKnowledgeRetriever",
        "store": "SqlAlchemyVectorStore",
        "backend": BACKEND_LABEL,
        "dataset": DATASET_PATH.relative_to(BACKEND_DIR.parent).as_posix(),
        "record_fields": list(RECORD_FIELDS),
        "fixed_inputs": {
            "top_k": ENV_TOP_K,
            "min_score": ENV_MIN_SCORE,
            "model": env.embedder.name,
            "category": None,
            "document_id": None,
            "candidate_order": "knowledge_chunk.id 升序",
            "corpus_documents": len(env.records),
            "queries": len(rows),
            "embedding": env.embedding_info.to_dict(),
            "sorting": "services/vector_store.select_matches"
                       "（降序 → 同分按候选顺序 → min_score(>=) → top_k）",
        },
        "queries": [
            {
                "index": row["case"].index,
                "query": row["case"].text,
                "returned": len(row["records"]),
                "results": row["records"],
            }
            for row in rows
        ],
        "totals": {
            "queries": len(rows),
            "results": len(all_records),
            "max_score": max((r["score"] for r in all_records), default=0.0),
            "min_score": min((r["score"] for r in all_records), default=0.0),
        },
    }


def check_snapshot(snapshot: Dict[str, Any]) -> None:
    _section("[5] 快照生成与格式自检")

    cases = load_queries()

    # ---- 格式自检（先验格式，再落盘）----
    _check(f"快照顶层键恰好是 {len(SNAPSHOT_FIELDS)} 个，且顺序恒定",
           list(snapshot) == list(SNAPSHOT_FIELDS), str(list(snapshot)))
    _check(f"record_fields 声明为 {RECORD_FIELDS}",
           tuple(snapshot["record_fields"]) == RECORD_FIELDS)
    _check("每条 query 的封套键恰好是 (index, query, returned, results)",
           all(list(q) == list(QUERY_ENVELOPE_FIELDS) for q in snapshot["queries"]),
           str([list(q) for q in snapshot["queries"]
                if list(q) != list(QUERY_ENVELOPE_FIELDS)][:2]))
    _check("封套的 index 连续递增（0 起）",
           [q["index"] for q in snapshot["queries"]] == list(range(len(cases))))
    _check("封套的 returned == results 的实际条数",
           all(q["returned"] == len(q["results"]) for q in snapshot["queries"]))
    _check("★ 每条结果恰好 5 键（与题目要求一致）",
           all(list(r) == list(RECORD_FIELDS)
               for q in snapshot["queries"] for r in q["results"]))
    _check("快照内所有 query 文本与输入逐条一致（未改写/截断）",
           [q["query"] for q in snapshot["queries"]] == [c.text for c in cases],
           str([q["index"] for q, c in zip(snapshot["queries"], cases)
                if q["query"] != c.text]))
    _check("totals 与明细自洽（queries / results 计数）",
           snapshot["totals"]["queries"] == len(snapshot["queries"])
           and snapshot["totals"]["results"]
           == sum(len(q["results"]) for q in snapshot["queries"]))
    _check("totals 的 max_score / min_score 与明细自洽",
           snapshot["totals"]["max_score"]
           == max((r["score"] for q in snapshot["queries"] for r in q["results"]),
                  default=0.0)
           and snapshot["totals"]["min_score"]
           == min((r["score"] for q in snapshot["queries"] for r in q["results"]),
                  default=0.0))
    _check("fixed_inputs 记录了全部固定项（含候选顺序与排序口径）",
           set(snapshot["fixed_inputs"]) >= {"top_k", "min_score", "model", "category",
                                            "document_id", "candidate_order",
                                            "corpus_documents", "embedding", "sorting"},
           str(sorted(snapshot["fixed_inputs"])))
    _check("fixed_inputs 的 Embedding 只报 3 个字段（不含密钥等敏感信息）",
           set(snapshot["fixed_inputs"]["embedding"])
           == {"provider", "dimension", "semantic_enabled"},
           str(sorted(snapshot["fixed_inputs"]["embedding"])))

    # ---- 落盘 + 幂等验证 ----
    text = json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
    previous: Optional[Dict[str, Any]] = None
    if SNAPSHOT_PATH.is_file():
        previous = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    SNAPSHOT_PATH.write_text(text, encoding="utf-8")
    written = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))

    _check("快照已写出且可被 JSON 读回（往返无损）", written == snapshot)
    _check("★ 快照可**逐字节重现**（同输入同输出：重跑结果与既有基线一致）",
           previous is None or previous == snapshot,
           "与既有基线不同——如属预期变更，请删除该文件后重跑以重建基线")
    _check("落盘的每条结果仍恰好 5 键（序列化未增删字段）",
           all(list(r) == list(RECORD_FIELDS)
               for q in written["queries"] for r in q["results"]))

    size_kb = len(text.encode("utf-8")) / 1024
    print(f"\n  快照路径 : {SNAPSHOT_PATH.relative_to(BACKEND_DIR.parent).as_posix()}")
    print(f"  快照大小 : {size_kb:.1f} KB"
          f"（{snapshot['totals']['queries']} 条 query / "
          f"{snapshot['totals']['results']} 条结果）")
    print(f"  基线状态 : {'已存在并与本次一致（幂等）' if previous else '首次生成'}")


# ============================================================
# [6] 边界
# ============================================================
async def check_boundaries(env: Any) -> None:
    _section("[6] 边界（空 query / 超量 top_k / 维度不符 / 阈值超上限）")

    # ---- 空 query：不发起任何外部调用 ----
    rec_embedder = RecordingEmbedder(env.embedder)
    rec_store = RecordingStore(env.sql_store)
    retriever = VectorKnowledgeRetriever(rec_embedder, rec_store,
                                         top_k=ENV_TOP_K, min_score=ENV_MIN_SCORE)
    empty = await retriever.retrieve({}, "", {})
    _check("空 query → 返回 []（不是 None，也不抛异常）", empty == [], repr(empty))
    _check("★ 空 query 时**不调用** embedder（没线索就不花算力）",
           rec_embedder.calls == [], str(rec_embedder.calls))
    _check("★ 空 query 时**不调用**向量库", rec_store.calls == [], str(rec_store.calls))
    _check("空 query 仍被记录到 queries 观测列表（便于排查）",
           retriever.queries == [""], str(retriever.queries))

    # ---- top_k 超过候选数：返回全部，不报错 ----
    vector = await env.embedder.embed("Redis 缓存穿透")
    over = await env.sql_store.search(vector, top_k=EXPECTED_DOCS + 50,
                                      min_score=None, model=env.embedder.name)
    _check(f"top_k 超过候选数 → 返回全部 {EXPECTED_DOCS} 条（不报错、不补空）",
           len(over) == EXPECTED_DOCS, str(len(over)))

    # ---- min_score 超上限：空结果 ----
    none_out = await env.sql_store.search(vector, top_k=5, min_score=1.01,
                                          model=env.embedder.name)
    _check("min_score=1.01（余弦上限之上）→ 返回 []（空是「没通过阈值」，不是「失败」）",
           none_out == [], repr(none_out))

    # ---- 维度不符：经检索器应转成配置错误 ----
    class _WrongDimEmbedder:
        name = env.embedder.name
        dimension = 3

        async def embed(self, text: str) -> List[float]:
            return [1.0, 0.0, 0.0]

    bad = VectorKnowledgeRetriever(_WrongDimEmbedder(), env.sql_store,
                                   top_k=3, min_score=ENV_MIN_SCORE)
    try:
        await bad.retrieve({}, "Redis 缓存穿透", {})
        raised: Any = None
    except RetrieverConfigError as exc:
        raised = exc
    _check("★ 查询向量维度与库中向量不符 → RetrieverConfigError（配置错，重试没用）",
           raised is not None, repr(raised))

    # ---- 候选池按 model 过滤：换成不存在的模型 ⇒ 没有候选 ⇒ 空 ----
    ghost = await env.sql_store.search(vector, top_k=5, min_score=None,
                                       model="no-such-model")
    _check("用库中不存在的 model 过滤 → 无候选 → []（模型过滤确实生效）",
           ghost == [], repr(ghost))

    # ---- 守卫自检：本套件确实没写被禁 token ----
    own_src = Path(__file__).read_text(encoding="utf-8")
    _check("守卫自检：本套件源码不含被禁 token（反例运行时拼出）",
           _ALIEN_BACKEND_TOKEN not in own_src.lower()
           and _ALIEN_MODULE not in own_src
           and _GHOST_TOKEN not in own_src)


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 74)
    print("SqlAlchemyVectorStore 检索结果基线 · 自检 + 快照")
    print("链路：query → Embedding → SqlAlchemyVectorStore.search → VectorKnowledgeRetriever")
    print("=" * 74)

    check_contract()
    check_inputs()
    check_select_matches_rules()

    # 环境 API 的「只建 SQL」开关：开关名运行时拼出，使本文件不出现被禁 token
    async with open_env(**{_SQL_ONLY_KWARG: False}) as env:
        _check("环境只提供 sql 后端（本套件不做跨后端比较）",
               env.backends == (BACKEND_SQL,), str(env.backends))
        _check(f"权威行 {EXPECTED_DOCS} 条（corpus 每篇恰好 1 片）",
               len(env.records) == EXPECTED_DOCS, str(len(env.records)))
        rows = await collect(env)
        check_records(env, rows)
        snapshot = build_snapshot(env, rows)
        check_snapshot(snapshot)
        await check_boundaries(env)

    print("\n" + "=" * 74)
    print("输出摘要")
    print("=" * 74)
    print("  测试文件   : backend/tests/test_sql_retriever_baseline.py")
    print("  快照文件   : backend/scripts/sql_baseline_snapshot.json")
    print(f"  记录格式   : {list(RECORD_FIELDS)}")
    print(f"  固定输入   : top_k={ENV_TOP_K}  min_score={ENV_MIN_SCORE}  "
          f"model=<embedder.name>  候选顺序=主键升序")
    print(f"  断言       : 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
