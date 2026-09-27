# -*- coding: utf-8 -*-
"""ChromaVectorStore 检索结果基线 · 自检 + 快照生成（脚本式，非 pytest）。

运行：``python backend/tests/test_chroma_retriever_baseline.py``

被测链路（**只有 ANN 一条**）::

    query ──embed──▶ vector ──HNSW 召回 + select_matches 重排──▶ VectorMatch
      ▲                    ▲                                        │
      │                    └─ ChromaVectorStore（HNSW / cosine）     ▼
      │                                              VectorKnowledgeRetriever
      └─ 统一测试环境的 query（backend/scripts/rag_query_set.json）

本套件做三件事
--------------
1. **记录**：每条 query 经上述链路检出的每个片段，落成一条五键记录
   ``{query, chunk_id, content, metadata, score}``；
2. **验证**：① 查询成功 ② 返回结构完整 ③ 排序符合**当前规则**；
3. **快照**：把全部记录写成 ``backend/scripts/chroma_baseline_snapshot.json``
   ——这就是「ANN 后端结果基线」，供后续回归比对。

**输入与 SQL 基线完全相同**（同一数据集、同一组 query、同一 `top_k` / `min_score`、
同一批 ``KnowledgeChunk``）——这一点由**同一来源**构造来保证，见 [2]。

[1] 契约与边界——只跑 ANN 后端；不与另一个后端比较；受保护模块一行未改
[2] 输入——query / top_k / min_score / KnowledgeChunk 四项均由**同一来源**构造
[3] 逐 query 记录与三项验证（① 查询成功 ② 结构完整 ③ 排序符合当前规则）
[4] 排序规则自检——先锁「当前规则」的两个前提（召回穷尽 + 候选顺序），
    再独立重算 :func:`select_matches` 逐位比对；另单测四条规则本身
[5] 快照生成与格式自检（含「重跑结果一致」的幂等验证）
[6] 边界——空 query 不发起调用、空索引、top_k 超量、阈值超上限、维度不符；
    以及 **float32 量化漂移**的度量（「允许的小数误差」到底多大）

.. note::
   本套件**只读**生产代码：``ChromaVectorStore`` / ``VectorStore`` /
   ``VectorKnowledgeRetriever`` / ``EmbeddingService`` 一行未改。
   唯一的写动作是产出快照文件（数据产物，不是代码）。

.. warning::
   本套件**刻意不做**跨后端比较——那属于对比测试的职责。
   因此源码里不应出现另一个后端的**store 属性名 / 类名 / 模块名**，
   也不应引用任何跨后端对比工具；该约束由 [1] 的守卫断言锁死。
   **注意**：守卫的扫描范围是**整个文件**（含文档串、断言名、打印串），
   所以被禁的 token 一律**运行时拼出**，断言名里也绝不能出现字面量
   ——否则守卫会匹配到自己而恒假（项目踩过的坑，已复发三次）。

.. warning::
   **ANN 索引按 float32 存取向量**（HNSW 的存储精度），因此本基线的 ``score``
   与「全精度重算」相比有 ~1e-8 的**量化漂移**——这是后端固有特性，
   **不是实现缺陷**。本套件把该漂移**度量出来并给出上界**
   （见 [6] 的 ``max_float32_score_drift``），快照里也记了
   ``storage_dtype="float32"`` 与 ``score_tolerance``。
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
    VectorStoreInputError,
    cosine_similarity,
    select_matches,
)
from services.vector_store_chroma import (  # noqa: E402
    DEFAULT_OVERSAMPLE,
    HNSW_SPACE,
    ChromaVectorStore,
    from_chroma_metadata,
)

from vector_backend_env import (  # noqa: E402
    DATASET_PATH,
    ENV_MIN_SCORE,
    ENV_TOP_K,
    RecordingEmbedder,
    RecordingStore,
    load_corpus_documents,
    load_queries,
    open_env,
    run_case_observed,
)

# ============================================================
# 常量：记录格式与快照格式（**改这里就是改契约**）
# ============================================================
#: 每条检索结果的五键记录——题目要求的字段与顺序（与 SQL 基线一致）
RECORD_FIELDS: Tuple[str, ...] = ("query", "chunk_id", "content", "metadata", "score")

#: 快照顶层键（顺序即写出顺序；与 SQL 基线保持结构平行）
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

CHROMA_SNAPSHOT_PATH = BACKEND_DIR / "scripts" / "chroma_baseline_snapshot.json"
SNAPSHOT_NAME = "chroma_retriever_baseline"
SNAPSHOT_VERSION = 1
BACKEND_LABEL = "chroma"

#: ``ChromaVectorStore.name`` / 类名（自检用）
CHROMA_STORE_ATTR = "chroma"
STORE_CLASS_NAME = "ChromaVectorStore"

#: 数据集规模（corpus 16 篇 ⇒ 16 片；query 16 条）
EXPECTED_DOCS = 16
EXPECTED_QUERIES = 16

#: 余弦相似度的理论取值区间
SCORE_MIN, SCORE_MAX = -1.0, 1.0

#: 知识分类枚举（与 models 的 KNOWLEDGE_CATEGORIES 字面一致）
CATEGORIES = ("job", "technical", "company", "project")

#: **允许的 float32 量化误差上界**（HNSW 按 float32 存取 ⇒ ~1e-8 量级漂移）
FLOAT32_TOL = 1e-6

#: 本套件「不要修改」的生产模块（供守卫断言引用）
PROTECTED_MODULES: Tuple[str, ...] = (
    "services/vector_store_chroma.py",
    "services/vector_store.py",
    "services/vector_knowledge_retriever.py",
    "services/embedding_service.py",
)

#: **显式固定**的集合名。
#: 不传的话环境会用进程内自增计数器生成（``env_consistency_N``）——那意味着
#: 快照里的集合名取决于「本进程第几次建环境」，是**偶然**确定而非**构造**确定：
#: 将来在它前面多建一个环境，快照就会变。显式固定后，确定性来自代码而非调用顺序。
COLLECTION_NAME = "chroma_baseline"

# ---- 被禁标识（一律**运行时拼出**，避免守卫匹配到自己）----
#: 另一个后端的 store 属性名
_ALIEN_STORE_ATTR = "sql" + "_store"
#: 另一个后端的类名
_ALIEN_CLASS = "SqlAlchemy" + "VectorStore"
#: 另一个后端的模块名
_ALIEN_MODULE = "vector_store_" + "sql"
#: 跨后端对比工具名
_FORBIDDEN_HELPERS: Tuple[str, ...] = (
    "compare_chunk_" + "lists", "stable_" + "hits", "ambiguous_" + "hits",
)
#: 守卫自检用的「必然不存在」token
_GHOST_TOKEN = "zzz" + "_sentinel"

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


def _pairs(matches: Sequence[Any]) -> List[Tuple[Any, float]]:
    """``(chunk_id, score)`` 序列——比对排序用的稳定投影。"""
    return [(m.chunk_id, m.score) for m in matches]


# ============================================================
# 索引读取（只读；用于「独立重算」与「索引内容取证」）
# ============================================================
def _as_list(value: Any) -> List[Any]:
    """把 chroma 返回值的一列转成 list；``None`` → ``[]``。

    **不能**写 ``value or []``：chroma 的 ``embeddings`` 是 numpy 数组，
    对它求布尔值会抛 ``ValueError: The truth value of an array ... is ambiguous``。
    """
    if value is None:
        return []
    return list(value)


def read_index(store: Any) -> List[VectorRecord]:
    """把 ANN 索引里的**全部**条目读回成 ``VectorRecord``（按 ``chunk_id`` 升序）。

    为什么要读私有句柄：``VectorStore`` 接口只暴露 ``add`` / ``search`` / ``count``，
    而「独立重算排序」必须拿到**索引里实际存的向量**（float32 回读值）——
    只有这样才能与 ``search`` 的候选集**逐条一致**。
    这是**只读**取证，不修改任何状态；``test_vector_store_chroma.py``
    已有同样的取用先例。
    """
    got = store._collection_handle().get(
        include=["embeddings", "documents", "metadatas"]
    )
    ids = _as_list(got.get("ids"))
    embeddings = _as_list(got.get("embeddings"))
    documents = _as_list(got.get("documents"))
    metadatas = _as_list(got.get("metadatas"))

    records: List[VectorRecord] = []
    for position, raw_id in enumerate(ids):
        try:
            chunk_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        metadata, model, document_id = from_chroma_metadata(
            metadatas[position] if position < len(metadatas) else None
        )
        try:
            records.append(VectorRecord(
                vector=embeddings[position] if position < len(embeddings) else None,
                content=(documents[position] if position < len(documents) else "") or "",
                document_id=document_id,
                chunk_id=chunk_id,
                metadata=metadata,
                model=model,
            ))
        except VectorStoreInputError:
            continue
    # 候选顺序 = chunk_id 升序（与 _candidates_from_query 的排序一致）
    records.sort(key=lambda record: record.chunk_id or 0)
    return records


def ann_n_results(total: int, top_k: int, oversample: int) -> int:
    """复刻 ``ChromaVectorStore.search`` 的召回条数公式：``min(total, max(top_k, top_k×ov)``。"""
    return min(total, max(top_k, top_k * oversample))


# ============================================================
# [1] 契约与边界
# ============================================================
def check_contract() -> None:
    _section("[1] 契约与边界（只跑 ANN；不与另一后端比较；受保护模块未改）")

    own_src = Path(__file__).read_text(encoding="utf-8")

    _check("★ 本套件只做 ANN 快照：源码不含另一个后端的 store 属性名",
           _ALIEN_STORE_ATTR not in own_src, "出现了被禁止的 store 属性名")
    _check("★ 源码不含另一个后端的类名",
           _ALIEN_CLASS not in own_src)
    _check("★ 源码不含另一个后端的模块名",
           _ALIEN_MODULE not in own_src)
    _check("★ 本套件不引用任何跨后端对比工具（只记录，不比较）",
           not any(token in own_src for token in _FORBIDDEN_HELPERS),
           str([t for t in _FORBIDDEN_HELPERS if t in own_src]))
    _check("守卫自检：token 检查能区分「有」与「无」（反例运行时拼出，不自引用）",
           (_ALIEN_STORE_ATTR not in own_src) is True
           and _GHOST_TOKEN not in own_src
           and _GHOST_TOKEN not in (BACKEND_DIR / PROTECTED_MODULES[0]).read_text(
               encoding="utf-8"))

    # ---- ANN 后端确实复用共享排序口径（静态接线取证）----
    src = (BACKEND_DIR / "services" / "vector_store_chroma.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    imported: set = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "services.vector_store":
            imported |= {alias.name for alias in node.names}
    _check("★ ChromaVectorStore 从接口模块导入共享排序口径 select_matches",
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
    _check("★ ChromaVectorStore.search 的**函数体内**确实调用了 select_matches",
           calls_select, "未找到调用" if search_fn is not None else "未找到 search 方法")
    _check("ChromaVectorStore.search 签名未变（参数顺序与接口一致）",
           search_fn is not None
           and [a.arg for a in search_fn.args.args] == ["self", "query_vector"]
           and [a.arg for a in search_fn.args.kwonlyargs]
           == ["top_k", "model", "document_id", "category", "min_score"],
           str([a.arg for a in search_fn.args.kwonlyargs]) if search_fn else "")
    _check("★ 召回条数公式仍是 min(索引总数, max(top_k, top_k×oversample))",
           "min(total, max(top_k, top_k * self.oversample))" in src
           or "min(total, max(top_k, top_k*self.oversample))" in src,
           "未找到召回条数公式")
    _check("★ 候选顺序仍是 chunk_id 升序（同分兜底口径）",
           "candidates.sort(key=lambda record: record.chunk_id or 0)" in src)

    # ---- 受保护模块不反向引用本套件 ----
    suite_stem = Path(__file__).stem
    offenders = [rel for rel in PROTECTED_MODULES
                 if suite_stem in (BACKEND_DIR / rel).read_text(encoding="utf-8")]
    _check("★ 四个受保护模块都不引用本套件（生产代码零测试依赖）",
           not offenders, str(offenders))
    _check("ChromaVectorStore 仍是 VectorStore 的公开实现，name 未变",
           ChromaVectorStore.name == CHROMA_STORE_ATTR,
           ChromaVectorStore.name)
    _check("索引距离度量仍是余弦（与 select_matches 口径一致）",
           HNSW_SPACE == "cosine", HNSW_SPACE)
    _check(f"召回倍率常量仍是 {DEFAULT_OVERSAMPLE}",
           DEFAULT_OVERSAMPLE == 4, str(DEFAULT_OVERSAMPLE))


# ============================================================
# [2] 输入（四项均由同一来源构造）
# ============================================================
def check_inputs() -> None:
    _section("[2] 输入（与 SQL 基线完全相同的四项，均由同一来源构造）")

    _check("数据集文件存在（两套基线的唯一数据来源）", DATASET_PATH.is_file(),
           DATASET_PATH.as_posix())
    raw = json.loads(DATASET_PATH.read_text(encoding="utf-8"))
    docs = load_corpus_documents()
    cases = load_queries()

    # ① query：取自数据集，逐条一致
    _check(f"① query {EXPECTED_QUERIES} 条，文本非空且无重复",
           len(cases) == EXPECTED_QUERIES
           and all(c.text.strip() for c in cases)
           and len({c.text for c in cases}) == len(cases),
           f"{len(cases)} 条")
    _check("① query 文本取自数据集（逐条一致，未改写/截断）",
           [c.text for c in cases] == [q["query"] for q in raw["queries"]])

    # ②③ top_k / min_score：取自**同一组环境常量**（不是数据集逐条标定值）
    _check(f"② 每条 query 的 top_k == 环境常量 {ENV_TOP_K}",
           all(c.top_k == ENV_TOP_K for c in cases),
           str(sorted({c.top_k for c in cases})))
    _check(f"③ 每条 query 的 min_score == 环境常量 {ENV_MIN_SCORE}",
           all(c.min_score == ENV_MIN_SCORE for c in cases),
           str(sorted({c.min_score for c in cases})))
    ds_top_k = {q["top_k"] for q in raw["queries"]}
    ds_min_score = {q["min_score"] for q in raw["queries"]}
    _check("②③ 数据集里逐条标定的 top_k / min_score 被**刻意忽略**（不是照搬）",
           ds_top_k != {ENV_TOP_K} and ds_min_score != {ENV_MIN_SCORE},
           f"数据集 top_k={sorted(ds_top_k)} min_score={sorted(ds_min_score)}")

    # ④ KnowledgeChunk：同一批 corpus（16 篇）
    _check(f"④ corpus {EXPECTED_DOCS} 篇，每篇恰好 4 键",
           len(docs) == EXPECTED_DOCS
           and all(set(d) == {"title", "content", "category", "source"} for d in docs),
           f"{len(docs)} 篇")
    _check("④ corpus 每篇正文 <= 480 字（DEFAULT_CHUNK_SIZE=500 ⇒ 每篇恰好 1 片）",
           all(len(d["content"]) <= 480 for d in docs),
           str([(d["title"], len(d["content"])) for d in docs
                if len(d["content"]) > 480]))

    print(f"  数据集   : {DATASET_PATH.relative_to(BACKEND_DIR.parent).as_posix()}")
    print(f"  corpus   : {len(docs)} 篇 ⇒ {len(docs)} 片（每篇 1 片）")
    print(f"  query    : {len(cases)} 条   top_k={ENV_TOP_K}   min_score={ENV_MIN_SCORE}")


# ============================================================
# [3] 逐 query 记录 + 三项验证
# ============================================================
async def collect(env: Any) -> Dict[str, Any]:
    """跑完所有 query，返回「逐 query 的观测记录」（**不做断言**，只记录）。"""
    store = env.chroma_store
    index_records = read_index(store)
    total = len(index_records)
    model = env.embedder.name
    n_results = ann_n_results(total, ENV_TOP_K, DEFAULT_OVERSAMPLE)

    # 权威行的**全精度**向量（用于量化 float32 漂移；与向量后端无关，只读列）
    rows = (await env.session.execute(
        select(KnowledgeChunkRow.id, KnowledgeChunkRow.embedding)
        .order_by(KnowledgeChunkRow.id)
    )).all()
    full_precision = {
        int(row_id): list(embedding)
        for row_id, embedding in rows if embedding is not None
    }

    rows_out: List[Dict[str, Any]] = []
    max_drift = 0.0
    for case in cases_iter(env):
        obs = await run_case_observed(env, BACKEND_LABEL, case)
        vector = list(obs.query_vector)

        # 路径①「独立重算」：索引里的全部向量（float32 回读值）→ 共享排序口径
        expected = select_matches(
            index_records, vector, top_k=case.top_k, min_score=case.min_score,
            store_name=CHROMA_STORE_ATTR,
        )
        # 路径②「直接直查」：问 ANN 后端要一次
        direct = await store.search(
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
        # float32 量化漂移：返回的 score vs 用全精度向量重算的余弦
        for record in records:
            precise = full_precision.get(record["chunk_id"])
            if precise is None:
                continue
            max_drift = max(
                max_drift, abs(record["score"] - cosine_similarity(vector, precise))
            )

        rows_out.append({
            "case": case,
            "obs": obs,
            "records": records,
            "expected": expected,
            "direct": direct,
            "vector": vector,
        })
    return {
        "rows": rows_out,
        "index_records": index_records,
        "total": total,
        "n_results": n_results,
        "max_float32_drift": max_drift,
    }


def cases_iter(env: Any) -> Sequence[Any]:
    """环境里的固定 query 序列。"""
    return env.queries


def check_records(env: Any, collected: Dict[str, Any]) -> None:
    _section("[3] 逐 query 记录与三项验证（① 查询成功 ② 结构完整 ③ 排序符合当前规则）")

    rows = collected["rows"]
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
    _check("① 检索器确实绑在 ANN 后端上（name == chroma，不是别的后端）",
           env.chroma_store.name == CHROMA_STORE_ATTR
           and type(env.chroma_store).__name__ == STORE_CLASS_NAME)

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

    # ---- ③ 排序符合当前规则 ----
    _check("③ 每条 query 的分数序列**单调不增**（降序）",
           all(_nonincreasing([r["score"] for r in row["records"]]) for row in rows),
           str([row["case"].index for row in rows
                if not _nonincreasing([r["score"] for r in row["records"]])]))
    _check("③ ★ 与**独立重算**（索引向量 → select_matches）的 chunk_id 序列逐位一致",
           all([m.chunk_id for m in row["expected"]]
               == [r["chunk_id"] for r in row["records"]] for row in rows),
           str([row["case"].index for row in rows
                if [m.chunk_id for m in row["expected"]]
                != [r["chunk_id"] for r in row["records"]]]))
    _check("③ ★ 与**直接调用** store.search() 的结果逐位一致（chunk_id + score）",
           all(_pairs(row["direct"]) == [(r["chunk_id"], r["score"]) for r in row["records"]]
               for row in rows),
           str([row["case"].index for row in rows
                if _pairs(row["direct"])
                != [(r["chunk_id"], r["score"]) for r in row["records"]]]))
    _check("③ 重算路径与直查路径的分数在 float32 容差内一致",
           all(all(abs(a[1] - b[1]) <= FLOAT32_TOL
                   for a, b in zip(_pairs(row["expected"]), _pairs(row["direct"])))
               for row in rows),
           str([row["case"].index for row in rows
                if not all(abs(a[1] - b[1]) <= FLOAT32_TOL
                           for a, b in zip(_pairs(row["expected"]),
                                           _pairs(row["direct"])))]))
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

    n = len(all_records)
    top1 = max((r["score"] for r in all_records), default=0.0)
    low = min((r["score"] for r in all_records), default=0.0)
    print(f"\n  记录总数 : {n} 条（{EXPECTED_QUERIES} 条 query，"
          f"平均 {n / EXPECTED_QUERIES:.2f} 条/query）")
    print(f"  分数区间 : {low:.6f} .. {top1:.6f}")


# ============================================================
# [4] 排序规则自检
# ============================================================
def check_sorting_rule(env: Any, collected: Dict[str, Any]) -> None:
    _section("[4] 排序规则自检（当前规则 = ANN 召回 → chunk_id 升序 → select_matches）")

    total = collected["total"]
    n_results = collected["n_results"]
    index_records = collected["index_records"]
    rows = collected["rows"]

    # ---- 前提一：召回必须**穷尽**，否则「与全量重算比对」不成立 ----
    _check(f"索引总数 == corpus 篇数（{EXPECTED_DOCS}）",
           total == EXPECTED_DOCS, str(total))
    _check(f"★ 召回穷尽：n_results({n_results}) == 索引总数({total})"
           f"（top_k×oversample = {ENV_TOP_K}×{DEFAULT_OVERSAMPLE} = "
           f"{ENV_TOP_K * DEFAULT_OVERSAMPLE} ≥ {total}）",
           n_results == total,
           f"n_results={n_results} < total={total} ⇒ 召回非穷尽，不能与全量重算比对")
    _check("召回穷尽 ⇒ 本基线的排序可与「全量候选重算」逐位比对（该前提已显式锁定）",
           n_results == total)
    _check("where 过滤（model）未削减候选集（16 条同一 model）",
           all(record.model == env.embedder.name for record in index_records),
           str(sorted({record.model for record in index_records})))

    # ---- 前提二：候选顺序 = chunk_id 升序 ----
    ids = [record.chunk_id for record in index_records]
    _check("★ 候选顺序 = chunk_id 升序（同分兜底口径与另一后端一致）",
           ids == sorted(ids), str(ids))

    # ---- 同分兜底：分数并列时，返回顺序必须是 chunk_id 升序 ----
    tie_violations: List[Any] = []
    for row in rows:
        recs = row["records"]
        for left, right in zip(recs, recs[1:]):
            if left["score"] == right["score"] and left["chunk_id"] >= right["chunk_id"]:
                tie_violations.append((row["case"].index,
                                       left["chunk_id"], right["chunk_id"]))
    _check("★ 分数并列时按 chunk_id 升序（候选顺序）返回，无并列项违反",
           not tie_violations, str(tie_violations[:3]))

    # ---- 索引内容 == 数据集 corpus（同一批 KnowledgeChunk 输入）----
    index_contents = [record.content for record in index_records]
    corpus_contents = [doc["content"] for doc in load_corpus_documents()]
    _check("★ 索引里的 16 条正文与数据集 corpus 逐条一致（同一批输入）",
           sorted(index_contents) == sorted(corpus_contents),
           f"索引 {len(index_contents)} 条 / corpus {len(corpus_contents)} 条")
    _check("索引里的向量维度都等于 embedder.dimension",
           all(len(record.vector) == env.embedder.dimension for record in index_records),
           str(sorted({len(record.vector) for record in index_records})))
    _check("索引里的每条都能被检索器读到（无脏条目被跳过）",
           len(index_records) == EXPECTED_DOCS, str(len(index_records)))

    # ---- select_matches 四条规则本身（共享口径，与后端无关）----
    q = [1.0, 0.0, 0.0, 0.0]

    def rec(chunk_id: int, vector: Sequence[float], content: str = "x") -> VectorRecord:
        return VectorRecord(vector=list(vector), content=content, chunk_id=chunk_id,
                            document_id=1, metadata={"category": "technical"})

    unordered = [rec(1, [0.0, 1.0, 0.0, 0.0]),
                 rec(2, [1.0, 0.0, 0.0, 0.0]),
                 rec(3, [1.0, 1.0, 0.0, 0.0])]
    out = select_matches(unordered, q, top_k=3)
    _check("规则·降序：排序由分数决定，与候选顺序无关（[低,高,中] → [高,中,低]）",
           [m.chunk_id for m in out] == [2, 3, 1], str([m.chunk_id for m in out]))
    _check("规则·降序：分数序列单调不增", _nonincreasing([m.score for m in out]))
    _check("规则·截断：top_k 取前 N 条",
           [m.chunk_id for m in select_matches(unordered, q, top_k=2)] == [2, 3],
           str([m.chunk_id for m in select_matches(unordered, q, top_k=2)]))

    tie = [rec(30, [1.0, 0.0, 0.0, 0.0]), rec(10, [1.0, 0.0, 0.0, 0.0]),
           rec(20, [1.0, 0.0, 0.0, 0.0])]
    tie_out = [m.chunk_id for m in select_matches(tie, q, top_k=3)]
    _check("规则·同分兜底按**候选顺序**（30,10,20 保持，而非按 id 升序）",
           tie_out == [30, 10, 20], str(tie_out))

    zero = rec(7, [0.0, 0.0, 0.0, 0.0])
    _check("规则·零向量余弦恰为 0.0（不做除零）",
           select_matches([zero], q, top_k=1)[0].score == 0.0)
    _check("规则·min_score 含边界：阈值 == 分数时**保留**该条",
           len(select_matches([zero], q, top_k=1, min_score=0.0)) == 1)
    _check("规则·min_score 超出一丝即过滤掉（0.0 < 0.001）",
           select_matches([zero], q, top_k=1, min_score=0.001) == [])
    _check("规则·min_score=1.01（余弦上限之上）→ 恒返回空",
           select_matches([rec(1, [1.0, 0.0, 0.0, 0.0])], q, top_k=1,
                          min_score=1.01) == [])

    mixed = [rec(1, [1.0, 0.0, 0.0]), rec(2, [1.0, 0.0, 0.0, 0.0])]
    _check("规则·维度不符的候选被跳过（不报错、也不参与排序）",
           [m.chunk_id for m in select_matches(mixed, q, top_k=5)] == [2])
    try:
        select_matches([rec(1, [1.0, 0.0, 0.0])], q, top_k=5)
        raised: Any = None
    except VectorStoreDimensionError as exc:
        raised = exc
    _check("规则·候选非空但一条维度都对不上 → VectorStoreDimensionError",
           raised is not None, repr(raised))

    print("  当前规则：ANN 召回 min(total, top_k×oversample) → 候选按 chunk_id 升序"
          " → select_matches（降序 → 同分按候选顺序 → min_score(>=) → top_k）")


# ============================================================
# [5] 快照生成与格式自检
# ============================================================
def build_snapshot(env: Any, collected: Dict[str, Any]) -> Dict[str, Any]:
    """按既定格式组装快照对象。"""
    rows = collected["rows"]
    all_records = [r for row in rows for r in row["records"]]
    return {
        "name": SNAPSHOT_NAME,
        "version": SNAPSHOT_VERSION,
        "purpose": "ChromaVectorStore（HNSW ANN）经 VectorKnowledgeRetriever 的检索结果基线"
                   "（回归比对用；不用于跨后端比较）",
        "generated_by": "backend/tests/test_chroma_retriever_baseline.py",
        "retriever": "VectorKnowledgeRetriever",
        "store": STORE_CLASS_NAME,
        "backend": BACKEND_LABEL,
        "dataset": DATASET_PATH.relative_to(BACKEND_DIR.parent).as_posix(),
        "record_fields": list(RECORD_FIELDS),
        "fixed_inputs": {
            "top_k": ENV_TOP_K,
            "min_score": ENV_MIN_SCORE,
            "model": env.embedder.name,
            "category": None,
            "document_id": None,
            "candidate_order": "chunk_id 升序（ANN 召回后由候选归一函数排序）",
            "corpus_documents": len(collected["index_records"]),
            "queries": len(rows),
            "embedding": env.embedding_info.to_dict(),
            "sorting": "ANN 召回 min(total, top_k×oversample) → 候选按 chunk_id 升序"
                       " → services/vector_store.select_matches"
                       "（降序 → 同分按候选顺序 → min_score(>=) → top_k）",
            "collection_name": env.collection_name,
            "hnsw_space": HNSW_SPACE,
            "oversample": DEFAULT_OVERSAMPLE,
            "ann_n_results": collected["n_results"],
            "recall_exhaustive": collected["n_results"] == collected["total"],
            "storage_dtype": "float32",
            "score_tolerance": FLOAT32_TOL,
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
            "max_float32_score_drift": collected["max_float32_drift"],
        },
    }


def check_snapshot(snapshot: Dict[str, Any]) -> None:
    _section("[5] 快照生成与格式自检")

    cases = load_queries()

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
    _check("fixed_inputs 记录了全部固定项（含召回参数与排序口径）",
           set(snapshot["fixed_inputs"]) >= {"top_k", "min_score", "model", "category",
                                            "document_id", "candidate_order",
                                            "corpus_documents", "embedding", "sorting",
                                            "collection_name", "hnsw_space", "oversample",
                                            "ann_n_results", "recall_exhaustive",
                                            "storage_dtype", "score_tolerance"},
           str(sorted(snapshot["fixed_inputs"])))
    _check("★ 快照**显式声明** float32 存储与容差（小数误差有据可依）",
           snapshot["fixed_inputs"]["storage_dtype"] == "float32"
           and snapshot["fixed_inputs"]["score_tolerance"] == FLOAT32_TOL)
    _check("★ 快照显式声明「召回穷尽」（否则排序比对的前提不成立）",
           snapshot["fixed_inputs"]["recall_exhaustive"] is True)
    _check("fixed_inputs 的 Embedding 只报 3 个字段（不含密钥等敏感信息）",
           set(snapshot["fixed_inputs"]["embedding"])
           == {"provider", "dimension", "semantic_enabled"},
           str(sorted(snapshot["fixed_inputs"]["embedding"])))

    text = json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
    previous: Optional[Dict[str, Any]] = None
    if CHROMA_SNAPSHOT_PATH.is_file():
        previous = json.loads(CHROMA_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    CHROMA_SNAPSHOT_PATH.write_text(text, encoding="utf-8")
    written = json.loads(CHROMA_SNAPSHOT_PATH.read_text(encoding="utf-8"))

    _check("快照已写出且可被 JSON 读回（往返无损）", written == snapshot)
    _check("★ 快照可**逐字节重现**（同输入同输出：重跑结果与既有基线一致）",
           previous is None or previous == snapshot,
           "与既有基线不同——如属预期变更，请删除该文件后重跑以重建基线")
    _check("落盘的每条结果仍恰好 5 键（序列化未增删字段）",
           all(list(r) == list(RECORD_FIELDS)
               for q in written["queries"] for r in q["results"]))

    size_kb = len(text.encode("utf-8")) / 1024
    print(f"\n  快照路径 : {CHROMA_SNAPSHOT_PATH.relative_to(BACKEND_DIR.parent).as_posix()}")
    print(f"  快照大小 : {size_kb:.1f} KB"
          f"（{snapshot['totals']['queries']} 条 query / "
          f"{snapshot['totals']['results']} 条结果）")
    print(f"  基线状态 : {'已存在并与本次一致（幂等）' if previous else '首次生成'}")


# ============================================================
# [6] 边界 + float32 漂移度量
# ============================================================
async def check_boundaries(env: Any, collected: Dict[str, Any]) -> None:
    _section("[6] 边界（空 query / 空索引 / 超量 top_k / 维度不符 / float32 漂移）")

    # ---- 空 query：不发起任何外部调用 ----
    rec_embedder = RecordingEmbedder(env.embedder)
    rec_store = RecordingStore(env.chroma_store)
    retriever = VectorKnowledgeRetriever(rec_embedder, rec_store,
                                         top_k=ENV_TOP_K, min_score=ENV_MIN_SCORE)
    empty = await retriever.retrieve({}, "", {})
    _check("空 query → 返回 []（不是 None，也不抛异常）", empty == [], repr(empty))
    _check("★ 空 query 时**不调用** embedder（没线索就不花算力）",
           rec_embedder.calls == [], str(rec_embedder.calls))
    _check("★ 空 query 时**不调用** ANN 索引", rec_store.calls == [], str(rec_store.calls))
    _check("空 query 仍被记录到 queries 观测列表（便于排查）",
           retriever.queries == [""], str(retriever.queries))

    vector = await env.embedder.embed("Redis 缓存穿透")

    # ---- 空索引：返回 []，且**不**被误判成维度问题 ----
    empty_store = ChromaVectorStore(
        env.session, client=env.chroma_store._client,
        collection_name=f"{env.collection_name}_empty",
    )
    _check("空索引的 count() == 0", await empty_store.count() == 0)
    _check("空索引检索 → 返回 []（「确实没有候选」，不是维度错误）",
           await empty_store.search(vector, top_k=5, min_score=None) == [])
    _check("空索引时检索器同样返回 []（不抛异常）",
           await VectorKnowledgeRetriever(
               env.embedder, empty_store, top_k=5).retrieve({}, "Redis", {}) == [])

    # ---- top_k 超过候选数：返回全部，不报错 ----
    over = await env.chroma_store.search(vector, top_k=EXPECTED_DOCS + 50,
                                        min_score=None, model=env.embedder.name)
    _check(f"top_k 超过候选数 → 返回全部 {EXPECTED_DOCS} 条（不报错、不补空）",
           len(over) == EXPECTED_DOCS, str(len(over)))

    # ---- min_score 超上限：空结果 ----
    none_out = await env.chroma_store.search(vector, top_k=5, min_score=1.01,
                                             model=env.embedder.name)
    _check("min_score=1.01（余弦上限之上）→ 返回 []（空是「没通过阈值」，不是「失败」）",
           none_out == [], repr(none_out))

    # ---- 维度不符：经检索器应转成配置错误 ----
    class _WrongDimEmbedder:
        name = env.embedder.name
        dimension = 3

        async def embed(self, text: str) -> List[float]:
            return [1.0, 0.0, 0.0]

    bad = VectorKnowledgeRetriever(_WrongDimEmbedder(), env.chroma_store,
                                   top_k=3, min_score=ENV_MIN_SCORE)
    try:
        await bad.retrieve({}, "Redis 缓存穿透", {})
        raised: Any = None
    except RetrieverConfigError as exc:
        raised = exc
    _check("★ 查询向量维度与索引不符 → RetrieverConfigError（配置错，重试没用）",
           raised is not None, repr(raised))

    # ---- 用不存在的 model 过滤：where 命中 0 条 ⇒ 无候选 ⇒ 空 ----
    ghost = await env.chroma_store.search(vector, top_k=5, min_score=None,
                                          model="no-such-model")
    _check("用索引中不存在的 model 过滤 → 无候选 → []（where 过滤确实生效）",
           ghost == [], repr(ghost))

    # ---- float32 量化漂移：度量并给出上界 ----
    drift = collected["max_float32_drift"]
    _check(f"★ float32 量化漂移在容差内：max|Δscore| = {drift:.3e} <= {FLOAT32_TOL:g}",
           drift <= FLOAT32_TOL, f"{drift:.3e}")
    _check("★ 漂移量级远小于容差（<= 容差的 1/10），说明「小数误差」是可解释的量化误差",
           drift <= FLOAT32_TOL / 10, f"{drift:.3e}")

    # ---- 守卫自检 ----
    own_src = Path(__file__).read_text(encoding="utf-8")
    _check("守卫自检：本套件源码不含被禁标识（反例运行时拼出）",
           _ALIEN_STORE_ATTR not in own_src
           and _ALIEN_CLASS not in own_src
           and _ALIEN_MODULE not in own_src
           and _GHOST_TOKEN not in own_src)


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 74)
    print("ChromaVectorStore（HNSW ANN）检索结果基线 · 自检 + 快照")
    print("链路：query → Embedding → ChromaVectorStore.search → VectorKnowledgeRetriever")
    print("=" * 74)

    check_contract()
    check_inputs()

    async with open_env(collection_name=COLLECTION_NAME) as env:
        _check("环境提供 ANN 后端（本套件只跑它）",
               env.chroma_available and env.chroma_store is not None)
        _check(f"集合名被显式固定为 {COLLECTION_NAME!r}（快照确定性来自构造）",
               env.collection_name == COLLECTION_NAME, env.collection_name)
        collected = await collect(env)
        check_records(env, collected)
        check_sorting_rule(env, collected)
        snapshot = build_snapshot(env, collected)
        check_snapshot(snapshot)
        await check_boundaries(env, collected)

    print("\n" + "=" * 74)
    print("输出摘要")
    print("=" * 74)
    print("  测试文件   : backend/tests/test_chroma_retriever_baseline.py")
    print("  快照文件   : backend/scripts/chroma_baseline_snapshot.json")
    print(f"  记录格式   : {list(RECORD_FIELDS)}")
    print(f"  固定输入   : top_k={ENV_TOP_K}  min_score={ENV_MIN_SCORE}  "
          f"model=<embedder.name>  oversample={DEFAULT_OVERSAMPLE}  "
          f"hnsw_space={HNSW_SPACE}")
    print(f"  存储精度   : float32（容差 {FLOAT32_TOL:g}）")
    print(f"  断言       : 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
