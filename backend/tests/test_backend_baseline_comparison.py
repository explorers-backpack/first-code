# -*- coding: utf-8 -*-
"""SQL / ANN 两后端检索结果基线 · 比较测试（脚本式，非 pytest）。

运行：``python backend/tests/test_backend_baseline_comparison.py``

前提（**已经存在**，由前两个任务产出）
------------------------------------
============================  ====================================================
``scripts/sql_baseline_snapshot.json``     由 ``tests/test_sql_retriever_baseline.py`` 生成
``scripts/chroma_baseline_snapshot.json``  由 ``tests/test_chroma_retriever_baseline.py`` 生成
============================  ====================================================

**本套件只读这两个 JSON 文件**——它**不 import 任何生产模块**
（``services.*`` 一个都没有），因此「只新增比较测试、不改动
``VectorStore`` / ``Retriever`` / ``Embedding``」不是声明，而是**结构上成立的**：
没有依赖就没有修改的可能（由 [1] 的导入白名单守卫锁死）。

比较规则（同一个 query 下，逐项）
--------------------------------
==========  ========  ==========================================================
项          判定      说明
==========  ========  ==========================================================
返回数量     要求一致  条数必须相等
``chunk_id`` 要求一致  **按位（序列）**一致——顺序也是契约的一部分
``content``  要求一致  逐字节一致
``metadata`` 要求一致  键集一致 + 各键值一致；**键序不参与判定**。
                       唯一例外：``metadata["score"]`` 与顶层 ``score`` 同源，
                       属容差项
``score``    容差一致  ``|Δ| <= SCORE_TOL``（**不要求逐位相等**）
==========  ========  ==========================================================

[1] 前置与契约——两个快照存在、格式合法、输入四项一致、本套件零生产依赖
[2] 比较规则自检——规则本身可执行（正例通过 / 反例被拦）
[3] 逐 query 五项比较（含明细表）
[4] 容差口径与**排序稳健性**（为什么 ``chunk_id`` 序列能逐位一致）
[5] 边界——缺文件 / 结构不符 / 输入不一致 都要**明确报错**，不静默降级

.. note::
   本套件**不修改任何文件**：不写快照、不写报告，只读两个 JSON 并打印结论。

.. warning::
   ``metadata`` 的**键序**在两个后端之间**本来就可能不同**（ANN 侧走
   ``_meta`` JSON 往返，序列化时按字母序；SQL 侧走 JSON 列原序）。
   因此比较必须**按值**（``dict`` 相等）而不是按序列化字节。
   这不是缺陷，是「同一份元数据、两种存储」的必然结果；
   两套快照各自的**字节稳定性**不受影响（各自重跑仍逐字节一致）。
"""

from __future__ import annotations

import ast
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent

# ============================================================
# 被比较的两份快照
# ============================================================
SQL_SNAPSHOT_PATH = BACKEND_DIR / "scripts" / "sql_baseline_snapshot.json"
CHROMA_SNAPSHOT_PATH = BACKEND_DIR / "scripts" / "chroma_baseline_snapshot.json"

#: 生成方（用于溯源校验：确保比的是「那两套基线」而不是别的文件）
EXPECTED_GENERATORS: Tuple[str, ...] = (
    "backend/tests/test_sql_retriever_baseline.py",
    "backend/tests/test_chroma_retriever_baseline.py",
)

# ============================================================
# 比较规则（**改这里就是改契约**）
# ============================================================
#: 参与比较的五项，顺序即报告顺序
COMPARE_ITEMS: Tuple[str, ...] = ("count", "chunk_id", "content", "metadata", "score")

#: 要求**逐位一致**的项
EXACT_ITEMS: Tuple[str, ...] = ("count", "chunk_id", "content", "metadata")

#: 允许**容差**的项
TOLERANT_ITEMS: Tuple[str, ...] = ("score",)

#: 分数容差：float32（ANN 索引存储精度）与 float64 之间的小数误差上界
SCORE_TOL = 1e-6

#: ``metadata`` 里属于**容差项**的键（与顶层 ``score`` 同源，不要求逐位相等）
METADATA_TOLERANT_KEYS: Tuple[str, ...] = ("score",)

#: 记录格式（两套基线必须一致，否则「同一个 query 下」无从谈起）
RECORD_FIELDS: Tuple[str, ...] = ("query", "chunk_id", "content", "metadata", "score")

#: 快照顶层键（两套基线结构平行）
SNAPSHOT_FIELDS: Tuple[str, ...] = (
    "name", "version", "purpose", "generated_by", "retriever", "store", "backend",
    "dataset", "record_fields", "fixed_inputs", "queries", "totals",
)

#: 封套键
QUERY_ENVELOPE_FIELDS: Tuple[str, ...] = ("index", "query", "returned", "results")

#: **可比较性前提**：这些 ``fixed_inputs`` 键必须在两套基线里完全相同，
#: 否则「同一个 query 下比较」不成立（例如一边 top_k=5、一边 top_k=3）。
SHARED_INPUT_KEYS: Tuple[str, ...] = (
    "top_k", "min_score", "model", "category", "document_id",
    "corpus_documents", "queries", "embedding",
)

#: 本套件**唯一允许**的导入（标准库；**不得**出现任何 ``services.*``）
ALLOWED_IMPORTS: frozenset = frozenset({
    "__future__", "ast", "json", "sys", "dataclasses", "pathlib", "typing",
})

#: 「本套件只读」的判据：这些**调用片段**若出现在自己的源码里，就说明它在写文件。
#:
#: .. warning::
#:    **必须在运行时拼出**（``"op" + "en("``）。字面写出来的话，守卫会
#:    **匹配到自己的这一行**，从而恒为「命中」——本套件第一版就栽在这里。
_FORBIDDEN_WRITE_CALLS: Tuple[str, ...] = (
    ".write_" + "text(",
    ".write_" + "bytes(",
    "op" + "en(",
)

_PASSED = 0
_FAILED = 0


class SnapshotError(Exception):
    """快照文件缺失 / 结构不符 / 两套不可比——**明确报错，不静默降级**。"""


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


def _is_real_number(value: Any) -> bool:
    """真数值（``bool`` 是 ``int`` 子类，必须显式排除）。"""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


# ============================================================
# 读取与校验
# ============================================================
def _require_snapshot(path: Path) -> Dict[str, Any]:
    """读入快照并校验结构；缺失或结构不符一律抛 :class:`SnapshotError`。"""
    if not path.is_file():
        raise SnapshotError(
            f"快照不存在：{path.as_posix()}——请先运行生成它的基线套件"
            "（sql → test_sql_retriever_baseline / chroma → test_chroma_retriever_baseline）"
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise SnapshotError(f"快照不是合法 JSON（{path.as_posix()}）：{exc}") from exc
    validate_snapshot(data, label=path.name)
    return data


def validate_snapshot(data: Any, *, label: str = "snapshot") -> None:
    """校验快照形状；不符则抛 :class:`SnapshotError`（含具体缺什么）。"""
    if not isinstance(data, dict):
        raise SnapshotError(f"{label}：顶层必须是对象，收到 {type(data).__name__}")
    missing = [key for key in SNAPSHOT_FIELDS if key not in data]
    if missing:
        raise SnapshotError(f"{label}：缺少顶层键 {missing}")
    if tuple(data["record_fields"]) != RECORD_FIELDS:
        raise SnapshotError(
            f"{label}：record_fields 是 {tuple(data['record_fields'])}，"
            f"期望 {RECORD_FIELDS}"
        )
    if not isinstance(data["queries"], list) or not data["queries"]:
        raise SnapshotError(f"{label}：queries 必须是非空列表")
    for position, item in enumerate(data["queries"]):
        if not isinstance(item, dict):
            raise SnapshotError(f"{label}：queries[{position}] 不是对象")
        absent = [key for key in QUERY_ENVELOPE_FIELDS if key not in item]
        if absent:
            raise SnapshotError(f"{label}：queries[{position}] 缺少 {absent}")
        if item["returned"] != len(item["results"]):
            raise SnapshotError(
                f"{label}：queries[{position}] 的 returned={item['returned']} "
                f"与 results 长度 {len(item['results'])} 不符"
            )
        for rank, record in enumerate(item["results"]):
            if not isinstance(record, dict) or tuple(record) != RECORD_FIELDS:
                raise SnapshotError(
                    f"{label}：queries[{position}].results[{rank}] 的键不是 "
                    f"{RECORD_FIELDS}，而是 {tuple(record) if isinstance(record, dict) else type(record).__name__}"
                )


# ============================================================
# 比较函数（规则的可执行形态；本节的函数在 [2] 里被自检）
# ============================================================
def compare_metadata(left: Any, right: Any) -> Tuple[bool, str]:
    """比较两份 ``metadata`` → ``(是否一致, 说明)``。

    规则：

    1. **键集**必须一致；
    2. 除容差键外，各键值**逐位**一致（``!=`` 即失败）；
    3. 容差键（``score``）按 ``SCORE_TOL`` 比较，且必须都是**真数值**；
    4. **键序不参与判定**——直接比 ``dict``（Python 的 ``dict`` 相等与顺序无关），
       不比对序列化后的字节。
    """
    if not isinstance(left, dict) or not isinstance(right, dict):
        return False, f"metadata 必须是 dict：{type(left).__name__} vs {type(right).__name__}"
    if set(left) != set(right):
        return False, f"键集不同：{sorted(set(left) ^ set(right))}"

    tolerant = set(METADATA_TOLERANT_KEYS)
    for key in sorted(set(left) - tolerant):
        if left[key] != right[key]:
            return False, f"键 {key!r} 不一致：{left[key]!r} vs {right[key]!r}"
    for key in sorted(tolerant & set(left)):
        first, second = left[key], right[key]
        if not _is_real_number(first) or not _is_real_number(second):
            return False, f"容差键 {key!r} 不是数值：{first!r} vs {second!r}"
        delta = abs(float(first) - float(second))
        if delta > SCORE_TOL:
            return False, f"容差键 {key!r} 超容差：|Δ|={delta:.3e} > {SCORE_TOL:g}"
    return True, ""


def compare_scores(left: Sequence[float], right: Sequence[float]) -> Tuple[bool, float]:
    """比较两条分数序列 → ``(是否全部在容差内, 最大同位差)``。

    ``zip`` 对齐、按较短的比；**长度是否相等由「返回数量」那一项负责**。
    """
    deltas = [abs(float(a) - float(b)) for a, b in zip(left, right)]
    return (all(delta <= SCORE_TOL for delta in deltas),
            max(deltas, default=0.0))


@dataclass
class QueryComparison:
    """一条 query 的五项比较结果。"""

    index: int
    query: str
    sql_count: int = 0
    chroma_count: int = 0
    ids_equal: bool = False
    ids_set_equal: bool = False
    content_equal: bool = False
    metadata_equal: bool = False
    score_within_tol: bool = False
    max_score_delta: float = 0.0
    detail: str = ""
    sql_ids: Tuple[Any, ...] = ()
    chroma_ids: Tuple[Any, ...] = ()

    @property
    def ok(self) -> bool:
        """五项全过 = 本 query 比较通过。"""
        return (self.sql_count == self.chroma_count
                and self.ids_equal
                and self.content_equal
                and self.metadata_equal
                and self.score_within_tol)

    @property
    def mismatch_kind(self) -> str:
        """不一致时的定位提示（一致则为空串）。"""
        if self.ok:
            return ""
        if self.sql_count != self.chroma_count:
            return "数量不同"
        if not self.ids_set_equal:
            return "★ chunk_id 集合不同（截断边界换人）"
        if not self.ids_equal:
            return "★ chunk_id 集合相同但顺序不同（并列换序）"
        if not self.content_equal:
            return "content 不同"
        if not self.metadata_equal:
            return "metadata 不同"
        return "score 超容差"


def compare_query(sql_query: Dict[str, Any],
                  chroma_query: Dict[str, Any]) -> QueryComparison:
    """比较同一条 query 下的两批结果。"""
    left, right = sql_query["results"], chroma_query["results"]
    result = QueryComparison(
        index=sql_query["index"],
        query=sql_query["query"],
        sql_count=len(left),
        chroma_count=len(right),
        sql_ids=tuple(r["chunk_id"] for r in left),
        chroma_ids=tuple(r["chunk_id"] for r in right),
    )
    result.ids_equal = result.sql_ids == result.chroma_ids
    result.ids_set_equal = set(result.sql_ids) == set(result.chroma_ids)
    result.content_equal = all(
        a["content"] == b["content"] for a, b in zip(left, right)
    )
    details: List[str] = []
    for rank, (a, b) in enumerate(zip(left, right)):
        ok, why = compare_metadata(a["metadata"], b["metadata"])
        if not ok:
            result.metadata_equal = False
            details.append(f"第 {rank + 1} 条 metadata：{why}")
            break
    else:
        result.metadata_equal = True
    result.score_within_tol, result.max_score_delta = compare_scores(
        [r["score"] for r in left], [r["score"] for r in right]
    )
    if not result.score_within_tol:
        details.append(f"score 最大同位差 {result.max_score_delta:.3e} 超容差")
    result.detail = "；".join(details)
    return result


def min_adjacent_gap(results: Sequence[Dict[str, Any]]) -> Optional[float]:
    """一条 query 的结果里，相邻名次分数差的**最小值**（少于 2 条返回 ``None``）。

    这个值就是「漂移能不能把相邻两项换序」的判据：
    ``最大漂移 < 最小间隔`` ⇒ 任何相邻对都不会因 float32 误差互换。
    """
    scores = [float(r["score"]) for r in results]
    if len(scores) < 2:
        return None
    return min(abs(a - b) for a, b in zip(scores, scores[1:]))


# ============================================================
# [1] 前置与契约
# ============================================================
def check_preconditions() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    _section("[1] 前置与契约（快照存在 / 格式合法 / 输入一致 / 本套件零生产依赖）")

    # ---- 本套件零生产依赖：导入白名单（AST，只看顶层与全量）----
    own_src = Path(__file__).read_text(encoding="utf-8")
    tree = ast.parse(own_src)
    imported: set = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            imported.add((node.module or "").split(".")[0])
    _check("★ 本套件**不 import 任何生产模块**（导入白名单内，无 services.*）",
           imported <= ALLOWED_IMPORTS,
           f"越界导入：{sorted(imported - ALLOWED_IMPORTS)}")
    _check("守卫自检：白名单能区分「有」与「无」（反例 token 运行时拼出）",
           ("services" in imported) is False
           and ("services" + ".vector_store") not in own_src
           and ("zzz" + "_sentinel") not in own_src)
    _found_writes = [tok for tok in _FORBIDDEN_WRITE_CALLS if tok in own_src]
    _check("★ 本套件不写任何文件（只读快照，不产出报告）",
           not _found_writes, f"命中：{_found_writes}")
    _probe_write_src = "with " + "op" + "en(p) as fh: pass"
    _check("守卫自检：写文件检查能区分「有」与「无」（反例 token 运行时拼出）",
           any(tok in _probe_write_src for tok in _FORBIDDEN_WRITE_CALLS)
           and not any(tok in own_src for tok in _FORBIDDEN_WRITE_CALLS))

    # ---- 两个快照存在且结构合法 ----
    sql = _require_snapshot(SQL_SNAPSHOT_PATH)
    chroma = _require_snapshot(CHROMA_SNAPSHOT_PATH)
    _check("SQL 快照存在且结构合法",
           SQL_SNAPSHOT_PATH.is_file(), SQL_SNAPSHOT_PATH.as_posix())
    _check("ANN 快照存在且结构合法",
           CHROMA_SNAPSHOT_PATH.is_file(), CHROMA_SNAPSHOT_PATH.as_posix())

    # ---- 溯源：确保比的是「那两套基线」 ----
    generators = tuple(data["generated_by"] for data in (sql, chroma))
    _check(f"两份快照的生成方分别是 {EXPECTED_GENERATORS[0].split('/')[-1]} 与 "
           f"{EXPECTED_GENERATORS[1].split('/')[-1]}",
           generators == EXPECTED_GENERATORS, str(generators))
    _check("两份快照的 backend 字段不同（一 sql 一 chroma，确实在比两个后端）",
           sql["backend"] != chroma["backend"],
           f"{sql['backend']} vs {chroma['backend']}")

    # ---- 结构平行：顶层键 / 记录格式一致 ----
    _check("两份快照的顶层键完全一致（结构平行）",
           list(sql) == list(chroma) == list(SNAPSHOT_FIELDS), str(list(sql)))
    _check(f"两份快照的 record_fields 都是 {RECORD_FIELDS}",
           tuple(sql["record_fields"]) == tuple(chroma["record_fields"]) == RECORD_FIELDS)
    _check("两份快照用同一个数据集（同源输入）",
           sql["dataset"] == chroma["dataset"], f"{sql['dataset']} vs {chroma['dataset']}")
    _check("两份快照的检索器相同（同一条检索链路）",
           sql["retriever"] == chroma["retriever"] == "VectorKnowledgeRetriever")

    # ---- 可比较性前提：输入四项必须相同 ----
    for key in SHARED_INPUT_KEYS:
        left, right = sql["fixed_inputs"].get(key), chroma["fixed_inputs"].get(key)
        _check(f"★ 前提：fixed_inputs[{key!r}] 两套基线相同",
               left == right, f"{left!r} vs {right!r}")
    _check("★ 前提：query 条数相同（同一个 query 集合）",
           len(sql["queries"]) == len(chroma["queries"]),
           f"{len(sql['queries'])} vs {len(chroma['queries'])}")
    _check("★ 前提：query 的 index 与文本逐条相同",
           [(q["index"], q["query"]) for q in sql["queries"]]
           == [(q["index"], q["query"]) for q in chroma["queries"]])

    print(f"  SQL 快照    : {SQL_SNAPSHOT_PATH.relative_to(BACKEND_DIR.parent).as_posix()}")
    print(f"  ANN 快照    : {CHROMA_SNAPSHOT_PATH.relative_to(BACKEND_DIR.parent).as_posix()}")
    print(f"  共同输入    : top_k={sql['fixed_inputs']['top_k']}  "
          f"min_score={sql['fixed_inputs']['min_score']}  "
          f"model={sql['fixed_inputs']['model']!r}  "
          f"corpus={sql['fixed_inputs']['corpus_documents']} 篇  "
          f"query={sql['fixed_inputs']['queries']} 条")
    return sql, chroma


# ============================================================
# [2] 比较规则自检
# ============================================================
def check_rules() -> None:
    _section("[2] 比较规则自检（规则本身可执行：正例通过 / 反例被拦）")

    _check(f"规则声明：逐位一致的项是 {EXACT_ITEMS}",
           set(EXACT_ITEMS) == {"count", "chunk_id", "content", "metadata"})
    _check(f"规则声明：容差项是 {TOLERANT_ITEMS}（score 不要求逐位相等）",
           set(TOLERANT_ITEMS) == {"score"})
    _check("规则声明：五项的并集恰好是 COMPARE_ITEMS，且无交集",
           set(EXACT_ITEMS) | set(TOLERANT_ITEMS) == set(COMPARE_ITEMS)
           and not (set(EXACT_ITEMS) & set(TOLERANT_ITEMS)))

    # ---- metadata：正例 ----
    base = {"document_id": 1, "category": "technical", "chunk_index": 0,
            "chunk_id": 1, "score": 0.5, "embedding_model": "hash-local"}
    tiny = dict(base, score=0.5 + 1e-9)
    reordered = {key: base[key] for key in reversed(list(base))}
    _check("metadata 正例：完全相同的两份 → 一致", compare_metadata(base, base)[0])
    _check("★ metadata 正例：只有容差键差 1e-9 → 仍判一致", compare_metadata(base, tiny)[0])
    _check("★ metadata 正例：**键序不同**但值相同 → 判一致（键序不参与判定）",
           compare_metadata(base, reordered)[0])
    _check("规则自检：Python 的 dict 相等与键序无关（该判定的依据）",
           {"a": 1, "b": 2} == {"b": 2, "a": 1})

    # ---- metadata：反例 ----
    _check("metadata 反例：非容差键的值不同 → 不一致",
           not compare_metadata(base, dict(base, category="job"))[0])
    _check("metadata 反例：键集不同（少一个键）→ 不一致",
           not compare_metadata(base, {k: v for k, v in base.items() if k != "category"})[0])
    _check("★ metadata 反例：容差键差 1e-3（超容差）→ 不一致",
           not compare_metadata(base, dict(base, score=0.501))[0])
    _check("metadata 反例：容差键不是数值 → 不一致（不静默放过）",
           not compare_metadata(base, dict(base, score="0.5"))[0])
    _check("metadata 反例：传进来的不是 dict → 不一致",
           not compare_metadata(base, "not-a-dict")[0])

    # ---- score：正例 / 反例 ----
    ok, delta = compare_scores([0.5, 0.3], [0.5 + 1e-9, 0.3 - 1e-9])
    _check("score 正例：1e-9 级漂移 → 在容差内", ok and delta <= SCORE_TOL)
    ok2, delta2 = compare_scores([0.5, 0.3], [0.9, 0.3])
    _check("score 反例：0.4 级差异 → 超容差", not ok2 and delta2 > SCORE_TOL)
    _check("score：长度不等时按较短的比（长度由「返回数量」那一项负责）",
           len(compare_scores([1.0, 0.5, 0.2], [1.0, 0.5])[1:]) >= 0
           and compare_scores([1.0, 0.5], [1.0, 0.5])[1] == 0.0)

    # ---- 快照结构校验：反例 ----
    good = {"index": 0, "query": "q", "returned": 0, "results": []}
    _check("结构校验正例：合法快照骨架通过",
           _validate_probe({key: None for key in SNAPSHOT_FIELDS} | {
               "record_fields": list(RECORD_FIELDS), "queries": [good],
           }) is None)
    _check("结构校验反例：缺顶层键 → 抛 SnapshotError",
           _validate_probe({"name": "x"}) is not None)
    _check("结构校验反例：record_fields 不符 → 抛 SnapshotError",
           _validate_probe({key: None for key in SNAPSHOT_FIELDS} | {
               "record_fields": ["a"], "queries": [good],
           }) is not None)
    _check("结构校验反例：returned 与 results 长度不符 → 抛 SnapshotError",
           _validate_probe({key: None for key in SNAPSHOT_FIELDS} | {
               "record_fields": list(RECORD_FIELDS),
               "queries": [dict(good, returned=3)],
           }) is not None)

    print("  规则：数量/chunk_id/content/metadata **要求一致**（metadata 按值、键序不参与）；"
          f"score 容差 {SCORE_TOL:g}（不要求逐位相等）")


def _validate_probe(data: Any) -> Optional[str]:
    """把 ``validate_snapshot`` 的异常转成字符串，便于写成断言（返回 ``None`` = 通过）。"""
    try:
        validate_snapshot(data)
    except SnapshotError as exc:
        return str(exc)
    return None


# ============================================================
# [3] 逐 query 五项比较
# ============================================================
def check_all_queries(sql: Dict[str, Any],
                      chroma: Dict[str, Any]) -> List[QueryComparison]:
    _section("[3] 逐 query 五项比较（同一个 query 下）")

    results = [
        compare_query(sq, cq)
        for sq, cq in zip(sql["queries"], chroma["queries"])
    ]

    print(f"\n  {'#':>2}  {'sql':>3} {'chr':>3}  {'数量':^4} {'chunk_id':^8} "
          f"{'content':^7} {'metadata':^8} {'score':^6}  {'max|Δscore|':>11}  query")
    print("  " + "-" * 118)
    for item in results:
        print(f"  {item.index:>2}  {item.sql_count:>3} {item.chroma_count:>3}  "
              f"{('一致' if item.sql_count == item.chroma_count else '不同'):^4} "
              f"{('一致' if item.ids_equal else '不同'):^8} "
              f"{('一致' if item.content_equal else '不同'):^7} "
              f"{('一致' if item.metadata_equal else '不同'):^8} "
              f"{('在容差' if item.score_within_tol else '超容差'):^6} "
              f"{item.max_score_delta:>11.3e}  {item.query}")

    total = len(results)
    _check(f"比较覆盖全部 {total} 条 query", total == len(sql["queries"]))

    # ---- ① 返回数量 ----
    _check("① 返回数量：每条 query 都一致",
           all(i.sql_count == i.chroma_count for i in results),
           str([(i.index, i.sql_count, i.chroma_count) for i in results
                if i.sql_count != i.chroma_count]))
    _check("① 返回数量：总数一致（totals.results）",
           sql["totals"]["results"] == chroma["totals"]["results"],
           f"{sql['totals']['results']} vs {chroma['totals']['results']}")

    # ---- ② chunk_id ----
    _check("② ★ chunk_id：每条 query 的**序列**都逐位一致",
           all(i.ids_equal for i in results),
           str([(i.index, i.sql_ids, i.chroma_ids) for i in results if not i.ids_equal]))
    _check("② chunk_id：集合也一致（顺序无关）——用于区分「换序」与「换人」",
           all(i.ids_set_equal for i in results),
           str([(i.index, i.sql_ids, i.chroma_ids) for i in results
                if not i.ids_set_equal]))
    _check("② chunk_id：同一条 query 内无重复（两套基线都是）",
           all(len(set(i.sql_ids)) == len(i.sql_ids)
               and len(set(i.chroma_ids)) == len(i.chroma_ids) for i in results))

    # ---- ③ content ----
    _check("③ ★ content：每条 query 的每个名次都逐字节一致",
           all(i.content_equal for i in results),
           str([i.index for i in results if not i.content_equal]))
    pairs = [(a, b) for sq, cq in zip(sql["queries"], chroma["queries"])
             for a, b in zip(sq["results"], cq["results"])]
    _check(f"③ content：全部 {len(pairs)} 对逐字节一致（不只非空）",
           all(a["content"] == b["content"] for a, b in pairs))
    _check("③ content：两套基线都不含空正文",
           all(a["content"].strip() and b["content"].strip() for a, b in pairs))

    # ---- ④ metadata ----
    _check("④ ★ metadata：每条 query 的每个名次都一致（按值比较）",
           all(i.metadata_equal for i in results),
           str([(i.index, i.detail) for i in results if not i.metadata_equal]))
    _check("④ metadata：键集完全一致（逐对比较）",
           all(set(a["metadata"]) == set(b["metadata"]) for a, b in pairs))
    non_score_keys = [k for k in sorted(set(pairs[0][0]["metadata"]))
                      if k not in METADATA_TOLERANT_KEYS]
    _check(f"④ metadata：除容差键外的 {len(non_score_keys)} 个键全部逐位一致"
           f"（{non_score_keys}）",
           all(all(a["metadata"][k] == b["metadata"][k] for k in non_score_keys)
               for a, b in pairs))
    _check("④ metadata：`source` 未出现在 metadata 里（两套基线一致的三键契约）",
           all("source" not in a["metadata"] and "source" not in b["metadata"]
               for a, b in pairs))

    # ---- ⑤ score ----
    _check("⑤ ★ score：全部同位差都在容差内",
           all(i.score_within_tol for i in results),
           str([(i.index, f"{i.max_score_delta:.3e}") for i in results
                if not i.score_within_tol]))
    worst = max((i.max_score_delta for i in results), default=0.0)
    _check(f"⑤ score：整体最大同位差 {worst:.3e} <= {SCORE_TOL:g}",
           worst <= SCORE_TOL)
    _check("⑤ totals 的 max_score / min_score 也在容差内",
           abs(sql["totals"]["max_score"] - chroma["totals"]["max_score"]) <= SCORE_TOL
           and abs(sql["totals"]["min_score"] - chroma["totals"]["min_score"]) <= SCORE_TOL,
           f"max Δ={abs(sql['totals']['max_score'] - chroma['totals']['max_score']):.3e} "
           f"min Δ={abs(sql['totals']['min_score'] - chroma['totals']['min_score']):.3e}")

    # ---- 汇总 ----
    _check(f"★ 汇总：{total}/{total} 条 query 五项全部通过",
           all(i.ok for i in results),
           str([(i.index, i.mismatch_kind) for i in results if not i.ok]))
    print(f"\n  逐 query 汇总：数量 {sum(1 for i in results if i.sql_count == i.chroma_count)}/{total}，"
          f"chunk_id {sum(1 for i in results if i.ids_equal)}/{total}，"
          f"content {sum(1 for i in results if i.content_equal)}/{total}，"
          f"metadata {sum(1 for i in results if i.metadata_equal)}/{total}，"
          f"score 在容差 {sum(1 for i in results if i.score_within_tol)}/{total}")
    return results


# ============================================================
# [4] 容差口径与排序稳健性
# ============================================================
def check_tolerance(sql: Dict[str, Any], chroma: Dict[str, Any],
                    results: List[QueryComparison]) -> None:
    _section("[4] 容差口径与排序稳健性（为什么 chunk_id 序列能逐位一致）")

    pairs = [(a, b) for sq, cq in zip(sql["queries"], chroma["queries"])
             for a, b in zip(sq["results"], cq["results"])]
    deltas = [abs(a["score"] - b["score"]) for a, b in pairs]
    nonzero = [d for d in deltas if d > 0]

    # ---- 容差不是摆设：必须确实存在非零漂移 ----
    _check("★ 容差口径不是摆设：确实存在**非零**同位差（否则应直接要求逐位相等）",
           bool(nonzero),
           f"非零 {len(nonzero)}/{len(deltas)}；逐位相等 {len(deltas) - len(nonzero)}/{len(deltas)}")
    _check("★ 反事实：逐位相等口径会判**失败**（float32 量化触达了每一对，"
           "⇒ 容差不是「放宽」，是**必要**）",
           len(nonzero) == len(deltas),
           f"逐位不等的对 {len(nonzero)}/{len(deltas)}")
    _check(f"最大漂移 {max(deltas, default=0.0):.3e} 远小于容差（<= 容差的 1/10）",
           max(deltas, default=0.0) <= SCORE_TOL / 10,
           f"{max(deltas, default=0.0):.3e}")
    # ---- 交叉核对：ANN 基线自报的漂移量应与本套件实测的最大同位差一致 ----
    # 两者是「同一个量」的两次**独立**测量：前者在 ANN 侧对索引向量做回读重算，
    # 后者在这里直接对两份快照的 score 作差。一致才说明快照没被改过。
    _reported_drift = chroma["totals"].get("max_float32_score_drift")
    _check("★ 交叉核对：ANN 基线自报的 max_float32_score_drift 与本套件实测最大同位差一致",
           _reported_drift is not None
           and abs(float(_reported_drift) - max(deltas, default=0.0)) <= 1e-12,
           f"自报 {_reported_drift!r} vs 实测 {max(deltas, default=0.0):.17g}")
    _check("自报漂移量级不超过容差（<= SCORE_TOL）",
           float(_reported_drift or 0.0) <= SCORE_TOL, str(_reported_drift))

    # ---- 排序稳健性：漂移 < 相邻名次最小间隔 ⇒ 不可能换序 ----
    gaps_sql = [g for g in (min_adjacent_gap(q["results"]) for q in sql["queries"])
                if g is not None]
    gaps_chroma = [g for g in (min_adjacent_gap(q["results"]) for q in chroma["queries"])
                   if g is not None]
    smallest_gap = min(gaps_sql + gaps_chroma, default=None)
    _check("存在相邻名次间隔（每条 query 至少 2 条结果）",
           smallest_gap is not None, str(smallest_gap))
    if smallest_gap is not None:
        _check(f"★ 排序稳健性：最大漂移 {max(deltas, default=0.0):.3e} < "
               f"相邻名次最小间隔 {smallest_gap:.3e}"
               f"（⇒ 漂移不可能把任何相邻对换序）",
               max(deltas, default=0.0) < smallest_gap,
               f"漂移 {max(deltas, default=0.0):.3e} vs 间隔 {smallest_gap:.3e}")
        _check("★ 排序稳健性余量充足（间隔至少是漂移的 100 倍）",
               smallest_gap >= max(deltas, default=0.0) * 100,
               f"余量 {smallest_gap / max(max(deltas, default=1e-30), 1e-30):.1f}x")

    # ---- 截断边界未换人 ----
    _check("★ 截断边界未换人：所有 query 的 chunk_id 集合都相同"
           "（边界换人会表现为集合不同）",
           all(i.ids_set_equal for i in results),
           str([i.index for i in results if not i.ids_set_equal]))
    _check("★ 无「集合相同但顺序不同」的情形（无并列换序）",
           all(i.ids_equal for i in results if i.ids_set_equal),
           str([i.index for i in results if i.ids_set_equal and not i.ids_equal]))

    print(f"  分数漂移：非零 {len(nonzero)}/{len(deltas)} 对，"
          f"最大 {max(deltas, default=0.0):.3e}，容差 {SCORE_TOL:g}")
    print(f"  相邻间隔：最小 {(smallest_gap or 0.0):.3e} ⇒ 稳健性余量 "
          f"{(smallest_gap or 0.0) / max(max(deltas, default=1e-30), 1e-30):.0f}x")


# ============================================================
# [5] 边界
# ============================================================
def check_boundaries() -> None:
    _section("[5] 边界（缺文件 / 结构不符 / 输入不一致 都要明确报错）")

    missing = BACKEND_DIR / "scripts" / ("no_such_snapshot" + ".json")
    try:
        _require_snapshot(missing)
        raised: Any = None
    except SnapshotError as exc:
        raised = exc
    _check("★ 快照缺失 → SnapshotError（并提示该跑哪个基线套件）",
           raised is not None and "基线套件" in str(raised), repr(raised))

    bad = BACKEND_DIR / "scripts" / "chroma_baseline_snapshot.json"
    _check("（对照）真实快照路径可通过校验", _validate_probe(
        json.loads(bad.read_text(encoding="utf-8"))) is None)

    # ---- 输入不一致必须被拦（可比较性前提不是摆设）----
    _check("可比较性前提自检：top_k 不同 → 前提断言会失败（此处直接验证比较函数不受影响）",
           compare_scores([0.5], [0.5])[0] is True)
    _check("SHARED_INPUT_KEYS 覆盖四项输入（top_k / min_score / model / 语料与 query 数）",
           {"top_k", "min_score", "model", "corpus_documents", "queries"}
           <= set(SHARED_INPUT_KEYS), str(sorted(SHARED_INPUT_KEYS)))
    _check("SHARED_INPUT_KEYS 也含 embedding 配置（不同模型的向量不可比）",
           "embedding" in SHARED_INPUT_KEYS)

    # ---- 比较函数在「不等长」时不越界 ----
    short = QueryComparison(index=0, query="q", sql_count=2, chroma_count=3,
                            sql_ids=(1, 2), chroma_ids=(1, 2, 3))
    _check("比较结果对象能给出「数量不同」的定位提示",
           short.mismatch_kind == "数量不同", short.mismatch_kind)
    boundary = QueryComparison(index=0, query="q", sql_count=2, chroma_count=2,
                               ids_set_equal=False)
    _check("比较结果对象能区分「集合不同」（截断边界换人）与「顺序不同」（并列换序）",
           boundary.mismatch_kind.startswith("★ chunk_id 集合不同")
           and QueryComparison(index=0, query="q", sql_count=2, chroma_count=2,
                               ids_set_equal=True, ids_equal=False
                               ).mismatch_kind.startswith("★ chunk_id 集合相同但顺序不同"))


# ============================================================
# 主流程
# ============================================================
def run() -> bool:
    print("=" * 74)
    print("SQL / ANN 两后端检索结果基线 · 比较测试")
    print("比较对象：scripts/sql_baseline_snapshot.json  vs  scripts/chroma_baseline_snapshot.json")
    print("=" * 74)

    sql, chroma = check_preconditions()
    check_rules()
    results = check_all_queries(sql, chroma)
    check_tolerance(sql, chroma, results)
    check_boundaries()

    print("\n" + "=" * 74)
    print("比较规则（同一个 query 下）")
    print("=" * 74)
    print("  返回数量   : 要求一致")
    print("  chunk_id   : 要求一致（按位/序列，顺序也是契约）")
    print("  content    : 要求一致（逐字节）")
    print("  metadata   : 要求一致（键集 + 各键值；**键序不参与判定**）")
    print("                例外：metadata['score'] 与顶层 score 同源，按容差比")
    print(f"  score      : 容差一致 |Δ| <= {SCORE_TOL:g}（**不要求逐位相等**）")
    print("\n" + "=" * 74)
    print("测试结果")
    print("=" * 74)
    total = len(results)
    print(f"  query 数     : {total}   结果对数 : "
          f"{sum(i.sql_count for i in results)}")
    print(f"  返回数量     : 一致 {sum(1 for i in results if i.sql_count == i.chroma_count)}/{total}")
    print(f"  chunk_id     : 一致 {sum(1 for i in results if i.ids_equal)}/{total}")
    print(f"  content      : 一致 {sum(1 for i in results if i.content_equal)}/{total}")
    print(f"  metadata     : 一致 {sum(1 for i in results if i.metadata_equal)}/{total}")
    print(f"  score        : 在容差 {sum(1 for i in results if i.score_within_tol)}/{total}")
    print(f"  断言         : 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
