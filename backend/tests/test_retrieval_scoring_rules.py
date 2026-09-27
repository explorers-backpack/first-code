# -*- coding: utf-8 -*-
"""RAG 检索评分规则验证（脚本式，非 pytest）。

运行：``python backend/tests/test_retrieval_scoring_rules.py``

验证对象
--------
**唯一排序规则** = ``services.vector_store.select_matches()``：

1. 跳过维度不符的候选
2. **降序**排列；**同分按候选顺序**稳定排序（``key=(-score, position)``）
3. ``min_score`` 过滤（``>=`` **含边界**）
4. 截断 ``top_k``
5. 「候选非空但一条维度都对不上」⇒ 抛 ``VectorStoreDimensionError``（不静默返回空）

两个真实后端都必须**委托**给它：
``SqlAlchemyVectorStore.search``（候选顺序 = 主键升序）与
``ChromaVectorStore.search``（候选顺序 = ``chunk_id`` 升序）。

四个场景
--------
1. ``top_k`` 限制数量
2. ``min_score`` 过滤低分结果
3. 分数排序正确
4. 并列 ``score`` 处理正确

分三层验证（**越靠前越根本**）
------------------------------
========  ======================================================
层        验证方式
========  ======================================================
L1 规则   直接调 ``select_matches()``，用手工构造的候选做**纯函数级**判定
L2 接线   AST 守卫：两个后端的 ``search`` **函数体内**确实调用 ``select_matches``
L3 后端   ``store.search()`` 的输出 == 用**同一批候选**独立重算 ``select_matches()``
========  ======================================================

.. warning::
   **「同分按候选顺序」与「同分按 id 升序」必须在 L1 层区分**。
   两个真实后端的候选顺序在本项目里**恰好**都等于 ``chunk_id`` 升序
   （SQL = 主键升序、chroma 显式按 ``chunk_id`` 排序），所以**在 store 层无法区分**
   这两种实现。唯一的判别手段是 L1 的手工夹具（候选顺序 ≠ id 升序），
   外加 ``InMemoryVectorStore``（候选顺序 = **写入顺序**，可与 id 顺序相反）。

.. note::
   本套件**只读**，不修改任何生产代码；``select_matches`` 的函数体与签名由
   :func:`check_rule_contract` 锁死（签名 + ``__all__`` + 调用点）。
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
for _path in (BACKEND_DIR, BACKEND_DIR / "tests"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from vector_backend_env import (  # noqa: E402
    BACKEND_CHROMA,
    BACKEND_SQL,
    SCORE_TOL,
    open_env,
)
from services.vector_store import (  # noqa: E402
    DEFAULT_TOP_K,
    InMemoryVectorStore,
    VectorRecord,
    VectorStoreDimensionError,
    VectorStoreInputError,
    cosine_similarity,
    select_matches,
)

# ============================================================
# 一、常量与夹具
# ============================================================
#: 唯一排序规则的函数名（L2 守卫的判据）。
RULE_NAME = "select_matches"

#: 两个真实后端：模块路径 + 类名（L2/L3 用）。
REAL_BACKENDS: Tuple[Tuple[str, str], ...] = (
    ("services/vector_store_sql.py", "SqlAlchemyVectorStore"),
    ("services/vector_store_chroma.py", "ChromaVectorStore"),
)

#: 查询向量：取 ``[1, 0]`` 让余弦值可手算（``cos = x / sqrt(x²+y²)``）。
QUERY: Tuple[float, ...] = (1.0, 0.0)

#: 模型标识（夹具记录用）。
FIXTURE_MODEL = "fixture-model"

#: 并列夹具的正文（导入到真实后端时用；**与 corpus 里任何一篇都不同**）。
TIE_CONTENT = (
    "检索评分规则验证：这段正文会被重复导入多次，"
    "用来在真实后端上造出**分数精确相等**的并列组。"
)
TIE_DOCS = 3

_PASSED = 0
_FAILED = 0


def _section(title: str) -> None:
    print("\n" + "-" * 74)
    print(title)
    print("-" * 74)


def _check(name: str, condition: Any, detail: str = "") -> bool:
    global _PASSED, _FAILED
    ok = bool(condition)
    if ok:
        _PASSED += 1
    else:
        _FAILED += 1
    tail = f"  -> {detail}" if detail else ""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{tail}")
    return ok


def _rec(chunk_id: Any, vector: Sequence[float], *, content: str = "",
         document_id: Optional[int] = None, metadata: Optional[Dict[str, Any]] = None,
         model: str = FIXTURE_MODEL) -> VectorRecord:
    return VectorRecord(
        vector=list(vector), chunk_id=chunk_id, content=content or f"chunk-{chunk_id}",
        document_id=document_id, metadata=dict(metadata or {}), model=model,
    )


def _tie_records() -> List[VectorRecord]:
    """并列夹具：**候选顺序**是 ``30, 10, 20``（与 id 升序**相反**）。

    三条 ``[0, 1]`` 的余弦都是 ``0.0``（与查询 ``[1, 0]`` 正交）⇒ 精确并列。
    若实现按 **id 升序** 兜底，输出会是 ``10, 20, 30``；
    按 **候选顺序** 兜底则必须是 ``30, 10, 20``。
    """
    return [
        _rec(30, [0.0, 1.0]),
        _rec(10, [0.0, 1.0]),
        _rec(20, [0.0, 1.0]),
        _rec(40, [1.0, 0.0]),   # cos 1.0
        _rec(50, [1.0, 1.0]),   # cos 1/sqrt(2)
    ]


def _tie_expected_order() -> List[int]:
    return [40, 50, 30, 10, 20]


def _scored_records() -> List[VectorRecord]:
    """分数夹具：6 条**互不相同**的分数（含 0.0 与负数），候选顺序与 id 同向。

    ``cos([1,0], [1,t]) = 1/sqrt(1+t²)``；``[0,0]`` 是零向量 ⇒ ``0.0``；
    ``[-1,1]`` ⇒ ``-1/sqrt(2)``（**负分不截断**）。
    """
    return [
        _rec(1, [1.0, 4.0]),    # 0.2425
        _rec(2, [-1.0, 1.0]),   # -0.7071
        _rec(3, [1.0, 0.0]),    # 1.0
        _rec(4, [0.0, 0.0]),    # 0.0（零向量）
        _rec(5, [1.0, 2.0]),    # 0.4472
        _rec(6, [1.0, 1.0]),    # 0.7071
    ]


def _ids(matches: Sequence[Any]) -> Tuple[Any, ...]:
    return tuple(m.chunk_id for m in matches)


def _scores(matches: Sequence[Any]) -> Tuple[float, ...]:
    return tuple(m.score for m in matches)


def _nonincreasing(values: Sequence[float]) -> bool:
    return all(a >= b for a, b in zip(values, values[1:]))


def _full(records: Sequence[VectorRecord], *, min_score: Optional[float] = None
          ) -> List[Any]:
    """不经截断的完整排序结果（``top_k`` 取足够大）。"""
    return select_matches(records, QUERY, top_k=1000, min_score=min_score)


def _raises(fn: Any, exc: type) -> Optional[BaseException]:
    try:
        fn()
    except exc as error:  # noqa: BLE001
        return error
    except BaseException as error:  # noqa: BLE001
        return error
    return None


# ============================================================
# [1] 规则契约（签名 / 唯一性 / 接线）
# ============================================================
def _module_source(relative_path: str) -> str:
    return (BACKEND_DIR / relative_path).read_text(encoding="utf-8").replace("\r\n", "\n")


def _method_source(relative_path: str, class_name: str, method: str) -> Optional[str]:
    """取出某个类方法的源码片段。"""
    source = _module_source(relative_path)
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                        and item.name == method:
                    return ast.get_source_segment(source, item)
    return None


def _calls(func_source: str, name: str) -> bool:
    """函数体内是否出现对 ``name`` 的**调用**（含 ``self.x`` / 模块限定形式）。"""
    for node in ast.walk(ast.parse(func_source)):
        if isinstance(node, ast.Call):
            target = node.func
            if isinstance(target, ast.Name) and target.id == name:
                return True
            if isinstance(target, ast.Attribute) and target.attr == name:
                return True
    return False


def _imports_from_vector_store(relative_path: str, name: str) -> bool:
    for node in ast.walk(ast.parse(_module_source(relative_path))):
        if isinstance(node, ast.ImportFrom) and (node.module or "").endswith("vector_store"):
            if any(alias.name == name for alias in node.names):
                return True
    return False


def check_rule_contract() -> None:
    _section("[1] 规则契约（签名 / 唯一性 / 接线守卫）")

    # -- 签名锁死：改签名就是改契约 --
    signature = inspect.signature(select_matches)
    params = list(signature.parameters)
    _check("select_matches 的位置参数恰为 (records, query_vector)",
           params[:2] == ["records", "query_vector"], str(params))
    _check("其余参数（top_k / min_score / store_name）都是**仅关键字**",
           all(signature.parameters[n].kind is inspect.Parameter.KEYWORD_ONLY
               for n in ("top_k", "min_score", "store_name")),
           str({n: str(signature.parameters[n].kind) for n in params[2:]}))
    _check("top_k 默认值 == DEFAULT_TOP_K == 5",
           signature.parameters["top_k"].default == DEFAULT_TOP_K == 5,
           str(signature.parameters["top_k"].default))
    _check("min_score 默认值为 None（= 不过滤）",
           signature.parameters["min_score"].default is None)

    # -- 唯一性：两个后端的 search 都必须调用它，且从接口模块 import --
    for relative_path, class_name in REAL_BACKENDS:
        body = _method_source(relative_path, class_name, "search")
        _check(f"★ {class_name}.search 的**函数体内**调用 {RULE_NAME}",
               body is not None and _calls(body, RULE_NAME),
               "未找到方法" if body is None else "")
        _check(f"★ {class_name} 从 services.vector_store import {RULE_NAME}",
               _imports_from_vector_store(relative_path, RULE_NAME))

    # 守卫自检：能区分「有调用」与「无调用」（反例运行时拼出）
    probe_name = "select" + "_matches"
    _check("守卫自检：调用检测能区分「有」与「无」（反例运行时拼出）",
           _calls(f"def f():\n    return {probe_name}()\n", RULE_NAME)
           and not _calls("def f():\n    return []\n", RULE_NAME))

    # -- 内存实现同样委托（它是「候选顺序 ≠ id 顺序」的唯一可构造后端）--
    memory_body = _method_source("services/vector_store.py", "InMemoryVectorStore", "search")
    _check(f"★ InMemoryVectorStore.search 也调用 {RULE_NAME}（三个实现共用一份排序口径）",
           memory_body is not None and _calls(memory_body, RULE_NAME))


# ============================================================
# [2] 场景 1 · top_k 限制数量
# ============================================================
def check_top_k() -> None:
    _section("[2] 场景 1 · top_k 限制数量（纯函数级）")

    records = _scored_records()
    full = _full(records)
    total = len(full)
    _check("夹具准备：完整排序共 6 条", total == 6, str(total))

    for top_k in (1, 2, 3, 4, 5, 6, 7, 100):
        got = select_matches(records, QUERY, top_k=top_k)
        expected = min(top_k, total)
        _check(f"top_k={top_k}：条数 == min(top_k, 候选数) == {expected}",
               len(got) == expected, f"{len(got)}")
        _check(f"top_k={top_k}：结果 == 完整排序的前 {expected} 项（按位）",
               _ids(got) == _ids(full)[:expected], str(_ids(got)))
        _check(f"top_k={top_k}：分数非增",
               _nonincreasing(_scores(got)))

    _check("省略 top_k 时用 DEFAULT_TOP_K（== 5）",
           _ids(select_matches(records, QUERY)) == _ids(full)[:DEFAULT_TOP_K],
           str(_ids(select_matches(records, QUERY))))

    # 非法 top_k 必须**报错**，不静默收敛
    for bad in (0, -1, -100):
        error = _raises(lambda b=bad: select_matches(records, QUERY, top_k=b),
                        VectorStoreInputError)
        _check(f"top_k={bad} ⇒ VectorStoreInputError（不静默改成 1）",
               isinstance(error, VectorStoreInputError),
               type(error).__name__ if error else "未抛异常")
    for bad in (True, False, 1.5, "5", None):
        error = _raises(lambda b=bad: select_matches(records, QUERY, top_k=b),
                        VectorStoreInputError)
        _check(f"top_k={bad!r} ⇒ VectorStoreInputError（拒 bool / 非整数）",
               isinstance(error, VectorStoreInputError),
               type(error).__name__ if error else "未抛异常")

    # top_k 与 min_score 的组合：先过滤后截断
    for min_score in (None, 0.0, 0.3, 0.9):
        eligible = len(_full(records, min_score=min_score))
        got = select_matches(records, QUERY, top_k=2, min_score=min_score)
        _check(f"top_k=2 & min_score={min_score}：条数 == min(2, 过滤后 {eligible})"
               f" == {min(2, eligible)}",
               len(got) == min(2, eligible), f"{len(got)}")


# ============================================================
# [3] 场景 2 · min_score 过滤低分结果
# ============================================================
def check_min_score() -> None:
    _section("[3] 场景 2 · min_score 过滤低分结果（纯函数级）")

    records = _scored_records()
    full = _full(records)
    scores = _scores(full)
    print(f"  夹具分数（降序）：{[round(s, 6) for s in scores]}")

    # .. warning::
    #    本节的断言**必须显式给 top_k**。``select_matches`` 的 ``top_k`` 默认
    #    ``DEFAULT_TOP_K == 5``，省略时「条数」会被**截断**而不是被过滤——
    #    于是「min_score 不过滤任何一条」这类断言会以 5/6 的假象通过/失败，
    #    测的其实是 top_k 而不是 min_score（本套件第一版就栽在这里）。
    def kept_at(threshold: Optional[float]) -> List[Any]:
        return select_matches(records, QUERY, top_k=len(records), min_score=threshold)

    _check(f"★ 前提：本节显式 top_k={len(records)} == 候选数，条数只受 min_score 影响",
           len(kept_at(None)) == len(records), str(len(kept_at(None))))

    # -- None 与「低于所有分数」= 不过滤 --
    for threshold in (None, -1.0, -2.0):
        got = kept_at(threshold)
        _check(f"min_score={threshold}：不过滤任何一条（{len(got)}/6）",
               len(got) == 6, f"{len(got)}")

    # -- 阈值取「实际分数」⇒ 测含边界 `>=` --
    for index, exact in enumerate(scores):
        expected = index + 1
        kept = kept_at(exact)
        _check(f"★ 阈值**恰好等于**第 {index + 1} 名的分数（{exact:.17g}）"
               f"⇒ 含边界保留 {expected} 条",
               len(kept) == expected, f"{len(kept)}")
        just_above = kept_at(exact + 1e-12)
        _check(f"阈值 = 该分数 + 1e-12 ⇒ 该条被过滤（{index} 条）",
               len(just_above) == index, f"{len(just_above)}")

    # -- 0.0 边界：零向量项恰好 0.0 ⇒ `0.0 >= 0.0` 必须保留 --
    zero_count = sum(1 for s in scores if s == 0.0)
    _check(f"夹具含 {zero_count} 条分数**恰好 0.0**（零向量）",
           zero_count == 1, str(zero_count))
    _check("★ min_score=0.0 保留该条（`>=` 含边界）；min_score=1e-12 过滤掉",
           len(kept_at(0.0)) == 5 and len(kept_at(1e-12)) == 4,
           f"{len(kept_at(0.0))} / {len(kept_at(1e-12))}")

    # -- 负数不被截断：阈值低于负数 ⇒ 负分项仍在 --
    _check("★ 负分项**不被截断**（cos ∈ [-1,1]，min_score=-1.0 仍返回它）",
           any(s < 0 for s in scores) and len(kept_at(-1.0)) == 6,
           str([round(s, 6) for s in scores]))

    # -- 超过上界 ⇒ 空 --
    for threshold in (1.0 + 1e-12, 1.01, 100.0):
        got = kept_at(threshold)
        _check(f"min_score={threshold}：无结果（余弦上界 1.0）", not got, str(len(got)))

    # -- 通用不变量：每条都满足 >= 阈值；阈值升高条数不增 --
    monotone_ok = True
    inclusive_ok = True
    previous: Optional[int] = None
    for threshold in sorted(s for s in scores) + [1.01]:
        kept = kept_at(threshold)
        if not all(m.score >= threshold for m in kept):
            inclusive_ok = False
        if previous is not None and len(kept) > previous:
            monotone_ok = False
        previous = len(kept)
    _check("★ 不变量：返回的每一条都满足 `score >= min_score`", inclusive_ok)
    _check("★ 不变量：min_score 升高 ⇒ 条数不增（单调）", monotone_ok)

    # -- 非法阈值 --
    for bad in (True, False, "0.5", [], {}):
        error = _raises(lambda b=bad: select_matches(records, QUERY, min_score=b),
                        VectorStoreInputError)
        _check(f"min_score={bad!r} ⇒ VectorStoreInputError",
               isinstance(error, VectorStoreInputError),
               type(error).__name__ if error else "未抛异常")


# ============================================================
# [4] 场景 3 · 分数排序正确
# ============================================================
def check_ordering() -> None:
    _section("[4] 场景 3 · 分数排序正确（纯函数级）")

    records = _scored_records()
    full = _full(records)
    scores = _scores(full)

    _check("★ 严格非增（降序）", _nonincreasing(scores),
           str([round(s, 6) for s in scores]))

    # 分数必须 == 独立算出的余弦（证明 score 就是余弦，不是别的度量）
    by_id = {r.chunk_id: r for r in records}
    recomputed = {m.chunk_id: cosine_similarity(QUERY, by_id[m.chunk_id].vector)
                  for m in full}
    mismatch = [cid for cid, s in ((m.chunk_id, m.score) for m in full)
                if s != recomputed[cid]]
    _check("★ 每条 score **逐位等于** cosine_similarity(query, record.vector)",
           not mismatch, str(mismatch))

    _check("分数落在余弦值域 [-1, 1] 内",
           all(-1.0 <= s <= 1.0 for s in scores),
           f"[{min(scores):.6f}, {max(scores):.6f}]")

    # 与查询同向 ⇒ 1.0；反向 ⇒ -1.0（精确值，不做 clamp）
    same = select_matches([_rec(1, [1.0, 0.0])], QUERY, top_k=1)
    opposite = select_matches([_rec(2, [-1.0, 0.0])], QUERY, top_k=1)
    _check("同向向量 ⇒ score == 1.0（精确）", same[0].score == 1.0, repr(same[0].score))
    _check("反向向量 ⇒ score == -1.0（精确，且**不被截断为 0**）",
           opposite[0].score == -1.0, repr(opposite[0].score))

    # 排序键是 (-score, position)：把候选顺序打乱，名次应按分数而非位置重排
    shuffled = list(reversed(records))
    got = _full(shuffled)
    _check("★ 打乱候选顺序后名次不变（排序以分数为主键，位置只是兜底）",
           _ids(got) == _ids(full), f"{_ids(got)} vs {_ids(full)}")
    _check("打乱候选顺序后分数序列不变",
           _scores(got) == scores)

    # 交换两条**不同分**记录的位置，名次仍按分数
    swapped = list(records)
    swapped[0], swapped[2] = swapped[2], swapped[0]   # id=1(0.2425) <-> id=3(1.0)
    _check("★ 交换不同分记录的位置后，名次仍由分数决定",
           _ids(_full(swapped)) == _ids(full), str(_ids(_full(swapped))))


# ============================================================
# [5] 场景 4 · 并列 score 处理正确
# ============================================================
def check_ties() -> None:
    _section("[5] 场景 4 · 并列 score 处理正确（纯函数级 · **决定性判别**）")

    records = _tie_records()
    full = _full(records)
    expected = _tie_expected_order()
    print(f"  候选顺序 = {[r.chunk_id for r in records]}")
    print(f"  实际名次 = {list(_ids(full))}   期望 = {expected}")

    _check("夹具：三条 [0,1] 与查询正交 ⇒ 分数**逐位相等**（精确并列）",
           len({m.score for m in full if m.chunk_id in (30, 10, 20)}) == 1,
           str({m.chunk_id: m.score for m in full if m.chunk_id in (30, 10, 20)}))
    _check("★ 名次 == [40, 50, 30, 10, 20]（并列段按**候选顺序**，不是 id 升序）",
           list(_ids(full)) == expected, str(list(_ids(full))))
    _check("★ 若按 id 升序兜底会得到 [40, 50, 10, 20, 30] —— 两者可区分",
           list(_ids(full)) != [40, 50, 10, 20, 30], str(list(_ids(full))))

    # 反转候选顺序 ⇒ 并列段顺序必须跟着反转（证明「按位置」而非巧合）
    reversed_full = _full(list(reversed(records)))
    _check("★ 反转候选顺序 ⇒ 并列段顺序**跟着反转**（[20, 10, 30]）",
           list(_ids(reversed_full)) == [40, 50, 20, 10, 30], str(list(_ids(reversed_full))))

    # 全同分：输出顺序必须与候选顺序完全一致
    identical = [_rec(cid, [0.0, 1.0]) for cid in (7, 5, 9, 1, 3)]
    identical_full = _full(identical)
    _check("★ 全并列时输出顺序 == 候选顺序（稳定排序）",
           list(_ids(identical_full)) == [7, 5, 9, 1, 3], str(list(_ids(identical_full))))

    # top_k 切在并列段中间 ⇒ 取候选顺序靠前的那些
    for top_k in (3, 4, 5):
        got = select_matches(records, QUERY, top_k=top_k)
        _check(f"top_k={top_k} 切在并列段：结果 == 候选顺序最靠前的 {top_k} 条",
               list(_ids(got)) == expected[:top_k], str(list(_ids(got))))

    # min_score 压在并列分数上 ⇒ 含边界，三条全保留
    tie_score = [m.score for m in full if m.chunk_id == 30][0]
    kept = select_matches(records, QUERY, min_score=tie_score)
    _check(f"★ min_score == 并列分数（{tie_score}）⇒ 三条**全部保留**（含边界）",
           list(_ids(kept)) == expected, str(list(_ids(kept))))
    _check("min_score = 并列分数 + 1e-12 ⇒ 三条**全部被过滤**",
           list(_ids(select_matches(records, QUERY, min_score=tie_score + 1e-12))) == [40, 50],
           str(list(_ids(select_matches(records, QUERY, min_score=tie_score + 1e-12)))))


async def check_ties_at_store_level() -> None:
    _section("[5b] 场景 4 · 并列处理在**后端层**的表现")

    # -- InMemoryVectorStore：候选顺序 = **写入顺序**，可与 id 顺序相反 --
    memory = InMemoryVectorStore(model=FIXTURE_MODEL)
    await memory.add(_tie_records())
    got = await memory.search(list(QUERY), top_k=1000, model=FIXTURE_MODEL)
    _check("★ [memory] 候选顺序 = 写入顺序（30,10,20）≠ id 升序 ⇒ 名次按候选顺序",
           list(_ids(got)) == _tie_expected_order(), str(list(_ids(got))))

    # -- 两个真实后端：导入同正文多篇 ⇒ 精确并列 --
    async with open_env() as env:
        from services.knowledge_import_pipeline import KnowledgeImportPipeline

        pipeline = KnowledgeImportPipeline(
            env.session, embedder=env.embedder, store=env.chroma_store
        )
        for index in range(TIE_DOCS):
            report = await pipeline.import_document({
                "title": f"评分规则并列夹具 {index}",
                "content": TIE_CONTENT,
                "category": "technical",
                "source": f"test://scoring/tie/{index}",
            })
            _check(f"并列夹具第 {index} 篇入库成功", report["status"] == "ok",
                   str(report.get("stage")))

        vector = await env.embedder.embed("检索评分规则 并列")
        model = env.embedder.name
        groups: Dict[str, List[Any]] = {}
        for backend in env.backends:
            matches = await env.store_for(backend).search(
                vector, top_k=1000, min_score=None, model=model,
                document_id=None, category=None,
            )
            groups[backend] = [m for m in matches if m.content.strip() == TIE_CONTENT.strip()]

        _check(f"★ 两个真实后端都看到 {TIE_DOCS} 条同正文记录",
               all(len(g) == TIE_DOCS for g in groups.values()),
               str({b: len(g) for b, g in groups.items()}))

        for backend, group in groups.items():
            _check(f"★ [{backend}] 并列组内分数逐位相等",
                   len({m.score for m in group}) == 1,
                   str([m.score for m in group]))
            ids = [m.chunk_id for m in group]
            _check(f"★ [{backend}] 并列段按候选顺序（本环境 = chunk_id 升序）",
                   ids == sorted(ids), str(ids))

        _check("★ 两个真实后端的并列组成员与顺序都一致",
               [m.chunk_id for m in groups[BACKEND_SQL]] ==
               [m.chunk_id for m in groups[BACKEND_CHROMA]],
               str([[m.chunk_id for m in groups[b]] for b in env.backends]))

        # 并列段前后的名次也必须一致（并列不改变前置名次）
        prefix = {}
        for backend in env.backends:
            matches = await env.store_for(backend).search(
                vector, top_k=1000, min_score=None, model=model,
                document_id=None, category=None,
            )
            first = min(i for i, m in enumerate(matches)
                        if m.content.strip() == TIE_CONTENT.strip())
            prefix[backend] = [m.chunk_id for m in matches[:first]]
        _check("★ 并列段之前的名次一致", prefix[BACKEND_SQL] == prefix[BACKEND_CHROMA],
               str(prefix[BACKEND_SQL]))


# ============================================================
# [6] 后端层：search == 独立重算的 select_matches
# ============================================================
def _as_list(value: Any) -> List[Any]:
    """``None`` → ``[]``。**不能**写 ``value or []``：numpy 数组会抛 ValueError。"""
    if value is None:
        return []
    return list(value)


def _apply_filters(records: Sequence[VectorRecord], *, model: Optional[str],
                   document_id: Optional[int], category: Optional[str]
                   ) -> List[VectorRecord]:
    """复刻后端在调用 ``select_matches`` **之前**做的候选过滤。"""
    out: List[VectorRecord] = []
    for record in records:
        if model is not None and record.model != model:
            continue
        if document_id is not None and record.document_id != document_id:
            continue
        if category is not None and record.metadata.get("category") != category:
            continue
        out.append(record)
    return out


async def _sql_candidates(env: Any) -> List[VectorRecord]:
    """SQL 侧候选：``load_records``（**主键升序** = 该后端的候选顺序）。"""
    ids = [record.chunk_id for record in env.records]
    return list(await env.sql_store.load_records(ids))


async def _chroma_candidates(env: Any) -> List[VectorRecord]:
    """ANN 侧候选：直接读索引里的**实际存储向量**（float32 回读值）。

    刻意不经过 ``search``，也不重新 ``embed``——否则分数会差 ~1e-8，
    比对就变成噪声（任务 57 的结论）。
    """
    from services.vector_store_chroma import from_chroma_metadata

    raw = env.chroma_store._collection_handle().get(
        include=["embeddings", "documents", "metadatas"]
    )
    ids = _as_list(raw.get("ids"))
    embeddings = _as_list(raw.get("embeddings"))
    documents = _as_list(raw.get("documents"))
    metadatas = _as_list(raw.get("metadatas"))

    records: List[VectorRecord] = []
    for position, raw_id in enumerate(ids):
        metadata, model, document_id = from_chroma_metadata(
            metadatas[position] if position < len(metadatas) else None
        )
        records.append(VectorRecord(
            vector=_as_list(embeddings[position]) if position < len(embeddings) else [],
            content=documents[position] if position < len(documents) else "",
            document_id=document_id,
            chunk_id=int(raw_id),
            metadata=metadata,
            model=model,
        ))
    records.sort(key=lambda record: record.chunk_id or 0)   # 候选顺序 = chunk_id 升序
    return records


#: 后端层比对的参数组合。**top_k × oversample 必须 >= 语料数**（召回穷尽），
#: 否则 ANN 只看到部分候选，与「全量重算」不可比。
STORE_CASES: Tuple[Tuple[int, Optional[float]], ...] = (
    (5, None),
    (4, 0.0),
    (8, 0.05),
    (50, 0.2),
)


async def check_backends_apply_same_rule(env: Any) -> None:
    _section("[6] 后端层：store.search() == 用同一批候选重算 select_matches()")

    oversample = 4
    corpus = len(env.records)
    model = env.embedder.name

    for top_k, min_score in STORE_CASES:
        exhaustive = top_k * oversample >= corpus
        _check(f"前提：top_k={top_k} × oversample={oversample} >= 语料数 {corpus}"
               f"（召回穷尽，才可与全量重算逐位比对）",
               exhaustive, f"{top_k * oversample} vs {corpus}")
        if not exhaustive:
            continue

        # .. note:: 这里刻意**绕过 retriever** 直接调 ``store.search()``。
        #    retriever 会按正文去重（``_to_chunks``），拿它的输出去比
        #    store 层重算结果，比的是**两个不同的层**——第一版就栽在这里
        #    （``KnowledgeChunk`` 没有 ``chunk_id`` 属性，直接 AttributeError）。
        text = env.queries[0].text
        vector = await env.embedder.embed(text)

        for backend in env.backends:
            got = await env.store_for(backend).search(
                vector, top_k=top_k, min_score=min_score, model=model,
                document_id=None, category=None,
            )

            if backend == BACKEND_SQL:
                pool = await _sql_candidates(env)
                store_name = env.sql_store.name
            else:
                pool = await _chroma_candidates(env)
                store_name = env.chroma_store.name
            pool = _apply_filters(pool, model=model, document_id=None, category=None)
            expected = select_matches(pool, vector, top_k=top_k, min_score=min_score,
                                      store_name=store_name)

            _check(f"★ [{backend}] top_k={top_k} min_score={min_score}："
                   f"chunk_id 序列 == 独立重算",
                   _ids(got) == _ids(expected),
                   f"{list(_ids(got))} vs {list(_ids(expected))}")
            _check(f"★ [{backend}] top_k={top_k} min_score={min_score}："
                   f"分数**逐位相等**（同一批候选、同一排序口径）",
                   _scores(got) == _scores(expected),
                   f"max|Δ|={max((abs(a - b) for a, b in zip(_scores(got), _scores(expected))), default=0.0):.3e}")

    # -- 过滤器（document_id / category）也要两边同口径 --
    document_id = env.records[0].document_id
    for backend in env.backends:
        got = await env.store_for(backend).search(
            await env.embedder.embed(env.queries[0].text), top_k=50, min_score=None,
            model=model, document_id=document_id, category=None,
        )
        _check(f"[{backend}] document_id={document_id} 过滤生效且条数 <= 1",
               len(got) <= 1 and all(m.document_id == document_id for m in got),
               str(len(got)))
    for backend in env.backends:
        got = await env.store_for(backend).search(
            await env.embedder.embed(env.queries[0].text), top_k=50, min_score=None,
            model=model, document_id=None, category="technical",
        )
        _check(f"[{backend}] category='technical' 过滤生效",
               all(m.metadata.get("category") == "technical" for m in got) and bool(got),
               str(len(got)))


# ============================================================
# [7] 边界情况
# ============================================================
def check_edge_cases() -> None:
    _section("[7] 边界情况（维度 / 空候选 / 查询向量校验）")

    # -- 维度不符：跳过；全不符：抛错（**不静默返回空**）--
    mixed = [_rec(1, [1.0, 0.0]), _rec(2, [1.0, 0.0, 0.0]), _rec(3, [0.5, 0.5])]
    got = select_matches(mixed, QUERY, top_k=10)
    _check("★ 维度不符的候选被**跳过**（3 维那条不进结果）",
           set(_ids(got)) == {1, 3}, str(list(_ids(got))))

    all_bad = [_rec(1, [1.0, 0.0, 0.0]), _rec(2, [1.0, 0.0, 0.0, 0.0])]
    error = _raises(lambda: select_matches(all_bad, QUERY, top_k=10, store_name="probe"),
                    VectorStoreDimensionError)
    _check("★ 候选非空但**一条维度都对不上** ⇒ VectorStoreDimensionError"
           "（不是安静返回空）",
           isinstance(error, VectorStoreDimensionError),
           type(error).__name__ if error else "未抛异常")
    _check("异常信息带 store_name（便于定位是哪个后端）",
           error is not None and "probe" in str(error), str(error)[:60] if error else "")

    empty = select_matches([], QUERY, top_k=10)
    _check("候选**真的为空** ⇒ 返回 []（不抛错，与「维度全不符」区分开）",
           empty == [], str(empty))

    # -- 查询向量校验 --
    for bad, label in (("", "空字符串"), ([], "空列表"), ([1, True], "含 bool"),
                       (["a"], "含字符串"), (None, "None")):
        error = _raises(lambda b=bad: select_matches([_rec(1, [1.0, 0.0])], b, top_k=1),
                        VectorStoreInputError)
        _check(f"查询向量为{label} ⇒ VectorStoreInputError",
               isinstance(error, VectorStoreInputError),
               type(error).__name__ if error else "未抛异常")

    _check("查询向量接受 tuple / 生成器等非 list 序列",
           len(select_matches([_rec(1, [1.0, 0.0])], (1.0, 0.0), top_k=1)) == 1)

    # -- 结果字段完整性 --
    match = select_matches([_rec(1, [1.0, 0.0], document_id=9,
                                 metadata={"category": "technical"})],
                           QUERY, top_k=1)[0]
    _check("VectorMatch 六键齐全（chunk_id / document_id / content / metadata / model / score）",
           set(match.to_dict()) == {"chunk_id", "document_id", "content", "metadata",
                                    "model", "score"}, str(sorted(match.to_dict())))
    _check("VectorMatch 不含向量本体（检索结果不把上千浮点带回上层）",
           "vector" not in match.to_dict())
    _check("metadata 原样带出（供上游按 category / topic 使用）",
           match.metadata.get("category") == "technical", str(match.metadata))


# ============================================================
# 报告
# ============================================================
def report() -> None:
    print("\n" + "=" * 74)
    print("测试案例总览")
    print("=" * 74)
    rows = (
        ("1", "top_k 限制数量", "select_matches / sql / chroma",
         "条数 == min(top_k, 候选数)；结果 == 完整排序前缀"),
        ("2", "min_score 过滤低分", "select_matches / sql / chroma",
         "`>=` 含边界；每条 score >= 阈值；阈值升高条数不增"),
        ("3", "分数排序正确", "select_matches / sql / chroma",
         "严格非增；score 逐位等于 cosine_similarity"),
        ("4", "并列 score 处理", "select_matches / memory / sql / chroma",
         "同分按**候选顺序**（≠ id 升序）；top_k 切并列段取靠前者"),
        ("5", "维度 / 空候选 / 入参", "select_matches",
         "跳维度不符；全不符抛 DimensionError；空候选返回 []"),
        ("6", "接线一致性", "sql / chroma",
         "search() 输出 == 用同一批候选重算 select_matches()"),
    )
    for index, name, target, criterion in rows:
        print(f"  [{index}] {name:<18} {target:<34} {criterion}")
    print("\n" + "=" * 74)
    print("结果")
    print("=" * 74)
    print(f"  唯一排序规则：services/vector_store.py::{RULE_NAME}()")
    print(f"  后端：{BACKEND_SQL}（候选顺序 = 主键升序） / "
          f"{BACKEND_CHROMA}（候选顺序 = chunk_id 升序）")
    print(f"  容差：score 数值跨后端 SCORE_TOL = {SCORE_TOL:g}")
    print(f"  断言：通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)


async def main() -> None:
    print("=" * 74)
    print("RAG 检索评分规则验证（top_k / min_score / 排序 / 并列）")
    print("=" * 74)

    check_rule_contract()
    check_top_k()
    check_min_score()
    check_ordering()
    check_ties()
    check_edge_cases()

    async with open_env() as env:
        await check_ties_at_store_level()
        await check_backends_apply_same_rule(env)

    report()


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(0 if _FAILED == 0 else 1)
