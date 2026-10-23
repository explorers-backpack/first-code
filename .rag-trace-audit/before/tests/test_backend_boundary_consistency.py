# -*- coding: utf-8 -*-
"""双后端检索一致性 · **边界测试**（脚本式，非 pytest）。

运行：``python backend/tests/test_backend_boundary_consistency.py``

被测对象
--------
``SqlAlchemyVectorStore``（全表扫描 + Python 侧余弦）与 ``ChromaVectorStore``
（HNSW ANN 召回 + ``select_matches`` 精确重排）——**两个实现、一份排序口径**。
两者共用 :func:`services.vector_store.select_matches`，所以「排序逻辑」理论上同源；
真正的差异只会来自 **ANN 的 float32 存储精度** 与 **召回近似**。

五个边界场景（本套件的全部内容）
--------------------------------
1. ``top_k`` 限制——截断点是否一致、是否等于全量排名的前缀
2. ``min_score`` 过滤——阈值过滤后的条数与成员是否一致
3. 空结果——阈值不可达 / 空 query / NaN 阈值
4. ``score`` 接近相等——**精确并列**（同正文）与**近似并列**（间隔 < 容差）
5. 浮点误差——漂移量级、是否必须容差、排序是否稳健

判定口径（本套件的核心产出，见 :data:`RULES`）
---------------------------------------------
======================  ================================================
比较项                    判定
======================  ================================================
返回数量                  见 :data:`RULES`（默认**要求一致**；阈值压线时**允许不同**）
``chunk_id`` 序列         默认**要求一致**；仅当两个后端的**候选顺序等价**时才有意义
排序（分数非增）          **要求一致**
``score`` 数值            **容差一致** ``|Δ| <= 1e-6``（**不得要求逐位相等**）
======================  ================================================

**三条不能混为一谈的口径**：

- **完全一致**（严格相等）：结构、成员、顺序、条数。
- **容差一致**：只有 ``score`` 的**数值**。实测漂移 ``1.343e-08``（容差的 1/74），
  且**没有任何一对是逐位相等的**（0/190）⇒ 要求逐位相等必然失败。
- **允许不同**：**阈值恰好压在某条分数上**时，边界项可能一侧保留一侧丢弃
  （``>=`` 含边界 + float32 漂移），**条数允许差 ≤ 落在阈值附近的分数条数**。
  这是**唯一**允许条数不同的情形，且必须能被「阈值附近有几条分数」解释。

**不要修改**（本套件用源码指纹锁死，见 :data:`FROZEN_SOURCES`）
- ``services/vector_store.py``（含 ``select_matches`` = 排序逻辑）
- ``services/vector_store_sql.py`` / ``services/vector_store_chroma.py``（VectorStore 实现）

.. note::
   ``tests/`` 下没有 ``__init__.py``，所以先 ``sys.path.insert`` 再 import
   ``vector_backend_env``（任务 54 的统一测试环境）。
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR / "tests") not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR / "tests"))

from vector_backend_env import (  # noqa: E402
    BACKEND_CHROMA,
    BACKEND_SQL,
    ENV_MIN_SCORE,
    SCORE_TOL,
    QueryCase,
    open_env,
    run_case,
    run_case_observed,
    scores_of,
)

# ============================================================
# 一、常量与规则表
# ============================================================
#: 语料规模（16 篇 ⇒ 16 片）与 chroma 的默认过采样倍率。
CORPUS_SIZE = 16
OVERSAMPLE = 4

#: 全量排名用的 ``top_k``（远大于语料规模 ⇒ 召回必然穷尽）。
FULL_TOP_K = 50

#: 「不过滤」用的 ``min_score``：**必须严格小于任何可能的余弦值**。
#: 用 ``None`` 更语义化，但本套件要显式记录「阈值」这个量，所以用 -2.0
#: （余弦 ∈ [-1, 1]，-2.0 保证全放行且可与 0.0 区分开）。
NO_FILTER = -2.0

#: 场景 1：``top_k`` 边界取值（含 < 语料数、= 语料数、> 语料数）。
TOP_K_CASES: Tuple[int, ...] = (1, 2, 3, 4, 5, 8, 16, 20)

#: 场景 2：``min_score`` 边界取值（含 0.0 这个「压在分数上」的阈值）。
MIN_SCORE_CASES: Tuple[float, ...] = (0.0, 0.05, 0.1, 0.2, 0.3, 0.45, 0.5, 1.01)

#: 场景 3：明确不可达的阈值（余弦上界 1.0）。
UNREACHABLE_MIN_SCORE = 1.01

#: 并列夹具的正文（**与 corpus 里任何一篇都不同**，避免混淆）。
TIE_CONTENT = (
    "Redis 的持久化方式包括 RDB 快照与 AOF 日志。"
    "RDB 是某一时刻的全量快照，恢复快但可能丢数据；"
    "AOF 记录写命令，数据更安全但文件更大、恢复更慢。"
)

#: 并列夹具的篇数（3 篇同正文 ⇒ 3 片同向量 ⇒ 精确并列）。
TIE_DOCS = 3

#: ★ 冻结的源码指纹（**规范化换行后的 sha256**）。
#: 用源码而不是 AST：AST 的 ``ast.dump`` 会随 Python 版本变化（3.12 起
#: ``FunctionDef`` 多了 ``type_params``），用源码则与解释器版本无关。
FROZEN_SOURCES: Tuple[Tuple[str, str], ...] = (
    ("services/vector_store.py",
     "0dd7c96fe9f5534f300850f8b98549a70170a640e11d409ca022e773df303e33"),
    ("services/vector_store_sql.py",
     "a81dd62411a1144798cac60b5525e9d43936a1d385585d7b6a1f1292d4c34fdc"),
    ("services/vector_store_chroma.py",
     "109e760e60962a663a80201b6c002abd636b0d107c6206a2d533a19b20fb0dd9"),
)

#: ★ ``select_matches`` 函数体本身的指纹（排序逻辑的最小冻结单元）。
FROZEN_SELECT_MATCHES = "9423b6823b0ae76f832adc73d4affdeb0a9b7fff50bc92556f2e03b459eebd0b"

#: ``select_matches`` 在 ``vector_store.py`` 里的行号（供报告与排查）。
SELECT_MATCHES_LINES = (285, 336)


@dataclass(frozen=True)
class Rule:
    """一条比较口径。``verdict`` 三选一：完全一致 / 容差一致 / 允许不同。"""

    key: str
    item: str
    verdict: str
    condition: str
    note: str = ""


EXACT = "完全一致"
TOLERANT = "容差一致"
DIVERGENT = "允许不同"

#: ★ **本套件的规则表**（改这里就是改契约）。
RULES: Tuple[Rule, ...] = (
    Rule("count-clear", "返回数量", EXACT,
         "min_score 与**所有分数**都有明显间隔（> 容差）时",
         "阈值不压线 ⇒ 过滤结果确定，条数必须相同"),
    Rule("ids-clear", "chunk_id 序列", EXACT,
         "同上，且**召回穷尽**（top_k × oversample >= 语料数）时",
         "召回不穷尽时只能要求条数与前缀性，序列一致属实测而非结构性保证"),
    Rule("count-boundary", "返回数量", DIVERGENT,
         "min_score 与**某条分数**的距离 <= 容差时",
         "`>=` 含边界 + float32 漂移 ⇒ 边界项可能一侧保留一侧丢弃；"
         "差量必须 <= 落在阈值附近的分数条数"),
    Rule("zero-band", "返回数量 / chunk_id 顺序", DIVERGENT,
         "min_score <= 0 且存在 score≈0 的项（正交/零分项）时",
         "这些项的分数在 0 两侧漂移（实测 0.0 vs -3.47e-18）⇒ "
         "既可能一侧入选一侧落选，相邻间隔也可能塌到 ~1e-18"),
    Rule("empty", "返回数量", EXACT,
         "阈值不可达 / query 为空 / 阈值为 NaN 时",
         "两边都必须为空"),
    Rule("order", "排序（分数非增）", EXACT,
         "任何情况",
         "共用 select_matches ⇒ 排序口径同源"),
    Rule("score", "score 数值", TOLERANT,
         "任何情况",
         "|Δ| <= 1e-6；**不得要求逐位相等**（本套件实测 0/256 逐位相等）"),
    Rule("tie-group", "并列组的成员与组内顺序", EXACT,
         "两个后端的**候选顺序等价**时（本项目 PK 序 == chunk_id 序）",
         "并列项按各自候选顺序排；候选顺序等价 ⇒ 顺序也一致"),
    Rule("dedup", "检索器去重后保留的成员", EXACT,
         "同一正文的多个切片",
         "保留**候选顺序最靠前**的那一个（两个后端一致）"),
    Rule("invalid", "非法入参的异常", EXACT,
         "top_k <= 0 / top_k 为 bool / min_score 为 bool",
         "两个后端都必须抛 VectorStoreInputError"),
)

VERDICTS = (EXACT, TOLERANT, DIVERGENT)

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


# ============================================================
# 二、小工具
# ============================================================
def _ids(chunks: Sequence[Any]) -> Tuple[Any, ...]:
    return tuple(c.metadata.get("chunk_id") for c in chunks)


def _max_delta(left: Sequence[float], right: Sequence[float]) -> float:
    return max((abs(a - b) for a, b in zip(left, right)), default=0.0)


def _nonincreasing(values: Sequence[float]) -> bool:
    return all(a >= b for a, b in zip(values, values[1:]))


def _min_adjacent_gap(values: Sequence[float]) -> Optional[float]:
    if len(values) < 2:
        return None
    return min(a - b for a, b in zip(values, values[1:]))


def _normalized_source(relative_path: str) -> str:
    """读源码并**统一换行**（CRLF/LF 都能得到同一指纹）。"""
    text = (BACKEND_DIR / relative_path).read_text(encoding="utf-8")
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _fingerprint(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _select_matches_source() -> Optional[str]:
    """取出 ``select_matches`` 函数的源码片段（**只看排序逻辑**，不含模块其它部分）。"""
    import ast

    source = _normalized_source("services/vector_store.py")
    for node in ast.parse(source).body:
        if isinstance(node, ast.FunctionDef) and node.name == "select_matches":
            return ast.get_source_segment(source, node)
    return None


def _near_scores(threshold: float, all_scores: Sequence[float], *, tol: float = SCORE_TOL
                 ) -> Tuple[float, ...]:
    """**落在阈值附近**（距离 <= 容差）的分数——即「可能被漂移推过阈值」的项。"""
    return tuple(s for s in all_scores if abs(s - threshold) <= tol)


def _threshold_is_clear(threshold: float, all_scores: Sequence[float],
                        *, tol: float = SCORE_TOL) -> bool:
    """阈值是否与**所有分数**都有明显间隔（⇒ 条数必须完全一致）。"""
    return not _near_scores(threshold, all_scores, tol=tol)


def _case(text: str, *, top_k: int, min_score: Optional[float], index: int = 0) -> QueryCase:
    """构造一条自定义参数的 query。

    ``min_score=None`` 表示**不过滤**；``QueryCase`` 的注解写的是 ``float``，
    但运行时不做校验，检索器也确实把 ``None`` 当「不设阈值」处理。
    """
    return QueryCase(index=index, text=text, top_k=top_k, min_score=min_score)  # type: ignore[arg-type]


# ============================================================
# [1] 前置与契约
# ============================================================
def check_frozen_sources() -> None:
    _section("[1] 前置与契约（源码指纹 / 环境契约）")

    actual = {}
    for rel, expected in FROZEN_SOURCES:
        got = _fingerprint(_normalized_source(rel))
        actual[rel] = got
        _check(f"★ 冻结：{rel} 源码未改动",
               got == expected,
               "" if got == expected else
               f"期望 {expected[:16]}… 实际 {got[:16]}… "
               f"（如确需改动 VectorStore 实现，请同步更新 FROZEN_SOURCES）")

    seg = _select_matches_source()
    _check("★ 冻结：select_matches（排序逻辑）存在且函数体未改动",
           seg is not None and _fingerprint(seg) == FROZEN_SELECT_MATCHES,
           "" if seg is not None else "未找到 select_matches")

    # 守卫自检：指纹必须能区分「有改动」与「无改动」
    probe = _normalized_source("services/vector_store.py")
    _check("守卫自检：指纹能区分「有改动」与「无改动」（改动运行时拼出）",
           _fingerprint(probe) != _fingerprint(probe + "\n# " + "guard" + "_probe"))

    _check("规则表的判定只有三种取值（完全一致 / 容差一致 / 允许不同）",
           all(r.verdict in VERDICTS for r in RULES),
           str(sorted({r.verdict for r in RULES})))


def check_env_contract(env: Any) -> None:
    _check("环境同时提供 sql 与 chroma 两个后端",
           env.backends == (BACKEND_SQL, BACKEND_CHROMA), str(list(env.backends)))
    _check("语料规模与常量一致（16 篇 ⇒ 16 片）",
           len(env.records) == CORPUS_SIZE, str(len(env.records)))
    _check("容差与文档一致（SCORE_TOL == 1e-6）", SCORE_TOL == 1e-6, str(SCORE_TOL))

    # ★ 并列项顺序一致的前提：两个后端的**候选顺序等价**
    pk_order = [r.chunk_id for r in env.records]
    _check("★ 候选顺序前提：权威行按主键升序读出后 chunk_id 已升序"
           "（⇒ SQL 主键序 == chroma chunk_id 序，并列项顺序才可能一致）",
           pk_order == sorted(pk_order), str(pk_order[:6]) + "…")

    corpus_contents = [r.content.strip() for r in env.records]
    _check("原始语料无重复正文（⇒ 检索器去重在场景 4 之前是 no-op）",
           len(set(corpus_contents)) == len(corpus_contents))


async def collect_full_ranking(env: Any) -> Dict[int, Dict[str, List[Any]]]:
    """每个后端、每条 query 的**全量排名**（不过滤、不截断）。"""
    full: Dict[int, Dict[str, List[Any]]] = {}
    for case in env.queries:
        per_backend = {}
        for backend in env.backends:
            per_backend[backend] = await run_case(
                env, backend, _case(case.text, top_k=FULL_TOP_K,
                                    min_score=NO_FILTER, index=case.index)
            )
        full[case.index] = per_backend
    return full


# ============================================================
# [2] 容差规则自检
# ============================================================
def check_rule_selfcheck() -> None:
    _section("[2] 容差规则自检（规则表本身可执行：正例通过 / 反例被拦）")

    # -- score：容差一致 --
    ok, delta = (abs(0.5 - (0.5 + 1e-9)) <= SCORE_TOL, abs(0.5 - (0.5 + 1e-9)))
    _check("score 正例：差 1e-9 ⇒ 判「在容差内」", ok, f"{delta:.3e}")
    delta_big = abs(0.5 - 0.501)
    _check("score 反例：差 1e-3 ⇒ 判「超容差」", delta_big > SCORE_TOL, f"{delta_big:.3e}")
    _check("★ score 反例：逐位相等不是判据（差 0 与差 1e-9 都必须判通过）",
           (abs(0.5 - 0.5) <= SCORE_TOL) and (abs(0.5 - (0.5 + 1e-9)) <= SCORE_TOL))

    # -- 条数：完全一致 --
    _check("条数正例：两边都是 5 ⇒ 判「一致」", 5 == 5)
    _check("条数反例：5 vs 4 ⇒ 判「不一致」", not (5 == 4))

    # -- 阈值压线分类 --
    scores = (0.0, 0.1, 0.25, 0.4)
    _check("阈值分类：0.25 恰好等于某条分数 ⇒ 判「压线」（允许条数不同）",
           not _threshold_is_clear(0.25, scores), str(_near_scores(0.25, scores)))
    _check("阈值分类：0.25 + 5e-7 仍在容差内 ⇒ 判「压线」",
           not _threshold_is_clear(0.25 + 5e-7, scores))
    _check("阈值分类：0.175 与所有分数都差 > 容差 ⇒ 判「明显间隔」（要求条数一致）",
           _threshold_is_clear(0.175, scores))
    _check("阈值分类：0.0 与分数 0.0 距离 0 ⇒ 判「压线」",
           not _threshold_is_clear(0.0, scores))

    # -- 排序判据 --
    _check("排序：严格非增序列判「已排序」", _nonincreasing((0.9, 0.5, 0.5, 0.1)))
    _check("排序：出现上升 ⇒ 判「未排序」", not _nonincreasing((0.9, 0.5, 0.6)))
    _check("相邻间隔：并列时最小间隔为 0（⇒ 并列必须按候选顺序兜底）",
           _min_adjacent_gap((0.5, 0.5, 0.1)) == 0.0)

    # -- 规则表结构 --
    keys = [r.key for r in RULES]
    _check("规则表无重复 key", len(set(keys)) == len(keys))
    _check("规则表同时包含「完全一致」与「允许不同」两类（否则口径不完整）",
           {EXACT, DIVERGENT} <= {r.verdict for r in RULES})
    _check("规则表覆盖全部五个比较项",
           {r.item for r in RULES} >= {"返回数量", "chunk_id 序列", "score 数值"},
           str(sorted({r.item for r in RULES})))


# ============================================================
# [3] 场景 1 · top_k 限制
# ============================================================
async def check_scenario_top_k(env: Any, full: Dict[int, Dict[str, List[Any]]]) -> None:
    _section("[3] 场景 1 · top_k 限制（不过滤 min_score，只变 top_k）")

    for top_k in TOP_K_CASES:
        exhaustive = top_k * OVERSAMPLE >= CORPUS_SIZE
        count_equal = 0
        prefix_ok = 0
        seq_equal = 0
        monotonic_ok = 0
        worst = 0.0
        expect_count = min(top_k, CORPUS_SIZE)
        count_ok = 0

        for case in env.queries:
            got = {}
            for backend in env.backends:
                got[backend] = await run_case(
                    env, backend, _case(case.text, top_k=top_k,
                                        min_score=NO_FILTER, index=case.index)
                )
            a, b = got[BACKEND_SQL], got[BACKEND_CHROMA]
            if len(a) == len(b):
                count_equal += 1
            if len(a) == expect_count and len(b) == expect_count:
                count_ok += 1
            if _ids(a) == _ids(full[case.index][BACKEND_SQL])[:top_k] and \
                    _ids(b) == _ids(full[case.index][BACKEND_CHROMA])[:top_k]:
                prefix_ok += 1
            if _ids(a) == _ids(b):
                seq_equal += 1
            if _nonincreasing(scores_of(a)) and _nonincreasing(scores_of(b)):
                monotonic_ok += 1
            worst = max(worst, _max_delta(scores_of(a), scores_of(b)))

        tag = "召回穷尽" if exhaustive else "召回不穷尽"
        print(f"  top_k={top_k:>2} [{tag}] 条数={count_ok}/16 条数一致={count_equal}/16 "
              f"前缀={prefix_ok}/16 序列一致={seq_equal}/16 非增={monotonic_ok}/16 "
              f"max|Δ|={worst:.3e}")

        _check(f"top_k={top_k}：两边条数都等于 min(top_k, 语料数)={expect_count}",
               count_ok == len(env.queries), f"{count_ok}/16")
        _check(f"top_k={top_k}：两边条数一致（不过滤 ⇒ 结构性成立）",
               count_equal == len(env.queries), f"{count_equal}/16")
        _check(f"top_k={top_k}：结果 == 该后端全量排名的前 {top_k} 项（前缀性）",
               prefix_ok == len(env.queries), f"{prefix_ok}/16")
        _check(f"top_k={top_k}：两边分数都非增（共用排序口径）",
               monotonic_ok == len(env.queries), f"{monotonic_ok}/16")
        _check(f"top_k={top_k}：同位分数差 <= 容差",
               worst <= SCORE_TOL, f"max|Δ|={worst:.3e}")

        if exhaustive:
            _check(f"★ top_k={top_k}：召回穷尽 ⇒ chunk_id 序列**要求完全一致**",
                   seq_equal == len(env.queries), f"{seq_equal}/16")
        else:
            # 召回不穷尽（top_k × oversample < 语料数）：ANN 只看到 top_k×4 个候选，
            # 理论上可能漏掉真正的 top-k。此处**只报告**，不要求一致。
            print(f"         ↳ 召回不穷尽（{top_k}×{OVERSAMPLE}={top_k * OVERSAMPLE} "
                  f"< {CORPUS_SIZE}）：chunk_id 序列实测 {seq_equal}/16 一致，"
                  f"但**不是**结构性保证，故不作断言")

    # 前缀性的另一面：top_k 增大时，小 top_k 的结果必须是大 top_k 的前缀
    chain_ok = 0
    for case in env.queries:
        prev: Optional[Tuple[Any, ...]] = None
        ok = True
        for top_k in sorted(TOP_K_CASES):
            for backend in env.backends:
                got = await run_case(
                    env, backend, _case(case.text, top_k=top_k,
                                        min_score=NO_FILTER, index=case.index)
                )
                if prev is not None and _ids(got)[:len(prev)] != prev:
                    ok = False
                prev = _ids(got)
        if ok:
            chain_ok += 1
    _check("★ 前缀链：top_k 递增时，小 top_k 的结果始终是大 top_k 结果的前缀",
           chain_ok == len(env.queries), f"{chain_ok}/16")


# ============================================================
# [4] 场景 2 · min_score 过滤
# ============================================================
async def check_scenario_min_score(env: Any, full: Dict[int, Dict[str, List[Any]]]) -> None:
    _section("[4] 场景 2 · min_score 过滤（top_k 远大于语料数 ⇒ 召回穷尽，只变阈值）")

    for threshold in MIN_SCORE_CASES:
        clear = 0
        boundary = 0
        count_equal = 0
        seq_equal = 0
        bound_ok = 0
        inclusive_ok = 0
        monotonic_ok = 0
        total = 0
        worst = 0.0

        for case in env.queries:
            all_scores = tuple(scores_of(full[case.index][BACKEND_SQL])) + \
                tuple(scores_of(full[case.index][BACKEND_CHROMA]))
            is_clear = _threshold_is_clear(threshold, all_scores)
            near = _near_scores(threshold, all_scores)

            got = {}
            for backend in env.backends:
                got[backend] = await run_case(
                    env, backend, _case(case.text, top_k=FULL_TOP_K,
                                        min_score=threshold, index=case.index)
                )
            a, b = got[BACKEND_SQL], got[BACKEND_CHROMA]
            total += len(a)

            if is_clear:
                clear += 1
                if len(a) == len(b):
                    count_equal += 1
                if _ids(a) == _ids(b):
                    seq_equal += 1
            else:
                boundary += 1
                # ★ 允许不同，但差量必须能被「阈值附近的分数条数」解释
                if abs(len(a) - len(b)) <= max(len(near), 1):
                    bound_ok += 1

            # `>=` 含边界：返回的每一条都必须 >= 阈值
            if all(s >= threshold for s in scores_of(a)) and \
                    all(s >= threshold for s in scores_of(b)):
                inclusive_ok += 1
            if _nonincreasing(scores_of(a)) and _nonincreasing(scores_of(b)):
                monotonic_ok += 1
            worst = max(worst, _max_delta(scores_of(a), scores_of(b)))

        print(f"  min_score={threshold:<5} 明显间隔 {clear}/16 压线 {boundary}/16 "
              f"总条数={total:3d} 条数一致={count_equal}/{max(clear, 0)} "
              f"序列一致={seq_equal}/{max(clear, 0)} max|Δ|={worst:.3e}")

        _check(f"min_score={threshold}：返回的每一条都满足 `>= 阈值`（含边界）",
               inclusive_ok == len(env.queries), f"{inclusive_ok}/16")
        _check(f"min_score={threshold}：两边分数都非增",
               monotonic_ok == len(env.queries), f"{monotonic_ok}/16")
        _check(f"min_score={threshold}：同位分数差 <= 容差",
               worst <= SCORE_TOL, f"max|Δ|={worst:.3e}")

        if clear:
            _check(f"★ min_score={threshold}：阈值与所有分数都有明显间隔 ⇒ "
                   f"条数**要求完全一致**",
                   count_equal == clear, f"{count_equal}/{clear}")
            _check(f"★ min_score={threshold}：同上 ⇒ chunk_id 序列**要求完全一致**",
                   seq_equal == clear, f"{seq_equal}/{clear}")
        if boundary:
            _check(f"min_score={threshold}：阈值压在分数上（{boundary} 条 query）⇒ "
                   f"条数差量 <= 阈值附近分数条数（允许不同但有界）",
                   bound_ok == boundary, f"{bound_ok}/{boundary}")

    # 单调性：阈值升高 ⇒ 结果条数不增（对每个后端都成立）
    monotone_ok = 0
    for case in env.queries:
        ok = True
        for backend in env.backends:
            prev: Optional[int] = None
            for threshold in sorted(MIN_SCORE_CASES):
                got = await run_case(
                    env, backend, _case(case.text, top_k=FULL_TOP_K,
                                        min_score=threshold, index=case.index)
                )
                if prev is not None and len(got) > prev:
                    ok = False
                prev = len(got)
        if ok:
            monotone_ok += 1
    _check("★ 阈值单调性：min_score 升高时条数不增（两个后端都成立）",
           monotone_ok == len(env.queries), f"{monotone_ok}/16")


# ============================================================
# [5] 场景 3 · 空结果
# ============================================================
async def check_scenario_empty(env: Any, full: Dict[int, Dict[str, List[Any]]]) -> None:
    _section("[5] 场景 3 · 空结果（阈值不可达 / 空 query / NaN 阈值）")

    # 5a. 阈值不可达
    empty_ok = 0
    for case in env.queries:
        got = {}
        for backend in env.backends:
            got[backend] = await run_case(
                env, backend, _case(case.text, top_k=FULL_TOP_K,
                                    min_score=UNREACHABLE_MIN_SCORE, index=case.index)
            )
        if not got[BACKEND_SQL] and not got[BACKEND_CHROMA]:
            empty_ok += 1
    _check(f"★ 阈值不可达（min_score={UNREACHABLE_MIN_SCORE}）⇒ 两边都必须为空",
           empty_ok == len(env.queries), f"{empty_ok}/16")

    # 5b. 阈值刚好高于最大分数
    max_score = max(
        (s for c in env.queries for s in scores_of(full[c.index][BACKEND_SQL])),
        default=0.0,
    )
    above = max_score + SCORE_TOL * 10
    above_ok = 0
    for case in env.queries:
        got = {}
        for backend in env.backends:
            got[backend] = await run_case(
                env, backend, _case(case.text, top_k=FULL_TOP_K,
                                    min_score=above, index=case.index)
            )
        if not got[BACKEND_SQL] and not got[BACKEND_CHROMA]:
            above_ok += 1
    _check(f"★ 阈值 = 最大分数 + 10×容差（{above:.6f}）⇒ 两边都必须为空",
           above_ok == len(env.queries), f"{above_ok}/16")

    # 5c. 空 query：检索器必须短路（不调用 embedder / store）
    short_circuit = 0
    for backend in env.backends:
        obs = await run_case_observed(
            env, backend, _case("", top_k=FULL_TOP_K, min_score=NO_FILTER)
        )
        if not obs.chunks and obs.embed_input == "" and obs.search_kwargs == {}:
            short_circuit += 1
    _check("★ 空 query ⇒ 两边都返回空，且**不调用** embedder / store（同一短路点）",
           short_circuit == len(env.backends), f"{short_circuit}/{len(env.backends)}")

    # 5d. NaN 阈值：比较恒为 False ⇒ 全部被过滤（两边一致）
    nan_ok = 0
    for case in env.queries[:4]:
        got = {}
        for backend in env.backends:
            got[backend] = await run_case(
                env, backend, _case(case.text, top_k=FULL_TOP_K,
                                    min_score=float("nan"), index=case.index)
            )
        if not got[BACKEND_SQL] and not got[BACKEND_CHROMA]:
            nan_ok += 1
    _check("★ min_score=NaN ⇒ 两边都必须为空（NaN 比较恒 False ⇒ 全过滤）",
           nan_ok == 4, f"{nan_ok}/4")


# ============================================================
# [6] 场景 4 · score 接近相等
# ============================================================
async def check_scenario_tie(env: Any) -> None:
    _section("[6] 场景 4 · score 接近相等（精确并列 / 近似并列 / 阈值压线）")

    from services.knowledge_import_pipeline import KnowledgeImportPipeline

    # 6a. 造精确并列：导入 3 篇**正文完全相同**的文档 ⇒ 3 片同向量
    pipeline = KnowledgeImportPipeline(
        env.session, embedder=env.embedder, store=env.chroma_store
    )
    for k in range(TIE_DOCS):
        report = await pipeline.import_document({
            "title": f"并列夹具 {k}",
            "content": TIE_CONTENT,
            "category": "technical",
            "source": f"test://boundary/tie/{k}",
        })
        _check(f"并列夹具第 {k} 篇入库成功（status={report['status']}）",
               report["status"] == "ok", str(report.get("stage")))

    query = "Redis RDB AOF 持久化"
    model = env.embedder.name
    vector = await env.embedder.embed(query)

    store_results = {}
    for backend in env.backends:
        store_results[backend] = await env.store_for(backend).search(
            vector, top_k=FULL_TOP_K, min_score=NO_FILTER, model=model,
            document_id=None, category=None,
        )

    # 并列组 = 正文等于夹具正文的那些记录（**动态识别**，不硬编码 id）
    groups = {}
    for backend, matches in store_results.items():
        groups[backend] = [m for m in matches if m.content.strip() == TIE_CONTENT.strip()]

    _check(f"★ 并列夹具确实造出 {TIE_DOCS} 条同正文记录（两个后端都看得到）",
           all(len(g) == TIE_DOCS for g in groups.values()),
           str({b: len(g) for b, g in groups.items()}))

    if all(len(g) == TIE_DOCS for g in groups.values()):
        for backend, group in groups.items():
            scores = [m.score for m in group]
            _check(f"★ [{backend}] 并列组内分数**逐位相等**（这是「精确并列」的定义）",
                   len(set(scores)) == 1, f"{scores[0]!r} × {len(scores)}")
            ids = [m.chunk_id for m in group]
            _check(f"★ [{backend}] 并列组按**候选顺序**排列（本环境 = chunk_id 升序）",
                   ids == sorted(ids), str(ids))

        _check("★ 两个后端的并列组**成员集合**一致（要求完全一致）",
               {m.chunk_id for m in groups[BACKEND_SQL]} ==
               {m.chunk_id for m in groups[BACKEND_CHROMA]},
               str([[m.chunk_id for m in groups[b]] for b in env.backends]))
        _check("★ 两个后端的并列组**组内顺序**一致（前提：候选顺序等价，见 [1]）",
               [m.chunk_id for m in groups[BACKEND_SQL]] ==
               [m.chunk_id for m in groups[BACKEND_CHROMA]],
               str([[m.chunk_id for m in groups[b]] for b in env.backends]))

        # 并列组在整个排名中的**位置**（前面的成员）也必须一致
        prefix = {}
        for backend, matches in store_results.items():
            first_tie = min(i for i, m in enumerate(matches)
                            if m.content.strip() == TIE_CONTENT.strip())
            prefix[backend] = [m.chunk_id for m in matches[:first_tie]]
        _check("★ 并列组之前的成员也一致（并列不改变前置名次）",
               prefix[BACKEND_SQL] == prefix[BACKEND_CHROMA],
               str(prefix[BACKEND_SQL]))

        group_scores = {b: [m.score for m in g] for b, g in groups.items()}
        delta = _max_delta(group_scores[BACKEND_SQL], group_scores[BACKEND_CHROMA])
        _check("★ 并列组的分数跨后端差 <= 容差（数值仍按容差比）",
               delta <= SCORE_TOL, f"max|Δ|={delta:.3e}")

    # 6b. 检索器去重：同正文多片只保留**候选顺序最靠前**的一个
    retr = {}
    for backend in env.backends:
        retr[backend] = await run_case(
            env, backend, _case(query, top_k=FULL_TOP_K, min_score=NO_FILTER)
        )
    for backend, chunks in retr.items():
        kept = [c for c in chunks if c.content.strip() == TIE_CONTENT.strip()]
        _check(f"★ [{backend}] 检索器去重：同正文 {TIE_DOCS} 片只保留 1 片",
               len(kept) == 1, str(len(kept)))
        if kept and groups.get(backend):
            _check(f"★ [{backend}] 保留的是**候选顺序最靠前**的那一片",
                   kept[0].metadata.get("chunk_id") == groups[backend][0].chunk_id,
                   f"保留 {kept[0].metadata.get('chunk_id')} vs 组首 "
                   f"{groups[backend][0].chunk_id}")
    _check("★ 去重结果在两个后端上一致（保留同一个 chunk_id）",
           _ids(retr[BACKEND_SQL]) == _ids(retr[BACKEND_CHROMA]),
           str([_ids(retr[b]) for b in env.backends]))

    # 6c. 近似并列：相邻间隔 < 容差，但**非**逐位相等（真正的「模糊带」）
    near_pairs = []
    for backend in env.backends:
        scores = [m.score for m in store_results[backend]]
        for i in range(len(scores) - 1):
            if scores[i] != scores[i + 1] and abs(scores[i] - scores[i + 1]) < SCORE_TOL:
                near_pairs.append((backend, scores[i], scores[i + 1]))
    _check("★ 「近似并列」（间隔 < 容差且非逐位相等）确实存在 ⇒ 模糊带非空",
           bool(near_pairs), str(near_pairs[:3]))
    _check("★ 且全部位于 score≈0（|两端分数| <= 容差）⇒ 只有正交/零分项进模糊带，"
           "正常命中项不受影响",
           all(abs(a) <= SCORE_TOL and abs(b) <= SCORE_TOL for _, a, b in near_pairs),
           str(near_pairs[:3]))
    _check("★ 「精确并列」与「近似并列」是两种不同的东西，本夹具两者都有",
           bool(near_pairs)
           and all(len({m.score for m in groups[b]}) == 1 for b in env.backends))

    # 6d. 阈值压在并列分数上：允许条数不同，但差量有界
    tie_score = groups[BACKEND_SQL][0].score if groups.get(BACKEND_SQL) else None
    if tie_score is not None:
        # ★ 注意：store_results 里是 ``VectorMatch``（分数在 ``.score`` 属性上），
        # **不能**用 ``scores_of()``——那个 helper 读的是 ``metadata["score"]``，
        # 只适用于 ``KnowledgeChunk``。用错了会静默得到一串 0.0。
        all_scores = tuple(m.score for m in store_results[BACKEND_SQL]) + \
            tuple(m.score for m in store_results[BACKEND_CHROMA])
        for label, threshold in (
            ("恰好等于并列分数", tie_score),
            ("低于并列分数 1e-12", tie_score - 1e-12),
            ("高于并列分数 1e-12", tie_score + 1e-12),
            ("低于并列分数 1e-9", tie_score - 1e-9),
            ("高于并列分数 1e-9", tie_score + 1e-9),
        ):
            counts = {}
            for backend in env.backends:
                got = await run_case(
                    env, backend,
                    _case(query, top_k=FULL_TOP_K, min_score=threshold)
                )
                counts[backend] = len(got)
            near = _near_scores(threshold, all_scores)
            diff = abs(counts[BACKEND_SQL] - counts[BACKEND_CHROMA])
            # 条数**允许**不同，但必须由「阈值附近确实有分数」解释：
            # 若阈值离所有分数都很远（near 为空），条数差就必须为 0。
            allowed = bool(near) and diff <= len(near)
            _check(f"阈值{label}（{threshold:.17g}）：条数差 {diff} <= "
                   f"阈值附近分数条数 {len(near)}（允许不同但有界）",
                   allowed, str(counts))
            if diff:
                print(f"         ↳ ★ 允许不同：{label} ⇒ sql={counts[BACKEND_SQL]} "
                      f"chroma={counts[BACKEND_CHROMA]}（float32 把边界项推过了阈值）")


# ============================================================
# [7] 场景 5 · 浮点误差
# ============================================================
def check_scenario_float_drift(env: Any, full: Dict[int, Dict[str, List[Any]]]) -> None:
    _section("[7] 场景 5 · 浮点误差（漂移量级 / 是否必须容差 / 排序是否稳健）")

    deltas: List[float] = []
    bitwise_equal = 0
    seq_equal = 0
    worst_delta = 0.0
    worst_at = None
    near_tie_pairs: List[Tuple[int, float, float]] = []

    for case in env.queries:
        sa = scores_of(full[case.index][BACKEND_SQL])
        sb = scores_of(full[case.index][BACKEND_CHROMA])
        if _ids(full[case.index][BACKEND_SQL]) == _ids(full[case.index][BACKEND_CHROMA]):
            seq_equal += 1
        for i, (x, y) in enumerate(zip(sa, sb)):
            d = abs(x - y)
            deltas.append(d)
            if x == y:
                bitwise_equal += 1
            if d > worst_delta:
                worst_delta, worst_at = d, (case.index, i)
        for i in range(len(sa) - 1):
            if abs(sa[i] - sa[i + 1]) < SCORE_TOL:
                near_tie_pairs.append((case.index, sa[i], sa[i + 1]))

    total = len(deltas)
    print(f"  [5a] 不过滤全量排名：对数={total} 逐位相等={bitwise_equal} "
          f"max|Δ|={worst_delta:.17g} at q{worst_at}")

    _check("★ 逐位相等的对数 == 0（⇒ 「要求 score 逐位相等」必然失败，容差是必需的）",
           bitwise_equal == 0, f"{bitwise_equal}/{total}")
    _check("★ 漂移非零（⇒ 容差不是摆设，确实有量化误差）",
           worst_delta > 0.0, f"{worst_delta:.3e}")
    _check(f"★ 最大漂移 {worst_delta:.3e} <= 容差 {SCORE_TOL:g}",
           worst_delta <= SCORE_TOL)
    _check("★ 最大漂移远小于容差（<= 容差的 1/10）",
           worst_delta <= SCORE_TOL / 10, f"{worst_delta:.3e}")
    _check("漂移量级与 ANN float32 的已知量级一致（<= 1e-7）",
           worst_delta <= 1e-7, f"{worst_delta:.3e}")
    _check("★ 不过滤时 chunk_id 序列仍要求完全一致（16/16）",
           seq_equal == len(env.queries), f"{seq_equal}/16")

    # ---- [5b] score≈0 的模糊带：顺序与「是否入选」都允许不同 ----
    print(f"  [5b] score≈0 的模糊带：相邻间隔 < 容差 的对数 = {len(near_tie_pairs)}")
    _check("★ 模糊带**非空**（存在相邻间隔 < 容差的对）⇒ 这些项的顺序不可靠",
           bool(near_tie_pairs), str(near_tie_pairs[:3]))
    _check("★ 且全部落在 score≈0 处（|两端分数| <= 容差）"
           "⇒ 只有正交/零分项受影响，正常命中项不受影响",
           all(abs(a) <= SCORE_TOL and abs(b) <= SCORE_TOL for _, a, b in near_tie_pairs),
           str(near_tie_pairs[:3]))

    # ---- [5c] 过滤掉 score≈0 之后，排序才是稳健的 ----
    gaps: List[float] = []
    for case in env.queries:
        for backend in env.backends:
            filtered = [s for s in scores_of(full[case.index][backend])
                        if s >= ENV_MIN_SCORE]
            gap = _min_adjacent_gap(filtered)
            if gap is not None:
                gaps.append(gap)
    min_gap = min(gaps, default=None)
    print(f"  [5c] 过滤后（min_score>={ENV_MIN_SCORE}）：最小相邻间隔="
          f"{'N/A' if min_gap is None else f'{min_gap:.6e}'}")

    _check("过滤后仍有可比较的相邻名次（每条 query 至少 2 条结果）",
           min_gap is not None, str(min_gap))
    if min_gap is not None:
        _check("★ 排序稳健性（过滤后）：最大漂移 < 最小相邻间隔 ⇒ 漂移不可能换序",
               worst_delta < min_gap, f"漂移 {worst_delta:.3e} vs 间隔 {min_gap:.3e}")
        _check("★ 排序稳健性余量充足（间隔 >= 漂移的 100 倍）",
               min_gap >= worst_delta * 100,
               f"余量 {min_gap / max(worst_delta, 1e-30):.0f}x")
        _check("★ 过滤后模糊带为空（最小间隔 > 容差）⇒ 截断边界不会换人",
               min_gap > SCORE_TOL, f"最小间隔 {min_gap:.3e}")
    print("         ↳ 结论：排序稳健性**只在过滤掉 score≈0 的项之后**成立；"
          "不过滤时那批项的间隔可塌到 ~1e-18")


# ============================================================
# [8] 边界与报错
# ============================================================
async def check_invalid_params(env: Any) -> None:
    _section("[8] 边界与报错（非法入参 / 确定性）")

    model = env.embedder.name
    vector = await env.embedder.embed("边界测试")

    invalid_kwargs = (
        ("top_k=0", dict(top_k=0)),
        ("top_k=-1", dict(top_k=-1)),
        ("top_k=True（bool 是 int 子类）", dict(top_k=True)),
        ("min_score=True", dict(top_k=5, min_score=True)),
    )
    for label, kwargs in invalid_kwargs:
        types = {}
        for backend in env.backends:
            try:
                await env.store_for(backend).search(
                    vector, model=model, document_id=None, category=None, **kwargs
                )
                types[backend] = "未抛异常"
            except Exception as exc:  # noqa: BLE001
                types[backend] = type(exc).__name__
        _check(f"★ 非法入参「{label}」两个后端都抛同一异常类型",
               len(set(types.values())) == 1 and types[BACKEND_SQL] != "未抛异常",
               str(types))

    # 确定性：同一输入连跑两次必须逐位相同（两个后端都是）
    for backend in env.backends:
        case = _case(env.queries[0].text, top_k=FULL_TOP_K, min_score=NO_FILTER)
        x = await run_case(env, backend, case)
        y = await run_case(env, backend, case)
        _check(f"★ [{backend}] 确定性：同输入连跑两次结果逐位相同",
               scores_of(x) == scores_of(y) and _ids(x) == _ids(y),
               f"max|Δ|={_max_delta(scores_of(x), scores_of(y)):.3e}")

    # 极小 top_k 与极大 top_k 都不报错（边界可用性）
    for backend in env.backends:
        small = await run_case(env, backend, _case(env.queries[0].text, top_k=1,
                                                   min_score=NO_FILTER))
        big = await run_case(env, backend, _case(env.queries[0].text, top_k=10_000,
                                                 min_score=NO_FILTER))
        _check(f"[{backend}] top_k=1 返回 1 条、top_k=10000 返回全部（不报错、不越界）",
               len(small) == 1 and len(big) == CORPUS_SIZE,
               f"{len(small)} / {len(big)}")


# ============================================================
# 报告
# ============================================================
def report_rules() -> None:
    print("\n" + "=" * 74)
    print("容差规则（改 RULES 就是改契约）")
    print("=" * 74)
    print(f"  {'比较项':<24} {'判定':<10} 生效条件")
    print("  " + "-" * 70)
    for rule in RULES:
        print(f"  {rule.item:<24} {rule.verdict:<10} {rule.condition}")
        if rule.note:
            print(f"  {'':<24} {'':<10} ↳ {rule.note}")


def report_totals() -> None:
    print("\n" + "=" * 74)
    print("测试结果")
    print("=" * 74)
    print(f"  语料 {CORPUS_SIZE} 篇 / query 16 条 / 后端 {BACKEND_SQL} + {BACKEND_CHROMA}")
    print(f"  容差 SCORE_TOL = {SCORE_TOL:g}")
    print(f"  断言：通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)


async def main() -> None:
    print("=" * 74)
    print("双后端检索一致性 · 边界测试")
    print(f"比较对象：{BACKEND_SQL}（全表扫描+Python 余弦） vs "
          f"{BACKEND_CHROMA}（HNSW 召回 + select_matches 重排）")
    print("=" * 74)

    check_frozen_sources()
    check_rule_selfcheck()

    async with open_env() as env:
        check_env_contract(env)
        full = await collect_full_ranking(env)
        await check_scenario_top_k(env, full)
        await check_scenario_min_score(env, full)
        await check_scenario_empty(env, full)
        check_scenario_float_drift(env, full)
        await check_invalid_params(env)

    # 并列夹具单独开一个环境（导入 3 篇同正文 ⇒ 3 片同向量），
    # 避免污染上面那些「语料无重复正文」的前提。
    async with open_env() as env:
        await check_scenario_tie(env)

    report_rules()
    report_totals()


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(0 if _FAILED == 0 else 1)
