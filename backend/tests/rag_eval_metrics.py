# -*- coding: utf-8 -*-
"""RAG 离线评估 · 指标定义（**纯函数，零依赖，可单测**）。

为什么指标要单独一个模块
------------------------
指标算错比没有指标更危险：一个恒为 1 的 Recall 会让所有后续优化看起来"有效"。
因此本模块只做纯计算，**不碰数据库、不碰检索器**，并由
``tests/test_rag_eval_metrics.py`` 用手算样例逐条钉死。

口径（必须与使用方的定义一致，否则数字无法解释）
------------------------------------------------
``retrieved`` 是**已按分数降序**的 id 列表（检索器保证降序，本模块不再排序）。

Recall@K
    前 K 条里命中的相关条数 / 相关条总数。回答「该找到的找到了没有」。
    相关集为空 ⇒ 返回 ``None``（**不参与平均**，不是 0——"没有标准答案"与
    "一条都没找到"是两件事，混在一起会把平均值拉低成一个假的指标）。

Precision@K
    前 K 条里相关条数 / **实际返回条数**（不是 / K）。
    分母用实际条数，因为检索器可能因为阈值只返回 2 条：
    这时"2 条里 2 条相关"是完美精确，用 /K 会错误地记成 0.4。

MRR
    第一个相关结果的名次的倒数（``1/rank``，rank 从 1 开始）。没命中 ⇒ ``0.0``。

nDCG@K
    二值相关性下的 ``DCG@K / IDCG@K``，``DCG = Σ rel_i / log2(i+1)``。
    它同时惩罚"相关结果排得靠后"与"命中数量少"，比 MRR 更细。

noise_injection_rate
    ``1 - Precision@K``，即**注入到 Prompt 里的切片中不相关的比例**。
    这是"检索质量"里最直接对应"Prompt 被污染"的指标。
    相关集为空时返回 ``None``（无从判断相关与否）。

latency
    用**最近秩法**（nearest-rank）取分位数：``p95`` = 排序后第 ``ceil(0.95n)`` 个。
    不用插值：样本量小的时候插值会给出一个从未真实出现过的数字。
"""

from math import log2
from typing import Any, Dict, Iterable, List, Optional, Sequence

__all__ = [
    "recall_at_k",
    "precision_at_k",
    "reciprocal_rank",
    "ndcg_at_k",
    "noise_injection_rate",
    "percentile",
    "summarize",
]


def _as_id_list(values: Optional[Iterable[Any]]) -> List[Any]:
    if values is None:
        return []
    return list(values)


def _as_relevant_set(relevant: Optional[Iterable[Any]]) -> set:
    return set(_as_id_list(relevant))


def recall_at_k(retrieved: Sequence[Any], relevant: Iterable[Any], k: int) -> Optional[float]:
    """Recall@K。相关集为空 ⇒ ``None``（无标准答案，不参与平均）。"""
    gold = _as_relevant_set(relevant)
    if not gold:
        return None
    if k <= 0:
        return 0.0
    top = _as_id_list(retrieved)[:k]
    return len(gold.intersection(top)) / float(len(gold))


def precision_at_k(retrieved: Sequence[Any], relevant: Iterable[Any], k: int) -> Optional[float]:
    """Precision@K。**分母是实际返回条数**；返回空 ⇒ ``None``。"""
    gold = _as_relevant_set(relevant)
    if not gold:
        return None
    top = _as_id_list(retrieved)[:max(k, 0)]
    if not top:
        return None
    return len(gold.intersection(top)) / float(len(top))


def reciprocal_rank(retrieved: Sequence[Any], relevant: Iterable[Any]) -> Optional[float]:
    """第一个相关结果名次的倒数；没命中 ⇒ ``0.0``；相关集为空 ⇒ ``None``。"""
    gold = _as_relevant_set(relevant)
    if not gold:
        return None
    for position, item in enumerate(_as_id_list(retrieved), start=1):
        if item in gold:
            return 1.0 / position
    return 0.0


def ndcg_at_k(retrieved: Sequence[Any], relevant: Iterable[Any], k: int) -> Optional[float]:
    """二值相关性 nDCG@K；相关集为空 ⇒ ``None``。"""
    gold = _as_relevant_set(relevant)
    if not gold:
        return None
    if k <= 0:
        return 0.0
    top = _as_id_list(retrieved)[:k]
    dcg = 0.0
    for position, item in enumerate(top, start=1):
        if item in gold:
            dcg += 1.0 / log2(position + 1)
    ideal_hits = min(len(gold), k)
    idcg = sum(1.0 / log2(position + 1) for position in range(1, ideal_hits + 1))
    if idcg == 0:
        return 0.0
    return dcg / idcg


def noise_injection_rate(
    retrieved: Sequence[Any], relevant: Iterable[Any], k: int
) -> Optional[float]:
    """噪声注入率 = ``1 - Precision@K``（返回空或相关集为空 ⇒ ``None``）。"""
    precision = precision_at_k(retrieved, relevant, k)
    if precision is None:
        return None
    return 1.0 - precision


def percentile(values: Sequence[float], q: float) -> Optional[float]:
    """最近秩法分位数（``q`` 取 ``0~1``）。空序列 ⇒ ``None``。"""
    items = sorted(float(value) for value in values)
    if not items:
        return None
    if q <= 0:
        return items[0]
    if q >= 1:
        return items[-1]
    # ceil(q * n) - 1，至少 0
    rank = int(-(-(q * len(items)) // 1)) - 1
    return items[max(0, min(rank, len(items) - 1))]


def summarize(values: Sequence[Optional[float]]) -> Dict[str, Any]:
    """把一组可能含 ``None`` 的指标聚合成 ``{n, mean, min, max, skipped}``。

    ``skipped`` 是 ``None`` 的个数——**必须显式报出来**：如果 16 条 query 里有 10 条
    没有标准答案，那么"mean=0.9"只代表 6 条，不说明全部。
    """
    present = [float(value) for value in values if value is not None]
    skipped = len(values) - len(present)
    if not present:
        return {"n": 0, "mean": None, "min": None, "max": None, "skipped": skipped}
    return {
        "n": len(present),
        "mean": round(sum(present) / len(present), 4),
        "min": round(min(present), 4),
        "max": round(max(present), 4),
        "skipped": skipped,
    }
