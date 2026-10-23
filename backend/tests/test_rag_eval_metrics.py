# -*- coding: utf-8 -*-
"""RAG 离线评估指标 · 自检（**用手算样例钉死定义**）。

为什么要有这个套件
------------------
指标算错比没有指标更危险：一个恒为 1 的 Recall 会让所有后续优化看起来"有效"。
本套件的每一条断言都对应一个**手算得出的期望值**，不接受"跑通就行"。

直接运行：``python backend/tests/test_rag_eval_metrics.py``
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import rag_eval_metrics as m  # noqa: E402


def _check(name: str, cond: bool, detail: str = "") -> bool:
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def _close(actual, expected, tol: float = 1e-4) -> bool:
    if actual is None or expected is None:
        return actual is expected
    return abs(float(actual) - float(expected)) <= tol


def _section(title: str) -> None:
    print("\n" + title)


def check_recall() -> bool:
    _section("[1] Recall@K")
    ok = True
    # retrieved=[1,2,3] relevant={1,3} k=3 ⇒ 命中 2 / 共 2
    ok &= _check("两条全命中 ⇒ 1.0", _close(m.recall_at_k([1, 2, 3], {1, 3}, 3), 1.0))
    # k=1 只看 [1] ⇒ 命中 1 / 共 2
    ok &= _check("k=1 只命中一半 ⇒ 0.5", _close(m.recall_at_k([1, 2, 3], {1, 3}, 1), 0.5))
    ok &= _check("k=2 命中 {1} ⇒ 0.5", _close(m.recall_at_k([1, 2, 3], {1, 3}, 2), 0.5))
    ok &= _check("相关集为空 ⇒ None（不参与平均）", m.recall_at_k([1, 2], set(), 3) is None)
    ok &= _check("结果为空但有标准答案 ⇒ 0.0（不是 None）",
                 _close(m.recall_at_k([], {1}, 3), 0.0))
    ok &= _check("k=0 ⇒ 0.0", _close(m.recall_at_k([1], {1}, 0), 0.0))
    ok &= _check("k 超过结果条数仍按相关集算 ⇒ 1/2",
                 _close(m.recall_at_k([1], {1, 2}, 5), 0.5))
    return ok


def check_precision() -> bool:
    _section("[2] Precision@K（分母是实际返回条数）")
    ok = True
    ok &= _check("3 条里 2 条相关 ⇒ 2/3",
                 _close(m.precision_at_k([1, 2, 3], {1, 3}, 3), 2.0 / 3.0))
    # ★ 关键语义：阈值把结果砍到 1 条且相关 ⇒ 精确率是 1.0，不是 1/5
    ok &= _check("★ 只返回 1 条且相关 ⇒ 1.0（不是 1/K）",
                 _close(m.precision_at_k([1], {1, 2, 3}, 5), 1.0))
    ok &= _check("返回空 ⇒ None", m.precision_at_k([], {1}, 3) is None)
    ok &= _check("相关集为空 ⇒ None", m.precision_at_k([1], set(), 3) is None)
    # 重复 id 占掉名额，因此精确率会下降 —— 这是刻意的口径
    ok &= _check("重复 id 占名额 ⇒ 0.5",
                 _close(m.precision_at_k([1, 1], {1}, 2), 0.5))
    return ok


def check_rr_and_ndcg() -> bool:
    _section("[3] MRR / nDCG@K")
    ok = True
    ok &= _check("首个就命中 ⇒ 1.0", _close(m.reciprocal_rank([1, 2], {1}), 1.0))
    ok &= _check("第 2 个命中 ⇒ 0.5", _close(m.reciprocal_rank([2, 1, 3], {1}), 0.5))
    ok &= _check("第 3 个命中 ⇒ 1/3",
                 _close(m.reciprocal_rank([2, 3, 1], {1}), 1.0 / 3.0))
    ok &= _check("没命中 ⇒ 0.0", _close(m.reciprocal_rank([2, 3], {1}), 0.0))
    ok &= _check("相关集为空 ⇒ None", m.reciprocal_rank([1], set()) is None)

    # retrieved=[1,2,3] relevant={1,3} k=3
    # DCG = 1/log2(2) + 1/log2(4) = 1 + 0.5 = 1.5
    # IDCG = 1/log2(2) + 1/log2(3) = 1 + 0.63093 = 1.63093
    ok &= _check("nDCG：两条相关且排 1/3 位 ⇒ 1.5/1.63093",
                 _close(m.ndcg_at_k([1, 2, 3], {1, 3}, 3), 1.5 / 1.63093))
    # retrieved=[2,1,3] relevant={1} ⇒ DCG = 1/log2(3) = 0.63093, IDCG = 1
    ok &= _check("nDCG：相关结果排第 2 ⇒ 0.63093",
                 _close(m.ndcg_at_k([2, 1, 3], {1}, 3), 1.0 / 1.58496, 2e-3))
    ok &= _check("nDCG：完美排序 ⇒ 1.0", _close(m.ndcg_at_k([1, 2], {1, 2}, 2), 1.0))
    ok &= _check("nDCG：k=0 ⇒ 0.0", _close(m.ndcg_at_k([1], {1}, 0), 0.0))
    ok &= _check("nDCG：相关集为空 ⇒ None", m.ndcg_at_k([1], set(), 3) is None)
    return ok


def check_noise() -> bool:
    _section("[4] 噪声注入率（= 1 - Precision）")
    ok = True
    ok &= _check("3 条里 1 条无关 ⇒ 1/3",
                 _close(m.noise_injection_rate([1, 2, 3], {1, 3}, 3), 1.0 / 3.0))
    ok &= _check("全部相关 ⇒ 0.0", _close(m.noise_injection_rate([1], {1}, 3), 0.0))
    ok &= _check("全部无关 ⇒ 1.0", _close(m.noise_injection_rate([9], {1}, 3), 1.0))
    ok &= _check("返回空 ⇒ None", m.noise_injection_rate([], {1}, 3) is None)
    ok &= _check("与 Precision 严格互补",
                 _close(m.noise_injection_rate([1, 2, 3], {1}, 3),
                        1.0 - m.precision_at_k([1, 2, 3], {1}, 3)))
    return ok


def check_percentile() -> bool:
    _section("[5] 延迟分位数（最近秩法）")
    ok = True
    values = [5, 1, 3, 2, 4]
    ok &= _check("p50（5 个样本）⇒ 3", _close(m.percentile(values, 0.5), 3.0))
    ok &= _check("p95 ⇒ 5（最近秩，不插值）", _close(m.percentile(values, 0.95), 5.0))
    ok &= _check("p0 ⇒ 1", _close(m.percentile(values, 0.0), 1.0))
    ok &= _check("p100 ⇒ 5", _close(m.percentile(values, 1.0), 5.0))
    ok &= _check("空序列 ⇒ None", m.percentile([], 0.5) is None)
    ok &= _check("单样本任意分位 ⇒ 该样本",
                 _close(m.percentile([7.5], 0.95), 7.5))
    # 最近秩法在小样本上不会给出"没出现过"的数字
    ok &= _check("★ 结果一定是样本里出现过的值",
                 m.percentile([1, 2], 0.99) in (1.0, 2.0))
    return ok


def check_summarize() -> bool:
    _section("[6] 聚合（必须显式报 skipped）")
    ok = True
    summary = m.summarize([0.5, None, 1.0])
    ok &= _check("n 只算有值的", summary["n"] == 2, str(summary))
    ok &= _check("mean 只对有值的求平均 ⇒ 0.75", _close(summary["mean"], 0.75), str(summary))
    ok &= _check("skipped 显式报出 ⇒ 1", summary["skipped"] == 1, str(summary))
    ok &= _check("min/max 正确",
                 _close(summary["min"], 0.5) and _close(summary["max"], 1.0), str(summary))
    empty = m.summarize([None, None])
    ok &= _check("全 None ⇒ n=0 且 mean=None（不是 0.0）",
                 empty["n"] == 0 and empty["mean"] is None and empty["skipped"] == 2, str(empty))
    return ok


def check_purity() -> bool:
    _section("[7] 纯函数（不改入参）")
    ok = True
    retrieved = [3, 1, 2]
    relevant = {1, 2}
    before_retrieved = list(retrieved)
    before_relevant = set(relevant)
    m.recall_at_k(retrieved, relevant, 3)
    m.precision_at_k(retrieved, relevant, 3)
    m.ndcg_at_k(retrieved, relevant, 3)
    m.reciprocal_rank(retrieved, relevant)
    m.noise_injection_rate(retrieved, relevant, 3)
    ok &= _check("retrieved 未被原地排序/修改", retrieved == before_retrieved, str(retrieved))
    ok &= _check("relevant 未被修改", relevant == before_relevant, str(relevant))
    return ok


def check_metric_can_fail() -> bool:
    _section("[8] 守卫自检：指标必须能「变差」")
    ok = True
    # 如果指标对「排序变差」不敏感，它就没有诊断价值
    good = m.ndcg_at_k([1, 2], {1, 2}, 2)
    bad = m.ndcg_at_k([2, 1], {1, 2}, 2)
    ok &= _check("nDCG 对排序不敏感就是废指标（应相等，因为两条都相关）",
                 _close(good, bad), f"{good} vs {bad}")
    better = m.ndcg_at_k([1, 9], {1}, 2)
    worse = m.ndcg_at_k([9, 1], {1}, 2)
    ok &= _check("★ 相关结果排更靠后 ⇒ nDCG 严格下降", worse < better, f"{better} vs {worse}")
    # 噪声率只看"注入了多少条无关"，与排序无关（排序由 MRR/nDCG 负责）——
    # 因此这里必须比**条数**，不能比顺序（顺序比法会得到相等，是错的断言）
    ok &= _check("★ 注入的无关切片变多 ⇒ 噪声注入率严格上升",
                 m.noise_injection_rate([1], {1}, 3) < m.noise_injection_rate([1, 9], {1}, 3),
                 f"{m.noise_injection_rate([1], {1}, 3)} vs "
                 f"{m.noise_injection_rate([1, 9], {1}, 3)}")
    ok &= _check("  └ 噪声率与顺序无关（同一集合换个顺序应相等）",
                 _close(m.noise_injection_rate([1, 9], {1}, 2),
                        m.noise_injection_rate([9, 1], {1}, 2)))
    return ok


def run() -> bool:
    print("=" * 68)
    print("RAG 离线评估指标 · 自检（手算样例）")
    print("=" * 68)
    ok = True
    ok &= check_recall()
    ok &= check_precision()
    ok &= check_rr_and_ndcg()
    ok &= check_noise()
    ok &= check_percentile()
    ok &= check_summarize()
    ok &= check_purity()
    ok &= check_metric_can_fail()
    print("\n" + "=" * 68)
    print("结果: " + ("全部通过" if ok else "存在失败项"))
    print("=" * 68)
    return ok


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
