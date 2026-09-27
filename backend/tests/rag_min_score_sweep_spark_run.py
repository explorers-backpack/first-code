# -*- coding: utf-8 -*-
"""R6 · 真实 Embedding 下的 ``min_score`` **定档扫描**（**非套件**运行器）。

为什么另起一个运行器（而不是改 R5）
------------------------------------
R5（``rag_min_score_calibration_spark_run.py``）回答的是「**窗口在哪**」，
结论是一串**每场景不同**的窗口中点（0.833 / 0.841 / 0.835）。
但生产只有一个全局旋钮 ``RAG_MIN_SCORE``，**没法按场景取值**。
本运行器回答另一个问题：**在给定的几个候选值里，哪个最值得当全局默认值。**

两者关系：R5 的窗口是「理论上限」（窗口内噪声为 0、目标不丢）；
本运行器是在**无法按场景取值**的现实约束下，量化各候选值的取舍。

数据管道**全部复用 R5**（同一个语料、同一批场景、同一个真实 embedder、
同一套 Prompt 渲染函数）——所以两者结果必然自洽，不会出现「换了套参数」。

零额外网络调用
--------------
全量候选**每场景只取一次**（``top_k=100``、``min_score=None``），之后所有阈值
都只是对这份排名做**离线过滤**。这成立是因为 ``select_matches`` 的顺序是

    跳维度不符 → ``(-score, 位置)`` 稳定排序 → ``min_score``（含等号）→ ``top_k``

即「先过滤、后截断」⇒ 对同一份排名，**离线过滤再截 top_k** 与
**重新带 min_score 检索**逐条等价。本运行器对每个阈值都**实跑一次真实检索做对照**，
把这个等价关系变成断言（不额外消耗上游配额：query 向量已缓存）。

指标口径（用户点名的四项）
--------------------------
1. **命中数量**  ``passed_total``（阈值后候选数）与 ``injected``（实际进 Prompt 的条数，
   受 ``top_k`` 截断）。两个都给：前者衡量阈值本身，后者衡量 Prompt 实际吃到多少。
2. **相关 chunk 保留率**  ``passed_targets / targets_total``（目标片在阈值后还剩几成）。
3. **无关 chunk 过滤率**  ``(noise_total - passed_noise) / noise_total``
   （**在全量候选池上算**，与 ``top_k`` 无关——阈值是按分数切，不是按名次切）。
4. **Prompt 长度变化**  用真实模板渲染（``render_question_prompt``）量字符数，
   并给出相对「无 RAG」臂的增量。

用法
----
::

    python tests/rag_min_score_sweep_spark_run.py                    # 默认档位
    python tests/rag_min_score_sweep_spark_run.py --min-scores 0.8,0.83 --pace 0

**联网 + 真实凭据 + 消耗配额**（语料 32 片 + 3 条 query，一次约 1.5 分钟）。
**不要**把它加进回归循环（回归只跑 ``test_*.py``；本文件是 ``*_run.py``）。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BACKEND_DIR))
sys.path.insert(0, str(TESTS_DIR))

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

#: ★ 复用 R5 的**全部**数据管道与纯函数。R5 带 ``if __name__ == "__main__"`` 守卫，
#: 因此 import 它**不会**触发它的 main（不会重复跑一遍标定）。
import rag_min_score_calibration_spark_run as r5  # noqa: E402

BENCHMARK_PATH = r5.BENCHMARK_PATH
RESULT_PATH = BACKEND_DIR / "scripts" / "rag_min_score_sweep_spark.json"

#: 用户点名的四档候选值。
DEFAULT_MIN_SCORES: Tuple[float, ...] = (0.80, 0.82, 0.83, 0.85)

#: 对照臂：无阈值 / 任务 73 声明的旧值（哈希标定，真实模型下已失效）。
BASELINES: Tuple[Tuple[str, Optional[float]], ...] = (
    ("no_threshold", None),
    ("declared_hash", 0.25),
)

#: 扫描固定用的 top_k —— 取任务 73 的推荐值 8（``min_score`` 生效后 top_k
#: 只剩「别把目标截掉」一个作用，8 在三个场景的目标片数（2/2/1）之上）。
SWEEP_TOP_K = 8

EXIT_OK = 0
EXIT_PROBLEMS = 1


# ============================================================
# 一、单场景 · 单阈值画像（纯离线过滤，不发网络请求）
# ============================================================
def _profile_at(
    rows: Dict[str, Any],
    ranking: Sequence[Any],
    targets: Sequence[str],
    min_score: Optional[float],
    *,
    top_k: int,
) -> Dict[str, Any]:
    """在**全量候选排名**上套一个阈值，给出四项指标。

    ``ranking`` 必须已是全量候选按 ``(-score, 位置)`` 降序的序列
    （即 ``retrieve(top_k=FULL_SCAN_TOP_K, min_score=None)`` 的返回）。
    """
    want = set(targets)
    kept = [c for c in ranking
            if min_score is None or c.metadata["score"] >= min_score]
    injected = kept[:top_k]
    return {
        "min_score": min_score,
        # 1) 命中数量
        "passed_total": len(kept),
        "injected": len(injected),
        # 2)/3) 分子（分母由聚合层给，保证是「总量口径」而不是「每场景平均」）
        "passed_targets": sum(1 for c in kept if c.source in want),
        "passed_noise": sum(1 for c in kept if c.source not in want),
        "injected_targets": sum(1 for c in injected if c.source in want),
        "injected_noise": sum(1 for c in injected if c.source not in want),
        "injected_sources": [c.source for c in injected],
        # 4) Prompt 长度（真实模板，不需要大模型）
        "prompt_chars": r5._prompt_chars(rows, injected)["prompt_chars"],
    }


# ============================================================
# 二、main
# ============================================================
def _parse_scores(raw: str) -> Tuple[float, ...]:
    out: List[float] = []
    for piece in raw.split(","):
        piece = piece.strip()
        if not piece:
            continue
        value = float(piece)
        if not 0.0 <= value <= 1.0:
            raise argparse.ArgumentTypeError(
                f"min_score 必须在 [0, 1]（余弦相似度）；得到 {value}"
            )
        out.append(value)
    if not out:
        raise argparse.ArgumentTypeError("至少要给一个 min_score")
    return tuple(out)


def _fmt(value: Optional[float]) -> str:
    return "None" if value is None else f"{value:.2f}"


def _rate(numerator: int, denominator: int) -> Optional[float]:
    return None if denominator <= 0 else round(numerator / denominator, 6)


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="R6 · 真实 Embedding 下的 min_score 定档扫描（不写生产库）"
    )
    parser.add_argument("--min-scores", type=_parse_scores, default=DEFAULT_MIN_SCORES,
                        help=f"逗号分隔的候选阈值，默认 {DEFAULT_MIN_SCORES}")
    parser.add_argument("--top-k", type=int, default=SWEEP_TOP_K,
                        help=f"扫描固定 top_k，默认 {SWEEP_TOP_K}")
    parser.add_argument("--pace", type=float, default=r5.DEFAULT_PACE,
                        help=f"逐条限流间隔秒数，默认 {r5.DEFAULT_PACE}")
    parser.add_argument("--retries", type=int, default=r5.DEFAULT_RETRIES)
    parser.add_argument("--retry-delay", type=float, default=r5.DEFAULT_RETRY_DELAY)
    parser.add_argument("--out", type=str, default=str(RESULT_PATH))
    args = parser.parse_args()

    thresholds: Tuple[Optional[float], ...] = (
        *(value for _, value in BASELINES), *args.min_scores
    )
    labels: Dict[Optional[float], str] = {value: name for name, value in BASELINES}
    for value in args.min_scores:
        labels.setdefault(value, f"sweep_{value:.2f}")

    started = time.time()
    print(f"{'=' * 74}\nR6 · 真实 Embedding 下的 min_score 定档扫描\n{'=' * 74}")
    print(f"候选阈值 {list(args.min_scores)}；对照臂 {[n for n, _ in BASELINES]}")
    print(f"固定 top_k = {args.top_k}；限流间隔 {args.pace}s\n")

    env = await r5._build_env(
        pace=args.pace, retries=args.retries, retry_delay=args.retry_delay
    )
    problems: List[str] = []
    scenarios_out: List[Dict[str, Any]] = []

    try:
        doc_info = r5._describe(env["doc_embedder"])
        query_info = r5._describe(env["query_embedder"])
        if not doc_info["semantic_enabled"] or not query_info["semantic_enabled"]:
            problems.append(
                "Embedding 不是语义模型（semantic_enabled=False）——"
                "多半是没启用 EMBEDDING_PROVIDER，扫描结果无意义"
            )

        benchmark = r5._load(BENCHMARK_PATH)

        # 分母：全量候选池 / 目标总数 / 噪声总数（跨场景汇总）
        pool_total = 0
        targets_total = 0

        for scenario in benchmark["scenarios"]:
            sid = scenario["id"]
            retrieval = scenario["retrieval"]
            targets = retrieval["expected_hit_sources"]
            print(f"{'─' * 74}\n[场景 {sid}] {scenario['label']}")

            rows = await r5._make_scenario_rows(env, scenario)

            # ★ 全量候选**只取这一次**；下面所有阈值都在它上面离线过滤。
            ranking = await r5._retrieve(
                env, scenario, rows, top_k=r5.FULL_SCAN_TOP_K, min_score=None
            )
            want = set(targets)
            pool = len(ranking)
            n_target = sum(1 for c in ranking if c.source in want)
            n_noise = pool - n_target
            pool_total += pool
            targets_total += n_target
            print(f"  全量候选 {pool} 条（目标 {n_target} / 噪声 {n_noise}）")

            profiles: Dict[str, Any] = {}
            for threshold in thresholds:
                key = labels[threshold]
                profile = _profile_at(rows, ranking, targets, threshold,
                                      top_k=args.top_k)

                # ★ 两段取证：离线过滤必须与**真实带阈值检索**逐条一致。
                #   不发新网络请求（query 向量已缓存），纯属对照。
                live = await r5._retrieve(env, scenario, rows,
                                          top_k=args.top_k, min_score=threshold)
                live_sources = [c.source for c in live]
                profile["live_retrieval_matches_offline_filter"] = (
                    live_sources == profile["injected_sources"]
                )
                if not profile["live_retrieval_matches_offline_filter"]:
                    problems.append(
                        f"{sid}: 阈值 {_fmt(threshold)} 下离线过滤与真实检索不一致"
                    )

                profiles[key] = profile

            rag_off_chars = r5._prompt_chars(rows, [])["prompt_chars"]
            for key, profile in profiles.items():
                profile["prompt_delta_vs_rag_off"] = (
                    profile["prompt_chars"] - rag_off_chars
                )

            scores_desc = [
                {"score": round(c.metadata["score"], 6),
                 "source": c.source,
                 "is_target": c.source in want}
                for c in ranking
            ]

            scenarios_out.append({
                "scenario_id": sid,
                "label": scenario["label"],
                "topic": retrieval["query_topic"],
                "expected_hit_sources": sorted(want),
                "declared": {"top_k": retrieval["top_k"],
                             "min_score": retrieval["min_score"]},
                "pool": {"total": pool, "targets": n_target, "noise": n_noise},
                "rag_off_prompt_chars": rag_off_chars,
                "profiles": profiles,
                "score_landscape": scores_desc,
            })

            # 终端上直接给一张小表，方便人眼比对
            print(f"    {'阈值':>8} {'候选':>5} {'进Prompt':>8} "
                  f"{'目标':>4} {'噪声':>4} {'Prompt':>7}")
            for threshold in thresholds:
                p = profiles[labels[threshold]]
                print(f"    {_fmt(threshold):>8} {p['passed_total']:>5} "
                      f"{p['injected']:>8} {p['passed_targets']:>4} "
                      f"{p['passed_noise']:>4} {p['prompt_chars']:>7}")

        noise_total = pool_total - targets_total

        # ---- 跨场景汇总（micro 口径：先加分子分母再算比率） ----
        summary: Dict[str, Any] = {}
        for threshold in thresholds:
            key = labels[threshold]
            passed_total = 0
            injected = 0
            passed_targets = 0
            passed_noise = 0
            injected_targets = 0
            injected_noise = 0
            prompt_chars = 0
            rag_off = 0
            for item in scenarios_out:
                p = item["profiles"][key]
                passed_total += p["passed_total"]
                injected += p["injected"]
                passed_targets += p["passed_targets"]
                passed_noise += p["passed_noise"]
                injected_targets += p["injected_targets"]
                injected_noise += p["injected_noise"]
                prompt_chars += p["prompt_chars"]
                rag_off += item["rag_off_prompt_chars"]
            summary[key] = {
                "label": key,
                "min_score": threshold,
                "top_k": args.top_k,
                # 1) 命中数量
                "passed_total": passed_total,
                "injected": injected,
                # 2) 相关 chunk 保留率
                "passed_targets": passed_targets,
                "targets_total": targets_total,
                "target_keep_rate": _rate(passed_targets, targets_total),
                # 3) 无关 chunk 过滤率（全量池口径）
                "passed_noise": passed_noise,
                "noise_total": noise_total,
                "noise_filter_rate": _rate(noise_total - passed_noise, noise_total),
                # 附：噪声实际混进 Prompt 的条数（top_k 截断后）
                "injected_noise": injected_noise,
                "injected_targets": injected_targets,
                # 4) Prompt 长度变化
                "prompt_chars": prompt_chars,
                "rag_off_prompt_chars": rag_off,
                "prompt_delta_vs_rag_off": prompt_chars - rag_off,
                "prompt_delta_vs_no_threshold": (
                    prompt_chars - summary["no_threshold"]["prompt_chars"]
                    if "no_threshold" in summary else None
                ),
                "keeps_all_targets": passed_targets == targets_total,
            }

        # ---- 推荐值：在**保住全部目标**的前提下，过滤率最高 ----
        sweep_keys = [labels[v] for v in args.min_scores]
        clean = [k for k in sweep_keys if summary[k]["keeps_all_targets"]]
        ranked = sorted(
            clean,
            key=lambda k: (-summary[k]["noise_filter_rate"], summary[k]["injected"]),
        )
        recommended = ranked[0] if ranked else None
        survivors = [k for k in sweep_keys if summary[k]["keeps_all_targets"]]
        losers = [k for k in sweep_keys if not summary[k]["keeps_all_targets"]]

        print(f"\n{'=' * 74}\n跨场景汇总（全量池 {pool_total} 条；目标 {targets_total}；"
              f"噪声 {noise_total}；top_k={args.top_k}）\n{'=' * 74}")
        header = (f"{'阈值':>8} {'候选':>5} {'进Prompt':>8} {'目标保留率':>10} "
                  f"{'噪声过滤率':>10} {'Prompt':>7} {'Δ无RAG':>8}")
        print(header)
        for threshold in thresholds:
            s = summary[labels[threshold]]
            print(f"{_fmt(threshold):>8} {s['passed_total']:>5} {s['injected']:>8} "
                  f"{s['target_keep_rate']:>10.4f} {s['noise_filter_rate']:>10.4f} "
                  f"{s['prompt_chars']:>7} {s['prompt_delta_vs_rag_off']:>+8}")

        print(f"\n保住全部目标的档位：{survivors}")
        print(f"丢目标的档位：{losers}")
        print(f"推荐值：{_fmt(summary[recommended]['min_score']) if recommended else '无'}"
              + (f"（过滤率 {summary[recommended]['noise_filter_rate']:.4f}、"
                 f"Prompt {summary[recommended]['prompt_chars']}）" if recommended else ""))

        payload = {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "runner": "tests/rag_min_score_sweep_spark_run.py",
            "purpose": "真实 Embedding 下的 min_score 定档扫描（全局单一阈值）",
            "method": {
                "candidates": list(args.min_scores),
                "baselines": [{"label": n, "min_score": v} for n, v in BASELINES],
                "top_k": args.top_k,
                "pool_scan_top_k": r5.FULL_SCAN_TOP_K,
                "filter_semantics": "保留 score >= min_score（含等号）",
                "order": "排序 → min_score → top_k（先过滤后截断）",
                "equivalence_check": "每个阈值都实跑一次真实检索，与离线过滤逐条比对",
                "metric_definitions": {
                    "target_keep_rate": "passed_targets / targets_total（跨场景汇总）",
                    "noise_filter_rate": "(noise_total - passed_noise) / noise_total",
                    "injected": "min(passed_total, top_k) —— 实际进 Prompt 的条数",
                    "prompt_chars": "render_question_prompt 真实渲染字符数（含/不含知识块）",
                },
            },
            "embedding": {
                "document": doc_info,
                "query": query_info,
                "calls": {
                    "document_attempts": len(env["doc_embedder"].durations),
                    "document_retried": env["doc_embedder"].retried,
                    "document_cache_hits": env["doc_embedder"].hits,
                    "query_attempts": len(env["query_embedder"].durations),
                    "query_retried": env["query_embedder"].retried,
                    "query_cache_hits": env["query_embedder"].hits,
                    "worst_seconds": (round(max(env["query_embedder"].durations
                                                 + env["doc_embedder"].durations), 4)
                                      if (env["query_embedder"].durations
                                          or env["doc_embedder"].durations) else None),
                },
            },
            "corpus": {
                "path": str(r5.CORPUS_PATH),
                "documents": len(env["documents"]),
                "chunks_in_store": env["chunks_in_store"],
                "failed_documents": [r for r in env["reports"] if not r["ok"]],
            },
            "benchmark": {
                "path": str(BENCHMARK_PATH),
                "scenarios": len(benchmark["scenarios"]),
            },
            "totals": {
                "pool_total": pool_total,
                "targets_total": targets_total,
                "noise_total": noise_total,
            },
            "summary": summary,
            "recommendation": {
                "value": summary[recommended]["min_score"] if recommended else None,
                "label": recommended,
                "keeps_all_targets": bool(recommended),
                "survivors": survivors,
                "drops_targets": losers,
                "rule": "在**保住全部目标片**的档位里，取无关 chunk 过滤率最高者",
                "caveat": "这是「全局单阈值」约束下的最优；R5 的按场景窗口中点"
                          "（0.833/0.841/0.835）理论上更干净，但需要能传每场景阈值。",
            },
            "scenarios": scenarios_out,
            "problems": problems,
        }
    finally:
        await env["db"].close()
        await env["engine"].dispose()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                        encoding="utf-8")

    print(f"\n耗时 {time.time() - started:.1f}s；结果写入 {out_path}")
    if problems:
        print(f"\n[PROBLEMS] {len(problems)} 项：")
        for p in problems:
            print(f"  - {p}")
        return EXIT_PROBLEMS
    print("\n[OK] 无完整性问题。")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
