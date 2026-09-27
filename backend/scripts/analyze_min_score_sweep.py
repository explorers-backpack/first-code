# -*- coding: utf-8 -*-
"""离线分析：从 R6 扫描结果派生**阈值曲线**与 **top_k × min_score 网格**。

**零网络**：只读 ``scripts/rag_min_score_sweep_spark.json`` 里的 ``score_landscape``
（每个场景全量候选的 ``(score, source, is_target)`` 完整排名）。

为什么单独一个脚本（而不是塞进运行器）
--------------------------------------
运行器要**联网 + 消耗配额**（语料 32 片逐条真实编码）；而这里做的全部是
对已有排名的**离线过滤**——同一次扫描可以派生任意多组阈值 / top_k 组合。
分开之后，反复分析不再重跑模型，也不会把「分析逻辑」和「取数逻辑」混在一起。

口径（与运行器**完全一致**，避免两处各说一套）
----------------------------------------------
- 过滤 ``score >= min_score``（含等号）；
- 顺序是「排序 → min_score → top_k」（先过滤后截断）；
- **目标保留率 / 噪声过滤率都在全量候选池上算**，与 ``top_k`` 无关
  （阈值按分数切，不按名次切）；``top_k`` 只影响「实际进 Prompt 的条数」。

用法
----
::

    python scripts/analyze_min_score_sweep.py
    python scripts/analyze_min_score_sweep.py --max-top-k 10 --out scripts/xxx.json
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent

DEFAULT_SOURCE = BACKEND_DIR / "scripts" / "rag_min_score_sweep_spark.json"
DEFAULT_OUT = BACKEND_DIR / "scripts" / "rag_min_score_sweep_analysis.json"

#: 曲线采样点（比扫描档位更细，用来找「拐点」）。
CURVE_STEPS: Tuple[float, ...] = (
    0.80, 0.81, 0.82, 0.825, 0.83, 0.833, 0.84, 0.85,
)

EXIT_OK = 0
EXIT_USAGE = 2


def _load(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _scenario_stats(scenario: Dict[str, Any]) -> Dict[str, Any]:
    """单场景的分数地形：目标下限 / 噪声上限 / 窗口宽。"""
    landscape = scenario["score_landscape"]
    targets = [c["score"] for c in landscape if c["is_target"]]
    noise = [c["score"] for c in landscape if not c["is_target"]]
    floor = min(targets) if targets else None
    ceiling = max(noise) if noise else None
    return {
        "scenario_id": scenario["scenario_id"],
        "topic": scenario["topic"],
        "pool": len(landscape),
        "target_count": len(targets),
        "noise_count": len(noise),
        "target_floor": floor,
        "noise_ceiling": ceiling,
        "window_width": (None if floor is None or ceiling is None else floor - ceiling),
        "separated": (None if floor is None or ceiling is None else floor > ceiling),
    }


def _profile(
    scenarios: Sequence[Dict[str, Any]],
    min_score: Optional[float],
    top_k: int,
) -> Dict[str, Any]:
    passed_targets = passed_noise = 0
    injected_targets = injected_noise = 0
    for scenario in scenarios:
        kept = [c for c in scenario["score_landscape"]
                if min_score is None or c["score"] >= min_score]
        injected = kept[:top_k]
        passed_targets += sum(1 for c in kept if c["is_target"])
        passed_noise += sum(1 for c in kept if not c["is_target"])
        injected_targets += sum(1 for c in injected if c["is_target"])
        injected_noise += sum(1 for c in injected if not c["is_target"])
    return {
        "min_score": min_score,
        "top_k": top_k,
        "passed_total": passed_targets + passed_noise,
        "passed_targets": passed_targets,
        "passed_noise": passed_noise,
        "injected": injected_targets + injected_noise,
        "injected_targets": injected_targets,
        "injected_noise": injected_noise,
    }


def _rates(profile: Dict[str, Any], targets_total: int, noise_total: int) -> Dict[str, Any]:
    return {
        "target_keep_rate": round(profile["passed_targets"] / targets_total, 6),
        "noise_filter_rate": round(
            (noise_total - profile["passed_noise"]) / noise_total, 6
        ),
        # ★ 两个口径必须分开：``keeps_all_targets`` 是**池级**（通过阈值），
        # ``injects_all_targets`` 是**注入级**（还得躲过 top_k 截断）。
        # 只要求前者会把「目标通过了阈值、却被 top_k 切掉」误判成「保住了」。
        "keeps_all_targets": profile["passed_targets"] == targets_total,
        "injects_all_targets": profile["injected_targets"] == targets_total,
        "injected_noise_share": (
            None if profile["injected"] == 0
            else round(profile["injected_noise"] / profile["injected"], 6)
        ),
    }


def _label(value: Optional[float]) -> str:
    """``:g`` 会去掉尾随零 ⇒ ``0.825`` / ``0.833`` / ``0.83315`` 彼此可区分
    （固定小数位会把 ``0.833`` 与 ``0.83315`` 撞成同一个标签）。"""
    return "无阈值" if value is None else f"{value:g}"


def main() -> int:
    parser = argparse.ArgumentParser(description="R6 扫描结果的离线分析（零网络）")
    parser.add_argument("--source", type=str, default=str(DEFAULT_SOURCE))
    parser.add_argument("--out", type=str, default=str(DEFAULT_OUT))
    parser.add_argument("--max-top-k", type=int, default=8)
    args = parser.parse_args()

    source = Path(args.source)
    if not source.exists():
        print(f"[用法错误] 找不到扫描结果 {source}；先跑 tests/rag_min_score_sweep_spark_run.py")
        return EXIT_USAGE

    data = _load(source)
    scenarios = data["scenarios"]
    totals = data["totals"]
    targets_total = totals["targets_total"]
    noise_total = totals["noise_total"]

    # ---- 一、分数地形（每个场景的窗口） ----
    terrain = [_scenario_stats(s) for s in scenarios]
    floors = [t["target_floor"] for t in terrain if t["target_floor"] is not None]
    max_safe = min(floors) if floors else None
    tightest = min(
        (t for t in terrain if t["target_floor"] is not None),
        key=lambda t: t["window_width"] if t["window_width"] is not None else -1e9,
    )

    print(f"{'=' * 78}\n分数地形（全量池 {totals['pool_total']} 条；目标 {targets_total}；"
          f"噪声 {noise_total}）\n{'=' * 78}")
    print(f"{'场景':<28} {'目标下限':>9} {'噪声上限':>9} {'窗口宽':>10} {'可分':>5}")
    for t in terrain:
        print(f"{t['scenario_id']:<28} {t['target_floor']:>9.6f} "
              f"{t['noise_ceiling']:>9.6f} {t['window_width']:>+10.6f} "
              f"{str(t['separated']):>5}")
    print(f"\n★ 最大安全阈值（= 各场景目标下限的最小值）= "
          f"{max_safe:.6f}" if max_safe is not None else "\n★ 无目标，无法定阈值")
    print(f"★ 最紧窗口 = {tightest['scenario_id']}"
          f"（宽 {tightest['window_width']:+.6f}）")

    # ---- 二、阈值曲线（固定 top_k = 运行器口径） ----
    curve_top_k = data["method"]["top_k"]
    curve: List[Dict[str, Any]] = []
    print(f"\n{'=' * 78}\n阈值曲线（top_k = {curve_top_k}）\n{'=' * 78}")
    print(f"{'阈值':>8} {'候选':>5} {'目标':>4} {'噪声':>4} {'目标保留率':>10} "
          f"{'噪声过滤率':>10} {'进Prompt':>8}")
    samples: List[Optional[float]] = [None]
    for value in (*CURVE_STEPS, max_safe):
        if value is None or value in samples:
            continue
        samples.append(value)
    samples[1:] = sorted(samples[1:])
    for threshold in samples:
        profile = _profile(scenarios, threshold, curve_top_k)
        rates = _rates(profile, targets_total, noise_total)
        row = {**profile, **rates}
        curve.append(row)
        print(f"{_label(threshold):>8} {profile['passed_total']:>5} "
              f"{profile['passed_targets']:>4} {profile['passed_noise']:>4} "
              f"{rates['target_keep_rate']:>10.4f} "
              f"{rates['noise_filter_rate']:>10.4f} {profile['injected']:>8}")

    # ---- 三、top_k × min_score 网格 ----
    grid: List[Dict[str, Any]] = []
    print(f"\n{'=' * 78}\ntop_k × min_score 网格（单元格 = 进 Prompt 条数 / 其中的噪声）\n{'=' * 78}")
    header = f"{'top_k':>6} " + " ".join(
        f"{_label(t):>14}" for t in samples
    )
    print(header)
    for top_k in range(1, args.max_top_k + 1):
        cells = []
        for threshold in samples:
            profile = _profile(scenarios, threshold, top_k)
            rates = _rates(profile, targets_total, noise_total)
            grid.append({**profile, **rates})
            cells.append(f"{profile['injected']}/{profile['injected_noise']}"
                         f"({profile['injected_targets']}✓)")
        print(f"{top_k:>6} " + " ".join(f"{c:>14}" for c in cells))
    print("\n（``注入/噪声``：分子是进 Prompt 的总条数，括号内是其中**命中目标**的条数）")

    # ---- 四、推荐：★ 目标必须**真的进了 Prompt**（躲过 top_k 截断），注入最少 ----
    safe = [row for row in grid if row["injects_all_targets"]]
    best = (min(safe, key=lambda r: (r["injected"], r["injected_noise"]))
            if safe else None)
    if best is not None:
        print(f"\n★ 全部 {targets_total} 条目标**都进了 Prompt**、且注入最少的一组："
              f"min_score={_label(best['min_score'])}、top_k={best['top_k']}"
              f"（注入 {best['injected']} 条，其中噪声 {best['injected_noise']}）")
    else:
        print(f"\n★ 网格内没有任何组合能让 {targets_total} 条目标全部进 Prompt")

    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "analyzer": "scripts/analyze_min_score_sweep.py",
        "source": str(source),
        "source_generated_at": data.get("generated_at"),
        "embedding": data.get("embedding"),
        "totals": totals,
        "terrain": terrain,
        "max_safe_min_score": max_safe,
        "tightest_window": tightest,
        "curve": curve,
        "grid": grid,
        "best_keep_all_targets": best,
        "notes": [
            "全部指标由 score_landscape 离线派生，未联网、未重新编码任何文本。",
            "目标保留率 / 噪声过滤率在全量候选池上算，与 top_k 无关。",
            "top_k 只决定「实际进 Prompt 的条数」。",
        ],
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                        encoding="utf-8")
    print(f"\n结果写入 {out_path}")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
