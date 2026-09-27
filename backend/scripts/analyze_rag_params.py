# -*- coding: utf-8 -*-
"""分析当前 RAG 检索参数（``top_k`` / ``min_score``）对 ``knowledge_context`` 的影响。

**只分析已有实验数据，不重跑实验、不修改任何代码。**

数据来源（全部只读）
--------------------
- ``backend/scripts/rag_baseline.json``（任务 67：实际检索到的 chunk、分数、Prompt 长度）
- ``backend/scripts/no_rag_baseline.json``（任务 66：无 RAG 的 Prompt 长度，用于算增量）
- ``backend/scripts/interview_eval_benchmark.json``（固定场景**声明**的
  ``retrieval.top_k`` / ``retrieval.min_score`` / ``expected_hit_sources`` /
  ``expected_chunk_count``，以及 ``topic_screening`` 的 ``runner_up_score``）

输出
----
``backend/scripts/rag_param_analysis.json``

分析口径
--------
1. **返回 chunk 数量**：实际条数 vs 基准声明的期望条数 vs「按声明 ``min_score`` 过滤后」的条数。
2. **score 分布**：逐场景列出全部分数，分「目标来源」与「非目标来源」两组，
   并算 min / max / mean / median / 噪声上限 / 目标下限 / 目标与噪声的间隔。
3. **低相关 chunk 比例**：两种口径各算一遍——
   **A 来源口径**（``source`` ∉ 基准声明的 ``expected_hit_sources``）、
   **B 阈值口径**（``score`` < 基准声明的 ``min_score``）。两者一致才敢下结论。
4. **Prompt 上下文长度**：无 RAG / 有 RAG 的 Prompt 字符数、增量与增幅、
   知识小节占比，并按「声明 ``min_score``」投影过滤后的长度。
   **投影前先做重建校验**：用记录的 chunk 重算知识小节，必须与实测逐字节相同，
   否则投影不可信。

.. note::
    本脚本**不评价生成质量**，只做参数与上下文的量化分析。
"""

from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
PROJECT_DIR = BACKEND_DIR.parent

RAG_PATH = BACKEND_DIR / "scripts" / "rag_baseline.json"
NO_RAG_PATH = BACKEND_DIR / "scripts" / "no_rag_baseline.json"
BENCHMARK_PATH = BACKEND_DIR / "scripts" / "interview_eval_benchmark.json"
OUT_PATH = BACKEND_DIR / "scripts" / "rag_param_analysis.json"

#: 知识小节的固定样板（与 ``prompts/interview/question_knowledge.txt`` 一致）。
#: 用于「重建校验」与「长度投影」——样板部分**不随 chunk 数量变化**。
BLOCK_HEADING = "## 四·五、参考知识（外部检索结果，可选）"
BODY_PREFIX = "参考知识：\n"
BODY_SUFFIX = (
    "\n\n出题时请把这些知识融入问题的场景与追问点，但不得整段照抄进 `question`，"
    "\n也不得把知识中的结论直接写进 `question`（`expected_points` 可参考）。"
)
SOURCE_FMT = "（来源：{source}）"
BULLET = "- "


# ============================================================
# 一、读取
# ============================================================
def _read(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"缺少输入文件：{path}")
    return json.loads(path.read_text(encoding="utf-8"))


# ============================================================
# 二、知识小节重建（校验 + 投影用同一套函数）
# ============================================================
def _render_line(content: str, source: str) -> str:
    return f"{content}{SOURCE_FMT.format(source=source)}" if source else content


def _render_body(chunks: List[Dict[str, Any]]) -> str:
    """按模板规则重建「注入正文」（不含标题行）。"""
    lines = [BULLET + _render_line(c["content"], c["source"]) for c in chunks]
    return BODY_PREFIX + "\n".join(lines) + BODY_SUFFIX


# ============================================================
# 三、单场景分析
# ============================================================
def _analyze_scenario(rag_run: Dict[str, Any],
                      no_rag_run: Dict[str, Any],
                      scenario: Dict[str, Any],
                      screening: Dict[str, Dict[str, Any]],
                      problems: List[str]) -> Dict[str, Any]:
    sid = rag_run["scenario_id"]
    rag = rag_run["rag"]
    prov = rag_run["provenance"]
    retrieval = scenario["retrieval"]

    chunks = rag["retrieved_chunks"]
    scores = [c["metadata"]["score"] for c in chunks]
    sources = [c["source"] for c in chunks]

    target_sources = list(retrieval["expected_hit_sources"])
    declared_min_score = retrieval["min_score"]
    declared_top_k = retrieval["top_k"]
    expected_chunk_count = retrieval["expected_chunk_count"]

    # ---- 1. 返回 chunk 数量 ----
    at_declared = [c for c in chunks if c["metadata"]["score"] >= declared_min_score]

    # ---- 2. score 分布 ----
    target_scores = [c["metadata"]["score"] for c in chunks
                     if c["source"] in target_sources]
    non_target_scores = [c["metadata"]["score"] for c in chunks
                         if c["source"] not in target_sources]
    noise_ceiling = max(non_target_scores) if non_target_scores else None
    target_floor = min(target_scores) if target_scores else None
    gap_after_target = (target_floor - noise_ceiling
                        if (target_floor is not None and noise_ceiling is not None)
                        else None)

    # 与基准 topic_screening 的 runner_up_score 交叉核对（噪声上限应一致）
    # 注意：基准里存的是**6 位小数**的舍入值，实测是全精度 ⇒ 按 6 位比对，
    # 用 1e-9 会比出「假不一致」。
    screen = screening.get(retrieval["query_topic"])
    runner_up_declared = screen["runner_up_score"] if screen else None
    ceiling_matches = (runner_up_declared is None
                       or (noise_ceiling is not None
                           and round(noise_ceiling, 6) == runner_up_declared))
    if not ceiling_matches:
        problems.append(f"[{sid}] 噪声上限与 topic_screening.runner_up_score 不一致："
                        f"{round(noise_ceiling, 6) if noise_ceiling else None} "
                        f"vs {runner_up_declared}")

    # ---- 3. 低相关比例（两种口径）----
    low_by_source = [c for c in chunks if c["source"] not in target_sources]
    low_by_score = [c for c in chunks if c["metadata"]["score"] < declared_min_score]
    if len(low_by_source) != len(low_by_score):
        problems.append(f"[{sid}] 两种低相关口径结果不一致："
                        f"来源口径 {len(low_by_source)} / 阈值口径 {len(low_by_score)}")

    # ---- 4. Prompt 长度 ----
    off_chars = no_rag_run["provenance"]["prompt_chars"]
    on_chars = prov["prompt_chars"]
    block_chars = rag["knowledge_block_chars"]
    actual_body = rag["knowledge_context"]

    # 重建校验：模型不对则投影不可信
    rebuilt = _render_body(chunks)
    if rebuilt != actual_body:
        problems.append(f"[{sid}] 知识小节重建与实测不一致"
                        f"（重建 {len(rebuilt)} 字 / 实测 {len(actual_body)} 字）")

    # 投影：只保留 score >= 声明 min_score 的 chunk
    projected_body = _render_body(at_declared)
    saved = len(actual_body) - len(projected_body)

    # ---- 5. 阈值可选窗口 ----
    window = ([noise_ceiling, target_floor]
              if (noise_ceiling is not None and target_floor is not None) else None)
    in_window = (window is not None
                 and window[0] < declared_min_score <= window[1])

    return {
        "scenario_id": sid,
        "query_topic": retrieval["query_topic"],
        "target_source": target_sources,
        "declared_params": {
            "top_k": declared_top_k,
            "min_score": declared_min_score,
            "expected_chunk_count": expected_chunk_count,
        },
        "chunk_count": {
            "actual": len(chunks),
            "at_declared_min_score": len(at_declared),
            "declared_expected": expected_chunk_count,
            "actual_equals_top_k": len(chunks) == declared_top_k,
            "extra_vs_declared": len(chunks) - expected_chunk_count,
            "at_declared_matches_expected": len(at_declared) == expected_chunk_count,
        },
        "score_distribution": {
            "all": [round(s, 6) for s in scores],
            "target_source": [round(s, 6) for s in target_scores],
            "non_target_source": [round(s, 6) for s in non_target_scores],
            "stats": {
                "count": len(scores),
                "min": round(min(scores), 6),
                "max": round(max(scores), 6),
                "mean": round(statistics.fmean(scores), 6),
                "median": round(statistics.median(scores), 6),
                "target_floor": round(target_floor, 6) if target_floor is not None else None,
                "noise_ceiling": round(noise_ceiling, 6) if noise_ceiling is not None else None,
                "gap_after_target": (round(gap_after_target, 6)
                                     if gap_after_target is not None else None),
            },
            "sources": sources,
            "buckets": _buckets(scores),
        },
        "low_relevance": {
            "definition_source": "source ∉ 基准声明的 expected_hit_sources",
            "definition_score": "score < 基准声明的 min_score",
            "by_source": {
                "count": len(low_by_source),
                "ratio": round(len(low_by_source) / len(chunks), 4),
                "sources": [c["source"] for c in low_by_source],
            },
            "by_score": {
                "count": len(low_by_score),
                "ratio": round(len(low_by_score) / len(chunks), 4),
                "scores": [round(c["metadata"]["score"], 6) for c in low_by_score],
            },
            "agrees": len(low_by_source) == len(low_by_score),
        },
        "prompt_length": {
            "off_chars": off_chars,
            "on_chars": on_chars,
            "delta_chars": on_chars - off_chars,
            "delta_ratio": round((on_chars - off_chars) / off_chars, 4),
            "block_chars": block_chars,
            "knowledge_context_chars": len(actual_body),
            "block_share_of_on": round(block_chars / on_chars, 4),
            "rebuilt_matches_actual": rebuilt == actual_body,
            "projected_at_declared": {
                "chunks": len(at_declared),
                "knowledge_context_chars": len(projected_body),
                "block_chars": len(projected_body) + len(BLOCK_HEADING) + 2,
                "prompt_chars": on_chars - saved,
                "saved_chars": saved,
                "saved_ratio_of_block": round(saved / len(actual_body), 4),
            },
        },
        "min_score_window": {
            "noise_ceiling": round(noise_ceiling, 6) if noise_ceiling is not None else None,
            "target_floor": round(target_floor, 6) if target_floor is not None else None,
            "safe_window": ([round(window[0], 6), round(window[1], 6)]
                            if window else None),
            "width": round(window[1] - window[0], 6) if window else None,
            "declared_in_window": in_window,
            "runner_up_score_crosscheck": runner_up_declared,
            "crosscheck_ok": ceiling_matches,
        },
    }


def _buckets(scores: List[float]) -> Dict[str, int]:
    edges = [(0.5, ">=0.50"), (0.3, "0.30-0.50"), (0.2, "0.20-0.30"),
             (0.1, "0.10-0.20")]
    out = {label: 0 for _, label in edges}
    out["<0.10"] = 0
    for s in scores:
        for edge, label in edges:
            if s >= edge:
                out[label] += 1
                break
        else:
            out["<0.10"] += 1
    return out


# ============================================================
# 四、主流程
# ============================================================
def main() -> int:
    rag = _read(RAG_PATH)
    no_rag = _read(NO_RAG_PATH)
    benchmark = _read(BENCHMARK_PATH)

    off_runs = {r["scenario_id"]: r for r in no_rag["runs"]}
    scenarios = {s["id"]: s for s in benchmark["scenarios"]}
    screening = {r["query"]: r for r in benchmark["topic_screening"]["rows"]}

    problems: List[str] = []
    per_scenario: List[Dict[str, Any]] = []
    for rag_run in rag["runs"]:
        sid = rag_run["scenario_id"]
        if sid not in off_runs or sid not in scenarios:
            problems.append(f"[{sid}] 缺对照数据（无 RAG 记录或基准声明）")
            continue
        per_scenario.append(_analyze_scenario(
            rag_run, off_runs[sid], scenarios[sid], screening, problems))

    # ---- 汇总 ----
    total_chunks = sum(s["chunk_count"]["actual"] for s in per_scenario)
    total_low = sum(s["low_relevance"]["by_source"]["count"] for s in per_scenario)
    total_at_declared = sum(s["chunk_count"]["at_declared_min_score"]
                            for s in per_scenario)
    off_sum = sum(s["prompt_length"]["off_chars"] for s in per_scenario)
    on_sum = sum(s["prompt_length"]["on_chars"] for s in per_scenario)
    proj_sum = sum(s["prompt_length"]["projected_at_declared"]["prompt_chars"]
                   for s in per_scenario)

    # 当前实际生效的参数：rag_baseline 记录的检索器配置
    retriever = rag["runs"][0]["rag"]["retriever"]
    declared_min_scores = {s["scenario_id"]: s["declared_params"]["min_score"]
                           for s in per_scenario}
    effective_min_score = retriever["min_score"]
    min_score_bypassed = (
        effective_min_score is None
        and any(v is not None for v in declared_min_scores.values()))

    if min_score_bypassed:
        # 这是**发现**（F1），不是分析本身出错 ⇒ 只记在 effective_params，
        # 不进 integrity.problems（那一位留给「数据自相矛盾」）。
        pass

    # ---- 分析本身的完整性：这些不成立则结论不可信 ----
    if not all(s["prompt_length"]["rebuilt_matches_actual"] for s in per_scenario):
        problems.append("知识小节重建与实测不一致 ⇒ 长度投影不可信")
    if not all(s["chunk_count"]["at_declared_matches_expected"] for s in per_scenario):
        problems.append("按声明 min_score 过滤后的条数 ≠ 基准声明的期望条数")
    if not all(s["low_relevance"]["agrees"] for s in per_scenario):
        problems.append("两种低相关口径结果不一致")

    findings = [
        {
            "id": "F1",
            "title": "min_score 被绕过：声明的阈值没有生效",
            "evidence": {
                "effective_min_score": effective_min_score,
                "declared_min_score": declared_min_scores,
                "why": "有 RAG 臂走 use_rag=True ⇒ resolve_retriever → "
                       "build_vector_retriever(db) 不传任何 retriever_kwargs "
                       "⇒ min_score 取默认 None",
            },
        },
        {
            "id": "F2",
            "title": "top_k=5 是唯一约束 ⇒ 无论相关与否都恰好注入 5 条",
            "evidence": {
                "top_k": retriever["top_k"],
                "chunks_per_scenario": [s["chunk_count"]["actual"] for s in per_scenario],
                "corpus_chunks_in_store": rag["runs"][0]["provenance"][
                    "corpus_chunks_in_store"],
            },
        },
        {
            "id": "F3",
            "title": "低相关 chunk 占比约三分之二，且两种口径结论一致",
            "evidence": {
                "total_chunks": total_chunks,
                "low_relevance_chunks": total_low,
                "ratio": round(total_low / total_chunks, 4),
                "per_scenario_ratio": [s["low_relevance"]["by_source"]["ratio"]
                                       for s in per_scenario],
                "two_definitions_agree": all(s["low_relevance"]["agrees"]
                                             for s in per_scenario),
            },
        },
        {
            "id": "F4",
            "title": "分数呈「头重脚轻」：目标 chunk 集中在头部，尾部骤降",
            "evidence": {
                "target_floor": {s["scenario_id"]:
                                 s["score_distribution"]["stats"]["target_floor"]
                                 for s in per_scenario},
                "noise_ceiling": {s["scenario_id"]:
                                  s["score_distribution"]["stats"]["noise_ceiling"]
                                  for s in per_scenario},
                "gap_after_target": {s["scenario_id"]:
                                     s["score_distribution"]["stats"]["gap_after_target"]
                                     for s in per_scenario},
            },
        },
        {
            "id": "F5",
            "title": "知识注入让 Prompt 长度接近翻倍",
            "evidence": {
                "prompt_chars_off_total": off_sum,
                "prompt_chars_on_total": on_sum,
                "delta_ratio_total": round((on_sum - off_sum) / off_sum, 4),
                "block_share_of_on": [s["prompt_length"]["block_share_of_on"]
                                      for s in per_scenario],
            },
        },
        {
            "id": "F6",
            "title": "按声明 min_score 过滤可精确复现基准的期望条数",
            "evidence": {
                "at_declared_chunks": [s["chunk_count"]["at_declared_min_score"]
                                       for s in per_scenario],
                "declared_expected": [s["chunk_count"]["declared_expected"]
                                      for s in per_scenario],
                "all_match": all(s["chunk_count"]["at_declared_matches_expected"]
                                 for s in per_scenario),
            },
        },
    ]

    recommendations = [
        {
            "id": "R1",
            "priority": "高",
            "title": "把 min_score 真正接上（当前唯一开关是 use_rag，接不了阈值）",
            "action": "按场景阈值运行时，改用**显式注入** "
                      "VectorKnowledgeRetriever(..., min_score=…, top_k=…) "
                      "（resolve_retriever 的优先级是「显式注入 > use_rag」）；"
                      "或给 build_vector_retriever 增加 retriever_kwargs 透传"
                      "（需改生产代码，本次未做）。",
            "expected_effect": f"注入条数 {total_chunks} → {total_at_declared}，"
                               f"Prompt 合计 {on_sum} → {proj_sum} 字",
            "evidence_ref": ["F1", "F6"],
        },
        {
            "id": "R2",
            "priority": "高",
            "title": "按实测分数窗口标定每个场景的 min_score",
            "action": "窗口 = (噪声上限, 目标下限)，取窗口内即可；"
                      "本实验三个场景的窗口与声明值均落在窗口内。",
            "per_scenario": {
                s["scenario_id"]: {
                    "safe_window": s["min_score_window"]["safe_window"],
                    "width": s["min_score_window"]["width"],
                    "declared": s["declared_params"]["min_score"],
                    "declared_in_window": s["min_score_window"]["declared_in_window"],
                } for s in per_scenario
            },
            "caveat": "窗口窄（如 s3）时对语料微调敏感，需重新标定。",
            "evidence_ref": ["F4"],
        },
        {
            "id": "R3",
            "priority": "中",
            "title": "top_k 可下调，保留少量余量即可",
            "action": "min_score 生效后 top_k 不再是约束；"
                      "按期望条数（2/2/1）+ 余量取 top_k=8 左右，"
                      "避免 top_k 过大把噪声一并纳入候选。",
            "evidence_ref": ["F2", "F6"],
        },
        {
            "id": "R4",
            "priority": "中",
            "title": "对同一来源限制条数（同源多片会挤占名额）",
            "action": "按 source 限流（如每来源最多 2 片），"
                      "避免单一文档占满 top_k。",
            "evidence": "本实验 s3 中 resume://project/order-middle-cache 出现 2 次",
            "evidence_ref": ["F2"],
        },
        {
            "id": "R5",
            "priority": "中",
            "title": "关注窄间隔场景：换更强 Embedding 或人工确认语料",
            "action": "当前 embedder 是词面哈希（hash-local，semantic_enabled=False），"
                      "分数只在**同一模型内**可比。窄间隔场景建议配置真实 Embedding "
                      "（EMBEDDING_* 环境变量）后重新标定。",
            "evidence_ref": ["F4"],
        },
        {
            "id": "R6",
            "priority": "低",
            "title": "把三个量纳入常规观测",
            "action": "注入条数 / 低相关比例 / Prompt 增幅——三者任一异常即告警。",
            "evidence_ref": ["F3", "F5"],
        },
    ]

    payload = {
        "name": "rag-param-analysis",
        "version": 1,
        "purpose": "分析当前 RAG 检索参数（top_k / min_score）对 knowledge_context 的影响，"
                   "定位检索策略问题。只分析已有实验数据，不评价生成质量。",
        "scope": {
            "modifies_code": False,
            "reruns_experiments": False,
            "evaluates_quality": False,
            "sources": {
                "rag_baseline": str(RAG_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
                "no_rag_baseline": str(NO_RAG_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
                "benchmark": str(BENCHMARK_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
            },
        },
        "effective_params": {
            "top_k": retriever["top_k"],
            "min_score": effective_min_score,
            "dedup": retriever["dedup"],
            "model_name": retriever["model_name"],
            "source_name": retriever["source_name"],
            "built_by": retriever["built_by"],
            "note": "use_rag=True 默认组装，不传 retriever_kwargs",
            "min_score_bypassed": min_score_bypassed,
            "min_score_bypassed_detail": (
                "基准为各场景声明了 min_score，但实际生效值为 None ⇒ 阈值过滤完全未生效"
                if min_score_bypassed else None),
        },
        "declared_params": {
            "top_k": {s["scenario_id"]: s["declared_params"]["top_k"]
                      for s in per_scenario},
            "min_score": declared_min_scores,
            "expected_chunk_count": {
                s["scenario_id"]: s["declared_params"]["expected_chunk_count"]
                for s in per_scenario},
        },
        "scenarios": per_scenario,
        "aggregate": {
            "scenarios": len(per_scenario),
            "chunks_injected_total": total_chunks,
            "chunks_at_declared_min_score_total": total_at_declared,
            "low_relevance_total": total_low,
            "low_relevance_ratio": round(total_low / total_chunks, 4),
            "prompt_chars_off_total": off_sum,
            "prompt_chars_on_total": on_sum,
            "prompt_chars_projected_total": proj_sum,
            "prompt_growth_ratio": round((on_sum - off_sum) / off_sum, 4),
            "prompt_saved_by_min_score": on_sum - proj_sum,
        },
        "findings": findings,
        "recommendations": recommendations,
        "integrity": {
            "checks_passed": not problems,
            "problems": problems,
        },
    }

    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                        encoding="utf-8")

    # ---- 控制台报告 ----
    print("=" * 78)
    print("RAG 检索参数分析（top_k / min_score → knowledge_context）")
    print("=" * 78)
    print(f"  实际生效：top_k={payload['effective_params']['top_k']}　"
          f"min_score={payload['effective_params']['min_score']}")
    print(f"  基准声明：min_score={declared_min_scores}")
    print(f"  ★ min_score 被绕过：{min_score_bypassed}")
    print()
    hdr = f"  {'scenario':<30}{'条数':>5}{'声明阈值':>9}{'过滤后':>7}{'低相关':>8}{'Prompt off→on':>18}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for s in per_scenario:
        pl = s["prompt_length"]
        print(f"  {s['scenario_id']:<30}"
              f"{s['chunk_count']['actual']:>5}"
              f"{s['declared_params']['min_score']:>9}"
              f"{s['chunk_count']['at_declared_min_score']:>7}"
              f"{s['low_relevance']['by_source']['count']:>8}"
              f"{pl['off_chars']:>10} →{pl['on_chars']:>6}")
    print()
    print("  分数分布（目标来源 / 非目标来源）：")
    for s in per_scenario:
        sd = s["score_distribution"]
        print(f"    {s['scenario_id']:<30}"
              f"目标 {sd['target_source']}　噪声 {sd['non_target_source']}")
    print()
    agg = payload["aggregate"]
    print(f"  注入合计 {agg['chunks_injected_total']} 条；"
          f"低相关 {agg['low_relevance_total']} 条"
          f"（{agg['low_relevance_ratio']:.1%}）；"
          f"按声明阈值过滤后 {agg['chunks_at_declared_min_score_total']} 条")
    print(f"  Prompt 合计 {agg['prompt_chars_off_total']} → "
          f"{agg['prompt_chars_on_total']} 字（+{agg['prompt_growth_ratio']:.1%}）；"
          f"接上 min_score 后预计 {agg['prompt_chars_projected_total']} 字")
    print()
    print(f"  完整性校验：{'通过' if not problems else '发现问题'}")
    for p in problems:
        print(f"    - {p}")
    print(f"  已写入：{OUT_PATH.relative_to(PROJECT_DIR)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
