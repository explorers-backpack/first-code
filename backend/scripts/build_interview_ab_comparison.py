# -*- coding: utf-8 -*-
"""把「无 RAG / 有 RAG」两组实验记录整理成结构化对比数据。

**只整理数据，不评价质量；不修改 Agent / Retriever / Prompt。**

输入（两个已存在的基准记录，本脚本**只读**）
------------------------------------------------
- ``backend/scripts/no_rag_baseline.json``（任务 66）
- ``backend/scripts/rag_baseline.json``（任务 67）
- ``backend/scripts/interview_eval_benchmark.json``（固定场景声明，取**原始** job / resume）

输出
----
``backend/scripts/interview_ab_comparison.json``

整理口径
--------
1. **输入条件**：``interview_plan`` / ``resume`` / ``job`` / ``difficulty`` /
   ``history_questions``（另带 ``current_stage``——它同属固定条件）。
   两臂输入**逐键相同** ⇒ 文件里每个场景**只存一份 ``input``**，
   让「输入字段一致」由**结构**保证，而不是靠人工核对。
   脚本仍会独立断言一次并留下证据。
2. **RAG 条件**：无 RAG ``{"enabled": false}``；
   有 RAG ``{"enabled": true, "corpus_chunks_in_store": 32, "retrieved_chunks": [...]}``。
3. **输出**：``question`` / ``question_type`` / ``difficulty`` / ``stage`` / ``reason``。

关于 ``resume`` / ``job``：同时给**原始数据**与**实际渲染进 Prompt 的值**——
两者不是一回事（渲染值会截断 / 拼接），混在一起会让人误判实验条件。

运行
----
``python backend/scripts/build_interview_ab_comparison.py``
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
PROJECT_DIR = BACKEND_DIR.parent

NO_RAG_PATH = BACKEND_DIR / "scripts" / "no_rag_baseline.json"
RAG_PATH = BACKEND_DIR / "scripts" / "rag_baseline.json"
BENCHMARK_PATH = BACKEND_DIR / "scripts" / "interview_eval_benchmark.json"
OUT_PATH = BACKEND_DIR / "scripts" / "interview_ab_comparison.json"

ARM_OFF = "rag_off"
ARM_ON = "rag_on"

#: 规范口径的固定条件（两臂共有）。注意 no_rag_baseline.json 的
#: ``conditions.fixed`` 少写了一个 ``interview_session``——那是**元数据口径不一致**，
#: 不是数据不一致（两臂 ``input`` 实测逐键相同）。本文件以这份规范列表为准。
FIXED_CONDITIONS = [
    "interview_session",
    "interview_plan",
    "resume",
    "job",
    "difficulty",
    "history_questions",
]

OUTPUT_FIELDS = ("question", "question_type", "difficulty", "stage", "reason")

PREVIEW_CHARS = 120


# ============================================================
# 一、读取
# ============================================================
def _read(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"缺少输入文件：{path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _runs_by_id(payload: Dict[str, Any], key: str) -> Dict[str, Dict[str, Any]]:
    return {run["scenario_id"]: run for run in payload.get("runs", [])}


# ============================================================
# 二、整理
# ============================================================
def _compact_chunks(chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """检索片段压成「可比对」的紧凑形状（正文全文留在 rag_baseline.json）。"""
    compact: List[Dict[str, Any]] = []
    for rank, chunk in enumerate(chunks):
        meta = chunk.get("metadata") or {}
        content = chunk.get("content") or ""
        compact.append({
            "rank": rank,
            "score": meta.get("score"),
            "source": chunk.get("source", ""),
            "document_id": meta.get("document_id"),
            "chunk_id": meta.get("chunk_id"),
            "content_chars": len(content),
            "content_preview": content[:PREVIEW_CHARS],
        })
    return compact


def _build_input(no_rag_run: Dict[str, Any],
                 benchmark_scenario: Dict[str, Any]) -> Dict[str, Any]:
    """把两臂共有的输入整理成用户要的五项（另带 current_stage）。"""
    fixed = no_rag_run["fixed_inputs"]
    profile = fixed["user_profile"]
    job_raw = benchmark_scenario["job"]
    resume_content = benchmark_scenario["candidate"]["resume_content"]
    summary_rendered = profile["resume_summary"]

    return {
        "interview_plan": fixed["interview_plan"],
        "resume": {
            "content": resume_content,
            "content_chars": len(resume_content),
            "summary_rendered": summary_rendered,
            "summary_rendered_chars": len(summary_rendered),
            "note": "summary_rendered 是**实际渲染进 Prompt** 的值（Agent 侧上限 1200 字）；"
                    "content 是原始简历正文",
        },
        "job": {
            "job_name": job_raw["job_name"],
            "skills": job_raw["skills"],
            "duty": job_raw["duty"],
            "description_rendered": profile["job"]["description"],
            "note": "description_rendered 是**实际渲染进 Prompt** 的值"
                    "（= 技能要求：{skills}；岗位职责：{duty}）",
        },
        "difficulty": fixed["difficulty"],
        "history_questions": list(fixed["history_questions"]),
        "current_stage": fixed["current_stage"],
    }


def _build_rag_off(no_rag_run: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "enabled": False,
        "prompt_template_key": no_rag_run["provenance"]["prompt_template"],
        "knowledge_injected": False,
    }


def _build_rag_on(rag_run: Dict[str, Any]) -> Dict[str, Any]:
    rag = rag_run["rag"]
    prov = rag_run["provenance"]
    retriever = rag.get("retriever") or {}
    return {
        "enabled": True,
        "corpus_chunks_in_store": prov.get("corpus_chunks_in_store"),
        "retrieved_chunks": _compact_chunks(rag.get("retrieved_chunks") or []),
        "retrieved_count": rag.get("retrieved_count"),
        "retriever": {
            "class": retriever.get("class"),
            "source_name": retriever.get("source_name"),
            "model_name": retriever.get("model_name"),
            "top_k": retriever.get("top_k"),
            "min_score": retriever.get("min_score"),
            "dedup": retriever.get("dedup"),
        },
        "query_topic": (rag.get("query") or {}).get("topic"),
        "knowledge_context_chars": len(rag.get("knowledge_context") or ""),
        "knowledge_context_lines": len(rag.get("knowledge_context_lines") or []),
        "knowledge_injected": not prov.get("knowledge_context_is_empty", True),
        "prompt_template_key": rag.get("prompt_template"),
        "chunks_full_text_in": {
            "file": str(RAG_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
            "path": f"runs[scenario_id={rag_run['scenario_id']}].rag.retrieved_chunks",
        },
    }


def _output_of(run: Dict[str, Any], key: str) -> Dict[str, Any]:
    source = run[key]
    return {field: source.get(field) for field in OUTPUT_FIELDS}


# ============================================================
# 三、主流程
# ============================================================
def main() -> int:
    no_rag = _read(NO_RAG_PATH)
    rag = _read(RAG_PATH)
    benchmark = _read(BENCHMARK_PATH)

    off_runs = _runs_by_id(no_rag, "fixed_inputs")
    on_runs = _runs_by_id(rag, "input")
    scenarios = {s["id"]: s for s in benchmark["scenarios"]}

    problems: List[str] = []

    if list(off_runs) != list(on_runs):
        problems.append(f"两臂场景集合不一致：off={list(off_runs)} on={list(on_runs)}")
    missing = [sid for sid in off_runs if sid not in scenarios]
    if missing:
        problems.append(f"基准里找不到这些场景的声明：{missing}")

    merged: List[Dict[str, Any]] = []
    for sid, off_run in off_runs.items():
        on_run = on_runs.get(sid)
        if on_run is None:
            continue
        scenario = scenarios.get(sid, {})

        off_input = off_run["fixed_inputs"]
        on_input = on_run["input"]
        identical = off_input == on_input
        if not identical:
            problems.append(f"[{sid}] 两臂输入不一致")
        if list(off_input) != list(on_input):
            problems.append(f"[{sid}] 两臂输入键序不一致")

        rag_on = _build_rag_on(on_run)
        if not rag_on["knowledge_injected"]:
            problems.append(f"[{sid}] 有 RAG 臂实际没有注入知识")
        if not rag_on["retrieved_count"]:
            problems.append(f"[{sid}] 有 RAG 臂检索条数为 0")

        merged.append({
            "scenario_id": sid,
            "label": off_run.get("label"),
            "input": _build_input(off_run, scenario),
            "input_identical_across_arms": identical,
            "arms": {
                ARM_OFF: {
                    "rag": _build_rag_off(off_run),
                    "output": _output_of(off_run, "result"),
                },
                ARM_ON: {
                    "rag": rag_on,
                    "output": _output_of(on_run, "output"),
                },
            },
        })

    corpus_sizes = {
        sid: merged_run["arms"][ARM_ON]["rag"]["corpus_chunks_in_store"]
        for sid, merged_run in ((m["scenario_id"], m) for m in merged)
    }
    if len(set(corpus_sizes.values())) > 1:
        problems.append(f"各场景语料切片数不一致：{corpus_sizes}")

    off_llm = (no_rag.get("conditions") or {}).get("llm")
    on_llm = (rag.get("conditions") or {}).get("llm")
    llm_same = off_llm == on_llm
    if not llm_same:
        problems.append("两臂模型配置不一致")

    payload = {
        "name": "interview-ab-comparison",
        "version": 1,
        "purpose": "把「无 RAG / 有 RAG」两组 AI 面试实验记录整理成结构化对比数据。"
                   "只整理数据，**不评价质量**。",
        "scope": {
            "evaluates_quality": False,
            "modifies_code": False,
            "does_not_modify": ["InterviewAgent", "Retriever", "Prompt 模板"],
            "runs_new_experiments": False,
        },
        "sources": {
            "arm_rag_off": {
                "file": str(NO_RAG_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
                "name": no_rag.get("name"),
                "version": no_rag.get("version"),
            },
            "arm_rag_on": {
                "file": str(RAG_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
                "name": rag.get("name"),
                "version": rag.get("version"),
            },
            "fixed_scenarios": {
                "file": str(BENCHMARK_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
                "name": benchmark.get("name"),
                "version": benchmark.get("version"),
            },
        },
        "arms": [
            {
                "id": ARM_OFF,
                "label": "无 RAG",
                "use_rag": False,
                "prompt_template_key": (no_rag.get("conditions") or {}).get(
                    "prompt_template_key"),
                "prompt_template_file": (no_rag.get("conditions") or {}).get(
                    "prompt_template_file"),
            },
            {
                "id": ARM_ON,
                "label": "有 RAG",
                "use_rag": True,
                "prompt_template_key": (rag.get("conditions") or {}).get(
                    "prompt_template_key"),
                "prompt_template_file": (rag.get("conditions") or {}).get(
                    "prompt_template_file"),
            },
        ],
        "conditions": {
            "fixed": FIXED_CONDITIONS,
            "the_only_change": "use_rag（False → True）；其余调用参数完全相同",
            "llm": off_llm,
            "llm_identical_across_arms": llm_same,
            "corpus": str((BACKEND_DIR / "scripts" / "interview_knowledge.json")
                          .relative_to(PROJECT_DIR)).replace("\\", "/"),
        },
        "comparability": {
            "checked": "每个场景：无 RAG 的 fixed_inputs == 有 RAG 的 input（键与值逐项相同）",
            "per_scenario": {m["scenario_id"]: m["input_identical_across_arms"]
                             for m in merged},
            "input_stored_once": "每个场景只存一份 input，两臂共用 ⇒ 结构上保证输入一致",
        },
        "schemas": {
            "input": {
                "interview_plan": "Planner（use_llm=False）产出的计划载荷，7 键",
                "resume": "content=原始简历正文；summary_rendered=实际进 Prompt 的值",
                "job": "job_name/skills/duty=原始岗位行；description_rendered=实际进 Prompt 的值",
                "difficulty": "会话行声明的难度，同时是 Agent 的期望难度",
                "history_questions": "context.asked_questions（用于查重）",
                "current_stage": "context.current_stage（固定条件之一，非用户五项但同属固定输入）",
            },
            "rag": {
                "enabled": "本臂是否注入检索知识",
                "corpus_chunks_in_store": "仅 enabled=true 时存在：向量库里可检索的切片总数",
                "retrieved_chunks": "仅 enabled=true 时存在：Retriever 实际返回的片段（紧凑形状）",
                "retrieved_chunks[].content_preview": f"正文前 {PREVIEW_CHARS} 字；"
                                                      "全文见 rag_baseline.json",
            },
            "output": {field: "见 field_map / 评价字段说明" for field in OUTPUT_FIELDS},
        },
        "field_map": {"type": "question_type"},
        "output_fields": list(OUTPUT_FIELDS),
        "scenarios": merged,
        "summary": {
            "scenarios": len(merged),
            "arms": 2,
            "runs_total": len(merged) * 2,
            "inputs_identical_all": all(m["input_identical_across_arms"] for m in merged),
            "llm_identical_across_arms": llm_same,
            "corpus_chunks_in_store": corpus_sizes,
            "retrieved_counts": {m["scenario_id"]:
                                 m["arms"][ARM_ON]["rag"]["retrieved_count"]
                                 for m in merged},
            "knowledge_injected_all": all(
                m["arms"][ARM_ON]["rag"]["knowledge_injected"] for m in merged),
            "note": "本文件只整理实验数据，不评价生成质量。",
        },
        "integrity": {
            "checks_passed": not problems,
            "problems": problems,
        },
    }

    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                        encoding="utf-8")

    print("=" * 74)
    print("无 RAG / 有 RAG 实验记录 · 结构化对比")
    print("=" * 74)
    print(f"  无 RAG 记录：{payload['sources']['arm_rag_off']['file']}")
    print(f"  有 RAG 记录：{payload['sources']['arm_rag_on']['file']}")
    print(f"  场景数：{payload['summary']['scenarios']}　"
          f"实验次数：{payload['summary']['runs_total']}")
    print(f"  输入逐键一致（全部场景）：{payload['summary']['inputs_identical_all']}")
    print(f"  模型配置两臂一致：{payload['summary']['llm_identical_across_arms']}")
    print(f"  语料切片数：{payload['summary']['corpus_chunks_in_store']}")
    print(f"  各场景检索条数：{payload['summary']['retrieved_counts']}")
    print("\n  场景 × 两臂（RAG 条件 → 输出字段数）：")
    print(f"    {'scenario_id':<30}{'rag_off':<12}{'rag_on'}")
    for m in merged:
        off = m["arms"][ARM_OFF]
        on = m["arms"][ARM_ON]
        print(f"    {m['scenario_id']:<30}"
              f"enabled={str(off['rag']['enabled']):<6}"
              f"enabled={on['rag']['enabled']}, "
              f"检索 {on['rag']['retrieved_count']} 条")
    print(f"\n  完整性校验：{'通过' if not problems else '失败 ' + str(problems)}")
    print(f"  已写入：{OUT_PATH.relative_to(PROJECT_DIR)}")

    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
