# -*- coding: utf-8 -*-
"""RAG 检索**生产默认配置**决策 · 实跑对比（只读，非套件）。

运行：``python backend/tests/rag_default_config_run.py``
落盘：``backend/scripts/rag_default_config.json``

它把「选哪一组 `{top_k, min_score}`」变成可复核的数字，覆盖用户点名的三项：

1. **为什么选该配置** —— 每个候选都给出「目标片是否全中 / 噪声片数 / 平均分」。
2. **对召回数量的影响** —— 注入条数（目标 + 噪声）逐场景列出。
3. **对 Prompt 长度的影响** —— 同时给**两代 formatter** 的长度
   （`normalize_knowledge_context` = 任务 72 之前；`format_knowledge_context` = 当前），
   这样「配置带来的变化」与「组装器带来的变化」可以分开看。

**本运行器不评价大模型生成质量**：结论只针对**检索链路**（召回条数 / 噪声 / Prompt 体积）。

.. note::
   ``top_k`` / ``min_score`` 一律通过**显式构造检索器**驱动（不写环境变量），
   因此本运行器**不需要**也不修改 ``backend/.env``。
   生产实际生效路径是 ``RAG_TOP_K`` / ``RAG_MIN_SCORE``（见 ``services/knowledge_rag.py``）。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Iterator, List, Optional, Sequence, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
PROJECT_DIR = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import InterviewSession, Job, Resume, User  # noqa: E402
from prompts import render_prompt  # noqa: E402
from services import interview_agent, interview_context, interview_core  # noqa: E402
from services.interview_agent import (  # noqa: E402
    PROMPT_GROUP,
    PROMPT_QUESTION_KNOWLEDGE,
    build_question_variables,
    format_knowledge_context,
    normalize_knowledge_context,
)
from services.knowledge_import_pipeline import KnowledgeImportPipeline  # noqa: E402
from services.knowledge_rag import default_embedder  # noqa: E402
from services.vector_knowledge_retriever import VectorKnowledgeRetriever  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

# ============================================================
# 一、常量
# ============================================================
BENCHMARK_PATH = BACKEND_DIR / "scripts" / "interview_eval_benchmark.json"
CORPUS_PATH = BACKEND_DIR / "scripts" / "interview_knowledge.json"
RESULT_PATH = BACKEND_DIR / "scripts" / "rag_default_config.json"

#: 每个场景声明值（基准文件里的 `retrieval.min_score`）。
_DECLARED = object()

#: Part B 的**召回上界**（任务 71 实测，全精度）：`min_score` 高于它，s2 会丢一片目标。
#: 只作**对照**用——它没有余量（`score >= min_score` 含等号，任何分数漂移都会误杀）。
PART_B_RECALL_CEILING = 0.33915287475109385

#: 候选配置。`top_k` / `min_score` 都要显式写出——否则测的是「省略后的默认值」。
CANDIDATES: Tuple[Dict[str, Any], ...] = (
    {
        "id": "current",
        "label": "现状（代码默认：不设阈值）",
        "top_k": 5,
        "min_score": None,
        "note": "top_k=5 = vector_store.DEFAULT_TOP_K；min_score 不设 = 不过滤",
    },
    {
        "id": "declared",
        "label": "基准逐场景声明值（s1/s2=0.25、s3=0.40）",
        "top_k": 5,
        "min_score": _DECLARED,
        "note": "基准文件 frozen_conditions_sha256 覆盖的口径，只作参照（单一全局值做不到）",
    },
    {
        "id": "prod",
        "label": "★ 推荐生产默认：top_k=8 × min_score=0.25",
        "top_k": 8,
        "min_score": 0.25,
        "note": "0.25 在 s1/s2 的标定窗口内；距 Part B 召回上界 0.3392 有 0.089 余量",
    },
    {
        "id": "prod_tk5",
        "label": "对照：top_k=5 × min_score=0.25",
        "top_k": 5,
        "min_score": 0.25,
        "note": "用来验证「top_k 8 与 5 在设阈值后等价」",
    },
    {
        "id": "ceiling",
        "label": "对照：top_k=8 × min_score=Part B 召回上界（无余量）",
        "top_k": 8,
        "min_score": PART_B_RECALL_CEILING,
        "note": "理论最小噪声点，但恰好压在边界上 ⇒ 不作为默认值",
    },
)

QUESTION_REPLY = json.dumps(
    {
        "question": "请结合你的项目经历，说明索引/缓存设计上的取舍。",
        "question_type": "technical",
        "topic": "综合",
        "difficulty": "mid",
        "expected_points": ["取舍", "量化"],
        "reason": "考察工程判断",
    },
    ensure_ascii=False,
)

AGENT_KEYS = {"ok", "question", "question_type", "topic", "difficulty",
              "expected_points", "reason", "error"}


class CountingSpark:
    def __init__(self, reply: str = QUESTION_REPLY) -> None:
        self.reply = reply
        self.calls: List[str] = []

    async def chat_async(self, prompt: str, **_kwargs: Any) -> str:
        self.calls.append(prompt)
        return self.reply


# ============================================================
# 二、环境
# ============================================================
def _load_corpus() -> List[Dict[str, Any]]:
    return json.loads(CORPUS_PATH.read_text(encoding="utf-8"))["documents"]


def _load_scenarios(only: Optional[List[str]]) -> List[Dict[str, Any]]:
    data = json.loads(BENCHMARK_PATH.read_text(encoding="utf-8"))
    scenarios = data["scenarios"]
    if only:
        wanted = set(only)
        scenarios = [s for s in scenarios if s["id"] in wanted]
    return scenarios


@asynccontextmanager
async def _env(scenario: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    session: Optional[AsyncSession] = None
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        session = factory()

        session.add(User(username="task73", email="task73@example.com",
                         password_hash="x", role="user"))
        await session.flush()

        job = Job(**scenario["job"])
        resume = Resume(user_id=1, filename="resume.md",
                        content=scenario["candidate"]["resume_content"])
        session.add_all([job, resume])
        await session.flush()

        cfg = scenario["session"]
        row = InterviewSession(
            user_id=1, job_id=job.id, resume_id=resume.id, status="created",
            interview_type=cfg["interview_type"], difficulty=cfg["difficulty"],
            duration=cfg["duration"], total_questions=cfg["total_questions"],
            current_question_no=1,
        )
        session.add(row)
        await session.commit()
        session_id = row.id

        plan = await interview_core._build_session_plan(session, row)

        ctx = scenario["context"]
        await interview_context.create_context(session, session_id)
        await interview_context.update_context(
            session, session_id,
            current_stage=ctx["current_stage"],
            asked_questions=list(ctx["asked_questions"]),
            covered_topics=list(ctx["covered_topics"]),
            weak_topics=list(ctx["weak_topics"]),
        )
        context = await interview_context.get_context(session, session_id)

        embedder = default_embedder()
        store = SqlAlchemyVectorStore(session)
        pipeline = KnowledgeImportPipeline(session, embedder=embedder, store=store)
        reports = [await pipeline.import_document(dict(doc)) for doc in _load_corpus()]

        yield {
            "db": session, "row": row, "session_id": session_id,
            "job": job, "resume": resume, "plan": plan, "context": context,
            "embedder": embedder, "store": store, "reports": reports,
            "chunks_in_store": await store.count(),
        }
    finally:
        if session is not None:
            await session.close()
        await engine.dispose()


# ============================================================
# 三、单（候选 × 场景）测量
# ============================================================
def _resolve_min_score(candidate: Dict[str, Any], scenario: Dict[str, Any]) -> Optional[float]:
    value = candidate["min_score"]
    if value is _DECLARED:
        return scenario["retrieval"]["min_score"]
    return value


async def run_case(scenario: Dict[str, Any], candidate: Dict[str, Any],
                   with_structure_check: bool = False) -> Dict[str, Any]:
    async with _env(scenario) as env:
        retrieval = scenario["retrieval"]
        min_score = _resolve_min_score(candidate, scenario)
        topic = retrieval["query_topic"]
        targets = set(retrieval["expected_hit_sources"])
        expected_count = retrieval["expected_chunk_count"]

        retriever = VectorKnowledgeRetriever(
            env["embedder"], env["store"],
            top_k=candidate["top_k"], min_score=min_score)
        chunks = await retriever.retrieve(env["job"], topic, env["context"])

        sources = [c.source for c in chunks]
        target_chunks = sum(1 for s in sources if s in targets)
        noise_chunks = len(chunks) - target_chunks

        variables = build_question_variables(env["context"], env["plan"],
                                             env["resume"], env["job"])
        # 两代 formatter 各渲染一次（同一次检索结果、同一组变量）
        legacy_lines = normalize_knowledge_context(chunks)
        legacy_prompt = render_prompt(
            PROMPT_QUESTION_KNOWLEDGE,
            {**variables, "knowledge_context": legacy_lines},
            group=PROMPT_GROUP,
        )
        new_lines = format_knowledge_context(chunks)
        _, new_prompt = interview_agent.render_question_prompt(variables, chunks)

        case: Dict[str, Any] = {
            "scenario_id": scenario["id"],
            "candidate_id": candidate["id"],
            "conditions": {
                "query_topic": topic,
                "top_k": candidate["top_k"],
                "min_score": min_score,
                "min_score_source": ("declared" if candidate["min_score"] is _DECLARED
                                     else "candidate"),
                "corpus_documents": len(_load_corpus()),
                "chunks_in_store": env["chunks_in_store"],
            },
            "recall": {
                "chunk_count": len(chunks),
                "target_chunks": target_chunks,
                "noise_chunks": noise_chunks,
                "expected_target_chunks": expected_count,
                "coverage": f"{target_chunks}/{expected_count}",
                "target_full": target_chunks >= expected_count,
                "sources": sources,
                "scores": [round(c.metadata["score"], 6) for c in chunks],
            },
            "length": {
                "legacy_formatter_lines": len(legacy_lines),
                "new_formatter_lines": len(new_lines),
                "legacy_knowledge_block_chars": len("\n".join(legacy_lines)),
                "new_knowledge_block_chars": len("\n".join(new_lines)),
                "legacy_prompt_chars": len(legacy_prompt),
                "new_prompt_chars": len(new_prompt),
            },
        }

        if with_structure_check:
            spark = CountingSpark()
            agent_out = await interview_agent.generate_question(
                env["context"], env["plan"], env["resume"], env["job"],
                knowledge_context=chunks, spark=spark)
            core_spark = CountingSpark()
            core_out = await interview_core.generate_next_question(
                env["db"], env["session_id"],
                context=env["context"], plan=env["plan"],
                retriever=retriever, spark=core_spark)
            case["structure"] = {
                "agent_keys": sorted(agent_out),
                "agent_keys_expected": agent_out.keys() == AGENT_KEYS,
                "core_keys_expected": tuple(core_out) == interview_core.QUESTION_RESULT_FIELDS,
                "agent_prompt_equals_rendered": (spark.calls[0] == new_prompt
                                                 if spark.calls else False),
                "core_prompt_equals_rendered": (core_spark.calls[0] == new_prompt
                                                if core_spark.calls else False),
            }
        return case


# ============================================================
# 四、聚合与报告
# ============================================================
def _totals(cases: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "scenarios": len(cases),
        "chunk_count": sum(c["recall"]["chunk_count"] for c in cases),
        "target_chunks": sum(c["recall"]["target_chunks"] for c in cases),
        "noise_chunks": sum(c["recall"]["noise_chunks"] for c in cases),
        "expected_target_chunks": sum(c["recall"]["expected_target_chunks"] for c in cases),
        "target_full_scenarios": sum(1 for c in cases if c["recall"]["target_full"]),
        "coverage": [c["recall"]["coverage"] for c in cases],
        "legacy_knowledge_block_chars": sum(c["length"]["legacy_knowledge_block_chars"]
                                            for c in cases),
        "new_knowledge_block_chars": sum(c["length"]["new_knowledge_block_chars"]
                                         for c in cases),
        "legacy_prompt_chars": sum(c["length"]["legacy_prompt_chars"] for c in cases),
        "new_prompt_chars": sum(c["length"]["new_prompt_chars"] for c in cases),
    }


def _delta(before: int, after: int) -> Dict[str, Any]:
    return {
        "before": before,
        "after": after,
        "saved": before - after,
        "ratio": round((before - after) / before * 100, 2) if before else None,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="RAG 检索生产默认配置决策（实跑）")
    parser.add_argument("--only", nargs="*", default=None, help="只跑指定场景 id")
    args = parser.parse_args(argv)

    scenarios = _load_scenarios(args.only)
    if not scenarios:
        print("没有匹配的场景", file=sys.stderr)
        return 2

    print("=" * 78)
    print("RAG 检索生产默认配置 · 决策实跑（只测检索链路，不评价生成质量）")
    print("=" * 78)
    print(f"  基准：{BENCHMARK_PATH.relative_to(PROJECT_DIR)}")
    print(f"  语料：{CORPUS_PATH.relative_to(PROJECT_DIR)}")
    print(f"  候选：{len(CANDIDATES)} 组 × {len(scenarios)} 场景")
    print(f"  Part B 召回上界（全精度）：{PART_B_RECALL_CEILING!r}")

    all_cases: List[Dict[str, Any]] = []
    for candidate in CANDIDATES:
        for scenario in scenarios:
            check = (candidate["id"] == "prod" and scenario is scenarios[0])
            all_cases.append(asyncio.run(run_case(scenario, candidate, check)))

    print("\n" + "-" * 78)
    print("逐候选 · 逐场景")
    print("-" * 78)
    for candidate in CANDIDATES:
        cases = [c for c in all_cases if c["candidate_id"] == candidate["id"]]
        total = _totals(cases)
        print(f"\n  ── [{candidate['id']}] {candidate['label']}")
        print(f"     top_k={candidate['top_k']} "
              f"min_score={'逐场景声明' if candidate['min_score'] is _DECLARED else candidate['min_score']}"
              f"   —— {candidate['note']}")
        for case in cases:
            r, ln = case["recall"], case["length"]
            print(f"     {case['scenario_id']:<32} 注入 {r['chunk_count']:>2} 条"
                  f"（目标 {r['target_chunks']} / 噪声 {r['noise_chunks']}，覆盖 {r['coverage']}）"
                  f"  旧fmt Prompt {ln['legacy_prompt_chars']:>5} 字"
                  f" / 新fmt {ln['new_prompt_chars']:>5} 字")
        print(f"     合计：{total['chunk_count']} 条（目标 {total['target_chunks']} / "
              f"噪声 {total['noise_chunks']}），目标全覆盖场景 "
              f"{total['target_full_scenarios']}/{total['scenarios']}")

    by_id = {c["id"]: _totals([x for x in all_cases if x["candidate_id"] == c["id"]])
             for c in CANDIDATES}
    base = by_id["current"]
    prod = by_id["prod"]

    print("\n" + "=" * 78)
    print("汇总（相对「现状」）")
    print("=" * 78)
    print(f"  候选数：{len(CANDIDATES)}    场景数：{len(scenarios)}")
    print(f"  现状  top_k=5 / min_score=None ：注入 {base['chunk_count']} 条"
          f"（目标 {base['target_chunks']} / 噪声 {base['noise_chunks']}）"
          f"  旧fmt Prompt {base['legacy_prompt_chars']} 字")
    print(f"  推荐  top_k=8 / min_score=0.25 ：注入 {prod['chunk_count']} 条"
          f"（目标 {prod['target_chunks']} / 噪声 {prod['noise_chunks']}）"
          f"  新fmt Prompt {prod['new_prompt_chars']} 字")
    print(f"  召回条数：{base['chunk_count']} → {prod['chunk_count']}"
          f"（−{base['chunk_count'] - prod['chunk_count']} 条）"
          f"；目标片 {base['target_chunks']} → {prod['target_chunks']}（不丢）")
    print(f"  目标全覆盖场景：{prod['target_full_scenarios']}/{prod['scenarios']}"
          f"（现状 {base['target_full_scenarios']}/{base['scenarios']}）")
    print(f"  Prompt（旧fmt）：{base['legacy_prompt_chars']} → {prod['legacy_prompt_chars']}")
    print(f"  Prompt（新fmt）：{base['new_prompt_chars']} → {prod['new_prompt_chars']}")
    print(f"  ★ 配置+组装器叠加：Prompt {base['legacy_prompt_chars']} → "
          f"{prod['new_prompt_chars']}")

    prod_cases = [c for c in all_cases if c["candidate_id"] == "prod"]
    structure = next((c["structure"] for c in prod_cases if "structure" in c), None)
    if structure:
        print("\n  结构核验（推荐配置，s1）："
              f"Agent 8 键={structure['agent_keys_expected']} / "
              f"Core 12 键={structure['core_keys_expected']} / "
              f"Agent 实发 Prompt == 手工渲染={structure['agent_prompt_equals_rendered']} / "
              f"Core 同={structure['core_prompt_equals_rendered']}")

    payload = {
        "name": "RAG 检索生产默认配置决策",
        "scope": "只针对检索链路（召回条数 / 噪声 / Prompt 体积）；不评价大模型生成质量",
        "benchmark": str(BENCHMARK_PATH.relative_to(PROJECT_DIR)),
        "corpus": str(CORPUS_PATH.relative_to(PROJECT_DIR)),
        "part_b_recall_ceiling_exact": PART_B_RECALL_CEILING,
        "candidates": [
            {k: (None if v is _DECLARED else v) for k, v in c.items()}
            for c in CANDIDATES
        ],
        "cases": all_cases,
        "totals": by_id,
        "summary": {
            "recommended": {"top_k": 8, "min_score": 0.25},
            "current": {"top_k": 5, "min_score": None},
            "recall_chunks": _delta(base["chunk_count"], prod["chunk_count"]),
            "target_chunks": _delta(base["target_chunks"], prod["target_chunks"]),
            "noise_chunks": _delta(base["noise_chunks"], prod["noise_chunks"]),
            "target_full_scenarios": f"{prod['target_full_scenarios']}/{prod['scenarios']}",
            "prompt_chars_legacy_formatter": _delta(base["legacy_prompt_chars"],
                                                    prod["legacy_prompt_chars"]),
            "prompt_chars_new_formatter": _delta(base["new_prompt_chars"],
                                                 prod["new_prompt_chars"]),
            "prompt_chars_combined": _delta(base["legacy_prompt_chars"],
                                            prod["new_prompt_chars"]),
            "top_k_equivalence": {
                "top_k_5_vs_8_at_0_25": {
                    "chunk_count": [by_id["prod_tk5"]["chunk_count"],
                                    by_id["prod"]["chunk_count"]],
                    "new_prompt_chars": [by_id["prod_tk5"]["new_prompt_chars"],
                                         by_id["prod"]["new_prompt_chars"]],
                    "identical": (by_id["prod_tk5"]["chunk_count"]
                                  == by_id["prod"]["chunk_count"]
                                  and by_id["prod_tk5"]["new_prompt_chars"]
                                  == by_id["prod"]["new_prompt_chars"]),
                },
            },
        },
    }
    RESULT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"\n  已落盘：{RESULT_PATH.relative_to(PROJECT_DIR)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
