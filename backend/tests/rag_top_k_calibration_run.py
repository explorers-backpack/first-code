# -*- coding: utf-8 -*-
"""R3 · ``top_k`` 取值口径校准运行器。

目标（对应任务 69 报告的建议 R3）
---------------------------------
任务 69 的建议原文是：

> ``top_k`` 可下调，保留少量余量即可。``min_score`` 生效后 ``top_k`` 不再是约束；
> 按期望条数（2/2/1）+ 余量取 ``top_k=8`` 左右，避免 ``top_k`` 过大把噪声一并纳入候选。

「``top_k`` 过大把噪声一并纳入候选」这句在 R2 之后**需要修正**：``min_score`` 生效后，
候选池里有多少噪声**不影响最终注入条数**——噪声会被阈值切掉。所以本运行器要实测清楚：

1. **``min_score`` 生效后，``top_k`` 到底还约束什么？**
   —— 实测 `top_k` 网格上的注入条数：只要 ``top_k >= 目标片数`` 就恒等于期望条数，
   再往上完全不变（平台区）。**它唯一还能造成的伤害是「截掉目标」**。
2. **「只调小 ``top_k``」能不能替代 ``min_score``？**
   —— 实测：不设阈值时 ``top_k == 期望条数`` 恰好也得到期望条数（**巧合**），
   但只要 ``top_k`` 加 1 就立刻混入噪声。``top_k`` 是**按名次**切、``min_score`` 是**按分数**切
   ⇒ 前者依赖「目标恰好全排在头部」，不可迁移。
3. **``top_k`` 与 Prompt 长度无关**（``min_score`` 生效时）——实测同一场景换 ``top_k``
   Prompt 字符数**逐字节相同**。
4. 给出**推荐 ``top_k``** 与**下限警告**。

本运行器做什么
--------------
- **A 检索层扫描**（纯检索、确定性）：``top_k × {None, 声明阈值}`` 的注入条数与来源。
- **B 平台区 / 下限**：``min_top_k_for_expected``（保住期望条数的最小 ``top_k``）、
  ``plateau_start``（结果不再随 ``top_k`` 变化的最小 ``top_k``）。
- **C 截断证明**：``top_k = 目标片数 - 1`` ⇒ 目标被截掉。
- **D 「只调 top_k」反证**：不设阈值时 ``top_k = 期望条数`` 干净、``+1`` 就脏。
- **E Prompt 无关性**：走真实 ``generate_next_question``（Mock 大模型）换 ``top_k``
  ⇒ Prompt 字符数不变；``top_k`` 小到截掉目标时 Prompt 才变短。

为什么用 Mock
-------------
要测的量（注入条数 / Prompt 字符数）由**检索 + 模板渲染**决定，与模型输出无关
（R2 已用真实星火的 ``rag_baseline.json`` 逐场景证明过这一点）。

为什么这个文件在 ``tests/`` 而不是 ``scripts/``
-----------------------------------------------
项目硬约束：**SQLite 只允许出现在 ``backend/tests/*.py`` 的内存库**。
本运行器要走 ``generate_next_question``（要读会话行 / 上下文）⇒ 必须有库 ⇒ 只能放这里。
**不带 ``test_`` 前缀，不是回归套件，不会进回归循环。**

运行
----
``python backend/tests/rag_top_k_calibration_run.py``
``python backend/tests/rag_top_k_calibration_run.py --scenario s3-cache-consistency-senior``

结果写入 ``backend/scripts/rag_top_k_calibration.json``。
**本运行器不修改任何生产代码、不改基准文件。**
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence

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
from services import interview_agent, interview_context, interview_core  # noqa: E402
from services.interview_agent import build_question_variables  # noqa: E402
from services.knowledge_import_pipeline import KnowledgeImportPipeline  # noqa: E402
from services.knowledge_rag import default_embedder  # noqa: E402
from services.vector_knowledge_retriever import VectorKnowledgeRetriever  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

BENCHMARK_PATH = BACKEND_DIR / "scripts" / "interview_eval_benchmark.json"
CORPUS_PATH = BACKEND_DIR / "scripts" / "interview_knowledge.json"
NO_RAG_PATH = BACKEND_DIR / "scripts" / "no_rag_baseline.json"
RAG_BASELINE_PATH = BACKEND_DIR / "scripts" / "rag_baseline.json"
CALIBRATION_PATH = BACKEND_DIR / "scripts" / "rag_min_score_calibration.json"
RESULT_PATH = BACKEND_DIR / "scripts" / "rag_top_k_calibration.json"

#: 全量扫描用的 top_k（拿「完整候选池」必须够大）。
FULL_SCAN_TOP_K = 100

#: 扫描网格（1 与「目标片数 - 1」也在里面，用来证明截断）。
TOP_K_GRID = (1, 2, 3, 4, 5, 6, 8, 10, 16, 32)

#: 推荐的统一 top_k（本运行器会实测它落在平台区内）。
RECOMMENDED_TOP_K = 8

#: Prompt 无关性验证用到的 top_k 取值（相对各场景声明值）。
#: ``truncated``（期望条数 - 1）只在「目标片数 ≥ 2」的场景才存在——
#: 目标只有 1 片时 ``top_k`` 无法再截（top-1 就是那片目标）。
PROMPT_TOPK_CASES = ("declared", "recommended", "full")
PROMPT_TOPK_CASE_TRUNCATED = "truncated"


# ============================================================
# 一、Mock 大模型
# ============================================================
class SpySpark:
    """记录每次收到的 Prompt；按顺序吐回复，超出即报错（用来断言调用次数）。"""

    def __init__(self, *replies: Any) -> None:
        self.replies = list(replies)
        self.calls: List[str] = []

    async def chat_async(self, message: str) -> str:
        self.calls.append(message)
        if not self.replies:
            raise AssertionError("SpySpark 被调用次数超出预期")
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


def _mock_reply_text(scenario: Dict[str, Any]) -> str:
    return json.dumps(scenario["mock_reply"], ensure_ascii=False)


# ============================================================
# 二、捕获组装器实收参数 / Agent 实收知识 / 最终 Prompt
# ============================================================
@asynccontextmanager
async def _capture() -> Iterator[Dict[str, Any]]:
    captured: Dict[str, Any] = {"resolve_calls": [], "agent_calls": 0}

    original_resolve = interview_core.resolve_retriever
    original_generate = interview_agent.generate_question

    def resolve_wrapper(db: Any, retriever: Any = None,
                        use_rag: bool = False, *,
                        retriever_kwargs: Any = None) -> Any:
        built, warnings = original_resolve(db, retriever, use_rag,
                                           retriever_kwargs=retriever_kwargs)
        captured["resolve_calls"].append({
            "retriever_kwargs_received": retriever_kwargs,
            "built_class": type(built).__name__ if built is not None else None,
            "built_top_k": getattr(built, "top_k", None),
            "built_min_score": getattr(built, "min_score", None),
            "warnings": list(warnings),
        })
        return built, warnings

    async def generate_wrapper(context: Any, plan: Any = None, resume: Any = None,
                               job: Any = None, *, knowledge_context: Any = None,
                               spark: Any = None) -> Dict[str, Any]:
        captured["agent_calls"] += 1
        captured["knowledge_context"] = knowledge_context
        variables = build_question_variables(context, plan, resume, job)
        template, prompt = interview_agent.render_question_prompt(
            variables, knowledge_context)
        captured["prompt_template"] = template
        captured["prompt"] = prompt
        return await original_generate(context, plan, resume, job,
                                       knowledge_context=knowledge_context, spark=spark)

    # ★ 替身必须与被替换函数**同签名**（项目里已因漏跟进而运行期 TypeError 过一次）。
    captured["signatures_preserved"] = {
        "resolve_retriever": (
            list(inspect.signature(resolve_wrapper).parameters)
            == list(inspect.signature(original_resolve).parameters)),
        "generate_question": (
            list(inspect.signature(generate_wrapper).parameters)
            == list(inspect.signature(original_generate).parameters)),
    }
    for name, ok in captured["signatures_preserved"].items():
        if not ok:
            raise AssertionError(f"{name} 替身签名与被替换函数不一致")

    interview_core.resolve_retriever = resolve_wrapper
    interview_agent.generate_question = generate_wrapper
    try:
        yield captured
    finally:
        interview_core.resolve_retriever = original_resolve
        interview_agent.generate_question = original_generate


# ============================================================
# 三、环境
# ============================================================
def _load_corpus() -> List[Dict[str, Any]]:
    return json.loads(CORPUS_PATH.read_text(encoding="utf-8"))["documents"]


@asynccontextmanager
async def _env(scenario: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
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

        session.add(User(username="r3_topk", email="r3_topk@example.com",
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
# 四、A：检索层扫描（纯检索，确定性）
# ============================================================
async def _sweep(env: Dict[str, Any], scenario: Dict[str, Any]) -> Dict[str, Any]:
    """``top_k × min_score`` 网格上的注入条数与来源。"""
    topic = scenario["retrieval"]["query_topic"]
    targets = set(scenario["retrieval"]["expected_hit_sources"])

    async def at(top_k: int, min_score: Optional[float]) -> Dict[str, Any]:
        retriever = VectorKnowledgeRetriever(env["embedder"], env["store"],
                                             top_k=top_k, min_score=min_score)
        chunks = await retriever.retrieve(scenario["job"], topic, env["context"])
        sources = [c.source for c in chunks]
        return {
            "top_k": top_k,
            "min_score": min_score,
            "count": len(chunks),
            "target_count": sum(1 for s in sources if s in targets),
            "non_target_count": sum(1 for s in sources if s not in targets),
            "sources": sources,
            "scores": [round(c.metadata["score"], 6) for c in chunks],
        }

    with_threshold = [await at(k, scenario["retrieval"]["min_score"])
                      for k in TOP_K_GRID]
    without_threshold = [await at(k, None) for k in TOP_K_GRID]

    # 完整候选池（32 片）：用于算「目标片数」与「全量」
    full = await at(FULL_SCAN_TOP_K, None)
    total_targets = full["target_count"]

    return {
        "topic": topic,
        "corpus_chunks": full["count"],
        "total_target_chunks": total_targets,
        "with_declared_threshold": with_threshold,
        "without_threshold": without_threshold,
    }


def _derive_plateau(sweep: Dict[str, Any],
                    expected: int) -> Dict[str, Any]:
    """从「带阈值」那一列推出：保住期望条数的最小 top_k、结果稳定的起点。"""
    rows = sweep["with_declared_threshold"]
    ok = [r["top_k"] for r in rows if r["count"] == expected]
    below = [r["top_k"] for r in rows if r["count"] < expected]
    above = [r["top_k"] for r in rows if r["count"] > expected]
    min_ok = min(ok) if ok else None
    # 平台区：从 min_ok 起，所有更大的 top_k 结果都相同（条数 + 来源序列）
    plateau_start = None
    if min_ok is not None:
        reference = next(r for r in rows if r["top_k"] == min_ok)
        plateau_start = min_ok
        for r in rows:
            if r["top_k"] <= min_ok:
                continue
            if (r["count"], r["sources"]) != (reference["count"], reference["sources"]):
                plateau_start = None
                break
    return {
        "min_top_k_for_expected": min_ok,
        "top_k_values_below_expected": below,
        "top_k_values_above_expected": above,
        "plateau_start": plateau_start,
        "is_plateau": plateau_start is not None,
        "expected": expected,
        "total_target_chunks": sweep["total_target_chunks"],
    }


# ============================================================
# 五、E：Prompt 无关性（走真实流程，Mock 大模型）
# ============================================================
async def _prompt_cases(env: Dict[str, Any], scenario: Dict[str, Any],
                        expected: int) -> Dict[str, Any]:
    declared_top_k = scenario["retrieval"]["top_k"]
    declared_min_score = scenario["retrieval"]["min_score"]
    wanted = {
        "declared": declared_top_k,
        "recommended": RECOMMENDED_TOP_K,
        "full": FULL_SCAN_TOP_K,
    }
    cases: List[str] = list(PROMPT_TOPK_CASES)
    if expected >= 2:
        # 只有目标片数 ≥ 2 时才存在「截掉一片目标」的 top_k
        wanted[PROMPT_TOPK_CASE_TRUNCATED] = expected - 1
        cases.insert(1, PROMPT_TOPK_CASE_TRUNCATED)

    out: Dict[str, Any] = {}
    for label in cases:
        top_k = wanted[label]
        spy = SpySpark(_mock_reply_text(scenario))
        async with _capture() as cap:
            result = await interview_core.generate_next_question(
                env["db"], env["session_id"],
                retriever=None,
                use_rag=True,
                retriever_kwargs={"top_k": top_k, "min_score": declared_min_score},
                spark=spy,
            )
        resolve = cap["resolve_calls"][-1]
        out[label] = {
            "top_k": top_k,
            "min_score": declared_min_score,
            "ok": result["ok"],
            "errors": list(result["errors"]),
            "knowledge_context_count": len(cap.get("knowledge_context") or []),
            "knowledge_context_sources": sorted(
                {c.source for c in (cap.get("knowledge_context") or [])}),
            "prompt_template": cap.get("prompt_template"),
            "prompt_chars": len(cap.get("prompt") or ""),
            "retriever_kwargs_received": resolve["retriever_kwargs_received"],
            "built_top_k": resolve["built_top_k"],
            "built_min_score": resolve["built_min_score"],
            "agent_calls": cap["agent_calls"],
            "spark_calls": len(spy.calls),
            "signatures_preserved": cap["signatures_preserved"],
        }
    out["_cases"] = cases
    out["_truncation_demonstrable"] = expected >= 2
    return out


# ============================================================
# 六、单场景
# ============================================================
async def run_scenario(scenario: Dict[str, Any], no_rag_runs: Dict[str, Any],
                       rag_baseline_runs: Dict[str, Any],
                       problems: List[str]) -> Dict[str, Any]:
    sid = scenario["id"]
    retrieval = scenario["retrieval"]
    expected = retrieval["expected_chunk_count"]
    declared_top_k = retrieval["top_k"]
    declared_min_score = retrieval["min_score"]

    print(f"\n{'─' * 74}\n[场景 {sid}] {scenario['label']}")

    async with _env(scenario) as env:
        print(f"  语料入库：22 篇 → {env['chunks_in_store']} 片")

        sweep = await _sweep(env, scenario)
        plateau = _derive_plateau(sweep, expected)
        print(f"  完整候选池：{sweep['corpus_chunks']} 片，其中目标片 "
              f"{sweep['total_target_chunks']} 片；期望注入 {expected} 条")
        print(f"  {'top_k':>6}{'不设阈值':>12}{'设阈值':>12}   设阈值时的来源")
        for row_t, row_n in zip(sweep["with_declared_threshold"],
                               sweep["without_threshold"]):
            print(f"  {row_t['top_k']:>6}{row_n['count']:>12}{row_t['count']:>12}   "
                  f"{row_t['sources']}")
        print(f"  ★ 保住期望条数的最小 top_k = {plateau['min_top_k_for_expected']}；"
              f"平台区起点 = {plateau['plateau_start']}")

        prompts = await _prompt_cases(env, scenario, expected)
        print(f"  Prompt 无关性（min_score={declared_min_score} 固定）：")
        for label in prompts["_cases"]:
            p = prompts[label]
            print(f"    {label:<12} top_k={p['top_k']:<4} 知识 {p['knowledge_context_count']} 条  "
                  f"Prompt {p['prompt_chars']:>5} 字  ok={p['ok']}")
        if not prompts["_truncation_demonstrable"]:
            print(f"    （该场景只有 1 片目标 ⇒ top_k 无法再截，top-1 就是那片目标）")

        # ---- 断言 ----
        no_rag_prompt = no_rag_runs.get(sid, {}).get("provenance", {}).get("prompt_chars")
        rag_base_prompt = rag_baseline_runs.get(sid, {}).get("provenance", {}).get("prompt_chars")

        # 「只调 top_k」反证：不设阈值时 top_k=期望条数 干净、+1 就脏
        clean_row = next((r for r in sweep["without_threshold"]
                          if r["top_k"] == expected), None)
        dirty_row = next((r for r in sweep["without_threshold"]
                          if r["top_k"] == expected + 1), None)

        checks = [
            ("★ 完整候选池 == 32 片", sweep["corpus_chunks"] == 32),
            ("★ 目标片数与全量扫描一致（> 0）", sweep["total_target_chunks"] > 0),
            (f"★ 设阈值时 top_k={declared_top_k} 的条数 == 期望（{expected}）",
             next(r for r in sweep["with_declared_threshold"]
                  if r["top_k"] == declared_top_k)["count"] == expected),
            ("★ 设阈值时 top_k 小于「目标片数」会截掉目标",
             all(r["count"] < expected for r in sweep["with_declared_threshold"]
                 if r["top_k"] < sweep["total_target_chunks"])),
            ("★ 设阈值时 top_k >= 目标片数 ⇒ 条数恒等于期望（平台区成立）",
             all(r["count"] == expected for r in sweep["with_declared_threshold"]
                 if r["top_k"] >= sweep["total_target_chunks"])),
            ("★ 平台区起点 == 目标片数（结果不再随 top_k 变化）",
             plateau["plateau_start"] == sweep["total_target_chunks"]),
            ("★ 推荐 top_k 落在平台区内",
             plateau["plateau_start"] is not None
             and RECOMMENDED_TOP_K >= plateau["plateau_start"]),
            ("★ 不设阈值时条数恒等于 top_k（阈值才是分离手段）",
             all(r["count"] == r["top_k"] for r in sweep["without_threshold"])),
            ("★ 反证：不设阈值、top_k == 期望条数时恰好干净（巧合）",
             clean_row is not None and clean_row["non_target_count"] == 0),
            ("★ 反证：不设阈值、top_k == 期望条数 + 1 立刻混入噪声",
             dirty_row is not None and dirty_row["non_target_count"] > 0),
            ("★ 不设阈值时目标并非全在头部（top_k 不可迁移）",
             next(r for r in sweep["without_threshold"]
                  if r["top_k"] == max(TOP_K_GRID))["non_target_count"] > 0),
            # Prompt 无关性
            ("★ Prompt：declared / recommended / full 三档字符数完全相同",
             len({prompts["declared"]["prompt_chars"],
                  prompts["recommended"]["prompt_chars"],
                  prompts["full"]["prompt_chars"]}) == 1),
            ("★ Prompt：三档的注入条数与来源也完全相同",
             len({(prompts["declared"]["knowledge_context_count"],
                   tuple(prompts["declared"]["knowledge_context_sources"])),
                  (prompts["recommended"]["knowledge_context_count"],
                   tuple(prompts["recommended"]["knowledge_context_sources"])),
                  (prompts["full"]["knowledge_context_count"],
                   tuple(prompts["full"]["knowledge_context_sources"]))}) == 1),
            ("★ Prompt：top_k 截到「期望条数 - 1」时目标被截掉 ⇒ 条数 < 期望",
             (not prompts["_truncation_demonstrable"])
             or prompts[PROMPT_TOPK_CASE_TRUNCATED]["knowledge_context_count"] < expected),
            ("★ Prompt：截断档的字符数 < 未截断档（确实少注入了内容）",
             (not prompts["_truncation_demonstrable"])
             or prompts[PROMPT_TOPK_CASE_TRUNCATED]["prompt_chars"]
             < prompts["declared"]["prompt_chars"]),
            ("★ 组装器实收 retriever_kwargs == 传入值（每一档）",
             all(prompts[label]["retriever_kwargs_received"]
                 == {"top_k": prompts[label]["top_k"], "min_score": declared_min_score}
                 for label in prompts["_cases"])),
            ("每档只调用一次大模型 / 一次 Agent",
             all(prompts[label]["spark_calls"] == 1
                 and prompts[label]["agent_calls"] == 1
                 for label in prompts["_cases"])),
            ("替身与被替换函数同签名",
             all(all(prompts[label]["signatures_preserved"].values())
                 for label in prompts["_cases"])),
            ("★ declared 档用 question_knowledge 模板",
             prompts["declared"]["prompt_template"] == "question_knowledge"),
        ]

        # 与既有基准交叉核对（真实星火记录）
        cross_checks = [
            {"name": "declared 档 Prompt 字符数 == 有 RAG 基准（真实星火）记录",
             "expected": rag_base_prompt, "actual": prompts["declared"]["prompt_chars"]},
            {"name": "无 RAG 基准 Prompt 字符数可用（对照，不参与本任务判定）",
             "expected": no_rag_prompt, "actual": no_rag_prompt},
        ]
        for cc in cross_checks:
            cc["match"] = cc["expected"] == cc["actual"]

        for name, ok in checks:
            if not ok:
                problems.append(f"[{sid}] {name}")
            print(f"    {'PASS' if ok else 'FAIL'}  {name}")

    return {
        "scenario_id": sid,
        "label": scenario["label"],
        "declared": {"top_k": declared_top_k, "min_score": declared_min_score,
                     "expected_chunk_count": expected},
        "sweep": sweep,
        "plateau": plateau,
        "prompt_invariance": prompts,
        "checks": [{"name": n, "passed": ok} for n, ok in checks],
        "all_checks_passed": all(ok for _, ok in checks),
        "cross_checks": cross_checks,
    }


# ============================================================
# 七、主流程
# ============================================================
def _load(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"缺少输入文件：{path}")
    return json.loads(path.read_text(encoding="utf-8"))


async def main() -> int:
    parser = argparse.ArgumentParser(description="R3 · top_k 取值口径校准")
    parser.add_argument("--scenario", action="append", default=None,
                        help="只跑指定场景 id（可重复）；缺省跑全部")
    parser.add_argument("--out", default=str(RESULT_PATH))
    args = parser.parse_args()

    benchmark = _load(BENCHMARK_PATH)
    no_rag_runs = {r["scenario_id"]: r for r in _load(NO_RAG_PATH)["runs"]}
    rag_baseline_runs = {r["scenario_id"]: r for r in _load(RAG_BASELINE_PATH)["runs"]}
    min_score_calibration = _load(CALIBRATION_PATH)

    scenarios = benchmark["scenarios"]
    if args.scenario:
        wanted = set(args.scenario)
        scenarios = [s for s in scenarios if s["id"] in wanted]
        missing = wanted - {s["id"] for s in scenarios}
        if missing:
            raise SystemExit(f"未知场景 id：{sorted(missing)}")

    print("=" * 74)
    print("R3 · top_k 取值口径校准")
    print("=" * 74)
    print(f"  条件来源：{BENCHMARK_PATH.relative_to(PROJECT_DIR)}")
    print(f"  上游标定：{CALIBRATION_PATH.relative_to(PROJECT_DIR)}（任务 70 · R2）")
    print(f"  扫描网格：top_k ∈ {list(TOP_K_GRID)}")
    print("  透传路径：use_rag=True + retriever_kwargs（任务 70 · R1）")
    print("  大模型：Mock（只测检索 + Prompt，与模型输出无关）")
    print(f"  场景数：{len(scenarios)}")

    problems: List[str] = []
    results: List[Dict[str, Any]] = []
    for scenario in scenarios:
        results.append(await run_scenario(
            scenario, no_rag_runs, rag_baseline_runs, problems))

    # ---- 汇总 ----
    aggregate = {
        "scenarios": len(results),
        "total_target_chunks": {
            r["scenario_id"]: r["sweep"]["total_target_chunks"] for r in results},
        "min_top_k_for_expected": {
            r["scenario_id"]: r["plateau"]["min_top_k_for_expected"] for r in results},
        "plateau_start": {
            r["scenario_id"]: r["plateau"]["plateau_start"] for r in results},
        "declared_top_k": {
            r["scenario_id"]: r["declared"]["top_k"] for r in results},
        "recommended_top_k": RECOMMENDED_TOP_K,
        "recommended_in_all_plateaus": all(
            r["plateau"]["plateau_start"] is not None
            and RECOMMENDED_TOP_K >= r["plateau"]["plateau_start"]
            for r in results),
        "prompt_chars_invariant_over_top_k": all(
            len({r["prompt_invariance"][label]["prompt_chars"]
                 for label in ("declared", "recommended", "full")}) == 1
            for r in results),
        "topk_only_cannot_separate": all(
            any(row["non_target_count"] > 0
                for row in r["sweep"]["without_threshold"]
                if row["top_k"] == r["declared"]["expected_chunk_count"] + 1)
            for r in results),
        "hard_floor_top_k": {
            r["scenario_id"]: r["sweep"]["total_target_chunks"] for r in results},
    }

    payload = {
        "name": "rag-top-k-calibration",
        "version": 1,
        "purpose": "校准 top_k 取值口径：实测 min_score 生效后 top_k 还约束什么、"
                   "「只调小 top_k」能否替代 min_score、top_k 是否影响 Prompt 长度，"
                   "并给出推荐值与下限警告。不修改生产代码、不改基准文件、不评价生成质量。",
        "scope": {
            "modifies_code": False,
            "modifies_benchmark": False,
            "evaluates_quality": False,
            "llm": "Mock（SpySpark + 场景 mock_reply）",
        },
        "sources": {
            "benchmark": str(BENCHMARK_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
            "corpus": str(CORPUS_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
            "rag_baseline": str(RAG_BASELINE_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
            "min_score_calibration": str(CALIBRATION_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
        },
        "grid": list(TOP_K_GRID),
        "recommended_top_k": RECOMMENDED_TOP_K,
        "upstream_min_score": {
            s["scenario_id"]: {
                "declared": s["calibration"]["declared_min_score"],
                "recommended": s["calibration"]["recommendation"]["recommended"],
                "window": s["calibration"]["window"]["display_6dp"],
            } for s in min_score_calibration["scenarios"]},
        "scenarios": results,
        "aggregate": aggregate,
        "integrity": {"checks_passed": not problems, "problems": problems},
    }

    out_path = Path(args.out)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                        encoding="utf-8")

    print("\n" + "=" * 74)
    print("结果")
    print("=" * 74)
    print(f"  目标片数：{aggregate['total_target_chunks']}")
    print(f"  保住期望条数的最小 top_k：{aggregate['min_top_k_for_expected']}")
    print(f"  平台区起点：{aggregate['plateau_start']}")
    print(f"  推荐 top_k = {RECOMMENDED_TOP_K}"
          f"（落在全部平台区：{aggregate['recommended_in_all_plateaus']}）")
    print(f"  min_score 生效时 Prompt 与 top_k 无关："
          f"{aggregate['prompt_chars_invariant_over_top_k']}")
    print(f"  「只调 top_k」不可分离（+1 即脏）："
          f"{aggregate['topk_only_cannot_separate']}")
    print()
    print(f"  完整性校验：{'通过' if not problems else '发现问题'}")
    for p in problems:
        print(f"    - {p}")
    try:
        shown = out_path.resolve().relative_to(PROJECT_DIR)
    except ValueError:
        shown = out_path
    print(f"  已写入：{shown}")

    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
