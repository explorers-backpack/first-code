# -*- coding: utf-8 -*-
"""R2 · ``min_score`` 阈值标定 + 实跑验证运行器。

目标（对应任务 69 报告的建议 R2）
---------------------------------
把任务 69 **实测**出来的阈值安全窗口

- s1 ``(0.156532, 0.377964]``
- s2 ``(0.138212, 0.339153]``
- s3 ``(0.360771, 0.423982]``

**落地为可用的标定值**，并用任务 70（R1）新增的 ``retriever_kwargs`` **透传路径实跑验证**：

- 注入条数应由 **15 → 5**
- Prompt 合计应由 **13810 → 9852** 字

本运行器做什么
--------------
1. **重测窗口**：对每个场景做一次**全量候选**扫描（``top_k=100`` / ``min_score=None``，
   32 片语料），按「目标来源 / 非目标来源」分组，取
   ``窗口 = (非目标最高分, 目标最低分]``——**不是**照抄任务 69 的数字，
   而是重新测一遍再与任务 69 交叉核对。
2. **四臂实跑**（走真实 ``generate_next_question``，只是把大模型换成 Mock）：

   ====================  =========================  ======================
   臂                    调用                       预期
   ====================  =========================  ======================
   ``rag_off``           ``use_rag=False``          0 条 / 与无 RAG 基准同长
   ``rag_on_no_threshold``  ``use_rag=True`` 不传 kwargs  5 条 / 与有 RAG 基准同长
   ``rag_on_declared``   ``use_rag=True`` + 声明阈值   ``expected_chunk_count``
   ``rag_on_recommended``  ``use_rag=True`` + 窗口中点   同 ``rag_on_declared``
   ====================  =========================  ======================

3. **边界探针**：直接驱动检索器，证明窗口端点就是**真实的判定边界**——
   ``min_score == 目标下限`` 时目标仍被保留（``>=`` 含等号），
   再抬高一个 ULP 就一条不剩；``min_score == 非目标上限`` 时噪声会被放进 1 条，
   再抬高一个 ULP 就干净。
4. **闭环核对**：把实跑结果与 ``rag_param_analysis.json``（任务 69 的**投影**）
   逐项对齐——投影 9852 必须等于实跑 9852，否则任务 69 的结论不成立。

为什么用 Mock 而不是真实星火
----------------------------
本运行器**要测的量**（注入条数、Prompt 字符数）完全由**检索 + 模板渲染**决定，
**与模型输出无关**；而真实星火每次输出不同、每次 11–19 s。基准文档
（``interview_eval_benchmark.json`` 的 ``comparison.llm_nondeterminism``）已明确：
比「输入条件」必须用同一条 Mock 回复。
**并且**本运行器会拿 Mock 结果去和**真实星火**跑出来的 ``rag_baseline.json``
逐场景核对 Prompt 字符数——若两者一致，就证明 Mock 跑出来的长度**就是**真实
链路会看到的长度（闭环见 ``cross_checks``）。

为什么这个文件在 ``tests/`` 而不是 ``scripts/``
-----------------------------------------------
项目硬约束：**SQLite 只允许出现在 ``backend/tests/*.py`` 的内存库**，
``scripts/`` 等运行时代码不得出现 ``sqlite`` / ``create_engine``。
本运行器要走 ``generate_next_question``（要读会话行 / 上下文）⇒ 必须有库
⇒ 只能放这里。**不带 ``test_`` 前缀，不是回归套件，不会进回归循环。**

运行
----
``python backend/tests/rag_min_score_calibration_run.py``
``python backend/tests/rag_min_score_calibration_run.py --scenario s3-cache-consistency-senior``

结果写入 ``backend/scripts/rag_min_score_calibration.json``。
**本运行器不修改任何生产代码、不改基准文件。**
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import math
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

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
ANALYSIS_PATH = BACKEND_DIR / "scripts" / "rag_param_analysis.json"
RESULT_PATH = BACKEND_DIR / "scripts" / "rag_min_score_calibration.json"

#: 全量扫描用的参数（拿"窗口"必须看全部候选，不能被 top_k 截断）。
FULL_SCAN_TOP_K = 100

#: 基准 ``topic_screening.usable_rule`` 用的经验间隔阈值：低于它认为窗口太窄。
USABLE_GAP_THRESHOLD = 0.02

#: 窗口宽度分档（仅用于给标定值贴"稳健度"标签，不参与判定）。
ROBUSTNESS_WIDE = 0.15
ROBUSTNESS_NARROW = 0.05

#: 四臂定义。
ARM_OFF = "rag_off"
ARM_ON_NO_THRESHOLD = "rag_on_no_threshold"
ARM_ON_DECLARED = "rag_on_declared"
ARM_ON_RECOMMENDED = "rag_on_recommended"
ARMS = (ARM_OFF, ARM_ON_NO_THRESHOLD, ARM_ON_DECLARED, ARM_ON_RECOMMENDED)

#: 知识小节边界（与 ``question_knowledge.txt`` 一致）。
KNOWLEDGE_HEADING = "## 四·五、参考知识（外部检索结果，可选）"


# ============================================================
# 一、Mock 大模型（只记录 Prompt，按场景 mock_reply 回复）
# ============================================================
class SpySpark:
    """记录每次收到的 Prompt；按顺序吐回复，超出即报错（用来断言调用次数）。"""

    def __init__(self, *replies: Any) -> None:
        self.replies = list(replies)
        self.calls: List[str] = []

    @property
    def call_count(self) -> int:
        return len(self.calls)

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
# 二、捕获「组装器实收参数 / 检索返回 / 最终 Prompt」
# ============================================================
@asynccontextmanager
async def _capture() -> Iterator[Dict[str, Any]]:
    """包装三处模块属性，记录**真实流程**里的实际取值。

    - ``resolve_retriever``：拿到它**实际收到**的 ``retriever_kwargs`` 与组装出的检索器
    - ``retrieve_knowledge``：拿到检索器的**实际返回**（全项目唯一接线点）
    - ``interview_agent.generate_question``：拿到 Agent 实际收到的知识 + 渲染出的 Prompt
    """
    captured: Dict[str, Any] = {
        "resolve_calls": [],
        "retrieve_calls": [],
        "agent_calls": 0,
    }

    original_resolve = interview_core.resolve_retriever
    original_retrieve = interview_core.retrieve_knowledge
    original_generate = interview_agent.generate_question

    def resolve_wrapper(db: Any, retriever: Any = None,
                        use_rag: bool = False, *,
                        retriever_kwargs: Any = None) -> Any:
        built, warnings = original_resolve(db, retriever, use_rag,
                                           retriever_kwargs=retriever_kwargs)
        captured["resolve_calls"].append({
            "use_rag": use_rag,
            "retriever_injected": retriever is not None,
            "retriever_kwargs_received": retriever_kwargs,
            "built_class": type(built).__name__ if built is not None else None,
            "built_top_k": getattr(built, "top_k", None),
            "built_min_score": getattr(built, "min_score", None),
            "warnings": list(warnings),
        })
        captured["retriever"] = built
        return built, warnings

    async def retrieve_wrapper(job: Any = None, topic: Any = "",
                               context: Any = None, *,
                               retriever: Any = None) -> List[Any]:
        chunks = await original_retrieve(job, topic, context, retriever=retriever)
        captured["retrieve_calls"].append({
            "topic": topic,
            "returned_count": len(chunks or []),
            "returned_sources": [getattr(c, "source", "") for c in (chunks or [])],
            "returned_scores": [getattr(c, "metadata", {}).get("score")
                                for c in (chunks or [])],
        })
        return chunks

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

    # ★ 替身必须与被替换函数**同签名**：R1 加 ``retriever_kwargs`` 时，
    #   项目里已有一个运行器（``rag_baseline_run.py``）因漏跟进而运行期 TypeError。
    captured["signatures_preserved"] = {
        "resolve_retriever": (
            list(inspect.signature(resolve_wrapper).parameters)
            == list(inspect.signature(original_resolve).parameters)),
        "retrieve_knowledge": (
            list(inspect.signature(retrieve_wrapper).parameters)
            == list(inspect.signature(original_retrieve).parameters)),
        "generate_question": (
            list(inspect.signature(generate_wrapper).parameters)
            == list(inspect.signature(original_generate).parameters)),
    }
    for name, ok in captured["signatures_preserved"].items():
        if not ok:
            raise AssertionError(f"{name} 替身签名与被替换函数不一致")

    interview_core.resolve_retriever = resolve_wrapper
    interview_core.retrieve_knowledge = retrieve_wrapper
    interview_agent.generate_question = generate_wrapper
    try:
        yield captured
    finally:
        interview_core.resolve_retriever = original_resolve
        interview_core.retrieve_knowledge = original_retrieve
        interview_agent.generate_question = original_generate


# ============================================================
# 三、环境（与基准测试同一套建库手法）
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

        session.add(User(username="r2_calib", email="r2_calib@example.com",
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

        # 与流程内部同一个 Planner 函数（use_llm=False ⇒ 确定性）
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

        # 语料入库（开启 RAG 的前提；不改变上面任何固定输入）
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
# 四、单臂实跑
# ============================================================
async def _run_arm(env: Dict[str, Any], scenario: Dict[str, Any], arm: str,
                   retriever_kwargs: Optional[Dict[str, Any]]
                   ) -> Dict[str, Any]:
    """跑一个臂：走真实 ``generate_next_question``，只把大模型换成 Mock。"""
    spy = SpySpark(_mock_reply_text(scenario))
    async with _capture() as cap:
        result = await interview_core.generate_next_question(
            env["db"], env["session_id"],
            retriever=None,                       # 四臂都不显式注入（走组装器）
            use_rag=(arm != ARM_OFF),
            retriever_kwargs=retriever_kwargs,
            spark=spy,
        )

    resolve_calls = cap["resolve_calls"]
    retrieve_calls = cap["retrieve_calls"]
    chunks = retrieve_calls[-1]["returned_sources"] if retrieve_calls else []
    prompt = cap.get("prompt") or ""
    knowledge_context = cap.get("knowledge_context")

    return {
        "arm": arm,
        "use_rag": arm != ARM_OFF,
        "retriever_kwargs_passed": retriever_kwargs,
        "ok": result["ok"],
        "errors": list(result["errors"]),
        "warnings": list(result["warnings"]),
        "prompt_template": cap.get("prompt_template"),
        "prompt_chars": len(prompt),
        "prompt_has_knowledge_block": KNOWLEDGE_HEADING in prompt,
        "knowledge_context_count": len(knowledge_context or []),
        "knowledge_context_sources": sorted({c.source for c in (knowledge_context or [])}),
        "retrieved_count": len(chunks),
        "retrieved_sources": chunks,
        "resolve_calls": resolve_calls,
        "retriever_kwargs_received": (
            resolve_calls[-1]["retriever_kwargs_received"] if resolve_calls else None),
        "effective_top_k": (resolve_calls[-1]["built_top_k"] if resolve_calls else None),
        "effective_min_score": (
            resolve_calls[-1]["built_min_score"] if resolve_calls else None),
        "retriever_class": (resolve_calls[-1]["built_class"] if resolve_calls else None),
        "agent_calls": cap["agent_calls"],
        "spark_calls": spy.call_count,
        "signatures_preserved": cap["signatures_preserved"],
    }


# ============================================================
# 五、窗口重测 + 边界探针（纯检索，确定性）
# ============================================================
def _window_from_chunks(chunks: Sequence[Any],
                        target_sources: Sequence[str]) -> Dict[str, Any]:
    """按「目标来源 / 非目标来源」分组，取窗口端点。

    .. warning::
        **必须保留全精度**（不要 ``round``）。窗口端点是要拿去当
        ``min_score`` 用的**判定边界**，而检索是 ``score >= min_score``：
        把 ``0.4239817…`` 舍成 ``0.423982`` 会让它**大于**真实分数 ⇒ 目标被误杀。
        （本运行器第一版就踩了这个坑：边界探针全 FAIL。）
        展示用的 6 位小数值另存 ``display_6dp``。
    """
    targets = set(target_sources)
    target = [c.metadata["score"] for c in chunks if c.source in targets]
    other = [c.metadata["score"] for c in chunks if c.source not in targets]
    return {
        "target_count": len(target),
        "non_target_count": len(other),
        "target_floor": min(target) if target else None,
        "noise_ceiling": max(other) if other else None,
    }


def _pack_window(stats: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if stats["target_floor"] is None or stats["noise_ceiling"] is None:
        return None
    low, high = stats["noise_ceiling"], stats["target_floor"]
    return {
        # 精确值（判定边界，勿舍入）
        "noise_ceiling": low,
        "target_floor": high,
        "safe_window": [low, high],
        "width": high - low,
        "separated": high > low,
        "target_count": stats["target_count"],
        "non_target_count": stats["non_target_count"],
        # 仅用于人读
        "display_6dp": {
            "noise_ceiling": round(low, 6),
            "target_floor": round(high, 6),
            "safe_window": [round(low, 6), round(high, 6)],
            "width": round(high - low, 6),
        },
    }


async def _measure_window(env: Dict[str, Any], scenario: Dict[str, Any]) -> Dict[str, Any]:
    """全量候选扫描 → 窗口；同时给出 top_k 候选上的窗口做交叉核对。"""
    topic = scenario["retrieval"]["query_topic"]
    target_sources = scenario["retrieval"]["expected_hit_sources"]

    full = VectorKnowledgeRetriever(env["embedder"], env["store"],
                                    top_k=FULL_SCAN_TOP_K, min_score=None)
    full_chunks = await full.retrieve(scenario["job"], topic, env["context"])
    full_stats = _window_from_chunks(full_chunks, target_sources)

    topk = VectorKnowledgeRetriever(env["embedder"], env["store"],
                                    top_k=scenario["retrieval"]["top_k"],
                                    min_score=None)
    topk_chunks = await topk.retrieve(scenario["job"], topic, env["context"])
    topk_stats = _window_from_chunks(topk_chunks, target_sources)

    return {
        "topic": topic,
        "corpus_chunks": len(full_chunks),
        "over_all_candidates": _pack_window(full_stats),
        "over_topk_candidates": _pack_window(topk_stats),
        "all_candidates_detail": {
            "target_count": full_stats["target_count"],
            "non_target_count": full_stats["non_target_count"],
            "target_scores_desc": sorted(
                (round(c.metadata["score"], 6) for c in full_chunks
                 if c.source in set(target_sources)), reverse=True),
            "non_target_scores_top6": sorted(
                (round(c.metadata["score"], 6) for c in full_chunks
                 if c.source not in set(target_sources)), reverse=True)[:6],
        },
    }


async def _boundary_probe(env: Dict[str, Any], scenario: Dict[str, Any],
                          window: Dict[str, Any]) -> Dict[str, Any]:
    """证明窗口端点就是真实判定边界（``score >= min_score`` 含等号）。

    四个取样点：目标下限、目标下限 + 1 ULP、非目标上限、非目标上限 + 1 ULP。
    每个点分别数「目标来源 / 非目标来源」各留下几条——
    只看总数会把「丢掉一条目标」和「留下一条噪声」混为一谈。
    """
    topic = scenario["retrieval"]["query_topic"]
    target_sources = set(scenario["retrieval"]["expected_hit_sources"])
    floor = window["target_floor"]
    ceiling = window["noise_ceiling"]
    just_above_floor = math.nextafter(floor, math.inf)
    just_above_ceiling = math.nextafter(ceiling, math.inf)

    async def count_at(min_score: float) -> Dict[str, Any]:
        retriever = VectorKnowledgeRetriever(env["embedder"], env["store"],
                                             top_k=FULL_SCAN_TOP_K,
                                             min_score=min_score)
        chunks = await retriever.retrieve(scenario["job"], topic, env["context"])
        return {
            "min_score": min_score,
            "count": len(chunks),
            "target_count": sum(1 for c in chunks if c.source in target_sources),
            "non_target_count": sum(1 for c in chunks if c.source not in target_sources),
        }

    at_floor = await count_at(floor)
    above_floor = await count_at(just_above_floor)
    at_ceiling = await count_at(ceiling)
    above_ceiling = await count_at(just_above_ceiling)

    expected_targets = window["target_count"]
    checks = {
        # 目标下限含等号 ⇒ 下限那一分仍被保留；抬高一个 ULP 恰好少掉**那一条**目标
        "floor_is_inclusive": (
            at_floor["target_count"] == expected_targets
            and above_floor["target_count"] == expected_targets - 1),
        # 非目标上限含等号 ⇒ 上限那一分仍被保留；抬高一个 ULP 恰好少掉**那一条**噪声
        "ceiling_is_inclusive": (
            at_ceiling["non_target_count"] == 1
            and above_ceiling["non_target_count"] == 0),
        # 窗口内部：噪声一条不留、目标一条不少
        "interior_keeps_all_targets_drops_all_noise": (
            at_floor["non_target_count"] == 0
            and at_ceiling["target_count"] == expected_targets),
    }
    return {
        "semantics": "检索保留 score >= min_score（含等号）⇒ 窗口下界开、上界闭",
        "expected_target_count": expected_targets,
        "at_target_floor": at_floor,
        "just_above_target_floor": above_floor,
        "at_noise_ceiling": at_ceiling,
        "just_above_noise_ceiling": above_ceiling,
        "checks": checks,
    }


# ============================================================
# 六、标定：从窗口推出"可用的标定值"
# ============================================================
def _recommend(window: Dict[str, Any]) -> Dict[str, Any]:
    """窗口中点＝两侧余量对称，对语料微调的容忍度最大。"""
    low, high = window["noise_ceiling"], window["target_floor"]
    midpoint = (low + high) / 2
    recommended = round(midpoint, 3)
    width = high - low
    if width >= ROBUSTNESS_WIDE:
        tier = "wide"
    elif width >= ROBUSTNESS_NARROW:
        tier = "narrow"
    else:
        tier = "fragile"
    return {
        "recommended": recommended,
        "midpoint_exact": midpoint,
        "reason": "取窗口 (非目标上限, 目标下限] 的中点 ⇒ 两侧余量对称，"
                  "对语料微调与浮点误差的容忍度最大",
        "recommended_in_window": low < recommended <= high,
        "recommended_margin_to_floor": round(high - recommended, 6),
        "recommended_margin_to_ceiling": round(recommended - low, 6),
        "width": round(width, 6),
        "width_tier": tier,
        "usable_by_gap_threshold": width > USABLE_GAP_THRESHOLD,
        "gap_threshold": USABLE_GAP_THRESHOLD,
    }


# ============================================================
# 七、单场景全流程
# ============================================================
async def run_scenario(scenario: Dict[str, Any], no_rag_runs: Dict[str, Any],
                       rag_baseline_runs: Dict[str, Any],
                       analysis_scenarios: Dict[str, Any],
                       problems: List[str]) -> Dict[str, Any]:
    sid = scenario["id"]
    retrieval = scenario["retrieval"]
    declared_min_score = retrieval["min_score"]
    declared_top_k = retrieval["top_k"]
    expected_chunk_count = retrieval["expected_chunk_count"]

    print(f"\n{'─' * 74}\n[场景 {sid}] {scenario['label']}")

    async with _env(scenario) as env:
        print(f"  语料入库：22 篇 → {env['chunks_in_store']} 片")

        # ---- 1. 窗口重测 ----
        measured = await _measure_window(env, scenario)
        window = measured["over_all_candidates"]
        window_topk = measured["over_topk_candidates"]
        if window is None or window_topk is None:
            problems.append(f"[{sid}] 窗口无法测定（目标或非目标为空）")
            raise SystemExit(f"[{sid}] 窗口无法测定")
        w6 = window["display_6dp"]
        t6 = window_topk["display_6dp"]
        print(f"  窗口（全量 {measured['corpus_chunks']} 片）：({w6['noise_ceiling']}, "
              f"{w6['target_floor']}]  宽 {w6['width']}")
        print(f"  窗口（top_k 候选）：({t6['noise_ceiling']}, "
              f"{t6['target_floor']}]  宽 {t6['width']}")
        if (window["noise_ceiling"] != window_topk["noise_ceiling"]
                or window["target_floor"] != window_topk["target_floor"]):
            # 端点一致 ⇒ top_k 截断没有藏起「更高分的噪声」或「更低分的目标」，
            # 窗口是全语料意义上的真窗口，不是「前 5 条里的局部窗口」。
            problems.append(
                f"[{sid}] top_k 截断改变了窗口端点 ⇒ 窗口只是局部窗口："
                f"全量 ({w6['noise_ceiling']}, {w6['target_floor']}] vs "
                f"top_k ({t6['noise_ceiling']}, {t6['target_floor']}]")

        # ---- 2. 与任务 69 的窗口交叉核对（任务 69 存的是 6 位小数）----
        analysis = analysis_scenarios.get(sid)
        declared_window = analysis["min_score_window"]["safe_window"] if analysis else None
        if declared_window is None:
            problems.append(f"[{sid}] 缺任务 69 分析数据（rag_param_analysis.json）")
        elif w6["safe_window"] != declared_window:
            problems.append(
                f"[{sid}] 重测窗口 ≠ 任务 69 记录的窗口："
                f"{w6['safe_window']} vs {declared_window}")

        # ---- 3. 标定值 ----
        recommendation = _recommend(window)
        declared_in_window = window["noise_ceiling"] < declared_min_score <= window["target_floor"]
        if not declared_in_window:
            problems.append(
                f"[{sid}] 基准声明的 min_score={declared_min_score} 不在窗口 "
                f"({w6['noise_ceiling']}, {w6['target_floor']}] 内")
        if not recommendation["recommended_in_window"]:
            problems.append(
                f"[{sid}] 推荐标定值 {recommendation['recommended']} 不在窗口内")
        calibration = {
            "query_topic": retrieval["query_topic"],
            "target_sources": list(retrieval["expected_hit_sources"]),
            "expected_chunk_count": expected_chunk_count,
            "top_k": declared_top_k,
            "window": window,
            "declared_min_score": declared_min_score,
            "declared_in_window": declared_in_window,
            "declared_margin_to_floor": round(window["target_floor"] - declared_min_score, 6),
            "declared_margin_to_ceiling": round(declared_min_score - window["noise_ceiling"], 6),
            "recommendation": recommendation,
            "caveat": ("窗口窄 ⇒ 对语料微调敏感，换语料 / 换 Embedding 后必须重新标定"
                       if recommendation["width_tier"] != "wide" else
                       "窗口较宽，对语料微调不敏感；换 Embedding 后仍需重新标定"),
        }
        print(f"  声明 min_score={declared_min_score}（窗口内={declared_in_window}，"
              f"距下界 {calibration['declared_margin_to_ceiling']} / "
              f"距上界 {calibration['declared_margin_to_floor']}）")
        print(f"  推荐标定值={recommendation['recommended']}（窗口中点，"
              f"稳健度={recommendation['width_tier']}，"
              f"窗口内={recommendation['recommended_in_window']}）")

        # ---- 4. 四臂实跑 ----
        recommended_min_score = recommendation["recommended"]
        arm_kwargs = {
            ARM_OFF: None,
            ARM_ON_NO_THRESHOLD: None,
            ARM_ON_DECLARED: {"top_k": declared_top_k, "min_score": declared_min_score},
            ARM_ON_RECOMMENDED: {"top_k": declared_top_k,
                                 "min_score": recommended_min_score},
        }
        arms: Dict[str, Any] = {}
        for arm in ARMS:
            arms[arm] = await _run_arm(env, scenario, arm, arm_kwargs[arm])
            a = arms[arm]
            print(f"  [{arm:<20}] ok={a['ok']}  知识 {a['knowledge_context_count']:>2} 条  "
                  f"Prompt {a['prompt_chars']:>5} 字  模板={a['prompt_template']:<18} "
                  f"top_k={a['effective_top_k']} min_score={a['effective_min_score']}  "
                  f"LLM 调用 {a['spark_calls']}")

        # ---- 5. 边界探针 ----
        boundary = await _boundary_probe(env, scenario, window)
        bp = boundary
        print(f"  边界（score >= min_score 含等号）：")
        print(f"    min_score = 目标下限 {w6['target_floor']} → 目标 "
              f"{bp['at_target_floor']['target_count']} / 噪声 "
              f"{bp['at_target_floor']['non_target_count']}；"
              f"再抬一个 ULP → 目标 {bp['just_above_target_floor']['target_count']} / 噪声 "
              f"{bp['just_above_target_floor']['non_target_count']}")
        print(f"    min_score = 非目标上限 {w6['noise_ceiling']} → 目标 "
              f"{bp['at_noise_ceiling']['target_count']} / 噪声 "
              f"{bp['at_noise_ceiling']['non_target_count']}；"
              f"再抬一个 ULP → 目标 {bp['just_above_noise_ceiling']['target_count']} / 噪声 "
              f"{bp['just_above_noise_ceiling']['non_target_count']}")

        # ---- 6. 断言 ----
        off = arms[ARM_OFF]
        on0 = arms[ARM_ON_NO_THRESHOLD]
        on_declared = arms[ARM_ON_DECLARED]
        on_recommended = arms[ARM_ON_RECOMMENDED]

        no_rag_prompt = no_rag_runs.get(sid, {}).get("provenance", {}).get("prompt_chars")
        rag_base_prompt = rag_baseline_runs.get(sid, {}).get("provenance", {}).get("prompt_chars")
        rag_base_count = rag_baseline_runs.get(sid, {}).get("rag", {}).get("retrieved_count")

        checks = [
            ("★ 窗口端点与 top_k 截断无关（全量候选与 top_k 候选端点一致）",
             window["noise_ceiling"] == window_topk["noise_ceiling"]
             and window["target_floor"] == window_topk["target_floor"]),
            ("★ 窗口是「真分离」：非目标上限 < 目标下限",
             window["separated"]),
            ("★ 重测窗口 == 任务 69 记录的窗口（6 位小数）",
             declared_window is None or w6["safe_window"] == declared_window),
            ("★ 声明 min_score 落在窗口内",
             declared_in_window),
            ("★ 推荐标定值（窗口中点）落在窗口内",
             recommendation["recommended_in_window"]),
            ("无 RAG 臂出题成功且无 errors", off["ok"] and not off["errors"]),
            ("无 RAG 臂不注入知识（0 条）", off["knowledge_context_count"] == 0),
            ("无 RAG 臂用 question 模板", off["prompt_template"] == "question"),
            (f"★ 无 RAG 臂 Prompt 字符数 == 无 RAG 基准记录（{no_rag_prompt}）",
             off["prompt_chars"] == no_rag_prompt),
            ("有 RAG 臂（不传阈值）出题成功且无 errors", on0["ok"] and not on0["errors"]),
            ("★ 有 RAG 臂（不传阈值）条数 == top_k（阈值未生效的现状）",
             on0["knowledge_context_count"] == declared_top_k),
            ("★ 有 RAG 臂（不传阈值）Prompt 字符数 == 有 RAG 基准（真实星火）记录",
             on0["prompt_chars"] == rag_base_prompt),
            ("★ 有 RAG 臂（不传阈值）条数 == 有 RAG 基准（真实星火）记录",
             on0["knowledge_context_count"] == rag_base_count),
            ("★ 组装器实际收到的 retriever_kwargs == 传入值（有 RAG 臂·不传阈值）",
             on0["retriever_kwargs_received"] is None),
            ("★ 组装器实际收到的 retriever_kwargs == 传入值（有 RAG 臂·声明阈值）",
             on_declared["retriever_kwargs_received"] == arm_kwargs[ARM_ON_DECLARED]),
            ("★ 组装出的检索器 min_score == 声明值（阈值真的进了检索器）",
             on_declared["effective_min_score"] == declared_min_score),
            ("★ 组装出的检索器 top_k == 声明值",
             on_declared["effective_top_k"] == declared_top_k),
            (f"★★ 声明阈值臂条数 == 基准期望条数（{expected_chunk_count}）",
             on_declared["knowledge_context_count"] == expected_chunk_count),
            ("★ 声明阈值臂命中来源 == expected_hit_sources",
             on_declared["knowledge_context_sources"]
             == sorted(retrieval["expected_hit_sources"])),
            ("★ 声明阈值臂注入正文 == 检索器返回（同一份）",
             on_declared["knowledge_context_count"] == on_declared["retrieved_count"]),
            (f"★ 推荐标定值臂条数 == {expected_chunk_count}（窗口内部同样安全）",
             on_recommended["knowledge_context_count"] == expected_chunk_count),
            ("★ 推荐标定值臂 Prompt 字符数 == 声明阈值臂（只换阈值不改长度）",
             on_recommended["prompt_chars"] == on_declared["prompt_chars"]),
            ("★ 阈值生效后 Prompt 变短（噪声被剔除）",
             on_declared["prompt_chars"] < on0["prompt_chars"]),
            ("有 RAG 臂用 question_knowledge 模板",
             on_declared["prompt_template"] == "question_knowledge"),
            ("每臂只调用一次大模型（一题一次 LLM 调用）",
             all(arms[a]["spark_calls"] == 1 for a in ARMS)),
            ("每臂只调用一次 Agent",
             all(arms[a]["agent_calls"] == 1 for a in ARMS)),
            ("替身与被替换函数同签名", all(
                all(arms[a]["signatures_preserved"].values()) for a in ARMS)),
            ("★ 边界：目标下限含等号（该分仍保留目标），抬高一个 ULP 恰好少一条目标",
             boundary["checks"]["floor_is_inclusive"]),
            ("★ 边界：非目标上限含等号（该分仍保留噪声），抬高一个 ULP 噪声归零",
             boundary["checks"]["ceiling_is_inclusive"]),
            ("★ 边界：窗口内部噪声全清、目标全留",
             boundary["checks"]["interior_keeps_all_targets_drops_all_noise"]),
        ]
        for name, ok in checks:
            if not ok:
                problems.append(f"[{sid}] {name}")
            print(f"    {'PASS' if ok else 'FAIL'}  {name}")

        # ---- 7. 与任务 69 的投影闭环 ----
        projected = None
        if analysis:
            projected = analysis["prompt_length"]["projected_at_declared"]
        cross_checks: List[Dict[str, Any]] = []
        if projected is not None:
            cross_checks = [
                {"name": "任务 69 投影的条数 == 实跑条数",
                 "expected": projected["chunks"], "actual": on_declared["knowledge_context_count"]},
                {"name": "任务 69 投影的 Prompt 字符数 == 实跑 Prompt 字符数",
                 "expected": projected["prompt_chars"], "actual": on_declared["prompt_chars"]},
                {"name": "任务 69 记录的 on_chars == 实跑（不传阈值）Prompt 字符数",
                 "expected": analysis["prompt_length"]["on_chars"], "actual": on0["prompt_chars"]},
                {"name": "任务 69 记录的 off_chars == 实跑无 RAG Prompt 字符数",
                 "expected": analysis["prompt_length"]["off_chars"], "actual": off["prompt_chars"]},
            ]
            for cc in cross_checks:
                cc["match"] = cc["expected"] == cc["actual"]
                if not cc["match"]:
                    problems.append(f"[{sid}] 闭环核对失败：{cc['name']} "
                                    f"{cc['expected']} vs {cc['actual']}")
                print(f"    {'PASS' if cc['match'] else 'FAIL'}  [闭环] {cc['name']}"
                      f"（{cc['expected']} vs {cc['actual']}）")

    return {
        "scenario_id": sid,
        "label": scenario["label"],
        "calibration": calibration,
        "measured_window": measured,
        "boundary_probe": boundary,
        "arms": arms,
        "checks": [{"name": n, "passed": ok} for n, ok in checks],
        "all_checks_passed": all(ok for _, ok in checks),
        "cross_checks_with_task69": cross_checks,
    }


# ============================================================
# 八、主流程
# ============================================================
def _load(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"缺少输入文件：{path}")
    return json.loads(path.read_text(encoding="utf-8"))


async def main() -> int:
    parser = argparse.ArgumentParser(description="R2 · min_score 阈值标定 + 实跑验证")
    parser.add_argument("--scenario", action="append", default=None,
                        help="只跑指定场景 id（可重复）；缺省跑全部")
    parser.add_argument("--out", default=str(RESULT_PATH))
    args = parser.parse_args()

    benchmark = _load(BENCHMARK_PATH)
    no_rag_runs = {r["scenario_id"]: r for r in _load(NO_RAG_PATH)["runs"]}
    rag_baseline_runs = {r["scenario_id"]: r for r in _load(RAG_BASELINE_PATH)["runs"]}
    analysis = _load(ANALYSIS_PATH)
    analysis_scenarios = {s["scenario_id"]: s for s in analysis["scenarios"]}

    scenarios = benchmark["scenarios"]
    if args.scenario:
        wanted = set(args.scenario)
        scenarios = [s for s in scenarios if s["id"] in wanted]
        missing = wanted - {s["id"] for s in scenarios}
        if missing:
            raise SystemExit(f"未知场景 id：{sorted(missing)}")

    print("=" * 74)
    print("R2 · min_score 阈值标定 + 实跑验证")
    print("=" * 74)
    print(f"  条件来源：{BENCHMARK_PATH.relative_to(PROJECT_DIR)}")
    print(f"  对照投影：{ANALYSIS_PATH.relative_to(PROJECT_DIR)}（任务 69）")
    print(f"  对照实测：{RAG_BASELINE_PATH.relative_to(PROJECT_DIR)}（真实星火）")
    print("  透传路径：use_rag=True + retriever_kwargs（任务 70 · R1）")
    print("  大模型：Mock（只测检索 + Prompt，与模型输出无关）")
    print(f"  场景数：{len(scenarios)}")

    problems: List[str] = []
    results: List[Dict[str, Any]] = []
    for scenario in scenarios:
        results.append(await run_scenario(
            scenario, no_rag_runs, rag_baseline_runs, analysis_scenarios, problems))

    # ---- 汇总 ----
    def _sum(key: str) -> int:
        return sum(r["arms"][key]["knowledge_context_count"] for r in results)

    def _chars(key: str) -> int:
        return sum(r["arms"][key]["prompt_chars"] for r in results)

    before_chunks = _sum(ARM_ON_NO_THRESHOLD)
    after_chunks = _sum(ARM_ON_DECLARED)
    before_chars = _chars(ARM_ON_NO_THRESHOLD)
    after_chars = _chars(ARM_ON_DECLARED)

    aggregate = {
        "scenarios": len(results),
        "chunks_injected_before": before_chunks,
        "chunks_injected_after": after_chunks,
        "chunks_removed": before_chunks - after_chunks,
        "chunks_expected_after": sum(
            r["calibration"]["expected_chunk_count"] for r in results),
        "prompt_chars_off": _chars(ARM_OFF),
        "prompt_chars_before": before_chars,
        "prompt_chars_after": after_chars,
        "prompt_chars_saved": before_chars - after_chars,
        "prompt_chars_saved_ratio": (round((before_chars - after_chars) / before_chars, 4)
                                     if before_chars else None),
        "prompt_chars_growth_ratio": (round((before_chars - _chars(ARM_OFF))
                                            / _chars(ARM_OFF), 4)
                                      if _chars(ARM_OFF) else None),
        "declared_min_scores": {r["scenario_id"]: r["calibration"]["declared_min_score"]
                                for r in results},
        "recommended_min_scores": {
            r["scenario_id"]: r["calibration"]["recommendation"]["recommended"]
            for r in results},
    }

    # 汇总层的验收判据（用户点名的那两个数）
    aggregate_checks = [
        {"name": "注入条数 15 → 5", "expected_before": 15, "expected_after": 5,
         "actual_before": before_chunks, "actual_after": after_chunks},
        {"name": "Prompt 13810 → 9852 字", "expected_before": 13810,
         "expected_after": 9852, "actual_before": before_chars, "actual_after": after_chars},
    ]
    for check in aggregate_checks:
        check["match"] = (check["actual_before"] == check["expected_before"]
                          and check["actual_after"] == check["expected_after"])
        if not check["match"]:
            problems.append(f"[汇总] {check['name']} 不成立："
                            f"{check['actual_before']} → {check['actual_after']}")

    payload = {
        "name": "rag-min-score-calibration",
        "version": 1,
        "purpose": "把任务 69 实测的 min_score 安全窗口落地为可用的标定值，"
                   "并用任务 70（R1）的 retriever_kwargs 透传路径实跑验证："
                   "注入条数 15 → 5、Prompt 13810 → 9852 字。"
                   "只做标定与验证，不修改生产代码、不改基准文件、不评价生成质量。",
        "scope": {
            "modifies_code": False,
            "modifies_benchmark": False,
            "evaluates_quality": False,
            "llm": "Mock（SpySpark + 场景 mock_reply）",
            "why_mock": "要测的量（注入条数 / Prompt 字符数）由检索 + 模板渲染决定，"
                        "与模型输出无关；真实星火输出不确定。已用真实星火跑出的 "
                        "rag_baseline.json 逐场景交叉核对 Prompt 字符数。",
        },
        "sources": {
            "benchmark": str(BENCHMARK_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
            "corpus": str(CORPUS_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
            "no_rag_baseline": str(NO_RAG_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
            "rag_baseline": str(RAG_BASELINE_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
            "task69_analysis": str(ANALYSIS_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
        },
        "calibration_basis": {
            "window_rule": "窗口 = (非目标来源最高分, 目标来源最低分]；"
                           "min_score 取值须落在此区间内才既能保住目标、又剔除噪声",
            "window_measured_over": f"全量候选（top_k={FULL_SCAN_TOP_K} / min_score=None）",
            "inclusivity": "检索保留 score >= min_score ⇒ 下界开、上界闭",
            "recommended_value_rule": "取窗口中点（两侧余量对称，容忍度最大）",
            "robustness_tiers": {"wide": f">= {ROBUSTNESS_WIDE}",
                                 "narrow": f">= {ROBUSTNESS_NARROW}",
                                 "fragile": f"< {ROBUSTNESS_NARROW}"},
            "usable_gap_threshold": USABLE_GAP_THRESHOLD,
            "embedder": "default_embedder()（hash-local，词面哈希 ⇒ 分数只在同模型内可比）",
            "corpus_chunks": results[0]["measured_window"]["corpus_chunks"] if results else None,
        },
        "passthrough_path": {
            "entry": "interview_core.generate_next_question(db, session_id, "
                     "retriever=None, use_rag=True, retriever_kwargs={...})",
            "assembly": "resolve_retriever(db, None, True, retriever_kwargs=...) "
                        "-> knowledge_rag.build_vector_retriever(db, **kwargs)",
            "added_by": "任务 70 · R1",
            "note": "retriever_kwargs 只在 use_rag=True 的组装分支生效；"
                    "显式注入 retriever 时不被读取",
        },
        "scenarios": results,
        "aggregate": aggregate,
        "aggregate_checks": aggregate_checks,
        "integrity": {
            "checks_passed": not problems,
            "problems": problems,
        },
    }

    out_path = Path(args.out)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                        encoding="utf-8")

    print("\n" + "=" * 74)
    print("结果")
    print("=" * 74)
    print(f"  注入条数：{before_chunks} → {after_chunks}"
          f"（去掉 {before_chunks - after_chunks} 条低相关）")
    print(f"  Prompt ：{before_chars} → {after_chars} 字"
          f"（省 {before_chars - after_chars} 字，"
          f"{aggregate['prompt_chars_saved_ratio']:.1%}）")
    print(f"  无 RAG 对照：{_chars(ARM_OFF)} 字")
    print(f"  标定值：{aggregate['declared_min_scores']}")
    print(f"  推荐值：{aggregate['recommended_min_scores']}")
    for check in aggregate_checks:
        print(f"  {'PASS' if check['match'] else 'FAIL'}  {check['name']}"
              f"（实跑 {check['actual_before']} → {check['actual_after']}）")
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
