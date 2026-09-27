# -*- coding: utf-8 -*-
"""RAG 检索默认参数 · 配置注入 · **实跑对比运行器**（非套件）。

对应任务：**让生产调用链真正使用 ``min_score`` 配置**。

为什么需要这个运行器
--------------------
``test_rag_retriever_config.py`` 已经用**哨兵 db** 证明了「配置注入确实生效」，
但它只断言「检索器上的 ``min_score`` 属性等于多少」——**没有实跑检索、没有出题**。
本运行器补上这一段：在**真实语料 + 真实向量库 + 真实出题流程**上，把
「不配 / 配 0.25 / 配标定值」三档跑出可比数字。

本运行器做什么
--------------
**Part A · 检索级（16 条已有测试 query × 4 档）**

用 ``backend/scripts/rag_query_set.json`` 自带的 16 条 query 与 16 篇受控语料，
**只通过环境变量**驱动组装器（不传任何 ``retriever_kwargs``），逐条记录：

- 返回 chunk 数量
- score 列表（全精度）
- top-1 的分类是否正确、是否含全部期望关键词
- 该条 query 的知识注入让 Prompt 长了多少字（固定 variables 基线 ⇒ 差值只反映检索）

**Part B · 端到端（3 个基准场景 × 4 档）**

走真实的 ``interview_core.generate_next_question``（只把大模型换成 Mock），
**只通过环境变量**驱动组装器，逐场景记录：

- ``knowledge_context`` 条数 / score 列表 / 来源
- **Prompt 长度**（字符数，以及知识小节字符数）
- **Agent 调用结果**（调用次数、``ok``、产出的题目文本与字数、warnings）

四档分别是：

============================  ==========================================
档                            环境变量
============================  ==========================================
``rag_off``                   ``use_rag=False``（对照：完全不接 RAG）
``rag_on_env_unset``          不设 ``RAG_*`` ⇒ 生产默认（``top_k=5`` / ``None``）
``rag_on_env_min_score_0.25`` ``RAG_MIN_SCORE=0.25``
``rag_on_env_declared``       ``RAG_MIN_SCORE=<该场景基准声明的阈值>``
============================  ==========================================

**为什么 Part B 也要跑 ``declared`` 档**：``0.25`` 在 s1/s2 上恰好，在 s3 上却
**低于窗口下界**（s3 窗口 ``(0.360771, 0.423982]``）⇒ 会把噪声放进来。
这正是「``min_score`` 与语料/场景绑定、不能照抄数字」的实测证据。

闭环核对（为什么可信）
----------------------
Part B 的每一档都要与 **任务 70 · R2** 已实测的**显式 ``retriever_kwargs`` 路径**
（``scripts/rag_min_score_calibration.json``）逐场景对齐：

- ``rag_on_env_unset`` ↔ R2 ``rag_on_no_threshold``
- ``rag_on_env_declared`` ↔ R2 ``rag_on_declared``
- ``rag_off`` ↔ R2 ``rag_off``

两条路径（**环境变量注入** vs **显式传参透传**）结果必须**逐字节相同**——
若相同，就证明「配置注入」没有引入任何额外行为差异。

为什么用 Mock 而不是真实星火
----------------------------
本运行器要测的量（条数、分数、Prompt 字符数）完全由**检索 + 模板渲染**决定，
**与模型输出无关**；真实星火每次输出不同。R2 已用「Mock 字符数 == 真实星火
``rag_baseline.json`` 字符数」证明这条等价性。

为什么这个文件在 ``tests/`` 而不是 ``scripts/``
-----------------------------------------------
项目硬约束：**SQLite 只允许出现在 ``backend/tests/*.py`` 的内存库**。
本运行器要走 ``generate_next_question``（读会话行 / 上下文）⇒ 必须有库 ⇒ 只能放这里。
**不带 ``test_`` 前缀，不是回归套件，不进回归循环。**

运行
----
``python backend/tests/rag_config_injection_run.py``

结果写入 ``backend/scripts/rag_config_injection.json``。
**本运行器不修改任何生产代码、不改基准文件、不改默认行为。**
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import sys
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
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
from services.knowledge_rag import (  # noqa: E402
    ENV_RAG_MIN_SCORE,
    ENV_RAG_TOP_K,
    build_vector_retriever,
    default_embedder,
)
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

# ============================================================
# 一、常量
# ============================================================
QUERY_SET_PATH = BACKEND_DIR / "scripts" / "rag_query_set.json"
BENCHMARK_PATH = BACKEND_DIR / "scripts" / "interview_eval_benchmark.json"
CORPUS_PATH = BACKEND_DIR / "scripts" / "interview_knowledge.json"
R2_PATH = BACKEND_DIR / "scripts" / "rag_min_score_calibration.json"
RAG_BASELINE_PATH = BACKEND_DIR / "scripts" / "rag_baseline.json"
RESULT_PATH = BACKEND_DIR / "scripts" / "rag_config_injection.json"

#: Part A 的 Prompt 增量基线所用的场景（**只借它的 variables**，与 Part A 的语料无关）。
PROMPT_BASELINE_SCENARIO = "s1-mysql-index-mid"

#: 用户明确要求对比的那一档。
COMPARE_MIN_SCORE = 0.25

#: 知识小节标题（与 ``question_knowledge.txt`` 一致）。
KNOWLEDGE_HEADING = "## 四·五、参考知识（外部检索结果，可选）"

# --- Part A：检索级四档（全部只靠环境变量驱动）---
A_ENV_UNSET = "env_unset"
A_ENV_0_25 = "env_min_score_0.25"
A_ENV_CALIBRATED = "env_min_score_calibrated"
A_ENV_QUERY_DECLARED = "env_query_declared"
A_ARMS = (A_ENV_UNSET, A_ENV_0_25, A_ENV_CALIBRATED, A_ENV_QUERY_DECLARED)
A_ARM_LABELS = {
    A_ENV_UNSET: "不配 RAG_*（生产默认：top_k=5 / min_score=None）",
    A_ENV_0_25: "RAG_MIN_SCORE=0.25（用户指定的对比档）",
    A_ENV_CALIBRATED: "RAG_MIN_SCORE=<该 query 的标定阈值>",
    A_ENV_QUERY_DECLARED: "RAG_TOP_K=<该 query 声明> + RAG_MIN_SCORE=<该 query 标定>",
}

# --- Part B：端到端四档 ---
B_OFF = "rag_off"
B_ENV_UNSET = "rag_on_env_unset"
B_ENV_0_25 = "rag_on_env_min_score_0.25"
B_ENV_DECLARED = "rag_on_env_declared"
B_ARMS = (B_OFF, B_ENV_UNSET, B_ENV_0_25, B_ENV_DECLARED)
B_ARM_LABELS = {
    B_OFF: "use_rag=False（对照：完全不接 RAG）",
    B_ENV_UNSET: "use_rag=True，不设 RAG_*（生产默认）",
    B_ENV_0_25: "use_rag=True，RAG_MIN_SCORE=0.25",
    B_ENV_DECLARED: "use_rag=True，RAG_MIN_SCORE=<场景声明阈值>",
}

#: 与 R2 产出对齐的臂名（闭环核对用）。
R2_ARM_OF = {
    B_OFF: "rag_off",
    B_ENV_UNSET: "rag_on_no_threshold",
    B_ENV_DECLARED: "rag_on_declared",
}


# ============================================================
# 二、工具
# ============================================================
@contextmanager
def _envvars(**pairs: Optional[str]) -> Iterator[None]:
    """临时设置环境变量，退出时**精确恢复**（``None`` 表示删除）。"""
    saved: Dict[str, Optional[str]] = {k: os.environ.get(k) for k in pairs}
    for key, value in pairs.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    try:
        yield
    finally:
        for key, old in saved.items():
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old


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


def _load_queries() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    data = json.loads(QUERY_SET_PATH.read_text(encoding="utf-8"))
    return data["queries"], data["corpus"]["documents"]


def _load_benchmark() -> List[Dict[str, Any]]:
    return json.loads(BENCHMARK_PATH.read_text(encoding="utf-8"))["scenarios"]


def _load_bench_corpus() -> List[Dict[str, Any]]:
    return json.loads(CORPUS_PATH.read_text(encoding="utf-8"))["documents"]


# ============================================================
# 三、建库环境
# ============================================================
@asynccontextmanager
async def _query_env(documents: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    """Part A 的环境：只建库 + 导入 ``rag_query_set.json`` 的受控语料。"""
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

        embedder = default_embedder()
        store = SqlAlchemyVectorStore(session)
        pipeline = KnowledgeImportPipeline(session, embedder=embedder, store=store)
        for doc in documents:
            await pipeline.import_document(dict(doc))

        yield {"db": session, "store": store, "embedder": embedder,
               "chunks_in_store": await store.count()}
    finally:
        if session is not None:
            await session.close()
        await engine.dispose()


@asynccontextmanager
async def _bench_env(scenario: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """Part B 的环境：与基准测试同一套建库手法（只 Mock 大模型）。"""
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

        session.add(User(username="cfg_inject", email="cfg_inject@example.com",
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
        for doc in _load_bench_corpus():
            await pipeline.import_document(dict(doc))

        yield {"db": session, "row": row, "session_id": session_id, "job": job,
               "resume": resume, "plan": plan, "context": context,
               "embedder": embedder, "store": store,
               "chunks_in_store": await store.count()}
    finally:
        if session is not None:
            await session.close()
        await engine.dispose()


# ============================================================
# 四、捕获「组装器实收 / 检索返回 / 最终 Prompt」
# ============================================================
@asynccontextmanager
async def _capture() -> Iterator[Dict[str, Any]]:
    """包装三处模块属性，记录**真实流程**里的实际取值。"""
    captured: Dict[str, Any] = {"resolve_calls": [], "retrieve_calls": [],
                                "agent_calls": 0}
    original_resolve = interview_core.resolve_retriever
    original_retrieve = interview_core.retrieve_knowledge
    original_generate = interview_agent.generate_question

    def resolve_wrapper(db: Any, retriever: Any = None, use_rag: bool = False, *,
                        retriever_kwargs: Any = None) -> Any:
        built, warnings = original_resolve(db, retriever, use_rag,
                                           retriever_kwargs=retriever_kwargs)
        captured["resolve_calls"].append({
            "use_rag": use_rag,
            "retriever_kwargs_received": retriever_kwargs,
            "built_class": type(built).__name__ if built is not None else None,
            "built_top_k": getattr(built, "top_k", None),
            "built_min_score": getattr(built, "min_score", None),
            "warnings": list(warnings),
        })
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
    signatures_preserved = {
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
    for name, ok in signatures_preserved.items():
        if not ok:
            raise AssertionError(f"{name} 替身签名与被替换函数不一致")
    captured["signatures_preserved"] = signatures_preserved

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
# 五、Part A · 检索级（16 条已有 query × 4 档，只靠环境变量）
# ============================================================
def _arm_env_a(arm: str, query: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Part A 每一档对应的环境变量集合（**始终把两个变量都写全**，避免继承残留）。"""
    if arm == A_ENV_UNSET:
        return {ENV_RAG_TOP_K: None, ENV_RAG_MIN_SCORE: None}
    if arm == A_ENV_0_25:
        return {ENV_RAG_TOP_K: None, ENV_RAG_MIN_SCORE: str(COMPARE_MIN_SCORE)}
    if arm == A_ENV_CALIBRATED:
        return {ENV_RAG_TOP_K: None, ENV_RAG_MIN_SCORE: str(query["min_score"])}
    if arm == A_ENV_QUERY_DECLARED:
        return {ENV_RAG_TOP_K: str(query["top_k"]),
                ENV_RAG_MIN_SCORE: str(query["min_score"])}
    raise ValueError(f"未知的 Part A 档位：{arm}")


async def run_part_a(variables: Dict[str, Any], base_prompt: str) -> Dict[str, Any]:
    """逐 query × 逐档实跑检索，返回可直接落盘的报告片段。"""
    queries, documents = _load_queries()
    rows: List[Dict[str, Any]] = []

    async with _query_env(documents) as env:
        db = env["db"]
        for query in queries:
            for arm in A_ARMS:
                with _envvars(**_arm_env_a(arm, query)):
                    retriever = build_vector_retriever(db)
                    chunks = await retriever.retrieve(None, query["query"], None)
                    effective_top_k = retriever.top_k
                    effective_min_score = retriever.min_score

                scores = [c.metadata.get("score") for c in chunks]
                sources = [c.source for c in chunks]
                top1 = chunks[0] if chunks else None
                top1_content = getattr(top1, "content", "") or ""
                keywords = list(query["expected_keywords"])

                # Prompt 增量：固定 variables 基线，只有 knowledge_context 在变。
                _tmpl, with_knowledge = interview_agent.render_question_prompt(
                    variables, chunks)
                rows.append({
                    "query": query["query"],
                    "arm": arm,
                    "expected_category": query["expected_category"],
                    "expected_keywords": keywords,
                    "query_declared_top_k": query["top_k"],
                    "query_declared_min_score": query["min_score"],
                    "effective_top_k": effective_top_k,
                    "effective_min_score": effective_min_score,
                    "chunk_count": len(chunks),
                    "scores": scores,
                    "sources": sources,
                    "top1_category": getattr(top1, "metadata", {}).get("category")
                    if top1 is not None else None,
                    "top1_category_ok": bool(
                        top1 is not None
                        and getattr(top1, "metadata", {}).get("category")
                        == query["expected_category"]),
                    "top1_keywords_ok": bool(
                        top1 is not None
                        and all(kw in top1_content for kw in keywords)),
                    "prompt_template": _tmpl,
                    "prompt_chars": len(with_knowledge),
                    "prompt_delta_chars": len(with_knowledge) - len(base_prompt),
                })

        summary = _summarize_part_a(rows, queries)
        return {
            "corpus": {"documents": len(documents),
                       "chunks_in_store": env["chunks_in_store"]},
            "arms": A_ARM_LABELS,
            "rows": rows,
            "summary": summary,
            "findings": _findings_part_a(summary, queries),
            "prompt_baseline": {
                "scenario_id": PROMPT_BASELINE_SCENARIO,
                "note": "Part A 的 Prompt 增量以该场景的 variables 为固定基线，"
                        "只有 knowledge_context 随 query/档位变化，因此差值只反映检索。",
                "base_prompt_chars": len(base_prompt),
            },
        }


def _findings_part_a(summary: Dict[str, Any],
                     queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Part A 的**观察结论**（不参与 integrity 判定）。"""
    total = len(queries)
    unset = summary["per_arm"][A_ENV_UNSET]
    zero = summary["per_arm"][A_ENV_0_25]
    cal = summary["per_arm"][A_ENV_CALIBRATED]
    return [
        {
            "name": "RAG_MIN_SCORE=0.25 在这 16 篇语料上是否过度过滤",
            "over_filtered": zero["queries_with_zero_chunks"] > 0,
            "detail": f"0.25 ⇒ {zero['queries_with_zero_chunks']}/{total} 条 query 零命中；"
                      f"top-1 分类正确率 {zero['top1_category_ok']}/{total}"
                      f"（默认档 {unset['top1_category_ok']}/{total}）",
        },
        {
            "name": "按该 query 标定阈值是否保持检索质量",
            "quality_preserved": (cal["top1_category_ok"] == unset["top1_category_ok"]
                                  and cal["queries_with_zero_chunks"] == 0),
            "detail": f"标定档 top-1 分类正确 {cal['top1_category_ok']}/{total}、"
                      f"零命中 {cal['queries_with_zero_chunks']}；"
                      f"默认档 {unset['top1_category_ok']}/{total}",
        },
        {
            "name": "阈值是否真的缩短了注入的 Prompt",
            "prompt_shorter": (cal["total_prompt_delta_chars"]
                               < unset["total_prompt_delta_chars"]),
            "detail": f"标定档增量 {cal['total_prompt_delta_chars']:+d} 字 vs "
                      f"默认档 {unset['total_prompt_delta_chars']:+d} 字",
        },
    ]


def _summarize_part_a(rows: List[Dict[str, Any]],
                      queries: List[Dict[str, Any]]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {"per_arm": {}, "queries": len(queries)}
    for arm in A_ARMS:
        arm_rows = [r for r in rows if r["arm"] == arm]
        summary["per_arm"][arm] = {
            "label": A_ARM_LABELS[arm],
            "total_chunks": sum(r["chunk_count"] for r in arm_rows),
            "mean_chunks": round(sum(r["chunk_count"] for r in arm_rows)
                                 / len(arm_rows), 3),
            "queries_with_zero_chunks": sum(1 for r in arm_rows
                                            if r["chunk_count"] == 0),
            "top1_category_ok": sum(1 for r in arm_rows if r["top1_category_ok"]),
            "top1_keywords_ok": sum(1 for r in arm_rows if r["top1_keywords_ok"]),
            "total_prompt_delta_chars": sum(r["prompt_delta_chars"] for r in arm_rows),
            "effective_top_k_values": sorted({r["effective_top_k"] for r in arm_rows}),
            "effective_min_score_values": sorted(
                {r["effective_min_score"] for r in arm_rows},
                key=lambda v: (v is None, v)),
        }
    return summary


# ============================================================
# 六、Part B · 端到端（3 场景 × 4 档，只靠环境变量）
# ============================================================
def _arm_env_b(arm: str, scenario: Dict[str, Any]) -> Dict[str, Optional[str]]:
    declared = scenario["retrieval"]["min_score"]
    if arm == B_OFF:
        return {ENV_RAG_TOP_K: None, ENV_RAG_MIN_SCORE: None}
    if arm == B_ENV_UNSET:
        return {ENV_RAG_TOP_K: None, ENV_RAG_MIN_SCORE: None}
    if arm == B_ENV_0_25:
        return {ENV_RAG_TOP_K: None, ENV_RAG_MIN_SCORE: str(COMPARE_MIN_SCORE)}
    if arm == B_ENV_DECLARED:
        return {ENV_RAG_TOP_K: None, ENV_RAG_MIN_SCORE: str(declared)}
    raise ValueError(f"未知的 Part B 档位：{arm}")


async def _run_bench_arm(env: Dict[str, Any], scenario: Dict[str, Any],
                         arm: str) -> Dict[str, Any]:
    use_rag = arm != B_OFF
    spy = SpySpark(_mock_reply_text(scenario))
    with _envvars(**_arm_env_b(arm, scenario)):
        async with _capture() as captured:
            # ★ 刻意**不传** retriever_kwargs：本次要验证的就是「环境变量这条路」。
            result = await interview_core.generate_next_question(
                env["db"], env["session_id"],
                context=env["context"], plan=env["plan"],
                use_rag=use_rag, spark=spy,
            )

    resolve_call = captured["resolve_calls"][-1] if captured["resolve_calls"] else {}
    retrieve_call = (captured["retrieve_calls"][-1]
                     if captured["retrieve_calls"] else {})
    prompt = captured.get("prompt", "")
    question = str(result.get("question") or "")
    return {
        "arm": arm,
        "label": B_ARM_LABELS[arm],
        "use_rag": use_rag,
        "retriever_kwargs_passed": None,
        "env": {k: v for k, v in _arm_env_b(arm, scenario).items() if v is not None},
        "ok": bool(result.get("ok")),
        "question_no": result.get("question_no"),
        "warnings": list(result.get("warnings") or []),
        "error": result.get("error"),
        # --- 检索侧 ---
        "retriever_class": resolve_call.get("built_class"),
        "effective_top_k": resolve_call.get("built_top_k"),
        "effective_min_score": resolve_call.get("built_min_score"),
        "query": retrieve_call.get("topic"),
        "chunk_count": retrieve_call.get("returned_count"),
        "scores": retrieve_call.get("returned_scores"),
        "sources": retrieve_call.get("returned_sources"),
        # --- Prompt 侧 ---
        "prompt_template": captured.get("prompt_template"),
        "prompt_chars": len(prompt),
        "prompt_has_knowledge_block": KNOWLEDGE_HEADING in prompt,
        # --- Agent 侧 ---
        "agent_calls": captured["agent_calls"],
        "spark_calls": spy.call_count,
        "question": question,
        "question_chars": len(question),
    }


async def run_part_b() -> Dict[str, Any]:
    scenarios_out: List[Dict[str, Any]] = []
    for scenario in _load_benchmark():
        async with _bench_env(scenario) as env:
            retrieval = scenario["retrieval"]
            arms = {arm: await _run_bench_arm(env, scenario, arm)
                    for arm in B_ARMS}
        expected = int(retrieval["expected_chunk_count"])
        checks, findings = _check_scenario(scenario, arms, expected)
        scenarios_out.append({
            "scenario_id": scenario["id"],
            "label": scenario["label"],
            "declared_retrieval": retrieval,
            "chunks_in_store": env["chunks_in_store"],
            "arms": arms,
            "checks": checks,
            "findings": findings,
            "all_checks_passed": all(c["ok"] for c in checks),
        })
    return {"arms": B_ARM_LABELS, "scenarios": scenarios_out,
            "aggregate": _aggregate_part_b(scenarios_out)}


def _check_scenario(scenario: Dict[str, Any], arms: Dict[str, Dict[str, Any]],
                    expected: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """返回 ``(checks, findings)``。

    **checks 与 findings 必须分开**（项目约定）：
    ``checks`` 是**必须成立**的硬不变量（不成立＝本次改动有问题）；
    ``findings`` 是**观察结论**（可能「不成立」但完全符合预期，例如
    「``0.25`` 在 s3 上低于窗口下界 ⇒ 会放进噪声」——那正是要展示的现象）。
    把 findings 混进 checks 会让 ``integrity`` 报出**假失败**。
    """
    declared = float(scenario["retrieval"]["min_score"])
    checks: List[Dict[str, Any]] = []

    def _add(name: str, ok: bool, detail: str = "") -> None:
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    off, unset = arms[B_OFF], arms[B_ENV_UNSET]
    zero25, decl = arms[B_ENV_0_25], arms[B_ENV_DECLARED]

    _add("rag_off：不注入知识、无参考知识小节",
         off["chunk_count"] == 0 and off["prompt_has_knowledge_block"] is False
         and off["prompt_template"] == "question",
         f"n={off['chunk_count']} tmpl={off['prompt_template']}")
    _add("rag_on_env_unset：走默认口径（top_k=5 / min_score=None）",
         unset["effective_top_k"] == 5 and unset["effective_min_score"] is None
         and unset["chunk_count"] == 5,
         f"top_k={unset['effective_top_k']} min_score={unset['effective_min_score']} "
         f"n={unset['chunk_count']}")
    _add("★ 环境变量真的被组装器读到了（env_min_score_0.25 ⇒ 检索器 min_score=0.25）",
         zero25["effective_min_score"] == COMPARE_MIN_SCORE,
         str(zero25["effective_min_score"]))
    _add("★ RAG_MIN_SCORE=<场景声明阈值> ⇒ 注入条数 == expected_chunk_count",
         decl["chunk_count"] == expected,
         f"n={decl['chunk_count']} expected={expected}")
    _add("  └ 且声明档的 Prompt 比默认档短（阈值真的省了 Prompt）",
         decl["prompt_chars"] < unset["prompt_chars"],
         f"{decl['prompt_chars']} < {unset['prompt_chars']}")
    _add("每一档都只调用一次 Agent / 一次大模型",
         all(a["agent_calls"] == 1 and a["spark_calls"] == 1 for a in arms.values()),
         str({k: (v["agent_calls"], v["spark_calls"]) for k, v in arms.items()}))
    _add("每一档出题都成功（ok=True）且无 warning",
         all(a["ok"] and a["warnings"] == [] for a in arms.values()),
         str({k: (v["ok"], v["warnings"]) for k, v in arms.items()}))

    # --- findings（观察结论，**不参与** integrity 判定）---
    findings = [
        {
            "name": "0.25 档与「该场景声明阈值」档是否等价",
            "equivalent": zero25["chunk_count"] == decl["chunk_count"],
            "detail": f"RAG_MIN_SCORE=0.25 -> {zero25['chunk_count']} 条；"
                      f"声明阈值 {declared} -> {decl['chunk_count']} 条"
                      f"（期望 {expected} 条）",
        },
        {
            "name": "0.25 档是否把噪声放进了 knowledge_context",
            "noise_injected": zero25["chunk_count"] > expected,
            "detail": f"条数 {zero25['chunk_count']} vs 期望 {expected}；"
                      f"Prompt {zero25['prompt_chars']} 字 vs 默认档 "
                      f"{unset['prompt_chars']} 字",
        },
    ]
    return checks, findings


def _aggregate_part_b(scenarios: List[Dict[str, Any]]) -> Dict[str, Any]:
    agg: Dict[str, Any] = {"total_scenarios": len(scenarios), "per_arm": {}}
    for arm in B_ARMS:
        runs = [s["arms"][arm] for s in scenarios]
        agg["per_arm"][arm] = {
            "label": B_ARM_LABELS[arm],
            "total_chunks": sum(r["chunk_count"] for r in runs),
            "total_prompt_chars": sum(r["prompt_chars"] for r in runs),
            "total_agent_calls": sum(r["agent_calls"] for r in runs),
            "all_ok": all(r["ok"] for r in runs),
        }
    unset_total = agg["per_arm"][B_ENV_UNSET]["total_prompt_chars"]
    for arm in B_ARMS:
        chars = agg["per_arm"][arm]["total_prompt_chars"]
        agg["per_arm"][arm]["prompt_chars_vs_unset"] = chars - unset_total
    agg["checks"] = [
        {"name": "★ 全部场景的 checks 都通过",
         "ok": all(s["all_checks_passed"] for s in scenarios)},
        {"name": "★ 声明档 Prompt 合计严格短于默认档",
         "ok": agg["per_arm"][B_ENV_DECLARED]["total_prompt_chars"] < unset_total,
         "detail": f"{agg['per_arm'][B_ENV_DECLARED]['total_prompt_chars']} < {unset_total}"},
        {"name": "★ 注入条数合计：默认档 → 声明档 必须严格减少",
         "ok": agg["per_arm"][B_ENV_DECLARED]["total_chunks"]
         < agg["per_arm"][B_ENV_UNSET]["total_chunks"],
         "detail": f"{agg['per_arm'][B_ENV_DECLARED]['total_chunks']} < "
                   f"{agg['per_arm'][B_ENV_UNSET]['total_chunks']}"},
    ]
    return agg


# ============================================================
# 七、闭环核对（环境变量路径 ↔ R2 的显式传参路径）
# ============================================================
def cross_check_with_r2(part_b: Dict[str, Any]) -> Dict[str, Any]:
    r2 = json.loads(R2_PATH.read_text(encoding="utf-8"))
    r2_by_scenario = {s["scenario_id"]: s for s in r2["scenarios"]}

    rows: List[Dict[str, Any]] = []
    for scenario in part_b["scenarios"]:
        sid = scenario["scenario_id"]
        r2_scenario = r2_by_scenario.get(sid)
        if r2_scenario is None:
            rows.append({"scenario_id": sid, "ok": False,
                         "detail": f"R2 产出里找不到场景 {sid}"})
            continue
        for arm, r2_arm in R2_ARM_OF.items():
            mine = scenario["arms"][arm]
            theirs = r2_scenario["arms"][r2_arm]
            same = (
                mine["chunk_count"] == theirs["knowledge_context_count"]
                and mine["prompt_chars"] == theirs["prompt_chars"]
                and mine["effective_min_score"] == theirs["effective_min_score"]
                and mine["prompt_template"] == theirs["prompt_template"]
            )
            rows.append({
                "scenario_id": sid,
                "arm": arm,
                "r2_arm": r2_arm,
                "ok": same,
                "env_path": {
                    "chunk_count": mine["chunk_count"],
                    "prompt_chars": mine["prompt_chars"],
                    "effective_min_score": mine["effective_min_score"],
                    "prompt_template": mine["prompt_template"],
                },
                "r2_kwargs_path": {
                    "chunk_count": theirs["knowledge_context_count"],
                    "prompt_chars": theirs["prompt_chars"],
                    "effective_min_score": theirs["effective_min_score"],
                    "prompt_template": theirs["prompt_template"],
                },
                "detail": "" if same else "两条路径结果不一致",
            })
    return {
        "source": R2_PATH.name,
        "note": "环境变量注入路径 与 显式 retriever_kwargs 透传路径 必须逐场景逐字节相同。",
        "rows": rows,
        "all_ok": all(r["ok"] for r in rows),
    }


def cross_check_with_rag_baseline(part_b: Dict[str, Any]) -> Dict[str, Any]:
    baseline = json.loads(RAG_BASELINE_PATH.read_text(encoding="utf-8"))
    counts = baseline["summary"]["retrieved_counts"]
    rows: List[Dict[str, Any]] = []
    for scenario in part_b["scenarios"]:
        sid = scenario["scenario_id"]
        expected = counts.get(sid)
        got = scenario["arms"][B_ENV_UNSET]["chunk_count"]
        rows.append({"scenario_id": sid, "ok": got == expected,
                     "env_unset_chunk_count": got,
                     "rag_baseline_retrieved_count": expected})
    return {
        "source": RAG_BASELINE_PATH.name,
        "note": "生产默认档（不设 RAG_*）的注入条数必须与既有 RAG 基准一致。",
        "rows": rows,
        "all_ok": all(r["ok"] for r in rows),
    }


# ============================================================
# 八、主流程
# ============================================================
def _integrity(part_a: Dict[str, Any], part_b: Dict[str, Any],
               cc_r2: Dict[str, Any], cc_base: Dict[str, Any]) -> Dict[str, Any]:
    problems: List[str] = []
    if part_a["corpus"]["chunks_in_store"] != part_a["corpus"]["documents"]:
        problems.append("Part A 语料切片数 != 文档数（query_set 约定每篇 1 片）")
    for scenario in part_b["scenarios"]:
        if not scenario["all_checks_passed"]:
            problems.append(f"{scenario['scenario_id']}: 场景 checks 未全通过")
    if not cc_r2["all_ok"]:
        problems.append("与 R2（显式 kwargs 路径）的闭环核对未全通过")
    if not cc_base["all_ok"]:
        problems.append("与 rag_baseline.json 的默认档条数核对未全通过")
    return {"ok": not problems, "problems": problems}


async def run() -> Dict[str, Any]:
    benchmark = _load_benchmark()
    baseline_scenario = next((s for s in benchmark
                              if s["id"] == PROMPT_BASELINE_SCENARIO), benchmark[0])
    async with _bench_env(baseline_scenario) as benv:
        variables = build_question_variables(
            benv["context"], benv["plan"], benv["resume"], benv["job"])
    _tmpl, base_prompt = interview_agent.render_question_prompt(variables, None)

    part_a = await run_part_a(variables, base_prompt)
    part_b = await run_part_b()
    cc_r2 = cross_check_with_r2(part_b)
    cc_base = cross_check_with_rag_baseline(part_b)

    return {
        "name": "rag-config-injection",
        "version": 1,
        "purpose": "验证「RAG 检索默认参数（RAG_TOP_K / RAG_MIN_SCORE）的配置注入」"
                   "在真实语料与真实出题流程上确实生效，且不改变默认行为。",
        "scope": "只驱动公开接口（build_vector_retriever / generate_next_question）；"
                 "不修改任何生产代码、不改基准文件。",
        "injection_point": {
            "env_vars": [ENV_RAG_TOP_K, ENV_RAG_MIN_SCORE],
            "resolver": "services.knowledge_rag.resolve_retriever_defaults",
            "assembler": "services.knowledge_rag.build_vector_retriever",
            "merge_rule": "最终参数 = { **环境变量默认值, **调用方显式传入 }",
            "production_path": "interview_core.resolve_retriever(db, None, True) "
                               "-> knowledge_rag.build_vector_retriever(db)",
            "note": "本运行器全程**不传** retriever_kwargs，只靠环境变量——"
                    "这正是「生产默认路径」的行为。",
        },
        "part_a_query_level": part_a,
        "part_b_end_to_end": part_b,
        "cross_checks": {
            "with_r2_explicit_kwargs_path": cc_r2,
            "with_rag_baseline": cc_base,
        },
        "integrity": _integrity(part_a, part_b, cc_r2, cc_base),
    }


def main() -> int:
    report = asyncio.run(run())
    RESULT_PATH.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=" * 78)
    print("Part A · 检索级（16 条已有 query，只靠环境变量）")
    print("=" * 78)
    pa = report["part_a_query_level"]
    print(f"语料：{pa['corpus']['documents']} 篇 / {pa['corpus']['chunks_in_store']} 片；"
          f"Prompt 基线 {pa['prompt_baseline']['base_prompt_chars']} 字")
    for arm, stat in pa["summary"]["per_arm"].items():
        print(f"  {arm:24s} 条数={stat['total_chunks']:3d} "
              f"零命中={stat['queries_with_zero_chunks']:2d}/{pa['summary']['queries']} "
              f"top1类正确={stat['top1_category_ok']:2d} "
              f"top1含关键词={stat['top1_keywords_ok']:2d} "
              f"Prompt增量={stat['total_prompt_delta_chars']:+6d}字 "
              f"min_score={stat['effective_min_score_values']}")
    print("\nPart A findings（观察结论，不参与 integrity）：")
    for f in pa["findings"]:
        print(f"  - {f['name']}：{f['detail']}")

    print()
    print("=" * 78)
    print("Part B · 端到端（3 个基准场景，只靠环境变量；Mock 大模型）")
    print("=" * 78)
    pb = report["part_b_end_to_end"]
    for scenario in pb["scenarios"]:
        print(f"\n[{scenario['scenario_id']}] 声明阈值="
              f"{scenario['declared_retrieval']['min_score']} "
              f"期望条数={scenario['declared_retrieval']['expected_chunk_count']}")
        for arm in B_ARMS:
            a = scenario["arms"][arm]
            print(f"  {arm:28s} min_score={str(a['effective_min_score']):>5s} "
                  f"条数={str(a['chunk_count']):>2s} "
                  f"Prompt={a['prompt_chars']:5d}字 "
                  f"Agent={a['agent_calls']}次 ok={a['ok']} "
                  f"题目={a['question_chars']}字")
        bad = [c for c in scenario["checks"] if not c["ok"]]
        print(f"  checks: {len(scenario['checks'])} 项，"
              f"{'全通过' if not bad else '失败 ' + str([c['name'] for c in bad])}")
        for f in scenario["findings"]:
            notable = bool(f.get("noise_injected")) or f.get("equivalent") is False
            print(f"    {'⚠ ' if notable else '  '}finding: {f['name']} -> {f['detail']}")
    print()
    print("汇总（三场景合计）：")
    for arm, stat in pb["aggregate"]["per_arm"].items():
        print(f"  {arm:28s} 条数={stat['total_chunks']:3d} "
              f"Prompt={stat['total_prompt_chars']:6d}字 "
              f"({stat['prompt_chars_vs_unset']:+6d}) Agent={stat['total_agent_calls']}次")

    print()
    print("=" * 78)
    print("闭环核对")
    print("=" * 78)
    print(f"  与 R2 显式 kwargs 路径：{'全一致' if report['cross_checks']['with_r2_explicit_kwargs_path']['all_ok'] else '有差异'}"
          f"（{len(report['cross_checks']['with_r2_explicit_kwargs_path']['rows'])} 项）")
    print(f"  与 rag_baseline.json：{'全一致' if report['cross_checks']['with_rag_baseline']['all_ok'] else '有差异'}")
    print(f"  integrity: {'OK' if report['integrity']['ok'] else report['integrity']['problems']}")
    print(f"\n结果写入 {RESULT_PATH}")
    return 0 if report["integrity"]["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
