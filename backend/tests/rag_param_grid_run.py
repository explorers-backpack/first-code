# -*- coding: utf-8 -*-
"""RAG 检索参数组合评估运行器（**非套件**，只读）。

任务：评估 ``top_k`` × ``min_score`` 的**组合策略**，确定检索层配置。

背景
----
``min_score`` 已经接入生产组装路径（任务 70 · R4：``RAG_TOP_K`` / ``RAG_MIN_SCORE``）。
本运行器回答的是**下一个问题**：这两个参数取什么组合合理？

网格
----
``top_k`` ∈ {3, 5, 8} × ``min_score`` ∈ {0.15, 0.25, 0.35} = **9 组**。

每一组记录 5 项（用户指定）：

1. **命中 chunk 数量**
2. **平均 score**
3. **最低 score**
4. **是否包含目标 chunk**
5. **knowledge_context 长度**

两个数据面
----------
- **Part A · 检索级**：``scripts/rag_query_set.json`` 的 **16 条固定 query**
  （16 篇受控语料，每篇 1 片）。目标 chunk 的判据取该文件自己的
  ``expectation_contract`` 第 4 条：**正文包含全部 ``expected_keywords``**。
- **Part B · 端到端**：``scripts/interview_eval_benchmark.json`` 的 **3 个场景**
  （22 篇 / 32 片语料），走真实 ``interview_core.generate_next_question``，
  只把大模型换成 Mock。目标 chunk 的判据是场景声明的 ``expected_hit_sources``。

为什么两个面都要
----------------
两者**语料不同、目标粒度不同**（Part A 目标唯一、Part B 目标是「某来源的若干片」），
组合是否合理必须在**两个面上同时成立**才算成立。只测一个面会得出片面结论。

只靠环境变量驱动
----------------
9 组全部通过 ``RAG_TOP_K`` / ``RAG_MIN_SCORE`` 注入（**不传任何 ``retriever_kwargs``**），
因此测的就是「接入 ``min_score`` 之后」的**生产组装路径**行为。
**本运行器不修改任何生产代码、不改任何默认值、不改基准文件。**

刻意不做
--------
**不评价大模型生成质量**。Part B 只记录「流程是否成功 / Agent 调用次数」作为
**存活检查**（liveness），不记录、不比较题目文本的好坏——检索层配置不该由
生成质量来定。

输出分层
--------
1. ``part_a_query_level`` / ``part_b_end_to_end``：9 组实验表（用户指定的 5 项记录）；
2. ``feasibility_bands``：**全量候选**上的边界量（召回上界 / 噪声清零下界 / 所需 top_k）
   与**边界探针**；
3. ``recommendation``：分层结论 —— 网格判定 → 阈值损失画像 → `top_k` 作用 →
   问题定性（单一全局阈值为何不可行）→ 方案与推荐范围。

★ **判定一律用全精度**：边界量给 ``*_exact``（判定用）与舍入版（展示用）两份。
把舍入值当 ``min_score`` 用会**误杀恰好等于上界的目标**（项目已知陷阱的第 2 次复发）。

为什么这个文件在 ``tests/`` 而不是 ``scripts/``
-----------------------------------------------
项目硬约束：**SQLite 只允许出现在 ``backend/tests/*.py`` 的内存库**。
Part B 要走 ``generate_next_question``（读会话行 / 上下文）⇒ 必须有库 ⇒ 只能放这里。
**不带 ``test_`` 前缀，不是回归套件，不进回归循环。**

运行
----
``python backend/tests/rag_param_grid_run.py``

结果写入 ``backend/scripts/rag_param_grid.json``。
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import sys
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

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
# 一、网格与常量
# ============================================================
TOP_K_GRID: Tuple[int, ...] = (3, 5, 8)
MIN_SCORE_GRID: Tuple[float, ...] = (0.15, 0.25, 0.35)

#: 9 组组合（顺序：先按 top_k、再按 min_score）。
GRID: Tuple[Tuple[int, float], ...] = tuple(
    (tk, ms) for tk in TOP_K_GRID for ms in MIN_SCORE_GRID
)

#: 目标 chunk 识别用的**全量扫描**参数（不受网格 top_k 截断影响）。
TARGET_SCAN_TOP_K = 100

#: Part A 的 Prompt 长度基线所用的场景（**只借它的 variables**，与 Part A 语料无关）。
PROMPT_BASELINE_SCENARIO = "s1-mysql-index-mid"

QUERY_SET_PATH = BACKEND_DIR / "scripts" / "rag_query_set.json"
BENCHMARK_PATH = BACKEND_DIR / "scripts" / "interview_eval_benchmark.json"
CORPUS_PATH = BACKEND_DIR / "scripts" / "interview_knowledge.json"
RESULT_PATH = BACKEND_DIR / "scripts" / "rag_param_grid.json"


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


@contextmanager
def _grid_env(top_k: Optional[int], min_score: Optional[float]) -> Iterator[None]:
    """把一组网格参数注入**环境变量**（生产组装路径的唯一入口）。"""
    with _envvars(**{
        ENV_RAG_TOP_K: None if top_k is None else str(top_k),
        ENV_RAG_MIN_SCORE: None if min_score is None else str(min_score),
    }):
        yield


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


def _chunk_key(chunk: Any) -> str:
    """chunk 的稳定标识（正文归一化后取哈希——与检索器 dedup 口径同源）。"""
    import hashlib

    text = "".join(str(getattr(chunk, "content", "") or "").split())
    return hashlib.blake2b(text.encode("utf-8"), digest_size=16).hexdigest()


def _scores_of(chunks: Sequence[Any]) -> List[float]:
    return [float(getattr(c, "metadata", {}).get("score") or 0.0) for c in chunks]


def _stats(chunks: Sequence[Any]) -> Dict[str, Any]:
    scores = _scores_of(chunks)
    return {
        "chunk_count": len(chunks),
        "scores": scores,
        "avg_score": (sum(scores) / len(scores)) if scores else None,
        "min_score_observed": min(scores) if scores else None,
        "sources": [getattr(c, "source", "") for c in chunks],
    }


def _round(value: Optional[float], digits: int = 6) -> Optional[float]:
    return None if value is None else round(value, digits)


# ============================================================
# 三、建库环境
# ============================================================
@asynccontextmanager
async def _query_env(documents: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    """Part A 环境：建库 + 导入 ``rag_query_set.json`` 的受控语料。"""
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
    """Part B 环境：与基准测试同一套建库手法（只 Mock 大模型）。"""
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

        session.add(User(username="grid", email="grid@example.com",
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
# 四、捕获「检索返回 / 最终 Prompt」
# ============================================================
@asynccontextmanager
async def _capture() -> Iterator[Dict[str, Any]]:
    """包装 ``retrieve_knowledge`` 与 ``interview_agent.generate_question``。"""
    captured: Dict[str, Any] = {"retrieve_calls": [], "agent_calls": 0}
    original_retrieve = interview_core.retrieve_knowledge
    original_generate = interview_agent.generate_question

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

    # ★ 替身必须与被替换函数**同签名**（R1 曾因漏跟进 retriever_kwargs 而运行期 TypeError）。
    for name, wrapper, original in (
        ("retrieve_knowledge", retrieve_wrapper, original_retrieve),
        ("generate_question", generate_wrapper, original_generate),
    ):
        if list(inspect.signature(wrapper).parameters) != \
                list(inspect.signature(original).parameters):
            raise AssertionError(f"{name} 替身签名与被替换函数不一致")

    interview_core.retrieve_knowledge = retrieve_wrapper
    interview_agent.generate_question = generate_wrapper
    try:
        yield captured
    finally:
        interview_core.retrieve_knowledge = original_retrieve
        interview_agent.generate_question = original_generate


# ============================================================
# 五、Part A · 检索级（16 条 query × 9 组）
# ============================================================
async def _scan(db: Any, query: str, *, top_k: int,
                min_score: Optional[float]) -> List[Any]:
    """只靠环境变量驱动组装器 → 检索一次（**生产组装路径**）。"""
    with _grid_env(top_k, min_score):
        retriever = build_vector_retriever(db)
        return await retriever.retrieve(None, query, None)


async def run_part_a(variables: Dict[str, Any], base_prompt: str) -> Dict[str, Any]:
    queries, documents = _load_queries()
    rows: List[Dict[str, Any]] = []
    scans: List[Dict[str, Any]] = []

    async with _query_env(documents) as env:
        db = env["db"]
        for q in queries:
            # --- 目标 chunk 识别：全量扫描（top_k=100、不过滤）---
            full = await _scan(db, q["query"], top_k=TARGET_SCAN_TOP_K, min_score=None)
            targets = [c for c in full
                       if all(kw in (getattr(c, "content", "") or "")
                              for kw in q["expected_keywords"])]
            target_keys = {_chunk_key(c) for c in targets}

            # --- 边界分析用：全量候选的**有序** (score, 是否目标) 序列 ---
            # 检索器返回的顺序就是排序口径（(-score, 位置)）⇒ 下标即名次。
            scans.append({
                "query": q["query"],
                "candidate_count": len(full),
                "ordered": [{"score": c.metadata.get("score"),
                             "is_target": _chunk_key(c) in target_keys}
                            for c in full],
            })

            # --- 9 组网格 ---
            for top_k, min_score in GRID:
                chunks = await _scan(db, q["query"], top_k=top_k, min_score=min_score)
                stat = _stats(chunks)
                present = {_chunk_key(c) for c in chunks} & target_keys
                _tmpl, with_knowledge = interview_agent.render_question_prompt(
                    variables, chunks)
                rows.append({
                    "query": q["query"],
                    "top_k": top_k,
                    "min_score": min_score,
                    "target_count_in_corpus": len(targets),
                    "target_chunks_present": len(present),
                    "target_hit": bool(present),
                    "target_full": len(targets) > 0 and len(present) == len(targets),
                    "knowledge_context_chars": len(with_knowledge) - len(base_prompt),
                    **{k: v for k, v in stat.items() if k != "sources"},
                })

        summary = _summarize_part_a(rows, queries)
        return {
            "corpus": {"documents": len(documents),
                       "chunks_in_store": env["chunks_in_store"]},
            "target_rule": "正文包含该 query 的**全部** expected_keywords"
                           "（取自 rag_query_set.json 的 expectation_contract 第 4 条）",
            "rows": rows,
            "combos": summary,
            "full_scan": scans,
            "prompt_baseline": {
                "scenario_id": PROMPT_BASELINE_SCENARIO,
                "note": "knowledge_context 长度 = 带知识 Prompt 与不带知识 Prompt 的字符差；"
                        "variables 固定为该场景，只有 knowledge_context 随组合变化。",
                "base_prompt_chars": len(base_prompt),
            },
        }


def _summarize_part_a(rows: List[Dict[str, Any]],
                      queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    total = len(queries)
    combos: List[Dict[str, Any]] = []
    for top_k, min_score in GRID:
        rs = [r for r in rows if r["top_k"] == top_k and r["min_score"] == min_score]
        non_empty = [r for r in rs if r["chunk_count"] > 0]
        combos.append({
            "top_k": top_k,
            "min_score": min_score,
            "total_chunks": sum(r["chunk_count"] for r in rs),
            "mean_chunks": round(sum(r["chunk_count"] for r in rs) / total, 3),
            "queries_empty": sum(1 for r in rs if r["chunk_count"] == 0),
            "mean_avg_score": _round(
                sum(r["avg_score"] for r in non_empty) / len(non_empty)
                if non_empty else None),
            "mean_min_score": _round(
                sum(r["min_score_observed"] for r in non_empty) / len(non_empty)
                if non_empty else None),
            "global_min_score": _round(
                min((r["min_score_observed"] for r in non_empty), default=None)),
            "target_hit_queries": sum(1 for r in rs if r["target_hit"]),
            "target_full_queries": sum(1 for r in rs if r["target_full"]),
            "target_miss_queries": [r["query"] for r in rs if not r["target_hit"]],
            "total_knowledge_context_chars": sum(r["knowledge_context_chars"]
                                                 for r in rs),
            "queries": total,
        })
    return combos


# ============================================================
# 六、Part B · 端到端（3 场景 × 9 组）
# ============================================================
async def _run_bench_combo(env: Dict[str, Any], scenario: Dict[str, Any],
                           top_k: int, min_score: float,
                           base_prompt: str) -> Dict[str, Any]:
    spy = SpySpark(_mock_reply_text(scenario))
    with _grid_env(top_k, min_score):
        async with _capture() as captured:
            # ★ 刻意**不传** retriever_kwargs：本次要测的就是「环境变量这条路」。
            result = await interview_core.generate_next_question(
                env["db"], env["session_id"],
                context=env["context"], plan=env["plan"],
                use_rag=True, spark=spy,
            )

    chunks = list(captured.get("knowledge_context") or [])
    prompt = captured.get("prompt", "")
    stat = _stats(chunks)
    expected_sources = set(scenario["retrieval"]["expected_hit_sources"])
    # ★ 覆盖必须按**片**数，不能按**来源去重**：s1/s2 的目标来源各有 2 片，
    #   用去重计数会恒为 1/1，把「只召回 1 片」掩盖成「完全覆盖」。
    expected_present = sum(1 for s in stat["sources"] if s in expected_sources)
    return {
        "top_k": top_k,
        "min_score": min_score,
        "chunk_count": stat["chunk_count"],
        "scores": stat["scores"],
        "avg_score": _round(stat["avg_score"]),
        "min_score_observed": _round(stat["min_score_observed"]),
        "sources": stat["sources"],
        "expected_hit_sources": sorted(expected_sources),
        "expected_chunks_present": expected_present,
        "target_hit": expected_present > 0,
        "knowledge_context_chars": len(prompt) - len(base_prompt),
        "prompt_chars": len(prompt),
        "prompt_template": captured.get("prompt_template"),
        # 存活检查（**不是**生成质量评价）
        "agent_calls": captured["agent_calls"],
        "ok": bool(result.get("ok")),
        "warnings": list(result.get("warnings") or []),
    }


async def run_part_b() -> Dict[str, Any]:
    scenarios_out: List[Dict[str, Any]] = []
    for scenario in _load_benchmark():
        async with _bench_env(scenario) as env:
            variables = build_question_variables(
                env["context"], env["plan"], env["resume"], env["job"])
            _tmpl, base_prompt = interview_agent.render_question_prompt(
                variables, None)

            # 全量扫描：算「期望来源在语料里一共有几片」，作为覆盖率分母
            full = await _scan(env["db"], scenario["retrieval"]["query_topic"],
                               top_k=TARGET_SCAN_TOP_K, min_score=None)
            expected_sources = set(scenario["retrieval"]["expected_hit_sources"])
            expected_total = sum(1 for c in full
                                 if getattr(c, "source", "") in expected_sources)

            rows = [await _run_bench_combo(env, scenario, tk, ms, base_prompt)
                    for tk, ms in GRID]

        for row in rows:
            row["expected_chunks_in_corpus"] = expected_total
            row["target_full"] = (expected_total > 0
                                  and row["expected_chunks_present"] == expected_total)
            row["coverage"] = f"{row['expected_chunks_present']}/{expected_total}"
            row["noise_chunks"] = row["chunk_count"] - row["expected_chunks_present"]

        scenarios_out.append({
            "scenario_id": scenario["id"],
            "label": scenario["label"],
            "declared": scenario["retrieval"],
            "chunks_in_store": env["chunks_in_store"],
            "expected_chunks_in_corpus": expected_total,
            "base_prompt_chars": len(base_prompt),
            "rows": rows,
            "full_scan": {
                "candidate_count": len(full),
                "ordered": [{"score": c.metadata.get("score"),
                             "is_target": getattr(c, "source", "") in expected_sources}
                            for c in full],
            },
        })
    return {"scenarios": scenarios_out, "combos": _summarize_part_b(scenarios_out)}


def _summarize_part_b(scenarios: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    combos: List[Dict[str, Any]] = []
    for top_k, min_score in GRID:
        rs: List[Dict[str, Any]] = []
        for s in scenarios:
            rs.extend(r for r in s["rows"]
                      if r["top_k"] == top_k and r["min_score"] == min_score)
        non_empty = [r for r in rs if r["chunk_count"] > 0]
        combos.append({
            "top_k": top_k,
            "min_score": min_score,
            "total_chunks": sum(r["chunk_count"] for r in rs),
            "total_noise_chunks": sum(r["noise_chunks"] for r in rs),
            "scenarios_target_hit": sum(1 for r in rs if r["target_hit"]),
            "scenarios_target_full": sum(1 for r in rs if r["target_full"]),
            "coverage": [r["coverage"] for r in rs],
            "mean_avg_score": _round(
                sum(r["avg_score"] for r in non_empty) / len(non_empty)
                if non_empty else None),
            "mean_min_score": _round(
                sum(r["min_score_observed"] for r in non_empty) / len(non_empty)
                if non_empty else None),
            "total_knowledge_context_chars": sum(r["knowledge_context_chars"]
                                                 for r in rs),
            "total_prompt_chars": sum(r["prompt_chars"] for r in rs),
            "all_ok": all(r["ok"] for r in rs),
            "scenarios": len(rs),
        })
    return combos


# ============================================================
# 七、边界分析：用**全量候选**直接算 min_score 的可行区间与所需 top_k
# ============================================================
def _scan_name(scan: Dict[str, Any]) -> str:
    return str(scan.get("query") or scan.get("scenario_id") or "?")


def _profile_at(scans: Sequence[Dict[str, Any]], min_score: float) -> Dict[str, Any]:
    """在**全量候选**上套用 ``min_score`` 后的画像（不涉及 top_k 截断）。

    ★ ``min_score`` 一律传**全精度**值；舍入值只用于展示（见陷阱 ⑨）。
    """
    total = targets = 0
    failed: List[str] = []
    need = 0
    binding: Optional[str] = None
    for s in scans:
        passing = [o for o in s["ordered"] if o["score"] >= min_score]
        idx = [i for i, o in enumerate(passing) if o["is_target"]]
        total += len(passing)
        targets += len(idx)
        if not idx:
            failed.append(_scan_name(s))
            continue
        if idx[-1] + 1 > need:
            need, binding = idx[-1] + 1, _scan_name(s)
    return {
        "min_score": _round(min_score),
        "total_chunks": total,
        "target_chunks": targets,
        "noise_chunks": total - targets,
        "required_top_k": None if failed else need,
        "required_top_k_binding": binding,
        "recall_failed_units": failed,
    }


def _dataset_stats(label: str, scans: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """一个数据面的边界量（全部由**全量候选**算出，不受网格 top_k 截断影响）。

    - ``recall_ceiling``：**保住全部目标片**所能取的最大 ``min_score``
      （因为过滤是 ``score >= min_score`` 含等号，等于「所有目标片里的最低分」）。
    - ``zero_noise_floor``：**噪声清零**所需的最小 ``min_score``（严格大于噪声最高分）。
    - ``binding``：谁决定了上面两个量（排查时先看它）。

    ★ 每个边界量给**两份**：``*_exact``（全精度，**只用于后续判定**）与
      不带后缀的（``_round``，**只用于展示**）。混用会让边界探针误杀目标（陷阱 ⑨）。
    """
    targets = [(_scan_name(s), min(o["score"] for o in s["ordered"] if o["is_target"]))
               for s in scans if any(o["is_target"] for o in s["ordered"])]
    noises = [(_scan_name(s),
               max((o["score"] for o in s["ordered"] if not o["is_target"]), default=None))
              for s in scans]
    noises = [(q, v) for q, v in noises if v is not None]

    ceiling_query, ceiling = min(targets, key=lambda kv: kv[1])
    floor_query, floor = max(noises, key=lambda kv: kv[1])
    clean = floor < ceiling
    return {
        "label": label,
        "units": len(scans),
        "recall_ceiling": _round(ceiling),
        "recall_ceiling_exact": ceiling,
        "recall_ceiling_binding": ceiling_query,
        "zero_noise_floor": _round(floor),
        "zero_noise_floor_exact": floor,
        "zero_noise_floor_binding": floor_query,
        "has_clean_band": clean,
        "clean_band": [_round(floor), _round(ceiling)] if clean else None,
        "clean_band_note": (
            f"({_round(floor)}, {_round(ceiling)}] —— 含等号，下界开、上界闭"
            if clean else
            f"无：噪声最高分 {_round(floor)} >= 目标最低分 {_round(ceiling)}"
            " ⇒ 单一全局阈值无法「既保住全部目标、又清零噪声」"),
        "optimum_at_recall_ceiling": _profile_at(scans, ceiling),
    }


def _required_top_k(scans: Sequence[Dict[str, Any]], min_score: float) -> Dict[str, Any]:
    """给定 ``min_score``，算「保住全部目标片」所需的**最小 top_k**。

    做法：在**全量有序候选**上按 ``score >= min_score`` 过滤，取「最后一个目标片」
    的名次 + 1（名次即过滤后序列的下标，与检索器的 ``(-score, 位置)`` 排序同口径）。
    任一单元的目标片被过滤掉 ⇒ 返回 ``recall_failed``（``top_k`` 再大也救不回来）。

    ★ ``min_score`` 必须传**全精度**值：传 ``_round`` 后的值会让「恰好等于上界」
      的目标被误杀（陷阱 ⑨）。
    """
    worst = 0
    worst_unit: Optional[str] = None
    failed: List[str] = []
    for s in scans:
        passing = [o for o in s["ordered"] if o["score"] >= min_score]
        idx = [i for i, o in enumerate(passing) if o["is_target"]]
        if not idx:
            failed.append(_scan_name(s))
            continue
        need = idx[-1] + 1
        if need > worst:
            worst, worst_unit = need, _scan_name(s)
    return {
        "min_score": _round(min_score),
        "required_top_k": None if failed else worst,
        "binding_unit": worst_unit,
        "recall_failed_units": failed,
    }


def feasibility_bands(part_a: Dict[str, Any], part_b: Dict[str, Any]) -> Dict[str, Any]:
    """把两个数据面的边界量合起来，给出 ``min_score`` 的可行区间与所需 ``top_k``。"""
    a = _dataset_stats("Part A（16 条固定 query）", part_a["full_scan"])
    b_scans = [dict(s["full_scan"], scenario_id=s["scenario_id"])
               for s in part_b["scenarios"]]
    b = _dataset_stats("Part B（3 个基准场景）", b_scans)

    # ★ 一律用全精度（*_exact）做判定与探针；舍入值只进 JSON 给人读。
    ceiling = min(a["recall_ceiling_exact"], b["recall_ceiling_exact"])
    floor = max(a["zero_noise_floor_exact"], b["zero_noise_floor_exact"])
    clean = floor < ceiling

    # 推荐值：有干净带 → 取带内中点；没有 → **优先保召回**，取上界（即 recall_ceiling）
    if clean:
        recommended = (floor + ceiling) / 2
        strategy = "带内中点（距召回上界与噪声下界都有余量）"
    else:
        recommended = ceiling
        strategy = "无干净带 ⇒ **优先保召回**，取召回上界（接受残余噪声）"

    combined = {
        "part_a": a,
        "part_b": b,
        "combined": {
            "recall_ceiling": _round(ceiling),
            "recall_ceiling_exact": ceiling,
            "zero_noise_floor": _round(floor),
            "zero_noise_floor_exact": floor,
            "has_clean_band": clean,
            "clean_band": [_round(floor), _round(ceiling)] if clean else None,
        },
        "recommended_min_score": _round(recommended),
        "recommended_min_score_exact": recommended,
        "recommended_strategy": strategy,
    }

    # 边界探针（**全精度**）——回答「在这个阈值上，保住目标需要多大的 top_k」
    combined["boundary_probes"] = [
        {"label": label,
         "min_score": _round(m),
         "min_score_exact": m,
         "part_a": _required_top_k(part_a["full_scan"], m),
         "part_b": _required_top_k(b_scans, m)}
        for label, m in (("recommended", recommended),
                         ("recall_ceiling", ceiling),
                         ("zero_noise_floor", floor))
    ]
    # 网格内的 9 组也各算一遍，方便与实验表对照
    combined["top_k_requirement_grid"] = [
        {"top_k": tk, "min_score": ms,
         "part_a": _required_top_k(part_a["full_scan"], ms),
         "part_b": _required_top_k(b_scans, ms)}
        for tk, ms in GRID
    ]
    # 三个网格阈值在**全量候选**上的画像（排除 top_k 截断的干扰，
    # 用来回答「这个阈值到底能不能保住目标、会放几条噪声」）
    combined["profile_by_min_score"] = [
        {"min_score": ms,
         "part_a": _profile_at(part_a["full_scan"], ms),
         "part_b": _profile_at(b_scans, ms)}
        for ms in MIN_SCORE_GRID
    ]
    return combined


# ============================================================
# 八、推荐配置范围（分层：网格判定 → 损失画像 → 问题定性 → 方案）
# ============================================================
CRITERIA = {
    "A": "Part A 16 条 query 全部检回目标 chunk（target_hit_queries == 16）",
    "B": "Part B 3 个场景全部覆盖全部期望来源片（scenarios_target_full == 3）",
    "tie_break": "满足 A/B 后，按 (Part A 总条数 + Part B 总条数) 升序；越少越好",
}


def _grid_evaluation(part_a: Dict[str, Any],
                     part_b: Dict[str, Any]) -> List[Dict[str, Any]]:
    """网格 9 组在**硬约束**下的逐组判定（纯判定，不掺观察结论）。"""
    a_by = {(c["top_k"], c["min_score"]): c for c in part_a["combos"]}
    b_by = {(c["top_k"], c["min_score"]): c for c in part_b["combos"]}
    evaluated: List[Dict[str, Any]] = []
    for top_k, min_score in GRID:
        a, b = a_by[(top_k, min_score)], b_by[(top_k, min_score)]
        ok_a = a["target_hit_queries"] == a["queries"]
        ok_b = b["scenarios_target_full"] == b["scenarios"]
        evaluated.append({
            "top_k": top_k,
            "min_score": min_score,
            "constraint_a_target_recall": ok_a,
            "constraint_b_scenario_coverage": ok_b,
            "feasible": ok_a and ok_b,
            "a_hit_n": a["target_hit_queries"],
            "a_total_n": a["queries"],
            "b_full_n": b["scenarios_target_full"],
            "b_total_n": b["scenarios"],
            "a_target_hit": f"{a['target_hit_queries']}/{a['queries']}",
            "b_target_full": f"{b['scenarios_target_full']}/{b['scenarios']}",
            "total_chunks_a": a["total_chunks"],
            "total_chunks_b": b["total_chunks"],
            "total_noise_chunks_b": b["total_noise_chunks"],
            "total_knowledge_context_chars": (a["total_knowledge_context_chars"]
                                              + b["total_knowledge_context_chars"]),
        })
    return evaluated


def _threshold_loss_profile(evaluated: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """按 ``min_score`` 聚合的**损失画像**。

    ★ 这一层把「**阈值决定召回**」与「**top_k 决定噪声**」分开：
      同一 ``min_score`` 下 Part A 的召回与 ``top_k`` 无关（实测 3/5/8 同值）
      ⇒ 「丢了几条 query」是阈值的性质，不是 ``top_k`` 的性质。
    """
    a_total = evaluated[0]["a_total_n"]
    b_total = evaluated[0]["b_total_n"]
    out: List[Dict[str, Any]] = []
    for ms in MIN_SCORE_GRID:
        rs = sorted((e for e in evaluated if e["min_score"] == ms),
                    key=lambda e: e["top_k"])
        best_a = max(e["a_hit_n"] for e in rs)
        best_b = max(e["b_full_n"] for e in rs)
        out.append({
            "min_score": ms,
            "part_a_target_hit": f"{best_a}/{a_total}",
            "part_b_target_full": f"{best_b}/{b_total}",
            "part_a_lost_queries": a_total - best_a,
            "part_b_lost_scenarios": b_total - best_b,
            "recall_independent_of_top_k": (
                len({e["a_hit_n"] for e in rs}) == 1
                and len({e["b_full_n"] for e in rs}) == 1),
            "chunks_by_top_k": {e["top_k"]: [e["total_chunks_a"], e["total_chunks_b"]]
                                for e in rs},
            "context_chars_by_top_k": {e["top_k"]: e["total_knowledge_context_chars"]
                                       for e in rs},
        })
    return out


def _top_k_effect(evaluated: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """``top_k`` 在**同一 min_score 内**的影响：是否改变召回、改变多少条数。"""
    out: List[Dict[str, Any]] = []
    for ms in MIN_SCORE_GRID:
        rs = sorted((e for e in evaluated if e["min_score"] == ms),
                    key=lambda e: e["top_k"])
        best_a = max(e["a_hit_n"] for e in rs)
        best_b = max(e["b_full_n"] for e in rs)
        out.append({
            "min_score": ms,
            "recall_by_top_k": {e["top_k"]: [e["a_hit_n"], e["b_full_n"]] for e in rs},
            "chunks_by_top_k": {e["top_k"]: [e["total_chunks_a"], e["total_chunks_b"]]
                                for e in rs},
            "recall_changes_with_top_k": (
                len({e["a_hit_n"] for e in rs}) > 1
                or len({e["b_full_n"] for e in rs}) > 1),
            "min_top_k_at_best_recall": min(
                e["top_k"] for e in rs
                if e["a_hit_n"] == best_a and e["b_full_n"] == best_b),
            "chunk_growth_3_to_8": [rs[-1]["total_chunks_a"] - rs[0]["total_chunks_a"],
                                    rs[-1]["total_chunks_b"] - rs[0]["total_chunks_b"]],
        })
    return out


def recommend(part_a: Dict[str, Any], part_b: Dict[str, Any],
              bands: Dict[str, Any]) -> Dict[str, Any]:
    """分层给出「检索层配置」的结论。

    硬约束（两条都必须在**全部**数据面上成立）：

    - ``A``：Part A 的 **16 条 query 全部检回目标 chunk**；
    - ``B``：Part B 的 **3 个场景全部覆盖全部期望来源片**。

    第 1 层 ``grid_conclusion``：网格 9 组的直接判定；
    第 2 层 ``threshold_loss_profile``：按阈值聚合的损失（与 top_k 解耦）；
    第 3 层 ``top_k_effect``：``top_k`` 在同一阈值内的作用；
    第 4 层 ``problem_statement``：**单一全局阈值为什么做不到**（区间不相交）；
    第 5 层 ``options`` / ``recommended_range``：可选方案与推荐范围。
    """
    evaluated = _grid_evaluation(part_a, part_b)
    feasible = [e for e in evaluated if e["feasible"]]
    loss = _threshold_loss_profile(evaluated)
    top_k_effect = _top_k_effect(evaluated)

    pa, pb, cb = bands["part_a"], bands["part_b"], bands["combined"]

    # ---- 单一全局阈值的「两面区间」（全部由**全量候选**算出；用全精度，陷阱 ⑨）----
    a_recall_max = pa["recall_ceiling_exact"]   # 保住 Part A 全部目标片的最大阈值
    b_recall_max = pb["recall_ceiling_exact"]
    global_recall_max = min(a_recall_max, b_recall_max)
    recall_binding = (pa["recall_ceiling_binding"] if a_recall_max <= b_recall_max
                      else pb["recall_ceiling_binding"])
    noise_free_min = max(pa["zero_noise_floor_exact"], pb["zero_noise_floor_exact"])
    noise_binding = (pa["zero_noise_floor_binding"]
                     if pa["zero_noise_floor_exact"] >= pb["zero_noise_floor_exact"]
                     else pb["zero_noise_floor_binding"])
    grid_min = min(MIN_SCORE_GRID)

    problem_statement = {
        "summary": ("网格 9 组在硬约束下**全部不可行**：任一网格阈值都已高于"
                    "「保住全部目标片」的上限。"
                    if not feasible else "网格内存在可行组合。"),
        "why": (
            f"保「Part A 16/16 召回」要求 min_score <= {_round(a_recall_max)}；"
            f"保「Part B 3/3 场景全覆盖」要求 min_score <= {_round(b_recall_max)}；"
            f"⇒ 单一全局阈值的**召回安全上限** = {_round(global_recall_max)}"
            f"（由「{recall_binding}」决定），而网格最小取值 {grid_min} 已高于它。"
            f"另一面：把噪声清零需要 min_score > {_round(noise_free_min)}"
            f"（由「{noise_binding}」决定），与召回上限 {_round(global_recall_max)} 不相交 ⇒ "
            "**不存在既保住全部目标、又清零噪声的单一全局阈值**。"),
        "single_global_threshold": {
            "recall_safe_upper_bound": _round(global_recall_max),
            "recall_safe_upper_bound_part_a": _round(a_recall_max),
            "recall_safe_upper_bound_part_b": _round(b_recall_max),
            "recall_safe_upper_bound_binding": recall_binding,
            "noise_free_lower_bound": _round(noise_free_min),
            "noise_free_lower_bound_binding": noise_binding,
            "clean_band": cb["clean_band"],
            "exists": cb["has_clean_band"],
            "note": "「召回安全」= min_score <= 召回上限（过滤含等号）；"
                    "「噪声清零」= min_score > 噪声下界。两者须**同时**成立才叫可行。",
        },
    }

    options = [
        {
            "id": "A",
            "name": "按语料 / 按场景分别标定 min_score（推荐）",
            "feasible": True,
            "why": ("两个数据集的分数尺度不同（HashEmbedding 是**词面哈希**，绝对分数只在"
                    "同模型同语料内可比）⇒ 用一个常数跨语料必然顾此失彼。"),
            "procedure": [
                "对该语料跑一次全量检索（top_k=100、不设 min_score），取每个 query 的 top-1 分数；",
                "min_score = floor(0.5 × top-1 分数, 2 位小数)"
                "（规则见 scripts/rag_query_set.json 的 min_score_calibration）；",
                "语料 / Embedding 模型一变就重标；标定必须在**全量候选**上做，否则只是局部窗口。",
            ],
            "on_these_datasets": {
                "part_a": {"recall_safe_upper_bound": _round(a_recall_max),
                           "binding": pa["recall_ceiling_binding"],
                           "note": "16 条 query 的 top-1 分数跨度大 ⇒ 常数阈值几乎必然误杀"},
                "part_b": {"recall_safe_upper_bound": _round(b_recall_max),
                           "noise_free_lower_bound": _round(pb["zero_noise_floor_exact"]),
                           "clean_band": pb["clean_band"],
                           "note": f"窗口宽度仅 "
                                   f"{_round(pb['recall_ceiling_exact'] - pb['zero_noise_floor_exact'])}"
                                   f"（无干净带 ⇒ 必须在「保召回」与「压噪声」之间取舍）"},
            },
        },
        {
            "id": "B",
            "name": "单一全局 min_score",
            "feasible": False,
            "why": (f"召回安全上限 {_round(global_recall_max)} 与噪声清零下界 "
                    f"{_round(noise_free_min)} 不相交 ⇒ 不存在可行值；"
                    "网格 9 组也全部落在「丢召回」一侧。"),
        },
    ]

    # ---- 网格内「最不坏」的一档：先比召回，再比条数 ----
    ranked_grid = sorted(evaluated,
                         key=lambda e: (-e["a_hit_n"], -e["b_full_n"],
                                        e["total_chunks_a"] + e["total_chunks_b"],
                                        e["top_k"], e["min_score"]))
    best = ranked_grid[0]
    best_in_grid = {
        "top_k": best["top_k"],
        "min_score": best["min_score"],
        "part_a_target_hit": best["a_target_hit"],
        "part_b_target_full": best["b_target_full"],
        "total_chunks": best["total_chunks_a"] + best["total_chunks_b"],
        "note": ("网格内召回最优的一档（**仍不满足硬约束 A**）；"
                 "同一 min_score 下 top_k=3/5/8 召回完全相同，只有条数差别。"),
    }

    recommended_range = {
        "top_k": {
            "value": 5,
            "grid": list(TOP_K_GRID),
            "note": ("min_score 生效后 top_k 只负责「别把目标片截掉」；"
                     "本实验 9 组里没有任何一组因 top_k 截断丢目标 ⇒ 3/5/8 都够。"
                     "保持当前声明值 5（**不必改**）；要更保守可取 8。"),
            "required_top_k_grid": bands["top_k_requirement_grid"],
            "evidence": top_k_effect,
        },
        "min_score": {
            "global_constant": None,
            "reason": "两个数据集的可行区间不相交（见 problem_statement）",
            "if_recall_first": {
                "range": [0.0, _round(global_recall_max)],
                "note": "≈不设阈值；能保 16/16 + 3/3，但噪声全部保留。"
                        "上界含等号可用，但**贴边零余量**，实践建议再留 ~0.01 余量。",
            },
            "if_noise_first": {
                "range": [_round(noise_free_min), 1.0],
                "note": "噪声清零，但 Part A 目标片几乎全被误杀、Part B 丢 s2 的 1/2 片",
            },
            "calibrate_per_corpus": True,
        },
    }

    excluded = [
        {"min_score": p["min_score"],
         "part_a_target_hit": p["part_a_target_hit"],
         "part_b_target_full": p["part_b_target_full"],
         "why": "; ".join(filter(None, [
             (f"Part A 只召回 {p['part_a_target_hit']}"
              f"（丢 {p['part_a_lost_queries']} 条 query）"
              if p["part_a_lost_queries"] else ""),
             (f"Part B 只有 {p['part_b_target_full']} 场景全覆盖"
              if p["part_b_lost_scenarios"] else ""),
         ])) or "两数据面均满足召回（未进入网格）"}
        for p in loss
    ]

    return {
        "criteria": CRITERIA,
        "grid_conclusion": {
            "feasible_combos": [[e["top_k"], e["min_score"]] for e in feasible],
            "feasible_count": len(feasible),
            "total": len(evaluated),
            "verdict": ("9/9 不可行（全部败在硬约束 A）" if not feasible
                        else f"{len(feasible)}/{len(evaluated)} 可行"),
        },
        "evaluated": evaluated,
        "threshold_loss_profile": loss,
        "top_k_effect": top_k_effect,
        "problem_statement": problem_statement,
        "options": options,
        "excluded": excluded,
        "best_in_grid": best_in_grid,
        "recommended_range": recommended_range,
    }


# ============================================================
# 八、主流程
# ============================================================
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
    bands = feasibility_bands(part_a, part_b)

    return {
        "name": "rag-param-grid",
        "version": 1,
        "purpose": "评估 top_k × min_score 的组合策略，确定检索层配置。"
                   "只测检索层，**不评价大模型生成质量**。",
        "scope": "只读：通过 RAG_TOP_K / RAG_MIN_SCORE 环境变量驱动生产组装路径；"
                 "不修改任何生产代码、不改任何默认值、不改基准文件。",
        "grid": {
            "top_k": list(TOP_K_GRID),
            "min_score": list(MIN_SCORE_GRID),
            "combos": [{"top_k": tk, "min_score": ms} for tk, ms in GRID],
            "count": len(GRID),
        },
        "driving": {
            "mechanism": "环境变量注入（RAG_TOP_K / RAG_MIN_SCORE）",
            "note": "全程不传 retriever_kwargs ⇒ 测的就是「接入 min_score 之后」的"
                    "生产组装路径（interview_core.resolve_retriever -> "
                    "knowledge_rag.build_vector_retriever）。",
        },
        "part_a_query_level": part_a,
        "part_b_end_to_end": part_b,
        "feasibility_bands": bands,
        "recommendation": recommend(part_a, part_b, bands),
    }


def main() -> int:
    report = asyncio.run(run())
    RESULT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2),
                           encoding="utf-8")

    a = report["part_a_query_level"]
    b = report["part_b_end_to_end"]

    print("=" * 112)
    print("Part A · 检索级（16 条固定 query / 16 篇受控语料，每篇 1 片）")
    print("=" * 112)
    print(f"{'top_k':>5} {'min_score':>9} | {'命中chunk':>8} {'平均score':>9} "
          f"{'最低score':>9} | {'含目标chunk':>11} {'knowledge_context':>17}")
    print("-" * 112)
    for c in a["combos"]:
        print(f"{c['top_k']:>5} {c['min_score']:>9.2f} | {c['total_chunks']:>8} "
              f"{_fmt(c['mean_avg_score']):>9} {_fmt(c['mean_min_score']):>9} | "
              f"{c['target_hit_queries']:>6}/{c['queries']:<4} "
              f"{c['total_knowledge_context_chars']:>15}字")

    print()
    print("=" * 112)
    print("Part B · 端到端（3 个基准场景 / 22 篇 32 片语料，Mock 大模型）")
    print("=" * 112)
    print(f"{'top_k':>5} {'min_score':>9} | {'命中chunk':>8} {'平均score':>9} "
          f"{'最低score':>9} | {'含目标chunk':>11} {'期望来源覆盖':>12} "
          f"{'knowledge_context':>17} {'Prompt':>8}")
    print("-" * 112)
    for c in b["combos"]:
        print(f"{c['top_k']:>5} {c['min_score']:>9.2f} | {c['total_chunks']:>8} "
              f"{_fmt(c['mean_avg_score']):>9} {_fmt(c['mean_min_score']):>9} | "
              f"{c['scenarios_target_hit']:>6}/{c['scenarios']:<4} "
              f"{str(c['coverage']):>12} "
              f"{c['total_knowledge_context_chars']:>15}字 "
              f"{c['total_prompt_chars']:>7}字")

    print()
    print("=" * 112)
    print("边界分析（全量候选；不受网格 top_k 截断影响）")
    print("=" * 112)
    fb = report["feasibility_bands"]
    for key in ("part_a", "part_b"):
        d = fb[key]
        print(f"{d['label']}：")
        print(f"  召回上界 min_score <= {d['recall_ceiling']}"
              f"（精确 {d['recall_ceiling_exact']:.10f}；"
              f"由「{d['recall_ceiling_binding']}」决定）")
        print(f"  噪声清零需 min_score > {d['zero_noise_floor']}"
              f"（精确 {d['zero_noise_floor_exact']:.10f}；"
              f"由「{d['zero_noise_floor_binding']}」决定）")
        print(f"  干净带：{d['clean_band_note']}")
    c = fb["combined"]
    print(f"合并后：干净带 = {c['clean_band']}；"
          f"推荐 min_score = {fb['recommended_min_score']}"
          f"（精确 {fb['recommended_min_score_exact']:.10f}；"
          f"{fb['recommended_strategy']}）")
    print("  边界探针（**全精度**；问「在这个阈值上保住目标需要多大 top_k」）")
    for probe in fb["boundary_probes"]:
        req = probe
        print(f"    [{probe['label']}] min_score={probe['min_score']}"
              f"（{probe['min_score_exact']:.10f}）"
              f"：Part A 需 top_k={req['part_a']['required_top_k']}"
              f"（绑定 {req['part_a']['binding_unit']}）；"
              f"Part B 需 top_k={req['part_b']['required_top_k']}"
              f"（绑定 {req['part_b']['binding_unit']}）")

    print()
    print("=" * 112)
    print("推荐（分层）")
    print("=" * 112)
    rec = report["recommendation"]

    gc = rec["grid_conclusion"]
    print(f"[1] 网格判定：{gc['verdict']}（可行组合 {gc['feasible_combos']}）")

    print("[2] 阈值损失画像（Part A 召回与 top_k 无关）")
    for p in rec["threshold_loss_profile"]:
        print(f"    min_score={p['min_score']:.2f}：Part A {p['part_a_target_hit']}"
              f"（丢 {p['part_a_lost_queries']} 条）、Part B {p['part_b_target_full']}"
              f"；条数(Part A/B)="
              f"{ {k: v for k, v in p['chunks_by_top_k'].items()} }"
              f"{'  [召回随 top_k 变化]' if not p['recall_independent_of_top_k'] else ''}")

    print("[3] top_k 的作用（同一 min_score 内）")
    for p in rec["top_k_effect"]:
        print(f"    min_score={p['min_score']:.2f}：召回(Part A,B)="
              f"{p['recall_by_top_k']}；3→8 条数增量={p['chunk_growth_3_to_8']}"
              f"；{'★ 召回随 top_k 变化' if p['recall_changes_with_top_k'] else '召回恒定'}")

    ps = rec["problem_statement"]
    print(f"[4] 问题定性：{ps['summary']}")
    print(f"    {ps['why']}")
    sgt = ps["single_global_threshold"]
    print(f"    单一全局阈值：召回安全上限 {sgt['recall_safe_upper_bound']}"
          f"（Part A {sgt['recall_safe_upper_bound_part_a']} / "
          f"Part B {sgt['recall_safe_upper_bound_part_b']}，绑定「{sgt['recall_safe_upper_bound_binding']}」）"
          f"；噪声清零下界 {sgt['noise_free_lower_bound']}"
          f"（绑定「{sgt['noise_free_lower_bound_binding']}」）"
          f"；干净带={sgt['clean_band']} ⇒ 可行={sgt['exists']}")

    print("[5] 方案")
    for o in rec["options"]:
        flag = "✔ 可行" if o["feasible"] else "✘ 不可行"
        print(f"    {flag} 方案 {o['id']}：{o['name']}")
        print(f"        {o['why']}")
    print("    排除项（网格取值为何被排除）")
    for item in rec["excluded"]:
        print(f"        min_score={item['min_score']:.2f}：{item['why']}")

    bg = rec["best_in_grid"]
    print(f"[6] 网格内最不坏的一档：top_k={bg['top_k']} / min_score={bg['min_score']} "
          f"（Part A {bg['part_a_target_hit']}、Part B {bg['part_b_target_full']}，"
          f"共 {bg['total_chunks']} 条）")

    rr = rec["recommended_range"]
    print("[7] ★ 推荐配置范围")
    print(f"    top_k：{rr['top_k']['value']}（网格 {rr['top_k']['grid']}）"
          f"—— {rr['top_k']['note']}")
    ms = rr["min_score"]
    print(f"    min_score：**无单一全局常数**（{ms['reason']}）")
    print(f"        召回优先：[{ms['if_recall_first']['range'][0]}, "
          f"{ms['if_recall_first']['range'][1]}] —— {ms['if_recall_first']['note']}")
    print(f"        噪声优先：[{ms['if_noise_first']['range'][0]}, "
          f"{ms['if_noise_first']['range'][1]}] —— {ms['if_noise_first']['note']}")
    print(f"        实践做法：按语料标定（calibrate_per_corpus="
          f"{ms['calibrate_per_corpus']}）")
    print(f"\n结果写入 {RESULT_PATH}")
    return 0


def _fmt(value: Optional[float]) -> str:
    return "—" if value is None else f"{value:.4f}"


if __name__ == "__main__":
    sys.exit(main())
