# -*- coding: utf-8 -*-
"""有 RAG 基准运行器 —— 开启 RAG 跑一次真实出题，把结果落盘成基准记录。

用途
----
建立「``use_rag=True``」的 AI 面试问题生成记录，**与无 RAG 基准逐字段可比较**
（``backend/scripts/no_rag_baseline.json``）。
**只记录，不评价生成质量**；**不修改 InterviewAgent / Prompt 模板 / Retriever / VectorStore**。

唯一变化
--------
与无 RAG 运行相比，**调用参数只差一个**：``use_rag=True``（``retriever`` 仍为 ``None``
⇒ 由 ``interview_core.resolve_retriever`` 组装真实链路）。

保持完全相同（由运行器**断言**，不靠人工核对）
------------------------------------------------
- ``interview_session``（同一套会话行配置）
- ``interview_plan``（同一个 Planner 函数、``use_llm=False``）
- 用户信息（``job`` + ``resume``）
- 面试阶段（``context.current_stage``）
- ``difficulty``
- 历史问题（``context.asked_questions``）

条件来源与无 RAG 臂**同一份**：``backend/scripts/interview_eval_benchmark.json``。

记录内容（用户指定形状 + 取证补充）
------------------------------------
``input`` / ``rag`` / ``output`` / ``field_map``，另加 ``full_output`` 与 ``provenance``。
其中 ``input`` 与无 RAG 记录的 ``fixed_inputs`` **逐键相同**（运行器会读回基线文件比对，
不一致直接判失败）。

``rag`` 里同时记录：
1. **实际 Retriever 返回内容**（``retrieved_chunks``，来自 ``interview_core.retrieve_knowledge``
   这个**唯一接线点**的返回值，含 ``source`` / ``metadata.score``）
2. **最终 Prompt 中的 knowledge_context**（``knowledge_context``，从真实 Prompt 里截出）
3. 检索器配置（``top_k`` / ``min_score`` / ``model_name`` / 后端来源）

为什么这个文件在 ``tests/`` 而不是 ``scripts/``
-----------------------------------------------
项目硬约束：**SQLite 只允许出现在 ``backend/tests/*.py`` 的内存库**，
``scripts/`` 等运行时代码不得出现 ``sqlite`` / ``create_engine``。
本运行器要走 ``generate_next_question``（要读会话行 / 上下文）⇒ 必须有库
⇒ 只能放这里。**不带 ``test_`` 前缀，不是回归套件，不会进回归循环。**

运行
----
``python backend/tests/rag_baseline_run.py``
``python backend/tests/rag_baseline_run.py --scenario s1-mysql-index-mid``

会调用**真实**星火 X1（``main.spark_api``）；结果写入 ``backend/scripts/rag_baseline.json``。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

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
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

BENCHMARK_PATH = BACKEND_DIR / "scripts" / "interview_eval_benchmark.json"
CORPUS_PATH = BACKEND_DIR / "scripts" / "interview_knowledge.json"
NO_RAG_PATH = BACKEND_DIR / "scripts" / "no_rag_baseline.json"
RESULT_PATH = BACKEND_DIR / "scripts" / "rag_baseline.json"

#: 本臂的固定条件（唯一变量 = 开启 RAG）。
ARM = {
    "id": "rag_on",
    "use_rag": True,
    "retriever": None,
    "knowledge_injected": True,
}

#: 用户词汇 → 代码字段名（与无 RAG 记录一致）。
FIELD_MAP = {
    "question": "question",
    "type": "question_type",
    "difficulty": "difficulty",
    "stage": "stage",
    "reason": "reason",
}
RECORDED_FIELDS = ("question", "question_type", "difficulty", "stage", "reason")

#: 知识小节的边界（与 question_knowledge.txt 模板一致）。
KNOWLEDGE_HEADING = "## 四·五、参考知识（外部检索结果，可选）"
KNOWLEDGE_END = "## 五、本场进度（用于避免重复）"


# ============================================================
# 一、真实星火客户端（计数包装）
# ============================================================
class CountingSpark:
    """薄包装：原样转发 ``chat_async``，只数调用次数（含修复请求）。"""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.calls = 0

    async def chat_async(self, message: str) -> str:
        self.calls += 1
        return await self._inner.chat_async(message)


def _real_spark() -> Any:
    from main import spark_api  # noqa: PLC0415 - 刻意延迟导入

    return spark_api


def _describe_spark(spark: Any) -> Dict[str, Any]:
    inner = getattr(spark, "_inner", spark)
    return {
        "provider": "iflytek-spark",
        "endpoint": f"wss://{getattr(inner, 'host', '?')}{getattr(inner, 'path', '?')}",
        "model": "x1（推理模型）",
        "nondeterministic": True,
    }


# ============================================================
# 二、捕获三处（与无 RAG 同一手法：包一层模块属性）
# ============================================================
def _jsonable(value: Any) -> Any:
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except (TypeError, ValueError):
        if isinstance(value, (list, tuple)):
            return [_jsonable(v) for v in value]
        return repr(value)


def _chunk_dict(chunk: Any) -> Dict[str, Any]:
    """把检索器返回的 KnowledgeChunk 归一成可 JSON 的形状。"""
    to_dict = getattr(chunk, "to_dict", None)
    if callable(to_dict):
        data = to_dict()
        return {
            "content": data.get("content", ""),
            "source": data.get("source", ""),
            "metadata": _jsonable(data.get("metadata", {})),
        }
    if isinstance(chunk, dict):
        return {
            "content": chunk.get("content", ""),
            "source": chunk.get("source", ""),
            "metadata": _jsonable(chunk.get("metadata", {})),
        }
    return {"content": repr(chunk), "source": "", "metadata": {}}


@asynccontextmanager
async def _capture_rag() -> Iterator[Dict[str, Any]]:
    """捕获「检索器配置 / 检索器实际返回 / 最终 Prompt」三处。

    - ``resolve_retriever``：拿到**组装出来的真实检索器**及其配置
    - ``retrieve_knowledge``：拿到**检索器的实际返回**（全项目唯一接线点）
    - ``generate_question``：拿到 Agent 实际收到的 ``knowledge_context`` 与最终 Prompt
    """
    captured: Dict[str, Any] = {
        "retriever": None,
        "retriever_warnings": [],
        "retrieve_calls": [],
        "agent_calls": 0,
    }

    original_resolve = interview_core.resolve_retriever
    original_retrieve = interview_core.retrieve_knowledge
    original_generate = interview_agent.generate_question

    def resolve_wrapper(db: Any, retriever: Any = None,
                        use_rag: bool = False, *,
                        retriever_kwargs: Any = None) -> Any:
        # ``retriever_kwargs`` 是任务 70（R1）新增的 keyword-only 透传参数：
        # 本运行器不传它（``None`` ⇒ 行为与 R1 之前逐字节相同），
        # 但**必须**在替身签名里接住，否则 Core 调用时会 TypeError。
        built, warnings = original_resolve(db, retriever, use_rag,
                                           retriever_kwargs=retriever_kwargs)
        captured["retriever"] = built
        captured["retriever_warnings"] = list(warnings)
        captured["retriever_kwargs"] = retriever_kwargs
        return built, warnings

    async def retrieve_wrapper(job: Any = None, topic: Any = "",
                               context: Any = None, *,
                               retriever: Any = None) -> List[Any]:
        chunks = await original_retrieve(job, topic, context, retriever=retriever)
        captured["retrieve_calls"].append({
            "topic": topic,
            "job_is_mapping": isinstance(job, dict),
            "job_repr": _jsonable(job) if isinstance(job, dict) else repr(job),
            "returned": [_chunk_dict(c) for c in (chunks or [])],
        })
        return chunks

    async def generate_wrapper(context: Any, plan: Any = None, resume: Any = None,
                               job: Any = None, *, knowledge_context: Any = None,
                               spark: Any = None) -> Dict[str, Any]:
        captured["agent_calls"] += 1
        captured["knowledge_context"] = knowledge_context
        variables = build_question_variables(context, plan, resume, job)
        captured["variables"] = variables
        template, prompt = interview_agent.render_question_prompt(
            variables, knowledge_context)
        captured["prompt_template"] = template
        captured["prompt"] = prompt
        return await original_generate(context, plan, resume, job,
                                       knowledge_context=knowledge_context, spark=spark)

    import inspect  # noqa: PLC0415

    captured["signature_preserved"] = (
        list(inspect.signature(generate_wrapper).parameters)
        == list(inspect.signature(original_generate).parameters))
    # ★ 替身必须与被替换函数**同签名**：任务 70 加 ``retriever_kwargs`` 时，
    #   本文件的 ``resolve_wrapper`` 漏跟进过一次（运行时 TypeError），
    #   而它不在回归循环里 ⇒ 靠这条断言把同类问题钉在运行期。
    captured["resolve_signature_preserved"] = (
        list(inspect.signature(resolve_wrapper).parameters)
        == list(inspect.signature(original_resolve).parameters))
    if not captured["resolve_signature_preserved"]:
        raise AssertionError(
            "resolve_retriever 替身签名与被替换函数不一致："
            f"{list(inspect.signature(resolve_wrapper).parameters)} vs "
            f"{list(inspect.signature(original_resolve).parameters)}")

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
# 三、按场景声明建一套真实前置数据（含语料入库 —— 开启 RAG 的前提）
# ============================================================
def _load_corpus() -> List[Dict[str, Any]]:
    data = json.loads(CORPUS_PATH.read_text(encoding="utf-8"))
    return list(data["documents"])


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

        session.add(User(username="rag_baseline", email="rag_baseline@example.com",
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

        # ---- 开启 RAG 的**前提**：语料必须已入库（否则检索恒空）----
        # 注意：这只是让「RAG 有东西可检索」，不改变上面任何固定输入。
        embedder = default_embedder()
        store = SqlAlchemyVectorStore(session)
        pipeline = KnowledgeImportPipeline(session, embedder=embedder, store=store)
        reports: List[Dict[str, Any]] = []
        for doc in _load_corpus():
            reports.append(await pipeline.import_document(dict(doc)))
        chunks_in_store = await store.count()

        yield {
            "db": session, "row": row, "session_id": session_id,
            "job": job, "resume": resume, "plan": plan, "context": context,
            "reports": reports, "chunks_in_store": chunks_in_store,
            "embedder": embedder,
        }
    finally:
        if session is not None:
            await session.close()
        await engine.dispose()


# ============================================================
# 四、从真实 Prompt 里截出知识小节
# ============================================================
def _knowledge_block(prompt: str) -> Optional[str]:
    if KNOWLEDGE_HEADING not in prompt or KNOWLEDGE_END not in prompt:
        return None
    start = prompt.index(KNOWLEDGE_HEADING)
    end = prompt.index(KNOWLEDGE_END)
    return prompt[start:end]


def _knowledge_body(block: Optional[str]) -> str:
    """去掉标题行与首尾空行，得到**注入正文**。"""
    if not block:
        return ""
    lines = block.splitlines()
    if lines and lines[0].strip() == KNOWLEDGE_HEADING:
        lines = lines[1:]
    return "\n".join(lines).strip()


def _retriever_config(retriever: Any) -> Optional[Dict[str, Any]]:
    if retriever is None:
        return None
    return {
        "class": type(retriever).__name__,
        "source_name": getattr(retriever, "source_name", None),
        "model_name": getattr(retriever, "model_name", None),
        "top_k": getattr(retriever, "top_k", None),
        "min_score": getattr(retriever, "min_score", None),
        "category": getattr(retriever, "category", None),
        "document_id": getattr(retriever, "document_id", None),
        "dedup": getattr(retriever, "dedup", None),
        "queries": list(getattr(retriever, "queries", []) or []),
        "built_by": "interview_core.resolve_retriever(db, None, True) "
                    "-> knowledge_rag.build_vector_retriever(db)",
    }


# ============================================================
# 五、跑一个场景
# ============================================================
async def run_scenario(scenario: Dict[str, Any], spark: CountingSpark,
                       no_rag_runs: Dict[str, Any]) -> Dict[str, Any]:
    sid = scenario["id"]
    print(f"\n[场景 {sid}] {scenario['label']}")

    async with _env(scenario) as env:
        async with _capture_rag() as captured:
            started = time.perf_counter()
            result = await interview_core.generate_next_question(
                env["db"], env["session_id"],
                retriever=None,          # 与无 RAG 相同：不显式注入
                use_rag=True,            # ★ 唯一变化
                spark=spark,
            )
            elapsed = time.perf_counter() - started

    variables = captured["variables"]
    calls = captured["retrieve_calls"]
    chunks = calls[-1]["returned"] if calls else []
    topic = calls[-1]["topic"] if calls else None
    block = _knowledge_block(captured.get("prompt") or "")
    raw_context = captured.get("knowledge_context")
    lines = interview_agent.normalize_knowledge_context(raw_context)

    fixed_inputs = {
        "interview_plan": variables["interview_plan"],
        "user_profile": {
            "job": {
                "job_name": variables["job_title"],
                "description": variables["job_description"],
            },
            "resume_summary": variables["resume_summary"],
        },
        "difficulty": variables["difficulty"],
        "history_questions": list(variables["asked_questions"]),
        "current_stage": variables["current_stage"],
    }

    # ★ 可比较性断言：固定输入必须与无 RAG 基准**逐键相同**
    baseline = no_rag_runs.get(sid)
    same_as_no_rag = baseline is not None and baseline.get("fixed_inputs") == fixed_inputs

    record = {
        "scenario_id": sid,
        "label": scenario["label"],
        "input": fixed_inputs,
        "rag": {
            "enabled": True,
            "use_rag": True,
            "retriever": _retriever_config(captured["retriever"]),
            "retriever_warnings": list(captured["retriever_warnings"]),
            "query": {
                "topic": topic,
                "retrieve_calls": len(calls),
            },
            "retrieved_chunks": chunks,
            "retrieved_count": len(chunks),
            "knowledge_context": _knowledge_body(block),
            "knowledge_context_lines": list(lines),
            "knowledge_block_present": block is not None,
            "knowledge_block_chars": len(block or ""),
            "prompt_template": captured.get("prompt_template"),
            "prompt_template_file": "prompts/interview/question_knowledge.txt",
        },
        "output": {field: result.get(field) for field in RECORDED_FIELDS},
        "full_output": dict(result),
        "field_map": {"type": "question_type"},
        "provenance": {
            "ok": result["ok"],
            "errors": list(result["errors"]),
            "warnings": list(result["warnings"]),
            "error": result["error"],
            "question_no": result["question_no"],
            "prompt_template": captured.get("prompt_template"),
            "prompt_chars": len(captured.get("prompt") or ""),
            "knowledge_context_raw_type": type(raw_context).__name__,
            "knowledge_context_is_empty": not raw_context,
            "agent_calls": captured["agent_calls"],
            "signature_preserved": captured["signature_preserved"],
            "spark_calls_this_scenario": None,   # 由调用方填（差量）
            "elapsed_seconds": round(elapsed, 2),
            "corpus_documents": len(env["reports"]),
            "corpus_chunks_in_store": env["chunks_in_store"],
            "input_matches_no_rag_baseline": same_as_no_rag,
        },
    }

    print(f"  ok={result['ok']}  题号={result['question_no']}  "
          f"模板={record['rag']['prompt_template']}  "
          f"检索 {len(chunks)} 条  耗时={record['provenance']['elapsed_seconds']}s")
    print(f"  检索 topic={topic!r}  检索器 top_k={record['rag']['retriever']['top_k']} "
          f"min_score={record['rag']['retriever']['min_score']}")
    print(f"  知识上下文是否为空：{record['provenance']['knowledge_context_is_empty']}"
          f"（{record['rag']['knowledge_block_chars']} 字知识小节）")
    print(f"  固定输入与无 RAG 基准逐键相同：{same_as_no_rag}")
    if result["ok"]:
        print(f"  question     : {result['question']}")
        print(f"  question_type: {result['question_type']}")
        print(f"  difficulty   : {result['difficulty']}")
        print(f"  stage        : {result['stage']}")
        print(f"  reason       : {result['reason']}")
    else:
        print(f"  失败：errors={result['errors']} error={result['error']}")
    return record


# ============================================================
# 六、主流程
# ============================================================
def _load_scenarios(only: Optional[List[str]]) -> List[Dict[str, Any]]:
    data = json.loads(BENCHMARK_PATH.read_text(encoding="utf-8"))
    scenarios = data["scenarios"]
    if only:
        wanted = set(only)
        scenarios = [s for s in scenarios if s["id"] in wanted]
        missing = wanted - {s["id"] for s in scenarios}
        if missing:
            raise SystemExit(f"未知场景 id：{sorted(missing)}")
    return scenarios


def _load_no_rag_runs() -> Dict[str, Any]:
    if not NO_RAG_PATH.exists():
        return {}
    data = json.loads(NO_RAG_PATH.read_text(encoding="utf-8"))
    return {r["scenario_id"]: r for r in data.get("runs", [])}


async def main() -> int:
    parser = argparse.ArgumentParser(description="有 RAG 基准运行器")
    parser.add_argument("--scenario", action="append", default=None,
                        help="只跑指定场景 id（可重复）；缺省跑全部")
    parser.add_argument("--out", default=str(RESULT_PATH))
    args = parser.parse_args()

    scenarios = _load_scenarios(args.scenario)
    no_rag_runs = _load_no_rag_runs()
    spark = CountingSpark(_real_spark())

    print("=" * 74)
    print("有 RAG 基准 · 运行（use_rag=True；与无 RAG 臂仅此一处不同）")
    print("=" * 74)
    print(f"  条件来源：{BENCHMARK_PATH.relative_to(PROJECT_DIR)}")
    print(f"  对照基线：{NO_RAG_PATH.relative_to(PROJECT_DIR)}"
          f"（{len(no_rag_runs)} 个场景）")
    print(f"  模型：{_describe_spark(spark)['endpoint']}（真实调用）")
    print(f"  场景数：{len(scenarios)}")

    runs: List[Dict[str, Any]] = []
    for scenario in scenarios:
        before = spark.calls
        record = await run_scenario(scenario, spark, no_rag_runs)
        record["provenance"]["spark_calls_this_scenario"] = spark.calls - before
        runs.append(record)

    payload = {
        "name": "interview-rag-baseline",
        "version": 1,
        "purpose": "建立「开启 RAG」的 AI 面试问题生成实验数据，与无 RAG 基准"
                   "（no_rag_baseline.json）逐字段可比较。只记录，不评价生成质量。",
        "scope": {
            "evaluates_quality": False,
            "modifies_code": False,
            "arm": ARM["id"],
            "use_rag": ARM["use_rag"],
            "retriever": ARM["retriever"],
            "knowledge_injected": ARM["knowledge_injected"],
        },
        "conditions": {
            "fixed": ["interview_session", "interview_plan", "user_profile",
                      "current_stage", "difficulty", "history_questions"],
            "the_only_change": "use_rag=True（其余调用参数与无 RAG 臂完全相同）",
            "source": str(BENCHMARK_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
            "corpus": str(CORPUS_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
            "prompt_template_key": "question_knowledge",
            "prompt_template_file": "prompts/interview/question_knowledge.txt",
            "vector_backend": "sql（VECTOR_STORE 未配置 ⇒ 默认 SqlAlchemyVectorStore）",
            "embedder": "default_embedder()（未配置 EMBEDDING_* ⇒ 离线占位 hash-local）",
            "llm": _describe_spark(spark),
            "entry_point": "interview_core.generate_next_question(retriever=None, use_rag=True)",
        },
        "comparability": {
            "baseline_file": str(NO_RAG_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
            "checked_key": "input == 无 RAG 记录的 fixed_inputs（逐键相同）",
            "per_scenario": {
                r["scenario_id"]: r["provenance"]["input_matches_no_rag_baseline"]
                for r in runs
            },
        },
        "field_map": {"type": "question_type"},
        "recorded_fields": list(RECORDED_FIELDS),
        "runs": runs,
        "summary": {
            "total": len(runs),
            "ok": sum(1 for r in runs if r["provenance"]["ok"]),
            "failed": sum(1 for r in runs if not r["provenance"]["ok"]),
            "spark_calls_total": spark.calls,
            "all_inputs_match_no_rag": all(
                r["provenance"]["input_matches_no_rag_baseline"] for r in runs),
            "knowledge_injected_all": all(
                not r["provenance"]["knowledge_context_is_empty"] for r in runs),
            "retrieved_counts": {
                r["scenario_id"]: r["rag"]["retrieved_count"] for r in runs},
            "templates_used": sorted({
                r["rag"]["prompt_template"] for r in runs}),
        },
    }

    out_path = Path(args.out)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                        encoding="utf-8")

    print("\n" + "=" * 74)
    print("结果")
    print("=" * 74)
    print(f"  成功 {payload['summary']['ok']} / 失败 {payload['summary']['failed']}")
    print(f"  星火调用总次数：{spark.calls}")
    print(f"  固定输入全部与无 RAG 基准一致："
          f"{payload['summary']['all_inputs_match_no_rag']}")
    print(f"  知识全部成功注入：{payload['summary']['knowledge_injected_all']}")
    print(f"  各场景检索条数：{payload['summary']['retrieved_counts']}")
    print(f"  用到的模板：{payload['summary']['templates_used']}")
    try:
        shown = out_path.resolve().relative_to(PROJECT_DIR)
    except ValueError:
        shown = out_path
    print(f"  已写入：{shown}")

    return 0 if payload["summary"]["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
