# -*- coding: utf-8 -*-
"""无 RAG 基准运行器 —— 关闭 RAG 跑一次真实出题，把结果落盘成基准记录。

用途
----
建立「``use_rag=False``」的 AI 面试问题生成**基准结果**，供后续与「开启 RAG」
对照。**只记录，不评价好坏**；**不修改任何业务代码**。

固定条件（四个，全部取自既有的评估基准，单一来源，不在这里另抄一份）
--------------------------------------------------------------------
- **面试计划**（interview_plan）：由真实 Planner 按会话行配置产出（``use_llm=False``）
- **用户信息**（user_profile）：``job``（岗位行）+ ``resume``（简历正文）
- **难度**（difficulty）：会话行声明的难度
- **历史问题**（history_questions）：``context.asked_questions``

条件来源：``backend/scripts/interview_eval_benchmark.json``
（该文件由 ``tests/test_interview_eval_benchmark.py`` 逐项自检；条件漂移会先在那里红。）

记录字段（Agent 生成结果）
--------------------------
``{question, question_type, difficulty, stage, reason}``
—— 用户口中的 ``type`` 即代码里的 ``question_type``，映射表写在记录的 ``field_map`` 里。

为什么这个文件在 ``tests/`` 而不是 ``scripts/``
-----------------------------------------------
项目硬约束：**SQLite 只允许出现在 ``backend/tests/*.py`` 的内存库**，
``scripts/`` 等运行时代码不得出现 ``sqlite`` / ``create_engine``。
本运行器需要 ``generate_next_question``（它要读会话行 / 上下文）⇒ 必须有库
⇒ 只能放这里。**它不带 ``test_`` 前缀，不是回归套件，不会进回归循环。**

运行
----
``python backend/tests/no_rag_baseline_run.py``               # 全部固定场景
``python backend/tests/no_rag_baseline_run.py --scenario s1-mysql-index-mid``

会调用**真实**星火 X1（``main.spark_api``），单次约十几秒；结果写入
``backend/scripts/no_rag_baseline.json``。
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

BENCHMARK_PATH = BACKEND_DIR / "scripts" / "interview_eval_benchmark.json"
RESULT_PATH = BACKEND_DIR / "scripts" / "no_rag_baseline.json"

#: 本臂的固定条件（唯一变量 = 不注入知识）。
ARM = {
    "id": "rag_off",
    "use_rag": False,
    "retriever": None,
    "knowledge_injected": False,
}

#: 用户词汇 → 代码字段名。用户说 ``type``，代码里叫 ``question_type``。
FIELD_MAP = {
    "question": "question",
    "type": "question_type",
    "difficulty": "difficulty",
    "stage": "stage",
    "reason": "reason",
}
RECORDED_FIELDS = ("question", "question_type", "difficulty", "stage", "reason")


# ============================================================
# 一、真实星火客户端（计数包装，只为记录调用次数）
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
    """取 ``main.spark_api``（真实讯飞星火 X1）。"""
    from main import spark_api  # noqa: PLC0415 - 刻意延迟导入（main 会建 FastAPI 应用）

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
# 二、捕获真实入参（与任务 64 同一手法：包一层 generate_question）
# ============================================================
@asynccontextmanager
async def _capture_agent_call() -> Iterator[Dict[str, Any]]:
    """捕获 Core 实际传给 Agent 的入参与最终 Prompt。

    之所以可行：Core 在**函数体内**导入 ``generate_question``
    ⇒ 运行时按模块属性解析，替换模块属性即生效。
    """
    original = interview_agent.generate_question
    captured: Dict[str, Any] = {"calls": 0}

    async def wrapper(context: Any, plan: Any = None, resume: Any = None,
                      job: Any = None, *, knowledge_context: Any = None,
                      spark: Any = None) -> Dict[str, Any]:
        captured["calls"] += 1
        captured["knowledge_context"] = knowledge_context
        variables = build_question_variables(context, plan, resume, job)
        captured["variables"] = variables
        template, prompt = interview_agent.render_question_prompt(
            variables, knowledge_context)
        captured["prompt_template"] = template
        captured["prompt"] = prompt
        return await original(context, plan, resume, job,
                              knowledge_context=knowledge_context, spark=spark)

    captured["signature_preserved"] = (
        list(__import__("inspect").signature(wrapper).parameters)
        == list(__import__("inspect").signature(original).parameters))
    interview_agent.generate_question = wrapper
    try:
        yield captured
    finally:
        interview_agent.generate_question = original


# ============================================================
# 三、按场景声明建一套真实前置数据（只 Mock 大模型；不导入语料）
# ============================================================
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

        session.add(User(username="no_rag_baseline", email="no_rag_baseline@example.com",
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

        # 真实 Planner（与流程内部同一个函数 `_build_session_plan`，use_llm=False ⇒ 确定性）
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

        yield {
            "db": session, "row": row, "session_id": session_id,
            "job": job, "resume": resume, "plan": plan, "context": context,
        }
    finally:
        if session is not None:
            await session.close()
        await engine.dispose()


def _jsonable(value: Any) -> Any:
    """尽力转成可 JSON 序列化的形状；失败则退回 ``repr``。"""
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except (TypeError, ValueError):
        if isinstance(value, (list, tuple)):
            return [_jsonable(v) for v in value]
        return repr(value)


# ============================================================
# 四、跑一个场景
# ============================================================
async def run_scenario(scenario: Dict[str, Any], spark: CountingSpark) -> Dict[str, Any]:
    sid = scenario["id"]
    print(f"\n[场景 {sid}] {scenario['label']}")

    async with _env(scenario) as env:
        async with _capture_agent_call() as captured:
            started = time.perf_counter()
            result = await interview_core.generate_next_question(
                env["db"], env["session_id"],
                retriever=None,          # 本臂：不注入检索器
                use_rag=False,           # 本臂：不组装真实 RAG 链路
                spark=spark,
            )
            elapsed = time.perf_counter() - started

    variables = captured["variables"]
    knowledge_context = captured.get("knowledge_context")

    record = {
        "scenario_id": sid,
        "label": scenario["label"],
        "fixed_inputs": {
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
        },
        "result": {field: result.get(field) for field in RECORDED_FIELDS},
        "full_result": dict(result),
        "provenance": {
            "ok": result["ok"],
            "errors": list(result["errors"]),
            "warnings": list(result["warnings"]),
            "error": result["error"],
            "question_no": result["question_no"],
            "prompt_template": captured.get("prompt_template"),
            "prompt_chars": len(captured.get("prompt") or ""),
            "knowledge_context_passed": _jsonable(knowledge_context),
            "knowledge_context_is_empty": not knowledge_context,
            "agent_calls": captured["calls"],
            "signature_preserved": captured["signature_preserved"],
            "spark_calls_this_scenario": None,   # 由调用方填（差量）
            "elapsed_seconds": round(elapsed, 2),
        },
    }

    print(f"  ok={result['ok']}  题号={result['question_no']}  "
          f"模板={record['provenance']['prompt_template']}  "
          f"耗时={record['provenance']['elapsed_seconds']}s")
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
# 五、主流程
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


async def main() -> int:
    parser = argparse.ArgumentParser(description="无 RAG 基准运行器")
    parser.add_argument("--scenario", action="append", default=None,
                        help="只跑指定场景 id（可重复）；缺省跑全部")
    parser.add_argument("--out", default=str(RESULT_PATH))
    args = parser.parse_args()

    scenarios = _load_scenarios(args.scenario)
    inner = _real_spark()
    spark = CountingSpark(inner)

    print("=" * 74)
    print("无 RAG 基准 · 运行（use_rag=False，不注入任何检索知识）")
    print("=" * 74)
    print(f"  条件来源：{BENCHMARK_PATH.relative_to(PROJECT_DIR)}")
    print(f"  模型：{_describe_spark(spark)['endpoint']}（真实调用）")
    print(f"  场景数：{len(scenarios)}")

    runs: List[Dict[str, Any]] = []
    for scenario in scenarios:
        before = spark.calls
        record = await run_scenario(scenario, spark)
        record["provenance"]["spark_calls_this_scenario"] = spark.calls - before
        runs.append(record)

    payload = {
        "name": "interview-no-rag-baseline",
        "version": 1,
        "purpose": "建立「关闭 RAG」的 AI 面试问题生成基准结果，供后续与「开启 RAG」对照。"
                   "只记录，不评价好坏。",
        "scope": {
            "evaluates_quality": False,
            "modifies_code": False,
            "arm": ARM["id"],
            "use_rag": ARM["use_rag"],
            "retriever": ARM["retriever"],
            "knowledge_injected": ARM["knowledge_injected"],
        },
        "conditions": {
            "fixed": ["interview_plan", "user_profile", "difficulty", "history_questions"],
            "source": str(BENCHMARK_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
            "prompt_template_key": "question",
            "prompt_template_file": "prompts/interview/question.txt",
            "template_files": {
                "question": "prompts/interview/question.txt",
                "question_knowledge": "prompts/interview/question_knowledge.txt",
            },
            "llm": _describe_spark(spark),
            "entry_point": "interview_core.generate_next_question(retriever=None, use_rag=False)",
        },
        "field_map": FIELD_MAP,
        "recorded_fields": list(RECORDED_FIELDS),
        "runs": runs,
        "summary": {
            "total": len(runs),
            "ok": sum(1 for r in runs if r["provenance"]["ok"]),
            "failed": sum(1 for r in runs if not r["provenance"]["ok"]),
            "spark_calls_total": spark.calls,
            "all_knowledge_empty": all(
                r["provenance"]["knowledge_context_is_empty"] for r in runs),
            "templates_used": sorted({
                r["provenance"]["prompt_template"] for r in runs}),
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
    print(f"  知识上下文全为空：{payload['summary']['all_knowledge_empty']}")
    print(f"  用到的模板：{payload['summary']['templates_used']}")
    try:
        shown = out_path.resolve().relative_to(PROJECT_DIR)
    except ValueError:
        shown = out_path
    print(f"  已写入：{shown}")

    return 0 if payload["summary"]["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
