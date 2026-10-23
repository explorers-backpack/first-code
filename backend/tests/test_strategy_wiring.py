# -*- coding: utf-8 -*-
"""AI 模拟面试 · 出题**策略层接线**自检（方案 B：主流程走出题策略层）

无需 pytest，直接运行：
    python backend/tests/test_strategy_wiring.py

不依赖本机 MySQL：DATABASE_URL 指向 SQLite 内存库 + StaticPool。
**不调用真实 LLM、不联网**：agent 模式一律注入 Mock Spark。

被测对象（出题策略层**首次进入生产链路**的接线）
--------------------------------------------------
- ``interview_service.begin_session_with_questions`` —— 公开落库原语（接收给定题目）
- ``interview_orchestrator.start_questions``          —— 策略化「开始面试」入口
- ``api/interview_ws._handle_start``                  —— 调用点切到策略化入口

覆盖范围
--------
[1] **等价性**：``start_questions(mode="rule")`` 落库题目 == ``build_question_plan``
    逐字段；且与 ``orch.start``（规则主流程）的交付形状一致
[2] 原语契约：协程 / 空题目 500 / 非本人 404 / 已 ongoing 400（与 ``start_session`` 同口径）
[3] **兜底**：``mode="agent"`` 生成失败 ⇒ 规则策略接管，题目照常落库
[4] agent 成功：单题落库（占号语义：question_no == 1）
[5] 非法 ``mode``：抛 ``UnknownModeError`` 且**状态回滚**、库里不留脏数据
[6] 接线取证（AST / 源码）：``_handle_start`` 走策略化入口；编排层**顶层**依赖未新增边；
    策略层在**函数体内延迟导入**
[7] 默认路径未动：``start_session`` 仍直调 ``build_question_plan``；HTTP 契约不出现新入口
"""

import asyncio
import ast
import inspect
import json
import os
import pathlib
import sys

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from fastapi import HTTPException  # noqa: E402
from sqlalchemy import func, select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import InterviewQuestion, InterviewSession, Job, User  # noqa: E402
from schemas.interview import SessionCreateRequest  # noqa: E402
from services import (  # noqa: E402
    interview_core,
    interview_orchestrator as orch,
    interview_service,
    question_generator as qg,
)
from services.resume_scoring import extract_skills  # noqa: E402

_PASSED = 0
_FAILED = 0

JOB_SKILLS = "Python,MySQL,Redis,Kafka,Docker"

#: 与 ``test_question_generator`` 同款：合法的 Agent 单题输出。
QUESTION_JSON = json.dumps(
    {
        "question": "请介绍一下你在项目中是如何使用 Redis 做缓存的？",
        "question_type": "technical",
        "topic": "Redis",
        "difficulty": "mid",
        "expected_points": ["缓存穿透", "过期策略"],
        "reason": "考察缓存实践经验",
    },
    ensure_ascii=False,
)

#: 落库题目的 6 个字段（与 ``build_question_plan`` / ``QUESTION_ITEM_FIELDS`` 一致）。
ITEM_KEYS = ("question_no", "question", "question_type", "topic", "difficulty",
             "expected_points")


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name
          + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def _top_level_imports(source: str) -> set:
    """只取**模块顶层** import（函数体内的延迟导入不算）。"""
    names: set = set()
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _all_imports(source: str) -> set:
    """**全量** import（含函数体内延迟导入）——判「从不 import X」必须走 AST。"""
    names: set = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


class MockSpark:
    """按顺序吐出预设回复的假 Spark；超出预设次数即报错。"""

    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls: list = []

    async def chat_async(self, message: str) -> str:
        self.calls.append(message)
        if not self.replies:
            raise AssertionError("MockSpark 被调用次数超出预期")
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed(db: AsyncSession):
    user = User(username="sw_t", email="sw_t@example.com", password_hash="x", role="user")
    other = User(username="sw_o", email="sw_o@example.com", password_hash="x", role="user")
    job = Job(
        job_name="后端开发工程师",
        salary="20-35K",
        edu_require="本科",
        major_require="不限",
        skills=JOB_SKILLS,
        duty="负责后端服务的设计与开发",
        city="深圳",
        industry="互联网",
    )
    db.add_all([user, other, job])
    await db.commit()
    await db.refresh(user)
    await db.refresh(other)
    await db.refresh(job)
    return user.id, other.id, job.id


async def _new_session(db: AsyncSession, user_id: int, job_id: int, total: int = 4) -> int:
    """建会话（``created``，**不 start**）——留给被测入口去开始。"""
    payload = SessionCreateRequest(job_id=job_id, total_questions=total)
    return (await interview_service.create_session(db, user_id, payload))["session"]["id"]


async def _expected_rule_plan(db: AsyncSession, session_id: int):
    """独立复算规则出题结果（直接调 ``build_question_plan``，不经任何策略）。"""
    session = await interview_core.load_session_row(db, session_id)
    job = await interview_core.load_job_row(db, session.job_id)
    resume = await interview_core.load_resume_row(db, session.resume_id)
    skills = extract_skills(resume.content) if resume is not None and resume.content else []
    return interview_core.build_question_plan(session, job, skills)


async def _stored_items(db: AsyncSession, session_id: int):
    """读库中落下的题目，投影成 6 字段（顺序按 question_no）。"""
    rows = (await db.execute(
        select(InterviewQuestion)
        .where(InterviewQuestion.session_id == session_id)
        .order_by(InterviewQuestion.question_no)
    )).scalars().all()
    return [
        {
            "question_no": r.question_no,
            "question": r.question,
            "question_type": r.question_type,
            "topic": r.topic,
            "difficulty": r.difficulty,
            "expected_points": list(r.expected_points or []),
        }
        for r in rows
    ]


async def _raises(coro_fn, exc_type):
    try:
        await coro_fn()
    except exc_type as exc:
        return True, exc
    except BaseException as exc:  # noqa: BLE001
        return False, exc
    return False, None


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 模拟面试 · 出题策略层接线自检（方案 B）")
    print("=" * 70)

    orch.clear_states()

    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        user_id, other_id, job_id = await _seed(db)

        # ------------------------------------------------------------
        # [1] 等价性：rule 策略 == 规则主流程
        # ------------------------------------------------------------
        print("\n[1] 等价性：start_questions(mode='rule') == 规则主流程")

        sid_rule = await _new_session(db, user_id, job_id, total=4)
        sid_ref = await _new_session(db, user_id, job_id, total=4)
        sid_plan = await _new_session(db, user_id, job_id, total=4)

        r_rule = await orch.start_questions(db, user_id, sid_rule, mode="rule")
        r_ref = await orch.start(db, user_id, sid_ref)

        _check("[1.1] 两条路径都到达 avatar_speaking",
               r_rule.get("state") == orch.STATE_AVATAR_SPEAKING
               and r_ref.get("state") == orch.STATE_AVATAR_SPEAKING,
               f"{r_rule.get('state')} / {r_ref.get('state')}")

        items_rule = await _stored_items(db, sid_rule)
        items_ref = await _stored_items(db, sid_ref)

        _check("[1.2] 落库题目**逐字段完全相同**（rule 策略 vs 规则主流程）",
               items_rule == items_ref, f"{items_rule[:1]} != {items_ref[:1]}")
        _check("  └ 题量为整场（不是单题）", len(items_rule) == 4, str(len(items_rule)))
        _check("  └ 题号 1..N 连续",
               [i["question_no"] for i in items_rule] == [1, 2, 3, 4],
               str([i["question_no"] for i in items_rule]))

        expected = await _expected_rule_plan(db, sid_plan)
        # 同一份配置下 build_question_plan 是确定性的 ⇒ 可跨会话直接比对
        _check("[1.3] 落库题目 == build_question_plan 直接复算结果",
               items_rule == [dict(x) for x in expected],
               f"{items_rule[:1]} != {expected[:1]}")

        _check("[1.4] 返回键集与 orch.start 一致（只是多了 state）",
               set(r_rule) == set(r_ref), f"{sorted(set(r_rule) ^ set(r_ref))}")
        _check("  └ message / job_name 一致",
               r_rule.get("message") == r_ref.get("message")
               and r_rule.get("job_name") == r_ref.get("job_name"))
        _check("  └ 首题 6 字段一致",
               {k: r_rule["question"][k] for k in ITEM_KEYS}
               == {k: r_ref["question"][k] for k in ITEM_KEYS})
        _check("  └ 会话状态字段一致（status / current_question_no / total_questions）",
               all(r_rule["session"][k] == r_ref["session"][k]
                   for k in ("status", "current_question_no", "total_questions")),
               str({k: (r_rule["session"][k], r_ref["session"][k])
                    for k in ("status", "current_question_no", "total_questions")}))

        # ------------------------------------------------------------
        # [2] 原语契约：begin_session_with_questions
        # ------------------------------------------------------------
        print("\n[2] 原语契约：begin_session_with_questions")

        _check("[2.1] 是协程函数（可直接 await）",
               inspect.iscoroutinefunction(interview_service.begin_session_with_questions))

        sid_empty = await _new_session(db, user_id, job_id, total=3)
        ok_empty, exc_empty = await _raises(
            lambda: interview_service.begin_session_with_questions(
                db, user_id, sid_empty, []),
            HTTPException,
        )
        _check("[2.2] 空题目 ⇒ HTTPException 500（与 start_session 同口径）",
               ok_empty and getattr(exc_empty, "status_code", None) == 500,
               f"{type(exc_empty).__name__}: {exc_empty}")

        sid_other = await _new_session(db, user_id, job_id, total=3)
        ok_other, exc_other = await _raises(
            lambda: interview_service.begin_session_with_questions(
                db, other_id, sid_other, [{"question_no": 1, "question": "x",
                                           "question_type": "t", "topic": "t",
                                           "difficulty": "mid",
                                           "expected_points": []}]),
            HTTPException,
        )
        _check("[2.3] 非本人 ⇒ HTTPException 404（不泄露他人会话存在性）",
               ok_other and getattr(exc_other, "status_code", None) == 404,
               f"{type(exc_other).__name__}: {exc_other}")

        sid_started = await _new_session(db, user_id, job_id, total=3)
        await orch.start_questions(db, user_id, sid_started, mode="rule")
        ok_ongoing, exc_ongoing = await _raises(
            lambda: interview_service.begin_session_with_questions(
                db, user_id, sid_started, [{"question_no": 1, "question": "x",
                                            "question_type": "t", "topic": "t",
                                            "difficulty": "mid",
                                            "expected_points": []}]),
            HTTPException,
        )
        _check("[2.4] 已 ongoing ⇒ HTTPException 400（重复开始被拦下）",
               ok_ongoing and getattr(exc_ongoing, "status_code", None) == 400,
               f"{type(exc_ongoing).__name__}: {exc_ongoing}")

        # ------------------------------------------------------------
        # [3] 兜底：agent 失败 ⇒ 规则策略接管
        # ------------------------------------------------------------
        print("\n[3] 兜底：mode='agent' 生成失败 ⇒ 规则策略接管")

        sid_fb = await _new_session(db, user_id, job_id, total=4)
        bad_spark = MockSpark("这不是 JSON", "这也不是 JSON")
        r_fb = await orch.start_questions(
            db, user_id, sid_fb, mode="agent", spark=bad_spark
        )
        items_fb = await _stored_items(db, sid_fb)

        _check("[3.1] 不抛异常、状态照常推进到 avatar_speaking",
               r_fb.get("state") == orch.STATE_AVATAR_SPEAKING, str(r_fb.get("state")))
        _check("  └ Agent 确实被尝试过（坏 Spark 被调用）", len(bad_spark.calls) >= 1,
               str(len(bad_spark.calls)))
        _check("[3.2] 兜底后落库题目 == 规则计划（逐字段）",
               items_fb == [dict(x) for x in await _expected_rule_plan(db, sid_fb)],
               f"{items_fb[:1]}")
        _check("  └ 题量为整场 4 道（规则策略的产出粒度）",
               len(items_fb) == 4, str(len(items_fb)))

        # ------------------------------------------------------------
        # [4] agent 成功：单题落库
        # ------------------------------------------------------------
        print("\n[4] agent 成功：单题落库（占号语义）")

        sid_ag = await _new_session(db, user_id, job_id, total=4)
        good_spark = MockSpark(QUESTION_JSON)
        r_ag = await orch.start_questions(
            db, user_id, sid_ag, mode="agent", spark=good_spark
        )
        items_ag = await _stored_items(db, sid_ag)

        _check("[4.1] 状态推进到 avatar_speaking",
               r_ag.get("state") == orch.STATE_AVATAR_SPEAKING, str(r_ag.get("state")))
        _check("[4.2] 只落库 1 道题（Agent 的单题粒度）",
               len(items_ag) == 1, str(len(items_ag)))
        _check("  └ 题号为 1（created 会话从第 1 题起算）",
               items_ag and items_ag[0]["question_no"] == 1, str(items_ag[:1]))
        _check("  └ 题目内容来自模型输出（不是规则模板）",
               items_ag and items_ag[0]["question"] == json.loads(QUESTION_JSON)["question"],
               str(items_ag[:1]))
        _check("  └ 返回的首题与落库一致",
               {k: r_ag["question"][k] for k in ITEM_KEYS} == items_ag[0])

        # ------------------------------------------------------------
        # [5] 非法 mode：报错 + 状态回滚 + 不留脏数据
        # ------------------------------------------------------------
        print("\n[5] 非法 mode：报错且状态回滚")

        sid_bad = await _new_session(db, user_id, job_id, total=3)
        # 登记编排状态（真实链路里由 orch.create_session 完成；这里直接置位更聚焦）
        orch.set_state(sid_bad, orch.STATE_WAITING_QUESTION)

        ok_mode, exc_mode = await _raises(
            lambda: orch.start_questions(db, user_id, sid_bad, mode="nope"),
            qg.UnknownModeError,
        )
        _check("[5.1] 非法 mode 抛 UnknownModeError（编程错误，不静默）", ok_mode,
               f"{type(exc_mode).__name__}: {exc_mode}")
        _check("  └ 状态**回滚**到调用前（waiting_question）",
               orch.get_state(sid_bad) == orch.STATE_WAITING_QUESTION,
               str(orch.get_state(sid_bad)))

        row = (await db.execute(
            select(InterviewSession).where(InterviewSession.id == sid_bad)
        )).scalar_one()
        _check("  └ 会话仍为 created（没有被激活）", row.status == "created", row.status)
        _check("  └ 库里没有落任何题目",
               len(await _stored_items(db, sid_bad)) == 0)

        # ------------------------------------------------------------
        # [6] 接线取证（AST / 源码）
        # ------------------------------------------------------------
        print("\n[6] 接线取证（AST / 源码）")

        ws_src = (BACKEND_DIR / "modules" / "interview" / "ws.py").read_text(encoding="utf-8")
        _check("[6.1] WS 的 start 处理走策略化入口",
               "await orch.start_questions(" in ws_src)
        _check("  └ 不再调用旧的 orch.start(（避免两条路径并存）",
               "await orch.start(" not in ws_src)

        orch_src = (BACKEND_DIR / "services" / "interview_orchestrator.py").read_text(
            encoding="utf-8")
        top = _top_level_imports(orch_src)
        _check("[6.2] 编排层**顶层** services.* 仍是 interview_core / interview_service",
               {m for m in top if m.startswith("services.")}
               == {"services.interview_core", "services.interview_service"},
               str(sorted(m for m in top if m.startswith("services."))))
        _check("  └ 顶层无第三方 / 无 models / 无 database",
               {m.split(".")[0] for m in top}
               <= {"__future__", "contextlib", "threading", "typing", "services"},
               str(sorted({m.split(".")[0] for m in top})))
        _check("[6.3] 策略层在**函数体内**延迟导入（不进顶层依赖面）",
               "services.question_generator" not in top
               and "services.question_generator" in _all_imports(orch_src),
               str(sorted(_all_imports(orch_src))))

        # ------------------------------------------------------------
        # [7] 默认路径未动
        # ------------------------------------------------------------
        print("\n[7] 默认路径未动")

        start_src = inspect.getsource(interview_service.start_session)
        _check("[7.1] start_session 仍直调 build_question_plan",
               "build_question_plan" in start_src)
        _check("  └ start_session 不认识出题策略（源码不含策略层模块名）",
               "question_generator" not in start_src)
        _check("[7.2] HTTP 契约层（modules/interview/api.py）不出现策略化入口",
               "start_questions" not in (
                   (BACKEND_DIR / "modules" / "interview" / "api.py").read_text(encoding="utf-8")))
        _check("[7.3] 策略层不反向依赖 Service（不 import interview_service）",
               "services.interview_service" not in _all_imports(
                   inspect.getsource(qg)))

    await engine.dispose()

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
