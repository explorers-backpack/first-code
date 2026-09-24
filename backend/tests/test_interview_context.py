# -*- coding: utf-8 -*-
"""AI 模拟面试 · InterviewContext 上下文管理自检

无需 pytest，直接运行：
    python backend/tests/test_interview_context.py

不依赖本机 MySQL：DATABASE_URL 指向 SQLite 内存库 + StaticPool。
被测对象是 ``services.interview_context``，不经 HTTP 层。

覆盖范围（对应用户要求的 8 项能力）：
1. 创建初始 Context（含幂等、默认值、非法参数）
2. 获取 Context（9 个必需字段齐全；不存在时 404）
3. 更新 Context（白名单校验、阶段合法性）
4. 增加已提问问题（字符串/对象；允许重复）
5. 增加已覆盖知识点（去重）
6. 增加薄弱知识点（去重）
7. 增加追问次数（累加、非法值拦截）
8. 判断是否达到最大问题数量（含边界）
"""

import asyncio
import os
import pathlib
import sys

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

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
from models import INTERVIEW_STAGES, InterviewContext, InterviewSession, Job, User  # noqa: E402
from schemas.interview import SessionCreateRequest  # noqa: E402
from services import interview_context, interview_service  # noqa: E402

REQUIRED_FIELDS = (
    "session_id",
    "current_question_no",
    "current_stage",
    "asked_questions",
    "covered_topics",
    "weak_topics",
    "follow_up_count",
    "total_questions",
    "max_questions",
)

_PASSED = 0
_FAILED = 0


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (f"  -> {detail}" if detail and not cond else ""))
    return cond


async def _rejected(coro, status: int) -> bool:
    """断言协程抛出指定状态码的 HTTPException。"""
    try:
        await coro
        return False
    except HTTPException as exc:
        return exc.status_code == status


def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed(db: AsyncSession):
    user = User(username="ctx", email="ctx@example.com", password_hash="x", role="user")
    job = Job(
        job_name="后端开发工程师",
        salary="20-35K",
        edu_require="本科",
        major_require="不限",
        skills="Python,MySQL,Redis",
        duty="负责后端服务的设计与开发",
        city="深圳",
        industry="互联网",
    )
    db.add_all([user, job])
    await db.commit()
    await db.refresh(user)
    await db.refresh(job)
    return user.id, job.id


async def _new_session(db, user_id, job_id, total=5) -> int:
    """复用既有 interview_service 创建会话（验证 Context 确实挂在现有 Session 上）。"""
    data = await interview_service.create_session(
        db, user_id, SessionCreateRequest(job_id=job_id, total_questions=total)
    )
    return data["session"]["id"]


async def run() -> bool:
    print("=" * 70)
    print("AI 模拟面试 · InterviewContext 上下文管理自检")
    print("=" * 70)

    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        user_id, job_id = await _seed(db)

        # ============================================================
        # [1] 创建初始 Context
        # ============================================================
        print("\n[1] 创建初始 Context")
        sid = await _new_session(db, user_id, job_id, total=5)
        ctx = await interview_context.create_context(db, sid)

        _check("session_id 正确", ctx["session_id"] == sid, str(ctx["session_id"]))
        _check("current_stage 默认 introduction", ctx["current_stage"] == "introduction", ctx["current_stage"])
        _check("asked_questions 初始为空数组", ctx["asked_questions"] == [])
        _check("covered_topics 初始为空数组", ctx["covered_topics"] == [])
        _check("weak_topics 初始为空数组", ctx["weak_topics"] == [])
        _check("follow_up_count 初始为 0", ctx["follow_up_count"] == 0, str(ctx["follow_up_count"]))
        _check("current_question_no 投影自 session（0）", ctx["current_question_no"] == 0, str(ctx["current_question_no"]))
        _check("total_questions 投影自 session（5）", ctx["total_questions"] == 5, str(ctx["total_questions"]))
        _check("max_questions 默认取 total_questions", ctx["max_questions"] == 5, str(ctx["max_questions"]))
        _check("9 个必需字段齐全", all(k in ctx for k in REQUIRED_FIELDS),
               str([k for k in REQUIRED_FIELDS if k not in ctx]))

        # 幂等
        again = await interview_context.create_context(db, sid)
        _check("重复创建幂等（不覆盖）", again == ctx)
        n = (await db.execute(
            select(func.count()).select_from(InterviewContext).where(InterviewContext.session_id == sid)
        )).scalar()
        _check("数据库层仍只有 1 条上下文（1:1）", n == 1, str(n))

        # 非法参数
        _check("非法 stage 被拒（400）",
               await _rejected(interview_context.create_context(db, sid, stage="unknown"), 400))
        _check("max_questions<1 被拒（400）",
               await _rejected(interview_context.create_context(db, sid, max_questions=0), 400))
        _check("会话不存在时 404",
               await _rejected(interview_context.create_context(db, 999999), 404))

        # 显式 max_questions
        sid_cap = await _new_session(db, user_id, job_id, total=3)
        cap_ctx = await interview_context.create_context(db, sid_cap, max_questions=8)
        _check("可显式指定 max_questions（8）", cap_ctx["max_questions"] == 8, str(cap_ctx["max_questions"]))

        # ============================================================
        # [2] 获取 Context
        # ============================================================
        print("\n[2] 获取 Context")
        got = await interview_context.get_context(db, sid)
        _check("读取结果与创建一致", got == ctx)
        sid_noctx = await _new_session(db, user_id, job_id)
        _check("未创建上下文时 404",
               await _rejected(interview_context.get_context(db, sid_noctx), 404))
        _check("会话不存在时 404",
               await _rejected(interview_context.get_context(db, 999999), 404))

        # ============================================================
        # [3] 更新 Context
        # ============================================================
        print("\n[3] 更新 Context")
        for stage in INTERVIEW_STAGES:
            upd = await interview_context.update_context(db, sid, current_stage=stage)
            _check(f"阶段可切到 {stage}", upd["current_stage"] == stage, upd["current_stage"])

        _check("非法 stage 被拒（400）",
               await _rejected(interview_context.update_context(db, sid, current_stage="nope"), 400))
        _check("未知字段被拒（400）",
               await _rejected(interview_context.update_context(db, sid, unknown_field=1), 400))
        _check("空更新被拒（400）",
               await _rejected(interview_context.update_context(db, sid), 400))
        _check("非数组列表被拒（400）",
               await _rejected(interview_context.update_context(db, sid, covered_topics="Python"), 400))
        _check("follow_up_count 负数被拒（400）",
               await _rejected(interview_context.update_context(db, sid, follow_up_count=-1), 400))

        # ============================================================
        # [4] 增加已提问问题
        # ============================================================
        print("\n[4] 增加已提问问题")
        r = await interview_context.add_asked_question(db, sid, "请做个自我介绍")
        _check("字符串问题已追加", r["asked_questions"] == ["请做个自我介绍"], str(r["asked_questions"]))
        r = await interview_context.add_asked_question(
            db, sid, {"question_no": 2, "question": "谈谈 Python 实践", "stage": "technical"}
        )
        _check("对象型问题已追加", r["asked_questions"][1]["question_no"] == 2)
        _check("提问数变为 2", len(r["asked_questions"]) == 2, str(len(r["asked_questions"])))
        r = await interview_context.add_asked_question(db, sid, "请做个自我介绍")
        _check("允许重复提问（日志语义，不去重）", len(r["asked_questions"]) == 3, str(len(r["asked_questions"])))
        _check("空字符串被拒（400）",
               await _rejected(interview_context.add_asked_question(db, sid, "   "), 400))
        _check("非法类型被拒（400）",
               await _rejected(interview_context.add_asked_question(db, sid, 123), 400))

        # 落库校验：重新读取仍存在（证明 JSON 变更已持久化）
        persisted = await interview_context.get_context(db, sid)
        _check("提问记录已持久化（重读一致）", len(persisted["asked_questions"]) == 3,
               str(len(persisted["asked_questions"])))

        # ============================================================
        # [5][6] 增加已覆盖 / 薄弱知识点（去重）
        # ============================================================
        print("\n[5] 增加已覆盖知识点（去重）")
        r = await interview_context.add_covered_topic(db, sid, "Python")
        r = await interview_context.add_covered_topic(db, sid, "MySQL")
        r = await interview_context.add_covered_topic(db, sid, "Python")
        _check("已覆盖知识点去重", r["covered_topics"] == ["Python", "MySQL"], str(r["covered_topics"]))
        _check("空 topic 被拒（400）",
               await _rejected(interview_context.add_covered_topic(db, sid, "  "), 400))

        print("\n[6] 增加薄弱知识点（去重）")
        r = await interview_context.add_weak_topic(db, sid, "分布式事务")
        r = await interview_context.add_weak_topic(db, sid, "分布式事务")
        _check("薄弱知识点去重", r["weak_topics"] == ["分布式事务"], str(r["weak_topics"]))
        _check("薄弱与已覆盖互不影响",
               r["covered_topics"] == ["Python", "MySQL"] and r["weak_topics"] == ["分布式事务"])

        # ============================================================
        # [7] 增加追问次数
        # ============================================================
        print("\n[7] 增加追问次数")
        r = await interview_context.increment_follow_up(db, sid)
        _check("默认 +1", r["follow_up_count"] == 1, str(r["follow_up_count"]))
        r = await interview_context.increment_follow_up(db, sid, count=3)
        _check("可一次 +3（累计 4）", r["follow_up_count"] == 4, str(r["follow_up_count"]))
        _check("count<1 被拒（400）",
               await _rejected(interview_context.increment_follow_up(db, sid, count=0), 400))

        # ============================================================
        # [8] 判断是否达到最大问题数量
        # ============================================================
        print("\n[8] 判断是否达到最大问题数量")
        sid_max = await _new_session(db, user_id, job_id, total=3)
        await interview_context.create_context(db, sid_max)  # max_questions = 3
        _check("初始未达到上限", (await interview_context.is_max_questions_reached(db, sid_max)) is False)

        await interview_context.add_asked_question(db, sid_max, "Q1")
        _check("提问 1 次未达上限", (await interview_context.is_max_questions_reached(db, sid_max)) is False)
        await interview_context.add_asked_question(db, sid_max, "Q2")
        await interview_context.add_asked_question(db, sid_max, "Q3")
        _check("提问 3 次（=上限）判定为已达到",
               (await interview_context.is_max_questions_reached(db, sid_max)) is True)

        st = await interview_context.get_context_state(db, sid_max)
        _check("get_context_state 返回 max_reached=True", st["max_reached"] is True)
        _check("remaining_questions 归零", st["context"]["remaining_questions"] == 0,
               str(st["context"]["remaining_questions"]))

        # 追问可突破 total_questions，但受 max_questions 约束
        sid_fu = await _new_session(db, user_id, job_id, total=2)
        await interview_context.create_context(db, sid_fu, max_questions=4)
        await interview_context.add_asked_question(db, sid_fu, "Q1")
        await interview_context.add_asked_question(db, sid_fu, "Q2")
        _check("已问满 total_questions 但未达 max_questions",
               (await interview_context.is_max_questions_reached(db, sid_fu)) is False)
        await interview_context.add_asked_question(db, sid_fu, "追问 1")
        await interview_context.add_asked_question(db, sid_fu, "追问 2")
        _check("追问到 max_questions 后判定达到",
               (await interview_context.is_max_questions_reached(db, sid_fu)) is True)

        # ============================================================
        # [9] current_question_no 投影（以 session 为权威，不双写）
        # ============================================================
        print("\n[9] current_question_no 由 session 实时投影")
        sid_proj = await _new_session(db, user_id, job_id, total=5)
        await interview_context.create_context(db, sid_proj)
        row = await db.get(InterviewSession, sid_proj)
        row.current_question_no = 3
        await db.commit()
        proj = await interview_context.get_context(db, sid_proj)
        _check("session 题号变化后 Context 同步反映（3）",
               proj["current_question_no"] == 3, str(proj["current_question_no"]))
        _check("asked_count 取题号与日志的较大值（3）", proj["asked_count"] == 3, str(proj["asked_count"]))
        _check("未达上限（3 < 5）", proj["max_reached"] is False)

    await engine.dispose()

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
