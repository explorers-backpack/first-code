# -*- coding: utf-8 -*-
"""AI 模拟面试 · 状态机与 ``current_question_no`` 语义自检

无需 pytest，直接运行：
    python backend/tests/test_interview_state_machine.py

不依赖本机 MySQL：DATABASE_URL 指向 SQLite 内存库 + StaticPool。
被测对象是 ``services.interview_service``，不经 HTTP 层。

本文件专门覆盖「状态机 + 题号语义」的 16 条验收口径：

    1  created 可以 start              9  current_question_no=0 不能提交答案
    2  created 不能 answer            10  current_question_no=1 可以回答第一题
    3  created 不能 end               11  current_question_no=N 可以回答最后一题
    4  ongoing 可以 answer            12  current_question_no=N+1 不能继续回答
    5  ongoing 可以 end               13  重复提交同一道题不导致题号跳跃
    6  finished 不能 start            14  已 finished 的 session 不能继续推进
    7  finished 不能 answer           15  start 不重复生成不同题目
    8  finished 不能再次修改          16  题号与实际题目数量保持一致

设计前提（**不得改动**）：start 一次性生成全部题目；出题与评分均为确定性规则。
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
from models import (  # noqa: E402
    SESSION_STATUS_CREATED,
    SESSION_STATUS_FINISHED,
    SESSION_STATUS_ONGOING,
    InterviewQuestion,
    InterviewSession,
    Job,
    User,
)
from schemas.interview import AnswerSubmitRequest, SessionCreateRequest  # noqa: E402
from services import interview_service  # noqa: E402

JOB_SKILLS = "Python,MySQL,Redis,Kafka,Docker"
ANSWER = (
    "首先，我负责订单中台重构，使用 Python 与 MySQL，引入 Redis 做缓存、Kafka 解耦。"
    "具体来说我权衡了拆分粒度与运维成本，最终按业务边界拆分。结果是 QPS 从 800 提升到 5000，"
    "响应时间从 1200ms 降到 80ms。因此我认为技术选型要匹配业务规模。"
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


async def _rejected(coro, status: int = 400) -> bool:
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
    user = User(username="sm", email="sm@example.com", password_hash="x", role="user")
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
    db.add_all([user, job])
    await db.commit()
    await db.refresh(user)
    await db.refresh(job)
    return user.id, job.id


async def _new_session(db, user_id, job_id, total=5, itype="comprehensive") -> int:
    data = await interview_service.create_session(
        db,
        user_id,
        SessionCreateRequest(job_id=job_id, interview_type=itype, total_questions=total),
    )
    return data["session"]["id"]


async def _row(db, session_id) -> InterviewSession:
    return await db.get(InterviewSession, session_id)


async def _force(db, session_id, **fields):
    """直接改写会话字段，用于构造 API 无法自然到达的防御性分支。"""
    s = await db.get(InterviewSession, session_id)
    for k, v in fields.items():
        setattr(s, k, v)
    await db.commit()
    return s


async def _answer(db, user_id, session_id):
    return await interview_service.submit_answer(
        db, user_id, session_id, AnswerSubmitRequest(answer_text=ANSWER)
    )


async def _question_count(db, session_id) -> int:
    return (
        await db.execute(
            select(func.count()).select_from(InterviewQuestion).where(
                InterviewQuestion.session_id == session_id
            )
        )
    ).scalar()


async def _question_nos(db, session_id):
    rows = (
        await db.execute(
            select(InterviewQuestion.question_no)
            .where(InterviewQuestion.session_id == session_id)
            .order_by(InterviewQuestion.question_no)
        )
    ).scalars().all()
    return list(rows)


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 模拟面试 · 状态机与 current_question_no 语义自检")
    print("=" * 70)

    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        user_id, job_id = await _seed(db)

        # ============================================================
        # [A] created 状态的三种流转（check 1/2/3）
        # ============================================================
        print("\n[A] created 状态：可 start，不可 answer，不可 end")
        sid = await _new_session(db, user_id, job_id, total=5)
        s = await _row(db, sid)
        _check("[1] created 初始 current_question_no=0", s.current_question_no == 0, str(s.current_question_no))
        _check("[1] created 初始 status=created", s.status == SESSION_STATUS_CREATED, s.status)

        _check(
            "[2] created 不能 answer（400）",
            await _rejected(_answer(db, user_id, sid), 400),
        )
        s = await _row(db, sid)
        _check("[2] 被拒后题号未被推进（仍为 0）", s.current_question_no == 0, str(s.current_question_no))

        _check(
            "[3] created 不能 end（400）",
            await _rejected(interview_service.end_session(db, user_id, sid), 400),
        )
        s = await _row(db, sid)
        _check("[3] 被拒后状态仍为 created", s.status == SESSION_STATUS_CREATED, s.status)

        _check(
            "[1] created 可以 start",
            (await interview_service.start_session(db, user_id, sid))["session"]["status"] == SESSION_STATUS_ONGOING,
        )
        s = await _row(db, sid)
        _check("[1] start 后 current_question_no=1", s.current_question_no == 1, str(s.current_question_no))

        # ============================================================
        # [B] ongoing 状态（check 4/10/11）
        # ============================================================
        print("\n[B] ongoing 状态：可 answer（第 1 题 / 第 N 题）")
        _check("[4] ongoing 可以 answer", (await _answer(db, user_id, sid))["answer"]["score"] is not None)
        s = await _row(db, sid)
        _check("[10] 回答第 1 题后题号推进为 2", s.current_question_no == 2, str(s.current_question_no))

        # 推进到第 N 题（N=5，已答 1 题，再答 3 题到第 4 题）
        for _ in range(3):
            await _answer(db, user_id, sid)
        s = await _row(db, sid)
        _check("[11] 回答第 N-1 题后题号=N", s.current_question_no == 5, str(s.current_question_no))

        last = await _answer(db, user_id, sid)
        s = await _row(db, sid)
        _check("[11] 可以回答最后一题（第 N 题）", last["answer"]["score"] is not None)
        _check("[11] 答完第 N 题后题号=N+1", s.current_question_no == 6, str(s.current_question_no))
        _check("[11] all_answered 为 True", last["all_answered"] is True)
        _check("[11] 不再返回下一题", last["next_question"] is None)

        # ============================================================
        # [C] current_question_no=N+1 不能继续回答（check 12）
        # ============================================================
        print("\n[C] current_question_no=N+1：不能继续回答")
        _check(
            "[12] N+1 时提交答案被拒（400）",
            await _rejected(_answer(db, user_id, sid), 400),
        )
        s = await _row(db, sid)
        _check("[12] 被拒后题号未被继续推进（仍为 N+1）", s.current_question_no == 6, str(s.current_question_no))
        cq = await interview_service.get_current_question(db, user_id, sid)
        _check("[12] 取题接口 all_answered=True 且无题目", cq["all_answered"] is True and cq["question"] is None)

        # ============================================================
        # [D] ongoing 可以 end（check 5）
        # ============================================================
        print("\n[D] ongoing 可以 end -> finished")
        ended = await interview_service.end_session(db, user_id, sid)
        _check("[5] ongoing 可以 end", ended["session"]["status"] == SESSION_STATUS_FINISHED, ended["session"]["status"])
        _check("[5] end 生成报告", ended["report"]["total_score"] is not None)

        # ============================================================
        # [E] finished 状态的封闭性（check 6/7/8/14）
        # ============================================================
        print("\n[E] finished 状态：不可 start / answer / 再次修改")
        before = await _row(db, sid)
        before_snapshot = (
            before.status, before.current_question_no, before.ended_at,
            await _question_count(db, sid),
        )

        _check("[6] finished 不能 start（400）", await _rejected(interview_service.start_session(db, user_id, sid), 400))
        _check("[7] finished 不能 answer（400）", await _rejected(_answer(db, user_id, sid), 400))

        again = await interview_service.end_session(db, user_id, sid)
        after = await _row(db, sid)
        after_snapshot = (
            after.status, after.current_question_no, after.ended_at,
            await _question_count(db, sid),
        )
        _check("[8] finished 再次 end 幂等返回既有报告", again["message"].startswith("面试已结束"), again["message"])
        _check(
            "[8][14] finished 的 status/题号/ended_at/题量均未被改动",
            before_snapshot == after_snapshot,
            f"{before_snapshot} vs {after_snapshot}",
        )
        _check(
            "[14] finished 后取题接口不再推进（无题目可答）",
            (await interview_service.get_current_question(db, user_id, sid))["question"] is None,
        )

        # ============================================================
        # [E2] 提前 end（未答完全部题）后：finished 为终态，不得再呈现待答题目
        # ============================================================
        print("\n[E2] 提前 end（答 2/8 即结束）后：不再呈现待答题目（check 14）")
        sid_early = await _new_session(db, user_id, job_id, total=8)
        await interview_service.start_session(db, user_id, sid_early)
        for _ in range(2):
            await _answer(db, user_id, sid_early)
        e_early = await interview_service.end_session(db, user_id, sid_early)
        _check("[14] 提前 end 后状态为 finished", e_early["session"]["status"] == SESSION_STATUS_FINISHED, e_early["session"]["status"])
        s_early = await _row(db, sid_early)
        _check("[14] 提前 end 后题号仍未越过 N（=3）", s_early.current_question_no == 3, str(s_early.current_question_no))

        cq_early = await interview_service.get_current_question(db, user_id, sid_early)
        _check("[14] finished 后 question_no 为 None", cq_early["question_no"] is None, str(cq_early["question_no"]))
        _check("[14] finished 后不再返回题目内容", cq_early["question"] is None)
        _check("[14] finished 后 all_answered=True", cq_early["all_answered"] is True)
        _check("[14] finished 后提示查看报告", "报告" in cq_early["message"], cq_early["message"])
        _check("[14] finished 后仍不能 answer（400）", await _rejected(_answer(db, user_id, sid_early), 400))
        _check("[14] 被拒后题号未被推进（仍为 3）", (await _row(db, sid_early)).current_question_no == 3)

        # ============================================================
        # [F] current_question_no=0 的防御性门禁（check 9）
        # ============================================================
        print("\n[F] current_question_no=0：即使状态为 ongoing 也不能提交答案")
        sid0 = await _new_session(db, user_id, job_id, total=3)
        await interview_service.start_session(db, user_id, sid0)
        await _force(db, sid0, current_question_no=0)  # 构造 API 不可达的异常态
        _check("[9] no=0 提交答案被拒（400）", await _rejected(_answer(db, user_id, sid0), 400))
        s0 = await _row(db, sid0)
        _check("[9] 被拒后题号仍为 0（未跳跃）", s0.current_question_no == 0, str(s0.current_question_no))

        # ============================================================
        # [G] 重复提交同一道题不得导致题号跳跃（check 13）
        # ============================================================
        print("\n[G] 重复提交同一道题：题号不得跳跃")
        sid13 = await _new_session(db, user_id, job_id, total=3)
        await interview_service.start_session(db, user_id, sid13)
        await _answer(db, user_id, sid13)          # 答第 1 题 -> no=2
        s13 = await _row(db, sid13)
        _check("[13] 第 1 题作答后题号=2", s13.current_question_no == 2, str(s13.current_question_no))

        # 把题号拨回 1，模拟「对已作答的第 1 题重复提交」
        await _force(db, sid13, current_question_no=1)
        _check("[13] 对已作答题目重复提交被拒（400）", await _rejected(_answer(db, user_id, sid13), 400))
        s13 = await _row(db, sid13)
        _check("[13] 重复提交后题号未跳跃（仍为 1）", s13.current_question_no == 1, str(s13.current_question_no))
        _check("[13] 重复提交未新增作答（题号未推进即可证明）", s13.current_question_no == 1)

        # 自然语义下的「重复 POST」：无 question_no 入参，服务端按 +1 推进，绝不跳号
        sid13b = await _new_session(db, user_id, job_id, total=3)
        await interview_service.start_session(db, user_id, sid13b)
        seq = [(await _answer(db, user_id, sid13b))["session"]["current_question_no"]]
        seq.append((await _answer(db, user_id, sid13b))["session"]["current_question_no"])
        _check("[13] 连续提交题号严格 +1 递增（2,3）", seq == [2, 3], str(seq))

        # ============================================================
        # [H] start 不重复生成不同题目（check 15）
        # ============================================================
        print("\n[H] start：一次性生成全部题目，重复 start 被拒且不改变题目")
        sid15 = await _new_session(db, user_id, job_id, total=5)
        await interview_service.start_session(db, user_id, sid15)
        n1 = await _question_count(db, sid15)
        q1 = (await interview_service.get_session_detail(db, user_id, sid15))["questions"]
        texts1 = [q["question"] for q in q1]

        _check("[15] start 后题目数=N（5）", n1 == 5, str(n1))
        _check("[15] 重复 start 被拒（400）", await _rejected(interview_service.start_session(db, user_id, sid15), 400))
        n2 = await _question_count(db, sid15)
        texts2 = [q["question"] for q in (await interview_service.get_session_detail(db, user_id, sid15))["questions"]]
        _check("[15] 重复 start 未新增题目", n1 == n2, f"{n1} vs {n2}")
        _check("[15] 重复 start 未替换题目内容", texts1 == texts2)

        # 确定性：同配置的另一会话，题目应完全一致
        sid15b = await _new_session(db, user_id, job_id, total=5)
        await interview_service.start_session(db, user_id, sid15b)
        texts_b = [q["question"] for q in (await interview_service.get_session_detail(db, user_id, sid15b))["questions"]]
        _check("[15] 出题确定性：同配置两会话题目一致", texts1 == texts_b)

        # ============================================================
        # [I] 题号与实际题目数量一致（check 16）
        # ============================================================
        print("\n[I] 题号与实际题目数量一致（含 1..20 × 3 种类型的全组合）")
        ok_plan, bad_plan = 0, []
        for total in range(1, 21):
            for itype in ("technical", "behavioral", "comprehensive"):
                types = interview_service._plan_question_types(total, itype)
                if len(types) == total:
                    ok_plan += 1
                else:
                    bad_plan.append((total, itype, len(types)))
        _check(
            "[16] 题型分配长度恒等于 total（1..20 × 3 类型 = 60 组）",
            not bad_plan,
            f"不符组数={len(bad_plan)}，示例={bad_plan[:3]}",
        )
        _check("[16] 60 组全部通过", ok_plan == 60, str(ok_plan))

        # 端到端：每个 total 都真实建会话并 start，校验题量与题号
        e2e_bad = []
        for total in range(1, 11):
            s_id = await _new_session(db, user_id, job_id, total=total)
            await interview_service.start_session(db, user_id, s_id)
            cnt = await _question_count(db, s_id)
            nos = await _question_nos(db, s_id)
            row = await _row(db, s_id)
            if not (cnt == total == row.total_questions and nos == list(range(1, total + 1))):
                e2e_bad.append((total, cnt, nos, row.total_questions))
        _check(
            "[16] 端到端：题量=total_questions 且题号严格 1..N（total=1..10）",
            not e2e_bad,
            f"异常={e2e_bad[:3]}",
        )

        # 题号单调不越界：全流程结束后必为 N+1
        s_id = await _new_session(db, user_id, job_id, total=4)
        await interview_service.start_session(db, user_id, s_id)
        seq_nos = [0, (await _row(db, s_id)).current_question_no]
        for _ in range(4):
            r = await _answer(db, user_id, s_id)
            seq_nos.append(r["session"]["current_question_no"])
        _check("[16] 题号序列为 1,2,3,4,5（N+1）", seq_nos == [0, 1, 2, 3, 4, 5], str(seq_nos))
        _check(
            "[16] 全程题号始终落在 [0, N+1] 且严格 +1",
            all(seq_nos[i] + 1 == seq_nos[i + 1] for i in range(len(seq_nos) - 1)),
            str(seq_nos),
        )

    await engine.dispose()

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
