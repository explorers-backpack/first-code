# -*- coding: utf-8 -*-
"""AI 模拟面试 · 会话生命周期自检（service 层）

无需 pytest，直接运行：
    python backend/tests/test_interview_lifecycle.py

不依赖本机 MySQL：通过环境变量把 DATABASE_URL 指向 SQLite 内存库，
并用 StaticPool 保证复用同一连接（内存库否则每次连接都是新库）。
被测对象是 ``services.interview_service``，不经 HTTP 层。

覆盖的验收口径：
1. 生命周期状态机：created → ongoing → finished，非法流转被拦
2. 题目生成：题量精确、题型配比正确、技术题命中岗位技能
3. 单题评分：0-100 区间、确定性（同输入同输出）、好坏回答可区分
4. 一题一答：数据库层与 service 层的重复保护
5. 报告汇总：八维齐全、加权总分合理、亮点/不足/建议非空
6. 越权隔离：访问他人会话一律 404
"""

import asyncio
import os
import pathlib
import sys

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
# load_dotenv 默认 override=False，因此这里设置的值不会被 .env 覆盖。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from fastapi import HTTPException  # noqa: E402
from pydantic import ValidationError  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import InterviewAnswer, Job, User  # noqa: E402
from schemas.interview import AnswerSubmitRequest, SessionCreateRequest  # noqa: E402
from services import interview_service  # noqa: E402

# ============================================================
# 测试数据
# ============================================================
JOB_SKILLS = "Python,MySQL,Redis,Kafka,Docker"

GOOD_ANSWER = (
    "首先，我在上一家公司负责订单中台的重构。当时系统 QPS 只有 800，"
    "我主导把单体服务拆成 8 个微服务，技术栈使用 Python 与 MySQL，"
    "引入 Redis 做缓存、Kafka 做异步解耦。具体来说，我权衡了拆分粒度与运维成本，"
    "最终按业务边界拆分，并对比了同步调用与消息队列两种方案的代价。"
    "结果是 QPS 提升到 5000，响应时间从 1200ms 降到 80ms。因此我认为，"
    "技术选型的核心是匹配当前业务规模。"
)

TECH_ANSWER = (
    "Python 在我的项目里主要用于后端服务开发。首先，它的 GIL 决定了 CPU 密集场景"
    "需要多进程而不是多线程，我因此用 Python 写 IO 密集的接口层，把计算任务交给"
    "单独的进程池。场景上，我用 Python 做过订单查询接口，通过连接池与异步 IO "
    "把响应时间从 300ms 降到 80ms。遇到的问题是慢查询，我通过索引优化解决了它。"
)

POOR_ANSWER = "不知道"

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


async def _expect_status(coro, status: int) -> bool:
    """断言协程抛出指定状态码的 HTTPException。"""
    try:
        await coro
        return False
    except HTTPException as exc:
        return exc.status_code == status


def _rejects_validation(factory, **kwargs) -> bool:
    """断言 Pydantic 拒绝该入参。"""
    try:
        factory(**kwargs)
        return False
    except ValidationError:
        return True


# ============================================================
# 环境准备
# ============================================================
def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed(db: AsyncSession):
    user = User(username="tester", email="tester@example.com", password_hash="x", role="user")
    other = User(username="other", email="other@example.com", password_hash="x", role="user")
    job = Job(
        job_name="后端开发工程师",
        salary="20-35K",
        edu_require="本科",
        major_require="不限",
        skills=JOB_SKILLS,
        duty="负责后端服务的设计与开发，参与需求分析、系统设计与性能优化",
        city="深圳",
        industry="互联网",
    )
    db.add_all([user, other, job])
    await db.commit()
    await db.refresh(user)
    await db.refresh(other)
    await db.refresh(job)
    return user.id, other.id, job.id


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 68)
    print("AI 模拟面试 · 会话生命周期自检（service 层）")
    print("=" * 68)

    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        user_id, other_user_id, job_id = await _seed(db)

        # ---------------- [1] 创建会话 ----------------
        print("\n[1] 创建会话（status=created，不生成题目）")
        created = await interview_service.create_session(
            db, user_id, SessionCreateRequest(job_id=job_id, difficulty="mid", total_questions=5)
        )
        session_id = created["session"]["id"]
        _check("初始状态为 created", created["session"]["status"] == "created", created["session"]["status"])
        _check("current_question_no 为 0", created["session"]["current_question_no"] == 0)
        _check("未生成题目（first_question 为空）", created["first_question"] is None)
        _check("回传岗位名", created["job_name"] == "后端开发工程师", str(created["job_name"]))

        print("\n  -- 入参校验（Pydantic 层）--")
        _check("非法 interview_type 被拒", _rejects_validation(SessionCreateRequest, interview_type="unknown"))
        _check("非法 difficulty 被拒", _rejects_validation(SessionCreateRequest, difficulty="expert"))
        _check("total_questions 越界被拒", _rejects_validation(SessionCreateRequest, total_questions=0))
        _check("duration 越界被拒", _rejects_validation(SessionCreateRequest, duration=1))
        _check("空回答文本被拒", _rejects_validation(AnswerSubmitRequest, answer_text=""))

        print("\n  -- 业务校验（service 层）--")
        _check(
            "不存在的 job_id 被拒（400）",
            await _expect_status(
                interview_service.create_session(db, user_id, SessionCreateRequest(job_id=99999)), 400
            ),
        )
        _check(
            "不存在的 resume_id 被拒（400）",
            await _expect_status(
                interview_service.create_session(db, user_id, SessionCreateRequest(resume_id=99999)), 400
            ),
        )
        _check(
            "created 状态不能提交回答（400）",
            await _expect_status(
                interview_service.submit_answer(db, user_id, session_id, AnswerSubmitRequest(answer_text="x")), 400
            ),
        )
        _check(
            "created 状态不能结束（400）",
            await _expect_status(interview_service.end_session(db, user_id, session_id), 400),
        )
        _check(
            "created 状态读报告（404）",
            await _expect_status(interview_service.get_report(db, user_id, session_id), 404),
        )

        # ---------------- [2] 开始面试 ----------------
        print("\n[2] 开始面试（生成题目，status=ongoing）")
        started = await interview_service.start_session(db, user_id, session_id)
        _check("状态变为 ongoing", started["session"]["status"] == "ongoing", started["session"]["status"])
        _check("started_at 已写入", started["session"]["started_at"] is not None)
        _check("current_question_no 为 1", started["session"]["current_question_no"] == 1)
        _check("返回第一题", started["question"] is not None)
        _check("第一题是自我介绍", started["question"]["question_type"] == "intro", started["question"]["question_type"])

        detail = await interview_service.get_session_detail(db, user_id, session_id)
        questions = detail["questions"]
        _check("题量等于 total_questions", len(questions) == 5, str(len(questions)))
        _check("题号连续 1..N", [q["question_no"] for q in questions] == [1, 2, 3, 4, 5])
        types = [q["question_type"] for q in questions]
        _check("最后一题为收尾题", types[-1] == "closing", str(types))
        _check("题目均带 expected_points", all(q["expected_points"] for q in questions))

        tech_qs = [q for q in questions if q["question_type"] == "technical"]
        job_skill_set = {s.strip() for s in JOB_SKILLS.split(",")}
        _check("生成技术题", len(tech_qs) >= 1, str(len(tech_qs)))
        _check(
            "技术题命中岗位技能",
            all(q["topic"] in job_skill_set for q in tech_qs),
            str([q["topic"] for q in tech_qs]),
        )
        _check(
            "重复 start 被拒（400）",
            await _expect_status(interview_service.start_session(db, user_id, session_id), 400),
        )

        # ---------------- [3] 取当前题 ----------------
        print("\n[3] 获取当前题目")
        current = await interview_service.get_current_question(db, user_id, session_id)
        _check("返回第 1 题", current["question_no"] == 1, str(current["question_no"]))
        _check("answered 为 False", current["answered"] is False)
        _check("all_answered 为 False", current["all_answered"] is False)

        # ---------------- [4] 提交回答与评分 ----------------
        print("\n[4] 提交回答（规则评分）")
        first_answer = await interview_service.submit_answer(
            db, user_id, session_id, AnswerSubmitRequest(answer_text=GOOD_ANSWER)
        )
        ans = first_answer["answer"]
        _check("总分在 0-100", isinstance(ans["score"], int) and 0 <= ans["score"] <= 100, str(ans["score"]))
        for field in ("technical_score", "logic_score", "expression_score", "adaptability_score"):
            _check(f"{field} 在 0-100", isinstance(ans[field], int) and 0 <= ans[field] <= 100, str(ans[field]))
        _check("feedback 非空", bool(ans["feedback"] and ans["feedback"].strip()))
        _check("feedback 含取证信息", "【取证】" in (ans["feedback"] or ""))
        _check("audio_url 允许为空", ans["audio_url"] is None)
        _check("推进到第 2 题", first_answer["session"]["current_question_no"] == 2)
        _check("返回下一题", first_answer["next_question"] is not None)
        _check("all_answered 为 False", first_answer["all_answered"] is False)
        print(
            f"        优秀回答得分 = {ans['score']}（技术 {ans['technical_score']} / "
            f"逻辑 {ans['logic_score']} / 表达 {ans['expression_score']} / 应变 {ans['adaptability_score']}）"
        )

        _check(
            "空白回答被拒（400）",
            await _expect_status(
                interview_service.submit_answer(db, user_id, session_id, AnswerSubmitRequest(answer_text="   ")),
                400,
            ),
        )

        print("\n  -- 评分确定性 --")
        q_dict = questions[0]
        s1 = interview_service.score_answer(q_dict, GOOD_ANSWER)
        s2 = interview_service.score_answer(q_dict, GOOD_ANSWER)
        _check("同输入两次评分一致", s1 == s2)
        poor = interview_service.score_answer(q_dict, POOR_ANSWER)
        _check("好坏回答可区分", poor["score"] < s1["score"], f"{poor['score']} vs {s1['score']}")

        tech_q = next(q for q in questions if q["question_type"] == "technical")
        tech_score = interview_service.score_answer(tech_q, TECH_ANSWER)
        _check(
            "切题的技术回答技术分 >= 70",
            tech_score["technical_score"] >= 70,
            f"技术分 {tech_score['technical_score']}（题目主题 {tech_q['topic']}）",
        )
        print(
            f"        技术题「{tech_q['topic']}」得分 = {tech_score['score']}"
            f"（技术 {tech_score['technical_score']}）"
        )

        # ---------------- [5] 重复作答保护 ----------------
        # 正常流程下题号会自动推进，无法通过接口对同一题提交两次；
        # 这里直接向数据库预置一条作答，模拟并发写入，验证 service 的保护分支。
        print("\n[5] 重复作答保护（独立会话）")
        dup = await interview_service.create_session(
            db, user_id, SessionCreateRequest(job_id=job_id, total_questions=3)
        )
        dup_id = dup["session"]["id"]
        await interview_service.start_session(db, user_id, dup_id)
        dup_current = await interview_service.get_current_question(db, user_id, dup_id)
        db.add(
            InterviewAnswer(
                question_id=dup_current["question"]["id"],
                answer_text="预置作答",
                score=50,
            )
        )
        await db.commit()
        _check(
            "已作答的题再次提交被拒（400）",
            await _expect_status(
                interview_service.submit_answer(db, user_id, dup_id, AnswerSubmitRequest(answer_text="重复")), 400
            ),
        )

        # ---------------- [6] 完成剩余题目 ----------------
        print("\n[6] 完成剩余题目")
        result = None
        guard = 0
        while True:
            snapshot = await interview_service.get_current_question(db, user_id, session_id)
            if snapshot["all_answered"]:
                break
            result = await interview_service.submit_answer(
                db, user_id, session_id, AnswerSubmitRequest(answer_text=GOOD_ANSWER)
            )
            guard += 1
            if guard > 20:
                _check("作答循环未失控", False, "超过 20 次仍未结束")
                break

        _check("全部作答后 all_answered 为 True", result is not None and result["all_answered"] is True)
        _check("不再返回下一题", result is not None and result["next_question"] is None)
        _check(
            "题号超出总题量",
            result is not None and result["session"]["current_question_no"] == 6,
            str(result["session"]["current_question_no"]) if result else "None",
        )
        after_all = await interview_service.get_current_question(db, user_id, session_id)
        _check("取题接口提示 all_answered", after_all["all_answered"] is True)
        _check(
            "全部作答后继续提交被拒（400）",
            await _expect_status(
                interview_service.submit_answer(db, user_id, session_id, AnswerSubmitRequest(answer_text="x")), 400
            ),
        )

        # ---------------- [7] 结束并生成报告 ----------------
        print("\n[7] 结束面试并生成报告")
        ended = await interview_service.end_session(db, user_id, session_id)
        report = ended["report"]
        _check("状态变为 finished", ended["session"]["status"] == "finished", ended["session"]["status"])
        _check("ended_at 已写入", ended["session"]["ended_at"] is not None)

        dims = (
            "total_score", "technical_score", "project_score", "logic_score",
            "expression_score", "communication_score", "adaptability_score", "job_match_score",
        )
        for field in dims:
            value = report[field]
            _check(
                f"报告 {field} 在 0-100",
                value is None or (isinstance(value, int) and 0 <= value <= 100),
                str(value),
            )
        _check("岗位匹配度已计算（指定了岗位技能）", report["job_match_score"] is not None)
        _check("strengths 非空", bool(report["strengths"]))
        _check("weaknesses 非空", bool(report["weaknesses"]))
        _check("suggestions 非空", bool(report["suggestions"] and report["suggestions"].strip()))
        print(
            f"        总分 {report['total_score']} | 技术 {report['technical_score']} "
            f"| 项目 {report['project_score']} | 逻辑 {report['logic_score']} "
            f"| 表达 {report['expression_score']} | 沟通 {report['communication_score']} "
            f"| 应变 {report['adaptability_score']} | 岗位匹配 {report['job_match_score']}"
        )
        print(f"        亮点 {len(report['strengths'])} 条 / 不足 {len(report['weaknesses'])} 条")

        fetched = await interview_service.get_report(db, user_id, session_id)
        _check("报告可通过 get_report 读回", fetched["total_score"] == report["total_score"])
        _check(
            "重复 end 幂等（返回既有报告）",
            (await interview_service.end_session(db, user_id, session_id))["report"]["id"] == report["id"],
        )

        # ---------------- [8] 越权与不存在 ----------------
        print("\n[8] 越权与不存在的会话")
        _check(
            "他人会话不可读（404）",
            await _expect_status(interview_service.get_session_detail(db, other_user_id, session_id), 404),
        )
        _check(
            "他人会话不可作答（404）",
            await _expect_status(
                interview_service.submit_answer(db, other_user_id, session_id, AnswerSubmitRequest(answer_text="x")),
                404,
            ),
        )
        _check(
            "他人会话不可读报告（404）",
            await _expect_status(interview_service.get_report(db, other_user_id, session_id), 404),
        )
        _check(
            "不存在的会话（404）",
            await _expect_status(interview_service.get_session_detail(db, user_id, 99999), 404),
        )

        # ---------------- [9] 提前结束（部分作答） ----------------
        print("\n[9] 提前结束（仅答 2 题即结束）")
        partial = await interview_service.create_session(
            db, user_id, SessionCreateRequest(job_id=job_id, total_questions=8)
        )
        pid = partial["session"]["id"]
        await interview_service.start_session(db, user_id, pid)
        for _ in range(2):
            await interview_service.submit_answer(db, user_id, pid, AnswerSubmitRequest(answer_text=GOOD_ANSWER))
        p_detail = await interview_service.get_session_detail(db, user_id, pid)
        _check("仅落库 2 条作答", p_detail["answered_count"] == 2, str(p_detail["answered_count"]))
        p_ended = await interview_service.end_session(db, user_id, pid)
        _check("可提前结束", p_ended["session"]["status"] == "finished")
        _check("报告基于已作答内容生成", p_ended["report"]["total_score"] is not None)
        _check("建议中说明作答题数", "2/8" in p_ended["report"]["suggestions"], p_ended["report"]["suggestions"][:40])

        # ---------------- [10] 未指定岗位 ----------------
        print("\n[10] 未指定岗位时的处理")
        nojob = await interview_service.create_session(db, user_id, SessionCreateRequest(total_questions=3))
        nid = nojob["session"]["id"]
        await interview_service.start_session(db, user_id, nid)
        for _ in range(3):
            await interview_service.submit_answer(db, user_id, nid, AnswerSubmitRequest(answer_text=GOOD_ANSWER))
        n_ended = await interview_service.end_session(db, user_id, nid)
        _check("未指定岗位时 job_match_score 为 None（不猜）", n_ended["report"]["job_match_score"] is None)
        _check("其余维度仍正常计算", n_ended["report"]["technical_score"] is not None)
        _check("总分已按可用维度归一化", 0 <= n_ended["report"]["total_score"] <= 100)

        # ---------------- [11] 题型配比（技术面 vs 行为面） ----------------
        print("\n[11] 不同面试类型的题型配比")
        tech_only = await interview_service.create_session(
            db, user_id, SessionCreateRequest(job_id=job_id, interview_type="technical", total_questions=6)
        )
        tq = await interview_service.start_session(db, user_id, tech_only["session"]["id"])
        t_detail = await interview_service.get_session_detail(db, user_id, tech_only["session"]["id"])
        t_types = [q["question_type"] for q in t_detail["questions"]]
        _check("技术面题量为 6", len(t_types) == 6, str(len(t_types)))
        _check(
            "技术面技术题占比最高",
            t_types.count("technical") >= t_types.count("behavioral"),
            str(t_types),
        )
        print(f"        技术面题型分布：{t_types}")

        beh = await interview_service.create_session(
            db, user_id, SessionCreateRequest(job_id=job_id, interview_type="behavioral", total_questions=6)
        )
        await interview_service.start_session(db, user_id, beh["session"]["id"])
        b_detail = await interview_service.get_session_detail(db, user_id, beh["session"]["id"])
        b_types = [q["question_type"] for q in b_detail["questions"]]
        _check(
            "行为面行为题占比最高",
            b_types.count("behavioral") >= b_types.count("technical"),
            str(b_types),
        )
        print(f"        行为面题型分布：{b_types}")

    await engine.dispose()

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
