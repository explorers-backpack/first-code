# -*- coding: utf-8 -*-
"""AI 模拟面试 · InterviewService（会话与 API 门面层）。

分层定位（四层）
----------------
::

    api/interview.py              ← HTTP 契约（参数校验 / 响应模型 / 鉴权）
        └── interview_service     ← ① 【本模块】Service：API 流程 / Session 管理 / 前端交互
              └── interview_core  ← ② Core：面试流程控制 / Context / Plan / Agent / Validator
                    ├── interview_agent      ③ Agent：LLM 调用 / Prompt 构造 / 候选问题
                    └── question_validator   ④ Validator：问题校验 / 字段标准化

本模块只做三件事
----------------
1. **Session 管理**：会话的创建 / 开始 / 查询 / 作答 / 结束 / 读报告，
   以及状态机守卫（``created → ongoing → finished``）与归属校验。
2. **数据访问**：本层是**唯一**接触数据库的地方（``_load_*`` 系列）。
   Core 不碰数据库，因此出题计划 / 评分 / 报告可脱离 DB 单测。
3. **前端交互契约**：把 ORM 行序列化成前端可直接消费的普通 dict
   （``_session_to_dict`` / ``_question_to_dict`` / ``_answer_to_dict``
   / ``_report_to_dict``）。

另有 **``generate_next_question``**（第四节）：按需出题的**大模型通道**，
Service 只做归属校验，其余全部委托 ``interview_core`` ——
**Service 永不直接调用 InterviewAgent / QuestionValidator**。

**交互模式**（``interview_mode``）
--------------------------------
会话新增 ``interview_mode``（``text`` 文字面试 / ``avatar`` 数字人视频面试），
由 ``create_session`` 落库、``_session_to_dict`` 回传。
它**只选择交互方式**：出题、评分、报告与状态机完全不看这个字段，
两种模式共用同一套面试核心（因此本层除「写入 / 读出」外无任何分支逻辑）。

分流点只有一处：``_deliver``（交付边界）——``text`` 原样返回，
``avatar`` 交给 ``services/avatar_interview.enter``（数字人通道入口，当前空实现）。
**面试逻辑一行都不分流**，这样 text 路径天然零变化、avatar 通道也不会阻塞流程。

**不在**本模块：出题计划、作答评分、报告汇总，以及 Context / Plan / Agent /
Validator 的编排——这些全部在 ``services/interview_core.py``。

对外约定
--------
所有函数以 ``db: AsyncSession`` 为第一参数（依赖注入），返回**普通 dict**，
不依赖 FastAPI 的请求上下文——因此可脱离 HTTP 单独测试（见
``tests/test_interview_lifecycle.py``）。

生命周期状态机
--------------
``created`` --start--> ``ongoing`` --end--> ``finished``

``current_question_no`` 语义：0 = 未开始；1..N = 当前待作答题号；
N+1 = 全部题目已作答（等待调用 ``end``）。

向后兼容（重要）
----------------
本次重构把业务函数**搬到了** ``interview_core``（**只搬位置、未改逻辑**）。
为不破坏既有调用方（``api/interview.py`` 与 ``tests/`` 下 3 个脚本），
本模块在下方对旧名字做**显式再导出**。新代码请直接 ``from services import
interview_core``，不要依赖这些别名。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import (
    DEFAULT_INTERVIEW_MODE,
    DIFFICULTIES,
    INTERVIEW_MODE_AVATAR,
    INTERVIEW_MODES,
    INTERVIEW_TYPES,
    SESSION_STATUS_CREATED,
    SESSION_STATUS_FINISHED,
    SESSION_STATUS_ONGOING,
    InterviewAnswer,
    InterviewQuestion,
    InterviewReport,
    InterviewSession,
    Job,
    Resume,
)
from schemas.interview import AnswerSubmitRequest, SessionCreateRequest
from services import avatar_interview, interview_core
from services.resume_scoring import extract_skills

# ============================================================
# 〇、向后兼容：旧符号再导出（业务实现已迁至 interview_core）
# ============================================================
# 仅做名字绑定，不复制实现——保证 `interview_service.score_answer` 与
# `interview_core.score_answer` 是**同一个对象**，不会出现两套逻辑。
DIFFICULTY_LABELS = interview_core.DIFFICULTY_LABELS
INTERVIEW_TYPE_LABELS = interview_core.INTERVIEW_TYPE_LABELS
ANSWER_DIMENSION_WEIGHTS = interview_core.ANSWER_DIMENSION_WEIGHTS
REPORT_DIMENSION_WEIGHTS = interview_core.REPORT_DIMENSION_WEIGHTS

score_answer = interview_core.score_answer
build_report = interview_core.build_report

# 旧私有名 → Core 公开名（`_plan_question_types` 被 test_interview_state_machine 直接调用）
_plan_question_types = interview_core.plan_question_types
_build_question_text = interview_core.build_question_text
_build_question_plan = interview_core.build_question_plan
_build_feedback = interview_core.build_answer_feedback

# ============================================================
# 一、序列化辅助（前端交互契约）
# ============================================================
def _session_to_dict(session: InterviewSession) -> Dict[str, Any]:
    return {
        "id": session.id,
        "user_id": session.user_id,
        "job_id": session.job_id,
        "resume_id": session.resume_id,
        "interview_type": session.interview_type,
        "difficulty": session.difficulty,
        "duration": session.duration,
        # 交互模式：只表示「怎么面」（文字 / 数字人视频）。
        # 未 flush 的对象上该属性可能为 None（列 default 在 INSERT 时生效），
        # 因此回退到默认值，保证出参契约里该字段恒为合法字符串。
        "interview_mode": session.interview_mode or DEFAULT_INTERVIEW_MODE,
        "status": session.status,
        "total_questions": session.total_questions,
        "current_question_no": session.current_question_no,
        "started_at": session.started_at,
        "ended_at": session.ended_at,
        "created_at": session.created_at,
    }


def _question_to_dict(question: InterviewQuestion) -> Dict[str, Any]:
    points = question.expected_points
    return {
        "id": question.id,
        "question_no": question.question_no,
        "question": question.question,
        "question_type": question.question_type,
        "topic": question.topic,
        "difficulty": question.difficulty,
        "expected_points": list(points) if isinstance(points, list) else None,
        "created_at": question.created_at,
    }


def _answer_to_dict(answer: InterviewAnswer) -> Dict[str, Any]:
    return {
        "id": answer.id,
        "question_id": answer.question_id,
        "answer_text": answer.answer_text,
        "audio_url": answer.audio_url,
        "score": answer.score,
        "technical_score": answer.technical_score,
        "logic_score": answer.logic_score,
        "expression_score": answer.expression_score,
        "adaptability_score": answer.adaptability_score,
        "feedback": answer.feedback,
        "created_at": answer.created_at,
    }


def _report_to_dict(report: InterviewReport) -> Dict[str, Any]:
    strengths = report.strengths
    weaknesses = report.weaknesses
    return {
        "id": report.id,
        "session_id": report.session_id,
        "total_score": report.total_score,
        "technical_score": report.technical_score,
        "project_score": report.project_score,
        "logic_score": report.logic_score,
        "expression_score": report.expression_score,
        "communication_score": report.communication_score,
        "adaptability_score": report.adaptability_score,
        "job_match_score": report.job_match_score,
        "strengths": list(strengths) if isinstance(strengths, list) else None,
        "weaknesses": list(weaknesses) if isinstance(weaknesses, list) else None,
        "suggestions": report.suggestions,
        "created_at": report.created_at,
    }


# ============================================================
# 二、数据访问（内部；本层是全项目唯一接触 DB 的地方）
# ============================================================
async def _load_session(
    db: AsyncSession, session_id: int, user_id: int
) -> InterviewSession:
    """按 id 加载会话并校验归属。不存在或非本人一律 404（不泄露他人会话存在性）。"""
    result = await db.execute(
        select(InterviewSession).where(InterviewSession.id == session_id)
    )
    session = result.scalar_one_or_none()
    if session is None or session.user_id != user_id:
        raise HTTPException(status_code=404, detail=f"面试会话 {session_id} 不存在")
    return session


async def _load_questions(
    db: AsyncSession, session_id: int
) -> List[InterviewQuestion]:
    result = await db.execute(
        select(InterviewQuestion)
        .where(InterviewQuestion.session_id == session_id)
        .order_by(InterviewQuestion.question_no)
    )
    return list(result.scalars().all())


async def _load_answers(
    db: AsyncSession, session_id: int
) -> List[InterviewAnswer]:
    """按会话取全部作答（answer → question → session 两跳关联）。"""
    result = await db.execute(
        select(InterviewAnswer)
        .join(InterviewQuestion, InterviewAnswer.question_id == InterviewQuestion.id)
        .where(InterviewQuestion.session_id == session_id)
        .order_by(InterviewQuestion.question_no)
    )
    return list(result.scalars().all())


async def _load_job(db: AsyncSession, job_id: Optional[int]) -> Optional[Job]:
    if not job_id:
        return None
    result = await db.execute(select(Job).where(Job.id == job_id))
    return result.scalar_one_or_none()


async def _load_resume_skills(
    db: AsyncSession, resume_id: Optional[int]
) -> List[str]:
    """读取简历技能。``resume`` 表当前无写入逻辑，取不到即返回空列表（不猜）。"""
    if not resume_id:
        return []
    result = await db.execute(select(Resume).where(Resume.id == resume_id))
    resume = result.scalar_one_or_none()
    if resume is None or not resume.content:
        return []
    return extract_skills(resume.content)


# ============================================================
# 二·五、交互模式分流（交付边界：text 原流程 / avatar 数字人通道）
# ============================================================
async def _deliver(
    db: AsyncSession,
    session: InterviewSession,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """按 ``session.interview_mode`` 把**交付内容**交给对应通道。

    这是全模块**唯一的交付边界分流点**：

    - ``text``（默认）→ **原样返回** ``payload``，行为与引入本函数之前完全一致；
    - ``avatar``       → 交给 ``services.avatar_interview.enter``（数字人通道入口，
      当前为空实现、原样透传，未接讯飞 / ASR / TTS）。

    刻意**不**在这里做任何业务判断：出题、评分、报告、状态机对两种模式完全共享，
    本函数只回答「这份结果交给谁」。因此未来 ``submit_answer`` / ``end_session``
    需要分流时，直接复用本函数即可，不必再写一遍分支。
    """
    if session.interview_mode == INTERVIEW_MODE_AVATAR:
        return await avatar_interview.enter(db, session, payload)
    return payload


# ============================================================
# 三、生命周期：对外接口（API 流程）
# ============================================================
async def create_session(
    db: AsyncSession, user_id: int, payload: SessionCreateRequest
) -> Dict[str, Any]:
    """创建面试会话（状态 ``created``，尚未生成题目）。

    校验 ``job_id`` / ``resume_id`` 指向的记录确实存在——外键能保证写入合法，
    但提前校验可给出可读的错误提示，而不是让数据库抛完整性错误。

    ``payload.mode`` 是**交互模式**（``text`` / ``avatar``），只落库备查：
    它不改变出题、评分与状态机——文字面试与数字人视频面试共用同一套面试核心。
    """
    if payload.interview_type not in INTERVIEW_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"interview_type 取值非法，允许：{'/'.join(INTERVIEW_TYPES)}",
        )
    if payload.difficulty not in DIFFICULTIES:
        raise HTTPException(
            status_code=400,
            detail=f"difficulty 取值非法，允许：{'/'.join(DIFFICULTIES)}",
        )
    # 双保险：API 层已由 Pydantic Literal 拦截非法值（422），
    # 这里再校验一次，保证直接调用 service（如测试 / 脚本）时口径一致。
    if payload.mode not in INTERVIEW_MODES:
        raise HTTPException(
            status_code=400,
            detail=f"mode 取值非法，允许：{'/'.join(INTERVIEW_MODES)}",
        )

    job = None
    if payload.job_id is not None:
        job = await _load_job(db, payload.job_id)
        if job is None:
            raise HTTPException(
                status_code=400, detail=f"岗位 {payload.job_id} 不存在，请先确认岗位库数据"
            )

    if payload.resume_id is not None:
        result = await db.execute(select(Resume).where(Resume.id == payload.resume_id))
        if result.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=400, detail=f"简历 {payload.resume_id} 不存在"
            )

    session = InterviewSession(
        user_id=user_id,
        job_id=payload.job_id,
        resume_id=payload.resume_id,
        interview_type=payload.interview_type,
        difficulty=payload.difficulty,
        duration=payload.duration,
        interview_mode=payload.mode,
        total_questions=payload.total_questions,
        current_question_no=0,
        status=SESSION_STATUS_CREATED,
    )
    db.add(session)
    await db.commit()
    await db.refresh(session)

    return {
        "session": _session_to_dict(session),
        "first_question": None,
        "job_name": job.job_name if job else None,
        "message": "面试会话创建成功，请调用 start 开始面试",
    }


async def start_session(
    db: AsyncSession, user_id: int, session_id: int
) -> Dict[str, Any]:
    """开始面试：生成整场题目计划并置为 ``ongoing``。

    题目计划一次性生成（当前为确定性规则，不依赖作答内容），
    因此 ``start`` 可安全重入判断——重复调用会被状态校验拦下。

    **交互模式分流**（本次新增）：状态守卫与出题**两种模式完全共享**，
    只在最后「交付第一题」这一步按 ``session.interview_mode`` 分流——
    ``text`` 原样返回（默认路径，行为不变），``avatar`` 走数字人通道入口。
    """
    session = await _load_session(db, session_id, user_id)

    if session.status == SESSION_STATUS_FINISHED:
        raise HTTPException(status_code=400, detail="面试已结束，无法重新开始")
    if session.status == SESSION_STATUS_ONGOING:
        raise HTTPException(
            status_code=400, detail="面试已在进行中，请直接获取当前题目"
        )

    job = await _load_job(db, session.job_id)
    resume_skills = await _load_resume_skills(db, session.resume_id)
    # 出题计划由 Core 负责（纯规则、确定性）；本层只负责读数据与落库
    plan = interview_core.build_question_plan(session, job, resume_skills)

    if not plan:
        raise HTTPException(status_code=500, detail="题目计划生成失败，请检查会话配置")

    for item in plan:
        db.add(
            InterviewQuestion(
                session_id=session.id,
                question_no=item["question_no"],
                question=item["question"],
                question_type=item["question_type"],
                topic=item["topic"],
                difficulty=item["difficulty"],
                expected_points=item["expected_points"],
            )
        )

    session.status = SESSION_STATUS_ONGOING
    session.started_at = datetime.utcnow()
    session.current_question_no = 1
    await db.commit()
    await db.refresh(session)

    questions = await _load_questions(db, session.id)
    first = questions[0] if questions else None

    payload = {
        "session": _session_to_dict(session),
        "question": _question_to_dict(first) if first else None,
        "job_name": job.job_name if job else None,
        "message": "面试已开始",
    }
    # 交付边界分流：text 原样返回（默认路径）；avatar 走数字人通道入口
    return await _deliver(db, session, payload)


async def get_session_detail(
    db: AsyncSession, user_id: int, session_id: int
) -> Dict[str, Any]:
    """查询会话状态与完整对话（题目 + 已提交的作答）。"""
    session = await _load_session(db, session_id, user_id)
    job = await _load_job(db, session.job_id)
    questions = await _load_questions(db, session.id)
    answers = await _load_answers(db, session.id)

    return {
        "session": _session_to_dict(session),
        "job_name": job.job_name if job else None,
        "questions": [_question_to_dict(q) for q in questions],
        "answers": [_answer_to_dict(a) for a in answers],
        "answered_count": len(answers),
    }


async def get_current_question(
    db: AsyncSession, user_id: int, session_id: int
) -> Dict[str, Any]:
    """获取当前待作答的题目。

    四种情况：
    - 会话已结束（``finished``）→ 终态，不再返回任何题目，提示查看报告
    - 会话未开始（``created``）→ 返回 ``question=None``，提示先 start
    - 有题待答 → 返回该题，``answered=False``
    - 全部作答完毕 → ``all_answered=True``，提示调用 end
    """
    session = await _load_session(db, session_id, user_id)
    questions = await _load_questions(db, session.id)
    answers = await _load_answers(db, session.id)
    answered_ids = {a.question_id for a in answers}

    # 终态优先：finished 是不可逆终态，即使 current_question_no 尚未越过 N
    # （例如提前 end），也不得再对外呈现「待作答题目」，否则前端会误判还有题目可答。
    if session.status == SESSION_STATUS_FINISHED:
        return {
            "session_id": session.id,
            "status": session.status,
            "question_no": None,
            "total_questions": session.total_questions,
            "question": None,
            "answered": False,
            "all_answered": True,
            "message": "面试已结束，请调用 report 接口查看面试报告",
        }

    if session.status == SESSION_STATUS_CREATED or session.current_question_no <= 0:
        return {
            "session_id": session.id,
            "status": session.status,
            "question_no": None,
            "total_questions": session.total_questions,
            "question": None,
            "answered": False,
            "all_answered": False,
            "message": "面试尚未开始，请先调用 start 接口",
        }

    if session.current_question_no > session.total_questions:
        return {
            "session_id": session.id,
            "status": session.status,
            "question_no": None,
            "total_questions": session.total_questions,
            "question": None,
            "answered": False,
            "all_answered": True,
            "message": "全部题目已作答，请调用 end 接口生成报告",
        }

    current = next(
        (q for q in questions if q.question_no == session.current_question_no), None
    )
    if current is None:
        raise HTTPException(
            status_code=500,
            detail=f"会话数据异常：第 {session.current_question_no} 题不存在，请重新创建面试",
        )

    return {
        "session_id": session.id,
        "status": session.status,
        "question_no": current.question_no,
        "total_questions": session.total_questions,
        "question": _question_to_dict(current),
        "answered": current.id in answered_ids,
        "all_answered": False,
        "message": "ok",
    }


async def submit_answer(
    db: AsyncSession, user_id: int, session_id: int, payload: AnswerSubmitRequest
) -> Dict[str, Any]:
    """提交当前题目的回答 → 规则评分 → 推进到下一题。"""
    session = await _load_session(db, session_id, user_id)

    if session.status != SESSION_STATUS_ONGOING:
        raise HTTPException(
            status_code=400,
            detail=f"当前状态为 {session.status}，仅 ongoing 状态可提交回答",
        )
    if session.current_question_no <= 0:
        raise HTTPException(status_code=400, detail="面试尚未开始，请先调用 start 接口")
    if session.current_question_no > session.total_questions:
        raise HTTPException(
            status_code=400, detail="全部题目已作答，请调用 end 接口生成报告"
        )

    answer_text = (payload.answer_text or "").strip()
    if not answer_text:
        raise HTTPException(status_code=400, detail="回答内容不能为空")

    questions = await _load_questions(db, session.id)
    current = next(
        (q for q in questions if q.question_no == session.current_question_no), None
    )
    if current is None:
        raise HTTPException(
            status_code=500,
            detail=f"会话数据异常：第 {session.current_question_no} 题不存在",
        )

    # 一题一答：question_id 在表上有 UNIQUE 约束，这里先给出可读提示
    existing = await db.execute(
        select(InterviewAnswer).where(InterviewAnswer.question_id == current.id)
    )
    if existing.scalar_one_or_none() is not None:
        raise HTTPException(
            status_code=400, detail=f"第 {current.question_no} 题已作答，请勿重复提交"
        )

    # 评分由 Core 负责（纯规则、确定性）
    scored = interview_core.score_answer(_question_to_dict(current), answer_text)
    answer = InterviewAnswer(
        question_id=current.id,
        answer_text=answer_text,
        audio_url=payload.audio_url,  # 预留字段，当前允许为空
        score=scored["score"],
        technical_score=scored["technical_score"],
        logic_score=scored["logic_score"],
        expression_score=scored["expression_score"],
        adaptability_score=scored["adaptability_score"],
        feedback=scored["feedback"],
    )
    db.add(answer)

    # 推进题号；超过总题量即表示全部作答完毕
    session.current_question_no += 1
    await db.commit()
    await db.refresh(answer)
    await db.refresh(session)

    all_answered = session.current_question_no > session.total_questions
    next_question = None
    if not all_answered:
        next_question = next(
            (q for q in questions if q.question_no == session.current_question_no), None
        )

    return {
        "answer": _answer_to_dict(answer),
        "session": _session_to_dict(session),
        "next_question": _question_to_dict(next_question) if next_question else None,
        "all_answered": all_answered,
        "message": "全部题目已作答，请调用 end 接口生成报告" if all_answered else "回答已提交",
    }


async def end_session(
    db: AsyncSession, user_id: int, session_id: int
) -> Dict[str, Any]:
    """结束面试并生成报告。

    - ``finished`` 且报告已存在 → 幂等返回既有报告（便于前端重试）
    - ``created`` → 400（还没开始，无意义）
    - ``ongoing`` → 允许提前结束，按已作答内容出报告
    """
    session = await _load_session(db, session_id, user_id)

    if session.status == SESSION_STATUS_CREATED:
        raise HTTPException(status_code=400, detail="面试尚未开始，无法结束")

    if session.status == SESSION_STATUS_FINISHED:
        existing = await db.execute(
            select(InterviewReport).where(InterviewReport.session_id == session.id)
        )
        report = existing.scalar_one_or_none()
        if report is not None:
            return {
                "session": _session_to_dict(session),
                "report": _report_to_dict(report),
                "message": "面试已结束（返回既有报告）",
            }

    job = await _load_job(db, session.job_id)
    resume_skills = await _load_resume_skills(db, session.resume_id)
    questions = await _load_questions(db, session.id)
    answers = await _load_answers(db, session.id)

    # 报告汇总由 Core 负责（纯规则、确定性）
    summary = interview_core.build_report(session, questions, answers, job, resume_skills)

    report = InterviewReport(
        session_id=session.id,
        total_score=summary["total_score"],
        technical_score=summary["technical_score"],
        project_score=summary["project_score"],
        logic_score=summary["logic_score"],
        expression_score=summary["expression_score"],
        communication_score=summary["communication_score"],
        adaptability_score=summary["adaptability_score"],
        job_match_score=summary["job_match_score"],
        strengths=summary["strengths"],
        weaknesses=summary["weaknesses"],
        suggestions=summary["suggestions"],
    )
    db.add(report)

    session.status = SESSION_STATUS_FINISHED
    session.ended_at = datetime.utcnow()
    await db.commit()
    await db.refresh(report)
    await db.refresh(session)

    return {
        "session": _session_to_dict(session),
        "report": _report_to_dict(report),
        "message": "面试已结束",
    }


async def get_report(
    db: AsyncSession, user_id: int, session_id: int
) -> Dict[str, Any]:
    """获取面试报告。未结束时明确提示，不返回半成品数据。"""
    session = await _load_session(db, session_id, user_id)

    result = await db.execute(
        select(InterviewReport).where(InterviewReport.session_id == session.id)
    )
    report = result.scalar_one_or_none()
    if report is None:
        raise HTTPException(
            status_code=404,
            detail=f"面试会话 {session_id} 尚无报告，当前状态为 {session.status}，请先调用 end 接口",
        )
    return _report_to_dict(report)


# ============================================================
# 四、按需出题入口（经 Core 编排，Service 不直接接触 Agent / Validator）
# ============================================================
async def generate_next_question(
    db: AsyncSession,
    user_id: int,
    session_id: int,
    *,
    spark: Any = None,
    retriever: Any = None,
    use_rag: bool = False,
    retriever_kwargs: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """生成下一道面试题——**经由 Core**，Service 不直接调用 Agent / Validator。

    调用链::

        Service（归属校验）→ Core（流程编排）→ KnowledgeRetriever（可选）
                                                      → Agent（生成）→ Validator（校验）

    与 ``start_session`` 的**规则出题路径互不影响**：本入口是「大模型出题」通道，
    需调用方显式触发，**不会自动接管**现有流程（否则会改变现有业务效果）。

    分工：
    - **Service**：归属校验（不存在 / 非本人 → 404）——唯一需要 HTTP 语义的一步
    - **Core**：session → context → plan → Retriever → Agent → Validator 全流程
    - **Agent / Validator**：只被 Core 调用，Service **不 import、不依赖**
    - **RAG**：``use_rag=True`` 时由 Core 组装真实链路（Embedding + 向量库 + 检索器）；
      ``retriever=`` 可直接注入现成检索器（优先级更高）。**默认都不开**——
      不显式要求就不会打开 RAG。Service 只做透传，**不自己组装**检索器
      （组装归 ``services.knowledge_rag``，见那里的模块文档）。
      ``retriever_kwargs=`` 一并透传给 Core，**只在 ``use_rag=True`` 组装时生效**
      （``top_k`` / ``min_score`` / ``category`` / ``document_id`` / ``dedup``），
      默认 ``None`` ⇒ 行为逐字节不变；注入了 ``retriever`` 时它不被读取。

    返回值即 Core 的结果字典（字段集恒定，见
    ``interview_core.QUESTION_RESULT_FIELDS``）。状态类错误（已结束 / 已答完 /
    生成失败 / 校验失败）以 ``ok=False`` 返回，**不抛 HTTPException**，
    便于数字人前端按字段分支处理。
    知识检索失败**不是失败**，只在 ``warnings`` 里记 ``knowledge_retrieval_failed``。
    """
    await _load_session(db, session_id, user_id)  # 归属校验：不存在/非本人 → 404
    return await interview_core.generate_next_question(
        db, session_id, user_id=user_id, spark=spark,
        retriever=retriever, use_rag=use_rag,
        retriever_kwargs=retriever_kwargs,
    )
