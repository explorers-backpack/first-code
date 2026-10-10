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
（v2：``_deliver`` 现被全部交互入口复用，见其函数文档。）

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

import os
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Tuple

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
from schemas.interview import (
    AnswerSubmitRequest,
    FollowUpRequest,
    SessionCreateRequest,
)
from services import (
    avatar_interview,
    interview_core,
    interview_followup,
    interview_next_flow,
)
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
# 〇·二、数字人「LLM 单题出题」开关（**默认开启**）
# ============================================================
# 打开后，数字人面试的「下一题」由大模型**逐题**生成（携带历史回答 + 可选知识），
# 代价是**答题提交路径上多一次网络调用**（数秒）；模型失败自动回退为规则计划的
# 第 k 项，因此出题失败**不会**打断面试。
#
# ★ 默认开启（不配置即生效）：产品语义上，数字人面试本来就该「听一句、追一句」。
#
# ★★ 但默认开启会撞上另一条硬约定——「**回归套件必须不受部署配置影响、且不真打网络**」：
#   本机 ``.env`` 带着**真实** ``SPARK_*`` 凭据，而 ``main.spark_api`` 是**无条件
#   构造**的模块级单例（``SparkAPI()`` 不看凭据是否存在），``_default_spark()``
#   必然拿得到它 ⇒ 任何走数字人闭环的套件都会在**提交答案**时真的去打星火
#   （实测单次 > 8s、消耗配额、结果依赖网络）。
#   ⇒ 因此 ``tests/regression_env.py`` 把本开关**钉成 ``"0"``**（回归里等价于
#     「显式关闭」），套件行为与引入本能力之前逐字节一致；
#     两个状态都由 ``tests/test_avatar_llm_flow.py`` 显式覆盖。
#
# 关闭方式（回到「整场规则题目一次性落库、下一题从库里取」）：``.env`` 里
# ``INTERVIEW_AVATAR_LLM_QUESTIONS=0``。见 ``.env.example``。
AVATAR_LLM_QUESTIONS_ENV = "INTERVIEW_AVATAR_LLM_QUESTIONS"

#: 视为「打开」的取值（大小写不敏感；**未配置 / 空串也等价于此**）。
_AVATAR_LLM_TRUTHY = ("1", "true", "yes", "on")
#: 视为「关闭」的取值（必须**显式**写，才会回到旧行为）。
_AVATAR_LLM_FALSY = ("0", "false", "no", "off")


def avatar_llm_questions_enabled() -> bool:
    """数字人面试是否走「LLM 单题出题」链路。

    - **未配置 / 空串 ⇒ ``True``**（**默认开启**）
    - ``1`` / ``true`` / ``yes`` / ``on`` ⇒ ``True``
    - ``0`` / ``false`` / ``no`` / ``off`` ⇒ ``False``（显式关闭 = 旧行为）
    - **其它取值 ⇒ 记一条 ERROR 日志后按默认（``True``）处理**——非法配置
      **不静默**（有日志可查），但也不让一次配置笔误把请求打成 500。

    ★ 默认开启 ⇒ **回归环境必须显式关闭它**：``tests/regression_env.py`` 把它钉成
    ``"0"``（本机 ``.env`` 带真实 ``SPARK_*``，不钉就会在提交答案时真打星火）。

    在**调用时**读环境变量（而非 import 期缓存成常量）：这样测试可以按用例
    开关它，部署侧改 ``.env`` 后重启即生效、不会出现「半个进程用旧值」。
    """
    raw = (os.environ.get(AVATAR_LLM_QUESTIONS_ENV) or "").strip().lower()
    if raw in _AVATAR_LLM_FALSY:
        return False
    if not raw or raw in _AVATAR_LLM_TRUTHY:
        return True
    print(
        f"[interview][ERROR] {AVATAR_LLM_QUESTIONS_ENV}={raw!r} 取值非法，"
        f"已按默认（开启）处理；允许：{'/'.join(_AVATAR_LLM_TRUTHY + _AVATAR_LLM_FALSY)}"
    )
    return True


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
    本函数只回答「这份结果交给谁」。

    覆盖范围（**全部交互入口**，一处实现、多处调用）::

        start_session        → 交付第一题（开场）
        get_current_question → 交付当前待答题（重播 / 语音播报）
        submit_answer        → 交付评分反馈 + 下一题
        end_session          → 交付结束语 + 报告

    只读投影 ``get_session_detail`` / ``get_report`` **不分流**：它们不产出
    「交给通道去表演」的交互内容，只是把已有数据原样回读。

    ``avatar`` 分支当前是**空实现**（``avatar_interview.enter`` 原样透传），
    因此两种模式的交付内容**逐字段相同**——接入数字人后，只需改 ``enter`` 的
    函数体，这里与 ``text`` 分支都不用动。
    """
    if session.interview_mode == INTERVIEW_MODE_AVATAR:
        return await avatar_interview.enter(db, session, payload)
    return payload


# ============================================================
# 二·六、数字人闭环的两项**显式能力**（预检 / ASR）
# ============================================================
# 为什么在这里再包一层（而不是让编排层直接调 ``avatar_interview``）：
#   ① 本模块是**唯一接触 DB 的地方** ⇒ ``session`` 行只能在这里取；
#   ② 归属校验（``_load_session`` 的 404）必须复用既有口径，不另造一套；
#   ③ 编排层 ``interview_orchestrator`` 因此**只需依赖 interview_service**，
#      不必新增「编排 → 通道层」的边（分层保持单向、依赖面最小）。
#
# 两者都**不抛 provider 异常**：失败以 ``ok=False`` + ``status`` 如实上报
# （见 ``avatar_interview`` 的 ``CHANNEL_*``），只有「会话不存在 / 非本人」
# 才照旧抛 404（与其余入口一致）。
async def ensure_avatar(
    db: AsyncSession, user_id: int, session_id: int
) -> Dict[str, Any]:
    """**初始化 / 预检数字人会话**（幂等）——「Avatar 启动」这一步。

    返回 ``avatar_interview.ensure_ready`` 的恒定键集信封
    （``ok`` / ``status`` / ``avatar_session_id`` / ``stream_url`` / …）。
    数字人不可用时 ``ok=False``，**不影响面试流程**（交付仍会降级为文字）。
    """
    session = await _load_session(db, session_id, user_id)
    return await avatar_interview.ensure_ready(db, session)


async def capture_avatar_answer(
    db: AsyncSession, user_id: int, session_id: int, *, timeout_ms: int = 0
) -> Dict[str, Any]:
    """**取用户语音回答的文本**（讯飞虚拟人内置 **ASR**）。

    返回 ``avatar_interview.capture_answer`` 的恒定 8 键信封
    （``ok`` / ``status`` / ``text`` / ``audio_url`` / ``avatar_session_id`` / …）。

    拿到 ``text`` 后由编排层交给 :func:`submit_answer` 评分——
    本函数**只取文本、不落库、不评分**（职责单一，便于单测）。
    """
    session = await _load_session(db, session_id, user_id)
    return await avatar_interview.capture_answer(db, session, timeout_ms=timeout_ms)


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


# ============================================================
# 二·七、开始面试的**共享原语**（规则出题 / 策略化出题 共用）
# ============================================================
# 为什么要把「校验 → 落库 → 置 ongoing → 交付第一题」拆成原语：
#   ``start_session`` 与 ``begin_session_with_questions`` 只差**题目从哪来**
#   （前者内部调 Core 的确定性规则出题，后者接收调用方算好的题目，例如出题
#   **策略层**的产出）。除题目来源外，两处的归属校验 / 状态守卫 / 落库 /
#   状态推进 / 交付分流必须**逐字节一致**，因此共用同一批原语。
#   这样「换出题方式」就只是换「谁来提供 items」，而不是复制一遍流程。
def _ensure_startable(session: InterviewSession) -> None:
    """开始面试的**前置状态守卫**（两个入口共用，口径只有一份）。"""
    if session.status == SESSION_STATUS_FINISHED:
        raise HTTPException(status_code=400, detail="面试已结束，无法重新开始")
    if session.status == SESSION_STATUS_ONGOING:
        raise HTTPException(
            status_code=400, detail="面试已在进行中，请直接获取当前题目"
        )


def _persist_question_items(
    db: AsyncSession, session: InterviewSession, items: List[Mapping[str, Any]]
) -> int:
    """把出题结果**逐条**落库（**不提交**），返回落库条数。

    ``items`` 中每一项恒为 6 个字段（与 ``interview_core.build_question_plan``
    的产出一致）：``question_no`` / ``question`` / ``question_type`` /
    ``topic`` / ``difficulty`` / ``expected_points``。
    """
    added = 0
    for item in items:
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
        added += 1
    return added


async def _activate_session(
    db: AsyncSession, session: InterviewSession, items: List[Mapping[str, Any]]
) -> InterviewSession:
    """落库题目 + 把会话置 ``ongoing`` 并指向第 1 题（**共享原语**）。

    「校验之后」的全部副作用都收在这里——两个开始入口因此不可能出现
    「一个置了 ``started_at``、另一个没置」这类漂移。
    """
    _persist_question_items(db, session, items)
    session.status = SESSION_STATUS_ONGOING
    session.started_at = datetime.utcnow()
    session.current_question_no = 1
    await db.commit()
    await db.refresh(session)
    return session


async def _start_payload(
    db: AsyncSession, session: InterviewSession, job: Optional[Job]
) -> Dict[str, Any]:
    """构造「面试已开始」的**交付载荷**（会话 + 第一题 + 岗位名）。"""
    questions = await _load_questions(db, session.id)
    first = questions[0] if questions else None
    return {
        "session": _session_to_dict(session),
        "question": _question_to_dict(first) if first else None,
        "job_name": job.job_name if job else None,
        "message": "面试已开始",
    }


def _questions_to_persist(
    session: InterviewSession, plan: List[Mapping[str, Any]]
) -> List[Mapping[str, Any]]:
    """开场时**落库哪些题目**（数字人 LLM 单题链的分岔点）。

    - **开关显式关闭（``=0``）/ 非数字人**：整场一次性落库——与引入本能力之前
      **逐字节一致**（下一题仍从库里取）。
    - **数字人 + 开关打开（默认）**：只落**第 1 题**。开场题保持规则题（确定性、
      零延迟、开场惯例固定），第 2 题起由 :func:`generate_and_persist_next` 逐题生成。

    抽成独立函数是为了让 ``start_session`` 的源码里**仍然只出现规则出题**
    （守卫测试断言 ``start_session`` 直调 ``build_question_plan``，
    且不认识任何出题策略）。
    """
    if session.interview_mode == INTERVIEW_MODE_AVATAR and avatar_llm_questions_enabled():
        return list(plan[:1])
    return plan


async def begin_session_with_questions(
    db: AsyncSession,
    user_id: int,
    session_id: int,
    items: List[Mapping[str, Any]],
) -> Dict[str, Any]:
    """**公开原语**：把**给定的**出题结果落库、置 ``ongoing``，并交付第一题。

    与 :func:`start_session` 的唯一差别是**题目从哪来**：

    - ``start_session``：内部调 ``interview_core.build_question_plan``
      （确定性规则、整场批量）；
    - 本函数：接收调用方算好的题目——典型调用方是出题**策略层**
      （``question_generator``，由编排层 ``interview_orchestrator.start_questions``
      按 ``mode`` 选策略后落库）。

    除题目来源外，**归属校验 / 状态守卫 / 落库 / 状态推进 / 交付分流**与
    ``start_session`` 完全一致（共用 :func:`_ensure_startable` /
    :func:`_activate_session` / :func:`_start_payload`），因此 text 会话下
    两者的返回形状与副作用**逐字段相同**。
    """
    session = await _load_session(db, session_id, user_id)
    _ensure_startable(session)
    if not items:
        raise HTTPException(status_code=500, detail="题目计划生成失败，请检查会话配置")
    await _activate_session(db, session, items)
    job = await _load_job(db, session.job_id)
    # 交付边界分流：text 原样返回（默认路径）；avatar 走数字人通道入口
    return await _deliver(db, session, await _start_payload(db, session, job))


async def start_session(
    db: AsyncSession, user_id: int, session_id: int
) -> Dict[str, Any]:
    """开始面试：生成整场题目计划并置为 ``ongoing``。

    题目计划一次性生成（当前为确定性规则，不依赖作答内容），
    因此 ``start`` 可安全重入判断——重复调用会被状态校验拦下。

    **交互模式分流**（本次新增）：状态守卫与出题**两种模式完全共享**，
    只在最后「交付第一题」这一步按 ``session.interview_mode`` 分流——
    ``text`` 原样返回（默认路径，行为不变），``avatar`` 走数字人通道入口。

    ★ 本入口的题目来源**固定**为 ``interview_core.build_question_plan``
    （确定性规则、整场批量），**不认识任何出题策略**——「由策略决定题目」
    的路径是 :func:`begin_session_with_questions`（由编排层的策略化入口调用）。
    这条分工是刻意的：默认路径不因引入策略而变。

    ★ **落库几道题**由 :func:`_questions_to_persist` 决定：默认（开关打开）只落
    **第 1 题**（第 2 题起由 :func:`generate_and_persist_next` 逐题生成）；
    开关被显式关掉（``INTERVIEW_AVATAR_LLM_QUESTIONS=0``）时整场一次性落库。
    """
    session = await _load_session(db, session_id, user_id)
    _ensure_startable(session)

    job = await _load_job(db, session.job_id)
    resume_skills = await _load_resume_skills(db, session.resume_id)
    # 出题计划由 Core 负责（纯规则、确定性）；本层只负责读数据与落库
    plan = interview_core.build_question_plan(session, job, resume_skills)

    if not plan:
        raise HTTPException(status_code=500, detail="题目计划生成失败，请检查会话配置")

    await _activate_session(db, session, _questions_to_persist(session, plan))

    # 交付边界分流：text 原样返回（默认路径）；avatar 走数字人通道入口
    return await _deliver(db, session, await _start_payload(db, session, job))


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
        return await _deliver(db, session, {
            "session_id": session.id,
            "status": session.status,
            "question_no": None,
            "total_questions": session.total_questions,
            "question": None,
            "answered": False,
            "all_answered": True,
            "message": "面试已结束，请调用 report 接口查看面试报告",
        })

    if session.status == SESSION_STATUS_CREATED or session.current_question_no <= 0:
        return await _deliver(db, session, {
            "session_id": session.id,
            "status": session.status,
            "question_no": None,
            "total_questions": session.total_questions,
            "question": None,
            "answered": False,
            "all_answered": False,
            "message": "面试尚未开始，请先调用 start 接口",
        })

    if session.current_question_no > session.total_questions:
        return await _deliver(db, session, {
            "session_id": session.id,
            "status": session.status,
            "question_no": None,
            "total_questions": session.total_questions,
            "question": None,
            "answered": False,
            "all_answered": True,
            "message": "全部题目已作答，请调用 end 接口生成报告",
        })

    current = next(
        (q for q in questions if q.question_no == session.current_question_no), None
    )
    if current is None:
        raise HTTPException(
            status_code=500,
            detail=f"会话数据异常：第 {session.current_question_no} 题不存在，请重新创建面试",
        )

    return await _deliver(db, session, {
        "session_id": session.id,
        "status": session.status,
        "question_no": current.question_no,
        "total_questions": session.total_questions,
        "question": _question_to_dict(current),
        "answered": current.id in answered_ids,
        "all_answered": False,
        "message": "ok",
    })


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
    next_question_dict = None
    if not all_answered:
        next_question = next(
            (q for q in questions if q.question_no == session.current_question_no), None
        )
        if next_question is not None:
            next_question_dict = _question_to_dict(next_question)
        elif (
            session.interview_mode == INTERVIEW_MODE_AVATAR
            and avatar_llm_questions_enabled()
        ):
            # ★ 数字人 LLM 单题链：库里还没有下一题 ⇒ 现场生成 + 落库。
            #   **必须在 ``_deliver`` 之前完成**——交付出去的题一定已在库里，
            #   否则重连 / 重播会拿到空题。生成失败时该函数自行降级
            #   （规则兜底 → 仍失败则 ``question=None``），**不抛异常**，
            #   因此不会把「答完这一题」这件事连带打断。
            generated = await generate_and_persist_next(db, user_id, session_id)
            next_question_dict = generated.get("question")

    result = {
        "answer": _answer_to_dict(answer),
        "session": _session_to_dict(session),
        "next_question": next_question_dict,
        "all_answered": all_answered,
        "message": "全部题目已作答，请调用 end 接口生成报告" if all_answered else "回答已提交",
    }
    # 交付边界分流：text 原样返回（默认路径）；avatar 走数字人通道入口
    return await _deliver(db, session, result)


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
            return await _deliver(db, session, {
                "session": _session_to_dict(session),
                "report": _report_to_dict(report),
                "message": "面试已结束（返回既有报告）",
            })

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

    result = {
        "session": _session_to_dict(session),
        "report": _report_to_dict(report),
        "message": "面试已结束",
    }
    # 交付边界分流：text 原样返回（默认路径）；avatar 走数字人通道入口
    return await _deliver(db, session, result)


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
      ``retriever=`` 可直接注入现成检索器（优先级更高）。**本函数级默认 ``False``**
      （保守库层默认 ⇒ 内部直接调用不隐式检索）；**生产 HTTP 契约层已默认开启**
      （``schemas.interview.NextQuestionRequest.use_rag`` 默认 ``True``，由路由透传进来）。
      Service 只做透传，**不自己组装**检索器
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


# ============================================================
# 四·五、数字人 LLM 单题链：生成下一题并**落库**
# ============================================================
# 与 ``generate_next_question`` 的分工：后者是**只读的按需出题接缝**（不落库、
# 不推进题号），本节的函数把「生成」与「落库」合成一步，供数字人答题提交路径
# 直接使用——因为那条路径必须保证「交付出去的题一定已在库里」。
#
# ★ 为什么这里**不走出题策略层**（``question_generator``）：
#   本模块有一条守卫断言——``interview_service`` **绝不 import**
#   ``services.question_generator``（策略选择在 Service 之上）。因此策略层只从
#   编排层进入生产链路（``interview_orchestrator.start_questions``），
#   Service 侧的落库原语则对「题目从哪来」保持中立。
#
# ★ 另一条守卫：本模块**源码全文**不得出现 Agent / Context 的接缝字面量
#   （连 docstring / 注释都不行，因为判据是 ``inspect.getsource``）⇒
#   「取上下文 + 检索 + 调模型」三步**全部委托**
#   ``interview_next_flow.generate_candidate``，本节只保留
#   「归属校验 + 状态机口径 + 幂等 + 规则兜底 + 落库」。
#
#: 题目项的 6 个字段（与 ``interview_core.build_question_plan`` 的产出一致）。
_QUESTION_ITEM_FIELDS: Tuple[str, ...] = (
    "question_no",
    "question",
    "question_type",
    "topic",
    "difficulty",
    "expected_points",
)

#: ``generate_and_persist_next`` 返回值的**恒定键集**（成功与失败**同形状**）。
NEXT_QUESTION_FIELDS: Tuple[str, ...] = ("ok", "question", "source", "errors", "error")

#: ``source`` 取值：题目来自大模型生成。
SOURCE_LLM = "llm"
#: ``source`` 取值：大模型失败，题目来自**规则兜底**（``build_question_plan`` 第 k 项）。
SOURCE_RULE_FALLBACK = "rule_fallback"
#: ``source`` 取值：幂等命中——该题号已落库，直接返回既有行。
SOURCE_EXISTING = "existing"
#: ``source`` 取值：全部题目已作答，没有下一题。
SOURCE_ALL_ANSWERED = "all_answered"
#: ``source`` 取值：两条路都没能出题（LLM 失败且规则兜底也取不到）。
SOURCE_FAILED = "failed"

#: 稳定错误码：下一题生成失败（LLM 与规则兜底都没成功）。
ERROR_QUESTION_GENERATE_FAILED = "question_generate_failed"


async def _rule_plan(
    db: AsyncSession, session: InterviewSession
) -> List[Dict[str, Any]]:
    """本场的**规则出题计划**（6 字段题目项，题号升序）。

    用途有两个：① 兜底取第 k 项；② 作为下一题 Prompt 里的「本场面试计划」。
    规则出题是确定性的，因此它与「整场一次性落库」得到的题目**逐字段相同**。
    """
    job = await _load_job(db, session.job_id)
    resume_skills = await _load_resume_skills(db, session.resume_id)
    return [
        dict(item)
        for item in interview_core.build_question_plan(session, job, resume_skills)
    ]


async def _rule_plan_item(
    db: AsyncSession, session: InterviewSession, question_no: int
) -> Optional[Dict[str, Any]]:
    """**规则兜底**：取 ``build_question_plan`` 的第 ``question_no`` 项。

    规则出题是确定性的，因此「只落第 1 题」时按题号取第 k 项，与「整场一次性
    落库」得到的第 k 题**逐字段相同**——兜底出来的题与正常路径同源、同质。
    """
    for item in await _rule_plan(db, session):
        if item["question_no"] == question_no:
            return item
    return None


async def generate_and_persist_next(
    db: AsyncSession,
    user_id: int,
    session_id: int,
    *,
    spark: Any = None,
    retriever: Any = None,
    use_rag: bool = True,
    retriever_kwargs: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """生成**下一题**并**落库**（数字人 LLM 单题链的核心步骤）。

    这是「只落第 1 题」模式的补题入口：``submit_answer`` 在评分推进后发现
    **库里没有下一题**时调用它，把题目补上**再**交付给通道播报。

    流程（**两级 fail-soft**，绝不因为出题失败打断面试）
    ----------------------------------------------------
    1. **归属 / 状态校验**：不存在 / 非本人 → 404；非 ``ongoing`` / 未开始 → 400
       （与其余入口同口径）。
    2. **幂等**：目标题号已落库 ⇒ 直接返回既有行（``source="existing"``）——
       重复调用**不会插出重复题**。
    3. **组装 + 生成**：委托 ``interview_next_flow.generate_candidate``——取上下文包
       （简历 + 岗位 + **历史回答** + 进度）→ 知识检索（可选增强，故障静默降级）
       → 调用模型（``interview_next/`` Prompt 组，携带历史回答）。
       ★ 接缝调用**全部收在组装层**，Service 侧只管落库与状态机口径；
       题号以**状态机**（``current_question_no``）为准，不信模型自报的题号。
    4. **规则兜底**：生成失败 ⇒ ``build_question_plan`` 的第 k 项
       （确定性，必然可得）。
    5. **落库**：投影成 6 字段写入 ``InterviewQuestion``。
       两条路都失败 ⇒ ``ok=False`` + ``errors=["question_generate_failed"]``，
       ``question=None``——**不抛异常**（调用方只需判 ``question`` 是否为空）。

    返回（恒定键集 ``NEXT_QUESTION_FIELDS``）::

        {
          "ok": bool,
          "question": dict | None,   # 已落库题目的序列化结果（含 id）
          "source": "llm" | "rule_fallback" | "existing" | "all_answered" | "failed",
          "errors": [稳定错误码, ...],   # 失败时 ``question_generate_failed`` **恒在最前**，
                                        # 其后跟 ``agent_failed`` 等诊断码
          "error": str | None,
        }

    ``use_rag`` 默认 ``True``（与按需出题入口的 HTTP 契约默认一致）；
    注入 ``retriever`` 时以注入者为准。
    """
    session = await _load_session(db, session_id, user_id)  # 归属校验：404

    if session.status != SESSION_STATUS_ONGOING:
        raise HTTPException(
            status_code=400,
            detail=f"当前状态为 {session.status}，仅 ongoing 状态可生成下一题",
        )
    if session.current_question_no <= 0:
        raise HTTPException(status_code=400, detail="面试尚未开始，请先调用 start 接口")

    question_no = session.current_question_no
    if question_no > session.total_questions:
        return {
            "ok": True,
            "question": None,
            "source": SOURCE_ALL_ANSWERED,
            "errors": [],
            "error": None,
        }

    # ① 幂等：该题号已落库 ⇒ 直接返回（重复调用安全）
    for row in await _load_questions(db, session.id):
        if row.question_no == question_no:
            return {
                "ok": True,
                "question": _question_to_dict(row),
                "source": SOURCE_EXISTING,
                "errors": [],
                "error": None,
            }

    # ② 组装（上下文 + 知识检索）并调用模型：接缝调用集中在组装层，
    #    Service 侧只保留「落库 + 状态机口径」——本模块有源码级守卫，
    #    全文不得出现 Agent / Context 的接缝字面量。
    plan_items = await _rule_plan(db, session)
    candidate = await interview_next_flow.generate_candidate(
        db,
        session,
        plan_items,
        question_no=question_no,
        spark=spark,
        retriever=retriever,
        use_rag=use_rag,
        retriever_kwargs=retriever_kwargs,
    )

    errors: List[Any] = []
    if candidate.get("ok"):
        item: Optional[Dict[str, Any]] = {
            key: candidate.get(key) for key in _QUESTION_ITEM_FIELDS
        }
        item["expected_points"] = list(item.get("expected_points") or [])
        item["question_no"] = question_no  # 题号以状态机为准（权威）
        source = SOURCE_LLM
    else:
        # ⑤ 规则兜底：LLM 失败不放弃这一题（出题失败不该让面试断掉）
        errors = [interview_core.ERROR_AGENT_FAILED]
        item = await _rule_plan_item(db, session, question_no)
        source = SOURCE_RULE_FALLBACK if item is not None else SOURCE_FAILED

    if item is None:
        # 稳定错误码**恒在最前**：调用方按它分支即可；其余码跟在后面做诊断。
        stable: List[Any] = [ERROR_QUESTION_GENERATE_FAILED]
        stable += [code for code in errors if code not in stable]
        return {
            "ok": False,
            "question": None,
            "source": SOURCE_FAILED,
            "errors": stable,
            "error": candidate.get("error") or "下一题生成失败",
        }

    row = InterviewQuestion(
        session_id=session.id,
        question_no=item["question_no"],
        question=item["question"],
        question_type=item["question_type"],
        topic=item["topic"],
        difficulty=item["difficulty"],
        expected_points=item["expected_points"],
    )
    db.add(row)
    await db.commit()
    await db.refresh(row)

    return {
        "ok": True,
        "question": _question_to_dict(row),
        "source": source,
        "errors": errors,
        "error": None,
    }


# ============================================================
# 五、追问入口（数字人面试的「继续提问」）
# ============================================================
async def generate_followup(
    db: AsyncSession,
    user_id: int,
    session_id: int,
    payload: FollowUpRequest,
    *,
    spark: Any = None,
    retriever: Any = None,
) -> Dict[str, Any]:
    """根据候选人的回答生成**一道追问** —— 经 ``interview_followup`` 编排。

    调用链::

        Service（归属校验 + 交付分流）
            └── interview_followup.generate_followup
                    ├── ② 分析回答        interview_core.score_answer（纯规则）
                    ├── ③ Retriever 检索  interview_core.resolve_retriever / retrieve_knowledge
                    └── ④ Agent 生成追问  interview_followup_agent.generate_followup

    分工与 ``generate_next_question`` **完全一致**：

    - **Service**：归属校验（不存在 / 非本人 → 404）+ **交付分流**（``_deliver``）
    - **Core**：评分 / 检索接缝 / 只读加载——Service 不直接 import Agent / Validator
    - **RAG**：``use_rag=True`` 才组装真实链路；``retriever=`` 可直接注入
      （优先级更高）。开关来自 ``payload.use_rag``（``FollowUpRequest`` 默认 ``True``
      ⇒ 生产入口默认检索；显式传 ``False`` 才关闭）。

    **交付分流**是本函数与 ``generate_next_question`` 的关键差别：追问是「要立刻
    播报出去的一句话」，因此结果会经 ``_deliver`` 交给对应通道——
    ``avatar`` 模式走 ``avatar_interview.enter``（数字人继续提问），
    ``text`` 模式原样返回（默认路径，行为不变）。

    返回值即 ``interview_followup`` 的结果字典（字段集恒定，见
    ``FOLLOWUP_RESULT_FIELDS``）。状态类错误以 ``ok=False`` 返回，**不抛
    HTTPException**，便于数字人前端按字段分支处理。

    **不落库**：本函数不写题目 / 作答 / 报告，也不推进题号——追问的持久化
    （含 ``interview_context.increment_follow_up``）由调用方按业务需要决定，
    因此同一入参重复调用是安全的。
    """
    session = await _load_session(db, session_id, user_id)  # 归属校验：404

    # 只把**显式给出**的检索参数传下去：都缺省 → None ⇒ 组装器用自身默认值
    retriever_kwargs: Dict[str, Any] = {}
    if payload.top_k is not None:
        retriever_kwargs["top_k"] = payload.top_k
    if payload.min_score is not None:
        retriever_kwargs["min_score"] = payload.min_score

    result = await interview_followup.generate_followup(
        db,
        session_id,
        answer_text=payload.answer_text,
        question_no=payload.question_no,
        user_id=user_id,
        spark=spark,
        retriever=retriever,
        use_rag=payload.use_rag,
        retriever_kwargs=retriever_kwargs or None,
    )
    # 交付边界分流：text 原样返回（默认路径）；avatar 走数字人通道入口
    return await _deliver(db, session, result)
