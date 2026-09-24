# -*- coding: utf-8 -*-
"""AI 模拟面试 · InterviewContext 上下文管理。

职责
----
为每个 ``InterviewSession`` 维护一份**独立的**面试上下文，记录这场面试
「进行到哪个阶段、问过哪些问题、覆盖了哪些知识点、哪些是薄弱点、追问了几次、
是否已达到题量上限」。

对外约定（与 ``services/`` 其它模块一致）
----------------------------------------
- 函数以 ``db: AsyncSession`` 为第一参数（依赖注入），**返回普通 dict**，
  不依赖 FastAPI 的请求上下文，可脱离 HTTP 单独测试。
- 业务错误抛 ``HTTPException``（404 会话/上下文不存在、400 参数非法）。

存储方案（为什么这样选）
------------------------
1. **独立表 ``interview_context``，与 ``interview_session`` 1:1**。
   ``main.py`` 的 lifespan 用 ``Base.metadata.create_all`` 建表——**新表会被自动创建**，
   而在**既有表上加列不会被补上**（需人工 ALTER）。独立表因此「零 DDL、零迁移」，
   且完全不触碰既有表结构。
2. **不重复存** ``current_question_no`` / ``total_questions``：二者的权威来源是
   ``interview_session`` 的既有列（状态机正在使用），Context 读取时**实时投影**，
   避免双写不一致。
3. **复用既有数据库会话**：全部函数使用调用方注入的 ``db``（来自
   ``database.get_db``），本模块**不新建 engine / 连接**。

语义说明
--------
- ``total_questions``：本场**计划**题量（来自 session）
- ``max_questions``  ：本场**允许的最大**题量（含追问），达到即应进入收尾
- ``asked_questions``：有序日志，**允许重复**（同一问题可能在追问中再问）
- ``covered_topics`` / ``weak_topics``：集合语义，**自动去重**
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import (
    INTERVIEW_STAGES,
    InterviewContext,
    InterviewSession,
)

# 默认阶段与题量上限
DEFAULT_STAGE = "introduction"
DEFAULT_MAX_QUESTIONS = 10

_STAGE_SET = frozenset(INTERVIEW_STAGES)

# update_context 允许直接改写的字段（其余字段由专用函数维护，防止绕过校验）
_UPDATABLE_FIELDS = (
    "current_stage",
    "asked_questions",
    "covered_topics",
    "weak_topics",
    "follow_up_count",
    "max_questions",
)


# ============================================================
# 内部工具
# ============================================================
async def _load_session(db: AsyncSession, session_id: int) -> InterviewSession:
    """取会话行；不存在则 404。"""
    session = (
        await db.execute(
            select(InterviewSession).where(InterviewSession.id == session_id)
        )
    ).scalar_one_or_none()
    if session is None:
        raise HTTPException(status_code=404, detail=f"面试会话 {session_id} 不存在")
    return session


async def _load_context(
    db: AsyncSession, session_id: int
) -> InterviewContext:
    """取上下文行；不存在则 404（提示先创建）。"""
    ctx = (
        await db.execute(
            select(InterviewContext).where(InterviewContext.session_id == session_id)
        )
    ).scalar_one_or_none()
    if ctx is None:
        raise HTTPException(
            status_code=404,
            detail=f"面试会话 {session_id} 尚未创建上下文，请先调用 create_context",
        )
    return ctx


def _as_list(value: Any) -> List[Any]:
    """把 JSON 列读成 list（兼容 NULL / 非 list 脏数据）。"""
    return list(value) if isinstance(value, list) else []


def asked_count(session: InterviewSession, ctx: InterviewContext) -> int:
    """已提问数量：取「状态机题号」与「上下文提问日志长度」的较大值。

    取较大值是为了在任一侧尚未同步时都不误判：
    - 只推进了状态机、还没调用 ``add_asked_question`` → 用题号
    - 记录了追问（题号未变）→ 用日志长度
    """
    return max(int(session.current_question_no or 0), len(_as_list(ctx.asked_questions)))


def _context_to_dict(
    session: InterviewSession, ctx: InterviewContext
) -> Dict[str, Any]:
    """投影成对外的完整 Context（含 9 个必需字段）。"""
    asked = _as_list(ctx.asked_questions)
    max_q = int(ctx.max_questions or 0)
    used = asked_count(session, ctx)
    return {
        # ---- 必需字段 ----
        "session_id": session.id,
        "current_question_no": session.current_question_no,
        "current_stage": ctx.current_stage,
        "asked_questions": asked,
        "covered_topics": _as_list(ctx.covered_topics),
        "weak_topics": _as_list(ctx.weak_topics),
        "follow_up_count": int(ctx.follow_up_count or 0),
        "total_questions": session.total_questions,
        "max_questions": max_q,
        # ---- 派生字段（只读，便于调用方判断）----
        "asked_count": used,
        "remaining_questions": max(max_q - used, 0),
        "max_reached": used >= max_q,
    }


# ============================================================
# 1. 创建初始 Context
# ============================================================
async def create_context(
    db: AsyncSession,
    session_id: int,
    max_questions: Optional[int] = None,
    stage: str = DEFAULT_STAGE,
) -> Dict[str, Any]:
    """为会话创建初始上下文（**幂等**：已存在则直接返回，不覆盖）。

    ``max_questions`` 缺省取 ``session.total_questions``；如需允许追问，
    显式传入更大的值。
    """
    session = await _load_session(db, session_id)

    if stage not in _STAGE_SET:
        raise HTTPException(
            status_code=400,
            detail=f"current_stage 取值非法，允许：{'/'.join(INTERVIEW_STAGES)}",
        )

    if max_questions is None:
        cap = int(session.total_questions or DEFAULT_MAX_QUESTIONS)
    else:
        cap = int(max_questions)
    if cap < 1:
        raise HTTPException(status_code=400, detail="max_questions 必须 >= 1")

    existing = (
        await db.execute(
            select(InterviewContext).where(InterviewContext.session_id == session_id)
        )
    ).scalar_one_or_none()
    if existing is not None:
        # 幂等：不覆盖已有上下文
        return _context_to_dict(session, existing)

    ctx = InterviewContext(
        session_id=session_id,
        current_stage=stage,
        asked_questions=[],
        covered_topics=[],
        weak_topics=[],
        follow_up_count=0,
        max_questions=cap,
    )
    db.add(ctx)
    await db.commit()
    await db.refresh(ctx)
    return _context_to_dict(session, ctx)


# ============================================================
# 2. 获取 Context
# ============================================================
async def get_context(db: AsyncSession, session_id: int) -> Dict[str, Any]:
    """读取会话上下文（含 9 个必需字段 + 派生字段）。"""
    session = await _load_session(db, session_id)
    ctx = await _load_context(db, session_id)
    return _context_to_dict(session, ctx)


# ============================================================
# 3. 更新 Context
# ============================================================
async def update_context(
    db: AsyncSession, session_id: int, **fields: Any
) -> Dict[str, Any]:
    """按字段更新上下文（只允许白名单字段，未知字段直接 400）。

    注意：JSON 列表必须**整体重新赋值**（SQLAlchemy 不追踪原地 mutate）。
    """
    unknown = [k for k in fields if k not in _UPDATABLE_FIELDS]
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"不支持更新的字段：{', '.join(sorted(unknown))}；"
            f"允许：{', '.join(_UPDATABLE_FIELDS)}",
        )
    if not fields:
        raise HTTPException(status_code=400, detail="未提供任何待更新字段")

    session = await _load_session(db, session_id)
    ctx = await _load_context(db, session_id)

    if "current_stage" in fields:
        stage = fields["current_stage"]
        if stage not in _STAGE_SET:
            raise HTTPException(
                status_code=400,
                detail=f"current_stage 取值非法，允许：{'/'.join(INTERVIEW_STAGES)}",
            )
        ctx.current_stage = stage

    if "max_questions" in fields:
        cap = int(fields["max_questions"])
        if cap < 1:
            raise HTTPException(status_code=400, detail="max_questions 必须 >= 1")
        ctx.max_questions = cap

    if "follow_up_count" in fields:
        count = int(fields["follow_up_count"])
        if count < 0:
            raise HTTPException(status_code=400, detail="follow_up_count 不能为负")
        ctx.follow_up_count = count

    for list_field in ("asked_questions", "covered_topics", "weak_topics"):
        if list_field in fields:
            value = fields[list_field]
            if not isinstance(value, list):
                raise HTTPException(
                    status_code=400, detail=f"{list_field} 必须是数组"
                )
            setattr(ctx, list_field, list(value))

    await db.commit()
    await db.refresh(ctx)
    return _context_to_dict(session, ctx)


# ============================================================
# 4. 增加已提问问题
# ============================================================
async def add_asked_question(
    db: AsyncSession, session_id: int, question: Any
) -> Dict[str, Any]:
    """追加一条已提问记录。

    ``question`` 支持字符串或 dict（如 ``{"question_no": 3, "question": "...",
    "stage": "technical"}``）。**允许重复**——同一问题可能在追问中再问一次。
    """
    if isinstance(question, str):
        entry: Any = question.strip()
        if not entry:
            raise HTTPException(status_code=400, detail="question 不能为空")
    elif isinstance(question, dict):
        if not question:
            raise HTTPException(status_code=400, detail="question 不能为空对象")
        entry = dict(question)
    else:
        raise HTTPException(
            status_code=400, detail="question 必须是字符串或对象"
        )

    session = await _load_session(db, session_id)
    ctx = await _load_context(db, session_id)

    asked = _as_list(ctx.asked_questions)
    asked.append(entry)
    ctx.asked_questions = asked          # 整体重新赋值，确保 JSON 变更被持久化
    await db.commit()
    await db.refresh(ctx)
    return _context_to_dict(session, ctx)


# ============================================================
# 5. 增加已覆盖知识点
# ============================================================
async def add_covered_topic(
    db: AsyncSession, session_id: int, topic: str
) -> Dict[str, Any]:
    """追加已覆盖知识点（**去重**，保持首次出现顺序）。"""
    return await _add_topic(db, session_id, topic, "covered_topics")


# ============================================================
# 6. 增加薄弱知识点
# ============================================================
async def add_weak_topic(
    db: AsyncSession, session_id: int, topic: str
) -> Dict[str, Any]:
    """追加薄弱知识点（**去重**，保持首次出现顺序）。"""
    return await _add_topic(db, session_id, topic, "weak_topics")


async def _add_topic(
    db: AsyncSession, session_id: int, topic: str, field: str
) -> Dict[str, Any]:
    if not isinstance(topic, str) or not topic.strip():
        raise HTTPException(status_code=400, detail="topic 必须是非空字符串")
    name = topic.strip()

    session = await _load_session(db, session_id)
    ctx = await _load_context(db, session_id)

    current = _as_list(getattr(ctx, field))
    if name not in current:
        current.append(name)
        setattr(ctx, field, current)     # 整体重新赋值
        await db.commit()
        await db.refresh(ctx)
    return _context_to_dict(session, ctx)


# ============================================================
# 7. 增加追问次数
# ============================================================
async def increment_follow_up(
    db: AsyncSession, session_id: int, count: int = 1
) -> Dict[str, Any]:
    """累加追问次数（``count`` 必须为正整数）。"""
    step = int(count)
    if step < 1:
        raise HTTPException(status_code=400, detail="count 必须 >= 1")

    session = await _load_session(db, session_id)
    ctx = await _load_context(db, session_id)

    ctx.follow_up_count = int(ctx.follow_up_count or 0) + step
    await db.commit()
    await db.refresh(ctx)
    return _context_to_dict(session, ctx)


# ============================================================
# 8. 判断是否达到最大问题数量
# ============================================================
async def is_max_questions_reached(db: AsyncSession, session_id: int) -> bool:
    """已提问数量是否已达到 ``max_questions``。

    已提问数量 = ``max(current_question_no, len(asked_questions))``，
    详见 :func:`asked_count`。
    """
    session = await _load_session(db, session_id)
    ctx = await _load_context(db, session_id)
    return asked_count(session, ctx) >= int(ctx.max_questions or 0)


async def get_context_state(db: AsyncSession, session_id: int) -> Dict[str, Any]:
    """返回 ``{context, max_reached}``，便于调用方一次拿到上下文与判断结果。"""
    ctx = await get_context(db, session_id)
    return {"context": ctx, "max_reached": ctx["max_reached"]}
