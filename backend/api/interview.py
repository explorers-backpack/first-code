# -*- coding: utf-8 -*-
"""AI 模拟面试路由层。

职责边界
--------
本层只做三件事：**参数校验 → 调用 service → 声明响应模型**。
一切业务判断（状态流转、出题、评分、汇总）都在
``services/interview_service.py``（会话与 API 门面）与
``services/interview_core.py``（面试流程控制与业务规则），便于脱离 HTTP 单独测试与复用。
本层**只依赖 ``interview_service``**，不直接依赖 Core / Agent / Validator。

异常处理
--------
- ``HTTPException``（业务错误，如 400/404）直接透传，保留原始状态码与提示
- 其余未预期异常统一收敛为 500，并附上可读说明，避免把堆栈直接暴露给前端
- 数据库完整性错误（如外键指向不存在的记录）在 service 层已提前校验并给出
  可读提示，不会走到这里

鉴权
----
全部接口要求 ``Authorization: Bearer <token>``，复用既有的
``deps.get_current_user``；``user_id`` 一律取自登录态，**不接受前端传入**，
避免越权访问他人面试记录。
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from deps import get_current_user
from schemas.interview import (
    AnswerSubmitRequest,
    AnswerSubmitResponse,
    CurrentQuestionResponse,
    EndSessionResponse,
    InterviewReportOut,
    SessionCreateRequest,
    SessionCreateResponse,
    SessionDetailResponse,
    SessionStartResponse,
)
from services import interview_service

router = APIRouter(prefix="/api/interview", tags=["AI 面试"])

SessionId = Path(..., ge=1, description="面试会话 id")


async def _run(action: str, coro):
    """统一异常收敛：业务异常透传，未预期异常转 500。"""
    try:
        return await coro
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 - 兜底，避免未捕获异常直接 500 无提示
        raise HTTPException(status_code=500, detail=f"{action}失败：{exc}") from exc


@router.post(
    "/create",
    response_model=SessionCreateResponse,
    summary="创建面试会话",
    description=(
        "创建一场模拟面试。此时仅落库会话配置，**不生成题目**，"
        "状态为 `created`；需再调用 `/start` 才会生成题目并进入 `ongoing`。\n\n"
        "`mode` 为**交互模式**：`text` 文字面试 / `avatar` 数字人视频面试，"
        "缺省 `text`。该字段只选择交互方式，不改变出题与评分逻辑。"
    ),
)
async def create_interview(
    payload: SessionCreateRequest,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> SessionCreateResponse:
    data = await _run(
        "创建面试会话",
        interview_service.create_session(db, current_user["user_id"], payload),
    )
    return SessionCreateResponse(**data)


@router.post(
    "/{session_id}/start",
    response_model=SessionStartResponse,
    summary="开始面试",
    description="生成整场题目并置为 `ongoing`，返回第一题。仅 `created` 状态可调用。",
)
async def start_interview(
    session_id: int = SessionId,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> SessionStartResponse:
    data = await _run(
        "开始面试",
        interview_service.start_session(db, current_user["user_id"], session_id),
    )
    return SessionStartResponse(**data)


@router.get(
    "/{session_id}",
    response_model=SessionDetailResponse,
    summary="查询面试会话详情",
    description="返回会话状态、全部题目与已提交的作答，可用于前端恢复中断的面试。",
)
async def get_interview(
    session_id: int = SessionId,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> SessionDetailResponse:
    data = await _run(
        "查询面试会话",
        interview_service.get_session_detail(db, current_user["user_id"], session_id),
    )
    return SessionDetailResponse(**data)


@router.get(
    "/{session_id}/question",
    response_model=CurrentQuestionResponse,
    summary="获取当前题目",
    description=(
        "返回当前待作答的题目。未开始时提示先 start；"
        "全部作答完毕时 `all_answered=true`，提示调用 end。"
    ),
)
async def get_question(
    session_id: int = SessionId,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CurrentQuestionResponse:
    data = await _run(
        "获取当前题目",
        interview_service.get_current_question(db, current_user["user_id"], session_id),
    )
    return CurrentQuestionResponse(**data)


@router.post(
    "/{session_id}/answer",
    response_model=AnswerSubmitResponse,
    summary="提交回答",
    description=(
        "提交当前题目的回答，服务端立即做规则评分并落库，随后返回下一题。"
        "一题只能作答一次（数据库层 `question_id` 唯一约束）。"
    ),
)
async def submit_answer(
    payload: AnswerSubmitRequest,
    session_id: int = SessionId,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> AnswerSubmitResponse:
    data = await _run(
        "提交回答",
        interview_service.submit_answer(
            db, current_user["user_id"], session_id, payload
        ),
    )
    return AnswerSubmitResponse(**data)


@router.post(
    "/{session_id}/end",
    response_model=EndSessionResponse,
    summary="结束面试并生成报告",
    description=(
        "结束面试（`ongoing` → `finished`），按已作答内容汇总生成八维报告。"
        "允许提前结束；若报告已存在则幂等返回。"
    ),
)
async def end_interview(
    session_id: int = SessionId,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> EndSessionResponse:
    data = await _run(
        "结束面试",
        interview_service.end_session(db, current_user["user_id"], session_id),
    )
    return EndSessionResponse(**data)


@router.get(
    "/{session_id}/report",
    response_model=InterviewReportOut,
    summary="获取面试报告",
    description="返回面试报告。未结束（无报告）时返回 404 并提示先调用 end。",
)
async def get_report(
    session_id: int = SessionId,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> InterviewReportOut:
    data = await _run(
        "获取面试报告",
        interview_service.get_report(db, current_user["user_id"], session_id),
    )
    return InterviewReportOut(**data)
