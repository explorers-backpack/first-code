# -*- coding: utf-8 -*-
"""AI 模拟面试 Pydantic 契约。

划分原则：
- ``*Request``  —— 入参校验，用 ``Literal`` / ``Field`` 约束取值范围
- ``*Out``      —— 出参契约，作为路由的 ``response_model``，同时生成 OpenAPI 文档

约定：service 层统一返回 **普通 dict**，由本文件的 ``*Out`` 负责校验与序列化，
这样 service 不依赖 FastAPI，可脱离 HTTP 单独测试。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, model_validator

# ============================================================
# 枚举（与 models.interview 中的常量保持一致）
# ============================================================
InterviewType = Literal["technical", "behavioral", "comprehensive"]
DifficultyLevel = Literal["junior", "mid", "senior"]
SessionStatus = Literal["created", "ongoing", "finished"]
StageName = Literal[
    "introduction", "resume", "technical", "project", "scenario", "hr", "closing"
]
PlanSource = Literal["rule", "llm"]


# ============================================================
# 请求体
# ============================================================
class SessionCreateRequest(BaseModel):
    """创建面试会话。``user_id`` 来自登录态，不接受前端传入。"""

    job_id: Optional[int] = Field(default=None, ge=1, description="目标岗位 id，关联 jobs 表")
    resume_id: Optional[int] = Field(default=None, ge=1, description="简历 id，关联 resume 表")
    interview_type: InterviewType = Field(default="comprehensive", description="面试类型")
    difficulty: DifficultyLevel = Field(default="mid", description="难度")
    duration: int = Field(default=30, ge=5, le=180, description="计划时长（分钟）")
    total_questions: int = Field(default=5, ge=1, le=20, description="计划题量")


class AnswerSubmitRequest(BaseModel):
    """提交一道题的回答。"""

    answer_text: str = Field(..., min_length=1, max_length=10000, description="回答文本")
    audio_url: Optional[str] = Field(
        default=None, max_length=300, description="录音地址（预留，暂不使用）"
    )


# ============================================================
# 响应体
# ============================================================
class InterviewQuestionOut(BaseModel):
    id: int
    question_no: int
    question: str
    question_type: str
    topic: Optional[str] = None
    difficulty: Optional[str] = None
    expected_points: Optional[List[str]] = None
    created_at: Optional[datetime] = None


class InterviewAnswerOut(BaseModel):
    id: int
    question_id: int
    answer_text: Optional[str] = None
    audio_url: Optional[str] = None
    score: Optional[int] = None
    technical_score: Optional[int] = None
    logic_score: Optional[int] = None
    expression_score: Optional[int] = None
    adaptability_score: Optional[int] = None
    feedback: Optional[str] = None
    created_at: Optional[datetime] = None


class InterviewSessionOut(BaseModel):
    id: int
    user_id: int
    job_id: Optional[int] = None
    resume_id: Optional[int] = None
    interview_type: str
    difficulty: str
    duration: int
    status: str
    total_questions: int
    current_question_no: int
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    created_at: Optional[datetime] = None


class InterviewReportOut(BaseModel):
    id: int
    session_id: int
    total_score: Optional[int] = None
    technical_score: Optional[int] = None
    project_score: Optional[int] = None
    logic_score: Optional[int] = None
    expression_score: Optional[int] = None
    communication_score: Optional[int] = None
    adaptability_score: Optional[int] = None
    job_match_score: Optional[int] = None
    strengths: Optional[List[str]] = None
    weaknesses: Optional[List[str]] = None
    suggestions: Optional[str] = None
    created_at: Optional[datetime] = None


# ============================================================
# InterviewPlan（Interview Planner 的输出契约）
# ============================================================
class InterviewPlanStage(BaseModel):
    """面试计划中的一个阶段。"""

    stage: StageName = Field(..., description="阶段名，与 models.INTERVIEW_STAGES 一致")
    weight: int = Field(..., ge=0, le=100, description="该阶段的时间权重（各阶段合计 100）")
    target_questions: int = Field(
        ..., ge=0, le=20, description="该阶段的目标题量（各阶段合计 = total_questions）"
    )


class InterviewPlanOut(BaseModel):
    """面试计划（Interview Planner 产出）。

    由 ``services.interview_planner`` 制定，**只描述考察计划，不含任何具体题目**。
    两个结构性不变量在模型层强制校验：

    1. ``stages[*].weight`` 合计 == 100
    2. ``stages[*].target_questions`` 合计 == ``total_questions``
    """

    interview_type: InterviewType
    difficulty: DifficultyLevel
    duration: int = Field(..., ge=5, le=180, description="计划时长（分钟）")
    total_questions: int = Field(..., ge=1, le=20, description="全场目标题量")

    stages: List[InterviewPlanStage] = Field(
        default_factory=list, description="阶段计划，顺序与状态机推进顺序一致"
    )
    target_topics: List[str] = Field(default_factory=list, description="本场需考察的知识点")
    priority_topics: List[str] = Field(
        default_factory=list, description="优先考察点，为 target_topics 的子集"
    )
    resume_focus_points: List[str] = Field(
        default_factory=list, description="简历中值得深挖的项目/系统"
    )

    source: PlanSource = Field(default="rule", description="计划来源：rule（规则）或 llm（模型增强）")
    job_id: Optional[int] = Field(default=None, description="目标岗位 id（未指定为 null）")
    resume_id: Optional[int] = Field(default=None, description="简历 id（未指定为 null）")

    @model_validator(mode="after")
    def _check_invariants(self) -> "InterviewPlanOut":
        if not self.stages:
            raise ValueError("stages 不能为空")
        weight_sum = sum(s.weight for s in self.stages)
        if weight_sum != 100:
            raise ValueError(f"stages 权重合计必须为 100，当前 {weight_sum}")
        question_sum = sum(s.target_questions for s in self.stages)
        if question_sum != self.total_questions:
            raise ValueError(
                f"stages 题量合计 {question_sum} 与 total_questions "
                f"{self.total_questions} 不一致"
            )
        stages = [s.stage for s in self.stages]
        if len(stages) != len(set(stages)):
            raise ValueError("stages 中阶段不可重复")
        return self


# ============================================================
# 组合响应（各接口的出参）
# ============================================================
class SessionCreateResponse(BaseModel):
    session: InterviewSessionOut
    first_question: Optional[InterviewQuestionOut] = None
    job_name: Optional[str] = None
    message: str = "面试会话创建成功"


class SessionStartResponse(BaseModel):
    session: InterviewSessionOut
    question: Optional[InterviewQuestionOut] = None
    job_name: Optional[str] = None
    message: str = "面试已开始"


class SessionDetailResponse(BaseModel):
    session: InterviewSessionOut
    job_name: Optional[str] = None
    questions: List[InterviewQuestionOut] = Field(default_factory=list)
    answers: List[InterviewAnswerOut] = Field(default_factory=list)
    answered_count: int = 0


class CurrentQuestionResponse(BaseModel):
    session_id: int
    status: str
    question_no: Optional[int] = None
    total_questions: int
    question: Optional[InterviewQuestionOut] = None
    answered: bool = False
    all_answered: bool = False
    message: str = "ok"


class AnswerSubmitResponse(BaseModel):
    answer: InterviewAnswerOut
    session: InterviewSessionOut
    next_question: Optional[InterviewQuestionOut] = None
    all_answered: bool = False
    message: str = "回答已提交"


class EndSessionResponse(BaseModel):
    session: InterviewSessionOut
    report: InterviewReportOut
    message: str = "面试已结束"


class ErrorResponse(BaseModel):
    """错误响应（FastAPI 默认 HTTPException 结构），仅用于文档展示。"""

    detail: Any = Field(default=None, description="错误说明")
