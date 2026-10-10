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

from pydantic import AliasChoices, BaseModel, Field, model_validator

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

# 交互模式：text=文字面试，avatar=数字人视频面试
# 该字段**只负责选择交互方式**，不改变出题 / 评分 / 状态机（见 models.interview）。
InterviewMode = Literal["text", "avatar"]


# ============================================================
# 请求体
# ============================================================
class SessionCreateRequest(BaseModel):
    """创建面试会话。``user_id`` 来自登录态，不接受前端传入。

    ``mode`` 是**交互模式**（``text`` 文字面试 / ``avatar`` 数字人视频面试）。
    为了同时兼容 ``{"mode": "avatar"}`` 与 ``{"interview_mode": "avatar"}``
    两种写法（后者与数据库列同名），这里用 ``AliasChoices`` 同时接受两个键名；
    两者都缺省时为 ``text``。
    """

    job_id: Optional[int] = Field(default=None, ge=1, description="目标岗位 id，关联 jobs 表")
    resume_id: Optional[int] = Field(default=None, ge=1, description="简历 id，关联 resume 表")
    interview_type: InterviewType = Field(default="comprehensive", description="面试类型")
    difficulty: DifficultyLevel = Field(default="mid", description="难度")
    duration: int = Field(default=30, ge=5, le=180, description="计划时长（分钟）")
    total_questions: int = Field(default=5, ge=1, le=20, description="计划题量")
    mode: InterviewMode = Field(
        default="text",
        validation_alias=AliasChoices("mode", "interview_mode"),
        description="交互模式：text=文字面试，avatar=数字人视频面试",
    )


class AnswerSubmitRequest(BaseModel):
    """提交一道题的回答。"""

    answer_text: str = Field(..., min_length=1, max_length=10000, description="回答文本")
    audio_url: Optional[str] = Field(
        default=None, max_length=300, description="录音地址（预留，暂不使用）"
    )


class NextQuestionRequest(BaseModel):
    """按需生成下一题（大模型出题通道）—— **RAG 的生产开关就在这里**。

    ``use_rag`` 默认 ``True``（**默认开启**）：不显式关闭就由
    ``interview_core.resolve_retriever`` 组装真实检索链路
    （Embedding + 向量后端 + 检索器），把命中的语料注入出题 Prompt；
    组装/检索失败**静默降级为「无知识」**
    （只在结果的 ``warnings`` 里记 ``knowledge_retrieval_failed``，不算失败）。
    显式传 ``False`` 可回到「不检索」——出题行为与引入本接口前**逐字节一致**。

    ★ 默认值只落在**本 HTTP 契约层**：``interview_service`` / ``interview_core``
    的函数默认参数仍保守为 ``False``，避免**内部直接调用**时隐式触发检索。

    ``top_k`` / ``min_score`` 是**检索参数**，只在 ``use_rag=True`` 时生效
    （一并透传给 ``services.knowledge_rag.build_vector_retriever``）；
    两者都缺省时不传该组参数 ⇒ 组装器用自身默认值（``top_k=5``、不过滤）。
    """

    use_rag: bool = Field(
        default=True, description="是否启用 RAG 知识检索（默认开启，传 false 即关闭）"
    )
    top_k: Optional[int] = Field(
        default=None, ge=1, le=100, description="检索候选条数（仅 use_rag=true 时生效）"
    )
    min_score: Optional[float] = Field(
        default=None, ge=-1, le=1, description="相似度下限，取值域 [-1, 1]（仅 use_rag=true 时生效）"
    )


class FollowUpRequest(BaseModel):
    """根据候选人的回答生成**一道追问**（数字人面试的「继续提问」入口）。

    链路：**分析回答 → Retriever 检索 → Agent 生成追问 → Avatar 继续提问**。
    上下文同时带上**简历 + 岗位 + 知识库 + 历史回答**（见
    ``services.interview_context_pack``）。

    ``use_rag`` / ``top_k`` / ``min_score`` 的语义与 ``/next-question`` **完全一致**
    （同一套开关、同一个组装器）：``use_rag`` **默认开启**，显式传 ``False``
    才回到「不检索」；组装 / 检索失败**静默降级为「无知识」**（只在结果的
    ``warnings`` 里体现）。默认值同样只落在**本 HTTP 契约层**。
    """

    answer_text: str = Field(
        ..., min_length=1, max_length=10000, description="候选人刚才的回答文本（ASR 或打字）"
    )
    question_no: Optional[int] = Field(
        default=None,
        ge=1,
        description="追问所针对的题号；缺省取「最近一道已作答的题」",
    )
    use_rag: bool = Field(
        default=True, description="是否启用 RAG 知识检索（默认开启，传 false 即关闭）"
    )
    top_k: Optional[int] = Field(
        default=None, ge=1, le=100, description="检索候选条数（仅 use_rag=true 时生效）"
    )
    min_score: Optional[float] = Field(
        default=None, ge=-1, le=1, description="相似度下限，取值域 [-1, 1]（仅 use_rag=true 时生效）"
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
    interview_mode: str = Field(
        default="text", description="交互模式：text=文字面试，avatar=数字人视频面试"
    )
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


class NextQuestionResponse(BaseModel):
    """按需出题结果 —— 即 ``interview_core`` 的结果字典。

    字段集**恒定**（成功与失败同形状），与
    ``services.interview_core.QUESTION_RESULT_FIELDS`` 一一对应，
    调用方无需按 ``ok`` 分支取值。约定：

    - ``ok=False`` 时 ``question`` 恒为 ``""``（**绝不放行未校验的问题**）
    - ``errors`` 放**稳定错误码**（``session_not_found`` / ``session_finished`` /
      ``all_answered`` / ``agent_failed`` / ``validation_failed``）
    - 知识检索失败**不是失败**：``ok`` 不受影响，只在 ``warnings`` 里记
      ``knowledge_retrieval_failed``
    - ``knowledge_chunks`` 是**正向信号**：本次真正注入 Prompt 的切片条数。
      与 ``warnings`` 配合才能完整区分三种状态——「没开 RAG」（0 条 + 无警告）、
      「开了但没命中」（0 条 + 无警告，与前者同形，需结合请求里的 ``use_rag``）、
      「命中 N 条」（N > 0）。检索失败仍是 0 条 + ``knowledge_retrieval_failed``。
    """

    ok: bool
    question_no: Optional[int] = None
    question: str = ""
    question_type: str = ""
    topic: str = ""
    difficulty: str = ""
    expected_points: List[str] = Field(default_factory=list)
    reason: str = ""
    stage: str = ""
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    error: Optional[str] = None
    knowledge_chunks: int = 0


class FollowUpResponse(BaseModel):
    """追问结果 —— 即 ``services.interview_followup`` 的结果字典。

    字段集**恒定**（成功与失败同形状），与
    ``services.interview_followup.FOLLOWUP_RESULT_FIELDS`` 一一对应，
    调用方无需按 ``ok`` 分支取值。约定：

    - ``ok=False`` 时 ``question`` 恒为 ``""``（**绝不放行未校验的问题**）
    - ``errors`` 放**稳定错误码**（``session_not_found`` / ``session_finished`` /
      ``answer_required`` / ``question_required`` / ``agent_failed``）
    - ``based_on`` 是**追问所针对的题目原文**，``focus`` 是本次追问的聚焦点
      （同时也是检索线索），两者一起构成「为什么问这一句」的取证
    - ``analysis`` 是回答分析（规则评分 + 命中 / 未命中的采分点 + 薄弱点），
      字段集见 ``services.interview_followup.ANALYSIS_FIELDS``；失败时为空 dict
    - 知识检索失败**不是失败**：``ok`` 不受影响，只在 ``warnings`` 里记
      ``knowledge_retrieval_failed``；``knowledge_chunks`` 是**正向信号**
      （本次真正注入 Prompt 的切片条数）
    """

    ok: bool
    question_no: Optional[int] = None
    question: str = ""
    question_type: str = ""
    topic: str = ""
    difficulty: str = ""
    expected_points: List[str] = Field(default_factory=list)
    reason: str = ""
    based_on: str = ""
    follow_up_no: int = 1
    focus: str = ""
    analysis: Dict[str, Any] = Field(default_factory=dict)
    knowledge_chunks: int = 0
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    error: Optional[str] = None


class ErrorResponse(BaseModel):
    """错误响应（FastAPI 默认 HTTPException 结构），仅用于文档展示。"""

    detail: Any = Field(default=None, description="错误说明")
