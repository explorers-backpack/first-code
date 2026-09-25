# -*- coding: utf-8 -*-
"""AI 模拟面试 ORM 模型（5 张表）。

设计说明
--------
1. **复用既有实体**：``user_id`` / ``job_id`` / ``resume_id`` 均通过
   ``ForeignKey`` 指向既有 ``user`` / ``jobs`` / ``resume`` 表，不重复建表。
   ``resume_id`` 可空——``resume`` 表目前无写入逻辑，面试允许不挂简历。
2. **表间关系**：question → session、answer → question、report → session
   均建外键；``interview_answer.question_id`` 与
   ``interview_report.session_id`` 加 UNIQUE，从数据库层保证
   「一题一答」「一场一报告」。
3. **不使用 ORM relationship**：与既有代码风格一致，异步场景下显式查询
   比惰性加载更可控（避免 MissingGreenlet）。
4. **新增字段**：``total_questions`` / ``current_question_no`` 是驱动面试
   生命周期所必需的进度状态，不在原始字段清单内，但无此二者无法判断
   「下一题是哪题」「是否已问完」。
5. **新增字段**：``interview_mode``（``text`` / ``avatar``）**只选择交互方式**，
   不参与出题、评分、报告与状态机中的任何判断——文字面试与数字人视频面试
   共用同一套面试核心，差异仅在前端交互与（未来的）音视频通道。
   默认 ``text``：与新增该列之前的既有会话行为完全一致，向后兼容。
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
)

from database import Base

# ============================================================
# 状态与枚举（供 service / schema 共用，避免散落魔法字符串）
# ============================================================
SESSION_STATUS_CREATED = "created"
SESSION_STATUS_ONGOING = "ongoing"
SESSION_STATUS_FINISHED = "finished"

INTERVIEW_TYPES = ("technical", "behavioral", "comprehensive")
DIFFICULTIES = ("junior", "mid", "senior")

# 交互模式（**仅**选择交互方式，不参与任何面试业务逻辑）
#   text   = 文字面试：前端输入框作答（当前既有流程）
#   avatar = 数字人视频面试：后续接入讯飞数字人 / ASR / TTS，
#            当前只记录选择，不产生任何音视频行为
INTERVIEW_MODE_TEXT = "text"
INTERVIEW_MODE_AVATAR = "avatar"
INTERVIEW_MODES = (INTERVIEW_MODE_TEXT, INTERVIEW_MODE_AVATAR)
DEFAULT_INTERVIEW_MODE = INTERVIEW_MODE_TEXT

# 面试阶段（InterviewContext.current_stage 的合法取值，顺序即推进顺序）
INTERVIEW_STAGES = (
    "introduction",
    "resume",
    "technical",
    "project",
    "scenario",
    "hr",
    "closing",
)


class InterviewSession(Base):
    """面试会话主表：一场面试的生命周期载体。"""

    __tablename__ = "interview_session"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # ---- 复用既有实体（外键关联，不重复建表）----
    user_id = Column(Integer, ForeignKey("user.id"), nullable=False, index=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=True, index=True)
    resume_id = Column(Integer, ForeignKey("resume.id"), nullable=True, index=True)

    # ---- 面试配置 ----
    interview_type = Column(String(30), nullable=False, default="comprehensive")
    difficulty = Column(String(20), nullable=False, default="mid")
    duration = Column(Integer, nullable=False, default=30)  # 计划时长（分钟）

    # ---- 交互模式（只选「怎么面」，不影响「面什么」）----
    # server_default 使「既有库 ALTER 补列」与「新建表」的默认值口径一致；
    # 见 utils/schema_sync.py：create_all 不给既有表补列，需要启动时幂等补齐。
    interview_mode = Column(
        String(20),
        nullable=False,
        default=DEFAULT_INTERVIEW_MODE,
        server_default=DEFAULT_INTERVIEW_MODE,
    )

    # ---- 生命周期 ----
    status = Column(String(20), nullable=False, default=SESSION_STATUS_CREATED, index=True)
    total_questions = Column(Integer, nullable=False, default=5)       # 计划题量
    current_question_no = Column(Integer, nullable=False, default=0)   # 已生成的题号
    started_at = Column(DateTime)
    ended_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)


class InterviewQuestion(Base):
    """面试题表：一道题一行，``question_no`` 在会话内自增。"""

    __tablename__ = "interview_question"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(
        Integer,
        ForeignKey("interview_session.id"),
        nullable=False,
        index=True,
    )
    question_no = Column(Integer, nullable=False)
    question = Column(Text, nullable=False)
    question_type = Column(String(30), nullable=False, default="technical")
    topic = Column(String(100))
    difficulty = Column(String(20))
    expected_points = Column(JSON)  # 期望覆盖的关键词，供评价取证使用
    created_at = Column(DateTime, default=datetime.utcnow)


class InterviewAnswer(Base):
    """面试作答表：一题一答（``question_id`` 唯一约束）。"""

    __tablename__ = "interview_answer"

    id = Column(Integer, primary_key=True, autoincrement=True)
    question_id = Column(
        Integer,
        ForeignKey("interview_question.id"),
        nullable=False,
        index=True,
        unique=True,
    )
    answer_text = Column(Text)
    audio_url = Column(String(300))  # 预留：接入 ASR / 录音后写入，当前恒为空

    # ---- 单题评分（0-100）----
    score = Column(Integer)
    technical_score = Column(Integer)
    logic_score = Column(Integer)
    expression_score = Column(Integer)
    adaptability_score = Column(Integer)

    feedback = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class InterviewReport(Base):
    """面试报告表：一场面试一份（``session_id`` 唯一约束）。"""

    __tablename__ = "interview_report"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(
        Integer,
        ForeignKey("interview_session.id"),
        nullable=False,
        index=True,
        unique=True,
    )

    # ---- 八维总分（0-100）----
    total_score = Column(Integer)
    technical_score = Column(Integer)
    project_score = Column(Integer)
    logic_score = Column(Integer)
    expression_score = Column(Integer)
    communication_score = Column(Integer)
    adaptability_score = Column(Integer)
    job_match_score = Column(Integer)

    # ---- 文字结论 ----
    strengths = Column(JSON)      # 突出表现（字符串数组）
    weaknesses = Column(JSON)     # 待改进项（字符串数组）
    suggestions = Column(Text)    # 改进建议（成段文字）

    created_at = Column(DateTime, default=datetime.utcnow)


class InterviewContext(Base):
    """面试上下文表：一场面试一份（``session_id`` 唯一约束），记录运行期状态。

    为什么是**独立表**而不是给 ``interview_session`` 加 JSON 列
    ----------------------------------------------------------
    ``main.py`` 的 lifespan 用 ``Base.metadata.create_all`` 建表——
    **新表会被自动创建**，但在**既有表上加列不会被补上**（必须人工 ALTER）。
    独立表因此做到「零 DDL、零迁移」，且完全不触碰既有表结构。

    为什么**不重复存** ``current_question_no`` / ``total_questions``
    --------------------------------------------------------------
    这两个字段的权威来源是 ``interview_session`` 的既有列（状态机正在使用它们）。
    Context 读取时**实时投影**，避免双写导致两处不一致。

    语义区分
    --------
    - ``total_questions``（来自 session）：本场**计划**题量
    - ``max_questions``（本表）：本场**允许的最大**题量（含追问），达到即应进入收尾
    """

    __tablename__ = "interview_context"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # 一场面试一份上下文：UNIQUE 从数据库层保证 1:1
    session_id = Column(
        Integer,
        ForeignKey("interview_session.id"),
        nullable=False,
        index=True,
        unique=True,
    )

    # ---- 阶段 ----
    current_stage = Column(String(20), nullable=False, default="introduction")

    # ---- 列表型状态（JSON 数组）----
    asked_questions = Column(JSON, nullable=False, default=list)  # 已提问（有序，允许重复）
    covered_topics = Column(JSON, nullable=False, default=list)   # 已覆盖知识点（去重）
    weak_topics = Column(JSON, nullable=False, default=list)      # 薄弱知识点（去重）

    # ---- 计数 ----
    follow_up_count = Column(Integer, nullable=False, default=0)   # 追问次数
    max_questions = Column(Integer, nullable=False, default=10)    # 题量上限

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
