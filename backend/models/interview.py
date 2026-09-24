# -*- coding: utf-8 -*-
"""AI 模拟面试 ORM 模型（4 张表）。

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
