# -*- coding: utf-8 -*-
"""ORM 模型包。

集中导出全部模型，作用有二：
1. 让 ``from models import User, Job, ...`` 成为统一入口；
2. **确保 ``Base.metadata`` 注册到全部表**——``main.py`` 的 lifespan 依赖
   ``Base.metadata.create_all`` 自动建表，只要本包被 import 过一次，
   所有表（含 interview 五张表）都会被创建。

注意：外键跨模块引用（如 ``ForeignKey("user.id")``）要求被引用模型在
``create_all`` 之前已注册，因此本文件按依赖顺序 import。
"""

from models.interview import (
    DIFFICULTIES,
    INTERVIEW_STAGES,
    INTERVIEW_TYPES,
    SESSION_STATUS_CREATED,
    SESSION_STATUS_FINISHED,
    SESSION_STATUS_ONGOING,
    InterviewAnswer,
    InterviewContext,
    InterviewQuestion,
    InterviewReport,
    InterviewSession,
)
from models.job import Job
from models.resume import ChatHistory, Resume
from models.user import User, UserLoginLog, UserSession

__all__ = [
    # 既有模型
    "User",
    "UserSession",
    "UserLoginLog",
    "Resume",
    "ChatHistory",
    "Job",
    # AI 面试模型
    "InterviewSession",
    "InterviewQuestion",
    "InterviewAnswer",
    "InterviewReport",
    "InterviewContext",
    # 枚举常量
    "SESSION_STATUS_CREATED",
    "SESSION_STATUS_ONGOING",
    "SESSION_STATUS_FINISHED",
    "INTERVIEW_TYPES",
    "DIFFICULTIES",
    "INTERVIEW_STAGES",
]
