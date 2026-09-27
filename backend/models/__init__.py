# -*- coding: utf-8 -*-
"""ORM 模型包。

集中导出全部模型，作用有二：
1. 让 ``from models import User, Job, ...`` 成为统一入口；
2. **确保 ``Base.metadata`` 注册到全部表**——``main.py`` 的 lifespan 依赖
   ``Base.metadata.create_all`` 自动建表，只要本包被 import 过一次，
   所有表（含 interview 五张表与 knowledge 两张表）都会被创建。

注意：外键跨模块引用（如 ``ForeignKey("user.id")``）要求被引用模型在
``create_all`` 之前已注册，因此本文件按依赖顺序 import。
"""

from models.interview import (
    DEFAULT_INTERVIEW_MODE,
    DIFFICULTIES,
    INTERVIEW_MODE_AVATAR,
    INTERVIEW_MODE_TEXT,
    INTERVIEW_MODES,
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
from models.knowledge import (
    KNOWLEDGE_CATEGORIES,
    KNOWLEDGE_CATEGORY_COMPANY,
    KNOWLEDGE_CATEGORY_JOB,
    KNOWLEDGE_CATEGORY_LABELS,
    KNOWLEDGE_CATEGORY_PROJECT,
    KNOWLEDGE_CATEGORY_TECHNICAL,
    KnowledgeChunk,
    KnowledgeDocument,
)
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
    # 知识库模型（本阶段只建数据结构，不含解析 / Embedding / 向量库 / Retriever）
    "KnowledgeDocument",
    "KnowledgeChunk",
    # 枚举常量
    "SESSION_STATUS_CREATED",
    "SESSION_STATUS_ONGOING",
    "SESSION_STATUS_FINISHED",
    "INTERVIEW_TYPES",
    "DIFFICULTIES",
    "INTERVIEW_STAGES",
    "INTERVIEW_MODES",
    "INTERVIEW_MODE_TEXT",
    "INTERVIEW_MODE_AVATAR",
    "DEFAULT_INTERVIEW_MODE",
    "KNOWLEDGE_CATEGORIES",
    "KNOWLEDGE_CATEGORY_JOB",
    "KNOWLEDGE_CATEGORY_TECHNICAL",
    "KNOWLEDGE_CATEGORY_COMPANY",
    "KNOWLEDGE_CATEGORY_PROJECT",
    "KNOWLEDGE_CATEGORY_LABELS",
]
