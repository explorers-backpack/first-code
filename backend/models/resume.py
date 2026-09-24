# -*- coding: utf-8 -*-
"""简历与对话历史 ORM 模型。

原定义于 ``main.py``，字段与表名逐字未改。

注：``resume`` / ``chat_history`` 目前仅建表、无写入逻辑（历史遗留），
AI 面试模块通过 ``interview_session.resume_id`` 外键引用 ``resume.id``。
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, Column, DateTime, Integer, String, Text

from database import Base


class Resume(Base):
    __tablename__ = "resume"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    filename = Column(String(200))
    content = Column(Text)
    parsed_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)


class ChatHistory(Base):
    __tablename__ = "chat_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    role = Column(String(20), nullable=False)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
