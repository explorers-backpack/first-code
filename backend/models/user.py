# -*- coding: utf-8 -*-
"""用户与会话相关 ORM 模型。

原定义于 ``main.py``，为支持 ``models/`` / ``services/`` / ``api/`` 分层
而迁移至此，字段与表名逐字未改。
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String

from database import Base


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(80), unique=True, nullable=False)
    email = Column(String(120), unique=True, nullable=False)
    password_hash = Column(String(200), nullable=False)
    role = Column(String(20), default="user")
    created_at = Column(DateTime, default=datetime.utcnow)


class UserSession(Base):
    __tablename__ = "user_session"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    token = Column(String(64), unique=True, nullable=False, index=True)
    email = Column(String(120), nullable=False)
    role = Column(String(20), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class UserLoginLog(Base):
    __tablename__ = "user_login_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(120), nullable=False)
    role = Column(String(20), nullable=False)
    last_login = Column(String(32), nullable=False)
    search_count = Column(Integer, default=0)
