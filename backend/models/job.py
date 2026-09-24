# -*- coding: utf-8 -*-
"""岗位 ORM 模型。

原定义于 ``main.py``，字段与表名逐字未改。
AI 面试模块通过 ``interview_session.job_id`` 外键引用 ``jobs.id``。
"""

from __future__ import annotations

from sqlalchemy import Column, Integer, String, Text

from database import Base


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_name = Column(String(200), nullable=False)
    salary = Column(String(50))
    edu_require = Column(String(50))
    major_require = Column(String(200))
    skills = Column(Text)
    duty = Column(Text)
    city = Column(String(50))
    industry = Column(String(50))
