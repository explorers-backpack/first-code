# -*- coding: utf-8 -*-
"""数据库基础设施：Base / engine / 会话工厂 / get_db 依赖。

为什么单独成模块
----------------
原先这些对象全部定义在 ``main.py``。新增 AI 面试模块后，``models/``、
``services/``、``api/`` 三个分层都需要使用同一套数据库对象，如果各自
``from main import ...`` 就会与 ``main.py`` 形成循环依赖
（main → api.interview → main）。

因此把「数据库对象」下沉到本模块，由各分层共同 import。

兼容性
------
``main.py`` 仍然 import 本模块的这些名字，所以 ``main.Base`` /
``main.engine`` / ``main.async_session`` / ``main.get_db`` 等既有引用
全部保持不变，对外行为与抽取前完全一致。
"""

from __future__ import annotations

import os

from dotenv import load_dotenv

# 本模块可能被独立 import（如测试脚本），因此自己加载一次 .env。
# load_dotenv 默认 override=False，已存在的环境变量不会被覆盖。
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase  # noqa: E402


class Base(DeclarativeBase):
    """所有 ORM 模型的公共基类。"""


# ============================================================
# 数据库配置（MySQL）
# ============================================================
# 连接串一律从环境变量读取（来源：backend/.env，已被 .gitignore 忽略）。
# 严禁把账号密码写回源码——源码会随 git 提交，.env 不会。
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError(
        "缺少数据库连接配置：请在 backend/.env 中设置 DATABASE_URL，"
        "格式为 mysql+aiomysql://<user>:<password>@<host>:<port>/<db>?charset=utf8mb4"
    )

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db():
    """FastAPI 依赖：产出一个异步数据库会话，请求结束后自动关闭。"""
    async with async_session() as session:
        yield session
