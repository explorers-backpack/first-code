# -*- coding: utf-8 -*-
"""FastAPI 依赖：鉴权相关。

从 ``main.py`` 抽取，供 ``api/`` 分层复用，避免反向 import ``main`` 造成
循环依赖。``main.py`` 仍然 import 这两个名字，因此 ``main.get_current_user``
/ ``main.get_current_admin`` 等既有引用保持不变。

鉴权口径（与抽取前完全一致）：
- 请求头 ``Authorization: Bearer <token>``
- token 命中 ``user_session`` 表即视为登录有效
- ``role == "admin"`` 才允许访问管理员接口
"""

from __future__ import annotations

from typing import Optional

from fastapi import Depends, Header, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import UserSession


async def get_current_user(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """解析当前登录用户；未登录或会话失效时抛 401。"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="未登录")
    token = authorization[7:]

    result = await db.execute(select(UserSession).where(UserSession.token == token))
    session = result.scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=401, detail="登录已过期")

    return {
        "user_id": session.user_id,
        "email": session.email,
        "role": session.role,
    }


async def get_current_admin(
    current_user: dict = Depends(get_current_user),
) -> dict:
    """在 get_current_user 基础上要求管理员角色；否则抛 403。"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return current_user
