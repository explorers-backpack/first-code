# -*- coding: utf-8 -*-
"""FastAPI 依赖：鉴权相关。

从 ``main.py`` 抽取，供 ``api/`` 分层复用，避免反向 import ``main`` 造成
循环依赖。``main.py`` 仍然 import 这两个名字，因此 ``main.get_current_user``
/ ``main.get_current_admin`` 等既有引用保持不变。

鉴权口径：
- 请求头 ``Authorization: Bearer <token>``
- token 命中 ``user_session`` 表、未过期、未撤销才视为登录有效
- ``role == "admin"`` 才允许访问管理员接口
"""

from __future__ import annotations

from typing import Optional

from fastapi import Depends, Header, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import User
from security import load_valid_session


async def get_current_session(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
) -> "UserSession":
    """解析当前登录会话；无效时抛 401。"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="未登录")
    token = authorization[7:]

    session = await load_valid_session(db, token)
    if session is None:
        raise HTTPException(status_code=401, detail="登录已过期")
    return session


async def get_current_user(
    session=Depends(get_current_session),
) -> dict:
    """将会话投影为 dict，供路由使用。"""
    return {
        "user_id": session.user_id,
        "email": session.email,
        "role": session.role,
    }


async def get_optional_session(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
) -> "UserSession | None":
    """尽力解析当前登录会话；**无效时返回 None，永不抛 401**。

    用途：某些端点的主鉴权走 ``get_current_user``（可能是被测试覆盖的版本），
    但**额外**需要 ``user_session.id``（例如把它写进 WS ticket，供连接存续期间
    复核撤销）。若这里也抛 401，就会与 ``dependency_overrides[get_current_user]``
    的测试链冲突——那些测试没有真实 Authorization 头，却期望端点正常工作。

    安全边界：返回 ``None`` **不代表已登录**。它只表示"这次拿不到登录会话"，
    端点必须另有真正的鉴权依赖；本函数不得单独作为鉴权使用。
    """
    if not authorization or not authorization.startswith("Bearer "):
        return None
    try:
        return await load_valid_session(db, authorization[7:])
    except Exception:  # noqa: BLE001 - 解析失败等价于"没拿到"，不影响主鉴权
        return None


async def get_current_admin(
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """在 get_current_user 基础上要求管理员角色；否则抛 403。

    管理员身份**实时查 User 表**，而不是信任登录时快照的 session.role，
    防止「角色被降级后旧会话仍保留 admin 权限」的滞后越权。
    仍依赖 ``get_current_user``：保持既有依赖覆盖链（测试可只覆盖这一层）。
    """
    result = await db.execute(
        select(User.role).where(User.id == current_user["user_id"])
    )
    current_role = result.scalar_one_or_none()
    if current_role != "admin":
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return {
        "user_id": current_user["user_id"],
        "email": current_user.get("email"),
        "role": current_role,
    }
