# -*- coding: utf-8 -*-
"""认证安全工具：密码哈希、token 生成、会话有效性判定。

本模块为 ``deps.py`` 与 ``api/interview_ws.py`` 的 token 校验提供**唯一口径**，
避免两处各自维护导致漂移。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
from datetime import datetime, timedelta
from typing import Optional

import bcrypt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import UserSession


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        v = int(raw)
    except ValueError:
        return default
    return v


SESSION_TTL_SECONDS = _env_int("AUTH_SESSION_TTL_SECONDS", 604800)  # 7 天
if SESSION_TTL_SECONDS < 60:
    SESSION_TTL_SECONDS = 60

BCRYPT_ROUNDS = _env_int("AUTH_BCRYPT_ROUNDS", 12)
if BCRYPT_ROUNDS < 4:
    BCRYPT_ROUNDS = 4

# ---- dummy bcrypt 常量（消除未知账号的时间侧信道）----
_DUMMY_BCRYPT_HASH = bcrypt.hashpw(
    b"__dummy__", bcrypt.gensalt(rounds=BCRYPT_ROUNDS)
)
_PREFIX = "bcrypt$"


def _pre(password: str) -> bytes:
    """bcrypt 只吃前 72 字节；中文密码先用 SHA-256 预哈希再 base64，确保不截断语义。"""
    return base64.b64encode(hashlib.sha256(password.encode()).digest())


def hash_password(password: str) -> str:
    """生成 bcrypt 哈希，带前缀标记算法。"""
    hashed = bcrypt.hashpw(_pre(password), bcrypt.gensalt(rounds=BCRYPT_ROUNDS))
    return _PREFIX + hashed.decode()


def verify_password(password: str, stored: str) -> tuple[bool, bool]:
    """校验密码，返回 (是否通过, 是否需要升级)。

    - 新格式（bcrypt$前缀）：直接 bcrypt check
    - 旧格式（64 位十六进制无前缀）：sha256 单轮比对，通过则 needs_upgrade=True
    - 空 stored：返回 (False, False)
    """
    if not stored:
        # 未知账号也跑一次 dummy bcrypt，消除「账号是否存在」的时间侧信道
        bcrypt.checkpw(_pre(password), _DUMMY_BCRYPT_HASH)
        return False, False

    if stored.startswith(_PREFIX):
        ok = bcrypt.checkpw(_pre(password), stored[len(_PREFIX) :].encode())
        return ok, False

    # 旧格式：无盐 SHA-256
    ok = hmac.compare_digest(
        stored,
        hashlib.sha256(password.encode()).hexdigest(),
    )
    return ok, ok


def generate_token() -> str:
    """生成高熵、URL-safe 的随机 token。"""
    return secrets.token_urlsafe(32)


def session_is_valid(session: UserSession, now: Optional[datetime] = None) -> bool:
    """判断会话是否有效（未撤销、未过期）。

    expires_at 为 NULL 时按 created_at + TTL 折算；
    两者都 NULL 则判为无效（fail-closed）。
    """
    if session.revoked_at is not None:
        return False

    now = now or datetime.utcnow()
    exp = session.expires_at
    if exp is None:
        if session.created_at is None:
            return False
        exp = session.created_at + timedelta(seconds=SESSION_TTL_SECONDS)
    return exp > now


async def load_valid_session(
    db: AsyncSession, token: str
) -> Optional[UserSession]:
    """按 token 查会话并校验有效性；无效返回 None（不抛异常）。"""
    if not token:
        return None
    result = await db.execute(
        select(UserSession).where(UserSession.token == token)
    )
    row = result.scalar_one_or_none()
    if row is None or not session_is_valid(row):
        return None
    return row


async def create_user_session(db: AsyncSession, user) -> UserSession:
    """为指定用户创建新会话（含过期时间），供 register/login 共用。"""
    token = generate_token()
    now = datetime.utcnow()
    session = UserSession(
        user_id=user.id,
        token=token,
        email=user.email,
        role=user.role,
        created_at=now,
        expires_at=now + timedelta(seconds=SESSION_TTL_SECONDS),
    )
    db.add(session)
    return session
