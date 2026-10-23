# -*- coding: utf-8 -*-
"""认证用例（注册 / 登录 / 登出 / 当前用户）—— **业务编排 + 事务边界**。

分层位置
--------
``modules/auth/api.py``（HTTP） → **本模块**（用例 + 事务） → ``security`` / ``models`` /
``services.login_throttle_store`` / ``services.ws_ticket_store`` / ``services.ws_revocation``

本模块**不 import FastAPI**：所有失败都以 :class:`AuthError`（带 ``status_code`` /
``detail`` / ``headers``）抛出，由 API 层翻译成 ``HTTPException``。
这样做不是洁癖——领域层一旦能直接抛 ``HTTPException``，就会出现「服务层里判断
Header / 构造 Response」的滑坡，而这些东西是没法脱离 HTTP 测试的。

为什么从 ``main.py`` 搬出来
--------------------------
原本四个端点的业务逻辑（同邮箱 5 账号上限、未知账号跑 dummy bcrypt 消除时间侧信道、
口令格式升级、登录限流 fail-closed、登出连带作废 WS ticket 并广播撤销）
**全部内联在路由函数里**。路由层因此既管 HTTP 又管事务边界和 SQL，
改任何一条业务规则都要动入口文件——这正是「模块化单体」要消掉的东西。

事务边界（**必须与搬迁前逐字一致**）
------------------------------------
- ``register``：``commit`` → ``refresh`` → ``create_user_session`` → ``commit``
- ``login``：读用户 → （按需升级口令）→ 写登录日志 + 会话 → ``commit`` → ``throttle_reset``
- ``logout``：``revoked_at`` → ``commit`` → 广播撤销 → 作废 ticket（后两步失败**不阻断**登出）

★ 上面三步的 **commit 次数与顺序** 是行为契约的一部分（登出后 token 必须立刻失效），
搬迁时不许"顺手优化"成一次 commit。
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import User, UserLoginLog
from security import (
    create_user_session,
    generate_token,
    hash_password,
    verify_password,
)
from services import ws_revocation, ws_ticket_store
from services.login_throttle_store import (
    LoginThrottleUnavailable,
    check as throttle_check,
    record as throttle_record,
    reset as throttle_reset,
)

logger = logging.getLogger(__name__)
_ticket_log = logging.getLogger("api.ws_ticket")

#: 同一 email 最多可注册的账号数（历史规则，前端文案与之一致）
MAX_ACCOUNTS_PER_EMAIL = 5


# ============================================================
# 错误模型（领域层不依赖 HTTP）
# ============================================================
class AuthError(Exception):
    """认证用例的可预期失败。

    携带 ``status_code`` / ``detail`` / ``headers``，由 API 层原样翻译成
    ``HTTPException`` ⇒ 对客户端来说**状态码与文案逐字不变**，
    但领域层从此不再 import FastAPI。
    """

    def __init__(
        self,
        detail: str,
        *,
        status_code: int = 400,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code
        self.headers = headers


class LoginThrottleError(AuthError):
    """登录限流存储不可用 ⇒ 503（**fail closed**，不静默放行）。"""

    def __init__(self) -> None:
        super().__init__("登录服务暂时不可用，请稍后再试", status_code=503)


# ============================================================
# 口令助手（历史兼容，**仅供旧格式校验与测试造数**）
# ============================================================
def _hash_password(password: str) -> str:
    """无盐 SHA-256（**仅旧格式兼容 / 测试造数**）。新写入请用 ``security.hash_password``。"""
    return hashlib.sha256(password.encode()).hexdigest()


#: 薄导出，供测试与脚本校验口令而不直接 import ``security``
_verify_password = verify_password


def _generate_token(user_id: int, email: str, role: str) -> str:
    """生成高熵 token（参数保留仅为了接口兼容）。"""
    return generate_token()


# ============================================================
# 内部：限流存储 fail-closed 包装
# ============================================================
async def _throttle(coro):
    """限流存储不可用 ⇒ :class:`LoginThrottleError`（不静默放行，也不混进 401）。"""
    try:
        return await coro
    except LoginThrottleUnavailable:
        raise LoginThrottleError() from None


def _user_payload(user: User) -> Dict[str, Any]:
    """对外暴露的用户字段（**永不包含 password_hash**）。"""
    return {
        "id": user.id,
        "username": user.username,
        "email": user.email,
        "role": user.role,
    }


# ============================================================
# 用例
# ============================================================
async def register(db: AsyncSession, data: Any) -> Dict[str, Any]:
    """注册：同邮箱最多 :data:`MAX_ACCOUNTS_PER_EMAIL` 个账号；用户名 / 邮箱唯一。"""
    result = await db.execute(
        select(func.count()).select_from(User).where(User.email == data.email)
    )
    if (result.scalar() or 0) >= MAX_ACCOUNTS_PER_EMAIL:
        raise AuthError(
            f"该邮箱注册账号数量已达上限（同一邮箱最多{MAX_ACCOUNTS_PER_EMAIL}个账号）"
        )

    result = await db.execute(select(User).where(User.username == data.username))
    if result.scalar_one_or_none():
        raise AuthError("用户名已存在")

    result = await db.execute(select(User).where(User.email == data.email))
    if result.scalar_one_or_none():
        raise AuthError("邮箱已被使用")

    new_user = User(
        username=data.username,
        email=data.email,
        password_hash=hash_password(data.password),
        role="user",
    )
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)

    # 创建会话（与搬迁前一致：create_user_session 只 add，不 commit）
    session = await create_user_session(db, new_user)
    await db.commit()
    token = session.token

    return {
        "success": True,
        "message": "注册成功",
        "user": _user_payload(new_user),
        "token": token,
    }


async def login(db: AsyncSession, data: Any, *, client_ip: str = "-") -> Dict[str, Any]:
    """登录：支持邮箱或用户名；未知账号与错密码**同一文案**；限流 fail-closed。"""
    throttle_key = f"{data.email.lower().strip()}|{client_ip}"

    retry_after = await _throttle(throttle_check(db, throttle_key))
    if retry_after:
        raise AuthError(
            "登录尝试过多，请稍后再试",
            status_code=429,
            headers={"Retry-After": str(retry_after)},
        )

    result = await db.execute(select(User).where(User.email == data.email))
    user = result.scalar_one_or_none()

    if not user:
        result = await db.execute(select(User).where(User.username == data.email))
        user = result.scalar_one_or_none()

    if not user:
        # 未知账号也跑一次口令校验，消除时间侧信道
        verify_password(data.password, "")
        await _throttle(throttle_record(db, throttle_key))
        raise AuthError("邮箱或密码错误", status_code=401)

    ok, needs_upgrade = verify_password(data.password, user.password_hash)
    if not ok:
        await _throttle(throttle_record(db, throttle_key))
        raise AuthError("邮箱或密码错误", status_code=401)

    if needs_upgrade:
        user.password_hash = hash_password(data.password)

    db.add(UserLoginLog(
        email=user.email,
        role=user.role,
        last_login=datetime.now().strftime("%Y-%m-%d %H:%M"),
        search_count=0,
    ))

    session = await create_user_session(db, user)
    db.add(session)
    await db.commit()
    token = session.token

    await throttle_reset(db, throttle_key)

    return {
        "success": True,
        "message": "登录成功",
        "token": token,
        "user": _user_payload(user),
    }


async def logout(db: AsyncSession, session) -> Dict[str, Any]:
    """登出：撤销会话 + 广播 WS 撤销 + 作废未使用的 WS ticket。

    ★ 后两步失败**不阻断**登出：主效应（token 撤销）已落库，ticket 30 秒后自然过期，
    撤销还有 60 秒周期复核兜底。但**必须留下 WARNING**（不静默）。
    """
    session.revoked_at = datetime.utcnow()
    db.add(session)
    await db.commit()

    try:
        await ws_revocation.record(
            db,
            user_id=session.user_id,
            auth_session_id=session.id,
            reason=ws_revocation.REASON_LOGOUT,
        )
    except ws_revocation.RevocationStoreUnavailable as exc:
        _ticket_log.warning(
            "ws_revocation_record_on_logout_failed user_id=%s error=%s",
            session.user_id,
            type(exc).__name__,
        )

    # ★ 为什么要连带作废 ticket：ticket 路径下**没有**长期 token 可复核
    #   （见 api/interview_ws.py 的 _auth_still_valid），已签发的 ticket 在 30 秒内
    #   仍然能换到一条 WS 连接。登出即作废它们，把"登出 ⇒ 立即失效"补齐。
    try:
        revoked = await ws_ticket_store.revoke_for_user(db, user_id=session.user_id)
        if revoked:
            _ticket_log.info(
                "ws_ticket_revoked_on_logout user_id=%s count=%s",
                session.user_id,
                revoked,
            )
    except ws_ticket_store.TicketStoreUnavailable as exc:
        _ticket_log.warning(
            "ws_ticket_revoke_on_logout_failed user_id=%s error=%s",
            session.user_id,
            exc,
        )

    return {"success": True, "message": "已安全退出"}


def me(current_user: Dict[str, Any]) -> Dict[str, Any]:
    """当前登录用户信息（供前端恢复登录态时校验）。"""
    return {"success": True, "user": current_user}


# ============================================================
# 兼容导出（供测试与旧调用方）
# ============================================================
__all__ = [
    "AuthError",
    "LoginThrottleError",
    "MAX_ACCOUNTS_PER_EMAIL",
    "_generate_token",
    "_hash_password",
    "_verify_password",
    "login",
    "logout",
    "me",
    "register",
]
