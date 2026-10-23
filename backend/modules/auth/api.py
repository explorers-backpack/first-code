# -*- coding: utf-8 -*-
"""认证模块的 HTTP 层 —— **只做协议、参数校验、鉴权与响应转换**。

本层职责边界（与 ``MODULAR_MONOLITH_PLAN.md`` 的 R7/R8 对应）：

- **不写 SQL**：所有查询在 :mod:`modules.auth.service` 里；
- **不判断业务规则**：同邮箱 5 账号上限、时间侧信道、限流、ticket 作废都在 service；
- **只做三件事**：从请求里取 ``client_ip`` / 依赖注入 / 把 :class:`AuthError`
  翻译成 ``HTTPException``。

★ 契约逐字不变
路径、方法、``tags``、请求体模型、响应体形状、状态码、``detail`` 文案
全部与搬迁前一致（``/api/auth/register|login|logout|me``）。因此前端与
``tests/test_auth_security.py`` / ``test_admin_role.py`` / ``test_authz_ownership.py``
无需任何改动。
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from deps import get_current_session, get_current_user
# ★ 用 `import modules.auth.service as service` 而不是 `from modules.auth import service`：
#   后者会让本模块依赖**包门面** `modules.auth`，而门面顶层又 `from modules.auth.api
#   import router` ⇒ `modules.auth <-> modules.auth.api` 构成**硬环**（顶层互相 import）。
#   运行期两种写法等价（`from X import sub` 取不到属性时会回落到子模块导入），
#   但 AST 上只有前者是「api → service」这条单向边。守卫见 tests/test_module_boundaries.py R6。
import modules.auth.service as service
from modules.auth.schemas import UserLogin, UserRegister

router = APIRouter(prefix="/api/auth", tags=["认证"])


def _to_http(exc: service.AuthError) -> HTTPException:
    """领域错误 → HTTP 错误（**状态码 / 文案 / headers 原样透传**）。"""
    return HTTPException(
        status_code=exc.status_code,
        detail=exc.detail,
        headers=exc.headers,
    )


@router.post("/register")
async def register(
    data: UserRegister,
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """用户注册接口。同一 email 最多注册 5 个账号。"""
    try:
        return await service.register(db, data)
    except service.AuthError as exc:
        raise _to_http(exc) from None


@router.post("/login")
async def login(
    data: UserLogin,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """用户登录接口。支持 email 或 username 登录。
    未知账号统一返回 401，不自动创建。

    限流存储由 ``AUTH_LOGIN_STORE`` 决定（``memory`` 默认 / ``db`` 共享表）。
    存储不可用时 **fail closed**（503）：放行等于在库抖动期间关掉爆破保护。
    """
    client_ip = request.client.host if request.client else "-"
    try:
        return await service.login(db, data, client_ip=client_ip)
    except service.AuthError as exc:
        raise _to_http(exc) from None


@router.post("/logout")
async def logout(
    session=Depends(get_current_session),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """撤销当前会话（使 token 立即失效），并作废该用户所有未使用的 WS ticket。

    ★ 还要广播一条撤销：**已建立**的 WS 连接不受 ticket 作废影响
    （ticket 早已被消费），必须由各实例的轮询器读到广播后主动断开。
    广播按 ``auth_session_id`` 精确匹配 ⇒ 只踢**这一条**登录会话，
    同一用户在其他设备上的面试不受影响。
    """
    try:
        return await service.logout(db, session)
    except service.AuthError as exc:
        raise _to_http(exc) from None


@router.get("/me")
async def me(current_user: dict = Depends(get_current_user)) -> Dict[str, Any]:
    """返回当前登录用户信息（供前端恢复登录态时校验）。"""
    return service.me(current_user)


__all__ = ["router"]
