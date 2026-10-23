# -*- coding: utf-8 -*-
"""认证安全硬化 · 自检（密码/token/会话/限流）

无需 pytest，直接运行：
    python backend/tests/test_auth_security.py
"""

import asyncio
import os
import pathlib
import sys

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

import regression_env  # noqa: E402,F401

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import main  # noqa: E402
import models  # noqa: E402,F401
from database import Base, get_db  # noqa: E402
from models import User, UserSession  # noqa: E402
# ★ 旧格式口令助手随认证模块迁到 modules/auth/service.py（原 main._hash_password）
from modules.auth import service as auth_service  # noqa: E402
from security import (  # noqa: E402
    hash_password,
    verify_password,
    generate_token,
    session_is_valid,
    create_user_session,
    load_valid_session,
    SESSION_TTL_SECONDS,
)

_USER_EMAIL = "tester@example.com"
_USER_PASSWORD = "tester123456"

_PASSED = 0
_FAILED = 0


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (f"  -> {detail}" if detail and not cond else ""))
    return cond


async def run() -> bool:
    print("=" * 68)
    print("认证安全硬化 · 自检")
    print("=" * 68)

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async def override_get_db():
        async with session_factory() as session:
            yield session

    main.app.dependency_overrides[get_db] = override_get_db

    # ========================================================
    print("\n[1] token 生成：高熵、每次不同、长度充足")
    t1 = generate_token()
    t2 = generate_token()
    _check("两次 token 不同", t1 != t2)
    _check("token 长度 >= 40", len(t1) >= 40)

    # ========================================================
    print("\n[2] bcrypt 哈希：同密码不同盐")
    h1 = hash_password(_USER_PASSWORD)
    h2 = hash_password(_USER_PASSWORD)
    _check("两次哈希不同", h1 != h2)
    _check("带 bcrypt$ 前缀", h1.startswith("bcrypt$"))

    # ========================================================
    print("\n[3] verify_password：新格式通过、旧格式通过且需升级、空串失败")
    ok, up = verify_password(_USER_PASSWORD, h1)
    _check("bcrypt 校验通过", ok)
    _check("bcrypt 不需要升级", not up)

    old_hash = auth_service._hash_password(_USER_PASSWORD)
    ok2, up2 = verify_password(_USER_PASSWORD, old_hash)
    _check("sha256 校验通过", ok2)
    _check("sha256 需要升级", up2)

    ok3, up3 = verify_password(_USER_PASSWORD, "")
    _check("空 stored 失败", not ok3)

    # ========================================================
    print("\n[4] session_is_valid：撤销/过期/NULL 语义")
    from datetime import datetime, timedelta

    now = datetime.utcnow()

    s_revoked = UserSession(user_id=1, token="t", email="a", role="user", revoked_at=now)
    _check("revoked_at 非空 → 无效", not session_is_valid(s_revoked, now))

    s_expired = UserSession(
        user_id=1, token="t", email="a", role="user",
        created_at=now - timedelta(seconds=SESSION_TTL_SECONDS + 10),
        expires_at=now - timedelta(seconds=5),
    )
    _check("expires_at 已过期 → 无效", not session_is_valid(s_expired, now))

    s_fresh = UserSession(
        user_id=1, token="t", email="a", role="user",
        created_at=now - timedelta(seconds=10),
        expires_at=now + timedelta(seconds=SESSION_TTL_SECONDS),
    )
    _check("expires_at 未过期 → 有效", session_is_valid(s_fresh, now))

    s_null = UserSession(
        user_id=1, token="t", email="a", role="user",
        created_at=now - timedelta(seconds=10),
    )
    _check("expires_at NULL 但 created_at 近期 → 有效", session_is_valid(s_null, now))

    s_bare = UserSession(user_id=1, token="t", email="a", role="user")
    _check("expires_at + created_at 都 NULL → 无效（fail-closed）", not session_is_valid(s_bare, now))

    # ========================================================
    print("\n[5] load_valid_session：bad token / 过期 / 撤销")
    async with session_factory() as s:
        bad = await load_valid_session(s, "no-such-token")
        _check("不存在的 token → None", bad is None)

    # ========================================================
    print("\n[6] create_user_session：正确设置 expires_at")
    async with session_factory() as s:
        u = User(username="alice", email=_USER_EMAIL, password_hash=h1, role="user")
        s.add(u)
        await s.commit()
        await s.refresh(u)

        sess = await create_user_session(s, u)
        await s.commit()
        _check("expires_at 已设置", sess.expires_at is not None)
        _check("expires_at > created_at", sess.expires_at > sess.created_at)

    # ========================================================
    transport = ASGITransport(app=main.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        print("\n[7] 注册：密码存为 bcrypt")
        r = await c.post("/api/auth/register", json={
            "username": "bob", "email": "bob@example.com", "password": "bob123456"})
        b = r.json()
        _check("HTTP 200", r.status_code == 200, str(b)[:160])
        bob_token = b.get("token")
        _check("返回 token", bool(bob_token))

        async with session_factory() as s:
            bob = (await s.execute(select(User).where(User.email == "bob@example.com"))).scalar_one_or_none()
            _check("DB 密码是 bcrypt", bob is not None and bob.password_hash.startswith("bcrypt$"))

        print("\n[8] 登录：旧 sha256 账号透明升级为 bcrypt")
        # alice 的密码是 h1（bcrypt），bob 是 bcrypt；创建一个 sha256 旧账号
        async with session_factory() as s:
            old_user = User(
                username="oldie", email="oldie@example.com",
                password_hash=auth_service._hash_password("oldie123456"), role="user")
            s.add(old_user)
            await s.commit()

        r = await c.post("/api/auth/login", json={"email": "oldie@example.com", "password": "oldie123456"})
        _check("旧账号登录 200", r.status_code == 200)
        async with session_factory() as s:
            oldie = (await s.execute(select(User).where(User.email == "oldie@example.com"))).scalar_one_or_none()
            _check("登录后密码已升级为 bcrypt", oldie is not None and oldie.password_hash.startswith("bcrypt$"))

        print("\n[9] 登录：错误密码 401，未知邮箱 401")
        r = await c.post("/api/auth/login", json={"email": "oldie@example.com", "password": "wrong"})
        _check("错误密码 → 401", r.status_code == 401)
        r = await c.post("/api/auth/login", json={"email": "nobody@example.com", "password": "any"})
        _check("未知邮箱 → 401", r.status_code == 401)

        print("\n[10] logout：撤销后 token 失效")
        ha = {"Authorization": f"Bearer {bob_token}"}
        r = await c.post("/api/auth/logout", headers=ha)
        _check("logout 200", r.status_code == 200)
        r = await c.get("/api/admin/user-logs", headers=ha)
        _check("撤销后再用原 token → 401", r.status_code == 401)

        print("\n[11] /api/auth/me：返回当前用户")
        # 用 oldie 的 token（登录后得到的）
        oldie_token = (await c.post("/api/auth/login", json={
            "email": "oldie@example.com", "password": "oldie123456"})).json()["token"]
        r = await c.get("/api/auth/me", headers={"Authorization": f"Bearer {oldie_token}"})
        b = r.json()
        _check("me 200", r.status_code == 200)
        _check("me 返回 email", b.get("user", {}).get("email") == "oldie@example.com")

        print("\n[12] 登录限流：5 次失败后第 6 次 429")
        # 先清空可能存在的旧计数（同一 email|ip）
        from utils.login_throttle import reset as throttle_reset
        throttle_reset("throttle_test@example.com|-")
        for i in range(5):
            r = await c.post("/api/auth/login", json={
                "email": "throttle_test@example.com", "password": "wrong"})
            _check(f"第 {i+1} 次失败登录 401", r.status_code == 401)
        r = await c.post("/api/auth/login", json={
            "email": "throttle_test@example.com", "password": "wrong"})
        _check("第 6 次失败登录 429", r.status_code == 429)
        _check("429 带 Retry-After", r.headers.get("retry-after") is not None, str(dict(r.headers)))

        print("\n[13] 422 校验失败：不回显提交值（密码不落响应体）")
        r = await c.post("/api/auth/register", json={
            "username": "ab", "email": "leak@example.com", "password": "SEC"})
        body_text = r.text
        _check("短密码 → 422", r.status_code == 422, str(r.status_code))
        _check("★ 响应体不含提交的密码", "SEC" not in body_text, body_text[:200])
        errors = r.json().get("detail", [])
        _check("detail 是错误数组", isinstance(errors, list) and len(errors) >= 1, str(errors)[:160])
        _check("★ 错误项不含 input 字段",
               all("input" not in item for item in errors), str(errors)[:200])
        _check("保留 loc/msg（前端可继续展示）",
               all("loc" in item and "msg" in item for item in errors), str(errors)[:200])

    print("\n[14] 访问日志脱敏：URL query 里的凭证被打码")
    import logging as _logging
    from uvicorn.logging import AccessFormatter
    from main import _LogRedactingFilter

    record = _logging.LogRecord(
        "uvicorn.access", _logging.INFO, "", 0,
        '%s - "%s %s HTTP/%s" %d',
        ("127.0.0.1:5000", "GET", "/api/interview/12/ws?ticket=SECRETTICKET123", "1.1", 101),
        None,
    )
    _LogRedactingFilter().filter(record)
    line = AccessFormatter('%(client_addr)s - "%(request_line)s" %(status_code)s').format(record)
    _check("★ ticket 值被掩码", "SECRETTICKET123" not in line, line)
    _check("  └ 保留末 4 位便于核对", line.rstrip().endswith('101 Switching Protocols') and "****" in line, line)

    record2 = _logging.LogRecord(
        "uvicorn.access", _logging.INFO, "", 0,
        '%s - "%s %s HTTP/%s" %d',
        ("127.0.0.1:5000", "GET", "/api/interview/12/ws?token=LONGTERMTOKEN", "1.1", 101),
        None,
    )
    _LogRedactingFilter().filter(record2)
    line2 = AccessFormatter('%(client_addr)s - "%(request_line)s" %(status_code)s').format(record2)
    _check("★ 长期 token 值被掩码", "LONGTERMTOKEN" not in line2, line2)
    _check("  └ args 结构未被破坏（AccessFormatter 正常解包）",
           "101 Switching Protocols" in line2, line2)

    # ★ 过滤器**真的挂上了**（不是只定义了一个类）：
    #   实例化一个过滤器能脱敏，不等于线上那条日志会经过它 —— 必须查 logger.filters。
    for name in ("", "uvicorn", "uvicorn.access", "uvicorn.error"):
        target = _logging.getLogger(name)
        _check(
            f"  └ 已挂到 {name or '<root>'} 日志器上（否则访问日志照样漏）",
            any(isinstance(f, _LogRedactingFilter) for f in target.filters),
            str(target.filters),
        )

    # 异常日志路径：httpx / 连接异常的消息里会**内嵌完整 URL**，
    # 这类日志走 uvicorn.error 而不是 uvicorn.access。
    exc_record = _logging.LogRecord(
        "uvicorn.error", _logging.ERROR, "", 0,
        "Exception in ASGI application: ConnectError(\"ws://h/api/interview/12/ws?ticket=EXCITICKET999\")",
        None, None,
    )
    _LogRedactingFilter().filter(exc_record)
    _check("★ 异常日志里内嵌的 ticket 也被打码（不只访问日志）",
           "EXCITICKET999" not in exc_record.getMessage(), exc_record.getMessage())

    exc_record2 = _logging.LogRecord(
        "careerai.ws_ticket", _logging.WARNING, "", 0,
        "handshake failed url=%s", ("ws://h/api/interview/12/ws?token=LONGTERMTOKEN2",), None,
    )
    _LogRedactingFilter().filter(exc_record2)
    _check("★ 以 args 形式传入的 URL 同样被打码（uvicorn 就是这种写法）",
           "LONGTERMTOKEN2" not in exc_record2.getMessage(), exc_record2.getMessage())

    main.app.dependency_overrides.clear()
    await engine.dispose()

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
