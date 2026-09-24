# -*- coding: utf-8 -*-
"""管理员账号与用户角色逻辑 · 自检（HTTP 全链路 + 脚本行为）

无需 pytest，直接运行：
    python backend/tests/test_admin_role.py

覆盖（需求 A~F 的可回归版本）
----------------------------
1. 注册接口把 role **固定为 user**；客户端传 ``{"role": "admin"}`` 被丢弃
2. ``UserRegister`` 请求模型**不含 role 字段**且 ``extra="ignore"``
3. 登录返回真实 role（admin -> admin；普通用户 -> user；未知邮箱自动创建为 user）
4. ``admin`` token 可访问 ``/api/admin/*``；``user`` token 返回 **403**；
   无 token / 伪造 token 返回 **401**
5. ``scripts/create_admin.py`` 幂等语义：
   - 不存在        -> 创建，role=admin
   - 已存在且 admin -> 跳过（退出码 0，不改凭据）
   - 已存在但非 admin -> **拒绝并退出码 1，不静默提权**
   - ``--promote-existing`` -> 才提权

数据库使用 SQLite 内存库（StaticPool），**不触碰本机 MySQL**，
也不触发 lifespan 建表逻辑。
"""

import argparse
import asyncio
import importlib.util
import os
import pathlib
import sys

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

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
from models import User  # noqa: E402

ADMIN_EMAIL = "admin@career.ai"
ADMIN_PASSWORD = "admin123"
ADMIN_ROLE = "admin"
USER_ROLE = "user"

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


def _load_create_admin():
    """从 scripts/create_admin.py 载入模块（scripts/ 非包，故用 importlib）。"""
    path = _BACKEND / "scripts" / "create_admin.py"
    spec = importlib.util.spec_from_file_location("career_ai_create_admin", str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _ns(**kw):
    base = dict(email=ADMIN_EMAIL, username="admin", password=ADMIN_PASSWORD,
                dry_run=False, promote_existing=False, reset_password=False)
    base.update(kw)
    return argparse.Namespace(**base)


async def run() -> bool:
    print("=" * 68)
    print("管理员账号与用户角色逻辑 · 自检")
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

    create_admin = _load_create_admin()

    async def run_script(**kw) -> int:
        """在测试库上执行 create_admin.run()（临时把 main.async_session 指向测试库）。"""
        original = main.async_session
        main.async_session = session_factory
        try:
            return await create_admin.run(_ns(**kw))
        finally:
            main.async_session = original

    async def db_user(email):
        async with session_factory() as s:
            return (await s.execute(select(User).where(User.email == email))).scalar_one_or_none()

    # ========================================================
    print("\n[1] create_admin.py：不存在 -> 创建 role=admin")
    code = await run_script()
    u = await db_user(ADMIN_EMAIL)
    _check("退出码 0", code == 0, str(code))
    _check(f"已创建且 role='admin'（实际 {u and u.role!r}）", u is not None and u.role == ADMIN_ROLE)
    _check("密码使用项目哈希机制 _hash_password",
           u is not None and u.password_hash == main._hash_password(ADMIN_PASSWORD))

    print("\n[2] create_admin.py：已存在且 admin -> 幂等跳过，不改凭据")
    old_hash = u.password_hash
    code = await run_script(password="another_password")
    u2 = await db_user(ADMIN_EMAIL)
    _check("退出码 0", code == 0, str(code))
    _check("role 仍为 admin", u2.role == ADMIN_ROLE)
    _check("密码未被静默修改", u2.password_hash == old_hash)

    print("\n[3] create_admin.py：已存在但 role=user -> 拒绝，不静默提权")
    async with session_factory() as s:
        s.add(User(username="bob", email="bob@example.com",
                   password_hash=main._hash_password("bob123456"), role=USER_ROLE))
        await s.commit()
    code = await run_script(email="bob@example.com", username="bob")
    bob = await db_user("bob@example.com")
    _check("退出码 1（明确失败，不假装成功）", code == 1, str(code))
    _check(f"role 保持 'user' 未被提权（实际 {bob.role!r}）", bob.role == USER_ROLE)

    print("\n[4] create_admin.py：显式 --promote-existing 才提权")
    code = await run_script(email="bob@example.com", username="bob", promote_existing=True)
    bob2 = await db_user("bob@example.com")
    _check("退出码 0", code == 0, str(code))
    _check(f"role 已提权为 'admin'（实际 {bob2.role!r}）", bob2.role == ADMIN_ROLE)

    print("\n[5] create_admin.py：--dry-run 不写库")
    code = await run_script(email="dry@example.com", username="dryuser", dry_run=True)
    _check("退出码 0", code == 0, str(code))
    _check("dry-run 未创建任何用户", await db_user("dry@example.com") is None)

    # ========================================================
    transport = ASGITransport(app=main.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:

        print("\n[6] 登录：admin 返回 role=admin（需求 A/B）")
        r = await c.post("/api/auth/login", json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
        b = r.json()
        _check("HTTP 200", r.status_code == 200, str(b)[:160])
        _check(f"user.role == 'admin'（实际 {b.get('user', {}).get('role')!r}）",
               b.get("user", {}).get("role") == ADMIN_ROLE)
        admin_token = b.get("token")
        _check("返回 token", bool(admin_token))

        print("\n[7] 注册：role 固定为 user（需求 C）")
        r = await c.post("/api/auth/register", json={
            "username": "alice", "email": "alice@example.com", "password": "alice123456"})
        b = r.json()
        _check("HTTP 200", r.status_code == 200, str(b)[:160])
        _check(f"响应 user.role == 'user'（实际 {b.get('user', {}).get('role')!r}）",
               b.get("user", {}).get("role") == USER_ROLE)
        user_token = b.get("token")
        _check("DB role == 'user'",
               (await db_user("alice@example.com")).role == USER_ROLE)

        print("\n[8] 注册时传 role=admin 不得成为管理员（需求 D）")
        r = await c.post("/api/auth/register", json={
            "username": "mallory", "email": "mallory@example.com", "password": "mallory123456",
            "role": "admin", "is_admin": True, "user_role": "admin"})
        b = r.json()
        _check("HTTP 200（越权字段被忽略而非报错）", r.status_code == 200, str(b)[:160])
        _check(f"响应 role 仍为 'user'（实际 {b.get('user', {}).get('role')!r}）",
               b.get("user", {}).get("role") == USER_ROLE)
        mallory = await db_user("mallory@example.com")
        _check(f"DB role 仍为 'user'（实际 {mallory.role!r}）", mallory.role == USER_ROLE)
        mallory_token = b.get("token")

        print("\n[9] UserRegister 模型不含 role 字段（需求 3）")
        from main import UserRegister
        _check("字段集合 == {username, email, password}",
               set(UserRegister.model_fields) == {"username", "email", "password"},
               str(set(UserRegister.model_fields)))
        _check("extra == 'ignore'", UserRegister.model_config.get("extra") == "ignore",
               str(UserRegister.model_config))

        print("\n[10] 登录：未知邮箱自动创建，且 role 必须是 user")
        r = await c.post("/api/auth/login", json={"email": "brandnew@example.com", "password": "pw123456"})
        b = r.json()
        _check("HTTP 200", r.status_code == 200, str(b)[:160])
        _check(f"自动创建的账号 role == 'user'（实际 {b.get('user', {}).get('role')!r}）",
               b.get("user", {}).get("role") == USER_ROLE)
        _check("DB 中亦为 'user'", (await db_user("brandnew@example.com")).role == USER_ROLE)

        print("\n[11] admin 可访问 /api/admin/*（需求 E）")
        ha = {"Authorization": f"Bearer {admin_token}"}
        r = await c.get("/api/admin/user-logs", headers=ha)
        _check(f"GET /api/admin/user-logs -> 200（实际 {r.status_code}）", r.status_code == 200,
               str(r.json())[:160])
        r = await c.post("/api/admin/add-job", headers=ha,
                         json={"job_name": "测试岗位", "city": "北京", "skills": ["Python"]})
        _check(f"POST /api/admin/add-job -> 200（实际 {r.status_code}）", r.status_code == 200,
               str(r.json())[:160])

        print("\n[12] user 访问 /api/admin/* 返回 403（需求 F）")
        for label, tok in (("普通注册用户", user_token), ("越权注册账号", mallory_token)):
            hu = {"Authorization": f"Bearer {tok}"}
            r = await c.get("/api/admin/user-logs", headers=hu)
            _check(f"{label} GET -> 403（实际 {r.status_code}）", r.status_code == 403, str(r.json())[:120])
            r = await c.post("/api/admin/add-job", headers=hu,
                             json={"job_name": "越权岗位", "city": "北京"})
            _check(f"{label} POST -> 403（实际 {r.status_code}）", r.status_code == 403)

        print("\n[13] 无 token / 伪造 token -> 401")
        _check("无 Authorization -> 401", (await c.get("/api/admin/user-logs")).status_code == 401)
        _check("伪造 token -> 401",
               (await c.get("/api/admin/user-logs",
                            headers={"Authorization": "Bearer not-a-real-token"})).status_code == 401)
        _check("格式错误的头 -> 401",
               (await c.get("/api/admin/user-logs",
                            headers={"Authorization": "Token abc"})).status_code == 401)

        print("\n[14] 普通用户登录保持 role=user（需求 7）")
        r = await c.post("/api/auth/login", json={"email": "alice@example.com", "password": "alice123456"})
        _check(f"role == 'user'（实际 {r.json().get('user', {}).get('role')!r}）",
               r.json().get("user", {}).get("role") == USER_ROLE)

    main.app.dependency_overrides.clear()
    await engine.dispose()

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
