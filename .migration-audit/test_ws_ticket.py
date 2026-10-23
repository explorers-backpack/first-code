# -*- coding: utf-8 -*-
"""WebSocket 一次性连接凭证（ticket）· 自检

无需 pytest，直接运行：
    python backend/tests/test_ws_ticket.py

★ 本套件的定位（能力边界，别当成"验证了多实例并发"）
----------------------------------------------------
跑在**临时文件 SQLite**上。SQLite 没有真正的多连接并发写，所以"两个 worker
同时消费同一个 ticket 只成功一次"**不在这里证明**——那由配套运行器
``backend/tests/ws_ticket_mysql_concurrency_run.py`` 在**真实 MySQL 8** 上验证
（``asyncio.gather`` + 每路独立物理连接）。本套件证明的是：

1. 存储语义：签发 / 消费 / 过期 / 重放 / 会话绑定 / 撤销 / 清理；
2. 消费的**原子性判据是 rowcount**（不是"先查再写"的进程内判断）；
3. 存储不可用时**安全失败**（签发 503 / 握手 1011），**不降级**到进程内字典；
4. 鉴权链路：端点鉴权 + 归属校验 + WS 握手成功 / 失败；
5. 源码守卫：进程内字典已彻底移除，存储只在 service 层实现一次。

覆盖：
[1]  签发：密码学随机、URL-safe、库里**只存摘要**（不存明文）
[2]  正常签发 + 消费：首次成功返回 user_id，且库里标记 used_at
[3]  单次使用：第二次失败（不可重放）
[4]  过期：expires_at 已过 → 消费失败
[5]  会话绑定：错配失败，且**不消耗**该 ticket
[6]  空 / 不存在：fail-closed
[7]  撤销：单个撤销 / 按用户批量撤销
[8]  清理：只删过期行，未过期的保留
[9]  存储不可用（表被删）：consume 抛 TicketStoreUnavailable（**不是** None）
[10] 端点：未登录 401 / 他人会话 404 / 本人 200
[11] 端点：存储不可用 → 503（不发废票）
[12] 实时通道：ticket 连接成功
[13] 实时通道：同一 ticket 重放 → 4401
[14] 实时通道：无凭证 → 4401
[15] 实时通道：他人 token → 4404
[15b] Origin 校验：白名单放行 / 跨站拒绝
[15c] 兼容路径开关：``INTERVIEW_WS_TOKEN_AUTH``（默认开 / 可显式关 / 非法值不静默）
[16] 实时通道：存储不可用 → 1011（安全失败，不降级）
[17] 登出：作废该用户未使用的 ticket
[18] 源码守卫：进程内字典已移除 / 存储只在 service 层 / 不碰编排层

``AVATAR_PROVIDER`` 由 ``regression_env`` 钉成空串 ⇒ 数字人走 Mock，零联网。
"""

import ast
import asyncio
import os
import pathlib
import shutil
import sys
import tempfile

_TMP_DIR = tempfile.mkdtemp(prefix="career_ai_ticket_test_")
_DB_PATH = pathlib.Path(_TMP_DIR) / "ticket_test.db"

os.environ.setdefault("DATABASE_URL", f"sqlite+aiosqlite:///{_DB_PATH.as_posix()}")

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))
sys.path.insert(0, str(_BACKEND / "tests"))

import regression_env  # noqa: E402,F401  （钉住 AVATAR_* ⇒ 数字人走 Mock，不联网）

from sqlalchemy import select, text, update  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.pool import NullPool  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402
from starlette.websockets import WebSocketDisconnect  # noqa: E402

import main  # noqa: E402
import models  # noqa: E402,F401
from api import interview_ws as ws  # noqa: E402
from database import Base  # noqa: E402
from models import InterviewSession, User, UserSession, WsTicket  # noqa: E402
from services import ws_ticket_store as store  # noqa: E402

DB_URL = f"sqlite+aiosqlite:///{_DB_PATH.as_posix()}"
OWNER_TOKEN = "ticket-test-owner-token"
OTHER_TOKEN = "ticket-test-other-token"

_PASSED = 0
_FAILED = 0


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(
        ("  [PASS] " if cond else "  [FAIL] ")
        + name
        + (f"  -> {detail}" if detail and not cond else "")
    )
    return cond


# ============================================================
# 数据库辅助（每次调用一条**独立物理连接**，模拟"另一个进程"）
# ============================================================
async def _with_db(fn):
    engine = create_async_engine(DB_URL, poolclass=NullPool)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with factory() as db:
            return await fn(db)
    finally:
        await engine.dispose()


def db_call(fn):
    return asyncio.run(_with_db(fn))


def _prepare_db():
    """建表 + 种子数据（两个用户、两条会话、两条面试会话）。"""

    async def _run():
        engine = create_async_engine(DB_URL, poolclass=NullPool)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as db:
            owner = User(username="ticket_owner", email="ticket_owner@example.com",
                         password_hash="x", role="user")
            other = User(username="ticket_other", email="ticket_other@example.com",
                         password_hash="x", role="user")
            db.add_all([owner, other])
            await db.commit()
            await db.refresh(owner)
            await db.refresh(other)

            db.add_all([
                UserSession(user_id=owner.id, token=OWNER_TOKEN, email=owner.email, role="user"),
                UserSession(user_id=other.id, token=OTHER_TOKEN, email=other.email, role="user"),
            ])

            owner_session = InterviewSession(
                user_id=owner.id, interview_type="comprehensive", difficulty="mid",
                duration=30, interview_mode="avatar", status="created",
                total_questions=5, current_question_no=0,
            )
            other_session = InterviewSession(
                user_id=other.id, interview_type="comprehensive", difficulty="mid",
                duration=30, interview_mode="avatar", status="created",
                total_questions=5, current_question_no=0,
            )
            db.add_all([owner_session, other_session])
            await db.commit()
            await db.refresh(owner_session)
            await db.refresh(other_session)
            ids = (owner.id, other.id, owner_session.id, other_session.id)
        await engine.dispose()
        return ids

    return asyncio.run(_run())


def _revoke_token(token: str) -> None:
    """把指定 token 标记为已撤销（模拟 logout / 管理员踢下线）。"""

    async def _run(db):
        from datetime import datetime

        await db.execute(
            update(UserSession)
            .where(UserSession.token == token)
            .values(revoked_at=datetime.utcnow())
        )
        await db.commit()

    db_call(_run)


def _unrevoke_token(token: str) -> None:
    """还原撤销状态，避免影响后续用例。"""

    async def _run(db):
        await db.execute(
            update(UserSession).where(UserSession.token == token).values(revoked_at=None)
        )
        await db.commit()

    db_call(_run)


def _count_tickets() -> int:
    async def _run(db):
        return len((await db.execute(select(WsTicket))).scalars().all())

    return db_call(_run)


def _drop_ticket_table() -> None:
    async def _run(db):
        await db.execute(text("DROP TABLE ws_ticket"))
        await db.commit()

    db_call(_run)


def _recreate_ticket_table() -> None:
    """把被删掉的 ``ws_ticket`` 表建回来（``create_all`` 只补缺失的表）。"""

    async def _run():
        engine = create_async_engine(DB_URL, poolclass=NullPool)
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        finally:
            await engine.dispose()

    asyncio.run(_run())


# ============================================================
# [1]-[9] 存储层（真实持久化层，不是 mock）
# ============================================================
def _store_checks() -> None:
    import hashlib

    print("\n[1] 签发：随机、URL-safe、库里只存摘要")
    t1 = db_call(lambda db: store.issue(db, session_id=1, user_id=10))
    t2 = db_call(lambda db: store.issue(db, session_id=1, user_id=10))
    _check("ticket 是非空字符串", isinstance(t1, str) and len(t1) > 0)
    _check("长度 ≥ 32（256-bit 熵的 base64url 编码）", len(t1) >= 32, str(len(t1)))
    _check("两次生成不同（不是时间戳/自增）", t1 != t2)
    _check("只含 URL-safe 字符", all(c.isalnum() or c in "-_" for c in t1), t1)

    async def _row_of(db):
        return (
            await db.execute(
                select(WsTicket).where(WsTicket.ticket_hash == store.hash_ticket(t1))
            )
        ).scalar_one_or_none()

    row = db_call(_row_of)
    _check("★ 库里按 sha256(ticket) 能查到行", row is not None)
    _check(
        "★ 库里存的是摘要（64 位 hex），不是明文",
        row is not None and row.ticket_hash == hashlib.sha256(t1.encode()).hexdigest()
        and t1 not in row.ticket_hash,
    )
    _check("行里不含 ticket 明文（任何字段都查不到）",
           row is not None and t1 not in repr(row.__dict__))

    print("\n[2] 正常签发 + 消费")
    ticket = db_call(lambda db: store.issue(db, session_id=7, user_id=42))
    _check("首次消费返回 user_id", db_call(
        lambda db: store.consume(db, ticket=ticket, expected_session_id=7)) == 42)
    used = db_call(_row_of_used(ticket))
    _check("消费后 used_at 已写入（不是删除行，便于区分'用过'与'不存在'）",
           used is not None)

    print("\n[3] 单次使用：第二次失败")
    _check("★ 二次消费失败（不可重放）", db_call(
        lambda db: store.consume(db, ticket=ticket, expected_session_id=7)) is None)

    print("\n[4] 过期：expires_at 已过 → 消费失败")
    expired = db_call(lambda db: store.issue(db, session_id=8, user_id=43))

    async def _expire(db):
        await db.execute(
            update(WsTicket)
            .where(WsTicket.ticket_hash == store.hash_ticket(expired))
            .values(expires_at=_dt(-10))
        )
        await db.commit()

    db_call(_expire)
    _check("★ 过期 ticket 消费失败", db_call(
        lambda db: store.consume(db, ticket=expired, expected_session_id=8)) is None)

    print("\n[5] 会话绑定：错配失败，且不消耗")
    bound = db_call(lambda db: store.issue(db, session_id=9, user_id=44))
    _check("★ 拿 A 会话的 ticket 连 B 会话 → None", db_call(
        lambda db: store.consume(db, ticket=bound, expected_session_id=10)) is None)
    _check("  └ 原会话仍可用（错配不消耗 ticket）", db_call(
        lambda db: store.consume(db, ticket=bound, expected_session_id=9)) == 44)

    print("\n[6] 空 / 不存在：fail-closed")
    _check("空串消费失败", db_call(
        lambda db: store.consume(db, ticket="", expected_session_id=1)) is None)
    _check("不存在的 ticket 消费失败", db_call(
        lambda db: store.consume(db, ticket="not-a-real-ticket", expected_session_id=1)) is None)

    print("\n[7] 撤销：单个 / 按用户")
    single = db_call(lambda db: store.issue(db, session_id=11, user_id=45))
    _check("撤销单个 ticket 返回 True", db_call(lambda db: store.revoke(db, ticket=single)) is True)
    _check("★ 撤销后不可消费", db_call(
        lambda db: store.consume(db, ticket=single, expected_session_id=11)) is None)
    _check("重复撤销返回 False（已不可再撤销）",
           db_call(lambda db: store.revoke(db, ticket=single)) is False)

    db_call(lambda db: store.issue(db, session_id=12, user_id=46))
    db_call(lambda db: store.issue(db, session_id=12, user_id=46))
    n = db_call(lambda db: store.revoke_for_user(db, user_id=46))
    _check("按用户批量撤销，撤销了 2 条", n == 2, str(n))

    print("\n[8] 清理：只删过期行")
    alive = db_call(lambda db: store.issue(db, session_id=13, user_id=47))
    dead = db_call(lambda db: store.issue(db, session_id=14, user_id=48))

    async def _expire_one(db):
        await db.execute(
            update(WsTicket)
            .where(WsTicket.ticket_hash == store.hash_ticket(dead))
            .values(expires_at=_dt(-10))
        )
        await db.commit()

    db_call(_expire_one)
    removed = db_call(lambda db: store.sweep_expired(db, force=True))
    _check("清理删掉了过期的行（≥1）", removed >= 1, str(removed))
    _check("★ 未过期的 ticket 仍然可用", db_call(
        lambda db: store.consume(db, ticket=alive, expected_session_id=13)) == 47)

    print("\n[9] 存储不可用：抛异常（不是返回 None）")
    _drop_ticket_table()
    try:
        db_call(lambda db: store.consume(db, ticket="whatever", expected_session_id=1))
        raised = ""
    except store.TicketStoreUnavailable as exc:
        raised = type(exc).__name__
    except Exception as exc:  # noqa: BLE001
        raised = f"WRONG:{type(exc).__name__}"
    _check("★★ 表缺失时抛 TicketStoreUnavailable（可区分于'凭证无效'）",
           raised == "TicketStoreUnavailable", raised)

    try:
        db_call(lambda db: store.issue(db, session_id=1, user_id=1))
        raised = ""
    except store.TicketStoreUnavailable as exc:
        raised = type(exc).__name__
    except Exception as exc:  # noqa: BLE001
        raised = f"WRONG:{type(exc).__name__}"
    _check("★★ 签发时同样抛 TicketStoreUnavailable（不返回废票）",
           raised == "TicketStoreUnavailable", raised)
    _recreate_ticket_table()


def _dt(delta_seconds: int):
    from datetime import datetime, timedelta

    return datetime.utcnow() + timedelta(seconds=delta_seconds)


def _row_of_used(ticket: str):
    async def _run(db):
        row = (
            await db.execute(
                select(WsTicket).where(WsTicket.ticket_hash == store.hash_ticket(ticket))
            )
        ).scalar_one_or_none()
        return row.used_at if row is not None else None

    return _run


# ============================================================
# [10]-[17] HTTP 端点 + 实时通道
# ============================================================
def _endpoint_checks(client: TestClient, owner_sid: int, other_sid: int) -> str:
    print("\n[10] 端点：鉴权 / 归属 / 正常签发")
    r = client.post(f"/api/interview/{owner_sid}/ws-ticket")
    _check("无 Authorization → 401", r.status_code == 401, str(r.status_code))

    r = client.post(
        f"/api/interview/{other_sid}/ws-ticket",
        headers={"Authorization": f"Bearer {OWNER_TOKEN}"},
    )
    _check("★ 他人会话 → 404（不是 403，避免泄露存在性）", r.status_code == 404, str(r.status_code))

    r = client.post(
        f"/api/interview/{owner_sid}/ws-ticket",
        headers={"Authorization": f"Bearer {OWNER_TOKEN}"},
    )
    _check("本人会话 → 200", r.status_code == 200, str(r.status_code))
    body = r.json()
    ticket = body.get("ticket")
    _check("响应含 ticket 字符串", isinstance(ticket, str) and len(ticket) >= 32, str(body))
    _check("响应含 expires_in（秒）", body.get("expires_in") == ws._TICKET_TTL_SECONDS, str(body))
    _check("★ 响应里没有 user_id 之类的内部字段（只给 ticket + 有效期）",
           set(body) == {"ticket", "expires_in"}, str(sorted(body)))
    return ticket


def _probe_token_auth(value: str) -> bool:
    """把 ``INTERVIEW_WS_TOKEN_AUTH`` 临时设成 ``value`` 并读回（读完恢复）。

    ★ 为什么需要它：开关是**实时读环境变量**的（不在 import 期缓存），
    所以要逐值探测只能临时改 ``os.environ``；用 try/finally 保证不污染后续用例
    ——否则「开关状态泄漏」会让后面依赖 ``?token=`` 的用例集体 4401。
    """
    prev = os.environ.get("INTERVIEW_WS_TOKEN_AUTH")
    try:
        os.environ["INTERVIEW_WS_TOKEN_AUTH"] = value
        return ws.token_auth_enabled()
    finally:
        if prev is None:
            os.environ.pop("INTERVIEW_WS_TOKEN_AUTH", None)
        else:
            os.environ["INTERVIEW_WS_TOKEN_AUTH"] = prev


def _ws_checks(client: TestClient, ticket: str, owner_sid: int, other_sid: int) -> None:
    print("\n[12] 实时通道：用 ticket 连接成功（先收到 hello）")
    with client.websocket_connect(f"/api/interview/{owner_sid}/ws?ticket={ticket}") as sock:
        hello = sock.receive_json()
        _check("连接成功并收到 hello", hello.get("type") == "hello", str(hello)[:160])
        _check("hello 带回 session_id", hello.get("session_id") == owner_sid, str(hello)[:160])

    print("\n[13] 实时通道：同一 ticket 第二次连接 → 4401")
    code = None
    with client.websocket_connect(f"/api/interview/{owner_sid}/ws?ticket={ticket}") as sock:
        first = sock.receive_json()
        _check("★ 被拒时先收到 error 事件（不是无声断开）", first.get("type") == "error", str(first)[:160])
        try:
            sock.receive_json()
        except WebSocketDisconnect as exc:
            code = exc.code
    _check("★ 随后以 4401 关闭（ticket 不可重放）", code == ws.WS_CLOSE_UNAUTHORIZED, str(code))

    print("\n[14] 实时通道：既无 ticket 也无 token → 4401")
    code = None
    with client.websocket_connect(f"/api/interview/{owner_sid}/ws") as sock:
        first = sock.receive_json()
        _check("先收到 error 事件", first.get("type") == "error", str(first)[:160])
        try:
            sock.receive_json()
        except WebSocketDisconnect as exc:
            code = exc.code
    _check("以 4401 关闭", code == ws.WS_CLOSE_UNAUTHORIZED, str(code))

    print("\n[15] 实时通道：拿他人 token 连他人会话 → 4404（归属校验）")
    code = None
    with client.websocket_connect(f"/api/interview/{other_sid}/ws?token={OWNER_TOKEN}") as sock:
        first = sock.receive_json()
        _check("先收到 error 事件", first.get("type") == "error", str(first)[:160])
        try:
            sock.receive_json()
        except WebSocketDisconnect as exc:
            code = exc.code
    _check("★ 以 4404 关闭（会话不存在或不属于当前用户）", code == ws.WS_CLOSE_NOT_FOUND, str(code))

    print("\n[15b] Origin 校验：白名单放行 / 跨站拒绝")
    with client.websocket_connect(
        f"/api/interview/{owner_sid}/ws?token={OWNER_TOKEN}",
        headers={"Origin": "http://localhost:5173"},
    ) as sock:
        hello = sock.receive_json()
        _check("白名单 Origin → 握手成功", hello.get("type") == "hello", str(hello)[:160])

    code = None
    with client.websocket_connect(
        f"/api/interview/{owner_sid}/ws?token={OWNER_TOKEN}",
        headers={"Origin": "https://evil.example"},
    ) as sock:
        first = sock.receive_json()
        _check("★ 跨站 Origin → 先收到 error", first.get("type") == "error", str(first)[:160])
        try:
            sock.receive_json()
        except WebSocketDisconnect as exc:
            code = exc.code
    _check("★ 跨站 Origin → 以 4403 关闭", code == ws.WS_CLOSE_FORBIDDEN, str(code))

    print("\n[15c] 兼容路径开关：INTERVIEW_WS_TOKEN_AUTH（默认开 / 可显式关）")
    # ★ 这一段是「不能为了兼容默默保留不安全 fallback」的取证：
    #   默认（未配置）行为**一点不变**（[15]/[15b] 就是证据，它们用的正是 ?token=），
    #   但运维可以显式关掉，且关掉后 ?token= **不再被当作登录**。
    _prev = os.environ.get("INTERVIEW_WS_TOKEN_AUTH")
    try:
        os.environ["INTERVIEW_WS_TOKEN_AUTH"] = ""
        _check("★ 未配置 / 空串 ⇒ 默认**开启**（= 改造前行为，不改变任何既有断言）",
               ws.token_auth_enabled() is True, repr(os.environ.get("INTERVIEW_WS_TOKEN_AUTH")))

        os.environ["INTERVIEW_WS_TOKEN_AUTH"] = "1"
        _check("显式 1 / true / on ⇒ 开启",
               ws.token_auth_enabled() is True
               and all(_probe_token_auth(v) for v in ("1", "true", "TRUE", "yes", "on")))

        os.environ["INTERVIEW_WS_TOKEN_AUTH"] = "0"
        _check("★ 显式 0 / false / no / off ⇒ 关闭",
               ws.token_auth_enabled() is False
               and not any(_probe_token_auth(v) for v in ("0", "false", "FALSE", "no", "off")))

        _check("★ 非法值**不静默**：按默认（开启）处理，不是「关掉了但没生效」",
               _probe_token_auth("maybe") is True)

        # 关闭后用 ?token= 建连 ⇒ 4401（不是 hello，也不是 4404）
        os.environ["INTERVIEW_WS_TOKEN_AUTH"] = "0"
        code = None
        with client.websocket_connect(
            f"/api/interview/{owner_sid}/ws?token={OWNER_TOKEN}"
        ) as sock:
            first = sock.receive_json()
            _check("★ 关闭后 ?token= 先收到 error（不是 hello）",
                   first.get("type") == "error", str(first)[:160])
            try:
                sock.receive_json()
            except WebSocketDisconnect as exc:
                code = exc.code
        _check("★★ 关闭后 ?token= → 4401（长期凭证路径真的被切断了）",
               code == ws.WS_CLOSE_UNAUTHORIZED, str(code))

        # 同一时刻 ticket 路径不受影响（两条路是**并列**的，不是主备）
        fresh = db_call(lambda db: store.issue(db, session_id=owner_sid, user_id=_owner_id[0]))
        with client.websocket_connect(f"/api/interview/{owner_sid}/ws?ticket={fresh}") as sock:
            hello = sock.receive_json()
            _check("★ 关闭 ?token= 不影响 ticket 路径（并列，不是「token 兜底 ticket」）",
                   hello.get("type") == "hello", str(hello)[:160])
    finally:
        if _prev is None:
            os.environ.pop("INTERVIEW_WS_TOKEN_AUTH", None)
        else:
            os.environ["INTERVIEW_WS_TOKEN_AUTH"] = _prev

    _check("恢复默认后 ?token= 又能用（开关是**实时读**，不是 import 期缓存）",
           ws.token_auth_enabled() is True)

    print("\n[16] 实时通道：存储不可用 → 1011（安全失败，不降级）")
    _drop_ticket_table()
    try:
        code = None
        with client.websocket_connect(f"/api/interview/{owner_sid}/ws?ticket=whatever") as sock:
            first = sock.receive_json()
            _check("★ 先收到 error(service_error)，而不是被当成未登录",
                   first.get("type") == "error" and first.get("code") == ws.ERROR_SERVICE,
                   str(first)[:160])
            try:
                sock.receive_json()
            except WebSocketDisconnect as exc:
                code = exc.code
        _check("★★ 以 1011 关闭（标准码 ⇒ 前端会重连重试，而不是当成登录过期）",
               code == ws.WS_CLOSE_UNAVAILABLE, str(code))
        _check("★ 1011 不是 4xxx（不破坏「4xxx = 应用自定义」约定）",
               1011 == ws.WS_CLOSE_UNAVAILABLE and code not in
               (ws.WS_CLOSE_UNAUTHORIZED, ws.WS_CLOSE_FORBIDDEN, ws.WS_CLOSE_NOT_FOUND,
                ws.WS_CLOSE_IDLE, ws.WS_CLOSE_BUSY))

        print("\n[11] 端点：存储不可用 → 503（不发废票）")
        r = client.post(
            f"/api/interview/{owner_sid}/ws-ticket",
            headers={"Authorization": f"Bearer {OWNER_TOKEN}"},
        )
        _check("★★ 503（不是 500，也不是 200 + 废票）", r.status_code == 503, str(r.status_code))
        _check("  └ 响应体里没有 ticket", "ticket" not in r.text, r.text[:120])
    finally:
        _recreate_ticket_table()

    print("\n[17] 登出：作废该用户未使用的 ticket")
    pending = db_call(lambda db: store.issue(db, session_id=owner_sid, user_id=_owner_id[0]))
    r = client.post("/api/auth/logout", headers={"Authorization": f"Bearer {OWNER_TOKEN}"})
    _check("登出成功", r.status_code == 200, str(r.status_code))
    _check("★★ 登出后该 ticket 不可再用（登出 ⇒ 立即失效）",
           db_call(lambda db: store.consume(db, ticket=pending, expected_session_id=owner_sid))
           is None)
    _unrevoke_token(OWNER_TOKEN)


def _source_guards() -> None:
    print("\n[18] 源码守卫：进程内字典已移除 / 存储只在 service 层")
    text_api = (_BACKEND / "api" / "interview.py").read_text(encoding="utf-8")
    text_ws = (_BACKEND / "api" / "interview_ws.py").read_text(encoding="utf-8")
    text_store = (_BACKEND / "services" / "ws_ticket_store.py").read_text(encoding="utf-8")

    _check("★ 文字面试 HTTP 契约文件不含 ticket 逻辑", "ws-ticket" not in text_api)
    _check("  └ 也不 import 实时通道", "interview_ws" not in text_api)
    _check("ticket 的签发与校验入口仍在实时通道层",
           "def create_ws_ticket" in text_ws and "def verify_ws_ticket" in text_ws)

    # ★ 进程内字典必须彻底消失：有它就意味着"多实例下 A 签发 B 校验"会随机失败
    # ★★ 判据必须用 **AST 的标识符**，不能用子串：
    #    ``"ws_ticket_store"`` 里就含 ``"_ticket_store"``，子串判断会恒假失败
    #    （本项目已复发多次的「裸词被同族 token 误伤」）。
    tree_ws = ast.parse(text_ws)
    module_names = set()
    for node in tree_ws.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for t in targets:
                if isinstance(t, ast.Name):
                    module_names.add(t.id)
    forbidden_names = {"_ticket_store", "_ticket_lock", "_MAX_TICKETS", "_sweep_tickets"}
    _check("★★ api 层不再有进程内 ticket 字典（AST 取模块级标识符）",
           not (module_names & forbidden_names),
           str(sorted(module_names & forbidden_names)))
    imported = set()
    for node in ast.walk(tree_ws):
        if isinstance(node, ast.Import):
            imported.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    _check("★ 实时通道不再 import threading（没有进程内锁可加）",
           "threading" not in imported, str(sorted(imported)))
    _check("★ 实时通道委托给 service 层存储",
           "ws_ticket_store" in text_ws)

    # 存储模块：必须用条件更新（rowcount 是原子性判据），不能"先查再写"
    _check("★ 消费用 UPDATE … WHERE used_at IS NULL（rowcount 判成败）",
           "used_at.is_(None)" in text_store and "rowcount" in text_store)
    _check("★ 消费后立刻 commit（否则行锁会握到 WS 连接结束）",
           text_store.count("await db.commit()") >= 3)

    # AST：存储模块不得 import 编排层（守住「可选层」性质）
    store_imports = set()
    for node in ast.walk(ast.parse(text_store)):
        if isinstance(node, ast.Import):
            store_imports.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            store_imports.add(node.module)
            store_imports.update(f"{node.module}.{a.name}" for a in node.names)
    _check("★ 存储模块不 import 编排层（不把可选层变成必经路径）",
           "services.interview_orchestrator" not in store_imports,
           str(sorted(store_imports)))

    # 端点确实挂在实时通道 router 上
    paths = []
    for node in ast.walk(ast.parse(text_ws)):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for dec in node.decorator_list:
                if (
                    isinstance(dec, ast.Call)
                    and isinstance(dec.func, ast.Attribute)
                    and dec.func.attr in ("get", "post", "put", "delete", "websocket")
                    and dec.args
                    and isinstance(dec.args[0], ast.Constant)
                ):
                    paths.append(dec.args[0].value)
    _check("实时通道 router 上确实注册了 ws-ticket 端点",
           any("ws-ticket" in str(p) for p in paths), str(paths))


_owner_id = [None]


def run() -> bool:
    # ★ 先建库：存储层用例直接读写真实表，不能靠 mock
    owner_id, other_id, owner_sid, other_sid = _prepare_db()
    _owner_id[0] = owner_id

    _store_checks()

    with TestClient(main.app) as client:
        ticket = _endpoint_checks(client, owner_sid, other_sid)
        if ticket:
            _ws_checks(client, ticket, owner_sid, other_sid)

    _source_guards()

    # 释放 app 引擎的连接：Windows 上 SQLite 文件被句柄占着时删不掉临时目录
    # （与 tests/test_interview_ws.py 同一处理）。
    try:
        import database

        asyncio.run(database.engine.dispose())
    except Exception:  # noqa: BLE001 - 释放失败只影响清理，不影响断言
        pass

    try:
        shutil.rmtree(_TMP_DIR, ignore_errors=False)
        _check("临时 SQLite 目录已清理", not pathlib.Path(_TMP_DIR).exists())
    except Exception as exc:  # noqa: BLE001
        print(f"  [WARN] 临时目录未能删除（不影响结果）：{_TMP_DIR} -> {exc}")

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
