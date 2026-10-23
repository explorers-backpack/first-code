# -*- coding: utf-8 -*-
"""管理员踢下线 / 会话撤销 → **实时通道主动断开** · 自检

无需 pytest，直接运行：
    python backend/tests/test_ws_revocation.py

★ 本套件要回答的三个问题
------------------------
1. **撤销对"已建立"的长连接到底多久生效？**（改造前实测：ticket 路径 >20s 内
   完全不生效、上界是空闲超时 300s；token 路径 61.7s。见
   ``tests/_probe_revoke_latency.py`` 与报告 §10。）
2. **跨实例怎么办？** 撤销发生在 A 实例，连接长在 B 实例。
3. **通知丢了 / 存储挂了 / 实例重启** 会不会漏踢或误踢？

跑在**临时文件 SQLite** 上，因此「跨实例」用**独立的 engine/连接**模拟
（每次 ``db_call`` 一条新物理连接）。真实 MySQL 上的并发语义由
``tests/ws_ticket_mysql_concurrency_run.py`` 与 CI 里的 MySQL 服务承担；
本套件证明的是**传播与断开逻辑**，不是数据库的并发能力。

覆盖：
[1]  存储：record / latest_id / fetch_since / prune / matches（纯函数 + 真表）
[2]  ticket 绑定 auth_session_id：签发落库、消费带回、复核口径
[3]  同实例：管理员踢下线 → 已建立连接被**主动断开**（实测延迟）
[4]  跨实例：撤销由"另一个进程"写入 → 本实例轮询器仍能读到并断开
[5]  撤销后高风险操作（finish）在执行前被拦（不只依赖 60s 复核）
[6]  不重放：轮询器启动**之前**的历史撤销不会误杀新连接
[7]  被踢后重连：签发 ticket 401、`?token=` 4401
[8]  存储故障：撤销表不可用 ⇒ 轮询器不崩、撤销主流程照常
[9]  接线守卫：轮询器/回调/连接表的位置与依赖方向
[10] 延迟汇总：实测值 vs 设计目标

``INTERVIEW_WS_REVOCATION_WATCH`` 在 ``regression_env`` 里被钉成 ``"0"``
（防后台任务污染其他套件）；**本套件显式打开**，因此两个状态都有覆盖。
"""

import asyncio
import ast
import os
import pathlib
import shutil
import sys
import tempfile
import time

_TMP_DIR = tempfile.mkdtemp(prefix="career_ai_revoke_test_")
_DB_PATH = pathlib.Path(_TMP_DIR) / "revoke_test.db"

os.environ.setdefault("DATABASE_URL", f"sqlite+aiosqlite:///{_DB_PATH.as_posix()}")

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))
sys.path.insert(0, str(_BACKEND / "tests"))

import regression_env  # noqa: E402,F401  （先钉住，再显式打开本套件需要的开关）

# ★ 必须在 import main 之前设置：lifespan 在 TestClient 进入时读环境变量。
#   0.2s 的周期只为让测试跑得快；生产默认 2.0s（见 ws_revocation_watcher）。
os.environ["INTERVIEW_WS_REVOCATION_WATCH"] = "1"
os.environ["INTERVIEW_WS_REVOCATION_WATCH_INTERVAL"] = "0.2"

from datetime import datetime, timedelta  # noqa: E402

from sqlalchemy import select, text, update  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.pool import NullPool  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402
from starlette.websockets import WebSocketDisconnect  # noqa: E402

import main  # noqa: E402
import models  # noqa: E402,F401
from api import interview_ws as ws  # noqa: E402
from database import Base  # noqa: E402
from models import InterviewSession, User, UserSession, WsRevocation  # noqa: E402
from services import ws_revocation  # noqa: E402
from services import ws_revocation_watcher as watcher  # noqa: E402
from services import ws_ticket_store as store  # noqa: E402

DB_URL = f"sqlite+aiosqlite:///{_DB_PATH.as_posix()}"

USER_TOKEN = "revoke-test-user-token"
ADMIN_TOKEN = "revoke-test-admin-token"
OTHER_TOKEN = "revoke-test-other-token"

_PASSED = 0
_FAILED = 0

#: 记录实测延迟，最后统一打印（**区分测试结果与设计目标**）
_MEASURED = {}


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
# 数据库辅助（每次一条**独立物理连接** ⇒ 等价于"另一个进程"）
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


_IDS = {}


def _prepare_db():
    """建表 + 种子：普通用户 / 管理员 / 另一个用户，各一条登录会话 + 面试会话。"""

    async def _run():
        engine = create_async_engine(DB_URL, poolclass=NullPool)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as db:
            u = User(username="rv_user", email="rv_user@example.com", password_hash="x", role="user")
            admin = User(username="rv_admin", email="rv_admin@example.com", password_hash="x", role="admin")
            other = User(username="rv_other", email="rv_other@example.com", password_hash="x", role="user")
            db.add_all([u, admin, other])
            await db.commit()
            for row in (u, admin, other):
                await db.refresh(row)

            now = datetime.utcnow()
            us = UserSession(
                user_id=u.id, token=USER_TOKEN, email=u.email, role="user",
                created_at=now, expires_at=now + timedelta(days=1),
            )
            ad = UserSession(
                user_id=admin.id, token=ADMIN_TOKEN, email=admin.email, role="admin",
                created_at=now, expires_at=now + timedelta(days=1),
            )
            ot = UserSession(
                user_id=other.id, token=OTHER_TOKEN, email=other.email, role="user",
                created_at=now, expires_at=now + timedelta(days=1),
            )
            db.add_all([us, ad, ot])
            s = InterviewSession(
                user_id=u.id, interview_type="comprehensive", difficulty="mid",
                duration=30, interview_mode="avatar", status="created",
                total_questions=5, current_question_no=0,
            )
            db.add(s)
            await db.commit()
            await db.refresh(us)
            await db.refresh(s)
            ids = {
                "user_id": u.id,
                "admin_id": admin.id,
                "other_id": other.id,
                "auth_session_id": us.id,
                "other_auth_session_id": ot.id,
                "interview_session_id": s.id,
            }
        await engine.dispose()
        return ids

    return asyncio.run(_run())


def _revoke_auth_session(auth_session_id: int) -> None:
    """只把 ``revoked_at`` 置位（**不写广播**）——用来单独验证周期性复核路径。"""

    async def _run(db):
        await db.execute(
            update(UserSession)
            .where(UserSession.id == int(auth_session_id))
            .values(revoked_at=datetime.utcnow())
        )
        await db.commit()

    db_call(_run)


def _unrevoke_all() -> None:
    async def _run(db):
        await db.execute(update(UserSession).values(revoked_at=None))
        await db.commit()

    db_call(_run)


def _drop_revocation_table() -> None:
    async def _run(db):
        await db.execute(text("DROP TABLE ws_revocation"))
        await db.commit()

    db_call(_run)


def _recreate_revocation_table() -> None:
    async def _run():
        engine = create_async_engine(DB_URL, poolclass=NullPool)
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        finally:
            await engine.dispose()

    asyncio.run(_run())


def _wait_disconnect(sock, budget: float = 8.0, step: float = 0.05):
    """等到连接被断开或收到 error；返回 ``(耗时秒, 观测到的码)``。

    码可能是：
    - ``4401``：直接观测到 WebSocket 关闭码（最理想）；
    - ``"unauthorized"``：先收到 error 事件（服务端**先发事件再关**）；
    - ``None``：预算内没断开（**只有这种情况算失败**）。
    """
    t0 = time.monotonic()
    while time.monotonic() - t0 < budget:
        time.sleep(step)
        try:
            sock.send_json({"type": "ping"})
            msg = sock.receive_json()
        except WebSocketDisconnect as exc:
            return time.monotonic() - t0, exc.code
        except Exception:  # noqa: BLE001 - 已断开时 send 可能抛别的异常
            return time.monotonic() - t0, "closed"
        if isinstance(msg, dict) and msg.get("type") == "error":
            return time.monotonic() - t0, msg.get("code")
    return None, None


def _is_revoked_signal(code) -> bool:
    """关闭信号是否等价于「因撤销被踢」。"""
    return code in (ws.WS_CLOSE_UNAUTHORIZED, ws.ERROR_UNAUTHORIZED)


def _still_alive(sock) -> bool:
    """发一条 ping，能收到 pong 就算还活着。"""
    try:
        sock.send_json({"type": "ping"})
        msg = sock.receive_json()
        return isinstance(msg, dict) and msg.get("type") == "pong"
    except Exception:  # noqa: BLE001
        return False


#: 连接建立后服务端会**主动推**一次快照（hello → state → question → avatar）。
#: 后续断言前必须把它排空，否则读到的永远是队列里的快照事件而不是"我刚发的那条的回应"。
_SNAPSHOT_TAIL = "avatar"


def _drain_snapshot(sock, limit: int = 8) -> list:
    """读到快照末尾（``avatar``）为止，返回读到的事件类型序列。"""
    seen = []
    for _ in range(limit):
        msg = sock.receive_json()
        if not isinstance(msg, dict):
            break
        seen.append(msg.get("type"))
        if msg.get("type") == _SNAPSHOT_TAIL:
            break
    return seen


def _imported_modules(path) -> set:
    """用 **AST** 取一个模块实际 import 的名字（含 ``from x import y`` 的 ``x.y``）。

    ★ 为什么不用子串：本项目的守卫**多次**栽在「目标文件自己的注释/文档里
    写了那个词」上（``inspect.getsource`` 连注释一起算）。判 import 关系必须看
    AST 里的 ``Import`` / ``ImportFrom`` 节点，而不是在源码文本里找词。
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if base:
                names.add(base)
            for alias in node.names:
                names.add(f"{base}.{alias.name}" if base else alias.name)
    return names


# ============================================================
# [1] 存储层
# ============================================================
def _store_checks() -> None:
    print("\n[1] 撤销广播存储：追加 / 水位线 / 批量读 / 清理 / 匹配")

    base = db_call(lambda db: ws_revocation.latest_id(db))
    _check("空表 latest_id = 0（水位线起点）", base == 0, str(base))

    id1 = db_call(lambda db: ws_revocation.record(
        db, user_id=101, auth_session_id=None, reason=ws_revocation.REASON_ADMIN_KICK))
    id2 = db_call(lambda db: ws_revocation.record(
        db, user_id=102, auth_session_id=7, reason=ws_revocation.REASON_LOGOUT))
    _check("★ 追加返回单调自增 id", isinstance(id1, int) and id2 == id1 + 1, f"{id1},{id2}")
    _check("latest_id 跟上", db_call(lambda db: ws_revocation.latest_id(db)) == id2)

    rows = db_call(lambda db: ws_revocation.fetch_since(db, id1))
    _check("★ fetch_since 是**开区间**（id > after）⇒ 不会重复投递",
           len(rows) == 1 and rows[0]["id"] == id2, str(rows))
    _check("  └ 行内容完整（user_id / auth_session_id / reason）",
           rows[0]["user_id"] == 102 and rows[0]["auth_session_id"] == 7
           and rows[0]["reason"] == "logout", str(rows[0]))
    _check("  └ 返回的是普通 dict（不是会失效的 ORM Row）",
           isinstance(rows[0], dict), type(rows[0]).__name__)

    all_rows = db_call(lambda db: ws_revocation.fetch_since(db, 0))
    _check("从 0 读能拿到全部（重启补偿语义）", len(all_rows) == 2, str(len(all_rows)))

    # matches：纯函数，边界就是安全边界
    _check("★ 按用户撤销（auth_session_id=None）命中该用户任意连接",
           ws_revocation.matches({"user_id": 1, "auth_session_id": None},
                                 user_id=1, auth_session_id=99) is True)
    _check("★ 按会话撤销只命中**同一条**登录会话",
           ws_revocation.matches({"user_id": 1, "auth_session_id": 5},
                                 user_id=1, auth_session_id=5) is True
           and ws_revocation.matches({"user_id": 1, "auth_session_id": 5},
                                     user_id=1, auth_session_id=6) is False)
    _check("★ 用户不同一律不命中（不误伤他人）",
           ws_revocation.matches({"user_id": 1, "auth_session_id": None},
                                 user_id=2, auth_session_id=5) is False)
    _check("★ 连接侧无 auth_session_id 时，「按会话撤销」不误伤它",
           ws_revocation.matches({"user_id": 1, "auth_session_id": 5},
                                 user_id=1, auth_session_id=None) is False)

    # 清理：只删过老的行
    async def _age_one(db):
        await db.execute(
            update(WsRevocation).where(WsRevocation.id == id1)
            .values(created_at=datetime.utcnow() - timedelta(hours=5))
        )
        await db.commit()

    db_call(_age_one)
    removed = db_call(lambda db: ws_revocation.prune(db, keep_seconds=3600))
    _check("★ prune 只删过老的行（保留窗口内的）", removed == 1, str(removed))
    _check("  └ 保留窗口内的行还在",
           db_call(lambda db: ws_revocation.latest_id(db)) == id2)


# ============================================================
# [2] ticket 绑定登录会话
# ============================================================
def _ticket_binding_checks() -> None:
    print("\n[2] ticket 绑定 auth_session_id（撤销能生效的前提）")
    sid = _IDS["interview_session_id"]
    uid = _IDS["user_id"]
    aid = _IDS["auth_session_id"]

    bound = db_call(lambda db: store.issue(
        db, session_id=sid, user_id=uid, auth_session_id=aid))
    got = db_call(lambda db: store.consume_ticket(
        db, ticket=bound, expected_session_id=sid))
    _check("★ 消费带回 (user_id, auth_session_id) 两段绑定",
           got is not None and got.user_id == uid and got.auth_session_id == aid,
           str(got))

    legacy = db_call(lambda db: store.issue(db, session_id=sid, user_id=uid))
    got2 = db_call(lambda db: store.consume(db, ticket=legacy, expected_session_id=sid))
    _check("★ 不传 auth_session_id 时，兼容入口 consume 仍返回 user_id（旧契约不变）",
           got2 == uid, str(got2))

    legacy2 = db_call(lambda db: store.issue(db, session_id=sid, user_id=uid))
    got3 = db_call(lambda db: store.consume_ticket(
        db, ticket=legacy2, expected_session_id=sid))
    _check("  └ 历史 ticket 的 auth_session_id 为 None（退化为弱复核）",
           got3 is not None and got3.auth_session_id is None, str(got3))

    # 库里确实存了这一列
    async def _read(db):
        from models import WsTicket

        return (await db.execute(
            select(WsTicket.auth_session_id).order_by(WsTicket.id.asc()).limit(1)
        )).scalar()

    _check("  └ 该列真的落库（不是只在内存里）", db_call(_read) == aid)


# ============================================================
# [3]-[8] 端到端（同一个 TestClient 内）
# ============================================================
def _end_to_end(client: TestClient) -> None:
    sid = _IDS["interview_session_id"]
    uid = _IDS["user_id"]
    aid = _IDS["auth_session_id"]

    # ---- [3] 同实例：踢下线 → 主动断开 ----
    print("\n[3] 同实例：管理员踢下线 → 已建立连接被主动断开（实测延迟）")
    ticket = db_call(lambda db: store.issue(
        db, session_id=sid, user_id=uid, auth_session_id=aid))
    with client.websocket_connect(f"/api/interview/{sid}/ws?ticket={ticket}") as sock:
        hello = sock.receive_json()
        _check("连接建立（收到 hello）", hello.get("type") == "hello", str(hello)[:120])
        _check("  └ 连接已登记到本进程的连接表", ws.active_conn_count() >= 1,
               str(ws.active_conn_count()))

        t0 = time.monotonic()
        r = client.post(
            f"/api/admin/users/{uid}/kick",
            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
        )
        _check("★ POST /api/admin/users/{id}/kick → 200", r.status_code == 200, str(r.status_code))
        body = r.json()
        _check("  └ 撤销了 1 条登录会话", body.get("revoked_sessions") == 1, str(body))
        _check("  └ 广播已写入（broadcast=true）", body.get("broadcast") is True, str(body))

        waited, code = _wait_disconnect(sock)
        elapsed = (time.monotonic() - t0) if waited is None else waited
        _MEASURED["same_instance"] = elapsed
        _check("★★ 连接被**主动断开**（不是等到空闲超时）", code is not None,
               f"code={code} waited={elapsed:.2f}s")
        _check("★★ 断开信号 = 4401 / unauthorized（前端对 4401 不重连 ⇒ 踢了就回不来）",
               _is_revoked_signal(code), str(code))
        # 设计目标：轮询周期（0.2s 测试 / 2.0s 生产）+ 一次查询 + 调度抖动
        _check("★ 生效延迟 ≤ 2s（测试周期 0.2s；生产目标 ≤ 3s）",
               elapsed is not None and elapsed <= 2.0, f"{elapsed}")

    # 断开后连接表应被 finally 清掉（给一点时间让服务端 unwind）
    for _ in range(20):
        if ws.active_conn_count() == 0:
            break
        time.sleep(0.05)
    _check("  └ 断开后连接表已清理", ws.active_conn_count() == 0, str(ws.active_conn_count()))

    # ---- [4] 跨实例：撤销由"另一个进程"写入 ----
    print("\n[4] 跨实例：撤销写入方 ≠ 连接所在实例")
    _unrevoke_all()
    ticket2 = db_call(lambda db: store.issue(
        db, session_id=sid, user_id=uid, auth_session_id=aid))
    with client.websocket_connect(f"/api/interview/{sid}/ws?ticket={ticket2}") as sock:
        sock.receive_json()
        # ★ 关键：用**独立 engine/连接**写入（等价于另一个 worker 收到管理员请求）
        db_call(lambda db: ws_revocation.record(
            db, user_id=uid, auth_session_id=None,
            reason=ws_revocation.REASON_ADMIN_KICK))
        t0 = time.monotonic()
        waited, code = _wait_disconnect(sock)
        elapsed = (time.monotonic() - t0) if waited is None else waited
        _MEASURED["cross_instance"] = elapsed
        _check("★★ 另一实例写入的撤销，本实例仍能断开（共享存储 + 轮询器）",
               _is_revoked_signal(code), str(code))
        _check("  └ 延迟同样在秒级", elapsed is not None and elapsed <= 2.0, f"{elapsed}")

    # ---- [5] 撤销后高风险操作在执行前被拦 ----
    print("\n[5] 撤销后高风险操作（finish）：执行前拦截，不靠 60s 复核")
    _unrevoke_all()
    ticket3 = db_call(lambda db: store.issue(
        db, session_id=sid, user_id=uid, auth_session_id=aid))
    with client.websocket_connect(f"/api/interview/{sid}/ws?ticket={ticket3}") as sock:
        _drain_snapshot(sock)
        # 只置 revoked_at，**不写广播** ⇒ 轮询器不会来断它
        # ⇒ 这时唯一能拦住 finish 的就是「执行前复核」
        _revoke_auth_session(aid)
        time.sleep(0.4)  # 让轮询器有机会跑，但它读不到新行
        sock.send_json({"type": "finish"})
        first = sock.receive_json()
        _check("★★ finish 被拒（error/unauthorized），而不是照常出报告",
               first.get("type") == "error" and first.get("code") == ws.ERROR_UNAUTHORIZED,
               str(first)[:160])
        try:
            sock.receive_json()
        except WebSocketDisconnect as exc:
            _check("  └ 随后以 4401 关闭", exc.code == ws.WS_CLOSE_UNAUTHORIZED, str(exc.code))

    # ---- [6] 不重放：启动前的历史撤销不误杀 ----
    print("\n[6] 不重放：轮询器启动前的历史撤销不会误杀新连接")
    _unrevoke_all()
    # 历史行在 TestClient 进入**之前**就已写入（见 run()），这里只验证新连接不被踢
    ticket4 = db_call(lambda db: store.issue(
        db, session_id=sid, user_id=uid, auth_session_id=aid))
    with client.websocket_connect(f"/api/interview/{sid}/ws?ticket={ticket4}") as sock:
        _drain_snapshot(sock)
        time.sleep(0.6)  # 3 个轮询周期
        _check("★ 新连接**没有**被历史撤销误杀（水位线从 max(id) 起步）",
               _still_alive(sock))

    # ---- [7] 被踢后重连 ----
    print("\n[7] 被踢后重连：签发 401 / 兼容路径 4401")
    _unrevoke_all()
    r = client.post(
        f"/api/admin/users/{uid}/kick",
        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
    )
    _check("再次踢下线成功", r.status_code == 200, str(r.status_code))
    r2 = client.post(
        f"/api/interview/{sid}/ws-ticket",
        headers={"Authorization": f"Bearer {USER_TOKEN}"},
    )
    _check("★★ 被踢后签发 ticket → 401（会话已撤销，拿不到新票）",
           r2.status_code == 401, str(r2.status_code))

    code = None
    with client.websocket_connect(f"/api/interview/{sid}/ws?token={USER_TOKEN}") as sock:
        first = sock.receive_json()
        try:
            sock.receive_json()
        except WebSocketDisconnect as exc:
            code = exc.code
    _check("★ 被踢后 ?token= 重连 → 4401", code == ws.WS_CLOSE_UNAUTHORIZED, str(code))
    _unrevoke_all()

    # ---- [8] 存储故障 ----
    print("\n[8] 存储故障：撤销表不可用")
    _drop_revocation_table()
    try:
        ok = True
        detail = ""
        try:
            db_call(lambda db: ws_revocation.record(db, user_id=uid, reason="x"))
            ok = False
            detail = "record 竟然成功了"
        except ws_revocation.RevocationStoreUnavailable as exc:
            detail = type(exc).__name__
        _check("★ 表缺失 ⇒ record 抛 RevocationStoreUnavailable（不是静默成功）", ok, detail)

        # ★ 轮询器**不崩**：它在 app 的事件循环里持续跑，遇到表缺失只记 WARNING。
        #   这里不直接调用 poll_once（那会在测试的另一个事件循环里复用 app 的
        #   engine，属于"测试脚手架问题"而非被测行为），而是观察真实后台任务。
        before = watcher.snapshot()
        time.sleep(0.8)
        after = watcher.snapshot()
        _check("★★ 轮询器遇到存储故障不崩（任务仍在跑，failures 递增）",
               after["running"] is True and after["failures"] > before["failures"],
               f"before={before} after={after}")

        # 撤销主流程不受影响：踢下线仍然成功（只是 broadcast=False）
        _unrevoke_all()
        r = client.post(
            f"/api/admin/users/{uid}/kick",
            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
        )
        _check("★ 广播不可用时，踢下线的**主效应**仍然生效（200）",
               r.status_code == 200 and r.json().get("revoked_sessions") == 1, str(r.text)[:160])
        _check("  └ 响应明确标注 broadcast=false（不假装踢干净了）",
               r.json().get("broadcast") is False, str(r.json()))
    finally:
        _recreate_revocation_table()
        _unrevoke_all()


async def _poll_in_loop() -> int:
    """（保留给将来在**独立进程**里跑轮询器的场景；当前套件不直接调用。）"""
    return await watcher.poll_once()


# ============================================================
# [9] 接线守卫
# ============================================================
def _wiring_guards() -> None:
    print("\n[9] 接线守卫：轮询器 / 回调 / 依赖方向")
    text_watcher = (_BACKEND / "services" / "ws_revocation_watcher.py").read_text(encoding="utf-8")
    text_revocation = (_BACKEND / "services" / "ws_revocation.py").read_text(encoding="utf-8")
    text_ws = (_BACKEND / "api" / "interview_ws.py").read_text(encoding="utf-8")
    text_main = (_BACKEND / "main.py").read_text(encoding="utf-8")

    # ★ 判 import 关系用 AST，不用子串：这两个文件的**文档里**就写着对方的名字
    #   （解释"为什么不反向依赖"），子串判断会恒假失败。
    watcher_imports = _imported_modules(_BACKEND / "services" / "ws_revocation_watcher.py")
    revocation_imports = _imported_modules(_BACKEND / "services" / "ws_revocation.py")
    ws_imports = _imported_modules(_BACKEND / "api" / "interview_ws.py")

    _check("★ 轮询器**不** import 实时通道（靠 register_handler 解耦）",
           not any("interview_ws" in m for m in watcher_imports),
           str(sorted(watcher_imports)))
    _check("★ 撤销存储**不** import 实时通道 / 编排层",
           not any(("interview_ws" in m) or ("orchestrator" in m) for m in revocation_imports),
           str(sorted(revocation_imports)))
    _check("★ 实时通道**不** import 轮询器（只 import 共享存储）",
           not any("ws_revocation_watcher" in m for m in ws_imports),
           str(sorted(m for m in ws_imports if "revocation" in m)))
    _check("  └ 但它确实 import 了共享存储（撤销匹配口径的唯一来源）",
           any(m == "services.ws_revocation" for m in ws_imports),
           str(sorted(m for m in ws_imports if "revocation" in m)))
    _check("★ 轮询器只用标准库 + database + 撤销存储（不碰业务 service）",
           all(
               m.split(".")[0]
               in {"__future__", "asyncio", "logging", "os", "typing", "database", "services"}
               for m in watcher_imports
           ),
           str(sorted(watcher_imports)))
    _check("  └ 且 services 下**只**用了 ws_revocation（不越界到业务层）",
           {m for m in watcher_imports if m.startswith("services.")}
           == {"services.ws_revocation"},
           str(sorted(m for m in watcher_imports if m.startswith("services"))))
    _check("★ lifespan 里注册回调并启停轮询器",
           "register_handler" in text_main and "ws_revocation_watcher.start" in text_main
           and "ws_revocation_watcher.stop" in text_main)
    _check("★ 连接表只在实时通道层（跨进程通知不靠它）",
           "_ACTIVE_CONNS" in text_ws)
    _check("★ 高风险操作（answer/finish）执行前复核",
           "ws_auth_revoked_on_high_risk" in text_ws)
    _check("★ 复核用 auth_session_id 查 user_session（不是只查 User 存在）",
           "UserSession.id == int(auth_session_id)" in text_ws)
    _check("★ 管理员踢下线端点存在且要求 admin 依赖",
           '"/api/admin/users/{user_id}/kick"' in text_main
           and "Depends(get_current_admin)" in text_main)
    _check("★ ws_ticket.auth_session_id 已登记到 schema_sync（既有表加列必须补）",
           "auth_session_id" in (_BACKEND / "utils" / "schema_sync.py").read_text(encoding="utf-8"))


# ============================================================
# 主流程
# ============================================================
def run() -> bool:
    print("=" * 68)
    print("后端 · 会话撤销 → 实时通道主动断开（踢下线）")
    print("=" * 68)

    _IDS.update(_prepare_db())
    _store_checks()
    _ticket_binding_checks()

    # ★ 历史撤销行必须在轮询器启动**之前**写入（验证"不重放"）。
    #   刻意用**同一个用户**：若水位线实现错了（从 0 起步），[3] 里新建的连接
    #   会被这条历史行误杀 —— 那样测出来才是真的失败。
    db_call(lambda db: ws_revocation.record(
        db, user_id=_IDS["user_id"], auth_session_id=None,
        reason=ws_revocation.REASON_ADMIN_KICK))

    ws.reset_active_conns()
    watcher.reset()

    with TestClient(main.app) as client:
        _check("★ 轮询器已在 lifespan 中启动", watcher.snapshot()["running"] is True,
               str(watcher.snapshot()))
        _end_to_end(client)

    snap = watcher.snapshot()
    _check("★ 轮询器确实跑过（polls > 0）", snap["polls"] > 0, str(snap))
    _check("★ 断开计数被记录（closed > 0）", snap["closed"] > 0, str(snap))

    _wiring_guards()

    # ---- [10] 延迟汇总 ----
    print("\n[10] 实测延迟 vs 设计目标")
    target_same = "≤ 2s（轮询周期 2.0s 生产 / 0.2s 测试 + 一次查询）"
    print(f"  同实例（管理员踢下线 → 连接断开）：{_MEASURED.get('same_instance'):.3f}s"
          if _MEASURED.get("same_instance") is not None else "  同实例：未测到")
    print(f"  跨实例（另一进程写入 → 本实例断开）：{_MEASURED.get('cross_instance'):.3f}s"
          if _MEASURED.get("cross_instance") is not None else "  跨实例：未测到")
    print(f"  设计目标：{target_same}")
    print("  改造前基线（tests/_probe_revoke_latency.py 实测）：")
    print("    · ticket 路径：>20s 预算内**完全不生效**（上界=空闲超时 300s，有心跳则永不失效）")
    print("    · token  路径：61.7s（60s 复核节流 + 依赖下一条消息）")
    _check("★ 两个方向都测到了真实数字（不是「未测到」）",
           _MEASURED.get("same_instance") is not None
           and _MEASURED.get("cross_instance") is not None,
           str(_MEASURED))

    # 释放引擎句柄（Windows 上 SQLite 文件被占着时删不掉临时目录）
    try:
        import database

        asyncio.run(database.engine.dispose())
    except Exception:  # noqa: BLE001
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
