# -*- coding: utf-8 -*-
"""一次性取证脚本（**不是回归套件**，无 test_ 前缀）：测量「撤销会话」对
**已建立**的 WS 连接的真实生效时间。

★ 改前 / 改后用**同一个脚本**测，方法完全一致：

- 改前（ticket 未绑定登录会话、无撤销广播轮询器）：
  ``ticket`` 路径 **>20s 预算内完全不生效**（上界=空闲超时 300s，前端有心跳 ⇒ 实际永不失效）；
  ``token`` 路径 **61.7s**（60s 复核节流 + 依赖下一条消息）。
- 改后（ticket 绑定 ``auth_session_id`` + 撤销广播轮询器）：
  ``ticket`` **0.53s**、``token`` **0.53s**。
  ★ 这两个数字被本脚本 0.5s 的轮询步长卡住，**真实延迟以
  ``tests/test_ws_revocation.py`` 的 50ms 步长为准：0.31–0.38s**
  （测试周期 0.2s；生产默认 2.0s ⇒ 目标 ≤ 3s）。

用法：python backend/tests/_probe_revoke_latency.py
      python backend/tests/_probe_revoke_latency.py --interval 2.0 --step 0.1
      （``--interval`` 就是 ``INTERVIEW_WS_REVOCATION_WATCH_INTERVAL``；
        默认 0.2 便于快速取值，**2.0 才是生产默认**）
"""
import argparse
import asyncio
import os
import pathlib
import sys
import tempfile
import time

_TMP = tempfile.mkdtemp(prefix="career_ai_probe_")
_DB = pathlib.Path(_TMP) / "probe.db"
os.environ.setdefault("DATABASE_URL", f"sqlite+aiosqlite:///{_DB.as_posix()}")

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))
sys.path.insert(0, str(_BACKEND / "tests"))

import regression_env  # noqa: E402,F401

# ★ 打开撤销广播轮询器（regression_env 默认把它钉成 "0"）：本脚本要测的正是
#   「撤销广播 + 主动断开」这条路径。
os.environ["INTERVIEW_WS_REVOCATION_WATCH"] = "1"

#: 测量步长（秒）：越小分辨率越高，但每步都要一次 ping/pong。
STEP = 0.5

#: 轮询周期（秒）：由命令行 ``--interval`` 覆盖，默认与套件一致（0.2）。
WATCH_INTERVAL = 0.2

from datetime import datetime  # noqa: E402

from sqlalchemy import select, update  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.pool import NullPool  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402
from starlette.websockets import WebSocketDisconnect  # noqa: E402

import main  # noqa: E402
import models  # noqa: E402,F401
from database import Base  # noqa: E402
from models import InterviewSession, User, UserSession  # noqa: E402
from services import ws_ticket_store as store  # noqa: E402

DB_URL = f"sqlite+aiosqlite:///{_DB.as_posix()}"
TOKEN = "probe-owner-token"
PROBE_USER_ID = [None]
PROBE_AUTH_SESSION_ID = [None]


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


def prepare():
    async def _run():
        engine = create_async_engine(DB_URL, poolclass=NullPool)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as db:
            u = User(username="probe", email="probe@example.com", password_hash="x", role="user")
            db.add(u)
            await db.commit()
            await db.refresh(u)
            db.add(UserSession(user_id=u.id, token=TOKEN, email=u.email, role="user"))
            s = InterviewSession(
                user_id=u.id, interview_type="comprehensive", difficulty="mid",
                duration=30, interview_mode="avatar", status="created",
                total_questions=5, current_question_no=0,
            )
            db.add(s)
            await db.commit()
            await db.refresh(s)
            auth = (await db.execute(
                select(UserSession).where(UserSession.token == TOKEN)
            )).scalar_one()
            ids = (u.id, s.id, auth.id)
        await engine.dispose()
        return ids

    return asyncio.run(_run())


def revoke_session():
    """撤销登录会话 **并**写一条撤销广播（= 管理员踢下线做的事）。"""
    from services import ws_revocation

    async def _run(db):
        await db.execute(
            update(UserSession).where(UserSession.token == TOKEN).values(revoked_at=datetime.utcnow())
        )
        await db.commit()
        await ws_revocation.record(
            db, user_id=PROBE_USER_ID[0], auth_session_id=None,
            reason=ws_revocation.REASON_ADMIN_KICK,
        )

    db_call(_run)


def measure(client, sid, query, label, budget=95.0):
    """建立连接 → 撤销 → 每 ``STEP`` 秒发一次 ping，直到被断开或超预算。"""
    t0 = None
    try:
        with client.websocket_connect(f"/api/interview/{sid}/ws?{query}") as sock:
            hello = sock.receive_json()
            assert hello.get("type") == "hello", hello
            # ★ 必须先把服务端**主动推**的快照排空（hello → state → question → avatar），
            #   否则每轮 `receive_json` 拿到的都是队列里的快照事件，把测得的延迟
            #   虚增成「快照条数 × 轮询步长」（改前那次 61.7s 的 token 测量同样受影响，
            #   但它的真因是 60s 节流，量级上不改变结论）。
            for _ in range(8):
                msg = sock.receive_json()
                if isinstance(msg, dict) and msg.get("type") == "avatar":
                    break
            t0 = time.monotonic()
            revoke_session()
            waited = 0.0
            while waited < budget:
                time.sleep(STEP)
                waited = time.monotonic() - t0
                try:
                    sock.send_json({"type": "ping"})
                    msg = sock.receive_json()
                except WebSocketDisconnect as exc:
                    print(f"  [{label}] 断开：code={exc.code}，耗时 {waited:.2f}s")
                    return waited, exc.code
                if msg.get("type") == "error":
                    print(f"  [{label}] 收到 error：{msg}，耗时 {waited:.2f}s")
                    return waited, msg.get("code")
            print(f"  [{label}] 预算 {budget:.0f}s 内**未断开**（仍可收发）")
            return None, None
    except WebSocketDisconnect as exc:
        print(f"  [{label}] 握手/接收阶段被断开：code={exc.code}")
        return 0.0, exc.code


def main_probe():
    # ★ 轮询周期必须在 TestClient 启动 lifespan **之前**写进环境变量：
    #   watcher.start() 在 lifespan 里读 ``watch_interval()``。
    os.environ["INTERVIEW_WS_REVOCATION_WATCH_INTERVAL"] = str(WATCH_INTERVAL)
    print(f"轮询周期 = {WATCH_INTERVAL}s（生产默认 2.0），测量步长 = {STEP}s\n")

    uid, sid, auth_id = prepare()
    PROBE_USER_ID[0] = uid
    PROBE_AUTH_SESSION_ID[0] = auth_id
    with TestClient(main.app) as client:
        # ★ ticket 现在**绑定登录会话**（``auth_session_id``）：这是「撤销能生效」
        #   的前提。改前的版本没有这个参数，所以 ticket 路径根本无法复核撤销。
        ticket = db_call(lambda db: store.issue(
            db, session_id=sid, user_id=uid, auth_session_id=auth_id))

        print("[A] ticket 路径（前端当前使用的那条）")
        r1 = measure(client, sid, f"ticket={ticket}", "ticket", budget=20.0)

        print("\n[B] token 路径（历史兼容）")
        # 复位撤销状态，重新用 token 连
        async def _unrevoke(db):
            await db.execute(update(UserSession).where(UserSession.token == TOKEN).values(revoked_at=None))
            await db.commit()

        db_call(_unrevoke)
        r2 = measure(client, sid, f"token={TOKEN}", "token", budget=95.0)

        print("\n结论：")
        print(f"  ticket 路径生效延迟 = {r1[0] if r1[0] is not None else '未生效（>20s 预算）'}；code={r1[1]}")
        print(f"  token  路径生效延迟 = {r2[0] if r2[0] is not None else '未生效（>95s 预算）'}；code={r2[1]}")
        print(f"  （轮询周期 {WATCH_INTERVAL}s，步长 {STEP}s ⇒ 分辨率 {STEP}s，"
              f"理论上界 ≈ 周期 + 步长）")
        print("\n改前基线（同一脚本、同一方法）：ticket >20s 不生效 / token 61.7s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="撤销生效延迟取证")
    parser.add_argument("--interval", type=float, default=0.2,
                        help="撤销广播轮询周期（秒）；2.0 = 生产默认")
    parser.add_argument("--step", type=float, default=0.5,
                        help="测量步长（秒）；越小分辨率越高")
    _args = parser.parse_args()
    WATCH_INTERVAL = _args.interval
    STEP = _args.step
    main_probe()
