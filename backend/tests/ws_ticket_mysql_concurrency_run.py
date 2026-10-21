# -*- coding: utf-8 -*-
"""WebSocket ticket 共享存储 · **真实 MySQL** 并发运行器（不是回归套件）

为什么需要它
------------
项目硬约束：SQLite 只允许出现在 ``backend/tests/*.py``，而回归套件又**不能依赖
本机 MySQL**（否则换台机器就红）。于是 ticket 的验证被拆成两层：

=========================  ======================================  ==========================
层                          文件                                     能证明什么
=========================  ======================================  ==========================
语义（SQLite）               ``tests/test_ws_ticket.py``              签发/消费/过期/重放/
                                                                    绑定/撤销/失败策略/鉴权链路
**原子性**（MySQL 8）        本文件（**不进回归循环**）                8 个**独立物理连接**并发
                                                                    消费同一个 ticket 时
                                                                    只有 1 个成功
=========================  ======================================  ==========================

★ **不能只靠 SQLite 推断**：SQLite 没有多连接并发写，也没有 ``REPEATABLE READ``
快照语义。而 ticket 的核心要求正是"两个 worker 并发验证同一个 ticket 时最多一个
成功"——这只能在真实 InnoDB 行锁上验证。

安全性（不碰生产数据）
----------------------
本运行器会 **CREATE 一个独立 schema**（``career_ticket_probe_<pid>_<ts>``），
在里面 ``create_all`` 建表、跑完全部场景，最后 **DROP**。生产库只做一次**只读**
检查（表是否存在），并打印结论供部署前确认。

用法::

    cd backend
    python tests/ws_ticket_mysql_concurrency_run.py            # 跑完即清理
    python tests/ws_ticket_mysql_concurrency_run.py --keep     # 保留 probe 库

退出码：0 全部通过 / 1 有失败 / 2 环境不满足（连不上 / 无建库权限）
"""

from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import sys
import time
import traceback
from datetime import datetime, timedelta

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
_TESTS = pathlib.Path(__file__).resolve().parent
for _p in (str(_BACKEND), str(_TESTS)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ★ 刻意**不** import regression_env：本运行器要的是真实配置。
#   它不联网（只连本机 MySQL）。
os.environ.setdefault("AVATAR_PROVIDER", "")

from sqlalchemy import inspect as sa_inspect  # noqa: E402
from sqlalchemy import select, text  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool  # noqa: E402

from database import DATABASE_URL  # noqa: E402
import mysql_guard  # noqa: E402
from models import WsTicket  # noqa: E402
from services import ws_ticket_store as store  # noqa: E402

_PASSED = 0
_FAILED = 0
PROBE_PREFIX = "career_ticket_probe_"


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name
          + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def _probe_url(base: str, schema: str) -> str:
    head, _, _tail = base.rpartition("/")
    if "?" in head:
        head = head.split("?")[0]
    return f"{head}/{schema}?charset=utf8mb4"


def _redact(url: str) -> str:
    return url.rsplit("@", 1)[-1] if "@" in url else url


async def _one_worker(url: str, fn):
    """模拟**一个独立 worker**：自己的 engine / 连接 / 会话。"""
    engine = create_async_engine(url, poolclass=NullPool)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with factory() as db:
            return await fn(db)
    except store.TicketStoreUnavailable as exc:
        return ("UNAVAILABLE", type(exc).__name__)
    except Exception as exc:  # noqa: BLE001
        if os.environ.get("TICKET_DEBUG"):
            traceback.print_exc()
        return ("EXC", type(exc).__name__)
    finally:
        await engine.dispose()


async def run(keep: bool) -> int:
    print("=" * 74)
    print("WS ticket 共享存储 · 真实 MySQL 并发运行器")
    print("=" * 74)
    print(f"  生产库（只读检查）：{_redact(DATABASE_URL)}")

    if not DATABASE_URL.startswith("mysql"):
        print("[SKIP] DATABASE_URL 不是 MySQL，本运行器不适用")
        return 2

    # ★ 目标库守卫：集成测试会建库建表并 DROP，绝不允许指向生产 / 开发库。
    _unsafe = mysql_guard.unsafe_reason(DATABASE_URL)
    if _unsafe:
        print(f"[SKIP] {_unsafe}")
        return 2

    schema = f"{PROBE_PREFIX}{os.getpid()}_{int(time.time())}"

    # ---------- 生产库：只读 ----------
    prod_engine = create_async_engine(DATABASE_URL, poolclass=NullPool)
    try:
        async with prod_engine.connect() as conn:
            def _prod(sync_conn):
                insp = sa_inspect(sync_conn)
                if not insp.has_table("ws_ticket"):
                    return None
                cols = {c["name"] for c in insp.get_columns("ws_ticket")}
                uqs = {u["name"] for u in insp.get_unique_constraints("ws_ticket")}
                uqs |= {i["name"] for i in insp.get_indexes("ws_ticket")
                        if i.get("unique") and i.get("name")}
                return {"columns": cols, "unique": uqs}

            prod = await conn.run_sync(_prod)
    finally:
        await prod_engine.dispose()

    print("\n[1] 生产库结构（只读）")
    if prod is None:
        # 不是失败：表由应用启动时的 create_all 自动创建（新表，无需人工 DDL）。
        print("  [INFO] 生产库暂无 ws_ticket 表 —— 下次启动由 create_all 自动创建")
        _check("[1.1] 生产库检查完成（表缺失不判失败，create_all 会建）", True)
    else:
        _check("[1.1] 生产库已有 ws_ticket 表", True)
        _check("[1.2] 生产库含 ticket_hash 唯一约束",
               any("ticket_hash" in n for n in prod["unique"]), str(prod["unique"]))
        _check("[1.3] 列集合符合预期",
               {"ticket_hash", "user_id", "session_id",
                "created_at", "expires_at", "used_at"} <= prod["columns"],
               str(sorted(prod["columns"])))

    # ---------- 建 probe schema ----------
    admin = create_async_engine(DATABASE_URL, poolclass=NullPool)
    try:
        async with admin.begin() as conn:
            await conn.execute(text(
                f"CREATE DATABASE IF NOT EXISTS `{schema}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            ))
    except Exception as exc:  # noqa: BLE001
        print(f"[SKIP] 无法创建 probe 库（{type(exc).__name__}）：{exc}")
        await admin.dispose()
        return 2
    finally:
        await admin.dispose()

    url = _probe_url(DATABASE_URL, schema)
    print(f"  probe 库：{schema}")

    from database import Base

    setup_engine = create_async_engine(url, poolclass=NullPool)
    # ★ 先手工建一张「已有表」，再跑 create_all：验证**非空 schema** 也能补建新表
    #   （这正是"生产库已经有 interview_* 等表、只缺 ws_ticket"的真实场景）。
    async with setup_engine.begin() as conn:
        await conn.execute(text("CREATE TABLE existing_marker (id INT PRIMARY KEY)"))
        await conn.run_sync(Base.metadata.create_all)
        facts = await conn.run_sync(lambda c: {
            "has_ticket": sa_inspect(c).has_table("ws_ticket"),
            "has_marker": sa_inspect(c).has_table("existing_marker"),
        })
    await setup_engine.dispose()

    print("\n[1b] 部署前提：非空 schema 上 create_all 会自动补建 ws_ticket（零人工 DDL）")
    _check("[1b.1] ★ create_all 在已有其他表的 schema 里建出了 ws_ticket",
           facts["has_ticket"] is True, str(facts))
    _check("[1b.2] 既有表未被破坏", facts["has_marker"] is True, str(facts))

    try:
        # ====================================================
        print("\n[2] 引擎与隔离级别")
        async with create_async_engine(url, poolclass=NullPool).connect() as conn:
            ver = (await conn.execute(text("SELECT VERSION()"))).scalar()
            level = (await conn.execute(
                text("SELECT @@transaction_isolation"))).scalar()
        _check("[2.1] 版本是 MySQL 8", str(ver).startswith("8."), str(ver))
        _check("[2.2] 默认隔离级别是 REPEATABLE-READ（UPDATE 是当前读，不受快照限制）",
               level == "REPEATABLE-READ", str(level))

        # ====================================================
        print("\n[3] ★ 8 个独立连接并发消费**同一个** ticket：只能成功 1 次")
        ticket = await _one_worker(url, lambda db: store.issue(db, session_id=101, user_id=201))
        _check("[3.0] 前置：ticket 已签发", isinstance(ticket, str) and len(ticket) >= 32,
               str(ticket)[:40])

        results = await asyncio.gather(*[
            _one_worker(url, lambda db: store.consume(
                db, ticket=ticket, expected_session_id=101))
            for _ in range(8)
        ])
        winners = [r for r in results if r == 201]
        _check("[3.1] ★★ 恰好 1 个 worker 拿到 user_id（其余全部失败）",
               len(winners) == 1, str(results))
        _check("[3.2] 失败的 7 个都是 None（不是异常 / 不是 UNAVAILABLE）",
               all(r is None for r in results if r != 201), str(results))

        async with create_async_engine(url, poolclass=NullPool).connect() as conn:
            rows = (await conn.execute(text(
                "SELECT COUNT(*), COUNT(used_at) FROM ws_ticket "
                "WHERE session_id = 101"))).one()
        _check("[3.3] 库里只有 1 行、且已被标记 used_at",
               int(rows[0]) == 1 and int(rows[1]) == 1, str(tuple(rows)))

        # ====================================================
        print("\n[4] 会话绑定不符：并发消费全部失败，且**不消耗**该 ticket")
        bound = await _one_worker(url, lambda db: store.issue(db, session_id=102, user_id=202))
        wrong = await asyncio.gather(*[
            _one_worker(url, lambda db: store.consume(
                db, ticket=bound, expected_session_id=999))
            for _ in range(4)
        ])
        _check("[4.1] 绑定不符时 4 个 worker 全部失败", all(r is None for r in wrong), str(wrong))
        still = await _one_worker(url, lambda db: store.consume(
            db, ticket=bound, expected_session_id=102))
        _check("[4.2] ★ 原会话仍可消费（错配没有把 ticket 用掉）", still == 202, str(still))

        # ====================================================
        print("\n[5] 过期：并发消费全部失败")
        expired = await _one_worker(url, lambda db: store.issue(
            db, session_id=103, user_id=203))

        async def _expire(db):
            from sqlalchemy import update
            await db.execute(
                update(WsTicket)
                .where(WsTicket.ticket_hash == store.hash_ticket(expired))
                .values(expires_at=datetime.utcnow() - timedelta(seconds=5))
            )
            await db.commit()

        await _one_worker(url, _expire)
        res = await asyncio.gather(*[
            _one_worker(url, lambda db: store.consume(
                db, ticket=expired, expected_session_id=103))
            for _ in range(4)
        ])
        _check("[5.1] ★ 过期 ticket 并发消费全部失败", all(r is None for r in res), str(res))

        # ====================================================
        print("\n[6] 跨连接语义：A 连接签发 → B 连接消费（模拟 A 实例签发 / B 实例校验）")
        cross = await _one_worker(url, lambda db: store.issue(
            db, session_id=104, user_id=204))
        got = await _one_worker(url, lambda db: store.consume(
            db, ticket=cross, expected_session_id=104))
        _check("[6.1] ★★ 不同物理连接能读到同一个 ticket 并消费成功", got == 204, str(got))

        # ====================================================
        print("\n[7] 撤销 与 消费 并发：不能同时成功")
        race = await _one_worker(url, lambda db: store.issue(
            db, session_id=105, user_id=205))
        consumed, revoked = await asyncio.gather(
            _one_worker(url, lambda db: store.consume(
                db, ticket=race, expected_session_id=105)),
            _one_worker(url, lambda db: store.revoke(db, ticket=race)),
        )
        _check("[7.1] ★ 二者最多只有一个生效",
               not (consumed == 205 and revoked is True),
               f"consumed={consumed} revoked={revoked}")

        # ====================================================
        print("\n[8] 存储不可用：抛 TicketStoreUnavailable（不降级、不返回 None）")
        async with create_async_engine(url, poolclass=NullPool).begin() as conn:
            await conn.execute(text("DROP TABLE ws_ticket"))

        unavailable = await _one_worker(url, lambda db: store.consume(
            db, ticket="whatever", expected_session_id=1))
        _check("[8.1] ★★ 表缺失时返回 UNAVAILABLE（可区分于'凭证无效'）",
               isinstance(unavailable, tuple) and unavailable[0] == "UNAVAILABLE",
               str(unavailable))
        issue_fail = await _one_worker(url, lambda db: store.issue(
            db, session_id=1, user_id=1))
        _check("[8.2] ★★ 签发同样 UNAVAILABLE（不返回废票）",
               isinstance(issue_fail, tuple) and issue_fail[0] == "UNAVAILABLE",
               str(issue_fail))

        # ====================================================
        print("\n[9] 库里不含 ticket 明文（凭证泄露面）")
        async with create_async_engine(url, poolclass=NullPool).begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        plain = await _one_worker(url, lambda db: store.issue(
            db, session_id=106, user_id=206))
        async with create_async_engine(url, poolclass=NullPool).connect() as conn:
            row = (await conn.execute(text(
                "SELECT ticket_hash, user_id, session_id FROM ws_ticket "
                "WHERE session_id = 106"))).one()
        _check("[9.1] ★ 库里只有 sha256 摘要（64 位 hex），与明文不同",
               len(row[0]) == 64 and row[0] != plain and row[0] == store.hash_ticket(plain),
               str(row[0])[:20])
        _check("[9.2] 摘要不可反推（明文不出现在该行任何字段）",
               plain not in f"{row[0]}|{row[1]}|{row[2]}")

        # ====================================================
        print("\n[10] 清理：过期行被删、未过期行保留")
        alive = await _one_worker(url, lambda db: store.issue(
            db, session_id=107, user_id=207))
        dead = await _one_worker(url, lambda db: store.issue(
            db, session_id=108, user_id=208))

        async def _expire_dead(db):
            from sqlalchemy import update
            await db.execute(
                update(WsTicket)
                .where(WsTicket.ticket_hash == store.hash_ticket(dead))
                .values(expires_at=datetime.utcnow() - timedelta(seconds=5))
            )
            await db.commit()

        await _one_worker(url, _expire_dead)
        removed = await _one_worker(url, lambda db: store.sweep_expired(db, force=True))
        _check("[10.1] 清理删除了过期行", isinstance(removed, int) and removed >= 1,
               str(removed))
        kept = await _one_worker(url, lambda db: store.consume(
            db, ticket=alive, expected_session_id=107))
        _check("[10.2] ★ 未过期的 ticket 仍然可用", kept == 207, str(kept))
    finally:
        if keep:
            print(f"\n[KEEP] 保留 probe 库：{schema}")
        else:
            cleanup = create_async_engine(DATABASE_URL, poolclass=NullPool)
            try:
                async with cleanup.begin() as conn:
                    await conn.execute(text(f"DROP DATABASE IF EXISTS `{schema}`"))
                print(f"\n[CLEAN] 已删除 probe 库：{schema}")
            finally:
                await cleanup.dispose()

    print("\n" + "=" * 74)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return 0 if _FAILED == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="WS ticket 真实 MySQL 并发验证")
    parser.add_argument("--keep", action="store_true", help="保留 probe 库供排查")
    args = parser.parse_args()
    return asyncio.run(run(args.keep))


if __name__ == "__main__":
    sys.exit(main())
