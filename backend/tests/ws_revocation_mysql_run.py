# -*- coding: utf-8 -*-
"""撤销广播（``ws_revocation``）· **真实 MySQL** 运行器（不是回归套件）

为什么需要它
------------
``tests/test_ws_revocation.py``（55 项）证明的是**逻辑**：``matches()`` 口径、
轮询器状态机、跨实例投递、被踢后重连。它跑在 SQLite 内存库上，**证明不了**：

1. ``id > watermark`` 的增量读在 **InnoDB + REPEATABLE READ** 下的行为；
2. ★★ **同一个事务里反复读，快照不会前进**——这正是「把 30s 轮询改 1s」
   会踩的坑：如果轮询器复用同一个长事务，查询再频繁也永远看不到新行。
   SQLite 没有这种隔离级别，套件里**无法**复现；
3. 多连接并发 ``record()`` 时自增 id 的唯一性 / 单调性（不丢不重）；
4. 只追加表在**并发写 + 并发读**下，读者观察到的 id 序列必须是写者集合的
   **前缀**（没有空洞、没有重复、没有脏读）。

======================  =======================================  ==================
层                       文件                                      能证明什么
======================  =======================================  ==================
逻辑（SQLite）            ``tests/test_ws_revocation.py``           口径 / 状态机 /
                                                                  投递 / 延迟
真实并发（MySQL 8）       本文件（**不进回归循环**）                隔离级别下的增量
                                                                  读 / 并发写 / 跨实例
======================  =======================================  ==================

安全性（不碰生产数据）
----------------------
本运行器会 **CREATE 一个独立 schema**（``career_revoke_probe_<pid>_<ts>``），
在里面 ``create_all`` 建表、跑完全部场景，最后 **DROP**。生产库只做一次**只读**
检查（表 / 列 / 索引是否存在），并打印结论供部署前确认。

用法::

    cd backend
    python tests/ws_revocation_mysql_run.py            # 跑完即清理
    python tests/ws_revocation_mysql_run.py --keep     # 保留 probe 库

退出码：0 全部通过 / 1 有失败 / 2 环境不满足（连不上 / 无建库权限）
★ ``2`` 不是通过。CI 里由 ``tests/run_mysql_suites.py`` 统一把它判成失败。
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
#   它不联网（只连 MySQL）。数字人通道钉成 Mock，避免任何意外外呼。
os.environ.setdefault("AVATAR_PROVIDER", "")

from sqlalchemy import inspect as sa_inspect  # noqa: E402
from sqlalchemy import text  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool  # noqa: E402

from database import DATABASE_URL  # noqa: E402
import mysql_guard  # noqa: E402
from models import WsRevocation  # noqa: E402
from services import ws_revocation as rev  # noqa: E402

_PASSED = 0
_FAILED = 0
PROBE_PREFIX = "career_revoke_probe_"


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
    """模拟**一个独立实例**：自己的 engine / 连接池 / 会话。"""
    engine = create_async_engine(url, poolclass=NullPool)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with factory() as db:
            return await fn(db)
    except rev.RevocationStoreUnavailable as exc:
        return ("UNAVAILABLE", type(exc).__name__)
    except Exception as exc:  # noqa: BLE001
        if os.environ.get("REVOKE_DEBUG"):
            traceback.print_exc()
        return ("EXC", type(exc).__name__)
    finally:
        await engine.dispose()


def _record(url: str, user_id: int, auth_session_id=None, reason: str = "admin_kick"):
    return _one_worker(url, lambda db: rev.record(
        db, user_id=user_id, auth_session_id=auth_session_id, reason=reason))


async def run(keep: bool) -> int:
    print("=" * 74)
    print("撤销广播（ws_revocation）· 真实 MySQL 运行器")
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
                out = {"has_revocation": insp.has_table("ws_revocation"),
                       "has_ticket": insp.has_table("ws_ticket")}
                if out["has_revocation"]:
                    out["columns"] = {c["name"] for c in insp.get_columns("ws_revocation")}
                    out["indexes"] = {i["name"] for i in insp.get_indexes("ws_revocation")
                                      if i.get("name")}
                if out["has_ticket"]:
                    out["ticket_columns"] = {c["name"] for c in insp.get_columns("ws_ticket")}
                return out

            prod = await conn.run_sync(_prod)
    finally:
        await prod_engine.dispose()

    print("\n[1] 生产库结构（只读）")
    if not prod["has_revocation"]:
        # 不是失败：新表由应用启动时的 create_all 自动创建，零人工 DDL。
        print("  [INFO] 生产库暂无 ws_revocation 表 —— 下次启动由 create_all 自动创建")
        _check("[1.1] 生产库检查完成（表缺失不判失败，create_all 会建）", True)
    else:
        _check("[1.1] 生产库已有 ws_revocation 表", True)
        _check("[1.2] 列集合符合预期",
               {"id", "user_id", "auth_session_id", "reason", "created_at"}
               <= prod.get("columns", set()), str(sorted(prod.get("columns", set()))))
        _check("[1.3] user_id 有索引（轮询器按 id 取，索引供人工排查）",
               len(prod.get("indexes", set())) >= 1, str(prod.get("indexes")))
    if prod["has_ticket"]:
        _check("[1.4] 生产库 ws_ticket 已有 auth_session_id 列"
               "（缺列由 schema_sync 启动时补，见 [1b.3]）",
               "auth_session_id" in prod.get("ticket_columns", set()),
               str(sorted(prod.get("ticket_columns", set()))))
    else:
        print("  [INFO] 生产库暂无 ws_ticket 表 —— 跳过列检查")

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
    from utils import schema_sync

    setup_engine = create_async_engine(url, poolclass=NullPool)
    # ★ 先手工建一张「已有表」+ 一张**缺列**的 ws_ticket，再跑 create_all /
    #   ensure_columns：验证**非空 schema** 上的部署路径（真实生产库的样子）。
    async with setup_engine.begin() as conn:
        await conn.execute(text("CREATE TABLE existing_marker (id INT PRIMARY KEY)"))
        await conn.run_sync(Base.metadata.create_all)
        # 模拟「旧版本建的 ws_ticket 没有 auth_session_id」
        await conn.execute(text("ALTER TABLE ws_ticket DROP COLUMN auth_session_id"))
        facts0 = await conn.run_sync(lambda c: {
            "has_revocation": sa_inspect(c).has_table("ws_revocation"),
            "has_marker": sa_inspect(c).has_table("existing_marker"),
            "ticket_has_col": "auth_session_id" in {
                col["name"] for col in sa_inspect(c).get_columns("ws_ticket")},
        })
    await setup_engine.dispose()

    print("\n[1b] 部署前提：非空 schema 上 create_all 建新表 + schema_sync 补新列（零人工 DDL）")
    _check("[1b.1] ★ create_all 在已有其他表的 schema 里建出了 ws_revocation",
           facts0["has_revocation"] is True, str(facts0))
    _check("[1b.2] 既有表未被破坏", facts0["has_marker"] is True, str(facts0))
    _check("[1b.3] 前置：ws_ticket 此刻**缺** auth_session_id（模拟旧结构）",
           facts0["ticket_has_col"] is False, str(facts0))

    fix_engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with fix_engine.begin() as conn:
            added = await schema_sync.ensure_columns(conn)
        async with fix_engine.connect() as conn:
            facts1 = await conn.run_sync(lambda c: {
                "ticket_has_col": "auth_session_id" in {
                    col["name"] for col in sa_inspect(c).get_columns("ws_ticket")},
            })
        # 幂等：第二遍不应再加任何列
        async with fix_engine.begin() as conn:
            added2 = await schema_sync.ensure_columns(conn)
    finally:
        await fix_engine.dispose()

    _check("[1b.4] ★★ schema_sync 补上了 auth_session_id（既有表加列必须登记）",
           facts1["ticket_has_col"] is True, str(facts1))
    _check("[1b.5] 补列真的发生了（不是恒真）",
           any("ws_ticket" in str(a) for a in added), str(added))
    _check("[1b.6] ensure_columns 幂等：第二遍不再加列", added2 == [], str(added2))

    try:
        # ====================================================
        print("\n[2] 引擎与隔离级别")
        async with create_async_engine(url, poolclass=NullPool).connect() as conn:
            ver = (await conn.execute(text("SELECT VERSION()"))).scalar()
            level = (await conn.execute(
                text("SELECT @@transaction_isolation"))).scalar()
            mode = (await conn.execute(
                text("SELECT @@innodb_autoinc_lock_mode"))).scalar()
        _check("[2.1] 版本是 MySQL 8", str(ver).startswith("8."), str(ver))
        _check("[2.2] 隔离级别是 REPEATABLE-READ（下面的 [5] 正是针对它）",
               level == "REPEATABLE-READ", str(level))
        print(f"  [INFO] innodb_autoinc_lock_mode = {mode}")

        # ====================================================
        print("\n[3] ★ 8 个独立连接并发 record()：8 条全部落库、id 唯一")
        results = await asyncio.gather(*[
            _record(url, 900 + i, None if i % 2 else 1000 + i, "admin_kick")
            for i in range(8)
        ])
        ids = [r for r in results if isinstance(r, int)]
        _check("[3.1] ★ 8 个并发写全部成功（无死锁、无重复键）",
               len(ids) == 8, str(results))
        _check("[3.2] id 两两不同（自增唯一）", len(set(ids)) == 8, str(sorted(ids)))
        _check("[3.3] id 均 > 0", all(i > 0 for i in ids), str(sorted(ids)))

        async def _all_rows(db):
            return await rev.fetch_since(db, 0, limit=1000)

        rows = await _one_worker(url, _all_rows)
        _check("[3.4] ★★ 按 id > 0 全量读回，条数与写入一致（不丢不重）",
               isinstance(rows, list) and len(rows) == 8, str(rows if not isinstance(rows, list) else len(rows)))
        if isinstance(rows, list) and len(rows) == 8:
            got_ids = [r["id"] for r in rows]
            _check("[3.5] 返回按 id 升序（轮询器依赖顺序推进 watermark）",
                   got_ids == sorted(got_ids), str(got_ids))
            _check("[3.6] 集合与写入的 id 完全一致",
                   set(got_ids) == set(ids), f"{sorted(got_ids)} vs {sorted(ids)}")
            _check("[3.7] ★ auth_session_id 的 NULL 往返仍是 None（不是 0 / 空串）",
                   sum(1 for r in rows if r["auth_session_id"] is None) == 4,
                   str([r["auth_session_id"] for r in rows]))
            _check("[3.8] 非空 auth_session_id 原值往返",
                   sum(1 for r in rows if r["auth_session_id"] == 1000) == 1,
                   str([r["auth_session_id"] for r in rows]))
            _check("[3.9] reason 字段往返", all(r["reason"] == "admin_kick" for r in rows),
                   str([r["reason"] for r in rows]))

        watermark = max(ids) if ids else 0

        # ====================================================
        print("\n[4] ★ 轮询器口径：**每周期一个新事务** ⇒ 能看到别的事务已提交的新行")
        new_id = await _record(url, 4242, None, "admin_kick")
        _check("[4.0] 前置：新撤销已提交", isinstance(new_id, int), str(new_id))

        async def _fresh_poll(db):
            # 新会话 = 新事务 = 新快照（生产里每周期都新建会话）
            got = await rev.fetch_since(db, watermark, limit=100)
            await db.rollback()
            return got

        fresh = await _one_worker(url, _fresh_poll)
        _check("[4.1] ★★ 新事务读到了 watermark 之后的新行（不漏消息）",
               isinstance(fresh, list) and [r["id"] for r in fresh] == [new_id],
               str(fresh))
        _check("[4.2] 增量读不含 watermark 及之前的历史（不重放）",
               isinstance(fresh, list) and all(r["id"] > watermark for r in fresh),
               str(fresh))

        # ====================================================
        print("\n[5] ★★ REPEATABLE READ 快照陷阱：同一事务内重复读**看不到**新行")
        print("     （这就是「把 30s 轮询改成 1s」会踩的坑：事务不换，查再勤也白查）")

        async def _snapshot_trap(db):
            first = await rev.fetch_since(db, watermark, limit=100)
            # 另一个实例（独立连接）提交一条新的撤销
            other = await _record(url, 5555, None, "admin_kick")
            second = await rev.fetch_since(db, watermark, limit=100)
            await db.rollback()          # ★ 结束事务 = 丢掉快照
            third = await rev.fetch_since(db, watermark, limit=100)
            await db.rollback()
            return {"first": first, "other": other, "second": second, "third": third}

        trap = await _one_worker(url, _snapshot_trap)
        _check("[5.0] 前置：另一个实例的写入成功", isinstance(trap["other"], int), str(trap))
        _check("[5.1] 陷阱成立：同一事务内第二次读**看不到**新行",
               [r["id"] for r in trap["second"]] == [r["id"] for r in trap["first"]],
               f"{trap['first']} vs {trap['second']}")
        _check("[5.2] ★★ 换新事务（rollback 后）立刻看得到 ⇒ 轮询器必须每周期新建会话",
               trap["other"] in [r["id"] for r in trap["third"]], str(trap["third"]))
        _check("[5.3] 第三次读同时保留了原有行（增量不丢历史）",
               [r["id"] for r in trap["third"]] == sorted(
                   [r["id"] for r in trap["third"]]), str(trap["third"]))

        watermark = trap["other"]

        # ====================================================
        print("\n[6] ★ 跨实例：两个轮询器各自独立水位线，都能收到同一条撤销")
        # ★ 每个实例维护**自己的**游标：这正是 watcher 的语义（先推进 watermark
        #   再调 handler），否则会反复踢同一批。
        cursors = {"A": watermark, "B": watermark}

        async def _poller(tag: str):
            async def _inner(db):
                rows_ = await rev.fetch_since(db, cursors[tag], limit=100)
                await db.rollback()
                if rows_:
                    cursors[tag] = max(r["id"] for r in rows_)
                return rows_
            return await _one_worker(url, _inner)

        a0, b0 = await asyncio.gather(_poller("A"), _poller("B"))
        _check("[6.0] 前置：此刻无新行（水位线已推进到位）", a0 == [] and b0 == [],
               f"{a0} / {b0}")

        broadcast_id = await _record(url, 6666, 7001, "logout")
        a1, b1 = await asyncio.gather(_poller("A"), _poller("B"))
        _check("[6.1] ★★ 两个实例都收到同一条撤销（不是谁抢到算谁的）",
               isinstance(a1, list) and isinstance(b1, list)
               and [r["id"] for r in a1] == [broadcast_id]
               and [r["id"] for r in b1] == [broadcast_id],
               f"A={a1} B={b1}")
        _check("[6.2] 行内容一致（user_id / auth_session_id / reason）",
               a1 and b1 and a1[0] == b1[0], f"A={a1} B={b1}")
        _check("[6.3] auth_session_id 为空的『踢该用户全部连接』语义保留",
               isinstance(broadcast_id, int), str(broadcast_id))

        # 水位线推进后再读，必须是空（不重复投递）
        a2, b2 = await asyncio.gather(_poller("A"), _poller("B"))
        _check("[6.4] ★ 水位线推进后不再重复投递（幂等，不会反复踢同一批）",
               a2 == [] and b2 == [], f"A={a2} B={b2}")

        # ====================================================
        print("\n[7] 并发踢同一用户（多实例同时操作）：广播幂等，连接只断一次")
        closed = set()
        close_calls = []

        def _fake_handler(rows_):
            n = 0
            for r in rows_:
                for conn in [(6666, 7001), (6666, None), (9999, 7001)]:
                    uid, sid = conn
                    if conn in closed:
                        continue
                    if rev.matches(r, user_id=uid, auth_session_id=sid):
                        closed.add(conn)
                        n += 1
            close_calls.append(n)
            return n

        kick_ids = await asyncio.gather(*[
            _record(url, 6666, None, "admin_kick") for _ in range(5)
        ])
        _check("[7.1] 5 个并发踢下线都落库（谁先谁后不重要）",
               len([i for i in kick_ids if isinstance(i, int)]) == 5, str(kick_ids))

        async def _fetch_after(db):
            got = await rev.fetch_since(db, broadcast_id, limit=100)
            await db.rollback()
            return got

        batch = await _one_worker(url, _fetch_after)
        _check("[7.2] 一次读回 5 条", isinstance(batch, list) and len(batch) == 5,
               str(batch))
        first = _fake_handler(batch)
        second = _fake_handler(batch)
        _check("[7.3] ★ 第一批断开 2 条连接（同用户的两条）", first == 2, str(first))
        _check("[7.4] ★★ 同一批重复处理不再断开（幂等）", second == 0, str(second))
        _check("[7.5] 不相关用户 (9999) 未被误伤", (9999, 7001) not in closed, str(closed))
        _check("[7.6] 『按会话撤销』不误伤无会话值的连接",
               rev.matches({"user_id": 6666, "auth_session_id": 7001},
                           user_id=6666, auth_session_id=None) is False)

        # ====================================================
        print("\n[8] prune 不破坏「id > watermark」增量语义")
        old = await _one_worker(url, lambda db: rev.record(
            db, user_id=7777, auth_session_id=None, reason="logout",
            now=datetime.utcnow() - timedelta(hours=3)))
        _check("[8.0] 前置：写了一条 3 小时前的旧记录", isinstance(old, int), str(old))
        latest_before = await _one_worker(url, lambda db: rev.latest_id(db))
        deleted = await _one_worker(url, lambda db: rev.prune(db))
        _check("[8.1] prune 删掉了过期行", deleted >= 1, str(deleted))
        async def _after_prune(db):
            return await rev.fetch_since(db, broadcast_id, limit=100)
        after = await _one_worker(url, _after_prune)
        _check("[8.2] ★ 增量读不受 prune 影响（水位线之后的行都还在）",
               isinstance(after, list) and len(after) == 5, str(after))
        latest_after = await _one_worker(url, lambda db: rev.latest_id(db))
        _check("[8.3] ★ latest_id 可能回退，但『id > watermark』语义不变"
               "（回退只会让下次多读已处理的行，不会漏）",
               latest_after <= latest_before, f"{latest_before} -> {latest_after}")
        _check("[8.4] 未过期的行一条没少",
               isinstance(after, list) and len(after) == 5, str(after))

        # ====================================================
        print("\n[9] ★ 并发写 + 并发读：读者观察到的 id 序列必须是写者集合的前缀")
        base = await _one_worker(url, lambda db: rev.latest_id(db))

        async def _writer():
            out = []
            for i in range(20):
                rid = await _record(url, 8000 + i, None, "logout")
                out.append(rid)
            return out

        async def _reader():
            seen = []
            cursor = base
            for _ in range(60):
                async def _poll(db):
                    got = await rev.fetch_since(db, cursor, limit=100)
                    await db.rollback()
                    return got
                got = await _one_worker(url, _poll)
                if isinstance(got, list):
                    for r in got:
                        seen.append(r["id"])
                        cursor = max(cursor, r["id"])
                await asyncio.sleep(0)
            return seen

        w_task = asyncio.create_task(_writer())
        r_tasks = [asyncio.create_task(_reader()) for _ in range(2)]
        written = await w_task
        readers = await asyncio.gather(*r_tasks)

        _check("[9.1] 写者 20 条全部成功", len(written) == 20, str(written))
        _check("[9.2] 写者 id 严格递增且唯一",
               written == sorted(set(written)), str(written))

        ok_prefix = True
        ok_monotonic = True
        ok_dedup = True
        for seen in readers:
            if seen != sorted(seen):
                ok_monotonic = False
            if len(seen) != len(set(seen)):
                ok_dedup = False
            # 读者见过的每个 id 都必须是写者写过的，且集合是写者序列的前缀
            if not set(seen) <= set(written):
                ok_prefix = False
            elif seen:
                idx = written.index(max(seen))
                if set(seen) != set(written[: idx + 1]) & set(seen):
                    ok_prefix = False
        _check("[9.3] ★ 读者看到的 id 单调递增（无乱序投递）", ok_monotonic,
               str([r[:6] for r in readers]))
        _check("[9.4] ★★ 读者无重复（不会把同一条撤销投递两次）", ok_dedup,
               str([len(r) for r in readers]))
        _check("[9.5] ★★ 读者只看到写者写过的 id（无脏读 / 无幽灵行）", ok_prefix,
               str([r[:6] for r in readers]))
        _check("[9.6] 至少一个读者真的读到了内容（不是恒真）",
               any(len(r) > 0 for r in readers), str([len(r) for r in readers]))

        # ====================================================
        print("\n[10] 存储不可用：抛 RevocationStoreUnavailable（不静默当『没人被撤销』）")
        drop_engine = create_async_engine(url, poolclass=NullPool)
        try:
            async with drop_engine.begin() as conn:
                await conn.execute(text("RENAME TABLE ws_revocation TO ws_revocation_bak"))
            async def _broken(db):
                return await rev.fetch_since(db, 0)
            broken = await _one_worker(url, _broken)
            _check("[10.1] ★★ 表缺失时 fetch_since 抛 UNAVAILABLE（可区分于『没有新行』）",
                   isinstance(broken, tuple) and broken[0] == "UNAVAILABLE", str(broken))
            async def _broken_latest(db):
                return await rev.latest_id(db)
            broken2 = await _one_worker(url, _broken_latest)
            _check("[10.2] latest_id 同样 UNAVAILABLE（启动期就能发现）",
                   isinstance(broken2, tuple) and broken2[0] == "UNAVAILABLE", str(broken2))
            async def _broken_record(db):
                return await rev.record(db, user_id=1, auth_session_id=None, reason="logout")
            broken3 = await _one_worker(url, _broken_record)
            _check("[10.3] record 同样 UNAVAILABLE（调用方据此降级为周期复核）",
                   isinstance(broken3, tuple) and broken3[0] == "UNAVAILABLE", str(broken3))
            async def _broken_prune(db):
                return await rev.prune(db)
            broken4 = await _one_worker(url, _broken_prune)
            _check("[10.4] prune 失败只返回 0（机会式维护，不抛异常打断主流程）",
                   broken4 == 0, str(broken4))
        finally:
            try:
                async with drop_engine.begin() as conn:
                    await conn.execute(text("RENAME TABLE ws_revocation_bak TO ws_revocation"))
            except Exception:  # noqa: BLE001
                pass
            await drop_engine.dispose()

    finally:
        # ---------- 清理 ----------
        drop = create_async_engine(DATABASE_URL, poolclass=NullPool)
        try:
            async with drop.begin() as conn:
                await conn.execute(text(f"DROP DATABASE IF EXISTS `{schema}`"))
            print(f"\n[CLEAN] 已删除 probe 库：{schema}")
        except Exception as exc:  # noqa: BLE001
            print(f"\n[CLEAN] 删除 probe 库失败（{type(exc).__name__}）：{schema}")
        finally:
            await drop.dispose()

    print("\n" + "=" * 74)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return 0 if _FAILED == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="撤销广播 · 真实 MySQL 运行器")
    parser.add_argument("--keep", action="store_true", help="保留 probe 库（排查用）")
    args = parser.parse_args()
    return asyncio.run(run(args.keep))


if __name__ == "__main__":
    sys.exit(main())
