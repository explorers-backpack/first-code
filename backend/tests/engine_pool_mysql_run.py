# -*- coding: utf-8 -*-
"""SQLAlchemy Async · **驱动 / 连接池 / 事务边界** 的真实 MySQL 验证。

为什么单独一套
--------------
其它四个运行器为了"真并发"，一律用 ``poolclass=NullPool``（每次一条独立物理
连接）。这能证明**数据库侧**的行锁与隔离级别，但**证明不了**生产实际用的
**连接池**行为：

============================  ==============================================
生产配置                       本套件要验证什么
============================  ==============================================
``aiomysql`` 异步驱动          真的是异步引擎、真的是 aiomysql（不是 pymysql）
``pool_pre_ping=True``         ★ 池里的连接被服务端杀掉后，下一次取用**自动重连**
                               （而不是把死连接交给业务）
``AsyncAdaptedQueuePool``      池真的有上限（``pool_size=1`` 时第二个并发取用会等）
``expire_on_commit=False``     commit 后对象属性仍可读（不触发额外 IO）
事务边界                        提交可见 / 回滚不可见 / 异常自动回滚 / 无脏读
============================  ==============================================

★ ``pool_pre_ping`` 那条**必须有对照组**：关掉 pre_ping 的引擎在同样条件下
必须**抛错**。否则"重连成功"可能只是巧合（MySQL 还没把死连接清理掉）。

用法::

    cd backend
    python tests/engine_pool_mysql_run.py

退出码：0 全部通过 / 1 有失败 / 2 环境不满足
"""

from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import sys
import time
import traceback

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
_TESTS = pathlib.Path(__file__).resolve().parent
for _p in (str(_BACKEND), str(_TESTS)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

os.environ.setdefault("AVATAR_PROVIDER", "")

from sqlalchemy import (  # noqa: E402
    Column,
    Integer,
    MetaData,
    String,
    Table,
    text,
)
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool  # noqa: E402

from database import DATABASE_URL  # noqa: E402
import mysql_guard  # noqa: E402

_PASSED = 0
_FAILED = 0
PROBE_PREFIX = "career_pool_probe_"

#: 事务边界用的探针表（Core 定义，避免依赖 ORM 模型）
TX = Table(
    "tx_probe",
    MetaData(),
    Column("id", Integer, primary_key=True),
    Column("v", String(16)),
)


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


async def _admin(sql: str) -> None:
    """用一条**独立**连接执行管理语句（不参与被测引擎的池）。"""
    engine = create_async_engine(DATABASE_URL, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(sql))
    finally:
        await engine.dispose()


async def run(keep: bool) -> int:
    print("=" * 74)
    print("SQLAlchemy Async · 驱动 / 连接池 / 事务边界（真实 MySQL）")
    print("=" * 74)
    print(f"  目标库（只读 + probe）：{_redact(DATABASE_URL)}")

    if not DATABASE_URL.startswith("mysql"):
        print("[SKIP] DATABASE_URL 不是 MySQL，本运行器不适用")
        return 2
    unsafe = mysql_guard.unsafe_reason(DATABASE_URL)
    if unsafe:
        print(f"[SKIP] {unsafe}")
        return 2

    schema = f"{PROBE_PREFIX}{os.getpid()}_{int(time.time())}"
    try:
        await _admin(f"CREATE DATABASE IF NOT EXISTS `{schema}` "
                     "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    except Exception as exc:  # noqa: BLE001
        print(f"[SKIP] 无法创建 probe 库（{type(exc).__name__}）：{exc}")
        return 2

    url = _probe_url(DATABASE_URL, schema)
    print(f"  probe 库：{schema}")

    try:
        # ====================================================
        print("\n[1] 驱动：必须是**异步**引擎 + aiomysql")
        import database as app_db

        probe_engine = create_async_engine(url)
        _check("[1.1] create_async_engine 返回异步引擎",
               type(probe_engine).__name__.startswith("Async"),
               type(probe_engine).__name__)
        _check("[1.2] 方言是 mysql", probe_engine.dialect.name == "mysql",
               probe_engine.dialect.name)
        _check("[1.3] ★ 驱动是 aiomysql（不是同步的 pymysql）",
               probe_engine.dialect.driver == "aiomysql",
               probe_engine.dialect.driver)
        _check("[1.4] 生产 engine（database.py）同样是异步 aiomysql",
               app_db.engine.dialect.name == "mysql"
               and app_db.engine.dialect.driver == "aiomysql",
               f"{app_db.engine.dialect.name}/{app_db.engine.dialect.driver}")
        await probe_engine.dispose()

        # ====================================================
        print("\n[2] 生产连接池配置（database.py 里真正生效的那些）")
        pool = app_db.engine.pool
        pool_name = type(pool).__name__
        _check("[2.1] ★ 生产用**池**（不是 NullPool / StaticPool）",
               "NullPool" not in pool_name and "StaticPool" not in pool_name,
               pool_name)
        _check("[2.2] ★★ pool_pre_ping 已开启（死连接不会交给业务）",
               bool(getattr(pool, "_pre_ping", False)),
               str(getattr(pool, "_pre_ping", "missing")))
        _check("[2.3] 池类名符合 SQLAlchemy 2.x 异步默认（QueuePool 族）",
               "QueuePool" in pool_name, pool_name)
        _check("[2.4] 生产 factory 用 expire_on_commit=False",
               app_db.async_session.kw.get("expire_on_commit") is False,
               str(app_db.async_session.kw.get("expire_on_commit")))

        # ====================================================
        print("\n[3] ★ 池上限是真的：pool_size=1 时第二个并发取用必须等待并超时")
        tight = create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=1)
        try:
            async with tight.connect() as c1:
                cid1 = (await c1.execute(text("SELECT CONNECTION_ID()"))).scalar()
                t0 = time.monotonic()
                timed_out = False
                try:
                    async with tight.connect() as c2:
                        await c2.execute(text("SELECT 1"))
                except Exception as exc:  # noqa: BLE001 - 期望 TimeoutError
                    timed_out = "Timeout" in type(exc).__name__
                waited = time.monotonic() - t0
            _check("[3.1] ★★ 第二个并发取用被池阻塞后超时（池上限生效）",
                   timed_out, f"timed_out={timed_out} waited={waited:.2f}s")
            _check("[3.2] 等待时长≈pool_timeout（不是立即失败）",
                   waited >= 0.9, f"{waited:.2f}s")
            _check("[3.3] 拿到的是真实物理连接 id",
                   isinstance(cid1, int) and cid1 > 0, str(cid1))
        finally:
            await tight.dispose()

        # ====================================================
        print("\n[4] ★★ pool_pre_ping：池里的连接被服务端杀掉后自动重连")
        async def _kill_cycle(pre_ping: bool):
            """取一条连接 → 归还 → 从外部 KILL → 再取。返回 (状态, 旧id, 新id)。"""
            eng = create_async_engine(
                url, pool_size=1, max_overflow=0, pool_pre_ping=pre_ping)
            try:
                async with eng.connect() as c:
                    old_id = (await c.execute(text("SELECT CONNECTION_ID()"))).scalar()
                # 连接已归还到池里；从另一条连接把它杀掉
                await _admin(f"KILL CONNECTION {int(old_id)}")
                await asyncio.sleep(0.2)
                try:
                    async with eng.connect() as c:
                        new_id = (await c.execute(
                            text("SELECT CONNECTION_ID()"))).scalar()
                        return ("OK", old_id, new_id)
                except Exception as exc:  # noqa: BLE001
                    return (type(exc).__name__, old_id, None)
            finally:
                await eng.dispose()

        status_on, old_id, new_id = await _kill_cycle(True)
        status_off, _old2, _new2 = await _kill_cycle(False)

        _check("[4.1] ★★ 开启 pre_ping ⇒ 下一次取用自动重连并查询成功",
               status_on == "OK", f"status={status_on} old_id={old_id}")
        _check("[4.2] ★★ 关闭 pre_ping ⇒ 同样条件下**报错**（对照组，证明不是巧合）",
               status_off != "OK", f"status={status_off}")
        _check("[4.3] ★ 重连后拿到的是**另一条**物理连接",
               status_on == "OK" and new_id is not None and new_id != old_id,
               f"old={old_id} new={new_id}")

        # ====================================================
        print("\n[5] 事务边界：提交可见 / 回滚不可见 / 异常自动回滚")
        ddl = create_async_engine(url, poolclass=NullPool)
        try:
            async with ddl.begin() as conn:
                await conn.execute(text(
                    "CREATE TABLE tx_probe (id INT PRIMARY KEY, v VARCHAR(16)) "
                    "ENGINE=InnoDB"))
        finally:
            await ddl.dispose()

        engine = create_async_engine(url, poolclass=NullPool)
        factory = async_sessionmaker(engine, class_=AsyncSession,
                                     expire_on_commit=False)

        async def _count(where: str = "") -> int:
            async with engine.connect() as c:
                return int((await c.execute(
                    text(f"SELECT COUNT(*) FROM tx_probe {where}"))).scalar() or 0)

        async with factory() as db:
            await db.execute(TX.insert().values(id=1, v="a"))
            await db.commit()
        _check("[5.1] ★ 提交后新连接看得到", await _count() == 1, str(await _count()))

        async with factory() as db:
            await db.execute(TX.insert().values(id=2, v="b"))
            inside = int((await db.execute(
                text("SELECT COUNT(*) FROM tx_probe"))).scalar() or 0)
            await db.rollback()
        _check("[5.2a] 同一事务内看得到自己未提交的写入", inside == 2, str(inside))
        _check("[5.2b] ★★ 回滚后别的连接看不到", await _count() == 1, str(await _count()))

        try:
            async with factory() as db:
                await db.execute(TX.insert().values(id=3, v="c"))
                await db.flush()
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        _check("[5.3] ★★ 异常退出 with 块 ⇒ 自动回滚（没有半截数据）",
               await _count() == 1, str(await _count()))

        # ====================================================
        print("\n[6] 隔离：两个独立会话看不到对方**未提交**的写入（无脏读）")
        async with factory() as a, factory() as b:
            await a.execute(TX.insert().values(id=9, v="x"))
            await a.flush()
            seen = int((await b.execute(
                text("SELECT COUNT(*) FROM tx_probe WHERE id = 9"))).scalar() or 0)
            await a.rollback()
        _check("[6.1] ★★ 另一会话看不到未提交行（无脏读）", seen == 0, str(seen))
        _check("[6.2] 回滚后该行确实不存在", await _count("WHERE id = 9") == 0,
               str(await _count("WHERE id = 9")))

        # ====================================================
        print("\n[7] 连接归还与复用：会话退出必须归还，池必须真的复用同一条物理连接")
        pooled = create_async_engine(url, pool_size=2, max_overflow=0)
        pooled_factory = async_sessionmaker(pooled, class_=AsyncSession,
                                            expire_on_commit=False)
        try:
            async with pooled_factory() as db:
                inside_id = (await db.execute(
                    text("SELECT CONNECTION_ID()"))).scalar()
                checkedout_inside = pooled.pool.checkedout()
            _check("[7.1] 会话进行中池里借出 1 条连接", checkedout_inside == 1,
                   str(checkedout_inside))
            _check("[7.2] ★ 会话退出后连接已归还（checkedout 归零）",
                   pooled.pool.checkedout() == 0, str(pooled.pool.checkedout()))
            _check("[7.3] 归还后池里留着可复用连接（checkedin ≥ 1）",
                   pooled.pool.checkedin() >= 1, str(pooled.pool.checkedin()))

            async with pooled_factory() as db:
                again_id = (await db.execute(
                    text("SELECT CONNECTION_ID()"))).scalar()
            _check("[7.4] ★★ 下一次会话复用的是**同一条**物理连接（池真的在复用）",
                   again_id == inside_id, f"first={inside_id} again={again_id}")
        finally:
            await pooled.dispose()
        _check("[7.5] dispose 后池清空（checkedout=0 且 checkedin=0）",
               pooled.pool.checkedout() == 0 and pooled.pool.checkedin() == 0,
               f"out={pooled.pool.checkedout()} in={pooled.pool.checkedin()}")

        # 收尾：把 [1.4] / [2] 用到的生产 engine 关掉，避免 asyncio 退出时告警
        await app_db.engine.dispose()

    except Exception:  # noqa: BLE001
        if os.environ.get("POOL_DEBUG"):
            traceback.print_exc()
        raise
    finally:
        try:
            if keep:
                print(f"\n[KEEP] 保留 probe 库：{schema}")
            else:
                await _admin(f"DROP DATABASE IF EXISTS `{schema}`")
                print(f"\n[CLEAN] 已删除 probe 库：{schema}")
        except Exception as exc:  # noqa: BLE001
            print(f"\n[CLEAN] 删除 probe 库失败（{type(exc).__name__}）：{schema}")

    print("\n" + "=" * 74)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return 0 if _FAILED == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="驱动/连接池/事务边界 · 真实 MySQL")
    parser.add_argument("--keep", action="store_true", help="保留 probe 库")
    args = parser.parse_args()
    return asyncio.run(run(args.keep))


if __name__ == "__main__":
    sys.exit(main())
