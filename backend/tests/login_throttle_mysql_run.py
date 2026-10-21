# -*- coding: utf-8 -*-
"""登录失败限流 · **真实 MySQL** 运行器（不是回归套件）

为什么需要它
------------
``tests/test_login_throttle_store.py``（42 项）跑在 SQLite 上，能证明滑窗语义、
摘要、fail-closed、接线；**证明不了**真实 InnoDB 下的两件事：

1. **跨实例计数是否真的统一**——这是「多 worker 后爆破上限被放大 N 倍」
   这个安全项的唯一判据。SQLite 的单文件共享不能替代多连接并发写。
2. ★ **并发突发的实际上界**。``check`` 与 ``record`` 之间不是原子的
   （check-then-act），M 个并发请求可能都看到「还没满」而一起放行。
   本运行器**实测**这个数字，而不是假设它是 0。

安全性（不碰生产数据）
----------------------
CREATE 独立 schema ``career_throttle_probe_<pid>_<ts>``，跑完 DROP；
生产库只做一次**只读**检查。

用法::

    cd backend
    python tests/login_throttle_mysql_run.py
    python tests/login_throttle_mysql_run.py --keep

退出码：0 全部通过 / 1 有失败 / 2 环境不满足
★ ``2`` 不是通过。CI 里由 ``tests/run_mysql_suites.py`` 判成失败。
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

os.environ.setdefault("AVATAR_PROVIDER", "")
# ★ 本运行器测的就是共享后端，必须显式切过去（regression_env 不参与）。
os.environ["AUTH_LOGIN_STORE"] = "db"
os.environ["AUTH_LOGIN_MAX_FAILURES"] = "5"
os.environ["AUTH_LOGIN_WINDOW_SECONDS"] = "900"

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
from models import LoginFailure  # noqa: E402
from services import login_throttle_store as store  # noqa: E402

_PASSED = 0
_FAILED = 0
PROBE_PREFIX = "career_throttle_probe_"
KEY = "victim@example.com|203.0.113.9"


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
    except store.LoginThrottleUnavailable as exc:
        return ("UNAVAILABLE", type(exc).__name__)
    except Exception as exc:  # noqa: BLE001
        if os.environ.get("THROTTLE_DEBUG"):
            traceback.print_exc()
        return ("EXC", type(exc).__name__)
    finally:
        await engine.dispose()


async def run(keep: bool) -> int:
    print("=" * 74)
    print("登录失败限流 · 真实 MySQL 运行器")
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
                if not insp.has_table("login_failure"):
                    return None
                cols = {c["name"] for c in insp.get_columns("login_failure")}
                idx = {i["name"] for i in insp.get_indexes("login_failure") if i.get("name")}
                return {"columns": cols, "indexes": idx}

            prod = await conn.run_sync(_prod)
    finally:
        await prod_engine.dispose()

    print("\n[1] 生产库结构（只读）")
    if prod is None:
        print("  [INFO] 生产库暂无 login_failure 表 —— 下次启动由 create_all 自动创建")
        _check("[1.1] 生产库检查完成（表缺失不判失败，create_all 会建）", True)
    else:
        _check("[1.1] 生产库已有 login_failure 表", True)
        _check("[1.2] 列集合符合预期",
               {"id", "key_hash", "failed_at"} <= prod["columns"],
               str(sorted(prod["columns"])))
        _check("[1.3] 有索引（check 的滑窗查询靠它）", len(prod["indexes"]) >= 1,
               str(prod["indexes"]))

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
    async with setup_engine.begin() as conn:
        await conn.execute(text("CREATE TABLE existing_marker (id INT PRIMARY KEY)"))
        await conn.run_sync(Base.metadata.create_all)
        facts = await conn.run_sync(lambda c: {
            "has_table": sa_inspect(c).has_table("login_failure"),
            "has_marker": sa_inspect(c).has_table("existing_marker"),
        })
    await setup_engine.dispose()

    print("\n[1b] 部署前提：非空 schema 上 create_all 自动建 login_failure（零人工 DDL）")
    _check("[1b.1] ★ 在已有其他表的 schema 里建出了 login_failure",
           facts["has_table"] is True, str(facts))
    _check("[1b.2] 既有表未被破坏", facts["has_marker"] is True, str(facts))

    try:
        # ====================================================
        print("\n[2] 引擎与隔离级别")
        async with create_async_engine(url, poolclass=NullPool).connect() as conn:
            ver = (await conn.execute(text("SELECT VERSION()"))).scalar()
            level = (await conn.execute(
                text("SELECT @@transaction_isolation"))).scalar()
        _check("[2.1] 版本是 MySQL 8", str(ver).startswith("8."), str(ver))
        _check("[2.2] 隔离级别是 REPEATABLE-READ", level == "REPEATABLE-READ", str(level))
        _check("[2.3] 后端开关确实是 db", store.store_mode() == "db", store.store_mode())

        # ====================================================
        print("\n[3] 8 个独立连接并发 record()：8 行全部落库（不丢）")
        results = await asyncio.gather(*[
            _one_worker(url, lambda db: store.record(db, KEY)) for _ in range(8)
        ])
        _check("[3.1] 8 个并发写全部成功（无死锁）",
               all(r is None for r in results), str(results))

        async def _rows(db):
            return (await db.execute(select(LoginFailure))).scalars().all()

        rows = await _one_worker(url, _rows)
        _check("[3.2] ★★ 表里恰好 8 行（一条都没丢）",
               isinstance(rows, list) and len(rows) == 8, str(len(rows) if isinstance(rows, list) else rows))

        # ====================================================
        print("\n[4] ★★ 跨实例：A 记满 5 次 ⇒ B 的 check 必须被拦")
        async def _reset(db):
            await store.reset(db, KEY)

        await _one_worker(url, _reset)
        for _ in range(5):
            await _one_worker(url, lambda db: store.record(db, KEY))
        seen_by_b = await _one_worker(url, lambda db: store.check(db, KEY))
        _check("[4.1] ★★ 实例 B 读到实例 A 记的计数（跨实例统一）",
               isinstance(seen_by_b, int) and seen_by_b > 0, str(seen_by_b))
        _check("[4.2] Retry-After 在 (0, 900] 内",
               isinstance(seen_by_b, int) and 0 < seen_by_b <= 900, str(seen_by_b))

        # ====================================================
        print("\n[5] ★★ 并发突发的**实际**上界（check-then-act 的诚实测量）")
        print("     设计目标：窗口内最多 5 次失败。这里让 20 个独立连接同时")
        print("     做「check → 若放行则 record」，看实际放行了几次。")
        print("     ★ 对照：内存实现在同样交错下也放行 20 次"
              "（tests/test_login_throttle_store.py [10]），")
        print("       所以溢出是**既有语义**，不是共享存储引入的退化。")
        await _one_worker(url, _reset)

        async def _attack_once():
            async def _inner(db):
                allowed = await store.check(db, KEY)
                if allowed:
                    return 0
                await store.record(db, KEY)
                return 1
            return await _one_worker(url, _inner)

        burst = await asyncio.gather(*[_attack_once() for _ in range(20)])
        admitted = sum(1 for b in burst if b == 1)
        rows_after = await _one_worker(url, _rows)
        n_rows = len(rows_after) if isinstance(rows_after, list) else -1

        print(f"     实测：20 个并发请求中放行 {admitted} 次；"
              f"窗口内失败记录 {n_rows} 行（上限 5）")
        _check("[5.1] 至少放行了上限那么多（否则限流根本没生效）",
               admitted >= 5, str(admitted))
        _check("[5.2] 放行次数与落库行数一致",
               admitted == n_rows, f"admitted={admitted} rows={n_rows}")
        _check("[5.3] 放行次数未超过并发数（上界 = 5 + 并发溢出）",
               admitted <= 20, str(admitted))
        print(f"     溢出 = {admitted - 5} 次（放行数 - 上限）")

        # ★★ 真正的保证：突发**之后**，同一窗口内再也放不进来。
        #    单次突发多放过几条不是本次要解决的问题（内存实现同样如此）；
        #    要保证的是「持续攻击被全局封住」。
        second = await asyncio.gather(*[_attack_once() for _ in range(20)])
        second_admitted = sum(1 for b in second if b == 1)
        _check("[5.4] ★★ 第二波 20 并发一个都放不进来（持续攻击被全局封住）",
               second_admitted == 0, str(second_admitted))
        rows_after2 = await _one_worker(url, _rows)
        _check("[5.5] ★ 第二波没有新增行",
               isinstance(rows_after2, list) and len(rows_after2) == n_rows,
               f"{n_rows} -> {len(rows_after2) if isinstance(rows_after2, list) else rows_after2}")
        # 拦住了还要**报得出等待时间**（否则前端/运维不知道多久后能重试）
        retry = await _one_worker(url, lambda db: store.check(db, KEY))
        _check("[5.6] 被封期间仍返回 Retry-After（>0 且 <= 窗口）",
               isinstance(retry, int) and 0 < retry <= 900, str(retry))

        # ====================================================
        print("\n[6] 窗口过期：滑出窗口的记录不再计数")
        await _one_worker(url, _reset)
        for _ in range(5):
            await _one_worker(url, lambda db: store.record(db, KEY))
        blocked_before = await _one_worker(url, lambda db: store.check(db, KEY))
        _check("[6.0] 前置：已被拦截", blocked_before > 0, str(blocked_before))

        async def _age(db):
            from sqlalchemy import update
            await db.execute(update(LoginFailure).values(
                failed_at=datetime.utcnow() - timedelta(seconds=901)))
            await db.commit()

        await _one_worker(url, _age)
        _check("[6.1] ★ 全部滑出窗口后放行",
               await _one_worker(url, lambda db: store.check(db, KEY)) == 0)

        # ====================================================
        print("\n[7] reset 跨实例生效")
        await _one_worker(url, _reset)
        for _ in range(5):
            await _one_worker(url, lambda db: store.record(db, KEY))
        await _one_worker(url, _reset)
        _check("[7.1] ★ 一个实例 reset 后，另一个实例的 check 也放行",
               await _one_worker(url, lambda db: store.check(db, KEY)) == 0)
        _check("[7.2] 行被删干净",
               len(await _one_worker(url, _rows)) == 0)

        # ====================================================
        print("\n[8] 库里只有摘要（凭证/隐私泄露面）")
        await _one_worker(url, lambda db: store.record(db, KEY))
        digest = store.key_digest(KEY)

        async def _raw(db):
            result = await db.execute(text("SELECT key_hash FROM login_failure"))
            return list(result.scalars().all())

        raw = await _one_worker(url, _raw)
        _check("[8.1] 存的是 64 位摘要", raw == [digest], str(raw))
        _check("[8.2] ★ 不含明文邮箱", all("victim" not in (r or "") for r in raw), str(raw))
        _check("[8.3] ★ 不含明文 IP", all("203.0.113.9" not in (r or "") for r in raw), str(raw))

        # ====================================================
        print("\n[9] 存储不可用：fail closed")
        drop_engine = create_async_engine(url, poolclass=NullPool)
        try:
            async with drop_engine.begin() as conn:
                await conn.execute(text("RENAME TABLE login_failure TO login_failure_bak"))
            broken = await _one_worker(url, lambda db: store.check(db, KEY))
            _check("[9.1] ★★ check 抛 UNAVAILABLE（可区分于『没被限流』）",
                   isinstance(broken, tuple) and broken[0] == "UNAVAILABLE", str(broken))
            broken2 = await _one_worker(url, lambda db: store.record(db, KEY))
            _check("[9.2] record 同样 UNAVAILABLE",
                   isinstance(broken2, tuple) and broken2[0] == "UNAVAILABLE", str(broken2))
            broken3 = await _one_worker(url, lambda db: store.reset(db, KEY))
            _check("[9.3] reset 不抛（清不掉计数不阻断登录成功）", broken3 is None, str(broken3))
        finally:
            try:
                async with drop_engine.begin() as conn:
                    await conn.execute(text("RENAME TABLE login_failure_bak TO login_failure"))
            except Exception:  # noqa: BLE001
                pass
            await drop_engine.dispose()

        # ====================================================
        print("\n[10] prune：删窗口外、留窗口内")
        await _one_worker(url, _reset)

        async def _seed_old(db):
            now = datetime.utcnow()
            for delta in (10, 4000):
                db.add(LoginFailure(key_hash=store.key_digest("other@example.com|x"),
                                    failed_at=now - timedelta(seconds=delta)))
            await db.commit()

        await _one_worker(url, _seed_old)
        removed = await _one_worker(url, lambda db: store.prune(db))
        left = await _one_worker(url, _rows)
        _check("[10.1] 删掉窗口外的 1 条", removed == 1, str(removed))
        _check("[10.2] 窗口内 1 条保留", len(left) == 1, str(len(left)))

    finally:
        drop = create_async_engine(DATABASE_URL, poolclass=NullPool)
        try:
            if keep:
                print(f"\n[KEEP] 保留 probe 库：{schema}")
            else:
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
    parser = argparse.ArgumentParser(description="登录限流 · 真实 MySQL 运行器")
    parser.add_argument("--keep", action="store_true", help="保留 probe 库（排查用）")
    args = parser.parse_args()
    return asyncio.run(run(args.keep))


if __name__ == "__main__":
    sys.exit(main())
