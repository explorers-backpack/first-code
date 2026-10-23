# -*- coding: utf-8 -*-
"""登录失败限流 · 共享存储（``AUTH_LOGIN_STORE=db``）自检。

这一层证明什么
--------------
``utils/login_throttle``（进程内字典）单 worker 够用，多 worker 时每个进程
独立计数 ⇒ 实际允许次数 = 上限 × 进程数（**安全项被放大**）。
本套件证明共享表实现：

1. 滑窗语义与旧实现**逐字等价**（含 ``Retry-After`` 的 ``+1`` 向上取整）；
2. 只存 ``sha256`` 摘要，**不落明文邮箱 / IP**；
3. 两个**独立连接**看到同一份计数（跨实例）；
4. 存储不可用时 **fail closed**（抛 ``LoginThrottleUnavailable``，不静默放行）；
5. ``main.login`` 真的接到了新实现上（**不是只改签名不改调用**）。

★ 不能证明什么：真实 InnoDB 下的并发计数上界。那需要真实 MySQL，
见 ``tests/login_throttle_mysql_run.py``（不进回归循环）。
"""

import asyncio
import os
import pathlib
import sys
import tempfile
from datetime import datetime, timedelta

_TMP = tempfile.mkdtemp(prefix="career_throttle_")
_SHARED_DB = pathlib.Path(_TMP) / "shared.db"
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import regression_env  # noqa: E402,F401

from sqlalchemy import select, text, update  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401
from database import Base  # noqa: E402
from models import LoginFailure  # noqa: E402
from services import login_throttle_store as store  # noqa: E402

_PASSED = 0
_FAILED = 0

MEM_DB = "sqlite+aiosqlite:///:memory:"
SHARED_URL = f"sqlite+aiosqlite:///{_SHARED_DB.as_posix()}"


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name
          + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def _set_env(**kwargs) -> None:
    for key, value in kwargs.items():
        os.environ[key] = value


async def _run_with(url: str, fn):
    """独立 engine + 独立会话 = 一个"实例"。"""
    engine = create_async_engine(url)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with factory() as db:
            return await fn(db)
    finally:
        await engine.dispose()


def _call(url: str, fn):
    return asyncio.run(_run_with(url, fn))


async def _main_engine():
    engine = create_async_engine(
        MEM_DB, poolclass=StaticPool, connect_args={"check_same_thread": False}
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return engine


def run() -> bool:
    print("=" * 70)
    print("登录失败限流 · 共享存储自检")
    print("=" * 70)

    engine = asyncio.run(_main_engine())
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    def db_call(fn):
        async def _inner():
            async with factory() as db:
                return await fn(db)

        return asyncio.run(_inner())

    # ========================================================
    print("\n[1] 后端开关：默认 memory、空串 memory、db、非法值不静默")
    os.environ["AUTH_LOGIN_STORE"] = ""
    _check("[1.1] 空串（未配置）⇒ memory（行为不变）", store.store_mode() == "memory",
           store.store_mode())
    os.environ.pop("AUTH_LOGIN_STORE", None)
    _check("[1.2] 键不存在 ⇒ memory", store.store_mode() == "memory", store.store_mode())
    os.environ["AUTH_LOGIN_STORE"] = "db"
    _check("[1.3] 'db' ⇒ db", store.store_mode() == "db", store.store_mode())
    _check("[1.4] use_shared_store() 与 store_mode() 一致", store.use_shared_store() is True)
    os.environ["AUTH_LOGIN_STORE"] = "DB"
    _check("[1.5] 大小写不敏感", store.store_mode() == "db", store.store_mode())
    os.environ["AUTH_LOGIN_STORE"] = "redis"
    _check("[1.6] ★ 非法值回退 memory（不静默接受，但也不炸）",
           store.store_mode() == "memory", store.store_mode())

    # ========================================================
    print("\n[2] 键摘要：不落明文")
    os.environ["AUTH_LOGIN_STORE"] = "db"
    raw_key = "alice@example.com|10.0.0.7"
    digest = store.key_digest(raw_key)
    _check("[2.1] 64 位十六进制", len(digest) == 64 and all(c in "0123456789abcdef" for c in digest),
           digest)
    _check("[2.2] ★ 摘要不含明文邮箱 / IP",
           "alice" not in digest and "10.0.0.7" not in digest, digest)
    _check("[2.3] 同一输入稳定", store.key_digest(raw_key) == digest)
    _check("[2.4] 不同输入不同",
           store.key_digest("alice@example.com|10.0.0.8") != digest)

    # ========================================================
    print("\n[3] memory 模式：转调旧实现，**不写库**")
    os.environ["AUTH_LOGIN_STORE"] = "memory"
    from utils import login_throttle as memory_impl

    memory_impl.reset(raw_key)

    async def _mem_flow(db):
        before = await store.check(db, raw_key)
        for _ in range(memory_impl.MAX_FAILURES):
            await store.record(db, raw_key)
        after = await store.check(db, raw_key)
        rows = (await db.execute(select(LoginFailure))).scalars().all()
        return before, after, len(rows)

    m_before, m_after, m_rows = db_call(_mem_flow)
    _check("[3.1] 初始放行", m_before == 0, str(m_before))
    _check("[3.2] ★ memory 模式确实被限流（真的接到旧实现上）", m_after > 0, str(m_after))
    _check("[3.3] ★ memory 模式一行都没写库", m_rows == 0, str(m_rows))
    memory_impl.reset(raw_key)

    # ========================================================
    print("\n[4] db 模式：滑窗计数 + Retry-After")
    os.environ["AUTH_LOGIN_STORE"] = "db"
    os.environ["AUTH_LOGIN_MAX_FAILURES"] = "5"
    os.environ["AUTH_LOGIN_WINDOW_SECONDS"] = "900"

    async def _db_flow(db):
        out = {"check0": await store.check(db, raw_key)}
        out["checks"] = []
        for _ in range(5):
            await store.record(db, raw_key)
            out["checks"].append(await store.check(db, raw_key))
        rows = (await db.execute(select(LoginFailure))).scalars().all()
        out["rows"] = len(rows)
        out["stored"] = {r.key_hash for r in rows}
        return out

    d = db_call(_db_flow)
    _check("[4.1] 初始放行", d["check0"] == 0, str(d["check0"]))
    _check("[4.2] ★ 前 4 次失败后仍放行（第 5 次才算满）",
           d["checks"][:4] == [0, 0, 0, 0], str(d["checks"]))
    _check("[4.3] ★★ 第 5 次失败后开始拦截", d["checks"][4] > 0, str(d["checks"]))
    _check("[4.4] Retry-After 在 (0, 窗口] 内", 0 < d["checks"][4] <= 900,
           str(d["checks"][4]))
    _check("[4.5] 恰好 5 行失败记录", d["rows"] == 5, str(d["rows"]))
    _check("[4.6] ★★ 库里只有摘要，没有明文",
           d["stored"] == {digest}, str(d["stored"]))
    _check("[4.7] ★ 库里没有任何含邮箱的字符串",
           all("alice" not in (r or "") for r in d["stored"]), str(d["stored"]))

    # ========================================================
    print("\n[4b] ★ Retry-After 钳制：时间戳晚于 now 快照也不会超过窗口")
    print("     （check 先取 now 再查询，另一实例可能在这之后写入 ⇒ 不钳会算出 901）")
    future_key = "future@example.com|127.0.0.1"
    future_digest = store.key_digest(future_key)

    async def _seed_future(db):
        now = datetime.utcnow()
        for _ in range(5):
            db.add(LoginFailure(key_hash=future_digest,
                                failed_at=now + timedelta(seconds=5)))
        await db.commit()

    async def _drop_future(db):
        await db.execute(
            LoginFailure.__table__.delete().where(
                LoginFailure.key_hash == future_digest)
        )
        await db.commit()

    db_call(_seed_future)
    ra_future = db_call(lambda db: store.check(db, future_key))
    _check("[4b.1] ★ 未来时间戳 ⇒ 仍被拦（>0）", ra_future > 0, str(ra_future))
    _check("[4b.2] ★★ Retry-After 被钳到窗口上限 900", ra_future <= 900, str(ra_future))
    db_call(_drop_future)

    # ========================================================
    print("\n[5] 窗口过期：老记录不再计数")
    async def _age(db):
        await db.execute(
            update(LoginFailure).values(
                failed_at=datetime.utcnow() - timedelta(seconds=901))
        )
        await db.commit()

    db_call(_age)
    _check("[5.1] ★ 全部滑出窗口后重新放行", db_call(lambda db: store.check(db, raw_key)) == 0)
    _check("[5.2] 过期行仍在表里（由 record/prune 机会式清理）",
           db_call(lambda db: _count(db)) == 5, str(db_call(lambda db: _count(db))))

    # ========================================================
    print("\n[6] reset：登录成功清空计数")
    async def _refill(db):
        await db.execute(text("DELETE FROM login_failure"))
        await db.commit()
        for _ in range(5):
            await store.record(db, raw_key)

    db_call(_refill)
    _check("[6.0] 前置：已满 5 次", db_call(lambda db: store.check(db, raw_key)) > 0)
    db_call(lambda db: store.reset(db, raw_key))
    _check("[6.1] ★ reset 后放行", db_call(lambda db: store.check(db, raw_key)) == 0)
    _check("[6.2] 该键的行被删干净", db_call(lambda db: _count(db)) == 0,
           str(db_call(lambda db: _count(db))))

    # ========================================================
    print("\n[7] ★ 跨实例：两个独立连接看到同一份计数")
    async def _seed_shared():
        engine2 = create_async_engine(SHARED_URL)
        try:
            async with engine2.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        finally:
            await engine2.dispose()

    asyncio.run(_seed_shared())
    # 实例 A 记 5 次
    for _ in range(5):
        _call(SHARED_URL, lambda db: store.record(db, raw_key))
    # 实例 B（另一个 engine/连接）必须看到
    cross = _call(SHARED_URL, lambda db: store.check(db, raw_key))
    _check("[7.1] ★★ 实例 A 记的失败，实例 B 读得到（共享计数）", cross > 0, str(cross))
    _call(SHARED_URL, lambda db: store.reset(db, raw_key))
    _check("[7.2] reset 也是共享的（B 重置后 A 也放行）",
           _call(SHARED_URL, lambda db: store.check(db, raw_key)) == 0)

    # ========================================================
    print("\n[8] 存储不可用：fail closed（不静默放行）")
    async def _drop(db):
        await db.execute(text("DROP TABLE login_failure"))
        await db.commit()

    db_call(_drop)

    async def _expect_raise(db, fn):
        try:
            await fn(db)
        except store.LoginThrottleUnavailable:
            return "RAISED"
        return "PASSED"

    _check("[8.1] ★★ check 抛 LoginThrottleUnavailable（不是返回 0）",
           db_call(lambda db: _expect_raise(db, lambda d: store.check(d, raw_key))) == "RAISED")
    _check("[8.2] ★★ record 同样抛（失败无法记账就不能假装记上了）",
           db_call(lambda db: _expect_raise(db, lambda d: store.record(d, raw_key))) == "RAISED")
    _check("[8.3] reset 只记 WARNING、不抛（清不掉计数不阻断登录成功）",
           db_call(lambda db: _expect_raise(db, lambda d: store.reset(d, raw_key))) == "PASSED")
    _check("[8.4] prune 失败返回 0（机会式维护不打断主流程）",
           db_call(lambda db: store.prune(db)) == 0)

    # ========================================================
    print("\n[9] prune：删窗口外、留窗口内")
    async def _rebuild(db):
        await db.execute(text(
            "CREATE TABLE login_failure ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "key_hash VARCHAR(64) NOT NULL, "
            "failed_at DATETIME NOT NULL)"
        ))
        now = datetime.utcnow()
        for delta in (10, 3000, 8000):
            db.add(LoginFailure(key_hash=digest,
                                failed_at=now - timedelta(seconds=delta)))
        await db.commit()

    db_call(_rebuild)
    removed = db_call(lambda db: store.prune(db))
    left = db_call(lambda db: _count(db))
    _check("[9.1] 删掉 2 条窗口外的", removed == 2, str(removed))
    _check("[9.2] 窗口内 1 条保留", left == 1, str(left))

    # ========================================================
    print("\n[10] ★ 并发突发：内存实现与共享实现**同样是 check-then-act**")
    print("     判据不是「谁更严」，而是「共享实现有没有比原实现更差」。")
    print("     这里用 asyncio.gather 模拟 20 个请求在 check 与 record 之间交错")
    print("     （真实路由中间隔着 bcrypt 与一次 await，交错是必然的）。")

    async def _burst(mode: str, key: str, n: int = 20, reset: bool = True):
        os.environ["AUTH_LOGIN_STORE"] = mode
        if reset:
            memory_impl.reset(key)
            await _clear_db()

        async def one():
            # 1) 判定（内存实现是同步的，共享实现要 await）
            if mode == "memory":
                blocked = memory_impl.check(key)
            else:
                blocked = await _check_db(key)
            if blocked:
                return 0
            # 2) 模拟 bcrypt + 用户查询这段"占位时间"：让其它请求插进来
            await asyncio.sleep(0.01)
            # 3) 记账
            if mode == "memory":
                memory_impl.record(key)
            else:
                await _record_db(key)
            return 1

        admitted = sum(await asyncio.gather(*[one() for _ in range(n)]))
        return admitted, await _count_db()

    async def _clear_db():
        async with factory() as db:
            await db.execute(text("DELETE FROM login_failure"))
            await db.commit()

    async def _check_db(key):
        async with factory() as db:
            return await store.check(db, key)

    async def _record_db(key):
        async with factory() as db:
            await store.record(db, key)

    async def _count_db():
        async with factory() as db:
            return len((await db.execute(select(LoginFailure))).scalars().all())

    burst_key = "burst@example.com|127.0.0.1"
    mem_admitted, _mem_rows = asyncio.run(_burst("memory", burst_key))
    db_admitted, db_rows = asyncio.run(_burst("db", burst_key))

    print(f"     内存实现：20 并发中放行 {mem_admitted} 次（只记内存，不落库）")
    print(f"     共享实现：20 并发中放行 {db_admitted} 次（落库 {db_rows} 行）")
    _check("[10.1] ★ 内存实现同样会突破上限（说明 check-then-act 是既有语义）",
           mem_admitted > memory_impl.MAX_FAILURES, str(mem_admitted))
    _check("[10.2] ★★ 共享实现的溢出量与内存实现**同量级**（不是本次引入的退化）",
           db_admitted <= mem_admitted + 2,
           f"mem={mem_admitted} db={db_admitted}")
    _check("[10.3] 共享实现的落库行数与放行次数一致",
           db_rows == db_admitted, f"rows={db_rows} admitted={db_admitted}")

    # ★ 真正的保证在这里：突发过后，**同一窗口内再也放不进来**（不 reset 再来一波）。
    _check("[10.4] ★★ 突发之后的下一个请求被拦（窗口内不再放行）",
           asyncio.run(_burst("db", burst_key, n=1, reset=False))[0] == 0)
    second_wave, rows_after = asyncio.run(_burst("db", burst_key, reset=False))
    _check("[10.5] ★★ 持续攻击被全局封住：第二波 20 并发一个都放不进来",
           second_wave == 0, str(second_wave))
    _check("[10.6] 第二波没有新增行", rows_after == db_rows,
           f"{db_rows} -> {rows_after}")

    os.environ["AUTH_LOGIN_STORE"] = "db"

    # ========================================================
    print("\n[11] 接线守卫：登录用例真的调了新实现（不是只改签名）")
    # ★ 登录用例已随认证模块迁到 modules/auth/service.py（原 main.login）。
    #   判据的**意图**不变：仍要求「真的调了带 db 的新签名，且 await 了结果」。
    #   同时新增一条反向断言：入口文件里不许再出现登录限流（防止有人复制回去）。
    src = (_BACKEND / "main.py").read_text(encoding="utf-8")
    auth_src = (_BACKEND / "modules" / "auth" / "service.py").read_text(encoding="utf-8")
    # ★ 状态上报函数在 P4 整体迁到 common/health.py（原 main.py 242-661 行），
    #   所以「源码里有这段实现」要在新位置找；main.py 只剩 lifespan 的调用点。
    health_src = (_BACKEND / "common" / "health.py").read_text(encoding="utf-8")
    _check("[11.1] ★ 认证用例从 services.login_throttle_store 导入",
           "from services.login_throttle_store import" in auth_src)
    _check("[11.2] ★ 认证用例不再直接导入 utils.login_throttle",
           "from utils.login_throttle import" not in auth_src)
    _check("[11.2b] ★★ 入口文件 main.py 里**不再**出现登录限流的调用（已归属 auth 模块）",
           "throttle_check" not in src and "throttle_record" not in src
           and "throttle_reset" not in src)
    _check("[11.3] ★★ 三个动作都带 db 参数（旧签名只有一个参数）",
           "throttle_check(db, throttle_key)" in auth_src
           and "throttle_record(db, throttle_key)" in auth_src
           and "throttle_reset(db, throttle_key)" in auth_src)
    _check("[11.4] ★ 三个动作都被 await（不是调了个协程就丢掉）",
           "await _throttle(throttle_check(db, throttle_key))" in auth_src
           and "await _throttle(throttle_record(db, throttle_key))" in auth_src
           and "await throttle_reset(db, throttle_key)" in auth_src)
    _check("[11.5] 存储不可用映射成 503（不混进 401 / 429）",
           "status_code=503" in auth_src and "LoginThrottleUnavailable" in auth_src)

    # ========================================================
    print("\n[12] 启动日志会暴露后端（否则多 worker 后限流被放大都没人知道）")
    report_src = health_src[health_src.find("def _report_auth_throttle_status"):]
    report_src = report_src[: report_src.find("def _report_avatar_status")]
    _check("[12.1] 存在 _report_auth_throttle_status", bool(report_src.strip()))
    _check("[12.2] ★ 进程内模式会打出「上限 × 进程数」的警告",
           "上限 × 进程数" in report_src)
    _check("[12.3] 并给出 AUTH_LOGIN_STORE=db 的处置建议",
           "AUTH_LOGIN_STORE=db" in report_src)
    _check("[12.4] lifespan 真的调用了它", "_report_auth_throttle_status()" in src)

    asyncio.run(engine.dispose())

    # 清理临时目录（SQLite 文件）
    for p in pathlib.Path(_TMP).glob("*"):
        try:
            p.unlink()
        except OSError:
            pass

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


async def _count(db) -> int:
    return len((await db.execute(select(LoginFailure))).scalars().all())


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
