# -*- coding: utf-8 -*-
"""登录失败限流 · **共享存储**实现（多 worker 安全）。

要解决的问题
------------
登录爆破限流原先只有 ``utils/login_throttle`` 的**进程内**字典实现：

========================  ============================================
部署                       实际允许的失败次数
========================  ============================================
1 个 worker                ``AUTH_LOGIN_MAX_FAILURES``（默认 5）
N 个 worker（同一账号）      **5 × N** —— 攻击者只要让请求散到各进程
========================  ============================================

这不是「体验问题」，是**安全项被静默放大**。``worker_guard`` 至今仍强制单
worker，主因就在这里（见 ``CONCURRENCY_CONSISTENCY_REPORT.md`` §9.6）。

本模块把「谁在什么时候失败过」放到**共享**的数据库里，任何实例看到同一份计数。
默认仍然是内存实现（``AUTH_LOGIN_STORE`` 不配 ⇒ 行为**逐字不变**），
需要多 worker 时显式设 ``AUTH_LOGIN_STORE=db``。

为什么是「一条失败一行」
------------------------
见 ``models.user.LoginFailure`` 的文档：滑窗语义与 ``deque`` 实现**逐字等价**、
只在失败时写、过期行按 ``failed_at`` 索引删。**没有**用固定窗口计数器——
固定窗口在边界处会出现双倍突发（第 900 秒 5 次 + 第 901 秒 5 次）。

★ 墙钟而非 ``monotonic``
-------------------------
原实现用 ``time.monotonic()``。它只在**单个进程的生命周期内**单调，跨实例 /
跨重启不可比。共享存储必须用 ``datetime.utcnow()``（UTC 墙钟），并假设各实例
时钟已同步（NTP）。时钟漂移会让窗口略微变宽/变窄，不会让限流失效。

已知边界（诚实清单）
--------------------
1. **check-then-act**：``check`` 与 ``record`` 之间不是原子的。**实测**
   （``tests/login_throttle_mysql_run.py [5]``，20 个独立连接同时发起）：
   单次突发 20 个请求**全部放行**，即上界是 ``5 + 并发数``，不是 5。
   ★ 这**不是**本次引入的退化：内存实现在同样的交错下也是 20 个全放行
   （``tests/test_login_throttle_store.py [10]`` 有对照）。
   真正的保证是**持续攻击被全局封住**：突发之后同一窗口内第二波 20 并发
   **一个都放不进来**（实测 0），且计数跨实例、跨重启都一致。
   为什么不加行锁：``SELECT ... FOR UPDATE`` 在**空表**上取的是间隙锁，
   会把**所有账号**的登录串行化（不是只串行化同一个 key），代价远大于收益。
   要彻底消掉溢出需要改成「验证密码前先原子占位」（upsert 计数器），
   属于后续可选优化，不在本次范围。
2. 存储不可用时 **fail closed**（抛 :class:`LoginThrottleUnavailable` →
   路由返回 503）。理由：用户查询本来就要读同一个库，库不可用时登录本来就
   不可能成功；此时"放行"只会让限流被绕过。
3. 多实例时钟需同步（NTP）；偏差 < 窗口（默认 900s）时不影响判定。
4. ``record`` **不做**过期行清理（只 INSERT）。曾经的写法「先 DELETE 再 INSERT」
   会让同一 key 的并发失败登录互相等**间隙锁**，实测 8 个里 7 个死锁 ⇒ 登录 503。
   过期行由 ``check`` 的 ``failed_at >= cutoff`` 过滤掉，清理交给 ``prune``。
"""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timedelta
from typing import Tuple

from sqlalchemy import delete, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from models import LoginFailure

logger = logging.getLogger("careerai.login_throttle")

#: 共享存储模式下，每这么多次 ``record`` 顺手做一次**全局**过期清理。
#: 单键的过期行每次 ``record`` / ``check`` 都会顺带删掉，这里是防「海量不同
#: 邮箱」把表撑大的兜底。
PRUNE_EVERY = 50

MODE_MEMORY = "memory"
MODE_DB = "db"
MODES = (MODE_MEMORY, MODE_DB)
DEFAULT_MODE = MODE_MEMORY

_PRUNE_CALLS = 0


class LoginThrottleUnavailable(RuntimeError):
    """限流存储不可用（表缺失 / 连接失败）。

    调用方**不得**把它当成「没有被限流」而放行——那是 fail open，
    等于在数据库抖动期间把爆破保护关掉。
    """


# ============================================================
# 一、配置（**调用期**读取，便于测试切换；空串 = 未配置 = 默认）
# ============================================================
def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        logger.warning("login_throttle_bad_int name=%s value=%r", name, raw)
        return default
    if value <= 0:
        logger.warning("login_throttle_non_positive name=%s value=%s", name, value)
        return default
    return value


def limits() -> Tuple[int, int]:
    """``(最多失败次数, 窗口秒数)``。与 ``utils.login_throttle`` 同名同默认。"""
    return (
        _env_int("AUTH_LOGIN_MAX_FAILURES", 5),
        _env_int("AUTH_LOGIN_WINDOW_SECONDS", 900),
    )


def store_mode() -> str:
    """当前限流存储后端：``memory``（默认）/ ``db``。

    非法值**不静默**：记 ERROR 后按默认（memory）走——与项目里其它开关
    （``AVATAR_PROVIDER`` / ``INTERVIEW_WS_TOKEN_AUTH``）同一口径。
    """
    raw = os.getenv("AUTH_LOGIN_STORE", "").strip().lower()
    if not raw:
        return DEFAULT_MODE
    if raw in MODES:
        return raw
    logger.error(
        "login_throttle_unknown_store value=%r fallback=%s allowed=%s",
        raw, DEFAULT_MODE, ",".join(MODES),
    )
    return DEFAULT_MODE


def use_shared_store() -> bool:
    return store_mode() == MODE_DB


# ============================================================
# 二、键的摘要（不落明文邮箱 / IP）
# ============================================================
def key_digest(key: str) -> str:
    """``sha256("<email>|<ip>")`` → 64 位十六进制。

    ★ 与 ``utils.login_throttle`` 用**同一个原始 key**（``main.login`` 拼的
      ``f"{email}|{ip}"``），只是入库前摘要一次。
    """
    return hashlib.sha256(str(key).encode("utf-8")).hexdigest()


# ============================================================
# 三、对外的三个动作（异步；内存模式会转调旧实现）
# ============================================================
async def check(db: AsyncSession, key: str) -> int:
    """是否被限流：``0`` 放行，``>0`` 为建议的 ``Retry-After`` 秒数。

    语义与 ``utils.login_throttle.check`` **逐字对齐**（含 ``+1`` 向上取整），
    只是计数来源换成共享表。
    """
    if not use_shared_store():
        from utils.login_throttle import check as memory_check

        return memory_check(key)

    max_failures, window_seconds = limits()
    now = datetime.utcnow()
    cutoff = now - timedelta(seconds=window_seconds)
    try:
        result = await db.execute(
            select(LoginFailure.failed_at)
            .where(
                LoginFailure.key_hash == key_digest(key),
                LoginFailure.failed_at >= cutoff,
            )
            .order_by(LoginFailure.failed_at.asc())
        )
        stamps = list(result.scalars().all())
    except SQLAlchemyError as exc:
        logger.error("login_throttle_check_failed error=%s", type(exc).__name__)
        raise LoginThrottleUnavailable(
            f"登录限流存储不可用：{type(exc).__name__}"
        ) from exc

    if len(stamps) < max_failures:
        return 0
    oldest = stamps[0]
    # ★ 钳到窗口上限：``now`` 是**查询之前**取的快照，而 ``oldest`` 可能是
    #   另一个实例在这个快照之后刚写进来的（failed_at > now）⇒ 不钳的话
    #   Retry-After 会算出 901（窗口 900），客户端要多等 1 秒。
    #   重复运行本套件时这条会偶发暴露，所以必须钳。
    retry_after = int((oldest + timedelta(seconds=window_seconds) - now).total_seconds()) + 1
    return max(1, min(retry_after, window_seconds))


async def record(db: AsyncSession, key: str) -> None:
    """记录一次登录失败（**自行 commit**）。

    ★ 为什么要自己 commit：失败路径上路由紧接着就 ``raise HTTPException``，
    ``get_db`` 在请求结束时关闭会话 ⇒ 未提交的插入会被回滚，计数永远不增长。
    失败路径上会话里没有别的待提交变更（此前只有 SELECT），因此这里的
    commit 不会顺手提交业务写入。
    """
    if not use_shared_store():
        from utils.login_throttle import record as memory_record

        memory_record(key)
        return

    global _PRUNE_CALLS
    _, window_seconds = limits()
    now = datetime.utcnow()
    digest = key_digest(key)
    try:
        # ★ 这里**只 INSERT**，不顺手 DELETE 过期行。
        #   曾经的写法是「先 DELETE 本键的过期行、再 INSERT」，8 个并发失败登录
        #   会在同一 key 的空索引区间上互相等待**间隙锁**，实测 8 个里 7 个
        #   死锁 ⇒ 抛 LoginThrottleUnavailable ⇒ 登录 503。
        #   过期行本来就被 ``check`` 的 ``failed_at >= cutoff`` 过滤掉，留着无害；
        #   清理交给 ``prune``（机会式，失败只记 WARNING）。
        #   ``tests/login_throttle_mysql_run.py [3]`` 就是这条的回归。
        db.add(LoginFailure(key_hash=digest, failed_at=now))
        await db.commit()
    except SQLAlchemyError as exc:
        try:
            await db.rollback()
        except Exception:  # noqa: BLE001 - 回滚失败不改变"失败"这个结论
            pass
        logger.error("login_throttle_record_failed error=%s", type(exc).__name__)
        raise LoginThrottleUnavailable(
            f"登录限流存储不可用：{type(exc).__name__}"
        ) from exc

    _PRUNE_CALLS += 1
    if _PRUNE_CALLS % PRUNE_EVERY == 0:
        # 机会式全局清理：失败只记 WARNING，不影响本次登录
        try:
            await prune(db, window_seconds=window_seconds)
        except LoginThrottleUnavailable:
            pass


async def reset(db: AsyncSession, key: str) -> None:
    """登录成功后清除该键的计数（**自行 commit**，理由同 :func:`record`）。"""
    if not use_shared_store():
        from utils.login_throttle import reset as memory_reset

        memory_reset(key)
        return

    try:
        await db.execute(
            delete(LoginFailure).where(LoginFailure.key_hash == key_digest(key))
        )
        await db.commit()
    except SQLAlchemyError as exc:
        try:
            await db.rollback()
        except Exception:  # noqa: BLE001
            pass
        # 清不掉计数只是"多拦一会儿"，**不阻断登录成功**
        logger.warning("login_throttle_reset_failed error=%s", type(exc).__name__)


# ============================================================
# 四、维护（机会式；不承担正确性）
# ============================================================
async def prune(
    db: AsyncSession, *, window_seconds: int = 0, now: datetime | None = None
) -> int:
    """删除窗口外的失败记录，返回删除条数。失败只记 WARNING 并返回 0。"""
    if not use_shared_store():
        return 0
    if window_seconds <= 0:
        window_seconds = limits()[1]
    now = now or datetime.utcnow()
    cutoff = now - timedelta(seconds=int(window_seconds))
    try:
        result = await db.execute(
            delete(LoginFailure).where(LoginFailure.failed_at < cutoff)
        )
        await db.commit()
    except SQLAlchemyError as exc:
        try:
            await db.rollback()
        except Exception:  # noqa: BLE001
            pass
        logger.warning("login_throttle_prune_failed error=%s", type(exc).__name__)
        return 0
    return int(result.rowcount or 0)


__all__ = [
    "DEFAULT_MODE",
    "MODES",
    "MODE_DB",
    "MODE_MEMORY",
    "PRUNE_EVERY",
    "LoginThrottleUnavailable",
    "check",
    "key_digest",
    "limits",
    "prune",
    "record",
    "reset",
    "store_mode",
    "use_shared_store",
]
