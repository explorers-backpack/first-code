# -*- coding: utf-8 -*-
"""撤销广播的**轮询器**：每个实例一个后台任务，把「已撤销」推到本进程的连接上。

为什么是后台任务而不是「收到消息时顺便查一下」
------------------------------------------
撤销是**服务端发起**的事件，客户端此时可能正安静地挂着（前端心跳 25s 一次）。
若只在收到消息时复核，最坏延迟 = 心跳间隔 + 节流间隔，正是改造前 61.7s 的来源。
要让它变成秒级，就必须有**服务端自己动**的机制。

负载口径（重要）
----------------
- 轮询周期默认 **2 秒**（``INTERVIEW_WS_REVOCATION_WATCH_INTERVAL``）；
- **每个实例**每周期只发**一条** ``SELECT … WHERE id > ?``（主键范围扫描）；
- 与连接数**完全无关**：1000 条连接也还是每 2 秒一条查询。
  这正是「不能把 30s 复核直接改成 1s」的原因——那种改法会让查询数随
  连接数线性增长，而本方案不会。

消息丢失 / 断线 / 重启
----------------------
- **不会丢**：撤销记录只追加且 id 单调，``id > watermark`` 天然补偿；
- **重启**：从 ``max(id)`` 起步，不重放历史（那时的连接本来也不存在）；
- **存储故障**：只记 WARNING 并继续下一轮，**不退出**；
  兜底是 ``api/interview_ws`` 里的周期性复核（见 ``AUTH_RECHECK_SECONDS``）。

★ 顶层**不 import** 实时通道 / 编排层：断开动作由 :func:`ws_revocation.register_handler`
注册进来的回调完成，避免反向依赖（与 ``interview_production.register_evict_hook`` 同一手法）。
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict, Optional

from database import async_session
from services import ws_revocation

logger = logging.getLogger("careerai.ws_revocation")

#: 默认轮询周期（秒）。2s ⇒ 撤销生效延迟上界 ≈ 2s + 一次查询耗时。
DEFAULT_INTERVAL_SECONDS = 2.0

#: 存储连续失败时，日志的最小间隔（秒）——避免故障期间把日志刷爆。
_LOG_MIN_INTERVAL_SECONDS = 30.0

_TRUE = ("1", "true", "yes", "on")
_FALSE = ("0", "false", "no", "off")


def watch_enabled() -> bool:
    """是否启用后台轮询（``INTERVIEW_WS_REVOCATION_WATCH``）。

    **默认开启**（未配置 / 空串）。关闭后撤销仍会落库，但「主动断开」退化为
    周期性复核（``AUTH_RECHECK_SECONDS``），延迟从秒级回到分钟级——
    因此**生产不建议关闭**，只有回归套件为了确定性才关（见
    ``tests/regression_env.py`` 的钉扎说明）。
    """
    raw = os.getenv("INTERVIEW_WS_REVOCATION_WATCH", "").strip().lower()
    if not raw:
        return True
    if raw in _FALSE:
        return False
    if raw in _TRUE:
        return True
    logger.warning(
        "ws_revocation_watch_invalid_config value=%s fallback=enabled", raw[:32]
    )
    return True


def watch_interval() -> float:
    """轮询周期（秒）。非法 / 过小的值回落到默认值并记 WARNING。"""
    raw = os.getenv("INTERVIEW_WS_REVOCATION_WATCH_INTERVAL", "").strip()
    if not raw:
        return DEFAULT_INTERVAL_SECONDS
    try:
        value = float(raw)
    except ValueError:
        logger.warning(
            "ws_revocation_watch_interval_invalid value=%s fallback=%s",
            raw[:32],
            DEFAULT_INTERVAL_SECONDS,
        )
        return DEFAULT_INTERVAL_SECONDS
    if value < 0.1:
        # 低于 100ms 就不再是「轮询」而是「忙等」，对 DB 不公平
        logger.warning(
            "ws_revocation_watch_interval_too_small value=%s clamp=0.1", value
        )
        return 0.1
    return value


class _State:
    """轮询器状态。刻意用**可变对象**而不是模块级 globals：
    测试里 `reset()` 能一次性清干净，不会因为 `from ... import x` 绑定到旧值。"""

    def __init__(self) -> None:
        self.task: Optional[asyncio.Task] = None
        self.watermark: int = 0
        self.polls: int = 0
        self.delivered: int = 0
        self.closed: int = 0
        self.failures: int = 0
        self.last_error: str = ""
        self._last_log_at: float = 0.0


STATE = _State()


def reset() -> None:
    """清空状态（测试用）。**不**取消已存在的任务——那要走 :func:`stop`。"""
    STATE.watermark = 0
    STATE.polls = 0
    STATE.delivered = 0
    STATE.closed = 0
    STATE.failures = 0
    STATE.last_error = ""
    STATE._last_log_at = 0.0


async def poll_once() -> int:
    """执行一次轮询，返回本次**断开**的连接数。

    存储不可用时**不抛**（记 WARNING 后返回 0）：轮询器是个长期后台任务，
    一次 DB 抖动不该把它杀掉。
    """
    try:
        async with async_session() as db:
            rows = await ws_revocation.fetch_since(db, STATE.watermark)
    except ws_revocation.RevocationStoreUnavailable as exc:
        STATE.failures += 1
        STATE.last_error = str(exc)
        now = asyncio.get_event_loop().time()
        if now - STATE._last_log_at >= _LOG_MIN_INTERVAL_SECONDS:
            STATE._last_log_at = now
            logger.warning(
                "ws_revocation_poll_failed failures=%s error=%s",
                STATE.failures,
                type(exc).__name__,
            )
        return 0

    STATE.polls += 1
    if not rows:
        return 0

    # ★ 先推进水位线再断开：即使下面的回调抛异常，也不会把同一批记录
    #   永远重放（重放的代价是「反复踢同一批连接」，比漏一次更难查）。
    STATE.watermark = max(int(r["id"]) for r in rows)
    STATE.delivered += len(rows)

    handler = ws_revocation.get_handler()
    if handler is None:
        # 没有处理器时**不推进**水位线（上面那行已经推进了，这里退回去），
        # 让下一轮再试——否则这批撤销在本实例上就永久丢了。
        STATE.watermark = min(int(r["id"]) for r in rows) - 1
        logger.warning("ws_revocation_no_handler rows=%s", len(rows))
        return 0

    try:
        result = handler(rows)
        if asyncio.iscoroutine(result):
            result = await result
        closed = int(result or 0)
    except Exception as exc:  # noqa: BLE001 - 断开失败不能让轮询器退出
        logger.warning(
            "ws_revocation_handler_failed rows=%s error=%s",
            len(rows),
            type(exc).__name__,
        )
        return 0
    STATE.closed += closed
    return closed


async def _loop(interval: float) -> None:
    try:
        while True:
            await asyncio.sleep(interval)
            await poll_once()
    except asyncio.CancelledError:  # noqa: TRY302 - 正常停止路径
        raise
    except Exception as exc:  # noqa: BLE001 - 兜底：绝不让任务静默死掉
        logger.error("ws_revocation_watch_crashed error=%s", type(exc).__name__)


async def start(interval: Optional[float] = None) -> bool:
    """启动后台轮询（幂等）。返回是否**本次**真的启动了。

    水位线从 ``max(id)`` 起步：实例启动**之前**发生的撤销不重放
    （那些连接不在本实例上，重放只会误伤刚建立的连接）。
    """
    if STATE.task is not None and not STATE.task.done():
        return False

    try:
        async with async_session() as db:
            STATE.watermark = await ws_revocation.latest_id(db)
    except ws_revocation.RevocationStoreUnavailable as exc:
        # 表还没建 / DB 抖动：从 0 起步。因为表为空，等价于不重放。
        STATE.watermark = 0
        logger.warning(
            "ws_revocation_watermark_unavailable error=%s fallback=0",
            type(exc).__name__,
        )

    effective = watch_interval() if interval is None else float(interval)
    STATE.task = asyncio.create_task(_loop(effective), name="ws-revocation-watch")
    logger.info(
        "ws_revocation_watch_started interval=%ss watermark=%s",
        effective,
        STATE.watermark,
    )
    return True


async def stop() -> None:
    """停止后台轮询（幂等；在 lifespan 关闭时调用）。"""
    task = STATE.task
    STATE.task = None
    if task is None:
        return
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):  # noqa: BLE001 - 停止不应抛
        pass


def snapshot() -> Dict[str, Any]:
    """轮询器当前状态（排障 / 测试用，不含任何凭据）。"""
    return {
        "running": STATE.task is not None and not STATE.task.done(),
        "watermark": STATE.watermark,
        "polls": STATE.polls,
        "delivered": STATE.delivered,
        "closed": STATE.closed,
        "failures": STATE.failures,
        "last_error": STATE.last_error,
    }


__all__ = [
    "DEFAULT_INTERVAL_SECONDS",
    "STATE",
    "poll_once",
    "reset",
    "snapshot",
    "start",
    "stop",
    "watch_enabled",
    "watch_interval",
]
