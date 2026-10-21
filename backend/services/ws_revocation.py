# -*- coding: utf-8 -*-
"""「会话已撤销」的**跨实例广播**（实时通道主动断开用）。

要解决的问题
------------
撤销动作发生在**某一个**实例上（管理员踢下线 / 登出），而受影响的 WS 连接
可能长在**另一个**实例上。进程内的字典 / asyncio 事件 / 回调**到不了**那边。

改造前的实测（``tests/_probe_revoke_latency.py``，见报告 §10）：

============================  ==========================================
路径                           撤销后已建立连接的实际失效时间
============================  ==========================================
ticket（前端在用）              **>20s 预算内完全不生效**（上界是空闲超时 300s，
                                而前端每 25s 有心跳 ⇒ 实际上**永不**失效）
token（历史兼容）               **61.7s**（60s 复核节流 + 依赖"下一条消息"）
============================  ==========================================

为什么用「只追加的表 + 每实例一个轮询器」而不是消息队列
----------------------------------------------------
1. 项目里**没有** Redis / MQ（选型理由见 ``ws_ticket_store`` 模块文档），
   为「踢下线」引入一个有状态中间件不成比例；
2. 只追加 + 单调自增主键 ⇒ **不会丢消息**：某实例错过一次轮询，下次用
   ``id > watermark`` 补齐。这是 pub/sub 做不到的（丢了就丢了）；
3. 实例重启从 ``max(id)`` 起步，不重放历史撤销（那时的连接本来也不存在）；
4. ★ **一条查询/实例/轮询周期**，与连接数**无关**。不是「每条连接每秒查一次库」
   —— 那才是把 30s 轮询改 1s 会造成的负载问题。

能力边界
--------
本模块**只负责「广播 + 读」**，不 import 实时通道、不碰编排层：
断开动作由调用方通过 :func:`register_handler` 注册的回调完成
（与 ``interview_production.register_evict_hook`` 同一手法，避免反向依赖）。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy import delete, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from models import WsRevocation

logger = logging.getLogger("careerai.ws_revocation")

#: 撤销行的保留时长（秒）。只为排障留痕，不承担任何正确性。
RETENTION_SECONDS = 3600

#: 单次轮询最多取多少行（防止一次异常写入把内存打爆；剩余的下轮再取）。
FETCH_BATCH = 500

#: 稳定的事件原因标签（可被日志系统聚合，不要写自由文本）。
REASON_ADMIN_KICK = "admin_kick"
REASON_LOGOUT = "logout"


class RevocationStoreUnavailable(RuntimeError):
    """撤销广播存储不可用（表缺失 / 连接失败）。

    调用方**不得**把它当成「没人被撤销」而静默继续——但也**不得**因此阻断
    撤销主流程：主效应（``user_session.revoked_at``）已经落库，
    这里的失败只影响「主动断开有多快」，由周期性复核兜底。
    """


# ============================================================
# 一、纯函数（可单测，无 IO）
# ============================================================
def matches(
    row: Dict[str, Any], *, user_id: int, auth_session_id: Optional[int]
) -> bool:
    """一条撤销记录是否命中某条连接。

    口径（**只从严，不从宽**）：

    - ``row.auth_session_id`` 非空 ⇒ 必须与连接的登录会话**相等**才命中
      （登出只踢自己那一条，不影响同一用户的其他设备）；
    - ``row.auth_session_id`` 为空 ⇒ 命中该用户的**所有**连接（管理员踢下线）；
    - ``row.user_id`` 必须与连接的用户一致。

    ★ 连接侧 ``auth_session_id`` 为空（历史 ticket / 无 Authorization 头）时，
      **只有**「按用户撤销」能命中；「按会话撤销」不会误伤它（因为它本来就
      无法证明自己是哪一条会话）。
    """
    if int(row.get("user_id") or 0) != int(user_id):
        return False
    target = row.get("auth_session_id")
    if target is None:
        return True
    return auth_session_id is not None and int(target) == int(auth_session_id)


# ============================================================
# 二、写入（撤销发生时调用一次）
# ============================================================
async def record(
    db: AsyncSession,
    *,
    user_id: int,
    auth_session_id: Optional[int] = None,
    reason: str = "",
    now: Optional[datetime] = None,
) -> int:
    """追加一条撤销广播，返回新行 id。

    存储不可用时抛 :class:`RevocationStoreUnavailable`——但**调用方不应因此
    回滚撤销本身**（``user_session.revoked_at`` 才是主效应）。
    """
    now = now or datetime.utcnow()
    row = WsRevocation(
        user_id=int(user_id),
        auth_session_id=None if auth_session_id is None else int(auth_session_id),
        reason=str(reason or "")[:32],
        created_at=now,
    )
    db.add(row)
    try:
        await db.commit()
    except SQLAlchemyError as exc:
        try:
            await db.rollback()
        except Exception:  # noqa: BLE001 - 回滚失败不改变"失败"这个结论
            pass
        logger.error(
            "ws_revocation_record_failed user_id=%s error=%s",
            user_id,
            type(exc).__name__,
        )
        raise RevocationStoreUnavailable(
            f"撤销广播存储不可用：{type(exc).__name__}"
        ) from exc
    return int(row.id)


# ============================================================
# 三、读取（每个实例的轮询器调用）
# ============================================================
async def latest_id(db: AsyncSession) -> int:
    """当前最大 id（实例启动时的水位线：不重放历史）。"""
    try:
        result = await db.execute(select(func.max(WsRevocation.id)))
        value = result.scalar()
    except SQLAlchemyError as exc:
        raise RevocationStoreUnavailable(
            f"撤销广播存储不可用：{type(exc).__name__}"
        ) from exc
    return int(value or 0)


async def fetch_since(
    db: AsyncSession, after_id: int, *, limit: int = FETCH_BATCH
) -> List[Dict[str, Any]]:
    """取 ``id > after_id`` 的撤销记录（按 id 升序，最多 ``limit`` 条）。"""
    try:
        result = await db.execute(
            select(
                WsRevocation.id,
                WsRevocation.user_id,
                WsRevocation.auth_session_id,
                WsRevocation.reason,
            )
            .where(WsRevocation.id > int(after_id))
            .order_by(WsRevocation.id.asc())
            .limit(int(limit))
        )
        rows = result.all()
    except SQLAlchemyError as exc:
        raise RevocationStoreUnavailable(
            f"撤销广播存储不可用：{type(exc).__name__}"
        ) from exc
    # ★ 立刻转成普通 dict：ORM Row 一旦离开 session 生命周期就不可靠
    return [
        {
            "id": int(r[0]),
            "user_id": int(r[1]),
            "auth_session_id": None if r[2] is None else int(r[2]),
            "reason": r[3] or "",
        }
        for r in rows
    ]


async def prune(
    db: AsyncSession, *, now: Optional[datetime] = None, keep_seconds: int = RETENTION_SECONDS
) -> int:
    """删除过老的撤销记录，返回删除条数（机会式维护，失败只记 WARNING）。"""
    now = now or datetime.utcnow()
    cutoff = now - timedelta(seconds=int(keep_seconds))
    try:
        result = await db.execute(delete(WsRevocation).where(WsRevocation.created_at < cutoff))
        await db.commit()
    except SQLAlchemyError as exc:
        try:
            await db.rollback()
        except Exception:  # noqa: BLE001
            pass
        logger.warning("ws_revocation_prune_failed error=%s", type(exc).__name__)
        return 0
    return int(result.rowcount or 0)


# ============================================================
# 四、回调注册（避免反向依赖实时通道）
# ============================================================
#: 处理一批撤销记录的回调：``fn(rows) -> 被断开的连接数``。
_HANDLER: Optional[Callable[[List[Dict[str, Any]]], Any]] = None


def register_handler(fn: Optional[Callable[[List[Dict[str, Any]]], Any]]) -> None:
    """注册「拿到撤销记录后怎么断开连接」的回调（幂等，后注册覆盖先注册）。"""
    global _HANDLER
    _HANDLER = fn


def get_handler() -> Optional[Callable[[List[Dict[str, Any]]], Any]]:
    """当前已注册的回调（供轮询器与测试读取）。"""
    return _HANDLER


__all__ = [
    "FETCH_BATCH",
    "REASON_ADMIN_KICK",
    "REASON_LOGOUT",
    "RETENTION_SECONDS",
    "RevocationStoreUnavailable",
    "fetch_since",
    "get_handler",
    "latest_id",
    "matches",
    "prune",
    "record",
    "register_handler",
]
