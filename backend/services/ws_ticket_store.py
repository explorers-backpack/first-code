# -*- coding: utf-8 -*-
"""WebSocket 一次性连接凭证（ticket）的**共享存储**。

问题
----
浏览器 ``WebSocket`` 不能自定义请求头，长期 Bearer Token 只能拼进 URL query，
会被网关/代理日志记录。改造方案是「HTTP 签发短时 ticket → WS 用 ticket 握手」。
ticket 的**签发方与校验方可能不是同一个进程**：

======================  ==========================  ==============================
部署形态                 进程内字典                    共享存储（本模块）
======================  ==========================  ==============================
单 worker               可用                            可用
多 worker / 多副本       ✗ A 签发、B 校验 ⇒ 随机 4401   ✓ 两边读同一张表
实例重启                 ✗ 已签发未使用的全部失效         ✓ 行还在
"单次使用"语义            ✗ 每个实例各有一份副本          ✓ 条件更新，全局只成功一次
======================  ==========================  ==============================

选型：**复用已有的 MySQL**，不引入 Redis
----------------------------------------
候选方案与结论（详见 ``CONCURRENCY_CONSISTENCY_REPORT.md`` §9）：

1. **MySQL 表 + 条件更新**（本方案）：项目已有 MySQL 8 + 唯一索引 + 行锁，
   零新增中间件；ticket 是「单行、短时、一次写入一次消费」的极简数据，
   正是 InnoDB 行锁最擅长的场景。新表由 ``Base.metadata.create_all`` 自动创建
   （``create_all`` 只建**新表**、不给既有表补约束，而本表是新表 ⇒ 零人工 DDL）。
2. **Redis ``SET NX`` / ``GETDEL``**：原子性最好，但要新增一个有状态中间件、
   一套高可用运维、一份新的故障域——为 30 秒的临时凭证引入它**不成比例**。
   等真的需要多实例限流 / 全局配额时再一并引入（见报告 §4.4）。
3. **进程内字典**：单 worker 可用，但正是本次要修的问题，且**不得**作为
   共享存储不可用时的降级路径（那会让"多实例"在故障时静默退化成"随机 4401"）。

原子消费怎么做
--------------
::

    UPDATE ws_ticket
       SET used_at = :now
     WHERE ticket_hash = :digest
       AND used_at IS NULL          ← 关键：只有第一个能匹配
       AND expires_at > :now
       AND session_id = :expected

``rowcount == 1`` 才算消费成功。两个 worker 并发校验同一个 ticket 时，
InnoDB 对同一行的 UPDATE 互斥；后到者执行时 ``used_at`` 已非 NULL ⇒
``rowcount == 0``。★ 注意 ``UPDATE`` 是**当前读**（locking read），
在 ``REPEATABLE READ`` 下也会读到最新已提交版本——所以这里**不依赖**事务快照，
也不需要在消费前后开长事务。

★ 消费成功后**立刻 commit**：``api/interview_ws.py`` 的 ``db`` 会随整条 WS 连接
存活数分钟，不提交会把行锁一直握到连接结束。

失败策略
--------
存储不可用时抛 :class:`TicketStoreUnavailable`（**绝不分叉到进程内字典**）：

- 签发路径 → HTTP **503**（不发一个注定验不过的 ticket）；
- 校验路径 → WS 以 **1011** 关闭（客户端会自动重连，是真正的"稍后重试"语义），
  **不**降级、**不**静默当作"未登录"。

日志约定
--------
**任何日志都不输出 ticket 明文**，也不输出可反推的片段；需要关联时只输出
``sha256(ticket)`` 的前 8 位（``digest_prefix``），它不可用于认证。
"""

from __future__ import annotations

import hashlib
import logging
import secrets
import time as _time
from datetime import datetime, timedelta
from typing import NamedTuple, Optional

from sqlalchemy import delete, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from models import WsTicket

logger = logging.getLogger("careerai.ws_ticket")

#: ticket 有效期（秒）。短到即使被日志记录也没有实际价值，长到足够完成一次握手。
TICKET_TTL_SECONDS = 30

#: 过期行清理的最小间隔（秒）。**只是降低清理频率的优化**，不承担任何正确性：
#: 即使某个进程从不清理，也只是表里多几行过期数据（消费时 ``expires_at`` 会挡住）。
SWEEP_MIN_INTERVAL_SECONDS = 60.0

#: 模块级节流时间戳（单调时钟）。刻意用进程内变量：它只影响"多久清一次"。
_last_sweep_at: float = 0.0


class TicketStoreUnavailable(RuntimeError):
    """ticket 存储不可用（表缺失 / 连接失败 / 权限不足 / 事务冲突）。

    ★ 这个异常的存在意义就是**阻断降级**：调用方必须把它当成"本次无法完成
    鉴权"来处理，而不是"退回进程内字典"或"当作未登录"。
    """


# ============================================================
# 纯函数
# ============================================================
def hash_ticket(ticket: str) -> str:
    """ticket → 存储用的摘要（SHA-256 hex，64 字符）。

    为什么是**不加盐**的 SHA-256：查询要按摘要做等值匹配，加盐就无法定位。
    安全性来自 ticket 本身的熵（``secrets.token_urlsafe(32)`` = 256 bit），
    与"密码要加盐"的场景不同——这里不存在低熵口令的字典攻击面。
    """
    return hashlib.sha256(ticket.encode("utf-8")).hexdigest()


def digest_prefix(digest: str) -> str:
    """摘要前 8 位，**只用于日志关联**（不可用于认证）。"""
    return (digest or "")[:8]


def new_ticket() -> tuple:
    """生成 ``(明文 ticket, 摘要)``。明文只在内存里存在，返回给客户端后不再保留。"""
    plaintext = secrets.token_urlsafe(32)
    return plaintext, hash_ticket(plaintext)


# ============================================================
# 内部工具
# ============================================================
async def _safe_rollback(db: AsyncSession) -> None:
    """回滚失败时不再抛（否则会盖掉真正的 ``TicketStoreUnavailable``）。"""
    try:
        await db.rollback()
    except Exception:  # noqa: BLE001 - 回滚失败只影响错误信息，不影响"失败"这个结论
        pass


def reset_sweep_throttle() -> None:
    """把清理节流复位（测试用；不影响任何正确性判据）。"""
    global _last_sweep_at
    _last_sweep_at = 0.0


# ============================================================
# 一、签发
# ============================================================
async def issue(
    db: AsyncSession,
    *,
    session_id: int,
    user_id: int,
    auth_session_id: Optional[int] = None,
    ttl: int = TICKET_TTL_SECONDS,
    now: Optional[datetime] = None,
) -> str:
    """签发一个 ticket，返回**明文**（只此一次，库里只有摘要）。

    存储不可用时抛 :class:`TicketStoreUnavailable`——**绝不返回一个验不过的
    ticket**：那会让客户端拿着废票去握手，错误现场离真因（存储故障）很远。

    :param auth_session_id: 绑定的**登录会话** id（``user_session.id``）。
        非空时，连接存续期间的复核可以查 ``revoked_at`` ⇒ 管理员踢下线 /
        登出能真正断开已建立的连接。为空则退化为「用户仍存在」（历史兼容）。
    """
    now = now or datetime.utcnow()
    plaintext, digest = new_ticket()

    # 清理过期行（节流；失败只记 WARNING——清理是维护动作，不该阻断签发）
    try:
        await sweep_expired(db, now=now)
    except TicketStoreUnavailable:
        pass

    db.add(
        WsTicket(
            ticket_hash=digest,
            user_id=int(user_id),
            session_id=int(session_id),
            auth_session_id=(
                None if auth_session_id is None else int(auth_session_id)
            ),
            created_at=now,
            expires_at=now + timedelta(seconds=int(ttl)),
        )
    )
    try:
        await db.commit()
    except SQLAlchemyError as exc:
        await _safe_rollback(db)
        logger.error(
            "ws_ticket_issue_failed session_id=%s user_id=%s error=%s",
            session_id,
            user_id,
            type(exc).__name__,
        )
        raise TicketStoreUnavailable(f"ticket 存储不可用：{type(exc).__name__}") from exc
    return plaintext


# ============================================================
# 二、消费（原子）
# ============================================================
class ConsumedTicket(NamedTuple):
    """一次成功消费的结果。"""

    user_id: int
    #: 绑定的登录会话 id；历史 ticket 为 ``None``。
    auth_session_id: Optional[int]


async def consume_ticket(
    db: AsyncSession,
    *,
    ticket: str,
    expected_session_id: int,
    now: Optional[datetime] = None,
) -> Optional[ConsumedTicket]:
    """原子消费一个 ticket：成功返回 :class:`ConsumedTicket`，失败返回 ``None``。

    ★ 为什么单独开一个函数而不是把 ``consume`` 的返回值改掉：
    ``consume`` 的 ``Optional[int]`` 契约被 20+ 处断言（含真实 MySQL 并发运行器）
    依赖，改返回类型等于让那些用例「因为签名变了」而失败——那种失败不携带任何
    安全信息。这里保持 ``consume`` 原样、新增带完整绑定的入口，两个入口共用
    同一段实现（同一个 ``UPDATE … WHERE used_at IS NULL``）。
    """
    if not isinstance(ticket, str) or not ticket:
        return None

    digest = hash_ticket(ticket)
    now = now or datetime.utcnow()

    try:
        row = (
            await db.execute(
                select(WsTicket).where(WsTicket.ticket_hash == digest)
            )
        ).scalar_one_or_none()
        if row is None:
            logger.info("ws_ticket_unknown digest=%s", digest_prefix(digest))
            return None

        # 绑定校验放在**写入之前**：错配不得消耗 ticket（否则一次误连就废掉凭证）
        if int(row.session_id) != int(expected_session_id):
            logger.warning(
                "ws_ticket_session_mismatch digest=%s expected=%s actual=%s",
                digest_prefix(digest),
                expected_session_id,
                row.session_id,
            )
            return None

        # ★ 先取普通值再更新：commit 之后 ORM 实例可能已过期/陈旧，
        #   不要再依赖 row 的属性（异步下访问过期属性会抛 MissingGreenlet）。
        user_id = int(row.user_id)
        auth_session_id = (
            None if row.auth_session_id is None else int(row.auth_session_id)
        )

        result = await db.execute(
            update(WsTicket)
            .where(
                WsTicket.ticket_hash == digest,
                WsTicket.used_at.is_(None),
                WsTicket.expires_at > now,
                WsTicket.session_id == int(expected_session_id),
            )
            .values(used_at=now)
            # 不同步 session 内的对象：这里只认 rowcount，同步反而多一次 SELECT
            .execution_options(synchronize_session=False)
        )
        # ★ 立刻提交：释放行锁。WS 连接的 db 会存活数分钟，不提交等于长事务。
        await db.commit()
    except SQLAlchemyError as exc:
        await _safe_rollback(db)
        logger.error(
            "ws_ticket_store_unavailable op=consume error=%s", type(exc).__name__
        )
        raise TicketStoreUnavailable(f"ticket 存储不可用：{type(exc).__name__}") from exc

    if result.rowcount != 1:
        # 走到这里说明"读到了行但没抢到消费权"：已用过 / 刚过期 / 并发失败
        logger.warning("ws_ticket_not_consumable digest=%s", digest_prefix(digest))
        return None
    return ConsumedTicket(user_id=user_id, auth_session_id=auth_session_id)


async def consume(
    db: AsyncSession,
    *,
    ticket: str,
    expected_session_id: int,
    now: Optional[datetime] = None,
) -> Optional[int]:
    """原子消费一个 ticket：成功返回 ``user_id``，失败返回 ``None``。

    失败原因**不区分**（不存在 / 已用过 / 已过期 / 会话绑定不符）——对外都是
    "凭证无效"，避免给出可用作探测的信号；差异只体现在日志里。

    存储不可用时抛 :class:`TicketStoreUnavailable`（**不是**返回 ``None``）：
    两者必须可区分，否则"存储挂了"会被误当成"用户凭证无效"，把运维事故
    伪装成用户登录过期。

    ★ 需要 ``auth_session_id`` 时用 :func:`consume_ticket`（本函数是它的
    向后兼容薄封装，语义与开销完全相同）。
    """
    consumed = await consume_ticket(
        db, ticket=ticket, expected_session_id=expected_session_id, now=now
    )
    return None if consumed is None else consumed.user_id


# ============================================================
# 三、撤销
# ============================================================
async def revoke(
    db: AsyncSession, *, ticket: str, now: Optional[datetime] = None
) -> bool:
    """撤销单个 ticket（未使用过才生效）。返回是否真的撤销了。"""
    if not isinstance(ticket, str) or not ticket:
        return False
    digest = hash_ticket(ticket)
    now = now or datetime.utcnow()
    try:
        result = await db.execute(
            update(WsTicket)
            .where(WsTicket.ticket_hash == digest, WsTicket.used_at.is_(None))
            .values(used_at=now)
            .execution_options(synchronize_session=False)
        )
        await db.commit()
    except SQLAlchemyError as exc:
        await _safe_rollback(db)
        raise TicketStoreUnavailable(f"ticket 存储不可用：{type(exc).__name__}") from exc
    return result.rowcount == 1


async def revoke_for_user(
    db: AsyncSession, *, user_id: int, now: Optional[datetime] = None
) -> int:
    """撤销某用户**所有未使用**的 ticket（登出时调用）。返回撤销条数。

    为什么登出要清：ticket 路径下没有长期 token 可复核，已签发的 ticket 在
    30 秒内仍然可用。登出后立刻作废它们，把"登出 ⇒ 立即失效"这条语义补齐。
    """
    now = now or datetime.utcnow()
    try:
        result = await db.execute(
            update(WsTicket)
            .where(WsTicket.user_id == int(user_id), WsTicket.used_at.is_(None))
            .values(used_at=now)
            .execution_options(synchronize_session=False)
        )
        await db.commit()
    except SQLAlchemyError as exc:
        await _safe_rollback(db)
        raise TicketStoreUnavailable(f"ticket 存储不可用：{type(exc).__name__}") from exc
    return int(result.rowcount or 0)


# ============================================================
# 四、清理
# ============================================================
async def sweep_expired(
    db: AsyncSession, *, now: Optional[datetime] = None, force: bool = False
) -> int:
    """删除已过期的 ticket 行，返回删除条数。

    节流：非 ``force`` 调用最多每 :data:`SWEEP_MIN_INTERVAL_SECONDS` 秒清一次。
    节流用进程内时间戳是**安全的**——它只决定"这次要不要清"，
    多进程重复清理最多多一次 ``DELETE``，不产生错误结果。
    """
    global _last_sweep_at

    now = now or datetime.utcnow()
    if not force:
        if _time.monotonic() - _last_sweep_at < SWEEP_MIN_INTERVAL_SECONDS:
            return 0

    try:
        result = await db.execute(
            delete(WsTicket).where(WsTicket.expires_at < now)
        )
        await db.commit()
    except SQLAlchemyError as exc:
        await _safe_rollback(db)
        logger.warning(
            "ws_ticket_sweep_failed error=%s", type(exc).__name__
        )
        raise TicketStoreUnavailable(f"ticket 存储不可用：{type(exc).__name__}") from exc

    _last_sweep_at = _time.monotonic()
    return int(result.rowcount or 0)


__all__ = [
    "SWEEP_MIN_INTERVAL_SECONDS",
    "TICKET_TTL_SECONDS",
    "ConsumedTicket",
    "TicketStoreUnavailable",
    "consume",
    "consume_ticket",
    "digest_prefix",
    "hash_ticket",
    "issue",
    "new_ticket",
    "reset_sweep_throttle",
    "revoke",
    "revoke_for_user",
    "sweep_expired",
]
