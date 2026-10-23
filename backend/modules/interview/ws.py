# -*- coding: utf-8 -*-
"""AI 模拟面试 · **实时通道（WebSocket）** —— 数字人视频面试的交互链路。

为什么需要它
------------
HTTP 是「请求 → 响应」的：前端每次都得自己决定「现在该调哪个接口」。
数字人面试却是一条**有状态的时间线**（播报中 → 等待作答 → 评分中 → 下一题），
中途还有服务端主动产生的状态（数字人会话就绪 / 流地址 / 播报结束）。
WebSocket 让服务端**主动推送**这些状态，前端只需渲染。

职责边界（与 ``modules/interview/api.py`` 完全一致）
--------------------------------------------
本层只做「协议编解码 + 调 service/orchestrator」，**不做任何业务判断**：
出题、评分、报告、状态机全部在 ``services/`` 里。本层尤其**不碰 provider**。

★ 与 ``interview_orchestrator`` 的关系（唯一的例外，且被守卫收口）
------------------------------------------------------------------
``interview_orchestrator`` 的既定约定是「**可选层**，不得被 core / service / api 反向
import」——否则会把「可选编排」变成「必经路径」，改变既有行为。
本模块是**刻意为之的例外**：它是一条**新增**的、只服务视频面试的路由，
其存在意义就是把编排层的时间线暴露给前端。
为守住「可选」这一性质，``tests/test_interview_ws.py`` 断言：
**全项目只有本模块 import ``services.interview_orchestrator``**
（``modules/interview/api.py`` / ``main.py`` / ``services/*`` 一律不得）。
即：编排层仍是「可选的」，只是这个**新**路由显式选了它。

鉴权
----
浏览器 ``WebSocket`` **无法自定义请求头**，因此凭证走 query：

- **首选**：先 ``POST /api/interview/{id}/ws-ticket``（带 Bearer）拿一次性
  ticket，再连 ``ws://host/api/interview/{id}/ws?ticket=<ticket>``；
- **历史兼容**：``?token=<长期登录 token>``。见下方「兼容路径」。

ticket 存在 **MySQL 表 ``ws_ticket``**（``services/ws_ticket_store.py``）里，
**不是进程内字典** ⇒ 多 worker / 多副本下「A 签发、B 校验」也能工作。
见该模块文档与 ``CONCURRENCY_CONSISTENCY_REPORT.md`` §9。

顺序刻意是 **先 ``accept()`` 再校验**：这样未登录时能给客户端一个**明确的关闭码**
（``4401``），而不是让浏览器只看到一个「握手失败」。两条路径的复核口径**统一**为
``user_session``：``token`` 路径按 token 查，ticket 路径按 ticket 里绑定的
``auth_session_id`` 查——**都覆盖撤销与过期**，见 :func:`_auth_still_valid`。

兼容路径（``?token=``）的边界
-----------------------------
**为什么还留着**：非浏览器客户端（``tests/test_interview_ws.py``、
``frontend/tests/interview_ws_integration_run.js``、命令行排障脚本）没有
「先 HTTP 换票」这一步的便利；且已发布的旧版前端在用户刷新页面之前仍在用它。
**它是什么**：一条**独立的、走完整 ``user_session`` 校验**的鉴权分支，
不是「ticket 验不过就用 token 兜底」——两者是并列关系，ticket 失败**绝不**回落
到 token（回落会让「票据存储故障」静默变成「换个凭证继续」）。

**边界**：这条路径把**长期凭证**放进 URL query ⇒ 会被 uvicorn 访问日志、
反向代理日志记录。本仓库已用 ``main._LogRedactingFilter`` 在**格式化前**脱敏
（``tests/test_auth_security.py`` §[14] 实测断言），但**反向代理侧的日志不归本仓库管**。

**怎么关**：``INTERVIEW_WS_TOKEN_AUTH=0``（也接受 ``false`` / ``no`` / ``off``）。
**默认开启**（未配置 / 空串 = 开启）以保持既有行为不变；前端已**不再**使用这条路径
（``frontend/src/api/interviewSocket.js`` 改为 fail closed，取不到 ticket 就不建连），
因此**新部署建议显式关闭**。关闭后仍带 ``?token=`` 的连接按「未登录」处理（``4401``）。

撤销（踢下线 / 登出）怎么作用到**已建立**的连接
----------------------------------------------
长连接不会自己回头查库，所以必须由**服务端主动**断。三层配合：

1. **共享广播**（``services/ws_revocation``）：撤销时往 ``ws_revocation`` 表追加一行
   （只追加、单调 id）。为什么不用进程内事件：撤销发生在某个实例上，连接可能长在
   另一个实例上，进程内通知**到不了**那边。
2. **每实例一个轮询器**（``services/ws_revocation_watcher``）：每 ``2s`` 一条
   ``SELECT … WHERE id > watermark``，把新记录交给 ``close_revoked_connections``，
   由它断开**本进程**上匹配的连接（关闭码 ``4401``，前端不重连）。
   ★ 一条查询/实例/周期，与连接数**无关**——不是「每条连接每秒查一次」。
3. **兜底**：``AUTH_RECHECK_SECONDS``（60s）的周期性复核 + **高风险操作执行前**
   （``answer`` / ``finish``）再验一次会话。第三层保证「广播丢了 / 轮询器被关了 /
   存储故障」时仍有**有界**的失效延迟，而不是无限期。

实测（``tests/test_ws_revocation.py``，50ms 采样步长）：同实例 **0.31s**、
跨实例 **0.38s**（测试轮询周期 0.2s；生产默认 2.0s ⇒ 目标 ≤ 3s）。
改造前基线：ticket 路径 **>20s 完全不生效**、token 路径 **61.7s**。

生产化
------
长连接的生产风险（半开连接累积 / 进程内状态无界增长 / 同一会话被两个标签页同时作答 /
凭据被打进日志）**都不是业务逻辑**，因此全部集中在 ``services/interview_production.py``，
本层只做接线（超时、清理、闸门、限流、结构化日志、指标）。**默认值即现状**：

- 空闲超时：``INTERVIEW_WS_IDLE_TIMEOUT``（默认 300 秒；前端有心跳，活连接不会命中）；
- 会话清理：``INTERVIEW_STATE_TTL`` / ``INTERVIEW_MAX_TRACKED_SESSIONS``
  （每次新连接建立时**机会式**清理，无后台任务）；
- 并发闸门：``INTERVIEW_MAX_WS_CONNECTIONS`` / ``INTERVIEW_MAX_SESSIONS_PER_USER``；
- 作答限流：``INTERVIEW_ANSWER_MIN_INTERVAL``（**默认 0 = 不限制**，需显式开启）。

新增关闭码 ``4408``（空闲超时）/ ``4409``（并发超限），前端对二者**都不重连**。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import date, datetime
from datetime import time as dtime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from deps import get_current_user, get_optional_session
from security import (
    load_session_by_id,
    load_valid_session,
    session_is_valid,
    user_exists,
)
from schemas.interview import AnswerSubmitRequest
from services import interview_orchestrator as orch
from services import interview_production as prod
from services import interview_service
from services import ws_revocation
from services import ws_ticket_store as ticket_store

# ============================================================
# WebSocket 一次性连接凭证（Ticket）
# ============================================================
# 浏览器 WebSocket 无法自定义请求头，长期 Bearer Token 走 URL query
# 会被代理/网关日志记录。改用短时（30 秒）、单次使用的 ticket：
#   1. 前端先 HTTP POST /api/interview/{session_id}/ws-ticket（带 Bearer）
#   2. 后端返回一次性 ticket
#   3. 前端用 ticket 连接 WS：?ticket=xxx
#   4. ticket 校验通过后**原子消费**，不可复用
#   5. 无 ticket 时可用 Bearer token（**历史兼容**，默认开、可显式关闭，
#      见下方「兼容路径」）；前端已不再使用它
#
# ★ 存储是 **MySQL 表 ``ws_ticket``**（``services/ws_ticket_store.py``），
#   不是进程内字典。多 worker / 多副本下「A 签发、B 校验」也能工作；
#   「单次使用」由 ``UPDATE … WHERE used_at IS NULL`` 的 rowcount 全局保证。
#   ★ 存储不可用时**安全失败**（签发 503 / 握手 1011），绝不退回进程内字典——
#     那会让多实例在故障时静默退化成随机 4401，比直接失败危险得多。
# ★ ticket 里还存了 **auth_session_id**（``user_session.id``）：连接存续期间的
#   复核要靠它判定「是否已被踢下线 / 登出」。
_TICKET_TTL_SECONDS = ticket_store.TICKET_TTL_SECONDS


async def create_ws_ticket(
    db: AsyncSession,
    session_id: int,
    user_id: int,
    auth_session_id: Optional[int] = None,
) -> str:
    """签发一次性 WS 连接 ticket（委托给共享存储）。

    :param auth_session_id: 绑定的**登录会话** id。带上它，连接存续期间的复核
        才能沿 ``user_session.revoked_at`` 判定「是否已被踢下线」；不带则退化为
        「用户仍存在」（历史兼容，会记 WARNING）。

    存储不可用时抛 ``TicketStoreUnavailable``，由端点转成 503——
    **绝不返回一个注定验不过的 ticket**（那会让故障现场离真因很远）。
    """
    return await ticket_store.issue(
        db,
        session_id=session_id,
        user_id=user_id,
        auth_session_id=auth_session_id,
    )


async def verify_ws_ticket(
    db: AsyncSession, ticket: str, expected_session_id: int
) -> Optional[ticket_store.ConsumedTicket]:
    """校验并**原子消费** ticket；有效返回 ``ConsumedTicket``，无效返回 ``None``。

    两个进程并发校验同一个 ticket 时**最多一个**能拿到结果：
    数据库条件更新的 rowcount 是唯一权威，不依赖任何进程内状态。
    """
    return await ticket_store.consume_ticket(
        db, ticket=ticket, expected_session_id=expected_session_id
    )


router = APIRouter(tags=["AI 面试 · 实时通道"])

# ============================================================
# Origin 校验（防跨站 WebSocket 劫持 / 浏览器侧来源收口）
# ============================================================
# 浏览器在 WS 握手时会带 ``Origin``；非浏览器客户端（脚本 / 移动端）通常不带。
# 口径：**带了 Origin 就必须命中白名单**；不带 Origin 时仍要求有效凭证
# （鉴权才是主防线，Origin 是纵深防御）。
# 默认值与 CORS 保持一致（本地 Vite 两个入口）；分域部署时用
# ``WS_ALLOWED_ORIGINS``（优先）或 ``CORS_ALLOW_ORIGINS`` 显式配置。
_DEFAULT_WS_ORIGINS = ("http://localhost:5173", "http://127.0.0.1:5173")


def _parse_ws_origins() -> tuple:
    raw = (
        os.getenv("WS_ALLOWED_ORIGINS", "").strip()
        or os.getenv("CORS_ALLOW_ORIGINS", "").strip()
    )
    if not raw:
        return _DEFAULT_WS_ORIGINS
    return tuple(item.strip() for item in raw.split(",") if item.strip())


ALLOWED_WS_ORIGINS = _parse_ws_origins()

# ============================================================
# 兼容路径开关：``?token=<长期登录 token>`` 是否允许
# ============================================================
# **默认开启**（未配置 / 空串）⇒ 行为与改造前**完全一致**。
# 前端已不再使用这条路径（取不到 ticket 就 fail closed），因此**新部署建议关闭**：
#
#     INTERVIEW_WS_TOKEN_AUTH=0
#
# ★ 每次握手**实时读环境变量**（不在 import 期缓存）：与 ``prod.safe_config()``
#   同一口径，运维改完配置重启即可，且测试可以逐用例切换两个状态。
# ★ 非法值**不静默**：记 WARNING 后按默认（开启）走，避免「配错了以为关上了」。
_TOKEN_AUTH_TRUE = ("1", "true", "yes", "on")
_TOKEN_AUTH_FALSE = ("0", "false", "no", "off")


def token_auth_enabled() -> bool:
    """是否允许用长期登录 token 建连（``INTERVIEW_WS_TOKEN_AUTH``）。"""
    raw = os.getenv("INTERVIEW_WS_TOKEN_AUTH", "").strip().lower()
    if not raw:
        return True
    if raw in _TOKEN_AUTH_FALSE:
        return False
    if raw in _TOKEN_AUTH_TRUE:
        return True
    prod.log_event(
        "ws_token_auth_invalid_config",
        level=logging.WARNING,
        value=raw[:32],
        fallback="enabled",
    )
    return True


#: 连接建立后重新校验登录态的最小间隔（秒）。
#: 撤销 / 过期不必等到空闲超时（默认 300s）才生效，也不能每条消息都查库。
AUTH_RECHECK_SECONDS = 60.0


def origin_allowed(origin: str, host: str = "") -> bool:
    """``Origin`` 为空视为非浏览器客户端（放行，仍要过鉴权）。

    放行顺序：通配符 → 显式白名单 → **同源**（Origin 的 netloc 与请求 Host 一致）。
    同源判定让「前后端同域部署」无需任何配置即可工作。
    """
    if not origin:
        return True
    if "*" in ALLOWED_WS_ORIGINS:
        return True
    if origin in ALLOWED_WS_ORIGINS:
        return True
    try:
        from urllib.parse import urlparse

        return bool(host) and urlparse(origin).netloc == host
    except Exception:  # noqa: BLE001 - 畸形 Origin 一律拒绝
        return False


async def _auth_still_valid(
    db: AsyncSession,
    *,
    auth_token: str,
    auth_session_id: Optional[int],
    user_id: int,
) -> bool:
    """连接存续期间复核登录态（**唯一口径**，与 ``deps.get_current_session`` 一致）。

    三条路径，从严到宽：

    1. **有 token**（历史兼容路径）：重新查 ``user_session``，覆盖撤销与过期；
    2. **有 auth_session_id**（ticket 正常路径）：按主键查 ``user_session``，
       同样覆盖撤销与过期。★ 这条路径是「管理员踢下线能踢掉已建立连接」的关键
       —— ticket 本身一次性、消费后就没有可复核的东西了，所以把**登录会话 id**
       一起存进 ticket（``ws_ticket.auth_session_id``），复核时沿它回查；
    3. **两者都没有**（历史 ticket / 测试里无 Authorization 头）：退化为
       「用户仍存在」。★ 这是**已知的弱口径**，不是设计意图：它意味着连接存续
       期间无法感知撤销。降级时记 WARNING（不静默），让它在日志里可被发现。
    """
    if auth_token:
        session = await load_valid_session(db, auth_token)
        return session is not None and session.user_id == user_id

    if auth_session_id is not None:
        row = await load_session_by_id(db, int(auth_session_id))
        if row is None or int(row.user_id) != int(user_id):
            return False
        return session_is_valid(row)

    logger_warning_unbound(user_id)
    return await user_exists(db, user_id)


#: 「无绑定」降级的告警节流（秒）：同一条连接每 60s 最多记一次，避免刷日志。
_UNBOUND_WARN_INTERVAL_SECONDS = 60.0
_unbound_warned: Dict[int, float] = {}


def logger_warning_unbound(user_id: int) -> None:
    """对「无法复核撤销」的连接记一条节流 WARNING（排障用，不含任何凭据）。"""
    now = time.monotonic()
    last = _unbound_warned.get(user_id, 0.0)
    if now - last < _UNBOUND_WARN_INTERVAL_SECONDS:
        return
    _unbound_warned[user_id] = now
    prod.log_event(
        "ws_auth_unbound_session",
        level=logging.WARNING,
        user_id=user_id,
        reason="ticket_without_auth_session_id",
    )


# ============================================================
# 本进程的活动连接注册表（**只用于断开自己身上的连接**）
# ============================================================
# ★ 它**不承担任何正确性**：连接对象只存在于某一个进程里，所以「谁来断它」
#   必须由那个进程自己完成。跨实例的**通知**走 ``services/ws_revocation``
#   的只追加表（共享存储），而不是这张表。
# ★ 因此这张表里存的东西都只是「本进程的内存句柄」：丢了最多是漏断一次，
#   由周期性复核（``AUTH_RECHECK_SECONDS``）兜底。
class _ActiveConn:
    """一条活动连接的最小描述（够用来判断"该不该断它"）。"""

    __slots__ = ("user_id", "auth_session_id", "websocket", "closed")

    def __init__(
        self,
        user_id: int,
        auth_session_id: Optional[int],
        websocket: WebSocket,
    ) -> None:
        self.user_id = int(user_id)
        self.auth_session_id = auth_session_id
        self.websocket = websocket
        self.closed = False


_ACTIVE_CONNS: Dict[int, _ActiveConn] = {}
_next_conn_id = [0]


def register_active_conn(
    user_id: int, auth_session_id: Optional[int], websocket: WebSocket
) -> int:
    """登记一条活动连接，返回本进程内的连接序号（用于注销）。"""
    _next_conn_id[0] += 1
    cid = _next_conn_id[0]
    _ACTIVE_CONNS[cid] = _ActiveConn(user_id, auth_session_id, websocket)
    return cid


def unregister_active_conn(conn_id: int) -> None:
    """注销连接（幂等）。"""
    _ACTIVE_CONNS.pop(int(conn_id), None)


def _conn_closed(conn_id: int) -> bool:
    """该连接是否已被**主动断开**（撤销广播 / 空闲超时）。

    ★ 为什么需要它：主动断开发生在**另一个任务**里，而本任务的循环可能正卡在
    ``receive_json`` 上。醒来后如果还继续 ``send``（比如回一个 pong），
    Starlette 会抛 ``Cannot call "send" once a close message has been sent``。
    这里在每轮开头挡一下，属于"已知会被并发关闭"的显式处理，而不是靠异常兜底。
    """
    conn = _ACTIVE_CONNS.get(int(conn_id))
    return conn is None or conn.closed


def active_conn_count() -> int:
    """本进程当前登记的活动连接数（排障 / 测试用）。"""
    return len(_ACTIVE_CONNS)


def reset_active_conns() -> None:
    """清空注册表（测试用；生产代码不应调用）。"""
    _ACTIVE_CONNS.clear()
    _unbound_warned.clear()


async def close_revoked_connections(rows) -> int:
    """把「已撤销」的连接**主动断开**，返回断开条数。

    由 ``services/ws_revocation_watcher`` 在轮询到新撤销记录时调用
    （通过 ``ws_revocation.register_handler`` 注册，避免反向依赖）。

    关闭码用 **4401**：语义就是「登录态失效」，且前端对 4401 **不重连**
    ——被踢下线的人不该被自动重连回来。
    """
    if not rows:
        return 0
    closed = 0
    for cid, conn in list(_ACTIVE_CONNS.items()):
        if conn.closed:
            continue
        hit = any(
            ws_revocation.matches(
                row, user_id=conn.user_id, auth_session_id=conn.auth_session_id
            )
            for row in rows
        )
        if not hit:
            continue
        # ★ 先置位再 await：`_close` 会让对端收到 FIN，对方可能立刻重连并
        #   触发一次新的轮询；不置位就会对同一条连接重复 close（有些 ASGI
        #   实现会因此抛 RuntimeError）。
        # ★ **不从 _ACTIVE_CONNS 里删**：删了 `_conn_closed(conn_id)` 就查不到
        #   状态，连接自己的循环会继续 send。注销交给它的 finally。
        conn.closed = True
        try:
            await _send(
                conn.websocket,
                error_event(ERROR_UNAUTHORIZED, "登录状态已失效，请重新登录"),
            )
            await _close(conn.websocket, WS_CLOSE_UNAUTHORIZED, "登录状态已失效")
            closed += 1
            prod.log_event(
                "ws_closed_by_revocation",
                level=logging.WARNING,
                user_id=conn.user_id,
                reason="session_revoked",
            )
        except Exception as exc:  # noqa: BLE001 - 连接可能已死，断开失败不影响其他连接
            prod.log_event(
                "ws_close_on_revocation_failed",
                level=logging.WARNING,
                user_id=conn.user_id,
                error=type(exc).__name__,
            )
    return closed


#: 路由模板（前端 ``interviewSocket.buildSocketUrl`` 必须与本值一致，有守卫测试）。
WS_PATH = "/api/interview/{session_id}/ws"

# ============================================================
# 一、协议常量（**前端镜像**，由测试比对两侧字面量，防漂移）
# ============================================================
# ---- 服务端 → 客户端 ----
EVENT_HELLO = "hello"
EVENT_STATE = "state"
EVENT_AVATAR = "avatar"
EVENT_QUESTION = "question"
EVENT_ANSWER = "answer"
EVENT_SCORE = "score"
EVENT_REPORT = "report"
EVENT_ERROR = "error"
EVENT_PONG = "pong"

SERVER_EVENTS = (
    EVENT_HELLO,
    EVENT_STATE,
    EVENT_AVATAR,
    EVENT_QUESTION,
    EVENT_ANSWER,
    EVENT_SCORE,
    EVENT_REPORT,
    EVENT_ERROR,
    EVENT_PONG,
)

# ---- 客户端 → 服务端 ----
CLIENT_START = "start"
CLIENT_SPEECH_DONE = "speech_done"
CLIENT_ANSWER = "answer"
CLIENT_FINISH = "finish"
CLIENT_SNAPSHOT = "snapshot"
CLIENT_PING = "ping"

CLIENT_MESSAGES = (
    CLIENT_START,
    CLIENT_SPEECH_DONE,
    CLIENT_ANSWER,
    CLIENT_FINISH,
    CLIENT_SNAPSHOT,
    CLIENT_PING,
)

# ---- ``answer`` 消息的两种提交方式 ----
#: 语音作答：服务端走 ``orch.answer_from_asr``（讯飞虚拟人内置 ASR 取文本）。
ANSWER_MODE_ASR = "asr"
#: 打字作答（兜底）：直接提交文本，走 ``orch.submit_answer``。
ANSWER_MODE_TEXT = "text"
ANSWER_MODES = (ANSWER_MODE_ASR, ANSWER_MODE_TEXT)

# ---- ``answer`` 事件的提交结果 ----
ANSWER_SUBMITTED = "submitted"
ANSWER_REJECTED = "rejected"

# ---- 关闭码（4xxx = 应用自定义，避免与标准码混淆） ----
WS_CLOSE_UNAUTHORIZED = 4401
WS_CLOSE_FORBIDDEN = 4403
WS_CLOSE_NOT_FOUND = 4404
#: 空闲超时：服务端判定该连接已长时间无任何客户端消息（前端心跳也停了）⇒ 释放资源。
#: 前端**不重连**（重连也还是没人操作，只会把资源又占回去）。
WS_CLOSE_IDLE = 4408
#: 并发超限：该用户的连接数 / 并发会话数已达上限 ⇒ 拒绝新连接。
#: 前端**不重连**（立刻重连必然再次被拒，会把提示刷屏）。
WS_CLOSE_BUSY = 4409
#: 凭证存储暂时不可用（ticket 表缺失 / MySQL 不可用）⇒ **安全失败**。
#: ★ 刻意用**标准码 1011**（Internal Error）而不是 4xxx：
#:   前端对 4xxx 一律**不重连**，而这里"稍后重试"才是正确行为
#:   （前端会重连并重新签发 ticket）。用标准码也就不必新增 4xxx 常量，
#:   不破坏「4xxx = 应用自定义」的既有约定。
WS_CLOSE_UNAVAILABLE = 1011

# ---- 稳定错误码（``error`` 事件的 ``code``；前端据此分支，不解析长句） ----
ERROR_BAD_MESSAGE = "bad_message"
ERROR_UNAUTHORIZED = "unauthorized"
ERROR_SESSION_NOT_FOUND = "session_not_found"
ERROR_STATE = "state_error"
ERROR_EMPTY_ANSWER = "empty_answer"
ERROR_SERVICE = "service_error"
ERROR_INTERNAL = "internal_error"
#: 并发闸门拒绝（同一时刻已有太多连接 / 会话）。
ERROR_BUSY = "busy"


# ============================================================
# 二、纯函数：协议编解码（可脱离连接单测）
# ============================================================
def normalize_client_message(raw: Any) -> Dict[str, Any]:
    """把客户端原始消息**归一**成固定形状（永不抛异常）。

    返回::

        {"type": str|None, "mode": str, "text": str, "error": str|None}

    - ``type`` 为 ``None`` 表示「不认识这条消息」，``error`` 给出原因；
    - ``mode`` 只对 ``answer`` 有意义，非法值回落 ``text``（**不静默**：由调用方报错）；
    - ``text`` 一律 ``strip()``（纯空白视为空串）。
    """
    if not isinstance(raw, dict):
        return {"type": None, "mode": "", "text": "", "error": "消息必须是 JSON 对象"}

    msg_type = raw.get("type")
    if not isinstance(msg_type, str) or msg_type not in CLIENT_MESSAGES:
        return {
            "type": None,
            "mode": "",
            "text": "",
            "error": f"未知消息类型：{msg_type!r}；合法类型：{'/'.join(CLIENT_MESSAGES)}",
        }

    text = raw.get("text")
    text = text.strip() if isinstance(text, str) else ""

    mode = raw.get("mode")
    mode = mode if isinstance(mode, str) and mode in ANSWER_MODES else ""

    return {"type": msg_type, "mode": mode, "text": text, "error": None}


def _avatar_config() -> Dict[str, Any]:
    """数字人**配置层**状态（纯读环境变量，不联网、**绝不抛异常**）。

    来源是 ``common.health.avatar_status``（第 10 轮 P4 从 ``main.py`` 整体迁出）。
    之前这里写的是 ``from main import avatar_status``——那是 ``api -> main`` 的
    反向依赖（L3 依赖 L9），能跑只是因为延迟导入绕开了环。现在 ``avatar_status``
    在 ``common/health.py`` 里，而 ``common.health`` 只依赖 ``models`` 与
    ``sqlalchemy``，**不 import 任何路由**，所以这条边是合法的向下依赖。
    """
    try:
        from common.health import avatar_status  # noqa: PLC0415

        return avatar_status()
    except Exception as exc:  # noqa: BLE001 - 状态上报永不抛异常
        return {
            "provider": "", "configured": False, "host": "", "base_path": "",
            "error": f"{type(exc).__name__}: {exc}",
        }


def avatar_event(probe: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """``avatar`` 事件（**恒定键集**）。

    ``probe`` 为 ``orch.prepare_avatar`` / ``interview_service.ensure_avatar`` 的返回值；
    未预检时传 ``None`` ⇒ ``session_probed=False``、``ok=None``、``status=None``
    ——**「还没探测过」与「探测失败」是两件事**，不能都写成 ``ok=False``。
    """
    config = _avatar_config()
    probed = isinstance(probe, dict)
    return {
        "type": EVENT_AVATAR,
        "session_probed": probed,
        "ok": bool(probe.get("ok")) if probed else None,
        "status": probe.get("status") if probed else None,
        "avatar_session_id": (probe.get("avatar_session_id") or "") if probed else "",
        "stream_url": (probe.get("stream_url") or "") if probed else "",
        "provider": config.get("provider", ""),
        "configured": bool(config.get("configured")),
        "host": config.get("host", ""),
        "base_path": config.get("base_path", ""),
        "error": (probe.get("error") if probed else None) or config.get("error"),
    }


def state_event(session_id: int) -> Dict[str, Any]:
    """``state`` 事件 = ``orch.status`` 逐字段 + ``type``（编排状态是**进程内**的）。"""
    return {"type": EVENT_STATE, **orch.status(session_id)}


def question_event(
    session: Optional[Dict[str, Any]],
    question: Optional[Dict[str, Any]],
    all_answered: bool = False,
) -> Dict[str, Any]:
    """``question`` 事件：**原样**带上 service 的 ``session`` / ``question`` 字典。

    刻意不在这里重新拼装题目字段——前端渲染的就是 HTTP 契约里的同一个对象
    （``interview_service._question_to_dict`` 的产物），**两条通道形状一致**。
    """
    return {
        "type": EVENT_QUESTION,
        "session": session,
        "question": question,
        "all_answered": bool(all_answered),
    }


def score_event(answer: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """``score`` 事件：把作答里的评分字段**摊平**（前端不必再挖一层）。"""
    answer = answer or {}
    return {
        "type": EVENT_SCORE,
        "question_id": answer.get("question_id"),
        "score": answer.get("score"),
        "technical_score": answer.get("technical_score"),
        "logic_score": answer.get("logic_score"),
        "expression_score": answer.get("expression_score"),
        "adaptability_score": answer.get("adaptability_score"),
        "feedback": answer.get("feedback"),
    }


def error_event(code: str, message: str) -> Dict[str, Any]:
    """``error`` 事件（``code`` 是**稳定码**，``message`` 是给人看的说明）。"""
    return {"type": EVENT_ERROR, "code": code, "message": message}


def answer_event(
    status: str,
    *,
    answer: Optional[Dict[str, Any]] = None,
    text: str = "",
    source: Optional[str] = None,
    asr: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    """``answer`` 事件（**恒定键集**）：提交结果 + 文本来源 + ASR 明细。"""
    return {
        "type": EVENT_ANSWER,
        "status": status,
        "answer": answer,
        "text": text,
        "source": source,
        "asr": asr,
        "error": error,
    }


# ============================================================
# 三、鉴权（口径与 deps.get_current_user 一致）
# ============================================================
async def resolve_user(db: AsyncSession, token: str) -> Optional[dict]:
    """按 token 解析登录态；无效返回 ``None``（**不抛异常**，由调用方决定关闭码）。

    ``session_id`` 是 ``user_session.id``（**登录会话**，不是面试会话）：
    有了它，连接存续期间的复核可以走主键查询并覆盖撤销/过期。
    """
    session = await load_valid_session(db, token)
    if session is None:
        return None
    return {
        "user_id": session.user_id,
        "email": session.email,
        "role": session.role,
        "session_id": session.id,
    }


# ============================================================
# 四、连接处理
# ============================================================
async def _send(websocket: WebSocket, payload: Dict[str, Any]) -> None:
    """发一条 JSON（``send_text`` 失败说明连接已断，交由上层捕获）。

    ★ 为什么不用 ``websocket.send_json``：它内部是**裸 ``json.dumps``**，
    而 ``interview_service`` 的 ``_session_to_dict`` 里带着 ``started_at`` /
    ``ended_at`` / ``created_at`` 三个 ``datetime``——HTTP 侧由 FastAPI 的
    ``jsonable_encoder`` 负责转换，WebSocket 侧**没有**这一层，直接发会抛
    ``TypeError: Object of type datetime is not JSON serializable``。
    因此这里显式提供一个只认「日期时间」的 ``default``：**其它类型仍然报错**
    （不静默 ``str()`` 兜底——那会把契约错误变成前端拿到一串不可解析的文本）。

    ★ 还会把「已经发过 close 之后再 send」的 ``RuntimeError`` 收敛成
    ``WebSocketDisconnect``：**主动断开**（撤销广播 / 空闲超时）发生在**另一个
    任务**里，而本任务可能正好卡在 ``receive_json`` 上、手里握着一条待回的
    ``ping``。此时 Starlette 会抛
    ``RuntimeError: Cannot call "send" once a close message has been sent.``，
    不收敛的话它会以「未知内部错误」的形式冒到最外层（日志噪音 + 可能被误判成
    业务异常）。语义上它就是「连接已经没了」，与 ``WebSocketDisconnect`` 完全一致。
    """
    try:
        await websocket.send_text(
            json.dumps(payload, ensure_ascii=False, default=_json_default)
        )
    except RuntimeError as exc:
        if "close message has been sent" in str(exc):
            raise WebSocketDisconnect(code=1006, reason="连接已被服务端关闭") from exc
        raise


def _json_default(value: Any) -> str:
    """``json.dumps`` 的 ``default``：日期时间 → ISO 字符串；其余**显式报错**。

    ★ 这里的 ``time`` 是 ``datetime.time``（导入时别名 ``dtime``）：
    本模块同时 ``import time``（标准库，用于 ``time.monotonic()``），
    若两个名字撞在一起，``isinstance(value, (datetime, date, time))`` 会拿到
    模块对象并抛 ``TypeError: isinstance() arg 2 must be a type``——
    错误信息会指向 isinstance 而不是真正无法序列化的值。
    """
    if isinstance(value, (datetime, date, dtime)):
        return value.isoformat()
    raise TypeError(f"实时通道无法序列化 {type(value).__name__}：{value!r}")


async def _close(websocket: WebSocket, code: int, reason: str) -> None:
    try:
        await websocket.close(code=code, reason=reason)
    except Exception:  # noqa: BLE001 - 连接可能已断开，关闭失败无所谓
        pass


async def _push_snapshot(websocket: WebSocket, db: AsyncSession, user_id: int, sid: int) -> None:
    """推一次「当前完整状态」：编排状态 + 当前题 + 数字人配置。

    供三种场合复用：连接建立、客户端主动 ``snapshot``、以及**重连后恢复**。
    """
    session_dict = await _session_dict(db, user_id, sid)
    # 进程内无编排记录时按库中状态登记一个起始态（见 adopt_state）——
    # 否则首帧 state 事件是 state=None，前端无法区分「准备中」与「没有记录」。
    adopt_state(sid, session_dict)

    await _send(websocket, state_event(sid))

    question: Optional[Dict[str, Any]] = None
    all_answered = False
    try:
        current = await interview_service.get_current_question(db, user_id, sid)
        question = current.get("question") or None
        all_answered = bool(current.get("all_answered"))
    except HTTPException:
        # 「尚未开始」等状态会以 400/404 拒绝 —— 这不是错误，只是「当前没有待答题」
        pass

    await _send(websocket, question_event(session_dict, question, all_answered))
    await _send(websocket, avatar_event(None))


async def _session_dict(db: AsyncSession, user_id: int, sid: int) -> Optional[Dict[str, Any]]:
    try:
        detail = await interview_service.get_session_detail(db, user_id, sid)
        return detail.get("session")
    except HTTPException:
        return None


#: 库中会话状态 → 编排状态（**仅在进程内无编排记录时**用于登记起始态）。
#: 见 :func:`adopt_state` 的说明。
_ADOPTED_STATES: Dict[str, str] = {
    "created": orch.STATE_WAITING_QUESTION,
    "ongoing": orch.STATE_WAITING_ANSWER,
    "finished": orch.STATE_FINISHED,
}


def adopt_state(session_id: int, session: Optional[Dict[str, Any]]) -> Optional[str]:
    """进程内**无**编排记录时，按库中会话状态登记一个起始态；已有记录则原样返回。

    为什么需要：编排状态是 ``interview_orchestrator`` 的**进程内**表，而会话是
    HTTP ``POST /api/interview/create`` 建的——``modules/interview/api.py`` **刻意不 import
    编排层**（守卫断言），所以第一帧快照里 ``state`` 会是 ``None``。
    ``None`` 与「准备中」在界面上无法区分；进程重启后更会把一个已结束的会话
    显示成「准备中」，用户点「开始面试」只会收到一个 400。

    映射刻意**保守**（宁可停在「等待作答」，也不猜「正在播报」）::

        created  → waiting_question   尚未出题
        ongoing  → waiting_answer     题已交付、用户欠一个回答（唯一可继续推进的稳定态）
        finished → finished           终态

    未知状态 ⇒ **不登记**（返回 ``None``）——不猜。已有记录 ⇒ **不改**（进程内状态优先）。
    """
    current = orch.get_state(session_id)
    if current is not None:
        return current
    if not isinstance(session, dict):
        return None
    target = _ADOPTED_STATES.get(session.get("status"))
    if target is None:
        return None
    return orch.set_state(session_id, target)


# ============================================================
# 三·五、生产化接线（超时 / 清理 / 并发 / 限流 / 日志 / 指标）
# ============================================================
# ★ 为什么「清理编排状态」的回调注册在**本模块**而不是 ``interview_production``：
#   ``tests/test_interview_ws.py`` 断言「编排层的消费者收口为唯一闭集
#   ``["modules/interview/ws.py"]``」——即 ``services/interview_production.py``
#   **不得** import 编排层。于是分工是：
#       interview_production 只回答「哪些 session_id 过期了」；
#       本模块（本来就 import 编排层）注册「过期了就 reset_state」。
#   ``register_evict_hook`` 幂等 ⇒ 重复注册安全。
prod.get_tracker().register_evict_hook(orch.reset_state)


async def _sweep_expired_sessions() -> None:
    """机会式清理：每次有新连接建立时顺手淘汰「过期 + 容量超限」的会话。

    刻意**不做后台定时任务**：那会在 ``lifespan`` 里引入新的启动顺序依赖，
    而「有人来连」本来就是「该清旧账了」的天然信号。清理失败**不阻断连接**。

    直接同步调用即可：``sweep()`` 只是几次内存字典操作 + 一把锁（微秒级），
    扔进线程池反而增加调度开销与「回调里再 await」的复杂度。
    """
    try:
        prod.get_tracker().sweep()
    except Exception as exc:  # noqa: BLE001 - 清理失败不该影响本次连接
        prod.log_event(
            "session_sweep_failed",
            level=logging.WARNING,
            error=f"{type(exc).__name__}: {exc}",
        )


async def _handle_start(websocket: WebSocket, db: AsyncSession, user_id: int, sid: int) -> None:
    """``start``：Avatar 启动预检 → 开始面试（生成题目 + 播报第一题）。

    ★ 第一行做**顺序守卫**——与 ``orch.answer_from_asr`` 同一条红线：顺序不对就
    **不打 provider、也不打服务层**。否则会白建一次数字人会话，而且前端会先收到
    一个无意义的 ``avatar`` 事件再收到 ``state_error``，界面状态对不上。

    ★ 出题走 ``orch.start_questions``（**策略化入口**，默认 ``mode="rule"``）：
    与原先的 ``orch.start`` **行为等价**（规则策略内部即 ``build_question_plan``，
    整场批量、逐字段一致），区别只是「题目怎么来」由策略层决定、后续可换。
    """
    orch.assert_can(sid, orch.EVENT_QUESTION_READY)

    ready = await orch.prepare_avatar(db, user_id, sid)
    await _send(websocket, avatar_event(ready))
    if not ready.get("ok"):
        # 数字人不可用**不阻断**面试（与通道层降级约定一致）：继续出题，交付退化为文字
        await _send(
            websocket,
            error_event(ERROR_STATE, f"数字人未就绪（{ready.get('status')}），已降级为文字交付"),
        )

    started = await orch.start_questions(db, user_id, sid)
    await _send(websocket, state_event(sid))
    await _send(
        websocket,
        question_event(started.get("session"), started.get("question"), False),
    )


async def _handle_answer(
    websocket: WebSocket, db: AsyncSession, user_id: int, sid: int, msg: Dict[str, Any]
) -> None:
    """``answer``：语音（ASR）或打字 → 评分 → 交付下一题。"""
    mode = msg.get("mode") or ANSWER_MODE_TEXT

    if mode == ANSWER_MODE_ASR:
        out = await orch.answer_from_asr(db, user_id, sid)
        asr = out.get("asr")
        if not out.get("ok"):
            # ASR 没取到文本（网络 / 空结果 / 会话失效）⇒ **不提交**，状态不变
            prod.METRICS.incr(prod.M_ANSWER_REJECTED)
            prod.log_event(
                "answer_rejected",
                level=logging.WARNING,
                session_id=sid,
                user_id=user_id,
                mode=ANSWER_MODE_ASR,
                status=(asr or {}).get("status"),
                error=out.get("error") or "语音回答未取到文本",
            )
            await _send(
                websocket,
                answer_event(
                    ANSWER_REJECTED,
                    source=None,
                    asr=asr,
                    error=out.get("error") or "语音回答未取到文本",
                ),
            )
            await _send(websocket, state_event(sid))
            return
        result = out.get("result") or {}
        prod.METRICS.incr(prod.M_ANSWER_SUBMITTED)
        prod.log_event(
            "answer_submitted",
            session_id=sid,
            user_id=user_id,
            mode=ANSWER_MODE_ASR,
            source=out.get("source"),
            chars=len(out.get("text") or ""),
        )
        await _send(
            websocket,
            answer_event(
                ANSWER_SUBMITTED,
                answer=result.get("answer"),
                text=out.get("text") or "",
                source=out.get("source"),
                asr=asr,
            ),
        )
    else:
        text = msg.get("text") or ""
        if not text:
            prod.METRICS.incr(prod.M_ANSWER_REJECTED)
            await _send(
                websocket,
                answer_event(
                    ANSWER_REJECTED,
                    source=ANSWER_MODE_TEXT,
                    error="打字作答必须带非空 text",
                ),
            )
            return
        result = await orch.submit_answer(
            db, user_id, sid, AnswerSubmitRequest(answer_text=text)
        )
        prod.METRICS.incr(prod.M_ANSWER_SUBMITTED)
        prod.log_event(
            "answer_submitted",
            session_id=sid,
            user_id=user_id,
            mode=ANSWER_MODE_TEXT,
            source=ANSWER_MODE_TEXT,
            chars=len(text),
        )
        await _send(
            websocket,
            answer_event(
                ANSWER_SUBMITTED,
                answer=result.get("answer"),
                text=text,
                source=ANSWER_MODE_TEXT,
            ),
        )

    await _send(websocket, score_event(result.get("answer")))
    await _send(
        websocket,
        question_event(
            result.get("session"), result.get("next_question"), bool(result.get("all_answered"))
        ),
    )
    await _send(websocket, state_event(sid))


async def _handle_finish(websocket: WebSocket, db: AsyncSession, user_id: int, sid: int) -> None:
    """``finish``：结束面试并生成报告。"""
    ended = await orch.finish(db, user_id, sid)
    await _send(
        websocket,
        {"type": EVENT_REPORT, "session": ended.get("session"), "report": ended.get("report")},
    )
    await _send(websocket, state_event(sid))


async def _handle_speech_done(websocket: WebSocket, sid: int) -> None:
    """``speech_done``：播报结束（由前端播放完成后回调）。"""
    orch.speech_done(sid)
    await _send(websocket, state_event(sid))


@router.post(
    "/api/interview/{session_id}/ws-ticket",
    summary="获取 WebSocket 连接一次性凭证",
    description=(
        "浏览器 WebSocket 无法自定义请求头，长期 Bearer Token 走 URL query "
        "会被代理 / 网关的访问日志记录下来。本接口签发**短时（30 秒）、"
        "单次使用**的 ticket，供 WS 握手时代替长期 token。"
        "签发前先做会话归属校验：不是本人的会话一律 404（与 HTTP 面试接口同口径）。"
    ),
    tags=["AI 面试 · 实时通道"],
)
async def create_ws_ticket_endpoint(
    session_id: int = Path(..., ge=1, description="面试会话 id"),
    current_user: dict = Depends(get_current_user),
    auth_session=Depends(get_optional_session),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """签发 WS 连接 ticket（归属校验通过后才发放）。

    存储不可用时返回 **503**（不是 500，也不是"发一张废票"）：
    503 语义上就是"服务暂时不可用，请稍后重试"，且前端不会把它当成鉴权失败。

    ★ ``get_optional_session`` 只用来拿 ``user_session.id``（写进 ticket，
    供连接存续期间复核撤销）。它**永不抛 401**——鉴权仍由 ``get_current_user``
    负责，因此既有的 ``dependency_overrides[get_current_user]`` 覆盖链不受影响
    （那类测试没有 Authorization 头，这里拿到 ``None``，ticket 退化为不绑定会话）。
    """
    # 归属校验复用 service 口径：会话不存在 / 不属于当前用户 → 404。
    await interview_service.get_session_detail(db, current_user["user_id"], session_id)
    try:
        ticket = await create_ws_ticket(
            db,
            session_id,
            current_user["user_id"],
            auth_session_id=None if auth_session is None else auth_session.id,
        )
    except ticket_store.TicketStoreUnavailable as exc:
        prod.log_event(
            "ws_ticket_issue_unavailable",
            level=logging.ERROR,
            session_id=session_id,
            user_id=current_user["user_id"],
            error=str(exc),
        )
        raise HTTPException(
            status_code=503, detail="连接凭证服务暂时不可用，请稍后重试"
        ) from exc
    return {"ticket": ticket, "expires_in": _TICKET_TTL_SECONDS}


@router.websocket(WS_PATH)
async def interview_socket(
    websocket: WebSocket,
    session_id: int,
    db: AsyncSession = Depends(get_db),
) -> None:
    """数字人面试实时通道。

    协议（详见模块文档与 ``SERVER_EVENTS`` / ``CLIENT_MESSAGES``）::

        连接      ws://…/api/interview/{id}/ws?token=…
        服务端推  hello → state → question → avatar
        客户端发  {"type":"start"} / {"type":"speech_done"}
                  {"type":"answer","mode":"asr"}            ← 语音作答
                  {"type":"answer","mode":"text","text":"…"} ← 打字作答
                  {"type":"finish"} / {"type":"snapshot"} / {"type":"ping"}

    关闭码：``4401`` 未登录 / ``4403`` 无权限 / ``4404`` 会话不存在 /
    ``4408`` 空闲超时 / ``4409`` 并发超限（后两者前端**不重连**）。
    """
    # 先 accept 再校验：未登录时能给客户端一个**明确的关闭码**（4401），
    # 而不是让浏览器只看到「握手失败」。校验口径与 deps.get_current_user 一致。
    await websocket.accept()

    # ★ Origin 校验（在鉴权之前）：带了 Origin 却不在白名单 ⇒ 直接拒绝。
    #   浏览器跨站页面无法读取本站 localStorage 里的 token，这条是**纵深防御**，
    #   不是替代鉴权。
    origin = websocket.headers.get("origin", "")
    if not origin_allowed(origin, websocket.headers.get("host", "")):
        prod.METRICS.incr(prod.M_WS_UNAUTHORIZED)
        prod.log_event(
            "ws_origin_rejected",
            level=logging.WARNING,
            session_id=session_id,
            origin=origin,
        )
        await _send(websocket, error_event(ERROR_UNAUTHORIZED, "来源不被允许"))
        await _close(websocket, WS_CLOSE_FORBIDDEN, "来源不被允许")
        return

    # 生产化策略（**默认值即现状**；配置非法时降级为默认值并**响亮记录**，不中断面试）
    config = prod.safe_config()
    tracker = prod.get_tracker()
    gate = prod.get_gate()
    mutex = prod.get_mutex()
    limiter = prod.get_limiter()
    # 再注册一次（**幂等**）：``prod.reset_runtime()`` 会重建 tracker 实例，
    # 模块级那次注册就落在旧实例上了。放在这里 ⇒ 无论实例被重建多少次，
    # 「会话淘汰 ⇒ 清编排状态」这条因果链始终成立。
    tracker.register_evict_hook(orch.reset_state)

    token = websocket.query_params.get("token", "")
    ticket = websocket.query_params.get("ticket", "")

    user_id: Optional[int] = None
    #: 用于连接存续期间复核登录态的长期 token（ticket 路径下为空）
    auth_token = ""
    #: 绑定的**登录会话** id（ticket 路径来自 ticket；token 路径来自会话行）
    auth_session_id: Optional[int] = None
    if ticket:
        try:
            consumed = await verify_ws_ticket(db, ticket, session_id)
        except ticket_store.TicketStoreUnavailable as exc:
            # ★ 安全失败：既**不**降级到 token 路径（那是静默降级），
            #   也**不**当作"未登录"（那会把运维事故伪装成登录过期）。
            prod.METRICS.incr(prod.M_WS_UNAUTHORIZED)
            prod.log_event(
                "ws_ticket_store_unavailable",
                level=logging.ERROR,
                session_id=session_id,
                error=str(exc),
            )
            await _send(
                websocket,
                error_event(ERROR_SERVICE, "连接凭证校验暂时不可用，请稍后重试"),
            )
            await _close(websocket, WS_CLOSE_UNAVAILABLE, "凭证存储暂时不可用")
            return
        if consumed is not None:
            user_id = consumed.user_id
            auth_session_id = consumed.auth_session_id

    if user_id is None and token:
        if token_auth_enabled():
            user = await resolve_user(db, token)
            if user is not None:
                user_id = user["user_id"]
                auth_token = token
                auth_session_id = user.get("session_id")
        else:
            # 兼容路径已被显式关闭：**不区分**「开关关了」与「凭证无效」，
            # 统一落到下面的 4401 —— 不给探测者「这台机器关了 token 登录」这种信号。
            prod.log_event(
                "ws_token_auth_rejected",
                level=logging.WARNING,
                session_id=session_id,
                reason="token_auth_disabled",
            )

    if user_id is None:
        prod.METRICS.incr(prod.M_WS_UNAUTHORIZED)
        prod.log_event("ws_unauthorized", level=logging.WARNING, session_id=session_id)
        await _send(websocket, error_event(ERROR_UNAUTHORIZED, "登录状态无效或已过期，请重新登录"))
        await _close(websocket, WS_CLOSE_UNAUTHORIZED, "登录状态无效或已过期")
        return

    # 归属校验：复用 service 的既有口径（不存在 / 非本人一律 404，不泄露他人会话存在性）
    try:
        detail = await interview_service.get_session_detail(db, user_id, session_id)
    except HTTPException as exc:
        prod.METRICS.incr(prod.M_WS_SESSION_NOT_FOUND)
        await _send(websocket, error_event(ERROR_SESSION_NOT_FOUND, str(exc.detail)))
        await _close(websocket, WS_CLOSE_NOT_FOUND, "会话不存在或不属于当前用户")
        return

    # 机会式清理（放在鉴权之后：不让人靠乱连触发清理动作）
    await _sweep_expired_sessions()

    # 并发闸门：先算连接数，再算会话数（后者超限要把前者还回去，否则额度泄漏）
    if not gate.acquire_connection(user_id):
        prod.METRICS.incr(prod.M_WS_REJECTED_BUSY)
        prod.log_event(
            "ws_rejected_busy",
            level=logging.WARNING,
            session_id=session_id,
            user_id=user_id,
            reason="too_many_connections",
            limit=config.max_connections_per_user,
        )
        await _send(
            websocket,
            error_event(ERROR_BUSY, f"同时打开的面试窗口过多（上限 {config.max_connections_per_user}）"),
        )
        await _close(websocket, WS_CLOSE_BUSY, "并发连接数超限")
        return
    if not gate.acquire_session(user_id, session_id):
        gate.release_connection(user_id)
        prod.METRICS.incr(prod.M_WS_REJECTED_BUSY)
        prod.log_event(
            "ws_rejected_busy",
            level=logging.WARNING,
            session_id=session_id,
            user_id=user_id,
            reason="too_many_sessions",
            limit=config.max_sessions_per_user,
        )
        await _send(
            websocket,
            error_event(ERROR_BUSY, f"同时进行的面试过多（上限 {config.max_sessions_per_user}）"),
        )
        await _close(websocket, WS_CLOSE_BUSY, "并发会话数超限")
        return

    tracker.touch(session_id)
    prod.METRICS.incr(prod.M_WS_CONNECT)
    prod.METRICS.set_gauge(prod.M_WS_ACTIVE, gate.active_connections())
    prod.log_event(
        "ws_connect",
        session_id=session_id,
        user_id=user_id,
        interview_mode=detail["session"].get("interview_mode"),
        idle_timeout=config.idle_timeout_seconds,
    )

    # ★ 登记到本进程的连接表：撤销广播到达时，本进程才有办法**主动断开**它。
    #   这张表只在本进程内有效（连接对象不可能跨进程），跨实例的「通知」由
    #   services/ws_revocation 的只追加表承担。
    conn_id = register_active_conn(user_id, auth_session_id, websocket)

    try:
        await _send(
            websocket,
            {
                "type": EVENT_HELLO,
                "session_id": session_id,
                "interview_mode": detail["session"].get("interview_mode"),
                "session_status": detail["session"].get("status"),
                "server_events": list(SERVER_EVENTS),
                "client_messages": list(CLIENT_MESSAGES),
                "answer_modes": list(ANSWER_MODES),
            },
        )
        await _push_snapshot(websocket, db, user_id, session_id)

        last_auth_check = time.monotonic()
        while True:
            # ★ 空闲超时：超时后**服务端主动关闭**，把「拔网线 / 进程被杀 / 浏览器崩溃」
            #   这类**不会发 FIN** 的半开连接真正释放掉（前端有心跳，活着的连接不会命中）。
            try:
                raw = await asyncio.wait_for(
                    websocket.receive_json(), timeout=config.idle_timeout_seconds
                )
            except asyncio.TimeoutError:
                prod.METRICS.incr(prod.M_WS_IDLE_CLOSED)
                prod.log_event(
                    "ws_idle_timeout",
                    level=logging.WARNING,
                    session_id=session_id,
                    user_id=user_id,
                    idle_seconds=config.idle_timeout_seconds,
                )
                await _send(
                    websocket,
                    error_event(ERROR_STATE, "长时间无操作，实时通道已关闭；重新进入面试间即可恢复"),
                )
                await _close(websocket, WS_CLOSE_IDLE, "空闲超时")
                return

            # 任何一条客户端消息都算「活跃」（含 ping / snapshot）——心跳即续期。
            tracker.touch(session_id)
            prod.METRICS.incr(prod.M_WS_MESSAGES)

            # ★ 醒来先看自己是不是已经被**主动断开**了（撤销广播 / 空闲超时）：
            #   是就直接收尾，别再 send（否则会撞上 "close message has been sent"）。
            if _conn_closed(conn_id):
                return

            # ★ 连接存续期间的登录态复核（**按时间节流**，不是每条消息都查库）：
            #   撤销 / 过期最多延迟 AUTH_RECHECK_SECONDS 生效，而不是等空闲超时。
            now = time.monotonic()
            if now - last_auth_check >= AUTH_RECHECK_SECONDS:
                last_auth_check = now
                if not await _auth_still_valid(
                    db,
                    auth_token=auth_token,
                    auth_session_id=auth_session_id,
                    user_id=user_id,
                ):
                    prod.METRICS.incr(prod.M_WS_UNAUTHORIZED)
                    prod.log_event(
                        "ws_auth_revoked",
                        level=logging.WARNING,
                        session_id=session_id,
                        user_id=user_id,
                    )
                    await _send(
                        websocket,
                        error_event(ERROR_UNAUTHORIZED, "登录状态已失效，请重新登录"),
                    )
                    await _close(websocket, WS_CLOSE_UNAUTHORIZED, "登录状态已失效")
                    return

            msg = normalize_client_message(raw)
            msg_type = msg["type"]

            if msg_type is None:
                await _send(websocket, error_event(ERROR_BAD_MESSAGE, msg["error"] or "无法解析的消息"))
                continue

            if msg_type == CLIENT_PING:
                await _send(websocket, {"type": EVENT_PONG})
                continue

            if msg_type == CLIENT_SNAPSHOT:
                await _push_snapshot(websocket, db, user_id, session_id)
                continue

            # 限流（**默认关闭**：``INTERVIEW_ANSWER_MIN_INTERVAL`` 不配即为 0 ⇒ 永不触发）
            if msg_type == CLIENT_ANSWER and not limiter.allow(("answer", user_id)):
                prod.METRICS.incr(prod.M_ANSWER_RATE_LIMITED)
                prod.log_event(
                    "answer_rate_limited",
                    level=logging.WARNING,
                    session_id=session_id,
                    user_id=user_id,
                    retry_after=round(limiter.retry_after(("answer", user_id)), 3),
                )
                await _send(
                    websocket,
                    answer_event(ANSWER_REJECTED, source=msg.get("mode") or None,
                                 error="提交过于频繁，请稍候再试"),
                )
                continue

            # ★ 高风险操作**执行前**再验一次会话。
            #   为什么不能只靠「建连时鉴权一次」+ 60s 节流复核：
            #   作答会写库、结束会生成报告——被踢下线的人在这两个动作上
            #   造成的后果是**持久**的（数据 / 报告），不像读快照那样可以重来。
            #   成本是一条主键（或 token 唯一索引）查询，远低于「撤销后仍能写数据」。
            if msg_type in (CLIENT_ANSWER, CLIENT_FINISH):
                if not await _auth_still_valid(
                    db,
                    auth_token=auth_token,
                    auth_session_id=auth_session_id,
                    user_id=user_id,
                ):
                    prod.METRICS.incr(prod.M_WS_UNAUTHORIZED)
                    prod.log_event(
                        "ws_auth_revoked_on_high_risk",
                        level=logging.WARNING,
                        session_id=session_id,
                        user_id=user_id,
                        msg_type=msg_type,
                    )
                    await _send(
                        websocket,
                        error_event(ERROR_UNAUTHORIZED, "登录状态已失效，请重新登录"),
                    )
                    await _close(websocket, WS_CLOSE_UNAUTHORIZED, "登录状态已失效")
                    return

            # ★ 每会话非阻塞互斥：两个标签页同时作答时，只放行先到的那个
            #   （排队会让用户看到「点了没反应」，直接拒绝反而能给出可理解的反馈）。
            if not mutex.try_enter(session_id):
                prod.METRICS.incr(prod.M_WS_REJECTED_BUSY)
                await _send(websocket, error_event(ERROR_BUSY, "上一条操作还在处理中，请稍候"))
                continue

            try:
                if msg_type == CLIENT_START:
                    await _handle_start(websocket, db, user_id, session_id)
                elif msg_type == CLIENT_SPEECH_DONE:
                    await _handle_speech_done(websocket, session_id)
                elif msg_type == CLIENT_ANSWER:
                    await _handle_answer(websocket, db, user_id, session_id, msg)
                elif msg_type == CLIENT_FINISH:
                    await _handle_finish(websocket, db, user_id, session_id)
            except orch.OrchestratorStateError as exc:
                # 顺序不对：**不改状态**，只把原因告诉前端（前端据此调整按钮可用性）
                prod.METRICS.incr(prod.M_STATE_ERRORS)
                await _send(websocket, error_event(ERROR_STATE, str(exc)))
                await _send(websocket, state_event(session_id))
            except HTTPException as exc:
                # 业务校验失败（空答案 / 重复作答 / 状态不符）：service 已回滚编排状态
                code = ERROR_EMPTY_ANSWER if "为空" in str(exc.detail) else ERROR_SERVICE
                prod.METRICS.incr(prod.M_SERVICE_ERRORS)
                await _send(websocket, error_event(code, str(exc.detail)))
                await _send(websocket, state_event(session_id))
            except WebSocketDisconnect:
                raise
            except Exception as exc:  # noqa: BLE001 - 单条消息失败不该拖垮整条连接
                prod.METRICS.incr(prod.M_SERVICE_ERRORS)
                prod.log_event(
                    "ws_message_failed",
                    level=logging.ERROR,
                    session_id=session_id,
                    user_id=user_id,
                    msg_type=msg_type,
                    error=f"{type(exc).__name__}: {exc}",
                )
                await _send(
                    websocket,
                    error_event(ERROR_INTERNAL, f"{type(exc).__name__}: {exc}"),
                )
            finally:
                mutex.exit(session_id)
    except WebSocketDisconnect:
        # 客户端断开是**正常事件**：会话与编排状态都还在（进程内状态不落库），
        # 重新连上后 `snapshot` 即可恢复；因此这里**不做任何业务动作**。
        return
    finally:
        # ★ 额度必须在 finally 释放：鉴权失败 / 超时 / 异常 / 正常断开都要还回去，
        #   否则额度会随连接次数单调泄漏，最终把用户自己挡在门外。
        # ★ 会话活跃记录**刻意不 forget**：它由 TTL 淘汰（淘汰时才 reset 编排状态），
        #   这样「断开后 5 分钟内重连」仍能拿到原状态；而 `acquire_session` 是幂等的。
        unregister_active_conn(conn_id)
        gate.release_connection(user_id)
        gate.release_session(user_id, session_id)
        prod.METRICS.set_gauge(prod.M_WS_ACTIVE, gate.active_connections())
        prod.METRICS.incr(prod.M_WS_CLOSE)
        prod.log_event("ws_disconnect", session_id=session_id, user_id=user_id)
