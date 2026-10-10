# -*- coding: utf-8 -*-
"""AI 模拟面试 · **实时通道（WebSocket）** —— 数字人视频面试的交互链路。

为什么需要它
------------
HTTP 是「请求 → 响应」的：前端每次都得自己决定「现在该调哪个接口」。
数字人面试却是一条**有状态的时间线**（播报中 → 等待作答 → 评分中 → 下一题），
中途还有服务端主动产生的状态（数字人会话就绪 / 流地址 / 播报结束）。
WebSocket 让服务端**主动推送**这些状态，前端只需渲染。

职责边界（与 ``api/interview.py`` 完全一致）
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
（``api/interview.py`` / ``main.py`` / ``services/*`` 一律不得）。
即：编排层仍是「可选的」，只是这个**新**路由显式选了它。

鉴权
----
浏览器 ``WebSocket`` **无法自定义请求头**，因此 token 走 query：
``ws://host/api/interview/{session_id}/ws?token=<登录 token>``。

顺序刻意是 **先 ``accept()`` 再校验**：这样未登录时能给客户端一个**明确的关闭码**
（``4401``），而不是让浏览器只看到一个「握手失败」。校验口径与
``deps.get_current_user`` **完全一致**（token 命中 ``user_session`` 表即视为登录）。

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
from datetime import date, datetime, time
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import UserSession
from schemas.interview import AnswerSubmitRequest
from services import interview_orchestrator as orch
from services import interview_production as prod
from services import interview_service

router = APIRouter(tags=["AI 面试 · 实时通道"])

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

    延迟 import ``main``：``main`` 会 include 本路由，模块顶层 import 会成环。
    """
    try:
        from main import avatar_status  # noqa: PLC0415

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
    """按 token 解析登录态；无效返回 ``None``（**不抛异常**，由调用方决定关闭码）。"""
    if not token:
        return None
    result = await db.execute(select(UserSession).where(UserSession.token == token))
    session = result.scalar_one_or_none()
    if session is None:
        return None
    return {"user_id": session.user_id, "email": session.email, "role": session.role}


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
    """
    await websocket.send_text(
        json.dumps(payload, ensure_ascii=False, default=_json_default)
    )


def _json_default(value: Any) -> str:
    """``json.dumps`` 的 ``default``：日期时间 → ISO 字符串；其余**显式报错**。"""
    if isinstance(value, (datetime, date, time)):
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
    HTTP ``POST /api/interview/create`` 建的——``api/interview.py`` **刻意不 import
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
#   ``["api/interview_ws.py"]``」——即 ``services/interview_production.py``
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
    user = await resolve_user(db, token)
    if user is None:
        prod.METRICS.incr(prod.M_WS_UNAUTHORIZED)
        prod.log_event("ws_unauthorized", level=logging.WARNING, session_id=session_id)
        await _send(websocket, error_event(ERROR_UNAUTHORIZED, "登录状态无效或已过期，请重新登录"))
        await _close(websocket, WS_CLOSE_UNAUTHORIZED, "登录状态无效或已过期")
        return

    user_id = user["user_id"]

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
        gate.release_connection(user_id)
        gate.release_session(user_id, session_id)
        prod.METRICS.set_gauge(prod.M_WS_ACTIVE, gate.active_connections())
        prod.METRICS.incr(prod.M_WS_CLOSE)
        prod.log_event("ws_disconnect", session_id=session_id, user_id=user_id)
