# -*- coding: utf-8 -*-
"""AI 模拟面试 · **实时通道（WebSocket）** 自检 —— 数字人视频面试的完整交互闭环。

无需 pytest，直接运行：
    python backend/tests/test_interview_ws.py

本套件走**真实的 FastAPI WebSocket 栈**（``starlette.testclient.TestClient``
的进程内 ASGI 传输），因此会一并验证：路由注册、query token 鉴权、
归属校验、事件推送顺序、以及「客户端消息 → 编排器 → 事件」的完整闭环。

覆盖
----
[1]  协议契约：事件/消息/作答方式的字面量集合；``WS_PATH`` 与**前端常量逐字一致**
[2]  纯函数：``normalize_client_message`` 穷举；``avatar_event`` 的「未探测 ≠ 探测失败」；
     ``score_event`` / ``error_event`` / ``question_event`` 键集
[3]  鉴权与归属：无 token / 错 token → ``error(unauthorized)`` + 关闭码 **4401**；
     他人会话 / 不存在的会话 → **4404**（不泄露他人会话存在性）
[4]  **端到端闭环（视频模式）**：hello → 快照 → start（Avatar 就绪 + 播报第一题）
     → speech_done → 语音作答（ASR）→ answer + score + question(下一题) + state
     → 第二题 → finish → report
[5]  打字作答路径（``mode=text``）：提交成功；空文本 → ``answer(rejected)`` **不落库**
[6]  错误分支：未知消息 → ``bad_message``；顺序不对 → ``state_error``（**状态不被污染**）；
     ``ping`` → ``pong``；``snapshot`` → 三连推
[7]  文字面试**完全不受影响**：HTTP 文字流程照常；WS 路由与既有 HTTP 路由互不干扰
[8]  边界守卫（AST）：**只有** ``modules/interview/ws.py`` import 编排层（守住「可选层」性质）；
     本模块不 import provider；``modules/interview/api.py`` 未被改动

数据库使用**临时文件** SQLite（``NullPool``，跨事件循环安全），不触碰本机 MySQL。
``AVATAR_PROVIDER`` 由 ``regression_env`` 钉成空串 ⇒ 数字人走 **Mock**，零联网。
"""

import ast
import asyncio
import gc
import os
import pathlib
import shutil
import sys
import tempfile

_TMP_DIR = tempfile.mkdtemp(prefix="career_ai_ws_test_")
_DB_PATH = pathlib.Path(_TMP_DIR) / "ws_test.db"

os.environ.setdefault("DATABASE_URL", f"sqlite+aiosqlite:///{_DB_PATH.as_posix()}")

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))
sys.path.insert(0, str(_BACKEND / "tests"))

import regression_env  # noqa: E402,F401  （钉住 AVATAR_* ⇒ 数字人走 Mock，不联网）

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.pool import NullPool  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402
from starlette.websockets import WebSocketDisconnect  # noqa: E402

import main  # noqa: E402
import models  # noqa: E402,F401
from modules.interview import ws as ws  # noqa: E402
from database import Base, get_db  # noqa: E402
from models import Job, User, UserSession  # noqa: E402
from services import avatar_interview, interview_orchestrator as orch  # noqa: E402
from services.avatar_provider import MockAvatarProvider  # noqa: E402

DB_URL = f"sqlite+aiosqlite:///{_DB_PATH.as_posix()}"
TOKEN = "ws-test-token-0001"
OTHER_TOKEN = "ws-test-token-0002"
ASR_TEXT = "我负责过订单中台重构，用 Redis 做缓存，QPS 从 800 提升到 5000。"
TYPED_TEXT = "我熟悉 Python 与 MySQL，做过订单中台的性能优化。"

_PASSED = 0
_FAILED = 0


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(
        ("  [PASS] " if cond else "  [FAIL] ")
        + name
        + (f"  -> {detail}" if detail and not cond else "")
    )
    return cond


def _all_imports(source: str):
    """全量 import（含函数体内延迟导入）——判「从不 import X」必须走 AST。

    子串匹配会被 docstring 里提到的模块名误伤（本项目已复发多次的坑）。
    """
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _prepare_db():
    """建表 + 种子数据（两个用户 + 一条会话 token）。"""

    async def _run():
        engine = create_async_engine(DB_URL, poolclass=NullPool)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as db:
            owner = User(username="ws_owner", email="ws_owner@example.com",
                         password_hash="x", role="user")
            other = User(username="ws_other", email="ws_other@example.com",
                         password_hash="x", role="user")
            job = Job(
                job_name="后端开发工程师", salary="20-35K", edu_require="本科",
                major_require="不限", skills="Python,MySQL,Redis",
                duty="负责后端服务的设计与开发", city="深圳", industry="互联网",
            )
            db.add_all([owner, other, job])
            await db.commit()
            await db.refresh(owner)
            await db.refresh(other)
            await db.refresh(job)
            db.add_all([
                UserSession(user_id=owner.id, token=TOKEN, email=owner.email, role="user"),
                UserSession(user_id=other.id, token=OTHER_TOKEN, email=other.email, role="user"),
            ])
            await db.commit()
            return owner.id, other.id, job.id
        await engine.dispose()

    return asyncio.run(_run())


def _recv(client, count: int):
    """收 ``count`` 条事件（返回 list[dict]）。

    刻意用**精确条数**而不是「读到某个标记为止」：事件的条数与顺序本身就是本套件
    要验证的契约，用宽松的 drain 会把「多推/少推一条」悄悄吞掉。
    """
    return [client.receive_json() for _ in range(count)]


def _types(events):
    return [e.get("type") for e in events]


# ============================================================
# 主流程
# ============================================================
def run() -> bool:  # noqa: C901 - 自检脚本，顺序即文档
    print("=" * 68)
    print("AI 模拟面试 · 实时通道（WebSocket）自检")
    print("=" * 68)

    # ------------------------------------------------------------
    # [1] 协议契约
    # ------------------------------------------------------------
    print("\n[1] 协议契约（事件 / 消息 / 路径）")
    _check("服务端事件集恒定（9 个）",
           ws.SERVER_EVENTS == ("hello", "state", "avatar", "question", "answer",
                                "score", "report", "error", "pong"),
           str(ws.SERVER_EVENTS))
    _check("客户端消息集恒定（6 个）",
           ws.CLIENT_MESSAGES == ("start", "speech_done", "answer", "finish",
                                  "snapshot", "ping"),
           str(ws.CLIENT_MESSAGES))
    _check("作答方式 = {asr, text}（语音 / 打字）",
           ws.ANSWER_MODES == ("asr", "text"), str(ws.ANSWER_MODES))
    _check("WS_PATH 与既有 HTTP 前缀一致（/api/interview/…）",
           ws.WS_PATH == "/api/interview/{session_id}/ws", ws.WS_PATH)
    _check("关闭码用 4xxx（避开标准码，语义明确）",
           (ws.WS_CLOSE_UNAUTHORIZED, ws.WS_CLOSE_FORBIDDEN, ws.WS_CLOSE_NOT_FOUND)
           == (4401, 4403, 4404))
    # 新版 FastAPI 延迟包含 router；通过公开反查 API 验证实际挂载路径，
    # 避免依赖 app.routes 必须是扁平列表这一内部实现。
    registered_path = str(main.app.url_path_for("interview_socket", session_id=123))
    _check("★ 路由已挂到 app 上（否则前端连不上）",
           registered_path == ws.WS_PATH.replace("{session_id}", "123"),
           registered_path)

    # ★ 前端镜像常量必须与后端逐字一致（防漂移：这是跨语言契约）
    fe_socket = (_BACKEND.parent / "frontend" / "src" / "api" / "interviewSocket.js")
    fe_src = fe_socket.read_text(encoding="utf-8")
    for literal in list(ws.SERVER_EVENTS) + list(ws.CLIENT_MESSAGES):
        _check(f"  └ 前端 interviewSocket.js 含事件字面量 {literal!r}",
               f"'{literal}'" in fe_src, "前端常量缺失或拼写不一致")
    _check("  └ 前端 WS 路径模板与后端 WS_PATH 一致",
           "'/api/interview/' " in fe_src or "/api/interview/${sessionId}/ws" in fe_src,
           "前端路径模板缺失")

    # ------------------------------------------------------------
    # [2] 纯函数（脱离连接）
    # ------------------------------------------------------------
    print("\n[2] 纯函数：消息归一 / 事件构造")
    ok_msg = ws.normalize_client_message({"type": "answer", "mode": "asr"})
    _check("合法消息归一：type/mode/text",
           ok_msg == {"type": "answer", "mode": "asr", "text": "", "error": None},
           str(ok_msg))
    _check("  └ text 一律 strip（纯空白视为空串）",
           ws.normalize_client_message({"type": "answer", "text": "  x  "})["text"] == "x")
    bad = ws.normalize_client_message({"type": "nope"})
    _check("未知消息类型 → type=None + error（**不抛异常**）",
           bad["type"] is None and bad["error"] is not None, str(bad))
    _check("  └ 非 dict → type=None + error",
           ws.normalize_client_message("not-a-dict")["type"] is None)
    _check("  └ 非 dict（list）同样被拦",
           ws.normalize_client_message([])["type"] is None)
    _check("非法 mode 回落空串（由调用方按 text 处理）",
           ws.normalize_client_message({"type": "answer", "mode": "bogus"})["mode"] == "")
    _check("未给 mode 时为空串（调用方默认按 text）",
           ws.normalize_client_message({"type": "answer", "text": "hi"})["mode"] == "")

    idle = ws.avatar_event(None)
    probed = ws.avatar_event({"ok": True, "status": "ok", "avatar_session_id": "a-1",
                              "stream_url": "mock://stream", "error": None})
    _check("★ 「未探测」≠「探测失败」：session_probed=False 且 ok=None",
           idle["session_probed"] is False and idle["ok"] is None and idle["status"] is None,
           str(idle))
    _check("  └ 探测成功：ok=True + 会话 id + stream_url",
           probed["session_probed"] is True and probed["ok"] is True
           and probed["avatar_session_id"] == "a-1" and probed["stream_url"] == "mock://stream")
    _check("  └ 两个信封**键集完全一致**（前端无需判键存在性）",
           set(idle) == set(probed), str(sorted(set(idle) ^ set(probed))))
    _check("  └ 都带配置层信息（provider / configured，来自 main.avatar_status）",
           "provider" in idle and "configured" in idle and idle["provider"] == "mock",
           str(idle))

    score = ws.score_event({"question_id": 7, "score": 82, "technical_score": 80,
                            "logic_score": 84, "expression_score": 78,
                            "adaptability_score": 86, "feedback": "结构清晰"})
    _check("score 事件摊平评分字段（前端不必再挖一层）",
           score["score"] == 82 and score["technical_score"] == 80
           and score["feedback"] == "结构清晰" and score["question_id"] == 7, str(score))
    _check("  └ answer=None 时不抛异常（键仍在，值为 None）",
           ws.score_event(None)["score"] is None and "feedback" in ws.score_event(None))
    _check("error 事件 = 稳定码 + 可读说明",
           ws.error_event("bad_message", "x") == {"type": "error", "code": "bad_message",
                                                  "message": "x"})
    q_evt = ws.question_event({"id": 1}, {"question": "Q"}, True)
    _check("question 事件原样带 service 的 session / question 字典（两条通道形状一致）",
           q_evt["session"] == {"id": 1} and q_evt["question"] == {"question": "Q"}
           and q_evt["all_answered"] is True)

    # ---- adopt_state：进程内无记录时按库中状态登记起始态 ----
    probe_sid = 990001
    orch.reset_state(probe_sid)
    _check("★ 库中 created + 无编排记录 ⇒ 登记为 waiting_question（不是 None）",
           ws.adopt_state(probe_sid, {"status": "created"}) == orch.STATE_WAITING_QUESTION)
    _check("  └ ★ 已有记录 ⇒ **不改**（进程内状态优先）",
           ws.adopt_state(probe_sid, {"status": "finished"}) == orch.STATE_WAITING_QUESTION,
           str(orch.get_state(probe_sid)))
    orch.reset_state(probe_sid)
    _check("  └ ongoing ⇒ waiting_answer（题已交付，用户欠一个回答）",
           ws.adopt_state(probe_sid, {"status": "ongoing"}) == orch.STATE_WAITING_ANSWER)
    orch.reset_state(probe_sid)
    _check("  └ finished ⇒ finished（终态不显示成「准备中」）",
           ws.adopt_state(probe_sid, {"status": "finished"}) == orch.STATE_FINISHED)
    orch.reset_state(probe_sid)
    _check("  └ ★ 未知状态 ⇒ **不登记**（返回 None，不猜）",
           ws.adopt_state(probe_sid, {"status": "bogus"}) is None
           and orch.get_state(probe_sid) is None)
    _check("  └ session 为 None ⇒ 不登记（不抛异常）",
           ws.adopt_state(probe_sid, None) is None)
    orch.reset_state(probe_sid)

    # ------------------------------------------------------------
    # [3]~[7] 真实 WebSocket 栈
    # ------------------------------------------------------------
    owner_id, other_id, job_id = _prepare_db()

    # ★ 依赖覆盖必须是 **async 生成器函数**：FastAPI 用
    #   ``inspect.isasyncgenfunction(call)`` 判断「这是不是一个 yield 依赖」，
    #   写成「同步函数返回异步生成器」会被当成普通可调用对象，于是路由收到的是
    #   一个 async generator 而不是 AsyncSession，第一次 await 就炸。
    #   NullPool ⇒ 每次会话新开连接，跨事件循环安全（TestClient 有自己的 loop）。
    _app_engine = create_async_engine(DB_URL, poolclass=NullPool)
    _app_factory = async_sessionmaker(
        _app_engine, class_=AsyncSession, expire_on_commit=False
    )

    async def override_get_db():
        async with _app_factory() as session:
            yield session

    main.app.dependency_overrides[get_db] = override_get_db

    avatar_interview.reset_provider(MockAvatarProvider(answer_text=ASR_TEXT))
    orch.clear_states()

    with TestClient(main.app) as client:
        from deps import get_current_user  # noqa: PLC0415

        def ws_url(session_id: int, token: str = TOKEN) -> str:
            return f"/api/interview/{session_id}/ws?token={token}"

        def as_user(fn):
            """以 owner 身份执行一次 HTTP 调用（临时覆盖 ``get_current_user``）。

            刻意用 **TestClient 自身的 `.post()`** 而不是 ``httpx.ASGITransport``：
            前者复用 TestClient 已经起好的 portal / 事件循环，后者会在**主线程**
            另起一个 loop，与 TestClient 的 loop 并存 —— 对文件 SQLite 尚可，
            但毫无必要，且 ``httpx.ASGITransport`` **只支持 AsyncClient**
            （同步 ``httpx.Client`` 会直接报「不支持该 transport」）。
            """
            main.app.dependency_overrides[get_current_user] = lambda: {
                "user_id": owner_id, "email": "ws_owner@example.com", "role": "user",
            }
            try:
                return fn()
            finally:
                main.app.dependency_overrides.pop(get_current_user, None)

        def create_session(mode: str, total_questions: int) -> int:
            resp = as_user(lambda: client.post(
                "/api/interview/create",
                json={"job_id": job_id, "mode": mode, "total_questions": total_questions},
            ))
            assert resp.status_code == 200, resp.text
            return resp.json()["session"]["id"]

        # ---- 建会话（HTTP，与文字面试同一条路） ----
        avatar_sid = create_session("avatar", 2)
        text_sid = create_session("text", 1)

        # ------------------------------------------------------------
        # [3] 鉴权与归属
        # ------------------------------------------------------------
        print("\n[3] 鉴权与归属（query token）")
        with client.websocket_connect(ws_url(avatar_sid, token="")) as sock:
            first = sock.receive_json()
            _check("无 token ⇒ 先收到 error(unauthorized)",
                   first["type"] == "error" and first["code"] == "unauthorized", str(first))
            code = None
            try:
                sock.receive_json()
            except WebSocketDisconnect as exc:
                code = exc.code
            _check("  └ 随后以关闭码 4401 关闭（不是「握手失败」）", code == 4401, str(code))

        with client.websocket_connect(ws_url(avatar_sid, token="bogus-token")) as sock:
            first = sock.receive_json()
            _check("错 token ⇒ 同样 unauthorized + 4401",
                   first["code"] == "unauthorized", str(first))
            code = None
            try:
                sock.receive_json()
            except WebSocketDisconnect as exc:
                code = exc.code
            _check("  └ 关闭码 4401", code == 4401, str(code))

        with client.websocket_connect(ws_url(avatar_sid, token=OTHER_TOKEN)) as sock:
            first = sock.receive_json()
            _check("★ 他人会话 ⇒ error(session_not_found)（不泄露存在性）",
                   first["type"] == "error" and first["code"] == "session_not_found", str(first))
            code = None
            try:
                sock.receive_json()
            except WebSocketDisconnect as exc:
                code = exc.code
            _check("  └ 关闭码 4404", code == 4404, str(code))

        with client.websocket_connect(ws_url(999999)) as sock:
            first = sock.receive_json()
            _check("不存在的会话 ⇒ 同样 4404", first["code"] == "session_not_found", str(first))

        # ------------------------------------------------------------
        # [4] 端到端闭环（视频模式）
        # ------------------------------------------------------------
        print("\n[4] 端到端闭环（视频模式，真实 WebSocket 栈）")
        orch.clear_states()
        avatar_interview.reset_provider(MockAvatarProvider(answer_text=ASR_TEXT))

        with client.websocket_connect(ws_url(avatar_sid)) as sock:
            hello, st0, q0, av0 = _recv(sock, 4)
            _check("连接后推 hello + 快照（state / question / avatar）",
                   _types([hello, st0, q0, av0])
                   == [ws.EVENT_HELLO, ws.EVENT_STATE, ws.EVENT_QUESTION, ws.EVENT_AVATAR],
                   str(_types([hello, st0, q0, av0])))
            _check("  └ hello 带会话模式与协议清单",
                   hello["interview_mode"] == "avatar" and hello["session_status"] == "created"
                   and set(hello["client_messages"]) == set(ws.CLIENT_MESSAGES), str(hello))
            _check("  └ 快照里的编排状态 = waiting_question（尚未开始）",
                   st0["state"] == orch.STATE_WAITING_QUESTION, str(st0))
            _check("  └ ★ 且 tracked=True（HTTP 建会话后按库中状态登记，不是 None）",
                   st0["tracked"] is True and st0["terminal"] is False
                   and st0["allowed_events"] == [orch.EVENT_QUESTION_READY, orch.EVENT_FINISH],
                   str(st0))
            _check("  └ ★ 快照的 avatar 事件是「**未探测**」而非「探测失败」",
                   av0["session_probed"] is False and av0["ok"] is None, str(av0))

            sock.send_json({"type": "ping"})
            _check("ping → pong", sock.receive_json()["type"] == ws.EVENT_PONG)

            # ---- start：Avatar 就绪 + 播报第一题 ----
            sock.send_json({"type": "start"})
            started = _recv(sock, 3)
            _check("start ⇒ avatar(ok) + state + question 三连推",
                   _types(started) == [ws.EVENT_AVATAR, ws.EVENT_STATE, ws.EVENT_QUESTION],
                   str(_types(started)))
            _check("  └ ★ avatar 事件：数字人会话已就绪（provider=mock，拿到 stream_url）",
                   started[0]["ok"] is True and started[0]["status"] == "ok"
                   and started[0]["stream_url"] == "mock://stream"
                   and bool(started[0]["avatar_session_id"]), str(started[0]))
            _check("  └ state 推进到 avatar_speaking（数字人正在播报）",
                   started[1]["state"] == orch.STATE_AVATAR_SPEAKING, str(started[1]))
            _check("  └ question 事件带第一题原文 + 会话（含总题量）",
                   started[2]["question"]["question_no"] == 1
                   and bool(started[2]["question"]["question"])
                   and started[2]["session"]["total_questions"] == 2, str(started[2]))

            # ---- speech_done ----
            sock.send_json({"type": "speech_done"})
            done = sock.receive_json()
            _check("speech_done ⇒ state 推进到 waiting_answer",
                   done["type"] == ws.EVENT_STATE
                   and done["state"] == orch.STATE_WAITING_ANSWER, str(done))

            # ---- 语音作答（ASR） ----
            sock.send_json({"type": "answer", "mode": "asr"})
            ans1 = _recv(sock, 4)
            _check("answer(asr) ⇒ answer + score + question + state（四连推）",
                   _types(ans1) == [ws.EVENT_ANSWER, ws.EVENT_SCORE,
                                    ws.EVENT_QUESTION, ws.EVENT_STATE],
                   str(_types(ans1)))
            _check("  └ ★ answer 事件：已提交，来源 = asr，文本即 ASR 识别结果",
                   ans1[0]["status"] == ws.ANSWER_SUBMITTED and ans1[0]["source"] == "asr"
                   and ans1[0]["text"] == ASR_TEXT, str(ans1[0]))
            _check("  └ ASR 明细如实回传（status=ok / retryable=False）",
                   ans1[0]["asr"]["status"] == "ok" and ans1[0]["asr"]["retryable"] is False,
                   str(ans1[0]["asr"]))
            _check("  └ ★ score 事件：评分 + 反馈（评分完成后立即推送）",
                   ans1[1]["score"] is not None and bool(ans1[1]["feedback"]), str(ans1[1]))
            _check("  └ ★ question 事件：已生成下一题（题号 2）",
                   ans1[2]["question"]["question_no"] == 2
                   and ans1[2]["all_answered"] is False, str(ans1[2]))
            _check("  └ state 回到 avatar_speaking（下一题已交付播报）",
                   ans1[3]["state"] == orch.STATE_AVATAR_SPEAKING, str(ans1[3]))
            _check("  └ ★ ASR 文本**原样落库**（answer 里 answer_text 逐字相等）",
                   ans1[0]["answer"]["answer_text"] == ASR_TEXT,
                   str(ans1[0]["answer"]["answer_text"])[:30])

            # ---- 第二题：播报结束 → 语音作答 → 全部答完 ----
            sock.send_json({"type": "speech_done"})
            sock.receive_json()
            sock.send_json({"type": "answer", "mode": "asr"})
            ans2 = _recv(sock, 4)
            _check("第二题同样闭环：all_answered=True + question=None + 状态 finished",
                   ans2[2]["all_answered"] is True and ans2[2]["question"] is None
                   and ans2[3]["state"] == orch.STATE_FINISHED, str(ans2[3]))

            # ---- finish：报告 ----
            sock.send_json({"type": "finish"})
            rep = _recv(sock, 2)
            _check("finish ⇒ report + state",
                   _types(rep) == [ws.EVENT_REPORT, ws.EVENT_STATE], str(_types(rep)))
            _check("  └ 报告含总分（八维报告已生成）",
                   rep[0]["report"]["total_score"] is not None, str(rep[0].get("report")))
            _check("  └ 状态 finished", rep[1]["state"] == orch.STATE_FINISHED, str(rep[1]))

            # ---- snapshot：断线重连的恢复入口 ----
            sock.send_json({"type": "snapshot"})
            snap = _recv(sock, 3)
            _check("snapshot ⇒ state + question + avatar（重连即可恢复界面）",
                   _types(snap) == [ws.EVENT_STATE, ws.EVENT_QUESTION, ws.EVENT_AVATAR],
                   str(_types(snap)))
            _check("  └ 恢复出的状态是 finished（不是重新开始）",
                   snap[0]["state"] == orch.STATE_FINISHED, str(snap[0]))

        # ------------------------------------------------------------
        # [5] 打字作答路径
        # ------------------------------------------------------------
        print("\n[5] 打字作答路径（mode=text）")
        orch.clear_states()
        avatar_interview.reset_provider(MockAvatarProvider(answer_text=ASR_TEXT))
        typed_sid = create_session("avatar", 1)

        with client.websocket_connect(ws_url(typed_sid)) as sock:
            _recv(sock, 4)
            sock.send_json({"type": "start"})
            _recv(sock, 3)
            sock.send_json({"type": "speech_done"})
            sock.receive_json()

            # 空文本 ⇒ 拒绝，且**不落库**
            sock.send_json({"type": "answer", "mode": "text", "text": "   "})
            rej = sock.receive_json()
            _check("空文本 ⇒ answer(rejected) + 说明（**不提交**）",
                   rej["type"] == ws.EVENT_ANSWER and rej["status"] == ws.ANSWER_REJECTED
                   and rej["error"] is not None, str(rej))
            _check("  └ 状态未被污染（仍 waiting_answer）",
                   orch.get_state(typed_sid) == orch.STATE_WAITING_ANSWER,
                   str(orch.get_state(typed_sid)))

            sock.send_json({"type": "answer", "mode": "text", "text": TYPED_TEXT})
            typed = _recv(sock, 4)
            _check("打字作答 ⇒ source=text + 文本原样落库",
                   typed[0]["status"] == ws.ANSWER_SUBMITTED
                   and typed[0]["source"] == "text" and typed[0]["text"] == TYPED_TEXT
                   and typed[0]["answer"]["answer_text"] == TYPED_TEXT, str(typed[0]))
            _check("  └ 无 ASR 明细（打字路径不产生 asr 字段值）",
                   typed[0]["asr"] is None, str(typed[0].get("asr")))

        # ------------------------------------------------------------
        # [6] 错误分支
        # ------------------------------------------------------------
        print("\n[6] 错误分支")
        orch.clear_states()
        # 只用 1 题：这样「答完 → 终态 → 再 start」可以在同一条连接里走完
        err_sid = create_session("avatar", 1)

        with client.websocket_connect(ws_url(err_sid)) as sock:
            _recv(sock, 4)

            sock.send_json({"type": "bogus-type"})
            bad = sock.receive_json()
            _check("未知消息 ⇒ error(bad_message)，连接**不断**",
                   bad["type"] == ws.EVENT_ERROR and bad["code"] == ws.ERROR_BAD_MESSAGE,
                   str(bad))
            sock.send_json({"type": "ping"})
            _check("  └ 连接仍然可用（bad_message 不拖垮连接）",
                   sock.receive_json()["type"] == ws.EVENT_PONG)

            # 顺序不对：还没 start 就提交作答
            sock.send_json({"type": "answer", "mode": "text", "text": TYPED_TEXT})
            st_err, st_after = _recv(sock, 2)
            _check("★ 顺序不对 ⇒ error(state_error)（状态机先拦下）",
                   st_err["type"] == ws.EVENT_ERROR and st_err["code"] == ws.ERROR_STATE,
                   str(st_err))
            _check("  └ 随后回推 state（前端据此修正按钮可用性）",
                   st_after["type"] == ws.EVENT_STATE
                   and st_after["state"] == orch.STATE_WAITING_QUESTION, str(st_after))

            # start 后不 speech_done 直接作答 → 同样 state_error
            sock.send_json({"type": "start"})
            _recv(sock, 3)
            sock.send_json({"type": "answer", "mode": "text", "text": TYPED_TEXT})
            st_err2, st_after2 = _recv(sock, 2)
            _check("★ 播报中就作答 ⇒ state_error，且状态**未被污染**",
                   st_err2["code"] == ws.ERROR_STATE
                   and orch.get_state(err_sid) == orch.STATE_AVATAR_SPEAKING,
                   f"{st_err2} state={orch.get_state(err_sid)}")
            _check("  └ 回推的 state 仍是 avatar_speaking（瞬时态没被写坏）",
                   st_after2["state"] == orch.STATE_AVATAR_SPEAKING, str(st_after2))

            # ---- 正常答完唯一一题 → 终态 ----
            sock.send_json({"type": "speech_done"})
            _check("speech_done ⇒ waiting_answer",
                   sock.receive_json()["state"] == orch.STATE_WAITING_ANSWER)
            sock.send_json({"type": "answer", "mode": "text", "text": TYPED_TEXT})
            done = _recv(sock, 4)
            _check("答完唯一一题 ⇒ answer/score/question/state 且进入 finished",
                   _types(done) == [ws.EVENT_ANSWER, ws.EVENT_SCORE,
                                    ws.EVENT_QUESTION, ws.EVENT_STATE]
                   and done[3]["state"] == orch.STATE_FINISHED, str(_types(done)))

            sock.send_json({"type": "finish"})
            fin = _recv(sock, 2)
            _check("finish ⇒ report + state（终态下幂等）",
                   _types(fin) == [ws.EVENT_REPORT, ws.EVENT_STATE]
                   and fin[0]["report"]["total_score"] is not None, str(_types(fin)))

            # 会话已结束后再 start → state_error（终态不可离开）
            sock.send_json({"type": "start"})
            late, late_state = _recv(sock, 2)
            _check("★ 终态后再 start ⇒ state_error（finished 不被降级）",
                   late["type"] == ws.EVENT_ERROR and late["code"] == ws.ERROR_STATE
                   and late_state["state"] == orch.STATE_FINISHED, str(late))
            _check("  └ ★ 且**没有**白打一次 provider（顺序守卫在 provider 之前）",
                   "avatar" not in _types([late, late_state]),
                   str(_types([late, late_state])))

        # ---- 数字人故障时**不阻断**面试（降级为文字交付） ----
        orch.clear_states()
        degraded_sid = create_session("avatar", 1)
        avatar_interview.reset_provider(MockAvatarProvider(fail_on=["initialize"]))
        with client.websocket_connect(ws_url(degraded_sid)) as sock:
            _recv(sock, 4)
            sock.send_json({"type": "start"})
            evts = _recv(sock, 4)
            _check("★ 数字人初始化失败 ⇒ avatar(ok=False) + error 提示，但**仍然出题**",
                   evts[0]["ok"] is False
                   and evts[1]["type"] == ws.EVENT_ERROR
                   and evts[3]["question"] is not None, str(_types(evts)))
            _check("  └ 降级不阻断：状态仍推进到 avatar_speaking",
                   evts[2]["state"] == orch.STATE_AVATAR_SPEAKING, str(evts[2]))
        avatar_interview.reset_provider(MockAvatarProvider(answer_text=ASR_TEXT))

        # ------------------------------------------------------------
        # [7] 文字面试完全不受影响
        # ------------------------------------------------------------
        print("\n[7] 文字面试不受影响")
        orch.clear_states()

        def _text_flow():
            r = client.post(f"/api/interview/{text_sid}/start")
            _check("HTTP /start（文字）仍正常返回第一题",
                   r.status_code == 200 and r.json()["question"]["question_no"] == 1,
                   f"{r.status_code} {r.text[:120]}")
            r = client.post(f"/api/interview/{text_sid}/answer",
                            json={"answer_text": TYPED_TEXT})
            _check("HTTP /answer（文字）仍正常评分并返回 all_answered",
                   r.status_code == 200 and r.json()["all_answered"] is True,
                   f"{r.status_code} {r.text[:120]}")
            r = client.post(f"/api/interview/{text_sid}/end")
            _check("HTTP /end（文字）仍正常出报告",
                   r.status_code == 200 and r.json()["report"]["total_score"] is not None,
                   f"{r.status_code} {r.text[:120]}")
            r = client.get(f"/api/interview/{text_sid}")
            _check("  └ 文字会话的 interview_mode 仍是 text（未被 WS 污染）",
                   r.json()["session"]["interview_mode"] == "text")

        as_user(_text_flow)

    main.app.dependency_overrides.clear()

    # 释放测试引擎的连接：Windows 上 SQLite 文件被句柄占着时删不掉临时目录。
    # ★ 实测（aiosqlite + NullPool）：即使 dispose + gc.collect()，Windows 上
    #   句柄仍可能要到**进程退出**才释放 ⇒ 删不掉就**如实打印**，
    #   不用 ``ignore_errors`` 假装清理干净了（临时目录在系统 temp 下，无害）。
    try:
        asyncio.run(_app_engine.dispose())
    except Exception:  # noqa: BLE001 - 释放失败只影响清理，不影响断言
        pass
    gc.collect()

    # ------------------------------------------------------------
    # [8] 边界守卫（AST）
    # ------------------------------------------------------------
    print("\n[8] 边界守卫（AST：分层单向 / 不碰 provider）")
    ws_src = (_BACKEND / "modules" / "interview" / "ws.py").read_text(encoding="utf-8")
    ws_imports = _all_imports(ws_src)
    _check("实时通道确实 import 编排层（它就是为这条时间线服务的）",
           "services.interview_orchestrator" in ws_imports, str(sorted(ws_imports)))
    _check("  └ 经 service 层取数据（归属校验 / 题目 / 报告都走 interview_service）",
           "services.interview_service" in ws_imports)
    _check("★ 不 import 任何 provider / 通道层（provider 细节不下沉到 api 层）",
           not any(m.startswith(("services.avatar_provider", "services.avatar_interview"))
                   for m in ws_imports),
           str(sorted(m for m in ws_imports if "avatar" in m)))
    _check("  └ 不 import Core / Agent / Validator（api 层只认 service）",
           not any(m.startswith(("services.interview_core", "services.interview_agent",
                                 "services.question_validator")) for m in ws_imports),
           str(sorted(m for m in ws_imports if m.startswith("services."))))
    _check("  └ 不 import 任何向量 / Embedding / 知识库模块",
           not any(tok in m.lower() for m in ws_imports
                   for tok in ("embedding", "vector", "chroma", "knowledge")),
           str(sorted(ws_imports)))

    # ★ 守住编排层的「可选」性质：全项目**只有** modules/interview/ws.py 引入它
    consumers = []
    for path in sorted((_BACKEND / "services").glob("*.py")) + sorted((_BACKEND / "api").glob("*.py")) + sorted((_BACKEND / "modules").rglob("*.py")):
        if "services.interview_orchestrator" in _all_imports(path.read_text(encoding="utf-8")):
            consumers.append(path.relative_to(_BACKEND).as_posix())
    if "services.interview_orchestrator" in _all_imports(
        (_BACKEND / "main.py").read_text(encoding="utf-8")
    ):
        consumers.append("main.py")
    _check("★★ 编排层消费者**收口为唯一闭集**：只有 modules/interview/ws.py",
           consumers == ["modules/interview/ws.py"], str(consumers))

    api_src = (_BACKEND / "modules" / "interview" / "api.py").read_text(encoding="utf-8")
    _check("★ modules/interview/api.py **未改**（文字面试的 HTTP 契约一行未动）",
           "interview_orchestrator" not in api_src and "interview_ws" not in api_src
           and "WebSocket" not in api_src)

    # 清理临时库（Windows 上句柄未释放时删不掉，**如实打印**而不是静默）
    try:
        shutil.rmtree(_TMP_DIR, ignore_errors=False)
        _check("临时 SQLite 目录已清理", not pathlib.Path(_TMP_DIR).exists())
    except Exception as exc:  # noqa: BLE001
        print(f"  [WARN] 临时目录未能删除（不影响结果）：{_TMP_DIR} -> {exc}")

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
