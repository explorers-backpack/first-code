# -*- coding: utf-8 -*-
"""AI 模拟面试 · 交互模式分流（text / avatar 通道）自检

无需 pytest，直接运行：
    python backend/tests/test_avatar_channel.py

覆盖范围
--------
- **通道模块契约**：``services/avatar_interview.py`` 是**空实现**（原样透传、
  协程入口、签名预留 ``db`` / ``session``），且元信息标明未接外部能力
- **text 流程不变**：``start_session`` 在 text 模式下**完全不碰** avatar 入口，
  返回结构与改动前逐字段一致（恰好 4 个键）
- **avatar 可以创建 session**：service 层 + HTTP 全链路
- **avatar 走 AvatarInterview 入口**：用 spy 证明 ``enter`` 被调用恰好一次，
  且收到的就是 Service 组装好的 payload、返回值被原样交回调用方
- **两种模式互不影响**：同岗位下题目完全一致；交错作答时题号各自独立推进；
  一边结束不影响另一边状态
- **边界**：未接讯飞数字人 / ASR / TTS；InterviewCore 不反向依赖通道层

数据库使用 SQLite 内存库（``StaticPool``），不触碰本机 MySQL。
"""

import asyncio
import ast
import inspect
import os
import pathlib
import sys
import textwrap

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import main  # noqa: E402
import models  # noqa: E402,F401
from database import Base, get_db  # noqa: E402
from deps import get_current_user  # noqa: E402
from models import (  # noqa: E402
    INTERVIEW_MODE_AVATAR,
    INTERVIEW_MODE_TEXT,
    INTERVIEW_MODES,
    Job,
    User,
)
from schemas.interview import AnswerSubmitRequest, SessionCreateRequest  # noqa: E402
from services import avatar_interview, interview_core, interview_service  # noqa: E402

API = "/api/interview"
ANSWER_TEXT = (
    "我负责过订单中台重构，把单体服务拆成 8 个微服务，引入 Redis 做缓存、"
    "Kafka 做异步解耦，QPS 从 800 提升到 5000，响应时间从 1200ms 降到 80ms。"
)

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


class _AsyncSpy:
    """把协程函数替换成可计数、可透传的替身（用于证明「谁被调用了」）。"""

    def __init__(self, target):
        self.original = target
        self.calls = []

    async def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return await self.original(*args, **kwargs)


def _imported_modules(source: str):
    """AST 收集模块的 import 目标名（**不要用子串匹配**——docstring 会误伤）。"""
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module)
                names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed(db: AsyncSession):
    user = User(username="chan_user", email="chan@example.com", password_hash="x", role="user")
    job = Job(
        job_name="后端开发工程师",
        salary="20-35K",
        edu_require="本科",
        major_require="不限",
        skills="Python,MySQL,Redis,Kafka,Docker",
        duty="负责后端服务的设计与开发，参与需求分析、系统设计与性能优化",
        city="深圳",
        industry="互联网",
    )
    db.add_all([user, job])
    await db.commit()
    await db.refresh(user)
    await db.refresh(job)
    return user.id, job.id


async def _make(db, user_id, job_id, mode, total=3):
    res = await interview_service.create_session(
        db, user_id, SessionCreateRequest(job_id=job_id, mode=mode, total_questions=total)
    )
    return res["session"]["id"]


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 68)
    print("AI 模拟面试 · 交互模式分流（text / avatar 通道）自检")
    print("=" * 68)

    # ------------------------------------------------------------
    # [1] 通道模块契约（空实现）
    # ------------------------------------------------------------
    print("\n[1] 数字人通道入口（services/avatar_interview.py）")
    _check("CHANNEL == 'avatar'", avatar_interview.CHANNEL == "avatar",
           avatar_interview.CHANNEL)
    _check("STATUS == pending（通道尚未接入）",
           avatar_interview.STATUS == avatar_interview.STATUS_PENDING == "pending",
           avatar_interview.STATUS)
    _check("未接外部能力：iflytek / asr / tts 全为 False",
           avatar_interview.INTEGRATIONS == {"iflytek": False, "asr": False, "tts": False},
           str(avatar_interview.INTEGRATIONS))
    _check("enter 是协程函数", inspect.iscoroutinefunction(avatar_interview.enter))
    _check("enter 签名预留 db / session / payload",
           list(inspect.signature(avatar_interview.enter).parameters) == ["db", "session", "payload"],
           str(list(inspect.signature(avatar_interview.enter).parameters)))
    _check("__all__ 覆盖公开常量与入口",
           {"CHANNEL", "STATUS", "STATUS_PENDING", "STATUS_READY", "INTEGRATIONS", "enter"}
           <= set(avatar_interview.__all__))

    payload = {"session": {"id": 1}, "question": None, "job_name": None, "message": "面试已开始"}
    out = await avatar_interview.enter(None, None, payload)
    _check("空实现：原样透传（返回同一对象）", out is payload)

    # ------------------------------------------------------------
    # [2] text 流程不变
    # ------------------------------------------------------------
    print("\n[2] text 流程不变（默认路径完全不碰通道入口）")
    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        user_id, job_id = await _seed(db)
        _check("模式常量：INTERVIEW_MODES == (text, avatar)",
               INTERVIEW_MODES == (INTERVIEW_MODE_TEXT, INTERVIEW_MODE_AVATAR),
               str(INTERVIEW_MODES))

        text_id = await _make(db, user_id, job_id, INTERVIEW_MODE_TEXT, total=3)

        # 默认不传 mode 也是 text
        default_id = (await interview_service.create_session(
            db, user_id, SessionCreateRequest(job_id=job_id, total_questions=3)
        ))["session"]["id"]
        _check("不传 mode 时默认 text（与既有行为一致）",
               (await interview_service.get_session_detail(db, user_id, default_id))
               ["session"]["interview_mode"] == "text")

        spy = _AsyncSpy(avatar_interview.enter)
        avatar_interview.enter = spy
        try:
            started = await interview_service.start_session(db, user_id, text_id)
        finally:
            avatar_interview.enter = spy.original

        _check("text 模式未调用数字人通道入口", spy.calls == [], str(spy.calls))
        _check("start 返回结构恰好 4 个键（未新增字段）",
               set(started) == {"session", "question", "job_name", "message"}, str(sorted(started)))
        _check("message 仍为「面试已开始」", started["message"] == "面试已开始",
               started["message"])
        _check("question 仍为第 1 题",
               started["question"] is not None and started["question"]["question_no"] == 1)
        _check("session 状态推进为 ongoing / current_question_no=1",
               started["session"]["status"] == "ongoing"
               and started["session"]["current_question_no"] == 1)
        _check("回传 interview_mode=text", started["session"]["interview_mode"] == "text")

        # _deliver 对 text 必须是恒等函数
        probe = {"k": "v"}
        _check("_deliver(text) 原样返回同一对象（未拷贝、未改写）",
               await interview_service._deliver(db, await interview_service._load_session(
                   db, text_id, user_id), probe) is probe)

        # ------------------------------------------------------------
        # [3] avatar 可以创建 session
        # ------------------------------------------------------------
        print("\n[3] avatar 可以创建 session")
        avatar_id = await _make(db, user_id, job_id, INTERVIEW_MODE_AVATAR, total=3)
        detail = await interview_service.get_session_detail(db, user_id, avatar_id)
        _check("创建成功且落库为 avatar",
               detail["session"]["interview_mode"] == "avatar",
               str(detail["session"].get("interview_mode")))
        _check("  └ 初始状态 created、题号 0",
               detail["session"]["status"] == "created"
               and detail["session"]["current_question_no"] == 0)
        _check("  └ 可正常查询（题目/作答为空）",
               detail["questions"] == [] and detail["answers"] == [])

        # ------------------------------------------------------------
        # [4] avatar 走 AvatarInterview 入口
        # ------------------------------------------------------------
        print("\n[4] avatar 走 AvatarInterview 入口")
        spy = _AsyncSpy(avatar_interview.enter)
        avatar_interview.enter = spy
        try:
            avatar_started = await interview_service.start_session(db, user_id, avatar_id)
        finally:
            avatar_interview.enter = spy.original

        _check("数字人通道入口被调用恰好 1 次", len(spy.calls) == 1, str(len(spy.calls)))
        args = spy.calls[0][0] if spy.calls else ()
        _check("  └ 入参为 (db, session, payload)",
               len(args) == 3 and isinstance(args[1], models.InterviewSession)
               and isinstance(args[2], dict), str(type(args[1])))
        _check("  └ 传入的是本次 avatar 会话行", args and args[1].id == avatar_id,
               str(getattr(args[1], "id", None)) if args else "")
        _check("  └ payload 即 Service 组装好的 4 键交付内容",
               args and set(args[2]) == {"session", "question", "job_name", "message"},
               str(sorted(args[2])) if args else "")
        _check("  └ payload 里已带 interview_mode=avatar",
               args and args[2]["session"]["interview_mode"] == "avatar")
        _check("入口返回值被原样交回调用方（空实现不改变交付内容）",
               avatar_started is args[2] if args else False)
        _check("avatar 会话状态机正常推进（分流不阻塞流程）",
               avatar_started["session"]["status"] == "ongoing"
               and avatar_started["question"]["question_no"] == 1)

        # ------------------------------------------------------------
        # [5] 两种模式互不影响
        # ------------------------------------------------------------
        print("\n[5] 两种模式互不影响")
        t2 = await _make(db, user_id, job_id, INTERVIEW_MODE_TEXT, total=4)
        a2 = await _make(db, user_id, job_id, INTERVIEW_MODE_AVATAR, total=4)
        t2_start = await interview_service.start_session(db, user_id, t2)
        a2_start = await interview_service.start_session(db, user_id, a2)

        _check("同岗位同配置下两模式第一题完全相同",
               t2_start["question"]["question"] == a2_start["question"]["question"],
               f"{t2_start['question']['question'][:26]} vs {a2_start['question']['question'][:26]}")
        t2_detail = await interview_service.get_session_detail(db, user_id, t2)
        a2_detail = await interview_service.get_session_detail(db, user_id, a2)
        _check("整场题目计划逐题一致（模式不参与出题）",
               [q["question"] for q in t2_detail["questions"]]
               == [q["question"] for q in a2_detail["questions"]])

        # 交错作答：两边题号必须各自独立推进
        await interview_service.submit_answer(
            db, user_id, t2, AnswerSubmitRequest(answer_text=ANSWER_TEXT))
        await interview_service.submit_answer(
            db, user_id, a2, AnswerSubmitRequest(answer_text=ANSWER_TEXT))
        await interview_service.submit_answer(
            db, user_id, t2, AnswerSubmitRequest(answer_text=ANSWER_TEXT))

        t2_detail = await interview_service.get_session_detail(db, user_id, t2)
        a2_detail = await interview_service.get_session_detail(db, user_id, a2)
        _check("交错作答后题号各自独立（text 到第 3 题、avatar 到第 2 题）",
               t2_detail["session"]["current_question_no"] == 3
               and a2_detail["session"]["current_question_no"] == 2,
               f"text={t2_detail['session']['current_question_no']} "
               f"avatar={a2_detail['session']['current_question_no']}")
        _check("  └ 各自已作答数量正确",
               t2_detail["answered_count"] == 2 and a2_detail["answered_count"] == 1,
               f"{t2_detail['answered_count']} / {a2_detail['answered_count']}")
        _check("  └ 两边模式字段未被互相污染",
               t2_detail["session"]["interview_mode"] == "text"
               and a2_detail["session"]["interview_mode"] == "avatar")

        # 一边结束不影响另一边
        await interview_service.end_session(db, user_id, a2)
        t2_after = await interview_service.get_session_detail(db, user_id, t2)
        a2_after = await interview_service.get_session_detail(db, user_id, a2)
        _check("avatar 结束后 text 会话状态不受影响（仍 ongoing）",
               t2_after["session"]["status"] == "ongoing",
               t2_after["session"]["status"])
        _check("  └ text 题号未被改动", t2_after["session"]["current_question_no"] == 3)
        _check("  └ avatar 已 finished 且有自己的报告",
               a2_after["session"]["status"] == "finished"
               and (await interview_service.get_report(db, user_id, a2))["total_score"] is not None)
        _check("  └ 两条会话 id 不同、互不覆盖", t2 != a2)

        # text 走完整流程仍正常（端到端不回退）
        await interview_service.submit_answer(
            db, user_id, t2, AnswerSubmitRequest(answer_text=ANSWER_TEXT))
        await interview_service.submit_answer(
            db, user_id, t2, AnswerSubmitRequest(answer_text=ANSWER_TEXT))
        await interview_service.end_session(db, user_id, t2)
        t2_final = await interview_service.get_session_detail(db, user_id, t2)
        _check("text 会话可完整走完（4 题 + 报告）",
               t2_final["session"]["status"] == "finished"
               and t2_final["answered_count"] == 4
               and (await interview_service.get_report(db, user_id, t2))["total_score"] is not None)

        http_user_id, http_job_id = user_id, job_id

    # ------------------------------------------------------------
    # [6] HTTP 全链路（avatar 创建 + 开始）
    # ------------------------------------------------------------
    print("\n[6] HTTP 全链路（avatar）")

    async def override_get_db():
        async with session_factory() as session:
            yield session

    main.app.dependency_overrides[get_db] = override_get_db

    async def override_get_current_user():
        return {"user_id": http_user_id, "email": "chan@example.com", "role": "user"}

    main.app.dependency_overrides[get_current_user] = override_get_current_user

    transport = ASGITransport(app=main.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post(f"{API}/create", json={"job_id": http_job_id, "mode": "avatar"})
        _check("POST /create mode=avatar → 200", r.status_code == 200,
               f"{r.status_code} {r.text[:120]}")
        http_avatar_id = r.json()["session"]["id"]
        _check("  └ 回传 interview_mode=avatar",
               r.json()["session"]["interview_mode"] == "avatar")

        r = await client.post(f"{API}/{http_avatar_id}/start")
        _check("POST /{id}/start（avatar）→ 200 且返回第一题",
               r.status_code == 200 and r.json()["question"]["question_no"] == 1,
               f"{r.status_code} {r.text[:160]}")
        _check("  └ 出参仍为 4 键（空实现未污染 API 契约）",
               set(r.json()) == {"session", "question", "job_name", "message"},
               str(sorted(r.json())))

        r = await client.post(
            f"{API}/create", json={"job_id": http_job_id, "mode": "text", "total_questions": 2})
        http_text_id = r.json()["session"]["id"]
        r = await client.post(f"{API}/{http_text_id}/start")
        _check("POST /{id}/start（text）行为与 avatar 一致（都正常出题）",
               r.status_code == 200 and r.json()["session"]["interview_mode"] == "text",
               f"{r.status_code} {r.text[:160]}")

    main.app.dependency_overrides.clear()

    # ------------------------------------------------------------
    # [7] 边界：未接外部能力 / 分层不被破坏
    # ------------------------------------------------------------
    print("\n[7] 边界（未接讯飞 / ASR / TTS，分层不被破坏）")
    avatar_src = pathlib.Path(avatar_interview.__file__).read_text(encoding="utf-8")
    avatar_imports = _imported_modules(avatar_src)
    _check("通道模块只 import 标准库 + sqlalchemy（无 websocket / requests）",
           not any(
               name in {"websocket", "websockets", "requests", "aiohttp"} or
               name.startswith(("websocket", "requests"))
               for name in avatar_imports
           ),
           str(sorted(avatar_imports)))
    _check("  └ 不 import main（避免拉入 Spark / 循环依赖）",
           "main" not in avatar_imports, str(sorted(avatar_imports)))
    _check("  └ 不 import interview_core / interview_service（通道层不反向依赖业务层）",
           not {"services.interview_core", "services.interview_service"} & avatar_imports,
           str(sorted(avatar_imports)))

    service_src = pathlib.Path(interview_service.__file__).read_text(encoding="utf-8")
    service_imports = _imported_modules(service_src)
    _check("Service 只经通道层调用数字人（import 了 services.avatar_interview）",
           "services.avatar_interview" in service_imports, str(sorted(service_imports)))
    _check("  └ Service 仍不直接 import Agent / Validator（既有约束未被破坏）",
           not {"services.interview_agent", "services.question_validator"} & service_imports,
           str(sorted(service_imports)))

    core_src = pathlib.Path(interview_core.__file__).read_text(encoding="utf-8")
    _check("InterviewCore 未反向依赖通道层（源码不含 avatar_interview）",
           "avatar_interview" not in core_src)

    enter_src = inspect.getsource(avatar_interview.enter)
    enter_def = ast.parse(textwrap.dedent(enter_src)).body[0]
    statements = [
        node for node in enter_def.body
        if not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant))
    ]
    _check("enter 函数体只有一条 return（确实是空实现，无任何外部调用）",
           len(statements) == 1
           and isinstance(statements[0], ast.Return)
           and isinstance(statements[0].value, ast.Name)
           and statements[0].value.id == "payload",
           f"{[type(n).__name__ for n in statements]}")

    await engine.dispose()

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
