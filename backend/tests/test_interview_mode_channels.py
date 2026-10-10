# -*- coding: utf-8 -*-
"""AI 模拟面试 · 交互模式「通道覆盖」自检

验证 ``interview_mode`` 的交付边界分流（``interview_service._deliver``）
覆盖**全部交互入口**：

    start_session / get_current_question / submit_answer / end_session
        ├── text   → 原样返回（既有流程，逐字段不变）
        └── avatar → services.avatar_interview.enter（数字人通道入口 → provider 层）

与 ``test_avatar_channel.py`` 的分工
------------------------------------
``test_avatar_channel.py`` 锁「通道模块本身的契约」与「start 一处分流」；
本套件锁「**分流覆盖面**」——即除 ``start`` 外的另外三个交互入口
（current-question / answer / end）也进通道，且 ``text`` 一处都不进。

无需 pytest，直接运行：
    python backend/tests/test_interview_mode_channels.py

数据库使用 SQLite 内存库（``StaticPool``），不触碰本机 MySQL。
"""

import asyncio
import ast
import os
import pathlib
import sys

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))
sys.path.insert(0, str(_BACKEND / "tests"))

import regression_env  # noqa: E402,F401  （钉住 EMBEDDING_/RAG_/VECTOR_ 旋钮）

from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401
from database import Base  # noqa: E402
from models import (  # noqa: E402
    INTERVIEW_MODE_AVATAR,
    INTERVIEW_MODE_TEXT,
    Job,
    User,
)
from schemas.interview import AnswerSubmitRequest, SessionCreateRequest  # noqa: E402
from services import avatar_interview, interview_service  # noqa: E402

ANSWER_TEXT = (
    "我负责过订单中台重构，把单体服务拆成 8 个微服务，引入 Redis 做缓存、"
    "Kafka 做异步解耦，QPS 从 800 提升到 5000，响应时间从 1200ms 降到 80ms。"
)

#: 四种交互入口交付内容的**键集**（用来证明「路由过去的确实是该入口的内容」）
START_KEYS = {"session", "question", "job_name", "message"}
CURRENT_KEYS = {
    "session_id", "status", "question_no", "total_questions",
    "question", "answered", "all_answered", "message",
}
ANSWER_KEYS = {"answer", "session", "next_question", "all_answered", "message"}
END_KEYS = {"session", "report", "message"}

#: 逐字段等价比较时要忽略的「易变 / 模式相关」键。
#: - 易变：id / 时间戳 / 外键（两场会话天然不同）
#: - 模式相关：``interview_mode``（这正是两种模式**应当**不同的唯一字段）
VOLATILE_KEYS = {
    "id", "created_at", "started_at", "ended_at",
    "session_id", "user_id", "question_id", "interview_mode",
}

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
    """可计数、可透传的协程替身（证明「谁被调用了、收到什么」）。"""

    def __init__(self, target):
        self.original = target
        self.calls = []

    async def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return await self.original(*args, **kwargs)


def _normalize(obj):
    """递归剔除易变 / 模式相关键，供「逐字段等价」比较。"""
    if isinstance(obj, dict):
        return {k: _normalize(v) for k, v in obj.items() if k not in VOLATILE_KEYS}
    if isinstance(obj, list):
        return [_normalize(v) for v in obj]
    return obj


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
    user = User(
        username="chan2_user", email="chan2@example.com", password_hash="x", role="user"
    )
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


async def _run_flow(db, user_id, session_id, total):
    """跑完整交互流程，返回每一步的交付字典（顺序即交付顺序）。"""
    steps = []
    steps.append(await interview_service.start_session(db, user_id, session_id))
    steps.append(await interview_service.get_current_question(db, user_id, session_id))
    for _ in range(total):
        steps.append(
            await interview_service.submit_answer(
                db, user_id, session_id, AnswerSubmitRequest(answer_text=ANSWER_TEXT)
            )
        )
    steps.append(await interview_service.end_session(db, user_id, session_id))
    return steps


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 68)
    print("AI 模拟面试 · 交互模式「通道覆盖」自检")
    print("=" * 68)

    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    TOTAL = 2

    async with session_factory() as db:
        user_id, job_id = await _seed(db)

        # ------------------------------------------------------------
        # [1] avatar：全部交互入口都进数字人通道
        # ------------------------------------------------------------
        print("\n[1] avatar 会话：四个交互入口都经过数字人通道")
        avatar_id = await _make(db, user_id, job_id, INTERVIEW_MODE_AVATAR, total=TOTAL)

        spy = _AsyncSpy(avatar_interview.enter)
        avatar_interview.enter = spy
        try:
            avatar_steps = await _run_flow(db, user_id, avatar_id, TOTAL)
        finally:
            avatar_interview.enter = spy.original

        expected_calls = 3 + TOTAL  # start + current + N×answer + end
        _check(
            f"通道入口被调用恰好 {expected_calls} 次（1 start + 1 current + {TOTAL} answer + 1 end）",
            len(spy.calls) == expected_calls,
            str(len(spy.calls)),
        )
        payloads = [c[0][2] for c in spy.calls]  # 每次调用的 payload
        _check("  └ 每次传入的都是 (db, session, payload) 三元组",
               all(len(c[0]) == 3 and isinstance(c[0][2], dict) for c in spy.calls))
        _check("  └ 第 1 次（start）payload 键集正确",
               set(payloads[0]) == START_KEYS, str(sorted(payloads[0])))
        _check("  └ 第 2 次（current-question）payload 键集正确",
               set(payloads[1]) == CURRENT_KEYS, str(sorted(payloads[1])))
        _check(f"  └ 第 3~{2 + TOTAL} 次（answer）payload 键集正确",
               all(set(payloads[i]) == ANSWER_KEYS for i in range(2, 2 + TOTAL)),
               str([sorted(payloads[i]) for i in range(2, 2 + TOTAL)]))
        _check("  └ 末次（end）payload 键集正确",
               set(payloads[-1]) == END_KEYS, str(sorted(payloads[-1])))
        _check("  └ 传入的 session 行就是本场 avatar 会话",
               all(c[0][1].id == avatar_id for c in spy.calls))

        # 空实现 ⇒ 交付内容与 text 逐字段相同（见 [3]），且流程状态机照常推进
        _check("流程未被分流阻塞（末步为结束、报告已生成）",
               avatar_steps[-1]["report"]["total_score"] is not None)

        # ------------------------------------------------------------
        # [2] text：一处都不进通道
        # ------------------------------------------------------------
        print("\n[2] text 会话：四个交互入口都不进通道")
        text_id = await _make(db, user_id, job_id, INTERVIEW_MODE_TEXT, total=TOTAL)

        spy = _AsyncSpy(avatar_interview.enter)
        avatar_interview.enter = spy
        try:
            text_steps = await _run_flow(db, user_id, text_id, TOTAL)
        finally:
            avatar_interview.enter = spy.original

        _check("通道入口一次都没被调用", spy.calls == [], str(spy.calls))
        _check("  └ text 流程正常跑完（报告已生成）",
               text_steps[-1]["report"]["total_score"] is not None)

        # ------------------------------------------------------------
        # [3] 交付内容逐字段等价（空实现 ⇒ text 与 avatar 完全一致）
        # ------------------------------------------------------------
        print("\n[3] 交付内容逐字段等价（text vs avatar，忽略 id / 时间戳 / 模式字段）")
        _check("step 数一致", len(text_steps) == len(avatar_steps))
        for idx, (t, a) in enumerate(zip(text_steps, avatar_steps)):
            _check(
                f"  └ step {idx} 交付内容逐字段相同",
                _normalize(t) == _normalize(a),
                f"text={_normalize(t)}\navatar={_normalize(a)}",
            )
        _check(
            "两种模式唯一差异字段是 interview_mode",
            text_steps[0]["session"]["interview_mode"] == "text"
            and avatar_steps[0]["session"]["interview_mode"] == "avatar",
        )

        # ------------------------------------------------------------
        # [4] 只读投影不分流
        # ------------------------------------------------------------
        print("\n[4] 只读投影（get_session_detail / get_report）不分流")
        spy = _AsyncSpy(avatar_interview.enter)
        avatar_interview.enter = spy
        try:
            detail = await interview_service.get_session_detail(db, user_id, avatar_id)
            report = await interview_service.get_report(db, user_id, avatar_id)
        finally:
            avatar_interview.enter = spy.original
        _check("get_session_detail 未触发通道入口", spy.calls == [], str(spy.calls))
        _check("  └ 仍能正常返回会话详情", detail["session"]["id"] == avatar_id)
        _check("  └ 仍能正常返回报告", report["total_score"] is not None)

        # ------------------------------------------------------------
        # [5] 边界：分层不被破坏（core 不反向依赖通道层）
        # ------------------------------------------------------------
        print("\n[5] 边界（core 不被触碰）")
        core_src = (_BACKEND / "services" / "interview_core.py").read_text(encoding="utf-8")
        _check("interview_core 源码不含 avatar_interview（未反向依赖通道层）",
               "avatar_interview" not in core_src)

        # 判断 import 关系**必须用 AST**：源码 docstring 会提到模块名，
        # 子串匹配必然误判（本文件首版就踩了这个坑）。
        service_imports = _imported_modules(
            (_BACKEND / "services" / "interview_service.py").read_text(encoding="utf-8")
        )
        _check("Service 只经通道层调用数字人（import 了 services.avatar_interview）",
               "services.avatar_interview" in service_imports, str(sorted(service_imports)))
        _check("Service 仍不直接 import Agent / Validator（既有约束未被破坏）",
               not {"services.interview_agent", "services.question_validator"} & service_imports,
               str(sorted(service_imports)))

    await engine.dispose()

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
