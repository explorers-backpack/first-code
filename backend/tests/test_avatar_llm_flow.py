# -*- coding: utf-8 -*-
"""AI 模拟面试 · 数字人「LLM 单题出题」链路自检（方案 A）

无需 pytest，直接运行：
    python backend/tests/test_avatar_llm_flow.py

不依赖本机 MySQL：DATABASE_URL 指向 SQLite 内存库 + StaticPool。
**不调用真实 LLM、不联网**：Agent 一律打桩；``regression_env`` 把 Embedding /
向量后端钉到离线默认态（本机 ``.env`` 里带着**真实** ``SPARK_*`` 凭据，
不钉扎就会真的打星火）。

被测对象
--------
- ``interview_service.avatar_llm_questions_enabled``   开关语义（默认关闭）
- ``interview_service.generate_and_persist_next``      生成下一题并落库
- ``interview_service.start_session``                  数字人开场只落第 1 题
- ``interview_service.submit_answer``                  评分后补题再交付

覆盖范围
--------
[1] 开关语义：未配置 ⇒ **开启（默认）** / 真值 / 假值 / 非法值（非法值记 ERROR 且按默认处理）
[2] **显式关闭（=0）⇒ 零变化**：数字人开场仍整场落库、下一题仍从库里取
[3] 打开后（默认态）：数字人开场**只落第 1 题**；text 会话**不受影响**（仍整场落库）
[4] 打开后：答题 → LLM 生成下一题并落库（题号连续、可一路走完并出报告）
[5] 打开后：LLM 失败 ⇒ **规则兜底**（第 k 项，与整场落库同源）
[6] 打开后：两条路都失败 ⇒ ``question=None`` + ``question_generate_failed``，**不抛异常**
[7] 幂等：重复调用不插出重复题
[8] 契约与守卫：恒定键集 / 协程 / 404 / 400 / 不 import 出题策略 / 默认路径未动
"""

import ast
import asyncio
import contextlib
import inspect
import json
import os
import pathlib
import sys

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))
sys.path.insert(0, str(BACKEND_DIR / "tests"))

import regression_env  # noqa: E402,F401  钉扎部署配置（必须早于项目模块）

from fastapi import HTTPException  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
import prompts  # noqa: E402
from database import Base  # noqa: E402
from models import (  # noqa: E402
    INTERVIEW_MODE_AVATAR,
    INTERVIEW_MODE_TEXT,
    SESSION_STATUS_ONGOING,
    InterviewQuestion,
    Job,
    User,
)
from schemas.interview import AnswerSubmitRequest, SessionCreateRequest  # noqa: E402
from services import (  # noqa: E402
    interview_context_pack,
    interview_core,
    interview_next_agent as na,
    interview_service,
)
from services.resume_scoring import extract_skills  # noqa: E402

_PASSED = 0
_FAILED = 0

JOB_SKILLS = "Python,MySQL,Redis,Kafka,Docker"
ANSWER = (
    "我在上一家公司负责订单中台重构，把单体拆成 8 个微服务，"
    "用 Python + MySQL + Redis + Kafka，QPS 从 800 提到 5000。"
)
ITEM_KEYS = ("question_no", "question", "question_type", "topic", "difficulty",
             "expected_points")

#: ``question_knowledge.txt`` 相对 ``question.txt`` **新增**的小节（占位符原样）。
KNOWLEDGE_BLOCK_RAW = (
    "## 四·五、参考知识（外部检索结果，可选）\n"
    "\n"
    "参考知识：\n"
    "{{knowledge_context}}\n"
    "\n"
    "出题时请把这些知识融入问题的场景与追问点，但不得整段照抄进 `question`，\n"
    "也不得把知识中的结论直接写进 `question`（`expected_points` 可参考）。\n"
    "\n"
)


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name
          + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def _imported_modules(source: str) -> set:
    """用 AST 提取真正被 import 的模块名（不用子串匹配：docstring 会误伤）。"""
    names: set = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


@contextlib.contextmanager
def _flag(value):
    """临时设置 / 清除 ``INTERVIEW_AVATAR_LLM_QUESTIONS`` 并在退出时还原。"""
    key = interview_service.AVATAR_LLM_QUESTIONS_ENV
    old = os.environ.get(key)
    if value is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = value
    try:
        yield
    finally:
        if old is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = old


class MockSpark:
    """按顺序吐出预设回复的假 Spark；超出预设次数即报错。"""

    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls: list = []

    async def chat_async(self, message: str) -> str:
        self.calls.append(message)
        if not self.replies:
            raise AssertionError("MockSpark 被调用次数超出预期")
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


def _spark_ok(question: str, *, qtype="technical", topic="Redis", difficulty="mid",
              expected_points=("缓存穿透", "过期策略"), reason="stub") -> MockSpark:
    """构造一个「总是返回合法 JSON」的假 Spark。"""
    return MockSpark(json.dumps({
        "question": question,
        "question_type": qtype,
        "topic": topic,
        "difficulty": difficulty,
        "expected_points": list(expected_points),
        "reason": reason,
    }, ensure_ascii=False))


def _spark_fail() -> MockSpark:
    """构造一个「两次都返回非法 JSON」的假 Spark（首次 + 1 次修复都失败）。"""
    return MockSpark("这不是 JSON", "这也不是 JSON")


@contextlib.contextmanager
def _installed_spark(client):
    """临时替换 ``interview_next_agent._default_spark`` 并在退出时还原。

    ★ 数字人补题走的是 ``interview_next_agent``（携带历史回答的 Prompt 组），
    它**自己**解析 Spark 客户端，因此这里替换的是它的默认取值来源——
    真实实现会去取 ``main.spark_api``（本机 ``.env`` 带真实凭据），
    不替换就会真的打星火。
    """
    original = na._default_spark
    na._default_spark = lambda: client
    try:
        yield client
    finally:
        na._default_spark = original


@contextlib.contextmanager
def _patched(obj, name, value):
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed(db: AsyncSession):
    user = User(username="al_t", email="al_t@example.com", password_hash="x", role="user")
    other = User(username="al_o", email="al_o@example.com", password_hash="x", role="user")
    job = Job(
        job_name="后端开发工程师",
        salary="20-35K",
        edu_require="本科",
        major_require="不限",
        skills=JOB_SKILLS,
        duty="负责后端服务的设计与开发",
        city="深圳",
        industry="互联网",
    )
    db.add_all([user, other, job])
    await db.commit()
    await db.refresh(user)
    await db.refresh(other)
    await db.refresh(job)
    return user.id, other.id, job.id


async def _make(db: AsyncSession, user_id: int, job_id: int, *, mode: str,
                total: int = 3) -> int:
    payload = SessionCreateRequest(job_id=job_id, total_questions=total, mode=mode)
    return (await interview_service.create_session(db, user_id, payload))["session"]["id"]


async def _stored(db: AsyncSession, session_id: int):
    rows = (await db.execute(
        select(InterviewQuestion)
        .where(InterviewQuestion.session_id == session_id)
        .order_by(InterviewQuestion.question_no)
    )).scalars().all()
    return [
        {k: (list(r.expected_points or []) if k == "expected_points" else getattr(r, k))
         for k in ITEM_KEYS}
        for r in rows
    ]


async def _rule_plan(db: AsyncSession, session_id: int):
    """独立复算规则出题结果（不经任何策略 / LLM）。"""
    session = await interview_core.load_session_row(db, session_id)
    job = await interview_core.load_job_row(db, session.job_id)
    resume = await interview_core.load_resume_row(db, session.resume_id)
    skills = extract_skills(resume.content) if resume is not None and resume.content else []
    return [dict(x) for x in interview_core.build_question_plan(session, job, skills)]


async def _submit(db: AsyncSession, user_id: int, sid: int):
    return await interview_service.submit_answer(
        db, user_id, sid, AnswerSubmitRequest(answer_text=ANSWER)
    )


async def _raises(coro_fn, exc_type):
    try:
        await coro_fn()
    except exc_type as exc:
        return True, exc
    except BaseException as exc:  # noqa: BLE001
        return False, exc
    return False, None


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 模拟面试 · 数字人 LLM 单题出题链路自检（方案 A）")
    print("=" * 70)

    # ------------------------------------------------------------
    # [1] 开关语义（纯函数，不碰 DB）
    # ------------------------------------------------------------
    print("\n[1] 开关语义：INTERVIEW_AVATAR_LLM_QUESTIONS")

    key = interview_service.AVATAR_LLM_QUESTIONS_ENV
    _check("[1.1] 开关名已声明", key == "INTERVIEW_AVATAR_LLM_QUESTIONS", key)

    # ★ 默认开启 ⇒ 回归必须把它钉住，否则本机真实 SPARK_* 会被打到（见 regression_env）。
    #   这条断言是「钉扎被删掉」的第一道警报。
    _check("[1.4] ★ 回归环境已把它钉成 '0'（防数字人闭环套件真打星火）",
           os.environ.get(key) == "0" and regression_env.PINNED.get(key) == "0",
           f"env={os.environ.get(key)!r} pinned={regression_env.PINNED.get(key)!r}")

    with _flag(None):
        _check("[1.2] 未配置 ⇒ **开启**（默认）",
               interview_service.avatar_llm_questions_enabled() is True)
    with _flag(""):
        _check("  └ 空串 ⇒ 开启（空串等价未配置）",
               interview_service.avatar_llm_questions_enabled() is True)
    for truthy in ("1", "true", "TRUE", "yes", "on", " 1 "):
        with _flag(truthy):
            _check(f"  └ {truthy!r} ⇒ 打开",
                   interview_service.avatar_llm_questions_enabled() is True)
    for falsy in ("0", "false", "NO", "off"):
        with _flag(falsy):
            _check(f"  └ {falsy!r} ⇒ 关闭（显式关闭回到旧行为）",
                   interview_service.avatar_llm_questions_enabled() is False)
    with _flag("maybe"):
        _check("[1.3] 非法值 ⇒ 按默认（**开启**）处理，不抛异常（有 ERROR 日志）",
               interview_service.avatar_llm_questions_enabled() is True)

    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        user_id, other_id, job_id = await _seed(db)

        # ------------------------------------------------------------
        # [2] 显式关闭（=0）⇒ 零变化
        # ------------------------------------------------------------
        print("\n[2] 显式关闭（INTERVIEW_AVATAR_LLM_QUESTIONS=0）⇒ 数字人面试行为零变化")

        with _flag("0"):
            sid_off = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=3)
            await interview_service.start_session(db, user_id, sid_off)
            items_off = await _stored(db, sid_off)
            _check("[2.1] 数字人开场仍**整场**落库（3 道）",
                   len(items_off) == 3, str(len(items_off)))
            _check("  └ 题目 == build_question_plan 逐字段",
                   items_off == await _rule_plan(db, sid_off))

            out_off = await _submit(db, user_id, sid_off)
            _check("[2.2] 答题后下一题**仍从库里取**（question_no=2，规则题）",
                   out_off["next_question"] is not None
                   and out_off["next_question"]["question_no"] == 2
                   and out_off["next_question"]["question"]
                   == (await _rule_plan(db, sid_off))[1]["question"],
                   str(out_off["next_question"])[:120])
            _check("  └ 库里仍是整场 3 道（没有被改写）",
                   len(await _stored(db, sid_off)) == 3)

        # ------------------------------------------------------------
        # [3] 默认态（**不设环境变量**）：数字人只落第 1 题；text 不受影响
        # ------------------------------------------------------------
        print("\n[3] 默认态（未配置 ⇒ 开启）：数字人开场只落第 1 题，text 不受影响")

        # ★ 这里刻意用 ``_flag(None)``（= 把环境变量**清掉**）而不是 ``_flag("1")``：
        #   要验的正是「**不配置**时的默认行为」——生产部署就是这个状态。
        with _flag(None):
            sid_on = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=3)
            started_on = await interview_service.start_session(db, user_id, sid_on)
            items_on = await _stored(db, sid_on)
            plan_on = await _rule_plan(db, sid_on)

            _check("[3.1] 数字人开场**只落第 1 题**", len(items_on) == 1, str(len(items_on)))
            _check("  └ 第 1 题是规则题（与整场落库的第 1 题逐字段一致）",
                   items_on == plan_on[:1], f"{items_on} vs {plan_on[:1]}")
            _check("  └ 交付的第一题就是它（题号 1）",
                   started_on["question"]["question_no"] == 1
                   and started_on["question"]["question"] == plan_on[0]["question"])
            _check("  └ 会话仍标记整场题量（total_questions=3，结束条件不变）",
                   started_on["session"]["total_questions"] == 3
                   and started_on["session"]["current_question_no"] == 1
                   and started_on["session"]["status"] == SESSION_STATUS_ONGOING)

            sid_txt = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_TEXT, total=3)
            await interview_service.start_session(db, user_id, sid_txt)
            items_txt = await _stored(db, sid_txt)
            _check("[3.2] text 会话**仍整场落库**（开关对文字面试无影响）",
                   len(items_txt) == 3 and items_txt == await _rule_plan(db, sid_txt),
                   str(len(items_txt)))

        # ------------------------------------------------------------
        # [4] 打开后：答题 → LLM 生成下一题并落库 → 可走完并出报告
        # ------------------------------------------------------------
        print("\n[4] 打开后：答题 ⇒ LLM 生成下一题并落库")

        with _flag("1"):
            sid_flow = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=3)
            await interview_service.start_session(db, user_id, sid_flow)

            with _installed_spark(_spark_ok("Q2-LLM：讲讲 Redis 的持久化机制")):
                out1 = await _submit(db, user_id, sid_flow)
            items_after1 = await _stored(db, sid_flow)

            _check("[4.1] 答题后 LLM 题已**落库**（库里 2 道）",
                   len(items_after1) == 2, str(len(items_after1)))
            _check("  └ 题号连续（1, 2）",
                   [i["question_no"] for i in items_after1] == [1, 2],
                   str([i["question_no"] for i in items_after1]))
            _check("  └ 第 2 题内容来自模型（不是规则模板）",
                   items_after1[1]["question"] == "Q2-LLM：讲讲 Redis 的持久化机制",
                   items_after1[1]["question"])
            _check("  └ 交付的 next_question 就是刚落库的那道（已带 id）",
                   out1["next_question"]["question_no"] == 2
                   and out1["next_question"]["question"] == items_after1[1]["question"]
                   and out1["next_question"]["id"] is not None,
                   str(out1["next_question"])[:120])
            _check("  └ 未答完（all_answered=False）", out1["all_answered"] is False)

            with _installed_spark(_spark_ok("Q3-LLM：分布式锁怎么设计")):
                out2 = await _submit(db, user_id, sid_flow)
            _check("[4.2] 第 2 题答完后同样补出第 3 题",
                   out2["next_question"]["question_no"] == 3
                   and out2["next_question"]["question"] == "Q3-LLM：分布式锁怎么设计",
                   str(out2["next_question"])[:120])

            out3 = await _submit(db, user_id, sid_flow)
            _check("[4.3] 第 3 题答完 ⇒ all_answered / next_question=None",
                   out3["all_answered"] is True and out3["next_question"] is None)
            _check("  └ 没有多出题（库里恰好 3 道）", len(await _stored(db, sid_flow)) == 3)

            ended = await interview_service.end_session(db, user_id, sid_flow)
            _check("[4.4] 可正常结束并出报告（报告口径未变）",
                   ended["session"]["status"] == "finished"
                   and 0 <= ended["report"]["total_score"] <= 100,
                   str(ended.get("report", {}).get("total_score")))

        # ------------------------------------------------------------
        # [5] 打开后：LLM 失败 ⇒ 规则兜底
        # ------------------------------------------------------------
        print("\n[5] 打开后：LLM 失败 ⇒ 规则兜底（第 k 项）")

        with _flag("1"):
            sid_fb = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=3)
            await interview_service.start_session(db, user_id, sid_fb)

            with _installed_spark(_spark_fail()):
                out_fb = await _submit(db, user_id, sid_fb)

            plan_fb = await _rule_plan(db, sid_fb)
            _check("[5.1] 面试**没有被打断**（答题仍成功、下一题仍在）",
                   out_fb["next_question"] is not None
                   and out_fb["all_answered"] is False, str(out_fb.get("next_question")))
            _check("  └ 兜底题 == 规则计划第 2 项（逐字段同源）",
                   {k: out_fb["next_question"][k] for k in ITEM_KEYS} == plan_fb[1],
                   f"{ {k: out_fb['next_question'][k] for k in ITEM_KEYS} } != {plan_fb[1]}")
            _check("  └ 兜底题已落库", len(await _stored(db, sid_fb)) == 2)

            gen = await interview_service.generate_and_persist_next(db, user_id, sid_fb)
            _check("[5.2] 幂等：题号已存在 ⇒ source=existing，不重复插题",
                   gen["source"] == interview_service.SOURCE_EXISTING
                   and len(await _stored(db, sid_fb)) == 2, str(gen["source"]))

        # ------------------------------------------------------------
        # [6] 打开后：两条路都失败 ⇒ 如实上报、不崩
        # ------------------------------------------------------------
        print("\n[6] 打开后：LLM 与规则兜底都失败 ⇒ 如实上报")

        with _flag("1"):
            sid_bad = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=3)
            await interview_service.start_session(db, user_id, sid_bad)

            async def _no_rule_item(db_, session_, question_no_):
                return None

            with _installed_spark(_spark_fail()), _patched(
                interview_service, "_rule_plan_item", _no_rule_item
            ):
                # 先答完第 1 题（题号推进到 2），此时「两条路都失败」才会真正触发
                out_bad = await _submit(db, user_id, sid_bad)
                _check("[6.1] submit_answer **不抛异常**（答题与评分照常完成）",
                       out_bad["answer"]["score"] is not None
                       and out_bad["next_question"] is None
                       and out_bad["all_answered"] is False,
                       str(out_bad.get("next_question")))
                _check("  └ 失败不留脏数据（库里仍只有第 1 题）",
                       len(await _stored(db, sid_bad)) == 1)

                gen_bad = await interview_service.generate_and_persist_next(
                    db, user_id, sid_bad
                )
                _check("[6.2] 直接调用：ok=False / source=failed / 稳定错误码",
                       gen_bad["ok"] is False
                       and gen_bad["source"] == interview_service.SOURCE_FAILED
                       and interview_service.ERROR_QUESTION_GENERATE_FAILED
                       in gen_bad["errors"]
                       and gen_bad["question"] is None,
                       str(gen_bad))
                _check("  └ 键集恒定（= NEXT_QUESTION_FIELDS）",
                       set(gen_bad) == set(interview_service.NEXT_QUESTION_FIELDS),
                       str(sorted(gen_bad)))

        # ------------------------------------------------------------
        # [7] 契约与守卫
        # ------------------------------------------------------------
        print("\n[7] 契约与守卫")

        _check("[7.1] generate_and_persist_next 是协程函数",
               inspect.iscoroutinefunction(interview_service.generate_and_persist_next))
        _check("  └ 恒定键集已声明（5 键）",
               interview_service.NEXT_QUESTION_FIELDS
               == ("ok", "question", "source", "errors", "error"),
               str(interview_service.NEXT_QUESTION_FIELDS))

        sid_nf = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=2)
        ok_404, exc_404 = await _raises(
            lambda: interview_service.generate_and_persist_next(db, other_id, sid_nf),
            HTTPException,
        )
        _check("[7.2] 非本人 ⇒ 404", ok_404 and getattr(exc_404, "status_code", None) == 404,
               f"{type(exc_404).__name__}: {exc_404}")

        ok_400, exc_400 = await _raises(
            lambda: interview_service.generate_and_persist_next(db, user_id, sid_nf),
            HTTPException,
        )
        _check("  └ 未开始（created）⇒ 400", ok_400
               and getattr(exc_400, "status_code", None) == 400,
               f"{type(exc_400).__name__}: {exc_400}")

        service_src = inspect.getsource(interview_service)
        service_imports = _imported_modules(service_src)
        submit_src = inspect.getsource(interview_service.submit_answer)
        start_src = inspect.getsource(interview_service.start_session)

        _check("[7.3] Service **不 import** 出题策略层（策略只从编排层进入）",
               "services.question_generator" not in service_imports,
               str(sorted(m for m in service_imports if "question" in m)))
        _check("  └ submit_answer 在「库里没有下一题」时才补题",
               "generate_and_persist_next" in submit_src
               and "avatar_llm_questions_enabled" in submit_src)
        _check("[7.4] 默认路径未动：start_session 仍直调 build_question_plan",
               "build_question_plan" in start_src)
        _check("  └ start_session 源码不含策略层模块名",
               "question_generator" not in start_src)

        # text 全链路逐字段不变（硬验收）
        with _flag("1"):
            sid_text = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_TEXT, total=3)
            await interview_service.start_session(db, user_id, sid_text)
            for _ in range(3):
                await _submit(db, user_id, sid_text)
            ended_text = await interview_service.end_session(db, user_id, sid_text)
        _check("[7.5] text 会话在开关打开时**逐字段不变**（仍整场规则出题）",
               await _stored(db, sid_text) == await _rule_plan(db, sid_text)
               and ended_text["session"]["status"] == "finished",
               str(ended_text["session"]["status"]))

        # ------------------------------------------------------------
        # [8] 历史回答注入（A-2）
        # ------------------------------------------------------------
        print("\n[8] 历史回答注入（有历史 vs 无历史）")

        with _flag("1"):
            sid_h = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=3)
            await interview_service.start_session(db, user_id, sid_h)
            q1_text = (await _stored(db, sid_h))[0]["question"]

            spark_h = _spark_ok("Q2-带历史：针对你刚才的回答继续深挖")
            with _installed_spark(spark_h):
                await _submit(db, user_id, sid_h)
            sent_prompt = spark_h.calls[0]

            _check("[8.1] ★ 端到端：送进模型的 Prompt 带着**第 1 题的题干**（历史回答已注入）",
                   q1_text in sent_prompt, sent_prompt[:200])
            _check("  └ 也带着候选人的作答内容",
                   ANSWER[:16] in sent_prompt)
            _check("  └ 走的是新的 Prompt 组（含「历史回答」小节标题）",
                   "## 五、历史回答" in sent_prompt)

        pack_with = {
            "interview_type": "technical", "difficulty": "mid",
            "current_stage": "technical", "job_title": "后端开发工程师",
            "job_description": "技能要求：Redis / MySQL",
            "resume_summary": "张三，5 年后端经验",
            "history": [{
                "question_no": 1, "question": "请介绍 Redis 的持久化机制",
                "answer": "RDB 是快照，AOF 是追加日志，我线上用 AOF everysec",
                "score": 72, "topic": "Redis", "question_type": "technical",
            }],
            "asked_questions": ["请介绍 Redis 的持久化机制"],
            "covered_topics": ["Redis"], "weak_topics": [],
        }
        pack_without = {**pack_with, "history": []}
        plan_items = [{
            "question_no": 1, "question": "请介绍 Redis 的持久化机制",
            "question_type": "technical", "topic": "Redis", "difficulty": "mid",
            "expected_points": [],
        }]

        vars_with = na.build_next_variables(pack_with, plan_items)
        vars_without = na.build_next_variables(pack_without, plan_items)
        _, p_with = na.render_next_prompt(vars_with)
        _, p_without = na.render_next_prompt(vars_without)

        _check("[8.2] 有历史 ⇒ Prompt 含问答行；无历史 ⇒ 含占位串",
               "RDB 是快照" in p_with
               and na.EMPTY_HISTORY in p_without
               and "RDB 是快照" not in p_without)
        rendered_history = "\n".join(
            f"- {line}" for line in interview_context_pack.history_lines(pack_with)
        )
        # ★ 注意渲染口径的不对称：非空历史是**列表** ⇒ 逐行加 "- " 前缀；
        #   空历史是**纯字符串**占位符 ⇒ 原样渲染、不带前缀。断言必须按真实口径写。
        _check("  └ ★ 字节级：两个 Prompt **只差历史小节**",
               p_with.replace(rendered_history, na.EMPTY_HISTORY, 1) == p_without,
               "两 Prompt 的差异不止历史小节")
        _check("  └ 严格模式未报错（变量集与调用点一致）",
               "{{" not in p_with and "{{" not in p_without)

        class _BranchingSpark:
            """按 Prompt 里**有没有那段历史回答**返回不同问题的假模型。"""

            def __init__(self):
                self.calls: list = []

            async def chat_async(self, message: str) -> str:
                self.calls.append(message)
                question = (
                    "针对你刚提到的 AOF，appendfsync 的三种取值你会怎么选？"
                    if "RDB 是快照" in message
                    else "请说说 MySQL 的索引下推是怎么工作的？"
                )
                return json.dumps({
                    "question": question, "question_type": "technical",
                    "topic": "Redis", "difficulty": "mid",
                    "expected_points": ["appendfsync"], "reason": "stub",
                }, ensure_ascii=False)

        b_with = _BranchingSpark()
        res_with = await na.generate_question(pack_with, plan_items, spark=b_with)
        b_without = _BranchingSpark()
        res_without = await na.generate_question(pack_without, plan_items, spark=b_without)
        b_again = _BranchingSpark()
        res_again = await na.generate_question(pack_with, plan_items, spark=b_again)

        _check("[8.3] ★★ 同一个假模型：有历史 ⇒ 出的题不同（历史**真的**影响了出题）",
               res_with.get("ok") is True and res_without.get("ok") is True
               and res_with["question"] != res_without["question"],
               f"{res_with.get('question')!r} vs {res_without.get('question')!r}")
        _check("  └ 恒真自检：同一 Prompt 两次 ⇒ 结果一致（分支确由 Prompt 决定）",
               res_again.get("question") == res_with.get("question"))
        _check("  └ 无历史时不会带出「深挖上一题」那类问题",
               "AOF" not in (res_without.get("question") or ""),
               str(res_without.get("question")))

        # ------------------------------------------------------------
        # [9] 模板契约与漂移守卫（A-2）
        # ------------------------------------------------------------
        print("\n[9] 新 Prompt 组契约与漂移守卫")

        _check("[9.1] prompts/interview_next 恰好 2 个模板",
               prompts.list_prompts("interview_next") == ["question", "question_knowledge"],
               str(prompts.list_prompts("interview_next")))
        _check("  └ prompts/interview 仍**恰好** 5 个（未被污染）",
               prompts.list_prompts("interview") == [
                   "planner", "question", "question_knowledge", "question_repair", "system"],
               str(prompts.list_prompts("interview")))

        base_vars = prompts.template_variables("question", group="interview_next")
        k_vars = prompts.template_variables("question_knowledge", group="interview_next")
        _check("[9.2] question.txt 声明 11 个变量（顺序 = 首次出现顺序）",
               tuple(base_vars) == na.NEXT_VARIABLES, str(base_vars))
        _check("  └ 变体 = 原 11 + knowledge_context，插在 interview_plan 之后",
               set(k_vars) == set(base_vars) | {na.KNOWLEDGE_VARIABLE}
               and tuple(k_vars) == tuple(base_vars[:7])
               + (na.KNOWLEDGE_VARIABLE,) + tuple(base_vars[7:]),
               str(k_vars))

        base_path = BACKEND_DIR / "prompts" / "interview_next" / "question.txt"
        k_path = base_path.with_name("question_knowledge.txt")
        base_src = base_path.read_text(encoding="utf-8")
        base_bytes = base_path.read_bytes()
        k_bytes = k_path.read_bytes()
        probe = KNOWLEDGE_BLOCK_RAW.encode("utf-8")

        _check("[9.3] 原模板不含知识小节",
               "## 四·五、参考知识" not in base_src)
        _check("  └ 变体含且**仅含一次**", k_bytes.count(probe) == 1,
               str(k_bytes.count(probe)))
        _check("  └ ★ 去掉知识小节后与 question.txt **逐字节相同**（防两模板分叉）",
               k_bytes.replace(probe, b"", 1) == base_bytes)
        base_crlf = base_bytes.count(b"\r\n")
        k_crlf = k_bytes.count(b"\r\n")
        _check("  └ 行尾符也一致（CRLF 计数相等）",
               base_crlf == k_crlf, f"{base_crlf} vs {k_crlf}")

        name_off, _prompt_off = na.render_next_prompt(vars_with)
        name_on, _prompt_on = na.render_next_prompt(
            vars_with, [{"content": "AOF everysec 会丢 1 秒数据", "source": "mock://handbook"}]
        )
        _check("[9.4] 空知识 ⇒ 走原模板",
               name_off == na.PROMPT_QUESTION, name_off)
        _check("  └ 非空知识 ⇒ 走变体",
               name_on == na.PROMPT_QUESTION_KNOWLEDGE, name_on)
        _check("  └ ★ 空知识时 Prompt 里没有「参考知识」小节（行为一致）",
               "参考知识" not in _prompt_off)
        _check("  └ 非空知识时知识正文进了 Prompt",
               "AOF everysec" in _prompt_on)

    await engine.dispose()

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
