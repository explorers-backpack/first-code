# -*- coding: utf-8 -*-
"""AI 模拟面试 · QuestionGenerator 抽象层自检

无需 pytest，直接运行：
    python backend/tests/test_question_generator.py

不依赖本机 MySQL：DATABASE_URL 指向 SQLite 内存库 + StaticPool。
**不调用真实 LLM**：agent 模式一律注入 Mock Spark。

被测对象：``services.question_generator``
（``QuestionGenerator`` / ``RuleQuestionGenerator`` / ``AgentQuestionGenerator``）

覆盖范围：
1. 抽象层与工厂（抽象基类、mode 标识、get_generator、未知 mode 异常）
2. **rule 模式结果与修改前逐字段一致**（封装 ``build_question_plan``，不落库、不调 LLM）
3. agent 模式调用 Mock Agent 生成问题（正常 / Agent 失败 / Validator 失败 / 闸门）
4. **两种模式互不影响**（探针证明各自只走自己的实现；互不干扰）
5. 不改变默认 ``start_session`` 行为（``build_question_plan`` 仍在、Service 不依赖本层）
6. 依赖边界与统一契约（不反向依赖 Service、不 import main、信封字段恒定）
"""

import asyncio
import ast
import inspect
import json
import os
import pathlib
import subprocess
import sys

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from sqlalchemy import func, select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import InterviewQuestion, Job, User  # noqa: E402
from schemas.interview import AnswerSubmitRequest, SessionCreateRequest  # noqa: E402
from services import (  # noqa: E402
    interview_agent,
    interview_core,
    interview_service,
    question_generator as qg,
)
from services.resume_scoring import extract_skills  # noqa: E402

_PASSED = 0
_FAILED = 0

JOB_SKILLS = "Python,MySQL,Redis,Kafka,Docker"

GOOD_ANSWER = (
    "首先，我在上一家公司负责订单中台的重构。当时系统 QPS 只有 800，"
    "我主导把单体服务拆成 8 个微服务，技术栈使用 Python 与 MySQL，"
    "引入 Redis 做缓存、Kafka 做异步解耦。结果是 QPS 提升到 5000。"
)

QUESTION_JSON = json.dumps(
    {
        "question": "请介绍一下你在项目中是如何使用 Redis 做缓存的？",
        "question_type": "technical",
        "topic": "Redis",
        "difficulty": "mid",
        "expected_points": ["缓存穿透", "过期策略"],
        "reason": "考察缓存实践经验",
    },
    ensure_ascii=False,
)


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def _imported_modules(source: str) -> set:
    """用 AST 提取真正被 import 的模块名（不用子串匹配：docstring 会误伤）。"""
    tree = ast.parse(source)
    names: set = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module)
                for alias in node.names:
                    names.add(f"{node.module}.{alias.name}")
    return names


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


class ExplodingSpark:
    """一旦被调用就报错——用来证明 rule 模式**从不**触碰大模型。"""

    async def chat_async(self, message: str) -> str:
        raise AssertionError("rule 模式不应调用大模型")


class _Spy:
    """记录调用次数并转发给原实现的探针。"""

    def __init__(self, original):
        self.original = original
        self.calls = 0

    def __call__(self, *args, **kwargs):
        self.calls += 1
        return self.original(*args, **kwargs)


def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed(db: AsyncSession):
    user = User(username="qg_t", email="qg_t@example.com", password_hash="x", role="user")
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
    db.add_all([user, job])
    await db.commit()
    await db.refresh(user)
    await db.refresh(job)
    return user.id, job.id


async def _make_session(db: AsyncSession, user_id: int, **kwargs) -> int:
    """建会话并 start（走规则出题，与生产路径一致）。"""
    payload = SessionCreateRequest(job_id=kwargs.pop("job_id", None),
                                   total_questions=kwargs.pop("total_questions", 5),
                                   **kwargs)
    session_id = (await interview_service.create_session(db, user_id, payload))["session"]["id"]
    await interview_service.start_session(db, user_id, session_id)
    return session_id


async def _expected_rule_plan(db: AsyncSession, session_id: int):
    """独立复算「修改前」的规则出题结果（直接调 build_question_plan）。"""
    session = await interview_core.load_session_row(db, session_id)
    job = await interview_core.load_job_row(db, session.job_id)
    resume = await interview_core.load_resume_row(db, session.resume_id)
    skills = extract_skills(resume.content) if resume is not None and resume.content else []
    return interview_core.build_question_plan(session, job, skills)


async def _question_count(db: AsyncSession) -> int:
    return int((await db.execute(select(func.count()).select_from(InterviewQuestion))).scalar())


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 模拟面试 · QuestionGenerator 抽象层自检（rule / agent 共存）")
    print("=" * 70)

    qg_src = inspect.getsource(qg)
    qg_imports = _imported_modules(qg_src)
    service_src = inspect.getsource(interview_service)
    service_imports = _imported_modules(service_src)
    core_src = inspect.getsource(interview_core)

    # ------------------------------------------------------------
    # [1] 抽象层与工厂
    # ------------------------------------------------------------
    print("\n[1] 抽象层与工厂")

    _check("QuestionGenerator 是抽象基类", inspect.isabstract(qg.QuestionGenerator))
    _check("  └ generate 是抽象方法",
           "generate" in getattr(qg.QuestionGenerator, "__abstractmethods__", set()))
    try:
        qg.QuestionGenerator()
        instantiable = True
    except TypeError:
        instantiable = False
    _check("  └ 抽象基类不可直接实例化", instantiable is False)

    _check("RuleQuestionGenerator.mode == 'rule'", qg.RuleQuestionGenerator.mode == "rule")
    _check("AgentQuestionGenerator.mode == 'agent'", qg.AgentQuestionGenerator.mode == "agent")
    _check("两个实现都继承 QuestionGenerator",
           issubclass(qg.RuleQuestionGenerator, qg.QuestionGenerator)
           and issubclass(qg.AgentQuestionGenerator, qg.QuestionGenerator))
    _check("两个实现的 generate 都是协程函数",
           inspect.iscoroutinefunction(qg.RuleQuestionGenerator.generate)
           and inspect.iscoroutinefunction(qg.AgentQuestionGenerator.generate))

    _check("GENERATOR_MODES == ('rule', 'agent')", qg.GENERATOR_MODES == ("rule", "agent"))
    _check("默认模式为 rule（不改变现有行为）", qg.DEFAULT_MODE == "rule")
    _check("get_generator() 默认返回规则生成器",
           isinstance(qg.get_generator(), qg.RuleQuestionGenerator))
    _check("get_generator('agent') 返回 Agent 生成器",
           isinstance(qg.get_generator("agent"), qg.AgentQuestionGenerator))
    _check("生成器无状态，可共享单例（同一对象）",
           qg.get_generator("rule") is qg.get_generator("rule"))

    try:
        qg.get_generator("nope")
        raised = False
    except qg.UnknownModeError as exc:
        raised = "nope" in str(exc)
    _check("未知 mode 抛 UnknownModeError", raised)
    _check("  └ UnknownModeError 继承 QuestionGeneratorError", 
           issubclass(qg.UnknownModeError, qg.QuestionGeneratorError))
    _check("  └ 同时继承 ValueError（编程错误，贴近内建异常）",
           issubclass(qg.UnknownModeError, ValueError))

    _check("信封字段契约已声明（9 键）",
           qg.GENERATED_SET_FIELDS == ("mode", "ok", "questions", "question", "count",
                                       "reason", "stage", "errors", "error"),
           str(qg.GENERATED_SET_FIELDS))
    _check("单题字段契约与 build_question_plan 产出一致",
           qg.QUESTION_ITEM_FIELDS == ("question_no", "question", "question_type",
                                       "topic", "difficulty", "expected_points"),
           str(qg.QUESTION_ITEM_FIELDS))

    # ------------------------------------------------------------
    # 环境准备
    # ------------------------------------------------------------
    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        user_id, job_id = await _seed(db)

        # ------------------------------------------------------------
        # [2] rule 模式结果与修改前逐字段一致
        # ------------------------------------------------------------
        print("\n[2] rule 模式：结果与修改前（build_question_plan）逐字段一致")

        rule_sid = await _make_session(db, user_id, job_id=job_id, total_questions=6)
        expected = await _expected_rule_plan(db, rule_sid)
        before = await _question_count(db)

        rule_res = await qg.generate_questions(db, rule_sid, "rule")

        _check("[2.1] ok=True / mode='rule'",
               rule_res["ok"] is True and rule_res["mode"] == "rule", str(rule_res.get("error")))
        _check("  └ questions **逐字段完全等于** build_question_plan 的输出",
               rule_res["questions"] == expected,
               f"{rule_res['questions'][:1]} != {expected[:1]}")
        _check("  └ count 与计划题量一致",
               rule_res["count"] == len(expected) == 6, str(rule_res["count"]))
        _check("  └ errors 为空、error 为 None",
               rule_res["errors"] == [] and rule_res["error"] is None)
        _check("  └ rule 模式不产生 reason / stage（恒为空串）",
               rule_res["reason"] == "" and rule_res["stage"] == "")
        _check("  └ 每道题恒为 6 字段",
               all(tuple(item) == qg.QUESTION_ITEM_FIELDS for item in rule_res["questions"]))
        _check("  └ 题号从 1 连续到 N",
               [i["question_no"] for i in rule_res["questions"]] == list(range(1, 7)))

        after = await _question_count(db)
        _check("  └ 不落库：题目表行数不变", before == after, f"{before} -> {after}")

        # 确定性 + 不调大模型
        again = await qg.generate_questions(db, rule_sid, "rule")
        _check("  └ 确定性：两次调用结果完全相同", again["questions"] == rule_res["questions"])

        with_spark = await qg.generate_questions(
            db, rule_sid, "rule", spark=ExplodingSpark()
        )
        _check("  └ 注入「一调就炸」的 Spark 仍成功 → 规则模式从不调用大模型",
               with_spark["ok"] is True and with_spark["questions"] == rule_res["questions"])

        with_plan = await qg.generate_questions(
            db, rule_sid, "rule", plan={"garbage": 1}
        )
        _check("  └ 注入 plan 不改变规则出题结果（该参数在 rule 模式不参与生成）",
               with_plan["questions"] == rule_res["questions"])

        # 三种面试类型都一致
        type_ok = True
        for itype in ("technical", "behavioral", "comprehensive"):
            sid = await _make_session(db, user_id, job_id=job_id, total_questions=5,
                                      interview_type=itype)
            exp = await _expected_rule_plan(db, sid)
            got = await qg.generate_questions(db, sid, "rule")
            if got["questions"] != exp or got["count"] != 5:
                type_ok = False
        _check("  └ 三种面试类型（technical/behavioral/comprehensive）均逐字段一致", type_ok)

        # context 注入 → question 访问器取当前题号那一道
        ctx = {"current_question_no": 3}
        picked = await qg.generate_questions(db, rule_sid, "rule", context=ctx)
        _check("  └ 注入 context 后 question 指向 current_question_no 那一道",
               picked["question"]["question_no"] == 3, str(picked["question"]["question_no"]))
        default_pick = await qg.generate_questions(db, rule_sid, "rule")
        _check("  └ 未注入 context 时 question 退回第一道",
               default_pick["question"]["question_no"] == 1)

        missing = await qg.generate_questions(db, 999999, "rule")
        _check("  └ 会话不存在：ok=False + session_not_found",
               missing["ok"] is False
               and interview_core.ERROR_SESSION_NOT_FOUND in missing["errors"],
               str(missing["errors"]))

        # ------------------------------------------------------------
        # [3] agent 模式：调用 Mock Agent 生成问题
        # ------------------------------------------------------------
        print("\n[3] agent 模式：调用 Mock Agent 生成问题")

        agent_sid = await _make_session(db, user_id, job_id=job_id, total_questions=5)
        before_agent = await _question_count(db)
        spark = MockSpark(QUESTION_JSON)
        agent_res = await qg.generate_questions(db, agent_sid, "agent", spark=spark)

        _check("[3.1] ok=True / mode='agent'",
               agent_res["ok"] is True and agent_res["mode"] == "agent",
               str(agent_res.get("error")))
        _check("  └ 恰好调用一次 Spark", len(spark.calls) == 1)
        _check("  └ 只产出 1 道题（单题粒度）",
               agent_res["count"] == 1 and len(agent_res["questions"]) == 1)
        _check("  └ 题目为统一 6 字段",
               tuple(agent_res["questions"][0]) == qg.QUESTION_ITEM_FIELDS,
               str(sorted(agent_res["questions"][0])))
        _check("  └ 题目内容来自模型输出",
               agent_res["question"]["topic"] == "Redis"
               and agent_res["question"]["difficulty"] == "mid"
               and agent_res["question"]["expected_points"] == ["缓存穿透", "过期策略"])
        _check("  └ question 访问器指向同一道题",
               agent_res["question"] is agent_res["questions"][0])
        _check("  └ 信封带 reason（模型出题理由）", agent_res["reason"] == "考察缓存实践经验")
        _check("  └ 信封带 stage（当前面试阶段）", agent_res["stage"] == "introduction",
               str(agent_res["stage"]))
        _check("  └ 不落库（持久化由调用方负责）",
               await _question_count(db) == before_agent,
               f"{before_agent} -> {await _question_count(db)}")

        # Agent 失败
        bad_spark = MockSpark("这不是 JSON", "这也不是 JSON")
        agent_fail = await qg.generate_questions(db, agent_sid, "agent", spark=bad_spark)
        _check("[3.2] Agent 失败：ok=False",
               agent_fail["ok"] is False)
        _check("  └ questions 为空、question 为 None",
               agent_fail["questions"] == [] and agent_fail["question"] is None)
        _check("  └ count == 0", agent_fail["count"] == 0)
        _check("  └ errors 含 agent_failed",
               interview_core.ERROR_AGENT_FAILED in agent_fail["errors"],
               str(agent_fail["errors"]))
        _check("  └ 非法 JSON 触发 1 次修复（共 2 次调用）", len(bad_spark.calls) == 2)

        # Validator 失败（Agent 自带内联校验，故用桩 Agent 构造「Agent 通过、Validator 拒绝」）
        original_agent_generate = interview_agent.generate_question

        async def _stub_agent_ok_but_invalid(
            context, plan=None, resume=None, job=None, *, knowledge_context=None, spark=None
        ):
            return {
                "ok": True,
                "question": "请解释一下 Redis 的持久化机制",
                "question_type": "technical",
                "topic": "Redis",
                "difficulty": "god_mode",
                "expected_points": ["RDB", "AOF"],
                "reason": "stub",
                "error": None,
            }

        interview_agent.generate_question = _stub_agent_ok_but_invalid
        try:
            agent_invalid = await qg.generate_questions(db, agent_sid, "agent", spark=MockSpark())
        finally:
            interview_agent.generate_question = original_agent_generate
        _check("[3.3] Validator 失败：ok=False + validation_failed",
               agent_invalid["ok"] is False
               and interview_core.ERROR_VALIDATION_FAILED in agent_invalid["errors"],
               str(agent_invalid["errors"]))
        _check("  └ 保留 Validator 原始错误码 invalid_difficulty",
               "invalid_difficulty" in agent_invalid["errors"], str(agent_invalid["errors"]))
        _check("  └ questions 为空（不放行未通过校验的问题）", agent_invalid["questions"] == [])

        # 闸门
        one_sid = await _make_session(db, user_id, total_questions=1)
        await interview_service.submit_answer(
            db, user_id, one_sid, AnswerSubmitRequest(answer_text=GOOD_ANSWER)
        )
        done = await qg.generate_questions(db, one_sid, "agent", spark=MockSpark())
        _check("[3.4] 全部答完：all_answered",
               interview_core.ERROR_ALL_ANSWERED in done["errors"], str(done["errors"]))

        await interview_service.end_session(db, user_id, one_sid)
        ended = await qg.generate_questions(db, one_sid, "agent", spark=MockSpark())
        _check("  └ 已结束：session_finished",
               interview_core.ERROR_SESSION_FINISHED in ended["errors"], str(ended["errors"]))

        # ------------------------------------------------------------
        # [4] 两种模式互不影响
        # ------------------------------------------------------------
        print("\n[4] 两种模式互不影响")

        iso_sid = await _make_session(db, user_id, job_id=job_id, total_questions=5)
        rule_first = await qg.generate_questions(db, iso_sid, "rule")
        await qg.generate_questions(db, iso_sid, "agent", spark=MockSpark(QUESTION_JSON))
        rule_after_agent = await qg.generate_questions(db, iso_sid, "rule")
        _check("[4.1] 跑过 agent 之后，rule 结果完全不变",
               rule_after_agent["questions"] == rule_first["questions"])

        plan_spy = _Spy(interview_core.build_question_plan)
        core_flow_spy = _Spy(interview_core.generate_next_question)
        interview_core.build_question_plan = plan_spy
        interview_core.generate_next_question = core_flow_spy
        try:
            await qg.generate_questions(db, iso_sid, "rule")
            _check("[4.2] rule 模式只走 build_question_plan",
                   plan_spy.calls == 1 and core_flow_spy.calls == 0,
                   f"plan={plan_spy.calls} flow={core_flow_spy.calls}")
            await qg.generate_questions(db, iso_sid, "agent", spark=MockSpark(QUESTION_JSON))
            _check("  └ agent 模式只走 generate_next_question",
                   core_flow_spy.calls == 1 and plan_spy.calls == 1,
                   f"plan={plan_spy.calls} flow={core_flow_spy.calls}")
        finally:
            interview_core.build_question_plan = plan_spy.original
            interview_core.generate_next_question = core_flow_spy.original

        # 替换规则实现 → agent 不受影响
        original_plan = interview_core.build_question_plan
        interview_core.build_question_plan = lambda session, job, skills: [
            {"question_no": 1, "question": "SENTINEL-RULE", "question_type": "intro",
             "topic": "t", "difficulty": "mid", "expected_points": []}
        ]
        try:
            agent_unaffected = await qg.generate_questions(
                db, iso_sid, "agent", spark=MockSpark(QUESTION_JSON)
            )
        finally:
            interview_core.build_question_plan = original_plan
        _check("  └ 替换规则实现后 agent 结果不受影响",
               agent_unaffected["ok"] is True
               and agent_unaffected["question"]["question"] != "SENTINEL-RULE")

        # 替换 agent 实现 → rule 不受影响
        original_flow = interview_core.generate_next_question

        async def _fake_flow(db_, sid, *, user_id=None, spark=None, context=None, plan=None):
            return {"ok": True, "question_no": 1, "question": "SENTINEL-AGENT",
                    "question_type": "x", "topic": "t", "difficulty": "mid",
                    "expected_points": [], "reason": "", "stage": "",
                    "warnings": [], "errors": [], "error": None}

        interview_core.generate_next_question = _fake_flow
        try:
            rule_unaffected = await qg.generate_questions(db, iso_sid, "rule")
        finally:
            interview_core.generate_next_question = original_flow
        _check("  └ 替换 agent 实现后 rule 结果不受影响",
               rule_unaffected["questions"] == rule_first["questions"])

        _check("  └ 两种模式信封字段集完全相同",
               set(rule_first) == set(agent_res) == set(qg.GENERATED_SET_FIELDS),
               f"{sorted(set(rule_first) ^ set(qg.GENERATED_SET_FIELDS))}")
        _check("  └ 两种模式题目项字段集完全相同",
               set(rule_first["questions"][0]) == set(agent_res["questions"][0]),
               f"{sorted(rule_first['questions'][0])} vs {sorted(agent_res['questions'][0])}")

        # ------------------------------------------------------------
        # [5] 不改变默认 start_session 行为
        # ------------------------------------------------------------
        print("\n[5] 不改变默认 start_session 行为")

        _check("[5.1] build_question_plan 未被删除（仍在 Core 且可调用）",
               callable(getattr(interview_core, "build_question_plan", None)))
        _check("  └ 仍在 Core 的 __all__ 中",
               "build_question_plan" in interview_core.__all__)

        start_src = inspect.getsource(interview_service.start_session)
        _check("[5.2] start_session 未引用 question_generator（默认路径不变）",
               "question_generator" not in start_src)
        _check("  └ start_session 仍直接调用 Core 的 build_question_plan",
               "build_question_plan" in start_src)
        _check("  └ Service 不 import question_generator",
               "services.question_generator" not in service_imports,
               str(sorted(service_imports)))
        _check("  └ Core 不 import question_generator（避免成环）",
               "services.question_generator" not in _imported_modules(core_src))

        # 完整生命周期仍然工作，且题目内容 == 规则出题结果
        life_sid = await _make_session(db, user_id, job_id=job_id, total_questions=3)
        detail = await interview_service.get_session_detail(db, user_id, life_sid)
        life_expected = await _expected_rule_plan(db, life_sid)
        _check("[5.3] start_session 生成的题目 == build_question_plan 结果",
               [q["question"] for q in detail["questions"]] == [q["question"] for q in life_expected])
        await interview_service.submit_answer(
            db, user_id, life_sid, AnswerSubmitRequest(answer_text=GOOD_ANSWER)
        )
        ended_life = await interview_service.end_session(db, user_id, life_sid)
        _check("  └ create→start→answer→end 全链路仍正常",
               ended_life["session"]["status"] == "finished"
               and 0 <= ended_life["report"]["total_score"] <= 100)

    await engine.dispose()

    # ------------------------------------------------------------
    # [6] 依赖边界与统一契约
    # ------------------------------------------------------------
    print("\n[6] 依赖边界与统一契约")

    _check("[6.1] question_generator 不反向依赖 interview_service",
           "services.interview_service" not in qg_imports, str(sorted(qg_imports)))
    _check("  └ 不 import main（避免与 Spark 单例成环）",
           "main" not in qg_imports)
    _check("  └ 只依赖 Core 与 resume_scoring 两个 services 模块",
           {m for m in qg_imports if m.startswith("services.")} ==
           {"services.interview_core", "services.resume_scoring",
            "services.resume_scoring.extract_skills"},
           str(sorted(m for m in qg_imports if m.startswith("services."))))

    probe_code = (
        "import sys;"
        "import services.question_generator as qg;"
        "print('PROBE',"
        " 'models' in sys.modules,"
        " 'database' in sys.modules,"
        " 'sqlalchemy' in sys.modules,"
        " 'main' in sys.modules,"
        " 'services.interview_service' in sys.modules)"
    )
    clean_env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
    probe = subprocess.run(
        [sys.executable, "-c", probe_code],
        cwd=str(BACKEND_DIR), env=clean_env, capture_output=True, text=True,
    )
    _check("[6.2] 无 DATABASE_URL 也能导入本模块（不在导入期读库）",
           probe.returncode == 0, probe.stderr.strip()[-300:])

    flags = {}
    if probe.returncode == 0:
        line = next((l for l in probe.stdout.splitlines() if l.startswith("PROBE")), "")
        keys = ("models", "database", "sqlalchemy", "main", "services.interview_service")
        flags = dict(zip(keys, [p == "True" for p in line.split()[1:]]))
    _check("  └ 导入期不拉入 models", flags.get("models") is False)
    _check("  └ 导入期不拉入 database / sqlalchemy",
           flags.get("database") is False and flags.get("sqlalchemy") is False)
    _check("  └ 导入期不拉入 main", flags.get("main") is False)
    _check("  └ 导入期不拉入 interview_service（无循环）",
           flags.get("services.interview_service") is False)

    _check("[6.3] 模块 __all__ 已声明且覆盖核心符号",
           all(n in qg.__all__ for n in (
               "QuestionGenerator", "RuleQuestionGenerator", "AgentQuestionGenerator",
               "get_generator", "generate_questions", "GENERATOR_MODES", "DEFAULT_MODE",
           )), str(qg.__all__))
    _check("  └ 便捷入口 generate_questions 是协程函数",
           inspect.iscoroutinefunction(qg.generate_questions))
    _check("  └ 工厂返回值即便捷入口所用生成器",
           qg.get_generator("rule") is qg.get_generator(qg.DEFAULT_MODE))

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
