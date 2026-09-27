# -*- coding: utf-8 -*-
"""AI 模拟面试 · Interview 分层架构自检

无需 pytest，直接运行：
    python backend/tests/test_interview_core.py

不依赖本机 MySQL：DATABASE_URL 指向 SQLite 内存库 + StaticPool。
**不调用真实 LLM**：Agent 接缝一律注入 Mock Spark。

被测对象：
- ``services.interview_core``（② Core：流程控制 / 出题计划 / 评分 / 报告 / 编排接缝）
- ``services.interview_service``（① Service：会话管理 / 数据访问 / 序列化门面）

覆盖范围：
1. 分层结构与职责边界（Service 不再自己实现业务规则）
2. 依赖方向：Core **零数据库 / 零 LLM 耦合**（子进程验证，无 DATABASE_URL 也能导入）
3. 向后兼容：Service 的旧符号与 Core 是**同一个对象**（不是复制实现）
4. 业务规则等价：出题计划 / 评分 / 报告 与重构前口径一致
5. 编排接缝：Context / Plan / Agent / Validator 四个协作者各一条转发路径
6. 调用链证明：替换 Core 的函数后，Service 的行为随之改变（Service → Core 确实生效）
7. **流程控制 `generate_next_question`**：正常生成 / Agent 失败 / Validator 失败 /
   四道闸门（不存在·非本人·已答完·已结束）/ Service 门面（Service → Core，不碰 Agent）
8. 接缝**未被现有业务路径调用**（本次重构不改变现有业务效果）
9. 纯函数 / 无副作用（不改入参、同输入同输出）
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

from fastapi import HTTPException  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import Job, User  # noqa: E402
from schemas.interview import AnswerSubmitRequest, SessionCreateRequest  # noqa: E402
from services import (  # noqa: E402
    interview_agent,
    interview_context,
    interview_core,
    interview_service,
)

_PASSED = 0
_FAILED = 0

# 分层契约：Service 对外必须保留的生命周期函数（api/interview.py 与既有测试都在用）
LIFECYCLE_API = (
    "create_session",
    "start_session",
    "get_session_detail",
    "get_current_question",
    "submit_answer",
    "end_session",
    "get_report",
)

# 本次重构必须搬进 Core 的业务规则（Service 侧只允许保留别名）
MOVED_RULES = (
    "score_answer",
    "build_report",
    "plan_question_types",
    "build_question_text",
    "build_question_plan",
    "build_answer_feedback",
    "job_match_score",
)

# 预留接缝（数字人视频面试阶段才会被调用）
SEAM_FUNCTIONS = (
    "load_context",
    "build_interview_plan",
    "build_interview_plan_for",
    "generate_candidate_question",
    "validate_candidate_question",
)

# 流程入口的恒定结果字段集
RESULT_KEYS = {
    "ok", "question_no", "question", "question_type", "topic", "difficulty",
    "expected_points", "reason", "stage", "warnings", "errors", "error",
}


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def _imported_modules(source: str) -> set:
    """用 AST 提取真正被 import 的模块名。

    刻意**不用子串匹配**：模块 docstring 里会提到其他模块名（如分层示意图），
    子串匹配会产生假阳性。这里只认 ``import X`` / ``from X import Y`` 语句。
    """
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


# ============================================================
# 测试数据与 Mock
# ============================================================
JOB_SKILLS = "Python,MySQL,Redis,Kafka,Docker"

GOOD_ANSWER = (
    "首先，我在上一家公司负责订单中台的重构。当时系统 QPS 只有 800，"
    "我主导把单体服务拆成 8 个微服务，技术栈使用 Python 与 MySQL，"
    "引入 Redis 做缓存、Kafka 做异步解耦。具体来说，我权衡了拆分粒度与运维成本，"
    "最终按业务边界拆分，并对比了同步调用与消息队列两种方案的代价。"
    "结果是 QPS 提升到 5000，响应时间从 1200ms 降到 80ms。因此我认为，"
    "技术选型的核心是匹配当前业务规模。"
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

CONTEXT = {
    "session_id": 1,
    "current_question_no": 3,
    "current_stage": "technical",
    "asked_questions": ["介绍 Spring Boot 自动配置", "Redis 为什么快"],
    "covered_topics": ["Java"],
    "weak_topics": [],
    "follow_up_count": 0,
    "total_questions": 6,
    "max_questions": 6,
}

PLAN = {
    "interview_type": "technical",
    "difficulty": "mid",
    "duration": 30,
    "total_questions": 6,
    "stages": [{"stage": "technical", "weight": 70, "target_questions": 4}],
    "target_topics": ["Redis", "MySQL"],
    "priority_topics": ["Redis"],
    "resume_focus_points": ["订单中台"],
    "source": "rule",
}


class MockSpark:
    """按顺序吐出预设回复的假 Spark；超出预设次数即报错（断言没有多余调用）。"""

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


class _Stub:
    """属性占位对象（模拟 ORM 行 / 计划对象）。"""

    def __init__(self, **kw):
        self.__dict__.update(kw)


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
    user = User(username="core_t", email="core_t@example.com", password_hash="x", role="user")
    other = User(username="core_o", email="core_o@example.com", password_hash="x", role="user")
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


async def _rejected(coro, status: int) -> bool:
    """断言协程抛出指定状态码的 HTTPException。"""
    try:
        await coro
        return False
    except HTTPException as exc:
        return exc.status_code == status


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 模拟面试 · Interview 分层架构自检（Service / Core / Agent / Validator）")
    print("=" * 70)

    # ------------------------------------------------------------
    # [1] 分层结构与职责边界
    # ------------------------------------------------------------
    print("\n[1] 分层结构与职责边界")

    service_src = inspect.getsource(interview_service)
    core_src = inspect.getsource(interview_core)
    service_imports = _imported_modules(service_src)
    core_imports = _imported_modules(core_src)

    for name in MOVED_RULES:
        _check(
            f"Service 不再自行实现 {name}（已迁至 Core）",
            f"def {name}(" not in service_src,
        )
        _check(f"Core 提供 {name}", hasattr(interview_core, name))

    for name in LIFECYCLE_API:
        fn = getattr(interview_service, name, None)
        _check(f"Service 保留生命周期接口 {name}", fn is not None)
        _check(f"  └ {name} 仍是协程（可直接 await）", inspect.iscoroutinefunction(fn))

    _check(
        "Service 是唯一接触数据库的层（_load_* 数据访问留在 Service）",
        all(hasattr(interview_service, n) for n in (
            "_load_session", "_load_questions", "_load_answers", "_load_job", "_load_resume_skills",
        )),
    )
    _check(
        "Core 不承担题目 / 作答的读取（那是 Service 的 _load_questions / _load_answers）",
        not any(hasattr(interview_core, n) for n in ("_load_questions", "_load_answers")),
    )
    _check(
        "Core 只读不写：源码中不出现 db.add( / db.commit(",
        "db.add(" not in core_src and "db.commit(" not in core_src,
    )
    _check(
        "Core 的会话读取仅限编排所需（load_session_row 等只读加载，公开供上层策略复用）",
        all(hasattr(interview_core, n) for n in (
            "load_session_row", "load_job_row", "load_resume_row",
        )),
    )
    _check(
        "Core 不含序列化函数（前端契约留在 Service）",
        not any(hasattr(interview_core, n) for n in (
            "_session_to_dict", "_question_to_dict", "_answer_to_dict", "_report_to_dict",
        )),
    )
    _check(
        "Core 不含 HTTP 路由 / 鉴权依赖",
        not any(hasattr(interview_core, n) for n in ("router", "get_current_user", "get_db")),
    )

    api_src = (BACKEND_DIR / "api" / "interview.py").read_text(encoding="utf-8")
    api_imports = _imported_modules(api_src)
    _check("路由层只依赖 Service，不直接 import Core",
           "services.interview_core" not in api_imports, str(sorted(api_imports)))
    _check(
        "路由层不绕过 Core 直接 import Agent / Validator",
        not any(m in api_imports for m in (
            "services.interview_agent", "services.question_validator",
        )),
        str(sorted(api_imports)),
    )
    _check("路由层确实 import 了 Service",
           "services.interview_service" in api_imports, str(sorted(api_imports)))

    # ------------------------------------------------------------
    # [2] 依赖方向：Core 零数据库 / 零 LLM 耦合
    # ------------------------------------------------------------
    print("\n[2] 依赖方向：Core 零数据库 / 零 LLM 耦合")

    probe_code = (
        "import sys;"
        "import services.interview_core as core;"
        "print('PROBE',"
        " 'models' in sys.modules,"
        " 'database' in sys.modules,"
        " 'sqlalchemy' in sys.modules,"
        " 'prompts.loader' in sys.modules,"
        " 'services.interview_agent' in sys.modules,"
        " 'services.question_validator' in sys.modules,"
        " 'services.interview_service' in sys.modules)"
    )
    # 刻意**不带 DATABASE_URL**：Core 若耦合了 database/models，此处会直接崩。
    clean_env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
    probe = subprocess.run(
        [sys.executable, "-c", probe_code],
        cwd=str(BACKEND_DIR),
        env=clean_env,
        capture_output=True,
        text=True,
    )
    _check("无 DATABASE_URL 也能导入 Core（零 DB 配置耦合）", probe.returncode == 0,
           probe.stderr.strip()[-300:])

    flags = {}
    if probe.returncode == 0:
        line = next((l for l in probe.stdout.splitlines() if l.startswith("PROBE")), "")
        parts = line.split()[1:]
        keys = ("models", "database", "sqlalchemy", "prompts.loader",
                "services.interview_agent", "services.question_validator",
                "services.interview_service")
        flags = dict(zip(keys, [p == "True" for p in parts]))

    _check("导入 Core 不会拉入 models", flags.get("models") is False)
    _check("导入 Core 不会拉入 database", flags.get("database") is False)
    _check("导入 Core 不会拉入 sqlalchemy（AsyncSession 仅用于类型标注）",
           flags.get("sqlalchemy") is False)
    _check("导入 Core 不会拉入 Prompt 加载器", flags.get("prompts.loader") is False)
    _check("导入 Core 不会拉入 Agent（延迟导入）",
           flags.get("services.interview_agent") is False)
    _check("导入 Core 不会拉入 Validator（延迟导入）",
           flags.get("services.question_validator") is False)
    _check("Core 不反向依赖 Service（无循环导入）",
           flags.get("services.interview_service") is False)
    _check("Core 未在模块级 import interview_service",
           "services.interview_service" not in core_imports, str(sorted(core_imports)))

    # ------------------------------------------------------------
    # [3] 向后兼容：旧符号与 Core 是同一对象
    # ------------------------------------------------------------
    print("\n[3] 向后兼容：旧符号与 Core 是同一对象")

    aliases = {
        "score_answer": "score_answer",
        "build_report": "build_report",
        "_plan_question_types": "plan_question_types",
        "_build_question_text": "build_question_text",
        "_build_question_plan": "build_question_plan",
        "_build_feedback": "build_answer_feedback",
    }
    for old, new in aliases.items():
        _check(
            f"interview_service.{old} is interview_core.{new}",
            getattr(interview_service, old) is getattr(interview_core, new),
        )

    for const in ("DIFFICULTY_LABELS", "INTERVIEW_TYPE_LABELS",
                  "ANSWER_DIMENSION_WEIGHTS", "REPORT_DIMENSION_WEIGHTS"):
        _check(
            f"常量 {const} 由 Core 单点定义（同一对象）",
            getattr(interview_service, const) is getattr(interview_core, const),
        )

    # ------------------------------------------------------------
    # [4] 业务规则等价
    # ------------------------------------------------------------
    print("\n[4] 业务规则等价（出题计划 / 评分 / 报告）")

    shape_ok = True
    for total in range(2, 21):
        for itype in ("technical", "behavioral", "comprehensive"):
            types = interview_core.plan_question_types(total, itype)
            if len(types) != total or types[0] != "intro" or types[-1] != "closing":
                shape_ok = False
                break
    _check("plan_question_types：题量 2..20 × 3 种面试类型，长度与首尾题型均正确", shape_ok)
    _check("plan_question_types(total<=1) 退化为单道 intro（既有既定行为，未改动）",
           interview_core.plan_question_types(1, "technical") == ["intro"])
    _check("未知 interview_type 回落到 comprehensive 权重",
           interview_core.plan_question_types(7, "unknown") ==
           interview_core.plan_question_types(7, "comprehensive"))

    q_stub = {"question": "介绍一下 Redis", "expected_points": ["Redis", "场景", "问题", "解决"]}
    s1 = interview_core.score_answer(q_stub, GOOD_ANSWER)
    s2 = interview_core.score_answer(q_stub, GOOD_ANSWER)
    _check("score_answer 确定性：同输入同输出", s1 == s2)
    _check("score_answer 返回六个键",
           set(s1) == {"score", "technical_score", "logic_score",
                       "expression_score", "adaptability_score", "feedback"},
           str(sorted(s1)))
    _check("score_answer 分数落在 0-100",
           all(0 <= s1[k] <= 100 for k in s1 if k.endswith("_score") or k == "score"))
    _check("空回答得分低于优质回答",
           interview_core.score_answer(q_stub, "")["score"] < s1["score"])

    plan = interview_core.build_question_plan(
        _Stub(total_questions=6, interview_type="technical", difficulty="mid"),
        _Stub(skills="Python,MySQL,Redis", job_name="后端开发工程师"),
        ["Docker"],
    )
    _check("build_question_plan 产出题量与计划一致", len(plan) == 6)
    _check("build_question_plan 每项六字段齐全",
           all(set(item) == {"question_no", "question", "question_type", "topic",
                             "difficulty", "expected_points"} for item in plan))
    _check("build_question_plan 题号连续且从 1 开始",
           [item["question_no"] for item in plan] == list(range(1, 7)))
    _check("build_question_plan 无岗位时仍可产出（技能退化为空）",
           len(interview_core.build_question_plan(
               _Stub(total_questions=3, interview_type="comprehensive", difficulty="junior"),
               None, [],
           )) == 3)
    _check("build_question_plan 确定性：同输入同输出",
           plan == interview_core.build_question_plan(
               _Stub(total_questions=6, interview_type="technical", difficulty="mid"),
               _Stub(skills="Python,MySQL,Redis", job_name="后端开发工程师"),
               ["Docker"],
           ))

    _check("job_match_score：未指定岗位返回 None（不猜）",
           interview_core.job_match_score(None, ["Redis"], []) is None)
    _check("job_match_score：岗位无技能要求返回 None",
           interview_core.job_match_score(_Stub(skills=""), [], []) is None)
    _check("job_match_score：简历技能命中 2/5 得 40 分",
           interview_core.job_match_score(
               _Stub(skills=JOB_SKILLS), [], ["Python", "Redis"]) == 40)

    # ------------------------------------------------------------
    # [5] 编排接缝：Context / Plan / Agent / Validator
    # ------------------------------------------------------------
    print("\n[5] 编排接缝：Context / Plan / Agent / Validator")

    for name in SEAM_FUNCTIONS:
        _check(f"Core 暴露接缝 {name}", hasattr(interview_core, name))
    _check("load_context 是协程（需读库）", inspect.iscoroutinefunction(interview_core.load_context))
    _check("build_interview_plan_for 是协程（需读库）",
           inspect.iscoroutinefunction(interview_core.build_interview_plan_for))
    _check("generate_candidate_question 是协程（需调 LLM）",
           inspect.iscoroutinefunction(interview_core.generate_candidate_question))
    _check("build_interview_plan 是纯同步函数（不读库）",
           not inspect.iscoroutinefunction(interview_core.build_interview_plan))
    _check("validate_candidate_question 是纯同步函数",
           not inspect.iscoroutinefunction(interview_core.validate_candidate_question))
    _check("generate_candidate_question 不接收 db（与 Agent 契约一致）",
           "db" not in inspect.signature(interview_core.generate_candidate_question).parameters)

    # --- Plan 接缝 ---
    built_plan = interview_core.build_interview_plan(
        interview_type="technical", difficulty="senior", duration=45,
        job=_Stub(job_name="后端开发工程师", skills="Java,Redis", duty="服务开发"),
        resume="项目经历\n订单中台重构\n",
    )
    _check("build_interview_plan 委托 Planner 并返回 stages",
           isinstance(built_plan.get("stages"), list) and bool(built_plan["stages"]))
    _check("build_interview_plan 透传 difficulty=senior", built_plan["difficulty"] == "senior")
    _check("build_interview_plan 标记来源（rule）", built_plan.get("source") == "rule")
    _check("build_interview_plan 产出 priority_topics / resume_focus_points",
           "priority_topics" in built_plan and "resume_focus_points" in built_plan)
    _check("build_interview_plan 不复制默认值：省略参数即用 Planner 默认值",
           interview_core.build_interview_plan()["interview_type"] == "comprehensive")

    # --- Validator 接缝 ---
    vr_ok = interview_core.validate_candidate_question(
        {"question": "解释一下 Redis 的持久化机制", "topic": "Redis", "difficulty": "mid"},
        CONTEXT, PLAN,
    )
    _check("validate_candidate_question 合法输入 valid=True", vr_ok.valid is True, str(vr_ok.errors))
    _check("validate_candidate_question 产出 normalized_question（六字段）",
           set(vr_ok.normalized_question) == {
               "question", "question_type", "topic", "difficulty",
               "expected_points", "reason"},
           str(vr_ok.normalized_question))

    vr_bad = interview_core.validate_candidate_question(
        {"question": "解释一下 Redis 的持久化机制", "topic": "Redis", "difficulty": "god_mode"},
        CONTEXT, PLAN,
    )
    _check("validate_candidate_question 非法 difficulty → invalid_difficulty",
           vr_bad.valid is False and "invalid_difficulty" in vr_bad.errors, str(vr_bad.errors))

    vr_fill = interview_core.validate_candidate_question(
        {"question": "解释一下 Redis 的持久化机制", "topic": "Redis"},
        CONTEXT, {"difficulty": "senior"},
    )
    _check("validate_candidate_question difficulty 缺失时回落到 plan.difficulty",
           vr_fill.normalized_question["difficulty"] == "senior",
           str(vr_fill.normalized_question))

    vr_dup = interview_core.validate_candidate_question(
        {"question": "Redis 为什么快", "topic": "Redis", "difficulty": "mid"}, CONTEXT, PLAN,
    )
    _check("validate_candidate_question 与历史完全相同 → duplicate_question",
           vr_dup.valid is False and "duplicate_question" in vr_dup.errors, str(vr_dup.errors))

    # --- Agent 接缝 ---
    spark = MockSpark(QUESTION_JSON)
    gen = await interview_core.generate_candidate_question(CONTEXT, PLAN, None, None, spark=spark)
    _check("generate_candidate_question 委托 Agent 成功出题", gen["ok"] is True, str(gen.get("error")))
    _check("  └ 返回结构化问题（question / topic / difficulty）",
           bool(gen["question"]) and gen["topic"] == "Redis" and gen["difficulty"] == "mid")
    _check("  └ 确实调用了注入的 Spark", len(spark.calls) == 1)
    _check("  └ Prompt 已渲染（无残留 {{占位符}}）", "{{" not in spark.calls[0])

    spark_bad = MockSpark("这不是 JSON", "这也不是 JSON")
    gen_bad = await interview_core.generate_candidate_question(CONTEXT, PLAN, None, None, spark=spark_bad)
    _check("Agent 失败路径不伪造问题（question 恒为空）",
           gen_bad["ok"] is False and gen_bad["question"] == "")
    _check("  └ 非法 JSON 触发 1 次修复（共 2 次调用）", len(spark_bad.calls) == 2)

    # ------------------------------------------------------------
    # [6] 调用链证明：Service 确实经由 Core
    # ------------------------------------------------------------
    print("\n[6] 调用链证明：Service 确实经由 Core")

    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        user_id, other_user_id, job_id = await _seed(db)

        created = await interview_service.create_session(
            db, user_id, SessionCreateRequest(job_id=job_id, total_questions=4)
        )
        sid = created["session"]["id"]

        plan_spy = _Spy(interview_core.build_question_plan)
        score_spy = _Spy(interview_core.score_answer)
        report_spy = _Spy(interview_core.build_report)
        interview_core.build_question_plan = plan_spy
        interview_core.score_answer = score_spy
        interview_core.build_report = report_spy
        try:
            await interview_service.start_session(db, user_id, sid)
            _check("start_session → Core.build_question_plan 被调用", plan_spy.calls == 1,
                   f"calls={plan_spy.calls}")

            await interview_service.submit_answer(
                db, user_id, sid, AnswerSubmitRequest(answer_text=GOOD_ANSWER)
            )
            _check("submit_answer → Core.score_answer 被调用", score_spy.calls == 1,
                   f"calls={score_spy.calls}")

            await interview_service.end_session(db, user_id, sid)
            _check("end_session → Core.build_report 被调用", report_spy.calls == 1,
                   f"calls={report_spy.calls}")
        finally:
            interview_core.build_question_plan = plan_spy.original
            interview_core.score_answer = score_spy.original
            interview_core.build_report = report_spy.original

        # 端到端仍然可用（搬迁后功能不回退）
        detail = await interview_service.get_session_detail(db, user_id, sid)
        _check("端到端：4 道题全部生成", len(detail["questions"]) == 4, str(len(detail["questions"])))
        _check("端到端：已落 1 条作答且带分数",
               detail["answered_count"] == 1 and detail["answers"][0]["score"] is not None)
        report = await interview_service.get_report(db, user_id, sid)
        _check("端到端：报告已生成且总分在 0-100",
               0 <= report["total_score"] <= 100, str(report["total_score"]))

        # 替换 Core 实现 → Service 行为随之改变（证明不存在第二套逻辑）
        original_score = interview_core.score_answer
        interview_core.score_answer = lambda question, text: {
            "score": 7, "technical_score": 7, "logic_score": 7,
            "expression_score": 7, "adaptability_score": 7, "feedback": "SENTINEL",
        }
        try:
            sid2 = (await interview_service.create_session(
                db, user_id, SessionCreateRequest(total_questions=3)
            ))["session"]["id"]
            await interview_service.start_session(db, user_id, sid2)
            ans = await interview_service.submit_answer(
                db, user_id, sid2, AnswerSubmitRequest(answer_text="任意回答")
            )
            _check("替换 Core.score_answer 后落库分数变为 7（Service 无第二套评分逻辑）",
                   ans["answer"]["score"] == 7 and ans["answer"]["feedback"] == "SENTINEL",
                   str(ans["answer"]["score"]))
        finally:
            interview_core.score_answer = original_score

        # 未作答时报告必须仍按既有口径拒绝
        sid3 = (await interview_service.create_session(
            db, user_id, SessionCreateRequest(total_questions=3)
        ))["session"]["id"]
        await interview_service.start_session(db, user_id, sid3)
        try:
            await interview_service.end_session(db, user_id, sid3)
            rejected = False
        except HTTPException as exc:
            rejected = exc.status_code == 400 and "尚未提交任何回答" in exc.detail
        _check("无作答时 end_session 仍 400（Core 抛出的 HTTPException 原样透传）", rejected)

        # ------------------------------------------------------------
        # [7] 流程控制：generate_next_question（文字 / 数字人共用核心）
        # ------------------------------------------------------------
        print("\n[7] 流程控制：generate_next_question（session→context→plan→Agent→Validator）")

        _check("Core 暴露流程入口 generate_next_question",
               hasattr(interview_core, "generate_next_question"))
        _check("  └ 是协程函数",
               inspect.iscoroutinefunction(interview_core.generate_next_question))
        _check("  └ 已声明在 __all__", "generate_next_question" in interview_core.__all__)
        _check("  └ 结果字段集常量与实现一致",
               set(interview_core.QUESTION_RESULT_FIELDS) == RESULT_KEYS,
               str(interview_core.QUESTION_RESULT_FIELDS))

        flow_sid = (await interview_service.create_session(
            db, user_id, SessionCreateRequest(job_id=job_id, total_questions=5)
        ))["session"]["id"]
        await interview_service.start_session(db, user_id, flow_sid)

        # ---- 9.1 正常生成问题 ----
        spark_ok = MockSpark(QUESTION_JSON)
        q_before = len((await interview_service.get_session_detail(db, user_id, flow_sid))["questions"])
        ok_res = await interview_core.generate_next_question(db, flow_sid, spark=spark_ok)
        q_after = len((await interview_service.get_session_detail(db, user_id, flow_sid))["questions"])

        _check("[7.1] 正常生成问题：ok=True", ok_res["ok"] is True, str(ok_res.get("error")))
        _check("  └ 返回 question / topic / difficulty / expected_points",
               bool(ok_res["question"]) and ok_res["topic"] == "Redis"
               and ok_res["difficulty"] == "mid"
               and isinstance(ok_res["expected_points"], list) and bool(ok_res["expected_points"]))
        _check("  └ question_no 取状态机当前题号", ok_res["question_no"] == 1, str(ok_res["question_no"]))
        _check("  └ stage 来自 Context", ok_res["stage"] == "introduction", str(ok_res["stage"]))
        _check("  └ 结果字段集恒定", set(ok_res) == RESULT_KEYS, str(sorted(set(ok_res) ^ RESULT_KEYS)))
        _check("  └ 确实调用了注入的 Spark（恰好 1 次）", len(spark_ok.calls) == 1)
        _check("  └ 不落库：题目数量不变", q_before == q_after, f"{q_before} -> {q_after}")
        ctx_row = await interview_context.get_context(db, flow_sid)
        _check("  └ 已幂等确保 Context 存在", ctx_row["session_id"] == flow_sid)
        _check("  └ 返回的是 Validator 标准化后的字段（六字段齐备）",
               all(k in ok_res for k in ("question", "question_type", "topic",
                                         "difficulty", "expected_points", "reason")))

        # ---- 9.2 Agent 失败 ----
        spark_bad = MockSpark("这不是 JSON", "这也不是 JSON")
        agent_res = await interview_core.generate_next_question(db, flow_sid, spark=spark_bad)
        _check("[7.2] Agent 失败：ok=False", agent_res["ok"] is False)
        _check("  └ question 恒为空（不伪造问题）", agent_res["question"] == "")
        _check("  └ errors 含 agent_failed", "agent_failed" in agent_res["errors"],
               str(agent_res["errors"]))
        _check("  └ 非法 JSON 触发 1 次修复（共 2 次调用）", len(spark_bad.calls) == 2)
        _check("  └ 字段集与成功时一致", set(agent_res) == RESULT_KEYS)
        _check("  └ 仍带 question_no / stage（便于前端定位）",
               agent_res["question_no"] == 1 and agent_res["stage"] == "introduction")

        # ---- 9.3 Validator 失败 ----
        # Agent 自身也带内联校验（rule-4 待移除），真实 LLM 路径下 Validator 很难失败；
        # 故此处用**桩 Agent** 构造「Agent 通过、Validator 拒绝」的输入，
        # 专门验证 Core 的 Validator 分支——这正是把校验拆成独立一层的意义。
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
            invalid_res = await interview_core.generate_next_question(db, flow_sid, spark=MockSpark())
        finally:
            interview_agent.generate_question = original_agent_generate

        _check("[7.3] Validator 失败：ok=False", invalid_res["ok"] is False)
        _check("  └ question 恒为空（不放行未通过校验的问题）", invalid_res["question"] == "")
        _check("  └ errors 含 validation_failed", "validation_failed" in invalid_res["errors"],
               str(invalid_res["errors"]))
        _check("  └ errors 保留 Validator 原始错误码 invalid_difficulty",
               "invalid_difficulty" in invalid_res["errors"], str(invalid_res["errors"]))
        _check("  └ error 文案可读", bool(invalid_res["error"]))
        _check("  └ 字段集与成功时一致", set(invalid_res) == RESULT_KEYS)

        # ---- 9.4 四道闸门 ----
        missing = await interview_core.generate_next_question(db, 999999, spark=MockSpark())
        _check("[7.4] 会话不存在：session_not_found",
               missing["ok"] is False and "session_not_found" in missing["errors"],
               str(missing["errors"]))

        foreign = await interview_core.generate_next_question(
            db, flow_sid, user_id=other_user_id, spark=MockSpark()
        )
        _check("  └ 归属不符按不存在处理（不泄露他人会话）",
               foreign["ok"] is False and "session_not_found" in foreign["errors"],
               str(foreign["errors"]))

        one_sid = (await interview_service.create_session(
            db, user_id, SessionCreateRequest(total_questions=1)
        ))["session"]["id"]
        await interview_service.start_session(db, user_id, one_sid)
        await interview_service.submit_answer(
            db, user_id, one_sid, AnswerSubmitRequest(answer_text=GOOD_ANSWER)
        )
        done = await interview_core.generate_next_question(db, one_sid, spark=MockSpark())
        _check("  └ 全部答完：all_answered",
               done["ok"] is False and "all_answered" in done["errors"], str(done["errors"]))

        await interview_service.end_session(db, user_id, one_sid)
        ended = await interview_core.generate_next_question(db, one_sid, spark=MockSpark())
        _check("  └ 已结束：session_finished",
               ended["ok"] is False and "session_finished" in ended["errors"],
               str(ended["errors"]))

        # ---- 9.5 Service 门面：Service → Core，Service 不碰 Agent ----
        via_service = await interview_service.generate_next_question(
            db, user_id, flow_sid, spark=MockSpark(QUESTION_JSON)
        )
        _check("[7.5] Service.generate_next_question 可用（Service → Core）",
               via_service["ok"] is True, str(via_service.get("error")))
        _check("  └ 他人会话被 Service 拦下（404）",
               await _rejected(
                   interview_service.generate_next_question(db, other_user_id, flow_sid), 404
               ))

        original_core_flow = interview_core.generate_next_question
        sentinel = {"SENTINEL": True}
        seen = {}

        async def _fake_core_flow(db_, sid, *, user_id=None, spark=None,
                                  retriever=None, use_rag=False,
                                  retriever_kwargs=None):
            seen.update(user_id=user_id, spark=spark, retriever=retriever,
                        use_rag=use_rag, retriever_kwargs=retriever_kwargs)
            return sentinel

        interview_core.generate_next_question = _fake_core_flow
        try:
            observed = await interview_service.generate_next_question(db, user_id, flow_sid)
        finally:
            interview_core.generate_next_question = original_core_flow
        _check("  └ 替换 Core 实现后 Service 返回值随之改变（确实经由 Core）",
               observed is sentinel)
        _check("  └ Service 把 retriever / use_rag / retriever_kwargs 原样透传"
               "（默认都不开 → 不接 RAG）",
               seen["retriever"] is None and seen["use_rag"] is False
               and seen["retriever_kwargs"] is None, str(seen))

        probe = object()
        interview_core.generate_next_question = _fake_core_flow
        try:
            await interview_service.generate_next_question(
                db, user_id, flow_sid, retriever=probe, use_rag=True
            )
        finally:
            interview_core.generate_next_question = original_core_flow
        _check("  └ 显式传入时也透传（Service 只做透传，不自己组装检索器）",
               seen["retriever"] is probe and seen["use_rag"] is True, str(seen))

        tuned = {"top_k": 3, "min_score": 0.25}
        interview_core.generate_next_question = _fake_core_flow
        try:
            await interview_service.generate_next_question(
                db, user_id, flow_sid, use_rag=True, retriever_kwargs=tuned
            )
        finally:
            interview_core.generate_next_question = original_core_flow
        _check("  └ retriever_kwargs 原样透传（Service 不解释、不裁剪、不白名单）",
               seen["retriever_kwargs"] is tuned and seen["use_rag"] is True, str(seen))

        _check("  └ Service 未直接调用 Agent 的 generate_question",
               "generate_question" not in service_src)
        _check("  └ Core 编排里 Agent 只被调用一次（不重复生成）",
               core_src.count("generate_candidate_question(") == 2,
               str(core_src.count("generate_candidate_question(")))

    await engine.dispose()

    # ------------------------------------------------------------
    # [8] 接缝未被现有业务路径调用
    # ------------------------------------------------------------
    print("\n[8] 接缝未被现有业务路径调用（不改变现有业务效果）")

    for fn_name in LIFECYCLE_API:
        fn_src = inspect.getsource(getattr(interview_service, fn_name))
        _check(
            f"{fn_name} 未调用 LLM 接缝（generate_candidate_question）",
            "generate_candidate_question" not in fn_src,
        )
    _check(
        "Service 源码中不出现 Agent / Validator / Planner / Context 的接缝调用",
        not any(k in service_src for k in (
            "generate_candidate_question", "validate_candidate_question",
            "build_interview_plan", "load_context",
        )),
    )
    _check(
        "Service 不直接 import Agent / Validator / Planner / Context",
        not any(m in service_imports for m in (
            "services.interview_agent", "services.question_validator",
            "services.interview_planner", "services.interview_context",
        )),
        str(sorted(service_imports)),
    )
    _check("Service 只依赖 Core 一个协作模块",
           "services.interview_core" in service_imports, str(sorted(service_imports)))

    # ------------------------------------------------------------
    # [9] 纯函数 / 无副作用
    # ------------------------------------------------------------
    print("\n[9] 纯函数 / 无副作用")

    q_in = {"question": "介绍一下 Redis", "expected_points": ["Redis", "场景"]}
    q_snapshot = json.loads(json.dumps(q_in, ensure_ascii=False))
    interview_core.score_answer(q_in, GOOD_ANSWER)
    _check("score_answer 不修改入参 question", q_in == q_snapshot)

    session_stub = _Stub(total_questions=5, interview_type="technical", difficulty="mid")
    job_stub = _Stub(skills="Python,MySQL", job_name="后端开发工程师")
    skills_in = ["Redis"]
    interview_core.build_question_plan(session_stub, job_stub, skills_in)
    _check("build_question_plan 不修改 resume_skills 入参", skills_in == ["Redis"])
    _check("build_question_plan 不修改 session / job 入参",
           session_stub.total_questions == 5 and job_stub.skills == "Python,MySQL")

    data_in = {"question": "解释 Redis 持久化", "topic": "Redis", "difficulty": "mid"}
    data_snapshot = json.loads(json.dumps(data_in, ensure_ascii=False))
    interview_core.validate_candidate_question(data_in, CONTEXT, PLAN)
    _check("validate_candidate_question 不修改入参 question_data", data_in == data_snapshot)

    ctx_snapshot = json.loads(json.dumps(CONTEXT, ensure_ascii=False))
    interview_core.validate_candidate_question(
        {"question": "Redis 为什么快", "topic": "Redis", "difficulty": "mid"}, CONTEXT, PLAN
    )
    _check("validate_candidate_question 不修改入参 context", CONTEXT == ctx_snapshot)

    _check("模块 __all__ 已声明且覆盖接缝",
           all(n in interview_core.__all__ for n in SEAM_FUNCTIONS),
           str(interview_core.__all__))
    _check("Core 不导出数据库 / HTTP 依赖",
           not any(n in interview_core.__all__ for n in ("AsyncSession", "HTTPException")))

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
