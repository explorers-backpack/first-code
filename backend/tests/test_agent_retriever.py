# -*- coding: utf-8 -*-
"""AI 模拟面试 · KnowledgeRetriever 接入 Agent 出题流程 自检

无需 pytest，直接运行：
    python backend/tests/test_agent_retriever.py

不依赖本机 MySQL：DATABASE_URL 指向 SQLite 内存库 + StaticPool。
**不调用真实 LLM**：一律注入 Mock Spark。**不接真实知识库**：用桩 / Mock 检索器。

被测对象
--------
``services.interview_core``（接缝 ``current_topic`` / ``retrieve_knowledge``）
``services.question_generator``（``RuleQuestionGenerator`` / ``AgentQuestionGenerator``）

覆盖范围
--------
1. **接缝契约**：``retrieve_knowledge`` 异步、不收 ``db``、缺省用空实现（恒 ``[]``）
2. **``current_topic`` 推导**：优先级链、确定性、纯函数（不改入参）
3. **agent 模式触发 Retriever**：恰好 1 次，入参为 ``(job, topic, context)``
4. **rule 模式不触发 Retriever**：注入桩后调用次数为 0，且结果与不注入逐字段一致
5. **Retriever 为空仍能生成**：空检索器 / ``chunks=[]`` → ``ok=True`` 且 Prompt 无知识
6. **有知识时确实注入 Prompt**：Mock 知识正文与来源出现在发给模型的 Prompt 里
7. **检索失败静默降级**：检索器抛异常 → 仍 ``ok=True``，只记 warning，不中断出题
8. **边界**：Agent 不 import 检索模块；Rule 生成器方法体内无任何检索调用
"""

import ast
import asyncio
import inspect
import json
import os
import pathlib
import sys

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import Job, User  # noqa: E402
from prompts import render_prompt  # noqa: E402
from schemas.interview import SessionCreateRequest  # noqa: E402
from services import (  # noqa: E402
    interview_agent,
    interview_core,
    interview_service,
    question_generator as qg,
)
from services.knowledge_retriever import (  # noqa: E402
    KnowledgeChunk,
    KnowledgeRetriever,
    MockKnowledgeRetriever,
)

_PASSED = 0
_FAILED = 0

JOB_SKILLS = "Python,MySQL,Redis,Kafka,Docker"

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

# 注入的上下文 / 计划（跳过读库，保证 topic 推导可预期）
CONTEXT = {
    "current_stage": "technical",
    "asked_questions": [],
    "covered_topics": [],
    "weak_topics": [],
    "current_question_no": 1,
    "total_questions": 5,
}
PLAN = {
    "interview_type": "technical",
    "difficulty": "mid",
    "total_questions": 5,
    "target_topics": ["Java", "Spring Boot", "Redis"],
    "priority_topics": ["Redis 持久化", "MySQL 索引"],
    "resume_focus_points": ["订单中台"],
}


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


def _method_node(source: str, class_name: str, method_name: str):
    """取 ``class X: def/async def y`` 的 AST 节点（找不到返回 None）。

    注意 ``async def`` 在 AST 里是 :class:`ast.AsyncFunctionDef`，**不是**
    ``FunctionDef``——只认后者会静默取不到节点。
    """
    fn_types = (ast.FunctionDef, ast.AsyncFunctionDef)
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, fn_types) and item.name == method_name:
                    return item
    return None


def _retrieval_calls_in(node) -> list:
    """方法体内是否出现「检索调用」：``x.retrieve(...)`` 或 ``retrieve_knowledge(...)``。

    只看**可执行代码**（``ast.walk`` 会连 docstring 一起走，但 docstring 是
    ``ast.Constant``，不会产生 ``Name`` / ``Attribute`` 节点，故不会误报）。
    ``node`` 为 ``None``（没取到）时视为「无调用」。
    """
    if node is None:
        return []
    hits = []
    for sub in ast.walk(node):
        if isinstance(sub, ast.Attribute) and sub.attr == "retrieve":
            hits.append(".<retrieve>")
        elif isinstance(sub, ast.Name) and sub.id == "retrieve_knowledge":
            hits.append("retrieve_knowledge")
    return hits


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


class SpyRetriever:
    """记录调用入参的桩检索器（不继承 KnowledgeRetriever，验证鸭子类型即可用）。"""

    def __init__(self, chunks=None, *, raises=None):
        self._chunks = list(chunks or [])
        self._raises = raises
        #: 形如 ``[(job_info, topic, context), ...]``
        self.calls: list = []

    async def retrieve(self, job_info, topic, context):
        self.calls.append((job_info, topic, context))
        if self._raises is not None:
            raise self._raises
        return [dict(c) if isinstance(c, dict) else c for c in self._chunks]


def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed(db: AsyncSession):
    user = User(username="kr_t", email="kr_t@example.com", password_hash="x", role="user")
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


async def _make_session(db: AsyncSession, user_id: int, job_id: int) -> int:
    """建会话并 start（走规则出题，与生产路径一致）。"""
    payload = SessionCreateRequest(job_id=job_id, total_questions=5)
    session_id = (await interview_service.create_session(db, user_id, payload))["session"]["id"]
    await interview_service.start_session(db, user_id, session_id)
    return session_id


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 模拟面试 · KnowledgeRetriever 接入 Agent 出题流程 自检")
    print("=" * 70)

    core_src = inspect.getsource(interview_core)
    qg_src = inspect.getsource(qg)

    # ------------------------------------------------------------
    # [1] 接缝契约
    # ------------------------------------------------------------
    print("\n[1] 接缝契约（Core 是唯一接线点）")
    _check("Core 暴露 retrieve_knowledge", hasattr(interview_core, "retrieve_knowledge"))
    _check("  └ 是协程函数（真实检索要访问外部服务）",
           inspect.iscoroutinefunction(interview_core.retrieve_knowledge))
    _check("  └ 已声明在 __all__",
           {"current_topic", "retrieve_knowledge", "WARNING_KNOWLEDGE_FAILED"}
           <= set(interview_core.__all__))
    _check("Core 暴露 current_topic", callable(interview_core.current_topic))

    sig = inspect.signature(interview_core.retrieve_knowledge)
    _check("retrieve_knowledge 不收 db（检索不耦合业务库）", "db" not in sig.parameters)
    _check("  └ retriever 是 keyword-only 且默认 None（不注入＝不接真实知识库）",
           sig.parameters["retriever"].kind is inspect.Parameter.KEYWORD_ONLY
           and sig.parameters["retriever"].default is None)
    _check("  └ 参数顺序为 (job, topic, context)（与 Retriever 接口对齐）",
           list(sig.parameters)[:3] == ["job", "topic", "context"],
           str(list(sig.parameters)))

    default_chunks = await interview_core.retrieve_knowledge(None, "Redis", None)
    _check("★ 缺省（不注入）＝ 空实现，恒返回 []", default_chunks == [],
           str(default_chunks))

    flow_sig = inspect.signature(interview_core.generate_next_question)
    _check("generate_next_question 新增 keyword-only retriever（默认 None）",
           flow_sig.parameters["retriever"].kind is inspect.Parameter.KEYWORD_ONLY
           and flow_sig.parameters["retriever"].default is None)
    _check("  └ 既有参数（db/session_id/user_id/spark/context/plan）顺序未变",
           list(flow_sig.parameters)[:6] ==
           ["db", "session_id", "user_id", "spark", "context", "plan"],
           str(list(flow_sig.parameters)))

    _check("Core 源码中只有一处真正调用检索（唯一接线点）",
           core_src.count("await retrieve_knowledge(") == 1,
           str(core_src.count("await retrieve_knowledge(")))

    # ------------------------------------------------------------
    # [2] current_topic 推导（纯函数）
    # ------------------------------------------------------------
    print("\n[2] current_topic 推导（纯函数、确定性）")
    ct = interview_core.current_topic
    _check("priority_topics 中未覆盖的第一项优先",
           ct(PLAN, CONTEXT) == "Redis 持久化", ct(PLAN, CONTEXT))
    _check("  └ 已覆盖则跳到下一个未覆盖项",
           ct(PLAN, {**CONTEXT, "covered_topics": ["Redis 持久化"]}) == "MySQL 索引")
    _check("  └ 全部覆盖则回到 priority 首项",
           ct(PLAN, {**CONTEXT, "covered_topics": ["Redis 持久化", "MySQL 索引"]})
           == "Redis 持久化")
    _check("无 priority 时取 target_topics 首项",
           ct({"target_topics": ["Java", "Redis"]}, CONTEXT) == "Java")
    _check("无 plan 时回落到 current_stage",
           ct(None, {"current_stage": "technical"}) == "technical")
    _check("  └ 全空则为空串（不猜）", ct(None, None) == "")
    _check("非 list 形状的字段被安全忽略（不逐字符拆字符串）",
           ct({"priority_topics": "Redis"}, CONTEXT) == "technical",
           ct({"priority_topics": "Redis"}, CONTEXT))
    _check("不改入参（纯函数）",
           (lambda p, c: (ct(p, c), p == PLAN and c == CONTEXT))(dict(PLAN), dict(CONTEXT))
           == ("Redis 持久化", True))
    _check("同输入同输出（确定性）", ct(PLAN, CONTEXT) == ct(PLAN, CONTEXT))
    _check("  └ 也支持 ORM 风格对象（getattr 读取）",
           ct(type("P", (), {"priority_topics": ["K8s"]})(), CONTEXT) == "K8s")

    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        user_id, job_id = await _seed(db)
        sid = await _make_session(db, user_id, job_id)

        # --------------------------------------------------------
        # [3] agent 模式触发 Retriever
        # --------------------------------------------------------
        print("\n[3] agent 模式触发 Retriever（要求 1）")
        spy = SpyRetriever()
        spark = MockSpark(QUESTION_JSON)
        res = await interview_core.generate_next_question(
            db, sid, spark=spark, context=CONTEXT, plan=PLAN, retriever=spy
        )
        _check("agent 出题成功", res["ok"] is True, str(res.get("error")))
        _check("★ 检索器恰好被调用 1 次（每题一次）", len(spy.calls) == 1,
               str(len(spy.calls)))
        if spy.calls:
            got_job, got_topic, got_ctx = spy.calls[0]
            _check("  └ 入参第 1 位是 job（岗位行）",
                   getattr(got_job, "id", None) == job_id,
                   str(getattr(got_job, "job_name", got_job)))
            _check("  └ 入参第 2 位是推导出的当前 topic",
                   got_topic == interview_core.current_topic(PLAN, CONTEXT), str(got_topic))
            _check("  └ 入参第 3 位是面试上下文", got_ctx is CONTEXT)
        _check("  └ 只调用模型一次（检索不额外消耗 LLM）", len(spark.calls) == 1,
               str(len(spark.calls)))
        _check("  └ 结果字段集恒定",
               set(res) == {"ok", "question_no", "question", "question_type", "topic",
                            "difficulty", "expected_points", "reason", "stage",
                            "warnings", "errors", "error"})
        _check("  └ 空检索器不产生 warning（空是正常语义，不是失败）",
               res["warnings"] == [], str(res["warnings"]))

        # 经策略层（AgentQuestionGenerator）也应触发
        spy2 = SpyRetriever()
        gen_res = await qg.generate_questions(
            db, sid, "agent", context=CONTEXT, plan=PLAN,
            retriever=spy2, spark=MockSpark(QUESTION_JSON),
        )
        _check("经 AgentQuestionGenerator 同样触发检索（mode=agent）",
               gen_res["ok"] is True and len(spy2.calls) == 1,
               f"ok={gen_res['ok']} calls={len(spy2.calls)}")
        agent_gen_node = _method_node(qg_src, "AgentQuestionGenerator", "generate")
        rule_gen_node = _method_node(qg_src, "RuleQuestionGenerator", "generate")
        _check("取到两个生成器的 generate AST 节点（否则后续断言会假通过）",
               agent_gen_node is not None and rule_gen_node is not None)
        _check("  └ 生成器不自己调 retrieve（只透传给 Core）",
               not _retrieval_calls_in(agent_gen_node),
               str(_retrieval_calls_in(agent_gen_node)))

        # --------------------------------------------------------
        # [4] rule 模式不触发 Retriever
        # --------------------------------------------------------
        print("\n[4] rule 模式不触发 Retriever（要求 2）")
        spy_rule = SpyRetriever()
        rule_res = await qg.generate_questions(
            db, sid, "rule", retriever=spy_rule, spark=ExplodingSpark()
        )
        _check("rule 出题成功（不依赖 LLM）", rule_res["ok"] is True,
               str(rule_res.get("error")))
        _check("★ 注入检索器后调用次数仍为 0", spy_rule.calls == [], str(spy_rule.calls))

        baseline_rule = await qg.generate_questions(db, sid, "rule")
        _check("  └ 与不注入 retriever 时结果逐字段一致（rule 完全不受影响）",
               rule_res == baseline_rule)
        _check("  └ 整场出题 5 道（规则批量语义未变）", rule_res["count"] == 5,
               str(rule_res["count"]))

        _check("RuleQuestionGenerator.generate 方法体内无任何检索调用",
               _retrieval_calls_in(rule_gen_node) == [],
               str(_retrieval_calls_in(rule_gen_node)))
        _check("  └ 签名里也接收 retriever（接口统一，但接收≠使用）",
               "retriever" in inspect.signature(
                   qg.RuleQuestionGenerator.generate).parameters)

        # --------------------------------------------------------
        # [5] Retriever 为空仍能生成（要求 3）
        # --------------------------------------------------------
        print("\n[5] Retriever 为空仍能生成（要求 3）")
        resume_row = await interview_core.load_resume_row(db, None)
        job_row = await interview_core.load_job_row(db, job_id)
        legacy_prompt = render_prompt(
            "question",
            interview_agent.build_question_variables(CONTEXT, PLAN, resume_row, job_row),
        )

        for label, retriever in (
            ("不注入（None → 空实现）", None),
            ("空基类 KnowledgeRetriever()", KnowledgeRetriever()),
            ("MockKnowledgeRetriever(chunks=[])", MockKnowledgeRetriever(chunks=[])),
            ("桩返回 []", SpyRetriever()),
        ):
            spark_k = MockSpark(QUESTION_JSON)
            r = await interview_core.generate_next_question(
                db, sid, spark=spark_k, context=CONTEXT, plan=PLAN, retriever=retriever
            )
            _check(f"{label} → 仍能生成", r["ok"] is True, str(r.get("error")))
            _check("  └ Prompt 与「无知识」基线逐字节相同",
                   spark_k.calls[0] == legacy_prompt)
            _check("  └ Prompt 不含知识痕迹",
                   "参考知识" not in spark_k.calls[0])
            _check("  └ 结果字段集恒定且 warnings 为空",
                   set(r) == set(res) and r["warnings"] == [])

        # --------------------------------------------------------
        # [6] 有知识时确实注入 Prompt
        # --------------------------------------------------------
        print("\n[6] 有知识时确实注入 Prompt")
        spark_hit = MockSpark(QUESTION_JSON)
        hit = await interview_core.generate_next_question(
            db, sid, spark=spark_hit, context=CONTEXT, plan=PLAN,
            retriever=MockKnowledgeRetriever(),
        )
        _check("有知识时仍能生成", hit["ok"] is True, str(hit.get("error")))
        prompt_hit = spark_hit.calls[0]
        _check("★ Prompt 切到知识模板（含「参考知识：」）", "参考知识：" in prompt_hit)
        _check("  └ 含 Mock 知识正文（RDB/AOF 片段）",
               "RDB" in prompt_hit and "AOF" in prompt_hit)
        _check("  └ 含来源标注", "mock://handbook/redis-persistence" in prompt_hit)
        _check("  └ 与无知识基线 Prompt 不同（确实注入了）", prompt_hit != legacy_prompt)
        _check("  └ 结果字段集不变（调用方无分支）", set(hit) == set(res))

        # --------------------------------------------------------
        # [7] 检索失败静默降级
        # --------------------------------------------------------
        print("\n[7] 检索失败静默降级（可选增强，不中断出题）")
        boom = SpyRetriever(raises=RuntimeError("向量库不可用"))
        spark_boom = MockSpark(QUESTION_JSON)
        boom_res = await interview_core.generate_next_question(
            db, sid, spark=spark_boom, context=CONTEXT, plan=PLAN, retriever=boom
        )
        _check("★ 检索抛异常时出题仍然成功", boom_res["ok"] is True,
               str(boom_res.get("error")))
        _check("  └ 记入 warnings（稳定码 knowledge_retrieval_failed）",
               boom_res["warnings"] == [interview_core.WARNING_KNOWLEDGE_FAILED],
               str(boom_res["warnings"]))
        _check("  └ 不进 errors（不是失败）", boom_res["errors"] == [],
               str(boom_res["errors"]))
        _check("  └ 降级为「无知识」：Prompt 与基线逐字节相同",
               spark_boom.calls[0] == legacy_prompt)
        _check("  └ 检索器确实被调用过（是失败，不是没调）", len(boom.calls) == 1)

        # --------------------------------------------------------
        # [8] 边界
        # --------------------------------------------------------
        print("\n[8] 边界：Agent 只接收、不检索；Service 未改")
        agent_src = pathlib.Path(interview_agent.__file__).read_text(encoding="utf-8")
        _check("InterviewAgent 仍不 import knowledge_retriever",
               not any(n.endswith("knowledge_retriever")
                       for n in _imported_modules(agent_src)))
        _check("  └ Agent 源码里没有任何 retrieve 调用（AST 取证，非子串匹配）",
               not any(isinstance(n, ast.Attribute) and n.attr == "retrieve"
                       for n in ast.walk(ast.parse(agent_src))))
        _check("Service 未接入检索（不 import knowledge_retriever）",
               not any(n.endswith("knowledge_retriever")
                       for n in _imported_modules(
                           inspect.getsource(interview_service))))
        _check("Core 仍不反向 import 策略层 question_generator",
               not any(n.endswith("question_generator")
                       for n in _imported_modules(core_src)))
        _check("   └ Core 也不 import Service（依赖方向单向）",
               not any(n.endswith("interview_service") for n in _imported_modules(core_src)))
        _check("KnowledgeChunk 仍可被 Agent 消费（鸭子类型）",
               await _duck_check())

    await engine.dispose()

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


async def _duck_check() -> bool:
    """Mock 检索器产出的 KnowledgeChunk 能直接交给 Agent 归一（不 import 具体类型）。"""
    chunks = await MockKnowledgeRetriever().retrieve(None, "Redis", None)
    lines = interview_agent.normalize_knowledge_context(chunks)
    return (
        len(lines) == len(chunks)
        and all(isinstance(line, str) and line for line in lines)
        and all(isinstance(c, KnowledgeChunk) for c in chunks)
    )


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
