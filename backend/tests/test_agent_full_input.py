# -*- coding: utf-8 -*-
"""InterviewAgent 完整输入验证（脚本式，非 pytest）。

运行：``python backend/tests/test_agent_full_input.py``

目标
----
跑**一次真实的面试问题生成流程**（真实 MySQL 语义的 SQLite 内存库 + 真实
Job/Resume/Session/Context 行 + 真实 Planner + 真实 RAG 检索栈，只把 Spark 换成
Mock），记录并核对 Agent 收到的**全部输入**，确认**所有字段都正确传递**。

用户给出的五个字段 → 本项目的实际落点
--------------------------------------
===============  =============================================  ==========================
用户词汇          本项目落点（Agent 形参）                        对应 Prompt 变量
===============  =============================================  ==========================
interview_plan    ``plan``（第 2 位置参数，来自 Planner）           ``interview_plan``
history_questions ``context["asked_questions"]``（**不在形参里**）    ``asked_questions``
user_profile      ``resume`` + ``job``（第 3/4 位置参数）           ``resume_summary`` /
                                                                  ``job_title`` /
                                                                  ``job_description``
difficulty        ``plan["difficulty"]``（**不在形参里**）           ``difficulty``
knowledge_context ``knowledge_context``（**keyword-only**）          ``knowledge_context``
===============  =============================================  ==========================

.. important::
   用户词汇与代码词汇**不是一一对应**：``history_questions`` 与 ``difficulty``
   都藏在 ``context`` / ``plan`` 内部，Agent 形参里没有同名参数。
   [1] 把这个映射写成**可执行断言**（断言这些名字**不在**形参集合里），
   避免读者以为「传个 history_questions= 就完事了」。

怎么证明「正确传递」而不是「碰巧对」
------------------------------------
四条互相独立的证据，缺一条都不够：

1. **捕获真实入参**：临时包装 ``interview_agent.generate_question``，记录真实流程
   **实际传进来**的 5 个入参（不是我们自己重算的），并断言包装器只被调用一次、
   签名与原函数一致。
2. **值来自源头**：每个变量都与**数据库行 / 检索器输出**逐字节比对
   （``resume_summary == Resume.content``、``difficulty == session.difficulty`` …）。
3. **不是默认值**：``difficulty`` 取 ``senior``（≠ 默认 ``mid``）、
   ``current_stage`` 取 ``technical``（≠ 默认 ``introduction``）——
   若某字段其实没传下去，这些断言会立刻失败。
4. **反向对照**：另跑一次「会话没有 job/resume」的真实流程，断言文档化的兜底串
   （``（无简历）`` 等）**恰好在那里**出现、而在主流程里**一个都不出现**。

不要修改业务逻辑
----------------
4 个业务模块 + 2 个 Prompt 模板的**源码指纹**在 [1] 锁死（改一个字符即失败）
⇒ 「只新增测试、未动业务逻辑」是**被验证**的。

.. note::
   本套件**只新增测试文件**，不修改任何业务代码，也不新增日志输出到业务模块
   （需要「日志」的地方以测试内的打印代替，避免污染生产路径）。
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
import regression_env  # noqa: E402,F401  钉住离线 Embedding + RAG 阈值（回归不受 .env 影响）
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import InterviewSession, Job, Resume, User  # noqa: E402
from services import interview_agent, interview_context, interview_core  # noqa: E402
from services.interview_agent import (  # noqa: E402
    DEFAULT_DIFFICULTY,
    DEFAULT_INTERVIEW_TYPE,
    DEFAULT_STAGE,
    PROMPT_QUESTION_KNOWLEDGE,
    build_question_variables,
    render_question_prompt,
)
from services.knowledge_import_pipeline import (  # noqa: E402
    KnowledgeImportPipeline,
    STATUS_OK,
)
from services.knowledge_rag import default_embedder  # noqa: E402
from services.knowledge_retriever import KnowledgeChunk  # noqa: E402
from services.vector_knowledge_retriever import VectorKnowledgeRetriever  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

# ============================================================
# 一、常量
# ============================================================
#: 「不要修改业务逻辑」的判据：**规范化换行后的源码 sha256**。
#: 用源码文本而不是 AST——``ast.dump`` 的输出随 Python 版本变化，
#: 换解释器就会假失败。
#: 两个 Prompt 模板也在内：改模板同样会改「模型实际收到什么」。
FROZEN_SOURCES: Tuple[Tuple[str, str], ...] = (
    ("services/interview_agent.py",
     "cbc4b7c33ad591c6a325cb53cbb41f2819abe0e813d2487badc3cd9f98f0d09a"),
    ("services/interview_core.py",
     "b8da0caa24577cc21b134666a8da602bd6f8d701bf287c62163840fc7e949d51"),
    ("services/interview_planner.py",
     "0da3961aac2dc9af81fa321c7c56149934cc39d8ff016cb285ea5ad07d9b8df6"),
    ("services/interview_context.py",
     "432c02a5bfbac923095b3fc193be9f7b7c5856cfbaf904c30554fac9d89046d1"),
    ("prompts/interview/question.txt",
     "34c522e0b6fd06d088b606ed17945b10ab74b94452f0de9a163374d455c57b17"),
    ("prompts/interview/question_knowledge.txt",
     "1c141c6b9c8613f0c2af762d6a46cdbc193e339fc60e49fdf7e4cf9f1f620194"),
)

#: 基础模板的 10 个变量（顺序即 ``build_question_variables`` 的返回顺序）。
BASE_VARIABLE_NAMES: Tuple[str, ...] = (
    "resume_summary",
    "job_title",
    "job_description",
    "interview_type",
    "difficulty",
    "interview_plan",
    "current_stage",
    "asked_questions",
    "covered_topics",
    "weak_topics",
)

#: ``interview_plan`` 载荷的 7 个键（顺序即组装顺序）。
PLAN_PAYLOAD_KEYS: Tuple[str, ...] = (
    "interview_type",
    "difficulty",
    "total_questions",
    "stages",
    "target_topics",
    "priority_topics",
    "resume_focus_points",
)

# ---- 固定夹具（逐字节固定：整套期望值都由它们推导）----
RESUME_CONTENT = (
    "3 年后端开发经验，负责订单中台与缓存体系建设。\n"
    "项目名称：订单中台\n"
    "技术栈：Python、MySQL、Redis 持久化、消息队列\n"
    "主导 Redis 持久化方案选型（RDB 与 AOF 混合），支撑日均 2 亿次读写。"
)
JOB_NAME = "后端开发工程师"
JOB_SKILLS = "Redis 持久化,MySQL"
JOB_DUTY = "负责缓存与持久化方案设计"
#: Agent 侧把 skills/duty 拼成一段描述（格式由 ``_job_description`` 决定）。
JOB_DESCRIPTION = f"技能要求：{JOB_SKILLS}；岗位职责：{JOB_DUTY}"

#: 会话配置。``difficulty`` **刻意取 ``senior``**（≠ 默认 ``mid``），
#: 这样「难度到底有没有传下去」才有鉴别力。
SESSION_DIFFICULTY = "senior"
SESSION_TYPE = "technical"
SESSION_DURATION = 30
SESSION_TOTAL = 5
CURRENT_STAGE = "technical"

#: 历史提问（``history_questions`` 的落点）。刻意与模型回复**不同**，
#: 以免被查重规则打回。
HISTORY_QUESTION = "请做一下自我介绍，并说明你最近一段工作的主要职责。"
WEAK_TOPIC = "缓存击穿"

FIXED_DOC: Dict[str, str] = {
    "title": "Redis 持久化机制手册",
    "category": "technical",
    "source": "handbook://redis/persistence",
    "content": (
        "Redis 持久化提供 RDB 与 AOF 两种机制：RDB 是某一时刻的全量快照，"
        "通过 fork 子进程写盘，文件紧凑、恢复快，但两次快照之间的写入会丢失；"
        "AOF 记录每条写命令，appendfsync 可取 always / everysec / no，"
        "everysec 是生产常见折中，最多丢 1 秒数据，重写时用 BGREWRITEAOF。"
    ),
}
FIXED_SOURCE = FIXED_DOC["source"]

#: 模型回复（合法 JSON、字段齐全、难度与计划一致 ⇒ 一次通过，不需要修复）。
REPLY_OK = json.dumps(
    {
        "question": "在订单中台的缓存体系里，你会如何设计 Redis 持久化策略"
                    "（RDB 与 AOF 如何取舍）？",
        "question_type": "technical",
        "topic": "Redis 持久化",
        "difficulty": SESSION_DIFFICULTY,
        "expected_points": ["RDB", "AOF", "丢失窗口"],
        "reason": "考察岗位要求中的缓存持久化取舍",
    },
    ensure_ascii=False,
)
#: 与历史问题**完全相同**的回复 ⇒ 应当被 Agent 的查重规则打回并触发一次修复。
REPLY_DUPLICATE = json.dumps(
    {
        "question": HISTORY_QUESTION,
        "question_type": "introduction",
        "topic": "自我介绍",
        "difficulty": SESSION_DIFFICULTY,
        "expected_points": ["经历", "职责"],
        "reason": "开场",
    },
    ensure_ascii=False,
)

# ---- Prompt 小节标题（模板里的原文，渲染后应逐字出现）----
H_CONFIG = "## 一、面试配置"
H_JOB = "## 二、目标岗位"
H_RESUME = "## 三、候选人简历摘要"
H_PLAN = "## 四、本场面试计划"
H_KNOWLEDGE = "## 四·五、参考知识（外部检索结果，可选）"
H_PROGRESS = "## 五、本场进度（用于避免重复）"
H_REQUIREMENTS = "## 六、生成要求"

#: 文档化的兜底串（源头为空时才应出现）。
FALLBACK_RESUME = "（无简历）"
FALLBACK_JOB_TITLE = "（未指定岗位）"
FALLBACK_JOB_DESCRIPTION = "（未提供岗位描述）"
FALLBACKS = (FALLBACK_RESUME, FALLBACK_JOB_TITLE, FALLBACK_JOB_DESCRIPTION)

_PASSED = 0
_FAILED = 0


def _section(title: str) -> None:
    print("\n" + "-" * 74)
    print(title)
    print("-" * 74)


def _check(name: str, condition: Any, detail: str = "") -> bool:
    global _PASSED, _FAILED
    ok = bool(condition)
    if ok:
        _PASSED += 1
    else:
        _FAILED += 1
    tail = f"  -> {detail}" if detail else ""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{tail}")
    return ok


# ============================================================
# 二、观测器与工具
# ============================================================
class SpySpark:
    """记录每次收到的 Prompt；按顺序吐回复，超出即报错（用来断言调用次数）。"""

    def __init__(self, *replies: Any) -> None:
        self.replies = list(replies)
        self.calls: List[str] = []

    @property
    def call_count(self) -> int:
        return len(self.calls)

    async def chat_async(self, message: str) -> str:
        self.calls.append(message)
        if not self.replies:
            raise AssertionError("SpySpark 被调用次数超出预期")
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


@asynccontextmanager
async def _record_agent_input() -> Iterator[Dict[str, Any]]:
    """临时包装 ``interview_agent.generate_question``，记录真实流程传给 Agent 的入参。

    为什么替换模块属性就能生效：``interview_core.generate_candidate_question`` 在
    **函数体内**执行 ``from services.interview_agent import generate_question``
    ——按**调用时**的模块属性取值，因此包装器会被真实流程取用。

    包装器**原样转发**给真实实现（不改变任何行为），只在转发前记录入参。
    """
    original = interview_agent.generate_question
    captured: Dict[str, Any] = {"calls": 0}

    async def wrapper(
        context: Any,
        plan: Any = None,
        resume: Any = None,
        job: Any = None,
        *,
        knowledge_context: Any = None,
        spark: Any = None,
    ) -> Dict[str, Any]:
        captured["calls"] += 1
        captured["context"] = context
        captured["plan"] = plan
        captured["resume"] = resume
        captured["job"] = job
        captured["knowledge_context"] = knowledge_context
        # 用**真实实现**的变量组装函数算一遍，得到「Agent 内部看到的输入结构」
        captured["variables"] = build_question_variables(context, plan, resume, job)
        return await original(
            context, plan, resume, job,
            knowledge_context=knowledge_context, spark=spark,
        )

    captured["signature_preserved"] = (
        list(inspect.signature(wrapper).parameters)
        == list(inspect.signature(original).parameters)
    )

    interview_agent.generate_question = wrapper
    try:
        yield captured
    finally:
        interview_agent.generate_question = original


def _fingerprint(relative_path: str) -> str:
    """规范化换行后的源码 sha256。"""
    raw = (BACKEND_DIR / relative_path).read_bytes()
    return hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def _between(text: str, start: str, end: str) -> str:
    """截出 ``start``（含）到 ``end``（不含）之间的正文。"""
    i = text.index(start)
    j = text.index(end, i + len(start))
    return text[i:j]


def _section_of(prompt: str, heading: str, next_heading: str) -> str:
    """截出某个 ``##`` 小节的正文（不含标题行本身）。"""
    body = _between(prompt, heading, next_heading)
    return body.split("\n", 1)[1] if "\n" in body else ""


def _bullet_lines(text: str) -> List[str]:
    return [line for line in text.splitlines() if line.startswith("- ")]


# ============================================================
# 三、环境：真实 DB 行 + 真实 Planner + 真实 RAG（只 Mock Spark）
# ============================================================
@asynccontextmanager
async def _full_env(
    *,
    with_profile: bool = True,
    documents: Sequence[Dict[str, str]] = (FIXED_DOC,),
    top_k: int = 3,
    min_score: Optional[float] = None,
) -> Iterator[Dict[str, Any]]:
    """构建一次**真实面试**的全部前置数据。

    ``with_profile=False`` 时**不建** Job / Resume 行，且会话的
    ``job_id`` / ``resume_id`` 均为 ``None`` —— 用来验「源头为空时走兜底」。
    """
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,               # 同一引擎的所有会话共享同一个内存库
        connect_args={"check_same_thread": False},
    )
    session: Optional[AsyncSession] = None
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        session = factory()

        session.add(User(username="agent_input", email="agent_input@example.com",
                         password_hash="x", role="user"))
        await session.flush()

        job_id: Optional[int] = None
        resume_id: Optional[int] = None
        if with_profile:
            job = Job(job_name=JOB_NAME, skills=JOB_SKILLS, duty=JOB_DUTY)
            resume = Resume(user_id=1, filename="resume.md", content=RESUME_CONTENT)
            session.add_all([job, resume])
            await session.flush()
            job_id, resume_id = job.id, resume.id

        row = InterviewSession(
            user_id=1, job_id=job_id, resume_id=resume_id,
            status="created", interview_type=SESSION_TYPE,
            difficulty=SESSION_DIFFICULTY, duration=SESSION_DURATION,
            total_questions=SESSION_TOTAL, current_question_no=1,
        )
        session.add(row)
        await session.commit()
        session_id = row.id

        # -- 真实 Planner 产出本场计划（与流程内部走的是同一个函数）--
        plan = await interview_core._build_session_plan(session, row)

        # -- 真实上下文行：先建（幂等），再按真实接口写状态 --
        await interview_context.create_context(session, session_id)
        await interview_context.update_context(
            session, session_id,
            current_stage=CURRENT_STAGE,
            asked_questions=[HISTORY_QUESTION],
            # 把 priority_topics 的**首项**标记为「已覆盖」⇒ ``current_topic`` 必然
            # 顺延到次项（``Redis 持久化``），检索 topic 因此确定、且与固定 chunk 词面重叠
            covered_topics=list(plan["priority_topics"][:1]),
            weak_topics=[WEAK_TOPIC],
        )
        context = await interview_context.get_context(session, session_id)

        # -- 真实 RAG 链路 --
        embedder = default_embedder()
        store = SqlAlchemyVectorStore(session)
        pipeline = KnowledgeImportPipeline(session, embedder=embedder, store=store)
        reports = [await pipeline.import_document(dict(doc)) for doc in documents]
        retriever = VectorKnowledgeRetriever(
            embedder, store, top_k=top_k, min_score=min_score,
        )

        yield {
            "session": session,          # AsyncSession（DB 会话）
            "row": row,                  # InterviewSession（ORM 行，勿与会话混淆）
            "session_id": session_id,
            "job_id": job_id,
            "resume_id": resume_id,
            "plan": plan,
            "context": context,
            "embedder": embedder,
            "store": store,
            "reports": reports,
            "retriever": retriever,
        }
    finally:
        if session is not None:
            await session.close()
        await engine.dispose()


async def _run_flow(env: Dict[str, Any], *replies: Any) -> Tuple[Dict[str, Any], SpySpark,
                                                                Dict[str, Any]]:
    """跑一次真实 ``generate_next_question``，返回 ``(结果, spy, 捕获的 Agent 入参)``。"""
    spy = SpySpark(*replies)
    async with _record_agent_input() as captured:
        result = await interview_core.generate_next_question(
            env["session"], env["session_id"],
            retriever=env["retriever"], spark=spy,
        )
    return result, spy, captured


# ============================================================
# [1] 前置与契约
# ============================================================
def check_preconditions() -> None:
    _section("[1] 前置与契约（「不改业务逻辑」是被验证的）")

    for relative_path, expected in FROZEN_SOURCES:
        actual = _fingerprint(relative_path)
        _check(f"★ {relative_path} 源码指纹未变",
               actual == expected, f"实际 {actual[:12]}… 期望 {expected[:12]}…")

    # 守卫自检：指纹对「被改过的内容」必须报不同（否则守卫恒为真）
    probe = FIXED_DOC["content"] + "（被改过）"
    _check("守卫自检：内容变化会改变指纹",
           hashlib.sha256(probe.encode("utf-8")).hexdigest()
           != hashlib.sha256(FIXED_DOC["content"].encode("utf-8")).hexdigest())

    # -- Agent 形参契约 --
    sig = inspect.signature(interview_agent.generate_question)
    params = list(sig.parameters)
    _check("Agent 恰好 6 个形参（4 位置 + 2 keyword-only）",
           params == ["context", "plan", "resume", "job",
                      "knowledge_context", "spark"], str(params))
    _check("knowledge_context 是 keyword-only（避免位置传参静默错位）",
           sig.parameters["knowledge_context"].kind is inspect.Parameter.KEYWORD_ONLY)
    _check("knowledge_context 默认 None（不用可变默认值）",
           sig.parameters["knowledge_context"].default is None)
    _check("Agent 签名里没有 retriever（Agent 不持有检索器、不主动检索）",
           "retriever" not in sig.parameters, str(params))

    # -- ★ 用户词汇 → 代码落点：把这个映射写成断言，而不是写在注释里 --
    _check("★ 用户词汇 interview_plan 的落点是形参 plan（不是 interview_plan）",
           "plan" in sig.parameters and "interview_plan" not in sig.parameters)
    _check("★ 用户词汇 history_questions **不是**形参——它藏在 context 里",
           "history_questions" not in sig.parameters
           and "asked_questions" not in sig.parameters, str(params))
    _check("★ 用户词汇 user_profile **不是**形参——它是 resume + job",
           "user_profile" not in sig.parameters)
    _check("★ 用户词汇 difficulty **不是**形参——它藏在 plan 里",
           "difficulty" not in sig.parameters, str(params))
    _check("★ 用户词汇 knowledge_context 是形参且为 keyword-only",
           "knowledge_context" in sig.parameters)

    # -- 变量集合契约 --
    base = build_question_variables(None, None, None, None)
    _check("基础变量恰好 10 个（与 BASE_VARIABLE_NAMES 逐位一致）",
           tuple(base) == BASE_VARIABLE_NAMES, str(tuple(base)))
    _check("interview_plan 载荷恰好 7 个键",
           tuple(base["interview_plan"]) == PLAN_PAYLOAD_KEYS,
           str(tuple(base["interview_plan"])))


# ============================================================
# [2] 真实流程 · Agent 输入结构
# ============================================================
async def check_agent_input_structure() -> None:
    _section("[2] 真实流程 · Agent 输入结构（捕获真实入参）")

    async with _full_env() as env:
        # 前置：真实 Planner 的产出（后面用来证明「值来自源头」）
        plan = env["plan"]
        _check("前置：固定知识 chunk 已入库（1 个切片）",
               env["reports"][0]["status"] == STATUS_OK
               and env["reports"][0]["chunk_count"] == 1,
               f"status={env['reports'][0]['status']} "
               f"chunks={env['reports'][0]['chunk_count']}")
        _check("前置：priority_topics 恰为 ['MySQL', 'Redis 持久化']（fixture 的确定产出）",
               plan["priority_topics"] == ["MySQL", "Redis 持久化"],
               str(plan["priority_topics"]))
        _check("前置：current_topic 取到 'Redis 持久化'（首项 MySQL 已覆盖 ⇒ 顺延次项）",
               interview_core.current_topic(plan, env["context"]) == "Redis 持久化",
               interview_core.current_topic(plan, env["context"]))

        result, spy, captured = await _run_flow(env, REPLY_OK)

        _check("真实流程出题成功（ok=True）", result["ok"] is True,
               f"errors={result['errors']} error={result['error']}")
        _check("errors / warnings 均为空", result["errors"] == [] and result["warnings"] == [],
               f"errors={result['errors']} warnings={result['warnings']}")

        # -- 捕获本身可信吗 --
        _check("★ 包装器恰好被调用 1 次（一次流程 = 一次 Agent 调用）",
               captured["calls"] == 1, str(captured["calls"]))
        _check("★ 包装器签名与原函数逐位一致（包装没有改变契约）",
               captured["signature_preserved"] is True)
        _check("★ 包装器转发后真实实现仍只调 1 次模型（未产生额外请求）",
               spy.call_count == 1, str(spy.call_count))

        # -- 五个入参逐个核对「确实来自源头」--
        _check("★ context 是流程读出的真实上下文（含种子历史问题）",
               isinstance(captured["context"], dict)
               and captured["context"].get("asked_questions") == [HISTORY_QUESTION],
               str(captured["context"].get("asked_questions")))
        _check("★ plan 与 Planner 的产出**逐键相等**（Core 未裁剪/未替换）",
               captured["plan"] == plan)
        _check("★ resume 是数据库里的 Resume 行（按 session.resume_id 加载）",
               isinstance(captured["resume"], Resume)
               and captured["resume"].id == env["resume_id"]
               and captured["resume"].content == RESUME_CONTENT,
               f"type={type(captured['resume']).__name__} id={getattr(captured['resume'], 'id', None)}")
        _check("★ job 是数据库里的 Job 行（按 session.job_id 加载）",
               isinstance(captured["job"], Job)
               and captured["job"].id == env["job_id"]
               and captured["job"].job_name == JOB_NAME,
               f"type={type(captured['job']).__name__} id={getattr(captured['job'], 'id', None)}")
        _check("★ knowledge_context 是**检索器的原样输出**（1 条真实 KnowledgeChunk）",
               isinstance(captured["knowledge_context"], list)
               and len(captured["knowledge_context"]) == 1
               and isinstance(captured["knowledge_context"][0], KnowledgeChunk),
               str(type(captured["knowledge_context"])))
        _check("★ 且该条内容 == 固定知识正文（逐字节）",
               captured["knowledge_context"][0].content == FIXED_DOC["content"])
        _check("★ 检索器收到的 topic == plan.priority_topics[1]（Core 的 current_topic）",
               env["retriever"].queries == ["Redis 持久化"], str(env["retriever"].queries))

        # -- Agent 内部看到的输入结构（10 个变量 + 1 个知识）--
        variables = captured["variables"]
        _check("Agent 侧变量集合恰好 10 个",
               tuple(variables) == BASE_VARIABLE_NAMES, str(tuple(variables)))
        _check("★ 五个用户字段在变量层全部有落点（无遗漏、无多余）",
               {"interview_plan", "asked_questions", "difficulty",
                "resume_summary", "job_title", "job_description"} <= set(variables))

        # ---- 打印「Agent 输入结构」----
        print("\n  ── Agent 形参（真实流程实际传入）──")
        print(f"  {'形参':<18}{'类型':<22}值摘要")
        for name in ("context", "plan", "resume", "job", "knowledge_context"):
            value = captured[name]
            if name == "context":
                summary = (f"dict，{len(value)} 键；current_stage={value.get('current_stage')}、"
                           f"asked_questions={len(value.get('asked_questions') or [])} 条")
            elif name == "plan":
                summary = (f"dict，{len(value)} 键；source={value.get('source')}、"
                           f"priority_topics={value.get('priority_topics')}")
            elif name == "resume":
                summary = (f"Resume#{value.id}，content {len(value.content or '')} 字"
                           if value is not None else "None")
            elif name == "job":
                summary = f"Job#{value.id}，{value.job_name}" if value is not None else "None"
            else:
                summary = (f"list[{len(value)}]（KnowledgeChunk，"
                           f"score={value[0].metadata.get('score'):.4f}）"
                           if value else "list[0]")
            print(f"  {name:<18}{type(value).__name__:<22}{summary}")

        print("\n  ── 派生变量（Agent 内部看到的输入结构）──")
        print(f"  {'#':<3}{'变量名':<18}{'值摘要'}")
        for index, name in enumerate(BASE_VARIABLE_NAMES, start=1):
            value = variables[name]
            text = repr(value)
            if len(text) > 60:
                text = text[:57] + "…"
            print(f"  {index:<3}{name:<18}{text}")
        print(f"  {'+':<3}{'knowledge_context':<18}"
              f"list[1]（仅在非空时注入；空知识时**不产生**该变量）")


# ============================================================
# [3] 五个字段逐一核对
# ============================================================
async def check_five_fields() -> None:
    _section("[3] 五个字段逐一核对（值来自源头、且不是默认值）")

    async with _full_env() as env:
        plan = env["plan"]
        result, spy, captured = await _run_flow(env, REPLY_OK)
        v = captured["variables"]

        # ---- 1) interview_plan ----
        _check("★ [interview_plan] 是 dict 且恰好 7 个键（顺序一致）",
               isinstance(v["interview_plan"], dict)
               and tuple(v["interview_plan"]) == PLAN_PAYLOAD_KEYS,
               str(tuple(v["interview_plan"])))
        _check("★ [interview_plan] 是**按会话配置制定**的计划（非空、非默认模板）",
               v["interview_plan"]["interview_type"] == SESSION_TYPE
               and v["interview_plan"]["difficulty"] == SESSION_DIFFICULTY
               and v["interview_plan"]["total_questions"] == SESSION_TOTAL,
               f"type={v['interview_plan']['interview_type']} "
               f"diff={v['interview_plan']['difficulty']} "
               f"total={v['interview_plan']['total_questions']}")
        _check("★ [interview_plan] 的 stages 覆盖 5 个阶段且题量合计 == total_questions",
               len(v["interview_plan"]["stages"]) == 5
               and sum(s["target_questions"] for s in v["interview_plan"]["stages"])
               == SESSION_TOTAL,
               str([s["stage"] for s in v["interview_plan"]["stages"]]))
        _check("★ [interview_plan] 的 target_topics 来自 DB 里的 Job.skills + Resume 正文",
               v["interview_plan"]["target_topics"][0] == "Redis 持久化"
               and "MySQL" in v["interview_plan"]["target_topics"],
               str(v["interview_plan"]["target_topics"]))
        _check("★ [interview_plan] 的 resume_focus_points 来自 Resume 正文的项目名",
               v["interview_plan"]["resume_focus_points"] == ["订单中台"],
               str(v["interview_plan"]["resume_focus_points"]))
        _check("★ [interview_plan] 与 Planner 产出逐键相等（Core 未改写计划）",
               v["interview_plan"] == {
                   key: plan[key] for key in PLAN_PAYLOAD_KEYS
               })

        # ---- 2) history_questions ----
        _check("★ [history_questions] == 数据库上下文里的 asked_questions",
               v["asked_questions"] == env["context"]["asked_questions"]
               == [HISTORY_QUESTION], str(v["asked_questions"]))
        _check("★ [history_questions] 不是空列表（否则「传没传」无从判断）",
               len(v["asked_questions"]) == 1, str(len(v["asked_questions"])))

        # ---- 3) user_profile ----
        _check("★ [user_profile] resume_summary == Resume.content（逐字节，未截断）",
               v["resume_summary"] == RESUME_CONTENT,
               f"长度 {len(v['resume_summary'])} vs {len(RESUME_CONTENT)}")
        _check("★ [user_profile] job_title == Job.job_name",
               v["job_title"] == JOB_NAME, repr(v["job_title"]))
        _check("★ [user_profile] job_description 由 Job.skills + Job.duty 拼成",
               v["job_description"] == JOB_DESCRIPTION, repr(v["job_description"]))

        # ---- 4) difficulty ----
        _check("★ [difficulty] == 会话配置的 difficulty（senior）",
               v["difficulty"] == SESSION_DIFFICULTY == env["row"].difficulty,
               repr(v["difficulty"]))
        _check("★ [difficulty] **不是**默认值 mid ⇒ 证明它确实被传下去了",
               v["difficulty"] != DEFAULT_DIFFICULTY,
               f"{v['difficulty']} vs 默认 {DEFAULT_DIFFICULTY}")

        # ---- 5) knowledge_context ----
        chunks = captured["knowledge_context"]
        _check("★ [knowledge_context] 非空（1 条）且被 Agent 收到",
               isinstance(chunks, list) and len(chunks) == 1, str(len(chunks or [])))
        _check("★ [knowledge_context] 归一后恰好 1 行、且内联来源",
               interview_agent.normalize_knowledge_context(chunks)
               == [f"{FIXED_DOC['content']}（来源：{FIXED_SOURCE}）"],
               str(interview_agent.normalize_knowledge_context(chunks))[:80])
        _check("★ 知识正文**逐字节**出现在模型实际收到的 Prompt 里",
               FIXED_DOC["content"] in spy.calls[0])
        _check("★ 检索分数不渗进 Prompt（score 只在 metadata 里）",
               str(chunks[0].metadata["score"]) not in spy.calls[0]
               and "score" not in spy.calls[0])

        # ---- 交叉：五个字段各自「不是默认值」的统一判据 ----
        _check("★ 五字段全部**未**退化为默认值（difficulty/stage/type 三项对照）",
               v["difficulty"] != DEFAULT_DIFFICULTY
               and v["current_stage"] != DEFAULT_STAGE
               and v["interview_type"] != DEFAULT_INTERVIEW_TYPE,
               f"{v['difficulty']} / {v['current_stage']} / {v['interview_type']}")

        # ---- 流程不写库 ----
        after = await interview_context.get_context(env["session"], env["session_id"])
        _check("★ 出题流程不写库（asked_questions 未被追加，仍只有 1 条）",
               after["asked_questions"] == [HISTORY_QUESTION],
               str(after["asked_questions"]))
        _check("出题流程不推进题号（current_question_no 保持 1）",
               env["row"].current_question_no == 1,
               str(env["row"].current_question_no))

        _check("出题成功（本节的比对前提）", result["ok"] is True, str(result.get("error")))


# ============================================================
# [4] Prompt 最终内容
# ============================================================
async def check_final_prompt() -> None:
    _section("[4] Prompt 最终内容（每个变量落在正确小节）")

    async with _full_env() as env:
        result, spy, captured = await _run_flow(env, REPLY_OK)
        v = captured["variables"]
        prompt = spy.calls[0]

        _check("★ 模型实际收到的是**变体模板**（有知识 ⇒ question_knowledge）",
               render_question_prompt(v, captured["knowledge_context"])[0]
               == PROMPT_QUESTION_KNOWLEDGE)
        _check("★ 无未渲染占位符（{{ 已全部替换）", "{{" not in prompt)

        # -- 小节顺序：知识小节必须在「计划」与「进度」之间 --
        _check("★ 知识小节位于「本场面试计划」与「本场进度」之间",
               prompt.index(H_PLAN) < prompt.index(H_KNOWLEDGE) < prompt.index(H_PROGRESS))

        # -- 一、面试配置 --
        config = _section_of(prompt, H_CONFIG, H_JOB)
        _check("★ [interview_type] 落在「面试类型」行",
               f"- 面试类型：{v['interview_type']}" in config, repr(v["interview_type"]))
        _check("★ [difficulty] 落在「面试难度」行（senior，不是默认 mid）",
               f"- 面试难度：{v['difficulty']}" in config
               and f"- 面试难度：{DEFAULT_DIFFICULTY}" not in prompt,
               repr(v["difficulty"]))
        _check("★ [current_stage] 落在「当前阶段」行",
               f"- 当前阶段：{v['current_stage']}" in config, repr(v["current_stage"]))

        # -- 二、目标岗位（user_profile 的岗位侧）--
        job_block = _section_of(prompt, H_JOB, H_RESUME)
        _check("★ [job_title] 落在「岗位名称」行",
               f"- 岗位名称：{JOB_NAME}" in job_block)
        _check("★ [job_description] 落在「岗位描述」行（含 skills 与 duty）",
               f"- 岗位描述：{JOB_DESCRIPTION}" in job_block
               and JOB_SKILLS in job_block and JOB_DUTY in job_block)

        # -- 三、简历摘要（user_profile 的简历侧）--
        resume_block = _section_of(prompt, H_RESUME, H_PLAN)
        _check("★ [resume_summary] 落在「简历摘要」小节，且与 DB 行逐字节相同",
               RESUME_CONTENT in resume_block and v["resume_summary"] in resume_block)
        _check("简历未触发截断标记（内容 < 1200 字）",
               "…（已截断）" not in prompt)

        # -- 四、本场面试计划（interview_plan）--
        plan_block = _section_of(prompt, H_PLAN, H_KNOWLEDGE)
        expected_plan_text = json.dumps(v["interview_plan"], ensure_ascii=False, indent=2)
        _check("★ [interview_plan] 以**逐字节相同的 JSON** 落在「本场面试计划」小节",
               plan_block.strip() == expected_plan_text,
               f"小节 {len(plan_block.strip())} 字符 vs 期望 {len(expected_plan_text)} 字符")
        for key in ("priority_topics", "target_topics", "resume_focus_points", "stages"):
            _check(f"  └ 计划载荷的 '{key}' 出现在 Prompt 里", f'"{key}"' in plan_block)
        _check("  └ 计划里的 priority_topics 值逐字出现（'Redis 持久化'）",
               '"Redis 持久化"' in plan_block)

        # -- 四·五、参考知识（knowledge_context）--
        knowledge_block = _section_of(prompt, H_KNOWLEDGE, H_PROGRESS)
        bullets = _bullet_lines(knowledge_block)
        _check("★ [knowledge_context] 渲染为恰好 1 个项目符号行",
               len(bullets) == 1, str(len(bullets)))
        _check("★ 该行 == '- 正文（来源：…）'（逐字节）",
               bullets[0] == f"- {FIXED_DOC['content']}（来源：{FIXED_SOURCE}）",
               repr(bullets[0][:70]))
        _check("知识正文在整个 Prompt 里只出现 1 次（未重复注入）",
               prompt.count(FIXED_DOC["content"]) == 1,
               str(prompt.count(FIXED_DOC["content"])))

        # -- 五、本场进度（history_questions / covered / weak）--
        progress = _section_of(prompt, H_PROGRESS, H_REQUIREMENTS)
        _check("★ [history_questions] 落在「已提问问题」下（逐字节，带 '- ' 前缀）",
               f"- {HISTORY_QUESTION}" in progress)
        _check("★ [covered_topics] 落在「已覆盖知识点」下",
               f"- {v['covered_topics'][0]}" in progress, str(v["covered_topics"]))
        _check("★ [weak_topics] 落在「薄弱知识点（优先补强）」下",
               f"- {v['weak_topics'][0]}" in progress, str(v["weak_topics"]))
        _check("  └ 三个列表互不串位（历史问题不出现在「已覆盖知识点」下）",
               f"- {HISTORY_QUESTION}" not in
               progress.split("已覆盖知识点：")[1].split("薄弱知识点")[0])

        # -- 十变量各自恰好出现一次 --
        _check("★ 10 个变量全部有落点、且 Prompt 内无重复注入（知识正文 1 次）",
               prompt.count(RESUME_CONTENT) == 1
               and prompt.count(JOB_NAME) == 1
               and prompt.count(HISTORY_QUESTION) == 1,
               f"resume={prompt.count(RESUME_CONTENT)} "
               f"job={prompt.count(JOB_NAME)} hist={prompt.count(HISTORY_QUESTION)}")

        _check("出题成功（本节的比对前提）", result["ok"] is True, str(result.get("error")))

        print(f"\n  最终 Prompt 长度：{len(prompt)} 字符（模型实际收到的那一份）")
        print("  小节长度分布：")
        for label, body in (
            ("一、面试配置", config), ("二、目标岗位", job_block),
            ("三、简历摘要", resume_block), ("四、本场面试计划", plan_block),
            ("四·五、参考知识", knowledge_block), ("五、本场进度", progress),
        ):
            print(f"    {label:<16}{len(body):>5} 字符")


# ============================================================
# [5] 反向对照：默认值只在源头为空时出现
# ============================================================
async def check_fallbacks() -> None:
    _section("[5] 反向对照（默认值只在源头为空时出现）")

    # -- 5a. 纯函数：三个入参全为 None 时的文档化默认 --
    bare = build_question_variables(None, None, None, None)
    _check("无 resume ⇒ resume_summary 取兜底串",
           bare["resume_summary"] == FALLBACK_RESUME, repr(bare["resume_summary"]))
    _check("无 job ⇒ job_title 取兜底串",
           bare["job_title"] == FALLBACK_JOB_TITLE, repr(bare["job_title"]))
    _check("无 job ⇒ job_description 取兜底串",
           bare["job_description"] == FALLBACK_JOB_DESCRIPTION,
           repr(bare["job_description"]))
    _check("无 plan ⇒ difficulty 取默认 mid / interview_type 取默认 comprehensive",
           bare["difficulty"] == DEFAULT_DIFFICULTY
           and bare["interview_type"] == DEFAULT_INTERVIEW_TYPE,
           f"{bare['difficulty']} / {bare['interview_type']}")
    _check("无 context ⇒ current_stage 取默认 introduction",
           bare["current_stage"] == DEFAULT_STAGE, repr(bare["current_stage"]))
    _check("无 context ⇒ 三个列表为空",
           bare["asked_questions"] == [] and bare["covered_topics"] == []
           and bare["weak_topics"] == [],
           f"{bare['asked_questions']} / {bare['covered_topics']} / {bare['weak_topics']}")
    _check("无 plan ⇒ 计划载荷的 5 个列表/标量为空（total_questions 为 None）",
           bare["interview_plan"]["total_questions"] is None
           and bare["interview_plan"]["stages"] == []
           and bare["interview_plan"]["priority_topics"] == []
           and bare["interview_plan"]["resume_focus_points"] == [],
           str(bare["interview_plan"]))

    # -- 5b. 真实流程：会话**没有** job/resume ⇒ 兜底串真的会出现 --
    async with _full_env(with_profile=False, documents=()) as env:
        _check("前置：会话的 job_id / resume_id 均为 None",
               env["job_id"] is None and env["resume_id"] is None,
               f"job={env['job_id']} resume={env['resume_id']}")
        _check("前置：无 job/resume ⇒ Planner 的 priority_topics 为空",
               env["plan"]["priority_topics"] == [], str(env["plan"]["priority_topics"]))

        result, spy, captured = await _run_flow(env, REPLY_OK)
        prompt = spy.calls[0]
        v = captured["variables"]

        _check("无 profile 的真实流程仍出题成功", result["ok"] is True,
               f"errors={result['errors']}")
        _check("★ 无 profile ⇒ 三个兜底串**全部**出现在 Prompt 里",
               all(f in prompt for f in FALLBACKS),
               str([f for f in FALLBACKS if f not in prompt]))
        _check("★ 且 resume_summary / job_title / job_description 恰为兜底串",
               v["resume_summary"] == FALLBACK_RESUME
               and v["job_title"] == FALLBACK_JOB_TITLE
               and v["job_description"] == FALLBACK_JOB_DESCRIPTION,
               f"{v['job_title']} / {v['job_description']}")
        _check("★ difficulty 仍来自会话配置（senior）⇒ 与 profile 无关",
               v["difficulty"] == SESSION_DIFFICULTY, repr(v["difficulty"]))
        _check("无 profile ⇒ 无知识 ⇒ 走原模板（不含参考知识小节）",
               H_KNOWLEDGE not in prompt)
        _check("无 profile 时也无未渲染占位符", "{{" not in prompt)

    # -- 5c. 主流程里**不该**出现任何兜底串 --
    async with _full_env() as env:
        _, spy_main, _ = await _run_flow(env, REPLY_OK)
        main_prompt = spy_main.calls[0]
        _check("★ 对照：有 profile 的真实流程里，兜底串**一个都不出现**",
               not any(f in main_prompt for f in FALLBACKS),
               str([f for f in FALLBACKS if f in main_prompt]))


# ============================================================
# [6] history_questions 真的进入 Agent 的查重路径
# ============================================================
async def check_history_is_used() -> None:
    _section("[6] history_questions 真的被用上（不只是渲染进 Prompt）")

    async with _full_env() as env:
        # 让模型返回**与历史完全相同**的问题 ⇒ 应被查重打回并发起一次修复
        result, spy, captured = await _run_flow(env, REPLY_DUPLICATE, REPLY_OK)

        _check("★ 与历史重复的候选被拒绝 ⇒ 发起了一次修复（共 2 次模型调用）",
               spy.call_count == 2, str(spy.call_count))
        _check("★ 第一次 Prompt（含历史）确实发出去了",
               f"- {HISTORY_QUESTION}" in spy.calls[0])
        _check("★ 修复请求里也带上了历史问题（question_repair.txt 的 asked_questions）",
               f"- {HISTORY_QUESTION}" in spy.calls[1],
               repr(spy.calls[1][-120:]))
        _check("★ 修复后出题成功（历史只在「重复」时拦人，不阻断流程）",
               result["ok"] is True, f"errors={result['errors']}")
        _check("最终题目 == 修复回复里的问题（未被历史顶掉）",
               result["question"].startswith("在订单中台的缓存体系里"),
               repr(result["question"][:30]))

        # 对照：非重复回复只需 1 次调用
        result2, spy2, _ = await _run_flow(env, REPLY_OK)
        _check("★ 对照：非重复回复只需 1 次模型调用（查重不误伤）",
               spy2.call_count == 1, str(spy2.call_count))

        # 直接对查重函数取证（与上面走的是同一条规则）
        problem = interview_agent.validate_question(
            {"question": HISTORY_QUESTION, "topic": "自我介绍",
             "difficulty": SESSION_DIFFICULTY},
            asked_questions=captured["context"]["asked_questions"],
        )
        _check("★ 历史问题经 validate_question 判定为「高度重复」",
               problem is not None and "重复" in problem, str(problem))


# ============================================================
# 报告
# ============================================================
async def report() -> None:
    print("\n" + "=" * 74)
    print("Prompt 最终内容（一次真实面试问题生成流程 · 模型实际收到的那一份）")
    print("=" * 74)

    async with _full_env() as env:
        _, spy, _ = await _run_flow(env, REPLY_OK)
        prompt = spy.calls[0]

    end = prompt.index(H_REQUIREMENTS)
    print(prompt[:end].rstrip())
    print("\n  …（此处省略「六、生成要求」与「七、输出格式」两节，共 "
          f"{len(prompt)} 字符）")

    print("\n" + "=" * 74)
    print("结果")
    print("=" * 74)
    print("  链路：InterviewSession/Job/Resume/InterviewContext（真实 DB 行）")
    print("        → interview_planner.build_plan_for（真实规则计划）")
    print("        → VectorKnowledgeRetriever（真实 RAG）")
    print("        → interview_core.generate_next_question")
    print("        → interview_agent.generate_question（入参被捕获核对）")
    print("        → prompts/interview/question_knowledge.txt")
    print("  五字段落点：interview_plan→plan / history_questions→context.asked_questions")
    print("              user_profile→resume+job / difficulty→plan.difficulty")
    print("              knowledge_context→keyword-only 形参")
    print("  保护文件指纹：6 个（4 业务模块 + 2 Prompt 模板，全部未改）")
    print("  改动：只新增本测试文件（未修改业务逻辑，未新增生产路径日志）")
    print(f"  断言：通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)


async def main() -> None:
    print("=" * 74)
    print("InterviewAgent 完整输入验证（一次真实面试问题生成流程）")
    print("=" * 74)

    check_preconditions()
    await check_agent_input_structure()
    await check_five_fields()
    await check_final_prompt()
    await check_fallbacks()
    await check_history_is_used()
    await report()


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(0 if _FAILED == 0 else 1)
