# -*- coding: utf-8 -*-
"""RAG **无结果**时 InterviewAgent 仍能正常出题（脚本式，非 pytest）。

运行：``python backend/tests/test_agent_no_knowledge.py``

两个模拟场景
------------
1. **Retriever 返回空结果** —— 真实 ``VectorKnowledgeRetriever`` 面对空库 /
   空查询短路 / 自定义恒空实现 / 默认 ``KnowledgeRetriever()`` 空实现
2. **VectorStore 无匹配知识** —— 空表 / ``min_score`` 全部滤掉 /
   ``category`` / ``document_id`` 过滤后为空

要验证的核心性质
----------------
**无知识时 ``knowledge_context`` 为空，但 Agent 流程继续**：

- ``knowledge_context`` 恰好是 ``[]``（**不是** ``None``、**不是** ``[""]``）
- Agent 仍然出题成功（``ok=True``）、仍然调用一次模型、**不抛异常**
- Prompt 回落原模板（``question.txt``），**不含**参考知识小节、无未渲染占位符
- **★ 与「完全不注入检索器」的 Prompt 逐字节相同** —— 这是「无知识时行为
  与引入知识能力之前完全一致」的**最强证据**（不是「差不多」，是同一个字符串）
- 修复路径（首次输出非法 JSON → 一次修复）在无知识时照常可用

三种「没有知识」的原因必须分开（``errors`` vs ``warnings`` vs 静默）
-------------------------------------------------------------------
============================  ==========================================
原因                          结果
============================  ==========================================
检索**抛异常**（服务故障）      ``[]`` + ``knowledge_retrieval_failed``
                              （**warning**，不进 ``errors``）
检索**正常但没结果**            ``[]`` + **无 warning**（不是故障）
不注入检索器 / ``use_rag=False`` ``[]`` + **无 warning**（默认安全态）
============================  ==========================================

不要修改
--------
``services/interview_core.py`` 与 ``services/question_validator.py`` 的
**源码指纹**在 [1] 锁死（改一个字符即失败）。本套件**纯新增测试**，
未改动任何生产代码。

.. note::
   本套件与 ``test_rag_into_agent_prompt.py`` [7] 节的区别：那里的空知识是
   **顺带**验的（一两条断言）；本套件把「无结果」当成**主题**做穷举——
   六种「检索不到」的成因、四种「不注入」的写法、三种 warning 语义，
   外加「Prompt 与不注入时逐字节相同」这条强断言。
"""

from __future__ import annotations

import asyncio
import hashlib
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
from models import InterviewSession, User  # noqa: E402
from services import interview_agent, interview_core  # noqa: E402
from services.interview_agent import (  # noqa: E402
    PROMPT_QUESTION,
    build_question_variables,
    render_question_prompt,
)
from services.knowledge_import_pipeline import (  # noqa: E402
    KnowledgeImportPipeline,
    STATUS_OK,
)
from services.knowledge_rag import default_embedder  # noqa: E402
from services.knowledge_retriever import KnowledgeRetriever  # noqa: E402
from services.vector_knowledge_retriever import (  # noqa: E402
    VectorKnowledgeRetriever,
    build_query,
)
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

# ============================================================
# 一、常量
# ============================================================
#: 「不要修改」的判据：源码指纹（规范化换行后的 sha256）。
#: 用**源码文本**而非 AST——``ast.dump`` 随 Python 版本变化，换解释器会假失败。
FROZEN_SOURCES: Tuple[Tuple[str, str], ...] = (
    ("services/interview_core.py",
     "b8da0caa24577cc21b134666a8da602bd6f8d701bf287c62163840fc7e949d51"),
    ("services/question_validator.py",
     "b23e6957edf39696fcc4803b4b627fbdc90c1548de86535bc804f4acc1c7e53b"),
)

#: 库里有知识时用的固定文档（用于「有知识但不匹配」那一档）。
FIXED_DOC: Dict[str, str] = {
    "title": "Redis 持久化机制手册",
    "category": "technical",
    "source": "handbook://redis/persistence",
    "content": "Redis 持久化提供 RDB 与 AOF 两种机制：RDB 是全量快照，AOF 记录写命令。",
}

#: 与固定文档**词面完全不重叠**的知识点。
#: .. warning::
#:   别假定「不相关 ⇒ 分数为负」。默认 embedder 是 ``HashEmbeddingService``
#:   （**词面哈希**，``semantic_enabled=False``），分数只保证「同一模型下可比较」，
#:   **不是**「越大越相关」——实测该 topic 得分 ≈ 0.171（**正数**）。
#:   所以本套件只断言「仍会被返回」，不断言符号；阈值取 0.5（实测得分明显低于它）。
UNRELATED_TOPIC = "量子计算与光合作用"

CONTEXT: Dict[str, Any] = {
    "current_stage": "technical",
    "asked_questions": [],
    "covered_topics": [],
    "weak_topics": [],
}
PLAN: Dict[str, Any] = {
    "interview_type": "technical",
    "difficulty": "mid",
    "total_questions": 5,
    "priority_topics": ["Redis 持久化"],
}

JOB: Dict[str, Any] = {"job_name": "后端开发工程师"}
RESUME: Dict[str, Any] = {"content": "3 年后端经验，技术栈 Python / MySQL / Redis"}

QUESTION_REPLY = json.dumps(
    {
        "question": "请说明 Redis 持久化的两种机制与取舍。",
        "question_type": "technical",
        "topic": "Redis 持久化",
        "difficulty": "mid",
        "expected_points": ["RDB", "AOF"],
        "reason": "考察岗位要求中的缓存中间件原理",
    },
    ensure_ascii=False,
)

#: 无知识时的 Prompt **必须**不含这些痕迹。
KNOWLEDGE_TRACES: Tuple[str, ...] = (
    "参考知识",              # 小节标签
    "knowledge_context",     # 变量名（渲染后不该出现）
    "四·五",                 # 小节编号
    "（来源：",               # 来源标注
)

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
    """记录每次收到的 Prompt；按顺序吐出回复，超出即报错。"""

    def __init__(self, *replies: Any) -> None:
        self.replies = list(replies)
        self.calls: List[str] = []

    async def chat_async(self, message: str) -> str:
        self.calls.append(message)
        if not self.replies:
            raise AssertionError("SpySpark 被调用次数超出预期")
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


class EmptyRetriever:
    """恒返回 ``[]`` 的检索器（模拟「检索器正常但没结果」）。"""

    def __init__(self) -> None:
        self.calls: List[Tuple[Any, Any, Any]] = []

    async def retrieve(self, job_info: Any, topic: Any, context: Any) -> List[Any]:
        self.calls.append((job_info, topic, context))
        return []


class BoomRetriever:
    """检索即抛异常的检索器（模拟「检索服务故障」）。"""

    def __init__(self, message: str = "检索服务挂了") -> None:
        self.message = message

    async def retrieve(self, job_info: Any, topic: Any, context: Any) -> List[Any]:
        raise RuntimeError(self.message)


class CountingEmbedder:
    """包装 embedder，统计 ``embed`` 被调用几次（验证空查询**短路**不发起 IO）。"""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.calls = 0

    @property
    def name(self) -> Any:
        return getattr(self.inner, "name", None)

    async def embed(self, text: str) -> Any:
        self.calls += 1
        return await self.inner.embed(text)


def _fingerprint(relative_path: str) -> str:
    raw = (BACKEND_DIR / relative_path).read_bytes()
    return hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def _no_knowledge_trace(prompt: str) -> bool:
    return not any(trace in prompt for trace in KNOWLEDGE_TRACES)


# ============================================================
# 三、环境
# ============================================================
@asynccontextmanager
async def _env(*, documents: Sequence[Dict[str, str]] = (),
               with_session: bool = True) -> Iterator[Dict[str, Any]]:
    """真实 SQLite + 真实 store + 真实 embedder；``documents`` 为空即**空库**。"""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    session: Optional[AsyncSession] = None
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        session = factory()

        session_id: Optional[int] = None
        if with_session:
            session.add(User(username="no_know", email="no_know@example.com",
                             password_hash="x", role="user"))
            await session.flush()
            row = InterviewSession(
                user_id=1, status="created", total_questions=5,
                current_question_no=1, interview_type="technical", difficulty="mid",
            )
            session.add(row)
            await session.commit()
            session_id = row.id

        embedder = default_embedder()
        store = SqlAlchemyVectorStore(session)
        reports = [
            await KnowledgeImportPipeline(session, embedder=embedder,
                                          store=store).import_document(dict(doc))
            for doc in documents
        ]

        yield {
            "session": session,
            "session_id": session_id,
            "embedder": embedder,
            "store": store,
            "reports": reports,
        }
    finally:
        if session is not None:
            await session.close()
        await engine.dispose()


async def _run_core(env: Dict[str, Any], *, spark: Any, **kwargs: Any) -> Dict[str, Any]:
    """走 Core 的端到端入口（注入 context / plan 以跳过读库）。"""
    plan = kwargs.pop("plan", PLAN)
    return await interview_core.generate_next_question(
        env["session"], env["session_id"],
        context=CONTEXT, plan=plan, spark=spark, **kwargs,
    )


# ============================================================
# [1] 前置与契约
# ============================================================
def check_preconditions() -> None:
    _section("[1] 前置与契约（「不要修改」是被验证的）")

    for relative_path, expected in FROZEN_SOURCES:
        actual = _fingerprint(relative_path)
        _check(f"★ {relative_path} 源码指纹未变",
               actual == expected, f"实际 {actual[:12]}… 期望 {expected[:12]}…")

    # 守卫自检：指纹对「被改过的内容」必须报不同（否则守卫恒为真）
    probe = "".join(["一", "二"]) + "三"
    _check("守卫自检：内容变化会改变指纹",
           hashlib.sha256(probe.encode("utf-8")).hexdigest()
           != hashlib.sha256("一二".encode("utf-8")).hexdigest())

    # 「无知识痕迹」的判据本身要能区分有/无（反例运行时拼出）
    probe_trace = "参考" + "知识"
    _check("守卫自检：无知识判据能区分「有参考知识」与「没有」",
           not _no_knowledge_trace(f"前文{probe_trace}后文")
           and _no_knowledge_trace("一段没有任何知识痕迹的普通 Prompt"))

    # Core 的降级约定：失败记 warning、不进 errors
    _check("Core 定义了稳定的 knowledge_retrieval_failed 警告码",
           interview_core.WARNING_KNOWLEDGE_FAILED == "knowledge_retrieval_failed",
           interview_core.WARNING_KNOWLEDGE_FAILED)
    _check("★ 该警告码**不在** errors 错误码集合里（warning 与 error 分开）",
           interview_core.WARNING_KNOWLEDGE_FAILED not in {
               interview_core.ERROR_SESSION_NOT_FOUND,
               interview_core.ERROR_SESSION_FINISHED,
               interview_core.ERROR_ALL_ANSWERED,
               interview_core.ERROR_AGENT_FAILED,
               interview_core.ERROR_VALIDATION_FAILED,
           })
    _check("结果字段集恒定为 QUESTION_RESULT_FIELDS（12 键，成功失败同形状）",
           len(interview_core.QUESTION_RESULT_FIELDS) == 12,
           str(len(interview_core.QUESTION_RESULT_FIELDS)))


# ============================================================
# [2] 场景 1 · Retriever 返回空结果
# ============================================================
async def check_scenario_empty_retriever() -> None:
    _section("[2] 场景 1 · Retriever 返回空结果")

    # -- 2a. 默认空实现 KnowledgeRetriever()（生产默认态）--
    base = KnowledgeRetriever()
    _check("默认 KnowledgeRetriever() 的 retrieve 恒返回 []",
           await base.retrieve(None, "任意 topic", {}) == [])
    _check("  └ 返回的是 list 而不是 None（调用方无需判空分支）",
           isinstance(await base.retrieve(None, "x", {}), list))
    _check("  └ 任意入参形状都返回 []（None / 空串 / 对象）",
           await base.retrieve(None, None, None) == []
           and await base.retrieve({}, "", object()) == [])

    # -- 2b. 自定义恒空检索器 --
    empty = EmptyRetriever()
    got, warnings = await interview_core._gather_knowledge(JOB, PLAN, CONTEXT, empty)
    _check("恒空检索器 ⇒ _gather_knowledge 返回 []", got == [])
    _check("★ 返回类型是 list（不是 None）", isinstance(got, list), type(got).__name__)
    _check("★ 检索器被正常调用（拿到 job / topic / context）", len(empty.calls) == 1)
    _check("  └ 传下去的 topic == plan.priority_topics[0]",
           empty.calls[0][1] == "Redis 持久化", str(empty.calls[0][1]))
    _check("★ 检索不到**不产生 warning**（不是故障）", warnings == [], str(warnings))

    # -- 2c. 真实检索器 + 空库 --
    async with _env(documents=()) as env:
        retriever = VectorKnowledgeRetriever(env["embedder"], env["store"], top_k=5)
        chunks = await retriever.retrieve(JOB, "Redis 持久化", CONTEXT)
        _check("真实检索器面对**空向量库** ⇒ 0 条", chunks == [], str(len(chunks)))
        _check("  └ 检索器仍记录了 query（说明走完了正常流程，不是短路）",
               retriever.queries == ["Redis 持久化"], str(retriever.queries))

    # -- 2d. 真实检索器 + 空查询 ⇒ 短路（不发起任何 IO）--
    counting = CountingEmbedder(default_embedder())
    async with _env(documents=()) as env:
        short_circuit = VectorKnowledgeRetriever(counting, env["store"], top_k=5)
        _check("build_query(None, '', {}) == ''（无线索可查）",
               build_query(None, "", {}) == "", repr(build_query(None, "", {})))
        chunks = await short_circuit.retrieve(None, "", {})
        _check("★ 空查询 ⇒ 检索器**短路**返回 []（不查库）", chunks == [])
        _check("★ 短路时 embedder **一次都没被调用**（不发起无意义的向量化 IO）",
               counting.calls == 0, str(counting.calls))
        _check("  └ 但仍记录 query（便于观测「确实没线索」）",
               short_circuit.queries == [""], str(short_circuit.queries))

    # -- 2e. resolve_retriever 不注入 ⇒ None（再由空实现兜底）--
    async with _env(documents=()) as env:
        retriever, warns = interview_core.resolve_retriever(env["session"], None, False)
        _check("★ resolve_retriever(db, None, use_rag=False) ⇒ (None, [])"
               "（不注入就不会悄悄打开 RAG）",
               retriever is None and warns == [], f"{retriever!r} / {warns}")
        _check("  └ 缺省不产生 warning", warns == [])


# ============================================================
# [3] 场景 2 · VectorStore 无匹配知识
# ============================================================
async def check_scenario_no_match() -> None:
    _section("[3] 场景 2 · VectorStore 无匹配知识")

    # -- 3a. 空表（store 里一条都没有）--
    async with _env(documents=()) as env:
        raw = await env["store"].search(
            await env["embedder"].embed("Redis 持久化"),
            top_k=5, model=env["embedder"].name, document_id=None,
            category=None, min_score=None,
        )
        _check("★ 空 store 的 search 直接返回 []（不是异常）", raw == [], str(len(raw)))
        _check("  └ 空 store 的 count() == 0", await env["store"].count() == 0,
               str(await env["store"].count()))

    # -- 3b. 库里有知识，但 topic 完全不匹配 + 有 min_score ⇒ 全被滤掉 --
    async with _env(documents=(FIXED_DOC,)) as env:
        report = env["reports"][0]
        _check("固定文档入库成功（库里确实有 1 条知识）",
               report["status"] == STATUS_OK and report["chunk_count"] == 1,
               f"status={report['status']} chunks={report['chunk_count']}")

        # 无阈值 ⇒ 不匹配的那条**仍会被返回**（这是实测事实，必须如实断言）
        loose = VectorKnowledgeRetriever(env["embedder"], env["store"], top_k=5,
                                         min_score=None)
        loose_chunks = await loose.retrieve(None, UNRELATED_TOPIC, CONTEXT)
        loose_score = (loose_chunks[0].metadata["score"] if loose_chunks else None)
        print(f"  · 词面不匹配的 query 实测得分 = {loose_score}"
              "（HashEmbeddingService 是**词面哈希**，score 与语义相关性不成正比——"
              "它只是「同一模型下可比较」，不是「越大越相关」）")
        _check("★ 无 min_score 时，词面不匹配的知识**仍会被返回**"
               "（store 只按相似度排序，不做相关性判断）",
               len(loose_chunks) == 1, f"score={loose_score}")
        _check("  └ 该得分明显低于 0.5 ⇒ 用 0.5 当阈值才有区分度",
               loose_score is not None and loose_score < 0.5, f"score={loose_score}")
        _check("  └ 说明「检索到东西」≠「检索到相关知识」，阈值才是分界线",
               len(loose_chunks) == 1 and loose_score is not None)

        strict = VectorKnowledgeRetriever(env["embedder"], env["store"], top_k=5,
                                          min_score=0.5)
        strict_chunks = await strict.retrieve(None, UNRELATED_TOPIC, CONTEXT)
        _check("★ min_score=0.5 把不匹配的那条滤掉 ⇒ 0 条",
               strict_chunks == [], str(len(strict_chunks)))

    # -- 3c. category / document_id 过滤后为空 --
    async with _env(documents=(FIXED_DOC,)) as env:
        by_category = VectorKnowledgeRetriever(
            env["embedder"], env["store"], top_k=5, category="company")
        _check("category='company'（库里只有 technical）⇒ 0 条",
               await by_category.retrieve(None, "Redis 持久化", CONTEXT) == [])
        by_doc = VectorKnowledgeRetriever(
            env["embedder"], env["store"], top_k=5, document_id=999999)
        _check("document_id=999999（不存在）⇒ 0 条",
               await by_doc.retrieve(None, "Redis 持久化", CONTEXT) == [])
        by_model = VectorKnowledgeRetriever(
            env["embedder"], env["store"], top_k=5, model="another-model")
        _check("model 不匹配（库里是 hash-local）⇒ 0 条",
               await by_model.retrieve(None, "Redis 持久化", CONTEXT) == [])


# ============================================================
# [4] knowledge_context 为空（不是 None / 不是 [""]）
# ============================================================
async def check_empty_context_shape() -> None:
    _section("[4] knowledge_context 为空的确切形状")

    for label, retriever in (
        ("默认空实现", KnowledgeRetriever()),
        ("自定义恒空", EmptyRetriever()),
    ):
        got, warnings = await interview_core._gather_knowledge(
            JOB, PLAN, CONTEXT, retriever)
        _check(f"★ [{label}] knowledge_context == []（空列表）", got == [], repr(got))
        _check(f"  └ 是 list，不是 None", isinstance(got, list) and got is not None)
        _check(f"  └ 不是 [''] / 不是含空串的列表（无伪造占位）",
               got == [] and "" not in got)
        _check(f"  └ warnings == []", warnings == [])

    # 归一化层：空输入必须归一为 []
    for label, value in (
        ("None", None), ("空列表", []), ("空元组", ()), ("空字符串", ""),
        ("全空元素列表", [None, "", {}]),
    ):
        _check(f"normalize_knowledge_context({label}) == []",
               interview_agent.normalize_knowledge_context(value) == [])

    # 真实空库路径
    async with _env(documents=()) as env:
        retriever = VectorKnowledgeRetriever(env["embedder"], env["store"], top_k=5)
        got, warnings = await interview_core._gather_knowledge(
            JOB, PLAN, CONTEXT, retriever)
        _check("★ 真实空库 ⇒ knowledge_context == [] 且无 warning",
               got == [] and warnings == [], f"{got!r} / {warnings}")

    # 空知识 ⇒ Agent 侧走原模板（不是「带空小节的变体模板」）
    variables = build_question_variables(CONTEXT, PLAN, RESUME, JOB)
    name, prompt = render_question_prompt(variables, [])
    _check("★ 空知识 ⇒ 模板名是 question（不是 question_knowledge）",
           name == PROMPT_QUESTION, name)
    _check("★ 空知识 Prompt **不含**任何知识痕迹（含无「（无）」占位）",
           _no_knowledge_trace(prompt))
    _check("  └ 也不含 knowledge_context 变量名", "knowledge_context" not in prompt)


# ============================================================
# [5] Agent 流程继续（核心）
# ============================================================
async def check_agent_continues() -> None:
    _section("[5] Agent 流程继续（核心）")

    async with _env(documents=()) as env:
        # -- 5a. 四种「无知识」写法都必须出题成功 --
        variants: Dict[str, str] = {}
        for label, kwargs in (
            ("不注入检索器", {}),
            ("注入空实现", {"retriever": KnowledgeRetriever()}),
            ("注入恒空检索器", {"retriever": EmptyRetriever()}),
            ("显式 use_rag=False", {"use_rag": False}),
        ):
            spy = SpySpark(QUESTION_REPLY)
            result = await _run_core(env, spark=spy, **kwargs)
            variants[label] = spy.calls[0]
            _check(f"★ [{label}] 仍能出题成功（ok=True）", result["ok"] is True,
                   f"errors={result['errors']} error={result['error']}")
            _check(f"  └ errors == []", result["errors"] == [], str(result["errors"]))
            _check(f"  └ warnings == []（无知识不是故障）", result["warnings"] == [],
                   str(result["warnings"]))
            _check(f"  └ 仍调用模型恰好一次", len(spy.calls) == 1, str(len(spy.calls)))
            _check(f"  └ 返回字段集恒定（12 键）",
                   tuple(result) == interview_core.QUESTION_RESULT_FIELDS)
            _check(f"  └ 题目字段正常（question / topic / difficulty）",
                   bool(result["question"]) and result["topic"] == "Redis 持久化"
                   and result["difficulty"] == "mid",
                   f"q={result['question'][:16]!r} topic={result['topic']}")
            _check(f"  └ Prompt 无知识痕迹", _no_knowledge_trace(spy.calls[0]))
            _check(f"  └ Prompt 无未渲染占位符", "{{" not in spy.calls[0])

        # -- 5b. ★ 最强断言：四种写法的 Prompt 逐字节相同 --
        _check("★ 四种「无知识」写法产出的 Prompt **逐字节相同**",
               len(set(variants.values())) == 1,
               f"去重后 {len(set(variants.values()))} 种 / 长度 "
               f"{sorted({len(v) for v in variants.values()})}")

        # -- 5c. ★ 且等于 Agent 直接不传 knowledge_context 的渲染结果 --
        # .. note:: 这里必须用 ``None, None`` 当 resume / job——
        #    Core 路径上会话的 ``resume_id`` / ``job_id`` 都是 None，
        #    ``load_resume_row`` / ``load_job_row`` 返回 None 并**原样**传给 Agent。
        #    若这里传 RESUME / JOB，比的就是**两组不同输入**渲染出的 Prompt
        #    （本套件第一版就栽在这里：Prompt 长度对不上）。
        direct = render_question_prompt(
            build_question_variables(CONTEXT, PLAN, None, None), None)[1]
        _check("★ 与「Agent 直接不传 knowledge_context」的 Prompt 逐字节相同"
               "（= 无知识时行为与引入知识能力前完全一致）",
               variants["不注入检索器"] == direct,
               f"长度 {len(variants['不注入检索器'])} vs {len(direct)}")

    # -- 5d. 空知识 + 首次输出非法 JSON ⇒ 修复路径照常可用 --
    async with _env(documents=()) as env:
        spy = SpySpark("这不是 JSON", QUESTION_REPLY)
        result = await _run_core(env, spark=spy, retriever=KnowledgeRetriever())
        _check("★ 无知识时非法 JSON 仍能修复成功", result["ok"] is True,
               str(result.get("error")))
        _check("  └ 共调用 2 次（首次 + 1 次修复）", len(spy.calls) == 2,
               str(len(spy.calls)))
        _check("  └ 两次 Prompt 都无知识痕迹",
               _no_knowledge_trace(spy.calls[0]) and _no_knowledge_trace(spy.calls[1]))
        _check("  └ 修复后仍无 knowledge_retrieval_failed 警告",
               result["warnings"] == [], str(result["warnings"]))

    # -- 5e. 模型返回失败哨兵 ⇒ 出题失败，但**不是**被知识拖累 --
    async with _env(documents=()) as env:
        spy = SpySpark("API调用失败：模拟故障")
        result = await _run_core(env, spark=spy, retriever=KnowledgeRetriever())
        _check("模型服务故障 ⇒ ok=False 且错误码是 agent_failed（与知识无关）",
               result["ok"] is False
               and interview_core.ERROR_AGENT_FAILED in result["errors"],
               f"ok={result['ok']} errors={result['errors']}")
        _check("  └ 失败时 question 恒为空串（绝不放行伪造问题）",
               result["question"] == "", repr(result["question"]))
        _check("  └ 失败结果字段集仍恒定（12 键）",
               tuple(result) == interview_core.QUESTION_RESULT_FIELDS)

    # -- 5f. Validator 在无知识时照常工作（未修改 Validator，只是被调用）--
    async with _env(documents=()) as env:
        validation = interview_core.validate_candidate_question(
            {"question": "请说明 Redis 持久化。", "topic": "Redis 持久化",
             "difficulty": "mid", "question_type": "technical",
             "expected_points": ["RDB"], "reason": "考察原理"},
            CONTEXT, PLAN,
        )
        _check("★ Validator 在无知识路径上照常校验通过",
               validation.valid is True, str(validation.errors))
        _check("  └ 产出规范化的 6 键问题",
               set(validation.normalized_question) == {
                   "question", "question_type", "topic", "difficulty",
                   "expected_points", "reason"},
               str(sorted(validation.normalized_question)))

        # 非法候选必须被拒（证明 Validator 不是「恒通过」）
        bad = interview_core.validate_candidate_question(
            {"question": "", "topic": "", "difficulty": "mid"}, CONTEXT, PLAN)
        _check("  └ 空 question 的候选被拒（Validator 不是恒通过）",
               bad.valid is False and bad.errors, str(bad.errors))


# ============================================================
# [6] 三种「没有知识」的成因必须分开
# ============================================================
async def check_degradation_matrix() -> None:
    _section("[6] 三种「没有知识」的成因分开（errors / warnings / 静默）")

    async with _env(documents=()) as env:
        rows: List[Tuple[str, Dict[str, Any], str]] = []

        # ① 检索抛异常 —— 服务故障
        spy1 = SpySpark(QUESTION_REPLY)
        r1 = await _run_core(env, spark=spy1, retriever=BoomRetriever())
        rows.append(("检索抛异常（服务故障）", r1, "warning"))

        # ② 检索正常但没结果 —— 不是故障
        spy2 = SpySpark(QUESTION_REPLY)
        r2 = await _run_core(env, spark=spy2, retriever=EmptyRetriever())
        rows.append(("检索正常但没结果", r2, "silent"))

        # ③ 不注入检索器 —— 默认安全态
        spy3 = SpySpark(QUESTION_REPLY)
        r3 = await _run_core(env, spark=spy3)
        rows.append(("不注入检索器（默认）", r3, "silent"))

        for label, result, expected in rows:
            _check(f"[{label}] 出题**仍然成功**（ok=True）", result["ok"] is True,
                   f"errors={result['errors']}")
            _check(f"  └ errors == []（没有知识不是错误）",
                   result["errors"] == [], str(result["errors"]))
            if expected == "warning":
                _check("  └ ★ 只记 warning knowledge_retrieval_failed",
                       result["warnings"] == [interview_core.WARNING_KNOWLEDGE_FAILED],
                       str(result["warnings"]))
            else:
                _check("  └ ★ 无任何 warning（静默降级）",
                       result["warnings"] == [], str(result["warnings"]))

        # 三种情形的 Prompt 也必须一致（知识一律为空 ⇒ 一律走原模板）
        prompts = [spy.calls[0] for spy in (spy1, spy2, spy3)]
        _check("★ 三种成因的 Prompt 逐字节相同（都回落原模板）",
               len(set(prompts)) == 1, str(sorted({len(p) for p in prompts})))
        _check("  └ 三份 Prompt 都无知识痕迹",
               all(_no_knowledge_trace(p) for p in prompts))

    # 对照：**有**知识时 Prompt 必然不同（证明上面不是「恒相同」的假象）
    # .. important:: 对照组必须**注入真实检索器**——``_run_core`` 缺省走
    #    ``resolve_retriever(db, None, False) → None``（空实现 ⇒ 无知识），
    #    光把文档入库不会自动接上检索（「不注入就不接」是刻意的安全默认）。
    async with _env(documents=(FIXED_DOC,)) as env:
        retriever = VectorKnowledgeRetriever(env["embedder"], env["store"], top_k=5)
        chunks = await retriever.retrieve(JOB, "Redis 持久化", CONTEXT)
        _check("对照组前置：真实检索器确实取到了知识", len(chunks) == 1,
               str(len(chunks)))
        spy = SpySpark(QUESTION_REPLY)
        with_knowledge = await _run_core(env, spark=spy, retriever=retriever)
        _check("对照组：有知识时出题成功且 warning 为空",
               with_knowledge["ok"] is True and with_knowledge["warnings"] == [],
               str(with_knowledge["warnings"]))
        _check("★ 对照组：有知识的 Prompt **含**参考知识小节（判据不是恒真）",
               "参考知识" in spy.calls[0])
        _check("★ 对照组：有知识的 Prompt 与无知识时**不同**",
               spy.calls[0] != prompts[0])

    # 有库、有阈值、但阈值把唯一一条滤掉 ⇒ 等价于无知识
    # .. note:: 这里用**同一个 PLAN**（不换 topic），只靠 min_score 滤空——
    #    这样 Prompt 里 ``interview_plan`` 那段载荷与上文完全一致，
    #    才**能**做逐字节比较（换 topic 会连带改掉 plan 载荷，比不了）。
    async with _env(documents=(FIXED_DOC,)) as env:
        strict = VectorKnowledgeRetriever(env["embedder"], env["store"],
                                          top_k=5, min_score=0.5)
        _check("前置：min_score=0.5 确实把该条滤空（否则下面的比较无意义）",
               await strict.retrieve(JOB, "Redis 持久化", CONTEXT) == [])
        spy = SpySpark(QUESTION_REPLY)
        result = await _run_core(env, spark=spy, retriever=strict)
        _check("有库但阈值滤空 ⇒ 出题成功且无 warning",
               result["ok"] is True and result["warnings"] == [],
               f"ok={result['ok']} warnings={result['warnings']}")
        _check("  └ Prompt 回落原模板（无知识痕迹）", _no_knowledge_trace(spy.calls[0]))
        _check("★ 且与「完全空库」的 Prompt 逐字节相同",
               spy.calls[0] == prompts[0],
               f"长度 {len(spy.calls[0])} vs {len(prompts[0])}")


# ============================================================
# 报告
# ============================================================
def report() -> None:
    print("\n" + "=" * 74)
    print("测试场景总览")
    print("=" * 74)
    rows = (
        ("1", "Retriever 返回空结果", "默认空实现 / 自定义恒空 / 真实空库 / 空查询短路"),
        ("2", "VectorStore 无匹配知识", "空表 / min_score 滤空 / category / document_id / model"),
        ("3", "knowledge_context 形状", "恰好 []（非 None、非 ['']）"),
        ("4", "Agent 流程继续", "出题成功 / 调模型一次 / 原模板 / 无知识痕迹"),
        ("5", "Prompt 逐字节一致", "四种「无知识」写法 == 不注入检索器"),
        ("6", "降级成因分开", "抛异常→warning；没结果/不注入→静默"),
    )
    for index, name, criterion in rows:
        print(f"  [{index}] {name:<24} {criterion}")
    print("\n" + "=" * 74)
    print("当前降级行为")
    print("=" * 74)
    print("  检索失败（抛异常）  → knowledge_context=[] + warnings=[knowledge_retrieval_failed]")
    print("  检索无结果（[]）    → knowledge_context=[] + warnings=[]（静默）")
    print("  不注入 / use_rag=False → knowledge_context=[] + warnings=[]（默认安全态）")
    print("  三种情形：ok=True、errors=[]、Prompt 回落 question.txt、与不注入时逐字节相同")
    print("  失败不伪造：question 恒为 ''；知识问题绝不进 errors")
    print(f"  断言：通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)


async def main() -> None:
    print("=" * 74)
    print("RAG 无结果时 InterviewAgent 是否正常运行")
    print("=" * 74)

    check_preconditions()
    await check_scenario_empty_retriever()
    await check_scenario_no_match()
    await check_empty_context_shape()
    await check_agent_continues()
    await check_degradation_matrix()
    report()


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(0 if _FAILED == 0 else 1)
