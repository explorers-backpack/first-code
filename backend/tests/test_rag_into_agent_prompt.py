# -*- coding: utf-8 -*-
"""RAG 检索结果 → InterviewAgent → 最终 Prompt 的端到端注入验证（脚本式，非 pytest）。

运行：``python backend/tests/test_rag_into_agent_prompt.py``

验证的三跳链路
--------------
::

    KnowledgeRetriever.retrieve()          ← 真实 VectorKnowledgeRetriever
        ↓  固定知识 chunk（入库 → 检索）
    interview_core._gather_knowledge()     ← Core 接缝
        ↓
    InterviewAgent.generate_question(knowledge_context=...)
        ↓  render_question_prompt（唯一分流点）
    prompts/interview/question_knowledge.txt 渲染出的**最终 Prompt**

三项检查
--------
1. **Agent 接收到 knowledge_context** —— 检索器输出经 Core 原样到达 Agent
2. **Prompt 包含知识内容** —— 实际发给模型的 Prompt 里有知识正文与来源
3. **知识格式正确** —— 每条一行 ``- 正文（来源：…）``；分数**不**渗入；
   无未渲染占位符；知识只出现在「参考知识」小节内

与 ``tests/test_agent_knowledge.py`` 的分工
-------------------------------------------
``test_agent_knowledge.py`` 用**鸭子类型假片段**（``_Chunk`` / 裸字符串）验证
Agent 侧的归一化与模板分流；本套件补的是它没覆盖的那一半——
**从真实检索器出发**：真实 ``HashEmbeddingService`` + 真实
``SqlAlchemyVectorStore`` + 真实 ``VectorKnowledgeRetriever``，
喂一个**固定知识 chunk**，一路验到最终 Prompt。

不要修改
--------
``Retriever`` / ``VectorStore`` / ``Embedding`` 三类模块的**源码指纹**在 [1] 锁死
（改一个字符即失败）⇒ 「只新增测试、未动生产代码」是**被验证**的。

.. note::
   实测结论是「**注入本来就是正确的**」，因此本套件**没有**改动任何 Prompt
   组装代码——用户给定的修复触发条件是「发现未注入」，该条件未成立。
   [5] 如实记录了一处**格式瑕疵**（多段落切片的项目符号）作为现状快照，
   但**未**擅自修改（那属于业务效果变更，需要明确授权）。
"""

from __future__ import annotations

import ast
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
from models import InterviewSession, User  # noqa: E402
from services import interview_agent, interview_core  # noqa: E402
from services.interview_agent import (  # noqa: E402
    PROMPT_QUESTION,
    PROMPT_QUESTION_KNOWLEDGE,
    build_question_variables,
    normalize_knowledge_context,
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
#: 「不要修改」的判据：这三类模块的**源码指纹**（规范化换行后的 sha256）。
#: 用**源码文本**而不是 AST——``ast.dump`` 的输出随 Python 版本变化
#: （3.12 给 ``FunctionDef`` 加了 ``type_params``），换解释器就会假失败。
FROZEN_SOURCES: Tuple[Tuple[str, str], ...] = (
    ("services/knowledge_retriever.py",
     "dcf74a93fbdf8bf4d226c52ab67f881fe374ade0945fe98ed0e81302929cb8cc"),
    ("services/vector_knowledge_retriever.py",
     "477782f1840a83d3a22347c7d3607c54a27850c37c06e9fb1b2ce304aece43e5"),
    ("services/vector_store.py",
     "0dd7c96fe9f5534f300850f8b98549a70170a640e11d409ca022e773df303e33"),
    ("services/vector_store_sql.py",
     "a81dd62411a1144798cac60b5525e9d43936a1d385585d7b6a1f1292d4c34fdc"),
    ("services/vector_store_chroma.py",
     "109e760e60962a663a80201b6c002abd636b0d107c6206a2d533a19b20fb0dd9"),
    ("services/embedding_service.py",
     "a7bd287ea5b25d33bb3b95567e6a344c90238d49babab8d98313cbc376bb123e"),
    ("services/embedding_provider.py",
     "d4e84cb89995adf267913aff296296bc3d8d2c798b251937c3f0d1ec4bada19e"),
)

#: 固定知识 chunk 的来源文档。**逐字节固定**——整份套件的期望值都由它推导。
#:
#: .. important::
#:    ``content`` **刻意是单段落**（不含 ``\n\n``）。原因：``stringify()`` 对字符串
#:    列表逐条输出 ``- {正文}``，而一条**内部含空行**的正文会被渲染成多行、
#:    且 ``（来源：…）`` 只附在**最后一段**末尾（见 [5] 的现状快照）。
#:    本套件要验的是「格式正确」，因此固定 chunk 必须取**规范形态**；
#:    多段落那一档单独在 [5] 里如实记录，不混进来污染主断言。
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

#: **多段落**文档：用来验证「一个切片内部含空行」时的渲染现状。
#: ``document_chunker`` 按段落边界切分后会**合并**短段落，
#: 因此一个 chunk 的 ``content`` 里出现 ``\n\n`` 是**可达的**，不是臆造的边界。
MULTI_PARA_DOC: Dict[str, str] = {
    "title": "多段落手册",
    "category": "technical",
    "source": "handbook://multi-paragraph",
    "content": (
        "第一段：Redis 持久化有两种机制。\n\n"
        "第二段：RDB 是全量快照，AOF 记录写命令。\n\n"
        "第三段：生产上常两者混用。"
    ),
}
MULTI_PARA_SOURCE = MULTI_PARA_DOC["source"]

#: 检索用的知识点。**必须与固定 chunk 正文有词面重叠**——
#: 默认 embedder 是 ``HashEmbeddingService``（``semantic_enabled=False``，词面检索）。
FIXED_TOPIC = "Redis 持久化"

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
    # ``priority_topics[0]`` 会成为 ``interview_core.current_topic`` 的结果，
    # 也就是检索器实际收到的 topic ⇒ 必须指向固定 chunk。
    "priority_topics": [FIXED_TOPIC],
}
JOB: Dict[str, Any] = {"job_name": "后端开发工程师", "duty": "负责后端服务的设计与开发"}
RESUME: Dict[str, Any] = {"content": "3 年后端经验，技术栈 Python / MySQL / Redis"}

#: Mock Spark 的回复（合法 JSON，字段齐全 ⇒ 不会被校验打回）。
QUESTION_REPLY = json.dumps(
    {
        "question": "请对比 Redis 的 RDB 与 AOF 两种持久化机制，你会怎么选？",
        "question_type": "technical",
        "topic": FIXED_TOPIC,
        "difficulty": "mid",
        "expected_points": ["RDB", "AOF", "丢失窗口"],
        "reason": "考察岗位要求中的缓存中间件原理",
    },
    ensure_ascii=False,
)

#: 模板里「参考知识」小节的标题与标签（渲染后应逐字出现）。
KNOWLEDGE_HEADING = "## 四·五、参考知识（外部检索结果，可选）"
KNOWLEDGE_LABEL = "参考知识："
#: 小节结束的锚点（下一节的标题）。
KNOWLEDGE_END = "## 五、本场进度"

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
    """记录每次收到的 Prompt；按顺序吐出回复，超出即报错（用来断言调用次数）。"""

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


class SpyRetriever:
    """记录调用入参、返回固定结果的检索器（用来验「接缝」而非「检索质量」）。"""

    def __init__(self, chunks: Sequence[Any]) -> None:
        self.chunks = list(chunks)
        self.calls: List[Tuple[Any, Any, Any]] = []

    async def retrieve(self, job_info: Any, topic: Any, context: Any) -> List[Any]:
        self.calls.append((job_info, topic, context))
        return list(self.chunks)


class BoomRetriever:
    """检索即抛异常的检索器（验证「检索失败 ⇒ 降级为无知识，不中断出题」）。"""

    def __init__(self, message: str = "检索服务挂了") -> None:
        self.message = message

    async def retrieve(self, job_info: Any, topic: Any, context: Any) -> List[Any]:
        raise RuntimeError(self.message)


def _fingerprint(relative_path: str) -> str:
    """规范化换行后的源码 sha256。"""
    raw = (BACKEND_DIR / relative_path).read_bytes()
    return hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def _module_source(relative_path: str) -> str:
    return (BACKEND_DIR / relative_path).read_text(encoding="utf-8").replace("\r\n", "\n")


def _call_keywords(source: str, func_name: str) -> Tuple[bool, set]:
    """源码里对 ``func_name`` 的调用：返回 ``(是否被调用, 关键字参数名集合)``。

    用 AST 而不是子串匹配——docstring 与注释里也会出现函数名。
    """
    called = False
    keywords: set = set()
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        name = target.id if isinstance(target, ast.Name) else getattr(target, "attr", None)
        if name != func_name:
            continue
        called = True
        keywords.update(kw.arg for kw in node.keywords if kw.arg)
    return called, keywords


def _knowledge_section(prompt: str) -> str:
    """截出「参考知识」小节正文（从标题到下一节标题）。"""
    start = prompt.index(KNOWLEDGE_HEADING)
    end = prompt.index(KNOWLEDGE_END)
    return prompt[start:end]


def _bullet_lines(text: str) -> List[str]:
    return [line for line in text.splitlines() if line.startswith("- ")]


def _base_variables() -> Dict[str, Any]:
    return build_question_variables(CONTEXT, PLAN, RESUME, JOB)


# ============================================================
# 三、环境：真实 SQLite + 真实 store + 真实 embedder + 真实检索器
# ============================================================
@asynccontextmanager
async def _rag_env(*, documents: Sequence[Dict[str, str]] = (FIXED_DOC,),
                   top_k: int = 3, min_score: Optional[float] = None,
                   with_session: bool = False) -> Iterator[Dict[str, Any]]:
    """构建一套**全真实**的 RAG 环境（只把 Spark 换成 Mock）。

    ``with_session=True`` 时额外造一条 ``InterviewSession`` 行，
    供 :func:`interview_core.generate_next_question` 端到端使用。
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

        session_id: Optional[int] = None
        if with_session:
            session.add(User(username="rag_inject", email="rag_inject@example.com",
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
        pipeline = KnowledgeImportPipeline(session, embedder=embedder, store=store)
        reports = [await pipeline.import_document(dict(doc)) for doc in documents]

        yield {
            "session": session,
            "session_id": session_id,
            "embedder": embedder,
            "store": store,
            "reports": reports,
            "retriever": VectorKnowledgeRetriever(
                embedder, store, top_k=top_k, min_score=min_score,
            ),
        }
    finally:
        if session is not None:
            await session.close()
        await engine.dispose()


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
    probe = FIXED_DOC["content"] + "（被改过）"
    _check("守卫自检：内容变化会改变指纹",
           hashlib.sha256(probe.encode("utf-8")).hexdigest()
           != hashlib.sha256(FIXED_DOC["content"].encode("utf-8")).hexdigest())

    # 默认 embedder 是词面检索 ⇒ 期望值必须建立在「词面重叠」上，不能指望语义召回
    from services.embedding_service import describe_embedding

    info = describe_embedding(default_embedder())
    _check("默认 embedder 是离线词面实现（语义检索未启用）",
           info.semantic_enabled is False, f"provider={info.provider} dim={info.dimension}")
    _check("★ 因此检索用 topic 必须与固定 chunk 有词面重叠（本套件用固定 topic 兜住）",
           any(token in FIXED_DOC["content"] for token in ("Redis", "持久化")),
           FIXED_TOPIC)

    # 接线守卫：Core 的两跳都要把 knowledge_context 传下去（AST，不是子串）
    core_src = _module_source("services/interview_core.py")
    called, keywords = _call_keywords(core_src, "generate_question")
    _check("★ interview_core 确实调用 interview_agent.generate_question", called)
    _check("★ 且以 knowledge_context= 关键字传入（不是漏传/位置错位）",
           "knowledge_context" in keywords, str(sorted(keywords)))
    called2, keywords2 = _call_keywords(core_src, "generate_candidate_question")
    _check("★ generate_next_question 也以 knowledge_context= 调 generate_candidate_question",
           called2 and "knowledge_context" in keywords2, str(sorted(keywords2)))

    # Agent 侧契约
    sig = inspect.signature(interview_agent.generate_question)
    _check("Agent 的 knowledge_context 是 keyword-only（避免位置传参错位）",
           sig.parameters["knowledge_context"].kind is inspect.Parameter.KEYWORD_ONLY)
    _check("Agent 的 knowledge_context 默认 None（不用可变默认值）",
           sig.parameters["knowledge_context"].default is None)
    _check("Agent 签名里没有 retriever 参数（Agent 不持有检索器）",
           "retriever" not in sig.parameters, str(list(sig.parameters)))


# ============================================================
# [2] 第 1 跳 · KnowledgeRetriever 返回内容
# ============================================================
async def check_retriever_output() -> None:
    _section("[2] 第 1 跳 · KnowledgeRetriever 返回内容（真实实现）")

    async with _rag_env(top_k=3, min_score=None) as env:
        report = env["reports"][0]
        _check("固定知识 chunk 入库成功", report["status"] == STATUS_OK,
               f"status={report['status']} stage={report['stage']}")
        _check("入库产出 1 个切片", report["chunk_count"] == 1, str(report["chunk_count"]))

        retriever = env["retriever"]
        _check("检索器是真实 VectorKnowledgeRetriever",
               type(retriever).__name__ == "VectorKnowledgeRetriever",
               type(retriever).__name__)
        _check("底层是真实 SqlAlchemyVectorStore（不是 InMemory 替身）",
               type(env["store"]).__name__ == "SqlAlchemyVectorStore",
               type(env["store"]).__name__)

        chunks = await retriever.retrieve(JOB, FIXED_TOPIC, CONTEXT)
        _check("检索到 1 条知识", len(chunks) == 1, str(len(chunks)))
        _check("检索器收到的 topic == plan.priority_topics[0]（Core 的 current_topic）",
               retriever.queries == [FIXED_TOPIC], str(retriever.queries))

        chunk = chunks[0]
        _check("返回类型是 knowledge_retriever.KnowledgeChunk",
               isinstance(chunk, KnowledgeChunk), type(chunk).__name__)
        _check("★ content == 固定知识正文（逐字节，检索不改写原文）",
               chunk.content == FIXED_DOC["content"], repr(chunk.content[:40]))
        _check("★ source == 文档的 source（从 metadata 提升而来）",
               chunk.source == FIXED_SOURCE, repr(chunk.source))
        _check("metadata 保留溯源信息（chunk_id / document_id / score）",
               {"chunk_id", "document_id", "score"} <= set(chunk.metadata),
               str(sorted(chunk.metadata)))
        _check("KnowledgeChunk.to_dict() 恒为三键",
               set(chunk.to_dict()) == {"content", "source", "metadata"},
               str(sorted(chunk.to_dict())))

        # 检索器输出必须是「新建对象」，且可被序列化（不携带向量本体）
        _check("检索结果不含向量本体（不把 256 个浮点带进 Prompt 预算）",
               "vector" not in chunk.metadata and "embedding" not in chunk.metadata,
               str(sorted(chunk.metadata)))
        _check("同输入同输出（重复检索结果一致）",
               await retriever.retrieve(JOB, FIXED_TOPIC, CONTEXT) == chunks)


# ============================================================
# [3] 第 2 跳 · Agent 接收到 knowledge_context（检查 1）
# ============================================================
async def check_agent_receives() -> None:
    _section("[3] 第 2 跳 · Agent 接收到 knowledge_context（检查 1）")

    async with _rag_env(top_k=3, min_score=None) as env:
        retriever = env["retriever"]
        chunks = await retriever.retrieve(JOB, FIXED_TOPIC, CONTEXT)

        # -- Core 接缝：检索器输出必须原样（逐条相等）出来 --
        gathered, warnings = await interview_core._gather_knowledge(
            JOB, PLAN, CONTEXT, retriever
        )
        _check("★ _gather_knowledge 返回检索器的全部结果（条数一致）",
               len(gathered) == len(chunks), f"{len(gathered)} vs {len(chunks)}")
        _check("★ 且内容逐条相等（Core 不裁剪、不改写、不重排）",
               gathered == chunks)
        _check("成功检索不产生 warning", warnings == [], str(warnings))
        _check("_gather_knowledge 把 topic 传给检索器（不是空串）",
               retriever.queries[-1] == FIXED_TOPIC, str(retriever.queries[-1]))

        # -- Core 接缝：generate_candidate_question 透传给 Agent --
        spy = SpySpark(QUESTION_REPLY)
        result = await interview_core.generate_candidate_question(
            CONTEXT, PLAN, RESUME, JOB, knowledge_context=gathered, spark=spy,
        )
        _check("经 generate_candidate_question 出题成功", result["ok"] is True,
               str(result.get("error")))
        _check("★ Agent 侧最终 Prompt 含知识正文 ⇒ knowledge_context 确实到达了 Agent",
               FIXED_DOC["content"] in spy.calls[0])

        # -- Agent 层直接调用：接收到的知识决定用哪个模板 --
        spy2 = SpySpark(QUESTION_REPLY)
        await interview_agent.generate_question(
            CONTEXT, PLAN, RESUME, JOB, knowledge_context=chunks, spark=spy2,
        )
        _check("Agent 收到非空知识 ⇒ 走变体模板（Prompt 含参考知识小节）",
               KNOWLEDGE_HEADING in spy2.calls[0])

        spy3 = SpySpark(QUESTION_REPLY)
        await interview_agent.generate_question(
            CONTEXT, PLAN, RESUME, JOB, knowledge_context=[], spark=spy3,
        )
        _check("Agent 收到空知识 ⇒ 走原模板（Prompt 不含参考知识小节）",
               KNOWLEDGE_HEADING not in spy3.calls[0])

        _check("两种输入的 Prompt 确实不同（分流生效）",
               spy2.calls[0] != spy3.calls[0])

        # -- 归一化保留 source --
        lines = normalize_knowledge_context(chunks)
        _check("归一化后恰好 1 行", len(lines) == 1, str(len(lines)))
        _check("★ 归一化把 source 内联进同一行",
               lines[0] == f"{FIXED_DOC['content']}（来源：{FIXED_SOURCE}）",
               repr(lines[0][:60]))


# ============================================================
# [4] 第 3 跳 · Prompt 包含知识内容（检查 2）
# ============================================================
async def check_prompt_contains() -> None:
    _section("[4] 第 3 跳 · Prompt 包含知识内容（检查 2）")

    async with _rag_env(top_k=3, min_score=None) as env:
        chunks = await env["retriever"].retrieve(JOB, FIXED_TOPIC, CONTEXT)
        variables = _base_variables()

        name, prompt = render_question_prompt(variables, chunks)
        _check("模板名 == question_knowledge", name == PROMPT_QUESTION_KNOWLEDGE, name)

        _check("★ Prompt 含知识正文（逐字节，不被截断/改写）",
               FIXED_DOC["content"] in prompt)
        _check("★ Prompt 含来源标注", FIXED_SOURCE in prompt)
        _check("★ Prompt 含「参考知识：」标签", KNOWLEDGE_LABEL in prompt)
        _check("★ Prompt 含知识小节标题", KNOWLEDGE_HEADING in prompt)

        # 知识必须落在这个小节里，而不是散落别处
        section = _knowledge_section(prompt)
        _check("知识正文落在「参考知识」小节**之内**",
               FIXED_DOC["content"] in section)
        _check("知识正文在整个 Prompt 里只出现 1 次（未重复注入）",
               prompt.count(FIXED_DOC["content"]) == 1,
               str(prompt.count(FIXED_DOC["content"])))
        _check("小节标题只出现 1 次", prompt.count(KNOWLEDGE_HEADING) == 1,
               str(prompt.count(KNOWLEDGE_HEADING)))

        # 原有 10 个变量的内容必须都还在（注入知识不能顶掉既有上下文）
        for label, needle in (
            ("岗位名", "后端开发工程师"),
            ("知识点", FIXED_TOPIC),
            ("阶段", "technical"),
            ("难度", "mid"),
        ):
            _check(f"注入知识后仍保留原有变量内容（{label}）", needle in prompt)

        _check("★ 无未渲染占位符（{{ 已全部替换）", "{{" not in prompt,
               prompt[prompt.index("{{") - 20:prompt.index("{{") + 30] if "{{" in prompt else "")

        # 与「无知识」的 Prompt 做对照：差异应当只有那一处小节
        _, legacy = render_question_prompt(variables, None)
        _check("无知识时用原模板", render_question_prompt(variables, None)[0] == PROMPT_QUESTION)
        _check("有知识 Prompt ≠ 无知识 Prompt", prompt != legacy)
        # 把知识小节替换成「小节标题行 + 占位符」即可还原成原模板的**同一份骨架**
        # （逐字节等价关系由 test_agent_knowledge.py [4] 的漂移守卫锁死，此处不重复）
        _check("★ 有知识 Prompt 比无知识 Prompt 只多出知识小节的内容",
               prompt.replace(f"{KNOWLEDGE_HEADING}\n\n{KNOWLEDGE_LABEL}\n"
                              f"- {FIXED_DOC['content']}（来源：{FIXED_SOURCE}）\n\n"
                              "出题时请把这些知识融入问题的场景与追问点，但不得整段照抄进 `question`，\n"
                              "也不得把知识中的结论直接写进 `question`（`expected_points` 可参考）。\n\n",
                              "") == legacy,
               "删掉知识小节后应逐字节等于无知识 Prompt")

        # 端到端：实际发给模型的 Prompt（不是我们手工渲染的那份）
        spy = SpySpark(QUESTION_REPLY)
        result = await interview_agent.generate_question(
            CONTEXT, PLAN, RESUME, JOB, knowledge_context=chunks, spark=spy,
        )
        _check("端到端出题成功（ok=True）", result["ok"] is True, str(result.get("error")))
        _check("★ **实际发给模型的** Prompt 含知识正文",
               FIXED_DOC["content"] in spy.calls[0])
        _check("实际 Prompt 含来源标注", FIXED_SOURCE in spy.calls[0])
        _check("只调用一次模型（知识不额外触发 LLM）", len(spy.calls) == 1, str(len(spy.calls)))
        _check("返回值字段集不变（8 键，调用方无需分支）",
               set(result) == {"ok", "question", "question_type", "topic", "difficulty",
                               "expected_points", "reason", "error"},
               str(sorted(result)))


# ============================================================
# [5] 知识格式正确（检查 3）
# ============================================================
async def check_knowledge_format() -> None:
    _section("[5] 知识格式正确（检查 3）")

    async with _rag_env(top_k=3, min_score=None) as env:
        chunks = await env["retriever"].retrieve(JOB, FIXED_TOPIC, CONTEXT)
        variables = _base_variables()
        _, prompt = render_question_prompt(variables, chunks)
        section = _knowledge_section(prompt)

        print("  ---- 「参考知识」小节原文 ----")
        for line in section.splitlines():
            print(f"  | {line}")
        print("  ---- 小节结束 ----")

        bullets = _bullet_lines(section)
        _check("小节里有且仅有 1 个项目符号行", len(bullets) == 1, str(len(bullets)))
        _check("★ 项目符号行 == '- 正文（来源：…）'（逐字节）",
               bullets[0] == f"- {FIXED_DOC['content']}（来源：{FIXED_SOURCE}）",
               repr(bullets[0][:70]))
        _check("★ 格式为「- 正文（来源：…）」：以 '- ' 开头", bullets[0].startswith("- "))
        _check("★ 来源用中文括号包裹、附在正文之后",
               bullets[0].endswith(f"（来源：{FIXED_SOURCE}）"))
        _check("正文与来源之间无多余分隔符（不是 '- 正文 - 来源'）",
               bullets[0].count("（来源：") == 1)

        # ★ 分数绝不能渗进 Prompt（``score`` 只在 metadata 里，归一化时被丢弃）
        _check("★ score 数值不出现在 Prompt 里（检索分数不进 Prompt）",
               str(chunks[0].metadata["score"]) not in prompt,
               str(chunks[0].metadata["score"]))
        _check("★ metadata 的键名也不出现（chunk_id / document_id / embedding_model）",
               not any(k in prompt for k in ("chunk_id", "document_id", "embedding_model")),
               str(sorted(chunks[0].metadata)))
        _check("'score' 一词不出现", "score" not in prompt)

        # -- 无 source 的知识：不应渲染出「（来源：）」空括号 --
        bare = KnowledgeChunk(content="没有来源的知识正文", source="", metadata={})
        _, prompt_bare = render_question_prompt(variables, [bare])
        _check("无 source 时不渲染「（来源：）」空括号",
               "（来源：）" not in prompt_bare and "没有来源的知识正文" in prompt_bare)
        _check("无 source 时该行就是 '- 正文'",
               f"- {'没有来源的知识正文'}" in _bullet_lines(_knowledge_section(prompt_bare)),
               str(_bullet_lines(_knowledge_section(prompt_bare))))

        # -- 多条知识：逐条一行，保序 --
        two = [
            KnowledgeChunk(content="第一条知识正文", source="mock://a", metadata={}),
            KnowledgeChunk(content="第二条知识正文", source="mock://b", metadata={}),
        ]
        _, prompt_two = render_question_prompt(variables, two)
        lines_two = _bullet_lines(_knowledge_section(prompt_two))
        _check("两条知识 ⇒ 两个项目符号行", len(lines_two) == 2, str(len(lines_two)))
        _check("★ 逐条一行且保序（与输入顺序一致）",
               lines_two == ["- 第一条知识正文（来源：mock://a）",
                             "- 第二条知识正文（来源：mock://b）"],
               str(lines_two))

        # -- 非知识标量必须被丢弃，不能把 repr 塞进 Prompt --
        _, prompt_scalar = render_question_prompt(variables, [123, True, None, "真知识"])
        _check("★ 数字/布尔/None 被丢弃（Prompt 里不出现 '123' / 'True'）",
               "123" not in prompt_scalar and "True" not in prompt_scalar
               and "真知识" in prompt_scalar)

    # -- 多段落切片：如实记录现状（★ 已知格式瑕疵，未擅自修改）--
    async with _rag_env(documents=(MULTI_PARA_DOC,), top_k=3, min_score=None) as env:
        report = env["reports"][0]
        chunks = await env["retriever"].retrieve(JOB, FIXED_TOPIC, CONTEXT)
        _check("多段落文档入库为 1 个切片（段落被合并）",
               report["chunk_count"] == 1, str(report["chunk_count"]))
        _check("该切片 content 内部含空行（\\n\\n）⇒ 多段落是**可达**的",
               len(chunks) == 1 and "\n\n" in chunks[0].content)
        _, prompt_multi = render_question_prompt(_base_variables(), chunks)
        section_multi = _knowledge_section(prompt_multi)
        print("  ---- 多段落切片渲染结果（现状）----")
        for line in section_multi.splitlines():
            print(f"  | {line}")
        print("  ---- 结束 ----")

        bullets_multi = _bullet_lines(section_multi)
        _check("★ 现状：只有**首行**带项目符号（后续段落无 '- ' 前缀）",
               len(bullets_multi) == 1, str(len(bullets_multi)))
        _check("★ 现状：三段正文**全部**保留（内容不丢失）",
               all(seg in prompt_multi for seg in ("第一段", "第二段", "第三段")))
        _check("★ 现状：来源附在**最后一段**之后（不是首段）",
               f"第三段：生产上常两者混用。（来源：{MULTI_PARA_SOURCE}）" in prompt_multi)
        _check("多段落时 Prompt 仍无未渲染占位符", "{{" not in prompt_multi)


# ============================================================
# [6] 端到端：generate_next_question（真实 session + 真实检索器）
# ============================================================
async def check_end_to_end() -> None:
    _section("[6] 端到端 · generate_next_question（真实 session 行 + 真实检索器）")

    async with _rag_env(top_k=3, min_score=None, with_session=True) as env:
        session = env["session"]
        session_id = env["session_id"]
        _check("会话行已创建", isinstance(session_id, int), str(session_id))

        retriever = env["retriever"]
        spy = SpySpark(QUESTION_REPLY)
        result = await interview_core.generate_next_question(
            session, session_id,
            context=CONTEXT, plan=PLAN, retriever=retriever, spark=spy,
        )

        _check("端到端出题成功（ok=True）", result["ok"] is True,
               f"errors={result['errors']} error={result['error']}")
        _check("errors 为空", result["errors"] == [], str(result["errors"]))
        _check("★ 检索成功 ⇒ 无 knowledge_retrieval_failed 警告",
               interview_core.WARNING_KNOWLEDGE_FAILED not in result["warnings"],
               str(result["warnings"]))
        _check("返回字段集恒定为 QUESTION_RESULT_FIELDS（12 键）",
               tuple(result) == interview_core.QUESTION_RESULT_FIELDS, str(tuple(result)))

        _check("检索器确实被调用（queries 非空）", retriever.queries == [FIXED_TOPIC],
               str(retriever.queries))
        _check("只调用一次模型", len(spy.calls) == 1, str(len(spy.calls)))

        final_prompt = spy.calls[0]
        _check("★ 最终 Prompt（模型实际收到的那一份）含知识正文",
               FIXED_DOC["content"] in final_prompt)
        _check("★ 最终 Prompt 含来源标注", FIXED_SOURCE in final_prompt)
        _check("★ 最终 Prompt 含参考知识小节", KNOWLEDGE_HEADING in final_prompt)
        _check("★ 最终 Prompt 的格式与单独渲染一致（Core 不改写 Prompt）",
               _bullet_lines(_knowledge_section(final_prompt))
               == [f"- {FIXED_DOC['content']}（来源：{FIXED_SOURCE}）"],
               str(_bullet_lines(_knowledge_section(final_prompt))[:1])[:70])

        _check("题号/阶段等既有字段仍正确",
               result["question_no"] == 1 and result["stage"] == "technical"
               and result["topic"] == FIXED_TOPIC,
               f"no={result['question_no']} stage={result['stage']} topic={result['topic']}")


# ============================================================
# [7] 边界与降级
# ============================================================
async def check_boundaries() -> None:
    _section("[7] 边界与降级")

    # -- 检索器抛异常 ⇒ 降级为无知识 + warning，出题不中断 --
    async with _rag_env(top_k=3, min_score=None, with_session=True) as env:
        spy = SpySpark(QUESTION_REPLY)
        result = await interview_core.generate_next_question(
            env["session"], env["session_id"],
            context=CONTEXT, plan=PLAN, retriever=BoomRetriever(), spark=spy,
        )
        _check("★ 检索抛异常时出题**仍成功**（可选增强不阻塞主流程）",
               result["ok"] is True, f"errors={result['errors']}")
        _check("★ 失败只记 warning，不进 errors（失败与「检索不到」分开）",
               result["errors"] == []
               and interview_core.WARNING_KNOWLEDGE_FAILED in result["warnings"],
               f"errors={result['errors']} warnings={result['warnings']}")
        _check("★ 降级后 Prompt 回落原模板（不含参考知识小节）",
               KNOWLEDGE_HEADING not in spy.calls[0])
        _check("降级后 Prompt 里没有未渲染占位符", "{{" not in spy.calls[0])

    # -- 检索不到（[]）⇒ 同样回落原模板 --
    async with _rag_env(top_k=3, min_score=None, with_session=True) as env:
        spy = SpySpark(QUESTION_REPLY)
        result = await interview_core.generate_next_question(
            env["session"], env["session_id"],
            context=CONTEXT, plan=PLAN, retriever=SpyRetriever([]), spark=spy,
        )
        _check("检索返回 [] ⇒ 出题成功", result["ok"] is True, str(result["error"]))
        _check("★ 检索返回 [] **不**产生 warning（「检索不到」不是故障）",
               result["warnings"] == [], str(result["warnings"]))
        _check("Prompt 回落原模板", KNOWLEDGE_HEADING not in spy.calls[0])

    # -- min_score 把唯一一条过滤掉 ⇒ 等价于无知识 --
    async with _rag_env(top_k=3, min_score=0.9, with_session=True) as env:
        chunks = await env["retriever"].retrieve(JOB, FIXED_TOPIC, CONTEXT)
        _check("★ min_score 高于该条得分 ⇒ 检索为空（阈值过滤生效）",
               chunks == [], str(len(chunks)))
        spy = SpySpark(QUESTION_REPLY)
        result = await interview_core.generate_next_question(
            env["session"], env["session_id"],
            context=CONTEXT, plan=PLAN, retriever=env["retriever"], spark=spy,
        )
        _check("过滤成空 ⇒ 出题成功且无 warning", result["ok"] is True
               and result["warnings"] == [], str(result["warnings"]))
        _check("过滤成空 ⇒ Prompt 回落原模板", KNOWLEDGE_HEADING not in spy.calls[0])

    # -- 去重：两层去重键不同，如实记录 --
    dup = [
        KnowledgeChunk(content="同一段知识正文", source="mock://s1", metadata={}),
        KnowledgeChunk(content="同一段知识正文", source="mock://s2", metadata={}),
    ]
    lines = normalize_knowledge_context(dup)
    _check("★ Agent 侧去重按「整行文本」（source 不同 ⇒ 两行都保留）",
           len(lines) == 2, str(lines))
    dup_same = [
        KnowledgeChunk(content="同一段知识正文", source="mock://s1", metadata={}),
        KnowledgeChunk(content="同一段知识正文", source="mock://s1", metadata={}),
    ]
    _check("★ 完全相同（含 source）的两条被去重成 1 行",
           len(normalize_knowledge_context(dup_same)) == 1,
           str(normalize_knowledge_context(dup_same)))

    # -- 检索器不修改入参（Core 传下去的 plan/context 不该被改写）--
    before = json.dumps(PLAN, sort_keys=True, ensure_ascii=False)
    async with _rag_env(top_k=3, min_score=None) as env:
        await env["retriever"].retrieve(JOB, FIXED_TOPIC, CONTEXT)
    _check("检索不修改 plan（Core 传入的 plan 保持原样）",
           json.dumps(PLAN, sort_keys=True, ensure_ascii=False) == before)


# ============================================================
# 报告
# ============================================================
async def report() -> None:
    print("\n" + "=" * 74)
    print("Prompt 示例（固定知识 chunk 经真实检索器注入后的**最终** Prompt）")
    print("=" * 74)

    async with _rag_env(top_k=3, min_score=None) as env:
        chunks = await env["retriever"].retrieve(JOB, FIXED_TOPIC, CONTEXT)
        spy = SpySpark(QUESTION_REPLY)
        await interview_agent.generate_question(
            CONTEXT, PLAN, RESUME, JOB, knowledge_context=chunks, spark=spy,
        )
        prompt = spy.calls[0]

    # 只打印到「六、生成要求」为止（输出格式那一段每次都一样，省掉）
    end = prompt.index("## 六、生成要求")
    print(prompt[:end].rstrip())
    print("\n  …（此处省略「六、生成要求」与「七、输出格式」两节，共 "
          f"{len(prompt)} 字符）")
    print("\n" + "=" * 74)
    print("结果")
    print("=" * 74)
    print("  链路：VectorKnowledgeRetriever → interview_core._gather_knowledge")
    print("        → interview_agent.generate_question → question_knowledge.txt")
    print("  固定知识 chunk：handbook://redis/persistence（1 个切片）")
    print("  检索实现：HashEmbeddingService + SqlAlchemyVectorStore + VectorKnowledgeRetriever")
    print("  保护模块指纹：7 个（Retriever / VectorStore / Embedding 全部未改）")
    print("  修复动作：无（注入本来就正确，未触发「发现未注入」条件）")
    print(f"  断言：通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)


async def main() -> None:
    print("=" * 74)
    print("RAG 检索结果 → InterviewAgent → 最终 Prompt 注入验证")
    print("=" * 74)

    check_preconditions()
    await check_retriever_output()
    await check_agent_receives()
    await check_prompt_contains()
    await check_knowledge_format()
    await check_end_to_end()
    await check_boundaries()
    await report()


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(0 if _FAILED == 0 else 1)
