# -*- coding: utf-8 -*-
"""RAG 检索参数透传 · 自检（脚本式，非 pytest）。

运行：``python backend/tests/test_rag_param_passthrough.py``

为什么需要这一套件
------------------
任务 69 的实测结论：调用方在基准里声明的 ``min_score``（``0.25 / 0.25 / 0.40``）
**从未生效**——当时唯一可用的开关 ``use_rag=True`` 无法携带任何参数，
``resolve_retriever`` 只能按默认值组装检索器，于是「配了阈值」与「阈值生效」
是两件事（``effective min_score`` 实测为 ``None``）。

本套件锁死这次修复：``retriever_kwargs`` 从**四个入口**一路透传到
``services.knowledge_rag.build_vector_retriever``，且默认 ``None`` 时
行为逐字节不变。

本套件验证什么
--------------
[1] **签名契约**——四个模块七个函数都长出了 keyword-only 的
    ``retriever_kwargs``（默认 ``None``），既有参数**顺序一个都没动**。
[2] **透传是结构性的**（AST）——每一跳都**确实**把 ``retriever_kwargs``
    传给了下一跳（不是只改签名不改调用）；且它**不越层扩散**
    （Planner / Agent / Validator / ``build_question_plan`` 的签名里都没有它，
    规则生成器的方法体里也**不读取**它）。
[3] **组装层语义**——``None`` / ``{}`` / 不传三者等价；合法键生效且
    **不是整体覆盖**（未提及的取检索器默认值）；非法键 / 非映射
    ⇒ 静默降级为「无知识」+ 稳定 warning（**不抛异常**）；
    注入了 ``retriever`` 时 kwargs **不被读取**。
[4] **端到端：参数真的改变了 ``knowledge_context``**（核心验收）——
    接线取证（组装器实际收到的 kwargs）+ 行为取证（与「显式注入检索器」
    这条**修复前就存在**的路径逐字段相等）。这是本套件最重要的一条：
    它把「配了阈值」升级为「阈值生效」。
[5] **Service 层同样透传**——经 ``interview_service.generate_next_question``
    走一遍，效果与 Core 直调一致。

.. note::
    本套件**只读**生产代码，不写任何业务文件、不改任何业务逻辑。
    内存 SQLite 仅出现在 ``tests/`` 下（项目硬约束）。
    期望值全部由**实测**派生（探测检索器的分数分布），不硬编码分数；
    唯一与语料绑定的断言（默认口径 5 条）已单独标注，语料本身由
    ``test_interview_eval_benchmark.py`` 的指纹锁死。
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
PROJECT_DIR = BACKEND_DIR.parent
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
from prompts import render_prompt  # noqa: E402
from services import (  # noqa: E402
    interview_agent,
    interview_context,
    interview_core,
    interview_service,
    knowledge_rag,
)
from services import question_generator as qg  # noqa: E402
from services.interview_agent import build_question_variables  # noqa: E402
from services.knowledge_import_pipeline import KnowledgeImportPipeline  # noqa: E402
from services.knowledge_rag import default_embedder  # noqa: E402
from services.vector_knowledge_retriever import (  # noqa: E402
    VectorKnowledgeRetriever,
)
from services.vector_store import DEFAULT_TOP_K  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

# ============================================================
# 一、常量
# ============================================================
BENCHMARK_PATH = BACKEND_DIR / "scripts" / "interview_eval_benchmark.json"
CORPUS_PATH = BACKEND_DIR / "scripts" / "interview_knowledge.json"

#: 复用基准里的场景 s1（MySQL 索引 / 中级 / 项目阶段）：
#: 它的 ``query_topic``、命中来源、分数分布都已在任务 65/69 记录过，
#: 便于把本套件的实测值与既有记录交叉核对。
SCENARIO_ID = "s1-mysql-index-mid"

#: 知识小节标题（有知识 → ``question_knowledge.txt``；无知识 → ``question.txt``）。
KNOWLEDGE_HEADING = "## 四·五、参考知识（外部检索结果，可选）"
KNOWLEDGE_END = "## 五、本场进度（用于避免重复）"

#: 透传链上的四个入口（Service → Core → 组装器；以及策略层）。
PASSTHROUGH_TARGETS: Tuple[Tuple[str, str], ...] = (
    ("services/interview_service.py", "generate_next_question"),
    ("services/interview_core.py", "generate_next_question"),
    ("services/interview_core.py", "resolve_retriever"),
    ("services/question_generator.py", "generate_questions"),
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
# 二、测试替身与观测器
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


class ExplodingRetriever:
    """一旦被调用就炸——用来证明「注入了 retriever 时不会走组装分支」。"""

    def __init__(self) -> None:
        self.calls = 0

    async def retrieve(self, job_info: Any, topic: Any, context: Any) -> List[Any]:
        self.calls += 1
        raise AssertionError("ExplodingRetriever 不该被调用")


@asynccontextmanager
async def _record_knowledge_context() -> Iterator[Dict[str, Any]]:
    """临时包装 ``interview_agent.generate_question``，记录**真实流程**注入的知识。

    为什么替换模块属性就生效：``interview_core.generate_candidate_question`` 在
    **函数体内** ``from services.interview_agent import generate_question``
    ⇒ 按**调用时**的模块属性取值。包装器原样转发，不改变行为。
    """
    original = interview_agent.generate_question
    captured: Dict[str, Any] = {"calls": 0}

    async def wrapper(context: Any, plan: Any = None, resume: Any = None,
                      job: Any = None, *, knowledge_context: Any = None,
                      spark: Any = None) -> Dict[str, Any]:
        captured["calls"] += 1
        captured["knowledge_context"] = knowledge_context
        captured["variables"] = build_question_variables(context, plan, resume, job)
        return await original(context, plan, resume, job,
                              knowledge_context=knowledge_context, spark=spark)

    interview_agent.generate_question = wrapper
    try:
        yield captured
    finally:
        interview_agent.generate_question = original


@asynccontextmanager
async def _spy_assembler() -> Iterator[List[Dict[str, Any]]]:
    """包装 ``knowledge_rag.build_vector_retriever``，记录它**实际收到**的参数。

    ``resolve_retriever`` 在**函数体内** ``from services.knowledge_rag import
    build_vector_retriever`` ⇒ 按调用时的模块属性取值，因此打桩生效。
    这是「接线取证」：光看签名不足以证明参数真的走到了组装器。
    """
    original = knowledge_rag.build_vector_retriever
    seen: List[Dict[str, Any]] = []

    def wrapper(db: Any, *, embedder: Any = None, model: Any = None,
                **kwargs: Any) -> Any:
        record: Dict[str, Any] = {
            "db_is_none": db is None,
            "embedder": embedder,
            "model": model,
            "kwargs": dict(kwargs),
        }
        built = original(db, embedder=embedder, model=model, **kwargs)
        record["built"] = built
        seen.append(record)
        return built

    knowledge_rag.build_vector_retriever = wrapper
    try:
        yield seen
    finally:
        knowledge_rag.build_vector_retriever = original


# ============================================================
# 三、静态检查工具（AST / 签名）
# ============================================================
def _module_ast(module: Any) -> ast.Module:
    return ast.parse(inspect.getsource(module))


def _ast_func(module: Any, func_name: str) -> Optional[ast.AST]:
    for node in _module_ast(module).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                and node.name == func_name:
            return node
    return None


def _ast_method(module: Any, class_name: str, method_name: str) -> Optional[ast.AST]:
    for node in _module_ast(module).body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                        and sub.name == method_name:
                    return sub
    return None


def _callee_name(call: ast.Call) -> Optional[str]:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _passes_kwarg(node: Optional[ast.AST], callee: str, kwarg: str) -> bool:
    """``node`` 体内是否有 ``callee(..., kwarg=...)`` 这种**关键字**调用。"""
    if node is None:
        return False
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call) and _callee_name(sub) == callee:
            if any(k.arg == kwarg for k in sub.keywords):
                return True
    return False


def _mentions(node: Optional[ast.AST], name: str) -> bool:
    """``node`` 体内是否**出现**该标识符（读取或传递都算）。"""
    if node is None:
        return False
    return any(isinstance(n, ast.Name) and n.id == name for n in ast.walk(node))


def _imported_modules(module: Any) -> List[str]:
    """模块里 import 的**模块名**（不看注释 / 文档字符串——它们常提到模块名）。"""
    found: List[str] = []
    for node in ast.walk(_module_ast(module)):
        if isinstance(node, ast.Import):
            found.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            found.append(base)
            found.extend(f"{base}.{alias.name}" for alias in node.names)
    return found


# ============================================================
# 四、环境
# ============================================================
def _load_scenario() -> Dict[str, Any]:
    data = json.loads(BENCHMARK_PATH.read_text(encoding="utf-8"))
    for scenario in data["scenarios"]:
        if scenario["id"] == SCENARIO_ID:
            return scenario
    raise SystemExit(f"基准 {BENCHMARK_PATH.name} 里找不到场景 {SCENARIO_ID}")


def _load_corpus() -> List[Dict[str, Any]]:
    return json.loads(CORPUS_PATH.read_text(encoding="utf-8"))["documents"]


def _mock_reply_text(scenario: Dict[str, Any]) -> str:
    return json.dumps(scenario["mock_reply"], ensure_ascii=False)


@asynccontextmanager
async def _env(scenario: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """按场景声明建一套真实前置数据（只 Mock 大模型）。"""
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

        session.add(User(username="passthrough", email="passthrough@example.com",
                         password_hash="x", role="user"))
        await session.flush()

        job = Job(**scenario["job"])
        resume = Resume(user_id=1, filename="resume.md",
                        content=scenario["candidate"]["resume_content"])
        session.add_all([job, resume])
        await session.flush()

        cfg = scenario["session"]
        row = InterviewSession(
            user_id=1, job_id=job.id, resume_id=resume.id, status="created",
            interview_type=cfg["interview_type"], difficulty=cfg["difficulty"],
            duration=cfg["duration"], total_questions=cfg["total_questions"],
            current_question_no=1,
        )
        session.add(row)
        await session.commit()
        session_id = row.id

        # 真实 Planner 走**数据库加载路径**产出计划（use_llm=False ⇒ 确定性）
        plan = await interview_core._build_session_plan(session, row)

        ctx = scenario["context"]
        await interview_context.create_context(session, session_id)
        await interview_context.update_context(
            session, session_id,
            current_stage=ctx["current_stage"],
            asked_questions=list(ctx["asked_questions"]),
            covered_topics=list(ctx["covered_topics"]),
            weak_topics=list(ctx["weak_topics"]),
        )
        context = await interview_context.get_context(session, session_id)

        embedder = default_embedder()
        store = SqlAlchemyVectorStore(session)
        pipeline = KnowledgeImportPipeline(session, embedder=embedder, store=store)
        doc_ids: Dict[str, int] = {}
        for doc in _load_corpus():
            report = await pipeline.import_document(dict(doc))
            doc_ids[doc["source"]] = report["document_id"]

        yield {
            "db": session,
            "row": row,
            "session_id": session_id,
            "user_id": 1,
            "job": job,
            "resume": resume,
            "plan": plan,
            "context": context,
            "embedder": embedder,
            "store": store,
            "doc_ids": doc_ids,
            "scenario": scenario,
        }
    finally:
        if session is not None:
            await session.close()
        await engine.dispose()


def _topic(env: Dict[str, Any]) -> str:
    return interview_core.current_topic(env["plan"], env["context"])


async def _direct_retrieve(env: Dict[str, Any], **retriever_kwargs: Any) -> List[Any]:
    """**修复前就存在**的路径：显式构造检索器再注入（作为期望值的来源）。"""
    retriever = VectorKnowledgeRetriever(env["embedder"], env["store"],
                                         **retriever_kwargs)
    return await retriever.retrieve(env["job"], _topic(env), env["context"])


async def _core_flow(env: Dict[str, Any], *, retriever_kwargs: Any = None,
                     use_rag: bool = True, spark: Optional[SpySpark] = None
                     ) -> Tuple[Dict[str, Any], List[Any], SpySpark]:
    """跑一次 Core 出题 → ``(结果, 实际注入 Agent 的 knowledge_context, spy)``。"""
    spy = spark if spark is not None else SpySpark(_mock_reply_text(env["scenario"]))
    async with _record_knowledge_context() as captured:
        result = await interview_core.generate_next_question(
            env["db"], env["session_id"],
            context=env["context"], plan=env["plan"],
            use_rag=use_rag, retriever_kwargs=retriever_kwargs, spark=spy,
        )
    return result, list(captured.get("knowledge_context") or []), spy


async def _service_flow(env: Dict[str, Any], *, retriever_kwargs: Any = None
                        ) -> Tuple[Dict[str, Any], List[Any], SpySpark]:
    """经 **Service 层**跑一次（证明 Service 也透传，而不是只改了 Core）。"""
    spy = SpySpark(_mock_reply_text(env["scenario"]))
    async with _record_knowledge_context() as captured:
        result = await interview_service.generate_next_question(
            env["db"], env["user_id"], env["session_id"],
            use_rag=True, retriever_kwargs=retriever_kwargs, spark=spy,
        )
    return result, list(captured.get("knowledge_context") or []), spy


# ============================================================
# [1] 签名契约
# ============================================================
def check_signatures() -> None:
    _section("[1] 签名契约（keyword-only retriever_kwargs，默认 None；既有顺序不动）")

    rr_sig = inspect.signature(interview_core.resolve_retriever)
    _check("resolve_retriever 参数顺序为 (db, retriever, use_rag, *, retriever_kwargs)",
           list(rr_sig.parameters) ==
           ["db", "retriever", "use_rag", "retriever_kwargs"],
           str(list(rr_sig.parameters)))
    _check("  └ retriever / use_rag 默认值未变（None / False）",
           rr_sig.parameters["retriever"].default is None
           and rr_sig.parameters["use_rag"].default is False)
    _check("  └ retriever_kwargs 是 keyword-only 且默认 None",
           rr_sig.parameters["retriever_kwargs"].kind
           is inspect.Parameter.KEYWORD_ONLY
           and rr_sig.parameters["retriever_kwargs"].default is None)

    core_sig = inspect.signature(interview_core.generate_next_question)
    _check("Core.generate_next_question 前 7 个参数顺序未变",
           list(core_sig.parameters)[:7] ==
           ["db", "session_id", "user_id", "spark", "context", "plan", "retriever"],
           str(list(core_sig.parameters)))
    _check("  └ retriever_kwargs 追加在**最后**（不打断既有位置参数）",
           list(core_sig.parameters)[-1] == "retriever_kwargs",
           str(list(core_sig.parameters)))
    _check("  └ 且是 keyword-only、默认 None",
           core_sig.parameters["retriever_kwargs"].kind
           is inspect.Parameter.KEYWORD_ONLY
           and core_sig.parameters["retriever_kwargs"].default is None)

    svc_sig = inspect.signature(interview_service.generate_next_question)
    _check("Service.generate_next_question 前 3 个参数为 (db, user_id, session_id)",
           list(svc_sig.parameters)[:3] == ["db", "user_id", "session_id"],
           str(list(svc_sig.parameters)))
    _check("  └ 也有 keyword-only retriever_kwargs（默认 None）",
           svc_sig.parameters["retriever_kwargs"].kind
           is inspect.Parameter.KEYWORD_ONLY
           and svc_sig.parameters["retriever_kwargs"].default is None)

    for cls in (qg.QuestionGenerator, qg.RuleQuestionGenerator,
                qg.AgentQuestionGenerator):
        sig = inspect.signature(cls.generate)
        _check(f"{cls.__name__}.generate 也有 keyword-only retriever_kwargs",
               sig.parameters["retriever_kwargs"].kind
               is inspect.Parameter.KEYWORD_ONLY
               and sig.parameters["retriever_kwargs"].default is None,
               str(list(sig.parameters)))
    gq_sig = inspect.signature(qg.generate_questions)
    _check("question_generator.generate_questions 同样具备",
           gq_sig.parameters["retriever_kwargs"].kind
           is inspect.Parameter.KEYWORD_ONLY
           and gq_sig.parameters["retriever_kwargs"].default is None)

    # 反向：入口函数必须仍然可**位置**调用前几个参数（不因为新参数变成 keyword-only 而破坏兼容）
    _check("守卫自检：签名检查能发现参数缺失（对不存在的名字必须为假）",
           "retriever_kwargs" not in inspect.signature(
               interview_core.build_question_plan).parameters)


# ============================================================
# [2] 透传是结构性的（AST）
# ============================================================
def check_ast_passthrough() -> None:
    _section("[2] 透传是结构性的（AST：每一跳都真的传下去；且不越层扩散）")

    core_flow = _ast_func(interview_core, "generate_next_question")
    _check("取到 Core.generate_next_question 的 AST 节点", core_flow is not None)
    _check("★ Core.generate_next_question 把 retriever_kwargs 传给 resolve_retriever",
           _passes_kwarg(core_flow, "resolve_retriever", "retriever_kwargs"))
    _check("  └ 且 resolve_retriever 体内确实调用了 build_vector_retriever",
           any(isinstance(n, ast.Call)
               and _callee_name(n) == "build_vector_retriever"
               for n in ast.walk(_ast_func(interview_core, "resolve_retriever"))))

    svc_flow = _ast_func(interview_service, "generate_next_question")
    _check("★ Service.generate_next_question 把 retriever_kwargs 传给 Core",
           _passes_kwarg(svc_flow, "generate_next_question", "retriever_kwargs"))

    agent_gen = _ast_method(qg, "AgentQuestionGenerator", "generate")
    _check("★ AgentQuestionGenerator.generate 把 retriever_kwargs 传给 Core",
           _passes_kwarg(agent_gen, "generate_next_question", "retriever_kwargs"))

    gq_flow = _ast_func(qg, "generate_questions")
    _check("★ question_generator.generate_questions 把它转发给生成器",
           _passes_kwarg(gq_flow, "generate", "retriever_kwargs"))

    # --- 不越层扩散 ---
    _check("build_question_plan 签名里没有 retriever_kwargs（规则路径不受影响）",
           "retriever_kwargs" not in inspect.signature(
               interview_core.build_question_plan).parameters)
    _check("interview_agent.generate_question 签名里没有它（Agent 只收 knowledge_context）",
           "retriever_kwargs" not in inspect.signature(
               interview_agent.generate_question).parameters)
    _check("retrieve_knowledge 签名里没有它（检索接缝只认 retriever）",
           "retriever_kwargs" not in inspect.signature(
               interview_core.retrieve_knowledge).parameters)
    _check("validate_candidate_question 签名里没有它（Validator 与检索无关）",
           "retriever_kwargs" not in inspect.signature(
               interview_core.validate_candidate_question).parameters)

    rule_gen = _ast_method(qg, "RuleQuestionGenerator", "generate")
    _check("★ RuleQuestionGenerator.generate 方法体**不读取** retriever_kwargs",
           not _mentions(rule_gen, "retriever_kwargs"))
    _check("  └ 规则出题方法体里也没有任何检索调用",
           not any(_callee_name(n) in ("retrieve", "retrieve_knowledge",
                                       "resolve_retriever", "build_vector_retriever")
                   for n in ast.walk(rule_gen)
                   if isinstance(n, ast.Call)))
    _check("★ Core 源码里 retriever_kwargs 只出现在那**两个**函数体内（没有第三处）",
           _count_occurrences(inspect.getsource(interview_core), "retriever_kwargs")
           == _count_in_two_funcs())


def _count_occurrences(text: str, token: str) -> int:
    """按 AST 统计标识符出现次数（不数注释 / 字符串里的同名字样）。"""
    return sum(1 for n in ast.walk(ast.parse(text))
               if isinstance(n, ast.Name) and n.id == token)


def _count_in_two_funcs() -> int:
    total = 0
    for func_name in ("generate_next_question", "resolve_retriever"):
        node = _ast_func(interview_core, func_name)
        total += sum(1 for n in ast.walk(node)
                     if isinstance(n, ast.Name) and n.id == "retriever_kwargs")
    return total


# ============================================================
# [3] 组装层语义
# ============================================================
async def check_resolve_semantics(env: Dict[str, Any]) -> None:
    _section("[3] 组装层语义（resolve_retriever 透传 / 降级 / 注入优先）")
    db = env["db"]

    base, warns = interview_core.resolve_retriever(db, None, True)
    none_r, warns_none = interview_core.resolve_retriever(
        db, None, True, retriever_kwargs=None)
    empty_r, warns_empty = interview_core.resolve_retriever(
        db, None, True, retriever_kwargs={})

    def _config(retriever: Any) -> Tuple[Any, ...]:
        return (retriever.top_k, retriever.min_score, retriever.dedup,
                retriever.category, retriever.document_id, retriever.model_name)

    _check("★ retriever_kwargs 省略 / None / {} 三者配置逐字段等价（默认行为不变）",
           _config(base) == _config(none_r) == _config(empty_r)
           and warns == warns_none == warns_empty == [],
           f"{_config(none_r)} vs {_config(base)}")
    _check("  └ 默认口径 = top_k 取默认值、min_score=None（与任务 69 实测一致）",
           base.top_k == DEFAULT_TOP_K and base.min_score is None,
           f"top_k={base.top_k} min_score={base.min_score}")

    tuned, tuned_warns = interview_core.resolve_retriever(
        db, None, True, retriever_kwargs={"top_k": 3, "min_score": 0.25})
    _check("★ retriever_kwargs 真正生效（top_k / min_score 都落到检索器上）",
           isinstance(tuned, VectorKnowledgeRetriever) and tuned_warns == []
           and tuned.top_k == 3 and tuned.min_score == 0.25,
           f"top_k={getattr(tuned, 'top_k', None)} "
           f"min_score={getattr(tuned, 'min_score', None)}")
    _check("  └ 未提及的参数仍取检索器默认值（不是整体覆盖）",
           tuned.dedup is True and tuned.category is None
           and tuned.document_id is None and tuned.model_name == base.model_name,
           f"dedup={tuned.dedup} category={tuned.category}")

    multi, _ = interview_core.resolve_retriever(
        db, None, True,
        retriever_kwargs={"top_k": 4, "min_score": 0.1, "dedup": False,
                          "category": "technical"})
    _check("  └ 多个键可同时生效（top_k / min_score / dedup / category）",
           multi.top_k == 4 and multi.min_score == 0.1
           and multi.dedup is False and multi.category == "technical")

    probe = ExplodingRetriever()
    got, warns_probe = interview_core.resolve_retriever(
        db, probe, True, retriever_kwargs={"top_k": 3})
    _check("★ 注入了 retriever ⇒ retriever_kwargs 不被读取（注入者自带配置）",
           got is probe and warns_probe == [] and probe.calls == 0)
    got, warns_probe = interview_core.resolve_retriever(
        db, probe, True, retriever_kwargs={"不存在的键": 1})
    _check("  └ 即使 kwargs 非法，注入路径也照样返回注入的检索器（不组装、不报错）",
           got is probe and warns_probe == [])

    for label, bad in (
        ("top_k 传字符串", {"top_k": "not-an-int"}),
        ("top_k 传 bool", {"top_k": True}),
        ("min_score 传字符串", {"min_score": "high"}),
        ("未知键", {"不存在的键": 1}),
        ("非映射（字符串）", "abc"),
    ):
        got, warns_bad = interview_core.resolve_retriever(
            db, None, True, retriever_kwargs=bad)
        _check(f"★ 非法 retriever_kwargs（{label}）⇒ 静默降级为「无知识」+ warning（不抛异常）",
               got is None
               and warns_bad == [interview_core.WARNING_KNOWLEDGE_FAILED],
               str(warns_bad))


# ============================================================
# [4] 端到端：参数真的改变了 knowledge_context
# ============================================================
async def check_end_to_end(env: Dict[str, Any]) -> None:
    _section("[4] 端到端（use_rag=True + retriever_kwargs ⇒ 与显式注入检索器逐字段相等）")

    topic = _topic(env)
    _check("场景的 current_topic 与基准声明一致（检索 query 未漂移）",
           topic == env["scenario"]["retrieval"]["query_topic"],
           f"{topic!r} vs {env['scenario']['retrieval']['query_topic']!r}")

    probe = await _direct_retrieve(env, top_k=100, min_score=None, dedup=False)
    scores = [chunk.metadata["score"] for chunk in probe]
    _check("探测：候选非空且分数降序（期望值由实测派生，不硬编码）",
           len(scores) >= 3 and all(scores[i] >= scores[i + 1]
                                    for i in range(len(scores) - 1)),
           f"n={len(scores)} top={scores[:3]}")
    t_between_1_2 = (scores[0] + scores[1]) / 2
    t_above_all = max(1.01, scores[0] + 0.01)

    mysql_doc_id = env["doc_ids"]["handbook://mysql/index"]
    cases: List[Tuple[str, Dict[str, Any], Optional[int]]] = [
        ("默认（不传 retriever_kwargs）", {}, DEFAULT_TOP_K),
        ("top_k=2（截断）", {"top_k": 2}, 2),
        ("top_k=2 + dedup=False", {"top_k": 2, "dedup": False}, 2),
        (f"min_score 卡在 1/2 名之间（{t_between_1_2:.4f}）",
         {"min_score": t_between_1_2, "dedup": False}, 1),
        ("min_score 高于全部命中", {"min_score": t_above_all}, 0),
        ("category=technical（只留手册）", {"category": "technical"}, None),
        ("document_id=MySQL 手册", {"document_id": mysql_doc_id}, None),
    ]

    for label, kwargs, expect_count in cases:
        expected = await _direct_retrieve(env, **kwargs)
        seen: List[Dict[str, Any]] = []
        async with _spy_assembler() as seen:
            result, actual, spy = await _core_flow(env, retriever_kwargs=dict(kwargs))
        _check(f"★ {label}：出题成功且只调一次模型",
               result.get("ok") is True and spy.call_count == 1,
               str(result.get("error")))
        _check(f"  └ 组装器实际收到 {sorted(kwargs)}（接线取证）",
               len(seen) == 1 and seen[0]["kwargs"] == dict(kwargs)
               and seen[0]["db_is_none"] is False,
               str(seen[0]["kwargs"] if seen else None))
        _check(f"  └ knowledge_context == 显式注入检索器的结果（逐字段）",
               actual == expected,
               f"n={len(actual)} vs {len(expected)}")
        if expect_count is not None:
            _check(f"  └ 条数为 {expect_count}（实测口径）",
                   len(actual) == expect_count, str(len(actual)))
        if kwargs.get("category") == "technical":
            _check("  └ category 过滤真的生效（只剩 handbook:// 手册切片）",
                   actual and all(c.source.startswith("handbook://") for c in actual),
                   str(sorted({c.source for c in actual}))[:160])
        if kwargs.get("document_id") == mysql_doc_id:
            _check("  └ document_id 过滤真的生效（只剩该文档的切片）",
                   actual and all(c.metadata.get("document_id") == mysql_doc_id
                                  for c in actual),
                   str({c.metadata.get("document_id") for c in actual}))

    # 与既有记录交叉核对：默认口径 5 条（`scripts/rag_baseline.json` 实测 5 条）
    result_default, default_ctx, spy_default = await _core_flow(env, retriever_kwargs=None)
    _check("默认口径命中 5 条（与 rag_baseline.json 的实测一致）",
           len(default_ctx) == 5, str(len(default_ctx)))
    _check("  └ 组装与检索都正常（结果无 warning）",
           result_default.get("warnings") == [],
           str(result_default.get("warnings")))

    # 最高价值的一条：阈值高到无命中 ⇒ Prompt 与「无知识」模板逐字节相同
    legacy = render_prompt(
        "question",
        build_question_variables(env["context"], env["plan"],
                                 env["resume"], env["job"]),
    )
    result_empty, empty_ctx, spy_empty = await _core_flow(
        env, retriever_kwargs={"min_score": t_above_all})
    _check("★ 阈值高到无命中 ⇒ knowledge_context 为空", empty_ctx == [],
           str(len(empty_ctx)))
    _check("  └ 且结果无 warning（「检索不到」不是「检索失败」）",
           result_empty.get("warnings") == [],
           str(result_empty.get("warnings")))
    _check("★ 且 Prompt 回落到「无知识」模板（逐字节相同）",
           spy_empty.calls[0] == legacy)
    _check("  └ 默认口径的 Prompt 与它不同、且含参考知识小节（知识确实进了 Prompt）",
           spy_default.calls[0] != spy_empty.calls[0]
           and KNOWLEDGE_HEADING in spy_default.calls[0]
           and KNOWLEDGE_END in spy_default.calls[0])
    _check("  └ 无知识模板里不含参考知识小节",
           KNOWLEDGE_HEADING not in spy_empty.calls[0])


# ============================================================
# [5] Service 层同样透传
# ============================================================
async def check_service_layer(env: Dict[str, Any]) -> None:
    _section("[5] Service 层透传（经 HTTP 之下那一层走一遍，效果与 Core 直调一致）")

    kwargs = {"top_k": 2, "dedup": False}
    expected = await _direct_retrieve(env, **kwargs)

    seen: List[Dict[str, Any]] = []
    async with _spy_assembler() as seen:
        result, actual, spy = await _service_flow(env, retriever_kwargs=dict(kwargs))

    _check("Service.generate_next_question 出题成功且只调一次模型",
           result.get("ok") is True and spy.call_count == 1,
           str(result.get("error")))
    _check("★ Service 层也把 retriever_kwargs 送到了组装器",
           len(seen) == 1 and seen[0]["kwargs"] == kwargs,
           str(seen[0]["kwargs"] if seen else None))
    _check("★ 结果与显式注入检索器逐字段相等", actual == expected,
           f"n={len(actual)} vs {len(expected)}")

    # 归属校验仍然生效（Service 的既有职责，不该被本次改动影响）
    try:
        await interview_service.generate_next_question(
            env["db"], env["user_id"] + 999, env["session_id"], use_rag=True)
        _check("★ 非本人会话仍被拒绝（404）", False, "竟然通过了")
    except Exception as exc:  # noqa: BLE001
        status = getattr(exc, "status_code", None)
        _check("★ 非本人会话仍被拒绝（404）", status == 404, f"{type(exc).__name__}/{status}")

    _check("Service 未 import 组装器（只透传，不自己组装）",
           not any("knowledge_rag" in m for m in _imported_modules(interview_service)),
           str([m for m in _imported_modules(interview_service)
                if "knowledge" in m]))
    _check("  └ 策略层同样未 import 组装器（AgentQuestionGenerator 也只透传）",
           not any("knowledge_rag" in m for m in _imported_modules(qg)))


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    check_signatures()
    check_ast_passthrough()
    scenario = _load_scenario()
    async with _env(scenario) as env:
        await check_resolve_semantics(env)
        await check_end_to_end(env)
        await check_service_layer(env)

    print("\n" + "=" * 74)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
