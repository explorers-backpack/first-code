# -*- coding: utf-8 -*-
"""AI 模拟面试 · 真实 RAG 接入 Agent 出题流程 自检

无需 pytest，直接运行：
    python backend/tests/test_rag_interview.py

不依赖本机 MySQL：跑在 SQLite 内存库上（StaticPool）；不联网、不需要密钥。
知识切片与向量**真实写入 knowledge_chunk 表**（走 SqlAlchemyVectorStore），
因此本套件验证的是**完整链路**，不是替身：

    Core ──resolve_retriever──▶ KnowledgeRetriever（真实向量检索）
         ──▶ InterviewAgent（参考知识进 Prompt）──▶ QuestionValidator

覆盖范围（对应用户点名的 4 条测试）
-----------------------------------
1. **Agent 有知识生成问题**：真实表 + 真实向量 → Prompt 出现「参考知识：」与切片正文
2. **无知识正常生成**：空知识库 / 默认不接 RAG / 空实现 → Prompt 与无知识基线逐字节相同
3. **Rule 模式不触发 RAG**：AST + 运行时双重取证（连组装都不发生）
4. **RAG 失败不影响规则模式**：检索抛异常 / 组装抛异常时，规则出题结果逐字段不变
另加：RAG 开关契约（``resolve_retriever``）、组装器契约、隔离守卫（AST / 子进程）。
"""

import ast
import asyncio
import inspect
import json
import os
import pathlib
import subprocess
import sys

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
import regression_env  # noqa: E402,F401  钉住离线 Embedding + RAG 阈值（回归不受 .env 影响）
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
from models import Job, KnowledgeDocument, User  # noqa: E402
from schemas.interview import SessionCreateRequest  # noqa: E402
from services import (  # noqa: E402
    interview_agent,
    interview_core,
    interview_service,
    knowledge_rag,
    question_generator as qg,
)
from services.embedding_service import HashEmbeddingService  # noqa: E402
from services.knowledge_rag import build_vector_retriever  # noqa: E402
from services.knowledge_retriever import (  # noqa: E402
    KnowledgeRetriever,
    MockKnowledgeRetriever,
)
from services.vector_knowledge_retriever import (  # noqa: E402
    RetrieverConfigError,
    VectorKnowledgeRetriever,
)
from services.vector_store import VectorRecord  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

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

#: 当前 topic（``current_topic(PLAN, CONTEXT)`` 的结果）—— 检索查询就是它
TOPIC = "Redis 持久化"

#: 知识库里的切片正文（**真实写入 knowledge_chunk 表**）
KNOWLEDGE = (
    ("Redis 持久化有 RDB 与 AOF 两种方式：RDB 是某一时刻的全量快照，恢复快但可能丢数据；"
     "AOF 记录写命令，可通过 appendfsync 控制落盘频率，数据更安全但文件更大。",
     "manual://handbook/redis-persistence"),
    ("判定链表是否有环用快慢指针（Floyd 判圈）：快指针每次走两步、慢指针走一步，"
     "若相遇则有环。时间 O(n)、空间 O(1)。",
     "manual://handbook/linked-list-cycle"),
)


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
    """AST 收集**全部**（含函数体内）import 目标名。"""
    names: set = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _module_level_imports(source: str) -> set:
    """只收集**模块顶层**（不在任何函数/类体内）的 import 目标名。

    用来验证「某个依赖是延迟导入的」——``ast.walk`` 会把函数体内的 import
    也算进来，那种写法无法区分「模块顶层依赖」与「延迟导入」。
    """
    names: set = set()
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _method_node(source: str, class_name: str, method_name: str):
    """取 ``class X: def/async def y`` 的 AST 节点（找不到返回 None）。"""
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                        and sub.name == method_name:
                    return sub
    return None


def _retrieval_calls(node) -> list:
    """方法体内是否出现「检索调用」：``x.retrieve(...)`` / ``retrieve_knowledge(...)``
    / ``resolve_retriever(...)`` / ``build_vector_retriever(...)``。"""
    if node is None:
        return []
    hits = []
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call):
            func = sub.func
            if isinstance(func, ast.Attribute) and func.attr in (
                "retrieve", "resolve_retriever", "build_vector_retriever",
            ):
                hits.append(func.attr)
            elif isinstance(func, ast.Name) and func.id in (
                "retrieve_knowledge", "resolve_retriever", "build_vector_retriever",
            ):
                hits.append(func.id)
    return hits


def _production_files():
    """后端生产代码（排除 tests / __pycache__），用于「谁 import 谁」的检查。"""
    files = []
    for pattern in ("*.py", "api/*.py", "services/*.py", "models/*.py",
                    "schemas/*.py", "utils/*.py"):
        for path in BACKEND_DIR.glob(pattern):
            if "__pycache__" in path.parts:
                continue
            files.append(path)
    return sorted(set(files))


async def _raises(coro, exc_type):
    try:
        await coro
    except exc_type as exc:
        return True, exc
    except Exception as exc:  # noqa: BLE001
        return False, f"抛了非预期异常 {type(exc).__name__}: {exc}"
    return False, "未抛异常"


# ============================================================
# 测试替身
# ============================================================
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
    """一旦被调用就报错——证明 rule 模式**从不**触碰大模型。"""

    async def chat_async(self, message: str) -> str:
        raise AssertionError("rule 模式不应调用大模型")


class ExplodingRetriever:
    """一旦被检索就报错——证明 rule 模式**从不**触碰检索。"""

    def __init__(self):
        self.calls = 0

    async def retrieve(self, job_info, topic, context):
        self.calls += 1
        raise AssertionError("rule 模式不应调用检索")


def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed(db: AsyncSession):
    user = User(username="rag_t", email="rag_t@example.com", password_hash="x", role="user")
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


async def _seed_knowledge(db: AsyncSession, *, embedder=None):
    """把知识切片 + 真实向量写进 knowledge_chunk（相当于「批量入库脚本」）。"""
    emb = embedder or HashEmbeddingService()
    doc = KnowledgeDocument(
        title="后端面试手册", content="（全文略）", category="technical",
        source="manual://handbook",
    )
    db.add(doc)
    await db.commit()
    await db.refresh(doc)
    doc_id = doc.id

    vectors = await emb.embed_batch([content for content, _ in KNOWLEDGE])
    store = SqlAlchemyVectorStore(db, model=emb.name)
    await store.add([
        VectorRecord(
            vector=vector, content=content, document_id=doc_id,
            metadata={"category": "technical", "source": source, "chunk_index": i},
            model=emb.name,
        )
        for i, ((content, source), vector) in enumerate(zip(KNOWLEDGE, vectors))
    ])
    return doc_id


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 模拟面试 · 真实 RAG 接入 Agent 出题流程 自检")
    print("=" * 70)

    core_src = inspect.getsource(interview_core)
    qg_src = inspect.getsource(qg)

    engine, Session = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with Session() as db:
        user_id, job_id = await _seed(db)
        sid = await _make_session(db, user_id, job_id)

        # ------------------------------------------------------------
        # [1] RAG 开关契约
        # ------------------------------------------------------------
        print("\n[1] RAG 开关契约（resolve_retriever = 全项目唯一开关）")
        _check("Core 暴露 resolve_retriever",
               hasattr(interview_core, "resolve_retriever"))
        _check("  └ 已声明在 __all__",
               "resolve_retriever" in interview_core.__all__)
        _check("  └ 是**同步**函数（只构造对象、不发起 IO）",
               not inspect.iscoroutinefunction(interview_core.resolve_retriever))
        rr_sig = inspect.signature(interview_core.resolve_retriever)
        _check("  └ 签名为 (db, retriever=None, use_rag=False, *, retriever_kwargs=None)",
               list(rr_sig.parameters) ==
               ["db", "retriever", "use_rag", "retriever_kwargs"]
               and rr_sig.parameters["retriever"].default is None
               and rr_sig.parameters["use_rag"].default is False
               and rr_sig.parameters["retriever_kwargs"].kind
               is inspect.Parameter.KEYWORD_ONLY
               and rr_sig.parameters["retriever_kwargs"].default is None,
               str(list(rr_sig.parameters)))

        probe = ExplodingRetriever()
        got, warns = interview_core.resolve_retriever(db, probe, True)
        _check("★ 注入的 retriever 优先（即使 use_rag=True 也不组装）",
               got is probe and warns == [])
        got, warns = interview_core.resolve_retriever(db, None, False)
        _check("★ 都不给 → None（不注入就不接，绝不悄悄打开 RAG）",
               got is None and warns == [])
        got, warns = interview_core.resolve_retriever(db, None, True)
        _check("★ use_rag=True → 组装出真实检索器",
               isinstance(got, VectorKnowledgeRetriever) and warns == [],
               type(got).__name__)
        got, warns = interview_core.resolve_retriever(None, None, True)
        _check("★ 组装失败 → 降级为 None + 稳定 warning（不抛异常）",
               got is None and warns == [interview_core.WARNING_KNOWLEDGE_FAILED],
               str(warns))

        # --- retriever_kwargs 透传（「配了阈值」≠「阈值生效」的修复点）---
        # 默认 None 与不传等价：组装出的检索器配置必须与基线逐字段相同。
        base_r, _ = interview_core.resolve_retriever(db, None, True)
        none_r, _ = interview_core.resolve_retriever(db, None, True, retriever_kwargs=None)
        empty_r, _ = interview_core.resolve_retriever(db, None, True, retriever_kwargs={})
        _check("★ retriever_kwargs=None / {} ⇒ 与不传逐字段等价（默认行为不变）",
               (none_r.top_k, none_r.min_score, none_r.dedup)
               == (base_r.top_k, base_r.min_score, base_r.dedup)
               == (empty_r.top_k, empty_r.min_score, empty_r.dedup),
               f"{none_r.top_k}/{none_r.min_score} vs {base_r.top_k}/{base_r.min_score}")

        tuned, warns = interview_core.resolve_retriever(
            db, None, True, retriever_kwargs={"top_k": 3, "min_score": 0.25}
        )
        _check("★ retriever_kwargs 真正透传到 VectorKnowledgeRetriever",
               isinstance(tuned, VectorKnowledgeRetriever) and warns == []
               and tuned.top_k == 3 and tuned.min_score == 0.25,
               f"top_k={getattr(tuned, 'top_k', None)} min_score={getattr(tuned, 'min_score', None)}")
        _check("  └ 未提及的参数仍取检索器默认值（不是整体覆盖）",
               tuned.dedup is True, str(tuned.dedup))

        probe_kw, _ = interview_core.resolve_retriever(
            db, probe, True, retriever_kwargs={"top_k": 3}
        )
        _check("★ 注入了 retriever ⇒ retriever_kwargs 不被读取（注入者自带配置）",
               probe_kw is probe)

        bad_kw, bad_warns = interview_core.resolve_retriever(
            db, None, True, retriever_kwargs={"top_k": "not-an-int"}
        )
        _check("★ 非法 retriever_kwargs ⇒ 静默降级为「无知识」+ warning（不抛异常）",
               bad_kw is None
               and bad_warns == [interview_core.WARNING_KNOWLEDGE_FAILED],
               str(bad_warns))

        flow_sig = inspect.signature(interview_core.generate_next_question)
        _check("generate_next_question 新增 keyword-only use_rag（默认 False）",
               flow_sig.parameters["use_rag"].kind is inspect.Parameter.KEYWORD_ONLY
               and flow_sig.parameters["use_rag"].default is False)
        _check("  └ 既有参数顺序未变（db/session_id/user_id/spark/context/plan/retriever）",
               list(flow_sig.parameters)[:7] ==
               ["db", "session_id", "user_id", "spark", "context", "plan", "retriever"],
               str(list(flow_sig.parameters)))
        _check("  └ 新增 keyword-only retriever_kwargs（默认 None，追加在最后）",
               list(flow_sig.parameters)[-1] == "retriever_kwargs"
               and flow_sig.parameters["retriever_kwargs"].kind
               is inspect.Parameter.KEYWORD_ONLY
               and flow_sig.parameters["retriever_kwargs"].default is None,
               str(list(flow_sig.parameters)))
        gq_sig = inspect.signature(qg.generate_questions)
        _check("question_generator.generate_questions 也有 use_rag（默认 False）",
               gq_sig.parameters["use_rag"].default is False)
        _check("  └ 且也有 retriever_kwargs（默认 None）",
               gq_sig.parameters["retriever_kwargs"].default is None
               and gq_sig.parameters["retriever_kwargs"].kind
               is inspect.Parameter.KEYWORD_ONLY)
        for cls in (qg.QuestionGenerator, qg.RuleQuestionGenerator, qg.AgentQuestionGenerator):
            _check(f"  └ {cls.__name__}.generate 签名含 use_rag",
                   "use_rag" in inspect.signature(cls.generate).parameters)
            _check(f"     └ 也含 retriever_kwargs（接口统一）",
                   "retriever_kwargs" in inspect.signature(cls.generate).parameters)

        # ------------------------------------------------------------
        # [2] 组装器 knowledge_rag
        # ------------------------------------------------------------
        print("\n[2] 组装器 knowledge_rag（唯一一处知道用哪个 Embedding / 向量后端）")
        built = build_vector_retriever(db)
        _check("★ 组装出 VectorKnowledgeRetriever",
               isinstance(built, VectorKnowledgeRetriever), type(built).__name__)
        _check("  └ embedder 是默认占位实现 HashEmbeddingService",
               isinstance(built.embedder, HashEmbeddingService))
        _check("  └ store 是 SqlAlchemyVectorStore（复用 knowledge_chunk 扩展字段）",
               isinstance(built.store, SqlAlchemyVectorStore), type(built.store).__name__)
        _check("  └ 默认按 embedder.name 过滤模型（不同模型的向量不可比）",
               built.model_name == built.embedder.name, str(built.model_name))

        custom = HashEmbeddingService(dimension=32)
        built2 = build_vector_retriever(db, embedder=custom, top_k=2, dedup=False)
        _check("★ 注入 embedder 生效（换模型只改一个参数）",
               built2.embedder is custom and built2.model_name == custom.name)
        _check("  └ retriever_kwargs 原样透传（top_k / dedup）",
               built2.top_k == 2 and built2.dedup is False)
        try:
            build_vector_retriever(None)
            _check("★ db 为空 → RetrieverConfigError（明确报错，不静默）", False, "竟然构造成功")
        except RetrieverConfigError as exc:
            _check("★ db 为空 → RetrieverConfigError（明确报错，不静默）",
                   "db" in str(exc), str(exc))
        try:
            build_vector_retriever(db, top_k=0)
            _check("★ 非法 kwargs 透传到检索器并报错", False, "竟然构造成功")
        except RetrieverConfigError:
            _check("★ 非法 kwargs 透传到检索器并报错（错误信息更精确）", True)

        rag_src = (BACKEND_DIR / "services" / "knowledge_rag.py").read_text(encoding="utf-8")
        _check("★ 组装器的三个协作者都是**延迟导入**（模块顶层不拉 SQLAlchemy）",
               not ({"sqlalchemy", "models", "database", "services.vector_store_sql",
                     "services.embedding_service", "services.vector_knowledge_retriever"}
                    & _module_level_imports(rag_src)),
               str(sorted(_module_level_imports(rag_src))))

        # ------------------------------------------------------------
        # [3] 要求 2：无知识正常生成（知识库为空时先测）
        # ------------------------------------------------------------
        print("\n[3] 要求 2 · 无知识正常生成（空知识库 / 默认不接）")
        base_spark = MockSpark(QUESTION_JSON)
        baseline = await interview_core.generate_next_question(
            db, sid, spark=base_spark, context=CONTEXT, plan=PLAN
        )
        _check("默认（use_rag=False）能生成", baseline["ok"] is True,
               str(baseline.get("error")))
        baseline_prompt = base_spark.calls[0]
        _check("  └ 无知识基线 Prompt 不含「参考知识」", "参考知识" not in baseline_prompt)
        _check("  └ warnings 为空（没检索就什么都没发生）",
               baseline["warnings"] == [], str(baseline["warnings"]))

        rag_spark = MockSpark(QUESTION_JSON)
        empty_kb = await interview_core.generate_next_question(
            db, sid, spark=rag_spark, context=CONTEXT, plan=PLAN, use_rag=True
        )
        _check("★ use_rag=True 但知识库为空 → 仍能生成", empty_kb["ok"] is True,
               str(empty_kb.get("error")))
        _check("  └ Prompt 与无知识基线**逐字节相同**（空结果＝无知识，不是注入空串）",
               rag_spark.calls[0] == baseline_prompt)
        _check("  └ 检索到 0 条**不是失败**：warnings 仍为空",
               empty_kb["warnings"] == [], str(empty_kb["warnings"]))

        for label, kwargs in (
            ("空实现 KnowledgeRetriever()", {"retriever": KnowledgeRetriever()}),
            ("MockKnowledgeRetriever(chunks=[])", {"retriever": MockKnowledgeRetriever(chunks=[])}),
        ):
            sp = MockSpark(QUESTION_JSON)
            r = await interview_core.generate_next_question(
                db, sid, spark=sp, context=CONTEXT, plan=PLAN, **kwargs
            )
            _check(f"★ {label} → 仍能生成且 Prompt 与基线相同",
                   r["ok"] is True and sp.calls[0] == baseline_prompt
                   and r["warnings"] == [], str(r.get("error")))
        _check("  └ 结果字段集恒定（调用方无分支）", set(baseline) == set(empty_kb))

        # ------------------------------------------------------------
        # [4] 要求 1：Agent 有知识生成问题（**完整真实链路**）
        # ------------------------------------------------------------
        print("\n[4] 要求 1 · Agent 有知识生成问题（真实表 + 真实向量 + 真实检索器）")
        doc_id = await _seed_knowledge(db)          # 真实写入 knowledge_chunk
        rag_spark2 = MockSpark(QUESTION_JSON)
        with_knowledge = await interview_core.generate_next_question(
            db, sid, spark=rag_spark2, context=CONTEXT, plan=PLAN, use_rag=True
        )
        _check("★ 有知识时仍能生成", with_knowledge["ok"] is True,
               str(with_knowledge.get("error")))
        prompt = rag_spark2.calls[0]
        _check("★ Prompt 切到知识模板：含「参考知识：」", "参考知识：" in prompt)
        _check("★ 含**真实切片正文**（来自 knowledge_chunk 表，不是 Mock 片段）",
               "appendfsync" in prompt, prompt[-400:])
        _check("  └ 含来源标注（可溯源）",
               "manual://handbook/redis-persistence" in prompt)
        _check("  └ 含第 2 条切片（top_k 生效，确实从库里取回多条）",
               "Floyd" in prompt)
        _check("  └ 与无知识基线不同（确实注入了）", prompt != baseline_prompt)
        _check("  └ 检索成功 → warnings 为空",
               with_knowledge["warnings"] == [], str(with_knowledge["warnings"]))
        _check("  └ 结果字段集与无知识时一致", set(with_knowledge) == set(baseline))
        _check("  └ 题目经 Validator 标准化（不是原始 LLM 输出）",
               with_knowledge["question"] and with_knowledge["difficulty"] == "mid",
               str(with_knowledge.get("difficulty")))
        _check("  └ Agent 只被调用一次（检索不触发额外 LLM 调用）",
               len(rag_spark2.calls) == 1, str(len(rag_spark2.calls)))

        # 走生成策略层（agent 模式）也应同链路
        qg_spark = MockSpark(QUESTION_JSON)
        via_qg = await qg.generate_questions(
            db, sid, "agent", CONTEXT, PLAN, use_rag=True, spark=qg_spark
        )
        _check("★ 经 question_generator(mode='agent') 也走同一条 RAG 链路",
               via_qg["ok"] is True and "参考知识：" in qg_spark.calls[0]
               and "appendfsync" in qg_spark.calls[0], str(via_qg.get("error")))

        # ------------------------------------------------------------
        # [5] 要求 3：Rule 模式不触发 RAG
        # ------------------------------------------------------------
        print("\n[5] 要求 3 · Rule 模式不触发 RAG（AST + 运行时双重取证）")
        rule_node = _method_node(qg_src, "RuleQuestionGenerator", "generate")
        _check("取到 RuleQuestionGenerator.generate 的 AST 节点", rule_node is not None)
        _check("★ 规则生成器方法体内**没有任何检索调用**（AST 取证，非子串匹配）",
               _retrieval_calls(rule_node) == [], str(_retrieval_calls(rule_node)))
        _check("  └ 也不 import knowledge_rag / vector_* / retriever 相关模块",
               not any(
                   ("knowledge_rag" in m or "vector_store" in m
                    or "vector_knowledge_retriever" in m or "knowledge_retriever" in m)
                   for m in _imported_modules(qg_src)),
               str(sorted(_imported_modules(qg_src))))
        _check("  └ Core 里唯一真正调用检索的地方只有一处",
               core_src.count("await retrieve_knowledge(") == 1,
               str(core_src.count("await retrieve_knowledge(")))

        # 运行时：即使误传 use_rag=True + 会爆炸的检索器，规则出题也必须成功
        built_calls = {"n": 0}
        original_build = knowledge_rag.build_vector_retriever

        def _counting_build(*args, **kwargs):  # pragma: no cover - 不该被调用
            built_calls["n"] += 1
            raise AssertionError("rule 模式不应组装 RAG")

        knowledge_rag.build_vector_retriever = _counting_build
        try:
            boom_rule = ExplodingRetriever()
            rule_res = await qg.generate_questions(
                db, sid, "rule", CONTEXT, PLAN,
                retriever=boom_rule, use_rag=True, spark=ExplodingSpark(),
            )
        finally:
            knowledge_rag.build_vector_retriever = original_build
        _check("★ rule + use_rag=True + 爆炸检索器 → 仍然成功（不受影响）",
               rule_res["ok"] is True, str(rule_res.get("error")))
        _check("★ 规则模式**根本没调用**检索器", boom_rule.calls == 0,
               str(boom_rule.calls))
        _check("★ 规则模式**根本没组装** RAG（组装次数 0）", built_calls["n"] == 0,
               str(built_calls["n"]))
        _check("  └ 规则模式不调大模型（ExplodingSpark 未被触发）",
               rule_res["count"] > 0, str(rule_res.get("count")))

        # ------------------------------------------------------------
        # [6] 要求 4：RAG 失败不影响规则模式
        # ------------------------------------------------------------
        print("\n[6] 要求 4 · RAG 失败不影响规则模式")
        # 先取「完全不给 RAG 参数」的规则结果作为基线
        plain_rule = await qg.generate_questions(db, sid, "rule", CONTEXT, PLAN)

        class _BoomRetriever:
            def __init__(self):
                self.calls = 0

            async def retrieve(self, job_info, topic, context):
                self.calls += 1
                raise RuntimeError("向量库不可用")

        # (a) 检索抛异常：agent 降级为「无知识」+ warning
        boom = _BoomRetriever()
        sp_a = MockSpark(QUESTION_JSON)
        agent_boom = await interview_core.generate_next_question(
            db, sid, spark=sp_a, context=CONTEXT, plan=PLAN, retriever=boom
        )
        _check("★ agent 模式检索抛异常 → 仍能生成", agent_boom["ok"] is True,
               str(agent_boom.get("error")))
        _check("  └ 记入稳定 warning 码（不是 errors）",
               agent_boom["warnings"] == [interview_core.WARNING_KNOWLEDGE_FAILED]
               and agent_boom["errors"] == [], str(agent_boom["warnings"]))
        _check("  └ 降级为「无知识」：Prompt 与基线逐字节相同",
               sp_a.calls[0] == baseline_prompt)
        _check("  └ 检索器确实被调用过（是失败，不是没调）", boom.calls == 1)

        # (b) 组装抛异常：同样降级
        def _boom_build(*args, **kwargs):
            raise RuntimeError("组装 RAG 失败（比如向量后端连不上）")

        knowledge_rag.build_vector_retriever = _boom_build
        try:
            sp_b = MockSpark(QUESTION_JSON)
            agent_build_fail = await interview_core.generate_next_question(
                db, sid, spark=sp_b, context=CONTEXT, plan=PLAN, use_rag=True
            )
        finally:
            knowledge_rag.build_vector_retriever = original_build
        _check("★ agent 模式组装抛异常 → 仍能生成", agent_build_fail["ok"] is True,
               str(agent_build_fail.get("error")))
        _check("  └ 同样降级为「无知识」+ warning",
               agent_build_fail["warnings"] == [interview_core.WARNING_KNOWLEDGE_FAILED]
               and sp_b.calls[0] == baseline_prompt,
               str(agent_build_fail["warnings"]))

        # (c) 同一时刻跑规则模式：结果必须与「RAG 完全正常」时**逐字段相同**
        knowledge_rag.build_vector_retriever = _boom_build
        try:
            rule_boom = await qg.generate_questions(
                db, sid, "rule", CONTEXT, PLAN, use_rag=True, spark=ExplodingSpark()
            )
        finally:
            knowledge_rag.build_vector_retriever = original_build
        _check("★ RAG 组装炸掉时，规则出题结果与基线**逐字段相同**",
               rule_boom == plain_rule,
               f"{rule_boom} != {plain_rule}")
        _check("  └ 规则结果里没有 warning（RAG 与它无关）",
               rule_boom["errors"] == [] and "warnings" not in rule_boom,
               str(sorted(rule_boom)))

        boom2 = _BoomRetriever()
        rule_boom2 = await qg.generate_questions(
            db, sid, "rule", CONTEXT, PLAN, retriever=boom2, use_rag=True
        )
        _check("★ RAG 检索炸掉时，规则出题结果同样逐字段相同",
               rule_boom2 == plain_rule, f"{rule_boom2} != {plain_rule}")
        _check("  └ 且规则模式一次都没调那个爆炸检索器", boom2.calls == 0)

        # (d) rule 结果与「有知识」时的 agent 结果互不影响
        _check("  └ 同一场面试里 rule 结果与 agent+知识结果互不干扰",
               with_knowledge["ok"] is True and rule_boom2["ok"] is True
               and rule_boom2["mode"] == "rule" and with_knowledge["ok"] is True)

        # ------------------------------------------------------------
        # [7] 隔离守卫
        # ------------------------------------------------------------
        print("\n[7] 隔离守卫（依赖方向 / 延迟导入 / 子进程）")
        agent_src = inspect.getsource(interview_agent)
        _check("★ InterviewAgent 未 import 检索器 / 组装器（知识由调用方传入）",
               not any(("knowledge_retriever" in m or "knowledge_rag" in m
                        or "vector_" in m) for m in _imported_modules(agent_src)),
               str(sorted(_imported_modules(agent_src))))
        _check("  └ Agent 源码里没有任何 retrieve 调用",
               not any(isinstance(n, ast.Attribute) and n.attr == "retrieve"
                       for n in ast.walk(ast.parse(agent_src))))
        _check("  └ Agent 仍只通过 knowledge_context 参数接收知识",
               "knowledge_context" in inspect.signature(
                   interview_agent.generate_question).parameters)

        _check("★ Core **模块顶层**不 import knowledge_rag（延迟导入，保持零耦合）",
               not any("knowledge_rag" in m
                       for m in _module_level_imports(core_src)),
               str(sorted(_module_level_imports(core_src))))
        _check("  └ Core 模块顶层也不 import sqlalchemy / models / database",
               not ({"sqlalchemy", "models", "database"}
                    & {m.split(".")[0] for m in _module_level_imports(core_src)}),
               str(sorted(_module_level_imports(core_src))))
        _check("  └ question_generator 不 import knowledge_rag（不自己组装）",
               not any("knowledge_rag" in m for m in _imported_modules(qg_src)))
        _check("  └ interview_service 不 import knowledge_rag（只透传）",
               not any("knowledge_rag" in m for m in _imported_modules(
                   inspect.getsource(interview_service))))

        # 组装器是全项目唯一知道「用哪个 Embedding / 哪个向量后端」的地方，
        # 消费者恰好三处、一读一写一维护（任务 46 加入写侧；任务 74 加入维护侧）：
        #   * interview_core            —— 读侧：resolve_retriever / retrieve_knowledge
        #   * knowledge_import_pipeline —— 写侧：入库时 build_vector_store / default_embedder
        #   * knowledge_maintenance     —— 维护：重建派生索引时 build_vector_store
        #   * knowledge_embedding_migration —— 迁移：换模型后重算既有切片向量
        # 仍然是**闭集 + 精确相等**，不是「谁都能 import」。
        consumers = []
        for path in _production_files():
            if path.name == "knowledge_rag.py":
                continue
            if any("knowledge_rag" in m
                   for m in _imported_modules(path.read_text(encoding="utf-8"))):
                consumers.append(path.relative_to(BACKEND_DIR).as_posix())
        _check("★ 组装器 knowledge_rag 的消费者 = 读侧 Core + 写侧 Pipeline + 维护 + 迁移模块",
               consumers == [
                   "services/interview_core.py",
                   "services/knowledge_embedding_migration.py",
                   "services/knowledge_import_pipeline.py",
                   "services/knowledge_maintenance.py",
               ], str(consumers))

        _check("★ start_session 仍走纯规则出题（RAG 不进入默认业务路径）",
               core_src.count("build_question_plan(") >= 1
               and "use_rag" not in inspect.signature(
                   interview_core.build_question_plan).parameters)
        _check("  └ 规则出题计划函数签名里没有任何检索参数",
               not ({"retriever", "use_rag", "knowledge_context"}
                    & set(inspect.signature(interview_core.build_question_plan).parameters)))

    await engine.dispose()

    # 子进程：无 DATABASE_URL 也能 import Core 与组装器（延迟导入的证明）
    # 注意：fastapi **不在**探测列表里 —— interview_core 顶层确实 import 了
    # ``from fastapi import HTTPException``（build_report 沿用旧行为，模块文档
    # 记为「分层例外」），这是既有事实、不是本次 RAG 引入的泄漏。
    # 本检查要证明的是：延迟导入没有把 **数据层**（sqlalchemy/models/database）
    # 以及 DB 驱动拉进 sys.modules。
    env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
    code = (
        "import sys; sys.path.insert(0, r'%s');"
        "import services.knowledge_rag as rag;"
        "import services.interview_core as core;"
        "leak = [x for x in ('sqlalchemy','aiomysql','pymysql','models',"
        "'database') if x in sys.modules];"
        "print('LEAK:' + ','.join(leak));"
        "print('OK:' + core.resolve_retriever.__name__ + '/' + rag.__name__)" % str(BACKEND_DIR)
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                          env=env, encoding="utf-8", cwd=str(BACKEND_DIR))
    _check("★ 子进程（无 DATABASE_URL）可 import Core 与组装器",
           proc.returncode == 0 and "OK:resolve_retriever" in (proc.stdout or ""),
           (proc.stderr or "")[-400:])
    leak_line = next((line for line in (proc.stdout or "").splitlines()
                      if line.startswith("LEAK:")), None)
    leaked = (leak_line or "LEAK:?").split(":", 1)[1].strip()
    _check("  └ 导入后 sqlalchemy / models / database 未泄漏进 sys.modules",
           leaked == "", leaked or "（未取到 LEAK 行）")

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
