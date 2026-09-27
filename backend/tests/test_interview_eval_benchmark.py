# -*- coding: utf-8 -*-
"""AI 面试问题评估基准 · 自检（脚本式，非 pytest）。

基准文件：``backend/scripts/interview_eval_benchmark.json``
说明文档：``docs/AI面试问题评估基准.md``

运行：``python backend/tests/test_interview_eval_benchmark.py``

本套件验证什么
--------------
**不是**「跑一次对比看谁好」——本轮只建立**测试数据与评价标准**。所以本套件验证的是
「这份基准**本身成立**」：

[1] **前置与契约**——业务文件 + Prompt 模板 + 语料文件的**源码指纹**锁死
    （「不改 Agent / Prompt / RAG」是被验证的）；基准文件的顶层结构与
    评价字段集合合法；gate / metric 的 ``kind`` 属于**闭集**；
    说明文档与 JSON 不漂移。
[2] **格式与自洽校验**（纯静态）——场景数量与维度覆盖、枚举合法、
    ``mock_reply`` 自身合法、诱饵 source 真实存在且与命中集合不相交、
    筛选表的 ``usable`` 与 rank/gap 自洽。
[3] **实测：候选 topic 筛选表复算**——把 22 篇语料导入内存 SQLite，
    逐条重算 rank / score / gap，与声明的 12 行**逐个比对**。
    这一步是基准可信度的地基：默认 embedder 是**词面哈希**，
    「语义上相关」**不等于**「检索得到」。
[4] **实测：每个场景的「有 RAG 臂」确实 armed**——Planner 真实产出的 plan 里
    含有声明的 ``query_topic``；``current_topic`` 等于声明值；
    检索命中集合**恰好**等于 ``expected_hit_sources``、条数一致、诱饵被排除。
[5] **实测：两臂实跑一次，条件差异「恰好」是知识注入**——两臂 plan 与
    10 个基础变量逐键相同；无 RAG 臂在**知识库非空**时仍不注入任何知识；
    有 RAG 臂的 Prompt 去掉知识小节后与无 RAG 臂**逐字节相同**；
    两臂都只调模型一次。
[6] **实测：五个检查字段的来源与闸门**（provenance 探针）——
    ``stage`` **不是**模型输出（塞假值也不生效）；``question_type`` **不做枚举校验**
    （非法值原样透传）；``difficulty`` 非法值被拦（并记录**先由谁拦**）；
    ``question`` 为空被拦；``reason`` 缺失**不阻断**。
[7] **条件冻结**——每个场景的 ``frozen_conditions_sha256`` 与实测一致
    （条件漂移会立刻失败，而不是静默地比出「假差异」）。

.. note::
    本套件**只读**基准与生产代码，不写任何业务文件、不改任何业务逻辑。
    「条件冻结」的失败信息里会打印新摘要，重新冻结只需改 JSON 里一行。
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
from models import DIFFICULTIES, INTERVIEW_STAGES, INTERVIEW_TYPES  # noqa: E402
from models import InterviewSession, Job, Resume, User  # noqa: E402
from prompts import parse_question_output  # noqa: E402
from services import interview_agent, interview_context, interview_core  # noqa: E402
from services.interview_agent import build_question_variables  # noqa: E402
from services.interview_planner import build_plan  # noqa: E402
from services.knowledge_import_pipeline import KnowledgeImportPipeline  # noqa: E402
from services.knowledge_rag import default_embedder  # noqa: E402
from services.question_validator import (  # noqa: E402
    ERROR_INVALID_DIFFICULTY,
    ERROR_QUESTION_EMPTY,
)
from services.vector_knowledge_retriever import VectorKnowledgeRetriever  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

# ============================================================
# 一、常量
# ============================================================
BENCHMARK_PATH = BACKEND_DIR / "scripts" / "interview_eval_benchmark.json"
CORPUS_PATH = BACKEND_DIR / "scripts" / "interview_knowledge.json"
DOC_PATH = PROJECT_DIR / "docs" / "AI面试问题评估基准.md"

#: 「不改 Agent / Prompt / RAG」的判据：规范化换行后的源码 sha256。
#: 语料文件也在内——筛选表的期望值是**绑定到这份语料**的，语料变了必须重测。
FROZEN_SOURCES: Tuple[Tuple[str, str], ...] = (
    ("services/interview_agent.py",
     "cbc4b7c33ad591c6a325cb53cbb41f2819abe0e813d2487badc3cd9f98f0d09a"),
    ("services/interview_core.py",
     "b8da0caa24577cc21b134666a8da602bd6f8d701bf287c62163840fc7e949d51"),
    ("services/interview_planner.py",
     "0da3961aac2dc9af81fa321c7c56149934cc39d8ff016cb285ea5ad07d9b8df6"),
    ("services/question_validator.py",
     "b23e6957edf39696fcc4803b4b627fbdc90c1548de86535bc804f4acc1c7e53b"),
    ("prompts/interview/question.txt",
     "34c522e0b6fd06d088b606ed17945b10ab74b94452f0de9a163374d455c57b17"),
    ("prompts/interview/question_knowledge.txt",
     "1c141c6b9c8613f0c2af762d6a46cdbc193e339fc60e49fdf7e4cf9f1f620194"),
    ("scripts/interview_knowledge.json",
     "fe72792e8982648b8c7e43024f83d42bc8863924ffdd276d08a047df351e4311"),
)

#: 需求点名的五个输出检查字段（顺序即报告顺序）。
REQUIRED_CHECK_FIELDS: Tuple[str, ...] = (
    "question", "question_type", "difficulty", "reason", "stage",
)

#: 需求点名的五个场景维度 → 在场景对象里的落点路径。
REQUIRED_SCENARIO_DIMENSIONS: Tuple[Tuple[str, str], ...] = (
    ("岗位类型", "job_type"),
    ("面试阶段", "context.current_stage"),
    ("候选人信息", "candidate.resume_content"),
    ("技能方向", "skill_direction"),
    ("难度", "session.difficulty"),
)

#: 基础模板的 10 个变量（顺序即 build_question_variables 的返回顺序）。
BASE_VARIABLE_NAMES: Tuple[str, ...] = (
    "resume_summary", "job_title", "job_description", "interview_type",
    "difficulty", "interview_plan", "current_stage", "asked_questions",
    "covered_topics", "weak_topics",
)

#: 两个臂的 id。
ARM_OFF = "rag_off"
ARM_ON = "rag_on"

#: gate / metric 的**闭集** kind——声明一个未实现的 kind 应当失败，而不是被忽略。
KNOWN_KINDS: Tuple[str, ...] = (
    "non_empty_text",
    "enum_member",
    "equals_declared",
    "not_duplicate_of_history",
    "text_length",
    "contains_any",
    "longest_common_substring",
)

#: 「可用 topic」判据：必须 rank 1 且间隔 > 该阈值。
USABLE_GAP = 0.02

#: 知识小节标题（有 RAG 臂的 Prompt 里应当出现、无 RAG 臂不应当出现）。
KNOWLEDGE_HEADING = "## 四·五、参考知识（外部检索结果，可选）"
KNOWLEDGE_END = "## 五、本场进度（用于避免重复）"

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


def _fingerprint(relative_path: str) -> str:
    raw = (BACKEND_DIR / relative_path).read_bytes()
    return hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def _dig(obj: Any, path: str) -> Any:
    """按点号路径取值（``"context.current_stage"``）。"""
    for part in path.split("."):
        if not isinstance(obj, dict) or part not in obj:
            return None
        obj = obj[part]
    return obj


def _conditions_digest(scenario: Dict[str, Any], variables: Dict[str, Any]) -> str:
    """「测试条件」的指纹：场景声明 + 派生出的 plan 载荷 + 10 个基础变量。

    刻意**不含** Prompt 正文（模板变更另有 task 64 的模板指纹管），
    只锁「输入条件」——两臂之间、跨运行之间都不该变。
    """
    payload = {
        "scenario_id": scenario["id"],
        "retrieval": scenario["retrieval"],
        "plan_payload": variables["interview_plan"],
        "base_variables": {k: v for k, v in variables.items() if k != "interview_plan"},
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                           separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _mock_reply_text(scenario: Dict[str, Any]) -> str:
    return json.dumps(scenario["mock_reply"], ensure_ascii=False)


def _knowledge_block(prompt: str) -> str:
    """截出「参考知识」小节（含标题行，到下一节标题前）。"""
    start = prompt.index(KNOWLEDGE_HEADING)
    end = prompt.index(KNOWLEDGE_END)
    return prompt[start:end]


def _longest_common_substring(left: str, right: str) -> int:
    """最长公共子串长度（滚动数组，O(len(left)*len(right)) 时间、O(len(right)) 空间）。"""
    if not left or not right:
        return 0
    previous = [0] * (len(right) + 1)
    best = 0
    for i in range(1, len(left) + 1):
        current = [0] * (len(right) + 1)
        for j in range(1, len(right) + 1):
            if left[i - 1] == right[j - 1]:
                current[j] = previous[j - 1] + 1
                best = max(best, current[j])
        previous = current
    return best


# ============================================================
# 三、环境
# ============================================================
def _load_benchmark() -> Dict[str, Any]:
    return json.loads(BENCHMARK_PATH.read_text(encoding="utf-8"))


def _load_corpus() -> List[Dict[str, Any]]:
    return json.loads(CORPUS_PATH.read_text(encoding="utf-8"))["documents"]


@asynccontextmanager
async def _env(scenario: Dict[str, Any], *, with_corpus: bool = True,
               documents: Optional[Sequence[Dict[str, Any]]] = None
               ) -> Iterator[Dict[str, Any]]:
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

        session.add(User(username="eval_bench", email="eval_bench@example.com",
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

        # 真实 Planner 走**数据库加载路径**产出计划
        # （与流程内部同一个函数 `_build_session_plan`；use_llm=False ⇒ 确定性）
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
        reports: List[Dict[str, Any]] = []
        if with_corpus:
            pipeline = KnowledgeImportPipeline(session, embedder=embedder, store=store)
            for doc in (documents if documents is not None else _load_corpus()):
                reports.append(await pipeline.import_document(dict(doc)))

        retrieval = scenario["retrieval"]
        retriever = VectorKnowledgeRetriever(
            embedder, store, top_k=retrieval["top_k"],
            min_score=retrieval["min_score"],
        )

        yield {
            "db": session,
            "row": row,
            "session_id": session_id,
            "job": job,
            "resume": resume,
            "plan": plan,
            "context": context,
            "store": store,
            "reports": reports,
            "retriever": retriever,
        }
    finally:
        if session is not None:
            await session.close()
        await engine.dispose()


async def _run_arm(env: Dict[str, Any], scenario: Dict[str, Any], arm: str
                   ) -> Tuple[Dict[str, Any], SpySpark, Dict[str, Any]]:
    """跑一个臂：返回 ``(Core 结果, spy, 捕获到的 Agent 知识入参)``。"""
    spy = SpySpark(_mock_reply_text(scenario))
    retriever = env["retriever"] if arm == ARM_ON else None
    async with _record_knowledge_context() as captured:
        result = await interview_core.generate_next_question(
            env["db"], env["session_id"],
            retriever=retriever, use_rag=False, spark=spy,
        )
    return result, spy, captured


# ============================================================
# [1] 前置与契约
# ============================================================
def check_preconditions(benchmark: Dict[str, Any]) -> None:
    _section("[1] 前置与契约（「不改 Agent / Prompt / RAG」是被验证的）")

    for relative_path, expected in FROZEN_SOURCES:
        actual = _fingerprint(relative_path)
        _check(f"★ {relative_path} 指纹未变",
               actual == expected, f"实际 {actual[:12]}… 期望 {expected[:12]}…")

    # 守卫自检：指纹对「被改过的内容」必须报不同（否则守卫恒为真）
    probe = "".join(["a", "b"]) + "c"
    _check("守卫自检：内容变化会改变指纹",
           hashlib.sha256(probe.encode("utf-8")).hexdigest()
           != hashlib.sha256("ab".encode("utf-8")).hexdigest())

    # -- 基准文件结构 --
    for key in ("name", "version", "purpose", "scope", "format", "comparison",
                "check_fields", "evaluation_contract", "retrieval_contract",
                "topic_screening", "scenarios", "kind_vocabulary"):
        _check(f"基准文件含顶层键 {key!r}", key in benchmark)
    _check("version 是整数（改基准须同时升版本）",
           isinstance(benchmark.get("version"), int), repr(benchmark.get("version")))

    # -- 评价字段集合 --
    names = [f["name"] for f in benchmark["check_fields"]]
    _check("★ 检查字段恰好是需求点名的 5 个（顺序一致）",
           tuple(names) == REQUIRED_CHECK_FIELDS, str(names))
    _check("★ 5 个字段都是 Core 返回契约的成员（不是自造字段）",
           set(names) <= set(interview_core.QUESTION_RESULT_FIELDS),
           str(sorted(set(names) - set(interview_core.QUESTION_RESULT_FIELDS))))
    _check("每个字段都声明了 type / provenance / hard_gates / observations / ab_expectation",
           all({"type", "provenance", "hard_gates", "observations", "ab_expectation"}
               <= set(f) for f in benchmark["check_fields"]))

    # -- kind 闭集 --
    declared_kinds = set()
    for field in benchmark["check_fields"]:
        for entry in [*field["hard_gates"], *field["observations"]]:
            declared_kinds.add(entry["kind"])
    _check("★ 所有 gate / metric 的 kind 都属于闭集（不会静默忽略未实现的 kind）",
           declared_kinds <= set(KNOWN_KINDS),
           str(sorted(declared_kinds - set(KNOWN_KINDS))))
    _check("闭集里的 kind 至少被用到一次（无死条目）",
           declared_kinds == set(KNOWN_KINDS),
           str(sorted(set(KNOWN_KINDS) - declared_kinds)))
    _check("kind_vocabulary 与 KNOWN_KINDS 完全一致（说明与实际不漂移）",
           set(benchmark["kind_vocabulary"]) == set(KNOWN_KINDS))

    # -- equals_declared 的 compare_to 必须指向真实存在的场景字段 --
    for field in benchmark["check_fields"]:
        for gate in field["hard_gates"] + field["observations"]:
            if gate["kind"] == "equals_declared":
                _check(f"  └ {field['name']}.{gate['id']} 声明了 compare_to",
                       "compare_to" in gate, str(gate))
            if gate["kind"] in ("enum_member",):
                _check(f"  └ {field['name']}.{gate['id']} 声明了非空 allowed",
                       bool(gate.get("allowed")), str(gate))

    # -- 说明文档不漂移 --
    _check("说明文档存在", DOC_PATH.is_file(), str(DOC_PATH))
    if DOC_PATH.is_file():
        doc = DOC_PATH.read_text(encoding="utf-8")
        missing = [n for n in REQUIRED_CHECK_FIELDS if n not in doc]
        _check("★ 文档覆盖全部 5 个检查字段名", not missing, str(missing))
        arms = [a["id"] for a in benchmark["comparison"]["arms"]]
        _check("★ 文档覆盖两个臂 id", all(a in doc for a in arms), str(arms))
        _check("★ 文档引用了基准文件路径",
               "interview_eval_benchmark.json" in doc)


# ============================================================
# [2] 格式与自洽校验（纯静态）
# ============================================================
def check_static_consistency(benchmark: Dict[str, Any]) -> None:
    _section("[2] 格式与自洽校验（纯静态，不碰数据库）")

    scenarios = benchmark["scenarios"]
    _check("场景数量为 3（三个不同岗位/阶段/难度组合）",
           len(scenarios) == 3, str(len(scenarios)))
    ids = [s["id"] for s in scenarios]
    _check("场景 id 唯一", len(set(ids)) == len(ids), str(ids))

    corpus_sources = {d["source"] for d in _load_corpus()}
    _check("语料可独立加载且 source 唯一",
           len(corpus_sources) == len(_load_corpus()), str(len(corpus_sources)))

    seen_dimensions: Dict[str, set] = {label: set() for label, _ in REQUIRED_SCENARIO_DIMENSIONS}
    for scenario in scenarios:
        sid = scenario["id"]
        for label, path in REQUIRED_SCENARIO_DIMENSIONS:
            value = _dig(scenario, path)
            _check(f"[{sid}] 维度「{label}」有落点且非空",
                   value not in (None, "", [], {}), repr(value))
            if isinstance(value, str):
                seen_dimensions[label].add(value)
            elif isinstance(value, list):
                seen_dimensions[label].add(tuple(value))

        # 枚举合法
        _check(f"[{sid}] difficulty ∈ models.DIFFICULTIES",
               scenario["session"]["difficulty"] in DIFFICULTIES,
               scenario["session"]["difficulty"])
        _check(f"[{sid}] interview_type ∈ models.INTERVIEW_TYPES",
               scenario["session"]["interview_type"] in INTERVIEW_TYPES,
               scenario["session"]["interview_type"])
        _check(f"[{sid}] current_stage ∈ models.INTERVIEW_STAGES",
               scenario["context"]["current_stage"] in INTERVIEW_STAGES,
               scenario["context"]["current_stage"])

        # mock_reply 自洽（否则「固定条件」是假的）
        reply = scenario["mock_reply"]
        _check(f"[{sid}] mock_reply 的 difficulty == 场景难度",
               reply["difficulty"] == scenario["session"]["difficulty"],
               f"{reply['difficulty']} vs {scenario['session']['difficulty']}")
        _check(f"[{sid}] mock_reply 的 question_type ∈ 模板声明的枚举",
               reply["question_type"] in
               ("introduction", "resume", "technical", "project",
                "scenario", "hr", "closing"),
               reply["question_type"])
        _check(f"[{sid}] mock_reply 的 question 非空且不短于 10 字",
               len(reply["question"]) >= 10, str(len(reply["question"])))
        _check(f"[{sid}] mock_reply 的 question 与历史问题不重复",
               not any(interview_agent.question_similarity(
                   reply["question"], q) >= interview_agent.DUPLICATE_THRESHOLD
                   for q in scenario["context"]["asked_questions"]))
        _check(f"[{sid}] mock_reply 的 topic 非空",
               bool(str(reply["topic"]).strip()), repr(reply["topic"]))

        # retrieval 声明自洽
        ret = scenario["retrieval"]
        hits = set(ret["expected_hit_sources"])
        excluded = set(ret["expected_excluded_sources"])
        _check(f"[{sid}] expected_hit_sources 非空",
               bool(hits), str(sorted(hits)))
        _check(f"[{sid}] expected_hit_sources ⊆ 语料 source",
               hits <= corpus_sources, str(sorted(hits - corpus_sources)))
        _check(f"[{sid}] ★ expected_excluded_sources ⊆ 语料 source（诱饵必须是真文档）",
               excluded <= corpus_sources, str(sorted(excluded - corpus_sources)))
        _check(f"[{sid}] ★ 命中与诱饵不相交（声明不能自相矛盾）",
               not (hits & excluded), str(sorted(hits & excluded)))
        _check(f"[{sid}] top_k / min_score 类型与范围合法",
               isinstance(ret["top_k"], int) and ret["top_k"] > 0
               and isinstance(ret["min_score"], (int, float))
               and not isinstance(ret["min_score"], bool)
               and 0 < ret["min_score"] < 1,
               f"top_k={ret['top_k']} min_score={ret['min_score']}")
        _check(f"[{sid}] ★ 声明了 min_score（不设阈值 ⇒ 「有 RAG」臂会被噪声污染）",
               ret["min_score"] is not None)

        # 筛选表必须收录本场景的 topic，且标记为可用
        row = next((r for r in benchmark["topic_screening"]["rows"]
                    if r["query"] == ret["query_topic"]), None)
        _check(f"[{sid}] query_topic 在筛选表里", row is not None,
               ret["query_topic"])
        if row is not None:
            _check(f"[{sid}] ★ 且筛选表标记为 usable（否则场景不可用）",
                   row["usable"] is True, str(row))
            _check(f"[{sid}] 筛选表的目标文档与场景声明一致",
                   row["target_source"] in hits, str(row["target_source"]))

    _check("★ 三个场景在「难度」维度上至少有 2 种取值",
           len(seen_dimensions["难度"]) >= 2, str(sorted(seen_dimensions["难度"])))
    _check("★ 三个场景在「面试阶段」维度上至少有 2 种取值",
           len(seen_dimensions["面试阶段"]) >= 2, str(sorted(seen_dimensions["面试阶段"])))
    _check("★ 三个场景在「岗位类型」维度上至少有 2 种取值",
           len(seen_dimensions["岗位类型"]) >= 2, str(sorted(seen_dimensions["岗位类型"])))

    # -- 筛选表自洽 --
    rows = benchmark["topic_screening"]["rows"]
    _check("筛选表覆盖全部候选 topic（12 条）", len(rows) == 12, str(len(rows)))
    _check("筛选表 query 唯一", len({r["query"] for r in rows}) == len(rows))
    bad = [r["query"] for r in rows
           if r["usable"] != (r["target_rank"] == 1 and r["score_gap"] > USABLE_GAP)]
    _check("★ 每行的 usable 与其 rank / gap 自洽（判据不是手填的）",
           not bad, str(bad))
    _check("★ 筛选表里同时存在 usable=true 与 usable=false（否则没有筛选意义）",
           {r["usable"] for r in rows} == {True, False},
           str(sorted({r["usable"] for r in rows})))
    _check("筛选表 target_source 都在语料里",
           all(r["target_source"] in corpus_sources for r in rows),
           str([r["target_source"] for r in rows
                if r["target_source"] not in corpus_sources]))

    # -- 两臂定义 --
    arms = {a["id"]: a for a in benchmark["comparison"]["arms"]}
    _check("恰好两个臂：rag_off / rag_on", set(arms) == {ARM_OFF, ARM_ON}, str(sorted(arms)))
    _check("无 RAG 臂声明 retriever=None / use_rag=False",
           "retriever=None" in arms[ARM_OFF]["how"] and "use_rag=False" in arms[ARM_OFF]["how"])
    _check("★ 有 RAG 臂声明**显式注入** retriever（而非 use_rag=True）",
           "retriever=VectorKnowledgeRetriever" in arms[ARM_ON]["how"]
           and "use_rag=False" in arms[ARM_ON]["how"])
    _check("★ 并说明了「为何不用 use_rag=True」（它不传 retriever_kwargs ⇒ 钉不住参数）",
           "retriever_kwargs" in arms[ARM_ON]["why_explicit_retriever_not_use_rag"])
    _check("两臂的期望 Prompt 模板不同（分流是处理的固有部分）",
           arms[ARM_OFF]["expected_prompt_template"]
           != arms[ARM_ON]["expected_prompt_template"])
    _check("★ 声明了 LLM 非确定性及其对本基准的影响（只用 Mock 比输入条件）",
           "Mock" in benchmark["comparison"]["llm_nondeterminism"])


# ============================================================
# [3] 实测：候选 topic 筛选表复算
# ============================================================
async def check_topic_screening(benchmark: Dict[str, Any]) -> None:
    _section("[3] 实测 · 候选 topic 筛选表复算（22 篇语料 / 12 条 query）")

    corpus = _load_corpus()
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:", poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        db = factory()
        embedder = default_embedder()
        store = SqlAlchemyVectorStore(db)
        pipeline = KnowledgeImportPipeline(db, embedder=embedder, store=store)
        reports = [await pipeline.import_document(dict(doc)) for doc in corpus]
        stored = await store.count()
        _check(f"语料全部入库（{len(corpus)} 篇 → {stored} 片）",
               len(reports) == 22 and all(r["status"] == "ok" for r in reports)
               and stored == sum(r["chunk_count"] for r in reports),
               f"报告 {len(reports)} 份、切片 {stored}")

        print(f"\n  {'query':<24}{'rank':<6}{'目标分':<11}{'次高分':<11}{'间隔':<11}usable")
        mismatches: List[str] = []
        for row in benchmark["topic_screening"]["rows"]:
            retriever = VectorKnowledgeRetriever(embedder, store, top_k=100,
                                                 min_score=None)
            chunks = await retriever.retrieve(None, row["query"], {})
            best: Dict[str, float] = {}
            for chunk in chunks:
                best.setdefault(chunk.source, chunk.metadata["score"])
            target_score = best[row["target_source"]]
            others = sorted((v for k, v in best.items()
                             if k != row["target_source"]), reverse=True)
            rank = sorted(best.values(), reverse=True).index(target_score) + 1
            gap = target_score - others[0]
            usable = rank == 1 and gap > USABLE_GAP

            matched = (
                rank == row["target_rank"]
                and round(target_score, 6) == round(row["target_score"], 6)
                and round(others[0], 6) == round(row["runner_up_score"], 6)
                and round(gap, 6) == round(row["score_gap"], 6)
                and usable == row["usable"]
            )
            if not matched:
                mismatches.append(row["query"])
            print(f"  {row['query']:<24}{rank:<6}{round(target_score, 6):<11}"
                  f"{round(others[0], 6):<11}{round(gap, 6):<11}{usable}")

        _check("★ 12 行筛选结果与声明**逐行一致**（rank / 目标分 / 次高分 / 间隔 / usable）",
               not mismatches, str(mismatches))
        _check("★ 存在不可用的候选（证明筛选真的在筛东西）",
               any(not r["usable"] for r in benchmark["topic_screening"]["rows"]))
        await db.close()
    finally:
        await engine.dispose()


# ============================================================
# [4] 实测：每个场景的「有 RAG 臂」确实 armed
# ============================================================
async def check_scenarios_armed(benchmark: Dict[str, Any]) -> None:
    _section("[4] 实测 · 每个场景的「有 RAG 臂」是否真的 armed")

    for scenario in benchmark["scenarios"]:
        sid = scenario["id"]
        ret = scenario["retrieval"]
        async with _env(scenario) as env:
            plan = env["plan"]
            context = env["context"]

            # 注意：语料是「22 篇文档 → N 个切片」（长文档会切成多片），
            # 因此断言的是「切片数 == 导入报告之和」，不是「== 文档数」。
            chunks_in_store = await env["store"].count()
            _check(f"[{sid}] 语料全部入库（22 篇 → {chunks_in_store} 片）",
                   len(env["reports"]) == 22
                   and all(r["status"] == "ok" for r in env["reports"])
                   and chunks_in_store == sum(r["chunk_count"] for r in env["reports"]),
                   f"报告 {len(env['reports'])} 份、切片 {chunks_in_store}")
            _check(f"[{sid}] 场景 topic 出现在 plan.priority_topics 或 target_topics 里",
                   ret["query_topic"] in (plan["priority_topics"] + plan["target_topics"]),
                   str(plan["priority_topics"]))
            _check(f"[{sid}] ★ current_topic(plan, context) == 声明的 query_topic",
                   interview_core.current_topic(plan, context) == ret["query_topic"],
                   f"实际 {interview_core.current_topic(plan, context)!r} "
                   f"期望 {ret['query_topic']!r}")
            _check(f"[{sid}] 播种的 covered_topics 确实生效（被回读出来）",
                   context["covered_topics"] == scenario["context"]["covered_topics"],
                   str(context["covered_topics"]))

            chunks = await env["retriever"].retrieve(
                scenario["job"], ret["query_topic"], context)
            sources = sorted({c.source for c in chunks})
            _check(f"[{sid}] ★ 检索命中集合 == expected_hit_sources（无诱饵混入）",
                   sources == sorted(ret["expected_hit_sources"]), str(sources))
            _check(f"[{sid}] ★ 切片条数 == expected_chunk_count",
                   len(chunks) == ret["expected_chunk_count"], str(len(chunks)))
            _check(f"[{sid}] ★ 声明的诱饵 source 一条都没被检索到",
                   set(ret["expected_excluded_sources"]).isdisjoint(sources),
                   str(sorted(set(ret["expected_excluded_sources"]) & set(sources))))
            _check(f"[{sid}] 检索 query 已记录（便于观测）",
                   env["retriever"].queries == [ret["query_topic"]],
                   str(env["retriever"].queries))
            _check(f"[{sid}] 同输入同输出（重复检索结果一致）",
                   await env["retriever"].retrieve(
                       scenario["job"], ret["query_topic"], context) == chunks)


# ============================================================
# [5] 实测：两臂实跑，条件差异「恰好」是知识注入
# ============================================================
async def check_two_arms(benchmark: Dict[str, Any]) -> None:
    _section("[5] 实测 · 两臂各跑一次，条件差异恰好是知识注入")

    for scenario in benchmark["scenarios"]:
        sid = scenario["id"]
        ret = scenario["retrieval"]
        async with _env(scenario) as env:
            print(f"\n  ── {sid} ──")
            off_result, off_spy, off_cap = await _run_arm(env, scenario, ARM_OFF)
            on_result, on_spy, on_cap = await _run_arm(env, scenario, ARM_ON)

            # -- 两臂都有效 --
            for arm, result in ((ARM_OFF, off_result), (ARM_ON, on_result)):
                _check(f"[{sid}/{arm}] 出题成功且 errors/warnings 为空",
                       result["ok"] is True and result["errors"] == []
                       and result["warnings"] == [],
                       f"ok={result['ok']} errors={result['errors']} "
                       f"warnings={result['warnings']}")

            # -- 唯一处理变量 --
            stored = await env["store"].count()
            _check(f"[{sid}] ★ 无 RAG 臂注入的知识恰好是 []（空列表，非 None）",
                   off_cap["knowledge_context"] == [], repr(off_cap["knowledge_context"]))
            _check(f"[{sid}] ★ 无 RAG 臂在**知识库非空**时仍不注入任何知识"
                   f"（证明处理变量是「是否注入」，不是「库里有没有」）",
                   stored > 0 and off_cap["knowledge_context"] == [],
                   f"库里 {stored} 片、注入 {len(off_cap['knowledge_context'] or [])} 条")
            _check(f"[{sid}] ★ 有 RAG 臂注入的知识条数 == expected_chunk_count",
                   len(on_cap["knowledge_context"] or []) == ret["expected_chunk_count"],
                   str(len(on_cap["knowledge_context"] or [])))
            _check(f"[{sid}] ★ 有 RAG 臂注入的知识来源 == expected_hit_sources",
                   sorted({c.source for c in on_cap["knowledge_context"]})
                   == sorted(ret["expected_hit_sources"]),
                   str(sorted({c.source for c in on_cap["knowledge_context"]})))

            # -- 两臂的 10 个基础变量逐键相同 --
            _check(f"[{sid}] ★ 两臂的 10 个基础变量**逐键相同**（上下文/计划未被处理影响）",
                   off_cap["variables"] == on_cap["variables"],
                   str([k for k in BASE_VARIABLE_NAMES
                        if off_cap["variables"][k] != on_cap["variables"][k]]))
            _check(f"[{sid}] 变量集合恰好 10 个（知识不在基础变量里）",
                   tuple(off_cap["variables"]) == BASE_VARIABLE_NAMES,
                   str(tuple(off_cap["variables"])))

            # -- Prompt 差异恰好是知识小节 --
            off_prompt, on_prompt = off_spy.calls[0], on_spy.calls[0]
            _check(f"[{sid}] ★ 无 RAG 臂 Prompt **不含**参考知识小节",
                   KNOWLEDGE_HEADING not in off_prompt)
            _check(f"[{sid}] ★ 有 RAG 臂 Prompt **含**参考知识小节",
                   KNOWLEDGE_HEADING in on_prompt)
            _check(f"[{sid}] ★ 有 RAG 臂 Prompt 含注入知识的正文（逐字节）",
                   all(c.content in on_prompt for c in on_cap["knowledge_context"]))
            _check(f"[{sid}] ★ 删掉知识小节后两臂 Prompt **逐字节相同**"
                   f"（⇒ 差异可归因于知识注入）",
                   on_prompt.replace(_knowledge_block(on_prompt), "") == off_prompt,
                   f"有知识 {len(on_prompt)} 字符 / 无知识 {len(off_prompt)} 字符")
            _check(f"[{sid}] 两臂 Prompt 都无未渲染占位符",
                   "{{" not in off_prompt and "{{" not in on_prompt)

            # -- 一次流程 = 一次模型调用 --
            _check(f"[{sid}] 两臂都只调用模型一次（每题一次 LLM 调用）",
                   off_spy.call_count == 1 and on_spy.call_count == 1,
                   f"off={off_spy.call_count} on={on_spy.call_count}")

            # -- 输出侧的「输入条件一致」：stage / difficulty 两臂必须相同 --
            _check(f"[{sid}] ★ 两臂的 stage 与 difficulty 相同（输入条件一致）",
                   off_result["stage"] == on_result["stage"]
                   and off_result["difficulty"] == on_result["difficulty"],
                   f"{off_result['stage']}/{off_result['difficulty']} vs "
                   f"{on_result['stage']}/{on_result['difficulty']}")
            _check(f"[{sid}] stage == 场景声明的 current_stage（硬闸门）",
                   off_result["stage"] == scenario["context"]["current_stage"]
                   and on_result["stage"] == scenario["context"]["current_stage"],
                   str(off_result["stage"]))
            _check(f"[{sid}] difficulty == 场景声明的难度（硬闸门）",
                   off_result["difficulty"] == scenario["session"]["difficulty"]
                   and on_result["difficulty"] == scenario["session"]["difficulty"],
                   str(off_result["difficulty"]))

            # -- 观察指标确实可计算（为后续对比铺路） --
            knowledge_text = "\n".join(c.content for c in on_cap["knowledge_context"])
            lcs = _longest_common_substring(on_result["question"], knowledge_text)
            print(f"    观察指标示例：question 长度 off={len(off_result['question'])} "
                  f"on={len(on_result['question'])}；"
                  f"question 与知识的最长公共子串={lcs} 字")
            # 度量自检：把知识原文当问题，最长公共子串应等于知识全长
            # （否则说明度量恒 0，用它比较「照抄程度」就是假指标）
            sanity = _longest_common_substring(knowledge_text, knowledge_text)
            _check(f"[{sid}] 观察指标自检：知识原文当问题 ⇒ 最长公共子串 == 知识长度"
                   f"（度量敏感、非恒 0）",
                   sanity == len(knowledge_text) and sanity > 0,
                   f"{sanity} vs 知识长度 {len(knowledge_text)}")


# ============================================================
# [6] 实测：五个检查字段的来源与闸门（provenance 探针）
# ============================================================
async def check_field_provenance(benchmark: Dict[str, Any]) -> None:
    _section("[6] 实测 · 五个检查字段的来源与闸门（provenance 探针）")

    scenario = benchmark["scenarios"][0]
    sid = scenario["id"]

    # ---- stage：不是模型输出 ----
    async with _env(scenario, with_corpus=False) as env:
        bogus = dict(scenario["mock_reply"])
        bogus["stage"] = "closing"          # 模型「声称」自己在收尾阶段
        spy = SpySpark(json.dumps(bogus, ensure_ascii=False))
        result = await interview_core.generate_next_question(
            env["db"], env["session_id"], retriever=None, use_rag=False, spark=spy)
        _check(f"[{sid}] ★ stage **不是**模型输出：塞入 stage='closing' 也不生效",
               result["stage"] == scenario["context"]["current_stage"]
               and result["stage"] != "closing",
               f"stage={result['stage']!r}（场景声明 "
               f"{scenario['context']['current_stage']!r}）")
        # 真实取证：结构归一确实把未知键丢掉了（不是靠「结果恰好对」蒙对）
        parsed = parse_question_output(json.dumps(bogus, ensure_ascii=False))
        _check("  └ 未知键 stage 在结构归一时被丢弃（只留 6 个业务字段）",
               parsed["ok"] is True and "stage" not in parsed,
               str(sorted(parsed)))

    # ---- question_type：不做枚举校验 ----
    async with _env(scenario, with_corpus=False) as env:
        odd = dict(scenario["mock_reply"])
        odd["question_type"] = "not_a_real_type"
        spy = SpySpark(json.dumps(odd, ensure_ascii=False))
        result = await interview_core.generate_next_question(
            env["db"], env["session_id"], retriever=None, use_rag=False, spark=spy)
        _check("★ question_type 不做枚举校验：非法值**原样透传**且流程成功",
               result["ok"] is True and result["question_type"] == "not_a_real_type",
               f"ok={result['ok']} question_type={result['question_type']!r}")
        # 真实取证：基准文件确实**没有**给 question_type 声明硬闸门（声明与实现一致）
        field = next(f for f in benchmark["check_fields"]
                     if f["name"] == "question_type")
        _check("  └ 因此基准把 question_type 的 hard_gates 留空（声明与实现一致）",
               field["hard_gates"] == [], str(field["hard_gates"]))

    # ---- difficulty：非法值被拦（先由 Agent 内联规则拦） ----
    async with _env(scenario, with_corpus=False) as env:
        bad_diff = dict(scenario["mock_reply"])
        bad_diff["difficulty"] = "god_mode"
        spy = SpySpark(json.dumps(bad_diff, ensure_ascii=False),
                       json.dumps(bad_diff, ensure_ascii=False))   # 修复也返回非法值
        result = await interview_core.generate_next_question(
            env["db"], env["session_id"], retriever=None, use_rag=False, spark=spy)
        _check("★ difficulty 非法值 ⇒ 流程失败（ok=False）",
               result["ok"] is False, f"ok={result['ok']} errors={result['errors']}")
        _check("★ 且**先由 Agent 内联规则拦下**（触发一次修复后仍不通过 ⇒ agent_failed）",
               interview_core.ERROR_AGENT_FAILED in result["errors"]
               and spy.call_count == 2,
               f"errors={result['errors']} calls={spy.call_count}")

        # Validator 侧是同一规则的第二道（直接调用可见其错误码）
        validation = interview_core.validate_candidate_question(
            {"question": "这是一个足够长的问题正文。", "topic": "MySQL 索引",
             "difficulty": "god_mode"},
            env["context"], env["plan"])
        _check("★ Validator 直接调用时给出 invalid_difficulty（架构化演进的第二道）",
               validation.valid is False
               and ERROR_INVALID_DIFFICULTY in validation.errors,
               str(validation.errors))

    # ---- question：为空被拦 ----
    async with _env(scenario, with_corpus=False) as env:
        empty = dict(scenario["mock_reply"])
        empty["question"] = ""
        spy = SpySpark(json.dumps(empty, ensure_ascii=False),
                       json.dumps(empty, ensure_ascii=False))
        result = await interview_core.generate_next_question(
            env["db"], env["session_id"], retriever=None, use_rag=False, spark=spy)
        _check("★ question 为空 ⇒ 流程失败（ok=False），且**绝不伪造问题**",
               result["ok"] is False and result["question"] == "",
               f"ok={result['ok']} question={result['question']!r}")
        validation = interview_core.validate_candidate_question(
            {"question": "   ", "topic": "MySQL 索引", "difficulty": "mid"},
            env["context"], env["plan"])
        _check("★ Validator 直接调用时给出 question_empty",
               validation.valid is False and ERROR_QUESTION_EMPTY in validation.errors,
               str(validation.errors))

    # ---- reason：缺失不阻断 ----
    async with _env(scenario, with_corpus=False) as env:
        no_reason = {k: v for k, v in scenario["mock_reply"].items() if k != "reason"}
        spy = SpySpark(json.dumps(no_reason, ensure_ascii=False))
        result = await interview_core.generate_next_question(
            env["db"], env["session_id"], retriever=None, use_rag=False, spark=spy)
        _check("★ reason 缺失 ⇒ **不阻断**，流程成功且 reason 为 \"\"",
               result["ok"] is True and result["reason"] == "",
               f"ok={result['ok']} reason={result['reason']!r}")

    # ---- 字段集恒定 ----
    async with _env(scenario, with_corpus=False) as env:
        result, _, _ = await _run_arm(env, scenario, ARM_OFF)
        _check("★ 成功结果字段集恒定为 QUESTION_RESULT_FIELDS（12 键）",
               tuple(result) == interview_core.QUESTION_RESULT_FIELDS, str(tuple(result)))


# ============================================================
# [7] 条件冻结
# ============================================================
async def check_conditions_frozen(benchmark: Dict[str, Any]) -> None:
    _section("[7] 条件冻结（条件漂移会立刻失败，而不是比出「假差异」）")

    for scenario in benchmark["scenarios"]:
        sid = scenario["id"]
        async with _env(scenario, with_corpus=False) as env:
            # 用与流程内部同一个 Planner 函数复算一次计划，确保条件可复现
            cfg = scenario["session"]
            plan_again = build_plan(
                interview_type=cfg["interview_type"], difficulty=cfg["difficulty"],
                duration=cfg["duration"], job=scenario["job"],
                resume=scenario["candidate"]["resume_content"],
                total_questions=cfg["total_questions"],
            )
            # 两条路径的**唯一**差异：Planner 主入口 `build_plan_for` 在 build_plan
            # 结果末尾追加 job_id / resume_id。键序也必须一致 ⇒ 用元组严格比对，
            # 任何多出来的键、或正文任何一个值不同，都会立刻失败。
            appended = ("job_id", "resume_id")
            _check(f"[{sid}] ★ 数据库加载路径产出的 plan == 纯函数路径 + 仅追加 "
                   f"job_id/resume_id（键序一致、正文逐键相同）",
                   tuple(env["plan"]) == tuple(plan_again) + appended
                   and {k: v for k, v in env["plan"].items() if k not in appended}
                   == plan_again,
                   f"DB {tuple(env['plan'])} / 纯函数 {tuple(plan_again)}")
            _check(f"[{sid}] 追加的两个键取**会话行**的外键（非默认值、非 None）",
                   env["job"].id is not None and env["resume"].id is not None
                   and env["plan"]["job_id"] == env["job"].id
                   and env["plan"]["resume_id"] == env["resume"].id,
                   f"job_id={env['plan']['job_id']} resume_id={env['plan']['resume_id']}")
            _check(f"[{sid}] 计划确实由**会话行**的配置驱动"
                   f"（difficulty / interview_type / total_questions）",
                   env["plan"]["difficulty"] == cfg["difficulty"]
                   and env["plan"]["interview_type"] == cfg["interview_type"]
                   and env["plan"]["total_questions"] == cfg["total_questions"]
                   and env["plan"]["source"] == "rule",
                   f"{env['plan']['interview_type']}/{env['plan']['difficulty']}/"
                   f"{env['plan']['total_questions']}/{env['plan']['source']}")

            variables = build_question_variables(
                env["context"], env["plan"], env["resume"], env["job"])
            actual = _conditions_digest(scenario, variables)
            expected = scenario["frozen_conditions_sha256"]
            _check(f"[{sid}] ★ frozen_conditions_sha256 与实测一致",
                   actual == expected,
                   f"实际 {actual} / 声明 {expected or '（空，需冻结）'}")


# ============================================================
# 报告
# ============================================================
def report(benchmark: Dict[str, Any]) -> None:
    print("\n" + "=" * 74)
    print("AI 面试问题评估基准 · 总览")
    print("=" * 74)
    print(f"  基准文件：{BENCHMARK_PATH.relative_to(PROJECT_DIR)}")
    print(f"  说明文档：{DOC_PATH.relative_to(PROJECT_DIR)}")
    print(f"  版本：v{benchmark['version']}　场景数：{len(benchmark['scenarios'])}")
    print("\n  两个臂（唯一变量 = 是否注入检索到的知识）：")
    for arm in benchmark["comparison"]["arms"]:
        print(f"    {arm['id']:<8}{arm['label']:<8}"
              f"模板={arm['expected_prompt_template']}")
    print("\n  固定面试场景：")
    print(f"    {'id':<28}{'岗位类型':<22}{'阶段':<11}{'难度':<8}{'技能方向'}")
    for s in benchmark["scenarios"]:
        print(f"    {s['id']:<28}{s['job_type']:<22}"
              f"{s['context']['current_stage']:<11}{s['session']['difficulty']:<8}"
              f"{'/'.join(s['skill_direction'])}")
    print("\n  输出检查字段（5 个）：")
    for field in benchmark["check_fields"]:
        print(f"    {field['name']:<16}硬闸门 {len(field['hard_gates'])} 个"
              f"　观察指标 {len(field['observations'])} 个")
    print("\n  候选 topic 筛选（默认 embedder 是词面哈希 ⇒ 必须实测）：")
    rows = benchmark["topic_screening"]["rows"]
    usable = [r for r in rows if r["usable"]]
    print(f"    可用 {len(usable)}/{len(rows)}；不可用的原因是"
          f"「目标文档不是 rank 1」或「与次高分间隔 ≤ {USABLE_GAP}」")
    print("\n" + "=" * 74)
    print("结果")
    print("=" * 74)
    print("  本套件只验证「基准本身成立」：格式 / 自洽 / 筛选表 / armed / 两臂条件 / 字段来源")
    print("  **未**执行两臂的生成质量对比（本轮只建立测试数据与评价标准）")
    print("  保护文件指纹：7 个（4 业务模块 + 2 Prompt 模板 + 1 语料文件）")
    print(f"  断言：通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)


async def main() -> None:
    print("=" * 74)
    print("AI 面试问题评估基准 · 自检（无 RAG vs 有 RAG 的测试条件与评价标准）")
    print("=" * 74)

    benchmark = _load_benchmark()
    check_preconditions(benchmark)
    check_static_consistency(benchmark)
    await check_topic_screening(benchmark)
    await check_scenarios_armed(benchmark)
    await check_two_arms(benchmark)
    await check_field_provenance(benchmark)
    await check_conditions_frozen(benchmark)
    report(benchmark)


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(0 if _FAILED == 0 else 1)
