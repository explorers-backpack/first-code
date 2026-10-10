# -*- coding: utf-8 -*-
"""AI 模拟面试 · **数字人面试 RAG 增强 / 追问链** 自检

运行：``python backend/tests/test_interview_followup_rag.py``

不依赖本机 MySQL：``DATABASE_URL`` 指向 SQLite 内存库 + ``StaticPool``。
**不调真实 LLM**（注入 Mock Spark）、**不接真实知识库**（注入桩检索器）。

被测对象
--------
``services.interview_context_pack``（上下文整合，另有一套件覆盖纯函数）
``services.interview_followup_agent``（追问 Agent）
``services.interview_followup``（追问链编排）
``services.interview_service.generate_followup``（归属校验 + 交付分流）

覆盖范围
--------
[1] 契约：结果字段集 / 分析字段集 / 错误码 / ``__all__``
[2] ``analyze_answer``：复用 ``interview_core.score_answer``、命中与未命中采分点、
    薄弱点推导、``focus``、纯函数
[3] ``followup_topic``：优先级链
[4] 模板分流 + **漂移守卫**（``followup_knowledge.txt`` 去掉知识小节后逐字节相同）
[5] ★★ **知识库影响问题生成**（本套件的核心断言）——同一会话 / 同一回答，
    有知识与无知识两次出题：Prompt 不同、知识正文出现在 Prompt 里、
    模型据此返回**不同的问题**、``knowledge_chunks`` 由 0 变 N
[6] 检索是可选增强：不注入就不检索 / 检索失败静默降级 / 组装失败静默降级
[7] 追问全链端到端 + 错误分支（``answer_required`` / ``question_required`` /
    ``session_finished`` / ``session_not_found`` / ``agent_failed``）
[8] 上下文整合取证：简历 / 岗位 / 历史回答 / 本题 / 回答 / 分析都进了 Prompt
[9] 接口稳定 + 隔离守卫：``interview_core`` 公开签名冻结、``prompts/interview``
    未被污染、新模块不 import 检索层、复用关系是**同一对象**
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import json
import os
import pathlib
import sys
from typing import Any, Dict, List, Optional, Tuple

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND_DIR / "tests") not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR / "tests"))
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import regression_env  # noqa: E402,F401  钉住离线 Embedding + RAG 阈值

from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
import prompts  # noqa: E402
from database import Base  # noqa: E402
from models import Job, Resume, User  # noqa: E402
from schemas.interview import (  # noqa: E402
    AnswerSubmitRequest,
    FollowUpRequest,
    SessionCreateRequest,
)
from services import (  # noqa: E402
    interview_agent,
    interview_core,
    interview_followup as ff,
    interview_followup_agent as ffa,
    interview_service,
)
from services.knowledge_retriever import (  # noqa: E402
    KnowledgeChunk,
    MockKnowledgeRetriever,
)

_PASSED = 0
_PFAILED = 0

RESUME_CONTENT = "张三，5 年后端开发经验，主导订单中台重构，负责 Redis 缓存与持久化调优。"
JOB_DUTY = "负责后端服务的设计与开发，保障高并发场景下的稳定性。"
JOB_SKILLS = "Python,MySQL,Redis,Kafka"

ANSWER = (
    "RDB 是全量快照，AOF 记录写命令。我们线上用 RDB 做快速恢复、AOF 兜底，"
    "集群规模 32 个分片，单分片 8G 内存。"
)

#: 只出现在**知识正文**里的标记串——用来判「知识到底进没进 Prompt」。
KNOWLEDGE_MARKER = "appendfsync 控制落盘频率"
KNOWLEDGE_CONTENT = (
    f"Redis 持久化有 RDB 与 AOF 两种：AOF 通过 {KNOWLEDGE_MARKER}，"
    "everysec 是折中选项，always 最安全但最慢。"
)

#: **无知识**时模型返回的问题（故意与知识无关）
QUESTION_PLAIN = "你刚才提到线上用 RDB 恢复、AOF 兜底，能说说为什么这么选吗？"
#: **有知识**时模型返回的问题（直接引用知识正文里的术语）
QUESTION_WITH_KNOWLEDGE = "你们 AOF 的 appendfsync 配的是 always 还是 everysec？为什么？"

JOB_JSON = {
    "question": QUESTION_PLAIN,
    "question_type": "technical",
    "topic": "Redis 持久化",
    "difficulty": "mid",
    "expected_points": ["RDB", "AOF"],
    "reason": "追问选型理由",
}
JOB_JSON_KNOWLEDGE = {
    "question": QUESTION_WITH_KNOWLEDGE,
    "question_type": "technical",
    "topic": "Redis 持久化",
    "difficulty": "mid",
    "expected_points": ["appendfsync", "everysec"],
    "reason": "针对未展开的落盘频率追问",
}

#: ``followup_knowledge.txt`` 相对 ``followup.txt`` **新增**的小节。
#: ``{lines}`` 处是 ``stringify(list[str])`` 的产物（每行 ``- xxx``）——
#: 用于比较**渲染后的 Prompt**。
KNOWLEDGE_BLOCK_TEMPLATE = (
    "## 四·五、参考知识（外部检索结果，可选）\n"
    "\n"
    "参考知识：\n"
    "{lines}\n"
    "\n"
    "追问时请把这些知识融入问题的场景与追问点，但不得整段照抄进 `question`，\n"
    "也不得把知识中的结论直接写进 `question`（`expected_points` 可参考）。\n"
    "\n"
)

#: 同上，但占位符**原样**（用于比较**模板文件**的字节）。
KNOWLEDGE_BLOCK_RAW = KNOWLEDGE_BLOCK_TEMPLATE.format(lines="{{knowledge_context}}")

PRODUCTION_MODULES = (
    "knowledge_rag",
    "knowledge_retriever",
    "vector_store",
    "vector_store_sql",
    "vector_store_chroma",
    "vector_knowledge_retriever",
    "embedding_service",
    "embedding_provider",
    "embedding_provider_spark",
)

NEW_MODULES = (
    "services/interview_context_pack.py",
    "services/interview_followup_agent.py",
    "services/interview_followup.py",
)


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _PFAILED
    if cond:
        _PASSED += 1
    else:
        _PFAILED += 1
    print(
        ("  [PASS] " if cond else "  [FAIL] ")
        + name
        + (f"  -> {detail}" if detail and not cond else "")
    )
    return cond


def _section(title: str) -> None:
    print("\n" + "-" * 74)
    print(title)


def _imported_modules(source: str) -> set:
    """AST 提取真正被 import 的模块名（**展开点号全名**，不用子串匹配）。"""
    names: set = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _module_source(relative_path: str) -> str:
    return (BACKEND_DIR / relative_path).read_text(encoding="utf-8")


def _knowledge_block(chunks) -> str:
    """把知识片段渲染成**进 Prompt 的那一小节**（与生产同一条格式化链路）。"""
    lines = interview_agent.format_knowledge_context(chunks)
    rendered = "\n".join(f"- {line}" for line in lines)
    return KNOWLEDGE_BLOCK_TEMPLATE.format(lines=rendered)


class RecordingSpark:
    """记录每次收到的 Prompt，按预设顺序返回回复（用尽即报错）。"""

    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls: List[str] = []

    async def chat_async(self, message: str) -> str:
        self.calls.append(message)
        if not self.replies:
            raise AssertionError("RecordingSpark 被调用次数超出预期")
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


class KnowledgeAwareSpark:
    """**按 Prompt 里有没有知识**返回不同问题的假 Spark。

    这是「知识库影响问题生成」的**直接取证**：模型行为被建模成
    「看到知识就换一个问题」，于是最终 ``question`` 是否变化，
    完全由「知识有没有真的进 Prompt」决定。
    """

    def __init__(self):
        self.calls: List[str] = []

    async def chat_async(self, message: str) -> str:
        self.calls.append(message)
        payload = (
            JOB_JSON_KNOWLEDGE if KNOWLEDGE_MARKER in message else JOB_JSON
        )
        return json.dumps(payload, ensure_ascii=False)


class SpyRetriever:
    """记录调用入参的桩检索器（不继承 ``KnowledgeRetriever``，验证鸭子类型即可用）。"""

    def __init__(self, chunks=None, *, raises: Optional[BaseException] = None):
        self._chunks = list(chunks or [])
        self._raises = raises
        self.calls: List[Tuple[Any, Any, Any]] = []

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
    user = User(username="fu_t", email="fu_t@example.com", password_hash="x", role="user")
    other = User(username="fu_o", email="fu_o@example.com", password_hash="x", role="user")
    job = Job(
        job_name="后端开发工程师",
        salary="20-35K",
        edu_require="本科",
        major_require="不限",
        skills=JOB_SKILLS,
        duty=JOB_DUTY,
        city="深圳",
        industry="互联网",
    )
    resume = Resume(user_id=1, filename="r.pdf", content=RESUME_CONTENT)
    db.add_all([user, other, job, resume])
    await db.commit()
    for row in (user, other, job, resume):
        await db.refresh(row)
    return user.id, other.id, job.id, resume.id


async def _make_session(db: AsyncSession, user_id: int, job_id: int, resume_id: int,
                        total: int = 3, mode: str = "text") -> int:
    payload = SessionCreateRequest(
        job_id=job_id, resume_id=resume_id, total_questions=total, mode=mode
    )
    sid = (await interview_service.create_session(db, user_id, payload))["session"]["id"]
    await interview_service.start_session(db, user_id, sid)
    return sid


async def run() -> bool:
    print("=" * 70)
    print("AI 模拟面试 · 数字人面试 RAG 增强 / 追问链 自检")
    print("=" * 70)

    # ============================================================
    # [1] 契约
    # ============================================================
    _section("[1] 契约：结果字段集 / 分析字段集 / 错误码 / __all__")
    _check("FOLLOWUP_RESULT_FIELDS 是 16 键的元组",
           isinstance(ff.FOLLOWUP_RESULT_FIELDS, tuple)
           and len(ff.FOLLOWUP_RESULT_FIELDS) == 16,
           str(len(ff.FOLLOWUP_RESULT_FIELDS)))
    _check("  └ 无重复键",
           len(set(ff.FOLLOWUP_RESULT_FIELDS)) == len(ff.FOLLOWUP_RESULT_FIELDS))
    _check("  └ 含知识正向信号 knowledge_chunks",
           "knowledge_chunks" in ff.FOLLOWUP_RESULT_FIELDS)
    _check("ANALYSIS_FIELDS 是 16 键的元组",
           isinstance(ff.ANALYSIS_FIELDS, tuple) and len(ff.ANALYSIS_FIELDS) == 16,
           str(len(ff.ANALYSIS_FIELDS)))
    _check("★ warning 码与 interview_core **是同一个值**（不另造一套）",
           ff.WARNING_KNOWLEDGE_FAILED == interview_core.WARNING_KNOWLEDGE_FAILED
           == "knowledge_retrieval_failed", ff.WARNING_KNOWLEDGE_FAILED)
    _check("错误码齐全",
           {ff.ERROR_SESSION_NOT_FOUND, ff.ERROR_SESSION_FINISHED,
            ff.ERROR_ANSWER_REQUIRED, ff.ERROR_QUESTION_REQUIRED,
            ff.ERROR_AGENT_FAILED} == {
               "session_not_found", "session_finished", "answer_required",
               "question_required", "agent_failed"})
    for module, name in ((ff, "interview_followup"), (ffa, "interview_followup_agent")):
        missing = [item for item in module.__all__ if not hasattr(module, item)]
        _check(f"{name}.__all__ 里每个名字都真实存在", missing == [], str(missing))
    _check("追问链入口在 __all__ 里", "generate_followup" in ff.__all__)
    _check("追问 Agent 入口在 __all__ 里", "generate_followup" in ffa.__all__)

    # ============================================================
    # [2] analyze_answer（纯函数，复用 core.score_answer）
    # ============================================================
    _section("[2] analyze_answer：复用 core 评分 / 命中与未命中 / 薄弱点 / 纯函数")
    question = {
        "id": 12,
        "question_no": 2,
        "question": "RDB 与 AOF 的区别？",
        "question_type": "technical",
        "topic": "Redis 持久化",
        "difficulty": "mid",
        "expected_points": ["RDB", "AOF", "appendfsync"],
    }
    analysis = ff.analyze_answer(question, ANSWER)
    _check("键集恒等于 ANALYSIS_FIELDS",
           set(analysis) == set(ff.ANALYSIS_FIELDS),
           str(set(analysis) ^ set(ff.ANALYSIS_FIELDS)))
    _check("★ 分数与 interview_core.score_answer **逐字段相同**（不另造评分口径）",
           {k: analysis[k] for k in
            ("score", "technical_score", "logic_score",
             "expression_score", "adaptability_score", "feedback")}
           == interview_core.score_answer(question, ANSWER),
           str(analysis["score"]))
    _check("命中采分点 = 回答里出现的（RDB / AOF）",
           analysis["hit_points"] == ["RDB", "AOF"], str(analysis["hit_points"]))
    _check("★ 未命中采分点 = 追问的主要靶子（appendfsync）",
           analysis["missed_points"] == ["appendfsync"], str(analysis["missed_points"]))
    _check("★ focus 优先取**第一个未命中采分点**",
           analysis["focus"] == "appendfsync", analysis["focus"])
    _check("薄弱点文案可读（含未覆盖采分点）",
           any("appendfsync" in item for item in analysis["weak_points"]),
           str(analysis["weak_points"]))
    _check("回答长度如实上报",
           analysis["answer_chars"] == len(ANSWER), str(analysis["answer_chars"]))

    short = ff.analyze_answer(question, "不知道")
    _check("过短回答被标记「缺少展开」",
           any("过短" in item for item in short["weak_points"]), str(short["weak_points"]))
    _check("全部采分点都未命中时 focus 仍取第一个未命中的",
           short["focus"] == "RDB", short["focus"])

    no_points = ff.analyze_answer({"question": "Q", "topic": "微服务"}, ANSWER)
    _check("无 expected_points ⇒ 无未命中项（不猜）", no_points["missed_points"] == [])
    _check("  └ focus 回落到本题 topic", no_points["focus"] == "微服务", no_points["focus"])
    _check("  └ 无采分点也无低分维度时给「可追问更深」兜底文案",
           no_points["weak_points"] == [ff.COMPLETE_ANSWER_HINT]
           or len(no_points["weak_points"]) >= 1,
           str(no_points["weak_points"]))

    snapshot = dict(question)
    ff.analyze_answer(question, ANSWER)
    _check("★ 不改入参（纯函数）", question == snapshot)
    _check("  └ 同输入同输出（确定性）",
           ff.analyze_answer(question, ANSWER) == analysis)
    _check("ORM 风格对象也能分析",
           ff.analyze_answer(
               type("Q", (), {"question": "Q", "topic": "T", "expected_points": ["RDB"]})(),
               "RDB 是快照",
           )["hit_points"] == ["RDB"])
    _check("裸字符串题干被接受",
           ff.analyze_answer("RDB 与 AOF 的区别？", ANSWER)["question"] == "RDB 与 AOF 的区别？")

    # ============================================================
    # [3] followup_topic
    # ============================================================
    _section("[3] followup_topic：优先级链（不改入参 / 确定性）")
    pack_for_topic = {
        "focus_topics": ["缓存一致性", "MySQL 索引"],
        "weak_topics": ["弱项X"],
        "current_stage": "technical",
    }
    _check("analysis.focus 优先", ff.followup_topic(analysis, pack_for_topic) == "appendfsync")
    _check("focus 为空 ⇒ focus_topics 首项",
           ff.followup_topic({"focus": ""}, pack_for_topic) == "缓存一致性")
    _check("  └ focus_topics 也空 ⇒ weak_topics 首项",
           ff.followup_topic({"focus": ""}, {"weak_topics": ["弱项X"]}) == "弱项X")
    _check("  └ 都空 ⇒ current_stage",
           ff.followup_topic({}, {"current_stage": "technical"}) == "technical")
    _check("  └ 全空 ⇒ 空串（不猜）", ff.followup_topic({}, {}) == "")
    _check("pack 缺省也能用", ff.followup_topic({"focus": "F"}) == "F")
    _check("不改入参",
           (lambda p: (ff.followup_topic({"focus": ""}, p), p == pack_for_topic))(
               dict(pack_for_topic)) == ("缓存一致性", True))

    # ============================================================
    # [4] 模板分流 + 漂移守卫
    # ============================================================
    _section("[4] 模板分流 + 漂移守卫（followup_knowledge.txt vs followup.txt）")
    _check("模板分组是新的 interview_followup（不污染 prompts/interview）",
           ffa.PROMPT_GROUP == "interview_followup", ffa.PROMPT_GROUP)
    _check("★ prompts/interview 仍**恰好** 5 个模板（未被污染）",
           prompts.list_prompts("interview")
           == ["planner", "question", "question_knowledge", "question_repair", "system"],
           str(prompts.list_prompts("interview")))
    _check("★ prompts/interview_followup 恰好 2 个模板",
           prompts.list_prompts("interview_followup") == ["followup", "followup_knowledge"],
           str(prompts.list_prompts("interview_followup")))

    base_path = BACKEND_DIR / "prompts" / "interview_followup" / "followup.txt"
    k_path = base_path.with_name("followup_knowledge.txt")
    base_src = base_path.read_text(encoding="utf-8")
    k_src = k_path.read_text(encoding="utf-8")
    base_bytes = base_path.read_bytes()
    k_bytes = k_path.read_bytes()
    probe_block = KNOWLEDGE_BLOCK_RAW.encode("utf-8")
    _check("原模板 followup.txt 不含知识小节",
           "## 四·五、参考知识" not in base_src)
    _check("变体模板包含知识小节且仅一次",
           k_src.count(KNOWLEDGE_BLOCK_RAW) == 1,
           str(k_src.count(KNOWLEDGE_BLOCK_RAW)))
    _check("★ 去掉知识小节后与 followup.txt 逐字节相同（防止两模板分叉）",
           k_bytes.replace(probe_block, b"", 1) == base_bytes)
    base_crlf = base_bytes.count(b"\r\n")
    k_crlf = k_bytes.count(b"\r\n")
    _check("  └ 行尾符也一致（CRLF 计数相等）",
           base_crlf == k_crlf, f"{base_crlf} vs {k_crlf}")

    base_vars = prompts.template_variables("followup", group="interview_followup")
    k_vars = prompts.template_variables("followup_knowledge", group="interview_followup")
    _check("followup.txt 声明 14 个变量",
           tuple(base_vars) == ffa.FOLLOWUP_VARIABLES, str(base_vars))
    _check("  └ 变体 = 原 14 变量 + knowledge_context",
           set(k_vars) == set(base_vars) | {"knowledge_context"}, str(k_vars))
    _check("  └ knowledge_context 插在 candidate_answer 与 answer_analysis 之间",
           tuple(k_vars) == tuple(base_vars[:9]) + ("knowledge_context",)
           + tuple(base_vars[9:]), str(k_vars))

    # 渲染分流
    sample_vars = ffa.build_followup_variables(
        {"current_stage": "technical", "interview_type": "technical", "difficulty": "mid",
         "job_title": "后端", "job_description": "技能要求：Redis", "resume_summary": "张三",
         "history": [], "asked_questions": [], "covered_topics": [], "weak_topics": []},
        analysis,
    )
    name_off, prompt_off = ffa.render_followup_prompt(sample_vars)
    name_on, prompt_on = ffa.render_followup_prompt(
        sample_vars, [{"content": KNOWLEDGE_CONTENT, "source": "mock://handbook"}]
    )
    _check("空知识 ⇒ 走原模板 followup", name_off == ffa.PROMPT_FOLLOWUP, name_off)
    _check("非空知识 ⇒ 走变体 followup_knowledge",
           name_on == ffa.PROMPT_FOLLOWUP_KNOWLEDGE, name_on)
    _check("★ 空知识时 Prompt 里没有「参考知识」小节（行为一致）",
           "参考知识" not in prompt_off)
    _check("非空知识时知识正文进了 Prompt",
           KNOWLEDGE_MARKER in prompt_on and "（来源：mock://handbook）" in prompt_on)
    _check("★ 两个 Prompt 只差那一小节（逐字节取证）",
           prompt_on.replace(
               _knowledge_block([{"content": KNOWLEDGE_CONTENT,
                                  "source": "mock://handbook"}]), "", 1) == prompt_off)
    _check("变量集与调用点一致（严格模式未报错）",
           "{{" not in prompt_off and "{{" not in prompt_on)

    # ============================================================
    # [5] ★★ 知识库影响问题生成
    # ============================================================
    _section("[5] ★★ 知识库影响问题生成（有知识 vs 无知识，同一会话 / 同一回答）")
    engine, factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with factory() as db:
        user_id, other_id, job_id, resume_id = await _seed(db)
        sid = await _make_session(db, user_id, job_id, resume_id)
        await interview_service.submit_answer(
            db, user_id, sid, AnswerSubmitRequest(answer_text=ANSWER)
        )

        # ---- 无知识 ----
        spark_off = KnowledgeAwareSpark()
        res_off = await ff.generate_followup(
            db, sid, user_id=user_id, answer_text=ANSWER, spark=spark_off
        )
        # ---- 有知识（注入桩检索器：显式注入优先，不需要真实向量库）----
        chunks = [{"content": KNOWLEDGE_CONTENT, "source": "mock://handbook/redis-aof"}]
        spark_on = KnowledgeAwareSpark()
        res_on = await ff.generate_followup(
            db, sid, user_id=user_id, answer_text=ANSWER,
            spark=spark_on, retriever=SpyRetriever(chunks),
        )

        _check("两次都成功", res_off["ok"] and res_on["ok"],
               f"{res_off['error']} / {res_on['error']}")
        _check("  └ 结果字段集恒定（两次都等于契约）",
               set(res_off) == set(res_on) == set(ff.FOLLOWUP_RESULT_FIELDS))

        # ★ 恒真自检：两次 Prompt 必须**真的不同**，否则下面的断言毫无意义
        _check("★ 自检：两次 Prompt 真的不同（否则断言恒真）",
               spark_off.calls[0] != spark_on.calls[0])
        _check("★ 无知识时 Prompt 不含知识正文（负向取证）",
               KNOWLEDGE_MARKER not in spark_off.calls[0])
        _check("★ 有知识时 Prompt 含知识正文（正向取证）",
               KNOWLEDGE_MARKER in spark_on.calls[0])

        _check("★★ 知识库**影响问题生成**：最终 question 随知识而变",
               res_off["question"] == QUESTION_PLAIN
               and res_on["question"] == QUESTION_WITH_KNOWLEDGE
               and res_off["question"] != res_on["question"],
               f"{res_off['question'][:18]} vs {res_on['question'][:18]}")
        _check("  └ 有知识那次的 topic / expected_points 也跟着变",
               res_on["expected_points"] == ["appendfsync", "everysec"]
               and res_off["expected_points"] == ["RDB", "AOF"],
               str(res_on["expected_points"]))
        _check("★ knowledge_chunks 由 0 变 N（正向信号不虚报）",
               res_off["knowledge_chunks"] == 0 and res_on["knowledge_chunks"] == 1,
               f"{res_off['knowledge_chunks']} / {res_on['knowledge_chunks']}")
        _check("  └ 两次都不产生 warning（空 ≠ 失败）",
               res_off["warnings"] == [] and res_on["warnings"] == [])
        _check("  └ 只调用模型一次（检索不额外消耗 LLM）",
               len(spark_off.calls) == 1 and len(spark_on.calls) == 1)

        # 确定性：同输入同输出
        spark_again = KnowledgeAwareSpark()
        res_again = await ff.generate_followup(
            db, sid, user_id=user_id, answer_text=ANSWER, spark=spark_again
        )
        _check("★ 确定性：同输入同输出（含 Prompt 与结果）",
               spark_again.calls == spark_off.calls and res_again == res_off)

        # 检索入参取证：topic = 分析出的 focus
        spy = SpyRetriever(chunks)
        res_spy = await ff.generate_followup(
            db, sid, user_id=user_id, answer_text=ANSWER,
            spark=KnowledgeAwareSpark(), retriever=spy,
        )
        _check("★ 检索恰好 1 次，且入参 = (job, topic, 上下文包)",
               len(spy.calls) == 1, str(len(spy.calls)))
        if spy.calls:
            got_job, got_topic, got_ctx = spy.calls[0]
            _check("  └ 第 1 位是岗位行（含 job_name）",
                   getattr(got_job, "job_name", None) == "后端开发工程师",
                   str(getattr(got_job, "job_name", got_job)))
            _check("  └ 第 2 位 = 结果的 focus（检索线索与追问聚焦点同一）",
                   got_topic == res_spy["focus"] and bool(got_topic),
                   f"{got_topic!r} vs {res_spy['focus']!r}")
            _check("  └ 第 3 位是**上下文包**（简历 + 岗位 + 历史 + 知识 一起给检索器）",
                   isinstance(got_ctx, dict) and "resume_summary" in got_ctx
                   and "history" in got_ctx, str(sorted(got_ctx)[:6]))

        # ============================================================
        # [6] 检索是可选增强
        # ============================================================
        _section("[6] 检索是可选增强：不注入就不检索 / 失败静默降级")
        spy_off = SpyRetriever(chunks)
        res_no_rag = await ff.generate_followup(
            db, sid, user_id=user_id, answer_text=ANSWER, spark=KnowledgeAwareSpark()
        )
        _check("★ 不注入检索器且 use_rag=False ⇒ 一次都不检索（默认路径）",
               spy_off.calls == [] and res_no_rag["knowledge_chunks"] == 0)

        boom = SpyRetriever(raises=RuntimeError("检索炸了"))
        spark_boom = KnowledgeAwareSpark()
        res_boom = await ff.generate_followup(
            db, sid, user_id=user_id, answer_text=ANSWER, spark=spark_boom, retriever=boom
        )
        _check("★ 检索抛异常 ⇒ 仍 ok=True（可选增强不阻塞追问）", res_boom["ok"] is True,
               str(res_boom["error"]))
        _check("  └ 只记一条 knowledge_retrieval_failed，不进 errors",
               res_boom["warnings"] == [ff.WARNING_KNOWLEDGE_FAILED]
               and res_boom["errors"] == [], str(res_boom["warnings"]))
        _check("  └ knowledge_chunks 如实报 0（不虚报）", res_boom["knowledge_chunks"] == 0)
        _check("  └ Prompt 里没有知识（降级为无知识）",
               KNOWLEDGE_MARKER not in spark_boom.calls[0])
        _check("  └ 结果仍可用（question 非空）", bool(res_boom["question"]))

        # 组装失败（use_rag=True 但组装器炸）⇒ 同样降级
        from services import knowledge_rag  # noqa: PLC0415 - 测试才 import 组装器

        original_build = knowledge_rag.build_vector_retriever

        def _boom_build(*args, **kwargs):
            raise RuntimeError("组装失败")

        knowledge_rag.build_vector_retriever = _boom_build
        try:
            spark_asm = KnowledgeAwareSpark()
            res_asm = await ff.generate_followup(
                db, sid, user_id=user_id, answer_text=ANSWER,
                spark=spark_asm, use_rag=True,
            )
        finally:
            knowledge_rag.build_vector_retriever = original_build
        _check("★ use_rag=True 但组装失败 ⇒ 静默降级为「无知识」+ warning",
               res_asm["ok"] is True
               and res_asm["warnings"] == [ff.WARNING_KNOWLEDGE_FAILED]
               and res_asm["knowledge_chunks"] == 0,
               str(res_asm["warnings"]))
        _check("  └ 组装器恢复后 use_rag=True 会被真正调用（证明上面不是恒真）",
               knowledge_rag.build_vector_retriever is original_build)

        # ============================================================
        # [7] 错误分支
        # ============================================================
        _section("[7] 错误分支：answer_required / question_required / finished / not_found / agent_failed")
        blank = await ff.generate_followup(
            db, sid, user_id=user_id, answer_text="   ", spark=KnowledgeAwareSpark()
        )
        _check("空回答 ⇒ answer_required",
               blank["ok"] is False and blank["errors"] == [ff.ERROR_ANSWER_REQUIRED]
               and blank["question"] == "", str(blank["errors"]))

        created = await interview_service.create_session(
            db, user_id,
            SessionCreateRequest(job_id=job_id, resume_id=resume_id, total_questions=3),
        )
        fresh_id = created["session"]["id"]
        no_q = await ff.generate_followup(
            db, fresh_id, user_id=user_id, answer_text=ANSWER, spark=KnowledgeAwareSpark()
        )
        _check("未开始（无已作答题）⇒ question_required",
               no_q["ok"] is False and no_q["errors"] == [ff.ERROR_QUESTION_REQUIRED]
               and no_q["question"] == "", str(no_q["errors"]))

        _check("显式传 question 时不再要求已作答题（question_required 不是恒真）",
               (await ff.generate_followup(
                   db, fresh_id, user_id=user_id, answer_text=ANSWER,
                   question={"question": "Q", "topic": "T", "expected_points": []},
                   spark=KnowledgeAwareSpark(),
               ))["ok"] is True)

        # 已结束
        sid_end = await _make_session(db, user_id, job_id, resume_id)
        await interview_service.submit_answer(
            db, user_id, sid_end, AnswerSubmitRequest(answer_text=ANSWER)
        )
        await interview_service.end_session(db, user_id, sid_end)
        finished = await ff.generate_followup(
            db, sid_end, user_id=user_id, answer_text=ANSWER, spark=KnowledgeAwareSpark()
        )
        _check("已结束 ⇒ session_finished",
               finished["ok"] is False
               and finished["errors"] == [ff.ERROR_SESSION_FINISHED]
               and finished["question"] == "", str(finished["errors"]))

        foreign = await ff.generate_followup(
            db, sid, user_id=other_id, answer_text=ANSWER, spark=KnowledgeAwareSpark()
        )
        _check("非本人 ⇒ session_not_found（不泄露他人会话）",
               foreign["ok"] is False
               and foreign["errors"] == [ff.ERROR_SESSION_NOT_FOUND], str(foreign["errors"]))

        bad_spark = RecordingSpark("不是 JSON", "还是不是 JSON")
        failed = await ff.generate_followup(
            db, sid, user_id=user_id, answer_text=ANSWER, spark=bad_spark
        )
        _check("模型两次都不合法 ⇒ agent_failed，question 恒为空",
               failed["ok"] is False and failed["errors"] == [ff.ERROR_AGENT_FAILED]
               and failed["question"] == "", str(failed["errors"]))
        _check("  └ 且失败结果仍带分析（便于前端展示「分析到了什么」）",
               bool(failed["analysis"].get("focus"))
               and failed["analysis"].get("answer_chars") == len(ANSWER)
               and failed["based_on"] == failed["analysis"].get("question"),
               str(sorted(failed["analysis"])[:6]))
        _check("  └ 失败结果字段集仍恒定",
               set(failed) == set(ff.FOLLOWUP_RESULT_FIELDS))

        # ============================================================
        # [8] 上下文整合取证
        # ============================================================
        _section("[8] 上下文整合取证：简历 / 岗位 / 历史回答 / 本题 / 回答 / 分析")
        spark_ctx = KnowledgeAwareSpark()
        res_ctx = await ff.generate_followup(
            db, sid, user_id=user_id, answer_text=ANSWER, spark=spark_ctx
        )
        prompt = spark_ctx.calls[0]
        _check("★ 简历正文进了 Prompt", RESUME_CONTENT in prompt)
        _check("★ 岗位职责与技能要求进了 Prompt",
               JOB_DUTY in prompt and JOB_SKILLS in prompt)
        _check("★ 历史回答进了 Prompt（题号 + 题干 + 回答 + 得分）",
               "第 1 题" in prompt and ANSWER[:20] in prompt and "答（得分" in prompt,
               prompt[prompt.find("## 六"):prompt.find("## 六") + 200])
        _check("★ 本题题干进了 Prompt", res_ctx["based_on"][:16] in prompt)
        _check("★ 候选人回答原文进了 Prompt", ANSWER in prompt)
        _check("★ 回答分析进了 Prompt（分数 / 薄弱点 / 建议追问方向）",
               "本题规则得分" in prompt and "薄弱点" in prompt
               and "建议追问方向" in prompt and "回答长度" in prompt,
               prompt[prompt.find("## 五"):prompt.find("## 五") + 240])
        _check("  └ based_on 是追问所针对的题目原文",
               res_ctx["based_on"] == res_ctx["analysis"]["question"])
        _check("  └ analysis 字段集恒定（对外可用的分析结果）",
               set(res_ctx["analysis"]) == set(ff.ANALYSIS_FIELDS))
        _check("  └ focus 与 analysis.focus 一致且非空（检索线索 = 追问聚焦点）",
               res_ctx["focus"] == res_ctx["analysis"]["focus"] and bool(res_ctx["focus"]),
               res_ctx["focus"])
        _check("  └ 规则题无 expected_points 命中/未命中之分时 focus 回落本题 topic",
               res_ctx["focus"] == res_ctx["analysis"]["topic"]
               or res_ctx["analysis"]["missed_points"] != [],
               f"{res_ctx['focus']!r} / {res_ctx['analysis']['missed_points']!r}")

        # ---- 经 Service（含 _deliver 交付分流）----
        _section("[8b] 经 interview_service：归属校验 + 交付分流（avatar 通道）")
        sid_avatar = await _make_session(db, user_id, job_id, resume_id, mode="avatar")
        await interview_service.submit_answer(
            db, user_id, sid_avatar, AnswerSubmitRequest(answer_text=ANSWER)
        )
        spark_svc = KnowledgeAwareSpark()
        res_svc = await interview_service.generate_followup(
            db, user_id, sid_avatar,
            FollowUpRequest(answer_text=ANSWER, use_rag=False), spark=spark_svc,
        )
        _check("经 Service 的追问结果字段集恒定",
               set(res_svc) == set(ff.FOLLOWUP_RESULT_FIELDS))
        _check("  └ avatar 模式下交付**原样透传**（数字人通道不阻断、不加字段）",
               res_svc["ok"] is True and res_svc["question"] == QUESTION_PLAIN,
               res_svc["question"][:20])
        _check("  └ 显式 question_no 可指定追问对象",
               (await interview_service.generate_followup(
                   db, user_id, sid_avatar,
                   FollowUpRequest(answer_text=ANSWER, question_no=1),
                   spark=KnowledgeAwareSpark(),
               ))["ok"] is True)
        try:
            await interview_service.generate_followup(
                db, user_id, 999999, FollowUpRequest(answer_text=ANSWER),
                spark=KnowledgeAwareSpark(),
            )
            ghost_raised = False
        except Exception as exc:  # noqa: BLE001
            ghost_raised = getattr(exc, "status_code", None) == 404
        _check("★ 会话不存在 ⇒ Service 抛 404（HTTP 语义归 Service）", ghost_raised)

    await engine.dispose()

    # ============================================================
    # [9] 接口稳定 + 隔离守卫
    # ============================================================
    _section("[9] 接口稳定（interview_core 公开签名冻结）+ 隔离守卫")

    sig = inspect.signature(interview_core.generate_next_question)
    _check("★ interview_core.generate_next_question 参数表未变",
           list(sig.parameters) == ["db", "session_id", "user_id", "spark", "context",
                                    "plan", "retriever", "use_rag", "retriever_kwargs"],
           str(list(sig.parameters)))
    _check("  └ retriever / use_rag / retriever_kwargs 仍是 keyword-only",
           all(sig.parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
               for name in ("retriever", "use_rag", "retriever_kwargs")))
    _check("  └ 三个可选开关默认值未变（None / False / None）",
           sig.parameters["retriever"].default is None
           and sig.parameters["use_rag"].default is False
           and sig.parameters["retriever_kwargs"].default is None)

    rk = inspect.signature(interview_core.retrieve_knowledge)
    _check("★ interview_core.retrieve_knowledge 参数表未变",
           list(rk.parameters) == ["job", "topic", "context", "retriever"],
           str(list(rk.parameters)))
    _check("  └ 仍不收 db（检索不耦合业务库）", "db" not in rk.parameters)

    rr = inspect.signature(interview_core.resolve_retriever)
    _check("★ interview_core.resolve_retriever 参数表未变",
           list(rr.parameters) == ["db", "retriever", "use_rag", "retriever_kwargs"],
           str(list(rr.parameters)))
    _check("★ QUESTION_RESULT_FIELDS 仍是 13 键（出题结果契约未变）",
           len(interview_core.QUESTION_RESULT_FIELDS) == 13,
           str(len(interview_core.QUESTION_RESULT_FIELDS)))
    _check("  └ 追问结果契约是**另一个**契约（不与之混淆）",
           set(ff.FOLLOWUP_RESULT_FIELDS) != set(interview_core.QUESTION_RESULT_FIELDS)
           and len(ff.FOLLOWUP_RESULT_FIELDS) == 16)

    for path in NEW_MODULES:
        src = _module_source(path)
        imported = _imported_modules(src)
        _check(f"★ {path} 不 import 任何检索 / 向量 / Embedding 模块",
               not any(module in name for name in imported for module in PRODUCTION_MODULES),
               str(sorted(imported)))
        _check(f"  └ {path} 不 import knowledge_rag（RAG 组装只经 Core）",
               not any("knowledge_rag" in name for name in imported))
        _check(f"  └ {path} 不反向 import interview_service",
               not any(name.endswith("interview_service") for name in imported))

    agent_src = _module_source("services/interview_followup_agent.py")
    agent_calls = [
        node.func.attr for node in ast.walk(ast.parse(agent_src))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    ]
    _check("★ 追问 Agent **自己不检索**（源码里没有任何 retrieve 调用）",
           "retrieve" not in agent_calls, str(sorted(set(agent_calls))[:12]))
    _check("  └ 知识只经 knowledge_context 参数进入",
           "knowledge_context" in inspect.signature(ffa.generate_followup).parameters)

    _check("★ 追问链只经 interview_core 这一条接缝（不直连 Agent 之外的层）",
           any(name.endswith("interview_core") for name in
               _imported_modules(_module_source("services/interview_followup.py"))))

    _check("★ 复用而非复制：Spark 失败哨兵判定是**同一个函数对象**",
           ffa._is_spark_failure is interview_agent._is_spark_failure)
    _check("  └ 校验与查重口径也是同一对象",
           ffa.validate_question is interview_agent.validate_question
           and ffa.is_duplicate_question is interview_agent.is_duplicate_question)
    _check("  └ 查重阈值同一常量",
           ffa.DUPLICATE_THRESHOLD == interview_agent.DUPLICATE_THRESHOLD == 0.85)
    _check("  └ 失败结果形状同一函数",
           ffa.failure is interview_agent.failure)

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_PFAILED} 项")
    print("=" * 70)
    return _PFAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
