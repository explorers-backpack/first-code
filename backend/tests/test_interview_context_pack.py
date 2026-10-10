# -*- coding: utf-8 -*-
"""AI 模拟面试 · **面试上下文整合**（简历 + 岗位 + 知识库 + 历史回答）自检

运行：``python backend/tests/test_interview_context_pack.py``

不依赖本机 MySQL：``DATABASE_URL`` 指向 SQLite 内存库 + ``StaticPool``。
**不调模型、不检索**：本套件只验证「取什么、怎么归一」。

被测对象
--------
``services.interview_context_pack``

覆盖范围
--------
[1] 契约：字段集恒定 / ``__all__`` 里每个名字真实存在 / 占位串与 ``interview_agent``
    **逐字相同**（两处实现必须一致，漂移即失败）
[2] ``build_context_pack``：简历 / 岗位归一、空值给占位串、不改入参、同输入同输出、
    兼容 ORM 风格对象
[3] ``merge_history``：**只保留已作答的题**、按题号升序、缺题号排最后、空作答剔除
[4] ``history_lines``：格式、单条截断、总量截断 + 显式标记、空历史 → ``[]``
[5] ``derive_focus_topics``：优先级链与保序去重
[6] ``attach_knowledge``：归一（str / dict / KnowledgeChunk）、``knowledge_count``
    口径 = **原始片段条数**、不改入参、``None`` ⇒ 空
[7] ``load_context_pack``：只读加载（简历正文 / 岗位职责 / 历史回答真的进来了）
[8] 隔离守卫（AST）：不 import ``knowledge_rag`` / ``vector_*`` / ``embedding_*`` /
    ``knowledge_retriever``；源码里没有任何 ``retrieve`` / ``chat_async`` 调用
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import os
import pathlib
import sys

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import Job, Resume, User  # noqa: E402
from schemas.interview import AnswerSubmitRequest, SessionCreateRequest  # noqa: E402
from services import interview_agent, interview_context_pack as cp  # noqa: E402
from services import interview_service  # noqa: E402
from services.knowledge_retriever import KnowledgeChunk  # noqa: E402

_PASSED = 0
_PFAILED = 0

RESUME_CONTENT = "张三，5 年后端开发经验，主导订单中台重构，负责 Redis 缓存与持久化调优。"
JOB_DUTY = "负责后端服务的设计与开发，保障高并发场景下的稳定性。"
JOB_SKILLS = "Python,MySQL,Redis,Kafka"

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


def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed(db: AsyncSession):
    user = User(username="cp_t", email="cp_t@example.com", password_hash="x", role="user")
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
    db.add_all([user, job, resume])
    await db.commit()
    await db.refresh(user)
    await db.refresh(job)
    await db.refresh(resume)
    return user.id, job.id, resume.id


async def run() -> bool:
    print("=" * 70)
    print("AI 模拟面试 · 面试上下文整合（简历 + 岗位 + 知识库 + 历史回答）自检")
    print("=" * 70)

    # ============================================================
    # [1] 契约
    # ============================================================
    _section("[1] 契约：字段集恒定 / __all__ / 占位串与 interview_agent 一致")
    _check("CONTEXT_PACK_FIELDS 是 17 键的元组",
           isinstance(cp.CONTEXT_PACK_FIELDS, tuple) and len(cp.CONTEXT_PACK_FIELDS) == 17,
           str(len(cp.CONTEXT_PACK_FIELDS)))
    _check("  └ 无重复键",
           len(set(cp.CONTEXT_PACK_FIELDS)) == len(cp.CONTEXT_PACK_FIELDS))
    _check("  └ 覆盖四类来源（简历 / 岗位 / 历史回答 / 知识库）",
           {"resume_summary", "job_title", "job_description"} <= set(cp.CONTEXT_PACK_FIELDS)
           and {"history", "history_count", "asked_questions"} <= set(cp.CONTEXT_PACK_FIELDS)
           and {"knowledge", "knowledge_count"} <= set(cp.CONTEXT_PACK_FIELDS))
    _check("HISTORY_ENTRY_FIELDS 是 6 键",
           isinstance(cp.HISTORY_ENTRY_FIELDS, tuple) and len(cp.HISTORY_ENTRY_FIELDS) == 6,
           str(cp.HISTORY_ENTRY_FIELDS))

    missing = [name for name in cp.__all__ if not hasattr(cp, name)]
    _check("__all__ 里每个名字都真实存在", missing == [], str(missing))
    _check("  └ 入口都在 __all__ 里",
           {"build_context_pack", "attach_knowledge", "history_lines",
            "load_context_pack", "merge_history", "derive_focus_topics"} <= set(cp.__all__),
           str(sorted(cp.__all__)))

    # ★ 两处实现的占位串必须逐字相同：``interview_agent`` 被指纹冻结，
    #   无法把它的私有常量公开，因此这里用**漂移守卫**代替跨模块 import。
    _check("★ 占位串与 interview_agent 逐字相同（漂移守卫）",
           (cp.EMPTY_RESUME, cp.EMPTY_JOB_TITLE, cp.EMPTY_JOB_DESCRIPTION)
           == (interview_agent._EMPTY_RESUME, interview_agent._EMPTY_JOB_TITLE,
               interview_agent._EMPTY_JOB_DESCRIPTION),
           str((cp.EMPTY_RESUME, cp.EMPTY_JOB_TITLE, cp.EMPTY_JOB_DESCRIPTION)))
    _check("  └ 简历摘要上限**不复制**默认值（复用 interview_agent 的常量）",
           "RESUME_SUMMARY_MAX" in inspect.getsource(cp)
           and getattr(interview_agent, "RESUME_SUMMARY_MAX", None) == 1200)

    # ============================================================
    # [2] build_context_pack（纯函数）
    # ============================================================
    _section("[2] build_context_pack：归一 / 空值占位 / 不改入参 / 确定性")

    session = {
        "id": 7,
        "interview_type": "technical",
        "difficulty": "mid",
        "current_question_no": 3,
        "total_questions": 5,
    }
    context = {
        "current_stage": "technical",
        "asked_questions": [{"question_no": 1, "question": "请做一下自我介绍"}, "RDB 与 AOF 的区别"],
        "covered_topics": ["Redis 持久化"],
        "weak_topics": ["缓存一致性"],
    }
    plan = {"priority_topics": ["Redis 持久化", "MySQL 索引"], "target_topics": ["Java"]}
    questions = [
        {"id": 11, "question_no": 1, "question": "请做一下自我介绍", "topic": "介绍",
         "question_type": "introduction"},
        {"id": 12, "question_no": 2, "question": "RDB 与 AOF 的区别", "topic": "Redis 持久化",
         "question_type": "technical"},
    ]
    answers = [{"question_id": 12, "answer_text": "RDB 是快照。", "score": 72, "feedback": "可展开"}]

    pack = cp.build_context_pack(
        session=session, resume={"content": RESUME_CONTENT},
        job={"job_name": "后端开发工程师", "skills": JOB_SKILLS, "duty": JOB_DUTY},
        context=context, plan=plan, questions=questions, answers=answers,
    )
    _check("★ 键集恒等于 CONTEXT_PACK_FIELDS",
           set(pack) == set(cp.CONTEXT_PACK_FIELDS),
           str(set(pack) ^ set(cp.CONTEXT_PACK_FIELDS)))
    _check("简历正文进了 resume_summary", pack["resume_summary"] == RESUME_CONTENT)
    _check("岗位名 / 描述进了 job_title / job_description",
           pack["job_title"] == "后端开发工程师"
           and JOB_SKILLS in pack["job_description"] and JOB_DUTY in pack["job_description"],
           pack["job_description"])
    _check("asked_questions 归一为问题文本（对象取 question 键）",
           pack["asked_questions"] == ["请做一下自我介绍", "RDB 与 AOF 的区别"],
           str(pack["asked_questions"]))
    _check("current_stage 直接用 interview_context 的键名（检索器读的就是它）",
           pack["current_stage"] == "technical")
    _check("focus_topics = 薄弱点 + 计划缺口 + 计划全量 + 目标（保序去重）",
           pack["focus_topics"] == ["缓存一致性", "MySQL 索引", "Redis 持久化", "Java"],
           str(pack["focus_topics"]))
    _check("history 每条的键集恒为 HISTORY_ENTRY_FIELDS",
           all(set(entry) == set(cp.HISTORY_ENTRY_FIELDS) for entry in pack["history"]),
           str(pack["history"]))
    _check("未给知识时 knowledge=[] / knowledge_count=0（不留占位文本）",
           pack["knowledge"] == [] and pack["knowledge_count"] == 0)

    # 空输入：**不抛异常**，给占位串
    empty = cp.build_context_pack()
    _check("★ 空输入不抛异常且键集仍恒定", set(empty) == set(cp.CONTEXT_PACK_FIELDS))
    _check("  └ 无简历 → 显式占位串（不是空串）", empty["resume_summary"] == cp.EMPTY_RESUME)
    _check("  └ 无岗位 → 显式占位串",
           empty["job_title"] == cp.EMPTY_JOB_TITLE
           and empty["job_description"] == cp.EMPTY_JOB_DESCRIPTION)
    _check("  └ 会话字段缺失时取 None（不猜 0）",
           empty["session_id"] is None and empty["total_questions"] is None)

    # 不改入参 / 确定性
    snapshot = (dict(session), dict(context), dict(plan), [dict(q) for q in questions],
                [dict(a) for a in answers])
    again = cp.build_context_pack(
        session=session, resume={"content": RESUME_CONTENT},
        job={"job_name": "后端开发工程师", "skills": JOB_SKILLS, "duty": JOB_DUTY},
        context=context, plan=plan, questions=questions, answers=answers,
    )
    _check("★ 不改入参（纯函数）",
           (session, context, plan, questions, answers) == snapshot)
    _check("  └ 同输入同输出（确定性）", again == pack)
    _check("  └ 历史条目是新建 dict（改它不污染入参）",
           pack["history"][0] is not answers[0])

    # ORM 风格对象
    class _Obj:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    orm_pack = cp.build_context_pack(
        session=_Obj(id=9, interview_type="technical", difficulty="mid",
                     current_question_no=1, total_questions=3),
        resume=_Obj(content="李四"),
        job=_Obj(job_name="数据工程师", skills="SQL", duty="数据管道"),
    )
    _check("兼容 ORM 风格对象（getattr 读取）",
           orm_pack["session_id"] == 9 and orm_pack["resume_summary"] == "李四"
           and orm_pack["job_title"] == "数据工程师",
           str(orm_pack["session_id"]))

    # ============================================================
    # [3] merge_history
    # ============================================================
    _section("[3] merge_history：只保留已作答 / 按题号升序 / 空作答剔除")
    q_rows = [
        {"id": 1, "question_no": 1, "question": "Q1", "topic": "A"},
        {"id": 2, "question_no": 2, "question": "Q2", "topic": "B"},
        {"id": 3, "question_no": 3, "question": "Q3", "topic": "C"},
    ]
    a_rows = [
        {"question_id": 2, "answer_text": "答2", "score": 60},
        {"question_id": 1, "answer_text": "答1", "score": 80},
        {"question_id": 3, "answer_text": "   ", "score": 50},   # 空白作答 ⇒ 剔除
    ]
    merged = cp.merge_history(q_rows, a_rows)
    _check("★ 未作答的题不进历史（只有 Q1 / Q2）",
           [entry["question"] for entry in merged] == ["Q1", "Q2"], str(merged))
    _check("★ 按题号升序（作答行输入顺序是乱的）",
           [entry["question_no"] for entry in merged] == [1, 2])
    _check("  └ 空白作答被剔除（Q3 不在）", 3 not in [e["question_no"] for e in merged])
    _check("  └ 得分与反馈一并带入",
           merged[0]["score"] == 80 and merged[1]["score"] == 60)
    _check("  └ 不改入参",
           q_rows[0] == {"id": 1, "question_no": 1, "question": "Q1", "topic": "A"})

    no_no = cp.merge_history(
        [{"id": 1, "question": "无题号"}, {"id": 2, "question_no": 5, "question": "有题号"}],
        [{"question_id": 1, "answer_text": "a"}, {"question_id": 2, "answer_text": "b"}],
    )
    _check("缺 question_no 的条目排在最后（不猜题号）",
           [e["question_no"] for e in no_no] == [5, None], str(no_no))
    _check("无作答行 → 空历史", cp.merge_history(q_rows, []) == [])
    _check("非 list 形状安全（不逐字符拆字符串）", cp.merge_history("Q1", None) == [])

    _check("max_history 只保留最近 N 条（截尾）",
           [e["question"] for e in cp.build_context_pack(
               questions=q_rows, answers=a_rows, max_history=1)["history"]] == ["Q2"])
    _check("  └ max_history=0 ⇒ 空", cp.build_context_pack(
        questions=q_rows, answers=a_rows, max_history=0)["history"] == [])
    _check("  └ max_history=None ⇒ 不截断",
           len(cp.build_context_pack(questions=q_rows, answers=a_rows,
                                     max_history=None)["history"]) == 2)

    # ============================================================
    # [4] history_lines
    # ============================================================
    _section("[4] history_lines：格式 / 单条截断 / 总量截断 + 显式标记")
    lines = cp.history_lines(pack)
    _check("有历史时渲染出题号 / 主题 / 得分",
           len(lines) == 1 and "第 2 题" in lines[0] and "Redis 持久化" in lines[0]
           and "得分 72" in lines[0],
           str(lines))
    _check("  └ 含题干与回答正文",
           "RDB 与 AOF 的区别" in lines[0] and "RDB 是快照。" in lines[0])
    _check("空历史 → []（不是 ['（无）']）", cp.history_lines(cp.build_context_pack()) == [])

    long_answer = "啊" * (cp.HISTORY_ANSWER_MAX + 200)
    long_pack = cp.build_context_pack(
        history=[{"question_no": 1, "question": "Q", "answer": long_answer}])
    long_lines = cp.history_lines(long_pack)
    _check("★ 单条回答超限被硬截断并加显式标记",
           "…（已截断）" in long_lines[0]
           and len(long_lines[0]) < len(long_answer),
           str(len(long_lines[0])))

    many = [
        {"question_no": i, "question": f"Q{i}" + "题" * 200, "answer": "答" * 100}
        for i in range(1, 21)
    ]
    many_lines = cp.history_lines(cp.build_context_pack(history=many, max_history=None))
    _check("★ 总量超限时保留**最近**的若干条 + 显式省略标记",
           cp.HISTORY_TRUNCATION_MARK in many_lines
           and sum(len(line) + 1 for line in many_lines) <= cp.HISTORY_BLOCK_MAX_CHARS + 2,
           f"lines={len(many_lines)} first={many_lines[0][:20]!r}")
    _check("  └ 保留的是最新的（最后一条真实历史是 Q20）",
           many_lines[-1].startswith("第 20 题"), many_lines[-1][:20])
    _check("  └ 省略标记在最前（表示「更早的已省略」）且恰好一次",
           many_lines[0] == cp.HISTORY_TRUNCATION_MARK
           and many_lines.count(cp.HISTORY_TRUNCATION_MARK) == 1,
           str(many_lines[:2]))
    single_huge = cp.history_lines(
        cp.build_context_pack(history=[{"question_no": 1, "question": "Q" * 5000}]),
        max_chars=200,
    )
    _check("单条就超限时先硬截该条再附标记",
           len(single_huge) == 1 and single_huge[0].endswith(cp.HISTORY_TRUNCATION_MARK)
           and len(single_huge[0]) <= 200 + 1, str(len(single_huge[0])))

    # ============================================================
    # [5] derive_focus_topics
    # ============================================================
    _section("[5] derive_focus_topics：优先级链与保序去重")
    d = cp.derive_focus_topics
    _check("薄弱点排最前",
           d(weak_topics=["W"], covered_topics=[], priority_topics=["P"],
             target_topics=["T"]) == ["W", "P", "T"])
    _check("计划缺口（priority 中未覆盖的）紧随其后",
           d(weak_topics=[], covered_topics=["P1"], priority_topics=["P1", "P2"],
             target_topics=["T"]) == ["P2", "P1", "T"])
    _check("priority 全量再补齐（含已覆盖的）",
           d(weak_topics=[], covered_topics=["P1", "P2"], priority_topics=["P1", "P2"],
             target_topics=["T"]) == ["P1", "P2", "T"])
    _check("无 plan 时只剩薄弱点", d(weak_topics=["W"]) == ["W"])
    _check("全空 → []（不猜）", d() == [])
    _check("保序去重（重复项只留首次）",
           d(weak_topics=["W", "W"], priority_topics=["W", "P"]) == ["W", "P"])
    _check("非 list 形状安全", d(weak_topics="Redis") == ["Redis"])

    # ============================================================
    # [6] attach_knowledge
    # ============================================================
    _section("[6] attach_knowledge：归一 / count 口径 / 不改入参 / None ⇒ 空")
    chunks = [
        {"content": "Redis 持久化有 RDB 与 AOF。", "source": "mock://a"},
        {"content": "AOF 记录写命令。", "source": "mock://a"},
        KnowledgeChunk(content="哈希槽共 16384 个。", source="mock://b"),
    ]
    with_k = cp.attach_knowledge(pack, chunks)
    _check("★ knowledge_count = **原始片段条数**（与 core 的 knowledge_chunks 同口径）",
           with_k["knowledge_count"] == 3, str(with_k["knowledge_count"]))
    _check("  └ knowledge 是归一后的文本行（同源合并 + 去重）",
           len(with_k["knowledge"]) == 2 and "（来源：mock://a）" in with_k["knowledge"][0],
           str(with_k["knowledge"]))
    _check("  └ 同源两片被合并成一行",
           "Redis 持久化有 RDB 与 AOF。AOF 记录写命令。" in with_k["knowledge"][0],
           with_k["knowledge"][0])
    _check("★ 不改入参（返回新包）",
           pack["knowledge"] == [] and pack["knowledge_count"] == 0
           and with_k is not pack)
    _check("其余字段原样带过去",
           with_k["resume_summary"] == pack["resume_summary"]
           and with_k["history"] == pack["history"])

    _check("None ⇒ 空（不产生占位文本）",
           cp.attach_knowledge(pack, None)["knowledge"] == [])
    _check("空列表 ⇒ 空", cp.attach_knowledge(pack, [])["knowledge_count"] == 0)
    _check("单个 str 也被接受",
           cp.attach_knowledge(pack, "一条知识")["knowledge"] == ["一条知识"])
    _check("非知识标量被丢弃（不塞 repr 进 Prompt）",
           cp.attach_knowledge(pack, [123, True, None])["knowledge"] == [],
           str(cp.attach_knowledge(pack, [123, True, None])["knowledge"]))

    # ============================================================
    # [7] load_context_pack（只读加载）
    # ============================================================
    _section("[7] load_context_pack：简历 / 岗位 / 历史回答真的被读进来")
    engine, factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with factory() as db:
        user_id, job_id, resume_id = await _seed(db)
        created = await interview_service.create_session(
            db, user_id,
            SessionCreateRequest(job_id=job_id, resume_id=resume_id, total_questions=3),
        )
        sid = created["session"]["id"]
        await interview_service.start_session(db, user_id, sid)
        await interview_service.submit_answer(
            db, user_id, sid,
            AnswerSubmitRequest(answer_text="RDB 是全量快照，AOF 记录写命令。"),
        )

        loaded = await cp.load_context_pack(db, sid)
        _check("键集恒定", set(loaded) == set(cp.CONTEXT_PACK_FIELDS))
        _check("★ 简历正文被读进来", loaded["resume_summary"] == RESUME_CONTENT,
               loaded["resume_summary"][:30])
        _check("★ 岗位名与职责被读进来",
               loaded["job_title"] == "后端开发工程师"
               and JOB_DUTY in loaded["job_description"])
        _check("★ 历史回答被读进来（1 条，且是刚作答的那题）",
               loaded["history_count"] == 1
               and loaded["history"][0]["answer"] == "RDB 是全量快照，AOF 记录写命令。",
               str(loaded["history"]))
        _check("  └ 历史条目的题号 = 已作答题号",
               loaded["history"][0]["question_no"] == 1)
        _check("  └ asked_questions 来自 interview_context（由 load 幂等创建）",
               isinstance(loaded["asked_questions"], list))
        _check("  └ current_stage 来自 interview_context",
               loaded["current_stage"] == "introduction", loaded["current_stage"])
        _check("history_lines 可直接渲染（进 Prompt 的形态）",
               len(cp.history_lines(loaded)) == 1
               and "第 1 题" in cp.history_lines(loaded)[0],
               str(cp.history_lines(loaded)))

        ghost = await cp.load_context_pack(db, 999999)
        _check("★ 会话不存在时**不抛异常**，返回空包（键集仍恒定）",
               set(ghost) == set(cp.CONTEXT_PACK_FIELDS)
               and ghost["resume_summary"] == cp.EMPTY_RESUME)

        before = loaded["history_count"]
        await cp.load_context_pack(db, sid)   # 再读一次
        _check("重复加载不改变历史（只读、幂等）",
               (await cp.load_context_pack(db, sid))["history_count"] == before)

    await engine.dispose()

    # ============================================================
    # [8] 隔离守卫（AST）
    # ============================================================
    _section("[8] 隔离守卫：零检索 / 零 LLM / 不越层")
    src = _module_source("services/interview_context_pack.py")
    imported = _imported_modules(src)
    _check("★ 不 import 任何检索 / 向量 / Embedding 模块",
           not any(module in name for name in imported for module in PRODUCTION_MODULES),
           str(sorted(imported)))
    _check("  └ 也不 import knowledge_rag（不自己组装）",
           not any("knowledge_rag" in name for name in imported))
    tree = ast.parse(src)
    calls = [
        node.func.attr for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    ]
    _check("★ 源码里没有任何 retrieve / chat_async 调用（不检索、不调模型）",
           "retrieve" not in calls and "chat_async" not in calls, str(sorted(set(calls))[:12]))
    top_names = set()
    for node in ast.parse(src).body:
        if isinstance(node, ast.Import):
            top_names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            top_names.add(node.module.split(".")[0])
    _check("  └ 顶层 import 的根模块只有 __future__ / typing / services",
           top_names <= {"__future__", "typing", "services"}, str(sorted(top_names)))
    _check("  └ 顶层确实没有 sqlalchemy / models / database（函数内延迟导入）",
           not ({"sqlalchemy", "models", "database"} & top_names), str(sorted(top_names)))
    _check("  └ 确实**有**函数内延迟导入（否则上一条会恒真地通过）",
           "from sqlalchemy import select" in src and "from models import" in src)

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_PFAILED} 项")
    print("=" * 70)
    return _PFAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
