# -*- coding: utf-8 -*-
"""AI 模拟面试 · **面试上下文整合**（简历 + 岗位 + 知识库 + 历史回答）。

职责
----
把「这场面试进行到此刻，模型应当看到的全部背景」整合成一个**恒定字段集**的
上下文包（context pack）::

    简历（resume） + 岗位（job） + 知识库（knowledge） + 历史回答（history）

为什么单独成模块
----------------
``services/interview_agent.build_question_variables`` 只组装 ``question.txt``
需要的 10 个变量——**不含历史回答**（历史只到「问过哪些题」这一层），且
``interview_agent`` 被源码指纹（``FROZEN_SOURCES``）冻结，不得改动。
数字人面试的**追问**需要一份更宽的上下文（尤其是「候选人上一句答了什么」），
因此这里另起一层：**只回答「取什么、怎么归一」，不调模型、不检索、不写库**。

与既有模块的分工（**不要混为一谈**）
------------------------------------
==========================================  ==================================
``interview_context``（既有）                本模块
==========================================  ==================================
会话**状态机**侧上下文                       出题**视野**侧上下文
阶段 / 已问 / 已覆盖 / 薄弱 / 追问次数         简历 / 岗位 / 知识 / 历史问答
**落库**（独立表 ``interview_context``）     **纯派生**，不落库
``update_context`` 等 8 个写入口             无写入口
==========================================  ==================================

本模块**读取** ``interview_context`` 的产出（``current_stage`` / ``asked_questions``
/ ``covered_topics`` / ``weak_topics``），**不修改、不替代**它。

设计约定（与 ``interview_core`` 同级）
--------------------------------------
- **零 LLM、零检索**：不 import ``knowledge_rag`` / ``vector_*`` /
  ``embedding_*`` / ``knowledge_retriever``（知识由调用方取好后经
  :func:`attach_knowledge` 填进来）。
- **本模块不建连接、不建表、不写业务数据**：``sqlalchemy`` 与 ``models``
  只在 :func:`load_context_pack` 内**延迟导入**；唯一的写入是幂等创建上下文
  （``interview_context.create_context``，与 ``interview_core`` 同一口径）。
- 纯函数部分（``build_context_pack`` / ``attach_knowledge`` / ``history_lines``
  / ``derive_focus_topics`` / ``merge_history``）**不改入参、同输入同输出**，
  可脱离 DB 单测。
- 字段集**恒定**（见 :data:`CONTEXT_PACK_FIELDS`）：信息缺失时给**占位串**或空列表，
  不删键——调用方与模板渲染都无需分支。
- 简历摘要上限**不复制**一份默认值：直接复用
  ``interview_agent.RESUME_SUMMARY_MAX``（避免两处默认值日后漂移）。
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from services.interview_agent import (
    RESUME_SUMMARY_MAX,
    format_knowledge_context,
)

# ============================================================
# 一、契约常量
# ============================================================
#: 上下文包的**恒定字段集**（成功与「信息缺失」同形状，调用方无需分支取值）。
CONTEXT_PACK_FIELDS: Tuple[str, ...] = (
    # ---- 会话进度（来自 interview_context / interview_session）----
    "session_id",
    "current_stage",
    "interview_type",
    "difficulty",
    "current_question_no",
    "total_questions",
    # ---- 简历 ----
    "resume_summary",
    # ---- 岗位 ----
    "job_title",
    "job_description",
    # ---- 历史回答（**本模块的核心增量**）----
    "history",
    "history_count",
    "asked_questions",
    "covered_topics",
    "weak_topics",
    "focus_topics",
    # ---- 知识库（由 attach_knowledge 填入；未填时为空）----
    "knowledge",
    "knowledge_count",
)

#: 单条历史问答的恒定字段集。
HISTORY_ENTRY_FIELDS: Tuple[str, ...] = (
    "question_no",
    "question",
    "answer",
    "score",
    "topic",
    "feedback",
)

#: 空值占位串。与 ``interview_agent`` 的私有常量**同字面量**——
#: 由测试的漂移守卫锁死（两处不一致会失败），刻意不跨模块 import 私有名。
EMPTY_RESUME = "（无简历）"
EMPTY_JOB_TITLE = "（未指定岗位）"
EMPTY_JOB_DESCRIPTION = "（未提供岗位描述）"

#: 注入 Prompt 的**单条**历史回答上限（防止超长作答把 Prompt 撑爆）。
HISTORY_ANSWER_MAX = 600

#: 默认保留的**最近**历史问答条数（``None`` = 不截断）。
DEFAULT_MAX_HISTORY = 6

#: ``history_qa`` 文本块的**总字符**上限（按整条粒度截断）。
HISTORY_BLOCK_MAX_CHARS = 3000

#: 历史块被截断时追加的显式标记行。
HISTORY_TRUNCATION_MARK = "……（更早的问答已省略）"


# ============================================================
# 二、内部工具（纯函数）
# ============================================================
def _get(source: Any, key: str, default: Any = None) -> Any:
    """从 dict 或 ORM 对象取值（兼容两种输入，便于脱离数据库测试）。"""
    if source is None:
        return default
    if isinstance(source, Mapping):
        value = source.get(key, default)
    else:
        value = getattr(source, key, default)
    return default if value is None else value


def _clean(value: Any) -> str:
    """归一为字符串：``None`` → ``""``；bool / 数字 → 文本；其余 → ``""``。"""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return ""


def _text_list(value: Any) -> List[str]:
    """归一为非空字符串列表（保序去重）；非 list/tuple/set 形状 → ``[]``。

    刻意**不**对字符串做逐字符拆分：``"Redis"`` 不是 ``["R","e",...]``。
    """
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, (list, tuple, set, frozenset)):
        return []
    out: List[str] = []
    seen: set[str] = set()
    for item in value:
        name = _clean(item)
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def _resume_text(resume: Any) -> str:
    """取简历正文。支持 ORM ``Resume``、``{"content": ...}`` 或裸字符串。"""
    if resume is None:
        return ""
    if isinstance(resume, str):
        return resume.strip()
    return _clean(_get(resume, "content"))


def _job_description(job: Any) -> str:
    """把岗位的技能要求与职责拼成一段描述（空时给显式占位串）。"""
    parts: List[str] = []
    skills = _clean(_get(job, "skills"))
    duty = _clean(_get(job, "duty"))
    if skills:
        parts.append(f"技能要求：{skills}")
    if duty:
        parts.append(f"岗位职责：{duty}")
    return "；".join(parts) if parts else EMPTY_JOB_DESCRIPTION


def _entry_score(value: Any) -> Optional[int]:
    """把得分归一为 ``int``；非法 / 缺失 → ``None``（**不猜 0**）。"""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_history_entry(entry: Any) -> Optional[Dict[str, Any]]:
    """把**一条**历史问答归一成 :data:`HISTORY_ENTRY_FIELDS` 形状。

    - 接受的形状：``Mapping``、带属性的对象（ORM 行 / ``namedtuple``）、裸字符串
      （视为「只有问题、没有回答」）
    - ``question`` 为空 → 返回 ``None``（该条**丢弃**：没有问题文本的问答没有意义）
    - 未知键**忽略**；缺失键取默认值
    """
    if entry is None:
        return None

    if isinstance(entry, str):
        question = entry.strip()
        if not question:
            return None
        return {
            "question_no": None,
            "question": question,
            "answer": "",
            "score": None,
            "topic": "",
            "feedback": "",
        }

    question = _clean(_get(entry, "question"))
    if not question:
        return None

    return {
        "question_no": _entry_score(_get(entry, "question_no")),
        "question": question,
        "answer": _clean(_get(entry, "answer")),
        "score": _entry_score(_get(entry, "score")),
        "topic": _clean(_get(entry, "topic")),
        "feedback": _clean(_get(entry, "feedback")),
    }


def merge_history(questions: Any, answers: Any) -> List[Dict[str, Any]]:
    """把**题目行**与**作答行**合并成有序历史问答（**只保留已作答的题**）。

    - ``questions`` / ``answers`` 可以是 ORM 行列表、dict 列表或任何带属性的对象
    - 关联键：``answer.question_id == question.id``（作答表对 ``question_id``
      有 UNIQUE 约束，因此一题至多一条作答）
    - 排序：``question_no`` 升序；缺 ``question_no`` 的条目排在最后
      （保持输入顺序，**不猜**题号）
    - 未作答的题**不进历史**——「问过什么」由 ``asked_questions`` 单独承载

    纯函数：不改入参、同输入同输出。
    """
    q_rows = list(questions) if isinstance(questions, (list, tuple)) else []
    a_rows = list(answers) if isinstance(answers, (list, tuple)) else []

    by_question_id: Dict[Any, Any] = {}
    for answer in a_rows:
        qid = _get(answer, "question_id")
        if qid is not None and qid not in by_question_id:
            by_question_id[qid] = answer

    merged: List[Tuple[int, int, Dict[str, Any]]] = []
    for index, question in enumerate(q_rows):
        qid = _get(question, "id")
        answer = by_question_id.get(qid)
        if answer is None:
            continue
        text = _clean(_get(answer, "answer_text"))
        if not text:
            continue
        raw_no = _entry_score(_get(question, "question_no"))
        merged.append(
            (
                0 if raw_no is not None else 1,
                raw_no if raw_no is not None else index,
                {
                    "question_no": raw_no,
                    "question": _clean(_get(question, "question")),
                    "answer": text,
                    "score": _entry_score(_get(answer, "score")),
                    "topic": _clean(_get(question, "topic")),
                    "feedback": _clean(_get(answer, "feedback")),
                },
            )
        )

    merged.sort(key=lambda item: (item[0], item[1]))
    return [entry for _, _, entry in merged]


def derive_focus_topics(
    *,
    weak_topics: Any = None,
    covered_topics: Any = None,
    priority_topics: Any = None,
    target_topics: Any = None,
) -> List[str]:
    """推导「本次最该补强的知识点」——**纯函数、确定性、保序去重**。

    优先级（前者全部在前，后者补齐）：

    1. ``weak_topics``（明确的薄弱点，最该补强）
    2. ``priority_topics`` 中**尚未出现在** ``covered_topics`` 的项（计划缺口）
    3. ``priority_topics`` 全量
    4. ``target_topics`` 全量

    与 ``interview_core.current_topic`` 的区别：那个返回**一个** topic（检索查询
    只需一个精确线索），本函数返回**有序列表**（供 Prompt 展示与多轮追问选择）。
    """
    covered = set(_text_list(covered_topics))
    out: List[str] = []
    seen: set[str] = set()

    def _push(names: Sequence[str]) -> None:
        for name in names:
            if name and name not in seen:
                seen.add(name)
                out.append(name)

    _push(_text_list(weak_topics))
    priority = _text_list(priority_topics)
    _push([name for name in priority if name not in covered])
    _push(priority)
    _push(_text_list(target_topics))
    return out


# ============================================================
# 三、上下文包（纯函数）
# ============================================================
def build_context_pack(
    *,
    session: Any = None,
    resume: Any = None,
    job: Any = None,
    context: Any = None,
    plan: Any = None,
    questions: Any = None,
    answers: Any = None,
    history: Any = None,
    knowledge: Any = None,
    max_history: Optional[int] = DEFAULT_MAX_HISTORY,
) -> Dict[str, Any]:
    """整合出**恒定字段集**的上下文包（见 :data:`CONTEXT_PACK_FIELDS`）。

    参数（全部 keyword-only，全部可选——缺什么就少什么，**不猜**）
    ---------------------------------------------------------------
    - ``session``：``InterviewSession`` 行 / dict（取 ``id`` / ``interview_type`` /
      ``difficulty`` / ``current_question_no`` / ``total_questions``）
    - ``resume``：``Resume`` 行 / ``{"content": ...}`` / 裸字符串
    - ``job``：``Job`` 行 / dict（取 ``job_name`` / ``skills`` / ``duty``）
    - ``context``：``interview_context.get_context`` 的产出（取 ``current_stage`` /
      ``asked_questions`` / ``covered_topics`` / ``weak_topics``）
    - ``plan``：``interview_planner.build_plan_for`` 的产出（取 ``priority_topics`` /
      ``target_topics``，仅用于推导 ``focus_topics``）
    - ``questions`` / ``answers``：题目行与作答行 → 合并成 ``history``
    - ``history``：**已合并好**的历史问答（给出时忽略 ``questions`` / ``answers``）
    - ``knowledge``：知识片段（``KnowledgeChunk`` / dict / str 均可），
      经 :func:`attach_knowledge` 归一；缺省 ``None`` ⇒ 无知识
    - ``max_history``：只保留**最近** N 条已作答问答（``None`` = 不截断）

    返回
    ----
    普通 dict，键集恒为 :data:`CONTEXT_PACK_FIELDS`。
    ``history`` 的每一条都是**新建** dict（键集恒为 :data:`HISTORY_ENTRY_FIELDS`），
    调用方改它不会污染入参。
    """
    session_id = _entry_score(_get(session, "id"))
    stage = _clean(_get(context, "current_stage"))
    asked = _text_list(
        [
            _clean(item.get("question")) if isinstance(item, Mapping) else _clean(item)
            for item in (
                _get(context, "asked_questions")
                if isinstance(_get(context, "asked_questions"), (list, tuple))
                else []
            )
        ]
    )

    if history is None:
        merged = merge_history(questions, answers)
    else:
        raw_entries = (
            list(history) if isinstance(history, (list, tuple)) else [history]
        )
        merged = [
            entry
            for entry in (normalize_history_entry(item) for item in raw_entries)
            if entry is not None
        ]

    if max_history is not None and int(max_history) >= 0:
        merged = merged[-int(max_history):] if int(max_history) else []

    resume_text = _resume_text(resume)
    pack: Dict[str, Any] = {
        "session_id": session_id,
        "current_stage": stage,
        "interview_type": _clean(_get(session, "interview_type")),
        "difficulty": _clean(_get(session, "difficulty")),
        "current_question_no": _entry_score(_get(session, "current_question_no")),
        "total_questions": _entry_score(_get(session, "total_questions")),
        "resume_summary": (
            resume_text[:RESUME_SUMMARY_MAX] if resume_text else EMPTY_RESUME
        ),
        "job_title": _clean(_get(job, "job_name")) or EMPTY_JOB_TITLE,
        "job_description": _job_description(job),
        "history": [dict(entry) for entry in merged],
        "history_count": len(merged),
        "asked_questions": asked,
        "covered_topics": _text_list(_get(context, "covered_topics")),
        "weak_topics": _text_list(_get(context, "weak_topics")),
        "focus_topics": derive_focus_topics(
            weak_topics=_get(context, "weak_topics"),
            covered_topics=_get(context, "covered_topics"),
            priority_topics=_get(plan, "priority_topics"),
            target_topics=_get(plan, "target_topics"),
        ),
        "knowledge": [],
        "knowledge_count": 0,
    }
    return attach_knowledge(pack, knowledge)


def attach_knowledge(pack: Any, knowledge: Any) -> Dict[str, Any]:
    """把知识片段挂进上下文包 → **新的**包（不改入参）。

    - ``knowledge`` 经 ``interview_agent.format_knowledge_context`` 归一
      （同源分组 + 去切片重叠 + 总量上限），**只影响 ``knowledge`` 文本行**
    - ``knowledge_count`` 是**原始片段条数**（与 ``interview_core`` 的
      ``knowledge_chunks`` 同一口径：**真正注入 Prompt 的切片条数**）
    - ``knowledge`` 为空 ⇒ 两键分别为 ``[]`` / ``0``，**不产生任何占位文本**
      （「无知识」必须与「有知识」在渲染层可区分，见追问模板的分流规则）
    """
    chunks = (
        list(knowledge)
        if isinstance(knowledge, (list, tuple, set, frozenset))
        else ([knowledge] if knowledge is not None else [])
    )
    lines = format_knowledge_context(knowledge)
    updated = dict(pack or {})
    updated["knowledge"] = lines
    updated["knowledge_count"] = len(chunks) if lines else 0
    return updated


def history_lines(
    pack: Any, *, max_chars: int = HISTORY_BLOCK_MAX_CHARS
) -> List[str]:
    """把 ``history`` 渲染成**进 Prompt 的文本行**（按整条粒度截断）。

    每条形如::

        第 3 题（technical / Redis 持久化）问：请介绍一下 RDB 与 AOF 的区别？
        └ 答（得分 72）：RDB 是某一时刻的全量快照……

    截断：从**最新**的问答往回取，直到超过 :data:`HISTORY_BLOCK_MAX_CHARS`，
    并在**最前面**插入一行 :data:`HISTORY_TRUNCATION_MARK`，让「更早的被省略」对
    模型与排查者都是**显式**的（而不是悄悄丢内容）。单条就超限时先硬截该条再附标记。

    返回顺序恒为「最早 → 最新」（与 ``pack["history"]`` 一致），标记在最前。
    """
    entries = _get(pack, "history")
    if not isinstance(entries, (list, tuple)) or not entries:
        return []

    rendered: List[str] = []
    for entry in entries:
        question = _clean(_get(entry, "question"))
        if not question:
            continue
        topic = _clean(_get(entry, "topic"))
        qtype = _clean(_get(entry, "question_type")) or _clean(_get(entry, "stage"))
        no = _entry_score(_get(entry, "question_no"))
        head = f"第 {no} 题" if no is not None else "上一题"
        meta = " / ".join(part for part in (qtype, topic) if part)
        if meta:
            head += f"（{meta}）"
        lines = [f"{head}问：{question}"]
        answer = _clean(_get(entry, "answer"))
        if answer:
            clipped = (
                answer if len(answer) <= HISTORY_ANSWER_MAX
                else answer[:HISTORY_ANSWER_MAX] + "…（已截断）"
            )
            score = _entry_score(_get(entry, "score"))
            lines.append(f"└ 答（得分 {score}）：{clipped}" if score is not None else f"└ 答：{clipped}")
        rendered.append("\n".join(lines))

    if not rendered:
        return []

    kept: List[str] = []
    total = 0
    for line in reversed(rendered):          # 从最新往回取
        extra = len(line) + (1 if kept else 0)
        if total + extra <= max_chars:
            kept.append(line)
            total += extra
            continue
        if kept:
            kept.append(HISTORY_TRUNCATION_MARK)
        else:
            head = max(max_chars - len(HISTORY_TRUNCATION_MARK), 0)
            kept.append(line[:head] + HISTORY_TRUNCATION_MARK)
        break
    kept.reverse()
    return kept


# ============================================================
# 四、加载（唯一接触 DB 的函数，延迟导入）
# ============================================================
async def load_context_pack(
    db: Any,
    session_id: int,
    *,
    max_history: Optional[int] = DEFAULT_MAX_HISTORY,
    knowledge: Any = None,
) -> Dict[str, Any]:
    """按会话 id 加载上下文包（``session`` / ``job`` / ``resume`` /
    ``context`` / ``questions`` / ``answers`` → :func:`build_context_pack`）。

    **只读业务数据**：不推进状态、不写题目 / 作答 / 报告。
    唯一的写入是**幂等**创建上下文（``interview_context.create_context``，
    已存在则不覆盖）——与 ``interview_core._ensure_context`` 同一口径，
    这样调用方不必先手工建上下文。
    ``sqlalchemy`` 与 ``models`` 在**函数内**延迟导入。

    会话不存在时**不抛异常**，返回一个「什么都不知道」的包
    （键集仍恒定）——「会话不存在」的报错语义属于 Service 层。
    """
    from sqlalchemy import select

    from models import (
        InterviewAnswer,
        InterviewQuestion,
        InterviewSession,
        Job,
        Resume,
    )
    from services.interview_context import create_context, get_context

    session = (
        await db.execute(
            select(InterviewSession).where(InterviewSession.id == session_id)
        )
    ).scalar_one_or_none()
    if session is None:
        # 会话不存在 ⇒ 返回「什么都不知道」的包（**不抛异常**：调用方决定怎么报）
        return build_context_pack(knowledge=knowledge, max_history=max_history)

    job = None
    if _get(session, "job_id"):
        job = (
            await db.execute(select(Job).where(Job.id == _get(session, "job_id")))
        ).scalar_one_or_none()

    resume = None
    if _get(session, "resume_id"):
        # ★ 归属过滤（纵深防御）：只取**会话属主自己**的简历。
        # 新建会话已由 ``interview_service.create_session`` 拦截「挂他人简历」，
        # 这里再加一道是为了兜住**历史脏行**——否则旧会话会把他人简历正文
        # 注入上下文包，进而进入 Prompt。
        resume = (
            await db.execute(
                select(Resume).where(
                    Resume.id == _get(session, "resume_id"),
                    Resume.user_id == _get(session, "user_id"),
                )
            )
        ).scalar_one_or_none()

    questions = list(
        (
            await db.execute(
                select(InterviewQuestion)
                .where(InterviewQuestion.session_id == session_id)
                .order_by(InterviewQuestion.question_no)
            )
        ).scalars()
    )
    answers: List[Any] = []
    if questions:
        answers = list(
            (
                await db.execute(
                    select(InterviewAnswer).where(
                        InterviewAnswer.question_id.in_([q.id for q in questions])
                    )
                )
            ).scalars()
        )

    await create_context(db, session_id)
    context = await get_context(db, session_id)

    return build_context_pack(
        session=session,
        resume=resume,
        job=job,
        context=context,
        questions=questions,
        answers=answers,
        knowledge=knowledge,
        max_history=max_history,
    )


__all__ = [
    "CONTEXT_PACK_FIELDS",
    "HISTORY_ENTRY_FIELDS",
    "EMPTY_RESUME",
    "EMPTY_JOB_TITLE",
    "EMPTY_JOB_DESCRIPTION",
    "HISTORY_ANSWER_MAX",
    "DEFAULT_MAX_HISTORY",
    "HISTORY_BLOCK_MAX_CHARS",
    "HISTORY_TRUNCATION_MARK",
    "normalize_history_entry",
    "merge_history",
    "derive_focus_topics",
    "build_context_pack",
    "attach_knowledge",
    "history_lines",
    "load_context_pack",
]
