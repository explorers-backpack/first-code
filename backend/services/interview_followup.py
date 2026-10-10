# -*- coding: utf-8 -*-
"""AI 模拟面试 · **追问链编排**（用户回答 → 分析回答 → Retriever 检索 → Agent 生成追问）。

为什么单独成模块
----------------
``interview_core.generate_next_question`` 是**出题**链（session → context → plan →
检索 → Agent → Validator），它被源码指纹冻结，且其语义是「出下一道题」。
**追问**是另一条链——输入多了一样东西（候选人刚才那句回答），流程也多了「分析回答」
这一步，因此按同一分层约定另起一个编排模块：

    interview_service
        └── interview_followup          ← 本模块（编排：分析 → 检索 → Agent）
                ├── interview_core              （纯规则评分 / 检索接缝 / 只读加载）
                ├── interview_context_pack      （上下文整合：简历 + 岗位 + 知识 + 历史回答）
                └── interview_followup_agent    （只生成：Prompt + Spark + 解析 + 校验）

链路（与用户要求的五步一一对应）::

    ① 用户回答          入参 ``answer_text``（ASR 文本或打字文本）
    ② 分析回答          :func:`analyze_answer`（复用 ``interview_core.score_answer``）
    ③ Retriever 检索    ``interview_core.resolve_retriever`` + ``retrieve_knowledge``
    ④ Agent 生成追问    ``interview_followup_agent.generate_followup``
    ⑤ Avatar 继续提问   结果交给 ``interview_service._deliver`` 按通道分流
                        （``avatar`` → ``avatar_interview.enter``）

硬约束（与全项目一致）
----------------------
- **RAG 默认开启（HTTP 契约层）**：``schemas.interview.FollowUpRequest.use_rag``
  默认 ``True`` ⇒ 生产入口默认检索；本模块**函数级默认仍保守 ``False``**
  （``use_rag=False`` 且不注入 ``retriever`` ⇒ 不检索、Prompt 与
  「引入知识能力之前」逐字节相同），避免内部直接调用时隐式检索。真实检索链路只经
  ``interview_core.resolve_retriever`` 组装（本模块**不 import** ``knowledge_rag``）。
- **检索是可选增强**：组装失败 / 检索抛异常 ⇒ 静默降级为「无知识」+ 记一条
  :data:`WARNING_KNOWLEDGE_FAILED`，**绝不中断追问**。
- **不写库**：本模块不落任何表（除上下文包的幂等创建，见 ``interview_context_pack``）。
  追问的持久化 / 题号推进由调用方决定——与 ``generate_next_question`` 同一约定。
- **失败不抛异常**：状态类问题以 ``ok=False`` + 稳定错误码返回，
  便于数字人前端按字段分支处理（``api`` 层再决定 HTTP 语义）。
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from services import interview_core, interview_followup_agent
from services.interview_context_pack import (
    DEFAULT_MAX_HISTORY,
    attach_knowledge,
    load_context_pack,
)

# ============================================================
# 一、契约常量
# ============================================================
#: 结果字段集（**恒定**：成功与失败同形状，调用方无需分支取值）。
FOLLOWUP_RESULT_FIELDS: Tuple[str, ...] = (
    "ok",
    "question_no",
    "question",
    "question_type",
    "topic",
    "difficulty",
    "expected_points",
    "reason",
    "based_on",
    "follow_up_no",
    "focus",
    "analysis",
    "knowledge_chunks",
    "warnings",
    "errors",
    "error",
)

#: 回答分析的字段集（恒定）。
ANALYSIS_FIELDS: Tuple[str, ...] = (
    "question",
    "question_no",
    "question_type",
    "topic",
    "answer",
    "answer_chars",
    "score",
    "technical_score",
    "logic_score",
    "expression_score",
    "adaptability_score",
    "feedback",
    "hit_points",
    "missed_points",
    "weak_points",
    "focus",
)

ERROR_SESSION_NOT_FOUND = "session_not_found"
ERROR_SESSION_FINISHED = "session_finished"
ERROR_ANSWER_REQUIRED = "answer_required"
ERROR_QUESTION_REQUIRED = "question_required"
ERROR_AGENT_FAILED = "agent_failed"

#: 与 ``interview_core`` **同一个值**（稳定 warning 码，进 ``warnings`` 不进 ``errors``）。
WARNING_KNOWLEDGE_FAILED = interview_core.WARNING_KNOWLEDGE_FAILED

#: 低于该分数的维度会被列入「薄弱点」（0-100 口径，与 ``interview_core`` 的评分一致）。
WEAK_DIMENSION_THRESHOLD = 60

#: 短于此长度的回答视为「过短，缺少展开」。
SHORT_ANSWER_CHARS = 60

#: 维度键 → 可读名（只用于「薄弱点」文案）
_DIMENSION_LABELS: Tuple[Tuple[str, str], ...] = (
    ("technical_score", "技术准确性"),
    ("logic_score", "逻辑结构"),
    ("expression_score", "表达清晰度"),
    ("adaptability_score", "应变与深度"),
)

#: 全部维度都合格时的兜底薄弱点文案（**不伪造问题**：只是提示可追问更深一层）
COMPLETE_ANSWER_HINT = "回答完整度尚可，可追问更深的取舍与边界"

DEFAULT_FOLLOW_UP_NO = 1


# ============================================================
# 二、内部工具
# ============================================================
def _clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return ""


def _str_list(value: Any) -> List[str]:
    """归一为非空字符串列表（保序去重）；字符串**不逐字符拆分**。"""
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


def _as_int(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def question_payload(question: Any) -> Dict[str, Any]:
    """把「本题」归一成 ``interview_core.score_answer`` 需要的 dict。

    接受的形状：``Mapping``、带属性的对象（ORM ``InterviewQuestion`` 行）、裸字符串
    （视为只有题干）。**未知键忽略、缺失键取默认值**，绝不猜内容。
    """
    if question is None:
        return {
            "id": None,
            "question_no": None,
            "question": "",
            "question_type": "",
            "topic": "",
            "difficulty": "",
            "expected_points": [],
        }

    if isinstance(question, str):
        return {
            "id": None,
            "question_no": None,
            "question": question.strip(),
            "question_type": "",
            "topic": "",
            "difficulty": "",
            "expected_points": [],
        }

    def _read(name: str, default: Any = None) -> Any:
        if isinstance(question, Mapping):
            value = question.get(name, default)
        else:
            value = getattr(question, name, default)
        return default if value is None else value

    return {
        "id": _read("id"),
        "question_no": _as_int(_read("question_no")),
        "question": _clean(_read("question")),
        "question_type": _clean(_read("question_type")),
        "topic": _clean(_read("topic")),
        "difficulty": _clean(_read("difficulty")),
        "expected_points": _str_list(_read("expected_points", [])),
    }


def _result(**overrides: Any) -> Dict[str, Any]:
    """构造恒定字段集的结果字典。"""
    payload: Dict[str, Any] = {
        "ok": False,
        "question_no": None,
        "question": "",
        "question_type": "",
        "topic": "",
        "difficulty": "",
        "expected_points": [],
        "reason": "",
        "based_on": "",
        "follow_up_no": DEFAULT_FOLLOW_UP_NO,
        "focus": "",
        "analysis": {},
        "knowledge_chunks": 0,
        "warnings": [],
        "errors": [],
        "error": None,
    }
    payload.update(overrides)
    return payload


def _fail(code: str, message: str, **extra: Any) -> Dict[str, Any]:
    """失败结果：``errors`` 放**稳定错误码**，``error`` 放可读说明。"""
    payload: Dict[str, Any] = {"ok": False, "errors": [code], "error": message}
    payload.update(extra)
    return _result(**payload)


# ============================================================
# 三、② 分析回答（纯函数，复用 interview_core.score_answer）
# ============================================================
def analyze_answer(question: Any, answer_text: Any) -> Dict[str, Any]:
    """**分析候选人回答** → 恒定字段集的分析结果（见 :data:`ANALYSIS_FIELDS`）。

    刻意**只复用** ``interview_core.score_answer``（纯规则、确定性、可申诉），
    不重新实现一套评分口径——两套分数必然分叉。

    在规则分之上再推导三样**可解释**的东西：

    1. ``hit_points`` / ``missed_points``：采分点命中与**未命中**（后者是追问的主要靶子）；
    2. ``weak_points``：低于 :data:`WEAK_DIMENSION_THRESHOLD` 的维度、过短的作答、
       以及未覆盖的采分点（都是**可读文案**，不是编码）；
    3. ``focus``：建议追问方向——优先取**第一个未命中的采分点**，
       其次取本题 ``topic``（都取不到就留空，由 :func:`followup_topic` 兜底）。

    纯函数：不改入参、同输入同输出；**不调模型、不检索**。
    """
    payload = question_payload(question)
    text = (answer_text or "").strip() if isinstance(answer_text, str) else ""
    scored = interview_core.score_answer(payload, text)

    expected = _str_list(payload.get("expected_points"))
    lower = text.lower()
    hit = [point for point in expected if point.lower() in lower]
    missed = [point for point in expected if point not in hit]

    weak: List[str] = []
    if missed:
        weak.append("未覆盖采分点：" + "、".join(missed))
    for key, label in _DIMENSION_LABELS:
        value = scored.get(key)
        if isinstance(value, int) and value < WEAK_DIMENSION_THRESHOLD:
            weak.append(f"{label}偏弱（{value} 分）")
    if len(text) < SHORT_ANSWER_CHARS:
        weak.append("回答过短，缺少展开")
    if not weak:
        weak.append(COMPLETE_ANSWER_HINT)

    focus = missed[0] if missed else _clean(payload.get("topic"))

    return {
        "question": _clean(payload.get("question")),
        "question_no": payload.get("question_no"),
        "question_type": _clean(payload.get("question_type")),
        "topic": _clean(payload.get("topic")),
        "answer": text,
        "answer_chars": len(text),
        "score": scored["score"],
        "technical_score": scored["technical_score"],
        "logic_score": scored["logic_score"],
        "expression_score": scored["expression_score"],
        "adaptability_score": scored["adaptability_score"],
        "feedback": _clean(scored["feedback"]),
        "hit_points": hit,
        "missed_points": missed,
        "weak_points": weak,
        "focus": focus,
    }


def followup_topic(analysis: Any, pack: Any = None) -> str:
    """推导「本次检索用什么线索」——纯函数、确定性、不改入参。

    优先级：

    1. ``analysis.focus``（由回答推导：未命中的采分点 / 本题 topic）
    2. ``pack.focus_topics`` 首项（薄弱点 + 计划缺口）
    3. ``pack.weak_topics`` 首项
    4. ``pack.current_stage``（兜底：至少有阶段语义）
    5. ``""``（**不猜**：检索层拿到空串会返回 ``[]``）
    """
    focus = _clean((analysis or {}).get("focus"))
    if focus:
        return focus
    pack = pack or {}
    for key in ("focus_topics", "weak_topics"):
        names = _str_list(pack.get(key))
        if names:
            return names[0]
    return _clean(pack.get("current_stage"))


# ============================================================
# 四、③ 检索（唯一接缝：interview_core）
# ============================================================
async def gather_knowledge(
    db: Any,
    session: Any,
    topic: str,
    context: Any,
    *,
    retriever: Any = None,
    use_rag: bool = False,
    retriever_kwargs: Optional[Mapping[str, Any]] = None,
) -> Tuple[List[Any], List[str], Any]:
    """取知识 → ``(chunks, warnings, resolved_retriever)``。

    **不复制** ``interview_core`` 的组装与降级逻辑：

    - 组装：``interview_core.resolve_retriever``（全项目唯一的 RAG 开关）
    - 检索：``interview_core.retrieve_knowledge``（全项目唯一的检索接线点）

    检索是**可选增强**：任何异常都静默回退为「无知识」并记一条
    :data:`WARNING_KNOWLEDGE_FAILED`，**绝不让追问中断**（与出题链同一约定）。
    """
    resolved, warnings = interview_core.resolve_retriever(
        db, retriever, use_rag, retriever_kwargs=retriever_kwargs
    )
    if resolved is None and not warnings:
        # 没开 RAG 也没注入检索器 ⇒ **一次检索调用都不发生**（默认路径）
        return [], [], resolved

    job = await interview_core.load_job_row(db, getattr(session, "job_id", None))
    try:
        chunks = await interview_core.retrieve_knowledge(
            job, topic, context, retriever=resolved
        )
    except Exception:  # noqa: BLE001 - 可选增强，任何故障都降级为「无知识」
        return [], [*warnings, WARNING_KNOWLEDGE_FAILED], resolved
    return list(chunks or []), list(warnings), resolved


# ============================================================
# 五、只读加载：按题号取题 / 取最近一道已作答的题
# ============================================================
async def load_question(db: Any, session_id: int, question_no: Any) -> Any:
    """按题号取题（只读；取不到返回 ``None``）。

    刻意按题号**精确匹配**而不是「取第 N 行」——**不猜**。
    """
    target = _as_int(question_no)
    if target is None or target < 1:
        return None

    from sqlalchemy import select

    from models import InterviewQuestion

    rows = (
        await db.execute(
            select(InterviewQuestion).where(InterviewQuestion.session_id == session_id)
        )
    ).scalars().all()
    for row in rows:
        if row.question_no == target:
            return row
    return None


async def load_last_answered_question(db: Any, session: Any) -> Any:
    """取**最近一道已作答**的题（只读；取不到返回 ``None``）。

    题号在作答后 +1，因此「最近已作答」= ``current_question_no - 1``。
    """
    session_id = getattr(session, "id", None)
    if session_id is None:
        return None
    target = (_as_int(getattr(session, "current_question_no", None)) or 1) - 1
    if target < 1:
        return None
    return await load_question(db, session_id, target)


# ============================================================
# 六、全链：生成追问
# ============================================================
async def generate_followup(
    db: Any,
    session_id: int,
    *,
    answer_text: Any,
    question: Any = None,
    question_no: Optional[int] = None,
    user_id: Optional[int] = None,
    spark: Any = None,
    retriever: Any = None,
    use_rag: bool = False,
    retriever_kwargs: Optional[Mapping[str, Any]] = None,
    pack: Any = None,
    max_history: Optional[int] = DEFAULT_MAX_HISTORY,
    follow_up_no: int = DEFAULT_FOLLOW_UP_NO,
) -> Dict[str, Any]:
    """**追问全链**：分析回答 → Retriever 检索 → Agent 生成追问。

    参数
    ----
    - ``db`` / ``session_id``：数据库会话与面试会话 id。
    - ``answer_text``：候选人刚才那句回答（**必需**；为空 → ``answer_required``）。
    - ``question``：追问所针对的题目（``Mapping`` / ORM 行 / 裸字符串）。
      缺省时按 ``question_no`` 取；``question_no`` 也缺省则取「最近一道已作答的题」；
      都取不到 → ``question_required``（**不猜题干**）。
    - ``question_no``：可选，指定题号（仅当 ``question`` 缺省时生效）。
    - ``user_id``：可选。传入则校验会话归属，非本人按「不存在」处理。
    - ``spark``：可选，注入的 Spark 客户端（测试一律注入 Mock）。
    - ``retriever`` / ``use_rag`` / ``retriever_kwargs``：**与出题链同一套开关**——
      注入 ``retriever`` 优先；``use_rag=True`` 才组装真实链路；都不给 ⇒ 不检索。
    - ``pack``：可选，**注入**上下文包（跳过读库，便于脱离 DB 单测）。
    - ``max_history``：上下文包保留的最近历史问答条数。
    - ``follow_up_no``：本次是第几次追问（仅用于 Prompt 展示）。

    返回
    ----
    字段集恒定，见 :data:`FOLLOWUP_RESULT_FIELDS`。失败时 ``ok=False``、
    ``question`` 恒为 ``""``（**绝不放行未校验的问题**），``errors`` 为稳定错误码：
    ``session_not_found`` / ``session_finished`` / ``answer_required`` /
    ``question_required`` / ``agent_failed``。
    知识检索失败**不进 ``errors``**，只在 ``warnings`` 里记
    :data:`WARNING_KNOWLEDGE_FAILED`。

    **不写库**：题目持久化、上下文更新、追问计数均由调用方负责
    （``interview_context.increment_follow_up`` 已提供），因此同一入参重复调用是安全的。
    """
    # ---- 1. session 闸门 ----
    session = await interview_core.load_session_row(db, session_id)
    if session is None or (user_id is not None and session.user_id != user_id):
        return _fail(ERROR_SESSION_NOT_FOUND, f"面试会话 {session_id} 不存在")

    from models import SESSION_STATUS_FINISHED

    if session.status == SESSION_STATUS_FINISHED:
        return _fail(ERROR_SESSION_FINISHED, "面试已结束，无法再生成追问")

    # ---- 2. 上下文整合（简历 + 岗位 + 历史回答；知识随后挂上）----
    if pack is None:
        pack = await load_context_pack(db, session_id, max_history=max_history)

    # ---- 3. 本题 + 回答 ----
    text = (answer_text or "").strip() if isinstance(answer_text, str) else ""
    if not text:
        return _fail(ERROR_ANSWER_REQUIRED, "候选人回答为空，无法生成追问")

    if question is None:
        if question_no is not None:
            question = await load_question(db, session_id, question_no)
        else:
            question = await load_last_answered_question(db, session)

    payload = question_payload(question)
    if not payload.get("question"):
        return _fail(ERROR_QUESTION_REQUIRED, "无法确定追问所针对的题目，请显式传入 question")

    # ---- 4. ② 分析回答 ----
    analysis = analyze_answer(payload, text)
    topic = followup_topic(analysis, pack)
    if topic:
        analysis = {**analysis, "focus": topic}

    # ---- 5. ③ 检索（可选增强；默认一次都不调用）----
    chunks, warnings, _resolved = await gather_knowledge(
        db,
        session,
        topic,
        pack,
        retriever=retriever,
        use_rag=use_rag,
        retriever_kwargs=retriever_kwargs,
    )
    pack_with_knowledge = (
        pack if not chunks else attach_knowledge(pack, chunks)
    )

    # ---- 6. ④ Agent 生成追问（携带知识；知识为空走原模板）----
    candidate = await interview_followup_agent.generate_followup(
        pack_with_knowledge,
        analysis,
        follow_up_no=follow_up_no,
        knowledge_context=chunks,
        spark=spark,
    )

    if not candidate.get("ok"):
        return _fail(
            ERROR_AGENT_FAILED,
            candidate.get("error") or "生成追问失败",
            question_no=analysis.get("question_no"),
            based_on=analysis.get("question", ""),
            follow_up_no=int(follow_up_no),
            focus=topic,
            analysis=analysis,
            knowledge_chunks=len(chunks),
            warnings=list(warnings),
        )

    # ---- 7. 结果 ----
    return _result(
        ok=True,
        question_no=analysis.get("question_no"),
        question=_clean(candidate.get("question")),
        question_type=_clean(candidate.get("question_type")),
        topic=_clean(candidate.get("topic")),
        difficulty=_clean(candidate.get("difficulty")),
        expected_points=_str_list(candidate.get("expected_points")),
        reason=_clean(candidate.get("reason")),
        based_on=analysis.get("question", ""),
        follow_up_no=int(follow_up_no),
        focus=topic,
        analysis=analysis,
        knowledge_chunks=len(chunks),
        warnings=list(warnings),
    )


__all__ = [
    "FOLLOWUP_RESULT_FIELDS",
    "ANALYSIS_FIELDS",
    "ERROR_SESSION_NOT_FOUND",
    "ERROR_SESSION_FINISHED",
    "ERROR_ANSWER_REQUIRED",
    "ERROR_QUESTION_REQUIRED",
    "ERROR_AGENT_FAILED",
    "WARNING_KNOWLEDGE_FAILED",
    "WEAK_DIMENSION_THRESHOLD",
    "SHORT_ANSWER_CHARS",
    "COMPLETE_ANSWER_HINT",
    "DEFAULT_FOLLOW_UP_NO",
    "question_payload",
    "analyze_answer",
    "followup_topic",
    "gather_knowledge",
    "load_question",
    "load_last_answered_question",
    "generate_followup",
]
