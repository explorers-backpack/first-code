# -*- coding: utf-8 -*-
"""AI 模拟面试 · **下一题生成 Agent（携带历史回答）**（只生成，不检索、不读库、不写库）。

职责边界
--------
本模块与 ``services/interview_agent`` 是**同一类角色**（Agent = 只生成）：

======================  ==============================  ==================================
                        ``interview_agent``             本模块
======================  ==============================  ==================================
任务                    生成**下一道**题                生成**下一道**题
模板                    ``interview/question.txt``      ``interview_next/question.txt``
上下文                  简历 + 岗位 + 计划 + 进度        **多一段「历史回答」**（已答题目 + 作答摘要）
LLM / 检索 / DB         不调检索、不读库、不写库          同上
======================  ==============================  ==================================

为什么另起模块而不是改 ``interview_agent``
------------------------------------------
``interview_agent.py`` 与 ``prompts/interview/question.txt`` 都被源码指纹
（``FROZEN_SOURCES``）**冻结——一个字节都不能改**。而「让下一题看见候选人的历史回答」
必然要改模板变量集（多一个 ``{{history_qa}}``），与出题模板的变量集是**两套契约**，
因此按同一约定另起一个 Agent 模块，两边互不干扰。

与追问 Agent 的关系
-------------------
``interview_followup_agent`` 生成的是「针对刚才那句回答的追问」（单句级）；
本模块生成的是「本场的下一道正式题」（题级），只是**额外看见了历史回答**。
两者共用同一套解析 / 校验 / 修复口径（都复用 ``interview_agent`` 的公开与私有原语）。

复用而不是复制
--------------
- ``_is_spark_failure`` / ``_SPARK_FAILURE_PREFIXES``：Spark 失败哨兵串；
- ``_clip`` / ``_default_spark``：截断与「取全局 Spark 实例」；
- ``validate_question`` / ``is_duplicate_question`` / ``DUPLICATE_THRESHOLD``：
  查重与校验口径（**必须与出题路径完全一致**）；
- ``format_knowledge_context``：知识行的唯一格式化落点；
- ``failure``：失败结果的字段集；
- ``interview_context_pack.history_lines``：历史问答的**唯一**渲染口径。

外部知识上下文（可选）
----------------------
``generate_question(..., knowledge_context=[...])`` 由 :func:`render_next_prompt` 分流：
**知识为空走 ``question.txt``（原模板）**，非空才走 ``question_knowledge.txt``。
两个模板的一致性由测试的漂移守卫锁死（去掉知识小节后必须逐字节相同）。

返回值形状
----------
成功与失败**字段集完全一致**（与 ``interview_agent.generate_question`` 同形状）::

    {"ok": bool, "question": str, "question_type": str, "topic": str,
     "difficulty": str, "expected_points": list, "reason": str, "error": str|None}

**绝不伪造问题**：任何失败路径返回 ``question=""`` + 可读的 ``error``。
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from prompts import parse_question_output, render_prompt
from services.interview_agent import (
    DUPLICATE_THRESHOLD,
    _clip,
    _default_spark,
    _is_spark_failure,
    failure,
    format_knowledge_context,
    is_duplicate_question,
    validate_question,
)
from services.interview_context_pack import history_lines

# ============================================================
# 一、常量
# ============================================================
PROMPT_GROUP = "interview_next"
#: 无知识时用的模板（**空知识必须走它**，见 :func:`render_next_prompt`）。
PROMPT_QUESTION = "question"
#: 带「参考知识」小节的变体模板。
PROMPT_QUESTION_KNOWLEDGE = "question_knowledge"
#: 修复提示词**复用出题路径的那一份**（它只约束 JSON 结构，与题型无关），
#: 因此分组仍是 ``interview``。
PROMPT_REPAIR = "question_repair"
PROMPT_REPAIR_GROUP = "interview"

#: 修复请求最多发起几次（与出题路径一致：最多一次）
MAX_REPAIR_ATTEMPTS = 1

#: 回填进修复提示词的上次原始输出上限
REPAIR_RAW_MAX = 1500

#: ``question.txt`` 需要的 11 个变量（顺序 = 模板中首次出现的顺序）。
NEXT_VARIABLES: Tuple[str, ...] = (
    "interview_type",
    "difficulty",
    "current_stage",
    "job_title",
    "job_description",
    "resume_summary",
    "interview_plan",
    "history_qa",
    "asked_questions",
    "covered_topics",
    "weak_topics",
)

#: 变体模板额外需要的 1 个变量
KNOWLEDGE_VARIABLE = "knowledge_context"

#: 空值占位（与 ``interview_followup_agent`` 同字面量，由测试的漂移守卫锁死）
EMPTY_HISTORY = "（无历史问答）"
EMPTY_PLAN = "（无面试计划）"


# ============================================================
# 二、Prompt 变量组装（纯函数）
# ============================================================
def _clean(value: Any) -> str:
    """归一为字符串（与 ``interview_agent._clean`` 同语义）。"""
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
    """归一为非空字符串列表（保序去重）。"""
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


def render_plan_lines(plan_items: Any) -> List[str]:
    """把规则出题计划渲染成**文本行**（供 ``{{interview_plan}}`` 变量）。

    只渲染**可解释**的信息（题号 / 题型 / 知识点 / 题干），不塞任何内部字段名——
    模型只需要看得懂的内容。空计划返回 ``[]``，由调用方决定占位串。
    """
    if not isinstance(plan_items, (list, tuple)) or not plan_items:
        return []

    lines: List[str] = []
    for index, item in enumerate(plan_items, start=1):
        question = _clean(
            item.get("question") if isinstance(item, Mapping) else getattr(item, "question", None)
        )
        if not question:
            continue
        no = item.get("question_no") if isinstance(item, Mapping) else None
        no = no if isinstance(no, int) and no > 0 else index
        qtype = _clean(
            item.get("question_type") if isinstance(item, Mapping) else None
        )
        topic = _clean(item.get("topic") if isinstance(item, Mapping) else None)
        meta = " / ".join(part for part in (qtype, topic) if part)
        head = f"第 {no} 题" + (f"（{meta}）" if meta else "")
        lines.append(f"- {head}：{question}")
    return lines


def build_next_variables(pack: Any, plan_items: Any = None) -> Dict[str, Any]:
    """组装 ``question.txt`` 需要的 11 个变量（不调模型、不读库，可单独测试）。

    ``pack`` 是 :func:`services.interview_context_pack.load_context_pack` 的产出
    （简历 + 岗位 + **历史回答** + 进度），``plan_items`` 是本场规则出题计划
    （6 字段题目项的序列），仅用于渲染 ``{{interview_plan}}``。
    """
    pack = pack or {}

    history = _str_list(pack.get("history_qa"))
    if not history:
        # ``pack`` 通常只带结构化 ``history``（由上下文整合层产出），
        # 这里就地渲染成文本行——**渲染口径唯一**在 ``history_lines``。
        history = history_lines(pack)

    plan_lines = render_plan_lines(plan_items)
    if not plan_lines:
        plan_lines = _str_list(pack.get("interview_plan"))

    return {
        "interview_type": _clean(pack.get("interview_type")) or "comprehensive",
        "difficulty": _clean(pack.get("difficulty")) or "mid",
        "current_stage": _clean(pack.get("current_stage")),
        "job_title": _clean(pack.get("job_title")),
        "job_description": _clean(pack.get("job_description")),
        "resume_summary": _clean(pack.get("resume_summary")),
        "interview_plan": plan_lines or EMPTY_PLAN,
        "history_qa": history or EMPTY_HISTORY,
        "asked_questions": _str_list(pack.get("asked_questions")),
        "covered_topics": _str_list(pack.get("covered_topics")),
        "weak_topics": _str_list(pack.get("weak_topics")),
    }


def render_next_prompt(
    variables: Mapping[str, Any],
    knowledge_context: Any = None,
) -> Tuple[str, str]:
    """渲染下一题 Prompt，返回 ``(模板名, Prompt 正文)``。

    **分流规则（与出题 / 追问路径完全一致）**：

    - 知识为空 → 用 :data:`PROMPT_QUESTION`（**原模板、原 11 变量**）；
    - 知识非空 → 用 :data:`PROMPT_QUESTION_KNOWLEDGE`，额外注入
      :data:`KNOWLEDGE_VARIABLE`。

    用**两个模板**而不是同一模板里的条件块：``prompts`` 的严格模式双向校验变量，
    模板里只要有 ``{{knowledge_context}}``，空知识时也会渲染出「参考知识：（无）」，
    那就不再是「行为一致」了。两个模板的一致性由漂移守卫锁死。
    """
    lines = format_knowledge_context(knowledge_context)
    if not lines:
        return PROMPT_QUESTION, render_prompt(
            PROMPT_QUESTION, variables, group=PROMPT_GROUP
        )

    with_knowledge = dict(variables)
    with_knowledge[KNOWLEDGE_VARIABLE] = lines
    return (
        PROMPT_QUESTION_KNOWLEDGE,
        render_prompt(PROMPT_QUESTION_KNOWLEDGE, with_knowledge, group=PROMPT_GROUP),
    )


# ============================================================
# 三、解析 + 校验（与出题路径同一口径）
# ============================================================
def _with_expected_difficulty(result: Dict[str, Any], expected: str) -> Dict[str, Any]:
    """``difficulty`` 缺失时用**已知的**本场难度补齐（不是伪造）。"""
    if _clean(result.get("difficulty")) or not expected:
        return result
    updated = dict(result)
    updated["difficulty"] = expected
    return updated


def _evaluate(
    raw: Any, asked_questions: Any, expected_difficulty: str
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """解析 + 校验一次模型输出。通过返回 ``(结果, None)``，否则 ``(None, 原因)``。"""
    parsed = parse_question_output(raw)
    if not parsed.get("ok"):
        return None, parsed.get("error") or "模型未返回有效问题"

    candidate = _with_expected_difficulty(parsed, expected_difficulty)
    problem = validate_question(candidate, asked_questions=asked_questions)
    if problem:
        return None, problem
    return candidate, None


# ============================================================
# 四、对外入口
# ============================================================
async def generate_question(
    pack: Any,
    plan_items: Any = None,
    *,
    knowledge_context: Any = None,
    spark: Any = None,
) -> Dict[str, Any]:
    """根据「简历 + 岗位 + **历史回答** + 进度」生成**下一道**题。

    参数
    ----
    - ``pack``：面试上下文包（简历 + 岗位 + 历史回答 + 进度）。
    - ``plan_items``：本场规则出题计划（6 字段题目项），用于 ``{{interview_plan}}``。
    - ``knowledge_context``：**外部知识上下文**（RAG 扩展点），可选。
      接受 ``List[str]`` / ``List[KnowledgeChunk]`` / ``List[dict]`` / 单个 ``str``。
      **为空时走原模板，Prompt 与「引入知识能力之前」逐字节相同**。
      **本模块不主动调用任何 Retriever**——知识由调用方取好后传进来。
    - ``spark``：可选，注入的 Spark 客户端（须提供 ``async chat_async(str) -> str``）。
      不传则取 ``main.spark_api``；测试一律注入 Mock。

    返回
    ----
    与 ``interview_agent.generate_question`` **同形状**的 dict；失败时
    ``ok=False`` 且 ``question=""``，**绝不返回伪造的问题**。

    另外：与历史问题重复（含 ``pack.asked_questions``）会被 ``validate_question``
    拦下，因此下一题**不会与已问过的题撞车**。
    """
    variables = build_next_variables(pack, plan_items)
    asked_questions = variables["asked_questions"]
    expected_difficulty = variables["difficulty"]

    client = spark if spark is not None else _default_spark()
    if client is None:
        return failure("Spark 服务不可用（未注入且无法加载 main.spark_api），无法生成下一题")

    # ---- 首次请求 ----
    try:
        _, prompt = render_next_prompt(variables, knowledge_context)
        raw = await client.chat_async(prompt)
    except Exception as exc:  # noqa: BLE001 - 服务级故障，不重试
        return failure(f"调用 Spark 服务失败：{exc}")

    if _is_spark_failure(raw):
        return failure(f"Spark 服务返回失败：{_clip(raw, 200)}")

    result, problem = _evaluate(raw, asked_questions, expected_difficulty)
    if result is not None:
        return result

    # ---- 最多一次修复（复用出题路径的修复模板）----
    for _ in range(MAX_REPAIR_ATTEMPTS):
        repair_variables = {
            "error": problem,
            "raw_output": _clip(raw, REPAIR_RAW_MAX),
            "asked_questions": asked_questions,
        }
        try:
            raw = await client.chat_async(
                render_prompt(
                    PROMPT_REPAIR, repair_variables, group=PROMPT_REPAIR_GROUP
                )
            )
        except Exception as exc:  # noqa: BLE001
            return failure(f"修复请求调用 Spark 失败：{exc}")

        if _is_spark_failure(raw):
            return failure(f"Spark 服务返回失败：{_clip(raw, 200)}")

        result, problem = _evaluate(raw, asked_questions, expected_difficulty)
        if result is not None:
            return result

    return failure(
        f"生成下一题失败（已发起 {MAX_REPAIR_ATTEMPTS} 次修复仍不通过）：{problem}"
    )


__all__ = [
    "PROMPT_GROUP",
    "PROMPT_QUESTION",
    "PROMPT_QUESTION_KNOWLEDGE",
    "PROMPT_REPAIR",
    "PROMPT_REPAIR_GROUP",
    "MAX_REPAIR_ATTEMPTS",
    "REPAIR_RAW_MAX",
    "NEXT_VARIABLES",
    "KNOWLEDGE_VARIABLE",
    "EMPTY_HISTORY",
    "EMPTY_PLAN",
    "DUPLICATE_THRESHOLD",
    "is_duplicate_question",
    "render_plan_lines",
    "build_next_variables",
    "render_next_prompt",
    "generate_question",
]
