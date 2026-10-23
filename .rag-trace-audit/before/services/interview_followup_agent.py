# -*- coding: utf-8 -*-
"""AI 模拟面试 · **追问生成 Agent**（只生成，不检索、不读库、不写库）。

职责边界
--------
本模块与 ``services/interview_agent`` 是**同一类角色**（Agent = 只生成）：

======================  ==============================  ==========================
                        ``interview_agent``             本模块
======================  ==============================  ==========================
任务                    生成**下一道**题                生成**针对刚才这句回答**的追问
模板                    ``interview/question.txt``      ``interview_followup/followup.txt``
上下文                  简历 + 岗位 + 计划 + 进度        上面的全部 **+ 本题 + 回答 + 分析 + 历史问答**
LLM / 检索 / DB         不调检索、不读库、不写库          同上
======================  ==============================  ==========================

为什么另起模块而不是改 ``interview_agent``
------------------------------------------
``interview_agent.py`` 被源码指纹（``FROZEN_SOURCES``）冻结——**一个字节都不能改**。
追问需要「候选人答了什么」这一层上下文，与出题模板的 10 个变量是**两套契约**，
因此按同一约定另起一个 Agent 模块，两边互不干扰。

复用而不是复制
--------------
以下能力**直接复用** ``interview_agent``（那份源码被冻结，无法把私有名改成公开名，
只能按原名复用；这是「单一事实来源」优于「避免下划线」的取舍）：

- ``_is_spark_failure`` / ``_SPARK_FAILURE_PREFIXES``：Spark 失败哨兵串——
  复制一份必然与 ``main.SparkAPI`` 的改动脱节；
- ``_clip`` / ``_default_spark``：截断与「取全局 Spark 实例」；
- ``validate_question`` / ``is_duplicate_question`` / ``DUPLICATE_THRESHOLD``：
  查重与校验口径（**必须与出题路径完全一致**，否则同一问题会被两套规则分别放行）；
- ``format_knowledge_context``：知识行的唯一格式化落点；
- ``failure``：失败结果的字段集。

外部知识上下文（可选）
----------------------
``generate_followup(..., knowledge_context=[...])`` 由
:func:`render_followup_prompt` 分流：**知识为空走 ``followup.txt``（原模板）**，
非空才走 ``followup_knowledge.txt``（原模板 + 参考知识小节）。
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
PROMPT_GROUP = "interview_followup"
#: 无知识时用的模板（**空知识必须走它**，见 :func:`render_followup_prompt`）。
PROMPT_FOLLOWUP = "followup"
#: 带「参考知识」小节的变体模板。
PROMPT_FOLLOWUP_KNOWLEDGE = "followup_knowledge"
#: 修复提示词**复用出题路径的那一份**（它只约束 JSON 结构，与题型无关），
#: 因此分组仍是 ``interview``。
PROMPT_FOLLOWUP_REPAIR = "question_repair"
PROMPT_REPAIR_GROUP = "interview"

#: 修复请求最多发起几次（与出题路径一致：最多一次）
MAX_REPAIR_ATTEMPTS = 1

#: 注入 Prompt 的**候选人回答**上限
CANDIDATE_ANSWER_MAX = 1500

#: 回填进修复提示词的上次原始输出上限
REPAIR_RAW_MAX = 1500

#: ``followup.txt`` 需要的 14 个变量（顺序 = 模板中首次出现的顺序）。
FOLLOWUP_VARIABLES: Tuple[str, ...] = (
    "interview_type",
    "difficulty",
    "current_stage",
    "follow_up_no",
    "job_title",
    "job_description",
    "resume_summary",
    "current_question",
    "candidate_answer",
    "answer_analysis",
    "history_qa",
    "asked_questions",
    "covered_topics",
    "weak_topics",
)

#: 变体模板额外需要的 1 个变量
KNOWLEDGE_VARIABLE = "knowledge_context"

#: 空值占位（与 ``interview_agent`` 的占位串同字面量，由测试的漂移守卫锁死）
EMPTY_HISTORY = "（无历史问答）"
EMPTY_QUESTION = "（无题目）"


# ============================================================
# 二、Prompt 变量组装（纯函数）
# ============================================================
def _clean(value: Any) -> str:
    """归一为字符串（与 ``interview_agent._clean`` 同语义，见模块文档的复用说明）。"""
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


def render_answer_analysis(analysis: Any) -> List[str]:
    """把 :func:`services.interview_followup.analyze_answer` 的产出渲染成**文本行**。

    只渲染**可解释**的信息（分数 / 命中与未命中的采分点 / 薄弱点 / 建议方向），
    不塞任何原始字段名或内部编码——模型只需要看得懂的内容。

    空分析（``None``）返回 ``[]``；由调用方决定占位串。
    """
    if not analysis:
        return []

    lines: List[str] = []
    score = analysis.get("score")
    if score is not None:
        dims = " / ".join(
            f"{label} {analysis.get(key)}"
            for key, label in (
                ("technical_score", "技术"),
                ("logic_score", "逻辑"),
                ("expression_score", "表达"),
                ("adaptability_score", "应变"),
            )
            if analysis.get(key) is not None
        )
        lines.append(f"- 本题规则得分：{score}" + (f"（{dims}）" if dims else ""))

    chars = analysis.get("answer_chars")
    if chars is not None:
        lines.append(f"- 回答长度：{chars} 字")

    hit = _str_list(analysis.get("hit_points"))
    if hit:
        lines.append("- 已覆盖采分点：" + "、".join(hit))

    missed = _str_list(analysis.get("missed_points"))
    if missed:
        lines.append("- **未覆盖采分点**：" + "、".join(missed))

    weak = _str_list(analysis.get("weak_points"))
    for item in weak:
        lines.append(f"- 薄弱点：{item}")

    focus = _clean(analysis.get("focus"))
    if focus:
        lines.append(f"- 建议追问方向：{focus}")

    feedback = _clean(analysis.get("feedback"))
    if feedback:
        lines.append(f"- 规则评语：{feedback}")

    return lines


def build_followup_variables(
    pack: Any, analysis: Any, *, follow_up_no: int = 1
) -> Dict[str, Any]:
    """组装 ``followup.txt`` 需要的 14 个变量（不调模型、不读库，可单独测试）。

    ``pack`` 是 :func:`services.interview_context_pack.build_context_pack` 的产出
    （简历 + 岗位 + 历史回答 + 知识库），``analysis`` 是
    :func:`services.interview_followup.analyze_answer` 的产出（本题 + 回答 + 分析）。
    """
    pack = pack or {}
    analysis = analysis or {}

    answer = _clean(analysis.get("answer"))
    if len(answer) > CANDIDATE_ANSWER_MAX:
        answer = _clip(answer, CANDIDATE_ANSWER_MAX)

    analysis_lines = render_answer_analysis(analysis)
    history = _str_list(pack.get("history_qa"))
    if not history:
        # ``pack`` 通常只带结构化 ``history``（由上下文整合层产出），
        # 这里就地渲染成文本行——**渲染口径唯一**在 ``history_lines``。
        history = history_lines(pack)

    return {
        "interview_type": _clean(pack.get("interview_type")) or "comprehensive",
        "difficulty": _clean(pack.get("difficulty")) or "mid",
        "current_stage": _clean(pack.get("current_stage")),
        "follow_up_no": int(follow_up_no),
        "job_title": _clean(pack.get("job_title")),
        "job_description": _clean(pack.get("job_description")),
        "resume_summary": _clean(pack.get("resume_summary")),
        "current_question": _clean(analysis.get("question")) or EMPTY_QUESTION,
        "candidate_answer": answer,
        "answer_analysis": analysis_lines,
        "history_qa": history or EMPTY_HISTORY,
        "asked_questions": _str_list(pack.get("asked_questions")),
        "covered_topics": _str_list(pack.get("covered_topics")),
        "weak_topics": _str_list(pack.get("weak_topics")),
    }


def render_followup_prompt(
    variables: Mapping[str, Any],
    knowledge_context: Any = None,
) -> Tuple[str, str]:
    """渲染追问 Prompt，返回 ``(模板名, Prompt 正文)``。

    **分流规则（与出题路径完全一致）**：

    - 知识为空 → 用 :data:`PROMPT_FOLLOWUP`（**原模板、原 14 变量**）；
    - 知识非空 → 用 :data:`PROMPT_FOLLOWUP_KNOWLEDGE`，额外注入
      :data:`KNOWLEDGE_VARIABLE`。

    用**两个模板**而不是同一模板里的条件块：``prompts`` 的严格模式双向校验变量，
    模板里只要有 ``{{knowledge_context}}``，空知识时也会渲染出「参考知识：（无）」，
    那就不再是「行为一致」了。两个模板的一致性由漂移守卫锁死。
    """
    lines = format_knowledge_context(knowledge_context)
    if not lines:
        return PROMPT_FOLLOWUP, render_prompt(
            PROMPT_FOLLOWUP, variables, group=PROMPT_GROUP
        )

    with_knowledge = dict(variables)
    with_knowledge[KNOWLEDGE_VARIABLE] = lines
    return (
        PROMPT_FOLLOWUP_KNOWLEDGE,
        render_prompt(PROMPT_FOLLOWUP_KNOWLEDGE, with_knowledge, group=PROMPT_GROUP),
    )


# ============================================================
# 三、解析 + 校验（与出题路径同一口径）
# ============================================================
def _with_expected_difficulty(result: Dict[str, Any], expected: str) -> Dict[str, Any]:
    """``difficulty`` 缺失时用**已知的**本场难度补齐（不是伪造，见 ``interview_agent``）。"""
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
async def generate_followup(
    pack: Any,
    analysis: Any,
    *,
    follow_up_no: int = 1,
    knowledge_context: Any = None,
    spark: Any = None,
) -> Dict[str, Any]:
    """根据「本题 + 候选人回答 + 回答分析 + 历史问答」生成**一道追问**。

    参数
    ----
    - ``pack``：面试上下文包（简历 + 岗位 + 历史回答 + 知识库）。
    - ``analysis``：回答分析（含本题、回答原文、规则评分与薄弱点）。
    - ``follow_up_no``：本次是第几次追问（仅用于 Prompt 展示）。
    - ``knowledge_context``：**外部知识上下文**（RAG 扩展点），可选。
      接受 ``List[str]`` / ``List[KnowledgeChunk]`` / ``List[dict]`` / 单个 ``str``。
      **为空时走原模板，Prompt 与「引入知识能力之前」逐字节相同**。
      **本模块不主动调用任何 Retriever**——知识由调用方（追问链）取好后传进来。
    - ``spark``：可选，注入的 Spark 客户端（须提供 ``async chat_async(str) -> str``）。
      不传则取 ``main.spark_api``；测试一律注入 Mock。

    返回
    ----
    与 ``interview_agent.generate_question`` **同形状**的 dict；失败时
    ``ok=False`` 且 ``question=""``，**绝不返回伪造的问题**。

    另外：与历史问题重复（含 ``pack.asked_questions``）会被
    ``validate_question`` 拦下，因此追问**不会与已问过的题撞车**。
    """
    variables = build_followup_variables(pack, analysis, follow_up_no=follow_up_no)
    asked_questions = variables["asked_questions"]
    expected_difficulty = variables["difficulty"]

    client = spark if spark is not None else _default_spark()
    if client is None:
        return failure("Spark 服务不可用（未注入且无法加载 main.spark_api），无法生成追问")

    # ---- 首次请求 ----
    try:
        _, prompt = render_followup_prompt(variables, knowledge_context)
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
                    PROMPT_FOLLOWUP_REPAIR, repair_variables, group=PROMPT_REPAIR_GROUP
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
        f"生成追问失败（已发起 {MAX_REPAIR_ATTEMPTS} 次修复仍不通过）：{problem}"
    )


__all__ = [
    "PROMPT_GROUP",
    "PROMPT_FOLLOWUP",
    "PROMPT_FOLLOWUP_KNOWLEDGE",
    "PROMPT_FOLLOWUP_REPAIR",
    "PROMPT_REPAIR_GROUP",
    "MAX_REPAIR_ATTEMPTS",
    "CANDIDATE_ANSWER_MAX",
    "REPAIR_RAW_MAX",
    "FOLLOWUP_VARIABLES",
    "KNOWLEDGE_VARIABLE",
    "EMPTY_HISTORY",
    "EMPTY_QUESTION",
    "DUPLICATE_THRESHOLD",
    "is_duplicate_question",
    "render_answer_analysis",
    "build_followup_variables",
    "render_followup_prompt",
    "generate_followup",
]
