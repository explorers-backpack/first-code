# -*- coding: utf-8 -*-
"""AI 模拟面试 · Interview Agent（问题生成）。

职责边界
--------
本阶段**只实现「根据当前面试状态生成一道问题」**。以下一律不做（留给后续阶段）：

- 回答评分
- 动态追问
- 下一题决策
- 面试报告
- 讯飞数字人 / ASR / TTS / RAG

``generate_question`` 是**无副作用的纯生成函数**：不读库、不写库、不推进面试状态、
不改动表结构。生成结果由调用方负责持久化（例如
``interview_context.add_asked_question``）——Agent 只负责「组装上下文 → 调模型 →
解析 → 校验 → 返回结构化问题」。

内部流程（对应用户要求的 9 步）
------------------------------
1. 取当前阶段 ``context.current_stage``
2. 取已提问问题 ``context.asked_questions``（用于查重）
3. 取已覆盖知识点 ``context.covered_topics``
4. 取优先考察点 ``plan.priority_topics``（经 ``interview_plan`` 注入模型）
5. 加载 ``prompts/interview/question.txt``
6. 调用**既有** Spark 服务（``SparkAPI.chat_async``，不重新实现鉴权）
7. 解析 JSON（``prompts.parse_question_output``）
8. 校验问题（非空 / 不重复 / topic 非空 / difficulty 合法）
9. 返回结构化 InterviewQuestion

失败处理策略
------------
- **可重试**：非法 JSON、以及各类校验不通过（空问题、缺 topic、难度非法、与历史重复）
  —— 最多发起**一次**修复请求（``prompts/interview/question_repair.txt``，
  带上失败原因与上次原始输出）。
- **不重试**：Spark 实例不可用、调用抛异常、返回失败哨兵串——属于服务级故障，
  重试无意义，直接返回明确错误。
- **绝不伪造问题**：任何失败路径返回 ``question=""`` + 可读的 ``error``，
  不会用占位文本或题库兜底。

返回值形状
----------
成功与失败**字段集完全一致**，便于调用方无分支取值::

    {"ok": bool, "question": str, "question_type": str, "topic": str,
     "difficulty": str, "expected_points": list, "reason": str, "error": str|None}

其中 ``ok=True`` 时的 6 个业务字段即为 InterviewQuestion 的结构化内容。
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional

from models import DIFFICULTIES, INTERVIEW_STAGES
from prompts import parse_question_output, render_prompt

# ============================================================
# 一、常量
# ============================================================
PROMPT_GROUP = "interview"
PROMPT_QUESTION = "question"
PROMPT_QUESTION_REPAIR = "question_repair"

DEFAULT_STAGE = INTERVIEW_STAGES[0]          # introduction
DEFAULT_INTERVIEW_TYPE = "comprehensive"
DEFAULT_DIFFICULTY = "mid"

#: 修复请求最多发起几次（用户要求：最多一次）
MAX_REPAIR_ATTEMPTS = 1

#: 注入 Prompt 的简历摘要上限
RESUME_SUMMARY_MAX = 1200

#: 回填进修复提示词的上次原始输出上限
REPAIR_RAW_MAX = 1500

#: 判为「高度重复」的相似度阈值（字符二元组 Dice 系数）
DUPLICATE_THRESHOLD = 0.85

#: 一方完整包含另一方时，较短一方达到该长度即直接判为高度重复
CONTAINMENT_MIN_LEN = 8

#: 相似度比较时先剔除的标点与空白
_PUNCTUATION = re.compile(
    r"[\s\u3000，。、；：？！,.;:?!\"'“”‘’（）()\[\]【】《》<>—\-_/\\|~`*#]+"
)

#: Spark 服务失败时返回的哨兵串前缀（见 ``main.SparkAPI.chat``，本模块只读取不修改）
_SPARK_FAILURE_PREFIXES = ("API调用失败", "SparkAPI not available", "AI 回答为空")

_EMPTY_RESUME = "（无简历）"
_EMPTY_JOB_TITLE = "（未指定岗位）"
_EMPTY_JOB_DESCRIPTION = "（未提供岗位描述）"


# ============================================================
# 二、内部工具
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
    """归一为字符串数组：只保留非空字符串，保序去重。"""
    if not isinstance(value, (list, tuple)):
        return []
    out: List[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        name = item.strip()
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def _question_texts(asked: Any) -> List[str]:
    """把 ``asked_questions`` 归一成问题文本列表。

    该字段可能同时存放两种形态：纯字符串，或
    ``{"question_no": 3, "question": "...", "stage": "..."}`` 这样的对象
    （``interview_context.add_asked_question`` 两种都接受）。
    """
    if not isinstance(asked, (list, tuple)):
        return []
    out: List[str] = []
    for item in asked:
        if isinstance(item, Mapping):
            text = _clean(item.get("question"))
        else:
            text = _clean(item)
        if text:
            out.append(text)
    return out


def _resume_text(resume: Any) -> str:
    """取简历正文。支持 ORM ``Resume``、``{"content": ...}`` 或裸字符串。"""
    if resume is None:
        return ""
    if isinstance(resume, str):
        return resume.strip()
    return _clean(_get(resume, "content"))


def _job_description(job: Any) -> str:
    """把岗位的技能要求与职责拼成一段描述。"""
    parts: List[str] = []
    skills = _clean(_get(job, "skills"))
    duty = _clean(_get(job, "duty"))
    if skills:
        parts.append(f"技能要求：{skills}")
    if duty:
        parts.append(f"岗位职责：{duty}")
    return "；".join(parts) if parts else _EMPTY_JOB_DESCRIPTION


def _clip(text: Any, limit: int) -> str:
    raw = "" if text is None else str(text)
    return raw if len(raw) <= limit else raw[:limit] + "…（已截断）"


def _is_spark_failure(raw: Any) -> bool:
    """判断 Spark 返回是否为失败哨兵（``SparkAPI`` 从不抛异常，只返回错误串）。"""
    if not isinstance(raw, str):
        return True
    text = raw.strip()
    if not text:
        return True
    return any(text.startswith(prefix) for prefix in _SPARK_FAILURE_PREFIXES)


def _default_spark() -> Any:
    """延迟获取全局 Spark 实例，避免 services → main 的模块级循环导入。"""
    try:
        from main import spark_api  # noqa: PLC0415 - 刻意延迟导入

        return spark_api
    except Exception:  # noqa: BLE001 - 环境不完整时视为不可用
        return None


def failure(error: str) -> Dict[str, Any]:
    """构造失败结果（字段集与成功结果一致，便于调用方无分支取值）。"""
    return {
        "ok": False,
        "question": "",
        "question_type": "",
        "topic": "",
        "difficulty": "",
        "expected_points": [],
        "reason": "",
        "error": error,
    }


# ============================================================
# 三、查重（确定性）
# ============================================================
def _normalize_question(text: Any) -> str:
    """归一化：转小写、去标点与空白，仅保留实义字符。"""
    return _PUNCTUATION.sub("", _clean(text).lower())


def _bigrams(text: str) -> set[str]:
    if not text:
        return set()
    if len(text) < 2:
        return {text}
    return {text[i:i + 2] for i in range(len(text) - 1)}


def question_similarity(left: Any, right: Any) -> float:
    """字符二元组 Dice 系数，取值 0.0-1.0。

    选它的理由：中文没有词边界，按字符二元组比较既能容忍虚词增删，
    又对「同一问题的改写」敏感；纯规则、确定性、可复现。

    另加一条**包含关系兜底**：「同一问题 + 附加从句」是最常见的重复形态，
    此时二元组系数会被长度差稀释（如 8 字的「请做一下自我介绍」对 19 字的
    扩写版本只有 0.58），单靠阈值会漏判。仅当较短一方本身已足够长
    （``CONTAINMENT_MIN_LEN``）时才启用，避免短串误命中。
    """
    a, b = _normalize_question(left), _normalize_question(right)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if min(len(a), len(b)) >= CONTAINMENT_MIN_LEN and (a in b or b in a):
        return 1.0
    grams_a, grams_b = _bigrams(a), _bigrams(b)
    if not grams_a or not grams_b:
        return 0.0
    return 2 * len(grams_a & grams_b) / (len(grams_a) + len(grams_b))


def is_duplicate_question(
    question: Any, history: Any, threshold: float = DUPLICATE_THRESHOLD
) -> bool:
    """新问题是否与历史问题高度重复。"""
    text = _clean(question)
    if not text:
        return False
    for previous in _question_texts(history):
        if question_similarity(text, previous) >= threshold:
            return True
    return False


# ============================================================
# 四、校验
# ============================================================
def validate_question(
    candidate: Mapping[str, Any],
    *,
    asked_questions: Any = None,
    allowed_difficulties: Any = DIFFICULTIES,
) -> Optional[str]:
    """校验生成结果，**通过返回 ``None``，失败返回可读的错误说明**。

    校验项（按顺序短路）：

    1. ``question`` 不能为空
    2. ``topic`` 不能为空
    3. ``difficulty`` 必须在 ``DIFFICULTIES`` 内
    4. 不能与 ``asked_questions`` 中的历史问题高度重复

    刻意**不校验** ``question_type`` 是否属于业务枚举：阶段/题型的推进规则属于
    状态机与业务层职责，本阶段不引入额外约束。
    """
    question = _clean((candidate or {}).get("question"))
    if not question:
        return "question 不能为空"

    topic = _clean((candidate or {}).get("topic"))
    if not topic:
        return "topic 不能为空"

    difficulty = _clean((candidate or {}).get("difficulty"))
    if difficulty not in tuple(allowed_difficulties or ()):
        return (
            f"difficulty 取值非法：{difficulty!r}，"
            f"允许：{'/'.join(allowed_difficulties or ())}"
        )

    history = _question_texts(asked_questions)
    for previous in history:
        score = question_similarity(question, previous)
        if score >= DUPLICATE_THRESHOLD:
            return f"与历史问题高度重复（相似度 {score:.0%}）：{previous[:60]}"
    return None


def _with_expected_difficulty(
    result: Dict[str, Any], expected: str
) -> Dict[str, Any]:
    """``difficulty`` 缺失时用**已知的**本场难度补齐。

    这不是「伪造」：本场难度是配置项（来自 InterviewPlan），值本身是确定的；
    只有模型给出了**非法值**时才判为校验失败。
    """
    if _clean(result.get("difficulty")) or not expected:
        return result
    updated = dict(result)
    updated["difficulty"] = expected
    return updated


# ============================================================
# 五、Prompt 变量组装
# ============================================================
def build_question_variables(
    context: Any, plan: Any = None, resume: Any = None, job: Any = None
) -> Dict[str, Any]:
    """组装 ``question.txt`` 需要的 10 个变量（不调模型、不读库，可单独测试）。

    ``priority_topics`` 通过 ``interview_plan`` 注入——它是模型判断「先问什么」的
    主要依据，因此这里把它显式归一后放进计划载荷，避免计划缺失时该信息丢失。
    """
    interview_type = _clean(_get(plan, "interview_type")) or DEFAULT_INTERVIEW_TYPE
    difficulty = _clean(_get(plan, "difficulty")) or DEFAULT_DIFFICULTY
    stage = _clean(_get(context, "current_stage")) or DEFAULT_STAGE

    plan_payload: Dict[str, Any] = {
        "interview_type": interview_type,
        "difficulty": difficulty,
        "total_questions": _get(plan, "total_questions"),
        "stages": _get(plan, "stages") or [],
        "target_topics": _str_list(_get(plan, "target_topics")),
        "priority_topics": _str_list(_get(plan, "priority_topics")),
        "resume_focus_points": _str_list(_get(plan, "resume_focus_points")),
    }

    resume_text = _resume_text(resume)
    return {
        "resume_summary": resume_text[:RESUME_SUMMARY_MAX] if resume_text else _EMPTY_RESUME,
        "job_title": _clean(_get(job, "job_name")) or _EMPTY_JOB_TITLE,
        "job_description": _job_description(job),
        "interview_type": interview_type,
        "difficulty": difficulty,
        "interview_plan": plan_payload,
        "current_stage": stage,
        "asked_questions": _question_texts(_get(context, "asked_questions")),
        "covered_topics": _str_list(_get(context, "covered_topics")),
        "weak_topics": _str_list(_get(context, "weak_topics")),
    }


# ============================================================
# 六、对外入口
# ============================================================
def _evaluate(
    raw: Any, asked_questions: Any, expected_difficulty: str
) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    """解析 + 校验一次模型输出。通过返回 ``(结果, None)``，否则 ``(None, 原因)``。"""
    parsed = parse_question_output(raw)
    if not parsed.get("ok"):
        return None, parsed.get("error") or "模型未返回有效问题"

    candidate = _with_expected_difficulty(parsed, expected_difficulty)
    problem = validate_question(candidate, asked_questions=asked_questions)
    if problem:
        return None, problem
    return candidate, None


async def generate_question(
    context: Any,
    plan: Any = None,
    resume: Any = None,
    job: Any = None,
    *,
    spark: Any = None,
) -> Dict[str, Any]:
    """根据当前面试状态生成**一道**问题。

    参数
    ----
    - ``context``：InterviewContext（``interview_context.get_context`` 的 dict，
      或 ORM 对象）。使用 ``current_stage`` / ``asked_questions`` /
      ``covered_topics`` / ``weak_topics``。
    - ``plan``：InterviewPlan（``interview_planner.build_plan_for`` 的 dict）。
      使用 ``interview_type`` / ``difficulty`` / ``priority_topics`` 等。
    - ``resume`` / ``job``：Resume / Job（ORM 对象、dict 或简历正文字符串）。
    - ``spark``：可选，注入的 Spark 客户端（须提供 ``async chat_async(str) -> str``）。
      不传则取 ``main.spark_api``；单元测试一律注入 Mock，不调用真实 API。

    返回
    ----
    见模块文档的「返回值形状」。失败时 ``ok=False`` 且 ``question=""``，
    绝不返回伪造的问题。
    """
    variables = build_question_variables(context, plan, resume, job)
    asked_questions = _get(context, "asked_questions")
    expected_difficulty = variables["difficulty"]

    client = spark if spark is not None else _default_spark()
    if client is None:
        return failure("Spark 服务不可用（未注入且无法加载 main.spark_api），无法生成问题")

    # ---- 首次请求 ----
    try:
        raw = await client.chat_async(
            render_prompt(PROMPT_QUESTION, variables, group=PROMPT_GROUP)
        )
    except Exception as exc:  # noqa: BLE001 - 服务级故障，不重试
        return failure(f"调用 Spark 服务失败：{exc}")

    if _is_spark_failure(raw):
        return failure(f"Spark 服务返回失败：{_clip(raw, 200)}")

    result, problem = _evaluate(raw, asked_questions, expected_difficulty)
    if result is not None:
        return result

    # ---- 最多一次修复 / 重新请求 ----
    for _ in range(MAX_REPAIR_ATTEMPTS):
        repair_variables = {
            "error": problem,
            "raw_output": _clip(raw, REPAIR_RAW_MAX),
            "asked_questions": variables["asked_questions"],
        }
        try:
            raw = await client.chat_async(
                render_prompt(PROMPT_QUESTION_REPAIR, repair_variables, group=PROMPT_GROUP)
            )
        except Exception as exc:  # noqa: BLE001
            return failure(f"修复请求调用 Spark 失败：{exc}")

        if _is_spark_failure(raw):
            return failure(f"Spark 服务返回失败：{_clip(raw, 200)}")

        result, problem = _evaluate(raw, asked_questions, expected_difficulty)
        if result is not None:
            return result

    return failure(
        f"生成问题失败（已发起 {MAX_REPAIR_ATTEMPTS} 次修复仍不通过）：{problem}"
    )
