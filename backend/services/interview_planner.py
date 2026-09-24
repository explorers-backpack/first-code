# -*- coding: utf-8 -*-
"""AI 模拟面试 · Interview Planner（面试计划制定）。

职责边界
--------
**只制定计划，不出题。** 本模块根据「简历 + 目标岗位 + 面试配置」产出结构化的
``InterviewPlan``：阶段划分、各阶段时间权重与目标题量，以及本场面试的
目标知识点 / 优先考察点 / 简历重点。**不生成任何具体面试问题**——那是
Interview Agent 的职责。

输入 / 输出
-----------
输入：``Resume``、``Job``、``interview_type``、``difficulty``、``duration``
输出：``InterviewPlan``（契约见 ``schemas.interview.InterviewPlanOut``）::

    {
      "interview_type": "comprehensive",
      "difficulty": "mid",
      "duration": 30,
      "total_questions": 12,
      "stages": [{"stage": "introduction", "weight": 10, "target_questions": 1}, ...],
      "target_topics": ["Java", "Spring Boot", "MySQL", "Redis"],
      "priority_topics": ["Redis", "MySQL"],
      "resume_focus_points": ["电商项目", "订单系统"],
      "source": "rule"
    }

确定性优先，LLM 可选
--------------------
- ``build_plan``：**纯规则、确定性**（同输入同输出），不访问网络、不查库，
  同时充当 LLM 失败时的 **fallback**。
- ``refine_plan_with_llm`` / ``build_plan_for(..., use_llm=True)``：在规则计划之上
  可选调用**既有 Spark 服务**，且**只用于增强知识点与简历重点的抽取**。
  阶段划分、权重、题量分配**始终由规则产出**并做不变量校验，LLM 输出无法破坏
  计划结构；任何异常 / 非法返回 / 哨兵错误串一律静默回退到规则计划。
  提示词正文存放在 ``prompts/interview/planner.txt``，本模块只负责注入变量
  （见 ``prompts/loader.py``），不在 Python 中内联 Prompt。

题量推导规则（确定性）
----------------------
1. 题量预算 = ``round(duration / 每分钟题耗)``，按难度取值
   （junior 3.0 / mid 2.5 / senior 2.0 分钟每题），收敛到 [5, 20]；
   调用方显式传入 ``total_questions`` 时以传入值为准（收敛到 [1, 20]）。
2. 阶段权重取自 ``_STAGE_WEIGHTS``（按 ``interview_type`` 选模板，合计恒为 100）。
3. 题量按权重用**最大余额法**分配，保证各阶段题量之和恰好等于总题量，
   且每个阶段至少 1 题。基准例（comprehensive / mid / 30 分钟 → 12 题）的
   分配结果为 introduction 1、resume 2、technical 4、project 3、scenario 2。

优先级规则（确定性）
--------------------
- ``target_topics``：岗位技能 → 岗位职责文本中抽取的技能 → 简历技能，保序去重。
- ``priority_topics``：junior/mid 取「岗位要求但简历未体现」的**能力缺口**优先，
  senior 取「岗位与简历都有」的**待验证项**优先（考察深度）；上限 5 个。
- ``resume_focus_points``：从简历正文识别项目 / 系统 / 平台类名称（正则启发式），
  上限 5 个；识别不到即返回空数组，不编造。

存储与副作用
------------
本模块**不写库**。``build_plan`` 无任何副作用；``build_plan_for`` 只读取
``jobs`` / ``resume`` 两张既有表，不新增表、不修改既有表结构。
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import DIFFICULTIES, INTERVIEW_STAGES, Job, Resume
from prompts import PromptJSONError, extract_json_object, render_prompt
from services.resume_scoring import extract_skills

# ============================================================
# 一、常量与模板
# ============================================================
DEFAULT_INTERVIEW_TYPE = "comprehensive"
DEFAULT_DIFFICULTY = "mid"
DEFAULT_DURATION = 30

# 各难度下「一道题平均占用多少分钟」，用于由时长反推题量。
# mid 30 分钟 → 12 题，与产品给出的基准计划一致。
_MINUTES_PER_QUESTION: Dict[str, float] = {"junior": 3.0, "mid": 2.5, "senior": 2.0}

# 题量预算区间（与 schemas.interview.SessionCreateRequest.total_questions 的口径一致）
_QUESTION_BUDGET_MIN = 5
_QUESTION_BUDGET_MAX = 20
_TOTAL_QUESTIONS_MAX = 20

# 时长区间（与 schemas.interview.SessionCreateRequest.duration 的口径一致）
_DURATION_MIN = 5
_DURATION_MAX = 180

# 各面试类型的阶段权重模板，合计恒为 100。
# 阶段顺序不在此处定义——统一按 models.INTERVIEW_STAGES 的推进顺序输出，
# 以保证计划与 InterviewContext.current_stage 的状态机顺序一致。
_STAGE_WEIGHTS: Dict[str, Dict[str, int]] = {
    "comprehensive": {
        "introduction": 10,
        "resume": 15,
        "technical": 35,
        "project": 25,
        "scenario": 15,
    },
    "technical": {
        "introduction": 10,
        "resume": 10,
        "technical": 45,
        "project": 25,
        "scenario": 10,
    },
    "behavioral": {
        "introduction": 10,
        "resume": 20,
        "project": 30,
        "scenario": 20,
        "hr": 20,
    },
}

# 列表型输出上限
_TOPIC_LIMIT = 12        # target_topics
_PRIORITY_LIMIT = 5      # priority_topics
_FOCUS_LIMIT = 5         # resume_focus_points
_TOPIC_MAX_LEN = 40      # 单个知识点最大长度
_FOCUS_MAX_LEN = 30      # 单个简历重点最大长度

# 提示词中的中文标签（本地定义，避免与 interview_service 互相耦合）
_TYPE_LABELS = {"technical": "技术面", "behavioral": "行为面", "comprehensive": "综合面"}
_DIFFICULTY_LABELS = {"junior": "初级", "mid": "中级", "senior": "高级"}

_RESUME_EXCERPT_MAX = 1200

# Spark 失败时返回的哨兵串（见 main.SparkAPI.chat），命中即视为失败
_SPARK_SENTINELS = ("API调用失败", "SparkAPI not available", "AI 回答为空")

# 技能字符串分隔符（与 jobs.skills 的存储格式一致，口径同 interview_service）
_SKILL_SPLIT = re.compile(r"[,，、/;；|]")

# 简历重点的识别模式。前两个是「显式声明」，命中即取该行唯一结果；
# 第三个是「通用后缀」，同一行内可取多个（一行常写多个项目）。
_EXPLICIT_FOCUS_PATTERNS: Sequence[re.Pattern[str]] = (
    # 显式声明：项目名称：电商平台
    re.compile(r"(?:项目名称|项目名|项目|系统名称)[:：]\s*([^\n，,；;。]{2,30})"),
    # 括号标注：【电商项目】
    re.compile(r"【([^】]{2,30})】"),
)
_GENERIC_FOCUS_PATTERN = re.compile(
    r"([\u4e00-\u9fa5A-Za-z0-9][\u4e00-\u9fa5A-Za-z0-9\s\-]{1,18}"
    r"(?:项目|系统|平台|中台|服务|模块|小程序|网站|APP|App|app))"
)

# 常见的前置动词 / 修饰语，命中后剥离，使「负责电商项目」→「电商项目」
_LEAD_NOISE = (
    "负责", "主导", "参与", "完成", "开发", "搭建", "设计", "实现", "独立",
    "主要", "担任", "作为", "熟悉", "了解", "掌握", "使用", "基于", "在",
    "曾", "并", "与", "及", "和",
)

# 纯章节标题行，不作为重点候选
_SECTION_MARKERS = (
    "项目经历", "项目经验", "工作经历", "实习经历", "教育背景", "教育经历",
    "技能特长", "专业技能", "自我评价", "个人信息", "荣誉奖项", "获奖情况",
    "实践经历", "校园经历", "个人优势",
)

_BULLET_PREFIX = re.compile(r"^\s*(?:[-*•·—>]+|\d+[.、)]|[①②③④⑤⑥⑦⑧⑨⑩]|[（(]\d+[)）])\s*")


# ============================================================
# 二、内部工具
# ============================================================
def _split_skills(raw: Optional[str]) -> List[str]:
    """按常见分隔符切分技能字符串（与 jobs.skills 的存储格式一致）。"""
    if not raw:
        return []
    return [t.strip() for t in _SKILL_SPLIT.split(str(raw)) if t.strip()]


def _dedupe(items: Any, limit: int, max_len: int) -> List[str]:
    """保序去重（忽略大小写）、剔除超长项、截断到上限。

    ``items`` 必须是 list/tuple——**非序列直接返回空列表**，避免把字符串按字符
    逐个迭代（模型返回 ``"Java,MySQL"`` 而非数组时会退化成 ``["J","a","v",...]``）。
    """
    if not isinstance(items, (list, tuple)):
        return []
    out: List[str] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, str):
            continue
        name = item.strip()
        if not name or len(name) > max_len:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
        if len(out) >= limit:
            break
    return out


def _field(obj: Any, name: str) -> Optional[str]:
    """从 ORM 对象或 dict 中取字符串字段（兼容两种输入，便于脱离数据库测试）。"""
    if obj is None:
        return None
    value = obj.get(name) if isinstance(obj, dict) else getattr(obj, name, None)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _resume_text(resume: Any) -> str:
    """取简历正文。支持 ORM ``Resume`` 对象、``{"content": ...}`` 或裸字符串。"""
    if resume is None:
        return ""
    if isinstance(resume, str):
        return resume.strip()
    return _field(resume, "content") or ""


def _job_topics(job: Any) -> List[str]:
    """岗位侧知识点：``skills`` 字段优先，其次从 ``duty`` 正文抽取。"""
    topics: List[str] = list(_split_skills(_field(job, "skills")))
    duty = _field(job, "duty")
    if duty:
        topics.extend(extract_skills(duty))
    return _dedupe(topics, _TOPIC_LIMIT, _TOPIC_MAX_LEN)


def _resume_topics(resume: Any) -> List[str]:
    """简历侧知识点：复用 ``resume_scoring.extract_skills`` 的 canonical 名称。"""
    text = _resume_text(resume)
    if not text:
        return []
    return _dedupe(extract_skills(text), _TOPIC_LIMIT, _TOPIC_MAX_LEN)


def _allocate_questions(total: int, weights: Dict[str, int]) -> Dict[str, int]:
    """按权重把 ``total`` 道题分配到各阶段（最大余额法）。

    保证：分配结果之和**恰好**等于 ``total``；题量足够时每阶段至少 1 题。
    同输入同输出（并列时按 ``INTERVIEW_STAGES`` 顺序决定，排序稳定）。
    """
    names = [s for s in INTERVIEW_STAGES if s in weights]
    if not names:
        return {}
    if total <= 0:
        return {s: 0 for s in names}

    # 题量不足以覆盖全部阶段：按权重取前 total 个阶段各 1 题
    if total < len(names):
        ranked = sorted(names, key=lambda s: (-weights[s], names.index(s)))
        chosen = set(ranked[:total])
        return {s: (1 if s in chosen else 0) for s in names}

    weight_sum = sum(weights[s] for s in names) or 1
    quotas = {s: total * weights[s] / weight_sum for s in names}
    counts = {s: int(quotas[s]) for s in names}

    left = total - sum(counts.values())
    remainder_order = sorted(
        names, key=lambda s: (-(quotas[s] - counts[s]), names.index(s))
    )
    for stage in remainder_order[:left]:
        counts[stage] += 1

    # 保底：每个阶段至少 1 题（从题量最多的阶段匀出，不改变总数）
    for stage in sorted(names, key=lambda s: (-weights[s], names.index(s))):
        if counts[stage] >= 1:
            continue
        donor = max(names, key=lambda s: (counts[s], -names.index(s)))
        if counts[donor] <= 1:
            break
        counts[donor] -= 1
        counts[stage] = 1
    return counts


def _extract_focus_points(text: str) -> List[str]:
    """从简历正文识别项目 / 系统 / 平台类名称（启发式，识别不到即返回空）。

    - 显式声明（``项目名称：`` / ``【…】``）优先，命中即取该行唯一结果；
    - 否则用通用后缀模式在同一行内**取全部命中**——一行可能写了多个项目
      （如「负责电商项目，主导订单系统重构」应抽出两条）；
    - 命中的前缀噪声词（负责 / 主导 / 熟悉 …）会被剥离。
    """
    if not text:
        return []
    candidates: List[str] = []
    for raw_line in text.splitlines():
        line = _BULLET_PREFIX.sub("", raw_line).strip()
        if not line or line in _SECTION_MARKERS or len(line) < 3:
            continue

        explicit: Optional[str] = None
        for pattern in _EXPLICIT_FOCUS_PATTERNS:
            match = pattern.search(line)
            if match:
                explicit = match.group(1)
                break
        if explicit is not None:
            candidates.append(explicit)
            continue

        candidates.extend(m.group(1) for m in _GENERIC_FOCUS_PATTERN.finditer(line))

    cleaned = [_strip_lead_noise(c.strip().strip("：: 　")) for c in candidates]
    return _dedupe([c for c in cleaned if 2 <= len(c) <= _FOCUS_MAX_LEN],
                   _FOCUS_LIMIT, _FOCUS_MAX_LEN)


def _strip_lead_noise(name: str) -> str:
    """剥离「负责 / 主导 / 熟悉」等前置噪声词。"""
    changed = True
    while changed:
        changed = False
        for noise in _LEAD_NOISE:
            if name.startswith(noise) and len(name) > len(noise) + 1:
                name = name[len(noise):].strip()
                changed = True
    return name


def _priority_topics(
    job_topics: List[str], resume_topics: List[str], difficulty: str
) -> List[str]:
    """优先考察点：junior/mid 缺口优先，senior 交集优先（验证深度）。"""
    resume_keys = {t.lower() for t in resume_topics}
    gaps = [t for t in job_topics if t.lower() not in resume_keys]
    common = [t for t in job_topics if t.lower() in resume_keys]
    ordered = (common + gaps) if difficulty == "senior" else (gaps + common)
    return _dedupe(ordered, _PRIORITY_LIMIT, _TOPIC_MAX_LEN)


def _question_budget(duration: int, difficulty: str) -> int:
    """由时长与难度推导题量预算。"""
    per_question = _MINUTES_PER_QUESTION.get(difficulty, _MINUTES_PER_QUESTION[DEFAULT_DIFFICULTY])
    budget = int(round(duration / per_question))
    return max(_QUESTION_BUDGET_MIN, min(_QUESTION_BUDGET_MAX, budget))


def _validate_plan(plan: Dict[str, Any]) -> None:
    """校验计划的结构不变量。任何一条不成立即视为实现缺陷。"""
    stages = plan.get("stages") or []
    if not stages:
        raise ValueError("InterviewPlan.stages 不能为空")
    weight_sum = sum(int(s["weight"]) for s in stages)
    if weight_sum != 100:
        raise ValueError(f"InterviewPlan 阶段权重合计必须为 100，当前 {weight_sum}")
    question_sum = sum(int(s["target_questions"]) for s in stages)
    if question_sum != int(plan["total_questions"]):
        raise ValueError(
            f"InterviewPlan 阶段题量合计 {question_sum} 与 total_questions "
            f"{plan['total_questions']} 不一致"
        )
    for key in ("target_topics", "priority_topics", "resume_focus_points"):
        if not isinstance(plan.get(key), list):
            raise ValueError(f"InterviewPlan.{key} 必须是数组")


# ============================================================
# 三、规则计划（确定性，兼作 fallback）
# ============================================================
def build_plan(
    *,
    interview_type: str = DEFAULT_INTERVIEW_TYPE,
    difficulty: str = DEFAULT_DIFFICULTY,
    duration: int = DEFAULT_DURATION,
    job: Any = None,
    resume: Any = None,
    total_questions: Optional[int] = None,
) -> Dict[str, Any]:
    """制定面试计划（纯规则、确定性、无副作用）。

    ``job`` / ``resume`` 可以是 ORM 对象、dict，或 ``None``；
    ``resume`` 亦可直接传简历正文（str）。**本函数不查库、不调 LLM、不写库**，
    因此也是 LLM 失败时的 fallback 路径。

    取值非法（如未知 ``interview_type``）时**不抛异常**，而是回落到默认模板——
    fallback 路径必须永不失败。
    """
    itype = interview_type if interview_type in _STAGE_WEIGHTS else DEFAULT_INTERVIEW_TYPE
    diff = difficulty if difficulty in DIFFICULTIES else DEFAULT_DIFFICULTY

    try:
        dur = int(duration)
    except (TypeError, ValueError):
        dur = DEFAULT_DURATION
    dur = max(_DURATION_MIN, min(_DURATION_MAX, dur))

    if total_questions is None:
        total = _question_budget(dur, diff)
    else:
        try:
            total = int(total_questions)
        except (TypeError, ValueError):
            total = _question_budget(dur, diff)
        total = max(1, min(_TOTAL_QUESTIONS_MAX, total))

    weights = _STAGE_WEIGHTS[itype]
    counts = _allocate_questions(total, weights)
    stages = [
        {
            "stage": stage,
            "weight": weights[stage],
            "target_questions": counts[stage],
        }
        for stage in INTERVIEW_STAGES
        if stage in weights
    ]

    job_topics = _job_topics(job)
    resume_topics = _resume_topics(resume)

    plan: Dict[str, Any] = {
        "interview_type": itype,
        "difficulty": diff,
        "duration": dur,
        "total_questions": total,
        "stages": stages,
        "target_topics": _dedupe(job_topics + resume_topics, _TOPIC_LIMIT, _TOPIC_MAX_LEN),
        "priority_topics": _priority_topics(job_topics, resume_topics, diff),
        "resume_focus_points": _extract_focus_points(_resume_text(resume)),
        "source": "rule",
    }
    _validate_plan(plan)
    return plan


# ============================================================
# 四、LLM 增强（可选，失败即回退）
# ============================================================
_LLM_PROMPT_NAME = "planner"  # 对应 prompts/interview/planner.txt


def _build_llm_prompt(plan: Dict[str, Any], job: Any, resume: Any) -> str:
    """拼装 LLM 提示词（只涉及知识点规划，不涉及出题）。

    Prompt 正文存放在 ``prompts/interview/planner.txt``，本函数只负责
    **注入变量**（缺省值也在此处给定，例如无简历时填「（无简历）」）。
    """
    text = _resume_text(resume)
    return render_prompt(
        _LLM_PROMPT_NAME,
        {
            "interview_type_label": _TYPE_LABELS.get(plan["interview_type"], "综合面"),
            "difficulty_label": _DIFFICULTY_LABELS.get(plan["difficulty"], "中级"),
            "duration": plan["duration"],
            "total_questions": plan["total_questions"],
            "job_name": _field(job, "job_name") or "（未指定岗位）",
            "job_skills": _field(job, "skills") or "（未标注）",
            "job_duty": _field(job, "duty") or "（未标注）",
            "resume_summary": text[:_RESUME_EXCERPT_MAX] if text else "（无简历）",
        },
    )


def _is_spark_failure(raw: Any) -> bool:
    """判断 Spark 返回是否为失败哨兵（``SparkAPI`` 从不抛异常，只返回错误串）。"""
    if not isinstance(raw, str):
        return True
    text = raw.strip()
    if not text:
        return True
    return any(text.startswith(s) for s in _SPARK_SENTINELS)


def _parse_llm_plan(raw: Any) -> Optional[Dict[str, Any]]:
    """从模型输出中解析并清洗计划增强项；任何异常/非法结构返回 ``None``。

    JSON 的提取与容错统一交给 ``prompts.loader.extract_json_object``，
    本模块不自行拼正则（Prompt 资源与解析逻辑集中在一处）。
    """
    if _is_spark_failure(raw):
        return None

    try:
        data = extract_json_object(raw)
    except PromptJSONError:
        return None

    target = _dedupe(data.get("target_topics") or [], _TOPIC_LIMIT, _TOPIC_MAX_LEN)
    if not target:
        return None

    # priority 必须是 target 的子集，否则该字段作废（保留规则结果）
    target_keys = {t.lower() for t in target}
    priority = [
        t
        for t in _dedupe(data.get("priority_topics") or [], _PRIORITY_LIMIT, _TOPIC_MAX_LEN)
        if t.lower() in target_keys
    ]

    focus = _dedupe(
        data.get("resume_focus_points") or [], _FOCUS_LIMIT, _FOCUS_MAX_LEN
    )
    return {
        "target_topics": target,
        "priority_topics": priority,
        "resume_focus_points": focus,
    }


def _merge_plan(plan: Dict[str, Any], enriched: Dict[str, Any]) -> Dict[str, Any]:
    """把 LLM 增强项并入规则计划。

    **只替换知识点类字段**，``stages`` / ``weight`` / ``target_questions`` 一律
    保持规则产出，从而保证 LLM 无法破坏计划结构。增强项为空则沿用规则结果。
    """
    merged = dict(plan)
    merged["stages"] = [dict(s) for s in plan["stages"]]

    if enriched.get("target_topics"):
        merged["target_topics"] = list(enriched["target_topics"])
    if enriched.get("priority_topics"):
        merged["priority_topics"] = list(enriched["priority_topics"])
    if enriched.get("resume_focus_points"):
        merged["resume_focus_points"] = list(enriched["resume_focus_points"])
    merged["source"] = "llm"

    _validate_plan(merged)
    return merged


async def refine_plan_with_llm(
    plan: Dict[str, Any],
    *,
    job: Any = None,
    resume: Any = None,
    spark: Any = None,
) -> Dict[str, Any]:
    """用既有 Spark 服务增强计划的**知识点类字段**；失败一律回退到规则计划。

    回退触发条件（全部静默处理，绝不向上抛异常）：
    - 拿不到 Spark 实例
    - 调用抛异常
    - 返回失败哨兵串 / 空串
    - 返回内容不是合法 JSON，或缺少 ``target_topics``
    """
    spark = spark if spark is not None else _default_spark()
    if spark is None:
        return plan

    try:
        raw = await spark.chat_async(_build_llm_prompt(plan, job, resume))
    except Exception:  # noqa: BLE001 - fallback 路径必须吞掉一切异常
        return plan

    enriched = _parse_llm_plan(raw)
    if not enriched:
        return plan

    try:
        return _merge_plan(plan, enriched)
    except ValueError:  # 理论上不会发生；保险起见仍回退
        return plan


def _default_spark() -> Any:
    """延迟获取全局 Spark 实例，避免 services → main 的模块级循环导入。"""
    try:
        from main import spark_api  # noqa: PLC0415 - 刻意延迟导入

        return spark_api
    except Exception:  # noqa: BLE001 - 环境不完整时视为不可用
        return None


# ============================================================
# 五、对外入口（异步，读库）
# ============================================================
async def _load_job(db: AsyncSession, job_id: Optional[int]) -> Optional[Job]:
    if not job_id:
        return None
    result = await db.execute(select(Job).where(Job.id == job_id))
    return result.scalar_one_or_none()


async def _load_resume(db: AsyncSession, resume_id: Optional[int]) -> Optional[Resume]:
    """读取简历。``resume`` 表当前无写入逻辑，取不到即返回 ``None``（不猜）。"""
    if not resume_id:
        return None
    result = await db.execute(select(Resume).where(Resume.id == resume_id))
    return result.scalar_one_or_none()


async def build_plan_for(
    db: AsyncSession,
    *,
    job_id: Optional[int] = None,
    resume_id: Optional[int] = None,
    interview_type: str = DEFAULT_INTERVIEW_TYPE,
    difficulty: str = DEFAULT_DIFFICULTY,
    duration: int = DEFAULT_DURATION,
    total_questions: Optional[int] = None,
    use_llm: bool = False,
    spark: Any = None,
) -> Dict[str, Any]:
    """按 id 载入岗位与简历后制定计划（对外主入口）。

    - 只**读取** ``jobs`` / ``resume``，不写库、不改表结构
    - ``use_llm=True`` 时尝试用 Spark 增强知识点，失败自动回退（``source`` 标记来源）
    - 岗位 / 简历不存在时按「无岗位 / 无简历」处理，不抛 404——
      计划必须总能产出，缺失信息体现在空的 topics / focus_points 上
    """
    job = await _load_job(db, job_id)
    resume = await _load_resume(db, resume_id)

    plan = build_plan(
        interview_type=interview_type,
        difficulty=difficulty,
        duration=duration,
        job=job,
        resume=resume,
        total_questions=total_questions,
    )
    plan["job_id"] = job.id if job is not None else None
    plan["resume_id"] = resume.id if resume is not None else None

    if use_llm:
        plan = await refine_plan_with_llm(plan, job=job, resume=resume, spark=spark)
    return plan
