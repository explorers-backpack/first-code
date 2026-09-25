# -*- coding: utf-8 -*-
"""AI 模拟面试 · InterviewCore（面试流程控制层）。

分层定位（四层）
----------------
::

    api/interview.py              ← HTTP 契约（参数校验 / 响应模型 / 鉴权）
        └── interview_service     ← ① Service：API 流程 / Session 管理 / 前端交互
              └── interview_core  ← ② Core：面试流程控制 / Context / Plan / Agent / Validator
                    ├── interview_context    上下文获取
                    ├── interview_planner    计划获取
                    ├── interview_agent      ③ Agent：LLM 调用 / Prompt 构造 / 候选问题
                    └── question_validator   ④ Validator：问题校验 / 字段标准化

本模块只做两件事
----------------
1. **业务规则（纯函数、确定性、可复现）**：出题计划、作答评分、报告汇总。
   不含 HTTP 契约、不碰数据库、不做序列化——那三件事属于 Service。
2. **编排接缝（全项目唯一一处）**：把 Context / Plan / Agent / Validator
   四个协作者收口到 :ref:`本模块的适配函数 <seam>`，Service 不再直接依赖它们。

零数据库耦合
------------
本模块**不 import ``models`` / ``database``**（仅 ``TYPE_CHECKING`` 下用于类型标注），
也不 import FastAPI 的请求上下文。需要读库的接缝函数由调用方**注入** ``db``，
因此出题计划 / 评分 / 报告可脱离数据库与 HTTP 单独测试。

当前状态（重要，勿误解）
------------------------
现有业务路径**仍是纯规则**，本次重构只搬位置、不换逻辑：

=============  ==========================================
``start``      :func:`build_question_plan`（确定性规则出题）
``answer``     :func:`score_answer`（确定性规则评分）
``end``        :func:`build_report`（确定性规则汇总）
=============  ==========================================

.. _seam:

四个底层编排接缝——:func:`load_context` / :func:`build_interview_plan_for`
/ :func:`generate_candidate_question` / :func:`validate_candidate_question`——
把 Context / Plan / Agent / Validator 收口到一处。

**对外流程入口**：:func:`generate_next_question`（第七节）——「文字面试」与
「数字人面试」共享的**同一个面试核心**：无论前端是文本框还是数字人视频，
出题都走这一条链::

    session → context → plan → KnowledgeRetriever（可选，只取知识）
            → Agent（生成）→ Validator（校验）→ InterviewQuestion

本函数**不写库**（不落题目、不改上下文），持久化与状态推进由调用方负责。

知识检索（RAG 扩展点，**只影响 Agent 出题路径**）
--------------------------------------------------
:func:`retrieve_knowledge` 是本模块对 ``services.knowledge_retriever`` 的**唯一接线点**，
由 :func:`generate_next_question` 在 plan 之后、Agent 之前调用，
把结果作为 ``knowledge_context`` 交给 Agent。

- **默认不接真实知识库**：``retriever`` 缺省时用 ``KnowledgeRetriever()``
  （基类空实现，恒返回 ``[]``）→ 出题结果与引入本能力之前**完全一致**。
  真实检索器由调用方**显式注入**，本模块**不提供全局单例**。
- **只影响 Agent 路径**：规则出题走 :func:`build_question_plan`，**不经过**本函数，
  因此 ``RuleQuestionGenerator`` 结构上不可能触发检索。
- **检索是可选增强，失败绝不中断出题**：检索抛异常时静默回退为 ``[]``
  并在结果 ``warnings`` 里记一条 :data:`WARNING_KNOWLEDGE_FAILED`
  （与 Planner 的 Spark 增强同一套「失败静默回退」约定）。

现有业务路径的边界
------------------
``start_session`` 仍走 :func:`build_question_plan`（纯规则、确定性），
**未**切到 :func:`generate_next_question`——切换会让出题从规则变为依赖大模型，
属于业务效果变更，需显式授权。因此本次**不改变任何现有业务效果**。

分层例外（已知债务，勿在此处"顺手修"）
--------------------------------------
:func:`build_report` 在无作答时直接抛 ``HTTPException(400)``，使本层依赖 FastAPI。
这是重构前既有行为，本次**原样保留**以保证错误码与提示文字不变；
后续应改为领域异常（如 ``InterviewCoreError``），由 Service 映射为 HTTP 状态码。
"""

from __future__ import annotations

import re
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from fastapi import HTTPException

from services.resume_scoring import (
    QUANT_UNIT_AFTER,
    QUANT_UNIT_BEFORE,
    extract_skills,
)

if TYPE_CHECKING:  # 仅类型标注：运行时不导入，保持本层零数据库耦合
    from sqlalchemy.ext.asyncio import AsyncSession

    from models import InterviewAnswer, InterviewQuestion, InterviewSession, Job
    from services.question_validator import ValidationResult

__all__ = [
    # 词表与权重
    "DIFFICULTY_LABELS",
    "INTERVIEW_TYPE_LABELS",
    "ANSWER_DIMENSION_WEIGHTS",
    "REPORT_DIMENSION_WEIGHTS",
    "STRUCTURE_MARKERS",
    "EXAMPLE_MARKERS",
    "TRADEOFF_MARKERS",
    "FILLER_MARKERS",
    # 出题计划（纯规则）
    "plan_question_types",
    "build_question_text",
    "build_question_plan",
    # 作答评分（纯规则）
    "score_answer",
    "build_answer_feedback",
    # 报告汇总（纯规则）
    "job_match_score",
    "build_report",
    # 编排接缝：Context / Plan / Agent / Validator
    "load_context",
    "build_interview_plan",
    "build_interview_plan_for",
    "generate_candidate_question",
    "validate_candidate_question",
    # 知识检索接缝（RAG 扩展点：只服务 Agent 出题路径）
    "current_topic",
    "retrieve_knowledge",
    # 流程入口（文字面试 / 数字人面试共用）
    "generate_next_question",
    "QUESTION_RESULT_FIELDS",
    "ERROR_SESSION_NOT_FOUND",
    "ERROR_SESSION_FINISHED",
    "ERROR_ALL_ANSWERED",
    "ERROR_AGENT_FAILED",
    "ERROR_VALIDATION_FAILED",
    "WARNING_KNOWLEDGE_FAILED",
    # 编排所需的只读加载（供 QuestionGenerator 等上层策略复用）
    "load_session_row",
    "load_job_row",
    "load_resume_row",
]


# ============================================================
# 一、常量与词表
# ============================================================
DIFFICULTY_LABELS = {"junior": "初级", "mid": "中级", "senior": "高级"}
INTERVIEW_TYPE_LABELS = {
    "technical": "技术面",
    "behavioral": "行为面",
    "comprehensive": "综合面",
}

# 题型配比（自我介绍与收尾固定各 1 道，其余按此权重分配）
_TYPE_WEIGHTS: Dict[str, Dict[str, int]] = {
    "comprehensive": {"technical": 5, "project": 2, "behavioral": 2},
    "technical": {"technical": 7, "project": 2, "behavioral": 1},
    "behavioral": {"technical": 1, "project": 3, "behavioral": 6},
}

# 评分维度权重（单题总分）
ANSWER_DIMENSION_WEIGHTS: Tuple[Tuple[str, str, float], ...] = (
    ("technical_score", "技术准确性", 0.35),
    ("logic_score", "逻辑结构", 0.25),
    ("expression_score", "表达清晰度", 0.20),
    ("adaptability_score", "应变与深度", 0.20),
)

# 报告维度权重（全场总分）
REPORT_DIMENSION_WEIGHTS: Tuple[Tuple[str, str, float], ...] = (
    ("technical_score", "技术能力", 0.25),
    ("project_score", "项目经验", 0.15),
    ("logic_score", "逻辑思维", 0.15),
    ("adaptability_score", "应变能力", 0.13),
    ("expression_score", "表达能力", 0.12),
    ("communication_score", "沟通能力", 0.10),
    ("job_match_score", "岗位匹配度", 0.10),
)

# 文本特征词表（用于纯规则评分，均按「出现次数」计权）
STRUCTURE_MARKERS = (
    "首先", "其次", "然后", "接着", "最后", "第一", "第二", "第三",
    "一是", "二是", "三是", "因为", "所以", "因此", "综上", "总之",
    "一方面", "另一方面", "另外", "此外", "总结",
)
EXAMPLE_MARKERS = (
    "例如", "比如", "举个例子", "当时", "我负责", "我主导", "我们团队",
    "我们组", "具体来说", "实际上", "在我", "项目中",
)
TRADEOFF_MARKERS = (
    "权衡", "取舍", "折中", "替代", "备选", "对比", "成本", "收益",
    "利弊", "反而", "但是", "不过", "然而", "代价",
)
FILLER_MARKERS = ("嗯", "那个那个", "就是说", "反正", "这个这个")

# 各维度得分偏低时给出的针对性建议
_DIMENSION_TIPS: Dict[str, str] = {
    "technical_score": "补充该技术点的底层原理与适用边界，说明你在真实场景中的取舍依据",
    "logic_score": "用「结论先行 + 分点展开」组织回答，先给结论再补理由，避免想到哪说到哪",
    "expression_score": "把回答控制在 1-2 分钟、3 个要点以内，减少口头禅，先讲结论再讲细节",
    "adaptability_score": "加入可量化的结果（如 QPS、耗时、覆盖率）和一个备选方案的对比",
}


# ============================================================
# 二、内部工具
# ============================================================
def _clamp(value: float) -> int:
    """把分数收敛到 0-100 的整数。"""
    return int(max(0, min(100, round(value))))


def _split_skills(raw: Optional[str]) -> List[str]:
    """按常见分隔符切分技能字符串（与 jobs.skills 的存储格式一致）。"""
    if not raw:
        return []
    return [t.strip() for t in re.split(r"[,，、/;；|]", raw) if t.strip()]


def _split_sentences(text: str) -> List[str]:
    """切句。用于判断表达是否分点、是否成句。"""
    parts = re.split(r"[。；;!！?？\n]|(?<=[A-Za-z0-9%])\.\s+", text)
    return [p.strip() for p in parts if p.strip()]


def _count_markers(text_lower: str, markers: Sequence[str]) -> int:
    """统计特征词出现次数（一个词多次出现按多次计）。"""
    return sum(text_lower.count(m) for m in markers)


def _count_quantified(text: str) -> int:
    """统计带度量单位的量化指标条数（复用 resume_scoring 的正则）。"""
    return len(QUANT_UNIT_AFTER.findall(text)) + len(QUANT_UNIT_BEFORE.findall(text))


def _length_score(chars: int) -> int:
    """回答长度的合理性评分：过短信息不足，过长则重点不清。"""
    if chars < 10:
        return 10
    if chars < 30:
        return 30
    if chars < 60:
        return 50
    if chars < 120:
        return 70
    if chars < 300:
        return 85
    if chars < 800:
        return 90
    return 78


# ============================================================
# 三、出题计划（纯规则）
# ============================================================
def plan_question_types(total: int, interview_type: str) -> List[str]:
    """把总题量分配到各题型，保证返回列表长度恰好等于 total。"""
    if total <= 1:
        return ["intro"]

    plan: List[str] = ["intro"]
    rest = total - 2  # 预留最后一道 closing
    weights = _TYPE_WEIGHTS.get(interview_type, _TYPE_WEIGHTS["comprehensive"])
    total_weight = sum(weights.values())

    allocated = {k: rest * w // total_weight for k, w in weights.items()}
    remainder = rest - sum(allocated.values())
    for key in sorted(weights, key=lambda k: -weights[k]):
        if remainder <= 0:
            break
        allocated[key] += 1
        remainder -= 1

    for key in ("technical", "project", "behavioral"):
        plan.extend([key] * allocated.get(key, 0))
    plan.append("closing")
    return plan


def build_question_text(
    question_type: str,
    ctx: Dict[str, Any],
) -> Dict[str, Any]:
    """按题型生成题目文本与期望要点。

    ctx 需包含：job_name / skill / difficulty / variant
    返回 ``{question, topic, expected_points}``

    扩展点：接入星火大模型后，本函数改为「组装 prompt + 解析结构化输出」即可，
    调用方无需改动；届时也可改由 :func:`generate_candidate_question` 走 Agent。
    """
    job_name = ctx.get("job_name") or "目标岗位"
    skill = ctx.get("skill")
    difficulty = ctx.get("difficulty", "mid")
    variant = ctx.get("variant", 0)
    label = DIFFICULTY_LABELS.get(difficulty, "中级")

    if question_type == "intro":
        templates = (
            f"请用 2-3 分钟做一个自我介绍，重点说明你与「{job_name}」这个岗位的匹配点。",
            f"我们先从自我介绍开始。请简述你的技术背景，以及你认为自己适合「{job_name}」的原因。",
        )
        return {
            "question": templates[variant % len(templates)],
            "topic": "自我介绍",
            "expected_points": ["项目", "技能", "经验", "岗位"],
        }

    if question_type == "technical":
        if not skill:
            return {
                "question": "请介绍一个你解决过的最有技术挑战的问题：当时的背景、你的方案，以及最终效果。",
                "topic": "技术问题解决",
                "expected_points": ["背景", "方案", "结果"],
            }
        templates = {
            "junior": (
                f"你在简历中提到掌握 {skill}。请说明它主要解决什么问题，并给出一个你实际使用过的场景。",
                f"请介绍 {skill} 的基本原理，以及你在项目中是如何使用它的。",
            ),
            "mid": (
                f"你在简历中提到掌握 {skill}。请结合一个具体项目，说明你如何使用 {skill}，"
                f"以及当时遇到的最大问题是如何定位并解决的。",
                f"围绕 {skill}，请谈谈你的实践经验：典型使用场景、你踩过的坑，以及最终如何解决。",
            ),
            "senior": (
                f"请从架构层面谈谈 {skill}：在什么规模下需要引入它，引入后会带来哪些新的复杂度，你如何权衡取舍？",
                f"假设要为一个高并发系统选型 {skill}，你会从哪些维度评估？请给出判断依据和一个反例。",
            ),
        }
        pool = templates.get(difficulty, templates["mid"])
        return {
            "question": pool[variant % len(pool)],
            "topic": skill,
            "expected_points": [skill, "场景", "问题", "解决"],
        }

    if question_type == "project":
        templates = (
            f"请挑一个你最有代表性的项目：说明你的具体职责、最大的技术难点，"
            f"以及最终可量化的结果。结合「{label}」水平，我们希望听到细节而非概括。",
            "请介绍一个你深度参与的项目，重点讲清楚「你做了什么」和「带来了什么可衡量的变化」。",
        )
        return {
            "question": templates[variant % len(templates)],
            "topic": "项目经历",
            "expected_points": ["职责", "难点", "结果", "优化"],
        }

    if question_type == "behavioral":
        templates = (
            "请描述一次你在团队协作中与他人产生技术分歧的经历：当时的背景、你的处理方式，以及最终结果。",
            "请讲一次你在时间紧、资源不足的情况下推进项目的经历：你是如何取舍和协调的？",
            "请描述一次你负责的事情出了问题的经历：你如何定位原因、如何补救、之后做了哪些改进？",
        )
        return {
            "question": templates[variant % len(templates)],
            "topic": "行为面试",
            "expected_points": ["背景", "处理", "结果", "改进"],
        }

    # closing
    templates = (
        "我的问题问完了。你有什么想了解的吗？可以谈谈你对这个岗位最关心的一点。",
        "面试到这里结束。请用一句话总结你认为自己最大的优势，以及你希望在这个岗位上获得什么。",
    )
    return {
        "question": templates[variant % len(templates)],
        "topic": "收尾",
        "expected_points": ["岗位", "团队", "业务"],
    }


def build_question_plan(
    session: InterviewSession,
    job: Optional[Job],
    resume_skills: List[str],
) -> List[Dict[str, Any]]:
    """生成整场面试的题目计划（确定性：同输入同输出）。

    这是**当前** ``start`` 使用的出题路径（纯规则）。
    ``session`` / ``job`` 只按属性读取（``total_questions`` / ``interview_type``
    / ``difficulty`` / ``skills``），因此也可传同形状的普通对象。
    """
    job_skills = _split_skills(job.skills) if job else []

    # 出题素材：岗位技能优先，其次简历技能；去重后作为技术题主题
    topics: List[str] = []
    for item in list(job_skills) + list(resume_skills):
        if item and item not in topics:
            topics.append(item)

    job_name = job.job_name if job else None
    types = plan_question_types(session.total_questions, session.interview_type)

    plan: List[Dict[str, Any]] = []
    counters: Dict[str, int] = {}
    topic_cursor = 0
    for index, qtype in enumerate(types, start=1):
        variant = counters.get(qtype, 0)
        counters[qtype] = variant + 1

        ctx: Dict[str, Any] = {
            "job_name": job_name,
            "difficulty": session.difficulty,
            "variant": variant,
        }
        if qtype == "technical" and topics:
            ctx["skill"] = topics[topic_cursor % len(topics)]
            topic_cursor += 1

        built = build_question_text(qtype, ctx)
        plan.append(
            {
                "question_no": index,
                "question": built["question"],
                "question_type": qtype,
                "topic": built["topic"],
                "difficulty": session.difficulty,
                "expected_points": built["expected_points"],
            }
        )
    return plan


# ============================================================
# 四、作答评分（纯规则）
# ============================================================
def score_answer(question: Dict[str, Any], answer_text: str) -> Dict[str, Any]:
    """对单道题的回答做多维度评分。

    返回 ``{score, technical_score, logic_score, expression_score,
    adaptability_score, feedback}``。

    扩展点：接入星火大模型后，可在此函数中「用模型输出替换文字型 feedback」，
    但分数仍建议保留规则口径，以维持可复现与可申诉。
    """
    text = (answer_text or "").strip()
    lower = text.lower()
    chars = len(text)
    sentences = _split_sentences(text)

    skills_in_answer = extract_skills(text)
    expected = [str(p) for p in (question.get("expected_points") or [])]
    hit_points = [p for p in expected if p.lower() in lower]
    point_ratio = (len(hit_points) / len(expected)) if expected else 0.0

    quant_hits = _count_quantified(text)
    structure_hits = _count_markers(lower, STRUCTURE_MARKERS)
    example_hits = _count_markers(lower, EXAMPLE_MARKERS)
    tradeoff_hits = _count_markers(lower, TRADEOFF_MARKERS)
    filler_hits = _count_markers(lower, FILLER_MARKERS)

    # ---- 技术准确性：技术名词密度 + 期望要点覆盖 + 量化佐证 ----
    technical = 20.0
    technical += min(30.0, len(skills_in_answer) * 6)
    technical += point_ratio * 30.0
    technical += min(15.0, quant_hits * 7.0)

    # ---- 逻辑结构：分点/因果连接词 + 成句数量 + 举例 ----
    logic = 30.0
    logic += min(35.0, structure_hits * 10.0)
    logic += min(20.0, max(0, len(sentences) - 2) * 7.0)
    logic += min(10.0, example_hits * 5.0)

    # ---- 表达清晰度：长度合理性 + 成句数量 - 口头禅 ----
    expression = float(_length_score(chars))
    expression += min(15.0, len(sentences) * 3.0)
    expression -= min(20.0, filler_hits * 7.0)

    # ---- 应变与深度：具体案例 + 方案权衡 + 量化结果 ----
    adaptability = 30.0
    adaptability += min(25.0, example_hits * 8.0)
    adaptability += min(20.0, tradeoff_hits * 7.0)
    adaptability += min(20.0, quant_hits * 10.0)

    raw = {
        "technical_score": _clamp(technical),
        "logic_score": _clamp(logic),
        "expression_score": _clamp(expression),
        "adaptability_score": _clamp(adaptability),
    }
    total = _clamp(
        sum(raw[key] * weight for key, _, weight in ANSWER_DIMENSION_WEIGHTS)
    )

    feedback = build_answer_feedback(raw, total, hit_points, skills_in_answer, chars)

    return {"score": total, **raw, "feedback": feedback}


def build_answer_feedback(
    raw: Dict[str, int],
    total: int,
    hit_points: List[str],
    skills_in_answer: List[str],
    chars: int,
) -> str:
    """把规则评分整理成可读、可取证的文字反馈（不含 LLM）。"""
    strengths = [
        name for key, name, _ in ANSWER_DIMENSION_WEIGHTS if raw[key] >= 75
    ]
    weaknesses = [
        key for key, _, _ in ANSWER_DIMENSION_WEIGHTS if raw[key] < 60
    ]

    lines = [f"【本题得分】{total} 分（回答 {chars} 字）"]
    lines.append(
        "【亮点】" + ("、".join(strengths) + "表现较好。" if strengths else "暂无明显突出维度。")
    )
    if weaknesses:
        weak_names = [name for key, name, _ in ANSWER_DIMENSION_WEIGHTS if key in weaknesses]
        lines.append("【不足】" + "、".join(weak_names) + "偏弱。")
    else:
        lines.append("【不足】各维度均在合格线以上。")

    # 取证：命中要点与技术名词，均来自候选人原话
    evidence_bits = []
    if hit_points:
        evidence_bits.append("命中要点：" + "、".join(hit_points))
    if skills_in_answer:
        evidence_bits.append("识别到技术名词：" + "、".join(skills_in_answer[:6]))
    lines.append(
        "【取证】" + ("；".join(evidence_bits) if evidence_bits else "未识别到可取证要点。")
    )

    # 建议：取最低分维度给出针对性动作
    if weaknesses:
        lowest = min(weaknesses, key=lambda k: raw[k])
        lines.append("【建议】" + _DIMENSION_TIPS.get(lowest, "补充更多具体细节。"))
    else:
        lines.append("【建议】保持当前表达结构，进一步补充量化结果以增强说服力。")

    return "\n".join(lines)


# ============================================================
# 五、报告汇总（纯规则）
# ============================================================
def job_match_score(
    job: Optional[Job],
    answer_texts: List[str],
    resume_skills: List[str],
) -> Optional[int]:
    """岗位匹配度：候选人（简历技能 + 回答中提到的技能）对岗位技能的覆盖率。

    岗位无技能要求或未指定岗位时返回 ``None``（不猜），由汇总逻辑重新归一化权重。
    """
    if job is None:
        return None
    required = _split_skills(job.skills)
    if not required:
        return None

    have = {s.lower() for s in resume_skills}
    for text in answer_texts:
        have.update(s.lower() for s in extract_skills(text))

    hit = sum(1 for r in required if r.lower() in have)
    return _clamp(hit / len(required) * 100)


def build_report(
    session: InterviewSession,
    questions: List[InterviewQuestion],
    answers: List[InterviewAnswer],
    job: Optional[Job],
    resume_skills: List[str],
) -> Dict[str, Any]:
    """汇总一场面试的报告。纯规则、确定性。

    .. warning::
       本函数直接抛 ``HTTPException``（沿用重构前行为，见模块文档「分层例外」）。
       业务规则不应依赖 HTTP 状态码，后续应改抛领域异常由 Service 映射。
    """
    if not answers:
        raise HTTPException(
            status_code=400,
            detail="尚未提交任何回答，无法生成面试报告",
        )

    def _avg(field: str) -> int:
        values = [getattr(a, field) for a in answers if getattr(a, field) is not None]
        return _clamp(sum(values) / len(values)) if values else 0

    technical = _avg("technical_score")
    logic = _avg("logic_score")
    expression = _avg("expression_score")
    adaptability = _avg("adaptability_score")

    # 项目经验：仅取项目类题目的技术分；无项目题则退回整体技术分
    project_type_ids = {
        q.id for q in questions if q.question_type == "project"
    }
    project_values = [
        a.technical_score
        for a in answers
        if a.question_id in project_type_ids and a.technical_score is not None
    ]
    project = _clamp(sum(project_values) / len(project_values)) if project_values else technical

    # 沟通能力：表达与应变的合成
    communication = _clamp(expression * 0.6 + adaptability * 0.4)

    answer_texts = [a.answer_text or "" for a in answers]
    job_match = job_match_score(job, answer_texts, resume_skills)

    dimension_values: Dict[str, Optional[int]] = {
        "technical_score": technical,
        "project_score": project,
        "logic_score": logic,
        "adaptability_score": adaptability,
        "expression_score": expression,
        "communication_score": communication,
        "job_match_score": job_match,
    }

    # 加权总分：缺失维度（如未指定岗位导致 job_match 为空）自动重新归一化
    available = [
        (key, name, weight)
        for key, name, weight in REPORT_DIMENSION_WEIGHTS
        if dimension_values.get(key) is not None
    ]
    weight_sum = sum(w for _, _, w in available) or 1.0
    total = _clamp(
        sum(dimension_values[key] * w for key, _, w in available) / weight_sum
    )

    # 亮点 / 不足：按维度得分分档
    strengths: List[str] = []
    weaknesses: List[str] = []
    for key, name, _ in REPORT_DIMENSION_WEIGHTS:
        value = dimension_values.get(key)
        if value is None:
            continue
        if value >= 75:
            strengths.append(f"{name}（{value} 分）表现突出")
        elif value < 60:
            weaknesses.append(f"{name}（{value} 分）有待提升")
    if not strengths:
        strengths.append("已完成全部作答，具备基本的表达与答题意识")
    if not weaknesses:
        weaknesses.append("各维度均在合格线以上，建议继续强化深度与量化表达")

    # 建议：取最低的两个维度
    ranked: List[Tuple[str, str, int]] = []
    for key, name, _ in REPORT_DIMENSION_WEIGHTS:
        value = dimension_values.get(key)
        if value is not None:
            ranked.append((key, name, value))
    ranked.sort(key=lambda item: item[2])

    suggestion_lines = []
    for key, name, value in ranked[:2]:
        tip = _DIMENSION_TIPS.get(key)
        if tip is None:
            if key == "project_score":
                tip = "补充项目中的量化结果与个人贡献边界，突出「你做了什么」而非「团队做了什么」"
            elif key == "communication_score":
                tip = "回答时先回应对方关切再展开，注意控制节奏与重点"
            elif key == "job_match_score":
                tip = "围绕目标岗位的职责描述组织回答，主动关联岗位所需的技能与业务场景"
            else:
                tip = "补充更多具体细节"
        suggestion_lines.append(f"{name}（{value} 分）：{tip}")

    answered_rounds = len(answers)
    suggestions = (
        f"本场共作答 {answered_rounds}/{session.total_questions} 题。"
        "优先改进以下两项：\n" + "\n".join(f"{i}. {s}" for i, s in enumerate(suggestion_lines, 1))
    )

    return {
        "session_id": session.id,
        "total_score": total,
        "technical_score": technical,
        "project_score": project,
        "logic_score": logic,
        "expression_score": expression,
        "communication_score": communication,
        "adaptability_score": adaptability,
        "job_match_score": job_match,
        "strengths": strengths,
        "weaknesses": weaknesses,
        "suggestions": suggestions,
    }


# ============================================================
# 六、编排接缝：Context / Plan / Agent / Validator
# ============================================================
# 说明
# ----
# 以下五个函数是「面试流程控制」与四个协作者之间的**唯一接缝**。
# 全部为**纯转发**（不含业务判断），因此：
#   1. 依赖方向单一：Service → Core → (Context | Planner | Agent | Validator)；
#   2. 各协作者可独立替换 / Mock（测试只注入 Mock，不触真实 LLM）；
#   3. 未来接数字人视频面试时，只需在此处调整编排，Service 无需改动。
#
# 延迟导入是刻意的：让「纯规则路径」（出题计划 / 评分 / 报告）不被迫携带
# Agent 与 Prompt 依赖；同时避免 services 内部出现模块级循环导入。
async def load_context(db: AsyncSession, session_id: int) -> Any:
    """获取面试上下文（委托 ``interview_context.get_context``）。

    ``db`` 由调用方注入（本层不建连接、不读配置）。
    """
    from services.interview_context import get_context

    return await get_context(db, session_id)


def build_interview_plan(
    *,
    interview_type: Optional[str] = None,
    difficulty: Optional[str] = None,
    duration: Optional[int] = None,
    job: Any = None,
    resume: Any = None,
    total_questions: Optional[int] = None,
) -> Dict[str, Any]:
    """制定面试计划（委托 ``interview_planner.build_plan``，纯规则、不读库）。

    ``None`` 表示「沿用 Planner 自身的默认值」——本函数**不复制**一份默认值，
    避免两处默认值日后漂移。
    """
    from services.interview_planner import build_plan

    optional: Dict[str, Any] = {}
    if interview_type is not None:
        optional["interview_type"] = interview_type
    if difficulty is not None:
        optional["difficulty"] = difficulty
    if duration is not None:
        optional["duration"] = duration
    if total_questions is not None:
        optional["total_questions"] = total_questions

    return build_plan(job=job, resume=resume, **optional)


async def build_interview_plan_for(
    db: AsyncSession,
    *,
    job_id: Optional[int] = None,
    resume_id: Optional[int] = None,
    interview_type: Optional[str] = None,
    difficulty: Optional[str] = None,
    duration: Optional[int] = None,
    total_questions: Optional[int] = None,
    use_llm: bool = False,
    spark: Any = None,
) -> Dict[str, Any]:
    """按 id 载入岗位与简历后制定计划（委托 ``interview_planner.build_plan_for``）。

    只读 ``jobs`` / ``resume``；``use_llm=True`` 时由 Planner 自行尝试 Spark 增强
    并在失败时静默回退（``source`` 字段标记来源），本层不做额外判断。
    """
    from services.interview_planner import build_plan_for

    optional: Dict[str, Any] = {}
    if interview_type is not None:
        optional["interview_type"] = interview_type
    if difficulty is not None:
        optional["difficulty"] = difficulty
    if duration is not None:
        optional["duration"] = duration
    if total_questions is not None:
        optional["total_questions"] = total_questions

    return await build_plan_for(
        db,
        job_id=job_id,
        resume_id=resume_id,
        use_llm=use_llm,
        spark=spark,
        **optional,
    )


async def generate_candidate_question(
    context: Any,
    plan: Any = None,
    resume: Any = None,
    job: Any = None,
    *,
    knowledge_context: Any = None,
    spark: Any = None,
) -> Dict[str, Any]:
    """调用 Agent 生成**一道候选问题**（委托 ``interview_agent.generate_question``）。

    ``knowledge_context`` 为**可选的外部知识**（由 :func:`retrieve_knowledge` 产出，
    也可由调用方直接注入）；缺省 ``None`` 时 Agent 走原模板，行为与引入知识能力前
    **逐字节相同**。``spark`` 缺省时由 Agent 自行取 ``main.spark_api``；
    测试一律注入 Mock。返回结构与 :func:`validate_candidate_question` 的输入对齐。
    """
    from services.interview_agent import generate_question

    return await generate_question(
        context,
        plan,
        resume,
        job,
        knowledge_context=knowledge_context,
        spark=spark,
    )


def validate_candidate_question(
    question_data: Any,
    context: Any = None,
    plan: Any = None,
) -> ValidationResult:
    """调用 Validator 校验候选问题（委托 ``question_validator.QuestionValidator``）。

    返回 ``ValidationResult``（``valid`` / ``errors`` / ``warnings`` /
    ``normalized_question``）。本层不追加规则，避免与 Validator 出现两套权威。
    """
    from services.question_validator import QuestionValidator

    return QuestionValidator().validate(question_data, context, plan)


# ============================================================
# 六·五、知识检索接缝（RAG 扩展点，**只服务 Agent 出题路径**）
# ============================================================
# 为什么放在 Core 而不是 Agent 里：
#   * Agent 的契约是「只出题、不检索」——它只**接收** knowledge_context；
#   * 「要不要检索、检索什么 topic」属于**流程编排**，与「取 context / 取 plan」
#     同级，因此归 Core；
#   * 规则出题走 build_question_plan，根本不经过本函数 → 天然互不影响。
def _as_text_list(value: Any) -> List[str]:
    """把可能是 list/tuple/set 的字段归一为非空字符串列表（其它形状 → ``[]``）。"""
    if isinstance(value, (list, tuple, set, frozenset)):
        return [str(item) for item in value if str(item).strip()]
    return []


def current_topic(plan: Any = None, context: Any = None) -> str:
    """推导「当前待考察知识点」——纯函数、确定性、不改入参。

    检索器需要知道「为哪个知识点取知识」，而流程里并没有一个现成的 ``topic``
    字段（知识点是 Agent 出题时才定下的）。因此这里按固定优先级推导：

    1. ``plan.priority_topics`` 中**尚未出现在 ``context.covered_topics``** 的第一项
       （最该补强的缺口）
    2. ``plan.priority_topics`` 首项（都覆盖过了就沿用优先级最高的）
    3. ``plan.target_topics`` 首项
    4. ``context.current_stage``（兜底：至少有阶段语义）
    5. ``""``

    ``plan`` / ``context`` 既可以是 dict（``get_context`` / Planner 的产出），
    也可以是带属性的对象。
    """
    priority = _as_text_list(_read_context_field(plan, "priority_topics"))
    target = _as_text_list(_read_context_field(plan, "target_topics"))
    covered = set(_as_text_list(_read_context_field(context, "covered_topics")))

    for name in priority:
        if name not in covered:
            return name
    if priority:
        return priority[0]
    if target:
        return target[0]
    return str(_read_context_field(context, "current_stage") or "")


async def retrieve_knowledge(
    job: Any = None,
    topic: Any = "",
    context: Any = None,
    *,
    retriever: Any = None,
) -> List[Any]:
    """调用知识检索器取知识片段（委托 ``services.knowledge_retriever``）。

    本函数是**全项目对 ``knowledge_retriever`` 的唯一接线点**。

    ``retriever`` 缺省（``None``）时用 ``KnowledgeRetriever()``——**基类空实现，
    恒返回 ``[]``**，即「默认没有任何知识」：不注入就不会悄悄接上真实知识库，
    出题行为与引入本能力之前完全一致。真实检索器由调用方**显式注入**。

    **本函数不做异常兜底**——「检索失败怎么办」是流程决策，
    由 :func:`generate_next_question` 决定（见 :data:`WARNING_KNOWLEDGE_FAILED`）。
    """
    if retriever is None:
        from services.knowledge_retriever import KnowledgeRetriever

        retriever = KnowledgeRetriever()

    return await retriever.retrieve(job, topic, context)


async def _gather_knowledge(
    job: Any,
    plan: Any,
    context: Any,
    retriever: Any,
) -> Tuple[List[Any], List[str]]:
    """取知识 → ``(knowledge_context, warnings)``。

    检索是**可选增强**：任何异常都静默回退为「无知识」并记一条
    :data:`WARNING_KNOWLEDGE_FAILED`，**绝不让出题中断**。
    这与 Planner 的 Spark 增强同一套约定（失败降级、不伪造、不阻塞）。
    """
    try:
        chunks = await retrieve_knowledge(
            job, current_topic(plan, context), context, retriever=retriever
        )
    except Exception:  # noqa: BLE001 - 可选增强，任何故障都降级为「无知识」
        return [], [WARNING_KNOWLEDGE_FAILED]
    return list(chunks or []), []


# ============================================================
# 七、面试流程控制：生成下一道问题
# ============================================================
# 这是「文字面试」与「数字人面试」共享的**同一个面试核心**——
# 前端是文本框还是数字人视频，出题都走这一条链：
#
#     session → context → plan → Agent（只生成）→ Validator（只校验）→ InterviewQuestion
#
# 职责边界：
#   * Core   —— 流程：取会话 / 取上下文 / 取计划 / 调 Agent / 调 Validator / 汇总结果
#   * Agent  —— 只生成（LLM 调用 + Prompt + JSON 解析）
#   * Validator —— 只校验（字段校验 + 标准化）
# Core **不重复**二者的规则，只做编排与闸门。
ERROR_SESSION_NOT_FOUND = "session_not_found"
ERROR_SESSION_FINISHED = "session_finished"
ERROR_ALL_ANSWERED = "all_answered"
ERROR_AGENT_FAILED = "agent_failed"
ERROR_VALIDATION_FAILED = "validation_failed"

#: 知识检索失败时的**稳定 warning 码**（进结果的 ``warnings``，不是 ``errors``）。
#: 检索是**可选增强**：失败只降级为「无知识」，绝不让出题失败。
WARNING_KNOWLEDGE_FAILED = "knowledge_retrieval_failed"

# 结果字段集（**恒定**：成功与失败同形状，调用方无需分支取值）
QUESTION_RESULT_FIELDS: Tuple[str, ...] = (
    "ok",
    "question_no",
    "question",
    "question_type",
    "topic",
    "difficulty",
    "expected_points",
    "reason",
    "stage",
    "warnings",
    "errors",
    "error",
)


def _question_result(**overrides: Any) -> Dict[str, Any]:
    """构造恒定字段集的结果字典。"""
    result: Dict[str, Any] = {
        "ok": False,
        "question_no": None,
        "question": "",
        "question_type": "",
        "topic": "",
        "difficulty": "",
        "expected_points": [],
        "reason": "",
        "stage": "",
        "warnings": [],
        "errors": [],
        "error": None,
    }
    result.update(overrides)
    return result


def _question_fail(code: str, message: str, **extra: Any) -> Dict[str, Any]:
    """失败结果：``errors`` 放**稳定错误码**，``error`` 放可读说明。

    ``extra`` 可覆盖默认值（例如 ``errors`` 追加 Validator 的原始错误码）。
    """
    payload: Dict[str, Any] = {"ok": False, "errors": [code], "error": message}
    payload.update(extra)
    return _question_result(**payload)


def _read_context_field(context: Any, name: str) -> Any:
    """从 Context 读字段：兼容 dict（``get_context`` 的产出）与 ORM 对象。"""
    if context is None:
        return None
    if isinstance(context, Mapping):
        return context.get(name)
    return getattr(context, name, None)


async def load_session_row(db: Any, session_id: int) -> Any:
    """读取会话行（**只读**，编排所需）。

    Core 只做只读加载：拿到 ``job_id`` / ``resume_id`` / ``difficulty`` /
    ``interview_type`` / ``total_questions`` / ``current_question_no`` / ``status``。
    **写入、序列化、归属校验仍在 Service**（Core 不 ``commit``、不返回前端结构）。
    """
    from sqlalchemy import select

    from models import InterviewSession

    result = await db.execute(
        select(InterviewSession).where(InterviewSession.id == session_id)
    )
    return result.scalar_one_or_none()


async def load_job_row(db: Any, job_id: Optional[int]) -> Any:
    """读取岗位行（只读；无 id 返回 ``None``，不猜）。"""
    if not job_id:
        return None
    from sqlalchemy import select

    from models import Job

    result = await db.execute(select(Job).where(Job.id == job_id))
    return result.scalar_one_or_none()


async def load_resume_row(db: Any, resume_id: Optional[int]) -> Any:
    """读取简历行（只读；无 id 返回 ``None``，不猜）。"""
    if not resume_id:
        return None
    from sqlalchemy import select

    from models import Resume

    result = await db.execute(select(Resume).where(Resume.id == resume_id))
    return result.scalar_one_or_none()


async def _ensure_context(db: Any, session_id: int) -> Dict[str, Any]:
    """确保会话上下文存在并返回它。

    ``interview_context.create_context`` 是**幂等**的（已存在则不覆盖），
    因此这里可以安全调用——这是「Context 获取」职责的一部分，
    避免调用方必须先手工建上下文。
    """
    from services.interview_context import create_context, get_context

    await create_context(db, session_id)
    return await get_context(db, session_id)


async def _build_session_plan(db: Any, session: Any) -> Dict[str, Any]:
    """按会话配置制定本场计划（委托 Planner）。

    刻意 ``use_llm=False``：计划走**确定性规则**，把每道题**唯一一次**大模型调用
    留给 Agent，避免同一道题打两次 LLM（也让 plan 可复现）。
    """
    from services.interview_planner import build_plan_for

    return await build_plan_for(
        db,
        job_id=session.job_id,
        resume_id=session.resume_id,
        interview_type=session.interview_type,
        difficulty=session.difficulty,
        duration=session.duration,
        total_questions=session.total_questions,
        use_llm=False,
    )


async def generate_next_question(
    db: Any,
    session_id: int,
    *,
    user_id: Optional[int] = None,
    spark: Any = None,
    context: Any = None,
    plan: Any = None,
    retriever: Any = None,
) -> Dict[str, Any]:
    """**生成下一道面试题**（文字面试 / 数字人面试共用入口）。

    参数
    ----
    - ``db``：数据库会话，由调用方注入（本层不建连接、不读配置）。
    - ``session_id``：面试会话 id（业务主键）。
    - ``user_id``：可选。传入则校验会话归属，非本人按「不存在」处理（不泄露他人会话）。
    - ``spark``：可选，注入的 Spark 客户端；测试一律注入 Mock，不调真实 API。
    - ``context`` / ``plan``：可选**注入**。传入则跳过对应的读库/建上下文步骤，
      便于脱离 DB 单测；不传（默认）时按正常流程加载。
    - ``retriever``：可选，注入的知识检索器。**缺省即不接真实知识库**
      （用 ``KnowledgeRetriever()`` 空实现 → 无知识），出题行为与引入知识能力前
      完全一致。真实检索器（向量库 / 手册 / 外部服务）由调用方显式注入。

    流程
    ----
    1. **session**：取会话行 → 存在性 / 归属 / 终态 / 是否已答完 四道闸门
    2. **context**：确保上下文存在并读取（幂等创建；注入 ``context`` 则跳过）
    3. **plan**：按会话配置制定计划（确定性规则，不调 LLM；注入 ``plan`` 则跳过）
    4. **KnowledgeRetriever**：按「岗位 + 当前 topic + 上下文」取知识
       （可选增强，失败静默降级为「无知识」）
    5. **Agent**：``generate_candidate_question`` 生成**一道候选问题**
       （携带上一步的知识上下文）
    6. **Validator**：``validate_candidate_question`` 校验并标准化
    7. **InterviewQuestion**：返回规范化后的题目字段

    返回
    ----
    字段集恒定，见 :data:`QUESTION_RESULT_FIELDS`：``ok`` / ``question_no`` /
    ``question`` / ``question_type`` / ``topic`` / ``difficulty`` /
    ``expected_points`` / ``reason`` / ``stage`` / ``warnings`` / ``errors`` /
    ``error``。

    失败时 ``ok=False``、``question`` 恒为 ``""``（**绝不放行未校验的问题**），
    ``errors`` 为稳定错误码：``session_not_found`` / ``session_finished`` /
    ``all_answered`` / ``agent_failed`` / ``validation_failed``。
    知识检索失败**不进 ``errors``**（不是失败），只在 ``warnings`` 里记
    :data:`WARNING_KNOWLEDGE_FAILED`。

    不写库
    ------
    本函数**不落库**（除幂等创建上下文外）：题目持久化、上下文更新、
    题号推进均由调用方负责。因此同一会话重复调用是安全的。
    """
    # ---- 1. session：四道闸门 ----
    session = await load_session_row(db, session_id)
    if session is None or (user_id is not None and session.user_id != user_id):
        return _question_fail(
            ERROR_SESSION_NOT_FOUND, f"面试会话 {session_id} 不存在"
        )

    from models import SESSION_STATUS_FINISHED

    if session.status == SESSION_STATUS_FINISHED:
        return _question_fail(
            ERROR_SESSION_FINISHED, "面试已结束，无法再生成问题"
        )

    # 闸门以**状态机题号**为准（权威）：1..N = 当前待作答题号，N+1 = 全部答完。
    # 刻意不用 context 的 max_reached——它语义是「已提问数量是否达到上限」，
    # 在第 N 题尚未作答时就会判 True，用作出题闸门会少出一道题。
    if session.current_question_no > session.total_questions:
        return _question_fail(
            ERROR_ALL_ANSWERED, "全部题目已作答，请结束面试并生成报告"
        )

    question_no = session.current_question_no if session.current_question_no > 0 else 1

    # ---- 2. context（可注入，跳过读库与幂等创建）----
    if context is None:
        context = await _ensure_context(db, session_id)
    stage = _read_context_field(context, "current_stage") or ""

    # ---- 3. plan（可注入，跳过读库）----
    if plan is None:
        plan = await _build_session_plan(db, session)

    # ---- 3.5 KnowledgeRetriever（只检索；缺省即空实现 → 无知识）----
    resume = await load_resume_row(db, session.resume_id)
    job = await load_job_row(db, session.job_id)
    knowledge_context, knowledge_warnings = await _gather_knowledge(
        job, plan, context, retriever
    )

    # ---- 4. Agent（只生成；携带可选知识）----
    candidate = await generate_candidate_question(
        context,
        plan,
        resume,
        job,
        knowledge_context=knowledge_context,
        spark=spark,
    )

    if not candidate.get("ok"):
        return _question_fail(
            ERROR_AGENT_FAILED,
            candidate.get("error") or "生成问题失败",
            question_no=question_no,
            stage=stage,
            warnings=list(knowledge_warnings),
        )

    # ---- 5. Validator（只校验 + 标准化）----
    validation = validate_candidate_question(candidate, context, plan)
    if not validation.valid:
        return _question_fail(
            ERROR_VALIDATION_FAILED,
            "问题校验未通过：" + "、".join(validation.errors),
            question_no=question_no,
            stage=stage,
            errors=[ERROR_VALIDATION_FAILED, *validation.errors],
            warnings=list(knowledge_warnings),
        )

    # ---- 6. InterviewQuestion ----
    normalized = validation.normalized_question
    return _question_result(
        ok=True,
        question_no=question_no,
        question=normalized.get("question", ""),
        question_type=normalized.get("question_type", ""),
        topic=normalized.get("topic", ""),
        difficulty=normalized.get("difficulty", ""),
        expected_points=list(normalized.get("expected_points") or []),
        reason=normalized.get("reason", ""),
        stage=stage,
        warnings=[*knowledge_warnings, *validation.warnings],
    )
