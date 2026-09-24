# -*- coding: utf-8 -*-
"""AI 模拟面试业务逻辑（Interview Service）。

设计原则（沿用 ``services/resume_scoring.py`` 的既定口径）
--------------------------------------------------------
1. **纯规则、无随机、可复现**：同一份输入任意次执行，题目与分数完全一致。
   不依赖大模型，避免不可复现的评分。
2. **证据优先**：每道题的评分都能回指候选人原话（``feedback`` 中给出命中要点
   与识别到的技术名词），不编造、不填充默认值。
3. **不重复造轮子**：技能识别与量化指标识别直接复用
   ``services.resume_scoring`` 的既有实现（``extract_skills`` /
   ``QUANT_UNIT_AFTER`` / ``QUANT_UNIT_BEFORE``），不另建一套词表。
4. **LLM 可插拔**：本阶段为纯规则实现（按要求不接数字人 / ASR / TTS / RAG）。
   后续接入星火大模型时，只需替换 ``_build_question_text`` 与 ``score_answer``
   两个函数的实现，会话生命周期与落库逻辑无需改动。

对外约定
--------
所有函数以 ``db: AsyncSession`` 为第一参数（依赖注入），返回 **普通 dict**，
不依赖 FastAPI —— 因此可以脱离 HTTP 层单独测试（见
``tests/test_interview_lifecycle.py``）。

生命周期状态机
--------------
``created`` --start--> ``ongoing`` --end--> ``finished``

``current_question_no`` 语义：0 = 未开始；1..N = 当前待作答题号；
N+1 = 全部题目已作答（等待调用 ``end``）。
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import (
    DIFFICULTIES,
    INTERVIEW_TYPES,
    SESSION_STATUS_CREATED,
    SESSION_STATUS_FINISHED,
    SESSION_STATUS_ONGOING,
    InterviewAnswer,
    InterviewQuestion,
    InterviewReport,
    InterviewSession,
    Job,
    Resume,
)
from schemas.interview import AnswerSubmitRequest, SessionCreateRequest
from services.resume_scoring import (
    QUANT_UNIT_AFTER,
    QUANT_UNIT_BEFORE,
    extract_skills,
)

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


def _plan_question_types(total: int, interview_type: str) -> List[str]:
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


# ============================================================
# 三、题目生成（纯规则）
# ============================================================
def _build_question_text(
    question_type: str,
    ctx: Dict[str, Any],
) -> Dict[str, Any]:
    """按题型生成题目文本与期望要点。

    ctx 需包含：job_name / skill / difficulty / variant
    返回 ``{question, topic, expected_points}``

    扩展点：接入星火大模型后，本函数改为「组装 prompt + 解析结构化输出」即可，
    调用方无需改动。
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


def _build_question_plan(
    session: InterviewSession,
    job: Optional[Job],
    resume_skills: List[str],
) -> List[Dict[str, Any]]:
    """生成整场面试的题目计划（确定性：同输入同输出）。"""
    job_skills = _split_skills(job.skills) if job else []

    # 出题素材：岗位技能优先，其次简历技能；去重后作为技术题主题
    topics: List[str] = []
    for item in list(job_skills) + list(resume_skills):
        if item and item not in topics:
            topics.append(item)

    job_name = job.job_name if job else None
    types = _plan_question_types(session.total_questions, session.interview_type)

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

        built = _build_question_text(qtype, ctx)
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

    feedback = _build_feedback(raw, total, hit_points, skills_in_answer, chars)

    return {"score": total, **raw, "feedback": feedback}


def _build_feedback(
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
# 五、报告汇总
# ============================================================
def _job_match_score(
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
    """汇总一场面试的报告。纯规则、确定性。"""
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
    job_match = _job_match_score(job, answer_texts, resume_skills)

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
# 六、序列化辅助
# ============================================================
def _session_to_dict(session: InterviewSession) -> Dict[str, Any]:
    return {
        "id": session.id,
        "user_id": session.user_id,
        "job_id": session.job_id,
        "resume_id": session.resume_id,
        "interview_type": session.interview_type,
        "difficulty": session.difficulty,
        "duration": session.duration,
        "status": session.status,
        "total_questions": session.total_questions,
        "current_question_no": session.current_question_no,
        "started_at": session.started_at,
        "ended_at": session.ended_at,
        "created_at": session.created_at,
    }


def _question_to_dict(question: InterviewQuestion) -> Dict[str, Any]:
    points = question.expected_points
    return {
        "id": question.id,
        "question_no": question.question_no,
        "question": question.question,
        "question_type": question.question_type,
        "topic": question.topic,
        "difficulty": question.difficulty,
        "expected_points": list(points) if isinstance(points, list) else None,
        "created_at": question.created_at,
    }


def _answer_to_dict(answer: InterviewAnswer) -> Dict[str, Any]:
    return {
        "id": answer.id,
        "question_id": answer.question_id,
        "answer_text": answer.answer_text,
        "audio_url": answer.audio_url,
        "score": answer.score,
        "technical_score": answer.technical_score,
        "logic_score": answer.logic_score,
        "expression_score": answer.expression_score,
        "adaptability_score": answer.adaptability_score,
        "feedback": answer.feedback,
        "created_at": answer.created_at,
    }


def _report_to_dict(report: InterviewReport) -> Dict[str, Any]:
    strengths = report.strengths
    weaknesses = report.weaknesses
    return {
        "id": report.id,
        "session_id": report.session_id,
        "total_score": report.total_score,
        "technical_score": report.technical_score,
        "project_score": report.project_score,
        "logic_score": report.logic_score,
        "expression_score": report.expression_score,
        "communication_score": report.communication_score,
        "adaptability_score": report.adaptability_score,
        "job_match_score": report.job_match_score,
        "strengths": list(strengths) if isinstance(strengths, list) else None,
        "weaknesses": list(weaknesses) if isinstance(weaknesses, list) else None,
        "suggestions": report.suggestions,
        "created_at": report.created_at,
    }


# ============================================================
# 七、数据访问（内部）
# ============================================================
async def _load_session(
    db: AsyncSession, session_id: int, user_id: int
) -> InterviewSession:
    """按 id 加载会话并校验归属。不存在或非本人一律 404（不泄露他人会话存在性）。"""
    result = await db.execute(
        select(InterviewSession).where(InterviewSession.id == session_id)
    )
    session = result.scalar_one_or_none()
    if session is None or session.user_id != user_id:
        raise HTTPException(status_code=404, detail=f"面试会话 {session_id} 不存在")
    return session


async def _load_questions(
    db: AsyncSession, session_id: int
) -> List[InterviewQuestion]:
    result = await db.execute(
        select(InterviewQuestion)
        .where(InterviewQuestion.session_id == session_id)
        .order_by(InterviewQuestion.question_no)
    )
    return list(result.scalars().all())


async def _load_answers(
    db: AsyncSession, session_id: int
) -> List[InterviewAnswer]:
    """按会话取全部作答（answer → question → session 两跳关联）。"""
    result = await db.execute(
        select(InterviewAnswer)
        .join(InterviewQuestion, InterviewAnswer.question_id == InterviewQuestion.id)
        .where(InterviewQuestion.session_id == session_id)
        .order_by(InterviewQuestion.question_no)
    )
    return list(result.scalars().all())


async def _load_job(db: AsyncSession, job_id: Optional[int]) -> Optional[Job]:
    if not job_id:
        return None
    result = await db.execute(select(Job).where(Job.id == job_id))
    return result.scalar_one_or_none()


async def _load_resume_skills(
    db: AsyncSession, resume_id: Optional[int]
) -> List[str]:
    """读取简历技能。``resume`` 表当前无写入逻辑，取不到即返回空列表（不猜）。"""
    if not resume_id:
        return []
    result = await db.execute(select(Resume).where(Resume.id == resume_id))
    resume = result.scalar_one_or_none()
    if resume is None or not resume.content:
        return []
    return extract_skills(resume.content)


# ============================================================
# 八、生命周期：对外接口
# ============================================================
async def create_session(
    db: AsyncSession, user_id: int, payload: SessionCreateRequest
) -> Dict[str, Any]:
    """创建面试会话（状态 ``created``，尚未生成题目）。

    校验 ``job_id`` / ``resume_id`` 指向的记录确实存在——外键能保证写入合法，
    但提前校验可给出可读的错误提示，而不是让数据库抛完整性错误。
    """
    if payload.interview_type not in INTERVIEW_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"interview_type 取值非法，允许：{'/'.join(INTERVIEW_TYPES)}",
        )
    if payload.difficulty not in DIFFICULTIES:
        raise HTTPException(
            status_code=400,
            detail=f"difficulty 取值非法，允许：{'/'.join(DIFFICULTIES)}",
        )

    job = None
    if payload.job_id is not None:
        job = await _load_job(db, payload.job_id)
        if job is None:
            raise HTTPException(
                status_code=400, detail=f"岗位 {payload.job_id} 不存在，请先确认岗位库数据"
            )

    if payload.resume_id is not None:
        result = await db.execute(select(Resume).where(Resume.id == payload.resume_id))
        if result.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=400, detail=f"简历 {payload.resume_id} 不存在"
            )

    session = InterviewSession(
        user_id=user_id,
        job_id=payload.job_id,
        resume_id=payload.resume_id,
        interview_type=payload.interview_type,
        difficulty=payload.difficulty,
        duration=payload.duration,
        total_questions=payload.total_questions,
        current_question_no=0,
        status=SESSION_STATUS_CREATED,
    )
    db.add(session)
    await db.commit()
    await db.refresh(session)

    return {
        "session": _session_to_dict(session),
        "first_question": None,
        "job_name": job.job_name if job else None,
        "message": "面试会话创建成功，请调用 start 开始面试",
    }


async def start_session(
    db: AsyncSession, user_id: int, session_id: int
) -> Dict[str, Any]:
    """开始面试：生成整场题目计划并置为 ``ongoing``。

    题目计划一次性生成（当前为确定性规则，不依赖作答内容），
    因此 ``start`` 可安全重入判断——重复调用会被状态校验拦下。
    """
    session = await _load_session(db, session_id, user_id)

    if session.status == SESSION_STATUS_FINISHED:
        raise HTTPException(status_code=400, detail="面试已结束，无法重新开始")
    if session.status == SESSION_STATUS_ONGOING:
        raise HTTPException(
            status_code=400, detail="面试已在进行中，请直接获取当前题目"
        )

    job = await _load_job(db, session.job_id)
    resume_skills = await _load_resume_skills(db, session.resume_id)
    plan = _build_question_plan(session, job, resume_skills)

    if not plan:
        raise HTTPException(status_code=500, detail="题目计划生成失败，请检查会话配置")

    for item in plan:
        db.add(
            InterviewQuestion(
                session_id=session.id,
                question_no=item["question_no"],
                question=item["question"],
                question_type=item["question_type"],
                topic=item["topic"],
                difficulty=item["difficulty"],
                expected_points=item["expected_points"],
            )
        )

    session.status = SESSION_STATUS_ONGOING
    session.started_at = datetime.utcnow()
    session.current_question_no = 1
    await db.commit()
    await db.refresh(session)

    questions = await _load_questions(db, session.id)
    first = questions[0] if questions else None

    return {
        "session": _session_to_dict(session),
        "question": _question_to_dict(first) if first else None,
        "job_name": job.job_name if job else None,
        "message": "面试已开始",
    }


async def get_session_detail(
    db: AsyncSession, user_id: int, session_id: int
) -> Dict[str, Any]:
    """查询会话状态与完整对话（题目 + 已提交的作答）。"""
    session = await _load_session(db, session_id, user_id)
    job = await _load_job(db, session.job_id)
    questions = await _load_questions(db, session.id)
    answers = await _load_answers(db, session.id)

    return {
        "session": _session_to_dict(session),
        "job_name": job.job_name if job else None,
        "questions": [_question_to_dict(q) for q in questions],
        "answers": [_answer_to_dict(a) for a in answers],
        "answered_count": len(answers),
    }


async def get_current_question(
    db: AsyncSession, user_id: int, session_id: int
) -> Dict[str, Any]:
    """获取当前待作答的题目。

    四种情况：
    - 会话已结束（``finished``）→ 终态，不再返回任何题目，提示查看报告
    - 会话未开始（``created``）→ 返回 ``question=None``，提示先 start
    - 有题待答 → 返回该题，``answered=False``
    - 全部作答完毕 → ``all_answered=True``，提示调用 end
    """
    session = await _load_session(db, session_id, user_id)
    questions = await _load_questions(db, session.id)
    answers = await _load_answers(db, session.id)
    answered_ids = {a.question_id for a in answers}

    # 终态优先：finished 是不可逆终态，即使 current_question_no 尚未越过 N
    # （例如提前 end），也不得再对外呈现「待作答题目」，否则前端会误判还有题目可答。
    if session.status == SESSION_STATUS_FINISHED:
        return {
            "session_id": session.id,
            "status": session.status,
            "question_no": None,
            "total_questions": session.total_questions,
            "question": None,
            "answered": False,
            "all_answered": True,
            "message": "面试已结束，请调用 report 接口查看面试报告",
        }

    if session.status == SESSION_STATUS_CREATED or session.current_question_no <= 0:
        return {
            "session_id": session.id,
            "status": session.status,
            "question_no": None,
            "total_questions": session.total_questions,
            "question": None,
            "answered": False,
            "all_answered": False,
            "message": "面试尚未开始，请先调用 start 接口",
        }

    if session.current_question_no > session.total_questions:
        return {
            "session_id": session.id,
            "status": session.status,
            "question_no": None,
            "total_questions": session.total_questions,
            "question": None,
            "answered": False,
            "all_answered": True,
            "message": "全部题目已作答，请调用 end 接口生成报告",
        }

    current = next(
        (q for q in questions if q.question_no == session.current_question_no), None
    )
    if current is None:
        raise HTTPException(
            status_code=500,
            detail=f"会话数据异常：第 {session.current_question_no} 题不存在，请重新创建面试",
        )

    return {
        "session_id": session.id,
        "status": session.status,
        "question_no": current.question_no,
        "total_questions": session.total_questions,
        "question": _question_to_dict(current),
        "answered": current.id in answered_ids,
        "all_answered": False,
        "message": "ok",
    }


async def submit_answer(
    db: AsyncSession, user_id: int, session_id: int, payload: AnswerSubmitRequest
) -> Dict[str, Any]:
    """提交当前题目的回答 → 规则评分 → 推进到下一题。"""
    session = await _load_session(db, session_id, user_id)

    if session.status != SESSION_STATUS_ONGOING:
        raise HTTPException(
            status_code=400,
            detail=f"当前状态为 {session.status}，仅 ongoing 状态可提交回答",
        )
    if session.current_question_no <= 0:
        raise HTTPException(status_code=400, detail="面试尚未开始，请先调用 start 接口")
    if session.current_question_no > session.total_questions:
        raise HTTPException(
            status_code=400, detail="全部题目已作答，请调用 end 接口生成报告"
        )

    answer_text = (payload.answer_text or "").strip()
    if not answer_text:
        raise HTTPException(status_code=400, detail="回答内容不能为空")

    questions = await _load_questions(db, session.id)
    current = next(
        (q for q in questions if q.question_no == session.current_question_no), None
    )
    if current is None:
        raise HTTPException(
            status_code=500,
            detail=f"会话数据异常：第 {session.current_question_no} 题不存在",
        )

    # 一题一答：question_id 在表上有 UNIQUE 约束，这里先给出可读提示
    existing = await db.execute(
        select(InterviewAnswer).where(InterviewAnswer.question_id == current.id)
    )
    if existing.scalar_one_or_none() is not None:
        raise HTTPException(
            status_code=400, detail=f"第 {current.question_no} 题已作答，请勿重复提交"
        )

    scored = score_answer(_question_to_dict(current), answer_text)
    answer = InterviewAnswer(
        question_id=current.id,
        answer_text=answer_text,
        audio_url=payload.audio_url,  # 预留字段，当前允许为空
        score=scored["score"],
        technical_score=scored["technical_score"],
        logic_score=scored["logic_score"],
        expression_score=scored["expression_score"],
        adaptability_score=scored["adaptability_score"],
        feedback=scored["feedback"],
    )
    db.add(answer)

    # 推进题号；超过总题量即表示全部作答完毕
    session.current_question_no += 1
    await db.commit()
    await db.refresh(answer)
    await db.refresh(session)

    all_answered = session.current_question_no > session.total_questions
    next_question = None
    if not all_answered:
        next_question = next(
            (q for q in questions if q.question_no == session.current_question_no), None
        )

    return {
        "answer": _answer_to_dict(answer),
        "session": _session_to_dict(session),
        "next_question": _question_to_dict(next_question) if next_question else None,
        "all_answered": all_answered,
        "message": "全部题目已作答，请调用 end 接口生成报告" if all_answered else "回答已提交",
    }


async def end_session(
    db: AsyncSession, user_id: int, session_id: int
) -> Dict[str, Any]:
    """结束面试并生成报告。

    - ``finished`` 且报告已存在 → 幂等返回既有报告（便于前端重试）
    - ``created`` → 400（还没开始，无意义）
    - ``ongoing`` → 允许提前结束，按已作答内容出报告
    """
    session = await _load_session(db, session_id, user_id)

    if session.status == SESSION_STATUS_CREATED:
        raise HTTPException(status_code=400, detail="面试尚未开始，无法结束")

    if session.status == SESSION_STATUS_FINISHED:
        existing = await db.execute(
            select(InterviewReport).where(InterviewReport.session_id == session.id)
        )
        report = existing.scalar_one_or_none()
        if report is not None:
            return {
                "session": _session_to_dict(session),
                "report": _report_to_dict(report),
                "message": "面试已结束（返回既有报告）",
            }

    job = await _load_job(db, session.job_id)
    resume_skills = await _load_resume_skills(db, session.resume_id)
    questions = await _load_questions(db, session.id)
    answers = await _load_answers(db, session.id)

    summary = build_report(session, questions, answers, job, resume_skills)

    report = InterviewReport(
        session_id=session.id,
        total_score=summary["total_score"],
        technical_score=summary["technical_score"],
        project_score=summary["project_score"],
        logic_score=summary["logic_score"],
        expression_score=summary["expression_score"],
        communication_score=summary["communication_score"],
        adaptability_score=summary["adaptability_score"],
        job_match_score=summary["job_match_score"],
        strengths=summary["strengths"],
        weaknesses=summary["weaknesses"],
        suggestions=summary["suggestions"],
    )
    db.add(report)

    session.status = SESSION_STATUS_FINISHED
    session.ended_at = datetime.utcnow()
    await db.commit()
    await db.refresh(report)
    await db.refresh(session)

    return {
        "session": _session_to_dict(session),
        "report": _report_to_dict(report),
        "message": "面试已结束",
    }


async def get_report(
    db: AsyncSession, user_id: int, session_id: int
) -> Dict[str, Any]:
    """获取面试报告。未结束时明确提示，不返回半成品数据。"""
    session = await _load_session(db, session_id, user_id)

    result = await db.execute(
        select(InterviewReport).where(InterviewReport.session_id == session.id)
    )
    report = result.scalar_one_or_none()
    if report is None:
        raise HTTPException(
            status_code=404,
            detail=f"面试会话 {session_id} 尚无报告，当前状态为 {session.status}，请先调用 end 接口",
        )
    return _report_to_dict(report)
