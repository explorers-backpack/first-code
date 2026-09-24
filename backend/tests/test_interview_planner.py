# -*- coding: utf-8 -*-
"""AI 模拟面试 · Interview Planner 自检

无需 pytest，直接运行：
    python backend/tests/test_interview_planner.py

被测对象是 ``services.interview_planner``。规则计划部分**不依赖数据库**；
仅最后一节用 SQLite 内存库（+ StaticPool）验证 ``build_plan_for`` 的读库路径。

覆盖范围：
1. 结构不变量与 Pydantic 契约（权重合计 100、题量合计 = total_questions）
2. 基准用例复现（comprehensive / mid / 30 分钟 → 12 题，分布符合产品示例）
3. 难度、时长对题量的影响
4. 面试类型对阶段集合与权重的影响
5. 阶段顺序与 ``models.INTERVIEW_STAGES`` 一致
6. 确定性（同输入同输出）
7. target_topics 的来源与去重
8. priority_topics 是 target_topics 的子集；junior/mid 缺口优先、senior 交集优先
9. resume_focus_points 的启发式抽取
10. 缺岗位 / 缺简历时的降级（空列表，不报错）
11. 非法入参兜底（不抛异常，回落默认值）
12. 题量边界与收敛
13. LLM 增强成功路径
14. LLM 各类失败一律回退到规则计划
15. LLM 无法破坏计划结构（stages / 题量不可被改写）
16. 提示词边界：只规划知识点，不生成题目
17. ``build_plan_for`` 读库路径（jobs / resume）
"""

import asyncio
import json
import os
import pathlib
import sys

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import INTERVIEW_STAGES, Job, Resume, User  # noqa: E402
from schemas.interview import InterviewPlanOut  # noqa: E402
from services import interview_planner  # noqa: E402

_PASSED = 0
_FAILED = 0


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (f"  -> {detail}" if detail and not cond else ""))
    return cond


class _FakeSpark:
    """假的 Spark 服务：可返回字符串、抛异常，并记录收到的提示词。"""

    def __init__(self, reply):
        self.reply = reply
        self.calls = []

    async def chat_async(self, message: str) -> str:
        self.calls.append(message)
        if isinstance(self.reply, BaseException):
            raise self.reply
        return self.reply


# ---- 固定输入素材（模拟真实 JD 与简历）----
JOB = {
    "id": 1,
    "job_name": "Java 后端开发工程师",
    "skills": "Java,Spring Boot,MySQL,Redis",
    "duty": "负责订单系统开发，熟悉分布式系统与消息队列",
}
RESUME_TEXT = (
    "项目经历\n"
    "负责电商项目后端开发，主导订单系统重构，QPS 提升 3 倍\n"
    "参与支付平台建设\n"
    "技能特长\n"
    "Java、Spring Boot、Kafka\n"
)


def _stage_map(plan) -> dict:
    return {s["stage"]: s for s in plan["stages"]}


def _question_sum(plan) -> int:
    return sum(s["target_questions"] for s in plan["stages"])


def _weight_sum(plan) -> int:
    return sum(s["weight"] for s in plan["stages"])


async def run() -> bool:
    print("=" * 70)
    print("AI 模拟面试 · Interview Planner 自检")
    print("=" * 70)

    # ============================================================
    # [1] 结构不变量与 Pydantic 契约
    # ============================================================
    print("\n[1] 结构不变量与 Pydantic 契约")
    base = interview_planner.build_plan(
        interview_type="comprehensive", difficulty="mid", duration=30,
        job=JOB, resume=RESUME_TEXT,
    )
    required = (
        "interview_type", "difficulty", "duration", "total_questions", "stages",
        "target_topics", "priority_topics", "resume_focus_points", "source",
    )
    _check("输出含全部必需字段", all(k in base for k in required),
           str([k for k in required if k not in base]))
    _check("stages 每项含 stage/weight/target_questions",
           all(set(s) == {"stage", "weight", "target_questions"} for s in base["stages"]))
    _check("阶段权重合计 100", _weight_sum(base) == 100, str(_weight_sum(base)))
    _check("阶段题量合计 = total_questions",
           _question_sum(base) == base["total_questions"],
           f"{_question_sum(base)} vs {base['total_questions']}")
    _check("每阶段至少 1 题", all(s["target_questions"] >= 1 for s in base["stages"]))
    _check("阶段不重复",
           len({s["stage"] for s in base["stages"]}) == len(base["stages"]))
    _check("source 默认为 rule", base["source"] == "rule", base["source"])
    _check("可通过 InterviewPlanOut 校验", InterviewPlanOut(**base) is not None)

    # ============================================================
    # [2] 基准用例：复现产品给出的示例分布
    # ============================================================
    print("\n[2] 基准用例（comprehensive / mid / 30 分钟）")
    _check("总题量为 12", base["total_questions"] == 12, str(base["total_questions"]))
    expect = {"introduction": 1, "resume": 2, "technical": 4, "project": 3, "scenario": 2}
    got = {s["stage"]: s["target_questions"] for s in base["stages"]}
    _check("各阶段题量与产品示例一致", got == expect, f"{got} != {expect}")
    expect_w = {"introduction": 10, "resume": 15, "technical": 35, "project": 25, "scenario": 15}
    got_w = {s["stage"]: s["weight"] for s in base["stages"]}
    _check("各阶段权重与产品示例一致", got_w == expect_w, f"{got_w} != {expect_w}")

    # ============================================================
    # [3] 难度影响题量
    # ============================================================
    print("\n[3] 难度影响题量（时长固定 30 分钟）")
    for diff, expected_total in (("junior", 10), ("mid", 12), ("senior", 15)):
        p = interview_planner.build_plan(difficulty=diff, duration=30, job=JOB)
        _check(f"{diff} 30 分钟 → {expected_total} 题",
               p["total_questions"] == expected_total, str(p["total_questions"]))
        _check(f"{diff} 题量分配仍守恒", _question_sum(p) == expected_total)

    # ============================================================
    # [4] 时长影响题量
    # ============================================================
    print("\n[4] 时长影响题量（难度固定 mid）")
    for dur, expected_total in ((15, 6), (20, 8), (30, 12), (45, 18)):
        p = interview_planner.build_plan(difficulty="mid", duration=dur, job=JOB)
        _check(f"{dur} 分钟 → {expected_total} 题",
               p["total_questions"] == expected_total, str(p["total_questions"]))
    p_short = interview_planner.build_plan(difficulty="junior", duration=5, job=JOB)
    _check("题量下限 5（5 分钟 junior）", p_short["total_questions"] == 5, str(p_short["total_questions"]))
    _check("极短时长仍每阶段 ≥1 题", all(s["target_questions"] >= 1 for s in p_short["stages"]))
    _check("极短时长题量仍守恒", _question_sum(p_short) == 5, str(_question_sum(p_short)))
    p_long = interview_planner.build_plan(difficulty="senior", duration=180, job=JOB)
    _check("题量上限 20（180 分钟 senior）", p_long["total_questions"] == 20, str(p_long["total_questions"]))

    # ============================================================
    # [5] 面试类型影响阶段集合与权重
    # ============================================================
    print("\n[5] 面试类型影响阶段集合与权重")
    p_tech = interview_planner.build_plan(interview_type="technical", difficulty="mid", duration=30, job=JOB)
    _check("technical 不含 hr 阶段", "hr" not in _stage_map(p_tech))
    _check("technical 的 technical 权重最高（45）",
           _stage_map(p_tech)["technical"]["weight"] == 45)
    _check("technical 题量更集中于技术（>=5）",
           _stage_map(p_tech)["technical"]["target_questions"] >= 5,
           str(_stage_map(p_tech)["technical"]["target_questions"]))

    p_beh = interview_planner.build_plan(interview_type="behavioral", difficulty="mid", duration=30, job=JOB)
    _check("behavioral 不含 technical 阶段", "technical" not in _stage_map(p_beh))
    _check("behavioral 含 hr 阶段", "hr" in _stage_map(p_beh))
    _check("behavioral 的 project 权重最高（30）",
           _stage_map(p_beh)["project"]["weight"] == 30)

    for itype in ("technical", "behavioral", "comprehensive"):
        p = interview_planner.build_plan(interview_type=itype, difficulty="mid", duration=30, job=JOB)
        _check(f"{itype} 权重合计 100", _weight_sum(p) == 100, str(_weight_sum(p)))
        _check(f"{itype} 题量守恒", _question_sum(p) == p["total_questions"])
        _check(f"{itype} 通过 Pydantic 校验", InterviewPlanOut(**p) is not None)

    # ============================================================
    # [6] 阶段顺序与状态机一致
    # ============================================================
    print("\n[6] 阶段顺序与 models.INTERVIEW_STAGES 一致")
    for itype in ("technical", "behavioral", "comprehensive"):
        p = interview_planner.build_plan(interview_type=itype, difficulty="mid", duration=30, job=JOB)
        stages = [s["stage"] for s in p["stages"]]
        canonical = [s for s in INTERVIEW_STAGES if s in stages]
        _check(f"{itype} 阶段顺序符合推进顺序", stages == canonical, f"{stages} != {canonical}")

    # ============================================================
    # [7] 确定性
    # ============================================================
    print("\n[7] 确定性（同输入同输出）")
    a = interview_planner.build_plan(difficulty="senior", duration=40, job=JOB, resume=RESUME_TEXT)
    b = interview_planner.build_plan(difficulty="senior", duration=40, job=JOB, resume=RESUME_TEXT)
    _check("两次调用结果完全相同", a == b)
    _check("与基准用例不同输入结果不同", a != base)

    # ============================================================
    # [8] target_topics
    # ============================================================
    print("\n[8] target_topics 来源与去重")
    topics = base["target_topics"]
    for t in ("Java", "Spring Boot", "MySQL", "Redis"):
        _check(f"含岗位技能 {t}", t in topics)
    _check("含岗位职责中抽取的技能（分布式系统）", "分布式系统" in topics, str(topics))
    _check("含简历技能（Kafka）", "Kafka" in topics, str(topics))
    _check("无重复项（忽略大小写）",
           len({t.lower() for t in topics}) == len(topics))
    _check("上限 12", len(topics) <= 12, str(len(topics)))
    _check("简历与岗位都有的 Java 只出现一次", topics.count("Java") == 1)

    # ============================================================
    # [9] priority_topics
    # ============================================================
    print("\n[9] priority_topics 规则")
    prio = base["priority_topics"]
    _check("是 target_topics 的子集", set(prio) <= set(base["target_topics"]))
    _check("非空", len(prio) > 0, str(prio))
    _check("上限 5", len(prio) <= 5, str(len(prio)))

    # mid：缺口优先——MySQL / Redis 岗位要求但简历未体现
    _check("mid 时缺口（MySQL）排在简历已具备的 Java 之前",
           prio.index("MySQL") < prio.index("Java"), str(prio))

    # senior：交集优先——Java 岗位与简历都有，用于验证深度
    p_senior = interview_planner.build_plan(difficulty="senior", duration=30, job=JOB, resume=RESUME_TEXT)
    prio_senior = p_senior["priority_topics"]
    _check("senior 时交集（Java）排在缺口（MySQL）之前",
           prio_senior.index("Java") < prio_senior.index("MySQL"), str(prio_senior))

    # 无简历时：岗位技能全部是缺口，按 JD 顺序取前 5
    p_noresume = interview_planner.build_plan(difficulty="mid", duration=30, job=JOB)
    _check("无简历时优先级取自岗位技能",
           p_noresume["priority_topics"][:4] == ["Java", "Spring Boot", "MySQL", "Redis"],
           str(p_noresume["priority_topics"]))

    # ============================================================
    # [10] resume_focus_points
    # ============================================================
    print("\n[10] resume_focus_points 抽取")
    focus = base["resume_focus_points"]
    _check("抽出电商项目", "电商项目" in focus, str(focus))
    _check("抽出订单系统（同行第二个项目也能识别）", "订单系统" in focus, str(focus))
    _check("抽出支付平台", "支付平台" in focus, str(focus))
    _check("已剥离「负责/主导」等前缀噪声",
           all(not f.startswith(("负责", "主导", "参与")) for f in focus), str(focus))
    _check("章节标题未被当作重点", "项目经历" not in focus and "技能特长" not in focus, str(focus))
    _check("上限 5", len(focus) <= 5, str(len(focus)))

    explicit = interview_planner.build_plan(
        job=JOB, resume="项目名称：电商平台\n- 参与支付中台建设\n【风控系统】"
    )
    _check("支持「项目名称：」显式声明",
           explicit["resume_focus_points"][0] == "电商平台", str(explicit["resume_focus_points"]))
    _check("支持【】标注与中台后缀",
           {"支付中台", "风控系统"} <= set(explicit["resume_focus_points"]),
           str(explicit["resume_focus_points"]))

    empty_focus = interview_planner.build_plan(job=JOB, resume="教育背景\n本科 计算机科学与技术")
    _check("无项目类文本时返回空数组（不编造）", empty_focus["resume_focus_points"] == [],
           str(empty_focus["resume_focus_points"]))

    # ============================================================
    # [11] 缺岗位 / 缺简历时的降级
    # ============================================================
    print("\n[11] 缺岗位 / 缺简历降级")
    none_both = interview_planner.build_plan(interview_type="comprehensive", difficulty="mid", duration=30)
    _check("无岗位无简历仍能产出计划", bool(none_both["stages"]))
    _check("无岗位无简历时 target_topics 为空", none_both["target_topics"] == [])
    _check("无岗位无简历时 priority_topics 为空", none_both["priority_topics"] == [])
    _check("无岗位无简历时 resume_focus_points 为空", none_both["resume_focus_points"] == [])
    _check("无岗位无简历时计划仍通过 Pydantic 校验", InterviewPlanOut(**none_both) is not None)
    _check("无岗位无简历时权重仍守恒", _weight_sum(none_both) == 100)

    only_resume = interview_planner.build_plan(job=None, resume=RESUME_TEXT, duration=30)
    _check("仅简历时 target_topics 取简历技能",
           "Kafka" in only_resume["target_topics"], str(only_resume["target_topics"]))
    _check("仅简历时 priority_topics 为空（无岗位要求）",
           only_resume["priority_topics"] == [], str(only_resume["priority_topics"]))
    _check("仅简历时重点仍可抽出", "电商项目" in only_resume["resume_focus_points"])

    # ============================================================
    # [12] 非法入参兜底（fallback 路径必须永不失败）
    # ============================================================
    print("\n[12] 非法入参兜底")
    bad_type = interview_planner.build_plan(interview_type="nope", difficulty="mid", duration=30)
    _check("未知 interview_type 回落 comprehensive",
           bad_type["interview_type"] == "comprehensive", bad_type["interview_type"])
    bad_diff = interview_planner.build_plan(interview_type="comprehensive", difficulty="god", duration=30)
    _check("未知 difficulty 回落 mid", bad_diff["difficulty"] == "mid", bad_diff["difficulty"])
    _check("未知 difficulty 题量按 mid 口径（12）", bad_diff["total_questions"] == 12,
           str(bad_diff["total_questions"]))
    bad_dur = interview_planner.build_plan(difficulty="mid", duration="abc")
    _check("非法 duration 回落 30", bad_dur["duration"] == 30, str(bad_dur["duration"]))
    _check("duration 超上限被收敛到 180",
           interview_planner.build_plan(duration=9999)["duration"] == 180)
    _check("duration 低于下限被收敛到 5",
           interview_planner.build_plan(duration=1)["duration"] == 5)
    _check("非法入参不抛异常且结构合法",
           InterviewPlanOut(**bad_type) is not None)

    # ============================================================
    # [13] 题量边界
    # ============================================================
    print("\n[13] 题量边界")
    for total in (1, 5, 8, 20):
        p = interview_planner.build_plan(difficulty="mid", duration=30, job=JOB, total_questions=total)
        _check(f"显式 total_questions={total} 被采纳", p["total_questions"] == total, str(p["total_questions"]))
        _check(f"total_questions={total} 题量守恒", _question_sum(p) == total,
               f"{_question_sum(p)} vs {total}")
    _check("total_questions=0 收敛到 1",
           interview_planner.build_plan(total_questions=0)["total_questions"] == 1)
    _check("total_questions=100 收敛到 20",
           interview_planner.build_plan(total_questions=100)["total_questions"] == 20)
    _check("total_questions 非数字时回落到时长口径",
           interview_planner.build_plan(duration=30, difficulty="mid",
                                        total_questions="abc")["total_questions"] == 12)

    # ============================================================
    # [14] LLM 增强成功路径
    # ============================================================
    print("\n[14] LLM 增强成功路径")
    reply = json.dumps({
        "target_topics": ["Java", "Spring Boot", "MySQL", "Redis", "JVM 调优"],
        "priority_topics": ["Redis", "MySQL"],
        "resume_focus_points": ["电商项目", "订单系统"],
    }, ensure_ascii=False)
    spark = _FakeSpark(reply)
    enriched = await interview_planner.refine_plan_with_llm(base, job=JOB, resume=RESUME_TEXT, spark=spark)
    _check("source 标记为 llm", enriched["source"] == "llm", enriched["source"])
    _check("target_topics 被模型结果替换",
           enriched["target_topics"] == ["Java", "Spring Boot", "MySQL", "Redis", "JVM 调优"],
           str(enriched["target_topics"]))
    _check("priority_topics 采用模型结果",
           enriched["priority_topics"] == ["Redis", "MySQL"], str(enriched["priority_topics"]))
    _check("resume_focus_points 采用模型结果",
           enriched["resume_focus_points"] == ["电商项目", "订单系统"],
           str(enriched["resume_focus_points"]))
    _check("增强后阶段划分不变", enriched["stages"] == base["stages"])
    _check("增强后通过 Pydantic 校验", InterviewPlanOut(**enriched) is not None)
    _check("提示词包含岗位名称", "Java 后端开发工程师" in spark.calls[0])
    _check("提示词包含简历正文", "电商项目" in spark.calls[0])

    # ============================================================
    # [15] LLM 失败回退
    # ============================================================
    print("\n[15] LLM 失败回退")
    for name, bad_reply in (
        ("哨兵串 API调用失败", "API调用失败: timeout"),
        ("哨兵串 SparkAPI not available", "SparkAPI not available"),
        ("哨兵串 AI 回答为空", "AI 回答为空"),
        ("空字符串", ""),
        ("纯文本无 JSON", "抱歉，我无法完成这个任务。"),
        ("JSON 不完整", '{"target_topics": ["Java"'),
        ("JSON 非对象", '["Java", "MySQL"]'),
        ("JSON 缺少 target_topics", '{"priority_topics": ["Java"]}'),
        ("target_topics 为空数组", '{"target_topics": []}'),
        ("target_topics 类型错误", '{"target_topics": "Java,MySQL"}'),
    ):
        out = await interview_planner.refine_plan_with_llm(
            base, job=JOB, resume=RESUME_TEXT, spark=_FakeSpark(bad_reply)
        )
        _check(f"{name} → 回退规则计划", out == base)

    exc_out = await interview_planner.refine_plan_with_llm(
        base, job=JOB, resume=RESUME_TEXT, spark=_FakeSpark(RuntimeError("网络中断"))
    )
    _check("调用抛异常 → 回退且不冒泡", exc_out == base)
    _check("回退后 source 仍为 rule", exc_out["source"] == "rule", exc_out["source"])

    # 拿不到 Spark 实例（如运行环境未配置）→ 直接回退，不发任何请求。
    # 此处替换 _default_spark，避免依赖本机 .env 中的真实凭据与网络。
    original_default = interview_planner._default_spark
    interview_planner._default_spark = lambda: None
    try:
        no_spark = await interview_planner.refine_plan_with_llm(
            base, job=JOB, resume=RESUME_TEXT, spark=None
        )
    finally:
        interview_planner._default_spark = original_default
    _check("拿不到 Spark 实例 → 回退规则计划", no_spark == base)
    _check("无 Spark 时 source 仍为 rule", no_spark["source"] == "rule", no_spark["source"])

    # ============================================================
    # [16] LLM 无法破坏计划结构
    # ============================================================
    print("\n[16] LLM 无法改写阶段与题量")
    hijack = json.dumps({
        "target_topics": ["Java", "MySQL"],
        "priority_topics": ["MySQL", "Java"],
        "resume_focus_points": ["电商项目"],
        "stages": [{"stage": "hr", "weight": 100, "target_questions": 99}],
        "total_questions": 99,
        "difficulty": "senior",
    }, ensure_ascii=False)
    guarded = await interview_planner.refine_plan_with_llm(
        base, job=JOB, resume=RESUME_TEXT, spark=_FakeSpark(hijack)
    )
    _check("stages 未被模型改写", guarded["stages"] == base["stages"])
    _check("total_questions 未被模型改写",
           guarded["total_questions"] == base["total_questions"])
    _check("difficulty 未被模型改写", guarded["difficulty"] == base["difficulty"])
    _check("被改写后仍通过 Pydantic 校验", InterviewPlanOut(**guarded) is not None)

    not_subset = json.dumps({
        "target_topics": ["Java", "MySQL"],
        "priority_topics": ["Kubernetes", "Java"],
        "resume_focus_points": [],
    }, ensure_ascii=False)
    filtered = await interview_planner.refine_plan_with_llm(
        base, job=JOB, resume=RESUME_TEXT, spark=_FakeSpark(not_subset)
    )
    _check("非 target 子集的 priority 项被过滤",
           filtered["priority_topics"] == ["Java"], str(filtered["priority_topics"]))
    _check("模型未给出重点时沿用规则结果",
           filtered["resume_focus_points"] == base["resume_focus_points"])

    dupes = json.dumps({
        "target_topics": ["Java", "java", " MySQL ", ""],
        "priority_topics": ["Java", "java"],
        "resume_focus_points": ["电商项目", "电商项目"],
    }, ensure_ascii=False)
    cleaned = await interview_planner.refine_plan_with_llm(
        base, job=JOB, resume=RESUME_TEXT, spark=_FakeSpark(dupes)
    )
    _check("模型输出的重复项被去重",
           cleaned["target_topics"] == ["Java", "MySQL"], str(cleaned["target_topics"]))
    _check("priority 去重后只留一项", cleaned["priority_topics"] == ["Java"],
           str(cleaned["priority_topics"]))

    mixed = json.dumps({
        "target_topics": ["Java", 123, None, "MySQL", {"a": 1}],
        "priority_topics": ["MySQL"],
        "resume_focus_points": [42, "电商项目"],
    }, ensure_ascii=False)
    sanitized = await interview_planner.refine_plan_with_llm(
        base, job=JOB, resume=RESUME_TEXT, spark=_FakeSpark(mixed)
    )
    _check("模型返回的非字符串元素被丢弃",
           sanitized["target_topics"] == ["Java", "MySQL"], str(sanitized["target_topics"]))
    _check("重点列表同样只保留字符串",
           sanitized["resume_focus_points"] == ["电商项目"],
           str(sanitized["resume_focus_points"]))

    # ============================================================
    # [17] 提示词边界：只规划，不出题
    # ============================================================
    print("\n[17] 提示词边界（只规划，不出题）")
    prompt = interview_planner._build_llm_prompt(base, JOB, RESUME_TEXT)
    _check("明确要求不要生成题目", "不要生成任何面试题目" in prompt)
    _check("要求只输出 JSON", "只输出 JSON" in prompt)
    _check("要求 priority 为 target 子集", "必须是 target_topics 的子集" in prompt)
    _check("包含面试配置（难度/时长/题量）",
           all(x in prompt for x in ("中级", "30 分钟", "12 题")), prompt[:200])
    _check("无简历时提示词标注（无简历）",
           "（无简历）" in interview_planner._build_llm_prompt(base, JOB, None))
    _check("无岗位时提示词标注（未指定岗位）",
           "（未指定岗位）" in interview_planner._build_llm_prompt(base, None, None))

    # ============================================================
    # [18] build_plan_for 读库路径
    # ============================================================
    print("\n[18] build_plan_for 读库路径")
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_factory() as db:
        user = User(username="planner", email="planner@example.com", password_hash="x", role="user")
        job = Job(
            job_name="Java 后端开发工程师", salary="20-35K", edu_require="本科",
            major_require="不限", skills="Java,Spring Boot,MySQL,Redis",
            duty="负责订单系统开发，熟悉分布式系统与消息队列",
            city="深圳", industry="互联网",
        )
        resume = Resume(user_id=1, filename="resume.txt", content=RESUME_TEXT)
        db.add_all([user, job, resume])
        await db.commit()
        await db.refresh(job)
        await db.refresh(resume)

        from_db = await interview_planner.build_plan_for(
            db, job_id=job.id, resume_id=resume.id,
            interview_type="technical", difficulty="mid", duration=30,
        )
        _check("读库后 job_id 正确回填", from_db["job_id"] == job.id, str(from_db["job_id"]))
        _check("读库后 resume_id 正确回填", from_db["resume_id"] == resume.id, str(from_db["resume_id"]))
        _check("读库后取到岗位技能", "Redis" in from_db["target_topics"], str(from_db["target_topics"]))
        _check("读库后取到简历重点", "电商项目" in from_db["resume_focus_points"],
               str(from_db["resume_focus_points"]))
        _check("读库结果通过 Pydantic 校验", InterviewPlanOut(**from_db) is not None)

        missing = await interview_planner.build_plan_for(
            db, job_id=999999, resume_id=999999, duration=30
        )
        _check("岗位/简历不存在时不报错，按缺省处理",
               missing["job_id"] is None and missing["resume_id"] is None)
        _check("岗位/简历不存在时计划仍合法", InterviewPlanOut(**missing) is not None)

        llm_plan = await interview_planner.build_plan_for(
            db, job_id=job.id, resume_id=resume.id, duration=30,
            use_llm=True, spark=_FakeSpark(reply),
        )
        _check("use_llm=True 时走增强路径", llm_plan["source"] == "llm", llm_plan["source"])
        fallback_plan = await interview_planner.build_plan_for(
            db, job_id=job.id, resume_id=resume.id, duration=30,
            use_llm=True, spark=_FakeSpark("API调用失败: boom"),
        )
        _check("use_llm=True 但调用失败 → source=rule", fallback_plan["source"] == "rule",
               fallback_plan["source"])

        rows = (await db.execute(select(Resume))).scalars().all()
        _check("读库路径不写库（resume 仍只有 1 行）", len(rows) == 1, str(len(rows)))

    await engine.dispose()

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
