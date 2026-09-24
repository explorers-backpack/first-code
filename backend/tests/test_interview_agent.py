# -*- coding: utf-8 -*-
"""AI 模拟面试 · Interview Agent（问题生成）自检

无需 pytest，直接运行：
    python backend/tests/test_interview_agent.py

**全程使用 Mock Spark，不调用真实 API、不联网、不读库。**
被测对象是 ``services.interview_agent``。

覆盖范围：
1. 正常生成问题（含 Prompt 变量注入、调用次数、返回结构）
2. 非法 JSON（含一次性修复成功 / 两次都失败）
3. 空问题
4. 重复问题（与历史问题高度重复）
5. 缺少 topic
6. difficulty 非法 / 缺失时的补齐
7. 服务级故障不重试（不可用 / 抛异常 / 哨兵串）
8. 不伪造问题（所有失败路径 question 恒为空）
9. 查重算法与校验函数的单元测试
10. 边界：不接收 db 会话（本阶段不碰数据库）、缺简历/缺岗位降级
"""

import asyncio
import inspect
import json
import os
import pathlib
import sys

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from models import DIFFICULTIES  # noqa: E402
from services import interview_agent, interview_planner  # noqa: E402

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


class MockSpark:
    """按顺序吐出预设回复的假 Spark。

    - 回复可以是字符串（正常返回）、``BaseException`` 实例（模拟抛异常）
    - 超出预设次数即抛 AssertionError，用来断言「没有多余调用」
    """

    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls: list[str] = []

    @property
    def call_count(self) -> int:
        return len(self.calls)

    async def chat_async(self, message: str) -> str:
        self.calls.append(message)
        if not self.replies:
            raise AssertionError("Mock Spark 被调用次数超出预期")
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


# ---- 固定素材 ----
JOB = {
    "job_name": "Java 后端开发工程师",
    "skills": "Java,Spring Boot,MySQL,Redis",
    "duty": "负责订单系统开发，熟悉分布式系统与消息队列",
}
RESUME_TEXT = (
    "项目经历\n"
    "负责电商项目后端开发，主导订单系统重构，QPS 提升 3 倍\n"
    "技能特长\nJava、Spring Boot、Kafka\n"
)
CONTEXT = {
    "session_id": 7,
    "current_question_no": 2,
    "current_stage": "technical",
    "asked_questions": ["请做一下自我介绍", "讲讲你在电商项目中负责的订单系统重构"],
    "covered_topics": ["Java", "Spring Boot"],
    "weak_topics": ["Redis 持久化"],
    "follow_up_count": 0,
    "total_questions": 12,
    "max_questions": 12,
}
EMPTY_CONTEXT = {
    "current_stage": "introduction",
    "asked_questions": [],
    "covered_topics": [],
    "weak_topics": [],
}


def _question_json(**overrides) -> str:
    payload = {
        "question": "请说明 Redis 持久化中 RDB 与 AOF 的取舍，以及在订单系统里你如何选型。",
        "question_type": "technical",
        "topic": "Redis 持久化",
        "difficulty": "mid",
        "expected_points": ["RDB 快照", "AOF 追加", "恢复速度", "数据安全"],
        "reason": "补强薄弱知识点 Redis 持久化",
    }
    payload.update(overrides)
    return json.dumps(payload, ensure_ascii=False)


def _plan():
    return interview_planner.build_plan(
        interview_type="technical", difficulty="mid", duration=30,
        job=JOB, resume=RESUME_TEXT,
    )


async def run() -> bool:
    print("=" * 70)
    print("AI 模拟面试 · Interview Agent（问题生成）自检")
    print("=" * 70)

    plan = _plan()

    # ============================================================
    # [1] 正常生成问题
    # ============================================================
    print("\n[1] 正常生成问题")
    spark = MockSpark(_question_json())
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)

    _check("ok=True", result["ok"] is True, str(result.get("error")))
    _check("question 非空且解析正确",
           result["question"].startswith("请说明 Redis 持久化"))
    _check("question_type 正确", result["question_type"] == "technical")
    _check("topic 正确", result["topic"] == "Redis 持久化")
    _check("difficulty 正确", result["difficulty"] == "mid")
    _check("expected_points 为 4 项",
           result["expected_points"] == ["RDB 快照", "AOF 追加", "恢复速度", "数据安全"],
           str(result["expected_points"]))
    _check("reason 正确", result["reason"] == "补强薄弱知识点 Redis 持久化")
    _check("error 为 None", result["error"] is None)
    _check("只调用一次 Spark（成功不重试）", spark.call_count == 1, str(spark.call_count))
    _check("输出字段集为 8 个（6 业务字段 + ok + error）",
           set(result) == {"ok", "question", "question_type", "topic", "difficulty",
                           "expected_points", "reason", "error"},
           str(sorted(result)))

    prompt = spark.calls[0]
    _check("注入当前阶段", "当前阶段：technical" in prompt)
    _check("注入岗位名称", "Java 后端开发工程师" in prompt)
    _check("注入岗位描述（技能要求 + 职责）",
           "技能要求：Java,Spring Boot,MySQL,Redis" in prompt
           and "岗位职责：负责订单系统开发" in prompt)
    _check("注入简历摘要", "主导订单系统重构" in prompt)
    _check("注入已提问问题", "- 讲讲你在电商项目中负责的订单系统重构" in prompt)
    _check("注入已覆盖知识点", "- Java" in prompt and "- Spring Boot" in prompt)
    _check("注入薄弱知识点", "- Redis 持久化" in prompt)
    _check("注入优先考察点（经 interview_plan）", '"priority_topics"' in prompt)
    _check("注入计划阶段", '"stages"' in prompt)
    _check("无残留占位符", "{{" not in prompt)
    _check("加载的是 question.txt（含 7 阶段指引）", "introduction：自我介绍" in prompt)
    _check("注入面试类型与难度", "面试类型：technical" in prompt and "面试难度：mid" in prompt)

    # ============================================================
    # [2] 非法 JSON
    # ============================================================
    print("\n[2] 非法 JSON → 修复一次后成功")
    spark = MockSpark("抱歉，我先解释一下思路……", _question_json())
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("修复后 ok=True", result["ok"] is True, str(result.get("error")))
    _check("共调用两次（首次 + 修复）", spark.call_count == 2, str(spark.call_count))
    _check("第二次是修复提示词", "修复上一次的输出" in spark.calls[1])
    _check("修复提示词带失败原因", "未找到合法的 JSON 对象" in spark.calls[1])
    _check("修复提示词带上次原始输出", "抱歉，我先解释一下思路" in spark.calls[1])
    _check("修复提示词带历史问题", "- 请做一下自我介绍" in spark.calls[1])
    _check("修复提示词无残留占位符", "{{" not in spark.calls[1])

    print("\n[2b] 非法 JSON 且修复仍失败 → 明确错误")
    spark = MockSpark("不是 JSON", "还是不是 JSON")
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("ok=False", result["ok"] is False)
    _check("question 为空（不伪造）", result["question"] == "")
    _check("expected_points 为空", result["expected_points"] == [])
    _check("error 说明修复次数", "已发起 1 次修复仍不通过" in (result["error"] or ""),
           str(result["error"]))
    _check("恰好调用两次（重试上限为 1）", spark.call_count == 2, str(spark.call_count))

    print("\n[2c] 其他非法 JSON 形态")
    for label, bad in (
        ("空字符串", ""),
        ("JSON 被截断", '{"question": "q", "topic": "t"'),
        ("JSON 数组", '["q"]'),
    ):
        spark = MockSpark(bad, bad)
        result = await interview_agent.generate_question(
            CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
        _check(f"{label} → ok=False 且 question 为空",
               result["ok"] is False and result["question"] == "", str(result.get("error")))

    # ============================================================
    # [3] 空问题
    # ============================================================
    print("\n[3] 空问题")
    spark = MockSpark(_question_json(question=""), _question_json())
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("首次空问题 → 触发修复并成功", result["ok"] is True, str(result.get("error")))
    _check("空问题时也重试一次", spark.call_count == 2, str(spark.call_count))
    _check("修复提示词指出 question 为空", "question 为空" in spark.calls[1], spark.calls[1][:200])

    spark = MockSpark(_question_json(question="   "), _question_json(question=""))
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("两次都空 → ok=False", result["ok"] is False)
    _check("失败时 question 为空字符串", result["question"] == "")
    _check("error 保留可读原因", "question 为空" in (result["error"] or ""),
           str(result["error"]))

    print("\n[3b] 模型显式返回 error 分支")
    spark = MockSpark(
        json.dumps({"question": "", "error": "面试计划已完成，无需继续出题"}, ensure_ascii=False),
        json.dumps({"question": "", "error": "面试计划已完成，无需继续出题"}, ensure_ascii=False),
    )
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("显式 error → ok=False", result["ok"] is False)
    _check("显式 error 原因被保留并上报",
           "面试计划已完成" in (result["error"] or ""), str(result["error"]))

    # ============================================================
    # [4] 重复问题
    # ============================================================
    print("\n[4] 与历史问题高度重复")
    duplicated = _question_json(question="请做一下自我介绍")
    spark = MockSpark(duplicated, _question_json())
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("首次重复 → 触发修复并成功", result["ok"] is True, str(result.get("error")))
    _check("重复时重试一次", spark.call_count == 2, str(spark.call_count))
    _check("修复提示词指出重复", "高度重复" in spark.calls[1], spark.calls[1][:200])

    spark = MockSpark(duplicated, duplicated)
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("两次都重复 → ok=False", result["ok"] is False)
    _check("失败时 question 为空（不伪造）", result["question"] == "")
    _check("error 含「高度重复」", "高度重复" in (result["error"] or ""), str(result["error"]))

    print("\n[4b] 近似改写（非字面相同）也能识别")
    rewritten = _question_json(question="请做一下自我介绍，简单说明你的职业经历。")
    spark = MockSpark(rewritten, rewritten)
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("短问题被扩写从句 → 判为重复", result["ok"] is False, str(result.get("error")))
    _check("扩写版相似度被判为 1.0",
           interview_agent.question_similarity(
               "请做一下自我介绍，简单说明你的职业经历。", "请做一下自我介绍") == 1.0)

    print("\n[4c] 包含关系兜底有长度下限（避免短串误命中）")
    _check("短于下限时不启用包含规则（6 字历史）",
           interview_agent.question_similarity(
               "讲讲你的项目以及技术难点", "讲讲你的项目") < 1.0,
           str(interview_agent.question_similarity(
               "讲讲你的项目以及技术难点", "讲讲你的项目")))
    _check("达到下限时启用包含规则（8 字历史）",
           interview_agent.question_similarity(
               "请做一下自我介绍并说明职业规划", "请做一下自我介绍") == 1.0)
    _check("等长但不同的问题不受包含规则影响",
           interview_agent.question_similarity(
               "请说明 Redis 的持久化机制", "请说明 Redis 的复制原理") < 1.0)

    print("\n[4d] 与历史不重复时正常通过")
    spark = MockSpark(_question_json(
        question="在订单系统里，你如何用 Redis 做缓存与库存扣减的一致性保障？"))
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("全新问题一次通过", result["ok"] is True, str(result.get("error")))
    _check("未触发多余调用", spark.call_count == 1, str(spark.call_count))

    # ============================================================
    # [5] 缺少 topic
    # ============================================================
    print("\n[5] 缺少 topic")
    spark = MockSpark(_question_json(topic=""), _question_json())
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("首次缺 topic → 触发修复并成功", result["ok"] is True, str(result.get("error")))
    _check("缺 topic 时重试一次", spark.call_count == 2, str(spark.call_count))
    _check("修复提示词指出 topic 为空", "topic 不能为空" in spark.calls[1])

    spark = MockSpark(_question_json(topic="   "), _question_json(topic=""))
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("两次都缺 topic → ok=False", result["ok"] is False)
    _check("失败时 question 为空", result["question"] == "")
    _check("error 含 topic 提示", "topic 不能为空" in (result["error"] or ""),
           str(result["error"]))

    print("\n[5b] topic 字段完全缺失")
    payload = json.loads(_question_json())
    payload.pop("topic")
    missing_topic = json.dumps(payload, ensure_ascii=False)
    spark = MockSpark(missing_topic, missing_topic)
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("缺字段同样判为失败", result["ok"] is False, str(result.get("error")))

    # ============================================================
    # [6] difficulty
    # ============================================================
    print("\n[6] difficulty 校验")
    spark = MockSpark(_question_json(difficulty="god_mode"), _question_json())
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("非法 difficulty → 触发修复并成功", result["ok"] is True, str(result.get("error")))
    _check("修复提示词指出 difficulty 非法", "difficulty 取值非法" in spark.calls[1])

    spark = MockSpark(_question_json(difficulty="god_mode"), _question_json(difficulty="god_mode"))
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("两次都非法 → ok=False", result["ok"] is False)
    _check("error 列出合法取值",
           "junior/mid/senior" in (result["error"] or ""), str(result["error"]))

    payload = json.loads(_question_json())
    payload.pop("difficulty")
    spark = MockSpark(json.dumps(payload, ensure_ascii=False))
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("difficulty 缺失时用本场难度补齐（不算失败）", result["ok"] is True,
           str(result.get("error")))
    _check("补齐值为 plan 的 difficulty（mid）", result["difficulty"] == "mid",
           str(result["difficulty"]))
    _check("补齐不触发重试", spark.call_count == 1, str(spark.call_count))

    for level in DIFFICULTIES:
        spark = MockSpark(_question_json(difficulty=level))
        result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
        _check(f"合法难度 {level} 被接受", result["ok"] is True)

    # ============================================================
    # [7] 服务级故障：不重试，直接明确错误
    # ============================================================
    print("\n[7] 服务级故障")
    spark = MockSpark(RuntimeError("网络中断"))
    result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
    _check("调用抛异常 → ok=False", result["ok"] is False)
    _check("异常信息被上报", "网络中断" in (result["error"] or ""), str(result["error"]))
    _check("抛异常时不重试", spark.call_count == 1, str(spark.call_count))
    _check("失败时 question 为空", result["question"] == "")

    for label, sentinel in (
        ("API调用失败", "API调用失败: timeout"),
        ("SparkAPI not available", "SparkAPI not available"),
        ("AI 回答为空", "AI 回答为空"),
        ("空返回", ""),
    ):
        spark = MockSpark(sentinel)
        result = await interview_agent.generate_question(
            CONTEXT, plan, RESUME_TEXT, JOB, spark=spark)
        _check(f"哨兵串「{label}」→ ok=False", result["ok"] is False)
        _check(f"哨兵串「{label}」不重试", spark.call_count == 1, str(spark.call_count))
        _check(f"哨兵串「{label}」不伪造问题", result["question"] == "")

    original_default = interview_agent._default_spark
    interview_agent._default_spark = lambda: None
    try:
        result = await interview_agent.generate_question(CONTEXT, plan, RESUME_TEXT, JOB, spark=None)
    finally:
        interview_agent._default_spark = original_default
    _check("拿不到 Spark 实例 → ok=False", result["ok"] is False)
    _check("不可用时给出明确原因", "Spark 服务不可用" in (result["error"] or ""),
           str(result["error"]))

    # ============================================================
    # [8] 不伪造问题
    # ============================================================
    print("\n[8] 所有失败路径都不伪造问题")
    failure_cases = [
        MockSpark("不是 JSON", "还不是 JSON"),
        MockSpark(_question_json(question=""), _question_json(question="")),
        MockSpark(_question_json(topic=""), _question_json(topic="")),
        MockSpark(_question_json(difficulty="x"), _question_json(difficulty="x")),
        MockSpark(_question_json(question="请做一下自我介绍"), _question_json(question="请做一下自我介绍")),
        MockSpark(RuntimeError("boom")),
        MockSpark("API调用失败: x"),
    ]
    for index, mock in enumerate(failure_cases, 1):
        result = await interview_agent.generate_question(
            CONTEXT, plan, RESUME_TEXT, JOB, spark=mock)
        _check(f"失败用例 {index}：ok=False 且 question 为空",
               result["ok"] is False and result["question"] == "",
               f"{result['ok']} / {result['question']!r}")
        _check(f"失败用例 {index}：error 非空", bool(result["error"]))
        _check(f"失败用例 {index}：字段集与成功结果一致",
               set(result) == {"ok", "question", "question_type", "topic", "difficulty",
                               "expected_points", "reason", "error"})
    _check("failure() 构造的结果 question 恒为空",
           interview_agent.failure("x")["question"] == "")

    # ============================================================
    # [9] 查重与校验的单元测试
    # ============================================================
    print("\n[9] 查重算法")
    _check("完全相同 → 1.0",
           interview_agent.question_similarity("请做一下自我介绍", "请做一下自我介绍") == 1.0)
    _check("标点差异视为相同",
           interview_agent.question_similarity("请做一下自我介绍。", "请做一下自我介绍") == 1.0)
    _check("大小写差异视为相同",
           interview_agent.question_similarity("Explain Redis", "explain redis") == 1.0)
    _check("无关问题 → 低相似度",
           interview_agent.question_similarity("请做一下自我介绍", "Redis 的持久化机制是什么")
           < 0.3,
           str(interview_agent.question_similarity(
               "请做一下自我介绍", "Redis 的持久化机制是什么")))
    _check("空串 → 0.0", interview_agent.question_similarity("", "abc") == 0.0)
    _check("相似度在 0-1 之间",
           0.0 <= interview_agent.question_similarity("abc", "abcdef") <= 1.0)
    _check("is_duplicate_question：命中历史",
           interview_agent.is_duplicate_question("请做一下自我介绍", ["请做一下自我介绍"]) is True)
    _check("is_duplicate_question：未命中历史",
           interview_agent.is_duplicate_question("Redis 持久化", ["请做一下自我介绍"]) is False)
    _check("is_duplicate_question：空问题不算重复",
           interview_agent.is_duplicate_question("", ["请做一下自我介绍"]) is False)
    _check("is_duplicate_question：空历史不算重复",
           interview_agent.is_duplicate_question("请做一下自我介绍", []) is False)
    _check("is_duplicate_question：支持对象形态的历史记录",
           interview_agent.is_duplicate_question(
               "请做一下自我介绍",
               [{"question_no": 1, "question": "请做一下自我介绍", "stage": "introduction"}]) is True)
    similar_pair = ("请说明 Redis 的持久化机制", "请说明 Redis 的复制原理")
    sim = interview_agent.question_similarity(*similar_pair)
    _check("等长近似问题的相似度落在 (0.6, 0.7)", 0.6 < sim < 0.7, str(sim))
    _check("阈值 0.7 时不算重复",
           interview_agent.is_duplicate_question(
               similar_pair[1], [similar_pair[0]], threshold=0.7) is False)
    _check("阈值 0.6 时算重复",
           interview_agent.is_duplicate_question(
               similar_pair[1], [similar_pair[0]], threshold=0.6) is True)
    _check("默认阈值 0.85 下该近似问题不算重复",
           interview_agent.is_duplicate_question(
               similar_pair[1], [similar_pair[0]]) is False)

    print("\n[9b] validate_question 各分支")
    valid = {"question": "Q", "topic": "T", "difficulty": "mid"}
    _check("全部合法 → None", interview_agent.validate_question(valid) is None)
    _check("空 question → 报错",
           interview_agent.validate_question({**valid, "question": ""}) == "question 不能为空")
    _check("缺 question 键 → 报错",
           interview_agent.validate_question({"topic": "T", "difficulty": "mid"})
           == "question 不能为空")
    _check("空 topic → 报错",
           interview_agent.validate_question({**valid, "topic": ""}) == "topic 不能为空")
    _check("非法 difficulty → 报错",
           "difficulty 取值非法" in (interview_agent.validate_question(
               {**valid, "difficulty": "nope"}) or ""))
    _check("空 difficulty → 报错（不会静默通过）",
           "difficulty 取值非法" in (interview_agent.validate_question(
               {**valid, "difficulty": ""}) or ""))
    _check("重复问题 → 报错",
           "高度重复" in (interview_agent.validate_question(
               valid, asked_questions=["Q"]) or ""))
    _check("可注入自定义难度白名单",
           interview_agent.validate_question(
               {**valid, "difficulty": "expert"},
               allowed_difficulties=("expert",)) is None)

    # ============================================================
    # [10] 边界与降级
    # ============================================================
    print("\n[10] 边界与降级")
    variables = interview_agent.build_question_variables(CONTEXT, plan, RESUME_TEXT, JOB)
    _check("变量数量为 question.txt 要求的 10 个",
           set(variables) == {
               "resume_summary", "job_title", "job_description", "interview_type",
               "difficulty", "interview_plan", "current_stage", "asked_questions",
               "covered_topics", "weak_topics"},
           str(sorted(variables)))
    _check("current_stage 取自 context", variables["current_stage"] == "technical")
    _check("interview_type 取自 plan", variables["interview_type"] == "technical")
    _check("priority_topics 在计划载荷中",
           isinstance(variables["interview_plan"]["priority_topics"], list))
    _check("asked_questions 归一为文本列表",
           variables["asked_questions"] == CONTEXT["asked_questions"])

    bare = interview_agent.build_question_variables(EMPTY_CONTEXT, None, None, None)
    _check("无计划时回落默认面试类型", bare["interview_type"] == "comprehensive")
    _check("无计划时回落默认难度", bare["difficulty"] == "mid")
    _check("无简历时填（无简历）", bare["resume_summary"] == "（无简历）")
    _check("无岗位时填（未指定岗位）", bare["job_title"] == "（未指定岗位）")
    _check("无岗位描述时填占位", bare["job_description"] == "（未提供岗位描述）")
    _check("无 stage 时回落 introduction", bare["current_stage"] == "introduction")
    _check("空列表变量为空数组",
           bare["asked_questions"] == [] and bare["covered_topics"] == []
           and bare["weak_topics"] == [])

    spark = MockSpark(_question_json())
    result = await interview_agent.generate_question(EMPTY_CONTEXT, None, None, None, spark=spark)
    _check("无计划/无简历/无岗位仍可生成", result["ok"] is True, str(result.get("error")))
    _check("降级后 Prompt 仍无残留占位符", "{{" not in spark.calls[0])

    # ORM 对象输入
    class _Orm:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    orm_context = _Orm(
        current_stage="project",
        asked_questions=[{"question": "讲讲你的项目", "stage": "project"}],
        covered_topics=["Java"],
        weak_topics=[],
    )
    orm_resume = _Orm(content="项目经历\n电商项目\n")
    orm_job = _Orm(job_name="后端工程师", skills="Go", duty="服务开发")
    spark = MockSpark(_question_json(question="全新的问题内容", topic="新知识点"))
    result = await interview_agent.generate_question(
        orm_context, plan, orm_resume, orm_job, spark=spark)
    _check("支持 ORM 对象作为输入", result["ok"] is True, str(result.get("error")))
    _check("ORM 的 current_stage 被读取", "当前阶段：project" in spark.calls[0])
    _check("ORM 的简历正文被读取", "电商项目" in spark.calls[0])
    _check("ORM 的岗位字段被读取", "后端工程师" in spark.calls[0])
    _check("对象形态的历史问题参与查重",
           "- 讲讲你的项目" in spark.calls[0])

    print("\n[10b] 职责边界")
    params = inspect.signature(interview_agent.generate_question).parameters
    _check("generate_question 不接收 db 会话（本阶段不碰数据库）", "db" not in params,
           str(list(params)))
    _check("generate_question 是协程函数", inspect.iscoroutinefunction(
        interview_agent.generate_question))
    _check("重试上限为 1（用户要求最多一次修复）",
           interview_agent.MAX_REPAIR_ATTEMPTS == 1)
    _check("未提供评分/追问/报告等越界接口",
           not any(hasattr(interview_agent, name) for name in (
               "score_answer", "generate_report", "next_question", "decide_next",
               "follow_up", "build_report")))

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
