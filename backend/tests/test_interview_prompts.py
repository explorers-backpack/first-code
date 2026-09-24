# -*- coding: utf-8 -*-
"""AI 模拟面试 · Prompt 资源加载 / 变量注入 / JSON 解析 自检

无需 pytest，直接运行：
    python backend/tests/test_interview_prompts.py

被测对象是 ``prompts.loader``（不依赖数据库，只有最后一节为验证
「模板变量与调用点一致」而 import 了 ``services.interview_planner``）。

覆盖范围：
A. Prompt 加载：存在性、内容、列表、缓存、缺失、非法名称、目录穿越、空文件
B. 变量注入：完整注入、缺变量、多变量、非严格模式、单次替换不递归、取值归一
C. JSON 解析：纯 JSON、markdown 围栏、前后有解释文字、嵌套、花括号陷阱、各类非法输入
D. question 输出解析：成功形状、error 分支、空 question、字段归一、不做业务校验
E. Prompt 内容契约：system.txt 的 9 条约束、question.txt 的 10 个输入变量、
   question_repair.txt 的修复契约、planner.txt 与调用点一致性
"""

import asyncio
import json
import os
import pathlib
import sys
import tempfile

# 与其它后端测试保持一致；Prompt 测试本身不需要数据库，
# 仅 E 节为验证调用点一致性而 import 了 services.interview_planner。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import prompts  # noqa: E402
from prompts import loader  # noqa: E402

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


def _raises(fn, exc_type) -> bool:
    """断言调用抛出指定异常。"""
    try:
        fn()
        return False
    except exc_type:
        return True
    except Exception:  # noqa: BLE001 - 抛了别的异常也算失败
        return False


# question.txt 约定的 10 个输入变量（与用户需求逐字对应）
QUESTION_VARIABLES = (
    "resume_summary",
    "job_title",
    "job_description",
    "interview_type",
    "difficulty",
    "interview_plan",
    "current_stage",
    "asked_questions",
    "covered_topics",
    "weak_topics",
)

# system.txt 必须覆盖的 9 条面试官约束（标签 -> 文件中的特征表述）
SYSTEM_CONSTRAINTS = (
    ("专业", "专业"),
    ("自然", "自然"),
    ("根据简历提问", "根据候选人简历提问"),
    ("根据岗位提问", "根据目标岗位提问"),
    ("不连续重复问题", "不连续重复问题"),
    ("不提前泄露答案", "不提前泄露标准答案"),
    ("不进行无关聊天", "不进行无关聊天"),
    ("不虚构候选人经历", "不虚构候选人的经历"),
    ("不直接修改面试状态", "不直接修改面试状态"),
    ("输出严格结构化结果", "输出严格结构化结果"),
)


async def run() -> bool:
    print("=" * 70)
    print("AI 模拟面试 · Prompt 加载 / 注入 / JSON 解析 自检")
    print("=" * 70)

    # ============================================================
    # [A] Prompt 加载
    # ============================================================
    print("\n[A1] 加载四个 Prompt 文件")
    for name in ("system", "question", "question_repair", "planner"):
        text = prompts.load_prompt(name)
        _check(f"{name}.txt 可加载", isinstance(text, str) and bool(text.strip()))
        _check(f"{name}.txt 内容长度 > 200", len(text) > 200, str(len(text)))
    _check("list_prompts 返回全部四个",
           prompts.list_prompts() == ["planner", "question", "question_repair", "system"],
           str(prompts.list_prompts()))
    _check("list_prompts 不存在的分组返回空列表",
           prompts.list_prompts("nope") == [])

    print("\n[A2] 缺失与非法名称")
    _check("不存在的 prompt 抛 PromptNotFoundError",
           _raises(lambda: prompts.load_prompt("nope"), prompts.PromptNotFoundError))
    _check("空名称抛 PromptNotFoundError",
           _raises(lambda: prompts.load_prompt(""), prompts.PromptNotFoundError))
    _check("None 名称抛 PromptNotFoundError",
           _raises(lambda: prompts.load_prompt(None), prompts.PromptNotFoundError))
    _check("路径分隔符被拒（目录穿越）",
           _raises(lambda: prompts.load_prompt("../loader"), prompts.PromptNotFoundError))
    _check("上跳名称被拒",
           _raises(lambda: prompts.load_prompt(".."), prompts.PromptNotFoundError))
    _check("子目录穿越被拒",
           _raises(lambda: prompts.load_prompt("interview/system"),
                   prompts.PromptNotFoundError))
    _check("非法分组被拒",
           _raises(lambda: prompts.load_prompt("system", group="../"), prompts.PromptNotFoundError))
    _check("异常类型继承自 FileNotFoundError",
           issubclass(prompts.PromptNotFoundError, FileNotFoundError))
    _check("异常类型继承自 PromptError",
           issubclass(prompts.PromptNotFoundError, prompts.PromptError))

    print("\n[A3] 缓存行为（用临时根目录验证真实读盘）")
    original_root = loader._PROMPTS_ROOT
    with tempfile.TemporaryDirectory() as tmp:
        group_dir = pathlib.Path(tmp) / "interview"
        group_dir.mkdir(parents=True)
        target = group_dir / "probe.txt"
        target.write_text("v1 {{x}}", encoding="utf-8")

        loader._PROMPTS_ROOT = pathlib.Path(tmp)
        prompts.clear_cache()
        try:
            _check("首次读取拿到 v1", prompts.load_prompt("probe") == "v1 {{x}}")
            target.write_text("v2 {{x}}", encoding="utf-8")
            _check("命中缓存，仍是 v1（未重新读盘）",
                   prompts.load_prompt("probe") == "v1 {{x}}")
            _check("use_cache=False 时读到最新 v2",
                   prompts.load_prompt("probe", use_cache=False) == "v2 {{x}}")
            prompts.clear_cache()
            _check("clear_cache 后读到最新 v2",
                   prompts.load_prompt("probe") == "v2 {{x}}")

            empty = group_dir / "empty.txt"
            empty.write_text("   \n\n", encoding="utf-8")
            _check("空文件抛 PromptError",
                   _raises(lambda: prompts.load_prompt("empty"), prompts.PromptError))
            _check("list_prompts 只列出 .txt",
                   prompts.list_prompts() == ["empty", "probe"],
                   str(prompts.list_prompts()))
        finally:
            loader._PROMPTS_ROOT = original_root
            prompts.clear_cache()
    _check("临时根目录已还原", loader._PROMPTS_ROOT == original_root)

    print("\n[A4] 占位符提取")
    question_vars = prompts.template_variables("question")
    _check("question.txt 声明了全部 10 个输入变量（无缺无多）",
           set(question_vars) == set(QUESTION_VARIABLES),
           str(set(question_vars) ^ set(QUESTION_VARIABLES)))
    _check("question.txt 变量数量恰为 10", len(question_vars) == 10, str(len(question_vars)))
    _check("question.txt 无重复占位符", len(question_vars) == len(set(question_vars)))
    _check("planner.txt 提取到 8 个变量",
           len(prompts.template_variables("planner")) == 8,
           str(prompts.template_variables("planner")))
    _check("system.txt 无占位符（纯角色定义）",
           prompts.template_variables("system") == [])

    # ============================================================
    # [B] 变量注入
    # ============================================================
    print("\n[B1] 完整注入")
    variables = {
        "resume_summary": "3 年后端经验，主导电商项目，技术栈 Java / Spring Boot",
        "job_title": "Java 后端开发工程师",
        "job_description": "负责订单系统开发，要求熟悉 MySQL 与 Redis",
        "interview_type": "technical",
        "difficulty": "mid",
        "interview_plan": {"stages": [{"stage": "technical", "weight": 45}]},
        "current_stage": "technical",
        "asked_questions": ["请做一下自我介绍", "讲讲你负责的电商项目"],
        "covered_topics": ["Java", "Spring Boot"],
        "weak_topics": ["Redis 持久化"],
    }
    rendered = prompts.render_prompt("question", variables)
    _check("渲染后无残留占位符", "{{" not in rendered and "}}" not in rendered)
    _check("注入简历摘要", "主导电商项目" in rendered)
    _check("注入岗位名称", "Java 后端开发工程师" in rendered)
    _check("注入岗位描述", "熟悉 MySQL 与 Redis" in rendered)
    _check("注入面试类型", "technical" in rendered)
    _check("注入当前阶段", "当前阶段：technical" in rendered)
    _check("列表变量渲染成项目符号",
           "- 请做一下自我介绍" in rendered and "- 讲讲你负责的电商项目" in rendered)
    _check("dict 变量渲染成 JSON", '"weight": 45' in rendered)
    _check("单花括号的 JSON 示例未被当作占位符",
           '"question": "面试问题的完整文本"' in rendered)
    _check("渲染结果仍是长文本", len(rendered) > 800, str(len(rendered)))

    print("\n[B2] 严格模式双向校验")
    _check("缺变量抛 PromptRenderError",
           _raises(lambda: prompts.render_prompt("question", {"job_title": "x"}),
                   prompts.PromptRenderError))
    _check("多传未使用变量抛 PromptRenderError",
           _raises(lambda: prompts.render_prompt(
               "question", {**variables, "typo_variable": 1}),
               prompts.PromptRenderError))
    _check("异常继承自 ValueError",
           issubclass(prompts.PromptRenderError, ValueError))

    partial = prompts.render_prompt("question", {"job_title": "Java"}, strict=False)
    _check("非严格模式下缺变量保留原占位符", "{{job_title}}" not in partial and "Java" in partial)
    _check("非严格模式下未提供的变量保持原样", "{{resume_summary}}" in partial)
    _check("非严格模式下多传变量被忽略",
           isinstance(prompts.render_prompt(
               "question", {**variables, "extra": 1}, strict=False), str))

    print("\n[B3] 单次替换，不递归展开")
    sneaky = prompts.render_template(
        "值：{{value}}", {"value": "{{interview_type}} 与 {{unknown}}"}, strict=True
    )
    _check("变量值中的占位符不被二次展开",
           sneaky == "值：{{interview_type}} 与 {{unknown}}", sneaky)
    _check("system.txt 无变量时可空注入渲染",
           prompts.render_prompt("system", {}) == prompts.load_prompt("system"))
    _check("system.txt 传了变量会报错（严格模式）",
           _raises(lambda: prompts.render_prompt("system", {"x": 1}),
                   prompts.PromptRenderError))

    print("\n[B4] 取值归一 stringify")
    _check("None → （无）", prompts.stringify(None) == prompts.EMPTY_VALUE)
    _check("空列表 → （无）", prompts.stringify([]) == prompts.EMPTY_VALUE)
    _check("True → true", prompts.stringify(True) == "true")
    _check("False → false", prompts.stringify(False) == "false")
    _check("整数 → 字符串", prompts.stringify(30) == "30")
    _check("字符串原样", prompts.stringify("abc") == "abc")
    _check("标量列表 → 项目符号",
           prompts.stringify(["a", "b"]) == "- a\n- b", prompts.stringify(["a", "b"]))
    _check("嵌套列表 → JSON",
           '"weight": 45' in prompts.stringify([{"weight": 45}]))
    _check("dict → JSON", '"k": "v"' in prompts.stringify({"k": "v"}))
    _check("空 dict 仍可序列化", prompts.stringify({}) == "{}")

    # ============================================================
    # [C] JSON 解析
    # ============================================================
    print("\n[C1] 正常解析")
    _check("纯 JSON", prompts.extract_json_object('{"a": 1}') == {"a": 1})
    _check("markdown 围栏（json 标注）",
           prompts.extract_json_object('```json\n{"a": 1}\n```') == {"a": 1})
    _check("markdown 围栏（无标注）",
           prompts.extract_json_object('```\n{"a": 1}\n```') == {"a": 1})
    _check("JSON 前有解释文字",
           prompts.extract_json_object('好的，结果如下：\n{"a": 1}') == {"a": 1})
    _check("JSON 后有解释文字",
           prompts.extract_json_object('{"a": 1}\n以上就是我的回答。') == {"a": 1})
    _check("前后都有文字",
           prompts.extract_json_object('结果：{"a": 1}\n请查收。') == {"a": 1})
    _check("嵌套对象",
           prompts.extract_json_object('{"a": {"b": [1, 2]}}') == {"a": {"b": [1, 2]}})
    _check("字符串值内含花括号",
           prompts.extract_json_object('{"a": "用 {x} 表示"}') == {"a": "用 {x} 表示"})
    _check("返回多个对象时取第一个",
           prompts.extract_json_object('{"a": 1} {"b": 2}') == {"a": 1})
    _check("中文未被转义为 \\u",
           prompts.extract_json_object('{"topic": "分布式系统"}')["topic"] == "分布式系统")

    print("\n[C2] 非法输入")
    for label, bad in (
        ("空字符串", ""),
        ("纯空白", "   \n "),
        ("纯自然语言", "抱歉，我无法完成这个任务。"),
        ("JSON 数组（非对象）", '["Java", "MySQL"]'),
        ("JSON 标量", "42"),
        ("JSON 被截断", '{"target_topics": ["Java"'),
        ("只有左花括号", "{"),
        ("None", None),
        ("非字符串（dict）", {"a": 1}),
        ("Spark 失败哨兵串", "API调用失败: timeout"),
    ):
        _check(f"{label} → PromptJSONError",
               _raises(lambda b=bad: prompts.extract_json_object(b),
                       prompts.PromptJSONError))
    _check("异常继承自 ValueError",
           issubclass(prompts.PromptJSONError, ValueError))
    try:
        prompts.extract_json_object("完全不是 JSON 的一段话")
    except prompts.PromptJSONError as exc:
        _check("错误信息含输出片段（便于排查）", "完全不是 JSON" in str(exc), str(exc))

    print("\n[C3] 代码块围栏剥离")
    _check("剥离 ```json", prompts.strip_code_fence("```json\n{}\n```") == "{}")
    _check("剥离 ```", prompts.strip_code_fence("```\n{}\n```") == "{}")
    _check("无围栏时原样返回", prompts.strip_code_fence('{"a":1}') == '{"a":1}')

    # ============================================================
    # [D] question 输出解析
    # ============================================================
    print("\n[D1] 成功形状")
    success = (
        '{"question": "请说明 Redis 持久化中 RDB 与 AOF 的取舍。",'
        ' "question_type": "technical", "topic": "Redis 持久化",'
        ' "difficulty": "mid", "expected_points": ["RDB 快照", "AOF 追加", "恢复速度", "数据安全"],'
        ' "reason": "补强薄弱知识点 Redis 持久化"}'
    )
    parsed = prompts.parse_question_output(success)
    _check("ok=True", parsed["ok"] is True)
    _check("question 解析正确",
           parsed["question"] == "请说明 Redis 持久化中 RDB 与 AOF 的取舍。")
    _check("question_type 解析正确", parsed["question_type"] == "technical")
    _check("topic 解析正确", parsed["topic"] == "Redis 持久化")
    _check("difficulty 解析正确", parsed["difficulty"] == "mid")
    _check("expected_points 解析为 4 项",
           parsed["expected_points"] == ["RDB 快照", "AOF 追加", "恢复速度", "数据安全"],
           str(parsed["expected_points"]))
    _check("reason 解析正确", parsed["reason"] == "补强薄弱知识点 Redis 持久化")
    _check("error 为 None", parsed["error"] is None)

    print("\n[D2] 失败形状")
    explicit = prompts.parse_question_output(
        '{"question": "", "error": "面试计划已全部完成，无需继续出题"}'
    )
    _check("显式 error → ok=False", explicit["ok"] is False)
    _check("error 原因被保留",
           explicit["error"] == "面试计划已全部完成，无需继续出题", str(explicit["error"]))
    _check("失败时 question 为空字符串", explicit["question"] == "")

    empty_q = prompts.parse_question_output('{"question": "   "}')
    _check("question 为空白 → ok=False", empty_q["ok"] is False)
    _check("question 为空白时给出可读原因",
           "question 为空" in (empty_q["error"] or ""), str(empty_q["error"]))

    broken = prompts.parse_question_output("模型超时，没有返回内容")
    _check("非法 JSON → ok=False", broken["ok"] is False)
    _check("非法 JSON 时 error 非空", bool(broken["error"]))

    sentinel = prompts.parse_question_output("API调用失败: connection reset")
    _check("Spark 哨兵串 → ok=False", sentinel["ok"] is False)
    _check("哨兵串不产生任何问题文本", sentinel["question"] == "")

    print("\n[D3] 字段归一与容错")
    partial = prompts.parse_question_output('{"question": "只有一个字段"}')
    _check("仅 question 时 ok=True", partial["ok"] is True)
    _check("缺失字段回落为空字符串",
           partial["question_type"] == "" and partial["topic"] == ""
           and partial["difficulty"] == "" and partial["reason"] == "")
    _check("缺失 expected_points 回落为空数组", partial["expected_points"] == [])

    dirty = prompts.parse_question_output(
        '{"question": "  问题前后有空白  ", "question_type": " technical ",'
        ' "expected_points": ["A", "A", "", 123, null, "B"],'
        ' "reason": "理由"}'
    )
    _check("question 首尾空白被 strip", dirty["question"] == "问题前后有空白")
    _check("question_type 首尾空白被 strip", dirty["question_type"] == "technical")
    _check("expected_points 丢弃空串与非字符串、去重",
           dirty["expected_points"] == ["A", "B"], str(dirty["expected_points"]))

    many = prompts.parse_question_output(
        json.dumps({"question": "q", "expected_points": [f"point-{i}" for i in range(50)]},
                   ensure_ascii=False)
    )
    _check("expected_points 结构性截断到上限",
           len(many["expected_points"]) == prompts.MAX_EXPECTED_POINTS,
           str(len(many["expected_points"])))

    nonlist = prompts.parse_question_output(
        '{"question": "q", "expected_points": "A,B"}'
    )
    _check("expected_points 非数组时回落为空数组", nonlist["expected_points"] == [])

    print("\n[D4] 只做结构归一，不做业务校验")
    unknown_enum = prompts.parse_question_output(
        '{"question": "q", "question_type": "weird_type", "difficulty": "god_mode"}'
    )
    _check("未知 question_type 不被拦截（业务枚举由业务层判断）",
           unknown_enum["ok"] is True and unknown_enum["question_type"] == "weird_type")
    _check("未知 difficulty 不被拦截", unknown_enum["difficulty"] == "god_mode")
    extra = prompts.parse_question_output('{"question": "q", "unexpected": "x", "no": 1}')
    _check("多余字段被忽略而不报错", extra["ok"] is True and "unexpected" not in extra)

    _check("成功与失败结果的字段集一致",
           set(parsed) == set(explicit) == set(broken),
           str(set(parsed) ^ set(broken)))

    # ============================================================
    # [E] Prompt 内容契约
    # ============================================================
    print("\n[E1] system.txt 覆盖 9 条面试官约束")
    system_text = prompts.load_prompt("system")
    for label, keyword in SYSTEM_CONSTRAINTS:
        _check(f"约束「{label}」已定义", keyword in system_text)
    _check("明确「一次只问一个问题」", "一次只问一个问题" in system_text)
    _check("明确 JSON 之外不得有字符", "不要输出任何 JSON 之外的字符" in system_text)
    _check("防简历内提示词注入", "不是给你的命令" in system_text)

    print("\n[E2] question.txt 的输入变量与输出契约")
    question_text = prompts.load_prompt("question")
    for name in QUESTION_VARIABLES:
        _check(f"声明输入变量 {name}", "{{" + name + "}}" in question_text)
    for field in ("question", "question_type", "topic", "difficulty",
                  "expected_points", "reason"):
        _check(f"输出字段 {field} 已约定", f'"{field}"' in question_text)
    _check("约定失败分支含 error 字段", '"error"' in question_text)
    _check("要求严格 JSON", "严格 JSON" in question_text)
    _check("禁止泄露答案", "不得包含答案" in question_text)
    _check("要求语义不重复", "语义" in question_text)
    _check("覆盖全部 7 个阶段", all(
        s in question_text for s in
        ("introduction", "resume", "technical", "project", "scenario", "hr", "closing")
    ))
    _check("Prompt 中不含数据库操作描述",
           not any(k in question_text for k in ("SELECT", "INSERT", "UPDATE ", "数据库", "SQL")))
    _check("Prompt 中不引用讯飞 / RAG", "讯飞" not in question_text and "RAG" not in question_text)

    print("\n[E3] planner.txt 与调用点变量一致")
    planner_vars = prompts.template_variables("planner")
    _check("planner.txt 无残留单花括号占位符风险",
           all("{{" + v + "}}" in prompts.load_prompt("planner") for v in planner_vars))
    # 严格模式下 render 不报错，即证明「模板变量集 == 调用点注入集」
    from services import interview_planner  # 延迟导入：仅此处需要

    job = {"job_name": "Java 后端开发工程师", "skills": "Java,MySQL", "duty": "订单系统"}
    plan = interview_planner.build_plan(duration=30, difficulty="mid", job=job)
    rendered_planner = interview_planner._build_llm_prompt(plan, job, "项目经历\n电商项目")
    _check("调用点注入集与模板变量集完全一致（严格模式未报错）",
           "{{" not in rendered_planner)
    _check("planner 提示词含岗位名", "Java 后端开发工程师" in rendered_planner)
    _check("planner 提示词含难度标签", "中级" in rendered_planner)
    _check("planner 提示词含时长与题量", "30 分钟" in rendered_planner and "12 题" in rendered_planner)
    _check("planner 提示词声明不出题", "不要生成任何面试题目" in rendered_planner)
    _check("planner 提示词声明 priority 为子集",
           "必须是 target_topics 的子集" in rendered_planner)
    _check("无简历时注入（无简历）",
           "（无简历）" in interview_planner._build_llm_prompt(plan, job, None))
    _check("无岗位时注入（未指定岗位）",
           "（未指定岗位）" in interview_planner._build_llm_prompt(plan, None, None))

    print("\n[E4] 目录结构")
    prompt_dir = pathlib.Path(__file__).resolve().parents[1] / "prompts" / "interview"
    _check("backend/prompts/interview 目录存在", prompt_dir.is_dir())
    for fname in ("system.txt", "question.txt", "question_repair.txt", "planner.txt"):
        _check(f"{fname} 存在于 prompts/interview/", (prompt_dir / fname).is_file())
    _check("loader 与 interview 分组分离（资产与代码不混放）",
           not list(prompt_dir.glob("*.py")))

    print("\n[E5] question_repair.txt 契约（供 Interview Agent 一次性修复使用）")
    repair_vars = prompts.template_variables("question_repair")
    _check("声明 3 个变量（error / raw_output / asked_questions）",
           set(repair_vars) == {"error", "raw_output", "asked_questions"},
           str(repair_vars))
    repair_text = prompts.load_prompt("question_repair")
    _check("要求只输出 JSON", "只输出一个合法的 JSON 对象" in repair_text)
    _check("给出失败分支形状", '"question": ""' in repair_text and '"error"' in repair_text)
    _check("列出全部合法 difficulty", "junior / mid / senior" in repair_text)
    _check("强调不得与已提问问题重复", "不得与其中任何一条重复" in repair_text)
    rendered_repair = prompts.render_prompt("question_repair", {
        "error": "topic 不能为空",
        "raw_output": '{"question": "q"}',
        "asked_questions": ["请做一下自我介绍"],
    })
    _check("修复提示词可渲染且无残留占位符",
           "{{" not in rendered_repair and "topic 不能为空" in rendered_repair)
    _check("修复提示词带上历史问题", "- 请做一下自我介绍" in rendered_repair)

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
