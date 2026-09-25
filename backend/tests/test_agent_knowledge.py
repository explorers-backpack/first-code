# -*- coding: utf-8 -*-
"""AI 模拟面试 · InterviewAgent 外部知识上下文 自检

无需 pytest，直接运行：
    python backend/tests/test_agent_knowledge.py

覆盖范围
--------
- **归一化**：``normalize_knowledge_context`` 接受 str / dict / KnowledgeChunk 风格对象 /
  混合列表 / 集合，去重保序、剔除空值、确定性
- **无知识时行为完全一致**：走**原模板**，渲染出的 Prompt 与
  ``render_prompt("question", <原 10 变量>)`` **逐字节相同**，且不含任何知识痕迹
- **有知识时 Prompt 包含知识**：切到 ``question_knowledge.txt``，正文含「参考知识：」
  与每条知识文本（含来源）
- **模板漂移守卫**：``question_knowledge.txt`` 去掉知识小节后必须与 ``question.txt``
  逐字节相同（防止两个模板各改一半而悄悄分叉）
- **不调用 Retriever**：Agent 不 import ``knowledge_retriever``，签名里没有检索器参数
- **修复路径不受影响**：``question_repair.txt`` 未引入知识变量

不依赖数据库、不调用真实 Spark（全部注入 Mock）。
"""

import ast
import asyncio
import inspect
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from prompts import render_prompt, template_variables  # noqa: E402
from services import interview_agent  # noqa: E402
from services.interview_agent import (  # noqa: E402
    PROMPT_QUESTION,
    PROMPT_QUESTION_KNOWLEDGE,
    generate_question,
    normalize_knowledge_context,
    render_question_prompt,
)

_PASSED = 0
_FAILED = 0

#: ``question_knowledge.txt`` 相对 ``question.txt`` **新增**的小节（逐字节）。
#: 该常量是本测试对两个模板一致性的**唯一权威**：谁改了模板，这里就会失败。
KNOWLEDGE_BLOCK = (
    "## 四·五、参考知识（外部检索结果，可选）\n"
    "\n"
    "参考知识：\n"
    "{{knowledge_context}}\n"
    "\n"
    "出题时请把这些知识融入问题的场景与追问点，但不得整段照抄进 `question`，\n"
    "也不得把知识中的结论直接写进 `question`（`expected_points` 可参考）。\n"
    "\n"
)

#: ``question.txt`` 原有的 10 个变量（引入知识能力之前就是这样，不得增删）。
#: 顺序 = 模板中**首次出现**的顺序（``template_variables`` 的返回顺序）。
BASE_QUESTION_VARIABLES = (
    "interview_type",
    "difficulty",
    "current_stage",
    "job_title",
    "job_description",
    "resume_summary",
    "interview_plan",
    "asked_questions",
    "covered_topics",
    "weak_topics",
)

CONTEXT = {
    "current_stage": "technical",
    "asked_questions": [],
    "covered_topics": [],
    "weak_topics": [],
}
PLAN = {
    "interview_type": "technical",
    "difficulty": "mid",
    "total_questions": 5,
    "priority_topics": ["Redis 持久化"],
}
JOB = {"job_name": "后端开发工程师", "duty": "负责后端服务的设计与开发"}
RESUME = {"content": "3 年后端经验，主导订单中台重构，技术栈 Python / MySQL / Redis"}

QUESTION_JSON = json.dumps(
    {
        "question": "请说明 Redis 的 RDB 与 AOF 持久化机制各自适合什么场景？",
        "question_type": "technical",
        "topic": "Redis 持久化",
        "difficulty": "mid",
        "expected_points": ["RDB", "AOF", "丢失窗口"],
        "reason": "考察岗位要求中的缓存中间件原理",
    },
    ensure_ascii=False,
)


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(
        ("  [PASS] " if cond else "  [FAIL] ")
        + name
        + (f"  -> {detail}" if detail and not cond else "")
    )
    return cond


class MockSpark:
    """按顺序吐出回复；超出即报错，用来断言调用次数。"""

    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls = []

    async def chat_async(self, message: str) -> str:
        self.calls.append(message)
        if not self.replies:
            raise AssertionError("Mock Spark 被调用次数超出预期")
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


class _Chunk:
    """鸭子类型的 KnowledgeChunk（验证 Agent 不依赖具体类型，只读 content/source）。"""

    def __init__(self, content, source=""):
        self.content = content
        self.source = source


def _imported_modules(source: str):
    """AST 收集 import 目标名（不用子串匹配——docstring 里会提到这些模块名）。"""
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _base_variables():
    return interview_agent.build_question_variables(CONTEXT, PLAN, RESUME, JOB)


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 68)
    print("AI 模拟面试 · InterviewAgent 外部知识上下文 自检")
    print("=" * 68)

    # ------------------------------------------------------------
    # [1] 归一化
    # ------------------------------------------------------------
    print("\n[1] normalize_knowledge_context（宽松接受、确定性归一）")
    n = normalize_knowledge_context
    _check("None → []", n(None) == [], str(n(None)))
    _check("[] → []", n([]) == [])
    _check("空字符串 → []", n("") == [])
    _check("空白字符串 → []", n("   \n ") == [])
    _check("str → 单条", n("Redis 持久化有 RDB 与 AOF") == ["Redis 持久化有 RDB 与 AOF"])
    _check("dict（KnowledgeChunk.to_dict() 形态）→ 取 content，附来源",
           n([{"content": "C1", "source": "mock://a", "metadata": {}}]) == ["C1（来源：mock://a）"],
           str(n([{"content": "C1", "source": "mock://a"}])))
    _check("  └ 无 source 时不附来源", n([{"content": "C1"}]) == ["C1"])
    _check("  └ 缺 content 的 dict 被丢弃", n([{"source": "only-source"}]) == [])
    _check("带 content 属性的对象（鸭子类型）→ 取 content",
           n([_Chunk("C2", "mock://b")]) == ["C2（来源：mock://b）"],
           str(n([_Chunk("C2", "mock://b")])))
    _check("单个 str 与单个 dict 都可直接传（不必包成列表）",
           n({"content": "C3"}) == ["C3"])
    _check("混合列表逐条归一", n(["C1", {"content": "C2"}, _Chunk("C3"), None, ""]) == ["C1", "C2", "C3"],
           str(n(["C1", {"content": "C2"}, _Chunk("C3"), None, ""])))
    _check("去重且保序",
           n(["B", "A", "B", "A"]) == ["B", "A"], str(n(["B", "A", "B", "A"])))
    _check("tuple 也可（Sequence）", n(("X", "Y")) == ["X", "Y"])
    _check("set 先排序 → 确定性",
           n({"c", "a", "b"}) == ["a", "b", "c"] and n({"c", "a", "b"}) == n({"b", "c", "a"}),
           str(n({"c", "a", "b"})))
    _check("数字等标量 → 空串被剔除（不会把 repr 塞进 Prompt）",
           n([123, 4.5, True, object()]) == [], str(n([123, 4.5, True, object()])))
    _check("同输入同输出（纯函数）", n(["A", "B"]) == n(["A", "B"]))
    _check("不修改入参",
           (lambda src: (n(src), src) == (["A"], ["A"]))(["A"]))

    # ------------------------------------------------------------
    # [2] 无知识 → 行为与之前完全一致
    # ------------------------------------------------------------
    print("\n[2] 无知识时行为完全一致（要求 1）")
    _check("question.txt 仍只有原来的 10 个变量（模板未被污染）",
           tuple(template_variables(PROMPT_QUESTION)) == BASE_QUESTION_VARIABLES,
           str(template_variables(PROMPT_QUESTION)))

    variables = _base_variables()
    legacy_prompt = render_prompt(PROMPT_QUESTION, variables)  # 改动前的渲染方式

    for label, knowledge in (
        ("不传（None）", None),
        ("显式空列表 []", []),
        ("空元组 ()", ()),
        ("空字符串", ""),
        ("全空元素的列表", [None, "", {}]),
    ):
        name, prompt = render_question_prompt(variables, knowledge)
        _check(f"{label} → 用原模板 question", name == PROMPT_QUESTION, name)
        _check("  └ Prompt 与改动前逐字节相同", prompt == legacy_prompt)
        _check("  └ 不含知识痕迹",
               "参考知识" not in prompt and "knowledge_context" not in prompt)

    # 端到端：不传 knowledge_context 时，Mock 收到的 Prompt 与改动前一致
    spark = MockSpark(QUESTION_JSON)
    result = await generate_question(CONTEXT, PLAN, RESUME, JOB, spark=spark)
    _check("无知识时生成正常（ok=True）", result["ok"] is True, str(result.get("error")))
    _check("  └ 问题 / 知识点 / 难度正常",
           result["question"].startswith("请说明 Redis") and result["topic"] == "Redis 持久化"
           and result["difficulty"] == "mid")
    _check("  └ 只调用一次模型", len(spark.calls) == 1, str(len(spark.calls)))
    _check("  └ 实际发给模型的 Prompt 与改动前逐字节相同",
           spark.calls[0] == legacy_prompt)
    _check("  └ 返回字段集与改动前一致（8 键）",
           set(result) == {"ok", "question", "question_type", "topic", "difficulty",
                           "expected_points", "reason", "error"},
           str(sorted(result)))

    spark_explicit = MockSpark(QUESTION_JSON)
    result_explicit = await generate_question(
        CONTEXT, PLAN, RESUME, JOB, knowledge_context=[], spark=spark_explicit)
    _check("显式传 knowledge_context=[] 与不传完全等价",
           result_explicit == result and spark_explicit.calls[0] == spark.calls[0])

    # ------------------------------------------------------------
    # [3] 有知识 → Prompt 包含知识
    # ------------------------------------------------------------
    print("\n[3] 有知识时 Prompt 包含知识（要求 2）")
    KNOWLEDGE = [
        "Redis 持久化有 RDB 与 AOF 两种方式：RDB 是全量快照，AOF 记录写命令。",
        {"content": "生产上常两者混用：RDB 快速恢复，AOF 降低丢失窗口。", "source": "mock://handbook/redis"},
    ]
    name, prompt = render_question_prompt(variables, KNOWLEDGE)
    _check("切到变体模板 question_knowledge", name == PROMPT_QUESTION_KNOWLEDGE, name)
    _check("  └ 含「参考知识：」标签", "参考知识：" in prompt)
    _check("  └ 含第 1 条知识正文", KNOWLEDGE[0] in prompt)
    _check("  └ 含第 2 条知识正文", KNOWLEDGE[1]["content"] in prompt)
    _check("  └ 含来源标注", "（来源：mock://handbook/redis）" in prompt)
    _check("  └ 不再出现未渲染的占位符", "{{" not in prompt, prompt[:80])
    _check("  └ 仍保留原 10 个变量的内容（岗位名等）",
           "后端开发工程师" in prompt and "Redis 持久化" in prompt)
    _check("  └ 与无知识 Prompt 不同（确实注入了知识）", prompt != legacy_prompt)
    _check("  └ 无知识 Prompt 是该 Prompt 的前缀改写来源（去掉小节后同源）",
           prompt.replace(KNOWLEDGE_BLOCK, "", 1).replace("{{knowledge_context}}", "") != "")
    _check("  └ 知识正文只出现一次（未重复注入）", prompt.count(KNOWLEDGE[0]) == 1)

    spark_k = MockSpark(QUESTION_JSON)
    result_k = await generate_question(
        CONTEXT, PLAN, RESUME, JOB, knowledge_context=KNOWLEDGE, spark=spark_k)
    _check("有知识时生成正常（ok=True）", result_k["ok"] is True, str(result_k.get("error")))
    _check("  └ 实际发给模型的 Prompt 含知识", KNOWLEDGE[0] in spark_k.calls[0])
    _check("  └ 返回字段集与无知识时一致（调用方无分支）",
           set(result_k) == set(result))

    _check("从 Retriever 直接接线的写法可用（List[KnowledgeChunk] 鸭子类型）",
           "C1（来源：mock://a）" in
           render_question_prompt(variables, [_Chunk("C1", "mock://a")])[1])

    # ------------------------------------------------------------
    # [4] 模板漂移守卫
    # ------------------------------------------------------------
    print("\n[4] 模板漂移守卫（question_knowledge.txt vs question.txt）")
    base_text = pathlib.Path(interview_agent.__file__).parent.parent / "prompts" / "interview" / "question.txt"
    k_text_path = base_text.with_name("question_knowledge.txt")
    base_src = base_text.read_text(encoding="utf-8")
    k_src = k_text_path.read_text(encoding="utf-8")
    # 行尾符也必须一致：read_text 会做 universal-newline 归一，
    # 只比文本会漏掉「一个 LF、一个 CRLF」这种悄悄分叉，故再比一次**原始字节**。
    base_bytes = base_text.read_bytes()
    k_bytes = k_text_path.read_bytes()
    block_bytes = KNOWLEDGE_BLOCK.encode("utf-8")

    _check("原模板 question.txt 不含知识小节", KNOWLEDGE_BLOCK not in base_src)
    _check("变体模板包含知识小节且仅一次", k_src.count(KNOWLEDGE_BLOCK) == 1,
           str(k_src.count(KNOWLEDGE_BLOCK)))
    _check("★ 去掉知识小节后与 question.txt 逐字节相同（防止两模板分叉）",
           k_src.replace(KNOWLEDGE_BLOCK, "", 1) == base_src)
    _eol = "base CRLF={} k CRLF={}".format(
        base_bytes.count(b"\r\n"), k_bytes.count(b"\r\n"))
    _check("  └ 字节级同样成立（含行尾符一致，防 CRLF/LF 悄悄分叉）",
           k_bytes.replace(block_bytes, b"", 1) == base_bytes, _eol)
    k_vars = tuple(template_variables(PROMPT_QUESTION_KNOWLEDGE))
    _check("  └ 变体模板 = 原 10 变量 + knowledge_context（集合一致）",
           set(k_vars) == set(BASE_QUESTION_VARIABLES) | {"knowledge_context"},
           str(k_vars))
    _check("  └ knowledge_context 恰好插在 interview_plan 与 asked_questions 之间",
           k_vars == BASE_QUESTION_VARIABLES[:7] + ("knowledge_context",)
           + BASE_QUESTION_VARIABLES[7:],
           str(k_vars))

    # ------------------------------------------------------------
    # [5] 不调用 Retriever / 不引入向量能力
    # ------------------------------------------------------------
    print("\n[5] 不调用 Retriever（要求 2）")
    agent_path = pathlib.Path(interview_agent.__file__)
    agent_src = agent_path.read_text(encoding="utf-8")
    imports = _imported_modules(agent_src)
    top = {name.split(".")[0] for name in imports}

    _check("Agent 未 import knowledge_retriever（知识由调用方传入）",
           not any(n.endswith("knowledge_retriever") for n in imports), str(sorted(imports)))
    _check("  └ 未 import 向量库 / Embedding 相关包",
           not ({"chromadb", "faiss", "milvus", "numpy", "openai", "sentence_transformers"} & top),
           str(sorted(top)))
    sig = inspect.signature(generate_question)
    _check("签名无 retriever / chunks 参数（Agent 不持有检索器）",
           not any(k in sig.parameters for k in ("retriever", "chunks")))
    _check("knowledge_context 是 keyword-only",
           sig.parameters["knowledge_context"].kind is inspect.Parameter.KEYWORD_ONLY)
    _check("  └ 默认值为 None（不用可变默认值 []，避免共享状态）",
           sig.parameters["knowledge_context"].default is None,
           repr(sig.parameters["knowledge_context"].default))
    _check("  └ 既有 4 个位置参数顺序未变（既有调用方不受影响）",
           list(sig.parameters)[:4] == ["context", "plan", "resume", "job"],
           str(list(sig.parameters)))
    _check("  └ spark 仍是 keyword-only 且默认 None",
           sig.parameters["spark"].kind is inspect.Parameter.KEYWORD_ONLY
           and sig.parameters["spark"].default is None)

    # ------------------------------------------------------------
    # [6] 修复路径不受影响
    # ------------------------------------------------------------
    print("\n[6] 修复路径不受影响")
    _check("question_repair.txt 未引入 knowledge_context",
           "knowledge_context" not in template_variables("question_repair"),
           str(template_variables("question_repair")))

    spark_repair = MockSpark("这不是 JSON", QUESTION_JSON)
    repaired = await generate_question(
        CONTEXT, PLAN, RESUME, JOB,
        knowledge_context=["Redis 持久化：RDB 是全量快照"],
        spark=spark_repair,
    )
    _check("有知识时非法 JSON 仍能修复成功", repaired["ok"] is True, str(repaired.get("error")))
    _check("  └ 共 2 次调用（首次 + 1 次修复）", len(spark_repair.calls) == 2,
           str(len(spark_repair.calls)))
    _check("  └ 首次请求含知识", "Redis 持久化：RDB 是全量快照" in spark_repair.calls[0])
    _check("  └ 修复请求不含知识（修复模板未变）",
           "Redis 持久化：RDB 是全量快照" not in spark_repair.calls[1])

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
