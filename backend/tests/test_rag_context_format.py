# -*- coding: utf-8 -*-
"""``knowledge_context`` 组装器（formatter）自检 —— 脚本式，非 pytest。

运行：``python backend/tests/test_rag_context_format.py``

本套件锁死的是**任务 72** 引入的组装口径：``interview_agent.format_knowledge_context``
是「检索结果 → 进 Prompt 的文本」的**唯一落点**，而
``interview_agent.normalize_knowledge_context`` 保持为**旧的退化形态**
（不分组、不去重叠、不截断）——后者被 ``rag_baseline_run`` 与多个套件当作
「改动前的期望值」，因此**必须逐字节不变**。

四条契约
--------
1. **只取 ``content`` 与 ``source``**：``metadata``（``score`` / ``chunk_id`` /
   ``embedding_model`` …）从不进 Prompt；数字 / 布尔等非知识标量一律丢弃。
2. **同源分组**：同一 ``source`` 的多片合并成一块，来源标注每个来源**只写一次**；
   组间顺序 = 各来源**首次出现**的顺序；无来源的条目各自成行。
3. **去切片重叠**：同源相邻片之间「上片结尾 == 下片开头」的重复文本被剥离
   （``document_chunker`` 默认留 80 字重叠）。
4. **总量上限**：``KNOWLEDGE_CONTEXT_MAX_CHARS``，按**整行**粒度截断并追加显式标记。

与 ``tests/test_rag_into_agent_prompt.py`` / ``tests/test_agent_knowledge.py`` 的分工
----------------------------------------------------------------------------------
那两份套件用**单条 / 单来源**的知识验证「注入本来就正确」；本套件补的是它们
**没有覆盖**的那一半：多片同源、切片重叠、总量上限，以及
「**除知识小节外，Prompt 逐字节不变**」这条结构不变性。
"""

from __future__ import annotations

import ast
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from prompts import render_prompt  # noqa: E402
from services import interview_agent  # noqa: E402
from services.interview_agent import (  # noqa: E402
    KNOWLEDGE_CONTEXT_MAX_CHARS,
    KNOWLEDGE_OVERLAP_MIN_CHARS,
    KNOWLEDGE_TRUNCATION_MARK,
    PROMPT_GROUP,
    PROMPT_QUESTION,
    PROMPT_QUESTION_KNOWLEDGE,
    format_knowledge_context,
    generate_question,
    normalize_knowledge_context,
    render_question_prompt,
)

# ============================================================
# 一、常量
# ============================================================
#: ★ 源码守卫要找的 token **一律运行时拼出**。
#: 若在源码里直接写出自己要找的 token，守卫会匹配到**自己这一行**——
#: 本项目已因这个坑复发 4 次（见 skill `career-ai-backend` §4）。
FORMATTER_NAME = "format_" + "knowledge_context"
LEGACY_NAME = "normalize_" + "knowledge_context"

AGENT_SOURCE_PATH = BACKEND_DIR / "services" / "interview_agent.py"

#: 模板里知识小节的上下界（用于「除知识小节外逐字节不变」的切片比对）。
KNOWLEDGE_HEADING = "## 四·五、参考知识（外部检索结果，可选）"
NEXT_HEADING = "## 五、本场进度（用于避免重复）"

#: ``render_question_prompt`` 需要的基础 10 变量（顺序即 ``build_question_variables``）。
BASE_VARIABLES: Dict[str, Any] = {
    "resume_summary": "3 年后端经验，技术栈 Python / MySQL / Redis。",
    "job_title": "后端开发工程师",
    "job_description": "负责订单中台的服务端开发与性能优化。",
    "interview_type": "technical",
    "difficulty": "mid",
    "interview_plan": {"difficulty": "mid", "total_questions": 5,
                       "priority_topics": ["MySQL 索引"], "stages": ["technical"]},
    "current_stage": "technical",
    "asked_questions": ["请说明你做过的一个高并发场景。"],
    "covered_topics": ["Redis 持久化"],
    "weak_topics": [],
}

#: 固定来源，避免测试里散落字面量。
SRC_A = "handbook://mysql/index"
SRC_B = "handbook://redis/persistence"

#: 切片重叠用的公共尾段（长度必须 ≥ :data:`KNOWLEDGE_OVERLAP_MIN_CHARS`）。
OVERLAP_TAIL = "联合索引遵循最左前缀原则，范围查询之后的列无法继续走索引。"

_PASSED = 0
_FAILED = 0


def _section(title: str) -> None:
    print("\n" + "-" * 74)
    print(title)
    print("-" * 74)


def _check(name: str, condition: Any, detail: str = "") -> bool:
    global _PASSED, _FAILED
    ok = bool(condition)
    if ok:
        _PASSED += 1
    else:
        _FAILED += 1
    tail = f"  -> {detail}" if detail else ""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{tail}")
    return ok


def _chunk(content: str, source: str = SRC_A,
           metadata: Any = None) -> Dict[str, Any]:
    """构造 ``KnowledgeChunk.to_dict()`` 形态的一条知识。"""
    return {"content": content, "source": source,
            "metadata": {} if metadata is None else metadata}


def _split_prompt(prompt: str) -> Tuple[str, str, str]:
    """把 Prompt 切成 ``(知识小节之前, 知识小节, 知识小节之后)``。"""
    start = prompt.index(KNOWLEDGE_HEADING)
    end = prompt.index(NEXT_HEADING)
    return prompt[:start], prompt[start:end], prompt[end:]


def _calls_in(source: str, function_name: str) -> set:
    """列出 ``source`` 里 ``function_name`` 函数体**直接出现**的被调用名集合。

    只做一层：找 ``ast.Call`` 的 ``func`` 是 ``Name``（``f(...)``）或
    ``Attribute``（``mod.f(...)``，取 ``attr``）。
    """
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            names = set()
            for sub in ast.walk(node):
                if not isinstance(sub, ast.Call):
                    continue
                func = sub.func
                if isinstance(func, ast.Name):
                    names.add(func.id)
                elif isinstance(func, ast.Attribute):
                    names.add(func.attr)
            return names
    return set()


class MockSpark:
    """记录 Prompt 的假 Spark（本套件只关心「发出去什么」）。"""

    def __init__(self, reply: str) -> None:
        self.reply = reply
        self.calls: List[str] = []

    async def chat_async(self, prompt: str, **_kwargs: Any) -> str:
        self.calls.append(prompt)
        return self.reply


QUESTION_JSON = json.dumps(
    {
        "question": "请说明 MySQL 联合索引的最左前缀原则，并给出一个走不到索引的例子。",
        "question_type": "technical",
        "topic": "MySQL 索引",
        "difficulty": "mid",
        "expected_points": ["最左前缀", "范围查询"],
        "reason": "考察岗位要求中的数据库优化能力",
    },
    ensure_ascii=False,
)


# ============================================================
# [1] 输入契约：与 normalize 完全一致（宽松接受、确定性、不塞 repr）
# ============================================================
def check_input_contract() -> None:
    _section("[1] 输入契约（与 normalize_knowledge_context 一致）")
    f = format_knowledge_context

    _check("None → []", f(None) == [], str(f(None)))
    _check("[] → []", f([]) == [])
    _check("空字符串 → []", f("") == [])
    _check("空白字符串 → []", f("   \n ") == [])
    _check("str → 单条", f("Redis 持久化有 RDB 与 AOF") == ["Redis 持久化有 RDB 与 AOF"])
    _check("dict（KnowledgeChunk.to_dict() 形态）→ 取 content，附来源",
           f([_chunk("C1")]) == [f"C1（来源：{SRC_A}）"], str(f([_chunk("C1")])))
    _check("  └ 无 source 时不附来源", f([{"content": "C1"}]) == ["C1"])
    _check("  └ 缺 content 的 dict 被丢弃", f([{"source": "only-source"}]) == [])
    _check("数字 / 布尔 / None / 任意对象 → 全部丢弃（不塞 repr）",
           f([123, 4.5, True, None, object()]) == [],
           str(f([123, 4.5, True, None, object()])))
    _check("★ 标量丢弃后仍保留真知识",
           f([123, "真知识", True]) == ["真知识"], str(f([123, "真知识", True])))
    _check("tuple 也可（Sequence）", f(("X", "Y")) == ["X", "Y"])
    _check("set 先按 str 排序 → 确定性",
           f({"c", "a", "b"}) == ["a", "b", "c"]
           and f({"c", "a", "b"}) == f({"b", "c", "a"}),
           str(f({"c", "a", "b"})))
    _check("同输入同输出（纯函数）", f([_chunk("A"), _chunk("B", SRC_B)])
           == f([_chunk("A"), _chunk("B", SRC_B)]))
    src = [_chunk("A")]
    f(src)
    _check("不修改入参", src == [_chunk("A")], str(src))


# ============================================================
# [2] 向后兼容：单条 / 单来源一片 ⇒ 与 normalize 逐字节相同
# ============================================================
def check_backward_compatible() -> None:
    _section("[2] 向后兼容（单条 / 每条来源仅一片 ⇒ 与 normalize 逐字节相同）")
    f, n = format_knowledge_context, normalize_knowledge_context

    cases: Sequence[Tuple[str, Any]] = (
        ("单条 dict（带来源）", [_chunk("唯一一条知识正文")]),
        ("单条 str", ["唯一一条知识正文"]),
        ("单条无来源 dict", [{"content": "唯一一条知识正文"}]),
        ("两条不同来源", [_chunk("甲", SRC_A), _chunk("乙", SRC_B)]),
        ("两条无来源", ["甲", "乙"]),
        ("混合：无来源 str + 带来源 dict",
         ["甲", _chunk("乙", SRC_B)]),
        ("空 / None", []),
    )
    for label, value in cases:
        _check(f"{label}：format == normalize", f(value) == n(value),
               f"format={f(value)} normalize={n(value)}")


# ============================================================
# [3] 同源分组（来源只写一次、组序 = 首次出现顺序）
# ============================================================
def check_grouping() -> None:
    _section("[3] 同源分组（来源标注每个来源只写一次）")

    chunks = [
        _chunk("第一片正文。", SRC_A),
        _chunk("第二片正文。", SRC_A),
        _chunk("第三片正文。", SRC_A),
    ]
    lines = format_knowledge_context(chunks)
    _check("★ 三片同源 ⇒ 合并为 1 行", len(lines) == 1, str(len(lines)))
    _check("★ 来源标注恰好出现 1 次", lines[0].count(f"（来源：{SRC_A}）") == 1,
           str(lines[0].count(f"（来源：{SRC_A}）")))
    _check("三片正文都在（顺序 = 输入顺序）",
           lines[0].startswith("第一片正文。第二片正文。第三片正文。"), repr(lines[0][:40]))
    _check("  └ 来源标注在块尾",
           lines[0].endswith(f"（来源：{SRC_A}）"), repr(lines[0][-30:]))

    # -- 组序 = 首次出现顺序（不是「按来源名排序」）--
    interleaved = [
        _chunk("A1", SRC_B), _chunk("B1", SRC_A), _chunk("A2", SRC_B),
    ]
    lines2 = format_knowledge_context(interleaved)
    _check("★ 组序 = 首次出现顺序（SRC_B 先出现 ⇒ 排在前面）",
           lines2 == [f"A1A2（来源：{SRC_B}）", f"B1（来源：{SRC_A}）"], str(lines2))

    # -- 无来源条目：各自成行（合并会把互不相关的知识粘成一行）--
    anon = ["甲", "乙", _chunk("丙", SRC_A)]
    lines3 = format_knowledge_context(anon)
    _check("★ 无来源条目各自成行（不被合并）",
           lines3 == ["甲", "乙", f"丙（来源：{SRC_A}）"], str(lines3))

    # -- 同源但内容完全相同的两片：去重叠后只剩一份 --
    dup = [_chunk("完全相同的正文。", SRC_A), _chunk("完全相同的正文。", SRC_A)]
    _check("★ 同源完全重复的两片只保留一份",
           format_knowledge_context(dup) == [f"完全相同的正文。（来源：{SRC_A}）"],
           str(format_knowledge_context(dup)))

    # -- 不同来源的相同正文**不**合并（溯源信息不同）--
    cross = [_chunk("相同正文。", SRC_A), _chunk("相同正文。", SRC_B)]
    _check("不同来源的相同正文各自保留（不跨来源去重）",
           len(format_knowledge_context(cross)) == 2,
           str(format_knowledge_context(cross)))


# ============================================================
# [4] 去切片重叠
# ============================================================
def check_overlap_stripping() -> None:
    _section("[4] 去切片重叠（chunker 的 80 字重叠）")

    head = "InnoDB 使用 B+ 树作为索引结构，非叶子节点只存键值不存数据。"
    tail = "用 EXPLAIN 判断索引是否生效，重点看 type / key / rows 三个字段。"
    piece_a = head + OVERLAP_TAIL
    piece_b = OVERLAP_TAIL + tail
    overlap_len = len(OVERLAP_TAIL)

    _check("前提：重叠段长度 ≥ KNOWLEDGE_OVERLAP_MIN_CHARS",
           overlap_len >= KNOWLEDGE_OVERLAP_MIN_CHARS,
           f"{overlap_len} vs {KNOWLEDGE_OVERLAP_MIN_CHARS}")

    merged = format_knowledge_context([_chunk(piece_a, SRC_A), _chunk(piece_b, SRC_A)])
    _check("★ 同源两片合并为 1 行", len(merged) == 1, str(len(merged)))
    _check("★ 重叠段只保留一次",
           merged[0].count(OVERLAP_TAIL) == 1, str(merged[0].count(OVERLAP_TAIL)))
    _check("★ 正文长度 == 两片之和 − 重叠段长度",
           len(merged[0]) == len(piece_a) + len(piece_b) - overlap_len
           + len(f"（来源：{SRC_A}）"),
           f"{len(merged[0])} vs {len(piece_a) + len(piece_b) - overlap_len}")
    _check("  └ 拼接结果 = head + 重叠段 + tail",
           merged[0] == f"{head}{OVERLAP_TAIL}{tail}（来源：{SRC_A}）",
           repr(merged[0][:60]))

    # -- 三片链式重叠：每一跳都只留一份 --
    # 真实切片链是「下片开头 == 上片结尾」，因此这里让每一片都以**上一片的尾段**开头。
    mid = "二级索引的叶子节点存放主键值，查非索引列需要回表。"
    chain_a = head + OVERLAP_TAIL
    chain_b = OVERLAP_TAIL + mid + tail
    chain_c = tail + "复合索引的列顺序应当把等值条件放前面、范围条件放后面。"
    chain = format_knowledge_context(
        [_chunk(chain_a, SRC_A), _chunk(chain_b, SRC_A), _chunk(chain_c, SRC_A)])
    _check("★ 三片链式重叠 ⇒ 合并为 1 行", len(chain) == 1, str(len(chain)))
    _check("★ 第一段重叠只出现 1 次", chain[0].count(OVERLAP_TAIL) == 1,
           str(chain[0].count(OVERLAP_TAIL)))
    _check("★ 第二段重叠只出现 1 次", chain[0].count(tail) == 1,
           str(chain[0].count(tail)))
    _check("  └ 三段正文按原顺序拼齐（内容不丢失）",
           chain[0] == f"{head}{OVERLAP_TAIL}{mid}{tail}"
                       "复合索引的列顺序应当把等值条件放前面、范围条件放后面。"
                       f"（来源：{SRC_A}）",
           repr(chain[0][:60]))

    # -- 短重复（< 16 字）**不**剥离：避免误删偶然相同的短串 --
    short_prev = "主键即聚簇索引"
    short_next = "索引的列顺序很重要"
    short_dup = format_knowledge_context(
        [_chunk(short_prev, SRC_A), _chunk(short_next, SRC_A)])
    _check(f"★ 短于 {KNOWLEDGE_OVERLAP_MIN_CHARS} 字的重复不剥离（防误删）",
           short_dup == [f"{short_prev}{short_next}（来源：{SRC_A}）"],
           str(short_dup))

    # -- 不同来源之间**不**做去重叠 --
    cross = format_knowledge_context(
        [_chunk(piece_a, SRC_A), _chunk(piece_b, SRC_B)])
    _check("★ 跨来源不做去重叠（重叠段各留一份）",
           cross[0].count(OVERLAP_TAIL) == 1 and cross[1].count(OVERLAP_TAIL) == 1,
           str([c.count(OVERLAP_TAIL) for c in cross]))


# ============================================================
# [5] 总量上限
# ============================================================
def check_length_cap() -> None:
    _section("[5] 总量上限（KNOWLEDGE_CONTEXT_MAX_CHARS）")
    cap = KNOWLEDGE_CONTEXT_MAX_CHARS

    _check("上限是正数常量", isinstance(cap, int) and cap > 0, str(cap))
    _check("★ 上限 ≥ 默认 top_k(5) × 默认切片(500) 的规模（正常检索不触发）",
           cap >= 5 * 500, f"{cap} vs {5 * 500}")

    # -- 单行超限：硬截 + 标记 --
    huge = "长" * (cap + 500)
    single = format_knowledge_context([{"content": huge}])
    _check("★ 单行超限 ⇒ 恰好 1 行", len(single) == 1, str(len(single)))
    _check("★ 该行以截断标记结尾", single[0].endswith(KNOWLEDGE_TRUNCATION_MARK),
           repr(single[0][-20:]))
    _check("★ 该行长度 == 上限（不溢出）", len(single[0]) == cap, str(len(single[0])))

    # -- 多行超限：整行粒度保留 + 追加标记行 --
    # 每行正文 LINE_LEN 字 + 来源后缀；上限 cap ⇒ 第 3 行放不下 ⇒ 共 2 行正文 + 1 标记行。
    LINE_LEN = 1500
    rows = [{"content": "行" * LINE_LEN, "source": f"mock://doc{i}"} for i in range(4)]
    capped = format_knowledge_context(rows)
    suffix_len = len("（来源：mock://doc0）")
    per_line = LINE_LEN + suffix_len
    _check("★ 多行超限 ⇒ 只保留放得下的整行 + 1 条标记行",
           len(capped) == 3,
           f"len={len(capped)}（{per_line}×2+1={per_line * 2 + 1} ≤ {cap} < "
           f"{per_line}×3+2={per_line * 3 + 2}）")
    _check("  └ 最后一行是截断标记", capped[-1] == KNOWLEDGE_TRUNCATION_MARK,
           repr(capped[-1]))
    _check("  └ 未截断任何保留行的正文（整行粒度）",
           all(len(line) == per_line for line in capped[:2]),
           str([len(line) for line in capped]))
    _check("★ 截断后的总长 ≤ 上限 + 标记长度",
           len("\n".join(capped)) <= cap + len(KNOWLEDGE_TRUNCATION_MARK),
           str(len("\n".join(capped))))

    # -- 恰好卡在上限：不触发截断 --
    exact_len = cap - len("（来源：mock://edge）")
    exact = format_knowledge_context(
        [{"content": "边" * exact_len, "source": "mock://edge"}])
    _check("★ 恰好等于上限 ⇒ 不截断（边界为闭区间）",
           exact == [f"{'边' * exact_len}（来源：mock://edge）"],
           f"len={len(exact[0])}")
    over_len = exact_len + 1
    over = format_knowledge_context(
        [{"content": "边" * over_len, "source": "mock://edge"}])
    _check("★ 超上限 1 字 ⇒ 触发截断",
           len(over) == 1 and over[0].endswith(KNOWLEDGE_TRUNCATION_MARK),
           f"len={len(over[0])}")


# ============================================================
# [6] 结构不变（只改知识小节）
# ============================================================
async def check_structure_unchanged() -> None:
    _section("[6] 结构不变（render_question_prompt 返回形状 / 只改知识小节）")

    # -- 同源两片（带切片重叠）+ 另一来源一片：旧口径 3 行、新口径 2 行 --
    piece_a = "InnoDB 使用 B+ 树作为索引结构。" + OVERLAP_TAIL
    piece_b = OVERLAP_TAIL + "用 EXPLAIN 判断索引是否生效。"
    chunks = [_chunk(piece_a, SRC_A), _chunk(piece_b, SRC_A),
              _chunk("Redis 有五种数据结构。", SRC_B)]

    result = render_question_prompt(BASE_VARIABLES, chunks)
    _check("★ 返回值仍是二元组", isinstance(result, tuple) and len(result) == 2, str(type(result)))
    name, prompt = result
    _check("★ 模板名仍是字符串常量", name == PROMPT_QUESTION_KNOWLEDGE, name)
    _check("★ Prompt 正文仍是 str", isinstance(prompt, str))

    _, empty_prompt = render_question_prompt(BASE_VARIABLES, None)
    _check("空知识仍走原模板", render_question_prompt(BASE_VARIABLES, None)[0] == PROMPT_QUESTION)
    _check("空知识 Prompt 与「直接渲染原模板」逐字节相同",
           empty_prompt == render_prompt(PROMPT_QUESTION, BASE_VARIABLES, group=PROMPT_GROUP))

    # -- 只改知识小节：前后两段必须逐字节不变 --
    old_lines = normalize_knowledge_context(chunks)
    old_prompt = render_prompt(
        PROMPT_QUESTION_KNOWLEDGE,
        {**BASE_VARIABLES, "knowledge_context": old_lines},
        group=PROMPT_GROUP,
    )
    pre_old, mid_old, post_old = _split_prompt(old_prompt)
    pre_new, mid_new, post_new = _split_prompt(prompt)
    _check("★ 知识小节**之前**的部分逐字节不变", pre_old == pre_new)
    _check("★ 知识小节**之后**的部分逐字节不变", post_old == post_new)
    _check("★ 知识小节确实变了（否则本优化无从体现）", mid_old != mid_new)
    _check("  └ 变短了", len(mid_new) < len(mid_old),
           f"{len(mid_new)} vs {len(mid_old)}")
    _check("无未渲染占位符", "{{" not in prompt)
    _check("★ 切片重叠段只出现 1 次（未重复注入）",
           prompt.count(OVERLAP_TAIL) == 1, str(prompt.count(OVERLAP_TAIL)))
    _check("  └ 两段正文都在且各 1 次",
           prompt.count("InnoDB 使用 B+ 树作为索引结构。") == 1
           and prompt.count("用 EXPLAIN 判断索引是否生效。") == 1)
    _check("  └ 来源标注每个来源各 1 次",
           prompt.count(f"（来源：{SRC_A}）") == 1
           and prompt.count(f"（来源：{SRC_B}）") == 1)

    # -- generate_question 的返回字段集不变（8 键）--
    context = {"current_stage": "technical", "asked_questions": [],
               "covered_topics": [], "weak_topics": []}
    plan = {"interview_type": "technical", "difficulty": "mid", "total_questions": 5,
            "priority_topics": ["MySQL 索引"]}
    spark = MockSpark(QUESTION_JSON)
    out = await generate_question(context, plan, {"content": "3 年后端经验"},
                                  {"job_name": "后端开发工程师"},
                                  knowledge_context=chunks, spark=spark)
    _check("★ generate_question 返回字段集恒为 8 键",
           set(out) == {"ok", "question", "question_type", "topic", "difficulty",
                        "expected_points", "reason", "error"}, str(sorted(out)))
    _check("  └ 出题成功", out["ok"] is True, str(out.get("error")))
    _check("  └ 只调用一次模型", len(spark.calls) == 1, str(len(spark.calls)))
    _check("  └ 实际发出的 Prompt 含合并后的知识（重叠已去）",
           "InnoDB 使用 B+ 树作为索引结构。" in spark.calls[0]
           and "用 EXPLAIN 判断索引是否生效。" in spark.calls[0]
           and "Redis 有五种数据结构。" in spark.calls[0]
           and spark.calls[0].count(OVERLAP_TAIL) == 1,
           str(spark.calls[0].count(OVERLAP_TAIL)))

    # -- 无知识时实际发出的 Prompt 仍与改动前一致 --
    spark_empty = MockSpark(QUESTION_JSON)
    await generate_question(context, plan, {"content": "3 年后端经验"},
                            {"job_name": "后端开发工程师"}, spark=spark_empty)
    _check("★ 无知识时实际 Prompt == 直接渲染原模板（逐字节）",
           spark_empty.calls[0] == render_prompt(PROMPT_QUESTION, {
               "resume_summary": "3 年后端经验", "job_title": "后端开发工程师",
               "job_description": "（未提供岗位描述）", "interview_type": "technical",
               "difficulty": "mid",
               "interview_plan": {"interview_type": "technical", "difficulty": "mid",
                                  "total_questions": 5, "stages": [],
                                  "target_topics": [], "priority_topics": ["MySQL 索引"],
                                  "resume_focus_points": []},
               "current_stage": "technical", "asked_questions": [],
               "covered_topics": [], "weak_topics": [],
           }, group=PROMPT_GROUP))


# ============================================================
# [7] metadata 不进 Prompt
# ============================================================
def check_metadata_excluded() -> None:
    _section("[7] metadata 不进 Prompt（只取 content 与 source）")

    meta = {"score": 0.987654321, "chunk_id": 42, "document_id": 7,
            "embedding_model": "hash-256", "index": 3}
    chunks = [_chunk("只有正文与来源该进 Prompt。", SRC_A, meta)]
    lines = format_knowledge_context(chunks)
    _check("★ 归一后恰好 1 行", len(lines) == 1, str(len(lines)))
    _check("★ 该行 == '- 正文（来源：…）'，无 metadata 痕迹",
           lines[0] == f"只有正文与来源该进 Prompt。（来源：{SRC_A}）", repr(lines[0]))

    _, prompt = render_question_prompt(BASE_VARIABLES, chunks)
    _check("★ score 数值不进 Prompt", str(meta["score"]) not in prompt)
    _check("★ metadata 的键名也不进 Prompt",
           not any(k in prompt for k in ("chunk_id", "document_id",
                                         "embedding_model", "score")),
           str(sorted(meta)))


# ============================================================
# [8] 源码守卫（防止 render_question_prompt 被改回旧口径）
# ============================================================
def check_source_guard() -> None:
    _section("[8] 源码守卫（render_question_prompt 用的是新 formatter）")

    source = AGENT_SOURCE_PATH.read_text(encoding="utf-8")
    calls = _calls_in(source, "render_question_prompt")
    _check(f"★ render_question_prompt 调用 {FORMATTER_NAME}",
           FORMATTER_NAME in calls, str(sorted(calls)))
    _check(f"  └ 不再直接调用 {LEGACY_NAME}（已退化为内部形态）",
           LEGACY_NAME not in calls, str(sorted(calls)))

    # -- 守卫自检：换一个实现必须能报「未使用」--
    positive = f"def render_question_prompt():\n    return {FORMATTER_NAME}(x)\n"
    negative = f"def render_question_prompt():\n    return {LEGACY_NAME}(x)\n"
    _check("★ 守卫自检：能找到正向写法",
           FORMATTER_NAME in _calls_in(positive, "render_question_prompt"))
    _check("★ 守卫自检：能找到反例写法（否则守卫恒为真、从未生效）",
           FORMATTER_NAME not in _calls_in(negative, "render_question_prompt"))

    _check("★ normalize_knowledge_context 仍是公开函数（基准/套件依赖它）",
           callable(getattr(interview_agent, LEGACY_NAME, None)))
    _check("★ format_knowledge_context 是公开函数",
           callable(getattr(interview_agent, FORMATTER_NAME, None)))


# ============================================================
# 入口
# ============================================================
async def main() -> int:
    print("=" * 74)
    print("AI 模拟面试 · knowledge_context 组装器（formatter）自检")
    print("=" * 74)

    check_input_contract()
    check_backward_compatible()
    check_grouping()
    check_overlap_stripping()
    check_length_cap()
    await check_structure_unchanged()
    check_metadata_excluded()
    check_source_guard()

    print("\n" + "=" * 74)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return 0 if _FAILED == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
