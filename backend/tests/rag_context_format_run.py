# -*- coding: utf-8 -*-
"""``knowledge_context`` 组装优化（任务 72）· 优化前后对比运行器（只读，非套件）。

运行：``python backend/tests/rag_context_format_run.py``
落盘：``backend/scripts/rag_context_format.json``

它回答用户提的三个问题，并给出**可复核的数字**：

1. **哪些字段模型真正需要** —— 逐场景列出「检索器返回了什么」与「实际进了 Prompt 的
   是什么」，证明 ``metadata``（``score`` / ``chunk_id`` / 模型名）**从未**进 Prompt。
2. **是否存在重复内容** —— 量化「同源跨片切片重叠」（``document_chunker`` 的 80 字
   重叠）与「来源标注按片重复」，并给出优化后消除的字符数。
3. **是否需要限制最大长度** —— 用合成超长输入探针验证总量上限确实生效。

同时验证两条**不变性**：

- ``Retriever`` 返回结构不变（``List[KnowledgeChunk]``：``content`` / ``source`` /
  ``metadata``，条数与来源序列与优化前一致）；
- ``InterviewAgent`` 输出结构不变（``render_question_prompt`` 仍是
  ``(模板名, Prompt 正文)``；``generate_question`` 仍是 8 键；
  ``interview_core.generate_next_question`` 仍是 12 键）；
  且 Prompt 中**知识小节之外**的部分逐字节不变。

本文件**不是回归套件**（无 ``test_`` 前缀）：它会真正跑检索与出题，耗时长；
回归口径由 ``tests/test_rag_context_format.py`` 承担。
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Iterator, List, Optional, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
PROJECT_DIR = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import InterviewSession, Job, Resume, User  # noqa: E402
from prompts import render_prompt  # noqa: E402
from services import interview_agent, interview_context, interview_core  # noqa: E402
from services.interview_agent import (  # noqa: E402
    KNOWLEDGE_CONTEXT_MAX_CHARS,
    KNOWLEDGE_OVERLAP_MIN_CHARS,
    KNOWLEDGE_TRUNCATION_MARK,
    PROMPT_GROUP,
    PROMPT_QUESTION_KNOWLEDGE,
    build_question_variables,
    format_knowledge_context,
    normalize_knowledge_context,
)
from services.knowledge_import_pipeline import KnowledgeImportPipeline  # noqa: E402
from services.knowledge_rag import default_embedder  # noqa: E402
from services.knowledge_retriever import KnowledgeChunk  # noqa: E402
from services.vector_knowledge_retriever import VectorKnowledgeRetriever  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

# ============================================================
# 一、常量
# ============================================================
BENCHMARK_PATH = BACKEND_DIR / "scripts" / "interview_eval_benchmark.json"
CORPUS_PATH = BACKEND_DIR / "scripts" / "interview_knowledge.json"
RESULT_PATH = BACKEND_DIR / "scripts" / "rag_context_format.json"

KNOWLEDGE_HEADING = "## 四·五、参考知识（外部检索结果，可选）"
NEXT_HEADING = "## 五、本场进度（用于避免重复）"

#: 两臂对照：同一份召回、同一套 formatter 改动，只换阈值。
#: ``min_score=None`` 让非目标片也留在候选里 —— 用来证明「省下的是**重复**，
#: 不是靠丢内容」：两种配置下省掉的字符数应当**几乎一致**。
_DECLARED = object()
ARMS: Tuple[Dict[str, Any], ...] = (
    {"id": "declared", "label": "按基准声明阈值（s1/s2=0.25、s3=0.40）",
     "min_score": _DECLARED},
    {"id": "raw", "label": "不设阈值（min_score=None，保留全部 top_k 召回）",
     "min_score": None},
)

QUESTION_REPLY = json.dumps(
    {
        "question": "请结合你做过的高并发场景，说明 MySQL 索引设计上的取舍。",
        "question_type": "technical",
        "topic": "MySQL 索引",
        "difficulty": "mid",
        "expected_points": ["最左前缀", "回表", "覆盖索引"],
        "reason": "考察岗位要求中的数据库优化能力",
    },
    ensure_ascii=False,
)

AGENT_KEYS = {"ok", "question", "question_type", "topic", "difficulty",
              "expected_points", "reason", "error"}

#: ``KnowledgeChunk`` 值对象的字段名（检索器返回结构的判据）。
CHUNK_FIELD_NAMES = sorted(
    f.name for f in dataclasses.fields(KnowledgeChunk))


class CountingSpark:
    """记录每次 Prompt 的假 Spark（不出网，结果确定性）。"""

    def __init__(self, reply: str = QUESTION_REPLY) -> None:
        self.reply = reply
        self.calls: List[str] = []

    async def chat_async(self, prompt: str, **_kwargs: Any) -> str:
        self.calls.append(prompt)
        return self.reply


# ============================================================
# 二、环境（与既有基准运行器同一套固定条件）
# ============================================================
def _load_corpus() -> List[Dict[str, Any]]:
    return json.loads(CORPUS_PATH.read_text(encoding="utf-8"))["documents"]


def _load_scenarios(only: Optional[List[str]]) -> List[Dict[str, Any]]:
    data = json.loads(BENCHMARK_PATH.read_text(encoding="utf-8"))
    scenarios = data["scenarios"]
    if only:
        wanted = set(only)
        scenarios = [s for s in scenarios if s["id"] in wanted]
    return scenarios


@asynccontextmanager
async def _env(scenario: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    session: Optional[AsyncSession] = None
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        session = factory()

        session.add(User(username="task72", email="task72@example.com",
                         password_hash="x", role="user"))
        await session.flush()

        job = Job(**scenario["job"])
        resume = Resume(user_id=1, filename="resume.md",
                        content=scenario["candidate"]["resume_content"])
        session.add_all([job, resume])
        await session.flush()

        cfg = scenario["session"]
        row = InterviewSession(
            user_id=1, job_id=job.id, resume_id=resume.id, status="created",
            interview_type=cfg["interview_type"], difficulty=cfg["difficulty"],
            duration=cfg["duration"], total_questions=cfg["total_questions"],
            current_question_no=1,
        )
        session.add(row)
        await session.commit()
        session_id = row.id

        plan = await interview_core._build_session_plan(session, row)

        ctx = scenario["context"]
        await interview_context.create_context(session, session_id)
        await interview_context.update_context(
            session, session_id,
            current_stage=ctx["current_stage"],
            asked_questions=list(ctx["asked_questions"]),
            covered_topics=list(ctx["covered_topics"]),
            weak_topics=list(ctx["weak_topics"]),
        )
        context = await interview_context.get_context(session, session_id)

        embedder = default_embedder()
        store = SqlAlchemyVectorStore(session)
        pipeline = KnowledgeImportPipeline(session, embedder=embedder, store=store)
        reports = [await pipeline.import_document(dict(doc)) for doc in _load_corpus()]

        yield {
            "db": session, "row": row, "session_id": session_id,
            "job": job, "resume": resume, "plan": plan, "context": context,
            "embedder": embedder, "store": store, "reports": reports,
            "chunks_in_store": await store.count(),
        }
    finally:
        if session is not None:
            await session.close()
        await engine.dispose()


# ============================================================
# 三、工具
# ============================================================
def _split_prompt(prompt: str) -> Tuple[str, str, str]:
    """把 Prompt 切成 ``(知识小节之前, 知识小节, 知识小节之后)``。"""
    start = prompt.index(KNOWLEDGE_HEADING)
    end = prompt.index(NEXT_HEADING)
    return prompt[:start], prompt[start:end], prompt[end:]


def _chunk_record(chunk: Any) -> Dict[str, Any]:
    return {
        "source": chunk.source,
        "content_chars": len(chunk.content),
        "metadata_keys": sorted(chunk.metadata),
    }


def _overlap_chars(a: str, b: str, minimum: int = 16) -> int:
    """``a`` 的后缀与 ``b`` 的前缀的最长公共长度（≥ minimum 才算）。"""
    limit = min(len(a), len(b))
    for size in range(limit, minimum - 1, -1):
        if a.endswith(b[:size]):
            return size
    return 0


# ============================================================
# 四、单场景对比
# ============================================================
async def run_scenario(scenario: Dict[str, Any], arm: Dict[str, Any]) -> Dict[str, Any]:
    async with _env(scenario) as env:
        retrieval = scenario["retrieval"]
        top_k = retrieval["top_k"]
        declared = retrieval["min_score"]
        min_score = declared if arm["min_score"] is _DECLARED else arm["min_score"]
        topic = retrieval["query_topic"]

        retriever = VectorKnowledgeRetriever(env["embedder"], env["store"],
                                             top_k=top_k, min_score=min_score)
        chunks = await retriever.retrieve(env["job"], topic, env["context"])

        # ---- ① 检索器返回结构（优化不碰它）----
        retriever_output = {
            "type": type(chunks).__name__,
            "count": len(chunks),
            "sources": [c.source for c in chunks],
            "chunks": [_chunk_record(c) for c in chunks],
        }

        # ---- ② 旧口径（改动前的 formatter）----
        old_lines = normalize_knowledge_context(chunks)
        # ---- ③ 新口径（format_knowledge_context）----
        new_lines = format_knowledge_context(chunks)

        variables = build_question_variables(env["context"], env["plan"],
                                             env["resume"], env["job"])
        old_prompt = render_prompt(
            PROMPT_QUESTION_KNOWLEDGE,
            {**variables, "knowledge_context": old_lines},
            group=PROMPT_GROUP,
        )
        template_name, new_prompt = interview_agent.render_question_prompt(
            variables, chunks)

        old_pre, old_mid, old_post = _split_prompt(old_prompt)
        new_pre, new_mid, new_post = _split_prompt(new_prompt)

        # ---- ④ 重复内容量化 ----
        # 同源相邻片的「尾-首」重叠（真实检索顺序，逐对算）
        overlaps: List[Dict[str, Any]] = []
        for i in range(len(chunks) - 1):
            left, right = chunks[i], chunks[i + 1]
            if left.source and left.source == right.source:
                size = _overlap_chars(left.content, right.content)
                if size:
                    overlaps.append({"left_index": i, "right_index": i + 1,
                                     "source": left.source, "overlap_chars": size})
        # 同源但**不相邻**的片：优化后仍会被并到同一块，其重叠也算被消除
        for i in range(len(chunks)):
            for j in range(i + 1, len(chunks)):
                if chunks[i].source and chunks[i].source == chunks[j].source:
                    size = _overlap_chars(chunks[i].content, chunks[j].content)
                    if size and not any(o["left_index"] == i and o["right_index"] == j
                                        for o in overlaps):
                        overlaps.append({"left_index": i, "right_index": j,
                                         "source": chunks[i].source,
                                         "overlap_chars": size, "non_adjacent": True})

        source_suffix_old = sum(
            len(f"（来源：{c.source}）") for c in chunks if c.source)
        source_suffix_new = sum(
            len(f"（来源：{s}）") for s in dict.fromkeys(c.source for c in chunks)
            if s)

        # ---- ⑤ Agent / Core 输出结构 ----
        spark = CountingSpark()
        agent_out = await interview_agent.generate_question(
            env["context"], env["plan"], env["resume"], env["job"],
            knowledge_context=chunks, spark=spark)
        core_spark = CountingSpark()
        core_out = await interview_core.generate_next_question(
            env["db"], env["session_id"],
            context=env["context"], plan=env["plan"],
            retriever=retriever, spark=core_spark)

        agent_prompt = spark.calls[0] if spark.calls else ""
        core_prompt = core_spark.calls[0] if core_spark.calls else ""
        c_pre, c_mid, c_post = _split_prompt(core_prompt) if core_prompt else ("", "", "")

        return {
            "scenario_id": scenario["id"],
            "label": scenario.get("label", scenario["id"]),
            "arm": arm["id"],
            "arm_label": arm["label"],
            "conditions": {
                "query_topic": topic,
                "top_k": top_k,
                "min_score": min_score,
                "min_score_declared": declared,
                "corpus_documents": len(_load_corpus()),
                "chunks_in_store": env["chunks_in_store"],
            },
            "retriever_output": retriever_output,
            "old": {
                "formatter": "normalize_knowledge_context",
                "line_count": len(old_lines),
                "knowledge_block_chars": len("\n".join(old_lines)),
                "knowledge_section_chars": len(old_mid),
                "prompt_chars": len(old_prompt),
                "source_suffix_chars": source_suffix_old,
                "lines": old_lines,
            },
            "new": {
                "formatter": "format_knowledge_context",
                "line_count": len(new_lines),
                "knowledge_block_chars": len("\n".join(new_lines)),
                "knowledge_section_chars": len(new_mid),
                "prompt_chars": len(new_prompt),
                "source_suffix_chars": source_suffix_new,
                "lines": new_lines,
                "truncated": KNOWLEDGE_TRUNCATION_MARK in new_lines,
            },
            "delta": {
                "knowledge_block_chars": len("\n".join(old_lines))
                - len("\n".join(new_lines)),
                "knowledge_block_ratio": _ratio(len("\n".join(old_lines)),
                                                len("\n".join(new_lines))),
                "knowledge_section_chars": len(old_mid) - len(new_mid),
                "prompt_chars": len(old_prompt) - len(new_prompt),
                "prompt_ratio": _ratio(len(old_prompt), len(new_prompt)),
                "line_count": len(old_lines) - len(new_lines),
            },
            "duplicates": {
                "same_source_overlaps": overlaps,
                "overlap_chars_total": sum(o["overlap_chars"] for o in overlaps),
                "source_suffix_chars_saved": source_suffix_old - source_suffix_new,
            },
            "invariants": {
                "retriever_returns_list": isinstance(chunks, list),
                "chunk_class": type(chunks[0]).__name__ if chunks else None,
                "chunk_field_names": sorted(vars(chunks[0])) if chunks else [],
                "chunk_field_names_expected": CHUNK_FIELD_NAMES,
                "prompt_prefix_identical": old_pre == new_pre,
                "prompt_suffix_identical": old_post == new_post,
                "knowledge_section_changed": old_mid != new_mid,
                "template_name": template_name,
                "agent_keys": sorted(agent_out),
                "agent_keys_expected": agent_out.keys() == AGENT_KEYS,
                "agent_prompt_equals_rendered": agent_prompt == new_prompt,
                "core_keys": list(core_out),
                "core_keys_expected": tuple(core_out) == interview_core.QUESTION_RESULT_FIELDS,
                "core_prompt_prefix_identical": c_pre == new_pre,
                "core_prompt_suffix_identical": c_post == new_post,
                "core_knowledge_section_equals_agent":
                    c_mid == new_mid if core_prompt else None,
            },
        }


def _ratio(before: int, after: int) -> Optional[float]:
    if not before:
        return None
    return round((before - after) / before * 100, 2)


# ============================================================
# 五、上限探针（合成输入）
# ============================================================
def cap_probe() -> Dict[str, Any]:
    """验证总量上限确实生效（真实语料达不到上限，故用合成输入）。"""
    cap = KNOWLEDGE_CONTEXT_MAX_CHARS
    line_len = 1500
    rows = [{"content": "探" * line_len, "source": f"probe://doc{i}"}
            for i in range(6)]
    lines = format_knowledge_context(rows)
    return {
        "max_chars": cap,
        "input_rows": len(rows),
        "input_chars": sum(len(r["content"]) for r in rows),
        "kept_lines": len(lines),
        "output_chars": len("\n".join(lines)),
        "truncated": KNOWLEDGE_TRUNCATION_MARK in lines,
        "marker": KNOWLEDGE_TRUNCATION_MARK,
        "verdict": ("上限生效：超限时按整行粒度截断并追加标记"
                    if KNOWLEDGE_TRUNCATION_MARK in lines else "未触发（不应发生）"),
    }


# ============================================================
# 六、报告
# ============================================================
def _print_scenario(run: Dict[str, Any]) -> None:
    cond = run["conditions"]
    old, new, delta = run["old"], run["new"], run["delta"]
    print(f"\n  ── [{run['arm']}] {run['scenario_id']}（{run['label']}）")
    print(f"     条件：topic={cond['query_topic']!r} top_k={cond['top_k']} "
          f"min_score={cond['min_score']}（声明值 {cond['min_score_declared']}）"
          f" 语料={cond['corpus_documents']} 篇 / 入库 {cond['chunks_in_store']} 片")
    print(f"     召回：{run['retriever_output']['count']} 片，来源 "
          f"{run['retriever_output']['sources']}")
    print(f"     旧：{old['line_count']} 行 / 知识块 {old['knowledge_block_chars']} 字 / "
          f"小节 {old['knowledge_section_chars']} 字 / Prompt {old['prompt_chars']} 字")
    print(f"     新：{new['line_count']} 行 / 知识块 {new['knowledge_block_chars']} 字 / "
          f"小节 {new['knowledge_section_chars']} 字 / Prompt {new['prompt_chars']} 字")
    print(f"     Δ ：知识块 −{delta['knowledge_block_chars']} 字"
          f"（{delta['knowledge_block_ratio']}%）/ 小节 −{delta['knowledge_section_chars']} 字"
          f" / Prompt −{delta['prompt_chars']} 字（{delta['prompt_ratio']}%）"
          f" / 行数 −{delta['line_count']}")
    dup = run["duplicates"]
    if dup["same_source_overlaps"]:
        for o in dup["same_source_overlaps"]:
            tag = "（不相邻）" if o.get("non_adjacent") else ""
            print(f"       重叠：片[{o['left_index']}]↔片[{o['right_index']}] "
                  f"{o['source']} = {o['overlap_chars']} 字{tag}")
    else:
        print("       重叠：无同源跨片")
    print(f"       来源标注：{old['source_suffix_chars']} → "
          f"{new['source_suffix_chars']} 字（省 {dup['source_suffix_chars_saved']}）")
    inv = run["invariants"]
    print(f"     不变性：小节之前逐字节相同={inv['prompt_prefix_identical']} / "
          f"之后逐字节相同={inv['prompt_suffix_identical']} / "
          f"小节确有变化={inv['knowledge_section_changed']}")
    print(f"             Agent 8 键={inv['agent_keys_expected']} / "
          f"Core 12 键={inv['core_keys_expected']} / "
          f"Agent 实发 Prompt == 手工渲染={inv['agent_prompt_equals_rendered']}")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="knowledge_context 组装优化前后对比")
    parser.add_argument("--only", nargs="*", default=None, help="只跑指定场景 id")
    args = parser.parse_args(argv)

    scenarios = _load_scenarios(args.only)
    if not scenarios:
        print("没有匹配的场景", file=sys.stderr)
        return 2

    print("=" * 74)
    print("knowledge_context 组装优化（任务 72）· 优化前后对比")
    print("=" * 74)
    print(f"  基准：{BENCHMARK_PATH.relative_to(PROJECT_DIR)}")
    print(f"  语料：{CORPUS_PATH.relative_to(PROJECT_DIR)}")
    print(f"  旧口径：normalize_knowledge_context（不分组 / 不去重叠 / 不截断）")
    print(f"  新口径：format_knowledge_context（同源分组 + 去切片重叠 + 总量上限）")
    print(f"  总量上限：{KNOWLEDGE_CONTEXT_MAX_CHARS} 字")
    print(f"  对照臂：{len(ARMS)} 个 —— " + "；".join(a["label"] for a in ARMS))

    runs = [asyncio.run(run_scenario(s, arm)) for arm in ARMS for s in scenarios]

    for arm in ARMS:
        print("\n" + "-" * 74)
        print(f"逐场景 · 臂 [{arm['id']}] {arm['label']}")
        print("-" * 74)
        for run in [r for r in runs if r["arm"] == arm["id"]]:
            _print_scenario(run)

    probe = cap_probe()
    print("\n" + "-" * 74)
    print("总量上限探针（合成输入）")
    print("-" * 74)
    print(f"  输入 {probe['input_rows']} 行 / {probe['input_chars']} 字 "
          f"→ 输出 {probe['kept_lines']} 行 / {probe['output_chars']} 字")
    print(f"  上限 {probe['max_chars']} 字；截断标记出现={probe['truncated']}")
    print(f"  结论：{probe['verdict']}")

    def _totals(subset: List[Dict[str, Any]]) -> Dict[str, Any]:
        old_p = sum(r["old"]["prompt_chars"] for r in subset)
        new_p = sum(r["new"]["prompt_chars"] for r in subset)
        old_b = sum(r["old"]["knowledge_block_chars"] for r in subset)
        new_b = sum(r["new"]["knowledge_block_chars"] for r in subset)
        return {
            "scenarios": len(subset),
            "prompt_chars_old": old_p,
            "prompt_chars_new": new_p,
            "prompt_chars_saved": old_p - new_p,
            "prompt_ratio": _ratio(old_p, new_p),
            "knowledge_block_chars_old": old_b,
            "knowledge_block_chars_new": new_b,
            "knowledge_block_chars_saved": old_b - new_b,
            "knowledge_block_ratio": _ratio(old_b, new_b),
            "line_count_old": sum(r["old"]["line_count"] for r in subset),
            "line_count_new": sum(r["new"]["line_count"] for r in subset),
            "invariants_hold": all(
                r["invariants"]["prompt_prefix_identical"]
                and r["invariants"]["prompt_suffix_identical"]
                and r["invariants"]["agent_keys_expected"]
                and r["invariants"]["core_keys_expected"]
                for r in subset),
        }

    arm_summaries = {arm["id"]: _totals([r for r in runs if r["arm"] == arm["id"]])
                     for arm in ARMS}

    print("\n" + "=" * 74)
    print("汇总（逐臂）")
    print("=" * 74)
    for arm in ARMS:
        s = arm_summaries[arm["id"]]
        print(f"\n  臂 [{arm['id']}] {arm['label']}")
        print(f"    场景数：{s['scenarios']}")
        print(f"    Prompt 总长：{s['prompt_chars_old']} → {s['prompt_chars_new']} 字"
              f"（−{s['prompt_chars_saved']}，{s['prompt_ratio']}%）")
        print(f"    知识块总长：{s['knowledge_block_chars_old']} → "
              f"{s['knowledge_block_chars_new']} 字"
              f"（−{s['knowledge_block_chars_saved']}，{s['knowledge_block_ratio']}%）")
        print(f"    注入条数（行）：{s['line_count_old']} → {s['line_count_new']}")
        print(f"    全部不变性成立：{s['invariants_hold']}")

    payload = {
        "name": "knowledge_context 组装优化（任务 72）",
        "scope": "只改 context formatter；Retriever 与 InterviewAgent 输出结构不变",
        "old_formatter": "normalize_knowledge_context",
        "new_formatter": "format_knowledge_context",
        "max_chars": KNOWLEDGE_CONTEXT_MAX_CHARS,
        "overlap_min_chars": KNOWLEDGE_OVERLAP_MIN_CHARS,
        "benchmark": str(BENCHMARK_PATH.relative_to(PROJECT_DIR)),
        "corpus": str(CORPUS_PATH.relative_to(PROJECT_DIR)),
        "arms": [{"id": a["id"], "label": a["label"]} for a in ARMS],
        "runs": runs,
        "cap_probe": probe,
        "summary": {
            "arms": arm_summaries,
            "all_arms": _totals(runs),
        },
    }
    RESULT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"\n  已落盘：{RESULT_PATH.relative_to(PROJECT_DIR)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
