# -*- coding: utf-8 -*-
"""AI 面试 RAG 测试数据集 · 可用性自检（脚本式，非 pytest）。

数据集文件：``backend/scripts/interview_knowledge.json``

运行：``python backend/tests/test_interview_knowledge_dataset.py``

本套件**只验证数据集本身**——不新增业务能力、不改任何生产代码
（``Retriever`` / ``Agent`` / ``InterviewCore`` 一行都不碰）。
它回答需求点名的三个问题，外加结构契约与隔离取证：

[1] 数据集结构与规模（22 篇 × 恰好 4 键 / category 合法 / 正文非空 / 自然键唯一）
[2] **测试 1 · 数据可以入库**——走真实 ``KnowledgeImportPipeline``，不是手搓记录
[3] **测试 2 · Chunk 正常生成**——每篇 ≥ 1 片、多片文档确实存在、``chunk_index`` 连续
[4] **测试 3 · Embedding 正常生成**——``embedded_chunks == chunk_count``、
    向量非空 / 维度一致 / ``embedding_model`` 已记、``embedding IS NULL`` 为 0
[5] 幂等复跑：同一批数据再跑一次 → 全部 ``skipped`` 且行数不变
[6] 隔离守卫：生产代码不引用本数据集；本套件不 import 三个受保护模块

.. warning::
   **当前环境未配置任何 ``EMBEDDING_*``**（``backend/.env`` 里只有 ``DATABASE_URL``
   与 ``SPARK_*``），因此 ``knowledge_rag.default_embedder()`` 返回的是
   :class:`~services.embedding_service.HashEmbeddingService`——**离线哈希占位，
   不是语义向量**。「Embedding 正常生成」在本环境下的准确含义是
   **「链路与落库口径正确」**，不代表检索质量；配好 ``EMBEDDING_API_KEY``
   即为真实语义向量，**本数据集与链路一行都不用改**。
"""

from __future__ import annotations

import ast
import asyncio
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))
import regression_env  # noqa: E402,F401  钉住离线 Embedding + RAG 阈值（回归不受 .env 影响）
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from sqlalchemy import func, select  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import KnowledgeChunk, KnowledgeDocument  # noqa: E402
from models.knowledge import KNOWLEDGE_CATEGORIES, KNOWLEDGE_CATEGORY_LABELS  # noqa: E402
from services.document_chunker import (  # noqa: E402
    DEFAULT_CHUNK_OVERLAP,
    DEFAULT_CHUNK_SIZE,
)
from services.embedding_service import HashEmbeddingService  # noqa: E402
from services.knowledge_import_pipeline import (  # noqa: E402
    IMPORT_RESULT_FIELDS,
    KnowledgeImportPipeline,
    STAGE_DONE,
    STATUS_OK,
    STATUS_SKIPPED,
)

DATASET_PATH = BACKEND_DIR / "scripts" / "interview_knowledge.json"

#: 数据集每篇文档**恰好**这 4 个键（与 create_document 的入参契约一致）
DOC_KEYS = ("title", "content", "category", "source")

#: 本轮要求「不要修改」的三个模块（隔离守卫用）
PROTECTED_MODULES = (
    "services/interview_core.py",
    "services/interview_agent.py",
    "services/vector_knowledge_retriever.py",
)

_PASSED = 0
_FAILED = 0


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name
          + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def _section(title: str) -> None:
    print("\n" + "-" * 74)
    print(title)
    print("-" * 74)


# ============================================================
# 数据集读取 / 结构校验
# ============================================================
def load_dataset() -> Dict[str, Any]:
    """读 JSON（**只解析、不 eval**）。"""
    return json.loads(DATASET_PATH.read_text(encoding="utf-8"))


def check_structure(data: Dict[str, Any]) -> List[Dict[str, str]]:
    _section("[1] 数据集结构契约")

    _check("数据集文件存在", DATASET_PATH.is_file(), str(DATASET_PATH))
    _check("是合法 JSON 且顶层是对象", isinstance(data, dict))

    docs = data.get("documents")
    _check("documents 是非空列表", isinstance(docs, list) and bool(docs),
           repr(type(docs)))
    if not isinstance(docs, list) or not docs:
        return []

    bad_keys = [i for i, d in enumerate(docs) if set(d) != set(DOC_KEYS)]
    _check(f"每篇文档恰好 4 个键 {DOC_KEYS}", not bad_keys, str(bad_keys))
    _check("四个键的取值都是字符串",
           all(isinstance(d.get(k), str) for d in docs for k in DOC_KEYS))
    _check("title / content 去空白后非空",
           all(d["title"].strip() and d["content"].strip() for d in docs))
    _check("source 非空（便于溯源）", all(d["source"].strip() for d in docs))

    illegal = sorted({d["category"] for d in docs} - set(KNOWLEDGE_CATEGORIES))
    _check(f"category 全部合法（{KNOWLEDGE_CATEGORIES}）", not illegal, str(illegal))

    natural = [(d["title"], d["source"], d["category"]) for d in docs]
    _check("文档自然键 (title+source+category) 无重复",
           len(set(natural)) == len(natural))

    _check("顶层 category_labels 与 models 口径一致",
           data.get("category_labels") == dict(KNOWLEDGE_CATEGORY_LABELS),
           f"{data.get('category_labels')} != {dict(KNOWLEDGE_CATEGORY_LABELS)}")

    return docs


# ============================================================
# 引擎
# ============================================================
async def _build_engine():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _count(db: AsyncSession, model: Any) -> int:
    return int((await db.execute(select(func.count()).select_from(model))).scalar_one())


# ============================================================
# [2] 数据可以入库
# ============================================================
async def check_import(db: AsyncSession, docs: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    _section("[2] 测试 1 · 数据可以入库（真实 KnowledgeImportPipeline）")

    pipeline = KnowledgeImportPipeline(db)
    embedder = pipeline.embedder
    print(f"  使用的 Embedding 实现：{type(embedder).__name__} "
          f"(name={embedder.name!r}, dimension={embedder.dimension})")
    print(f"  使用的切片器：size={pipeline.chunker.chunk_size}, "
          f"overlap={pipeline.chunker.chunk_overlap}")

    reports: List[Dict[str, Any]] = []
    for index, doc in enumerate(docs):
        reports.append(await pipeline.import_document(doc))

    _check("报告键集恒为 IMPORT_RESULT_FIELDS（12 键）",
           all(set(r) == set(IMPORT_RESULT_FIELDS) for r in reports),
           str([sorted(set(r) ^ set(IMPORT_RESULT_FIELDS)) for r in reports
                if set(r) != set(IMPORT_RESULT_FIELDS)][:1]))

    failed = [(i, docs[i]["title"], r["status"], r["stage"], r["errors"])
              for i, r in enumerate(reports) if r["status"] != STATUS_OK]
    _check(f"全部 {len(docs)} 篇 status 均为 {STATUS_OK!r}", not failed, str(failed[:3]))
    _check("全部报告 ok=True", all(r["ok"] for r in reports))
    _check("全部报告 stage 跑到 DONE", all(r["stage"] == STAGE_DONE for r in reports),
           str([r["stage"] for r in reports if r["stage"] != STAGE_DONE]))
    _check("全部报告 errors 为空", all(r["errors"] == [] for r in reports),
           str([r["errors"] for r in reports if r["errors"]][:3]))
    _check("全部报告拿到 document_id", all(isinstance(r["document_id"], int) for r in reports))
    _check("全部报告 reused_document=False（首次入库全是新建）",
           all(r["reused_document"] is False for r in reports))

    doc_rows = await _count(db, KnowledgeDocument)
    chunk_rows = await _count(db, KnowledgeChunk)
    _check(f"knowledge_document 表实际行数 == {len(docs)}", doc_rows == len(docs),
           f"db={doc_rows}")
    _check("knowledge_chunk 表实际行数 == 报告 chunk_count 之和",
           chunk_rows == sum(r["chunk_count"] for r in reports),
           f"db={chunk_rows}, report={sum(r['chunk_count'] for r in reports)}")

    return reports


# ============================================================
# [3] Chunk 正常生成
# ============================================================
async def check_chunks(db: AsyncSession, docs: List[Dict[str, str]],
                       reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    _section("[3] 测试 2 · Chunk 正常生成")

    _check("每篇 chunk_count >= 1", all(r["chunk_count"] >= 1 for r in reports),
           str([(i, r["chunk_count"]) for i, r in enumerate(reports)
                if r["chunk_count"] < 1]))
    _check("saved_chunks == chunk_count（落库数与切片数一致）",
           all(r["saved_chunks"] == r["chunk_count"] for r in reports),
           str([(i, r["saved_chunks"], r["chunk_count"]) for i, r in enumerate(reports)
                if r["saved_chunks"] != r["chunk_count"]]))
    _check("skipped_chunks == 0（首次入库没有跳过）",
           all(r["skipped_chunks"] == 0 for r in reports))

    multi = [i for i, r in enumerate(reports) if r["chunk_count"] > 1]
    _check("确实存在被切成多片的文档（多片路径被真实走到）", bool(multi),
           f"multi={multi}")
    print(f"  多片文档：{len(multi)} 篇 / 共 {len(reports)} 篇")

    # 逐篇：切片序号连续 0..n-1、正文非空、长度 <= chunk_size
    # 注意：chunk_index 存在 JSON 列 chunk_metadata（DB 列名 metadata）里，不是独立列
    chunk_size = DEFAULT_CHUNK_SIZE
    problems = []
    for index, doc in enumerate(docs):
        doc_id = reports[index]["document_id"]
        rows = (await db.execute(
            select(KnowledgeChunk).where(KnowledgeChunk.document_id == doc_id)
        )).scalars().all()
        indexes = [int((r.chunk_metadata or {}).get("chunk_index", -1)) for r in rows]
        if sorted(indexes) != list(range(len(rows))):
            problems.append((doc["title"], "chunk_index 不连续", sorted(indexes)))
        for row in rows:
            if not (row.content or "").strip():
                problems.append((doc["title"], "空切片", row.chunk_metadata))
            if len(row.content) > chunk_size:
                problems.append((doc["title"], "超长切片", len(row.content)))
    _check("每篇切片 chunk_index 连续 0..n-1、正文非空、单片 <= chunk_size",
           not problems, str(problems[:3]))

    # 切片长度分布
    all_rows = (await db.execute(select(KnowledgeChunk))).scalars().all()
    lengths = sorted(len(r.content) for r in all_rows)
    stats = {
        "chunk_total": len(all_rows),
        "len_min": lengths[0] if lengths else 0,
        "len_max": lengths[-1] if lengths else 0,
        "len_avg": round(sum(lengths) / len(lengths), 1) if lengths else 0.0,
        "multi_docs": len(multi),
    }
    return stats


# ============================================================
# [4] Embedding 正常生成
# ============================================================
async def check_embeddings(db: AsyncSession, reports: List[Dict[str, Any]],
                           embedder: Any) -> Dict[str, Any]:
    _section("[4] 测试 3 · Embedding 正常生成")

    _check("每篇 embedded_chunks == chunk_count（逐片都编码了）",
           all(r["embedded_chunks"] == r["chunk_count"] for r in reports),
           str([(i, r["embedded_chunks"], r["chunk_count"]) for i, r in enumerate(reports)
                if r["embedded_chunks"] != r["chunk_count"]]))
    _check("failed_index 全为 None（没有一片编码失败）",
           all(r["failed_index"] is None for r in reports),
           str([(i, r["failed_index"]) for i, r in enumerate(reports)
                if r["failed_index"] is not None]))

    rows = (await db.execute(select(KnowledgeChunk))).scalars().all()
    null_embedding = [r.id for r in rows if r.embedding is None]
    _check("knowledge_chunk 中 embedding IS NULL 的行数为 0", not null_embedding,
           f"null={null_embedding[:5]}")

    _check("embedding_model 全部 == 本次 embedder.name",
           {r.embedding_model for r in rows} == {embedder.name},
           f"{ {r.embedding_model for r in rows} } vs {embedder.name!r}")
    _check("embedding_dim 全部 == embedder.dimension",
           {r.embedding_dim for r in rows} == {embedder.dimension},
           f"{ {r.embedding_dim for r in rows} } vs {embedder.dimension}")

    bad_shape = [r.id for r in rows
                 if not isinstance(r.embedding, list)
                 or len(r.embedding) != embedder.dimension
                 or not all(isinstance(x, (int, float)) and not isinstance(x, bool)
                            for x in r.embedding)]
    _check(f"每条向量都是长度 {embedder.dimension} 的数值数组（无 bool / 无嵌套）",
           not bad_shape, str(bad_shape[:5]))

    zero = [r.id for r in rows
            if all(float(x) == 0.0 for x in (r.embedding or []))]
    _check("没有全零向量（编码确实产生了信息）", not zero, str(zero[:5]))

    # 区分度：不同正文 → 不同向量（哈希占位也应满足）
    by_text = {r.content: tuple(r.embedding or ()) for r in rows}
    _check("不同切片正文对应不同向量（非常量输出）",
           len(set(by_text.values())) == len(by_text),
           f"distinct={len(set(by_text.values()))}/{len(by_text)}")

    return {"embedded_rows": len(rows) - len(null_embedding),
            "dimension": embedder.dimension,
            "embedder": f"{type(embedder).__name__}({embedder.name})",
            "offline_placeholder": isinstance(embedder, HashEmbeddingService)}


# ============================================================
# [5] 幂等复跑
# ============================================================
async def check_idempotent(db: AsyncSession, docs: List[Dict[str, str]],
                           before: Dict[str, int]) -> None:
    _section("[5] 幂等复跑（重复执行不重复写）")

    pipeline = KnowledgeImportPipeline(db)
    reports = [await pipeline.import_document(doc) for doc in docs]

    _check(f"复跑全部 status == {STATUS_SKIPPED!r}", 
           all(r["status"] == STATUS_SKIPPED for r in reports),
           str([(i, r["status"]) for i, r in enumerate(reports)
                if r["status"] != STATUS_SKIPPED][:3]))
    _check("复跑 ok=True（skipped 是成功的空操作，不是失败）",
           all(r["ok"] for r in reports))
    _check("复跑 reused_document=True（按自然键复用了既有文档）",
           all(r["reused_document"] is True for r in reports))
    _check("复跑 skipped_chunks == chunk_count",
           all(r["skipped_chunks"] == r["chunk_count"] for r in reports))

    after = {"doc": await _count(db, KnowledgeDocument),
             "chunk": await _count(db, KnowledgeChunk)}
    _check("复跑后 knowledge_document 行数不变",
           after["doc"] == before["doc"], f"{before['doc']} -> {after['doc']}")
    _check("复跑后 knowledge_chunk 行数不变",
           after["chunk"] == before["chunk"], f"{before['chunk']} -> {after['chunk']}")


# ============================================================
# [6] 隔离守卫
# ============================================================
def _source_mentions(path: Path, needle: str) -> bool:
    """``path`` 的源码里是否出现 ``needle``（**读文件，不是 import**）。"""
    return needle in path.read_text(encoding="utf-8")


def check_isolation() -> None:
    _section("[6] 隔离守卫（本轮只准备数据，不动生产代码）")

    # 守卫自身有效性：证明「子串检查」不是恒真的假守卫。
    # 反例 token **必须在运行时拼出来**——直接写成字面量会出现在本文件源码里，
    # 于是「应该查不到」的 token 被自己查到，守卫自检恒假（实测踩到）。
    absent = "interview_knowledge" + "_" + "NO" + "PE"
    _check("守卫自检：子串检查能区分「有」与「无」",
           _source_mentions(Path(__file__), "PROTECTED_MODULES") is True
           and _source_mentions(Path(__file__), absent) is False)

    offenders = [rel for rel in PROTECTED_MODULES
                 if _source_mentions(BACKEND_DIR / rel, "interview_knowledge")]
    _check("生产代码（Core / Agent / Retriever）不引用本数据集",
           not offenders, str(offenders))

    # 数据集本身不含代码（纯 JSON 数据，无 import / 无函数）
    raw = DATASET_PATH.read_text(encoding="utf-8")
    _check("数据集是纯数据（无 'import ' / 'def ' / 'lambda' 等代码痕迹）",
           not any(tok in raw for tok in ("import ", "def ", "lambda", "__")))

    # 本套件不 import 三个受保护模块（AST 看顶层，避免 docstring 误伤）
    own = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    imported = set()
    for node in own.body:
        if isinstance(node, ast.Import):
            imported.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    banned = {"services.interview_core", "services.interview_agent",
              "services.vector_knowledge_retriever", "services.interview_service"}
    _check("本套件顶层未 import 受保护的面试模块", not (imported & banned),
           str(sorted(imported & banned)))
    _check("本套件顶层只 import 数据集链路所需模块（含 models / database / services.*）",
           {"models", "database"} <= imported
           and any(m.startswith("services.") for m in imported),
           str(sorted(imported)))


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 74)
    print("AI 面试 RAG 测试数据集 · 可用性自检")
    print(f"数据集：{DATASET_PATH.relative_to(BACKEND_DIR.parent)}")
    print(f"切片默认：size={DEFAULT_CHUNK_SIZE}, overlap={DEFAULT_CHUNK_OVERLAP}")
    print("=" * 74)

    data = load_dataset()
    docs = check_structure(data)
    if not docs:
        print("\n数据集为空，后续检查跳过。")
        return False

    engine, session_factory = await _build_engine()
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        async with session_factory() as db:
            reports = await check_import(db, docs)
            embedder = KnowledgeImportPipeline(db).embedder
            chunk_stats = await check_chunks(db, docs, reports)
            emb_stats = await check_embeddings(db, reports, embedder)
            before = {"doc": await _count(db, KnowledgeDocument),
                      "chunk": await _count(db, KnowledgeChunk)}
            await check_idempotent(db, docs, before)

        check_isolation()

        # ---------------- 三节汇报 ----------------
        by_category = Counter(d["category"] for d in docs)
        chunks_by_category = Counter()
        for doc, rep in zip(docs, reports):
            chunks_by_category[doc["category"]] += rep["chunk_count"]

        print("\n" + "=" * 74)
        print("一、测试数据规模")
        print("=" * 74)
        print(f"  文档总数            : {len(docs)}")
        for cat in KNOWLEDGE_CATEGORIES:
            print(f"    - {cat:<10} ({KNOWLEDGE_CATEGORY_LABELS[cat]}) : "
                  f"{by_category.get(cat, 0)} 篇")
        print(f"  正文字符总数        : {sum(len(d['content']) for d in docs)}")
        print(f"  最短 / 最长正文     : {min(len(d['content']) for d in docs)} / "
              f"{max(len(d['content']) for d in docs)} 字符")
        print(f"  切片总数            : {chunk_stats['chunk_total']}")
        for cat in KNOWLEDGE_CATEGORIES:
            print(f"    - {cat:<10} : {chunks_by_category.get(cat, 0)} 片")
        print(f"  被切成多片的文档    : {chunk_stats['multi_docs']} 篇")
        print(f"  切片长度 min/avg/max: {chunk_stats['len_min']} / "
              f"{chunk_stats['len_avg']} / {chunk_stats['len_max']} 字符")

        print("\n" + "=" * 74)
        print("二、数据结构")
        print("=" * 74)
        print(f"  文件                : backend/scripts/interview_knowledge.json")
        print(f"  顶层键              : {sorted(data)}")
        print(f"  documents[] 每项    : {DOC_KEYS}（恰好 4 键）")
        print(f"  category 取值       : {KNOWLEDGE_CATEGORIES}")
        print(f"  category_labels     : {dict(KNOWLEDGE_CATEGORY_LABELS)}")
        print(f"  文档自然键          : (title, source, category)")
        print(f"  source 命名空间     :")
        for prefix in sorted({d["source"].split("://")[0] + "://" for d in docs}):
            n = sum(1 for d in docs if d["source"].startswith(prefix))
            print(f"    - {prefix:<12} {n} 条")
        print(f"  落库目标            : knowledge_document（主表）+ knowledge_chunk（切片）")

        print("\n" + "=" * 74)
        print("三、入库结果")
        print("=" * 74)
        print(f"  入库方式            : KnowledgeImportPipeline(db).import_document()")
        print(f"  Embedding 实现      : {emb_stats['embedder']}"
              + ("   ← 离线哈希占位（无语义）" if emb_stats["offline_placeholder"] else ""))
        print(f"  向量维度            : {emb_stats['dimension']}")
        print(f"  knowledge_document  : {before['doc']} 行")
        print(f"  knowledge_chunk     : {before['chunk']} 行（全部已向量化）")
        print(f"  embedding 已写入    : {emb_stats['embedded_rows']} 行")
        print(f"  embedding IS NULL   : 0 行（无待处理）")
        print(f"  入库失败            : 0 篇（status 全为 {STATUS_OK!r}）")
        print(f"  幂等复跑            : {len(docs)} 篇全部 {STATUS_SKIPPED!r}，行数不变")
        print("=" * 74)
        print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
        print("=" * 74)
        return _FAILED == 0

    finally:
        await engine.dispose()


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
