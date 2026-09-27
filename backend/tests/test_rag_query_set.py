# -*- coding: utf-8 -*-
"""AI 面试 RAG · **检索准确性固定 query 测试集**自检（脚本式，非 pytest）。

数据集文件：``backend/scripts/rag_query_set.json``（自带受控 corpus，共 16 篇 / 16 条 query）

运行：``python backend/tests/test_rag_query_set.py``

对应用户点名的两条测试：

[1] **数据格式校验**——每条 query 恰好 5 键、类型合法、category 合法、
    top_k / min_score 合法、关键词非空、query 不重复、三类知识都有覆盖
[2] **可以被测试读取**——能独立加载、能算出规模与分布、corpus 每篇恰好 1 片
[3] 端到端检索准确性：把 corpus 导入**内存 SQLite**，逐条 query 走真实
    ``VectorKnowledgeRetriever``，按 expectation_contract 四条断言并输出命中表
[4] 边界：**不修改业务代码**（Retriever / VectorStore / Embedding / InterviewAgent
    一行未改，且都不认识本测试集）、**不依赖生产知识库**

.. note::
   本套件**只调用**生产代码，不修改它。corpus 是文件自带的受控语料，
   测试全程用内存 SQLite，**不读生产 MySQL 的 knowledge_document / knowledge_chunk**。
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple

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
from models.knowledge import KNOWLEDGE_CATEGORIES  # noqa: E402
from services import interview_agent as agent_module  # noqa: E402
from services.document_chunker import DEFAULT_CHUNK_SIZE  # noqa: E402
from services.embedding_service import EmbeddingService  # noqa: E402
from services.knowledge_import_pipeline import (  # noqa: E402
    KnowledgeImportPipeline,
    STATUS_OK,
)
from services.knowledge_rag import default_embedder  # noqa: E402
from services.vector_knowledge_retriever import VectorKnowledgeRetriever  # noqa: E402
from services.vector_store import VectorStore  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

SET_PATH = BACKEND_DIR / "scripts" / "rag_query_set.json"

#: 每条 query **恰好**这 5 个键
QUERY_KEYS = ("query", "expected_category", "expected_keywords", "top_k", "min_score")
#: corpus 每篇 **恰好**这 4 个键
DOC_KEYS = ("title", "content", "category", "source")

#: 需求点名的三类知识必须都有覆盖
REQUIRED_CATEGORIES = ("technical", "job", "project")

#: 本轮要求「不要修改」的四个模块
PROTECTED_MODULES = (
    "services/vector_knowledge_retriever.py",
    "services/vector_store.py",
    "services/embedding_service.py",
    "services/interview_agent.py",
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


def _imported_modules(source: str) -> set:
    """全量 import（含函数体）——守卫用；docstring 里的模块名不会误伤。"""
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module)
                for alias in node.names:
                    names.add(f"{node.module}.{alias.name}")
    return names


def _is_real_int(value: Any) -> bool:
    """正整数校验，**显式挡掉 bool**（bool 是 int 的子类）。"""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_real_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


# ============================================================
# [1] 数据格式校验
# ============================================================
def check_format(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    _section("[1] 数据格式校验")

    _check("文件存在", SET_PATH.is_file(), str(SET_PATH))
    _check("是合法 JSON 且顶层是对象", isinstance(data, dict))

    for key in ("name", "version", "purpose", "format", "corpus", "queries"):
        _check(f"顶层含必填键 {key!r}", key in data)

    corpus = data.get("corpus")
    _check("corpus 是对象且含 documents 列表",
           isinstance(corpus, dict) and isinstance(corpus.get("documents"), list))
    docs = corpus.get("documents") if isinstance(corpus, dict) else None
    queries = data.get("queries")
    _check("queries 是非空列表", isinstance(queries, list) and bool(queries))
    if not isinstance(docs, list) or not docs or not isinstance(queries, list) or not queries:
        return [], []

    # ---- corpus ----
    _check(f"corpus 每篇恰好 4 键 {DOC_KEYS}",
           all(set(d) == set(DOC_KEYS) for d in docs),
           str([i for i, d in enumerate(docs) if set(d) != set(DOC_KEYS)]))
    _check("corpus 四键取值都是非空字符串",
           all(isinstance(d[k], str) and d[k].strip() for d in docs for k in DOC_KEYS))
    bad_cat = sorted({d["category"] for d in docs} - set(KNOWLEDGE_CATEGORIES))
    _check(f"corpus category 全部合法（{KNOWLEDGE_CATEGORIES}）", not bad_cat, str(bad_cat))
    natural = [(d["title"], d["source"], d["category"]) for d in docs]
    _check("corpus 文档自然键 (title+source+category) 无重复",
           len(set(natural)) == len(natural))

    # 注意：corpus_policy 是**顶层**键，不在 corpus 内部（写错会静默取到默认值 500，
    # 于是下面「上限 < DEFAULT_CHUNK_SIZE」这条断言会因为错误的原因失败）。
    limit = int(data.get("corpus_policy", {}).get("每篇正文长度上限", DEFAULT_CHUNK_SIZE))
    over = [(d["title"], len(d["content"])) for d in docs if len(d["content"]) > limit]
    _check(f"★ corpus 每篇正文 <= {limit} 字（保证每篇恰好 1 片，关键词取证无歧义）",
           not over, str(over))
    _check(f"  └ 该上限确实小于 DEFAULT_CHUNK_SIZE({DEFAULT_CHUNK_SIZE})",
           limit < DEFAULT_CHUNK_SIZE, str(limit))

    # ---- queries ----
    bad_keys = [i for i, q in enumerate(queries) if set(q) != set(QUERY_KEYS)]
    _check(f"★ 每条 query 恰好 5 键 {QUERY_KEYS}", not bad_keys, str(bad_keys))
    _check("query 是非空字符串",
           all(isinstance(q["query"], str) and q["query"].strip() for q in queries))
    _check("expected_category 是合法 category",
           all(q["expected_category"] in KNOWLEDGE_CATEGORIES for q in queries),
           str([q["expected_category"] for q in queries
                if q["expected_category"] not in KNOWLEDGE_CATEGORIES]))
    _check("expected_keywords 是非空字符串列表（每个元素非空）",
           all(isinstance(q["expected_keywords"], list) and q["expected_keywords"]
               and all(isinstance(k, str) and k.strip() for k in q["expected_keywords"])
               for q in queries))
    _check("★ top_k 是正整数（显式挡掉 bool）",
           all(_is_real_int(q["top_k"]) and q["top_k"] >= 1 for q in queries),
           str([(q["query"], q["top_k"]) for q in queries
                if not (_is_real_int(q["top_k"]) and q["top_k"] >= 1)]))
    _check("★ min_score 是数值（显式挡掉 bool）且落在余弦取值域 [-1, 1]",
           all(_is_real_number(q["min_score"]) and -1.0 <= q["min_score"] <= 1.0
               for q in queries),
           str([(q["query"], q["min_score"]) for q in queries
                if not (_is_real_number(q["min_score"])
                        and -1.0 <= q["min_score"] <= 1.0)]))
    texts = [q["query"] for q in queries]
    _check("query 文本无重复（固定测试集不该有重复项）",
           len(set(texts)) == len(texts))

    # ---- 覆盖度 ----
    covered = {q["expected_category"] for q in queries}
    missing = sorted(set(REQUIRED_CATEGORIES) - covered)
    _check(f"★ 需求点名的三类知识都有覆盖 {REQUIRED_CATEGORIES}", not missing, str(missing))
    _check("corpus 的类别集合与 query 期望类别一致（没有查不到的类别）",
           covered == {d["category"] for d in docs},
           f"queries={sorted(covered)} docs={sorted({d['category'] for d in docs})}")

    return docs, queries


# ============================================================
# [2] 可以被测试读取
# ============================================================
def check_readable(data: Dict[str, Any], docs: List[Dict[str, Any]],
                   queries: List[Dict[str, Any]]) -> None:
    _section("[2] 可以被测试读取")

    raw = SET_PATH.read_bytes()
    _check("文件可按 UTF-8 解码（中文内容无乱码）",
           raw.decode("utf-8").startswith("{"))
    _check("重新 json.loads 得到与首次一致的结构（可重复读取）",
           json.loads(raw.decode("utf-8")) == data)
    _check("不需要任何业务对象即可读出规模（纯数据）",
           len(docs) == 16 and len(queries) == 16, f"docs={len(docs)} queries={len(queries)}")

    by_cat = Counter(d["category"] for d in docs)
    q_by_cat = Counter(q["expected_category"] for q in queries)
    print(f"  corpus {len(docs)} 篇：" +
          " / ".join(f"{c}={by_cat[c]}" for c in KNOWLEDGE_CATEGORIES))
    print(f"  queries {len(queries)} 条：" +
          " / ".join(f"{c}={q_by_cat[c]}" for c in KNOWLEDGE_CATEGORIES))
    print(f"  top_k 取值：{sorted({q['top_k'] for q in queries})}")
    print(f"  min_score 区间：[{min(q['min_score'] for q in queries)}, "
          f"{max(q['min_score'] for q in queries)}]")

    _check("每类都有 query（读取后能算出完整分布）",
           all(q_by_cat[c] > 0 for c in REQUIRED_CATEGORIES))
    _check("top_k 不止一个取值（覆盖不同截断）",
           len({q["top_k"] for q in queries}) >= 2,
           str(sorted({q["top_k"] for q in queries})))
    _check("min_score 逐条不同（是逐条标定，不是一刀切）",
           len({q["min_score"] for q in queries}) >= 5,
           str(sorted({q["min_score"] for q in queries})))


# ============================================================
# [3] 端到端检索准确性
# ============================================================
async def check_retrieval(docs: List[Dict[str, Any]],
                          queries: List[Dict[str, Any]]) -> Dict[str, Any]:
    _section("[3] 端到端检索准确性（内存 SQLite + 真实检索器）")

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with factory() as db:
            pipeline = KnowledgeImportPipeline(db)
            embedder = pipeline.embedder
            print(f"  embedder={embedder.name} dimension={embedder.dimension} "
                  f"（离线占位，非语义——见文件 model_dependency 字段）")

            for doc in docs:
                report = await pipeline.import_document(doc)
                if report["status"] != STATUS_OK or report["chunk_count"] != 1:
                    _check(f"corpus 入库：{doc['title']}", False,
                           f"status={report['status']} chunks={report['chunk_count']}")

            _check("corpus 全部入库成功", True)
            doc_rows = int((await db.execute(
                select(func.count()).select_from(KnowledgeDocument))).scalar_one())
            chunk_rows = int((await db.execute(
                select(func.count()).select_from(KnowledgeChunk))).scalar_one())
            _check("knowledge_document 行数 == corpus 篇数", doc_rows == len(docs),
                   f"{doc_rows} vs {len(docs)}")
            _check("★ knowledge_chunk 行数 == corpus 篇数（每篇恰好 1 片）",
                   chunk_rows == len(docs), f"{chunk_rows} vs {len(docs)}")

            title_by_source = {d["source"]: d["title"] for d in docs}
            store = SqlAlchemyVectorStore(db)

            rows: List[Dict[str, Any]] = []
            print(f"\n  {'score':>7}  {'期望类别':<10} {'命中类别':<10} "
                  f"{'条数':>4} {'top_k':>5}  结果")
            print("  " + "-" * 100)
            for q in queries:
                retriever = VectorKnowledgeRetriever(
                    embedder, store, top_k=q["top_k"], min_score=q["min_score"]
                )
                chunks = await retriever.retrieve({}, q["query"], {})
                scores = [float(c.metadata.get("score", 0.0)) for c in chunks]
                top = chunks[0] if chunks else None
                hit_cat = (top.metadata.get("category") if top else None)
                hit_source = top.source if top else None
                missing_kw = ([k for k in q["expected_keywords"] if k not in top.content]
                              if top else list(q["expected_keywords"]))

                rows.append({
                    "query": q["query"], "count": len(chunks),
                    "scores": scores, "hit_cat": hit_cat,
                    "hit_title": title_by_source.get(hit_source, hit_source),
                    "missing_kw": missing_kw,
                    "cat_ok": hit_cat == q["expected_category"],
                    "kw_ok": not missing_kw,
                    "count_ok": len(chunks) <= q["top_k"],
                    "floor_ok": all(s >= q["min_score"] for s in scores),
                    "nonempty": bool(chunks),
                })
                mark = "OK " if (rows[-1]["cat_ok"] and rows[-1]["kw_ok"]) else "MISS"
                print(f"  {scores[0] if scores else 0:>7.4f}  "
                      f"{q['expected_category']:<10} {str(hit_cat):<10} "
                      f"{len(chunks):>4} {q['top_k']:>5}  [{mark}] {q['query']}")
                print(f"  {'':>7}  -> {rows[-1]['hit_title']}"
                      + (f"   缺关键词={missing_kw}" if missing_kw else ""))

            # ---- expectation_contract 四条 ----
            _check("契约 1：每条 query 的结果条数 <= top_k",
                   all(r["count_ok"] for r in rows),
                   str([r["query"] for r in rows if not r["count_ok"]]))
            _check("契约 2：每条结果 score >= min_score",
                   all(r["floor_ok"] for r in rows),
                   str([r["query"] for r in rows if not r["floor_ok"]]))
            _check("契约 3：top-1 的 category == expected_category",
                   all(r["cat_ok"] for r in rows),
                   str([(r["query"], r["hit_cat"]) for r in rows if not r["cat_ok"]]))
            _check("契约 4：top-1 正文包含全部 expected_keywords",
                   all(r["kw_ok"] for r in rows),
                   str([(r["query"], r["missing_kw"]) for r in rows if not r["kw_ok"]]))
            _check("每条 query 都检出了结果（不是靠空结果绕过断言）",
                   all(r["nonempty"] for r in rows),
                   str([r["query"] for r in rows if not r["nonempty"]]))
            _check("★ 检索准确率 = 100%（top-1 类别与关键词全部命中）",
                   all(r["cat_ok"] and r["kw_ok"] for r in rows),
                   f"{sum(1 for r in rows if r['cat_ok'] and r['kw_ok'])}/{len(rows)}")
            _check("min_score 确实起了过滤作用（至少一条 query 的条数 < top_k）",
                   any(r["count"] < q["top_k"] for r, q in zip(rows, queries)),
                   str([(r["query"], r["count"]) for r, q in zip(rows, queries)
                        if r["count"] < q["top_k"]]))

            accuracy = {
                "total": len(rows),
                "correct": sum(1 for r in rows if r["cat_ok"] and r["kw_ok"]),
                "by_category": dict(Counter(
                    r["hit_cat"] for r in rows)),
            }
            return accuracy
    finally:
        await engine.dispose()


# ============================================================
# [4] 边界
# ============================================================
def check_boundaries() -> None:
    _section("[4] 边界（不修改业务代码 / 不依赖生产知识库）")

    # ---- 4.1 四个受保护模块不认识本测试集 ----
    offenders = [rel for rel in PROTECTED_MODULES
                 if "rag_query_set" in (BACKEND_DIR / rel).read_text(encoding="utf-8")]
    _check("★ Retriever / VectorStore / Embedding / InterviewAgent 都不引用本测试集",
           not offenders, str(offenders))

    # ---- 4.2 关键接口签名未变 ----
    _check("VectorKnowledgeRetriever.retrieve 签名仍是 (self, job_info, topic, context)",
           list(inspect.signature(VectorKnowledgeRetriever.retrieve).parameters)
           == ["self", "job_info", "topic", "context"],
           str(list(inspect.signature(VectorKnowledgeRetriever.retrieve).parameters)))
    _check("VectorStore 的抽象方法仍是 add / search / count",
           getattr(VectorStore, "__abstractmethods__", None)
           == frozenset({"add", "search", "count"}),
           str(getattr(VectorStore, "__abstractmethods__", None)))
    _check("EmbeddingService 的抽象方法仍是 {_embed_one}",
           getattr(EmbeddingService, "__abstractmethods__", None) == frozenset({"_embed_one"}),
           str(getattr(EmbeddingService, "__abstractmethods__", None)))
    _check("InterviewAgent.generate_question 的 knowledge_context 仍是 keyword-only",
           inspect.signature(agent_module.generate_question)
           .parameters["knowledge_context"].kind is inspect.Parameter.KEYWORD_ONLY)

    # ---- 4.3 不依赖生产知识库 ----
    set_src = SET_PATH.read_text(encoding="utf-8")
    # 只认「真的指向某个库」的**连接串/生产表名**，不认裸词 mysql——
    # corpus 的 source 里本来就有 handbook://mysql/index 这种知识来源命名空间，
    # 它是知识主题词，不是数据库引用。守卫要精确，否则会误伤而被迫放宽。
    db_tokens = ("knowledge_document", "knowledge_chunk", "DATABASE_URL",
                 "aiomysql", "pymysql", "create_engine", "sqlalchemy",
                 "career.db", "mysql+", "mysql://", "127.0.0.1", "localhost")
    hits = [t for t in db_tokens if t in set_src]
    _check("★ 测试集文件不含任何数据库连接串/生产表名（纯数据，不指向生产库）",
           not hits, str(hits))
    probe = "mysql" + "+aiomysql://root@127.0.0.1:3306/career"
    _check("  └ 守卫词表对真实连接串有效（自检，证明不是空跑）",
           any(t in probe for t in db_tokens))
    _check("  └ 本套件用内存 SQLite（corpus 自己导入，不读生产库）",
           os.environ.get("DATABASE_URL", "").startswith("sqlite"))
    _check("  └ 本套件没有 import 生产库读取路径（knowledge_document_service）",
           "services.knowledge_document_service" not in
           _imported_modules(Path(__file__).read_text(encoding="utf-8")))

    # ---- 4.4 守卫自身验真（反例 token 运行时拼出，避免自引用恒假）----
    absent = "rag_query" + "_" + "SET_ZZZ"
    _check("守卫自检：子串检查能区分「有」与「无」",
           ("rag_query_set" in set_src) is True
           and (absent in set_src) is False
           and ("rag_query_set" in (BACKEND_DIR / PROTECTED_MODULES[0]).read_text(
               encoding="utf-8")) is False)


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 74)
    print("RAG 检索准确性 · 固定 query 测试集 自检")
    print(f"数据集：{SET_PATH.relative_to(BACKEND_DIR.parent)}")
    print("=" * 74)

    data = json.loads(SET_PATH.read_text(encoding="utf-8"))
    docs, queries = check_format(data)
    if not docs or not queries:
        print("\n数据集结构不合法，后续检查跳过。")
        return False

    check_readable(data, docs, queries)
    accuracy = await check_retrieval(docs, queries)
    check_boundaries()

    print("\n" + "=" * 74)
    print("测试数据规模")
    print("=" * 74)
    print(f"  corpus      : {len(docs)} 篇（每篇 1 片 ⇒ {len(docs)} 片）")
    print(f"  queries     : {len(queries)} 条")
    print(f"  检索准确率  : {accuracy['correct']}/{accuracy['total']} top-1 命中")
    print("=" * 74)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
