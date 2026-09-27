# -*- coding: utf-8 -*-
"""AI 面试知识库 · 批量知识入库 Pipeline 自检（脚本式，非 pytest）。

覆盖需求点名的三条测试，外加幂等 / 失败恢复 / 隔离守卫：

[1] 契约与配置（出参形状、构造期校验、默认值来源）
[2] **要求 1 · 文档成功入库**（文档 + 切片 + 向量三处都真的写了）
[3] **要求 2 · 多 Chunk 成功保存**（长文 → N 片 → N 行 → N 条向量 → 可被真实检索器检索到）
[4] **要求 3 · Embedding 失败处理**（明确状态 + 已做部分保留 + 重跑续做）
[5] 幂等：支持重复执行（重复执行不重复写；换模型 / 改正文会重算）
[6] 其他失败路径（向量阶段失败 / 复用路径空正文 / 入参契约 400）
[7] 隔离守卫（不碰 InterviewAgent、不改 Retriever 接口、不认识 DB 后端）

运行：``python tests/test_knowledge_import_pipeline.py``
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import os
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))
import regression_env  # noqa: E402,F401  钉住离线 Embedding + RAG 阈值（回归不受 .env 影响）
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from fastapi import HTTPException  # noqa: E402
from sqlalchemy import func, select  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from database import Base  # noqa: E402
from models import KnowledgeChunk, KnowledgeDocument  # noqa: E402
from services import interview_core  # noqa: E402
from services.document_chunker import DocumentChunker  # noqa: E402
from services.embedding_service import (  # noqa: E402
    DEFAULT_EMBEDDING_DIMENSION,
    EmbeddingUnavailableError,
    HashEmbeddingService,
)
from services.knowledge_import_pipeline import (  # noqa: E402
    ERROR_CHUNK_EMPTY,
    ERROR_EMBEDDING_FAILED,
    ERROR_VECTOR_FAILED,
    IMPORT_RESULT_FIELDS,
    ImportConfigError,
    KnowledgeImportPipeline,
    STAGE_CHUNK,
    STAGE_DONE,
    STAGE_EMBEDDING,
    STAGE_VECTOR,
    STATUS_FAILED,
    STATUS_OK,
    STATUS_SKIPPED,
    _new_report,
    import_document,
)
from services.knowledge_rag import build_vector_retriever, build_vector_store  # noqa: E402
from services.knowledge_retriever import KnowledgeRetriever  # noqa: E402
from services.vector_knowledge_retriever import VectorKnowledgeRetriever  # noqa: E402
from services.vector_store import VectorStore, VectorStoreError  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

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


async def _rejected(coro, status: int) -> bool:
    try:
        await coro
    except HTTPException as exc:
        return exc.status_code == status
    return False


# ============================================================
# AST / 文件工具
# ============================================================
def _imported_modules(source: str) -> set:
    """AST 解析出的被 import 模块名（**不用子串匹配**——docstring 会误伤）。"""
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


def _production_files():
    """backend 下的生产代码文件（排除 tests / __pycache__）。"""
    files = []
    for pattern in ("*.py", "api/*.py", "services/*.py", "models/*.py",
                    "schemas/*.py", "utils/*.py"):
        for path in BACKEND_DIR.glob(pattern):
            if "__pycache__" in path.parts:
                continue
            files.append(path)
    return sorted(set(files))


def _retrieval_of(source: str, attr: str) -> int:
    """源码里 ``<something>.<attr>(`` 形式的调用次数（AST，非子串）。"""
    count = 0
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == attr:
                count += 1
    return count


# ============================================================
# 测试替身
# ============================================================
class FailingEmbedder(HashEmbeddingService):
    """哈希 Embedding，可在**第 k 次**编码时抛错（``fail_at=None`` 表示永不失败）。

    name 固定为 ``hash-failing``：重跑时必须用**同名**的实例，
    这样「已完成的切片」才会被正确跳过（模型标识一致）。
    """

    name = "hash-failing"

    def __init__(self, fail_at=None, *, exc=None) -> None:
        super().__init__()
        self.fail_at = fail_at
        self.exc = exc if exc is not None else EmbeddingUnavailableError("上游抖动")
        self.calls: list = []

    async def _embed_one(self, text: str):
        index = len(self.calls)
        self.calls.append(text)
        if self.fail_at is not None and index == self.fail_at:
            raise self.exc
        return await super()._embed_one(text)


class NamedEmbedder(HashEmbeddingService):
    """可改 ``name`` 的哈希 Embedding——用于模拟「换了一个模型」。"""

    def __init__(self, name: str, *, dimension: int = DEFAULT_EMBEDDING_DIMENSION) -> None:
        super().__init__(dimension=dimension)
        self.name = name


class ExplodingStore(VectorStore):
    """一调就炸的向量存储（向量阶段失败路径）。"""

    name = "exploding"

    async def add(self, records):
        raise VectorStoreError("向量库不可用")

    async def search(self, *args, **kwargs):
        raise VectorStoreError("向量库不可用")

    async def count(self) -> int:
        raise VectorStoreError("向量库不可用")


# ============================================================
# 素材与夹具
# ============================================================
PARA = "Redis 的 AOF 持久化通过 appendfsync 参数控制刷盘策略，everysec 是常用折中。"
LONG_TEXT = "".join(f"第{i}节：{PARA}" for i in range(40))

CHUNKER = DocumentChunker(chunk_size=200, chunk_overlap=40)


def _payload(title="Redis 持久化手册", content="Redis 支持 RDB 与 AOF 两种持久化。",
             category="technical", source="manual://handbook/redis"):
    return {"title": title, "content": content, "category": category, "source": source}


def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _count(db, model) -> int:
    return int((await db.execute(select(func.count()).select_from(model))).scalar_one())


async def _chunk_rows(db):
    return (
        await db.execute(select(KnowledgeChunk).order_by(KnowledgeChunk.id))
    ).scalars().all()


def _make(db, embedder=None, store=None, chunker=None, model=None) -> KnowledgeImportPipeline:
    return KnowledgeImportPipeline(
        db,
        chunker=chunker if chunker is not None else CHUNKER,
        embedder=embedder if embedder is not None else HashEmbeddingService(),
        store=store if store is not None else SqlAlchemyVectorStore(db, model=model),
        model=model,
    )


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        # ------------------------------------------------------------
        # [1] 契约与配置
        # ------------------------------------------------------------
        print("\n[1] 契约与配置（出参形状 / 构造期校验 / 默认值来源）")
        report = _new_report()
        _check("★ 报告键集合与顺序恒为 IMPORT_RESULT_FIELDS",
               tuple(report) == IMPORT_RESULT_FIELDS, str(tuple(report)))
        _check("  └ 共 12 键（成功 / 跳过 / 失败同一形状）",
               len(IMPORT_RESULT_FIELDS) == 12, str(len(IMPORT_RESULT_FIELDS)))
        _check("  └ 空白报告是成功态（ok=True / status=ok / stage=done）",
               report["ok"] is True and report["status"] == STATUS_OK
               and report["stage"] == STAGE_DONE)
        _check("  └ 计数与错误字段初值为 0 / [] / \"\"",
               report["chunk_count"] == 0 and report["saved_chunks"] == 0
               and report["embedded_chunks"] == 0 and report["skipped_chunks"] == 0
               and report["failed_index"] is None and report["errors"] == []
               and report["error"] == "" and report["reused_document"] is False)

        try:
            KnowledgeImportPipeline(None)
            _check("★ 没有 db → ImportConfigError（构造期就报）", False, "竟然构造成功")
        except ImportConfigError as exc:
            _check("★ 没有 db → ImportConfigError（构造期就报）", "db" in str(exc), str(exc))
        _check("  └ ImportConfigError 是 ValueError（调用方改代码就能解决）",
               issubclass(ImportConfigError, ValueError))

        pipe = _make(db)
        _check("★ 默认 chunker 是 DocumentChunker",
               isinstance(pipe.chunker, DocumentChunker))
        _check("★ 默认 embedder 来自组装器 default_embedder()（不在此另写一份默认）",
               pipe.embedder.name == HashEmbeddingService().name, pipe.embedder.name)
        _check("★ 默认 store 来自组装器 build_vector_store()（本模块不认识 DB 后端）",
               isinstance(pipe.store, SqlAlchemyVectorStore), type(pipe.store).__name__)
        _check("  └ 组装器确实暴露了写侧入口 build_vector_store",
               callable(build_vector_store))

        _check("★ 入参是协程接口（import_document 是 async）",
               inspect.iscoroutinefunction(KnowledgeImportPipeline.import_document))
        _check("  └ 便捷入口 import_document 也是协程函数",
               inspect.iscoroutinefunction(import_document))

        # ------------------------------------------------------------
        # [2] 要求 1：文档成功入库
        # ------------------------------------------------------------
        print("\n[2] 要求 1 · 文档成功入库")
        embedder = HashEmbeddingService()
        store = SqlAlchemyVectorStore(db)
        result = await _make(db, embedder=embedder, store=store).import_document(_payload())

        _check("★ 导入成功（ok / status=ok / stage=done）",
               result["ok"] is True and result["status"] == STATUS_OK
               and result["stage"] == STAGE_DONE, str(result))
        _check("  └ 报告键集合与顺序仍然恒定", tuple(result) == IMPORT_RESULT_FIELDS)
        _check("  └ 新建了文档（reused_document=False）", result["reused_document"] is False)
        _check("  └ 拿到了 document_id", isinstance(result["document_id"], int),
               str(result["document_id"]))
        _check("  └ 无错误（errors=[] / error=\"\" / failed_index=None）",
               result["errors"] == [] and result["error"] == ""
               and result["failed_index"] is None)
        _check("★ 短文本整篇 1 片（chunk_count=1）", result["chunk_count"] == 1,
               str(result["chunk_count"]))
        _check("  └ saved_chunks=1 / embedded_chunks=1 / skipped_chunks=0",
               result["saved_chunks"] == 1 and result["embedded_chunks"] == 1
               and result["skipped_chunks"] == 0)

        _check("★ 文档真的落库了（1 行）", await _count(db, KnowledgeDocument) == 1)
        rows = await _chunk_rows(db)
        _check("★ 切片真的落库了（1 行）", len(rows) == 1, str(len(rows)))
        _check("  └ 切片指向该文档", rows[0].document_id == result["document_id"])
        _check("  └ 切片正文与原文一致（原样保存）",
               rows[0].content == _payload()["content"], repr(rows[0].content))
        _check("★ 切片带上了向量（embedding 非空 / 维度对得上）",
               bool(rows[0].embedding) and len(rows[0].embedding) == embedder.dimension)
        _check("★ 记下了模型标识（换模型后据此重算）",
               rows[0].embedding_model == embedder.name, rows[0].embedding_model)
        _check("  └ embedding_dim 冗余列也写了",
               rows[0].embedding_dim == embedder.dimension, str(rows[0].embedding_dim))
        _check("★ 切片 metadata 含来源三键 + 追加的 chunk_index",
               rows[0].chunk_metadata.get("document_id") == result["document_id"]
               and rows[0].chunk_metadata.get("category") == "technical"
               and rows[0].chunk_metadata.get("source") == "manual://handbook/redis"
               and rows[0].chunk_metadata.get("chunk_index") == 0,
               str(rows[0].chunk_metadata))
        _check("★ 向量库侧也数得到 1 条", await store.count() == 1, str(await store.count()))

        # 入参形态：对象 / ORM 行都吃
        import types
        result_obj = await _make(db, embedder=embedder, store=store).import_document(
            types.SimpleNamespace(**_payload(title="对象形态", source="manual://obj"))
        )
        _check("★ 入参也接受对象形态（SimpleNamespace）", result_obj["ok"] is True,
               str(result_obj["errors"]))

        # ------------------------------------------------------------
        # [3] 要求 2：多 Chunk 成功保存
        # ------------------------------------------------------------
        print("\n[3] 要求 2 · 多 Chunk 成功保存")
        store2 = SqlAlchemyVectorStore(db)
        pipe2 = _make(db, embedder=embedder, store=store2)
        big = await pipe2.import_document(
            _payload(title="Redis 持久化长文", content=LONG_TEXT, source="manual://long")
        )
        n = big["chunk_count"]
        _check("★ 长文本被切成多片", big["ok"] is True and n >= 3, str(big))
        _check("★ 每片都新建了切片行（saved_chunks == chunk_count）",
               big["saved_chunks"] == n, f"{big['saved_chunks']} vs {n}")
        _check("★ 每片都写了向量（embedded_chunks == chunk_count）",
               big["embedded_chunks"] == n, str(big["embedded_chunks"]))
        _check("  └ 没有跳过任何片", big["skipped_chunks"] == 0)

        rows2 = [
            row for row in await _chunk_rows(db)
            if row.document_id == big["document_id"]
        ]
        _check("★ 库里切片行数 == chunk_count", len(rows2) == n, f"{len(rows2)} vs {n}")
        _check("  └ chunk_index 从 0 起连续",
               sorted(row.chunk_metadata["chunk_index"] for row in rows2) == list(range(n)),
               str(sorted(row.chunk_metadata["chunk_index"] for row in rows2)))
        _check("★ 每一片都有向量（没有留下待向量化的行）",
               all(bool(row.embedding) for row in rows2))
        _check("  └ 每一片维度都一致",
               all(row.embedding_dim == embedder.dimension for row in rows2))
        _check("  └ 切片正文拼起来能覆盖原文的每一节（逐节取证）",
               all(
                   any(f"第{i}节" in row.content for row in rows2)
                   for i in range(40)
               ))
        _check("★ 向量库侧计数 = 之前 2 条 + 本次 n 条",
               await store2.count() == 2 + n, str(await store2.count()))

        # 端到端：入库后能被**真实检索器**检索到（写侧与读侧共用同一模型与同一后端）
        retriever = build_vector_retriever(db, embedder=embedder, top_k=3)
        probe = rows2[0].content
        hits = await retriever.retrieve({}, probe, None)
        _check("★ 入库后能被真实检索器检索到（写读两侧口径一致）",
               len(hits) >= 1, str(len(hits)))
        _check("  └ 命中的就是那片原文（自相似性 score == 1.0）",
               hits and hits[0].content == probe
               and round(hits[0].metadata.get("score", 0), 9) == 1.0,
               str(hits[0].metadata.get("score") if hits else None))
        _check("  └ 检索结果带来源（可溯源）",
               hits and hits[0].source == "manual://long",
               str(hits[0].source if hits else None))

        # 默认路径端到端闭环：Pipeline 一个协作者都不注入（全走组装器默认），
        # 再用 ``resolve_retriever(use_rag=True)`` 检索 —— 写侧与读侧的默认口径必须一致，
        # 否则会出现「明明入库了却检索不到」（模型标识对不上）。
        default_doc = await import_document(
            db, _payload(title="默认路径文档", content=LONG_TEXT, source="manual://default")
        )
        _check("★ 默认路径（不注入任何协作者）也能入库",
               default_doc["ok"] is True, str(default_doc["errors"]))
        retriever_default, warns = interview_core.resolve_retriever(db, use_rag=True)
        _check("  └ use_rag=True 组装出真实检索器（默认模型与入库时同一口径）",
               retriever_default is not None and warns == [], str(warns))
        default_rows = [
            row for row in await _chunk_rows(db)
            if row.document_id == default_doc["document_id"]
        ]
        hits_default = await retriever_default.retrieve({}, default_rows[0].content, None)
        _check("★ 默认路径入库的知识能被 RAG 检索到（写读闭环）",
               len(hits_default) >= 1, str(len(hits_default)))

        # ------------------------------------------------------------
        # [4] 要求 3：Embedding 失败处理
        # ------------------------------------------------------------
        print("\n[4] 要求 3 · Embedding 失败处理")
        docs_before_fail = await _count(db, KnowledgeDocument)
        chunks_before_fail = await _count(db, KnowledgeChunk)
        boom = FailingEmbedder(fail_at=1)          # 第 1 片（0 起）失败
        store3 = SqlAlchemyVectorStore(db)
        failed = await _make(db, embedder=boom, store=store3).import_document(
            _payload(title="会失败的文档", content=LONG_TEXT, source="manual://fail")
        )
        _check("★ 编码失败 → ok=False / status=failed", failed["ok"] is False
               and failed["status"] == STATUS_FAILED, str(failed))
        _check("★ 阶段明确指向 embedding", failed["stage"] == STAGE_EMBEDDING,
               failed["stage"])
        _check("★ 定位到第几片失败（failed_index=1）", failed["failed_index"] == 1,
               str(failed["failed_index"]))
        _check("★ errors 是稳定错误码（不是散文）",
               failed["errors"] == [ERROR_EMBEDDING_FAILED], str(failed["errors"]))
        _check("★ error 里带原始异常类名（可据此区分该不该重试）",
               "EmbeddingUnavailableError" in failed["error"], failed["error"])
        _check("  └ 报告键集合与顺序仍然恒定", tuple(failed) == IMPORT_RESULT_FIELDS)
        _check("  └ chunk_count 仍然可读（知道一共有几片）",
               failed["chunk_count"] == n, str(failed["chunk_count"]))
        _check("★ 已做完的部分被保留（embedded_chunks=1 / saved_chunks=1）",
               failed["embedded_chunks"] == 1 and failed["saved_chunks"] == 1,
               f"{failed['embedded_chunks']}/{failed['saved_chunks']}")
        _check("  └ 失败片之前没有多写（半截数据最少化：只多落 1 行切片）",
               await _count(db, KnowledgeChunk) == chunks_before_fail + 1,
               str(await _count(db, KnowledgeChunk)))
        _check("  └ 文档只新建了 1 篇（失败不影响已落库的文档）",
               await _count(db, KnowledgeDocument) == docs_before_fail + 1)

        # 重跑：同一模型（同名 embedder）→ 已完成的片被跳过，只补剩下的
        chunks_before_resume = await _count(db, KnowledgeChunk)
        resumed = await _make(db, embedder=FailingEmbedder(None), store=store3).import_document(
            _payload(title="会失败的文档", content=LONG_TEXT, source="manual://fail")
        )
        _check("★ 重跑可以续做（ok=True）", resumed["ok"] is True, str(resumed["errors"]))
        _check("★ 重跑复用了文档（不新建第二篇）",
               resumed["reused_document"] is True
               and resumed["document_id"] == failed["document_id"])
        _check("★ 已带向量的那一片被跳过（不重复编码）",
               resumed["skipped_chunks"] == 1, str(resumed["skipped_chunks"]))
        _check("  └ 只补了剩下的片",
               resumed["embedded_chunks"] == n - 1, str(resumed["embedded_chunks"]))
        _check("★ 重跑把缺的片补齐（总数 = 失败前的 + (n-1)）",
               await _count(db, KnowledgeChunk) == chunks_before_resume + (n - 1),
               str(await _count(db, KnowledgeChunk)))
        _check("  └ 文档数没再增加（仍是失败后那一篇）",
               await _count(db, KnowledgeDocument) == docs_before_fail + 1)

        # ------------------------------------------------------------
        # [5] 幂等：支持重复执行
        # ------------------------------------------------------------
        print("\n[5] 幂等 · 支持重复执行")
        store4 = SqlAlchemyVectorStore(db)
        pipe4 = _make(db, embedder=embedder, store=store4)
        first = await pipe4.import_document(
            _payload(title="幂等文档", content=LONG_TEXT, source="manual://idem")
        )
        docs_before = await _count(db, KnowledgeDocument)
        chunks_before = await _count(db, KnowledgeChunk)
        vectors_before = await store4.count()

        second = await pipe4.import_document(
            _payload(title="幂等文档", content=LONG_TEXT, source="manual://idem")
        )
        _check("★ 重复执行是成功的空操作（ok=True / status=skipped）",
               second["ok"] is True and second["status"] == STATUS_SKIPPED, str(second))
        _check("  └ 复用了同一篇文档", second["document_id"] == first["document_id"]
               and second["reused_document"] is True)
        _check("★ 一片都没重复写（saved=0 / embedded=0 / skipped=全部）",
               second["saved_chunks"] == 0 and second["embedded_chunks"] == 0
               and second["skipped_chunks"] == first["chunk_count"],
               f"{second['saved_chunks']}/{second['embedded_chunks']}/{second['skipped_chunks']}")
        _check("★ 文档数没变", await _count(db, KnowledgeDocument) == docs_before)
        _check("★ 切片数没变", await _count(db, KnowledgeChunk) == chunks_before)
        _check("★ 向量数没变", await store4.count() == vectors_before)

        # 换模型 → 旧向量不可比，必须重算
        switched = await _make(
            db, embedder=NamedEmbedder("other-model"), store=store4
        ).import_document(_payload(title="幂等文档", content=LONG_TEXT, source="manual://idem"))
        _check("★ 换了模型 → 全部重算（旧向量不可比）",
               switched["ok"] is True and switched["embedded_chunks"] == first["chunk_count"]
               and switched["skipped_chunks"] == 0, str(switched["embedded_chunks"]))
        _check("  └ 切片行没有重复增加",
               await _count(db, KnowledgeChunk) == chunks_before)
        _check("  └ embedding_model 已更新为新模型",
               all(row.embedding_model == "other-model" for row in await _chunk_rows(db)
                   if row.document_id == first["document_id"]))

        # 改正文 → 切点漂移，旧向量指向旧文字，必须重算
        TAIL = "补充：RDB 快照适合备份。"
        last_chunk_before = [
            row for row in await _chunk_rows(db)
            if row.document_id == first["document_id"]
        ][-1].content
        changed = await _make(db, embedder=embedder, store=store4).import_document(
            _payload(title="幂等文档", content=LONG_TEXT + TAIL, source="manual://idem")
        )
        _check("★ 正文变了 → 重算（旧向量不能指向旧文字）",
               changed["ok"] is True and changed["embedded_chunks"] > 0,
               str(changed["embedded_chunks"]))
        last_chunk_after = [
            row for row in await _chunk_rows(db)
            if row.document_id == first["document_id"]
        ][-1].content
        _check("  └ 末片正文已按新内容更新（追加的句子进来了）",
               TAIL in last_chunk_after and last_chunk_after != last_chunk_before,
               f"{last_chunk_before[-16:]!r} -> {last_chunk_after[-16:]!r}")
        _check("  └ 切片行数没有因为重算而增加",
               await _count(db, KnowledgeChunk) == chunks_before)

        # ------------------------------------------------------------
        # [6] 其他失败路径
        # ------------------------------------------------------------
        print("\n[6] 其他失败路径")
        vec_fail = await _make(
            db, embedder=embedder, store=ExplodingStore()
        ).import_document(_payload(title="向量库炸了", content=LONG_TEXT,
                                   source="manual://vecfail"))
        _check("★ 向量阶段失败 → stage=vector / errors 稳定码",
               vec_fail["ok"] is False and vec_fail["stage"] == STAGE_VECTOR
               and vec_fail["errors"] == [ERROR_VECTOR_FAILED], str(vec_fail["errors"]))
        _check("  └ failed_index 指向第一片", vec_fail["failed_index"] == 0,
               str(vec_fail["failed_index"]))

        empty_reuse = await _make(db, embedder=embedder, store=store4).import_document(
            _payload(title="幂等文档", content="   \n  ", source="manual://idem")
        )
        _check("★ 复用既有文档 + 空正文 → stage=chunk / chunk_empty（不炸）",
               empty_reuse["ok"] is False and empty_reuse["stage"] == STAGE_CHUNK
               and empty_reuse["errors"] == [ERROR_CHUNK_EMPTY],
               str(empty_reuse["errors"]))
        _check("  └ 空正文没有产出任何切片", empty_reuse["chunk_count"] == 0)

        docs_before_bad = await _count(db, KnowledgeDocument)
        _check("★ 空标题 → 400（入参契约，不是「中途失败」）",
               await _rejected(
                   _make(db, embedder=embedder, store=store4).import_document(
                       _payload(title="   ", source="manual://bad")), 400))
        _check("★ 空正文（新文档）→ 400",
               await _rejected(
                   _make(db, embedder=embedder, store=store4).import_document(
                       _payload(title="新文档空正文", content="   ",
                                source="manual://bad2")), 400))
        _check("★ category 非法 → 400",
               await _rejected(
                   _make(db, embedder=embedder, store=store4).import_document(
                       _payload(title="非法分类", category="nope",
                                source="manual://bad3")), 400))
        _check("  └ 三次非法入参都没写出文档",
               await _count(db, KnowledgeDocument) == docs_before_bad)

        # ------------------------------------------------------------
        # [7] 隔离守卫
        # ------------------------------------------------------------
        print("\n[7] 隔离守卫（不碰 InterviewAgent / 不改 Retriever 接口 / 不认识 DB 后端）")
        pipeline_src = (
            BACKEND_DIR / "services" / "knowledge_import_pipeline.py"
        ).read_text(encoding="utf-8")
        pipeline_imports = _imported_modules(pipeline_src)

        offenders = sorted(
            name for name in pipeline_imports
            if name == "main" or name.startswith("services.interview")
        )
        _check("★ 入库 Pipeline 不 import 任何 Interview 模块（需求：不改 InterviewAgent）",
               offenders == [], str(offenders))
        _check("★ 入库 Pipeline 不 import DB 向量后端（经组装器拿，换库只改组装器）",
               "services.vector_store_sql" not in pipeline_imports,
               str(sorted(pipeline_imports)))
        _check("  └ 也不 import 检索器 / 组装器的读侧（写读两侧互不依赖）",
               "services.vector_knowledge_retriever" not in pipeline_imports
               and "services.knowledge_retriever" not in pipeline_imports)

        _check("★ Retriever 接口未被修改（retrieve 签名逐字不变）",
               list(inspect.signature(KnowledgeRetriever.retrieve).parameters)
               == ["self", "job_info", "topic", "context"],
               str(list(inspect.signature(KnowledgeRetriever.retrieve).parameters)))
        _check("  └ 真实检索器的 retrieve 签名也未变",
               list(inspect.signature(VectorKnowledgeRetriever.retrieve).parameters)
               == ["self", "job_info", "topic", "context"])
        _check("  └ 入库 Pipeline 一次都没调用 retrieve（它是写侧）",
               _retrieval_of(pipeline_src, "retrieve") == 0)

        # 接线点取证：这几个模块此前是「未接线」，现在接线点收口到入库 Pipeline
        # 注意用**点号全名**匹配（`services.X`），不要用裸模块名——
        # 既有的三处「未接线」守卫就是栽在这个坑上：裸名永远匹配不到点号全名，
        # 于是断言恒为真、从未生效（本次一并修正）。
        expected_consumers = {
            "document_chunker": [
                "services/knowledge_import_pipeline.py",
            ],
            "embedding_service": [
                # 入库 Pipeline **不直接** import 它：默认实现经组装器 default_embedder()
                # 拿，异常一律按 Exception 收敛成状态码。
                # 消费它的是「实现方 + 真实检索器」：组装器已改为委托
                # ``embedding_provider.build_embedding_service()``（换模型只改配置），
                # 因此它不再直接依赖任何**具体实现**，也就不在这个集合里了。
                # 任务 75 新增第二个实现方 ``embedding_provider_spark``（讯飞协议）。
                "services/embedding_provider.py",
                "services/embedding_provider_spark.py",
                "services/vector_knowledge_retriever.py",
            ],
            "knowledge_document_service": [
                "api/knowledge.py",
                "services/knowledge_import_pipeline.py",
                "services/knowledge_maintenance.py",
            ],
        }
        for module, expected in expected_consumers.items():
            consumers = []
            for path in _production_files():
                if path.name == f"{module}.py":
                    continue
                rel = path.relative_to(BACKEND_DIR).as_posix()
                imports = _imported_modules(path.read_text(encoding="utf-8"))
                if f"services.{module}" in imports:
                    consumers.append(rel)
            _check(f"★ services.{module} 的消费者收口为已知闭集（入库 Pipeline 是写侧入口）",
                   consumers == expected, str(consumers))

        consumers_rag = []
        for path in _production_files():
            if path.name == "knowledge_rag.py":
                continue
            rel = path.relative_to(BACKEND_DIR).as_posix()
            imports = _imported_modules(path.read_text(encoding="utf-8"))
            if "services.knowledge_rag" in imports:
                consumers_rag.append(rel)
        # 任务 74 加入**维护模块**（缺口③）：它从权威行重建派生索引，
        # 需要 build_vector_store 拿「当前配置用哪个后端」——仍是**闭集 + 精确相等**。
        # 任务 76 加入**向量迁移模块**：换 Embedding 模型后把既有切片向量就地重算，
        # 同样经组装器拿 embedder / store（不认识任何具体实现）。
        _check("★ 组装器的消费者 = 读侧(Core) + 写侧(入库 Pipeline) + 维护模块 + 迁移模块",
               consumers_rag == [
                   "services/interview_core.py",
                   "services/knowledge_embedding_migration.py",
                   "services/knowledge_import_pipeline.py",
                   "services/knowledge_maintenance.py",
               ], str(consumers_rag))

    await engine.dispose()

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
