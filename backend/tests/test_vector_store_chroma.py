# -*- coding: utf-8 -*-
"""AI 面试知识库 · 真实 **ANN** 向量后端（chromadb）自检

无需 pytest，直接运行：
    python backend/tests/test_vector_store_chroma.py

不依赖本机 MySQL：权威行跑在 SQLite 内存库上（StaticPool）。
不联网：chroma 用 ``EphemeralClient`` / 临时目录，**不启动任何服务**。
相似度全用**手写向量**算，结果可精确断言。

覆盖范围
--------
1. **后端选择**：``VECTOR_STORE`` 解析（默认 / 别名 / 非法值）、
   ``build_vector_store`` 按后端返回不同实现、**不设环境变量时默认不变**
2. **元数据编解码**：扁平标量 + ``_meta`` 保真、``where`` 子句形状
3. **保存向量（测试要求 1）**：新建切片 / 给既有切片补向量（只更新向量三列）/
   ``chunk_id`` 不存在报错 / 空批次 / 入参形状
4. **查询 top_k（测试要求 2）**：截断、``min_score``、按 model / document_id /
   category 过滤、空索引返回 ``[]``
5. **返回结果正确（测试要求 3）**：``score`` = 精确余弦（与 ``cosine_similarity``
   逐位相等）、降序、命中自己 ≈ 1.0、``to_dict`` 形状
6. **换后端不换语义**：同一组数据在 内存 / SQL / chroma 三个后端上结果一致
7. **ANN 维度契约**：批次内混维度 / 与索引维度不符 / 查询维度不符 →
   抛 ``VectorStoreDimensionError``，且**在写库之前**抛（不留半截状态）
8. **持久化**：重开客户端索引仍在；索引与权威行 1:1
9. **依赖收口**：chromadb 延迟导入、接口模块零依赖未被破坏、
   后端消费者闭集、面试流程模块不碰向量层
10. **入库 Pipeline 端到端**：文档 → 切片 → 向量 → ANN 索引；
    重跑**幂等**（``status="skipped"``）——证明「索引派生自权威行」这个设计成立
"""

import ast
import asyncio
import itertools
import os
import pathlib
import shutil
import sys
import tempfile

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from sqlalchemy import func, select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import KnowledgeChunk, KnowledgeDocument  # noqa: E402
from services import knowledge_rag as rag  # noqa: E402
from services import vector_store as vs  # noqa: E402
from services.embedding_service import HashEmbeddingService  # noqa: E402
from services.vector_knowledge_retriever import RetrieverConfigError  # noqa: E402
from services.vector_store import (  # noqa: E402
    DEFAULT_TOP_K,
    InMemoryVectorStore,
    VectorRecord,
    VectorStoreDimensionError,
    VectorStoreInputError,
    cosine_similarity,
)
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

_PASSED = 0
_FAILED = 0

#: 手写正交向量：便于精确断言相似度（1.0 / 0.7071 / 0.0）
V_X = [1.0, 0.0, 0.0]
V_Y = [0.0, 1.0, 0.0]
V_XY = [1.0, 1.0, 0.0]

#: 完整元数据（含**非标量**值）：验证「扁平标量 + ``_meta`` 保真」两条路径
RICH_META = [
    {"category": "technical", "chunk_index": 0, "tags": ["redis", "cache"]},
    {"category": "job", "chunk_index": 1, "weight": 0.5},
    {"category": "technical", "chunk_index": 2, "nested": {"a": 1}, "note": None},
]


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


# ============================================================
# 通用辅助
# ============================================================
def _imported_modules(source: str) -> set:
    """用 AST 提取真正被 import 的模块名（不用子串匹配：docstring 会误伤）。"""
    names: set = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _top_level_imports(source: str) -> set:
    """只取**模块顶层**（不在任何函数体内）的 import 模块名。

    「可选依赖必须延迟导入」这类断言必须用它：``_imported_modules`` 会把函数体内的
    import 也算进来，区分不出顶层与延迟。
    """
    names: set = set()
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def _production_files():
    """后端生产代码（排除 tests / __pycache__），用于「依赖收口」检查。"""
    files = []
    for pattern in ("*.py", "api/*.py", "services/*.py", "models/*.py",
                    "schemas/*.py", "utils/*.py"):
        for path in BACKEND_DIR.glob(pattern):
            if "__pycache__" in path.parts:
                continue
            files.append(path)
    return sorted(set(files))


def _raises_sync(fn, exc_type):
    """同步版 ``_raises``：返回 (是否抛了指定异常, 异常实例或说明)。"""
    try:
        fn()
    except exc_type as exc:
        return True, exc
    except Exception as exc:  # noqa: BLE001
        return False, f"抛了非预期异常 {type(exc).__name__}: {exc}"
    return False, "未抛异常"


async def _raises(coro, exc_type):
    """执行协程，返回 (是否抛了指定异常, 异常实例或说明)。"""
    try:
        await coro
    except exc_type as exc:
        return True, exc
    except Exception as exc:  # noqa: BLE001
        return False, f"抛了非预期异常 {type(exc).__name__}: {exc}"
    return False, "未抛异常"


def _brief(matches):
    """把检索结果压成可比对的形状（去掉浮点尾差）。"""
    return [(m.content, m.chunk_id, round(m.score, 9)) for m in matches]


def _records(document_id, model="m1", *, metadata=None, vectors=None):
    """三条手写向量记录（x / y / x+y），顺序固定，便于断言排序。

    元信息里把 ``job`` 放在**第二条**：这样「按 category 过滤」的结果
    与「按相似度排序」的结果不同，能证明过滤真的生效、而不是碰巧靠排序过的。
    """
    meta = metadata or RICH_META
    vecs = vectors or [V_X, V_Y, V_XY]
    return [
        VectorRecord(vector=vecs[0], content="第一条：向量 x", document_id=document_id,
                     metadata=meta[0], model=model),
        VectorRecord(vector=vecs[1], content="第二条：向量 y", document_id=document_id,
                     metadata=meta[1], model=model),
        VectorRecord(vector=vecs[2], content="第三条：向量 x+y", document_id=document_id,
                     metadata=meta[2], model=model),
    ]


# ============================================================
# chroma 客户端辅助
# ============================================================
def _import_chromadb():
    """导入 chromadb；未安装时直接判定整份测试失败（本文件就是它的自检）。"""
    try:
        import chromadb
    except ImportError as exc:  # pragma: no cover
        print(f"\n[致命] 未安装 chromadb，无法运行本自检：{exc}")
        print("       安装：pip install chromadb")
        raise SystemExit(1) from exc
    return chromadb


CHROMADB = _import_chromadb()
from services.vector_store_chroma import (  # noqa: E402
    ChromaBackendError,
    ChromaConfigError,
    ChromaVectorStore,
    DEFAULT_COLLECTION,
    DEFAULT_OVERSAMPLE,
    META_MODEL,
    META_PAYLOAD,
    build_where,
    from_chroma_metadata,
    load_chromadb,
    open_client,
    to_chroma_metadata,
)


def _ephemeral_client():
    """进程内 chroma 客户端（不落盘、不启动服务）。

    .. warning::
       ``chromadb.EphemeralClient()`` **不是**「每次调用都得到一个空库」：
       chroma 内部按 (settings) 缓存 System，因此同一进程里多次调用会**共享**同一份
       内存数据。所以每个测试用例必须用**不同的集合名**（见 :func:`_collection_name`），
       否则用例之间会互相污染——这个坑本文件已经踩过一次。
    """
    factory = getattr(CHROMADB, "EphemeralClient", None)
    if factory is not None:
        return factory()
    return CHROMADB.Client(settings=CHROMADB.Settings(
        is_persistent=False, anonymized_telemetry=False,
    ))


_COLLECTION_SEQ = itertools.count(1)


def _collection_name() -> str:
    """每个用例一个独立集合名（见 :func:`_ephemeral_client` 的警告）。"""
    return f"test_ann_{next(_COLLECTION_SEQ)}"


def _store(session, *, model=None, collection_name=None, client=None, **kwargs):
    """构造一个用独立集合的 :class:`ChromaVectorStore`（测试隔离用）。"""
    return ChromaVectorStore(
        session,
        client=client if client is not None else _ephemeral_client(),
        collection_name=collection_name or _collection_name(),
        model=model,
        **kwargs,
    )


# ============================================================
# 主流程
# ============================================================
async def _new_engine():
    """每个用例一个**全新**的内存 SQLite 引擎（含建表）。

    为什么不在整个文件里共用一个引擎：``SqlAlchemyVectorStore.count()`` 统计的是
    **全表**已向量化行数，共用引擎会让「chroma 索引条数 == 权威行条数」这类断言
    被前面用例写下的数据污染。一个用例一个库，断言才只反映本用例。
    """
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return engine


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def run() -> bool:
    tmpdirs = []
    #: 单 session 的用例（签名 ``(session)``）：每个用例拿到**自己的引擎 + 库**
    single_session_suites = [
        _suite_backend_selection,
        _suite_save_vectors,
        _suite_search_top_k,
        _suite_result_correctness,
        _suite_backend_swap,
        _suite_dimension_contract,
    ]
    #: 需要跨 session（重开客户端 / 多轮导入）的用例（签名 ``(factory)``）
    multi_session_suites = [
        lambda factory: _suite_persistence(factory, tmpdirs),
        _suite_pipeline_end_to_end,
    ]
    try:
        for suite in single_session_suites:
            engine = await _new_engine()
            try:
                async with _session_factory(engine)() as session:
                    await suite(session)
            finally:
                await engine.dispose()
        _suite_metadata_codec()             # 纯函数，不需要库
        for suite in multi_session_suites:
            engine = await _new_engine()
            try:
                await suite(_session_factory(engine))
            finally:
                await engine.dispose()
        _suite_dependency_closure()
    finally:
        for path in tmpdirs:
            shutil.rmtree(path, ignore_errors=True)

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


# ------------------------------------------------------------
# [1] 后端选择
# ------------------------------------------------------------
async def _suite_backend_selection(session):
    print("\n[1] 后端选择：VECTOR_STORE 解析 + 组装器按后端返回不同实现")

    _check("★ 不设 VECTOR_STORE → 默认 sql（**接入 ANN 不改变默认行为**）",
           rag.resolve_vector_backend({}) == rag.BACKEND_SQL,
           rag.resolve_vector_backend({}))
    _check("  └ 空字符串同样视为未设置",
           rag.resolve_vector_backend({rag.ENV_VECTOR_STORE: ""}) == rag.BACKEND_SQL)
    _check("  └ 只有空白字符也视为未设置",
           rag.resolve_vector_backend({rag.ENV_VECTOR_STORE: "   "}) == rag.BACKEND_SQL)
    for raw in ("chroma", "ChromaDB", "CHROMA", "  chromadb  "):
        _check(f"  └ {raw!r} → chroma（大小写 / 空白不敏感）",
               rag.resolve_vector_backend({rag.ENV_VECTOR_STORE: raw}) == rag.BACKEND_CHROMA,
               raw)
    for raw in ("sql", "SQL", "sqlalchemy", "mysql"):
        _check(f"  └ {raw!r} → sql",
               rag.resolve_vector_backend({rag.ENV_VECTOR_STORE: raw}) == rag.BACKEND_SQL,
               raw)

    ok, info = _raises_sync(
        lambda: rag.resolve_vector_backend({rag.ENV_VECTOR_STORE: "faiss"}),
        RetrieverConfigError,
    )
    _check("★ 非法取值**报错**而不是静默退回默认（否则「以为开了 ANN」会藏起来）",
           ok and "faiss" in str(info), str(info))

    ok, info = _raises_sync(lambda: rag.build_vector_store(None), RetrieverConfigError)
    _check("  └ build_vector_store 不给 db → RetrieverConfigError",
           ok and "db" in str(info), str(info))

    store_sql = rag.build_vector_store(session, backend="sql")
    _check("  └ backend='sql' → SqlAlchemyVectorStore",
           isinstance(store_sql, SqlAlchemyVectorStore) and store_sql.name == "sqlalchemy",
           repr(store_sql))

    store_chroma = rag.build_vector_store(session, backend="chroma")
    _check("★ backend='chroma' → ChromaVectorStore（真实 ANN 后端）",
           isinstance(store_chroma, ChromaVectorStore) and store_chroma.name == "chroma",
           repr(store_chroma))
    _check("  └ 两者都拿同一个 db：切片行的权威副本始终在 knowledge_chunk",
           store_chroma.db is session and store_sql.db is session)

    ok, info = _raises_sync(
        lambda: rag.build_vector_store(session, backend="milvus"), RetrieverConfigError)
    _check("  └ 显式传非法 backend 也报错", ok and "milvus" in str(info), str(info))

    # 环境变量真的会被组装器读到（用显式传参覆盖，避免污染真实环境）
    os.environ[rag.ENV_VECTOR_STORE] = "chroma"
    try:
        picked = rag.build_vector_store(session)
    finally:
        os.environ.pop(rag.ENV_VECTOR_STORE, None)
    _check("  └ 组装器真的会读环境变量（VECTOR_STORE=chroma → ChromaVectorStore）",
           isinstance(picked, ChromaVectorStore), repr(picked))
    default_after = rag.build_vector_store(session)
    _check("  └ 环境变量撤销后回到默认 sql（不留残留状态）",
           isinstance(default_after, SqlAlchemyVectorStore), repr(default_after))


# ------------------------------------------------------------
# [2] 元数据编解码（纯函数）
# ------------------------------------------------------------
async def _suite_metadata_codec():
    print("\n[2] 元数据编解码：扁平标量供过滤 + _meta 保真（chroma 存不了嵌套）")

    record = VectorRecord(
        vector=V_X, content="正文", document_id=7,
        metadata={"category": "technical", "tags": ["a", "b"], "note": None, "n": 3},
        model="m1",
    )
    meta = to_chroma_metadata(record)
    _check("★ 标量键被扁平化（category 因此能在**索引层**过滤）",
           meta.get("category") == "technical" and meta.get("n") == 3, str(meta))
    _check("★ 非标量（list / None）不扁平化（chroma 元数据只接受标量）",
           "tags" not in meta and "note" not in meta, str(meta))
    _check("  └ document_id / embedding_model 由记录字段写入（保留键）",
           meta.get("document_id") == 7 and meta.get(META_MODEL) == "m1", str(meta))
    _check("  └ _meta 里是完整元数据的 JSON（保真载荷）", META_PAYLOAD in meta, str(meta))

    back, model, document_id = from_chroma_metadata(meta)
    _check("★ 读回**保真**：嵌套 list 与 None 都还原了",
           back == {"category": "technical", "tags": ["a", "b"], "note": None, "n": 3},
           str(back))
    _check("  └ model / document_id 一并还原", model == "m1" and document_id == 7,
           f"{model} {document_id}")

    # 业务元数据里出现与保留键同名的键：不能把过滤条件带偏
    tricky = VectorRecord(vector=V_X, document_id=1,
                          metadata={"document_id": 999, "embedding_model": "fake"})
    tricky_meta = to_chroma_metadata(tricky)
    _check("  └ 业务元数据里的保留键**不覆盖**记录字段（过滤条件不会被带偏）",
           tricky_meta["document_id"] == 1 and tricky_meta[META_MODEL] == "",
           str(tricky_meta))

    # 没有 _meta（人为写库 / 旧数据）：退回扁平标量，不报错
    fallback, model2, doc2 = from_chroma_metadata({"category": "job", META_MODEL: "m2"})
    _check("  └ 缺 _meta 时退回扁平标量（宁少信息也不报错）",
           fallback == {"category": "job"} and model2 == "m2" and doc2 is None,
           f"{fallback} {model2} {doc2}")
    _check("  └ 非 Mapping 入参 → 空结果（不抛异常）",
           from_chroma_metadata(None) == ({}, "", None), str(from_chroma_metadata(None)))

    _check("  └ build_where 无条件 → None", build_where() is None, str(build_where()))
    _check("  └ build_where 单条件 → 直接给键值对",
           build_where(model="m1") == {META_MODEL: "m1"}, str(build_where(model="m1")))
    where_two = build_where(model="m1", document_id=3)
    _check("★ build_where 多条件 → $and 列表（形状写错会静默返回空结果）",
           where_two == {"$and": [{META_MODEL: "m1"}, {"document_id": 3}]}, str(where_two))
    where_three = build_where(model="m1", document_id=3, category="job")
    _check("  └ 三条件同样包在 $and 里", isinstance(where_three, dict)
           and len(where_three["$and"]) == 3, str(where_three))


# ------------------------------------------------------------
# [3] 保存向量（测试要求 1）
# ------------------------------------------------------------
async def _seed_document(session, *, title="ANN 测试文档", category="technical"):
    document = KnowledgeDocument(title=title, content="正文", category=category, source="test")
    session.add(document)
    await session.commit()
    return document.id


async def _chunk_rows(session, document_id):
    return (await session.execute(
        select(KnowledgeChunk).where(KnowledgeChunk.document_id == document_id)
        .order_by(KnowledgeChunk.id)
    )).scalars().all()


async def _suite_save_vectors(session):
    print("\n[3] 保存向量（测试要求 1）：新建 / 补向量 / 报错路径")

    document_id = await _seed_document(session, title="保存向量")
    store = _store(session, model="m1")

    written = await store.add(_records(document_id))
    _check("★ add 返回写入条数", written == 3, str(written))
    _check("★ 索引里确实存下了 3 条向量（向量保存）", await store.count() == 3,
           str(await store.count()))

    rows = await _chunk_rows(session, document_id)
    _check("  └ 权威行也建好了（切片行仍在 knowledge_chunk）", len(rows) == 3, str(len(rows)))
    _check("  └ 权威行的 embedding 列已写入（Pipeline 的幂等判据依赖它）",
           all(row.embedding and row.embedding_dim == 3 for row in rows),
           str([(row.embedding_dim, bool(row.embedding)) for row in rows]))
    _check("  └ 权威行的 embedding_model 已写入", all(row.embedding_model == "m1" for row in rows))
    _check("  └ 索引条数 == 权威已向量化条数（索引是派生数据，1:1）",
           await store.count() == await SqlAlchemyVectorStore(session).count(),
           f"{await store.count()} vs {await SqlAlchemyVectorStore(session).count()}")

    # 空批次：不写、不报错
    _check("  └ add([]) → 0（空批次是合法入参）", await store.add([]) == 0)

    # 单条对象（不是列表）→ 报错（dict 可迭代，必须显式拦）
    ok, info = await _raises(store.add(_records(document_id)[0]), VectorStoreInputError)
    _check("  └ 把单个 VectorRecord 当批量传 → VectorStoreInputError",
           ok and "一组" in str(info), str(info))
    ok, info = await _raises(store.add(None), VectorStoreInputError)
    _check("  └ add(None) → VectorStoreInputError（提示传 []）",
           ok and "None" in str(info), str(info))

    # 给**既有**切片补向量：只更新向量三列，正文 / 元数据 / 文档 id 不动
    target = rows[0]
    before_meta = dict(target.chunk_metadata)
    await store.add([VectorRecord(
        chunk_id=target.id, vector=[0.0, 0.0, 1.0], model="m2",
        content="这条正文应当被忽略", document_id=999999,
        metadata={"category": "被忽略"},
    )])
    await session.refresh(target)
    _check("★ 补向量只更新向量三列，**不动**切片正文",
           target.content == "第一条：向量 x", repr(target.content))
    _check("  └ 元数据 / 文档 id 也一律不动",
           target.chunk_metadata == before_meta and target.document_id == document_id,
           f"{target.chunk_metadata} {target.document_id}")
    _check("  └ 向量与模型确实换了", target.embedding_model == "m2"
           and target.embedding_dim == 3, f"{target.embedding_model} {target.embedding_dim}")

    # 索引侧：内容取自**权威行**（不是调用方入参），因此正文不会变成「这条正文应当被忽略」
    hits = await store.search([0.0, 0.0, 1.0], top_k=1, model="m2")
    _check("★ 索引内容取自权威行：补向量后索引里的正文**没有**被入参覆盖",
           len(hits) == 1 and hits[0].content == "第一条：向量 x",
           str(_brief(hits)))
    _check("  └ 索引里的元数据同样来自权威行", hits[0].metadata.get("category") == "technical",
           str(hits[0].metadata))

    # chunk_id 有值但不存在 → 报错（与 SQL 后端一致；内存后端才视为新建）
    ok, info = await _raises(
        store.add([VectorRecord(chunk_id=987654, vector=V_X)]), VectorStoreInputError)
    _check("★ chunk_id 指向不存在的切片 → 报错（不悄悄新建）",
           ok and "987654" in str(info), str(info))

    # 新建切片缺 document_id / 空正文 → 报错（校验口径来自 SQL 后端，不重写一份）
    ok, info = await _raises(
        store.add([VectorRecord(vector=V_X, content="有正文")]), VectorStoreInputError)
    _check("  └ 新建切片缺 document_id → 报错", ok and "document_id" in str(info), str(info))
    ok, info = await _raises(
        store.add([VectorRecord(vector=V_X, document_id=document_id, content="   ")]),
        VectorStoreInputError)
    _check("  └ 新建切片正文全空白 → 报错", ok and "content" in str(info), str(info))


# ------------------------------------------------------------
# [4] 查询 top_k（测试要求 2）
# ------------------------------------------------------------
async def _suite_search_top_k(session):
    print("\n[4] 查询 top_k（测试要求 2）：截断 / min_score / 三种过滤")

    document_id = await _seed_document(session, title="查询 top_k")
    other_document_id = await _seed_document(session, title="另一个文档")
    store = _store(session, model="m1")
    await store.add(_records(document_id))
    await store.add(_records(other_document_id, model="m2"))

    hits = await store.search(V_X, top_k=1, model="m1")
    _check("★ top_k=1 → 只回 1 条", len(hits) == 1, str(_brief(hits)))
    _check("  └ 且是相似度最高的那条（向量 x 命中自己）",
           hits[0].content == "第一条：向量 x", str(_brief(hits)))

    hits = await store.search(V_X, top_k=2, model="m1")
    _check("  └ top_k=2 → 2 条", len(hits) == 2, str(_brief(hits)))
    hits = await store.search(V_X, top_k=99, model="m1")
    _check("  └ top_k 超过候选数 → 返回全部候选（不报错）", len(hits) == 3, str(_brief(hits)))
    _check("  └ 不传 top_k → 用默认值（这里候选少于默认值，返回全部）",
           len(await store.search(V_X, model="m1")) == 3)

    ok, info = await _raises(store.search(V_X, top_k=0, model="m1"), VectorStoreInputError)
    _check("  └ top_k=0 → 报错（检索语义参数不静默收敛）", ok and "top_k" in str(info), str(info))

    # min_score
    hits = await store.search(V_X, top_k=3, model="m1", min_score=0.9)
    _check("★ min_score 过滤：只剩 x（1.0）", len(hits) == 1 and hits[0].content == "第一条：向量 x",
           str(_brief(hits)))
    hits = await store.search(V_X, top_k=3, model="m1", min_score=0.5)
    _check("  └ 阈值 0.5 → x(1.0) + x+y(0.7071)", len(hits) == 2, str(_brief(hits)))
    _check("  └ 阈值 1.01（余弦上限之上）→ 空", await store.search(
        V_X, top_k=3, model="m1", min_score=1.01) == [])

    # model 过滤
    hits = await store.search(V_X, top_k=5, model="m2")
    _check("★ model 过滤：只看 m2 的向量（不同模型的向量不可比）",
           len(hits) == 3 and all(h.model == "m2" for h in hits), str(_brief(hits)))
    _check("  └ 不传 model → 用构造函数的默认值 m1",
           len(await store.search(V_X, top_k=5)) == 3)

    # document_id 过滤
    hits = await store.search(V_X, top_k=5, model="m2", document_id=other_document_id)
    _check("★ document_id 过滤（真列语义）",
           len(hits) == 3 and all(h.document_id == other_document_id for h in hits),
           str(_brief(hits)))
    _check("  └ 指向不存在的文档 → 空", await store.search(
        V_X, top_k=5, model="m2", document_id=999999) == [])

    # category 过滤：在**索引层**完成（SQL 后端是 Python 侧过滤）
    hits = await store.search(V_X, top_k=5, model="m1", category="job")
    _check("★ category 过滤：只剩 job 那一条（第二条）",
           len(hits) == 1 and hits[0].content == "第二条：向量 y", str(_brief(hits)))
    hits = await store.search(V_X, top_k=5, model="m1", category="technical")
    _check("  └ category=technical → 两条", len(hits) == 2, str(_brief(hits)))
    _check("  └ 多条件同时生效（model + document_id + category）",
           len(await store.search(
               V_X, top_k=5, model="m1", document_id=document_id, category="job")) == 1)

    # 空索引
    empty = _store(session, model="m1")
    _check("★ 空索引 → []（而不是抛维度错）", await empty.search(V_X, top_k=3) == [])
    _check("  └ 空索引 count == 0", await empty.count() == 0)


# ------------------------------------------------------------
# [5] 返回结果正确（测试要求 3）
# ------------------------------------------------------------
async def _suite_result_correctness(session):
    print("\n[5] 返回结果正确（测试要求 3）：score 是精确余弦 + 降序 + to_dict 形状")

    document_id = await _seed_document(session, title="结果正确性")
    store = _store(session, model="m1")
    await store.add(_records(document_id))

    hits = await store.search(V_X, top_k=3, model="m1")
    _check("★ 命中自己时 score ≈ 1.0",
           abs(hits[0].score - 1.0) < 1e-9, str(hits[0].score))
    _check("★ score 与 cosine_similarity **逐位一致**（精确余弦，不是距离换算）",
           round(hits[1].score, 9) == round(cosine_similarity(V_X, V_XY), 9),
           f"{hits[1].score} vs {cosine_similarity(V_X, V_XY)}")
    _check("  └ 第三名 score 也逐位一致（正交向量 x·y = 0）",
           round(hits[2].score, 9) == round(cosine_similarity(V_X, V_Y), 9) == 0.0,
           f"{hits[2].score} vs {cosine_similarity(V_X, V_Y)}")
    _check("  └ x·(x+y) = 1/√2 ≈ 0.707106781",
           abs(hits[1].score - 0.7071067811865475) < 1e-9, str(hits[1].score))
    _check("  └ 顺序：x(1.0) > x+y(0.7071) > y(0.0)",
           [h.content for h in hits] == ["第一条：向量 x", "第三条：向量 x+y", "第二条：向量 y"],
           str(_brief(hits)))
    _check("  └ 结果按 score 降序",
           all(hits[i].score >= hits[i + 1].score for i in range(len(hits) - 1)),
           str([round(h.score, 6) for h in hits]))

    _check("  └ 元数据里的非标量值**保真**回传（索引层也是完整元数据）",
           hits[0].metadata.get("tags") == ["redis", "cache"], str(hits[0].metadata))
    _check("  └ to_dict 形状 == VECTOR_MATCH_FIELDS（不含向量本体）",
           set(hits[0].to_dict()) == set(vs.VECTOR_MATCH_FIELDS), str(sorted(hits[0].to_dict())))
    _check("  └ to_dict 里带 score", isinstance(hits[0].to_dict()["score"], float))
    _check("  └ chunk_id 是整数（与权威行主键一致）",
           all(isinstance(h.chunk_id, int) for h in hits), str([h.chunk_id for h in hits]))

    # 同分兜底：两条正交向量对同一个查询可能同分时按 chunk_id 升序
    tie_store = _store(session, model="tie")
    tie_doc = await _seed_document(session, title="同分兜底")
    await tie_store.add([
        VectorRecord(vector=V_Y, content="同分 A", document_id=tie_doc, metadata={"category": "technical"}, model="tie"),
        VectorRecord(vector=[0.0, 1.0, 0.0], content="同分 B", document_id=tie_doc, metadata={"category": "technical"}, model="tie"),
    ])
    tie_hits = await tie_store.search(V_Y, top_k=2, model="tie")
    _check("  └ 同分时按 chunk_id 升序（与 SQL 后端的主键升序口径一致）",
           [h.chunk_id for h in tie_hits] == sorted(h.chunk_id for h in tie_hits),
           str([(h.chunk_id, round(h.score, 6)) for h in tie_hits]))
    _check("  └ 两条 score 确实相同（构造有效）",
           abs(tie_hits[0].score - tie_hits[1].score) < 1e-12,
           str([h.score for h in tie_hits]))


# ------------------------------------------------------------
# [6] 换后端不换语义
# ------------------------------------------------------------
async def _suite_backend_swap(session):
    print("\n[6] 换后端不换语义：内存 / SQL / chroma 三后端结果一致")

    document_id = await _seed_document(session, title="换后端")

    # 关键：三个后端必须拿到**同一批记录**（含同一个 chunk_id），
    # 否则比的是「三份不同的数据」而不是「三个后端的语义」。
    # 做法：先让权威后端建行并回读，再把这批记录灌给另外两个后端。
    sql = SqlAlchemyVectorStore(session, model="m1")
    ids = await sql.add_returning_ids(_records(document_id))
    records = await sql.load_records(ids)
    _check("  └ 权威后端建了 3 行并回读成 3 条记录", len(records) == 3, str(len(records)))

    memory = InMemoryVectorStore(model="m1")
    await memory.add(records)
    chroma = _store(session, model="m1")
    await chroma.add(records)                   # chunk_id 已存在 → 补向量 + 镜像

    stores = (memory, sql, chroma)
    for top_k in (1, 2, 3):
        results = [
            _brief(await store.search(V_X, top_k=top_k, model="m1")) for store in stores
        ]
        _check(f"★ top_k={top_k}：三个后端结果**完全一致**（含 chunk_id 与 score）",
               results[0] == results[1] == results[2],
               str(results))

    for kwargs in ({"min_score": 0.9}, {"category": "job"}, {"document_id": document_id}):
        results = [
            _brief(await store.search(V_X, top_k=5, model="m1", **kwargs))
            for store in stores
        ]
        _check(f"  └ 过滤 {kwargs} 时也一致", results[0] == results[1] == results[2],
               str(results))

    counts = [await store.count() for store in stores]
    _check("  └ 三后端 count 一致", counts[0] == counts[1] == counts[2], str(counts))


# ------------------------------------------------------------
# [7] ANN 维度契约
# ------------------------------------------------------------
async def _suite_dimension_contract(session):
    print("\n[7] ANN 维度契约：单索引单维度，且**在写库之前**就报错")

    document_id = await _seed_document(session, title="维度契约")
    store = _store(session, model="m1")

    rows_before = await session.scalar(
        select(func.count()).select_from(KnowledgeChunk)
        .where(KnowledgeChunk.document_id == document_id))

    # 批次内混维度
    ok, info = await _raises(store.add([
        VectorRecord(vector=V_X, document_id=document_id, content="3 维", model="m1"),
        VectorRecord(vector=[1.0, 0.0, 0.0, 0.0], document_id=document_id, content="4 维", model="m1"),
    ]), VectorStoreDimensionError)
    _check("★ 同一批次里混用维度 → VectorStoreDimensionError",
           ok and "多种维度" in str(info), str(info))

    rows_after = await session.scalar(
        select(func.count()).select_from(KnowledgeChunk)
        .where(KnowledgeChunk.document_id == document_id))
    _check("★ 且**没有**写进权威表（校验在写库之前 → 不留半截状态）",
           rows_before == rows_after, f"{rows_before} -> {rows_after}")
    _check("  └ 索引也仍然为空", await store.count() == 0, str(await store.count()))

    # 先建 3 维索引，再写 4 维
    await store.add(_records(document_id))
    ok, info = await _raises(store.add([
        VectorRecord(vector=[1.0, 0.0, 0.0, 0.0], document_id=document_id,
                     content="4 维", model="m1"),
    ]), VectorStoreDimensionError)
    _check("★ 与索引已有维度不符 → VectorStoreDimensionError（HNSW 只支持单一维度）",
           ok and "4 维" in str(info), str(info))

    # 查询维度不符
    ok, info = await _raises(store.search([1.0, 0.0, 0.0, 0.0], top_k=3, model="m1"),
                             VectorStoreDimensionError)
    _check("★ 查询维度与索引不符 → VectorStoreDimensionError（不静默返回空）",
           ok and "查询向量" in str(info), str(info))

    # 对照：SQL 后端允许混存不同维度（逐行记 embedding_dim），行为差异是刻意的
    sql_store = SqlAlchemyVectorStore(session, model="dim")
    other_doc = await _seed_document(session, title="SQL 混维度")
    await sql_store.add([
        VectorRecord(vector=V_X, document_id=other_doc, content="3 维", model="dim"),
        VectorRecord(vector=[1.0, 0.0, 0.0, 0.0], document_id=other_doc,
                     content="4 维", model="dim"),
    ])
    other_rows = (await session.execute(
        select(KnowledgeChunk).where(KnowledgeChunk.document_id == other_doc)
        .order_by(KnowledgeChunk.id))).scalars().all()
    _check("  └ 对照：SQL 后端允许混存不同维度（它逐行记维度，不是 ANN 索引）",
           len(other_rows) == 2
           and sorted(row.embedding_dim for row in other_rows) == [3, 4],
           str([row.embedding_dim for row in other_rows]))


# ------------------------------------------------------------
# [8] 持久化
# ------------------------------------------------------------
async def _suite_persistence(session_factory, tmpdirs):
    print("\n[8] 持久化：重开客户端索引仍在（真实向量库，不是进程内实现）")

    path = tempfile.mkdtemp(prefix="chroma_ann_")
    tmpdirs.append(path)
    #: 同一个集合名贯穿「写 → 重开 → 读」，否则重开后看到的是另一个空集合
    collection = _collection_name()

    def _client():
        return CHROMADB.PersistentClient(
            path=path, settings=CHROMADB.Settings(anonymized_telemetry=False))

    async with session_factory() as session:
        document_id = await _seed_document(session, title="持久化")
        store = _store(session, model="m1", client=_client(), collection_name=collection)
        await store.add(_records(document_id))
        _check("  └ 写入后索引有 3 条", await store.count() == 3, str(await store.count()))
        first = _brief(await store.search(V_X, top_k=3, model="m1"))

    # 重开客户端（模拟进程重启）
    async with session_factory() as session:
        store2 = _store(session, model="m1", client=_client(), collection_name=collection)
        _check("★ 重开客户端后索引数据**仍在**（真的落盘了）",
               await store2.count() == 3, str(await store2.count()))
        again = _brief(await store2.search(V_X, top_k=3, model="m1"))
        _check("★ 检索结果与重开前完全一致", again == first, f"{again} vs {first}")
        _check("  └ 索引条数与权威已向量化条数一致（索引是派生数据）",
               await store2.count() == await SqlAlchemyVectorStore(session).count(),
               f"{await store2.count()} vs {await SqlAlchemyVectorStore(session).count()}")
        _check("  └ 集合名可显式指定（默认与权威表同名，便于排查）",
               store2.collection_name == collection
               and DEFAULT_COLLECTION == "knowledge_chunk", store2.collection_name)

    # open_client 走 CHROMA_PATH 环境变量（不注入 client，完全靠配置）
    os.environ["CHROMA_PATH"] = path
    try:
        async with session_factory() as session:
            # 刻意**不传 client**：构造时会走 open_client(path=CHROMA_PATH)
            opened = ChromaVectorStore(session, collection_name=collection, model="m1")
            _check("★ open_client 按 CHROMA_PATH 打开持久化客户端（不需要注入 client）",
                   await opened.count() == 3, str(await opened.count()))
            _check("  └ 因此不注入 client 也能检索到刚写的索引",
                   len(await opened.search(V_X, top_k=3, model="m1")) == 3)
    finally:
        os.environ.pop("CHROMA_PATH", None)

    # 构造参数问题 → ChromaConfigError（ValueError：调用方改代码即可解决）
    for label, factory in (
        ("没给 db", lambda: ChromaVectorStore(None)),
        ("集合名为空", lambda: ChromaVectorStore(object(), collection_name="")),
        ("oversample=0", lambda: ChromaVectorStore(object(), oversample=0)),
        ("oversample 是 bool", lambda: ChromaVectorStore(object(), oversample=True)),
    ):
        ok, info = _raises_sync(factory, ChromaConfigError)
        _check(f"  └ {label} → ChromaConfigError", ok, str(info))

    ok, info = _raises_sync(lambda: open_client(path="   "), ChromaConfigError)
    _check("  └ 索引目录是纯空白 → ChromaConfigError（不悄悄退回默认目录）", ok, str(info))

    class _BrokenClient:
        def get_or_create_collection(self, **_kwargs):
            raise RuntimeError("模拟底层索引打不开")

    ok, info = _raises_sync(
        lambda: ChromaVectorStore(object(), client=_BrokenClient())._collection_handle(),
        ChromaBackendError)
    _check("★ 底层索引打不开 → 包成领域异常（不是裸 RuntimeError）",
           ok and "RuntimeError" in str(info), str(info))



# ------------------------------------------------------------
# [9] 依赖收口（AST）
# ------------------------------------------------------------
def _suite_dependency_closure():
    print("\n[9] 依赖收口：chromadb 延迟导入、接口零依赖未被破坏、消费者闭集")

    interface = BACKEND_DIR / "services" / "vector_store.py"
    interface_top = _top_level_imports(interface.read_text(encoding="utf-8"))
    _check("★ 接口模块仍是**零第三方依赖**（这正是 ANN 后端必须另起模块的原因）",
           interface_top == {"__future__", "abc", "collections.abc", "dataclasses", "typing"},
           str(sorted(interface_top)))
    _check("  └ 接口模块不 import 任何具体后端",
           not any("vector_store_sql" in m or "vector_store_chroma" in m
                   for m in _imported_modules(interface.read_text(encoding="utf-8"))))

    chroma_path = BACKEND_DIR / "services" / "vector_store_chroma.py"
    chroma_src = chroma_path.read_text(encoding="utf-8")
    chroma_top = _top_level_imports(chroma_src)
    _check("★ chromadb **不在模块顶层** import（默认后端不装它也能跑）",
           "chromadb" not in chroma_top, str(sorted(chroma_top)))
    _check("  └ 它只在函数体内延迟导入",
           "chromadb" in _imported_modules(chroma_src), str(sorted(chroma_top)))
    _check("  └ load_chromadb() 未安装时给可读的领域异常（不是裸 ImportError）",
           "ChromaBackendError" in chroma_src and "pip install chromadb" in chroma_src)

    rag_src = (BACKEND_DIR / "services" / "knowledge_rag.py").read_text(encoding="utf-8")
    rag_top = _top_level_imports(rag_src)
    _check("★ 组装器顶层仍只有标准库（缺 DATABASE_URL 也能 import）",
           not any(m.startswith("services") or m in ("chromadb", "sqlalchemy")
                   for m in rag_top), str(sorted(rag_top)))
    _check("  └ chroma 后端在组装器里也是**函数内延迟导入**",
           "services.vector_store_chroma" not in rag_top
           and "services.vector_store_chroma" in _imported_modules(rag_src))

    # 后端消费者闭集：只有组装器知道「用哪个后端」
    consumers: dict = {}
    for path in _production_files():
        if path.name in ("vector_store.py", "vector_store_sql.py", "vector_store_chroma.py"):
            continue
        rel = path.relative_to(BACKEND_DIR).as_posix()
        imports = _imported_modules(path.read_text(encoding="utf-8"))
        for module in ("vector_store_sql", "vector_store_chroma"):
            if module in imports or f"services.{module}" in imports:
                consumers.setdefault(module, []).append(rel)
    _check("★ 两个后端的消费者都**恰好**是 knowledge_rag（收口为唯一组装器）",
           consumers == {
               "vector_store_sql": ["services/knowledge_rag.py"],
               "vector_store_chroma": ["services/knowledge_rag.py"],
           },
           str(consumers))

    for rel in ("services/interview_core.py", "services/interview_agent.py",
                "services/interview_service.py", "services/knowledge_retriever.py",
                "services/vector_knowledge_retriever.py", "services/embedding_service.py"):
        _check(f"  └ {rel} 不碰 ANN 后端",
               not any("vector_store_chroma" in m or "chromadb" in m
                       for m in _imported_modules(
                           (BACKEND_DIR / rel).read_text(encoding="utf-8"))))

    retriever_src = (BACKEND_DIR / "services" / "vector_knowledge_retriever.py").read_text(
        encoding="utf-8")
    _check("★ 检索器仍只依赖**接口**（需求：不改 Retriever 调用方式）",
           "services.vector_store" in _imported_modules(retriever_src)
           and not any("vector_store_sql" in m or "vector_store_chroma" in m
                       for m in _imported_modules(retriever_src)))
    _check("  └ 检索器构造签名未变（embedder + store + 关键字参数）",
           "def __init__" in retriever_src and "store" in retriever_src)

    _check("  └ 默认过采样倍率 >= 1（给精确重排留腾挪空间）",
           DEFAULT_OVERSAMPLE >= 1 and ChromaVectorStore(
               object(), oversample=DEFAULT_OVERSAMPLE).oversample == DEFAULT_OVERSAMPLE,
           str(DEFAULT_OVERSAMPLE))
    _check("  └ ChromaVectorStore 是 VectorStore 的子类（接口未变）",
           issubclass(ChromaVectorStore, vs.VectorStore))
    _check("  └ 它没有重写 add/search/count 之外的接口方法",
           set(ChromaVectorStore.__dict__) & {
               "retrieve", "build_query", "embed"} == set(),
           str(sorted(ChromaVectorStore.__dict__)))


# ------------------------------------------------------------
# [10] 入库 Pipeline 端到端
# ------------------------------------------------------------
async def _suite_pipeline_end_to_end(session_factory):
    print("\n[10] 入库 Pipeline 端到端：文档 → 切片 → 向量 → ANN 索引（重跑幂等）")

    from services.knowledge_import_pipeline import KnowledgeImportPipeline
    from services.document_chunker import DocumentChunker

    content = "\n\n".join(
        f"第 {i} 段：Redis 的持久化方式与淘汰策略，需要结合业务场景权衡。"
        for i in range(1, 7)
    )

    async with session_factory() as session:
        store = ChromaVectorStore(
            session, client=_ephemeral_client(), model=None)
        pipeline = KnowledgeImportPipeline(
            session,
            chunker=DocumentChunker(chunk_size=120, chunk_overlap=20),
            embedder=HashEmbeddingService(dimension=32),
            store=store,
        )
        report = await pipeline.import_document({
            "title": "Redis 手册", "content": content,
            "category": "technical", "source": "manual://redis",
        })
        _check("★ 文档成功入库", report["ok"] and report["status"] == "ok", str(report))
        _check("  └ 产生了切片", report["chunk_count"] > 1, str(report["chunk_count"]))
        _check("  └ 每片都写了向量（embedded_chunks == chunk_count）",
               report["embedded_chunks"] == report["chunk_count"], str(report))
        _check("  └ 没有失败", report["errors"] == [] and report["error"] == "", str(report))

        index_count = await store.count()
        _check("★ ANN 索引里确实有这些向量", index_count == report["chunk_count"],
               f"{index_count} vs {report['chunk_count']}")

        # 检索真的能命中（用真实 Embedding 生成的向量）
        query = await HashEmbeddingService(dimension=32).embed("Redis 持久化方式")
        hits = await store.search(query, top_k=3)
        _check("★ 用查询向量能检索到入库的知识（返回 content + score）",
               len(hits) == 3 and all(h.content and isinstance(h.score, float) for h in hits),
               str(_brief(hits)))
        _check("  └ 检索结果的 chunk_id 都能对上权威行",
               all(isinstance(h.chunk_id, int) for h in hits),
               str([h.chunk_id for h in hits]))

        # 重跑：三层幂等仍然成立（证明「索引派生自权威行」没有破坏 Pipeline 契约）
        again = await pipeline.import_document({
            "title": "Redis 手册", "content": content,
            "category": "technical", "source": "manual://redis",
        })
        _check("★ 重跑幂等：status='skipped'（索引镜像**没有**破坏幂等判据）",
               again["ok"] and again["status"] == "skipped", str(again))
        _check("  └ 没有重复写向量", again["embedded_chunks"] == 0, str(again))
        _check("  └ 索引条数不变", await store.count() == index_count, str(await store.count()))

    # 组装器按 VECTOR_STORE 真的能造出可用的 Pipeline 后端
    async with session_factory() as session:
        built = rag.build_vector_store(session, backend="chroma")
        _check("★ 组装器造出的 ANN 后端可直接用于入库（默认参数即可）",
               isinstance(built, ChromaVectorStore), repr(built))


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
