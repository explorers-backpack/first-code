# -*- coding: utf-8 -*-
"""AI 面试知识库 · 向量存储层（VectorStore）自检

无需 pytest，直接运行：
    python backend/tests/test_vector_store.py

不依赖本机 MySQL：SQL 后端跑在 SQLite 内存库上（StaticPool）。
不联网、不需要密钥；相似度全用**手写向量**算，结果可精确断言。

覆盖范围
--------
1. **接口与零依赖**：``VectorStore`` 是抽象接口，签名里**没有任何数据库概念**；
   接口模块只 import 标准库（AST 顶层依赖**相等**断言）
2. **值对象**：``VectorRecord`` 自校验（构造即合法）、``VectorMatch``、``to_dict`` 形状
3. **相似度数学**：余弦 / 点积 / 模长、维度不符抛错、零向量不除零
4. **入参校验**：向量 / ``top_k`` / ``min_score`` / 批量记录类型
5. **保存向量（测试要求 1）**：内存实现 + SQL 实现（新建 / 给既有切片补向量 / 报错路径）
6. **相似查询（测试要求 2）**：排序、``top_k``、``min_score``、按模型 / 文档 / 分类过滤、
   维度不符的处理、空库返回 ``[]``、同分稳定排序
7. **换后端不换语义**：同一组数据在两个实现上检索结果一致
8. **端到端**：``HashEmbeddingService`` 生成向量 → 存 → 按向量查回自己
9. **未接线**：生产代码无任何模块 import 本层；不接 Retriever / Interview
"""

import ast
import asyncio
import inspect
import os
import pathlib
import sys

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import KnowledgeChunk, KnowledgeDocument  # noqa: E402
from services import vector_store as vs  # noqa: E402
from services.embedding_service import HashEmbeddingService  # noqa: E402
from services.vector_store import (  # noqa: E402
    DEFAULT_TOP_K,
    InMemoryVectorStore,
    VectorMatch,
    VectorRecord,
    VectorStore,
    VectorStoreDimensionError,
    VectorStoreError,
    VectorStoreInputError,
    cosine_similarity,
    dot_product,
    l2_norm,
    require_records,
    require_top_k,
)
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

_PASSED = 0
_FAILED = 0

#: 手写正交向量：便于精确断言相似度（1.0 / 0.7071 / 0.0）
V_X = [1.0, 0.0, 0.0]
V_Y = [0.0, 1.0, 0.0]
V_XY = [1.0, 1.0, 0.0]


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

    用于「可选依赖必须延迟导入」这类断言：``_imported_modules`` 会把函数体内的
    import 也算进来，区分不出「顶层 import」与「延迟 import」。
    """
    names: set = set()
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def _production_files():
    """后端生产代码（排除 tests / __pycache__），用于「未接线」检查。"""
    files = []
    for pattern in ("*.py", "api/*.py", "services/*.py", "models/*.py",
                    "schemas/*.py", "utils/*.py"):
        for path in BACKEND_DIR.glob(pattern):
            if "__pycache__" in path.parts:
                continue
            files.append(path)
    return sorted(set(files))


async def _raises(coro, exc_type):
    """执行协程，返回 (是否抛了指定异常, 异常实例或说明)。"""
    try:
        await coro
    except exc_type as exc:
        return True, exc
    except Exception as exc:  # noqa: BLE001
        return False, f"抛了非预期异常 {type(exc).__name__}: {exc}"
    return False, "未抛异常"


def _records(document_id, model="m1", *, metadata=None):
    """三条手写向量记录（x / y / x+y），顺序固定，便于断言排序。

    元信息里把 ``job`` 放在**第二条**：这样「按 category 过滤」的结果
    与「按相似度排序」的结果不同（y 的相似度排第二），能证明过滤真的生效、
    而不是碰巧靠排序过的。
    """
    meta = metadata or [
        {"category": "technical", "chunk_index": 0},
        {"category": "job", "chunk_index": 1},
        {"category": "technical", "chunk_index": 2},
    ]
    return [
        VectorRecord(vector=V_X, content="第一条：向量 x", document_id=document_id,
                     metadata=meta[0], model=model),
        VectorRecord(vector=V_Y, content="第二条：向量 y", document_id=document_id,
                     metadata=meta[1], model=model),
        VectorRecord(vector=V_XY, content="第三条：向量 x+y", document_id=document_id,
                     metadata=meta[2], model=model),
    ]


def _brief(matches):
    """把检索结果压成可比对的形状（去掉浮点尾差）。

    **刻意不含 ``chunk_id``**：内存实现是自增 id、SQL 实现是数据库主键，
    两者「值」未必相同（虽然本测试里恰好都是 1/2/3），拿来比对会把
    「换后端不换语义」的断言变成对 id 分配方式的隐式依赖。
    """
    return [(m.content, round(m.score, 9)) for m in matches]


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 面试知识库 · 向量存储层（VectorStore）自检")
    print("=" * 70)

    # ------------------------------------------------------------
    # [1] 接口 + 零依赖
    # ------------------------------------------------------------
    print("\n[1] VectorStore 接口 + 零依赖（不绑定任何数据库）")
    _check("services.vector_store 可导入", hasattr(vs, "VectorStore"))
    _check("★ VectorStore 是抽象接口，三个方法都未实现",
           getattr(VectorStore, "__abstractmethods__", None)
           == frozenset({"add", "search", "count"}),
           str(getattr(VectorStore, "__abstractmethods__", None)))
    try:
        VectorStore()
        _check("  └ 直接实例化被拒", False, "竟然实例化成功了")
    except TypeError:
        _check("  └ 直接实例化被拒（接口必须被实现）", True)

    add_params = list(inspect.signature(VectorStore.add).parameters)
    search_params = list(inspect.signature(VectorStore.search).parameters)
    _check("★ 接口签名里没有任何数据库概念（无 db / session / engine）",
           not any(p in ("db", "session", "engine", "conn", "table")
                   for p in add_params + search_params),
           str(add_params + search_params))
    _check("  └ add 只收一组记录", add_params == ["self", "records"], str(add_params))
    _check("  └ search 的过滤参数齐全（top_k / model / document_id / category / min_score）",
           {"top_k", "model", "document_id", "category", "min_score"} <= set(search_params),
           str(search_params))
    _check("默认 top_k 是正整数", isinstance(DEFAULT_TOP_K, int) and DEFAULT_TOP_K > 0,
           str(DEFAULT_TOP_K))

    src = (BACKEND_DIR / "services" / "vector_store.py").read_text(encoding="utf-8")
    top_level = {m.split(".")[0] for m in _imported_modules(src)}
    _check("★ 接口模块只 import 标准库（顶层依赖恰为 5 个）",
           top_level == {"__future__", "abc", "collections", "dataclasses", "typing"},
           str(sorted(top_level)))
    _check("  └ 不 import 任何数据库 / 向量库 / 业务模块",
           not ({"sqlalchemy", "models", "database", "fastapi", "pydantic", "deps",
                 "main", "numpy", "chromadb", "faiss", "milvus", "qdrant",
                 "pinecone", "weaviate"} & top_level)
           and not any("interview" in m or "embedding_service" in m
                       or "knowledge_retriever" in m for m in _imported_modules(src)),
           str(sorted(_imported_modules(src))))

    # ------------------------------------------------------------
    # [2] 值对象
    # ------------------------------------------------------------
    print("\n[2] 值对象 VectorRecord / VectorMatch")
    record = VectorRecord(vector=[1, 0, 0], content="正文", document_id=3,
                          chunk_id=9, metadata={"category": "job"}, model="m1")
    _check("★ 构造时把 int 向量归一成 float",
           record.vector == [1.0, 0.0, 0.0] and all(isinstance(v, float) for v in record.vector),
           str(record.vector))
    _check("  └ dimension 反映向量长度", record.dimension == 3, str(record.dimension))
    _check("  └ to_dict 键集合固定（顺序稳定）",
           tuple(record.to_dict()) == vs.VECTOR_RECORD_FIELDS,
           str(tuple(record.to_dict())))
    _check("  └ to_dict 里的容器是副本（改它不影响对象）",
           (lambda d: (d["metadata"].__setitem__("x", 1),
                       record.metadata.get("x") is None)[1])(record.to_dict()))
    _check("  └ 是 frozen dataclass（不可重新赋值）",
           VectorRecord.__dataclass_params__.frozen is True)
    _check("  └ metadata 默认是独立空 dict（不是共享字面量）",
           VectorRecord(vector=[1.0]).metadata == {}
           and VectorRecord(vector=[1.0]).metadata is not VectorRecord(vector=[1.0]).metadata)

    for label, kwargs in {
        "空向量": {"vector": []},
        "字符串": {"vector": "abc"},
        "含 bool": {"vector": [True]},
        "含字符串元素": {"vector": ["0.1"]},
        "含 None": {"vector": [None]},
        "不可迭代": {"vector": 42},
    }.items():
        try:
            VectorRecord(**kwargs)
            _check(f"★ 构造即拒绝非法向量：{label}", False, "未抛异常")
        except VectorStoreInputError:
            _check(f"★ 构造即拒绝非法向量：{label}", True)

    match = VectorMatch(chunk_id=1, document_id=2, content="命中", metadata={"a": 1},
                        model="m1", score=0.75)
    _check("VectorMatch.to_dict 键集合固定",
           tuple(match.to_dict()) == vs.VECTOR_MATCH_FIELDS, str(tuple(match.to_dict())))
    _check("  └ 检索结果**不含向量本体**（不带上千个浮点回上层）",
           "vector" not in match.to_dict())
    _check("  └ score 归一为 float", isinstance(match.score, float))

    # ------------------------------------------------------------
    # [3] 相似度数学
    # ------------------------------------------------------------
    print("\n[3] 相似度数学")
    _check("点积", dot_product(V_X, V_XY) == 1.0)
    _check("模长", abs(l2_norm([3.0, 4.0]) - 5.0) < 1e-12, str(l2_norm([3.0, 4.0])))
    _check("★ 自己与自己余弦 == 1", abs(cosine_similarity(V_X, V_X) - 1.0) < 1e-12)
    _check("★ 正交向量余弦 == 0", abs(cosine_similarity(V_X, V_Y)) < 1e-12)
    _check("★ 相反向量余弦 == -1", abs(cosine_similarity(V_X, [-1.0, 0.0, 0.0]) + 1.0) < 1e-12)
    _check("45 度向量余弦 ≈ 0.7071",
           abs(cosine_similarity(V_X, V_XY) - 2 ** -0.5) < 1e-12,
           str(cosine_similarity(V_X, V_XY)))
    _check("  └ 与向量长度无关（只看向量方向）",
           abs(cosine_similarity(V_X, [10.0, 0.0, 0.0]) - 1.0) < 1e-12)
    _check("零向量不除零（返回 0.0）",
           cosine_similarity([0.0, 0.0, 0.0], V_X) == 0.0)
    try:
        cosine_similarity(V_X, V_Y[:2])
        _check("★ 维度不符抛 VectorStoreDimensionError（不静默截断）", False, "未抛异常")
    except VectorStoreDimensionError:
        _check("★ 维度不符抛 VectorStoreDimensionError（不静默截断）", True)

    # ------------------------------------------------------------
    # [4] 入参校验
    # ------------------------------------------------------------
    print("\n[4] 入参校验")
    for label, bad in {"0": 0, "-1": -1, "True": True, "1.5": 1.5, "字符串": "3",
                       "None": None}.items():
        try:
            require_top_k(bad)
            _check(f"拒绝非法 top_k：{label}", False, "未抛异常")
        except VectorStoreInputError:
            _check(f"拒绝非法 top_k：{label}", True)
    _check("top_k=1 合法", require_top_k(1) == 1)

    _check("★ add 不接受单个对象（要包成 [record]）",
           all(_raises_sync(lambda r=r: require_records(r), VectorStoreInputError)
               for r in (record, {"vector": [1.0]}, "abc")))
    _check("  └ add([]) 合法（空批次）", require_records([]) == [])
    _check("  └ dict 形式可归一成 VectorRecord",
           require_records([{"vector": [1.0, 2.0], "content": "x"}])[0].dimension == 2)
    _check("  └ 非记录类型被拒（错误信息含第几条）",
           _raises_sync(lambda: require_records([record, 123]), VectorStoreInputError)
           and "第 1 条" in _raises_sync(lambda: require_records([record, 123]),
                                        VectorStoreInputError)[1].args[0])

    # ------------------------------------------------------------
    # [5] 内存实现：保存向量（测试要求 1）
    # ------------------------------------------------------------
    print("\n[5] 保存向量（内存实现）")
    memory = InMemoryVectorStore(model="m1")
    _check("初始为空", await memory.count() == 0)
    written = await memory.add(_records(document_id=1))
    _check("★ add 返回实际写入条数", written == 3, str(written))
    _check("★ count 反映已向量化条数", await memory.count() == 3, str(await memory.count()))
    _check("  └ 未给 chunk_id 时自动分配自增 id",
           [r.chunk_id for r in memory.records] == [1, 2, 3],
           str([r.chunk_id for r in memory.records]))
    _check("  └ 记录自带 model 时以记录为准",
           all(r.model == "m1" for r in memory.records))

    upsert = VectorRecord(vector=[0.0, 1.0, 0.0], content="（不该被写入的正文）",
                          document_id=1, chunk_id=1, metadata={"category": "job"},
                          model="m1")
    _check("★ upsert：同 chunk_id 覆盖而非新增",
           await memory.add([upsert]) == 1 and await memory.count() == 3,
           str(await memory.count()))
    _check("★ 补向量只更新 vector（新向量生效）",
           memory.records[0].vector == [0.0, 1.0, 0.0], str(memory.records[0].vector))
    _check("★ 补向量**不改切片正文**（向量层不负责切片内容）",
           memory.records[0].content == "第一条：向量 x", memory.records[0].content)
    _check("  └ 补向量也不改切片元信息 / 归属文档",
           memory.records[0].metadata.get("category") == "technical"
           and memory.records[0].document_id == 1,
           f"{memory.records[0].metadata} / {memory.records[0].document_id}")
    _check("空批次写入 0 条", await memory.add([]) == 0)

    defaulted = InMemoryVectorStore(model="m-default")
    await defaulted.add([VectorRecord(vector=V_X, content="无模型记录", document_id=1)])
    _check("记录未带 model 时用 store 的默认模型补齐（避免无主向量）",
           defaulted.records[0].model == "m-default", defaulted.records[0].model)

    # ------------------------------------------------------------
    # [6] 内存实现：相似查询（测试要求 2）
    # ------------------------------------------------------------
    print("\n[6] 相似查询（内存实现）")
    store = InMemoryVectorStore(model="m1")
    await store.add(_records(document_id=1))

    hits = await store.search(V_X, top_k=3, model="m1")
    _check("★ 查询向量 == 第一条时，第一条排第一",
           hits[0].content == "第一条：向量 x", str(_brief(hits)))
    _check("★ 相似度排序正确：x(1.0) > x+y(0.707) > y(0.0)",
           [round(h.score, 6) for h in hits] == [1.0, round(2 ** -0.5, 6), 0.0],
           str([round(h.score, 6) for h in hits]))
    _check("  └ 返回 VectorMatch 且带 chunk_id / document_id",
           all(isinstance(h, VectorMatch) and h.chunk_id and h.document_id == 1
               for h in hits))
    _check("★ top_k 截断生效", len(await store.search(V_X, top_k=1, model="m1")) == 1)
    _check("  └ top_k 大于候选数时返回全部",
           len(await store.search(V_X, top_k=99, model="m1")) == 3)

    filtered = await store.search(V_X, top_k=5, model="m1", min_score=0.5)
    _check("★ min_score 过滤低相似结果", len(filtered) == 2, str(_brief(filtered)))
    _check("  └ 阈值高于余弦上限（1.0）时返回空",
           await store.search(V_X, top_k=5, model="m1", min_score=1.01) == [])

    _check("★ 按 category 过滤（取自 metadata）",
           len(await store.search(V_X, top_k=5, model="m1", category="job")) == 1)
    _check("  └ 按 document_id 过滤",
           await store.search(V_X, top_k=5, model="m1", document_id=999) == [])
    _check("  └ 按 model 过滤（另一模型查不到）",
           await store.search(V_X, top_k=5, model="other") == [])

    _check("★ 空库查询返回 []（确实没有候选，不报错）",
           await InMemoryVectorStore().search(V_X) == [])

    mixed = InMemoryVectorStore(model="m1")
    await mixed.add([VectorRecord(vector=V_X, content="三维", document_id=1, model="m1")])
    await mixed.add([VectorRecord(vector=[1.0, 0.0], content="二维", document_id=1, model="m1")])
    # 查询 4 维：库里有 3 维和 2 维，**一条都对不上** → 必须报错而不是返回 []
    ok, info = await _raises(mixed.search([1.0, 0.0, 0.0, 0.0], model="m1"),
                             VectorStoreDimensionError)
    _check("★ 候选非空但维度全不符 → 抛错（不安静返回空）",
           ok and "不可比" in str(info), str(info))
    partial = await mixed.search([1.0, 0.0], model="m1")
    _check("  └ 部分维度不符时跳过不符的，其余照常返回",
           len(partial) == 1 and partial[0].content == "二维", str(_brief(partial)))

    tied = InMemoryVectorStore(model="m1")
    await tied.add([
        VectorRecord(vector=V_X, content="同分 A", document_id=1, model="m1"),
        VectorRecord(vector=V_X, content="同分 B", document_id=1, model="m1"),
    ])
    tie_hits = await tied.search(V_X, top_k=2, model="m1")
    _check("★ 同分时按候选顺序稳定排序（同输入同输出）",
           [h.content for h in tie_hits] == ["同分 A", "同分 B"], str(_brief(tie_hits)))

    # ------------------------------------------------------------
    # 建库（SQL 实现用）
    # ------------------------------------------------------------
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with Session() as db:
        doc = KnowledgeDocument(title="Redis 手册", content="RDB 与 AOF",
                                category="technical", source="manual://redis")
        db.add(doc)
        await db.commit()
        await db.refresh(doc)
        # 提前取出主键：后面会 db.expire_all() 让实例过期，届时再读 doc.id
        # 会在协程外触发惰性加载（MissingGreenlet）。主键是稳定值，先拿下来最省事。
        doc_id = doc.id

        # --------------------------------------------------------
        # [7] SQL 实现：保存向量
        # --------------------------------------------------------
        print("\n[7] 保存向量（SQL 实现，复用 knowledge_chunk.embedding 扩展字段）")
        sql_store = SqlAlchemyVectorStore(db, model="m1")
        _check("初始无已向量化切片", await sql_store.count() == 0)
        _check("★ 空库查询返回 []（确实没有候选，不报错）",
               await sql_store.search(V_X, model="m1") == [])

        written = await sql_store.add(_records(document_id=doc_id))
        _check("★ add 返回写入条数", written == 3, str(written))
        _check("★ count == 3（只统计已向量化的行）", await sql_store.count() == 3)

        rows = (await db.execute(
            select(KnowledgeChunk).where(KnowledgeChunk.document_id == doc_id)
            .order_by(KnowledgeChunk.id))).scalars().all()
        # 同样提前取 id：下面 db.expire_all() 之后 rows 里的实例会过期
        first_id = rows[0].id
        _check("★ 新建切片确实落库（content + embedding 三列都写入）",
               len(rows) == 3
               and rows[0].embedding == [1.0, 0.0, 0.0]
               and rows[0].embedding_model == "m1"
               and rows[0].embedding_dim == 3,
               str([(r.embedding, r.embedding_model, r.embedding_dim) for r in rows]))
        _check("  └ metadata 落到 chunk_metadata（列名仍是 metadata）",
               rows[0].chunk_metadata.get("category") == "technical",
               str(rows[0].chunk_metadata))

        # 给既有切片补向量：走 chunk_id 的 upsert 路径
        target_id = rows[2].id
        await sql_store.add([VectorRecord(
            vector=[0.0, 0.0, 1.0], content="", document_id=doc_id,
            chunk_id=target_id, metadata={}, model="m2")])
        db.expire_all()
        refreshed = (await db.execute(
            select(KnowledgeChunk).where(KnowledgeChunk.id == target_id))).scalar_one()
        _check("★ 给既有切片补向量（upsert，不新增行）",
               await sql_store.count() == 3
               and refreshed.embedding == [0.0, 0.0, 1.0],
               str(refreshed.embedding))
        _check("  └ 模型名与维度同步更新（换模型后可筛旧向量）",
               refreshed.embedding_model == "m2" and refreshed.embedding_dim == 3,
               f"{refreshed.embedding_model}/{refreshed.embedding_dim}")
        _check("  └ 补向量不会覆盖既有 content",
               refreshed.content == "第三条：向量 x+y", refreshed.content)

        ok, info = await _raises(sql_store.add([VectorRecord(
            vector=V_X, content="x", document_id=doc_id, chunk_id=999999)]),
            VectorStoreInputError)
        _check("★ 给不存在的切片写向量 → 明确报错（不悄悄新建）",
               ok and "999999" in str(info), str(info))
        ok, info = await _raises(sql_store.add([VectorRecord(
            vector=V_X, content="x")]), VectorStoreInputError)
        _check("  └ 新建切片缺 document_id → 报错", ok, str(info))
        ok, info = await _raises(sql_store.add([VectorRecord(
            vector=V_X, content="   ", document_id=doc_id)]), VectorStoreInputError)
        _check("  └ 新建切片缺非空 content → 报错", ok, str(info))

        # --------------------------------------------------------
        # [8] SQL 实现：相似查询
        # --------------------------------------------------------
        print("\n[8] 相似查询（SQL 实现）")
        # 现在库里有：x / y / z(0,0,1)，其中 x、y 是 m1，z 是 m2
        hits = await sql_store.search(V_X, top_k=3, model="m1")
        _check("★ 查询 x：x(1.0) 排第一，y(0.0) 第二",
               [h.content for h in hits] == ["第一条：向量 x", "第二条：向量 y"],
               str(_brief(hits)))
        _check("★ 相似度数值正确",
               [round(h.score, 6) for h in hits] == [1.0, 0.0],
               str([round(h.score, 6) for h in hits]))
        _check("★ model 过滤生效（m2 的向量不参与 m1 的查询）",
               all(h.model == "m1" for h in hits), str([h.model for h in hits]))
        _check("  └ 查 m2 只能查到 z",
               [h.content for h in await sql_store.search(V_X, top_k=3, model="m2")]
               == ["第三条：向量 x+y"],
               str(_brief(await sql_store.search(V_X, top_k=3, model="m2"))))
        _check("  └ 不传 model 时用 store 默认模型（构造时给的 m1）",
               len(await sql_store.search(V_X, top_k=5)) == 2)
        _check("★ top_k 截断生效",
               len(await sql_store.search(V_X, top_k=1, model="m1")) == 1)
        _check("★ min_score 过滤生效",
               len(await sql_store.search(V_X, top_k=5, model="m1", min_score=0.5)) == 1)
        _check("★ 按 category 过滤（Python 侧，因 metadata 是 JSON）",
               [h.content for h in await sql_store.search(
                   V_X, top_k=5, model="m1", category="job")] == ["第二条：向量 y"],
               str(_brief(await sql_store.search(V_X, top_k=5, model="m1",
                                                 category="job"))))
        _check("  └ 按 document_id 过滤（SQL 侧，真列）",
               await sql_store.search(V_X, top_k=5, model="m1", document_id=999) == [])
        _check("  └ 返回的 chunk_id 是数据库真 id",
               hits[0].chunk_id == first_id, f"{hits[0].chunk_id} vs {first_id}")

        ok, info = await _raises(
            sql_store.search([1.0, 0.0], model="m1"), VectorStoreDimensionError)
        _check("★ 维度不符 → 抛错（不是安静返回空）", ok, str(info))

        # --------------------------------------------------------
        # [9] 换后端不换语义
        # --------------------------------------------------------
        print("\n[9] 换后端不换语义（同一数据、同一查询，两实现结果一致）")
        mirror = InMemoryVectorStore(model="m1")
        await mirror.add(_records(document_id=doc_id))
        # 只传 chunk_id + vector 补向量（**不传正文 / 元信息**，模拟批量向量化任务）：
        # 两实现都必须保留切片原有正文与元信息，只换掉向量与模型名。
        await mirror.add([VectorRecord(vector=[0.0, 0.0, 1.0], chunk_id=3, model="m2")])
        _check("★ 补向量不改切片正文（内存实现，与 SQL 实现同口径）",
               mirror.records[2].content == "第三条：向量 x+y",
               mirror.records[2].content)
        mem_hits = await mirror.search(V_X, top_k=5, model="m1")
        sql_hits = await sql_store.search(V_X, top_k=5, model="m1")
        _check("★ 两个实现的检索结果完全一致（接口把语义固定住了）",
               _brief(mem_hits) == _brief(sql_hits),
               f"mem={_brief(mem_hits)} sql={_brief(sql_hits)}")
        _check("  └ count 也一致", await mirror.count() == await sql_store.count())

        # --------------------------------------------------------
        # [10] 端到端：真实向量化 → 存 → 查回自己
        # --------------------------------------------------------
        print("\n[10] 端到端：HashEmbeddingService → VectorStore → 查回自己")
        emb = HashEmbeddingService(dimension=64)
        texts = ["Redis 持久化使用 RDB 与 AOF", "Kafka 的分区与副本机制",
                 "MySQL 索引下推优化"]
        vectors = await emb.embed_batch(texts)
        e2e = SqlAlchemyVectorStore(db, model=emb.name)
        await e2e.add([
            VectorRecord(vector=v, content=t, document_id=doc_id,
                         metadata={"category": "technical", "chunk_index": i},
                         model=emb.name)
            for i, (t, v) in enumerate(zip(texts, vectors))
        ])
        e2e_hits = await e2e.search(vectors[0], top_k=3, model=emb.name)
        _check("★ 用第一条文本的向量查询，命中它自己且相似度 ≈ 1",
               e2e_hits[0].content == texts[0] and abs(e2e_hits[0].score - 1.0) < 1e-9,
               str(_brief(e2e_hits)))
        _check("  └ 返回 3 条（该模型下全部候选）", len(e2e_hits) == 3)
        _check("  └ 结果按相似度降序",
               all(e2e_hits[i].score >= e2e_hits[i + 1].score for i in range(len(e2e_hits) - 1)),
               str([round(h.score, 4) for h in e2e_hits]))
        _check("  └ to_dict 可直接交给上层（无向量本体）",
               set(e2e_hits[0].to_dict()) == set(vs.VECTOR_MATCH_FIELDS))
        _check("  └ count 统计**全部**已向量化切片（不按模型区分）：3 + 3",
               await e2e.count() == 6, str(await e2e.count()))

    await engine.dispose()

    # ------------------------------------------------------------
    # [11] 依赖收口
    # ------------------------------------------------------------
    print("\n[11] 依赖收口：向量层的消费者是已知闭集（写侧入库 / 读侧检索）")
    # 「接口」与「后端实现」必须分开看：
    # - 接口 ``services.vector_store`` 零依赖，允许被**按注入消费**的模块 import。
    #   消费它的恰好两处，一读一写：
    #     * ``vector_knowledge_retriever``（**读侧**：面试出题时按向量检索）
    #     * ``knowledge_import_pipeline``（**写侧**：文档入库时把切片+向量写进去）
    # - 后端实现（``vector_store_sql`` / ``vector_store_chroma``）会把 SQLAlchemy +
    #   models 拉进来（chroma 还会拉进 chromadb），因此消费者必须**收口为唯一一个
    #   组装器** ``knowledge_rag``——「谁决定用哪个后端」只有一个答案，
    #   比「谁都不许碰」更可验证（后者在 RAG 真接入后就无法成立）。
    #   写侧也不直接碰它们：入库 Pipeline 经 ``knowledge_rag.build_vector_store`` 拿后端。
    # - 后端之间只允许**单向**组合：``vector_store_chroma`` 把 ``vector_store_sql``
    #   当「权威行读写器」组合进来（ANN 索引是派生数据），反向必须禁止。
    # - 面试流程模块（``interview_*``）**一律不许**直接碰向量层。
    BACKEND_FILES = {"vector_store_sql.py", "vector_store_chroma.py"}
    interface_consumers = []
    backend_consumers: dict = {}
    for path in _production_files():
        if path.name == "vector_store.py" or path.name in BACKEND_FILES:
            continue
        rel = path.relative_to(BACKEND_DIR).as_posix()
        imports = _imported_modules(path.read_text(encoding="utf-8"))
        if "vector_store" in imports or "services.vector_store" in imports:
            interface_consumers.append(rel)
        for module in ("vector_store_sql", "vector_store_chroma"):
            if module in imports or f"services.{module}" in imports:
                backend_consumers.setdefault(module, []).append(rel)

    # 第三个消费者是**维护模块**（任务 74：缺口③ 索引删 / 重建）：
    # 它构造 VectorRecord 并调用接口上的 add 来重建派生索引，
    # 只依赖**接口**（不 import 任何具体后端），因此仍属「已知闭集」。
    # 第四个是**向量迁移模块**（任务 76）：换 Embedding 模型后读既有切片、
    # 重算向量再经接口写回——同样只依赖接口（且经组装器拿后端）。
    _check("★ 消费向量存储**接口**的恰好是读侧检索器 + 写侧入库 Pipeline + 维护 + 迁移模块",
           interface_consumers == [
               "services/knowledge_embedding_migration.py",
               "services/knowledge_import_pipeline.py",
               "services/knowledge_maintenance.py",
               "services/vector_knowledge_retriever.py",
           ],
           str(interface_consumers))
    _check("★ 每个后端实现的消费者都**收口为唯一组装器** knowledge_rag",
           backend_consumers == {
               "vector_store_sql": ["services/knowledge_rag.py"],
               "vector_store_chroma": ["services/knowledge_rag.py"],
           },
           str(backend_consumers))
    all_backend_consumers = sorted({
        rel for rels in backend_consumers.values() for rel in rels
    })
    _check("  └ 面试流程模块（interview_*）都不直接碰向量层",
           not any(rel.startswith("services/interview")
                   for rel in interface_consumers + all_backend_consumers),
           str(interface_consumers + all_backend_consumers))

    sql_imports = _imported_modules(
        (BACKEND_DIR / "services" / "vector_store_sql.py").read_text(encoding="utf-8"))
    _check("SQL 后端确实依赖 SQLAlchemy 与模型（它就是那个 DB 后端）",
           "sqlalchemy" in sql_imports and "models" in sql_imports, str(sorted(sql_imports)))
    _check("  └ 但它不 import embedding_service / Retriever / Interview",
           not any("embedding_service" in m or "knowledge_retriever" in m
                   or "interview" in m for m in sql_imports), str(sorted(sql_imports)))
    _check("  └ 也不 import chroma 后端（组合方向是 chroma → sql，不能反过来）",
           not any("vector_store_chroma" in m for m in sql_imports), str(sorted(sql_imports)))

    chroma_path = BACKEND_DIR / "services" / "vector_store_chroma.py"
    if chroma_path.exists():
        chroma_imports = _imported_modules(chroma_path.read_text(encoding="utf-8"))
        _check("★ ANN 后端**在单独模块**里（接口模块的零依赖断言要求如此）",
               "services.vector_store_chroma" not in _imported_modules(
                   (BACKEND_DIR / "services" / "vector_store.py").read_text(encoding="utf-8")),
               "vector_store.py 不应 import 具体后端")
        _check("  └ 它组合 SQL 后端作为权威行读写器（不是另建一套切片存储）",
               "services.vector_store_sql" in chroma_imports, str(sorted(chroma_imports)))
        _check("  └ 但它不 import embedding / Retriever / Interview / Prompt",
               not any("embedding" in m or "knowledge_retriever" in m
                       or "interview" in m or "prompt" in m.lower()
                       for m in chroma_imports), str(sorted(chroma_imports)))
        _check("  └ chromadb 只在**函数体内**延迟导入（默认后端不装它也能跑）",
               "chromadb" not in _top_level_imports(
                   chroma_path.read_text(encoding="utf-8")),
               str(sorted(_top_level_imports(chroma_path.read_text(encoding="utf-8")))))
    else:
        _check("★ ANN 后端模块存在（services/vector_store_chroma.py）", False, "文件不存在")

    for rel in ("services/interview_core.py", "services/interview_agent.py",
                "services/interview_service.py", "services/knowledge_retriever.py",
                "services/embedding_service.py", "services/document_chunker.py"):
        _check(f"  └ {rel} 未被改动",
               not any("vector_store" in m for m in _imported_modules(
                   (BACKEND_DIR / rel).read_text(encoding="utf-8"))))

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


def _raises_sync(fn, exc_type):
    """同步版的 ``_raises``：返回 (是否抛出, 异常或说明)。"""
    try:
        fn()
    except exc_type as exc:
        return True, exc
    except Exception as exc:  # noqa: BLE001
        return False, f"抛了非预期异常 {type(exc).__name__}: {exc}"
    return False, "未抛异常"


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
