# -*- coding: utf-8 -*-
"""AI 面试知识库数据模型 自检

无需 pytest，直接运行：
    python backend/tests/test_knowledge_models.py

不依赖本机 MySQL：DATABASE_URL 指向 SQLite 内存库 + StaticPool。
本阶段**只建数据结构**，故本测试不涉及解析 / Embedding / 向量库 / Retriever。

覆盖范围
--------
1. **模型可导入**：``models`` / ``models.knowledge`` 均可导入，分类常量与中文名齐全
2. **表结构（模型定义）**：表名、列名与顺序、类型、可空性、主键、索引、外键
3. **表结构（真实 DDL）**：``create_all`` 能建出两张表；MySQL 方言 DDL 逐句核对
4. **字段清单与需求逐字对齐**：doc = id/title/content/category/source/created_at；
   chunk = 任务 39 的 id/document_id/content/metadata **原样保留**，
   之后**追加**任务 42 的向量预留三列 embedding / embedding_model / embedding_dim
5. **``metadata`` 保留名避让**：数据库列名仍是 ``metadata``，Python 属性名是
   ``chunk_metadata``（直接写 ``metadata = Column(...)`` 会抛 ``InvalidRequestError``）
5.5. **向量预留列**：embedding 可空 JSON、embedding_model 非空默认空串、embedding_dim 可空整数
6. **与 ``utils/schema_sync`` 口径一致**：登记列 == ORM 非主键列，且
   类型 / 可空性 / 默认值三项逐一匹配
7. **同名不同物**：``models.KnowledgeChunk``（ORM 行）与
   ``services.knowledge_retriever.KnowledgeChunk``（内存值对象）是**两个类**
8. **边界**：不改动 Interview 逻辑；不引入向量库 / Embedding 依赖；可真实读写
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

from sqlalchemy import inspect as sa_inspect  # noqa: E402
from sqlalchemy import select, text  # noqa: E402
from sqlalchemy.dialects import mysql  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402
from sqlalchemy.schema import CreateTable  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import (  # noqa: E402
    KNOWLEDGE_CATEGORIES,
    KNOWLEDGE_CATEGORY_COMPANY,
    KNOWLEDGE_CATEGORY_JOB,
    KNOWLEDGE_CATEGORY_LABELS,
    KNOWLEDGE_CATEGORY_PROJECT,
    KNOWLEDGE_CATEGORY_TECHNICAL,
    KnowledgeChunk,
    KnowledgeDocument,
)
from utils import schema_sync  # noqa: E402

_PASSED = 0
_FAILED = 0

#: 需求里逐字给定的字段清单（顺序即建表顺序，``id`` 恒为第一列）
DOC_FIELDS = ("id", "title", "content", "category", "source", "created_at")

#: 任务 39 的原始切片契约——**必须原样保留**（只允许在后面追加列）
CHUNK_BASE_FIELDS = ("id", "document_id", "content", "metadata")

#: 任务 42 追加的向量存储列（预留：只建结构，本阶段不写入）
CHUNK_EMBEDDING_FIELDS = ("embedding", "embedding_model", "embedding_dim")

CHUNK_FIELDS = CHUNK_BASE_FIELDS + CHUNK_EMBEDDING_FIELDS

DOC_CATEGORY_SAMPLE = {
    KNOWLEDGE_CATEGORY_JOB: ("后端开发工程师岗位知识", "负责后端服务的设计与开发，要求熟悉 Redis"),
    KNOWLEDGE_CATEGORY_TECHNICAL: ("Redis 持久化手册", "RDB 是全量快照，AOF 记录写命令"),
    KNOWLEDGE_CATEGORY_COMPANY: ("某公司面试风格", "偏好考察项目取舍与故障排查"),
    KNOWLEDGE_CATEGORY_PROJECT: ("订单中台重构复盘", "单体拆 8 个微服务，QPS 从 800 到 5000"),
}


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


def _col(model, name):
    return model.__table__.c[name]


def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 面试知识库数据模型 自检（只建数据结构）")
    print("=" * 70)

    # ------------------------------------------------------------
    # [1] 模型可导入（要求 1）
    # ------------------------------------------------------------
    print("\n[1] 模型可导入（要求 1）")
    from models import knowledge as km

    _check("models.knowledge 可导入", hasattr(km, "KnowledgeDocument"))
    _check("从 models 包可直接取到两个模型",
           isinstance(KnowledgeDocument, type) and isinstance(KnowledgeChunk, type))
    _check("  └ 均继承 Base（是 ORM 模型）",
           issubclass(KnowledgeDocument, Base) and issubclass(KnowledgeChunk, Base))
    _check("两个模型都出现在 models.__all__ 中",
           {"KnowledgeDocument", "KnowledgeChunk"} <= set(models.__all__))
    _check("分类常量已导出",
           {"KNOWLEDGE_CATEGORIES", "KNOWLEDGE_CATEGORY_LABELS"} <= set(models.__all__))

    _check("恰好四类知识：岗位 / 技术 / 公司 / 项目",
           KNOWLEDGE_CATEGORIES == ("job", "technical", "company", "project"),
           str(KNOWLEDGE_CATEGORIES))
    _check("  └ 四个具名常量与元组一致",
           (KNOWLEDGE_CATEGORY_JOB, KNOWLEDGE_CATEGORY_TECHNICAL,
            KNOWLEDGE_CATEGORY_COMPANY, KNOWLEDGE_CATEGORY_PROJECT)
           == KNOWLEDGE_CATEGORIES)
    _check("  └ 中文名齐全且与取值一一对应",
           set(KNOWLEDGE_CATEGORY_LABELS) == set(KNOWLEDGE_CATEGORIES)
           and KNOWLEDGE_CATEGORY_LABELS[KNOWLEDGE_CATEGORY_JOB] == "岗位知识"
           and KNOWLEDGE_CATEGORY_LABELS[KNOWLEDGE_CATEGORY_TECHNICAL] == "技术知识"
           and KNOWLEDGE_CATEGORY_LABELS[KNOWLEDGE_CATEGORY_COMPANY] == "公司知识"
           and KNOWLEDGE_CATEGORY_LABELS[KNOWLEDGE_CATEGORY_PROJECT] == "项目经验知识",
           str(KNOWLEDGE_CATEGORY_LABELS))
    _check("表已注册进 Base.metadata（create_all 才会建）",
           {"knowledge_document", "knowledge_chunk"} <= set(Base.metadata.tables),
           str(sorted(Base.metadata.tables)))

    # ------------------------------------------------------------
    # [2] 表结构（模型定义）
    # ------------------------------------------------------------
    print("\n[2] 表结构（模型定义）")
    _check("表名正确", KnowledgeDocument.__tablename__ == "knowledge_document"
           and KnowledgeChunk.__tablename__ == "knowledge_chunk")

    _check("★ KnowledgeDocument 列名与顺序 = 需求字段清单",
           tuple(KnowledgeDocument.__table__.c.keys()) == DOC_FIELDS,
           str(tuple(KnowledgeDocument.__table__.c.keys())))
    _check("★ KnowledgeChunk 列名与顺序 = 需求字段清单",
           tuple(KnowledgeChunk.__table__.c.keys()) == CHUNK_FIELDS,
           str(tuple(KnowledgeChunk.__table__.c.keys())))

    _check("两表主键都是自增整数 id",
           _col(KnowledgeDocument, "id").primary_key
           and _col(KnowledgeChunk, "id").primary_key
           and _col(KnowledgeDocument, "id").autoincrement is True
           and _col(KnowledgeChunk, "id").autoincrement is True)

    _check("document.title 是 VARCHAR(300) 且非空",
           str(_col(KnowledgeDocument, "title").type).upper() == "VARCHAR(300)"
           and _col(KnowledgeDocument, "title").nullable is False,
           str(_col(KnowledgeDocument, "title").type))
    _check("document.content / chunk.content 是 TEXT 且非空",
           str(_col(KnowledgeDocument, "content").type).upper() == "TEXT"
           and _col(KnowledgeDocument, "content").nullable is False
           and str(_col(KnowledgeChunk, "content").type).upper() == "TEXT"
           and _col(KnowledgeChunk, "content").nullable is False)
    _check("document.category 非空且建索引（要按分类检索）",
           _col(KnowledgeDocument, "category").nullable is False
           and _col(KnowledgeDocument, "category").index is True)
    _check("document.source 非空、Python 默认值与库默认值口径一致",
           _col(KnowledgeDocument, "source").nullable is False
           and _col(KnowledgeDocument, "source").default.arg == ""
           and str(_col(KnowledgeDocument, "source").server_default.arg) == "",
           f"py={_col(KnowledgeDocument, 'source').default.arg!r}")
    _check("document.created_at 是 DATETIME",
           str(_col(KnowledgeDocument, "created_at").type).upper() == "DATETIME")

    _check("chunk.metadata 是 JSON 且非空",
           str(_col(KnowledgeChunk, "metadata").type).upper() == "JSON"
           and _col(KnowledgeChunk, "metadata").nullable is False)
    _check("  └ 默认值是 dict 工厂（可调用），不是共享字面量 {}",
           _col(KnowledgeChunk, "metadata").default.is_callable is True
           and callable(_col(KnowledgeChunk, "metadata").default.arg),
           str(_col(KnowledgeChunk, "metadata").default))

    fks = list(KnowledgeChunk.__table__.foreign_keys)
    _check("chunk.document_id 外键指向 knowledge_document.id",
           len(fks) == 1 and fks[0].parent.name == "document_id"
           and fks[0].target_fullname == "knowledge_document.id",
           str([(f.parent.name, f.target_fullname) for f in fks]))
    _check("  └ document_id 非空且建索引（1:N 关联查询）",
           _col(KnowledgeChunk, "document_id").nullable is False
           and _col(KnowledgeChunk, "document_id").index is True)
    _check("两表之间不建 ORM relationship（与既有代码风格一致，避免异步惰性加载）",
           not KnowledgeChunk.__mapper__.relationships
           and not KnowledgeDocument.__mapper__.relationships)

    # ------------------------------------------------------------
    # [3] 表结构（真实 DDL，MySQL 8 方言）
    # ------------------------------------------------------------
    print("\n[3] 表结构（真实 DDL，MySQL 8 方言）")
    doc_ddl = str(CreateTable(KnowledgeDocument.__table__).compile(dialect=mysql.dialect()))
    chunk_ddl = str(CreateTable(KnowledgeChunk.__table__).compile(dialect=mysql.dialect()))

    for frag, label in (
        ("CREATE TABLE knowledge_document", "建表语句"),
        ("id INTEGER NOT NULL AUTO_INCREMENT", "自增主键"),
        ("title VARCHAR(300) NOT NULL", "title"),
        ("content TEXT NOT NULL", "content"),
        ("category VARCHAR(30) NOT NULL", "category"),
        ("source VARCHAR(300) NOT NULL DEFAULT ''", "source 默认空串"),
        ("created_at DATETIME", "created_at"),
        ("PRIMARY KEY (id)", "主键约束"),
    ):
        _check(f"document DDL 含「{label}」", frag in doc_ddl, doc_ddl.replace("\n", " ")[:120])

    for frag, label in (
        ("CREATE TABLE knowledge_chunk", "建表语句"),
        ("document_id INTEGER NOT NULL", "document_id"),
        ("content TEXT NOT NULL", "content"),
        ("metadata JSON NOT NULL", "metadata 列名与 JSON 类型"),
        ("FOREIGN KEY(document_id) REFERENCES knowledge_document (id)", "外键约束"),
        ("embedding JSON", "embedding 可空 JSON（向量本体）"),
        ("embedding_model VARCHAR(100) NOT NULL DEFAULT ''", "embedding_model 默认空串"),
        ("embedding_dim INTEGER", "embedding_dim 可空整数"),
    ):
        _check(f"chunk DDL 含「{label}」", frag in chunk_ddl, chunk_ddl.replace("\n", " ")[:120])

    # 真实建表（SQLite 内存库），确认 create_all 不报错且表可读写
    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        reflected = await conn.run_sync(lambda sc: sa_inspect(sc).get_table_names())
    _check("create_all 成功建出两张表", {"knowledge_document", "knowledge_chunk"}
           <= set(reflected), str(sorted(reflected)))

    async with engine.begin() as conn:
        cols = await conn.run_sync(
            lambda sc: [c["name"] for c in sa_inspect(sc).get_columns("knowledge_document")]
        )
    _check("  └ 库里实际列名与模型一致", tuple(cols) == DOC_FIELDS, str(cols))

    # ------------------------------------------------------------
    # [4] 字段清单与需求逐字对齐
    # ------------------------------------------------------------
    print("\n[4] 字段清单与需求逐字对齐（不多不少）")
    _check("KnowledgeDocument 恰好 6 列",
           len(KnowledgeDocument.__table__.c) == len(DOC_FIELDS),
           str(len(KnowledgeDocument.__table__.c)))
    _check("KnowledgeChunk 恰好 7 列（原 4 列 + 向量预留 3 列）",
           len(KnowledgeChunk.__table__.c) == len(CHUNK_FIELDS),
           str(len(KnowledgeChunk.__table__.c)))
    _check("  └ ★ 原 4 列原样保留且仍在最前（未删 / 未改名 / 未重排）",
           tuple(KnowledgeChunk.__table__.c.keys())[:len(CHUNK_BASE_FIELDS)]
           == CHUNK_BASE_FIELDS,
           str(tuple(KnowledgeChunk.__table__.c.keys())))
    _check("  └ 没有自行加 created_at / updated_at 等额外列",
           set(KnowledgeChunk.__table__.c.keys()) == set(CHUNK_FIELDS),
           str(sorted(KnowledgeChunk.__table__.c.keys())))

    # ------------------------------------------------------------
    # [4.5] 向量存储列（任务 42 预留：只建结构，不写入）
    # ------------------------------------------------------------
    print("\n[4.5] 向量存储列（预留，只建结构）")
    _check("embedding 是 JSON 且可空（NULL = 尚未向量化）",
           str(_col(KnowledgeChunk, "embedding").type).upper() == "JSON"
           and _col(KnowledgeChunk, "embedding").nullable is True,
           f"{_col(KnowledgeChunk, 'embedding').type} nullable="
           f"{_col(KnowledgeChunk, 'embedding').nullable}")
    _check("embedding_model 是 VARCHAR(100) 且非空",
           str(_col(KnowledgeChunk, "embedding_model").type).upper() == "VARCHAR(100)"
           and _col(KnowledgeChunk, "embedding_model").nullable is False,
           str(_col(KnowledgeChunk, "embedding_model").type))
    _check("  └ Python 默认值与库默认值口径一致（都是空串）",
           _col(KnowledgeChunk, "embedding_model").default.arg == ""
           and str(_col(KnowledgeChunk, "embedding_model").server_default.arg) == "",
           f"py={_col(KnowledgeChunk, 'embedding_model').default} "
           f"server={_col(KnowledgeChunk, 'embedding_model').server_default}")
    _check("embedding_dim 是 INTEGER 且可空",
           str(_col(KnowledgeChunk, "embedding_dim").type).upper() == "INTEGER"
           and _col(KnowledgeChunk, "embedding_dim").nullable is True,
           str(_col(KnowledgeChunk, "embedding_dim").type))
    _check("  └ 三列都未建索引（本阶段不做向量检索，无需索引）",
           not any(_col(KnowledgeChunk, n).index for n in CHUNK_EMBEDDING_FIELDS))
    _check("  └ 向量是**独立列**，未塞进 metadata（metadata 要保持轻量）",
           "embedding" in KnowledgeChunk.__table__.c
           and _col(KnowledgeChunk, "metadata").default.is_callable
           and callable(_col(KnowledgeChunk, "metadata").default.arg))

    # ------------------------------------------------------------
    # [5] metadata 保留名避让
    # ------------------------------------------------------------
    print("\n[5] metadata 保留名避让（SQLAlchemy Declarative 限制）")
    _check("数据库列名仍是 metadata（与需求一致）",
           "metadata" in KnowledgeChunk.__table__.c)
    _check("★ Python 属性名是 chunk_metadata（metadata 是保留名，不能直接用作属性）",
           hasattr(KnowledgeChunk, "chunk_metadata"))
    _check("  └ 该属性映射到名为 metadata 的列",
           KnowledgeChunk.chunk_metadata.property.columns[0].name == "metadata",
           str(KnowledgeChunk.chunk_metadata.property.columns[0].name))
    _check("  └ 类上没有名为 metadata 的属性（未遮蔽 Base.metadata）",
           "metadata" not in KnowledgeChunk.__dict__
           and "metadata" not in KnowledgeDocument.__dict__)

    # ------------------------------------------------------------
    # [6] 与 schema_sync 口径一致
    # ------------------------------------------------------------
    print("\n[6] 与 utils.schema_sync 口径一致")
    desired = schema_sync.DESIRED_COLUMNS
    for model in (KnowledgeDocument, KnowledgeChunk):
        table = model.__tablename__
        _check(f"{table} 已登记进 DESIRED_COLUMNS", table in desired, str(sorted(desired)))
        if table not in desired:
            continue
        spec_cols = set(desired[table])
        orm_cols = {c.name for c in model.__table__.c if not c.primary_key}
        _check(f"  └ 登记列 == 模型非主键列（不遗漏、不臆造）",
               spec_cols == orm_cols, f"spec={sorted(spec_cols)} orm={sorted(orm_cols)}")
        for name, spec in desired[table].items():
            col = _col(model, name)
            _check(f"  └ {name} 类型一致",
                   str(col.type).upper() == spec["type"].upper(),
                   f"orm={col.type} spec={spec['type']}")
            _check(f"  └ {name} 可空性一致",
                   bool(col.nullable) is bool(spec.get("nullable", True)),
                   f"orm={col.nullable} spec={spec.get('nullable', True)}")
            if "default" in spec:
                _check(f"  └ {name} 默认值一致（ORM 必须有 server_default）",
                       col.server_default is not None
                       and str(col.server_default.arg).strip("'")
                       == spec["default"].strip("'"),
                       f"orm={col.server_default} spec={spec['default']}")
            else:
                _check(f"  └ {name} 未声明默认值时 ORM 也不该有 server_default",
                       col.server_default is None, str(col.server_default))
    _check("登记项本身合法（DDL 可渲染，不抛 UnsupportedColumnSpecError）",
           all(schema_sync.build_column_ddl(s)
               for cols in desired.values() for s in cols.values()))

    # ------------------------------------------------------------
    # [7] 同名不同物
    # ------------------------------------------------------------
    print("\n[7] 同名不同物：ORM 行 vs 检索值对象")
    from services.knowledge_retriever import KnowledgeChunk as RetrievalChunk

    _check("★ 两个 KnowledgeChunk 是不同的类",
           KnowledgeChunk is not RetrievalChunk
           and KnowledgeChunk.__name__ == RetrievalChunk.__name__ == "KnowledgeChunk",
           f"{KnowledgeChunk.__module__} vs {RetrievalChunk.__module__}")
    _check("  └ 检索侧的是 frozen dataclass（内存值对象，无 id / document_id）",
           RetrievalChunk.__dataclass_params__.frozen is True
           and not hasattr(RetrievalChunk, "document_id"))
    _check("  └ 检索侧仍零依赖：未 import models（保持可脱离 DB 单测）",
           not any(n.endswith("models") or n.endswith("knowledge")
                   for n in _imported_modules(
                       pathlib.Path(inspect.getfile(RetrievalChunk)).read_text(encoding="utf-8"))),
           str(sorted(_imported_modules(
               pathlib.Path(inspect.getfile(RetrievalChunk)).read_text(encoding="utf-8")))))

    # ------------------------------------------------------------
    # [8] 边界：不改 Interview 逻辑 / 不引入向量能力
    # ------------------------------------------------------------
    print("\n[8] 边界：不改 Interview 逻辑；不引入向量库 / Embedding")
    for rel in ("models/interview.py", "services/interview_core.py",
                "services/interview_agent.py", "services/interview_service.py"):
        imports = _imported_modules((BACKEND_DIR / rel).read_text(encoding="utf-8"))
        hits = sorted(n for n in imports
                      if n.endswith("models.knowledge") or "models.knowledge" in n)
        _check(f"{rel} 未 import 新增的知识库模型（Interview 逻辑未改）",
               hits == [], str(hits))

    # 反向：知识库模型也不得依赖 Interview / 检索 / 向量能力
    km_imports = _imported_modules(
        (BACKEND_DIR / "models" / "knowledge.py").read_text(encoding="utf-8"))
    _check("models/knowledge.py 未 import 向量库 / Embedding / LLM",
           not ({"chromadb", "faiss", "milvus", "weaviate", "pinecone", "qdrant",
                 "numpy", "openai", "sentence_transformers", "transformers",
                 "requests", "websocket", "websockets"}
                & {n.split(".")[0] for n in km_imports}),
           str(sorted(km_imports)))
    _check("  └ 也不 import Retriever / Interview（纯数据结构）",
           not any("knowledge_retriever" in n or "interview" in n for n in km_imports),
           str(sorted(km_imports)))

    # ------------------------------------------------------------
    # [9] 端到端：四类知识可真实读写
    # ------------------------------------------------------------
    print("\n[9] 端到端：四类知识可真实读写")
    async with session_factory() as db:
        for cat, (title, content) in DOC_CATEGORY_SAMPLE.items():
            db.add(KnowledgeDocument(title=title, content=content, category=cat,
                                     source=f"manual://{cat}"))
        await db.commit()

        total = (await db.execute(select(KnowledgeDocument))).scalars().all()
        _check("四类知识各写入一条", len(total) == 4, str(len(total)))
        _check("  └ created_at 由 ORM 自动填充（非 None）",
               all(d.created_at is not None for d in total))
        _check("  └ source 落库正确",
               {d.source for d in total} == {f"manual://{c}" for c in KNOWLEDGE_CATEGORIES})

        by_cat = (await db.execute(
            select(KnowledgeDocument).where(
                KnowledgeDocument.category == KNOWLEDGE_CATEGORY_TECHNICAL)
        )).scalars().all()
        _check("按 category 检索可用（分类是索引列）",
               len(by_cat) == 1 and by_cat[0].title == "Redis 持久化手册")

        doc_id = by_cat[0].id
        db.add(KnowledgeChunk(document_id=doc_id,
                              content="RDB 是某一时刻的全量快照，恢复快但可能丢数据",
                              chunk_metadata={"topic": "Redis 持久化", "difficulty": "mid"}))
        db.add(KnowledgeChunk(document_id=doc_id, content="AOF 记录写命令，可通过 appendfsync 控制"))
        await db.commit()

        chunks = (await db.execute(
            select(KnowledgeChunk).where(KnowledgeChunk.document_id == doc_id)
        )).scalars().all()
        _check("切片按 document_id 查回（1:N 关联可用）", len(chunks) == 2, str(len(chunks)))
        _check("  └ metadata 为 JSON 字典且可读",
               any(c.chunk_metadata.get("topic") == "Redis 持久化" for c in chunks),
               str([c.chunk_metadata for c in chunks]))
        _check("  └ 未显式传 metadata 的切片默认是空 dict（不是 None / 共享对象）",
               any(c.chunk_metadata == {} for c in chunks),
               str([c.chunk_metadata for c in chunks]))

        # ---- 向量预留列的端到端读写（本阶段只验证「存得下、读得回」）----
        _check("★ 未向量化的切片：embedding 为 NULL（即「待处理」筛选条件）",
               all(c.embedding is None for c in chunks),
               str([c.embedding for c in chunks]))
        _check("  └ embedding_model 默认落成空串（不是 NULL）",
               all(c.embedding_model == "" for c in chunks),
               str([c.embedding_model for c in chunks]))
        _check("  └ embedding_dim 默认为 NULL",
               all(c.embedding_dim is None for c in chunks))

        vector = [0.5, -0.25, 0.125]
        chunks[0].embedding = vector
        chunks[0].embedding_model = "hash-local"
        chunks[0].embedding_dim = len(vector)
        await db.commit()

        refreshed = (await db.execute(
            select(KnowledgeChunk).where(KnowledgeChunk.id == chunks[0].id)
        )).scalar_one()
        _check("★ 向量可写入并原样读回（JSON 列保真）",
               refreshed.embedding == vector, str(refreshed.embedding))
        _check("  └ 模型名与维度一并落库（换模型后可筛出旧向量）",
               refreshed.embedding_model == "hash-local"
               and refreshed.embedding_dim == 3,
               f"{refreshed.embedding_model}/{refreshed.embedding_dim}")
        _check("  └ 可用 SQL 直接筛「待向量化」的行（embedding IS NULL）",
               len((await db.execute(
                   select(KnowledgeChunk).where(KnowledgeChunk.embedding.is_(None))
               )).scalars().all()) == 1)

    await engine.dispose()

    # ------------------------------------------------------------
    # [10] schema_sync 对新表真的生效（不是只写在配置里）
    # ------------------------------------------------------------
    print("\n[10] schema_sync 接入验证：老库缺列能自动补齐")
    legacy = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with legacy.begin() as conn:
        # 造一张「老版本」的 knowledge_document：只有前 4 列
        await conn.execute(text(
            "CREATE TABLE knowledge_document ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " title VARCHAR(300) NOT NULL,"
            " content TEXT NOT NULL,"
            " category VARCHAR(30) NOT NULL)"
        ))
        before = await conn.run_sync(
            lambda sc: [c["name"] for c in sa_inspect(sc).get_columns("knowledge_document")])
        added = await schema_sync.ensure_columns(conn)
        after = await conn.run_sync(
            lambda sc: [c["name"] for c in sa_inspect(sc).get_columns("knowledge_document")])
        again = await schema_sync.ensure_columns(conn)
        tables = await conn.run_sync(lambda sc: sa_inspect(sc).get_table_names())

    _check("老库确实缺 source / created_at",
           set(before) == {"id", "title", "content", "category"}, str(before))
    _check("★ ensure_columns 自动补上缺失列",
           added == ["knowledge_document.source", "knowledge_document.created_at"],
           str(added))
    _check("  └ 补齐后列集合与模型一致",
           set(after) == set(DOC_FIELDS), str(after))
    _check("  └ 幂等：第二次调用不重复 ALTER", again == [], str(again))
    _check("  └ 表不存在时跳过、不建表（knowledge_chunk 未被创建）",
           "knowledge_chunk" not in tables, str(sorted(tables)))
    await legacy.dispose()

    # ------------------------------------------------------------
    # [11] schema_sync 对**已有数据的** knowledge_chunk 补向量列
    # ------------------------------------------------------------
    # 这是真实场景：knowledge_chunk 表在任务 39/40 就已由 create_all 建出，
    # 老库上并没有 embedding / embedding_model / embedding_dim 三列。
    print("\n[11] schema_sync：老库的 knowledge_chunk 缺向量列 → 自动补齐")
    legacy_chunk = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with legacy_chunk.begin() as conn:
        await conn.execute(text(
            "CREATE TABLE knowledge_document ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " title VARCHAR(300) NOT NULL,"
            " content TEXT NOT NULL,"
            " category VARCHAR(30) NOT NULL,"
            " source VARCHAR(300) NOT NULL DEFAULT '',"
            " created_at DATETIME)"
        ))
        # 「老版本」的 knowledge_chunk：只有任务 39 的 4 列，且**已有数据**
        await conn.execute(text(
            "CREATE TABLE knowledge_chunk ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " document_id INTEGER NOT NULL,"
            " content TEXT NOT NULL,"
            " metadata JSON NOT NULL)"
        ))
        await conn.execute(text(
            "INSERT INTO knowledge_chunk (document_id, content, metadata)"
            " VALUES (1, '既有切片', '{}')"
        ))
        chunk_before = await conn.run_sync(
            lambda sc: [c["name"] for c in sa_inspect(sc).get_columns("knowledge_chunk")])
        added2 = await schema_sync.ensure_columns(conn)
        chunk_after = await conn.run_sync(
            lambda sc: [c["name"] for c in sa_inspect(sc).get_columns("knowledge_chunk")])
        again2 = await schema_sync.ensure_columns(conn)
        # NOT NULL 的 embedding_model 有 DEFAULT ''，所以**有数据也能补**，
        # 且既有行会被填成默认值，而不是留下 NULL / 报错。
        row = (await conn.execute(text(
            "SELECT content, embedding, embedding_model, embedding_dim"
            " FROM knowledge_chunk WHERE id = 1"))).fetchone()

    _check("老库的 knowledge_chunk 确实只有原 4 列",
           set(chunk_before) == {"id", "document_id", "content", "metadata"},
           str(chunk_before))
    _check("★ ensure_columns 自动补上三个向量列",
           added2 == ["knowledge_chunk.embedding",
                      "knowledge_chunk.embedding_model",
                      "knowledge_chunk.embedding_dim"],
           str(added2))
    _check("  └ 补齐后列集合与模型一致",
           set(chunk_after) == set(CHUNK_FIELDS), str(chunk_after))
    _check("  └ 幂等：第二次调用不重复 ALTER", again2 == [], str(again2))
    _check("★ 既有行未被清空（只加列，不动数据）",
           row is not None and row[0] == "既有切片", str(row))
    _check("  └ 既有行的 embedding 为 NULL（尚未向量化）", row is not None and row[1] is None,
           str(row[1] if row else None))
    _check("  └ 既有行的 embedding_model 自动填默认空串（NOT NULL 有 DEFAULT 才能补）",
           row is not None and row[2] == "", str(row[2] if row else None))
    _check("  └ 既有行的 embedding_dim 为 NULL",
           row is not None and row[3] is None, str(row[3] if row else None))
    await legacy_chunk.dispose()

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
