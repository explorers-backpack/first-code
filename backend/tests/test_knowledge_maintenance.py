# -*- coding: utf-8 -*-
"""知识库维护动作自检（**索引删 / 重建**）

无需 pytest，直接运行：
    python backend/tests/test_knowledge_maintenance.py

不依赖本机 MySQL（权威行跑内存 SQLite）、不联网（chroma 用 ``EphemeralClient``）。
相似度全用**手写向量**，结果可精确断言。

覆盖
----
[1] 索引删除原语 ``ChromaVectorStore.delete``：真实移除、**权威行一条不动**、
    返回真实条数、幂等、空入参短路、入参校验（含 ``bool`` / 字符串）
[2] 索引清空原语 ``ChromaVectorStore.reset``：清空、**权威行一条不动**、
    空索引返回 0、幂等
[3] ``delete_document``：有删除钩子 ⇒ ``index_removed`` 取**后端回报值**；
    无钩子且索引分离 ⇒ ``index_error`` 如实说明；索引即权威行 ⇒ ``None``
[4] ``rebuild_index``：``purge=False`` **清不掉残留**（这正是 ``purge`` 存在的理由）、
    ``purge=True`` 真清、``purge`` 对 sql 后端**被拒绝**（不能删权威数据）、
    ``purge`` 与 ``document_id`` **互斥**、清空失败进 ``errors``
[5] 报告契约：``DELETE_RESULT_FIELDS`` / ``REBUILD_RESULT_FIELDS`` 与实际返回**逐键相等**
[6] 接口守卫：``VectorStore.__abstractmethods__`` 仍是 ``{add, search, count}``，
    两个原语**刻意不是接口方法**（「换后端不换语义」的契约没被破坏）
[7] 端到端：真实入库 Pipeline → 删文档（权威行与索引同步为 0）→ ``purge`` 重建恢复
"""

import ast
import asyncio
import itertools
import os
import pathlib
import sys
from typing import Any, Dict, List, Optional

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
import regression_env  # noqa: E402,F401  —— 钉住 .env 的部署配置（见其模块文档）

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from sqlalchemy import delete as sa_delete  # noqa: E402
from sqlalchemy import func, select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401
from database import Base  # noqa: E402
from models import KnowledgeChunk, KnowledgeDocument  # noqa: E402
from services import knowledge_maintenance  # noqa: E402
from services.knowledge_import_pipeline import KnowledgeImportPipeline  # noqa: E402
from services.knowledge_maintenance import (  # noqa: E402
    DELETE_RESULT_FIELDS,
    PURGE_REFUSED_HINT,
    PURGE_SCOPED_HINT,
    REBUILD_RESULT_FIELDS,
    SEPARATE_INDEX_BACKENDS,
    delete_document,
    rebuild_index,
)
from services.vector_store import (  # noqa: E402
    InMemoryVectorStore,
    VectorRecord,
    VectorStore,
    VectorStoreInputError,
)
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

_PASSED = 0
_FAILED = 0


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


def _section(title: str) -> None:
    print(f"\n{title}")


async def _raises(coro, exc_type) -> bool:
    """协程是否抛指定类型异常。"""
    try:
        await coro
        return False
    except exc_type:
        return True
    except Exception:  # noqa: BLE001 - 别的异常也算「不是期望的那种」
        return False


# ============================================================
# 夹具
# ============================================================
V_X = [1.0, 0.0]
V_Y = [0.0, 1.0]
V_XY = [1.0, 1.0]
MODEL = "m1"

_COLLECTION_SEQ = itertools.count(1)


def _import_chromadb():
    """导入 chromadb；未安装时直接判定整份自检失败（本文件要验真实 ANN 后端）。"""
    try:
        import chromadb
    except ImportError as exc:  # pragma: no cover
        print(f"\n[致命] 未安装 chromadb，无法运行本自检：{exc}")
        raise SystemExit(1) from exc
    return chromadb


CHROMADB = _import_chromadb()

from services.vector_store_chroma import ChromaVectorStore  # noqa: E402


def _ephemeral_client():
    """进程内 chroma 客户端。

    .. warning::
       ``EphemeralClient()`` **不是**「每次调用都得到空库」（chroma 按 settings 缓存
       System，同进程内共享内存数据）⇒ 必须**每个用例一个集合名**，见
       :func:`_collection_name`。这条在 ``test_vector_store_chroma`` 已踩过一次。
    """
    factory = getattr(CHROMADB, "EphemeralClient", None)
    if factory is not None:
        return factory()
    return CHROMADB.Client(
        settings=CHROMADB.Settings(is_persistent=False, anonymized_telemetry=False)
    )


def _collection_name() -> str:
    return f"test_maint_{next(_COLLECTION_SEQ)}"


async def _new_env():
    """每个用例一个**全新**内存 SQLite 引擎 + 建表。

    共用引擎会被前面用例写下的行污染：``SqlAlchemyVectorStore.count()`` 统计的是
    **全表**已向量化行数，而本套件的断言全是「权威行 vs 索引条数」的对比。
    """
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return engine, factory


def _chroma_store(session, *, model=MODEL):
    return ChromaVectorStore(
        session, client=_ephemeral_client(), collection_name=_collection_name(), model=model
    )


def _record(vector, *, document_id, content, model=MODEL):
    return VectorRecord(vector=vector, content=content, document_id=document_id, model=model)


async def _make_document(session, *, title: str, source: str = "") -> int:
    row = KnowledgeDocument(title=title, content="正文", category="technical", source=source)
    session.add(row)
    await session.commit()
    return int(row.id)


async def _authority_chunk_count(session) -> int:
    """权威表里**已向量化**的切片数（= ``SqlAlchemyVectorStore.count()`` 的口径）。"""
    result = await session.execute(
        select(func.count()).select_from(KnowledgeChunk)
        .where(KnowledgeChunk.embedding.is_not(None))
    )
    return int(result.scalar_one())


async def _chunk_ids(session, document_id: int) -> List[int]:
    """某文档的全部切片 id（按主键升序）。

    刻意**不用 ``store.add`` 的返回值**——``VectorStore.add`` 的契约是「返回写入**条数**」
    （``ChromaVectorStore.add`` 内部走 ``add_returning_ids`` 再取 ``len()``），
    拿它当 id 列表会得到 ``1`` 这种「单个 int」，直接撞上入参校验。
    """
    rows = await session.execute(
        select(KnowledgeChunk.id)
        .where(KnowledgeChunk.document_id == document_id)
        .order_by(KnowledgeChunk.id)
    )
    return [int(value) for value in rows.scalars().all()]


def _imported_modules(source: str) -> List[str]:
    """AST 全量扫描：``import`` / ``from … import`` 的模块名（含函数体内）。"""
    found: List[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            found.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            found.append(module)
            found.extend(f"{module}.{alias.name}" for alias in node.names)
    return found


# ============================================================
# 替身
# ============================================================
class _NoDeleteStore:
    """只有 ``name`` / ``add``，**没有删除原语**的后端替身。"""

    name = "chroma"

    def __init__(self) -> None:
        self.added = 0

    async def add(self, records: Any) -> int:
        self.added += len(records)
        return len(records)


class _ResetBoomStore(_NoDeleteStore):
    """``reset`` 会抛的后端替身（验「清空失败不静默」）。"""

    async def reset(self) -> int:
        raise RuntimeError("索引清空炸了")

    async def delete(self, chunk_ids: Any) -> int:
        return 0


class _ReturningDeleteStore(_NoDeleteStore):
    """``delete`` 回报**真实条数**的替身（验「用后端回报值而不是入参长度」）。"""

    def __init__(self, removed: int) -> None:
        super().__init__()
        self.removed = removed
        self.seen: List[int] = []

    async def delete(self, chunk_ids: Any) -> int:
        self.seen.extend(list(chunk_ids))
        return self.removed


# ============================================================
# [1] 索引删除原语
# ============================================================
async def check_delete_primitive() -> None:
    _section("[1] chroma 索引删除原语 delete()（只动派生索引）")
    engine, factory = await _new_env()
    try:
        async with factory() as session:
            doc_a = await _make_document(session, title="A")
            doc_b = await _make_document(session, title="B")
            store = _chroma_store(session)

            await store.add([_record(V_X, document_id=doc_a, content="A1")])
            ids_a = await _chunk_ids(session, doc_a)
            await store.add([_record(V_Y, document_id=doc_b, content="B1")])
            ids_b = await _chunk_ids(session, doc_b)
            _check("前置：索引 2 条", await store.count() == 2, str(await store.count()))
            _check("前置：权威行 2 条", await _authority_chunk_count(session) == 2)
            _check("前置：拿到 1 个 A 切片 id", len(ids_a) == 1, str(ids_a))

            removed = await store.delete(ids_a)
            _check("delete 返回真实移除条数 1", removed == 1, str(removed))
            _check("索引剩 1 条", await store.count() == 1, str(await store.count()))
            _check(
                "★ 权威行**一条没动**（delete 只动派生索引）",
                await _authority_chunk_count(session) == 2,
                str(await _authority_chunk_count(session)),
            )
            _check(
                "被删的是 A 的切片（B 仍能被检索到）",
                [m.chunk_id for m in await store.search(V_Y, top_k=5, model=MODEL)] == ids_b,
                str([m.chunk_id for m in await store.search(V_Y, top_k=5, model=MODEL)]),
            )

            # 幂等：再删一次返回 0
            again = await store.delete(ids_a)
            _check("重复删除幂等（返回 0）", again == 0, str(again))

            # 空入参短路：不报错、返回 0
            _check("空列表返回 0（不报错）", await store.delete([]) == 0)

            # 混合「存在 + 不存在」：只数真实存在的
            mixed = await store.delete([ids_b[0], 999999])
            _check("存在/不存在混合 ⇒ 只数存在的（1）", mixed == 1, str(mixed))
            _check("索引清空为 0", await store.count() == 0, str(await store.count()))

            # 入参校验：显式拒 None / 单个 int / 字符串 / bool / 字符串元素
            _check("delete(None) 抛 VectorStoreInputError",
                   await _raises(store.delete(None), VectorStoreInputError))
            _check("delete(单个 int) 抛错",
                   await _raises(store.delete(1), VectorStoreInputError))
            _check("delete(字符串) 抛错",
                   await _raises(store.delete("12"), VectorStoreInputError))
            _check("delete(True) 抛错（bool 是 int 子类，必须显式拒）",
                   await _raises(store.delete(True), VectorStoreInputError))
            _check("delete([\"12\"]) 抛错（元素不得是字符串）",
                   await _raises(store.delete(["12"]), VectorStoreInputError))
            _check("delete([True]) 抛错",
                   await _raises(store.delete([True]), VectorStoreInputError))
            _check("delete([\"a\"]) 抛错",
                   await _raises(store.delete(["a"]), VectorStoreInputError))
    finally:
        await engine.dispose()


# ============================================================
# [2] 索引清空原语
# ============================================================
async def check_reset_primitive() -> None:
    _section("[2] chroma 索引清空原语 reset()（只动派生索引）")
    engine, factory = await _new_env()
    try:
        async with factory() as session:
            doc = await _make_document(session, title="A")
            store = _chroma_store(session)

            _check("空索引 reset 返回 0", await store.reset() == 0)

            await store.add([
                _record(V_X, document_id=doc, content="A1"),
                _record(V_Y, document_id=doc, content="A2"),
            ])
            _check("前置：索引 2 条", await store.count() == 2, str(await store.count()))

            cleared = await store.reset()
            _check("reset 返回清掉的条数 2", cleared == 2, str(cleared))
            _check("索引清空为 0", await store.count() == 0, str(await store.count()))
            _check(
                "★ 权威行**一条没动**（reset 只动派生索引）",
                await _authority_chunk_count(session) == 2,
                str(await _authority_chunk_count(session)),
            )
            _check("重复 reset 幂等（返回 0）", await store.reset() == 0)

            # 清空后维度缓存失效 ⇒ 可以写入**另一个维度**（否则会被旧维度挡住）
            await store.add([_record([1.0, 0.0, 0.0], document_id=doc, content="A3")])
            _check("reset 后维度缓存失效（可写入新维度）", await store.count() == 1)
    finally:
        await engine.dispose()


# ============================================================
# [3] delete_document 的索引同步语义
# ============================================================
async def check_delete_document() -> None:
    _section("[3] delete_document：索引同步如实回报")
    engine, factory = await _new_env()
    try:
        async with factory() as session:
            # --- 真实 chroma 后端：有 delete 原语 ⇒ index_removed 为真实条数 ---
            doc = await _make_document(session, title="A")
            store = _chroma_store(session)
            await store.add([
                _record(V_X, document_id=doc, content="A1"),
                _record(V_Y, document_id=doc, content="A2"),
            ])
            result = await delete_document(session, doc, store=store)
            _check("真实 chroma：index_removed == 2", result["index_removed"] == 2, str(result))
            _check("真实 chroma：index_error 为 None", result["index_error"] is None, str(result))
            _check("索引同步为 0", await store.count() == 0, str(await store.count()))
            _check("权威行同步为 0", await _authority_chunk_count(session) == 0)

            # --- 后端回报条数 ≠ 入参长度 ⇒ 用后端回报值（不谎报）---
            doc2 = await _make_document(session, title="B")
            await SqlAlchemyVectorStore(session).add([
                _record(V_X, document_id=doc2, content="B1"),
                _record(V_Y, document_id=doc2, content="B2"),
            ])
            partial = _ReturningDeleteStore(removed=1)
            result2 = await delete_document(session, doc2, store=partial)
            _check("★ 用后端回报值（1）而不是入参长度（2）",
                   result2["index_removed"] == 1, str(result2))
            _check("钩子确实收到了两个 id", len(partial.seen) == 2, str(partial.seen))

            # --- 无删除原语 + 索引分离 ⇒ index_error 如实说明 ---
            doc3 = await _make_document(session, title="C")
            await SqlAlchemyVectorStore(session).add(
                [_record(V_X, document_id=doc3, content="C1")]
            )
            result3 = await delete_document(session, doc3, store=_NoDeleteStore())
            _check(
                "无删除原语 + 索引分离 ⇒ index_error 如实说明",
                bool(result3["index_error"]) and "rebuild" in str(result3["index_error"]),
                str(result3["index_error"]),
            )
            _check("index_removed 保持 None（没同步就不谎报）",
                   result3["index_removed"] is None, str(result3))

            # --- 索引即权威行（sqlalchemy）⇒ 无需同步，两个字段都是 None ---
            doc4 = await _make_document(session, title="D")
            sql_store = SqlAlchemyVectorStore(session)
            await sql_store.add([_record(V_X, document_id=doc4, content="D1")])
            result4 = await delete_document(session, doc4, store=sql_store)
            _check("sql 后端：index_removed 为 None", result4["index_removed"] is None)
            _check("sql 后端：index_error 为 None", result4["index_error"] is None)
            _check("sql 后端：删行即同步（权威行 0）",
                   await _authority_chunk_count(session) == 0,
                   str(await _authority_chunk_count(session)))

            # --- 不传 store ⇒ 不做索引同步（保持既有语义）---
            doc5 = await _make_document(session, title="E")
            await SqlAlchemyVectorStore(session).add([_record(V_X, document_id=doc5, content="E1")])
            result5 = await delete_document(session, doc5)
            _check("不传 store ⇒ index_removed/index_error 都是 None",
                   result5["index_removed"] is None and result5["index_error"] is None,
                   str(result5))

            _check("SEPARATE_INDEX_BACKENDS 恰为 {chroma}",
                   set(SEPARATE_INDEX_BACKENDS) == {"chroma"}, str(SEPARATE_INDEX_BACKENDS))
    finally:
        await engine.dispose()


# ============================================================
# [4] rebuild_index：purge 的三条安全约束
# ============================================================
async def check_rebuild_purge() -> None:
    _section("[4] rebuild_index：purge 真清 vs upsert 清不掉残留")
    engine, factory = await _new_env()
    try:
        async with factory() as session:
            doc_a = await _make_document(session, title="A")
            doc_b = await _make_document(session, title="B")
            store = _chroma_store(session)
            await store.add([_record(V_X, document_id=doc_a, content="A1")])
            await store.add([_record(V_Y, document_id=doc_b, content="B1")])
            _check("前置：索引 2 / 权威行 2",
                   await store.count() == 2 and await _authority_chunk_count(session) == 2)

            # 直接删 B 的**权威行**（绕过维护模块）⇒ 索引里留下「残留」
            await session.execute(
                sa_delete(KnowledgeChunk).where(KnowledgeChunk.document_id == doc_b)
            )
            await session.commit()
            _check("制造残留：权威行 1 / 索引仍 2",
                   await _authority_chunk_count(session) == 1 and await store.count() == 2,
                   f"auth={await _authority_chunk_count(session)} idx={await store.count()}")

            # upsert 重建：**清不掉残留**
            r_upsert = await rebuild_index(session, store=store)
            _check("purge=False：scanned/rebuilt == 1",
                   r_upsert["scanned_chunks"] == 1 and r_upsert["rebuilt_chunks"] == 1,
                   str(r_upsert))
            _check("purge=False：purged_chunks 为 None、purge_note 为空",
                   r_upsert["purged_chunks"] is None and r_upsert["purge_note"] == "",
                   str(r_upsert))
            _check("★ upsert 重建后索引仍是 2（残留清不掉 ⇒ 需要 purge）",
                   await store.count() == 2, str(await store.count()))

            # purge 重建：真清
            r_purge = await rebuild_index(session, store=store, purge=True)
            _check("purge=True：purged_chunks == 2", r_purge["purged_chunks"] == 2, str(r_purge))
            _check("purge=True：purge_note 非空（说明做了什么）",
                   bool(r_purge["purge_note"]), str(r_purge["purge_note"]))
            _check("★ purge 重建后索引 == 权威行 == 1",
                   await store.count() == 1, str(await store.count()))
            _check("purge 不动权威行", await _authority_chunk_count(session) == 1)
            _check("purge 重建无 errors", r_purge["errors"] == [], str(r_purge["errors"]))

            # --- purge 与 document_id 互斥 ---
            doc_c = await _make_document(session, title="C")
            await store.add([_record(V_XY, document_id=doc_c, content="C1")])
            before = await store.count()
            r_scoped = await rebuild_index(session, store=store, document_id=doc_c, purge=True)
            _check("purge + document_id ⇒ 跳过清空（互斥）",
                   r_scoped["purged_chunks"] is None, str(r_scoped))
            _check("互斥说明与常量一致", r_scoped["purge_note"] == PURGE_SCOPED_HINT,
                   str(r_scoped["purge_note"]))
            _check("★ 互斥时索引未被清空（别的文档索引没被误删）",
                   await store.count() == before, f"{before} -> {await store.count()}")

            # --- purge 对「索引即权威行」的后端被拒绝 ---
            doc_d = await _make_document(session, title="D")
            sql_store = SqlAlchemyVectorStore(session)
            await sql_store.add([_record(V_X, document_id=doc_d, content="D1")])
            auth_before = await _authority_chunk_count(session)
            r_sql = await rebuild_index(session, store=sql_store, purge=True)
            _check("sql 后端 purge ⇒ purged_chunks 为 None", r_sql["purged_chunks"] is None)
            _check("sql 后端 purge ⇒ 说明与 PURGE_REFUSED_HINT 一致",
                   r_sql["purge_note"] == PURGE_REFUSED_HINT, str(r_sql["purge_note"]))
            _check("★ sql 后端 purge 被拒绝：权威行一条没少",
                   await _authority_chunk_count(session) == auth_before,
                   f"{auth_before} -> {await _authority_chunk_count(session)}")
            _check("sql 后端仍完成 upsert 重建（rebuilt > 0）",
                   r_sql["rebuilt_chunks"] > 0, str(r_sql))

            # --- 清空失败 ⇒ 进 errors，不静默 ---
            r_boom = await rebuild_index(session, store=_ResetBoomStore(), purge=True)
            _check("清空失败 ⇒ purged_chunks 为 None", r_boom["purged_chunks"] is None)
            _check("清空失败 ⇒ 进 errors", bool(r_boom["errors"]), str(r_boom["errors"]))
            _check("清空失败 ⇒ purge_note 说明已跳过",
                   "跳过" in str(r_boom["purge_note"]), str(r_boom["purge_note"]))

            # --- 无 purge 时不受影响（回归基线）---
            r_plain = await rebuild_index(session, store=store)
            _check("purge 默认 False（不传时不产生 purge 字段副作用）",
                   r_plain["purged_chunks"] is None and r_plain["purge_note"] == "",
                   str(r_plain))
    finally:
        await engine.dispose()


# ============================================================
# [5] 报告契约
# ============================================================
async def check_report_contract() -> None:
    _section("[5] 报告契约：键集合与顺序恒定")
    engine, factory = await _new_env()
    try:
        async with factory() as session:
            doc = await _make_document(session, title="A")
            store = _chroma_store(session)
            await store.add([_record(V_X, document_id=doc, content="A1")])

            delete_report = await delete_document(session, doc, store=store)
            _check("delete_document 键集合 == DELETE_RESULT_FIELDS",
                   set(delete_report) == set(DELETE_RESULT_FIELDS), str(sorted(delete_report)))
            _check("delete_document 键**顺序**也一致",
                   tuple(delete_report) == DELETE_RESULT_FIELDS, str(tuple(delete_report)))

            rebuild_report = await rebuild_index(session, store=store)
            _check("rebuild_index 键集合 == REBUILD_RESULT_FIELDS",
                   set(rebuild_report) == set(REBUILD_RESULT_FIELDS),
                   str(sorted(rebuild_report)))
            _check("rebuild_index 键**顺序**也一致",
                   tuple(rebuild_report) == REBUILD_RESULT_FIELDS, str(tuple(rebuild_report)))

            _check("DELETE_RESULT_FIELDS 恰 5 键", len(DELETE_RESULT_FIELDS) == 5)
            _check("REBUILD_RESULT_FIELDS 恰 7 键", len(REBUILD_RESULT_FIELDS) == 7)
            _check(
                "★ 失败报告形状相同（清空失败也返回同一组键）",
                set(await rebuild_index(session, store=_ResetBoomStore(), purge=True))
                == set(REBUILD_RESULT_FIELDS),
            )
    finally:
        await engine.dispose()


# ============================================================
# [6] 接口守卫
# ============================================================
async def check_interface_guard() -> None:
    _section("[6] 接口守卫：两个原语刻意不是接口方法")
    _check(
        "★ VectorStore.__abstractmethods__ 仍是 {add, search, count}",
        set(VectorStore.__abstractmethods__) == {"add", "search", "count"},
        str(sorted(VectorStore.__abstractmethods__)),
    )
    _check("delete 不在接口上", "delete" not in VectorStore.__dict__)
    _check("reset 不在接口上", "reset" not in VectorStore.__dict__)
    _check("★ ChromaVectorStore 提供 delete", callable(getattr(ChromaVectorStore, "delete", None)))
    _check("★ ChromaVectorStore 提供 reset", callable(getattr(ChromaVectorStore, "reset", None)))
    _check(
        "两个原语都是 async（与 add/search/count 同风格）",
        asyncio.iscoroutinefunction(ChromaVectorStore.delete)
        and asyncio.iscoroutinefunction(ChromaVectorStore.reset),
    )
    _check(
        "sql / memory 后端**没有**这两个原语（索引即权威行，不需要）",
        not hasattr(SqlAlchemyVectorStore, "reset") and not hasattr(InMemoryVectorStore, "reset"),
    )
    maint_src = pathlib.Path(knowledge_maintenance.__file__).read_text(encoding="utf-8")
    maint_imports = _imported_modules(maint_src)
    _check(
        "★ 维护模块**不 import** 任何具体后端（只用鸭子类型的 getattr 发现原语）",
        not any(
            module == "services.vector_store_chroma"
            or module.startswith("services.vector_store_chroma.")
            or module == "services.vector_store_sql"
            or module.startswith("services.vector_store_sql.")
            for module in maint_imports
        ),
        str(sorted(maint_imports)),
    )
    _check(
        "维护模块**不 import** interview 三件套（写侧与面试链路解耦）",
        not any("interview" in module for module in maint_imports),
        str(sorted(maint_imports)),
    )
    _check(
        "维护模块的索引分离集合恰为 {chroma}",
        knowledge_maintenance.SEPARATE_INDEX_BACKENDS == frozenset({"chroma"}),
    )


# ============================================================
# [7] 端到端：入库 → 删文档 → purge 重建
# ============================================================
async def check_end_to_end() -> None:
    _section("[7] 端到端：真实 Pipeline 入库 → 删文档 → purge 重建")
    engine, factory = await _new_env()
    try:
        async with factory() as session:
            store = _chroma_store(session)
            pipeline = KnowledgeImportPipeline(session, store=store)
            report = await pipeline.import_document({
                "title": "Redis 持久化手册",
                "content": (
                    "RDB 是某一时刻的全量快照，通过 fork 子进程写盘，文件紧凑、恢复快，"
                    "但两次快照之间的写入会丢失。AOF 记录每条写命令，appendfsync 可取 "
                    "always / everysec / no，everysec 是生产常见折中，最多丢 1 秒数据，"
                    "重写时用 BGREWRITEAOF 触发。混合持久化把 RDB 与 AOF 结合，"
                    "重启时先加载 RDB 再回放增量 AOF。生产环境建议开启 AOF 并选择 "
                    "everysec，同时定期做 RDB 备份以防 AOF 文件损坏。"
                ),
                "category": "technical",
                "source": "handbook://redis/persistence",
            })
            _check("前置：入库 ok", report.get("ok") is True, str(report))
            chunks = int(report.get("chunk_count") or 0)
            _check("前置：产生了切片", chunks >= 1, str(chunks))
            _check("前置：索引 == 权威行 == 切片数",
                   await store.count() == chunks
                   and await _authority_chunk_count(session) == chunks,
                   f"idx={await store.count()} auth={await _authority_chunk_count(session)}")

            document_id = int(report["document_id"])

            # 删文档：权威行 + 派生索引同步为 0
            deleted = await delete_document(session, document_id, store=store)
            _check("删除：deleted_chunks == 切片数",
                   deleted["deleted_chunks"] == chunks, str(deleted))
            _check("删除：index_removed == 切片数（索引真的同步了）",
                   deleted["index_removed"] == chunks, str(deleted))
            _check("删除：index_error 为 None", deleted["index_error"] is None)
            _check("删除后：索引 0 / 权威行 0",
                   await store.count() == 0 and await _authority_chunk_count(session) == 0,
                   f"idx={await store.count()} auth={await _authority_chunk_count(session)}")

            # 重建：权威行已空 ⇒ purge 重建后索引仍为 0（而不是留下残留）
            rebuilt = await rebuild_index(session, store=store, purge=True)
            _check("删空后 purge 重建：scanned == 0", rebuilt["scanned_chunks"] == 0, str(rebuilt))
            _check("★ 删空后 purge 重建：索引仍为 0（无残留）",
                   await store.count() == 0, str(await store.count()))

            # 再入库一次 + purge 重建，索引恢复
            report2 = await pipeline.import_document({
                "title": "MySQL 索引手册",
                "content": (
                    "聚簇索引把数据行存在叶子节点，二级索引叶子存主键值，"
                    "因此回表是二级索引查询的常见代价。覆盖索引可以避免回表。"
                    "最左前缀原则决定了联合索引的可用范围。"
                ),
                "category": "technical",
                "source": "handbook://mysql/index",
            })
            _check("再入库 ok", report2.get("ok") is True, str(report2))
            chunks2 = int(report2.get("chunk_count") or 0)
            await store.reset()
            _check("reset 后索引 0（权威行仍在）",
                   await store.count() == 0
                   and await _authority_chunk_count(session) == chunks2,
                   f"idx={await store.count()} auth={await _authority_chunk_count(session)}")
            recovered = await rebuild_index(session, store=store)
            _check("★ purge 后重建把索引恢复成权威行条数",
                   await store.count() == chunks2, f"idx={await store.count()} expect={chunks2}")
            _check("恢复后无 errors", recovered["errors"] == [], str(recovered["errors"]))
    finally:
        await engine.dispose()


async def run() -> bool:
    await check_delete_primitive()
    await check_reset_primitive()
    await check_delete_document()
    await check_rebuild_purge()
    await check_report_contract()
    await check_interface_guard()
    await check_end_to_end()

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
