# -*- coding: utf-8 -*-
"""知识库维护动作（**显式删除 / 向量索引重建**）。

为什么单独一个模块
------------------
向量层**刻意没有** ``rebuild``（那是**编排**，要读 ``knowledge_chunk``），
只有两个**索引维护原语**：``ChromaVectorStore.delete`` / ``reset``
——它们**只动派生 ANN 索引、绝不动权威行**。
删权威数据是**维护决策**，不该藏在检索后端里被顺手调用；本模块就是那个
「独立的维护动作」——**只被显式的管理接口调用**，
每次删除都回报「删了什么、删了几条」，**绝不静默清理**。

本模块**不认识任何具体后端**：只用 ``name`` 与鸭子类型的 ``delete`` / ``remove`` /
``reset``（``getattr`` 发现），因此换向量后端**本文件一行不用改**。

权威 vs 派生
------------
======================  ====================================================
**权威副本**            MySQL 的 ``knowledge_document`` / ``knowledge_chunk``
                        （含 ``embedding`` / ``embedding_model`` / ``embedding_dim``）
**派生索引**            ANN 索引（``chroma``）；``sql`` / ``memory`` 后端的
                        「索引」**就是权威行本身**
======================  ====================================================

由此推出两条动作：

- ``delete_document``：删**权威行**（先切片、后文档，避免外键中间态），
  再调后端的**索引删除原语**（``store.delete`` / ``store.remove``，若存在）。
  ``sql`` / ``memory`` 后端**无需额外动作**（检索直接查表，删行即同步）；
  ``chroma`` 有独立索引 ⇒ ``ChromaVectorStore.delete`` 会把切片从 ANN 集合里移除
  （**只动索引、不动权威行**）。若某后端既没有删除原语、索引又与权威行分离，
  本函数把这一点写进 ``index_error``，**而不是假装同步成功**。
- ``rebuild_index``：从**权威行**重新构造 ``VectorRecord`` 并调用 ``store.add``
  （接口方法，upsert 语义）⇒ 两个后端都适用，且**不改 ``VectorStore`` 接口**。
  ``purge=True`` 时先调 ``store.reset`` 清空派生索引再重建——``add`` 只能补写、
  **清不掉残留**（权威行被直接删掉时索引里的旧条目会一直留着），
  所以「真正重建」需要 ``purge``。``purge`` 对「索引即权威行」的后端**直接拒绝**
  （清空等于删知识库正文），见 ``PURGE_REFUSED_HINT``。

刻意不做
--------
- **不删调用方没点名的任何东西**：本模块只做点名删除与点名重建，没有「清理孤儿数据」
  这类自作主张的批量动作；``purge`` 也只清**派生索引**，权威行一条不动。
- **不静默吞错**：索引同步失败写进 ``index_error``，重建 / 清空失败写进 ``errors``，
  与 ``knowledge_import_pipeline`` 的「失败返回报告、不抛异常」口径一致。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import delete as sa_delete
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import KnowledgeChunk, KnowledgeDocument
from services.knowledge_document_service import get_document
from services.knowledge_rag import build_vector_store
from services.vector_store import VectorRecord

#: 索引与权威表**分离**的后端。删掉权威行之后，这些后端的索引仍留着旧向量，
#: 必须显式重建；而 ``sqlalchemy`` / ``memory`` 的「索引」就是权威行本身，
#: 删行即同步，不需要任何额外动作。
SEPARATE_INDEX_BACKENDS = frozenset({"chroma"})

#: 索引同步失败时的统一提示（措辞固定，便于前端按字符串展示）
REBUILD_HINT = "请调用 POST /api/knowledge/rebuild 重建派生索引"

#: ``delete_document`` 的恒定报告键（成功 / 失败同一形状）
DELETE_RESULT_FIELDS = (
    "document_id", "deleted_document", "deleted_chunks", "index_removed", "index_error",
)

#: ``rebuild_index`` 的恒定报告键（成功 / 失败同一形状）
REBUILD_RESULT_FIELDS = (
    "scanned_chunks", "rebuilt_chunks", "skipped_chunks", "purged_chunks",
    "purge_note", "model", "errors",
)

#: ``purge`` 被拒绝时的说明（**索引即权威行**的后端：清空会删掉知识库正文）
PURGE_REFUSED_HINT = (
    "purge 只清「派生索引」；当前后端的索引就是权威行本身，清空会删掉知识库正文，"
    "已拒绝执行（要删文档请用 DELETE /api/knowledge/documents/{id}）"
)

#: ``purge`` 与 ``document_id`` 互斥时的说明（否则会把**别的**文档的索引一起清掉）
PURGE_SCOPED_HINT = (
    "purge 会清空整个派生索引，与 document_id（只重建该文档）互斥，已跳过清空"
)


def _removed_count(result: Any, *, fallback: int) -> int:
    """把后端的删除 / 清空返回值归一成「移除了几条」。

    后端回报了整数就用它（**如实**），否则退化为按入参计。``bool`` 显式排除——
    ``isinstance(True, int)`` 为真，``True`` 会被当成「删了 1 条」这种荒唐结论。
    """
    if isinstance(result, int) and not isinstance(result, bool):
        return int(result)
    return fallback


def _store_name(store: Any) -> str:
    """取后端实现名（``VectorStore.name`` 是接口上的类属性，恒存在）。"""
    return str(getattr(store, "name", "") or "")


async def _call_hook(hook: Any, *args: Any) -> Any:
    """调用同步或异步钩子并返回结果（本模块要同时兼容两种实现）。"""
    result = hook(*args)
    if hasattr(result, "__await__"):
        result = await result
    return result


async def delete_document(
    db: AsyncSession,
    document_id: int,
    *,
    store: Any = None,
) -> Dict[str, Any]:
    """删除一篇知识文档及其**全部切片**（显式维护动作）。

    顺序（重要）：**先删切片，再删文档**。``knowledge_chunk.document_id`` 是
    指向 ``knowledge_document.id`` 的外键，反序在 MySQL 上会直接违反外键约束。

    返回 5 键恒定报告（``DELETE_RESULT_FIELDS``）：``document_id`` /
    ``deleted_document`` / ``deleted_chunks`` / ``index_removed`` / ``index_error``。

    - 文档不存在 → 抛 ``HTTPException(404)``（复用 ``knowledge_document_service.get_document``，
      与查询接口**同一口径、同一提示语**）
    - ``index_removed``：真正从派生索引移除的条数（**后端回报优先**）；
      ``None`` = 后端没有独立索引（无需移除）
    - ``index_error``：``None`` = 已同步或无需同步；非空 = 派生索引**可能仍含已删切片**
    """
    # 存在性校验（复用既有 404 口径；不存在时这里就抛出，不会删到一半）
    await get_document(db, document_id)

    chunk_ids = [
        int(value)
        for value in (
            await db.execute(
                select(KnowledgeChunk.id).where(
                    KnowledgeChunk.document_id == document_id
                )
            )
        )
        .scalars()
        .all()
    ]

    await db.execute(
        sa_delete(KnowledgeChunk).where(KnowledgeChunk.document_id == document_id)
    )
    await db.execute(
        sa_delete(KnowledgeDocument).where(KnowledgeDocument.id == document_id)
    )
    await db.commit()

    index_removed: Optional[int] = None
    index_error: Optional[str] = None

    if store is not None and chunk_ids:
        hook = getattr(store, "delete", None) or getattr(store, "remove", None)
        if hook is not None:
            try:
                result = await _call_hook(hook, chunk_ids)
                # 后端回报了条数就用它（如实回答「索引同步了几条」），否则按入参计
                index_removed = _removed_count(result, fallback=len(chunk_ids))
            except Exception as exc:  # noqa: BLE001 - 变成可读的 index_error
                index_error = f"派生索引同步失败（{type(exc).__name__}: {exc}）；{REBUILD_HINT}"
        elif _store_name(store) in SEPARATE_INDEX_BACKENDS:
            index_error = (
                f"后端 {_store_name(store)} 的索引是派生数据、且不提供删除接口，"
                f"权威行已删但索引可能仍含这些切片；{REBUILD_HINT}"
            )
        # 其余后端（sqlalchemy / memory）：索引即权威表，删行已同步，index_removed 保持 None

    return {
        "document_id": document_id,
        "deleted_document": True,
        "deleted_chunks": len(chunk_ids),
        "index_removed": index_removed,
        "index_error": index_error,
    }


async def rebuild_index(
    db: AsyncSession,
    *,
    store: Any = None,
    model: Optional[str] = None,
    document_id: Optional[int] = None,
    purge: bool = False,
) -> Dict[str, Any]:
    """从 **MySQL 权威行**重建派生向量索引（``store.add``，upsert 语义）。

    - 只取 ``embedding IS NOT NULL`` 的切片（口径与「待向量化」筛选条件一致）
    - 给了 ``document_id`` 就只重建该文档的切片，否则全量
    - ``embedding`` 不是非空列表的行（脏数据）**跳过并计数**，不让一条坏数据打挂整次重建
    - ``store.add`` 抛异常（如 ANN 后端的**混维度**限制）→ 记进 ``errors``，
      **不抛给调用方**（与导入管线同一口径：失败返回报告）

    ``purge``（默认 ``False``）
    --------------------------
    ``add`` 是 **upsert**，它只能**补写**、**清不掉残留**（权威行已被直接删掉时，
    派生索引里的旧条目会一直留着）。``purge=True`` 先清空派生索引再从权威行重建，
    才叫「真正重建」。三条安全约束（缺一条就会删掉不该删的东西）：

    1. **只对「索引与权威行分离」的后端生效**（``SEPARATE_INDEX_BACKENDS``）；
       ``sqlalchemy`` / ``memory`` 的索引就是权威行本身，清空 = **删掉知识库正文**，
       因此**直接拒绝**并把理由写进 ``purge_note``。
    2. **与 ``document_id`` 互斥**：清空是全局动作，而 ``document_id`` 只重建一篇，
       组合起来会把**别的**文档的索引一起清掉且不重建。
    3. 清空失败**不静默继续**：理由写进 ``purge_note`` 并计入 ``errors``。

    返回 7 键恒定报告（``REBUILD_RESULT_FIELDS``）：``scanned_chunks`` /
    ``rebuilt_chunks`` / ``skipped_chunks`` / ``purged_chunks`` / ``purge_note`` /
    ``model`` / ``errors``。

    **幂等**：``add`` 是 upsert，重复执行只会把同样的向量再写一遍，不会产生重复行。
    """
    if store is None:
        store = build_vector_store(db, model=model)

    purged_chunks: Optional[int] = None
    purge_note = ""
    errors: List[str] = []

    if purge:
        if document_id is not None:
            purge_note = PURGE_SCOPED_HINT
        else:
            reset = getattr(store, "reset", None)
            if reset is None:
                purge_note = (
                    PURGE_REFUSED_HINT
                    if _store_name(store) not in SEPARATE_INDEX_BACKENDS
                    else f"后端 {_store_name(store)} 未提供 reset()，已跳过清空，仅做 upsert"
                )
            else:
                try:
                    result = await _call_hook(reset)
                    purged_chunks = _removed_count(result, fallback=0)
                    purge_note = "已先清空派生索引，再从权威行重建"
                except Exception as exc:  # noqa: BLE001 - 变成可读的 errors
                    purge_note = "清空派生索引失败，已跳过清空，仅做 upsert"
                    errors.append(f"purge 失败：{type(exc).__name__}: {exc}")

    stmt = select(KnowledgeChunk).where(KnowledgeChunk.embedding.isnot(None))
    if document_id is not None:
        stmt = stmt.where(KnowledgeChunk.document_id == document_id)

    rows = (await db.execute(stmt.order_by(KnowledgeChunk.id))).scalars().all()

    records: List[VectorRecord] = []
    skipped = 0
    for row in rows:
        vector = row.embedding
        if not isinstance(vector, (list, tuple)) or not vector:
            skipped += 1
            continue
        records.append(
            VectorRecord(
                vector=list(vector),
                content=row.content,
                document_id=row.document_id,
                chunk_id=row.id,
                metadata=dict(row.chunk_metadata or {}),
                model=row.embedding_model or "",
            )
        )

    rebuilt = 0
    if records:
        try:
            rebuilt = int(await store.add(records))
        except Exception as exc:  # noqa: BLE001 - 变成可读的 errors
            errors.append(f"{type(exc).__name__}: {exc}")

    return {
        "scanned_chunks": len(rows),
        "rebuilt_chunks": rebuilt,
        "skipped_chunks": skipped,
        "purged_chunks": purged_chunks,
        "purge_note": purge_note,
        "model": model or getattr(store, "model", None),
        "errors": errors,
    }


__all__ = [
    "DELETE_RESULT_FIELDS",
    "PURGE_REFUSED_HINT",
    "PURGE_SCOPED_HINT",
    "REBUILD_HINT",
    "REBUILD_RESULT_FIELDS",
    "SEPARATE_INDEX_BACKENDS",
    "delete_document",
    "rebuild_index",
]
