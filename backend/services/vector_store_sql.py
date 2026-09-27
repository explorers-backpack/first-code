# -*- coding: utf-8 -*-
"""向量存储 · 基于既有 MySQL 表 ``knowledge_chunk`` 的**扩展字段**实现。

为什么用「扩展字段」而不是「独立表」
------------------------------------
需求允许两种做法，本项目选**扩展字段**，理由：

1. 任务 42 已经在 ``knowledge_chunk`` 上预留了 ``embedding`` / ``embedding_model`` /
   ``embedding_dim`` 三列——**结构已经在那儿了**，无需再建表、也无需动 ``schema_sync``。
2. 切片与向量是 **1:1**，独立表只会多一次 JOIN 和一套额外的生命周期管理
   （删切片要记得删向量表，否则留孤儿行）。
3. 检索时按 ``embedding IS NULL`` 就能天然表达「待向量化」，一个表就够。

**本模块只是 ``VectorStore`` 的一个后端实现**。接口（``services/vector_store.py``）
里没有任何数据库概念，因此换真实向量库（chromadb / milvus / pgvector …）
**不需要动接口、值对象、排序口径，也不需要动任何调用方**——见
``services/vector_store_chroma.py``（真实 ANN 后端，任务 48 新增）。

本模块仍然是**默认后端**（``VECTOR_STORE`` 未配置时）。它同时扮演
「**切片行的权威写入方**」：``vector_store_chroma`` 会组合本模块，先落 MySQL
再镜像到 ANN 索引，因此 ``knowledge_chunk.embedding`` 始终是向量的权威副本。

当前实现的规模边界（重要，别当成向量数据库）
--------------------------------------------
没有 ANN 索引，``search`` 是**全表扫描 + Python 侧算余弦**：

- 需要把所有已向量化的行读进内存（``WHERE embedding IS NOT NULL``）
- ``category`` 过滤也在 Python 侧做——因为 ``metadata`` 是 JSON，
  MySQL 与 SQLite 的 JSON 语法不同，写进 SQL 就失去可移植性
  （``document_id`` 是**真列**，所以它在 SQL 里过滤）

因此**只适合中小规模**（几千条以内）。切片上万、或 QPS 上来之后，
应当换真实向量库；届时本模块整体替换，调用方一行不用改。

为什么必须记 ``embedding_model``
--------------------------------
**不同模型的向量不可比**（维度可能不同、语义空间也不同）。因此：

- 写入时把模型标识落进 ``embedding_model``（记录自带 ``model``，否则用构造函数里的默认值）
- 查询时可用 ``model=`` 只比较同一模型的向量；**强烈建议显式传**
- 维度对不上的候选会被 ``select_matches`` 跳过；若「候选非空但一条都对不上」，
  会抛 ``VectorStoreDimensionError`` 而不是安静返回空
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import KnowledgeChunk
from services.vector_store import (
    DEFAULT_TOP_K,
    VectorMatch,
    VectorRecord,
    VectorStore,
    VectorStoreInputError,
    require_records,
    require_top_k,
    require_vector,
    select_matches,
)


def _row_to_record(row: KnowledgeChunk) -> Optional[VectorRecord]:
    """把 ``knowledge_chunk`` 行归一成 :class:`VectorRecord`；脏行返回 ``None``。

    库里可能存在**脏向量**（人为改过库 / 半截写入 / 手工塞了字符串）：那种行不该
    把整次检索打挂，也不该被镜像进 ANN 索引，因此统一在这里跳过——
    调用方只需 ``if record is not None``，判断只有这一处。
    """
    metadata = row.chunk_metadata if isinstance(row.chunk_metadata, dict) else {}
    try:
        return VectorRecord(
            vector=row.embedding,
            content=row.content or "",
            document_id=row.document_id,
            chunk_id=row.id,
            metadata=metadata,
            model=row.embedding_model or "",
        )
    except VectorStoreInputError:
        return None


class SqlAlchemyVectorStore(VectorStore):
    """把向量存进既有表 ``knowledge_chunk`` 的扩展字段。

    :param db: 由调用方注入的 ``AsyncSession``（依赖注入，便于脱离 HTTP 单测）。
        本对象很轻，**按请求构造**即可——不要做全局单例（session 有生命周期）。
    :param model: 默认模型标识。记录自带 ``model`` 时以记录为准；未带时用它补齐，
        避免出现一堆 ``embedding_model=""`` 的无主向量。同时作为 ``search``
        未显式传 ``model`` 时的默认筛选条件。
    """

    name = "sqlalchemy"

    def __init__(self, db: AsyncSession, *, model: Optional[str] = None) -> None:
        self.db = db
        self.model = model

    # --------------------------------------------------------
    # 写入
    # --------------------------------------------------------
    async def add(self, records: Any) -> int:
        """写入一组「切片 + 向量」（upsert 语义），返回写入条数。

        就是 :meth:`add_returning_ids` 加一个 ``len()``——**没有第二份实现**，
        因此「只回条数」与「回 id」两条路径的校验、字段覆盖范围、提交时机完全一致。
        """
        return len(await self.add_returning_ids(records))

    async def add_returning_ids(self, records: Any) -> List[int]:
        """与 :meth:`add` **完全相同的写入语义**，但返回每条记录对应的切片 id（顺序同入参）。

        为什么需要它：``add`` 只回条数，而「把同一批向量**镜像**到 ANN 索引」的后端
        （``vector_store_chroma.ChromaVectorStore``）必须知道每条记录最终落在哪个
        ``chunk_id`` 上——尤其 ``chunk_id`` 留空的记录，id 是**自增主键、写之前并不存在**。
        让权威存储把 id 报出来，好过让镜像方自己去建行、或者靠 ``SELECT`` 猜。
        （本方法**不是** ``VectorStore`` 接口的一部分：接口仍只有 ``add`` / ``search`` / ``count``。）

        - ``chunk_id`` 为空 → **新建**切片（必须给 ``document_id`` 与非空 ``content``）
        - ``chunk_id`` 有值 → 给**既有**切片补/更新向量；切片不存在则报错
          （而不是悄悄新建——那多半是调用方拿错了 id）

        补向量时**只写 ``embedding`` / ``embedding_model`` / ``embedding_dim`` 三列**，
        ``content`` / ``chunk_metadata`` / ``document_id`` 一律不动——与
        ``InMemoryVectorStore`` 的 upsert 语义保持一致（见 ``VectorStore.add`` 契约）。
        """
        items = require_records(records)
        ids: List[Optional[int]] = []
        #: 新建行先占位，``flush`` 拿到自增主键后回填（见下方）
        pending: List[Tuple[int, KnowledgeChunk]] = []
        for record in items:
            model = record.model or self.model or ""
            if record.chunk_id is None:
                if record.document_id is None:
                    raise VectorStoreInputError(
                        "新建切片必须提供 document_id（knowledge_chunk.document_id 非空）"
                    )
                if not record.content.strip():
                    raise VectorStoreInputError(
                        "新建切片必须提供非空 content（空切片入库没有意义）"
                    )
                row = KnowledgeChunk(
                    document_id=record.document_id,
                    content=record.content,
                    chunk_metadata=dict(record.metadata),
                    # JSON 列：整体赋值（原地改不会落库，见项目约定）
                    embedding=list(record.vector),
                    embedding_model=model,
                    embedding_dim=record.dimension,
                )
                self.db.add(row)
                pending.append((len(ids), row))
                ids.append(None)                # 占位：flush 后回填真实主键
            else:
                row = await self._load(record.chunk_id)
                if row is None:
                    raise VectorStoreInputError(
                        f"切片 {record.chunk_id} 不存在，无法写入向量"
                        "（若要新建切片，请把 chunk_id 留空）"
                    )
                row.embedding = list(record.vector)     # JSON 列必须整体重新赋值
                row.embedding_model = model
                row.embedding_dim = record.dimension
                ids.append(record.chunk_id)
        if pending:
            # flush 才能拿到自增 id（必须在 commit 之前，否则回填不到）
            await self.db.flush()
            for position, row in pending:
                ids[position] = row.id
        await self.db.commit()
        return [int(value) for value in ids]

    # --------------------------------------------------------
    # 检索
    # --------------------------------------------------------
    async def search(
        self,
        query_vector: Any,
        *,
        top_k: int = DEFAULT_TOP_K,
        model: Optional[str] = None,
        document_id: Optional[int] = None,
        category: Optional[str] = None,
        min_score: Optional[float] = None,
    ) -> List[VectorMatch]:
        query = require_vector(query_vector, what="query_vector")
        require_top_k(top_k)                    # 早失败：别等全表读完才发现 top_k 写错
        effective_model = model if model is not None else self.model

        stmt = select(KnowledgeChunk).where(KnowledgeChunk.embedding.is_not(None))
        if effective_model is not None:
            stmt = stmt.where(KnowledgeChunk.embedding_model == effective_model)
        if document_id is not None:
            stmt = stmt.where(KnowledgeChunk.document_id == document_id)
        # 固定候选顺序（主键升序）→ 同分时结果确定，满足「同输入同输出」
        stmt = stmt.order_by(KnowledgeChunk.id)
        rows = (await self.db.execute(stmt)).scalars().all()

        candidates: List[VectorRecord] = []
        for row in rows:
            record = _row_to_record(row)
            if record is None:
                continue                        # 脏行：跳过，不让一条坏数据打挂整次检索
            if category is not None and record.metadata.get("category") != category:
                continue
            candidates.append(record)

        return select_matches(
            candidates, query, top_k=top_k, min_score=min_score, store_name=self.name,
        )

    # --------------------------------------------------------
    # 权威行读回（给「镜像到 ANN 索引」的后端用）
    # --------------------------------------------------------
    async def load_records(self, chunk_ids: Any) -> List[VectorRecord]:
        """按 id 读回切片（**含向量**），返回 :class:`VectorRecord` 列表（按主键升序）。

        为什么存在：``vector_store_chroma.ChromaVectorStore`` 把本模块当作**切片行的
        权威副本**，写入后从**库里**读回内容与元信息再镜像进 ANN 索引。
        这样索引里的 ``content`` / ``metadata`` 永远等于权威行，而不会取调用方入参——
        入参在「给既有切片补向量」时**本就被契约要求忽略**（见 ``VectorStore.add``），
        若拿它当索引内容，改过正文的文档会让索引指向旧文字。

        找不到的 id 直接跳过（不报错）；脏行同样跳过（见 :func:`_row_to_record`）。
        空入参返回 ``[]``，**不查库**。
        """
        if chunk_ids is None:
            raise VectorStoreInputError("load_records 不接受 None；要读全部请自行传 id 列表")
        if isinstance(chunk_ids, (str, bytes, bytearray, int)) or isinstance(chunk_ids, bool):
            raise VectorStoreInputError(
                f"load_records 需要一组 id，收到单个 {type(chunk_ids).__name__}；请包成 [id]"
            )
        try:
            wanted = [int(value) for value in chunk_ids]
        except (TypeError, ValueError) as exc:
            raise VectorStoreInputError(f"load_records 的 id 必须是整数：{exc}") from exc
        if not wanted:
            return []

        rows = (await self.db.execute(
            select(KnowledgeChunk).where(KnowledgeChunk.id.in_(wanted))
            # 主键升序 = 全项目统一的「候选顺序」，同分兜底排序据此确定
            .order_by(KnowledgeChunk.id)
        )).scalars().all()
        records: List[VectorRecord] = []
        for row in rows:
            record = _row_to_record(row)
            if record is not None:
                records.append(record)
        return records

    # --------------------------------------------------------
    # 统计
    # --------------------------------------------------------
    async def count(self) -> int:
        """**已向量化**的切片数（不是切片总数）。"""
        result = await self.db.execute(
            select(func.count()).select_from(KnowledgeChunk)
            .where(KnowledgeChunk.embedding.is_not(None))
        )
        return int(result.scalar_one())

    # --------------------------------------------------------
    # 内部
    # --------------------------------------------------------
    async def _load(self, chunk_id: int) -> Optional[KnowledgeChunk]:
        result = await self.db.execute(
            select(KnowledgeChunk).where(KnowledgeChunk.id == chunk_id)
        )
        return result.scalar_one_or_none()


__all__ = ["SqlAlchemyVectorStore"]
