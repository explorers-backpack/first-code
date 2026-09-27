# -*- coding: utf-8 -*-
"""临时探针：打印当前知识库的文档 / 切片 / 向量分布。用完即删。"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import func, select  # noqa: E402

from database import async_session, engine  # noqa: E402
from models import KnowledgeChunk, KnowledgeDocument  # noqa: E402


async def main() -> None:
    try:
        await _report()
    finally:
        # 必须在**同一个事件循环内**关掉连接池：否则 aiomysql 的
        # ``Connection.__del__`` 会在循环关闭后才触发，打印一串与本次探测无关的
        # "Event loop is closed" 噪声。
        await engine.dispose()


async def _report() -> None:
    async with async_session() as db:
        docs = (await db.execute(select(func.count()).select_from(KnowledgeDocument))).scalar_one()
        chunks = (await db.execute(select(func.count()).select_from(KnowledgeChunk))).scalar_one()
        print(f"documents = {docs}")
        print(f"chunks    = {chunks}")

        rows = (await db.execute(
            select(
                KnowledgeChunk.embedding_model,
                KnowledgeChunk.embedding_dim,
                func.count(),
            ).group_by(KnowledgeChunk.embedding_model, KnowledgeChunk.embedding_dim)
        )).all()
        print("按 (embedding_model, embedding_dim) 分组：")
        for model, dim, count in rows:
            print(f"  model={model!r} dim={dim!r} count={count}")

        nulls = (await db.execute(
            select(func.count()).select_from(KnowledgeChunk)
            .where(KnowledgeChunk.embedding.is_(None))
        )).scalar_one()
        print(f"embedding IS NULL = {nulls}")

        for doc in (await db.execute(
            select(KnowledgeDocument).order_by(KnowledgeDocument.id)
        )).scalars().all():
            n = (await db.execute(
                select(func.count()).select_from(KnowledgeChunk)
                .where(KnowledgeChunk.document_id == doc.id)
            )).scalar_one()
            print(f"  doc#{doc.id} [{doc.category}] {doc.title!r} source={doc.source!r} chunks={n}")


asyncio.run(main())
