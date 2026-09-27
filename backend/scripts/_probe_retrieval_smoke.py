# -*- coding: utf-8 -*-
"""临时冒烟：用生产读侧路径（role=query）检索刚导入的真实库。用完即删。"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import async_session, engine  # noqa: E402
from services.knowledge_rag import build_vector_retriever  # noqa: E402

QUERIES = [
    "Redis 的 RDB 和 AOF 有什么区别？",
    "MySQL 联合索引为什么要注意最左前缀？",
    "Kafka 怎么保证消息不丢？",
    "JVM 线上 CPU 飙高怎么排查？",
    "秒杀场景怎么做限流和降级？",
    "公司的面试流程是怎样的？",
]


async def main() -> None:
    try:
        async with async_session() as db:
            retriever = build_vector_retriever(db, top_k=3)
            print("读侧 embedder:", retriever.embedder.name,
                  "domain=", getattr(retriever.embedder, "domain", None))
            for q in QUERIES:
                hits = await retriever.retrieve({"job_title": "后端工程师"}, q, "")
                print(f"\nQ: {q}")
                if not hits:
                    print("   （无命中）")
                for h in hits:
                    print(f"   {h.metadata.get('score', 0):.6f}  {h.source}")
                await asyncio.sleep(3)
    finally:
        await engine.dispose()


asyncio.run(main())
