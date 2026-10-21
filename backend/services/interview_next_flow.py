# -*- coding: utf-8 -*-
"""数字人 LLM 单题出题 · **组装层**（取材料 → 知识检索 → 调 Agent）。

分工（与追问链同构）
--------------------
``interview_service`` 负责**落库**与状态机口径（题号以 ``current_question_no``
为准）；本模块负责**取材料并调用模型**，把「上下文包 + 知识检索 + Agent 调用」
三步收在一处——这样 ``interview_service`` 不必触碰 Agent / Context 的接缝
（Service 侧有**源码级**守卫：全文不得出现 ``generate_question`` /
``load_context`` 等接缝字面量）。

★ 本模块**不落库**、**不推进题号**、**不 import 出题策略层**
（``services.question_generator`` 只从编排层进入生产链路）。
★ 任何检索故障都静默降级为「无知识」，绝不让出题中断（与追问链同一约定）。
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Sequence

from sqlalchemy.ext.asyncio import AsyncSession

from services import (
    interview_context_pack,
    interview_followup,
    interview_next_agent,
)


#: :func:`prepare_candidate` 返回快照的**恒定键集**（调用方可按它断言形状）。
PREPARED_FIELDS: tuple = (
    "pack",
    "plan_items",
    "knowledge_context",
    "topic",
    "question_no",
)


async def prepare_candidate(
    db: AsyncSession,
    session: Any,
    plan_items: Sequence[Mapping[str, Any]],
    *,
    question_no: int,
    retriever: Any = None,
    use_rag: bool = True,
    retriever_kwargs: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """**取材料**（只读，不调用模型）→ 返回纯数据快照。

    ★ 为什么要拆成「取材料 / 调模型」两段（并发安全的核心）
    ------------------------------------------------------
    模型调用实测 **8~19 秒**。如果它和前面的 DB 读取处在**同一个事务**里，
    就会持有一条长读事务（MySQL ``REPEATABLE READ`` 下还要保留 undo 快照）
    并长期占用连接池连接 —— 症状不是"卡死"而是"高并发下超时"，很难归因。

    因此本函数**只做 DB 读取与知识检索**，把结果整理成**普通 dict**（不含 ORM 对象），
    由调用方在返回后**结束事务**，再交给 :func:`generate_from_prepared` 去调模型。
    ⇒ 模型调用期间**没有任何数据库事务在开着**。

    返回键集（``PREPARED_FIELDS``，恒定）::

        {"pack", "plan_items", "knowledge_context", "topic", "question_no"}
    """
    topic = next(
        (item["topic"] for item in plan_items if item["question_no"] == question_no), ""
    )
    pack = await interview_context_pack.load_context_pack(db, session.id)
    chunks, _warnings, _resolved = await interview_followup.gather_knowledge(
        db,
        session,
        topic,
        pack,
        retriever=retriever,
        use_rag=use_rag,
        retriever_kwargs=retriever_kwargs,
    )
    return {
        "pack": pack,
        "plan_items": list(plan_items),
        "knowledge_context": list(chunks or []),
        "topic": topic,
        "question_no": question_no,
    }


async def generate_from_prepared(
    prepared: Mapping[str, Any], *, spark: Any = None
) -> Dict[str, Any]:
    """**纯外部调用**：拿 :func:`prepare_candidate` 的快照去调模型（**不碰数据库**）。

    ★ 本函数刻意**不接收 ``db``**：没有数据库句柄，就不可能无意中开事务。
    返回值与 ``interview_next_agent.generate_question`` 同形状，不解释也不裁剪。
    """
    return await interview_next_agent.generate_question(
        prepared["pack"],
        prepared["plan_items"],
        knowledge_context=prepared["knowledge_context"],
        spark=spark,
    )


async def generate_candidate(
    db: AsyncSession,
    session: Any,
    plan_items: Sequence[Mapping[str, Any]],
    *,
    question_no: int,
    spark: Any = None,
    retriever: Any = None,
    use_rag: bool = True,
    retriever_kwargs: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """组装上下文与知识，生成**第 ``question_no`` 题**的候选（**不落库**）。

    ★ **兼容入口**：等价于 ``prepare_candidate`` + ``generate_from_prepared``，
    保留它是为了让既有调用方/测试无需改动。**需要"事务不跨模型调用"的调用方
    必须显式分两步调用**（见 :func:`prepare_candidate` 的说明）。
    """
    prepared = await prepare_candidate(
        db,
        session,
        plan_items,
        question_no=question_no,
        retriever=retriever,
        use_rag=use_rag,
        retriever_kwargs=retriever_kwargs,
    )
    return await generate_from_prepared(prepared, spark=spark)


__all__ = [
    "PREPARED_FIELDS",
    "generate_candidate",
    "generate_from_prepared",
    "prepare_candidate",
]
