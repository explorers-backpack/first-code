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

    步骤
    ----
    1. **上下文整合**：取上下文包（简历 + 岗位 + **历史回答** + 进度），
       只读派生、不落库。
    2. **知识检索**（可选增强）：注入 ``retriever`` 优先；``use_rag=True``
       才组装真实链路；故障静默降级为「无知识」。
    3. **模型生成**：调用 ``interview_next_agent``（携带历史回答的 Prompt 组），
       知识为空时走原模板。

    返回 ``interview_next_agent`` 的候选结果（``ok`` / ``question`` / ``error``
    等键与它同形状），**本函数不解释也不裁剪**。
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
    return await interview_next_agent.generate_question(
        pack, plan_items, knowledge_context=chunks, spark=spark
    )


__all__ = ["generate_candidate"]
