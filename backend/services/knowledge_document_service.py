# -*- coding: utf-8 -*-
"""AI 面试知识库 · KnowledgeDocumentService（文档导入：只落库，不做 RAG）。

分层定位
--------
::

    api/*（本阶段**未**新增 HTTP 路由）   ← 将来：/api/admin/knowledge/*
        └── knowledge_document_service     ← 【本模块】知识文档的写入与查询
              └── models.knowledge         ← KnowledgeDocument / KnowledgeChunk（ORM）

本模块只做一件事：**把一篇知识内容存进知识库**（``create_document``），
外加把它读回来（``get_document`` / ``list_documents``）。刻意**不实现**：

- **文档解析**（PDF / Word / Markdown → 纯文本）——本模块接收的入参**已经是纯文本**，
  「二进制 → 文本」是解析器的职责，与存储解耦
- **切片（Chunk）**——``KnowledgeChunk`` 表**一行都不写**，本阶段保持空表；
  怎么切、切多大是检索侧的决定，不由写入侧擅自决定
- **Embedding / 向量库 / 真实 Retriever**
- **InterviewAgent / InterviewCore / InterviewService 的任何修改**

因此本模块与面试流程**完全解耦**：它不知道面试的存在，面试也不知道它的存在
（``services/knowledge_retriever.py`` 那条「零依赖」边界依然成立）。
将来的真实 RAG 链路是::

    导入（本模块）→ 切片 → Embedding → 向量库 → Retriever
                  → Core.retrieve_knowledge → Agent

本模块只占**第一环**。

四类知识（``category``）
------------------------
取值来自 ``models.KNOWLEDGE_CATEGORIES``，本模块**不硬编码**（避免两处枚举漂移）：

==================  ==================  ==================================
取值                中文                典型来源
==================  ==================  ==================================
``job``             岗位知识            岗位 JD / 能力模型（**岗位JD**）
``technical``       技术知识            技术手册 / 官方文档（**技术文档**）
``company``         公司知识            公司介绍 / 业务线（**公司资料**）
``project``         项目经验知识        真实项目复盘 / 案例库
==================  ==================  ==================================

输入形状
--------
``create_document(db, payload)`` 的 ``payload`` 是 ``{title, content, category, source}``。
为了同时服务三种调用方——脚本里的 ``dict``、将来 API 的 Pydantic 模型、
测试里的 ``SimpleNamespace``——本模块用 ``_read_field`` 统一取值
（**映射取键 / 对象取属性**），**不绑定具体类型**，故不必为每个调用方写一层适配。

对外约定（与 ``interview_service`` 同口径）
-------------------------------------------
- 函数以 ``db: AsyncSession`` 为**第一参数**（依赖注入），返回**普通 dict**
- 业务错误抛 ``HTTPException``（``400`` 参数非法 / ``404`` 不存在），
  与既有 Service 一致——将来加 HTTP 路由时可**原样透传**，不必重写错误处理
- 不依赖 FastAPI 的请求上下文，因此可脱离 HTTP 单独测试

字段口径（重要）
----------------
- ``title`` / ``source`` 是**标签**，落库前 ``strip()`` 去首尾空白（列表里更整洁）
- ``content`` 是**正文**，**原样保存**（不 strip、不改写）——知识库正文是权威来源，
  静默改写会让切片与溯源失真；但**空判定**用 ``content.strip()``，
  故「只有空白字符的正文」同样视为空并被拒绝
- ``created_at`` 由 ORM 侧 ``default=datetime.utcnow`` 赋值，本模块**不手工设置**

刻意**不做**的两件事
--------------------
1. **不去重**：同一 ``title`` 可以多次导入（每次都是新行）。
   去重/覆盖策略是产品决策（按 ``source`` 覆盖？按 ``title`` 唯一？），
   本阶段不擅自引入——真实导入流程定下来后再决定。
2. **不建 HTTP 路由**：本阶段只交付 Service。加路由要先定「谁能导入知识」
   （管理员？普通用户？），这属权限设计，需显式授权后再做。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, Optional

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import KNOWLEDGE_CATEGORIES, KnowledgeDocument

#: 出参契约：``_document_to_dict`` 恒返回这 6 个键（成功 / 失败同一形状）
DOCUMENT_FIELDS = ("id", "title", "content", "category", "source", "created_at")

#: 列表项契约：**刻意不含 ``content``**——列表接口一次可能返回几十篇，
#: 把整库正文塞进响应既慢又无意义；要正文请用 ``get_document`` 单篇取。
DOCUMENT_SUMMARY_FIELDS = ("id", "title", "category", "source", "created_at")

#: 列表默认页大小 / 单页上限（防止一次把整个知识库拉回前端）
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100


# ============================================================
# 一、入参归一与序列化辅助
# ============================================================
def _read_field(payload: Any, name: str) -> Any:
    """从**映射**或**对象**里取字段；缺省返回 ``None``（表示「未提供」）。

    同时支持 ``{"title": ...}`` 与 ``SimpleNamespace(title=...)`` / Pydantic 模型，
    使本模块可被脚本、测试与将来的 API 层复用，而不必各自做适配。
    """
    if isinstance(payload, Mapping):
        return payload.get(name)
    return getattr(payload, name, None)


def _as_label(value: Any) -> str:
    """标签类字段归一：``None`` → ``""``；其余转字符串并去首尾空白。"""
    if value is None:
        return ""
    return str(value).strip()


def _as_content(value: Any) -> str:
    """正文字段归一：``None`` → ``""``；其余转字符串但**保留首尾空白**（原样保存）。"""
    if value is None:
        return ""
    return value if isinstance(value, str) else str(value)


def _document_to_dict(document: KnowledgeDocument) -> Dict[str, Any]:
    """整篇文档 → 普通 dict（键集合恒为 ``DOCUMENT_FIELDS``）。"""
    return {
        "id": document.id,
        "title": document.title,
        "content": document.content,
        "category": document.category,
        # 列 default 在 INSERT 时生效，未 flush 的对象上可能为 None，
        # 回退到 "" 保证出参契约里该字段恒为字符串（同 _session_to_dict 的写法）。
        "source": document.source or "",
        "created_at": document.created_at,
    }


def _document_summary(document: KnowledgeDocument) -> Dict[str, Any]:
    """文档元信息 → 普通 dict（键集合恒为 ``DOCUMENT_SUMMARY_FIELDS``，**无正文**）。"""
    return {
        "id": document.id,
        "title": document.title,
        "category": document.category,
        "source": document.source or "",
        "created_at": document.created_at,
    }


# ============================================================
# 二、写入：文档导入
# ============================================================
async def create_document(db: AsyncSession, payload: Any) -> Dict[str, Any]:
    """把一篇知识内容写入知识库，返回落库后的文档 dict。

    ``payload`` 需含 ``{title, content, category, source}``
    （``source`` 可选，缺省 ``""``）。

    校验顺序（先报最外层的错误，便于调用方定位）：

    1. ``title`` 非空（去首尾空白后）——列定义 ``nullable=False``，空标题属脏数据
    2. ``content`` 非空（**只有空白也算空**）——空知识入库毫无意义
    3. ``category`` ∈ ``models.KNOWLEDGE_CATEGORIES``

    **只写 ``KnowledgeDocument``，不生成任何 ``KnowledgeChunk``**
    （本阶段不切片；切片是检索侧的派生数据，等真实 RAG 链路定下来再做）。
    """
    title = _as_label(_read_field(payload, "title"))
    if not title:
        raise HTTPException(status_code=400, detail="title 不能为空")

    content = _as_content(_read_field(payload, "content"))
    if not content.strip():
        raise HTTPException(status_code=400, detail="content 不能为空")

    category = _as_label(_read_field(payload, "category"))
    if category not in KNOWLEDGE_CATEGORIES:
        raise HTTPException(
            status_code=400,
            detail=f"category 取值非法，允许：{'/'.join(KNOWLEDGE_CATEGORIES)}",
        )

    source = _as_label(_read_field(payload, "source"))

    document = KnowledgeDocument(
        title=title,
        content=content,
        category=category,
        source=source,
    )
    db.add(document)
    await db.commit()
    await db.refresh(document)
    return _document_to_dict(document)


# ============================================================
# 三、查询：单篇读取 / 列表分页
# ============================================================
async def get_document(db: AsyncSession, document_id: int) -> Dict[str, Any]:
    """按 id 读回整篇文档（含 ``content``）；不存在抛 ``404``。"""
    result = await db.execute(
        select(KnowledgeDocument).where(KnowledgeDocument.id == document_id)
    )
    document = result.scalar_one_or_none()
    if document is None:
        raise HTTPException(status_code=404, detail=f"知识文档 {document_id} 不存在")
    return _document_to_dict(document)


def _filtered(stmt, category: Optional[str], keyword: Optional[str]):
    """给查询语句叠加过滤条件（列表与计数共用，保证两者口径永远一致）。"""
    if category is not None:
        stmt = stmt.where(KnowledgeDocument.category == category)
    if keyword is not None:
        # 仅匹配标题：正文全文检索留给将来的向量检索，避免在 TEXT 上做全表 LIKE
        stmt = stmt.where(KnowledgeDocument.title.like(f"%{keyword}%"))
    return stmt


async def list_documents(
    db: AsyncSession,
    *,
    category: Optional[str] = None,
    keyword: Optional[str] = None,
    limit: int = DEFAULT_PAGE_SIZE,
    offset: int = 0,
) -> Dict[str, Any]:
    """分页查询文档**元信息**（不含 ``content``），返回 ``{total, items, limit, offset}``。

    - ``category``：按四类知识之一过滤；传了非法值直接 ``400``（而不是静默返回空，
      否则拼错枚举会得到「知识库是空的」这种误导性结论）
    - ``keyword``：**标题**模糊匹配（去首尾空白后为空则视为不传）
    - ``limit`` / ``offset``：越界自动收敛（``1..MAX_PAGE_SIZE`` / ``>=0``），
      不抛错——列表接口对分页参数宽容一些更实用
    """
    if category is not None:
        category = _as_label(category) or None
        if category is not None and category not in KNOWLEDGE_CATEGORIES:
            raise HTTPException(
                status_code=400,
                detail=f"category 取值非法，允许：{'/'.join(KNOWLEDGE_CATEGORIES)}",
            )

    keyword = _as_label(keyword) or None

    limit = max(1, min(int(limit), MAX_PAGE_SIZE))
    offset = max(0, int(offset))

    total = (
        await db.execute(
            _filtered(
                select(func.count()).select_from(KnowledgeDocument), category, keyword
            )
        )
    ).scalar_one()

    rows = (
        await db.execute(
            _filtered(select(KnowledgeDocument), category, keyword)
            .order_by(KnowledgeDocument.id.desc())
            .limit(limit)
            .offset(offset)
        )
    ).scalars().all()

    return {
        "total": int(total),
        "items": [_document_summary(row) for row in rows],
        "limit": limit,
        "offset": offset,
    }


__all__ = [
    "DEFAULT_PAGE_SIZE",
    "DOCUMENT_FIELDS",
    "DOCUMENT_SUMMARY_FIELDS",
    "MAX_PAGE_SIZE",
    "create_document",
    "get_document",
    "list_documents",
]
