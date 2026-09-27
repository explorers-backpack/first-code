# -*- coding: utf-8 -*-
"""知识库路由层（导入 / 查询 / 维护）。

职责边界
--------
与 ``api/interview.py`` 同款：**参数校验 → 调用 service → 声明响应模型**。
一切业务判断都在 service 层：

- ``services/knowledge_document_service``   —— 文档读写与校验（唯一校验口径）
- ``services/knowledge_import_pipeline``    —— 切片 + 向量化 + 落库（**唯一写侧接线点**）
- ``services/knowledge_maintenance``        —— 显式删除 / 索引重建（维护动作）

本层**不 import** Agent / Core / Retriever，也不自己组装向量库或 Embedding。

权限（本文件给出「谁能导入知识」的答案）
----------------------------------------
- **读**（``GET`` 列表 / 详情）：任意已登录用户
- **写**（``POST`` 导入 / 重建、``DELETE`` 删除）：**仅 admin**

理由：知识库是**维护面**。普通用户需要的是「面试时用上知识」，不是「改知识」；
写入与删除会**改变所有用户的检索结果**，属于运维动作。
越权写入由 ``deps.get_current_admin`` 拦下并返回 ``403``。

失败语义
--------
导入与重建都是**恒定形状的报告**（``ok`` / ``status`` / ``errors`` …），
因此失败时**仍返回 200**，由报告字段承载结果——与
``/api/interview/{id}/next-question`` 的 ``ok=False`` 口径一致。
唯一走非 2xx 的是**入参契约**（400/422）与**资源不存在**（404）。
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from deps import get_current_admin, get_current_user
from schemas.knowledge import (
    KnowledgeDeleteResponse,
    KnowledgeDocumentListResponse,
    KnowledgeDocumentOut,
    KnowledgeImportReport,
    KnowledgeImportRequest,
    KnowledgeRebuildResponse,
)
from services import knowledge_document_service, knowledge_import_pipeline
from services import knowledge_maintenance

router = APIRouter(prefix="/api/knowledge", tags=["知识库"])

DocumentId = Path(..., ge=1, description="知识文档 id")


async def _run(action: str, coro):
    """统一异常收敛：业务异常透传，未预期异常转 500。"""
    try:
        return await coro
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 - 兜底，避免未捕获异常直接 500 无提示
        raise HTTPException(status_code=500, detail=f"{action}失败：{exc}") from exc


# ============================================================
# 写：导入（admin）
# ============================================================
@router.post(
    "/documents",
    response_model=KnowledgeImportReport,
    summary="导入知识文档（切片 + 向量化 + 落库）",
    description=(
        "把一篇**纯文本**知识完整导入知识库：落库 → 切片 → 逐片向量化 → 写向量库。\n\n"
        "**仅 admin 可调用**（写入会改变所有用户的检索结果）。\n\n"
        "入参契约问题（空标题 / 空正文 / 非法 `category`）返回 **400**；\n"
        "**中途失败**（切片 / 向量化 / 落库 / 写向量）返回 **200** + `ok=false`，"
        "报告里 `stage` / `failed_index` / `errors` 说明跑到哪一步、为什么失败，"
        "计数保留已做完的部分，**重跑会接着做**（三层幂等）。\n\n"
        "本接口**不做文件解析**：`content` 必须是已解析好的纯文本。"
    ),
)
async def import_knowledge_document(
    payload: KnowledgeImportRequest,
    current_admin: dict = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> KnowledgeImportReport:
    report = await _run(
        "导入知识文档",
        knowledge_import_pipeline.import_document(db, payload.model_dump()),
    )
    return KnowledgeImportReport(**report)


# ============================================================
# 读：列表 / 详情（登录用户）
# ============================================================
@router.get(
    "/documents",
    response_model=KnowledgeDocumentListResponse,
    summary="查询知识文档列表",
    description=(
        "分页返回文档**元信息**（**不含正文**，避免一次拉出整库正文）。\n\n"
        "`category` 传非法值直接 **400**（而不是静默返回空——否则拼错枚举会得到"
        "「知识库是空的」这种误导性结论）；`keyword` 只匹配**标题**，"
        "正文全文检索走向量检索。`limit` / `offset` 越界自动收敛，不报错。"
    ),
)
async def list_knowledge_documents(
    category: Optional[str] = Query(default=None, description="按四类知识过滤"),
    keyword: Optional[str] = Query(default=None, description="标题模糊匹配"),
    limit: int = Query(default=20, ge=1, le=100, description="每页条数"),
    offset: int = Query(default=0, ge=0, description="偏移量"),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> KnowledgeDocumentListResponse:
    data = await _run(
        "查询知识文档列表",
        knowledge_document_service.list_documents(
            db, category=category, keyword=keyword, limit=limit, offset=offset
        ),
    )
    return KnowledgeDocumentListResponse(**data)


@router.get(
    "/documents/{document_id}",
    response_model=KnowledgeDocumentOut,
    summary="查询知识文档详情",
    description="按 id 返回整篇文档（含正文全文）；不存在返回 404。",
)
async def get_knowledge_document(
    document_id: int = DocumentId,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> KnowledgeDocumentOut:
    data = await _run(
        "查询知识文档详情",
        knowledge_document_service.get_document(db, document_id),
    )
    return KnowledgeDocumentOut(**data)


# ============================================================
# 维护：删除 / 重建（admin）
# ============================================================
@router.delete(
    "/documents/{document_id}",
    response_model=KnowledgeDeleteResponse,
    summary="删除知识文档（含全部切片）",
    description=(
        "**显式维护动作**：删除该文档的**全部切片**再删文档本身，并同步派生索引。\n\n"
        "**仅 admin 可调用**。项目约定「不静默删数据」——本接口是那个"
        "「独立的维护动作」，因此每次删除都回报删了几条切片、索引同步了几条。\n\n"
        "`index_removed` = 从派生索引**真实移除**的条数；`sqlalchemy` / `memory` "
        "后端的索引就是权威行本身（删行即同步），故为 `null`。\n"
        "若某后端既没有删除原语、索引又与权威行分离，会在 `index_error` 里"
        "**明确说明**并提示重建，**不会假装同步成功**。\n\n"
        "文档不存在返回 **404**。"
    ),
)
async def delete_knowledge_document(
    document_id: int = DocumentId,
    current_admin: dict = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> KnowledgeDeleteResponse:
    data = await _run(
        "删除知识文档",
        knowledge_maintenance.delete_document(db, document_id),
    )
    return KnowledgeDeleteResponse(**data)


@router.post(
    "/rebuild",
    response_model=KnowledgeRebuildResponse,
    summary="重建向量索引（从 MySQL 权威副本）",
    description=(
        "按 `knowledge_chunk.embedding` 的**权威行**重新写入向量索引（upsert 语义，幂等）。\n\n"
        "**仅 admin 可调用**。适用场景：换了向量后端 / ANN 索引被删 / "
        "删过文档后需要清掉派生索引里的残留。\n\n"
        "`document_id` 给了就只重建该文档，否则**全量**。\n"
        "`purge=true` 时**先清空派生索引**再重建——`add` 只能补写、清不掉残留，"
        "所以「真正重建」需要它。两条安全约束：\n\n"
        "1. `purge` 只对**索引与权威行分离**的后端（`chroma`）生效；"
        "`sqlalchemy` / `memory` 的索引就是权威行本身，清空等于删知识库正文 ⇒ "
        "**直接拒绝**，理由写进 `purge_note`；\n"
        "2. `purge` 与 `document_id` **互斥**（清空是全局动作，"
        "配 `document_id` 会把别的文档的索引一起清掉且不重建）。\n\n"
        "`embedding` 为空或格式不对的行**跳过并计数**；写入失败（如 ANN 后端"
        "不支持混维度）记进 `errors`，**返回 200 而不是抛异常**。"
    ),
)
async def rebuild_knowledge_index(
    document_id: Optional[int] = Query(
        default=None, ge=1, description="只重建该文档；不传 = 全量重建"
    ),
    purge: bool = Query(
        default=False, description="先清空派生索引再重建（仅对 chroma 生效；与 document_id 互斥）"
    ),
    current_admin: dict = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> KnowledgeRebuildResponse:
    data = await _run(
        "重建向量索引",
        knowledge_maintenance.rebuild_index(db, document_id=document_id, purge=purge),
    )
    return KnowledgeRebuildResponse(**data)
