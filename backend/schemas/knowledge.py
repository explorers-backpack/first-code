# -*- coding: utf-8 -*-
"""知识库 Pydantic 契约（导入 / 查询 / 维护）。

划分原则与 ``schemas/interview.py`` 一致：
- ``*Request`` —— 入参校验
- ``*Out`` / ``*Response`` —— 出参契约，同时作为路由的 ``response_model``

**刻意不在本文件重复业务校验**
--------------------------------
``title`` / ``content`` / ``category`` 的合法性由
``services.knowledge_document_service.create_document`` 判定（它会抛
``HTTPException(400)``，且**消息里列出允许的 category 取值**）。
本文件因此把这三个字段声明为**无约束的 ``str``**——若在这里加
``Literal`` / ``min_length``，非法输入会先被 pydantic 拦成 **422**，
service 里那份「唯一校验口径」就变成**不可达的死代码**，
两处校验也会随时间漂移。**保留 service 为唯一判定处**是刻意的。

字段集恒定的报告
----------------
``KnowledgeImportReport`` 与 ``services.knowledge_import_pipeline.IMPORT_RESULT_FIELDS``
的 12 个键一一对应：**成功 / 跳过 / 失败同一形状**，调用方无需分支取值。
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field

# ============================================================
# 请求体
# ============================================================
class KnowledgeImportRequest(BaseModel):
    """导入一篇**纯文本**知识（服务端负责切片 + 向量化 + 落库）。

    ``content`` 是已解析好的纯文本——「二进制 → 文本」（PDF / Word 解析）
    不属于本接口，由调用方在导入前完成。
    ``category`` 取 ``job`` / ``technical`` / ``company`` / ``project`` 之一
    （见 ``models.knowledge.KNOWLEDGE_CATEGORIES``），**合法性由 service 判定**。
    """

    title: str = Field(..., description="文档标题（非空，≤300 字符，由 service 校验）")
    content: str = Field(..., description="文档正文纯文本（非空，由 service 校验）")
    category: str = Field(
        ...,
        description="知识分类：job / technical / company / project（由 service 校验）",
    )
    source: Optional[str] = Field(
        default=None,
        description="来源标识（自由文本，如 manual://handbook/redis；缺省为空串）",
    )


# ============================================================
# 响应体
# ============================================================
class KnowledgeDocumentSummary(BaseModel):
    """文档**元信息**（列表项用）——刻意**不含 ``content``**。

    列表接口若带上正文，一次分页就会把整库正文拉进响应，既慢又无意义。
    """

    id: int
    title: str
    category: str
    source: str = ""
    created_at: Optional[datetime] = None


class KnowledgeDocumentOut(KnowledgeDocumentSummary):
    """文档详情（含正文全文）。"""

    content: str


class KnowledgeDocumentListResponse(BaseModel):
    total: int
    items: List[KnowledgeDocumentSummary] = Field(default_factory=list)
    limit: int
    offset: int


class KnowledgeImportReport(BaseModel):
    """导入报告 —— 与 ``IMPORT_RESULT_FIELDS`` 的 12 键一一对应，**形状恒定**。

    ``ok=False`` 时 ``status="failed"``，``stage`` 指出失败发生在哪一步
    （``document`` / ``chunk`` / ``embedding`` / ``persist`` / ``vector``），
    ``failed_index`` 是切片序号，``errors`` 是稳定错误码。
    计数字段保留**已经做完的部分**，因此「跑到哪儿了」可读、重跑会接着做。
    """

    ok: bool
    status: str
    stage: str
    document_id: Optional[int] = None
    chunk_count: int = 0
    saved_chunks: int = 0
    embedded_chunks: int = 0
    skipped_chunks: int = 0
    reused_document: bool = False
    failed_index: Optional[int] = None
    errors: List[str] = Field(default_factory=list)
    error: str = ""


class KnowledgeUploadReport(KnowledgeImportReport):
    """**文件上传**导入报告 = 导入报告 + 「这份文本是怎么来的」。

    继承 ``KnowledgeImportReport``，因此 12 键形状**完全不变**（前端可复用同一块
    报告渲染），额外携带解析阶段的如实信息：

    - ``parsed_format``  —— 实际走的解析器（``pdf`` / ``docx`` / ``pptx`` / ``html`` / ``text``）
    - ``parsed_chars``   —— 解析出的正文字符数（**不是**文件字节数）
    - ``parsed_encoding``—— 文本类文件实际使用的编码；非 UTF-8 时会同时进 ``parse_warnings``
    - ``parse_warnings`` —— 解析告警（稳定码：``decoded_as_gb18030`` /
      ``pdf_encrypted_with_empty_password`` / ``page_N_no_text_layer`` / ``slide_N_no_text`` …）

    ``parse_warnings`` **不是失败**：解析成功才会走到导入，它只表示「有件事你该知道」。
    """

    filename: str = ""
    parsed_format: str = ""
    parsed_chars: int = 0
    parsed_encoding: str = ""
    parse_warnings: List[str] = Field(default_factory=list)


class KnowledgeDeleteResponse(BaseModel):
    """删除文档的结果（**显式维护动作**，不做静默清理）。"""

    document_id: int
    deleted_document: bool
    deleted_chunks: int
    index_removed: Optional[int] = None
    index_error: Optional[str] = None


class KnowledgeRebuildResponse(BaseModel):
    """向量索引重建结果（从 MySQL 权威副本重建派生索引）。

    与 ``services.knowledge_maintenance.REBUILD_RESULT_FIELDS`` 的 7 个键一一对应。

    ``purge=True`` 时先清空**派生索引**再重建（``add`` 是 upsert，只能补写、
    清不掉残留）；``purged_chunks`` 是清掉的条数，``purge_note`` 说明清空是否
    真的执行了——对「索引即权威行」的后端（``sqlalchemy`` / ``memory``）会被
    **拒绝**（清空等于删知识库正文），此时 ``purged_chunks`` 为 ``None``。
    """

    scanned_chunks: int
    rebuilt_chunks: int
    skipped_chunks: int
    purged_chunks: Optional[int] = None
    purge_note: str = ""
    model: Optional[str] = None
    errors: List[str] = Field(default_factory=list)
