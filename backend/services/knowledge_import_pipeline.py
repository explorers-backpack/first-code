# -*- coding: utf-8 -*-
"""AI 面试知识库 · KnowledgeImportPipeline（知识入库：文档 → 切片 → 向量 → 向量库）。

分层定位
--------
::

    knowledge_document_service    文档落库（只写主表，不切片）
        └── document_chunker      正文 → 切片（纯内存、零依赖）
              └── embedding_service    切片 → 向量（纯内存、零依赖）
                    └── vector_store    切片 + 向量 → 存储后端
                          （DB 后端在 vector_store_sql，只由 knowledge_rag 组装）
                              └── knowledge_import_pipeline   ← 【本模块】把四环串成一条流程

这四环**彼此不认识**——相邻环之间没有任何调用关系（见 ``services/__init__.py``
的「知识库流水线」）。本模块是**全项目唯一**把它们串成一条流程的地方，
也就是各模块文档里一直写着的那句「将来由入库脚本串起来」。

流程
----
.. code-block:: text

    await import_document(document)
      │
      ├─ [document]  find-or-create KnowledgeDocument（自然键 title + source + category）
      ├─ [chunk]     DocumentChunker.split(content, document_id=…, category=…, source=…)
      ├─ [embedding] EmbeddingService.embed(切片正文)     ← **逐片**编码，失败能定位到第几片
      ├─ [persist]   KnowledgeChunk upsert（按 document_id + chunk_index）
      └─ [vector]    VectorStore.add([VectorRecord(chunk_id=…, vector=…)])

顺序是刻意的：**先编码、后落库**。编码失败时该切片行根本不会产生，
「半截数据」最少化，重跑即从那一处继续。

五步与需求逐条对应
------------------
==================  ==========================================================
需求                 本模块的做法
==================  ==========================================================
1 创建 Document      ``_resolve_document``：按自然键找既有文档，没有才
                     ``knowledge_document_service.create_document``（校验口径复用，不重写）
2 生成 Chunk         ``DocumentChunker.split``（切片参数由调用方注入，本模块不设默认值以外的偏好）
3 生成向量           ``EmbeddingService.embed``（**逐片**，不是 ``embed_batch``：见下）
4 保存 Chunk         ``KnowledgeChunk`` upsert（``document_id`` + ``chunk_index`` 唯一确定一行）
5 写入 VectorStore   ``VectorStore.add``（``chunk_id`` 指向刚落库的切片行）
==================  ==========================================================

**为什么逐片编码而不是 ``embed_batch``**：需求要求「中途失败有明确状态」。
批量接口在默认实现里是逐条 ``embed``，一旦第 k 条抛错，调用方拿不到「是第几条」；
而本 Pipeline 的失败报告需要 ``failed_index``。真实厂商实现覆盖 ``embed_batch`` 后
（原生批量是性能来源）可以用 ``embed_batch`` 换性能，代价是丢失失败位置——
本阶段选**可定位**，规模上去后再权衡。

幂等语义（支持重复执行）
------------------------
三层幂等，重跑同一篇文档**不会产生重复数据**：

1. **文档层**：自然键 ``(title, source, category)`` 命中既有行 → **复用**，
   不新建（``reused_document=True``）。想导入同名不同内容的文档请改 ``title``。
2. **切片层**：按 ``(document_id, chunk_index)`` 定位行——存在就更新
   ``content`` / ``metadata``，不存在才新建。因此重复执行不会堆积切片。
3. **向量层**：切片已带**同模型**的向量、且**正文未变**时**跳过编码**；
   以下任一情况会重算：无向量 / ``embedding_model`` 与当前 embedder 不符
   （**换了模型，旧向量不可比**）/ 正文变了（切点漂移会让旧向量指向旧文字）。
   全部跳过时 ``status="skipped"``、``ok=True``——这是成功的空操作，不是失败。

失败语义（中途失败有明确状态）
------------------------------
**本模块不抛异常表示中途失败**，一律返回同一形状的报告（``IMPORT_RESULT_FIELDS``）：

- ``ok=False``、``status="failed"``
- ``stage``：失败发生在哪一步（``document`` / ``chunk`` / ``embedding`` /
  ``persist`` / ``vector``）
- ``failed_index``：切片序号（``chunk`` 之前为 ``None``）
- ``errors``：**稳定错误码**（``chunk_empty`` / ``embedding_failed`` /
  ``persist_failed`` / ``vector_failed``）
- ``error``：可读信息（含原始异常类名，便于定位）
- 计数保留**已经做完的部分**（``saved_chunks`` / ``embedded_chunks``），
  于是「跑到哪儿了」是可读的，重跑会接着做。

唯一**会抛异常**的是**入参契约**问题（空标题 / 空正文 / 非法 ``category``）——
那不是「中途失败」而是「还没开始」，由 ``knowledge_document_service.create_document``
抛 ``HTTPException(400)``（校验口径与消息复用同一处，不在本模块重写一遍）。
例外：**复用既有文档**时不会走 ``create_document``，此时空正文表现为
``stage="chunk"`` + ``chunk_empty`` 失败状态（切片器对空正文返回 ``[]``）。

出参契约
--------
``import_document`` 恒返回 ``IMPORT_RESULT_FIELDS`` 这 12 个键（成功 / 跳过 / 失败同一形状，
调用方无分支取值）：:

    ok                 bool   是否成功（``skipped`` 也算成功）
    status             str    ok / skipped / failed
    stage              str    document / chunk / embedding / persist / vector / done
    document_id        int    落库或复用的文档 id（失败于 document 之前为 None）
    chunk_count        int    切片器产出的切片数
    saved_chunks       int    **新建**的切片行数（复用的不算）
    embedded_chunks    int    本次写入向量的切片数
    skipped_chunks     int    已有同模型向量且正文未变、直接跳过的切片数
    reused_document    bool   文档是否复用了既有行
    failed_index       int    失败切片序号；不适用为 None
    errors             list   稳定错误码列表
    error              str    可读错误信息；成功为 ""

刻意不做
--------
- **文件上传 / PDF 解析**（需求点名不做）：本模块接收的 ``document`` 已经是
  ``{title, content, category, source}`` 形态的**纯文本**。「二进制 → 文本」是解析器的职责。
- **HTTP 路由**：加路由要先定「谁能导入知识」（管理员 / 普通用户），属权限设计。
- **不改 InterviewAgent、不改 Retriever 接口**（需求点名不做）。
- **不删过期切片**：若同一文档的内容被改短，多出来的旧切片行会**保留**
  （本模块只 upsert、不删）。清理由独立的维护动作负责——静默删数据风险更大。
- **不做全局单例**：由调用方构造并注入 ``chunker`` / ``embedder`` / ``store``。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import KNOWLEDGE_CATEGORIES, KnowledgeChunk, KnowledgeDocument
from services.document_chunker import DocumentChunker
from services.knowledge_document_service import create_document
from services.knowledge_rag import build_vector_store, default_embedder
from services.vector_store import VectorRecord

# ============================================================
# 出参契约与状态 / 阶段 / 错误码常量
# ============================================================
#: 出参键集合（顺序即 ``_new_report`` 的插入顺序；成功 / 跳过 / 失败同一形状）
IMPORT_RESULT_FIELDS = (
    "ok", "status", "stage", "document_id", "chunk_count",
    "saved_chunks", "embedded_chunks", "skipped_chunks",
    "reused_document", "failed_index", "errors", "error",
)

#: 整体状态
STATUS_OK = "ok"              # 本次真的做了写入
STATUS_SKIPPED = "skipped"    # 已完整入库，本次无事可做（**成功的空操作**）
STATUS_FAILED = "failed"      # 中途失败，见 stage / failed_index / errors

#: 阶段（``stage`` 字段的取值；``STAGE_DONE`` 表示跑完）
STAGE_DOCUMENT = "document"
STAGE_CHUNK = "chunk"
STAGE_EMBEDDING = "embedding"
STAGE_PERSIST = "persist"
STAGE_VECTOR = "vector"
STAGE_DONE = "done"

#: 稳定错误码（**进 ``errors``，不是散文**，调用方可据此分支）
ERROR_CHUNK_EMPTY = "chunk_empty"
ERROR_EMBEDDING_FAILED = "embedding_failed"
ERROR_PERSIST_FAILED = "persist_failed"
ERROR_VECTOR_FAILED = "vector_failed"


# ============================================================
# 异常（项目规范：领域基类 + 最贴近的内建异常）
# ============================================================
class KnowledgeImportError(Exception):
    """入库 Pipeline 的领域基类。"""


class ImportConfigError(KnowledgeImportError, ValueError):
    """Pipeline **构造期**的配置问题（如没给 ``db``）。

    归 ``ValueError``：调用方改代码就能解决，重试无用。
    """


# ============================================================
# 入参归一（与 ``document_chunker`` / ``knowledge_document_service`` 同款；
# 刻意重复这几行而不跨模块引用私有函数——见 ``document_chunker`` 里同样的说明）
# ============================================================
def _read_field(payload: Any, name: str) -> Any:
    """从**映射**或**对象**里取字段（缺省 ``None``）。

    于是 ``import_document`` 既能吃 ``dict``（脚本）、``SimpleNamespace``（测试），
    也能直接吃已经落库的 ``KnowledgeDocument`` ORM 行。
    """
    if isinstance(payload, Mapping):
        return payload.get(name)
    return getattr(payload, name, None)


def _label(value: Any) -> str:
    """标签类字段归一：``None`` → ``""``；其余转字符串并去首尾空白。

    必须与 ``knowledge_document_service._as_label`` **口径一致**——
    自然键查找用的就是它归一后的值，口径不一致会查不到刚写进去的文档。
    """
    if value is None:
        return ""
    return str(value).strip()


def _as_content(value: Any) -> str:
    """正文归一：``None`` → ``""``；其余转字符串但**保留首尾空白**（正文是权威来源）。"""
    if value is None:
        return ""
    return value if isinstance(value, str) else str(value)


# ============================================================
# 报告构造（纯函数，便于单测）
# ============================================================
def _new_report() -> Dict[str, Any]:
    """空白报告（键集合与顺序恒为 ``IMPORT_RESULT_FIELDS``）。"""
    return {
        "ok": True,
        "status": STATUS_OK,
        "stage": STAGE_DONE,
        "document_id": None,
        "chunk_count": 0,
        "saved_chunks": 0,
        "embedded_chunks": 0,
        "skipped_chunks": 0,
        "reused_document": False,
        "failed_index": None,
        "errors": [],
        "error": "",
    }


def _fail(
    report: Dict[str, Any],
    stage: str,
    code: str,
    exc: Any,
    *,
    index: Optional[int] = None,
) -> Dict[str, Any]:
    """把报告标成失败（就地改，返回同一个 dict）。

    ``error`` 里带上**原始异常类名**：``embedding_failed`` 这一个错误码背后可能是
    ``EmbeddingInputError``（我们写错了，改代码）或 ``EmbeddingUnavailableError``
    （上游抖动，可重试），两者的处理方式完全不同——类名就是分流依据。
    """
    report["ok"] = False
    report["status"] = STATUS_FAILED
    report["stage"] = stage
    report["errors"] = [code]
    report["failed_index"] = index
    report["error"] = f"{type(exc).__name__}: {exc}"
    return report


# ============================================================
# Pipeline
# ============================================================
class KnowledgeImportPipeline:
    """把一篇文档跑完「落库 → 切片 → 向量 → 向量库」全流程。

    三个协作者**全部可注入**，默认值取项目既定实现：

    :param db: ``AsyncSession``（依赖注入）。**必填**——文档与切片都要落库。
    :param chunker: 切片器。``None`` → ``DocumentChunker()``（默认 500 / 80）。
    :param embedder: Embedding 服务。``None`` → ``knowledge_rag.default_embedder()``
        ——刻意复用组装器的默认值，**不在这里另写一个默认**，
        否则「默认用哪个模型」就有两处说法了（该默认值按 ``EMBEDDING_*`` 配置选实现：
        未配置时是**离线占位、无语义**，配了密钥即为真实模型）。
    :param store: 向量存储后端。``None`` → ``knowledge_rag.build_vector_store(db)``
        ——同理，本模块**不认识** ``SqlAlchemyVectorStore``，换向量库只改组装器。
    :param model: 传给向量存储后端的默认模型标识（记录自带 ``model`` 时以记录为准）。
    """

    def __init__(
        self,
        db: AsyncSession,
        *,
        chunker: Optional[DocumentChunker] = None,
        embedder: Any = None,
        store: Any = None,
        model: Optional[str] = None,
    ) -> None:
        if db is None:
            raise ImportConfigError("KnowledgeImportPipeline 需要 db（文档与切片都要落库）")
        self.db = db
        self.chunker = chunker if chunker is not None else DocumentChunker()
        self.embedder = embedder if embedder is not None else default_embedder()
        self.store = store if store is not None else build_vector_store(db, model=model)
        self.model = model

    # --------------------------------------------------------
    # 主入口
    # --------------------------------------------------------
    async def import_document(self, document: Any) -> Dict[str, Any]:
        """把 ``document`` 完整导入知识库，返回报告（恒为 ``IMPORT_RESULT_FIELDS``）。

        ``document`` 需含 ``{title, content, category, source}``（``source`` 可选）；
        带 ``id``（或就是一条已落库的 ``KnowledgeDocument``）时按该文档导入。
        入参是 ``dict`` / 对象 / ORM 行都可以（见 :func:`_read_field`）。

        **不抛异常表示中途失败**（见模块文档「失败语义」）；只有入参契约问题
        （空标题 / 空正文 / 非法 category）会由 ``create_document`` 抛
        ``HTTPException(400)``——那是「还没开始」，不是「中途」。
        """
        report = _new_report()

        title = _label(_read_field(document, "title"))
        content = _as_content(_read_field(document, "content"))
        category = _label(_read_field(document, "category"))
        source = _label(_read_field(document, "source"))

        # ---- [1/5] document：找到既有文档或新建（校验口径复用 create_document）----
        document_id, reused = await self._resolve_document(
            document, title=title, content=content, category=category, source=source
        )
        report["document_id"] = document_id
        report["reused_document"] = reused

        # ---- [2/5] chunk：纯内存切片（空正文 → []，不产占位片）----
        try:
            chunks = self.chunker.split(
                content, document_id=document_id, category=category, source=source
            )
        except Exception as exc:  # noqa: BLE001 - 任何切片故障都要变成明确状态
            return _fail(report, STAGE_CHUNK, ERROR_CHUNK_EMPTY, exc)
        report["chunk_count"] = len(chunks)
        if not chunks:
            return _fail(
                report, STAGE_CHUNK, ERROR_CHUNK_EMPTY,
                ValueError("切片结果为空（正文为空 / 纯空白？）"),
            )

        # ---- 既有切片一次性读出来（含后续判断所需的列值，避免 commit 后再惰性加载）----
        existing = await self._load_chunks(document_id)

        # ---- [3~5/5] 逐片：编码 → 落库 → 写向量 ----
        for chunk in chunks:
            index = chunk["metadata"]["chunk_index"]
            text = chunk["content"]
            metadata = dict(chunk["metadata"])

            row, row_id, needs_work = self._plan_chunk(existing, index, text)

            if not needs_work:
                report["skipped_chunks"] += 1
                continue

            # [3/5] embedding
            try:
                vector = await self.embedder.embed(text)
            except Exception as exc:  # noqa: BLE001 - 变成 status=failed + failed_index
                return _fail(
                    report, STAGE_EMBEDDING, ERROR_EMBEDDING_FAILED, exc, index=index
                )

            # [4/5] persist（新建 / 更新切片行）
            try:
                if row is None:
                    row = KnowledgeChunk(
                        document_id=document_id,
                        content=text,
                        # JSON 列：整体赋值（原地改不落库，见项目约定）
                        chunk_metadata=metadata,
                    )
                    self.db.add(row)
                    # flush 才能拿到自增 id（下一步 store.add 需要 chunk_id 指向它）
                    await self.db.flush()
                    row_id = row.id
                    report["saved_chunks"] += 1
                else:
                    row.content = text
                    row.chunk_metadata = metadata
            except Exception as exc:  # noqa: BLE001
                return _fail(
                    report, STAGE_PERSIST, ERROR_PERSIST_FAILED, exc, index=index
                )

            # [5/5] vector
            try:
                await self.store.add([VectorRecord(
                    chunk_id=row_id,
                    vector=vector,
                    model=self.embedder.name,
                    document_id=document_id,
                    content=text,
                    metadata=metadata,
                )])
            except Exception as exc:  # noqa: BLE001
                return _fail(report, STAGE_VECTOR, ERROR_VECTOR_FAILED, exc, index=index)

            report["embedded_chunks"] += 1

        # ---- 收尾：全部跳过 = 已完整入库（成功的空操作）----
        if report["embedded_chunks"] == 0 and report["saved_chunks"] == 0:
            report["status"] = STATUS_SKIPPED
        return report

    # --------------------------------------------------------
    # 内部：文档解析（自然键幂等）
    # --------------------------------------------------------
    async def _resolve_document(
        self,
        document: Any,
        *,
        title: str,
        content: str,
        category: str,
        source: str,
    ) -> Tuple[int, bool]:
        """返回 ``(document_id, reused)``。

        优先级：**显式 ``id``** > **自然键 ``(title, source, category)``** > 新建。
        新建走 ``knowledge_document_service.create_document``——
        校验顺序与错误消息只有那一处定义，本模块不复制一份。
        """
        explicit_id = _read_field(document, "id")
        existing = None
        if isinstance(explicit_id, int) and not isinstance(explicit_id, bool):
            existing = await self._load_document(explicit_id)
        if existing is None:
            existing = await self._find_document(title, source, category)
        if existing is not None:
            return existing.id, True

        created = await create_document(self.db, {
            "title": title,
            "content": content,
            "category": category,
            "source": source,
        })
        return created["id"], False

    async def _load_document(self, document_id: int) -> Optional[KnowledgeDocument]:
        result = await self.db.execute(
            select(KnowledgeDocument).where(KnowledgeDocument.id == document_id)
        )
        return result.scalar_one_or_none()

    async def _find_document(
        self, title: str, source: str, category: str
    ) -> Optional[KnowledgeDocument]:
        """按自然键查既有文档。

        ``title`` 为空 / ``category`` 非法时**直接返回 None**（而不是硬查一遍）：
        那种入参必然查不到，交给 ``create_document`` 报出准确的 400 更有用。
        """
        if not title or category not in KNOWLEDGE_CATEGORIES:
            return None
        result = await self.db.execute(
            select(KnowledgeDocument)
            .where(
                KnowledgeDocument.title == title,
                KnowledgeDocument.source == source,
                KnowledgeDocument.category == category,
            )
            .order_by(KnowledgeDocument.id)
            .limit(1)
        )
        return result.scalars().first()

    # --------------------------------------------------------
    # 内部：切片读取与计划
    # --------------------------------------------------------
    async def _load_chunks(
        self, document_id: int
    ) -> Dict[int, Tuple[Any, int, bool, str, str]]:
        """把该文档的既有切片按 ``chunk_index`` 收成一张表。

        返回 ``{chunk_index: (row, row_id, has_vector, embedding_model, content)}``。

        **为什么把列值一并取出来**：后续每一步 ``store.add`` 都会 commit，
        之后再读 ORM 属性可能触发**协程外**的惰性加载（``MissingGreenlet``，
        项目踩过的坑）。这里一次性把要用的值读成普通 Python 对象，后面只碰局部变量。
        """
        rows = (
            await self.db.execute(
                select(KnowledgeChunk)
                .where(KnowledgeChunk.document_id == document_id)
                .order_by(KnowledgeChunk.id)
            )
        ).scalars().all()

        by_index: Dict[int, Tuple[Any, int, bool, str, str]] = {}
        for row in rows:
            metadata = row.chunk_metadata if isinstance(row.chunk_metadata, dict) else {}
            index = metadata.get("chunk_index")
            # bool 是 int 的子类，要显式挡掉（项目老坑）
            if isinstance(index, bool) or not isinstance(index, int):
                continue
            by_index[index] = (
                row,
                row.id,
                bool(row.embedding),          # 非空向量 = 已向量化
                row.embedding_model or "",
                row.content or "",
            )
        return by_index

    def _plan_chunk(
        self,
        existing: Dict[int, Tuple[Any, int, bool, str, str]],
        index: int,
        text: str,
    ) -> Tuple[Any, Optional[int], bool]:
        """决定这一片要不要干活：返回 ``(row_or_None, row_id_or_None, needs_work)``。

        跳过（``needs_work=False``）要求**四个条件同时成立**：

        1. 该 ``chunk_index`` 已有切片行
        2. 该行已有向量
        3. 向量由**当前模型**产生（``embedding_model == embedder.name``）
           —— 不同模型的向量不可比，换了模型必须重算
        4. 切片正文**没变** —— 正文变了说明切点漂移，旧向量指向的是旧文字

        （条件 3 / 4 是「幂等」与「正确」的分界：只按「有没有向量」跳过，
        会在换模型或改文档后留下**指向旧内容的向量**，检索出驴唇不对马嘴的结果。）
        """
        info = existing.get(index)
        if info is None:
            return None, None, True
        row, row_id, has_vector, model_name, row_content = info
        if not has_vector:
            return row, row_id, True
        if model_name != self.embedder.name:
            return row, row_id, True
        if row_content != text:
            return row, row_id, True
        return row, row_id, False


# ============================================================
# 便捷入口
# ============================================================
async def import_document(
    db: AsyncSession,
    document: Any,
    *,
    chunker: Optional[DocumentChunker] = None,
    embedder: Any = None,
    store: Any = None,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """默认组装的便捷入口：``KnowledgeImportPipeline(db, …).import_document(document)``。

    要一次导入多篇、或想复用同一个 Pipeline（同一套 chunker / embedder / store），
    请自行构造 :class:`KnowledgeImportPipeline`。
    """
    pipeline = KnowledgeImportPipeline(
        db, chunker=chunker, embedder=embedder, store=store, model=model
    )
    return await pipeline.import_document(document)


__all__ = [
    "ERROR_CHUNK_EMPTY",
    "ERROR_EMBEDDING_FAILED",
    "ERROR_PERSIST_FAILED",
    "ERROR_VECTOR_FAILED",
    "IMPORT_RESULT_FIELDS",
    "ImportConfigError",
    "KnowledgeImportError",
    "KnowledgeImportPipeline",
    "STAGE_CHUNK",
    "STAGE_DOCUMENT",
    "STAGE_DONE",
    "STAGE_EMBEDDING",
    "STAGE_PERSIST",
    "STAGE_VECTOR",
    "STATUS_FAILED",
    "STATUS_OK",
    "STATUS_SKIPPED",
    "import_document",
]
