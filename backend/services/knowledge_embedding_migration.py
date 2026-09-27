# -*- coding: utf-8 -*-
"""知识库**向量迁移**：把既有 ``KnowledgeChunk`` 的向量**就地重算**成当前 Embedding 模型。

为什么必须另起一个模块（与 ``knowledge_import_pipeline`` 的区别）
------------------------------------------------------------------
======================  ==========================================  ========================
                        ``knowledge_import_pipeline``              本模块
======================  ==========================================  ========================
切片                    重新 ``DocumentChunker.split``（**切点可能变**）  **不切片**，原样沿用既有切片
切片正文                会被 chunker 覆盖                              **逐字节不变**（硬约束）
切片行数                可能增删（内容变短时留旧行）                     **不变**（只 UPDATE 三列）
chunk_index / metadata  由 chunker 重算                                **不变**
适用场景                导入新文档 / 全量重跑                           **换 Embedding 模型后重算旧向量**
======================  ==========================================  ========================

「换模型」这一场景下两者的差别是**正确性**问题而不只是效率问题：旧知识库的切片
是按旧切点切好的，用 chunker 重跑会改变 ``chunk_index``，于是
「切片 7」在迁移前后**不是同一段文字**，而 ``knowledge_chunk`` 上的
``embedding`` 是**按行**存的——错位之后向量指向的文字全变了。
因此迁移必须**只碰向量三列**。

流程（严格对齐需求）
--------------------
.. code-block:: text

    [1] 读取已有 chunk      SELECT knowledge_chunk（可选 document_id / chunk_ids / limit）
    [2] 判定是否需要迁移     embedding 为空 / embedding_model 不符 / embedding_dim 不符
    [3] 调用 EmbeddingService  embed(content)  ← **文档模式**（讯飞 = domain: para）
    [4] 生成新向量           List[float]
    [5] 更新数据库           store.add([VectorRecord(chunk_id=…, vector=…, model=…)])
                            ⇒ 后端只写 embedding / embedding_model / embedding_dim 三列

写回为什么走 ``VectorStore`` 接口，而不是直接 ``UPDATE`` ORM
------------------------------------------------------------
1. **语义完全吻合**：``store.add`` 对「``chunk_id`` 有值」的记录，契约就是
   「给既有切片补/更新向量；``content`` / ``metadata`` / ``document_id`` 一律不动」
   （见 ``vector_store_sql.add_returning_ids``）——正是本模块要的语义，
   自己再写一遍 UPDATE 只会多出一份会漂移的实现。
2. **ANN 后端的派生索引必须同步**：``VECTOR_STORE=chroma`` 时 ``store.add`` 会
   先落权威行、再镜像进 HNSW 索引。若绕过接口直接写 MySQL，索引会停留在旧模型上，
   而检索器按 ``embedding_model == embedder.name`` 过滤 ⇒ 表现为「一条都检索不到」。
3. **不改接口**：本模块**只消费** ``VectorStore`` / ``Retriever`` 的既有接口，
   一行都没动它们（守卫断言见 ``tests/test_knowledge_embedding_migration.py``）。

幂等
----
跳过（不重算）要求**三个条件同时成立**：``embedding`` 非空 **且**
``embedding_model == target_model`` **且** ``embedding_dim == target_dim``。
于是重复执行第二次是**成功的空操作**（``status="skipped"``），
中途失败后直接重跑也能从断点继续（已迁移的自动跳过）。

.. note::
   ``target_dim`` 取 ``embedder.dimension``；当实现**不声明维度**（如
   ``openai-compatible`` 的默认 ``dimension = 0``）时**跳过维度判据**，
   只按模型标识判断——否则会把「未知维度」误判成「维度不符」而反复重算。

失败恢复
--------
逐片编码、**逐批写回**（一批一次 commit），因此失败面被限制在一批之内：

- **单片编码失败**（``stage="embed"``）：记进 ``failures``，**同批其它片照常迁移**。
- **整批写回失败**（``stage="persist"``）：先 ``rollback`` 让会话可继续，
  再把该批**全部**切片记进 ``failures``（这一批一条都没落库），继续下一批。
- **不抛异常表示中途失败**（与 ``knowledge_import_pipeline`` 同一口径）：
  一切失败都进报告。唯一**会抛**的是**入参契约**问题（``MigrationConfigError``）——
  那不是「跑到一半失败」而是「根本没法开始」。
- 报告里保留**完整**失败明细（``failures`` 的 ``chunk_id`` 列表），
  脚本据此可用 ``--retry-failed`` **精确重跑**失败的那些片，不重跑已成功的。

出参契约（``MIGRATION_RESULT_FIELDS``，16 键恒定，成功 / 跳过 / 部分失败 / 全失败同一形状）
--------------------------------------------------------------------------------------------
===================  ====  ==========================================================
``ok``               bool  是否**一条都没失败**（``partial`` 时为 ``False``）
``status``           str   ``ok`` / ``skipped`` / ``partial`` / ``failed``
``stage``            str   ``scan`` / ``embed`` / ``persist`` / ``done``（最后一次失败所在阶段）
``scanned_chunks``   int   扫描到的切片总数
``pending_chunks``   int   判定为「需迁移」的切片数
``migrated_chunks``  int   真正写入新向量的切片数
``skipped_chunks``   int   已同模型同维度、直接跳过的切片数
``failed_chunks``    int   失败切片数（= ``len(failures)``）
``batches``          int   实际执行的写回批次数
``target_model``     str   本次目标模型标识（``embedder.name``）
``target_dim``       int   本次目标维度（实现未声明维度时为 ``0``，此时不校验维度）
``dry_run``          bool  是否只扫描、不编码、不写库
``stopped_early``    bool  是否因 ``max_failures`` 提前停止（剩余片未尝试）
``errors``           list  **批次级**错误（``store.add`` 抛出的可读信息）
``failures``         list  **切片级**失败明细 ``[{chunk_id, document_id, stage, error}]``
``error``            str   第一条失败的可读信息；无失败为 ``""``
===================  ====  ==========================================================

刻意不做
--------
- **不改切片**：不切片、不新建行、不删行、不动 ``content`` / ``metadata`` / ``document_id``。
- **不改接口**：``VectorStore`` / ``KnowledgeRetriever`` 一行未改，只做消费方。
- **不删旧向量**：迁移是**覆盖写**；旧模型的向量被新向量替换（同一行，不是两行）。
- **不做全局单例**：``embedder`` / ``store`` 由调用方注入，默认值取项目既定实现
  （``knowledge_rag.default_embedder`` / ``knowledge_rag.build_vector_store``）。
- **不自动触发**：本模块不会被任何链路自动调用，只由迁移脚本 / 管理动作显式调用。
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, List, Optional, Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import KnowledgeChunk
from services.knowledge_rag import build_vector_store, default_embedder
from services.vector_store import VectorRecord

# ============================================================
# 出参契约与状态 / 阶段常量
# ============================================================
#: 出参键集合（顺序即 ``_new_report`` 的插入顺序；四种结局同一形状）
MIGRATION_RESULT_FIELDS = (
    "ok", "status", "stage",
    "scanned_chunks", "pending_chunks", "migrated_chunks",
    "skipped_chunks", "failed_chunks", "batches",
    "target_model", "target_dim", "dry_run", "stopped_early",
    "errors", "failures", "error",
)

#: 整体状态
STATUS_OK = "ok"              # 有待迁移的片，且全部迁移成功
STATUS_SKIPPED = "skipped"    # 没有待迁移的片（**成功的空操作**，重复执行即此态）
STATUS_PARTIAL = "partial"    # 部分成功、部分失败
STATUS_FAILED = "failed"      # 有待迁移的片，但一条都没成功

#: 阶段（``stage`` 字段取值；``STAGE_DONE`` 表示无失败跑完）
STAGE_SCAN = "scan"
STAGE_EMBED = "embed"
STAGE_PERSIST = "persist"
STAGE_DONE = "done"

#: 默认批大小。**取值理由**：一批 = 一次 commit = 一个失败面。
#: 太小 → 提交次数多、断点恢复粒度细但慢；太大 → 一批全挂时白跑很多次编码。
DEFAULT_BATCH_SIZE = 8

#: 默认「逐条之间的间隔秒数」。讯飞 Embedding 上游有**限流**
#: （实测连续快速调用会返回 ``code=11202 licc failed``），间隔 3s 可稳定通过。
#: 默认 0 保持「不引入额外等待」——限流场景由调用方显式给 ``--pace``。
DEFAULT_PACE_SECONDS = 0.0

#: 默认重试次数（0 = 不重试）。只对**单条编码**生效，整批写回失败不自动重试
#: （那多半是库/配置问题，重试只会重复失败）。
DEFAULT_RETRIES = 0

#: 默认重试间隔（秒）。
DEFAULT_RETRY_DELAY = 1.0


# ============================================================
# 异常（项目规范：领域基类 + 最贴近的内建异常）
# ============================================================
class EmbeddingMigrationError(Exception):
    """向量迁移的领域基类。"""


class MigrationConfigError(EmbeddingMigrationError, ValueError):
    """迁移**构造期**的配置问题（没给 db / embedder 无 ``embed`` / 参数非法）。

    归 ``ValueError``：调用方改代码或改参数就能解决，重试无用。
    """


# ============================================================
# 小工具（纯函数，便于单测）
# ============================================================
def _require_positive_int(value: Any, name: str, *, allow_zero: bool = False) -> int:
    """正整数校验（**显式拒 ``bool``**——``bool`` 是 ``int`` 子类，项目老坑）。"""
    if isinstance(value, bool) or not isinstance(value, int):
        raise MigrationConfigError(f"{name} 必须是整数，收到 {type(value).__name__}")
    if allow_zero:
        if value < 0:
            raise MigrationConfigError(f"{name} 必须 >= 0，收到 {value}")
    elif value < 1:
        raise MigrationConfigError(f"{name} 必须 >= 1，收到 {value}")
    return value


def _require_non_negative_float(value: Any, name: str) -> float:
    """非负有限浮点校验（``NaN`` / ``inf`` 一律拒）。"""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MigrationConfigError(f"{name} 必须是数值，收到 {type(value).__name__}")
    number = float(value)
    if number != number or number in (float("inf"), float("-inf")):
        raise MigrationConfigError(f"{name} 必须是有限数值，收到 {value!r}")
    if number < 0:
        raise MigrationConfigError(f"{name} 必须 >= 0，收到 {number}")
    return number


def _new_report(
    *, target_model: str, target_dim: int, dry_run: bool
) -> Dict[str, Any]:
    """空白报告（键集合与顺序恒为 ``MIGRATION_RESULT_FIELDS``）。"""
    return {
        "ok": True,
        "status": STATUS_SKIPPED,
        "stage": STAGE_DONE,
        "scanned_chunks": 0,
        "pending_chunks": 0,
        "migrated_chunks": 0,
        "skipped_chunks": 0,
        "failed_chunks": 0,
        "batches": 0,
        "target_model": target_model,
        "target_dim": target_dim,
        "dry_run": dry_run,
        "stopped_early": False,
        "errors": [],
        "failures": [],
        "error": "",
    }


def needs_migration(
    *,
    has_vector: bool,
    model: str,
    dim: Optional[int],
    target_model: str,
    target_dim: int,
) -> bool:
    """判定某一片是否需要重算（**纯函数**，幂等判据的唯一实现）。

    - 没有向量 ⇒ 需要
    - 向量由**别的模型**产生 ⇒ 需要（不同模型向量不可比）
    - 向量维度与目标不符 ⇒ 需要（同一模型也可能改维度，或行被人改坏）
    - ``target_dim <= 0`` ⇒ **跳过维度判据**（实现未声明维度，无从比较）

    三个条件都满足才算「已有可用向量」，此时返回 ``False``（跳过 = 幂等）。
    """
    if not has_vector:
        return True
    if model != target_model:
        return True
    if target_dim > 0 and dim != target_dim:
        return True
    return False


# ============================================================
# 迁移器
# ============================================================
class EmbeddingMigration:
    """把既有切片的向量就地重算成 ``embedder`` 产生的向量。

    :param db: ``AsyncSession``（依赖注入）。**必填**——切片要读、向量要写。
    :param embedder: Embedding 服务。``None`` → ``knowledge_rag.default_embedder()``
        （**文档模式**：``role`` 取默认的 ``document``）。要求提供 ``async embed(text)``
        与非空 ``name``；``dimension`` 可选（``<= 0`` 表示不声明，此时不校验维度）。
    :param store: 向量存储后端。``None`` → ``knowledge_rag.build_vector_store(db)``
        ——本模块**不认识**任何具体后端，换向量库只改组装器。
    :param target_model: 目标模型标识。``None`` → ``embedder.name``。
        显式传入的用途：把库里的 ``embedding_model`` 钉成约定值（如 ``xinghuo-embedding``）。
    :param target_dim: 目标维度。``None`` → ``embedder.dimension``（缺失记 0）。
    :param batch_size: 写回批大小（一批 = 一次 commit = 一个失败面）。
    :param pace_seconds: **每条编码之间**的间隔（上游限流时用）。
    :param retries: 单条编码的重试次数（不含首次）。
    :param retry_delay: 重试间隔（秒）。
    :param max_failures: 失败片数达到该值就**停止后续批次**（``None`` = 不限制）。
        用途：上游整体不可用时避免把整库都跑成失败。
    :param dry_run: 只扫描与判定，**不编码、不写库**。
    :param progress: 可选回调 ``fn(done, total)``，每完成一批调用一次（日志用）。
    """

    def __init__(
        self,
        db: AsyncSession,
        *,
        embedder: Any = None,
        store: Any = None,
        target_model: Optional[str] = None,
        target_dim: Optional[int] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        pace_seconds: float = DEFAULT_PACE_SECONDS,
        retries: int = DEFAULT_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY,
        max_failures: Optional[int] = None,
        dry_run: bool = False,
        progress: Optional[Callable[[int, int], None]] = None,
    ) -> None:
        if db is None:
            raise MigrationConfigError(
                "EmbeddingMigration 需要 db（切片要读、向量要写）"
            )
        self.db = db

        if embedder is None:
            embedder = default_embedder()
        embed = getattr(embedder, "embed", None)
        if embed is None or not callable(embed):
            raise MigrationConfigError("embedder 必须提供 async embed(text)")
        name = str(getattr(embedder, "name", "") or "").strip()
        if not name:
            raise MigrationConfigError(
                "embedder 必须提供非空 name（它会被写进 embedding_model，"
                "是「换模型后据此重算」的唯一依据）"
            )
        self.embedder = embedder

        resolved_model = str(target_model).strip() if target_model is not None else name
        if not resolved_model:
            raise MigrationConfigError("target_model 不能是空字符串")

        if target_dim is None:
            raw_dim = getattr(embedder, "dimension", 0)
            # 显式拒 bool：``True`` 会被当成 1 维，静默写坏 embedding_dim
            resolved_dim = 0 if isinstance(raw_dim, bool) or not isinstance(raw_dim, int) else max(raw_dim, 0)
        else:
            resolved_dim = _require_positive_int(target_dim, "target_dim", allow_zero=True)

        self.target_model = resolved_model
        self.target_dim = resolved_dim
        self.store = store if store is not None else build_vector_store(db)
        self.batch_size = _require_positive_int(batch_size, "batch_size")
        self.pace_seconds = _require_non_negative_float(pace_seconds, "pace_seconds")
        self.retries = _require_positive_int(retries, "retries", allow_zero=True)
        self.retry_delay = _require_non_negative_float(retry_delay, "retry_delay")
        self.max_failures = (
            None
            if max_failures is None
            else _require_positive_int(max_failures, "max_failures")
        )
        self.dry_run = bool(dry_run)
        self.progress = progress

    # --------------------------------------------------------
    # 主入口
    # --------------------------------------------------------
    async def run(
        self,
        *,
        document_id: Optional[int] = None,
        chunk_ids: Optional[Sequence[int]] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """执行迁移，返回 ``MIGRATION_RESULT_FIELDS`` 恒定报告。

        :param document_id: 只迁移该文档的切片（``None`` = 全部文档）。
        :param chunk_ids: 只迁移这些切片 id（``None`` = 不按 id 过滤）。
            用途：拿上一次报告的 ``failures[].chunk_id`` **精确重跑失败片**。
        :param limit: 最多扫描多少片（``None`` = 不限）。按主键升序取前 N 片，
            便于分批推进与先小样本试跑。
        """
        report = _new_report(
            target_model=self.target_model,
            target_dim=self.target_dim,
            dry_run=self.dry_run,
        )

        items = await self._scan(document_id=document_id, chunk_ids=chunk_ids, limit=limit)
        report["scanned_chunks"] = len(items)

        pending: List[Dict[str, Any]] = []
        for item in items:
            if needs_migration(
                has_vector=item["has_vector"],
                model=item["model"],
                dim=item["dim"],
                target_model=self.target_model,
                target_dim=self.target_dim,
            ):
                pending.append(item)
            else:
                report["skipped_chunks"] += 1
        report["pending_chunks"] = len(pending)

        # dry_run：只回答「有多少要迁移」，一条都不编码（因此不花配额、不写库）
        if self.dry_run or not pending:
            return self._finalize(report)

        total_batches = (len(pending) + self.batch_size - 1) // self.batch_size
        _ = total_batches  # 供 progress 回调对照「共几批」；报告里用 batches 表达实际跑了几批
        for offset in range(0, len(pending), self.batch_size):
            batch = pending[offset:offset + self.batch_size]
            report["batches"] += 1

            records: List[VectorRecord] = []
            for item in batch:
                try:
                    vector = await self._embed_one(item["content"])
                except Exception as exc:  # noqa: BLE001 - 变成可定位的失败明细
                    self._record_failure(report, item, STAGE_EMBED, exc)
                    continue
                finally:
                    if self.pace_seconds:
                        await asyncio.sleep(self.pace_seconds)
                records.append(VectorRecord(
                    # ``chunk_id`` 有值 ⇒ 后端**只更新向量三列**，切片正文/元信息不动
                    chunk_id=item["chunk_id"],
                    vector=vector,
                    model=self.target_model,
                ))

            if records:
                try:
                    await self.store.add(records)
                except Exception as exc:  # noqa: BLE001 - 变成批次级错误 + 该批全失败
                    await self._safe_rollback(report)
                    report["errors"].append(f"{type(exc).__name__}: {exc}")
                    for record in records:
                        self._record_failure(
                            report,
                            {"chunk_id": record.chunk_id, "document_id": None},
                            STAGE_PERSIST,
                            exc,
                        )
                else:
                    report["migrated_chunks"] += len(records)

            if self.progress is not None:
                self.progress(min(offset + self.batch_size, len(pending)), len(pending))

            if (
                self.max_failures is not None
                and report["failed_chunks"] >= self.max_failures
            ):
                report["stopped_early"] = True
                break

        return self._finalize(report)

    # --------------------------------------------------------
    # 内部：扫描
    # --------------------------------------------------------
    async def _scan(
        self,
        *,
        document_id: Optional[int],
        chunk_ids: Optional[Sequence[int]],
        limit: Optional[int],
    ) -> List[Dict[str, Any]]:
        """把待判定的切片读成普通 dict（**不持有 ORM 对象**）。

        为什么立刻转成 dict：写回时 ``store.add`` 会 ``commit``，之后再读 ORM 属性
        可能触发**协程外**惰性加载（``MissingGreenlet``，项目踩过的坑）。
        这里一次性把要用的值读成普通 Python 对象，后面只碰局部变量。
        """
        if document_id is not None:
            _require_positive_int(document_id, "document_id")
        if limit is not None:
            _require_positive_int(limit, "limit")

        stmt = select(KnowledgeChunk)
        if document_id is not None:
            stmt = stmt.where(KnowledgeChunk.document_id == document_id)
        if chunk_ids is not None:
            wanted = self._normalize_ids(chunk_ids)
            if not wanted:
                return []
            stmt = stmt.where(KnowledgeChunk.id.in_(wanted))
        # 主键升序 = 全项目统一的「候选顺序」，也让 limit 的语义稳定（前 N 片）
        stmt = stmt.order_by(KnowledgeChunk.id)
        if limit is not None:
            stmt = stmt.limit(limit)

        rows = (await self.db.execute(stmt)).scalars().all()
        items: List[Dict[str, Any]] = []
        for row in rows:
            items.append({
                "chunk_id": row.id,
                "document_id": row.document_id,
                "content": row.content or "",
                "model": row.embedding_model or "",
                "dim": row.embedding_dim,
                "has_vector": bool(row.embedding),
            })
        return items

    def _normalize_ids(self, chunk_ids: Sequence[int]) -> List[int]:
        """归一 id 列表（去重保序；拒单值 / 拒 ``bool``）。"""
        if isinstance(chunk_ids, (str, bytes, bytearray)) or isinstance(chunk_ids, bool):
            raise MigrationConfigError(
                f"chunk_ids 需要一组 id，收到单个 {type(chunk_ids).__name__}；请包成 [id]"
            )
        try:
            raw = list(chunk_ids)
        except TypeError as exc:
            raise MigrationConfigError(f"chunk_ids 必须是可迭代的一组 id：{exc}") from exc
        seen: set = set()
        result: List[int] = []
        for value in raw:
            if isinstance(value, bool) or not isinstance(value, int):
                raise MigrationConfigError(
                    f"chunk_ids 里必须都是整数，收到 {type(value).__name__}"
                )
            if value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result

    # --------------------------------------------------------
    # 内部：编码（带重试）
    # --------------------------------------------------------
    async def _embed_one(self, text: str) -> List[float]:
        """编码单条文本，按 ``retries`` / ``retry_delay`` 重试。

        为什么在**这里**重试而不是靠 ``embed_batch``：迁移要的是
        「哪一条失败」可定位（与入库 Pipeline 同一取舍），
        批量接口一旦第 k 条抛错就丢掉了位置信息。
        """
        attempt = 0
        while True:
            try:
                return await self.embedder.embed(text)
            except Exception:  # noqa: BLE001 - 重试耗尽后原样抛出，由调用方记明细
                if attempt >= self.retries:
                    raise
                attempt += 1
                if self.retry_delay:
                    await asyncio.sleep(self.retry_delay)

    # --------------------------------------------------------
    # 内部：失败记账与收尾
    # --------------------------------------------------------
    def _record_failure(
        self,
        report: Dict[str, Any],
        item: Dict[str, Any],
        stage: str,
        exc: Any,
    ) -> None:
        """记一条失败明细（``stage`` 取**最后一次**失败所在阶段）。"""
        report["failed_chunks"] += 1
        report["stage"] = stage
        report["failures"].append({
            "chunk_id": item.get("chunk_id"),
            "document_id": item.get("document_id"),
            "stage": stage,
            "error": f"{type(exc).__name__}: {exc}",
        })

    async def _safe_rollback(self, report: Dict[str, Any]) -> None:
        """批次写回失败后回滚会话（否则后续批次全部会以同样的方式失败）。"""
        try:
            await self.db.rollback()
        except Exception as exc:  # noqa: BLE001 - 回滚本身失败也要如实上报
            report["errors"].append(f"rollback 失败：{type(exc).__name__}: {exc}")

    def _finalize(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """按计数定状态（**唯一的定状态处**，四种结局只在这里产生）。"""
        if report["failed_chunks"] == 0:
            report["ok"] = True
            report["stage"] = STAGE_DONE
            report["status"] = (
                STATUS_SKIPPED if report["pending_chunks"] == 0 else STATUS_OK
            )
        else:
            report["ok"] = False
            report["status"] = (
                STATUS_PARTIAL if report["migrated_chunks"] > 0 else STATUS_FAILED
            )
            report["error"] = report["failures"][0]["error"]
        return report


# ============================================================
# 便捷入口
# ============================================================
async def migrate_embeddings(
    db: AsyncSession,
    *,
    embedder: Any = None,
    store: Any = None,
    document_id: Optional[int] = None,
    chunk_ids: Optional[Sequence[int]] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    **kwargs: Any,
) -> Dict[str, Any]:
    """默认组装的便捷入口：``EmbeddingMigration(db, …).run(…)``。

    ``**kwargs`` 原样透传给 :class:`EmbeddingMigration` 的构造参数
    （``batch_size`` / ``pace_seconds`` / ``retries`` / ``retry_delay`` /
    ``max_failures`` / ``target_model`` / ``target_dim`` / ``progress``）。

    要一次迁移多批、或复用同一个迁移器（同一套 embedder / store），
    请自行构造 :class:`EmbeddingMigration`。
    """
    migration = EmbeddingMigration(
        db, embedder=embedder, store=store, dry_run=dry_run, **kwargs
    )
    return await migration.run(
        document_id=document_id, chunk_ids=chunk_ids, limit=limit
    )


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_PACE_SECONDS",
    "DEFAULT_RETRIES",
    "DEFAULT_RETRY_DELAY",
    "EmbeddingMigration",
    "EmbeddingMigrationError",
    "MIGRATION_RESULT_FIELDS",
    "MigrationConfigError",
    "STAGE_DONE",
    "STAGE_EMBED",
    "STAGE_PERSIST",
    "STAGE_SCAN",
    "STATUS_FAILED",
    "STATUS_OK",
    "STATUS_PARTIAL",
    "STATUS_SKIPPED",
    "migrate_embeddings",
    "needs_migration",
]
