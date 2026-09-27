# -*- coding: utf-8 -*-
"""AI 面试知识库 · 向量存储层（**接口 + 值对象 + 内存实现**，不绑定任何数据库）。

分层定位
--------
::

    embedding_service          text → vector
        └── vector_store       【本模块】接口 VectorStore（add / search）+ 内存实现
              └── vector_store_sql   基于既有 MySQL 表 knowledge_chunk 的**扩展字段**实现
                    └── (将来) 真实向量库（chromadb / milvus / pgvector …）

本模块只做两件事：**存进去**（``add``）与**按向量找相似的**（``search``）。
刻意**不实现**：

- 任何具体数据库 / 向量库的读写（那是 ``vector_store_sql`` 与将来各后端的职责）
- 文本 → 向量（那是 ``embedding_service`` 的职责；本模块只认**已经算好的向量**）
- 切片（``document_chunker``）、Retriever（``knowledge_retriever``）
- 落库之外的任何编排；**不接 InterviewAgent / InterviewCore / InterviewService**

**零第三方依赖**：本模块只 import 标准库（``abc`` / ``collections`` / ``dataclasses`` /
``typing``），因此接口与相似度算法**可脱离数据库单测**——这也是「不绑定具体数据库」
在结构上的体现：换后端只是换一个 ``VectorStore`` 子类，接口、值对象、排序口径都不动。

两个核心概念
------------
:class:`VectorRecord`（写入单位）
    一条「切片 + 向量」。``chunk_id`` 为空表示**新建切片**；非空表示**给既有切片补向量**
    （正好对应 ``models/knowledge.py`` 里 ``embedding IS NULL`` = 待向量化的口径）。

:class:`VectorMatch`（检索结果）
    命中的切片 + 相似度 ``score``（**余弦相似度**，取值 ``[-1, 1]``，越大越像）。

接口契约
--------
``add(records) -> int``
    写入一组记录，返回**实际写入条数**。语义是 **upsert**：

    - ``chunk_id`` 为空 → **新建**（内存实现自动分配自增 id）
    - ``chunk_id`` 已存在 → **只更新向量相关字段**（``vector`` / ``model``），
      **不动** ``content`` / ``metadata`` / ``document_id``。这条规则很关键：
      向量层的职责是「给切片写向量」，不是「改切片」。批量补向量的任务通常只传
      ``chunk_id + vector``，若整体覆盖就会把切片正文清空。
    - ``chunk_id`` 有值但**不存在**：内存实现视为新建（它没有「切片」概念，
      id 只是一个键）；SQL 实现**报错**（``chunk_id`` 指向真实切片行，
      不存在说明调用方拿错了 id）。这一点两个实现刻意不同——存储模型不同，
      但对**接口使用者**而言，``search`` 的语义完全一致（见 ``tests`` 的换后端测试）。

    同一次 ``add`` 里重复的 ``chunk_id`` 只保留最后一条。
    **不要求同一批次维度一致**——``embedding_dim`` 是逐行记录的，混用模型是允许的；
    但查询时只有与查询向量同维的切片才会参与比较（见下）。

``search(query_vector, *, top_k, model, document_id, category, min_score) -> List[VectorMatch]``
    按余弦相似度**降序**返回至多 ``top_k`` 条。同分时按**候选顺序**稳定排序，
    保证「同输入同输出」（候选顺序由各实现固定：内存实现按写入顺序，
    SQL 实现按主键升序）。

``count() -> int``
    **已向量化**的切片数（不是切片总数）——即「存了多少条向量」。

维度不符怎么办（重要）
----------------------
**不同模型的向量不可比**（维度可能不同、语义空间也不同）。因此 ``search`` 会
**跳过**与查询向量维度不同的候选；但若「候选里明明有向量、却一条维度都对不上」，
说明多半是**拿着另一个模型的向量在查询**，此时抛
:class:`VectorStoreDimensionError` ——**而不是安静地返回空列表**。
空列表只用于「确实没有候选」这一种情况。这个区分能省掉大量"为什么搜不到"的排查。

.. warning::
   :class:`InMemoryVectorStore` 是**进程内**实现：数据不落库、进程退出即消失，
   只适合单测与本地打通链路。生产请用 ``vector_store_sql`` 或真实向量库。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Optional

#: ``search`` 的默认返回条数
DEFAULT_TOP_K = 5

#: ``VectorRecord.to_dict()`` 的键（顺序稳定，供上游适配器归一）
VECTOR_RECORD_FIELDS = (
    "chunk_id", "document_id", "content", "metadata", "model", "vector", "dimension",
)

#: ``VectorMatch.to_dict()`` 的键
VECTOR_MATCH_FIELDS = (
    "chunk_id", "document_id", "content", "metadata", "model", "score",
)


# ============================================================
# 异常（按项目规范：领域基类 + 最贴近的内建异常）
# ============================================================
class VectorStoreError(Exception):
    """向量存储的领域基类。"""


class VectorStoreInputError(VectorStoreError, ValueError):
    """**入参**问题：向量为空 / 含非数值元素 / 记录类型不对 / 缺少必填字段。

    归为 ``ValueError``：调用方改代码就能解决。
    """


class VectorStoreDimensionError(VectorStoreError, ValueError):
    """**维度**问题：查询向量与所有候选都对不上（多半是换了模型）。

    刻意与 :class:`VectorStoreInputError` 分开：前者是「单条入参写错了」，
    后者是「**两边不匹配**」，排查方向完全不同（一个是看调用代码，一个是看模型）。
    """


# ============================================================
# 一、校验与相似度（纯函数，两个实现共用）
# ============================================================
def require_vector(value: Any, *, what: str = "vector") -> List[float]:
    """校验并归一向量：必须是可迭代的数值序列且非空。

    只要求「可迭代」——真实实现可能从 JSON / numpy / tuple 里取到向量。
    但**显式拒绝** ``bool`` / ``str`` 元素（``isinstance(True, int)`` 为真、
    ``float("0.1")`` 会成功，这两类混进来一定是上游写错了）。

    .. note::
       与 ``embedding_service._require_vector`` 形状相同但**刻意各写一份**：
       本模块要保持「零依赖、不 import 任何业务模块」，且这里的失败是**入参**错误
       （``VectorStoreInputError``）而不是「模型返回了脏数据」。
    """
    if isinstance(value, (str, bytes, bytearray)):
        raise VectorStoreInputError(f"{what} 必须是数值序列，收到字符串")
    try:
        items = list(value)
    except TypeError as exc:
        raise VectorStoreInputError(
            f"{what} 必须是可迭代的数值序列，收到 {type(value).__name__}"
        ) from exc
    if not items:
        raise VectorStoreInputError(f"{what} 不能是空向量")
    out: List[float] = []
    for item in items:
        if isinstance(item, (bool, str, bytes, bytearray)):
            raise VectorStoreInputError(
                f"{what} 含非数值元素：{item!r}（{type(item).__name__}）"
            )
        try:
            out.append(float(item))
        except (TypeError, ValueError) as exc:
            raise VectorStoreInputError(
                f"{what} 含非数值元素：{item!r}（{type(item).__name__}）"
            ) from exc
    return out


def dot_product(a: Sequence[float], b: Sequence[float]) -> float:
    """点积（要求等长）。"""
    if len(a) != len(b):
        raise VectorStoreDimensionError(f"点积要求等长：{len(a)} vs {len(b)}")
    return sum(x * y for x, y in zip(a, b))


def l2_norm(a: Sequence[float]) -> float:
    """L2 模长。"""
    return sum(x * x for x in a) ** 0.5


def cosine_similarity(a: Sequence[float], b: Sequence[float]) -> float:
    """余弦相似度，取值 ``[-1, 1]``。

    - 等长才可比，否则抛 :class:`VectorStoreDimensionError`（**不静默截断**）
    - 任一边是零向量（模长为 0）时返回 ``0.0``（方向未定义，不做除零）
    """
    if len(a) != len(b):
        raise VectorStoreDimensionError(
            f"余弦相似度要求等长：{len(a)} vs {len(b)}"
        )
    na, nb = l2_norm(a), l2_norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return dot_product(a, b) / (na * nb)


def require_top_k(top_k: Any) -> int:
    """校验 ``top_k``：必须是正整数。

    这里**不**像分页参数那样「越界自动收敛」——``top_k`` 是检索语义参数，
    ``top_k=0`` 只可能是调用方写错了，静默改成 1 会让 bug 藏起来。
    """
    if isinstance(top_k, bool) or not isinstance(top_k, int) or top_k <= 0:
        raise VectorStoreInputError(f"top_k 必须是正整数，当前 {top_k!r}")
    return top_k


def require_min_score(min_score: Any) -> Optional[float]:
    """校验 ``min_score``（可选）：必须是数值。"""
    if min_score is None:
        return None
    if isinstance(min_score, bool) or not isinstance(min_score, (int, float)):
        raise VectorStoreInputError(f"min_score 必须是数值或 None，当前 {min_score!r}")
    return float(min_score)


# ============================================================
# 二、值对象
# ============================================================
@dataclass(frozen=True)
class VectorRecord:
    """一条「切片 + 向量」（写入单位）。

    - ``chunk_id=None``：**新建**切片（SQL 实现要求同时给 ``document_id`` 与 ``content``）
    - ``chunk_id`` 有值：**给既有切片补/更新向量**（对应 ``embedding IS NULL`` 待处理口径）。
      此时 ``content`` / ``metadata`` / ``document_id`` **会被忽略**——它们属于切片，
      不属于向量；补向量不该改写切片正文（见 :meth:`VectorStore.add` 的契约）
    - ``model``：编码该向量的模型标识，会落进 ``embedding_model``（换模型后据此重算）
    - ``metadata``：切片元信息（``category`` / ``topic`` …），检索时可按它过滤

    ``__post_init__`` 会**校验并归一** ``vector``、拷贝 ``metadata``，
    因此「造出来的对象一定是合法的」——不用等 ``add`` 才发现写错。
    """

    vector: List[float]
    content: str = ""
    document_id: Optional[int] = None
    chunk_id: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    model: str = ""

    def __post_init__(self) -> None:
        # frozen dataclass 里赋值要用 object.__setattr__
        object.__setattr__(self, "vector", require_vector(self.vector, what="record.vector"))
        object.__setattr__(self, "metadata", dict(self.metadata or {}))
        object.__setattr__(self, "content", "" if self.content is None else str(self.content))
        object.__setattr__(self, "model", "" if self.model is None else str(self.model))

    @property
    def dimension(self) -> int:
        """向量维度。"""
        return len(self.vector)

    def to_dict(self) -> Dict[str, Any]:
        """普通 dict（容器做浅拷贝，改它不影响本对象）。"""
        return {
            "chunk_id": self.chunk_id,
            "document_id": self.document_id,
            "content": self.content,
            "metadata": dict(self.metadata),
            "model": self.model,
            "vector": list(self.vector),
            "dimension": self.dimension,
        }


@dataclass(frozen=True)
class VectorMatch:
    """一条检索命中：切片信息 + 相似度。"""

    chunk_id: Optional[int]
    document_id: Optional[int]
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    model: str = ""
    score: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", dict(self.metadata or {}))
        object.__setattr__(self, "score", float(self.score))

    def to_dict(self) -> Dict[str, Any]:
        """普通 dict（**不含向量本体**——检索结果不需要把上千个浮点带回上层）。"""
        return {
            "chunk_id": self.chunk_id,
            "document_id": self.document_id,
            "content": self.content,
            "metadata": dict(self.metadata),
            "model": self.model,
            "score": self.score,
        }


# ============================================================
# 三、排序口径（两个实现共用，保证「换后端不换语义」）
# ============================================================
def select_matches(
    records: Sequence[VectorRecord],
    query_vector: Any,
    *,
    top_k: int = DEFAULT_TOP_K,
    min_score: Optional[float] = None,
    store_name: str = "vector_store",
) -> List[VectorMatch]:
    """在候选里按余弦相似度排序，返回前 ``top_k`` 条。

    规则：

    1. **跳过维度不符的候选**（不同模型的向量不可比）
    2. 降序排列；**同分按候选顺序**稳定排序（各实现都固定了候选顺序 → 同输入同输出）
    3. ``min_score`` 过滤低于阈值的结果
    4. 若「候选非空、但一条维度都对不上」→ 抛
       :class:`VectorStoreDimensionError`（而不是安静返回空，见模块文档）
    """
    query = require_vector(query_vector, what="query_vector")
    top_k = require_top_k(top_k)
    min_score = require_min_score(min_score)

    dimension = len(query)
    scored: List[Any] = []
    skipped = 0
    for position, record in enumerate(records):
        if record.dimension != dimension:
            skipped += 1
            continue
        scored.append((cosine_similarity(query, record.vector), position, record))

    if not scored and skipped:
        raise VectorStoreDimensionError(
            f"{store_name}：{skipped} 条候选向量的维度都不是查询向量的 {dimension} 维，"
            "无法比较——多半是在用**另一个模型**的向量查询"
            "（不同模型的向量不可比，请确认 embedding_model 是否一致）"
        )

    scored.sort(key=lambda item: (-item[0], item[1]))
    if min_score is not None:
        scored = [item for item in scored if item[0] >= min_score]
    return [
        VectorMatch(
            chunk_id=record.chunk_id,
            document_id=record.document_id,
            content=record.content,
            metadata=record.metadata,
            model=record.model,
            score=score,
        )
        for score, _, record in scored[:top_k]
    ]


# ============================================================
# 四、接口
# ============================================================
class VectorStore(ABC):
    """向量存储接口（``add`` / ``search`` / ``count``）。

    刻意**不含任何数据库概念**：没有 ``db`` 参数、没有 SQL、没有表名。
    需要数据库的后端（``vector_store_sql``）在**构造函数**里接依赖，
    因此本接口对「内存 / MySQL / 真实向量库」一视同仁。
    """

    #: 实现名，出现在异常信息里，便于定位是哪个后端出的问题
    name: str = "base"

    @abstractmethod
    async def add(self, records: Sequence[VectorRecord]) -> int:
        """写入一组「切片 + 向量」，返回实际写入条数（upsert 语义）。"""

    @abstractmethod
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
        """按余弦相似度检索最相似的切片。

        :param model: 只比较该模型编码的向量（**强烈建议传**，避免混用不同模型的向量）
        :param document_id: 只在指定文档内检索
        :param category: 只检索该知识分类（取自切片 ``metadata["category"]``）
        :param min_score: 相似度下限
        """

    @abstractmethod
    async def count(self) -> int:
        """**已向量化**的切片数。"""

    def __repr__(self) -> str:  # pragma: no cover - 便于调试打印
        return f"<{type(self).__name__} name={self.name!r}>"


# ============================================================
# 五、内存实现（进程内，单测 / 本地打通链路用）
# ============================================================
def _coerce_record(item: Any, index: int) -> VectorRecord:
    """把 ``VectorRecord`` 或同名字段的 dict 归一成 ``VectorRecord``。"""
    if isinstance(item, VectorRecord):
        return item
    if isinstance(item, Mapping):
        known = {"vector", "content", "document_id", "chunk_id", "metadata", "model"}
        return VectorRecord(**{key: item[key] for key in known if key in item})
    raise VectorStoreInputError(
        f"（第 {index} 条）必须是 VectorRecord 或 dict，收到 {type(item).__name__}"
    )


def require_records(records: Any) -> List[VectorRecord]:
    """校验批量入参并归一成 ``List[VectorRecord]``。

    挡掉「把单个对象当成批量传」——``VectorRecord`` 不是可迭代的，但 ``dict`` 是
    （会被逐 key 拆开），所以对 ``Mapping`` / 字符串都要显式拦。
    """
    if records is None:
        raise VectorStoreInputError("add 不接受 None；空批次请传 []")
    if isinstance(records, (str, bytes, bytearray, VectorRecord, Mapping)):
        raise VectorStoreInputError(
            f"add 需要**一组**记录，收到单个 {type(records).__name__}；请包成 [record]"
        )
    try:
        items = list(records)
    except TypeError as exc:
        raise VectorStoreInputError(
            f"add 入参必须是可迭代的一组记录，收到 {type(records).__name__}"
        ) from exc
    return [_coerce_record(item, idx) for idx, item in enumerate(items)]


class InMemoryVectorStore(VectorStore):
    """进程内向量库：**不落库**，进程退出即消失。

    只适合单测与本地打通链路（生产请用 ``vector_store_sql`` 或真实向量库）。

    :param model: 默认模型标识。记录自带 ``model`` 时以记录为准；
        未带时用这个值补齐（避免出现一堆 ``embedding_model=""`` 的无主向量）。
        同时作为 ``search`` 未显式传 ``model`` 时的默认筛选条件。
    """

    name = "memory"

    def __init__(self, *, model: Optional[str] = None) -> None:
        self.model = model
        self._records: List[VectorRecord] = []
        self._next_id = 1

    @property
    def records(self) -> List[VectorRecord]:
        """当前所有记录（副本）。"""
        return list(self._records)

    async def add(self, records: Sequence[VectorRecord]) -> int:
        items = require_records(records)
        for record in items:
            record = replace(record, model=record.model or self.model or "")
            if record.chunk_id is None:
                # 新建：分配自增 id（不覆盖已有记录）
                self._records.append(replace(record, chunk_id=self._next_id))
                self._next_id += 1
                continue
            for position, existing in enumerate(self._records):
                if existing.chunk_id == record.chunk_id:
                    # upsert = **只更新向量相关字段**（与 SqlAlchemyVectorStore 一致）：
                    # content / metadata / document_id 属于「切片」，向量层不改写它们。
                    # 批量补向量的调用方通常只传 chunk_id + vector，若整体覆盖会把
                    # 切片正文清成空串。
                    self._records[position] = replace(
                        existing, vector=record.vector, model=record.model,
                    )
                    break
            else:
                # 内存实现没有「切片」概念，chunk_id 只是一个键：未知 id 视为新建。
                # （SQL 实现在这里会报错——见接口文档，两实现的差异是刻意的。）
                self._records.append(record)
        return len(items)

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
        effective_model = model if model is not None else self.model
        # 候选顺序 = 写入顺序（稳定），select_matches 用它做同分兜底排序
        candidates = [
            record for record in self._records
            if (effective_model is None or record.model == effective_model)
            and (document_id is None or record.document_id == document_id)
            and (category is None or record.metadata.get("category") == category)
        ]
        return select_matches(
            candidates, query_vector, top_k=top_k, min_score=min_score,
            store_name=self.name,
        )

    async def count(self) -> int:
        return len(self._records)


__all__ = [
    "DEFAULT_TOP_K",
    "InMemoryVectorStore",
    "VECTOR_MATCH_FIELDS",
    "VECTOR_RECORD_FIELDS",
    "VectorMatch",
    "VectorRecord",
    "VectorStore",
    "VectorStoreDimensionError",
    "VectorStoreError",
    "VectorStoreInputError",
    "cosine_similarity",
    "dot_product",
    "l2_norm",
    "require_records",
    "require_top_k",
    "require_vector",
    "select_matches",
]
