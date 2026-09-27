# -*- coding: utf-8 -*-
"""AI 模拟面试 · 知识检索接口（**RAG 扩展点，不实现真实 RAG**）。

为什么存在
----------
``InterviewAgent`` 出题时只看到「岗位 + 简历 + 计划 + 上下文」。真实面试官还会
**依据岗位领域知识**追问（如「你们 Redis 用的是 RDB 还是 AOF？为什么」）。
本模块把「取领域知识」抽象成一个**可替换的接口**，让 Agent 未来能消费它，
而**不必知道知识从哪来**（向量库 / 关键词库 / 人工手册 / 外部服务都可以）。

当前状态
--------
**只有接口与 Mock，没有真实检索。** 本模块**刻意**做到：

============================  ==========================================
不依赖数据库                   只 import 标准库，无 SQLAlchemy / Session
不依赖向量库                   无 chromadb / faiss / milvus / numpy …
不调用 LLM                    无 Spark / requests / websocket / openai
不修改 InterviewAgent         本模块**没有任何生产调用方**（纯预留）
============================  ==========================================

**无知识时返回 ``[]``** —— 这是接口的默认语义（基类实现即如此），
调用方不需要判 ``None``，也不会因为检索不到知识而中断出题。

接入真实 RAG 时怎么做
---------------------
1. 继承 :class:`KnowledgeRetriever`，覆写 :meth:`KnowledgeRetriever.retrieve`；
2. 把结果统一构造成 :class:`KnowledgeChunk`（用 ``from_dict`` 转换上游原始结构）；
3. 由调用方显式注入——生产链路上是 ``interview_core.retrieve_knowledge`` 的
   ``retriever`` 参数（本模块**不**提供全局单例，避免「悄悄接上 RAG」）。

.. warning::
   **同名不同物**：本模块的 :class:`KnowledgeChunk` 是**内存值对象**
   （``@dataclass(frozen=True)``）；``models.knowledge.KnowledgeChunk`` 是
   **ORM 数据库行**（有 ``id`` / ``document_id``）。

   两者**刻意不互相 import**：本模块必须保持「零第三方依赖、可脱离 DB 单测」，
   所以**不要在这里加 ``from_row`` / ``to_model`` 之类的方法**——那会让
   ``models``（进而 SQLAlchemy）被拉进 ``sys.modules``，直接破坏该性质，
   并被 ``tests/test_knowledge_retriever.py`` 的 AST 与子进程守卫拦下。
   「ORM 行 → 本模块值对象」的转换属于**真实 Retriever 自己的职责**
   （在它的模块里完成，本模块只提供 ``from_dict`` 这个归一入口）。

**接口签名已按异步设计**：真实检索几乎一定要访问外部服务（向量库 / HTTP），
现在写成 ``async`` 是为了届时不必修改任何调用方——反过来则会破坏所有调用点。

设计约束（与 ``question_validator`` 同级）
------------------------------------------
**纯内存、零第三方依赖、可脱离 HTTP 与数据库单测**；``retrieve`` 不修改入参、
不持有跨调用状态、同输入同输出。返回的每个 ``KnowledgeChunk`` 都是**新建对象**，
调用方改它不会污染检索器内部状态。

异常规范
--------
沿用项目约定：领域基类继承 ``Exception``，具体异常再额外继承最贴近的内建异常。
**「检索不到」不是异常**——它是 ``[]``；异常只用于「检索流程本身无法进行」。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

# ============================================================
# 契约常量
# ============================================================
#: :class:`KnowledgeChunk` 的字段名与顺序（``to_dict()`` 恒为此三键）。
KNOWLEDGE_CHUNK_FIELDS: Tuple[str, ...] = ("content", "source", "metadata")


# ============================================================
# 异常
# ============================================================
class KnowledgeRetrieverError(Exception):
    """知识检索的领域基类。"""


class InvalidChunkError(KnowledgeRetrieverError, ValueError):
    """构造 :class:`KnowledgeChunk` 时入参结构非法。"""


# ============================================================
# 值对象：知识片段
# ============================================================
@dataclass(frozen=True)
class KnowledgeChunk:
    """一条知识片段 —— 检索结果的**唯一**载体。

    **独立定义**：不继承 Pydantic / ORM / 向量库的任何类型，可自由构造、
    可被单元测试直接断言、可 ``to_dict()`` 后直接作为 API 响应体。

    序列化形状（``to_dict()`` 恒为此三键）::

        {"content": "", "source": "", "metadata": {}}

    字段
    ----
    - ``content``：知识正文。**唯一必需有意义的字段**，空串表示「有片段但没内容」。
    - ``source``：来源标识（如 ``mock://handbook/redis-persistence``）。
      用于**取证与溯源**——面试场景下「这条知识从哪来」必须可解释、可申诉。
    - ``metadata``：附加信息（``topic`` / ``difficulty`` / ``kind`` 等）。
      **上游结构里的额外字段一律放这里**，本类不保留未知键。

    **为什么 frozen**：检索结果会被 Agent、日志、测试多处消费，
    冻结可防止下游误改（也让实例可哈希、可放进 set / 当 dict 键）。
    注意 ``frozen`` 只阻止**属性重新绑定**，``metadata`` 这个 dict 本身仍可被改；
    ``to_dict()`` 返回的是它的**浅拷贝**。
    """

    content: str = ""
    source: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """转成普通 dict（三键）。``metadata`` 是**新建**的浅拷贝。"""
        return {
            "content": self.content,
            "source": self.source,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "KnowledgeChunk":
        """由普通 dict 构造。

        - 未知键**忽略**（额外信息请放进 ``metadata``）
        - 缺失键取默认值（``""`` / ``{}``）
        - 类型不符（``content`` / ``source`` 非 ``str``、``metadata`` 非 Mapping、
          入参本身不是 Mapping）→ 抛 :class:`InvalidChunkError`

        这是给**上游适配器**用的：向量库 / HTTP 返回的原始结构统一先转成本类，
        再交给 Agent，避免各处的形状差异渗进面试流程。
        """
        if not isinstance(data, Mapping):
            raise InvalidChunkError(
                f"知识片段必须是 Mapping，收到 {type(data).__name__}"
            )

        content = data.get("content", "")
        if not isinstance(content, str):
            raise InvalidChunkError(f"content 必须是 str，收到 {type(content).__name__}")

        source = data.get("source", "")
        if not isinstance(source, str):
            raise InvalidChunkError(f"source 必须是 str，收到 {type(source).__name__}")

        metadata = data.get("metadata", {})
        if metadata is None:
            metadata = {}
        if not isinstance(metadata, Mapping):
            raise InvalidChunkError(
                f"metadata 必须是 Mapping，收到 {type(metadata).__name__}"
            )

        return cls(content=content, source=source, metadata=dict(metadata))


# ============================================================
# 接口：知识检索器
# ============================================================
class KnowledgeRetriever:
    """知识检索接口（**基类 / 默认空实现**）。

    默认行为是「没有知识」——直接返回 ``[]``。因此本类**可以直接实例化使用**，
    语义明确：*我不提供任何知识*。真实实现继承本类并覆写
    :meth:`retrieve`，其余调用方代码一行不用改。

    ``retrieve`` 是**异步**的：真实检索几乎一定要访问外部服务（向量库 / HTTP），
    现在定为 ``async`` 是为了届时不必修改调用方；反过来则会破坏所有调用点。
    """

    #: 检索器标识，便于日志与测试区分实现（默认实现为 ``"none"``）。
    source_name: str = "none"

    async def retrieve(
        self,
        job_info: Any,
        topic: Any,
        context: Any,
    ) -> List[KnowledgeChunk]:
        """按「岗位 + 知识点 + 面试上下文」检索知识片段。

        参数
        ----
        - ``job_info``：岗位信息。可以是普通 dict，也可以是 ORM 行 / 任何带属性的对象
          ——**本模块不假设形状**，由具体实现自行决定怎么读。
        - ``topic``：待考察的知识点（如 ``"Redis 持久化"``）。
        - ``context``：面试上下文（阶段、已问问题、薄弱知识点等），供实现做加权。

        **本模块刻意不收 ``db`` 参数**：知识检索不应耦合业务数据库。

        返回
        ----
        ``List[KnowledgeChunk]``。**无知识时返回 ``[]``（绝不返回 ``None``）**，
        调用方无需判空分支即可继续出题。

        约定
        ----
        - 不修改入参，不持有跨调用状态，同输入同输出
        - 返回的每个片段都是**新建对象**，调用方改它不影响检索器内部状态
        - 「检索不到」是 ``[]``，不是异常
        """
        return []


# ============================================================
# Mock 实现（供测试与本地联调）
# ============================================================
#: Mock 返回的**固定**知识片段（与任何输入无关，保证可复现）。
MOCK_CHUNKS: Tuple[Dict[str, Any], ...] = (
    {
        "content": (
            "Redis 持久化有 RDB 与 AOF 两种方式：RDB 是某一时刻的全量快照，恢复快但可能丢数据；"
            "AOF 记录写命令，可通过 appendfsync 控制落盘频率，数据更安全但文件更大。"
            "生产上常两者混用：RDB 用于快速恢复，AOF 用于降低丢失窗口。"
        ),
        "source": "mock://handbook/redis-persistence",
        "metadata": {"topic": "Redis 持久化", "difficulty": "mid", "kind": "concept"},
    },
    {
        "content": (
            "判定链表是否有环用快慢指针（Floyd 判圈）：快指针每次走两步、慢指针走一步，"
            "若相遇则有环；再让一个指针从头出发与相遇点同步前进，重合处即环入口。"
            "时间 O(n)、空间 O(1)。"
        ),
        "source": "mock://handbook/linked-list-cycle",
        "metadata": {"topic": "链表", "difficulty": "junior", "kind": "algorithm"},
    },
    {
        "content": (
            "订单中台拆分微服务时，先按业务能力划边界，再用领域事件解耦；"
            "跨服务一致性优先用本地消息表 + 最终一致，避免强分布式事务带来的可用性下降。"
        ),
        "source": "mock://handbook/microservice-boundary",
        "metadata": {"topic": "微服务拆分", "difficulty": "senior", "kind": "experience"},
    },
)


class MockKnowledgeRetriever(KnowledgeRetriever):
    """Mock 检索器：返回**固定**知识片段，**忽略所有入参**。

    用途：让 Agent / Core 的测试能在**不接向量库、不调 LLM** 的前提下
    走通「拿到知识」这条分支。生产环境**不要**使用。

    - ``chunks`` 省略时返回模块级常量 :data:`MOCK_CHUNKS`
    - 传 ``chunks=[]`` 可模拟「无知识」路径
    - 入参会被记录在 ``calls`` 里（供测试断言调用次数与传参）
    """

    source_name = "mock"

    def __init__(
        self,
        chunks: Optional[Sequence[Any]] = None,
        *,
        source: Optional[str] = None,
    ) -> None:
        raw: Sequence[Any] = MOCK_CHUNKS if chunks is None else chunks
        self._chunks: List[KnowledgeChunk] = [
            item if isinstance(item, KnowledgeChunk) else KnowledgeChunk.from_dict(item)
            for item in raw
        ]
        self._source_override = source
        #: 每次调用的入参，形如 ``[(job_info, topic, context), ...]``
        self.calls: List[Tuple[Any, Any, Any]] = []

    @property
    def chunks(self) -> List[KnowledgeChunk]:
        """当前配置的固定片段（副本，改它不影响检索器）。"""
        return list(self._chunks)

    async def retrieve(
        self,
        job_info: Any,
        topic: Any,
        context: Any,
    ) -> List[KnowledgeChunk]:
        """返回固定片段（**每次都是新建对象**，与入参无关）。"""
        self.calls.append((job_info, topic, context))
        return [
            KnowledgeChunk(
                content=chunk.content,
                source=self._source_override or chunk.source,
                metadata=dict(chunk.metadata),
            )
            for chunk in self._chunks
        ]


__all__ = [
    "KNOWLEDGE_CHUNK_FIELDS",
    "KnowledgeRetrieverError",
    "InvalidChunkError",
    "KnowledgeChunk",
    "KnowledgeRetriever",
    "MockKnowledgeRetriever",
    "MOCK_CHUNKS",
]
