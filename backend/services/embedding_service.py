# -*- coding: utf-8 -*-
"""AI 面试知识库 · EmbeddingService（文本向量化：**接口 + 离线默认实现，不绑厂商**）。

分层定位
--------
::

    knowledge_document_service   导入：{title, content, …} → KnowledgeDocument（落库）
        └── document_chunker     content → [{content, metadata}, …]（纯内存）
              └── embedding_service   【本模块】text → vector（纯内存）
                    └── (将来) 写入 KnowledgeChunk.embedding → 向量检索 → Retriever
                          → Core.retrieve_knowledge → Agent

本模块只做一件事：**把一段文本变成向量**。刻意**不实现**：

- 任何具体厂商的调用（OpenAI / 通义 / 智谱 / BGE / 讯飞 …）——**真实实现在
  ``services/embedding_provider.py``**（OpenAI 兼容 ``/embeddings`` + ``EMBEDDING_*`` 配置）。
  之所以另起一个模块而不是写在这里：本模块的「零第三方依赖」被守卫测试锁死，
  真实实现必然要发 HTTP，塞进来会当场破坏它（同 ``vector_knowledge_retriever`` 的取舍）。
  下面「怎么接真实模型」一节保留，作为**自己写一个实现**时的最小步骤说明。
- 向量数据库（chromadb / faiss / milvus …）与相似度检索
- Retriever（``services/knowledge_retriever.py`` 的真实实现）
- 落库（``KnowledgeChunk.embedding`` 这一列已经预留，写入是下一步）
- ``InterviewAgent`` / ``InterviewCore`` / ``InterviewService`` 的任何修改

三个要求怎么落地
----------------
1. **支持后续替换模型** —— 抽象基类 :class:`EmbeddingService` 只规定
   ``_embed_one`` 一个钩子，``embed`` / ``embed_batch`` 是**带校验的模板方法**：
   换模型 = 写一个子类、实现一个方法、声明 ``name`` / ``dimension``，其余零改动。
2. **不绑定具体厂商** —— 本模块**不 import 任何厂商 SDK / HTTP 客户端**；
   自带两个实现都**零第三方依赖**：
   :class:`HashEmbeddingService`（离线、确定性，打通链路与单测用）与
   :class:`MockEmbeddingService`（固定向量，测试替身）。
   **不做全局单例**，由调用方显式构造并注入（与 ``knowledge_retriever`` 同约定），
   避免「悄悄换了模型」这类不可控变更。
3. **异常处理清晰** —— 四类异常各司其职（见下），且**基类统一做入参 / 出参校验**，
   子类作者不必重复写；上游故障与「模型给了错东西」分得清清楚楚。

异常（按项目规范：领域基类 + 最贴近的内建异常）
------------------------------------------------
======================  ==========================  ==================================
异常                    同时继承                    什么时候抛
======================  ==========================  ==================================
``EmbeddingError``      ``Exception``               领域基类，捕获"所有向量化问题"
``EmbeddingInputError`` ``ValueError``              **入参**问题：空文本 / 纯空白 /
                                                    非字符串 / 把单个字符串传给批量接口
``EmbeddingUnavailableError`` ``RuntimeError``      **服务**问题：模型未配置 / 网络失败 /
                                                    上游返回错误 / 限流
``EmbeddingDimensionError`` ``ValueError``          **出参**问题：向量不是数值序列 /
                                                    空向量 / 与声明的 ``dimension`` 不符
======================  ==========================  ==================================

区分「入参错」（调用方改）与「服务不可用」（运维 / 重试）是本模块的核心意图——
真实模型接进来以后，这两类的处理方式完全不同（前者不该重试，后者该重试 / 降级）。

怎么接真实模型（替换步骤）
--------------------------
**现成实现**：``services/embedding_provider.EmbeddingProvider``（OpenAI 兼容协议 + 环境变量配置 +
原生批量 + 异常映射），由 ``knowledge_rag.default_embedder()`` 按 ``EMBEDDING_*`` 自动选用。
想自己接别的厂商，按下述最小步骤即可：
::

    class MyVendorEmbedding(EmbeddingService):
        name = "myvendor-v3"          # 写进 KnowledgeChunk.embedding_model，便于换模型后重算
        dimension = 1024

        def __init__(self, api_key: str, *, endpoint: str = "..."):
            self._client = SomeClient(api_key, endpoint)

        async def _embed_one(self, text: str) -> List[float]:
            try:
                resp = await self._client.embed(text)
            except SomeNetworkError as exc:            # 上游故障 → 服务级异常
                raise EmbeddingUnavailableError(f"向量服务不可用：{exc}") from exc
            return resp.vector

        async def embed_batch(self, texts):            # 厂商有原生批量接口就覆盖它
            ...                                        # 覆盖后仍应复用基类的校验口径

换模型时必须注意：**不同模型的向量不可比**。``KnowledgeChunk`` 上已预留
``embedding_model`` / ``embedding_dim`` 两列，换模型后要按它们找出「旧向量」重算，
不要新旧混用（详见 ``models/knowledge.py``）。

.. warning::
   :class:`HashEmbeddingService` **不是语义向量**——它只按字面 token 做哈希散列，
   "苹果手机" 与 "iPhone" 在它眼里毫不相关。它的价值是**离线、确定性、零依赖**：
   让整条链路（切片 → 向量 → 存储 → 检索）在没有密钥、没有网络的机器上也能跑通并单测。
   **不要**用它评估检索质量，也不要把它接到生产检索链路上。

运行状态标识（供日志 / 调试）
-----------------------------
「当前用的是离线占位还是真实语义模型」以前只写在文档里、靠人记。现在它是一个**值**：
:func:`describe_embedding` 读出 :class:`EmbeddingInfo`（``provider`` / ``dimension`` /
``semantic_enabled``），**纯读取、无 IO、不编码文本**，可直接进启动日志或健康检查。
``semantic_enabled`` 由**各实现自行声明**（``HashEmbeddingService`` /
``MockEmbeddingService`` 为 ``False``，真实 Provider 为 ``True``）——
**基类一行未改**（接口面保持原样），未声明的实现按「非语义」**保守上报**。
组装器侧的入口是 ``knowledge_rag.describe_default_embedding()``：
一句话回答「本进程默认会用什么 Embedding」。

存储方式（为什么是这三列）
--------------------------
``KnowledgeChunk`` 新增：``embedding``（JSON，可空，存 ``List[float]``）、
``embedding_model``（VARCHAR(100)，非空默认空串）、``embedding_dim``（INTEGER，可空）。
不用 ``pgvector`` 之类专用类型，因为运行库是 **MySQL 8**，而 JSON 列在 SQLite / MySQL
上都能用、无需额外扩展；换模型后靠 ``embedding_model`` 定位待重算的行。
"""

from __future__ import annotations

import hashlib
import re
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, Dict, List, NamedTuple, Optional

#: 离线默认实现的向量维度。真实模型一般是 768 / 1024 / 1536。
DEFAULT_EMBEDDING_DIMENSION = 256

#: 哈希种子。固定它才能保证「同文本 → 同向量」跨进程稳定。
DEFAULT_EMBEDDING_SEED = "career-ai"


# ============================================================
# 异常
# ============================================================
class EmbeddingError(Exception):
    """向量化的领域基类（捕获"所有向量化问题"用）。"""


class EmbeddingInputError(EmbeddingError, ValueError):
    """**入参**问题：空文本 / 纯空白 / 非字符串 / 把单个字符串当成批量。

    归为 ``ValueError``：调用方改代码就能解决，**不该重试**。
    """


class EmbeddingUnavailableError(EmbeddingError, RuntimeError):
    """**服务**问题：模型未配置 / 网络失败 / 上游报错 / 限流。

    归为 ``RuntimeError``：与调用方代码无关，**可以重试或降级**。
    """


class EmbeddingDimensionError(EmbeddingError, ValueError):
    """**出参**问题：向量不是数值序列 / 空向量 / 与声明的 ``dimension`` 不符。

    这类错误说明**实现有问题**（模型换了维度却没改 ``dimension``、上游返回了脏数据），
    必须显式暴露而不是悄悄放过——否则脏向量会一路流进检索。
    """


# ============================================================
# 入参 / 出参校验（基类统一做，子类不用重复写）
# ============================================================
def _require_text(value: Any, *, index: Optional[int] = None) -> str:
    """校验单条文本；返回原文本（**不做 strip**，只按 strip 结果判空）。"""
    where = "" if index is None else f"（第 {index} 条）"
    if not isinstance(value, str):
        raise EmbeddingInputError(
            f"{where}只接受 str，收到 {type(value).__name__}；"
            "数字 / None 等需要调用方先转成字符串"
        )
    if not value.strip():
        raise EmbeddingInputError(f"{where}不接受空文本或纯空白文本——没有语义可编码")
    return value


def _require_texts(texts: Any) -> List[str]:
    """校验批量入参；返回 ``list``。

    特别挡住「把单个字符串传进批量接口」：``str`` 是可迭代的，若不拦，
    ``embed_batch("你好")`` 会被静默拆成两条向量，是最隐蔽的一类 bug。
    """
    if texts is None:
        raise EmbeddingInputError("embed_batch 不接受 None；空批次请传 []")
    if isinstance(texts, (str, bytes, bytearray)):
        raise EmbeddingInputError(
            f"embed_batch 需要**一组**文本，收到单个 {type(texts).__name__}——"
            "传字符串会被逐字符拆开；单条请用 embed()，或包成 [text]"
        )
    try:
        items = list(texts)
    except TypeError as exc:
        raise EmbeddingInputError(
            f"embed_batch 入参必须是可迭代的一组文本，收到 {type(texts).__name__}"
        ) from exc
    for idx, item in enumerate(items):
        _require_text(item, index=idx)
    return items


def _require_vector(vector: Any, service: str, dimension: int) -> List[float]:
    """校验并归一向量：必须是数值序列、非空、与声明的 ``dimension`` 一致。

    刻意**不要求**入参是 ``list``——真实厂商的 SDK 常返回 ``tuple`` 或 numpy 数组，
    因此这里只要求「可迭代」，再逐个把元素转成 ``float``。
    """
    if isinstance(vector, (str, bytes, bytearray)):
        raise EmbeddingDimensionError(
            f"{service} 返回的向量必须是数值序列，收到字符串（上游多半返回了 JSON 字符串）"
        )
    try:
        items = list(vector)
    except TypeError as exc:
        raise EmbeddingDimensionError(
            f"{service} 返回的向量必须是可迭代的数值序列，收到 {type(vector).__name__}"
        ) from exc

    values: List[float] = []
    for item in items:
        # bool 是 int 的子类；字符串能被 float() 转换——这三类混进来一定是实现写错了
        if isinstance(item, (bool, str, bytes, bytearray)):
            raise EmbeddingDimensionError(
                f"{service} 返回的向量含非数值元素：{item!r}（{type(item).__name__}）"
            )
        try:
            values.append(float(item))
        except (TypeError, ValueError) as exc:
            raise EmbeddingDimensionError(
                f"{service} 返回的向量含非数值元素：{item!r}（{type(item).__name__}）"
            ) from exc

    if not values:
        raise EmbeddingDimensionError(f"{service} 返回了空向量")
    if dimension and len(values) != dimension:
        raise EmbeddingDimensionError(
            f"{service} 声明 {dimension} 维，实际返回 {len(values)} 维；"
            "换模型时必须同步修改 dimension"
        )
    return values


# ============================================================
# 一、抽象基类
# ============================================================
class EmbeddingService(ABC):
    """文本向量化接口。

    子类只需实现 :meth:`_embed_one`（单条），并声明 ``name`` / ``dimension``；
    :meth:`embed` / :meth:`embed_batch` 由基类提供，且**统一带校验**：

    - 入参：空文本 / 纯空白 / 非字符串 → ``EmbeddingInputError``
    - 出参：向量结构或维度不合法 → ``EmbeddingDimensionError``
    - 上游故障：子类**应当**抛 ``EmbeddingUnavailableError``

    这样「换一个模型」不会顺便换掉错误口径——调用方的 ``except`` 永远有效。
    """

    #: 实现名，写进 ``KnowledgeChunk.embedding_model``（换模型后据此找出旧向量）
    name: str = "base"

    #: 声明维度；``0`` 表示「不声明 / 不校验维度」
    dimension: int = 0

    async def embed(self, text: str) -> List[float]:
        """把**一段**文本编码成向量（``List[float]``）。"""
        cleaned = _require_text(text)
        vector = await self._embed_one(cleaned)
        return _require_vector(vector, self.name, self.dimension)

    async def embed_batch(self, texts: Sequence[str]) -> List[List[float]]:
        """把**一组**文本编码成向量列表（返回顺序与入参一致）。

        默认实现逐条调用 :meth:`embed`（已先整体校验入参，因此错误信息能指出是第几条）。
        **厂商有原生批量接口时应当覆盖本方法**——那是真实模型最主要的性能来源；
        覆盖后请保持同样的校验口径与返回顺序。
        """
        items = _require_texts(texts)
        return [await self.embed(item) for item in items]

    @abstractmethod
    async def _embed_one(self, text: str) -> List[float]:
        """真正的编码逻辑（已保证 ``text`` 是非空字符串）。

        **异步**是刻意的：真实实现几乎一定访问外部服务，先 async 可以避免届时
        改坏所有调用点（与 ``KnowledgeRetriever.retrieve`` 同一考量）。
        """

    def __repr__(self) -> str:  # pragma: no cover - 便于调试打印
        return f"<{type(self).__name__} name={self.name!r} dimension={self.dimension}>"


# ============================================================
# 二、离线默认实现（确定性、零依赖）
# ============================================================
_WORD_PATTERN = re.compile(r"[a-z0-9_]+")
_CJK_RUN_PATTERN = re.compile(r"[\u4e00-\u9fff]+")


def tokenize(text: str) -> List[str]:
    """把文本切成 token：英文/数字按词，中文按**单字 + 相邻二字组**。

    中文没有空格，按词切需要分词器（引第三方依赖）；单字 + 二字组是零依赖的近似，
    既保留单字信息，又让"持久化"与"持久化配置"共享 "持久" 这个二字组。

    公开出来是为了让测试能直接验证「同 token → 同向量」这条因果链，
    而不是只看两个向量是否相等。
    """
    lowered = text.lower()
    tokens: List[str] = [m.group(0) for m in _WORD_PATTERN.finditer(lowered)]
    for match in _CJK_RUN_PATTERN.finditer(text):
        run = match.group(0)
        tokens.extend(run)                                     # 单字
        tokens.extend(run[i:i + 2] for i in range(len(run) - 1))  # 相邻二字组
    return tokens


def _l2_normalize(vector: List[float]) -> List[float]:
    """L2 归一化：归一后余弦相似度退化为点积，下游检索实现更简单。

    全零向量（文本里没有任何可编码 token，如只有标点）原样返回，
    避免除零；下游可用「模长为 0」识别这类退化结果。
    """
    norm = sum(v * v for v in vector) ** 0.5
    if norm == 0:
        return vector
    return [v / norm for v in vector]


class HashEmbeddingService(EmbeddingService):
    """**离线占位实现**：按 token 哈希散列到固定维度（不是语义向量！）。

    - **确定性**：用 ``hashlib.blake2b``（不是内置 ``hash()``——后者受
      ``PYTHONHASHSEED`` 影响，**跨进程不稳定**），同文本永远同向量
    - **零依赖**：只用标准库，无网络、无密钥
    - **有区分度**：共享 token 的两段文本会得到相近的向量（可用于自测相似度）

    用途仅限**打通链路与单测**，绝不可用于评估检索质量（见模块文档的 warning）。
    """

    name = "hash-local"
    dimension = DEFAULT_EMBEDDING_DIMENSION
    #: 本实现**不是**语义向量（按字面 token 哈希散列）——供 :func:`describe_embedding`
    #: 上报运行状态；离线占位必须如实报 ``False``，否则日志会误导使用者。
    semantic_enabled = False

    def __init__(
        self,
        *,
        dimension: int = DEFAULT_EMBEDDING_DIMENSION,
        normalize: bool = True,
        seed: str = DEFAULT_EMBEDDING_SEED,
    ) -> None:
        if isinstance(dimension, bool) or not isinstance(dimension, int) or dimension <= 0:
            raise EmbeddingInputError(f"dimension 必须是正整数，当前 {dimension!r}")
        self.dimension = dimension
        self.normalize = normalize
        self.seed = seed

    async def _embed_one(self, text: str) -> List[float]:
        vector = [0.0] * self.dimension
        for token in tokenize(text):
            digest = hashlib.blake2b(
                f"{self.seed}\x00{token}".encode("utf-8"), digest_size=8
            ).digest()
            value = int.from_bytes(digest, "big")
            # 低位决定落到哪一维，最高位决定正负号（符号哈希，抵消哈希碰撞的系统性偏置）
            vector[value % self.dimension] += 1.0 if (value >> 63) & 1 else -1.0
        return _l2_normalize(vector) if self.normalize else vector


# ============================================================
# 三、测试替身
# ============================================================
class MockEmbeddingService(EmbeddingService):
    """固定向量的测试替身：**不关心文本内容**，但记录每次调用。

    与 ``knowledge_retriever.MockKnowledgeRetriever`` 同款设计——
    断言「被调了几次、收到什么文本」用 ``calls``，而不是比对向量数值。

    :param raises: 传异常实例时，每次 ``_embed_one`` 都抛它（用于测异常路径）
    """

    name = "mock"
    dimension = 8
    #: 测试替身同样不是语义向量（固定向量，不看内容）。
    semantic_enabled = False

    def __init__(
        self,
        *,
        dimension: int = 8,
        value: float = 1.0,
        raises: Optional[BaseException] = None,
    ) -> None:
        self.dimension = dimension
        self.value = value
        self.raises = raises
        self.calls: List[str] = []

    async def _embed_one(self, text: str) -> List[float]:
        self.calls.append(text)
        if self.raises is not None:
            raise self.raises
        return [self.value] * self.dimension


# ============================================================
# 四、运行状态标识（供日志 / 调试：一眼看清「当前用的是哪种 Embedding」）
# ============================================================
#: 实现用来声明「本实现是否产出**语义**向量」的类属性名。
#:
#: **刻意不把它声明在 :class:`EmbeddingService` 基类上**——那会改动接口面。
#: 基类保持零改动，由各实现自行声明；读取时用 ``getattr(..., False)``，
#: 于是**未声明的实现一律按「非语义」上报**：宁可少报，绝不**多**报——
#: 日志里把离线占位说成语义模型，比不报更糟（会让人误以为检索质量可信）。
SEMANTIC_ENABLED_ATTR = "semantic_enabled"


class EmbeddingInfo(NamedTuple):
    """Embedding **运行状态标识**（只读快照，供日志与调试；不参与任何业务判断）。

    用途：让「系统当前到底在用哪种 Embedding」变成**可读、可断言**的一个值，
    而不是散落在文档里的口头说明。

    :param provider: 实现标识 —— 即 ``embedder.name``，也正是写进
        ``KnowledgeChunk.embedding_model`` 的那个值。离线占位是 ``hash-local``；
        真实 Provider 是**模型名**（如 ``text-embedding-3-small``）。
    :param dimension: 声明维度；``0`` 表示实现**不声明 / 不校验**维度
        （真实 Provider 未配 ``EMBEDDING_DIMENSION`` 时就是 ``0``）。
    :param semantic_enabled: 是否产出**语义**向量。``False`` ⇒ 只够打通链路，
        **不可用于评估检索质量**。
    """

    provider: str
    dimension: int
    semantic_enabled: bool

    def to_dict(self) -> Dict[str, Any]:
        """摊平成 dict（日志 / JSON 序列化用）。恒为三键。"""
        return {
            "provider": self.provider,
            "dimension": self.dimension,
            "semantic_enabled": self.semantic_enabled,
        }


def describe_embedding(embedder: EmbeddingService) -> EmbeddingInfo:
    """读出 ``embedder`` 的运行状态标识。

    **纯读取**：不编码任何文本、不发网络请求、不读配置、不碰数据库——
    因此可以在启动日志、健康检查、排障脚本里随便调用。

    :raises EmbeddingInputError: ``embedder`` 不是 :class:`EmbeddingService`，
        或它的 ``name`` / ``dimension`` / ``semantic_enabled`` **声明不合法**。
        归为「入参错」（``ValueError``）：改代码即可解决，**不该重试**。
    """
    if not isinstance(embedder, EmbeddingService):
        raise EmbeddingInputError(
            f"describe_embedding 只接受 EmbeddingService，收到 {type(embedder).__name__}"
        )

    provider = embedder.name
    if not isinstance(provider, str) or not provider.strip():
        raise EmbeddingInputError(
            f"EmbeddingService.name 必须是非空字符串，当前 {provider!r}"
        )

    dimension = embedder.dimension
    # bool 是 int 的子类：dimension=True 会静默变成 1，必须显式挡掉
    if isinstance(dimension, bool) or not isinstance(dimension, int) or dimension < 0:
        raise EmbeddingInputError(
            f"EmbeddingService.dimension 必须是非负整数（0 = 不声明维度），当前 {dimension!r}"
        )

    declared = getattr(embedder, SEMANTIC_ENABLED_ATTR, False)
    # 不写成 bool(declared)：字符串 "False" / 数字 0 会被真值判断悄悄放过，
    # 于是「日志说语义、实际不是」这类静默错误就查不出来了。
    if not isinstance(declared, bool):
        raise EmbeddingInputError(
            f"{SEMANTIC_ENABLED_ATTR} 必须是 bool，当前 {declared!r}"
            f"（{type(embedder).__name__} 的声明写错了）"
        )

    return EmbeddingInfo(
        provider=provider, dimension=dimension, semantic_enabled=declared
    )


__all__ = [
    "DEFAULT_EMBEDDING_DIMENSION",
    "DEFAULT_EMBEDDING_SEED",
    "EmbeddingDimensionError",
    "EmbeddingError",
    "EmbeddingInfo",
    "EmbeddingInputError",
    "EmbeddingService",
    "EmbeddingUnavailableError",
    "HashEmbeddingService",
    "MockEmbeddingService",
    "SEMANTIC_ENABLED_ATTR",
    "describe_embedding",
    "tokenize",
]
