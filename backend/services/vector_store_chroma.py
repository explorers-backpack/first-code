# -*- coding: utf-8 -*-
"""向量存储 · 真实 **ANN** 后端（chromadb）。

为什么是单独一个模块
--------------------
``services/vector_store.py`` 的**零第三方依赖**被测试用 AST **相等**断言锁死
（顶层只许 import ``__future__`` / ``abc`` / ``collections`` / ``dataclasses`` /
``typing``）。真实 ANN 库必然带第三方依赖，因此**不能**写进那个模块——
与 ``vector_knowledge_retriever`` / ``embedding_provider`` 另起模块是同一个理由：
**接口与实现分离**，守卫锁住的是接口那一份。

为什么选 chromadb
-----------------
需求是「接入一种向量数据库」。候选与取舍：

===================  ==========================================================
候选                 结论
===================  ==========================================================
**chromadb**         ✅ **采用**。嵌入式（无服务端、无独立端口）、内置
                     **真实 HNSW**（``chroma-hnswlib``）、持久化到本地目录、
                     原生支持元数据过滤；Windows 有 ``cp39-abi3`` 轮子
                     （一个轮子覆盖 3.10+，不需要本机编译 C++）
hnswlib              仅源码包（``.tar.gz``），Windows 上要装 MSVC 才能编译
qdrant-client        真 ANN 需要**跑一个服务**（Docker / 独立进程），
                     与「嵌入式、开箱即用」的诉求不符
faiss-cpu / usearch  是真 ANN 库，但不是**向量数据库**（无持久化 / 无元数据过滤 /
                     无集合管理），要自己补一层存储与过滤
milvus-lite          Milvus Lite 官方不支持 Windows
===================  ==========================================================

代价要说清楚：``chromadb`` 会带进一批传递依赖（``onnxruntime`` / ``grpcio`` /
``kubernetes`` / ``tokenizers`` …）。因此本项目把它做成**可选后端**：
``requirements.txt`` 里单独一段标注，**默认后端仍是 ``vector_store_sql``**，
不选它就不需要装它（``knowledge_rag`` 里也是**函数内延迟导入**）。

它解决什么问题
--------------
``vector_store_sql`` **没有 ANN 索引**：每次 ``search`` 都是
``WHERE embedding IS NOT NULL`` 全表扫描 + Python 侧算余弦，只适合几千条以内。
本模块用 HNSW 做**近似最近邻**召回，把「全表扫描」换成「图搜索」，
是这一层唯一的性能升级点。

架构：**ANN 索引是派生数据，权威副本仍在 MySQL**
-----------------------------------------------
本模块**组合** :class:`~services.vector_store_sql.SqlAlchemyVectorStore`，
而不是另建一套切片存储。理由：

1. **不能有两份真相**。切片正文与元信息的权威在 ``knowledge_chunk`` 行；
   若向量库自成一套，``knowledge_import_pipeline`` 的幂等判据
   （``row.embedding IS NULL`` = 待向量化）就永远为真 → 每次重跑都全量重算。
2. **写入顺序固定为「先权威、后索引」**：``add`` 先落 MySQL（沿用 SQL 后端
   的全部校验与字段覆盖范围），再把**从库里读回来的行**镜像进索引。
   索引内容因此永远等于权威行，而不是等于调用方入参——
   入参在「给既有切片补向量」时**本就被契约要求忽略**（见 ``VectorStore.add``）。
3. **索引可以随时丢弃重建**（``knowledge_chunk.embedding`` 就是权威副本）。

检索：**ANN 召回 + 精确重排**
----------------------------
``search`` 先用 HNSW 取回 ``top_k × OVERSAMPLE`` 个候选，再交给
:func:`~services.vector_store.select_matches` 做**精确**余弦排序。

因此三个后端（内存 / SQL / chroma）的**排序口径、``score`` 口径、``min_score``
过滤、同分兜底顺序完全一致**——这是「换后端不换语义」可被测试断言的前提。
代价是**召回**是近似的：ANN 只保证「大概率命中」，不保证与全量扫描的候选集相同。
这是 ANN 的固有性质，不是实现缺陷；要绝对精确请用 ``vector_store_sql``。

维度契约（比 SQL 后端更严）
---------------------------
一个 ANN 索引只能有**一个维度**（HNSW 的图结构依赖固定维度），因此：

- ``add`` 时若**同一批次**维度不一致，或**与索引已有维度**不一致 →
  抛 :class:`~services.vector_store.VectorStoreDimensionError`
  （SQL 后端允许混存不同维度，因为它逐行记 ``embedding_dim``；本模块做不到，**刻意报错而不是悄悄丢弃**）
- ``search`` 时若查询维度与索引维度不一致 → 同样抛
  :class:`~services.vector_store.VectorStoreDimensionError`

校验发生在**写 MySQL 之前**（见 :meth:`ChromaVectorStore.add`），
避免出现「权威行已写、索引没镜像」的半截状态。

元数据怎么落
------------
chromadb 的元数据只接受**标量**（``str`` / ``int`` / ``float`` / ``bool``），
而 ``VectorRecord.metadata`` 允许嵌套。因此每个条目同时写两份：

- **扁平标量**：供 chroma 的 ``where`` 过滤（``document_id`` / ``embedding_model`` /
  元数据里的标量键，如 ``category``）。于是 ``category`` 过滤在**索引层**完成，
  比 SQL 后端的 Python 侧过滤更早、更省。
- ``_meta``：完整元数据的 **JSON 字符串**，供读回时**保真还原**（含嵌套值）。

``__post_init__`` 校验失败的行（脏数据）在镜像与检索时一律**跳过**，不让一条坏数据打挂整次流程。

刻意不做
--------
- **不做全局单例**：chroma 客户端按需创建、由调用方持有；本模块只在
  :class:`ChromaVectorStore` 实例内部缓存句柄（与 SQL 后端「按请求构造」一致）。
- **不接 InterviewAgent / InterviewCore / InterviewService**：向量层只被
  ``knowledge_rag`` 组装，面试流程模块一律不碰。
- **不做线程池**：chroma 的本地客户端是**同步**的，本模块直接调用（见
  :meth:`ChromaVectorStore._collection_handle` 的说明）。

**删/重建的边界（与既有约定一致，勿误读为「删数据」）**
--------------------------------------------------------
本模块提供 :meth:`ChromaVectorStore.delete` 与 :meth:`ChromaVectorStore.reset`
——它们**只动派生 ANN 索引，绝不动权威行**（``knowledge_chunk``）。因此：

- 它们是**索引维护原语**，不是「删数据」接口；权威数据的删除由
  ``services/knowledge_maintenance.delete_document`` 显式执行。
- **不提供** ``rebuild``：从权威行重建索引要读 ``knowledge_chunk``，
  那是**编排**（维护模块的职责），不是存储后端的职责——本模块只提供
  「按 id 移除」与「清空」两个原语，编排留给
  ``services/knowledge_maintenance.rebuild_index``。
- 与 ``VectorStore`` 接口的关系：这两个方法**刻意不是接口方法**
  （同 ``vector_store_sql.add_returning_ids`` / ``load_records`` 的先例），
  因此 ``VectorStore.__abstractmethods__`` 仍是 ``{add, search, count}``——
  换后端不换语义的契约一个字没动。
- ``sqlalchemy`` / ``memory`` 后端**没有**这两个方法，也不需要：
  它们的「索引」就是权威行本身，删行即同步。
- ``delete`` 按 id 移除；``reset`` 是**丢弃并重建集合**（不是逐行删空）——
  HNSW 集合的维度是粘的，只有重建才回到「维度未定」，这也是
  「换 Embedding 模型后重建索引」的唯一路径（详见 :meth:`ChromaVectorStore.reset`）。

.. note::
   **import 本模块需要 ``DATABASE_URL``**：它顶层 import ``vector_store_sql`` →
   ``models`` → ``database``，而 ``database`` 缺 ``DATABASE_URL`` 时直接 ``RuntimeError``。
   这与 ``vector_store_sql`` 完全一致，也是 ``knowledge_rag`` **延迟导入**本模块的原因。
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from services.vector_store import (
    DEFAULT_TOP_K,
    VectorMatch,
    VectorRecord,
    VectorStore,
    VectorStoreDimensionError,
    VectorStoreError,
    VectorStoreInputError,
    require_records,
    require_top_k,
    require_vector,
    select_matches,
)
from services.vector_store_sql import SqlAlchemyVectorStore

# ============================================================
# 配置
# ============================================================
#: 索引目录的环境变量（不配则用 ``backend/data/chroma``）
ENV_PATH = "CHROMA_PATH"

#: 默认集合名（与 ``knowledge_chunk`` 同名：索引与权威表一一对应，便于排查）
DEFAULT_COLLECTION = "knowledge_chunk"

#: 默认索引目录（``backend/data/chroma``；已在 ``.gitignore`` 中排除）
DEFAULT_PATH = str(Path(__file__).resolve().parents[1] / "data" / "chroma")

#: ANN 召回倍率：取 ``top_k × OVERSAMPLE`` 个候选再精确重排。
#: 1 表示「完全信任 HNSW 的排序」（不重排）；取 >1 是为了让精确重排有腾挪空间
#: （尤其配合 ``min_score`` 过滤时）。越大越接近全量扫描，也越慢。
DEFAULT_OVERSAMPLE = 4

#: 距离度量：HNSW 用余弦（与 ``select_matches`` 的余弦口径一致，
#: 否则「ANN 排出来的序」与「精确重排的序」会系统性错位，召回质量白白浪费）
HNSW_SPACE = "cosine"

# ---- chroma 元数据保留键 ------------------------------------
#: 完整元数据的 JSON（保真还原用；chroma 存不了嵌套结构）
META_PAYLOAD = "_meta"
#: 文档 id（真列语义；供 ``where`` 过滤）
META_DOCUMENT_ID = "document_id"
#: 模型标识（对应 ``knowledge_chunk.embedding_model``；供 ``where`` 过滤）
META_MODEL = "embedding_model"
#: 知识分类（来自 ``metadata["category"]``；供 ``where`` 过滤）
META_CATEGORY = "category"

#: ``where`` 过滤里**不**当作业务元数据的保留键（读回时剔除）
_RESERVED_META_KEYS = frozenset({META_PAYLOAD, META_DOCUMENT_ID, META_MODEL})


# ============================================================
# 异常（项目规范：领域基类 + 最贴近的内建异常）
# ============================================================
class ChromaBackendError(VectorStoreError, RuntimeError):
    """**运行时 / 依赖**问题：没装 ``chromadb``、索引目录打不开、客户端建不起来。

    归 ``RuntimeError``（与 ``EmbeddingUnavailableError`` 同款口径）：
    重试或改环境可能有效，不是调用方写错了代码。
    """


class ChromaConfigError(VectorStoreError, ValueError):
    """**构造参数**问题：没给 ``db``、集合名为空。

    归 ``ValueError``：调用方改代码就能解决，重试无用。
    """


# ============================================================
# 依赖加载（延迟到真正要用时）
# ============================================================
def load_chromadb() -> Any:
    """导入并返回 ``chromadb`` 模块；未安装时抛 :class:`ChromaBackendError`。

    **刻意不在模块顶层 import**：``chromadb`` 是可选后端，默认后端
    （``vector_store_sql``）不该被它的依赖拖累。同理，报错信息直接给出
    「怎么装」，而不是让 ``ImportError`` 裸奔。
    """
    try:
        import chromadb  # noqa: PLC0415 - 可选依赖，必须延迟导入
    except ImportError as exc:  # pragma: no cover - 取决于本机是否安装
        raise ChromaBackendError(
            "未安装 chromadb，无法使用 ANN 向量后端。"
            "安装：pip install chromadb；"
            "或改用默认后端（不设 VECTOR_STORE / 设为 sql）"
        ) from exc
    return chromadb


def _build_settings(chromadb: Any) -> Any:
    """构造 chroma 客户端设置：**关掉匿名遥测**。

    chromadb 默认会把使用统计发往第三方（posthog）。本项目是**离线、内网、含简历与
    知识库正文**的场景，默认就该关掉——不能依赖「用户自己去关」。
    """
    return chromadb.Settings(anonymized_telemetry=False)


def open_client(*, path: Optional[str] = None) -> Any:
    """打开一个**持久化**的本地 chroma 客户端（目录不存在会自动创建）。

    :param path: 索引目录。``None`` → 环境变量 ``CHROMA_PATH`` → ``DEFAULT_PATH``。
    :raises ChromaBackendError: 未安装 ``chromadb`` 或目录不可用。
    """
    chromadb = load_chromadb()
    resolved = path or os.environ.get(ENV_PATH) or DEFAULT_PATH
    if not str(resolved).strip():
        raise ChromaConfigError(f"{ENV_PATH} 不能是空字符串（要么别设，要么给目录）")
    try:
        return chromadb.PersistentClient(
            path=str(resolved), settings=_build_settings(chromadb)
        )
    except Exception as exc:  # noqa: BLE001 - 目录不可写 / 版本不兼容都要给出可读信息
        raise ChromaBackendError(
            f"打开 chroma 索引目录失败（{resolved}）：{type(exc).__name__}: {exc}"
        ) from exc


# ============================================================
# 元数据编解码（纯函数，便于单测）
# ============================================================
def to_chroma_metadata(record: VectorRecord) -> Dict[str, Any]:
    """``VectorRecord`` → chroma 元数据（扁平标量 + ``_meta`` 保真载荷）。

    规则：

    - **标量**（``str`` / ``int`` / ``float`` / ``bool``）原样保留 → 可被 ``where`` 过滤
    - **非标量**（嵌套 dict / list / ``None``）**不**扁平化，只留在 ``_meta`` 里
      （chroma 的元数据不接受它们；``None`` 会让整条写入失败）
    - ``document_id`` / ``embedding_model`` 是**保留键**，用记录自身的字段覆盖，
      不让业务元数据里同名的键把过滤条件带偏
    """
    out: Dict[str, Any] = {
        META_PAYLOAD: json.dumps(
            dict(record.metadata or {}), ensure_ascii=False, sort_keys=True, default=str,
        ),
    }
    for key, value in (record.metadata or {}).items():
        name = str(key)
        if name in _RESERVED_META_KEYS:
            continue
        if isinstance(value, bool) or isinstance(value, (int, float, str)):
            out[name] = value
    if record.document_id is not None:
        out[META_DOCUMENT_ID] = int(record.document_id)
    out[META_MODEL] = record.model or ""
    return out


def from_chroma_metadata(
    meta: Any,
) -> Tuple[Dict[str, Any], str, Optional[int]]:
    """chroma 元数据 → ``(metadata, model, document_id)``。

    优先用 ``_meta`` 里的 JSON **保真还原**（嵌套结构、``None`` 都在）；
    没有 ``_meta``（人为写库 / 旧数据）时退回「扁平标量里除保留键以外的部分」，
    宁可少一点信息也不报错——检索路径不该因为一条元数据格式异常而整体失败。
    """
    if not isinstance(meta, Mapping):
        return {}, "", None

    model = str(meta.get(META_MODEL) or "")
    raw_document_id = meta.get(META_DOCUMENT_ID)
    document_id = (
        int(raw_document_id)
        if isinstance(raw_document_id, int) and not isinstance(raw_document_id, bool)
        else None
    )

    payload = meta.get(META_PAYLOAD)
    if isinstance(payload, str):
        try:
            decoded = json.loads(payload)
        except ValueError:
            decoded = None
        if isinstance(decoded, dict):
            return decoded, model, document_id

    fallback = {
        str(key): value for key, value in meta.items()
        if str(key) not in _RESERVED_META_KEYS
    }
    return fallback, model, document_id


def build_where(
    *,
    model: Optional[str] = None,
    document_id: Optional[int] = None,
    category: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """把三个过滤条件拼成 chroma 的 ``where`` 子句（全为空则返回 ``None``）。

    chroma 的语法要求多个条件必须包在 ``{"$and": [...]}`` 里，单个条件则直接给
    那个键值对——形状写错不会报错、只会**静默返回空结果**，所以单独抽成函数并单测。
    """
    conditions: List[Dict[str, Any]] = []
    if model is not None:
        conditions.append({META_MODEL: model})
    if document_id is not None:
        conditions.append({META_DOCUMENT_ID: int(document_id)})
    if category is not None:
        conditions.append({META_CATEGORY: category})
    if not conditions:
        return None
    if len(conditions) == 1:
        return conditions[0]
    return {"$and": conditions}


def _first_row(raw: Any, key: str) -> List[Any]:
    """取 chroma 返回值里「第一批查询」的那一行。

    ``query`` 的结果形状是 ``{"ids": [[...]], "embeddings": [[...]], ...}``
    （外层是「一批查询」，本模块一次只查一条向量）。键缺失或形状不对时返回 ``[]``，
    让调用方按「没有候选」处理，而不是在这里抛异常。
    """
    if not isinstance(raw, Mapping):
        return []
    block = raw.get(key)
    if not block:
        return []
    try:
        row = block[0]
    except (TypeError, IndexError, KeyError):
        return []
    return list(row) if row is not None else []


def _require_chunk_ids(value: Any, *, what: str = "chunk_ids") -> List[int]:
    """校验并归一「一组切片 id」（去重 + 保序）。

    入参口径与 ``SqlAlchemyVectorStore.load_records`` 一致：``None`` / 单个 ``int`` /
    字符串 / ``bool`` 一律拒绝（``bool`` 是 ``int`` 子类，必须显式挡），
    元素必须是可转成 ``int`` 的值且**不得是字符串**（``"12"`` 能 ``int()`` 成功，
    但那多半是调用方把 id 当文本传了）。

    空列表是**合法**的（= 没有要删的），返回 ``[]`` 让调用方短路，而不是报错。
    去重保序是刻意的：返回条数要能如实回答「移除了几条」，重复 id 只能算一条。
    """
    if value is None:
        raise VectorStoreInputError(f"{what} 不接受 None；要清空整个索引请用 reset()")
    if isinstance(value, (bool, str, bytes, bytearray, int)):
        raise VectorStoreInputError(
            f"{what} 需要一组 id，收到单个 {type(value).__name__}；请包成 [id]"
        )
    try:
        items = list(value)
    except TypeError as exc:
        raise VectorStoreInputError(
            f"{what} 需要一组可迭代的 id，收到 {type(value).__name__}"
        ) from exc

    seen: set = set()
    unique: List[int] = []
    for item in items:
        if isinstance(item, (bool, str, bytes, bytearray)):
            raise VectorStoreInputError(
                f"{what} 含非整数 id：{item!r}（{type(item).__name__}）"
            )
        try:
            chunk_id = int(item)
        except (TypeError, ValueError) as exc:
            raise VectorStoreInputError(f"{what} 含非整数 id：{item!r}") from exc
        if chunk_id not in seen:
            seen.add(chunk_id)
            unique.append(chunk_id)
    return unique


# ============================================================
# 后端实现
# ============================================================
class ChromaVectorStore(VectorStore):
    """``VectorStore`` 的真实 ANN 实现（chromadb / HNSW）。

    :param db: ``AsyncSession``。**必填**——切片行的权威副本仍在 ``knowledge_chunk``
        （本模块把 SQL 后端当「权威行读写器」组合进来，见模块文档「架构」）。
    :param client: 已建好的 chroma 客户端（依赖注入，便于单测塞
        ``chromadb.EphemeralClient()``）。``None`` → :func:`open_client` 按
        ``CHROMA_PATH`` 打开持久化客户端。
    :param path: 索引目录（``client`` 为 ``None`` 时才用）。
    :param collection_name: 集合名。``None`` → ``DEFAULT_COLLECTION``。
    :param model: 默认模型标识（记录自带 ``model`` 时以记录为准；同 SQL 后端）。
    :param base: 权威行读写器（依赖注入，便于单测）。``None`` →
        ``SqlAlchemyVectorStore(db, model=model)``。
    """

    name = "chroma"

    def __init__(
        self,
        db: Any,
        *,
        client: Any = None,
        path: Optional[str] = None,
        collection_name: Optional[str] = None,
        model: Optional[str] = None,
        base: Any = None,
        oversample: int = DEFAULT_OVERSAMPLE,
    ) -> None:
        if db is None:
            raise ChromaConfigError(
                "ChromaVectorStore 需要 db：切片行（含向量）的权威副本仍在 "
                "knowledge_chunk，向量库只做 ANN 索引"
            )
        if collection_name is not None and not str(collection_name).strip():
            raise ChromaConfigError("collection_name 不能是空字符串（要么别传，要么给名字）")
        if isinstance(oversample, bool) or not isinstance(oversample, int) or oversample < 1:
            raise ChromaConfigError(f"oversample 必须是 >=1 的整数，当前 {oversample!r}")

        self.db = db
        self.model = model
        self.collection_name = str(collection_name or DEFAULT_COLLECTION)
        self.oversample = oversample
        self._base = base if base is not None else SqlAlchemyVectorStore(db, model=model)
        self._client = client
        self._path = path
        self._collection: Any = None
        #: 索引维度缓存（一个集合只有一个维度；读到一次就够）。
        #: **只有 :meth:`reset`（丢弃并重建集合）才清空**——删行不改变集合维度，
        #: 见 :meth:`delete` 的说明。
        self._dimension: Optional[int] = None

    # --------------------------------------------------------
    # 客户端 / 集合句柄
    # --------------------------------------------------------
    def _collection_handle(self) -> Any:
        """拿到集合句柄（首次调用时建客户端 / 建集合）。

        .. note::
           chroma 的本地客户端是**同步**的，这里直接调用、**不**丢进线程池：
           - 本地嵌入式实现是进程内 SQLite + HNSW，单次操作在**微秒级**，
             阻塞事件循环的时间可忽略；
           - 反过来，chroma 的 SQLite 连接**不保证可跨线程**，
             引入线程池会带来「连接在 A 线程建、B 线程用」的隐患，
             属于用正确性换一点延迟，不划算。
           将来若改用 chroma 的 **HTTP 模式**（真网络调用），应当改走
           ``asyncio.to_thread``。
        """
        if self._collection is None:
            try:
                self._collection = self._client_handle().get_or_create_collection(
                    name=self.collection_name,
                    metadata={"hnsw:space": HNSW_SPACE},
                )
            except Exception as exc:  # noqa: BLE001 - 版本差异 / 目录只读都要可读信息
                raise ChromaBackendError(
                    f"打开 chroma 集合失败（{self.collection_name}）："
                    f"{type(exc).__name__}: {exc}"
                ) from exc
        return self._collection

    def _client_handle(self) -> Any:
        """拿到客户端句柄：注入的优先，否则按 ``CHROMA_PATH`` 打开并**缓存到实例**。

        为什么必须缓存而不是「每次现开一个」：:meth:`reset` 要调
        ``delete_collection``，这必须在**同一个客户端对象**上做。另开一个「同路径」
        的客户端虽然多半也能命中同一个 System，但那是 chroma 的实现细节
        （``EphemeralClient`` 的行为已被本项目踩过一次），不该被依赖。
        """
        if self._client is None:
            self._client = open_client(path=self._path)
        return self._client

    def _index_dimension(self) -> Optional[int]:
        """索引里向量的维度；索引为空时返回 ``None``。按实例缓存。

        为什么不用「捕获 chroma 的维度异常再翻译」：那要靠异常类名或**消息文本**判断，
        而项目约定是**按类型**分类异常、不嗅探文案。主动读一次维度（一条本地读）
        换来确定、可测的行为，更划算。
        """
        if self._dimension is None:
            got = self._collection_handle().get(limit=1, include=["embeddings"])
            vectors = got.get("embeddings") if isinstance(got, Mapping) else None
            if vectors is not None and len(vectors):
                first = vectors[0]
                if first is not None:
                    self._dimension = len(list(first))
        return self._dimension

    def _require_dimension(self, dimension: int, *, what: str) -> None:
        """校验维度与索引一致（索引为空时把当前维度定为索引维度）。"""
        current = self._index_dimension()
        if current is None:
            self._dimension = dimension
            return
        if current != dimension:
            raise VectorStoreDimensionError(
                f"{self.name}：{what} 是 {dimension} 维，而 ANN 索引是 {current} 维。"
                "一个 HNSW 索引只支持单一维度（图结构依赖固定维度），"
                "混用维度必须先换集合（或改用 vector_store_sql，它逐行记维度、可混存）"
            )

    # --------------------------------------------------------
    # 写入
    # --------------------------------------------------------
    async def add(self, records: Any) -> int:
        """写入一组「切片 + 向量」（upsert 语义），返回写入条数。

        顺序是刻意的：

        1. **先校验维度**（HNSW 只支持单维度）——必须在写库**之前**，
            否则会出现「权威行已写、索引没镜像」的半截状态；
        2. **再落权威行**（复用 ``SqlAlchemyVectorStore.add_returning_ids``，
            校验与字段覆盖范围与 SQL 后端**逐字一致**，只更新向量三列）；
        3. **最后从库里读回这些行**镜像进索引——索引内容取自**权威行**，
            不取调用方入参（入参在补向量场景下按契约本就被忽略）。
        """
        items = require_records(records)
        if not items:
            return 0

        # [1/3] 维度校验：同一批次内部一致 + 与索引一致
        dimensions = {len(record.vector) for record in items}
        if len(dimensions) > 1:
            raise VectorStoreDimensionError(
                f"{self.name}：同一批次里出现了多种维度 {sorted(dimensions)}，"
                "ANN 索引不支持（vector_store_sql 允许混存，本后端刻意报错而不是悄悄丢弃）"
            )
        self._require_dimension(dimensions.pop(), what="待写入的向量")

        # [2/3] 权威行（拿到每条记录真正落到的 chunk_id）
        chunk_ids = await self._base.add_returning_ids(items)

        # [3/3] 镜像进 ANN 索引
        await self._upsert_from_authority(chunk_ids)
        return len(items)

    async def _upsert_from_authority(self, chunk_ids: List[int]) -> int:
        """把 ``chunk_ids`` 对应的**权威行**写进索引，返回镜像条数。

        用 ``load_records`` 回表（按主键升序、自动去重、脏行跳过），因此
        「同一批次里重复的 ``chunk_id``」天然只留最后一条——与接口文档一致。
        """
        records = await self._base.load_records(chunk_ids)
        if not records:
            return 0
        self._collection_handle().upsert(
            ids=[str(record.chunk_id) for record in records],
            embeddings=[list(record.vector) for record in records],
            documents=[record.content for record in records],
            metadatas=[to_chroma_metadata(record) for record in records],
        )
        return len(records)

    # --------------------------------------------------------
    # 索引删除（**只动派生索引**，权威行由维护模块负责）
    # --------------------------------------------------------
    async def delete(self, chunk_ids: Any) -> int:
        """从 ANN 索引移除给定切片，返回**真实移除**的条数（幂等）。

        **只动派生索引，绝不动权威行**（``knowledge_chunk``）——权威行的删除由
        ``services/knowledge_maintenance.delete_document`` 显式执行。两者刻意分开：
        「删切片」与「同步索引」是两个决策，混在一个方法里会让
        「谁删了权威数据」不可追溯。

        语义：

        - 不在索引里的 id **直接跳过**（不报错）⇒ 重复调用安全（幂等）
        - 空入参返回 ``0``，**不碰索引**（不发起无意义的读）
        - 返回的是**索引里实际存在并被移除**的条数，不是入参长度——
          这样调用方才能如实回答「索引同步了几条」（而不是把入参长度当成功）

        为什么是**非接口方法**：``VectorStore`` 接口刻意只有
        ``add`` / ``search`` / ``count``（``__abstractmethods__`` 被多套件断言锁死）。
        「索引与权威行分离」只对 ANN 后端成立，写进接口会逼
        ``sqlalchemy`` / ``memory`` 实现两个空方法。同
        ``vector_store_sql.add_returning_ids`` / ``load_records`` 的先例。
        """
        ids = _require_chunk_ids(chunk_ids, what="delete 的 chunk_ids")
        if not ids:
            return 0
        collection = self._collection_handle()
        present = self._present_ids(collection, ids)
        if not present:
            return 0
        collection.delete(ids=present)
        # **刻意不清维度缓存**：删行不改变集合维度（实测删空后 chroma 仍按旧维度
        # 拒绝别的维度）。清掉缓存只会让本实例「忘记」一个 chroma 依然在强制的事实，
        # 于是下一次写入会绕过 `_require_dimension`、最后炸在 chroma 内部，
        # 把「混维度必须抛 VectorStoreDimensionError」的契约打掉。
        return len(present)

    async def reset(self) -> int:
        """**丢弃并重建集合**，返回清掉的条数（幂等，空索引返回 ``0``）。

        **绝不动权威行**——这是「真正重建派生索引」的第一步
        （见 ``services/knowledge_maintenance.rebuild_index`` 的 ``purge`` 参数）。
        为什么是「丢集合」而不是「逐行删空」：**集合的维度是粘的**。实测
        （chromadb 1.5.9）逐行删空后再写另一种维度，chroma 仍报
        ``InvalidArgumentError: Collection expecting embedding with dimension of 2``
        ——HNSW 的维度在集合创建时定死，删行**不**释放。只有 ``delete_collection``
        重建才真正回到「维度未定」的初始状态。

        由此得到本方法的契约（可被测试断言）：

        - ``reset()`` 之后本实例的行为**与全新实例完全一致**（维度缓存一并清空）
          ⇒ 换 Embedding 模型后重建索引不必手工删目录；
        - **即使索引为空也照样丢集合**——否则「被 :meth:`delete` 删空」的集合会把
          旧维度一直粘着，「等价于全新实例」就不成立了；
        - 代价是空索引调用会「建一次又丢一次」集合，可忽略（本地嵌入式、微秒级）。
        """
        collection = self._collection_handle()
        ids = self._all_ids(collection)
        # 即使为空也丢集合：见 docstring —— 「reset 后等价于全新实例」要靠这一步成立
        self._drop_collection()
        return len(ids)

    def _drop_collection(self) -> None:
        """丢弃集合（索引是**派生数据**，权威副本在 ``knowledge_chunk``）。

        句柄与维度缓存一并失效，下次使用会 ``get_or_create_collection`` 重建。
        """
        if self._collection is None:
            return
        try:
            self._client_handle().delete_collection(name=self.collection_name)
        except Exception as exc:  # noqa: BLE001 - 目录只读 / 版本差异都要可读信息
            raise ChromaBackendError(
                f"丢弃 chroma 集合失败（{self.collection_name}）："
                f"{type(exc).__name__}: {exc}"
            ) from exc
        finally:
            # 无论成功与否都别留旧句柄：集合可能已被删掉，句柄就悬空了
            self._collection = None
            self._dimension = None

    # ---- 内部：索引 id 读 ----
    @staticmethod
    def _present_ids(collection: Any, ids: List[int]) -> List[str]:
        """索引里**实际存在**的那些 id（chroma 的 id 恒为字符串）。

        缺失的 id 会被 chroma 静默忽略 ⇒ 天然幂等，不需要先 diff 再删。
        """
        got = collection.get(ids=[str(value) for value in ids])
        raw = got.get("ids") if isinstance(got, Mapping) else None
        if raw is None:
            return []
        return [str(value) for value in raw]

    @staticmethod
    def _all_ids(collection: Any) -> List[str]:
        """索引里的全部 id。``get()`` 的 ``ids`` 是普通 list（不是 numpy 数组）。"""
        got = collection.get()
        raw = got.get("ids") if isinstance(got, Mapping) else None
        if raw is None:
            return []
        return [str(value) for value in raw]

    # --------------------------------------------------------
    # 检索
    # --------------------------------------------------------
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
        """ANN 召回 + 精确重排，返回至多 ``top_k`` 条（含 ``score``）。

        流程：

        1. 查询维度与索引维度不一致 → 抛 ``VectorStoreDimensionError``
           （与 SQL 后端的可观测行为一致：**不静默返回空**）
        2. HNSW 召回 ``min(索引总数, top_k × oversample)`` 个候选，
           过滤条件（``model`` / ``document_id`` / ``category``）**下推到索引层**
        3. 候选按 ``chunk_id`` 升序排好（= SQL 后端的主键升序，同分兜底口径一致）
        4. 交给 :func:`~services.vector_store.select_matches` 精确排序 / 过滤 / 截断
        """
        query = require_vector(query_vector, what="query_vector")
        top_k = require_top_k(top_k)
        effective_model = model if model is not None else self.model

        index_dimension = self._index_dimension()
        if index_dimension is not None and index_dimension != len(query):
            raise VectorStoreDimensionError(
                f"{self.name}：查询向量是 {len(query)} 维，而 ANN 索引是 {index_dimension} 维，"
                "无法比较——多半是在用**另一个模型**的向量查询"
                "（不同模型的向量不可比，请确认 embedding_model 是否一致）"
            )

        collection = self._collection_handle()
        total = int(collection.count())
        if total == 0:
            return []                           # 索引为空 = 确实没有候选（不是维度问题）

        n_results = min(total, max(top_k, top_k * self.oversample))
        raw = collection.query(
            query_embeddings=[query],
            n_results=n_results,
            where=build_where(
                model=effective_model, document_id=document_id, category=category,
            ),
            include=["embeddings", "documents", "metadatas"],
        )

        return select_matches(
            _candidates_from_query(raw), query,
            top_k=top_k, min_score=min_score, store_name=self.name,
        )

    # --------------------------------------------------------
    # 统计
    # --------------------------------------------------------
    async def count(self) -> int:
        """**索引里可被检索**的向量数。

        正常情况等于「已向量化的切片数」（索引是权威行的镜像）；
        若两者不等，说明索引落后于权威行——**不在这里偷偷修复**，
        那属于删/重建数据的维护决策（见模块文档「刻意不做」）。
        """
        return int(self._collection_handle().count())

    # --------------------------------------------------------
    # 调试
    # --------------------------------------------------------
    def __repr__(self) -> str:  # pragma: no cover - 便于调试打印
        return (
            f"<{type(self).__name__} name={self.name!r} "
            f"collection={self.collection_name!r} dim={self._dimension!r}>"
        )


def _candidates_from_query(raw: Any) -> List[VectorRecord]:
    """把 chroma 的 ``query`` 结果转成 ``VectorRecord`` 列表（按 ``chunk_id`` 升序）。

    - 脏条目（id 不是整数 / 向量为空 / 含非数值）**跳过**，不让一条坏数据打挂整次检索
    - 排序目的是对齐 SQL 后端的「主键升序」候选顺序 → 同分时结果确定
    """
    ids = _first_row(raw, "ids")
    embeddings = _first_row(raw, "embeddings")
    documents = _first_row(raw, "documents")
    metadatas = _first_row(raw, "metadatas")

    candidates: List[VectorRecord] = []
    for position, raw_id in enumerate(ids):
        try:
            chunk_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        metadata, model, document_id = from_chroma_metadata(
            metadatas[position] if position < len(metadatas) else None
        )
        content = documents[position] if position < len(documents) else ""
        try:
            candidates.append(VectorRecord(
                vector=embeddings[position] if position < len(embeddings) else None,
                content=content or "",
                document_id=document_id,
                chunk_id=chunk_id,
                metadata=metadata,
                model=model,
            ))
        except VectorStoreInputError:
            continue
    candidates.sort(key=lambda record: record.chunk_id or 0)
    return candidates


__all__ = [
    "ChromaBackendError",
    "ChromaConfigError",
    "ChromaVectorStore",
    "DEFAULT_COLLECTION",
    "DEFAULT_OVERSAMPLE",
    "DEFAULT_PATH",
    "ENV_PATH",
    "HNSW_SPACE",
    "META_CATEGORY",
    "META_DOCUMENT_ID",
    "META_MODEL",
    "META_PAYLOAD",
    "build_where",
    "from_chroma_metadata",
    "load_chromadb",
    "open_client",
    "to_chroma_metadata",
]
