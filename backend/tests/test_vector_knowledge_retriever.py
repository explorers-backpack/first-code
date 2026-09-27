# -*- coding: utf-8 -*-
"""AI 面试知识库 · 真实知识检索器（VectorKnowledgeRetriever）自检

无需 pytest，直接运行：
    python backend/tests/test_vector_knowledge_retriever.py

不依赖本机 MySQL：SQL 后端跑在 SQLite 内存库上（StaticPool）。
不联网、不需要密钥；用**脚本化 embedder + 手写向量**算分，结果可精确断言。

覆盖范围
--------
1. **接口兼容**：签名与基类逐字一致（可无感替换 Mock / 空实现）
2. **query 构造**：topic → stage → 岗位名 的优先级、纯函数、确定性
3. **查询返回知识（测试要求 1）**：分数排序、source 提升、metadata 溯源字段
4. **无结果返回 []（测试要求 2）**：空库 / 低于阈值 / 无查询线索 / 模型不匹配
5. **错误处理（测试要求 3）**：embedder 与向量库的异常按类型收敛成两类，
   原始异常挂在 ``__cause__``；「检索不到」不是异常
6. **配置校验**：错误在**构造期**就报，不伪装成「检索不到」
7. **过滤 / 去重 / 截断**：top_k、min_score、category、document_id、空正文、重复正文
8. **端到端**：HashEmbeddingService + 内存向量库；结果直接喂给 Agent 的归一函数
9. **端到端（真实表）**：SqlAlchemyVectorStore + knowledge_chunk 扩展字段
10. **未接线 + 不破坏既有约定**：无生产调用方、零第三方依赖、既有模块未被改动
"""

import ast
import asyncio
import inspect
import os
import pathlib
import subprocess
import sys

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import KnowledgeChunk as OrmChunk  # noqa: E402
from models import KnowledgeDocument  # noqa: E402
from services import vector_knowledge_retriever as vkr  # noqa: E402
from services.embedding_service import (  # noqa: E402
    EmbeddingDimensionError,
    EmbeddingInputError,
    EmbeddingUnavailableError,
    HashEmbeddingService,
)
from services.knowledge_retriever import (  # noqa: E402
    KNOWLEDGE_CHUNK_FIELDS,
    MOCK_CHUNKS,
    KnowledgeChunk,
    KnowledgeRetriever,
    KnowledgeRetrieverError,
    MockKnowledgeRetriever,
)
from services.vector_knowledge_retriever import (  # noqa: E402
    RETRIEVAL_RESULT_FIELDS,
    SCORE_METADATA_KEY,
    RetrieverConfigError,
    RetrieverUnavailableError,
    VectorKnowledgeRetriever,
    VectorRetrieverError,
    build_query,
    chunk_to_result,
)
from services.vector_store import (  # noqa: E402
    InMemoryVectorStore,
    VectorRecord,
    VectorStoreDimensionError,
    VectorStoreError,
    VectorStoreInputError,
)
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

_PASSED = 0
_FAILED = 0

#: 手写正交向量：便于精确断言相似度（1.0 / 0.7071 / 0.0）
V_X = [1.0, 0.0, 0.0]
V_Y = [0.0, 1.0, 0.0]
V_XY = [1.0, 1.0, 0.0]


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(
        ("  [PASS] " if cond else "  [FAIL] ")
        + name
        + (f"  -> {detail}" if detail and not cond else "")
    )
    return cond


def _imported_modules(source: str) -> set:
    """用 AST 提取真正被 import 的模块名（不用子串匹配：docstring 会误伤）。"""
    names: set = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _production_files():
    """后端生产代码（排除 tests / __pycache__），用于「未接线」检查。"""
    files = []
    for pattern in ("*.py", "api/*.py", "services/*.py", "models/*.py",
                    "schemas/*.py", "utils/*.py"):
        for path in BACKEND_DIR.glob(pattern):
            if "__pycache__" in path.parts:
                continue
            files.append(path)
    return sorted(set(files))


async def _raises(coro, exc_type):
    """执行协程，返回 (是否抛了指定异常, 异常实例或说明)。"""
    try:
        await coro
    except exc_type as exc:
        return True, exc
    except Exception as exc:  # noqa: BLE001
        return False, f"抛了非预期异常 {type(exc).__name__}: {exc}"
    return False, "未抛异常"


# ============================================================
# 测试替身
# ============================================================
class _ScriptedEmbedder:
    """按脚本返回固定向量；记录调用；可选抛指定异常。

    刻意**不继承** ``EmbeddingService``——真实厂商实现可能只是鸭子类型，
    检索器必须靠 ``embed`` 这一个方法工作。
    """

    name = "scripted-v1"

    def __init__(self, vector=None, *, raises=None, raw=None, name=None):
        if name is not None:
            self.name = name
        self._vector = list(vector) if vector is not None else None
        self._raw = raw
        self.raises = raises
        self.calls = []

    async def embed(self, text):
        self.calls.append(text)
        if self.raises is not None:
            raise self.raises
        if self._raw is not None:
            return self._raw
        return list(self._vector)


class _BoomStore:
    """被调用就炸——用来断言「根本不该被调用」。"""

    def __init__(self):
        self.calls = 0

    async def search(self, *args, **kwargs):  # pragma: no cover - 不该走到
        self.calls += 1
        raise AssertionError("store.search 不应被调用")

    async def count(self):
        return 0


class _RaisingStore:
    """调用 ``search`` 时抛指定异常。"""

    def __init__(self, exc):
        self.exc = exc
        self.calls = 0

    async def search(self, *args, **kwargs):
        self.calls += 1
        raise self.exc


def _records(document_id=1, model="scripted-v1", *, source="manual://x", category="technical"):
    """三条手写向量记录（x / y / x+y），顺序固定，便于断言排序。"""
    return [
        VectorRecord(vector=V_X, content="第一条：向量 x", document_id=document_id,
                     metadata={"category": category, "source": source, "chunk_index": 0},
                     model=model),
        VectorRecord(vector=V_Y, content="第二条：向量 y", document_id=document_id,
                     metadata={"category": category, "source": source, "chunk_index": 1},
                     model=model),
        VectorRecord(vector=V_XY, content="第三条：向量 x+y", document_id=document_id,
                     metadata={"category": category, "source": source, "chunk_index": 2},
                     model=model),
    ]


def _scores(chunks):
    return [round(c.metadata[SCORE_METADATA_KEY], 9) for c in chunks]


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 面试知识库 · 真实知识检索器（VectorKnowledgeRetriever）自检")
    print("=" * 70)

    # ------------------------------------------------------------
    # [1] 接口兼容
    # ------------------------------------------------------------
    print("\n[1] 接口兼容（可无感替换 Mock / 空实现）")
    _check("services.vector_knowledge_retriever 可导入", hasattr(vkr, "VectorKnowledgeRetriever"))
    _check("★ 继承自 KnowledgeRetriever（isinstance 可用）",
           issubclass(VectorKnowledgeRetriever, KnowledgeRetriever))
    _check("★ retrieve 签名与基类**逐字一致**",
           list(inspect.signature(VectorKnowledgeRetriever.retrieve).parameters)
           == list(inspect.signature(KnowledgeRetriever.retrieve).parameters),
           f"{list(inspect.signature(VectorKnowledgeRetriever.retrieve).parameters)}"
           f" vs {list(inspect.signature(KnowledgeRetriever.retrieve).parameters)}")
    _check("  └ 参数就是 (self, job_info, topic, context)",
           list(inspect.signature(VectorKnowledgeRetriever.retrieve).parameters)
           == ["self", "job_info", "topic", "context"])
    _check("  └ retrieve 是 async（真实检索要访问外部服务）",
           inspect.iscoroutinefunction(VectorKnowledgeRetriever.retrieve))
    _check("★ 签名里没有 db（知识检索不耦合业务数据库）",
           "db" not in inspect.signature(VectorKnowledgeRetriever.retrieve).parameters
           and "db" not in inspect.signature(VectorKnowledgeRetriever.__init__).parameters)
    _check("source_name 与基类 / Mock 都不同",
           VectorKnowledgeRetriever.source_name == "vector"
           and VectorKnowledgeRetriever.source_name not in
           (KnowledgeRetriever.source_name, MockKnowledgeRetriever.source_name),
           VectorKnowledgeRetriever.source_name)
    _check("摊平视图的键恰为 (content, source, score)",
           RETRIEVAL_RESULT_FIELDS == ("content", "source", "score"),
           str(RETRIEVAL_RESULT_FIELDS))
    _check("  └ 分数键常量是 'score'", SCORE_METADATA_KEY == "score")

    # ------------------------------------------------------------
    # [2] query 构造
    # ------------------------------------------------------------
    print("\n[2] query 构造（build_query，纯函数）")
    _check("★ topic 优先（最精确的检索线索）",
           build_query({"job_name": "Java"}, "Redis 持久化", {"current_stage": "x"})
           == "Redis 持久化")
    _check("  └ topic 为空 → 回落到 context.current_stage",
           build_query({"job_name": "Java"}, "", {"current_stage": "技术深度"})
           == "技术深度")
    _check("  └ topic 空白串也算空（去首尾空白）",
           build_query({"job_name": "Java"}, "   ", {"current_stage": "技术深度"})
           == "技术深度")
    _check("  └ 都空 → 回落到岗位名（job_name）",
           build_query({"job_name": "Java 后端工程师"}, None, None)
           == "Java 后端工程师")
    _check("  └ 岗位字段按 job_name / title / name / position 顺序找",
           build_query({"position": "后端"}, None, None) == "后端"
           and build_query({"title": "T", "name": "N"}, None, None) == "T")
    _check("★ 全空 → 空串（没有可检索的线索）",
           build_query(None, None, None) == "" and build_query({}, "", {}) == "")
    _check("  └ 支持 ORM 行风格的对象（取属性，非 Mapping）",
           build_query(type("J", (), {"job_name": "Python 工程师"})(), None, None)
           == "Python 工程师")
    _check("  └ 列表值（如 skills）用空格连接",
           build_query({"job_name": ["Java", "Spring"]}, None, None) == "Java Spring")
    _check("  └ bool / dict / 对象 不会被 str() 成查询文本",
           build_query({"job_name": True}, None, None) == ""
           and build_query({"job_name": {"a": 1}}, None, None) == ""
           and build_query({"job_name": object()}, None, None) == "")
    _check("  └ 纯函数：不改入参",
           (lambda j: (build_query(j, "t", None), j == {"job_name": "Java"})[1])(
               {"job_name": "Java"}))
    _check("  └ 同输入同输出（确定性）",
           build_query({"job_name": "Java"}, "t", {"current_stage": "s"})
           == build_query({"job_name": "Java"}, "t", {"current_stage": "s"}))

    # ------------------------------------------------------------
    # [3] 查询返回知识（测试要求 1）
    # ------------------------------------------------------------
    print("\n[3] 查询返回知识（脚本化 embedder + 手写向量，精确断言）")
    embedder = _ScriptedEmbedder(V_X)
    store = InMemoryVectorStore(model=embedder.name)
    await store.add(_records())
    retriever = VectorKnowledgeRetriever(embedder, store, top_k=3)

    hits = await retriever.retrieve({"job_name": "Java"}, "Redis 持久化", {"current_stage": "x"})
    _check("★ 查到 3 条知识", len(hits) == 3, str(len(hits)))
    _check("★ 返回的都是 KnowledgeChunk 值对象",
           all(isinstance(c, KnowledgeChunk) for c in hits))
    _check("★ 按相似度降序：x(1.0) > x+y(0.707) > y(0.0)",
           _scores(hits) == [1.0, round(2 ** -0.5, 9), 0.0], str(_scores(hits)))
    _check("  └ 最相关的排第一",
           hits[0].content == "第一条：向量 x", hits[0].content)
    _check("★ 每次调用都是**新建**对象（改它不污染检索器）",
           await retriever.retrieve(None, "Redis 持久化", None) is not hits)
    _check("  └ 入参被记录在 queries 里（观测用，不影响输出）",
           retriever.queries == ["Redis 持久化", "Redis 持久化"], str(retriever.queries))
    _check("  └ query 真的传给了 embedder",
           embedder.calls == ["Redis 持久化", "Redis 持久化"], str(embedder.calls))

    _check("★ source 从切片 metadata 提升到顶层字段",
           hits[0].source == "manual://x", hits[0].source)
    _check("  └ 提升后 metadata 里不再重复留 source",
           "source" not in hits[0].metadata, str(hits[0].metadata))
    _check("★ metadata 带分数（供排序 / 观测）",
           hits[0].metadata[SCORE_METADATA_KEY] == 1.0,
           str(hits[0].metadata.get(SCORE_METADATA_KEY)))
    _check("  └ metadata 带 chunk_id / document_id（面试场景必须可溯源）",
           hits[0].metadata.get("chunk_id") == 1 and hits[0].metadata.get("document_id") == 1,
           str(hits[0].metadata))
    _check("  └ metadata 带 embedding_model（换模型后能看出这批向量是谁算的）",
           hits[0].metadata.get("embedding_model") == "scripted-v1",
           str(hits[0].metadata))
    _check("  └ 切片自身的元信息保留（category / chunk_index）",
           hits[0].metadata.get("category") == "technical"
           and hits[0].metadata.get("chunk_index") == 0,
           str(hits[0].metadata))
    _check("  └ content 原样保留（可取证，不悄悄改写正文）",
           hits[0].content == "第一条：向量 x")

    results = [chunk_to_result(c) for c in hits]
    _check("★ 摊平视图恰为 {content, source, score}（需求点名的返回形状）",
           all(tuple(r) == RETRIEVAL_RESULT_FIELDS for r in results),
           str([tuple(r) for r in results]))
    _check("  └ 摊平视图的分数与 metadata 逐位相等（只读投影，不做二次计算）",
           [r["score"] for r in results]
           == [c.metadata[SCORE_METADATA_KEY] for c in hits],
           f"{[r['score'] for r in results]} vs "
           f"{[c.metadata[SCORE_METADATA_KEY] for c in hits]}")
    _check("  └ 数值等于 45° 余弦（9 位小数；注意 1/sqrt(2) 与 2**-0.5 差 1 ULP）",
           [round(r["score"], 9) for r in results] == [1.0, round(2 ** -0.5, 9), 0.0],
           str([r["score"] for r in results]))
    _check("  └ chunk_to_result 是只读投影（不改原对象）",
           (lambda r: (r.__setitem__("score", 9.9), hits[0].metadata[SCORE_METADATA_KEY] == 1.0)[1])(
               chunk_to_result(hits[0])))
    _check("  └ 手工构造的片段（无 score）不报错，给 0.0",
           chunk_to_result(KnowledgeChunk(content="c"))["score"] == 0.0)

    # ------------------------------------------------------------
    # [4] 无结果返回 []（测试要求 2）
    # ------------------------------------------------------------
    print("\n[4] 无结果返回 []（不是异常）")
    _check("★ 空向量库 → []",
           await VectorKnowledgeRetriever(embedder, InMemoryVectorStore()).retrieve(
               None, "Redis", None) == [])
    _check("★ 全部低于 min_score → []（余弦上限是 1.0，阈值取 1.01）",
           await VectorKnowledgeRetriever(embedder, store, min_score=1.01).retrieve(
               None, "Redis", None) == [])
    _check("  └ min_score 恰好等于最高分时**保留**它",
           len(await VectorKnowledgeRetriever(embedder, store, min_score=1.0).retrieve(
               None, "Redis", None)) == 1)

    boom_store = _BoomStore()
    boom_embedder = _ScriptedEmbedder(V_X)
    silent = VectorKnowledgeRetriever(boom_embedder, boom_store)
    _check("★ 没有查询线索（topic / stage / 岗位名全空）→ []",
           await silent.retrieve(None, "", None) == [])
    _check("  └ 且**根本不调用** embedder / 向量库（省一次外部调用）",
           boom_embedder.calls == [] and boom_store.calls == 0,
           f"embedder={boom_embedder.calls} store={boom_store.calls}")
    _check("  └ 空 query 仍记进 queries（便于排查「为什么没知识」）",
           silent.queries == [""], str(silent.queries))

    other_model = InMemoryVectorStore(model="另一个模型")
    await other_model.add(_records(model="另一个模型"))
    _check("★ 库里只有**别的模型**的向量 → []（不拿不可比的向量算分）",
           await VectorKnowledgeRetriever(embedder, other_model).retrieve(
               None, "Redis", None) == [])
    _check("  └ 显式 model='' 关掉过滤后能查到",
           len(await VectorKnowledgeRetriever(
               embedder, other_model, model="").retrieve(None, "Redis", None)) == 3)

    # ------------------------------------------------------------
    # [5] 错误处理（测试要求 3）
    # ------------------------------------------------------------
    print("\n[5] 错误处理（按类型分流，原始异常挂 __cause__）")
    cases = [
        ("embedder 报「入参错」",
         _ScriptedEmbedder(raises=EmbeddingInputError("空文本")), _BoomStore(),
         RetrieverConfigError),
        ("embedder 报「维度错」",
         _ScriptedEmbedder(raises=EmbeddingDimensionError("维度 0")), _BoomStore(),
         RetrieverConfigError),
        ("embedder 报「不可用」",
         _ScriptedEmbedder(raises=EmbeddingUnavailableError("服务 503")), _BoomStore(),
         RetrieverUnavailableError),
        ("embedder 抛未预期异常",
         _ScriptedEmbedder(raises=ValueError("厂商 SDK 炸了")), _BoomStore(),
         RetrieverUnavailableError),
        ("embedder 返回非法向量（非数值序列）",
         _ScriptedEmbedder(raw="不是向量"), _BoomStore(), RetrieverConfigError),
        ("embedder 返回空向量",
         _ScriptedEmbedder(raw=[]), _BoomStore(), RetrieverConfigError),
        ("向量库报「维度不符」（换模型没重算）",
         _ScriptedEmbedder(V_X), _RaisingStore(VectorStoreDimensionError("维度 3 vs 64")),
         RetrieverConfigError),
        ("向量库报「入参错」",
         _ScriptedEmbedder(V_X), _RaisingStore(VectorStoreInputError("top_k 非法")),
         RetrieverConfigError),
        ("向量库报「其他领域错」",
         _ScriptedEmbedder(V_X), _RaisingStore(VectorStoreError("连接池耗尽")),
         RetrieverUnavailableError),
        ("向量库抛未预期异常",
         _ScriptedEmbedder(V_X), _RaisingStore(ConnectionError("连不上")),
         RetrieverUnavailableError),
    ]
    for label, emb, st, expected in cases:
        ok, info = await _raises(
            VectorKnowledgeRetriever(emb, st).retrieve(None, "Redis", None), expected)
        _check(f"★ {label} → {expected.__name__}",
               ok and isinstance(info, expected), str(info))

    ok, info = await _raises(
        VectorKnowledgeRetriever(_ScriptedEmbedder(raises=EmbeddingUnavailableError("503")),
                                 _BoomStore()).retrieve(None, "Redis", None),
        RetrieverUnavailableError)
    _check("★ 原始异常挂在 __cause__ 上（不丢根因）",
           ok and isinstance(info.__cause__, EmbeddingUnavailableError),
           str(getattr(info, "__cause__", None)))
    _check("  └ 错误信息里带 query 预览（便于排查是哪次检索）",
           ok and "Redis" in str(info), str(info))

    ok, info = await _raises(
        VectorKnowledgeRetriever(_ScriptedEmbedder(V_X),
                                 _RaisingStore(VectorStoreDimensionError("x"))).retrieve(
            None, "Redis", None),
        RetrieverConfigError)
    _check("  └ 维度不符的提示里点明「换了模型」（最常见的真实原因）",
           ok and "模型" in str(info), str(info))

    _check("★ 两类异常都能被领域基类一把捕获",
           issubclass(RetrieverConfigError, VectorRetrieverError)
           and issubclass(RetrieverUnavailableError, VectorRetrieverError)
           and issubclass(VectorRetrieverError, KnowledgeRetrieverError))
    _check("★ 两类互不误捕（类型分流才有意义）",
           issubclass(RetrieverConfigError, ValueError)
           and not issubclass(RetrieverConfigError, RuntimeError)
           and issubclass(RetrieverUnavailableError, RuntimeError)
           and not issubclass(RetrieverUnavailableError, ValueError))
    _check("  └ 空结果与异常的边界：查不到不是异常",
           await VectorKnowledgeRetriever(embedder, InMemoryVectorStore()).retrieve(
               None, "Redis", None) == [])

    # ------------------------------------------------------------
    # [6] 配置校验（构造期就报）
    # ------------------------------------------------------------
    print("\n[6] 配置校验（错误在构造期暴露，不伪装成「检索不到」）")
    bad_configs = {
        "embedder 缺 embed 方法": (object(), InMemoryVectorStore(), {}),
        "store 缺 search 方法": (embedder, object(), {}),
        "top_k=0": (embedder, store, {"top_k": 0}),
        "top_k=True": (embedder, store, {"top_k": True}),
        "top_k='3'": (embedder, store, {"top_k": "3"}),
        "min_score='x'": (embedder, store, {"min_score": "x"}),
        "min_score=True": (embedder, store, {"min_score": True}),
        "dedup=1（非 bool）": (embedder, store, {"dedup": 1}),
    }
    for label, (emb, st, kwargs) in bad_configs.items():
        try:
            VectorKnowledgeRetriever(emb, st, **kwargs)
            _check(f"★ 构造期拒绝：{label}", False, "竟然构造成功")
        except RetrieverConfigError:
            _check(f"★ 构造期拒绝：{label}", True)

    _check("  └ 合法配置可构造（min_score=None / dedup=False 都行）",
           VectorKnowledgeRetriever(embedder, store, min_score=None,
                                    dedup=False).source_name == "vector")

    # ------------------------------------------------------------
    # [7] 过滤 / 去重 / 截断
    # ------------------------------------------------------------
    print("\n[7] 过滤 / 去重 / 截断")
    _check("★ top_k 截断",
           len(await VectorKnowledgeRetriever(embedder, store, top_k=1).retrieve(
               None, "Redis", None)) == 1)
    _check("★ min_score 过滤",
           len(await VectorKnowledgeRetriever(embedder, store, min_score=0.5).retrieve(
               None, "Redis", None)) == 2)
    _check("★ category 过滤（透传给向量库）",
           len(await VectorKnowledgeRetriever(embedder, store, category="technical").retrieve(
               None, "Redis", None)) == 3
           and await VectorKnowledgeRetriever(embedder, store, category="job").retrieve(
               None, "Redis", None) == [])
    _check("★ document_id 过滤（透传给向量库）",
           await VectorKnowledgeRetriever(embedder, store, document_id=999).retrieve(
               None, "Redis", None) == [])

    mixed = InMemoryVectorStore(model=embedder.name)
    await mixed.add([
        VectorRecord(vector=V_X, content="有正文", document_id=1, model=embedder.name),
        VectorRecord(vector=V_X, content="   ", document_id=1, model=embedder.name),
        VectorRecord(vector=V_X, content="", document_id=1, model=embedder.name),
    ])
    kept = await VectorKnowledgeRetriever(embedder, mixed, top_k=5).retrieve(
        None, "Redis", None)
    _check("★ 空 / 纯空白正文的切片被跳过（占 Prompt 预算没意义）",
           len(kept) == 1 and kept[0].content == "有正文", str([c.content for c in kept]))

    dup = InMemoryVectorStore(model=embedder.name)
    await dup.add([
        VectorRecord(vector=V_X, content="同一段知识", document_id=1, model=embedder.name),
        VectorRecord(vector=V_XY, content="同一段  知识", document_id=2, model=embedder.name),
        VectorRecord(vector=V_Y, content="另一段知识", document_id=1, model=embedder.name),
    ])
    deduped = await VectorKnowledgeRetriever(embedder, dup, top_k=5).retrieve(
        None, "Redis", None)
    _check("★ 重复正文去重（忽略空白差异），保留最高分那条",
           len(deduped) == 2 and deduped[0].content == "同一段知识"
           and deduped[0].metadata["document_id"] == 1,
           str([(c.content, c.metadata.get("document_id")) for c in deduped]))
    _check("  └ dedup=False 时不去重",
           len(await VectorKnowledgeRetriever(embedder, dup, top_k=5, dedup=False).retrieve(
               None, "Redis", None)) == 3)

    _check("★ model_name 默认取 embedder.name（不同模型向量不可比）",
           VectorKnowledgeRetriever(embedder, store).model_name == "scripted-v1")
    _check("  └ 显式 model='' → None（关掉过滤）",
           VectorKnowledgeRetriever(embedder, store, model="").model_name is None)
    _check("  └ 显式 model='other' → 覆盖 embedder.name",
           VectorKnowledgeRetriever(embedder, store, model="other").model_name == "other")

    # ------------------------------------------------------------
    # [8] 端到端：HashEmbeddingService + 内存向量库
    # ------------------------------------------------------------
    print("\n[8] 端到端：HashEmbeddingService → 内存向量库 → 检索 → 喂给 Agent")
    emb = HashEmbeddingService(dimension=64)
    texts = [
        "Redis 持久化有 RDB 与 AOF 两种方式，RDB 是全量快照、AOF 记录写命令。",
        "判定链表是否有环用快慢指针，时间 O(n)、空间 O(1)。",
        "微服务拆分先按业务能力划边界，再用领域事件解耦。",
    ]
    sources = ["manual://handbook/redis", "manual://handbook/list", "manual://handbook/ms"]
    vectors = await emb.embed_batch(texts)
    e2e_store = InMemoryVectorStore(model=emb.name)
    await e2e_store.add([
        VectorRecord(vector=v, content=t, document_id=i + 1,
                     metadata={"category": "technical", "source": s, "chunk_index": i},
                     model=emb.name)
        for i, (t, s, v) in enumerate(zip(texts, sources, vectors))
    ])
    e2e = VectorKnowledgeRetriever(emb, e2e_store, top_k=3)
    # 用与第 1 条切片**完全相同**的文本查询 → 自相似度必然是 1.0（可精确断言）
    hits = await e2e.retrieve({"job_name": "Java 后端"}, texts[0], {"current_stage": "技术"})
    _check("★ 命中它自己且相似度 == 1（全链路接线正确）",
           hits[0].content == texts[0] and abs(hits[0].metadata["score"] - 1.0) < 1e-12,
           str([(c.content[:12], round(c.metadata["score"], 6)) for c in hits]))
    _check("  └ 结果按相似度降序",
           all(hits[i].metadata["score"] >= hits[i + 1].metadata["score"]
               for i in range(len(hits) - 1)),
           str(_scores(hits)))
    _check("  └ 溯源信息齐全（source 来自切片 metadata）",
           hits[0].source == sources[0], hits[0].source)

    from services.interview_agent import normalize_knowledge_context  # noqa: E402

    normalized = normalize_knowledge_context(hits)
    _check("★ 结果可直接喂给 Agent 的归一函数（Agent 无需任何修改）",
           len(normalized) == 3 and all(isinstance(line, str) for line in normalized),
           str(normalized)[:160])
    _check("  └ 归一后带正文与来源",
           texts[0] in normalized[0] and sources[0] in normalized[0], normalized[0][:120])
    _check("  └ 分数**没有**渗进 Prompt（不污染给大模型的文本）",
           all("score" not in line for line in normalized), str(normalized)[:160])

    from services.interview_core import current_topic, retrieve_knowledge  # noqa: E402

    via_core = await retrieve_knowledge(
        {"job_name": "Java"}, texts[2], {"current_stage": "技术"}, retriever=e2e)
    _check("★ 通过 Core 的唯一接线点调用 → 走同一条链路（可无感替换 Mock / 空实现）",
           len(via_core) == 3 and via_core[0].content == texts[2],
           str([(c.content[:12], round(c.metadata["score"], 6)) for c in via_core]))
    derived = current_topic({"priority_topics": ["Redis 持久化"]}, {"covered_topics": []})
    via_topic = await retrieve_knowledge(None, derived, None, retriever=e2e)
    _check("  └ 与 current_topic 推导出的 topic 串起来可用",
           derived == "Redis 持久化" and len(via_topic) == 3, str(derived))

    # ------------------------------------------------------------
    # [9] 端到端（真实表）：SqlAlchemyVectorStore
    # ------------------------------------------------------------
    print("\n[9] 端到端（真实表）：knowledge_chunk 扩展字段")
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with Session() as db:
        doc = KnowledgeDocument(title="Redis 手册", content="RDB 与 AOF",
                                category="technical", source="manual://handbook/redis")
        db.add(doc)
        await db.commit()
        await db.refresh(doc)
        doc_id = doc.id

        sql_store = SqlAlchemyVectorStore(db, model=emb.name)
        await sql_store.add([
            VectorRecord(vector=v, content=t, document_id=doc_id,
                         metadata={"category": "technical", "source": s, "chunk_index": i},
                         model=emb.name)
            for i, (t, s, v) in enumerate(zip(texts, sources, vectors))
        ])
        sql_retriever = VectorKnowledgeRetriever(emb, sql_store, top_k=3)
        sql_hits = await sql_retriever.retrieve(None, texts[1], None)
        _check("★ 走真实表也能检索到知识",
               len(sql_hits) == 3 and sql_hits[0].content == texts[1],
               str([(c.content[:12], round(c.metadata["score"], 6)) for c in sql_hits]))
        _check("  └ 相似度 == 1（同一条切片）",
               abs(sql_hits[0].metadata["score"] - 1.0) < 1e-12,
               str(sql_hits[0].metadata["score"]))
        _check("  └ chunk_id 是数据库真主键（可溯源到行）",
               isinstance(sql_hits[0].metadata.get("chunk_id"), int)
               and sql_hits[0].metadata["chunk_id"] > 0,
               str(sql_hits[0].metadata))
        mirror_hits = await VectorKnowledgeRetriever(emb, e2e_store, top_k=3).retrieve(
            None, texts[1], None)
        _check("  └ 与内存向量库的检索结果一致（换后端不换语义）",
               [(c.content, round(c.metadata["score"], 9)) for c in sql_hits]
               == [(c.content, round(c.metadata["score"], 9)) for c in mirror_hits],
               f"sql={_scores(sql_hits)} mem={_scores(mirror_hits)}")
        _check("  └ 表里确实写进了向量三列",
               all(row.embedding is not None and row.embedding_model == emb.name
                   for row in (await db.execute(
                       select(OrmChunk).order_by(OrmChunk.id))).scalars().all()))

    await engine.dispose()

    # ------------------------------------------------------------
    # [10] 未接线 + 不破坏既有约定
    # ------------------------------------------------------------
    print("\n[10] 未接线 + 不破坏既有约定")
    # 本模块的消费者必须**收口为唯一组装器** ``knowledge_rag``：
    # 面试流程（Core / Service / Agent）**一律不直接**拿检索器，
    # 只通过 ``interview_core.resolve_retriever`` 间接决定——「谁把 RAG 接上线」只有一个答案。
    consumers = []
    for path in _production_files():
        if path.name == "vector_knowledge_retriever.py":
            continue
        imports = _imported_modules(path.read_text(encoding="utf-8"))
        if "vector_knowledge_retriever" in imports \
                or "services.vector_knowledge_retriever" in imports:
            consumers.append(path.relative_to(BACKEND_DIR).as_posix())
    _check("★ 消费者**收口为唯一组装器** knowledge_rag（面试流程不直接用它）",
           consumers == ["services/knowledge_rag.py"], str(consumers))
    _check("  └ 面试流程模块（interview_*）都不直接 import 本模块",
           not any(rel.startswith("services/interview") for rel in consumers), str(consumers))

    src = (BACKEND_DIR / "services" / "vector_knowledge_retriever.py").read_text(encoding="utf-8")
    all_imports = _imported_modules(src)
    top_level = {m.split(".")[0] for m in all_imports}
    _check("★ 零第三方依赖（顶层只有 __future__ / collections / math / typing / services）",
           top_level == {"__future__", "collections", "math", "typing", "services"},
           str(sorted(top_level)))
    _check("  └ 不 import 任何具体后端 / 数据库 / 框架",
           not ({"sqlalchemy", "models", "database", "fastapi", "pydantic", "main",
                 "vector_store_sql", "services.vector_store_sql", "numpy",
                 "chromadb", "faiss", "milvus", "qdrant"} & all_imports),
           str(sorted(all_imports)))
    _check("  └ 尤其**不 import** vector_store_sql（SQLAlchemy 不能被拉进来）",
           not any("vector_store_sql" in m for m in all_imports))

    env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
    code = (
        "import sys; sys.path.insert(0, r'%s');"
        "import services.vector_knowledge_retriever as m;"
        "leak = [x for x in ('sqlalchemy','fastapi','aiomysql','pymysql','models',"
        "'database','numpy','chromadb','faiss','openai','requests') if x in sys.modules];"
        "print('LEAK:' + ','.join(leak));"
        "print('OK:' + m.VectorKnowledgeRetriever.source_name)" % str(BACKEND_DIR)
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                          env=env, encoding="utf-8", cwd=str(BACKEND_DIR))
    _check("子进程（无 DATABASE_URL）可导入本模块",
           proc.returncode == 0 and "OK:vector" in (proc.stdout or ""),
           (proc.stderr or "")[-300:])
    leak_line = next((line for line in (proc.stdout or "").splitlines()
                      if line.startswith("LEAK:")), None)
    leaked = (leak_line or "LEAK:?").split(":", 1)[1].strip()
    _check("  └ 导入后无第三方 / 基础设施模块泄漏进 sys.modules",
           leaked == "", leaked or "（未取到 LEAK 行）")

    for rel in ("services/knowledge_retriever.py", "services/interview_agent.py",
                "services/interview_core.py", "services/embedding_service.py",
                "services/vector_store.py"):
        imports = _imported_modules((BACKEND_DIR / rel).read_text(encoding="utf-8"))
        _check(f"  └ {rel} 未被改动（不 import 本模块）",
               not any("vector_knowledge_retriever" in m for m in imports))

    _check("★ knowledge_retriever.py 的三键契约**未被改动**",
           KNOWLEDGE_CHUNK_FIELDS == ("content", "source", "metadata"),
           str(KNOWLEDGE_CHUNK_FIELDS))
    _check("  └ MockKnowledgeRetriever 仍在（测试仍可用）",
           MockKnowledgeRetriever is not None and len(MOCK_CHUNKS) == 3
           and len(await MockKnowledgeRetriever().retrieve(None, None, None)) == 3)
    _check("  └ 空实现（基类）仍返回 []（默认不接知识库）",
           await KnowledgeRetriever().retrieve(None, None, None) == [])

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
