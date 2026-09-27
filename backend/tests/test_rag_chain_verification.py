# -*- coding: utf-8 -*-
"""AI 面试 · **真实 RAG 检索链路端到端验证**（SQL 后端 vs Chroma ANN 后端）

无需 pytest，直接运行：
    python backend/tests/test_rag_chain_verification.py

本套件是**验证**性质的：不新增业务能力，只对已完成的链路做取证。
不依赖本机 MySQL（SQLite 内存库 + StaticPool）；不联网、不需要密钥。

被验证的完整调用链
------------------
::

    query（由 Core 的 current_topic 推导出的 topic）
      │
      │  build_query(job, topic, context)          ← 纯函数，只取一个来源
      ▼
    query 文本 ──为空──▶ []（没有可检索的线索 ≠ 故障）
      │  EmbeddingService.embed(query)
      ▼
    query 向量
      │  VectorStore.search(vector, top_k, model, min_score)
      ▼
    select_matches(candidates, vector)              ← 全项目唯一排序口径
      │  跳过维度不符 → (-score, 位置) 稳定排序 → min_score → top_k
      ▼
    List[VectorMatch]
      │  VectorKnowledgeRetriever._to_chunks（过滤空正文 + 去重 + score 落 metadata）
      ▼
    List[KnowledgeChunk]  {content, source, metadata{score, chunk_id, …}}
      │  interview_core._gather_knowledge → generate_candidate_question
      ▼
    InterviewAgent.generate_question(..., knowledge_context=[...])
      │  render_question_prompt → question_knowledge.txt（「参考知识：」小节）
      ▼
    Prompt（知识正文出现在其中）→ Spark → QuestionValidator

覆盖范围（对应用户点名的 5 条测试 + 1 条对比）
----------------------------------------------
[1] 完整调用链取证：每一环的**真实实现**都被走到（运行时 spy，不是读代码猜）
[2] **插入真实知识 Chunk**（测试 1）：走 `KnowledgeImportPipeline`，不是手搓记录
[3] **使用相关 query 检索**（测试 2）：命中且目标切片排第一
[4] **验证 score 排序**（测试 3）：降序 + 与手算 `cosine_similarity` 逐位一致 + 同分兜底
[5] **验证 top_k**（测试 4）：1 / 2 / 超过候选数 / 非法值
[6] **验证 min_score 过滤**（测试 5）：卡在两档之间 / 上界之外 / 与不过滤一致
[7] **SQL 后端 vs Chroma 后端结果对比**：同一批 chunk_id、同一 query，
    逐字段比对（顺序 / chunk_id / score / content / metadata），并在**检索器层**再比一次
[8] 语义能力验证：确定性概念 Embedding 替身，query 与目标切片**零 token 重叠**
    仍然命中——证明「检索层是语义就绪的」，瓶颈只在 Embedding 模型
[9] 端到端到 InterviewAgent：知识正文确实进了 Prompt（Mock Spark，不联网）
[10] 边界取证：本轮未改动 InterviewCore / Agent 接口 / Validator

.. warning::
   **当前环境未配置任何 ``EMBEDDING_*``**（``backend/.env`` 里只有 ``DATABASE_URL`` 与
   ``SPARK_*``），因此 ``knowledge_rag.default_embedder()`` 返回的是
   :class:`~services.embedding_service.HashEmbeddingService`——**离线哈希占位，
   不是语义向量**。所以 [3]~[7] 里的「相关」是**词面（token 共享）相关**，
   不是语义相关；真正的语义相关性由 [8] 用确定性替身单独取证。
   要得到真实语义检索，只需配好 ``EMBEDDING_API_KEY``（链路一行都不用改）。
"""

import ast
import asyncio
import inspect
import json
import os
import pathlib
import re
import shutil
import sys
import tempfile
from typing import Any, Dict, List, Optional, Tuple

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
import regression_env  # noqa: E402,F401  钉住离线 Embedding + RAG 阈值（回归不受 .env 影响）
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
from models import Job, KnowledgeChunk as OrmChunk, KnowledgeDocument, User  # noqa: E402
from schemas.interview import SessionCreateRequest  # noqa: E402
from services import interview_agent as ag  # noqa: E402
from services import interview_core, interview_service, knowledge_rag  # noqa: E402
from services.embedding_service import (  # noqa: E402
    EmbeddingService,
    HashEmbeddingService,
    tokenize,
)
from services.knowledge_import_pipeline import (  # noqa: E402
    STATUS_OK,
    KnowledgeImportPipeline,
)
from services.knowledge_retriever import KnowledgeChunk, KnowledgeRetriever  # noqa: E402
from services.vector_knowledge_retriever import (  # noqa: E402
    SCORE_METADATA_KEY,
    VectorKnowledgeRetriever,
)
from services.vector_store import (  # noqa: E402
    VectorMatch,
    VectorRecord,
    VectorStore,
    cosine_similarity,
    select_matches,
)
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

_PASSED = 0
_FAILED = 0

# ============================================================
# 测试数据：5 篇真实知识文档（走入库 Pipeline，不是手搓切片）
# ============================================================
#: ``(title, category, source, content)``。前四篇各产出 1 片；
#: 最后一篇（项目经验）较长，会切成 2 片——用来验证多切片文档的溯源与 top_k。
KNOWLEDGE_DOCS: Tuple[Tuple[str, str, str, str], ...] = (
    (
        "Redis 持久化机制",
        "technical",
        "manual://handbook/redis-persistence",
        "Redis 持久化有 RDB 与 AOF 两种方式。RDB 是某一时刻的全量快照，文件紧凑、"
        "恢复快，但两次快照之间宕机会丢数据；AOF 记录每一条写命令，可通过 "
        "appendfsync 控制落盘频率，数据更安全但文件更大、恢复更慢。生产上常用两者混合。",
    ),
    (
        "MySQL 索引原理",
        "technical",
        "manual://handbook/mysql-index",
        "MySQL 的 InnoDB 使用 B+ 树索引。联合索引遵循最左前缀原则，范围查询会让其后的列"
        "失去索引效果。回表是二级索引查到主键后再去聚簇索引取整行，覆盖索引可以避免回表。"
        "用 EXPLAIN 看 type 与 key 判断索引是否生效。",
    ),
    (
        "JVM 垃圾回收",
        "technical",
        "manual://handbook/jvm-gc",
        "JVM 的堆分为新生代与老年代。新生代用复制算法，Eden 与两个 Survivor 之间来回复制；"
        "老年代用标记整理。G1 把堆切成 Region，按回收收益排序，可以设置停顿目标。"
        "频繁 Full GC 通常意味着内存泄漏或大对象直接进入老年代。",
    ),
    (
        "Kafka 重复消费治理",
        "technical",
        "manual://handbook/kafka-idempotent",
        "Kafka 的消费者通过位移提交来记录消费进度。重复消费的常见原因是自动提交位移与业务"
        "处理不在同一个事务里。实现幂等要靠业务侧的唯一键去重，或者把位移提交与业务写库放进"
        "同一个事务。分区数决定最大并行度。",
    ),
    (
        "订单中台缓存治理实践",
        "project",
        "resume://project/order-middle",
        "订单中台在双十一前出现过缓存击穿导致数据库被打挂的事故，当时核心下单接口的"
        "P99 冲到了 3 秒以上，触发了熔断。复盘后我们做了四件事。"
        "第一，对热点商品做逻辑过期，value 里带一个过期时间戳，命中后先返回旧值再异步"
        "重建缓存，避免同一时刻大量请求同时回源数据库。"
        "第二，对空结果做短 TTL 的空值缓存，把不存在的商品 id 也缓存 30 秒，挡住穿透流量；"
        "后来又在接入层加了一层布隆过滤器，把明显不存在的请求直接拦掉。"
        "第三，给缓存过期时间加随机扰动，在基础 TTL 上叠加 0 到 300 秒的随机值，"
        "防止大批 key 在同一秒失效引发雪崩。"
        "第四，把本地缓存与 Redis 组成两级缓存，本地缓存扛住单机热点，Redis 保证集群一致；"
        "同时在更新数据库后主动删除缓存而不是更新缓存，降低并发下的脏数据概率。"
        "第五，补了一套缓存健康度监控，把命中率、大 key 数量、慢查询、回源 QPS 四个指标"
        "做成实时告警，超过阈值就自动降级为本地缓存兜底，宁可少返回一点数据也不能让"
        "数据库被打挂。"
        "改造上线后数据库 QPS 从峰值 8 万降到 1.2 万，P99 从 480ms 降到 90ms，"
        "缓存命中率从 76% 提升到 96%。"
        "事后复盘时我最大的体会是，缓存治理真正的难点不在选哪个组件，而在于把失效策略、"
        "降级预案和监控告警当成一个整体来设计；单独优化任何一环，遇到真实流量峰值时"
        "都还是会出问题。"
        "另外还有一条经验：压测一定要用接近生产的流量模型，我们第一次压测只覆盖了"
        "均匀分布的商品 id，完全没暴露出热点 key 的问题，后来改成按真实成交分布造数，"
        "才把逻辑过期这条路走通。",
    ),
)

#: 词面相关 query（与目标切片共享 token）→ 期望命中的 ``source``
LEXICAL_QUERIES: Tuple[Tuple[str, str], ...] = (
    ("MySQL 索引优化", "manual://handbook/mysql-index"),
    ("Redis 持久化 RDB AOF", "manual://handbook/redis-persistence"),
    ("Kafka 重复消费 幂等", "manual://handbook/kafka-idempotent"),
)

#: [8] 用的「语义」语料：与 query **零 token 重叠**，但同属「数据库性能」这一概念。
SEMANTIC_TARGET_CONTENT = (
    "B+ 树把数据按页组织，非叶子节点只存键值，因此树高很低，"
    "一次定位通常只需三四次磁盘 IO，这正是它比二叉树更适合做磁盘索引的原因。"
)
SEMANTIC_OTHER_CONTENTS: Tuple[str, ...] = (
    "G1 把堆切成 Region，按回收收益排序，可以设置停顿目标。",
    "消费者通过位移提交记录进度，重复消费要靠唯一键去重。",
    "RDB 是全量快照，AOF 记录写命令，可通过 appendfsync 控制落盘频率。",
)
#: 该 query 与 ``SEMANTIC_TARGET_CONTENT`` **没有一个公共 token**（套件里会断言）
SEMANTIC_QUERY = "如何提升慢查询性能"

#: 注入的上下文 / 计划（跳过读库，保证 topic 可预期）
CONTEXT: Dict[str, Any] = {
    "current_stage": "technical",
    "asked_questions": [],
    "covered_topics": [],
    "weak_topics": [],
    "current_question_no": 1,
    "total_questions": 5,
}
PLAN: Dict[str, Any] = {
    "interview_type": "technical",
    "difficulty": "mid",
    "total_questions": 5,
    "target_topics": ["Java", "Spring Boot", "Redis"],
    "priority_topics": ["Redis 持久化", "MySQL 索引"],
    "resume_focus_points": ["订单中台"],
}
#: ``current_topic(PLAN, CONTEXT)`` 的结果 —— 就是检索用的 query 文本
TOPIC = "Redis 持久化"

JOB_SKILLS = "Python,MySQL,Redis,Kafka,Docker"

QUESTION_JSON = json.dumps(
    {
        "question": "请介绍一下你在项目中是如何治理缓存击穿的？",
        "question_type": "project",
        "topic": "Redis 持久化",
        "difficulty": "mid",
        "expected_points": ["逻辑过期", "空值缓存", "随机扰动"],
        "reason": "考察缓存治理的实战经验",
    },
    ensure_ascii=False,
)

#: **向量层**：Core / Agent / Validator 都不该在**顶层** import
#: （那会破坏「延迟导入」与「零 DB 耦合」，见各自模块文档）。
FORBIDDEN_VECTOR_LAYER = (
    "chromadb",
    "services.vector_store",
    "services.vector_store_sql",
    "services.vector_store_chroma",
    "services.knowledge_retriever",
    "services.vector_knowledge_retriever",
    "services.knowledge_rag",
    "services.embedding_service",
    "services.embedding_provider",
    "services.embedding_provider_spark",
    "services.knowledge_import_pipeline",
)

#: **数据层**：Core 顶层不许有（它必须能脱离 ``DATABASE_URL`` 被 import）。
FORBIDDEN_DATA_LAYER = ("sqlalchemy", "database", "models", "aiomysql", "pymysql")

#: Validator 是「零第三方依赖」的纯函数模块：除标准库外一律不许。
FORBIDDEN_THIRD_PARTY = ("fastapi", "pydantic")

#: **已知的分层例外**（刻意保留，不是缺陷）：``interview_core.build_report``
#: 直接抛 ``HTTPException`` 沿用旧行为（见 SKILL.md「已知分层例外」）。
CORE_KNOWN_EXCEPTION = ("fastapi",)

#: 跨后端比较时的 **score 容差**。
#:
#: 为什么不能逐位相等：**ANN 索引把向量按 float32 存取**。chroma 回读的向量有
#: ~12/256 个分量与库里的 float64 不同（实测），于是「重算余弦」会带上 ~1e-8 的漂移。
#: 当两条候选的分数在**数值上等于 0**（近正交）时，这个漂移足以翻转顺序：
#: 实测 SQL 侧 ``#4 = +1.04e-17`` 而 chroma 侧 ``#4 = -3.95e-9``。
#: 该量级比任何有意义的分数差（0.3167 vs 0.0474）小 6 个数量级，因此：
#: **排序规则一致**成立，但「近似并列项」的顺序不作逐位要求。
SCORE_TOL = 1e-6


# ============================================================
# 断言 / 通用工具
# ============================================================
def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(
        ("  [PASS] " if cond else "  [FAIL] ") + name
        + (f"  -> {detail}" if detail and not cond else "")
    )
    return cond


def _brief(items: Any, limit: int = 3) -> str:
    """把结果列表压成可读的短串（用于失败信息）。"""
    if not isinstance(items, (list, tuple)):
        return repr(items)
    parts = []
    for item in list(items)[:limit]:
        if isinstance(item, VectorMatch):
            parts.append(f"#{item.chunk_id}:{round(item.score, 4)}")
        elif isinstance(item, KnowledgeChunk):
            parts.append(
                f"#{item.metadata.get('chunk_id')}:"
                f"{round(item.metadata.get(SCORE_METADATA_KEY, 0.0), 4)}"
            )
        else:
            parts.append(repr(item))
    if len(items) > limit:
        parts.append(f"…(+{len(items) - limit})")
    return "[" + ", ".join(parts) + "]"


def _match_keys(matches: List[VectorMatch]) -> List[Tuple[Optional[int], float]]:
    """把检索结果压成 ``[(chunk_id, score)]``（**不取整**，便于容差比较）。"""
    return [(m.chunk_id, m.score) for m in matches]


def _chunk_pairs(chunks: List[KnowledgeChunk]) -> List[Tuple[Optional[int], float]]:
    """检索器层的 ``(chunk_id, score)`` 序列（score 取自 ``metadata``）。"""
    return [
        (c.metadata.get("chunk_id"), float(c.metadata.get(SCORE_METADATA_KEY, 0.0)))
        for c in chunks
    ]


async def _raises(coro: Any, exc_type: type) -> Tuple[bool, Any]:
    """执行协程并断言抛出指定异常 → ``(是否命中, 异常对象)``。"""
    try:
        await coro
    except exc_type as exc:
        return True, exc
    except BaseException as exc:  # noqa: BLE001 - 用于报告「抛了别的异常」
        return False, exc
    return False, None


def _module_names(source: str, *, top_level_only: bool = True) -> set:
    """收集 import 的模块名，返回**点号全名**（``services.vector_store_sql``）。

    两个容易踩的坑，这里一次性避开：

    1. **只看顶层**（``ast.parse(src).body``，不要 ``ast.walk``）——否则函数体内的
       延迟导入也会被算进来，判「是否延迟导入」的断言恒为假；
    2. **必须用点号全名**——``from services.X import Y`` 的首段只是 ``services``，
       用首段匹配会**永远匹配不到**，守卫变成恒真的假守卫。
       ``from services import X`` 形态则补上 ``services.X``。
    """
    tree = ast.parse(source)
    nodes = tree.body if top_level_only else ast.walk(tree)
    found = set()
    for node in nodes:
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.level == 0:
                found.add(node.module)
                for alias in node.names:
                    found.add(f"{node.module}.{alias.name}")
    return found


def _offenders(source: str, forbidden: Tuple[str, ...]) -> List[str]:
    """顶层 import 里命中了禁列表的模块（**点号全名**前缀匹配）。"""
    names = _module_names(source, top_level_only=True)
    return sorted(
        name for name in names
        if any(name == bad or name.startswith(bad + ".") for bad in forbidden)
    )


def _compare_pairs(
    left: List[Tuple[Optional[int], float]],
    right: List[Tuple[Optional[int], float]],
    *,
    tol: float = SCORE_TOL,
) -> List[str]:
    """比较两个后端的 ``(chunk_id, score)`` 序列，返回不一致说明（空 = 一致）。

    规则（**比"逐位相等"更准确**）：

    A. 条数必须相同；
    B. **分数序列（降序）逐项近似相等**——这是「排序口径一致」的核心；
    C. 同一位置上 id 不同时，两条分数之差必须 ``<= tol``
       ——即只允许「近似并列项」互换顺序；
    D. **显著命中集合**（分数 ``> tol``）必须完全一致
       ——一个后端漏掉一条明显相关的命中，是真正的语义差异。

    为什么 C 要留口子：ANN 索引按 **float32** 存取向量，回读重算的余弦有 ~1e-8 漂移
    （见 :data:`SCORE_TOL`）。当分数在数值上等于 0（近正交）时，
    这个漂移既可能翻转顺序，也可能改变 **top_k 截断处**由谁入选。

    留口子**不会掩盖真问题**：真正的差异（漏掉 top-1、把 0.31 排到 0.05 之后、
    某条相关命中消失）分数差都远超 ``tol``，规则 B/C/D 照样抓得出来。
    """
    if len(left) != len(right):
        return [f"条数不同：{len(left)} vs {len(right)}"]
    problems: List[str] = []

    for position, (mine, other) in enumerate(zip(left, right), start=1):
        if abs(mine[1] - other[1]) > tol:
            problems.append(
                f"第 {position} 名的 score 差 {abs(mine[1] - other[1]):.3e}"
                f"（{mine[1]:.6f} vs {other[1]:.6f}）"
            )
    for position, (mine, other) in enumerate(zip(left, right), start=1):
        if mine[0] != other[0] and abs(mine[1] - other[1]) > tol:
            problems.append(
                f"第 {position} 名不同：#{mine[0]}({mine[1]:.6f}) "
                f"vs #{other[0]}({other[1]:.6f})"
            )
    left_strong = {cid for cid, score in left if score > tol}
    right_strong = {cid for cid, score in right if score > tol}
    if left_strong != right_strong:
        problems.append(
            f"显著命中集合不同：{sorted(left_strong)} vs {sorted(right_strong)}"
        )
    return problems


def _max_drift(
    left: List[Tuple[Optional[int], float]], right: List[Tuple[Optional[int], float]]
) -> float:
    """同 id 的 score 最大偏差（用于量化 float32 漂移）。"""
    left_scores = dict(left)
    return max(
        (abs(left_scores[cid] - score) for cid, score in right if cid in left_scores),
        default=0.0,
    )


# ============================================================
# 测试替身
# ============================================================
class MockSpark:
    """按顺序吐回复；超出即报错，用于断言调用次数。"""

    def __init__(self, *replies: Any) -> None:
        self.replies = list(replies)
        self.calls: List[str] = []

    @property
    def call_count(self) -> int:
        return len(self.calls)

    async def chat_async(self, message: str) -> str:
        self.calls.append(message)
        if not self.replies:
            raise AssertionError("Mock Spark 被调用次数超出预期")
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


class RecordingEmbedder(HashEmbeddingService):
    """真实哈希实现 + 记录「收到什么文本、产出什么向量」。

    继承**真实实现**（而不是另写一份），所以它算出来的向量就是链路上真正用的向量，
    可以据此断言「embed 产出的向量 == search 收到的向量」。
    """

    name = "hash-local-recording"

    def __init__(self) -> None:
        super().__init__()
        #: ``[(text, vector)]``
        self.calls: List[Tuple[str, List[float]]] = []

    async def _embed_one(self, text: str) -> List[float]:
        vector = await super()._embed_one(text)
        self.calls.append((text, vector))
        return vector


class RecordingStore(VectorStore):
    """包装真实后端 + 记录 ``search`` 收到的入参。"""

    name = "recording"

    def __init__(self, inner: VectorStore) -> None:
        self.inner = inner
        self.calls: List[Dict[str, Any]] = []

    async def add(self, records: Any) -> int:
        return await self.inner.add(records)

    async def search(self, query_vector: Any, **kwargs: Any) -> List[VectorMatch]:
        self.calls.append({"vector": list(query_vector), **kwargs})
        return await self.inner.search(query_vector, **kwargs)

    async def count(self) -> int:
        return await self.inner.count()


class ConceptEmbedder(EmbeddingService):
    """**确定性「语义」替身**：把文本映射到 4 个概念轴。

    用途是**隔离变量**：把「Embedding 有没有语义」与「检索层会不会按向量远近排序」
    分开取证。真实模型（``embedding_provider.EmbeddingProvider``）做的事与此同构——
    只是它的概念空间是学出来的、有几万维，而这里是我手写的 4 维。

    因此：**query 与目标切片零 token 重叠**也能命中，就说明排序依据是向量方向，
    不是字符串匹配。
    """

    name = "concept-test"
    dimension = 4

    #: 每个概念轴的命中词（刻意让 query 与目标切片**命中同一个轴但用词完全不同**）
    CONCEPTS: Tuple[Tuple[str, ...], ...] = (
        ("索引", "慢", "查询", "优化", "数据库", "B+", "磁盘", "IO", "explain", "性能"),
        ("缓存", "redis", "穿透", "雪崩", "过期", "淘汰", "rdb", "aof"),
        ("垃圾回收", "jvm", "gc", "堆", "内存", "eden", "region"),
        ("kafka", "消息", "幂等", "重复消费", "位移", "分区"),
    )

    def __init__(self) -> None:
        self.calls: List[str] = []

    @staticmethod
    def _hits(word: str, lowered: str) -> bool:
        """命中判定：**ASCII 词按词边界**，中文按子串。

        为什么不能一律用子串：``"io"`` 会命中 ``"reg**io**n"``，把「JVM/Region」
        那条误算成「数据库性能」轴——实测就是这么误判的（score 0.4472 而不是 0.0）。
        """
        if word.isascii():
            pattern = rf"(?<![a-z0-9]){re.escape(word.lower())}(?![a-z0-9])"
            return re.search(pattern, lowered) is not None
        return word in lowered

    async def _embed_one(self, text: str) -> List[float]:
        self.calls.append(text)
        lowered = text.lower()
        vector = [
            float(sum(1 for word in words if self._hits(word, lowered)))
            for words in self.CONCEPTS
        ]
        norm = sum(value * value for value in vector) ** 0.5
        if norm == 0:
            return vector
        return [value / norm for value in vector]


# ============================================================
# 数据库 / 语料辅助
# ============================================================
def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed_identity(db: AsyncSession) -> Tuple[int, int]:
    """建一个用户 + 一个岗位（走真实会话流程需要它们）。"""
    user = User(
        username="rag_chain", email="rag_chain@example.com", password_hash="x", role="user"
    )
    job = Job(
        job_name="后端开发工程师",
        salary="20-35K",
        edu_require="本科",
        major_require="不限",
        skills=JOB_SKILLS,
        duty="负责后端服务的设计与开发",
        city="深圳",
        industry="互联网",
    )
    db.add_all([user, job])
    await db.commit()
    await db.refresh(user)
    await db.refresh(job)
    return user.id, job.id


async def _import_corpus(db: AsyncSession, embedder: Any) -> List[Dict[str, Any]]:
    """**测试 1：插入真实知识 Chunk** —— 走入库 Pipeline（不是手搓 VectorRecord）。"""
    pipeline = KnowledgeImportPipeline(db, embedder=embedder)
    reports = []
    for title, category, source, content in KNOWLEDGE_DOCS:
        reports.append(
            await pipeline.import_document(
                {"title": title, "content": content, "category": category, "source": source}
            )
        )
    return reports


async def _chunk_ids_by_source(db: AsyncSession) -> Dict[str, List[int]]:
    """``source → [chunk_id 按主键升序]``。"""
    rows = (await db.execute(select(OrmChunk.id, OrmChunk.document_id).order_by(OrmChunk.id))).all()
    doc_sources = dict(
        (await db.execute(select(KnowledgeDocument.id, KnowledgeDocument.source))).all()
    )
    out: Dict[str, List[int]] = {}
    for chunk_id, document_id in rows:
        out.setdefault(doc_sources.get(document_id) or "", []).append(chunk_id)
    return out


async def _tie_break_probe(
    embedder: Any, query_vector: List[float]
) -> Tuple[List[int], List[VectorMatch]]:
    """在**独立引擎**上验证同分兜底。

    必须独立：`SqlAlchemyVectorStore.count()` 统计全表，且这两个额外切片会改变
    候选集大小，污染后面「top_k 超过候选数」与「两后端条数相等」的断言。
    （同 ``test_vector_store_chroma`` 的教训：每个用例一个引擎。）
    """
    engine, Session = _build_session_factory()
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        async with Session() as db:
            doc = KnowledgeDocument(
                title="同分语料", content="x", category="technical", source="tie://x"
            )
            db.add(doc)
            await db.commit()
            await db.refresh(doc)
            store = SqlAlchemyVectorStore(db, model=embedder.name)
            ids = await store.add_returning_ids(
                [
                    VectorRecord(
                        vector=query_vector, content="同分切片 A", model=embedder.name,
                        document_id=doc.id, metadata={"category": "technical"},
                    ),
                    VectorRecord(
                        vector=query_vector, content="同分切片 B", model=embedder.name,
                        document_id=doc.id, metadata={"category": "technical"},
                    ),
                ]
            )
            return ids, await store.search(query_vector, top_k=2)
    finally:
        await engine.dispose()


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 74)
    print("AI 面试 · 真实 RAG 检索链路端到端验证（SQL 后端 vs Chroma ANN 后端）")
    print("=" * 74)

    engine, Session = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    chroma_dir = tempfile.mkdtemp(prefix="careerai-chroma-verify-")
    chroma_available = True
    chroma_import_error = ""
    try:
        import chromadb  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        chroma_available = False
        chroma_import_error = f"{type(exc).__name__}: {exc}"

    try:
        async with Session() as db:
            # ============================================================
            # [0] 前置：默认 Embedding 是哪一个（决定「相关」的含义）
            # ============================================================
            print("\n[0] 前置：默认 Embedding 实现（决定「相关」的含义）")
            embedder = knowledge_rag.default_embedder()
            _check(
                "未配置 EMBEDDING_* → 默认是离线哈希占位 HashEmbeddingService",
                isinstance(embedder, HashEmbeddingService),
                type(embedder).__name__,
            )
            _check(
                "  └ 它是零依赖离线实现（不联网、不需要密钥）",
                embedder.name == "hash-local",
                embedder.name,
            )
            print(
                "        ⇒ 因此 [3]~[7] 的「相关」是**词面（token 共享）相关**；"
                "真语义由 [8] 单独取证。"
            )

            # ============================================================
            # [1] 完整调用链取证（每一环都走真实实现）
            # ============================================================
            print("\n[1] 完整调用链取证：query → Embedding → search → select_matches → Retriever")
            _check(
                "VectorKnowledgeRetriever 实现自 KnowledgeRetriever 接口",
                issubclass(VectorKnowledgeRetriever, KnowledgeRetriever),
            )
            _check(
                "  └ retrieve 是 async（接口签名已提前异步化）",
                inspect.iscoroutinefunction(VectorKnowledgeRetriever.retrieve),
            )
            _check(
                "  └ retrieve 签名与基类逐字一致 (job_info, topic, context)",
                list(inspect.signature(VectorKnowledgeRetriever.retrieve).parameters)
                == list(inspect.signature(KnowledgeRetriever.retrieve).parameters),
                str(list(inspect.signature(VectorKnowledgeRetriever.retrieve).parameters)),
            )

            # 用 spy 跑一次「空库」链路：只为取证调用顺序与向量一致性
            spy_embedder = RecordingEmbedder()
            spy_store = RecordingStore(SqlAlchemyVectorStore(db, model=spy_embedder.name))
            spy_retriever = VectorKnowledgeRetriever(spy_embedder, spy_store)
            empty_result = await spy_retriever.retrieve(
                {"job_name": "后端开发工程师"}, TOPIC, CONTEXT
            )
            _check(
                "① build_query 取到 topic（只取一个来源，不拼接）",
                [text for text, _ in spy_embedder.calls] == [TOPIC],
                str([text for text, _ in spy_embedder.calls]),
            )
            _check(
                "② embed 的产出**原样**传给 store.search（同一条 query 向量）",
                bool(spy_store.calls)
                and spy_store.calls[0]["vector"] == spy_embedder.calls[0][1],
            )
            _check(
                "③ search 的 model 过滤 = embedder.name（不同模型的向量不可比）",
                bool(spy_store.calls) and spy_store.calls[0]["model"] == spy_embedder.name,
                str(spy_store.calls[0]["model"] if spy_store.calls else None),
            )
            _check(
                "④ search 收到 top_k / min_score（来自检索器配置，不是硬编码）",
                bool(spy_store.calls)
                and spy_store.calls[0]["top_k"] == 5
                and spy_store.calls[0]["min_score"] is None,
                str(spy_store.calls[0] if spy_store.calls else None),
            )
            _check("⑤ 空库 → []（「没有候选」不是异常）", empty_result == [])
            _check(
                "  └ 空库也会真的走完 embed + search（不是提前 return）",
                len(spy_embedder.calls) == 1 and len(spy_store.calls) == 1,
            )

            probe_records = [
                VectorRecord(vector=[1.0, 0.0], content="a", chunk_id=1),
                VectorRecord(vector=[0.0, 1.0], content="b", chunk_id=2),
            ]
            _check(
                "⑥ select_matches 是后端共用的排序口径（手算 == 结果）",
                _match_keys(select_matches(probe_records, [1.0, 0.0], top_k=2))
                == [(1, 1.0), (2, 0.0)],
            )

            # ============================================================
            # [2] 插入真实知识 Chunk（测试要求 1）
            # ============================================================
            print("\n[2] 插入真实知识 Chunk（走 KnowledgeImportPipeline：文档→切片→向量→落库）")
            user_id, job_id = await _seed_identity(db)
            reports = await _import_corpus(db, embedder)

            _check(
                "5 篇文档全部入库成功（status=ok）",
                all(r["status"] == STATUS_OK and r["ok"] for r in reports),
                str([(r["status"], r["error"]) for r in reports]),
            )
            _check(
                "  └ 每篇都真的产出了切片",
                all(r["chunk_count"] >= 1 for r in reports),
                str([r["chunk_count"] for r in reports]),
            )
            _check(
                "  └ 切片数 == 新建切片数（首次入库，无复用）",
                all(r["saved_chunks"] == r["chunk_count"] for r in reports),
                str([(r["chunk_count"], r["saved_chunks"]) for r in reports]),
            )
            _check(
                "  └ 每片都写入了向量（embedded_chunks == chunk_count）",
                all(r["embedded_chunks"] == r["chunk_count"] for r in reports),
                str([(r["chunk_count"], r["embedded_chunks"]) for r in reports]),
            )

            doc_rows = (await db.execute(select(KnowledgeDocument))).scalars().all()
            chunk_rows = (await db.execute(select(OrmChunk))).scalars().all()
            _check(
                "文档表真的落了 5 行",
                len(doc_rows) == len(KNOWLEDGE_DOCS),
                str(len(doc_rows)),
            )
            _check(
                "切片表真的落了 ≥5 行（长文档被切成多片）",
                len(chunk_rows) >= len(KNOWLEDGE_DOCS),
                str(len(chunk_rows)),
            )
            _check(
                "  └ 每行切片都带 embedding + embedding_model + embedding_dim",
                all(
                    row.embedding is not None
                    and row.embedding_model == embedder.name
                    and row.embedding_dim == embedder.dimension
                    for row in chunk_rows
                ),
                str([(row.embedding_model, row.embedding_dim) for row in chunk_rows][:3]),
            )
            _check(
                "  └ embedding IS NULL 的行数为 0（没有「半截数据」）",
                sum(1 for row in chunk_rows if row.embedding is None) == 0,
            )

            sql_store = SqlAlchemyVectorStore(db, model=embedder.name)
            _check(
                "向量库 count() == 已向量化切片数",
                await sql_store.count() == len(chunk_rows),
                f"{await sql_store.count()} vs {len(chunk_rows)}",
            )

            ids_by_source = await _chunk_ids_by_source(db)
            _check(
                "能按 source 追溯到切片（面试场景必须可溯源）",
                all(source in ids_by_source for _, _, source, _ in KNOWLEDGE_DOCS),
                str(sorted(ids_by_source)),
            )
            _check(
                "长文档确实被切成了多片（验证多切片文档）",
                any(len(ids) > 1 for ids in ids_by_source.values()),
                str({s: len(ids) for s, ids in ids_by_source.items()}),
            )

            all_ids = sorted(cid for ids in ids_by_source.values() for cid in ids)
            authoritative = await sql_store.load_records(all_ids)
            _check(
                "load_records 能按主键回读全部权威行（含向量）",
                len(authoritative) == len(all_ids)
                and all(len(record.vector) == embedder.dimension for record in authoritative),
                f"{len(authoritative)} vs {len(all_ids)}",
            )
            by_id = {record.chunk_id: record for record in authoritative}
            total_candidates = len(all_ids)

            # ============================================================
            # [3] 使用相关 query 检索（测试要求 2）
            # ============================================================
            print("\n[3] 使用相关 query 检索（测试要求 2）")
            retriever = VectorKnowledgeRetriever(embedder, sql_store, top_k=5)

            for query, expected_source in LEXICAL_QUERIES:
                chunks = await retriever.retrieve({"job_name": "后端"}, query, CONTEXT)
                expected_ids = set(ids_by_source.get(expected_source, []))
                _check(
                    f"query={query!r} → 检索到知识（非空）",
                    len(chunks) > 0,
                    str(len(chunks)),
                )
                _check(
                    f"  └ 排名第 1 的切片来自期望文档 {expected_source!r}",
                    bool(chunks) and chunks[0].metadata.get("chunk_id") in expected_ids,
                    _brief(chunks),
                )
                _check(
                    "  └ 结果形如 KnowledgeChunk(content/source/metadata)",
                    all(isinstance(c, KnowledgeChunk) for c in chunks),
                )
                _check(
                    f"  └ source 被提升为顶层字段（= {expected_source!r}）",
                    bool(chunks) and chunks[0].source == expected_source,
                    chunks[0].source if chunks else "",
                )
                _check(
                    f"  └ score 落在 metadata[{SCORE_METADATA_KEY!r}]（不渗进 Prompt）",
                    bool(chunks) and SCORE_METADATA_KEY in chunks[0].metadata,
                )
                _check(
                    "  └ 检索器记录了实际构造出的 query 文本",
                    retriever.queries[-1] == query,
                    str(retriever.queries[-1:]),
                )

            silent_embedder = RecordingEmbedder()
            silent_store = RecordingStore(SqlAlchemyVectorStore(db, model=silent_embedder.name))
            silent_retriever = VectorKnowledgeRetriever(silent_embedder, silent_store)
            none_result = await silent_retriever.retrieve(None, "", None)
            _check(
                "无查询线索（topic/stage/岗位名全空）→ [] 且**不发起**任何外部调用",
                none_result == [] and silent_embedder.calls == [] and silent_store.calls == [],
                f"{none_result} embed={len(silent_embedder.calls)} "
                f"search={len(silent_store.calls)}",
            )

            # ============================================================
            # [4] 验证 score 排序（测试要求 3）
            # ============================================================
            print("\n[4] 验证 score 排序（测试要求 3）")
            query = "MySQL 索引优化"
            query_vector = await embedder.embed(query)
            matches = await sql_store.search(query_vector, top_k=total_candidates)
            scores = [m.score for m in matches]

            _check(
                "结果按 score **降序**排列",
                scores == sorted(scores, reverse=True),
                str([round(s, 4) for s in scores]),
            )
            _check(
                "  └ 排名第 1 的就是与 query 最相关的切片（MySQL 索引）",
                bool(matches)
                and matches[0].chunk_id in set(ids_by_source["manual://handbook/mysql-index"]),
                _brief(matches),
            )
            _check(
                "  └ 第 1 名的 score 严格高于第 2 名（不是并列）",
                len(scores) >= 2 and scores[0] > scores[1],
                str([round(s, 6) for s in scores[:2]]),
            )

            recomputed = [
                (m.chunk_id, cosine_similarity(query_vector, by_id[m.chunk_id].vector))
                for m in matches
            ]
            _check(
                "  └ 每条 score 与**独立手算**的 cosine_similarity 逐位一致",
                _match_keys(matches) == recomputed,
                f"{[(c, round(s, 6)) for c, s in _match_keys(matches)[:2]]} vs "
                f"{[(c, round(s, 6)) for c, s in recomputed[:2]]}",
            )
            _check(
                "  └ 命中自己（同一条向量）时 score ≈ 1.0",
                round(cosine_similarity(query_vector, query_vector), 9) == 1.0,
            )

            tie_ids, tie_matches = await _tie_break_probe(embedder, query_vector)
            _check(
                "同分时按候选顺序稳定排序（SQL = 主键升序）→ 同输入同输出",
                [m.chunk_id for m in tie_matches] == sorted(tie_ids),
                f"{[m.chunk_id for m in tie_matches]} vs {sorted(tie_ids)}",
            )
            _check(
                "  └ 两条同分切片 score 完全相等",
                len(tie_matches) == 2 and tie_matches[0].score == tie_matches[1].score,
            )
            _check(
                "  └ 同分探针跑在独立引擎上，未污染主语料",
                await sql_store.count() == total_candidates,
                f"{await sql_store.count()} vs {total_candidates}",
            )

            # ============================================================
            # [5] 验证 top_k（测试要求 4）
            # ============================================================
            print("\n[5] 验证 top_k（测试要求 4）")
            for k in (1, 2, 3):
                got = await sql_store.search(query_vector, top_k=k)
                _check(f"top_k={k} → 恰好返回 {k} 条", len(got) == k, str(len(got)))
            over = await sql_store.search(query_vector, top_k=total_candidates + 50)
            _check(
                "top_k 超过候选数 → 返回全部候选（不报错）",
                len(over) == total_candidates,
                f"{len(over)} vs {total_candidates}",
            )
            _check(
                "  └ 前 k 条就是全量结果的前 k 条（截断不改顺序）",
                _match_keys(await sql_store.search(query_vector, top_k=2))
                == _match_keys(over)[:2],
            )
            bad_topk, exc = await _raises(sql_store.search(query_vector, top_k=0), Exception)
            _check(
                "top_k=0 → 报错（不静默收敛成 1，避免藏 bug）",
                bad_topk and type(exc).__name__ == "VectorStoreInputError",
                type(exc).__name__ if exc else "未抛异常",
            )
            bad_topk2, exc2 = await _raises(sql_store.search(query_vector, top_k=True), Exception)
            _check(
                "top_k=True → 报错（bool 是 int 子类，必须显式挡）",
                bad_topk2 and type(exc2).__name__ == "VectorStoreInputError",
                type(exc2).__name__ if exc2 else "未抛异常",
            )

            # ============================================================
            # [6] 验证 min_score 过滤（测试要求 5）
            # ============================================================
            print("\n[6] 验证 min_score 过滤（测试要求 5）")
            full = await sql_store.search(query_vector, top_k=total_candidates)
            _check(
                "min_score=None → 与不过滤一致（全量）",
                _match_keys(await sql_store.search(query_vector, top_k=total_candidates))
                == _match_keys(full),
            )
            top_score, second_score = full[0].score, full[1].score
            threshold = (top_score + second_score) / 2
            filtered = await sql_store.search(
                query_vector, top_k=total_candidates, min_score=threshold
            )
            _check(
                f"阈值卡在 1/2 名之间（{round(threshold, 6)}）→ 只留第 1 名",
                len(filtered) == 1 and filtered[0].chunk_id == full[0].chunk_id,
                _brief(filtered),
            )
            at_threshold = await sql_store.search(
                query_vector, top_k=total_candidates, min_score=top_score
            )
            _check(
                "  └ 阈值是「≥」而不是「>」（等于阈值的结果保留）",
                _match_keys(at_threshold) == _match_keys(full)[:1],
                _brief(at_threshold),
            )
            _check(
                "阈值 > 1.0（余弦上界）→ 空结果",
                await sql_store.search(query_vector, top_k=total_candidates, min_score=1.01)
                == [],
            )
            _check(
                "  └ 高阈值返回 [] 是「没到线」，不是异常（与维度错区分开）",
                await sql_store.search(query_vector, top_k=3, min_score=1.01) == [],
            )
            kept = await sql_store.search(query_vector, top_k=total_candidates, min_score=0.0)
            _check(
                "min_score=0.0 → 只留非负相似度",
                all(m.score >= 0.0 for m in kept) and len(kept) <= len(full),
                str([round(m.score, 4) for m in kept]),
            )
            _check(
                "  └ 过滤只减不增、且保持降序（不重排）",
                [m.chunk_id for m in kept]
                == [m.chunk_id for m in full if m.score >= 0.0],
            )

            # ============================================================
            # [7] SQL 后端 vs Chroma ANN 后端结果对比
            # ============================================================
            print("\n[7] SQL 后端 vs Chroma ANN 后端：结果规则一致性")
            if not chroma_available:
                _check(f"chromadb 可用（{chroma_import_error}）", False, chroma_import_error)
            else:
                from services.vector_store_chroma import ChromaVectorStore

                chroma_store = ChromaVectorStore(
                    db, model=embedder.name, path=chroma_dir, collection_name="verify_chain"
                )
                written = await chroma_store.add(authoritative)
                _check(
                    "chroma 后端接受同一批权威记录（add 返回条数）",
                    written == len(authoritative),
                    f"{written} vs {len(authoritative)}",
                )
                _check(
                    "  └ 索引条数 == 权威行条数（索引是权威行的镜像）",
                    await chroma_store.count() == await sql_store.count(),
                    f"{await chroma_store.count()} vs {await sql_store.count()}",
                )
                chroma_top = await chroma_store.search(query_vector, top_k=1)
                _check(
                    "  └ 索引内容取自权威行（正文逐字一致）",
                    bool(chroma_top)
                    and chroma_top[0].content == by_id[chroma_top[0].chunk_id].content,
                )

                cases = [
                    (q, k, ms)
                    for q, _ in LEXICAL_QUERIES
                    for k, ms in (
                        (1, None),
                        (3, None),
                        (total_candidates, None),
                        (total_candidates, threshold),
                        (total_candidates, 1.01),
                    )
                ]
                mismatches = []
                worst_drift = 0.0
                for case_query, k, ms in cases:
                    vector = await embedder.embed(case_query)
                    sql_hits = await sql_store.search(vector, top_k=k, min_score=ms)
                    chroma_hits = await chroma_store.search(vector, top_k=k, min_score=ms)
                    left, right = _match_keys(sql_hits), _match_keys(chroma_hits)
                    worst_drift = max(worst_drift, _max_drift(left, right))
                    problems = _compare_pairs(left, right)
                    if problems:
                        mismatches.append((case_query, k, ms, problems))
                _check(
                    f"★ {len(cases)} 组 (query, top_k, min_score) 组合下，两后端"
                    "「命中集合 + 分数 + 排序」规则一致",
                    not mismatches,
                    str(mismatches[:2]),
                )
                _check(
                    f"  └ score 最大偏差 {worst_drift:.2e} ≤ {SCORE_TOL:g}"
                    "（float32 回读漂移，实测 ~1e-9 量级）",
                    worst_drift <= SCORE_TOL,
                    f"{worst_drift:.3e}",
                )
                top1_drift = _max_drift(
                    _match_keys(await sql_store.search(query_vector, top_k=1)),
                    _match_keys(await chroma_store.search(query_vector, top_k=1)),
                )
                _check(
                    "  └ 真正有意义的排序（分数差 >> 容差）逐位一致：top-1 完全相同",
                    top1_drift <= SCORE_TOL,
                    f"{top1_drift:.3e}",
                )

                # 取 top_k=2（分数 0.3167 / 0.0474，远超容差）逐字段比对，
                # 避开「截断处落在数值 0 的并列组内」这种 float32 抖动。
                sql_hits = await sql_store.search(query_vector, top_k=2)
                chroma_hits = await chroma_store.search(query_vector, top_k=2)
                sql_by_id = {m.chunk_id: m for m in sql_hits}
                chroma_by_id = {m.chunk_id: m for m in chroma_hits}
                _check(
                    "  └ 正文逐字一致（top-2，按 chunk_id 对齐）",
                    sorted(sql_by_id) == sorted(chroma_by_id)
                    and all(
                        sql_by_id[cid].content == chroma_by_id[cid].content
                        for cid in sql_by_id
                    ),
                    f"{sorted(sql_by_id)} vs {sorted(chroma_by_id)}",
                )
                _check(
                    "  └ metadata 逐键一致（category / source 等保真还原）",
                    sorted(sql_by_id) == sorted(chroma_by_id)
                    and all(
                        dict(sql_by_id[cid].metadata) == dict(chroma_by_id[cid].metadata)
                        for cid in sql_by_id
                    ),
                    str([chroma_by_id[cid].metadata for cid in sorted(chroma_by_id)][:1]),
                )
                _check(
                    "  └ 两后端都按同一降序口径（select_matches 唯一）",
                    [m.score for m in chroma_hits]
                    == sorted([m.score for m in chroma_hits], reverse=True),
                )

                sql_retriever = VectorKnowledgeRetriever(embedder, sql_store, top_k=3)
                chroma_retriever = VectorKnowledgeRetriever(embedder, chroma_store, top_k=3)
                for case_query, _ in LEXICAL_QUERIES:
                    left = await sql_retriever.retrieve({"job_name": "后端"}, case_query, CONTEXT)
                    right = await chroma_retriever.retrieve(
                        {"job_name": "后端"}, case_query, CONTEXT
                    )
                    left_by_id = {c.metadata.get("chunk_id"): c for c in left}
                    right_by_id = {c.metadata.get("chunk_id"): c for c in right}
                    shared = sorted(set(left_by_id) & set(right_by_id))
                    _check(
                        f"检索器层一致：query={case_query!r} 的 (chunk_id, score) 规则相同",
                        not _compare_pairs(_chunk_pairs(left), _chunk_pairs(right)),
                        f"{_brief(left)} vs {_brief(right)}",
                    )
                    _check(
                        "  └ 共同命中部分的 content / source 完全相同",
                        bool(shared)
                        and all(
                            (left_by_id[cid].content, left_by_id[cid].source)
                            == (right_by_id[cid].content, right_by_id[cid].source)
                            for cid in shared
                        ),
                        f"共同命中 {shared}",
                    )
                    _check(
                        "  └ top-1（分数最高、必然不并列）两后端完全相同",
                        bool(left)
                        and bool(right)
                        and left[0].metadata.get("chunk_id") == right[0].metadata.get("chunk_id")
                        and abs(
                            left[0].metadata[SCORE_METADATA_KEY]
                            - right[0].metadata[SCORE_METADATA_KEY]
                        )
                        <= SCORE_TOL,
                        f"{_brief(left, 1)} vs {_brief(right, 1)}",
                    )

                wrong_dim = [0.1] * (embedder.dimension + 3)
                sql_bad, _ = await _raises(sql_store.search(wrong_dim, top_k=3), Exception)
                chroma_bad, chroma_exc = await _raises(
                    chroma_store.search(wrong_dim, top_k=3), Exception
                )
                _check(
                    "维度不符时两后端都**抛错**（不是静默返回空）",
                    sql_bad and chroma_bad,
                    f"sql={sql_bad} chroma={chroma_bad}",
                )
                _check(
                    "  └ 抛的是同一个异常类型 VectorStoreDimensionError",
                    type(chroma_exc).__name__ == "VectorStoreDimensionError",
                    type(chroma_exc).__name__,
                )
                _check(
                    "★ ANN 索引是**派生**数据：权威副本始终在 knowledge_chunk",
                    len(authoritative) == len(all_ids)
                    and all(by_id[cid].content for cid in all_ids),
                )
                print(
                    "        注 1：本语料 6 条候选、oversample=4 ⇒ 召回即全量，"
                    "两后端**规则**逐位一致；\n"
                    "              候选数超过 top_k×4 后 ANN 召回是**近似**的——"
                    "排序口径一致，但可能漏掉个别候选。\n"
                    "        注 2：ANN 索引按 **float32** 存取向量，回读重算的余弦有 "
                    "~1e-9 漂移；\n"
                    "              对「分数在数值上等于 0」的近似并列项，顺序可能互换"
                    "（规则本身一致）。"
                )

            # ============================================================
            # [8] 语义能力验证：query 与目标切片**零 token 重叠**
            # ============================================================
            print("\n[8] 语义能力验证（确定性概念 Embedding：query 与目标零 token 重叠）")
            overlap = set(tokenize(SEMANTIC_QUERY)) & set(tokenize(SEMANTIC_TARGET_CONTENT))
            _check(
                f"前置：query={SEMANTIC_QUERY!r} 与目标切片**没有一个公共 token**",
                not overlap,
                str(sorted(overlap)),
            )

            concept = ConceptEmbedder()
            semantic_store = SqlAlchemyVectorStore(db, model=concept.name)
            semantic_texts = [SEMANTIC_TARGET_CONTENT, *SEMANTIC_OTHER_CONTENTS]
            vectors = await concept.embed_batch(semantic_texts)
            semantic_ids = await semantic_store.add_returning_ids(
                [
                    VectorRecord(
                        vector=vector,
                        content=text,
                        model=concept.name,
                        document_id=doc_rows[0].id,
                        metadata={"category": "technical"},
                    )
                    for text, vector in zip(semantic_texts, vectors)
                ]
            )
            semantic_retriever = VectorKnowledgeRetriever(
                concept, semantic_store, top_k=len(semantic_texts)
            )
            got = await semantic_retriever.retrieve({"job_name": "后端"}, SEMANTIC_QUERY, CONTEXT)
            _check(
                "★ 零词面重叠的 query 仍然命中了「数据库性能」那条切片（排第 1）",
                bool(got) and got[0].metadata.get("chunk_id") == semantic_ids[0],
                _brief(got),
            )
            _check(
                "  └ 它的 score ≈ 1.0（同概念轴 → 同方向）",
                bool(got) and round(got[0].metadata[SCORE_METADATA_KEY], 9) == 1.0,
                str(got[0].metadata.get(SCORE_METADATA_KEY) if got else None),
            )
            _check(
                "  └ 其余 3 条（不同概念轴）score 为 0.0（正交）",
                len(got) == 4
                and all(round(c.metadata[SCORE_METADATA_KEY], 9) == 0.0 for c in got[1:]),
                str([round(c.metadata[SCORE_METADATA_KEY], 4) for c in got]),
            )
            _check(
                "  └ 结论：检索层是**语义就绪**的，瓶颈只在 Embedding 模型本身",
                bool(got) and got[0].metadata.get("chunk_id") == semantic_ids[0],
            )

            # ============================================================
            # [9] 端到端：链路走到 InterviewAgent（知识正文进 Prompt）
            # ============================================================
            print("\n[9] 端到端：Core → Retriever → Agent，知识正文进入 Prompt")
            session_payload = SessionCreateRequest(job_id=job_id, total_questions=5)
            session_id = (
                await interview_service.create_session(db, user_id, session_payload)
            )["session"]["id"]
            await interview_service.start_session(db, user_id, session_id)

            spark = MockSpark(QUESTION_JSON)
            result = await interview_core.generate_next_question(
                db,
                session_id,
                spark=spark,
                context=CONTEXT,
                plan=PLAN,
                retriever=VectorKnowledgeRetriever(embedder, sql_store, top_k=3),
            )
            _check(
                "Core 出题成功（ok=True，走的是注入的真实检索器）",
                result.get("ok") is True,
                str(result.get("error")),
            )
            _check(
                "  └ 检索失败/无知识都不进 errors（检索是可选增强）",
                result.get("errors") == [],
                str(result.get("errors")),
            )
            _check(
                "  └ Agent 被调用恰好 1 次（每题只给一次 LLM 调用）",
                spark.call_count == 1,
                str(spark.call_count),
            )
            prompt = spark.calls[0] if spark.calls else ""
            _check("Prompt 里出现「参考知识：」小节（知识真的传到了 Agent）", "参考知识" in prompt)

            redis_chunks = await VectorKnowledgeRetriever(embedder, sql_store, top_k=3).retrieve(
                {"job_name": "后端"}, TOPIC, CONTEXT
            )
            redis_text = redis_chunks[0].content if redis_chunks else ""
            redis_source = redis_chunks[0].source if redis_chunks else ""
            _check(
                "  └ 检索到的**切片正文**原样出现在 Prompt 中",
                bool(redis_text) and redis_text in prompt,
                redis_text[:40],
            )
            _check(
                "  └ 来源也一起渲染（「（来源：…）」）——可溯源",
                bool(redis_source) and redis_source in prompt and "（来源：" in prompt,
                redis_source,
            )
            _check(
                "  └ 溯源 metadata **没有**渗进 Prompt（chunk_id 不外泄）",
                "chunk_id" not in prompt and "document_id" not in prompt,
            )
            _check(
                "  └ 非空知识走的是**变体模板**（与无知识模板不是同一个）",
                ag.PROMPT_QUESTION != ag.PROMPT_QUESTION_KNOWLEDGE
                and "knowledge" in ag.PROMPT_QUESTION_KNOWLEDGE,
                f"{ag.PROMPT_QUESTION} / {ag.PROMPT_QUESTION_KNOWLEDGE}",
            )

            baseline_spark = MockSpark(QUESTION_JSON)
            baseline = await interview_core.generate_next_question(
                db, session_id, spark=baseline_spark, context=CONTEXT, plan=PLAN
            )
            _check(
                "对照：不注入 retriever → 仍能出题（不注入就不接，默认路径不变）",
                baseline.get("ok") is True,
                str(baseline.get("error")),
            )
            _check(
                "  └ 基线 Prompt **不含**「参考知识」（空知识走原模板）",
                bool(baseline_spark.calls) and "参考知识" not in baseline_spark.calls[0],
            )
            _check(
                "  └ 两版 Prompt 的题目字段完全一致（知识只影响参考小节）",
                result.get("question") == baseline.get("question")
                and result.get("difficulty") == baseline.get("difficulty"),
            )

            # ============================================================
            # [10] 边界取证：不修改 InterviewCore / Agent 接口 / Validator
            # ============================================================
            print("\n[10] 边界取证：本轮未改动 InterviewCore / Agent 接口 / Validator")
            core_src = inspect.getsource(interview_core)
            agent_src = inspect.getsource(ag)
            validator_src = inspect.getsource(
                __import__("services.question_validator", fromlist=["x"])
            )

            _check(
                "InterviewCore 顶层不含向量层（RAG 仍是函数内延迟导入）",
                _offenders(core_src, FORBIDDEN_VECTOR_LAYER) == [],
                str(_offenders(core_src, FORBIDDEN_VECTOR_LAYER)),
            )
            _check(
                "  └ 顶层不含数据层（仍能脱离 DATABASE_URL 被 import）",
                _offenders(core_src, FORBIDDEN_DATA_LAYER) == [],
                str(_offenders(core_src, FORBIDDEN_DATA_LAYER)),
            )
            core_third = _offenders(core_src, FORBIDDEN_THIRD_PARTY)
            _check(
                "  └ 唯一的第三方顶层 import 是**已知例外** fastapi.HTTPException",
                core_third == ["fastapi", "fastapi.HTTPException"],
                str(core_third),
            )
            _check(
                "  └ 已知例外是刻意的（build_report 沿用旧行为，非本轮引入）",
                all(name.startswith("fastapi") for name in core_third),
                str(core_third),
            )

            _check(
                "Agent 顶层不 import 任何 Retriever / 向量库（知识由调用方注入）",
                _offenders(agent_src, FORBIDDEN_VECTOR_LAYER) == [],
                str(_offenders(agent_src, FORBIDDEN_VECTOR_LAYER)),
            )
            _check(
                "  └ Agent 只依赖 models（difficulty 词汇表）+ prompts（模板）",
                _offenders(agent_src, FORBIDDEN_DATA_LAYER) == ["models", "models.DIFFICULTIES",
                                                                 "models.INTERVIEW_STAGES"],
                str(_offenders(agent_src, FORBIDDEN_DATA_LAYER)),
            )

            _check(
                "Validator 顶层仍零第三方依赖（纯标准库）",
                _offenders(validator_src, FORBIDDEN_VECTOR_LAYER + FORBIDDEN_DATA_LAYER
                           + FORBIDDEN_THIRD_PARTY) == [],
                str(_offenders(validator_src, FORBIDDEN_VECTOR_LAYER + FORBIDDEN_DATA_LAYER
                               + FORBIDDEN_THIRD_PARTY)),
            )
            _check(
                "  └ 守卫本身有效（点号全名匹配，不是恒真的假守卫）",
                _module_names("from services.vector_store_sql import X")
                == {"services.vector_store_sql", "services.vector_store_sql.X"}
                and _module_names("import json") == {"json"}
                and _offenders("from services.vector_store_sql import X",
                               FORBIDDEN_VECTOR_LAYER) == ["services.vector_store_sql",
                                                           "services.vector_store_sql.X"],
            )
            _check(
                "  └ Agent 的 knowledge_context 仍是 keyword-only（签名未变）",
                inspect.signature(ag.generate_question).parameters["knowledge_context"].kind
                is inspect.Parameter.KEYWORD_ONLY,
            )

    finally:
        await engine.dispose()
        shutil.rmtree(chroma_dir, ignore_errors=True)

    print("\n" + "=" * 74)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
