# -*- coding: utf-8 -*-
"""AI 面试知识库 · EmbeddingService（文本向量化）自检

无需 pytest，直接运行：
    python backend/tests/test_embedding_service.py

不依赖本机 MySQL、不联网、不需要任何密钥：默认实现是**离线确定性**的哈希散列。
本阶段**只做向量化**，故本测试不涉及向量库 / 相似度检索 / Retriever。

覆盖范围
--------
1. **模块可导入 + 契约**：抽象基类的钩子与模板方法、默认维度、``tokenize`` 行为
2. **单文本生成向量（要求 1）**：维度 / 类型 / 非零 / 归一 / 确定性 / 不同文本不同向量
3. **批量生成向量（要求 2）**：顺序保持、与逐条一致、空批次、**挡掉「把字符串当批次」**
4. **支持后续替换模型**：新模型 = 实现一个 ``_embed_one`` + 声明 name/dimension；
   基类模板方法对**所有**实现统一做入参 / 出参校验
5. **不绑定具体厂商**：AST 断言顶层依赖恰为标准库；无厂商 SDK / HTTP 客户端 / 向量库
6. **异常处理清晰**：四类异常各司其职，入参错（不该重试）与服务错（该重试）可分流
7. **确定性与区分度**：跨进程稳定（不依赖内置 ``hash()`` 的随机化）；共享 token 更相似
8. **边界**：不 import models / DB / FastAPI / Retriever / Interview；生产代码**未接线**
9. **与预留存储联动**：向量可写入 ``KnowledgeChunk.embedding`` 并原样读回
"""

import ast
import asyncio
import os
import pathlib
import subprocess
import sys
from typing import List

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
from models import KnowledgeChunk, KnowledgeDocument  # noqa: E402
from services import embedding_service as es  # noqa: E402
from services.embedding_service import (  # noqa: E402
    EmbeddingDimensionError,
    EmbeddingError,
    EmbeddingInputError,
    EmbeddingService,
    EmbeddingUnavailableError,
    HashEmbeddingService,
    MockEmbeddingService,
    tokenize,
)

_PASSED = 0
_FAILED = 0


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


def _l2(vector) -> float:
    return sum(v * v for v in vector) ** 0.5


def _cos(a, b) -> float:
    return sum(x * y for x, y in zip(a, b))


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
# 自定义实现（用于验证「换模型只需实现一个钩子」）
# ============================================================
class _FakeVendorEmbedding(EmbeddingService):
    """模拟一个真实厂商实现：只实现 ``_embed_one``，声明 name / dimension。"""

    name = "fake-vendor-v1"
    dimension = 4

    async def _embed_one(self, text: str) -> List[float]:
        # 故意返回 tuple（真实 SDK 常返回 tuple / numpy 数组），基类应照样接受
        return tuple(float(len(text) + i) for i in range(self.dimension))


class _BrokenDimEmbedding(EmbeddingService):
    name = "broken-dim"
    dimension = 8

    async def _embed_one(self, text: str) -> List[float]:
        return [0.1] * 3          # 声明 8 维却只给 3 维


class _BrokenTypeEmbedding(EmbeddingService):
    name = "broken-type"
    dimension = 0

    def __init__(self, payload):
        self.payload = payload

    async def _embed_one(self, text: str) -> List[float]:
        return self.payload


class _BatchCountingEmbedding(EmbeddingService):
    """覆盖 ``embed_batch``（真实模型的原生批量接口），验证覆盖点存在且生效。"""

    name = "batch-counting"
    dimension = 2

    def __init__(self):
        self.batch_calls: List[List[str]] = []

    async def _embed_one(self, text: str) -> List[float]:
        return [1.0, 0.0]

    async def embed_batch(self, texts):
        items = list(texts)
        self.batch_calls.append(items)
        return [[1.0, 0.0] for _ in items]


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 面试知识库 · EmbeddingService（文本向量化）自检")
    print("=" * 70)

    # ------------------------------------------------------------
    # [1] 模块可导入 + 契约
    # ------------------------------------------------------------
    print("\n[1] 模块可导入 + 契约")
    _check("services.embedding_service 可导入", hasattr(es, "EmbeddingService"))
    _check("EmbeddingService 是抽象基类（不能直接实例化）",
           issubclass(EmbeddingService, object)
           and getattr(EmbeddingService, "__abstractmethods__", None) == frozenset({"_embed_one"}),
           str(getattr(EmbeddingService, "__abstractmethods__", None)))
    try:
        EmbeddingService()
        _check("  └ 直接实例化被拒", False, "竟然实例化成功了")
    except TypeError:
        _check("  └ 直接实例化被拒（未实现 _embed_one）", True)
    _check("公开三个实现：基类 + 离线哈希 + 测试替身",
           all(isinstance(c, type) for c in (EmbeddingService, HashEmbeddingService,
                                             MockEmbeddingService)))
    _check("默认维度是正整数",
           isinstance(es.DEFAULT_EMBEDDING_DIMENSION, int)
           and es.DEFAULT_EMBEDDING_DIMENSION > 0, str(es.DEFAULT_EMBEDDING_DIMENSION))
    _check("__all__ 导出四类异常与两个实现",
           {"EmbeddingError", "EmbeddingInputError", "EmbeddingUnavailableError",
            "EmbeddingDimensionError", "HashEmbeddingService", "MockEmbeddingService",
            "EmbeddingService", "tokenize"} <= set(es.__all__))

    _check("tokenize：英文按词、小写归一",
           tokenize("Redis AOF") == ["redis", "aof"], str(tokenize("Redis AOF")))
    _check("tokenize：中文按单字 + 相邻二字组",
           tokenize("持久化") == ["持", "久", "化", "持久", "久化"],
           str(tokenize("持久化")))
    _check("tokenize：无内容 → 空列表",
           tokenize("") == [] and tokenize("！！！，。") == [],
           str(tokenize("！！！，。")))
    _check("tokenize：中英混排都能取到 token",
           "redis" in tokenize("Redis 持久化") and "持久" in tokenize("Redis 持久化"),
           str(tokenize("Redis 持久化")))

    # ------------------------------------------------------------
    # [2] 单文本生成向量（测试要求 1）
    # ------------------------------------------------------------
    print("\n[2] 单文本生成向量（测试要求 1）")
    svc = HashEmbeddingService(dimension=64)
    vector = await svc.embed("Redis 持久化有 RDB 和 AOF 两种方式")

    _check("★ embed 返回 list", isinstance(vector, list), type(vector).__name__)
    _check("★ 维度 == 声明的 dimension", len(vector) == 64, str(len(vector)))
    _check("  └ 元素全是 float", all(isinstance(v, float) for v in vector))
    _check("  └ 不是全零（文本确实被编码了）", any(v != 0.0 for v in vector))
    _check("  └ 默认做了 L2 归一（模长 ≈ 1）",
           abs(_l2(vector) - 1.0) < 1e-9, str(_l2(vector)))

    raw = HashEmbeddingService(dimension=64, normalize=False)
    raw_vector = await raw.embed("Redis 持久化有 RDB 和 AOF 两种方式")
    _check("normalize=False 时不归一（模长 != 1）",
           abs(_l2(raw_vector) - 1.0) > 1e-6, str(_l2(raw_vector)))
    _check("  └ 归一与不归一只差一个比例因子（方向一致）",
           all(abs(a * _l2(raw_vector) - b) < 1e-9
               for a, b in zip(vector, raw_vector)))

    again = await svc.embed("Redis 持久化有 RDB 和 AOF 两种方式")
    _check("★ 同文本 → 完全相同的向量（确定性）", again == vector)
    other = await svc.embed("今天天气不错，适合出去散步")
    _check("★ 不同文本 → 不同向量", other != vector)

    mixed = await svc.embed("Redis 持久化")
    _check("中英混排可编码", len(mixed) == 64 and any(v != 0.0 for v in mixed))

    punctuation = await HashEmbeddingService(dimension=8).embed("！！！，。……")
    _check("只有标点（无可编码 token）→ 全零向量，不除零报错",
           punctuation == [0.0] * 8, str(punctuation))

    for label, bad in {
        "空串": "",
        "纯空白": "   \n\t ",
        "None": None,
        "整数": 123,
        "列表": ["不是文本"],
    }.items():
        ok, info = await _raises(svc.embed(bad), EmbeddingInputError)
        _check(f"★ 拒绝入参：{label}", ok, str(info))

    try:
        HashEmbeddingService(dimension=0)
        _check("dimension=0 被拒", False, "未抛异常")
    except EmbeddingInputError:
        _check("dimension=0 被拒（正整数校验）", True)
    try:
        HashEmbeddingService(dimension=True)
        _check("  └ dimension=True 被拒（bool 是 int 子类，要单独挡）", False, "未抛异常")
    except EmbeddingInputError:
        _check("  └ dimension=True 被拒（bool 是 int 子类，要单独挡）", True)

    # ------------------------------------------------------------
    # [3] 批量生成向量（测试要求 2）
    # ------------------------------------------------------------
    print("\n[3] 批量生成向量（测试要求 2）")
    texts = ["Redis 持久化", "Kafka 分区与副本", "MySQL 索引下推"]
    batch = await svc.embed_batch(texts)

    _check("★ 返回列表且条数一致", isinstance(batch, list) and len(batch) == 3,
           str(len(batch)))
    _check("★ 每条形如单条 embed 的结果（维度一致）",
           all(len(v) == 64 for v in batch), str([len(v) for v in batch]))
    _check("★ 顺序与入参一致（逐条比对）",
           batch == [await svc.embed(t) for t in texts])
    _check("  └ tuple 入参同样可用",
           await svc.embed_batch(tuple(texts)) == batch)
    _check("  └ 单元素批次可用", len(await svc.embed_batch(["只有一条"])) == 1)
    _check("★ 空批次 → []（不报错）", await svc.embed_batch([]) == [])

    for label, bad in {
        "None": None,
        "整数": 123,
        "含 None 元素": ["正常文本", None],
        "含空文本元素": ["正常文本", ""],
    }.items():
        ok, info = await _raises(svc.embed_batch(bad), EmbeddingInputError)
        _check(f"★ 拒绝批量入参：{label}", ok, str(info))

    ok, info = await _raises(svc.embed_batch("你好"), EmbeddingInputError)
    _check("★ 把**单个字符串**传进批量接口被拒（否则会被逐字符拆开）", ok, str(info))
    ok, info = await _raises(svc.embed_batch(["ok", None]), EmbeddingInputError)
    _check("  └ 元素级错误指出是第几条", "第 1 条" in str(info), str(info))
    ok, info = await _raises(svc.embed_batch([""]), EmbeddingInputError)
    _check("  └ 空文本元素同样被拒且指出索引", "第 0 条" in str(info), str(info))

    # ------------------------------------------------------------
    # [4] 支持后续替换模型
    # ------------------------------------------------------------
    print("\n[4] 支持后续替换模型（换模型 = 实现一个钩子）")
    fake = _FakeVendorEmbedding()
    fake_vector = await fake.embed("任意文本")
    _check("★ 子类只实现 _embed_one 即可获得完整能力",
           len(fake_vector) == 4 and all(isinstance(v, float) for v in fake_vector),
           str(fake_vector))
    _check("  └ tuple 返回值被接受（真实 SDK 常返回 tuple / numpy）",
           isinstance(fake_vector, list) and len(fake_vector) == 4)
    _check("  └ embed_batch 对子类同样可用（逐条复用基类校验）",
           await fake.embed_batch(["a", "b"]) == [[1.0, 2.0, 3.0, 4.0]] * 2,
           str(await fake.embed_batch(["a", "b"])))
    _check("  └ name 用于标记「哪次编码产生的」（写进 embedding_model）",
           fake.name == "fake-vendor-v1" and isinstance(fake.name, str))

    ok, info = await _raises(_BrokenDimEmbedding().embed("x"), EmbeddingDimensionError)
    _check("★ 子类返回维度与声明不符 → EmbeddingDimensionError（不放过脏向量）",
           ok, str(info))

    for label, payload in {
        "空向量": [],
        "字符串": "not-a-vector",
        "含 bool": [True, False],
        "含字符串元素": ["0.1", "0.2"],
        "含 None 元素": [1.0, None],
        "不可迭代": 42,
    }.items():
        ok, info = await _raises(_BrokenTypeEmbedding(payload).embed("x"),
                                 EmbeddingDimensionError)
        _check(f"★ 拒绝非法向量：{label}", ok, str(info))

    undeclared = _BrokenTypeEmbedding([0.5, 0.5])
    _check("dimension=0（不声明）时不校验维度长度",
           await undeclared.embed("x") == [0.5, 0.5])

    counting = _BatchCountingEmbedding()
    _check("★ embed_batch 是可覆盖点（原生批量接口）",
           await counting.embed_batch(["a", "b"]) == [[1.0, 0.0], [1.0, 0.0]]
           and counting.batch_calls == [["a", "b"]], str(counting.batch_calls))

    mock = MockEmbeddingService(dimension=3, value=0.5)
    mock_batch = await mock.embed_batch(["第一条", "第二条"])
    _check("MockEmbeddingService 返回固定向量",
           mock_batch == [[0.5, 0.5, 0.5]] * 2, str(mock_batch))
    _check("  └ 记录每次调用的文本（按断言被调次数用，不比对数值）",
           mock.calls == ["第一条", "第二条"], str(mock.calls))

    # ------------------------------------------------------------
    # [5] 不绑定具体厂商
    # ------------------------------------------------------------
    print("\n[5] 不绑定具体厂商（AST 取证）")
    src = (BACKEND_DIR / "services" / "embedding_service.py").read_text(encoding="utf-8")
    mods = _imported_modules(src)
    top_level = {m.split(".")[0] for m in mods}
    _check("★ 顶层依赖恰为标准库（无任何厂商 SDK / HTTP 客户端）",
           top_level == {"__future__", "abc", "collections", "hashlib", "re", "typing"},
           str(sorted(top_level)))

    vendor_like = ("openai", "anthropic", "dashscope", "zhipu", "qianfan", "volc",
                   "iflytek", "spark", "bge", "sentence_transformers", "transformers",
                   "torch", "numpy", "requests", "httpx", "aiohttp", "urllib",
                   "chromadb", "faiss", "milvus", "qdrant", "pinecone", "weaviate")
    hit = sorted(m for m in mods if any(v in m.lower() for v in vendor_like))
    _check("  └ 模块名里也没有厂商 / 向量库 / HTTP 依赖", hit == [], str(hit))
    _check("  └ 未 import 业务侧（models / database / fastapi / Retriever / Interview）",
           not ({"models", "database", "fastapi", "sqlalchemy", "deps", "main",
                 "services.knowledge_retriever"} & top_level)
           and not any("interview" in m or "knowledge" in m for m in mods),
           str(sorted(mods)))
    _check("  └ 未做全局单例（无模块级实例，由调用方显式构造并注入）",
           not any(isinstance(getattr(es, n), EmbeddingService) for n in dir(es)))

    # ------------------------------------------------------------
    # [6] 异常处理清晰
    # ------------------------------------------------------------
    print("\n[6] 异常处理清晰（入参错 vs 服务错可分流）")
    _check("四类异常都继承 EmbeddingError（可一把捕获）",
           all(issubclass(e, EmbeddingError)
               for e in (EmbeddingInputError, EmbeddingUnavailableError,
                         EmbeddingDimensionError)))
    _check("★ EmbeddingInputError 同时是 ValueError（调用方改代码，不该重试）",
           issubclass(EmbeddingInputError, ValueError))
    _check("★ EmbeddingUnavailableError 同时是 RuntimeError（服务问题，可重试/降级）",
           issubclass(EmbeddingUnavailableError, RuntimeError))
    _check("EmbeddingDimensionError 同时是 ValueError（实现写错，必须暴露）",
           issubclass(EmbeddingDimensionError, ValueError))
    _check("  └ 三类互不误捕（Input 不是 RuntimeError，Unavailable 不是 ValueError）",
           not issubclass(EmbeddingInputError, RuntimeError)
           and not issubclass(EmbeddingUnavailableError, ValueError))

    try:
        await svc.embed("")
    except EmbeddingError as exc:
        _check("★ 用 EmbeddingError 能捕获全部向量化异常", isinstance(exc, EmbeddingInputError))
    except Exception as exc:  # noqa: BLE001
        _check("★ 用 EmbeddingError 能捕获全部向量化异常", False, type(exc).__name__)

    boom = EmbeddingUnavailableError("上游 503")
    failing = MockEmbeddingService(raises=boom)
    ok, info = await _raises(failing.embed("任意文本"), EmbeddingUnavailableError)
    _check("★ 服务级异常原样向上传播（不被基类吞掉 / 不换类型）",
           ok and info is boom, str(info))
    ok, info = await _raises(failing.embed_batch(["a", "b"]), EmbeddingUnavailableError)
    _check("  └ 批量路径同样传播", ok, str(info))

    # ------------------------------------------------------------
    # [7] 确定性与区分度
    # ------------------------------------------------------------
    print("\n[7] 确定性与区分度")
    _check("同一实例两次结果相同", await svc.embed("Kafka 副本") == await svc.embed("Kafka 副本"))
    _check("不同实例（同参数）结果相同",
           await HashEmbeddingService(dimension=64).embed("Kafka 副本")
           == await svc.embed("Kafka 副本"))
    _check("换 seed → 换向量（seed 确实生效）",
           await HashEmbeddingService(dimension=64, seed="other").embed("Kafka 副本")
           != await svc.embed("Kafka 副本"))

    _check("与自己余弦相似度 == 1", abs(_cos(vector, vector) - 1.0) < 1e-9)
    near = await svc.embed("Redis 持久化有 RDB 和 AOF 两种方式，很相似")
    far = await svc.embed("今天天气不错，适合出去散步")
    _check("★ 共享 token 的文本比不相关文本更相似（有区分度）",
           _cos(vector, near) > _cos(vector, far),
           f"near={_cos(vector, near):.4f} far={_cos(vector, far):.4f}")

    # 跨进程稳定：内置 hash() 受 PYTHONHASHSEED 影响，blake2b 不受影响。
    # 这是「同文本同向量」能否在真实多进程部署里成立的关键。
    probe = (
        "import sys, asyncio;"
        f"sys.path.insert(0, r'{BACKEND_DIR}');"
        "from services.embedding_service import HashEmbeddingService;"
        "v = asyncio.run(HashEmbeddingService(dimension=8).embed('Redis 持久化'));"
        "print(','.join(f'{x:.9f}' for x in v))"
    )
    outputs = []
    for seed in ("0", "1", "12345"):
        env = dict(os.environ, PYTHONHASHSEED=seed, DATABASE_URL="sqlite+aiosqlite:///:memory:")
        outputs.append(subprocess.run(
            [sys.executable, "-c", probe], capture_output=True, text=True,
            cwd=str(BACKEND_DIR), env=env).stdout.strip())
    _check("★★ 跨进程 / 跨 PYTHONHASHSEED 完全一致（未依赖内置 hash()）",
           len(set(outputs)) == 1 and outputs[0] != "", str(outputs))

    # ------------------------------------------------------------
    # [8] 边界：消费者是已知闭集
    # ------------------------------------------------------------
    print("\n[8] 边界：消费者收口（能力已就绪，消费方都是显式接线）")
    # ⚠️ 匹配必须用**点号全名** ``services.embedding_service``：
    # ``_imported_modules`` 收集的是 ``from services.embedding_service import X`` 里的
    # ``services.embedding_service``，**从不含裸模块名** ``embedding_service``。
    # 原先写裸名 → 集合里永远匹配不到 → 断言恒为真、**从未真正生效**（任务 46 修正）。
    # 现在消费它的恰好是三处：
    #   * ``embedding_provider`` —— **实现方**（继承基类 + 复用同一处入参/出参校验）
    #   * ``embedding_provider_spark`` —— **另一套协议的实现方**（任务 75；同样继承基类 +
    #     复用 ``_require_texts`` / ``_require_vector``，因此校验口径不会两条协议各写一份）
    #   * ``vector_knowledge_retriever`` —— 只 import 异常类型做映射
    # 注意 ``knowledge_rag`` **不在**这个集合里：它的 ``default_embedder()`` 已改为
    # 委托 ``embedding_provider.build_embedding_service()``（「用哪个模型」收口在工厂），
    # 于是组装器不再直接依赖任何**具体实现**——换实现（离线占位 / 真实模型 / Mock）
    # 完全不用动组装器。写侧的入库 Pipeline 同样不直接 import 本模块。
    offenders = []
    for path in _production_files():
        if path.name == "embedding_service.py":
            continue
        if "services.embedding_service" in _imported_modules(
                path.read_text(encoding="utf-8")):
            offenders.append(path.relative_to(BACKEND_DIR).as_posix())
    _check("★ 消费者收口为已知闭集（实现方 Provider + 真实检索器）",
           offenders == [
               "services/embedding_provider.py",
               "services/embedding_provider_spark.py",
               "services/vector_knowledge_retriever.py",
           ], str(offenders))
    for rel in ("services/interview_core.py", "services/interview_agent.py",
                "services/interview_service.py", "services/knowledge_retriever.py"):
        _check(f"  └ {rel} 未被改动（未 import 本服务）",
               "services.embedding_service" not in _imported_modules(
                   (BACKEND_DIR / rel).read_text(encoding="utf-8")))
    _check("  └ models/knowledge.py 不依赖本服务（模型不反向依赖服务）",
           "embedding_service" not in _imported_modules(
               (BACKEND_DIR / "models" / "knowledge.py").read_text(encoding="utf-8")))

    # ------------------------------------------------------------
    # [9] 与预留存储联动（单文本 → 向量 → 落库 → 读回）
    # ------------------------------------------------------------
    print("\n[9] 与预留存储联动：embedding 列可存可读")
    _check("★ KnowledgeChunk 已预留三列",
           {"embedding", "embedding_model", "embedding_dim"}
           <= set(KnowledgeChunk.__table__.c.keys()),
           str(sorted(KnowledgeChunk.__table__.c.keys())))

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
                                category="technical", source="manual://redis")
        db.add(doc)
        await db.commit()
        await db.refresh(doc)

        chunker_vector = await HashEmbeddingService(dimension=32).embed("RDB 是全量快照")
        chunk = KnowledgeChunk(document_id=doc.id, content="RDB 是全量快照",
                               chunk_metadata={},
                               embedding=chunker_vector,
                               embedding_model=HashEmbeddingService.name,
                               embedding_dim=len(chunker_vector))
        db.add(chunk)
        await db.commit()

        loaded = (await db.execute(
            select(KnowledgeChunk).where(KnowledgeChunk.document_id == doc.id)
        )).scalar_one()
        _check("★ 向量写入后原样读回（JSON 列保真，浮点不丢精度）",
               loaded.embedding == chunker_vector, str(loaded.embedding[:3]))
        _check("  └ 同时记录模型名与维度（换模型后可筛出旧向量重算）",
               loaded.embedding_model == "hash-local" and loaded.embedding_dim == 32,
               f"{loaded.embedding_model}/{loaded.embedding_dim}")
        _check("  └ 未向量化的切片可用 embedding IS NULL 筛出",
               len((await db.execute(select(KnowledgeChunk).where(
                   KnowledgeChunk.embedding.is_(None)))).scalars().all()) == 0)

    await engine.dispose()

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
