# -*- coding: utf-8 -*-
"""AI 面试知识库 · **Embedding 运行状态标识**自检（脚本式，非 pytest）。

运行：``python backend/tests/test_embedding_info.py``

本套件验证「系统能明确当前 Embedding 模式」这件事，对应用户点名的两条测试：

[1] :class:`EmbeddingInfo` 的结构契约（恰好三字段 / 只读 / 可序列化）
[2] **测试 1 · Hash 模式返回正确信息**（``hash-local`` / 256 / ``semantic_enabled=False``）
[3] **测试 2 · 真实模式配置后返回正确信息**（模型名 / 配置维度 / ``True``）
[4] 组装器入口 ``knowledge_rag.describe_default_embedding()``：两种模式下都对
[5] 边界取证：**不修改 Embedding 接口**、**不修改 Retriever**（AST + 消费者闭集）
[6] 日志 / 调试可用性（零 IO、无密钥泄露、repr / JSON 友好）

.. note::
   ``semantic_enabled`` 是**各实现自行声明**的类属性，**不在** ``EmbeddingService``
   基类上——基类一行未改（用户要求「不修改 Embedding 接口」）。未声明的实现按
   「非语义」保守上报：日志里把离线占位说成语义模型，比不报更糟。
"""

from __future__ import annotations

import ast
import inspect
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch

BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from services import knowledge_rag  # noqa: E402
from services import embedding_provider as ep  # noqa: E402
from services import embedding_service as es  # noqa: E402
from services.embedding_provider import build_embedding_service  # noqa: E402
from services.embedding_service import (  # noqa: E402
    DEFAULT_EMBEDDING_DIMENSION,
    EmbeddingInfo,
    EmbeddingInputError,
    EmbeddingService,
    HashEmbeddingService,
    MockEmbeddingService,
    SEMANTIC_ENABLED_ATTR,
    describe_embedding,
)
from services.vector_knowledge_retriever import VectorKnowledgeRetriever  # noqa: E402

_PASSED = 0
_FAILED = 0

#: 真实模式用例的配置（**假密钥**，且永远发不出请求——本轮只读配置，不联网）
REAL_ENV = {
    "EMBEDDING_PROVIDER": "openai",
    "EMBEDDING_API_KEY": "sk-test-1234",
    "EMBEDDING_MODEL": "text-embedding-3-small",
    "EMBEDDING_DIMENSION": "1024",
}


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name
          + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def _section(title: str) -> None:
    print("\n" + "-" * 74)
    print(title)
    print("-" * 74)


# ============================================================
# AST 工具（与既有套件同口径）
# ============================================================
def _imported_modules(source: str) -> set:
    """全量 import（含函数体）——用于「消费者闭集」这类断言。"""
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module)
                for alias in node.names:
                    names.add(f"{node.module}.{alias.name}")
    return names


def _module_level_imports(source: str) -> set:
    """**只看模块顶层**的 import（只扫 ``ast.parse(src).body``）。

    「延迟导入」类断言必须用这个版本：``ast.walk`` 会把函数体内的 import 也算进来，
    断言恒为假（项目老陷阱，见 SKILL §4 陷阱 12）。
    """
    names = set()
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module)
                for alias in node.names:
                    names.add(f"{node.module}.{alias.name}")
    return names


def _production_files() -> List[Path]:
    files: List[Path] = []
    for pattern in ("*.py", "api/*.py", "services/*.py", "models/*.py",
                    "schemas/*.py", "utils/*.py"):
        for path in BACKEND_DIR.glob(pattern):
            if "__pycache__" in path.parts:
                continue
            files.append(path)
    return sorted(set(files))


def _source(rel: str) -> str:
    return (BACKEND_DIR / rel).read_text(encoding="utf-8")


def _env_without_embedding() -> Dict[str, str]:
    """当前环境**去掉全部 ``EMBEDDING_*``** 的副本。

    用例必须能确定性复现「一个都不配 → 离线占位」，不能受开发者 ``.env`` 影响。
    """
    return {k: v for k, v in os.environ.items() if not k.startswith("EMBEDDING_")}


class _BoomTransport:
    """一旦被调用就炸的传输层替身——用来证明「describe 全程零网络」。"""

    def __init__(self) -> None:
        self.calls: List[Any] = []

    async def post_json(self, url: str, **kwargs: Any) -> Any:  # pragma: no cover
        self.calls.append((url, kwargs))
        raise AssertionError("describe_embedding 不应该发起任何 HTTP 请求")


# ============================================================
# [1] 结构契约
# ============================================================
def check_structure() -> None:
    _section("[1] EmbeddingInfo 结构契约")

    fields = tuple(EmbeddingInfo._fields)
    _check("字段恰好是 (provider, dimension, semantic_enabled) 且顺序固定",
           fields == ("provider", "dimension", "semantic_enabled"), str(fields))
    _check("是 NamedTuple（可当普通元组解构）",
           issubclass(EmbeddingInfo, tuple))
    _check("已导出到 __all__（公开 API）",
           {"EmbeddingInfo", "describe_embedding", "SEMANTIC_ENABLED_ATTR"}
           <= set(es.__all__), str(sorted(es.__all__)))

    info = EmbeddingInfo(provider="p", dimension=3, semantic_enabled=True)
    _check("to_dict() 恒为三键且值一一对应",
           info.to_dict() == {"provider": "p", "dimension": 3,
                              "semantic_enabled": True}, str(info.to_dict()))
    _check("to_dict() 是普通 dict（不是 Mapping 视图 / 不共享引用）",
           type(info.to_dict()) is dict and info.to_dict() is not info.to_dict())
    _check("可 JSON 序列化（日志 / 调试直接用）",
           json.loads(json.dumps(info.to_dict(), ensure_ascii=False)) == info.to_dict())

    try:
        info.provider = "x"  # type: ignore[misc]
        _check("只读（不可变）：赋值应抛 AttributeError", False, "竟然赋值成功")
    except AttributeError:
        _check("只读（不可变）：赋值抛 AttributeError", True)

    _check("SEMANTIC_ENABLED_ATTR 就是属性名（唯一口径，避免各处写字符串）",
           SEMANTIC_ENABLED_ATTR == "semantic_enabled", SEMANTIC_ENABLED_ATTR)


# ============================================================
# [2] 测试 1 · Hash 模式
# ============================================================
def check_hash_mode() -> None:
    _section("[2] 测试 1 · Hash 模式返回正确信息")

    info = describe_embedding(HashEmbeddingService())
    print(f"  HashEmbeddingService() -> {info}")
    _check("provider == 'hash-local'", info.provider == "hash-local", info.provider)
    _check("dimension == 256", info.dimension == DEFAULT_EMBEDDING_DIMENSION,
           str(info.dimension))
    _check("★ semantic_enabled is False（离线占位，不是语义向量）",
           info.semantic_enabled is False, repr(info.semantic_enabled))
    _check("semantic_enabled 是**真 bool**（不是 0 / 不是字符串）",
           isinstance(info.semantic_enabled, bool))
    _check("与用户给的示例逐字段一致",
           info.to_dict() == {"provider": "hash-local", "dimension": 256,
                              "semantic_enabled": False}, str(info.to_dict()))

    # 自定义维度的 Hash 实现也要如实上报
    small = describe_embedding(HashEmbeddingService(dimension=32))
    _check("自定义 dimension 如实上报（32）", small.dimension == 32, str(small))
    _check("  └ 仍报非语义", small.semantic_enabled is False)

    mock_info = describe_embedding(MockEmbeddingService())
    print(f"  MockEmbeddingService() -> {mock_info}")
    _check("测试替身同样报非语义（provider='mock'）",
           mock_info.provider == "mock" and mock_info.semantic_enabled is False,
           str(mock_info))

    # 经工厂（空配置）也必须是 hash
    from_env = describe_embedding(build_embedding_service(env={}))
    _check("★ 经工厂：一个 EMBEDDING_* 都不配 → 仍是 hash 模式",
           from_env == info, str(from_env))


# ============================================================
# [3] 测试 2 · 真实模式
# ============================================================
def check_real_mode() -> None:
    _section("[3] 测试 2 · 真实模式配置后返回正确信息")

    transport = _BoomTransport()
    embedder = build_embedding_service(env=REAL_ENV, transport=transport)
    _check("工厂按配置选出 EmbeddingProvider",
           type(embedder).__name__ == "EmbeddingProvider", type(embedder).__name__)

    info = describe_embedding(embedder)
    print(f"  EmbeddingProvider(config) -> {info}")
    _check("provider == 模型名（即 embedding_model 列会记的值）",
           info.provider == "text-embedding-3-small", info.provider)
    _check("dimension == 配置的 EMBEDDING_DIMENSION（1024）",
           info.dimension == 1024, str(info.dimension))
    _check("★ semantic_enabled is True（真实模型产出语义向量）",
           info.semantic_enabled is True, repr(info.semantic_enabled))
    _check("semantic_enabled 是**真 bool**",
           isinstance(info.semantic_enabled, bool))
    _check("与用户给的示例形状一致（provider / dimension / semantic_enabled）",
           set(info.to_dict()) == {"provider", "dimension", "semantic_enabled"})

    _check("★ 全程零网络：构造 + describe 都没有发过请求",
           transport.calls == [], str(transport.calls))

    # 只配密钥（provider 留空 → 自动判定为 openai）
    auto = describe_embedding(build_embedding_service(
        env={"EMBEDDING_API_KEY": "sk-auto", "EMBEDDING_MODEL": "bge-m3"},
        transport=_BoomTransport()))
    _check("只配密钥（provider 留空）→ 自动真实模式，provider 为模型名",
           auto == EmbeddingInfo("bge-m3", 0, True), str(auto))

    # 未配 EMBEDDING_DIMENSION → dimension 上报 0（= 不声明 / 不校验），不瞎猜
    _check("未声明维度时如实报 0（0 = 不声明维度，不是编一个数）",
           auto.dimension == 0, str(auto.dimension))

    # 真实模式不会把密钥带进状态标识
    _check("★ 状态标识里不含密钥（可安全进日志）",
           "sk-test-1234" not in repr(info) and "sk-test-1234" not in str(info.to_dict()))

    # 显式 hash 优先于密钥（配置口径）
    explicit = describe_embedding(build_embedding_service(
        env={**REAL_ENV, "EMBEDDING_PROVIDER": "hash"}))
    _check("显式 EMBEDDING_PROVIDER=hash 时仍报非语义（配置优先于密钥）",
           explicit.semantic_enabled is False and explicit.provider == "hash-local",
           str(explicit))


# ============================================================
# [4] 组装器入口
# ============================================================
def check_assembler_entry() -> None:
    _section("[4] 组装器入口 knowledge_rag.describe_default_embedding()")

    with patch.dict(os.environ, _env_without_embedding(), clear=True):
        offline = knowledge_rag.describe_default_embedding()
        print(f"  无 EMBEDDING_* -> {offline}")
        _check("无任何 EMBEDDING_* → 默认是 hash 模式且非语义",
               offline == EmbeddingInfo("hash-local", DEFAULT_EMBEDDING_DIMENSION, False),
               str(offline))
        _check("  └ 与 describe_embedding(default_embedder()) 同源（不会各说各话）",
               offline == describe_embedding(knowledge_rag.default_embedder()))

        with patch.dict(os.environ, REAL_ENV, clear=True):
            configured = knowledge_rag.describe_default_embedding()
            print(f"  配了 EMBEDDING_* -> {configured}")
            _check("★ 配好密钥后 → 真实模式且 semantic_enabled=True",
                   configured == EmbeddingInfo("text-embedding-3-small", 1024, True),
                   str(configured))
            _check("  └ 与 describe_embedding(default_embedder()) 同源",
                   configured == describe_embedding(knowledge_rag.default_embedder()))

    _check("已导出到组装器 __all__",
           "describe_default_embedding" in knowledge_rag.__all__,
           str(knowledge_rag.__all__))

    # 配置非法时不吞异常（「部署配错了」不该伪装成「离线占位」）
    bad = {**_env_without_embedding(), "EMBEDDING_PROVIDER": "openai"}
    with patch.dict(os.environ, bad, clear=True):
        try:
            knowledge_rag.describe_default_embedding()
            _check("配置非法（openai 无密钥）时抛 EmbeddingProviderConfigError",
                   False, "竟然返回了信息")
        except ep.EmbeddingProviderConfigError as exc:
            _check("配置非法（openai 无密钥）时抛 EmbeddingProviderConfigError",
                   "EMBEDDING_API_KEY" in str(exc), str(exc))


# ============================================================
# [5] 边界取证：不动接口、不动 Retriever
# ============================================================
def check_boundaries() -> None:
    _section("[5] 边界取证（不修改 Embedding 接口 / 不修改 Retriever）")

    # --- 5.1 接口面一行未改 ---
    public = {n for n in dir(EmbeddingService) if not n.startswith("_")}
    _check("★ EmbeddingService 的公开面仍是 {name, dimension, embed, embed_batch}",
           public == {"name", "dimension", "embed", "embed_batch"}, str(sorted(public)))
    _check("★ 基类**没有** semantic_enabled（新增能力放在实现上，接口未动）",
           "semantic_enabled" not in EmbeddingService.__dict__
           and not hasattr(EmbeddingService, "semantic_enabled"))
    _check("★ 基类没有 describe_embedding（是模块级函数，不是方法）",
           "describe_embedding" not in EmbeddingService.__dict__
           and not hasattr(EmbeddingService, "describe_embedding"))
    _check("抽象方法仍恰好是 {_embed_one}",
           getattr(EmbeddingService, "__abstractmethods__", None) == frozenset({"_embed_one"}),
           str(getattr(EmbeddingService, "__abstractmethods__", None)))
    _check("embed / embed_batch / _embed_one 的参数名与顺序未变",
           [list(inspect.signature(getattr(EmbeddingService, m)).parameters)
            for m in ("embed", "embed_batch", "_embed_one")]
           == [["self", "text"], ["self", "texts"], ["self", "text"]])
    _check("embed / embed_batch 仍是协程函数（签名未异步化/去异步）",
           inspect.iscoroutinefunction(EmbeddingService.embed)
           and inspect.iscoroutinefunction(EmbeddingService.embed_batch))
    _check("接口模块仍零第三方依赖（顶层依赖恰为标准库）",
           {m.split(".")[0] for m in _imported_modules(_source("services/embedding_service.py"))}
           == {"__future__", "abc", "collections", "hashlib", "re", "typing"},
           str(sorted({m.split(".")[0] for m in
                       _imported_modules(_source("services/embedding_service.py"))})))

    # --- 5.2 消费者闭集没被撑大 ---
    consumers = []
    for path in _production_files():
        if path.name == "embedding_service.py":
            continue
        if "services.embedding_service" in _imported_modules(path.read_text(encoding="utf-8")):
            consumers.append(path.relative_to(BACKEND_DIR).as_posix())
    _check("★ embedding_service 的消费者仍是已知闭集（没有新增消费者）",
           consumers == ["services/embedding_provider.py",
                         "services/embedding_provider_spark.py",
                         "services/vector_knowledge_retriever.py"], str(consumers))
    _check("★ 组装器顶层仍不 import embedding_service（保持经工厂取实现）",
           "services.embedding_service" not in _module_level_imports(
               _source("services/knowledge_rag.py")))

    # --- 5.3 Retriever 未被改动 ---
    retriever_src = _source("services/vector_knowledge_retriever.py")
    _check("★ 真实检索器没有引用本次新增的任何名字",
           not any(tok in retriever_src for tok in
                   ("EmbeddingInfo", "describe_embedding", "semantic_enabled")))
    _check("  └ 检索器的 retrieve 签名仍是 (self, job_info, topic, context)",
           list(inspect.signature(VectorKnowledgeRetriever.retrieve).parameters)
           == ["self", "job_info", "topic", "context"],
           str(list(inspect.signature(VectorKnowledgeRetriever.retrieve).parameters)))
    _check("  └ 接口模块 knowledge_retriever 仍零第三方依赖（顶层恰为标准库）",
           {m.split(".")[0] for m in _imported_modules(_source("services/knowledge_retriever.py"))}
           == {"__future__", "collections", "dataclasses", "typing"},
           str(sorted({m.split(".")[0] for m in
                       _imported_modules(_source("services/knowledge_retriever.py"))})))

    # --- 5.4 守卫自身验真（防「恒真的假守卫」）---
    # 反例 token 必须**运行时拼出来**：写成字面量它就出现在本文件源码里，
    # 于是「应该查不到」的 token 被自己查到，守卫自检恒假（项目陷阱 17）。
    absent = "describe_" + "embed" + "dingZZZ"
    _check("守卫自检：子串检查能区分「有」与「无」",
           ("EmbeddingInfo" in retriever_src) is False
           and ("EmbeddingInfo" in _source("services/embedding_service.py")) is True
           and (absent in retriever_src) is False)


# ============================================================
# [6] 日志 / 调试可用性
# ============================================================
def check_logging_usability() -> None:
    _section("[6] 日志 / 调试可用性")

    mock = MockEmbeddingService()
    info = describe_embedding(mock)
    _check("★ 零 IO：describe 没有编码任何文本（替身 calls 为空）",
           mock.calls == [], str(mock.calls))
    _check("repr 自带三字段（直接打进日志可读）",
           all(k in repr(info) for k in ("provider=", "dimension=", "semantic_enabled=")),
           repr(info))

    boom = _BoomTransport()
    provider = build_embedding_service(env=REAL_ENV, transport=boom)
    describe_embedding(provider)
    _check("★ 真实模式下 describe 也没发请求（不联网即可报状态）",
           boom.calls == [], str(boom.calls))

    # 错误入参要显式报错，而不是返回一个看似正常的标识
    for bad, why in ((object(), "非 EmbeddingService"),
                     (None, "None")):
        try:
            describe_embedding(bad)  # type: ignore[arg-type]
            _check(f"入参非法（{why}）抛 EmbeddingInputError", False, "竟然返回了信息")
        except EmbeddingInputError:
            _check(f"入参非法（{why}）抛 EmbeddingInputError", True)

    # 声明写错的实现必须**显式报错**，不能静默返回一个看似正常的标识。
    # 注意这些坏实现要**直接继承 EmbeddingService**——继承 HashEmbeddingService 不行：
    # 它的 __init__ 会 `self.dimension = dimension` 覆盖掉类属性，坏值根本留不住
    # （实测踩到：用例因错误的原因通过）。
    class _BadFlag(EmbeddingService):
        name = "bad-flag"
        dimension = 4
        semantic_enabled = "yes"  # type: ignore[assignment]    # 故意写错类型

        async def _embed_one(self, text: str) -> List[float]:
            return [0.0] * 4

    class _BadDim(EmbeddingService):
        name = "bad-dim"
        dimension = True  # type: ignore[assignment]            # bool 是 int 子类

        async def _embed_one(self, text: str) -> List[float]:
            return [0.0]

    class _BadName(EmbeddingService):
        name = "   "
        dimension = 4

        async def _embed_one(self, text: str) -> List[float]:
            return [0.0] * 4

    for cls, why in ((_BadFlag, "semantic_enabled 不是 bool"),
                     (_BadDim, "dimension 是 bool"),
                     (_BadName, "name 是纯空白")):
        try:
            describe_embedding(cls())
            _check(f"实现声明写错（{why}）→ 显式报错而非静默放过", False, "竟然通过了")
        except EmbeddingInputError:
            _check(f"实现声明写错（{why}）→ 显式报错而非静默放过", True)

    # 未声明的实现 → 保守上报「非语义」，而不是乐观地报 True
    class _Undeclared(EmbeddingService):
        name = "third-party-x"
        dimension = 4

        async def _embed_one(self, text: str) -> List[float]:
            return [0.1] * 4

    undeclared = describe_embedding(_Undeclared())
    _check("★ 未声明 semantic_enabled 的实现 → 保守报 False（宁少报不多报）",
           undeclared == EmbeddingInfo("third-party-x", 4, False), str(undeclared))


# ============================================================
# 主流程
# ============================================================
def run() -> bool:
    print("=" * 74)
    print("Embedding 运行状态标识（EmbeddingInfo）· 自检")
    print("=" * 74)

    check_structure()
    check_hash_mode()
    check_real_mode()
    check_assembler_entry()
    check_boundaries()
    check_logging_usability()

    print("\n" + "=" * 74)
    print("状态示例")
    print("=" * 74)
    print(f"  Hash  : {describe_embedding(HashEmbeddingService()).to_dict()}")
    print(f"  Mock  : {describe_embedding(MockEmbeddingService()).to_dict()}")
    print("  Real  : " + str(describe_embedding(
        build_embedding_service(env=REAL_ENV, transport=_BoomTransport())).to_dict()))
    print("=" * 74)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
