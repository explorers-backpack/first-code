# -*- coding: utf-8 -*-
"""AI 面试知识库 · 真实 Embedding Provider（``services/embedding_provider.py``）自检

无需 pytest，直接运行：
    python backend/tests/test_embedding_provider.py

**全程不联网、不需要任何密钥**：HTTP 传输层是可注入的，本测试注入
① 纯假 transport（断言「发了几次 / URL / 请求体 / 头是什么 / 顺序如何」），
② ``httpx.MockTransport``（覆盖真实的「状态码映射 / JSON 解析 / 超时」路径）。

覆盖范围（对应任务要求）
------------------------
1. **模块与配置**：``EmbeddingProviderConfig`` 字段与不可变性、``repr`` 不泄露密钥、
   ``load_embedding_config`` 的全部取值分支与校验
2. **单文本向量生成（要求 1）**：``embed(text)`` 接口未变（未重写 + 签名逐字相同）、
   请求体 / URL / 头正确、返回 ``list[float]`` 且维度吻合
3. **批量生成（要求 2）**：原生批量接口、按 ``batch_size`` 分批、**顺序与入参一致**、
   ``index`` 乱序可还原、空批次不发起请求、非法入参**在任何 HTTP 之前**被拒
4. **异常处理（要求 3）**：网络 / 超时 / 非 2xx / 2xx 但结构不对 / 维度不符 / 上游 error
   六类路径各归其位；**密钥绝不进入异常消息**
5. **Mock 模式仍可用（要求 4）**：``EMBEDDING_PROVIDER=mock`` 可选、MockEmbeddingService
   行为不变、离线占位实现一行未改
6. **边界与守卫**：模块顶层只有标准库（httpx 延迟导入）、不 import 数据层 / FastAPI /
   ``interview_*`` / 任何向量层、无全局单例、无 ``DATABASE_URL`` 也能 import
7. **与组装器联动**：``knowledge_rag.default_embedder()`` 按配置选实现——未配置走离线占位
   （默认行为不变），配好密钥走真实模型（整条链路换模型只改配置）
"""

import ast
import asyncio
import contextlib
import inspect
import os
import pathlib
import subprocess
import sys
from typing import Any, Dict, List, Optional

# 必须在 import 业务模块之前设置：SQLite 内存库，避免依赖本机 MySQL。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from services import embedding_provider as ep  # noqa: E402
from services.embedding_provider import (  # noqa: E402
    DEFAULT_BASE_URL,
    DEFAULT_BATCH_SIZE,
    DEFAULT_MODEL,
    DEFAULT_TIMEOUT,
    ENV_API_KEY,
    ENV_BASE_URL,
    ENV_BATCH_SIZE,
    ENV_DIMENSION,
    ENV_MODEL,
    ENV_PROVIDER,
    ENV_TIMEOUT,
    EmbeddingProvider,
    EmbeddingProviderConfig,
    EmbeddingProviderConfigError,
    EmbeddingTransport,
    HttpxEmbeddingTransport,
    OpenAICompatibleEmbeddingProvider,
    PROVIDER_HASH,
    PROVIDER_MOCK,
    PROVIDER_OPENAI,
    build_embedding_service,
    load_embedding_config,
)
from services.embedding_service import (  # noqa: E402
    EmbeddingDimensionError,
    EmbeddingError,
    EmbeddingInputError,
    EmbeddingService,
    EmbeddingUnavailableError,
    HashEmbeddingService,
    MockEmbeddingService,
)

_PASSED = 0
_FAILED = 0

SECRET = "sk-test-SECRET-abcdefgh1234"


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


async def _raises(coro, exc_type):
    """执行协程，返回 (是否抛了指定异常, 异常实例或说明)。"""
    try:
        await coro
    except exc_type as exc:
        return True, exc
    except Exception as exc:  # noqa: BLE001
        return False, f"抛了非预期异常 {type(exc).__name__}: {exc}"
    return False, "未抛异常"


def _sync_raises(func, exc_type, *args, **kwargs):
    try:
        func(*args, **kwargs)
    except exc_type as exc:
        return True, exc
    except Exception as exc:  # noqa: BLE001
        return False, f"抛了非预期异常 {type(exc).__name__}: {exc}"
    return False, "未抛异常"


def _imported_modules(source: str) -> set:
    """AST 提取真正被 import 的模块名（不用子串匹配：docstring 会误伤）。"""
    names: set = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _module_level_imports(source: str) -> set:
    """只取**模块顶层**（``ast.parse(src).body``）的 import，不含函数体内的延迟导入。"""
    names: set = set()
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _production_files():
    """后端生产代码（排除 tests / __pycache__）。"""
    files = []
    for pattern in ("*.py", "api/*.py", "services/*.py", "models/*.py",
                    "schemas/*.py", "utils/*.py"):
        for path in BACKEND_DIR.glob(pattern):
            if "__pycache__" in path.parts:
                continue
            files.append(path)
    return sorted(set(files))


@contextlib.contextmanager
def _env(values: Dict[str, str]):
    """临时设置环境变量（值为 ``""`` 表示**删除**该变量，用于验证「留空 = 自动」）。

    ⚠️ 入参必须是**映射**，不能用关键字传参：``_env(ENV_PROVIDER="")`` 只会得到
    名为 ``"ENV_PROVIDER"`` 的字面量键，而不是 ``"EMBEDDING_PROVIDER"``——
    这样写出来的测试会「静默地什么都没设置」并因错误的原因通过。
    """
    saved = {k: os.environ.get(k) for k in values}
    try:
        for key, value in values.items():
            if value == "":
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


# ============================================================
# 测试替身
# ============================================================
class FakeTransport(EmbeddingTransport):
    """假 HTTP 传输：记录每次请求；返回可预测的向量（值 = 文本长度 + 下标）。

    默认响应**带正确的 ``index``**；``responder`` 可覆盖成任意（含畸形）响应。
    """

    def __init__(self, *, dimension: int = 4, raises: Optional[BaseException] = None,
                 responder=None) -> None:
        self.dimension = dimension
        self.raises = raises
        self.responder = responder
        self.calls: List[Dict[str, Any]] = []

    @property
    def call_count(self) -> int:
        return len(self.calls)

    async def post_json(self, url, *, headers, payload, timeout):
        self.calls.append({
            "url": url,
            "headers": dict(headers),
            "payload": payload,
            "timeout": timeout,
        })
        if self.raises is not None:
            raise self.raises
        if self.responder is not None:
            return self.responder(payload)
        return {
            "data": [
                {"index": i, "embedding": [float(len(text)) + j for j in range(self.dimension)]}
                for i, text in enumerate(payload["input"])
            ]
        }


class NoPostJsonTransport:
    """缺 ``post_json`` 的对象：用于验证构造期就拒绝非法 transport。"""


def _httpx_transport(handler):
    """把 ``httpx.MockTransport`` 塞进默认传输实现 → 不联网也能覆盖真实 HTTP 路径。"""
    import httpx

    return HttpxEmbeddingTransport(
        client_factory=lambda **kw: httpx.AsyncClient(
            transport=httpx.MockTransport(handler), **kw
        )
    )


def _provider(*, dimension: int = 4, batch_size: int = DEFAULT_BATCH_SIZE,
              transport=None, api_key: str = SECRET, base_url: str = DEFAULT_BASE_URL,
              model: str = DEFAULT_MODEL, timeout: float = DEFAULT_TIMEOUT):
    config = EmbeddingProviderConfig(
        provider=PROVIDER_OPENAI, api_key=api_key, base_url=base_url,
        model=model, dimension=dimension, timeout=timeout, batch_size=batch_size,
    )
    return EmbeddingProvider(config, transport=transport)


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 面试知识库 · 真实 Embedding Provider 自检")
    print("=" * 70)

    # ------------------------------------------------------------
    # [1] 模块与配置
    # ------------------------------------------------------------
    print("\n[1] 模块可导入 + 配置解析")
    _check("services.embedding_provider 可导入", hasattr(ep, "EmbeddingProvider"))
    _check("★ EmbeddingProvider 是 EmbeddingService 的子类（换模型不换调用方式）",
           issubclass(EmbeddingProvider, EmbeddingService))
    _check("  └ 没有未实现的抽象方法（可直接实例化）",
           not getattr(EmbeddingProvider, "__abstractmethods__", None),
           str(getattr(EmbeddingProvider, "__abstractmethods__", None)))
    _check("OpenAICompatibleEmbeddingProvider 是同一对象的别名",
           OpenAICompatibleEmbeddingProvider is EmbeddingProvider)
    _check("★ EmbeddingProviderConfigError 同时是 ValueError（配置错，重试没用）",
           issubclass(EmbeddingProviderConfigError, ValueError)
           and issubclass(EmbeddingProviderConfigError, EmbeddingError))
    _check("EmbeddingTransport 是抽象基类（必须实现 post_json）",
           inspect.isabstract(EmbeddingTransport)
           and getattr(EmbeddingTransport, "__abstractmethods__", None) == frozenset({"post_json"}))
    _check("__all__ 导出配置项 / 工厂 / 实现 / 常量",
           {"EmbeddingProvider", "EmbeddingProviderConfig", "EmbeddingProviderConfigError",
            "EmbeddingTransport", "HttpxEmbeddingTransport", "build_embedding_service",
            "load_embedding_config", "PROVIDER_HASH", "PROVIDER_MOCK", "PROVIDER_OPENAI"}
           <= set(ep.__all__))

    # --- 默认值：一个变量都不配 → 离线占位（默认行为不变）---
    cfg = load_embedding_config({})
    _check("★ 空环境 → provider=hash（离线占位，默认行为不变）",
           cfg.provider == PROVIDER_HASH, cfg.provider)
    _check("  └ is_remote=False", cfg.is_remote is False)
    _check("  └ 默认 base_url / model / timeout / batch_size",
           cfg.base_url == DEFAULT_BASE_URL and cfg.model == DEFAULT_MODEL
           and cfg.timeout == DEFAULT_TIMEOUT and cfg.batch_size == DEFAULT_BATCH_SIZE)
    _check("  └ 默认 dimension=0（不校验维度）", cfg.dimension == 0)
    _check("  └ api_key 为空", cfg.api_key == "" and cfg.masked_api_key == "")

    # --- 只配密钥 → 自动切到 openai ---
    cfg = load_embedding_config({ENV_API_KEY: SECRET})
    _check("★ 只配 EMBEDDING_API_KEY → 自动 provider=openai（配置即替换）",
           cfg.provider == PROVIDER_OPENAI and cfg.is_remote is True, cfg.provider)

    # --- 显式指定 ---
    for raw, expected in {"hash": PROVIDER_HASH, "local": PROVIDER_HASH,
                          "offline": PROVIDER_HASH, "mock": PROVIDER_MOCK,
                          "openai": PROVIDER_OPENAI,
                          "openai-compatible": PROVIDER_OPENAI,
                          "OPENAI": PROVIDER_OPENAI}.items():
        env = {ENV_PROVIDER: raw}
        if expected == PROVIDER_OPENAI:
            env[ENV_API_KEY] = SECRET          # openai 必填密钥，否则构造期就该报错
        got = load_embedding_config(env).provider
        _check(f"  └ EMBEDDING_PROVIDER={raw!r} → {expected}", got == expected, got)
    _check("  └ hash 模式下即使配了密钥也保持 hash（显式优先）",
           load_embedding_config({ENV_PROVIDER: "hash", ENV_API_KEY: SECRET}).provider
           == PROVIDER_HASH)

    ok, info = _sync_raises(load_embedding_config, EmbeddingProviderConfigError,
                            {ENV_PROVIDER: "not-a-provider"})
    _check("★ 非法 provider 取值 → EmbeddingProviderConfigError", ok, str(info))
    ok, info = _sync_raises(load_embedding_config, EmbeddingProviderConfigError,
                            {ENV_PROVIDER: PROVIDER_OPENAI})
    _check("★ openai 却没给密钥 → 构造期就报（不伪装成「检索不到」）", ok, str(info))
    _check("  └ 报错信息点明该配哪个变量", ok and ENV_API_KEY in str(info), str(info))

    for env, why in (
        ({ENV_DIMENSION: "abc"}, "维度非整数"),
        ({ENV_DIMENSION: "-1"}, "维度为负"),
        ({ENV_TIMEOUT: "0"}, "超时为 0"),
        ({ENV_TIMEOUT: "-3"}, "超时为负"),
        ({ENV_TIMEOUT: "soon"}, "超时非数字"),
        ({ENV_BATCH_SIZE: "0"}, "批次大小为 0"),
        ({ENV_BATCH_SIZE: "-2"}, "批次大小为负"),
    ):
        ok, info = _sync_raises(load_embedding_config, EmbeddingProviderConfigError, env)
        _check(f"★ 非法配置被拒：{why}", ok, str(info))
    _check("  └ EMBEDDING_DIMENSION=0 合法（0 = 不校验）",
           load_embedding_config({ENV_DIMENSION: "0"}).dimension == 0)

    cfg = load_embedding_config({
        ENV_API_KEY: SECRET, ENV_BASE_URL: "https://example.test/v1/",
        ENV_MODEL: "bge-m3", ENV_DIMENSION: "1024",
        ENV_TIMEOUT: "12.5", ENV_BATCH_SIZE: "8",
    })
    _check("★ 完整配置逐项生效",
           cfg.provider == PROVIDER_OPENAI and cfg.api_key == SECRET
           and cfg.base_url == "https://example.test/v1" and cfg.model == "bge-m3"
           and cfg.dimension == 1024 and cfg.timeout == 12.5 and cfg.batch_size == 8,
           str(cfg))
    _check("  └ base_url 末尾的 / 被去掉（拼 endpoint 不会出现 //）",
           cfg.base_url == "https://example.test/v1")

    # --- 密钥安全 ---
    _check("★ config 的 repr 不含密钥（dataclass 默认会把所有字段打出来）",
           SECRET not in repr(cfg), repr(cfg))
    _check("  └ 只显示打码形式（保留末 4 位）",
           cfg.masked_api_key.endswith("1234") and SECRET not in cfg.masked_api_key,
           cfg.masked_api_key)
    _check("  └ 短密钥整体打码（不泄露长度以外的信息）",
           EmbeddingProviderConfig(api_key="abc").masked_api_key == "***")
    try:
        cfg.api_key = "x"
        _check("config 不可变（frozen dataclass）", False, "竟然赋值成功")
    except Exception:  # noqa: BLE001 - dataclasses.FrozenInstanceError
        _check("config 不可变（frozen dataclass）", True)

    # --- 工厂 ---
    _check("★ 工厂：hash → HashEmbeddingService（离线占位）",
           isinstance(build_embedding_service(load_embedding_config({})), HashEmbeddingService))
    _check("★ 工厂：mock → MockEmbeddingService（测试替身）",
           isinstance(build_embedding_service(
               load_embedding_config({ENV_PROVIDER: PROVIDER_MOCK})), MockEmbeddingService))
    _check("★ 工厂：openai → EmbeddingProvider（真实模型）",
           isinstance(build_embedding_service(
               load_embedding_config({ENV_API_KEY: SECRET})), EmbeddingProvider))
    _check("  └ 工厂读环境变量（env 缺省 = os.environ）",
           isinstance(build_embedding_service(None, env={ENV_API_KEY: SECRET}), EmbeddingProvider))
    _check("★ 不做全局单例（每次返回新对象）",
           build_embedding_service(load_embedding_config({}))
           is not build_embedding_service(load_embedding_config({})))
    ok, info = _sync_raises(build_embedding_service, EmbeddingProviderConfigError, "not-a-config")
    _check("★ 工厂收到非法 config 类型 → 明确报错", ok, str(info))

    # --- Provider 构造期校验 ---
    ok, info = _sync_raises(EmbeddingProvider, EmbeddingProviderConfigError,
                            EmbeddingProviderConfig(provider=PROVIDER_HASH))
    _check("★ 用 hash 配置构造 EmbeddingProvider → 构造期拒绝", ok, str(info))
    ok, info = _sync_raises(EmbeddingProvider, EmbeddingProviderConfigError,
                            EmbeddingProviderConfig(provider=PROVIDER_OPENAI))
    _check("  └ openai 配置但没有密钥 → 拒绝", ok, str(info))
    ok, info = _sync_raises(EmbeddingProvider, EmbeddingProviderConfigError,
                            EmbeddingProviderConfig(provider=PROVIDER_OPENAI, api_key=SECRET),
                            transport=NoPostJsonTransport())
    _check("  └ transport 缺 post_json → 拒绝（错误信息说明契约）",
           ok and "post_json" in str(info), str(info))

    # ------------------------------------------------------------
    # [2] 单文本向量生成（要求 1）
    # ------------------------------------------------------------
    print("\n[2] 单文本向量生成（要求 1）")
    _check("★ embed(text) 未被重写 —— 接口不变",
           "embed" not in EmbeddingProvider.__dict__)
    _check("  └ 签名与基类逐字相同",
           inspect.signature(EmbeddingProvider.embed) == inspect.signature(EmbeddingService.embed))
    _check("  └ 参数恰为 (self, text)",
           list(inspect.signature(EmbeddingProvider.embed).parameters) == ["self", "text"],
           str(list(inspect.signature(EmbeddingProvider.embed).parameters)))
    _check("★ embed_batch 被覆盖（厂商原生批量接口是真实性能来源）",
           "embed_batch" in EmbeddingProvider.__dict__)

    fake = FakeTransport(dimension=4)
    svc = _provider(dimension=4, transport=fake)
    vector = await svc.embed("abcd")

    _check("★ embed 返回 list[float]", isinstance(vector, list)
           and all(isinstance(v, float) for v in vector), str(vector))
    _check("★ 维度 == 声明的 dimension", len(vector) == 4, str(len(vector)))
    _check("  └ 值来自上游响应（不是占位）", vector == [4.0, 5.0, 6.0, 7.0], str(vector))
    _check("★ 恰好发出 1 次请求", fake.call_count == 1, str(fake.call_count))
    _check("  └ URL = base_url + /embeddings",
           fake.calls[0]["url"] == f"{DEFAULT_BASE_URL}/embeddings", fake.calls[0]["url"])
    _check("  └ 请求体含 model 与 input=[text]",
           fake.calls[0]["payload"] == {"model": DEFAULT_MODEL, "input": ["abcd"]},
           str(fake.calls[0]["payload"]))
    _check("  └ 头里带 Bearer 密钥", fake.calls[0]["headers"]["Authorization"] == f"Bearer {SECRET}")
    _check("  └ 头里带 Content-Type: application/json",
           fake.calls[0]["headers"]["Content-Type"] == "application/json")
    _check("  └ 超时按配置传入", fake.calls[0]["timeout"] == DEFAULT_TIMEOUT)
    _check("★ name 记录「哪次编码产生的」（写进 embedding_model）",
           svc.name == DEFAULT_MODEL, svc.name)

    _check("  └ 自定义 base_url / model 生效",
           _provider(base_url="https://x.test/v1", model="bge-m3",
                     transport=FakeTransport()).endpoint == "https://x.test/v1/embeddings"
           and _provider(model="bge-m3", transport=FakeTransport()).name == "bge-m3")

    # 入参校验沿用基类，且**不产生任何网络请求**
    for label, bad in {"空串": "", "纯空白": "   \n\t", "None": None, "整数": 123}.items():
        probe = FakeTransport()
        ok, info = await _raises(_provider(transport=probe).embed(bad), EmbeddingInputError)
        _check(f"★ 拒绝入参：{label}", ok and probe.call_count == 0,
               f"{info} | calls={probe.call_count}")

    # ------------------------------------------------------------
    # [3] 批量生成（要求 2）
    # ------------------------------------------------------------
    print("\n[3] 批量生成向量（要求 2）")
    texts = ["a", "bb", "ccc", "dddd", "eeeee"]
    fake = FakeTransport(dimension=4)
    svc = _provider(dimension=4, batch_size=2, transport=fake)
    batch = await svc.embed_batch(texts)

    _check("★ 返回条数与入参一致", isinstance(batch, list) and len(batch) == 5, str(len(batch)))
    _check("★ 每条形如单条 embed 的结果（维度一致）", all(len(v) == 4 for v in batch), str(batch))
    _check("★ 顺序与入参一致（逐条对应文本长度）",
           [v[0] for v in batch] == [1.0, 2.0, 3.0, 4.0, 5.0], str([v[0] for v in batch]))
    _check("★ 按 batch_size=2 分批 → 5 条 = 3 次请求", fake.call_count == 3, str(fake.call_count))
    _check("  └ 每批的 input 是正确切片",
           [c["payload"]["input"] for c in fake.calls] == [["a", "bb"], ["ccc", "dddd"], ["eeeee"]],
           str([c["payload"]["input"] for c in fake.calls]))
    _check("  └ 单批不超 batch_size",
           all(len(c["payload"]["input"]) <= 2 for c in fake.calls))

    fake1 = FakeTransport(dimension=4)
    _check("  └ batch_size=1 时逐条请求",
           len(await _provider(batch_size=1, transport=fake1).embed_batch(texts)) == 5
           and fake1.call_count == 5, str(fake1.call_count))
    _check("  └ batch_size 大于条数时只发 1 次",
           await _provider(batch_size=99, transport=FakeTransport()).embed_batch(texts)
           == await _provider(batch_size=2, transport=FakeTransport()).embed_batch(texts))

    _check("  └ tuple 入参同样可用",
           await _provider(transport=FakeTransport()).embed_batch(tuple(texts))
           == await _provider(transport=FakeTransport()).embed_batch(texts))

    probe = FakeTransport()
    _check("★ 空批次 → [] 且**不发起请求**",
           await _provider(transport=probe).embed_batch([]) == [] and probe.call_count == 0,
           str(probe.call_count))

    # index 乱序 → 按 index 还原
    def _reversed(payload):
        items = [
            {"index": i, "embedding": [float(len(t)) + j for j in range(4)]}
            for i, t in enumerate(payload["input"])
        ]
        return {"data": list(reversed(items))}

    out = await _provider(transport=FakeTransport(responder=_reversed)).embed_batch(["a", "bb", "ccc"])
    _check("★ 上游 data 乱序但带 index → 按 index 还原顺序",
           [v[0] for v in out] == [1.0, 2.0, 3.0], str([v[0] for v in out]))

    # 非法入参：校验发生在**任何 HTTP 之前**
    for label, bad in {"None": None, "整数": 123, "含 None 元素": ["ok", None],
                       "含空文本元素": ["ok", ""]}.items():
        probe = FakeTransport()
        ok, info = await _raises(_provider(transport=probe).embed_batch(bad), EmbeddingInputError)
        _check(f"★ 拒绝批量入参：{label}", ok and probe.call_count == 0,
               f"{info} | calls={probe.call_count}")

    probe = FakeTransport()
    ok, info = await _raises(_provider(transport=probe).embed_batch("你好"), EmbeddingInputError)
    _check("★ 把**单个字符串**传进批量接口被拒（否则会被逐字符拆开）",
           ok and probe.call_count == 0, f"{info} | calls={probe.call_count}")
    ok, info = await _raises(_provider(transport=FakeTransport()).embed_batch(["ok", None]),
                             EmbeddingInputError)
    _check("  └ 元素级错误指出是第几条", "第 1 条" in str(info), str(info))

    # ------------------------------------------------------------
    # [4] 异常处理（要求 3）
    # ------------------------------------------------------------
    print("\n[4] 异常处理（要求 3）")
    boom = EmbeddingUnavailableError("上游 503")
    ok, info = await _raises(_provider(transport=FakeTransport(raises=boom)).embed("x"),
                             EmbeddingUnavailableError)
    _check("★ transport 抛服务级异常 → 原样向上传播（不被吞掉 / 不换类型）",
           ok and info is boom, str(info))
    ok, info = await _raises(_provider(transport=FakeTransport(raises=boom)).embed_batch(["x"]),
                             EmbeddingUnavailableError)
    _check("  └ 批量路径同样传播", ok, str(info))

    # 2xx 但结构不对 → EmbeddingDimensionError
    for label, responder in {
        "非对象": lambda p: ["not-a-mapping"],
        "缺 data": lambda p: {"model": "x"},
        "data 是字符串": lambda p: {"data": "nope"},
        "条数不符": lambda p: {"data": [{"index": 0, "embedding": [1.0]},
                                        {"index": 1, "embedding": [2.0]}]},
        "元素缺 embedding": lambda p: {"data": [{"index": 0}]},
        "向量是字符串": lambda p: {"data": [{"index": 0, "embedding": "nope"}]},
        "向量含 bool": lambda p: {"data": [{"index": 0, "embedding": [True, False]}]},
        "向量含字符串元素": lambda p: {"data": [{"index": 0, "embedding": ["0.1", "0.2"]}]},
        "空向量": lambda p: {"data": [{"index": 0, "embedding": []}]},
    }.items():
        ok, info = await _raises(
            _provider(dimension=0, transport=FakeTransport(responder=responder)).embed("abcd"),
            EmbeddingDimensionError)
        _check(f"★ 2xx 但结构不对 → EmbeddingDimensionError：{label}", ok, str(info))

    ok, info = await _raises(_provider(dimension=8, transport=FakeTransport(dimension=4)).embed("abcd"),
                             EmbeddingDimensionError)
    _check("★ 上游维度与 EMBEDDING_DIMENSION 声明不符 → EmbeddingDimensionError", ok, str(info))
    _check("  └ 报错点明「换模型要同步改 dimension」",
           ok and "dimension" in str(info), str(info))
    _check("  └ dimension=0 时不校验维度（按上游返回）",
           len(await _provider(dimension=0, transport=FakeTransport(dimension=4)).embed("abcd")) == 4)

    # 上游把错误塞在 200 响应体里
    ok, info = await _raises(
        _provider(transport=FakeTransport(responder=lambda p: {"error": {"message": "quota"}})).embed("x"),
        EmbeddingUnavailableError)
    _check("★ 2xx 但响应体带 error → EmbeddingUnavailableError（不是当成正常结果）", ok, str(info))

    # --- 真实 HTTP 路径（httpx.MockTransport，不联网）---
    import httpx

    def _ok_handler(request):
        payload = httpx.Response(200, json={
            "data": [{"index": 0, "embedding": [0.5, 0.5, 0.5, 0.5]}]
        })
        return payload

    svc_http = _provider(dimension=4, transport=_httpx_transport(_ok_handler))
    _check("★ 走真实 httpx 客户端路径也能取到向量（MockTransport，不联网）",
           await svc_http.embed("abcd") == [0.5, 0.5, 0.5, 0.5])

    def _status_handler(code):
        def handler(request):
            return httpx.Response(code, json={"error": {"message": f"boom {code} {SECRET}"}})
        return handler

    for code in (401, 403, 404, 429, 500, 503):
        ok, info = await _raises(
            _provider(transport=_httpx_transport(_status_handler(code))).embed("x"),
            EmbeddingUnavailableError)
        _check(f"★ HTTP {code} → EmbeddingUnavailableError（带状态码）",
               ok and str(code) in str(info), str(info))
        _check(f"  └ HTTP {code} 的报错**不含密钥**（上游回显也要打码）",
               ok and SECRET not in str(info), str(info))

    ok, info = await _raises(
        _provider(transport=_httpx_transport(_status_handler(401))).embed("x"),
        EmbeddingUnavailableError)
    _check("  └ 401 的报错提示检查 EMBEDDING_API_KEY", ok and ENV_API_KEY in str(info), str(info))
    ok, info = await _raises(
        _provider(transport=_httpx_transport(_status_handler(404))).embed("x"),
        EmbeddingUnavailableError)
    _check("  └ 404 的报错提示检查 EMBEDDING_BASE_URL", ok and ENV_BASE_URL in str(info), str(info))

    def _non_json_handler(request):
        return httpx.Response(200, text="<html>not json</html>")

    ok, info = await _raises(
        _provider(transport=_httpx_transport(_non_json_handler)).embed("x"),
        EmbeddingDimensionError)
    _check("★ HTTP 200 但响应不是 JSON → EmbeddingDimensionError", ok, str(info))

    def _connect_error_handler(request):
        raise httpx.ConnectError("connection refused")

    ok, info = await _raises(
        _provider(transport=_httpx_transport(_connect_error_handler)).embed("x"),
        EmbeddingUnavailableError)
    _check("★ 网络失败 → EmbeddingUnavailableError（可重试/降级）", ok, str(info))

    def _timeout_handler(request):
        raise httpx.ReadTimeout("timed out")

    ok, info = await _raises(
        _provider(transport=_httpx_transport(_timeout_handler)).embed("x"),
        EmbeddingUnavailableError)
    _check("★ 超时 → EmbeddingUnavailableError 且消息点明超时",
           ok and "超时" in str(info), str(info))

    _check("★ 三类异常互不误捕（入参错不是 RuntimeError，服务错不是 ValueError）",
           not issubclass(EmbeddingInputError, RuntimeError)
           and not issubclass(EmbeddingUnavailableError, ValueError)
           and not issubclass(EmbeddingProviderConfigError, RuntimeError))
    _check("  └ 用 EmbeddingError 能一把捕获配置错与上游错",
           issubclass(EmbeddingProviderConfigError, EmbeddingError)
           and issubclass(EmbeddingUnavailableError, EmbeddingError))

    # ------------------------------------------------------------
    # [5] Mock 模式仍可用（要求 4）+ 离线实现一行未改
    # ------------------------------------------------------------
    print("\n[5] Mock 模式仍可用（要求 4）")
    mock = MockEmbeddingService(dimension=3, value=0.5)
    _check("★ MockEmbeddingService 仍可直接使用（返回固定向量）",
           await mock.embed("任意文本") == [0.5, 0.5, 0.5])
    _check("  └ 记录每次调用的文本（按被调次数断言，不比对数值）",
           mock.calls == ["任意文本"], str(mock.calls))
    _check("  └ 批量可用且顺序一致",
           await mock.embed_batch(["第一条", "第二条"]) == [[0.5, 0.5, 0.5]] * 2)
    _check("★ 经工厂按配置选 Mock（EMBEDDING_PROVIDER=mock）",
           isinstance(build_embedding_service(
               load_embedding_config({ENV_PROVIDER: PROVIDER_MOCK})), MockEmbeddingService))
    _check("  └ Mock 抛异常时原样传播（测异常路径用）",
           (await _raises(MockEmbeddingService(raises=EmbeddingUnavailableError("x")).embed("t"),
                          EmbeddingUnavailableError))[0])

    offline = HashEmbeddingService(dimension=64)
    v1 = await offline.embed("Redis 持久化")
    _check("★ 离线哈希占位实现行为不变（确定性，同文本同向量）",
           v1 == await offline.embed("Redis 持久化") and len(v1) == 64)
    _check("  └ 未联网、未使用密钥即可工作", HashEmbeddingService.name == "hash-local")

    # ------------------------------------------------------------
    # [6] 边界与守卫
    # ------------------------------------------------------------
    print("\n[6] 边界与守卫（AST 取证）")
    provider_path = BACKEND_DIR / "services" / "embedding_provider.py"
    src = provider_path.read_text(encoding="utf-8")
    mods = _imported_modules(src)
    top_level = {m.split(".")[0] for m in _module_level_imports(src)}

    _check("★ 模块顶层只有标准库 + services（真实实现不把 HTTP 客户端拉进顶层）",
           top_level == {"__future__", "os", "abc", "collections", "dataclasses",
                         "typing", "services"},
           str(sorted(top_level)))
    _check("★ httpx 是**延迟导入**（只在 post_json 函数体内）",
           "httpx" not in _module_level_imports(src) and "httpx" in mods,
           f"top={sorted(_module_level_imports(src))}")
    _check("  └ 未 import 数据层 / FastAPI / 向量层 / 面试侧",
           not any(m in mods for m in ("models", "database", "sqlalchemy", "fastapi",
                                       "deps", "main"))
           and not any("interview" in m or "vector_store" in m
                       or "knowledge_retriever" in m or "knowledge_rag" in m
                       or "knowledge_import" in m for m in mods),
           str(sorted(mods)))
    _check("  └ 只从 embedding_service 取接口与校验（复用同一处校验口径）",
           "services.embedding_service" in mods, str(sorted(mods)))
    _check("  └ 无全局单例（无模块级 EmbeddingService 实例）",
           not any(isinstance(getattr(ep, n), EmbeddingService) for n in dir(ep)))

    es_src = (BACKEND_DIR / "services" / "embedding_service.py").read_text(encoding="utf-8")
    _check("★ embedding_service 仍**零第三方依赖**（接口模块一行未改）",
           {m.split(".")[0] for m in _imported_modules(es_src)}
           == {"__future__", "abc", "collections", "hashlib", "re", "typing"},
           str(sorted({m.split(".")[0] for m in _imported_modules(es_src)})))

    # embedding_service 的消费者是**已知闭集**：
    #   * embedding_provider —— 实现方（继承基类 + 复用校验），这是「实现」不是「接线」
    #   * embedding_provider_spark —— 另一套协议的实现方（任务 75，同样继承基类 + 复用校验）
    #   * vector_knowledge_retriever —— 只 import 异常类型做映射
    # 注意 knowledge_rag **不在**这个集合里：它的 default_embedder() 现在委托给
    # embedding_provider 的工厂（「用哪个模型」收口在工厂一处），因此不再直接
    # 依赖任何**具体实现**——这正是我们想要的：换实现不用动组装器。
    consumers = []
    for path in _production_files():
        if path.name == "embedding_service.py":
            continue
        if "services.embedding_service" in _imported_modules(path.read_text(encoding="utf-8")):
            consumers.append(path.relative_to(BACKEND_DIR).as_posix())
    _check("★ embedding_service 的消费者是已知闭集（实现方 + 真实检索器）",
           consumers == ["services/embedding_provider.py",
                         "services/embedding_provider_spark.py",
                         "services/vector_knowledge_retriever.py"], str(consumers))
    _check("  └ 组装器不再直接依赖具体实现（经工厂选实现，换模型只改配置）",
           "services/embedding_service" not in _imported_modules(
               (BACKEND_DIR / "services" / "knowledge_rag.py").read_text(encoding="utf-8")))

    # 无 DATABASE_URL 也能 import（不拉数据层），且 httpx 未被拉进来
    probe = (
        "import sys;"
        f"sys.path.insert(0, r'{BACKEND_DIR}');"
        "import services.embedding_provider as ep;"
        "print('LEAK:' + ','.join(m for m in ('sqlalchemy', 'models', 'database', "
        "'fastapi', 'httpx', 'pydantic', 'aiomysql') if m in sys.modules));"
        "print('NAME:' + ep.EmbeddingProvider.name)"
    )
    env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
    result = subprocess.run([sys.executable, "-c", probe], capture_output=True,
                            text=True, cwd=str(BACKEND_DIR), env=env)
    lines = result.stdout.splitlines()
    leak = next((line[len("LEAK:"):] for line in lines if line.startswith("LEAK:")), "NO_OUTPUT")
    name = next((line[len("NAME:"):] for line in lines if line.startswith("NAME:")), "")
    _check("★ 无 DATABASE_URL 也能 import（顶层不碰数据层）",
           leak == "", f"leak={leak} rc={result.returncode} {result.stderr[-200:]}")
    _check("  └ 子进程里 httpx 也未被导入（延迟导入确实生效）", "httpx" not in leak, leak)
    _check("  └ 子进程成功取到类属性（证明真的导入成功，不是「导入失败所以没泄漏」）",
           name == "openai-compatible", f"name={name!r} rc={result.returncode}")

    # ------------------------------------------------------------
    # [7] 与组装器联动（配置即替换，默认行为不变）
    # ------------------------------------------------------------
    print("\n[7] 与组装器联动：knowledge_rag.default_embedder() 按配置选实现")
    from services.knowledge_rag import default_embedder

    with _env({ENV_PROVIDER: "", ENV_API_KEY: ""}):
        built = default_embedder()
    _check("★ 未配置 → 离线占位 HashEmbeddingService（默认行为逐字节不变）",
           isinstance(built, HashEmbeddingService) and built.name == "hash-local", repr(built))

    with _env({ENV_PROVIDER: "", ENV_API_KEY: SECRET, ENV_MODEL: "bge-m3", ENV_DIMENSION: "1024"}):
        built = default_embedder()
    _check("★ 配好密钥 → 真实 Provider（整条链路换模型只改配置）",
           isinstance(built, EmbeddingProvider) and built.name == "bge-m3"
           and built.dimension == 1024, repr(built))
    _check("  └ 端点由配置拼出", built.endpoint == f"{DEFAULT_BASE_URL}/embeddings", built.endpoint)

    with _env({ENV_PROVIDER: PROVIDER_MOCK, ENV_API_KEY: ""}):
        built = default_embedder()
    _check("  └ 显式 mock → 测试替身（离线单测仍可跑整条链路）",
           isinstance(built, MockEmbeddingService), repr(built))

    with _env({ENV_PROVIDER: PROVIDER_OPENAI, ENV_API_KEY: ""}):
        ok, info = _sync_raises(default_embedder, EmbeddingProviderConfigError)
    _check("★ openai 但缺密钥 → 构造期报错（读侧会静默降级为「无知识」，不伪装成检索不到）",
           ok, str(info))

    with _env({ENV_PROVIDER: "", ENV_API_KEY: ""}):
        _check("  └ 每次调用返回新对象（组装器也不做单例）",
               default_embedder() is not default_embedder())

    rag_src = (BACKEND_DIR / "services" / "knowledge_rag.py").read_text(encoding="utf-8")
    _check("★ 组装器仍是延迟导入（模块顶层不拉 embedding_provider / SQLAlchemy）",
           not ({"sqlalchemy", "models", "database", "services.vector_store_sql",
                 "services.embedding_provider", "services.embedding_service",
                 "services.vector_knowledge_retriever"} & _module_level_imports(rag_src)),
           str(sorted(_module_level_imports(rag_src))))

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
