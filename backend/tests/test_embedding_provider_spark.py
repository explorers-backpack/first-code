# -*- coding: utf-8 -*-
"""AI 面试知识库 · 讯飞星火 Embedding Provider（``services/embedding_provider_spark.py``）自检

无需 pytest，直接运行：
    python backend/tests/test_embedding_provider_spark.py

**全程不联网、不需要任何密钥**：HTTP 传输层可注入，签名时刻可传入固定值。
覆盖范围
--------
1. **模块与配置**：``SparkEmbeddingConfig`` 字段 / 不可变性 / ``repr`` 不泄露密钥；
   ``load_spark_config`` 的全部取值分支与校验（缺凭据 / 非法 domain / 非法数值）
2. **签名（逐字节可复现）**：用**测试自己写的** HMAC 公式独立复算，而不是只断言
   「返回了一个字符串」；固定 ``now`` ⇒ 同一输入必得同一 URL
3. **请求体**：``header`` / ``parameter.emb.domain`` / ``base64(JSON)`` 三条都要对
4. **单条与批量编码**：``embed`` 未被重写（签名逐字相同）、``embed_batch`` 被覆盖、
   顺序一致、非法入参**在任何 HTTP 之前**被拒
5. **解析**：base64 → **小端** float32（大端会解出完全不同的数）、维度校验、
   上游 ``header.code != 0`` 必须当失败、结构不对必须报错而不是静默返回空
6. **密钥安全**：异常消息里不得出现签名 URL 的 query string / api_key / api_secret；
   走**真实 httpx 客户端**（``MockTransport``，不联网）也要成立
7. **边界与守卫**：模块顶层只有标准库 + services；不 import 数据层 / FastAPI /
   ``interview_*`` / 向量层；源码里没有硬编码凭据；``.env.example`` 里没有真实凭据
8. **工厂分派与「默认行为不变」**：``EMBEDDING_PROVIDER=spark`` 才走本实现；
   **只配 ``SPARK_*``（`.env` 里本来就有）不得改变默认行为**；``role`` → ``domain`` 映射；
   读侧 ``build_vector_retriever`` 确实按 ``role="query"`` 取 embedder
"""

import ast
import asyncio
import base64
import contextlib
import hashlib
import hmac
import inspect
import json
import os
import pathlib
import re
import struct
import subprocess
import sys
from datetime import datetime
from time import mktime
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlsplit
from wsgiref.handlers import format_date_time

# 必须在 import 业务模块之前设置：SQLite 内存库，避免依赖本机 MySQL。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from services import embedding_provider as ep  # noqa: E402
from services import embedding_provider_spark as sp  # noqa: E402
from services.embedding_provider import (  # noqa: E402
    ENV_BASE_URL,
    ENV_DIMENSION,
    ENV_MODEL,
    ENV_PROVIDER,
    ENV_TIMEOUT,
    PROVIDER_HASH,
    PROVIDER_OPENAI,
    PROVIDER_SPARK,
    ROLE_DOCUMENT,
    ROLE_QUERY,
    EmbeddingProvider,
    EmbeddingProviderConfigError,
    EmbeddingTransport,
    HttpxEmbeddingTransport,
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
    describe_embedding,
)

_PASSED = 0
_FAILED = 0

APP_ID = "test-app-id"
API_KEY = "test-api-key-0123456789"
API_SECRET = "test-api-secret-abcdef"
FIXED_NOW = datetime(2026, 9, 27, 12, 0, 0)


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
    """只取**模块顶层**的 import，不含函数体内的延迟导入。"""
    names: set = set()
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _production_files():
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
    """临时设置环境变量（值为 ``""`` 表示**删除**该变量）。

    ⚠️ 入参必须是**映射**：``_env(ENV_PROVIDER="")`` 只会得到名为 ``"ENV_PROVIDER"``
    的字面量键，于是测试「静默地什么都没设置」并因错误的原因通过。
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
# 测试替身 / 工具
# ============================================================
def _vector_b64(values: List[float]) -> str:
    """把小端 float32 数组编成 base64（与上游响应的编码方式一致）。"""
    return base64.b64encode(struct.pack(f"<{len(values)}f", *values)).decode("ascii")


def _text_of(payload: Dict[str, Any]) -> str:
    """从请求体里取回明文文本（验证「base64(JSON) 里装的确实是这条文本」）。"""
    inner = json.loads(base64.b64decode(payload["payload"]["messages"]["text"]))
    return inner["messages"][0]["content"]


def _ok_response(values: List[float]) -> Dict[str, Any]:
    return {
        "header": {"code": 0, "message": "success", "sid": "test-sid"},
        "payload": {"feature": {
            "encoding": "utf8", "compress": "raw", "format": "plain",
            "seq": 0, "status": 3, "text": _vector_b64(values),
        }},
    }


class FakeTransport(EmbeddingTransport):
    """假 HTTP 传输：记录每次请求；默认按「文本长度」造一个可预测的向量。"""

    def __init__(self, *, dimension: int = sp.DEFAULT_DIMENSION,
                 raises: Optional[BaseException] = None, responder=None) -> None:
        self.dimension = dimension
        self.raises = raises
        self.responder = responder
        self.calls: List[Dict[str, Any]] = []

    @property
    def call_count(self) -> int:
        return len(self.calls)

    async def post_json(self, url, *, headers, payload, timeout):
        self.calls.append({
            "url": url, "headers": dict(headers),
            "payload": payload, "timeout": timeout,
        })
        if self.raises is not None:
            raise self.raises
        if self.responder is not None:
            return self.responder(payload, url)
        return _ok_response([float(len(_text_of(payload)))] * self.dimension)


class NoPostJsonTransport:
    """缺 ``post_json`` 的对象：用于验证构造期就拒绝非法 transport。"""


class EmbedderSpy:
    """哨兵 embedder：够 ``VectorKnowledgeRetriever`` 的构造校验（有 ``embed``），
    但不做任何事——本套件只用它取证「组装器把哪个对象交下去了」。"""

    name = "spy"

    async def embed(self, text: str) -> List[float]:  # pragma: no cover - 不会被调用
        return [0.0]


class StoreStub:
    """哨兵向量库：够构造校验（有 ``search``）。"""

    async def search(self, *args, **kwargs):  # pragma: no cover - 不会被调用
        return []


def _env_file_values() -> Dict[str, str]:
    """读 ``backend/.env`` 的键值（**不打印任何值**）；文件不存在则返回空。"""
    path = BACKEND_DIR / ".env"
    if not path.exists():
        return {}
    out: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.split("#", 1)[0].strip()
    return out


def _config(**overrides) -> sp.SparkEmbeddingConfig:
    base = dict(app_id=APP_ID, api_key=API_KEY, api_secret=API_SECRET)
    base.update(overrides)
    return sp.SparkEmbeddingConfig(**base)


def _provider(*, dimension: int = sp.DEFAULT_DIMENSION, transport=None,
              domain: str = sp.DOMAIN_PARA, **cfg_overrides) -> sp.SparkEmbeddingProvider:
    cfg = _config(dimension=dimension, domain=domain, **cfg_overrides)
    return sp.SparkEmbeddingProvider(cfg, transport=transport)


def _spark_env(**extra) -> Dict[str, str]:
    env = {
        ENV_PROVIDER: PROVIDER_SPARK,
        sp.ENV_APP_ID: APP_ID,
        sp.ENV_API_KEY: API_KEY,
        sp.ENV_API_SECRET: API_SECRET,
    }
    env.update(extra)
    return env


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 面试知识库 · 讯飞星火 Embedding Provider 自检")
    print("=" * 70)

    # ------------------------------------------------------------
    # [1] 模块与配置
    # ------------------------------------------------------------
    print("\n[1] 模块可导入 + 配置解析")
    _check("services.embedding_provider_spark 可导入",
           hasattr(sp, "SparkEmbeddingProvider"))
    _check("★ SparkEmbeddingProvider 是 EmbeddingService 的子类（换模型不换调用方式）",
           issubclass(sp.SparkEmbeddingProvider, EmbeddingService))
    _check("  └ 没有未实现的抽象方法（可直接实例化）",
           not getattr(sp.SparkEmbeddingProvider, "__abstractmethods__", None),
           str(getattr(sp.SparkEmbeddingProvider, "__abstractmethods__", None)))
    _check("★ SparkEmbeddingProviderConfigError 是 ValueError + EmbeddingError",
           issubclass(EmbeddingProviderConfigError, ValueError)
           and issubclass(EmbeddingProviderConfigError, EmbeddingError))
    _check("SparkEmbeddingTransport 实现了 EmbeddingTransport（同一个可注入接缝）",
           issubclass(sp.SparkEmbeddingTransport, EmbeddingTransport))
    _check("PROVIDER_SPARK 与本模块的取值一致（两处不能各写一份）",
           ep.PROVIDER_SPARK == sp.PROVIDER_SPARK == "spark")
    _check("★ role → domain 映射只有一张表（厂商词汇不散落）",
           sp.ROLE_TO_DOMAIN == {ROLE_DOCUMENT: sp.DOMAIN_PARA,
                                 ROLE_QUERY: sp.DOMAIN_QUERY},
           str(sp.ROLE_TO_DOMAIN))
    _check("__all__ 导出配置 / 实现 / 协议纯函数 / 常量",
           {"SparkEmbeddingConfig", "SparkEmbeddingProvider", "SparkEmbeddingTransport",
            "build_spark_embedding_service", "load_spark_config", "sign_url",
            "build_body", "decode_feature_text", "PROVIDER_SPARK",
            "DOMAIN_PARA", "DOMAIN_QUERY", "DEFAULT_DIMENSION"} <= set(sp.__all__))

    cfg = _config()
    _check("★ 默认 model / dimension / timeout / domain",
           cfg.model == sp.DEFAULT_MODEL and cfg.dimension == sp.DEFAULT_DIMENSION
           and cfg.timeout == sp.DEFAULT_TIMEOUT and cfg.domain == sp.DOMAIN_PARA,
           str(cfg))
    _check("★ 默认维度是实测值 2560（而不是 0 = 不校验）",
           sp.DEFAULT_DIMENSION == 2560, str(sp.DEFAULT_DIMENSION))
    _check("★ 默认模型名与离线占位区分开（否则入库会误判「已有可用向量」）",
           sp.DEFAULT_MODEL != HashEmbeddingService.name, sp.DEFAULT_MODEL)
    _check("  └ 默认 base_url 是讯飞 embedding 端点（不含 query）",
           cfg.base_url.startswith("https://emb-") and "?" not in cfg.base_url,
           cfg.base_url)
    _check("is_remote=True（本类只表示远程配置）", cfg.is_remote is True)

    # --- 密钥安全 ---
    _check("★ config 的 repr 不含 api_key / api_secret（dataclass 默认会全打出来）",
           API_KEY not in repr(cfg) and API_SECRET not in repr(cfg), repr(cfg))
    _check("  └ 只显示打码形式（保留末 4 位）",
           cfg.masked_api_key.endswith("6789") and API_KEY not in cfg.masked_api_key
           and cfg.masked_api_secret.endswith("cdef") and API_SECRET not in cfg.masked_api_secret,
           f"{cfg.masked_api_key} / {cfg.masked_api_secret}")
    _check("  └ 短凭据整体打码",
           sp.SparkEmbeddingConfig(api_key="ab").masked_api_key == "**")
    try:
        cfg.api_key = "x"
        _check("config 不可变（frozen dataclass）", False, "竟然赋值成功")
    except Exception:  # noqa: BLE001 - dataclasses.FrozenInstanceError
        _check("config 不可变（frozen dataclass）", True)

    # --- load_spark_config：正常路径 ---
    loaded = sp.load_spark_config(_spark_env())
    _check("★ 三项凭据齐备 → 配置读全",
           loaded.app_id == APP_ID and loaded.api_key == API_KEY
           and loaded.api_secret == API_SECRET, str(loaded))
    _check("  └ 缺省 role=document → domain=para",
           loaded.domain == sp.DOMAIN_PARA, loaded.domain)
    _check("  └ role=query → domain=query",
           sp.load_spark_config(_spark_env(), role=ROLE_QUERY).domain == sp.DOMAIN_QUERY)
    _check("  └ ENV_DOMAIN 可显式覆盖 role 映射（排障逃生口）",
           sp.load_spark_config(_spark_env(**{sp.ENV_DOMAIN: "query"})).domain
           == sp.DOMAIN_QUERY
           and sp.load_spark_config(_spark_env(**{sp.ENV_DOMAIN: "para"}),
                                    role=ROLE_QUERY).domain == sp.DOMAIN_PARA)

    # --- load_spark_config：逐项缺凭据 ---
    for missing, why in ((sp.ENV_APP_ID, "缺 APP_ID"),
                         (sp.ENV_API_KEY, "缺 API_KEY"),
                         (sp.ENV_API_SECRET, "缺 API_SECRET")):
        env = _spark_env()
        env[missing] = ""
        ok, info = _sync_raises(sp.load_spark_config, EmbeddingProviderConfigError, env)
        _check(f"★ 凭据不全被拒：{why}", ok and missing in str(info), str(info))
    env = _spark_env()
    env[sp.ENV_APP_ID] = "   "
    _check("  └ 纯空白也算缺失（不把空白当有效凭据）",
           _sync_raises(sp.load_spark_config, EmbeddingProviderConfigError, env)[0])

    # --- 两组凭据：专用组优先 / 逐项回落（Embedding 与文本模型解耦） ---
    # 讯飞的「文本大模型 X1」与「Embedding」是**两项独立授权**，同一个 AppId 未必
    # 都开通；因此 Embedding 需要自己的一组变量，同时**不能**让「只配 SPARK_*」的
    # 既有部署改变行为（本项目「不配 = 默认 = 行为不变」）。
    dedicated = {
        sp.ENV_SPARK_EMBEDDING_APP_ID: "emb-app",
        sp.ENV_SPARK_EMBEDDING_API_KEY: "emb-key",
        sp.ENV_SPARK_EMBEDDING_API_SECRET: "emb-secret",
    }
    base = _spark_env()

    cfg = sp.load_spark_config(base)
    _check("★ 只配回落组 SPARK_* → 取到的就是 SPARK_* 的值（既有部署行为不变）",
           (cfg.app_id, cfg.api_key, cfg.api_secret) == (APP_ID, API_KEY, API_SECRET),
           f"{cfg.app_id}/{cfg.api_key}/{cfg.api_secret}")

    cfg = sp.load_spark_config({**base, **dedicated})
    _check("★ 两组都配 → 专用组优先（Embedding 用自己那组，不与文本模型串味）",
           (cfg.app_id, cfg.api_key, cfg.api_secret) == ("emb-app", "emb-key", "emb-secret"),
           f"{cfg.app_id}/{cfg.api_key}/{cfg.api_secret}")

    cfg = sp.load_spark_config({**base, sp.ENV_SPARK_EMBEDDING_APP_ID: "emb-app"})
    _check("  └ 只补一个专用变量 → **逐项**回落（AppId 用专用、Key/Secret 用 SPARK_*）",
           (cfg.app_id, cfg.api_key, cfg.api_secret) == ("emb-app", API_KEY, API_SECRET),
           f"{cfg.app_id}/{cfg.api_key}/{cfg.api_secret}")

    # 变量名刻意不互为子串：否则「报错消息里是否提到某个变量名」这类断言会**恒真**
    _check("★ 专用名与回落名不互为子串（防「因错误的原因通过」）",
           sp.ENV_APP_ID not in sp.ENV_SPARK_EMBEDDING_APP_ID
           and sp.ENV_SPARK_EMBEDDING_APP_ID not in sp.ENV_APP_ID)

    ok, info = _sync_raises(sp.load_spark_config, EmbeddingProviderConfigError, {})
    _check("★ 两组都缺 → 报错同时点明专用名与回落名（排障不用翻源码）",
           ok and sp.ENV_SPARK_EMBEDDING_APP_ID in str(info) and sp.ENV_APP_ID in str(info),
           str(info))

    # --- load_spark_config：非法取值 ---
    for env, why in (
        ({sp.ENV_DOMAIN: "doc"}, "domain 取值非法"),
        ({sp.ENV_DOMAIN: "PARA "}, "domain 大小写/空白已归一（应合法）"),
        ({ENV_DIMENSION: "-1"}, "维度为负"),
        ({ENV_DIMENSION: "abc"}, "维度非整数"),
        ({ENV_TIMEOUT: "0"}, "超时为 0"),
        ({ENV_TIMEOUT: "soon"}, "超时非数字"),
        ({ENV_TIMEOUT: "61"}, f"超时超过上游上限 {sp.MAX_SESSION_SECONDS:g}s"),
        ({ENV_BASE_URL: "example.test/v1"}, "base_url 不是绝对地址"),
    ):
        ok, info = _sync_raises(sp.load_spark_config, EmbeddingProviderConfigError,
                                _spark_env(**env))
        if why.endswith("（应合法）"):
            _check(f"★ {why}", not ok, str(info))
        else:
            _check(f"★ 非法配置被拒：{why}", ok, str(info))
    # 空串 = 未配置（沿用 _read_str 口径：退回默认值，与 openai 路径一致）
    _check("  └ 空串 base_url → 退回默认端点（不是报错，与 EMBEDDING_* 既有口径一致）",
           sp.load_spark_config(_spark_env(**{ENV_BASE_URL: "   "})).base_url
           == sp.DEFAULT_BASE_URL)
    _check(f"  └ {ENV_TIMEOUT}={sp.MAX_SESSION_SECONDS:g} 恰好合法（边界闭）",
           sp.load_spark_config(_spark_env(**{ENV_TIMEOUT: "60"})).timeout == 60.0)
    ok, info = _sync_raises(sp.load_spark_config, EmbeddingProviderConfigError,
                            _spark_env(), role="problem")
    _check("★ 未知 role → 报错（不静默退回默认：那会表现为「召回莫名变差」）",
           ok and "role" in str(info), str(info))

    loaded = sp.load_spark_config(_spark_env(**{
        ENV_BASE_URL: "https://example.test/v2/", ENV_MODEL: "bge-m3",
        ENV_DIMENSION: "1024", ENV_TIMEOUT: "12.5",
    }))
    _check("★ 通用旋钮（base_url/model/dimension/timeout）逐项生效",
           loaded.base_url == "https://example.test/v2" and loaded.model == "bge-m3"
           and loaded.dimension == 1024 and loaded.timeout == 12.5, str(loaded))
    _check("  └ base_url 末尾的 / 被去掉（拼 path 不会出现 //）",
           loaded.base_url == "https://example.test/v2")

    # --- 构造期校验 ---
    ok, info = _sync_raises(sp.SparkEmbeddingProvider, EmbeddingProviderConfigError,
                            {"not": "a config"})
    _check("★ 非 SparkEmbeddingConfig → 构造期拒绝", ok, str(info))
    ok, info = _sync_raises(sp.SparkEmbeddingProvider, EmbeddingProviderConfigError,
                            _config(app_id=""))
    _check("  └ 缺 app_id → 拒绝（错误信息点明变量）",
           ok and sp.ENV_APP_ID in str(info), str(info))
    ok, info = _sync_raises(sp.SparkEmbeddingProvider, EmbeddingProviderConfigError,
                            _config(), transport=NoPostJsonTransport())
    _check("  └ transport 缺 post_json → 拒绝（错误信息说明契约）",
           ok and "post_json" in str(info), str(info))

    # ------------------------------------------------------------
    # [2] 签名（逐字节可复现）
    # ------------------------------------------------------------
    print("\n[2] HMAC-SHA256 签名（用测试自己写的公式独立复算）")
    url = sp.sign_url("https://emb-cn-huabei-1.xf-yun.com/", api_key=API_KEY,
                      api_secret=API_SECRET, now=FIXED_NOW)
    parts = urlsplit(url)
    query = parse_qs(parts.query)
    _check("★ 签名 URL 恰好带 authorization / date / host 三个参数",
           sorted(query) == ["authorization", "date", "host"], str(sorted(query)))
    _check("  └ host 参数 = base_url 的 host",
           query["host"] == ["emb-cn-huabei-1.xf-yun.com"], str(query["host"]))
    _check("  └ path 为 /（base_url 末尾的 / 被归一成 /）",
           parts.path == "/", parts.path)

    # 独立复算（**不是**调用被测函数）：完全按官方公式重写一遍
    host = parts.netloc
    date = format_date_time(mktime(FIXED_NOW.timetuple()))
    signing_text = f"host: {host}\ndate: {date}\nPOST / HTTP/1.1"
    expected_sig = base64.b64encode(
        hmac.new(API_SECRET.encode(), signing_text.encode(), hashlib.sha256).digest()
    ).decode()
    _check("★ date 是 RFC1123（本地时刻 → GMT，服务端允许 ±300s）",
           query["date"] == [date], f"{query['date']} vs {date}")
    origin = base64.b64decode(query["authorization"][0]).decode("utf-8")
    _check("★ authorization = base64(api_key=…, algorithm=…, headers=…, signature=…)",
           origin.startswith(f'api_key="{API_KEY}", algorithm="hmac-sha256", '
                             'headers="host date request-line", signature="'),
           origin[:70])
    _check("  └ signature 与独立复算逐字符相同（HMAC 口径没写歪）",
           f'signature="{expected_sig}"' in origin, expected_sig)
    _check("  └ headers 声明含 request-line（少一项服务端会拒）",
           'headers="host date request-line"' in origin)

    _check("★ 同一 now 同一输入 → 同一 URL（可复现，便于排障与回归）",
           sp.sign_url("https://emb-cn-huabei-1.xf-yun.com/", api_key=API_KEY,
                       api_secret=API_SECRET, now=FIXED_NOW) == url)
    _check("  └ now 变 → date 与 signature 都变",
           sp.sign_url("https://emb-cn-huabei-1.xf-yun.com/", api_key=API_KEY,
                       api_secret=API_SECRET, now=datetime(2026, 9, 27, 12, 0, 1)) != url)
    _check("  └ api_secret 变 → signature 变（密钥真的参与了签名）",
           sp.sign_url("https://emb-cn-huabei-1.xf-yun.com/", api_key=API_KEY,
                       api_secret=API_SECRET + "x", now=FIXED_NOW) != url)
    _check("  └ api_key 变 → authorization 变",
           sp.sign_url("https://emb-cn-huabei-1.xf-yun.com/", api_key=API_KEY + "x",
                       api_secret=API_SECRET, now=FIXED_NOW) != url)
    _check("  └ 原始 api_key 不出现在 URL 里（只以 base64 形式内嵌）",
           API_KEY not in url, "url 含明文密钥")

    # path 参与签名：签名文本里含 "POST {path} HTTP/1.1"，但那是 HMAC 的**输入**，
    # 从 URL 上看不见 ⇒ 只能用「换 path 就换签名」来取证（而不是去消息里找字面量）。
    root = sp.sign_url("https://example.test/", api_key=API_KEY,
                       api_secret=API_SECRET, now=FIXED_NOW)
    sub = sp.sign_url("https://example.test/v2/", api_key=API_KEY,
                      api_secret=API_SECRET, now=FIXED_NOW)
    root_auth = parse_qs(urlsplit(root).query)["authorization"][0]
    sub_auth = parse_qs(urlsplit(sub).query)["authorization"][0]
    _check("★ base_url 带子路径时 path 参与签名（不是写死 /）",
           urlsplit(sub).path == "/v2/" and root_auth != sub_auth,
           f"{urlsplit(sub).path} / same_auth={root_auth == sub_auth}")
    _check("  └ host 相同但 path 不同 ⇒ 只有签名不同（其余参数不受影响）",
           parse_qs(urlsplit(root).query)["host"] == parse_qs(urlsplit(sub).query)["host"]
           and parse_qs(urlsplit(root).query)["date"] == parse_qs(urlsplit(sub).query)["date"])
    ok, info = _sync_raises(sp.sign_url, EmbeddingProviderConfigError, "not-a-url",
                            api_key=API_KEY, api_secret=API_SECRET, now=FIXED_NOW)
    _check("  └ base_url 不是绝对地址 → 明确报错", ok, str(info))

    # ------------------------------------------------------------
    # [3] 请求体
    # ------------------------------------------------------------
    print("\n[3] 请求体（header / parameter.emb.domain / base64(JSON)）")
    body = sp.build_body("MySQL 索引", app_id=APP_ID, domain=sp.DOMAIN_QUERY)
    _check("★ header.app_id / uid / status 齐全",
           body["header"]["app_id"] == APP_ID and body["header"]["status"] == 3
           and bool(body["header"]["uid"]), str(body["header"]))
    _check("★ parameter.emb.domain 按入参（query / para 由用途决定）",
           body["parameter"]["emb"]["domain"] == sp.DOMAIN_QUERY)
    _check("  └ feature 三项 encoding/compress/format 固定为 utf8/raw/plain",
           body["parameter"]["emb"]["feature"]
           == {"encoding": "utf8", "compress": "raw", "format": "plain"},
           str(body["parameter"]["emb"]["feature"]))
    _check("★ payload.messages 是 base64(JSON)，且格式声明为 json",
           body["payload"]["messages"]["format"] == "json"
           and body["payload"]["messages"]["encoding"] == "utf8"
           and body["payload"]["messages"]["compress"] == "raw",
           str(body["payload"]["messages"]))
    _check("  └ 解出来恰是 messages[0] = {content: 原文, role: user}",
           json.loads(base64.b64decode(body["payload"]["messages"]["text"]))
           == {"messages": [{"content": "MySQL 索引", "role": "user"}]})
    _check("  └ 中文不被转义成 \\uXXXX（ensure_ascii=False，字节数可控）",
           "\\u" not in body["payload"]["messages"]["text"])

    probe = FakeTransport()
    svc = _provider(transport=probe, domain=sp.DOMAIN_QUERY, app_id=APP_ID)
    await svc.embed("abcd")
    sent = probe.calls[0]["payload"]
    _check("★ 真正发出的请求体：domain 取自 config、app_id 取自 config",
           sent["parameter"]["emb"]["domain"] == sp.DOMAIN_QUERY
           and sent["header"]["app_id"] == APP_ID)
    _check("  └ 请求头只有 Content-Type（凭据在 query，不在头里）",
           set(probe.calls[0]["headers"]) == {"Content-Type"}
           and probe.calls[0]["headers"]["Content-Type"] == "application/json",
           str(probe.calls[0]["headers"]))
    _check("  └ 超时按配置传入", probe.calls[0]["timeout"] == sp.DEFAULT_TIMEOUT)
    _check("  └ 实际请求的 URL 已签名（含 authorization）",
           "authorization=" in probe.calls[0]["url"])

    # ------------------------------------------------------------
    # [4] 单条与批量编码
    # ------------------------------------------------------------
    print("\n[4] 单条 / 批量编码")
    _check("★ embed(text) 未被重写 —— 接口不变",
           "embed" not in sp.SparkEmbeddingProvider.__dict__)
    _check("  └ 签名与基类逐字相同",
           inspect.signature(sp.SparkEmbeddingProvider.embed)
           == inspect.signature(EmbeddingService.embed))
    _check("  └ 参数恰为 (self, text)",
           list(inspect.signature(sp.SparkEmbeddingProvider.embed).parameters)
           == ["self", "text"])
    _check("★ embed_batch 被覆盖（上游只接受单条，覆盖点在于「先整体校验再逐条发」）",
           "embed_batch" in sp.SparkEmbeddingProvider.__dict__)

    fake = FakeTransport(dimension=sp.DEFAULT_DIMENSION)
    svc = _provider(transport=fake)
    vector = await svc.embed("abcd")
    _check("★ embed 返回 list[float]", isinstance(vector, list)
           and all(isinstance(v, float) for v in vector), str(type(vector)))
    _check("★ 维度 == 声明的 dimension（2560）", len(vector) == 2560, str(len(vector)))
    _check("  └ 值来自上游响应（不是占位）", vector[0] == 4.0 and vector[-1] == 4.0, str(vector[:2]))
    _check("★ 恰好发出 1 次请求", fake.call_count == 1, str(fake.call_count))
    _check("★ name 记录「哪次编码产生的」（写进 embedding_model）",
           svc.name == sp.DEFAULT_MODEL, svc.name)
    _check("  └ dimension 属性 == 配置声明值", svc.dimension == 2560, str(svc.dimension))
    _check("  └ endpoint 是**未签名**地址（可安全打印）",
           svc.endpoint == "https://emb-cn-huabei-1.xf-yun.com"
           and "authorization" not in svc.endpoint, svc.endpoint)
    _check("  └ describe_embedding 认它是语义实现",
           describe_embedding(svc).semantic_enabled is True
           and describe_embedding(svc).dimension == 2560,
           str(describe_embedding(svc)))

    _check("  └ 自定义 model 生效（写进 embedding_model 的就是它）",
           _provider(model="xinghuo-embedding-v2", transport=FakeTransport()).name
           == "xinghuo-embedding-v2")

    # 入参校验沿用基类，且**不产生任何网络请求**
    for label, bad in {"空串": "", "纯空白": "   \n\t", "None": None, "整数": 123}.items():
        probe = FakeTransport()
        ok, info = await _raises(_provider(transport=probe).embed(bad), EmbeddingInputError)
        _check(f"★ 拒绝入参：{label}", ok and probe.call_count == 0,
               f"{info} | calls={probe.call_count}")

    texts = ["a", "bb", "ccc", "dddd"]
    probe = FakeTransport()
    batch = await _provider(transport=probe).embed_batch(texts)
    _check("★ 批量返回条数与入参一致", len(batch) == 4, str(len(batch)))
    _check("★ 顺序与入参一致（逐条对应文本长度）",
           [v[0] for v in batch] == [1.0, 2.0, 3.0, 4.0], str([v[0] for v in batch]))
    _check("★ 上游只接受单条 ⇒ N 条 = N 次请求（逐条串行，不打并发）",
           probe.call_count == 4, str(probe.call_count))
    _check("  └ 每次请求体里装的确实是那一条文本",
           [_text_of(c["payload"]) for c in probe.calls] == texts,
           str([_text_of(c["payload"]) for c in probe.calls]))
    _check("  └ tuple 入参同样可用",
           await _provider(transport=FakeTransport()).embed_batch(tuple(texts))
           == await _provider(transport=FakeTransport()).embed_batch(texts))

    probe = FakeTransport()
    _check("★ 空批次 → [] 且**不发起请求**",
           await _provider(transport=probe).embed_batch([]) == []
           and probe.call_count == 0, str(probe.call_count))

    for label, bad in {"None": None, "整数": 123, "含 None 元素": ["ok", None],
                       "含空文本元素": ["ok", ""]}.items():
        probe = FakeTransport()
        ok, info = await _raises(_provider(transport=probe).embed_batch(bad),
                                 EmbeddingInputError)
        _check(f"★ 拒绝批量入参：{label}", ok and probe.call_count == 0,
               f"{info} | calls={probe.call_count}")
    probe = FakeTransport()
    ok, info = await _raises(_provider(transport=probe).embed_batch("你好"),
                             EmbeddingInputError)
    _check("★ 把**单个字符串**传进批量接口被拒（否则会被逐字符拆开）",
           ok and probe.call_count == 0, f"{info} | calls={probe.call_count}")
    ok, info = await _raises(_provider(transport=FakeTransport()).embed_batch(["ok", None]),
                             EmbeddingInputError)
    _check("  └ 元素级错误指出是第几条（复用基类同一处校验）",
           "第 1 条" in str(info), str(info))

    # ------------------------------------------------------------
    # [5] 响应解析
    # ------------------------------------------------------------
    print("\n[5] 响应解析（base64 → 小端 float32）")
    _check("★ 小端解析：float32 的 1.0 / -2.5 / 0.25 逐位还原",
           sp.decode_feature_text(_vector_b64([1.0, -2.5, 0.25])) == [1.0, -2.5, 0.25])
    big = base64.b64encode(struct.pack(">3f", 1.0, -2.5, 0.25)).decode()
    _check("★ 大端字节流解出来**不是**同一组数（证明真的按小端解）",
           sp.decode_feature_text(big) != [1.0, -2.5, 0.25],
           str(sp.decode_feature_text(big)))
    _check("  └ 维度 = 字节数 / 4",
           len(sp.decode_feature_text(_vector_b64([0.0] * 2560))) == 2560)

    for label, bad in {"None": None, "空串": "", "纯空白": "   ",
                       "非 base64（解出 0 字节）": "!!!!",
                       "字节数不是 4 的倍数": base64.b64encode(b"abc").decode()}.items():
        ok, info = _sync_raises(sp.decode_feature_text, EmbeddingDimensionError, bad)
        _check(f"★ 非法 feature.text 被拒：{label}", ok, str(info))

    # 2xx 但结构不对
    for label, responder in {
        "非对象": lambda p, u: ["not-a-mapping"],
        "缺 payload": lambda p, u: {"header": {"code": 0}},
        "缺 feature": lambda p, u: {"header": {"code": 0}, "payload": {}},
        "缺 feature.text": lambda p, u: {"header": {"code": 0},
                                         "payload": {"feature": {"format": "plain"}}},
        "feature 是字符串": lambda p, u: {"header": {"code": 0}, "payload": {"feature": "x"}},
    }.items():
        ok, info = await _raises(
            _provider(transport=FakeTransport(responder=responder)).embed("abcd"),
            EmbeddingDimensionError)
        _check(f"★ 2xx 但结构不对 → EmbeddingDimensionError：{label}", ok, str(info))

    ok, info = await _raises(
        _provider(dimension=1024, transport=FakeTransport(dimension=2560)).embed("abcd"),
        EmbeddingDimensionError)
    _check("★ 上游维度与 EMBEDDING_DIMENSION 声明不符 → EmbeddingDimensionError", ok, str(info))
    _check("  └ 报错点明「换模型要同步改 dimension」",
           ok and "dimension" in str(info), str(info))
    _check("  └ dimension=0 时不校验维度（按上游返回）",
           len(await _provider(dimension=0, transport=FakeTransport(dimension=8)).embed("ab")) == 8)

    # 上游把业务错误塞在 200 响应体里
    for code, must_hint in ((10313, sp.ENV_APP_ID), (11200, sp.ENV_APP_ID),
                            (11202, "重试"), (10139, "domain")):
        ok, info = await _raises(
            _provider(transport=FakeTransport(responder=lambda p, u, c=code: {
                "header": {"code": c, "message": "boom"}, "payload": {}
            })).embed("x"),
            EmbeddingUnavailableError)
        _check(f"★ 2xx 但 header.code={code} → EmbeddingUnavailableError（不当成成功）",
               ok and str(code) in str(info), str(info))
        _check(f"  └ code={code} 附排查提示（点明该查什么）",
               ok and must_hint in str(info), str(info))

    ok, info = await _raises(
        _provider(transport=FakeTransport(responder=lambda p, u: {
            "header": {"code": 99999, "message": "unknown thing"}, "payload": {}
        })).embed("x"), EmbeddingUnavailableError)
    _check("  └ 未知错误码不臆测原因，只回显 message",
           ok and "unknown thing" in str(info) and "99999" in str(info), str(info))
    ok, info = await _raises(
        _provider(transport=FakeTransport(responder=lambda p, u: {"payload": {}})).embed("x"),
        EmbeddingUnavailableError)
    _check("  └ 缺 header 也算失败（不把「没写 code」当 code=0）", ok, str(info))

    # ------------------------------------------------------------
    # [6] 密钥安全（异常消息里不得出现凭据）
    # ------------------------------------------------------------
    print("\n[6] 密钥安全（异常消息 / 日志 / repr）")

    class _EchoUrlTransport(EmbeddingTransport):
        """把**实际收到的 URL** 原样回显进异常消息（模拟默认传输的超时消息）。

        必须回显「真实传入的 URL」而不是测试自己拼一个：签名带时间戳，
        自拼的 URL 与 provider 实际发出的那条**不是同一个字符串**，
        那样的用例会因为「抹不掉自己造的串」而假失败。
        """

        def __init__(self):
            self.seen: List[str] = []

        async def post_json(self, url, *, headers, payload, timeout):
            self.seen.append(url)
            raise EmbeddingUnavailableError(f"Embedding 请求超时（{timeout}s）：{url}")

    echo = _EchoUrlTransport()
    wrapped = sp.SparkEmbeddingTransport(inner=echo)
    ok, info = await _raises(
        _provider(transport=wrapped).embed("x"), EmbeddingUnavailableError)
    sent_url = echo.seen[0] if echo.seen else ""
    sent_query = urlsplit(sent_url).query
    _check("★ 传输层把签名 URL 的 query string 整段抹掉（凭据可逆，不能进消息）",
           ok and sent_query and sent_query not in str(info)
           and "authorization=" not in str(info),
           str(info)[:160])
    _check("  └ 未签名部分（base_url）保留 —— 抹的是凭据不是整条消息",
           ok and "emb-cn-huabei-1.xf-yun.com" in str(info), str(info)[:160])
    _check("  └ 异常**类型**不变（调用方的 except 仍然有效）",
           ok and isinstance(info, EmbeddingUnavailableError))
    _check("  └ 原始异常仍挂在 __cause__ 上（排障不丢信息）",
           isinstance(getattr(info, "__cause__", None), EmbeddingUnavailableError),
           str(getattr(info, "__cause__", None)))
    _check("  └ 消息里也不出现明文 api_key / api_secret",
           API_KEY not in str(info) and API_SECRET not in str(info), str(info)[:160])

    ok, info = await _raises(
        sp.SparkEmbeddingTransport(inner=FakeTransport(
            raises=EmbeddingDimensionError("结构不对"))).post_json(
                "https://x.test/", headers={}, payload={}, timeout=1.0),
        EmbeddingDimensionError)
    _check("  └ 消息里没有可抹的东西时**原样抛出**（不无谓换异常对象）", ok, str(info))

    _check("★ provider 的 repr 不含凭据",
           API_KEY not in repr(_provider()) and API_SECRET not in repr(_provider()),
           repr(_provider())[:120])

    # --- 真实 httpx 路径（MockTransport，不联网）---
    import httpx

    def _http_transport(handler):
        return sp.SparkEmbeddingTransport(
            client_factory=lambda **kw: httpx.AsyncClient(
                transport=httpx.MockTransport(handler), **kw
            )
        )

    def _ok_handler(request):
        seen_urls.append(str(request.url))
        return httpx.Response(200, json=_ok_response([0.5] * 2560))

    seen_urls: List[str] = []
    svc_http = _provider(transport=_http_transport(_ok_handler))
    got = await svc_http.embed("abcd")
    _check("★ 走真实 httpx 客户端路径也能取到向量（MockTransport，不联网）",
           len(got) == 2560 and got[0] == 0.5)
    _check("  └ 真实路径下请求 URL 确实带了签名参数（不是只拼了 base_url）",
           len(seen_urls) == 1 and "authorization=" in seen_urls[0]
           and "host=" in seen_urls[0], str(seen_urls)[:120])

    def _status_handler(request):
        return httpx.Response(500, json={"header": {"code": 11202, "message": "licc failed"}})

    ok, info = await _raises(
        _provider(transport=_http_transport(_status_handler)).embed("x"),
        EmbeddingUnavailableError)
    _check("★ HTTP 500 → EmbeddingUnavailableError（可重试/降级）",
           ok and "500" in str(info), str(info))
    _check("  └ 该消息里不含 query string / 凭据",
           ok and "authorization=" not in str(info) and API_KEY not in str(info),
           str(info)[:160])

    def _connect_error_handler(request):
        raise httpx.ConnectError("connection refused")

    ok, info = await _raises(
        _provider(transport=_http_transport(_connect_error_handler)).embed("x"),
        EmbeddingUnavailableError)
    _check("★ 网络失败 → EmbeddingUnavailableError", ok, str(info))

    def _non_json_handler(request):
        return httpx.Response(200, text="<html>not json</html>")

    ok, info = await _raises(
        _provider(transport=_http_transport(_non_json_handler)).embed("x"),
        EmbeddingDimensionError)
    _check("★ HTTP 200 但响应不是 JSON → EmbeddingDimensionError", ok, str(info))

    # ------------------------------------------------------------
    # [7] 边界与守卫（AST 取证）
    # ------------------------------------------------------------
    print("\n[7] 边界与守卫（AST 取证）")
    spark_path = BACKEND_DIR / "services" / "embedding_provider_spark.py"
    src = spark_path.read_text(encoding="utf-8")
    mods = _imported_modules(src)
    top_level = {m.split(".")[0] for m in _module_level_imports(src)}
    _check("★ 模块顶层只有标准库 + services（HTTP 客户端不在顶层）",
           top_level == {"__future__", "base64", "hashlib", "hmac", "json", "os",
                         "struct", "collections", "dataclasses", "datetime", "time",
                         "typing", "urllib", "wsgiref", "services"},
           str(sorted(top_level)))
    _check("  └ 未 import 数据层 / FastAPI / 向量层 / 面试侧 / 组装器",
           not any(m in mods for m in ("models", "database", "sqlalchemy", "fastapi",
                                       "deps", "main", "httpx"))
           and not any("interview" in m or "vector_store" in m
                       or "knowledge_retriever" in m or "knowledge_rag" in m
                       or "knowledge_import" in m for m in mods),
           str(sorted(mods)))
    _check("  └ 只从 embedding_service 取接口与校验 + 从 embedding_provider 取传输与常量",
           "services.embedding_service" in mods and "services.embedding_provider" in mods,
           str(sorted(mods)))
    _check("  └ 无全局单例（无模块级 EmbeddingService 实例）",
           not any(isinstance(getattr(sp, n), EmbeddingService) for n in dir(sp)))

    # 源码里不得硬编码凭据。
    # ⚠️ 反例 token 必须**运行时拼出**：写成字面量它就出现在本文件源码里，
    # 于是「应该查不到」的 token 被自己查到（项目陷阱：守卫不得写出自己要找的 token）。
    # 另外正则刻意**排除花括号**：`api_key="{api_key}"` 这种 f-string 示例不是凭据。
    cred_pattern = re.compile(r"(api_key|api_secret|app_id)\s*=\s*['\"][A-Za-z0-9_\-]{12,}['\"]")
    sample = "api_" + "key" + "='abcdefghijklmnop'"
    _check("守卫自检：凭据正则能区分「有」与「无」",
           bool(cred_pattern.search(sample))
           and not cred_pattern.search("api_key = value")
           and not cred_pattern.search('api_key="{api_key}"'))
    _check("★ 源码里没有硬编码凭据", not cred_pattern.search(src),
           str(cred_pattern.search(src)))

    # 更强的取证：若 backend/.env 提供了真实凭据，则它**一个都不能**出现在源码 / 模板里
    # （只报键名，绝不打印值）
    real = _env_file_values()
    cred_keys = (sp.ENV_APP_ID, sp.ENV_API_KEY, sp.ENV_API_SECRET,
                 sp.ENV_SPARK_EMBEDDING_APP_ID, sp.ENV_SPARK_EMBEDDING_API_KEY,
                 sp.ENV_SPARK_EMBEDDING_API_SECRET)
    present = [k for k in cred_keys if real.get(k)]
    example_text = (BACKEND_DIR / ".env.example").read_text(encoding="utf-8")
    if present:
        leaked = [k for k in present
                  if real[k] in src or real[k] in example_text]
        _check("★ backend/.env 里的真实凭据（两组都查）未出现在源码 / .env.example 中",
               not leaked, str(leaked))
    else:
        _check("（跳过）backend/.env 未提供 SPARK_* 真实凭据 —— 跳过该取证", True)

    for name in (sp.ENV_APP_ID, sp.ENV_API_KEY, sp.ENV_API_SECRET,
                 sp.ENV_SPARK_EMBEDDING_APP_ID, sp.ENV_SPARK_EMBEDDING_API_KEY,
                 sp.ENV_SPARK_EMBEDDING_API_SECRET, ep.ENV_API_KEY):
        occurrences = [
            line.strip() for line in example_text.splitlines()
            if re.match(rf"^#?\s*{name}\s*=", line.strip())
        ]
        # 值（去掉行尾注释后）必须为空：注释掉也算安全，但**不能**带真实值
        values = [ln.split("=", 1)[1].split("#", 1)[0].strip() for ln in occurrences]
        _check(f"★ .env.example 的 {name} 只放变量名 / 空值（不带真实凭据）",
               bool(occurrences) and all(v == "" for v in values), repr(occurrences))

    # 无 DATABASE_URL 也能 import，且 httpx 未被拉进来
    probe = (
        "import sys;"
        f"sys.path.insert(0, r'{BACKEND_DIR}');"
        "import services.embedding_provider_spark as sp;"
        "print('LEAK:' + ','.join(m for m in ('sqlalchemy', 'models', 'database', "
        "'fastapi', 'httpx', 'pydantic', 'aiomysql') if m in sys.modules));"
        "print('NAME:' + sp.SparkEmbeddingProvider.name)"
    )
    env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
    result = subprocess.run([sys.executable, "-c", probe], capture_output=True,
                            text=True, cwd=str(BACKEND_DIR), env=env)
    lines = result.stdout.splitlines()
    leak = next((ln[len("LEAK:"):] for ln in lines if ln.startswith("LEAK:")), "NO_OUTPUT")
    name = next((ln[len("NAME:"):] for ln in lines if ln.startswith("NAME:")), "")
    _check("★ 无 DATABASE_URL 也能 import（顶层不碰数据层）",
           leak == "", f"leak={leak} rc={result.returncode} {result.stderr[-200:]}")
    _check("  └ 子进程里 httpx 也未被导入（它只在 embedding_provider 的传输里延迟导入）",
           "httpx" not in leak, leak)
    _check("  └ 子进程真的导入成功（不是「导入失败所以没泄漏」）",
           name == sp.DEFAULT_MODEL, f"name={name!r} rc={result.returncode}")

    # ------------------------------------------------------------
    # [8] 工厂分派与「默认行为不变」
    # ------------------------------------------------------------
    print("\n[8] 工厂分派 / role 映射 / 默认行为不变")
    built = build_embedding_service(env=_spark_env())
    _check("★ EMBEDDING_PROVIDER=spark → SparkEmbeddingProvider",
           isinstance(built, sp.SparkEmbeddingProvider)
           and not isinstance(built, EmbeddingProvider), type(built).__name__)
    _check("  └ 缺省 role → domain=para（写侧：知识原文）",
           built.domain == sp.DOMAIN_PARA, built.domain)
    _check("★ role=query → domain=query（读侧：用户问题）",
           build_embedding_service(env=_spark_env(), role=ROLE_QUERY).domain
           == sp.DOMAIN_QUERY)
    _check("  └ 别名 iflytek / xinghuo / XFYUN 都归到 spark",
           all(isinstance(build_embedding_service(env=_spark_env(**{ENV_PROVIDER: alias})),
                          sp.SparkEmbeddingProvider)
               for alias in ("iflytek", "xinghuo", "XFYUN")))
    _check("  └ 不做全局单例（每次返回新对象）",
           build_embedding_service(env=_spark_env())
           is not build_embedding_service(env=_spark_env()))
    ok, info = _sync_raises(build_embedding_service, EmbeddingProviderConfigError,
                            env=_spark_env(), role="problem")
    _check("  └ 非法 role 在工厂层就被拒（不静默退回默认）", ok, str(info))
    ok, info = _sync_raises(build_embedding_service, EmbeddingProviderConfigError,
                            env=_spark_env(**{sp.ENV_API_SECRET: ""}))
    _check("  └ 缺凭据在构造期就报（读侧会静默降级为「无知识」，不伪装成检索不到）",
           ok, str(info))

    # ★★ 关键：只配 SPARK_* 不得改变默认行为（.env 里本来就有这三行）
    only_spark = {k: v for k, v in _spark_env().items() if k != ENV_PROVIDER}
    _check("★★ 只配 SPARK_*（无 EMBEDDING_PROVIDER）→ 仍是离线占位（默认行为逐字节不变）",
           isinstance(build_embedding_service(env=only_spark), HashEmbeddingService),
           type(build_embedding_service(env=only_spark)).__name__)
    _check("★ 空环境 → 离线占位",
           isinstance(build_embedding_service(env={}), HashEmbeddingService))
    _check("  └ 配 EMBEDDING_API_KEY → 仍是 openai 兼容实现（没被 spark 抢走）",
           isinstance(build_embedding_service(env={ep.ENV_API_KEY: "sk-x"}),
                      EmbeddingProvider))
    ok, info = _sync_raises(load_embedding_config, EmbeddingProviderConfigError,
                            {ENV_PROVIDER: PROVIDER_SPARK})
    _check("★ load_embedding_config 明确拒绝 spark（别拿 OpenAI 字段装 spark 的配置）",
           ok and "build_embedding_service" in str(info), str(info))
    _check("  └ 显式传 provider=spark 的 config 也会被工厂分派到 spark 实现",
           isinstance(build_embedding_service(
               ep.EmbeddingProviderConfig(provider=PROVIDER_SPARK), env=_spark_env()),
               sp.SparkEmbeddingProvider))

    # --- 组装器：写侧默认 document、读侧必须 query ---
    from services import knowledge_rag

    with _env({ENV_PROVIDER: "", ep.ENV_API_KEY: ""}):
        _check("★ default_embedder() 未配置 → 离线占位（role 被忽略，无副作用）",
               isinstance(knowledge_rag.default_embedder(), HashEmbeddingService))
        _check("  └ 对称实现下 role=query 也不改变实现（传了不会炸）",
               isinstance(knowledge_rag.default_embedder(role=ROLE_QUERY),
                          HashEmbeddingService))

    sentinel = EmbedderSpy()
    seen: List[Any] = []

    def _spy_default_embedder(*, role=None):
        seen.append(role)
        return sentinel

    def _spy_build_vector_store(db, *, model=None, backend=None):
        return StoreStub()

    saved = (knowledge_rag.default_embedder, knowledge_rag.build_vector_store)
    try:
        knowledge_rag.default_embedder = _spy_default_embedder
        knowledge_rag.build_vector_store = _spy_build_vector_store
        retriever = knowledge_rag.build_vector_retriever(None)
        _check("★★ 读侧 build_vector_retriever 确实按 role='query' 取 embedder"
               "（运行时取证：非对称实现必须配 query，配错只是掉召回、不报错）",
               seen == [ROLE_QUERY], str(seen))
        _check("  └ 拿到的正是这个 embedder（没有被悄悄换成默认值）",
               retriever.embedder is sentinel, repr(getattr(retriever, "embedder", None)))
        seen.clear()
        knowledge_rag.build_vector_retriever(None, embedder=sentinel)
        _check("  └ 显式注入 embedder 时不再调 default_embedder（注入优先）",
               seen == [], str(seen))
    finally:
        knowledge_rag.default_embedder, knowledge_rag.build_vector_store = saved

    _check("  └ 写侧入库 Pipeline 用默认 role（document），与读侧区分开",
           "default_embedder()" in (BACKEND_DIR / "services"
                                    / "knowledge_import_pipeline.py").read_text(
                                        encoding="utf-8")
           and "ROLE_QUERY" not in (BACKEND_DIR / "services"
                                    / "knowledge_import_pipeline.py").read_text(
                                        encoding="utf-8"))

    rag_src = (BACKEND_DIR / "services" / "knowledge_rag.py").read_text(encoding="utf-8")
    _check("★ 组装器仍不在顶层 import 任何具体实现（经工厂取实现）",
           not ({"services.embedding_provider", "services.embedding_provider_spark",
                 "services.embedding_service"} & _module_level_imports(rag_src)),
           str(sorted(_module_level_imports(rag_src))))

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
