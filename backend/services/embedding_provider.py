# -*- coding: utf-8 -*-
"""AI 面试知识库 · **真实 Embedding Provider**（OpenAI 兼容 ``/embeddings`` 协议）。

为什么单独一个模块（而不是塞进 ``embedding_service``）
------------------------------------------------------
``services/embedding_service.py`` 的**零第三方依赖**是被守卫测试锁死的硬约束
（AST 断言「模块顶层 import 恰为标准库、且模块名里没有任何厂商 / HTTP 客户端」）。
真实实现必然要发 HTTP，塞进去会当场破坏那条守卫，并把「可脱离网络单测」的性质
一起毁掉——与任务 44 把真实检索器放进 ``vector_knowledge_retriever.py`` 而不是
``knowledge_retriever.py`` 是**同一条理由**。

于是本模块与接口模块的分工是：

::

    services/embedding_service.py      接口 + 校验 + 离线占位实现（零依赖，一行未改）
        └── services/embedding_provider.py   【本模块】真实 Provider + 配置 + 工厂
              └── knowledge_rag.default_embedder()   组装器（唯一决定「用哪个模型」）
                    ├── 读侧 build_vector_retriever → interview_core.resolve_retriever
                    └── 写侧 build_vector_store     → knowledge_import_pipeline

**接口不变**：本模块**不重写** ``embed(text)``——:class:`EmbeddingProvider` 只实现基类的
``_embed_one`` 钩子，因此 ``embed`` 的签名、入参校验、出参校验全部沿用基类那一套
（"换模型不换错误口径"）。只有 ``embed_batch`` 被**覆盖**（厂商原生批量接口是真实性能来源）。

配置方式（全部走环境变量，密钥只写在 ``backend/.env``）
--------------------------------------------------------
==============================  ==========================  ==========================================
变量                            默认值                      说明
==============================  ==========================  ==========================================
``EMBEDDING_PROVIDER``          *(空 → 自动)*               ``hash`` / ``mock`` / ``openai`` / ``spark``；
                                                            留空时：配了 ``EMBEDDING_API_KEY`` → ``openai``，
                                                            否则 → ``hash``（**离线，默认行为不变**）。
                                                            ``spark`` **必须显式写**——`SPARK_*` 的存在
                                                            不会触发自动切换（那会让「只配了文本模型
                                                            凭据」的部署悄悄改变检索行为）
``EMBEDDING_API_KEY``           *(空)*                      密钥；``openai`` 时必填，**禁止提交**
``EMBEDDING_BASE_URL``          ``https://api.openai.com/v1``  OpenAI 兼容服务地址（不含 ``/embeddings``）
``EMBEDDING_MODEL``             ``text-embedding-3-small``  模型名；写进 ``KnowledgeChunk.embedding_model``
``EMBEDDING_DIMENSION``         ``0``                       声明维度；``0`` = 不校验（按上游返回）
``EMBEDDING_TIMEOUT``           ``30``                      单次请求超时（秒）
``EMBEDDING_BATCH_SIZE``        ``16``                      单次请求最多几条文本
==============================  ==========================  ==========================================

**默认行为零变化**：一个变量都不配时 :func:`build_embedding_service` 返回
``HashEmbeddingService``（离线哈希占位）——不联网、不需要密钥、既有测试与离线开发不受影响。
一旦配上 ``EMBEDDING_API_KEY``（或显式 ``EMBEDDING_PROVIDER=openai``），整条 RAG 链路
（读侧检索 + 写侧入库）就都换成真实模型——因为**两边都从 ``knowledge_rag.default_embedder()`` 拿**，
「用哪个模型」仍然只有一处说法，不会出现「入库用 A、检索用 B」而检索不到的情况。

**第二种真实协议（讯飞星火）**：``EMBEDDING_PROVIDER=spark`` 走
``services/embedding_provider_spark.py``——它的凭据 / 端点 / 请求体 / 响应格式都与
OpenAI 兼容协议不同，因此**单独一个模块**，本模块只负责「按配置分派」。
两条真实协议共用 ``EMBEDDING_BASE_URL`` / ``_MODEL`` / ``_DIMENSION`` / ``_TIMEOUT`` 这四个
**与协议无关**的旋钮名，因此「换模型」的配置习惯不变。

**role（非对称编码）**：讯飞把「用户问题」与「知识原文」分两个 ``domain``，配错了不报错、
只是掉召回。接口 ``embed(text)`` 分不出用途，所以由 :data:`ROLE_DOCUMENT` / :data:`ROLE_QUERY`
在**实例**上区分（写侧入库用 ``document``、读侧检索用 ``query``）。
对称实现（hash / mock / openai）**忽略 role**，传了也没有副作用。

异常映射（沿用 ``embedding_service`` 的四分类，不另起一套）
------------------------------------------------------------
=================================  ================================================
上游情况                            抛什么
=================================  ================================================
网络失败 / 超时                      ``EmbeddingUnavailableError``（+``RuntimeError``，可重试/降级）
HTTP 非 2xx（401 / 403 / 404 /      同上，消息里带状态码与响应片段（**密钥会被打码**）
429 / 5xx / 其它）
2xx 但结构不对（不是对象 / 没有      ``EmbeddingDimensionError``（+``ValueError``，实现或上游数据问题）
``data`` / 条数不符 / 元素不是
``{"embedding": …}`` / 向量非数值
序列 / 空向量 / 与声明的维度不符）
入参是空文本 / 纯空白 / 非字符串     ``EmbeddingInputError``（+``ValueError``，**调用方改代码，不该重试**）
把单个字符串传给 ``embed_batch``    同上（``str`` 可迭代，不挡会被逐字符拆开）
配置本身不合法（非法 provider /     ``EmbeddingProviderConfigError``（+``ValueError``，**构造期就报**）
``openai`` 却没密钥 / 维度或超时
非正整数 / 批次大小 < 1）
=================================  ================================================

**密钥安全**：``EmbeddingProviderConfig.api_key`` 声明为 ``repr=False``，
``__repr__`` 只显示打码后的形式；所有异常消息都会把密钥替换成 ``***``。

怎么在**无网络**环境测试
------------------------
HTTP 客户端是可注入的：:class:`EmbeddingTransport` 只有一个 ``post_json`` 抽象方法，
默认实现 :class:`HttpxEmbeddingTransport` **在函数体内**才 ``import httpx``
（所以本模块顶层依然只有标准库）。测试注入一个假 transport 即可断言
「发了几次、URL / 请求体 / 头是什么、返回顺序如何」，完全不联网。

刻意不做
--------
- 不重试、不做退避：**重试策略属于调用方**（写侧 Pipeline 已把失败做成「有明确状态 + 可续做」的报告）
- 不做全局单例：由 ``knowledge_rag`` 或调用方显式构造（与 ``knowledge_retriever`` 同约定）
- 不发送 ``dimensions`` 参数（并非所有 OpenAI 兼容服务都支持，会平白多出 400）；
  ``EMBEDDING_DIMENSION`` 只用于**校验**上游返回值
- 不做异步并发批量（按 ``batch_size`` 顺序分批）：顺序可预测、失败定位简单
- 不 import ``models`` / ``database`` / ``fastapi`` / Retriever / 任何 ``interview_*``
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from services.embedding_service import (
    EmbeddingDimensionError,
    EmbeddingError,
    EmbeddingInfo,
    EmbeddingService,
    EmbeddingUnavailableError,
    HashEmbeddingService,
    MockEmbeddingService,
    _require_texts,
    _require_vector,
    describe_embedding,
)

# ============================================================
# 常量：环境变量名与 provider 取值
# ============================================================
ENV_PROVIDER = "EMBEDDING_PROVIDER"
ENV_API_KEY = "EMBEDDING_API_KEY"
ENV_BASE_URL = "EMBEDDING_BASE_URL"
ENV_MODEL = "EMBEDDING_MODEL"
ENV_DIMENSION = "EMBEDDING_DIMENSION"
ENV_TIMEOUT = "EMBEDDING_TIMEOUT"
ENV_BATCH_SIZE = "EMBEDDING_BATCH_SIZE"

#: 离线哈希占位（默认；不联网、不需要密钥）
PROVIDER_HASH = "hash"
#: 测试替身（固定向量）
PROVIDER_MOCK = "mock"
#: 真实模型（OpenAI 兼容 ``/embeddings``）
PROVIDER_OPENAI = "openai"
#: 讯飞星火 Embedding（**另一套协议**，实现见 ``services/embedding_provider_spark.py``）
PROVIDER_SPARK = "spark"

#: 允许的取值与别名（大小写不敏感，``-``/``_`` 等价）
_PROVIDER_ALIASES: Dict[str, str] = {
    PROVIDER_HASH: PROVIDER_HASH,
    "local": PROVIDER_HASH,
    "offline": PROVIDER_HASH,
    PROVIDER_MOCK: PROVIDER_MOCK,
    PROVIDER_OPENAI: PROVIDER_OPENAI,
    "openai_compatible": PROVIDER_OPENAI,
    "http": PROVIDER_OPENAI,
    PROVIDER_SPARK: PROVIDER_SPARK,
    "iflytek": PROVIDER_SPARK,
    "xfyun": PROVIDER_SPARK,
    "xinghuo": PROVIDER_SPARK,
}

#: **与厂商无关**的角色取值：同一条文本，是「知识原文」还是「用户问题」。
#:
#: 有些厂商（讯飞、BGE…）用**非对称编码**：问题与原文要配不同的 ``domain`` / 前缀，
#: 配错了**不会报错**、只是掉召回。而 :class:`~services.embedding_service.EmbeddingService`
#: 的接口只有 ``embed(text)``，**分不出这一条是问题还是原文**（读侧检索与写侧入库都调它），
#: 所以区分只能落在**实例**上 —— 由工厂/组装器按用途构造不同 role 的实例。
#:
#: 对 OpenAI 兼容 / 离线占位 / Mock 这些**对称**实现，role 被忽略（传了也无副作用）。
ROLE_DOCUMENT = "document"
ROLE_QUERY = "query"

DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_MODEL = "text-embedding-3-small"
DEFAULT_TIMEOUT = 30.0
DEFAULT_BATCH_SIZE = 16
#: ``0`` = 不校验维度（按上游返回）
DEFAULT_DIMENSION = 0

#: 响应体片段在异常消息里的最大长度
_SNIPPET_LIMIT = 200


# ============================================================
# 异常
# ============================================================
class EmbeddingProviderConfigError(EmbeddingError, ValueError):
    """**配置**问题：非法 provider / ``openai`` 却没密钥 / 维度或超时非正整数 / 批次大小 < 1。

    归为 ``ValueError``：与「上游此刻不可用」不同，这是**部署配错了**，
    重试一万次也一样——所以必须在**构造期**就报出来（同 ``RetrieverConfigError`` 的取舍）。
    """


# ============================================================
# 配置
# ============================================================
@dataclass(frozen=True)
class EmbeddingProviderConfig:
    """真实 Provider 的配置（不可变）。

    ``api_key`` 声明为 ``repr=False``：``dataclass`` 默认会把所有字段打进 ``repr``，
    一旦这个对象被日志打印就会**泄露密钥**。
    """

    provider: str = PROVIDER_HASH
    api_key: str = field(default="", repr=False)
    base_url: str = DEFAULT_BASE_URL
    model: str = DEFAULT_MODEL
    dimension: int = DEFAULT_DIMENSION
    timeout: float = DEFAULT_TIMEOUT
    batch_size: int = DEFAULT_BATCH_SIZE

    @property
    def is_remote(self) -> bool:
        """是否走真实网络调用。"""
        return self.provider == PROVIDER_OPENAI

    @property
    def masked_api_key(self) -> str:
        """打码后的密钥（只保留末 4 位），供日志 / 调试安全展示。"""
        if not self.api_key:
            return ""
        if len(self.api_key) <= 4:
            return "*" * len(self.api_key)
        return "*" * (len(self.api_key) - 4) + self.api_key[-4:]


def _read_str(env: Mapping, key: str, default: str) -> str:
    raw = env.get(key)
    if raw is None:
        return default
    value = str(raw).strip()
    return value if value else default


def _read_int(env: Mapping, key: str, default: int, *, minimum: int) -> int:
    raw = env.get(key)
    if raw is None or not str(raw).strip():
        return default
    text = str(raw).strip()
    try:
        value = int(text)
    except ValueError as exc:
        raise EmbeddingProviderConfigError(
            f"{key} 必须是整数，当前 {text!r}"
        ) from exc
    if value < minimum:
        raise EmbeddingProviderConfigError(
            f"{key} 必须 >= {minimum}，当前 {value}"
        )
    return value


def _read_float(env: Mapping, key: str, default: float, *, minimum_exclusive: float) -> float:
    raw = env.get(key)
    if raw is None or not str(raw).strip():
        return default
    text = str(raw).strip()
    try:
        value = float(text)
    except ValueError as exc:
        raise EmbeddingProviderConfigError(
            f"{key} 必须是数字，当前 {text!r}"
        ) from exc
    if not value > minimum_exclusive:
        raise EmbeddingProviderConfigError(
            f"{key} 必须 > {minimum_exclusive}，当前 {value}"
        )
    return value


def _resolve_provider(env: Mapping) -> str:
    """解析 provider 取值；**留空时自动**：配了密钥 → openai，否则 → hash。"""
    raw = str(env.get(ENV_PROVIDER) or "").strip().lower().replace("-", "_")
    if not raw:
        return PROVIDER_OPENAI if str(env.get(ENV_API_KEY) or "").strip() else PROVIDER_HASH
    resolved = _PROVIDER_ALIASES.get(raw)
    if resolved is None:
        raise EmbeddingProviderConfigError(
            f"{ENV_PROVIDER} 取值非法：{raw!r}；"
            f"可选 {PROVIDER_HASH} / {PROVIDER_MOCK} / {PROVIDER_OPENAI} / {PROVIDER_SPARK}"
            "（留空则按是否配置 EMBEDDING_API_KEY 自动选择；"
            f"{PROVIDER_SPARK} **必须显式指定**）"
        )
    return resolved


def _require_role(role: Optional[str]) -> str:
    """归一 ``role``：``None`` → :data:`ROLE_DOCUMENT`；未知取值 → 报错。

    **刻意不静默退回默认**：role 写错的表现是「召回莫名变差」，比直接报错难查得多。
    """
    if role is None:
        return ROLE_DOCUMENT
    value = str(role).strip().lower()
    if value not in (ROLE_DOCUMENT, ROLE_QUERY):
        raise EmbeddingProviderConfigError(
            f"未知 role：{role!r}；可选 {ROLE_DOCUMENT} / {ROLE_QUERY}"
        )
    return value


def load_embedding_config(env: Optional[Mapping] = None) -> EmbeddingProviderConfig:
    """从环境变量读配置（``env=None`` 时读 ``os.environ``）。

    **只负责 OpenAI 兼容协议**的配置。``EMBEDDING_PROVIDER=spark`` 时**直接报错**：
    讯飞的凭据（``SPARK_APP_ID`` / ``SPARK_API_KEY`` / ``SPARK_API_SECRET``）、端点与
    请求体都不一样，由 ``services/embedding_provider_spark.py`` 自行加载。
    返回一个「字段是 OpenAI 的、provider 却写着 spark」的对象只会误导使用者。

    正确入口是 :func:`build_embedding_service`（它按 provider 分派）。

    :raises EmbeddingProviderConfigError: provider 取值非法 / ``spark``（走错函数）/
        ``openai`` 缺密钥 / 维度为负 / 超时非正 / 批次大小 < 1。
    """
    source: Mapping = os.environ if env is None else env

    provider = _resolve_provider(source)
    if provider == PROVIDER_SPARK:
        raise EmbeddingProviderConfigError(
            f"{ENV_PROVIDER}={PROVIDER_SPARK} 的凭据 / 端点 / 请求体都与 OpenAI 兼容协议不同，"
            "其配置由 services/embedding_provider_spark.py 自行加载；"
            f"请改用 build_embedding_service()（它按 {ENV_PROVIDER} 正确分派），"
            "不要直接调用 load_embedding_config()"
        )
    api_key = _read_str(source, ENV_API_KEY, "")
    base_url = _read_str(source, ENV_BASE_URL, DEFAULT_BASE_URL).rstrip("/")
    model = _read_str(source, ENV_MODEL, DEFAULT_MODEL)
    dimension = _read_int(source, ENV_DIMENSION, DEFAULT_DIMENSION, minimum=0)
    timeout = _read_float(source, ENV_TIMEOUT, DEFAULT_TIMEOUT, minimum_exclusive=0)
    batch_size = _read_int(source, ENV_BATCH_SIZE, DEFAULT_BATCH_SIZE, minimum=1)

    if provider == PROVIDER_OPENAI and not api_key:
        raise EmbeddingProviderConfigError(
            f"{ENV_PROVIDER}={PROVIDER_OPENAI} 需要 {ENV_API_KEY}"
            f"（密钥只写在 backend/.env，禁止提交）；"
            f"离线环境请用 {ENV_PROVIDER}={PROVIDER_HASH}"
        )
    if provider == PROVIDER_OPENAI and not model:
        raise EmbeddingProviderConfigError(f"{ENV_PROVIDER}={PROVIDER_OPENAI} 需要 {ENV_MODEL}")
    if not base_url:
        raise EmbeddingProviderConfigError(f"{ENV_BASE_URL} 不能为空")

    return EmbeddingProviderConfig(
        provider=provider,
        api_key=api_key,
        base_url=base_url,
        model=model,
        dimension=dimension,
        timeout=timeout,
        batch_size=batch_size,
    )


# ============================================================
# HTTP 传输层（可注入 → 无网络也能测）
# ============================================================
class EmbeddingTransport(ABC):
    """HTTP 传输接口：**只发一个 JSON POST 并返回已解析的 JSON**。

    抽出来是为了让测试能注入假实现——真实 Provider 的逻辑（分批、解析、异常映射）
    与「怎么发请求」解耦，于是全部可在无网络环境断言。
    """

    @abstractmethod
    async def post_json(
        self,
        url: str,
        *,
        headers: Dict[str, str],
        payload: Dict[str, Any],
        timeout: float,
    ) -> Any:
        """POST ``payload`` 到 ``url``，返回解析后的 JSON。

        :raises EmbeddingUnavailableError: 网络失败 / 超时 / HTTP 非 2xx。
        """


def _bearer_token(headers: Mapping) -> str:
    """从请求头里取出 Bearer 令牌，供 :func:`_redact` 使用。

    传输层**只认识 headers**（不认识 config），所以让它「按自己刚发出去的凭据」打码，
    既不需要给 ``post_json`` 多加参数，也不会漏掉任何一条错误路径。
    """
    value = str(headers.get("Authorization") or "")
    prefix = "Bearer "
    if value.startswith(prefix):
        return value[len(prefix):].strip()
    return ""


def _redact(text: str, secret: str) -> str:
    """把密钥从文本里抹掉（异常消息可能带上游回显，绝不能把密钥带出去）。"""
    if secret and secret in text:
        text = text.replace(secret, "***")
    return text


def _snippet(value: Any, *, secret: str = "") -> str:
    """把任意对象压成单行短文本，供异常消息使用。"""
    text = " ".join(str(value).split())
    if len(text) > _SNIPPET_LIMIT:
        text = text[:_SNIPPET_LIMIT] + "…"
    return _redact(text, secret)


class HttpxEmbeddingTransport(EmbeddingTransport):
    """默认传输实现：``httpx.AsyncClient``（**延迟导入**，模块顶层保持只有标准库）。

    :param client_factory: 可选的客户端工厂，签名 ``(**kwargs) -> AsyncClient``。
        默认 ``None`` → ``httpx.AsyncClient``。测试传
        ``lambda **kw: httpx.AsyncClient(transport=httpx.MockTransport(handler), **kw)``
        即可**不联网**地覆盖真实的「状态码映射 / JSON 解析 / 超时」路径。
    """

    def __init__(self, *, client_factory: Optional[Any] = None) -> None:
        self._client_factory = client_factory

    async def post_json(
        self,
        url: str,
        *,
        headers: Dict[str, str],
        payload: Dict[str, Any],
        timeout: float,
    ) -> Any:
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover - 依赖缺失时的清晰报错
            raise EmbeddingUnavailableError(
                "真实 Embedding 需要 httpx（pip install httpx）；"
                f"离线环境请改用 {ENV_PROVIDER}={PROVIDER_HASH}"
            ) from exc

        factory = self._client_factory or httpx.AsyncClient
        try:
            async with factory(timeout=timeout) as client:
                response = await client.post(url, headers=headers, json=payload)
        except httpx.TimeoutException as exc:
            raise EmbeddingUnavailableError(
                f"Embedding 请求超时（{timeout}s）：{url}"
            ) from exc
        except httpx.HTTPError as exc:
            raise EmbeddingUnavailableError(
                f"Embedding 网络请求失败：{type(exc).__name__}: {exc}"
            ) from exc

        if response.status_code >= 400:
            raise EmbeddingUnavailableError(
                _http_error_message(response, secret=_bearer_token(headers))
            )

        try:
            return response.json()
        except Exception as exc:  # noqa: BLE001 - 上游返回了非 JSON
            raise EmbeddingDimensionError(
                "Embedding 上游返回的不是合法 JSON："
                f"{_snippet(response.text, secret=_bearer_token(headers))}"
            ) from exc


def _http_error_message(response: Any, *, secret: str = "") -> str:
    """把非 2xx 响应压成可读消息。

    **只带状态码与响应体片段，绝不带请求头**；响应体里若回显了密钥，也会被
    :func:`_redact` 抹成 ``***``（上游把请求内容原样回显是真实存在的）。
    """
    status = getattr(response, "status_code", "?")
    hint = ""
    if status in (401, 403):
        hint = f"（鉴权失败，检查 {ENV_API_KEY}）"
    elif status == 404:
        hint = f"（检查 {ENV_BASE_URL} 是否正确）"
    elif status == 429:
        hint = "（限流，稍后重试）"
    return (
        f"Embedding 上游返回 HTTP {status}{hint}："
        f"{_snippet(getattr(response, 'text', ''), secret=secret)}"
    )


# ============================================================
# 真实 Provider
# ============================================================
class EmbeddingProvider(EmbeddingService):
    """真实向量化实现：调用 OpenAI 兼容的 ``POST {base_url}/embeddings``。

    **只实现 ``_embed_one`` 钩子 + 覆盖 ``embed_batch``**，``embed(text)`` 一行都没重写——
    接口与校验口径完全沿用 :class:`~services.embedding_service.EmbeddingService`。

    :param config: :class:`EmbeddingProviderConfig`（``provider`` 必须是 ``openai``）。
    :param transport: HTTP 传输实现；``None`` → :class:`HttpxEmbeddingTransport`。
        测试注入假 transport 即可完全离线。
    """

    #: 实例上会被 ``config.model`` 覆盖；``embedding_model`` 列记录的就是它
    name = "openai-compatible"
    #: ``0`` = 不校验维度
    dimension = 0
    #: 真实模型产出的是**语义**向量 ⇒ 供 :func:`describe_embedding` 上报运行状态。
    #: 注意这是**实现声明**，不写在 ``EmbeddingService`` 基类上（接口面保持零改动）。
    semantic_enabled = True

    def __init__(
        self,
        config: EmbeddingProviderConfig,
        *,
        transport: Optional[EmbeddingTransport] = None,
    ) -> None:
        if not isinstance(config, EmbeddingProviderConfig):
            raise EmbeddingProviderConfigError(
                f"config 必须是 EmbeddingProviderConfig，收到 {type(config).__name__}"
            )
        if config.provider != PROVIDER_OPENAI:
            raise EmbeddingProviderConfigError(
                f"EmbeddingProvider 只处理 provider={PROVIDER_OPENAI}，"
                f"当前是 {config.provider!r}"
            )
        if not config.api_key:
            raise EmbeddingProviderConfigError(
                f"{ENV_PROVIDER}={PROVIDER_OPENAI} 需要 {ENV_API_KEY}"
            )
        if transport is not None and not callable(getattr(transport, "post_json", None)):
            raise EmbeddingProviderConfigError(
                "transport 必须提供可调用的 post_json(url, *, headers, payload, timeout)"
            )

        self.config = config
        self.name = config.model or self.name
        self.dimension = config.dimension
        self._transport = transport if transport is not None else HttpxEmbeddingTransport()

    # ------------------------------------------------------------
    # 请求
    # ------------------------------------------------------------
    @property
    def endpoint(self) -> str:
        """完整请求地址（配置里的 ``base_url`` **不含** ``/embeddings``）。"""
        return f"{self.config.base_url}/embeddings"

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }

    def _payload(self, texts: Sequence[str]) -> Dict[str, Any]:
        # 刻意不发 ``dimensions``：并非所有 OpenAI 兼容服务都支持，会平白多出 400。
        # ``config.dimension`` 只用于校验上游返回值。
        return {"model": self.config.model, "input": list(texts)}

    async def _request(self, texts: Sequence[str]) -> List[List[float]]:
        """发一次请求并解析出向量（数量与入参一致、顺序与入参一致）。"""
        raw = await self._transport.post_json(
            self.endpoint,
            headers=self._headers(),
            payload=self._payload(texts),
            timeout=self.config.timeout,
        )
        return self._parse(raw, expected=len(texts))

    # ------------------------------------------------------------
    # 解析
    # ------------------------------------------------------------
    def _parse(self, raw: Any, *, expected: int) -> List[List[float]]:
        """把上游响应解析成 ``List[List[float]]``；结构不对一律 ``EmbeddingDimensionError``。"""
        if not isinstance(raw, Mapping):
            raise EmbeddingDimensionError(
                f"{self.name} 返回结构不是对象，收到 {type(raw).__name__}"
            )
        if raw.get("error"):
            raise EmbeddingUnavailableError(
                f"{self.name} 返回错误：{_snippet(raw.get('error'), secret=self.config.api_key)}"
            )

        data = raw.get("data")
        if isinstance(data, (str, bytes, bytearray)) or not isinstance(data, Sequence):
            raise EmbeddingDimensionError(
                f"{self.name} 响应缺少 data 数组，收到 {type(data).__name__}"
            )
        if len(data) != expected:
            raise EmbeddingDimensionError(
                f"{self.name} 返回 {len(data)} 条向量，期望 {expected} 条"
                "（上游批量接口未按入参条数返回）"
            )

        # 只有当**每一条**都带合法 index 时才按 index 重排；否则保持上游顺序。
        indices = [item.get("index") if isinstance(item, Mapping) else None for item in data]
        if all(isinstance(i, int) and not isinstance(i, bool) for i in indices):
            order = sorted(range(len(data)), key=lambda pos: indices[pos])
        else:
            order = list(range(len(data)))

        vectors: List[List[float]] = []
        for pos in order:
            item = data[pos]
            if not isinstance(item, Mapping) or "embedding" not in item:
                raise EmbeddingDimensionError(
                    f"{self.name} 第 {pos} 条缺少 embedding 字段："
                    f"{_snippet(item, secret=self.config.api_key)}"
                )
            # 复用基类的向量校验：数值序列 / 非空 / 维度相符 / 显式拒 bool 与 str 元素
            vectors.append(
                _require_vector(item["embedding"], self.name, self.dimension)
            )
        return vectors

    # ------------------------------------------------------------
    # 编码（接口部分：只实现钩子 + 覆盖批量）
    # ------------------------------------------------------------
    async def _embed_one(self, text: str) -> List[float]:
        vectors = await self._request([text])
        return vectors[0]

    async def embed_batch(self, texts: Sequence[str]) -> List[List[float]]:
        """原生批量：按 ``batch_size`` 分批请求，**返回顺序与入参一致**。

        入参校验复用基类的 ``_require_texts``（同一处校验，避免覆盖后口径漂移），
        且**在任何 HTTP 调用之前**完成——非法入参不会产生一次网络请求。
        """
        items = _require_texts(texts)
        if not items:
            return []

        size = self.config.batch_size
        vectors: List[List[float]] = []
        for start in range(0, len(items), size):
            vectors.extend(await self._request(items[start:start + size]))
        return vectors

    def __repr__(self) -> str:  # pragma: no cover - 便于调试打印
        return (
            f"<EmbeddingProvider name={self.name!r} dimension={self.dimension} "
            f"base_url={self.config.base_url!r} api_key={self.config.masked_api_key!r}>"
        )


#: 语义化别名（同一对象）：本类讲的是 **OpenAI 兼容** 的 ``/embeddings`` 协议，
#: 多数厂商（OpenAI / 通义 / 智谱 / 硅基流动 / 本地 BGE-server …）都实现了它。
OpenAICompatibleEmbeddingProvider = EmbeddingProvider


# ============================================================
# 工厂（全项目唯一「按配置选实现」的入口）
# ============================================================
def _build_spark(
    env: Mapping, *, transport: Optional[EmbeddingTransport], role: str
) -> EmbeddingService:
    """构造讯飞实现（**延迟导入**：模块顶层保持只有标准库 + services）。"""
    from services.embedding_provider_spark import build_spark_embedding_service

    return build_spark_embedding_service(env=env, transport=transport, role=role)


def build_embedding_service(
    config: Optional[EmbeddingProviderConfig] = None,
    *,
    env: Optional[Mapping] = None,
    transport: Optional[EmbeddingTransport] = None,
    role: Optional[str] = None,
) -> EmbeddingService:
    """按配置构造 Embedding 服务。

    - ``config=None`` → 按 ``EMBEDDING_PROVIDER`` 选（缺省走 :func:`load_embedding_config`）
    - ``hash`` → :class:`~services.embedding_service.HashEmbeddingService`（离线占位，默认）
    - ``mock`` → :class:`~services.embedding_service.MockEmbeddingService`（测试替身）
    - ``openai`` → :class:`EmbeddingProvider`（OpenAI 兼容的真实模型）
    - ``spark`` → ``embedding_provider_spark.SparkEmbeddingProvider``（讯飞星火；
      **配置由它自己的模块加载**，因此这里在加载通用 config **之前**就分派）

    :param role: :data:`ROLE_DOCUMENT` / :data:`ROLE_QUERY`（缺省 ``document``）。
        只有**非对称**实现（目前是 spark）会用到它；对称实现一律忽略。
    :param env: 环境变量映射（``None`` → ``os.environ``）；透传给选中的实现。

    **不做全局单例**：每次调用返回新对象，由调用方持有（避免「悄悄换了模型」）。
    """
    source: Mapping = os.environ if env is None else env
    resolved_role = _require_role(role)

    if config is None and _resolve_provider(source) == PROVIDER_SPARK:
        return _build_spark(source, transport=transport, role=resolved_role)
    if isinstance(config, EmbeddingProviderConfig) and config.provider == PROVIDER_SPARK:
        return _build_spark(source, transport=transport, role=resolved_role)

    if config is None:
        config = load_embedding_config(source)
    if not isinstance(config, EmbeddingProviderConfig):
        raise EmbeddingProviderConfigError(
            f"config 必须是 EmbeddingProviderConfig，收到 {type(config).__name__}"
        )

    if config.provider == PROVIDER_HASH:
        return HashEmbeddingService()
    if config.provider == PROVIDER_MOCK:
        return MockEmbeddingService()
    if config.provider == PROVIDER_OPENAI:
        return EmbeddingProvider(config, transport=transport)

    # 理论上不可达（_resolve_provider 已挡），保留兜底以免将来加了取值忘了改这里
    raise EmbeddingProviderConfigError(f"未知 provider：{config.provider!r}")


# ``EmbeddingInfo`` / ``describe_embedding`` 定义在 ``embedding_service``（接口模块，
# 因为「运行状态」是**任何** EmbeddingService 都有的属性），这里**再导出**一份：
# 于是「构造实现的工厂」与「读运行状态」在同一处，调用方不必为了打个日志
# 去 import 接口模块；组装器 ``knowledge_rag.describe_default_embedding()`` 也经此取用
# （组装器**不得**直接 import ``embedding_service``——那条守卫锁死了「不再直接依赖具体实现」）。
__all__ = [
    "DEFAULT_BASE_URL",
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_DIMENSION",
    "DEFAULT_MODEL",
    "DEFAULT_TIMEOUT",
    "ENV_API_KEY",
    "ENV_BASE_URL",
    "ENV_BATCH_SIZE",
    "ENV_DIMENSION",
    "ENV_MODEL",
    "ENV_PROVIDER",
    "ENV_TIMEOUT",
    "EmbeddingInfo",
    "EmbeddingProvider",
    "EmbeddingProviderConfig",
    "EmbeddingProviderConfigError",
    "EmbeddingTransport",
    "HttpxEmbeddingTransport",
    "OpenAICompatibleEmbeddingProvider",
    "PROVIDER_HASH",
    "PROVIDER_MOCK",
    "PROVIDER_OPENAI",
    "PROVIDER_SPARK",
    "ROLE_DOCUMENT",
    "ROLE_QUERY",
    "build_embedding_service",
    "describe_embedding",
    "load_embedding_config",
]
