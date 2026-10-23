# -*- coding: utf-8 -*-
"""AI 能力接入层 · 统一错误分类。

为什么需要这一层
----------------
重构前的 LLM 调用把失败**编码成字符串**：``main.SparkAPI.chat()`` 在异常时返回
``"API调用失败: ..."``，调用方只能 ``text.startswith(prefix)`` 去猜。这带来三个
具体问题：

1. **无法分类**——超时 / 鉴权失败 / 限流 / 服务不可用 / 响应为空被压成同一个前缀，
   上层无法判断"该重试"还是"该降级"。
2. **无法重试**——重试策略需要知道错误是否可重试，字符串里没有这个信息。
3. **无法度量**——没有错误类型就只能统计"失败了几次"，不能按类型计数。

本模块把每种故障提升为一个类，并用 **``retryable`` 类属性**承载"能不能重试"这一
唯一判据：重试策略只看它，**绝不解析异常消息文本**（解析文本必然随厂商文案漂移）。

设计约束
--------
* **分类失败时保守处理**：无法识别的异常归到 :class:`AiError` 且
  ``retryable = False``。宁可少重试一次，也不盲目重试。
* 本模块**零第三方依赖**（只用标准库），避免把厂商细节带进接口层。
* ``LEGACY_FAILURE_PREFIXES`` 是**旧实现的失败哨兵前缀**，集中在此处定义，
  供 ``llm_spark``（识别旧客户端的字符串失败）与 ``compat``（回写旧哨兵）共用，
  避免两处各抄一份导致漂移。
"""

from typing import Any, Dict

__all__ = [
    "AiError",
    "AiConfigError",
    "AiModeError",
    "AiAuthError",
    "AiTimeoutError",
    "AiRateLimitError",
    "AiUnavailableError",
    "AiResponseError",
    "AiBoundaryError",
    "LEGACY_FAILURE_PREFIXES",
    "is_legacy_failure",
    "classify_transport_error",
]


#: 旧实现（``main.SparkAPI.chat``）把失败当**正常返回值**，前缀如下。
#: ``services.interview_agent._is_spark_failure`` 依赖同一组前缀，**不得修改**。
LEGACY_FAILURE_PREFIXES = ("API调用失败", "SparkAPI not available", "AI 回答为空")


def is_legacy_failure(text: Any) -> bool:
    """判断一段模型输出是否是**旧实现的失败哨兵串**。

    只用于兼容层：新代码应当处理 :class:`AiError`，而不是判字符串。
    """
    if not isinstance(text, str):
        return True
    stripped = text.lstrip()
    return any(stripped.startswith(prefix) for prefix in LEGACY_FAILURE_PREFIXES)


class AiError(RuntimeError):
    """AI 能力接入层的根异常。

    ``kind`` / ``retryable`` 是**类属性**（不是实例属性），因此可以
    ``type(exc).retryable`` 直接取，不必构造实例。
    """

    kind = "ai_error"
    retryable = False

    def __init__(
        self,
        message: str,
        *,
        provider: str = "",
        request_id: str = "",
    ) -> None:
        super().__init__(message)
        self.message = message
        self.provider = provider
        self.request_id = request_id

    def as_dict(self) -> Dict[str, Any]:
        """可序列化的摘要（**不含 Prompt / 凭据 / 用户数据**）。"""
        return {
            "kind": self.kind,
            "retryable": self.retryable,
            "provider": self.provider,
            "request_id": self.request_id,
            "message": self.message,
        }


class AiConfigError(AiError, ValueError):
    """配置错误（启动期就该暴露，不应等到第一次调用）。"""

    kind = "config"
    retryable = False


class AiModeError(AiConfigError):
    """运行模式非法，或生产模式缺少必需配置。

    **生产模式缺凭据必须是这个错误**——绝不能静默回退到 Mock。
    """

    kind = "mode"
    retryable = False


class AiAuthError(AiError):
    """鉴权 / 授权失败（凭据错误、过期、无权限）。**重试无意义**。"""

    kind = "auth"
    retryable = False


class AiTimeoutError(AiError):
    """请求超时（含客户端主动超时与对端无响应）。"""

    kind = "timeout"
    retryable = True


class AiRateLimitError(AiError):
    """被供应商限流（429 / 配额耗尽）。"""

    kind = "rate_limit"
    retryable = True


class AiUnavailableError(AiError):
    """连接失败 / 5xx / 服务不可用。**可重试**（只读调用重试无副作用）。"""

    kind = "unavailable"
    retryable = True


class AiResponseError(AiError):
    """对端返回了不可用的内容（空、不可解析、业务错误码）。**重试无意义**。"""

    kind = "response"
    retryable = False


class AiBoundaryError(AiError):
    """Prompt 数据边界被突破（越权数据、其他用户信息、超长注入）。"""

    kind = "boundary"
    retryable = False


#: 判定"可重试"的关键词表。**只用于给未知异常做保守归类**，
#: 已知类型一律靠类属性，不查表。
_TIMEOUT_HINTS = ("timeout", "timed out", "time out", "deadline exceeded")
_UNAVAILABLE_HINTS = (
    "connection",
    "refused",
    "reset by peer",
    "unreachable",
    "network",
    "getaddrinfo",
    "name resolution",
    "broken pipe",
    "websocket",
    "ssl",
    "handshake",
    "502",
    "503",
    "504",
)
_AUTH_HINTS = ("401", "403", "unauthorized", "forbidden", "invalid api key", "auth")
_RATE_LIMIT_HINTS = ("429", "rate limit", "quota", "too many requests")


def classify_transport_error(
    exc: BaseException,
    *,
    provider: str = "",
    request_id: str = "",
) -> AiError:
    """把**任意**传输层异常收敛为 :class:`AiError` 子类。

    已是 :class:`AiError` 的原样返回（幂等，便于多层包装）。
    无法识别时返回 ``retryable=False`` 的 :class:`AiError`——**保守不重试**。
    """
    if isinstance(exc, AiError):
        return exc

    name = type(exc).__name__.lower()
    text = ""
    try:
        text = str(exc).lower()
    except Exception:  # noqa: BLE001 - 异常自身的 __str__ 也可能炸
        text = ""
    haystack = name + " " + text

    if any(hint in haystack for hint in _TIMEOUT_HINTS):
        return AiTimeoutError(
            "调用超时：%s" % (text[:200] or name), provider=provider, request_id=request_id
        )
    if any(hint in haystack for hint in _AUTH_HINTS):
        return AiAuthError(
            "鉴权失败：%s" % (text[:200] or name), provider=provider, request_id=request_id
        )
    if any(hint in haystack for hint in _RATE_LIMIT_HINTS):
        return AiRateLimitError(
            "被限流：%s" % (text[:200] or name), provider=provider, request_id=request_id
        )
    if any(hint in haystack for hint in _UNAVAILABLE_HINTS):
        return AiUnavailableError(
            "服务不可用：%s" % (text[:200] or name), provider=provider, request_id=request_id
        )
    return AiError(
        "调用失败：%s: %s" % (type(exc).__name__, text[:200]),
        provider=provider,
        request_id=request_id,
    )
