# -*- coding: utf-8 -*-
"""AI 能力接入层 · 超时 / 有限重试 / 指数退避 / 降级。

三条规则
--------
1. **默认只尝试 1 次**。重试是**显式 opt-in** 的能力，不是默认行为——
   默认重试会把一次故障放大成 N 次（放大供应商限流、放大计费）。
2. **只重试"可重试"的错误**。判据是 :class:`~modules.ai.errors.AiError`
   的 ``retryable`` 类属性：超时 / 限流 / 不可用可重试，鉴权失败 / 响应不可解析 /
   数据边界违规**不重试**（重试一万次也还是错的）。
3. **非幂等操作绝不重试**。``LlmRequest.idempotent=False`` ⇒
   :meth:`CallPolicy.attempts_for` 直接返回 1，配置写得再大也没用。

退避用 **equal jitter**：``delay = cap/2 + rand*cap/2``。
纯指数退避会让所有实例在同一时刻重试（惊群），纯随机又可能退避过短。
"""

import asyncio
import os
import math
import random
from dataclasses import dataclass, replace
from typing import Any, Awaitable, Callable, Dict, Mapping, Optional

from modules.ai.errors import (
    AiConfigError,
    AiError,
    AiTimeoutError,
    classify_transport_error,
)
from modules.ai.llm import LlmProvider, LlmRequest, LlmResponse
from modules.ai.telemetry import (
    METRICS,
    METRIC_CALLS,
    METRIC_ERRORS,
    METRIC_LATENCY,
    METRIC_RETRIES,
    METRIC_TIMEOUTS,
    METRIC_TOKENS_COMPLETION,
    METRIC_TOKENS_PROMPT,
    CallScope,
    MetricsRegistry,
    current_prompt_id,
    current_scope,
    log_event,
    new_request_id,
)

__all__ = [
    "ENV_TIMEOUT",
    "ENV_MAX_ATTEMPTS",
    "ENV_BACKOFF_BASE",
    "DEFAULT_TIMEOUT_SECONDS",
    "CallPolicy",
    "call_llm",
]

ENV_TIMEOUT = "AI_LLM_TIMEOUT"
ENV_MAX_ATTEMPTS = "AI_LLM_MAX_ATTEMPTS"
ENV_BACKOFF_BASE = "AI_LLM_BACKOFF_BASE"

#: 外层超时**必须大于** transport 自身的超时（旧 ``SparkAPI`` 的 websocket 是 60s）。
#: 否则外层先放弃，反而拿不到更精确的失败原因（"鉴权失败"会退化成"客户端超时"）。
#: 90s 只在 transport 真正卡死（如 DNS 挂住）时才生效。
DEFAULT_TIMEOUT_SECONDS = 90.0
#: **默认 1 次**：重试是显式 opt-in。默认重试会让一次故障变成两次最长等待，
#: 直接把用户可感知的延迟翻倍。
DEFAULT_MAX_ATTEMPTS = 1
DEFAULT_BACKOFF_BASE = 0.5
DEFAULT_BACKOFF_MAX = 8.0
DEFAULT_JITTER_RATIO = 0.5

SleepFn = Callable[[float], Awaitable[None]]


def _read_number(env: Optional[Mapping[str, Any]], key: str, default: float, *,
                 minimum: float, integer: bool) -> float:
    source = os.environ if env is None else env
    raw = source.get(key, "")
    text = "" if raw is None else str(raw).strip()
    if not text:
        return default
    try:
        value = float(text)
    except (TypeError, ValueError):
        raise AiConfigError("%s 取值非法：%r（应为数字）" % (key, text))
    if not math.isfinite(value) or value < minimum or (integer and not value.is_integer()):
        raise AiConfigError("%s 取值非法：%r（不得小于 %s）" % (key, text, minimum))
    return float(int(value)) if integer else value


@dataclass(frozen=True)
class CallPolicy:
    """一次调用的策略参数（不可变，便于缓存与断言）。"""

    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    base_delay_seconds: float = DEFAULT_BACKOFF_BASE
    max_delay_seconds: float = DEFAULT_BACKOFF_MAX
    jitter_ratio: float = DEFAULT_JITTER_RATIO

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, Any]] = None) -> "CallPolicy":
        """从环境变量读取。**非法值抛 :class:`AiConfigError`（不静默）**。

        * ``AI_LLM_TIMEOUT`` —— 单次调用超时秒数，默认 90，最小 1
        * ``AI_LLM_MAX_ATTEMPTS`` —— 最大尝试次数（含首次），默认 1（即不重试），最小 1
        * ``AI_LLM_BACKOFF_BASE`` —— 退避基数秒，默认 0.5，最小 0
        """
        return cls(
            timeout_seconds=_read_number(env, ENV_TIMEOUT, DEFAULT_TIMEOUT_SECONDS,
                                         minimum=1.0, integer=False),
            max_attempts=int(_read_number(env, ENV_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS,
                                          minimum=1.0, integer=True)),
            base_delay_seconds=_read_number(env, ENV_BACKOFF_BASE, DEFAULT_BACKOFF_BASE,
                                            minimum=0.0, integer=False),
        )

    def attempts_for(self, request: LlmRequest) -> int:
        """本次请求允许的尝试次数。

        **非幂等请求恒为 1**——这是硬规则，不受 ``AI_LLM_MAX_ATTEMPTS`` 影响。
        """
        if not getattr(request, "idempotent", True):
            return 1
        return max(1, int(self.max_attempts))

    def backoff_seconds(self, attempt: int, *, rand: Optional[float] = None) -> float:
        """第 ``attempt`` 次失败后的等待秒数（``attempt`` 从 1 开始，1 表示首次失败）。"""
        exponent = max(0, int(attempt) - 1)
        cap = min(self.base_delay_seconds * (2 ** exponent), self.max_delay_seconds)
        if cap <= 0:
            return 0.0
        ratio = min(max(self.jitter_ratio, 0.0), 1.0)
        draw = random.random() if rand is None else min(max(rand, 0.0), 1.0)
        return cap * (1.0 - ratio) + cap * ratio * draw

    def as_dict(self) -> Dict[str, Any]:
        return {
            "timeout_seconds": self.timeout_seconds,
            "max_attempts": self.max_attempts,
            "base_delay_seconds": self.base_delay_seconds,
            "max_delay_seconds": self.max_delay_seconds,
            "jitter_ratio": self.jitter_ratio,
        }


def _normalize(exc: BaseException, provider: str, request_id: str) -> AiError:
    if isinstance(exc, AiError):
        if not exc.request_id:
            exc.request_id = request_id
        if not exc.provider:
            exc.provider = provider
        return exc
    if isinstance(exc, asyncio.TimeoutError):
        return AiTimeoutError("调用超时（客户端主动放弃）", provider=provider, request_id=request_id)
    return classify_transport_error(exc, provider=provider, request_id=request_id)


async def _invoke(provider: LlmProvider, request: LlmRequest, timeout: float) -> LlmResponse:
    if timeout and timeout > 0:
        return await asyncio.wait_for(provider.complete(request), timeout=timeout)
    return await provider.complete(request)


async def call_llm(
    provider: LlmProvider,
    request: LlmRequest,
    *,
    policy: Optional[CallPolicy] = None,
    metrics: Optional[MetricsRegistry] = None,
    sleep: Optional[SleepFn] = None,
    scope: Optional[CallScope] = None,
) -> LlmResponse:
    """按策略执行一次补全。**成功返回 LlmResponse，失败抛 AiError 子类**。

    :param sleep: 可注入的等待函数（测试注入"零等待"，避免真实退避拖慢套件）
    :param scope: :func:`modules.ai.telemetry.ai_call_scope` 产出的作用域；
        成功/失败都会把结果写回，便于调用方在 ``with`` 块外读结构化原因
    """
    policy = policy or CallPolicy()
    metrics = metrics or METRICS
    sleep_fn: SleepFn = sleep or asyncio.sleep
    # 作用域与 Prompt 版本都**自动继承**上下文里声明的值：
    # 调用方只要 `with ai_call_scope("x@v1")`，不必逐层透传参数。
    if scope is None:
        scope = current_scope()
    if not request.prompt_id:
        ambient_prompt = current_prompt_id()
        if ambient_prompt:
            request = replace(request, prompt_id=ambient_prompt)
    request_id = scope.request_id if scope is not None else new_request_id()

    attempts = policy.attempts_for(request)
    last_error: Optional[AiError] = None

    for attempt in range(1, attempts + 1):
        metrics.incr(METRIC_CALLS)
        try:
            response = await _invoke(provider, request, policy.timeout_seconds)
        except BaseException as exc:  # noqa: BLE001 - 全部收敛为 AiError
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            error = _normalize(exc, getattr(provider, "name", ""), request_id)
            last_error = error
            metrics.incr(METRIC_ERRORS)
            if isinstance(error, AiTimeoutError):
                metrics.incr(METRIC_TIMEOUTS)

            retry_left = attempt < attempts and bool(error.retryable)
            if not retry_left:
                if scope is not None:
                    scope.error = error
                    scope.attempts = attempt
                    scope.provider = getattr(provider, "name", "")
                log_event(
                    "ai_call_failed",
                    provider=getattr(provider, "name", ""),
                    request_id=request_id,
                    prompt_id=request.prompt_id,
                    prompt_version=request.prompt_version,
                    attempt=attempt,
                    attempts=attempts,
                    error_kind=error.kind,
                    retryable=error.retryable,
                    # 不记录上游原文：可能回显 Prompt、凭据或敏感文档。
                )
                raise error

            metrics.incr(METRIC_RETRIES)
            delay = policy.backoff_seconds(attempt)
            log_event(
                "ai_call_retry",
                provider=getattr(provider, "name", ""),
                request_id=request_id,
                prompt_id=request.prompt_id,
                attempt=attempt,
                attempts=attempts,
                error_kind=error.kind,
                delay_ms=round(delay * 1000.0, 1),
            )
            if delay > 0:
                await sleep_fn(delay)
            continue

        # ---- 成功 ----
        response = replace(
            response,
            request_id=response.request_id or request_id,
            prompt_id=response.prompt_id or request.prompt_id,
            prompt_version=response.prompt_version or request.prompt_version,
        )
        provider_name = getattr(provider, "name", "")
        metrics.observe(METRIC_LATENCY, response.latency_ms, provider=provider_name)
        if response.usage.prompt_tokens:
            metrics.observe(METRIC_TOKENS_PROMPT, float(response.usage.prompt_tokens),
                            provider=provider_name)
        if response.usage.completion_tokens:
            metrics.observe(METRIC_TOKENS_COMPLETION, float(response.usage.completion_tokens),
                            provider=provider_name)
        if scope is not None:
            scope.response = response
            scope.error = None
            scope.attempts = attempt
            scope.provider = provider_name
        log_event(
            "ai_call_ok",
            provider=provider_name,
            request_id=response.request_id,
            prompt_id=response.prompt_id,
            prompt_version=response.prompt_version,
            model=response.model,
            attempt=attempt,
            latency_ms=round(response.latency_ms, 1),
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
            chars=len(response.text),
        )
        return response

    # 理论不可达（循环内要么 return 要么 raise），保留以免静态检查误报。
    raise last_error or AiError("AI 调用未产生结果")
