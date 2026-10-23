# -*- coding: utf-8 -*-
"""AI 能力接入层 · 请求追踪与指标。

三件事
------
1. **请求追踪**：每次 AI 调用一个 ``request_id``，贯穿日志、错误对象与响应体，
   出问题能一条线串起来（重构前连"这次失败是哪次调用"都分不清）。
2. **统一指标**：调用数 / 错误数 / 重试数 / 超时数 / 累计耗时 / Token 用量。
   指标名是**固定闭集**（:data:`METRIC_KEYS`），便于断言"键集不变"。
3. **Prompt 版本**：用 :func:`ai_call_scope` 声明本次调用用的模板标识，
   由 Provider 自动带进 :class:`~modules.ai.llm.LlmResponse`，
   避免"改完 Prompt 不知道线上跑的是哪版"。

为什么不挂在 ``/api/health`` 上
-------------------------------
``/api/health`` 的 ``metrics`` 已被既有契约断言为**固定 12 键**，加键等于破坏契约。
本模块只提供 :func:`snapshot`，由日志 / 运维接口按需读取；**不改 health 契约**。
"""

import json
import logging
import secrets
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Dict, Iterator, Optional

__all__ = [
    "METRIC_KEYS",
    "METRICS",
    "MetricsRegistry",
    "new_request_id",
    "current_prompt_id",
    "current_request_id",
    "CallScope",
    "ai_call_scope",
    "log_event",
    "snapshot",
]

_log = logging.getLogger("career_ai.ai")

METRIC_CALLS = "ai_calls_total"
METRIC_ERRORS = "ai_errors_total"
METRIC_RETRIES = "ai_retries_total"
METRIC_TIMEOUTS = "ai_timeouts_total"
METRIC_LATENCY = "ai_latency_ms_total"
METRIC_TOKENS_PROMPT = "ai_tokens_prompt_total"
METRIC_TOKENS_COMPLETION = "ai_tokens_completion_total"
METRIC_BOUNDARY_BLOCKS = "ai_boundary_blocks_total"

#: **固定闭集**。新增指标必须同步改这里与对应测试（避免"悄悄多一个键"）。
METRIC_KEYS = (
    METRIC_CALLS,
    METRIC_ERRORS,
    METRIC_RETRIES,
    METRIC_TIMEOUTS,
    METRIC_LATENCY,
    METRIC_TOKENS_PROMPT,
    METRIC_TOKENS_COMPLETION,
    METRIC_BOUNDARY_BLOCKS,
)


def new_request_id() -> str:
    """生成请求追踪 ID（进程内唯一即可，不做全局排序）。"""
    return "ai-" + secrets.token_hex(8)


class MetricsRegistry:
    """进程内累加计数器。

    * 只做**累加**，不做滑动窗口——AI 调用的量级不需要 HDR 直方图；
    * 加锁是因为 ``chat()`` 会在 ``run_in_executor`` 的线程池里跑；
    * 键集固定，:meth:`snapshot` 永远返回 :data:`METRIC_KEYS` 全集（缺的补 0），
      这样"键集不变"可以被断言。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._values: Dict[str, float] = {key: 0.0 for key in METRIC_KEYS}
        self._by_provider: Dict[str, Dict[str, float]] = {}

    def incr(self, name: str, value: float = 1.0) -> None:
        self._add(name, value, provider=None)

    def observe(self, name: str, value: float, *, provider: str = "") -> None:
        self._add(name, value, provider=provider)

    def _add(self, name: str, value: float, *, provider: Optional[str]) -> None:
        if name not in self._values:
            # 未登记的指标名一律忽略：宁可丢一个自定义指标，
            # 也不要让键集悄悄漂移（契约测试断言的是固定闭集）。
            return
        with self._lock:
            self._values[name] += value
            if provider:
                bucket = self._by_provider.setdefault(provider, {})
                bucket[name] = bucket.get(name, 0.0) + value

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {key: _tidy(self._values.get(key, 0.0)) for key in METRIC_KEYS}

    def snapshot_by_provider(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                provider: {key: _tidy(value) for key, value in sorted(bucket.items())}
                for provider, bucket in sorted(self._by_provider.items())
            }

    def reset(self) -> None:
        with self._lock:
            self._values = {key: 0.0 for key in METRIC_KEYS}
            self._by_provider = {}


def _tidy(value: float) -> Any:
    """整数值保持 int，避免 ``3.0`` 这种噪声；浮点保留 3 位。"""
    if float(value).is_integer():
        return int(value)
    return round(float(value), 3)


METRICS = MetricsRegistry()


_PROMPT_ID: ContextVar[str] = ContextVar("career_ai_ai_prompt_id", default="")
_REQUEST_ID: ContextVar[str] = ContextVar("career_ai_ai_request_id", default="")
_CURRENT_SCOPE: ContextVar[Optional["CallScope"]] = ContextVar("career_ai_ai_scope", default=None)


def current_prompt_id() -> str:
    return _PROMPT_ID.get()


def current_request_id() -> str:
    return _REQUEST_ID.get()


def current_scope() -> Optional["CallScope"]:
    """当前作用域（没有 ``with ai_call_scope(...)`` 时为 ``None``）。"""
    return _CURRENT_SCOPE.get()


class CallScope:
    """一次 AI 调用的作用域对象。

    由 :func:`ai_call_scope` 创建并 yield；Provider 会把本次调用的
    ``error`` / ``response`` 写回来，调用方**在 with 块结束后**读取，
    从而既能拿到旧式的字符串返回值，也能拿到结构化的失败原因。
    """

    __slots__ = ("request_id", "prompt_id", "provider", "error", "response", "attempts", "started_at")

    def __init__(self, request_id: str, prompt_id: str) -> None:
        self.request_id = request_id
        self.prompt_id = prompt_id
        self.provider = ""
        self.error: Optional[BaseException] = None
        self.response: Any = None
        self.attempts = 0
        self.started_at = time.monotonic()

    @property
    def elapsed_ms(self) -> float:
        return (time.monotonic() - self.started_at) * 1000.0

    def error_dict(self) -> Optional[Dict[str, Any]]:
        """失败原因摘要（无失败 ⇒ ``None``）。**不含 Prompt 内容**。"""
        error = self.error
        if error is None:
            return None
        as_dict = getattr(error, "as_dict", None)
        if callable(as_dict):
            return as_dict()
        return {
            "kind": "unknown",
            "retryable": False,
            "provider": self.provider,
            "request_id": self.request_id,
            "message": "%s" % (type(error).__name__,),
        }

    def __repr__(self) -> str:  # pragma: no cover - 仅排障用
        return "<CallScope %s prompt=%s provider=%s err=%s>" % (
            self.request_id,
            self.prompt_id or "-",
            self.provider or "-",
            type(self.error).__name__ if self.error else "none",
        )


@contextmanager
def ai_call_scope(prompt_id: str = "", *, provider: str = "") -> Iterator[CallScope]:
    """声明"接下来这次 AI 调用用的是哪个 Prompt 模板"。

    用法（**不改变被调用方的签名**，因此旧代码零改动）::

        with ai_call_scope("resume/analysis@v1") as scope:
            text = await spark_api.chat_async(prompt)
        if scope.error is not None:
            ...  # 结构化降级

    ``prompt_id`` 约定形如 ``<分组>/<模板>@<版本>``；版本段缺省视为 ``v1``。
    """
    scope = CallScope(new_request_id(), prompt_id or "")
    scope.provider = provider
    token_prompt = _PROMPT_ID.set(scope.prompt_id)
    token_request = _REQUEST_ID.set(scope.request_id)
    token_scope = _CURRENT_SCOPE.set(scope)
    try:
        yield scope
    finally:
        _CURRENT_SCOPE.reset(token_scope)
        _PROMPT_ID.reset(token_prompt)
        _REQUEST_ID.reset(token_request)


def log_event(event: str, **fields: Any) -> None:
    """结构化日志：一行一条 JSON，便于采集。

    **调用方不得把 Prompt 正文、凭据、用户手机号等塞进 fields**——
    这里不做内容脱敏（脱敏是数据边界层 :mod:`modules.ai.boundary` 的职责），
    本函数只保证"一条日志 = 一行 JSON"。
    """
    payload: Dict[str, Any] = {"event": event}
    for key, value in fields.items():
        if value is None or value == "":
            continue
        payload[key] = value
    try:
        line = json.dumps(payload, ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001 - 日志不得影响主流程
        line = '{"event": "%s", "log_error": "unserializable"}' % event
    _log.info(line)


def snapshot() -> Dict[str, Any]:
    """指标快照（含按 provider 的分解）。"""
    return {
        "metrics": METRICS.snapshot(),
        "by_provider": METRICS.snapshot_by_provider(),
    }
