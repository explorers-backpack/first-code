# -*- coding: utf-8 -*-
"""AI 能力接入层 · 旧接口兼容层（``spark_api.chat_async`` 字符串契约）。

为什么需要兼容层
----------------
重构前，``main.spark_api`` 的契约是「``async chat_async(prompt) -> str``，
失败时返回**中文字符串哨兵**」。这套契约被两处代码硬依赖：

* ``services/interview_agent._is_spark_failure`` —— 按前缀判定失败
  （``interview_agent`` / ``interview_planner`` / ``interview_followup_agent`` /
  ``interview_next_agent`` 都走它）；
* ``main.py`` 的三个路由 —— 同样按前缀判定。

这些文件里有 5 个在**源码指纹冻结清单**内（改动即触发守卫），所以本次重构
**一行都不改**它们，而是把 ``main.spark_api`` 换成一个**门面**：
对外仍是"字符串进、字符串出、失败给哨兵"，对内则走
模式 → Provider → 策略（超时/重试/退避）→ 指标 → 追踪 的完整链路。

失败哨兵**逐字沿用旧文案**，因此冻结文件的 ``_is_spark_failure`` 判定、
以及既有测试对哨兵串的断言全部不变。同时门面把**结构化原因**写进
:class:`~modules.ai.telemetry.CallScope`，新代码可以用类型化错误做降级，
不必再猜字符串。
"""

import asyncio
from dataclasses import replace
from typing import Any, Dict, Optional

from modules.ai.errors import (
    AiBoundaryError,
    AiAuthError,
    AiError,
    AiModeError,
    AiRateLimitError,
    AiResponseError,
    AiTimeoutError,
    AiUnavailableError,
)
from modules.ai.factory import get_call_policy, get_llm_provider
from modules.ai.boundary import PromptBoundary, build_guarded_prompt
from modules.ai.llm import LlmProvider, LlmRequest
from modules.ai.llm_fake import OfflineLlmProvider
from modules.ai.policy import CallPolicy, call_llm
from modules.ai.telemetry import CallScope, current_prompt_id, current_scope
from modules.ai.telemetry import METRICS, METRIC_BOUNDARY_BLOCKS, log_event, new_request_id

__all__ = ["LegacySparkFacade", "build_legacy_spark_facade"]


class LegacySparkFacade:
    """旧式 ``spark_api`` 门面。

    :param provider_getter: 取 Provider 的函数（默认 :func:`get_llm_provider`）
    :param policy: 固定策略；``None`` ⇒ 每次按环境取（:func:`get_call_policy`）

    **门面自身不持有配置**——配置解析全部交给工厂，这样"换 Provider 不改业务代码"
    这条验收标准才成立。
    """

    def __init__(
        self,
        *,
        provider_getter: Any = None,
        policy: Optional[CallPolicy] = None,
    ) -> None:
        self._provider_getter = provider_getter or get_llm_provider
        self._policy = policy

    # ------------------------------------------------------------------
    # 旧契约
    # ------------------------------------------------------------------
    def chat(self, message: str) -> str:
        """同步版本（保留是为了兼容可能的旧调用方）。

        **不得在事件循环内调用**——旧实现是阻塞 websocket，同样如此。
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.chat_async(message))
        raise RuntimeError("LegacySparkFacade.chat() 不能在事件循环内调用，请改用 await chat_async()")

    async def chat_async(self, message: str) -> str:
        """``str -> str``：成功返回模型文本，失败返回**旧式哨兵串**（不抛异常）。"""
        scope: Optional[CallScope] = current_scope()
        try:
            # 所有生产业务共享此入口；在 Provider 选择/传输之前拒绝越界输入。
            prompt = build_guarded_prompt(message, PromptBoundary())
            request = LlmRequest(prompt=prompt, prompt_id=current_prompt_id())
            provider: LlmProvider = self._provider_getter()
            policy = self._policy or get_call_policy()
            if isinstance(provider, OfflineLlmProvider):
                policy = replace(policy, max_attempts=1)
            response = await call_llm(provider, request, policy=policy, scope=scope)
        except BaseException as exc:  # noqa: BLE001 - 旧契约要求"绝不抛"
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            error = exc if isinstance(exc, AiError) else AiError("%s: %s" % (type(exc).__name__, exc))
            if not error.request_id:
                error.request_id = scope.request_id if scope else new_request_id()
            if isinstance(error, AiBoundaryError):
                METRICS.incr(METRIC_BOUNDARY_BLOCKS)
            # 配置/边界错误发生在策略之前，也必须可诊断；不记录 Prompt 正文。
            if scope is None or scope.error is None:
                log_event("ai_boundary_failed", error_kind=error.kind,
                          request_id=error.request_id, prompt_id=current_prompt_id())
            if scope is not None and scope.error is None:
                scope.error = error
            return self.render_failure(error)
        return response.text

    # ------------------------------------------------------------------
    # 兼容渲染
    # ------------------------------------------------------------------
    @staticmethod
    def render_failure(error: BaseException) -> str:
        """把类型化错误渲染成**旧式哨兵串**（前缀必须落在 ``LEGACY_FAILURE_PREFIXES`` 内）。"""
        if isinstance(error, AiResponseError):
            return "AI 回答为空"
        if isinstance(error, AiModeError):
            # 未配置 / 配置非法：与旧实现 `_spark_ok == False` 的文案保持一致
            return "SparkAPI not available"
        if isinstance(error, AiTimeoutError):
            return "API调用失败: 请求超时"
        if isinstance(error, AiAuthError):
            return "API调用失败: 鉴权失败"
        if isinstance(error, AiRateLimitError):
            return "API调用失败: 供应商限流"
        if isinstance(error, AiUnavailableError):
            return "API调用失败: 服务不可用"
        if isinstance(error, AiError):
            return "API调用失败: %s" % (error.kind or "unknown")
        return "API调用失败: %s" % type(error).__name__

    def describe(self) -> Dict[str, Any]:
        """当前门面背后的真实 Provider（**不联网、不抛异常**）。"""
        try:
            provider = self._provider_getter()
        except Exception as exc:  # noqa: BLE001 - 状态上报永不抛异常
            return {"provider": "unavailable", "error": "%s: %s" % (type(exc).__name__, exc)}
        detail = provider.describe() if hasattr(provider, "describe") else {}
        return {"provider": getattr(provider, "name", ""), "detail": detail}

    def __repr__(self) -> str:  # pragma: no cover - 仅排障用
        return "<LegacySparkFacade>"


def build_legacy_spark_facade(
    *,
    provider_getter: Any = None,
    policy: Optional[CallPolicy] = None,
) -> LegacySparkFacade:
    """构造门面（供 ``main.py`` 顶替旧的 ``spark_api = SparkAPI()``）。"""
    return LegacySparkFacade(provider_getter=provider_getter, policy=policy)
