# -*- coding: utf-8 -*-
"""AI 能力接入层 · 离线 Provider（Fake / Offline）。

两个都"不联网"，但**语义相反**，不能合并：

* :class:`FakeLlmProvider` —— **假装成功**。按脚本返回确定性文本，
  用来跑通业务逻辑、做契约测试、前端联调。零密钥、零网络、零随机。
* :class:`OfflineLlmProvider` —— **确定失败**。任何调用都抛
  :class:`~modules.ai.errors.AiUnavailableError`，用来验证**降级路径**
  （"模型挂了页面还能不能用"这条路径，靠 Fake 是测不出来的）。

两者都记录 :attr:`calls`，契约测试可以直接断言"业务层把正确的 Prompt 传下来了"，
而不必去解析日志。
"""

import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

from modules.ai.errors import AiError, AiUnavailableError
from modules.ai.llm import LlmProvider, LlmRequest, LlmResponse, LlmUsage, elapsed_ms

__all__ = [
    "MOCK_LLM_TEXT",
    "FakeLlmProvider",
    "OfflineLlmProvider",
    "ScriptedLlmProvider",
]

#: ``mock`` 模式下返回的固定文本。**必须明显标记为 mock**——
#: 让假数据看起来像真数据，是这类模式最常见的生产事故来源。
MOCK_LLM_TEXT = (
    "【核心优势】：（mock 模式）本次未调用真实模型，输出为固定占位文本。\n"
    "【提升建议】：需要真实诊断时，请配置凭据并设置 AI_RUN_MODE=production。"
)

#: 脚本可以是"固定文本序列"或"按请求生成文本的函数"。
ScriptItem = Union[str, Callable[[LlmRequest], str]]
FailureItem = Union[BaseException, Callable[[LlmRequest], BaseException]]


class FakeLlmProvider(LlmProvider):
    """按脚本返回确定性结果的 LLM 替身。

    :param responses: 文本序列（可为空 ⇒ 一律用 :data:`MOCK_LLM_TEXT`）。
        序列耗尽后**重复最后一条**，避免测试因为"多问了一次"而越界崩溃。
    :param failures: 失败脚本。与 ``responses`` **独立计数**：第 n 次调用若
        ``failures`` 还有剩余，就抛对应异常（用于验证重试 / 退避）。
    :param usage: 固定回报的 Token 用量（默认全 0 且 ``estimated=False``）。
    """

    name = "fake"

    def __init__(
        self,
        responses: Optional[Sequence[ScriptItem]] = None,
        *,
        failures: Optional[Sequence[FailureItem]] = None,
        default_text: str = MOCK_LLM_TEXT,
        model: str = "fake-llm",
        usage: Optional[LlmUsage] = None,
        latency_ms: float = 0.0,
    ) -> None:
        self._responses: List[ScriptItem] = list(responses or [])
        self._failures: List[FailureItem] = list(failures or [])
        self._default_text = default_text
        self._model = model
        self._usage = usage or LlmUsage()
        self._latency_ms = float(latency_ms)
        self.calls: List[LlmRequest] = []

    # ---- 供测试断言 ----
    @property
    def call_count(self) -> int:
        return len(self.calls)

    @property
    def prompts(self) -> List[str]:
        return [call.prompt for call in self.calls]

    def describe(self) -> Dict[str, Any]:
        return {
            "provider": self.name,
            "model": self._model,
            "scripted": len(self._responses),
            "scripted_failures": len(self._failures),
            "call_count": len(self.calls),
        }

    def _next_failure(self, request: LlmRequest) -> Optional[BaseException]:
        if not self._failures:
            return None
        item = self._failures.pop(0)
        if callable(item) and not isinstance(item, BaseException):
            produced = item(request)
            return produced if isinstance(produced, BaseException) else AiError(str(produced))
        return item

    def _next_text(self, request: LlmRequest) -> str:
        if not self._responses:
            return self._default_text
        item = self._responses.pop(0) if len(self._responses) > 1 else self._responses[0]
        if callable(item):
            return item(request)
        return item

    async def complete(self, request: LlmRequest) -> LlmResponse:
        self.calls.append(request)
        started = time.monotonic()

        failure = self._next_failure(request)
        if failure is not None:
            raise failure

        text = self._next_text(request)
        return LlmResponse(
            text=text,
            provider=self.name,
            model=request.model or self._model,
            usage=self._usage,
            latency_ms=self._latency_ms or elapsed_ms(started),
            request_id="",
            prompt_id=request.prompt_id,
            prompt_version=request.prompt_version,
        )


class ScriptedLlmProvider(FakeLlmProvider):
    """语义别名：强调"按脚本执行"。行为与 :class:`FakeLlmProvider` 完全一致。"""

    name = "scripted"


class OfflineLlmProvider(LlmProvider):
    """离线模式：**确定失败**，永不联网。

    抛 :class:`AiUnavailableError`（``retryable=True``），但离线模式的
    重试次数在工厂层被压成 1 次——离线重试没有意义，只会拖慢降级。
    """

    name = "offline"

    def __init__(self, reason: str = "AI 处于离线模式（未配置凭据或显式 AI_RUN_MODE=offline）") -> None:
        self._reason = reason
        self.calls: List[LlmRequest] = []

    @property
    def call_count(self) -> int:
        return len(self.calls)

    def describe(self) -> Dict[str, Any]:
        return {"provider": self.name, "model": "", "reason": self._reason}

    async def complete(self, request: LlmRequest) -> LlmResponse:
        self.calls.append(request)
        raise AiUnavailableError(self._reason, provider=self.name)
