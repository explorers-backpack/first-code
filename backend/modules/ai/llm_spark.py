# -*- coding: utf-8 -*-
"""AI 能力接入层 · 星火（讯飞 Spark X1）LLM Provider。

刻意**不重写协议**
------------------
旧实现 ``main.SparkAPI`` 里那套「HMAC 签名 URL + WebSocket 分帧 + ``reasoning_content``
与 ``content`` 分开累计」的逻辑是有状态、有历史包袱的（X1 是推理模型，
两者混在一起会把思考过程当结论返回给用户）。重写它等于重新踩一遍这些坑，
而且没有真实密钥根本测不了。

所以本 Provider 只接受一个 **transport**：``async (prompt: str) -> str``。
生产上注入 ``main.SparkAPI().chat_async``，测试上注入任意 Fake。
这样"协议"与"策略"分开：

* transport 负责「怎么说星火的话」；
* 本 Provider 负责「把它的字符串失败收敛成类型化错误、把成功装进 LlmResponse」；
* :func:`modules.ai.policy.call_llm` 负责「超时 / 重试 / 退避 / 指标」。

换供应商 = 换一个 transport（或换一个 Provider），**面试业务核心代码零改动**。
"""

from common.rag_trace import context_submitted
import time
from typing import Any, Awaitable, Callable, Dict, Optional

from modules.ai.errors import (
    AiAuthError,
    AiRateLimitError,
    AiResponseError,
    AiTimeoutError,
    AiUnavailableError,
    classify_transport_error,
    is_legacy_failure,
)
from modules.ai.llm import LlmProvider, LlmRequest, LlmResponse, LlmUsage, elapsed_ms

__all__ = ["SparkLlmProvider", "SparkTransport"]

#: ``async (prompt) -> str``。**允许返回旧式失败哨兵串**——本 Provider 会识别它。
SparkTransport = Callable[[str], Awaitable[Any]]


def _classify_legacy_failure(text: str, provider: str = "") -> Exception:
    """把旧客户端的失败哨兵串转成类型化错误。

    ``main.SparkAPI`` 的失败分支只有三种文案（见 ``errors.LEGACY_FAILURE_PREFIXES``）：
    空回答、不可用、以及"API调用失败: <原始异常>"。最后一种内部才是真正的错误类型，
    所以再做一次关键词归类——这里的关键词**只用于兼容层**，新代码不该走这条路径。
    """
    body = text.strip()
    lowered = body.lower()
    if body.startswith("AI 回答为空"):
        return AiResponseError("模型返回空内容", provider=provider)
    if body.startswith("SparkAPI not available"):
        return AiUnavailableError(
            "星火客户端不可用（websocket-client 未就绪或未初始化）", provider=provider
        )
    if any(hint in lowered for hint in ("timeout", "timed out", "超时")):
        return AiTimeoutError("星火调用超时：%s" % body[:200], provider=provider)
    if any(hint in lowered for hint in ("401", "403", "auth", "鉴权", "signature")):
        return AiAuthError("星火鉴权失败：%s" % body[:200], provider=provider)
    if any(hint in lowered for hint in ("429", "rate limit", "quota", "限流")):
        return AiRateLimitError("星火限流：%s" % body[:200], provider=provider)
    return AiUnavailableError("星火返回失败：%s" % body[:200], provider=provider)


class SparkLlmProvider(LlmProvider):
    """星火 X1 的 LLM Provider（transport 注入式）。"""

    name = "spark"

    def __init__(
        self,
        transport: SparkTransport,
        *,
        model: str = "x1",
        provider_name: str = "",
        max_prompt_chars: int = 20000,
    ) -> None:
        if not callable(transport):
            raise TypeError("SparkLlmProvider 需要可调用的 transport(prompt) -> awaitable")
        self._transport = transport
        self._model = model
        self._name = provider_name or self.name
        self._max_prompt_chars = max_prompt_chars

    def describe(self) -> Dict[str, Any]:
        return {
            "provider": self._name,
            "model": self._model,
            "transport": getattr(self._transport, "__qualname__", repr(self._transport)),
            "max_prompt_chars": self._max_prompt_chars,
        }

    async def complete(self, request: LlmRequest) -> LlmResponse:
        started = time.monotonic()
        prompt = request.prompt
        if self._max_prompt_chars and len(prompt) > self._max_prompt_chars:
            # 兜底：即使调用方漏了边界检查，也不把超大 Prompt 发出去。
            raise AiResponseError(
                "Prompt 长度 %d 超过 Provider 上限 %d" % (len(prompt), self._max_prompt_chars),
                provider=self._name,
            )

        try:
            context_submitted(prompt)
            raw = await self._transport(prompt)
        except Exception as exc:  # noqa: BLE001 - 统一收敛，不让厂商异常外泄
            raise classify_transport_error(exc, provider=self._name) from exc

        text = "" if raw is None else (raw if isinstance(raw, str) else str(raw))

        if is_legacy_failure(text):
            # 旧客户端把失败当返回值；**必须在这里升级为异常**，
            # 否则上层又会退化成"判字符串前缀"。
            raise _classify_legacy_failure(text, provider=self._name)

        if not text.strip():
            raise AiResponseError("模型返回空内容", provider=self._name)

        return LlmResponse(
            text=text,
            provider=self._name,
            model=request.model or self._model,
            # 旧 transport 不回报 Token 用量：**保持 0 并标记未估算**，不猜。
            usage=LlmUsage(estimated=False),
            latency_ms=elapsed_ms(started),
            request_id="",
            prompt_id=request.prompt_id,
            prompt_version=request.prompt_version,
        )


def build_transport_from_client(client: Any) -> Optional[SparkTransport]:
    """从旧式客户端（需提供 ``async chat_async(str) -> str``）取 transport。

    取不到返回 ``None``，由调用方决定"报错"还是"降级"——
    **本函数不抛异常**，因为它在启动路径上。
    """
    chat_async = getattr(client, "chat_async", None)
    if callable(chat_async):
        return chat_async  # type: ignore[return-value]
    return None
