# -*- coding: utf-8 -*-
"""AI 能力接入层 · LLM 窄接口与数据模型。

为什么是"窄接口"而不是一个万能 Provider
---------------------------------------
LLM、Embedding、语音识别、数字人的**协议与数据模型完全不同**：

* LLM 输入一段文本、输出一段文本（还有 Token 用量、模型版本）；
* Embedding 输入一批文本、输出一批定长向量（还有维度、role=query|para）；
* 语音识别输入音频流、输出中间/最终文本（还有断句、置信度）；
* 数字人是有状态的会话（创建 / 驱动 / 打断 / 结束）。

把它们合并成 ``Provider.invoke(anything)`` 只会得到一个**每个字段都只对一种能力
有意义**的结构，调用方仍要自己判断"这个字段现在有没有用"——那是把类型系统扔掉
换来的"统一"。所以本层按能力分接口：

* LLM —— 本模块 :class:`LlmProvider`（新增）
* Embedding —— 既有 ``services.embedding_provider.EmbeddingProvider``
  （已有 ABC + 配置 + 超时 + 批量 + role，本层**复用不重写**）
* 数字人（含内置 ASR/TTS）—— 既有 ``services.avatar_provider.AvatarProvider``
  （已有 ABC + Mock + 错误分类，本层**复用不重写**）

四者的**统一之处**放在横向关注点上，而不是接口上：运行模式
（:mod:`modules.ai.modes`）、错误分类（:mod:`modules.ai.errors`）、
指标与追踪（:mod:`modules.ai.telemetry`）、数据边界（:mod:`modules.ai.boundary`）。
"""

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

__all__ = [
    "LlmUsage",
    "LlmRequest",
    "LlmResponse",
    "LlmProvider",
]


@dataclass(frozen=True)
class LlmUsage:
    """Token 用量。供应商不返回时**保持 0 并置 ``estimated=False``**，不猜。"""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    estimated: bool = False

    def as_dict(self) -> Dict[str, Any]:
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "estimated": self.estimated,
        }


@dataclass(frozen=True)
class LlmRequest:
    """一次补全请求。

    :param prompt: 完整 Prompt 正文（**必须已经过数据边界检查**）
    :param prompt_id: 模板标识，形如 ``resume/analysis@v1``（用于"线上跑的是哪版 Prompt"）
    :param model: 期望模型；空串 = 用 Provider 默认
    :param temperature: 采样温度；``None`` = 用 Provider 默认
    :param max_tokens: 输出上限；``None`` = 用 Provider 默认
    :param idempotent: **能否安全重试**。

        LLM 补全本身是只读调用，默认 ``True``（失败重试不会产生重复副作用）。
        但若调用方把"补全 + 落库/扣费"当成一次语义操作，重复执行会有副作用，
        必须显式置 ``False``——此时 :class:`~modules.ai.policy.CallPolicy`
        **强制只尝试 1 次**，不重试。
    :param metadata: 附加追踪字段（**不得放 Prompt 正文 / 凭据 / 用户隐私**）
    """

    prompt: str
    prompt_id: str = ""
    model: str = ""
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    idempotent: bool = True
    metadata: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.prompt, str):
            raise TypeError("LlmRequest.prompt 必须是 str，收到 %s" % type(self.prompt).__name__)

    @property
    def prompt_version(self) -> str:
        """从 ``prompt_id`` 里取版本段（``a/b@v2`` ⇒ ``v2``；无 ``@`` ⇒ ``v1``）。"""
        if "@" in self.prompt_id:
            version = self.prompt_id.rsplit("@", 1)[1].strip()
            if version:
                return version
        return "v1"


@dataclass(frozen=True)
class LlmResponse:
    """一次补全结果（**成功才会出现**；失败一律抛 :class:`~modules.ai.errors.AiError`）。"""

    text: str
    provider: str = ""
    model: str = ""
    usage: LlmUsage = field(default_factory=LlmUsage)
    latency_ms: float = 0.0
    request_id: str = ""
    prompt_id: str = ""
    prompt_version: str = "v1"
    finish_reason: str = "stop"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "usage": self.usage.as_dict(),
            "latency_ms": round(self.latency_ms, 3),
            "request_id": self.request_id,
            "prompt_id": self.prompt_id,
            "prompt_version": self.prompt_version,
            "finish_reason": self.finish_reason,
            "chars": len(self.text),
        }


class LlmProvider(ABC):
    """LLM 供应商的**唯一**接口。

    实现方只需要做三件事：发请求、把失败收敛成 :class:`~modules.ai.errors.AiError`
    子类、把结果装进 :class:`LlmResponse`。**超时 / 重试 / 退避 / 指标不在这里做**——
    那是 :func:`modules.ai.policy.call_llm` 的职责，这样换供应商不会连带丢策略。
    """

    #: 供应商标识（进日志与指标，必须是稳定短名）
    name = "llm"

    @abstractmethod
    async def complete(self, request: LlmRequest) -> LlmResponse:
        """执行一次补全。**失败必须抛 AiError 子类**，不得返回哨兵字符串。"""

    async def aclose(self) -> None:  # pragma: no cover - 默认无资源可释放
        """释放连接 / 会话。默认空实现。"""
        return None

    def describe(self) -> Dict[str, Any]:
        """配置摘要（**不联网、不抛异常、不含凭据**）。"""
        return {"provider": self.name}


def elapsed_ms(started_at: float) -> float:
    """``time.monotonic()`` 起点到现在的毫秒数。"""
    return (time.monotonic() - started_at) * 1000.0
