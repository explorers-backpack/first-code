# -*- coding: utf-8 -*-
"""AI 能力接入层。

分层
----
::

    api / services（业务）
            │  只依赖窄接口 + 工厂
            ▼
    modules.ai  ────────────────────────────────────────────
      llm.py           LLM 窄接口（LlmProvider / LlmRequest / LlmResponse）
      llm_spark.py     星火实现（transport 注入，不重写协议）
      llm_fake.py      Fake（假装成功）/ Offline（确定失败）
      policy.py        超时 / 有限重试 / 指数退避 / 非幂等不重试
      telemetry.py     请求追踪 / Prompt 版本 / 指标 / Token 用量
      boundary.py      Prompt 数据边界（越权、无关数据、敏感信息）
      modes.py         mock / offline / production + 启动校验
      factory.py       唯一装配点（按模式构造 Provider）
      compat.py        旧字符串契约门面（冻结业务文件零改动）
    ──────────────────────────────────────────────────────────
            │
            ▼
    供应商 SDK / 客户端（main.SparkAPI 的 websocket 协议等）

四类能力**不合并**成一个大接口：LLM 用 :class:`LlmProvider`；
Embedding 复用 ``services.embedding_provider`` 的既有 ABC；
数字人（含内置 ASR/TTS）复用 ``services.avatar_provider`` 的既有 ABC。
统一的是**横向关注点**（模式 / 错误 / 指标 / 追踪 / 边界），不是接口。

典型用法
--------
业务代码（新链路）::

    from modules.ai import LlmRequest, call_llm, get_llm_provider, ai_call_scope

    with ai_call_scope("resume/analysis@v1") as scope:
        try:
            resp = await call_llm(get_llm_provider(), LlmRequest(prompt=prompt))
        except AiUnavailableError:
            text = fallback_text()

旧链路（冻结文件，零改动）::

    text = await spark_api.chat_async(prompt)   # 门面内部已走完整链路
"""

from modules.ai.boundary import (
    DEFAULT_MAX_PROMPT_CHARS,
    PromptBoundary,
    build_guarded_prompt,
    check_boundary,
    ensure_owner,
    scrub_contact_info,
)
from modules.ai.compat import LegacySparkFacade, build_legacy_spark_facade
from modules.ai.errors import (
    AiAuthError,
    AiBoundaryError,
    AiConfigError,
    AiError,
    AiModeError,
    AiRateLimitError,
    AiResponseError,
    AiTimeoutError,
    AiUnavailableError,
    LEGACY_FAILURE_PREFIXES,
    classify_transport_error,
    is_legacy_failure,
)
from modules.ai.factory import (
    ENV_LLM_PROVIDER,
    LLM_PROVIDERS,
    PROVIDER_FAKE,
    PROVIDER_SPARK,
    build_llm_provider,
    describe_capabilities,
    describe_llm,
    get_call_policy,
    get_llm_provider,
    get_spark_transport,
    register_spark_transport,
    reset_llm_provider,
    validate_startup,
)
from modules.ai.llm import LlmProvider, LlmRequest, LlmResponse, LlmUsage
from modules.ai.llm_fake import MOCK_LLM_TEXT, FakeLlmProvider, OfflineLlmProvider
from modules.ai.llm_spark import SparkLlmProvider
from modules.ai.modes import (
    ENV_RUN_MODE,
    MODE_MOCK,
    MODE_OFFLINE,
    MODE_PRODUCTION,
    RUN_MODES,
    credentials_configured,
    describe_run_mode,
    resolve_run_mode,
    validate_ai_config,
)
from modules.ai.policy import CallPolicy, call_llm
from modules.ai.telemetry import (
    METRIC_KEYS,
    METRICS,
    CallScope,
    ai_call_scope,
    current_prompt_id,
    current_scope,
    log_event,
    new_request_id,
    snapshot,
)

__all__ = [
    # errors
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
    # modes
    "ENV_RUN_MODE",
    "MODE_MOCK",
    "MODE_OFFLINE",
    "MODE_PRODUCTION",
    "RUN_MODES",
    "resolve_run_mode",
    "credentials_configured",
    "describe_run_mode",
    "validate_ai_config",
    # llm
    "LlmProvider",
    "LlmRequest",
    "LlmResponse",
    "LlmUsage",
    "SparkLlmProvider",
    "FakeLlmProvider",
    "OfflineLlmProvider",
    "MOCK_LLM_TEXT",
    # policy
    "CallPolicy",
    "call_llm",
    # telemetry
    "METRICS",
    "METRIC_KEYS",
    "CallScope",
    "ai_call_scope",
    "current_prompt_id",
    "current_scope",
    "new_request_id",
    "log_event",
    "snapshot",
    # boundary
    "PromptBoundary",
    "DEFAULT_MAX_PROMPT_CHARS",
    "check_boundary",
    "build_guarded_prompt",
    "scrub_contact_info",
    "ensure_owner",
    # factory
    "ENV_LLM_PROVIDER",
    "LLM_PROVIDERS",
    "PROVIDER_SPARK",
    "PROVIDER_FAKE",
    "build_llm_provider",
    "get_llm_provider",
    "reset_llm_provider",
    "get_call_policy",
    "get_spark_transport",
    "register_spark_transport",
    "describe_llm",
    "describe_capabilities",
    "validate_startup",
    # compat
    "LegacySparkFacade",
    "build_legacy_spark_facade",
]
