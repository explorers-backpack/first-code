# -*- coding: utf-8 -*-
"""AI 能力接入层 · Provider 工厂与启动校验。

唯一装配点
----------
业务代码**不得**自己 ``os.getenv("SPARK_...")`` 或 ``SparkAPI()``。
需要 LLM 就调 :func:`get_llm_provider`，需要看配置就调 :func:`describe_llm`。
换供应商只改这里 + 注入一个新的 transport，**面试业务核心零改动**。

生产模式的硬规则
----------------
``production`` 下：

* 只允许真实 Provider（``spark``）；
* 显式选 ``fake`` ⇒ :class:`AiModeError`——**生产环境不能静默回退到 Mock**，
  显式选也不行（假数据进了生产就是事故）；
* 传输层未注册 ⇒ :class:`AiModeError`（启动期就该发现）。

配置项（全部"不配 = 默认 = 行为不变"）
--------------------------------------
* ``AI_RUN_MODE`` —— ``mock`` / ``offline`` / ``production``；留空按凭据自动判定
* ``AI_LLM_PROVIDER`` —— ``spark``（默认）/ ``fake``（仅非生产模式可用）
* ``AI_LLM_TIMEOUT`` / ``AI_LLM_MAX_ATTEMPTS`` / ``AI_LLM_BACKOFF_BASE`` —— 见 policy
"""

import logging
import os
from typing import Any, Dict, Mapping, Optional

from modules.ai.errors import AiConfigError, AiModeError
from modules.ai.llm import LlmProvider
from modules.ai.llm_fake import MOCK_LLM_TEXT, FakeLlmProvider, OfflineLlmProvider
from modules.ai.llm_spark import SparkLlmProvider, SparkTransport
from modules.ai.modes import (
    MODE_MOCK,
    MODE_OFFLINE,
    MODE_PRODUCTION,
    credentials_configured,
    describe_run_mode,
    missing_credentials,
    resolve_run_mode,
    validate_ai_config,
)
from modules.ai.policy import CallPolicy
from modules.ai.telemetry import METRICS, snapshot

__all__ = [
    "ENV_LLM_PROVIDER",
    "PROVIDER_SPARK",
    "PROVIDER_FAKE",
    "LLM_PROVIDERS",
    "register_spark_transport",
    "get_spark_transport",
    "build_llm_provider",
    "get_llm_provider",
    "reset_llm_provider",
    "get_call_policy",
    "describe_llm",
    "describe_capabilities",
    "validate_startup",
]

_log = logging.getLogger("career_ai.ai")

ENV_LLM_PROVIDER = "AI_LLM_PROVIDER"
PROVIDER_SPARK = "spark"
PROVIDER_FAKE = "fake"
LLM_PROVIDERS = (PROVIDER_SPARK, PROVIDER_FAKE)

#: 星火传输层（``async (prompt) -> str``）。由 ``main.py`` 在 import 期注册，
#: 这样工厂不必反向 import ``main``（会循环）。
_TRANSPORT: Optional[SparkTransport] = None

#: 进程内缓存的 Provider / 策略。缓存键含模式与 Provider 名，
#: 环境变化时自动重建（不必让测试到处 reset）。
_CACHE: Dict[str, Any] = {"key": None, "provider": None, "policy": None, "policy_key": None}


def register_spark_transport(transport: Optional[SparkTransport]) -> None:
    """注册星火传输层（**唯一**允许业务侧注入的点）。``None`` = 注销。"""
    global _TRANSPORT
    if transport is not None and not callable(transport):
        raise TypeError("transport 必须可调用（async (prompt) -> str）")
    _TRANSPORT = transport
    reset_llm_provider()


def get_spark_transport() -> Optional[SparkTransport]:
    return _TRANSPORT


def _read(env: Optional[Mapping[str, Any]], key: str, default: str = "") -> str:
    source = os.environ if env is None else env
    value = source.get(key, "")
    text = default if value is None else str(value).strip()
    return text or default


def build_llm_provider(env: Optional[Mapping[str, Any]] = None) -> LlmProvider:
    """按运行模式装配 Provider。**非法配置一律抛错，不静默降级**。"""
    mode = resolve_run_mode(env)

    if mode == MODE_MOCK:
        return FakeLlmProvider(default_text=MOCK_LLM_TEXT, model="mock-llm")

    if mode == MODE_OFFLINE:
        return OfflineLlmProvider()

    # ---- production ----
    chosen = _read(env, ENV_LLM_PROVIDER, PROVIDER_SPARK).lower()
    if chosen not in LLM_PROVIDERS:
        raise AiModeError(
            "%s 取值非法：%r，允许 %s" % (ENV_LLM_PROVIDER, chosen, " / ".join(LLM_PROVIDERS))
        )
    if chosen == PROVIDER_FAKE:
        raise AiModeError(
            "生产模式禁止使用 fake Provider（假数据不得进入生产）；"
            "如需联调请显式设置 AI_RUN_MODE=mock"
        )
    if not credentials_configured(env):
        raise AiModeError(
            "生产模式缺少凭据：%s" % " / ".join(missing_credentials(env))
        )
    transport = _TRANSPORT
    if transport is None:
        raise AiModeError(
            "生产模式未注册星火传输层：请在启动时调用 "
            "modules.ai.register_spark_transport(SparkAPI().chat_async)"
        )
    return SparkLlmProvider(transport)


def _cache_key(env: Optional[Mapping[str, Any]]) -> tuple:
    return (
        resolve_run_mode(env),
        _read(env, ENV_LLM_PROVIDER, PROVIDER_SPARK).lower(),
        id(_TRANSPORT),
    )


def get_llm_provider() -> LlmProvider:
    """取进程内 LLM Provider（按模式缓存，模式变化自动重建）。"""
    # 缓存不能绕过配置撤销检查（生产模式凭据被移除时立即停止调用）。
    validate_ai_config()
    key = _cache_key(None)
    if _CACHE["provider"] is None or _CACHE["key"] != key:
        _CACHE["provider"] = build_llm_provider(None)
        _CACHE["key"] = key
    return _CACHE["provider"]  # type: ignore[return-value]


def reset_llm_provider(provider: Optional[LlmProvider] = None) -> Optional[LlmProvider]:
    """清空 / 替换缓存（**测试用**）。``reset_llm_provider()`` ⇒ 下次重新按环境解析。"""
    _CACHE["provider"] = provider
    _CACHE["key"] = _cache_key(None) if provider is not None else None
    return _CACHE["provider"]


def get_call_policy() -> CallPolicy:
    """取进程内策略。

    配置非法时**记 ERROR 并退回默认值**——与 ``INTERVIEW_AVATAR_LLM_QUESTIONS``
    的既有口径一致：不静默（有 ERROR 日志），但不让一个配置拼写错误打挂整个服务。
    启动校验（:func:`validate_startup`）会把这个错误提前暴露。
    """
    env_key = (
        os.environ.get("AI_LLM_TIMEOUT", ""),
        os.environ.get("AI_LLM_MAX_ATTEMPTS", ""),
        os.environ.get("AI_LLM_BACKOFF_BASE", ""),
    )
    if _CACHE["policy"] is not None and _CACHE["policy_key"] == env_key:
        return _CACHE["policy"]  # type: ignore[return-value]
    try:
        policy = CallPolicy.from_env(None)
    except AiConfigError as exc:
        _log.error("[ai] 策略配置非法，已退回默认值：%s", exc)
        policy = CallPolicy()
    _CACHE["policy"] = policy
    _CACHE["policy_key"] = env_key
    return policy


def describe_llm(env: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """纯读配置、**不构造 Provider、不联网、绝不抛异常**（供日志 / 运维查询）。"""
    out: Dict[str, Any] = {
        "run_mode": describe_run_mode(env),
        "provider": _read(env, ENV_LLM_PROVIDER, PROVIDER_SPARK).lower(),
        "known_providers": list(LLM_PROVIDERS),
        "transport_registered": _TRANSPORT is not None,
        "credentials_configured": credentials_configured(env),
        "missing_credentials": missing_credentials(env),
        "effective_provider": "",
        "policy": {},
        "error": None,
    }
    try:
        out["policy"] = CallPolicy.from_env(env).as_dict()
    except AiConfigError as exc:
        out["error"] = "%s: %s" % (type(exc).__name__, exc)
        out["policy"] = CallPolicy().as_dict()
    try:
        provider = build_llm_provider(env)
        out["effective_provider"] = provider.name
        out["provider_detail"] = provider.describe()
    except Exception as exc:  # noqa: BLE001 - 状态上报永不抛异常
        out["error"] = out["error"] or "%s: %s" % (type(exc).__name__, exc)
        out["effective_provider"] = "unavailable"
    return out


def describe_capabilities(env: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """四类 AI 能力的**统一可观测视图**。

    注意：这里只是**汇总状态**，不是把四个接口合并成一个。
    每类能力仍然是自己的窄接口（LLM / Embedding / Avatar 各自独立），
    Embedding 与数字人的状态直接复用它们既有的 ``describe_*`` 实现，
    本层不重复实现、也不改写它们的配置口径。
    """
    result: Dict[str, Any] = {
        "run_mode": describe_run_mode(env),
        "capabilities": {
            "llm": describe_llm(env),
            "embedding": _describe_embedding(env),
            "avatar": _describe_avatar(env),
            # 语音识别不是独立能力：由数字人 Provider 内置（fetch_answer），
            # 项目里刻意不新增 ASR / TTS Provider（既有守卫会拒绝这种文件）。
            "speech": {
                "provider": "avatar-internal",
                "note": "ASR 复用数字人 Provider 的 fetch_answer，不单独抽象",
            },
        },
        "metrics": snapshot(),
    }
    return result


def _describe_embedding(env: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    try:
        from services.embedding_provider import describe_embedding  # noqa: PLC0415 - 延迟导入避免装配环

        return dict(describe_embedding(env))
    except Exception as exc:  # noqa: BLE001 - 状态上报永不抛异常
        return {"error": "%s: %s" % (type(exc).__name__, exc)}


def _describe_avatar(env: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    try:
        from services.avatar_provider_iflytek import (  # noqa: PLC0415 - 延迟导入避免装配环
            describe_avatar_provider,
        )

        return dict(describe_avatar_provider(env))
    except Exception as exc:  # noqa: BLE001 - 状态上报永不抛异常
        return {"error": "%s: %s" % (type(exc).__name__, exc)}


def validate_startup(env: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """**启动期**统一校验：模式合法 + 生产模式凭据齐全 + 策略参数可解析。

    返回可打日志的摘要（**不含凭据值**）。任一硬规则不满足 ⇒ 抛
    :class:`AiModeError` / :class:`AiConfigError`，让进程**起不来**，
    而不是等到第一个用户请求才发现配置错了。
    """
    summary = validate_ai_config(env)
    policy = CallPolicy.from_env(env)  # 非法值直接抛 AiConfigError
    provider = build_llm_provider(env)
    summary.update(
        {
            "provider": provider.name,
            "policy": policy.as_dict(),
            "metrics_keys": sorted(METRICS.snapshot().keys()),
        }
    )
    return summary
