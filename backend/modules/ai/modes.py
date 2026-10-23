# -*- coding: utf-8 -*-
"""AI 能力接入层 · 运行模式（mock / offline / production）。

三种模式的区别**不是"真假"，而是"允许做什么"**
------------------------------------------------
===========  ==========  ==============  ==============================
模式          允许联网     需要凭据        行为
===========  ==========  ==============  ==============================
``mock``      否          否              返回**确定性假结果**（结构合法、内容固定），
                                         供前端联调 / 演示 / 契约测试使用
``offline``   否          否              任何调用都以 :class:`AiUnavailableError`
                                         失败，用于验证**降级路径**（CI / 离线开发）
``production`` 是         是              调真实供应商；**缺凭据启动即失败**
===========  ==========  ==============  ==============================

``mock`` 与 ``offline`` 都"不联网"，但语义相反：前者**假装成功**（用来跑通 UI），
后者**确定失败**（用来验证降级）。混成一个 ``fake`` 模式就无法分别测试这两条路径。

默认口径：「不配 = 默认 = 行为不变」
------------------------------------
``AI_RUN_MODE`` 未配置时按**自动判定**解析：

* 星火凭据（``SPARK_APP_ID`` / ``SPARK_API_KEY`` / ``SPARK_API_SECRET``）齐全
  ⇒ ``production``（与重构前"有凭据就真调"一致）；
* 凭据不全 ⇒ ``offline``（与重构前"必然失败"一致，但**不再真的去打网络**）。

**自动判定永远不会落到 ``mock``**——"静默返回假数据"是比"失败"严重得多的错误，
必须由人显式指定。
"""

import os
from typing import Any, Dict, Mapping, Optional

from modules.ai.errors import AiModeError

__all__ = [
    "ENV_RUN_MODE",
    "MODE_MOCK",
    "MODE_OFFLINE",
    "MODE_PRODUCTION",
    "RUN_MODES",
    "CREDENTIAL_KEYS",
    "resolve_run_mode",
    "credentials_configured",
    "describe_run_mode",
    "validate_ai_config",
]

#: 运行模式开关。未配置 / 空串 ⇒ 自动判定（见模块文档）。
ENV_RUN_MODE = "AI_RUN_MODE"

MODE_MOCK = "mock"
MODE_OFFLINE = "offline"
MODE_PRODUCTION = "production"

RUN_MODES = (MODE_MOCK, MODE_OFFLINE, MODE_PRODUCTION)

#: 判定"真实凭据是否齐全"的键。**顺序固定**，供报错文案与状态上报使用。
CREDENTIAL_KEYS = ("SPARK_APP_ID", "SPARK_API_KEY", "SPARK_API_SECRET")


def _read(env: Optional[Mapping[str, Any]], key: str) -> str:
    source = os.environ if env is None else env
    value = source.get(key, "")
    return "" if value is None else str(value).strip()


def credentials_configured(env: Optional[Mapping[str, Any]] = None) -> bool:
    """真实 LLM 凭据是否齐全（**只看配置，不联网**）。"""
    return all(_read(env, key) for key in CREDENTIAL_KEYS)


def missing_credentials(env: Optional[Mapping[str, Any]] = None) -> list:
    """缺失的凭据键名列表（供报错文案 / 状态上报，**不返回值本身**）。"""
    return [key for key in CREDENTIAL_KEYS if not _read(env, key)]


def resolve_run_mode(env: Optional[Mapping[str, Any]] = None) -> str:
    """解析运行模式。

    * 显式配置 ``mock`` / ``offline`` / ``production``（大小写不敏感）⇒ 原样返回；
    * 未配置 / 空串 ⇒ 自动判定（凭据齐全 ⇒ production，否则 offline）；
    * **其它取值 ⇒ :class:`AiModeError`**（非法值报错，不静默降级）。
    """
    raw = _read(env, ENV_RUN_MODE)
    if not raw:
        return MODE_PRODUCTION if credentials_configured(env) else MODE_OFFLINE

    chosen = raw.lower()
    if chosen not in RUN_MODES:
        raise AiModeError(
            "%s 取值非法：%r，允许 %s（留空则按凭据自动判定）"
            % (ENV_RUN_MODE, raw, " / ".join(RUN_MODES))
        )
    return chosen


def describe_run_mode(env: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """纯读配置、**不构造 Provider、不联网、绝不抛异常**（供日志 / 健康检查）。"""
    raw = _read(env, ENV_RUN_MODE)
    out: Dict[str, Any] = {
        "mode": "",
        "requested": raw,
        "auto": not raw,
        "known": list(RUN_MODES),
        "credentials_configured": credentials_configured(env),
        "missing_credentials": missing_credentials(env),
        "error": None,
    }
    try:
        out["mode"] = resolve_run_mode(env)
    except Exception as exc:  # noqa: BLE001 - 状态上报永不抛异常
        out["error"] = "%s: %s" % (type(exc).__name__, exc)
        out["mode"] = MODE_OFFLINE
    return out


def validate_ai_config(env: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """**启动期**配置校验。

    只有一条硬规则：``production`` 模式必须凭据齐全，否则抛
    :class:`AiModeError`（**生产不允许静默回退到 Mock / offline**）。
    其它模式一律放行——``mock`` / ``offline`` 本来就不需要凭据。

    返回可直接打日志的摘要（**不含凭据值**）。
    """
    mode = resolve_run_mode(env)
    summary: Dict[str, Any] = {
        "mode": mode,
        "credentials_configured": credentials_configured(env),
        "missing_credentials": missing_credentials(env),
    }
    if mode == MODE_PRODUCTION and not summary["credentials_configured"]:
        raise AiModeError(
            "%s=production 但缺少凭据：%s。"
            "生产环境不会回退到 mock/offline；请补齐凭据，"
            "或显式改回 %s（仅限联调 / 离线环境）"
            % (ENV_RUN_MODE, " / ".join(summary["missing_credentials"]), ENV_RUN_MODE)
        )
    return summary
