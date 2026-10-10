# -*- coding: utf-8 -*-
"""数字人视频面试通道 —— **交互通道层**（``interview_mode == "avatar"`` 的入口）。

定位
----
``interview_mode`` 只决定「**怎么交付**」，不决定「**面什么**」。因此：

::

    InterviewService（会话流程 / 数据访问 / 前端契约）
        ├── 面试逻辑：出题 · 评分 · 报告 · 状态机   ← 两种模式**完全共享**（在 interview_core）
        └── 交付边界（按 interview_mode 分流）
              ├── text   → 原样返回（前端用输入框/文本渲染）      ← 既有流程，一行未改
              └── avatar → 【本模块】enter()                    ← 数字人通道入口

**本模块不参与出题 / 评分 / 报告**，只负责「把 Service 组装好的交付内容交给数字人通道」。

与 Provider 的关系
------------------
::

    avatar_interview.enter(db, session, payload)     ← 本模块：**阶段判别 + 降级**
        └── services/avatar_provider.AvatarProvider  ← 接口（Mock / 讯飞实现）
              └── services/avatar_provider_iflytek   ← 真实讯飞实现（HTTP）

本模块**不认识讯飞**：只按接口调用 ``create_session`` / ``initialize`` / ``speak`` /
``fetch_answer`` / ``stop`` / ``status``。换厂商 = 换 provider，本模块一行不用改。

阶段判别（为什么靠 payload 形状）
---------------------------------
``enter`` 的签名被刻意固定为 ``(db, session, payload)``（见下），没有 ``stage`` 参数。
而四种交互入口的交付内容**键集互不相同**，因此用「标志键」判别阶段即可，无需改签名：

===================  ==========================  ====================================
阶段                 标志键                       驱动动作
===================  ==========================  ====================================
``start``            ``job_name``                create → initialize → speak(第一题)
``current_question`` ``session_id`` + ``status``  create → initialize → speak(当前题)
``answer``           ``answer``                  … → speak(下一题) → fetch_answer(ASR)
``end``              ``report``                  … → stop()
===================  ==========================  ====================================

语音回答（ASR）与 Avatar 预检
------------------------------
``enter`` 是**交付边界**（Service 调、返回交付内容），它刻意不往外传任何东西。
但「数字人面试闭环」还需要另外两件事，它们是**显式能力**、不是交付：

======================================  ==================================================
:func:`ensure_ready`                    初始化 / 预检数字人会话（拿 ``avatar_session_id``
                                        与 ``stream_url``）——失败即「**Avatar 启动失败**」
:func:`capture_answer`                  取用户语音回答（**ASR**）的文本
                                        ——失败即「**ASR 失败**」
======================================  ==================================================

两者都返回**恒定键集的信封**（成功 / 失败同形状，调用方无需分支判断键存在性）::

    ensure_ready   → {ok, status, avatar_session_id, stream_url, error, error_type, retryable}
    capture_answer → {ok, status, text, audio_url, avatar_session_id, error, error_type, retryable}

``status`` 取值（``CHANNEL_*``）与处置::

    ok            成功
    empty         这一轮没听到语音（ASR 结果为空）—— retryable=True（让用户「再说一次」）
    unavailable   上游不可用（网络 / 超时 / 5xx）—— retryable=True（原地重试）
    expired       数字人会话已失效 —— retryable=**False**（重试无用，需**重建会话**）
    unsupported   该会话不适用本通道（如 ``interview_mode=text`` 的会话）—— retryable=False
    disabled      provider 未启用 / 配置非法 —— retryable=False（属部署问题）
    failed        其它（响应不可解析 / 值对象非法）—— retryable=False

**不新增 ASR / TTS Provider**：语音识别就是 ``provider.fetch_answer``、文本驱动就是
``provider.speak``——都是**讯飞虚拟人的内置能力**，本模块只做「分类 + 封装」，
不引入任何第二套语音服务。

降级（**核心原则**）
--------------------
``enter`` 捕获**一切**异常（provider 解析失败 / 网络失败 / 上游业务错误）并
**原样返回 ``payload``** —— 数字人故障**绝不允许阻断面试**，也不改变任何交付内容。
因此本模块的返回值与入参**始终是同一个对象**（text 与 avatar 两种模式在 provider
不可用时交付内容完全一致）。

``ensure_ready`` / ``capture_answer`` 同样**不抛异常**（除入参类型错误外），
但它们**如实上报**失败：调用方据 ``status`` 决定「重试 / 重建会话 / 降级为文字作答」。
——「静默吞掉」与「不阻断」是两回事：前者会让故障无从察觉。

API 契约**刻意不变**
--------------------
``enter`` 不往 ``payload`` 里塞任何新字段。原因：``payload`` 最终要经
``schemas/interview.py`` 的响应模型序列化，给共享模型加可选字段会让**text 响应也
多出 ``null`` 键**，等于改了默认路径的契约。把数字人信息暴露给前端应当是**独立的、
显式的**一步（如新增 ``/api/interview/{id}/avatar`` 端点），不在本次范围内。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from services.avatar_provider import (
    ANSWER_READY,
    SESSION_CREATED,
    SESSION_STOPPED,
    AvatarProvider,
    AvatarProviderConfigError,
    AvatarProviderError,
    AvatarProviderSessionExpiredError,
    AvatarProviderUnavailableError,
)

# ============================================================
# 通道元信息（供上层与测试判断「接没接」）
# ============================================================
CHANNEL = "avatar"

#: ``pending`` = 尚未接入；``ready`` = 通道已实现（**当前**）
STATUS_PENDING = "pending"
STATUS_READY = "ready"
STATUS = STATUS_READY

#: 外部能力开关。provider 层已实现这四项能力 ⇒ 全部为 True。
#: ``asr`` / ``tts`` 都是**讯飞虚拟人内置能力**（分别走 ``provider.fetch_answer`` /
#: ``provider.speak``），**不另起独立的 ASR / TTS Provider**。
#: （**注意**：True 表示「通道已具备该能力」，真实可用性取决于 provider 配置——
#:  默认 ``AVATAR_PROVIDER`` 未配置时用 Mock，不产生任何真实音视频。）
INTEGRATIONS: Dict[str, bool] = {
    "iflytek": True,  # 讯飞虚拟人（provider 已实现，需配置凭据启用）
    "asr": True,      # 语音识别（provider.fetch_answer，虚拟人内置）
    "tts": True,      # 语音合成 / 文本驱动（provider.speak，虚拟人内置）
}

# ============================================================
# 阶段常量
# ============================================================
STAGE_START = "start"
STAGE_CURRENT = "current_question"
STAGE_ANSWER = "answer"
STAGE_END = "end"
STAGE_UNKNOWN = "unknown"

# ============================================================
# 通道结果状态（``ensure_ready`` / ``capture_answer`` 的 ``status`` 字段）
# ============================================================
#: 成功。
CHANNEL_OK = "ok"
#: 这一轮没有语音（ASR 结果为空）——可让用户「再说一次」。
CHANNEL_EMPTY = "empty"
#: 上游不可用（网络 / 超时 / 5xx）——可原地重试。
CHANNEL_UNAVAILABLE = "unavailable"
#: 数字人会话已失效——**重试无用**，需重建会话。
CHANNEL_EXPIRED = "expired"
#: 该会话**不适用本通道**（如 ``interview_mode=text`` 的会话被误调 ASR）。
CHANNEL_UNSUPPORTED = "unsupported"
#: provider 未启用 / 配置非法——属部署问题，不该重试。
CHANNEL_DISABLED = "disabled"
#: 其它（响应不可解析 / 值对象非法 / 未知异常）。
CHANNEL_FAILED = "failed"

#: 全部合法 ``status``（便于测试穷举与前端映射）。
CHANNEL_STATES: tuple = (
    CHANNEL_OK,
    CHANNEL_EMPTY,
    CHANNEL_UNAVAILABLE,
    CHANNEL_EXPIRED,
    CHANNEL_UNSUPPORTED,
    CHANNEL_DISABLED,
    CHANNEL_FAILED,
)

#: 可重试的 ``status``（调用方据此决定「原地重试」还是「换一条路」）。
RETRYABLE_STATES: tuple = (CHANNEL_EMPTY, CHANNEL_UNAVAILABLE)

#: ``capture_answer`` 信封的**恒定键集**（成功与失败同形状）。
CAPTURE_FIELDS: tuple = (
    "ok", "status", "text", "audio_url", "avatar_session_id",
    "error", "error_type", "retryable",
)
#: ``ensure_ready`` 信封的**恒定键集**（成功与失败同形状）。
READY_FIELDS: tuple = (
    "ok", "status", "avatar_session_id", "stream_url",
    "error", "error_type", "retryable",
)

#: ``capture_answer`` 的默认 ASR 等待时长（``0`` = 交给 provider 决定）。
DEFAULT_ANSWER_TIMEOUT_MS = 0

_log = logging.getLogger(__name__)

# 进程内缓存的 provider（Mock 需要跨调用保持会话状态，故不能每次新建）
_PROVIDER: Optional[AvatarProvider] = None
_PROVIDER_RESOLVED = False


def _build_provider() -> AvatarProvider:
    """延迟导入具体实现（顶层只留接口，避免把厂商细节拉进通道层）。"""
    from services.avatar_provider_iflytek import build_avatar_provider

    return build_avatar_provider()


def get_provider() -> AvatarProvider:
    """取进程内 provider（首次按环境变量解析并缓存）。"""
    global _PROVIDER, _PROVIDER_RESOLVED
    if not _PROVIDER_RESOLVED:
        _PROVIDER = _build_provider()
        _PROVIDER_RESOLVED = True
    return _PROVIDER


def reset_provider(provider: Optional[AvatarProvider] = None) -> Optional[AvatarProvider]:
    """清空 / 替换缓存（**测试用**）。``reset_provider()`` ⇒ 下次重新按环境解析。"""
    global _PROVIDER, _PROVIDER_RESOLVED
    _PROVIDER = provider
    _PROVIDER_RESOLVED = provider is not None
    return _PROVIDER


# ============================================================
# 阶段判别与文本提取（纯函数，可单独单测）
# ============================================================
def classify_payload(payload: Any) -> str:
    """按交付内容的**标志键**判别阶段（键集见模块文档）。"""
    if not isinstance(payload, dict):
        return STAGE_UNKNOWN
    if "report" in payload:
        return STAGE_END
    if "answer" in payload:
        return STAGE_ANSWER
    if "job_name" in payload:
        return STAGE_START
    if "session_id" in payload and "status" in payload:
        return STAGE_CURRENT
    return STAGE_UNKNOWN


def _question_text(payload: Dict[str, Any], key: str) -> str:
    question = payload.get(key)
    if isinstance(question, dict):
        text = question.get("question")
        if isinstance(text, str):
            return text
    return ""


def spoken_text(stage: str, payload: Dict[str, Any]) -> str:
    """本次应当「播报」的文本（无则空串 ⇒ 不调用 ``speak``）。"""
    if stage in (STAGE_START, STAGE_CURRENT):
        return _question_text(payload, "question")
    if stage == STAGE_ANSWER:
        return _question_text(payload, "next_question")
    return ""


# ============================================================
# 驱动（把「阶段」翻译成 provider 调用序列）
# ============================================================
async def _ensure_ready(provider: AvatarProvider, session: Any):
    """确保数字人会话已创建并初始化（``create_session`` 对同一 ref 幂等）。"""
    avatar = await provider.create_session(session_ref=getattr(session, "id", None))
    if avatar.status == SESSION_CREATED:
        avatar = await provider.initialize(avatar.avatar_session_id)
    return avatar


async def drive(
    provider: AvatarProvider, session: Any, payload: Dict[str, Any]
) -> Optional[str]:
    """按阶段驱动 provider，返回本次用到的 ``avatar_session_id``。

    未知阶段（如 ``create_session`` 的交付内容）**不做任何事**并返回 ``None``。
    """
    stage = classify_payload(payload)
    if stage == STAGE_UNKNOWN:
        return None

    avatar = await _ensure_ready(provider, session)
    avatar_session_id = avatar.avatar_session_id

    if stage == STAGE_END:
        await provider.stop(avatar_session_id)
        return avatar_session_id

    text = spoken_text(stage, payload)
    if text:
        await provider.speak(avatar_session_id, text)

    if stage == STAGE_ANSWER:
        # 取用户语音回答（ASR）。当前只取回、不外传，供后续接入「语音作答」。
        await provider.fetch_answer(avatar_session_id)

    return avatar_session_id


# ============================================================
# 显式能力：Avatar 预检 / ASR 取文本（**恒定键集信封，永不抛异常**）
# ============================================================
def classify_error(exc: BaseException) -> str:
    """把 provider 异常**分类**成 ``CHANNEL_*`` 之一（纯函数，可单测）。

    分类顺序即优先级（``expired`` 必须排在 ``unavailable`` / ``failed`` 之前，
    因为它是 ``AvatarProviderResponseError`` 的子类）::

        SessionExpiredError  → expired      （重建会话，重试无用）
        UnavailableError     → unavailable  （网络 / 超时，可重试）
        ConfigError          → disabled     （部署问题，别重试）
        AvatarProviderError  → failed       （上游业务错误 / 响应不可解析）
        其它                  → failed
    """
    if isinstance(exc, AvatarProviderSessionExpiredError):
        return CHANNEL_EXPIRED
    if isinstance(exc, AvatarProviderUnavailableError):
        return CHANNEL_UNAVAILABLE
    if isinstance(exc, AvatarProviderConfigError):
        return CHANNEL_DISABLED
    if isinstance(exc, AvatarProviderError):
        return CHANNEL_FAILED
    return CHANNEL_FAILED


def _error_fields(exc: BaseException) -> Dict[str, Any]:
    """异常 → 信封里那三个字段（``status`` / ``error`` / ``error_type`` / ``retryable``）。"""
    status = classify_error(exc)
    return {
        "status": status,
        "error": f"{type(exc).__name__}: {exc}",
        "error_type": type(exc).__name__,
        "retryable": status in RETRYABLE_STATES,
    }


def _capture_envelope(**overrides: Any) -> Dict[str, Any]:
    """``capture_answer`` 信封骨架（键集恒定，缺失一律取中性默认）。"""
    base: Dict[str, Any] = {
        "ok": False, "status": CHANNEL_FAILED, "text": "", "audio_url": "",
        "avatar_session_id": "", "error": None, "error_type": None, "retryable": False,
    }
    base.update(overrides)
    return base


def _ready_envelope(**overrides: Any) -> Dict[str, Any]:
    """``ensure_ready`` 信封骨架（键集恒定）。"""
    base: Dict[str, Any] = {
        "ok": False, "status": CHANNEL_FAILED, "avatar_session_id": "", "stream_url": "",
        "error": None, "error_type": None, "retryable": False,
    }
    base.update(overrides)
    return base


def _wrong_mode(session: Any) -> Optional[str]:
    """会话是否**不适用本通道**。

    返回 ``None`` = 适用（或无法判断，**不拦**）；返回字符串 = 实际的 ``interview_mode``。

    判据用 ``CHANNEL``（``"avatar"``）本身，**不 import ``models``**——通道层刻意
    不依赖模型层，避免「通道 → 模型」这条本不必要的边。
    """
    mode = getattr(session, "interview_mode", None)
    if isinstance(mode, str) and mode and mode != CHANNEL:
        return mode
    return None


async def ensure_ready(db: AsyncSession, session: Any) -> Dict[str, Any]:
    """**初始化 / 预检数字人会话**（幂等）——即「Avatar 启动」。

    与 ``drive`` 内部用的 ``_ensure_ready`` 是同一条链路（``create_session`` 对同一
    ``session_ref`` 幂等 + 未初始化则 ``initialize``），差别只在**失败时如实上报**：

    - 成功 → ``ok=True`` / ``status="ok"`` / ``avatar_session_id`` / ``stream_url``；
    - **Avatar 启动失败** → ``ok=False``，``status`` 区分
      ``unavailable``（网络）/ ``expired``（会话失效）/ ``disabled``（配置错）/ ``failed``。

    本函数**不抛 provider 异常**——调用方据 ``status`` 决策（重试 / 重建 / 降级）。
    """
    mode = _wrong_mode(session)
    if mode is not None:
        return _ready_envelope(
            status=CHANNEL_UNSUPPORTED,
            error=f"非数字人会话（interview_mode={mode}），本通道不适用",
        )

    try:
        provider = get_provider()
    except Exception as exc:  # noqa: BLE001 - 配置错误也要能上报，不能把面试打断
        _log.warning("数字人 provider 解析失败：%s", exc)
        return _ready_envelope(**_error_fields(exc))

    try:
        avatar = await _ensure_ready(provider, session)
    except Exception as exc:  # noqa: BLE001 - 上游故障分类上报
        _log.warning("数字人会话初始化失败：%s", exc)
        return _ready_envelope(**_error_fields(exc))

    return _ready_envelope(
        ok=True,
        status=CHANNEL_OK,
        avatar_session_id=avatar.avatar_session_id,
        stream_url=avatar.stream_url,
    )


async def capture_answer(
    db: AsyncSession, session: Any, *, timeout_ms: int = DEFAULT_ANSWER_TIMEOUT_MS
) -> Dict[str, Any]:
    """**取用户语音回答的文本**（讯飞虚拟人 **内置 ASR**，``provider.fetch_answer``）。

    闭环里它接在「用户说完话」之后：拿到 ``text`` 就交给
    ``interview_service.submit_answer`` 评分。**不新增独立 ASR Provider**。

    返回恒定 8 键信封（见 ``CAPTURE_FIELDS``）::

        {"ok", "status", "text", "audio_url", "avatar_session_id",
         "error", "error_type", "retryable"}

    - ``ok=True``  → ``status="ok"``，``text`` 非空（**可直接提交**）；
    - **ASR 失败** → ``ok=False``：``empty``（没听到，可重试）/
      ``unavailable``（网络异常，可重试）/ ``expired``（会话失效，需重建）/
      ``disabled`` / ``failed``。

    ★ 语义红线：``status="empty"`` **不等于**「用户没回答」——它只表示**这一轮
    没识别到语音**。调用方必须让用户确认或重试，**不得**据此提交空答案
    （服务层也会以 400 拒绝空答案）。
    """
    mode = _wrong_mode(session)
    if mode is not None:
        return _capture_envelope(
            status=CHANNEL_UNSUPPORTED,
            error=f"非数字人会话（interview_mode={mode}），无语音回答可取",
        )

    try:
        provider = get_provider()
    except Exception as exc:  # noqa: BLE001
        _log.warning("数字人 provider 解析失败，无法取语音回答：%s", exc)
        return _capture_envelope(**_error_fields(exc))

    try:
        avatar = await _ensure_ready(provider, session)
        answer = await provider.fetch_answer(avatar.avatar_session_id, timeout_ms=timeout_ms)
    except Exception as exc:  # noqa: BLE001
        _log.warning("取语音回答（ASR）失败：%s", exc)
        return _capture_envelope(**_error_fields(exc))

    text = answer.text if isinstance(answer.text, str) else ""
    audio_url = answer.audio_url if isinstance(answer.audio_url, str) else ""
    if answer.status == ANSWER_READY and text.strip():
        return _capture_envelope(
            ok=True,
            status=CHANNEL_OK,
            text=text,
            audio_url=audio_url,
            avatar_session_id=answer.avatar_session_id,
        )
    return _capture_envelope(
        status=CHANNEL_EMPTY,
        text=text,
        audio_url=audio_url,
        avatar_session_id=answer.avatar_session_id,
        error="未识别到语音回答（ASR 结果为空）——可重试或改用文字作答",
        retryable=True,
    )


# ============================================================
# 入口（唯一接线点）
# ============================================================
async def enter(
    db: AsyncSession,
    session: Any,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """数字人通道入口 —— 驱动 provider，**失败一律降级为原样交付**。

    调用链::

        interview_service.start_session / get_current_question
                          / submit_answer / end_session
            └── avatar_interview.enter(db, session, payload)   ← 本函数
                    └── AvatarProvider.*                        ← Mock / 讯飞

    参数
    ----
    - ``db``：会话用的 ``AsyncSession``。当前 provider 不需要它（预留：将来把
      ``avatar_session_id`` 落库时用）。
    - ``session``：``InterviewSession`` 行（读 ``id`` 作为 provider 的 ``session_ref``）。
    - ``payload``：Service 已组装好的交付内容。

    返回值
    ------
    **与入参 ``payload`` 同一个对象**——本函数不新增 / 不改写任何字段，
    因此 text 与 avatar 两种模式的交付内容与 API 契约**逐字节一致**。
    """
    try:
        provider = get_provider()
    except Exception as exc:  # noqa: BLE001 - 配置错误也必须降级，不能阻断面试
        _log.warning("数字人 provider 解析失败，已降级为纯文本交付：%s", exc)
        return payload

    try:
        await drive(provider, session, payload)
    except Exception as exc:  # noqa: BLE001 - 上游故障一律降级
        _log.warning("数字人通道执行失败，已降级为纯文本交付：%s", exc)
    return payload


async def query_status(avatar_session_id: str) -> Optional[Dict[str, Any]]:
    """状态查询（只读）。provider 不可用时返回 ``None``，**不抛异常**。"""
    if not avatar_session_id:
        return None
    try:
        status = await get_provider().status(avatar_session_id)
    except Exception as exc:  # noqa: BLE001
        _log.warning("数字人状态查询失败：%s", exc)
        return None
    return status.to_dict()


__all__ = [
    "CHANNEL",
    "STATUS",
    "STATUS_PENDING",
    "STATUS_READY",
    "INTEGRATIONS",
    "STAGE_START",
    "STAGE_CURRENT",
    "STAGE_ANSWER",
    "STAGE_END",
    "STAGE_UNKNOWN",
    # ---- 通道结果状态（ASR / 预检信封）----
    "CHANNEL_OK",
    "CHANNEL_EMPTY",
    "CHANNEL_UNAVAILABLE",
    "CHANNEL_EXPIRED",
    "CHANNEL_UNSUPPORTED",
    "CHANNEL_DISABLED",
    "CHANNEL_FAILED",
    "CHANNEL_STATES",
    "RETRYABLE_STATES",
    "CAPTURE_FIELDS",
    "READY_FIELDS",
    "DEFAULT_ANSWER_TIMEOUT_MS",
    "classify_payload",
    "spoken_text",
    "drive",
    "enter",
    # ---- 显式能力：预检 / ASR ----
    "classify_error",
    "ensure_ready",
    "capture_answer",
    "get_provider",
    "reset_provider",
    "query_status",
]
