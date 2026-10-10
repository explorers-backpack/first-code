# -*- coding: utf-8 -*-
"""数字人 Avatar Provider —— **抽象接口 + 值对象 + Mock 实现**（零第三方依赖）。

分层位置
--------
::

    services/avatar_interview.py          ← 交互通道层（按 interview_mode 分流）
        └── services/avatar_provider.py   ← 【本模块】Provider 抽象 + 值对象 + Mock
              └── services/avatar_provider_iflytek.py  ← 真实讯飞实现（HTTP，非 SDK）

本模块**不认识任何厂商**：只规定「数字人 Provider 该有哪些能力、返回什么形状」。
换厂商 = 新写一个 `AvatarProvider` 子类，`avatar_interview` 一行不用改。

六个能力（与需求逐条对应）
--------------------------
=========================  ==================================================
``create_session``         创建数字人 Session
``initialize``             初始化数字人（拿 stream_url）
``speak``                  播放文本（文本驱动）
``fetch_answer``           获取用户语音回答（ASR 结果）
``stop``                   停止 Session
``status``                 状态查询
=========================  ==================================================

为什么「零第三方依赖」
----------------------
与 ``embedding_service`` / ``vector_store`` 同源：真实实现必然要发 HTTP，
塞进接口模块会当场破坏「接口层可离线单测」。因此真实实现另起
``avatar_provider_iflytek``，并把 ``httpx`` **延迟到函数体内 import**。

错误分类（四档，处理方式完全不同）
----------------------------------
- :class:`AvatarProviderConfigError`（+``ValueError``）：**配置**问题（缺 AppId /
  非法 timeout）。构造期就报——不把「部署配错了」伪装成「数字人连不上」。
- :class:`AvatarProviderUnavailableError`：**上游不可用**（网络 / 超时 / 5xx）。
  可重试，也可降级为「无数字人」。
- :class:`AvatarProviderSessionExpiredError`：**会话失效**（上游说这场数字人会话
  不存在 / 已过期）。**重试无用**，必须重新 ``create_session``。
- :class:`AvatarProviderResponseError`：上游**业务错误**（``header.code != 0``）
  或响应结构不可解析。

降级约定（**重要**）
--------------------
``avatar_interview.enter`` 捕获上述**全部**异常并降级为「纯文本交付」——
数字人故障**绝不允许阻断面试**。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Optional, Sequence

__all__ = [
    # 错误
    "AvatarProviderError",
    "AvatarProviderConfigError",
    "AvatarProviderUnavailableError",
    "AvatarProviderSessionExpiredError",
    "AvatarProviderResponseError",
    "AvatarValueError",
    # 状态常量
    "SESSION_CREATED",
    "SESSION_INITIALIZED",
    "SESSION_SPEAKING",
    "SESSION_STOPPED",
    "SESSION_FAILED",
    "SESSION_STATES",
    "ANSWER_PENDING",
    "ANSWER_READY",
    "ANSWER_EMPTY",
    "ANSWER_STATES",
    # 值对象
    "AvatarSession",
    "AvatarSpeech",
    "AvatarAnswer",
    "AvatarStatus",
    "SESSION_FIELDS",
    "SPEECH_FIELDS",
    "ANSWER_FIELDS",
    "STATUS_FIELDS",
    # 接口与 Mock
    "AvatarProvider",
    "MockAvatarProvider",
]


# ============================================================
# 一、错误分类
# ============================================================
class AvatarProviderError(RuntimeError):
    """数字人 Provider 错误基类。"""


class AvatarProviderConfigError(AvatarProviderError, ValueError):
    """配置问题（缺凭据 / 非法取值）——构造期就报。"""


class AvatarProviderUnavailableError(AvatarProviderError):
    """上游不可用（网络 / 超时 / 5xx）——可重试或降级。"""


class AvatarProviderResponseError(AvatarProviderError):
    """上游业务错误或响应不可解析。"""


class AvatarProviderSessionExpiredError(AvatarProviderResponseError):
    """**数字人会话已失效**（上游会话不存在 / 已过期）。

    单独成类的原因：它与「网络抖一下」的处置**完全不同**——

    - :class:`AvatarProviderUnavailableError`：**原地重试**可能就好了；
    - 本类：重试多少次都一样，**必须重新 ``create_session``**（换一场会话）。

    因此上层（``avatar_interview`` / ``interview_orchestrator``）据它给出
    「重建会话」而不是「重试」的提示。

    继承 ``AvatarProviderResponseError``：它是「上游明确回了业务错误」的一个特例，
    既有只捕获 ResponseError 的调用方**行为不变**（不破坏向后兼容）。
    """


class AvatarValueError(AvatarProviderError, ValueError):
    """值对象构造 / 解析错误。"""


# ============================================================
# 二、状态常量
# ============================================================
SESSION_CREATED = "created"
SESSION_INITIALIZED = "initialized"
SESSION_SPEAKING = "speaking"
SESSION_STOPPED = "stopped"
SESSION_FAILED = "failed"
SESSION_STATES = (
    SESSION_CREATED,
    SESSION_INITIALIZED,
    SESSION_SPEAKING,
    SESSION_STOPPED,
    SESSION_FAILED,
)

ANSWER_PENDING = "pending"
ANSWER_READY = "ready"
ANSWER_EMPTY = "empty"
ANSWER_STATES = (ANSWER_PENDING, ANSWER_READY, ANSWER_EMPTY)


# ============================================================
# 三、值对象（frozen dataclass + to_dict / from_dict，契约恒定）
# ============================================================
def _require_str(value: Any, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise AvatarValueError(f"{name} 必须是非空字符串，收到 {value!r}")


def _require_dict(value: Any, name: str) -> None:
    if not isinstance(value, dict):
        raise AvatarValueError(f"{name} 必须是 dict，收到 {type(value).__name__}")


class _ValueObject:
    """统一的 ``to_dict`` / ``from_dict``（未知键忽略、缺失取默认、类型不符抛错）。"""

    FIELDS: Sequence[str] = ()

    def to_dict(self) -> Dict[str, Any]:  # pragma: no cover - 由子类实现
        raise NotImplementedError

    @classmethod
    def from_dict(cls, data: Any):
        if not isinstance(data, dict):
            raise AvatarValueError(
                f"{cls.__name__}.from_dict 需要 dict，收到 {type(data).__name__}"
            )
        kwargs: Dict[str, Any] = {}
        for name in cls.FIELDS:  # type: ignore[attr-defined]
            if name in data and data[name] is not None:
                kwargs[name] = data[name]
        try:
            return cls(**kwargs)  # type: ignore[call-arg]
        except TypeError as exc:
            raise AvatarValueError(f"{cls.__name__} 字段不符：{exc}") from exc


SESSION_FIELDS: Sequence[str] = (
    "avatar_session_id", "provider", "status", "stream_url", "avatar_id", "vcn", "raw",
)
SPEECH_FIELDS: Sequence[str] = (
    "avatar_session_id", "text", "status", "stream_url", "duration_ms", "raw",
)
ANSWER_FIELDS: Sequence[str] = (
    "avatar_session_id", "status", "text", "audio_url", "raw",
)
STATUS_FIELDS: Sequence[str] = (
    "avatar_session_id", "status", "initialized", "speaking", "raw",
)


@dataclass(frozen=True)
class AvatarSession(_ValueObject):
    """一次数字人会话。``avatar_session_id`` 是**上游**的会话标识。"""

    avatar_session_id: str
    provider: str = ""
    status: str = SESSION_CREATED
    stream_url: str = ""
    avatar_id: str = ""
    vcn: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    FIELDS = SESSION_FIELDS

    def __post_init__(self) -> None:
        _require_str(self.avatar_session_id, "avatar_session_id")
        _require_str(self.status, "status")
        _require_dict(self.raw, "raw")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "avatar_session_id": self.avatar_session_id,
            "provider": self.provider,
            "status": self.status,
            "stream_url": self.stream_url,
            "avatar_id": self.avatar_id,
            "vcn": self.vcn,
            "raw": dict(self.raw),
        }


@dataclass(frozen=True)
class AvatarSpeech(_ValueObject):
    """一次文本驱动（播放文本）的结果。"""

    avatar_session_id: str
    text: str
    status: str = SESSION_SPEAKING
    stream_url: str = ""
    duration_ms: int = 0
    raw: Dict[str, Any] = field(default_factory=dict)

    FIELDS = SPEECH_FIELDS

    def __post_init__(self) -> None:
        _require_str(self.avatar_session_id, "avatar_session_id")
        if not isinstance(self.text, str):
            raise AvatarValueError("text 必须是字符串")
        if not isinstance(self.duration_ms, int) or self.duration_ms < 0:
            raise AvatarValueError("duration_ms 必须是 >= 0 的整数")
        _require_dict(self.raw, "raw")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "avatar_session_id": self.avatar_session_id,
            "text": self.text,
            "status": self.status,
            "stream_url": self.stream_url,
            "duration_ms": self.duration_ms,
            "raw": dict(self.raw),
        }


@dataclass(frozen=True)
class AvatarAnswer(_ValueObject):
    """用户语音回答（ASR 结果）。

    ``status`` 三态：``ready``（拿到文本）/ ``pending``（用户还没说完）/
    ``empty``（这一轮没有语音）。
    """

    avatar_session_id: str
    status: str = ANSWER_PENDING
    text: str = ""
    audio_url: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    FIELDS = ANSWER_FIELDS

    def __post_init__(self) -> None:
        _require_str(self.avatar_session_id, "avatar_session_id")
        if not isinstance(self.text, str):
            raise AvatarValueError("text 必须是字符串")
        _require_dict(self.raw, "raw")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "avatar_session_id": self.avatar_session_id,
            "status": self.status,
            "text": self.text,
            "audio_url": self.audio_url,
            "raw": dict(self.raw),
        }


@dataclass(frozen=True)
class AvatarStatus(_ValueObject):
    """状态查询结果。"""

    avatar_session_id: str
    status: str = SESSION_CREATED
    initialized: bool = False
    speaking: bool = False
    raw: Dict[str, Any] = field(default_factory=dict)

    FIELDS = STATUS_FIELDS

    def __post_init__(self) -> None:
        _require_str(self.avatar_session_id, "avatar_session_id")
        _require_str(self.status, "status")
        _require_dict(self.raw, "raw")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "avatar_session_id": self.avatar_session_id,
            "status": self.status,
            "initialized": bool(self.initialized),
            "speaking": bool(self.speaking),
            "raw": dict(self.raw),
        }


# ============================================================
# 四、抽象接口
# ============================================================
class AvatarProvider(ABC):
    """数字人 Provider 接口。

    实现约定
    --------
    - **``create_session`` 必须对同一 ``session_ref`` 幂等**：上层在
      ``start`` / ``answer`` / ``end`` 都会调用它（不额外持久化会话 id），
      重复调用应返回同一场会话，而不是每次新建。
    - 六个方法**全部异步**（真实实现必然访问外部服务）。
    - 失败一律抛 :class:`AvatarProviderError` 子类；**不要返回 ``None``**。
    """

    #: provider 名（写进 ``AvatarSession.provider``，也用于状态上报）
    name: str = ""

    @abstractmethod
    async def create_session(
        self, *, session_ref: Any, avatar_id: str = "", vcn: str = ""
    ) -> AvatarSession:
        """创建（或按 ``session_ref`` 复用）一场数字人会话。"""

    @abstractmethod
    async def initialize(self, avatar_session_id: str, *, stream: bool = True) -> AvatarSession:
        """初始化数字人；``stream=True`` 时返回可播放的 ``stream_url``。"""

    @abstractmethod
    async def speak(
        self, avatar_session_id: str, text: str, *, interrupt: bool = False
    ) -> AvatarSpeech:
        """文本驱动：让数字人播报 ``text``。"""

    @abstractmethod
    async def fetch_answer(self, avatar_session_id: str, *, timeout_ms: int = 0) -> AvatarAnswer:
        """获取用户语音回答（ASR 结果）。"""

    @abstractmethod
    async def status(self, avatar_session_id: str) -> AvatarStatus:
        """状态查询。"""

    @abstractmethod
    async def stop(self, avatar_session_id: str) -> AvatarStatus:
        """停止（关闭）会话。"""


# ============================================================
# 五、Mock 实现（无 SDK 环境测试用：离线、确定性、可注入失败）
# ============================================================
class MockAvatarProvider(AvatarProvider):
    """内存实现：不联网、无随机、无时间依赖，用于单测与本地打通链路。

    - ``calls`` 记录每一次调用（``[{"action": ..., ...}]``），供断言「谁被按什么顺序调用」。
    - ``fail_on`` 注入失败：``MockAvatarProvider(fail_on=["speak"])`` 会让 ``speak``
      抛 :class:`AvatarProviderUnavailableError`，用来验证**降级**路径。
    - ``answer_text`` 模拟 ASR 拿到的文本（空串 ⇒ ``fetch_answer`` 返回 ``empty``）。
    """

    name = "mock"

    def __init__(
        self,
        *,
        answer_text: str = "",
        fail_on: Sequence[str] = (),
        stream_url: str = "mock://stream",
        audio_url_template: str = "mock://audio/{sid}",
    ) -> None:
        self._sessions: Dict[str, AvatarSession] = {}
        self._refs: Dict[str, str] = {}  # session_ref(str) -> avatar_session_id
        self._seq = 0
        self.calls: List[Dict[str, Any]] = []
        self.fail_on = set(fail_on)
        self.answer_text = answer_text
        self.stream_url = stream_url
        self.audio_url_template = audio_url_template

    # ---- 内部 ----
    def _record(self, action: str, **kw: Any) -> None:
        self.calls.append({"action": action, **kw})

    def _maybe_fail(self, action: str) -> None:
        if action in self.fail_on:
            raise AvatarProviderUnavailableError(f"[mock] 注入的失败：{action}")

    def _get(self, avatar_session_id: str) -> AvatarSession:
        session = self._sessions.get(avatar_session_id)
        if session is None:
            # 会话不在内存里 = 「这场数字人会话不存在」——正是「会话失效」的语义。
            # 用专门子类（继承 ResponseError）⇒ 既有只捕获 ResponseError 的调用方不受影响。
            raise AvatarProviderSessionExpiredError(f"[mock] 会话不存在：{avatar_session_id}")
        return session

    def _put(self, session: AvatarSession) -> AvatarSession:
        self._sessions[session.avatar_session_id] = session
        return session

    # ---- 只读辅助（供上层 / 测试） ----
    def session_of(self, session_ref: Any) -> Optional[AvatarSession]:
        """按上层会话 id 取数字人会话（未创建则 ``None``）。"""
        sid = self._refs.get(str(session_ref))
        return self._sessions.get(sid) if sid else None

    def actions(self) -> List[str]:
        return [c["action"] for c in self.calls]

    def reset(self) -> None:
        self._sessions.clear()
        self._refs.clear()
        self._seq = 0
        self.calls.clear()

    # ---- 接口实现 ----
    async def create_session(
        self, *, session_ref: Any, avatar_id: str = "", vcn: str = ""
    ) -> AvatarSession:
        self._record("create_session", session_ref=session_ref, avatar_id=avatar_id)
        self._maybe_fail("create_session")
        key = str(session_ref)
        existing = self._refs.get(key)
        if existing is not None:  # 幂等：同一 session_ref 复用
            return self._sessions[existing]
        self._seq += 1
        sid = f"mock-{self._seq:04d}-{key}"
        session = AvatarSession(
            avatar_session_id=sid,
            provider=self.name,
            status=SESSION_CREATED,
            avatar_id=avatar_id,
            vcn=vcn,
            raw={"session_ref": key},
        )
        self._refs[key] = sid
        return self._put(session)

    async def initialize(self, avatar_session_id: str, *, stream: bool = True) -> AvatarSession:
        self._record("initialize", avatar_session_id=avatar_session_id, stream=stream)
        self._maybe_fail("initialize")
        session = self._get(avatar_session_id)
        return self._put(
            replace(
                session,
                status=SESSION_INITIALIZED,
                stream_url=self.stream_url if stream else "",
            )
        )

    async def speak(
        self, avatar_session_id: str, text: str, *, interrupt: bool = False
    ) -> AvatarSpeech:
        self._record("speak", avatar_session_id=avatar_session_id, text=text)
        self._maybe_fail("speak")
        session = self._get(avatar_session_id)
        self._put(replace(session, status=SESSION_SPEAKING))
        return AvatarSpeech(
            avatar_session_id=avatar_session_id,
            text=text,
            status=SESSION_SPEAKING,
            stream_url=session.stream_url,
            # 60ms / 字：确定性，便于断言；真实实现由上游给 duration
            duration_ms=len(text) * 60,
            raw={"interrupt": interrupt},
        )

    async def fetch_answer(self, avatar_session_id: str, *, timeout_ms: int = 0) -> AvatarAnswer:
        self._record("fetch_answer", avatar_session_id=avatar_session_id, timeout_ms=timeout_ms)
        self._maybe_fail("fetch_answer")
        self._get(avatar_session_id)
        text = self.answer_text
        status = ANSWER_READY if text else ANSWER_EMPTY
        return AvatarAnswer(
            avatar_session_id=avatar_session_id,
            status=status,
            text=text,
            audio_url=self.audio_url_template.format(sid=avatar_session_id) if text else "",
            raw={"timeout_ms": timeout_ms},
        )

    async def status(self, avatar_session_id: str) -> AvatarStatus:
        self._record("status", avatar_session_id=avatar_session_id)
        self._maybe_fail("status")
        session = self._get(avatar_session_id)
        return AvatarStatus(
            avatar_session_id=avatar_session_id,
            status=session.status,
            initialized=session.status in (SESSION_INITIALIZED, SESSION_SPEAKING),
            speaking=session.status == SESSION_SPEAKING,
            raw=dict(session.raw),
        )

    async def stop(self, avatar_session_id: str) -> AvatarStatus:
        self._record("stop", avatar_session_id=avatar_session_id)
        self._maybe_fail("stop")
        session = self._get(avatar_session_id)
        self._put(replace(session, status=SESSION_STOPPED))
        return AvatarStatus(
            avatar_session_id=avatar_session_id,
            status=SESSION_STOPPED,
            initialized=False,
            speaking=False,
            raw=dict(session.raw),
        )
