# -*- coding: utf-8 -*-
"""讯飞虚拟人 Avatar Provider —— **HTTP 协议实现（非官方 SDK）** + 配置 + 工厂。

为什么不是 SDK
--------------
讯飞虚拟人官方明确「**不适用于浏览器环境**」（不支持跨域），必须后端代理；
且其接入面就是一组 HTTP 接口 + HMAC-SHA256 鉴权。本项目**不引入官方 SDK**
（避免把厂商 SDK 及其传递依赖拉进后端），直接按协议发 HTTP。

★ 端点与响应字段均为**占位**
------------------------------
本模块按「讯飞系通用协议」实现：**HMAC-SHA256 签名（host / date / request-line
三段）+ 三段式报文（header / parameter / payload）**——与既有 ``main.SparkAPI``
（X1 文本模型）和 ``embedding_provider_spark`` 同族。但**虚拟人的具体路径、
``parameter`` 键名、``payload`` 字段名必须与官方文档核对后修正**，故全部收敛到
本模块顶部的常量与 ``*_PATHS`` 表，改一处即可。详见
``docs/数字人Provider接入说明.md``。

鉴权（鉴权能力 = ``build_auth_query``）
---------------------------------------
::

    sig_str = "host: {host}\ndate: {date}\nPOST {path} HTTP/1.1"
    signature = base64(hmac_sha256(api_secret, sig_str))
    authorization = base64('api_key="...", algorithm="hmac-sha256", '
                           'headers="host date request-line", signature="..."')
    url = f"https://{host}{path}?authorization=...&date=...&host=..."

注意与 ``main._make_url`` 的唯一差别：**请求行是 ``POST`` 而不是 ``GET``**。
（因此不能直接复用 ``main._make_url``——它把方法硬编码成 GET。本模块自带一份，
并用固定 ``date`` 做可复现的签名单测。）

零依赖约定
----------
本模块**顶层只有标准库 + ``services.avatar_provider``**；``httpx`` 在
``HttpxAvatarTransport.post_json`` 的**函数体内**才 import ⇒ 未装 httpx 也能
import 整个后端，只是真调讯飞时会抛出明确的「请先安装 httpx」。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Mapping, Optional, Tuple
from urllib.parse import urlencode

from services.avatar_provider import (
    ANSWER_EMPTY,
    ANSWER_PENDING,
    ANSWER_READY,
    ANSWER_STATES,
    SESSION_CREATED,
    SESSION_INITIALIZED,
    SESSION_SPEAKING,
    SESSION_STATES,
    AvatarAnswer,
    AvatarProvider,
    AvatarProviderConfigError,
    AvatarProviderResponseError,
    AvatarProviderSessionExpiredError,
    AvatarProviderUnavailableError,
    AvatarSession,
    AvatarSpeech,
    AvatarStatus,
    MockAvatarProvider,
)

__all__ = [
    "PROVIDER_NAME",
    "PROVIDER_MOCK",
    "PROVIDER_IFLYTEK",
    "KNOWN_PROVIDERS",
    "DEFAULT_HOST",
    "DEFAULT_BASE_PATH",
    "DEFAULT_TIMEOUT",
    "ENV_PROVIDER",
    "ENV_APP_ID",
    "ENV_API_KEY",
    "ENV_API_SECRET",
    "ENV_HOST",
    "ENV_BASE_PATH",
    "ENV_AVATAR_ID",
    "ENV_VCN",
    "ENV_TIMEOUT",
    "SESSION_EXPIRED_CODES",
    "AvatarProviderConfig",
    "load_avatar_config",
    "build_auth_query",
    "build_signed_url",
    "RETRYABLE_STATUS_CODES",
    "sanitize_endpoint",
    "redact_url_in",
    "retry_after_note",
    "AvatarTransport",
    "HttpxAvatarTransport",
    "IflytekAvatarProvider",
    "build_avatar_provider",
    "resolve_avatar_provider",
    "describe_avatar_provider",
]

PROVIDER_NAME = "iflytek"
PROVIDER_MOCK = "mock"
PROVIDER_IFLYTEK = "iflytek"
KNOWN_PROVIDERS = (PROVIDER_MOCK, PROVIDER_IFLYTEK)

#: ★ 占位：以讯飞控制台「AI 虚拟人」实际域名为准
DEFAULT_HOST = "avatar.cn-huabei-1.xf-yun.com"
#: ★ 占位：以官方文档实际路径前缀为准
DEFAULT_BASE_PATH = "/v1/avatar"
DEFAULT_TIMEOUT = 15.0

ENV_PROVIDER = "AVATAR_PROVIDER"
ENV_APP_ID = "AVATAR_IFLYTEK_APP_ID"
ENV_API_KEY = "AVATAR_IFLYTEK_API_KEY"
ENV_API_SECRET = "AVATAR_IFLYTEK_API_SECRET"
ENV_HOST = "AVATAR_IFLYTEK_HOST"
ENV_BASE_PATH = "AVATAR_IFLYTEK_BASE_PATH"
ENV_AVATAR_ID = "AVATAR_IFLYTEK_AVATAR_ID"
ENV_VCN = "AVATAR_IFLYTEK_VCN"
ENV_TIMEOUT = "AVATAR_TIMEOUT"

#: ★ 占位：六个能力的子路径（相对 ``base_path``）
CREATE_PATH = "/session"
INIT_PATH = "/session/{sid}/init"
SPEAK_PATH = "/session/{sid}/speak"
ANSWER_PATH = "/session/{sid}/answer"
STATUS_PATH = "/session/{sid}/status"
STOP_PATH = "/session/{sid}/stop"

#: ★ 占位：从响应里「第一个取到的非空字符串」当作结果（容忍字段名差异）
SESSION_ID_PATHS = (
    "payload.session_id",
    "payload.session.id",
    "payload.avatar_session_id",
    "header.sid",
)
STREAM_URL_PATHS = ("payload.stream_url", "payload.stream.url", "payload.video_url")

#: ★ 占位：哪些 ``header.code`` 代表「**会话不存在 / 已过期**」。
#:
#: **默认为空** ⇒ 所有非 0 code 一律归为 ``AvatarProviderResponseError``，
#: 与引入本常量之前**逐字节一致**（「不配 = 默认 = 行为不变」）。
#: 核对官方文档后把错误码填进来，``_unwrap`` 就会把它们升级成
#: :class:`AvatarProviderSessionExpiredError`，上层据此提示「重建会话」而不是「重试」。
SESSION_EXPIRED_CODES: Tuple[str, ...] = ()


# ============================================================
# 一、配置（全走环境变量；一个都不配 ⇒ 不启用本 provider）
# ============================================================
def _read_str(source: Mapping[str, Any], key: str, default: str = "") -> str:
    """读字符串；**空串等价于未配置**（本项目统一口径）。"""
    raw = source.get(key)
    if raw is None:
        return default
    text = str(raw).strip()
    return text or default


@dataclass
class AvatarProviderConfig:
    """讯飞虚拟人配置。``api_key`` / ``api_secret`` 声明 ``repr=False``（不进日志）。"""

    app_id: str = ""
    api_key: str = field(default="", repr=False)
    api_secret: str = field(default="", repr=False)
    host: str = DEFAULT_HOST
    base_path: str = DEFAULT_BASE_PATH
    avatar_id: str = ""
    vcn: str = ""
    timeout: float = DEFAULT_TIMEOUT

    @property
    def masked_api_key(self) -> str:
        if not self.api_key:
            return ""
        if len(self.api_key) <= 4:
            return "*" * len(self.api_key)
        return "*" * (len(self.api_key) - 4) + self.api_key[-4:]

    @property
    def configured(self) -> bool:
        return bool(self.app_id and self.api_key and self.api_secret)

    def validate(self) -> None:
        """构造期校验：缺什么就报什么，不把配置问题伪装成「连不上」。"""
        missing = [
            name
            for name, value in (
                (ENV_APP_ID, self.app_id),
                (ENV_API_KEY, self.api_key),
                (ENV_API_SECRET, self.api_secret),
            )
            if not value
        ]
        if missing:
            raise AvatarProviderConfigError(
                "讯飞虚拟人配置缺失：" + " / ".join(missing)
                + f"（需先在讯飞控制台开通「AI 虚拟人」，把凭据写入 backend/.env）"
            )
        if not self.host:
            raise AvatarProviderConfigError(f"{ENV_HOST} 不能为空")
        if not self.base_path.startswith("/"):
            raise AvatarProviderConfigError(f"{ENV_BASE_PATH} 必须以 / 开头：{self.base_path!r}")
        if not isinstance(self.timeout, (int, float)) or self.timeout <= 0:
            raise AvatarProviderConfigError(f"{ENV_TIMEOUT} 必须是正数：{self.timeout!r}")


def load_avatar_config(env: Optional[Mapping[str, Any]] = None) -> AvatarProviderConfig:
    """从环境变量读配置（**只读、不校验**——校验交给 ``validate``）。"""
    source: Mapping[str, Any] = os.environ if env is None else env
    timeout_raw = _read_str(source, ENV_TIMEOUT, "")
    timeout: float = DEFAULT_TIMEOUT
    if timeout_raw:
        try:
            timeout = float(timeout_raw)
        except (TypeError, ValueError) as exc:
            raise AvatarProviderConfigError(
                f"{ENV_TIMEOUT} 必须是数字，收到 {timeout_raw!r}"
            ) from exc
    return AvatarProviderConfig(
        app_id=_read_str(source, ENV_APP_ID, ""),
        api_key=_read_str(source, ENV_API_KEY, ""),
        api_secret=_read_str(source, ENV_API_SECRET, ""),
        host=_read_str(source, ENV_HOST, DEFAULT_HOST),
        base_path=_read_str(source, ENV_BASE_PATH, DEFAULT_BASE_PATH),
        avatar_id=_read_str(source, ENV_AVATAR_ID, ""),
        vcn=_read_str(source, ENV_VCN, ""),
        timeout=timeout,
    )


# ============================================================
# 二、鉴权（HMAC-SHA256，POST 请求行）
# ============================================================
def build_auth_query(
    host: str,
    path: str,
    api_key: str,
    api_secret: str,
    *,
    method: str = "POST",
    date: Optional[str] = None,
) -> Dict[str, str]:
    """生成讯飞鉴权 query（``authorization`` / ``date`` / ``host``）。

    ``date`` 可显式传入 ⇒ **签名可复现**（单测用固定 date 手算比对）。
    """
    date_str = date or datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    sig_str = f"host: {host}\ndate: {date_str}\n{method.upper()} {path} HTTP/1.1"
    signature = hmac.new(api_secret.encode("utf-8"), sig_str.encode("utf-8"), hashlib.sha256).digest()
    sig_b64 = base64.b64encode(signature).decode()
    auth_origin = (
        f'api_key="{api_key}", algorithm="hmac-sha256", '
        f'headers="host date request-line", signature="{sig_b64}"'
    )
    auth = base64.b64encode(auth_origin.encode("utf-8")).decode()
    return {"authorization": auth, "date": date_str, "host": host}


def build_signed_url(
    host: str, path: str, api_key: str, api_secret: str, *, date: Optional[str] = None
) -> str:
    """拼出带鉴权 query 的 HTTPS URL。"""
    query = build_auth_query(host, path, api_key, api_secret, date=date)
    return f"https://{host}{path}?{urlencode(query)}"


#: 这两类 HTTP 状态**可重试**（不是「业务拒绝」）：
#: ``408`` 请求超时、``429`` 限流。归到 :class:`AvatarProviderUnavailableError` 才会被通道层
#: 判为 ``unavailable``（见 ``avatar_interview.RETRYABLE_STATES``）；
#: 若归到 ``ResponseError`` 会被当成 ``failed``——而「等一会儿再来」与「这条路走不通」
#: 是两种完全不同的处置（前者前端该给重试按钮，后者该改配置）。
RETRYABLE_STATUS_CODES: Tuple[int, ...] = (408, 429)


def sanitize_endpoint(url: str) -> str:
    """把带鉴权 query 的 URL 收敛成「可以安全写进日志/事件/界面」的形式。

    ★ 为什么必须有：讯飞鉴权把 ``authorization``（HMAC 签名的 base64）放在 **URL query** 里
    （见 :func:`build_auth_query`），而 ``httpx`` 的网络类异常消息里**会内嵌完整 URL**。
    若直接 ``f"…失败：{exc}"``，签名就顺着错误消息进了日志、进了 ``avatar`` 事件的
    ``error`` 字段、乃至进了前端界面——这是**凭据泄漏**，不是「日志有点吵」。

    处置：**只保留 scheme + host + path，丢弃整个 query**（query 里只有鉴权参数，
    没有任何排障需要的信息；path 足以定位是哪个能力失败）。
    """
    head, _, _ = str(url or "").partition("?")
    return head


def redact_url_in(text: Any, url: str) -> str:
    """把一段文本里出现的 ``url`` 换成 :func:`sanitize_endpoint` 的版本。

    ``httpx`` 的异常把请求 URL 原样带在消息里（``str(exc)``），因此**不能**只脱敏
    我们自己拼的字符串——必须把消息里嵌的那份也换掉。
    """
    raw = str(url or "")
    if not raw:
        return str(text)
    return str(text).replace(raw, sanitize_endpoint(raw))


def retry_after_note(resp: Any) -> str:
    """把上游的 ``Retry-After`` 头**原样转述**进消息（不解析、不猜测单位）。

    为什么原样：上游可能给秒数也可能给 HTTP 日期（RFC 7231 两种都合法），
    我们无从判断，硬解析反而会把「上游建议 30 秒」说成「上游建议 0 秒」。
    """
    try:
        value = resp.headers.get("Retry-After")
    except Exception:  # noqa: BLE001 - 读头失败不该影响错误分类
        return ""
    return f"（上游建议 {value} 后重试）" if value else ""


# ============================================================
# 三、传输层（抽象出来 ⇒ 单测注入假 transport 即可完全离线）
# ============================================================
class AvatarTransport(ABC):
    """只做一件事：POST JSON 并返回解析后的 dict。"""

    @abstractmethod
    async def post_json(
        self,
        url: str,
        *,
        body: Mapping[str, Any],
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        ...


class HttpxAvatarTransport(AvatarTransport):
    """默认实现。**httpx 在函数体内 import** ⇒ 未装也不影响 import 后端。

    ★ 本类**所有**错误消息都经过 :func:`sanitize_endpoint` / :func:`redact_url_in`：
    鉴权签名在 URL query 里，而 ``httpx`` 的异常会内嵌完整 URL —— 不处理就等于
    把签名写进日志与前端可见的 ``error`` 字段。
    """

    async def post_json(
        self,
        url: str,
        *,
        body: Mapping[str, Any],
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        safe = sanitize_endpoint(url)
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover - 依赖缺失路径
            raise AvatarProviderUnavailableError(
                "未安装 httpx，无法调用讯飞虚拟人（pip install httpx）"
            ) from exc
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(url, json=dict(body), headers=dict(headers or {}))
        except Exception as exc:  # noqa: BLE001 - 网络类异常统一归为「上游不可用」
            # ★ 消息里**只留 endpoint**（丢 query）+ 把异常自带的 URL 也换掉
            raise AvatarProviderUnavailableError(
                f"调用讯飞虚拟人失败（{safe}）：{redact_url_in(exc, url)}"
            ) from exc
        if resp.status_code >= 500:
            raise AvatarProviderUnavailableError(
                f"讯飞虚拟人返回 {resp.status_code}（服务端错误，可重试）"
            )
        if resp.status_code in RETRYABLE_STATUS_CODES:
            # 408 / 429：**可重试**，不是业务拒绝 —— 分类错了前端就会以为「这条路走不通」
            raise AvatarProviderUnavailableError(
                f"讯飞虚拟人返回 {resp.status_code}（限流或超时，可重试）"
                f"{retry_after_note(resp)}"
            )
        if resp.status_code >= 400:
            raise AvatarProviderResponseError(
                f"讯飞虚拟人返回 {resp.status_code}（{safe}）：{resp.text[:200]}"
            )
        try:
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            raise AvatarProviderResponseError(f"讯飞虚拟人响应不是合法 JSON：{exc}") from exc


# ============================================================
# 四、响应解析辅助（★ 字段名待与官方文档核对）
# ============================================================
def _dig(data: Any, path: str, default: Any = None) -> Any:
    current = data
    for key in path.split("."):
        if isinstance(current, Mapping) and key in current:
            current = current[key]
        else:
            return default
    return current


def _dig_str(data: Any, paths: Any, default: str = "") -> str:
    for path in paths:
        value = _dig(data, path)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return default


def _dig_bool(data: Any, paths: Any, default: bool = False) -> bool:
    for path in paths:
        value = _dig(data, path)
        if isinstance(value, bool):
            return value
    return default


def _unwrap(
    data: Any, action: str, *, expired_codes: Tuple[str, ...] = ()
) -> Dict[str, Any]:
    """校验三段式响应：``header.code != 0`` ⇒ 业务错误。

    ``expired_codes`` 里的 code 会被**升级**为
    :class:`AvatarProviderSessionExpiredError`（「会话失效，需重建」而非「重试」）。
    默认空 ⇒ 所有非 0 code 都是普通 :class:`AvatarProviderResponseError`，
    与引入该参数之前行为一致。
    """
    if not isinstance(data, Mapping):
        raise AvatarProviderResponseError(
            f"[{action}] 响应不是对象：{type(data).__name__}"
        )
    code = _dig(data, "header.code", 0)
    if code not in (0, "0", None):
        message = _dig_str(data, ("header.message", "header.msg"), "")
        if str(code) in {str(c) for c in expired_codes}:
            raise AvatarProviderSessionExpiredError(
                f"[{action}] 数字人会话已失效 code={code}：{message}"
            )
        raise AvatarProviderResponseError(f"[{action}] 上游返回 code={code}：{message}")
    return dict(data)


# ============================================================
# 五、讯飞 Provider
# ============================================================
class IflytekAvatarProvider(AvatarProvider):
    """讯飞虚拟人实现（HTTP）。``transport`` 可注入 ⇒ 单测零联网。"""

    name = PROVIDER_NAME

    def __init__(
        self,
        config: Optional[AvatarProviderConfig] = None,
        *,
        transport: Optional[AvatarTransport] = None,
        env: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.config = config if config is not None else load_avatar_config(env)
        self.config.validate()
        self._transport = transport if transport is not None else HttpxAvatarTransport()

    # ---- 内部：发一次请求 ----
    async def _call(
        self,
        action: str,
        *,
        path: str,
        parameter: Optional[Mapping[str, Any]] = None,
        payload: Optional[Mapping[str, Any]] = None,
        uid: Any = None,
    ) -> Dict[str, Any]:
        url = build_signed_url(
            self.config.host, path, self.config.api_key, self.config.api_secret
        )
        body = {
            "header": {"app_id": self.config.app_id, "uid": str(uid if uid is not None else "career-ai")},
            "parameter": {action: dict(parameter or {})},
            "payload": dict(payload or {}),
        }
        data = await self._transport.post_json(
            url,
            body=body,
            headers={"Content-Type": "application/json"},
            timeout=self.config.timeout,
        )
        return _unwrap(data, action, expired_codes=SESSION_EXPIRED_CODES)

    def _path(self, template: str, sid: str = "") -> str:
        return f"{self.config.base_path}{template.format(sid=sid)}"

    # ---- 接口实现 ----
    async def create_session(
        self, *, session_ref: Any, avatar_id: str = "", vcn: str = ""
    ) -> AvatarSession:
        data = await self._call(
            "session_create",
            path=self._path(CREATE_PATH),
            parameter={
                "avatar_id": avatar_id or self.config.avatar_id,
                "vcn": vcn or self.config.vcn,
                "session_ref": str(session_ref if session_ref is not None else ""),
            },
            payload={},
            uid=session_ref,
        )
        sid = _dig_str(data, SESSION_ID_PATHS, "")
        if not sid:
            raise AvatarProviderResponseError(
                "[session_create] 响应里找不到会话 id（待核对字段名："
                + " / ".join(SESSION_ID_PATHS) + "）"
            )
        return AvatarSession(
            avatar_session_id=sid,
            provider=self.name,
            status=SESSION_CREATED,
            avatar_id=avatar_id or self.config.avatar_id,
            vcn=vcn or self.config.vcn,
            raw=dict(data),
        )

    async def initialize(self, avatar_session_id: str, *, stream: bool = True) -> AvatarSession:
        data = await self._call(
            "session_init",
            path=self._path(INIT_PATH, avatar_session_id),
            parameter={"stream": bool(stream)},
            payload={"session_id": avatar_session_id},
            uid=avatar_session_id,
        )
        return AvatarSession(
            avatar_session_id=avatar_session_id,
            provider=self.name,
            status=SESSION_INITIALIZED,
            stream_url=_dig_str(data, STREAM_URL_PATHS, ""),
            avatar_id=self.config.avatar_id,
            vcn=self.config.vcn,
            raw=dict(data),
        )

    async def speak(
        self, avatar_session_id: str, text: str, *, interrupt: bool = False
    ) -> AvatarSpeech:
        data = await self._call(
            "text_drive",
            path=self._path(SPEAK_PATH, avatar_session_id),
            parameter={"interrupt": bool(interrupt)},
            payload={"session_id": avatar_session_id, "text": text},
            uid=avatar_session_id,
        )
        duration = _dig(data, "payload.duration_ms", 0)
        return AvatarSpeech(
            avatar_session_id=avatar_session_id,
            text=text,
            status=SESSION_SPEAKING,
            stream_url=_dig_str(data, STREAM_URL_PATHS, ""),
            duration_ms=int(duration) if isinstance(duration, (int, float)) else 0,
            raw=dict(data),
        )

    async def fetch_answer(self, avatar_session_id: str, *, timeout_ms: int = 0) -> AvatarAnswer:
        data = await self._call(
            "answer_fetch",
            path=self._path(ANSWER_PATH, avatar_session_id),
            parameter={"timeout_ms": int(timeout_ms)},
            payload={"session_id": avatar_session_id},
            uid=avatar_session_id,
        )
        text = _dig_str(data, ("payload.text", "payload.answer.text", "payload.asr.text"), "")
        audio_url = _dig_str(data, ("payload.audio_url", "payload.audio.url"), "")
        declared = _dig_str(data, ("payload.status",), "")
        if declared in ANSWER_STATES:
            status = declared
        elif text:
            status = ANSWER_READY
        else:
            status = ANSWER_EMPTY
        return AvatarAnswer(
            avatar_session_id=avatar_session_id,
            status=status,
            text=text,
            audio_url=audio_url,
            raw=dict(data),
        )

    async def status(self, avatar_session_id: str) -> AvatarStatus:
        data = await self._call(
            "status_query",
            path=self._path(STATUS_PATH, avatar_session_id),
            parameter={},
            payload={"session_id": avatar_session_id},
            uid=avatar_session_id,
        )
        declared = _dig_str(data, ("payload.status", "payload.session.status"), "")
        status = declared if declared in SESSION_STATES else SESSION_INITIALIZED
        return AvatarStatus(
            avatar_session_id=avatar_session_id,
            status=status,
            initialized=_dig_bool(
                data,
                ("payload.initialized", "payload.session.initialized"),
                default=status in (SESSION_INITIALIZED, SESSION_SPEAKING),
            ),
            speaking=_dig_bool(
                data,
                ("payload.speaking", "payload.session.speaking"),
                default=status == SESSION_SPEAKING,
            ),
            raw=dict(data),
        )

    async def stop(self, avatar_session_id: str) -> AvatarStatus:
        data = await self._call(
            "session_stop",
            path=self._path(STOP_PATH, avatar_session_id),
            parameter={},
            payload={"session_id": avatar_session_id},
            uid=avatar_session_id,
        )
        return AvatarStatus(
            avatar_session_id=avatar_session_id,
            status="stopped",
            initialized=False,
            speaking=False,
            raw=dict(data),
        )


# ============================================================
# 六、工厂（唯一「按配置选 provider」的地方）
# ============================================================
def build_avatar_provider(
    name: Optional[str] = None,
    *,
    env: Optional[Mapping[str, Any]] = None,
    transport: Optional[AvatarTransport] = None,
    config: Optional[AvatarProviderConfig] = None,
) -> AvatarProvider:
    """按 ``AVATAR_PROVIDER`` 构造 provider。

    - 未配置 / 空串 / ``mock`` → :class:`MockAvatarProvider`（**离线、确定性**，
      「不配 = 默认 = 行为不变」，与 ``EMBEDDING_PROVIDER`` 的默认口径一致）；
    - ``iflytek`` → :class:`IflytekAvatarProvider`（**显式 opt-in**，需凭据）；
    - 其它取值 → :class:`AvatarProviderConfigError`（**报错不静默**）。
    """
    source: Mapping[str, Any] = os.environ if env is None else env
    chosen = (name if name is not None else _read_str(source, ENV_PROVIDER, PROVIDER_MOCK)).strip().lower()
    if chosen in ("", PROVIDER_MOCK):
        return MockAvatarProvider()
    if chosen == PROVIDER_IFLYTEK:
        return IflytekAvatarProvider(config, transport=transport, env=source)
    raise AvatarProviderConfigError(
        f"{ENV_PROVIDER} 取值非法：{chosen!r}，允许 {' / '.join(KNOWN_PROVIDERS)}"
    )


def resolve_avatar_provider(
    env: Optional[Mapping[str, Any]] = None,
) -> AvatarProvider:
    """``build_avatar_provider`` 的语义别名（供上层按「解析」语义调用）。"""
    return build_avatar_provider(env=env)


def describe_avatar_provider(env: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """纯读配置、**不构造 provider、不联网、绝不抛异常**（供日志 / 健康检查）。

    返回：``{provider, known, configured, host, base_path, masked_api_key, error}``。
    配置非法时把原因塞进 ``error``，而不是让调用方 500。
    """
    source: Mapping[str, Any] = os.environ if env is None else env
    out: Dict[str, Any] = {
        "provider": _read_str(source, ENV_PROVIDER, PROVIDER_MOCK).lower() or PROVIDER_MOCK,
        "known": list(KNOWN_PROVIDERS),
        "configured": False,
        "host": "",
        "base_path": "",
        "masked_api_key": "",
        "error": None,
    }
    try:
        chosen = out["provider"]
        if chosen == PROVIDER_IFLYTEK:
            cfg = load_avatar_config(source)
            out["host"] = cfg.host
            out["base_path"] = cfg.base_path
            out["masked_api_key"] = cfg.masked_api_key
            out["configured"] = cfg.configured
            if not cfg.configured:
                out["error"] = "讯飞虚拟人凭据不全（需 APP_ID / API_KEY / API_SECRET）"
        elif chosen == PROVIDER_MOCK:
            out["configured"] = True
        else:
            out["error"] = f"未知 provider：{chosen!r}"
    except Exception as exc:  # noqa: BLE001 - 状态上报永不抛异常
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out
