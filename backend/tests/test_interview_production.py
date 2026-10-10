# -*- coding: utf-8 -*-
"""AI 模拟面试 · **生产化支撑层**自检（超时 / 清理 / 并发 / 限流 / 日志 / 指标 / 脱敏）。

无需 pytest，直接运行：
    python backend/tests/test_interview_production.py

覆盖
----
[1]  配置：**不配＝默认＝行为不变**；空串等价未配置；非法值**报错不静默**
[2]  脱敏：``mask_secret`` / ``redact_text`` / ``redact_mapping`` / ``RedactingFilter``
     （**在真实日志器上取证**：凭据值不进输出，但末 4 位仍可核对）；
     ★ 讯飞侧**鉴权签名在 URL query 里**，而 ``httpx`` 异常内嵌完整 URL ⇒
     用桩 httpx 真实走一遍 ``HttpxAvatarTransport``，验证「错误消息里没有签名」
     以及「408 / 429 归入可重试、其它 4xx 归入业务拒绝」
[3]  会话清理：TTL 过期、容量上限（淘汰最久未活动）、``forget`` 不触发回调、
     回调异常不阻断其余回调、回调注册幂等
[4]  并发闸门：每用户连接数 / 会话数上限；幂等登记；释放后额度回收
[5]  每会话互斥：非阻塞、拒绝而非排队、重复 ``exit`` 安全
[6]  限流：默认 **0 = 不限制**（行为不变）；显式配置后按最小间隔放行/拒绝
[7]  指标：计数器 / 瞬时量 / 快照形状恒定
[8]  状态上报：``describe_production_status`` 固定 12 键；``GET /api/health`` 回带
     ``metrics``，且既有 6 键（``status``/``service``/``embedding``/``vector``/
     ``threshold``/``avatar``）**向后兼容**
[9]  协议契约**未被破坏**：9 事件 / 6 消息 / 既有 3 个关闭码逐字不变；
     新增 ``4408`` / ``4409`` 且**前端镜像**（跨语言契约必须在源码里逐字对齐）
[10] 边界守卫（AST）：生产化模块**不 import 任何项目内模块**（尤其编排层）⇒
     「编排层消费者收口为唯一闭集」这条既有断言仍然成立；顶层只用标准库
[11] **真实 ASGI / WebSocket 栈上的行为取证**：空闲超时**真的**会以 ``4408`` 关连接；
     连接数超限**真的**会以 ``4409`` 拒绝

★ 为什么 [11] 必须真连：``api/interview_ws.py`` 的既有自检用的是**真栈**，而
「接线对不对」这件事——``asyncio.wait_for`` 有没有真的包住 ``receive_json``、
闸门有没有真的在鉴权之后调用——**只有在真连一次时才会暴露**（本项目已复发多次）。

数据库使用**临时文件** SQLite（``NullPool``，跨事件循环安全），不触碰本机 MySQL。
``AVATAR_*`` 由 ``regression_env`` 钉成空串 ⇒ 数字人走 **Mock**，零联网。
"""

import ast
import asyncio
import io
import logging
import os
import pathlib
import shutil
import sys
import tempfile
import types

_TMP_DIR = tempfile.mkdtemp(prefix="career_ai_prod_test_")
_DB_PATH = pathlib.Path(_TMP_DIR) / "prod_test.db"

os.environ.setdefault("DATABASE_URL", f"sqlite+aiosqlite:///{_DB_PATH.as_posix()}")

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))
sys.path.insert(0, str(_BACKEND / "tests"))

import regression_env  # noqa: E402,F401  （钉住 AVATAR_* ⇒ 数字人走 Mock，不联网）

from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402
from starlette.websockets import WebSocketDisconnect  # noqa: E402

import main  # noqa: E402
import models  # noqa: E402,F401
from api import interview_ws as ws  # noqa: E402
from database import Base, get_db  # noqa: E402
from models import Job, User, UserSession  # noqa: E402
from services import interview_production as prod  # noqa: E402

DB_URL = f"sqlite+aiosqlite:///{_DB_PATH.as_posix()}"
TOKEN = "prod-test-token-0001"

#: 编排层的点号全名——**运行时拼出**，避免「守卫在源码里写出自己要找的 token」
#: 这条本项目已复发多次的坑。
_ORCH_MODULE = "services." + "interview" + "_orchestrator"

_PASSED = 0
_FAILED = 0


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(
        ("  [PASS] " if cond else "  [FAIL] ")
        + name
        + (f"  -> {detail}" if detail and not cond else "")
    )
    return cond


def _all_imports(source: str):
    """全量 import（含函数体内延迟导入）——判「从不 import X」必须走 AST。

    子串匹配会被 docstring / 注释里提到的模块名误伤（本项目已复发多次）。
    """
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _top_level_roots(source: str):
    """顶层 import 的**根模块名**（``from a.b import c`` ⇒ ``a``）。"""
    roots = set()
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            roots.add(node.module.split(".")[0])
    return roots


def _read(rel: str) -> str:
    return (_BACKEND / rel).read_text(encoding="utf-8")


# ============================================================
# 主流程
# ============================================================
def run() -> bool:  # noqa: C901 - 自检脚本，顺序即文档
    print("=" * 68)
    print("AI 模拟面试 · 生产化支撑层自检")
    print("=" * 68)

    # ------------------------------------------------------------
    # [1] 配置
    # ------------------------------------------------------------
    print("\n[1] 配置（不配＝默认＝行为不变）")
    _check("一个环境变量都不配 ⇒ 全部取默认值",
           prod.config_from_env({}) == prod.DEFAULT_CONFIG, str(prod.config_from_env({})))
    _check("空串等价于未配置（不是「配了个空值」）",
           prod.config_from_env({prod.ENV_IDLE_TIMEOUT: "   "}) == prod.DEFAULT_CONFIG)
    _check("显式配置生效（idle_timeout=45）",
           prod.config_from_env({prod.ENV_IDLE_TIMEOUT: "45"}).idle_timeout_seconds == 45.0)
    _check("整数旋钮显式配置生效（max_tracked=7）",
           prod.config_from_env({prod.ENV_MAX_TRACKED: "7"}).max_tracked_sessions == 7)

    raised = False
    try:
        prod.config_from_env({prod.ENV_IDLE_TIMEOUT: "abc"})
    except ValueError:
        raised = True
    _check("★ 非法值**报错不静默**（超时配成 abc ⇒ ValueError）", raised)

    raised = False
    try:
        prod.config_from_env({prod.ENV_MAX_TRACKED: "0"})
    except ValueError:
        raised = True
    _check("★ 上限为 0 ⇒ 报错（0 不是「无上限」，不猜）", raised)

    raised = False
    try:
        prod.config_from_env({prod.ENV_MAX_CONNECTIONS_PER_USER: "-1"})
    except ValueError:
        raised = True
    _check("★ 负数 ⇒ 报错", raised)

    _check("★ 作答限流默认 0（= 不限制）——不配即行为不变",
           prod.DEFAULT_CONFIG.min_answer_interval_seconds == 0.0,
           str(prod.DEFAULT_CONFIG.min_answer_interval_seconds))
    _check("  └ 但 0 是**合法显式值**（区别于非法值）",
           prod.config_from_env({prod.ENV_MIN_ANSWER_INTERVAL: "0"}).min_answer_interval_seconds
           == 0.0)
    _check("  └ 显式开启后生效（0.4）",
           prod.config_from_env({prod.ENV_MIN_ANSWER_INTERVAL: "0.4"}).min_answer_interval_seconds
           == 0.4)
    _check("配置对象可原样上报（to_dict 与字段一一对应）",
           set(prod.DEFAULT_CONFIG.to_dict()) == {
               "idle_timeout_seconds", "state_ttl_seconds", "max_tracked_sessions",
               "max_sessions_per_user", "max_connections_per_user",
               "min_answer_interval_seconds",
           }, str(sorted(prod.DEFAULT_CONFIG.to_dict())))

    # ------------------------------------------------------------
    # [2] 脱敏（凭据绝不进日志）
    # ------------------------------------------------------------
    print("\n[2] 脱敏（API Key 安全管理）")
    _check("mask_secret 保留末 4 位（够核对「是哪一把」，不够还原）",
           prod.mask_secret("abcdef123456") == "********3456", prod.mask_secret("abcdef123456"))
    _check("mask_secret 短值整体掩码（不泄漏长度以外的信息）",
           prod.mask_secret("abc") == "***", prod.mask_secret("abc"))
    _check("mask_secret 空值 → 空串（不产出 'None'）",
           prod.mask_secret("") == "" and prod.mask_secret(None) == "")

    redacted = prod.redact_text("api_key=SUPERSECRET host=x")
    _check("redact_text 把行内 k=v 的值换成掩码",
           "SUPERSECRET" not in redacted and "api_key=" in redacted, redacted)
    _check("  └ 无关文本**逐字不变**（不做过度脱敏）",
           prod.redact_text("session_id=12 state=ok") == "session_id=12 state=ok")
    _check("  └ Authorization 头也认",
           "Bearer" not in prod.redact_text("Authorization: Bearer abcdefghijkl"),
           prod.redact_text("Authorization: Bearer abcdefghijkl"))

    mapping = prod.redact_mapping(
        {"api_key": "K1", "api_secret": "S1", "host": "h", "nested": {"token": "T1"}}
    )
    _check("redact_mapping 脱敏敏感键、保留其余键",
           mapping["host"] == "h" and mapping["api_key"] == prod.mask_secret("K1"),
           str(mapping))
    _check("  └ 递归到嵌套映射", mapping["nested"]["token"] == prod.mask_secret("T1"), str(mapping))
    _check("is_sensitive_key 认 camelCase / kebab-case / 全大写",
           all(prod.is_sensitive_key(k)
               for k in ("apiKey", "api-key", "API_KEY", "Authorization", "access_key")))

    # ★ 在**真实日志器**上取证：脱敏过滤器是不是真的挂在 logger 上、真的生效。
    #   （只断言「函数能脱敏」是不够的——那证明不了它被接进了日志管线。）
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger = prod.get_logger()
    logger.addHandler(handler)
    old_level = logger.level
    logger.setLevel(logging.INFO)
    try:
        logger.info(
            "provider config api_key=%s api_secret=%s",
            "REALSECRET1234",
            "MYSECRET5678",
        )
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)
    out = stream.getvalue()
    _check("★ 真实日志器：两个凭据值都**不进**日志输出",
           "REALSECRET1234" not in out and "MYSECRET5678" not in out, out.strip())
    _check("  └ 但末 4 位仍保留（可核对是哪一把、不可还原）",
           "1234" in out and "5678" in out, out.strip())

    # log_event 的结构化字段同样过脱敏
    stream2 = io.StringIO()
    handler2 = logging.StreamHandler(stream2)
    handler2.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler2)
    logger.setLevel(logging.INFO)
    try:
        prod.log_event("ws_connect", session_id=1, api_key="TOPSECRET9999")
    finally:
        logger.removeHandler(handler2)
        logger.setLevel(old_level)
    _check("log_event 的字段值同样脱敏（不是只有 %s 通道）",
           "TOPSECRET9999" not in stream2.getvalue(), stream2.getvalue().strip())

    # ---- 讯飞侧：鉴权签名在 URL query 里 ⇒ 任何要写出去的地方必须先脱敏 ----
    #   ★ 这是「API Key 安全管理」与「讯飞 SDK 异常处理」的交点：httpx 的网络异常
    #     消息里**内嵌完整 URL**（含 authorization=…），直接 f"…{exc}" 就把签名
    #     写进了日志、写进了 avatar 事件的 error 字段、乃至写进了前端界面。
    from services import avatar_provider_iflytek as xf  # noqa: PLC0415

    signed = xf.build_signed_url("avatar.example.com", "/v1/avatar/session", "APPKEY", "SECRET")
    _check("★ 取证：鉴权签名**确实在 URL query 里**（所以错误消息带 URL 就是泄漏）",
           "authorization=" in signed, signed[:70])
    _check("sanitize_endpoint 丢掉整个 query（只留 scheme+host+path）",
           xf.sanitize_endpoint(signed) == "https://avatar.example.com/v1/avatar/session",
           xf.sanitize_endpoint(signed))
    _check("  └ 脱敏结果里不含 authorization / signature",
           "authorization" not in xf.sanitize_endpoint(signed)
           and "signature" not in xf.sanitize_endpoint(signed))
    leaky = f"ConnectError: [Errno 11001] getaddrinfo failed for url '{signed}'"
    _check("redact_url_in 把异常消息里**内嵌**的完整 URL 也换掉",
           "authorization=" not in xf.redact_url_in(leaky, signed),
           xf.redact_url_in(leaky, signed)[:90])
    _check("  └ 但仍保留可定位的 endpoint（排障信息不丢）",
           "/v1/avatar/session" in xf.redact_url_in(leaky, signed))
    _check("★ 408 / 429 被归入「可重试」（否则前端会以为这条路走不通）",
           xf.RETRYABLE_STATUS_CODES == (408, 429), str(xf.RETRYABLE_STATUS_CODES))

    # ---- 真实走一遍 HttpxAvatarTransport 的分流（用桩 httpx，零联网） ----
    stub_state: dict = {}

    class _StubResponse:
        def __init__(self, status_code, *, headers=None, text="", payload=None):
            self.status_code = status_code
            self.headers = headers or {}
            self.text = text
            self._payload = payload if payload is not None else {}

        def json(self):
            return self._payload

    class _StubAsyncClient:
        def __init__(self, *, timeout=None):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return False

        async def post(self, url, json=None, headers=None):  # noqa: A002 - 对齐 httpx 签名
            stub_state["url"] = url
            outcome = stub_state["outcome"]
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

    def _call_transport(outcome):
        stub = types.ModuleType("httpx")
        stub.AsyncClient = _StubAsyncClient
        saved = sys.modules.get("httpx")
        sys.modules["httpx"] = stub
        stub_state["outcome"] = outcome
        try:
            return asyncio.run(
                xf.HttpxAvatarTransport().post_json(
                    signed,
                    body={"header": {}, "parameter": {}, "payload": {}},
                    headers={"Content-Type": "application/json"},
                    timeout=15.0,
                )
            )
        finally:
            if saved is None:
                sys.modules.pop("httpx", None)
            else:
                sys.modules["httpx"] = saved

    def _call_transport_error(outcome):
        """跑一次并返回异常对象（不抛出去）。"""
        try:
            _call_transport(outcome)
        except Exception as exc:  # noqa: BLE001 - 就是要把异常抓出来看
            return exc
        return None

    net_exc = _call_transport_error(RuntimeError(f"connect failed for {signed}"))
    _check("★ 网络异常 → UnavailableError，且消息里**没有**鉴权签名",
           isinstance(net_exc, xf.AvatarProviderUnavailableError)
           and "authorization=" not in str(net_exc),
           f"{type(net_exc).__name__}: {net_exc}")
    _check("  └ 但保留了 endpoint（能定位到哪个能力失败）",
           "/v1/avatar/session" in str(net_exc), str(net_exc))

    limited = _call_transport_error(_StubResponse(429, headers={"Retry-After": "30"}))
    _check("★ 429 → UnavailableError（可重试），不是 ResponseError",
           isinstance(limited, xf.AvatarProviderUnavailableError)
           and not isinstance(limited, xf.AvatarProviderResponseError),
           f"{type(limited).__name__}: {limited}")
    _check("  └ 原样转述上游 Retry-After（不猜测单位）",
           "30" in str(limited), str(limited))

    server_err = _call_transport_error(_StubResponse(503, text="unavailable"))
    _check("5xx → UnavailableError（既有口径未变）",
           isinstance(server_err, xf.AvatarProviderUnavailableError), str(server_err))

    client_err = _call_transport_error(_StubResponse(404, text="no such action"))
    _check("其它 4xx → ResponseError（业务拒绝，重试无用）",
           isinstance(client_err, xf.AvatarProviderResponseError), str(client_err))

    ok_payload = _call_transport(_StubResponse(200, payload={"header": {"code": 0}}))
    _check("2xx → 原样返回解析后的 JSON", ok_payload == {"header": {"code": 0}}, str(ok_payload))

    # ------------------------------------------------------------
    # [3] 会话清理（TTL + 容量上限）
    # ------------------------------------------------------------
    print("\n[3] 会话清理（TTL / 容量上限 / 清理回调）")
    fake = {"t": 1000.0}
    tracker = prod.SessionTracker(ttl_seconds=60.0, max_tracked=3, clock=lambda: fake["t"])
    evicted = []

    def _collect(sid):
        evicted.append(sid)

    tracker.register_evict_hook(_collect)
    tracker.register_evict_hook(_collect)  # 幂等：重复注册只生效一次
    for sid in (1, 2, 3):
        tracker.touch(sid)
    _check("touch 后跟踪 3 条", tracker.size() == 3, str(tracker.size()))
    _check("未过期 ⇒ expired() 为空（**纯计算，不改状态**）",
           tracker.expired() == [] and tracker.size() == 3)
    _check("last_seen 可读（排障用）", tracker.last_seen(1) == 1000.0)

    fake["t"] = 1061.0
    _check("超过 TTL ⇒ expired() 列出（仍不改状态）",
           tracker.expired() == [1, 2, 3] and tracker.size() == 3, str(tracker.expired()))
    swept = tracker.sweep()
    _check("★ sweep 摘除过期会话并触发清理回调",
           swept == [1, 2, 3] and tracker.size() == 0, str(swept))
    _check("  └ 回调注册**幂等**（重复注册不会把回调调两次）",
           evicted == [1, 2, 3], str(evicted))

    # 容量上限：只保留「最新 N 条」——★ 上限由 **sweep** 执行（清理是机会式的），
    # 因此允许在两次 sweep 之间短暂超限；真正要保证的是「sweep 后收敛」。
    fake2 = {"t": 0.0}
    tracker2 = prod.SessionTracker(ttl_seconds=99999.0, max_tracked=2, clock=lambda: fake2["t"])
    for sid in (1, 2, 3, 4):
        fake2["t"] = float(sid)
        tracker2.touch(sid)
    _check("touch 不负责执行上限（清理是机会式的，不是每次写都扫）",
           tracker2.size() == 4, str(tracker2.size()))
    overflow = tracker2.sweep()
    _check("★ sweep 后收敛到上限（防无界增长）", tracker2.size() == 2, str(tracker2.size()))
    _check("  └ 淘汰的是**最久未活动**的两条（1、2）", overflow == [1, 2], str(overflow))

    # forget：主动断开 ≠ 过期
    fake3 = {"t": 0.0}
    tracker3 = prod.SessionTracker(ttl_seconds=1.0, max_tracked=10, clock=lambda: fake3["t"])
    forgotten = []
    tracker3.register_evict_hook(forgotten.append)
    tracker3.touch(7)
    tracker3.forget(7)
    _check("forget 摘除记录但**不触发**清理回调（主动断开 ≠ 过期）",
           tracker3.size() == 0 and forgotten == [], str(forgotten))

    # 回调异常不阻断其余回调
    fake4 = {"t": 0.0}
    tracker4 = prod.SessionTracker(ttl_seconds=1.0, max_tracked=10, clock=lambda: fake4["t"])
    survived = []

    def _boom(sid):
        raise RuntimeError("boom")

    tracker4.register_evict_hook(_boom)
    tracker4.register_evict_hook(survived.append)
    tracker4.touch(9)
    fake4["t"] = 5.0
    tracker4.sweep()
    _check("★ 单个清理回调抛异常**不阻断**其余回调（清理是尽力而为）",
           survived == [9], str(survived))
    _check("  └ 且该条仍被摘除（不会卡在表里反复失败）", tracker4.size() == 0)

    # ------------------------------------------------------------
    # [4] 并发闸门
    # ------------------------------------------------------------
    print("\n[4] 并发闸门（每用户连接数 / 会话数）")
    gate = prod.ConcurrencyGate(max_connections_per_user=2, max_sessions_per_user=1)
    _check("前 2 条连接放行", gate.acquire_connection(1) and gate.acquire_connection(1))
    _check("★ 第 3 条被拒（**非阻塞、不排队**）", gate.acquire_connection(1) is False)
    _check("  └ 活跃连接数如实统计", gate.active_connections() == 2, str(gate.active_connections()))
    _check("不同用户互不影响", gate.acquire_connection(2) is True)

    gate.release_connection(1)
    _check("释放后额度回收（不会泄漏）",
           gate.connection_count(1) == 1 and gate.acquire_connection(1) is True,
           str(gate.connection_count(1)))
    for _ in range(5):
        gate.release_connection(1)
    _check("重复释放不会把计数压成负数", gate.connection_count(1) == 0, str(gate.connection_count(1)))

    _check("会话额度：第 1 个放行", gate.acquire_session(1, 100) is True)
    _check("  └ 同一会话重复登记**幂等**（重连不被自己的旧记录挤掉）",
           gate.acquire_session(1, 100) is True and gate.session_count(1) == 1,
           str(gate.session_count(1)))
    _check("  └ 第 2 个会话超限被拒", gate.acquire_session(1, 200) is False)
    gate.release_session(1, 100)
    _check("  └ 释放后额度回收", gate.acquire_session(1, 200) is True
           and gate.session_count(1) == 1, str(gate.session_count(1)))
    _check("snapshot 形状恒定",
           set(gate.snapshot()) == {
               "active_connections", "active_sessions",
               "max_connections_per_user", "max_sessions_per_user",
           }, str(sorted(gate.snapshot())))

    # ------------------------------------------------------------
    # [5] 每会话互斥
    # ------------------------------------------------------------
    print("\n[5] 每会话互斥（两个标签页同时作答）")
    mutex = prod.SessionMutex()
    _check("首次进入成功", mutex.try_enter(1) is True)
    _check("★ 同一会话再次进入被拒（**非阻塞**，不排队）",
           mutex.try_enter(1) is False and mutex.busy(1) is True)
    mutex.exit(1)
    _check("退出后可再次进入", mutex.try_enter(1) is True)
    _check("不同会话互不阻塞", mutex.try_enter(2) is True)
    _check("busy_count 如实统计", mutex.busy_count() == 2, str(mutex.busy_count()))
    mutex.exit(1)
    mutex.exit(2)
    mutex.exit(2)
    _check("重复 exit 安全（幂等，不会把计数压成负数）", mutex.busy_count() == 0)

    # ------------------------------------------------------------
    # [6] 限流
    # ------------------------------------------------------------
    print("\n[6] 限流（默认关闭；显式开启后生效）")
    clock = {"t": 0.0}
    limiter = prod.RateLimiter(min_interval_seconds=0.5, clock=lambda: clock["t"])
    _check("首次放行", limiter.allow("k") is True)
    _check("间隔内被拒", limiter.allow("k") is False)
    _check("  └ retry_after 给出剩余等待秒数",
           abs(limiter.retry_after("k") - 0.5) < 1e-6, str(limiter.retry_after("k")))
    clock["t"] = 0.5
    _check("间隔到 ⇒ 放行", limiter.allow("k") is True)
    _check("不同 key 互不影响", limiter.allow("other") is True)
    clock["t"] = 100.0
    _check("sweep 清理久未使用的 key（防 key 空间无界增长）",
           limiter.sweep(max_idle_seconds=10.0) == 2 and limiter.size() == 0,
           str(limiter.size()))

    unlimited = prod.RateLimiter(min_interval_seconds=0.0, clock=lambda: clock["t"])
    _check("★ 间隔 0 ⇒ 永不拦截（默认行为不变）",
           all(unlimited.allow("k") for _ in range(5)))

    # ------------------------------------------------------------
    # [7] 指标
    # ------------------------------------------------------------
    print("\n[7] 指标（计数器 / 瞬时量）")
    metrics = prod.Metrics()
    metrics.incr("a")
    metrics.incr("a", 2)
    metrics.set_gauge(prod.M_WS_ACTIVE, 3)
    snap = metrics.snapshot()
    _check("计数器累加", snap["counters"].get("a") == 3, str(snap["counters"]))
    _check("瞬时量进 gauges（不混进 counters）",
           snap["gauges"].get(prod.M_WS_ACTIVE) == 3 and "a" not in snap["gauges"],
           str(snap["gauges"]))
    _check("快照形状恒定（counters / gauges / uptime_seconds）",
           set(snap) == {"counters", "gauges", "uptime_seconds"}, str(sorted(snap)))
    _check("uptime_seconds 是数字", isinstance(snap["uptime_seconds"], float))
    metrics.reset()
    _check("reset 清零（测试隔离）", metrics.snapshot()["counters"] == {})
    _check("★ 模块级单例存在（健康检查与实时通道看的是同一份）",
           isinstance(prod.METRICS, prod.Metrics))
    _check("指标名是稳定字符串（可被日志/采集端聚合）",
           prod.M_WS_IDLE_CLOSED == "ws_idle_timeouts_total"
           and prod.M_SESSIONS_EVICTED == "sessions_evicted_total")

    # ------------------------------------------------------------
    # [8] 状态上报 + /api/health
    # ------------------------------------------------------------
    print("\n[8] 状态上报（describe_production_status / GET /api/health）")
    status = prod.describe_production_status()
    _check("★ 上报键集**恒定**（12 键）",
           set(status) == set(prod.PRODUCTION_STATUS_FIELDS), str(sorted(status)))
    _check("  └ 未配置 ⇒ error 为 None（配置正常）", status["error"] is None, str(status["error"]))
    _check("  └ 默认值原样上报（与 DEFAULT_CONFIG 一致）",
           status["idle_timeout_seconds"] == prod.DEFAULT_CONFIG.idle_timeout_seconds
           and status["min_answer_interval_seconds"]
           == prod.DEFAULT_CONFIG.min_answer_interval_seconds)
    _check("  └ metrics 子块形状正确",
           set(status["metrics"]) == {"counters", "gauges", "uptime_seconds"},
           str(sorted(status["metrics"])))

    async def _fetch_health():
        transport = ASGITransport(app=main.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get("/api/health")

    health = asyncio.run(_fetch_health())
    body = health.json()
    _check("GET /api/health 返回 200", health.status_code == 200, str(health.status_code))
    _check("★ 新增 metrics 键", isinstance(body.get("metrics"), dict), str(type(body.get("metrics"))))
    # ★ 不能拿两份快照做**全等**：内层 ``metrics.uptime_seconds`` 是
    #   ``time.monotonic()`` 的**实时采样**（``round(..., 3)``），两次调用跨毫秒
    #   边界就必然不等 ⇒ 原写法「两次快照全等」会**偶发假红**
    #   （全量回归时实测复现一次，单独复跑 3 次却全绿——典型的时序偶发）。
    #   改为：把**非确定性的** ``uptime_seconds`` 摘掉后逐键全等（其余 11 键 +
    #   counters / gauges 全部照比——health 若返回缓存/陈旧字典仍会在这里露出来），
    #   uptime 单独断言形状。
    def _stable(snapshot: dict) -> dict:
        stable = dict(snapshot)
        inner = dict(stable.get("metrics") or {})
        inner.pop("uptime_seconds", None)
        stable["metrics"] = inner
        return stable

    _health_metrics = body.get("metrics") or {}
    _status_metrics = main.production_status()
    _check("  └ 与 main.production_status() **同源**（除实时 uptime 外逐键相等）",
           _stable(_health_metrics) == _stable(_status_metrics)
           and isinstance((_health_metrics.get("metrics") or {}).get("uptime_seconds"), float),
           str(_health_metrics))
    _check("  └ metrics 固定 12 键",
           set(body.get("metrics") or {}) == set(prod.PRODUCTION_STATUS_FIELDS),
           str(sorted(body.get("metrics") or {})))
    for legacy in ("status", "service", "embedding", "vector", "threshold", "avatar"):
        _check(f"  └ ★ 旧键 {legacy!r} 仍在（向后兼容）", legacy in body)
    _check("  └ status / service 值**逐字未变**",
           body["status"] == "ok" and body["service"] == "Career.ai FastAPI", str(body["status"]))

    # ------------------------------------------------------------
    # [9] 协议契约未被破坏
    # ------------------------------------------------------------
    print("\n[9] 协议契约（既有事件/消息/关闭码逐字不变 + 新关闭码）")
    _check("服务端事件集仍是 9 个且逐字不变",
           ws.SERVER_EVENTS == ("hello", "state", "avatar", "question", "answer",
                                "score", "report", "error", "pong"),
           str(ws.SERVER_EVENTS))
    _check("客户端消息集仍是 6 个且逐字不变",
           ws.CLIENT_MESSAGES == ("start", "speech_done", "answer", "finish",
                                  "snapshot", "ping"),
           str(ws.CLIENT_MESSAGES))
    _check("★ 既有三个关闭码**值未变**（顺序与取值都不许动）",
           (ws.WS_CLOSE_UNAUTHORIZED, ws.WS_CLOSE_FORBIDDEN, ws.WS_CLOSE_NOT_FOUND)
           == (4401, 4403, 4404))
    _check("新增关闭码：空闲超时 4408 / 并发超限 4409",
           (ws.WS_CLOSE_IDLE, ws.WS_CLOSE_BUSY) == (4408, 4409),
           str((ws.WS_CLOSE_IDLE, ws.WS_CLOSE_BUSY)))
    _check("新增稳定错误码 busy（供前端分支）", ws.ERROR_BUSY == "busy", ws.ERROR_BUSY)

    fe_socket = (_BACKEND.parent / "frontend" / "src" / "api" / "interviewSocket.js")
    fe_src = fe_socket.read_text(encoding="utf-8")
    _check("★ 前端镜像了 4408（跨语言契约必须逐字对齐）",
           "WS_CLOSE_IDLE = 4408" in fe_src)
    _check("★ 前端镜像了 4409", "WS_CLOSE_BUSY = 4409" in fe_src)
    _check("  └ 前端镜像了 ERROR_BUSY", "ERROR_BUSY = 'busy'" in fe_src)

    # 重连名单：在 `_scheduleReconnect` **函数体内**取证（不是「文件里出现过」）
    reconnect_at = fe_src.index("_scheduleReconnect(code)")
    reconnect_body = fe_src[reconnect_at: reconnect_at + 1200]
    _check("★ 4408 / 4409 都在「不重连」名单里（否则会无限重试刷屏）",
           "WS_CLOSE_IDLE" in reconnect_body and "WS_CLOSE_BUSY" in reconnect_body,
           reconnect_body[:200])
    _check("  └ 名单里仍含既有的 4401 / 4404（未被新代码挤掉）",
           "WS_CLOSE_UNAUTHORIZED" in reconnect_body and "WS_CLOSE_NOT_FOUND" in reconnect_body)
    _check("★ 前端有心跳（否则服务端的空闲超时会误杀活连接）",
           "HEARTBEAT_INTERVAL_MS" in fe_src and "this.ping()" in fe_src)
    _check("  └ 心跳定时器在关闭/重连时被清掉（不泄漏定时器）",
           fe_src.count("_stopHeartbeat()") >= 3, str(fe_src.count("_stopHeartbeat()")))

    # ------------------------------------------------------------
    # [10] 边界守卫（AST）
    # ------------------------------------------------------------
    print("\n[10] 边界守卫（AST：分层单向 / 零第三方）")
    prod_src = _read("services/interview_production.py")
    prod_imports = _all_imports(prod_src)
    _check("★ 生产化模块**不 import 任何项目内模块**（尤其编排层）",
           not any(m.startswith("services.") for m in prod_imports),
           str(sorted(m for m in prod_imports if m.startswith("services."))))
    _check("  └ 顶层只用标准库",
           _top_level_roots(prod_src) <= {
               "__future__", "logging", "os", "re", "threading", "time",
               "dataclasses", "typing",
           }, str(sorted(_top_level_roots(prod_src))))
    _check("  └ 未引入第三方依赖（httpx / requests / aiohttp）",
           not any(tok in m for m in prod_imports for tok in ("httpx", "requests", "aiohttp")),
           str(sorted(prod_imports)))

    # ★ 既有断言「编排层消费者收口为唯一闭集」必须仍然成立（生产化模块没打破它）
    consumers = []
    for path in sorted((_BACKEND / "services").glob("*.py")) + sorted((_BACKEND / "api").glob("*.py")):
        if _ORCH_MODULE in _all_imports(path.read_text(encoding="utf-8")):
            consumers.append(path.relative_to(_BACKEND).as_posix())
    if _ORCH_MODULE in _all_imports(_read("main.py")):
        consumers.append("main.py")
    _check("★★ 编排层消费者仍是唯一闭集（新增模块未打破「可选层」性质）",
           consumers == ["api/interview_ws.py"], str(consumers))

    ws_src = _read("api/interview_ws.py")
    _check("实时通道已接线：注册会话淘汰回调", "register_evict_hook" in ws_src)
    _check("实时通道已接线：空闲超时用 asyncio.wait_for 包住 receive_json",
           "asyncio.wait_for" in ws_src and "receive_json()" in ws_src)
    _check("实时通道已接线：并发闸门（连接数 + 会话数）",
           "acquire_connection" in ws_src and "acquire_session" in ws_src)
    _check("  └ 且额度在 finally 释放（否则随连接次数单调泄漏）",
           "gate.release_connection" in ws_src and "gate.release_session" in ws_src)
    _check("实时通道已接线：每会话互斥", "try_enter" in ws_src and "mutex.exit" in ws_src)
    _check("实时通道已接线：指标计数", "METRICS.incr" in ws_src)
    _check("实时通道已接线：结构化日志", "log_event" in ws_src)
    _check("★ api/interview.py **仍未被改**（文字面试 HTTP 契约一行未动）",
           "WebSocket" not in _read("api/interview.py")
           and "interview_ws" not in _read("api/interview.py"))

    svc_dir = _BACKEND / "services"
    offenders = sorted(
        p.name for p in svc_dir.glob("*.py")
        if any(tok in p.stem.lower() for tok in ("asr", "tts", "speech_recognition", "synthes"))
    )
    _check("★ 未新增独立 ASR / TTS Provider 文件（复用虚拟人内置能力）",
           offenders == [], str(offenders))
    _check("  └ 生产化模块文件名本身也不含这些词",
           not any(tok in "interview_production" for tok in ("asr", "tts", "synthes")))

    # ------------------------------------------------------------
    # [11] 真实 ASGI / WebSocket 栈上的行为取证
    # ------------------------------------------------------------
    print("\n[11] 真机行为取证（空闲超时 / 并发超限）")

    # ★ 必须先设环境变量再让 main.lifespan 触发配置缓存（否则拿到的是旧缓存值）。
    #   阶段 (a) 只把空闲超时调小；连接数上限保持默认（本阶段不测它）。
    os.environ[prod.ENV_IDLE_TIMEOUT] = "0.4"
    prod.reset_runtime()
    _check("阶段 (a) 配置已生效（idle=0.4s）",
           prod.get_config().idle_timeout_seconds == 0.4, str(prod.get_config()))

    async def _seed():
        engine = create_async_engine(DB_URL, poolclass=NullPool)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as db:
            owner = User(username="prod_owner", email="prod_owner@example.com",
                         password_hash="x", role="user")
            job = Job(
                job_name="后端开发工程师", salary="20-35K", edu_require="本科",
                major_require="不限", skills="Python,MySQL",
                duty="负责后端服务的设计与开发", city="深圳", industry="互联网",
            )
            db.add_all([owner, job])
            await db.commit()
            await db.refresh(owner)
            await db.refresh(job)
            db.add(UserSession(user_id=owner.id, token=TOKEN, email=owner.email, role="user"))
            await db.commit()
            return owner.id, job.id
        await engine.dispose()

    owner_id, job_id = asyncio.run(_seed())

    _app_engine = create_async_engine(DB_URL, poolclass=NullPool)
    _app_factory = async_sessionmaker(_app_engine, class_=AsyncSession, expire_on_commit=False)

    async def override_get_db():
        async with _app_factory() as session:
            yield session

    main.app.dependency_overrides[get_db] = override_get_db

    try:
        with TestClient(main.app) as client:
            from deps import get_current_user  # noqa: PLC0415

            def as_user(fn):
                main.app.dependency_overrides[get_current_user] = lambda: {
                    "user_id": owner_id, "email": "prod_owner@example.com", "role": "user",
                }
                try:
                    return fn()
                finally:
                    main.app.dependency_overrides.pop(get_current_user, None)

            def make_session() -> int:
                resp = as_user(lambda: client.post(
                    "/api/interview/create",
                    json={"job_id": job_id, "mode": "avatar", "total_questions": 2},
                ))
                assert resp.status_code == 200, resp.text
                return resp.json()["session"]["id"]

            def ws_url(sid: int) -> str:
                return f"/api/interview/{sid}/ws?token={TOKEN}"

            # ---- (a) 空闲超时：连上后**一句话不说**，服务端应主动以 4408 关闭 ----
            sid_idle = make_session()
            with client.websocket_connect(ws_url(sid_idle)) as sock:
                first = [sock.receive_json() for _ in range(4)]
                _check("(a) 连上后先推 hello + 快照（既有行为未变）",
                       [e.get("type") for e in first]
                       == ["hello", "state", "question", "avatar"],
                       str([e.get("type") for e in first]))
                notice = sock.receive_json()
                _check("  └ ★ 空闲超时前先推一条 error 说明（不是无声断开）",
                       notice.get("type") == "error" and notice.get("code") == ws.ERROR_STATE,
                       str(notice))
                close_code = None
                try:
                    sock.receive_json()
                except WebSocketDisconnect as exc:
                    close_code = exc.code
                _check("  └ ★ 随后以关闭码 **4408** 关闭（服务端真的释放了连接）",
                       close_code == ws.WS_CLOSE_IDLE, str(close_code))

            # ---- (b) 并发上限：第 2 条连接应被拒（4409） ----
            # ★ 换一套配置：空闲超时调回宽裕值（否则第 1 条连接会在我们测试期间
            #   被空闲超时关掉，测的就不是「并发上限」了），连接上限压到 1。
            os.environ[prod.ENV_IDLE_TIMEOUT] = "60"
            os.environ[prod.ENV_MAX_CONNECTIONS_PER_USER] = "1"
            prod.reset_runtime()
            _check("阶段 (b) 配置已生效（idle=60s / 每用户连接上限=1）",
                   prod.get_config().idle_timeout_seconds == 60.0
                   and prod.get_config().max_connections_per_user == 1,
                   str(prod.get_config()))

            sid_busy = make_session()
            with client.websocket_connect(ws_url(sid_busy)) as sock1:
                opened = [sock1.receive_json() for _ in range(4)]
                _check("(b) 第 1 条连接正常建立（hello + 快照）",
                       [e.get("type") for e in opened]
                       == ["hello", "state", "question", "avatar"],
                       str([e.get("type") for e in opened]))
                with client.websocket_connect(ws_url(sid_busy)) as sock2:
                    rejected = sock2.receive_json()
                    _check("  └ ★ 第 2 条连接被拒（error.busy）",
                           rejected.get("type") == "error"
                           and rejected.get("code") == ws.ERROR_BUSY, str(rejected))
                    close_code = None
                    try:
                        sock2.receive_json()
                    except WebSocketDisconnect as exc:
                        close_code = exc.code
                    _check("  └ ★ 关闭码 **4409**", close_code == ws.WS_CLOSE_BUSY, str(close_code))
                # 第 1 条连接**不受影响**（拒绝是局部的，不会连坐）
                sock1.send_json({"type": "ping"})
                _check("  └ ★ 既有连接不受影响（ping → pong 仍正常）",
                       sock1.receive_json().get("type") == "pong")
    finally:
        main.app.dependency_overrides.clear()
        try:
            asyncio.run(_app_engine.dispose())
        except Exception:  # noqa: BLE001 - 释放失败只影响清理，不影响断言
            pass

    # 清理临时库（Windows 上句柄未释放时删不掉，**如实打印**而不是静默）
    try:
        shutil.rmtree(_TMP_DIR, ignore_errors=False)
    except Exception as exc:  # noqa: BLE001
        print(f"  [WARN] 临时目录未能删除（不影响结果）：{_TMP_DIR} -> {exc}")

    # ------------------------------------------------------------
    # 汇总
    # ------------------------------------------------------------
    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
