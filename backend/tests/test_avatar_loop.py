# -*- coding: utf-8 -*-
"""讯飞数字人**完整交互闭环**自检（提问 → 播报 → ASR 语音作答 → 评分 → 下一题）

无需 pytest，直接运行：
    python backend/tests/test_avatar_loop.py

闭环（本套件逐段取证）
----------------------
::

    ① create_session   → 状态 waiting_question
    ② prepare_avatar   → 【Avatar 启动】预检 / 初始化数字人（幂等）
    ③ start            → 生成整场题目 + 数字人播报第一题 → 状态 avatar_speaking
    ④ speech_done      → 播报结束 → 状态 waiting_answer
    ⑤ answer_from_asr  → 【讯飞虚拟人 ASR → 取文本 → submit_answer()】
                         评分 + 交付下一题 → 状态 avatar_speaking / finished
    ⑥ finish           → 结束 + 报告

覆盖
----
[1]  契约：通道结果状态集 / 两个信封的恒定键集 / ``__all__`` / 新增异常档位
[2]  异常分类：``classify_error`` 穷举（含**子类优先级**）；Mock 未知会话 ⇒ expired；
     讯飞「失效码」映射位（**默认空 ⇒ 行为不变**；注入 ⇒ 升级为 SessionExpiredError）
[3]  ``ensure_ready``（Avatar 启动）：成功 / 初始化失败 / 建会话失败 / 配置非法 /
     非数字人会话（unsupported）
[4]  ``capture_answer``（ASR 取文本）：ready / empty / 网络异常 / 会话失效 /
     非数字人会话 / **信封键集恒定（成功与失败同形状）**
[5]  Service 层包装：``ensure_avatar`` / ``capture_avatar_answer`` + 归属校验 404
[6]  **完整闭环端到端**：断言 **provider 调用序列**、状态推进、**ASR 文本真的落库**、
     报告生成；并断言 text 会话**零** provider 调用（不受影响）
[7]  异常分支：ASR 失败**不提交**（零 ``submit_answer``）/ 空结果可重试 / 兜底文本 /
     顺序守卫（**先校验后调外部**）/ 数字人会话失效（需重建）/ 面试会话不存在 404
[8]  边界守卫（AST）：编排层**未新增分层边**（顶层依赖不变）/ 通道层不反向依赖编排层 /
     Service 不直接 import provider / **未新增独立 ASR·TTS Provider 文件**

数据库使用 SQLite 内存库（``StaticPool``），不触碰本机 MySQL。
``AVATAR_PROVIDER`` 由 ``regression_env`` 钉成空串 ⇒ 走 **Mock**，零联网。
"""

import asyncio
import ast
import contextlib
import os
import pathlib
import sys

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))
sys.path.insert(0, str(_BACKEND / "tests"))

import regression_env  # noqa: E402,F401  （钉住 AVATAR_* ⇒ 数字人走 Mock，不联网）

from fastapi import HTTPException  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401
from database import Base  # noqa: E402
from models import INTERVIEW_MODE_AVATAR, INTERVIEW_MODE_TEXT, Job, User  # noqa: E402
from schemas.interview import AnswerSubmitRequest, SessionCreateRequest  # noqa: E402
from services import (  # noqa: E402
    avatar_interview,
    avatar_provider,
    avatar_provider_iflytek,
    interview_orchestrator as orch,
    interview_service,
)
from services.avatar_provider import (  # noqa: E402
    ANSWER_EMPTY,
    SESSION_CREATED,
    SESSION_INITIALIZED,
    SESSION_STOPPED,
    AvatarAnswer,
    AvatarProvider,
    AvatarProviderConfigError,
    AvatarProviderError,
    AvatarProviderResponseError,
    AvatarProviderSessionExpiredError,
    AvatarProviderUnavailableError,
    AvatarSession,
    AvatarSpeech,
    AvatarStatus,
    MockAvatarProvider,
)

#: 全部为**占位符**——真实凭据只应写进 backend/.env，绝不进仓库。
APP_ID = "APPID_PLACEHOLDER"
API_KEY = "KEY_PLACEHOLDER"
API_SECRET = "SECRET_PLACEHOLDER"
API_KEYS = {
    avatar_provider_iflytek.ENV_APP_ID: APP_ID,
    avatar_provider_iflytek.ENV_API_KEY: API_KEY,
    avatar_provider_iflytek.ENV_API_SECRET: API_SECRET,
}

#: 「讯飞虚拟人 ASR 识别出来的用户回答」——闭环第 ⑤ 步要把它**原样**送进评分。
ASR_TEXT = "我负责过订单中台重构，用 Redis 做缓存，QPS 从 800 提升到 5000。"

#: 闭环全程的 provider 调用序列（逐次核对，见 [6]）。
#: 说明：``create_session`` 每次都要调（接口约定「对同一 session_ref 幂等」），
#: 因此它反复出现；``initialize`` 只在第一次（未初始化时）出现。
EXPECTED_LOOP_ACTIONS = [
    "create_session", "initialize",          # ② prepare_avatar
    "create_session", "speak",               # ③ start（播报第一题）
    "create_session", "fetch_answer",        # ⑤ 第 1 次 ASR
    "create_session", "speak", "fetch_answer",  # ⑤ submit_answer → 播报第 2 题
    "create_session", "fetch_answer",        # ⑤ 第 2 次 ASR
    "create_session", "fetch_answer",        # ⑤ submit_answer → 无下一题（不 speak）
    "create_session", "stop",                # ⑥ finish
]

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


def _raises(fn, exc_type):
    try:
        fn()
    except exc_type as exc:
        return True, exc
    except Exception as exc:  # noqa: BLE001
        return False, f"抛了非预期异常 {type(exc).__name__}: {exc}"
    return False, "未抛异常"


async def _araises(coro, exc_type):
    try:
        await coro
    except exc_type as exc:
        return True, exc
    except Exception as exc:  # noqa: BLE001
        return False, f"抛了非预期异常 {type(exc).__name__}: {exc}"
    return False, "未抛异常"


def _top_level_imports(source: str):
    """只取**模块顶层** import；``from services import a, b`` 同时产出裸包名与**点号全名**。

    ★ 守卫匹配模块名必须用点号全名——裸名 ``services`` 会让断言恒真失效
    （本项目已复发多次的坑）。
    """
    names = set()
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _all_imports(source: str):
    """**全量** import（含函数体内的延迟导入）——判「从不 import X」必须走 AST。

    子串匹配会被 docstring 里提到的模块名误伤。
    """
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


class _FakeTransport(avatar_provider_iflytek.AvatarTransport):
    """按顺序吐出预置响应（零联网）。"""

    def __init__(self, responses=None, *, exc=None):
        self._responses = list(responses or [])
        self._exc = exc
        self.requests = []

    async def post_json(self, url, *, body, headers=None, timeout=None):
        self.requests.append({"url": url, "body": body})
        if self._exc is not None:
            raise self._exc
        if not self._responses:
            raise AssertionError("假 transport 的预置响应已用尽")
        return self._responses.pop(0)


def _ok(**payload):
    return {"header": {"code": 0, "message": "success"}, "payload": payload}


class _ExpiredProvider(AvatarProvider):
    """``create_session`` / ``initialize`` 正常，但 **ASR 报「会话失效」**。

    用来稳定复现「数字人会话失效」这一档——Mock 无法构造（它的会话永远在内存里）。
    """

    name = "expired"

    def __init__(self):
        self.calls = []

    async def create_session(self, *, session_ref, avatar_id="", vcn=""):
        self.calls.append("create_session")
        return AvatarSession(avatar_session_id="exp-1", provider=self.name,
                             status=SESSION_CREATED)

    async def initialize(self, avatar_session_id, *, stream=True):
        self.calls.append("initialize")
        return AvatarSession(avatar_session_id=avatar_session_id, provider=self.name,
                             status=SESSION_INITIALIZED, stream_url="exp://stream")

    async def speak(self, avatar_session_id, text, *, interrupt=False):
        self.calls.append("speak")
        return AvatarSpeech(avatar_session_id=avatar_session_id, text=text)

    async def fetch_answer(self, avatar_session_id, *, timeout_ms=0):
        self.calls.append("fetch_answer")
        raise AvatarProviderSessionExpiredError("会话已过期，请重新创建")

    async def status(self, avatar_session_id):
        return AvatarStatus(avatar_session_id=avatar_session_id)

    async def stop(self, avatar_session_id):
        return AvatarStatus(avatar_session_id=avatar_session_id, status=SESSION_STOPPED)


@contextlib.contextmanager
def _spy_service(*names):
    """在 ``interview_service`` 上装调用计数器（原样透传返回值）。"""
    counter: dict = {}
    saved = {n: getattr(interview_service, n) for n in names}

    def _wrap(name, fn):
        async def wrapper(*args, **kwargs):
            counter[name] = counter.get(name, 0) + 1
            return await fn(*args, **kwargs)
        return wrapper

    try:
        for name, fn in saved.items():
            setattr(interview_service, name, _wrap(name, fn))
        yield counter
    finally:
        for name, fn in saved.items():
            setattr(interview_service, name, fn)


def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed(db: AsyncSession):
    user = User(username="loop_user", email="loop@example.com", password_hash="x", role="user")
    job = Job(
        job_name="后端开发工程师", salary="20-35K", edu_require="本科", major_require="不限",
        skills="Python,MySQL,Redis", duty="负责后端服务的设计与开发", city="深圳", industry="互联网",
    )
    db.add_all([user, job])
    await db.commit()
    await db.refresh(user)
    await db.refresh(job)
    return user.id, job.id


async def _make(db, user_id, job_id, *, mode, total=2) -> int:
    result = await orch.create_session(
        db, user_id, SessionCreateRequest(job_id=job_id, mode=mode, total_questions=total)
    )
    return result["session"]["id"]


async def _speak_start(db, user_id, sid) -> dict:
    """② 预检 Avatar + ③ 开始（播报第一题）+ ④ 播报结束——闭环前半段。"""
    ready = await orch.prepare_avatar(db, user_id, sid)
    started = await orch.start(db, user_id, sid)
    orch.speech_done(sid)
    return {"ready": ready, "started": started}


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 68)
    print("讯飞数字人完整交互闭环自检（提问 → 播报 → ASR → 评分 → 下一题）")
    print("=" * 68)

    # ------------------------------------------------------------
    # [1] 契约
    # ------------------------------------------------------------
    print("\n[1] 契约：通道结果状态 / 信封键集 / 异常档位")
    _check("通道结果状态集恒定（7 个，含 unsupported）",
           avatar_interview.CHANNEL_STATES == (
               avatar_interview.CHANNEL_OK, avatar_interview.CHANNEL_EMPTY,
               avatar_interview.CHANNEL_UNAVAILABLE, avatar_interview.CHANNEL_EXPIRED,
               avatar_interview.CHANNEL_UNSUPPORTED, avatar_interview.CHANNEL_DISABLED,
               avatar_interview.CHANNEL_FAILED,
           ), str(avatar_interview.CHANNEL_STATES))
    _check("可重试状态 = {empty, unavailable}（其余重试无用）",
           set(avatar_interview.RETRYABLE_STATES)
           == {avatar_interview.CHANNEL_EMPTY, avatar_interview.CHANNEL_UNAVAILABLE},
           str(avatar_interview.RETRYABLE_STATES))
    _check("capture_answer 信封 = 恒定 8 键",
           avatar_interview.CAPTURE_FIELDS == (
               "ok", "status", "text", "audio_url", "avatar_session_id",
               "error", "error_type", "retryable"), str(avatar_interview.CAPTURE_FIELDS))
    _check("ensure_ready 信封 = 恒定 7 键",
           avatar_interview.READY_FIELDS == (
               "ok", "status", "avatar_session_id", "stream_url",
               "error", "error_type", "retryable"), str(avatar_interview.READY_FIELDS))
    _check("INTEGRATIONS：asr / tts 标为已具备（虚拟人**内置**能力）",
           avatar_interview.INTEGRATIONS == {"iflytek": True, "asr": True, "tts": True},
           str(avatar_interview.INTEGRATIONS))
    missing = [n for n in avatar_interview.__all__ if not hasattr(avatar_interview, n)]
    _check("通道层 __all__ 里每个名字都真实存在", missing == [], str(missing))
    _check("通道层 __all__ 暴露预检 / ASR / 分类",
           {"ensure_ready", "capture_answer", "classify_error",
            "CHANNEL_STATES", "CAPTURE_FIELDS", "READY_FIELDS"} <= set(avatar_interview.__all__))

    _check("编排层 ASR 结果键集 = 恒定 7 键",
           orch.ASR_RESULT_FIELDS == ("ok", "source", "text", "state", "error", "asr", "result"),
           str(orch.ASR_RESULT_FIELDS))
    missing = [n for n in orch.__all__ if not hasattr(orch, n)]
    _check("编排层 __all__ 里每个名字都真实存在", missing == [], str(missing))
    _check("编排层 __all__ 暴露闭环入口",
           {"prepare_avatar", "answer_from_asr", "ASR_RESULT_FIELDS",
            "SOURCE_ASR", "SOURCE_FALLBACK"} <= set(orch.__all__))

    _check("★ 新增异常档位：SessionExpiredError 继承 ResponseError（既有调用方行为不变）",
           issubclass(AvatarProviderSessionExpiredError, AvatarProviderResponseError)
           and issubclass(AvatarProviderSessionExpiredError, AvatarProviderError),
           str(AvatarProviderSessionExpiredError.__mro__))
    _check("  └ 且在 avatar_provider.__all__ 里公开",
           "AvatarProviderSessionExpiredError" in avatar_provider.__all__)
    _check("  └ expired 与 unavailable 是**兄弟**（处置完全不同，不可互相捕获）",
           not issubclass(AvatarProviderSessionExpiredError, AvatarProviderUnavailableError)
           and not issubclass(AvatarProviderUnavailableError, AvatarProviderSessionExpiredError))

    # ------------------------------------------------------------
    # [2] 异常分类
    # ------------------------------------------------------------
    print("\n[2] 异常分类（classify_error + 失效码映射位）")
    _check("SessionExpiredError → expired（**优先于** ResponseError 判定）",
           avatar_interview.classify_error(AvatarProviderSessionExpiredError("x"))
           == avatar_interview.CHANNEL_EXPIRED)
    _check("UnavailableError → unavailable",
           avatar_interview.classify_error(AvatarProviderUnavailableError("x"))
           == avatar_interview.CHANNEL_UNAVAILABLE)
    _check("ConfigError → disabled（部署问题，不该重试）",
           avatar_interview.classify_error(AvatarProviderConfigError("x"))
           == avatar_interview.CHANNEL_DISABLED)
    _check("普通 ResponseError → failed",
           avatar_interview.classify_error(AvatarProviderResponseError("x"))
           == avatar_interview.CHANNEL_FAILED)
    _check("未知异常 → failed（兜底，不静默）",
           avatar_interview.classify_error(RuntimeError("x"))
           == avatar_interview.CHANNEL_FAILED)
    classified = [
        avatar_interview.classify_error(exc)
        for exc in (AvatarProviderSessionExpiredError("x"), AvatarProviderUnavailableError("x"),
                    AvatarProviderConfigError("x"), AvatarProviderResponseError("x"),
                    AvatarProviderError("x"), ValueError("x"))
    ]
    _check("★ 全部分类结果都落在 CHANNEL_STATES 内（无越界值）",
           all(s in avatar_interview.CHANNEL_STATES for s in classified), str(classified))
    _check("  └ 且 ConfigError 不会被 ResponseError 分支抢走（顺序敏感）",
           classified[2] == avatar_interview.CHANNEL_DISABLED, str(classified))

    # Mock 未知会话 ⇒ expired（不是笼统的 failed）
    mock_probe = MockAvatarProvider()
    ok, exc = await _araises(mock_probe.status("不存在"), AvatarProviderResponseError)
    _check("★ Mock 未知会话抛 SessionExpiredError（仍可被 ResponseError 捕获）",
           ok and isinstance(exc, AvatarProviderSessionExpiredError), str(exc))

    # 讯飞「失效码」映射位
    _check("★ SESSION_EXPIRED_CODES 默认为空（不配 = 默认 = 行为不变）",
           avatar_provider_iflytek.SESSION_EXPIRED_CODES == (),
           str(avatar_provider_iflytek.SESSION_EXPIRED_CODES))
    t_default = _FakeTransport([{"header": {"code": 11202, "message": "licc failed"}}])
    p_default = avatar_provider_iflytek.IflytekAvatarProvider(env={**API_KEYS}, transport=t_default)
    ok, exc = await _araises(p_default.create_session(session_ref=1), AvatarProviderResponseError)
    _check("  └ 默认：非 0 code → 普通 ResponseError（**不**升级为 expired）",
           ok and not isinstance(exc, AvatarProviderSessionExpiredError), str(exc))

    saved_codes = avatar_provider_iflytek.SESSION_EXPIRED_CODES
    avatar_provider_iflytek.SESSION_EXPIRED_CODES = ("11202",)
    try:
        t_mapped = _FakeTransport([{"header": {"code": 11202, "message": "licc failed"}}])
        p_mapped = avatar_provider_iflytek.IflytekAvatarProvider(
            env={**API_KEYS}, transport=t_mapped)
        ok, exc = await _araises(p_mapped.create_session(session_ref=1),
                                 AvatarProviderSessionExpiredError)
        _check("★ 注入失效码后：同一 code 升级为 SessionExpiredError（映射**真的接线**）",
               ok, str(exc))
    finally:
        avatar_provider_iflytek.SESSION_EXPIRED_CODES = saved_codes

    # ------------------------------------------------------------
    # [3] ensure_ready（Avatar 启动）
    # ------------------------------------------------------------
    print("\n[3] ensure_ready —— Avatar 启动（预检 / 初始化）")

    class _SessionProbe:
        """最小会话替身（只需 id / interview_mode）。"""

        def __init__(self, sid=7, mode="avatar"):
            self.id = sid
            self.interview_mode = mode

    mock = MockAvatarProvider(answer_text=ASR_TEXT)
    avatar_interview.reset_provider(mock)
    ready = await avatar_interview.ensure_ready(None, _SessionProbe(7))
    _check("成功 ⇒ ok=True / status=ok / 拿到 avatar_session_id + stream_url",
           ready["ok"] is True and ready["status"] == avatar_interview.CHANNEL_OK
           and bool(ready["avatar_session_id"]) and ready["stream_url"] == "mock://stream",
           str(ready))
    _check("  └ 信封键集恒定（= READY_FIELDS）", set(ready) == set(avatar_interview.READY_FIELDS),
           str(sorted(ready)))
    _check("  └ 链路 = create_session + initialize",
           mock.actions() == ["create_session", "initialize"], str(mock.actions()))
    ready_again = await avatar_interview.ensure_ready(None, _SessionProbe(7))
    _check("  └ 幂等：再次预检不再 initialize（复用同一场会话）",
           ready_again["avatar_session_id"] == ready["avatar_session_id"]
           and mock.actions() == ["create_session", "initialize", "create_session"],
           str(mock.actions()))

    broken = MockAvatarProvider(fail_on=["initialize"])
    avatar_interview.reset_provider(broken)
    r = await avatar_interview.ensure_ready(None, _SessionProbe(8))
    _check("★ 初始化失败（网络类）⇒ ok=False / status=unavailable / retryable=True",
           r["ok"] is False and r["status"] == avatar_interview.CHANNEL_UNAVAILABLE
           and r["retryable"] is True and r["error_type"] == "AvatarProviderUnavailableError",
           str(r))
    _check("  └ 失败信封与成功信封**同键集**（调用方无需判键存在性）",
           set(r) == set(ready), str(sorted(set(r) ^ set(ready))))

    broken2 = MockAvatarProvider(fail_on=["create_session"])
    avatar_interview.reset_provider(broken2)
    r2 = await avatar_interview.ensure_ready(None, _SessionProbe(9))
    _check("★ 建会话失败 ⇒ 同样如实上报（不静默吞掉）",
           r2["ok"] is False and r2["status"] == avatar_interview.CHANNEL_UNAVAILABLE, str(r2))

    avatar_interview.reset_provider()
    os.environ["AVATAR_PROVIDER"] = "bogus"
    try:
        r3 = await avatar_interview.ensure_ready(None, _SessionProbe(10))
        _check("★ provider 配置非法 ⇒ status=disabled（部署问题，别重试）",
               r3["ok"] is False and r3["status"] == avatar_interview.CHANNEL_DISABLED
               and r3["retryable"] is False, str(r3))
    finally:
        os.environ["AVATAR_PROVIDER"] = ""
        avatar_interview.reset_provider()

    avatar_interview.reset_provider(MockAvatarProvider(answer_text=ASR_TEXT))
    r4 = await avatar_interview.ensure_ready(None, _SessionProbe(11, mode="text"))
    _check("★ 非数字人会话 ⇒ status=unsupported（不误建数字人会话）",
           r4["ok"] is False and r4["status"] == avatar_interview.CHANNEL_UNSUPPORTED, str(r4))

    # ------------------------------------------------------------
    # [4] capture_answer（ASR 取文本）
    # ------------------------------------------------------------
    print("\n[4] capture_answer —— 讯飞虚拟人 ASR 取文本")
    mock2 = MockAvatarProvider(answer_text=ASR_TEXT)
    avatar_interview.reset_provider(mock2)
    cap = await avatar_interview.capture_answer(None, _SessionProbe(20))
    _check("ready ⇒ ok=True / status=ok / text = ASR 识别文本 / audio_url 非空",
           cap["ok"] is True and cap["status"] == avatar_interview.CHANNEL_OK
           and cap["text"] == ASR_TEXT and bool(cap["audio_url"]), str(cap))
    _check("  └ 信封键集恒定（= CAPTURE_FIELDS）", set(cap) == set(avatar_interview.CAPTURE_FIELDS),
           str(sorted(cap)))
    _check("  └ 链路 = create_session + initialize + fetch_answer（**未**新增 ASR Provider）",
           mock2.actions() == ["create_session", "initialize", "fetch_answer"],
           str(mock2.actions()))

    empty = MockAvatarProvider(answer_text="")
    avatar_interview.reset_provider(empty)
    cap_empty = await avatar_interview.capture_answer(None, _SessionProbe(21))
    _check("★ ASR 空结果 ⇒ status=empty / ok=False / **retryable=True**（让用户重说）",
           cap_empty["ok"] is False and cap_empty["status"] == avatar_interview.CHANNEL_EMPTY
           and cap_empty["retryable"] is True and cap_empty["text"] == "", str(cap_empty))
    _check("  └ 与成功信封同键集", set(cap_empty) == set(cap))

    netfail = MockAvatarProvider(fail_on=["fetch_answer"])
    avatar_interview.reset_provider(netfail)
    cap_net = await avatar_interview.capture_answer(None, _SessionProbe(22))
    _check("★ 网络异常 ⇒ status=unavailable / retryable=True",
           cap_net["ok"] is False and cap_net["status"] == avatar_interview.CHANNEL_UNAVAILABLE
           and cap_net["retryable"] is True, str(cap_net))

    avatar_interview.reset_provider(_ExpiredProvider())
    cap_exp = await avatar_interview.capture_answer(None, _SessionProbe(23))
    _check("★ 会话失效 ⇒ status=expired / **retryable=False**（重试无用，需重建会话）",
           cap_exp["ok"] is False and cap_exp["status"] == avatar_interview.CHANNEL_EXPIRED
           and cap_exp["retryable"] is False
           and cap_exp["error_type"] == "AvatarProviderSessionExpiredError", str(cap_exp))

    avatar_interview.reset_provider(MockAvatarProvider(answer_text=ASR_TEXT))
    cap_unsup = await avatar_interview.capture_answer(None, _SessionProbe(24, mode="text"))
    _check("★ 非数字人会话 ⇒ status=unsupported",
           cap_unsup["status"] == avatar_interview.CHANNEL_UNSUPPORTED, str(cap_unsup))
    _check("  └ 四种失败信封与成功信封**键集完全一致**",
           {frozenset(e) for e in (cap, cap_empty, cap_net, cap_exp, cap_unsup)}
           == {frozenset(avatar_interview.CAPTURE_FIELDS)},
           str([sorted(e) for e in (cap, cap_empty, cap_net, cap_exp, cap_unsup)]))

    # ------------------------------------------------------------
    # [5] Service 层包装
    # ------------------------------------------------------------
    print("\n[5] Service 层包装（唯一接触 DB 处 + 归属校验）")
    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        user_id, job_id = await _seed(db)
        orch.clear_states()
        avatar_interview.reset_provider(MockAvatarProvider(answer_text=ASR_TEXT))

        _check("interview_service 暴露两个闭环能力",
               all(asyncio.iscoroutinefunction(getattr(interview_service, n, None))
                   for n in ("ensure_avatar", "capture_avatar_answer")))

        sid_svc = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR)
        svc_ready = await interview_service.ensure_avatar(db, user_id, sid_svc)
        _check("ensure_avatar ⇒ 信封键集 = READY_FIELDS 且 ok=True",
               svc_ready["ok"] is True and set(svc_ready) == set(avatar_interview.READY_FIELDS),
               str(svc_ready))
        svc_cap = await interview_service.capture_avatar_answer(db, user_id, sid_svc)
        _check("capture_avatar_answer ⇒ 信封键集 = CAPTURE_FIELDS 且 text = ASR 文本",
               svc_cap["ok"] is True and set(svc_cap) == set(avatar_interview.CAPTURE_FIELDS)
               and svc_cap["text"] == ASR_TEXT, str(svc_cap))
        ok, exc = await _araises(interview_service.capture_avatar_answer(db, user_id, 999999),
                                 HTTPException)
        _check("★ 归属 / 存在性校验沿用既有口径（不存在 ⇒ 404，不泄露他人会话）",
               ok and getattr(exc, "status_code", None) == 404, str(exc))
        ok, exc = await _araises(interview_service.ensure_avatar(db, user_id, 999999),
                                 HTTPException)
        _check("  └ ensure_avatar 同样 404", ok and getattr(exc, "status_code", None) == 404)

        # ------------------------------------------------------------
        # [6] 完整闭环端到端
        # ------------------------------------------------------------
        print("\n[6] 完整闭环端到端（数字人模式，逐段取证）")
        orch.clear_states()
        loop_mock = MockAvatarProvider(answer_text=ASR_TEXT)
        avatar_interview.reset_provider(loop_mock)

        sid = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=2)
        _check("① 创建 Session → 状态 waiting_question / 库状态 created",
               orch.get_state(sid) == orch.STATE_WAITING_QUESTION
               and (await interview_service.get_session_detail(db, user_id, sid))
               ["session"]["status"] == "created")
        _check("  └ 此时尚未碰 provider（只建库记录）",
               loop_mock.actions() == [], str(loop_mock.actions()))

        ready = await orch.prepare_avatar(db, user_id, sid)
        _check("② Avatar 启动 ⇒ ok=True / stream_url 拿到 / 状态仍 waiting_question",
               ready["ok"] is True and ready["stream_url"] == "mock://stream"
               and ready["state"] == orch.STATE_WAITING_QUESTION, str(ready))
        _check("  └ provider 序列 = create_session + initialize",
               loop_mock.actions() == ["create_session", "initialize"], str(loop_mock.actions()))

        started = await orch.start(db, user_id, sid)
        _check("③ 生成问题 + 数字人播报第一题 ⇒ 状态 avatar_speaking",
               started["state"] == orch.STATE_AVATAR_SPEAKING
               and started["question"]["question_no"] == 1
               and bool(started["question"]["question"]), str(started.get("state")))
        _check("  └ ★ 播报的正是第一题原文（Core 出的题 → provider.speak）",
               loop_mock.calls[3]["action"] == "speak"
               and loop_mock.calls[3]["text"] == started["question"]["question"],
               str(loop_mock.calls[3]))
        _check("  └ 整场题目一次性落库（total=2）",
               started["session"]["total_questions"] == 2
               and started["session"]["status"] == "ongoing")

        _check("④ 播报结束 → waiting_answer",
               orch.speech_done(sid) == orch.STATE_WAITING_ANSWER
               and orch.get_state(sid) == orch.STATE_WAITING_ANSWER)

        out1 = await orch.answer_from_asr(db, user_id, sid)
        _check("⑤ ASR 取到文本 ⇒ ok=True / source=asr / text 即识别结果",
               out1["ok"] is True and out1["source"] == orch.SOURCE_ASR
               and out1["text"] == ASR_TEXT and out1["asr"]["status"] == "ok", str(out1["error"]))
        _check("  └ ★ ASR 文本**原样落库**（answer_text 与识别结果逐字相等）",
               out1["result"]["answer"]["answer_text"] == ASR_TEXT,
               out1["result"]["answer"]["answer_text"][:30])
        _check("  └ 评分完成（分数 + 反馈）",
               out1["result"]["answer"]["score"] is not None
               and bool(out1["result"]["answer"]["feedback"]))
        _check("  └ ★ 评分完成后**生成下一题**（next_question 题号 2）",
               out1["result"]["next_question"] is not None
               and out1["result"]["next_question"]["question_no"] == 2)
        _check("  └ 状态 → avatar_speaking（下一题已交付播报）",
               out1["state"] == orch.STATE_AVATAR_SPEAKING
               and out1["result"]["all_answered"] is False)
        _check("  └ ASR 结果键集恒定（= ASR_RESULT_FIELDS）",
               set(out1) == set(orch.ASR_RESULT_FIELDS), str(sorted(out1)))

        orch.speech_done(sid)
        out2 = await orch.answer_from_asr(db, user_id, sid)
        _check("  └ 第 2 题同样闭环：ASR → 提交 → all_answered=True / 状态 finished",
               out2["ok"] is True and out2["result"]["all_answered"] is True
               and out2["result"]["next_question"] is None
               and out2["state"] == orch.STATE_FINISHED, str(out2.get("state")))
        _check("  └ 两次作答都真实落库",
               (await interview_service.get_session_detail(db, user_id, sid))
               ["answered_count"] == 2)

        ended = await orch.finish(db, user_id, sid)
        _check("⑥ 结束 + 报告生成",
               ended["report"]["total_score"] is not None
               and ended["state"] == orch.STATE_FINISHED)

        _check("★★ 闭环全程 provider 调用序列与预期**逐项相等**",
               loop_mock.actions() == EXPECTED_LOOP_ACTIONS,
               f"{loop_mock.actions()} != {EXPECTED_LOOP_ACTIONS}")
        ids = {c["avatar_session_id"] for c in loop_mock.calls if "avatar_session_id" in c}
        _check("  └ 全程只有**一场**数字人会话（create_session 幂等，未重复新建）",
               len(ids) == 1, str(ids))

        # text 会话不受影响
        text_id = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_TEXT, total=1)
        before = len(loop_mock.calls)
        await interview_service.start_session(db, user_id, text_id)
        await interview_service.submit_answer(
            db, user_id, text_id, AnswerSubmitRequest(answer_text=ASR_TEXT))
        await interview_service.end_session(db, user_id, text_id)
        _check("text 会话全程 **零** provider 调用（闭环不影响默认路径）",
               len(loop_mock.calls) == before, f"{before} -> {len(loop_mock.calls)}")

        # ------------------------------------------------------------
        # [7] 异常分支
        # ------------------------------------------------------------
        print("\n[7] 异常分支（4 类故障的处置）")
        orch.clear_states()

        # ---- 7.1 ASR 失败（网络）→ 不提交 ----
        fail_mock = MockAvatarProvider(answer_text=ASR_TEXT, fail_on=["fetch_answer"])
        avatar_interview.reset_provider(fail_mock)
        sid7 = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=2)
        await _speak_start(db, user_id, sid7)
        with _spy_service("submit_answer") as counter:
            out_net = await orch.answer_from_asr(db, user_id, sid7)
        _check("★ ASR 网络失败 ⇒ ok=False / status=unavailable / retryable=True",
               out_net["ok"] is False
               and out_net["asr"]["status"] == avatar_interview.CHANNEL_UNAVAILABLE
               and out_net["asr"]["retryable"] is True, str(out_net["error"]))
        _check("  └ ★★ **绝不调用** submit_answer（不会写入空答案）",
               counter.get("submit_answer") is None, str(counter))
        _check("  └ 状态保持不变（仍 waiting_answer，可重试）",
               out_net["state"] == orch.STATE_WAITING_ANSWER
               and orch.get_state(sid7) == orch.STATE_WAITING_ANSWER)
        _check("  └ 未落库任何作答",
               (await interview_service.get_session_detail(db, user_id, sid7))
               ["answered_count"] == 0)

        # ---- 7.2 ASR 空结果 → 不提交、可重试 ----
        empty_mock = MockAvatarProvider(answer_text="")
        avatar_interview.reset_provider(empty_mock)
        sid7b = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=2)
        await _speak_start(db, user_id, sid7b)
        with _spy_service("submit_answer") as counter2:
            out_empty = await orch.answer_from_asr(db, user_id, sid7b)
        _check("★ ASR 空结果 ⇒ status=empty / retryable=True，**不提交**",
               out_empty["ok"] is False
               and out_empty["asr"]["status"] == avatar_interview.CHANNEL_EMPTY
               and out_empty["asr"]["retryable"] is True
               and counter2.get("submit_answer") is None, str(counter2))
        _check("  └ 语义红线：empty ≠「用户没回答」——不据此提交空答案",
               out_empty["text"] == "" and out_empty["error"] is not None)

        # ---- 7.3 兜底文本（ASR 不可用时前端输入框） ----
        sid7c = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=1)
        await _speak_start(db, user_id, sid7c)
        out_fb = await orch.answer_from_asr(db, user_id, sid7c, fallback_text="  手打回答  ")
        _check("★ ASR 失败但给了兜底文本 ⇒ source=fallback，文本被 strip 后提交",
               out_fb["ok"] is True and out_fb["source"] == orch.SOURCE_FALLBACK
               and out_fb["text"] == "手打回答"
               and out_fb["result"]["answer"]["answer_text"] == "手打回答", str(out_fb.get("error")))
        _check("  └ ASR 信封仍如实上报失败（不掩盖）",
               out_fb["asr"]["status"] == avatar_interview.CHANNEL_EMPTY, str(out_fb["asr"]))

        # ---- 7.4 顺序守卫：先校验，后调外部 ----
        sid7d = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=2)
        guard_mock = MockAvatarProvider(answer_text=ASR_TEXT)
        avatar_interview.reset_provider(guard_mock)
        await orch.start(db, user_id, sid7d)          # 停在 avatar_speaking
        calls_before = len(guard_mock.calls)
        ok, exc = await _araises(orch.answer_from_asr(db, user_id, sid7d),
                                 orch.OrchestratorStateError)
        _check("★ 顺序不对（还在播报就取 ASR）⇒ 状态机**先**报错",
               ok, str(exc))
        _check("  └ ★★ 报错发生在**调 provider 之前**（零新增 provider 调用）",
               len(guard_mock.calls) == calls_before,
               f"{calls_before} -> {len(guard_mock.calls)}")
        _check("  └ 状态未被污染（仍 avatar_speaking）",
               orch.get_state(sid7d) == orch.STATE_AVATAR_SPEAKING)

        # ---- 7.5 数字人会话失效 → 需重建 ----
        exp_provider = _ExpiredProvider()
        avatar_interview.reset_provider(exp_provider)
        sid7e = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=2)
        await _speak_start(db, user_id, sid7e)
        out_exp = await orch.answer_from_asr(db, user_id, sid7e)
        _check("★ 数字人会话失效 ⇒ status=expired / retryable=False（重试无用）",
               out_exp["ok"] is False
               and out_exp["asr"]["status"] == avatar_interview.CHANNEL_EXPIRED
               and out_exp["asr"]["retryable"] is False, str(out_exp["error"]))
        _check("  └ 状态保持不变（等待重建后重试）",
               orch.get_state(sid7e) == orch.STATE_WAITING_ANSWER)
        avatar_interview.reset_provider(MockAvatarProvider(answer_text=ASR_TEXT))
        out_recover = await orch.answer_from_asr(db, user_id, sid7e)
        _check("  └ ★ 重建 provider（换一场会话）后同一调用即恢复成功",
               out_recover["ok"] is True and out_recover["source"] == orch.SOURCE_ASR
               and out_recover["text"] == ASR_TEXT, str(out_recover.get("error")))

        # ---- 7.6 面试会话不存在 → 服务层 404 向上抛（不吞异常） ----
        orch.set_state(999999, orch.STATE_WAITING_ANSWER)
        try:
            ok, exc = await _araises(orch.answer_from_asr(db, user_id, 999999), HTTPException)
            _check("★ 面试会话不存在 ⇒ 沿用服务层口径抛 404（不吞异常、不静默降级）",
                   ok and getattr(exc, "status_code", None) == 404, str(exc))
        finally:
            orch.reset_state(999999)

        # ---- 7.7 text 会话被误调 ASR → unsupported ----
        sid7g = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_TEXT, total=1)
        orch.set_state(sid7g, orch.STATE_WAITING_ANSWER)
        out_unsup = await orch.answer_from_asr(db, user_id, sid7g)
        _check("★ text 会话被误调 ASR ⇒ status=unsupported，**不提交**、不打 provider",
               out_unsup["ok"] is False
               and out_unsup["asr"]["status"] == avatar_interview.CHANNEL_UNSUPPORTED, str(out_unsup))

        avatar_interview.reset_provider(MockAvatarProvider(answer_text=ASR_TEXT))

    await engine.dispose()

    # ------------------------------------------------------------
    # [8] 边界守卫（AST）
    # ------------------------------------------------------------
    print("\n[8] 边界守卫（AST：分层单向 / 不新增 Provider）")
    orch_src = (_BACKEND / "services" / "interview_orchestrator.py").read_text(encoding="utf-8")
    top = _top_level_imports(orch_src)
    roots = {m.split(".")[0] for m in top}
    _check("编排层顶层依赖**不变**（标准库 + services，无 fastapi / schemas / models）",
           roots <= {"__future__", "contextlib", "threading", "typing", "services"},
           str(sorted(roots)))
    _check("★ 编排层**未新增分层边**：顶层 services.* 仍是 interview_core / interview_service",
           {m for m in top if m.startswith("services.")}
           == {"services.interview_core", "services.interview_service"},
           str(sorted(m for m in top if m.startswith("services."))))
    _check("  └ 编排层**不**直接依赖通道层（ASR 经 Service 包装，不越界）",
           not any(m.startswith("services.avatar") for m in _all_imports(orch_src)),
           str(sorted(m for m in _all_imports(orch_src) if m.startswith("services.avatar"))))
    _check("  └ ``schemas`` 只在函数体内延迟导入（不破坏顶层依赖约定）",
           "schemas.interview" in _all_imports(orch_src)
           and "schemas.interview" not in top, str(sorted(_all_imports(orch_src))))

    for rel in ("services/avatar_interview.py", "services/interview_service.py",
                "services/avatar_provider.py", "services/avatar_provider_iflytek.py",
                "services/interview_core.py", "api/interview.py", "main.py"):
        mods = _all_imports((_BACKEND / rel).read_text(encoding="utf-8"))
        _check(f"{rel} 不反向 import 编排层",
               "services.interview_orchestrator" not in mods,
               str(sorted(m for m in mods if "orchestrator" in m)))

    svc_mods = _all_imports((_BACKEND / "services" / "interview_service.py").read_text(encoding="utf-8"))
    _check("Service 仍经通道层调用数字人（未直接 import provider）",
           "services.avatar_interview" in svc_mods
           and not any(m.startswith("services.avatar_provider") for m in svc_mods),
           str(sorted(m for m in svc_mods if m.startswith("services.avatar"))))

    _check("通道层不 import 模型层 / 编排层（判据用 CHANNEL 常量，不加新边）",
           not any(m.startswith(("models", "services.interview", "services.interview_orchestrator"))
                   for m in _all_imports(
                       (_BACKEND / "services" / "avatar_interview.py").read_text(encoding="utf-8"))))

    # ★ 用户硬约束：**不新增独立 ASR / TTS Provider**（用虚拟人内置能力）
    svc_dir = _BACKEND / "services"
    offenders = sorted(
        p.name for p in svc_dir.glob("*.py")
        if any(tok in p.stem.lower() for tok in ("asr", "tts", "speech_recognition", "synthes"))
    )
    _check("★ 未新增独立 ASR / TTS Provider 文件（复用虚拟人内置能力）",
           offenders == [], str(offenders))
    for rel in ("services/avatar_interview.py", "services/avatar_provider.py",
                "services/avatar_provider_iflytek.py", "services/interview_service.py",
                "services/interview_orchestrator.py"):
        mods = {m.lower() for m in _all_imports((_BACKEND / rel).read_text(encoding="utf-8"))}
        _check(f"  └ {rel} 未 import 任何 asr / tts 独立模块",
               not any(("asr" in m or "tts" in m) and "avatar" not in m for m in mods),
               str(sorted(mods)))

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
