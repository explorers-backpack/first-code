# -*- coding: utf-8 -*-
"""数字人 Avatar Provider 自检（**单元测试 + Mock 流程测试**）

无需 pytest，直接运行：
    python backend/tests/test_avatar_provider.py

覆盖
----
[1]  值对象契约（to_dict 键集恒定 / from_dict 往返 / 未知键忽略 / 类型不符抛错）
[2]  错误分类（继承关系决定「可重试 / 需修配置 / 该暴露」）
[3]  Mock Provider 六能力全流程 + 状态流转 + 幂等 + calls 记录
[4]  Mock 失败注入（逐个 action，供降级测试）
[5]  讯飞配置：缺凭据 / 非法 timeout → ConfigError；masked_api_key；空串=未配置
[6]  讯飞鉴权：HMAC-SHA256 签名**可复现**（固定 date，手算比对）
[7]  讯飞 Provider + 假 transport：六能力走通 / 三段式报文 / code!=0 / 网络异常
[8]  工厂与状态上报：env 分支、非法值报错、describe **不抛异常**
[9]  零依赖守卫（AST：接口模块顶层只有标准库；httpx 只在 iflytek 模块函数体内）
[10] **Mock 流程测试**：经 interview_service + avatar_interview 跑完整面试，
     断言 provider 被按阶段驱动；provider 故障时**降级**且流程照常。

数据库使用 SQLite 内存库（``StaticPool``），不触碰本机 MySQL。
"""

import asyncio
import ast
import base64
import hashlib
import hmac
import os
import pathlib
import sys

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))
sys.path.insert(0, str(_BACKEND / "tests"))

import regression_env  # noqa: E402,F401  （钉住 AVATAR_* 等部署旋钮）

from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401
from database import Base  # noqa: E402
from models import (  # noqa: E402
    INTERVIEW_MODE_AVATAR,
    INTERVIEW_MODE_TEXT,
    Job,
    User,
)
from schemas.interview import AnswerSubmitRequest, SessionCreateRequest  # noqa: E402
from services import avatar_interview, avatar_provider, avatar_provider_iflytek, interview_service  # noqa: E402
from services.avatar_provider import (  # noqa: E402
    ANSWER_EMPTY,
    ANSWER_READY,
    SESSION_CREATED,
    SESSION_INITIALIZED,
    SESSION_SPEAKING,
    SESSION_STOPPED,
    AvatarAnswer,
    AvatarProviderConfigError,
    AvatarProviderError,
    AvatarProviderResponseError,
    AvatarProviderUnavailableError,
    AvatarSession,
    AvatarSpeech,
    AvatarStatus,
    AvatarValueError,
    MockAvatarProvider,
)
from services.avatar_provider_iflytek import (  # noqa: E402
    AvatarProviderConfig,
    AvatarTransport,
    IflytekAvatarProvider,
    build_auth_query,
    build_avatar_provider,
    build_signed_url,
    describe_avatar_provider,
    load_avatar_config,
)

#: 全部为**占位符**——真实凭据只应写进 backend/.env，绝不进仓库。
APP_ID = "APPID_PLACEHOLDER"
API_KEY = "KEY_PLACEHOLDER"
API_SECRET = "SECRET_PLACEHOLDER"
#: 用模块常量拼 env 名（避免在测试里写死字符串而与实现漂移）
API_KEYS = {
    avatar_provider_iflytek.ENV_APP_ID: APP_ID,
    avatar_provider_iflytek.ENV_API_KEY: API_KEY,
    avatar_provider_iflytek.ENV_API_SECRET: API_SECRET,
}
ANSWER_TEXT = "我负责过订单中台重构，QPS 从 800 提升到 5000。"

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
    """只取**模块顶层** import（函数体内的延迟导入不算）。"""
    names = set()
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module)
    return names


def _all_imports(source: str):
    """**全量** import（含函数体内的延迟导入）——用于「本模块从不 import X」这类断言。

    ★ 不能用子串匹配 ``"httpx" not in src``：docstring 里出现「httpx」是**说明**，
      不是依赖。判 import 关系必须走 AST（本项目已复发多次的坑）。
    """
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module)
    return names


class _FakeTransport(AvatarTransport):
    """按顺序吐出预置响应；记录每次请求（url / body）。"""

    def __init__(self, responses=None, *, exc=None):
        self._responses = list(responses or [])
        self._exc = exc
        self.requests = []

    async def post_json(self, url, *, body, headers=None, timeout=None):
        self.requests.append({"url": url, "body": body, "headers": headers, "timeout": timeout})
        if self._exc is not None:
            raise self._exc
        if not self._responses:
            raise AssertionError("假 transport 的预置响应已用尽")
        return self._responses.pop(0)


def _ok(**payload):
    return {"header": {"code": 0, "message": "success"}, "payload": payload}


def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed(db: AsyncSession):
    user = User(username="av_user", email="av@example.com", password_hash="x", role="user")
    job = Job(
        job_name="后端开发工程师", salary="20-35K", edu_require="本科", major_require="不限",
        skills="Python,MySQL,Redis", duty="负责后端服务的设计与开发", city="深圳", industry="互联网",
    )
    db.add_all([user, job])
    await db.commit()
    await db.refresh(user)
    await db.refresh(job)
    return user.id, job.id


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 68)
    print("数字人 Avatar Provider 自检（单元 + Mock 流程）")
    print("=" * 68)

    # ------------------------------------------------------------
    # [1] 值对象契约
    # ------------------------------------------------------------
    print("\n[1] 值对象契约")
    session = AvatarSession(
        avatar_session_id="xf-1", provider="iflytek", status=SESSION_CREATED,
        stream_url="rtmp://x/1", avatar_id="a1", vcn="v1", raw={"k": 1},
    )
    _check("AvatarSession.to_dict 键集恒定（7 键）",
           set(session.to_dict()) == set(avatar_provider.SESSION_FIELDS),
           str(sorted(session.to_dict())))
    _check("from_dict(to_dict(x)) == x（往返）",
           AvatarSession.from_dict(session.to_dict()) == session)
    _check("from_dict 未知键忽略",
           AvatarSession.from_dict({"avatar_session_id": "s", "unknown": 1}).avatar_session_id == "s")
    _check("from_dict 缺失取默认（status=created）",
           AvatarSession.from_dict({"avatar_session_id": "s"}).status == SESSION_CREATED)
    ok, _ = _raises(lambda: AvatarSession.from_dict({"avatar_session_id": 123}), AvatarValueError)
    _check("from_dict 类型不符 → AvatarValueError", ok)
    ok, _ = _raises(lambda: AvatarSession.from_dict("not-a-dict"), AvatarValueError)
    _check("from_dict 非 dict → AvatarValueError", ok)
    ok, _ = _raises(lambda: AvatarSession(avatar_session_id=""), AvatarValueError)
    _check("空 avatar_session_id → AvatarValueError", ok)
    _check("AvatarSpeech / AvatarAnswer / AvatarStatus 键集恒定",
           set(AvatarSpeech(avatar_session_id="s", text="t").to_dict()) == set(avatar_provider.SPEECH_FIELDS)
           and set(AvatarAnswer(avatar_session_id="s").to_dict()) == set(avatar_provider.ANSWER_FIELDS)
           and set(AvatarStatus(avatar_session_id="s").to_dict()) == set(avatar_provider.STATUS_FIELDS))
    _check("to_dict 的 raw 是浅拷贝（改返回不污染内部）",
           (lambda d: d["raw"].__setitem__("z", 1) or "z" not in session.raw)(session.to_dict()))

    # ------------------------------------------------------------
    # [2] 错误分类
    # ------------------------------------------------------------
    print("\n[2] 错误分类（决定处理方式）")
    _check("ConfigError 同时是 ValueError（配置错不该重试）",
           issubclass(AvatarProviderConfigError, ValueError)
           and issubclass(AvatarProviderConfigError, AvatarProviderError))
    _check("UnavailableError 是 RuntimeError（可重试 / 可降级）",
           issubclass(AvatarProviderUnavailableError, RuntimeError))
    _check("ResponseError / ValueError 均属 AvatarProviderError",
           issubclass(AvatarProviderResponseError, AvatarProviderError)
           and issubclass(AvatarValueError, ValueError))

    # ------------------------------------------------------------
    # [3] Mock 全流程
    # ------------------------------------------------------------
    print("\n[3] Mock Provider：六能力全流程")
    mock = MockAvatarProvider(answer_text="我的回答", stream_url="mock://s")
    s1 = await mock.create_session(session_ref=7, avatar_id="a1", vcn="v1")
    _check("create_session 返回 created", s1.status == SESSION_CREATED and s1.avatar_id == "a1")
    s1b = await mock.create_session(session_ref=7)
    _check("create_session 对同一 session_ref 幂等", s1b.avatar_session_id == s1.avatar_session_id)
    s1c = await mock.create_session(session_ref=8)
    _check("不同 session_ref ⇒ 不同会话", s1c.avatar_session_id != s1.avatar_session_id)
    s1 = await mock.initialize(s1.avatar_session_id)
    _check("initialize ⇒ initialized + stream_url",
           s1.status == SESSION_INITIALIZED and s1.stream_url == "mock://s")
    speech = await mock.speak(s1.avatar_session_id, "第一题")
    _check("speak ⇒ speaking + duration_ms = 60×字数",
           speech.status == SESSION_SPEAKING and speech.duration_ms == 3 * 60)
    answer = await mock.fetch_answer(s1.avatar_session_id)
    _check("fetch_answer ⇒ ready + text + audio_url",
           answer.status == ANSWER_READY and answer.text == "我的回答" and answer.audio_url)
    st = await mock.status(s1.avatar_session_id)
    _check("status ⇒ speaking / initialized",
           st.status == SESSION_SPEAKING and st.speaking is True and st.initialized is True)
    st = await mock.stop(s1.avatar_session_id)
    _check("stop ⇒ stopped", st.status == SESSION_STOPPED and st.initialized is False)
    _check("calls 记录完整且有序",
           mock.actions() == ["create_session", "create_session", "create_session",
                              "initialize", "speak", "fetch_answer", "status", "stop"],
           str(mock.actions()))
    empty_mock = MockAvatarProvider(answer_text="")
    es = await empty_mock.create_session(session_ref=1)
    _check("answer_text 为空 ⇒ fetch_answer 返回 empty",
           (await empty_mock.fetch_answer(es.avatar_session_id)).status == ANSWER_EMPTY)
    ok, _ = await _araises(mock.status("nope"), AvatarProviderResponseError)
    _check("未知会话 → ResponseError（不返回 None）", ok)

    # ------------------------------------------------------------
    # [4] Mock 失败注入
    # ------------------------------------------------------------
    print("\n[4] Mock 失败注入（供降级测试）")
    for action in ("create_session", "initialize", "speak", "fetch_answer", "status", "stop"):
        failing = MockAvatarProvider(fail_on=[action])
        sess = await MockAvatarProvider().create_session(session_ref=1)  # 另取一个合法 id
        coro = {
            "create_session": lambda: failing.create_session(session_ref=1),
            "initialize": lambda: failing.initialize(sess.avatar_session_id),
            "speak": lambda: failing.speak(sess.avatar_session_id, "t"),
            "fetch_answer": lambda: failing.fetch_answer(sess.avatar_session_id),
            "status": lambda: failing.status(sess.avatar_session_id),
            "stop": lambda: failing.stop(sess.avatar_session_id),
        }[action]()
        ok, _ = await _araises(coro, AvatarProviderUnavailableError)
        _check(f"  └ fail_on={action!r} → UnavailableError", ok)

    # ------------------------------------------------------------
    # [5] 讯飞配置
    # ------------------------------------------------------------
    print("\n[5] 讯飞 Provider 配置")
    ok, exc = _raises(lambda: IflytekAvatarProvider(env={}), AvatarProviderConfigError)
    _check("缺凭据 → ConfigError（构造期就报）", ok, str(exc))
    ok, _ = _raises(
        lambda: IflytekAvatarProvider(env={**API_KEYS, "AVATAR_TIMEOUT": "abc"}),
        AvatarProviderConfigError,
    )
    _check("非法 AVATAR_TIMEOUT → ConfigError", ok)
    ok, _ = _raises(
        lambda: IflytekAvatarProvider(env={**API_KEYS, "AVATAR_TIMEOUT": "-1"}),
        AvatarProviderConfigError,
    )
    _check("非正数 AVATAR_TIMEOUT → ConfigError", ok)
    ok, _ = _raises(
        lambda: IflytekAvatarProvider(env={**API_KEYS, "AVATAR_IFLYTEK_BASE_PATH": "v1"}),
        AvatarProviderConfigError,
    )
    _check("base_path 不以 / 开头 → ConfigError", ok)
    cfg = load_avatar_config({**API_KEYS})
    _check("空串等价未配置（host 回落默认占位）", cfg.host == avatar_provider_iflytek.DEFAULT_HOST)
    _check("masked_api_key 只露尾 4 位",
           cfg.masked_api_key.endswith(API_KEY[-4:])
           and "*" in cfg.masked_api_key and API_KEY not in cfg.masked_api_key)
    _check("api_key / api_secret 不进 repr",
           "SECRET_PLACEHOLDER" not in repr(cfg) and "KEY_PLACEHOLDER" not in repr(cfg))
    _check("configured 判据 = 三项齐全", cfg.configured is True)

    # ------------------------------------------------------------
    # [6] 鉴权签名可复现
    # ------------------------------------------------------------
    print("\n[6] 讯飞鉴权（HMAC-SHA256，POST 请求行）")
    fixed_date = "Fri, 09 Oct 2026 00:00:00 GMT"
    query = build_auth_query(
        "avatar.example.com", "/v1/avatar/session", "MYKEY", "MYSECRET", date=fixed_date
    )
    sig_str = (
        "host: avatar.example.com\n"
        f"date: {fixed_date}\n"
        "POST /v1/avatar/session HTTP/1.1"
    )
    expected_sig = base64.b64encode(
        hmac.new(b"MYSECRET", sig_str.encode("utf-8"), hashlib.sha256).digest()
    ).decode()
    expected_auth = base64.b64encode(
        (
            f'api_key="MYKEY", algorithm="hmac-sha256", '
            f'headers="host date request-line", signature="{expected_sig}"'
        ).encode("utf-8")
    ).decode()
    _check("固定 date ⇒ 签名逐字节可复现", query["authorization"] == expected_auth)
    _check("query 三键齐全（authorization / date / host）",
           set(query) == {"authorization", "date", "host"})
    _check("请求行用的是 POST（与 main._make_url 的 GET 不同）",
           query["authorization"] != build_auth_query(
               "avatar.example.com", "/v1/avatar/session", "MYKEY", "MYSECRET",
               method="GET", date=fixed_date)["authorization"])
    url = build_signed_url("avatar.example.com", "/v1/avatar/session", "MYKEY", "MYSECRET", date=fixed_date)
    _check("signed url 为 https 且带鉴权 query",
           url.startswith("https://avatar.example.com/v1/avatar/session?authorization=")
           and "&host=avatar.example.com" in url)

    # ------------------------------------------------------------
    # [7] 讯飞 Provider + 假 transport
    # ------------------------------------------------------------
    print("\n[7] 讯飞 Provider（假 transport，零联网）")
    transport = _FakeTransport([
        _ok(session_id="xf-1"),                                  # create
        _ok(stream_url="rtmp://xf/1"),                            # initialize
        _ok(duration_ms=1200),                                    # speak
        _ok(text="用户语音回答", audio_url="http://a/1.mp3"),      # fetch_answer
        _ok(status="initialized", initialized=True, speaking=False),  # status
        _ok(),                                                    # stop
    ])
    xf = IflytekAvatarProvider(env={**API_KEYS}, transport=transport)
    xs = await xf.create_session(session_ref=42, avatar_id="av9", vcn="v9")
    _check("create_session 解析出上游会话 id", xs.avatar_session_id == "xf-1" and xs.provider == "iflytek")
    xs = await xf.initialize(xs.avatar_session_id)
    _check("initialize 解析出 stream_url", xs.stream_url == "rtmp://xf/1" and xs.status == SESSION_INITIALIZED)
    sp = await xf.speak(xs.avatar_session_id, "你好")
    _check("speak 解析出 duration_ms", sp.duration_ms == 1200 and sp.status == SESSION_SPEAKING)
    ans = await xf.fetch_answer(xs.avatar_session_id)
    _check("fetch_answer 解析出 text / audio_url",
           ans.text == "用户语音回答" and ans.status == ANSWER_READY and ans.audio_url.endswith(".mp3"))
    st = await xf.status(xs.avatar_session_id)
    _check("status 解析出 initialized / speaking",
           st.initialized is True and st.speaking is False and st.status == SESSION_INITIALIZED)
    st = await xf.stop(xs.avatar_session_id)
    _check("stop ⇒ stopped", st.status == SESSION_STOPPED)
    _check("共发出 6 次请求，路径与能力一一对应",
           len(transport.requests) == 6
           and transport.requests[0]["url"].endswith("/v1/avatar/session?authorization=")
           or "/v1/avatar/session?" in transport.requests[0]["url"],
           str([r["url"][:60] for r in transport.requests]))
    bodies_ok = all(set(r["body"]) == {"header", "parameter", "payload"} for r in transport.requests)
    _check("三段式报文（header / parameter / payload）", bodies_ok)
    _check("  └ header 带 app_id，uid 用上层会话 id",
           transport.requests[0]["body"]["header"]["app_id"] == APP_ID
           and transport.requests[0]["body"]["header"]["uid"] == "42")
    _check("  └ speak 的 payload 带 text（文本驱动）",
           transport.requests[2]["body"]["payload"].get("text") == "你好")
    _check("  └ 每次请求都带 Content-Type + timeout",
           all(r["headers"] == {"Content-Type": "application/json"} and r["timeout"] == 15.0
               for r in transport.requests))

    bad = _FakeTransport([{"header": {"code": 11202, "message": "licc failed"}}])
    xf_bad = IflytekAvatarProvider(env={**API_KEYS}, transport=bad)
    ok, exc = await _araises(xf_bad.create_session(session_ref=1), AvatarProviderResponseError)
    _check("header.code != 0 → ResponseError（带上游 message）", ok and "11202" in str(exc), str(exc))

    net = _FakeTransport(exc=AvatarProviderUnavailableError("连接超时"))
    xf_net = IflytekAvatarProvider(env={**API_KEYS}, transport=net)
    ok, _ = await _araises(xf_net.create_session(session_ref=1), AvatarProviderUnavailableError)
    _check("网络异常 → UnavailableError（可降级）", ok)

    no_id = _FakeTransport([_ok()])  # 响应里没有会话 id
    xf_noid = IflytekAvatarProvider(env={**API_KEYS}, transport=no_id)
    ok, _ = await _araises(xf_noid.create_session(session_ref=1), AvatarProviderResponseError)
    _check("响应缺会话 id → ResponseError（不静默造 id）", ok)

    # ------------------------------------------------------------
    # [8] 工厂与状态上报
    # ------------------------------------------------------------
    print("\n[8] 工厂（按 AVATAR_PROVIDER 选实现）")
    _check("未配置 → Mock（不配=默认=离线）",
           isinstance(build_avatar_provider(env={}), MockAvatarProvider))
    _check("空串 → Mock（空串等价未配置）",
           isinstance(build_avatar_provider(env={"AVATAR_PROVIDER": ""}), MockAvatarProvider))
    _check("mock → Mock",
           isinstance(build_avatar_provider(env={"AVATAR_PROVIDER": "mock"}), MockAvatarProvider))
    _check("iflytek + 凭据 → IflytekAvatarProvider",
           isinstance(build_avatar_provider(env={**API_KEYS, "AVATAR_PROVIDER": "iflytek"},
                                            transport=_FakeTransport([])),
                      IflytekAvatarProvider))
    ok, _ = _raises(lambda: build_avatar_provider(env={"AVATAR_PROVIDER": "bogus"}),
                    AvatarProviderConfigError)
    _check("非法取值 → ConfigError（报错不静默）", ok)
    desc = describe_avatar_provider({})
    _check("describe（未配置）→ provider=mock / configured=True / error=None",
           desc["provider"] == "mock" and desc["configured"] is True and desc["error"] is None)
    desc2 = describe_avatar_provider({"AVATAR_PROVIDER": "iflytek"})
    _check("describe（讯飞缺凭据）→ configured=False 且 error 有说明，**不抛异常**",
           desc2["configured"] is False and bool(desc2["error"]))
    desc3 = describe_avatar_provider({"AVATAR_PROVIDER": "bogus"})
    _check("describe（非法取值）→ 把原因塞进 error，不抛异常", bool(desc3["error"]))
    desc4 = describe_avatar_provider({**API_KEYS, "AVATAR_PROVIDER": "iflytek"})
    _check("describe（讯飞齐全）→ configured=True + masked_api_key",
           desc4["configured"] is True and desc4["masked_api_key"].endswith("LDER"))

    # ------------------------------------------------------------
    # [9] 零依赖守卫
    # ------------------------------------------------------------
    print("\n[9] 零依赖守卫（AST）")
    iface_src = (_BACKEND / "services" / "avatar_provider.py").read_text(encoding="utf-8")
    _check("接口模块顶层只 import 标准库",
           _top_level_imports(iface_src) <= {"__future__", "abc", "dataclasses", "typing"},
           str(sorted(_top_level_imports(iface_src))))
    _check("  └ 全文从不 import httpx（真实实现不在本模块）",
           not any(m == "httpx" or m.startswith("httpx.") for m in _all_imports(iface_src)),
           str(sorted(_all_imports(iface_src))))
    xf_src = (_BACKEND / "services" / "avatar_provider_iflytek.py").read_text(encoding="utf-8")
    _check("讯飞模块顶层不 import httpx（延迟到函数体内）",
           "httpx" not in _top_level_imports(xf_src), str(sorted(_top_level_imports(xf_src))))
    _check("  └ 顶层只 import 标准库 + services.avatar_provider",
           _top_level_imports(xf_src) <= {
               "__future__", "base64", "hashlib", "hmac", "os", "abc", "dataclasses",
               "datetime", "typing", "urllib.parse", "services.avatar_provider",
           }, str(sorted(_top_level_imports(xf_src))))
    ai_src = (_BACKEND / "services" / "avatar_interview.py").read_text(encoding="utf-8")
    _check("通道层不 import 任何厂商 SDK / 网络库",
           not any(m.startswith(("websocket", "requests", "aiohttp", "httpx"))
                   for m in _top_level_imports(ai_src)), str(sorted(_top_level_imports(ai_src))))

    # ------------------------------------------------------------
    # [10] Mock 流程测试（完整面试）
    # ------------------------------------------------------------
    print("\n[10] Mock 流程：完整面试经通道驱动 provider")
    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        user_id, job_id = await _seed(db)

        mock = MockAvatarProvider(answer_text="这是用户语音回答")
        avatar_interview.reset_provider(mock)

        async def _make(mode, total=2):
            res = await interview_service.create_session(
                db, user_id,
                SessionCreateRequest(job_id=job_id, mode=mode, total_questions=total),
            )
            return res["session"]["id"]

        avatar_id = await _make(INTERVIEW_MODE_AVATAR)
        started = await interview_service.start_session(db, user_id, avatar_id)
        _check("start ⇒ create_session + initialize + speak(第一题)",
               mock.actions()[:3] == ["create_session", "initialize", "speak"], str(mock.actions()))
        _check("  └ 播报的正是第一题原文",
               mock.calls[2]["text"] == started["question"]["question"])
        _check("  └ 流程状态机照常推进（ongoing / 第 1 题）",
               started["session"]["status"] == "ongoing" and started["question"]["question_no"] == 1)

        await interview_service.submit_answer(
            db, user_id, avatar_id, AnswerSubmitRequest(answer_text=ANSWER_TEXT))
        _check("answer ⇒ create(幂等复用) + speak(下一题) + fetch_answer(ASR)",
               mock.actions()[3:] == ["create_session", "speak", "fetch_answer"], str(mock.actions()))
        _check("  └ 复用的是同一场数字人会话（未重复新建）",
               mock.session_of(avatar_id) is not None
               and len({c["avatar_session_id"] for c in mock.calls if "avatar_session_id" in c}) == 1,
               str(mock.calls))

        ended = await interview_service.end_session(db, user_id, avatar_id)
        _check("end ⇒ stop", mock.actions()[-1] == "stop", str(mock.actions()))
        _check("  └ 报告照常生成", ended["report"]["total_score"] is not None)

        text_id = await _make(INTERVIEW_MODE_TEXT)
        before = len(mock.calls)
        await interview_service.start_session(db, user_id, text_id)
        await interview_service.submit_answer(
            db, user_id, text_id, AnswerSubmitRequest(answer_text=ANSWER_TEXT))
        await interview_service.end_session(db, user_id, text_id)
        _check("text 会话全程 **零** provider 调用（text 完全不受影响）",
               len(mock.calls) == before, f"{before} -> {len(mock.calls)}")

        # ---- 降级：provider 故障时面试照常 ----
        broken = MockAvatarProvider(fail_on=["speak"])
        avatar_interview.reset_provider(broken)
        avatar2 = await _make(INTERVIEW_MODE_AVATAR)
        payload_probe = {"session": {"id": 1}, "question": {"question": "Q"}, "job_name": None, "message": "m"}
        out = await avatar_interview.enter(db, None, payload_probe)
        _check("provider 抛异常时 enter **原样返回同一对象**（降级）", out is payload_probe)
        started2 = await interview_service.start_session(db, user_id, avatar2)
        _check("  └ 数字人故障不影响 start 出题",
               started2["session"]["status"] == "ongoing" and started2["question"]["question_no"] == 1)
        await interview_service.submit_answer(
            db, user_id, avatar2, AnswerSubmitRequest(answer_text=ANSWER_TEXT))
        ended2 = await interview_service.end_session(db, user_id, avatar2)
        _check("  └ 全流程仍可跑完并出报告（降级不阻断）",
               ended2["report"]["total_score"] is not None)

        # ---- provider 解析失败也要降级 ----
        avatar_interview.reset_provider()
        os.environ["AVATAR_PROVIDER"] = "bogus"
        try:
            out2 = await avatar_interview.enter(db, None, dict(payload_probe))
            _check("provider 配置非法时 enter 仍原样返回（降级）",
                   out2 == payload_probe and out2 is not payload_probe)
        finally:
            os.environ.pop("AVATAR_PROVIDER", None)
            avatar_interview.reset_provider()

    await engine.dispose()

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
