# -*- coding: utf-8 -*-
"""AI 能力接入层（modules.ai）契约自检。

**零密钥、零网络**：所有 Provider 都是 Fake，所有等待都是注入的"零等待"。
真实供应商的联网验证在 ``tests/*_run.py`` 里（不带 ``test_`` 前缀、不进回归循环）。

覆盖的验收标准
--------------
1. 每条关键业务链路都能追踪实际使用的 Provider —— [6] / [9]
2. Provider 故障时有明确错误和降级行为 —— [3] / [4] / [8] / [9]
3. 离线测试不依赖真实密钥 —— 全篇（``regression_env`` 把 ``AI_RUN_MODE`` 钉成 offline）
4. 更换 Provider 不需要修改面试业务核心代码 —— [9] / [10]
5. 旧功能和 API 契约没有未经批准的破坏性变化 —— [8]
"""

import ast
import asyncio
import os
import pathlib
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

import regression_env  # noqa: E402,F401  —— 必须早于任何项目模块

import modules.ai as ai  # noqa: E402
from modules.ai import (  # noqa: E402
    LEGACY_FAILURE_PREFIXES,
    AiAuthError,
    AiBoundaryError,
    AiConfigError,
    AiError,
    AiModeError,
    AiRateLimitError,
    AiResponseError,
    AiTimeoutError,
    AiUnavailableError,
    CallPolicy,
    FakeLlmProvider,
    LlmProvider,
    LlmRequest,
    LlmResponse,
    LlmUsage,
    OfflineLlmProvider,
    PromptBoundary,
    SparkLlmProvider,
    build_llm_provider,
    build_legacy_spark_facade,
    call_llm,
    classify_transport_error,
    describe_capabilities,
    describe_llm,
    register_spark_transport,
    resolve_run_mode,
    scrub_contact_info,
    validate_ai_config,
    validate_startup,
)

_CREDS = {"SPARK_APP_ID": "app", "SPARK_API_KEY": "key", "SPARK_API_SECRET": "secret"}


def _check(name: str, cond: bool, detail: str = "") -> bool:
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def _section(title: str) -> None:
    print("\n" + title)


async def _no_sleep(_delay: float) -> None:
    """零等待（测试注入，避免真实退避拖慢套件）。"""
    return None


class _SlowProvider(LlmProvider):
    """永不返回的 Provider（用来触发外层超时）。"""

    name = "slow"

    def __init__(self, seconds: float = 1.0) -> None:
        self._seconds = seconds

    async def complete(self, request: LlmRequest) -> LlmResponse:  # pragma: no cover - 不会返回
        await asyncio.sleep(self._seconds)
        raise AssertionError("超时用例不应该走到这里")


class _BoomProvider(LlmProvider):
    """抛非 AiError 的 Provider（验证任意异常都被收敛）。"""

    name = "boom"

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    async def complete(self, request: LlmRequest) -> LlmResponse:
        raise self._exc


# ============================================================
# [1] 运行模式解析
# ============================================================
def check_modes() -> bool:
    _section("[1] 运行模式（mock / offline / production）")
    ok = True

    ok &= _check("凭据缺失 ⇒ 自动判定为 offline（不联网）",
                 resolve_run_mode({}) == "offline", resolve_run_mode({}))
    ok &= _check("凭据齐全 ⇒ 自动判定为 production",
                 resolve_run_mode(dict(_CREDS)) == "production", resolve_run_mode(dict(_CREDS)))
    ok &= _check("显式 mock（大小写不敏感）",
                 resolve_run_mode({"AI_RUN_MODE": " MOCK "}) == "mock")
    ok &= _check("显式 offline 覆盖齐全的凭据",
                 resolve_run_mode({"AI_RUN_MODE": "offline", **_CREDS}) == "offline")

    try:
        resolve_run_mode({"AI_RUN_MODE": "prod"})
        ok &= _check("非法取值必须报错（不静默降级）", False, "未抛异常")
    except AiModeError as exc:
        ok &= _check("非法取值必须报错（不静默降级）", True, str(exc))

    status = ai.describe_run_mode({"AI_RUN_MODE": "prod"})
    ok &= _check("describe_run_mode 绝不抛异常，把原因塞进 error",
                 status["error"] is not None and status["mode"] == "offline", str(status))
    ok &= _check("  └ 缺失的凭据键名被列出（不返回值本身）",
                 status["missing_credentials"] == ["SPARK_APP_ID", "SPARK_API_KEY", "SPARK_API_SECRET"],
                 str(status["missing_credentials"]))

    try:
        validate_ai_config({"AI_RUN_MODE": "production"})
        ok &= _check("production + 缺凭据 ⇒ 启动校验失败", False, "未抛异常")
    except AiModeError:
        ok &= _check("production + 缺凭据 ⇒ 启动校验失败", True)
    ok &= _check("production + 凭据齐全 ⇒ 启动校验通过",
                 validate_ai_config({"AI_RUN_MODE": "production", **_CREDS})["mode"] == "production")
    return ok


# ============================================================
# [2] 生产模式绝不静默回退
# ============================================================
def check_no_silent_fallback() -> bool:
    _section("[2] 生产模式不静默回退到 Mock / offline")
    ok = True

    register_spark_transport(None)

    def _expect_mode_error(env, label):
        nonlocal ok
        try:
            provider = build_llm_provider(env)
            ok &= _check(label, False, "拿到了 provider=%s" % provider.name)
        except AiModeError as exc:
            ok &= _check(label, True, str(exc))

    _expect_mode_error({"AI_RUN_MODE": "production"}, "production 缺凭据 ⇒ AiModeError")
    _expect_mode_error({"AI_RUN_MODE": "production", **_CREDS},
                       "production 未注册传输层 ⇒ AiModeError")
    register_spark_transport(lambda prompt: _async_text("ok"))
    _expect_mode_error({"AI_RUN_MODE": "production", **_CREDS, "AI_LLM_PROVIDER": "fake"},
                       "production 显式选 fake ⇒ AiModeError（假数据不得进生产）")
    _expect_mode_error({"AI_RUN_MODE": "production", **_CREDS, "AI_LLM_PROVIDER": "openai"},
                       "production 选未知 provider ⇒ AiModeError")

    prod = build_llm_provider({"AI_RUN_MODE": "production", **_CREDS})
    ok &= _check("production 装配出真实 Provider", prod.name == "spark", prod.name)
    ok &= _check("mock 模式装配出 Fake（假装成功）",
                 build_llm_provider({"AI_RUN_MODE": "mock"}).name == "fake")
    ok &= _check("offline 模式装配出 Offline（确定失败）",
                 build_llm_provider({"AI_RUN_MODE": "offline"}).name == "offline")

    # 收尾：恢复成"未注册"，避免影响后续小节
    register_spark_transport(None)
    return ok


# ============================================================
# [3] 错误分类
# ============================================================
def check_error_taxonomy() -> bool:
    _section("[3] 错误分类（retryable 是类属性，不解析文案）")
    ok = True

    table = (
        (AiAuthError, "auth", False),
        (AiTimeoutError, "timeout", True),
        (AiRateLimitError, "rate_limit", True),
        (AiUnavailableError, "unavailable", True),
        (AiResponseError, "response", False),
        (AiBoundaryError, "boundary", False),
        (AiModeError, "mode", False),
        (AiConfigError, "config", False),
    )
    for cls, kind, retryable in table:
        ok &= _check("%s ⇒ kind=%s retryable=%s" % (cls.__name__, kind, retryable),
                     cls.kind == kind and cls.retryable is retryable,
                     "%s / %s" % (cls.kind, cls.retryable))

    ok &= _check("TimeoutError ⇒ AiTimeoutError",
                 isinstance(classify_transport_error(TimeoutError("timed out")), AiTimeoutError))
    ok &= _check("ConnectionResetError ⇒ AiUnavailableError",
                 isinstance(classify_transport_error(ConnectionResetError("reset by peer")),
                            AiUnavailableError))
    ok &= _check("HTTP 401 文案 ⇒ AiAuthError",
                 isinstance(classify_transport_error(RuntimeError("HTTP 401 Unauthorized")),
                            AiAuthError))
    ok &= _check("HTTP 429 文案 ⇒ AiRateLimitError",
                 isinstance(classify_transport_error(RuntimeError("429 Too Many Requests")),
                            AiRateLimitError))
    unknown = classify_transport_error(ValueError("完全看不懂的错误"))
    ok &= _check("无法识别 ⇒ 保守归类为不可重试",
                 isinstance(unknown, AiError) and unknown.retryable is False, unknown.kind)

    original = AiTimeoutError("已分类")
    ok &= _check("已是 AiError 的原样返回（幂等）",
                 classify_transport_error(original) is original)
    return ok


# ============================================================
# [4] 超时 / 有限重试 / 指数退避
# ============================================================
async def check_policy() -> bool:
    _section("[4] 超时 / 有限重试 / 指数退避（可注入零等待）")
    ok = True

    policy = CallPolicy(timeout_seconds=5.0, max_attempts=3, base_delay_seconds=0.5,
                        max_delay_seconds=8.0, jitter_ratio=0.5)
    sleeps = []

    async def record(delay: float) -> None:
        sleeps.append(delay)

    fake = FakeLlmProvider(["最终成功"],
                           failures=[AiUnavailableError("第一次挂"), AiUnavailableError("第二次挂")])
    response = await call_llm(fake, LlmRequest(prompt="p"), policy=policy, sleep=record)
    ok &= _check("可重试错误重试后成功", response.text == "最终成功", response.text)
    ok &= _check("  └ 恰好调用 3 次", fake.call_count == 3, str(fake.call_count))
    ok &= _check("  └ 退避 2 次（首次失败不等待）", len(sleeps) == 2, str(sleeps))

    # 退避口径：equal jitter，区间 [cap*(1-ratio), cap]
    ok &= _check("退避下界 = cap*(1-ratio)", abs(policy.backoff_seconds(1, rand=0.0) - 0.25) < 1e-9,
                 str(policy.backoff_seconds(1, rand=0.0)))
    ok &= _check("退避上界 = cap", abs(policy.backoff_seconds(1, rand=1.0) - 0.5) < 1e-9,
                 str(policy.backoff_seconds(1, rand=1.0)))
    ok &= _check("指数增长：第 2 次失败区间翻倍",
                 abs(policy.backoff_seconds(2, rand=1.0) - 1.0) < 1e-9,
                 str(policy.backoff_seconds(2, rand=1.0)))
    ok &= _check("封顶：第 20 次失败不超过 max_delay",
                 policy.backoff_seconds(20, rand=1.0) <= policy.max_delay_seconds,
                 str(policy.backoff_seconds(20, rand=1.0)))
    ok &= _check("jitter 真的在抖动（rand 两端不等）",
                 policy.backoff_seconds(1, rand=0.0) != policy.backoff_seconds(1, rand=1.0))

    # 超时
    try:
        await call_llm(_SlowProvider(1.0), LlmRequest(prompt="p"),
                       policy=CallPolicy(timeout_seconds=0.05, max_attempts=1), sleep=record)
        ok &= _check("外层超时 ⇒ AiTimeoutError", False, "未抛异常")
    except AiTimeoutError as exc:
        ok &= _check("外层超时 ⇒ AiTimeoutError", exc.kind == "timeout", str(exc))

    # 不可重试错误：一次即抛
    auth_fake = FakeLlmProvider(["不该用"], failures=[AiAuthError("401")])
    try:
        await call_llm(auth_fake, LlmRequest(prompt="p"), policy=policy, sleep=record)
        ok &= _check("鉴权失败不重试", False, "未抛异常")
    except AiAuthError:
        ok &= _check("鉴权失败不重试（只调用 1 次）", auth_fake.call_count == 1,
                     str(auth_fake.call_count))

    # 重试耗尽
    always = FakeLlmProvider(["x"], failures=[AiUnavailableError("挂") for _ in range(5)])
    try:
        await call_llm(always, LlmRequest(prompt="p"), policy=policy, sleep=record)
        ok &= _check("重试耗尽后抛出最后一次错误", False, "未抛异常")
    except AiUnavailableError:
        ok &= _check("重试耗尽后抛出最后一次错误（调用 3 次）", always.call_count == 3,
                     str(always.call_count))

    # 任意异常都被收敛（不让厂商异常外泄）
    try:
        await call_llm(_BoomProvider(RuntimeError("connection refused")), LlmRequest(prompt="p"),
                       policy=CallPolicy(max_attempts=1), sleep=record)
        ok &= _check("非 AiError 异常被收敛为 AiError", False, "未抛异常")
    except AiError as exc:
        ok &= _check("非 AiError 异常被收敛为 AiError", exc.kind == "unavailable", exc.kind)
    return ok


# ============================================================
# [5] 非幂等操作绝不盲目重试
# ============================================================
async def check_non_idempotent() -> bool:
    _section("[5] 非幂等操作不得盲目自动重试")
    ok = True

    policy = CallPolicy(max_attempts=5, base_delay_seconds=0.0)
    ok &= _check("幂等请求按策略允许 5 次",
                 policy.attempts_for(LlmRequest(prompt="p")) == 5)
    ok &= _check("★ 非幂等请求恒为 1 次（配置写得再大也没用）",
                 policy.attempts_for(LlmRequest(prompt="p", idempotent=False)) == 1)

    fake = FakeLlmProvider(["x"], failures=[AiUnavailableError("挂") for _ in range(3)])
    try:
        await call_llm(fake, LlmRequest(prompt="p", idempotent=False), policy=policy,
                       sleep=_no_sleep)
        ok &= _check("非幂等调用只尝试一次", False, "未抛异常")
    except AiUnavailableError:
        ok &= _check("★ 非幂等调用只尝试一次（即使错误本身可重试）", fake.call_count == 1,
                     str(fake.call_count))
    return ok


# ============================================================
# [6] 指标 / 追踪 / Prompt 版本
# ============================================================
async def check_telemetry() -> bool:
    _section("[6] 请求追踪 / Prompt 版本 / 指标 / Token 用量")
    ok = True

    ok &= _check("指标键集为固定闭集（8 键）", len(ai.METRIC_KEYS) == 8, str(len(ai.METRIC_KEYS)))
    ok &= _check("  └ snapshot 键集与 METRIC_KEYS 完全一致",
                 sorted(ai.METRICS.snapshot().keys()) == sorted(ai.METRIC_KEYS))

    ai.METRICS.reset()
    ai.METRICS.incr("ai_calls_total", 1)
    ai.METRICS.incr("这个指标没登记过", 999)
    ok &= _check("未登记的指标名被忽略（键集不漂移）",
                 sorted(ai.METRICS.snapshot().keys()) == sorted(ai.METRIC_KEYS))
    ai.METRICS.reset()

    usage = LlmUsage(prompt_tokens=11, completion_tokens=22, total_tokens=33)
    fake = FakeLlmProvider(["回答"], usage=usage, latency_ms=12.5)
    with ai.ai_call_scope("resume/analysis@v3") as scope:
        response = await call_llm(fake, LlmRequest(prompt="p"), sleep=_no_sleep)

    ok &= _check("Prompt 版本贯穿到响应", response.prompt_version == "v3", response.prompt_version)
    ok &= _check("  └ prompt_id 贯穿到响应", response.prompt_id == "resume/analysis@v3",
                 response.prompt_id)
    ok &= _check("  └ request_id 非空且贯穿到 scope", bool(response.request_id)
                 and response.request_id == scope.request_id, response.request_id)
    ok &= _check("Token 用量被透传", response.usage.total_tokens == 33, str(response.usage))
    ok &= _check("耗时被记录", response.latency_ms == 12.5, str(response.latency_ms))
    ok &= _check("scope.response 被写回", scope.response is response)
    ok &= _check("scope.error 为空", scope.error is None)

    snap = ai.METRICS.snapshot()
    ok &= _check("调用数被计数", snap["ai_calls_total"] == 1, str(snap["ai_calls_total"]))
    ok &= _check("累计耗时被记录", snap["ai_latency_ms_total"] == 12.5,
                 str(snap["ai_latency_ms_total"]))
    ok &= _check("Token 用量被计数",
                 snap["ai_tokens_prompt_total"] == 11 and snap["ai_tokens_completion_total"] == 22,
                 str(snap))
    ok &= _check("按 provider 分解", "fake" in ai.METRICS.snapshot_by_provider(),
                 str(ai.METRICS.snapshot_by_provider()))

    # 失败时 scope 带结构化原因
    ai.METRICS.reset()
    bad = FakeLlmProvider(["x"], failures=[AiRateLimitError("429")])
    with ai.ai_call_scope("resume/analysis@v1") as scope2:
        try:
            await call_llm(bad, LlmRequest(prompt="PROMPT-BODY-SHOULD-NOT-LEAK"), sleep=_no_sleep)
        except AiRateLimitError:
            pass
    detail = scope2.error_dict()
    ok &= _check("失败时 scope.error_dict 给出结构化原因",
                 detail is not None and detail["kind"] == "rate_limit" and detail["retryable"] is True,
                 str(detail))
    ok &= _check("  └ 错误里不含 Prompt 正文",
                 bool(detail) and "PROMPT-BODY-SHOULD-NOT-LEAK" not in detail.get("message", ""),
                 str(detail))
    ok &= _check("错误数 / 超时数被计数", ai.METRICS.snapshot()["ai_errors_total"] == 1,
                 str(ai.METRICS.snapshot()))

    as_dict = response.as_dict()
    ok &= _check("响应摘要含 model / usage / latency（供日志）",
                 {"provider", "model", "usage", "latency_ms", "request_id",
                  "prompt_version"} <= set(as_dict), str(sorted(as_dict)))
    return ok


# ============================================================
# [7] Prompt 数据边界
# ============================================================
def check_boundary() -> bool:
    _section("[7] Prompt 数据边界（越权 / 无关数据 / 敏感信息）")
    ok = True

    boundary = PromptBoundary.for_user("u-1", max_chars=100, forbidden=("13900000000",))
    try:
        ai.check_boundary("x" * 101, boundary)
        ok &= _check("超长 Prompt 被拒绝（不截断）", False, "未抛异常")
    except AiBoundaryError:
        ok &= _check("超长 Prompt 被拒绝（不截断）", True)

    try:
        ai.check_boundary("联系我 13900000000", boundary)
        ok &= _check("禁止注入的标识符被拒绝", False, "未抛异常")
    except AiBoundaryError as exc:
        ok &= _check("禁止注入的标识符被拒绝", "13900000000" not in str(exc), str(exc))

    ok &= _check("正常 Prompt 通过", ai.build_guarded_prompt("正常内容", boundary) == "正常内容")

    try:
        ai.ensure_owner("u-1", "u-2", what="简历")
        ok &= _check("归属不一致必须拒绝", False, "未抛异常")
    except AiBoundaryError:
        ok &= _check("归属不一致必须拒绝", True)
    ok &= _check("归属一致放行", ai.ensure_owner("u-1", "u-1") is None)
    ok &= _check("双方都为空（系统级）放行", ai.ensure_owner("", "") is None)

    scrubbed = scrub_contact_info("张三 13912345678 a@b.com 110101199001011234")
    ok &= _check("手机号被脱敏", "13912345678" not in scrubbed, scrubbed)
    ok &= _check("邮箱被脱敏", "a@b.com" not in scrubbed, scrubbed)
    ok &= _check("身份证被脱敏", "110101199001011234" not in scrubbed, scrubbed)
    ok &= _check("正常文字不被误伤", "张三" in scrubbed, scrubbed)
    return ok


# ============================================================
# [8] 旧字符串契约兼容
# ============================================================
async def check_legacy_compat() -> bool:
    _section("[8] 旧字符串契约（失败哨兵逐字沿用，冻结文件零改动）")
    ok = True

    from services import interview_agent  # noqa: PLC0415 - 延迟导入，避免污染前几节

    register_spark_transport(lambda prompt: _async_text("真实回答"))
    facade = build_legacy_spark_facade(
        provider_getter=lambda: build_llm_provider({"AI_RUN_MODE": "production", **_CREDS}),
        policy=CallPolicy(max_attempts=1),
    )
    ok &= _check("成功时原样返回文本", await facade.chat_async("p") == "真实回答")

    # 各类失败都被渲染成**旧前缀**，且旧判定函数仍然认
    cases = (
        (AiTimeoutError("t"), "API调用失败: 请求超时"),
        (AiAuthError("a"), "API调用失败: 鉴权失败"),
        (AiRateLimitError("r"), "API调用失败: 供应商限流"),
        (AiUnavailableError("u"), "API调用失败: 服务不可用"),
        (AiResponseError("e"), "AI 回答为空"),
        (AiModeError("m"), "SparkAPI not available"),
    )
    for error, expected in cases:
        rendered = facade.render_failure(error)
        ok &= _check("%s ⇒ %s" % (type(error).__name__, expected), rendered == expected, rendered)
        ok &= _check("  └ 旧判定仍认它是失败", interview_agent._is_spark_failure(rendered) is True,
                     rendered)
        ok &= _check("  └ 前缀落在 LEGACY_FAILURE_PREFIXES 内",
                     any(rendered.startswith(prefix) for prefix in LEGACY_FAILURE_PREFIXES))

    # 旧客户端把失败当返回值：必须被升级为类型化错误，再渲染回哨兵
    for legacy_text, kind in (("API调用失败: 连接超时", "timeout"),
                              ("API调用失败: 401 unauthorized", "auth"),
                              ("AI 回答为空", "response"),
                              ("SparkAPI not available", "unavailable")):
        provider = SparkLlmProvider(_make_transport(legacy_text))
        try:
            await provider.complete(LlmRequest(prompt="p"))
            ok &= _check("旧哨兵串 %r 被识别为失败" % legacy_text, False, "未抛异常")
        except AiError as exc:
            ok &= _check("旧哨兵串 %r ⇒ kind=%s" % (legacy_text, kind), exc.kind == kind, exc.kind)

    # 门面在 offline 模式下不联网、立即失败
    register_spark_transport(None)
    offline_facade = build_legacy_spark_facade(
        provider_getter=lambda: build_llm_provider({"AI_RUN_MODE": "offline"}),
        policy=CallPolicy(max_attempts=1),
    )
    text = await offline_facade.chat_async("p")
    ok &= _check("offline 模式立即失败且给出旧哨兵",
                 interview_agent._is_spark_failure(text), text)

    # 门面绝不抛异常（旧契约）
    class _Exploding(LlmProvider):
        name = "exploding"

        async def complete(self, request: LlmRequest) -> LlmResponse:
            raise RuntimeError("炸了")

    boom_facade = build_legacy_spark_facade(provider_getter=lambda: _Exploding(),
                                            policy=CallPolicy(max_attempts=1))
    try:
        text = await boom_facade.chat_async("p")
        ok &= _check("门面把任意异常也收敛成哨兵（绝不抛）",
                     interview_agent._is_spark_failure(text), text)
    except Exception as exc:  # noqa: BLE001
        ok &= _check("门面把任意异常也收敛成哨兵（绝不抛）", False, repr(exc))
    return ok


# ============================================================
# [9] 契约测试：换 Provider 不改业务核心
# ============================================================
async def check_contract_with_business_core() -> bool:
    _section("[9] 契约测试：注入 Fake Provider，面试业务核心零改动")
    ok = True

    from services import interview_agent, interview_planner  # noqa: PLC0415

    job = {"job_name": "Java 后端开发工程师", "skills": "Java,Spring Boot,MySQL,Redis",
           "duty": "负责订单系统开发，熟悉分布式系统与消息队列"}
    resume = "项目经历\n负责电商项目后端开发，主导订单系统重构\n技能特长\nJava、Spring Boot、Kafka\n"
    context = {
        "session_id": 7, "current_question_no": 2, "current_stage": "technical",
        "asked_questions": ["请做一下自我介绍"],
        "covered_topics": ["Java"], "weak_topics": ["Redis 持久化"],
        "follow_up_count": 0, "total_questions": 12, "max_questions": 12,
    }
    plan = interview_planner.build_plan(
        interview_type="technical", difficulty="mid", duration=30, job=job, resume=resume,
    )
    question_json = (
        '{"question": "请说明 Redis 持久化中 RDB 与 AOF 的取舍。", "question_type": "technical",'
        ' "topic": "Redis 持久化", "difficulty": "mid",'
        ' "expected_points": ["RDB 快照", "AOF 追加"], "reason": "补强薄弱知识点"}'
    )

    fake = FakeLlmProvider([question_json])
    facade = build_legacy_spark_facade(provider_getter=lambda: fake,
                                       policy=CallPolicy(max_attempts=1))
    result = await interview_agent.generate_question(context, plan, resume, job, spark=facade)
    ok &= _check("★ 冻结的 interview_agent 用新 Provider 正常出题", result["ok"] is True,
                 str(result.get("error")))
    ok &= _check("  └ 解析出题目", result["question"].startswith("请说明 Redis 持久化"),
                 result.get("question", ""))
    ok &= _check("  └ Fake 收到了 Prompt", fake.call_count == 1, str(fake.call_count))
    ok &= _check("  └ Prompt 里带上了本次简历的技能（数据边界内正常注入）",
                 "Kafka" in fake.prompts[0], fake.prompts[0][:120])
    ok &= _check("  └ 已问过的问题进了变量层（上下文正常注入）",
                 "请做一下自我介绍" in fake.prompts[0])

    # 同一个业务函数换 Offline Provider ⇒ 明确失败，绝不伪造题目
    offline_facade = build_legacy_spark_facade(
        provider_getter=lambda: OfflineLlmProvider(),
        policy=CallPolicy(max_attempts=1),
    )
    failed = await interview_agent.generate_question(context, plan, resume, job,
                                                     spark=offline_facade)
    ok &= _check("★ 换 Offline Provider ⇒ ok=False 且 question 为空（不伪造）",
                 failed["ok"] is False and failed["question"] == "", str(failed.get("error")))
    ok &= _check("  └ 失败原因可读（含 Spark 或 服务不可用）",
                 "Spark" in (failed.get("error") or "") or "服务不可用" in (failed.get("error") or ""),
                 str(failed.get("error")))

    # 换第三个 Provider（脚本化）也不需要改业务代码
    scripted = FakeLlmProvider([question_json.replace("Redis 持久化", "Kafka 幂等")])
    scripted_facade = build_legacy_spark_facade(provider_getter=lambda: scripted,
                                                policy=CallPolicy(max_attempts=1))
    third = await interview_agent.generate_question(context, plan, resume, job, spark=scripted_facade)
    ok &= _check("★ 换第三个 Provider 同样零改动", third["ok"] is True, str(third.get("error")))
    return ok


# ============================================================
# [10] AST 守卫
# ============================================================
def check_source_guards() -> bool:
    _section("[10] 源码守卫（新增的是外围，不是改写核心）")
    ok = True

    frozen = (
        "services/interview_agent.py",
        "services/interview_core.py",
        "services/interview_planner.py",
        "services/interview_context.py",
        "services/question_validator.py",
    )
    for rel in frozen:
        imports = _top_imports(_BACKEND / rel)
        offenders = sorted(name for name in imports if _matches(name, ("modules.ai",)))
        ok &= _check("冻结文件 %s 不 import modules.ai（零改动）" % rel, offenders == [],
                     str(offenders))

    ai_dir = _BACKEND / "modules" / "ai"
    ok &= _check("modules/ai 是包（有 __init__.py）", (ai_dir / "__init__.py").exists())

    forbidden = ("services.knowledge_rag", "chromadb", "httpx", "requests", "aiohttp", "main")
    for path in sorted(ai_dir.glob("*.py")):
        imports = _top_imports(path)
        offenders = sorted(name for name in imports if _matches(name, forbidden))
        ok &= _check("modules/ai/%s 顶层不 import %s" % (path.name, "/".join(forbidden)),
                     offenders == [], str(offenders))

    ok &= _check("Spark 协议实现独立归属传输模块，入口不再定义",
                 "class SparkAPI" in (_BACKEND / "modules/ai/spark_transport.py").read_text(encoding="utf-8")
                 and "class SparkAPI" not in (_BACKEND / "main.py").read_text(encoding="utf-8"))

    main_src = (_BACKEND / "main.py").read_text(encoding="utf-8")
    ok &= _check("main.py 已装配门面（spark_api = build_legacy_spark_facade()）",
                 "build_legacy_spark_facade()" in main_src)
    ok &= _check("  └ 传输层已注册", "register_spark_transport(" in main_src)
    ok &= _check("  └ 启动期做 AI 配置校验", "_report_ai_status()" in main_src)
    return ok


def _matches(name: str, prefixes) -> bool:
    """点号全名匹配（``modules.ai`` 命中 ``modules.ai.llm``，不命中 ``modules.aix``）。"""
    for prefix in prefixes:
        if name == prefix or name.startswith(prefix + "."):
            return True
    return False


def _top_imports(path: pathlib.Path):
    """AST 取**顶层** import 的模块全名（不看函数内延迟导入）。"""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(node.module + "." + alias.name for alias in node.names)
    return names


def _make_transport(text: str):
    async def _transport(_prompt: str) -> str:
        return text

    return _transport


async def _async_text(value: str) -> str:
    return value


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 能力接入层（modules.ai）· 契约自检")
    print("=" * 70)

    ok = True
    ok &= check_modes()
    ok &= check_no_silent_fallback()
    ok &= check_error_taxonomy()
    ok &= await check_policy()
    ok &= await check_non_idempotent()
    ok &= await check_telemetry()
    ok &= check_boundary()
    ok &= await check_legacy_compat()
    ok &= await check_contract_with_business_core()
    ok &= check_source_guards()

    _section("[11] 配置摘要（describe_llm / describe_capabilities 永不抛异常）")
    register_spark_transport(lambda prompt: _async_text("x"))
    summary = describe_llm({"AI_RUN_MODE": "production", **_CREDS})
    ok &= _check("describe_llm 报出有效 Provider", summary["effective_provider"] == "spark",
                 str(summary.get("effective_provider")))
    broken = describe_llm({"AI_RUN_MODE": "nonsense"})
    ok &= _check("describe_llm 配置非法也不抛，把原因写进 error",
                 broken["error"] is not None, str(broken))
    caps = describe_capabilities({"AI_RUN_MODE": "mock"})
    ok &= _check("describe_capabilities 汇总四类能力",
                 {"llm", "embedding", "avatar", "speech"} <= set(caps["capabilities"]),
                 str(sorted(caps["capabilities"])))
    ok &= _check("  └ 语音识别明确标注「复用数字人，不单独抽象」",
                 caps["capabilities"]["speech"]["provider"] == "avatar-internal")
    ok &= _check("  └ 指标快照随能力视图一起暴露",
                 sorted(caps["metrics"]["metrics"].keys()) == sorted(ai.METRIC_KEYS))

    try:
        validate_startup({"AI_RUN_MODE": "production"})
        ok &= _check("validate_startup 在生产缺凭据时失败", False, "未抛异常")
    except AiModeError:
        ok &= _check("validate_startup 在生产缺凭据时失败", True)

    print("\n" + "=" * 70)
    print("结果: " + ("全部通过" if ok else "存在失败项"))
    print("=" * 70)
    return ok


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
