# -*- coding: utf-8 -*-
"""AI 模拟面试 · **生产化支撑层** —— 超时 / 清理 / 并发 / 限流 / 日志 / 指标 / 脱敏。

为什么要有这个模块
------------------
``api/interview_ws.py`` 把「编排时间线」暴露给了前端，但它是一条**长连接**通道：
长连接与短请求的生产风险完全不同——半开连接会累积、进程内状态会无界增长、
同一条会话可能被两个标签页同时作答、凭据可能被顺手打进日志。

这些都不是**业务**逻辑（业务在 ``interview_core`` / ``interview_followup`` 里），
而是**运维**逻辑。把它们集中到这里，好处有三：

1. ``api/interview_ws.py`` 仍然只做「协议编解码 + 调 service」；
2. 这些策略**可以脱离连接单测**（纯类 + 可注入时钟），不必起 ASGI；
3. 全部开关都是**环境变量且默认值即现状**——不配置＝行为不变（本项目硬约束）。

★ 分层约束（务必守住，否则既有守卫会红）
------------------------------------------
- 本模块**不 import** ``services.interview_orchestrator``：
  ``tests/test_interview_ws.py`` 断言「编排层消费者收口为唯一闭集
  ``["api/interview_ws.py"]``」。因此「清理编排状态」这件事由
  **调用方**（``api/interview_ws.py``，它本来就 import 编排层）通过
  :meth:`SessionTracker.register_evict_hook` **注册回调**完成——
  本模块只负责「谁过期了」，不关心「过期了要清什么」。
- 本模块**不 import** 任何 provider / 向量 / Embedding / 知识库模块（同理，守卫收口）。
- 顶层只用**标准库**：不引入新的第三方依赖。
"""
from __future__ import annotations

import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

__all__ = [
    # 配置
    "ProductionConfig",
    "DEFAULT_CONFIG",
    "config_from_env",
    "get_config",
    "safe_config",
    "describe_config_error",
    # 运行时
    "SessionTracker",
    "ConcurrencyGate",
    "SessionMutex",
    "RateLimiter",
    "get_tracker",
    "get_gate",
    "get_mutex",
    "get_limiter",
    "reset_runtime",
    # 日志
    "LOGGER_NAME",
    "get_logger",
    "log_event",
    "format_fields",
    # 脱敏
    "SENSITIVE_KEYS",
    "mask_secret",
    "redact_text",
    "redact_mapping",
    "RedactingFilter",
    # 指标
    "Metrics",
    "METRICS",
    "M_WS_CONNECT",
    "M_WS_CLOSE",
    "M_WS_ACTIVE",
    "M_WS_MESSAGES",
    "M_WS_IDLE_CLOSED",
    "M_WS_UNAUTHORIZED",
    "M_WS_SESSION_NOT_FOUND",
    "M_WS_REJECTED_BUSY",
    "M_ANSWER_SUBMITTED",
    "M_ANSWER_REJECTED",
    "M_ANSWER_RATE_LIMITED",
    "M_STATE_ERRORS",
    "M_SERVICE_ERRORS",
    "M_SESSIONS_EVICTED",
    # 状态上报
    "PRODUCTION_STATUS_FIELDS",
    "describe_production_status",
]


# ============================================================
# 一、配置（**环境变量「不配＝默认＝行为不变」**）
# ============================================================
#: WS 空闲超时：超过这么久没收到任何客户端消息 ⇒ 服务端主动关闭并释放资源。
#: 默认 5 分钟——前端有心跳（``interviewSocket.js`` 每 25s 发一次 ``ping``），
#: 因此「活着但暂时没操作」的连接**不会**被误杀；被清掉的是**真的没了**的连接
#: （拔网线 / 进程被杀 / 浏览器崩溃——这些都不会发 FIN，连接会一直挂着）。
DEFAULT_IDLE_TIMEOUT_SECONDS = 300.0

#: 进程内编排状态 / 会话活跃记录的 TTL。超过即视为「早就结束了的面试」。
DEFAULT_STATE_TTL_SECONDS = 21600.0  # 6 小时

#: 进程内最多跟踪多少条会话（超过按「最久未活动」淘汰，防无界增长）。
DEFAULT_MAX_TRACKED_SESSIONS = 2000

#: 单个用户最多同时开几个**面试会话**（防止刷接口建会话）。
DEFAULT_MAX_SESSIONS_PER_USER = 3

#: 单个用户最多同时开几条 **WS 连接**（一个面试间 1 条；多标签页 / 重复连接会被拒）。
DEFAULT_MAX_CONNECTIONS_PER_USER = 4

#: 同一用户两次作答之间的最小间隔（秒）——防误触连点 / 脚本重放造成的重复评分。
#: ★ 默认 **0.0 = 不限制**：本项目硬约束是「环境变量『不配＝默认＝行为不变』」，
#: 而「两次作答最短间隔」会改变**正常作答节奏**的判定，因此必须**显式 opt-in**。
#: 生产建议设 ``0.4``~``1.0``（真人不可能在 0.4 秒内说完一段回答）。
DEFAULT_MIN_ANSWER_INTERVAL_SECONDS = 0.0

ENV_IDLE_TIMEOUT = "INTERVIEW_WS_IDLE_TIMEOUT"
ENV_STATE_TTL = "INTERVIEW_STATE_TTL"
ENV_MAX_TRACKED = "INTERVIEW_MAX_TRACKED_SESSIONS"
ENV_MAX_SESSIONS_PER_USER = "INTERVIEW_MAX_SESSIONS_PER_USER"
ENV_MAX_CONNECTIONS_PER_USER = "INTERVIEW_MAX_WS_CONNECTIONS"
ENV_MIN_ANSWER_INTERVAL = "INTERVIEW_ANSWER_MIN_INTERVAL"


@dataclass(frozen=True)
class ProductionConfig:
    """生产化策略的**全部**旋钮（不可变值对象，便于在日志/健康检查里原样上报）。"""

    idle_timeout_seconds: float = DEFAULT_IDLE_TIMEOUT_SECONDS
    state_ttl_seconds: float = DEFAULT_STATE_TTL_SECONDS
    max_tracked_sessions: int = DEFAULT_MAX_TRACKED_SESSIONS
    max_sessions_per_user: int = DEFAULT_MAX_SESSIONS_PER_USER
    max_connections_per_user: int = DEFAULT_MAX_CONNECTIONS_PER_USER
    min_answer_interval_seconds: float = DEFAULT_MIN_ANSWER_INTERVAL_SECONDS

    def to_dict(self) -> Dict[str, Any]:
        return {
            "idle_timeout_seconds": self.idle_timeout_seconds,
            "state_ttl_seconds": self.state_ttl_seconds,
            "max_tracked_sessions": self.max_tracked_sessions,
            "max_sessions_per_user": self.max_sessions_per_user,
            "max_connections_per_user": self.max_connections_per_user,
            "min_answer_interval_seconds": self.min_answer_interval_seconds,
        }


#: 「一个环境变量都不配」时的配置——**与既有行为完全一致**。
DEFAULT_CONFIG = ProductionConfig()


def _read_float(
    env: Mapping[str, str], name: str, default: float, *, allow_zero: bool = False
) -> float:
    """读一个浮点环境变量；**未配置 / 空串 ⇒ 默认**，**非法值 ⇒ 报错不静默**。

    :param allow_zero: ``True`` 时 ``0`` 是**合法值**（表示「关闭该保护」）；
        否则必须为正数。
    """
    raw = env.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"环境变量 {name} 必须是数字，实际是 {raw!r}") from exc
    if value < 0 or (value == 0 and not allow_zero):
        raise ValueError(f"环境变量 {name} 必须为{'非负' if allow_zero else '正'}数，实际是 {value!r}")
    return value


def _read_int(env: Mapping[str, str], name: str, default: int) -> int:
    """读一个正整环境变量；语义同上（空串＝未配置，非法值＝报错）。"""
    raw = env.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"环境变量 {name} 必须是整数，实际是 {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"环境变量 {name} 必须为正整数，实际是 {value!r}")
    return value


def config_from_env(env: Optional[Mapping[str, str]] = None) -> ProductionConfig:
    """从环境变量构造配置。

    :param env: 显式环境映射（测试用）；缺省读 ``os.environ``。
    :raises ValueError: 任一变量**已配置但非法**（不静默回落默认值——静默会让
        「以为开了保护、其实没开」这种事故无从察觉）。
    """
    source: Mapping[str, str] = os.environ if env is None else env
    return ProductionConfig(
        idle_timeout_seconds=_read_float(source, ENV_IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT_SECONDS),
        state_ttl_seconds=_read_float(source, ENV_STATE_TTL, DEFAULT_STATE_TTL_SECONDS),
        max_tracked_sessions=_read_int(source, ENV_MAX_TRACKED, DEFAULT_MAX_TRACKED_SESSIONS),
        max_sessions_per_user=_read_int(
            source, ENV_MAX_SESSIONS_PER_USER, DEFAULT_MAX_SESSIONS_PER_USER
        ),
        max_connections_per_user=_read_int(
            source, ENV_MAX_CONNECTIONS_PER_USER, DEFAULT_MAX_CONNECTIONS_PER_USER
        ),
        min_answer_interval_seconds=_read_float(
            source, ENV_MIN_ANSWER_INTERVAL, DEFAULT_MIN_ANSWER_INTERVAL_SECONDS, allow_zero=True
        ),
    )


_CONFIG_CACHE: Optional[ProductionConfig] = None
_CONFIG_ERROR: Optional[str] = None
_CONFIG_LOCK = threading.Lock()


def get_config() -> ProductionConfig:
    """缓存版 :func:`config_from_env`（进程内只解析一次）。

    :raises ValueError: 配置非法时**每次都抛**（不缓存「坏配置」，否则修好了也不生效）。
    """
    global _CONFIG_CACHE, _CONFIG_ERROR
    with _CONFIG_LOCK:
        if _CONFIG_CACHE is not None:
            return _CONFIG_CACHE
        try:
            _CONFIG_CACHE = config_from_env()
            _CONFIG_ERROR = None
        except ValueError as exc:
            _CONFIG_ERROR = str(exc)
            raise
        return _CONFIG_CACHE


def describe_config_error() -> Optional[str]:
    """返回上一次配置解析的错误（无错则 ``None``）——供健康检查**纯读**上报。"""
    return _CONFIG_ERROR


def safe_config() -> ProductionConfig:
    """**永不抛**的配置读取：非法时回落 :data:`DEFAULT_CONFIG` 并记一条 ERROR 日志。

    为什么需要它：配置非法不该让**整条实时通道**打不开（面试中断是更坏的后果），
    但也**绝不能静默**——所以这里「降级 + 响亮记录 + 指标计数」，让问题一定看得见。
    启动期 ``main._report_production_status`` 会另打一条 ``[ERROR]``。
    """
    try:
        return get_config()
    except ValueError as exc:
        METRICS.incr(M_CONFIG_ERRORS)
        get_logger().error("生产化配置非法，已回落默认值（行为与未配置一致）：%s", exc)
        return DEFAULT_CONFIG


# ============================================================
# 二、脱敏（**凭据绝不进日志**）
# ============================================================
#: 键名命中这些词（小写、去分隔符后比较）即视为敏感值。
SENSITIVE_KEYS: Tuple[str, ...] = (
    "apikey",
    "apisecret",
    "secret",
    "password",
    "passwd",
    "token",
    "authorization",
    "auth",
    "credential",
    "signature",
    "accesskey",
    "privatekey",
)


def _normalize_key(key: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(key).lower())


def is_sensitive_key(key: Any) -> bool:
    """键名是否敏感（``api_key`` / ``apiKey`` / ``api-key`` 都命中）。"""
    normalized = _normalize_key(key)
    return any(token in normalized for token in SENSITIVE_KEYS)


def mask_secret(value: Any, *, keep: int = 4) -> str:
    """把凭据换成 ``****1234`` 形式（保留末 ``keep`` 位以便核对**是哪一把**）。"""
    text = "" if value is None else str(value)
    if not text:
        return ""
    if len(text) <= keep:
        return "*" * len(text)
    return "*" * (len(text) - keep) + text[-keep:]


#: 形如 ``api_key=xxx`` / ``Authorization: Bearer xxx`` 的**行内**凭据。
#: ``ticket`` 也在此列：WS 一次性连接凭证虽然短时，但同样不应落进访问日志。
_INLINE_SECRET_RE = re.compile(
    r"(?i)\b(api[_-]?key|api[_-]?secret|secret|password|passwd|token|ticket|authorization|signature)"
    r"\b(\s*[:=]\s*)(\S+)"
)


def redact_text(text: Any) -> str:
    """把一段文本里 ``k=v`` 形式的凭据值替换成掩码（其余原样保留）。"""
    return _INLINE_SECRET_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}{mask_secret(m.group(3))}", str(text))


def redact_mapping(mapping: Mapping[Any, Any]) -> Dict[str, Any]:
    """递归脱敏一个映射（只脱敏**键名敏感**的值，其余原样）。"""
    out: Dict[str, Any] = {}
    for key, value in mapping.items():
        name = str(key)
        if is_sensitive_key(name):
            out[name] = mask_secret(value)
        elif isinstance(value, Mapping):
            out[name] = redact_mapping(value)
        elif isinstance(value, (list, tuple)):
            out[name] = [
                redact_mapping(item) if isinstance(item, Mapping) else item for item in value
            ]
        else:
            out[name] = value
    return out


class RedactingFilter(logging.Filter):
    """给日志器挂上的**兜底脱敏**过滤器。

    为什么需要它：调用方可能一不留神把整个配置对象塞进日志（``logger.info(cfg)``），
    而 ``AvatarProviderConfig`` 的 ``api_key`` 只在 ``repr`` 上做了 ``repr=False``
    ——``logging`` 的 ``%s`` 走的是 ``str()``，**不保证**同样安全。
    本过滤器在 ``record`` 落盘前把消息与参数过一遍 :func:`redact_text`，
    把「凭据泄漏」从「靠人自觉」变成「结构上不可能」。
    """

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003 - logging 接口
        try:
            if record.args:
                # ★ 必须对**格式化之后**的整条消息脱敏：``logger.info("k=%s", v)`` 里
                #   键名在 msg、值在 args，分开处理**看不见** ``k=v`` 这个模式
                #   （本项目的既有教训：契约要按「拼出来的东西」判，不是按片段判）。
                record.msg = redact_text(record.getMessage())
                record.args = ()
            elif isinstance(record.msg, str) and record.msg:
                record.msg = redact_text(record.msg)
        except Exception:  # noqa: BLE001 - 脱敏失败绝不能吞掉日志
            pass
        return True


# ============================================================
# 三、结构化日志
# ============================================================
#: 统一的面试域日志器名（``logging.getLogger`` 层级，便于生产侧按前缀分流）。
LOGGER_NAME = "careerai.interview"

_FILTER_INSTALLED = False
_FILTER_LOCK = threading.Lock()


def get_logger() -> logging.Logger:
    """取面试域日志器（**幂等**安装脱敏过滤器）。"""
    global _FILTER_INSTALLED
    logger = logging.getLogger(LOGGER_NAME)
    with _FILTER_LOCK:
        if not _FILTER_INSTALLED:
            logger.addFilter(RedactingFilter())
            _FILTER_INSTALLED = True
    return logger


def format_fields(fields: Mapping[str, Any]) -> str:
    """把结构化字段拼成 ``k=v`` 串（**先脱敏**；``None`` 一律跳过）。"""
    safe = redact_mapping(fields)
    parts = [f"{key}={value}" for key, value in sorted(safe.items()) if value is not None]
    return " ".join(parts)


def log_event(
    event: str,
    *,
    level: int = logging.INFO,
    session_id: Optional[int] = None,
    user_id: Optional[int] = None,
    **fields: Any,
) -> None:
    """记一条结构化事件（**永不抛异常**——日志不该把请求带崩）。

    :param event: 稳定事件名（如 ``ws_connect`` / ``answer_submitted``），**可被日志系统聚合**。
    """
    try:
        payload: Dict[str, Any] = {}
        if session_id is not None:
            payload["session_id"] = session_id
        if user_id is not None:
            payload["user_id"] = user_id
        payload.update(fields)
        suffix = format_fields(payload)
        get_logger().log(level, "%s%s", event, f" {suffix}" if suffix else "")
    except Exception:  # noqa: BLE001 - 日志失败不影响业务
        pass


# ============================================================
# 四、指标（进程内计数器；供 ``GET /api/health`` 的 ``metrics`` 键上报）
# ============================================================
M_WS_CONNECT = "ws_connections_total"
M_WS_CLOSE = "ws_closes_total"
M_WS_ACTIVE = "ws_connections_active"
M_WS_MESSAGES = "ws_messages_total"
M_WS_IDLE_CLOSED = "ws_idle_timeouts_total"
M_WS_UNAUTHORIZED = "ws_unauthorized_total"
M_WS_SESSION_NOT_FOUND = "ws_session_not_found_total"
M_WS_REJECTED_BUSY = "ws_rejected_busy_total"
M_ANSWER_SUBMITTED = "answer_submitted_total"
M_ANSWER_REJECTED = "answer_rejected_total"
M_ANSWER_RATE_LIMITED = "answer_rate_limited_total"
M_STATE_ERRORS = "orchestrator_state_errors_total"
M_SERVICE_ERRORS = "service_errors_total"
M_SESSIONS_EVICTED = "sessions_evicted_total"
M_CONFIG_ERRORS = "config_errors_total"

#: 这些是**瞬时量**（当前值），其余按累加计数。
_GAUGE_NAMES = frozenset({M_WS_ACTIVE})


class Metrics:
    """极简进程内指标表（累加计数 + 瞬时量）。

    刻意不引入 prometheus_client：本项目「不新增第三方依赖」，而运维真正要看的
    几个数（连接数 / 消息数 / 超时数 / 作答数 / 淘汰数）用 ``dict`` + 锁就够了。
    多副本部署时每个副本各自计数——**这本身就是正确的**（要聚合请由采集端做）。
    """

    def __init__(self) -> None:
        self._values: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._started = time.monotonic()

    def incr(self, name: str, amount: float = 1.0) -> None:
        """累加一个计数器（``M_WS_ACTIVE`` 这类瞬时量**不**走这里，见 ``set_gauge``）。"""
        with self._lock:
            self._values[name] = self._values.get(name, 0.0) + float(amount)

    def set_gauge(self, name: str, value: float) -> None:
        """设置一个瞬时量（如「当前活跃连接数」）。"""
        with self._lock:
            self._values[name] = float(value)

    def get(self, name: str, default: float = 0.0) -> float:
        with self._lock:
            return self._values.get(name, default)

    def snapshot(self) -> Dict[str, Any]:
        """固定形状快照：``{counters, gauges, uptime_seconds}``（**永不抛**）。"""
        try:
            with self._lock:
                values = dict(self._values)
            counters = {k: int(v) for k, v in sorted(values.items()) if k not in _GAUGE_NAMES}
            gauges = {k: int(v) for k, v in sorted(values.items()) if k in _GAUGE_NAMES}
            return {
                "counters": counters,
                "gauges": gauges,
                "uptime_seconds": round(time.monotonic() - self._started, 3),
            }
        except Exception:  # noqa: BLE001 - 状态上报永不抛
            return {"counters": {}, "gauges": {}, "uptime_seconds": 0.0}

    def reset(self) -> None:
        """清零（测试用）。"""
        with self._lock:
            self._values.clear()
            self._started = time.monotonic()


#: 全进程唯一指标表（模块级单例——健康检查与实时通道看到的是同一份）。
METRICS = Metrics()


# ============================================================
# 五、会话活跃表（TTL + 容量上限）
# ============================================================
class SessionTracker:
    """进程内「会话最近活跃时刻」表，用来决定**哪些会话该被清理**。

    ★ 它**不认识编排层**：``sweep()`` 只回答「这些 id 过期了」，具体清什么由
    调用方通过 :meth:`register_evict_hook` 注册的回调决定（见模块文档的分层约束）。

    清理时机刻意是**机会式**的——每次新连接建立时顺手 ``sweep()`` 一次。
    好处：不需要后台线程、不需要在 ``lifespan`` 里挂任务（那会引入新的启动顺序依赖），
    而「有人来连」本来就是「该清清旧账了」的天然信号。
    """

    def __init__(
        self,
        *,
        ttl_seconds: float = DEFAULT_STATE_TTL_SECONDS,
        max_tracked: int = DEFAULT_MAX_TRACKED_SESSIONS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ttl = float(ttl_seconds)
        self._max = int(max_tracked)
        self._clock = clock
        self._seen: Dict[int, float] = {}
        self._hooks: List[Callable[[int], None]] = []
        self._lock = threading.Lock()

    # ---- 注册清理回调 ----
    def register_evict_hook(self, hook: Callable[[int], None]) -> None:
        """注册「会话被淘汰时」的回调（如 ``interview_orchestrator.reset_state``）。

        **幂等**：同一函数重复注册只生效一次（``api/interview_ws.py`` 可能在
        每次连接时都注册，重复添加会让回调被调用多次）。
        """
        with self._lock:
            if hook not in self._hooks:
                self._hooks.append(hook)

    def clear_hooks(self) -> None:
        """清空回调（测试用）。"""
        with self._lock:
            self._hooks = []

    # ---- 记录 ----
    def touch(self, session_id: int) -> None:
        """记录/刷新一条会话的活跃时刻。"""
        with self._lock:
            self._seen[int(session_id)] = self._clock()

    def forget(self, session_id: int) -> None:
        """摘掉一条会话（**主动断开**，不是过期 ⇒ **不触发**清理回调）。"""
        with self._lock:
            self._seen.pop(int(session_id), None)

    def is_tracked(self, session_id: int) -> bool:
        with self._lock:
            return int(session_id) in self._seen

    def size(self) -> int:
        with self._lock:
            return len(self._seen)

    def last_seen(self, session_id: int) -> Optional[float]:
        with self._lock:
            return self._seen.get(int(session_id))

    # ---- 过期判定 ----
    def expired(self) -> List[int]:
        """**纯计算**：返回已超 TTL 的会话 id（升序），不改动任何状态。"""
        now = self._clock()
        with self._lock:
            return sorted(sid for sid, ts in self._seen.items() if now - ts >= self._ttl)

    def _overflow(self) -> List[int]:
        """**纯计算**：容量超限时需要淘汰的「最久未活动」会话 id（升序）。"""
        with self._lock:
            if len(self._seen) <= self._max:
                return []
            ordered = sorted(self._seen.items(), key=lambda kv: (kv[1], kv[0]))
            return sorted(sid for sid, _ in ordered[: len(ordered) - self._max])

    def sweep(self) -> List[int]:
        """摘除「过期 + 容量超限」的会话并触发清理回调，返回被淘汰的 id（升序）。"""
        targets = sorted(set(self.expired()) | set(self._overflow()))
        if not targets:
            return []
        with self._lock:
            for sid in targets:
                self._seen.pop(sid, None)
            hooks = list(self._hooks)
        for sid in targets:
            METRICS.incr(M_SESSIONS_EVICTED)
            for hook in hooks:
                try:
                    hook(sid)
                except Exception as exc:  # noqa: BLE001 - 单个回调失败不阻断其余清理
                    log_event(
                        "session_evict_hook_failed",
                        level=logging.WARNING,
                        session_id=sid,
                        error=f"{type(exc).__name__}: {exc}",
                    )
        log_event("sessions_evicted", count=len(targets), session_ids=",".join(map(str, targets)))
        return targets

    def snapshot(self) -> Dict[str, Any]:
        return {"tracked": self.size(), "ttl_seconds": self._ttl, "max_tracked": self._max}


# ============================================================
# 六、并发闸门（每用户会话数 / 连接数上限）
# ============================================================
class ConcurrencyGate:
    """每用户并发额度（连接数 + 面试会话数）。

    为什么需要：``interview_orchestrator`` 的 ``_STATES_LOCK`` 只保护**它自己那张表**
    的读改写，**不是**业务互斥；而一个用户反复建会话/反复连 WS 会把进程内状态顶起来。
    这里给的是**粗粒度配额**，不是限流（限流见 :class:`RateLimiter`）。
    """

    def __init__(
        self,
        *,
        max_connections_per_user: int = DEFAULT_MAX_CONNECTIONS_PER_USER,
        max_sessions_per_user: int = DEFAULT_MAX_SESSIONS_PER_USER,
    ) -> None:
        self._max_conn = int(max_connections_per_user)
        self._max_sess = int(max_sessions_per_user)
        self._conns: Dict[int, int] = {}
        self._sessions: Dict[int, set] = {}
        self._lock = threading.Lock()

    # ---- 连接 ----
    def acquire_connection(self, user_id: int) -> bool:
        """占用一个连接额度；超限返回 ``False``（**不阻塞、不排队**）。"""
        with self._lock:
            current = self._conns.get(int(user_id), 0)
            if current >= self._max_conn:
                return False
            self._conns[int(user_id)] = current + 1
            return True

    def release_connection(self, user_id: int) -> None:
        with self._lock:
            current = self._conns.get(int(user_id), 0)
            if current <= 1:
                self._conns.pop(int(user_id), None)
            else:
                self._conns[int(user_id)] = current - 1

    def connection_count(self, user_id: int) -> int:
        with self._lock:
            return self._conns.get(int(user_id), 0)

    # ---- 会话 ----
    def acquire_session(self, user_id: int, session_id: int) -> bool:
        """登记一个「正在被本人使用」的会话；超限返回 ``False``。

        **幂等**：同一 ``session_id`` 重复登记不重复计数（重连不该被自己的旧记录挤掉）。
        """
        with self._lock:
            owned = self._sessions.setdefault(int(user_id), set())
            sid = int(session_id)
            if sid in owned:
                return True
            if len(owned) >= self._max_sess:
                return False
            owned.add(sid)
            return True

    def release_session(self, user_id: int, session_id: int) -> None:
        with self._lock:
            owned = self._sessions.get(int(user_id))
            if not owned:
                return
            owned.discard(int(session_id))
            if not owned:
                self._sessions.pop(int(user_id), None)

    def session_count(self, user_id: int) -> int:
        with self._lock:
            return len(self._sessions.get(int(user_id), ()))

    # ---- 汇总 ----
    def active_connections(self) -> int:
        with self._lock:
            return sum(self._conns.values())

    def active_sessions(self) -> int:
        with self._lock:
            return sum(len(v) for v in self._sessions.values())

    def snapshot(self) -> Dict[str, Any]:
        return {
            "active_connections": self.active_connections(),
            "active_sessions": self.active_sessions(),
            "max_connections_per_user": self._max_conn,
            "max_sessions_per_user": self._max_sess,
        }


class SessionMutex:
    """**每会话非阻塞互斥**：同一条会话同一时刻只允许一个操作在跑。

    为什么用「非阻塞 + 拒绝」而不是「排队」：实时通道里排队会让用户看到
    「点了没反应」；直接回一个 ``state_error``/忙提示，前端能立刻给出可理解的反馈。
    典型场景：用户开了两个标签页，同时点了「提交作答」。
    """

    def __init__(self) -> None:
        self._busy: set = set()
        self._lock = threading.Lock()

    def try_enter(self, session_id: int) -> bool:
        with self._lock:
            sid = int(session_id)
            if sid in self._busy:
                return False
            self._busy.add(sid)
            return True

    def exit(self, session_id: int) -> None:
        with self._lock:
            self._busy.discard(int(session_id))

    def busy(self, session_id: int) -> bool:
        with self._lock:
            return int(session_id) in self._busy

    def busy_count(self) -> int:
        with self._lock:
            return len(self._busy)


# ============================================================
# 七、限流（最小间隔；防连点）
# ============================================================
class RateLimiter:
    """按 key 的**最小间隔**限流（key 通常是 ``(动作, user_id)``）。

    刻意用「最小间隔」而不是「滑动窗口 N 次/分钟」：本场景要挡的是**连点/脚本重放**
    （间隔毫秒级），不是正常使用节奏；最小间隔实现更简单、行为更好预测。
    """

    def __init__(
        self,
        *,
        min_interval_seconds: float = DEFAULT_MIN_ANSWER_INTERVAL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._interval = float(min_interval_seconds)
        self._last: Dict[Any, float] = {}
        self._lock = threading.Lock()
        self._clock = clock

    def allow(self, key: Any) -> bool:
        """是否放行；放行时**顺带记时**（未放行不刷新时间，避免「越点越久」）。"""
        now = self._clock()
        with self._lock:
            last = self._last.get(key)
            if last is not None and now - last < self._interval:
                return False
            self._last[key] = now
            return True

    def retry_after(self, key: Any) -> float:
        """还要等多少秒才放行（已可放行返回 ``0.0``）。"""
        with self._lock:
            last = self._last.get(key)
        if last is None:
            return 0.0
        return max(0.0, self._interval - (self._clock() - last))

    def forget(self, key: Any) -> None:
        with self._lock:
            self._last.pop(key, None)

    def size(self) -> int:
        with self._lock:
            return len(self._last)

    def sweep(self, *, max_idle_seconds: float) -> int:
        """清掉久未使用的 key（防 key 空间无界增长）；返回清理条数。"""
        now = self._clock()
        with self._lock:
            stale = [k for k, ts in self._last.items() if now - ts >= max_idle_seconds]
            for key in stale:
                self._last.pop(key, None)
        return len(stale)


# ============================================================
# 八、运行时单例（健康检查与实时通道**必须看到同一份**）
# ============================================================
_RUNTIME_LOCK = threading.Lock()
_TRACKER: Optional[SessionTracker] = None
_GATE: Optional[ConcurrencyGate] = None
_MUTEX: Optional[SessionMutex] = None
_LIMITER: Optional[RateLimiter] = None


def get_tracker() -> SessionTracker:
    global _TRACKER
    with _RUNTIME_LOCK:
        if _TRACKER is None:
            cfg = safe_config()
            _TRACKER = SessionTracker(
                ttl_seconds=cfg.state_ttl_seconds, max_tracked=cfg.max_tracked_sessions
            )
        return _TRACKER


def get_gate() -> ConcurrencyGate:
    global _GATE
    with _RUNTIME_LOCK:
        if _GATE is None:
            cfg = safe_config()
            _GATE = ConcurrencyGate(
                max_connections_per_user=cfg.max_connections_per_user,
                max_sessions_per_user=cfg.max_sessions_per_user,
            )
        return _GATE


def get_mutex() -> SessionMutex:
    global _MUTEX
    with _RUNTIME_LOCK:
        if _MUTEX is None:
            _MUTEX = SessionMutex()
        return _MUTEX


def get_limiter() -> RateLimiter:
    global _LIMITER
    with _RUNTIME_LOCK:
        if _LIMITER is None:
            _LIMITER = RateLimiter(min_interval_seconds=safe_config().min_answer_interval_seconds)
        return _LIMITER


def reset_runtime() -> None:
    """清空全部运行时单例 + 指标（**测试专用**；生产不调用）。"""
    global _TRACKER, _GATE, _MUTEX, _LIMITER, _CONFIG_CACHE, _CONFIG_ERROR
    with _RUNTIME_LOCK:
        _TRACKER = None
        _GATE = None
        _MUTEX = None
        _LIMITER = None
    with _CONFIG_LOCK:
        _CONFIG_CACHE = None
        _CONFIG_ERROR = None
    METRICS.reset()


# ============================================================
# 九、状态上报（供 ``GET /api/health`` 的 ``metrics`` 键）
# ============================================================
#: 上报键集**恒定**（新增能力时在这里加，不要按条件增删）。
PRODUCTION_STATUS_FIELDS: Tuple[str, ...] = (
    "idle_timeout_seconds",
    "state_ttl_seconds",
    "max_tracked_sessions",
    "max_sessions_per_user",
    "max_connections_per_user",
    "min_answer_interval_seconds",
    "tracked_sessions",
    "active_connections",
    "active_sessions",
    "busy_sessions",
    "metrics",
    "error",
)


def describe_production_status() -> Dict[str, Any]:
    """生产化运行时状态（**固定键集**、**永不抛异常**）。

    与既有 ``embedding_status`` / ``vector_status`` / ``threshold_status`` /
    ``avatar_status`` 同一取舍：健康检查永远不该 500，配置/读数出错就把原因放进
    ``error`` 字段暴露出来。

    刻意**不读库、不发网络请求**：本端点必须永远廉价。
    """
    try:
        cfg = safe_config()
        tracker = get_tracker()
        gate = get_gate()
        mutex = get_mutex()
        status: Dict[str, Any] = dict(cfg.to_dict())
        status["tracked_sessions"] = tracker.size()
        status["active_connections"] = gate.active_connections()
        status["active_sessions"] = gate.active_sessions()
        status["busy_sessions"] = mutex.busy_count()
        status["metrics"] = METRICS.snapshot()
        status["error"] = describe_config_error()
        return status
    except Exception as exc:  # noqa: BLE001 - 状态上报永不抛
        fallback: Dict[str, Any] = dict(DEFAULT_CONFIG.to_dict())
        fallback.update(
            {
                "tracked_sessions": 0,
                "active_connections": 0,
                "active_sessions": 0,
                "busy_sessions": 0,
                "metrics": {"counters": {}, "gauges": {}, "uptime_seconds": 0.0},
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        return fallback
