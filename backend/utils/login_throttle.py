# -*- coding: utf-8 -*-
"""登录失败滑动窗口限流（进程内）。

单 worker 部署下够用；多 worker 时各进程独立计数，已知限制。
"""

from __future__ import annotations

import os
from collections import deque
from time import monotonic
from typing import Dict, List


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        v = int(raw)
    except ValueError:
        return default
    return v


MAX_FAILURES = _env_int("AUTH_LOGIN_MAX_FAILURES", 5)
WINDOW_SECONDS = _env_int("AUTH_LOGIN_WINDOW_SECONDS", 900)
_MAX_KEYS = 10_000

# key -> deque of timestamps
_failures: Dict[str, deque] = {}


def _prune() -> None:
    """淘汰全局最老的键，防止字典无限增长。"""
    if len(_failures) <= _MAX_KEYS:
        return
    # 简单策略：按 key 数量上限淘汰，不精确按时间
    # 当超限 10% 时，清空最老的 20%
    excess = len(_failures) - _MAX_KEYS
    if excess < _MAX_KEYS // 10:
        return
    # 按最早一条记录排序
    sorted_keys = sorted(
        _failures, key=lambda k: _failures[k][0] if _failures[k] else monotonic()
    )
    for k in sorted_keys[: len(sorted_keys) // 5]:
        _failures.pop(k, None)


def check(key: str) -> int:
    """检查 key 是否被限流。

    返回 0 表示放行；返回 >0 表示还需等待的秒数（Retry-After）。
    """
    now = monotonic()
    dq = _failures.get(key)
    if not dq:
        return 0
    # 修剪窗口
    cutoff = now - WINDOW_SECONDS
    while dq and dq[0] < cutoff:
        dq.popleft()
    if not dq:
        return 0
    if len(dq) < MAX_FAILURES:
        return 0
    retry_after = int(dq[0] + WINDOW_SECONDS - now) + 1
    return max(retry_after, 1)


def record(key: str) -> None:
    """记录一次失败。"""
    now = monotonic()
    dq = _failures.setdefault(key, deque())
    cutoff = now - WINDOW_SECONDS
    while dq and dq[0] < cutoff:
        dq.popleft()
    dq.append(now)
    _prune()


def reset(key: str) -> None:
    """登录成功时清除计数。"""
    _failures.pop(key, None)
