# -*- coding: utf-8 -*-
"""单 worker 启动断言。

本项目的进程内状态（orchestrator ``_STATES`` / ``SessionTracker`` /
``ConcurrencyGate`` / ``RateLimiter`` / ``utils.login_throttle``）**无法跨 Worker
共享**。多 worker 部署会导致互斥失效、并发计数漂移、空闲超时不一致等严重问题。
本模块在启动时强制校验，防止误配。

★ 已经**不再**属于这个清单的：

- **WebSocket 一次性凭证（ticket）**：已迁到 MySQL 表 ``ws_ticket``
  （``services/ws_ticket_store.py``），「A 实例签发、B 实例校验」与「并发消费
  只成功一次」都有真实 MySQL 并发测试（``tests/ws_ticket_mysql_concurrency_run.py``）。
- **撤销广播**：已迁到只追加表 ``ws_revocation`` + 每实例轮询器
  （``services/ws_revocation.py``），撤销生效延迟实测 0.31s（测试周期 0.2s）/
  2.05s（生产周期 2.0s），见 ``tests/ws_revocation_mysql_run.py``。
- **登录爆破限流**：已提供共享实现 ``services/login_throttle_store.py``
  （表 ``login_failure``），**设 ``AUTH_LOGIN_STORE=db`` 即跨实例统一计数**
  （实测见 ``tests/login_throttle_mysql_run.py``）。默认仍是 ``memory``
  （行为不变），所以要上多 worker 必须显式切过去。

★ 但本断言**仍然保留**：剩下的阻塞项是**编排状态**（``orchestrator._STATES``
需要会话亲和）、``SessionTracker`` / ``ConcurrencyGate`` / ``RateLimiter``
（变成每 worker 各算各的：全局配额 = 上限 × worker 数）。
允许水平扩展的验收条件见 ``CONCURRENCY_CONSISTENCY_REPORT.md`` §9.6。
"""

from __future__ import annotations

import os
import sys


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def assert_single_worker() -> None:
    """若检测到多 worker 配置，立即抛 RuntimeError 阻断启动。"""
    workers = max(
        _env_int("WEB_CONCURRENCY", 0),
        _env_int("UVICORN_WORKERS", 0),
        _env_int("GUNICORN_WORKERS", 0),
    )
    if workers > 1:
        raise RuntimeError(
            f"检测到多 worker 配置（workers={workers}）。"
            "本项目仍有进程内状态：编排状态（需会话亲和）/ SessionTracker / "
            "并发闸门 / 作答限流 —— 多 worker 会让它们按 worker 各算各的"
            "（全局配额 = 上限 × worker 数，互斥与空闲超时也会失效）。"
            "ticket、撤销广播已下沉到 MySQL；登录限流设 AUTH_LOGIN_STORE=db 即可共享。"
            "解除本断言的验收条件见 CONCURRENCY_CONSISTENCY_REPORT.md §9.6。"
        )

    # 最佳努力：尝试文件锁（不阻塞启动，仅增强校验）
    try:
        _try_file_lock()
    except Exception:
        pass


def _try_file_lock() -> None:
    """对 instance 目录下的 .app.lock 取非阻塞独占锁。失败则报错。"""
    import pathlib

    lock_dir = pathlib.Path(__file__).resolve().parents[1] / "instance"
    lock_dir.mkdir(exist_ok=True)
    lock_path = lock_dir / ".app.lock"

    if sys.platform == "win32":
        import msvcrt

        with open(lock_path, "w") as f:
            # 非阻塞：立即抛异常如果被占用
            try:
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
                # 持有锁直到进程结束（不释放）
                msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
            except OSError as exc:
                raise RuntimeError(
                    "另一个进程已持有 app 锁（可能已有本应用实例在运行）。"
                    "单 worker 模式下同一主机不应启动多个进程。"
                ) from exc
    else:
        import fcntl

        with open(lock_path, "w") as f:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError(
                    "另一个进程已持有 app 锁（可能已有本应用实例在运行）。"
                    "单 worker 模式下同一主机不应启动多个进程。"
                ) from exc
