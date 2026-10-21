# -*- coding: utf-8 -*-
"""CI 里运行**真实 MySQL** 运行器，并给出**唯一**的通过/失败判据。

为什么需要这一层
----------------
``tests/*_run.py`` 是**手工**运行器，它们的退出码是：

======  ==========================================
退出码   含义
======  ==========================================
``0``   全部通过
``1``   有失败
``2``   **环境不满足**（``DATABASE_URL`` 不是 MySQL / 连不上 / 无建库权限）
======  ==========================================

``2`` 对**人**是友好的（"这台机器没配 MySQL，跳过吧"），对 **CI 是致命的**：
如果 workflow 里直接 ``python tests/xxx_run.py``，那么"MySQL 容器没起来"
会让这一步返回 ``2``——有人为了让它变绿加一句 ``|| true``，或者干脆不看退出码，
于是**一个用例都没跑，CI 却是绿的**。这正是"用测试制造绿色"的典型形态。

本脚本因此把判据收紧成一条：

    **只有 ``0`` 是通过。``1`` / ``2`` / 任何其它值 / 被信号杀掉，全部判失败。**

同时它**不修改**那几个运行器的退出码语义——本地手工跑的行为一字不变
（仍然可以用 ``echo $?`` 区分"环境没配"和"测试红了"）。

用法::

    cd backend
    python tests/run_mysql_suites.py                  # 跑全部
    python tests/run_mysql_suites.py --only ticket    # 只跑名字含 ticket 的
    python tests/run_mysql_suites.py --list

★ 绝不打印 ``DATABASE_URL`` 的账号密码部分；子进程输出原样透传（运行器自己
  已经做过 ``_redact()``，且不会打印 ticket / token 明文）。
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import subprocess
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
_TESTS = pathlib.Path(__file__).resolve().parent
for _p in (str(_BACKEND), str(_TESTS)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import mysql_guard  # noqa: E402

# 本地手工运行时从 ``backend/.env`` 取连接串（与 ``database.py`` 同一手法），
# 这样 ``python tests/run_mysql_suites.py`` 和 ``python tests/xxx_run.py``
# 的行为一致。CI 里没有 ``.env`` ⇒ 这一步是空操作，连接串来自 workflow 的 env。
try:
    from dotenv import load_dotenv

    load_dotenv(str(_BACKEND / ".env"))
except Exception:  # noqa: BLE001 - 缺 python-dotenv 时照常按环境变量走
    pass

#: (短名, 文件, 覆盖什么)。顺序即执行顺序。
SUITES = [
    ("pool", "engine_pool_mysql_run.py",
     "驱动/连接池/事务边界：aiomysql 异步、pre_ping 对照组、池上限、提交回滚隔离"),
    ("ticket", "ws_ticket_mysql_concurrency_run.py",
     "ticket 共享存储：8 连接并发消费同一 ticket 只能成功 1 次"),
    ("concurrency", "interview_mysql_concurrency_run.py",
     "面试业务并发：行锁串行化 / 幂等 / 事务边界"),
    ("revocation", "ws_revocation_mysql_run.py",
     "撤销广播：REPEATABLE READ 下的增量读 / 跨实例 / 并发写读"),
    ("throttle", "login_throttle_mysql_run.py",
     "登录限流：跨实例统一计数 / 并发突发上界 / 只存摘要"),
]

#: 运行器自己的结果行：``结果: 通过 51 项，失败 0 项``
_RESULT_RE = re.compile(r"结果[:：]\s*通过\s*(\d+)\s*项[，,]\s*失败\s*(\d+)\s*项")

EXIT_OK = 0
EXIT_TEST_FAILED = 1
EXIT_ENV_UNSATISFIED = 2
EXIT_ABNORMAL = 3


def _redact(url: str) -> str:
    """去掉 ``user:password@`` 部分（只留 host/db），用于打印。"""
    return url.rsplit("@", 1)[-1] if "@" in url else url


def _run_one(name: str, script: str, label: str) -> dict:
    path = _TESTS / script
    print("\n" + "=" * 74)
    print(f"[SUITE] {name} · {script}")
    print(f"        {label}")
    print("=" * 74)

    if not path.exists():
        print(f"[FAIL] 运行器文件不存在：{path}")
        return {"name": name, "script": script, "code": EXIT_ABNORMAL,
                "passed": 0, "failed": 0, "note": "文件缺失"}

    proc = subprocess.Popen(
        [sys.executable, str(path)],
        cwd=str(_BACKEND),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        env=os.environ.copy(),
    )

    passed = failed = 0
    tail = []
    assert proc.stdout is not None
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        m = _RESULT_RE.search(line)
        if m:
            passed, failed = int(m.group(1)), int(m.group(2))
        tail.append(line.rstrip())
        if len(tail) > 40:
            tail.pop(0)

    code = proc.wait()

    note = {0: "通过", 1: "有失败", 2: "环境不满足"}.get(code, f"异常退出({code})")
    if code not in (0, 1, 2):
        # 被信号杀掉（负数）或 argparse 之类的 2 以外的错误码
        note = f"异常退出({code})"

    return {"name": name, "script": script, "code": code,
            "passed": passed, "failed": failed, "note": note, "tail": tail}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="CI 用的真实 MySQL 运行器包装（只有 0 是通过）")
    parser.add_argument("--only", default="", help="只跑名字/文件名包含该子串的套件")
    parser.add_argument("--list", action="store_true", help="只列出套件")
    args = parser.parse_args()

    selected = [s for s in SUITES
                if not args.only or args.only in s[0] or args.only in s[1]]

    if args.list:
        for name, script, label in selected:
            print(f"{name:12s} {script:38s} {label}")
        return EXIT_OK

    if not selected:
        print(f"[FAIL] --only {args.only!r} 没有匹配任何套件")
        return EXIT_ABNORMAL

    url = os.environ.get("DATABASE_URL", "").strip()
    print("=" * 74)
    print("真实 MySQL 运行器（CI 判据：只有退出码 0 是通过）")
    print("=" * 74)
    print(f"  DATABASE_URL（已脱敏）：{_redact(url) if url else '<未设置>'}")

    if not url:
        print("[FAIL] 未设置 DATABASE_URL —— CI 必须提供真实 MySQL 连接串")
        return EXIT_ENV_UNSATISFIED
    if not url.startswith("mysql"):
        print("[FAIL] DATABASE_URL 不是 MySQL —— 本脚本只接受真实 MySQL"
              "（SQLite 无法验证行锁与隔离级别）")
        return EXIT_ENV_UNSATISFIED

    # ★ 目标库守卫：与各运行器内的守卫同一条判据（双保险）。
    #   集成测试会建库建表并 DROP，绝不允许指向生产 / 开发库。
    unsafe = mysql_guard.unsafe_reason(url)
    if unsafe:
        print(f"[FAIL] {unsafe}")
        return EXIT_ENV_UNSATISFIED

    results = [_run_one(name, script, label) for name, script, label in selected]

    print("\n" + "=" * 74)
    print("汇总")
    print("=" * 74)
    print(f"  {'套件':<14}{'退出码':<8}{'通过':<6}{'失败':<6}判定")
    for r in results:
        print(f"  {r['name']:<14}{r['code']:<8}{r['passed']:<6}{r['failed']:<6}{r['note']}")

    env_bad = [r for r in results if r["code"] == EXIT_ENV_UNSATISFIED]
    # ★ 区分「断言失败」和「根本没跑到断言就崩了」：后者 ``failed == 0``。
    #   把崩溃说成"断言失败"会掩盖真实原因（连不上库 / 驱动异常 / 建库无权限）。
    test_bad = [r for r in results
                if r["code"] not in (0, 2) and r["failed"] > 0]
    other_bad = [r for r in results
                 if r["code"] not in (0, 2) and r["failed"] == 0]

    if env_bad:
        print("\n[FAIL] ★ 有套件因**环境不满足**退出（数据库没起来 / 无建库权限）。")
        print("       CI 里这**不是**跳过，而是失败：没有任何用例真正跑过。")
        for r in env_bad:
            print(f"       - {r['name']}（{r['script']}）")
        return EXIT_ENV_UNSATISFIED

    if other_bad:
        print("\n[FAIL] ★ 有套件**没跑到断言就异常退出**"
              "（连不上库 / 驱动异常 / 崩溃 / 被信号杀掉）：")
        for r in other_bad:
            print(f"       - {r['name']}（{r['script']}）退出码 {r['code']}，"
                  f"已过 {r['passed']} 项")
            for line in (r.get("tail") or [])[-15:]:
                print(f"         | {line}")
        return EXIT_ABNORMAL

    if test_bad:
        print("\n[FAIL] ★ 有套件断言失败：")
        for r in test_bad:
            print(f"       - {r['name']}（{r['script']}）失败 {r['failed']} 项")
        return EXIT_TEST_FAILED

    total = sum(r["passed"] for r in results)
    print(f"\n[OK] {len(results)} 个套件全部通过，共 {total} 项断言")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
