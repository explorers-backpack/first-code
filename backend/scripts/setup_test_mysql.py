# -*- coding: utf-8 -*-
"""为真实 MySQL 集成测试准备**专用测试库 + 专用账号**（幂等，可重复执行）。

为什么要这个脚本
----------------
集成测试会 ``CREATE DATABASE`` 建 probe 库、``create_all`` 建表、跑完 ``DROP``。
如果直接拿生产 / 开发账号跑，唯一的保护就是"操作者记得别指错"——那不是保证。

本脚本创建一个**最小权限**账号，让它**物理上**碰不到别的库：

.. code-block:: sql

    GRANT ALL PRIVILEGES ON `career\_ci\_test`.*          TO 'career_ci'@'%';
    GRANT ALL PRIVILEGES ON `career\_%\_probe\_%`.*       TO 'career_ci'@'%';

- ``career_ci_test``：测试主库（建表 / 跑用例）；
- ``career_%_probe_%``：各运行器自己建的临时 probe 库（跑完即 DROP）。

★ ``_`` 与 ``%`` 在 MySQL 的库级授权里是**通配符**，所以用 ``\\_`` 转义成字面量。
  这条不是猜的：本脚本末尾的 ``[1] 专用账号的最小权限验证`` 会**用这个账号真连一次**
  ——能建/删 probe 库、连不上 admin 库（1044）。四项目前每次执行都会实测。

用法（本地）::

    cd backend
    python scripts/setup_test_mysql.py --password 'local_test_pw' --show-url
    # 按输出 export DATABASE_URL=...
    python tests/run_mysql_suites.py

用法（CI）::

    MYSQL_ADMIN_URL='mysql+aiomysql://root:***@127.0.0.1:3306/mysql' \\
    python scripts/setup_test_mysql.py --password "$CI_TEST_PW" --no-show-url

退出码：0 成功 / 1 失败（权限不足、账号验证不通过）
★ 默认**不打印密码**；``--show-url`` 才会输出完整连接串（给操作者复制用）。
"""

from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import sys
from urllib.parse import urlsplit

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

# 本地：从 backend/.env 取 admin 连接串（与 database.py 同一手法）。
# CI：没有 .env，连接串来自 workflow 的 env（MYSQL_ADMIN_URL）。
try:
    from dotenv import load_dotenv

    load_dotenv(str(_BACKEND / ".env"))
except Exception:  # noqa: BLE001 - 缺 python-dotenv 时按环境变量走
    pass

from sqlalchemy import text  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402
from sqlalchemy.pool import NullPool  # noqa: E402

DEFAULT_DB = "career_ci_test"
DEFAULT_USER = "career_ci"
PROBE_GRANT_DB = r"career\_%\_probe\_%"

_PASSED = 0
_FAILED = 0


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [ok]   " if cond else "  [FAIL] ") + name
          + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def _redact(url: str) -> str:
    """``scheme://user:pw@host/db`` → ``scheme://user:***@host/db``。"""
    if "@" not in url:
        return url
    head, tail = url.split("://", 1)[0], url.split("://", 1)[1]
    cred, hostpart = tail.rsplit("@", 1)
    user = cred.split(":", 1)[0]
    return f"{head}://{user}:***@{hostpart}"


def _build_url(base: str, *, user: str, password: str, db: str) -> str:
    """按 base 的主机/端口拼一个新连接串（用于专用账号）。"""
    parts = urlsplit(base)
    host = parts.hostname or "127.0.0.1"
    port = parts.port or 3306
    return (
        f"mysql+aiomysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
    )


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _escape_db_pattern(name: str) -> str:
    """把库名里的 ``_`` / ``%`` 转义成字面量（MySQL 库级授权里它们是通配符）。"""
    return name.replace("\\", "\\\\").replace("_", r"\_").replace("%", r"\%")


async def _run(admin_url: str, db: str, user: str, password: str) -> int:
    if any(ch in password for ch in ("'", "\\")):
        print("[FAIL] 测试口令不能含单引号或反斜杠（避免 DDL 转义歧义）")
        return 1

    print("=" * 74)
    print("真实 MySQL 集成测试 · 测试库与专用账号准备")
    print("=" * 74)
    print(f"  admin 连接（已脱敏）：{_redact(admin_url)}")
    print(f"  测试库：{db}    专用账号：{user}@'%'")

    engine = create_async_engine(admin_url, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(
                f"CREATE DATABASE IF NOT EXISTS {_quote_ident(db)} "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            ))
            await conn.execute(text(
                f"CREATE USER IF NOT EXISTS '{user}'@'%' IDENTIFIED BY '{password}'"
            ))
            # 幂等：口令可能被改过，显式对齐
            await conn.execute(text(
                f"ALTER USER '{user}'@'%' IDENTIFIED BY '{password}'"
            ))
            await conn.execute(text(
                f"GRANT ALL PRIVILEGES ON {_quote_ident(_escape_db_pattern(db))}.* "
                f"TO '{user}'@'%'"
            ))
            await conn.execute(text(
                f"GRANT ALL PRIVILEGES ON {_quote_ident(PROBE_GRANT_DB)}.* "
                f"TO '{user}'@'%'"
            ))
            await conn.execute(text("FLUSH PRIVILEGES"))
    except Exception as exc:  # noqa: BLE001
        print(f"[FAIL] 准备失败（{type(exc).__name__}）：{str(exc).splitlines()[0][:160]}")
        print("       需要具备 CREATE USER / GRANT 权限的账号；"
              "CI 用 root，本地可用 .env 里的账号。")
        await engine.dispose()
        return 1
    finally:
        await engine.dispose()

    print("\n[1] 专用账号的最小权限验证（这一步证明它碰不到别的库）")
    test_url = _build_url(admin_url, user=user, password=password, db=db)

    async def _try(url: str, sql: str):
        eng = create_async_engine(url, poolclass=NullPool)
        try:
            async with eng.begin() as conn:
                await conn.execute(text(sql))
            return "OK"
        except Exception as exc:  # noqa: BLE001
            return type(exc).__name__
        finally:
            await eng.dispose()

    probe = "career_selftest_probe_0_0"
    _check("[1.1] 能以专用账号连上测试库",
           await _try(test_url, "SELECT 1") == "OK")
    _check("[1.2] 能建 probe 库（运行器需要）",
           await _try(test_url, f"CREATE DATABASE {_quote_ident(probe)}") == "OK")
    _check("[1.3] 能删 probe 库（运行器清理需要）",
           await _try(test_url, f"DROP DATABASE IF EXISTS {_quote_ident(probe)}") == "OK")

    admin_db = (urlsplit(admin_url).path or "/").lstrip("/") or "mysql"
    if admin_db != db:
        _check(f"[1.4] ★ 连不上 admin 库 `{admin_db}`（越权被 MySQL 拒绝）",
               await _try(_build_url(admin_url, user=user, password=password, db=admin_db),
                          "SELECT 1") != "OK")

    print("\n[2] 复制下面这行到你的 shell（★ 含口令，别提交进仓库）")
    if _no_show_url:
        print(f"  export DATABASE_URL='{_redact(test_url)}'   # 口令见你传入的参数")
    else:
        print(f"  export DATABASE_URL='{test_url}'")

    print(f"\n结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    return 0 if _FAILED == 0 else 1


def main() -> int:
    global _no_show_url
    parser = argparse.ArgumentParser(
        description="创建真实 MySQL 集成测试用的测试库与专用账号")
    parser.add_argument("--admin-url", default="",
                        help="具备 CREATE USER 权限的连接串（默认取 MYSQL_ADMIN_URL 或 DATABASE_URL）")
    parser.add_argument("--db", default=DEFAULT_DB, help=f"测试库名（默认 {DEFAULT_DB}）")
    parser.add_argument("--user", default=DEFAULT_USER, help=f"专用账号（默认 {DEFAULT_USER}）")
    parser.add_argument("--password", default=os.getenv("MYSQL_TEST_PASSWORD", ""),
                        help="专用账号口令（默认取 MYSQL_TEST_PASSWORD）")
    parser.add_argument("--show-url", action="store_true",
                        help="输出**含口令**的完整 DATABASE_URL（给操作者复制）")
    args = parser.parse_args()

    _no_show_url = not args.show_url

    admin_url = (args.admin_url or os.getenv("MYSQL_ADMIN_URL", "")
                 or os.getenv("DATABASE_URL", "")).strip()
    if not admin_url:
        print("[FAIL] 缺少 admin 连接串：用 --admin-url 或设置 MYSQL_ADMIN_URL / DATABASE_URL")
        return 1
    if not admin_url.startswith("mysql"):
        print("[FAIL] admin 连接串不是 MySQL")
        return 1
    if not args.password:
        print("[FAIL] 缺少测试账号口令：用 --password 或设置 MYSQL_TEST_PASSWORD")
        return 1

    return asyncio.run(_run(admin_url, args.db, args.user, args.password))


_no_show_url = True

if __name__ == "__main__":
    sys.exit(main())
