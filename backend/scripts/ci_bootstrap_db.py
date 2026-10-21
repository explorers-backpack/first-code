# -*- coding: utf-8 -*-
"""CI 专用：在**真实 MySQL** 上执行初始化 / 正式迁移（幂等）。

为什么单独成脚本
----------------
CI 要证明的不是"表能建出来"，而是**部署路径能建出来**——也就是应用启动时
真正执行的那两步：

.. code-block:: text

    main.py lifespan
        ├── Base.metadata.create_all      # 建**新表**
        └── schema_sync.ensure_columns    # 给**既有表**补新列（只加列）

如果把这两步复制进 workflow 的 ``run:`` 里，CI 验证的就是一份**平行实现**：
它绿了不代表应用启动时能绿。写成一个脚本，CI 调用的就是生产同一条代码路径。

``--twice`` 连跑两遍，用于验证**幂等**（重复启动不会重复 ALTER、不会报错）。

用法::

    cd backend
    DATABASE_URL=mysql+aiomysql://... python scripts/ci_bootstrap_db.py
    DATABASE_URL=... python scripts/ci_bootstrap_db.py --twice

退出码：0 成功 / 1 失败（连接失败、迁移异常、结构不符）
★ 绝不打印 ``DATABASE_URL`` 的账号密码部分。
"""

from __future__ import annotations

import argparse
import asyncio
import pathlib
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))


def _redact(url: str) -> str:
    return url.rsplit("@", 1)[-1] if "@" in url else url


#: CI 必须看到的结构（表 -> 关键列）。缺任何一项都判失败。
REQUIRED = {
    "user": {"id", "username", "role"},
    "user_session": {"id", "user_id", "token", "expires_at", "revoked_at"},
    "interview_session": {"id", "user_id", "status"},
    "ws_ticket": {"ticket_hash", "user_id", "session_id",
                  "created_at", "expires_at", "used_at", "auth_session_id"},
    "ws_revocation": {"id", "user_id", "auth_session_id", "reason", "created_at"},
    "login_failure": {"id", "key_hash", "failed_at"},
}


async def _bootstrap(pass_no: int) -> int:
    from sqlalchemy import inspect as sa_inspect
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool

    import models  # noqa: F401 - 让所有 ORM 模型注册到 Base.metadata
    from database import DATABASE_URL, Base
    from utils import schema_sync

    engine = create_async_engine(DATABASE_URL, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            added = await schema_sync.ensure_columns(conn)
        async with engine.connect() as conn:
            facts = await conn.run_sync(lambda c: {
                table: {col["name"] for col in sa_inspect(c).get_columns(table)}
                if sa_inspect(c).has_table(table) else None
                for table in REQUIRED
            })
    finally:
        await engine.dispose()

    print(f"\n[PASS {pass_no}] 目标库：{_redact(DATABASE_URL)}")
    print(f"  create_all + ensure_columns 完成；本次补列 {len(added)} 条：{added}")

    ok = True
    for table, need in REQUIRED.items():
        got = facts.get(table)
        if got is None:
            print(f"  [FAIL] 表 {table} 不存在")
            ok = False
            continue
        missing = need - got
        if missing:
            print(f"  [FAIL] 表 {table} 缺列：{sorted(missing)}")
            ok = False
        else:
            print(f"  [ok]   {table}：{len(got)} 列，关键列齐全")

    if pass_no > 1 and added:
        print(f"  [FAIL] 第二遍仍在补列（迁移不幂等）：{added}")
        ok = False
    if pass_no > 1 and not added:
        print("  [ok]   第二遍未补任何列（幂等）")

    return 0 if ok else 1


async def _main(twice: bool) -> int:
    try:
        from database import DATABASE_URL
    except RuntimeError as exc:
        print(f"[FAIL] 数据库配置不可用：{exc}")
        return 1

    if not DATABASE_URL.startswith("mysql"):
        print("[FAIL] DATABASE_URL 不是 MySQL —— CI 必须用真实 MySQL")
        return 1

    print("=" * 74)
    print("CI 数据库初始化 / 正式迁移（与 main.py lifespan 同一条路径）")
    print("=" * 74)

    rc = await _bootstrap(1)
    if rc != 0:
        return rc
    if twice:
        rc = await _bootstrap(2)
        if rc != 0:
            return rc

    print("\n[OK] 数据库初始化完成")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="CI 数据库初始化 / 迁移")
    parser.add_argument("--twice", action="store_true",
                        help="连跑两遍，验证幂等（CI 用这个）")
    args = parser.parse_args()
    return asyncio.run(_main(args.twice))


if __name__ == "__main__":
    sys.exit(main())
