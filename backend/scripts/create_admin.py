# -*- coding: utf-8 -*-
"""创建 / 校验管理员账号（幂等，可重复执行）

背景
----
项目此前**没有用户初始化机制**：``init_mysql.py`` 只建表并 seed 岗位数据，不创建用户；
旧 ``fix_admin.py`` 是硬编码 ``WHERE username='admin'`` 的一次性脚本，且直连 pymysql、
绕过了项目的 ``DATABASE_URL`` 配置。本脚本提供可重复执行、可审计的管理员创建入口。

行为（幂等）
------------
- 账号不存在                -> 创建，``role`` 固定为 ``admin``
- 已存在且 ``role=admin``    -> 跳过（**不修改密码**，除非显式 ``--reset-password``）
- 已存在但 ``role != admin`` -> **明确报错并以退出码 1 结束，绝不静默提权**
  （确需提权请显式加 ``--promote-existing``）
- 目标用户名被其它邮箱占用   -> 明确报错退出，不创建

安全约定
--------
- 密码使用**项目现有哈希机制** ``main._hash_password``（sha256），与注册/登录完全一致
- ``role`` 由本脚本固定为 ``admin``，**不接受任何外部输入**
- 默认密码 ``admin123`` 仅用于本地开发；生产环境请用 ``ADMIN_PASSWORD`` 环境变量
  （或 ``--password``，但命令行会留在 shell 历史中，故推荐环境变量）

用法
----
    cd backend
    python scripts/create_admin.py --dry-run          # 仅检查现状，不写库
    python scripts/create_admin.py                    # 创建（已存在则幂等跳过）
    ADMIN_PASSWORD='强密码' python scripts/create_admin.py --reset-password
    python scripts/create_admin.py --promote-existing # 显式将同名普通账号提权
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select  # noqa: E402

import main  # noqa: E402

DEFAULT_EMAIL = "admin@career.ai"
DEFAULT_USERNAME = "admin"
DEFAULT_PASSWORD = "admin123"
ADMIN_ROLE = "admin"


async def run(args) -> int:
    email = args.email.strip()
    username = args.username.strip()
    password = args.password

    print("=" * 72)
    print("创建 / 校验管理员账号" + ("（DRY RUN，不写库）" if args.dry_run else ""))
    print("=" * 72)
    print(f"目标 email    : {email}")
    print(f"目标 username : {username}")
    print(f"目标 role     : {ADMIN_ROLE}  （由脚本固定，不接受外部输入）")
    print(f"密码哈希机制  : sha256（main._hash_password，与注册/登录一致）")
    if password == DEFAULT_PASSWORD:
        print("密码          : 使用默认开发密码 admin123"
              "（生产环境请用 ADMIN_PASSWORD 环境变量覆盖）")

    async with main.async_session() as session:
        by_email = (
            await session.execute(select(main.User).where(main.User.email == email))
        ).scalar_one_or_none()
        by_name = (
            await session.execute(select(main.User).where(main.User.username == username))
        ).scalar_one_or_none()

        # ---------- 情况 A：邮箱不存在，但目标用户名已被别人占用 ----------
        if by_email is None and by_name is not None:
            print(f"\n[冲突] username={username!r} 已被 email={by_name.email!r} 占用，无法创建。")
            print("       请用 --username 指定其它用户名后重试。")
            return 1

        # ---------- 情况 B：账号不存在 -> 创建 ----------
        if by_email is None:
            print(f"\n[创建] 未找到 email={email!r}，将新建管理员账号。")
            if args.dry_run:
                print("       DRY RUN：未写库。")
                return 0

            user = main.User(
                username=username,
                email=email,
                password_hash=main._hash_password(password),
                role=ADMIN_ROLE,
            )
            session.add(user)
            await session.commit()
            await session.refresh(user)
            print(f"       [OK] 已创建 id={user.id} username={user.username!r} "
                  f"email={user.email!r} role={user.role!r}")
            print("       可用该邮箱 + 密码调用 POST /api/auth/login 获取 token。")
            return 0

        # ---------- 情况 C：账号已存在 ----------
        existing = by_email
        print(f"\n[已存在] id={existing.id} username={existing.username!r} "
              f"email={existing.email!r} role={existing.role!r}")
        password_matches = existing.password_hash == main._hash_password(password)
        print(f"       密码与给定值一致: {password_matches}")

        if existing.role == ADMIN_ROLE:
            if password_matches:
                print("       [OK] role 已是 admin 且密码一致 -> 无需变更（幂等跳过）。")
                return 0

            if not args.reset_password:
                print("       [警告] role 已是 admin，但密码与给定值不一致。")
                print("              **未修改密码**（不静默改动凭据）。"
                      "如需重置请显式加 --reset-password。")
                return 0

            if args.dry_run:
                print("       DRY RUN：未写库（本应重置密码）。")
                return 0
            existing.password_hash = main._hash_password(password)
            await session.commit()
            print("       [OK] 已按显式 --reset-password 重置密码。")
            print("              已有会话 token 仍然有效；如需强制失效请另行清理 user_session。")
            return 0

        # role != admin
        if not args.promote_existing:
            print(f"       [拒绝] 该账号 role={existing.role!r}，不是 admin。")
            print("              脚本**不会静默提权普通账号**。若确认要提升，请显式执行：")
            print("                  python scripts/create_admin.py --promote-existing")
            return 1

        if args.dry_run:
            print("       DRY RUN：未写库（本应把 role 提升为 admin）。")
            return 0
        existing.role = ADMIN_ROLE
        await session.commit()
        print("       [OK] 已按显式 --promote-existing 将 role 提升为 admin。")
        print("              注意：权限判定读取的是 user_session.role，"
              "请让该账号**重新登录**以获取 admin 会话。")
        return 0


def main_cli() -> int:
    parser = argparse.ArgumentParser(
        description="创建 / 校验管理员账号（幂等）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--email", default=DEFAULT_EMAIL, help=f"管理员邮箱（默认 {DEFAULT_EMAIL}）")
    parser.add_argument("--username", default=DEFAULT_USERNAME, help=f"管理员用户名（默认 {DEFAULT_USERNAME}）")
    parser.add_argument(
        "--password",
        default=os.getenv("ADMIN_PASSWORD", DEFAULT_PASSWORD),
        help="管理员密码（默认取环境变量 ADMIN_PASSWORD，否则 admin123）",
    )
    parser.add_argument("--dry-run", action="store_true", help="仅检查现状，不写库")
    parser.add_argument(
        "--promote-existing",
        action="store_true",
        help="已存在同名账号但 role 非 admin 时，显式将其提升为 admin（默认拒绝并退出）",
    )
    parser.add_argument(
        "--reset-password",
        action="store_true",
        help="已存在且 role=admin 时，显式重置其密码（默认不改动凭据）",
    )
    args = parser.parse_args()

    async def _runner() -> int:
        # 连接池必须在同一个事件循环内创建并释放，避免跨 loop 报
        # "Event loop is closed"
        try:
            return await run(args)
        finally:
            await main.engine.dispose()

    return asyncio.run(_runner())


if __name__ == "__main__":
    sys.exit(main_cli())
