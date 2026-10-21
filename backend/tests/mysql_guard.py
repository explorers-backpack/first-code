# -*- coding: utf-8 -*-
"""真实 MySQL 运行器的**目标库守卫**：拒绝把集成测试指向生产 / 开发库。

为什么需要
----------
``tests/*_run.py`` 会 ``CREATE DATABASE`` 建 probe 库、``create_all`` 建表、
``DROP DATABASE`` 清理。它们对**自己的** probe 库是安全的，但：

1. 每个运行器都会对 ``DATABASE_URL`` 指向的库做一次**只读结构检查**——
   如果那个库是生产库，检查本身无害，但它意味着"测试跑在了生产服务器上"；
2. 更危险的是**误配置**：有人把 ``DATABASE_URL`` 指向生产库，然后手工跑一个
   运行器。今天这些运行器不写业务表，但这是**约定**，不是**保证**。

所以判据放在最前面：**库名看起来不像测试库，就拒绝运行**。

两道防线（都要有）
------------------
========================================  ==================================
防线                                       作用
========================================  ==================================
**账号权限**（``scripts/setup_test_mysql``）  专用账号只能碰 ``<测试库>`` 与
                                          ``career_*_probe_*``；连生产库会被
                                          MySQL 直接 1044 拒绝——**物理上碰不到**
**库名守卫**（本模块）                      即使误用了 root，也会在跑第一个
                                          用例之前停下
========================================  ==================================

★ 刻意**不提供**"跳过守卫"的开关。一个 ``ALLOW_PROD=1`` 之类的后门，
迟早会出现在某人的 shell 历史里。需要换库名就改 :data:`TEST_DB_PATTERN`。
"""

from __future__ import annotations

import re

#: 允许作为集成测试目标的库名。
#:
#: - ``career_ci`` / ``career_ci_test``：CI 与本地文档使用；
#: - ``career_test*`` / ``test_*`` / ``*_test`` / ``*_ci``：常见命名习惯。
TEST_DB_PATTERN = re.compile(
    r"^(career_ci|career_ci_test|career_test.*|test_.*|.*_test|.*_ci)$",
    re.IGNORECASE,
)


def database_name(url: str) -> str:
    """从连接串里取出库名（``.../dbname?charset=...`` ⇒ ``dbname``）。"""
    tail = url.split("://", 1)[-1]
    path = tail.split("/", 1)[-1] if "/" in tail else ""
    name = path.split("?", 1)[0]
    return name.strip()


def unsafe_reason(url: str) -> str:
    """安全则返回 ``""``，否则返回一句**给人看**的拒绝理由。"""
    if not url.startswith("mysql"):
        return "DATABASE_URL 不是 MySQL —— 集成测试必须连真实 MySQL"
    name = database_name(url)
    if not name:
        return "DATABASE_URL 未指定库名 —— 无法确认目标库是不是测试库"
    if TEST_DB_PATTERN.match(name):
        return ""
    return (
        f"拒绝运行：目标库 `{name}` 看起来**不是测试库**。"
        "集成测试会建库建表并 DROP，不允许指向生产 / 开发库。"
        "请用 backend/scripts/setup_test_mysql.py 创建专用测试库与账号，"
        "或把 DATABASE_URL 指到 `career_ci_test`。"
    )


def is_test_database(url: str) -> bool:
    return unsafe_reason(url) == ""


__all__ = ["TEST_DB_PATTERN", "database_name", "is_test_database", "unsafe_reason"]
