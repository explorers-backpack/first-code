# -*- coding: utf-8 -*-
"""轻量级 schema 同步 —— **只加列**，不做迁移。

解决什么问题
------------
``main.py`` 的 lifespan 用 ``Base.metadata.create_all`` 建表：

- **新表**会被自动创建 ✅
- **既有表新增的列不会被补上** ❌（必须人工 ``ALTER TABLE``）

于是「给模型加一个字段」会变成线上事故：代码已经按新字段查询，
而运行库还是旧结构，任何相关查询都报
``Unknown column 'xxx' in 'field list'``。

本模块在应用启动时补齐这个缺口。

启动流程
--------
::

    应用启动（main.py lifespan）
        ↓
    schema_sync.ensure_columns(conn)
        ↓
    SQLAlchemy Inspector 检查表结构与列
        ↓
    缺失的列 → ALTER TABLE <表> ADD COLUMN <列> <类型> [NOT NULL] [DEFAULT ...]

能力边界（重要）
----------------
**只允许新增字段。** 本模块在结构上无法表达其他 DDL：

- 没有删除字段的代码路径（绝不生成 ``DROP COLUMN``）
- 没有修改字段的代码路径（绝不生成 ``MODIFY`` / ``CHANGE`` / ``ALTER COLUMN``）
- 不触碰数据（绝不生成 ``DELETE`` / ``TRUNCATE`` / ``UPDATE``）
- 列已存在则**完全跳过**，因此重复启动不会重复 ``ALTER``（幂等）
- 表不存在则跳过（交给 ``create_all``），本模块**不建表**、不报错

改列名 / 改类型 / 删列这类有数据风险的演进，请人工评估后执行——
不适合放在应用启动时自动跑。

配置
----
``DESIRED_COLUMNS``：``{表名: {列名: {type, default, nullable}}}``。
``type`` 必填；``default`` 的值就是 **SQL 字面量**（字符串要自带引号，
如 ``"'text'"``）；``nullable`` 省略视为可空。
配置键只允许这三个，多传会抛 ``UnsupportedColumnSpecError``——
这也是「只加列」的护栏：任何改列 / 删列的诉求都写不进来。
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import AsyncConnection


# ============================================================
# 异常（按项目规范：领域基类 + 最贴近的内建异常）
# ============================================================
class SchemaSyncError(Exception):
    """schema_sync 的领域基类。"""


class UnsupportedColumnSpecError(SchemaSyncError, ValueError):
    """``DESIRED_COLUMNS`` 中出现了不支持的配置键或缺少 ``type``。"""


# ============================================================
# 一、期望的列配置
# ============================================================
# 允许的配置键——刻意收窄到「描述一个列」所需的最小集合。
# 没有 drop / rename / modify 之类的键，所以「只加列」不是靠自觉，而是靠结构。
_ALLOWED_SPEC_KEYS: Tuple[str, ...] = ("type", "default", "nullable")

DESIRED_COLUMNS: Dict[str, Dict[str, Dict[str, Any]]] = {
    "interview_session": {
        # 交互模式：text=文字面试，avatar=数字人视频面试（只选择交互方式）
        # 与 models.interview.InterviewSession.interview_mode 的口径保持一致：
        #   String(20) / nullable=False / server_default="text"
        "interview_mode": {
            "type": "VARCHAR(20)",
            "default": "'text'",  # 值即 SQL 字面量，字符串自带引号
            "nullable": False,
        },
    },
}

# 表名 / 列名均为本文件硬编码常量，不接受任何外部输入，
# 因此拼接 DDL 不构成注入面。
_ALTER_TEMPLATE = "ALTER TABLE {table} ADD COLUMN {column} {ddl}"


# ============================================================
# 二、DDL 生成
# ============================================================
def build_column_ddl(spec: Dict[str, Any]) -> str:
    """把列配置渲染成 ``ADD COLUMN`` 用的类型片段。

    例：``{"type": "VARCHAR(20)", "default": "'text'", "nullable": False}``
    → ``"VARCHAR(20) NOT NULL DEFAULT 'text'"``

    配置键非法或缺 ``type`` 时抛 ``UnsupportedColumnSpecError``。
    """
    unknown = [key for key in spec if key not in _ALLOWED_SPEC_KEYS]
    if unknown:
        raise UnsupportedColumnSpecError(
            f"不支持的列配置键 {unknown}，只允许 {list(_ALLOWED_SPEC_KEYS)}"
            "（本模块只加列，不支持改列 / 删列）"
        )

    column_type = spec.get("type")
    if not column_type or not isinstance(column_type, str):
        raise UnsupportedColumnSpecError(f"列配置缺少必填项 type：{spec!r}")

    parts: List[str] = [column_type.strip()]
    if spec.get("nullable") is False:
        parts.append("NOT NULL")
    default = spec.get("default")
    if default is not None:
        parts.append(f"DEFAULT {default}")
    return " ".join(parts)


# ============================================================
# 三、检查（同步实现，供 AsyncConnection.run_sync 调用）
# ============================================================
def missing_columns(sync_conn) -> List[Tuple[str, str, str]]:
    """比对「期望列」与「库中实际列」，返回 ``[(表名, 列名, 列 DDL)]``。

    - 只包含**确实缺失**的列
    - 表不存在则整体跳过（交给 ``create_all``）
    - 只读：仅调用 Inspector，不执行任何 DDL
    """
    inspector = inspect(sync_conn)
    plan: List[Tuple[str, str, str]] = []
    for table, columns in DESIRED_COLUMNS.items():
        if not inspector.has_table(table):
            continue
        existing = {col["name"] for col in inspector.get_columns(table)}
        for name, spec in columns.items():
            if name not in existing:
                plan.append((table, name, build_column_ddl(spec)))
    return plan


# ============================================================
# 四、执行（唯一会写 DDL 的地方）
# ============================================================
async def ensure_columns(conn: AsyncConnection) -> List[str]:
    """补齐缺失的列，返回本次**实际新增**的 ``"表名.列名"`` 列表。

    幂等：列已存在时不生成任何 DDL，第二次调用返回空列表。
    表不存在时跳过且不报错。

    建议与 ``create_all`` 复用同一个连接 / 事务：:

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await ensure_columns(conn)
    """
    pending = await conn.run_sync(missing_columns)
    for table, name, ddl in pending:
        await conn.execute(text(_ALTER_TEMPLATE.format(table=table, column=name, ddl=ddl)))
    return [f"{table}.{name}" for table, name, _ in pending]


__all__ = [
    "DESIRED_COLUMNS",
    "SchemaSyncError",
    "UnsupportedColumnSpecError",
    "build_column_ddl",
    "ensure_columns",
    "missing_columns",
]
