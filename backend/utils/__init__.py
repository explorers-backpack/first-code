# -*- coding: utf-8 -*-
"""通用工具包（与具体业务无关的基础设施）。

当前内容
--------
``schema_sync``
    轻量级 schema 同步：启动时检查「模型需要的列」在数据库里是否存在，
    缺失则 ``ALTER TABLE ... ADD COLUMN``。**只加列**，不做任何迁移
    （不改类型、不改名、不删列、不清数据）。

为什么单独成包
--------------
补列逻辑与业务无关，属于「启动基础设施」，因此不放进 ``services/``
（那里是业务服务），也不塞进 ``database.py``（那里只负责 engine / Session / Base）。
"""

from utils.schema_sync import (  # noqa: F401
    DESIRED_COLUMNS,
    SchemaSyncError,
    UnsupportedColumnSpecError,
    build_column_ddl,
    ensure_columns,
    missing_columns,
)

__all__ = [
    "DESIRED_COLUMNS",
    "SchemaSyncError",
    "UnsupportedColumnSpecError",
    "build_column_ddl",
    "ensure_columns",
    "missing_columns",
]
