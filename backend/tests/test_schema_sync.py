# -*- coding: utf-8 -*-
"""轻量级 schema 同步（utils/schema_sync.py）自检

无需 pytest，直接运行：
    python backend/tests/test_schema_sync.py

覆盖范围
--------
- **配置契约**：``DESIRED_COLUMNS`` 结构、允许的配置键、DDL 渲染、非法配置被拒
- **缺少字段自动新增**：既有表缺列 → 执行 ``ALTER TABLE ADD COLUMN``，
  历史行自动填默认值，且**既有列与既有数据完好无损**
- **已存在字段不重复执行**：重复调用返回空、**一条 DDL 都不再发**
- **表不存在正常跳过**：不报错、不建表
- **只增不改不删**：真实运行中唯一的 DDL 必须是 ``ADD COLUMN``

数据库使用 SQLite 内存库（``StaticPool``），不触碰本机 MySQL。
"""

import asyncio
import os
import pathlib
import sys

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from sqlalchemy import event, inspect, text  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401
from models import InterviewSession  # noqa: E402
from utils import schema_sync  # noqa: E402

# 绝不允许出现在本模块产出 SQL 中的动词（只加列）
_FORBIDDEN_SQL_VERBS = (
    "DROP", "MODIFY", "CHANGE", "ALTER COLUMN", "RENAME",
    "TRUNCATE", "DELETE", "UPDATE", "INSERT",
)

_PASSED = 0
_FAILED = 0


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(
        ("  [PASS] " if cond else "  [FAIL] ")
        + name
        + (f"  -> {detail}" if detail and not cond else "")
    )
    return cond


def _build_engine():
    return create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )


def _record_sql(engine):
    """记录引擎上执行过的全部 SQL 语句（用于断言「只发了一条 DDL」）。"""
    seen = []

    def handler(conn, cursor, statement, parameters, context, executemany):
        seen.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", handler)
    return seen


def _alters(seen):
    return [s for s in seen if "ALTER TABLE" in s.upper()]


def _raises(func, *args) -> bool:
    try:
        func(*args)
    except Exception:  # noqa: BLE001
        return True
    return False


async def _columns(conn, table: str):
    return await conn.run_sync(
        lambda sync_conn: {c["name"]: c for c in inspect(sync_conn).get_columns(table)}
    )


async def _table_exists(conn, table: str) -> bool:
    return await conn.run_sync(lambda sync_conn: inspect(sync_conn).has_table(table))


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 68)
    print("轻量级 schema 同步自检（utils/schema_sync.py）")
    print("=" * 68)

    # ------------------------------------------------------------
    # [1] 配置契约
    # ------------------------------------------------------------
    print("\n[1] 配置契约（DESIRED_COLUMNS）")
    _check("DESIRED_COLUMNS 是 {表: {列: {配置}}} 两层字典",
           isinstance(schema_sync.DESIRED_COLUMNS, dict)
           and all(isinstance(v, dict) for v in schema_sync.DESIRED_COLUMNS.values())
           and all(
               isinstance(spec, dict)
               for cols in schema_sync.DESIRED_COLUMNS.values()
               for spec in cols.values()
           ))
    _check("已声明 interview_session.interview_mode",
           "interview_session" in schema_sync.DESIRED_COLUMNS
           and "interview_mode" in schema_sync.DESIRED_COLUMNS["interview_session"])
    mode_spec = schema_sync.DESIRED_COLUMNS["interview_session"]["interview_mode"]
    _check("  └ type = VARCHAR(20)", mode_spec.get("type") == "VARCHAR(20)", str(mode_spec))
    _check("  └ default = 'text'（SQL 字面量，自带引号）",
           mode_spec.get("default") == "'text'", str(mode_spec))
    _check("  └ nullable = False", mode_spec.get("nullable") is False, str(mode_spec))
    _check("允许的配置键只有 type / default / nullable",
           schema_sync._ALLOWED_SPEC_KEYS == ("type", "default", "nullable"),
           str(schema_sync._ALLOWED_SPEC_KEYS))

    _check("DDL 渲染：type + NOT NULL + DEFAULT",
           schema_sync.build_column_ddl(
               {"type": "VARCHAR(20)", "default": "'text'", "nullable": False}
           ) == "VARCHAR(20) NOT NULL DEFAULT 'text'",
           schema_sync.build_column_ddl(mode_spec))
    _check("DDL 渲染：省略 nullable 视为可空",
           schema_sync.build_column_ddl({"type": "INT"}) == "INT")
    _check("DDL 渲染：省略 default 不生成 DEFAULT 子句",
           schema_sync.build_column_ddl({"type": "INT", "nullable": False}) == "INT NOT NULL")

    _check("非法配置键被拒（如 drop / modify）",
           _raises(schema_sync.build_column_ddl, {"type": "INT", "drop": True}))
    _check("缺少 type 被拒", _raises(schema_sync.build_column_ddl, {"default": "1"}))
    _check("非法配置异常继承 ValueError（领域基类 + 内建异常）",
           issubclass(schema_sync.UnsupportedColumnSpecError, schema_sync.SchemaSyncError)
           and issubclass(schema_sync.UnsupportedColumnSpecError, ValueError))

    _check("ALTER 模板只含 ADD COLUMN",
           "ADD COLUMN" in schema_sync._ALTER_TEMPLATE
           and schema_sync._ALTER_TEMPLATE.strip().upper().startswith("ALTER TABLE"))
    _check("  └ 模板不含任何删 / 改动词",
           not any(v in schema_sync._ALTER_TEMPLATE.upper() for v in _FORBIDDEN_SQL_VERBS),
           schema_sync._ALTER_TEMPLATE)

    # ------------------------------------------------------------
    # [2] 缺少字段 → 自动新增
    # ------------------------------------------------------------
    print("\n[2] 缺少字段 → 自动新增（模拟升级上来的旧库）")
    engine = _build_engine()
    seen = _record_sql(engine)

    async with engine.begin() as conn:
        # 旧库：interview_session 已存在、有历史数据，但没有 interview_mode 列
        await conn.execute(text(
            "CREATE TABLE interview_session ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " user_id INTEGER NOT NULL,"
            " status VARCHAR(20) NOT NULL DEFAULT 'created',"
            " note VARCHAR(50)"
            ")"
        ))
        await conn.execute(text(
            "INSERT INTO interview_session (user_id, status, note)"
            " VALUES (1, 'finished', 'keep-me')"
        ))

        before = await _columns(conn, "interview_session")
        _check("改造前：表存在但缺 interview_mode",
               "interview_mode" not in before and "status" in before,
               str(sorted(before)))
        alters_before = len(_alters(seen))
        _check("  └ 检查阶段是只读的（尚未发 DDL）", alters_before == 0, str(alters_before))

        added = await schema_sync.ensure_columns(conn)
        _check("ensure_columns 返回实际新增的列",
               added == ["interview_session.interview_mode"], str(added))

        alters = _alters(seen)
        _check("恰好执行了 1 条 ALTER TABLE", len(alters) == 1, str(alters))
        _check("  └ 语句形状正确（MySQL 语法）",
               alters and alters[0].strip()
               == "ALTER TABLE interview_session ADD COLUMN interview_mode "
                  "VARCHAR(20) NOT NULL DEFAULT 'text'",
               str(alters[0] if alters else None))
        _check("  └ 只含 ADD COLUMN，无删 / 改动词",
               not any(v in alters[0].upper() for v in _FORBIDDEN_SQL_VERBS)
               if alters else False)

        after = await _columns(conn, "interview_session")
        _check("列已新增", "interview_mode" in after, str(sorted(after)))
        _check("  └ 类型为 VARCHAR(20)", str(after["interview_mode"]["type"]).upper() == "VARCHAR(20)",
               str(after["interview_mode"]["type"]))
        _check("  └ NOT NULL", after["interview_mode"]["nullable"] is False,
               str(after["interview_mode"]["nullable"]))
        _check("  └ DEFAULT 'text'",
               str(after["interview_mode"]["default"]).strip("'") == "text",
               str(after["interview_mode"]["default"]))

        # 既有数据与既有列必须完好
        _check("既有列一个不少",
               {"id", "user_id", "status", "note"} <= set(after), str(sorted(after)))
        row = (await conn.execute(text(
            "SELECT user_id, status, note, interview_mode FROM interview_session WHERE id = 1"
        ))).first()
        _check("历史行仍在（未被清空）", row is not None)
        _check("  └ 既有字段值未变",
               row is not None and row[0] == 1 and row[1] == "finished" and row[2] == "keep-me",
               str(row))
        _check("  └ 新列自动填充默认值 text",
               row is not None and row[3] == "text", str(row[3] if row else None))

        # ------------------------------------------------------------
        # [3] 已存在字段 → 不重复执行（幂等）
        # ------------------------------------------------------------
        print("\n[3] 已存在字段 → 不重复执行（幂等）")
        again = await schema_sync.ensure_columns(conn)
        _check("二次调用返回空列表", again == [], str(again))
        _check("  └ ALTER 总数仍为 1（未重复 ALTER）", len(_alters(seen)) == 1,
               str(_alters(seen)))

        third = await schema_sync.ensure_columns(conn)
        _check("三次调用依旧为空", third == [], str(third))
        _check("  └ ALTER 总数仍为 1", len(_alters(seen)) == 1, str(_alters(seen)))

        pending = await conn.run_sync(schema_sync.missing_columns)
        _check("missing_columns 对齐全的表返回空", pending == [], str(pending))

    # ------------------------------------------------------------
    # [4] 表不存在 → 跳过、不报错、不建表
    # ------------------------------------------------------------
    print("\n[4] 表不存在 → 跳过（不报错、不建表）")
    fresh_engine = _build_engine()
    fresh_seen = _record_sql(fresh_engine)
    async with fresh_engine.begin() as conn:
        ok = True
        try:
            result = await schema_sync.ensure_columns(conn)
        except Exception as exc:  # noqa: BLE001
            ok = False
            result = None
            print("      ", type(exc).__name__, exc)
        _check("不报错", ok)
        _check("  └ 返回空列表", result == [], str(result))
        _check("  └ 未创建 interview_session（建表归 create_all 管）",
               not await _table_exists(conn, "interview_session"))
        _check("  └ 一条 DDL 都没发", _alters(fresh_seen) == [], str(fresh_seen))

    # ------------------------------------------------------------
    # [5] 只增不改不删：全部配置渲染出的语句都是 ADD COLUMN
    # ------------------------------------------------------------
    print("\n[5] 只增不改不删（护栏）")
    rendered = [
        (table, name, schema_sync.build_column_ddl(spec))
        for table, columns in schema_sync.DESIRED_COLUMNS.items()
        for name, spec in columns.items()
    ]
    _check("至少声明了 1 列", len(rendered) >= 1, str(rendered))
    _check("所有配置都能渲染成非空 DDL",
           all(ddl.strip() for _, _, ddl in rendered), str(rendered))
    _check("任何配置的 DDL 都不含删 / 改动词",
           not any(v in ddl.upper() for v in _FORBIDDEN_SQL_VERBS for _, _, ddl in rendered),
           str(rendered))
    _check("DESIRED_COLUMNS 无法表达「删列 / 改列」意图（配置键已收窄）",
           all(
               set(spec) <= set(schema_sync._ALLOWED_SPEC_KEYS)
               for columns in schema_sync.DESIRED_COLUMNS.values()
               for spec in columns.values()
           ))

    # ------------------------------------------------------------
    # [6] 与 ORM 模型口径一致
    # ------------------------------------------------------------
    print("\n[6] 与 ORM 模型口径一致")
    orm_column = InterviewSession.__table__.c["interview_mode"]
    _check("类型一致（VARCHAR(20)）",
           str(orm_column.type).upper() == mode_spec["type"].upper(),
           f"orm={orm_column.type} spec={mode_spec['type']}")
    _check("可空性一致（均为 NOT NULL）",
           orm_column.nullable is False and mode_spec["nullable"] is False)
    _check("默认值一致（均为 text）",
           str(orm_column.server_default.arg).strip("'") == mode_spec["default"].strip("'"),
           f"orm={orm_column.server_default.arg} spec={mode_spec['default']}")

    await engine.dispose()
    await fresh_engine.dispose()

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
