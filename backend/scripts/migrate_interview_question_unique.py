# -*- coding: utf-8 -*-
"""InterviewQuestion (session_id, question_no) 唯一约束迁移脚本。

用法
----
--dry-run    只查重，不执行 DDL
--yes        确认执行（无重复时加约束；有重复则 exit 1）
--drop       回滚：删除约束
--dedupe     遇到重复时自动清理（仅删除无 interview_answer 引用的行）

退出码
------
0  成功 / 已存在 / dry-run 无异常
1  有重复或执行失败
2  参数错误
3  重复行带作答引用，拒绝自动清理
"""

from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import sys

# 直接运行本脚本时 sys.path[0] 是 scripts/ 目录，backend/ 不在路径上
# ⇒ 必须把 backend 根目录插到最前面，否则 `from database import ...` 会
# ModuleNotFoundError（与 scripts/audit_resume_ownership.py 同处理）。
_BACKEND_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

# ★ 这里**不能** setdefault("DATABASE_URL", "")：空串会让 database.py 的
#   load_dotenv(override=False) 认为「键已存在」而放弃从 .env 读取，于是
#   DATABASE_URL 恒为空、脚本必然 RuntimeError。本脚本是**面向真实库**的
#   迁移工具，一律走 .env；缺配置就让 database.py 抛出明确错误。
from sqlalchemy import inspect, select, text  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402

from database import DATABASE_URL  # noqa: E402
from models import InterviewAnswer, InterviewQuestion  # noqa: E402

CONSTRAINT_NAME = "uq_interview_question_session_no"


def _redact(url: str) -> str:
    if not url:
        return "<未配置>"
    if "@" in url:
        return url.rsplit("@", 1)[-1]
    return url


async def main() -> int:
    parser = argparse.ArgumentParser(description="InterviewQuestion 唯一约束迁移")
    parser.add_argument("--dry-run", action="store_true", help="只查重，不执行")
    parser.add_argument("--yes", action="store_true", help="确认执行")
    parser.add_argument("--drop", action="store_true", help="删除约束")
    parser.add_argument("--dedupe", action="store_true", help="允许自动清理无作答的重复行")
    args = parser.parse_args()

    if not DATABASE_URL:
        print(f"[ERROR] DATABASE_URL 未配置（当前：{_redact(DATABASE_URL)}）")
        return 2

    engine = create_async_engine(DATABASE_URL)
    try:
        async with engine.begin() as conn:
            # ★ 所有 Inspector 调用都必须在**同一个** run_sync 里完成。
            #   把 Inspector 对象带出 run_sync 再调用（has_table /
            #   get_unique_constraints）会在非 greenlet 上下文里触发 IO，
            #   直接抛 MissingGreenlet（"greenlet_spawn has not been called"）。
            def _inspect_facts(sync_conn):
                insp = inspect(sync_conn)
                if not insp.has_table("interview_question"):
                    return {"exists": False, "unique": set()}
                names = {
                    uq["name"] for uq in insp.get_unique_constraints("interview_question")
                }
                # MySQL 的 UNIQUE 约束落地为唯一索引，两种入口都收一遍，
                # 避免「约束其实在、但 get_unique_constraints 没报」导致重复 ALTER。
                for idx in insp.get_indexes("interview_question"):
                    if idx.get("unique") and idx.get("name"):
                        names.add(idx["name"])
                return {"exists": True, "unique": names}

            facts = await conn.run_sync(_inspect_facts)
            if not facts["exists"]:
                print("[SKIP] interview_question 表不存在")
                return 0

            existing_names = facts["unique"]

            if args.drop:
                if CONSTRAINT_NAME not in existing_names:
                    print("[SKIP] 约束不存在，无需删除")
                    return 0
                await conn.execute(
                    text(f"ALTER TABLE interview_question DROP INDEX {CONSTRAINT_NAME}")
                )
                print(f"[OK] 已删除约束 {CONSTRAINT_NAME}")
                return 0

            if CONSTRAINT_NAME in existing_names:
                print(f"[SKIP] 约束 {CONSTRAINT_NAME} 已存在")
                return 0

            # 查重
            dupes = await conn.execute(
                text(
                    "SELECT session_id, question_no, COUNT(*) c, MIN(id) keep_id "
                    "FROM interview_question GROUP BY session_id, question_no HAVING c > 1"
                )
            )
            rows = dupes.all()
            if rows:
                print(f"[ERROR] 发现 {len(rows)} 组重复 (session_id, question_no)：")
                for r in rows[:20]:
                    print(f"  session_id={r.session_id} question_no={r.question_no} count={r.c} keep_id={r.keep_id}")
                if len(rows) > 20:
                    print(f"  ... 还有 {len(rows)-20} 组")

                if args.dedupe:
                    # 检查重复行是否有作答引用
                    for r in rows:
                        refs = await conn.execute(
                            select(InterviewAnswer.id).where(
                                InterviewAnswer.question_id.in_(
                                    select(InterviewQuestion.id).where(
                                        InterviewQuestion.session_id == r.session_id,
                                        InterviewQuestion.question_no == r.question_no,
                                    )
                                )
                            )
                        )
                        if refs.scalar_one_or_none() is not None:
                            print("[ERROR] 重复行中存在 interview_answer 引用，拒绝自动清理")
                            return 3
                    # 安全删除：保留每组 keep_id，删除其余
                    for r in rows:
                        await conn.execute(
                            text(
                                "DELETE FROM interview_question "
                                "WHERE session_id = :sid AND question_no = :qno AND id != :kid"
                            ).bindparams(sid=r.session_id, qno=r.question_no, kid=r.keep_id)
                        )
                        print(f"[DEDUPE] 清理 session_id={r.session_id} question_no={r.question_no}，保留 id={r.keep_id}")
                else:
                    print("[HINT] 使用 --dedupe 可自动清理无作答引用的重复行")
                    return 1

            if args.dry_run:
                print("[DRY-RUN] 无重复，可安全添加约束")
                return 0

            if not args.yes:
                print("[ERROR] 有重复或需要执行时，请显式加 --yes 或 --dry-run")
                return 2

            await conn.execute(
                text(
                    "ALTER TABLE interview_question ADD CONSTRAINT "
                    f"{CONSTRAINT_NAME} UNIQUE (session_id, question_no)"
                )
            )
            print(f"[OK] 已添加约束 {CONSTRAINT_NAME}")
            return 0
    finally:
        await engine.dispose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
