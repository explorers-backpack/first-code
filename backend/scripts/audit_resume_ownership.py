# -*- coding: utf-8 -*-
"""简历归属审计：找出「会话挂着的简历不属于会话属主」的历史行。

背景
----
``interview_session.resume_id`` 只是一个外键，而 ``resume.user_id`` 才是归属。
加固前 ``create_session`` 只校验「简历 id 存在」，不校验归属 ⇒ 可能存在
「A 的会话挂着 B 的简历」这类历史脏行。加固后新写入已被拦截，**历史行不会自动修复**。

本脚本默认**只读**：只报告，不改任何数据。
（符合「无法安全自动迁移的地方先给方案，不破坏现有数据」的原则。）

用法
----
python scripts/audit_resume_ownership.py                # 只审计
python scripts/audit_resume_ownership.py --json out.json # 审计 + 导出明细
python scripts/audit_resume_ownership.py --fix --yes     # 先导出备份，再把 resume_id 置 NULL

``--fix`` 的语义是**解除引用**，不删除简历行本身；执行前必须先有 ``--json`` 备份
（未提供时脚本自动写到 ``resume_ownership_backup_<时间戳>.json``）。

退出码
------
0  无脏行（或 --fix 已执行成功）
1  发现脏行（只读模式下）
2  参数 / 环境错误
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
from datetime import datetime

os.environ.setdefault("DATABASE_URL", "")

# 支持 `python scripts/audit_resume_ownership.py` 直接运行：
# 此时 sys.path[0] 是 scripts/，需要把 backend 根目录补进来才能 import database/models。
_BACKEND_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

from sqlalchemy import select, update  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine  # noqa: E402

from database import DATABASE_URL  # noqa: E402
from models import InterviewSession, Resume  # noqa: E402


def _redact(url: str) -> str:
    if not url:
        return "<未配置>"
    if "@" in url:
        return url.rsplit("@", 1)[-1]
    return url


async def main() -> int:
    parser = argparse.ArgumentParser(description="简历归属审计（默认只读）")
    parser.add_argument("--json", dest="json_path", default="",
                        help="把脏行明细导出到该 JSON 文件")
    parser.add_argument("--fix", action="store_true",
                        help="把脏行的 resume_id 置 NULL（需同时给 --yes）")
    parser.add_argument("--yes", action="store_true", help="确认执行 --fix")
    args = parser.parse_args()

    if args.fix and not args.yes:
        print("[ERROR] --fix 属破坏性操作，必须同时提供 --yes")
        return 2
    if not DATABASE_URL:
        print(f"[ERROR] DATABASE_URL 未配置（当前：{_redact(DATABASE_URL)}）")
        return 2

    engine = create_async_engine(DATABASE_URL)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as db:
            # outer join：resume 行不存在时 resume_owner 为 None（悬挂外键）
            stmt = (
                select(
                    InterviewSession.id,
                    InterviewSession.user_id,
                    InterviewSession.resume_id,
                    Resume.user_id.label("resume_owner"),
                )
                .outerjoin(Resume, Resume.id == InterviewSession.resume_id)
                .where(InterviewSession.resume_id.isnot(None))
                .order_by(InterviewSession.id)
            )
            rows = (await db.execute(stmt)).all()

        offending = [
            {
                "session_id": int(r[0]),
                "session_owner": int(r[1]),
                "resume_id": int(r[2]),
                "resume_owner": None if r[3] is None else int(r[3]),
                "reason": "简历不存在（悬挂外键）" if r[3] is None else "简历属于他人",
            }
            for r in rows
            if r[3] is None or int(r[3]) != int(r[1])
        ]

        print(f"扫描会话（挂了简历的）：{len(rows)} 条")
        print(f"发现越权/悬挂引用    ：{len(offending)} 条")

        if offending:
            for item in offending:
                print(
                    f"  - session {item['session_id']} (owner={item['session_owner']}) "
                    f"-> resume {item['resume_id']} "
                    f"(owner={item['resume_owner']}) [{item['reason']}]"
                )

            # 备份：--fix 前必须有落盘证据
            if args.json_path or args.fix:
                path = args.json_path or (
                    f"resume_ownership_backup_"
                    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
                with open(path, "w", encoding="utf-8") as fh:
                    json.dump(offending, fh, ensure_ascii=False, indent=2)
                print(f"明细已导出：{path}")

        if args.fix:
            if not offending:
                print("[SKIP] 无脏行，未执行任何写操作")
                return 0
            ids = [item["session_id"] for item in offending]
            async with factory() as db:
                await db.execute(
                    update(InterviewSession)
                    .where(InterviewSession.id.in_(ids))
                    .values(resume_id=None)
                )
                await db.commit()
            print(f"[DONE] 已把 {len(ids)} 条会话的 resume_id 置 NULL（简历行未删除）")
            return 0

        return 1 if offending else 0
    finally:
        await engine.dispose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
