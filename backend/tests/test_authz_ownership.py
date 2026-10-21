# -*- coding: utf-8 -*-
"""资源归属与权限边界 · 自检（越权 / 过期 Token / 管理员降级）

无需 pytest，直接运行：
    python backend/tests/test_authz_ownership.py

覆盖：
[1] 面试会话越权：改 session_id 访问他人会话 → **404**（不泄露存在性）
    - 详情 / 当前题目 / 提交回答 / 结束 / 报告 五个入口逐一取证
[2] 越权写路径同样被拦：他人的会话**一行数据都不被改动**
[3] 过期 Token → 401（不是 200，也不是 500）
[4] 已撤销 Token → 401（logout 立即生效）
[5] 管理员降级：会话里的 role 快照是 admin，但 User 表已降为 user ⇒ **403**
    （证明权限实时查库，不信任登录时快照）
[6] 知识库写接口：普通用户 → 403；未登录 → 401（读接口登录即可）

数据库使用**临时文件** SQLite（NullPool），不触碰本机 MySQL。
``AVATAR_PROVIDER`` 由 ``regression_env`` 钉成空串 ⇒ 数字人走 Mock，零联网。
"""

import asyncio
import os
import pathlib
import shutil
import sys
import tempfile
from datetime import datetime, timedelta

_TMP_DIR = tempfile.mkdtemp(prefix="career_ai_authz_test_")
_DB_PATH = pathlib.Path(_TMP_DIR) / "authz_test.db"

os.environ.setdefault("DATABASE_URL", f"sqlite+aiosqlite:///{_DB_PATH.as_posix()}")

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))
sys.path.insert(0, str(_BACKEND / "tests"))

import regression_env  # noqa: E402,F401

from sqlalchemy import select, update  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.pool import NullPool  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

import main  # noqa: E402
import models  # noqa: E402,F401
from database import Base  # noqa: E402
from models import InterviewSession, Resume, User, UserSession  # noqa: E402

DB_URL = f"sqlite+aiosqlite:///{_DB_PATH.as_posix()}"
OWNER_TOKEN = "authz-owner-token"
OTHER_TOKEN = "authz-other-token"
EXPIRED_TOKEN = "authz-expired-token"
REVOKED_TOKEN = "authz-revoked-token"
ADMIN_TOKEN = "authz-admin-token"

# 两份**可区分**的简历正文：用于证明「归属过滤真的在过滤」而不是恒返回空。
OWNER_RESUME_TEXT = "本人简历：3 年后端经验，熟悉 Python 与 Redis。"
OTHER_RESUME_TEXT = "他人简历：5 年经验，精通 Kafka 与 MySQL。"

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


def _prepare_db():
    """建表 + 两个用户 / 两条面试会话 / 四种 token（正常 / 过期 / 撤销 / admin）。"""

    async def _run():
        engine = create_async_engine(DB_URL, poolclass=NullPool)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as db:
            owner = User(username="authz_owner", email="authz_owner@example.com",
                         password_hash="x", role="user")
            other = User(username="authz_other", email="authz_other@example.com",
                         password_hash="x", role="user")
            admin = User(username="authz_admin", email="authz_admin@example.com",
                         password_hash="x", role="admin")
            db.add_all([owner, other, admin])
            await db.commit()
            await db.refresh(owner)
            await db.refresh(other)
            await db.refresh(admin)

            now = datetime.utcnow()
            db.add_all([
                UserSession(user_id=owner.id, token=OWNER_TOKEN, email=owner.email,
                            role="user", created_at=now,
                            expires_at=now + timedelta(days=7)),
                UserSession(user_id=other.id, token=OTHER_TOKEN, email=other.email,
                            role="user", created_at=now,
                            expires_at=now + timedelta(days=7)),
                UserSession(user_id=owner.id, token=EXPIRED_TOKEN, email=owner.email,
                            role="user", created_at=now - timedelta(days=8),
                            expires_at=now - timedelta(days=1)),
                UserSession(user_id=owner.id, token=REVOKED_TOKEN, email=owner.email,
                            role="user", created_at=now,
                            expires_at=now + timedelta(days=7), revoked_at=now),
                # ★ 会话快照 role=admin（模拟「登录时是管理员」）
                UserSession(user_id=admin.id, token=ADMIN_TOKEN, email=admin.email,
                            role="admin", created_at=now,
                            expires_at=now + timedelta(days=7)),
            ])

            owner_session = InterviewSession(
                user_id=owner.id, interview_type="comprehensive", difficulty="mid",
                duration=30, interview_mode="text", status="ongoing",
                total_questions=3, current_question_no=0,
            )
            other_session = InterviewSession(
                user_id=other.id, interview_type="comprehensive", difficulty="mid",
                duration=30, interview_mode="text", status="ongoing",
                total_questions=3, current_question_no=0,
            )
            db.add_all([owner_session, other_session])

            # ★ 简历归属用例的数据：两人各一份简历。
            # ``Resume.user_id`` 是真实列（nullable=False），所以「简历属于谁」
            # 是可判定的——这正是 resume_id 越权面的前提。
            own_resume = Resume(user_id=owner.id, filename="own.pdf",
                                content=OWNER_RESUME_TEXT)
            other_resume = Resume(user_id=other.id, filename="other.pdf",
                                  content=OTHER_RESUME_TEXT)
            db.add_all([own_resume, other_resume])
            await db.commit()
            await db.refresh(own_resume)
            await db.refresh(other_resume)

            await db.refresh(owner_session)
            await db.refresh(other_session)
            ids = (owner.id, other.id, admin.id, owner_session.id, other_session.id,
                   own_resume.id, other_resume.id)
        await engine.dispose()
        return ids

    return asyncio.run(_run())


def _set_role(user_id: int, role: str) -> None:
    async def _run():
        engine = create_async_engine(DB_URL, poolclass=NullPool)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as db:
            await db.execute(update(User).where(User.id == user_id).values(role=role))
            await db.commit()
        await engine.dispose()

    asyncio.run(_run())


def _count_other_answers(other_sid: int) -> int:
    """他人的作答行数（用于证明越权写没有落库）。"""

    async def _run():
        engine = create_async_engine(DB_URL, poolclass=NullPool)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as db:
            from models import InterviewAnswer, InterviewQuestion

            q_ids = select(InterviewQuestion.id).where(
                InterviewQuestion.session_id == other_sid
            )
            rows = await db.execute(
                select(InterviewAnswer).where(InterviewAnswer.question_id.in_(q_ids))
            )
            n = len(list(rows.scalars().all()))
        await engine.dispose()
        return n

    return asyncio.run(_run())


def _load_skills(resume_id: int, user_id: int):
    """直接调 service 的读路径，取「归属过滤后」的技能列表。

    这是简历越权的**第二道闸**：即使库里已有历史脏行（会话挂着他人简历），
    读路径也必须返回空列表，而不是把他人简历正文抽成技能。
    """

    async def _run():
        engine = create_async_engine(DB_URL, poolclass=NullPool)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as db:
            from services import interview_service

            out = await interview_service._load_resume_skills(db, resume_id, user_id)
        await engine.dispose()
        return out

    return asyncio.run(_run())


def run() -> bool:
    owner_id, other_id, admin_id, owner_sid, other_sid, own_resume_id, other_resume_id = _prepare_db()

    owner_h = {"Authorization": f"Bearer {OWNER_TOKEN}"}
    expired_h = {"Authorization": f"Bearer {EXPIRED_TOKEN}"}
    revoked_h = {"Authorization": f"Bearer {REVOKED_TOKEN}"}
    admin_h = {"Authorization": f"Bearer {ADMIN_TOKEN}"}

    with TestClient(main.app) as client:
        # ---------------- [0] 基线：本人访问自己的会话正常 ----------------
        print("\n[0] 基线：本人访问自己的会话")
        r = client.get(f"/api/interview/{owner_sid}", headers=owner_h)
        _check("本人详情 → 200", r.status_code == 200, f"{r.status_code} {r.text[:120]}")

        # ---------------- [1] 越权读 ----------------
        print("\n[1] 面试会话越权：改 session_id 访问他人会话")
        r = client.get(f"/api/interview/{other_sid}", headers=owner_h)
        _check("★ 他人会话详情 → 404", r.status_code == 404, f"{r.status_code} {r.text[:120]}")
        r = client.get(f"/api/interview/{other_sid}/question", headers=owner_h)
        _check("★ 他人当前题目 → 404", r.status_code == 404, f"{r.status_code} {r.text[:120]}")
        r = client.get(f"/api/interview/{other_sid}/report", headers=owner_h)
        _check("★ 他人报告 → 404", r.status_code == 404, f"{r.status_code} {r.text[:120]}")

        # ---------------- [2] 越权写 ----------------
        print("\n[2] 越权写路径：他人的会话不被改动")
        before = _count_other_answers(other_sid)
        r = client.post(
            f"/api/interview/{other_sid}/answer",
            json={"answer_text": "越权写入尝试"},
            headers=owner_h,
        )
        _check("★ 向他人会话提交回答 → 404", r.status_code == 404, f"{r.status_code} {r.text[:120]}")
        r = client.post(f"/api/interview/{other_sid}/end", headers=owner_h)
        _check("★ 结束他人会话 → 404", r.status_code == 404, f"{r.status_code} {r.text[:120]}")
        after = _count_other_answers(other_sid)
        _check("  └ 他人会话的作答行数未变（没有落库）", after == before, f"{before} -> {after}")

        # ---------------- [3] 过期 Token ----------------
        print("\n[3] 过期 Token")
        r = client.get(f"/api/interview/{owner_sid}", headers=expired_h)
        _check("★ 过期 token → 401", r.status_code == 401, f"{r.status_code} {r.text[:120]}")
        r = client.get("/api/auth/me", headers=expired_h)
        _check("  └ /api/auth/me 同样 401", r.status_code == 401, str(r.status_code))

        # ---------------- [4] 已撤销 Token ----------------
        print("\n[4] 已撤销 Token")
        r = client.get(f"/api/interview/{owner_sid}", headers=revoked_h)
        _check("★ 已撤销 token → 401", r.status_code == 401, f"{r.status_code} {r.text[:120]}")

        # ---------------- [5] 管理员降级 ----------------
        print("\n[5] 管理员降级：不信任登录时快照")
        r = client.get("/api/admin/user-logs", headers=admin_h)
        _check("降级前：admin token → 200", r.status_code == 200, f"{r.status_code} {r.text[:120]}")
        _set_role(admin_id, "user")  # 后台把该账号降为普通用户（会话快照仍是 admin）
        r = client.get("/api/admin/user-logs", headers=admin_h)
        _check("★ 降级后：同一 token → 403（权限实时查库）",
               r.status_code == 403, f"{r.status_code} {r.text[:120]}")
        _set_role(admin_id, "admin")  # 还原，避免影响后续用例

        # ---------------- [6] 知识库写权限 ----------------
        print("\n[6] 知识库写接口：仅 admin")
        r = client.post("/api/knowledge/documents", json={
            "title": "越权导入", "content": "x", "category": "technical", "source": ""
        }, headers=owner_h)
        _check("★ 普通用户导入 → 403", r.status_code == 403, f"{r.status_code} {r.text[:120]}")
        r = client.delete("/api/knowledge/documents/1", headers=owner_h)
        _check("★ 普通用户删除 → 403", r.status_code == 403, f"{r.status_code} {r.text[:120]}")
        r = client.post("/api/knowledge/documents", json={
            "title": "x", "content": "x", "category": "technical", "source": ""
        })
        _check("未登录导入 → 401", r.status_code == 401, str(r.status_code))
        r = client.get("/api/knowledge/documents", headers=owner_h)
        _check("普通用户读列表 → 200（知识库是共享资源）",
               r.status_code == 200, f"{r.status_code} {r.text[:120]}")

        # ---------------- [7] 简历归属（resume_id 越权） ----------------
        print("\n[7] 简历归属：resume_id 不能借用他人简历")
        base = {"interview_type": "comprehensive", "difficulty": "mid",
                "duration": 30, "total_questions": 3, "mode": "text"}

        r = client.post("/api/interview/create",
                        json={**base, "resume_id": other_resume_id}, headers=owner_h)
        _check("★ 挂**他人**简历创建会话 → 400",
               r.status_code == 400, f"{r.status_code} {r.text[:120]}")
        _check("  └ 文案与「不存在」一致（不泄露他人简历是否存在）",
               "不存在" in r.text and "权限" not in r.text, r.text[:160])

        r = client.post("/api/interview/create",
                        json={**base, "resume_id": 999999}, headers=owner_h)
        _check("  └ 不存在的 id → 同样 400（不可区分）",
               r.status_code == 400, f"{r.status_code} {r.text[:120]}")

        r = client.post("/api/interview/create",
                        json={**base, "resume_id": own_resume_id}, headers=owner_h)
        _check("本人简历创建会话 → 200（正常路径未被误伤）",
               r.status_code == 200, f"{r.status_code} {r.text[:120]}")

        # 读路径取证：三组输入必须给出**不同**结果，否则断言是恒真的
        foreign = _load_skills(other_resume_id, owner_id)
        _check("★ 读路径：他人 resume_id + 自己 user_id → 空",
               foreign == [], str(foreign))
        as_owner = _load_skills(other_resume_id, other_id)
        _check("  └ 同一份简历由**属主**读 → 非空（证明过滤真的在过滤）",
               as_owner == ["Kafka", "MySQL"], str(as_owner))
        own = _load_skills(own_resume_id, owner_id)
        _check("  └ 本人简历本人读 → 非空且内容不同",
               own == ["Python", "Redis"], str(own))

    try:
        shutil.rmtree(_TMP_DIR, ignore_errors=False)
        _check("临时 SQLite 目录已清理", not pathlib.Path(_TMP_DIR).exists())
    except Exception as exc:  # noqa: BLE001
        print(f"  [WARN] 临时目录未能删除（不影响结果）：{_TMP_DIR} -> {exc}")

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
