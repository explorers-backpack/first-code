# -*- coding: utf-8 -*-
"""AI 模拟面试 · API 层自检（HTTP 全链路）

无需 pytest，直接运行：
    python backend/tests/test_interview_api.py

与 ``test_interview_lifecycle.py`` 的区别：本脚本走**真实的 FastAPI HTTP 栈**
（httpx ASGITransport），因此会一并验证：
- 路由注册与路径参数校验
- ``Depends(get_current_user)`` 鉴权（401 分支）
- ``response_model`` 出参校验（字段缺失会在此暴露为 500）
- 业务异常到 HTTP 状态码的映射（400 / 404 / 422）

数据库使用 SQLite 内存库，通过 ``dependency_overrides`` 注入，
不触碰本机 MySQL，也不触发 lifespan 的建表逻辑。
"""

import asyncio
import os
import pathlib
import sys

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import main  # noqa: E402
import models  # noqa: E402,F401
from database import Base, get_db  # noqa: E402
from deps import get_current_user  # noqa: E402
from models import Job, User  # noqa: E402

API = "/api/interview"
ANSWER_TEXT = (
    "首先，我负责过订单中台的重构。当时系统 QPS 只有 800，我把单体服务拆成 8 个微服务，"
    "使用 Python 与 MySQL，引入 Redis 做缓存、Kafka 做异步解耦。具体来说，我权衡了"
    "拆分粒度与运维成本。结果是 QPS 提升到 5000，响应时间从 1200ms 降到 80ms。"
)

_PASSED = 0
_FAILED = 0


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (f"  -> {detail}" if detail and not cond else ""))
    return cond


async def run() -> bool:
    print("=" * 68)
    print("AI 模拟面试 · API 层自检（HTTP 全链路）")
    print("=" * 68)

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # ---- 种子数据 ----
    async with session_factory() as db:
        user = User(username="api_user", email="api@example.com", password_hash="x", role="user")
        job = Job(
            job_name="后端开发工程师",
            salary="20-35K",
            edu_require="本科",
            major_require="不限",
            skills="Python,MySQL,Redis,Kafka,Docker",
            duty="负责后端服务的设计与开发",
            city="深圳",
            industry="互联网",
        )
        db.add_all([user, job])
        await db.commit()
        await db.refresh(user)
        await db.refresh(job)
        user_id, job_id = user.id, job.id

    # ---- 依赖注入：把 get_db 指向测试库 ----
    async def override_get_db():
        async with session_factory() as session:
            yield session

    main.app.dependency_overrides[get_db] = override_get_db

    transport = ASGITransport(app=main.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # ---------------- [1] 鉴权 ----------------
        print("\n[1] 鉴权（未登录应 401）")
        r = await client.post(f"{API}/create", json={"job_id": job_id})
        _check("无 token 创建会话返回 401", r.status_code == 401, f"{r.status_code} {r.text[:80]}")
        r = await client.get(f"{API}/1")
        _check("无 token 查询会话返回 401", r.status_code == 401, str(r.status_code))
        r = await client.get(f"{API}/1/report")
        _check("无 token 读报告返回 401", r.status_code == 401, str(r.status_code))

        # 注入登录态（模拟已登录用户）
        async def override_get_current_user():
            return {"user_id": user_id, "email": "api@example.com", "role": "user"}

        main.app.dependency_overrides[get_current_user] = override_get_current_user

        # ---------------- [2] 创建会话 ----------------
        print("\n[2] POST /create")
        r = await client.post(
            f"{API}/create",
            json={"job_id": job_id, "difficulty": "mid", "interview_type": "comprehensive", "total_questions": 5},
        )
        _check("状态码 200", r.status_code == 200, f"{r.status_code} {r.text[:200]}")
        body = r.json()
        _check("返回 session 对象", "session" in body)
        session_id = body["session"]["id"]
        _check("初始状态 created", body["session"]["status"] == "created", str(body["session"]["status"]))
        _check("回传 job_name", body.get("job_name") == "后端开发工程师", str(body.get("job_name")))

        print("\n  -- 入参校验（422）--")
        r = await client.post(f"{API}/create", json={"interview_type": "unknown"})
        _check("非法 interview_type 返回 422", r.status_code == 422, str(r.status_code))
        r = await client.post(f"{API}/create", json={"total_questions": 0})
        _check("total_questions 越界返回 422", r.status_code == 422, str(r.status_code))
        r = await client.post(f"{API}/create", json={"job_id": 0})
        _check("job_id 小于 1 返回 422", r.status_code == 422, str(r.status_code))

        print("\n  -- 业务校验（400）--")
        r = await client.post(f"{API}/create", json={"job_id": 999999})
        _check("不存在的岗位返回 400", r.status_code == 400, f"{r.status_code} {r.text[:120]}")
        _check("错误信息可读", "不存在" in r.json().get("detail", ""), r.text[:120])

        # ---------------- [3] 开始面试 ----------------
        print("\n[3] POST /{session_id}/start")
        r = await client.post(f"{API}/{session_id}/start")
        _check("状态码 200", r.status_code == 200, f"{r.status_code} {r.text[:200]}")
        body = r.json()
        _check("状态变为 ongoing", body["session"]["status"] == "ongoing", str(body["session"]["status"]))
        _check("返回第一题", body["question"] is not None)
        _check("第一题为 intro", body["question"]["question_type"] == "intro", str(body["question"]["question_type"]))
        _check("expected_points 已序列化", isinstance(body["question"]["expected_points"], list))

        r = await client.post(f"{API}/{session_id}/start")
        _check("重复 start 返回 400", r.status_code == 400, str(r.status_code))

        # ---------------- [4] 查询会话 ----------------
        print("\n[4] GET /{session_id}")
        r = await client.get(f"{API}/{session_id}")
        _check("状态码 200", r.status_code == 200, f"{r.status_code} {r.text[:200]}")
        body = r.json()
        _check("questions 数量正确", len(body["questions"]) == 5, str(len(body["questions"])))
        _check("answers 初始为空", body["answers"] == [])
        _check("answered_count 为 0", body["answered_count"] == 0)

        r = await client.get(f"{API}/999999")
        _check("不存在的会话返回 404", r.status_code == 404, str(r.status_code))

        # ---------------- [5] 获取当前题 ----------------
        print("\n[5] GET /{session_id}/question")
        r = await client.get(f"{API}/{session_id}/question")
        _check("状态码 200", r.status_code == 200, f"{r.status_code} {r.text[:200]}")
        body = r.json()
        _check("返回第 1 题", body["question_no"] == 1, str(body["question_no"]))
        _check("answered 为 False", body["answered"] is False)
        _check("all_answered 为 False", body["all_answered"] is False)

        # ---------------- [6] 提交回答 ----------------
        print("\n[6] POST /{session_id}/answer")
        r = await client.post(f"{API}/{session_id}/answer", json={"answer_text": ANSWER_TEXT})
        _check("状态码 200", r.status_code == 200, f"{r.status_code} {r.text[:200]}")
        body = r.json()
        _check("返回评分", isinstance(body["answer"]["score"], int), str(body["answer"]["score"]))
        _check("返回 feedback", bool(body["answer"]["feedback"]))
        _check("audio_url 为空（预留字段）", body["answer"]["audio_url"] is None)
        _check("返回下一题", body["next_question"] is not None)
        _check("all_answered 为 False", body["all_answered"] is False)
        print(f"        得分 {body['answer']['score']}，下一题「{body['next_question']['topic']}」")

        r = await client.post(f"{API}/{session_id}/answer", json={"answer_text": ""})
        _check("空回答返回 422", r.status_code == 422, str(r.status_code))

        # ---------------- [7] 完成全部题目 ----------------
        print("\n[7] 完成全部题目")
        for _ in range(20):
            snap = (await client.get(f"{API}/{session_id}/question")).json()
            if snap["all_answered"]:
                break
            await client.post(f"{API}/{session_id}/answer", json={"answer_text": ANSWER_TEXT})

        r = await client.get(f"{API}/{session_id}/question")
        body = r.json()
        _check("全部作答后 all_answered 为 True", body["all_answered"] is True)
        _check("question 为 None", body["question"] is None)

        r = await client.post(f"{API}/{session_id}/answer", json={"answer_text": "再答一次"})
        _check("超量作答返回 400", r.status_code == 400, f"{r.status_code} {r.text[:120]}")

        # ---------------- [8] 结束面试 ----------------
        print("\n[8] POST /{session_id}/end")
        r = await client.post(f"{API}/{session_id}/end")
        _check("状态码 200", r.status_code == 200, f"{r.status_code} {r.text[:200]}")
        body = r.json()
        _check("状态变为 finished", body["session"]["status"] == "finished", str(body["session"]["status"]))
        report = body["report"]
        for field in (
            "total_score", "technical_score", "project_score", "logic_score",
            "expression_score", "communication_score", "adaptability_score", "job_match_score",
        ):
            _check(f"报告含 {field}", report.get(field) is not None, str(report.get(field)))
        _check("strengths 为数组", isinstance(report["strengths"], list) and len(report["strengths"]) > 0)
        _check("weaknesses 为数组", isinstance(report["weaknesses"], list) and len(report["weaknesses"]) > 0)
        _check("suggestions 为文本", isinstance(report["suggestions"], str) and len(report["suggestions"]) > 0)
        print(f"        总分 {report['total_score']}（技术 {report['technical_score']} / 岗位匹配 {report['job_match_score']}）")

        # ---------------- [9] 读取报告 ----------------
        print("\n[9] GET /{session_id}/report")
        r = await client.get(f"{API}/{session_id}/report")
        _check("状态码 200", r.status_code == 200, f"{r.status_code} {r.text[:200]}")
        _check("总分一致", r.json()["total_score"] == report["total_score"])

        # ---------------- [10] 未开始/未结束的状态拦截 ----------------
        print("\n[10] 状态机拦截（HTTP 层）")
        r = await client.post(f"{API}/create", json={"job_id": job_id, "total_questions": 3})
        new_id = r.json()["session"]["id"]
        r = await client.post(f"{API}/{new_id}/end")
        _check("created 状态 end 返回 400", r.status_code == 400, str(r.status_code))
        r = await client.get(f"{API}/{new_id}/report")
        _check("无报告时读报告返回 404", r.status_code == 404, str(r.status_code))
        r = await client.get(f"{API}/{new_id}/question")
        _check("未开始时取题返回 200 且带提示", r.status_code == 200 and r.json()["question"] is None)
        _check("提示文案可读", "start" in r.json().get("message", ""), r.json().get("message", ""))
        r = await client.post(f"{API}/{new_id}/answer", json={"answer_text": "x"})
        _check("未开始作答返回 400", r.status_code == 400, str(r.status_code))

        # ---------------- [11] OpenAPI 文档 ----------------
        print("\n[11] OpenAPI 文档")
        r = await client.get("/openapi.json")
        _check("openapi.json 可生成", r.status_code == 200, str(r.status_code))
        paths = r.json().get("paths", {})
        for path in (
            f"{API}/create",
            f"{API}/{{session_id}}/start",
            f"{API}/{{session_id}}",
            f"{API}/{{session_id}}/question",
            f"{API}/{{session_id}}/answer",
            f"{API}/{{session_id}}/end",
            f"{API}/{{session_id}}/report",
        ):
            _check(f"文档含 {path}", path in paths)
        _check("既有路由未被破坏（/api/chat）", "/api/chat" in paths)
        _check("既有路由未被破坏（/api/resume/analyze）", "/api/resume/analyze" in paths)

    # ---- 清理 ----
    main.app.dependency_overrides.clear()
    await engine.dispose()

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
