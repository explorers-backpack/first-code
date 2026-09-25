# -*- coding: utf-8 -*-
"""AI 模拟面试 · 交互模式（interview_mode）自检

无需 pytest，直接运行：
    python backend/tests/test_interview_mode.py

覆盖范围
--------
本脚本验证「交互模式」这一新增字段的**全部行为面**：

- **枚举与模型列**：``text`` / ``avatar``、默认值、列可空性与 server_default
- **请求契约**：``{"mode": "..."}`` 与 ``{"interview_mode": "..."}`` 两种键名、
  缺省默认、非法值拒绝
- **落库**：``create_session`` 把模式写进 ``interview_session`` 表
- **回传**：``_session_to_dict`` 与 HTTP 全链路都带 ``interview_mode``
- **只选择交互方式**：两种模式生成的题目计划**完全一致**（不影响出题 / 状态机）
- **已纳入 schema 同步清单**：``interview_session.interview_mode`` 登记在
  ``utils/schema_sync.DESIRED_COLUMNS`` 中

补列机制本身的行为（缺少则新增 / 已存在不重复 ALTER / 表不存在跳过）
由独立套件 ``tests/test_schema_sync.py`` 覆盖。

数据库使用 SQLite 内存库（``StaticPool``），不触碰本机 MySQL。
"""

import asyncio
import os
import pathlib
import sys

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy import select  # noqa: E402
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
from models import (  # noqa: E402
    DEFAULT_INTERVIEW_MODE,
    INTERVIEW_MODES,
    InterviewSession,
    Job,
    User,
)
from schemas.interview import InterviewSessionOut, SessionCreateRequest  # noqa: E402
from services import interview_core, interview_service  # noqa: E402
from utils import schema_sync  # noqa: E402

API = "/api/interview"

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


def _rejects_validation(model, **kwargs) -> bool:
    """Pydantic 是否拒绝了这组入参（用于 422 类断言）。"""
    try:
        model(**kwargs)
    except Exception:  # pydantic.ValidationError
        return True
    return False


async def _rejected(coro, status: int) -> bool:
    """断言协程抛 HTTPException 且状态码匹配。"""
    from fastapi import HTTPException

    try:
        await coro
    except HTTPException as exc:
        return exc.status_code == status
    except Exception:
        return False
    return False


def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed(db: AsyncSession):
    user = User(username="mode_user", email="mode@example.com", password_hash="x", role="user")
    job = Job(
        job_name="后端开发工程师",
        salary="20-35K",
        edu_require="本科",
        major_require="不限",
        skills="Python,MySQL,Redis,Kafka,Docker",
        duty="负责后端服务的设计与开发，参与需求分析、系统设计与性能优化",
        city="深圳",
        industry="互联网",
    )
    db.add_all([user, job])
    await db.commit()
    await db.refresh(user)
    await db.refresh(job)
    return user.id, job.id


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 68)
    print("AI 模拟面试 · 交互模式（interview_mode）自检")
    print("=" * 68)

    # ------------------------------------------------------------
    # [1] 枚举与模型列
    # ------------------------------------------------------------
    print("\n[1] 枚举与模型列")
    _check("INTERVIEW_MODES 恰为 text / avatar", INTERVIEW_MODES == ("text", "avatar"),
           str(INTERVIEW_MODES))
    _check("默认交互模式为 text（向后兼容既有会话）", DEFAULT_INTERVIEW_MODE == "text",
           DEFAULT_INTERVIEW_MODE)

    column = InterviewSession.__table__.c.get("interview_mode")
    _check("InterviewSession 存在 interview_mode 列", column is not None)
    _check("  └ 非空（NOT NULL）", column.nullable is False)
    _check("  └ 长度 20（VARCHAR(20)）", getattr(column.type, "length", None) == 20,
           str(column.type))
    _check("  └ 有 server_default（既有库 ALTER 时历史行有值）",
           column.server_default is not None and str(column.server_default.arg) == "text",
           str(column.server_default))
    _check("  └ 模型层 default 为 text",
           column.default is not None and column.default.arg == "text",
           str(column.default))
    _check("  └ 未加索引（模式不做检索维度）", not column.index)

    # ------------------------------------------------------------
    # [2] 请求契约
    # ------------------------------------------------------------
    print("\n[2] 请求契约（SessionCreateRequest）")
    _check("缺省时 mode == text", SessionCreateRequest().mode == "text",
           SessionCreateRequest().mode)
    _check('{"mode": "text"} 通过', SessionCreateRequest(**{"mode": "text"}).mode == "text")
    _check('{"mode": "avatar"} 通过', SessionCreateRequest(**{"mode": "avatar"}).mode == "avatar")
    _check('{"interview_mode": "avatar"} 亦可（列名别名）',
           SessionCreateRequest(**{"interview_mode": "avatar"}).mode == "avatar")
    _check("mode 字段名对外就是 mode（非 interview_mode）",
           "mode" in SessionCreateRequest.model_json_schema()["properties"])
    _check("非法 mode 被 Pydantic 拒绝（422）",
           _rejects_validation(SessionCreateRequest, mode="video"))
    _check("大小写敏感：TEXT 被拒", _rejects_validation(SessionCreateRequest, mode="TEXT"))
    _check("空字符串被拒", _rejects_validation(SessionCreateRequest, mode=""))
    _check("出参契约含 interview_mode 字段",
           "interview_mode" in InterviewSessionOut.model_fields)
    _check("  └ 出参缺省为 text", InterviewSessionOut.model_fields["interview_mode"].default == "text")

    # ------------------------------------------------------------
    # [3] Service 落库
    # ------------------------------------------------------------
    print("\n[3] Service 落库（create_session → interview_session.interview_mode）")
    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        user_id, job_id = await _seed(db)

        text_res = await interview_service.create_session(
            db, user_id, SessionCreateRequest(job_id=job_id, mode="text", total_questions=3)
        )
        avatar_res = await interview_service.create_session(
            db, user_id, SessionCreateRequest(job_id=job_id, mode="avatar", total_questions=3)
        )
        default_res = await interview_service.create_session(
            db, user_id, SessionCreateRequest(job_id=job_id, total_questions=3)
        )

        text_id = text_res["session"]["id"]
        avatar_id = avatar_res["session"]["id"]
        default_id = default_res["session"]["id"]

        _check("text 会话回传 interview_mode=text",
               text_res["session"]["interview_mode"] == "text",
               str(text_res["session"].get("interview_mode")))
        _check("avatar 会话回传 interview_mode=avatar",
               avatar_res["session"]["interview_mode"] == "avatar",
               str(avatar_res["session"].get("interview_mode")))
        _check("未指定时落库为 text（默认值）",
               default_res["session"]["interview_mode"] == "text",
               str(default_res["session"].get("interview_mode")))

        # 直查数据库，确认不是只改了内存里的 dict
        rows = (await db.execute(
            select(InterviewSession.id, InterviewSession.interview_mode)
        )).all()
        stored = {row.id: row.interview_mode for row in rows}
        _check("DB 中 text 会话 interview_mode == 'text'", stored.get(text_id) == "text",
               str(stored.get(text_id)))
        _check("DB 中 avatar 会话 interview_mode == 'avatar'", stored.get(avatar_id) == "avatar",
               str(stored.get(avatar_id)))
        _check("DB 中缺省会话 interview_mode == 'text'", stored.get(default_id) == "text",
               str(stored.get(default_id)))

        # 归属 / 越权与既有校验不受影响
        # 用 model_construct 绕过 Pydantic 校验，专门验证 service 层的防御性检查
        illegal_mode = SessionCreateRequest.model_construct(
            job_id=job_id,
            resume_id=None,
            interview_type="comprehensive",
            difficulty="mid",
            duration=30,
            total_questions=5,
            mode="hologram",
        )
        _check("非法 mode 直调 service 返回 400",
               await _rejected(interview_service.create_session(db, user_id, illegal_mode), 400))

        illegal_type = SessionCreateRequest.model_construct(
            job_id=job_id,
            resume_id=None,
            interview_type="unknown",
            difficulty="mid",
            duration=30,
            total_questions=5,
            mode="text",
        )
        _check("既有 interview_type 校验仍在（非法 → 400）",
               await _rejected(interview_service.create_session(db, user_id, illegal_type), 400))

        # ------------------------------------------------------------
        # [4] 只选择交互方式：不影响出题与状态机
        # ------------------------------------------------------------
        print("\n[4] 只选择交互方式：不影响出题 / 状态机")
        text_start = await interview_service.start_session(db, user_id, text_id)
        avatar_start = await interview_service.start_session(db, user_id, avatar_id)

        _check("text 与 avatar 的第一题完全相同",
               text_start["question"]["question"] == avatar_start["question"]["question"],
               f"{text_start['question']['question'][:30]} vs {avatar_start['question']['question'][:30]}")
        _check("  └ 题号 / 类型 / 知识点 / 难度一致",
               all(
                   text_start["question"][k] == avatar_start["question"][k]
                   for k in ("question_no", "question_type", "topic", "difficulty")
               ))
        _check("  └ 期望要点一致",
               text_start["question"]["expected_points"]
               == avatar_start["question"]["expected_points"])

        text_detail = await interview_service.get_session_detail(db, user_id, text_id)
        avatar_detail = await interview_service.get_session_detail(db, user_id, avatar_id)
        _check("整场题目计划逐题一致（模式不参与出题）",
               [q["question"] for q in text_detail["questions"]]
               == [q["question"] for q in avatar_detail["questions"]],
               f"{len(text_detail['questions'])} vs {len(avatar_detail['questions'])}")
        _check("状态机推进一致（均为 ongoing / 第 1 题）",
               text_detail["session"]["status"] == avatar_detail["session"]["status"] == "ongoing"
               and text_detail["session"]["current_question_no"]
               == avatar_detail["session"]["current_question_no"] == 1)

        # 规则出题计划本身与模式无关：同一会话配置下两模式计划完全相同
        text_row = (await db.execute(
            select(InterviewSession).where(InterviewSession.id == text_id)
        )).scalar_one()
        avatar_row = (await db.execute(
            select(InterviewSession).where(InterviewSession.id == avatar_id)
        )).scalar_one()
        job = await interview_service._load_job(db, job_id)
        plan_text = interview_core.build_question_plan(text_row, job, [])
        plan_avatar = interview_core.build_question_plan(avatar_row, job, [])
        _check("build_question_plan 输出与模式无关", plan_text == plan_avatar)

        # ------------------------------------------------------------
        # [5] HTTP 全链路
        # ------------------------------------------------------------
        print("\n[5] HTTP 全链路（POST /create 与 GET /{id}）")

    async def override_get_db():
        async with session_factory() as session:
            yield session

    main.app.dependency_overrides[get_db] = override_get_db

    async def override_get_current_user():
        return {"user_id": user_id, "email": "mode@example.com", "role": "user"}

    main.app.dependency_overrides[get_current_user] = override_get_current_user

    transport = ASGITransport(app=main.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post(f"{API}/create", json={"job_id": job_id, "mode": "text"})
        _check("POST /create mode=text → 200", r.status_code == 200, f"{r.status_code} {r.text[:120]}")
        body = r.json()
        _check("  └ 出参带 interview_mode=text",
               body["session"].get("interview_mode") == "text", str(body["session"].get("interview_mode")))
        http_text_id = body["session"]["id"]

        r = await client.post(f"{API}/create", json={"job_id": job_id, "mode": "avatar"})
        _check("POST /create mode=avatar → 200", r.status_code == 200, f"{r.status_code} {r.text[:120]}")
        body = r.json()
        _check("  └ 出参带 interview_mode=avatar",
               body["session"].get("interview_mode") == "avatar", str(body["session"].get("interview_mode")))
        http_avatar_id = body["session"]["id"]

        r = await client.post(f"{API}/create", json={"job_id": job_id})
        _check("POST /create 不传 mode → 默认 text",
               r.status_code == 200 and r.json()["session"]["interview_mode"] == "text",
               f"{r.status_code} {r.text[:120]}")

        r = await client.post(f"{API}/create", json={"job_id": job_id, "interview_mode": "avatar"})
        _check("POST /create 用 interview_mode 键名亦可",
               r.status_code == 200 and r.json()["session"]["interview_mode"] == "avatar",
               f"{r.status_code} {r.text[:120]}")

        r = await client.post(f"{API}/create", json={"job_id": job_id, "mode": "video"})
        _check("POST /create 非法 mode → 422", r.status_code == 422, str(r.status_code))

        r = await client.get(f"{API}/{http_text_id}")
        _check("GET /{id} 回传 interview_mode=text",
               r.status_code == 200 and r.json()["session"]["interview_mode"] == "text",
               f"{r.status_code} {r.text[:120]}")

        r = await client.get(f"{API}/{http_avatar_id}")
        _check("GET /{id} 回传 interview_mode=avatar",
               r.status_code == 200 and r.json()["session"]["interview_mode"] == "avatar",
               f"{r.status_code} {r.text[:120]}")

        r = await client.post(f"{API}/{http_avatar_id}/start")
        _check("avatar 会话可正常 start（模式不阻塞流程）",
               r.status_code == 200 and r.json()["session"]["interview_mode"] == "avatar",
               f"{r.status_code} {r.text[:120]}")

        r = await client.get(f"{API}/{http_avatar_id}/question")
        _check("avatar 会话可正常取题", r.status_code == 200 and r.json()["question"] is not None,
               f"{r.status_code} {r.text[:120]}")

        # 完整跑完一场 avatar 会话：模式必须全程保留，且不影响任何流程
        r = await client.post(
            f"{API}/{http_avatar_id}/answer",
            json={"answer_text": (
                "我负责过订单中台重构，把单体服务拆成 8 个微服务，引入 Redis 做缓存、"
                "Kafka 做异步解耦，QPS 从 800 提升到 5000，响应时间从 1200ms 降到 80ms。"
            )},
        )
        _check("POST /answer 回传 interview_mode=avatar",
               r.status_code == 200 and r.json()["session"]["interview_mode"] == "avatar",
               f"{r.status_code} {r.text[:120]}")
        _check("  └ 作答流程不受模式影响（仍返回下一题）",
               r.json().get("next_question") is not None)

        r = await client.post(f"{API}/{http_avatar_id}/end")
        _check("POST /end 回传 interview_mode=avatar",
               r.status_code == 200 and r.json()["session"]["interview_mode"] == "avatar",
               f"{r.status_code} {r.text[:120]}")
        _check("  └ 报告已生成",
               r.json().get("report", {}).get("total_score") is not None,
               str(r.json().get("report"))[:120])

        r = await client.get(f"{API}/{http_avatar_id}")
        _check("结束后再查询仍为 avatar（已持久化，非内存态）",
               r.status_code == 200 and r.json()["session"]["interview_mode"] == "avatar",
               f"{r.status_code} {r.text[:120]}")
        _check("  └ 会话状态为 finished", r.json()["session"]["status"] == "finished")

    main.app.dependency_overrides.clear()

    # ------------------------------------------------------------
    # [6] 交互模式已纳入 schema 同步清单
    # ------------------------------------------------------------
    # 补列机制本身的行为（缺少则新增 / 已存在不重复 / 表不存在跳过）
    # 由独立套件 tests/test_schema_sync.py 覆盖，这里只验证「已正确登记」。
    print("\n[6] 交互模式已纳入 schema 同步清单（utils/schema_sync.py）")
    spec = schema_sync.DESIRED_COLUMNS.get("interview_session", {}).get("interview_mode")
    _check("DESIRED_COLUMNS 登记了 interview_session.interview_mode", spec is not None,
           str(schema_sync.DESIRED_COLUMNS))
    _check("  └ 与模型列口径一致（VARCHAR(20) / NOT NULL / DEFAULT 'text'）",
           spec == {"type": "VARCHAR(20)", "default": "'text'", "nullable": False}, str(spec))
    _check("  └ 启动补列入口可调用", callable(schema_sync.ensure_columns))
    _check("main.py 的 lifespan 接入的正是该实现（同一对象）",
           main.ensure_columns is schema_sync.ensure_columns)

    await engine.dispose()

    # ------------------------------------------------------------
    # [7] 向后兼容
    # ------------------------------------------------------------
    print("\n[7] 向后兼容")
    _check("_session_to_dict 在未 flush 的对象上也不返回 None",
           interview_service._session_to_dict(
               InterviewSession(user_id=1, status="created", total_questions=1, current_question_no=0)
           )["interview_mode"] == "text")
    _check("既有出参字段全部保留",
           {"id", "user_id", "job_id", "resume_id", "interview_type", "difficulty",
            "duration", "status", "total_questions", "current_question_no",
            "started_at", "ended_at", "created_at"} <= set(
               interview_service._session_to_dict(
                   InterviewSession(user_id=1, status="created", total_questions=1,
                                    current_question_no=0)
               )
           ))
    _check("interview_core 未被本次改动触及（不引用 interview_mode）",
           "interview_mode" not in open(
               pathlib.Path(interview_core.__file__), encoding="utf-8"
           ).read())

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
