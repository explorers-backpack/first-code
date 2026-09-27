# -*- coding: utf-8 -*-
"""生产路径 RAG 接线自检（缺口①：`/next-question` 路由 + `use_rag` 开关）

无需 pytest，直接运行：
    python backend/tests/test_rag_production_route.py

本套件回答一个问题：**生产 HTTP 路径上到底能不能用上 RAG，而且默认不用**。

覆盖：
[1] 路由与鉴权（路由存在 + 无 token 401）
[2] 默认不检索（`use_rag` 缺省 ⇒ 检索器组装器**一次都没被调用**）
[3] `use_rag=true` 时组装器被调用，且**没给参数就不传参数**（默认值由组装器决定）
[4] `top_k` / `min_score` **真的透传**到组装器
[5] `use_rag=false` 时 `top_k` / `min_score` **不生效**（组装器不被调用）
[6] 端到端：库里有知识 + `use_rag=true` ⇒ Agent 收到的 Prompt **含知识小节**
[7] 对照：同一份库 + `use_rag=false` ⇒ Agent 收到的 Prompt **不含知识小节**
[8] 归属校验：会话不存在 ⇒ **404**（Service 先拦；Core 里的 `session_not_found` 是纵深防御）
[9] 契约与源码守卫（AST）：schema 默认 `False`；路由的 `use_rag` 由**请求**驱动

不触碰本机 MySQL（SQLite 内存库），不调真实星火（替换 `main.spark_api` 为替身）。
"""

import ast
import asyncio
import json
import os
import pathlib
import sys
from typing import Any, Dict, List, Optional

import regression_env  # noqa: E402,F401  钉住离线 Embedding + RAG 阈值（回归不受 .env 影响）
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
from schemas.interview import NextQuestionRequest  # noqa: E402
from services import knowledge_import_pipeline  # noqa: E402
from services import knowledge_rag  # noqa: E402

API = "/api/interview"

#: 知识小节标题的**片段**（只用于断言 Prompt 正文，不是源码守卫 token）
KNOWLEDGE_MARKER = "参考知识"

#: 组装器名（运行时拼出，避免守卫匹配到本行自身）
BUILDER_NAME = "build_vector" + "_retriever"
ROUTE_PATH = "/api/interview/{session_id}/next-question"

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


AGENT_REPLY = json.dumps(
    {
        "question": "请结合你的项目经历，说明索引与缓存设计上的取舍。",
        "question_type": "technical",
        "topic": "综合",
        "difficulty": "mid",
        "expected_points": ["取舍", "量化"],
        "reason": "考察工程判断",
    },
    ensure_ascii=False,
)

DOC = {
    "title": "MySQL 索引设计手册",
    "content": (
        "MySQL 索引选择性与最左前缀原则。复合索引的列顺序决定了能否命中；"
        "回表代价与覆盖索引。缓存一致性采用先更新数据库再删除缓存。"
    ),
    "category": "technical",
    "source": "handbook://mysql/index",
}


class SpySpark:
    """星火替身：记录每次 Prompt，返回固定 JSON（不调真实 API）。"""

    def __init__(self, reply: str = AGENT_REPLY) -> None:
        self.reply = reply
        self.prompts: List[str] = []

    async def chat_async(self, message: str, **_kwargs: Any) -> str:
        self.prompts.append(message)
        return self.reply


class StubRetriever:
    """检索器替身（只实现接口上的 ``retrieve``）。"""

    def __init__(self, chunks: Optional[List[Any]] = None) -> None:
        self.chunks = chunks or []
        self.calls = 0

    async def retrieve(self, job: Any = None, topic: Any = None, context: Any = None):
        self.calls += 1
        return list(self.chunks)


class BuilderSpy:
    """替换 ``knowledge_rag.build_vector_retriever``，记录每次调用的 kwargs。"""

    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, db: Any, **kwargs: Any) -> StubRetriever:
        self.calls.append(dict(kwargs))
        return StubRetriever()


def _route_exists() -> bool:
    for route in main.app.routes:
        if getattr(route, "path", "") == ROUTE_PATH:
            return "POST" in (getattr(route, "methods", set()) or set())
    return False


def _kwargs_of_call(source: str, func_name: str) -> Optional[Dict[str, Any]]:
    """AST：取 ``func_name`` 调用的关键字实参**源码形态**（值为字符串描述）。"""
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
            if name != func_name:
                continue
            out: Dict[str, Any] = {}
            for keyword in node.keywords:
                if keyword.arg is None:
                    out["**"] = ast.unparse(keyword.value)
                else:
                    out[keyword.arg] = ast.unparse(keyword.value)
            return out
    return None


async def run() -> bool:
    print("=" * 68)
    print("生产路径 RAG 接线自检（缺口①：/next-question + use_rag）")
    print("=" * 68)

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    session_factory = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        user = User(
            username="rag_route", email="rag_route@example.com", password_hash="x", role="user"
        )
        job = Job(
            job_name="后端开发工程师",
            salary="20-35K",
            edu_require="本科",
            major_require="不限",
            skills="Python,MySQL,Redis",
            duty="负责后端服务的设计与开发",
            city="深圳",
            industry="互联网",
        )
        db.add_all([user, job])
        await db.commit()
        await db.refresh(user)
        await db.refresh(job)
        user_id, job_id = user.id, job.id

    async def override_get_db():
        async with session_factory() as session:
            yield session

    main.app.dependency_overrides[get_db] = override_get_db

    spark = SpySpark()
    original_spark = main.spark_api
    main.spark_api = spark  # type: ignore[assignment]

    real_builder = knowledge_rag.build_vector_retriever
    spy = BuilderSpy()
    knowledge_rag.build_vector_retriever = spy  # type: ignore[assignment]

    transport = ASGITransport(app=main.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # ---------------- [1] 路由与鉴权 ----------------
        print("\n[1] 路由与鉴权")
        _check("路由存在且为 POST", _route_exists(), ROUTE_PATH)
        r = await client.post(f"{API}/1/next-question", json={})
        _check("无 token 返回 401", r.status_code == 401, str(r.status_code))

        async def override_get_current_user():
            return {"user_id": user_id, "email": "rag_route@example.com", "role": "user"}

        main.app.dependency_overrides[get_current_user] = override_get_current_user

        r = await client.post(
            f"{API}/create",
            json={"job_id": job_id, "difficulty": "mid", "total_questions": 3},
        )
        session_id = r.json()["session"]["id"]
        await client.post(f"{API}/{session_id}/start")

        # ---------------- [2] 默认不检索 ----------------
        print("\n[2] 默认（use_rag 缺省）")
        spy.calls.clear()
        spark.prompts.clear()
        r = await client.post(f"{API}/{session_id}/next-question", json={})
        _check("状态码 200", r.status_code == 200, f"{r.status_code} {r.text[:160]}")
        body = r.json()
        _check("ok=true", body.get("ok") is True, str(body)[:200])
        _check("组装器未被调用（默认不检索）", spy.calls == [], str(spy.calls))
        _check(
            "Prompt 不含知识小节",
            spark.prompts and KNOWLEDGE_MARKER not in spark.prompts[-1],
            "prompt 缺失或含知识小节",
        )
        _check(
            "结果字段集恒定（12 键）",
            set(body.keys())
            == {
                "ok", "question_no", "question", "question_type", "topic",
                "difficulty", "expected_points", "reason", "stage", "warnings",
                "errors", "error",
            },
            str(sorted(body.keys())),
        )

        # ---------------- [3] use_rag=true 不带参数 ----------------
        print("\n[3] use_rag=true（不给检索参数）")
        spy.calls.clear()
        r = await client.post(f"{API}/{session_id}/next-question", json={"use_rag": True})
        _check("状态码 200", r.status_code == 200, str(r.status_code))
        _check("组装器被调用 1 次", len(spy.calls) == 1, str(spy.calls))
        _check("不给参数就不传参数", spy.calls == [{}], str(spy.calls))
        _check("ok=true", r.json().get("ok") is True, str(r.json())[:200])

        # ---------------- [4] 参数透传 ----------------
        print("\n[4] use_rag=true + top_k / min_score")
        spy.calls.clear()
        r = await client.post(
            f"{API}/{session_id}/next-question",
            json={"use_rag": True, "top_k": 3, "min_score": 0.5},
        )
        _check("状态码 200", r.status_code == 200, str(r.status_code))
        _check(
            "top_k / min_score 原样透传",
            spy.calls == [{"top_k": 3, "min_score": 0.5}],
            str(spy.calls),
        )
        spy.calls.clear()
        r = await client.post(
            f"{API}/{session_id}/next-question",
            json={"use_rag": True, "top_k": 7},
        )
        _check("只给 top_k 时只传 top_k", spy.calls == [{"top_k": 7}], str(spy.calls))

        # ---------------- [5] use_rag=false 时参数不生效 ----------------
        print("\n[5] use_rag=false + 检索参数")
        spy.calls.clear()
        r = await client.post(
            f"{API}/{session_id}/next-question",
            json={"use_rag": False, "top_k": 3, "min_score": 0.5},
        )
        _check("状态码 200", r.status_code == 200, str(r.status_code))
        _check("组装器未被调用（参数不生效）", spy.calls == [], str(spy.calls))

        # ---------------- [6] 端到端真检索 ----------------
        print("\n[6] 端到端：库里有知识 + use_rag=true")
        knowledge_rag.build_vector_retriever = real_builder  # 恢复真组装器
        async with session_factory() as db:
            report = await knowledge_import_pipeline.import_document(db, DOC)
        _check("知识导入成功", report.get("ok") is True, str(report)[:200])

        spark.prompts.clear()
        r = await client.post(f"{API}/{session_id}/next-question", json={"use_rag": True})
        body = r.json()
        _check("状态码 200", r.status_code == 200, str(r.status_code))
        _check("ok=true", body.get("ok") is True, str(body)[:240])
        _check(
            "知识检索未降级（无 knowledge_retrieval_failed）",
            "knowledge_retrieval_failed" not in (body.get("warnings") or []),
            str(body.get("warnings")),
        )
        _check(
            "Agent 收到的 Prompt 含知识小节（RAG 真的接上了）",
            bool(spark.prompts) and KNOWLEDGE_MARKER in spark.prompts[-1],
            "prompt 未含知识小节",
        )

        # ---------------- [7] 对照：同一份库，关闭 RAG ----------------
        print("\n[7] 对照：同一份库 + use_rag=false")
        spark.prompts.clear()
        r = await client.post(f"{API}/{session_id}/next-question", json={})
        _check(
            "Prompt 不含知识小节",
            bool(spark.prompts) and KNOWLEDGE_MARKER not in spark.prompts[-1],
            "prompt 含知识小节",
        )
        _check("对照臂 ok=true", r.json().get("ok") is True, str(r.json())[:200])

        # ---------------- [8] 归属校验：Service 先拦（404），Core 的 ok=false 是纵深防御 ----------------
        print("\n[8] 会话不存在 / 非本人")
        r = await client.post(f"{API}/999999/next-question", json={})
        _check("会话不存在返回 404", r.status_code == 404, f"{r.status_code} {r.text[:120]}")
        _check(
            "404 提示可读",
            "不存在" in r.json().get("detail", ""),
            r.text[:120],
        )

    knowledge_rag.build_vector_retriever = real_builder  # type: ignore[assignment]
    main.spark_api = original_spark  # type: ignore[assignment]
    main.app.dependency_overrides.clear()
    await engine.dispose()

    # ---------------- [9] 契约与源码守卫 ----------------
    print("\n[9] 契约与源码守卫")
    _check(
        "NextQuestionRequest.use_rag 默认 False",
        NextQuestionRequest.model_fields["use_rag"].default is False,
        str(NextQuestionRequest.model_fields["use_rag"].default),
    )
    _check(
        "NextQuestionRequest 默认实例 use_rag=False",
        NextQuestionRequest().use_rag is False,
    )

    api_path = pathlib.Path(__file__).resolve().parents[1] / "api" / "interview.py"
    source = api_path.read_text(encoding="utf-8")
    kwargs = _kwargs_of_call(source, "generate_next_question")
    _check("路由确实调用了 generate_next_question", kwargs is not None, str(kwargs))
    _check(
        "use_rag 由请求驱动（payload.use_rag）",
        bool(kwargs) and kwargs.get("use_rag") == "payload.use_rag",
        str(kwargs),
    )
    hardcoded = "use_rag" + "=True"
    _check(
        "源码未硬编码开启 RAG",
        hardcoded not in source,
        hardcoded,
    )

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
