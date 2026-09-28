# -*- coding: utf-8 -*-
"""知识库 HTTP 接口自检（缺口②导入路由 + 缺口③删除 / 索引重建）

无需 pytest，直接运行：
    python backend/tests/test_knowledge_api.py

覆盖：
[1] 鉴权：读 / 写都要求登录（无 token → 401）
[2] 权限：**写接口仅 admin**（普通用户 → 403），读接口普通用户可用
[3] 导入（admin）：200 + `ok=true` + 计数正确；空标题 / 非法 category → **400**
[4] 列表：只回元信息（**不含正文**）；category 过滤；非法 category → 400；分页收敛
[5] 详情：含正文；不存在 → 404
[6] 删除（admin）：切片与文档一并删除；再删 → 404；列表里消失
[7] 重建（admin）：幂等（重复执行不产生重复行）；`document_id` 只重建该文档
[7b] `purge` 参数三态：默认不产生副作用；sql 后端**被拒绝**（理由进 `purge_note`）；
     与 `document_id` 互斥；非法布尔值 → 422
[8] 重建的脏数据口径：`embedding=[]` 的行**跳过并计数**，不打挂整次重建
[9] 服务层删除语义：后端无删除接口时 `index_error` **如实说明**（不假装成功）
[10] 源码守卫（AST）：写接口挂 `get_current_admin`、读接口挂 `get_current_user`；
     本层**不 import** interview 三件套（分层）

环境钉扎：本套件会真实走 ``knowledge_import_pipeline``（内含 Embedding），
因此必须 ``import regression_env`` 把 ``EMBEDDING_*`` 钉成空串 ⇒ 用离线哈希占位。
**未钉扎时它会真的去打讯飞 Embedding 接口**（`backend/.env` 里配了
``EMBEDDING_PROVIDER=spark``），既消耗配额又让回归结果依赖网络。
"""

import ast
import asyncio
import os
import pathlib
import sys
from typing import Any, Dict, List, Optional

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import regression_env  # noqa: E402,F401  （必须在项目模块之前）

from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy import func, select  # noqa: E402
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
from models import KnowledgeChunk, KnowledgeDocument, User  # noqa: E402
from schemas.knowledge import KnowledgeRebuildResponse  # noqa: E402
from services import knowledge_maintenance  # noqa: E402
from services.knowledge_maintenance import (  # noqa: E402
    PURGE_REFUSED_HINT,
    PURGE_SCOPED_HINT,
)

API = "/api/knowledge"

DOC = {
    "title": "Redis 持久化手册",
    "content": (
        "RDB 是快照，AOF 是追加日志。AOF 重写通过 BGREWRITEAOF 触发；"
        "混合持久化把 RDB 与 AOF 结合。生产建议 appendfsync everysec。"
    ),
    "category": "technical",
    "source": "handbook://redis/persistence",
}

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


class FakeStore:
    """向量后端替身：只有 ``name`` / ``add``，**没有删除接口**。"""

    def __init__(self, name: str = "chroma") -> None:
        self.name = name
        self.added = 0

    async def add(self, records: Any) -> int:
        self.added += len(records)
        return len(records)


class DeletableStore(FakeStore):
    """带删除钩子的替身（鸭子类型：``delete``）。"""

    def __init__(self, name: str = "chroma") -> None:
        super().__init__(name)
        self.deleted: List[int] = []

    async def delete(self, ids: Any) -> None:
        self.deleted.extend(list(ids))


def _depends_of(source: str, func_name: str) -> List[str]:
    """AST：取某个路由函数里所有 ``Depends(<name>)`` 的 ``<name>``。"""
    tree = ast.parse(source)
    found: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == func_name:
            for default in list(node.args.defaults) + [d for d in node.args.kw_defaults if d]:
                if not isinstance(default, ast.Call):
                    continue
                callee = default.func
                callee_name = callee.attr if isinstance(callee, ast.Attribute) else getattr(callee, "id", "")
                if callee_name != "Depends" or not default.args:
                    continue
                arg = default.args[0]
                found.append(arg.attr if isinstance(arg, ast.Attribute) else getattr(arg, "id", ""))
    return found


def _imports_of(source: str) -> List[str]:
    """AST：取所有 ``import`` / ``from ... import`` 的模块名（全量 walk）。"""
    tree = ast.parse(source)
    out: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            out.append(node.module or "")
    return out


async def _count(db: AsyncSession, model: Any) -> int:
    return int((await db.execute(select(func.count()).select_from(model))).scalar_one())


async def run() -> bool:
    print("=" * 68)
    print("知识库 HTTP 接口自检（缺口②导入 + 缺口③删除 / 重建）")
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
        user = User(username="kb_user", email="kb@example.com", password_hash="x", role="user")
        admin = User(username="kb_admin", email="kb_admin@example.com", password_hash="x", role="admin")
        db.add_all([user, admin])
        await db.commit()
        await db.refresh(user)
        await db.refresh(admin)
        user_id, admin_id = user.id, admin.id

    async def override_get_db():
        async with session_factory() as session:
            yield session

    main.app.dependency_overrides[get_db] = override_get_db

    #: 可变登录态：读接口用 user，写接口切 admin（``get_current_admin`` 依赖 ``get_current_user``）
    CURRENT: Dict[str, Any] = {"user_id": user_id, "email": "kb@example.com", "role": "user"}

    async def override_get_current_user():
        return dict(CURRENT)

    transport = ASGITransport(app=main.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # ---------------- [1] 鉴权 ----------------
        print("\n[1] 鉴权（未登录 401）")
        r = await client.get(f"{API}/documents")
        _check("读列表无 token → 401", r.status_code == 401, str(r.status_code))
        r = await client.post(f"{API}/documents", json=DOC)
        _check("写导入无 token → 401", r.status_code == 401, str(r.status_code))

        main.app.dependency_overrides[get_current_user] = override_get_current_user

        # ---------------- [2] 权限 ----------------
        print("\n[2] 权限（写接口仅 admin）")
        r = await client.post(f"{API}/documents", json=DOC)
        _check("普通用户导入 → 403", r.status_code == 403, f"{r.status_code} {r.text[:120]}")
        r = await client.post(f"{API}/rebuild")
        _check("普通用户重建 → 403", r.status_code == 403, str(r.status_code))
        r = await client.delete(f"{API}/documents/1")
        _check("普通用户删除 → 403", r.status_code == 403, str(r.status_code))
        r = await client.get(f"{API}/documents")
        _check("普通用户读列表 → 200", r.status_code == 200, str(r.status_code))

        CURRENT.update({"user_id": admin_id, "email": "kb_admin@example.com", "role": "admin"})

        # ---------------- [3] 导入 ----------------
        print("\n[3] 导入（admin）")
        r = await client.post(f"{API}/documents", json=DOC)
        _check("状态码 200", r.status_code == 200, f"{r.status_code} {r.text[:160]}")
        report = r.json()
        _check("ok=true", report.get("ok") is True, str(report)[:200])
        _check("status=ok", report.get("status") == "ok", str(report.get("status")))
        _check("stage=done", report.get("stage") == "done", str(report.get("stage")))
        document_id = report.get("document_id")
        _check("回传 document_id", isinstance(document_id, int), str(document_id))
        _check("chunk_count ≥ 1", report.get("chunk_count", 0) >= 1, str(report.get("chunk_count")))
        _check(
            "embedded_chunks == chunk_count",
            report.get("embedded_chunks") == report.get("chunk_count"),
            str(report),
        )

        r = await client.post(f"{API}/documents", json={**DOC, "title": "   "})
        _check("空标题 → 400", r.status_code == 400, f"{r.status_code} {r.text[:120]}")
        r = await client.post(f"{API}/documents", json={**DOC, "category": "nope"})
        _check("非法 category → 400", r.status_code == 400, f"{r.status_code} {r.text[:120]}")
        r = await client.post(f"{API}/documents", json={"title": "x", "content": "y"})
        _check("缺 category → 422", r.status_code == 422, str(r.status_code))

        # ---------------- [4] 列表 ----------------
        print("\n[4] 列表")
        CURRENT.update({"user_id": user_id, "email": "kb@example.com", "role": "user"})
        r = await client.get(f"{API}/documents")
        _check("状态码 200", r.status_code == 200, str(r.status_code))
        body = r.json()
        _check("total == 1", body.get("total") == 1, str(body.get("total")))
        _check("items 非空", len(body.get("items") or []) == 1, str(len(body.get("items") or [])))
        _check("列表项不含正文", "content" not in (body["items"][0] if body.get("items") else {}))
        _check("列表项含来源", body["items"][0].get("source") == DOC["source"] if body.get("items") else False)
        r = await client.get(f"{API}/documents", params={"category": "technical"})
        _check("category 过滤命中", r.json().get("total") == 1, str(r.json().get("total")))
        r = await client.get(f"{API}/documents", params={"category": "job"})
        _check("category 过滤未命中", r.json().get("total") == 0, str(r.json().get("total")))
        r = await client.get(f"{API}/documents", params={"category": "nope"})
        _check("非法 category → 400", r.status_code == 400, str(r.status_code))
        r = await client.get(f"{API}/documents", params={"limit": 9999, "offset": -5})
        _check("分页越界被拒（422）", r.status_code == 422, str(r.status_code))

        # ---------------- [5] 详情 ----------------
        print("\n[5] 详情")
        r = await client.get(f"{API}/documents/{document_id}")
        _check("状态码 200", r.status_code == 200, str(r.status_code))
        _check("含正文", r.json().get("content") == DOC["content"])
        r = await client.get(f"{API}/documents/999999")
        _check("不存在 → 404", r.status_code == 404, str(r.status_code))

        # ---------------- [7] 重建（先测，保证删除前后都覆盖） ----------------
        print("\n[7] 重建（admin）")
        CURRENT.update({"user_id": admin_id, "email": "kb_admin@example.com", "role": "admin"})
        r = await client.post(f"{API}/rebuild")
        _check("状态码 200", r.status_code == 200, f"{r.status_code} {r.text[:160]}")
        first = r.json()
        _check("errors 为空", first.get("errors") == [], str(first.get("errors")))
        _check(
            "rebuilt_chunks == scanned_chunks",
            first.get("rebuilt_chunks") == first.get("scanned_chunks"),
            str(first),
        )
        async with session_factory() as db:
            chunks_before = await _count(db, KnowledgeChunk)
        r = await client.post(f"{API}/rebuild")
        second = r.json()
        _check("重复重建 rebuilt 数不变", second.get("rebuilt_chunks") == first.get("rebuilt_chunks"), str(second))
        async with session_factory() as db:
            chunks_after = await _count(db, KnowledgeChunk)
        _check("重复重建不产生重复行（幂等）", chunks_after == chunks_before, f"{chunks_before} -> {chunks_after}")

        r = await client.post(f"{API}/rebuild", params={"document_id": document_id})
        _check("按文档重建：scanned == 该文档切片数", r.json().get("scanned_chunks") == first.get("scanned_chunks"), str(r.json()))
        r = await client.post(f"{API}/rebuild", params={"document_id": 999999})
        _check("按不存在文档重建：scanned == 0", r.json().get("scanned_chunks") == 0, str(r.json()))

        # ---------------- [7b] purge 三态（写接口的「索引删 / 重建」入口） ----------------
        print("\n[7b] purge 参数（先清空派生索引再重建）")
        r = await client.post(f"{API}/rebuild")
        body = r.json()
        _check(
            "响应键集合 == KnowledgeRebuildResponse（7 键）",
            set(body.keys()) == set(KnowledgeRebuildResponse.model_fields),
            str(sorted(body.keys())),
        )
        _check("默认 purge 未传 ⇒ purged_chunks 为 None", body.get("purged_chunks") is None, str(body))
        _check("默认 purge 未传 ⇒ purge_note 为空串", body.get("purge_note") == "", str(body))
        # 本套件用的是 sql 后端（索引就是权威行）⇒ purge 必须**被拒绝**，而不是清空
        r = await client.post(f"{API}/rebuild", params={"purge": "true"})
        _check("purge=true 状态码 200（拒绝也走 200，理由进 purge_note）",
               r.status_code == 200, f"{r.status_code} {r.text[:160]}")
        body = r.json()
        _check("sql 后端 purge ⇒ purged_chunks 为 None（没清）", body.get("purged_chunks") is None, str(body))
        _check(
            "sql 后端 purge ⇒ purge_note == PURGE_REFUSED_HINT",
            body.get("purge_note") == PURGE_REFUSED_HINT,
            str(body.get("purge_note")),
        )
        _check("purge 被拒时仍完成 upsert 重建", body.get("rebuilt_chunks", 0) > 0, str(body))
        r = await client.post(f"{API}/rebuild", params={"purge": "true", "document_id": document_id})
        _check(
            "purge 与 document_id 互斥 ⇒ purge_note == PURGE_SCOPED_HINT",
            r.json().get("purge_note") == PURGE_SCOPED_HINT,
            str(r.json().get("purge_note")),
        )
        r = await client.post(f"{API}/rebuild", params={"purge": "nope"})
        _check("purge 非法布尔值 → 422（不静默当 false）", r.status_code == 422, str(r.status_code))

        # ---------------- [8] 脏数据口径 ----------------
        print("\n[8] 重建的脏数据口径")
        async with session_factory() as db:
            db.add(
                KnowledgeChunk(
                    document_id=document_id,
                    content="脏行：embedding 是空列表",
                    chunk_metadata={"chunk_index": 99},
                    embedding=[],
                    embedding_model="hash-local",
                    embedding_dim=0,
                )
            )
            await db.commit()
        r = await client.post(f"{API}/rebuild", params={"document_id": document_id})
        body = r.json()
        _check("脏行被计入 scanned", body.get("scanned_chunks") == first.get("scanned_chunks") + 1, str(body))
        _check("脏行被 skipped", body.get("skipped_chunks") == 1, str(body))
        _check("errors 仍为空（脏数据不打挂重建）", body.get("errors") == [], str(body.get("errors")))

        # ---------------- [9] 服务层删除语义（含 index_error 如实说明） ----------------
        print("\n[9] 服务层删除语义")
        async with session_factory() as db:
            chroma_like = FakeStore(name="chroma")
            result = await knowledge_maintenance.delete_document(db, document_id, store=chroma_like)
        _check("deleted_document=True", result.get("deleted_document") is True, str(result))
        _check("deleted_chunks ≥ 2", result.get("deleted_chunks", 0) >= 2, str(result))
        _check(
            "无删除接口的后端 → index_error 如实说明",
            bool(result.get("index_error")) and "rebuild" in str(result.get("index_error")),
            str(result.get("index_error")),
        )
        _check("index_removed 保持 None（没同步就不谎报）", result.get("index_removed") is None, str(result))

        # ---------------- [6] 删除（HTTP） ----------------
        print("\n[6] 删除（admin）")
        r = await client.post(f"{API}/documents", json=DOC)
        fresh_id = r.json()["document_id"]
        async with session_factory() as db:
            before_docs = await _count(db, KnowledgeDocument)
            before_chunks = await _count(db, KnowledgeChunk)
        r = await client.delete(f"{API}/documents/{fresh_id}")
        _check("状态码 200", r.status_code == 200, f"{r.status_code} {r.text[:160]}")
        deleted = r.json()
        _check("deleted_document=True", deleted.get("deleted_document") is True, str(deleted))
        _check("deleted_chunks ≥ 1", deleted.get("deleted_chunks", 0) >= 1, str(deleted))
        _check("sql 后端无需同步 ⇒ index_error 为 None", deleted.get("index_error") is None, str(deleted))
        async with session_factory() as db:
            after_docs = await _count(db, KnowledgeDocument)
            after_chunks = await _count(db, KnowledgeChunk)
        _check("文档数减 1", after_docs == before_docs - 1, f"{before_docs} -> {after_docs}")
        _check(
            "切片数按 deleted_chunks 减少",
            after_chunks == before_chunks - deleted["deleted_chunks"],
            f"{before_chunks} -> {after_chunks}",
        )
        r = await client.delete(f"{API}/documents/{fresh_id}")
        _check("重复删除 → 404", r.status_code == 404, str(r.status_code))
        r = await client.get(f"{API}/documents/{fresh_id}")
        _check("删除后详情 → 404", r.status_code == 404, str(r.status_code))

    main.app.dependency_overrides.clear()
    await engine.dispose()

    # ---------------- [10] 源码守卫 ----------------
    print("\n[10] 源码守卫（AST）")
    api_path = pathlib.Path(__file__).resolve().parents[1] / "api" / "knowledge.py"
    source = api_path.read_text(encoding="utf-8")

    for func in ("import_knowledge_document", "delete_knowledge_document", "rebuild_knowledge_index"):
        deps = _depends_of(source, func)
        _check(f"{func} 挂 get_current_admin", "get_current_admin" in deps, str(deps))
    for func in ("list_knowledge_documents", "get_knowledge_document"):
        deps = _depends_of(source, func)
        _check(f"{func} 挂 get_current_user", "get_current_user" in deps, str(deps))
        _check(f"{func} 不挂 admin 依赖", "get_current_admin" not in deps, str(deps))

    modules = _imports_of(source)
    for banned in ("interview_core", "interview_agent", "interview_service", "interview_planner"):
        _check(
            f"知识库路由层不 import {banned}",
            not any(banned in name for name in modules),
            str([n for n in modules if banned in n]),
        )
    _check(
        "知识库路由层不 import 具体向量后端",
        not any("vector_store_sql" in n or "vector_store_chroma" in n for n in modules),
        str(modules),
    )

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
