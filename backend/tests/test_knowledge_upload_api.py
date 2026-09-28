# -*- coding: utf-8 -*-
"""知识库**文件上传**路由自检（``POST /api/knowledge/documents/upload``）

无需 pytest，直接运行：
    python backend/tests/test_knowledge_upload_api.py

覆盖：
[1] 鉴权：无 token → 401；普通用户 → 403（写接口仅 admin）
[2] 上传各格式：docx / pdf / pptx / html / txt(utf-8) / txt(gb18030)
[3] 解析信息回传：filename / parsed_format / parsed_chars / parsed_encoding / parse_warnings
[4] 标题回落：缺省取文件名去扩展名；显式 title 优先；纯空白 title 也回落
[5] **真的落库**：详情接口的正文 == 解析出的文本（不是「接口返回 200」就算数）
[6] 错误口径：不支持格式 / 无文本层 PDF / 0 字节 / 非法 category → **400**
[7] 413：超过单文件上限的映射
[8] 校验失败**不产生半成品文档**（非法 category 后文档数不变）
[9] 现有 JSON 契约未被破坏：`POST /documents` 仍按 JSON 正常工作
[10] 源码守卫（AST）：上传路由挂 `get_current_admin`；本层不 import interview 三件套

环境钉扎：本套件会真实走 ``knowledge_import_pipeline``（内含 Embedding），
因此必须 ``import regression_env`` 把 ``EMBEDDING_*`` 钉成空串 ⇒ 用离线哈希占位，
**不打讯飞接口**、不受 ``backend/.env`` 的部署取值影响。
"""

import ast
import asyncio
import os
import pathlib
import sys
from typing import Any, Dict, List

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))
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
from services import document_parser  # noqa: E402
from document_fixtures import (  # noqa: E402
    DOCX_PARAGRAPHS,
    PDF_LINES,
    make_docx,
    make_html,
    make_pdf,
    make_pptx,
    make_text,
)

API = "/api/knowledge"
UPLOAD = f"{API}/documents/upload"

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


def _imports_of(source: str) -> List[str]:
    """AST：取所有 ``import`` / ``from ... import`` 的模块名（全量 walk）。"""
    out: List[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            out.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            out.append(node.module or "")
    return out


def _depends_of(source: str, func_name: str) -> List[str]:
    """AST：取某个路由函数里所有 ``Depends(<name>)`` 的 ``<name>``。"""
    found: List[str] = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name != func_name:
            continue
        defaults = list(node.args.defaults) + [d for d in node.args.kw_defaults if d]
        for default in defaults:
            if not isinstance(default, ast.Call):
                continue
            callee = default.func
            name = callee.attr if isinstance(callee, ast.Attribute) else getattr(callee, "id", "")
            if name != "Depends" or not default.args:
                continue
            arg = default.args[0]
            found.append(arg.attr if isinstance(arg, ast.Attribute) else getattr(arg, "id", ""))
    return found


async def _count(db: AsyncSession, model: Any) -> int:
    return int((await db.execute(select(func.count()).select_from(model))).scalar_one())


async def run() -> bool:
    print("=" * 68)
    print("知识库文件上传路由自检（POST /api/knowledge/documents/upload）")
    print("=" * 68)

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        user = User(username="up_user", email="up@example.com", password_hash="x", role="user")
        admin = User(username="up_admin", email="up_admin@example.com", password_hash="x", role="admin")
        db.add_all([user, admin])
        await db.commit()
        await db.refresh(user)
        await db.refresh(admin)
        user_id, admin_id = user.id, admin.id

    async def override_get_db():
        async with session_factory() as session:
            yield session

    main.app.dependency_overrides[get_db] = override_get_db
    CURRENT: Dict[str, Any] = {"user_id": user_id, "email": "up@example.com", "role": "user"}

    async def override_get_current_user():
        return dict(CURRENT)

    transport = ASGITransport(app=main.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # ---------------- [1] 鉴权 / 权限 ----------------
        print("\n[1] 鉴权与权限")
        r = await client.post(UPLOAD, files={"file": ("a.txt", b"x", "text/plain")}, data={"category": "technical"})
        _check("无 token → 401", r.status_code == 401, str(r.status_code))

        main.app.dependency_overrides[get_current_user] = override_get_current_user
        r = await client.post(UPLOAD, files={"file": ("a.txt", b"x", "text/plain")}, data={"category": "technical"})
        _check("普通用户 → 403", r.status_code == 403, f"{r.status_code} {r.text[:120]}")

        CURRENT.update({"user_id": admin_id, "email": "up_admin@example.com", "role": "admin"})

        # ---------------- [2][3] DOCX 上传 ----------------
        print("\n[2] 上传 DOCX（解析 + 导入全链路）")
        r = await client.post(
            UPLOAD,
            files={
                "file": (
                    "Redis 持久化手册.docx",
                    make_docx(),
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )
            },
            data={"category": "technical", "source": "file://redis.docx"},
        )
        _check("状态码 200", r.status_code == 200, f"{r.status_code} {r.text[:200]}")
        body = r.json()

        print("\n[3] 解析信息回传（形状与取值）")
        _check("ok=true", body.get("ok") is True, str(body)[:200])
        _check("status=ok", body.get("status") == "ok", str(body.get("status")))
        _check("stage=done", body.get("stage") == "done", str(body.get("stage")))
        _check("filename 原样回传", body.get("filename") == "Redis 持久化手册.docx", str(body.get("filename")))
        _check("parsed_format=docx", body.get("parsed_format") == "docx", str(body.get("parsed_format")))
        _check("parsed_chars > 0", isinstance(body.get("parsed_chars"), int) and body["parsed_chars"] > 0, str(body.get("parsed_chars")))
        _check("docx 无 parse_warnings", body.get("parse_warnings") == [], str(body.get("parse_warnings")))
        _check("继承导入报告的 12 键（chunk_count）", "chunk_count" in body, str(list(body.keys())))
        _check("document_id 为 int", isinstance(body.get("document_id"), int), str(body.get("document_id")))
        _check("chunk_count ≥ 1", body.get("chunk_count", 0) >= 1, str(body.get("chunk_count")))
        _check(
            "embedded_chunks == chunk_count",
            body.get("embedded_chunks") == body.get("chunk_count"),
            str(body.get("chunk_count")) + " vs " + str(body.get("embedded_chunks")),
        )
        docx_id = body["document_id"]

        # ---------------- [4] 标题回落 ----------------
        print("\n[4] 标题回落")
        r = await client.get(f"{API}/documents/{docx_id}")
        detail = r.json()
        _check("缺省标题 = 文件名去扩展名", detail.get("title") == "Redis 持久化手册", str(detail.get("title")))
        _check("source 透传", detail.get("source") == "file://redis.docx", str(detail.get("source")))

        r = await client.post(
            UPLOAD,
            files={"file": ("原名.txt", make_text("utf-8"), "text/plain")},
            data={"category": "technical", "title": "  显式标题优先  "},
        )
        explicit = r.json()
        r = await client.get(f"{API}/documents/{explicit['document_id']}")
        _check("显式 title 优先且被 strip", r.json().get("title") == "显式标题优先", str(r.json().get("title")))

        r = await client.post(
            UPLOAD,
            files={"file": ("空白标题.txt", make_text("utf-8"), "text/plain")},
            data={"category": "technical", "title": "    "},
        )
        blank = r.json()
        r = await client.get(f"{API}/documents/{blank['document_id']}")
        _check(
            "纯空白 title 回落到文件名（不会 400）",
            r.json().get("title") == "空白标题",
            str(r.json().get("title")),
        )

        # ---------------- [5] 真的落库 ----------------
        print("\n[5] 正文确实落库（不是只看状态码）")
        r = await client.get(f"{API}/documents/{docx_id}")
        stored = r.json().get("content") or ""
        for paragraph in DOCX_PARAGRAPHS:
            _check(f"落库正文含「{paragraph}」", paragraph in stored, stored[:120])
        _check("落库正文不含 XML 标签", "<w:" not in stored and "</" not in stored, stored[:120])
        _check("落库正文字符数与回传 parsed_chars 一致", len(stored) == body["parsed_chars"], f"{len(stored)} vs {body['parsed_chars']}")

        async with session_factory() as db:
            chunks = await _count(db, KnowledgeChunk)
            docs = await _count(db, KnowledgeDocument)
        _check("知识切片表确有写入", chunks >= 1, str(chunks))
        _check("文档表确有写入", docs >= 3, str(docs))

        # ---------------- [6] 其余格式 ----------------
        print("\n[6] 其余格式")
        r = await client.post(
            UPLOAD,
            files={"file": ("handbook.pdf", make_pdf(), "application/pdf")},
            data={"category": "technical"},
        )
        pdf_body = r.json()
        _check("PDF 上传 200 + ok", r.status_code == 200 and pdf_body.get("ok") is True, f"{r.status_code} {r.text[:160]}")
        _check("parsed_format=pdf", pdf_body.get("parsed_format") == "pdf", str(pdf_body.get("parsed_format")))
        r = await client.get(f"{API}/documents/{pdf_body['document_id']}")
        _check("PDF 正文含提取出的文字", PDF_LINES[0] in (r.json().get("content") or ""), str(r.json().get("content"))[:120])

        r = await client.post(
            UPLOAD,
            files={"file": ("技术分享.pptx", make_pptx(), "application/vnd.openxmlformats-officedocument.presentationml.presentation")},
            data={"category": "technical"},
        )
        pptx_body = r.json()
        _check("PPTX 上传 200 + ok", r.status_code == 200 and pptx_body.get("ok") is True, f"{r.status_code} {r.text[:160]}")
        _check("parsed_format=pptx", pptx_body.get("parsed_format") == "pptx", str(pptx_body.get("parsed_format")))
        r = await client.get(f"{API}/documents/{pptx_body['document_id']}")
        _check("PPTX 正文含页码分隔", "【第 1 页】" in (r.json().get("content") or ""), str(r.json().get("content"))[:120])

        r = await client.post(
            UPLOAD,
            files={"file": ("page.html", make_html(), "text/html")},
            data={"category": "technical"},
        )
        html_body = r.json()
        _check("HTML 上传 200 + ok", r.status_code == 200 and html_body.get("ok") is True, f"{r.status_code} {r.text[:160]}")
        _check("parsed_format=html", html_body.get("parsed_format") == "html", str(html_body.get("parsed_format")))
        r = await client.get(f"{API}/documents/{html_body['document_id']}")
        html_text = r.json().get("content") or ""
        _check("HTML 正文已去标签", "var x=1" not in html_text and "<" not in html_text, html_text[:120])

        r = await client.post(
            UPLOAD,
            files={"file": ("utf8.txt", make_text("utf-8"), "text/plain")},
            data={"category": "job"},
        )
        utf8_body = r.json()
        _check("UTF-8 txt 上传成功", utf8_body.get("ok") is True, str(utf8_body)[:160])
        _check("parsed_encoding=utf-8", utf8_body.get("parsed_encoding") == "utf-8", str(utf8_body.get("parsed_encoding")))
        _check("UTF-8 无 parse_warnings", utf8_body.get("parse_warnings") == [], str(utf8_body.get("parse_warnings")))

        r = await client.post(
            UPLOAD,
            files={"file": ("gbk.txt", make_text("gb18030"), "text/plain")},
            data={"category": "job"},
        )
        gbk_body = r.json()
        _check("GB18030 txt 上传成功", gbk_body.get("ok") is True, str(gbk_body)[:160])
        _check("parsed_encoding=gb18030", gbk_body.get("parsed_encoding") == "gb18030", str(gbk_body.get("parsed_encoding")))
        _check(
            "非 UTF-8 的告警被回传（不静默）",
            "decoded_as_gb18030" in (gbk_body.get("parse_warnings") or []),
            str(gbk_body.get("parse_warnings")),
        )
        r = await client.get(f"{API}/documents/{gbk_body['document_id']}")
        _check("GB18030 正文解码正确", "中文内容测试" in (r.json().get("content") or ""), str(r.json().get("content"))[:80])

        # ---------------- [7] 错误口径 ----------------
        print("\n[7] 错误口径（400 / 413）")
        async with session_factory() as db:
            docs_before = await _count(db, KnowledgeDocument)

        r = await client.post(UPLOAD, files={"file": ("old.doc", b"xx", "application/msword")}, data={"category": "technical"})
        _check(".doc → 400", r.status_code == 400, f"{r.status_code} {r.text[:120]}")
        _check("400 detail 提示另存为 .docx", ".docx" in r.text, r.text[:160])

        r = await client.post(UPLOAD, files={"file": ("x.xlsx", b"xx", "application/vnd.ms-excel")}, data={"category": "technical"})
        _check(".xlsx → 400", r.status_code == 400, str(r.status_code))

        r = await client.post(UPLOAD, files={"file": ("scan.pdf", make_pdf(with_text=False), "application/pdf")}, data={"category": "technical"})
        _check("无文本层 PDF → 400（不导入空文档）", r.status_code == 400, f"{r.status_code} {r.text[:160]}")
        _check("提示里点明 OCR / 扫描件", "OCR" in r.text or "扫描" in r.text, r.text[:160])

        r = await client.post(UPLOAD, files={"file": ("empty.txt", b"", "text/plain")}, data={"category": "technical"})
        _check("0 字节文件 → 400", r.status_code == 400, f"{r.status_code} {r.text[:120]}")

        r = await client.post(UPLOAD, files={"file": ("ok.txt", make_text("utf-8"), "text/plain")}, data={"category": "nope"})
        _check("非法 category → 400", r.status_code == 400, f"{r.status_code} {r.text[:160]}")
        _check("400 detail 列出允许的 category", "job" in r.text, r.text[:160])

        async with session_factory() as db:
            docs_after = await _count(db, KnowledgeDocument)
        _check(
            "以上 5 次失败**都没产生半成品文档**",
            docs_after == docs_before,
            f"{docs_before} -> {docs_after}",
        )

        # 413：临时把上限调小以验证路由的映射分支（真实 20MB 上限在 test_document_parser 断言）
        original_max = document_parser.MAX_UPLOAD_BYTES
        try:
            document_parser.MAX_UPLOAD_BYTES = 16
            r = await client.post(UPLOAD, files={"file": ("big.txt", b"a" * 64, "text/plain")}, data={"category": "technical"})
            _check("超限 → 413", r.status_code == 413, f"{r.status_code} {r.text[:120]}")
        finally:
            document_parser.MAX_UPLOAD_BYTES = original_max

        r = await client.post(UPLOAD, files={"file": ("ok.txt", make_text("utf-8"), "text/plain")})
        _check("缺 category 表单字段 → 422", r.status_code == 422, f"{r.status_code} {r.text[:120]}")

        r = await client.post(UPLOAD, data={"category": "technical"})
        _check("缺 file 字段 → 422", r.status_code == 422, f"{r.status_code} {r.text[:120]}")

        # ---------------- [8] 现有 JSON 契约未变 ----------------
        print("\n[8] 现有 JSON 契约未被破坏")
        r = await client.post(
            f"{API}/documents",
            json={
                "title": "手工录入的纯文本",
                "content": "RDB 是快照，AOF 是追加日志，生产建议 appendfsync everysec。",
                "category": "technical",
                "source": "manual://test",
            },
        )
        _check("POST /documents 仍按 JSON 工作", r.status_code == 200 and r.json().get("ok") is True, f"{r.status_code} {r.text[:160]}")
        json_body = r.json()
        _check(
            "JSON 路由的报告里**没有**解析字段（契约未变）",
            "parsed_format" not in json_body and "parsed_chars" not in json_body,
            str([k for k in json_body if k.startswith("parsed")]),
        )
        _check("JSON 路由仍校验空标题 → 400",
              (await client.post(f"{API}/documents", json={"title": "  ", "content": "x", "category": "technical"})).status_code == 400)

        # ---------------- [9] 源码守卫（AST） ----------------
        print("\n[9] 源码守卫（AST）")
        api_source = (_BACKEND / "api" / "knowledge.py").read_text(encoding="utf-8")
        depends = _depends_of(api_source, "upload_knowledge_document")
        _check("上传路由挂 get_current_admin", "get_current_admin" in depends, str(depends))
        _check("上传路由挂 get_db", "get_db" in depends, str(depends))
        _check("上传路由**不**挂 get_current_user（写接口只认 admin）", "get_current_user" not in depends, str(depends))

        api_imports = _imports_of(api_source)
        for banned in ("interview_service", "interview_core", "interview_agent", "interview_planner"):
            offenders = [name for name in api_imports if name.endswith(banned)]
            _check(f"知识库路由层不 import {banned}", offenders == [], str(offenders))

        parser_source = (_BACKEND / "services" / "document_parser.py").read_text(encoding="utf-8")
        parser_tree = ast.parse(parser_source)
        top_imports: List[str] = []
        for node in parser_tree.body:
            if isinstance(node, ast.Import):
                top_imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                top_imports.append(node.module or "")
        for banned in ("database", "fastapi", "sqlalchemy", "models", "schemas"):
            offenders = [n for n in top_imports if n == banned or n.startswith(banned + ".")]
            _check(f"解析层顶层不 import {banned}", offenders == [], str(offenders))

        # ---------------- [10] 报告形状恒定 ----------------
        print("\n[10] 报告形状恒定（成功 / 失败同形状）")
        from schemas.knowledge import KnowledgeUploadReport  # noqa: E402

        expected = set(KnowledgeUploadReport.model_fields.keys())
        _check(
            "KnowledgeUploadReport 继承导入报告的 12 键",
            {"ok", "status", "stage", "document_id", "chunk_count", "saved_chunks",
             "embedded_chunks", "skipped_chunks", "reused_document", "failed_index",
             "errors", "error"} <= expected,
            str(sorted(expected)),
        )
        _check(
            "额外 5 个解析字段齐备",
            {"filename", "parsed_format", "parsed_chars", "parsed_encoding", "parse_warnings"} <= expected,
            str(sorted(expected)),
        )

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
