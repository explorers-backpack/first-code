# -*- coding: utf-8 -*-
"""AI 面试知识库 · KnowledgeDocumentService（文档导入）自检

无需 pytest，直接运行：
    python backend/tests/test_knowledge_document_service.py

不依赖本机 MySQL：DATABASE_URL 指向 SQLite 内存库 + StaticPool。
本阶段**只做文档导入**，故本测试不涉及解析 / 切片 / Embedding / 向量库 / Retriever。

覆盖范围
--------
1. **模块可导入 + 契约常量**：``DOCUMENT_FIELDS`` / ``DOCUMENT_SUMMARY_FIELDS`` / 页大小
2. **创建文档成功（要求 1）**：四类知识各建一篇（含技术文档 / 岗位JD / 公司资料），
   字段逐字回读；**KnowledgeChunk 表保持 0 行**（要求「不生成 Chunk」）
3. **查询文档成功（要求 2）**：``get_document`` 单篇读回；``list_documents`` 分页 /
   分类过滤 / 标题检索 / 参数收敛 / 非法分类 400
4. **空内容拒绝（要求 3）**：空串 / 纯空白 / None / 缺字段一律 400，且**不写库**；
   另覆盖空 title 与非法 category
5. **输入形状**：``dict`` 与对象（``SimpleNamespace``）均可；``source`` 缺省 ``""``；
   标签去空白、正文原样保留
6. **边界**：不 import Embedding / 向量库 / Retriever / Interview 模块；
   生产代码**暂无**任何模块 import 本服务（本阶段未接线）
7. **出参契约恒定**：两个序列化函数键集合与常量严格一致（不多不少）
"""

import ast
import asyncio
import os
import pathlib
import sys
from types import SimpleNamespace

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from fastapi import HTTPException  # noqa: E402
from sqlalchemy import func, select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import (  # noqa: E402
    KNOWLEDGE_CATEGORY_COMPANY,
    KNOWLEDGE_CATEGORY_JOB,
    KNOWLEDGE_CATEGORY_PROJECT,
    KNOWLEDGE_CATEGORY_TECHNICAL,
    KnowledgeChunk,
    KnowledgeDocument,
)
from services import knowledge_document_service as svc  # noqa: E402

_PASSED = 0
_FAILED = 0

#: 需求点名的三类素材 + 模型里的第四类（项目经验知识），逐一建一篇
DOC_SAMPLE = {
    KNOWLEDGE_CATEGORY_TECHNICAL: (
        "Redis 持久化手册",
        "RDB 是全量快照，AOF 记录写命令；两者可同时开启。\n"
        "AOF everysec 丢数据上限约 1 秒。",
    ),
    KNOWLEDGE_CATEGORY_JOB: (
        "后端开发工程师 · 岗位JD",
        "岗位职责：负责订单中台服务的设计与开发。\n"
        "任职要求：3 年以上 Java/Go 经验，熟悉 Redis、Kafka。",
    ),
    KNOWLEDGE_CATEGORY_COMPANY: (
        "某互联网公司 · 公司资料",
        "主营业务：电商与本地生活；技术栈以 Java + Go 为主。",
    ),
    KNOWLEDGE_CATEGORY_PROJECT: (
        "订单中台重构复盘",
        "单体拆为 8 个微服务，QPS 从 800 提升到 5000。",
    ),
}


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


def _imported_modules(source: str) -> set:
    """用 AST 提取真正被 import 的模块名（不用子串匹配：docstring 会误伤）。"""
    names: set = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _imported_names(source: str) -> set:
    """用 AST 提取 ``from X import a, b`` 里被导入的**符号名**（a / b）。"""
    names: set = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            names.update(alias.name for alias in node.names)
    return names


def _production_files():
    """后端生产代码（排除 tests / __pycache__），用于「未接线」与「未反向依赖」检查。"""
    files = []
    for pattern in ("*.py", "api/*.py", "services/*.py", "models/*.py",
                    "schemas/*.py", "utils/*.py"):
        for path in BACKEND_DIR.glob(pattern):
            if "__pycache__" in path.parts:
                continue
            files.append(path)
    return sorted(set(files))


async def _expect_http(coro, status: int):
    """执行协程并断言抛出指定状态码的 ``HTTPException``；返回 (是否通过, 说明)。"""
    try:
        await coro
    except HTTPException as exc:
        return exc.status_code == status, f"status={exc.status_code} detail={exc.detail}"
    except Exception as exc:  # noqa: BLE001 - 测试里要把非预期异常也报出来
        return False, f"非 HTTPException：{type(exc).__name__}: {exc}"
    return False, "未抛异常"


async def _count_chunks(db: AsyncSession) -> int:
    return int(
        (await db.execute(select(func.count()).select_from(KnowledgeChunk))).scalar_one()
    )


async def _count_docs(db: AsyncSession) -> int:
    return int(
        (await db.execute(select(func.count()).select_from(KnowledgeDocument))).scalar_one()
    )


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 面试知识库 · 文档导入（KnowledgeDocumentService）自检")
    print("=" * 70)

    # ------------------------------------------------------------
    # [1] 模块可导入 + 契约常量
    # ------------------------------------------------------------
    print("\n[1] 模块可导入 + 出参契约常量")
    _check("services.knowledge_document_service 可导入", hasattr(svc, "create_document"))
    _check("暴露 create_document / get_document / list_documents",
           all(callable(getattr(svc, n, None))
               for n in ("create_document", "get_document", "list_documents")))
    _check("★ DOCUMENT_FIELDS 与需求字段清单逐字一致",
           svc.DOCUMENT_FIELDS == ("id", "title", "content", "category",
                                   "source", "created_at"),
           str(svc.DOCUMENT_FIELDS))
    _check("★ 列表项契约 = 整篇契约去掉 content",
           svc.DOCUMENT_SUMMARY_FIELDS == ("id", "title", "category",
                                           "source", "created_at"),
           str(svc.DOCUMENT_SUMMARY_FIELDS))
    _check("  └ 列表项确实不含 content",
           "content" not in svc.DOCUMENT_SUMMARY_FIELDS)
    _check("分页常量合法（0 < 默认 <= 上限）",
           0 < svc.DEFAULT_PAGE_SIZE <= svc.MAX_PAGE_SIZE,
           f"{svc.DEFAULT_PAGE_SIZE}/{svc.MAX_PAGE_SIZE}")
    _check("__all__ 列出三个公开函数",
           {"create_document", "get_document", "list_documents"} <= set(svc.__all__))

    # ------------------------------------------------------------
    # 建库
    # ------------------------------------------------------------
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with Session() as db:
        # ------------------------------------------------------------
        # [2] 创建文档成功（要求 1）
        # ------------------------------------------------------------
        print("\n[2] 创建文档成功（要求 1）")
        created = {}
        for category, (title, content) in DOC_SAMPLE.items():
            created[category] = await svc.create_document(
                db,
                {
                    "title": title,
                    "content": content,
                    "category": category,
                    "source": f"test://{category}",
                },
            )

        first = created[KNOWLEDGE_CATEGORY_TECHNICAL]
        _check("create_document 返回普通 dict", isinstance(first, dict), type(first).__name__)
        _check("★ 返回键集合 == DOCUMENT_FIELDS（不多不少）",
               tuple(first.keys()) == svc.DOCUMENT_FIELDS, str(tuple(first.keys())))
        _check("落库后拿到自增 id（正整数）",
               isinstance(first["id"], int) and first["id"] > 0, str(first["id"]))
        _check("  └ 四篇文档 id 互不相同",
               len({d["id"] for d in created.values()}) == len(DOC_SAMPLE))
        _check("title / content / category / source 逐字回读",
               first["title"] == DOC_SAMPLE[KNOWLEDGE_CATEGORY_TECHNICAL][0]
               and first["content"] == DOC_SAMPLE[KNOWLEDGE_CATEGORY_TECHNICAL][1]
               and first["category"] == KNOWLEDGE_CATEGORY_TECHNICAL
               and first["source"] == "test://technical",
               str(first))
        _check("created_at 已由 ORM 自动填充（非 None）",
               first["created_at"] is not None, str(first["created_at"]))
        _check("★ 三类素材都能建：技术文档 / 岗位JD / 公司资料",
               {KNOWLEDGE_CATEGORY_TECHNICAL, KNOWLEDGE_CATEGORY_JOB,
                KNOWLEDGE_CATEGORY_COMPANY} <= set(created))
        _check("  └ 第四类项目经验知识同样可建",
               KNOWLEDGE_CATEGORY_PROJECT in created)
        _check("四篇都真的落库了（count == 4）",
               await _count_docs(db) == 4, str(await _count_docs(db)))
        _check("★ 未生成任何切片（要求「不生成 Chunk」）",
               await _count_chunks(db) == 0, str(await _count_chunks(db)))

        # ------------------------------------------------------------
        # [3] 查询文档成功（要求 2）
        # ------------------------------------------------------------
        print("\n[3] 查询文档成功（要求 2）")
        target_id = created[KNOWLEDGE_CATEGORY_JOB]["id"]
        got = await svc.get_document(db, target_id)
        _check("get_document 按 id 读回整篇",
               got == created[KNOWLEDGE_CATEGORY_JOB], str(got))
        _check("  └ 含正文且与创建时一致",
               got["content"] == DOC_SAMPLE[KNOWLEDGE_CATEGORY_JOB][1])

        ok, detail = await _expect_http(svc.get_document(db, 999999), 404)
        _check("get_document 读不存在的 id → 404", ok, detail)

        page = await svc.list_documents(db)
        _check("list_documents 返回 {total, items, limit, offset}",
               set(page) == {"total", "items", "limit", "offset"}, str(sorted(page)))
        _check("  └ total == 4 且 items 长度 4",
               page["total"] == 4 and len(page["items"]) == 4,
               f"total={page['total']} items={len(page['items'])}")
        _check("  └ 每项键集合 == DOCUMENT_SUMMARY_FIELDS",
               all(tuple(i.keys()) == svc.DOCUMENT_SUMMARY_FIELDS for i in page["items"]))
        _check("  └ ★ 列表项不含 content（避免整库正文回传）",
               all("content" not in i for i in page["items"]))
        _check("  └ 默认按 id 倒序（最新在前）",
               [i["id"] for i in page["items"]] == sorted(
                   (i["id"] for i in page["items"]), reverse=True),
               str([i["id"] for i in page["items"]]))

        by_cat = await svc.list_documents(db, category=KNOWLEDGE_CATEGORY_TECHNICAL)
        _check("按 category 过滤：technical → 1 篇",
               by_cat["total"] == 1
               and by_cat["items"][0]["category"] == KNOWLEDGE_CATEGORY_TECHNICAL,
               str(by_cat["total"]))

        by_kw = await svc.list_documents(db, keyword="订单")
        _check("按 keyword 检索标题：'订单' → 1 篇",
               by_kw["total"] == 1
               and by_kw["items"][0]["title"] == "订单中台重构复盘",
               str([i["title"] for i in by_kw["items"]]))
        blank_kw = await svc.list_documents(db, keyword="   ")
        _check("  └ 纯空白 keyword 视为不传（total 仍 4）",
               blank_kw["total"] == 4, str(blank_kw["total"]))

        p1 = await svc.list_documents(db, limit=2, offset=0)
        p2 = await svc.list_documents(db, limit=2, offset=2)
        _check("分页：limit=2 返回 2 条但 total 仍 4",
               len(p1["items"]) == 2 and p1["total"] == 4,
               f"items={len(p1['items'])} total={p1['total']}")
        _check("  └ 第二页与第一页不重叠",
               not ({i["id"] for i in p1["items"]} & {i["id"] for i in p2["items"]}))
        _check("  └ 两页合计覆盖全部 4 篇",
               {i["id"] for i in p1["items"]} | {i["id"] for i in p2["items"]}
               == {d["id"] for d in created.values()})

        clamp = await svc.list_documents(db, limit=9999, offset=-5)
        _check("分页参数越界自动收敛（limit→上限，offset→0）",
               clamp["limit"] == svc.MAX_PAGE_SIZE and clamp["offset"] == 0,
               f"limit={clamp['limit']} offset={clamp['offset']}")
        clamp0 = await svc.list_documents(db, limit=0)
        _check("  └ limit=0 收敛为 1", clamp0["limit"] == 1, str(clamp0["limit"]))

        ok, detail = await _expect_http(svc.list_documents(db, category="unknown"), 400)
        _check("列表传非法 category → 400（不静默返回空）", ok, detail)

        # ------------------------------------------------------------
        # [4] 空内容拒绝（要求 3）
        # ------------------------------------------------------------
        print("\n[4] 空内容拒绝（要求 3）")
        before = await _count_docs(db)
        bad_bodies = {
            "content 空串": {"title": "t", "content": "", "category": "technical"},
            "content 纯空白（空格/换行/制表）":
                {"title": "t", "content": "   \n\t  ", "category": "technical"},
            "content=None": {"title": "t", "content": None, "category": "technical"},
            "content 缺失": {"title": "t", "category": "technical"},
        }
        for name, payload in bad_bodies.items():
            ok, detail = await _expect_http(svc.create_document(db, payload), 400)
            _check(f"★ 拒绝：{name}", ok, detail)
        _check("  └ 拒绝时提示指向 content",
               "content" in (await _expect_http(
                   svc.create_document(db, {"title": "t", "content": " ",
                                            "category": "technical"}), 400))[1])

        for name, payload in {
            "title 空串": {"title": "", "content": "正文", "category": "technical"},
            "title 纯空白": {"title": "   ", "content": "正文", "category": "technical"},
            "title 缺失": {"content": "正文", "category": "technical"},
        }.items():
            ok, detail = await _expect_http(svc.create_document(db, payload), 400)
            _check(f"拒绝：{name}", ok, detail)

        for name, payload in {
            "category 非法值": {"title": "t", "content": "正文", "category": "unknown"},
            "category 缺失": {"title": "t", "content": "正文"},
            "category 空串": {"title": "t", "content": "正文", "category": ""},
        }.items():
            ok, detail = await _expect_http(svc.create_document(db, payload), 400)
            _check(f"拒绝：{name}", ok, detail)

        ok, detail = await _expect_http(
            svc.create_document(db, {"title": "", "content": "",
                                     "category": "unknown"}), 400)
        _check("校验顺序：title 与 content 同时空 → 先报 title",
               ok and "title" in detail, detail)

        _check("★ 全部非法请求均未写库（行数不变）",
               await _count_docs(db) == before, f"{before} → {await _count_docs(db)}")
        _check("  └ 非法请求也未生成切片", await _count_chunks(db) == 0)

        # ------------------------------------------------------------
        # [5] 输入形状与字段口径
        # ------------------------------------------------------------
        print("\n[5] 输入形状（dict / 对象）与字段口径")
        obj = SimpleNamespace(title="对象入参文档", content="正文内容",
                              category=KNOWLEDGE_CATEGORY_COMPANY, source="obj://1")
        made = await svc.create_document(db, obj)
        _check("对象（SimpleNamespace）入参同样可创建",
               made["title"] == "对象入参文档" and made["source"] == "obj://1", str(made))

        no_source = await svc.create_document(
            db, {"title": "无来源文档", "content": "正文", "category": "job"})
        _check("source 缺省为 \"\"（不是 None）",
               no_source["source"] == "", repr(no_source["source"]))

        trimmed = await svc.create_document(db, {
            "title": "  带空白的标题  ",
            "content": "  正文首尾有空白\n\n",
            "category": "technical",
            "source": "  spaced://x  ",
        })
        _check("标签类字段去首尾空白（title）",
               trimmed["title"] == "带空白的标题", repr(trimmed["title"]))
        _check("  └ 标签类字段去首尾空白（source）",
               trimmed["source"] == "spaced://x", repr(trimmed["source"]))
        _check("★ 正文字段原样保存（不 strip，保真）",
               trimmed["content"] == "  正文首尾有空白\n\n", repr(trimmed["content"]))
        _check("  └ 正文含换行的多行内容不被压平",
               "\n" in created[KNOWLEDGE_CATEGORY_JOB]["content"])

        _check("多次导入同名文档允许（本阶段刻意不去重）",
               (await svc.list_documents(db, keyword="无来源文档"))["total"] == 1)

    await engine.dispose()

    # ------------------------------------------------------------
    # [6] 边界：不引入 RAG / 不改面试
    # ------------------------------------------------------------
    print("\n[6] 边界：只落库，不引入 RAG，不改面试")
    src = (BACKEND_DIR / "services" / "knowledge_document_service.py").read_text(
        encoding="utf-8")
    mods = _imported_modules(src)
    banned_substrings = ("embedding", "vector", "chroma", "faiss", "milvus",
                         "numpy", "openai", "sentence_transformers", "transformers")
    hit = sorted(m for m in mods
                 if any(b in m.lower() for b in banned_substrings))
    _check("★ 未 import Embedding / 向量库 / 大模型 SDK", hit == [], str(hit))

    banned_mods = {
        "services.knowledge_retriever", "services.interview_core",
        "services.interview_agent", "services.interview_service",
        "services.question_generator", "services.interview_planner",
        "services.interview_context", "services.question_validator",
        "services.avatar_interview", "main",
    }
    hit = sorted(banned_mods & mods)
    _check("★ 未 import Retriever / 任何 Interview 模块（与面试完全解耦）",
           hit == [], str(hit))

    names = _imported_names(src)
    _check("只依赖 KnowledgeDocument，不碰 KnowledgeChunk（本阶段不切片）",
           "KnowledgeDocument" in names and "KnowledgeChunk" not in names, str(sorted(names)))

    # ⚠️ 匹配必须用**点号全名** ``services.knowledge_document_service``：
    # ``_imported_modules`` 收集的是 ``from services.knowledge_document_service import X``
    # 里的点号全名，**从不含裸模块名**。原先写裸名 → 断言恒为真、**从未真正生效**
    # （任务 46 修正）。任务 74 后消费者是**已知闭集三处**：
    #   * services/knowledge_import_pipeline —— 写侧：导入 → 切片 → 向量的编排
    #   * services/knowledge_maintenance    —— 维护：复用 get_document 的 404 口径
    #   * api/knowledge.py                  —— 路由：查询透传
    # 本服务仍然只写主表、不切片。
    offenders = []
    for path in _production_files():
        if path.name == "knowledge_document_service.py":
            continue
        if "services.knowledge_document_service" in _imported_modules(
                path.read_text(encoding="utf-8")):
            offenders.append(path.relative_to(BACKEND_DIR).as_posix())
    _check("★ 接线点收口为已知闭集（导入仍不自动切片）",
           offenders == [
               "api/knowledge.py",
               "services/knowledge_import_pipeline.py",
               "services/knowledge_maintenance.py",
           ], str(offenders))

    for rel in ("services/interview_core.py", "services/interview_service.py",
                "services/interview_agent.py"):
        mods_i = _imported_modules((BACKEND_DIR / rel).read_text(encoding="utf-8"))
        _check(f"  └ {rel} 未被改动（未 import 知识库模型 / 本服务）",
               "models.knowledge" not in mods_i
               and "services.knowledge_document_service" not in mods_i)

    # ------------------------------------------------------------
    # [7] 出参契约恒定（纯函数级）
    # ------------------------------------------------------------
    print("\n[7] 出参契约恒定")
    probe = KnowledgeDocument(title="契约探针", content="正文",
                              category="technical", source="")
    _check("_document_to_dict 键集合 == DOCUMENT_FIELDS",
           tuple(svc._document_to_dict(probe).keys()) == svc.DOCUMENT_FIELDS,
           str(tuple(svc._document_to_dict(probe).keys())))
    _check("_document_summary 键集合 == DOCUMENT_SUMMARY_FIELDS",
           tuple(svc._document_summary(probe).keys()) == svc.DOCUMENT_SUMMARY_FIELDS,
           str(tuple(svc._document_summary(probe).keys())))
    _check("两个契约常量都是元组（顺序稳定）",
           isinstance(svc.DOCUMENT_FIELDS, tuple)
           and isinstance(svc.DOCUMENT_SUMMARY_FIELDS, tuple))
    _check("_document_summary 是 _document_to_dict 的严格子集",
           set(svc.DOCUMENT_SUMMARY_FIELDS) < set(svc.DOCUMENT_FIELDS))

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
