# -*- coding: utf-8 -*-
"""向量后端 **运行状态上报 + 规模告警 + 切换验证** 自检（脚本式，非 pytest）。

无需 pytest，直接运行：
    python backend/tests/test_vector_backend_surface.py

要防的那件事
------------
``VECTOR_STORE`` 不配时默认走 ``vector_store_sql``——它**没有 ANN 索引**，每次检索都把
**全部**切片（含 2560 维向量的 JSON）捞进内存做 Python 侧余弦。规模小的时候完全无感，
规模一大就变成每次出题都读几十 MB。而「现在到底走的是哪条路」以前只能靠人记环境变量，
于是最容易出的问题是：**以为开了 ANN、其实还在全表扫描**（且没有任何报错）。
本套件把这件事变成可见 + 可断言，并顺带验证「真的切过去」这条路径是通的。

覆盖：
[1] ``main.vector_status()`` 形状契约（固定 3 键；成功时 ``error=None``）
[2] 未配置档 ⇒ ``sql`` / ``ann=False``（**静默全表扫描的那一档**）
[3] chroma 档 ⇒ ``chroma`` / ``ann=True``（含取值别名）
[4] 非法档 ⇒ ``error`` 非空且**不抛异常**（健康检查永远不该 500）
[5] 规模告警**纯函数** ``_vector_size_warning``（阈值上下、非 sql 后端、未知规模）
[6] 启动日志 ``_report_vector_status``（常规 / 超阈值告警 / chroma / 配置非法）
[7] ``GET /api/health`` 回带 ``vector``；``status`` / ``service`` / ``embedding`` 向后兼容
[8] 组装器同源：``describe_vector_backend`` 与 ``main.vector_status`` 逐键相等；阈值常量合法
[9] ★ **切换验证**：同一份权威行，``sql`` → ``chroma`` 全链路
    （含「切后端后必须先重建派生索引，否则检索**静默返回 0 条**」这条坑）
[10] AST 守卫：``main.py`` 顶层**不** import ``services.knowledge_rag`` / ``chromadb``

环境钉扎：必须 ``import regression_env``——``.env`` 里的部署取值会被 ``load_dotenv``
带进 ``os.environ``，不钉扎本套件就会读到真实部署配置（不确定）。
``VECTOR_STORE`` / ``CHROMA_PATH`` 也在 ``regression_env.PINNED`` 里，本套件额外
自己快照/还原这两个键（[2]~[9] 会临时改写它们）。
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import io
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import regression_env  # noqa: E402,F401  钉住离线 Embedding + RAG 阈值 + 向量后端
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

import main  # noqa: E402
from database import Base  # noqa: E402
from models import KnowledgeChunk  # noqa: E402
from services import interview_core, knowledge_rag  # noqa: E402
from services.document_chunker import DocumentChunker  # noqa: E402
from services.embedding_service import HashEmbeddingService  # noqa: E402
from services.knowledge_import_pipeline import KnowledgeImportPipeline  # noqa: E402
from services.knowledge_maintenance import rebuild_index  # noqa: E402
from services.knowledge_rag import (  # noqa: E402
    BACKEND_CHROMA,
    BACKEND_SQL,
    ENV_VECTOR_STORE,
    FULL_SCAN_WARN_CHUNKS,
    build_vector_retriever,
    describe_vector_backend,
)

#: ``vector_status()`` 的固定键集。**改它就要同步改这里**（契约）。
VECTOR_STATUS_KEYS = {"backend", "ann", "error"}

#: 本套件会临时改写、事后必须还原的环境变量。
MUTATED_ENV = (ENV_VECTOR_STORE, "CHROMA_PATH")

PARA = "Redis 的 AOF 持久化通过 appendfsync 参数控制刷盘策略，everysec 是常用折中。"
LONG_TEXT = "".join(f"第{i}节：{PARA}" for i in range(40))
CHUNKER = DocumentChunker(chunk_size=200, chunk_overlap=40)

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


def _snapshot_env() -> Dict[str, Any]:
    return {k: os.environ.get(k) for k in MUTATED_ENV}


def _restore_env(saved: Dict[str, Any]) -> None:
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _set_vector_store(value: str) -> None:
    """按项目约定写 ``VECTOR_STORE``：**空串 = 未配置**（不是 pop）。"""
    os.environ[ENV_VECTOR_STORE] = value


def _capture_report(chunks: Optional[int]) -> str:
    """跑一次 ``_report_vector_status`` 并把 stdout 抓回来。"""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        main._report_vector_status(chunks)
    return buf.getvalue()


def _top_level_imports(source: str) -> List[str]:
    """AST：**只看顶层** ``ast.parse(src).body`` 的 import（延迟导入不算）。"""
    out: List[str] = []
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            out.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            out.append(node.module or "")
    return out


def _payload(title: str, content: str, source: str) -> Dict[str, Any]:
    return {"title": title, "content": content, "category": "technical", "source": source}


#: 本套件建的临时索引目录前缀（用于「上次运行的下次启动即回收」）。
TMP_PREFIX = "career_ai_chroma_switch_"


def _sweep_stale_tmp() -> None:
    """删掉**上一次**运行留下的临时索引目录。

    chroma 的本地客户端没有 ``close()``，句柄要等**进程退出**才释放 ⇒ 本次运行
    建的目录在 ``finally`` 里删不掉（Windows 尤其明显）。所以改为：本次启动时
    先把**上次**的残留清掉。这样磁盘上最多只会留一个（正在运行的那一个）。
    """
    import glob

    for path in glob.glob(os.path.join(tempfile.gettempdir(), TMP_PREFIX + "*")):
        shutil.rmtree(path, ignore_errors=True)


async def _run_switch_verification() -> None:
    """[9] 同一份权威行：``sql`` → ``chroma`` 全链路切换验证。

    这是本套件的**核心**。它证明三件事：
    ① 生产组装路径（``build_vector_retriever``）真的会按 ``VECTOR_STORE`` 换后端；
    ② ``chroma`` 是**派生**索引——权威行（``knowledge_chunk``）在、向量也在，
       但**切过去之后不重建就一条也检索不到**（这是最危险的静默失败）；
    ③ 走 ``rebuild_index(purge=True)`` 重建后，检索结果与 ``sql`` 后端一致。
    """
    try:
        from services.vector_store_chroma import ChromaVectorStore
        from services.vector_store_sql import SqlAlchemyVectorStore
    except Exception as exc:  # noqa: BLE001 - chromadb 是可选依赖
        _check("★ chroma 后端可加载（本机应已安装 chromadb）", False, repr(exc))
        return

    _sweep_stale_tmp()
    tmpdir = tempfile.mkdtemp(prefix=TMP_PREFIX)
    os.environ["CHROMA_PATH"] = tmpdir

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    embedder = HashEmbeddingService()
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as db:
            # ---- 阶段 A：默认后端（sql）入库 --------------------------------
            _set_vector_store("")
            _check("  └ 阶段 A：未配置 ⇒ 组装器选 sql",
                   describe_vector_backend()["backend"] == BACKEND_SQL,
                   str(describe_vector_backend()))

            pipeline = KnowledgeImportPipeline(
                db, chunker=CHUNKER, embedder=embedder,   # store=None ⇒ 走组装器
            )
            _check("  └ 管线默认 store 来自组装器（sql）",
                   isinstance(pipeline.store, SqlAlchemyVectorStore),
                   type(pipeline.store).__name__)

            doc = await pipeline.import_document(
                _payload("Redis 持久化切换验证", LONG_TEXT, "manual://switch/redis")
            )
            _check("  └ 入库成功且切成多片",
                   doc["ok"] is True and doc["chunk_count"] > 1,
                   f"ok={doc['ok']} chunks={doc['chunk_count']}")
            n = int(doc["chunk_count"])

            rows = (
                await db.execute(
                    select(KnowledgeChunk).order_by(KnowledgeChunk.id)
                )
            ).scalars().all()
            _check("  └ 权威行都在 knowledge_chunk（含向量）",
                   len(rows) == n and all(bool(r.embedding) for r in rows),
                   f"rows={len(rows)} n={n}")

            probe = rows[0].content

            retriever_sql = build_vector_retriever(db, embedder=embedder, top_k=3)
            _check("  └ 读侧组装出的 store 也是 sql",
                   isinstance(retriever_sql.store, SqlAlchemyVectorStore),
                   type(retriever_sql.store).__name__)
            hits_sql = await retriever_sql.retrieve({}, probe, None)
            _check("  └ sql 后端检索命中自身（score≈1.0）",
                   bool(hits_sql) and hits_sql[0].content == probe
                   and round(hits_sql[0].metadata.get("score", 0), 9) == 1.0,
                   str(hits_sql[0].metadata.get("score") if hits_sql else None))

            # ---- 阶段 B：切到 chroma -----------------------------------------
            _set_vector_store(BACKEND_CHROMA)
            status = describe_vector_backend()
            _check("  └ 阶段 B：VECTOR_STORE=chroma ⇒ ann=True",
                   status["backend"] == BACKEND_CHROMA and status["ann"] is True,
                   str(status))

            retriever_chroma = build_vector_retriever(db, embedder=embedder, top_k=3)
            _check("  └ 读侧组装出的 store 换成 ChromaVectorStore",
                   isinstance(retriever_chroma.store, ChromaVectorStore),
                   type(retriever_chroma.store).__name__)

            hits_before = await retriever_chroma.retrieve({}, probe, None)
            _check("  ★ 切后端后**未重建**派生索引 ⇒ 检索静默返回 0 条"
                   "（权威行与向量都在，坑就在这）",
                   hits_before == [],
                   f"实际命中 {len(hits_before)} 条")

            report = await rebuild_index(db, purge=True)
            _check("  └ rebuild_index(purge=True) 从权威行重建派生索引",
                   report["errors"] == [] and report["rebuilt_chunks"] == n,
                   str(report))
            _check("  └ 重建扫描到的就是那 n 条权威行",
                   report["scanned_chunks"] == n and report["skipped_chunks"] == 0,
                   str(report))

            # ★ purge 会「丢弃集合并重建」⇒ 集合 id 变了 ⇒ **已经持有旧集合句柄的
            #   store 实例会失效**（chroma 报 NotFoundError）。生产里 ``resolve_retriever``
            #   每次出题都新建检索器/后端，因此不受影响；但「先 purge 再复用同一个
            #   store 实例检索」会炸。这里只如实记录现象，**不做断言**——它取决于
            #   chromadb 的版本行为，锁死会变成「升级库就红」。
            try:
                stale_hits = await retriever_chroma.retrieve({}, probe, None)
                stale_note = f"旧实例仍可检索（命中 {len(stale_hits)} 条）"
            except Exception as exc:  # noqa: BLE001 - 只是为了把现象记下来
                stale_note = f"旧实例已失效（{type(exc).__name__}）"
            print(f"  [INFO] purge 丢弃并重建集合 ⇒ {stale_note}；生产里检索器每次新建，故不受影响")

            # 按**生产口径**新建一个检索器（每次出题都新建，见 interview_core.resolve_retriever）
            retriever_chroma2 = build_vector_retriever(db, embedder=embedder, top_k=3)
            hits_after = await retriever_chroma2.retrieve({}, probe, None)
            _check("  ★ 重建后 chroma 检索命中自身（与 sql 后端一致）",
                   bool(hits_after) and hits_after[0].content == probe
                   and round(hits_after[0].metadata.get("score", 0), 9) == 1.0,
                   str(hits_after[0].metadata.get("score") if hits_after else None))

            # ---- 阶段 C：生产面试链路（interview_core）也走通 -----------------
            resolved, warns = interview_core.resolve_retriever(db, use_rag=True)
            _check("  └ interview_core.resolve_retriever(use_rag=True) 拿到真实检索器",
                   resolved is not None and warns == [], str(warns))
            if resolved is not None:
                _check("  └ 它用的后端就是 chroma（环境变量贯通到面试链路）",
                       isinstance(resolved.store, ChromaVectorStore),
                       type(resolved.store).__name__)
                hits_core = await resolved.retrieve({}, probe, None)
                _check("  ★ 面试链路在 chroma 后端下同样检索到知识",
                       bool(hits_core) and hits_core[0].content == probe,
                       str(len(hits_core)))
    finally:
        await engine.dispose()
        _set_vector_store("")
        os.environ.pop("CHROMA_PATH", None)
        shutil.rmtree(tmpdir, ignore_errors=True)
        if os.path.isdir(tmpdir):
            # chroma 的本地客户端**没有 close()**，句柄要等进程退出才释放；
            # Windows 上因此删不掉。如实报出来，别假装清理干净了。
            print(f"  [INFO] 临时索引目录未能删除（chroma 句柄未释放）：{tmpdir}")


async def run() -> bool:
    print("=" * 72)
    print("向量后端运行状态上报 / 规模告警 / 切换验证自检")
    print("=" * 72)

    saved = _snapshot_env()

    # ---------------- [1] 形状契约 ----------------
    print("\n[1] 形状契约（固定 3 键）")
    _set_vector_store("")
    status = main.vector_status()
    _check("键集恰好为 3 键", set(status) == VECTOR_STATUS_KEYS, str(sorted(status)))
    _check("成功时 error is None", status["error"] is None, repr(status["error"]))
    _check("ann 是**真 bool**（不是 0 / 不是字符串）",
           isinstance(status["ann"], bool), repr(status["ann"]))
    _check("backend 是 str", isinstance(status["backend"], str), repr(status["backend"]))

    # ---------------- [2] 未配置档 ----------------
    print("\n[2] 未配置档 ⇒ sql（**这就是以前完全静默的全表扫描**）")
    _set_vector_store("")
    status = main.vector_status()
    _check("★ backend == sql", status["backend"] == BACKEND_SQL, repr(status["backend"]))
    _check("★ ann is False（没有 ANN 索引）", status["ann"] is False, repr(status["ann"]))
    _check("error is None（未配置**不是**配置错误）", status["error"] is None, repr(status["error"]))
    _check("空白取值也按未配置处理",
           describe_vector_backend({ENV_VECTOR_STORE: "   "})["backend"] == BACKEND_SQL,
           str(describe_vector_backend({ENV_VECTOR_STORE: "   "})))

    # ---------------- [3] chroma 档 ----------------
    print("\n[3] chroma 档 ⇒ ann=True（含取值别名）")
    _set_vector_store(BACKEND_CHROMA)
    status = main.vector_status()
    _check("★ backend == chroma", status["backend"] == BACKEND_CHROMA, repr(status["backend"]))
    _check("★ ann is True", status["ann"] is True, repr(status["ann"]))
    _check("error is None", status["error"] is None, repr(status["error"]))
    for alias in ("CHROMA", "chromadb", " chroma "):
        got = describe_vector_backend({ENV_VECTOR_STORE: alias})
        _check(f"  └ 别名 {alias!r} 也归一到 chroma",
               got["backend"] == BACKEND_CHROMA and got["ann"] is True, str(got))

    # ---------------- [4] 配置非法档 ----------------
    print("\n[4] 配置非法档 ⇒ 错误可见、但**不抛异常**")
    _set_vector_store("faiss")
    try:
        status = main.vector_status()
        raised = None
    except Exception as exc:  # noqa: BLE001
        status, raised = {}, exc
    _check("★ 不抛异常（健康检查永远不该 500）", raised is None, repr(raised))
    _check("error 非空", bool(status.get("error")), repr(status.get("error")))
    _check("backend is None", status.get("backend") is None, repr(status.get("backend")))
    _check("ann is None（未知，不伪装成 False）", status.get("ann") is None, repr(status.get("ann")))
    _check("键集仍为 3 键", set(status) == VECTOR_STATUS_KEYS, str(sorted(status)))
    _check("错误里带得出具体原因", "VECTOR_STORE" in str(status.get("error")),
           str(status.get("error"))[:140])

    # ---------------- [5] 规模告警纯函数 ----------------
    print("\n[5] 规模告警纯函数（不读库、可单测）")
    warn = main._vector_size_warning
    _check("★ sql + 未达阈值 ⇒ None",
           warn(BACKEND_SQL, FULL_SCAN_WARN_CHUNKS - 1) is None,
           repr(warn(BACKEND_SQL, FULL_SCAN_WARN_CHUNKS - 1)))
    _check("★ sql + 恰好达阈值 ⇒ 有文案（含等号）",
           isinstance(warn(BACKEND_SQL, FULL_SCAN_WARN_CHUNKS), str),
           repr(warn(BACKEND_SQL, FULL_SCAN_WARN_CHUNKS)))
    _check("★ sql + 超阈值 ⇒ 文案含实际切片数",
           str(FULL_SCAN_WARN_CHUNKS + 500) in str(warn(BACKEND_SQL, FULL_SCAN_WARN_CHUNKS + 500)),
           str(warn(BACKEND_SQL, FULL_SCAN_WARN_CHUNKS + 500)))
    _check("★ chroma + 超阈值 ⇒ None（已是 ANN，无需告警）",
           warn(BACKEND_CHROMA, FULL_SCAN_WARN_CHUNKS * 10) is None,
           repr(warn(BACKEND_CHROMA, FULL_SCAN_WARN_CHUNKS * 10)))
    _check("★ 切片数未知（None）⇒ None（不误报）",
           warn(BACKEND_SQL, None) is None, repr(warn(BACKEND_SQL, None)))
    _check("★ 文案点明「建议切 chroma」", "chroma" in str(warn(BACKEND_SQL, 10 ** 6)),
           str(warn(BACKEND_SQL, 10 ** 6))[:140])

    # ---------------- [6] 启动日志 ----------------
    print("\n[6] 启动日志（全表扫描 + 规模超阈值必须告警）")
    _set_vector_store("")
    text = _capture_report(32)
    _check("常规：含 backend=sql 与 ann=False",
           "backend=sql" in text and "ann=False" in text, repr(text))
    _check("常规：含实测切片数", "chunks=32" in text, repr(text))
    _check("常规：不误报 WARN", "WARN" not in text, repr(text))

    text = _capture_report(FULL_SCAN_WARN_CHUNKS)
    _check("★ 超阈值：含 WARN", "WARN" in text, repr(text))
    _check("★ 超阈值：点明全表扫描的代价", "全表扫描" in text, repr(text))
    _check("★ 超阈值：给出可执行建议（VECTOR_STORE=chroma）",
           "VECTOR_STORE=chroma" in text, repr(text))

    _set_vector_store(BACKEND_CHROMA)
    text = _capture_report(10 ** 6)
    _check("chroma：含 ann=True 且**无** WARN",
           "ann=True" in text and "WARN" not in text, repr(text))

    _set_vector_store("faiss")
    text = _capture_report(1)
    _check("配置非法：走 ERROR 分支（不静默）",
           "ERROR" in text and "VECTOR_STORE" in text, repr(text))

    # ---------------- [7] /api/health ----------------
    print("\n[7] GET /api/health 回带 vector（且向后兼容）")
    _set_vector_store("")
    transport = ASGITransport(app=main.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/api/health")
        _check("状态码 200", r.status_code == 200, str(r.status_code))
        body = r.json()
        _check("★ 既有键 status 未变", body.get("status") == "ok", repr(body.get("status")))
        _check("★ 既有键 service 未变",
               body.get("service") == "Career.ai FastAPI", repr(body.get("service")))
        _check("★ 既有键 embedding 仍在且是 dict",
               isinstance(body.get("embedding"), dict), repr(body.get("embedding")))
        _check("★ 新增键 vector 存在", "vector" in body, str(list(body.keys())))
        _check("vector 键集为 3 键",
               set(body.get("vector") or {}) == VECTOR_STATUS_KEYS,
               str(sorted((body.get("vector") or {}).keys())))
        _check("vector 与 vector_status() 同源（逐键相等）",
               body.get("vector") == main.vector_status(),
               f"{body.get('vector')} vs {main.vector_status()}")
        _check("未配置环境下报 sql / ann=False",
               body["vector"]["backend"] == BACKEND_SQL and body["vector"]["ann"] is False,
               str(body["vector"]))

        _set_vector_store(BACKEND_CHROMA)
        body2 = (await client.get("/api/health")).json()
        _check("★ 切到 chroma 后 health 如实变化（配置是请求期读取的）",
               body2["vector"]["backend"] == BACKEND_CHROMA
               and body2["vector"]["ann"] is True,
               str(body2["vector"]))

    # ---------------- [8] 组装器同源 + 常量合法 ----------------
    print("\n[8] 组装器同源与阈值常量")
    _set_vector_store("")
    _check("★ describe_vector_backend() 与 main.vector_status() 逐键相等",
           describe_vector_backend() == main.vector_status(),
           f"{describe_vector_backend()} vs {main.vector_status()}")
    _check("★ FULL_SCAN_WARN_CHUNKS 是正整数（且不是 bool）",
           isinstance(FULL_SCAN_WARN_CHUNKS, int)
           and not isinstance(FULL_SCAN_WARN_CHUNKS, bool)
           and FULL_SCAN_WARN_CHUNKS >= 1,
           repr(FULL_SCAN_WARN_CHUNKS))
    _check("★ 常量已进 knowledge_rag.__all__（对外契约）",
           "FULL_SCAN_WARN_CHUNKS" in knowledge_rag.__all__,
           str(knowledge_rag.__all__))
    _check("★ describe_vector_backend 已进 knowledge_rag.__all__",
           "describe_vector_backend" in knowledge_rag.__all__,
           str(knowledge_rag.__all__))
    _check("★ 只读配置：describe_vector_backend 不会把 chromadb 拉起来",
           "chromadb" not in sys.modules,
           str([m for m in sys.modules if "chroma" in m]))

    # ---------------- [9] 切换验证 ----------------
    print("\n[9] ★ 切换验证：同一份权威行，sql → chroma 全链路")
    await _run_switch_verification()

    # ---------------- [10] AST 守卫 ----------------
    print("\n[10] 源码守卫（AST）：main.py 顶层不 import 向量/Embedding 组装器")
    _restore_env(saved)
    main_src = (BACKEND_DIR / "main.py").read_text(encoding="utf-8")
    top = _top_level_imports(main_src)
    _check("★ 顶层**不**出现 services.knowledge_rag（必须函数内延迟导入）",
           "services.knowledge_rag" not in top,
           str([m for m in top if "knowledge" in m or "services" in m]))
    _check("顶层不出现 chromadb（可选依赖不能被启动路径拉起）",
           "chromadb" not in top, str([m for m in top if "chroma" in m]))
    _check("顶层不出现 services.vector_store_chroma",
           "services.vector_store_chroma" not in top,
           str([m for m in top if "vector" in m]))
    # 守卫自检：确认这个判据**能**失败（否则恒为真）
    fake = "from services.knowledge_rag import describe_vector_backend\n"
    _check("守卫自检：顶层 import 时判据确实会命中",
           "services.knowledge_rag" in _top_level_imports(fake),
           str(_top_level_imports(fake)))

    _restore_env(saved)

    print("\n" + "=" * 72)
    print(f"通过 {_PASSED} / {_PASSED + _FAILED}")
    print("=" * 72)
    return _FAILED == 0


def main_entry() -> None:
    ok = asyncio.run(run())
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main_entry()
