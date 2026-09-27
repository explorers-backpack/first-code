# -*- coding: utf-8 -*-
"""AI 面试知识库 · **向量迁移**（``knowledge_embedding_migration``）自检（脚本式，非 pytest）。

对应需求：旧知识库向量由 HashEmbedding 产生、无法继续使用，
需在**不重新切片、不改切片正文**的前提下把向量重算成当前真实 Embedding。

[1] 契约：出参 16 键恒定 + 状态 / 阶段常量 + ``needs_migration`` 真值表
[2] 构造期校验（db / embedder / batch_size / pace / retries / target_model）
[3] dry-run：只扫描不编码不写库
[4] 迁移正确性：向量已换，**切片正文 / 元数据 / 归属 / 行数逐字节不变**
[5] 幂等：复跑是空操作（**一次编码都不发生**）
[6] 判定边界：模型不符 / 维度不符 / 无向量 各自触发迁移
[7] 失败恢复①：**单片编码失败** → 该片记明细、同批其它片照常迁移 → 复跑精确补漏
[8] 失败恢复②：**整批写回失败** → 回滚且旧向量未被破坏 → 下一批继续 → 复跑成功
[9] 失败恢复③：``max_failures`` 提前停止 + ``chunk_ids`` 精确重跑
[10] 隔离守卫（AST）：不改 ``VectorStore`` / ``Retriever`` 接口、不碰具体后端、不引数据层以外依赖

运行：``python tests/test_knowledge_embedding_migration.py``
"""

from __future__ import annotations

import ast
import asyncio
import hashlib
import inspect
import os
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from sqlalchemy import func, select  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from database import Base  # noqa: E402
from models import KnowledgeChunk, KnowledgeDocument  # noqa: E402
from services.embedding_service import EmbeddingUnavailableError  # noqa: E402
from services.knowledge_embedding_migration import (  # noqa: E402
    DEFAULT_BATCH_SIZE,
    DEFAULT_PACE_SECONDS,
    DEFAULT_RETRIES,
    MIGRATION_RESULT_FIELDS,
    STAGE_DONE,
    STAGE_EMBED,
    STAGE_PERSIST,
    STATUS_FAILED,
    STATUS_OK,
    STATUS_PARTIAL,
    STATUS_SKIPPED,
    EmbeddingMigration,
    MigrationConfigError,
    _new_report,
    migrate_embeddings,
    needs_migration,
)
from services.knowledge_retriever import KnowledgeRetriever  # noqa: E402
from services.vector_knowledge_retriever import VectorKnowledgeRetriever  # noqa: E402
from services.vector_store import VectorStore  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

_PASSED = 0
_FAILED = 0

#: 旧模型（离线占位）——「待迁移」的来源
LEGACY_MODEL = "hash-local"
LEGACY_DIM = 8

#: 新模型（替身，语义实现）——「迁移目标」
NEW_MODEL = "fake-real-model"
NEW_DIM = 4


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name
          + (f"  -> {detail}" if detail and not cond else ""))
    return cond


# ============================================================
# 替身
# ============================================================
class FakeEmbedder:
    """语义替身：``embed`` 记录调用、可按文本注入失败、返回固定维度向量。

    **不是** ``EmbeddingService`` 子类——本模块只要求鸭子类型 ``embed`` + ``name``
    （与 ``build_vector_retriever`` 的「必须提供 async embed」同一取舍）。
    """

    name = NEW_MODEL
    dimension = NEW_DIM
    semantic_enabled = True

    def __init__(self, *, fail_on=(), error=None):
        self.calls = []
        self.fail_on = set(fail_on)
        self.error = error or EmbeddingUnavailableError("上游抖动")

    async def embed(self, text):
        self.calls.append(text)
        if text in self.fail_on:
            raise self.error
        # 固定但可区分的向量（同一文本恒定 → 便于断言「写进去的就是这次算的」）
        digest = hashlib.blake2b(text.encode("utf-8"), digest_size=8).digest()
        value = int.from_bytes(digest, "big")
        return [float((value >> (8 * i)) & 0xFF) / 255.0 for i in range(NEW_DIM)]


class FailingStore:
    """写回必失败的替身（模拟库/磁盘问题）。"""

    name = "failing-store"

    def __init__(self):
        self.calls = 0

    async def add(self, records):
        self.calls += 1
        raise RuntimeError("模拟写回失败")


class FlakyStore:
    """前 ``fail_times`` 次写回失败，之后转发给真实后端（模拟**可恢复**的失败）。"""

    name = "flaky-store"

    def __init__(self, inner, *, fail_times=1):
        self.inner = inner
        self.fail_times = fail_times
        self.calls = 0

    async def add(self, records):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("模拟瞬时写回失败")
        return await self.inner.add(records)


class RecordingStore:
    """只记录、不落库（用于验证 dry-run 一条都不写）。"""

    name = "recording-store"

    def __init__(self):
        self.batches = []

    async def add(self, records):
        self.batches.append(list(records))
        return len(records)


# ============================================================
# 夹具
# ============================================================
async def _make_engine():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return engine


def _legacy_vector(seed: int) -> list:
    """旧模型产出的定长向量（值本身不重要，只要「迁移前」可识别）。"""
    return [float(seed)] + [0.0] * (LEGACY_DIM - 1)


async def _seed(session, texts, *, title="迁移用例", with_vector=True,
                model=LEGACY_MODEL, dim=LEGACY_DIM):
    """建 1 篇文档 + N 个切片（默认带旧向量）。返回 ``document_id``。"""
    document = KnowledgeDocument(
        title=title, content="\n\n".join(texts), category="technical",
        source="migration-test://",
    )
    session.add(document)
    await session.flush()
    for index, text in enumerate(texts):
        session.add(KnowledgeChunk(
            document_id=document.id,
            content=text,
            chunk_metadata={"chunk_index": index, "category": "technical",
                            "topic": f"t{index}"},
            embedding=_legacy_vector(index) if with_vector else None,
            embedding_model=model if with_vector else "",
            embedding_dim=dim if with_vector else None,
        ))
    await session.commit()
    return document.id


async def _snapshot(session):
    """把「切片行」除向量三列以外的全部字段快照下来（用于断言**没被改**）。"""
    rows = (await session.execute(
        select(KnowledgeChunk).order_by(KnowledgeChunk.id)
    )).scalars().all()
    return [
        (row.id, row.document_id, row.content, row.chunk_metadata)
        for row in rows
    ]


async def _vectors(session):
    """``{chunk_id: (vector_len, model, dim)}``。"""
    rows = (await session.execute(
        select(KnowledgeChunk).order_by(KnowledgeChunk.id)
    )).scalars().all()
    return {
        row.id: (
            len(row.embedding) if isinstance(row.embedding, list) else None,
            row.embedding_model,
            row.embedding_dim,
        )
        for row in rows
    }


# ============================================================
# AST / 文件工具
# ============================================================
def _imported_modules(source: str) -> set:
    """AST 解析出的被 import 模块名（**不用子串匹配**——docstring 会误伤）。"""
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module)
                for alias in node.names:
                    names.add(f"{node.module}.{alias.name}")
    return names


def _module_level_imports(source: str) -> set:
    """**模块顶层**（不含函数体内）的 import 模块名。"""
    names = set()
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module)
                for alias in node.names:
                    names.add(f"{node.module}.{alias.name}")
    return names


def _production_files():
    """backend 下的生产代码文件（排除 tests / __pycache__）。"""
    files = []
    for pattern in ("*.py", "api/*.py", "services/*.py", "models/*.py",
                    "schemas/*.py", "utils/*.py"):
        for path in BACKEND_DIR.glob(pattern):
            if "__pycache__" in path.parts:
                continue
            files.append(path)
    return sorted(set(files))


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    module_path = BACKEND_DIR / "services" / "knowledge_embedding_migration.py"
    src = module_path.read_text(encoding="utf-8")

    # ------------------------------------------------------------
    print("\n[1] 契约：出参键集合 / 常量 / needs_migration 真值表")
    # ------------------------------------------------------------
    _check("报告键集合恰好是 MIGRATION_RESULT_FIELDS（16 键）",
           len(MIGRATION_RESULT_FIELDS) == 16, str(MIGRATION_RESULT_FIELDS))
    blank = _new_report(target_model=NEW_MODEL, target_dim=NEW_DIM, dry_run=False)
    _check("  └ _new_report 的键与顺序完全一致（调用方无分支取值）",
           tuple(blank.keys()) == MIGRATION_RESULT_FIELDS, str(list(blank.keys())))
    _check("  └ 空白报告默认是「无事可做」的 skipped / ok",
           blank["status"] == STATUS_SKIPPED and blank["ok"] is True
           and blank["stage"] == STAGE_DONE, str(blank))
    _check("四个状态互不重复",
           len({STATUS_OK, STATUS_SKIPPED, STATUS_PARTIAL, STATUS_FAILED}) == 4)
    _check("阶段常量互不重复且含 embed / persist",
           {STAGE_EMBED, STAGE_PERSIST, STAGE_DONE}.__len__() == 3)
    _check("默认值：batch_size=8 / pace=0 / retries=0（不引入额外等待）",
           DEFAULT_BATCH_SIZE == 8 and DEFAULT_PACE_SECONDS == 0.0
           and DEFAULT_RETRIES == 0)

    cases = [
        # (has_vector, model, dim, 期望)
        (False, "", None, True),                          # 没向量
        (True, LEGACY_MODEL, LEGACY_DIM, True),           # 换了模型
        (True, NEW_MODEL, LEGACY_DIM, True),              # 同模型但维度不符
        (True, NEW_MODEL, NEW_DIM, False),                # 同模型同维度 → 跳过
        (True, NEW_MODEL, None, True),                    # 同模型但维度缺失
    ]
    ok = True
    for has_vector, model, dim, expected in cases:
        got = needs_migration(has_vector=has_vector, model=model, dim=dim,
                              target_model=NEW_MODEL, target_dim=NEW_DIM)
        ok = ok and got == expected
    _check("needs_migration 真值表（无向量/换模型/维度不符 → 迁移；同模型同维度 → 跳过）",
           ok)
    _check("  └ target_dim<=0（实现不声明维度）时不按维度判定",
           needs_migration(has_vector=True, model=NEW_MODEL, dim=999,
                           target_model=NEW_MODEL, target_dim=0) is False)

    # ------------------------------------------------------------
    print("\n[2] 构造期校验（配置问题当场报，不伪装成「跑完发现没迁」）")
    # ------------------------------------------------------------
    engine = await _make_engine()
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as db:
        def _rejected(fn):
            try:
                fn()
            except MigrationConfigError:
                return True
            return False

        _check("db=None → MigrationConfigError",
               _rejected(lambda: EmbeddingMigration(None)))
        _check("embedder 没有 embed → 报错",
               _rejected(lambda: EmbeddingMigration(db, embedder=object())))
        _check("embedder 的 name 为空 → 报错（embedding_model 是唯一重算依据）",
               _rejected(lambda: EmbeddingMigration(
                   db, embedder=type("E", (), {"embed": lambda s, t: None,
                                               "name": ""})())))
        _check("target_model 空串 → 报错",
               _rejected(lambda: EmbeddingMigration(
                   db, embedder=FakeEmbedder(), target_model="")))
        _check("batch_size=True → 报错（bool 是 int 子类，项目老坑）",
               _rejected(lambda: EmbeddingMigration(
                   db, embedder=FakeEmbedder(), batch_size=True)))
        _check("batch_size=0 → 报错",
               _rejected(lambda: EmbeddingMigration(
                   db, embedder=FakeEmbedder(), batch_size=0)))
        _check("pace_seconds=-1 / NaN → 报错",
               _rejected(lambda: EmbeddingMigration(
                   db, embedder=FakeEmbedder(), pace_seconds=-1))
               and _rejected(lambda: EmbeddingMigration(
                   db, embedder=FakeEmbedder(), pace_seconds=float("nan"))))
        _check("retries=True → 报错",
               _rejected(lambda: EmbeddingMigration(
                   db, embedder=FakeEmbedder(), retries=True)))
        _check("默认 target_model/target_dim 取自 embedder",
               (lambda m: (m.target_model, m.target_dim))(
                   EmbeddingMigration(db, embedder=FakeEmbedder()))
               == (NEW_MODEL, NEW_DIM))

        # ------------------------------------------------------------
        print("\n[3] dry-run：只扫描、不编码、不写库")
        # ------------------------------------------------------------
        doc_id = await _seed(db, ["A 段正文", "B 段正文", "C 段正文"])
        before_rows = await _snapshot(db)
        before_vectors = await _vectors(db)
        recorder = RecordingStore()
        probe_embedder = FakeEmbedder()
        dry = await EmbeddingMigration(
            db, embedder=probe_embedder, store=recorder, dry_run=True
        ).run()
        _check("dry-run 报告 status=ok（有待迁移）+ dry_run=True",
               dry["status"] == STATUS_OK and dry["dry_run"] is True, str(dry))
        _check("  └ 扫描 3 片、待迁移 3 片、迁移 0 片",
               (dry["scanned_chunks"], dry["pending_chunks"], dry["migrated_chunks"])
               == (3, 3, 0), str(dry))
        _check("★ dry-run **一次都没有编码**（不花上游配额）",
               probe_embedder.calls == [], str(probe_embedder.calls))
        _check("★ dry-run 一条都没写向量库",
               recorder.batches == [], str(recorder.batches))
        _check("★ dry-run 后切片行与向量完全未变",
               await _snapshot(db) == before_rows
               and await _vectors(db) == before_vectors)

        # ------------------------------------------------------------
        print("\n[4] 迁移正确性：向量已换，切片本体逐字节不变")
        # ------------------------------------------------------------
        embedder = FakeEmbedder()
        report = await EmbeddingMigration(db, embedder=embedder).run()
        expected_vectors = [await embedder.embed(t)
                            for t in ("A 段正文", "B 段正文", "C 段正文")]
        _check("报告 status=ok / ok=True / stage=done",
               report["status"] == STATUS_OK and report["ok"] is True
               and report["stage"] == STAGE_DONE, str(report))
        _check("  └ 扫描 3 / 待迁移 3 / 已迁移 3 / 跳过 0 / 失败 0",
               (report["scanned_chunks"], report["pending_chunks"],
                report["migrated_chunks"], report["skipped_chunks"],
                report["failed_chunks"]) == (3, 3, 3, 0, 0), str(report))
        _check("  └ target_model / target_dim 如实上报",
               (report["target_model"], report["target_dim"]) == (NEW_MODEL, NEW_DIM))
        _check("  └ 3 片 = 1 批（batch_size=8）",
               report["batches"] == 1, str(report["batches"]))

        rows = (await db.execute(
            select(KnowledgeChunk).order_by(KnowledgeChunk.id)
        )).scalars().all()
        _check("★ 每行向量都换成了新模型算出来的那个（逐值相等）",
               [list(row.embedding) for row in rows] == expected_vectors)
        _check("★ embedding_model 全部更新为 target_model",
               all(row.embedding_model == NEW_MODEL for row in rows))
        _check("★ embedding_dim 全部更新为新维度",
               all(row.embedding_dim == NEW_DIM for row in rows))
        _check("★ 行数不变（迁移是覆盖写，不新建/不删除切片）",
               len(rows) == 3, str(len(rows)))
        _check("★ 切片正文 / 元数据 / 文档归属**逐字节不变**",
               await _snapshot(db) == before_rows)
        _check("  └ chunk_index 也未漂移",
               [r.chunk_metadata.get("chunk_index") for r in rows] == [0, 1, 2])

        # ------------------------------------------------------------
        print("\n[5] 幂等：复跑是空操作（一次编码都不发生）")
        # ------------------------------------------------------------
        again = FakeEmbedder()
        second = await EmbeddingMigration(db, embedder=again).run()
        _check("复跑 status=skipped / ok=True（成功的空操作，不是失败）",
               second["status"] == STATUS_SKIPPED and second["ok"] is True,
               str(second))
        _check("  └ 待迁移 0 / 已迁移 0 / 跳过 3",
               (second["pending_chunks"], second["migrated_chunks"],
                second["skipped_chunks"]) == (0, 0, 3), str(second))
        _check("★ 复跑**一次编码都没发生**（幂等判据在编码之前生效）",
               again.calls == [], str(again.calls))
        _check("  └ 向量仍是上一次那批（没被重写）",
               [list(r.embedding) for r in (await db.execute(
                   select(KnowledgeChunk).order_by(KnowledgeChunk.id)
               )).scalars().all()] == expected_vectors)

        # ------------------------------------------------------------
        print("\n[6] 判定边界：维度不符 / 无向量 各自触发迁移")
        # ------------------------------------------------------------
        dim_doc = await _seed(db, ["维度不符片"], title="维度用例",
                              model=NEW_MODEL, dim=LEGACY_DIM)
        null_doc = await _seed(db, ["无向量片"], title="无向量用例",
                               with_vector=False)
        mixed = await EmbeddingMigration(db, embedder=FakeEmbedder()).run()
        _check("模型对但维度不符 → 迁移（同模型换维度也要重算）",
               mixed["migrated_chunks"] == 2 and mixed["skipped_chunks"] == 3,
               str(mixed))
        fresh = await _vectors(db)
        _check("  └ 该两片现在都是新模型新维度",
               all(fresh[cid][1:] == (NEW_MODEL, NEW_DIM)
                   for cid in fresh if fresh[cid][0] is not None))
        _check("  └ 新文档的切片归属正确（未被挪到别的文档）",
               (await db.execute(
                   select(func.count()).select_from(KnowledgeChunk)
                   .where(KnowledgeChunk.document_id == dim_doc)
               )).scalar_one() == 1
               and (await db.execute(
                   select(func.count()).select_from(KnowledgeChunk)
                   .where(KnowledgeChunk.document_id == null_doc)
               )).scalar_one() == 1)

        # ------------------------------------------------------------
        print("\n[7] 失败恢复①：单片编码失败 → 记明细 + 同批其它片照常迁移")
        # ------------------------------------------------------------
        texts = ["P 段", "Q 段", "R 段", "S 段"]
        await _seed(db, texts, title="编码失败用例")
        partial_embedder = FakeEmbedder(fail_on={"Q 段"})
        partial = await EmbeddingMigration(
            db, embedder=partial_embedder, batch_size=2
        ).run()
        _check("status=partial / ok=False（不假装成功）",
               partial["status"] == STATUS_PARTIAL and partial["ok"] is False,
               str(partial))
        _check("  └ 已迁移 3 / 失败 1 / 跳过 5（前两节累计已迁移的 5 片）",
               (partial["migrated_chunks"], partial["failed_chunks"],
                partial["skipped_chunks"]) == (3, 1, 5), str(partial))
        _check("  └ 失败明细带 chunk_id / stage=embed / 原始异常类名",
               len(partial["failures"]) == 1
               and partial["failures"][0]["stage"] == STAGE_EMBED
               and "EmbeddingUnavailableError" in partial["failures"][0]["error"],
               str(partial["failures"]))
        _check("  └ error 字段非空（第一条失败的可读信息）",
               partial["error"] != "" and "EmbeddingUnavailableError" in partial["error"])
        _check("  └ 同批里**没失败的那片**照常迁移（失败被隔离在单片）",
               partial["migrated_chunks"] == 3)

        failed_id = partial["failures"][0]["chunk_id"]
        rows = (await db.execute(
            select(KnowledgeChunk).where(KnowledgeChunk.id == failed_id)
        )).scalars().all()
        _check("★ 失败那片的**旧向量与正文都完好**（没写坏数据）",
               rows[0].embedding_model == LEGACY_MODEL
               and rows[0].content == "Q 段"
               and rows[0].chunk_metadata.get("chunk_index") == 1)

        retry = await EmbeddingMigration(
            db, embedder=FakeEmbedder()
        ).run(chunk_ids=[failed_id])
        _check("★ 复跑（chunk_ids 精确指定）只补那一片：迁移 1 / 跳过 0",
               (retry["migrated_chunks"], retry["skipped_chunks"]) == (1, 0),
               str(retry))
        after_retry = await _vectors(db)
        _check("  └ 该片现在已是新模型新维度",
               after_retry[failed_id][1:] == (NEW_MODEL, NEW_DIM))

        # ------------------------------------------------------------
        print("\n[8] 失败恢复②：整批写回失败 → 回滚且旧向量未被破坏 → 下一批继续")
        # ------------------------------------------------------------
        await _seed(db, ["W 段", "X 段", "Y 段", "Z 段"], title="写回失败用例")
        snapshot_before = await _snapshot(db)
        vectors_before = await _vectors(db)
        failing = FailingStore()
        broken = await EmbeddingMigration(
            db, embedder=FakeEmbedder(), store=failing, batch_size=2
        ).run()
        _check("整批写回失败 → status=failed（一条都没成功）",
               broken["status"] == STATUS_FAILED and broken["ok"] is False,
               str(broken))
        _check("  └ 该批**全部**切片记进失败明细（不是只记一条）",
               broken["failed_chunks"] == 4
               and all(f["stage"] == STAGE_PERSIST for f in broken["failures"]),
               str(broken["failures"]))
        _check("  └ errors 里有批次级可读信息",
               len(broken["errors"]) == 2
               and all("模拟写回失败" in e for e in broken["errors"]),
               str(broken["errors"]))
        _check("★ 回滚生效：切片行与旧向量**完全未变**",
               await _snapshot(db) == snapshot_before
               and await _vectors(db) == vectors_before)

        flaky = FlakyStore(SqlAlchemyVectorStore(db), fail_times=1)
        recovered = await EmbeddingMigration(
            db, embedder=FakeEmbedder(), store=flaky, batch_size=2
        ).run()
        _check("★ 会话回滚后仍可用：第 2 批成功（部分恢复）",
               recovered["status"] == STATUS_PARTIAL
               and recovered["migrated_chunks"] == 2
               and recovered["failed_chunks"] == 2, str(recovered))
        final = await EmbeddingMigration(db, embedder=FakeEmbedder()).run()
        _check("  └ 再跑一次把剩下的补齐（幂等 + 断点续做）",
               final["migrated_chunks"] == 2 and final["failed_chunks"] == 0,
               str(final))
        _check("  └ 最终全部是新模型新维度",
               all(v[1] == NEW_MODEL and v[2] == NEW_DIM
                   for v in (await _vectors(db)).values()),
               str(await _vectors(db)))

        # ------------------------------------------------------------
        print("\n[9] 失败恢复③：max_failures 提前停止 + limit 小样本")
        # ------------------------------------------------------------
        await _seed(db, [f"F{i}" for i in range(6)], title="提前停止用例")
        all_fail = FakeEmbedder(fail_on={f"F{i}" for i in range(6)})
        stopped = await EmbeddingMigration(
            db, embedder=all_fail, batch_size=2, max_failures=2
        ).run()
        _check("max_failures=2 → 只跑了 1 批就停，stopped_early=True",
               stopped["stopped_early"] is True and stopped["batches"] == 1,
               str(stopped))
        _check("  └ 只记 2 条失败（剩余 4 片未尝试）",
               stopped["failed_chunks"] == 2, str(stopped["failed_chunks"]))

        limit_doc = await _seed(db, ["L0 段", "L1 段", "L2 段"], title="limit 用例")
        limited = await EmbeddingMigration(
            db, embedder=FakeEmbedder()
        ).run(document_id=limit_doc, limit=2)
        _check("document_id + limit=2 → 只扫描该文档前 2 片、只迁移 2 片",
               (limited["scanned_chunks"], limited["migrated_chunks"]) == (2, 2),
               str(limited))
        remaining = (await db.execute(
            select(KnowledgeChunk)
            .where(KnowledgeChunk.document_id == limit_doc)
            .order_by(KnowledgeChunk.id)
        )).scalars().all()
        _check("  └ 第 3 片仍是旧模型（limit 是「取前 N 片」而不是「只迁 N 片」的错觉）",
               remaining[2].embedding_model == LEGACY_MODEL
               and remaining[2].content == "L2 段")

        # ------------------------------------------------------------
        print("\n[10] 隔离守卫（AST 取证：不改接口、不碰具体后端）")
        # ------------------------------------------------------------
        imports = _imported_modules(src)
        top_level = _module_level_imports(src)
        _check("★ 只消费 VectorStore / Retriever 的**既有接口**（一行未改）",
               "services.vector_store" in imports)
        _check("★ 不认识任何具体向量后端（经组装器取后端）",
               "services.vector_store_sql" not in imports
               and "services.vector_store_chroma" not in imports
               and "chromadb" not in imports, str(sorted(imports)))
        _check("  └ 经组装器拿 embedder / store（换模型/换后端只改配置）",
               "services.knowledge_rag" in imports)
        _check("★ 不 import embedding_service（其消费者是已知闭集）",
               "services.embedding_service" not in imports)
        _check("  └ 异常一律按 Exception 收敛成报告，不 import 异常基类",
               not any("embedding_service" in m for m in imports))
        _check("  └ 不 import fastapi / pydantic（不是 HTTP 层）",
               "fastapi" not in imports and "pydantic" not in imports)
        _check("  └ 顶层不 import httpx（本模块不自己发请求）",
               "httpx" not in top_level and "httpx" not in imports)

        for rel in ("services/vector_store.py", "services/vector_knowledge_retriever.py",
                    "services/knowledge_retriever.py", "services/knowledge_rag.py",
                    "services/knowledge_import_pipeline.py",
                    "services/knowledge_maintenance.py"):
            other = (BACKEND_DIR / rel).read_text(encoding="utf-8")
            _check(f"  └ {rel} 不反向依赖迁移模块（无循环）",
                   "knowledge_embedding_migration" not in other)

        # 接口签名未被改动（迁移模块只做消费方）
        _check("★ VectorStore 接口签名未变（add / search / count）",
               list(inspect.signature(VectorStore.add).parameters) == ["self", "records"]
               and list(inspect.signature(VectorStore.search).parameters)
               == ["self", "query_vector", "top_k", "model", "document_id",
                   "category", "min_score"]
               and list(inspect.signature(VectorStore.count).parameters) == ["self"],
               str(list(inspect.signature(VectorStore.search).parameters)))
        _check("★ Retriever 接口签名未变",
               list(inspect.signature(KnowledgeRetriever.retrieve).parameters)
               == ["self", "job_info", "topic", "context"]
               and list(inspect.signature(VectorKnowledgeRetriever.retrieve).parameters)
               == ["self", "job_info", "topic", "context"])

        # 便捷入口与类入口同源
        _check("便捷入口 migrate_embeddings 与类入口同源（同一份实现）",
               inspect.iscoroutinefunction(migrate_embeddings)
               and "EmbeddingMigration(" in inspect.getsource(migrate_embeddings))

    await engine.dispose()

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
