# -*- coding: utf-8 -*-
"""AI 面试知识库 · DocumentChunker（文档切片）自检

无需 pytest，直接运行：
    python backend/tests/test_document_chunker.py

不依赖本机 MySQL：DATABASE_URL 指向 SQLite 内存库 + StaticPool
（只有 [7] 节为了拿到真实自增 id 才落库一篇文档，其余全是纯内存断言）。
本阶段**只切片**，故本测试不涉及 Embedding / 向量库 / Retriever / 落库。

覆盖范围
--------
1. **模块可导入 + 契约常量**：``CHUNK_FIELDS`` / metadata 必含与追加字段 / 默认参数
2. **短文本保持完整（要求 2）**：``<= chunk_size`` 整篇即 1 片；空正文返回 ``[]``
3. **长文本切片（要求 1）**：片数 > 1、每片 ``<= chunk_size``、无空洞、完整覆盖、
   位置可被 ``text.find`` 精确还原（逐片比对原文子串）
4. **保留上下文**：相邻片重叠恰为 ``chunk_overlap``；切点落在**自然边界**
   （不切断句子）；无边界长句才硬切
5. **metadata 正确（要求 3）**：``document_id`` / ``category`` / ``source`` 必含且取值正确；
   ``chunk_index`` 0 起连续；未提供时 ``document_id`` 为 ``None``
6. **配置校验与终止性**：非法 ``chunk_size`` / ``chunk_overlap`` 抛
   ``ChunkConfigError``（同时是 ``ValueError``）；``overlap`` 极大时仍终止且不越界
7. **``split_document``**：真实 ORM 行（真实 id）/ ``dict`` / ``SimpleNamespace`` 均可
8. **零依赖 + 未接线（AST）**：只 import 标准库；不 import models / DB / FastAPI /
   Retriever / 向量库；**无任何生产模块 import 本模块**
9. **确定性**：同输入两次结果完全相同
"""

import ast
import asyncio
import os
import pathlib
import re
import sys
from types import SimpleNamespace

# 必须在 import database 之前设置：SQLite 内存库，避免依赖本机 MySQL。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import KnowledgeDocument  # noqa: E402
from services import document_chunker as dc  # noqa: E402
from services.document_chunker import (  # noqa: E402
    CLAUSE_BREAK_CHARS,
    ChunkConfigError,
    DocumentChunker,
    DocumentChunkerError,
    PUNCTUATION_CHARS,
    SENTENCE_END_CHARS,
)

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


def _production_files():
    """后端生产代码（排除 tests / __pycache__），用于「未接线」检查。"""
    files = []
    for pattern in ("*.py", "api/*.py", "services/*.py", "models/*.py",
                    "schemas/*.py", "utils/*.py"):
        for path in BACKEND_DIR.glob(pattern):
            if "__pycache__" in path.parts:
                continue
            files.append(path)
    return sorted(set(files))


# ============================================================
# 素材
# ============================================================
def _make_para_text(paragraphs: int = 10, sentences: int = 3) -> str:
    """段落 + 句子结构的中文正文（每句都唯一，便于用 find 精确定位）。"""
    blocks = []
    for i in range(1, paragraphs + 1):
        body = "".join(
            f"第{i}段的第{j}句话，编号{i}-{j}，用于验证切片策略。"
            for j in range(1, sentences + 1)
        )
        blocks.append(f"【第{i}段】{body}")
    return "\n\n".join(blocks)


def _make_dense_text(n: int = 1200) -> str:
    """无空白、无标点的正文（每个字符都唯一）——用来验证「无边界时的精确平铺」。"""
    return "".join(chr(0x4E00 + i) for i in range(n))


def _make_blank_text(n: int = 700) -> str:
    """全篇同一个字、无任何边界——用来验证「硬切兜底」。"""
    return "啊" * n


PARA_TEXT = _make_para_text()
DENSE_TEXT = _make_dense_text(1200)
BLANK_TEXT = _make_blank_text(700)


# ============================================================
# 断言工具
# ============================================================
def _positions(text: str, chunks: list):
    """把每片定位回原文，返回 [(start, end), ...]；定位失败返回 ``None``。"""
    out = []
    cursor = 0
    for chunk in chunks:
        piece = chunk["content"]
        idx = text.find(piece, cursor)
        if idx < 0:
            return None
        out.append((idx, idx + len(piece)))
        cursor = idx + 1
    return out


def _gaps_are_blank(text: str, positions: list):
    """相邻片之间不得有**非空白**空洞；返回 (是否通过, 说明)。"""
    for i in range(1, len(positions)):
        prev_end = positions[i - 1][1]
        cur_start = positions[i][0]
        if cur_start > prev_end and text[prev_end:cur_start].strip():
            return False, f"第{i}片前存在空洞：{prev_end}..{cur_start}"
    return True, ""


def _overlap_len(a: str, b: str) -> int:
    """a 的后缀与 b 的前缀的最长公共长度（用于验证「重叠保留上下文」）。"""
    limit = min(len(a), len(b))
    for k in range(limit, 0, -1):
        if a[-k:] == b[:k]:
            return k
    return 0


def _cut_is_natural(text: str, piece: str, cut: int) -> bool:
    """切点是否落在自然边界（标点 / 换行 / 空白）上，而不是句子中间。"""
    if piece and piece[-1] in PUNCTUATION_CHARS:
        return True
    if cut < len(text) and text[cut] in "\n\t ":
        return True
    return piece.endswith("\n")


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("AI 面试知识库 · 文档切片（DocumentChunker）自检")
    print("=" * 70)

    # ------------------------------------------------------------
    # [1] 模块可导入 + 契约常量
    # ------------------------------------------------------------
    print("\n[1] 模块可导入 + 出参契约常量")
    _check("services.document_chunker 可导入", hasattr(dc, "DocumentChunker"))
    _check("DocumentChunker 暴露 split / split_document",
           callable(getattr(DocumentChunker, "split", None))
           and callable(getattr(DocumentChunker, "split_document", None)))
    _check("★ CHUNK_FIELDS = (content, metadata)",
           dc.CHUNK_FIELDS == ("content", "metadata"), str(dc.CHUNK_FIELDS))
    _check("★ metadata 必含字段 = 需求点名的三键",
           dc.CHUNK_METADATA_REQUIRED_FIELDS == ("document_id", "category", "source"),
           str(dc.CHUNK_METADATA_REQUIRED_FIELDS))
    _check("  └ chunk_index 登记为「追加」字段（不是需求点名）",
           dc.CHUNK_METADATA_EXTRA_FIELDS == ("chunk_index",),
           str(dc.CHUNK_METADATA_EXTRA_FIELDS))
    _check("默认参数合法（0 < overlap < size）",
           0 < dc.DEFAULT_CHUNK_OVERLAP < dc.DEFAULT_CHUNK_SIZE,
           f"{dc.DEFAULT_CHUNK_OVERLAP}/{dc.DEFAULT_CHUNK_SIZE}")
    _check("标点常量可用于判断切点",
           "。" in SENTENCE_END_CHARS and "，" in CLAUSE_BREAK_CHARS
           and PUNCTUATION_CHARS == SENTENCE_END_CHARS + CLAUSE_BREAK_CHARS)

    # ------------------------------------------------------------
    # [2] 短文本保持完整（要求 2）
    # ------------------------------------------------------------
    print("\n[2] 短文本保持完整（要求 2）")
    ck = DocumentChunker(chunk_size=200, chunk_overlap=40)

    short = "很短的一段文字。"
    got = ck.split(short)
    _check("★ 短文本 → 恰好 1 片", len(got) == 1, str(len(got)))
    _check("  └ 内容与原文逐字一致（未被切分）", got[0]["content"] == short, repr(got[0]))
    _check("  └ chunk_index = 0", got[0]["metadata"]["chunk_index"] == 0)

    exactly = "甲" * 200
    _check("长度恰好 == chunk_size → 仍是 1 片（边界含等号）",
           len(ck.split(exactly)) == 1, str(len(ck.split(exactly))))
    _check("长度 == chunk_size + 1 → 切成 2 片",
           len(ck.split("甲" * 201)) == 2, str(len(ck.split("甲" * 201))))

    for name, blank in {
        "空串": "",
        "纯空白": "   \n\t  ",
        "None": None,
    }.items():
        _check(f"空正文返回 []（{name}）——不产出占位片",
               ck.split(blank) == [], str(ck.split(blank)))

    crlf = ck.split("第一行\r\n第二行\r\n")
    _check("CRLF 归一为 LF 且去掉整篇首尾空白",
           len(crlf) == 1 and crlf[0]["content"] == "第一行\n第二行",
           repr(crlf[0]["content"]) if crlf else "[]")
    _check("非字符串入参也能处理（转成字符串）",
           len(ck.split(12345)) == 1 and ck.split(12345)[0]["content"] == "12345")

    # ------------------------------------------------------------
    # [3] 长文本切片（要求 1）
    # ------------------------------------------------------------
    print("\n[3] 长文本切片（要求 1）")
    para_chunks = ck.split(PARA_TEXT)
    _check("长文本被切成多片", len(para_chunks) > 1, str(len(para_chunks)))
    _check("★ 每片长度 <= chunk_size",
           all(len(c["content"]) <= ck.chunk_size for c in para_chunks),
           str([len(c["content"]) for c in para_chunks]))
    _check("  └ 片数在合理区间（未碎成一地、也未退化为整篇）",
           3 <= len(para_chunks) <= 8, str(len(para_chunks)))

    pos = _positions(PARA_TEXT, para_chunks)
    _check("每片都能在原文中精确定位（切片是原文连续子串）", pos is not None)
    if pos:
        _check("  └ 首片从原文开头起", pos[0][0] == 0, str(pos[0]))
        _check("  └ 末片覆盖到原文结尾", pos[-1][1] == len(PARA_TEXT), str(pos[-1]))
        ok, why = _gaps_are_blank(PARA_TEXT, pos)
        _check("★ 无空洞：相邻片相接或重叠（空隙只能是空白）", ok, why)
        _check("  └ 位置单调不回退",
               all(pos[i][0] < pos[i + 1][0] for i in range(len(pos) - 1)),
               str(pos[:4]))

    # 无边界文本：精确平铺（可逐片与原文子串比对）
    dense = DocumentChunker(chunk_size=200, chunk_overlap=40).split(DENSE_TEXT)
    _check("无边界长文本切出 8 片", len(dense) == 8, str(len(dense)))
    _check("★ 每片 == 原文对应区间的精确子串",
           all(dense[i]["content"] == DENSE_TEXT[160 * i: 160 * i + 200]
               for i in range(7))
           and dense[7]["content"] == DENSE_TEXT[1120:1200],
           str([len(c["content"]) for c in dense]))
    _check("  └ 末片不足一窗（1200 - 7*160 = 80）",
           len(dense[-1]["content"]) == 80, str(len(dense[-1]["content"])))

    tiled = DocumentChunker(chunk_size=200, chunk_overlap=0).split(DENSE_TEXT)
    _check("chunk_overlap=0 → 不重叠的整齐平铺（6 片）", len(tiled) == 6, str(len(tiled)))
    _check("  └ 每片恰好 200 字",
           all(len(c["content"]) == 200 for c in tiled),
           str([len(c["content"]) for c in tiled]))

    # ------------------------------------------------------------
    # [4] 保留上下文：重叠 + 自然边界
    # ------------------------------------------------------------
    print("\n[4] 保留上下文（重叠 + 自然边界）")
    overlaps = [_overlap_len(dense[i]["content"], dense[i + 1]["content"])
                for i in range(len(dense) - 1)]
    _check("★ 相邻片重叠长度恰为 chunk_overlap",
           overlaps == [40] * 7, str(overlaps))
    _check("  └ 重叠 > 0：跨边界的那句话至少完整出现在一片里",
           all(o > 0 for o in overlaps))

    dense_pos = _positions(DENSE_TEXT, dense)
    _check("  └ 按位置算的重叠也恰为 40",
           dense_pos is not None
           and [dense_pos[i - 1][1] - dense_pos[i][0] for i in range(1, len(dense_pos))]
           == [40] * 7,
           str(dense_pos))

    if pos:
        cuts = [(pos[i][1], para_chunks[i]["content"]) for i in range(len(pos) - 1)]
        bad = [(cut, piece[-6:]) for cut, piece in cuts
               if not _cut_is_natural(PARA_TEXT, piece, cut)]
        _check("★ 切点全落在自然边界（未切断句子）", bad == [], str(bad))
        _check("  └ 至少一片以句末标点收尾（用到句子边界）",
               any(c["content"][-1] in SENTENCE_END_CHARS for c in para_chunks),
               str([c["content"][-1] for c in para_chunks]))
        _check("  └ 末片以原文的句号收尾",
               para_chunks[-1]["content"].endswith("。"),
               repr(para_chunks[-1]["content"][-10:]))

        # 「保留上下文」的最强形态：逐句取证——每句话都完整出现在某一片里
        sentences = [s.strip() for s in re.findall(r"[^。]*。", PARA_TEXT)]
        sentences = [s for s in sentences if s]
        missing = [s for s in sentences
                   if not any(s in c["content"] for c in para_chunks)]
        _check("★★ 每句话都完整出现在某一片切片中（逐句取证）",
               missing == [] and len(sentences) >= 30,
               f"共 {len(sentences)} 句，缺失 {len(missing)} 句：{missing[:1]}")

    blank = DocumentChunker(chunk_size=200, chunk_overlap=50).split(BLANK_TEXT)
    _check("★ 无任何边界的超长句 → 硬切兜底，仍不越界",
           len(blank) == 5
           and [len(c["content"]) for c in blank] == [200, 200, 200, 200, 100],
           str([len(c["content"]) for c in blank]))

    # 注意：BLANK_TEXT 全篇同一个字，text.find 无法定位（到处都能匹配），
    # 因此「无空洞 / 完整覆盖」必须换一段**字符唯一**的无边界文本来验。
    hard_text = _make_dense_text(700)
    hard = DocumentChunker(chunk_size=200, chunk_overlap=50).split(hard_text)
    hard_pos = _positions(hard_text, hard)
    _check("  └ 硬切后依然完整覆盖原文、无空洞",
           hard_pos is not None and hard_pos[-1][1] == len(hard_text)
           and _gaps_are_blank(hard_text, hard_pos)[0]
           and [len(c["content"]) for c in hard] == [200, 200, 200, 200, 100],
           str(hard_pos))

    # ------------------------------------------------------------
    # [5] metadata 正确（要求 3）
    # ------------------------------------------------------------
    print("\n[5] metadata 正确（要求 3）")
    m = para_chunks[0]["metadata"]
    _check("★ 每个切片恰好两键 (content, metadata)",
           all(tuple(c.keys()) == dc.CHUNK_FIELDS for c in para_chunks))
    _check("★ metadata 含需求点名的三键",
           set(dc.CHUNK_METADATA_REQUIRED_FIELDS) <= set(m), str(sorted(m)))
    _check("  └ metadata 键集合 == 必含 + 追加（不多不少）",
           set(m) == set(dc.CHUNK_METADATA_REQUIRED_FIELDS)
           | set(dc.CHUNK_METADATA_EXTRA_FIELDS), str(sorted(m)))

    tagged = ck.split(PARA_TEXT, document_id=42,
                      category="technical", source="manual://redis")
    _check("★ document_id 写入正确",
           all(c["metadata"]["document_id"] == 42 for c in tagged))
    _check("★ category 写入正确",
           all(c["metadata"]["category"] == "technical" for c in tagged))
    _check("★ source 写入正确",
           all(c["metadata"]["source"] == "manual://redis" for c in tagged))
    _check("★ chunk_index 0 起连续递增",
           [c["metadata"]["chunk_index"] for c in tagged] == list(range(len(tagged))),
           str([c["metadata"]["chunk_index"] for c in tagged]))

    untagged = ck.split(PARA_TEXT)
    _check("未提供 document_id 时为 None（允许先切片后落库）",
           all(c["metadata"]["document_id"] is None for c in untagged))
    _check("  └ category / source 缺省为 \"\"（不是 None）",
           all(c["metadata"]["category"] == "" and c["metadata"]["source"] == ""
               for c in untagged))
    _check("标签类值会被去首尾空白",
           ck.split("正文", category="  job  ", source="  s://1  ")[0]["metadata"]
           == {"document_id": None, "category": "job", "source": "s://1",
               "chunk_index": 0})

    # ------------------------------------------------------------
    # [6] 配置校验与终止性
    # ------------------------------------------------------------
    print("\n[6] 配置校验与终止性")
    for name, kwargs in {
        "chunk_size=0": {"chunk_size": 0},
        "chunk_size=-1": {"chunk_size": -1},
        "chunk_size=True（bool 不算正整数）": {"chunk_size": True},
        "chunk_size=\"500\"（非整数）": {"chunk_size": "500"},
        "chunk_overlap=-1": {"chunk_overlap": -1},
        "chunk_overlap == chunk_size": {"chunk_size": 100, "chunk_overlap": 100},
        "chunk_overlap > chunk_size": {"chunk_size": 100, "chunk_overlap": 150},
    }.items():
        try:
            DocumentChunker(**kwargs)
            _check(f"拒绝非法参数：{name}", False, "未抛异常")
        except ChunkConfigError as exc:
            _check(f"拒绝非法参数：{name}", True)
            if name == "chunk_size=0":
                _check("  └ 异常同时是 DocumentChunkerError 与 ValueError",
                       isinstance(exc, DocumentChunkerError)
                       and isinstance(exc, ValueError))
        except Exception as exc:  # noqa: BLE001
            _check(f"拒绝非法参数：{name}", False, f"{type(exc).__name__}: {exc}")

    _check("chunk_overlap=0 合法（允许不重叠）",
           DocumentChunker(chunk_size=100, chunk_overlap=0).chunk_overlap == 0)
    try:
        DocumentChunker(100, 10)
        _check("参数是 keyword-only（位置传参被拒绝，防止两个数字写反）", False,
               "未抛异常")
    except TypeError:
        _check("参数是 keyword-only（位置传参被拒绝，防止两个数字写反）", True)

    greedy = DocumentChunker(chunk_size=100, chunk_overlap=99)
    gchunks = greedy.split(_make_dense_text(300))
    _check("overlap 极大时仍终止且每片 <= chunk_size",
           len(gchunks) == 201
           and all(len(c["content"]) <= 100 for c in gchunks),
           f"chunks={len(gchunks)} maxlen={max(len(c['content']) for c in gchunks)}")
    gpos = _positions(_make_dense_text(300), gchunks)
    _check("  └ 覆盖完整、无空洞",
           gpos is not None and gpos[-1][1] == 300
           and _gaps_are_blank(_make_dense_text(300), gpos)[0], str(gpos and gpos[-1]))

    # ------------------------------------------------------------
    # [7] split_document（真实 ORM 行 / dict / 对象）
    # ------------------------------------------------------------
    print("\n[7] split_document：KnowledgeDocument → 切片")
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with Session() as db:
        document = KnowledgeDocument(
            title="Redis 持久化手册",
            content=PARA_TEXT,
            category="technical",
            source="manual://redis",
        )
        db.add(document)
        await db.commit()
        await db.refresh(document)

        from_doc = ck.split_document(document)
        _check("★ 真实 ORM 行可切片", len(from_doc) > 1, str(len(from_doc)))
        _check("★ metadata.document_id == 文档真实自增 id",
               all(c["metadata"]["document_id"] == document.id for c in from_doc),
               f"doc.id={document.id} meta={from_doc[0]['metadata']}")
        _check("★ metadata.category / source 取自文档",
               all(c["metadata"]["category"] == "technical"
                   and c["metadata"]["source"] == "manual://redis" for c in from_doc))
        _check("  └ 与显式传参的切片结果完全一致",
               from_doc == ck.split(PARA_TEXT, document_id=document.id,
                                    category="technical", source="manual://redis"))

    from_dict = ck.split_document({
        "id": 7, "content": "短正文", "category": "job", "source": "job://1"})
    _check("dict 形式的文档同样可用",
           len(from_dict) == 1 and from_dict[0]["metadata"] == {
               "document_id": 7, "category": "job", "source": "job://1",
               "chunk_index": 0}, str(from_dict))

    from_obj = ck.split_document(SimpleNamespace(
        id=9, content="短正文", category="company", source="c://1"))
    _check("对象形式的文档同样可用",
           from_obj[0]["metadata"]["document_id"] == 9
           and from_obj[0]["metadata"]["category"] == "company", str(from_obj[0]))
    _check("缺字段的对象不炸（content 缺失 → []）",
           ck.split_document(SimpleNamespace(id=1)) == [])

    await engine.dispose()

    # ------------------------------------------------------------
    # [8] 零依赖 + 未接线（AST）
    # ------------------------------------------------------------
    print("\n[8] 零依赖 + 未接线（AST）")
    src = (BACKEND_DIR / "services" / "document_chunker.py").read_text(encoding="utf-8")
    mods = _imported_modules(src)
    top_level = {m.split(".")[0] for m in mods}
    _check("★ 只 import 标准库（顶层依赖恰为 4 个）",
           top_level == {"__future__", "re", "collections", "typing"},
           str(sorted(top_level)))

    banned = ("models", "database", "fastapi", "sqlalchemy", "pydantic", "deps",
              "main", "services.knowledge_retriever", "services.knowledge_document_service",
              "services.interview_core", "services.interview_agent")
    hit = sorted(b for b in banned if b in mods or b in top_level)
    _check("★ 不 import models / DB / FastAPI / Retriever / Interview", hit == [], str(hit))

    forbidden_substrings = ("embedding", "vector", "chroma", "faiss", "milvus",
                            "numpy", "openai", "transformers")
    hit = sorted(m for m in mods
                 if any(s in m.lower() for s in forbidden_substrings))
    _check("★ 不 import Embedding / 向量库 / 大模型 SDK", hit == [], str(hit))

    # ⚠️ 匹配必须用**点号全名** ``services.document_chunker``：
    # ``_imported_modules`` 收集的是 ``from services.document_chunker import X`` 里的
    # ``services.document_chunker``，**从不含裸模块名** ``document_chunker``。
    # 原先写裸名 → 集合里永远匹配不到 → 断言恒为真、**从未真正生效**（任务 46 修正）。
    offenders = []
    for path in _production_files():
        if path.name == "document_chunker.py":
            continue
        if "services.document_chunker" in _imported_modules(
                path.read_text(encoding="utf-8")):
            offenders.append(path.relative_to(BACKEND_DIR).as_posix())
    _check("★ 接线点收口为入库 Pipeline 唯一一处（切片器不自己落库）",
           offenders == ["services/knowledge_import_pipeline.py"], str(offenders))
    _check("  └ knowledge_document_service 未被改动（导入仍不自动切片）",
           "services.document_chunker" not in _imported_modules(
               (BACKEND_DIR / "services" / "knowledge_document_service.py")
               .read_text(encoding="utf-8")))

    # ------------------------------------------------------------
    # [9] 确定性
    # ------------------------------------------------------------
    print("\n[9] 确定性（同输入同输出）")
    a = DocumentChunker(chunk_size=200, chunk_overlap=40).split(
        PARA_TEXT, document_id=1, category="job", source="x")
    b = DocumentChunker(chunk_size=200, chunk_overlap=40).split(
        PARA_TEXT, document_id=1, category="job", source="x")
    _check("两次调用结果完全相同（无随机 / 无时间依赖）", a == b)
    _check("  └ 不合成新字符：每片都是原文的连续子串",
           all(c["content"] in PARA_TEXT for c in a))
    _check("  └ 各片长度合计 >= 原文长度（重叠只会更多、不会丢内容）",
           sum(len(c["content"]) for c in a) >= len(PARA_TEXT),
           f"sum={sum(len(c['content']) for c in a)} text={len(PARA_TEXT)}")

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
