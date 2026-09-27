# -*- coding: utf-8 -*-
"""AI 面试知识库 · DocumentChunker（文档切片：正文 → 带上下文的切片列表）。

分层定位
--------
::

    knowledge_document_service   导入：{title, content, …} → KnowledgeDocument（落库）
        └── document_chunker     【本模块】content → [chunk, chunk, …]（纯内存）
              └── (将来) 落库 knowledge_chunk → Embedding → 向量库 → 真实 Retriever
                    → Core.retrieve_knowledge → Agent

本模块只做一件事：**把一篇文档的正文按长度切成若干片段，每片带来源 metadata**。
刻意**不实现**：

- **Embedding / 向量化**
- **向量数据库**（chromadb / faiss / milvus …）
- **Retriever**（``services/knowledge_retriever.py`` 的真实实现）
- **落库**——本模块**零第三方依赖**（不 import ``models`` / ``database`` / ``fastapi``
  / ``knowledge_retriever``），纯内存、**纯函数**（同输入同输出），返回**普通 dict**。
  「把切片写进 ``knowledge_chunk`` 表」是**下一步**的职责，见文末「怎么接下一步」。

为什么必须切片
--------------
检索命中的必须是**片段**而不是整篇文档：整篇塞进 Prompt 会超长且噪声大。
但「简单按固定长度硬切」会**在句子/段落中间断开**，于是就有了下面的三步策略。

切片策略（三步，与需求逐条对应）
--------------------------------
1. **按长度切分**——窗口宽 ``chunk_size`` 个字符（默认 500）。
2. **保留上下文**——两重含义，都实现：

   a. **相邻切片重叠** ``chunk_overlap`` 个字符（默认 80）：下一片从上一片结尾前
      80 字开始，因此**跨边界的那句话至少完整出现在其中一片里**，不会两头都残缺；
   b. **在自然边界收口**：切点回退到窗口后半段内的**最粗**自然边界
      （段落空行 → 换行 → 句末标点 ``。！？；!?;`` → 句内停顿 ``，,、`` → 空白），
      因此**不会在句子中间断开**；找不到边界才硬切。

   两条合起来给出一个可验证的强性质：**每一句话都完整地出现在某一篇切片里**
   （切片结尾落在句子/段落边界上，下一片又从结尾前 ``chunk_overlap`` 处开始，
   跨切点的内容必在相邻两片之一中保持完整）。测试对这一条逐句取证。

   另外每片都带 ``metadata``（见第 3 点），来源可溯源——这也是「上下文」的一部分。
3. **保存 metadata**——每片带 ``{document_id, category, source, chunk_index}``。

算法
----
::

    正文（CRLF 归一为 LF；整篇去首尾空白）
      │
      ├─ len <= chunk_size ? → 整篇即 1 片（**短文本保持完整**）
      │
      └─ 滑窗：
           end = min(start + chunk_size, len)
           end < len ? → end = _snap(自然边界回退，最多回退半个窗口)
           产出 text[start:end].strip()
           start = end - chunk_overlap        （若未前进则退化为 start = end）
           直到 end 覆盖到结尾

不变量（测试逐条锁死）
----------------------
- 每片长度 **≤ chunk_size**
- **必然终止**：即使 ``chunk_overlap`` 很大，也保证每轮 ``start`` 严格前进
- **无空洞**：相邻两片的覆盖区间要么相接、要么重叠（空隙只可能是被 ``strip`` 掉的空白）
- **确定性**：无随机、无时间依赖，同输入同输出
- **零依赖**：只 import 标准库（``re``）

出参契约
--------
每个切片是**恰好两键**的普通 dict::

    {
        "content":  "切片正文",
        "metadata": {"document_id": 1, "category": "technical",
                     "source": "manual://redis", "chunk_index": 0},
    }

``metadata`` 中 ``document_id`` / ``category`` / ``source`` 是**需求点名必含**的三键；
``chunk_index``（0 起，连续）是**追加**的——没有它，切片顺序与去重都无从表达。

怎么接下一步（本模块刻意不做，留接口给将来）
--------------------------------------------
::

    # 落库（写 knowledge_chunk 表）：
    chunks = DocumentChunker().split_document(document)
    db.add_all([
        KnowledgeChunk(
            document_id=c["metadata"]["document_id"],
            content=c["content"],
            chunk_metadata=c["metadata"],   # 注意：属性名是 chunk_metadata
        )                                   # （列名仍是 metadata，见 models/knowledge.py）
        for c in chunks
    ])
    await db.commit()

    # 再往后：Embedding → 向量库 → 真实 Retriever → Core.retrieve_knowledge → Agent

**不提供全局单例**：由调用方自行构造 ``DocumentChunker(...)`` 并显式传入参数，
避免「悄悄按某个默认参数切完一整套知识库」这类不可控变更
（与 ``knowledge_retriever`` 的约定一致）。
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, Dict, List, Optional

#: 单个切片的字符数上限（含重叠前缀；超长句会被硬切到不超过它）
DEFAULT_CHUNK_SIZE = 500

#: 相邻切片的重叠字符数（保留上下文用）；必须 < chunk_size
DEFAULT_CHUNK_OVERLAP = 80

#: 出参契约：每个切片恰好这两个键
CHUNK_FIELDS = ("content", "metadata")

#: metadata 中**需求点名必含**的键（顺序即字典插入顺序）
CHUNK_METADATA_REQUIRED_FIELDS = ("document_id", "category", "source")

#: metadata 中本模块**追加**的键
CHUNK_METADATA_EXTRA_FIELDS = ("chunk_index",)

#: 自然边界，**由粗到细**（下标越小 = 越大的语义单位）。
#: 只在窗口后半段内查找，取**最靠后**的一个；同位置时取下标最小的（更粗的）。
_BOUNDARY_PATTERNS = (
    re.compile(r"\n[ \t]*\n"),   # 段落分隔（空行）
    re.compile(r"\n"),           # 换行
    re.compile(r"[。！？；!?;]"),  # 句末标点（中英）
    re.compile(r"[，,、]"),       # 句内停顿
    re.compile(r"[ \t]"),        # 空白
)

#: 句末/句内标点（测试用来判断「切点是否落在自然边界」）
SENTENCE_END_CHARS = "。！？；!?;"
CLAUSE_BREAK_CHARS = "，,、"
PUNCTUATION_CHARS = SENTENCE_END_CHARS + CLAUSE_BREAK_CHARS


class DocumentChunkerError(Exception):
    """切片器的领域基类（全项目无全局异常基类，各模块自带）。"""


class ChunkConfigError(DocumentChunkerError, ValueError):
    """切片参数非法（``chunk_size`` 非正 / ``chunk_overlap`` 越界）。"""


# ============================================================
# 入参归一
# ============================================================
def _normalize(content: Any) -> str:
    """正文归一：``None`` → ``""``；CRLF/CR 统一为 LF；去掉整篇首尾空白。

    只做这三件事——**不做**任何正文改写（不压平空行、不去多余空格），
    保证切片是原文的**连续子串**，可被 ``text.find`` 精确定位。
    """
    if content is None:
        return ""
    text = content if isinstance(content, str) else str(content)
    return text.replace("\r\n", "\n").replace("\r", "\n").strip()


def _as_label(value: Any) -> str:
    """标签类 metadata 值归一：``None`` → ``""``；其余转字符串并去首尾空白。"""
    if value is None:
        return ""
    return str(value).strip()


def _read_field(payload: Any, name: str) -> Any:
    """从**映射**或**对象**里取字段（缺省 ``None``）。

    与 ``knowledge_document_service._read_field`` 同款、**刻意重复这 4 行**而不跨模块
    引用私有函数——本模块要保持「零依赖、可单独复制使用」，不为一行工具引入耦合。
    有了它，``split_document`` 既能吃 ORM 行，也能吃 ``dict`` / ``SimpleNamespace``。
    """
    if isinstance(payload, Mapping):
        return payload.get(name)
    return getattr(payload, name, None)


# ============================================================
# 切片器
# ============================================================
class DocumentChunker:
    """把文档正文切成带上下文的片段。

    :param chunk_size: 单片刻度上限（字符数），必须为正整数
    :param chunk_overlap: 相邻片重叠字符数，必须满足 ``0 <= overlap < chunk_size``

    两个参数都是 **keyword-only**，避免 ``DocumentChunker(500, 80)`` 这种
    位置传参把两个数字写反却毫无提示。
    """

    def __init__(
        self,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
    ) -> None:
        if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size <= 0:
            raise ChunkConfigError(f"chunk_size 必须是正整数，当前 {chunk_size!r}")
        if isinstance(chunk_overlap, bool) or not isinstance(chunk_overlap, int) \
                or chunk_overlap < 0:
            raise ChunkConfigError(f"chunk_overlap 必须是非负整数，当前 {chunk_overlap!r}")
        if chunk_overlap >= chunk_size:
            raise ChunkConfigError(
                f"chunk_overlap({chunk_overlap}) 必须小于 chunk_size({chunk_size})，"
                "否则切片无法前进"
            )
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    # --------------------------------------------------------
    # 主入口
    # --------------------------------------------------------
    def split(
        self,
        content: Any,
        *,
        document_id: Optional[int] = None,
        category: Any = "",
        source: Any = "",
    ) -> List[Dict[str, Any]]:
        """把正文切成切片列表。

        - 空正文 / 纯空白 → ``[]``（**不返回占位片**，否则下游会把空片喂给模型）
        - ``len(text) <= chunk_size`` → **整篇即 1 片**（短文本保持完整，不做任何切分）
        - 否则滑窗切分，规则见模块文档

        ``document_id`` / ``category`` / ``source`` 只用于填 metadata，
        **不参与切分**；``document_id`` 允许为 ``None``（调用方可能先切片后落库）。
        """
        text = _normalize(content)
        if not text:
            return []

        if len(text) <= self.chunk_size:
            return [self._make_chunk(text, 0, document_id, category, source)]

        chunks: List[Dict[str, Any]] = []
        start = 0
        n = len(text)
        while start < n:
            end = min(start + self.chunk_size, n)
            if end < n:
                # 只在未到结尾时收口；到结尾就自然结束，无需回退
                end = self._snap(text, start, end)
            piece = text[start:end].strip()
            if piece:
                # 片序号取「已产出片数」，因此即使中间跳过空白片也保持 0 起连续
                chunks.append(
                    self._make_chunk(piece, len(chunks), document_id, category, source)
                )
            if end >= n:
                break
            nxt = end - self.chunk_overlap
            # 保证严格前进：overlap 过大时退化为不重叠，绝不原地打转
            start = nxt if nxt > start else end
        return chunks

    def split_document(self, document: Any) -> List[Dict[str, Any]]:
        """``KnowledgeDocument``（ORM 行 / dict / 任意带同名属性的对象）→ 切片列表。

        这是「把一篇文档变成切片」的便捷入口：自动把 ``id`` / ``category`` / ``source``
        搬进 metadata，调用方不必逐字段搬运。``id`` 为 ``None``（未落库）时照常切片。
        """
        return self.split(
            _read_field(document, "content"),
            document_id=_read_field(document, "id"),
            category=_read_field(document, "category"),
            source=_read_field(document, "source"),
        )

    # --------------------------------------------------------
    # 内部
    # --------------------------------------------------------
    def _snap(self, text: str, start: int, end: int) -> int:
        """把切点 ``end`` 回退到窗口后半段内的**最粗**自然边界；找不到就硬切。

        两条规则：

        1. **由粗到细**：先试段落空行，再换行、句末标点、句内停顿、空白。
           一旦某档在窗口后半段内有命中，就用它的**最靠后**一个，不再往细档找。
           这样切片**优先在段落/句子处收口**——若只按「最靠后的边界」选，
           常常会落在一个逗号或空格上，把句子拦腰截断（实测确实如此）。
        2. **最多回退半个窗口**（``chunk_size // 2``）：宁可少切一点，
           也不能因为一个很靠前的段落符号就把切片切得只有几十字。

        由此得到一条重要性质：**每句话都完整地出现在某一篇切片里**——
        切片结尾总是落在句子/段落边界上，而下一片又从上一片结尾前 ``chunk_overlap``
        处开始，因此跨越切点的内容必定在相邻两片之一中保持完整。

        返回位置**必然 > ``start``**，这是「循环必然终止」的前提。
        """
        min_pos = start + max(1, self.chunk_size // 2)
        if end <= min_pos:
            return end
        window = text[:end]
        for pattern in _BOUNDARY_PATTERNS:
            best = -1
            for match in pattern.finditer(window, start):
                pos = match.end()
                if pos > min_pos:
                    best = pos
            if best > 0:
                return best
        return end

    @staticmethod
    def _make_chunk(
        text: str, index: int, document_id: Any, category: Any, source: Any
    ) -> Dict[str, Any]:
        """构造一个切片（恰好 ``CHUNK_FIELDS`` 两键）。"""
        return {
            "content": text,
            "metadata": {
                "document_id": document_id,
                "category": _as_label(category),
                "source": _as_label(source),
                "chunk_index": index,
            },
        }


__all__ = [
    "CHUNK_FIELDS",
    "CHUNK_METADATA_EXTRA_FIELDS",
    "CHUNK_METADATA_REQUIRED_FIELDS",
    "CLAUSE_BREAK_CHARS",
    "ChunkConfigError",
    "DEFAULT_CHUNK_OVERLAP",
    "DEFAULT_CHUNK_SIZE",
    "DocumentChunker",
    "DocumentChunkerError",
    "PUNCTUATION_CHARS",
    "SENTENCE_END_CHARS",
]
