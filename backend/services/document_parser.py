# -*- coding: utf-8 -*-
"""文档解析器：把上传的文件（二进制 / 文本）转成**纯文本**。

为什么单独成模块
----------------
``services/knowledge_import_pipeline`` 的模块文档里明确写着「**文件上传 / PDF 解析
（需求点名不做）**……『二进制 → 文本』是**解析器的职责**」。
本模块就是那个解析器：**只做「字节 → 文本」**，其余（切片 / 向量化 / 落库 / 向量索引）
一概不碰——那是 ``knowledge_import_pipeline`` 的事。

边界
----
- **不做**：OCR（扫描件 PDF / 图片）、``.doc`` 等旧二进制 Office 格式、
  电子表格（``.xlsx``）、网页抓取、压缩包解包。
- **不做**：切片与向量化。``parse_document`` 只返回文本。
- **不依赖** DB / FastAPI / 项目内其它模块 ⇒ 可脱离一切环境单独 import 与测试。

依赖
----
- **标准库**：``.txt/.md/.csv/.json/.log/.yml/.yaml/.ini`` 等纯文本用编码探测；
  ``.html/.htm`` 用 ``html.parser`` 去标签；``.docx`` / ``.pptx`` 都是 OOXML（ZIP +
  XML），用 ``zipfile`` + ``xml.etree.ElementTree`` 直接取文本 ⇒ **不需要 lxml / python-docx**。
- **唯一第三方依赖**：``pypdf``（解析 PDF）。它在**函数体内延迟导入**，
  因此不装 pypdf 也能 import 本模块，只是解析 PDF 时会给出明确的安装提示。

失败语义
--------
抛**领域异常**（不抛 ``HTTPException``——本模块不该知道 HTTP 的存在）：

- ``UnsupportedFormatError``  —— 扩展名不在支持列表内（API 层映射 400）
- ``FileTooLargeError``       —— 超过 ``MAX_UPLOAD_BYTES``（API 层映射 413）
- ``DocumentParseError``      —— 格式对但读不出来（损坏 / 加密 / 无文本层）（API 层映射 400）

编码探测
--------
UTF-8（含 BOM）优先；失败时在中文常见遗留编码（``gb18030`` / ``big5``）里
**按「中文字符占比」择优选**，并把实际用的编码放进 ``warnings``——
避免「盲选一个编码、解出一屏乱码却当成成功」。
"""

from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree

__all__ = [
    "MAX_UPLOAD_BYTES",
    "TEXT_EXTENSIONS",
    "HTML_EXTENSIONS",
    "DOCX_EXTENSIONS",
    "PPTX_EXTENSIONS",
    "PDF_EXTENSIONS",
    "SUPPORTED_EXTENSIONS",
    "FORMAT_OF_EXTENSION",
    "ParsedDocument",
    "DocumentParseError",
    "UnsupportedFormatError",
    "FileTooLargeError",
    "parse_document",
    "suggest_title",
    "is_supported",
]


# ============================================================
# 一、常量与异常
# ============================================================
#: 单文件上限（20 MB）。刻意**不做成环境变量**：它只影响新上传路由，
#: 而「往 .env 加行为旋钮」在项目里是要评估回归污染的动作，没必要为此引入。
MAX_UPLOAD_BYTES = 20 * 1024 * 1024

#: 标题长度上限，与 ``knowledge_document_service.create_document`` 的校验口径一致。
MAX_TITLE_CHARS = 300

TEXT_EXTENSIONS = (
    ".txt", ".md", ".markdown", ".csv", ".tsv", ".json", ".log",
    ".yml", ".yaml", ".ini", ".conf", ".rst", ".text",
)
HTML_EXTENSIONS = (".html", ".htm", ".xhtml")
DOCX_EXTENSIONS = (".docx",)
PPTX_EXTENSIONS = (".pptx",)
PDF_EXTENSIONS = (".pdf",)

SUPPORTED_EXTENSIONS: Tuple[str, ...] = (
    PDF_EXTENSIONS + DOCX_EXTENSIONS + PPTX_EXTENSIONS + HTML_EXTENSIONS + TEXT_EXTENSIONS
)

#: 扩展名 → ``source_format``（给报告用，便于前端如实展示「这是什么格式解析出来的」）
FORMAT_OF_EXTENSION: Dict[str, str] = {
    **{ext: "pdf" for ext in PDF_EXTENSIONS},
    **{ext: "docx" for ext in DOCX_EXTENSIONS},
    **{ext: "pptx" for ext in PPTX_EXTENSIONS},
    **{ext: "html" for ext in HTML_EXTENSIONS},
    **{ext: "text" for ext in TEXT_EXTENSIONS},
}

#: 明确**不支持**的常见格式 → 给用户的解释（避免只丢一句「不支持」）
UNSUPPORTED_HINTS: Dict[str, str] = {
    ".doc": "旧版二进制 Word 格式（.doc）无法直接解析，请另存为 .docx 或 PDF 后重试",
    ".xls": "电子表格（.xls）请另存为 .csv 或 .xlsx 后按纯文本/表格处理",
    ".xlsx": "电子表格（.xlsx）暂不支持，请另存为 .csv 后上传",
    ".ppt": "旧版二进制 PowerPoint（.ppt）请另存为 .pptx 后重试",
    ".jpg": "图片不做 OCR，请先用 OCR 工具转成文本再上传",
    ".jpeg": "图片不做 OCR，请先用 OCR 工具转成文本再上传",
    ".png": "图片不做 OCR，请先用 OCR 工具转成文本再上传",
    ".zip": "压缩包不会被解包，请解压后逐个上传文件",
    ".rar": "压缩包不会被解包，请解压后逐个上传文件",
    ".7z": "压缩包不会被解包，请解压后逐个上传文件",
}


class DocumentParseError(Exception):
    """格式在支持列表内，但内容读不出来（损坏 / 加密 / 无文本层）。"""


class UnsupportedFormatError(DocumentParseError):
    """扩展名不在支持列表内。"""


class FileTooLargeError(DocumentParseError):
    """超过 :data:`MAX_UPLOAD_BYTES`。"""


@dataclass
class ParsedDocument:
    """解析结果（**只有文本与元信息**，不含任何切片）。"""

    text: str
    source_format: str
    extension: str
    suggested_title: str = ""
    encoding: str = ""
    warnings: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

    @property
    def chars(self) -> int:
        return len(self.text)


# ============================================================
# 二、通用工具
# ============================================================
_TRAILING_SPACE = re.compile(r"[ \t]+\n")
_MANY_BLANKS = re.compile(r"\n{3,}")
_SLIDE_NAME = re.compile(r"^ppt/slides/slide(\d+)\.xml$")


def _extension_of(filename: str) -> str:
    """取小写扩展名（含点）。无扩展名返回空串。"""
    name = (filename or "").strip().replace("\\", "/").rsplit("/", 1)[-1]
    dot = name.rfind(".")
    return name[dot:].lower() if dot > 0 else ""


def is_supported(filename: str) -> bool:
    return _extension_of(filename) in SUPPORTED_EXTENSIONS


def suggest_title(filename: str) -> str:
    """用文件名（去扩展名）当默认标题，并按标题长度上限截断。"""
    name = (filename or "").strip().replace("\\", "/").rsplit("/", 1)[-1]
    stem = name[: name.rfind(".")] if name.rfind(".") > 0 else name
    stem = stem.strip() or "未命名文档"
    return stem[:MAX_TITLE_CHARS]


def _normalize(text: str) -> str:
    """统一换行、去 BOM / 不换行空格、压掉连续空行、去首尾空白。"""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\ufeff", "").replace("\u00a0", " ")
    text = _TRAILING_SPACE.sub("\n", text)
    text = _MANY_BLANKS.sub("\n\n", text)
    return text.strip()


def _cjk_ratio(text: str) -> float:
    """CJK 汉字占比——用于在遗留编码之间择优。"""
    if not text:
        return 0.0
    hits = sum(1 for ch in text if "\u4e00" <= ch <= "\u9fff")
    return hits / len(text)


def _decode_text(data: bytes) -> Tuple[str, str, List[str]]:
    """探测编码并解码，返回 ``(text, encoding, warnings)``。

    顺序：BOM 优先 → 严格 UTF-8 → 中文遗留编码按汉字占比择优 → 兜底 replace。
    """
    warnings: List[str] = []

    if data.startswith(b"\xef\xbb\xbf"):
        return data.decode("utf-8-sig"), "utf-8-sig", warnings
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        try:
            return data.decode("utf-16"), "utf-16", warnings
        except UnicodeDecodeError:
            pass

    try:
        return data.decode("utf-8"), "utf-8", warnings
    except UnicodeDecodeError:
        pass

    best: Optional[Tuple[str, float, str]] = None
    for encoding in ("gb18030", "big5"):
        try:
            candidate = data.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            continue
        score = _cjk_ratio(candidate)
        if best is None or score > best[1]:
            best = (candidate, score, encoding)

    if best is not None:
        # 明确记录实际编码：非 UTF-8 是**需要人知道**的事实，不能静默
        warnings.append(f"decoded_as_{best[2]}")
        return best[0], best[2], warnings

    warnings.append("decoded_with_replacement_chars")
    return data.decode("utf-8", errors="replace"), "utf-8/replace", warnings


class _HtmlTextExtractor(HTMLParser):
    """把 HTML 抽成纯文本：丢 script/style，块级标签转换行，单元格转制表符。"""

    _SKIP = {"script", "style", "noscript", "head", "title", "meta", "link", "svg"}
    _BLOCK = {
        "p", "div", "br", "li", "tr", "ul", "ol", "table", "thead", "tbody",
        "h1", "h2", "h3", "h4", "h5", "h6", "section", "article", "header",
        "footer", "blockquote", "pre", "hr", "dl", "dt", "dd", "figure",
    }
    _CELL = {"td", "th"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: List[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        if tag in self._SKIP:
            self._skip_depth += 1
        elif tag in self._CELL:
            self._parts.append("\t")
        elif tag in self._BLOCK:
            self._parts.append("\n")

    def handle_startendtag(self, tag: str, attrs: Any) -> None:
        # ``<br/>`` 等自闭合标签：只按「开始」处理一次，避免产生两个换行
        self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        if tag in self._SKIP:
            if self._skip_depth:
                self._skip_depth -= 1
        elif tag in self._BLOCK:
            self._parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip_depth:
            self._parts.append(data)

    @property
    def text(self) -> str:
        return "".join(self._parts)


# ============================================================
# 三、各格式解析器
# ============================================================
_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
_A = "{http://schemas.openxmlformats.org/drawingml/2006/main}"


def _open_ooxml(data: bytes, kind: str) -> zipfile.ZipFile:
    """打开 OOXML（docx / pptx）包。两者本质都是 ZIP + XML。"""
    try:
        return zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as exc:
        raise DocumentParseError(
            f"{kind} 不是有效的 OOXML 包（可能是改了扩展名的旧格式或已损坏）：{exc}"
        ) from exc


def _xml_root(payload: bytes, what: str) -> ElementTree.Element:
    try:
        return ElementTree.fromstring(payload)
    except ElementTree.ParseError as exc:
        raise DocumentParseError(f"{what} 的 XML 解析失败：{exc}") from exc


def _docx_text(data: bytes) -> Tuple[str, List[str]]:
    """DOCX 正文：按文档顺序取 ``w:t`` 文本，``w:p`` 作段落分隔。

    只取 ``w:t``（真正的文本节点），**不取** ``w:instrText``（域代码）与
    ``w:delText``（修订删除内容）——那些不是正文。
    """
    warnings: List[str] = []
    with _open_ooxml(data, "docx") as zf:
        if "word/document.xml" not in zf.namelist():
            raise DocumentParseError("docx 内缺少 word/document.xml，文件可能已损坏")
        payload = zf.read("word/document.xml")

    parts: List[str] = []
    for element in _xml_root(payload, "docx 主文档").iter():
        tag = element.tag
        if tag == _W + "t":
            parts.append(element.text or "")
        elif tag == _W + "tab":
            parts.append("\t")
        elif tag in (_W + "br", _W + "cr"):
            parts.append("\n")
        elif tag == _W + "p":
            parts.append("\n")
    return "".join(parts), warnings


def _pptx_text(data: bytes) -> Tuple[str, List[str]]:
    """PPTX 文本：逐张幻灯片取 ``a:t``，每页加「第 N 页」分隔，便于检索时定位。"""
    warnings: List[str] = []
    with _open_ooxml(data, "pptx") as zf:
        slides: List[Tuple[int, str]] = []
        for name in zf.namelist():
            matched = _SLIDE_NAME.match(name)
            if matched:
                slides.append((int(matched.group(1)), name))
        if not slides:
            raise DocumentParseError("pptx 内没有找到任何幻灯片（ppt/slides/slideN.xml）")
        slides.sort()

        blocks: List[str] = []
        for number, name in slides:
            root = _xml_root(zf.read(name), f"第 {number} 张幻灯片")
            parts: List[str] = []
            for element in root.iter():
                if element.tag == _A + "t":
                    parts.append(element.text or "")
                elif element.tag == _A + "p":
                    parts.append("\n")
            page = _normalize("".join(parts))
            if page:
                blocks.append(f"【第 {number} 页】\n{page}")
            else:
                # 纯图片页（无文本层）如实记一条，不假装解析到了内容
                warnings.append(f"slide_{number}_no_text")
    return "\n\n".join(blocks), warnings


def _pdf_text(data: bytes) -> Tuple[str, List[str]]:
    """PDF 文本层：逐页 ``extract_text()``。**不做 OCR**。"""
    warnings: List[str] = []
    try:
        from pypdf import PdfReader
    except ImportError as exc:  # pragma: no cover - 取决于环境是否装了 pypdf
        raise DocumentParseError(
            "未安装 pypdf，无法解析 PDF。请先执行 "
            "`pip install --no-cache-dir pypdf`（已声明在 backend/requirements.txt）"
        ) from exc

    try:
        reader = PdfReader(io.BytesIO(data))
        if reader.is_encrypted:
            # 只设了「权限口令」、用空口令即可打开的 PDF 很常见，先试空口令
            reader.decrypt("")
            if reader.is_encrypted:
                raise DocumentParseError(
                    "PDF 已加密且空口令无法打开。本接口不提供口令破解，"
                    "请先解除保护后再上传"
                )
            warnings.append("pdf_encrypted_with_empty_password")

        pages: List[str] = []
        for index, page in enumerate(reader.pages, 1):
            try:
                text = page.extract_text() or ""
            except Exception:  # noqa: BLE001 - 单页失败不该打挂整篇
                warnings.append(f"page_{index}_extract_failed")
                continue
            if not text.strip():
                # 该页没有文本层（扫描图）——如实记账，最终若整篇为空会明确报错
                warnings.append(f"page_{index}_no_text_layer")
            pages.append(text)
    except DocumentParseError:
        raise
    except Exception as exc:  # noqa: BLE001 - pypdf 的异常类型很杂，统一收敛
        raise DocumentParseError(f"PDF 解析失败：{exc}") from exc

    return "\n\n".join(pages), warnings


# ============================================================
# 四、统一入口
# ============================================================
def parse_document(filename: str, data: bytes) -> ParsedDocument:
    """把 ``filename`` + ``data`` 解析成 :class:`ParsedDocument`。

    抛 :class:`UnsupportedFormatError` / :class:`FileTooLargeError` /
    :class:`DocumentParseError`；**成功时保证 ``text`` 非空**（空文本一律报错，
    绝不放行一个「解析成功但没内容」的结果去污染知识库）。
    """
    extension = _extension_of(filename)

    if extension not in SUPPORTED_EXTENSIONS:
        hint = UNSUPPORTED_HINTS.get(extension)
        supported = "、".join(SUPPORTED_EXTENSIONS)
        if hint:
            raise UnsupportedFormatError(
                f"不支持的文件格式「{extension or '（无扩展名）'}」：{hint}。"
                f"当前支持：{supported}"
            )
        raise UnsupportedFormatError(
            f"不支持的文件格式「{extension or '（无扩展名）'}」。当前支持：{supported}"
        )

    if not data:
        raise DocumentParseError(f"文件《{filename}》内容为空（0 字节），没有可导入的正文")

    if len(data) > MAX_UPLOAD_BYTES:
        raise FileTooLargeError(
            f"文件 {len(data) / 1024 / 1024:.1f} MB 超过单文件上限 "
            f"{MAX_UPLOAD_BYTES // 1024 // 1024} MB，请拆分后再上传"
        )

    source_format = FORMAT_OF_EXTENSION[extension]
    encoding = ""
    warnings: List[str] = []
    meta: Dict[str, Any] = {}

    if source_format == "text":
        raw, encoding, warnings = _decode_text(data)
        text = raw
    elif source_format == "html":
        raw, encoding, warnings = _decode_text(data)
        extractor = _HtmlTextExtractor()
        extractor.feed(raw)
        extractor.close()
        text = extractor.text
    elif source_format == "docx":
        text, warnings = _docx_text(data)
    elif source_format == "pptx":
        text, warnings = _pptx_text(data)
    else:  # pdf
        text, warnings = _pdf_text(data)

    normalized = _normalize(text)
    if not normalized:
        # 空结果必须报错：否则会写进一篇「有标题、没正文」的知识，检索时永远命中不到
        detail = "（该 PDF 可能是扫描件，没有文本层；本接口不做 OCR）" if source_format == "pdf" else ""
        raise DocumentParseError(
            f"解析后正文为空，无法导入{detail}。原始文件：{filename}"
        )

    if source_format == "pdf":
        # 用 pypdf 再读一次页数代价很低，但为避免重复解析这里只做提示性说明
        meta["pages_without_text"] = sum(
            1 for item in warnings if item.endswith("_no_text_layer")
        )

    return ParsedDocument(
        text=normalized,
        source_format=source_format,
        extension=extension,
        suggested_title=suggest_title(filename),
        encoding=encoding,
        warnings=warnings,
        meta=meta,
    )
