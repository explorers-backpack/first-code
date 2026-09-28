# -*- coding: utf-8 -*-
"""文档解析层自检（``services/document_parser``）

无需 pytest，直接运行：
    python backend/tests/test_document_parser.py

覆盖：
[1] 扩展名识别与支持清单（大小写、路径、无扩展名）
[2] 纯文本编码探测：UTF-8 / BOM / GB18030 / Big5（含**两种编码互相误判**的对抗样本）
[3] HTML 去标签：丢 script/style/title，块级转换行，单元格转制表符
[4] DOCX：段落分隔、同段多 run 拼接、制表符
[5] PPTX：逐页分隔、**按数字排序**（slide10 排在 slide2 之后）、纯图片页告警
[6] PDF：文本层提取（pypdf 为**声明依赖**，缺失即失败而非静默跳过）
[7] 归一化：CRLF / 连续空行 / BOM / 不换行空格
[8] 错误路径：不支持格式（附可操作提示）、0 字节、损坏包、无文本层 PDF、超限
[9] 标题回落：去扩展名、无扩展名、超长截断、空名
[10] 分层守卫（AST）：本模块**不依赖** DB / FastAPI / ORM / 其它 service
[11] 纯函数：同输入两次结果一致（无隐藏全局状态）
"""

import ast
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from services import document_parser as dp  # noqa: E402
from document_fixtures import (  # noqa: E402
    DOCX_PARAGRAPHS,
    PDF_LINES,
    PPTX_SLIDES,
    make_docx,
    make_html,
    make_pdf,
    make_pptx,
    make_text,
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


def _raises(kind, func, *args, **kwargs):
    """返回 ``(是否命中期望异常, 异常实例或 None)``。"""
    try:
        func(*args, **kwargs)
    except kind as exc:
        return True, exc
    except Exception as exc:  # noqa: BLE001
        return False, exc
    return False, None


def _run() -> bool:
    print("=" * 68)
    print("文档解析层自检（services/document_parser）")
    print("=" * 68)

    # ---------------- [1] 扩展名识别 ----------------
    print("\n[1] 扩展名识别与支持清单")
    _check(".pdf 支持", dp.is_supported("a.pdf"))
    _check(".PDF 大写也支持", dp.is_supported("a.PDF"))
    _check("带路径也识别", dp.is_supported("C:\\tmp\\dir\\手册.docx"))
    _check("正斜杠路径也识别", dp.is_supported("/tmp/dir/手册.docx"))
    _check(".doc 不支持", not dp.is_supported("old.doc"))
    _check("无扩展名不支持", not dp.is_supported("README"))
    _check("点开头文件名不被当成扩展名", not dp.is_supported(".gitignore"))
    for ext in (".pdf", ".docx", ".pptx", ".html", ".txt", ".md", ".csv"):
        _check(f"支持清单含 {ext}", ext in dp.SUPPORTED_EXTENSIONS)
    _check(
        "支持清单内每个扩展名都有 source_format 映射",
        all(ext in dp.FORMAT_OF_EXTENSION for ext in dp.SUPPORTED_EXTENSIONS),
        str([e for e in dp.SUPPORTED_EXTENSIONS if e not in dp.FORMAT_OF_EXTENSION]),
    )

    # ---------------- [2] 纯文本编码 ----------------
    print("\n[2] 纯文本编码探测")
    parsed = dp.parse_document("utf8.txt", make_text("utf-8"))
    _check("UTF-8 解码正确", "中文内容测试" in parsed.text, parsed.text[:40])
    _check("UTF-8 记 encoding=utf-8", parsed.encoding == "utf-8", parsed.encoding)
    _check("UTF-8 无告警", parsed.warnings == [], str(parsed.warnings))
    _check("source_format=text", parsed.source_format == "text", parsed.source_format)

    bom = b"\xef\xbb\xbf" + make_text("utf-8")
    parsed = dp.parse_document("bom.txt", bom)
    _check("UTF-8 BOM 正确剥离", parsed.text.startswith("中文内容测试"), parsed.text[:20])
    _check("BOM 记为 utf-8-sig", parsed.encoding == "utf-8-sig", parsed.encoding)
    _check("BOM 不出现在正文里", "\ufeff" not in parsed.text)

    parsed = dp.parse_document("gbk.txt", make_text("gb18030"))
    _check("GB18030 解码正确", "中文内容测试" in parsed.text, parsed.text[:40])
    _check("GB18030 记为 gb18030", parsed.encoding == "gb18030", parsed.encoding)
    _check(
        "非 UTF-8 必须留告警（不静默）",
        "decoded_as_gb18030" in parsed.warnings,
        str(parsed.warnings),
    )

    # ★ 对抗样本：两种编码都能「解出东西」时，必须按汉字占比选对
    big5_source = "中文內容測試，Redis 快照持久化，這是一份技術手冊。"
    parsed = dp.parse_document("big5.txt", big5_source.encode("big5"))
    _check(
        "Big5 对抗样本解码正确（不被 gb18030 抢走）",
        parsed.text == big5_source,
        f"{parsed.encoding} / {parsed.text[:30]}",
    )
    _check("Big5 记为 big5", parsed.encoding == "big5", parsed.encoding)

    # 反过来：GB18030 字节用 big5 解会直接抛 UnicodeDecodeError
    parsed = dp.parse_document("gb2.txt", "中文内容测试，快照持久化。".encode("gb18030"))
    _check(
        "GB18030 对抗样本解码正确",
        parsed.text == "中文内容测试，快照持久化。",
        f"{parsed.encoding} / {parsed.text[:30]}",
    )

    # ---------------- [3] HTML ----------------
    print("\n[3] HTML 去标签")
    parsed = dp.parse_document("page.html", make_html())
    _check("source_format=html", parsed.source_format == "html", parsed.source_format)
    _check("保留标题文字", "Redis 持久化" in parsed.text, parsed.text)
    _check("保留段落文字", "RDB 是快照" in parsed.text, parsed.text)
    _check("丢掉 script 内容", "var x=1" not in parsed.text, parsed.text)
    _check("丢掉 style 内容", "color:red" not in parsed.text, parsed.text)
    _check("丢掉 <title> 内容", "t\n" not in parsed.text and parsed.text != "t", parsed.text)
    _check("不留尖括号", "<" not in parsed.text and ">" not in parsed.text, parsed.text)
    _check(
        "表格单元格之间有制表符",
        "\t" in parsed.text,
        repr(parsed.text),
    )

    # ---------------- [4] DOCX ----------------
    print("\n[4] DOCX（OOXML，标准库解析）")
    parsed = dp.parse_document("手册.docx", make_docx())
    _check("source_format=docx", parsed.source_format == "docx", parsed.source_format)
    _check("第一段完整", DOCX_PARAGRAPHS[0] in parsed.text, parsed.text)
    _check(
        "同段多个 run 被拼接（不丢字、不插分隔）",
        DOCX_PARAGRAPHS[1] in parsed.text,
        repr(parsed.text),
    )
    _check("段落之间是换行", "\n" in parsed.text, repr(parsed.text))
    _check("w:tab 变制表符", "\t缩进项" in parsed.text, repr(parsed.text))
    _check("解析出的字符数与 text 一致", parsed.chars == len(parsed.text))
    _check("docx 无告警", parsed.warnings == [], str(parsed.warnings))

    # ---------------- [5] PPTX ----------------
    print("\n[5] PPTX（逐页 + 数字序）")
    parsed = dp.parse_document("分享.pptx", make_pptx())
    _check("source_format=pptx", parsed.source_format == "pptx", parsed.source_format)
    for number, title, bullet in PPTX_SLIDES:
        _check(f"第 {number} 页标题在正文里", title in parsed.text, parsed.text[:120])
        _check(f"第 {number} 页要点在正文里", bullet in parsed.text, parsed.text[:120])
    _check("带页码分隔标记", "【第 1 页】" in parsed.text, parsed.text[:80])
    _check(
        "slide10 排在 slide2 之后（按数字而非字典序）",
        parsed.text.index("第十页标题") > parsed.text.index("第二页标题"),
        "字典序会得到 slide1 < slide10 < slide2",
    )

    parsed = dp.parse_document("含图片页.pptx", make_pptx(empty_slide_numbers=(2,)))
    _check(
        "纯图片页记 slide_2_no_text 告警",
        "slide_2_no_text" in parsed.warnings,
        str(parsed.warnings),
    )
    _check("纯图片页不产生空页码块", "【第 2 页】" not in parsed.text, parsed.text[:120])
    _check("有文本的页照常解析", "第一页标题" in parsed.text)

    # ---------------- [6] PDF ----------------
    print("\n[6] PDF（pypdf，声明依赖）")
    try:
        import pypdf  # noqa: F401

        has_pypdf = True
    except ImportError:
        has_pypdf = False
    _check("pypdf 可用（已在 requirements.txt 声明）", has_pypdf, "请 pip install pypdf")

    if has_pypdf:
        parsed = dp.parse_document("handbook.pdf", make_pdf())
        _check("source_format=pdf", parsed.source_format == "pdf", parsed.source_format)
        _check("PDF 文本被提取", PDF_LINES[0] in parsed.text, repr(parsed.text))
        _check("PDF 多行都提到", PDF_LINES[1] in parsed.text, repr(parsed.text))
        _check("正常 PDF 无告警", parsed.warnings == [], str(parsed.warnings))
        _check(
            "meta.pages_without_text 为 0",
            parsed.meta.get("pages_without_text") == 0,
            str(parsed.meta),
        )

        # 扫描件：无文本层 → 必须明确报错，不能导入空文档
        ok, exc = _raises(dp.DocumentParseError, dp.parse_document, "scan.pdf", make_pdf(with_text=False))
        _check("无文本层 PDF 报错", ok, repr(exc))
        _check(
            "报错信息点明「扫描件 / OCR」",
            exc is not None and ("OCR" in str(exc) or "扫描" in str(exc)),
            str(exc),
        )

    # ---------------- [7] 归一化 ----------------
    print("\n[7] 文本归一化")
    parsed = dp.parse_document("crlf.txt", "第一行\r\n第二行\r第三行".encode("utf-8"))
    _check("CRLF / CR 统一为 LF", parsed.text == "第一行\n第二行\n第三行", repr(parsed.text))

    parsed = dp.parse_document("blanks.txt", "甲\n\n\n\n\n乙".encode("utf-8"))
    _check("连续空行压成一行空行", parsed.text == "甲\n\n乙", repr(parsed.text))

    parsed = dp.parse_document("nbsp.txt", "甲\u00a0乙".encode("utf-8"))
    _check("不换行空格转普通空格", parsed.text == "甲 乙", repr(parsed.text))

    parsed = dp.parse_document("space.txt", "甲   \n乙  ".encode("utf-8"))
    _check("行尾空白被去掉", parsed.text == "甲\n乙", repr(parsed.text))

    # ---------------- [8] 错误路径 ----------------
    print("\n[8] 错误路径")
    ok, exc = _raises(dp.UnsupportedFormatError, dp.parse_document, "old.doc", b"x")
    _check(".doc 抛 UnsupportedFormatError", ok, repr(exc))
    _check(
        ".doc 提示里给出「另存为 .docx」的可操作建议",
        exc is not None and ".docx" in str(exc),
        str(exc),
    )
    _check(
        "不支持格式的错误里列出当前支持项",
        exc is not None and ".pdf" in str(exc),
        str(exc),
    )

    for name, payload in (("a.xlsx", b"x"), ("b.png", b"x"), ("c.zip", b"x")):
        ok, exc = _raises(dp.UnsupportedFormatError, dp.parse_document, name, payload)
        _check(f"{name} 被明确拒绝且带提示", ok and "。" in str(exc), str(exc)[:80])

    ok, exc = _raises(dp.DocumentParseError, dp.parse_document, "empty.txt", b"")
    _check("0 字节文件报错", ok and isinstance(exc, dp.DocumentParseError), repr(exc))
    _check(
        "0 字节报错不是 UnsupportedFormatError（区分「空」与「不支持」）",
        exc is not None and not isinstance(exc, dp.UnsupportedFormatError),
        type(exc).__name__,
    )

    ok, exc = _raises(dp.DocumentParseError, dp.parse_document, "bad.docx", b"not a zip")
    _check("损坏 docx 报错且不是裸 zipfile 异常", ok, repr(exc))
    _check("损坏 docx 提示是 OOXML/损坏", exc is not None and "OOXML" in str(exc), str(exc))

    ok, exc = _raises(dp.DocumentParseError, dp.parse_document, "bad.pptx", b"not a zip")
    _check("损坏 pptx 报错", ok, repr(exc))

    ok, exc = _raises(dp.DocumentParseError, dp.parse_document, "bad.pdf", b"not a pdf at all")
    _check("损坏 pdf 报错且被收敛成 DocumentParseError", ok, repr(exc))

    # 空白文本（只有空白字符）也要报错，否则会写进「有标题没正文」的知识
    ok, exc = _raises(dp.DocumentParseError, dp.parse_document, "blank.txt", b"   \n\t\n  ")
    _check("只有空白的文本报错", ok, repr(exc))

    # 超限：直接把上限临时改小来验证判定分支（真实 20MB 上限见下方常量断言）
    _check("MAX_UPLOAD_BYTES 为 20 MB", dp.MAX_UPLOAD_BYTES == 20 * 1024 * 1024, str(dp.MAX_UPLOAD_BYTES))
    original = dp.MAX_UPLOAD_BYTES
    try:
        dp.MAX_UPLOAD_BYTES = 16
        ok, exc = _raises(dp.FileTooLargeError, dp.parse_document, "big.txt", b"a" * 32)
        _check("超限抛 FileTooLargeError", ok, repr(exc))
        _check(
            "超限是 DocumentParseError 的子类（便于 API 层统一处理）",
            isinstance(exc, dp.DocumentParseError),
            type(exc).__name__,
        )
        _check("超限提示给出上限 MB 数", exc is not None and "MB" in str(exc), str(exc))
    finally:
        dp.MAX_UPLOAD_BYTES = original

    # ---------------- [9] 标题回落 ----------------
    print("\n[9] 标题回落")
    _check("去扩展名", dp.suggest_title("Redis 持久化手册.pdf") == "Redis 持久化手册")
    _check("带路径也去扩展名", dp.suggest_title("C:\\a\\b\\手册.docx") == "手册")
    _check("无扩展名时原样", dp.suggest_title("README") == "README")
    _check("空文件名有兜底", dp.suggest_title("") == "未命名文档", dp.suggest_title(""))
    _check("只有扩展名时有兜底", dp.suggest_title(".pdf") == ".pdf", dp.suggest_title(".pdf"))
    long_name = "长" * 400 + ".txt"
    _check(
        f"超长标题截断到 {dp.MAX_TITLE_CHARS}",
        len(dp.suggest_title(long_name)) == dp.MAX_TITLE_CHARS,
        str(len(dp.suggest_title(long_name))),
    )
    parsed = dp.parse_document("我的手册.md", "# 标题\n正文内容。".encode("utf-8"))
    _check("解析结果自带 suggested_title", parsed.suggested_title == "我的手册", parsed.suggested_title)

    # ---------------- [10] 分层守卫（AST） ----------------
    print("\n[10] 分层守卫（AST）")
    source_path = pathlib.Path(dp.__file__)
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source)

    top_level_imports = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            top_level_imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            top_level_imports.append(node.module or "")

    banned = ("database", "fastapi", "sqlalchemy", "models", "schemas", "main", "deps")
    for module in banned:
        # 用点号全名匹配：裸名会让 `services.database_x` 之类被误伤，也让守卫形同虚设
        offenders = [
            name
            for name in top_level_imports
            if name == module or name.startswith(module + ".")
        ]
        _check(f"顶层不 import {module}", offenders == [], str(offenders))

    _check(
        "顶层不 import 项目内其它 service",
        [
            name
            for name in top_level_imports
            if name.startswith("services.") and name != "services.document_parser"
        ]
        == [],
        str([n for n in top_level_imports if n.startswith("services")]),
    )

    # pypdf 必须延迟导入：否则「不装 pypdf 也能起后端」这条性质就没了
    top_has_pypdf = any(name == "pypdf" or name.startswith("pypdf.") for name in top_level_imports)
    _check("pypdf 不在顶层 import（延迟导入）", not top_has_pypdf, str(top_level_imports))

    # ---------------- [11] 纯函数 ----------------
    print("\n[11] 纯函数（无隐藏状态）")
    first = dp.parse_document("a.docx", make_docx())
    second = dp.parse_document("a.docx", make_docx())
    _check("同输入两次结果一致", first.text == second.text and first.warnings == second.warnings)

    parsed = dp.parse_document("x.txt", make_text("utf-8"))
    before = parsed.text
    dp.parse_document("y.pdf" if False else "y.txt", b"other")
    _check("解析别的文件不污染上一次结果", parsed.text == before)

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if _run() else 1)
