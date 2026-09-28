# -*- coding: utf-8 -*-
"""``tests/`` 共享的**内存样本构造器**（供 ``test_document_parser`` 与
``test_knowledge_upload_api`` 复用）。

**不是回归套件**（无 ``test_`` 前缀，不进回归循环），与 ``regression_env`` /
``vector_backend_env`` 同类：只提供工具函数，不 import 任何项目模块、不碰 DB。

为什么自己造样本而不用现成文件
------------------------------
1. 仓库里不放二进制样本（体积、授权、diff 噪音）；
2. 造出来的样本**内容可断言**——「解析出来必须是这几行字」，而不是「文件没报错」；
3. docx / pptx 本质是 OOXML（ZIP + XML），**几十行就能造出结构合法的样本**；
4. PDF 的 xref 偏移必须算对，否则解析器会走「重建 xref」的容错分支，
   测到的就不是正常路径了——所以这里按字节偏移精确拼装。
"""

from __future__ import annotations

import io
import zipfile
from typing import List, Optional

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
A_NS = "http://schemas.openxmlformats.org/drawingml/2006/main"
P_NS = "http://schemas.openxmlformats.org/presentationml/2006/main"

#: 供断言使用的固定文案（改这里 = 改所有断言，保持单一来源）
DOCX_PARAGRAPHS = ("Redis 持久化", "RDB 是快照，AOF 是追加日志。")
PPTX_SLIDES = (
    (1, "第一页标题", "要点一"),
    (2, "第二页标题", "要点二"),
    (10, "第十页标题", "要点十"),
)
PDF_LINES = ("Redis persistence handbook", "RDB is snapshot")


def make_docx(paragraphs=None) -> bytes:
    """造一个结构合法的 ``.docx``（含 ``word/document.xml``）。

    第二段刻意拆成**两个 ``w:r`` run**，用来验证「同段内多个 run 会被拼接」
    ——这是真实 Word 文件的常态（改过格式的文字会被拆 run）。
    """
    paragraphs = paragraphs or DOCX_PARAGRAPHS
    body: List[str] = []
    for index, text in enumerate(paragraphs):
        if index == 1 and "，" in text:
            head, tail = text.split("，", 1)
            body.append(
                f"<w:p><w:r><w:t>{head}</w:t></w:r><w:r><w:t>，{tail}</w:t></w:r></w:p>"
            )
        else:
            body.append(f"<w:p><w:r><w:t>{text}</w:t></w:r></w:p>")
    # 第三段验证制表符与软换行
    body.append("<w:p><w:r><w:tab/><w:t>缩进项</w:t></w:r></w:p>")

    xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<w:document xmlns:w="{W_NS}"><w:body>{"".join(body)}</w:body></w:document>'
    ).encode("utf-8")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", "<Types/>")
        archive.writestr("word/document.xml", xml)
    return buffer.getvalue()


def _slide_xml(title: str, bullet: str) -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<p:sld xmlns:a="{A_NS}" xmlns:p="{P_NS}">'
        "<p:cSld><p:spTree><p:sp><p:txBody>"
        f"<a:p><a:r><a:t>{title}</a:t></a:r></a:p>"
        f"<a:p><a:r><a:t>{bullet}</a:t></a:r></a:p>"
        "</p:txBody></p:sp></p:spTree></p:cSld></p:sld>"
    ).encode("utf-8")


def make_pptx(slides=None, empty_slide_numbers=()) -> bytes:
    """造一个 ``.pptx``。``empty_slide_numbers`` 里的页会写成**无文本**，
    用来验证「纯图片页」会被如实记进 ``slide_N_no_text`` 告警。"""
    slides = slides or PPTX_SLIDES
    empty = set(empty_slide_numbers)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for number, title, bullet in slides:
            payload = (
                _slide_xml("", "")
                if number in empty
                else _slide_xml(title, bullet)
            )
            archive.writestr(f"ppt/slides/slide{number}.xml", payload)
    return buffer.getvalue()


def make_pdf(lines=None, with_text: bool = True) -> bytes:
    """造一个**结构合法**的最小 PDF（正确的 xref 偏移 + startxref）。

    ``with_text=False`` 造出「无文本层」的页（模拟扫描件），
    用来验证解析器会**明确报错**而不是导入一篇空文档。
    """
    lines = lines or PDF_LINES

    if with_text:
        ops = ["BT /F1 24 Tf 72 700 Td"]
        for index, line in enumerate(lines):
            if index:
                ops.append("0 -30 Td")
            ops.append(f"({line}) Tj")
        ops.append("ET")
        content = " ".join(ops).encode("latin-1")
    else:
        content = b""

    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length " + str(len(content)).encode() + b" >>\nstream\n" + content + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]

    out = bytearray(b"%PDF-1.4\n")
    offsets: List[int] = []
    for number, body in enumerate(objects, 1):
        offsets.append(len(out))
        out += b"%d 0 obj\n" % number + body + b"\nendobj\n"

    xref_at = len(out)
    out += b"xref\n0 %d\n" % (len(objects) + 1)
    out += b"0000000000 65535 f \n"
    for offset in offsets:
        out += b"%010d 00000 n \n" % offset
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\n" % (len(objects) + 1)
    out += b"startxref\n%d\n%%%%EOF\n" % xref_at
    return bytes(out)


def make_html() -> bytes:
    """含 ``<script>`` / ``<style>``（必须被丢掉）与表格（单元格转制表符）。"""
    return (
        b"<html><head><title>t</title><style>body{color:red}</style></head><body>"
        b"<h1>Redis \xe6\x8c\x81\xe4\xb9\x85\xe5\x8c\x96</h1>"
        b"<p>RDB \xe6\x98\xaf\xe5\xbf\xab\xe7\x85\xa7</p>"
        b"<script>var x=1;</script>"
        b"<table><tr><td>AOF</td><td>\xe8\xbf\xbd\xe5\x8a\xa0\xe6\x97\xa5\xe5\xbf\x97</td></tr></table>"
        b"</body></html>"
    )


def make_text(encoding: str = "utf-8") -> bytes:
    return "中文内容测试，Redis 持久化手册 RDB 与 AOF。".encode(encoding)
