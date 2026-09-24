# -*- coding: utf-8 -*-
"""Prompt 资源加载 / 变量注入 / JSON 解析。

职责边界
--------
本模块是 **Prompt 与 Python 业务逻辑之间的唯一接缝**，只做三件事：

1. **加载**：从 ``prompts/<group>/<name>.txt`` 读取纯文本模板（带缓存）；
2. **变量注入**：把 Python 侧的变量填进模板占位符；
3. **JSON 解析**：把模型返回的文本稳健地解析成 dict。

它**不含任何面试业务规则**：不判断题号、不推进阶段、不校验业务枚举、不读写数据库，
也**不依赖** ``database`` / ``models`` / ``fastapi`` / ``main``，因此可脱离任何运行
环境单独测试（不需要 ``DATABASE_URL``）。

占位符语法
----------
统一使用 ``{{variable}}``（双花括号）。刻意**不用** ``str.format``：

- Prompt 正文需要内嵌 JSON 示例（含 ``{`` / ``}``），``str.format`` 要求把每个花括号
  转义成 ``{{``，既难写又极易出错；
- ``{{name}}`` 只匹配双花括号，正文里的单个花括号原样保留。

两个安全性质：

- **单次替换、不递归展开**——变量值中若含 ``{{...}}`` 不会被二次展开，
  避免变量之间互相污染；
- **名称白名单**——``group`` / ``name`` 只允许字母、数字、下划线、连字符，
  拒绝 ``..`` 与路径分隔符，杜绝目录穿越。

``strict`` 模式（默认开启）会双向校验变量：模板里缺变量、或 Python 多传了模板用不到的
变量，都会立刻报错——后者能捕获调用方拼错变量名这类静默缺陷。
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

# ============================================================
# 一、常量
# ============================================================
#: Prompt 根目录（本文件所在目录），子目录即 group
_PROMPTS_ROOT: Path = Path(__file__).resolve().parent

#: 默认分组
DEFAULT_GROUP = "interview"

#: 空值的统一占位（让 Prompt 读起来自然，而不是留一个空白行）
EMPTY_VALUE = "（无）"

#: 采分点数量的结构性上限（防止模型返回无界数组写进数据库 JSON 列）
MAX_EXPECTED_POINTS = 12

#: 占位符：{{variable}}
_PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")

#: group / name 白名单（杜绝目录穿越）
_NAME_RE = re.compile(r"^[A-Za-z0-9_\-]+$")

#: markdown 代码块围栏（模型偶发包裹）
_FENCE_OPEN = re.compile(r"^```[A-Za-z0-9_+\-]*[ \t]*\r?\n?")
_FENCE_CLOSE = re.compile(r"\r?\n?```[ \t]*$")

#: (group, name) -> 模板内容
_CACHE: Dict[Tuple[str, str], str] = {}


# ============================================================
# 二、异常
# ============================================================
class PromptError(Exception):
    """Prompt 相关错误的基类。"""


class PromptNotFoundError(PromptError, FileNotFoundError):
    """Prompt 文件不存在 / 名称为空 / 名称非法。"""


class PromptRenderError(PromptError, ValueError):
    """变量注入失败：缺变量、多变量，或名称非法。"""


class PromptJSONError(PromptError, ValueError):
    """模型输出无法解析为合法 JSON 对象。"""


# ============================================================
# 三、加载
# ============================================================
def _root() -> Path:
    """当前 Prompt 根目录（经函数取，便于测试临时替换 ``_PROMPTS_ROOT``）。"""
    return _PROMPTS_ROOT


def _resolve_path(name: str, group: str) -> Path:
    """把 (group, name) 解析成绝对路径，并拒绝任何越界名称。"""
    if not isinstance(name, str) or not _NAME_RE.match(name or ""):
        raise PromptNotFoundError(f"非法的 prompt 名称：{name!r}")
    if not isinstance(group, str) or not _NAME_RE.match(group or ""):
        raise PromptNotFoundError(f"非法的 prompt 分组：{group!r}")

    root = _root().resolve()
    path = (root / group / f"{name}.txt").resolve()
    if not path.is_relative_to(root):
        raise PromptNotFoundError(f"prompt 路径越界：{path}")
    return path


def _read_text(path: Path) -> str:
    if not path.is_file():
        raise PromptNotFoundError(f"Prompt 文件不存在：{path}")
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        raise PromptError(f"Prompt 文件为空：{path}")
    return text


def load_prompt(name: str, group: str = DEFAULT_GROUP, *, use_cache: bool = True) -> str:
    """读取 Prompt 模板原文（未注入变量）。

    读取结果按 ``(group, name)`` 缓存，避免每次出题都读盘。
    """
    key = (group, name)
    if use_cache and key in _CACHE:
        return _CACHE[key]

    text = _read_text(_resolve_path(name, group))
    if use_cache:
        _CACHE[key] = text
    return text


def list_prompts(group: str = DEFAULT_GROUP) -> List[str]:
    """列出某分组下的全部 Prompt 名（不含 ``.txt`` 后缀），按名称排序。"""
    if not isinstance(group, str) or not _NAME_RE.match(group or ""):
        raise PromptNotFoundError(f"非法的 prompt 分组：{group!r}")
    directory = (_root() / group).resolve()
    if not directory.is_dir():
        return []
    return sorted(p.stem for p in directory.glob("*.txt") if p.is_file())


def clear_cache() -> None:
    """清空模板缓存（测试或热更新时使用）。"""
    _CACHE.clear()


# ============================================================
# 四、变量注入
# ============================================================
def stringify(value: Any) -> str:
    """把任意 Python 值转成适合塞进 Prompt 的文本。

    规则（确定性）：

    - ``None`` → ``EMPTY_VALUE``
    - ``bool`` → ``"true"`` / ``"false"``（不走 ``str()``，避免出现 ``True``）
    - 数字 → ``str()``
    - 字符串 → 原样
    - 空列表 → ``EMPTY_VALUE``
    - 全标量列表 → 逐行 ``- item`` 的项目符号列表（模型更易读）
    - 含嵌套结构的列表 / dict → 缩进 JSON
    """
    if value is None:
        return EMPTY_VALUE
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        if not value:
            return EMPTY_VALUE
        if all(not isinstance(item, (list, tuple, dict)) for item in value):
            return "\n".join(f"- {stringify(item)}" for item in value)
        return json.dumps(list(value), ensure_ascii=False, indent=2)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, indent=2)
    return str(value)


def render_template(
    template: str, variables: Optional[Mapping[str, Any]] = None, *, strict: bool = True
) -> str:
    """把变量注入模板。**单次替换，不递归展开。**

    ``strict=True`` 时：模板中存在未被提供的变量 → :class:`PromptRenderError`；
    提供了模板中不存在的变量 → 同样报错（捕获调用方拼错变量名）。
    """
    provided = dict(variables or {})
    used: set[str] = set()

    def _substitute(match: "re.Match[str]") -> str:
        key = match.group(1)
        used.add(key)
        if key in provided:
            return stringify(provided[key])
        if strict:
            raise PromptRenderError(f"Prompt 缺少变量：{key}")
        return match.group(0)

    rendered = _PLACEHOLDER.sub(_substitute, template)

    if strict:
        unused = sorted(set(provided) - used)
        if unused:
            raise PromptRenderError(
                "提供了 Prompt 未使用的变量：" + ", ".join(unused)
            )
    return rendered


def render_prompt(
    name: str,
    variables: Optional[Mapping[str, Any]] = None,
    group: str = DEFAULT_GROUP,
    *,
    strict: bool = True,
    use_cache: bool = True,
) -> str:
    """加载并渲染 Prompt（业务代码的唯一入口）。"""
    template = load_prompt(name, group, use_cache=use_cache)
    return render_template(template, variables, strict=strict)


def template_variables(name: str, group: str = DEFAULT_GROUP) -> List[str]:
    """列出模板中出现的占位符名称（去重、保序），便于调用方自检。"""
    template = load_prompt(name, group)
    seen: List[str] = []
    for match in _PLACEHOLDER.finditer(template):
        if match.group(1) not in seen:
            seen.append(match.group(1))
    return seen


# ============================================================
# 五、JSON 解析
# ============================================================
def strip_code_fence(raw: str) -> str:
    """去掉模型偶发包裹的 markdown 代码块围栏。"""
    text = (raw or "").strip()
    if text.startswith("```"):
        text = _FENCE_OPEN.sub("", text)
        text = _FENCE_CLOSE.sub("", text)
    return text.strip()


def extract_json_object(raw: Any) -> Dict[str, Any]:
    """从模型输出中提取**第一个**合法 JSON 对象。

    比「取首个 ``{`` 到末个 ``}`` 再 loads」更稳健：用
    :meth:`json.JSONDecoder.raw_decode` 从每个 ``{`` 起尝试解析，
    因此能容忍 JSON 前后的解释性文字，也不会被正文里的花括号带偏。

    解析失败抛 :class:`PromptJSONError`（错误信息含输出片段，便于排查）。
    """
    if not isinstance(raw, str) or not raw.strip():
        raise PromptJSONError("模型输出为空，无法解析 JSON")

    text = strip_code_fence(raw)
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            parsed, _ = decoder.raw_decode(text[index:])
        except ValueError:
            continue
        if isinstance(parsed, dict):
            return parsed

    preview = text[:120].replace("\n", " ")
    raise PromptJSONError(f"模型输出中未找到合法的 JSON 对象：{preview}")


def _clean_str(value: Any) -> str:
    """归一为去空白字符串；非字符串走 ``str()``，``None`` 视为空串。"""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return ""


def _str_list(value: Any, limit: int = MAX_EXPECTED_POINTS) -> List[str]:
    """归一为字符串数组：**只保留字符串元素**（丢弃数字、布尔、null、嵌套结构），
    再剔除空串、保序去重、截断到上限。

    刻意不做类型强转——``expected_points`` 的语义是「采分点关键词」，
    把 ``123`` 强转成 ``"123"`` 只会往数据库里塞无意义的噪声。
    """
    if not isinstance(value, (list, tuple)):
        return []
    out: List[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        name = item.strip()
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(name)
        if len(out) >= limit:
            break
    return out


# ============================================================
# 六、question.txt 的输出解析
# ============================================================
def _question_failure(message: str) -> Dict[str, Any]:
    """失败结果的统一形状（与成功结果字段对齐，便于调用方无分支取值）。"""
    return {
        "ok": False,
        "question": "",
        "question_type": "",
        "topic": "",
        "difficulty": "",
        "expected_points": [],
        "reason": "",
        "error": message,
    }


def parse_question_output(raw: Any) -> Dict[str, Any]:
    """解析 ``question.txt`` 的模型输出。

    返回 ``{"ok": bool, "question", "question_type", "topic", "difficulty",
    "expected_points", "reason", "error"}``。

    失败（``ok=False``）的三种情形，都会把原因写进 ``error``：

    1. 输出不是合法 JSON 对象；
    2. 模型显式返回 ``error`` 字段（即 Prompt 约定的「无法生成」分支）；
    3. ``question`` 为空或缺失。

    **只做结构归一，不做业务校验**：``question_type`` / ``difficulty`` 是否属于
    业务枚举、题号是否推进、是否与上下文冲突，均属于业务层职责，本函数不判断。
    """
    try:
        data = extract_json_object(raw)
    except PromptJSONError as exc:
        return _question_failure(str(exc))

    error = _clean_str(data.get("error"))
    if error:
        return _question_failure(error)

    question = _clean_str(data.get("question"))
    if not question:
        return _question_failure("模型未返回有效问题：question 为空")

    return {
        "ok": True,
        "question": question,
        "question_type": _clean_str(data.get("question_type")),
        "topic": _clean_str(data.get("topic")),
        "difficulty": _clean_str(data.get("difficulty")),
        "expected_points": _str_list(data.get("expected_points")),
        "reason": _clean_str(data.get("reason")),
        "error": None,
    }
