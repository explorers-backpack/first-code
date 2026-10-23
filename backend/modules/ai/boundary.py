# -*- coding: utf-8 -*-
"""AI 能力接入层 · Prompt 数据边界。

要防的三类事故
--------------
1. **越权注入**：A 用户的会话里塞进 B 用户的简历。归属校验的**唯一可信来源**
   是 ``resume.user_id``（``interview_session.resume_id`` 只是外键，不能当归属用）。
2. **无关数据注入**：把整个 ORM 对象 / 全表 / 其他会话历史拼进 Prompt。
3. **敏感信息外泄**：手机号、邮箱、身份证号被原样发给第三方模型。

本模块只提供**判定与清洗**，不碰数据库——归属查询属于 service 层，
边界层拿到的是"这次 Prompt 属于谁、允许出现什么"，然后做**最后一道检查**。
把它做成独立一层的原因：这类检查一旦散落在各业务函数里，
新增一个 Prompt 就漏一次，而漏一次就是数据事故。
"""

import re
from dataclasses import dataclass, field
from typing import Iterable, Optional, Tuple

from modules.ai.errors import AiBoundaryError

__all__ = [
    "DEFAULT_MAX_PROMPT_CHARS",
    "PromptBoundary",
    "scrub_contact_info",
    "check_boundary",
    "build_guarded_prompt",
    "ensure_owner",
]

#: 单次 Prompt 的字符上限。超限说明拼装逻辑出了问题（把整表 / 整份文档塞进去了），
#: 直接拒绝而不是截断——截断会**静默丢数据**，比报错更难排查。
DEFAULT_MAX_PROMPT_CHARS = 20000

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
#: 中国大陆手机号 / 座机（含分隔符写法）
_PHONE_RE = re.compile(r"(?<!\d)(?:\+?86[-\s]?)?1[3-9]\d{9}(?!\d)")
_ID_CARD_RE = re.compile(r"(?<!\d)\d{17}[\dXx](?!\d)")


def scrub_contact_info(text: str, *, placeholder: str = "[已脱敏]") -> str:
    """抹掉手机号 / 邮箱 / 身份证号。

    **只在"确实需要把原文发给第三方模型"时使用**；结构化字段（姓名、学校）
    通常不必抹——诊断质量依赖它们。是否抹由调用方按 Prompt 用途决定，
    本函数只提供能力，不强制。
    """
    if not text:
        return text
    out = _ID_CARD_RE.sub(placeholder, text)
    out = _PHONE_RE.sub(placeholder, out)
    out = _EMAIL_RE.sub(placeholder, out)
    return out


@dataclass(frozen=True)
class PromptBoundary:
    """一次 Prompt 拼装的数据边界声明。

    :param owner_id: 这份数据归属的用户 ID（空串 = 系统级 Prompt，不做归属校验）
    :param max_chars: 字符上限
    :param forbidden: **绝对不允许出现**的字符串（通常是其他用户的标识符）。
        只放"确定性字符串"（用户 ID / 手机号 / 邮箱），不要放需要模糊匹配的东西。
    """

    owner_id: str = ""
    max_chars: int = DEFAULT_MAX_PROMPT_CHARS
    forbidden: Tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def for_user(cls, owner_id: str, *, max_chars: int = DEFAULT_MAX_PROMPT_CHARS,
                 forbidden: Iterable[str] = ()) -> "PromptBoundary":
        return cls(
            owner_id=str(owner_id or ""),
            max_chars=max_chars,
            forbidden=tuple(item for item in forbidden if item),
        )


def ensure_owner(record_owner: Optional[str], actor_id: Optional[str], *, what: str = "数据") -> None:
    """归属校验：``record_owner`` 必须等于 ``actor_id``。

    两者任一为空时**不做判定**（系统级调用 / 单元测试注入的场景），
    但"有一个非空一个为空"属于**拼装错误**，同样拒绝。
    """
    left = "" if record_owner is None else str(record_owner).strip()
    right = "" if actor_id is None else str(actor_id).strip()
    if not left and not right:
        return
    if left != right:
        raise AiBoundaryError(
            "%s归属校验失败：记录归属与当前用户不一致，已阻止注入 Prompt" % what
        )


def check_boundary(prompt: str, boundary: PromptBoundary) -> None:
    """最后一道检查。违规 ⇒ :class:`AiBoundaryError`（**不截断、不静默**）。"""
    if boundary.max_chars and len(prompt) > boundary.max_chars:
        raise AiBoundaryError(
            "Prompt 长度 %d 超过上限 %d，疑似注入了无关数据（拒绝截断以免静默丢信息）"
            % (len(prompt), boundary.max_chars)
        )
    for token in boundary.forbidden:
        if token and token in prompt:
            # 只报"出现了哪个 token 的类别"，不回显 token 本身（回显等于二次泄露）。
            raise AiBoundaryError(
                "Prompt 中出现了禁止注入的标识符（长度 %d），已阻止发送" % len(token)
            )


def build_guarded_prompt(text: str, boundary: PromptBoundary, *, scrub: bool = False) -> str:
    """按边界处理 Prompt：可选脱敏 → 校验 → 返回。

    ``scrub=True`` 会先抹掉联系方式再校验（脱敏后仍超长 / 仍含禁注入标识符 ⇒ 依旧拒绝）。
    """
    body = scrub_contact_info(text) if scrub else (text or "")
    check_boundary(body, boundary)
    return body
