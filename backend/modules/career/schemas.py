# -*- coding: utf-8 -*-
"""求职业务（career）的请求契约。

★ 这些模型的字段约束**逐字对齐** ``main.py`` 迁移前的定义。放宽会让原本
返回 422 的请求变成 200；收紧会让原本能通过的请求突然失败——两种都是
**静默的契约变更**，前端不会报错，只是行为变了。

三个模型都没有 ``extra`` 配置，即沿用 Pydantic 默认的 ``ignore``：
客户端多传字段不会 422。迁移前就是这个行为，保持不变。
"""

from typing import List, Optional

from pydantic import BaseModel


class ChatRequest(BaseModel):
    """``POST /api/chat`` 的请求体。

    ``jobs`` 由前端直接传入岗位列表时走「数据库来源」分支，
    为空则后端按 ``message`` 里的技能关键词自己查库。
    """

    message: str
    jobs: Optional[List[dict]] = None


class MatchRequest(BaseModel):
    """``POST /api/match`` 的请求体。"""

    skills: List[str]
    min_match_rate: int = 30


class ResumeParseRequest(BaseModel):
    """``POST /api/resume/parse`` 的请求体。"""

    resume_text: str
