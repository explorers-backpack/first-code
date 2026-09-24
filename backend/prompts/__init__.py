# -*- coding: utf-8 -*-
"""Prompt 资源包。

目录结构::

    prompts/
      loader.py            # 加载 / 变量注入 / JSON 解析（唯一接缝）
      interview/
        system.txt         # AI 面试官角色定义
        question.txt       # 问题生成 Prompt
        planner.txt        # 面试计划 Prompt

设计原则
--------
**Prompt 与 Python 业务逻辑分离**：

- Prompt 一律以纯文本文件存放，业务代码中不内联长 Prompt；
- Python 只负责「注入变量」与「解析 JSON」，不把业务规则写进 Prompt，
  也不在 Prompt 里描述数据库操作；
- 本包不依赖 ``database`` / ``models`` / ``fastapi``，可单独测试。
"""

from prompts.loader import (
    DEFAULT_GROUP,
    EMPTY_VALUE,
    MAX_EXPECTED_POINTS,
    PromptError,
    PromptJSONError,
    PromptNotFoundError,
    PromptRenderError,
    clear_cache,
    extract_json_object,
    list_prompts,
    load_prompt,
    parse_question_output,
    render_prompt,
    render_template,
    stringify,
    strip_code_fence,
    template_variables,
)

__all__ = [
    "DEFAULT_GROUP",
    "EMPTY_VALUE",
    "MAX_EXPECTED_POINTS",
    "PromptError",
    "PromptNotFoundError",
    "PromptRenderError",
    "PromptJSONError",
    "load_prompt",
    "render_prompt",
    "render_template",
    "list_prompts",
    "template_variables",
    "clear_cache",
    "stringify",
    "strip_code_fence",
    "extract_json_object",
    "parse_question_output",
]
