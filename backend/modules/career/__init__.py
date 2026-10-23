# -*- coding: utf-8 -*-
"""求职业务模块：简历、岗位、匹配与职业规划对话。

结构
----
::

    modules/career/
      __init__.py   门面（只 re-export router）
      api.py        HTTP 端点（协议 / 校验 / 响应转换）
      service.py    用例编排 + 岗位数据访问（零 FastAPI）
      schemas.py    请求契约
      prompts.py    Prompt 模板（逐字节迁出，独立于代码演进）
      ports.py      LLM 能力的显式注册点（避免反向依赖 main）

依赖方向：``api -> service / schemas / ports``，``service -> services.*``。
没有任何一条边指向 ``main`` 或 ``api``。
"""

from modules.career.api import router

__all__ = ["router"]
