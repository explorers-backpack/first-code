# -*- coding: utf-8 -*-
"""API 路由包。

各业务域一个模块，统一在 ``main.py`` 中通过 ``app.include_router`` 挂载，
以保持 ``main.py`` 不随业务增长而膨胀。

当前包含：
- ``interview``：AI 模拟面试（``/api/interview/*``）
"""
