# -*- coding: utf-8 -*-
"""认证模块的**请求契约**（纯数据，不含任何业务逻辑）。

为什么从 ``main.py`` 搬出来
--------------------------
这两个模型原本定义在入口文件里，与 ``JobCreate`` / ``ChatRequest`` / ``MatchRequest``
等**别的业务域**的模型混在一起。它们只服务于 ``/api/auth/*``，属于认证模块的契约，
跟着模块走才能让「改认证不影响岗位」这件事在文件层面成立。

★ 安全约定（**不要添加 role 字段**）
``UserRegister`` 显式声明 ``extra="ignore"``：客户端即使传 ``{"role": "admin"}``
也不会被 Pydantic 收下，注册出来的账号恒为普通用户。这条约定由
``tests/test_admin_role.py`` 第 [9] 节断言。

刻意**不**新增响应模型：四个端点的响应体是既有的字典形状（``success`` / ``message`` /
``token`` / ``user``），加一层 Pydantic 响应模型只会多一处需要同步的契约
（「避免重复的接口封装」）。
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class UserRegister(BaseModel):
    """注册请求体。

    **不包含 ``role``**：注册一律是普通用户，管理员只能由运维用脚本创建。

    ★ 约束值必须与搬迁前**逐字一致**（``username`` 2~80 / ``email`` ≤120 /
    ``password`` 6~128）：放宽会让原本 422 的请求变成 200，收紧会让原本能注册的
    用户突然失败——两种都是**静默的契约变更**。
    """

    model_config = ConfigDict(extra="ignore")

    username: str = Field(..., min_length=2, max_length=80)
    email: str = Field(..., max_length=120)
    password: str = Field(..., min_length=6, max_length=128)


class UserLogin(BaseModel):
    """登录请求体：``email`` 字段**同时接受邮箱或用户名**（历史行为，勿改）。"""

    email: str
    password: str


__all__ = ["UserLogin", "UserRegister"]
