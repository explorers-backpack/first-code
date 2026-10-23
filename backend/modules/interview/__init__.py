# -*- coding: utf-8 -*-
"""面试模块（`interview`）：面试会话、题目、答案、评分与报告。

分层
----
::

    modules/interview/api.py    ← 文字面试 HTTP 契约（/api/interview/*）
    modules/interview/ws.py     ← 数字人实时通道（WebSocket 协议编解码）
    modules/interview/service.py（P5-2 迁入）← 用例编排 + 事务边界
    services/interview_core.py  ← ★ 冻结：流程控制与业务规则（零 DB 耦合）
    services/interview_agent.py 等 ← ★ 冻结：出题 / 校验 / 计划 / 上下文

为什么 `services/` 里还留着 `interview_*`
------------------------------------------
那批文件的**内容**被 ``tests/test_agent_full_input.py`` 等 4 个套件的
``FROZEN_SOURCES`` 用 sha256 锁死（改一个字节整个回归就红）。搬它们的**位置**
要同时改指纹表的键与全仓约 150 处路径字面量，一次改动会同时影响 20+ 个套件，
无法定位回归。

因此本模块的迁移分三步：
- **P5-1**（已完成）路由层；
- **P5-2** 会话门面 ``interview_service``；
- **P5-3** 编排 / 生产化 / 追问 / 数字人适配。

冻结文件**永远留在 `services/`**，作为「不可变的领域核心」——这正是模块化单体
里「领域内核 vs 应用层」的正常分工，不是妥协。

对外只暴露两个路由对象
----------------------
``router``（HTTP）与 ``ws_router``（WebSocket）。业务函数由调用方直接从
``service`` 模块取，**不做包级 re-export 大礼包**（避免 `__init__` 变成万能门面）。
"""

from __future__ import annotations

from modules.interview.api import router
from modules.interview.ws import router as ws_router

__all__ = ["router", "ws_router"]
