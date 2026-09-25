# -*- coding: utf-8 -*-
"""数字人视频面试通道 —— **预留入口，当前为空实现**。

定位（交互通道层）
------------------
``interview_mode`` 只决定「**怎么交付**」，不决定「**面什么**」。因此：

::

    InterviewService（会话流程 / 数据访问 / 前端契约）
        ├── 面试逻辑：出题 · 评分 · 报告 · 状态机   ← 两种模式**完全共享**（在 interview_core）
        └── 交付边界（按 interview_mode 分流）
              ├── text   → 原样返回（前端用输入框/文本渲染）      ← 既有流程，一行未改
              └── avatar → 【本模块】enter()                    ← 数字人通道入口

**本模块不参与出题 / 评分 / 报告**，只负责「把 Service 组装好的交付内容交给数字人通道」。

当前状态
--------
``STATUS = "pending"``：通道**尚未接入**，``enter()`` 是**原样透传**的空实现。
因此 avatar 会话目前与 text 会话拿到**完全相同**的交付内容——
两种模式互不影响，也意味着**不会因为本模块未完成而阻塞任何流程**。

明确未接入（本次不接）
----------------------
- 讯飞数字人（``INTEGRATIONS["iflytek"] is False``）
- ASR（语音识别）
- TTS（语音合成）

接入时怎么改（只需改本模块）
----------------------------
数字人能力接入后，**只改 ``enter()`` 的函数体**，``interview_service`` 与
``interview_core`` 都不用动。典型步骤：

1. 用 ``session.id`` 向数字人服务注册/唤醒一场会话；
2. 调 TTS 合成开场白，拿到音频流地址；
3. 把通道专属字段（如 ``stream_url`` / ``avatar_id``）补进 ``payload`` 再返回，
   并在 ``schemas`` 的响应模型上声明这些可选字段；
4. 同时把 ``STATUS`` 改为 ``"ready"``、``INTEGRATIONS`` 对应项改为 ``True``。

``enter()`` 之所以签名里带 ``db`` 与 ``session``，就是为了让上面第 1 步
（可能落库记录数字人会话 id）不需要再改调用方。
"""

from __future__ import annotations

from typing import Any, Dict

from sqlalchemy.ext.asyncio import AsyncSession

# ============================================================
# 通道元信息（供上层与测试判断「接没接」）
# ============================================================
CHANNEL = "avatar"

#: ``pending`` = 尚未接入（当前）；``ready`` = 已可交付
STATUS_PENDING = "pending"
STATUS_READY = "ready"
STATUS = STATUS_PENDING

#: 外部能力接入开关。当前**全部为 False** —— 本次不接讯飞数字人 / ASR / TTS。
INTEGRATIONS: Dict[str, bool] = {
    "iflytek": False,  # 讯飞数字人
    "asr": False,      # 语音识别
    "tts": False,      # 语音合成
}


# ============================================================
# 入口（唯一接线点）
# ============================================================
async def enter(
    db: AsyncSession,
    session: Any,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """数字人通道入口 —— **空实现**，原样透传 ``payload``。

    调用链::

        interview_service.start_session
            └── avatar_interview.enter(db, session, payload)   ← 本函数
                    └── return payload                          ← 空实现

    参数
    ----
    - ``db``：会话用的 ``AsyncSession``。**当前不用**，预留给出「登记数字人会话 id」
      这类需要落库的实现，避免届时再改调用方签名。
    - ``session``：``InterviewSession`` 行（可读 ``id`` / ``interview_mode`` /
      ``total_questions`` 等），供数字人服务定位这场面试。
    - ``payload``：Service 已组装好的交付内容
      （``session`` / ``question`` / ``job_name`` / ``message``）。

    返回值
    ------
    与入参 ``payload`` **同一个对象**（未做任何拷贝或改写），
    保证 avatar 与 text 两种模式在通道未接入前交付内容完全一致。

    TODO(avatar-1)：注册/唤醒数字人会话
    TODO(avatar-2)：TTS 合成开场白并回填音频流地址
    TODO(avatar-3)：ASR 接管作答（届时 ``submit_answer`` 也走本通道）
    """
    return payload


__all__ = [
    "CHANNEL",
    "STATUS",
    "STATUS_PENDING",
    "STATUS_READY",
    "INTEGRATIONS",
    "enter",
]
