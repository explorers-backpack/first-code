# -*- coding: utf-8 -*-
"""backend.services —— 业务服务层。

当前包含：
- ``resume_scoring``：简历 8 维可取证评分（纯规则、确定性、每维附原文证据）
- ``job_matching``：岗位技能匹配（``keyword_match`` / ``match_job``，
  纯规则、确定性；原 ``main.py`` 中重复定义两份的 ``keyword_match``
  已合并至此，作为全项目唯一实现）
- ``interview_service``：AI 模拟面试（会话生命周期 / 出题 / 评分 / 报告汇总，
  纯规则、确定性，复用 ``resume_scoring`` 的技能与量化识别词表）

设计约定
--------
- service 层不依赖 FastAPI 的请求上下文，函数以 ``db: AsyncSession`` 为第一参数
  （依赖注入），返回普通 dict；HTTP 相关职责由 ``api/`` 层承担。
- 分数一律由确定性规则产出，保证同输入同输出、可复现、可申诉。
"""
