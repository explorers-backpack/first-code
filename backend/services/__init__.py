# -*- coding: utf-8 -*-
"""backend.services —— 业务服务层。

当前包含：
- ``resume_scoring``：简历 8 维可取证评分（纯规则、确定性、每维附原文证据）
- ``job_matching``：岗位技能匹配（``keyword_match`` / ``match_job``，
  纯规则、确定性；原 ``main.py`` 中重复定义两份的 ``keyword_match``
  已合并至此，作为全项目唯一实现）
- ``interview_service``：AI 模拟面试（会话生命周期 / 出题 / 评分 / 报告汇总，
  纯规则、确定性，复用 ``resume_scoring`` 的技能与量化识别词表）
- ``interview_context``：AI 模拟面试的**上下文管理**（每场面试一份独立上下文，
  记录阶段 / 已问问题 / 已覆盖与薄弱知识点 / 追问次数 / 题量上限；
  上下文存于独立表 ``interview_context``，与 ``interview_session`` 1:1）
- ``interview_planner``：AI 模拟面试的**计划制定**（由「简历 + 岗位 + 面试配置」
  产出结构化 InterviewPlan：阶段权重 / 各阶段目标题量 / 目标知识点 /
  优先考察点 / 简历重点；**只制定计划、不出题**。纯规则确定性实现，
  可选调用既有 Spark 服务增强知识点抽取，失败自动回退）
- ``interview_agent``：AI 模拟面试的**问题生成**（按「上下文 + 计划 + 简历 + 岗位」
  生成**一道**问题，经解析与校验后返回结构化 InterviewQuestion）。
  **只做出题**：不做回答评分、动态追问、下一题决策、面试报告；
  不读库不写库（持久化由调用方负责）。  Prompt 取自 ``prompts/interview/``，
  失败最多修复一次，绝不伪造问题。
- ``question_validator``：面试问题的**校验器**。
  ``QuestionValidator.validate(question_data, context, plan) -> ValidationResult``，
  ``errors`` 中为**稳定错误码**（``question_empty`` / ``topic_empty``）。
  零第三方依赖（不 import fastapi / sqlalchemy / main），纯内存、可脱离 HTTP 与
  数据库单测。当前**只实现 question / topic 的基础字段校验**；
  difficulty 校验、重复检测、字段补全、Agent 集成尚未实现。

设计约定
--------
- service 层不依赖 FastAPI 的请求上下文，函数以 ``db: AsyncSession`` 为第一参数
  （依赖注入），返回普通 dict；HTTP 相关职责由 ``api/`` 层承担。
- 分数一律由确定性规则产出，保证同输入同输出、可复现、可申诉。
"""
