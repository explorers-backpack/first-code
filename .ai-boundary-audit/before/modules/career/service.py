# -*- coding: utf-8 -*-
"""求职业务（career）的用例编排与事务边界。

职责
----
* 岗位读取（``get_all_jobs``）—— 本项目里唯一查 ``Job`` 表的地方；
* 技能匹配（``filter_jobs_by_keywords``）—— 阈值过滤 + 按命中率降序；
* 三个需要大模型的用例（``chat`` / ``analyze_resume_text`` / ``parse_resume_text``）。

不做什么
--------
* **不 import FastAPI**：没有 ``HTTPException``、没有 ``Request``/``Response``。
  入参非法由 ``api.py`` 在调用前拦掉（那是协议层的事）。
* **不读全局变量拿 LLM**：``llm`` 一律是显式参数（鸭子类型，只需
  ``async chat_async(prompt) -> str``）。这样测试可以直接传假对象，
  不需要 monkeypatch 任何模块属性。
* **不碰上传文件**：``UploadFile`` 留在 ``api.py``，这里只收已解码的 ``str``。

迁移说明
--------
本文件的函数体**逐字迁出**自 ``main.py``（原 216-275 / 1002-1058 /
1123-1231 / 1234-1270 行）。除了把 ``spark_api`` 换成参数 ``llm``、
把 ``async_session`` 换成模块级 import 之外，逻辑与文案一行未改。

★ ``chat`` 里的 ``ai_response.startswith("Error")`` 是**已知死代码**：门面
失败时返回的是 ``"API调用失败: ..."``，不以 ``"Error"`` 开头。这里**刻意
保留**——本轮是结构迁移，不夹带行为修复（修了会让 ``/api/chat`` 的失败
文案变化，属另一件事）。已在 ``MODULAR_MONOLITH_REPORT.md`` 登记为遗留项。
"""

import json
import re
from typing import Any, Dict, List

from sqlalchemy import select

from database import async_session
from models import Job
from modules.career.prompts import EMPTY_PROMPT, RAG_PROMPT, RESUME_ANALYSIS_PROMPT
from modules.ai import ai_call_scope
from services.job_matching import keyword_match
from services.resume_scoring import build_prompt_summary, evaluate_resume


# ============================================================
# 数据访问：岗位
# ============================================================
async def get_all_jobs() -> List[dict]:
    """读出全部岗位（8 个字段的扁平 dict）。

    ★ 这是全项目**唯一**直接 ``select(Job)`` 的地方。路由层不写 SQL（守卫
    ``test_module_boundaries`` R7），管理员新增岗位也应落到这里，而不是在
    路由函数里 ``db.add()``（当前仍在 ``main:add_job``，P4 迁出）。

    ★ 用 ``async_session()`` 自己开事务，不接收外部 ``db``：调用方是路由层，
    没有跨多次查询的事务需求；如果将来需要与调用方共享事务，再加一个
    ``db`` 参数，不要改成全局会话。
    """
    async with async_session() as session:
        result = await session.execute(select(Job))
        jobs = result.scalars().all()
        return [
            {
                "id": j.id,
                "job_name": j.job_name,
                "salary": j.salary,
                "edu_require": j.edu_require,
                "major_require": j.major_require,
                "skills": j.skills,
                "duty": j.duty,
                "city": j.city,
                "industry": j.industry,
            }
            for j in jobs
        ]


async def filter_jobs_by_keywords(user_skills: List[str], min_match_rate: int = 30):
    """按技能关键词匹配岗位，只保留命中率 >= 阈值的，按命中率降序。

    返回项结构固定 5 键：``{job_id, job_name, city, salary, keyword_match}``。
    前端与 ``clean_jobs_to_text`` 都依赖这个结构（``test_job_matching`` 有断言）。
    """
    jobs = await get_all_jobs()
    results = []
    for job in jobs:
        result = keyword_match(user_skills, job.get("skills", ""))
        if result["match_rate"] >= min_match_rate:
            results.append(
                {
                    "job_id": job["id"],
                    "job_name": job["job_name"],
                    "city": job["city"],
                    "salary": job["salary"],
                    "keyword_match": result,
                }
            )
    results.sort(key=lambda x: x["keyword_match"]["match_rate"], reverse=True)
    return results


def clean_jobs_to_text(jobs):
    """把匹配结果渲染成 Prompt 里的岗位段（人读格式）。

    ★ 输出文案逐字节不变：模型看到的输入变了，生成质量就会变，而这轮
    不改行为。
    """
    if not jobs:
        return "暂无匹配岗位"
    lines = []
    for i, job in enumerate(jobs, 1):
        match_rate = job.get("keyword_match", {}).get("match_rate", 0)
        matched = job.get("keyword_match", {}).get("matched", [])
        skills = job.get("skills", "未标注")
        line = (
            f"【岗位{i}】{job.get('job_name', '未知')} | 薪资：{job.get('salary', '未标注')} "
            f"| 城市：{job.get('city', '未标注')} | 技能要求：{skills} "
            f"| 当前匹配度：{match_rate}% | 用户已有技能：{','.join(matched) if matched else '暂无'}"
        )
        lines.append(line)
    return "\n".join(lines)


# ============================================================
# 用例：职业规划对话
# ============================================================
async def chat(data: Any, *, llm: Any) -> Dict[str, Any]:
    """``POST /api/chat`` 的用例。

    两条数据来源分支：
    * 前端直接给了岗位列表（``data.jobs``）⇒ 用它，``source="database"``；
    * 没给 ⇒ 按 ``message`` 里的逗号分隔技能查库取前 5 条；查不到则用
      ``EMPTY_PROMPT`` 走通用建议，``source="ai"``。

    ``message`` 太短（< 5 字符）时不调模型，直接回固定话术
    ``source="invalid_input"`` —— 这是既有的省配额逻辑，保持不变。
    """
    message = data.message
    front_jobs = data.jobs or []

    if not message or len(message.strip()) < 5:
        return {
            "chat_answer": "由于您提供的信息（专业、技能、学历）不足，我目前无法为您量身定制职业规划。为了得到精准建议，请您详细描述您的背景（例如：熟悉 Python 开发的应届生）。",
            "top_jobs": [],
            "source": "invalid_input",
        }

    user_skills = message.strip()

    if front_jobs:
        jobs_text = clean_jobs_to_text(front_jobs)
        prompt = RAG_PROMPT.format(user_skills=user_skills, jobs_text=jobs_text)
        jobs_data = front_jobs
        source_type = "database"
    else:
        skills = [s.strip() for s in message.split(",") if s.strip()]
        results = await filter_jobs_by_keywords(skills, min_match_rate=20)
        db_jobs = results[:5]

        if db_jobs:
            jobs_text = clean_jobs_to_text(db_jobs)
            prompt = RAG_PROMPT.format(user_skills=user_skills, jobs_text=jobs_text)
            jobs_data = [
                {
                    "job_id": j["job_id"],
                    "job_name": j["job_name"],
                    "city": j["city"],
                    "salary": j["salary"],
                    "keyword_match": j["keyword_match"],
                }
                for j in db_jobs
            ]
            source_type = "database"
        else:
            prompt = EMPTY_PROMPT.format(user_skills=user_skills)
            jobs_data = []
            source_type = "ai"

    try:
        ai_response = await llm.chat_async(prompt)
    except Exception:
        ai_response = "抱歉，暂时无法获取分析结果。"

    # ★ 已知死代码（见模块 docstring）：门面失败返回 "API调用失败: ..."。
    #   保留是为了不让本轮结构迁移改变失败文案。
    if ai_response.startswith("Error"):
        ai_response = "抱歉，暂时无法获取分析结果。"

    return {
        "chat_answer": ai_response,
        "top_jobs": jobs_data,
        "source": source_type,
    }


# ============================================================
# 用例：简历分析（规则评分 + LLM 文字诊断）
# ============================================================
async def analyze_resume_text(text: str, *, llm: Any) -> Dict[str, Any]:
    """对已解码的简历正文做 8 维可取证诊断。

    分工：维度分数与证据由 ``services.resume_scoring`` 的**纯规则**算出
    （同输入同输出，可复现）；LLM 只负责基于这些结构化事实写文字诊断。

    降级策略（三层，顺序不能变）：
    1. 岗位库不可用 ⇒ 只影响「技能匹配度 / 岗位相关性」两维，**不让整个
       接口 500**；
    2. LLM 不可用 ⇒ ``_fallback_diagnosis()`` 直接用规则引擎的维度分生成
       文案，**不编造内容**；
    3. ``ai_error`` 结构化记录失败类型（``scope.error`` 优先，其次是旧式
       哨兵串），供前端/运维判断。
    """
    # 岗位库仅用于「技能匹配度 / 岗位相关性」两维；数据库不可用时应降级为
    # 这两维计 0 分并说明原因，而不是让整个简历分析接口 500。
    try:
        jobs = await get_all_jobs()
    except Exception:
        jobs = []

    result = evaluate_resume(text, jobs)

    metrics = result["metrics"]
    skills = result["skills"]
    score = result["score"]

    prompt = RESUME_ANALYSIS_PROMPT.format(summary=build_prompt_summary(result))

    def _fallback_diagnosis() -> str:
        """LLM 不可用时的降级文案：直接由规则引擎的维度分生成，不编造内容。"""
        ranked = sorted(metrics, key=lambda m: (-m["score"], m["key"]))
        best = ranked[0]
        weakest = "；".join(f"{m['name']}（{m['score']} 分）" for m in ranked[-2:])
        return (
            f"【核心优势】：{best['name']} 得分最高（{best['score']} 分）。\n"
            f"【提升建议】：优先补强 {weakest}。"
            "（AI 文字诊断暂不可用，以上结论直接取自规则引擎的维度评分与原文证据。）"
        )

    ai_available = True
    ai_error = None

    # ★ 本次调用的 Prompt 版本**显式声明**：日志与指标里能直接查到
    #   "线上跑的是哪版模板"，不必再去翻代码。作用域同时把结构化失败原因
    #   （超时 / 鉴权 / 限流 / 不可用 / 空响应）写回 scope.error。
    with ai_call_scope("resume/analysis@v1") as scope:
        try:
            chat_answer = await llm.chat_async(prompt)
        except Exception:
            chat_answer = ""

    # 失败判定分两层：
    #   ① 结构化原因（scope.error）—— 新链路，能区分故障类型，供前端/运维判断；
    #   ② 旧式哨兵串 —— 兼容既有口径（spark_api 被替换成普通协程时只剩这一层）。
    if scope.error is not None:
        ai_available = False
        ai_error = scope.error_dict()

    raw_answer = chat_answer or ""
    if (
        len(raw_answer.strip()) < 10
        or raw_answer.startswith(("API调用失败", "SparkAPI not available", "AI 回答为空"))
    ):
        ai_available = False
        chat_answer = _fallback_diagnosis()
        if ai_error is None:
            ai_error = {
                "kind": "empty" if not raw_answer.strip() else "legacy_sentinel",
                "retryable": False,
                "provider": scope.provider,
                "request_id": scope.request_id,
                "message": "模型输出不可用，已降级为规则引擎诊断",
            }

    try:
        results = await filter_jobs_by_keywords(skills, min_match_rate=30) if skills else []
        recommended_jobs = [
            {
                "job_id": j["job_id"],
                "job_name": j["job_name"],
                "city": j["city"],
                "salary": j["salary"],
                "keyword_match": j["keyword_match"],
            }
            for j in results[:5]
        ]
    except Exception:
        recommended_jobs = []

    return {
        "chat_answer": chat_answer,
        "ai_available": ai_available,
        # 新增（**向后兼容的附加字段**）：失败时给出结构化原因
        # {kind, retryable, provider, request_id, message}；成功时为 None。
        # 旧客户端不读这个键即可，行为与重构前完全一致。
        "ai_error": ai_error,
        "skills": skills,
        "score": score,
        "metrics": metrics,
        "reference_job": result["reference_job"],
        "sections": result["sections"],
        "recommended_jobs": recommended_jobs,
    }


# ============================================================
# 用例：简历文本结构化解析
# ============================================================
async def parse_resume_text(resume_text: str, *, llm: Any) -> Dict[str, Any]:
    """从简历正文里抽出姓名/专业/技能/学历/摘要/评分。

    ★ 返回值的中文键（``"姓名"`` / ``"核心竞争力评分"`` …）是**前端契约**，
    不是随手起的名字——``App.vue`` 直接按这些键渲染。改成英文键会让页面
    静默变空。

    失败时返回 ``{"error": str(e)}``（**不抛异常**）：前端按「有没有 error
    键」判断，这是迁移前就有的行为。
    """
    prompt = f"""从以下简历文本提取信息，直接返回JSON格式，不要其他内容。

示例格式: {{"name":"张三","major":"计算机科学","skills":["Python","Flask","MySQL"],"education":"本科","summary":"...","score":85}}

【重要】技能点必须保留原始技术名词，不得翻译或本地化。例如：
- Python 不能写成"派森"或"python"
- Flask 不能写成"烧瓶"或"flask"
- MySQL 不能写成"mysql"或"数据库"
- 必须使用标准的英文技术名称

简历内容:
{resume_text}"""

    try:
        result = await llm.chat_async(prompt)
        match = re.search(r"\{[\s\S]+\}", result)
        if match:
            text = match.group().replace("```json", "").replace("```", "")
            parsed = json.loads(text)
            return {
                "姓名": parsed.get("name"),
                "专业": parsed.get("major"),
                "技能": parsed.get("skills", []),
                "学历": parsed.get("education"),
                "摘要": parsed.get("summary"),
                "核心竞争力评分": parsed.get("score"),
            }
        return {"raw_response": result}
    except Exception as e:
        return {"error": str(e)}
