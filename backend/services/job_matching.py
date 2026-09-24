# -*- coding: utf-8 -*-
"""backend.services.job_matching —— 岗位技能匹配（纯规则、确定性）。

背景
----
``main.py`` 中曾存在**两个同名同实现的** ``keyword_match`` 定义
（逐字节相同，后者覆盖前者，属历史遗留的重复定义）。为消除重复并统一入口，
岗位匹配逻辑统一收敛到本模块；``main.py`` 仅保留调用方，不再保留任何同名定义。

设计约定
--------
- 纯函数：不依赖 FastAPI、数据库、LLM，给定相同输入恒返回相同结果。
- 返回结构（与历史实现**完全一致**，前端 ``job.keyword_match.*`` 与
  ``main.clean_jobs_to_text`` 均依赖该契约）::

      {
          "match_rate":     float,      # 命中率百分比，保留 1 位小数
          "matched":        List[str],  # 命中的岗位技能（小写、已去空格）
          "missing":        List[str],  # 未命中的岗位技能（小写、已去空格）
          "total_required": int,        # 岗位技能总数
          "matched_count":  int,        # 命中数量
      }

兼容性说明（均为历史行为，本次刻意保留，不做行为变更）
--------------------------------------------------
1. ``matched`` / ``missing`` 中的技能一律为**小写**——因为岗位技能串在拆分时
   已 ``.lower()``。前端与 ``clean_jobs_to_text`` 展示的即该小写形式。
2. 匹配规则为**双向子串包含**（``job_skill in user_skill`` 或
   ``user_skill in job_skill``），因此 ``java`` 与 ``javascript`` 会互相命中。
3. 空串 / ``None`` / 空列表走「零值早返回」；而 ``" , "`` 这类仅由分隔符与
   空白构成的串会拆出 ``["", ""]``，空串可被任意技能包含，历史上会得到
   100% 命中——此为既有行为，本次不改（详见 ``test_job_matching.py``）。
"""

from typing import Dict, List, Optional

__all__ = ["split_job_skills", "keyword_match", "match_job"]


def _empty_result() -> Dict:
    """零匹配结果。每次返回新对象，避免可变默认值被多次调用共享。"""
    return {
        "match_rate": 0,
        "matched": [],
        "missing": [],
        "total_required": 0,
        "matched_count": 0,
    }


def split_job_skills(job_skills_str: Optional[str]) -> List[str]:
    """把岗位技能串拆成「去空格 + 小写」的技能列表。

    与历史实现保持一致：按 ``,`` 切分后逐项 ``strip().lower()``，
    **不过滤空串**（例如 ``"python,,"`` -> ``["python", "", ""]``），
    以保证与旧 ``keyword_match`` 的行为逐字节对齐。

    :param job_skills_str: 岗位技能串，如 ``"Python, MySQL"``；假值返回 ``[]``
    """
    if not job_skills_str:
        return []
    return [s.strip().lower() for s in job_skills_str.split(",")]


def keyword_match(user_skills: List[str], job_skills_str: str) -> Dict:
    """计算用户技能对岗位技能要求的命中情况。

    本函数即原 ``main.py`` 中两份重复定义**合并后的唯一实现**：两者当时
    逐字节相同，故此处保持原逻辑与返回契约不变。

    :param user_skills: 用户技能列表（可含任意大小写与前后空格）
    :param job_skills_str: 岗位技能串，逗号分隔，如 ``"Python, MySQL"``
    :return: 见模块文档所述的 5 键 dict
    """
    # 空值早返回：无岗位技能 或 无用户技能，均视为零匹配
    if not job_skills_str or not user_skills:
        return _empty_result()

    job_skills = split_job_skills(job_skills_str)
    user_skills_lower = [s.strip().lower() for s in user_skills]

    matched: List[str] = []
    missing: List[str] = []
    for job_skill in job_skills:
        # 双向子串匹配：任一方向包含即视为命中
        found = any(job_skill in us or us in job_skill for us in user_skills_lower)
        if found:
            matched.append(job_skill)
        else:
            missing.append(job_skill)

    # 防御性分支：正常入参下 job_skills 恒非空（假值已在前面挡掉），
    # 保留以与历史实现的结构保持一致。
    if not job_skills:
        return _empty_result()

    match_rate = len(matched) / len(job_skills) * 100
    return {
        "match_rate": round(match_rate, 1),
        "matched": matched,
        "missing": missing,
        "total_required": len(job_skills),
        "matched_count": len(matched),
    }


def match_job(user_skills: List[str], job: dict) -> Dict:
    """对单个岗位 dict 做技能匹配，返回「岗位摘要 + 匹配结果」。

    该结构即 ``main.py: filter_jobs_by_keywords`` 内联拼装的结构，
    抽出为独立函数以便复用与单测（不改变其字段含义）。

    :param user_skills: 用户技能列表
    :param job: 岗位 dict，至少含 ``skills``；建议含
        ``id`` / ``job_name`` / ``city`` / ``salary``
    :return::

        {
            "job_id":   job.get("id"),
            "job_name": job.get("job_name"),
            "city":     job.get("city"),
            "salary":   job.get("salary"),
            "keyword_match": {<keyword_match 的 5 键结果>},
        }
    """
    job = job or {}
    return {
        "job_id": job.get("id"),
        "job_name": job.get("job_name"),
        "city": job.get("city"),
        "salary": job.get("salary"),
        # 岗位未标注 skills 时传 ""，keyword_match 会走零值早返回
        "keyword_match": keyword_match(user_skills, job.get("skills", "")),
    }
