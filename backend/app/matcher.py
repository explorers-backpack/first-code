import sqlite3
import os
import json
import re
from .spark import SparkAPI

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'career.db')

def get_job_requirements(job_id):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM jobs WHERE id = ?', (job_id,))
    job = cursor.fetchone()
    conn.close()
    return dict(job) if job else None

def get_all_jobs():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM jobs')
    jobs = cursor.fetchall()
    conn.close()
    return [dict(job) for job in jobs]

def keyword_match(user_skills, job_skills_str):
    if not job_skills_str or not user_skills:
        return 0
    
    job_skills = [s.strip().lower() for s in job_skills_str.split(',')]
    user_skills_lower = [s.strip().lower() for s in user_skills]
    
    matched = []
    missing = []
    
    for job_skill in job_skills:
        found = False
        for user_skill in user_skills_lower:
            if job_skill in user_skill or user_skill in job_skill:
                matched.append(job_skill)
                found = True
                break
        if not found:
            missing.append(job_skill)
    
    if not job_skills:
        return 0
    
    match_rate = len(matched) / len(job_skills) * 100
    return {
        'match_rate': round(match_rate, 1),
        'matched': matched,
        'missing': missing,
        'total_required': len(job_skills),
        'matched_count': len(matched)
    }

def filter_jobs_by_keywords(user_skills, min_match_rate=30):
    jobs = get_all_jobs()
    results = []
    
    for job in jobs:
        result = keyword_match(user_skills, job.get('skills', ''))
        if result['match_rate'] >= min_match_rate:
            results.append({
                'job_id': job['id'],
                'job_name': job['job_name'],
                'city': job['city'],
                'salary': job['salary'],
                'keyword_match': result
            })
    
    results.sort(key=lambda x: x['keyword_match']['match_rate'], reverse=True)
    return results

def analyze_with_ai(user_profile, job):
    spark = SparkAPI()
    
    user_skills_str = ', '.join(user_profile.get('skills', []))
    user_experience = user_profile.get('experience', '')
    user_education = user_profile.get('education', '')
    
    prompt = f"""你是一位职业发展顾问。请分析求职者和岗位的契合度并给出建议。

## 求职者信息
- 技能: {user_skills_str}
- 经验: {user_experience}
- 学历: {user_education}

## 岗位信息
- 岗位名称: {job['job_name']}
- 薪资: {job['salary']}
- 学历要求: {job['edu_require']}
- 专业要求: {job['major_require']}
- 技能要求: {job['skills']}
- 岗位职责: {job['duty']}
- 工作城市: {job['city']}
- 行业: {job['industry']}

【重要】技能点必须保留原始技术名词，不得翻译或本地化。
例如：Python 不能写成"派森"，Flask 不能写成"烧瓶"，MySQL 必须保持大写形式。

请以JSON格式返回分析结果，格式如下：
{{
    "match_percentage": 数字(0-100),
    "analysis": "简要分析(50字内)",
    "strengths": ["优势1", "优势2"],
    "weaknesses": ["不足1", "不足2"],
    "suggestions": ["建议1", "建议2"]
}}"""

    result = spark.chat(prompt)
    
    try:
        json_match = re.search(r'\{[\s\S]*\}', result)
        if json_match:
            return json.loads(json_match.group())
    except:
        pass
    
    return {
        'match_percentage': 50,
        'analysis': '根据基本信息分析',
        'strengths': ['相关技能'],
        'weaknesses': ['需要提升'],
        'suggestions': ['继续学习']
    }

def get_detailed_match(user_profile, job_id):
    job = get_job_requirements(job_id)
    if not job:
        return {'error': 'Job not found'}
    
    keyword_result = keyword_match(user_profile.get('skills', []), job.get('skills', ''))
    
    ai_result = analyze_with_ai(user_profile, job)
    
    final_match = (keyword_result['match_rate'] * 0.4 + ai_result.get('match_percentage', 50) * 0.6)
    
    return {
        'job': job,
        'keyword_match': keyword_result,
        'ai_analysis': ai_result,
        'final_match_percentage': round(final_match, 1),
        'suggestions': ai_result.get('suggestions', [])
    }