from flask import Blueprint, request, jsonify
from .spark import SparkAPI
from .matcher import filter_jobs_by_keywords, get_all_jobs
from .resume_parser import parse_resume

api_bp = Blueprint('api', __name__)

spark_api = SparkAPI()

def clean_jobs_to_text(jobs):
    if not jobs:
        return "暂无匹配岗位"
    
    lines = []
    for i, job in enumerate(jobs, 1):
        match_rate = job.get('keyword_match', {}).get('match_rate', 0)
        matched = job.get('keyword_match', {}).get('matched', [])
        skills = job.get('skills', '未标注')
        
        line = f"""【岗位{i}】{job.get('job_name', '未知')} | 薪资：{job.get('salary', '未标注')} | 城市：{job.get('city', '未标注')} | 技能要求：{skills} | 当前匹配度：{match_rate}% | 用户已有技能：{','.join(matched) if matched else '暂无'}"""
        lines.append(line)
    
    return "\n".join(lines)

RAG_PROMPT = """你是一个资深职业规划顾问。请直接基于以下真实岗位数据进行分析，直接告诉用户适配理由和提升建议。

【用户提供技能】: {user_skills}

【岗位数据】
{jobs_text}

【强制指令】
1. 严禁说"根据您的技能xxx"这类废话！
2. 必须直接说：岗位X适配原因是什么、差距在哪、需要学什么
3. 禁止生成任何虚假岗位名
4. 每个岗位格式：先说明适配度，再分析差距，最后给具体学习建议
5. 如果用户的技能描述与数据库中的岗位完全不相关，请礼貌地提示用户提供更具体的职业信息，不要强行匹配或虚构建议。
6. 【重要】技能点必须保留原始技术名词，不得翻译或本地化。例如：Python 不能写成"派森"，Flask 不能写成"烧瓶"，MySQL 必须保持大写形式。"""

EMPTY_PROMPT = """你是一个职业规划顾问。用户技能：{user_skills}，未找到精准匹配岗位。

请直接给出通用职业规划建议，包括：推荐方向、入门技能、学习路径。

【重要限制】
1. 如果用户的技能描述与数据库中的岗位完全不相关，请礼貌地提示用户提供更具体的职业信息，不要强行匹配或虚构建议。
2. 【重要】技能点必须保留原始技术名词，不得翻译或本地化。例如：Python 不能写成"派森"，Flask 不能写成"烧瓶"，MySQL 必须保持大写形式。"""

@api_bp.route('/chat', methods=['POST'])
def chat():
    try:
        data = request.json
        message = data.get('message', '')
        front_jobs = data.get('jobs', [])
        
        if not message:
            return jsonify({'error': 'Message is required'}), 400
        
        user_skills = message.strip()
        
        if not user_skills or len(user_skills) < 5:
            return jsonify({
                'chat_answer': '由于您提供的信息（专业、技能、学历）不足，我目前无法为您量身定制职业规划。为了得到精准建议，请您详细描述您的背景（例如：熟悉 Python 开发的应届生）。',
                'top_jobs': [],
                'source': 'invalid_input'
            })
        
        if front_jobs and len(front_jobs) > 0:
            jobs_text = clean_jobs_to_text(front_jobs)
            prompt = RAG_PROMPT.format(user_skills=user_skills, jobs_text=jobs_text)
            jobs_data = front_jobs
            source_type = 'database'
        else:
            skills = [s.strip() for s in message.split(',') if s.strip()]
            results = filter_jobs_by_keywords(skills, min_match_rate=20)
            db_jobs = results[:5] if results else []
            
            if db_jobs:
                jobs_text = clean_jobs_to_text(db_jobs)
                prompt = RAG_PROMPT.format(user_skills=user_skills, jobs_text=jobs_text)
                
                jobs_data = [{
                    'job_id': j['job_id'],
                    'job_name': j['job_name'],
                    'city': j['city'],
                    'salary': j['salary'],
                    'keyword_match': j['keyword_match']
                } for j in db_jobs]
                source_type = 'database'
            else:
                prompt = EMPTY_PROMPT.format(user_skills=user_skills)
                jobs_data = []
                source_type = 'ai'
        
        try:
            ai_response = spark_api.chat(prompt)
        except Exception as e:
            ai_response = "服务暂时不可用，请稍后重试。"
        
        if ai_response.startswith('Error'):
            ai_response = "抱歉，暂时无法获取分析结果。"
        
        return jsonify({
            'chat_answer': ai_response,
            'top_jobs': jobs_data,
            'source': source_type
        })
            
    except Exception as e:
        return jsonify({
            'chat_answer': f"服务错误，请稍后重试。",
            'top_jobs': [],
            'source': 'error'
        }), 500

@api_bp.route('/jobs', methods=['GET'])
def get_jobs():
    jobs = get_all_jobs()
    return jsonify({'jobs': jobs})

@api_bp.route('/match', methods=['POST'])
def match_jobs():
    data = request.json
    user_skills = data.get('skills', [])
    min_match = data.get('min_match_rate', 30)
    
    if not user_skills:
        return jsonify({'error': 'skills is required'}), 400
    
    results = filter_jobs_by_keywords(user_skills, min_match_rate=min_match)
    return jsonify({'results': results})

@api_bp.route('/resume/parse', methods=['POST'])
def parse_resume_api():
    data = request.json
    resume_text = data.get('resume_text', '')
    
    if not resume_text:
        return jsonify({'error': 'resume_text is required'}), 400
    
    result = parse_resume(resume_text)
    return jsonify(result)

@api_bp.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})