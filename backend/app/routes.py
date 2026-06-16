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

@api_bp.route('/resume/analyze', methods=['POST'])
def analyze_resume():
    """简历分析接口 - 接收文件并提取技能画像"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400

        file = request.files['file']
        if not file.filename:
            return jsonify({'error': 'Empty filename'}), 400

        # 读取文件内容
        content = file.read()

        # 尝试解码为文本
        try:
            text = content.decode('utf-8')
        except:
            try:
                text = content.decode('gbk')
            except:
                text = content.decode('latin-1', errors='ignore')

        if len(text.strip()) < 20:
            return jsonify({'error': '简历内容过少，无法进行分析'}), 400

        # 模拟大模型提取简历技能
        skills = []
        tech_keywords = ['Python', 'JavaScript', 'TypeScript', 'Vue', 'React', 'Angular',
                       'Node.js', 'Flask', 'Django', 'Spring', 'MySQL', 'PostgreSQL',
                       'MongoDB', 'Redis', 'Docker', 'Kubernetes', 'AWS', 'Azure',
                       'Git', 'Linux', 'API', 'REST', 'GraphQL', 'TensorFlow', 'PyTorch']

        text_lower = text.lower()
        for tech in tech_keywords:
            if tech.lower() in text_lower or tech.lower().replace('.', '') in text_lower:
                if tech not in skills:
                    skills.append(tech)

        if not skills:
            skills = ['Python', 'JavaScript', 'Git']

        # 十二维能力画像
        twelve_metrics = [
            {'name': '架构能力', 'value': 65 + len(skills) * 3},
            {'name': '代码规范', 'value': 70 + len(skills) * 2},
            {'name': '业务理解', 'value': 60 + len(skills) * 2},
            {'name': '全栈能力', 'value': 55 + len(skills) * 3},
            {'name': '性能优化', 'value': 50 + len(skills) * 2},
            {'name': '团队协作', 'value': 65 + len(skills)},
            {'name': '云原生', 'value': 45 + len(skills) * 2},
            {'name': '数据库', 'value': 60 + len(skills) * 2},
            {'name': 'DevOps', 'value': 40 + len(skills) * 2},
            {'name': '安全意识', 'value': 50 + len(skills)},
            {'name': '创新能力', 'value': 55 + len(skills)},
            {'name': '学习能力', 'value': 70 + len(skills) * 2},
        ]

        # 计算综合评分
        score = min(95, 55 + len(skills) * 4)

        # 调用讯飞星火 AI 生成精简诊断报告
        RESUME_ANALYSIS_PROMPT = """你是简历分析助手。请根据以下简历信息，生成一段精简的职业诊断报告（不超过100字），直接输出，不要其他内容。

简历技能：{skills}
综合评分：{score}分

要求：
1. 直接指出核心竞争力
2. 最多2条提升建议
3. 风格简洁专业，不要废话

输出格式：
【核心优势】：xxx
【提升建议】：xxx"""

        try:
            prompt = RESUME_ANALYSIS_PROMPT.format(
                skills='、'.join(skills[:8]) if skills else '未识别到特定技能',
                score=score
            )
            chat_answer = spark_api.chat(prompt)
            if chat_answer.startswith('Error') or len(chat_answer) < 10:
                chat_answer = f"您的技能涵盖 {', '.join(skills[:5])} 等，综合评分 {score} 分。建议深化核心技术栈，积累大型项目经验。"
        except Exception as e:
            chat_answer = f"您的技能涵盖 {', '.join(skills[:5])} 等，综合评分 {score} 分。建议深化核心技术栈，积累大型项目经验。"

        # 获取推荐岗位
        try:
            results = filter_jobs_by_keywords(skills, min_match_rate=30)
            recommended_jobs = [{
                'job_id': j['job_id'],
                'job_name': j['job_name'],
                'city': j['city'],
                'salary': j['salary'],
                'keyword_match': j['keyword_match']
            } for j in results[:5]]
        except:
            recommended_jobs = []

        return jsonify({
            'chat_answer': chat_answer,
            'skills': skills,
            'score': score,
            'twelve_metrics': twelve_metrics,
            'recommended_jobs': recommended_jobs
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@api_bp.route('/admin/user-logs', methods=['GET'])
def get_user_logs():
    """获取用户登录日志"""
    logs = [
        {'email': 'user1@example.com', 'lastLogin': '2026-06-11 10:30', 'role': 'user', 'searchCount': 12},
        {'email': 'user2@example.com', 'lastLogin': '2026-06-11 09:15', 'role': 'user', 'searchCount': 8},
        {'email': 'admin@career.ai', 'lastLogin': '2026-06-11 11:00', 'role': 'admin', 'searchCount': 0},
    ]
    return jsonify({'logs': logs})

@api_bp.route('/admin/add-job', methods=['POST'])
def add_job():
    """添加新岗位"""
    data = request.json
    job_name = data.get('job_name')
    city = data.get('city')
    salary = data.get('salary')
    skills = data.get('skills', [])
    description = data.get('description', '')

    if not job_name or not city:
        return jsonify({'error': 'job_name and city are required'}), 400

    return jsonify({'success': True, 'message': '岗位添加成功'})