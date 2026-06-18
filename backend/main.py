"""
Career.ai FastAPI 后端
全面支持异步编程 + Pydantic 数据校验 + 异步 SQLAlchemy 持久化
"""

import os
import re
import json
import asyncio
import hashlib
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from contextlib import asynccontextmanager
from typing import Optional, List

from fastapi import FastAPI, HTTPException, UploadFile, File, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import aiosqlite
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, Integer, String, Text, JSON, DateTime, func, select, text

# ============================================================
# 数据库配置（MySQL）
# ============================================================
DATABASE_URL = "mysql+aiomysql://root:2549966637@localhost:3306/career?charset=utf8mb4"

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


# ============================================================
# SQLAlchemy ORM 模型（异步）
# ============================================================
class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(80), unique=True, nullable=False)
    email = Column(String(120), unique=True, nullable=False)
    password_hash = Column(String(200), nullable=False)
    role = Column(String(20), default="user")
    created_at = Column(DateTime, default=datetime.utcnow)


class Resume(Base):
    __tablename__ = "resume"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    filename = Column(String(200))
    content = Column(Text)
    parsed_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)


class ChatHistory(Base):
    __tablename__ = "chat_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    role = Column(String(20), nullable=False)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class UserSession(Base):
    __tablename__ = "user_session"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    token = Column(String(64), unique=True, nullable=False, index=True)
    email = Column(String(120), nullable=False)
    role = Column(String(20), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class UserLoginLog(Base):
    __tablename__ = "user_login_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(120), nullable=False)
    role = Column(String(20), nullable=False)
    last_login = Column(String(32), nullable=False)
    search_count = Column(Integer, default=0)


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_name = Column(String(200), nullable=False)
    salary = Column(String(50))
    edu_require = Column(String(50))
    major_require = Column(String(200))
    skills = Column(Text)
    duty = Column(Text)
    city = Column(String(50))
    industry = Column(String(50))


# ============================================================
# 数据库会话依赖
# ============================================================
async def get_db():
    async with async_session() as session:
        yield session


# ============================================================
# SparkAI X1 WebSocket（认证已修复）
# ============================================================
import hashlib
import hmac
import base64
import datetime
from urllib.parse import urlencode

try:
    import websocket as _ws
    _spark_ok = True
except ImportError:
    _spark_ok = False


def _make_url(host, path, api_key, api_secret):
    date_str = datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    sig_str = f"host: {host}\ndate: {date_str}\nGET {path} HTTP/1.1"
    sig = hmac.new(api_secret.encode(), sig_str.encode(), hashlib.sha256).digest()
    sig_b64 = base64.b64encode(sig).decode()
    auth_orig = (f'api_key="{api_key}", algorithm="hmac-sha256", '
                 f'headers="host date request-line", signature="{sig_b64}"')
    auth = base64.b64encode(auth_orig.encode()).decode()
    params = {"authorization": auth, "date": date_str, "host": host}
    return f"wss://{host}{path}?{urlencode(params)}"


class SparkAPI:
    def __init__(self):
        self.app_id = os.getenv("SPARK_APP_ID", "")
        self.api_key = os.getenv("SPARK_API_KEY", "")
        self.api_secret = os.getenv("SPARK_API_SECRET", "")
        self.host = "spark-api.xf-yun.com"
        self.path = "/v1/x1"

    def chat(self, message: str) -> str:
        if not _spark_ok:
            return "SparkAPI not available"
        try:
            url = _make_url(self.host, self.path, self.api_key, self.api_secret)
            ws = _ws.create_connection(url, timeout=60)
            req = {
                "header": {"app_id": self.app_id, "uid": "user_001"},
                "parameter": {"chat": {"domain": "x1", "temperature": 0.5, "max_tokens": 2048}},
                "payload": {"message": {"text": [{"role": "user", "content": message}]}},
            }
            ws.send(json.dumps(req))
            text = ""
            while True:
                frame = json.loads(ws.recv())
                hdr = frame.get("header", {})
                if hdr.get("code", 0) != 0:
                    ws.close()
                    return f"API调用失败: {hdr.get('message', hdr.get('code'))}"
                choices = frame.get("payload", {}).get("choices", {}).get("text", [])
                if choices:
                    chunk = choices[0].get("content") or choices[0].get("reasoning_content") or ""
                    text += chunk
                if hdr.get("status") == 2:
                    break
            ws.close()
            return text if text else "AI 回答为空"
        except Exception as e:
            return f"API调用失败: {e}"

    async def chat_async(self, message: str) -> str:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.chat, message)


spark_api = SparkAPI()


# ============================================================
# 岗位匹配工具（异步 MySQL）
# ============================================================
async def get_all_jobs() -> List[dict]:
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


def keyword_match(user_skills: List[str], job_skills_str: str) -> dict:
    if not job_skills_str or not user_skills:
        return {
            "match_rate": 0,
            "matched": [],
            "missing": [],
            "total_required": 0,
            "matched_count": 0,
        }
    job_skills = [s.strip().lower() for s in job_skills_str.split(",")]
    user_skills_lower = [s.strip().lower() for s in user_skills]
    matched, missing = [], []
    for job_skill in job_skills:
        found = any(job_skill in us or us in job_skill for us in user_skills_lower)
        if found:
            matched.append(job_skill)
        else:
            missing.append(job_skill)
    if not job_skills:
        return {
            "match_rate": 0,
            "matched": [],
            "missing": [],
            "total_required": 0,
            "matched_count": 0,
        }
    match_rate = len(matched) / len(job_skills) * 100
    return {
        "match_rate": round(match_rate, 1),
        "matched": matched,
        "missing": missing,
        "total_required": len(job_skills),
        "matched_count": len(matched),
    }


def keyword_match(user_skills: List[str], job_skills_str: str) -> dict:
    if not job_skills_str or not user_skills:
        return {
            "match_rate": 0,
            "matched": [],
            "missing": [],
            "total_required": 0,
            "matched_count": 0,
        }
    job_skills = [s.strip().lower() for s in job_skills_str.split(",")]
    user_skills_lower = [s.strip().lower() for s in user_skills]
    matched, missing = [], []
    for job_skill in job_skills:
        found = any(job_skill in us or us in job_skill for us in user_skills_lower)
        if found:
            matched.append(job_skill)
        else:
            missing.append(job_skill)
    if not job_skills:
        return {
            "match_rate": 0,
            "matched": [],
            "missing": [],
            "total_required": 0,
            "matched_count": 0,
        }
    match_rate = len(matched) / len(job_skills) * 100
    return {
        "match_rate": round(match_rate, 1),
        "matched": matched,
        "missing": missing,
        "total_required": len(job_skills),
        "matched_count": len(matched),
    }


async def filter_jobs_by_keywords(user_skills: List[str], min_match_rate: int = 30):
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
# 密码与 Token 工具
# ============================================================
def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def _generate_token(user_id: int, email: str, role: str) -> str:
    return hashlib.md5(f"{user_id}_{email}_{time.time()}".encode()).hexdigest()


# ============================================================
# Pydantic Schemas
# ============================================================


class UserRegister(BaseModel):
    username: str = Field(..., min_length=2, max_length=80)
    email: str = Field(..., max_length=120)
    password: str = Field(..., min_length=6, max_length=128)


class UserLogin(BaseModel):
    email: str
    password: str


class JobCreate(BaseModel):
    job_name: str = Field(..., min_length=1, max_length=200)
    city: str = Field(..., min_length=1, max_length=50)
    salary: str = Field(default="面议", max_length=50)
    skills: List[str] = Field(default_factory=list)
    description: str = Field(default="")


class ChatRequest(BaseModel):
    message: str
    jobs: Optional[List[dict]] = None


class MatchRequest(BaseModel):
    skills: List[str]
    min_match_rate: int = 30


class ResumeParseRequest(BaseModel):
    resume_text: str


# ============================================================
# 依赖：当前用户（从数据库会话表查询）
# ============================================================
async def get_current_user(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="未登录")
    token = authorization[7:]

    result = await db.execute(select(UserSession).where(UserSession.token == token))
    session = result.scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=401, detail="登录已过期")

    return {
        "user_id": session.user_id,
        "email": session.email,
        "role": session.role,
    }


async def get_current_admin(
    current_user: dict = Depends(get_current_user),
) -> dict:
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return current_user


# ============================================================
# Lifespan（MySQL 下由 SQLAlchemy create_all 自动建表）
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


# ============================================================
# FastAPI App
# ============================================================
app = FastAPI(
    title="Career.ai API",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# 路由：认证模块
# ============================================================


@app.post("/api/auth/register", tags=["认证"])
async def register(data: UserRegister, db: AsyncSession = Depends(get_db)):
    """
    用户注册接口。同一 email 最多注册 5 个账号。
    """
    # 查询该 email 已注册数量
    result = await db.execute(
        select(func.count()).select_from(User).where(User.email == data.email)
    )
    email_count = result.scalar() or 0

    if email_count >= 5:
        raise HTTPException(
            status_code=400,
            detail="该邮箱注册账号数量已达上限（同一邮箱最多5个账号）",
        )

    # 检查重复
    result = await db.execute(
        select(User).where(User.username == data.username)
    )
    if result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="用户名已存在")

    result = await db.execute(
        select(User).where(User.email == data.email)
    )
    if result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="邮箱已被使用")

    # 写入数据库
    new_user = User(
        username=data.username,
        email=data.email,
        password_hash=_hash_password(data.password),
        role="user",
    )
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)

    # 创建会话
    token = _generate_token(new_user.id, new_user.email, new_user.role)
    session = UserSession(
        user_id=new_user.id,
        token=token,
        email=new_user.email,
        role=new_user.role,
    )
    db.add(session)
    await db.commit()

    return {
        "success": True,
        "message": "注册成功",
        "user": {
            "id": new_user.id,
            "username": new_user.username,
            "email": new_user.email,
            "role": new_user.role,
        },
        "token": token,
    }


@app.post("/api/auth/login", tags=["认证"])
async def login(data: UserLogin, db: AsyncSession = Depends(get_db)):
    """
    用户登录接口。支持 email 或 username 登录。
    """
    result = await db.execute(select(User).where(User.email == data.email))
    user = result.scalar_one_or_none()

    if not user:
        result = await db.execute(select(User).where(User.username == data.email))
        user = result.scalar_one_or_none()

    # 未注册则创建（兼容模式）
    if not user:
        new_user = User(
            username=data.email.split("@")[0],
            email=data.email,
            password_hash=_hash_password(data.password),
            role="user",
        )
        db.add(new_user)
        await db.commit()
        await db.refresh(new_user)
        user = new_user

    if user.password_hash != _hash_password(data.password):
        raise HTTPException(status_code=401, detail="密码错误")

    # 记录登录日志
    log = UserLoginLog(
        email=user.email,
        role=user.role,
        last_login=datetime.now().strftime("%Y-%m-%d %H:%M"),
        search_count=0,
    )
    db.add(log)

    # 创建会话 token
    token = _generate_token(user.id, user.email, user.role)
    session = UserSession(
        user_id=user.id,
        token=token,
        email=user.email,
        role=user.role,
    )
    db.add(session)
    await db.commit()

    return {
        "success": True,
        "message": "登录成功",
        "token": token,
        "user": {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "role": user.role,
        },
    }


# ============================================================
# 路由：AI 看板模块
# ============================================================

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


@app.post("/api/chat", tags=["AI 看板"])
async def chat(data: ChatRequest):
    """职业规划对话接口"""
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
        ai_response = await spark_api.chat_async(prompt)
    except Exception:
        ai_response = "抱歉，暂时无法获取分析结果。"

    if ai_response.startswith("Error"):
        ai_response = "抱歉，暂时无法获取分析结果。"

    return {
        "chat_answer": ai_response,
        "top_jobs": jobs_data,
        "source": source_type,
    }


@app.get("/api/jobs", tags=["岗位"])
async def get_jobs():
    """获取所有岗位列表"""
    return {"jobs": await get_all_jobs()}


@app.post("/api/match", tags=["岗位"])
async def match_jobs(data: MatchRequest):
    """技能匹配岗位接口"""
    return {"results": await filter_jobs_by_keywords(data.skills, data.min_match_rate)}


# ============================================================
# 路由：简历分析
# ============================================================
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


@app.post("/api/analyze-resume", tags=["AI 看板"])
async def analyze_resume(file: UploadFile = File(...)):
    """
    简历分析接口 - 接收简历文件，进行十二维诊断。
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="文件名为空")

    content = await file.read()

    try:
        text = content.decode("utf-8")
    except Exception:
        try:
            text = content.decode("gbk")
        except Exception:
            text = content.decode("latin-1", errors="ignore")

    if len(text.strip()) < 20:
        raise HTTPException(status_code=400, detail="简历内容过少，无法进行分析")

    tech_keywords = [
        "Python", "JavaScript", "TypeScript", "Vue", "React", "Angular",
        "Node.js", "Flask", "Django", "FastAPI", "Spring", "MySQL",
        "PostgreSQL", "MongoDB", "Redis", "Docker", "Kubernetes",
        "AWS", "Azure", "Git", "Linux", "API", "REST", "GraphQL",
        "TensorFlow", "PyTorch", "Java", "Golang", "Rust", "C++",
        "HTML", "CSS", "Scss", "jQuery", "Webpack", "Vite",
    ]

    text_lower = text.lower()
    skills = [
        tech for tech in tech_keywords
        if tech.lower() in text_lower or tech.lower().replace(".", "") in text_lower
    ]
    if not skills:
        skills = ["Python", "JavaScript", "Git"]

    twelve_metrics = [
        {"name": "架构能力",   "value": min(95, 65 + len(skills) * 3)},
        {"name": "代码规范",   "value": min(95, 70 + len(skills) * 2)},
        {"name": "业务理解",   "value": min(95, 60 + len(skills) * 2)},
        {"name": "全栈能力",   "value": min(95, 55 + len(skills) * 3)},
        {"name": "性能优化",   "value": min(95, 50 + len(skills) * 2)},
        {"name": "团队协作",   "value": min(95, 65 + len(skills))},
        {"name": "云原生",     "value": min(95, 45 + len(skills) * 2)},
        {"name": "数据库",     "value": min(95, 60 + len(skills) * 2)},
        {"name": "DevOps",     "value": min(95, 40 + len(skills) * 2)},
        {"name": "安全意识",   "value": min(95, 50 + len(skills))},
        {"name": "创新能力",   "value": min(95, 55 + len(skills))},
        {"name": "学习能力",   "value": min(95, 70 + len(skills) * 2)},
    ]

    score = min(95, 55 + len(skills) * 4)

    prompt = RESUME_ANALYSIS_PROMPT.format(
        skills="、".join(skills[:8]) if skills else "未识别到特定技能",
        score=score,
    )

    try:
        chat_answer = await spark_api.chat_async(prompt)
        if chat_answer.startswith("Error") or len(chat_answer) < 10:
            chat_answer = f"您的技能涵盖 {', '.join(skills[:5])} 等，综合评分 {score} 分。建议深化核心技术栈，积累大型项目经验。"
    except Exception:
        chat_answer = f"您的技能涵盖 {', '.join(skills[:5])} 等，综合评分 {score} 分。建议深化核心技术栈，积累大型项目经验。"

    try:
        results = await filter_jobs_by_keywords(skills, min_match_rate=30)
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
        "skills": skills,
        "score": score,
        "twelve_metrics": twelve_metrics,
        "recommended_jobs": recommended_jobs,
    }


@app.post("/api/resume/parse", tags=["简历解析"])
async def parse_resume(data: ResumeParseRequest):
    """简历文本解析接口"""
    resume_text = data.resume_text
    if not resume_text or not resume_text.strip():
        raise HTTPException(status_code=400, detail="简历文本为空")

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
        result = await spark_api.chat_async(prompt)
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


# ============================================================
# 路由：管理员模块
# ============================================================


@app.get("/api/admin/user-logs", tags=["管理员"])
async def get_user_logs(
    current_admin: dict = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """获取普通用户登录历史日志（仅 admin 可访问）。"""
    result = await db.execute(
        select(UserLoginLog).order_by(UserLoginLog.id.desc()).limit(100)
    )
    logs = result.scalars().all()

    return {
        "logs": [
            {
                "email": log.email,
                "lastLogin": log.last_login,
                "role": log.role,
                "searchCount": log.search_count,
            }
            for log in logs
        ]
    }


@app.post("/api/admin/add-job", tags=["管理员"])
async def add_job(
    data: JobCreate,
    current_admin: dict = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """添加新岗位（仅 admin 可访问）。"""
    new_job = Job(
        job_name=data.job_name,
        salary=data.salary,
        edu_require="本科",
        major_require="不限",
        skills=",".join(data.skills),
        duty=data.description,
        city=data.city,
        industry="互联网",
    )
    db.add(new_job)
    await db.commit()
    await db.refresh(new_job)

    return {
        "success": True,
        "message": "岗位添加成功",
        "job": {
            "job_id": new_job.id,
            "job_name": new_job.job_name,
            "city": new_job.city,
            "salary": new_job.salary,
            "skills": new_job.skills,
            "description": new_job.duty,
        },
    }


# ============================================================
# 健康检查
# ============================================================


@app.get("/api/health", tags=["系统"])
async def health():
    return {"status": "ok", "service": "Career.ai FastAPI"}


# ============================================================
# 启动命令
# ============================================================
# cd backend
# pip install -r requirements.txt
# python -m uvicorn main:app --reload --port 5000 --host 0.0.0.0
