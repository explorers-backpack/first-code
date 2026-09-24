"""
Career.ai FastAPI 后端
全面支持异步编程 + Pydantic 数据校验 + 异步 SQLAlchemy 持久化
"""

import os
import re
import json
import sys
import asyncio
import hashlib
import hmac
import base64
import time
from datetime import datetime
from urllib.parse import urlencode
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from contextlib import asynccontextmanager
from typing import Optional, List

from fastapi import FastAPI, HTTPException, UploadFile, File, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, select

# 保证以任意工作目录启动都能导入本项目的顶层模块
# （database / deps / models / services / api）
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ---- 基础设施：Base / engine / 会话工厂 / get_db ----
# 原定义在本文件，为支持 models / services / api 分层而抽取到 database.py，
# 避免分层模块反向 import main 造成循环依赖。
# DATABASE_URL 在本文件已无直接引用，保留导入是为了不破坏既有的
# `main.DATABASE_URL` 引用点（与抽取前的模块命名空间保持一致）。
from database import (  # noqa: E402,F401
    DATABASE_URL,
    Base,
    async_session,
    engine,
    get_db,
)

# ---- 鉴权依赖：原定义在本文件，同样为分层复用而抽取到 deps.py ----
from deps import get_current_admin, get_current_user  # noqa: E402

# ---- ORM 模型：已迁移至 models/ 包，此处导入以保持既有引用（User/Job/...）不变 ----
# Resume / ChatHistory 在本文件中无直接引用，保留是为了维持
# `main.Resume` / `main.ChatHistory` 的模块命名空间兼容；
# 表的注册由 models/__init__.py 负责，不依赖本文件是否 import。
from models import (  # noqa: E402,F401
    ChatHistory,
    Job,
    Resume,
    User,
    UserLoginLog,
    UserSession,
)

# ---- 业务服务 ----
from services.resume_scoring import build_prompt_summary, evaluate_resume  # noqa: E402

# ---- 岗位匹配：keyword_match 曾在本文件重复定义两份（逐字节相同），
# 现统一收敛为 services/job_matching.py 中的唯一实现，此处仅导入使用。
from services.job_matching import keyword_match  # noqa: E402

# ---- 路由：AI 模拟面试模块（独立分层，见 api/interview.py）----
from api.interview import router as interview_router  # noqa: E402


# ============================================================
# 数据库会话依赖
# ============================================================
# get_db 已随 engine / async_session 一并抽取到 database.py，
# 由顶部 `from database import ... get_db` 导入。
# 既有用法 `db: AsyncSession = Depends(get_db)` 保持不变。


# ============================================================
# SparkAI X1 WebSocket（认证已修复）
# ============================================================
# 注：此处曾重复 `import hashlib` / `import datetime`。后者把顶部
# `from datetime import datetime` 绑定的「类」重新绑定为「模块」，
# 导致运行期 `datetime.now()` 抛 AttributeError（登录接口恒 500）。
# 相关依赖已统一收敛到文件顶部，此处不再重复导入。
try:
    import websocket as _ws
    _spark_ok = True
except ImportError:
    _spark_ok = False


def _make_url(host, path, api_key, api_secret):
    date_str = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
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
            reasoning = ""
            while True:
                frame = json.loads(ws.recv())
                hdr = frame.get("header", {})
                if hdr.get("code", 0) != 0:
                    ws.close()
                    return f"API调用失败: {hdr.get('message', hdr.get('code'))}"
                choices = frame.get("payload", {}).get("choices", {}).get("text", [])
                for choice in choices:
                    # X1 为推理模型：content 是最终答案，reasoning_content 是内部思考过程。
                    # 两者必须分开累计，否则会把思考过程当结论一起返回给用户。
                    if choice.get("content"):
                        text += choice["content"]
                    elif choice.get("reasoning_content"):
                        reasoning += choice["reasoning_content"]
                if hdr.get("status") == 2:
                    break
            ws.close()
            # 仅当 content 全程为空时才退回 reasoning，保证不返回空答案
            answer = text.strip() or reasoning.strip()
            return answer if answer else "AI 回答为空"
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


# keyword_match 曾在此处**重复定义两次**（两份逐字节相同，后者覆盖前者）。
# 现合并为唯一实现并迁移至 services/job_matching.py，
# 本文件顶部已 `from services.job_matching import keyword_match`，
# 下方 filter_jobs_by_keywords 的调用点保持不变。


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
    """注册请求体。

    安全约定（**不要添加 role 字段**）：显式声明 ``extra="ignore"``，客户端即使传入
    ``{"role": "admin"}`` 也会被丢弃；role 由 ``register()`` 在服务端固定为 ``"user"``，
    任何情况下都不允许由请求体决定权限。
    """

    model_config = ConfigDict(extra="ignore")

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
# get_current_user / get_current_admin 已抽取到 deps.py，由顶部导入，
# 供 main.py 与 api/ 分层（如 api/interview.py）共用同一套鉴权口径。
# 既有用法 `Depends(get_current_admin)` 保持不变。


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

# ============================================================
# CORS 中间件 —— 必须在 app 创建后第一位注册（否则 500 报错时 CORS 头丢失）
# ============================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",   # Vite 默认端口
        "http://127.0.0.1:5173",   # 本地 IP 访问
    ],
    allow_credentials=True,
    allow_methods=["*"],           # GET / POST / PUT / DELETE / OPTIONS
    allow_headers=["*"],           # Content-Type / Authorization 等
)

# 兜底 CORS 异常处理器 —— 保证 500 错误也能带上 CORS 头
@app.exception_handler(Exception)
async def cors_exception_handler(request: Request, exc: Exception):
    """全局异常捕获，防止 500 丢 CORS 头"""
    origin = request.headers.get("origin", "")
    allowed = ["http://localhost:5173", "http://127.0.0.1:5173"]
    headers = {}
    if origin in allowed:
        headers.update({
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
        })
    return JSONResponse(
        status_code=500,
        content={"detail": f"服务器内部错误: {str(exc)}"},
        headers=headers,
    )


# ============================================================
# 路由挂载：AI 模拟面试模块
# ============================================================
# 独立分层实现（api/interview.py + services/interview_service.py +
# schemas/interview.py + models/interview.py），此处仅做挂载。
# 前缀为 /api/interview，与既有路由（/api/chat、/api/resume/* 等）互不影响。
app.include_router(interview_router)


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
6. 【重要】技能点必须保留原始技术名词，不得翻译或本地化。例如：Python 不能写成"派森"，Flask 不能写成"烧瓶"，MySQL 必须保持大写形式。

【系统标签输出规范 —— 必须严格遵守】
在你的分析报告结尾，必须紧跟以下精确格式的标签块（用于前端结构化提取，前后不要加任何解释性文字）：
注意：学习路径按阶段用 "->" 分隔；任务列表每条用中文分号"；"分隔。

[学习路径] 阶段一：xxx -> 阶段二：xxx -> 阶段三：xxx
[任务-P0] 1. xxx；2. xxx；3. xxx
[任务-P1] 1. xxx；2. xxx
[任务-P2] 1. xxx；2. xxx

其中：
- P0 为紧急核心任务（必须立刻着手的高优先级技能点）
- P1 为进阶拓展任务（中期需要掌握的进阶内容）
- P2 为长期成长任务（持续关注的前沿动态与软技能）
- 每条任务务必具体可执行，例如"完成TypeScript官方文档泛型章节的学习"而非"学习TS"
- 学习路径的阶段名称应简洁有力，体现技能成长阶梯"""

EMPTY_PROMPT = """你是一个职业规划顾问。用户技能：{user_skills}，未找到精准匹配岗位。

请直接给出通用职业规划建议，包括：推荐方向、入门技能、学习路径。

【重要限制】
1. 如果用户的技能描述与数据库中的岗位完全不相关，请礼貌地提示用户提供更具体的职业信息，不要强行匹配或虚构建议。
2. 【重要】技能点必须保留原始技术名词，不得翻译或本地化。例如：Python 不能写成"派森"，Flask 不能写成"烧瓶"，MySQL 必须保持大写形式。

【系统标签输出规范 —— 必须严格遵守】
在你的分析报告结尾，必须紧跟以下精确格式的标签块（用于前端结构化提取，前后不要加任何解释性文字）：
注意：学习路径按阶段用 "->" 分隔；任务列表每条用中文分号"；"分隔。

[学习路径] 阶段一：xxx -> 阶段二：xxx -> 阶段三：xxx
[任务-P0] 1. xxx；2. xxx；3. xxx
[任务-P1] 1. xxx；2. xxx
[任务-P2] 1. xxx；2. xxx

其中：
- P0 为紧急核心任务（必须立刻着手的高优先级技能点）
- P1 为进阶拓展任务（中期需要掌握的进阶内容）
- P2 为长期成长任务（持续关注的前沿动态与软技能）
- 每条任务务必具体可执行，例如"完成TypeScript官方文档泛型章节的学习"而非"学习TS"
- 学习路径的阶段名称应简洁有力，体现技能成长阶梯"""


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
# 说明：评分不再由 LLM 决定。维度分数与证据由 services.resume_scoring 纯规则计算
# （同输入同输出，可复现），LLM 只负责基于这些结构化事实撰写文字诊断。
RESUME_ANALYSIS_PROMPT = """你是简历分析助手。下面是对该简历的**结构化事实**，由规则引擎从简历原文提取，每条维度均附原文证据。

{summary}

要求：
1. 只依据上述事实撰写诊断，不得引入事实之外的信息
2. 直接指出核心竞争力，并引用至少一条原文证据
3. 最多 2 条提升建议，须针对「未覆盖」或「得分为 0」的维度
4. 禁止编造简历中不存在的经历、技能或成果；禁止复述分数
5. 不超过 150 字，风格简洁专业，不要废话

输出格式：
【核心优势】：xxx
【提升建议】：xxx"""


def _decode_resume(content: bytes) -> str:
    """解码简历文本。

    二进制格式（PDF / DOCX）不在此处硬解——用 latin-1 强行解码会把二进制
    变成"看起来成功"的乱码，进而产出基于噪声的评分。此处显式报错，交由前端
    提示用户，PDF / DOCX 的真实解析在下一阶段接入。
    """
    head = content[:8]
    if head.startswith(b"%PDF"):
        raise HTTPException(
            status_code=415,
            detail="暂不支持 PDF 解析：请上传 TXT / MD 文本简历（PDF 解析将在下一阶段接入）",
        )
    if head.startswith(b"PK\x03\x04"):
        raise HTTPException(
            status_code=415,
            detail="暂不支持 DOCX 解析：请上传 TXT / MD 文本简历（DOCX 解析将在下一阶段接入）",
        )
    for enc in ("utf-8", "gb18030", "utf-16"):
        try:
            return content.decode(enc)
        except UnicodeDecodeError:
            continue
    raise HTTPException(
        status_code=400,
        detail="无法识别文件编码，请另存为 UTF-8 编码的 TXT / MD 文件后重试",
    )


@app.post("/api/resume/analyze", tags=["简历分析"])
@app.post("/api/analyze-resume", tags=["简历分析"], include_in_schema=False)
async def analyze_resume(file: UploadFile = File(...)):
    """
    简历分析接口 —— 8 维可取证诊断。

    每个维度返回 {key, name, weight, score, rationale, evidence}，
    evidence 为指向简历原文的片段；取不到证据的维度记 0 分并说明原因，
    不做无依据的推算。
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="文件名为空")

    content = await file.read()
    text = _decode_resume(content)

    if len(text.strip()) < 20:
        raise HTTPException(status_code=400, detail="简历内容过少，无法进行分析")

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
    try:
        chat_answer = await spark_api.chat_async(prompt)
    except Exception:
        chat_answer = ""

    # SparkAPI 失败时返回的是中文字符串而非异常，必须按前缀识别
    if (
        not chat_answer
        or len(chat_answer.strip()) < 10
        or chat_answer.startswith(("API调用失败", "SparkAPI not available", "AI 回答为空"))
    ):
        ai_available = False
        chat_answer = _fallback_diagnosis()

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
        "skills": skills,
        "score": score,
        "metrics": metrics,
        "reference_job": result["reference_job"],
        "sections": result["sections"],
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
