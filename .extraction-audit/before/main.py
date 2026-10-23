"""
Career.ai FastAPI 后端
全面支持异步编程 + Pydantic 数据校验 + 异步 SQLAlchemy 持久化
"""

import os
import re
import json
import sys
import asyncio
import logging
import hashlib
import hmac
import base64
import time
import secrets
from datetime import datetime
from urllib.parse import urlencode
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

# 保证以任意工作目录启动都能导入本项目的顶层模块
# （database / deps / models / services / api）
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ---- 基础设施：Base / engine / 会话工厂 / get_db ----
# 原定义在本文件，为支持 models / services / api 分层而抽取到 database.py，
# 避免分层模块反向 import main 造成循环依赖。
# DATABASE_URL 在本文件已无直接引用，保留导入是为了不破坏既有的
# `main.DATABASE_URL` 引用点（与抽取前的模块命名空间保持一致）。
# ★ ``async_session`` 在这里**没有本文件的直接使用点**，但必须保留：
#   scripts/create_admin.py 与 tests/test_admin_role.py 通过 ``main.async_session``
#   注入测试库会话工厂（同一个兼容命名空间的既有约定，与 main.User /
#   main.engine 一致）。删掉它 = 那两个消费方在运行期 AttributeError。
from database import (  # noqa: E402,F401
    DATABASE_URL,
    Base,
    async_session,
    engine,
    get_db,
)

# ---- 鉴权依赖：原定义在本文件，同样为分层复用而抽取到 deps.py ----
from deps import get_current_admin, get_current_session, get_current_user  # noqa: E402

# ---- 认证模块：业务用例与 HTTP 端点都在 modules/auth（本文件只挂载路由）----
# ★ 为什么 main.py 里不再 import security 的口令/token 工具与登录限流：
#   它们只被认证用例使用，跟着模块走。留着就是「重复的接口封装」，
#   而且会让人误以为入口文件仍是认证逻辑的归属地。
from modules.auth import router as auth_router  # noqa: E402

# ---- 求职业务模块：简历 / 岗位 / 匹配 / 职业规划对话 ----
# ★ LLM 门面走 ports 显式注册（见 modules/career/ports.py）：业务层不 import
#   本文件，否则依赖方向反转（modules 在 L2、main 是装配层）且立刻成硬环。
from modules.career import router as career_router  # noqa: E402
from modules.career.ports import configure as configure_career  # noqa: E402

# ---- 共享基础设施：运行状态上报（健康检查 + 启动日志）----
# 14 个 status/_report_ 函数整体迁到 common/health.py，本文件只留调用点。
# ★ 必须用别名：本文件有一个 `async def health()` 端点（GET /api/health），
#   它的函数名会覆盖同名模块 import。直接用 `from common import health` 时
#   `health.embedding_status()` 里的 health 是那个**函数对象**，运行期
#   AttributeError: 'function' object has no attribute 'embedding_status'。
#   改端点函数名会改 OpenAPI 的 operationId ⇒ 只在这里用别名。
from common import health as health_report  # noqa: E402

# ---- 轻量 schema 同步：create_all 不给既有表加列，新增列需启动时幂等补齐 ----
from utils.schema_sync import ensure_columns  # noqa: E402
from utils.worker_guard import assert_single_worker  # noqa: E402

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

# ---- WS 一次性连接凭证的共享存储（登出时作废未使用的 ticket）----
# 放在这里而不是 api 层：登出端点在本文件，而 ticket 存储是多处共用的基础设施。
from services import ws_ticket_store  # noqa: E402

# ---- 撤销广播（跨实例让实时通道主动断开）----
# 踢下线 / 登出时写一行；每个实例的轮询器读到后断开**自己身上**的连接。
from services import ws_revocation  # noqa: E402
from services import ws_revocation_watcher  # noqa: E402

#: 与 ``services/ws_ticket_store.py`` 同一个 logger 名 ⇒ 日志配置一处生效。
_ticket_log = logging.getLogger("careerai.ws_ticket")


# ---- 路由：AI 模拟面试模块（独立分层，见 modules/interview/api.py）----
from modules.interview import router as interview_router  # noqa: E402
from modules.interview import ws_router as interview_ws_router  # noqa: E402
from api.knowledge import router as knowledge_router  # noqa: E402


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


# ============================================================
# AI 能力接入层装配（modules.ai）
# ------------------------------------------------------------
# `SparkAPI` 从此**只作为协议传输层**存在（HMAC 签名 + websocket 分帧 +
# X1 的 content/reasoning_content 分离），不再直接对外暴露。
# 对外暴露的是 modules.ai 的门面：
#
#   运行模式（mock/offline/production）→ Provider → 超时/有限重试/指数退避
#   → 错误分类 → 指标 → 请求追踪 → Prompt 版本
#
# 门面**仍然**是 `async chat_async(prompt) -> str`，失败仍然给旧式哨兵串，
# 所以 services/interview_agent.py / interview_planner.py 等
# **源码指纹冻结文件一行都不用改**，却已经走在新链路上。
#
# 换供应商 = 换一个 transport（或换一个 Provider 实现），业务代码零改动。
# ============================================================
from modules.ai import (  # noqa: E402
    ai_call_scope,
    build_legacy_spark_facade,
    register_spark_transport,
)

_SPARK_CLIENT = SparkAPI()
register_spark_transport(_SPARK_CLIENT.chat_async)
spark_api = build_legacy_spark_facade()


# ============================================================
# Pydantic Schemas
# ============================================================
# 注：``UserRegister`` / ``UserLogin`` 已随认证模块迁到
# ``modules/auth/schemas.py``（它们只服务 ``/api/auth/*``）。


class JobCreate(BaseModel):
    job_name: str = Field(..., min_length=1, max_length=200)
    city: str = Field(..., min_length=1, max_length=50)
    salary: str = Field(default="面议", max_length=50)
    skills: List[str] = Field(default_factory=list)
    description: str = Field(default="")


# ============================================================
# 依赖：当前用户（从数据库会话表查询）
# ============================================================
# get_current_user / get_current_admin 已抽取到 deps.py，由顶部导入，
# 供 main.py 与路由分层（如 modules/interview/api.py）共用同一套鉴权口径。
# 既有用法 `Depends(get_current_admin)` 保持不变。


# ============================================================
# Lifespan（MySQL 下由 SQLAlchemy create_all 自动建表 + 幂等补列）
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    assert_single_worker()
    # ★ AI 接入层校验放在**最前面**：配置错了就让进程起不来，
    #   而不是先连库、先建表、最后才在第一个用户请求上暴露。
    health_report._report_ai_status()
    health_report._report_embedding_status()
    async with engine.begin() as conn:
        # ① 建表：只创建**不存在的新表**
        await conn.run_sync(Base.metadata.create_all)
        # ② 补列：create_all **不会给既有表加列**。
        #    模型新增的列（如 interview_session.interview_mode）由本步幂等补齐，
        #    否则升级上来的库会在查询时报 Unknown column。
        #    只加列、不改列、不删列、不清数据，重复启动不会重复 ALTER。
        #    见 utils/schema_sync.py。
        added = await ensure_columns(conn)
        if added:
            print(f"[schema] 已补齐缺失列：{', '.join(added)}")
        # 规模可见：切片数在**建表之后**才数得到（只用于告警，失败不影响启动）。
        chunk_count = await health_report._count_knowledge_chunks(conn)
    health_report._report_vector_status(chunk_count)
    health_report._report_threshold_status()
    health_report._report_avatar_status()
    health_report._report_production_status()
    health_report._report_auth_throttle_status()

    # ---- 撤销广播轮询器（**每个实例一个**后台任务）----
    # 先注册「拿到撤销记录后怎么断开连接」的回调：由实时通道层提供，
    # 这样本文件不必知道连接表长什么样，ws_revocation 也不必反向 import 实时通道。
    from modules.interview import ws as _ws_module

    ws_revocation.register_handler(_ws_module.close_revoked_connections)
    if ws_revocation_watcher.watch_enabled():
        await ws_revocation_watcher.start()
    else:
        print(
            "[production] 撤销广播轮询器**已关闭**（INTERVIEW_WS_REVOCATION_WATCH=0）："
            "踢下线 / 登出对已建立连接的生效延迟退化为周期性复核"
            f"（{_ws_module.AUTH_RECHECK_SECONDS:.0f}s 量级）。生产环境不建议关闭。"
        )
    try:
        yield
    finally:
        await ws_revocation_watcher.stop()


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
# 允许来源由环境变量配置，**默认值即现状**（本地 Vite 两个入口）。
# 生产同源部署时无需配置；前后端分域部署时用逗号分隔显式列出，
# 例如 CORS_ALLOW_ORIGINS="https://app.example.com,https://www.example.com"。
def _parse_cors_origins() -> list:
    raw = os.getenv("CORS_ALLOW_ORIGINS", "").strip()
    if not raw:
        return [
            "http://localhost:5173",   # Vite 默认端口
            "http://127.0.0.1:5173",   # 本地 IP 访问
        ]
    return [item.strip() for item in raw.split(",") if item.strip()]


CORS_ALLOW_ORIGINS = _parse_cors_origins()
# 通配符 + 凭证在浏览器侧本就互斥（规范禁止），显式配置 "*" 时关闭凭证，
# 避免「配置看起来生效、实际被浏览器拒绝」的隐性故障。
CORS_ALLOW_CREDENTIALS = "*" not in CORS_ALLOW_ORIGINS
if not CORS_ALLOW_CREDENTIALS:
    logging.getLogger(__name__).warning(
        "CORS_ALLOW_ORIGINS 含 '*'，已自动关闭 allow_credentials（浏览器规范要求）"
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=CORS_ALLOW_CREDENTIALS,
    allow_methods=["*"],           # GET / POST / PUT / DELETE / OPTIONS
    allow_headers=["*"],           # Content-Type / Authorization 等
)

# 兜底 CORS 异常处理器 —— 保证 500 错误也能带上 CORS 头
@app.exception_handler(Exception)
async def cors_exception_handler(request: Request, exc: Exception):
    """全局异常捕获，防止 500 丢 CORS 头"""
    origin = request.headers.get("origin", "")
    headers = {}
    if origin in CORS_ALLOW_ORIGINS:
        headers.update({
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Credentials": "true" if CORS_ALLOW_CREDENTIALS else "false",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
        })
    return JSONResponse(
        status_code=500,
        content={"detail": "服务器内部错误"},
        headers=headers,
    )


# 校验失败：**不回显提交值**（默认 422 会把 ``input`` 原样带回，注册/登录时
# 等于把用户刚输入的密码写进响应体与错误监控）。保留 type/loc/msg 三项，
# 前端 ``_extractDetail`` 的既有解析口径不变。
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    safe_errors = [
        {"type": err.get("type"), "loc": list(err.get("loc", ())), "msg": err.get("msg")}
        for err in exc.errors()
    ]
    return JSONResponse(status_code=422, content={"detail": safe_errors})


# ============================================================
# 日志脱敏兜底：**凭证绝不进访问日志**
# ============================================================
# WS 握手凭证走 URL query（浏览器 WebSocket 不能自定义请求头），而 uvicorn 的
# 访问日志会原样打印 ``full_path``（含 query）。这里挂一个**保留 args 结构**的
# 过滤器：uvicorn 的 AccessFormatter 会按位置解包 ``record.args``，
# 因此不能像 ``RedactingFilter`` 那样把 args 清空，只能逐项脱敏。
class _LogRedactingFilter(logging.Filter):
    """对日志消息与参数做行内凭据脱敏，**保持 args 元组长度不变**。"""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        try:
            from services.interview_production import redact_text

            if isinstance(record.msg, str):
                record.msg = redact_text(record.msg)
            if isinstance(record.args, tuple):
                record.args = tuple(
                    redact_text(a) if isinstance(a, str) else a for a in record.args
                )
            elif isinstance(record.args, dict):
                record.args = {
                    k: (redact_text(v) if isinstance(v, str) else v)
                    for k, v in record.args.items()
                }
        except Exception:  # noqa: BLE001 - 脱敏失败绝不能吞掉日志
            pass
        return True


def _install_log_redaction() -> None:
    """幂等安装到根日志器与 uvicorn 日志器（访问日志是主要泄漏面）。"""
    for name in ("", "uvicorn", "uvicorn.access", "uvicorn.error"):
        target = logging.getLogger(name)
        if not any(isinstance(f, _LogRedactingFilter) for f in target.filters):
            target.addFilter(_LogRedactingFilter())


_install_log_redaction()


# ============================================================
# 路由挂载：AI 模拟面试模块
# ============================================================
# 独立分层实现（modules/interview/api.py 路由 + services/interview_service.py 会话门面
# + services/interview_core.py 流程控制与业务规则 + services/interview_agent.py
# + services/question_validator.py + schemas/interview.py + models/interview.py），
# 此处仅做挂载。
# 前缀为 /api/interview，与既有路由（/api/chat、/api/resume/* 等）互不影响。
app.include_router(interview_router)


# ============================================================
# 路由挂载：AI 面试 · 实时通道（WebSocket）
# ============================================================
# 只服务**数字人视频面试**的交互链路：服务端主动推送 avatar / question /
# answer / score / state 等事件，前端不必自己轮询「现在该调哪个接口」。
# 文字面试**一行都不受影响**——它仍走上面那组 HTTP 路由。
# 路径：ws://host/api/interview/{session_id}/ws?token=<登录 token>
# （浏览器 WebSocket 无法自定义请求头，故 token 走 query）
app.include_router(interview_ws_router)


# ============================================================
# 路由挂载：知识库模块
# ============================================================
# 独立分层实现（api/knowledge.py 路由 + services/knowledge_document_service.py
# 文档读写 + services/knowledge_import_pipeline.py 切片/向量化/落库
# + services/knowledge_maintenance.py 删除与索引重建 + schemas/knowledge.py），
# 此处仅做挂载。
# 前缀为 /api/knowledge：读接口需登录，写接口（导入 / 删除 / 重建）仅 admin。
app.include_router(knowledge_router)


# ============================================================
# 路由挂载：认证模块
# ============================================================
# 独立分层实现（modules/auth/api.py 路由 + modules/auth/service.py 用例与事务
# + modules/auth/schemas.py 请求契约），此处仅做挂载。
# 前缀为 /api/auth：register / login / logout / me。
app.include_router(auth_router)


# ============================================================
# 路由挂载：求职业务模块
# ============================================================
# 独立分层实现（modules/career/api.py 端点 + service.py 用例与岗位数据访问
# + prompts.py 模板 + schemas.py 契约），此处仅做挂载与 LLM 门面注册。
# 前缀为 /api：chat / jobs / match / resume/analyze / analyze-resume / resume/parse。
#
# ★ configure_career 必须在 include_router 之前：路由函数用 Depends(get_llm)
#   取门面，注册晚于首次请求就会 500。
configure_career(spark_api)
app.include_router(career_router)


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


@app.post("/api/admin/users/{user_id}/kick", tags=["管理员"])
async def kick_user_sessions(
    user_id: int,
    current_admin: dict = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """**管理员踢下线**：撤销该用户全部登录会话，并让已建立的实时通道断开。

    三步（顺序有意）：
    1. ``user_session.revoked_at = now``（**主效应**：HTTP 侧下一次请求即 401）；
    2. 作废该用户所有**未使用**的 WS ticket（防止踢完再用旧票建连）；
    3. 写一条**跨实例**撤销广播（``user_id`` 级）⇒ 各实例的轮询器读到后
       主动断开自己身上的连接（目标延迟见 ``services/ws_revocation_watcher``）。

    ★ 为什么第 3 步不能省：``revoked_at`` 只是"以后不认"，而 WS 连接是**已经建立**
    的长连接，它不会自己回头查库。改造前实测：ticket 路径的连接在撤销后
    **>20s 内完全不失效**（上界是空闲超时 300s，而前端有心跳 ⇒ 实际永不失效）。
    ★ 第 1 步失败会直接 500（主效应必须成功）；第 2/3 步失败只记 WARNING
    ——主效应已落库，兜底是周期性复核。
    """
    target = (
        await db.execute(select(User).where(User.id == int(user_id)))
    ).scalar_one_or_none()
    if target is None:
        raise HTTPException(status_code=404, detail="用户不存在")

    now = datetime.utcnow()
    result = await db.execute(
        update(UserSession)
        .where(UserSession.user_id == int(user_id), UserSession.revoked_at.is_(None))
        .values(revoked_at=now)
        .execution_options(synchronize_session=False)
    )
    await db.commit()
    revoked_sessions = int(result.rowcount or 0)

    revoked_tickets = 0
    try:
        revoked_tickets = await ws_ticket_store.revoke_for_user(db, user_id=int(user_id))
    except ws_ticket_store.TicketStoreUnavailable as exc:
        _ticket_log.warning(
            "ws_ticket_revoke_on_kick_failed user_id=%s error=%s",
            user_id,
            type(exc).__name__,
        )

    broadcast = True
    try:
        await ws_revocation.record(
            db,
            user_id=int(user_id),
            auth_session_id=None,  # 空 = 该用户所有会话
            reason=ws_revocation.REASON_ADMIN_KICK,
        )
    except ws_revocation.RevocationStoreUnavailable as exc:
        broadcast = False
        _ticket_log.warning(
            "ws_revocation_record_on_kick_failed user_id=%s error=%s",
            user_id,
            type(exc).__name__,
        )

    return {
        "success": True,
        "message": "已踢下线",
        "user_id": int(user_id),
        "revoked_sessions": revoked_sessions,
        "revoked_tickets": revoked_tickets,
        # 广播失败时明确告知：调用方要知道"主动断开"这次可能不生效，
        # 而不是以为已经踢干净了。
        "broadcast": broadcast,
    }


# ============================================================
# 健康检查
# ============================================================


@app.get("/api/health", tags=["系统"])
async def health():
    """健康检查。

    ``status`` / ``service`` 两个键**保持不变**（既有消费者不受影响）；
    新增 ``embedding`` 键上报默认 Embedding 的运行状态——见 :func:`embedding_status`。
    它是判断「RAG 到底跑在真实语义模型上、还是退化成了离线哈希占位」的**唯一在线入口**。

    新增 ``vector`` 键上报**向量后端**（``sql`` 全表扫描 / ``chroma`` ANN）——
    见 :func:`vector_status`。**纯读配置**：本端点仍然不读库、不发网络请求，
    因此永远廉价、永远不 500；切片数只在**启动日志**里报（见 :func:`_report_vector_status`）。

    新增 ``threshold`` 键上报**检索阈值是否仍与当前模型 + 语料匹配**——
    见 :func:`threshold_status`（只读一份 JSON + 一次语料文件 sha256）。

    新增 ``avatar`` 键上报**数字人通道**在用哪个 provider、凭据是否齐全——
    见 :func:`avatar_status`（只读环境变量，不构造 provider、不联网）。

    新增 ``metrics`` 键上报**实时通道的生产化运行时状态**（活跃连接 / 跟踪会话数 /
    空闲超时次数 / 作答与淘汰计数）——见 :func:`production_status`（只读内存）。
    **既有的 6 个键一个字节未动**（``status`` / ``service`` / ``embedding`` /
    ``vector`` / ``threshold`` / ``avatar``）。
    """
    return {
        "status": "ok",
        "service": "Career.ai FastAPI",
        "embedding": health_report.embedding_status(),
        "vector": health_report.vector_status(),
        "threshold": health_report.threshold_status(),
        "avatar": health_report.avatar_status(),
        "metrics": health_report.production_status(),
    }


# ============================================================
# 启动命令
# ============================================================
# cd backend
# pip install -r requirements.txt
# python -m uvicorn main:app --reload --port 5000 --host 0.0.0.0
