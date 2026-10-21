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
from typing import Any, Dict, Optional, List

from fastapi import FastAPI, HTTPException, UploadFile, File, Depends, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, select, update

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
from deps import get_current_admin, get_current_session, get_current_user  # noqa: E402

# ---- 安全工具：密码哈希、token、会话有效性 ----
from security import hash_password, verify_password, generate_token, create_user_session  # noqa: E402

# ---- 登录失败限流：默认进程内，``AUTH_LOGIN_STORE=db`` 切到共享表（多 worker 安全）----
# 本模块是**门面**：内存模式转调 ``utils.login_throttle``（行为逐字不变），
# db 模式走 ``login_failure`` 表。见 services/login_throttle_store 的模块文档。
from services.login_throttle_store import (  # noqa: E402
    LoginThrottleUnavailable,
    check as throttle_check,
    record as throttle_record,
    reset as throttle_reset,
)

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
from services.resume_scoring import build_prompt_summary, evaluate_resume  # noqa: E402

# ---- WS 一次性连接凭证的共享存储（登出时作废未使用的 ticket）----
# 放在这里而不是 api 层：登出端点在本文件，而 ticket 存储是多处共用的基础设施。
from services import ws_ticket_store  # noqa: E402

# ---- 撤销广播（跨实例让实时通道主动断开）----
# 踢下线 / 登出时写一行；每个实例的轮询器读到后断开**自己身上**的连接。
from services import ws_revocation  # noqa: E402
from services import ws_revocation_watcher  # noqa: E402

#: 与 ``services/ws_ticket_store.py`` 同一个 logger 名 ⇒ 日志配置一处生效。
_ticket_log = logging.getLogger("careerai.ws_ticket")

# ---- 岗位匹配：keyword_match 曾在本文件重复定义两份（逐字节相同），
# 现统一收敛为 services/job_matching.py 中的唯一实现，此处仅导入使用。
from services.job_matching import keyword_match  # noqa: E402

# ---- 路由：AI 模拟面试模块（独立分层，见 api/interview.py）----
from api.interview import router as interview_router  # noqa: E402
from api.interview_ws import router as interview_ws_router  # noqa: E402
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
    """无盐 SHA-256（**仅旧格式兼容 / 测试造数**）。新写入请用 ``security.hash_password``。"""
    return hashlib.sha256(password.encode()).hexdigest()


# 薄导出，供测试与脚本校验密码而不直接 import security
_verify_password = verify_password


def _generate_token(user_id: int, email: str, role: str) -> str:
    """生成高熵 token（参数保留仅为了接口兼容）。"""
    return generate_token()


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
# Embedding 运行状态上报（只读：不读库、不发网络请求、不编码文本）
# ============================================================
def embedding_status() -> Dict[str, Any]:
    """当前进程**默认 Embedding** 的运行状态标识（固定 4 键，成功时 ``error=None``）。

    为什么要有它：``default_embedder()`` 在「一个 ``EMBEDDING_*`` 都不配」时会返回
    ``HashEmbeddingService``（``provider="hash-local"``，**词面哈希、无语义**）。
    那种状态下 RAG **不会报错**——它只是一条都检索不到（检索器按 ``embedding_model``
    过滤，模型名对不上）。于是「部署漏配凭据」的表现是「知识库好像没内容」，
    **静默且无从排查**。本函数把「到底在用哪个 Embedding、它是不是语义模型」
    摆到启动日志与健康检查里，让这件事不可能再被忽略。

    纯读配置：构造 Provider 只解析环境变量，**不读库、不发网络请求、不编码任何文本**，
    因此可安全地在启动期与每次健康检查时调用。
    配置非法（如 ``openai`` 却没给密钥）时**刻意不抛异常**——健康检查永远不该 500，
    改为把错误放进 ``error`` 字段暴露出来。
    """
    try:
        # 延迟导入：与 services.knowledge_rag 同一取舍（模块顶层只留标准库）。
        from services.knowledge_rag import describe_default_embedding

        status: Dict[str, Any] = dict(describe_default_embedding().to_dict())
        status["error"] = None
        return status
    except Exception as exc:  # noqa: BLE001 - 配置错误要变成可见字段，不是 500
        return {
            "provider": None,
            "dimension": None,
            "semantic_enabled": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _report_embedding_status() -> None:
    """把 Embedding 状态打进启动日志；**非语义**时给出醒目告警。"""
    status = embedding_status()
    if status["error"] is not None:
        print(f"[embedding][ERROR] 默认 Embedding 配置非法：{status['error']}")
        return
    if status["semantic_enabled"]:
        print(
            f"[embedding] provider={status['provider']} "
            f"dim={status['dimension']} semantic=True"
        )
        return
    print(
        f"[embedding][WARN] provider={status['provider']} "
        f"dim={status['dimension']} semantic=False —— 这是**离线占位**（词面哈希），"
        "检索出的「相似度」没有语义含义，RAG 知识注入会静默失效。"
        "生产部署请配置 EMBEDDING_* 后重启。"
    )


# ============================================================
# 向量后端运行状态上报（只读配置；切片数只用于**规模告警**，不参与检索）
# ============================================================
def vector_status() -> Dict[str, Any]:
    """当前进程**向量后端**的运行状态（固定 3 键，成功时 ``error=None``）。

    为什么要有它：``VECTOR_STORE`` 不配时默认走 ``vector_store_sql``——它**没有 ANN
    索引**，每次检索都把**全部**切片（含 2560 维向量 JSON）捞进内存做 Python 侧余弦。
    规模小的时候完全无感，规模一大就变成每次出题都读几十 MB。
    本函数把「现在到底走的是哪条路、有没有 ANN」摆到启动日志与健康检查里，
    让「以为开了 ANN、其实还在全表扫描」不可能再被忽略。

    纯读配置：**不读库、不发网络请求、不构造任何后端实例**（``chroma`` 要 import
    ``chromadb``，那是可选依赖，不能因为「看一眼配置」就被拉起来）。
    配置非法（``VECTOR_STORE`` 取值无法识别）时**刻意不抛异常**——健康检查永远不该
    500，改为把错误放进 ``error`` 字段。
    """
    try:
        # 延迟导入：与 services.knowledge_rag 同一取舍（模块顶层只留标准库/ORM）。
        from services.knowledge_rag import describe_vector_backend

        return dict(describe_vector_backend())
    except Exception as exc:  # noqa: BLE001 - 配置错误要变成可见字段，不是 500
        return {"backend": None, "ann": None, "error": f"{type(exc).__name__}: {exc}"}


def _vector_size_warning(backend: Any, chunks: Optional[int]) -> Optional[str]:
    """规模告警文案：全表扫描后端 + 切片数达阈值 ⇒ 返回文案；否则 ``None``。

    单独抽成**纯函数**（不读库、不打印）是为了让它**可被单测直接断言**——
    否则「阈值到底有没有生效」只能靠读日志猜。
    """
    if chunks is None:
        return None
    from services.knowledge_rag import BACKEND_SQL, FULL_SCAN_WARN_CHUNKS

    if backend != BACKEND_SQL or chunks < FULL_SCAN_WARN_CHUNKS:
        return None
    return (
        f"切片数 {chunks} 已达告警阈值 {FULL_SCAN_WARN_CHUNKS}，而向量后端是 "
        f"{BACKEND_SQL}（全表扫描：每次检索都把全部切片连同向量读进内存算余弦）。"
        "建议改用 HNSW ANN 索引：安装 chromadb 后设 VECTOR_STORE=chroma 并重启"
        "（切换前可先跑 backend/tests/rag_vector_backend_switch_run.py 验证）。"
    )


async def _count_knowledge_chunks(conn: Any) -> Optional[int]:
    """数 ``knowledge_chunk`` 有多少行（只读；失败返回 ``None``，**不影响启动**）。"""
    try:
        from models import KnowledgeChunk

        result = await conn.execute(select(func.count()).select_from(KnowledgeChunk))
        return int(result.scalar_one())
    except Exception as exc:  # noqa: BLE001 - 规模告警只是附加信息，绝不能拖垮启动
        print(f"[vector] 切片数统计失败（不影响启动）：{type(exc).__name__}: {exc}")
        return None


def _report_vector_status(chunks: Optional[int] = None) -> None:
    """把向量后端状态打进启动日志；**全表扫描 + 规模超阈值**时给出醒目告警。"""
    status = vector_status()
    if status["error"] is not None:
        print(f"[vector][ERROR] 向量后端配置非法：{status['error']}")
        return

    backend = status["backend"]
    size = "" if chunks is None else f" chunks={chunks}"
    if status["ann"]:
        print(f"[vector] backend={backend} ann=True{size}")
        return

    warning = _vector_size_warning(backend, chunks)
    if warning is not None:
        print(f"[vector][WARN] {warning}")
        return
    print(f"[vector] backend={backend} ann=False{size}（全表扫描）")


# ============================================================
# RAG 阈值溯源上报（只读；判定「阈值是否仍与当前模型 + 语料匹配」）
# ============================================================
def threshold_status(provenance_path: Any = None) -> Dict[str, Any]:
    """当前检索阈值是否仍与**当前 Embedding + 当前语料**匹配（固定 6 键）。

    为什么要有它：``RAG_MIN_SCORE`` / ``RAG_MIN_SCORE_RATIO`` 是在**特定模型 + 特定语料**
    上标定出来的。换模型、改语料之后它们**立刻失效**，但系统**不会报错**——
    表现为「一条都检索不到」或「注入一堆无关噪声」，两次都踩过（见
    ``services/rag_threshold_provenance`` 的模块文档）。本函数把这件事变成
    ``GET /api/health`` 上的一个值 + 启动日志里的一行告警。

    **只读**：读一份 JSON（``scripts/rag_threshold_provenance.json``）、算一次语料文件
    sha256、读一遍环境变量——不碰数据库、不发网络请求。配置非法或产物缺失时
    **刻意不抛异常**：前者进 ``error``，后者是 ``status="unknown"``
    （「无法判定」**不等于**「没问题」）。

    :return: 固定 6 键 —— ``status`` / ``reasons`` / ``notes`` / ``expected`` /
        ``current`` / ``error``。
    :param provenance_path: 溯源产物路径。``None`` → 默认
        （``scripts/rag_threshold_provenance.json``）。**只为可测性留的口子**
        ——健康检查与启动日志都走默认值。
    """
    try:
        from services.knowledge_rag import resolve_retriever_defaults
        from services.rag_threshold_provenance import (
            check_provenance,
            corpus_fingerprint,
            load_provenance,
            resolve_corpus_path,
        )

        provenance = load_provenance(provenance_path)
        embedding = embedding_status()
        corpus_sha = None
        if isinstance(provenance, dict):
            corpus_sha = corpus_fingerprint(
                resolve_corpus_path((provenance.get("corpus") or {}).get("file"))
            )
        result = check_provenance(
            provenance,
            {
                "provider": embedding.get("provider"),
                "dimension": embedding.get("dimension"),
                "corpus_sha256": corpus_sha,
                "params": dict(resolve_retriever_defaults()),
            },
        )
        result["error"] = None
        return result
    except Exception as exc:  # noqa: BLE001 - 配置错误要变成可见字段，不是 500
        return {
            "status": "unknown",
            "reasons": [],
            "notes": [],
            "expected": None,
            "current": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _report_threshold_status(provenance_path: Any = None) -> None:
    """把阈值溯源核对结果打进启动日志；**过期 / 无法判定**时给出醒目告警。

    :param provenance_path: 同 :func:`threshold_status`（只为可测性留的口子）。
    """
    status = threshold_status(provenance_path)
    if status["error"] is not None:
        print(f"[threshold][ERROR] 无法核对阈值溯源：{status['error']}")
        return

    if status["status"] == "ok":
        expected = status.get("expected") or {}
        print(
            f"[threshold] 阈值溯源核对通过（provider={expected.get('provider')} "
            f"dim={expected.get('dimension')} params={expected.get('params')}）"
        )
        return

    if status["status"] == "unknown":
        notes = status.get("notes") or ["无标定溯源产物"]
        print("[threshold][WARN] 无法判定检索阈值是否仍适用：" + "；".join(notes))
        return

    print("[threshold][WARN] 检索阈值可能**已过期**（换过模型或改过语料？）：")
    for reason in status["reasons"]:
        print(f"             · {reason}")
    print(
        "             重跑 tests/rag_production_query_calibration_spark_run.py 后，"
        "用 scripts/apply_threshold_calibration.py --yes 一键写回并登记溯源。"
    )
    for note in status.get("notes") or []:
        print(f"             · （提示）{note}")


# ============================================================
# 数字人通道状态上报（只读配置；**不构造 provider、不联网**）
# ============================================================
def avatar_status() -> Dict[str, Any]:
    """当前进程**数字人 Avatar Provider** 的运行状态（固定 7 键，``error`` 缺省 ``None``）。

    为什么要有它：``interview_mode="avatar"`` 的通道在 provider 不可用时**会静默降级**
    为纯文本交付（这是刻意的——数字人故障不该阻断面试）。副作用是「到底在用 Mock
    还是真讯飞、凭据配齐没有」**从接口返回值上看不出来**。本函数把这件事摆到启动日志
    与健康检查里：``provider`` / ``configured`` / ``host`` / ``error``。

    纯读配置：``describe_avatar_provider`` **不构造 provider、不联网、不抛异常**
    （配置非法时把原因塞进 ``error``），因此可安全地在启动期与每次健康检查时调用。

    :return: ``{provider, known, configured, host, base_path, masked_api_key, error}``。
    """
    try:
        # 延迟导入：与 embedding / vector / threshold 同一取舍（模块顶层只留标准库）。
        from services.avatar_provider_iflytek import describe_avatar_provider

        return describe_avatar_provider()
    except Exception as exc:  # noqa: BLE001 - 配置错误要变成可见字段，不是 500
        return {
            "provider": None,
            "known": [],
            "configured": False,
            "host": "",
            "base_path": "",
            "masked_api_key": "",
            "error": f"{type(exc).__name__}: {exc}",
        }


def _report_auth_throttle_status() -> None:
    """把登录限流的**存储后端**打进启动日志。

    为什么要显式打：这是「能不能上多 worker」的判据之一。如果部署方以为
    已经切到共享表、实际仍在进程内计数，多 worker 后爆破上限会被静默放大 N 倍
    —— 而这种问题**不会**在功能上表现出来，只能靠启动日志暴露。
    """
    from services.login_throttle_store import limits, store_mode

    mode = store_mode()
    max_failures, window_seconds = limits()
    if mode == "db":
        print(
            f"[auth] 登录失败限流 = **共享表**（login_failure）："
            f"{max_failures} 次 / {window_seconds}s，跨实例统一计数。"
        )
    else:
        print(
            f"[auth] 登录失败限流 = **进程内**：{max_failures} 次 / {window_seconds}s。"
            "★ 多 worker 时每个进程独立计数 ⇒ 实际上限 × 进程数；"
            "需要多 worker 请设 AUTH_LOGIN_STORE=db 后重启。"
        )


def _report_avatar_status() -> None:
    """把数字人通道状态打进启动日志。

    三档措辞刻意区分**后果**：
    - ``mock`` → 正常（离线 Mock，不产生真实音视频）；
    - 配置非法 / 凭据不全 → ``[WARN]``（通道**静默降级为纯文本交付**，面试不中断）；
    - 连状态都读不出来 → ``[ERROR]``（不该发生，说明 describe 自身炸了）。
    """
    status = avatar_status()
    provider = status["provider"]
    if provider is None:
        print(f"[avatar][ERROR] 无法读取数字人通道状态：{status['error']}")
        return
    if provider == "mock":
        print(
            "[avatar] provider=mock —— 数字人通道走**离线 Mock**（不产生真实音视频）；"
            "avatar 模式面试照常跑完，交付内容与 text 逐字段一致。"
            "要接真实讯飞虚拟人请配置 AVATAR_PROVIDER=iflytek 与 AVATAR_IFLYTEK_* 后重启。"
        )
        return
    if status["error"] is not None:
        print(
            f"[avatar][WARN] {status['error']} —— 通道会**静默降级为纯文本交付**"
            "（面试不中断，但不会有数字人画面）。"
        )
        return
    print(
        f"[avatar] provider={provider} host={status['host']} "
        f"base_path={status['base_path']} api_key={status['masked_api_key']} configured=True"
    )


# ============================================================
# 生产化运行时状态上报（只读内存：不读库、不发网络请求、不构造 provider）
# ============================================================
def production_status() -> Dict[str, Any]:
    """实时通道的**生产化运行时状态**（固定 12 键，``error`` 缺省 ``None``）。

    为什么要有它：``GET /api/health`` 此前只回答「依赖配好了没有」，回答不了
    「**现在**跑得怎么样」——活跃连接数、进程内跟踪了多少条会话、空闲超时关掉了几条、
    作答提交/拒绝各多少、会话淘汰了多少。这些是排障与容量判断的**第一手数据**。

    纯读内存：不读库、不发网络请求、不构造 provider（与 embedding / vector /
    threshold / avatar 四个状态块同一取舍），因此可安全地每次健康检查都调用，
    并且**刻意不抛异常**——健康检查永远不该 500，出错就把原因放进 ``error``。

    :return: ``{idle_timeout_seconds, state_ttl_seconds, max_tracked_sessions,
        max_sessions_per_user, max_connections_per_user, min_answer_interval_seconds,
        tracked_sessions, active_connections, active_sessions, busy_sessions,
        metrics, error}``。
    """
    try:
        # 延迟导入：与 embedding / vector / threshold / avatar 同一取舍。
        from services.interview_production import describe_production_status

        return describe_production_status()
    except Exception as exc:  # noqa: BLE001 - 状态错误要变成可见字段，不是 500
        return {
            "idle_timeout_seconds": None,
            "state_ttl_seconds": None,
            "max_tracked_sessions": None,
            "max_sessions_per_user": None,
            "max_connections_per_user": None,
            "min_answer_interval_seconds": None,
            "tracked_sessions": 0,
            "active_connections": 0,
            "active_sessions": 0,
            "busy_sessions": 0,
            "metrics": {"counters": {}, "gauges": {}, "uptime_seconds": 0.0},
            "error": f"{type(exc).__name__}: {exc}",
        }


def _report_production_status() -> None:
    """把生产化策略打进启动日志（**配置非法时醒目告警**）。

    措辞刻意区分**后果**：默认值都是「保护性且宽松」的，配错了不会立刻出事，
    所以必须让操作者一眼看到「我配的那条没生效」。
    """
    status = production_status()
    if status.get("error") is not None:
        print(
            f"[production][ERROR] 生产化配置非法（已回落默认值，保护**未按你的配置生效**）："
            f"{status['error']}"
        )
        return
    idle = status["idle_timeout_seconds"]
    interval = status["min_answer_interval_seconds"]
    rate_note = "未限制" if not interval else f"最小间隔 {interval}s"
    print(
        f"[production] 实时通道：空闲超时 {idle}s / 会话 TTL {status['state_ttl_seconds']}s / "
        f"跟踪上限 {status['max_tracked_sessions']} / 每用户连接上限 "
        f"{status['max_connections_per_user']} / 每用户会话上限 "
        f"{status['max_sessions_per_user']} / 作答限流 {rate_note}"
    )
    if not interval:
        print(
            "[production] （提示）作答限流未开启；需要防连点/脚本重放请设置 "
            "INTERVIEW_ANSWER_MIN_INTERVAL=0.4 后重启。"
        )


# ============================================================
# Lifespan（MySQL 下由 SQLAlchemy create_all 自动建表 + 幂等补列）
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    assert_single_worker()
    _report_embedding_status()
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
        chunk_count = await _count_knowledge_chunks(conn)
    _report_vector_status(chunk_count)
    _report_threshold_status()
    _report_avatar_status()
    _report_production_status()
    _report_auth_throttle_status()

    # ---- 撤销广播轮询器（**每个实例一个**后台任务）----
    # 先注册「拿到撤销记录后怎么断开连接」的回调：由实时通道层提供，
    # 这样本文件不必知道连接表长什么样，ws_revocation 也不必反向 import 实时通道。
    from api import interview_ws as _ws_module

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
# 独立分层实现（api/interview.py 路由 + services/interview_service.py 会话门面
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
        password_hash=hash_password(data.password),
        role="user",
    )
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)

    # 创建会话
    session = await create_user_session(db, new_user)
    await db.commit()
    token = session.token

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
async def login(data: UserLogin, request: Request, db: AsyncSession = Depends(get_db)):
    """
    用户登录接口。支持 email 或 username 登录。
    未知账号统一返回 401，不自动创建。

    限流存储由 ``AUTH_LOGIN_STORE`` 决定（``memory`` 默认 / ``db`` 共享表）。
    存储不可用时 **fail closed**（503）：放行等于在库抖动期间关掉爆破保护。
    """
    client_ip = request.client.host if request.client else "-"
    throttle_key = f"{data.email.lower().strip()}|{client_ip}"

    async def _throttle(coro):
        """限流存储不可用 ⇒ 503（不静默放行，也不把它混进 401）。"""
        try:
            return await coro
        except LoginThrottleUnavailable:
            raise HTTPException(
                status_code=503,
                detail="登录服务暂时不可用，请稍后再试",
            ) from None

    retry_after = await _throttle(throttle_check(db, throttle_key))
    if retry_after:
        raise HTTPException(
            status_code=429,
            detail="登录尝试过多，请稍后再试",
            headers={"Retry-After": str(retry_after)},
        )

    result = await db.execute(select(User).where(User.email == data.email))
    user = result.scalar_one_or_none()

    if not user:
        result = await db.execute(select(User).where(User.username == data.email))
        user = result.scalar_one_or_none()

    if not user:
        # 未知账号也跑 dummy bcrypt，消除时间侧信道
        verify_password(data.password, "")
        await _throttle(throttle_record(db, throttle_key))
        raise HTTPException(status_code=401, detail="邮箱或密码错误")

    ok, needs_upgrade = verify_password(data.password, user.password_hash)
    if not ok:
        await _throttle(throttle_record(db, throttle_key))
        raise HTTPException(status_code=401, detail="邮箱或密码错误")

    if needs_upgrade:
        user.password_hash = hash_password(data.password)

    # 记录登录日志
    log = UserLoginLog(
        email=user.email,
        role=user.role,
        last_login=datetime.now().strftime("%Y-%m-%d %H:%M"),
        search_count=0,
    )
    db.add(log)

    # 创建会话
    session = await create_user_session(db, user)
    db.add(session)
    await db.commit()
    token = session.token

    await throttle_reset(db, throttle_key)

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


@app.post("/api/auth/logout", tags=["认证"])
async def logout(
    session=Depends(get_current_session),
    db: AsyncSession = Depends(get_db),
):
    """撤销当前会话（使 token 立即失效），并作废该用户所有未使用的 WS ticket。

    ★ 还要广播一条撤销：**已建立**的 WS 连接不受 ticket 作废影响
    （ticket 早已被消费），必须由各实例的轮询器读到广播后主动断开。
    广播按 ``auth_session_id`` 精确匹配 ⇒ 只踢**这一条**登录会话，
    同一用户在其他设备上的面试不受影响。
    """
    session.revoked_at = datetime.utcnow()
    db.add(session)
    await db.commit()

    # 撤销广播（失败不阻断登出：主效应已落库，且有周期性复核兜底）
    try:
        await ws_revocation.record(
            db,
            user_id=session.user_id,
            auth_session_id=session.id,
            reason=ws_revocation.REASON_LOGOUT,
        )
    except ws_revocation.RevocationStoreUnavailable as exc:
        _ticket_log.warning(
            "ws_revocation_record_on_logout_failed user_id=%s error=%s",
            session.user_id,
            type(exc).__name__,
        )

    # ★ 为什么要连带作废 ticket：ticket 路径下**没有**长期 token 可复核
    #   （见 api/interview_ws.py 的 _auth_still_valid），已签发的 ticket 在 30 秒内
    #   仍然能换到一条 WS 连接。登出即作废它们，把"登出 ⇒ 立即失效"补齐。
    # ★ 失败**不阻断登出**：主效应（token 撤销）已经落库，且 ticket 30 秒后自然过期；
    #   这里吞掉异常是**有意的**，但必须留下 WARNING（不静默）。
    try:
        revoked = await ws_ticket_store.revoke_for_user(db, user_id=session.user_id)
        if revoked:
            _ticket_log.info(
                "ws_ticket_revoked_on_logout user_id=%s count=%s",
                session.user_id,
                revoked,
            )
    except ws_ticket_store.TicketStoreUnavailable as exc:
        _ticket_log.warning(
            "ws_ticket_revoke_on_logout_failed user_id=%s error=%s",
            session.user_id,
            exc,
        )
    return {"success": True, "message": "已安全退出"}


@app.get("/api/auth/me", tags=["认证"])
async def me(current_user: dict = Depends(get_current_user)):
    """返回当前登录用户信息（供前端恢复登录态时校验）。"""
    return {"success": True, "user": current_user}


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
        "embedding": embedding_status(),
        "vector": vector_status(),
        "threshold": threshold_status(),
        "avatar": avatar_status(),
        "metrics": production_status(),
    }


# ============================================================
# 启动命令
# ============================================================
# cd backend
# pip install -r requirements.txt
# python -m uvicorn main:app --reload --port 5000 --host 0.0.0.0
