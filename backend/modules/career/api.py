# -*- coding: utf-8 -*-
"""求职业务（career）的 HTTP 端点。

这一层只做四件事：**协议解析、参数校验、认证授权、响应转换**。
业务编排在 ``modules/career/service.py``，Prompt 文案在
``modules/career/prompts.py``，请求契约在 ``modules/career/schemas.py``。

端点清单（路径与迁移前逐字一致）
--------------------------------
======================  ==========================  ==================
方法/路径                用途                         大模型
======================  ==========================  ==================
``POST /api/chat``       职业规划对话                 是
``GET  /api/jobs``       岗位列表                     否
``POST /api/match``      技能匹配岗位                 否
``POST /api/resume/analyze``  简历 8 维诊断            是
``POST /api/analyze-resume``  同上（旧路径别名，不进 OpenAPI）  是
``POST /api/resume/parse``    简历结构化解析           是
======================  ==========================  ==================

★ 为什么 ``_decode_resume`` 留在这一层
--------------------------------------
它把「文件头是 ``%PDF``」翻译成 **415**、「编码认不出」翻译成 **400**。这是
HTTP 语义，不是业务规则——``service`` 只接受已经解码好的 ``str``。如果把它
下沉到 service，领域层就得知道「415 是什么」，那正是 R8 要防的。

★ 为什么这里 import ``AsyncSession`` 之外什么都不碰
--------------------------------------------------
本模块不查库：``/api/jobs`` 与 ``/api/match`` 都走 ``service``。守卫
``test_module_boundaries`` 的 R7（路由层不写 SQL）与 R10（不 import
``models``）会盯着这一点。
"""

from typing import Any

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile

import modules.career.service as service
from modules.career.ports import get_llm
from modules.career.schemas import ChatRequest, MatchRequest, ResumeParseRequest

router = APIRouter(prefix="/api")


# ============================================================
# 协议层助手
# ============================================================
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


# ============================================================
# 路由：AI 看板
# ============================================================
@router.post("/chat", tags=["AI 看板"])
async def chat(data: ChatRequest, llm: Any = Depends(get_llm)):
    """职业规划对话接口"""
    return await service.chat(data, llm=llm)


# ============================================================
# 路由：岗位
# ============================================================
@router.get("/jobs", tags=["岗位"])
async def get_jobs():
    """获取所有岗位列表"""
    return {"jobs": await service.get_all_jobs()}


@router.post("/match", tags=["岗位"])
async def match_jobs(data: MatchRequest):
    """技能匹配岗位接口"""
    return {"results": await service.filter_jobs_by_keywords(data.skills, data.min_match_rate)}


# ============================================================
# 路由：简历分析
# ============================================================
# 说明：评分不再由 LLM 决定。维度分数与证据由 services.resume_scoring 纯规则计算
# （同输入同输出，可复现），LLM 只负责基于这些结构化事实撰写文字诊断。
@router.post("/resume/analyze", tags=["简历分析"])
@router.post("/analyze-resume", tags=["简历分析"], include_in_schema=False)
async def analyze_resume(file: UploadFile = File(...), llm: Any = Depends(get_llm)):
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

    return await service.analyze_resume_text(text, llm=llm)


# ============================================================
# 路由：简历解析
# ============================================================
@router.post("/resume/parse", tags=["简历解析"])
async def parse_resume(data: ResumeParseRequest, llm: Any = Depends(get_llm)):
    """简历文本解析接口"""
    resume_text = data.resume_text
    if not resume_text or not resume_text.strip():
        raise HTTPException(status_code=400, detail="简历文本为空")

    return await service.parse_resume_text(resume_text, llm=llm)
