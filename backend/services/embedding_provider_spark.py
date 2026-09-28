# -*- coding: utf-8 -*-
"""AI 面试知识库 · **讯飞星火 Embedding Provider**（``https://emb-cn-huabei-1.xf-yun.com/``）。

为什么单独一个模块（而不是塞进 ``embedding_provider.py``）
----------------------------------------------------------
``embedding_provider.py`` 讲的是 **OpenAI 兼容** 协议（``POST {base_url}/embeddings`` +
``Authorization: Bearer`` + ``{"data":[{"embedding":[…]}]}``）。讯飞星火是**另一套协议**：

===========================  ==========================  ==========================
环节                         OpenAI 兼容                 讯飞星火
===========================  ==========================  ==========================
鉴权                         请求头 ``Authorization``     **HMAC-SHA256 签名放 query string**
请求体                       ``{"model","input"}``        ``{"header","parameter","payload"}``
文本传法                     明文 JSON 数组                ``base64(JSON)`` 塞进 ``payload.messages.text``
响应取向量                   ``data[i].embedding``        ``payload.feature.text`` = **base64(小端 float32)**
批量                         原生 ``input`` 数组           **只接受单条文本**
向量维度                     由模型决定                    实测 **2560**
===========================  ==========================  ==========================

硬塞进同一个类会让「按配置选实现」的工厂里堆满 ``if provider == …`` 的协议分支，
也让两条协议的测试互相污染。**与任务 44 / ``vector_store_chroma`` 另起模块是同一条理由**：
``embedding_provider.py`` 的顶层依赖被 AST 断言锁死（只能标准库 + ``services``），
而本模块需要 ``hmac`` / ``struct`` / ``wsgiref`` 这类**额外的标准库**，
以及「把签名 URL 从异常消息里抹掉」这种只有本协议才需要的逻辑。

于是分工是：

::

    services/embedding_service.py            接口 + 校验 + 离线占位（零依赖，一行未改）
        └── services/embedding_provider.py   OpenAI 兼容实现 + 配置 + 工厂（一行协议逻辑未改）
              └── services/embedding_provider_spark.py   【本模块】讯飞星火实现
                    └── knowledge_rag.default_embedder()  组装器（唯一决定「用哪个模型」）

**接口不变**：本模块只实现基类的 ``_embed_one`` 钩子 + 覆盖 ``embed_batch``；
``embed(text)`` 一行都没重写，入参 / 出参校验口径与离线占位、OpenAI 兼容实现**完全一致**
（直接复用 ``embedding_service`` 的 ``_require_texts`` / ``_require_vector``）。

配置方式（全部走环境变量；密钥只写在 ``backend/.env``）
------------------------------------------------------
==============================  ===================================  ==========================================
变量                            默认值                               说明
==============================  ===================================  ==========================================
``EMBEDDING_PROVIDER``          *(空 → 自动)*                        **必须显式写 ``spark`` 才会用本模块**；
                                                                     留空时只按 ``EMBEDDING_API_KEY`` 判断
                                                                     （``SPARK_*`` 的存在**不会**自动切换）
``SPARK_EMBEDDING_APP_ID``      *(空 → 回落 ``SPARK_APP_ID``)*        **必填**；Embedding 专用 AppId
``SPARK_EMBEDDING_API_KEY``     *(空 → 回落 ``SPARK_API_KEY``)*       **必填**；参与签名，**禁止提交**
``SPARK_EMBEDDING_API_SECRET``  *(空 → 回落 ``SPARK_API_SECRET``)*    **必填**；参与签名，**禁止提交**
``SPARK_APP_ID`` / ``_API_KEY`` / ``_API_SECRET``                     **回落组**：**文本大模型 X1** 的凭据
                                *(空)*                                （见 ``main.SparkAPI``）。专用组未配置时
                                                                      逐项回落 ⇒ 既有部署行为不变
``EMBEDDING_BASE_URL``          ``https://emb-cn-huabei-1.xf-yun.com``   不含 query；签名后拼上 ``?authorization=…``
                                                                     （末尾 ``/`` 会被去掉）
``EMBEDDING_MODEL``             ``xinghuo-embedding``                写进 ``KnowledgeChunk.embedding_model``
``EMBEDDING_DIMENSION``         ``2560``                             声明维度；换模型必须同步改（否则会报维度不符）
``EMBEDDING_TIMEOUT``           ``30``                               单次请求超时（秒）；**必须 <= 60**
                                                                     （官方要求「全链路请求会话时长
                                                                     不超过 1 分钟」）
``EMBEDDING_SPARK_DOMAIN``      *(空 → 由 role 决定)*                 ``query`` / ``para``；只在排障时手工覆盖
==============================  ===================================  ==========================================

**为什么要两组凭据**：讯飞的「文本大模型 X1」与「Embedding」是**两项独立授权**，
同一个 AppId 未必都开通——实测（2026-09-28）一个 AppId 只通 X1 文本
（Embedding 报 ``HTTP 500 / code=11200 licc failed``），另一个只通 Embedding
（X1 报 ``AppIdNoAuthError``）。于是两组凭据**必须能同时存在、各用各的**。
只配一组也能跑：Embedding 优先读专用组，取不到就回落 ``SPARK_*``。

**刻意不读 ``EMBEDDING_BATCH_SIZE``**：上游接口只接受**单条**文本（``payload.messages.text``
里是一个 ``messages`` 数组），没有可用的原生批量 ⇒ 批量只能逐条发。
配了 ``EMBEDDING_BATCH_SIZE`` 对 spark **无任何效果**，这里如实写明，而不是假装分批。

``query`` / ``para``（非对称检索）—— 由**通用 role** 映射，而不是让调用方认识讯飞的词汇
----------------------------------------------------------------------------------------
讯飞把「用户问题」与「知识原文」分两个 ``domain``（同 BGE 的 ``query:`` / ``passage:``）：
**配对错了会掉召回，但不会报错**。而 ``EmbeddingService`` 的接口只有 ``embed(text)``，
**分不出这一条是问题还是原文**（读侧检索与写侧入库都调 ``embed``）。
所以区分只能落在**实例**上：``embedding_provider`` 定义了与厂商无关的
:data:`~services.embedding_provider.ROLE_DOCUMENT` / :data:`~services.embedding_provider.ROLE_QUERY`，
本模块负责把它翻成讯飞的 ``para`` / ``query``。映射表 :data:`ROLE_TO_DOMAIN`：

- 写侧（``knowledge_import_pipeline`` → ``default_embedder()``）→ ``document`` → ``para``
- 读侧（``knowledge_rag.build_vector_retriever`` → ``default_embedder(role="query")``）→ ``query``

异常映射（沿用 ``embedding_service`` 的四分类，不另起一套）
----------------------------------------------------------
=================================  ================================================
上游情况                            抛什么
=================================  ================================================
网络失败 / 超时 / HTTP 非 2xx       ``EmbeddingUnavailableError``（+``RuntimeError``）
HTTP 2xx 但 ``header.code != 0``    同上（**业务错误码也算「上游不可用」**：鉴权没配好、
                                    限流、服务异常都不该被当成「编码成功」；消息里带
                                    ``code`` 与 ``message``，并对已知码附排查提示）
2xx 但结构不对（不是对象 / 没有      ``EmbeddingDimensionError``（+``ValueError``）
``payload.feature.text`` / 不是合法
base64 / 字节数不是 4 的倍数 / 维度
与声明的 ``EMBEDDING_DIMENSION`` 不符）
入参空文本 / 纯空白 / 非字符串       ``EmbeddingInputError``（+``ValueError``）
配置不合法（缺 ``SPARK_*`` /         ``EmbeddingProviderConfigError``（+``ValueError``，
``domain`` 取值非法 / 超时非正）      **构造期就报**）
=================================  ================================================

**密钥安全**：``SparkEmbeddingConfig.api_key`` / ``api_secret`` 都是 ``repr=False``，
``__repr__`` 只显示打码形式；并且 :class:`SparkEmbeddingTransport` 会把**签名 URL 的
query string 从异常消息里整段抹掉**——讯飞的凭据在 URL 里（``authorization`` 是
``base64(api_key + signature)``，**可逆**），而默认传输在超时消息里会带上完整 URL。

怎么在**无网络**环境测试
------------------------
与 ``embedding_provider`` 同一取舍：传输层可注入。测试注入假 transport 即可断言
「签名 URL 长什么样、请求体是什么、返回顺序如何」，**完全不联网**；
签名本身用固定 ``now`` 传入，因此**逐字节可复现**。

刻意不做
--------
- 不重试、不做退避（重试策略属于调用方，同 ``embedding_provider``）
- 不做全局单例；不 import ``models`` / ``database`` / ``fastapi`` / Retriever / ``interview_*``
- 不把 ``api_key`` 打进任何日志 / 异常消息 / ``repr``
- **不做并发批量**：上游**只接受单条**文本，且实测有 license 限流（见下），
  并发只会把限流打成「大面积失败」

实测注意（2026-09，用本项目凭据真实调用）
----------------------------------------
- 请求方法与地址：**``POST https://emb-cn-huabei-1.xf-yun.com/``**（签名参数在 query string）
- **官方要求：全链路请求会话时长不超过 1 分钟** ⇒ ``EMBEDDING_TIMEOUT`` 上限 60s
  （见 :data:`MAX_SESSION_SECONDS`）。本实现「一条文本 = 一次短请求」，
  单次远低于该上限，不会撞到它。
- 响应是 ``base64`` 的**小端 float32**，10240 字节 ⇒ **2560 维**
- ``domain=query`` 与 ``domain=para`` 都能正常返回，维度一致
- **上游有 license 限流**：连续快速调用会返回 ``HTTP 500`` + ``code=11202``
  （``message="licc failed"``）；**间隔 3 秒逐条调用则连续成功**。
  该错误经 HTTP 非 2xx 路径映射为 ``EmbeddingUnavailableError``（**可重试/降级**），
  分类正确——它**不是**配置错，不该让调用方以为「密钥填错了」。
  因此批量入库请保持**串行 + 调用方重试**，不要并发压。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import struct
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from time import mktime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlsplit
from wsgiref.handlers import format_date_time

from services.embedding_provider import (
    ENV_BASE_URL,
    ENV_DIMENSION,
    ENV_MODEL,
    ENV_TIMEOUT,
    ROLE_DOCUMENT,
    ROLE_QUERY,
    EmbeddingProviderConfigError,
    EmbeddingTransport,
    HttpxEmbeddingTransport,
    _read_float,
    _read_int,
    _read_str,
)
from services.embedding_service import (
    EmbeddingDimensionError,
    EmbeddingError,
    EmbeddingService,
    EmbeddingUnavailableError,
    _require_texts,
    _require_vector,
)

# ============================================================
# 常量：环境变量名 / provider 取值 / 默认值
# ============================================================
#: **Embedding 专用**凭据（优先读取）。
#:
#: 讯飞的「文本大模型 X1」与「Embedding」是**两项独立授权**，同一个 AppId 未必
#: 都开通——本机实测（2026-09-28）：一个 AppId 只通 X1 文本、另一个只通 Embedding。
#: 因此本模块优先读下面这组专用变量，把 Embedding 的凭据与文本模型**解耦**。
ENV_SPARK_EMBEDDING_APP_ID = "SPARK_EMBEDDING_APP_ID"
ENV_SPARK_EMBEDDING_API_KEY = "SPARK_EMBEDDING_API_KEY"
ENV_SPARK_EMBEDDING_API_SECRET = "SPARK_EMBEDDING_API_SECRET"

#: **回落组**：``SPARK_*`` 是**文本大模型 X1** 的凭据（见 ``main.SparkAPI``）。
#:
#: 专用变量未配置时按本组回落 ⇒ 对「只配了 ``SPARK_*``」的既有部署，
#: 行为与引入专用变量**之前完全一致**（符合本项目「不配 = 默认 = 行为不变」）。
#: 注意变量名**刻意不互为前缀**（``SPARK_EMBEDDING_APP_ID`` 不含子串
#: ``SPARK_APP_ID``），否则「消息里是否提到某个变量名」这类断言会恒真。
ENV_APP_ID = "SPARK_APP_ID"
ENV_API_KEY = "SPARK_API_KEY"
ENV_API_SECRET = "SPARK_API_SECRET"

#: 回落组的三个名字（报错时一并提示，便于排障时知道还有哪组可用）
ENV_FALLBACK_GROUP: Tuple[str, str, str] = (ENV_APP_ID, ENV_API_KEY, ENV_API_SECRET)

#: 手工覆盖 ``domain`` 的逃生口（正常情况由通用 role 决定）
ENV_DOMAIN = "EMBEDDING_SPARK_DOMAIN"

#: provider 取值（与 ``embedding_provider.PROVIDER_SPARK`` 同一个字符串）
PROVIDER_SPARK = "spark"

#: 讯飞 embedding 服务地址（**不含** query；签名后才会拼上凭据）。
#:
#: 末尾**不带** ``/``（与 ``embedding_provider.DEFAULT_BASE_URL`` 同款约定）：
#: 签名时的 path 会归一成 ``/``，因此带不带尾斜杠都能工作，但常量只留一种写法，
#: 免得「默认值」与「环境变量读进来的值」长得不一样。
DEFAULT_BASE_URL = "https://emb-cn-huabei-1.xf-yun.com"

#: 写进 ``knowledge_chunk.embedding_model`` 的模型标识。
#:
#: **必须与 ``hash-local`` / OpenAI 模型名区分开**：入库 Pipeline 用
#: ``embedding_model == embedder.name`` 判断「旧向量是否由当前模型产生」，
#: 标识相同就会被误判成「已有可用向量」而**跳过重算**（表现为检索质量莫名变差）。
DEFAULT_MODEL = "xinghuo-embedding"

#: 实测维度（``payload.feature.text`` 解出 10240 字节 = 2560 × float32）。
#:
#: 声明它（而不是写 ``0`` = 不校验）是刻意的：换模型 / 上游改协议时，
#: 维度不符会**当场报错**，而不是把 2560 维向量混进 1024 维的库里、
#: 最后表现为「检索不到」。
DEFAULT_DIMENSION = 2560

DEFAULT_TIMEOUT = 30.0

#: **上游硬约束**：官方文档明确「全链路请求会话时长不超过 1 分钟」。
#:
#: 本实现是「一条文本 = 一次短请求」，因此天然远低于该上限；
#: 但 ``EMBEDDING_TIMEOUT`` 若被配成 > 60s，只会得到「等到超时也拿不到结果」
#: 的假失败——所以在**配置期**直接拦下，而不是让它上线后表现为随机失败。
MAX_SESSION_SECONDS = 60.0

#: 上游只接受单条文本 ⇒ 没有原生批量（见模块文档）。
DEFAULT_DOMAIN = "para"

#: 讯飞的两种 ``domain``
DOMAIN_PARA = "para"
DOMAIN_QUERY = "query"

#: 通用 role → 讯飞 domain（**唯一**一处厂商词汇映射）
ROLE_TO_DOMAIN: Dict[str, str] = {
    ROLE_DOCUMENT: DOMAIN_PARA,
    ROLE_QUERY: DOMAIN_QUERY,
}

#: 请求头里的 ``uid``：讯飞用它做调用统计，非凭据
DEFAULT_UID = "career-ai"

#: ``header.code`` 已知取值的排查提示（未知码不臆测，只回显 message）
CODE_HINTS: Dict[int, str] = {
    10009: "输入非法（检查文本是否为空 / 超长）",
    10139: "参数错误（检查 domain 是否为 query / para）",
    10313: f"{ENV_SPARK_EMBEDDING_APP_ID} 与 {ENV_SPARK_EMBEDDING_API_KEY} 不匹配"
           f"（须同属一个应用；回落组 {' / '.join(ENV_FALLBACK_GROUP[:2])} 同理）",
    11200: f"未授权（检查 {ENV_SPARK_EMBEDDING_APP_ID}（回落 {ENV_APP_ID}），"
           f"以及该应用是否已开通 Embedding 服务）",
    11202: "上游 license 校验失败（**实测多为限流 / 并发过高**，稍后重试即可）",
}

#: 异常消息里回显 ``message`` 的最大长度
_SNIPPET_LIMIT = 200


# ============================================================
# 配置
# ============================================================
@dataclass(frozen=True)
class SparkEmbeddingConfig:
    """讯飞星火 Embedding 的配置（不可变）。

    ``api_key`` / ``api_secret`` 声明为 ``repr=False``：``dataclass`` 默认会把所有字段
    打进 ``repr``，一旦这个对象被日志打印就会**泄露密钥**（同 ``EmbeddingProviderConfig``）。
    """

    app_id: str = field(default="", repr=False)
    api_key: str = field(default="", repr=False)
    api_secret: str = field(default="", repr=False)
    base_url: str = DEFAULT_BASE_URL
    model: str = DEFAULT_MODEL
    dimension: int = DEFAULT_DIMENSION
    timeout: float = DEFAULT_TIMEOUT
    domain: str = DEFAULT_DOMAIN

    @property
    def is_remote(self) -> bool:
        """是否走真实网络调用（本类**只**表示远程配置，恒为 ``True``）。"""
        return True

    @property
    def masked_api_key(self) -> str:
        """打码后的 ``api_key``（只保留末 4 位），供日志 / 调试安全展示。"""
        return _mask(self.api_key)

    @property
    def masked_api_secret(self) -> str:
        """打码后的 ``api_secret``。"""
        return _mask(self.api_secret)


def _mask(secret: str) -> str:
    if not secret:
        return ""
    if len(secret) <= 4:
        return "*" * len(secret)
    return "*" * (len(secret) - 4) + secret[-4:]


def _resolve_domain(env: Mapping, role: str) -> str:
    """把通用 ``role`` 翻成讯飞 ``domain``；``ENV_DOMAIN`` 可显式覆盖。"""
    if role not in ROLE_TO_DOMAIN:
        raise EmbeddingProviderConfigError(
            f"未知 role：{role!r}；可选 {ROLE_DOCUMENT} / {ROLE_QUERY}"
            f"（role 是**与厂商无关**的说法，本模块负责翻成讯飞的 {DOMAIN_PARA} / {DOMAIN_QUERY}）"
        )
    raw = str(env.get(ENV_DOMAIN) or "").strip().lower()
    if not raw:
        return ROLE_TO_DOMAIN[role]
    if raw not in (DOMAIN_PARA, DOMAIN_QUERY):
        raise EmbeddingProviderConfigError(
            f"{ENV_DOMAIN} 取值非法：{raw!r}；可选 {DOMAIN_QUERY} / {DOMAIN_PARA}"
            f"（一般不需要设置本变量——它只是排障用的逃生口）"
        )
    return raw


def load_spark_config(
    env: Optional[Mapping] = None, *, role: str = ROLE_DOCUMENT
) -> SparkEmbeddingConfig:
    """从环境变量读讯飞 Embedding 配置（``env=None`` 时读 ``os.environ``）。

    通用旋钮（``EMBEDDING_BASE_URL`` / ``_MODEL`` / ``_DIMENSION`` / ``_TIMEOUT``）
    沿用 ``embedding_provider`` 的名字与解析函数——**同一处校验口径**，
    于是「配了个非法数字」在两条协议下的报错完全一致。

    :param role: 与厂商无关的角色（``document`` / ``query``），决定 ``domain``。
    :raises EmbeddingProviderConfigError: 缺凭据（专用组与回落组**都**取不到）/
        ``role`` 或 ``ENV_DOMAIN`` 取值非法 / 维度为负 / 超时非正。
    """
    source: Mapping = os.environ if env is None else env

    # 优先专用组，整项回落 ``SPARK_*``（文本模型 X1 的凭据）——逐项回落而不是整组：
    # 允许「只补一个 AppId」这种过渡配置，且不改变任何既有部署的行为。
    app_id = _read_str(source, ENV_SPARK_EMBEDDING_APP_ID, "") or _read_str(
        source, ENV_APP_ID, ""
    )
    api_key = _read_str(source, ENV_SPARK_EMBEDDING_API_KEY, "") or _read_str(
        source, ENV_API_KEY, ""
    )
    api_secret = _read_str(source, ENV_SPARK_EMBEDDING_API_SECRET, "") or _read_str(
        source, ENV_API_SECRET, ""
    )
    missing = [
        name
        for name, value in (
            (ENV_SPARK_EMBEDDING_APP_ID, app_id),
            (ENV_SPARK_EMBEDDING_API_KEY, api_key),
            (ENV_SPARK_EMBEDDING_API_SECRET, api_secret),
        )
        if not value
    ]
    if missing:
        raise EmbeddingProviderConfigError(
            f"{PROVIDER_SPARK} 需要 {' / '.join(missing)}"
            "（三项都必填，且只写在 backend/.env，禁止提交）；"
            f"也可整组回落用 {' / '.join(ENV_FALLBACK_GROUP)}（文本模型 X1 的凭据）；"
            f"离线环境请用 EMBEDDING_PROVIDER=hash"
        )

    domain = _resolve_domain(source, role)
    base_url = _read_str(source, ENV_BASE_URL, DEFAULT_BASE_URL).rstrip("/")
    model = _read_str(source, ENV_MODEL, DEFAULT_MODEL)
    dimension = _read_int(source, ENV_DIMENSION, DEFAULT_DIMENSION, minimum=0)
    timeout = _read_float(source, ENV_TIMEOUT, DEFAULT_TIMEOUT, minimum_exclusive=0)

    if not base_url:
        raise EmbeddingProviderConfigError(f"{ENV_BASE_URL} 不能为空")
    if not urlsplit(base_url).netloc:
        # 配置错必须在**构造期**暴露：否则要等到第一次调用、签名时才发现
        # （读侧还会把它静默降级成「无知识」，表现为「检索不到」）
        raise EmbeddingProviderConfigError(
            f"{ENV_BASE_URL} 必须是绝对地址（含 scheme 与 host），当前 {base_url!r}"
        )
    if not model:
        raise EmbeddingProviderConfigError(f"{ENV_MODEL} 不能为空")
    if timeout > MAX_SESSION_SECONDS:
        raise EmbeddingProviderConfigError(
            f"{ENV_TIMEOUT}={timeout} 超过上游上限 {MAX_SESSION_SECONDS:g}s"
            "（官方要求「全链路请求会话时长不超过 1 分钟」）；"
            f"本实现是「一条文本 = 一次短请求」，默认 {DEFAULT_TIMEOUT:g}s 已足够"
        )

    return SparkEmbeddingConfig(
        app_id=app_id,
        api_key=api_key,
        api_secret=api_secret,
        base_url=base_url,
        model=model,
        dimension=dimension,
        timeout=timeout,
        domain=domain,
    )


# ============================================================
# 协议：签名 / 请求体 / 响应解析（三个纯函数，可单独断言）
# ============================================================
def sign_url(
    base_url: str,
    *,
    api_key: str,
    api_secret: str,
    now: Optional[datetime] = None,
) -> str:
    """把 ``base_url`` 签成可直接 POST 的地址（凭据放 **query string**）。

    签名口径（与讯飞官方 SDK 一致，**逐字节可复现**）：

    .. code-block:: text

        date        = RFC1123(now)                      # 例：Wed, 27 Sep 2026 12:00:00 GMT
        tmp         = "host: {host}\\ndate: {date}\\nPOST {path} HTTP/1.1"
        signature   = base64(HMAC-SHA256(api_secret, tmp))
        origin      = 'api_key="{api_key}", algorithm="hmac-sha256", '
                      'headers="host date request-line", signature="{signature}"'
        authorization = base64(origin)
        → {scheme}://{host}{path}?authorization=…&date=…&host=…

    :param now: 签名时刻（默认 ``datetime.now()``）。**测试传固定值**即可逐字节比对；
        服务端允许 ±300s 偏差，因此生产无需同步时钟。
    """
    parts = urlsplit(base_url)
    host = parts.netloc
    path = parts.path or "/"
    if not host:
        raise EmbeddingProviderConfigError(
            f"{ENV_BASE_URL} 必须是绝对地址（含 host），当前 {base_url!r}"
        )

    date = format_date_time(mktime((now if now is not None else datetime.now()).timetuple()))
    signing_text = f"host: {host}\ndate: {date}\nPOST {path} HTTP/1.1"
    signature = base64.b64encode(
        hmac.new(api_secret.encode("utf-8"), signing_text.encode("utf-8"), digestmod=hashlib.sha256).digest()
    ).decode("ascii")
    authorization_origin = (
        f'api_key="{api_key}", algorithm="hmac-sha256", '
        f'headers="host date request-line", signature="{signature}"'
    )
    authorization = base64.b64encode(authorization_origin.encode("utf-8")).decode("ascii")
    query = urlencode({"authorization": authorization, "date": date, "host": host})
    return f"{parts.scheme}://{host}{path}?{query}"


def build_body(text: str, *, app_id: str, domain: str, uid: str = DEFAULT_UID) -> Dict[str, Any]:
    """构造讯飞 embedding 请求体（**单条**文本）。"""
    text_b64 = base64.b64encode(
        json.dumps(
            {"messages": [{"content": text, "role": "user"}]}, ensure_ascii=False
        ).encode("utf-8")
    ).decode("ascii")
    return {
        "header": {"app_id": app_id, "uid": uid, "status": 3},
        "parameter": {
            "emb": {
                "domain": domain,
                "feature": {"encoding": "utf8", "compress": "raw", "format": "plain"},
            }
        },
        "payload": {
            "messages": {
                "encoding": "utf8",
                "compress": "raw",
                "format": "json",
                "status": 3,
                "text": text_b64,
            }
        },
    }


def decode_feature_text(text: Any, *, where: str = "payload.feature.text") -> List[float]:
    """把 ``payload.feature.text`` 解成 ``List[float]``（base64 → 小端 float32）。

    :raises EmbeddingDimensionError: 不是字符串 / 空 / 字节数不是 4 的倍数。
        **不静默返回空列表**：那是「上游改了协议」的信号，必须响亮地失败。
    """
    if not isinstance(text, str) or not text.strip():
        raise EmbeddingDimensionError(
            f"{where} 缺失或为空，收到 {type(text).__name__}；"
            "上游返回结构可能已变更（期望 base64 的 float32 数组）"
        )
    # 不用 validate=True：上游可能带换行/空白，validate 会把它当成非法字符。
    # 代价是「非 base64 的字符会被忽略」，所以下面必须补字节数校验兜住。
    blob = base64.b64decode(text.strip(), validate=False)
    if not blob:
        raise EmbeddingDimensionError(
            f"{where} 不是合法的 base64（解出 0 字节）"
        )
    if len(blob) % 4:
        raise EmbeddingDimensionError(
            f"{where} 解出 {len(blob)} 字节，不是 4 的倍数 —— 不是 float32 数组"
        )
    return list(struct.unpack(f"<{len(blob) // 4}f", blob))


def _snippet(value: Any, *, limit: int = _SNIPPET_LIMIT) -> str:
    """把任意对象压成单行短文本（**只用于非凭据内容**：错误码 / message）。"""
    text = " ".join(str(value).split())
    return text if len(text) <= limit else text[:limit] + "…"


def _strip_credentials(text: str, url: str) -> str:
    """把签名 URL 的 query string 从 ``text`` 里抹掉。

    讯飞的凭据在 URL 里（``authorization`` = ``base64(api_key + signature)``，**可逆**），
    而默认传输在超时消息里会带上完整 URL ⇒ 必须整段抹掉。
    """
    base, sep, query = url.partition("?")
    if sep and len(query) >= 8 and query in text:
        text = text.replace(query, "***")
    return text.replace(url, base) if url in text else text


# ============================================================
# HTTP 传输层（可注入 → 无网络也能测）
# ============================================================
class SparkEmbeddingTransport(EmbeddingTransport):
    """默认传输（``httpx``）+ **把签名 URL 从异常消息里抹掉**。

    :param inner: 被包裹的传输实现（默认 :class:`HttpxEmbeddingTransport`）。
        测试可注入任意 ``post_json`` 对象，从而**完全不联网**。
    """

    def __init__(self, *, client_factory: Optional[Any] = None, inner: Any = None) -> None:
        if inner is not None and not callable(getattr(inner, "post_json", None)):
            raise EmbeddingProviderConfigError(
                "inner 必须提供可调用的 post_json(url, *, headers, payload, timeout)"
            )
        self._inner = inner if inner is not None else HttpxEmbeddingTransport(client_factory=client_factory)

    async def post_json(
        self,
        url: str,
        *,
        headers: Dict[str, str],
        payload: Dict[str, Any],
        timeout: float,
    ) -> Any:
        try:
            return await self._inner.post_json(
                url, headers=headers, payload=payload, timeout=timeout
            )
        except EmbeddingError as exc:
            safe = _strip_credentials(str(exc), url)
            if safe == str(exc):
                raise
            # 保持异常**类型**不变（调用方的 except 仍然有效），只换消息；
            # 原始异常挂在 __cause__ 上，排障时仍能拿到。
            raise type(exc)(safe) from exc


# ============================================================
# 真实 Provider
# ============================================================
class SparkEmbeddingProvider(EmbeddingService):
    """讯飞星火向量化实现：``POST {base_url}?authorization=…``（单条文本）。

    **只实现 ``_embed_one`` 钩子 + 覆盖 ``embed_batch``**，``embed(text)`` 一行未重写——
    接口与校验口径完全沿用 :class:`~services.embedding_service.EmbeddingService`。

    :param config: :class:`SparkEmbeddingConfig`。
    :param transport: HTTP 传输实现；``None`` → :class:`SparkEmbeddingTransport`。
    """

    #: 实例上会被 ``config.model`` 覆盖；``embedding_model`` 列记录的就是它
    name = DEFAULT_MODEL
    #: ``0`` = 不校验维度
    dimension = 0
    #: 真实模型产出的是**语义**向量 ⇒ 供 :func:`~services.embedding_service.describe_embedding` 上报。
    semantic_enabled = True

    def __init__(
        self,
        config: SparkEmbeddingConfig,
        *,
        transport: Optional[Any] = None,
    ) -> None:
        if not isinstance(config, SparkEmbeddingConfig):
            raise EmbeddingProviderConfigError(
                f"config 必须是 SparkEmbeddingConfig，收到 {type(config).__name__}"
            )
        missing = [
            name
            for name, value in (
                (ENV_APP_ID, config.app_id),
                (ENV_API_KEY, config.api_key),
                (ENV_API_SECRET, config.api_secret),
            )
            if not value
        ]
        if missing:
            raise EmbeddingProviderConfigError(
                f"{PROVIDER_SPARK} 需要 {' / '.join(missing)}"
            )
        if transport is not None and not callable(getattr(transport, "post_json", None)):
            raise EmbeddingProviderConfigError(
                "transport 必须提供可调用的 post_json(url, *, headers, payload, timeout)"
            )

        self.config = config
        self.name = config.model or self.name
        self.dimension = config.dimension
        self.domain = config.domain
        self._transport = transport if transport is not None else SparkEmbeddingTransport()

    # ------------------------------------------------------------
    # 请求
    # ------------------------------------------------------------
    @property
    def endpoint(self) -> str:
        """**未签名**的请求地址（不含凭据，可安全打印）。"""
        return self.config.base_url

    def signed_url(self, *, now: Optional[datetime] = None) -> str:
        """当前时刻的签名 URL（**含凭据，绝不进日志 / 异常消息**）。"""
        return sign_url(
            self.config.base_url,
            api_key=self.config.api_key,
            api_secret=self.config.api_secret,
            now=now,
        )

    def _headers(self) -> Dict[str, str]:
        return {"Content-Type": "application/json"}

    def _payload(self, text: str) -> Dict[str, Any]:
        return build_body(text, app_id=self.config.app_id, domain=self.domain)

    async def _request(self, text: str) -> List[float]:
        raw = await self._transport.post_json(
            self.signed_url(),
            headers=self._headers(),
            payload=self._payload(text),
            timeout=self.config.timeout,
        )
        return self._parse(raw)

    # ------------------------------------------------------------
    # 解析
    # ------------------------------------------------------------
    def _parse(self, raw: Any) -> List[float]:
        """把上游响应解析成一条向量；结构不对一律 ``EmbeddingDimensionError``。"""
        if not isinstance(raw, Mapping):
            raise EmbeddingDimensionError(
                f"{self.name} 返回结构不是对象，收到 {type(raw).__name__}"
            )

        header = raw.get("header")
        code = header.get("code") if isinstance(header, Mapping) else None
        if code != 0:
            # 讯飞把「业务错误」也放在 HTTP 200 里 ⇒ 不判 code 会把失败当成功。
            message = header.get("message") if isinstance(header, Mapping) else None
            hint = CODE_HINTS.get(code) if isinstance(code, int) and not isinstance(code, bool) else None
            raise EmbeddingUnavailableError(
                f"{self.name} 上游返回 code={code}：{_snippet(message)}"
                + (f"（{hint}）" if hint else "")
            )

        payload = raw.get("payload")
        feature = payload.get("feature") if isinstance(payload, Mapping) else None
        text = feature.get("text") if isinstance(feature, Mapping) else None
        values = decode_feature_text(text)
        # 复用基类的向量校验：非空 / 维度相符 / 显式拒 bool 与 str 元素
        return _require_vector(values, self.name, self.dimension)

    # ------------------------------------------------------------
    # 编码（接口部分：只实现钩子 + 覆盖批量）
    # ------------------------------------------------------------
    async def _embed_one(self, text: str) -> List[float]:
        return await self._request(text)

    async def embed_batch(self, texts: Sequence[str]) -> List[List[float]]:
        """批量编码：**上游只接受单条** ⇒ 逐条请求，返回顺序与入参一致。

        入参校验复用基类的 ``_require_texts``（同一处校验，避免覆盖后口径漂移），
        且**在任何 HTTP 调用之前**完成——非法入参不会产生一次网络请求。
        与默认实现（逐条 ``embed``）的差别只有一处：本方法**不重复做入参校验**，
        因此错误信息里的「第几条」由 ``_require_texts`` 统一给出。
        """
        items = _require_texts(texts)
        return [await self._request(item) for item in items]

    def __repr__(self) -> str:  # pragma: no cover - 便于调试打印
        return (
            f"<SparkEmbeddingProvider name={self.name!r} dimension={self.dimension} "
            f"domain={self.domain!r} base_url={self.config.base_url!r} "
            f"api_key={self.config.masked_api_key!r}>"
        )


# ============================================================
# 工厂
# ============================================================
def build_spark_embedding_service(
    *,
    env: Optional[Mapping] = None,
    transport: Optional[Any] = None,
    role: str = ROLE_DOCUMENT,
) -> SparkEmbeddingProvider:
    """按配置构造讯飞 Embedding 服务（``config`` 由本模块自己加载）。

    **不做全局单例**：每次调用返回新对象，由调用方持有（避免「悄悄换了模型」）。
    """
    config = load_spark_config(env, role=role)
    return SparkEmbeddingProvider(config, transport=transport)


__all__ = [
    "CODE_HINTS",
    "DEFAULT_BASE_URL",
    "DEFAULT_DIMENSION",
    "DEFAULT_DOMAIN",
    "DEFAULT_MODEL",
    "DEFAULT_TIMEOUT",
    "DEFAULT_UID",
    "DOMAIN_PARA",
    "DOMAIN_QUERY",
    "ENV_API_KEY",
    "ENV_API_SECRET",
    "ENV_APP_ID",
    "ENV_DOMAIN",
    "MAX_SESSION_SECONDS",
    "PROVIDER_SPARK",
    "ROLE_TO_DOMAIN",
    "SparkEmbeddingConfig",
    "SparkEmbeddingProvider",
    "SparkEmbeddingTransport",
    "build_body",
    "build_spark_embedding_service",
    "decode_feature_text",
    "load_spark_config",
    "sign_url",
]
