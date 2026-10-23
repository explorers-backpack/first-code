# -*- coding: utf-8 -*-
"""回归套件的**环境钉扎**：让 ``backend/tests`` 不受 ``backend/.env`` 的部署配置影响。

为什么需要它
------------
``database.py`` / ``main.py`` 在 import 期无条件 ``load_dotenv()``
（``override=False``），于是**任何 import 了 ``database`` / ``main`` 的套件**
都会把 ``.env`` 里的**部署取值**带进 ``os.environ``。已经踩到两次同类事故：

===================  ==========================================================
变量                  不钉扎时的后果
===================  ==========================================================
``EMBEDDING_PROVIDER``  断言「未配置 ⇒ 离线哈希占位」的套件当场失败；
                        更糟的是**其它套件真的去打讯飞接口**——消耗配额、
                        单条 ~0.3s、还会撞 ``code=11202 licc failed`` 限流
``RAG_TOP_K`` /         ``.env`` 里的 ``min_score`` 是**真实模型**标定的
``RAG_MIN_SCORE`` /     （0.83）；而套件跑在**离线哈希**上（分数尺度完全不同）
``RAG_MIN_SCORE_RATIO`` ⇒ 0.83 把结果**全部滤掉**，RAG 链路套件当场拿不到知识
                        （实测 3 套失败：``test_rag_interview`` /
                        ``test_rag_param_passthrough`` / ``test_rag_production_route``）；
                        相对阈值 α 同属部署配置，一并钉住
``VECTOR_STORE``        换后端会换**召回集合**（``chroma`` 是派生索引，权威行在、
                        索引空 ⇒ 检索静默返回 0 条）⇒ 断言「检索到 N 条」的套件
                        会**因部署配置**而红；空串恰好等于「未配置」⇒ 默认 ``sql``
``CHROMA_PATH``         ANN 索引目录。空串 ⇒ ``open_client`` 回落到
                        ``backend/data/chroma``（与未配置一致）；钉住是为了防止
                        将来有人往 ``.env`` 里指到某个已有索引目录、把测试串起来
``AVATAR_PROVIDER`` +   数字人通道。``AVATAR_PROVIDER`` 空串 = 未配置 = **Mock**
``AVATAR_IFLYTEK_*``    （离线）；若被设成 ``iflytek``，avatar 套件会去构造真实
                        讯飞 provider（缺凭据报错 / 真打网络耗配额）。凭据与端点
                        一并钉空 —— 空串对每个旋钮都等价于「未配置」
``INTERVIEW_AVATAR_``   数字人「LLM 单题出题」**默认开启**，会让数字人闭环套件
``LLM_QUESTIONS``       在**提交答案**时走一次大模型（本机 ``.env`` 带真实
                        ``SPARK_*`` ⇒ **真打星火**，单次 > 8s、耗配额、结果依赖
                        网络）。★ 空串在这里等价于「未配置」= **开启**，所以这是
                        **唯一一个必须钉成非空串（``"0"``）**的旋钮
===================  ==========================================================

回归套件必须**不受部署配置影响**（否则「本机绿、CI 红」且原因与代码无关），
所以统一在 import 期把这些变量**钉成空串**：

- ``load_dotenv(override=False)`` **不覆盖已存在的键** ⇒ 之后无论谁再 load 都不会灌回来；
- 对本项目的每个旋钮，**空串都恰好等价于「未配置」**，而这正是套件要断言的默认态：
  - ``EMBEDDING_PROVIDER`` 空 → 自动分支 → 无密钥 → ``HashEmbeddingService``（离线占位）；
  - ``RAG_TOP_K`` / ``RAG_MIN_SCORE`` 空 → ``resolve_retriever_defaults`` 返回 ``{}``
    ⇒ 检索器用自己的默认（``top_k=5`` / ``min_score=None``），与引入配置前**逐字节一致**。

★★ **唯一的例外是 ``INTERVIEW_AVATAR_LLM_QUESTIONS``**：该能力的「未配置」= **开启**
（见 ``services/interview_service.avatar_llm_questions_enabled``），所以空串**达不到**
「钉住」的目的，必须显式钉成 ``"0"``（= 显式关闭）。判据是「套件不得真打网络」，
而不是「值必须是空串」——这两条冲突时，前者优先。

为什么是「设成空串」而不是「pop 掉」
------------------------------------
``pop`` 之后 ``load_dotenv`` 会把 ``.env`` 的值**重新灌回来**（键不存在 ⇒ 会写入）。
设成空串则键**始终存在**，dotenv 永不覆盖，且空串本身就是这些旋钮文档化的
「未配置」写法。**这是本项目唯一正确的钉扎手法**，别改成 ``pop``。

用法
----
放在**所有项目模块 import 之前**，紧挨既有的 ``DATABASE_URL`` 那一行：

.. code-block:: python

    os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
    import regression_env  # noqa: E402,F401

本模块**只写 ``os.environ``、不 import 任何项目模块**（否则会与 import 顺序打架），
因此放在任何位置都安全（幂等、可重复调用）。

**新增会读部署配置的套件时，先把它加进 :data:`PINNED`**，再在套件里 import 本模块。
**不要**在需要真实模型的**联网运行器**（``tests/*_run.py``，不带 ``test_`` 前缀、
不进回归循环）里导入它——那些运行器要的正是真实配置。
"""

import os

#: 钉成空串（而非删除）：空串让 ``load_dotenv(override=False)`` 不再灌入，
#: 且对每个旋钮都恰好等于「未配置」。
#:
#: ``EMBEDDING_API_KEY`` 一并钉住，是为了防止将来有人往 ``.env`` 里加密钥后，
#: 自动分支又变成 ``openai``。
PINNED = {
    "EMBEDDING_PROVIDER": "",
    "EMBEDDING_API_KEY": "",
    "RAG_TOP_K": "",
    "RAG_MIN_SCORE": "",
    "RAG_MIN_SCORE_RATIO": "",
    # 向量后端：空串 = 未配置 = 默认 sql（**不改变任何既有断言**）；
    # 钉住是为了防「有人往 .env 里设了 VECTOR_STORE=chroma，回归全线变红」。
    "VECTOR_STORE": "",
    "CHROMA_PATH": "",
    # 数字人通道：``AVATAR_PROVIDER`` 空串 = 未配置 = **Mock**（离线、确定性）。
    # 钉住是为了防「有人往 .env 里设了 AVATAR_PROVIDER=iflytek」——那样 avatar
    # 相关套件会去构造真实讯飞 provider：轻则缺凭据报错、重则**真打网络**消耗配额。
    # 凭据 / 端点一并钉空（空串 = 未配置，``load_avatar_config`` 回落到默认占位）。
    "AVATAR_PROVIDER": "",
    "AVATAR_IFLYTEK_APP_ID": "",
    "AVATAR_IFLYTEK_API_KEY": "",
    "AVATAR_IFLYTEK_API_SECRET": "",
    "AVATAR_IFLYTEK_HOST": "",
    "AVATAR_IFLYTEK_BASE_PATH": "",
    "AVATAR_IFLYTEK_AVATAR_ID": "",
    "AVATAR_IFLYTEK_VCN": "",
    "AVATAR_TIMEOUT": "",
    # ★★ 唯一的**非空串**钉扎，且**必须**非空：数字人「LLM 单题出题」默认**开启**，
    #    而 ``main.spark_api`` 是**无条件构造**的模块级单例（``SparkAPI()`` 不看凭据
    #    是否存在）⇒ 只把 ``SPARK_*`` 钉空**没用**，``_default_spark()`` 照样拿得到
    #    真实客户端。空串在这里等价于「未配置」= **开启**，因此只能显式钉成 ``"0"``
    #    （等价于「显式关闭」）。
    #    不钉的后果：所有走数字人闭环的套件（``test_avatar_loop`` /
    #    ``test_interview_orchestrator`` / ``test_interview_ws`` /
    #    ``test_interview_production`` / ``test_interview_mode_channels`` …）
    #    都会在**提交答案**时真的去打星火：单次 > 8s、耗配额、结果依赖网络。
    #    开关的**两个状态**都由 ``tests/test_avatar_llm_flow.py`` 自己显式覆盖，
    #    所以这里钉住不会造成覆盖缺口。
    "INTERVIEW_AVATAR_LLM_QUESTIONS": "0",
    # 认证硬化：空串 = 未配置 = 默认值（TTL 7 天 / 限流 5 次/15 分钟）
    "AUTH_SESSION_TTL_SECONDS": "",
    "AUTH_LOGIN_MAX_FAILURES": "",
    "AUTH_LOGIN_WINDOW_SECONDS": "",
    # 登录失败限流的**存储后端**：空串 = 未配置 = ``memory``（进程内，行为不变）。
    # 钉住是为了防「有人为了本地多 worker 调试设了 AUTH_LOGIN_STORE=db 后忘了改回」——
    # 那样 ``test_auth_security [12]`` 用 ``utils.login_throttle.reset`` 清计数会
    # **清不到**共享表（它清的是内存字典），表现为"第 6 次没有 429"的假红。
    # 两个后端都由 ``tests/test_login_throttle_store.py`` 显式覆盖，钉住无覆盖缺口。
    "AUTH_LOGIN_STORE": "",
    # 实时通道的**历史兼容**凭证路径（``?token=<长期登录 token>``）。
    # 空串 = 未配置 = **默认开启**（= 改造前行为，既有套件一条断言都不用改）；
    # 钉住是为了防「有人用 ``INTERVIEW_WS_TOKEN_AUTH=0`` 跑本地服务后忘了改回」——
    # 那样 ``test_interview_ws`` / ``test_interview_ws_stream`` 里所有用
    # ``?token=`` 建连的用例会集体 4401，看起来像代码坏了，其实是环境脏了。
    # 开关的**两个状态**都由 ``tests/test_ws_ticket.py`` 自己显式覆盖。
    "INTERVIEW_WS_TOKEN_AUTH": "",
    # 撤销广播的**后台轮询器**（每实例一个 asyncio 任务，默认开启）。
    # ★ 这里**必须钉成 "0"（显式关闭）**，不能留空串：
    #   空串 = 未配置 = **开启** ⇒ 每个用 TestClient 的套件都会多出一个
    #   每 2 秒查一次库的后台任务，与套件自己的 asyncio.run()/临时 SQLite
    #   抢连接、并在关闭时留下未取消的任务 ⇒ 偶发假红且极难定位。
    #   它**不是**安全项的默认值，只是「主动断开有多快」；撤销本身照样落库，
    #   由 ``AUTH_RECHECK_SECONDS`` 的周期性复核兜底。
    #   两个状态（开 / 关）由 ``tests/test_ws_revocation.py`` 显式覆盖，
    #   并在那里**实测**撤销生效延迟，所以钉住不会造成覆盖缺口。
    "INTERVIEW_WS_REVOCATION_WATCH": "0",
    # ------------------------------------------------------------------
    # AI 接入层（modules.ai）——**回归套件必须与真实密钥彻底解耦**
    # ------------------------------------------------------------------
    # ★★ 必须钉成 "offline"（**非空串**）。空串 = 未配置 = 按凭据**自动判定**，
    #    而本机 ``.env`` 里带着真实 ``SPARK_*`` ⇒ 自动判定会落到 ``production``
    #    ⇒ 任何没打桩的 ``spark_api.chat_async`` 调用都会**真打星火**（单次 8~19s、
    #    耗配额、结果依赖网络）。钉成 offline 后 AI 调用一律**立即失败**，
    #    走各自的降级路径，套件既不联网也不依赖密钥。
    #    三种模式（mock / offline / production）与"生产不得静默回退"都由
    #    ``tests/test_ai_layer.py`` 显式覆盖，所以钉住不会造成覆盖缺口。
    "AI_RUN_MODE": "offline",
    # 空串 = 未配置 = 默认值（spark / 90s / 1 次 / 0.5s 退避）。
    # 钉住是为了防「本地调大 AI_LLM_MAX_ATTEMPTS 后忘了改回」把失败路径拖成 N 倍等待。
    "AI_LLM_PROVIDER": "",
    "AI_LLM_TIMEOUT": "",
    "AI_LLM_MAX_ATTEMPTS": "",
    "AI_LLM_BACKOFF_BASE": "",
}


def pin_regression_env(environ=None) -> None:
    """把回归环境钉到「未配置」状态（幂等，可重复调用）。

    :param environ: 要写入的映射，缺省写真实 ``os.environ``。传映射便于单测。
    """
    target = os.environ if environ is None else environ
    for key, value in PINNED.items():
        target[key] = value


pin_regression_env()
