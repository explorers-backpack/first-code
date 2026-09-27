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
===================  ==========================================================

回归套件必须**不受部署配置影响**（否则「本机绿、CI 红」且原因与代码无关），
所以统一在 import 期把这些变量**钉成空串**：

- ``load_dotenv(override=False)`` **不覆盖已存在的键** ⇒ 之后无论谁再 load 都不会灌回来；
- 对本项目的每个旋钮，**空串都恰好等价于「未配置」**，而这正是套件要断言的默认态：
  - ``EMBEDDING_PROVIDER`` 空 → 自动分支 → 无密钥 → ``HashEmbeddingService``（离线占位）；
  - ``RAG_TOP_K`` / ``RAG_MIN_SCORE`` 空 → ``resolve_retriever_defaults`` 返回 ``{}``
    ⇒ 检索器用自己的默认（``top_k=5`` / ``min_score=None``），与引入配置前**逐字节一致**。

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
}


def pin_regression_env(environ=None) -> None:
    """把回归环境钉到「未配置」状态（幂等，可重复调用）。

    :param environ: 要写入的映射，缺省写真实 ``os.environ``。传映射便于单测。
    """
    target = os.environ if environ is None else environ
    for key, value in PINNED.items():
        target[key] = value


pin_regression_env()
