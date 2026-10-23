# -*- coding: utf-8 -*-
"""career 模块的**端口**：把「LLM 调用能力」以显式注册的方式交给业务层。

为什么需要这一层
----------------
``career`` 的四个端点里有三个要调大模型（``/api/chat``、``/api/resume/analyze``、
``/api/resume/parse``）。而 LLM 门面 ``spark_api`` 是 ``main.py`` 里的**进程级
单例**（``main.py: spark_api = build_legacy_spark_facade()``）。

业务层不能 ``import main``：
* 依赖方向反了（``services``/``modules`` 在 L2，``main`` 是 L9 装配层）；
* 会立刻形成硬环（``main`` → ``modules.career.api`` → ``main``）。

所以走**端口注册**：装配层（``main.py``）在挂载路由时把实例交进来，业务层只
通过 :func:`get_llm` 取。这样：

* 依赖方向仍是单向（``api`` → ``ports``，同包内）；
* 业务函数**不读全局变量**——``service`` 的签名显式接收 ``llm`` 参数，
  ``api`` 用 ``Depends(get_llm)`` 注入，测试可以直接传假对象；
* 只有一个注册点，没有「谁先 import 谁就先赢」的隐式初始化顺序。

为什么不在这里 ``build_legacy_spark_facade()`` 自己造一个
--------------------------------------------------------
能造，但会得到**第二个实例**：两个门面各自持有 provider、策略与指标计数器，
``/api/health`` 的 AI 指标就只统计到其中一半。单例的意义就是全进程一份。

★ 为什么放 ``ports.py`` 而不是 ``__init__.py``
---------------------------------------------
``__init__.py`` 要 ``from modules.career.api import router`` 做门面导出。如果
``get_llm`` 也在 ``__init__`` 里，``api.py`` 又反过来 ``from modules.career
import get_llm`` ⇒ ``modules.career <-> modules.career.api`` 硬环（auth 模块
在 P1 踩过同一个坑，见 ``modules/auth/api.py`` 顶部的注释）。单独一个
``ports.py`` 让边变成 ``api -> ports``，单向。
"""

from typing import Any, Optional

#: 已注册的 LLM 门面（鸭子类型：只需要 ``async chat_async(prompt) -> str``）
_LLM: Optional[Any] = None


def configure(llm: Any) -> None:
    """注册 LLM 门面。由 ``main.py`` 在挂载路由前调用一次。

    允许重复调用（幂等）：同一个实例重复注册没有副作用；换成另一个实例则
    以最后一次为准——这与「模块级单例」的既有语义一致。
    """
    global _LLM
    _LLM = llm


def get_llm() -> Any:
    """取已注册的 LLM 门面（供 ``Depends`` 注入）。

    ★ 未注册时**响亮报错**而不是返回 ``None`` 或造一个假的：静默降级会让
    线上变成「AI 永远返回空」，而日志里一条记录都没有。启动期就炸掉，
    才能在部署时发现问题。
    """
    if _LLM is None:
        raise RuntimeError(
            "career 模块的 LLM 门面尚未注册：main.py 必须在 include_router 之前"
            "调用 modules.career.ports.configure(spark_api)"
        )
    return _LLM


def reset() -> None:
    """清空注册（仅供测试使用）。"""
    global _LLM
    _LLM = None
