# -*- coding: utf-8 -*-
"""把「这次 RAG 到底发生了什么」判成一个**明确的状态**，而不是靠调用方猜。

要解决的那件事
--------------
出题链与追问链的降级约定是「**检索失败不算失败**」：``ok`` 不受影响，只在
``warnings`` 里记一条 ``knowledge_retrieval_failed``，并返回 ``knowledge_chunks=0``。
这个约定本身是对的（面试不能因为向量库抖动就中断），但它把三种**完全不同**的
情况压成了同一个可观测形态：

======================  =================  ==========================  ============================
情况                     ``knowledge_chunks``  ``warnings``                真实含义
======================  =================  ==========================  ============================
**没开 RAG**             0                  无                          Prompt 里本来就不该有知识
**开了、真的零命中**     0                  无                          检索链路正常，只是库里没有相关内容
**开了、检索故障**       0                  ``knowledge_retrieval_failed``  链路坏了，**知识本该有却没进来**
======================  =================  ==========================  ============================

后两者在响应体里**同形**（都是 0 条），差别只在 ``warnings`` 一项；而「没开 RAG」
与「零命中」的差别**根本不在响应里**，必须结合请求的 ``use_rag`` 才能分辨。
于是最危险的那种——**向量库挂了、没人发现、面试题一路在无知识状态下生成**——
在日志里**一条记录都没有**（``api/interview.py`` 此前只对异常落日志，
而检索失败是被 core 吞掉后当成正常返回值返回的）。

本模块只做一件事：把 ``(use_rag, knowledge_chunks, warnings)`` 这**三个已经存在**的
信号判成四态之一，并给出「该不该告警」。它**不改变任何行为**——不改检索、
不改 Prompt、不改响应字段；调用方拿它去**落日志 / 打点 / 决定是否告警**。

四态（:data:`RAG_STATE_OFF` / ``FAILED`` / ``EMPTY`` / ``INJECTED``）
-------------------------------------------------------------------
.. code-block:: text

    use_rag=False ─────────────────────────────────► off       （未启用，不算降级）
    use_rag=True  ── 有失败告警 ──────────────────► failed    （★ 降级，必须告警）
                  └─ 无失败告警 ─┬─ chunks > 0 ──► injected  （正常命中）
                                 └─ chunks == 0 ─► empty     （正常零命中）

判据的优先级刻意是「**先看失败告警，再看条数**」：检索失败时 ``chunks`` 必然是 0，
反过来「chunks == 0」却**不能**推出失败。把优先级写反就会把故障说成「零命中」——
这正是本模块要防的那个错误。

刻意不做
--------
- **不 import 任何项目模块**（连 ``interview_core`` 都不 import）：失败告警码由调用方
  作为参数传入（默认值见 :data:`DEFAULT_FAILURE_CODES`），这样它既不会成为
  ``interview_core`` 的新消费者，也不会在导入期产生副作用。
- **不改任何行为**：纯函数，只读入参、只返回新 dict，不写库、不打日志、不发请求。
- **不猜**：``use_rag`` 不是 ``bool`` 就抛 ``RagDiagnosisError``——「不知道开没开」
  与「没开」是两件事，混起来会得出「一切正常」的假结论。
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Sequence, Tuple

# ============================================================
# 四态常量
# ============================================================
#: 未启用 RAG（请求里 ``use_rag=False``）。**不是降级**：Prompt 里本来就没有知识。
RAG_STATE_OFF = "off"

#: 检索**故障**（开了 RAG，但链路抛错并被降级为「无知识」）。★ 这是唯一的告警态。
RAG_STATE_FAILED = "failed"

#: 检索**成功但零命中**（链路正常，只是没有相关内容/阈值过严）。**不是故障**。
RAG_STATE_EMPTY = "empty"

#: 检索**成功且命中**（本次真正注入 Prompt 的切片条数 > 0）。
RAG_STATE_INJECTED = "injected"

#: 全部合法状态（闭集，便于守卫断言「没有第五种」）。
RAG_STATES: Tuple[str, ...] = (
    RAG_STATE_OFF,
    RAG_STATE_FAILED,
    RAG_STATE_EMPTY,
    RAG_STATE_INJECTED,
)

#: 判定结果的**恒定键集**（顺序即 :func:`classify_rag_outcome` 的插入顺序）。
DIAGNOSIS_FIELDS: Tuple[str, ...] = (
    "state",
    "degraded",
    "alert",
    "use_rag",
    "chunks",
    "failure_code",
    "reason",
)

#: 默认的「检索失败」告警码。与 ``interview_core.WARNING_KNOWLEDGE_FAILED`` /
#: ``interview_followup.WARNING_KNOWLEDGE_FAILED`` **同值**，但刻意不 import 它们
#: （见模块文档「刻意不做」）。调用方可显式传入自己的码表。
DEFAULT_FAILURE_CODES: Tuple[str, ...] = ("knowledge_retrieval_failed",)

#: 各状态的可读原因（**固定文案**，便于日志聚合与前端展示）
_REASONS: Dict[str, str] = {
    RAG_STATE_OFF: "未启用 RAG（use_rag=False），Prompt 不含知识",
    RAG_STATE_FAILED: "RAG 检索失败，已降级为「无知识」继续出题",
    RAG_STATE_EMPTY: "RAG 检索成功但零命中（库中无相关内容或阈值过严）",
    RAG_STATE_INJECTED: "RAG 检索成功，已注入知识切片",
}


class RagDiagnosisError(ValueError):
    """判定入参不合法（``use_rag`` 非 bool / ``chunks`` 非法）。

    归 ``ValueError``：调用方改代码就能解决，重试无用。**绝不**把非法入参
    悄悄当成「没开 RAG」——那会把配置错误伪装成「一切正常」。
    """


# ============================================================
# 入参归一
# ============================================================
def _require_bool(value: Any, name: str) -> bool:
    """严格 bool（**显式拒 ``0`` / ``1`` / ``"false"``**）。

    这里刻意不接受「真值」：``"false"`` 在 Python 里是真值，放进来会把
    「显式关闭 RAG」判成「开启」，正好判反。
    """
    if isinstance(value, bool):
        return value
    raise RagDiagnosisError(
        f"{name} 必须是 bool（True/False），收到 {type(value).__name__}: {value!r}"
    )


def _require_count(value: Any, name: str) -> int:
    """严格非负整数（**显式拒 ``bool``**——``bool`` 是 ``int`` 子类，项目老坑）。"""
    if isinstance(value, bool) or not isinstance(value, int):
        raise RagDiagnosisError(
            f"{name} 必须是非负整数，收到 {type(value).__name__}: {value!r}"
        )
    if value < 0:
        raise RagDiagnosisError(f"{name} 必须 >= 0，收到 {value}")
    return value


def _as_codes(values: Optional[Iterable[Any]]) -> Tuple[str, ...]:
    """把告警序列归一成非空字符串元组（``None`` → 空元组）。"""
    if values is None:
        return ()
    if isinstance(values, (str, bytes, bytearray)):
        # 传了单个字符串：包成一项，而不是按字符拆开（拆开永远匹配不上）
        return (values.decode("utf-8", "replace") if isinstance(values, bytes) else str(values),)
    result = []
    for item in values:
        if item is None:
            continue
        text = str(item).strip()
        if text:
            result.append(text)
    return tuple(result)


# ============================================================
# 主判定
# ============================================================
def classify_rag_outcome(
    *,
    use_rag: Any,
    knowledge_chunks: Any,
    warnings: Optional[Sequence[Any]] = None,
    failure_codes: Sequence[str] = DEFAULT_FAILURE_CODES,
) -> Dict[str, Any]:
    """把三个既有信号判成四态之一，返回 ``DIAGNOSIS_FIELDS`` 恒定键集。

    :param use_rag: 本次调用是否请求了 RAG（**必须**是 ``bool``）。
    :param knowledge_chunks: 本次**真正注入 Prompt** 的切片条数（必须是非负整数）。
    :param warnings: 结果里的 ``warnings`` 列表（可为 ``None``）。
    :param failure_codes: 视为「检索失败」的告警码集合。默认
        :data:`DEFAULT_FAILURE_CODES`；调用方可传自己的码表（如追问链的码）。

    返回字典（顺序固定）：

    - ``state``：四态之一（``off`` / ``failed`` / ``empty`` / ``injected``）
    - ``degraded``：是否**发生了降级**（只有 ``failed`` 为 ``True``）
    - ``alert``：是否**应当告警**（只有 ``failed`` 为 ``True``；与 ``degraded`` 同值，
      但语义不同——将来若出现「值得告警但不算降级」的情况，两者会分叉）
    - ``use_rag``：回显入参
    - ``chunks``：回显入参
    - ``failure_code``：命中的失败告警码（未命中为 ``""``）
    - ``reason``：固定可读文案

    .. note::
       判据顺序是「**先看失败告警，再看条数**」。反过来的话，
       「检索失败 ⇒ chunks 必然是 0」就会被误判成「零命中」，故障从此不可见。
    """
    requested = _require_bool(use_rag, "use_rag")
    chunks = _require_count(knowledge_chunks, "knowledge_chunks")

    codes = _as_codes(failure_codes)
    warning_list = _as_codes(warnings)
    hit = ""
    for warning in warning_list:
        if warning in codes:
            hit = warning
            break

    if not requested:
        state = RAG_STATE_OFF
    elif hit:
        # ★ 失败优先于条数：chunks 此时必然是 0，先判条数就会把故障说成「零命中」
        state = RAG_STATE_FAILED
    elif chunks > 0:
        state = RAG_STATE_INJECTED
    else:
        state = RAG_STATE_EMPTY

    return {
        "state": state,
        "degraded": state == RAG_STATE_FAILED,
        "alert": state == RAG_STATE_FAILED,
        "use_rag": requested,
        "chunks": chunks,
        "failure_code": hit,
        "reason": _REASONS[state],
    }


def describe_rag_outcome(diagnosis: Dict[str, Any]) -> str:
    """把判定结果渲染成**一行日志文案**（便于 grep / 聚合）。

    形如：``rag_state=failed degraded=True chunks=0 code=knowledge_retrieval_failed``。
    刻意不掺时间戳 / 随机量：同一输入永远同一输出，测试可直接逐字节比对。
    """
    state = str(diagnosis.get("state", ""))
    degraded = bool(diagnosis.get("degraded", False))
    chunks = diagnosis.get("chunks", 0)
    code = str(diagnosis.get("failure_code", "") or "-")
    return f"rag_state={state} degraded={degraded} chunks={chunks} code={code}"


__all__ = [
    "DEFAULT_FAILURE_CODES",
    "DIAGNOSIS_FIELDS",
    "RAG_STATES",
    "RAG_STATE_EMPTY",
    "RAG_STATE_FAILED",
    "RAG_STATE_INJECTED",
    "RAG_STATE_OFF",
    "RagDiagnosisError",
    "classify_rag_outcome",
    "describe_rag_outcome",
]
