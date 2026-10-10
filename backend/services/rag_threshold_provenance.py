# -*- coding: utf-8 -*-
"""RAG 检索阈值的**标定溯源**与**过期检测**（纯逻辑：零 DB、零网络、零项目依赖）。

要防的那件事
------------
``.env`` 里的检索阈值（``top_k`` / ``min_score`` / ``min_score_ratio``）是在
**特定 Embedding 模型 + 特定语料**上标定出来的。换了模型、换了语料，这些数字
**立刻失效**——但系统不会报错：它只是「一条都检索不到」或「注入一堆无关噪声」。
本项目的实测例子：哈希占位时代的 ``min_score=0.25`` 在真实模型下**一条都切不掉**；
而为长问句标定的 ``0.83`` 会让生产短词 query **命中 0**（等于把 RAG 关掉）。
两次都是**静默**的，靠人记「这两个数是怎么来的」显然不可靠。

本模块把「这套阈值是对着哪套模型 + 哪份语料标出来的」变成一份**可核对的值**
（:data:`PROVENANCE_FILENAME`），并提供 :func:`check_provenance` 给出过期判定。

它只做三件事（都是纯函数 / 纯 IO，便于单测）
-------------------------------------------
1. :func:`load_provenance` —— 读标定溯源产物；
2. :func:`corpus_fingerprint` —— 语料文件的 sha256（规范化换行后）；
3. :func:`check_provenance` —— 拿「产物里记的」与「现在实际的」逐项比对，
   给出 ``ok`` / ``stale`` / ``unknown`` 三态 + 人类可读的原因。

外加 :func:`derive_thresholds` —— 从标定产物**重新推导**阈值与可行区间
（供 ``scripts/apply_threshold_calibration.py`` 一键重标定使用）。

.. note::
   本模块**刻意不 import 任何项目模块**（连 ``knowledge_rag`` 都不 import）：
   「当前 Embedding 是什么」「当前阈值是多少」由调用方（``main.py``）作为
   **参数**传进来。这样它既不会成为 ``knowledge_rag`` 的新消费者
   （那会打破两处消费者闭集守卫），也不会在导入期产生任何副作用。
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

#: 标定溯源产物的文件名（默认放在 ``backend/scripts/`` 下，与标定产物同目录）。
PROVENANCE_FILENAME = "rag_threshold_provenance.json"

#: 参与「是否过期」比对的检索参数名（**与检索器的参数名一致**，不是环境变量名）。
PARAM_KEYS: Tuple[str, ...] = ("top_k", "min_score", "min_score_ratio")

#: :func:`check_provenance` 的三态。
STATUS_OK = "ok"            # 产物存在，且与当前模型/语料/配置一致
STATUS_STALE = "stale"      # 产物存在，但有明确的过期理由
STATUS_UNKNOWN = "unknown"  # 没有产物 / 产物损坏 ⇒ 无法判定（**不等于**没问题）

#: ``check_provenance`` 返回的固定键集（契约）。
CHECK_RESULT_KEYS: Tuple[str, ...] = (
    "status", "reasons", "notes", "expected", "current",
)

#: 浮点参数比对的容差。阈值都是 4~6 位小数，1e-9 足以区分「写错」与「格式化差异」。
_FLOAT_TOL = 1e-9


# ============================================================
# 路径
# ============================================================
def backend_dir() -> Path:
    """``backend/`` 目录（本文件在 ``backend/services/`` 下）。"""
    return Path(__file__).resolve().parents[1]


def default_provenance_path() -> Path:
    """默认的标定溯源产物路径：``backend/scripts/rag_threshold_provenance.json``。"""
    return backend_dir() / "scripts" / PROVENANCE_FILENAME


def resolve_corpus_path(recorded: Any) -> Optional[Path]:
    """把产物里记的语料路径解析成绝对路径。

    产物记的是**相对 ``backend/`` 的路径**（如 ``scripts/interview_knowledge.json``）
    ——这样整个仓库换机器 / 换盘符都不用改产物。绝对路径原样返回。
    """
    if not recorded:
        return None
    path = Path(str(recorded))
    return path if path.is_absolute() else backend_dir() / path


# ============================================================
# 读产物
# ============================================================
def load_provenance(path: Any = None) -> Optional[Dict[str, Any]]:
    """读标定溯源产物；**读不到 / 内容不是对象**时返回 ``None``（不抛异常）。

    刻意宽容：本函数会被启动日志与 ``GET /api/health`` 调用，那里**绝不能因为
    「文件缺失」或「JSON 坏了」而 500**——那属于「无法判定」，不是「服务故障」。
    """
    target = Path(path) if path is not None else default_provenance_path()
    try:
        raw = target.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def corpus_fingerprint(path: Any) -> Optional[str]:
    """语料文件的 sha256（**先把 CRLF 规范化成 LF**，与源码指纹同一口径）。

    为什么规范化换行：同一份语料在 Windows / Linux 上 checkout 出来的字节不同，
    不规范化会让「跨平台核对」变成误报。**读不到文件返回 ``None``**
    （= 无法核对，由调用方决定怎么表述，绝不是「通过」）。
    """
    if path is None:
        return None
    try:
        data = Path(path).read_bytes()
    except OSError:
        return None
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


# ============================================================
# 过期判定
# ============================================================
def _as_params(raw: Any) -> Dict[str, Any]:
    """把 ``{"top_k": …, "min_score": …}`` 之类的映射收成只含 :data:`PARAM_KEYS` 的 dict。"""
    if not isinstance(raw, Mapping):
        return {}
    return {key: raw[key] for key in PARAM_KEYS if key in raw}


def _same_number(left: Any, right: Any) -> bool:
    """数值比对：都当作 ``float`` 比，带容差；非数值则直接 ``==``。"""
    try:
        return abs(float(left) - float(right)) <= _FLOAT_TOL
    except (TypeError, ValueError):
        return left == right


def _fmt(value: Any) -> str:
    """给人看的数值格式（判定一律用全精度，这里只负责显示）。"""
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def check_provenance(
    provenance: Optional[Mapping[str, Any]],
    current: Mapping[str, Any],
) -> Dict[str, Any]:
    """比对「标定产物」与「当前实际」，返回固定 5 键的判定结果。

    :param provenance: :func:`load_provenance` 的返回值（``None`` = 没有产物）。
    :param current: 当前实际状态，形如::

            {
              "provider": "xinghuo-embedding",   # None = 无法确定
              "dimension": 2560,                 # None = 无法确定
              "corpus_sha256": "…",              # None = 无法核对（文件缺失）
              "params": {"top_k": 8, "min_score": 0.7685, "min_score_ratio": 0.9947},
            }

        ``params`` 里**缺失的键**按「该项未配置」处理，与产物记录不一致即算过期。

    :return: ``{"status", "reasons", "notes", "expected", "current"}``。

        - ``status``：:data:`STATUS_OK` / :data:`STATUS_STALE` / :data:`STATUS_UNKNOWN`
        - ``reasons``：**过期理由**（非空即 ``stale``）
        - ``notes``：**非致命提示**（如「语料文件读不到，未核对指纹」）——
          它**不**改变 ``status``，只是把「没能核对的部分」如实说出来
        - ``expected`` / ``current``：比对双方的关键字段快照（便于排查）

    .. warning::
        ``unknown`` **不等于**「没问题」。没有溯源产物时，本函数只能回答
        「我不知道这套阈值是对着什么标的」——把 ``unknown`` 说成 ``ok``
        正是本项目反复强调的那类「因错误的原因通过」。
    """
    current_params = _as_params((current or {}).get("params"))
    snapshot_current: Dict[str, Any] = {
        "provider": (current or {}).get("provider"),
        "dimension": (current or {}).get("dimension"),
        "corpus_sha256": (current or {}).get("corpus_sha256"),
        "params": current_params,
    }

    if not isinstance(provenance, Mapping):
        return {
            "status": STATUS_UNKNOWN,
            "reasons": [],
            "notes": [
                f"未找到标定溯源产物（{PROVENANCE_FILENAME}）⇒ 无法判定当前阈值是否仍适用。"
                "请用 scripts/apply_threshold_calibration.py 生成。"
            ],
            "expected": None,
            "current": snapshot_current,
        }

    reasons: List[str] = []
    notes: List[str] = []

    embedder = provenance.get("embedder") if isinstance(provenance.get("embedder"), Mapping) else {}
    corpus = provenance.get("corpus") if isinstance(provenance.get("corpus"), Mapping) else {}
    expected_params = _as_params(provenance.get("params"))

    # ---- 1. Embedding 模型 ----
    exp_provider = embedder.get("provider")
    exp_dimension = embedder.get("dimension")
    cur_provider = snapshot_current["provider"]
    cur_dimension = snapshot_current["dimension"]
    if cur_provider is None or cur_dimension is None:
        notes.append("无法确定当前 Embedding（配置非法？）⇒ 已跳过模型核对。")
    else:
        if exp_provider is not None and cur_provider != exp_provider:
            reasons.append(
                f"Embedding 已变：阈值是对着 {exp_provider} 标的，当前是 {cur_provider}。"
            )
        if exp_dimension is not None and cur_dimension != exp_dimension:
            reasons.append(
                f"Embedding 维度已变：标定时 {exp_dimension} 维，当前 {cur_dimension} 维。"
            )

    # ---- 2. 语料指纹 ----
    exp_sha = corpus.get("sha256")
    cur_sha = snapshot_current["corpus_sha256"]
    if exp_sha is None:
        notes.append("溯源产物未记录语料指纹 ⇒ 未核对语料是否变过。")
    elif cur_sha is None:
        notes.append("语料文件读不到 ⇒ 未核对语料是否变过。")
    elif cur_sha != exp_sha:
        reasons.append(
            f"语料已变（sha256 {exp_sha[:12]}… → {cur_sha[:12]}…）。"
        )

    # ---- 3. 配置值 vs 溯源记录 ----
    for key in PARAM_KEYS:
        if key not in expected_params:
            continue
        if key not in current_params:
            reasons.append(f"检索参数 {key} 未配置，而标定时是 {_fmt(expected_params[key])}。")
        elif not _same_number(current_params[key], expected_params[key]):
            reasons.append(
                f"检索参数 {key} = {_fmt(current_params[key])}，"
                f"与溯源记录的 {_fmt(expected_params[key])} 不一致（可能被手工改过）。"
            )

    snapshot_expected = {
        "provider": exp_provider,
        "dimension": exp_dimension,
        "corpus_sha256": exp_sha,
        "params": expected_params,
        "generated_at": provenance.get("generated_at"),
    }
    return {
        "status": STATUS_STALE if reasons else STATUS_OK,
        "reasons": reasons,
        "notes": notes,
        "expected": snapshot_expected,
        "current": snapshot_current,
    }


# ============================================================
# 从标定产物重新推导（供一键重标定脚本使用）
# ============================================================
class DerivationError(ValueError):
    """标定产物缺字段 / 结构不符，无法推导（属「入参问题」，改产物即可，不该重试）。"""


def _top1(landscape: Sequence[Mapping[str, Any]]) -> Optional[float]:
    """一条 query 的 top1 分数（空地形返回 ``None``）。"""
    if not landscape:
        return None
    first = landscape[0]
    score = first.get("score") if isinstance(first, Mapping) else None
    try:
        return float(score)
    except (TypeError, ValueError):
        return None


def derive_thresholds(calibration: Mapping[str, Any]) -> Dict[str, Any]:
    """从 R8 标定产物推导「绝对下限」与「相对阈值 α」的**可行区间 + 推荐值**。

    输入是 ``tests/rag_production_query_calibration_spark_run.py`` 的产物
    （``scripts/rag_production_query_calibration_spark.json``）。

    ``min_score``（绝对下限）
    ------------------------
    规则：**夹在「语料未覆盖主题的最高 top1」与「语料已覆盖主题的最低 top1」之间**，
    取窗口中点。语义上：

    - 未覆盖主题的 top1 若被放进来，等于「凭相对值硬凑出一堆无关片段」；
    - 已覆盖主题的 top1 若被切掉，等于「有知识却检索不到」。

    因此可行区间是 ``(未覆盖最高, 已覆盖最低]``（下界开、上界闭，与
    ``score >= min_score`` 的过滤语义一致）。若某一侧不存在（所有主题都有覆盖、
    或全都没覆盖），返回 ``None`` 并说明原因——**不猜**。

    ``min_score_ratio``（相对阈值 α）
    --------------------------------
    直接取产物 ``combined_alpha`` 的 ``(lo_exclusive, hi_inclusive]`` 与中点。

    :raises DerivationError: 产物里没有可用的 ``short_query.topics`` 或 ``combined_alpha``。
    """
    if not isinstance(calibration, Mapping):
        raise DerivationError("标定产物必须是 JSON 对象")

    topics = ((calibration.get("short_query") or {}).get("topics")) or {}
    if not isinstance(topics, Mapping) or not topics:
        raise DerivationError("标定产物缺少 short_query.topics（无法推导绝对下限）")

    uncovered: List[Tuple[str, float]] = []
    covered: List[Tuple[str, float]] = []
    for name, item in topics.items():
        landscape = (item or {}).get("landscape") if isinstance(item, Mapping) else None
        if not isinstance(landscape, list):
            continue
        top1 = _top1(landscape)
        if top1 is None:
            continue
        has_target = any(bool(h.get("is_target")) for h in landscape if isinstance(h, Mapping))
        (covered if has_target else uncovered).append((str(name), top1))

    if uncovered and covered:
        lo = max(score for _, score in uncovered)
        hi = min(score for _, score in covered)
        if hi <= lo:
            absolute = {
                "lo_exclusive": lo, "hi_inclusive": hi, "recommended": None,
                "reason": "「未覆盖主题最高 top1」不低于「已覆盖主题最低 top1」⇒ 单一绝对下限不可行",
            }
        else:
            absolute = {
                "lo_exclusive": lo, "hi_inclusive": hi, "recommended": (lo + hi) / 2.0,
                "lo_topic": max(uncovered, key=lambda pair: pair[1])[0],
                "hi_topic": min(covered, key=lambda pair: pair[1])[0],
                "rule": "夹在「语料未覆盖主题的最高 top1」与「语料已覆盖主题的最低 top1」之间，取中点",
            }
    else:
        absolute = {
            "lo_exclusive": None, "hi_inclusive": None, "recommended": None,
            "reason": (
                "全部主题都有覆盖（没有「未覆盖」参照）" if not uncovered
                else "没有任何主题被覆盖（没有「已覆盖」参照）"
            ),
        }

    combined = calibration.get("combined_alpha") or {}
    if not isinstance(combined, Mapping) or combined.get("lo_exclusive") is None:
        raise DerivationError("标定产物缺少 combined_alpha.lo_exclusive（无法推导相对阈值）")
    alpha = {
        "lo_exclusive": float(combined["lo_exclusive"]),
        "hi_inclusive": float(combined["hi_inclusive"]),
        "recommended": (
            float(combined["midpoint"]) if combined.get("midpoint") is not None
            else (float(combined["lo_exclusive"]) + float(combined["hi_inclusive"])) / 2.0
        ),
        "scenarios": list(combined.get("scenarios") or []),
        "rule": "各场景可行 α 区间的交集，取中点",
    }

    return {
        "min_score": absolute,
        "min_score_ratio": alpha,
        "corpus": {
            "documents": ((calibration.get("short_query") or {}).get("total_candidates")),
        },
    }


def within(value: Any, lo_exclusive: Any, hi_inclusive: Any) -> bool:
    """判定 ``value`` 是否落在 ``(lo_exclusive, hi_inclusive]`` 内（**全精度**）。

    :func:`derive_thresholds` 给出的区间就是这个语义：下界开（等于下界会让
    「未覆盖主题」通过）、上界闭（等于上界仍能保住「已覆盖主题」）。
    """
    try:
        v = float(value)
        lo = float(lo_exclusive)
        hi = float(hi_inclusive)
    except (TypeError, ValueError):
        return False
    return lo < v <= hi


__all__ = [
    "CHECK_RESULT_KEYS",
    "DerivationError",
    "PARAM_KEYS",
    "PROVENANCE_FILENAME",
    "STATUS_OK",
    "STATUS_STALE",
    "STATUS_UNKNOWN",
    "backend_dir",
    "check_provenance",
    "corpus_fingerprint",
    "default_provenance_path",
    "derive_thresholds",
    "load_provenance",
    "resolve_corpus_path",
    "within",
]
