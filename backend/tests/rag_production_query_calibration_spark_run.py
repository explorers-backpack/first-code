# -*- coding: utf-8 -*-
"""R8 · **生产 query 形态下的分数地形 + 相对阈值 ``α`` 标定**（**非套件**运行器）。

回答的问题
----------
任务 79 把 ``RAG_MIN_SCORE`` 从 0.25 标到 **0.83**，但那是**长问句** query
（``"MySQL 索引"`` / ``"分布式缓存一致性"``）标出来的；生产真实 query 是
``current_topic(plan, context)`` 推出的 ``plan.priority_topics`` —— **岗位技能短词**
（``"Python"`` / ``"MySQL"`` / ``"Redis"``）。任务 80（R7）实测：短词 top1 只有
0.766~0.777 ⇒ **0.83 下命中 0**。

**绝对阈值不可能同时适配两种形态**（长问句 0.83~0.85、短词 0.77）⇒ 本运行器把
两种形态放在**同一把尺子**上，标定**相对阈值** ``min_score = α × top1`` 的可行区间。

与 R7 的关系
------------
- 复用 R7 的 ``_snapshot_production`` / ``_row_dict``（**只读**生产库、原样复制向量）。
- 长问句场景**不重跑上游**：直接读 ``scripts/rag_min_score_sweep_spark.json``
  里的 ``score_landscape``（任务 79 已产出的全量 32 条排名）。
- 只对**短词 query** 调上游（3 个 topic + 2 个备选 query 形态）。

**判「目标片」的口径**：``topic.lower() in source.lower()``
（``MySQL`` → ``handbook://mysql/*``、``Redis`` → ``handbook://redis/*``；
``Python`` 在语料里**没有覆盖** ⇒ 天然无目标片，用来验证「无覆盖时的行为」）。

用法
----
::

    python tests/rag_production_query_calibration_spark_run.py
    python tests/rag_production_query_calibration_spark_run.py --out scripts/xxx.json

**联网 + 真实凭据 + 少量配额**（短词侧约 9~15 次）。**不要**加进回归循环。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import pathlib
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))
sys.path.insert(0, str(BACKEND_DIR / "tests"))

#: ★ 刻意**不** import ``regression_env``：本运行器要的就是真实配置。
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base, engine as prod_engine  # noqa: E402
from models import Job, KnowledgeChunk as OrmChunk, KnowledgeDocument as OrmDoc  # noqa: E402
from rag_production_chain_spark_run import (  # noqa: E402
    _row_dict,
    _snapshot_production,
)
from rag_min_score_calibration_spark_run import PacedCachingEmbedder  # noqa: E402
from services import interview_core, knowledge_rag  # noqa: E402

DEFAULT_OUT = BACKEND_DIR / "scripts" / "rag_production_query_calibration_spark.json"
SWEEP_JSON = BACKEND_DIR / "scripts" / "rag_min_score_sweep_spark.json"

#: 相对阈值候选（``min_score = α × top1``）。
ALPHA_GRID: Tuple[float, ...] = (
    1.0, 0.9999, 0.9995, 0.999, 0.998, 0.997, 0.996, 0.995,
    0.99, 0.985, 0.98, 0.97, 0.96, 0.95, 0.94, 0.92, 0.90,
)

#: 绝对阈值候选（对照）。
ABS_GRID: Tuple[float, ...] = (0.83, 0.80, 0.78, 0.77, 0.76, 0.75, 0.70)

#: 备选 query 形态（生产 baseline 之外）。
QUERY_FORMS: Tuple[Tuple[str, str], ...] = (
    ("baseline", "{topic}"),
    ("topic_phrase", "关于{topic}的核心知识点"),
    ("job_plus_topic", "{job} {topic}"),
)

_JOB_NAME_FALLBACK = "后端开发工程师"


def _fmt(value: Optional[float], digits: int = 6) -> str:
    return "-" if value is None else f"{value:.{digits}f}"


def _profile(landscape: Sequence[Dict[str, Any]], min_score: Optional[float]) -> Dict[str, Any]:
    """在**全量排名**上套一个绝对阈值（``select_matches`` 是「排序 → min_score → top_k」）。"""
    passed = [row for row in landscape
              if min_score is None or row["score"] >= min_score]
    return {
        "min_score": min_score,
        "hits": len(passed),
        "targets_kept": sum(1 for r in passed if r["is_target"]),
        "noise_kept": sum(1 for r in passed if not r["is_target"]),
    }


def _alpha_profile(landscape: Sequence[Dict[str, Any]], alpha: float) -> Dict[str, Any]:
    """套相对阈值 ``α × top1``（top1 为空时返回 0 命中）。"""
    if not landscape:
        return {"alpha": alpha, "threshold": None, "hits": 0,
                "targets_kept": 0, "noise_kept": 0}
    threshold = alpha * landscape[0]["score"]
    return {"alpha": alpha, "threshold": round(threshold, 6),
            **_profile(landscape, threshold)}


def _geometry(landscape: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """分数地形：目标下限 / 噪声上限 / 窗口 / 可行 α 区间。

    - 可行 α 需要 ``α × top1 > noise_ceiling``（**严格大于**，因为过滤是 ``>=``）
      且 ``α × top1 <= target_floor``。
    - 无目标片（语料未覆盖）时 ``feasible_alpha`` 为 ``None`` —— **结构性无法用阈值救**。
    """
    if not landscape:
        return {"total": 0, "top1": None, "target_floor": None,
                "noise_ceiling": None, "window": None, "separable": False,
                "feasible_alpha": None}
    top1 = landscape[0]["score"]
    targets = [r["score"] for r in landscape if r["is_target"]]
    noise = [r["score"] for r in landscape if not r["is_target"]]
    target_floor = min(targets) if targets else None
    noise_ceiling = max(noise) if noise else None

    window = None
    separable = False
    feasible: Optional[Dict[str, Any]] = None
    if target_floor is not None and noise_ceiling is not None:
        window = target_floor - noise_ceiling
        separable = window > 0
        lo = noise_ceiling / top1          # 必须**大于**它
        hi = target_floor / top1           # 可以**等于**它
        if hi > lo:
            feasible = {
                "lo_exclusive": lo,
                "hi_inclusive": hi,
                "midpoint": (lo + hi) / 2.0,
                "lo_display": round(lo, 6),
                "hi_display": round(hi, 6),
                "midpoint_display": round((lo + hi) / 2.0, 6),
            }
    return {
        "total": len(landscape),
        "targets": len(targets),
        "noise": len(noise),
        "top1": round(top1, 6),
        "target_floor": None if target_floor is None else round(target_floor, 6),
        "noise_ceiling": None if noise_ceiling is None else round(noise_ceiling, 6),
        "window": None if window is None else round(window, 6),
        "separable": separable,
        "feasible_alpha": feasible,
    }


async def _build_env() -> Tuple[Any, Any, List[Dict[str, Any]], List[Dict[str, Any]]]:
    """内存 SQLite + 生产向量原样复制 ⇒ ``(engine, factory, docs, chunks)``。"""
    snapshot = await _snapshot_production()
    docs, chunks = snapshot["documents"], snapshot["chunks"]
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as db:
        for row in docs:
            db.add(OrmDoc(**row))
        await db.flush()
        for row in chunks:
            db.add(OrmChunk(**row))
        db.add(Job(job_name=_JOB_NAME_FALLBACK, salary="20-35K", edu_require="本科",
                   major_require="不限", skills="Python,MySQL,Redis",
                   duty="负责后端服务的设计与开发", city="深圳", industry="互联网"))
        await db.commit()
    return engine, factory, docs, chunks


def _landscape_from_hits(hits: Sequence[Any], topic: str) -> List[Dict[str, Any]]:
    needle = topic.lower()
    rows = []
    for hit in hits:
        source = getattr(hit, "source", "") or ""
        rows.append({
            "score": float(getattr(hit, "metadata", {}).get("score", 0.0)),
            "source": source,
            "is_target": needle in source.lower(),
        })
    rows.sort(key=lambda r: -r["score"])
    return rows


async def _short_query_landscapes(factory: Any) -> Dict[str, Any]:
    """对生产 topic（+ 备选 query 形态）取**全量**候选排名。"""
    async with factory() as db:
        topics = ["Python", "MySQL", "Redis"]
        total = len((await db.execute(select(OrmChunk))).scalars().all())
        paced = PacedCachingEmbedder(
            knowledge_rag.default_embedder(role="query"),
            pace_seconds=3.0, retries=3, retry_delay=2.0, verbose=True,
        )
        job = (await db.execute(select(Job).order_by(Job.id))).scalars().first()
        out: Dict[str, Any] = {"total_candidates": total, "topics": {}, "query_forms": {}}

        for topic in topics:
            retriever = knowledge_rag.build_vector_retriever(
                db, embedder=paced, top_k=total, min_score=None,
            )
            hits = await retriever.retrieve(job, topic, None)
            landscape = _landscape_from_hits(hits, topic)
            out["topics"][topic] = {
                "query": topic,
                "landscape": landscape,
                "geometry": _geometry(landscape),
                "absolute": [_profile(landscape, m) for m in ABS_GRID],
                "alpha": [_alpha_profile(landscape, a) for a in ALPHA_GRID],
            }
            print(f"  [{topic}] top1={_fmt(landscape[0]['score'] if landscape else None)} "
                  f"命中(≥0.83)={_profile(landscape, 0.83)['hits']}")

        # 备选 query 形态：只为「MySQL」（有覆盖）测，看能否把分数抬进 0.83 区
        for name, template in QUERY_FORMS:
            if name == "baseline":
                continue
            query = template.format(topic="MySQL", job=_JOB_NAME_FALLBACK)
            retriever = knowledge_rag.build_vector_retriever(
                db, embedder=paced, top_k=total, min_score=None,
            )
            hits = await retriever.retrieve(job, query, None)
            landscape = _landscape_from_hits(hits, "MySQL")
            out["query_forms"][name] = {
                "template": template,
                "query": query,
                "landscape": landscape,
                "geometry": _geometry(landscape),
                "absolute": [_profile(landscape, m) for m in ABS_GRID],
                "alpha": [_alpha_profile(landscape, a) for a in ALPHA_GRID],
            }
            print(f"  [form={name}] query={query!r} "
                  f"top1={_fmt(landscape[0]['score'] if landscape else None)}")
    return out


def _load_long_query_scenarios() -> List[Dict[str, Any]]:
    """读任务 79 的产（**零网络**）：长问句场景的全量排名。"""
    if not SWEEP_JSON.exists():
        return []
    data = json.loads(SWEEP_JSON.read_text(encoding="utf-8"))
    rows = []
    for scenario in data.get("scenarios", []):
        landscape = scenario.get("score_landscape") or []
        rows.append({
            "scenario_id": scenario.get("scenario_id"),
            "topic": scenario.get("topic"),
            "landscape": landscape,
            "geometry": _geometry(landscape),
            "absolute": [_profile(landscape, m) for m in ABS_GRID],
            "alpha": [_alpha_profile(landscape, a) for a in ALPHA_GRID],
        })
    return rows


def _intersect_alpha(groups: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """求「所有**可分**场景同时可行」的 α 区间 = 各场景 ``(lo, hi]`` 的交集。"""
    usable = []
    for group in groups:
        feasible = (group.get("geometry") or {}).get("feasible_alpha")
        if feasible:
            usable.append((group.get("scenario_id") or group.get("topic"), feasible))
    if not usable:
        return {"scenarios": [], "lo_exclusive": None, "hi_inclusive": None,
                "midpoint": None, "recommended_display": None}
    lo = max(f["lo_exclusive"] for _, f in usable)
    hi = min(f["hi_inclusive"] for _, f in usable)
    if hi <= lo:
        return {"scenarios": [name for name, _ in usable],
                "lo_exclusive": lo, "hi_inclusive": hi, "midpoint": None,
                "recommended_display": None,
                "note": "各场景可行区间**不相交** ⇒ 单一 α 不可行"}
    mid = (lo + hi) / 2.0
    return {
        "scenarios": [name for name, _ in usable],
        "lo_exclusive": lo,
        "hi_inclusive": hi,
        "midpoint": mid,
        "lo_display": round(lo, 6),
        "hi_display": round(hi, 6),
        "recommended_display": round(mid, 6),
    }


async def main_async(args: argparse.Namespace) -> int:
    started = time.time()
    print("=" * 74)
    print("R8 · 生产 query 形态分数地形 + 相对阈值 α 标定")
    print("=" * 74)

    engine, factory, docs, chunks = await _build_env()
    try:
        return await _run_body(args, started, engine, factory, docs, chunks)
    finally:
        # 两个连接池都要在**同一个事件循环内**释放，否则 aiomysql 的
        # ``Connection.__del__`` 会在循环关闭后才触发，打印无关噪声。
        await engine.dispose()
        await prod_engine.dispose()


async def _run_body(
    args: argparse.Namespace,
    started: float,
    engine: Any,
    factory: Any,
    docs: List[Dict[str, Any]],
    chunks: List[Dict[str, Any]],
) -> int:
    print(f"\n[1] 生产快照：文档 {len(docs)} 篇 / 切片 {len(chunks)} 片")

    print("\n[2] 短词 query（生产真实形态）全量地形")
    short = await _short_query_landscapes(factory)

    print("\n[3] 长问句场景（读任务 79 产物，零网络）")
    long_rows = _load_long_query_scenarios()
    for row in long_rows:
        geo = row["geometry"]
        print(f"  {row['scenario_id']:<30} topic={row['topic']!r} "
              f"top1={_fmt(geo['top1'])} 窗口={_fmt(geo['window'])} 可分={geo['separable']}")

    print("\n[4] 相对阈值 α 的可行区间")
    groups: List[Dict[str, Any]] = []
    for name, item in short["topics"].items():
        groups.append({"scenario_id": f"short::{name}", **item})
    groups.extend(long_rows)
    for group in groups:
        geo = group["geometry"]
        feas = geo.get("feasible_alpha")
        span = ("-" if not feas
                else f"({_fmt(feas['lo_exclusive'])}, {_fmt(feas['hi_inclusive'])}]")
        print(f"  {group['scenario_id']:<30} 目标={geo['targets']:>2} "
              f"可分={str(geo['separable']):<5} 可行α={span}")
    combined = _intersect_alpha(groups)
    print(f"  ⇒ 交集：({_fmt(combined['lo_exclusive'])}, {_fmt(combined['hi_inclusive'])}]"
          f"  推荐 α = {combined['recommended_display']}")

    print("\n[5] 备选 query 形态（看能否把短词抬进 0.83 区）")
    for name, item in short["query_forms"].items():
        geo = item["geometry"]
        print(f"  {name:<16} query={item['query']!r} top1={_fmt(geo['top1'])} "
              f"命中(≥0.83)={item['absolute'][0]['hits']}")

    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "runner": "tests/rag_production_query_calibration_spark_run.py",
        "purpose": "生产短词 query 的分数地形 + 相对阈值 α 标定（长问句数据复用任务 79 产物）",
        "alpha_grid": list(ALPHA_GRID),
        "absolute_grid": list(ABS_GRID),
        "short_query": short,
        "long_query": long_rows,
        "combined_alpha": combined,
        "elapsed_seconds": round(time.time() - started, 2),
    }
    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n结果写入 {out_path}")
    return 0


def _entry() -> int:
    parser = argparse.ArgumentParser(
        description="R8 · 生产 query 形态分数地形 + 相对阈值 α 标定（不写生产库）")
    parser.add_argument("--out", type=str, default=str(DEFAULT_OUT))
    args = parser.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(_entry())
