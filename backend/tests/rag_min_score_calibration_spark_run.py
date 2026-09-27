# -*- coding: utf-8 -*-
"""R5 · **真实 Embedding（讯飞星火）**下的 ``min_score`` 重新标定（**非套件**运行器）。

为什么需要它
------------
任务 70 · R2 的标定（``scripts/rag_min_score_calibration.json``）是在
**离线哈希占位**（``HashEmbeddingService``，``semantic_enabled=False``）上做的，
因此结论 ``{top_k: 8, min_score: 0.25}``（任务 73）**只对那个模型成立**。

任务 75 接入真实 Embedding 后实测发现：真实模型的分数带**只有 ~0.03 宽**
（最高与最低只差 0.03），而 ``0.25`` 是哈希模型下的绝对分数下限
⇒ **在真实模型下 ``0.25`` 什么都切不掉**（全部候选 ≫ 0.25），RAG 会把
``top_k`` 条**全部**注入。这正是 MEMORY 里那条
「换 Embedding 模型必须重标 ``min_score``」的实例。

本运行器把 R2 的方法论**原样搬到真实模型上**：
窗口 = ``(非目标最高分, 目标最低分]``，**在全量候选（``top_k=100``）上算**，
推荐值 = 窗口中点（两侧余量对称）。

与 R2 运行器的三点差别（都是刻意的）
------------------------------------
1. **共用**一个内存 SQLite 引擎、语料**只入库一次**（R2 是每场景一个引擎）。
   理由：真实模型每次编码都要**联网 + 消耗配额**，R2 那样会把 32 片编码 3 遍。
2. 查询向量**按文本缓存**（同一 topic 只编码一次）。embedder 对同一文本是确定性的，
   缓存只省网络调用、**不改变任何分数**。
3. 输出写**另一个文件** ``scripts/rag_min_score_calibration_spark.json``
   —— **不覆盖** R2 的冻结产物。

用法
----
::

    cd backend
    EMBEDDING_PROVIDER=spark python tests/rag_min_score_calibration_spark_run.py

``EMBEDDING_PROVIDER=spark`` 是**必须**的显式 opt-in：不设它时
``default_embedder()`` 返回离线占位，本运行器会**拒绝执行**（退出码 3）
——用哈希模型标定出来的阈值对真实模型毫无意义。

**本运行器不修改任何生产代码、不改基准文件、不写生产库**（只用内存 SQLite）。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
PROJECT_DIR = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import Base  # noqa: E402
from models import InterviewSession, Job, Resume, User  # noqa: E402
from services import interview_context, interview_core  # noqa: E402
from services.embedding_provider import ROLE_DOCUMENT, ROLE_QUERY  # noqa: E402
from services.embedding_service import (  # noqa: E402
    EmbeddingService,
    describe_embedding,
)
from services.interview_agent import (  # noqa: E402
    build_question_variables,
    render_question_prompt,
)
from services.knowledge_import_pipeline import KnowledgeImportPipeline  # noqa: E402
from services.knowledge_rag import default_embedder  # noqa: E402
from services.vector_knowledge_retriever import VectorKnowledgeRetriever  # noqa: E402
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

BENCHMARK_PATH = BACKEND_DIR / "scripts" / "interview_eval_benchmark.json"
CORPUS_PATH = BACKEND_DIR / "scripts" / "interview_knowledge.json"
#: R2 的**离线哈希**标定结果（只读，用于对照，**绝不覆盖**）。
HASH_CALIBRATION_PATH = BACKEND_DIR / "scripts" / "rag_min_score_calibration.json"
RESULT_PATH = BACKEND_DIR / "scripts" / "rag_min_score_calibration_spark.json"

#: 拿「窗口」必须看全部候选，不能被 top_k 截断。
FULL_SCAN_TOP_K = 100

#: 窗口宽度分档 / 可用性阈值（与 R2 运行器同口径，便于对照）。
USABLE_GAP_THRESHOLD = 0.02
ROBUSTNESS_WIDE = 0.15
ROBUSTNESS_NARROW = 0.05

#: 上游限流参数（实测 ``code=11202 licc failed``；间隔 3s 逐条则稳定）。
DEFAULT_PACE = 3.0
DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAY = 3.0

EXIT_OK = 0
EXIT_PROBLEMS = 1
EXIT_USAGE = 2
EXIT_SAFETY = 3

#: 知识小节标题（与 ``question_knowledge.txt`` 一致）。
KNOWLEDGE_HEADING = "## 四·五、参考知识（外部检索结果，可选）"


# ============================================================
# 一、限流 + 重试 + 缓存的 Embedding 包装
# ============================================================
class PacedCachingEmbedder(EmbeddingService):
    """包住真实 embedder：**逐条间隔 + 失败重试 + 按文本缓存**，其余原样透传。

    ``name`` / ``dimension`` / ``semantic_enabled`` / ``domain`` 必须透传：
    检索器用 ``embedder.name`` 当向量库的 ``model`` 过滤条件，写侧 Pipeline
    用 ``embedder.name`` 写 ``embedding_model`` —— 透传错了就变成「一条都查不到」。

    缓存是**纯收益**：同一文本 ⇒ 同一向量（确定性），缓存只省网络调用。
    标定要跑几十次 ``retrieve``（窗口 / 边界探针 / 各档阈值），
    没有缓存就会把同一个 query 编码几十遍。
    """

    def __init__(
        self,
        inner: EmbeddingService,
        *,
        pace_seconds: float = 0.0,
        retries: int = 0,
        retry_delay: float = 1.0,
        verbose: bool = True,
    ) -> None:
        self.inner = inner
        self.name = inner.name
        self.dimension = inner.dimension
        self.semantic_enabled = getattr(inner, "semantic_enabled", False)
        self.pace_seconds = float(pace_seconds)
        self.retries = int(retries)
        self.retry_delay = float(retry_delay)
        self.verbose = verbose
        self.durations: List[float] = []   # 每次**尝试**的耗时（含失败那几次）
        self.retried = 0
        self.hits = 0
        self._cache: Dict[str, List[float]] = {}

    @property
    def domain(self) -> str:
        return getattr(self.inner, "domain", "")

    async def _embed_one(self, text: str) -> List[float]:
        cached = self._cache.get(text)
        if cached is not None:
            self.hits += 1
            return list(cached)

        for attempt in range(self.retries + 1):
            started = time.perf_counter()
            try:
                vector = await self.inner.embed(text)
            except Exception as exc:  # noqa: BLE001 - 重试耗尽后原样抛出
                self.durations.append(time.perf_counter() - started)
                if attempt >= self.retries:
                    raise
                self.retried += 1
                if self.verbose:
                    print(f"      [retry {attempt + 1}/{self.retries}] {str(exc)[:110]}")
                if self.retry_delay:
                    await asyncio.sleep(self.retry_delay)
                continue
            self.durations.append(time.perf_counter() - started)
            # 限流间隔放在**成功之后**：失败路径已有 retry_delay 退避，不必叠加
            if self.pace_seconds:
                await asyncio.sleep(self.pace_seconds)
            self._cache[text] = list(vector)
            return vector
        raise AssertionError("unreachable")  # pragma: no cover

    async def embed(self, text: str) -> List[float]:  # type: ignore[override]
        return await self._embed_one(text)

    async def embed_batch(self, texts):  # type: ignore[override]
        return [await self._embed_one(t) for t in texts]


# ============================================================
# 二、窗口 / 推荐（纯函数，与 R2 运行器**同口径**）
# ============================================================
def _window_from_chunks(
    chunks: Sequence[Any], target_sources: Sequence[str]
) -> Dict[str, Any]:
    """按「目标来源 / 非目标来源」分组，取窗口端点。

    .. warning::
        **必须保留全精度**（不要 ``round``）。检索是 ``score >= min_score``：
        把 ``0.4239817…`` 舍成 ``0.423982`` 会让它**大于**真实分数 ⇒ 目标被误杀。
        （R2 运行器第一版就踩过这个坑。）
        展示用的 6 位小数值另存 ``display_6dp``。
    """
    targets = set(target_sources)
    target = [c.metadata["score"] for c in chunks if c.source in targets]
    other = [c.metadata["score"] for c in chunks if c.source not in targets]
    return {
        "target_count": len(target),
        "non_target_count": len(other),
        "target_floor": min(target) if target else None,
        "noise_ceiling": max(other) if other else None,
        "target_scores_desc": sorted(target, reverse=True),
        "non_target_scores_desc": sorted(other, reverse=True),
    }


def _pack_window(stats: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if stats["target_floor"] is None or stats["noise_ceiling"] is None:
        return None
    low, high = stats["noise_ceiling"], stats["target_floor"]
    return {
        # 精确值（判定边界，勿舍入）
        "noise_ceiling": low,
        "target_floor": high,
        "safe_window": [low, high],
        "width": high - low,
        "separated": high > low,
        "target_count": stats["target_count"],
        "non_target_count": stats["non_target_count"],
        # 仅用于人读
        "display_6dp": {
            "noise_ceiling": round(low, 6),
            "target_floor": round(high, 6),
            "safe_window": [round(low, 6), round(high, 6)],
            "width": round(high - low, 6),
        },
    }


def _recommend(window: Dict[str, Any]) -> Dict[str, Any]:
    """窗口中点 = 两侧余量对称，对语料微调与浮点误差的容忍度最大。"""
    low, high = window["noise_ceiling"], window["target_floor"]
    midpoint = (low + high) / 2
    recommended = round(midpoint, 3)
    width = high - low
    if width >= ROBUSTNESS_WIDE:
        tier = "wide"
    elif width >= ROBUSTNESS_NARROW:
        tier = "narrow"
    else:
        tier = "fragile"
    return {
        "recommended": recommended,
        "midpoint_exact": midpoint,
        "reason": "取窗口 (非目标上限, 目标下限] 的中点 ⇒ 两侧余量对称，"
                  "对语料微调与浮点误差的容忍度最大",
        "recommended_in_window": low < recommended <= high,
        "recommended_margin_to_floor": round(high - recommended, 6),
        "recommended_margin_to_ceiling": round(recommended - low, 6),
        "width": round(width, 6),
        "width_tier": tier,
        "usable_by_gap_threshold": width > USABLE_GAP_THRESHOLD,
        "gap_threshold": USABLE_GAP_THRESHOLD,
    }


# ============================================================
# 三、环境（**一个**引擎；语料只入库一次）
# ============================================================
def _load_corpus() -> List[Dict[str, Any]]:
    return json.loads(CORPUS_PATH.read_text(encoding="utf-8"))["documents"]


async def _build_env(
    *, pace: float, retries: int, retry_delay: float
) -> Dict[str, Any]:
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    db = factory()

    db.add(User(username="r5_calib", email="r5_calib@example.com",
                password_hash="x", role="user"))
    await db.flush()

    doc_embedder = PacedCachingEmbedder(
        default_embedder(role=ROLE_DOCUMENT),
        pace_seconds=pace, retries=retries, retry_delay=retry_delay,
    )
    query_embedder = PacedCachingEmbedder(
        default_embedder(role=ROLE_QUERY),
        pace_seconds=pace, retries=retries, retry_delay=retry_delay,
    )
    store = SqlAlchemyVectorStore(db)

    documents = _load_corpus()
    print(f"  语料入库：{len(documents)} 篇 → 逐片真实编码（限流间隔 {pace}s）…")
    pipeline = KnowledgeImportPipeline(db, embedder=doc_embedder, store=store)
    reports = [await pipeline.import_document(dict(doc)) for doc in documents]
    failed = [r for r in reports if not r["ok"]]
    chunks_in_store = await store.count()
    print(f"  入库完成：{chunks_in_store} 片；失败 {len(failed)} 篇")

    return {
        "engine": engine,
        "db": db,
        "store": store,
        "doc_embedder": doc_embedder,
        "query_embedder": query_embedder,
        "documents": documents,
        "reports": reports,
        "chunks_in_store": chunks_in_store,
    }


async def _make_scenario_rows(
    env: Dict[str, Any], scenario: Dict[str, Any]
) -> Dict[str, Any]:
    db = env["db"]
    job = Job(**scenario["job"])
    resume = Resume(user_id=1, filename="resume.md",
                    content=scenario["candidate"]["resume_content"])
    db.add_all([job, resume])
    await db.flush()

    cfg = scenario["session"]
    row = InterviewSession(
        user_id=1, job_id=job.id, resume_id=resume.id, status="created",
        interview_type=cfg["interview_type"], difficulty=cfg["difficulty"],
        duration=cfg["duration"], total_questions=cfg["total_questions"],
        current_question_no=1,
    )
    db.add(row)
    await db.commit()
    session_id = row.id

    # 与流程内部**同一个** Planner 函数（use_llm=False ⇒ 确定性）
    plan = await interview_core._build_session_plan(db, row)

    ctx = scenario["context"]
    await interview_context.create_context(db, session_id)
    await interview_context.update_context(
        db, session_id,
        current_stage=ctx["current_stage"],
        asked_questions=list(ctx["asked_questions"]),
        covered_topics=list(ctx["covered_topics"]),
        weak_topics=list(ctx["weak_topics"]),
    )
    context = await interview_context.get_context(db, session_id)

    return {
        "job": job, "resume": resume, "row": row,
        "session_id": session_id, "plan": plan, "context": context,
    }


# ============================================================
# 四、标定
# ============================================================
async def _retrieve(
    env: Dict[str, Any], scenario: Dict[str, Any], rows: Dict[str, Any],
    *, top_k: int, min_score: Optional[float],
) -> List[Any]:
    retriever = VectorKnowledgeRetriever(
        env["query_embedder"], env["store"], top_k=top_k, min_score=min_score
    )
    return await retriever.retrieve(
        rows["job"], scenario["retrieval"]["query_topic"], rows["context"]
    )


async def _profile_at(
    env: Dict[str, Any], scenario: Dict[str, Any], rows: Dict[str, Any],
    min_score: Optional[float],
) -> Dict[str, Any]:
    """在**全量候选**上套阈值后的画像（不含 top_k 截断）。"""
    targets = set(scenario["retrieval"]["expected_hit_sources"])
    chunks = await _retrieve(env, scenario, rows, top_k=FULL_SCAN_TOP_K,
                             min_score=min_score)
    return {
        "min_score": min_score,
        "count": len(chunks),
        "target_count": sum(1 for c in chunks if c.source in targets),
        "non_target_count": sum(1 for c in chunks if c.source not in targets),
        "sources": [c.source for c in chunks],
    }


def _required_top_k(
    ranking: Sequence[Any], target_sources: Sequence[str], min_score: Optional[float]
) -> Optional[int]:
    """给定阈值下「保住全部目标片」所需的**最小 top_k**。

    ``ranking`` 必须已是**全量候选按分数降序**的序列（同 ``select_matches`` 的
    ``(-score, 位置)`` 口径）。名次从 1 开始；无目标 ⇒ ``None``。
    """
    targets = set(target_sources)
    kept = [c for c in ranking
            if min_score is None or c.metadata["score"] >= min_score]
    positions = [i + 1 for i, c in enumerate(kept) if c.source in targets]
    return max(positions) if positions else None


async def _boundary_probe(
    env: Dict[str, Any], scenario: Dict[str, Any], rows: Dict[str, Any],
    window: Dict[str, Any],
) -> Dict[str, Any]:
    """证明窗口端点就是真实判定边界（``score >= min_score`` 含等号）。

    四个取样点：目标下限、目标下限 + 1 ULP、非目标上限、非目标上限 + 1 ULP。
    每个点**分别数**目标 / 非目标各留下几条——只看总数会把
    「丢掉一条目标」和「留下一条噪声」混为一谈。
    """
    floor = window["target_floor"]
    ceiling = window["noise_ceiling"]
    expected_targets = window["target_count"]

    at_floor = await _profile_at(env, scenario, rows, floor)
    above_floor = await _profile_at(env, scenario, rows, math.nextafter(floor, math.inf))
    at_ceiling = await _profile_at(env, scenario, rows, ceiling)
    above_ceiling = await _profile_at(
        env, scenario, rows, math.nextafter(ceiling, math.inf)
    )

    checks = {
        # 下限含等号 ⇒ 下限那一分仍被保留；抬高一个 ULP 恰好少掉**那一条**目标
        "floor_is_inclusive": (
            at_floor["target_count"] == expected_targets
            and above_floor["target_count"] == expected_targets - 1
        ),
        # 上限含等号 ⇒ 上限那一分仍被保留；抬高一个 ULP 恰好少掉**那一条**噪声
        "ceiling_is_inclusive": (
            at_ceiling["non_target_count"] == 1
            and above_ceiling["non_target_count"] == 0
        ),
        # 窗口内部：噪声一条不留、目标一条不少
        "interior_keeps_all_targets_drops_all_noise": (
            at_floor["non_target_count"] == 0
            and at_ceiling["target_count"] == expected_targets
        ),
    }
    return {
        "semantics": "检索保留 score >= min_score（含等号）⇒ 窗口下界开、上界闭",
        "expected_target_count": expected_targets,
        "at_target_floor": at_floor,
        "just_above_target_floor": above_floor,
        "at_noise_ceiling": at_ceiling,
        "just_above_noise_ceiling": above_ceiling,
        "checks": checks,
    }


def _prompt_chars(
    rows: Dict[str, Any], chunks: Sequence[Any]
) -> Dict[str, Any]:
    """按真实模板渲染 Prompt 并量长度（**不需要大模型**）。

    ``render_question_prompt`` 对空知识回落原模板 ⇒ ``chunks=[]`` 即「无 RAG」臂。
    """
    variables = build_question_variables(
        rows["context"], rows["plan"], rows["resume"], rows["job"]
    )
    template, prompt = render_question_prompt(variables, list(chunks) or None)
    return {
        "template": template,
        "prompt_chars": len(prompt),
        "has_knowledge_block": KNOWLEDGE_HEADING in prompt,
    }


async def calibrate_scenario(
    env: Dict[str, Any], scenario: Dict[str, Any], problems: List[str]
) -> Dict[str, Any]:
    sid = scenario["id"]
    retrieval = scenario["retrieval"]
    declared_min_score = retrieval["min_score"]
    declared_top_k = retrieval["top_k"]
    expected_chunk_count = retrieval["expected_chunk_count"]
    targets = set(retrieval["expected_hit_sources"])

    print(f"\n{'─' * 74}\n[场景 {sid}] {scenario['label']}")

    rows = await _make_scenario_rows(env, scenario)

    # ---- 全量候选排名（一次性；后面所有画像都基于它） ----
    full_chunks = await _retrieve(env, scenario, rows, top_k=FULL_SCAN_TOP_K,
                                  min_score=None)
    full_stats = _window_from_chunks(full_chunks, targets)
    window = _pack_window(full_stats)
    print(f"  全量候选 {len(full_chunks)} 条（目标 {full_stats['target_count']}）")

    if window is None:
        problems.append(f"{sid}: 窗口不存在（目标或非目标一侧为空）")
        return {"scenario_id": sid, "topic": retrieval["query_topic"],
                "window": None, "problem": "窗口不存在"}

    rec = _recommend(window)
    print(f"  窗口 = ({window['display_6dp']['noise_ceiling']}, "
          f"{window['display_6dp']['target_floor']}]  宽 {rec['width']} [{rec['width_tier']}]")
    print(f"  推荐值（窗口中点）= {rec['recommended']}")

    probe = await _boundary_probe(env, scenario, rows, window)
    for name, ok in probe["checks"].items():
        if not ok:
            problems.append(f"{sid}: 边界探针 {name} 未通过")

    # ---- 声明阈值 / 推荐值 / 无阈值的画像 ----
    profile_none = await _profile_at(env, scenario, rows, None)
    profile_declared = await _profile_at(env, scenario, rows, declared_min_score)
    profile_recommended = await _profile_at(env, scenario, rows, rec["recommended"])

    # ---- top_k 平台区（min_score 生效后 top_k 只剩「别把目标截掉」） ----
    required_k = _required_top_k(full_chunks, targets, rec["recommended"])
    required_k_declared = _required_top_k(full_chunks, targets, declared_min_score)
    plateau_checks = {
        "targets_never_truncated_at_declared_top_k": (
            required_k is not None and required_k <= declared_top_k
        ),
        "targets_never_truncated_at_recommended_top_k_8": (
            required_k is not None and required_k <= 8
        ),
    }
    for name, ok in plateau_checks.items():
        if not ok:
            problems.append(f"{sid}: top_k 平台区 {name} 未通过")

    # ---- Prompt 长度（真实模板，无大模型） ----
    prompts = {
        "rag_off": _prompt_chars(rows, []),
        "no_threshold": _prompt_chars(
            rows, await _retrieve(env, scenario, rows, top_k=declared_top_k,
                                  min_score=None)),
        "declared": _prompt_chars(
            rows, await _retrieve(env, scenario, rows, top_k=declared_top_k,
                                  min_score=declared_min_score)),
        "recommended": _prompt_chars(
            rows, await _retrieve(env, scenario, rows, top_k=max(declared_top_k, 8),
                                  min_score=rec["recommended"])),
    }

    # ---- 与基准声明的一致性（如实记录，不替基准改数） ----
    declared_ok = (
        profile_declared["target_count"] == full_stats["target_count"]
        and profile_declared["non_target_count"] == 0
    )
    if profile_declared["non_target_count"] > 0:
        problems.append(
            f"{sid}: 基准声明的 min_score={declared_min_score} 放进了 "
            f"{profile_declared['non_target_count']} 条噪声"
        )
    if profile_declared["target_count"] < full_stats["target_count"]:
        problems.append(
            f"{sid}: 基准声明的 min_score={declared_min_score} 丢掉了 "
            f"{full_stats['target_count'] - profile_declared['target_count']} 条目标"
        )

    return {
        "scenario_id": sid,
        "label": scenario["label"],
        "topic": retrieval["query_topic"],
        "declared": {
            "top_k": declared_top_k,
            "min_score": declared_min_score,
            "expected_hit_sources": sorted(targets),
            "expected_chunk_count": expected_chunk_count,
        },
        "corpus_chunks": len(full_chunks),
        "window": window,
        "recommendation": rec,
        "boundary_probe": probe,
        "profiles": {
            "no_threshold": {k: v for k, v in profile_none.items() if k != "sources"},
            "declared": {k: v for k, v in profile_declared.items() if k != "sources"},
            "recommended": {k: v for k, v in profile_recommended.items() if k != "sources"},
        },
        "top_k_plateau": {
            "required_top_k_at_recommended": required_k,
            "required_top_k_at_declared": required_k_declared,
            "checks": plateau_checks,
            "note": "min_score 生效后 top_k 只剩「别把目标截掉」；调 top_k 不会缩短 Prompt",
        },
        "prompt_chars": prompts,
        "declared_threshold_is_clean": declared_ok,
        "ranking": {
            "target_scores_desc": [round(s, 6) for s in full_stats["target_scores_desc"]],
            "non_target_scores_top8": [
                round(s, 6) for s in full_stats["non_target_scores_desc"][:8]
            ],
        },
    }


# ============================================================
# 五、main
# ============================================================
def _load(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _describe(embedder: EmbeddingService) -> Dict[str, Any]:
    info = describe_embedding(embedder)
    return {
        "provider": info.provider,
        "dimension": info.dimension,
        "semantic_enabled": info.semantic_enabled,
        "role": getattr(embedder, "domain", "") or None,
    }


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="R5 · 真实 Embedding 下的 min_score 重新标定（不写生产库）"
    )
    parser.add_argument("--scenario", action="append", default=None,
                        help="只跑指定场景 id（可重复）")
    parser.add_argument("--out", default=str(RESULT_PATH))
    parser.add_argument("--pace", type=float, default=DEFAULT_PACE,
                        help=f"每条编码后的间隔秒数（默认 {DEFAULT_PACE}）")
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--retry-delay", type=float, default=DEFAULT_RETRY_DELAY)
    args = parser.parse_args()

    print("=" * 74)
    print("R5 · 真实 Embedding 下的 min_score 重新标定")
    print("=" * 74)

    benchmark = _load(BENCHMARK_PATH)
    scenarios = benchmark["scenarios"]
    if args.scenario:
        wanted = set(args.scenario)
        scenarios = [s for s in scenarios if s["id"] in wanted]
        if not scenarios:
            print(f"[USAGE] 未匹配到任何场景：{sorted(wanted)}")
            return EXIT_USAGE

    # ---- 安全闸：拒绝用离线占位做标定 ----
    probe = default_embedder(role=ROLE_DOCUMENT)
    info = describe_embedding(probe)
    print(f"  Embedding：provider={info.provider} dim={info.dimension} "
          f"semantic_enabled={info.semantic_enabled}")
    if not info.semantic_enabled:
        print("\n[SAFETY] 当前默认 Embedding 是**离线占位**（semantic_enabled=False）。")
        print("         用它标定出来的 min_score 对真实模型**毫无意义**。")
        print("         请显式启用真实模型后重跑：")
        print("             EMBEDDING_PROVIDER=spark python "
              "tests/rag_min_score_calibration_spark_run.py")
        return EXIT_SAFETY

    problems: List[str] = []
    started = time.time()
    env = await _build_env(pace=args.pace, retries=args.retries,
                           retry_delay=args.retry_delay)
    try:
        results: List[Dict[str, Any]] = []
        for scenario in scenarios:
            results.append(await calibrate_scenario(env, scenario, problems))

        # ---- 汇总 ----
        def _sum(key: str, field: str = "count") -> int:
            return sum(r["profiles"][key][field] for r in results if r.get("profiles"))

        def _chars(key: str) -> int:
            return sum(r["prompt_chars"][key]["prompt_chars"]
                       for r in results if r.get("prompt_chars"))

        recommended_values = [r["recommendation"]["recommended"]
                              for r in results if r.get("recommendation")]
        summary = {
            "scenarios": len(results),
            "recommended_min_score_per_scenario": {
                r["scenario_id"]: r["recommendation"]["recommended"]
                for r in results if r.get("recommendation")
            },
            "recommended_min_score_range": (
                [min(recommended_values), max(recommended_values)]
                if recommended_values else None
            ),
            "width_tiers": {
                r["scenario_id"]: r["recommendation"]["width_tier"]
                for r in results if r.get("recommendation")
            },
            "window_widths": {
                r["scenario_id"]: r["recommendation"]["width"]
                for r in results if r.get("recommendation")
            },
            "chunks": {
                "no_threshold": _sum("no_threshold"),
                "declared": _sum("declared"),
                "recommended": _sum("recommended"),
            },
            "noise": {
                "no_threshold": _sum("no_threshold", "non_target_count"),
                "declared": _sum("declared", "non_target_count"),
                "recommended": _sum("recommended", "non_target_count"),
            },
            "targets": {
                "no_threshold": _sum("no_threshold", "target_count"),
                "declared": _sum("declared", "target_count"),
                "recommended": _sum("recommended", "target_count"),
            },
            "prompt_chars": {
                "rag_off": _chars("rag_off"),
                "no_threshold": _chars("no_threshold"),
                "declared": _chars("declared"),
                "recommended": _chars("recommended"),
            },
            "declared_threshold_is_clean_all": all(
                r.get("declared_threshold_is_clean") for r in results
            ),
        }

        payload = {
            "generated_at": datetime.now(timezone(timedelta(hours=8))).isoformat(
                timespec="seconds"),
            "runner": "tests/rag_min_score_calibration_spark_run.py",
            "purpose": "在**真实 Embedding**（讯飞星火）下重新标定 min_score / 复核 top_k",
            "method": {
                "window": "(非目标来源最高分, 目标来源最低分]；"
                          "过滤语义 score >= min_score ⇒ 下界开、上界闭",
                "must_use_full_candidates": (
                    f"窗口必须在全量候选（top_k={FULL_SCAN_TOP_K}）上算，"
                    "否则只是局部窗口"
                ),
                "recommendation": "窗口中点（两侧余量对称）",
                "boundary_kept_full_precision": (
                    "判定边界一律保留全精度；display_6dp 仅供人读"
                ),
                "pace_seconds": args.pace,
                "retries": args.retries,
                "retry_delay": args.retry_delay,
            },
            "embedding": {
                "document": _describe(env["doc_embedder"]),
                "query": _describe(env["query_embedder"]),
            },
            "corpus": {
                "file": str(CORPUS_PATH.relative_to(PROJECT_DIR)).replace("\\", "/"),
                "documents": len(env["documents"]),
                "chunks": env["chunks_in_store"],
                "import_failed": sum(1 for r in env["reports"] if not r["ok"]),
            },
            "embedding_calls": {
                "document_attempts": len(env["doc_embedder"].durations),
                "document_retried": env["doc_embedder"].retried,
                "query_attempts": len(env["query_embedder"].durations),
                "query_retried": env["query_embedder"].retried,
                "query_cache_hits": env["query_embedder"].hits,
                "worst_seconds": round(
                    max(env["doc_embedder"].durations
                        + env["query_embedder"].durations), 4),
                "avg_seconds": round(
                    sum(env["doc_embedder"].durations
                        + env["query_embedder"].durations)
                    / max(1, len(env["doc_embedder"].durations)
                          + len(env["query_embedder"].durations)), 4),
            },
            "hash_calibration_reference": {
                "file": str(HASH_CALIBRATION_PATH.relative_to(PROJECT_DIR)).replace(
                    "\\", "/"),
                "note": "任务 70 · R2 的**离线哈希**标定结果；仅作对照，本运行器不覆盖它",
            },
            "summary": summary,
            "scenarios": results,
            "problems": problems,
        }
    finally:
        await env["db"].close()
        await env["engine"].dispose()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                        encoding="utf-8")

    elapsed = time.time() - started
    print(f"\n{'=' * 74}")
    print(f"耗时 {elapsed:.1f}s；结果写入 {out_path}")
    print(f"注入条数  无阈值 {summary['chunks']['no_threshold']} → "
          f"声明阈值 {summary['chunks']['declared']} → "
          f"推荐阈值 {summary['chunks']['recommended']}")
    print(f"噪声条数  无阈值 {summary['noise']['no_threshold']} → "
          f"声明阈值 {summary['noise']['declared']} → "
          f"推荐阈值 {summary['noise']['recommended']}")
    print(f"目标条数  无阈值 {summary['targets']['no_threshold']} → "
          f"声明阈值 {summary['targets']['declared']} → "
          f"推荐阈值 {summary['targets']['recommended']}")
    print(f"Prompt 字数 无 RAG {summary['prompt_chars']['rag_off']} / "
          f"无阈值 {summary['prompt_chars']['no_threshold']} / "
          f"声明 {summary['prompt_chars']['declared']} / "
          f"推荐 {summary['prompt_chars']['recommended']}")
    print(f"推荐 min_score：{summary['recommended_min_score_per_scenario']}")

    if problems:
        print(f"\n[PROBLEMS] {len(problems)} 项：")
        for p in problems:
            print(f"  - {p}")
        return EXIT_PROBLEMS

    print("\n[OK] 无完整性问题。")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
