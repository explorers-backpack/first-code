# -*- coding: utf-8 -*-
"""R7 · **真实 Embedding 下的业务链路端到端验证**（**非套件**运行器）。

回答的问题
----------
「RAG 接入真实 Embedding 之后，生产业务链路还正常吗？」

与 ``test_rag_production_route.py`` 的区别
-----------------------------------------
那个套件跑在**离线哈希占位**上（被 ``regression_env`` 钉住）、语料是**临时造的 1 篇**；
本运行器跑**真实讯飞 Embedding**、语料是**生产库里的真语料**（从 MySQL 只读搬运向量）。

**不写生产库**：生产库只做 ``SELECT``；会话 / 岗位等测试数据全建在**内存 SQLite** 上，
再把生产库的 ``knowledge_document`` / ``knowledge_chunk`` 行**原样复制**进去。
⇒ 用的是**生产真向量**（2560 维 xinghuo-embedding），且**不需要重新编码语料**
（只有 query 侧会调上游，实测 2~4 次）。

文本大模型用**替身**（``SpySpark``）⇒ 确定性、不消耗文本配额、能抓 Prompt 原文。
**Embedding 保持真实**——本文件**刻意不 import ``regression_env``**。

覆盖
----
[1] 生产库快照可用（文档 / 切片 / 维度 / 模型）
[2] ``use_rag=false``（默认）⇒ 不检索，Prompt 无知识小节
[3] ``use_rag=true`` ⇒ **真实检索**、Prompt 含知识小节、含**生产来源串**、无降级 warning
[4] 请求级 ``top_k`` / ``min_score`` 在真实链路上**真的覆盖** ``.env`` 默认值
[5] **降级**：Embedding 不可用时 ⇒ **200 + ok=true + warning**（不 500、不阻塞出题）
[6] 前后对照：同一会话 RAG 开 / 关的 Prompt 字数差
[7] **真实生产 topic 画像**：由 ``plan.priority_topics``（经 ``current_topic``）推导出的
    **真实 query 形态**逐词给出 top1 分数与默认阈值下的命中数 ⇒ 用来判断
    「接线正常」是否等于「内容有效」。**内容失效记 ``findings``、不计 FAIL**。

判据（**接线层** 与 **内容层** 分开）
-----------------------------------
``checks`` 只断言**接线**是否正确（开关、真实模型、role、参数透传、失败降级）；
「默认阈值把真实 topic 滤空」属于**内容层发现**，进 ``findings``、**不影响退出码** ——
接线没坏，坏的是「阈值与真实 query 形态不匹配」。

用法
----
::

    python tests/rag_production_chain_spark_run.py
    python tests/rag_production_chain_spark_run.py --out scripts/xxx.json

**联网 + 真实凭据 + 少量配额**（query 侧 2~4 次）。**不要**加进回归循环。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import re
import sys
import time
from typing import Any, Dict, List, Optional

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

#: ★ 刻意**不** import ``regression_env``：本运行器要的就是真实配置。
from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy import func, select  # noqa: E402
from sqlalchemy import inspect as sa_inspect  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import main  # noqa: E402
import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from database import (  # noqa: E402
    Base,
    async_session as prod_session,
    engine as prod_engine,
    get_db,
)
from deps import get_current_user  # noqa: E402
from models import (  # noqa: E402
    InterviewSession,
    Job,
    KnowledgeChunk as OrmChunk,
    KnowledgeDocument as OrmDoc,
    User,
)
from services import interview_context, interview_core, knowledge_rag  # noqa: E402

DEFAULT_OUT = BACKEND_DIR / "scripts" / "rag_production_chain_spark.json"

API = "/api/interview"
KNOWLEDGE_MARKER = "参考知识"
WARNING_DEGRADED = "knowledge_retrieval_failed"

#: 从 Prompt 里抠出知识片段的来源标注（``render_question_prompt`` 追加的 ``（来源：…）``）。
SOURCE_RE = re.compile(r"来源：([^\s）)]+)")

AGENT_REPLY = json.dumps(
    {
        "question": "请结合项目说明索引与缓存的取舍。",
        "question_type": "technical",
        "topic": "综合",
        "difficulty": "mid",
        "expected_points": ["取舍", "量化"],
        "reason": "考察工程判断",
    },
    ensure_ascii=False,
)

_RESULTS: List[Dict[str, Any]] = []


def _check(name: str, cond: bool, detail: str = "") -> bool:
    _RESULTS.append({"name": name, "ok": bool(cond), "detail": str(detail)})
    print(("  [PASS] " if cond else "  [FAIL] ") + name
          + (f"  -> {detail}" if detail and not cond else ""))
    return bool(cond)


class SpySpark:
    """文本大模型替身：记录每次 Prompt，返回固定 JSON。"""

    def __init__(self) -> None:
        self.prompts: List[str] = []

    async def chat_async(self, message: str, **_kwargs: Any) -> str:
        self.prompts.append(message)
        return AGENT_REPLY


class RecordingRetriever:
    """包住**真实**检索器：只记录「问了什么 / 命中几条 / 来源」，不改行为。"""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.calls: List[Dict[str, Any]] = []

    async def retrieve(self, job: Any = None, topic: Any = None, context: Any = None):
        hits = await self.inner.retrieve(job, topic, context)
        self.calls.append({
            "topic": topic,
            "count": len(hits),
            "sources": [getattr(h, "source", "") for h in hits],
            "scores": [round(float(getattr(h, "metadata", {}).get("score", 0.0)), 6)
                       for h in hits],
        })
        return hits


def _row_dict(obj: Any) -> Dict[str, Any]:
    """把 ORM 行摊平成纯 dict（在会话仍打开时调用，避免协程外惰性加载）。

    ★ **必须用 ``mapper.column_attrs``（属性名），不能用 ``columns``（列名）**：
    ``KnowledgeChunk.chunk_metadata = Column("metadata", JSON)`` 的列名是
    ``metadata``，而 ``sa_inspect(cls).columns`` 迭代出的 ``c.key`` 是**列名**。
    拿它去 ``OrmChunk(**row)`` 会**静默丢弃** —— 因为 ``metadata`` 是
    ``Base.metadata`` 的继承属性、``hasattr`` 为真，Declarative 构造函数
    只会 ``setattr(self, "metadata", ...)`` 而不报错，``chunk_metadata`` 仍是默认
    ``{}``。症状是「向量照常命中、``source`` 全空」这种**因错误的原因通过**。
    """
    mapper = sa_inspect(type(obj)).mapper
    return {attr.key: getattr(obj, attr.key) for attr in mapper.column_attrs}


def _field(obj: Any, name: str) -> Any:
    """从 dict 或对象上读字段（``plan`` 两种形态都可能）。"""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def _fmt(value: Any, digits: int = 6) -> str:
    """``None`` → ``"-"``；数值 → 定长小数（只给人读，判定一律用全精度）。"""
    return "-" if value is None else f"{float(value):.{digits}f}"


async def _snapshot_production() -> Dict[str, Any]:
    """**只读**生产库的 knowledge 两张表（不写任何东西）。"""
    async with prod_session() as db:
        docs = (await db.execute(select(OrmDoc).order_by(OrmDoc.id))).scalars().all()
        chunks = (await db.execute(select(OrmChunk).order_by(OrmChunk.id))).scalars().all()
        doc_rows = [_row_dict(d) for d in docs]
        chunk_rows = [_row_dict(c) for c in chunks]
    return {"documents": doc_rows, "chunks": chunk_rows}


async def main_async(args: argparse.Namespace) -> int:
    started = time.time()
    print("=" * 74)
    print("R7 · 真实 Embedding 下的业务链路端到端验证")
    print("=" * 74)

    # ---------------- [1] 生产库快照 ----------------
    print("\n[1] 生产库知识快照（只读）")
    snapshot = await _snapshot_production()
    docs, chunks = snapshot["documents"], snapshot["chunks"]
    models_seen = sorted({c["embedding_model"] for c in chunks})
    dims_seen = sorted({c["embedding_dim"] for c in chunks})
    missing = sum(1 for c in chunks if c["embedding"] is None)
    print(f"  文档 {len(docs)} 篇 / 切片 {len(chunks)} 片；模型 {models_seen}；维度 {dims_seen}")
    _check("生产库有知识文档", len(docs) > 0, str(len(docs)))
    _check("生产库有知识切片", len(chunks) > 0, str(len(chunks)))
    _check("切片全部已向量化（无 NULL）", missing == 0, f"NULL={missing}")
    _check("向量模型唯一", len(models_seen) == 1, str(models_seen))
    _check("向量维度唯一且非空", len(dims_seen) == 1 and bool(dims_seen[0]), str(dims_seen))

    # ---------------- 内存库 + 复制生产向量 ----------------
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
        user = User(username="rag_chain", email="rag_chain@example.com",
                    password_hash="x", role="user")
        # 技能顺序决定 plan.priority_topics 的顺序，进而决定 current_topic（取首项）。
        # 刻意把**语料已覆盖**的 MySQL 放首位：这样 [3] 节能对「真实链路真的把知识
        # 注入了 Prompt」做**正向**断言，而不是只记录「0 条」。Python 放最后是为了
        # 让 [7] 节同时覆盖「语料未覆盖的主题」这一分支。
        job = Job(job_name="后端开发工程师", salary="20-35K", edu_require="本科",
                  major_require="不限", skills="MySQL,Redis,Python",
                  duty="负责后端服务的设计与开发", city="深圳", industry="互联网")
        db.add_all([user, job])
        await db.commit()
        await db.refresh(user)
        await db.refresh(job)
        user_id, job_id = user.id, job.id
        copied = (await db.execute(select(func.count()).select_from(OrmChunk))).scalar_one()
        copied_rows = (await db.execute(select(OrmChunk))).scalars().all()
        copied_with_source = sum(
            1 for c in copied_rows if (c.chunk_metadata or {}).get("source")
        )
    _check("生产向量已复制进内存库", copied == len(chunks), f"{copied} vs {len(chunks)}")
    # 守卫：搬运必须**连 metadata 一起**带过来，否则「命中数 / 分数」照样对，
    # 只有 ``source`` 全空 —— 典型的「因错误的原因通过」。
    _check(
        "切片 metadata 完整搬运（source 非空）",
        copied_with_source == len(chunks),
        f"{copied_with_source} vs {len(chunks)}（_row_dict 取错列名会静默丢 metadata）",
    )

    async def override_get_db():
        async with factory() as session:
            yield session

    main.app.dependency_overrides[get_db] = override_get_db

    spark = SpySpark()
    original_spark = main.spark_api
    main.spark_api = spark  # type: ignore[assignment]

    real_builder = knowledge_rag.build_vector_retriever
    recorder = RecordingRetriever(None)
    holder: Dict[str, Any] = {}

    def recording_builder(db: Any, **kwargs: Any) -> Any:
        holder["kwargs"] = dict(kwargs)
        inner = real_builder(db, **kwargs)
        holder["retriever_config"] = {
            "top_k": getattr(inner, "top_k", None),
            "min_score": getattr(inner, "min_score", None),
            "min_score_ratio": getattr(inner, "min_score_ratio", None),
            "model": getattr(inner, "model", None),
            "embedder": getattr(getattr(inner, "embedder", None), "name", None),
            "domain": getattr(getattr(inner, "embedder", None), "domain", None),
        }
        recorder.inner = inner
        return recorder

    knowledge_rag.build_vector_retriever = recording_builder  # type: ignore[assignment]

    transport = ASGITransport(app=main.app)
    exit_code = 0
    #: 「内容层」观察，与「接线层」断言分开放：接线有问题才是 FAIL，
    #: 阈值把真实 topic 滤空属于**发现**（详见 [7]）。
    findings: List[Dict[str, Any]] = []
    profile: List[Dict[str, Any]] = []
    real_topic = ""
    #: 生产路径（[3] 节，use_rag=True 且不覆盖参数）实际用的检索配置。
    #: 不能直接读 ``holder``：它会被 [4]/[7] 的覆盖调用改写。
    prod_config: Dict[str, Any] = {}
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            async def override_get_current_user():
                return {"user_id": user_id, "email": "rag_chain@example.com", "role": "user"}

            main.app.dependency_overrides[get_current_user] = override_get_current_user

            r = await client.post(f"{API}/create", json={
                "job_id": job_id, "difficulty": "mid", "total_questions": 3})
            session_id = r.json()["session"]["id"]
            await client.post(f"{API}/{session_id}/start")

            # ---------------- [2] 默认不检索 ----------------
            print("\n[2] use_rag=false（默认）")
            spark.prompts.clear()
            recorder.calls.clear()
            r = await client.post(f"{API}/{session_id}/next-question", json={})
            body = r.json()
            prompt_off = spark.prompts[-1] if spark.prompts else ""
            _check("HTTP 200", r.status_code == 200, f"{r.status_code} {r.text[:160]}")
            _check("ok=true", body.get("ok") is True, str(body)[:200])
            _check("未组装检索器", not holder.get("kwargs") and not recorder.calls,
                   str(holder.get("kwargs")))
            _check("Prompt 无知识小节", KNOWLEDGE_MARKER not in prompt_off, "含知识小节")

            # ---------------- [3] 真实检索 ----------------
            print("\n[3] use_rag=true（真实 Embedding + 生产语料）")
            spark.prompts.clear()
            recorder.calls.clear()
            holder.clear()
            r = await client.post(f"{API}/{session_id}/next-question", json={"use_rag": True})
            body = r.json()
            prompt_on = spark.prompts[-1] if spark.prompts else ""
            warnings = body.get("warnings") or []
            print(f"  检索器配置: {json.dumps(holder.get('retriever_config'), ensure_ascii=False)}")
            print(f"  检索调用: {json.dumps(recorder.calls, ensure_ascii=False)[:400]}")

            cfg3 = holder.get("retriever_config") or {}
            prod_config = dict(cfg3)
            _check("HTTP 200", r.status_code == 200, f"{r.status_code} {r.text[:160]}")
            _check("ok=true", body.get("ok") is True, str(body)[:240])
            _check("检索器已组装", bool(recorder.calls), str(cfg3))
            _check(
                "读侧用真实模型（非离线占位）",
                cfg3.get("embedder") not in (None, "hash-local"),
                str(cfg3),
            )
            _check(
                "读侧 role=query（非对称编码配对正确）",
                cfg3.get("domain") == "query",
                str(cfg3),
            )
            _check("无降级 warning", WARNING_DEGRADED not in warnings, str(warnings))

            injected = (recorder.calls[0]["count"] if recorder.calls else 0)
            default_min = cfg3.get("min_score")
            _check("注入条数 <= top_k", injected <= (cfg3.get("top_k") or 10**9),
                   str(injected))
            _check(
                "检索到的分数全部 >= min_score",
                all(s >= (default_min if default_min is not None else -1.0)
                    for c in recorder.calls for s in c["scores"]),
                str([c["scores"] for c in recorder.calls])[:200],
            )

            # 「Prompt 是否真的带上知识」取决于**检索结果是否非空**：
            # 非空才断言内容正确；为空则记 finding（接线无误、内容层失效）。
            sources = sorted(set(SOURCE_RE.findall(prompt_on)))
            prod_sources = {d["source"] for d in docs}
            if injected > 0:
                _check("Prompt 含知识小节", KNOWLEDGE_MARKER in prompt_on, "缺知识小节")
                _check("知识小节含来源标注", bool(sources), str(sources[:5]))
                _check(
                    "来源全部来自生产语料",
                    bool(sources) and set(sources) <= prod_sources,
                    f"越界={sorted(set(sources) - prod_sources)[:5]}",
                )
            else:
                findings.append({
                    "kind": "zero_knowledge_at_default_threshold",
                    "topic": (recorder.calls[0]["topic"] if recorder.calls else None),
                    "min_score": default_min,
                    "top_k": cfg3.get("top_k"),
                    "note": ("接线正常（真实 embedder + role=query），但默认阈值把该 topic "
                             "的全部候选滤掉 ⇒ 注入 0 条，RAG 实际等于关闭"),
                })
                print("  [FINDING] 默认阈值下零命中：接线正常，内容层为空")

            # ---------------- [4] 请求级参数覆盖 .env 默认值 ----------------
            print("\n[4] use_rag=true + 显式 top_k / min_score（应覆盖 .env）")
            recorder.calls.clear()
            holder.clear()
            r = await client.post(f"{API}/{session_id}/next-question",
                                  json={"use_rag": True, "top_k": 2, "min_score": 0.9})
            body = r.json()
            cfg = holder.get("retriever_config") or {}
            _check("HTTP 200", r.status_code == 200, str(r.status_code))
            _check("显式 top_k 生效", cfg.get("top_k") == 2, str(cfg))
            _check("显式 min_score 生效", cfg.get("min_score") == 0.9, str(cfg))
            _check("ok=true（0.9 过严也只是无知识，不报错）", body.get("ok") is True,
                   str(body)[:200])

            # ---------------- [5] 降级：Embedding 不可用 ----------------
            print("\n[5] 降级：Embedding 构造失败")
            def boom(**_kwargs: Any) -> Any:
                raise RuntimeError("模拟上游不可用")

            saved = knowledge_rag.default_embedder
            knowledge_rag.default_embedder = boom  # type: ignore[assignment]
            try:
                r = await client.post(f"{API}/{session_id}/next-question",
                                      json={"use_rag": True})
                body = r.json()
            finally:
                knowledge_rag.default_embedder = saved  # type: ignore[assignment]
            _check("HTTP 200（不 500）", r.status_code == 200, f"{r.status_code} {r.text[:160]}")
            _check("ok=true（不阻塞出题）", body.get("ok") is True, str(body)[:240])
            _check("带降级 warning", WARNING_DEGRADED in (body.get("warnings") or []),
                   str(body.get("warnings")))

            # ---------------- [6] 对照 ----------------
            print("\n[6] 同一会话 RAG 开 / 关 Prompt 长度")
            print(f"  RAG 关 {len(prompt_off)} 字 / RAG 开 {len(prompt_on)} 字 "
                  f"/ 差 {len(prompt_on) - len(prompt_off):+d}")

            # ---------------- [7] 真实生产 topic 画像 ----------------
            # 生产 query 不是「长问句」，而是 current_topic(plan, context) 推导出的
            # plan.priority_topics 项 —— 岗位技能短词。本节点把这一形态**实测**出来，
            # 并按**生效阈值**（绝对下限 + 相对阈值 α）给出命中数。
            print("\n[7] 真实生产 topic（current_topic 推导）逐词画像")
            defaults = knowledge_rag.resolve_retriever_defaults()
            floor = defaults.get("min_score")
            ratio = defaults.get("min_score_ratio")
            print(f"  生效规则：score >= max(min_score={floor}, α={ratio} × top1)")
            async with factory() as db:
                session_row = await db.get(InterviewSession, session_id)
                # 与生产同一路径：core._build_session_plan（use_llm=False，确定性）
                plan = await interview_core._build_session_plan(db, session_row)
                ctx = await interview_context.get_context(db, session_id)
                real_topic = interview_core.current_topic(plan, ctx)
                priority = list(_field(plan, "priority_topics") or [])
                job_row = await db.get(Job, job_id)
                # 用「不过滤」的检索器拿完整名次，再离线套生效阈值 —— 依据
                # select_matches 的顺序是「排序 → min_score → top_k」（先过滤后截断）。
                # 上游对 query 侧有频控（11202 licc failed）⇒ 复用 R5 的限速包装
                # （逐条间隔 + 重试 + 缓存），否则连发几个短词必然被打回。
                from rag_min_score_calibration_spark_run import PacedCachingEmbedder

                paced = PacedCachingEmbedder(
                    knowledge_rag.default_embedder(role="query"),
                    pace_seconds=3.0, retries=3, retry_delay=2.0, verbose=False,
                )
                profiler = knowledge_rag.build_vector_retriever(
                    db, embedder=paced, top_k=8, min_score=None,
                )
                for topic in priority:
                    try:
                        hits = await profiler.retrieve(job_row, topic, ctx)
                        error = None
                    except Exception as exc:  # noqa: BLE001 - 上游抖动不中断整轮画像
                        hits, error = [], f"{type(exc).__name__}: {exc}"
                    scored = sorted(
                        (float(getattr(h, "metadata", {}).get("score", 0.0)) for h in hits),
                        reverse=True,
                    )
                    top1 = scored[0] if scored else 0.0
                    effective = floor
                    if ratio is not None and scored:
                        relative = ratio * top1
                        effective = relative if effective is None else max(effective, relative)
                    profile.append({
                        "topic": topic,
                        "top1": round(top1, 6),
                        "candidates": len(scored),
                        "effective_threshold": (None if effective is None
                                                else round(effective, 6)),
                        "hits": sum(1 for s in scored
                                    if effective is None or s >= effective),
                        # 「语料里到底有没有这个主题」：按来源命名空间判
                        # （MySQL → handbook://mysql/*，Python → 无）
                        "covers_topic": any(
                            topic.lower() in (getattr(h, "source", "") or "").lower()
                            for h in hits
                        ),
                        "is_short_term": " " not in topic.strip(),
                        "error": error,
                    })
            for row in profile:
                suffix = f"  [ERROR] {row['error']}" if row.get("error") else ""
                print(f"  {row['topic']:<12} top1={row['top1']:.4f} "
                      f"生效阈值={_fmt(row['effective_threshold'])} "
                      f"命中={row['hits']} 语料覆盖={row['covers_topic']}{suffix}")
            covered = [r for r in profile if r["covers_topic"]]
            uncovered = [r for r in profile if not r["covers_topic"]]
            _check("能从 plan 推导出真实 current_topic", bool(real_topic), repr(real_topic))
            _check("画像覆盖 plan.priority_topics",
                   len(profile) == len(priority), f"{len(profile)} vs {len(priority)}")
            _check("生产 topic 全部是短技能词（无空格）",
                   bool(profile) and all(r["is_short_term"] for r in profile),
                   str([r["topic"] for r in profile]))
            _check("画像无上游错误（结论不被频控污染）",
                   bool(profile) and all(r["error"] is None for r in profile),
                   str([r["error"] for r in profile if r["error"]]))
            # ★ 内容层是否修好：语料**覆盖**的主题必须真的注入知识；
            #   语料**未覆盖**的主题必须注入 0 条（靠绝对下限挡住，而不是硬凑噪声）。
            _check("★ 已覆盖主题在生效阈值下确实注入知识（内容层可用）",
                   bool(covered) and all(r["hits"] > 0 for r in covered),
                   str([(r["topic"], r["hits"]) for r in covered]))
            _check("★ 未覆盖主题在生效阈值下注入 0 条（不靠相对值硬凑无关片段）",
                   all(r["hits"] == 0 for r in uncovered),
                   str([(r["topic"], r["hits"], r["top1"]) for r in uncovered]))
            if not covered:
                findings.append({
                    "kind": "no_production_topic_covered_by_corpus",
                    "topics": [r["topic"] for r in profile],
                    "note": "本次会话的 priority_topics 全部不在语料覆盖范围内",
                })
    finally:
        knowledge_rag.build_vector_retriever = real_builder  # type: ignore[assignment]
        main.spark_api = original_spark  # type: ignore[assignment]
        main.app.dependency_overrides.clear()
        await engine.dispose()
        # 生产连接池也要在**同一个事件循环内**释放，否则 aiomysql 的
        # ``Connection.__del__`` 会在循环关闭后才触发，打印一串无关的
        # "Event loop is closed" 噪声。
        await prod_engine.dispose()

    passed = sum(1 for r in _RESULTS if r["ok"])
    failed = len(_RESULTS) - passed
    print("\n" + "=" * 74)
    print(f"结果: 通过 {passed} 项，失败 {failed} 项；耗时 {time.time() - started:.1f}s")
    print("=" * 74)
    if failed:
        exit_code = 1

    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "runner": "tests/rag_production_chain_spark_run.py",
        "purpose": "真实 Embedding 下的业务链路端到端验证（生产语料只读搬运）",
        "production_snapshot": {
            "documents": len(docs),
            "chunks": len(chunks),
            "embedding_models": models_seen,
            "embedding_dims": dims_seen,
            "unvectorized_chunks": missing,
            "sources": sorted({d["source"] for d in docs}),
        },
        "retriever_config": prod_config,
        "profiler_config": holder.get("retriever_config"),
        "effective_rule": {"min_score": floor, "min_score_ratio": ratio},
        "retrieval_calls": recorder.calls,
        "real_current_topic": real_topic,
        "production_topic_profile": profile,
        "findings": findings,
        "prompt_chars": {"rag_off": len(prompt_off), "rag_on": len(prompt_on)},
        "checks": _RESULTS,
        "passed": passed,
        "failed": failed,
    }
    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"结果写入 {out_path}")
    return exit_code


def _entry() -> int:
    parser = argparse.ArgumentParser(
        description="R7 · 真实 Embedding 下的业务链路端到端验证（不写生产库）")
    parser.add_argument("--out", type=str, default=str(DEFAULT_OUT))
    args = parser.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    # ★ 入口函数**不能叫 ``main``**：本模块顶部有 ``import main``（FastAPI 应用），
    # 同名函数会在模块级把它遮蔽掉，``main.app`` 就变成「函数没有 app 属性」。
    raise SystemExit(_entry())
