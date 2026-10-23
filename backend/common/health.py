# -*- coding: utf-8 -*-
"""运行状态上报：健康检查响应体 + 启动日志。

这是什么
--------
``GET /api/health`` 的 5 个只读状态块，以及它们在启动时打印到日志的孪生函数
（``_report_*``）。两者**同源**：``_report_*`` 一律调用 ``*_status()`` 再打印，
所以「日志里说的」和「健康检查返回的」永远一致（``test_*_surface`` 有断言）。

共同约定（**改动时必须保持**）
------------------------------
1. **只读、永不抛异常**：构造 Provider / 读配置只解析环境变量，不读库、
   不发网络请求、不编码文本。配置非法时把原因放进 ``error`` 字段，
   **不让健康检查 500**——健康检查自己挂掉是最糟的故障模式。
2. **键集是契约**：``embedding`` 4 键 / ``vector`` 3 键 / ``threshold`` 6 键 /
   ``avatar`` 7 键 / ``production`` 12 键。前端与运维脚本按这些键取值，
   加键是兼容的，**删键或改名不是**。
3. ``/api/health`` 的既有 6 键（``status`` / ``database`` / ``version`` …）
   逐字未变；``metrics`` 是第 7 个键，固定 12 键闭集。

迁移说明
--------
本文件**逐字迁出**自 ``main.py``（原 242-661 行，14 个函数 + 段标题注释），
用行切片搬运，没有手抄。迁移后 ``main.py`` 只保留 ``/api/health`` 端点本身
与 lifespan 里的调用点——它们属于装配层。

为什么放在 ``common/`` 而不是 ``modules/``
------------------------------------------
它不属于任何一条业务线：embedding / 向量后端 / RAG 阈值 / 数字人 / AI 模式 /
生产化参数，每一块都横跨多个模块。放 ``modules/`` 里会逼着某一个业务模块
「拥有」别人的运行状态，那是错的归属。
"""

from typing import Any, Dict, Optional

from sqlalchemy import func, select

from models import KnowledgeChunk


# ============================================================
# Embedding 运行状态上报（只读：不读库、不发网络请求、不编码文本）
# ============================================================
def embedding_status() -> Dict[str, Any]:
    """当前进程**默认 Embedding** 的运行状态标识（固定 4 键，成功时 ``error=None``）。

    为什么要有它：``default_embedder()`` 在「一个 ``EMBEDDING_*`` 都不配」时会返回
    ``HashEmbeddingService``（``provider="hash-local"``，**词面哈希、无语义**）。
    那种状态下 RAG **不会报错**——它只是一条都检索不到（检索器按 ``embedding_model``
    过滤，模型名对不上）。于是「部署漏配凭据」的表现是「知识库好像没内容」，
    **静默且无从排查**。本函数把「到底在用哪个 Embedding、它是不是语义模型」
    摆到启动日志与健康检查里，让这件事不可能再被忽略。

    纯读配置：构造 Provider 只解析环境变量，**不读库、不发网络请求、不编码任何文本**，
    因此可安全地在启动期与每次健康检查时调用。
    配置非法（如 ``openai`` 却没给密钥）时**刻意不抛异常**——健康检查永远不该 500，
    改为把错误放进 ``error`` 字段暴露出来。
    """
    try:
        # 延迟导入：与 services.knowledge_rag 同一取舍（模块顶层只留标准库）。
        from services.knowledge_rag import describe_default_embedding

        status: Dict[str, Any] = dict(describe_default_embedding().to_dict())
        status["error"] = None
        return status
    except Exception as exc:  # noqa: BLE001 - 配置错误要变成可见字段，不是 500
        return {
            "provider": None,
            "dimension": None,
            "semantic_enabled": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _report_embedding_status() -> None:
    """把 Embedding 状态打进启动日志；**非语义**时给出醒目告警。"""
    status = embedding_status()
    if status["error"] is not None:
        print(f"[embedding][ERROR] 默认 Embedding 配置非法：{status['error']}")
        return
    if status["semantic_enabled"]:
        print(
            f"[embedding] provider={status['provider']} "
            f"dim={status['dimension']} semantic=True"
        )
        return
    print(
        f"[embedding][WARN] provider={status['provider']} "
        f"dim={status['dimension']} semantic=False —— 这是**离线占位**（词面哈希），"
        "检索出的「相似度」没有语义含义，RAG 知识注入会静默失效。"
        "生产部署请配置 EMBEDDING_* 后重启。"
    )


# ============================================================
# 向量后端运行状态上报（只读配置；切片数只用于**规模告警**，不参与检索）
# ============================================================
def vector_status() -> Dict[str, Any]:
    """当前进程**向量后端**的运行状态（固定 3 键，成功时 ``error=None``）。

    为什么要有它：``VECTOR_STORE`` 不配时默认走 ``vector_store_sql``——它**没有 ANN
    索引**，每次检索都把**全部**切片（含 2560 维向量 JSON）捞进内存做 Python 侧余弦。
    规模小的时候完全无感，规模一大就变成每次出题都读几十 MB。
    本函数把「现在到底走的是哪条路、有没有 ANN」摆到启动日志与健康检查里，
    让「以为开了 ANN、其实还在全表扫描」不可能再被忽略。

    纯读配置：**不读库、不发网络请求、不构造任何后端实例**（``chroma`` 要 import
    ``chromadb``，那是可选依赖，不能因为「看一眼配置」就被拉起来）。
    配置非法（``VECTOR_STORE`` 取值无法识别）时**刻意不抛异常**——健康检查永远不该
    500，改为把错误放进 ``error`` 字段。
    """
    try:
        # 延迟导入：与 services.knowledge_rag 同一取舍（模块顶层只留标准库/ORM）。
        from services.knowledge_rag import describe_vector_backend

        return dict(describe_vector_backend())
    except Exception as exc:  # noqa: BLE001 - 配置错误要变成可见字段，不是 500
        return {"backend": None, "ann": None, "error": f"{type(exc).__name__}: {exc}"}


def _vector_size_warning(backend: Any, chunks: Optional[int]) -> Optional[str]:
    """规模告警文案：全表扫描后端 + 切片数达阈值 ⇒ 返回文案；否则 ``None``。

    单独抽成**纯函数**（不读库、不打印）是为了让它**可被单测直接断言**——
    否则「阈值到底有没有生效」只能靠读日志猜。
    """
    if chunks is None:
        return None
    from services.knowledge_rag import BACKEND_SQL, FULL_SCAN_WARN_CHUNKS

    if backend != BACKEND_SQL or chunks < FULL_SCAN_WARN_CHUNKS:
        return None
    return (
        f"切片数 {chunks} 已达告警阈值 {FULL_SCAN_WARN_CHUNKS}，而向量后端是 "
        f"{BACKEND_SQL}（全表扫描：每次检索都把全部切片连同向量读进内存算余弦）。"
        "建议改用 HNSW ANN 索引：安装 chromadb 后设 VECTOR_STORE=chroma 并重启"
        "（切换前可先跑 backend/tests/rag_vector_backend_switch_run.py 验证）。"
    )


async def _count_knowledge_chunks(conn: Any) -> Optional[int]:
    """数 ``knowledge_chunk`` 有多少行（只读；失败返回 ``None``，**不影响启动**）。"""
    try:
        from models import KnowledgeChunk

        result = await conn.execute(select(func.count()).select_from(KnowledgeChunk))
        return int(result.scalar_one())
    except Exception as exc:  # noqa: BLE001 - 规模告警只是附加信息，绝不能拖垮启动
        print(f"[vector] 切片数统计失败（不影响启动）：{type(exc).__name__}: {exc}")
        return None


def _report_vector_status(chunks: Optional[int] = None) -> None:
    """把向量后端状态打进启动日志；**全表扫描 + 规模超阈值**时给出醒目告警。"""
    status = vector_status()
    if status["error"] is not None:
        print(f"[vector][ERROR] 向量后端配置非法：{status['error']}")
        return

    backend = status["backend"]
    size = "" if chunks is None else f" chunks={chunks}"
    if status["ann"]:
        print(f"[vector] backend={backend} ann=True{size}")
        return

    warning = _vector_size_warning(backend, chunks)
    if warning is not None:
        print(f"[vector][WARN] {warning}")
        return
    print(f"[vector] backend={backend} ann=False{size}（全表扫描）")


# ============================================================
# RAG 阈值溯源上报（只读；判定「阈值是否仍与当前模型 + 语料匹配」）
# ============================================================
def threshold_status(provenance_path: Any = None) -> Dict[str, Any]:
    """当前检索阈值是否仍与**当前 Embedding + 当前语料**匹配（固定 6 键）。

    为什么要有它：``RAG_MIN_SCORE`` / ``RAG_MIN_SCORE_RATIO`` 是在**特定模型 + 特定语料**
    上标定出来的。换模型、改语料之后它们**立刻失效**，但系统**不会报错**——
    表现为「一条都检索不到」或「注入一堆无关噪声」，两次都踩过（见
    ``services/rag_threshold_provenance`` 的模块文档）。本函数把这件事变成
    ``GET /api/health`` 上的一个值 + 启动日志里的一行告警。

    **只读**：读一份 JSON（``scripts/rag_threshold_provenance.json``）、算一次语料文件
    sha256、读一遍环境变量——不碰数据库、不发网络请求。配置非法或产物缺失时
    **刻意不抛异常**：前者进 ``error``，后者是 ``status="unknown"``
    （「无法判定」**不等于**「没问题」）。

    :return: 固定 6 键 —— ``status`` / ``reasons`` / ``notes`` / ``expected`` /
        ``current`` / ``error``。
    :param provenance_path: 溯源产物路径。``None`` → 默认
        （``scripts/rag_threshold_provenance.json``）。**只为可测性留的口子**
        ——健康检查与启动日志都走默认值。
    """
    try:
        from services.knowledge_rag import resolve_retriever_defaults
        from services.rag_threshold_provenance import (
            check_provenance,
            corpus_fingerprint,
            load_provenance,
            resolve_corpus_path,
        )

        provenance = load_provenance(provenance_path)
        embedding = embedding_status()
        corpus_sha = None
        if isinstance(provenance, dict):
            corpus_sha = corpus_fingerprint(
                resolve_corpus_path((provenance.get("corpus") or {}).get("file"))
            )
        result = check_provenance(
            provenance,
            {
                "provider": embedding.get("provider"),
                "dimension": embedding.get("dimension"),
                "corpus_sha256": corpus_sha,
                "params": dict(resolve_retriever_defaults()),
            },
        )
        result["error"] = None
        return result
    except Exception as exc:  # noqa: BLE001 - 配置错误要变成可见字段，不是 500
        return {
            "status": "unknown",
            "reasons": [],
            "notes": [],
            "expected": None,
            "current": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _report_threshold_status(provenance_path: Any = None) -> None:
    """把阈值溯源核对结果打进启动日志；**过期 / 无法判定**时给出醒目告警。

    :param provenance_path: 同 :func:`threshold_status`（只为可测性留的口子）。
    """
    status = threshold_status(provenance_path)
    if status["error"] is not None:
        print(f"[threshold][ERROR] 无法核对阈值溯源：{status['error']}")
        return

    if status["status"] == "ok":
        expected = status.get("expected") or {}
        print(
            f"[threshold] 阈值溯源核对通过（provider={expected.get('provider')} "
            f"dim={expected.get('dimension')} params={expected.get('params')}）"
        )
        return

    if status["status"] == "unknown":
        notes = status.get("notes") or ["无标定溯源产物"]
        print("[threshold][WARN] 无法判定检索阈值是否仍适用：" + "；".join(notes))
        return

    print("[threshold][WARN] 检索阈值可能**已过期**（换过模型或改过语料？）：")
    for reason in status["reasons"]:
        print(f"             · {reason}")
    print(
        "             重跑 tests/rag_production_query_calibration_spark_run.py 后，"
        "用 scripts/apply_threshold_calibration.py --yes 一键写回并登记溯源。"
    )
    for note in status.get("notes") or []:
        print(f"             · （提示）{note}")


# ============================================================
# 数字人通道状态上报（只读配置；**不构造 provider、不联网**）
# ============================================================
def avatar_status() -> Dict[str, Any]:
    """当前进程**数字人 Avatar Provider** 的运行状态（固定 7 键，``error`` 缺省 ``None``）。

    为什么要有它：``interview_mode="avatar"`` 的通道在 provider 不可用时**会静默降级**
    为纯文本交付（这是刻意的——数字人故障不该阻断面试）。副作用是「到底在用 Mock
    还是真讯飞、凭据配齐没有」**从接口返回值上看不出来**。本函数把这件事摆到启动日志
    与健康检查里：``provider`` / ``configured`` / ``host`` / ``error``。

    纯读配置：``describe_avatar_provider`` **不构造 provider、不联网、不抛异常**
    （配置非法时把原因塞进 ``error``），因此可安全地在启动期与每次健康检查时调用。

    :return: ``{provider, known, configured, host, base_path, masked_api_key, error}``。
    """
    try:
        # 延迟导入：与 embedding / vector / threshold 同一取舍（模块顶层只留标准库）。
        from services.avatar_provider_iflytek import describe_avatar_provider

        return describe_avatar_provider()
    except Exception as exc:  # noqa: BLE001 - 配置错误要变成可见字段，不是 500
        return {
            "provider": None,
            "known": [],
            "configured": False,
            "host": "",
            "base_path": "",
            "masked_api_key": "",
            "error": f"{type(exc).__name__}: {exc}",
        }


def _report_auth_throttle_status() -> None:
    """把登录限流的**存储后端**打进启动日志。

    为什么要显式打：这是「能不能上多 worker」的判据之一。如果部署方以为
    已经切到共享表、实际仍在进程内计数，多 worker 后爆破上限会被静默放大 N 倍
    —— 而这种问题**不会**在功能上表现出来，只能靠启动日志暴露。
    """
    from services.login_throttle_store import limits, store_mode

    mode = store_mode()
    max_failures, window_seconds = limits()
    if mode == "db":
        print(
            f"[auth] 登录失败限流 = **共享表**（login_failure）："
            f"{max_failures} 次 / {window_seconds}s，跨实例统一计数。"
        )
    else:
        print(
            f"[auth] 登录失败限流 = **进程内**：{max_failures} 次 / {window_seconds}s。"
            "★ 多 worker 时每个进程独立计数 ⇒ 实际上限 × 进程数；"
            "需要多 worker 请设 AUTH_LOGIN_STORE=db 后重启。"
        )


def _report_avatar_status() -> None:
    """把数字人通道状态打进启动日志。

    三档措辞刻意区分**后果**：
    - ``mock`` → 正常（离线 Mock，不产生真实音视频）；
    - 配置非法 / 凭据不全 → ``[WARN]``（通道**静默降级为纯文本交付**，面试不中断）；
    - 连状态都读不出来 → ``[ERROR]``（不该发生，说明 describe 自身炸了）。
    """
    status = avatar_status()
    provider = status["provider"]
    if provider is None:
        print(f"[avatar][ERROR] 无法读取数字人通道状态：{status['error']}")
        return
    if provider == "mock":
        print(
            "[avatar] provider=mock —— 数字人通道走**离线 Mock**（不产生真实音视频）；"
            "avatar 模式面试照常跑完，交付内容与 text 逐字段一致。"
            "要接真实讯飞虚拟人请配置 AVATAR_PROVIDER=iflytek 与 AVATAR_IFLYTEK_* 后重启。"
        )
        return
    if status["error"] is not None:
        print(
            f"[avatar][WARN] {status['error']} —— 通道会**静默降级为纯文本交付**"
            "（面试不中断，但不会有数字人画面）。"
        )
        return
    print(
        f"[avatar] provider={provider} host={status['host']} "
        f"base_path={status['base_path']} api_key={status['masked_api_key']} configured=True"
    )


def _report_ai_status() -> None:
    """AI 接入层的**启动期校验 + 状态上报**。

    与其它 ``_report_*`` 的区别：这一项**会抛异常**。
    ``AI_RUN_MODE=production`` 但凭据不全（或 Provider / 传输层没装配好）时，
    进程必须**起不来**——"生产环境静默回退到 Mock"是最坏的一种"可用"：
    页面一切正常，只是所有内容都是假的，且没人会发现。
    非生产模式（mock / offline）一律放行，只打一行摘要。
    """
    from modules.ai import describe_run_mode, validate_startup  # noqa: PLC0415 - 启动路径延迟导入

    summary = validate_startup()
    mode = summary["mode"]
    policy = summary["policy"]
    if mode == "production":
        print(
            f"[ai] 运行模式=production provider={summary['provider']} "
            f"超时={policy['timeout_seconds']}s 最大尝试={policy['max_attempts']} —— 调用真实模型"
        )
        return
    detail = describe_run_mode()
    if mode == "mock":
        print(
            "[ai] 运行模式=mock —— 返回**固定占位文本**，不调用真实模型。"
            "该模式只用于联调 / 演示 / 契约测试，**禁止用于生产**。"
        )
        return
    print(
        "[ai] 运行模式=offline —— 不联网，AI 调用一律失败并走各自的降级路径"
        f"（缺凭据：{' / '.join(detail['missing_credentials']) or '无'}）。"
        "要调用真实模型请配置凭据或显式设置 AI_RUN_MODE=production。"
    )


# ============================================================
# 生产化运行时状态上报（只读内存：不读库、不发网络请求、不构造 provider）
# ============================================================
def production_status() -> Dict[str, Any]:
    """实时通道的**生产化运行时状态**（固定 12 键，``error`` 缺省 ``None``）。

    为什么要有它：``GET /api/health`` 此前只回答「依赖配好了没有」，回答不了
    「**现在**跑得怎么样」——活跃连接数、进程内跟踪了多少条会话、空闲超时关掉了几条、
    作答提交/拒绝各多少、会话淘汰了多少。这些是排障与容量判断的**第一手数据**。

    纯读内存：不读库、不发网络请求、不构造 provider（与 embedding / vector /
    threshold / avatar 四个状态块同一取舍），因此可安全地每次健康检查都调用，
    并且**刻意不抛异常**——健康检查永远不该 500，出错就把原因放进 ``error``。

    :return: ``{idle_timeout_seconds, state_ttl_seconds, max_tracked_sessions,
        max_sessions_per_user, max_connections_per_user, min_answer_interval_seconds,
        tracked_sessions, active_connections, active_sessions, busy_sessions,
        metrics, error}``。
    """
    try:
        # 延迟导入：与 embedding / vector / threshold / avatar 同一取舍。
        from services.interview_production import describe_production_status

        return describe_production_status()
    except Exception as exc:  # noqa: BLE001 - 状态错误要变成可见字段，不是 500
        return {
            "idle_timeout_seconds": None,
            "state_ttl_seconds": None,
            "max_tracked_sessions": None,
            "max_sessions_per_user": None,
            "max_connections_per_user": None,
            "min_answer_interval_seconds": None,
            "tracked_sessions": 0,
            "active_connections": 0,
            "active_sessions": 0,
            "busy_sessions": 0,
            "metrics": {"counters": {}, "gauges": {}, "uptime_seconds": 0.0},
            "error": f"{type(exc).__name__}: {exc}",
        }


def _report_production_status() -> None:
    """把生产化策略打进启动日志（**配置非法时醒目告警**）。

    措辞刻意区分**后果**：默认值都是「保护性且宽松」的，配错了不会立刻出事，
    所以必须让操作者一眼看到「我配的那条没生效」。
    """
    status = production_status()
    if status.get("error") is not None:
        print(
            f"[production][ERROR] 生产化配置非法（已回落默认值，保护**未按你的配置生效**）："
            f"{status['error']}"
        )
        return
    idle = status["idle_timeout_seconds"]
    interval = status["min_answer_interval_seconds"]
    rate_note = "未限制" if not interval else f"最小间隔 {interval}s"
    print(
        f"[production] 实时通道：空闲超时 {idle}s / 会话 TTL {status['state_ttl_seconds']}s / "
        f"跟踪上限 {status['max_tracked_sessions']} / 每用户连接上限 "
        f"{status['max_connections_per_user']} / 每用户会话上限 "
        f"{status['max_sessions_per_user']} / 作答限流 {rate_note}"
    )
    if not interval:
        print(
            "[production] （提示）作答限流未开启；需要防连点/脚本重放请设置 "
            "INTERVIEW_ANSWER_MIN_INTERVAL=0.4 后重启。"
        )

