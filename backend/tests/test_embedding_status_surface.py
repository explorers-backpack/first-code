# -*- coding: utf-8 -*-
"""默认 Embedding **运行状态上报**自检（``main.embedding_status`` / ``GET /api/health``）

无需 pytest，直接运行：
    python backend/tests/test_embedding_status_surface.py

要防的那件事
------------
``default_embedder()`` 在「一个 ``EMBEDDING_*`` 都不配」时返回 ``HashEmbeddingService``
（``provider="hash-local"``，**词面哈希、无语义**）。那种状态下 RAG **不报错**——
检索器按 ``embedding_model`` 过滤，模型名对不上 ⇒ 一条都检索不到 ⇒ 表现是
「知识库好像没内容」。**部署漏配凭据因此是完全静默的。**
本套件锁死「这件事必须可见」：健康检查与启动日志都要如实上报。

覆盖：
[1] 形状契约：固定 4 键；成功时 ``error=None``；``semantic_enabled`` 是真 bool
[2] **未配置档** ⇒ ``hash-local`` / 256 / ``semantic_enabled=False``（静默降级那一档）
[3] **真实 provider 档**（假凭据）⇒ ``semantic_enabled=True``，且**构造期零网络请求**
[4] **配置非法档** ⇒ ``error`` 非空且**不抛异常**（健康检查永远不该 500）
[5] ``GET /api/health`` 回带 ``embedding``；``status`` / ``service`` 键保持向后兼容
[6] 启动日志：非语义 ⇒ 含 ``WARN``；语义 ⇒ 含 provider 名
[7] AST 守卫：``main.py`` **顶层不** import ``services.knowledge_rag``（必须是函数内延迟导入）
[8] 哈希时代的旧标定产物**仍带** ``superseded_by`` 标记（防回退）

环境钉扎：必须 ``import regression_env``——``.env`` 里的 ``EMBEDDING_PROVIDER=spark``
会被 ``load_dotenv`` 带进 ``os.environ``，不钉扎本套件就会读到真实部署取值（不确定）。
**本套件全程离线**：``describe_default_embedding`` 只解析配置、不发网络请求。
"""

import ast
import asyncio
import contextlib
import io
import json
import os
import pathlib
import sys
from typing import Any, Dict, List

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import regression_env  # noqa: E402,F401  （必须在项目模块之前）

from httpx import ASGITransport, AsyncClient  # noqa: E402

import main  # noqa: E402

#: ``embedding_status()`` 的固定键集。**改它就要同步改这里**（契约）。
STATUS_KEYS = {"provider", "dimension", "semantic_enabled", "error"}

#: 会被本套件临时改写、事后必须还原的环境变量。
MUTATED_ENV = (
    "EMBEDDING_PROVIDER",
    "EMBEDDING_API_KEY",
    "EMBEDDING_MODEL",
    "EMBEDDING_BASE_URL",
    "SPARK_EMBEDDING_APP_ID",
    "SPARK_EMBEDDING_API_KEY",
    "SPARK_EMBEDDING_API_SECRET",
)

#: 哈希时代的旧标定产物——它们必须在文件里自带废弃说明。
SUPERSEDED_ARTIFACTS = (
    "scripts/rag_min_score_calibration.json",
    "scripts/rag_top_k_calibration.json",
)

_PASSED = 0
_FAILED = 0


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(
        ("  [PASS] " if cond else "  [FAIL] ")
        + name
        + (f"  -> {detail}" if detail and not cond else "")
    )
    return cond


def _snapshot_env() -> Dict[str, Any]:
    return {k: os.environ.get(k) for k in MUTATED_ENV}


def _restore_env(saved: Dict[str, Any]) -> None:
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _clear_all_embedding_env() -> None:
    """把全部 Embedding 相关旋钮**设成空串**（不是 pop）。

    为什么不是 ``pop``：``database`` 在 import 期已 ``load_dotenv(override=False)``，
    已存在的键不会被再灌回，但一旦 ``pop`` 掉，任何后续的 ``load_dotenv`` 又会把它读回来。
    空串 = 项目约定的「未配置」，语义明确且稳定。
    """
    for key in MUTATED_ENV:
        os.environ[key] = ""


def _top_level_imports(source: str) -> List[str]:
    """AST：**只看顶层** ``ast.parse(src).body`` 的 import（延迟导入不算）。"""
    out: List[str] = []
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            out.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            out.append(node.module or "")
    return out


async def run() -> bool:
    print("=" * 68)
    print("默认 Embedding 运行状态上报自检（embedding_status / /api/health）")
    print("=" * 68)

    saved = _snapshot_env()

    # ---------------- [1] 形状契约 ----------------
    print("\n[1] 形状契约（固定 4 键）")
    _clear_all_embedding_env()
    status = main.embedding_status()
    _check("键集恰好为 4 键", set(status) == STATUS_KEYS, str(sorted(status)))
    _check("成功时 error is None", status["error"] is None, repr(status["error"]))
    _check(
        "semantic_enabled 是**真 bool**（不是 0 / 不是字符串）",
        isinstance(status["semantic_enabled"], bool),
        repr(status["semantic_enabled"]),
    )
    _check(
        "dimension 是 int（或 None）",
        status["dimension"] is None or isinstance(status["dimension"], int),
        repr(status["dimension"]),
    )

    # ---------------- [2] 未配置档：静默降级的那一档 ----------------
    print("\n[2] 未配置档 ⇒ 离线哈希占位（**这就是以前完全静默的情形**）")
    _clear_all_embedding_env()
    status = main.embedding_status()
    _check("provider == hash-local", status["provider"] == "hash-local", repr(status["provider"]))
    _check("dimension == 256", status["dimension"] == 256, repr(status["dimension"]))
    _check("★ semantic_enabled is False", status["semantic_enabled"] is False, repr(status["semantic_enabled"]))
    _check("error is None（未配置**不是**配置错误）", status["error"] is None, repr(status["error"]))

    # ---------------- [3] 真实 provider 档（假凭据，零网络） ----------------
    print("\n[3] 真实 provider 档（假凭据）⇒ semantic=True，且构造期零网络")
    _clear_all_embedding_env()
    os.environ["EMBEDDING_PROVIDER"] = "openai"
    os.environ["EMBEDDING_API_KEY"] = "sk-fake-for-offline-test"
    os.environ["EMBEDDING_MODEL"] = "text-embedding-3-small"
    status = main.embedding_status()
    _check("provider == text-embedding-3-small", status["provider"] == "text-embedding-3-small", repr(status["provider"]))
    _check("★ semantic_enabled is True", status["semantic_enabled"] is True, repr(status["semantic_enabled"]))
    _check("error is None", status["error"] is None, repr(status["error"]))
    _check(
        "★ 假凭据也能构造成功 ⇒ 构造期**没有**发网络请求（真发请求会 401）",
        status["provider"] is not None,
        repr(status),
    )
    _check(
        "未配置 EMBEDDING_DIMENSION 时 dimension == 0（= 不校验，按上游返回）",
        status["dimension"] == 0,
        repr(status["dimension"]),
    )

    # ---------------- [4] 配置非法档 ----------------
    print("\n[4] 配置非法档 ⇒ 错误可见、但**不抛异常**")
    _clear_all_embedding_env()
    os.environ["EMBEDDING_PROVIDER"] = "openai"
    os.environ["EMBEDDING_API_KEY"] = ""
    try:
        status = main.embedding_status()
        raised = None
    except Exception as exc:  # noqa: BLE001
        status, raised = {}, exc
    _check("★ 不抛异常（健康检查永远不该 500）", raised is None, repr(raised))
    _check("error 非空", bool(status.get("error")), repr(status.get("error")))
    _check("provider is None", status.get("provider") is None, repr(status.get("provider")))
    _check("semantic_enabled is None（未知，不伪装成 False）", status.get("semantic_enabled") is None, repr(status.get("semantic_enabled")))
    _check("键集仍为 4 键", set(status) == STATUS_KEYS, str(sorted(status)))
    _check("错误里带得出具体原因", "EMBEDDING_API_KEY" in str(status.get("error")), str(status.get("error"))[:120])

    # ---------------- [5] /api/health ----------------
    print("\n[5] GET /api/health 回带 embedding（且向后兼容）")
    _restore_env(saved)
    transport = ASGITransport(app=main.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/api/health")
        _check("状态码 200", r.status_code == 200, str(r.status_code))
        body = r.json()
        _check("★ 既有键 status 未变", body.get("status") == "ok", repr(body.get("status")))
        _check("★ 既有键 service 未变", body.get("service") == "Career.ai FastAPI", repr(body.get("service")))
        _check("★ 新增键 embedding 存在", "embedding" in body, str(list(body.keys())))
        _check("embedding 是 dict", isinstance(body.get("embedding"), dict), repr(body.get("embedding")))
        _check(
            "embedding 键集为 4 键",
            set(body.get("embedding") or {}) == STATUS_KEYS,
            str(sorted((body.get("embedding") or {}).keys())),
        )
        _check(
            "embedding 与 embedding_status() 同源（逐键相等）",
            body.get("embedding") == main.embedding_status(),
            f"{body.get('embedding')} vs {main.embedding_status()}",
        )
        _check(
            "钉扎环境下报 hash-local / semantic=False",
            body["embedding"]["provider"] == "hash-local"
            and body["embedding"]["semantic_enabled"] is False,
            str(body["embedding"]),
        )

    # ---------------- [6] 启动日志 ----------------
    print("\n[6] 启动日志（非语义必须告警）")
    _clear_all_embedding_env()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        main._report_embedding_status()
    text = buf.getvalue()
    _check("非语义 ⇒ 日志含 WARN", "WARN" in text, repr(text))
    _check("非语义 ⇒ 日志含 provider 名", "hash-local" in text, repr(text))
    _check("非语义 ⇒ 日志点明「无语义/离线占位」", ("离线占位" in text or "无语义" in text), repr(text))

    _clear_all_embedding_env()
    os.environ["EMBEDDING_PROVIDER"] = "openai"
    os.environ["EMBEDDING_API_KEY"] = "sk-fake-for-offline-test"
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        main._report_embedding_status()
    text = buf.getvalue()
    _check("语义 ⇒ 日志无 WARN", "WARN" not in text, repr(text))
    _check("语义 ⇒ 日志含 semantic=True", "semantic=True" in text, repr(text))

    _restore_env(saved)

    # ---------------- [7] AST 守卫：延迟导入 ----------------
    print("\n[7] 源码守卫（AST）：main.py 顶层不 import services.knowledge_rag")
    main_src = (_BACKEND / "main.py").read_text(encoding="utf-8")
    top = _top_level_imports(main_src)
    _check(
        "★ 顶层**不**出现 services.knowledge_rag（必须函数内延迟导入）",
        "services.knowledge_rag" not in top,
        str([m for m in top if "knowledge" in m or "services" in m]),
    )
    _check(
        "顶层不出现 services.embedding_provider",
        "services.embedding_provider" not in top,
        str([m for m in top if "embedding" in m]),
    )
    # 守卫自检：确认这个判据**能**失败（否则恒为真）
    fake = "from services.knowledge_rag import describe_default_embedding\n"
    _check(
        "守卫自检：顶层 import 时判据确实会命中",
        "services.knowledge_rag" in _top_level_imports(fake),
        str(_top_level_imports(fake)),
    )

    # ---------------- [8] 旧产物废弃标记 ----------------
    print("\n[8] 哈希时代的旧标定产物仍带 superseded_by 标记")
    for rel in SUPERSEDED_ARTIFACTS:
        path = _BACKEND / rel
        _check(f"{rel} 存在", path.exists(), str(path))
        if not path.exists():
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        marker = data.get("superseded_by")
        _check(f"{rel} 带 superseded_by", isinstance(marker, dict), repr(type(marker)))
        if not isinstance(marker, dict):
            continue
        _check(f"{rel} status 已标记", bool(marker.get("status")), repr(marker.get("status")))
        _check(
            f"{rel} 说明了「哈希分数不可比」",
            "哈希" in str(marker.get("reason", "")),
            str(marker.get("reason"))[:100],
        )
        _check(
            f"{rel} 给出了当前生产取值",
            isinstance(marker.get("current_production_values"), dict),
            repr(marker.get("current_production_values")),
        )

    print("\n" + "=" * 68)
    print(f"通过 {_PASSED} / {_PASSED + _FAILED}")
    print("=" * 68)
    return _FAILED == 0


def main_entry() -> None:
    ok = asyncio.run(run())
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main_entry()
