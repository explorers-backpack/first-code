# -*- coding: utf-8 -*-
"""RAG 检索阈值 **标定溯源 + 过期检测 + 一键重标定** 自检（脚本式，非 pytest）。

无需 pytest，直接运行：
    python backend/tests/test_rag_threshold_provenance.py

要防的那件事
------------
``.env`` 里的检索阈值是在**特定 Embedding 模型 + 特定语料**上标定出来的。换模型、
改语料之后它们立刻失效，但系统**不报错**——表现为「一条都检索不到」或「注入一堆
无关噪声」。项目里已踩过两次：哈希时代的 ``min_score=0.25`` 在真实模型下一条都切不掉；
为长问句标定的 ``0.83`` 让生产短词 query 命中 0（等于把 RAG 关掉）。两次都是**静默**的。
本套件锁死「这件事必须可见」，并锁死「一键重标定不会乱改健康配置」。

覆盖：
[1] ``load_provenance`` / ``corpus_fingerprint`` 的宽容语义（缺失 → None，不抛）
[2] 语料指纹口径：CRLF 规范化（跨平台一致）、内容变了指纹就变
[3] ``check_provenance`` 判定：ok / stale（模型 / 维度 / 语料 / 参数缺失 / 参数不一致）/ unknown
[4] ``notes`` 只是提示，**不改变** status（「无法核对」不等于「有问题」，更不等于「没问题」）
[5] ``derive_thresholds``：从真实标定产物推导可行区间；结构不符 → ``DerivationError``
[6] ``within`` 的边界语义：**下界开、上界闭**（与 ``score >= min_score`` 一致）
[7] ``health.threshold_status()``：固定 6 键；真实产物 + 钉扎环境 ⇒ 报「模型已变」（守卫真的在干活）
[8] ``GET /api/health`` 回带 ``threshold``；``status`` / ``service`` / ``embedding`` / ``vector`` 向后兼容
[9] 启动日志：ok / stale / unknown 三种文案
[10] AST 守卫：溯源模块**顶层不 import 任何项目模块**（零依赖），且不含
     ``test_rag_retriever_config`` 扫描的那三个带引号 token（否则那套会红）
[11] 一键重标定的纯函数：``_read_env_value`` / ``_set_env_value`` / ``_plan_param``

环境钉扎：``import regression_env``（``EMBEDDING_*`` / ``RAG_*`` 都会被钉成空串）。
★ 因此**本套件里当前 Embedding 是 hash-local / 256**，而仓库里的真实溯源产物记的是
xinghuo-embedding / 2560 ⇒ [7] 应当报 **stale**。这不是 bug，正是守卫在干活
（「换过模型」确实会让那套阈值失效）。要构造 ok 档，本套件用**临时产物**。
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import importlib.util
import io
import json
import os
import pathlib
import sys
import tempfile
from typing import Any, Dict, List, Optional

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import regression_env  # noqa: E402,F401  （必须在项目模块之前）
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from httpx import ASGITransport, AsyncClient  # noqa: E402

import main  # noqa: E402
from common import health  # noqa: E402
from services import rag_threshold_provenance as prov  # noqa: E402

#: ``threshold_status()`` 的固定键集。**改它就要同步改这里**（契约）。
STATUS_KEYS = {"status", "reasons", "notes", "expected", "current", "error"}

#: ``check_provenance`` 的固定键集。
CHECK_KEYS = set(prov.CHECK_RESULT_KEYS)

#: 会被本套件临时改写、事后必须还原的环境变量。
MUTATED_ENV = ("EMBEDDING_PROVIDER", "EMBEDDING_API_KEY", "RAG_TOP_K",
               "RAG_MIN_SCORE", "RAG_MIN_SCORE_RATIO")

#: ``test_rag_retriever_config`` 扫描的带引号 token（**运行时拼出**，
#: 免得本文件自己成为那套守卫的误报来源）。
GUARDED_TOKENS = tuple("RAG_" + suffix for suffix in ("TOP_K", "MIN_SCORE", "MIN_SCORE_RATIO"))

_PASSED = 0
_FAILED = 0


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name
          + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def _snapshot_env() -> Dict[str, Any]:
    return {k: os.environ.get(k) for k in MUTATED_ENV}


def _restore_env(saved: Dict[str, Any]) -> None:
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _top_level_imports(source: str) -> List[str]:
    """AST：**只看顶层** ``ast.parse(src).body`` 的 import（延迟导入不算）。"""
    out: List[str] = []
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            out.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            out.append(node.module or "")
    return out


def _write_json(path: pathlib.Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_script_module():
    """按路径加载 ``scripts/apply_threshold_calibration.py``（``scripts/`` 不是包）。"""
    path = BACKEND_DIR / "scripts" / "apply_threshold_calibration.py"
    spec = importlib.util.spec_from_file_location("_apply_threshold_calibration", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _sample_calibration() -> Dict[str, Any]:
    """最小可用的标定产物样本（**结构照抄真实产物**，数值是编的）。"""
    return {
        "generated_at": "2026-01-01T00:00:00+0800",
        "runner": "tests/rag_production_query_calibration_spark_run.py",
        "short_query": {
            "total_candidates": 3,
            "topics": {
                # 未覆盖主题（没有 is_target）⇒ 提供窗口下界
                "Python": {"landscape": [{"score": 0.70, "source": "x", "is_target": False},
                                         {"score": 0.60, "source": "y", "is_target": False}]},
                # 已覆盖主题 ⇒ 提供窗口上界
                "MySQL": {"landscape": [{"score": 0.80, "source": "handbook://mysql/i",
                                         "is_target": True},
                                        {"score": 0.55, "source": "z", "is_target": False}]},
            },
        },
        "combined_alpha": {"lo_exclusive": 0.99, "hi_inclusive": 0.995,
                           "midpoint": 0.9925, "scenarios": ["short::MySQL"]},
    }


async def run() -> bool:
    print("=" * 72)
    print("RAG 检索阈值溯源 / 过期检测 / 一键重标定自检")
    print("=" * 72)

    saved = _snapshot_env()
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="career_ai_threshold_"))
    try:
        # ---------------- [1] 宽容语义 ----------------
        print("\n[1] load_provenance / corpus_fingerprint 的宽容语义")
        _check("缺失文件 → None（不抛）",
               prov.load_provenance(tmp / "nope.json") is None)
        bad = tmp / "bad.json"
        bad.write_text("{ not json", encoding="utf-8")
        _check("JSON 坏了 → None（不抛）", prov.load_provenance(bad) is None)
        arr = tmp / "arr.json"
        arr.write_text("[1,2,3]", encoding="utf-8")
        _check("顶层不是对象 → None（不抛）", prov.load_provenance(arr) is None)
        _check("corpus_fingerprint 缺文件 → None",
               prov.corpus_fingerprint(tmp / "nope.json") is None)
        _check("corpus_fingerprint(None) → None", prov.corpus_fingerprint(None) is None)

        # ---------------- [2] 指纹口径 ----------------
        print("\n[2] 语料指纹口径（CRLF 规范化 ⇒ 跨平台一致）")
        crlf = tmp / "crlf.json"
        lf = tmp / "lf.json"
        crlf.write_bytes(b'{"a": 1}\r\n{"b": 2}\r\n')
        lf.write_bytes(b'{"a": 1}\n{"b": 2}\n')
        fp_crlf, fp_lf = prov.corpus_fingerprint(crlf), prov.corpus_fingerprint(lf)
        _check("★ CRLF 与 LF 内容相同 ⇒ 指纹相同（跨平台不误报）",
               fp_crlf == fp_lf and fp_crlf is not None, f"{fp_crlf} vs {fp_lf}")
        other = tmp / "other.json"
        other.write_bytes(b'{"a": 1}\n{"b": 3}\n')
        _check("★ 内容变了 ⇒ 指纹变了", prov.corpus_fingerprint(other) != fp_lf)
        _check("  └ 且是 64 位十六进制（sha256）",
               len(str(fp_lf)) == 64 and all(c in "0123456789abcdef" for c in str(fp_lf)),
               str(fp_lf))

        # ---------------- [3] check_provenance 判定 ----------------
        print("\n[3] check_provenance：ok / stale / unknown")
        base = {
            "provider": "xinghuo-embedding", "dimension": 2560,
            "corpus_sha256": "abc", "params": {"top_k": 8, "min_score": 0.7,
                                               "min_score_ratio": 0.99},
        }
        good = {
            "generated_at": "T",
            "embedder": {"provider": "xinghuo-embedding", "dimension": 2560},
            "corpus": {"file": "scripts/x.json", "sha256": "abc"},
            "params": {"top_k": 8, "min_score": 0.7, "min_score_ratio": 0.99},
        }
        result = prov.check_provenance(good, base)
        _check("键集恰好 5 键", set(result) == CHECK_KEYS, str(sorted(result)))
        _check("★ 完全一致 ⇒ status == ok",
               result["status"] == prov.STATUS_OK and result["reasons"] == [], str(result))

        _check("无产物 ⇒ unknown（**不是** ok）",
               prov.check_provenance(None, base)["status"] == prov.STATUS_UNKNOWN)
        _check("  └ 且给出可执行建议（指向重标定脚本）",
               any("apply_threshold_calibration" in n
                   for n in prov.check_provenance(None, base)["notes"]),
               str(prov.check_provenance(None, base)["notes"]))

        changed_model = dict(base, provider="hash-local")
        r = prov.check_provenance(good, changed_model)
        _check("★ 换模型 ⇒ stale 且理由点明模型",
               r["status"] == prov.STATUS_STALE and any("Embedding 已变" in x for x in r["reasons"]),
               str(r["reasons"]))

        r = prov.check_provenance(good, dict(base, dimension=256))
        _check("★ 换维度 ⇒ stale",
               r["status"] == prov.STATUS_STALE and any("维度" in x for x in r["reasons"]),
               str(r["reasons"]))

        r = prov.check_provenance(good, dict(base, corpus_sha256="def"))
        _check("★ 语料变了 ⇒ stale",
               r["status"] == prov.STATUS_STALE and any("语料已变" in x for x in r["reasons"]),
               str(r["reasons"]))

        r = prov.check_provenance(good, dict(base, params={"top_k": 8, "min_score": 0.9,
                                                           "min_score_ratio": 0.99}))
        _check("★ 参数被手工改过 ⇒ stale",
               r["status"] == prov.STATUS_STALE and any("min_score" in x for x in r["reasons"]),
               str(r["reasons"]))

        r = prov.check_provenance(good, dict(base, params={"top_k": 8, "min_score_ratio": 0.99}))
        _check("★ 参数被**删掉**（未配置）⇒ stale",
               r["status"] == prov.STATUS_STALE and any("未配置" in x for x in r["reasons"]),
               str(r["reasons"]))

        # ---------------- [4] notes 不改变 status ----------------
        print("\n[4] notes 只是提示，**不改变** status")
        r = prov.check_provenance(good, dict(base, corpus_sha256=None))
        _check("★ 语料读不到 ⇒ 仍是 ok + 一条 note（不是 stale，也不是静默通过）",
               r["status"] == prov.STATUS_OK and any("读不到" in n for n in r["notes"]),
               f"{r['status']} {r['notes']}")
        r = prov.check_provenance(good, dict(base, provider=None, dimension=None))
        _check("★ 当前 Embedding 无法确定 ⇒ ok + note（跳过模型核对）",
               r["status"] == prov.STATUS_OK and any("跳过模型核对" in n for n in r["notes"]),
               f"{r['status']} {r['notes']}")
        _check("★ 反例：真过期时 status 必须变 stale（note 机制没把 stale 吞掉）",
               prov.check_provenance(good, changed_model)["status"] == prov.STATUS_STALE)

        # ---------------- [5] derive_thresholds ----------------
        print("\n[5] derive_thresholds：从标定产物推导可行区间")
        derived = prov.derive_thresholds(_sample_calibration())
        ms = derived["min_score"]
        _check("★ min_score 下界 = 未覆盖主题最高 top1", ms["lo_exclusive"] == 0.70, str(ms))
        _check("★ min_score 上界 = 已覆盖主题最低 top1", ms["hi_inclusive"] == 0.80, str(ms))
        _check("★ 推荐值 = 窗口中点（全精度）", ms["recommended"] == 0.75, str(ms["recommended"]))
        alpha = derived["min_score_ratio"]
        _check("★ α 区间取自 combined_alpha",
               alpha["lo_exclusive"] == 0.99 and alpha["hi_inclusive"] == 0.995, str(alpha))
        _check("★ α 推荐值 = 区间中点", alpha["recommended"] == 0.9925, str(alpha["recommended"]))

        for bad_payload, label in (
            ({}, "空对象"),
            ({"short_query": {"topics": {}}}, "无 topics"),
            ({"short_query": {"topics": {"A": {"landscape": [{"score": 0.5}]}}}}, "无 combined_alpha"),
        ):
            try:
                prov.derive_thresholds(bad_payload)
                raised: Optional[BaseException] = None
            except prov.DerivationError as exc:
                raised = exc
            _check(f"结构不符（{label}）⇒ DerivationError", raised is not None, repr(raised))

        only_uncovered = {"short_query": {"topics": {"P": {"landscape": [
            {"score": 0.7, "is_target": False}]}}},
            "combined_alpha": {"lo_exclusive": 0.9, "hi_inclusive": 0.95}}
        d2 = prov.derive_thresholds(only_uncovered)
        _check("★ 只有未覆盖主题 ⇒ 绝对下限**不可推**（推荐值为 None，不猜）",
               d2["min_score"]["recommended"] is None and bool(d2["min_score"].get("reason")),
               str(d2["min_score"]))

        # ---------------- [6] within 边界语义 ----------------
        print("\n[6] within：下界**开**、上界**闭**")
        _check("★ 等于下界 ⇒ 不在区间内（放进来会注入未覆盖主题）",
               prov.within(0.70, 0.70, 0.80) is False)
        _check("★ 等于上界 ⇒ 在区间内（仍能保住已覆盖主题）",
               prov.within(0.80, 0.70, 0.80) is True)
        _check("区间内 ⇒ True", prov.within(0.75, 0.70, 0.80) is True)
        _check("区间外 ⇒ False", prov.within(0.81, 0.70, 0.80) is False)
        _check("非数值 ⇒ False（不抛）", prov.within("x", 0.70, 0.80) is False)
        _check("★ 判定用**全精度**：0.7000000001 仍然 <= 下界之外",
               prov.within(0.7000000001, 0.70, 0.80) is True
               and prov.within(0.70, 0.70, 0.80) is False)

        # ---------------- [7] health.threshold_status ----------------
        print("\n[7] health.threshold_status()：形状 + 真实产物下的判定")
        real = health.threshold_status()
        _check("键集恰好 6 键", set(real) == STATUS_KEYS, str(sorted(real)))
        _check("error is None", real["error"] is None, repr(real["error"]))
        _check("★ 真实产物 + 钉扎（hash-local）⇒ **stale**（守卫在干活：模型确实变了）",
               real["status"] == prov.STATUS_STALE
               and any("Embedding 已变" in x for x in real["reasons"]),
               f"{real['status']} {real['reasons']}")
        _check("  └ expected 快照里记的是真实产物里的模型",
               (real.get("expected") or {}).get("provider") == "xinghuo-embedding",
               str(real.get("expected")))
        _check("  └ current 快照里是钉扎后的模型",
               (real.get("current") or {}).get("provider") == "hash-local",
               str(real.get("current")))

        # 构造「与当前环境完全匹配」的临时产物 ⇒ 应报 ok
        corpus = tmp / "corpus.json"
        corpus.write_text('{"documents": []}\n', encoding="utf-8")
        params = {"top_k": 8, "min_score": 0.7, "min_score_ratio": 0.99}
        os.environ["RAG_TOP_K"] = "8"
        os.environ["RAG_MIN_SCORE"] = "0.7"
        os.environ["RAG_MIN_SCORE_RATIO"] = "0.99"
        matching = tmp / "prov_ok.json"
        _write_json(matching, {
            "generated_at": "T",
            "embedder": {"provider": "hash-local", "dimension": 256},
            "corpus": {"file": str(corpus), "sha256": prov.corpus_fingerprint(corpus)},
            "params": params,
        })
        ok = health.threshold_status(matching)
        _check("★ 与当前环境匹配的产物 ⇒ status == ok",
               ok["status"] == prov.STATUS_OK, f"{ok['status']} {ok['reasons']}")
        _check("  └ current.params 与配置逐键相等",
               (ok.get("current") or {}).get("params") == params,
               str((ok.get("current") or {}).get("params")))

        os.environ["RAG_MIN_SCORE"] = "0.95"
        bad_param = health.threshold_status(matching)
        _check("★ 改 .env 阈值 ⇒ 立刻 stale（溯源能抓到配置漂移）",
               bad_param["status"] == prov.STATUS_STALE
               and any("min_score" in x for x in bad_param["reasons"]),
               str(bad_param["reasons"]))
        _restore_env(saved)

        missing = health.threshold_status(tmp / "nope.json")
        _check("★ 产物缺失 ⇒ unknown + error is None（不 500）",
               missing["status"] == prov.STATUS_UNKNOWN and missing["error"] is None,
               str(missing))

        # ---------------- [8] /api/health ----------------
        print("\n[8] GET /api/health 回带 threshold（且向后兼容）")
        transport = ASGITransport(app=main.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/api/health")
            _check("状态码 200", r.status_code == 200, str(r.status_code))
            body = r.json()
            _check("★ 既有键 status 未变", body.get("status") == "ok", repr(body.get("status")))
            _check("★ 既有键 service 未变",
                   body.get("service") == "Career.ai FastAPI", repr(body.get("service")))
            _check("★ 既有键 embedding 仍在", isinstance(body.get("embedding"), dict))
            _check("★ 既有键 vector 仍在", isinstance(body.get("vector"), dict))
            _check("★ 新增键 threshold 存在", "threshold" in body, str(list(body.keys())))
            _check("threshold 键集为 6 键",
                   set(body.get("threshold") or {}) == STATUS_KEYS,
                   str(sorted((body.get("threshold") or {}).keys())))
            _check("threshold 与 threshold_status() 同源（逐键相等）",
                   body.get("threshold") == health.threshold_status(),
                   f"{body.get('threshold')} vs {health.threshold_status()}")

        # ---------------- [9] 启动日志 ----------------
        print("\n[9] 启动日志（ok / stale / unknown 三态都要能看出来）")
        # ok 档需要「配置 + 模型 + 语料」三者都匹配 ⇒ 这里把配置调回 matching 记录的值
        os.environ["RAG_TOP_K"] = "8"
        os.environ["RAG_MIN_SCORE"] = "0.7"
        os.environ["RAG_MIN_SCORE_RATIO"] = "0.99"
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            health._report_threshold_status(matching)
        text = buf.getvalue()
        _check("ok ⇒ 含「核对通过」且无 WARN",
               "核对通过" in text and "WARN" not in text, repr(text))

        _restore_env(saved)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            health._report_threshold_status()
        text = buf.getvalue()
        _check("★ stale ⇒ 含 WARN 且列出理由", "WARN" in text and "过期" in text, repr(text))
        _check("  └ 给出可执行的补救步骤（重跑标定 + 一键写回）",
               "apply_threshold_calibration" in text, repr(text))

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            health._report_threshold_status(tmp / "nope.json")
        text = buf.getvalue()
        _check("★ unknown ⇒ 含 WARN 且**明确说「无法判定」**（不说成通过）",
               "WARN" in text and "无法判定" in text, repr(text))

        # ---------------- [10] AST 守卫 ----------------
        print("\n[10] 源码守卫：溯源模块零项目依赖 + 不撞 token 守卫")
        src = (BACKEND_DIR / "services" / "rag_threshold_provenance.py").read_text(encoding="utf-8")
        top = _top_level_imports(src)
        project_imports = [m for m in top if m.startswith(("services", "api", "models",
                                                          "schemas", "utils", "database", "deps"))]
        _check("★ 顶层不 import 任何项目模块（连 knowledge_rag 都不 import）",
               project_imports == [], str(project_imports))
        _check("顶层只 import 标准库",
               all(m in ("__future__", "hashlib", "json", "pathlib", "typing") for m in top),
               str(top))
        for token in GUARDED_TOKENS:
            _check(f"★ 不含带引号的 {token}（否则 test_rag_retriever_config 的闭集守卫会红）",
                   f'"{token}"' not in src)
        # 守卫自检：确认这个判据**能**失败（否则恒为真）
        fake = 'import os\nX = "RAG_MIN_SCORE"\n'
        _check("守卫自检：写入带引号 token 时判据确实会命中",
               f'"{GUARDED_TOKENS[1]}"' in fake)

        # ---------------- [11] 一键重标定的纯函数 ----------------
        print("\n[11] 一键重标定：.env 读写与「保留 / 替换」决策")
        script = _load_script_module()
        env_text = "# 注释行 RAG_MIN_SCORE=9\nRAG_TOP_K=8\nRAG_MIN_SCORE=0.7685\n"
        _check("读得到键值", script._read_env_value(env_text, "RAG_MIN_SCORE") == "0.7685")
        _check("★ 不匹配注释行（注释里的同名键不算配置）",
               script._read_env_value("# RAG_MIN_SCORE=9", "RAG_MIN_SCORE") is None)
        _check("键不存在 → None", script._read_env_value(env_text, "RAG_MIN_SCORE_RATIO") is None)

        new_text, changed = script._set_env_value(env_text, "RAG_MIN_SCORE", "0.7")
        _check("替换已存在的行", changed and "RAG_MIN_SCORE=0.7" in new_text)
        _check("  └ 只动目标行（其它行原样）",
               "RAG_TOP_K=8" in new_text and "# 注释行 RAG_MIN_SCORE=9" in new_text)
        _check("  └ 只替换**第一处**（注释行没被碰）",
               new_text.count("RAG_MIN_SCORE=0.7") == 1)
        appended, changed2 = script._set_env_value(env_text, "RAG_MIN_SCORE_RATIO", "0.99")
        _check("不存在的键 ⇒ 追加", changed2 and "RAG_MIN_SCORE_RATIO=0.99" in appended)
        same, changed3 = script._set_env_value(env_text, "RAG_MIN_SCORE", "0.7685")
        _check("值相同时报告「无变化」", changed3 is False and same == env_text)

        window = {"lo_exclusive": 0.766, "hi_inclusive": 0.770, "recommended": 0.768}
        plan = script._plan_param("min_score", "0.7685", window)
        _check("★ 现有值在区间内 ⇒ **保留**（本工具不改健康的配置）",
               plan["action"] == "keep" and plan["value"] == "0.7685", str(plan))
        plan = script._plan_param("min_score", "0.83", window)
        _check("★ 现有值越界 ⇒ 采用推荐值（6 位小数）",
               plan["action"] == "set" and plan["value"] == "0.768000", str(plan))
        plan = script._plan_param("min_score", None, window)
        _check("未配置 + 有推荐值 ⇒ set", plan["action"] == "set", str(plan))
        plan = script._plan_param("min_score", "0.83",
                                  {"lo_exclusive": None, "hi_inclusive": None,
                                   "recommended": None})
        _check("★ 区间推不出来 ⇒ 保留现有值并提示人工复核（不猜）",
               plan["action"] == "keep" and plan["value"] == "0.83"
               and "人工" in plan["note"], str(plan))
        _check("边界：等于上界 ⇒ 保留", script._plan_param(
            "min_score", "0.770", window)["action"] == "keep")
        _check("边界：等于下界 ⇒ 越界（替换）", script._plan_param(
            "min_score", "0.766", window)["action"] == "set")
    finally:
        _restore_env(saved)
        import shutil

        shutil.rmtree(tmp, ignore_errors=True)

    print("\n" + "=" * 72)
    print(f"通过 {_PASSED} / {_PASSED + _FAILED}")
    print("=" * 72)
    return _FAILED == 0


def main_entry() -> None:
    ok = asyncio.run(run())
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main_entry()
