# -*- coding: utf-8 -*-
"""RAG 检索阈值 · **一键重标定**（把标定产物的结论写回 ``.env`` 并登记溯源）。

它解决什么
----------
``.env`` 里的 ``RAG_MIN_SCORE`` / ``RAG_MIN_SCORE_RATIO`` 是在**特定 Embedding 模型 +
特定语料**上标定出来的。换模型、改语料之后必须重算——但「重算完还要手工改 .env、
再记住这两个数是从哪来的」很容易漏。本脚本把后半段自动化：

::

    python tests/rag_production_query_calibration_spark_run.py   # ① 重跑标定（联网）
    python scripts/apply_threshold_calibration.py --yes          # ② 一键写回 + 登记溯源

② 做三件事：

1. 从标定产物**重新推导**两个阈值的**可行区间**与推荐值；
2. **现有值仍落在可行区间内就保留**（本工具是「修复」不是「改配置」——
   在健康的库上跑它，什么都不该变）；
3. 写 ``backend/.env``（只动需要动的那两行）+ 写
   ``scripts/rag_threshold_provenance.json``（启动日志与 ``/api/health`` 据此判断过期）。

安全闸
------
- **默认 dry-run**：不加 ``--yes`` 只打印计划，一个文件都不写。
- **非语义 Embedding 拒绝执行**（除非 ``--allow-offline``）：哈希占位下标出来的
  阈值毫无意义，登记成溯源只会误导后续判断。

用法::

    python scripts/apply_threshold_calibration.py             # 看计划
    python scripts/apply_threshold_calibration.py --yes       # 真写
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

# ★ 先 load_dotenv（database/main 在 import 期也会做，这里显式做一次更直观）。
from dotenv import load_dotenv  # noqa: E402

load_dotenv(BACKEND_DIR / ".env")

from services.rag_threshold_provenance import (  # noqa: E402
    PARAM_KEYS,
    corpus_fingerprint,
    default_provenance_path,
    derive_thresholds,
    within,
)

#: 退出码：与项目其它脚本一致（0 成功 / 2 用法错 / 3 安全闸拒绝 / 1 运行错）。
EXIT_OK = 0
EXIT_USAGE = 2
EXIT_SAFETY = 3
EXIT_ERROR = 1

DEFAULT_ARTIFACT = BACKEND_DIR / "scripts" / "rag_production_query_calibration_spark.json"
DEFAULT_CORPUS = BACKEND_DIR / "scripts" / "interview_knowledge.json"
DEFAULT_ENV_FILE = BACKEND_DIR / ".env"

#: 写进 ``.env`` 的小数位数（与既有写法风格一致；判定一律用全精度）。
DISPLAY_DP = 6


# ============================================================
# .env 读写（只动目标行）
# ============================================================
def _read_env_value(text: str, key: str) -> Optional[str]:
    """读 ``KEY=...`` 的**原始字符串**（没配置返回 ``None``；不匹配注释行）。"""
    match = re.search(rf"^{re.escape(key)}=(.*)$", text, re.MULTILINE)
    return match.group(1).strip() if match else None


def _set_env_value(text: str, key: str, value: str) -> Tuple[str, bool]:
    """把 ``KEY=...`` 替换成新值；没有该行就**追加**。返回 ``(新文本, 是否变化)``。"""
    pattern = re.compile(rf"^{re.escape(key)}=.*$", re.MULTILINE)
    if pattern.search(text):
        updated = pattern.sub(f"{key}={value}", text, count=1)
        return updated, updated != text
    separator = "" if text.endswith("\n") else "\n"
    return f"{text}{separator}{key}={value}\n", True


def _fmt(value: float) -> str:
    return f"{float(value):.{DISPLAY_DP}f}"


def _count_corpus_documents(path: Path) -> Optional[int]:
    """数语料里有几篇文档。

    兼容两种形态：顶层就是数组（``[{…}, …]``）或 ``{"documents": [{…}, …]}``
    （``scripts/interview_knowledge.json`` 是后者）。**数错了比数不出更糟**
    （会让人以为语料变了），所以结构不认识就返回 ``None``。
    """
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict) and isinstance(data.get("documents"), list):
        return len(data["documents"])
    return None


# ============================================================
# 计划
# ============================================================
def _plan_param(
    key: str, current_raw: Optional[str], window: Dict[str, Any]
) -> Dict[str, Any]:
    """决定某个参数「保留现有值」还是「采用推荐值」。

    规则：现有值**仍落在可行区间内**就保留（工具是修复不是改配置）；
    否则用推荐值。推荐值本身也可能缺失（区间不存在）⇒ 只能保留现有值并告警。
    """
    lo = window.get("lo_exclusive")
    hi = window.get("hi_inclusive")
    recommended = window.get("recommended")
    plan: Dict[str, Any] = {
        "key": key,
        "current_raw": current_raw,
        "window": {"lo_exclusive": lo, "hi_inclusive": hi},
        "recommended": recommended,
    }
    if current_raw is None or current_raw == "":
        if recommended is None:
            plan.update(action="keep", value=None,
                        note="未配置且无可推荐值（可行区间不存在）——需人工判断")
        else:
            plan.update(action="set", value=_fmt(recommended),
                        note="未配置 ⇒ 采用推荐值")
        return plan

    if lo is None or hi is None:
        plan.update(action="keep", value=current_raw,
                    note="可行区间不存在（标定数据不足）⇒ 保留现有值，请人工复核")
        return plan

    if within(current_raw, lo, hi):
        plan.update(action="keep", value=current_raw,
                    note="现有值仍在可行区间内 ⇒ 保留（本工具不改健康的配置）")
        return plan

    if recommended is None:
        plan.update(action="keep", value=current_raw,
                    note="现有值已越界，但无可推荐值 ⇒ 保留并请人工复核")
        return plan

    plan.update(action="set", value=_fmt(recommended),
                note="现有值已越出可行区间 ⇒ 采用推荐值")
    return plan


def run(args: argparse.Namespace) -> int:
    print("=" * 74)
    print("RAG 检索阈值 · 一键重标定（写 .env + 登记溯源）")
    print("=" * 74)

    artifact_path = Path(args.artifact)
    corpus_path = Path(args.corpus)
    env_path = Path(args.env_file)
    provenance_path = Path(args.provenance) if args.provenance else default_provenance_path()

    for path, label in ((artifact_path, "标定产物"), (env_path, "env 文件")):
        if not path.exists():
            print(f"[X] {label}不存在：{path}", file=sys.stderr)
            return EXIT_USAGE
    if not corpus_path.exists():
        print(f"[X] 语料文件不存在：{corpus_path}", file=sys.stderr)
        return EXIT_USAGE

    print(f"标定产物     : {artifact_path}")
    print(f"语料         : {corpus_path}")
    print(f"env 文件     : {env_path}")
    print(f"溯源产物     : {provenance_path}")
    print(f"模式         : {'真写' if args.yes else 'DRY-RUN（不写任何文件）'}")
    print("-" * 74)

    # ---- 0. 当前 Embedding（安全闸）----
    from services.knowledge_rag import describe_default_embedding

    info = describe_default_embedding()
    print(f"Embedding    : provider={info.provider} dim={info.dimension} "
          f"semantic={info.semantic_enabled}")
    if not info.semantic_enabled and not args.allow_offline:
        print("[X] 当前 Embedding 是**离线占位**（无语义）——用它标出来的阈值毫无意义，"
              "登记成溯源只会误导后续判断。", file=sys.stderr)
        print("    请先配好 EMBEDDING_* 并重跑标定；测试可用 --allow-offline 越过。",
              file=sys.stderr)
        return EXIT_SAFETY

    # ---- 1. 读产物并推导 ----
    calibration = json.loads(artifact_path.read_text(encoding="utf-8"))
    try:
        derived = derive_thresholds(calibration)
    except Exception as exc:  # noqa: BLE001 - 产物结构问题要给出可读信息
        print(f"[X] 无法从产物推导阈值：{type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_ERROR

    # 产物若自带当时的 Embedding 记录，就**交叉核对**（不一致直接拒绝）
    recorded_embedding = calibration.get("embedding")
    if isinstance(recorded_embedding, dict):
        rec_doc = recorded_embedding.get("document") or recorded_embedding
        rec_provider = rec_doc.get("provider") if isinstance(rec_doc, dict) else None
        rec_dim = rec_doc.get("dimension") if isinstance(rec_doc, dict) else None
        if rec_provider is not None and rec_provider != info.provider:
            print(f"[X] 产物是在 {rec_provider} 下标定的，当前是 {info.provider} —— "
                  "请先重跑标定。", file=sys.stderr)
            return EXIT_ERROR
        if rec_dim is not None and int(rec_dim) != int(info.dimension):
            print(f"[X] 产物标定维度 {rec_dim}，当前 {info.dimension} —— 请先重跑标定。",
                  file=sys.stderr)
            return EXIT_ERROR
        print("             └ 产物自带 Embedding 记录，已交叉核对通过")
    else:
        print("[!] 产物**未记录**标定当时的 Embedding ⇒ 本脚本按**当前** Embedding 登记。")
        print("    若产物其实是在别的模型下产出的，请先重跑 "
              "tests/rag_production_query_calibration_spark_run.py。")

    # ---- 2. 计划 ----
    env_text = env_path.read_text(encoding="utf-8")
    plans: List[Dict[str, Any]] = []
    for key, window in (("min_score", derived["min_score"]),
                        ("min_score_ratio", derived["min_score_ratio"])):
        plans.append(_plan_param(key, _read_env_value(env_text, _env_name(key)), window))
    top_k_raw = _read_env_value(env_text, _env_name("top_k"))

    print("\n[1] 推导结果（标定产物 → 可行区间）")
    for key in ("min_score", "min_score_ratio"):
        window = derived[key]
        lo, hi = window.get("lo_exclusive"), window.get("hi_inclusive")
        span = "-" if lo is None or hi is None else f"({lo!r}, {hi!r}]"
        rec = window.get("recommended")
        print(f"  {key:<18} 可行区间={span}  推荐={'-' if rec is None else _fmt(rec)}")
        if window.get("rule"):
            print(f"  {'':<18} 规则：{window['rule']}")
        if window.get("reason"):
            print(f"  {'':<18} 注意：{window['reason']}")

    print("\n[2] 写回计划")
    final_params: Dict[str, Any] = {}
    changed_lines: List[Tuple[str, str]] = []
    for plan in plans:
        mark = "改" if plan["action"] == "set" else "留"
        print(f"  [{mark}] {plan['key']:<18} {plan['current_raw']!r} → {plan['value']!r}"
              f"   （{plan['note']}）")
        if plan["action"] == "set":
            changed_lines.append((_env_name(plan["key"]), str(plan["value"])))
            final_params[plan["key"]] = float(plan["value"])
        elif plan["value"] is not None:
            final_params[plan["key"]] = float(plan["value"])
    print(f"  [留] {'top_k':<18} {top_k_raw!r} （top_k 不由本标定推导，保持不动）")
    if top_k_raw:
        final_params["top_k"] = int(top_k_raw)

    if not changed_lines:
        print("\n[i] 无需改动 .env（现有阈值仍全部落在可行区间内）。")
    if not args.yes:
        print("\n[i] DRY-RUN 结束：一个文件都没写。确认无误后加 --yes 真写。")
        return EXIT_OK

    # ---- 3. 写 .env ----
    for key, value in changed_lines:
        env_text, changed = _set_env_value(env_text, key, value)
        print(f"  [.env] {key}={value}  {'已更新' if changed else '无变化'}")
    env_path.write_text(env_text, encoding="utf-8")

    # ---- 4. 写溯源产物 ----
    corpus_sha = corpus_fingerprint(corpus_path)
    documents = _count_corpus_documents(corpus_path)
    chunks = (calibration.get("short_query") or {}).get("total_candidates")
    provenance = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "source": {
            "runner": calibration.get("runner"),
            "artifact": str(artifact_path.relative_to(BACKEND_DIR)).replace("\\", "/"),
            "artifact_generated_at": calibration.get("generated_at"),
            "applied_by": "scripts/apply_threshold_calibration.py",
            "embedder_verified": isinstance(recorded_embedding, dict),
        },
        "embedder": {
            "provider": info.provider,
            "dimension": info.dimension,
            "semantic_enabled": info.semantic_enabled,
        },
        "corpus": {
            # ★ 相对 ``backend/`` 的路径（换机器/换盘符都不用改产物）
            "file": str(corpus_path.relative_to(BACKEND_DIR)).replace("\\", "/"),
            "documents": documents,
            "chunks": chunks,
            "sha256": corpus_sha,
        },
        "params": final_params,
        "windows": {
            key: {
                "lo_exclusive": derived[key].get("lo_exclusive"),
                "hi_inclusive": derived[key].get("hi_inclusive"),
                "recommended": derived[key].get("recommended"),
            }
            for key in ("min_score", "min_score_ratio")
        },
    }
    provenance_path.parent.mkdir(parents=True, exist_ok=True)
    provenance_path.write_text(
        json.dumps(provenance, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"  [溯源] 已写入 {provenance_path}")
    print(f"         provider={info.provider} dim={info.dimension} "
          f"corpus.sha256={(corpus_sha or '')[:16]}… params={final_params}")

    print("\n[i] 完成。重启后端即可看到 [threshold] 核对日志；"
          "GET /api/health 的 threshold 字段也会同步。")
    return EXIT_OK


def _env_name(param: str) -> str:
    """检索参数名 → 环境变量名（``min_score`` → ``RAG_MIN_SCORE``）。"""
    return "RAG_" + param.upper()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="apply_threshold_calibration.py",
        description="从标定产物重算 RAG 阈值、写回 .env 并登记标定溯源",
    )
    parser.add_argument("--artifact", default=str(DEFAULT_ARTIFACT),
                        help=f"标定产物 JSON，默认 {DEFAULT_ARTIFACT.name}")
    parser.add_argument("--corpus", default=str(DEFAULT_CORPUS),
                        help=f"语料文件（算指纹用），默认 {DEFAULT_CORPUS.name}")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE),
                        help="要写入的 .env，默认 backend/.env")
    parser.add_argument("--provenance", default=None,
                        help="溯源产物输出路径，默认 backend/scripts/rag_threshold_provenance.json")
    parser.add_argument("--yes", action="store_true", help="真写（不加则 DRY-RUN）")
    parser.add_argument("--allow-offline", action="store_true",
                        help="允许非语义 Embedding（仅测试；生产**不要**加）")
    return parser


if __name__ == "__main__":
    sys.exit(run(build_parser().parse_args()))
