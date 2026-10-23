# -*- coding: utf-8 -*-
"""RAG 结果四态诊断 + 范围守卫 · 自检
（``services/rag_diagnostics.py`` + ``api/interview.py`` 告警接线）

要防的那件事
------------
出题链 / 追问链的降级约定是「**检索失败不算失败**」：``ok`` 不受影响，只在
``warnings`` 里记 ``knowledge_retrieval_failed``、``knowledge_chunks=0``。
这个约定本身没错，但它让三种情况在响应体里**同形**：

    「没开 RAG」/「开了但零命中」/「开了但检索故障」   ⇒  全都是 0 条

后果是**最危险的那一种**（向量库挂了、面试题一路在无知识状态下生成）
在日志里**一条记录都没有**。本套件钉死三件事：

1. 四态判定本身（含**优先级**：失败码必须先于条数判断）；
2. **API 层真的会告警**（AST 只证明「写了调用」，还要运行期证明实参透传对了）；
3. **范围守卫**：知识库当前是单租户（无归属列），一旦有人「只加了一半」
   （给模型加 ``user_id`` 但不给检索加过滤）⇒ 立刻变红。

直接运行：``python backend/tests/test_rag_diagnostics.py``
"""

import ast
import asyncio
import logging
import os
import pathlib
import sys
from typing import Any, Dict, List

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import regression_env  # noqa: E402,F401  （必须在项目模块之前：钉死离线环境）

from services import rag_diagnostics as rd  # noqa: E402

_FAILED_CODE = "knowledge_retrieval_failed"


def _check(name: str, cond: bool, detail: str = "") -> bool:
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def _section(title: str) -> None:
    print("\n" + title)


def _state(**kwargs: Any) -> str:
    return rd.classify_rag_outcome(**kwargs)["state"]


# ============================================================
# [1] 四态判定（手算样例）
# ============================================================
def check_states() -> bool:
    _section("[1] 四态判定（每一种都给出手算期望值）")
    ok = True

    # use_rag=False ⇒ 没开，不管 chunks 是多少都是 off
    ok &= _check("use_rag=False, chunks=0 ⇒ off",
                 _state(use_rag=False, knowledge_chunks=0) == rd.RAG_STATE_OFF,
                 _state(use_rag=False, knowledge_chunks=0))
    ok &= _check("use_rag=False, chunks=3 ⇒ 仍是 off（未启用就不该有知识）",
                 _state(use_rag=False, knowledge_chunks=3) == rd.RAG_STATE_OFF,
                 _state(use_rag=False, knowledge_chunks=3))

    # 开了 + 有失败码 ⇒ failed
    ok &= _check("use_rag=True, chunks=0, 带失败码 ⇒ failed",
                 _state(use_rag=True, knowledge_chunks=0, warnings=[_FAILED_CODE]) == rd.RAG_STATE_FAILED,
                 _state(use_rag=True, knowledge_chunks=0, warnings=[_FAILED_CODE]))

    # 开了 + 零命中 + 无失败码 ⇒ empty（**不是故障**）
    ok &= _check("use_rag=True, chunks=0, 无告警 ⇒ empty（正常零命中）",
                 _state(use_rag=True, knowledge_chunks=0) == rd.RAG_STATE_EMPTY,
                 _state(use_rag=True, knowledge_chunks=0))

    # 开了 + 命中 ⇒ injected
    ok &= _check("use_rag=True, chunks=3, 无告警 ⇒ injected",
                 _state(use_rag=True, knowledge_chunks=3) == rd.RAG_STATE_INJECTED,
                 _state(use_rag=True, knowledge_chunks=3))

    # 不相关的告警码**不能**被当成失败（否则任何 warning 都会触发告警）
    ok &= _check("无关键告警码 ⇒ 不影响状态（仍 injected）",
                 _state(use_rag=True, knowledge_chunks=2, warnings=["something_else"])
                 == rd.RAG_STATE_INJECTED,
                 _state(use_rag=True, knowledge_chunks=2, warnings=["something_else"]))
    ok &= _check("失败码混在其它告警里也能认出来",
                 _state(use_rag=True, knowledge_chunks=0,
                        warnings=["a", _FAILED_CODE, "b"]) == rd.RAG_STATE_FAILED)
    return ok


# ============================================================
# [2] ★ 优先级：失败码必须先于条数
# ============================================================
def check_priority() -> bool:
    _section("[2] ★ 判据优先级（写反就会把故障说成「零命中」）")
    ok = True

    # 这是本模块存在的理由：失败时 chunks 必然 0，但「chunks==0」不能推出失败。
    ok &= _check("★ chunks=0 + 失败码 ⇒ failed（不是 empty）",
                 _state(use_rag=True, knowledge_chunks=0, warnings=[_FAILED_CODE])
                 == rd.RAG_STATE_FAILED)

    # 反方向也要成立：chunks>0 但带失败码 ⇒ 仍然 failed
    # （宁可多报一次告警，也不要把「部分失败」说成「一切正常」）
    ok &= _check("★ chunks=5 + 失败码 ⇒ 仍判 failed（不因条数好看就放行）",
                 _state(use_rag=True, knowledge_chunks=5, warnings=[_FAILED_CODE])
                 == rd.RAG_STATE_FAILED,
                 _state(use_rag=True, knowledge_chunks=5, warnings=[_FAILED_CODE]))

    # 而 empty 与 injected 的分界**只**看条数
    ok &= _check("无失败码时 chunks 0→1 跨过 empty/injected 边界",
                 _state(use_rag=True, knowledge_chunks=0) == rd.RAG_STATE_EMPTY
                 and _state(use_rag=True, knowledge_chunks=1) == rd.RAG_STATE_INJECTED)
    return ok


# ============================================================
# [3] degraded / alert 语义
# ============================================================
def check_flags() -> bool:
    _section("[3] degraded / alert：只有 failed 才算降级、才告警")
    ok = True
    cases = [
        (dict(use_rag=False, knowledge_chunks=0), rd.RAG_STATE_OFF, False),
        (dict(use_rag=True, knowledge_chunks=0, warnings=[_FAILED_CODE]), rd.RAG_STATE_FAILED, True),
        (dict(use_rag=True, knowledge_chunks=0), rd.RAG_STATE_EMPTY, False),
        (dict(use_rag=True, knowledge_chunks=2), rd.RAG_STATE_INJECTED, False),
    ]
    for kwargs, expected_state, expected_flag in cases:
        diag = rd.classify_rag_outcome(**kwargs)
        ok &= _check(
            f"{expected_state}: degraded/alert == {expected_flag}",
            diag["degraded"] is expected_flag and diag["alert"] is expected_flag,
            f"{diag['degraded']} / {diag['alert']}",
        )
    # ★ 零命中**不是**降级：链路是好的，只是库里没有相关内容。
    #   把它算成降级会让告警天天响，最终没人看。
    ok &= _check("★ 零命中不是降级（empty ⇒ degraded=False）",
                 rd.classify_rag_outcome(use_rag=True, knowledge_chunks=0)["degraded"] is False)
    # failure_code 只在 failed 时有值
    ok &= _check("injected 时 failure_code 为空串",
                 rd.classify_rag_outcome(use_rag=True, knowledge_chunks=1)["failure_code"] == "")
    ok &= _check("failed 时 failure_code 回填命中码",
                 rd.classify_rag_outcome(
                     use_rag=True, knowledge_chunks=0, warnings=[_FAILED_CODE]
                 )["failure_code"] == _FAILED_CODE)
    return ok


# ============================================================
# [4] 入参严格性（不许把配置错误伪装成「一切正常」）
# ============================================================
def _raises(fn) -> bool:
    try:
        fn()
    except Exception:
        return True
    return False


def check_strict_inputs() -> bool:
    _section("[4] 入参严格性（非法入参**必须抛**，不许静默当成 off）")
    ok = True

    # use_rag 必须是真 bool：0/1 会被 isinstance(True, int) 蒙混，"false" 在 Python 里是真值
    for bad in (0, 1, "false", "true", None, [], 0.0):
        ok &= _check(
            f"use_rag={bad!r} ⇒ RagDiagnosisError",
            _raises(lambda b=bad: rd.classify_rag_outcome(use_rag=b, knowledge_chunks=0)),
        )
    # ★ "false" 是真值：若接受真值，它会被判成「开了 RAG」，正好判反
    ok &= _check("★ 'false' 不会被当成 True（显式拒绝，而不是 bool() 转换）",
                 _raises(lambda: rd.classify_rag_outcome(use_rag="false", knowledge_chunks=0)))

    # chunks 必须是非负 int，且拒 bool
    for bad in (True, False, -1, 1.5, "3", None):
        ok &= _check(
            f"knowledge_chunks={bad!r} ⇒ RagDiagnosisError",
            _raises(lambda b=bad: rd.classify_rag_outcome(use_rag=True, knowledge_chunks=b)),
        )
    # ★ bool 是 int 子类：不显式拒就会把 True 当成「命中 1 条」
    ok &= _check("★ True 不会被当成 chunks=1（bool 是 int 子类，项目老坑）",
                 _raises(lambda: rd.classify_rag_outcome(use_rag=True, knowledge_chunks=True)))
    return ok


# ============================================================
# [5] 键集与状态闭集
# ============================================================
def check_contract() -> bool:
    _section("[5] 键集恒定 / 状态闭集")
    ok = True
    diag = rd.classify_rag_outcome(use_rag=True, knowledge_chunks=2)
    ok &= _check("键集合与顺序恒为 DIAGNOSIS_FIELDS",
                 tuple(diag) == rd.DIAGNOSIS_FIELDS, str(tuple(diag)))
    ok &= _check("共 7 键", len(rd.DIAGNOSIS_FIELDS) == 7, str(len(rd.DIAGNOSIS_FIELDS)))
    ok &= _check("四态闭集恰好 4 个",
                 tuple(rd.RAG_STATES) == ("off", "failed", "empty", "injected"),
                 str(rd.RAG_STATES))
    ok &= _check("四种状态都能被产生出来（没有死状态）",
                 {rd.classify_rag_outcome(**kw)["state"] for kw in (
                     dict(use_rag=False, knowledge_chunks=0),
                     dict(use_rag=True, knowledge_chunks=0, warnings=[_FAILED_CODE]),
                     dict(use_rag=True, knowledge_chunks=0),
                     dict(use_rag=True, knowledge_chunks=1),
                 )} == set(rd.RAG_STATES))
    ok &= _check("每个状态都有非空 reason",
                 all(rd.classify_rag_outcome(**kw)["reason"] for kw in (
                     dict(use_rag=False, knowledge_chunks=0),
                     dict(use_rag=True, knowledge_chunks=0, warnings=[_FAILED_CODE]),
                     dict(use_rag=True, knowledge_chunks=0),
                     dict(use_rag=True, knowledge_chunks=1),
                 )))
    return ok


# ============================================================
# [6] 纯函数 + 描述文案确定性
# ============================================================
def check_pure_and_describe() -> bool:
    _section("[6] 纯函数 / 描述文案（同一输入同一输出）")
    ok = True
    warnings = ["x", _FAILED_CODE]
    before = list(warnings)
    diag = rd.classify_rag_outcome(use_rag=True, knowledge_chunks=0, warnings=warnings)
    ok &= _check("不改入参 warnings", warnings == before, str(warnings))
    ok &= _check("两次调用结果逐键相同",
                 rd.classify_rag_outcome(use_rag=True, knowledge_chunks=0, warnings=warnings) == diag)
    line = rd.describe_rag_outcome(diag)
    ok &= _check("描述含 state / degraded / chunks / code",
                 all(token in line for token in ("rag_state=failed", "degraded=True",
                                                "chunks=0", f"code={_FAILED_CODE}")), line)
    ok &= _check("同一 diagnosis 两次描述逐字节相同",
                 rd.describe_rag_outcome(diag) == line)
    ok &= _check("无失败码时 code 渲染成 -",
                 rd.describe_rag_outcome(
                     rd.classify_rag_outcome(use_rag=True, knowledge_chunks=1)
                 ).endswith("code=-"))
    return ok


# ============================================================
# [7] 零项目依赖守卫（AST）
# ============================================================
def check_no_project_imports() -> bool:
    _section("[7] 守卫：本模块**不 import 任何项目模块**（否则会打破消费者闭集）")
    ok = True
    source = (_BACKEND / "services" / "rag_diagnostics.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    top_imports: List[str] = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            top_imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            # 相对导入（level>0）也算项目内依赖
            if node.level:
                top_imports.append("." * node.level + (node.module or ""))
            else:
                top_imports.append(node.module or "")

    ok &= _check("顶层 import 只有 __future__ / typing",
                 sorted(top_imports) == ["__future__", "typing"], str(top_imports))

    # 函数体内的延迟导入也不许出现项目模块（保持可单测、零副作用）
    inner: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for sub in ast.walk(node):
                if isinstance(sub, ast.Import):
                    inner.extend(alias.name for alias in sub.names)
                elif isinstance(sub, ast.ImportFrom):
                    inner.append(("." * sub.level) + (sub.module or ""))
    ok &= _check("函数体内也没有任何 import（纯函数模块）", inner == [], str(inner))
    return ok


# ============================================================
# [8] ★ 运行期接线取证：API 层真的会告警
# ============================================================
class _Capture(logging.Handler):
    """把 ``api.interview`` 的日志记录全收下来（不改格式、不改传播）。"""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: List[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _capture_into(logger: logging.Logger):
    handler = _Capture()
    old_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)   # 否则 isEnabledFor(DEBUG) 为 False，正常路径不落日志
    return handler, old_level


def check_api_alerts() -> bool:
    _section("[8] ★ 运行期取证：_observe_rag 在失败时真的打 WARNING")
    ok = True
    from api import interview as api_interview

    logger = logging.getLogger("api.interview")
    handler, old_level = _capture_into(logger)
    try:
        # ---- 检索失败：必须 WARNING ----
        api_interview._observe_rag(
            "生成下一题", 42,
            {"knowledge_chunks": 0, "warnings": [_FAILED_CODE]},
            True,
        )
        ok &= _check("★ failed ⇒ 打出 1 条 WARNING",
                     len(handler.records) == 1 and handler.records[0].levelno == logging.WARNING,
                     str([(r.levelname, r.getMessage()) for r in handler.records]))
        record = handler.records[0] if handler.records else None
        ok &= _check("  └ 结构化字段 rag_state=failed",
                     record is not None and getattr(record, "rag_state", "") == "failed",
                     str(getattr(record, "rag_state", "")))
        ok &= _check("  └ rag_degraded=True / rag_chunks=0 / 失败码回填",
                     record is not None
                     and getattr(record, "rag_degraded", None) is True
                     and getattr(record, "rag_chunks", None) == 0
                     and getattr(record, "rag_failure_code", "") == _FAILED_CODE)
        ok &= _check("  └ 文案含 session_id（可定位到具体会话）",
                     record is not None and "session_id=42" in record.getMessage(),
                     record.getMessage() if record else "")

        # ---- 正常命中：只 DEBUG（不污染生产日志）----
        handler.records.clear()
        api_interview._observe_rag("生成下一题", 7, {"knowledge_chunks": 3, "warnings": []}, True)
        ok &= _check("injected ⇒ 只 DEBUG（不是 WARNING）",
                     len(handler.records) == 1 and handler.records[0].levelno == logging.DEBUG,
                     str([(r.levelname, r.getMessage()) for r in handler.records]))

        # ---- 零命中：INFO（值得知道，但不是故障）----
        handler.records.clear()
        api_interview._observe_rag("生成追问", 7, {"knowledge_chunks": 0, "warnings": []}, True)
        ok &= _check("empty ⇒ INFO",
                     len(handler.records) == 1 and handler.records[0].levelno == logging.INFO,
                     str([(r.levelname, r.getMessage()) for r in handler.records]))

        # ---- 未启用：DEBUG ----
        handler.records.clear()
        api_interview._observe_rag("生成追问", 7, {"knowledge_chunks": 0, "warnings": []}, False)
        ok &= _check("off ⇒ DEBUG",
                     len(handler.records) == 1 and handler.records[0].levelno == logging.DEBUG,
                     str([(r.levelname, r.getMessage()) for r in handler.records]))

        # ---- 观测失败绝不影响主流程 ----
        handler.records.clear()
        raised = False
        try:
            api_interview._observe_rag("生成下一题", 7, {"knowledge_chunks": "坏值"}, True)
        except Exception:
            raised = True
        ok &= _check("★ 观测本身失败也不抛异常（可观测性不得反过来打挂面试）", raised is False)
        ok &= _check("  └ 但会留一条 ERROR 日志（我们自己的入参约定不一致）",
                     len(handler.records) == 1 and handler.records[0].levelno == logging.ERROR,
                     str([r.levelname for r in handler.records]))
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)
    return ok


# ============================================================
# [9] ★ 端点级取证：两个 RAG 入口都接了线
# ============================================================
def check_endpoints_wired() -> bool:
    _section("[9] ★ 端点级取证：/next-question 与 /follow-up 都调用了 _observe_rag")
    ok = True
    path = _BACKEND / "api" / "interview.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))

    def _calls_observe(func_name: str) -> bool:
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == func_name:
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Call):
                        target = sub.func
                        name = getattr(target, "id", None) or getattr(target, "attr", None)
                        if name == "_observe_rag":
                            return True
        return False

    ok &= _check("next_question 调用了 _observe_rag", _calls_observe("next_question"))
    ok &= _check("follow_up 调用了 _observe_rag", _calls_observe("follow_up"))
    # 只接线但把 use_rag 写死 = 假接线 ⇒ 断言第 4 个实参是 payload.use_rag
    for func_name in ("next_question", "follow_up"):
        found = False
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == func_name:
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Call) and getattr(sub.func, "id", None) == "_observe_rag":
                        if len(sub.args) >= 4:
                            arg = sub.args[3]
                            found = (isinstance(arg, ast.Attribute) and arg.attr == "use_rag"
                                     and isinstance(arg.value, ast.Name) and arg.value.id == "payload")
        ok &= _check(f"  └ {func_name} 透传的是 payload.use_rag（不是写死的常量）", found)

    # ★ 第二段取证：AST 只能证明「写了调用」，还得在**运行期**证明
    #   「端点真的把请求里的 use_rag 交给了观测函数、且响应体一字未改」。
    #   只做 AST 会在「调用写了但实参取错变量」时静默通过（项目踩过的坑）。
    ok &= _run_endpoint_probe()
    return ok


def _run_endpoint_probe() -> bool:
    """真调端点（service 打桩），断言观测函数收到的 use_rag 与响应体。"""
    from api import interview as api_interview
    from schemas.interview import FollowUpRequest, NextQuestionRequest

    async def _probe() -> List[bool]:
        results: List[bool] = []
        seen: Dict[str, Any] = {}

        async def fake_next(db, user_id, session_id, *, use_rag, retriever_kwargs=None):
            seen["next_use_rag"] = use_rag
            return {"ok": True, "knowledge_chunks": 0,
                    "warnings": [_FAILED_CODE], "errors": [], "error": None}

        async def fake_follow(db, user_id, session_id, payload):
            seen["follow_use_rag"] = payload.use_rag
            return {"ok": True, "knowledge_chunks": 0,
                    "warnings": [_FAILED_CODE], "errors": [], "error": None}

        logger = logging.getLogger("api.interview")
        handler, old_level = _capture_into(logger)
        original_next = api_interview.interview_service.generate_next_question
        original_follow = api_interview.interview_service.generate_followup
        try:
            api_interview.interview_service.generate_next_question = fake_next
            api_interview.interview_service.generate_followup = fake_follow

            # ---- /next-question（use_rag 缺省 = True）----
            handler.records.clear()
            resp = await api_interview.next_question(
                NextQuestionRequest(), session_id=11,
                current_user={"user_id": 1}, db=None,
            )
            results.append(_check(
                "★ 运行期：端点把 use_rag=True 交给了 service",
                seen.get("next_use_rag") is True, str(seen.get("next_use_rag"))))
            results.append(_check(
                "★ 运行期：端点真的打出了 failed 告警",
                any(getattr(r, "rag_state", "") == "failed" for r in handler.records),
                str([getattr(r, "rag_state", "?") for r in handler.records])))
            results.append(_check(
                "★ 响应体**未**被观测逻辑改动（仍 0 条知识）",
                resp.knowledge_chunks == 0 and resp.ok is True,
                f"{resp.knowledge_chunks} / {resp.ok}"))

            # ---- /next-question（显式 use_rag=False）----
            seen.clear()
            handler.records.clear()
            await api_interview.next_question(
                NextQuestionRequest(use_rag=False), session_id=11,
                current_user={"user_id": 1}, db=None,
            )
            results.append(_check(
                "★ 运行期：显式 use_rag=False 也如实透传",
                seen.get("next_use_rag") is False, str(seen.get("next_use_rag"))))
            results.append(_check(
                "  └ 关闭时状态判成 off（不是 failed）",
                any(getattr(r, "rag_state", "") == "off" for r in handler.records),
                str([getattr(r, "rag_state", "?") for r in handler.records])))

            # ---- /follow-up ----
            seen.clear()
            handler.records.clear()
            await api_interview.follow_up(
                FollowUpRequest(answer_text="我用 Redis 做过分布式锁"),
                session_id=11, current_user={"user_id": 1}, db=None,
            )
            results.append(_check(
                "★ 运行期：追问端点也观测（use_rag=True）",
                seen.get("follow_use_rag") is True
                and any(getattr(r, "rag_state", "") == "failed" for r in handler.records),
                f"{seen.get('follow_use_rag')} / "
                f"{[getattr(r, 'rag_state', '?') for r in handler.records]}"))
        finally:
            api_interview.interview_service.generate_next_question = original_next
            api_interview.interview_service.generate_followup = original_follow
            logger.removeHandler(handler)
            logger.setLevel(old_level)
        return results

    return all(asyncio.run(_probe()))


# ============================================================
# [10] 守卫自检：这套判定必须「能变差」
# ============================================================
def check_self_guard() -> bool:
    _section("[10] 守卫自检：判定必须能区分，否则等于没做")
    ok = True
    # 若实现恒返回 injected，下列断言必须失败（用「四态互不相等」把它钉住）
    states = [
        rd.classify_rag_outcome(use_rag=False, knowledge_chunks=0)["state"],
        rd.classify_rag_outcome(use_rag=True, knowledge_chunks=0, warnings=[_FAILED_CODE])["state"],
        rd.classify_rag_outcome(use_rag=True, knowledge_chunks=0)["state"],
        rd.classify_rag_outcome(use_rag=True, knowledge_chunks=1)["state"],
    ]
    ok &= _check("★ 四种输入给出四个**互不相同**的状态", len(set(states)) == 4, str(states))
    # 只有 failed 会告警：若把 empty 也算告警，这里会失败
    alerts = [
        rd.classify_rag_outcome(use_rag=True, knowledge_chunks=0)["alert"],
        rd.classify_rag_outcome(use_rag=True, knowledge_chunks=0, warnings=[_FAILED_CODE])["alert"],
    ]
    ok &= _check("★ empty 不告警、failed 告警（两者必须不同）", alerts == [False, True], str(alerts))
    return ok


# ============================================================
# [11] ★ 范围守卫（单租户 tripwire）
# ============================================================
#: 「归属」类列名。知识库一旦引入其中任何一个，检索就必须带范围过滤。
_OWNERSHIP_COLUMNS = ("user_id", "owner_id", "tenant_id", "org_id", "workspace_id")

#: 检索器构造时**可用于收窄范围**的关键字（`VectorKnowledgeRetriever` 支持）。
_SCOPE_KWARGS = ("document_id", "category")


def check_scope_tripwire() -> bool:
    """★ 半成品多租户 tripwire。

    现状（**已核实**）：``knowledge_document`` / ``knowledge_chunk`` **没有任何归属列**，
    知识库是**全局共享**的（写操作仅 admin），因此今天**不存在**跨用户串读——
    但也**不存在任何强制**。真正的风险是「只加了一半」：

    - 给模型加了 ``user_id``，但检索仍按全库召回 ⇒ 每个用户都能检索到别人的知识，
      而且**静默**（召回结果看起来完全正常）。

    本守卫不阻止引入多租户，只阻止**半引入**：一旦归属列出现，就必须同时
    让检索链路带范围参数（``retriever_kwargs`` 里的 ``document_id`` / ``category``，
    或新增等价的 scope 机制），否则这里立刻变红。
    """
    _section("[11] ★ 范围守卫：知识库单租户现状 vs 「只加了归属列」的半成品")
    ok = True

    import models  # noqa: PLC0415 - 只在需要时导入，避免影响其它小节

    def _columns(model_name: str) -> List[str]:
        model = getattr(models, model_name)
        return [column.name for column in model.__table__.columns]

    doc_columns = _columns("KnowledgeDocument")
    chunk_columns = _columns("KnowledgeChunk")
    owned = sorted(
        {name for name in doc_columns + chunk_columns if name in _OWNERSHIP_COLUMNS}
    )

    # 检索链路是否带范围：看 API 层给 service 的 retriever_kwargs 里有没有 scope 键
    api_tree = ast.parse((_BACKEND / "api" / "interview.py").read_text(encoding="utf-8"))
    scope_keys: List[str] = []
    for node in ast.walk(api_tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                # 形态①：`retriever_kwargs = {"document_id": ...}`
                if isinstance(target, ast.Name) and target.id == "retriever_kwargs":
                    if isinstance(node.value, ast.Dict):
                        scope_keys.extend(
                            key.value for key in node.value.keys
                            if isinstance(key, ast.Constant) and isinstance(key.value, str)
                        )
                # 形态②：`retriever_kwargs["document_id"] = ...`（本项目实际写法）
                if isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name) \
                        and target.value.id == "retriever_kwargs":
                    key = target.slice
                    if isinstance(key, ast.Constant) and isinstance(key.value, str):
                        scope_keys.append(key.value)
    api_scoped = any(key in _SCOPE_KWARGS for key in scope_keys)

    if owned:
        ok &= _check(
            f"★ 模型已引入归属列 {owned} ⇒ 检索链路必须同时带范围过滤",
            api_scoped,
            f"retriever_kwargs 里的键 = {scope_keys}；"
            f"需要 {_SCOPE_KWARGS} 之一，或补一套等价的 scope 机制"
            "（否则用户之间会静默串读）",
        )
    else:
        ok &= _check(
            "知识库仍是单租户（无归属列）⇒ 检索不需要范围参数（现状自洽）",
            True,
        )
        ok &= _check(
            "  └ 且 API 层确实没有伪装成「已隔离」（retriever_kwargs 不含 scope 键）",
            not api_scoped,
            f"{scope_keys}",
        )

    # 无论哪种形态，这两条必须成立：
    ok &= _check(
        "写操作仍然只有 admin 能触发（单租户成立的前提）",
        _admin_only_writes(),
        "api/knowledge.py 的写路由必须全部挂 get_current_admin",
    )
    ok &= _check(
        "★ 检索器本身支持范围收窄（引入多租户时不必改冻结文件）",
        _retriever_accepts_scope(),
        "VectorKnowledgeRetriever 必须接受 document_id / category",
    )
    return ok


def _admin_only_writes() -> bool:
    """``api/knowledge.py`` 的每个**写**路由都必须挂 ``get_current_admin``。"""
    tree = ast.parse((_BACKEND / "api" / "knowledge.py").read_text(encoding="utf-8"))
    write_methods = {"post", "put", "patch", "delete"}
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue
            func = decorator.func
            if not (isinstance(func, ast.Attribute) and func.attr in write_methods):
                continue
            guards = {
                getattr(sub, "id", None) or getattr(sub, "attr", None)
                for sub in ast.walk(node)
            }
            # ★ 必须同时看 Name 与 Attribute：`from deps import get_current_admin` 之后
            #   它是 ast.Name，只认 ast.Attribute 会漏判成「没挂鉴权」（假红）。
            if "get_current_admin" not in guards:
                return False
    return True


def _retriever_accepts_scope() -> bool:
    """``VectorKnowledgeRetriever.__init__`` 必须接受 ``document_id`` / ``category``。"""
    import inspect  # noqa: PLC0415

    from services.vector_knowledge_retriever import VectorKnowledgeRetriever  # noqa: PLC0415

    params = inspect.signature(VectorKnowledgeRetriever.__init__).parameters
    return all(name in params for name in _SCOPE_KWARGS)


# ============================================================
# 主流程
# ============================================================
def main() -> int:
    print("=" * 70)
    print("RAG 结果四态诊断 · 自检")
    print("=" * 70)
    ok = True
    ok &= check_states()
    ok &= check_priority()
    ok &= check_flags()
    ok &= check_strict_inputs()
    ok &= check_contract()
    ok &= check_pure_and_describe()
    ok &= check_no_project_imports()
    ok &= check_api_alerts()
    ok &= check_endpoints_wired()
    ok &= check_self_guard()
    ok &= check_scope_tripwire()
    print("\n" + "=" * 70)
    print("结果: 全部通过" if ok else "结果: 存在失败项")
    print("=" * 70)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
