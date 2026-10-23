# -*- coding: utf-8 -*-
"""Interview Orchestrator 自检（**状态机 + 编排流程 + 复用取证**）

无需 pytest，直接运行：
    python backend/tests/test_interview_orchestrator.py

覆盖
----
[1]  契约：5 状态 / 6 事件 / 转移表形状 / ``__all__`` 覆盖 / 评分直通是同一对象
[2]  纯函数：``transition`` 全部合法边 + **转移表之外的全部组合都报错**（不静默）
[3]  进程内状态表：set / get / apply_event / reset / clear / snapshot + 无记录报错
[4]  端到端：第一题生成 → 数字人播报 → 回答提交 → 下一题流程 → 全部答完 → 结束
[5]  复用取证：spy 证明「复用既有入口」而非「重造」
     （``start_session`` / ``submit_answer`` / ``end_session`` /
      ``generate_next_question`` / ``score_answer`` 的调用次数）
[6]  按需出题：``ok=True`` → ``avatar_speaking``；``ok=False`` → 状态**不变**
[7]  健壮性：服务层拒绝时状态**回滚**；无记录**自愈**；``finish`` 幂等；
     顺序不对时**状态机先报错**（不打到服务层）
[8]  边界守卫（AST）：依赖单向（不 import 受保护模块 / 不被 core·service·api 反向依赖）

数据库使用 SQLite 内存库（``StaticPool``），不触碰本机 MySQL。
"""

import asyncio
import ast
import contextlib
import json
import os
import pathlib
import sys

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))
sys.path.insert(0, str(_BACKEND / "tests"))

import regression_env  # noqa: E402,F401  （钉住 AVATAR_* 等部署旋钮 → 数字人走 Mock）

from fastapi import HTTPException  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401
import services.interview_core as interview_core  # noqa: E402
import services.interview_service as interview_service  # noqa: E402
from database import Base  # noqa: E402
from models import INTERVIEW_MODE_AVATAR, INTERVIEW_MODE_TEXT, Job, User  # noqa: E402
from schemas.interview import AnswerSubmitRequest, SessionCreateRequest  # noqa: E402
from services import interview_orchestrator as orch  # noqa: E402

ANSWER_TEXT = "我负责过订单中台重构，用 Redis 做缓存，QPS 从 800 提升到 5000。"

#: 一份**合法**的候选问题 JSON（与 ``test_agent_retriever`` 同款，Agent 直接解析通过）。
QUESTION_JSON = json.dumps(
    {
        "question": "请介绍一下你在项目中是如何使用 Redis 做缓存的？",
        "question_type": "technical",
        "topic": "Redis",
        "difficulty": "mid",
        "expected_points": ["缓存穿透", "过期策略"],
        "reason": "考察缓存实践经验",
    },
    ensure_ascii=False,
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


def _raises(fn, exc_type):
    try:
        fn()
    except exc_type as exc:
        return True, exc
    except Exception as exc:  # noqa: BLE001
        return False, f"抛了非预期异常 {type(exc).__name__}: {exc}"
    return False, "未抛异常"


async def _araises(coro, exc_type):
    try:
        await coro
    except exc_type as exc:
        return True, exc
    except Exception as exc:  # noqa: BLE001
        return False, f"抛了非预期异常 {type(exc).__name__}: {exc}"
    return False, "未抛异常"


def _top_level_imports(source: str):
    """只取**模块顶层** import（函数体内的延迟导入不算）。

    与项目既有约定一致：``from services import a, b`` 同时产出**裸包名**
    ``services`` 与**点号全名** ``services.a`` / ``services.b``
    （守卫匹配模块名必须用点号全名，裸名会恒真失效）。
    """
    names = set()
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _all_imports(source: str):
    """**全量** import（含函数体内延迟导入）——判「从不 import X」必须走 AST。

    子串匹配会被 docstring 里提到的模块名误伤（本项目已复发多次的坑）。
    """
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


# ============================================================
# 测试替身
# ============================================================
class MockSpark:
    """按顺序吐出预设回复的假 Spark；超出预设次数即报错。"""

    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls: list = []

    async def chat_async(self, message: str) -> str:
        self.calls.append(message)
        if not self.replies:
            raise AssertionError("MockSpark 被调用次数超出预期")
        return self.replies.pop(0)


class JunkSpark:
    """永远返回非 JSON——用来稳定复现「出题失败（ok=False）」。"""

    def __init__(self):
        self.calls: list = []

    async def chat_async(self, message: str) -> str:
        self.calls.append(message)
        return "这不是 JSON，也没有任何结构化内容。"


class _AsyncSpy:
    """记录调用次数的异步包装（原样透传返回值）。"""

    def __init__(self, name: str, fn, counter: dict):
        self._name = name
        self._fn = fn
        self._counter = counter

    async def __call__(self, *args, **kwargs):
        self._counter[self._name] = self._counter.get(self._name, 0) + 1
        return await self._fn(*args, **kwargs)


@contextlib.contextmanager
def _spy_reuse():
    """在 ``interview_service`` / ``interview_core`` 上装调用计数器。

    这是「复用取证」的关键：编排器调用的是**模块属性**
    （``interview_service.start_session`` …），因此替换模块属性即可统计，
    无需改生产代码。计数 > 0 即证明编排器**确实走了既有入口**。
    """
    counter: dict = {}
    svc_names = (
        "create_session",
        "start_session",
        "submit_answer",
        "end_session",
        "generate_next_question",
    )
    saved_svc = {n: getattr(interview_service, n) for n in svc_names}
    saved_score = interview_core.score_answer

    def _score(question, answer_text):
        counter["score_answer"] = counter.get("score_answer", 0) + 1
        return saved_score(question, answer_text)

    try:
        for name, fn in saved_svc.items():
            setattr(interview_service, name, _AsyncSpy(name, fn, counter))
        interview_core.score_answer = _score
        yield counter
    finally:
        for name, fn in saved_svc.items():
            setattr(interview_service, name, fn)
        interview_core.score_answer = saved_score


# ============================================================
# 数据库
# ============================================================
def _build_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed(db: AsyncSession):
    user = User(username="orch_user", email="orch@example.com", password_hash="x", role="user")
    job = Job(
        job_name="后端开发工程师", salary="20-35K", edu_require="本科", major_require="不限",
        skills="Python,MySQL,Redis", duty="负责后端服务的设计与开发", city="深圳", industry="互联网",
    )
    db.add_all([user, job])
    await db.commit()
    await db.refresh(user)
    await db.refresh(job)
    return user.id, job.id


async def _make_session(db: AsyncSession, user_id: int, job_id: int, *, mode, total=2) -> int:
    result = await orch.create_session(
        db, user_id,
        SessionCreateRequest(job_id=job_id, mode=mode, total_questions=total),
    )
    return result["session"]["id"]


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 68)
    print("Interview Orchestrator 自检（状态机 + 编排 + 复用取证）")
    print("=" * 68)

    # ------------------------------------------------------------
    # [1] 契约
    # ------------------------------------------------------------
    print("\n[1] 契约：状态 / 事件 / 转移表 / 评分直通")
    _check(
        "5 个状态，顺序 = 生命周期顺序",
        orch.STATES == (
            orch.STATE_WAITING_QUESTION, orch.STATE_AVATAR_SPEAKING,
            orch.STATE_WAITING_ANSWER, orch.STATE_PROCESSING_ANSWER,
            orch.STATE_FINISHED,
        ),
        str(orch.STATES),
    )
    _check("  └ 状态字面量符合需求原文",
           (orch.STATE_WAITING_QUESTION, orch.STATE_AVATAR_SPEAKING,
            orch.STATE_WAITING_ANSWER, orch.STATE_PROCESSING_ANSWER,
            orch.STATE_FINISHED) == ("waiting_question", "avatar_speaking",
                                     "waiting_answer", "processing_answer", "finished"))
    _check("初始态 = waiting_question（新会话尚未拿题）",
           orch.INITIAL_STATE == orch.STATE_WAITING_QUESTION)
    _check("终态集合 = {finished}", orch.TERMINAL_STATES == (orch.STATE_FINISHED,))
    _check("6 个事件",
           orch.EVENTS == ("question_ready", "speech_done", "answer_submitted",
                           "scored", "all_answered", "finish"),
           str(orch.EVENTS))
    _check("转移表恰好 10 条（5 条生命周期边 + 5 个 finish 逃生舱）",
           len(orch.TRANSITIONS) == 10, str(len(orch.TRANSITIONS)))
    _check("  └ 5 条生命周期边逐条正确",
           all(orch.TRANSITIONS[k] == v for k, v in {
               (orch.STATE_WAITING_QUESTION, orch.EVENT_QUESTION_READY): orch.STATE_AVATAR_SPEAKING,
               (orch.STATE_AVATAR_SPEAKING, orch.EVENT_SPEECH_DONE): orch.STATE_WAITING_ANSWER,
               (orch.STATE_WAITING_ANSWER, orch.EVENT_ANSWER_SUBMITTED): orch.STATE_PROCESSING_ANSWER,
               (orch.STATE_PROCESSING_ANSWER, orch.EVENT_SCORED): orch.STATE_AVATAR_SPEAKING,
               (orch.STATE_PROCESSING_ANSWER, orch.EVENT_ALL_ANSWERED): orch.STATE_FINISHED,
           }.items()))
    _check("  └ 5 个 finish 逃生舱（任意状态 → finished，终态幂等）",
           all(orch.TRANSITIONS[(s, orch.EVENT_FINISH)] == orch.STATE_FINISHED
               for s in orch.STATES))
    _check("转移表的目标态全部是合法状态",
           all(v in orch.STATES for v in orch.TRANSITIONS.values()))
    _check("★ 评分直通：orchestrator.score_answer **就是** interview_core.score_answer（未重造）",
           orch.score_answer is interview_core.score_answer)
    missing = [n for n in orch.__all__ if not hasattr(orch, n)]
    _check("__all__ 里每个名字都真实存在", missing == [], str(missing))
    expected_public = {
        "STATE_WAITING_QUESTION", "STATE_AVATAR_SPEAKING", "STATE_WAITING_ANSWER",
        "STATE_PROCESSING_ANSWER", "STATE_FINISHED", "STATES", "INITIAL_STATE",
        "TERMINAL_STATES", "EVENT_QUESTION_READY", "EVENT_SPEECH_DONE",
        "EVENT_ANSWER_SUBMITTED", "EVENT_SCORED", "EVENT_ALL_ANSWERED",
        "EVENT_FINISH", "EVENTS", "TRANSITIONS", "transition", "can_transition",
        "is_valid_state", "is_terminal", "get_state", "set_state", "apply_event",
        "reset_state", "clear_states", "states_snapshot", "create_session", "start",
        "speech_done", "generate_question", "submit_answer", "finish", "status",
        "score_answer", "preview_score", "OrchestratorError", "OrchestratorStateError",
    }
    _check("__all__ 覆盖全部对外契约名", expected_public <= set(orch.__all__),
           str(sorted(expected_public - set(orch.__all__))))
    _check("OrchestratorStateError 是 OrchestratorError 的子类（可分级捕获）",
           issubclass(orch.OrchestratorStateError, orch.OrchestratorError)
           and issubclass(orch.OrchestratorError, RuntimeError))

    # ------------------------------------------------------------
    # [2] 纯函数：transition
    # ------------------------------------------------------------
    print("\n[2] 纯函数 transition（脱离 DB）")
    for (state, event), expected in sorted(orch.TRANSITIONS.items()):
        got = orch.transition(state, event)
        _check(f"{state} --{event}--> {expected}", got == expected, f"得到 {got}")
    _check("can_transition 对合法边为 True",
           orch.can_transition(orch.STATE_WAITING_ANSWER, orch.EVENT_ANSWER_SUBMITTED) is True)
    _check("can_transition 对非法组合为 False（**不抛错**）",
           orch.can_transition(orch.STATE_WAITING_ANSWER, orch.EVENT_SCORED) is False
           and orch.can_transition("bogus", orch.EVENT_FINISH) is False
           and orch.can_transition(orch.STATE_FINISHED, "bogus") is False)
    _check("is_valid_state / is_terminal",
           orch.is_valid_state(orch.STATE_FINISHED) is True
           and orch.is_valid_state("bogus") is False
           and orch.is_terminal(orch.STATE_FINISHED) is True
           and orch.is_terminal(orch.STATE_WAITING_ANSWER) is False)

    # ★ 穷举：转移表之外的**每一组**组合都必须报错（不静默）
    silent = []
    for state in orch.STATES:
        for event in orch.EVENTS:
            if (state, event) in orch.TRANSITIONS:
                continue
            ok, _ = _raises(lambda s=state, e=event: orch.transition(s, e),
                            orch.OrchestratorStateError)
            if not ok:
                silent.append((state, event))
    total = len(orch.STATES) * len(orch.EVENTS)
    _check(f"★ 转移表之外的全部组合（{total} - 10 = {total - 10} 组）都报错，无静默通过",
           silent == [], str(silent))
    ok, exc = _raises(lambda: orch.transition("bogus", orch.EVENT_FINISH),
                      orch.OrchestratorStateError)
    _check("未知状态 → OrchestratorStateError", ok, str(exc))
    ok, exc = _raises(lambda: orch.transition(orch.STATE_WAITING_QUESTION, "bogus"),
                      orch.OrchestratorStateError)
    _check("未知事件 → OrchestratorStateError", ok, str(exc))
    ok, exc = _raises(lambda: orch.transition(orch.STATE_WAITING_ANSWER, orch.EVENT_SCORED),
                      orch.OrchestratorStateError)
    _check("非法转移的报错信息含「该状态只接受」提示（可诊断）",
           ok and "该状态只接受" in str(exc), str(exc))

    # ------------------------------------------------------------
    # [3] 进程内状态表
    # ------------------------------------------------------------
    print("\n[3] 进程内状态表（不落库）")
    orch.clear_states()
    _check("clear 后无记录：get_state 返回 None", orch.get_state(999) is None)
    _check("  └ 可指定 default", orch.get_state(999, orch.INITIAL_STATE) == orch.INITIAL_STATE)
    _check("set_state 置位并回读", orch.set_state(999, orch.STATE_WAITING_ANSWER)
           == orch.STATE_WAITING_ANSWER and orch.get_state(999) == orch.STATE_WAITING_ANSWER)
    ok, _ = _raises(lambda: orch.set_state(999, "bogus"), orch.OrchestratorStateError)
    _check("set_state 非法状态 → 报错", ok)
    _check("apply_event 在已记录状态上推进",
           orch.apply_event(999, orch.EVENT_ANSWER_SUBMITTED) == orch.STATE_PROCESSING_ANSWER
           and orch.get_state(999) == orch.STATE_PROCESSING_ANSWER)
    ok, exc = _raises(lambda: orch.apply_event(999, orch.EVENT_SCORED + "_bogus"),
                      orch.OrchestratorStateError)
    _check("apply_event 未知事件 → 报错", ok, str(exc))
    orch.reset_state(999)
    _check("reset_state 删除记录", orch.get_state(999) is None)
    ok, exc = _raises(lambda: orch.apply_event(999, orch.EVENT_SPEECH_DONE),
                      orch.OrchestratorStateError)
    _check("★ 无记录时 apply_event **直接报错**（不猜初始态）", ok, str(exc))
    orch.set_state(1001, orch.STATE_AVATAR_SPEAKING)
    snap = orch.states_snapshot()
    snap[1001] = "被篡改"
    _check("states_snapshot 是**副本**（改它不影响内部状态）",
           orch.get_state(1001) == orch.STATE_AVATAR_SPEAKING)
    orch.clear_states()
    _check("clear_states 清空全部", orch.states_snapshot() == {})

    # ------------------------------------------------------------
    # [4] 端到端：第一题生成 → 播报 → 回答提交 → 下一题 → 结束
    # ------------------------------------------------------------
    print("\n[4] 端到端编排（数字人模式，SQLite 内存库）")
    engine, session_factory = _build_session_factory()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as db:
        user_id, job_id = await _seed(db)
        orch.clear_states()

        # ---- 创建 Session ----
        sid = await _make_session(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR)
        _check("① 创建 Session → 状态 waiting_question",
               orch.get_state(sid) == orch.STATE_WAITING_QUESTION)
        _check("  └ 会话状态仍是 created（尚未开始）",
               (await interview_service.get_session_detail(db, user_id, sid))["session"]["status"]
               == "created")

        # ---- 开始：初始化 Avatar + 生成问题 + 播报第一题 ----
        started = await orch.start(db, user_id, sid)
        _check("② 开始面试 → 状态 avatar_speaking（数字人正在播报）",
               started["state"] == orch.STATE_AVATAR_SPEAKING
               and orch.get_state(sid) == orch.STATE_AVATAR_SPEAKING)
        _check("  └ ③ 第一题已生成（question_no=1 且题目非空）",
               started["question"] is not None
               and started["question"]["question_no"] == 1
               and bool(started["question"]["question"]))
        _check("  └ 会话状态 ongoing（整场题目已一次性落库）",
               started["session"]["status"] == "ongoing"
               and started["session"]["total_questions"] == 2)
        _check("  └ 播报态允许的事件 = speech_done / finish",
               orch.status(sid)["allowed_events"] == [orch.EVENT_SPEECH_DONE, orch.EVENT_FINISH],
               str(orch.status(sid)["allowed_events"]))

        # ---- 播报结束 → 等待回答 ----
        _check("④ 播报结束 → waiting_answer",
               orch.speech_done(sid) == orch.STATE_WAITING_ANSWER
               and orch.get_state(sid) == orch.STATE_WAITING_ANSWER)

        # ---- 提交答案 → 评分 → 下一题 ----
        first = await orch.submit_answer(
            db, user_id, sid, AnswerSubmitRequest(answer_text=ANSWER_TEXT))
        _check("⑤ 回答提交 → 评分落库（answer 含分数与反馈）",
               first["answer"]["score"] is not None
               and bool(first["answer"]["feedback"])
               and first["answer"]["answer_text"] == ANSWER_TEXT)
        _check("  └ ⑦ 下一题流程：next_question 非空、题号推进到 2",
               first["next_question"] is not None
               and first["next_question"]["question_no"] == 2)
        _check("  └ 状态 → avatar_speaking（下一题已交付播报）",
               first["state"] == orch.STATE_AVATAR_SPEAKING
               and first["all_answered"] is False)

        # ---- 第二题：播报 → 提交 → 全部答完 ----
        orch.speech_done(sid)
        second = await orch.submit_answer(
            db, user_id, sid, AnswerSubmitRequest(answer_text=ANSWER_TEXT))
        _check("  └ 全部答完 → all_answered=True 且状态 finished",
               second["all_answered"] is True
               and second["next_question"] is None
               and second["state"] == orch.STATE_FINISHED)
        _check("  └ 两次作答都真实落库",
               (await interview_service.get_session_detail(db, user_id, sid))["answered_count"] == 2)

        # ---- 结束：出报告 ----
        ended = await orch.finish(db, user_id, sid)
        _check("⑧ 结束面试 → 报告生成、状态 finished",
               ended["report"]["total_score"] is not None
               and ended["state"] == orch.STATE_FINISHED)
        ended2 = await orch.finish(db, user_id, sid)
        _check("  └ finish 幂等（重复调用返回既有报告，状态仍 finished）",
               ended2["state"] == orch.STATE_FINISHED
               and ended2["report"]["id"] == ended["report"]["id"])
        _check("  └ 终态下 allowed_events 只剩 finish",
               orch.status(sid)["allowed_events"] == [orch.EVENT_FINISH],
               str(orch.status(sid)["allowed_events"]))
        _check("status() 标记 tracked / terminal",
               orch.status(sid)["tracked"] is True and orch.status(sid)["terminal"] is True)
        _check("未登记的会话：status() 返回 state=None / tracked=False / 无合法事件",
               orch.status(424242) == {
                   "session_id": 424242, "state": None, "tracked": False,
                   "terminal": False, "allowed_events": [],
               }, str(orch.status(424242)))

        # ---- text 模式同样可用（编排层不按模式分支） ----
        text_sid = await _make_session(db, user_id, job_id, mode=INTERVIEW_MODE_TEXT, total=1)
        t_started = await orch.start(db, user_id, text_sid)
        orch.speech_done(text_sid)
        t_submit = await orch.submit_answer(
            db, user_id, text_sid, AnswerSubmitRequest(answer_text=ANSWER_TEXT))
        _check("text 模式走同一套编排（无分支）：start → avatar_speaking → 答完 finished",
               t_started["state"] == orch.STATE_AVATAR_SPEAKING
               and t_submit["all_answered"] is True
               and t_submit["state"] == orch.STATE_FINISHED)

        # ------------------------------------------------------------
        # [5] 复用取证
        # ------------------------------------------------------------
        print("\n[5] 复用取证（spy：编排器确实走了既有入口）")
        orch.clear_states()
        with _spy_reuse() as counter:
            sid5 = await _make_session(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR)
            await orch.start(db, user_id, sid5)
            orch.speech_done(sid5)
            await orch.submit_answer(db, user_id, sid5, AnswerSubmitRequest(answer_text=ANSWER_TEXT))
            orch.speech_done(sid5)
            await orch.submit_answer(db, user_id, sid5, AnswerSubmitRequest(answer_text=ANSWER_TEXT))
            await orch.finish(db, user_id, sid5)
        _check("★ 复用 interview_service.start_session（1 次）",
               counter.get("start_session") == 1, str(counter))
        _check("★ 复用 interview_service.submit_answer（2 次，每题一次）",
               counter.get("submit_answer") == 2, str(counter))
        _check("★ 复用 interview_service.end_session（1 次）",
               counter.get("end_session") == 1, str(counter))
        _check("★ 复用 interview_core.score_answer（2 次，与提交次数一致）",
               counter.get("score_answer") == 2, str(counter))
        _check("  └ 规则出题流程**不**触碰 generate_next_question（按需通道默认不启用）",
               counter.get("generate_next_question") is None, str(counter))

        with _spy_reuse() as counter2:
            sid6 = await _make_session(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR)
            await orch.generate_question(db, user_id, sid6, spark=MockSpark(QUESTION_JSON))
        _check("★ 复用 interview_service.generate_next_question（1 次）",
               counter2.get("generate_next_question") == 1, str(counter2))

        # preview_score 直通：与 core 同输入同输出
        probe_q = {"question": "请介绍 Redis 缓存", "question_type": "technical",
                   "topic": "Redis", "difficulty": "mid", "expected_points": ["缓存穿透"]}
        _check("preview_score 与 interview_core.score_answer 同输入同输出（直通、不落库）",
               orch.preview_score(probe_q, ANSWER_TEXT)
               == interview_core.score_answer(probe_q, ANSWER_TEXT))

        # ------------------------------------------------------------
        # [6] 按需出题（大模型通道）
        # ------------------------------------------------------------
        print("\n[6] generate_question（按需出题）")
        orch.clear_states()
        sid7 = await _make_session(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR)
        spark = MockSpark(QUESTION_JSON)
        gen = await orch.generate_question(db, user_id, sid7, spark=spark)
        _check("ok=True → 题目已就绪，状态推进到 avatar_speaking",
               gen["ok"] is True and gen["state"] == orch.STATE_AVATAR_SPEAKING
               and bool(gen["question"]), str(gen.get("error")))
        _check("  └ 确实调用了注入的 Spark", len(spark.calls) == 1, str(len(spark.calls)))
        _check("  └ 出参含恒定字段集 + state（在 Core 契约上**追加** state）",
               set(gen) == set(interview_core.QUESTION_RESULT_FIELDS) | {"state"},
               str(sorted(set(gen) ^ (set(interview_core.QUESTION_RESULT_FIELDS) | {"state"}))))

        orch.clear_states()
        sid8 = await _make_session(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR)
        bad = await orch.generate_question(db, user_id, sid8, spark=JunkSpark())
        _check("ok=False → 出题失败，状态**不变**（仍 waiting_question）",
               bad["ok"] is False and bad["state"] == orch.STATE_WAITING_QUESTION
               and bad["question"] == "", str(bad.get("error")))
        _check("  └ 失败以 ok=False 返回，**不抛异常**（便于前端分支）", True)

        # 终态下出题失败**不得**把 finished 降级
        orch.clear_states()
        sid9 = await _make_session(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=1)
        await orch.start(db, user_id, sid9)
        orch.speech_done(sid9)
        await orch.submit_answer(db, user_id, sid9, AnswerSubmitRequest(answer_text=ANSWER_TEXT))
        _check("前置：该会话已 finished", orch.get_state(sid9) == orch.STATE_FINISHED)
        late = await orch.generate_question(db, user_id, sid9, spark=MockSpark(QUESTION_JSON))
        _check("★ 终态下出题失败**不会**把 finished 降级回 waiting_question",
               late["ok"] is False and orch.get_state(sid9) == orch.STATE_FINISHED,
               f"ok={late['ok']} state={orch.get_state(sid9)} error={late.get('error')}")

        # ------------------------------------------------------------
        # [7] 健壮性
        # ------------------------------------------------------------
        print("\n[7] 健壮性：回滚 / 自愈 / 顺序守卫")
        orch.clear_states()
        sid10 = await _make_session(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR)
        await orch.start(db, user_id, sid10)
        orch.speech_done(sid10)
        before_state = orch.get_state(sid10)
        ok, exc = await _araises(
            orch.submit_answer(db, user_id, sid10, AnswerSubmitRequest(answer_text="   ")),
            HTTPException,
        )
        _check("★ 服务层拒绝（空答案 400）→ 异常向上抛，状态**回滚**到 waiting_answer",
               ok and getattr(exc, "status_code", None) == 400
               and orch.get_state(sid10) == before_state == orch.STATE_WAITING_ANSWER,
               f"{exc} state={orch.get_state(sid10)}")

        # 顺序守卫：还在播报就提交 → 状态机先报错（不打到服务层）
        orch.clear_states()
        sid11 = await _make_session(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR)
        await orch.start(db, user_id, sid11)
        ok, exc = await _araises(
            orch.submit_answer(db, user_id, sid11, AnswerSubmitRequest(answer_text=ANSWER_TEXT)),
            orch.OrchestratorStateError,
        )
        _check("★ 顺序不对（avatar_speaking 就提交）→ 状态机先报错，不打到服务层",
               ok, str(exc))
        _check("  └ 报错后状态仍是 avatar_speaking（未被污染）",
               orch.get_state(sid11) == orch.STATE_AVATAR_SPEAKING)

        # 自愈：清空进程内状态后，只要事件在初始态上合法就能重新编排
        # （模拟「进程重启」：库里有会话、内存里没有编排状态）
        sid12 = await _make_session(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR)
        orch.clear_states()
        _check("前置：内存无记录，但库里有会话",
               orch.get_state(sid12) is None and orch.status(sid12)["tracked"] is False)
        healed = await orch.start(db, user_id, sid12)
        _check("★ 无记录时 start 自愈：状态从初始态起算并推进到 avatar_speaking",
               healed["state"] == orch.STATE_AVATAR_SPEAKING
               and orch.get_state(sid12) == orch.STATE_AVATAR_SPEAKING,
               str(healed.get("state")))

        # 自愈不放松合法性：无记录 + scored 仍应报错
        orch.clear_states()
        ok, exc = _raises(lambda: orch._advance(12345, orch.EVENT_SCORED),
                          orch.OrchestratorStateError)
        _check("★ 自愈**不放松合法性**：无记录时应用 scored 仍报错", ok, str(exc))

    await engine.dispose()

    # ------------------------------------------------------------
    # [8] 边界守卫（AST）
    # ------------------------------------------------------------
    print("\n[8] 边界守卫（AST：依赖单向）")
    orch_path = _BACKEND / "services" / "interview_orchestrator.py"
    orch_src = orch_path.read_text(encoding="utf-8")
    top = _top_level_imports(orch_src)
    roots = {m.split(".")[0] for m in top}
    _check("编排层顶层只依赖标准库 + services 包（无第三方 / 无 models / 无 database）",
           roots <= {"__future__", "contextlib", "threading", "typing", "services"},
           str(sorted(roots)))
    _check("  └ 顶层 import 的 services.* 恰好是 interview_core / interview_service",
           {m for m in top if m.startswith("services.")}
           == {"services.interview_core", "services.interview_service"},
           str(sorted(m for m in top if m.startswith("services."))))
    _check("  └ 确实 import 了 interview_service（复用而非重造）",
           "services.interview_service" in top)
    _check("  └ 确实 import 了 interview_core（评分直通）",
           "services.interview_core" in top)
    _check("  └ 不 import 任何向量 / Embedding / 知识库模块",
           not any(tok in m.lower() for m in _all_imports(orch_src)
                   for tok in ("embedding", "vector", "chroma", "knowledge")),
           str(sorted(_all_imports(orch_src))))

    # ★ 反向依赖：下层**不得** import 编排层（否则「可选编排」变成「必经路径」）
    for rel in ("services/interview_core.py", "services/interview_service.py",
                "services/avatar_interview.py", "services/avatar_provider.py",
                "services/avatar_provider_iflytek.py", "modules/interview/api.py", "main.py"):
        mods = _all_imports((_BACKEND / rel).read_text(encoding="utf-8"))
        _check(f"{rel} 不反向 import 编排层",
               "services.interview_orchestrator" not in mods, str(sorted(mods)))

    # ★ 编排层不重造评分：源码里没有第二个 score_answer 定义
    score_defs = [
        n.name for n in ast.walk(ast.parse(orch_src))
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == "score_answer"
    ]
    _check("★ 编排层没有自己定义 score_answer（只做名字绑定）", score_defs == [], str(score_defs))
    bound = [
        t.id for n in ast.walk(ast.parse(orch_src))
        if isinstance(n, ast.Assign) and isinstance(n.value, ast.Attribute)
        for t in n.targets if isinstance(t, ast.Name) and t.id == "score_answer"
    ]
    _check("  └ score_answer 是 `interview_core.score_answer` 的名字绑定",
           bound == ["score_answer"], str(bound))

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
