# -*- coding: utf-8 -*-
"""面试并发正确性 · 自检（逻辑收敛层）

无需 pytest，直接运行：
    python backend/tests/test_interview_concurrency.py

★ 本套件的能力边界（必须如实理解，别当成"验证了 MySQL 并发"）
------------------------------------------------------------------
本套件跑在 **SQLite 内存库**上（项目硬约束：SQLite 只允许出现在
``backend/tests/*.py``）。SQLite 上 ``SELECT ... FOR UPDATE`` 是**空操作**，
也没有真正的多语句并发 ⇒ 本套件验证的是：

1. **状态机与守卫口径**（哪些转移合法、哪些必须 400）；
2. **收敛逻辑**：当"竞争失败者"看到已被别人写好的数据时，是否**正确地返回既有行**
   而不是报错 / 插出重复行（即 ``IntegrityError`` → 重读 → ``existing`` 这条路）；
3. **幂等性**：同一操作重复执行不产生第二份副作用；
4. **事务边界**（AST 取证）：大模型调用之前事务确实被关掉；
5. **缺行自愈**：失败窗口留下的"会话推进了但题目缺失"能被自动恢复。

**真实 MySQL 的行锁语义 / 真并行客户端**由配套运行器验证：
``backend/tests/interview_mysql_concurrency_run.py``（**不进回归循环**，
因为它需要一个可连的真实 MySQL）。两者的分工与"为什么不能只靠 SQLite 推断"
写在同一文件的分节标题里。
"""

import ast
import asyncio
import contextlib
import inspect
import json
import os
import pathlib
import sys
import types

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

import regression_env  # noqa: E402,F401

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

from sqlalchemy import inspect as sa_inspect  # noqa: E402
from sqlalchemy import select, text  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool  # noqa: E402

import models  # noqa: E402,F401
from database import Base  # noqa: E402
from models import (  # noqa: E402
    INTERVIEW_MODE_AVATAR,
    SESSION_STATUS_CREATED,
    SESSION_STATUS_FINISHED,
    SESSION_STATUS_ONGOING,
    InterviewAnswer,
    InterviewContext,
    InterviewQuestion,
    InterviewReport,
    InterviewSession,
    Job,
    User,
)
from schemas.interview import AnswerSubmitRequest, SessionCreateRequest  # noqa: E402
from services import (  # noqa: E402
    interview_context,
    interview_core,
    interview_next_agent as na,
    interview_next_flow,
    interview_service,
)
from services.resume_scoring import extract_skills  # noqa: E402

_PASSED = 0
_FAILED = 0

ANSWER = (
    "我在上一家公司负责订单中台重构，把单体拆成 8 个微服务，"
    "用 Python + MySQL + Redis + Kafka，QPS 从 800 提到 5000。"
)

SERVICE_SRC = (_BACKEND / "services" / "interview_service.py").read_text(encoding="utf-8")
FLOW_SRC = (_BACKEND / "services" / "interview_next_flow.py").read_text(encoding="utf-8")
CONTEXT_SRC = (_BACKEND / "services" / "interview_context.py").read_text(encoding="utf-8")


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name
          + (f"  -> {detail}" if detail and not cond else ""))
    return cond


# ============================================================
# 辅助
# ============================================================
def _imported_modules(source: str) -> set:
    """用 AST 取真正被 import 的模块名（**不要用子串匹配**：docstring 会误伤）。"""
    names: set = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _func_node(source: str, name: str):
    """按名字取**顶层**函数 AST 节点（取不到返回 None）。"""
    for node in ast.parse(source).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None


def _call_lines(node) -> list:
    """返回 ``[(lineno, 被调名), ...]``，被调名取 ``f`` 或 ``a.b`` 的末段。"""
    out = []
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call):
            fn = sub.func
            if isinstance(fn, ast.Name):
                out.append((sub.lineno, fn.id))
            elif isinstance(fn, ast.Attribute):
                out.append((sub.lineno, fn.attr))
    return sorted(out)


class _FakeDb:
    """只为 ``_idempotent_insert_stmt`` 提供 ``get_bind().dialect.name``。

    ★ 为什么需要它：那个函数按**方言**选分支，而本套件跑在 SQLite 上。
    有了这个假对象，就能在本套件里把 **MySQL 分支**也编译出来看一眼
    （否则生产方言的 SQL 形态永远没人验证）。
    """

    def __init__(self, dialect_name: str):
        self._bind = types.SimpleNamespace(
            dialect=types.SimpleNamespace(name=dialect_name)
        )

    def get_bind(self):
        return self._bind


class MockSpark:
    """按顺序吐回复的假 Spark；超出预设次数即报错。"""

    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls: list = []

    async def chat_async(self, message: str) -> str:
        self.calls.append(message)
        if not self.replies:
            raise AssertionError("MockSpark 被调用次数超出预期")
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


def _spark_ok(question: str) -> MockSpark:
    return MockSpark(json.dumps({
        "question": question,
        "question_type": "technical",
        "topic": "Redis",
        "difficulty": "mid",
        "expected_points": ["缓存穿透"],
        "reason": "stub",
    }, ensure_ascii=False))


@contextlib.contextmanager
def _installed_spark(client):
    original = na._default_spark
    na._default_spark = lambda: client
    try:
        yield client
    finally:
        na._default_spark = original


@contextlib.contextmanager
def _flag(value):
    key = interview_service.AVATAR_LLM_QUESTIONS_ENV
    old = os.environ.get(key)
    if value is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = value
    try:
        yield
    finally:
        if old is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = old


@contextlib.contextmanager
def _patched(obj, name, value):
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


async def _rejected(coro, status: int):
    """返回 ``(是否按期望状态码拒绝, 响应 detail)``。"""
    from fastapi import HTTPException

    try:
        await coro
        return False, ""
    except HTTPException as exc:
        return exc.status_code == status, str(exc.detail)


async def _stored(db: AsyncSession, session_id: int):
    rows = (await db.execute(
        select(InterviewQuestion)
        .where(InterviewQuestion.session_id == session_id)
        .order_by(InterviewQuestion.question_no)
    )).scalars().all()
    return list(rows)


async def _make(db: AsyncSession, user_id: int, job_id: int, *, mode: str = "text",
                total: int = 3) -> int:
    payload = SessionCreateRequest(job_id=job_id, total_questions=total, mode=mode)
    return (await interview_service.create_session(db, user_id, payload))["session"]["id"]


async def _rule_plan(db: AsyncSession, session_id: int):
    """独立复算规则出题结果（不经任何策略 / 模型）。"""
    session = await interview_core.load_session_row(db, session_id)
    job = await interview_core.load_job_row(db, session.job_id)
    resume = await interview_core.load_resume_row(db, session.resume_id)
    skills = extract_skills(resume.content) if resume is not None and resume.content else []
    return [dict(x) for x in interview_core.build_question_plan(session, job, skills)]


async def _submit(db: AsyncSession, user_id: int, sid: int):
    return await interview_service.submit_answer(
        db, user_id, sid, AnswerSubmitRequest(answer_text=ANSWER)
    )


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 70)
    print("面试并发正确性 · 自检（逻辑收敛层 / SQLite）")
    print("=" * 70)

    # ========================================================
    # [1] 持久化状态机基线：只有 3 个状态（不新增状态）
    # ========================================================
    print("\n[1] 持久化状态机：真实只有 3 个状态，本次不新增")

    _check("[1.1] 三个状态常量取值未变",
           (SESSION_STATUS_CREATED, SESSION_STATUS_ONGOING, SESSION_STATUS_FINISHED)
           == ("created", "ongoing", "finished"),
           f"{SESSION_STATUS_CREATED}/{SESSION_STATUS_ONGOING}/{SESSION_STATUS_FINISHED}")
    _check("[1.2] 模型里没有 in_progress / completed / cancelled 这类别名",
           not any(x in SERVICE_SRC for x in ("in_progress", "completed", "cancelled")))
    _check("[1.3] status 列仍是 String(20) 且带索引",
           "index=True" in str(InterviewSession.__table__.c.status.__dict__)
           or InterviewSession.__table__.c.status.index is True)

    # 合法 / 非法转移的**守卫口径**（读码穷举后的可执行断言）
    print("  · 守卫口径（400 而不是静默收敛）")
    guard_node = _func_node(SERVICE_SRC, "_ensure_startable")
    _check("[1.4] _ensure_startable 存在且对 finished / ongoing 都抛 400",
           guard_node is not None
           and "HTTPException" in ast.dump(guard_node)
           and sum(1 for _ in ast.walk(guard_node) if isinstance(_, ast.Raise)) == 2,
           ast.dump(guard_node)[:120] if guard_node else "未找到")

    # ========================================================
    # [2] 唯一约束必须在**数据库里**存在（不能只查模型声明）
    # ========================================================
    print("\n[2] 唯一约束：查**建出来的库**，不是查模型声明")

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    session_factory = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    def _uq(sync_conn):
        # ★ SQLite 把 `Column(unique=True)` 建成**唯一索引**，`get_unique_constraints`
        #   不会报它；MySQL 则两者都报。所以两个入口都要收，否则本断言会在
        #   SQLite 上"因方言差异"假失败（与迁移脚本同一处理）。
        insp = sa_inspect(sync_conn)
        out = {}
        for t in ("interview_question", "interview_answer",
                  "interview_report", "interview_context"):
            names = {u["name"] for u in insp.get_unique_constraints(t)}
            names |= {
                i["name"] for i in insp.get_indexes(t)
                if i.get("unique") and i.get("name")
            }
            out[t] = names
        return out

    async with engine.connect() as conn:
        uqs = await conn.run_sync(_uq)

    _check("[2.1] interview_question 在库里真有 (session_id, question_no) 唯一约束",
           "uq_interview_question_session_no" in uqs["interview_question"],
           str(uqs["interview_question"]))
    _check("[2.2] interview_answer 的 question_id 唯一（一题一答）",
           any("question_id" in n for n in uqs["interview_answer"]),
           str(uqs["interview_answer"]))
    _check("[2.3] interview_report 的 session_id 唯一（一报告一会话）",
           any("session_id" in n for n in uqs["interview_report"]),
           str(uqs["interview_report"]))
    # ★ 这条断言的意义：模型声明 ≠ 数据库事实。真实 MySQL 上本条曾长期为假
    #   （约束从未被 create_all 创建），修复方式是迁移脚本
    #   scripts/migrate_interview_question_unique.py，由
    #   tests/interview_mysql_concurrency_run.py 在真实库上复核。
    _check("[2.4] 约束名与迁移脚本一致（防止改名后真实库对不上）",
           "uq_interview_question_session_no"
           in (_BACKEND / "scripts" / "migrate_interview_question_unique.py").read_text(
               encoding="utf-8"))

    async with session_factory() as db:
        user = User(username="conc", email="conc@example.com",
                    password_hash="x", role="user")
        job = Job(job_name="并发测试岗", city="北京", skills="Python,Redis",
                  salary="10-20K")
        db.add_all([user, job])
        await db.commit()
        await db.refresh(user)
        await db.refresh(job)
        user_id, job_id = user.id, job.id

        # ====================================================
        # [3] 事务边界：大模型调用之前必须关掉事务（AST 取证）
        # ====================================================
        print("\n[3] 事务边界：模型调用期间不得有事务开着（AST 取证）")

        node = _func_node(SERVICE_SRC, "generate_and_persist_next")
        _check("[3.1] 能取到 generate_and_persist_next 的 AST 节点", node is not None)
        calls = _call_lines(node) if node else []
        end_lines = [ln for ln, name in calls if name == "_end_read_transaction"]
        llm_lines = [ln for ln, name in calls if name == "generate_from_prepared"]
        _check("[3.2] 模型调用恰好 1 次", len(llm_lines) == 1, str(llm_lines))
        _check("[3.3] 模型调用之前至少结束过 1 次只读事务",
               bool(end_lines) and bool(llm_lines) and min(end_lines) < min(llm_lines),
               f"end={end_lines} llm={llm_lines}")
        _check("[3.4] 取材料阶段（prepare_candidate）也在模型调用之前",
               bool([ln for ln, n in calls if n == "prepare_candidate"])
               and min([ln for ln, n in calls if n == "prepare_candidate"]) < min(llm_lines))
        _check("[3.5] 旧的一次性入口 generate_candidate 已不再被 Service 调用",
               "generate_candidate" not in [n for _ln, n in calls])

        prep = _func_node(FLOW_SRC, "generate_from_prepared")
        _check("[3.6] generate_from_prepared 的形参里没有 db（结构上开不出事务）",
               prep is not None
               and not any(a.arg in ("db", "session") for a in prep.args.args),
               str([a.arg for a in prep.args.args]) if prep else "未找到")

        # ====================================================
        # [4] _end_read_transaction 的安全闸
        # ====================================================
        print("\n[4] _end_read_transaction：只允许在只读阶段调用")

        _check("[4.1] 有未提交的新增对象时显式报错（不悄悄提交）",
               await _raises_runtime_error(db, user))

        # ====================================================
        # [5] 重复作答 ⇒ 400 + 稳定文案，且不产生第二行
        # ====================================================
        print("\n[5] 重复作答：400 + 稳定文案，不产生重复行")

        sid = await _make(db, user_id, job_id, total=3)
        await interview_service.start_session(db, user_id, sid)
        first = (await _stored(db, sid))[0]
        await _submit(db, user_id, sid)

        n_ans = (await db.execute(
            select(InterviewAnswer).where(InterviewAnswer.question_id == first.id)
        )).scalars().all()
        _check("[5.1] 首次作答落库 1 行", len(n_ans) == 1, str(len(n_ans)))

        # 题号已推进 ⇒ 第二次提交答的是第 2 题，不算重复
        out2 = await _submit(db, user_id, sid)
        _check("[5.2] 题号已推进 ⇒ 第二次提交答的是第 2 题（不是重复）",
               out2["answer"]["score"] is not None
               and out2["session"]["current_question_no"] == 3,
               str(out2["session"]["current_question_no"]))

        # 直接绕过题号守卫：手动把题号改回 1，模拟"两个请求都看到第 1 题"
        row = await db.get(InterviewSession, sid)
        row.current_question_no = 1
        await db.commit()
        ok2, detail2 = await _rejected(_submit(db, user_id, sid), 400)
        _check("[5.3] 同一题再次作答 ⇒ 400 且文案是既有契约",
               ok2 and "已作答" in detail2, detail2)
        n_ans2 = (await db.execute(
            select(InterviewAnswer).where(InterviewAnswer.question_id == first.id)
        )).scalars().all()
        _check("[5.4] 仍然只有 1 行作答（没有重复评分/重复落库）", len(n_ans2) == 1)

        # ====================================================
        # [6] 重复补题 ⇒ source=existing，不新增行
        # ====================================================
        print("\n[6] 重复补题：幂等返回既有行")

        with _flag("1"):
            sid_q = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=3)
            await interview_service.start_session(db, user_id, sid_q)
            before = len(await _stored(db, sid_q))
            _check("[6.1] 数字人 + 开关打开 ⇒ 开场只落第 1 题", before == 1, str(before))

            # 答完第 1 题时 submit_answer 内部会补出第 2 题
            with _installed_spark(_spark_ok("Q2：Redis 持久化")):
                out_q = await _submit(db, user_id, sid_q)
            _check("[6.2] 补题走 LLM（交付出去的下一题就是模型给的）",
                   out_q["next_question"] is not None
                   and out_q["next_question"]["question"] == "Q2：Redis 持久化",
                   str(out_q.get("next_question"))[:100])
            after1 = len(await _stored(db, sid_q))
            _check("[6.3] 恰好新增 1 行", after1 == before + 1, f"{before}->{after1}")

            gen2 = await interview_service.generate_and_persist_next(db, user_id, sid_q)
            after2 = len(await _stored(db, sid_q))
            _check("[6.4] 再次调用 ⇒ source=existing 且行数不变（幂等）",
                   gen2["source"] == interview_service.SOURCE_EXISTING
                   and after2 == after1, f"{gen2['source']} / {after1}->{after2}")
            _check("[6.5] 返回的是**同一行**（id 相同）",
                   out_q["next_question"]["id"] == gen2["question"]["id"])

        # ====================================================
        # [7] 重复 end ⇒ 幂等返回既有报告，报告表只有 1 行
        # ====================================================
        print("\n[7] 重复结束：幂等返回既有报告")

        sid_e = await _make(db, user_id, job_id, total=2)
        await interview_service.start_session(db, user_id, sid_e)
        await _submit(db, user_id, sid_e)
        end1 = await interview_service.end_session(db, user_id, sid_e)
        end2 = await interview_service.end_session(db, user_id, sid_e)
        _check("[7.1] 第一次 end 生成报告", end1["report"] is not None)
        _check("[7.2] 第二次 end 返回**同一份**报告（幂等）",
               end1["report"]["id"] == end2["report"]["id"])
        reports = (await db.execute(
            select(InterviewReport).where(InterviewReport.session_id == sid_e)
        )).scalars().all()
        _check("[7.3] 报告表只有 1 行（没有重复出报告）", len(reports) == 1,
               str(len(reports)))
        _check("[7.4] 会话状态停在 finished（未被回滚成 ongoing）",
               end2["session"]["status"] == SESSION_STATUS_FINISHED)

        # ====================================================
        # [8] 重复 start ⇒ 400，题数不翻倍
        # ====================================================
        print("\n[8] 重复开始：400 + 题数不翻倍")

        sid_s = await _make(db, user_id, job_id, total=3)
        await interview_service.start_session(db, user_id, sid_s)
        n_before = len(await _stored(db, sid_s))
        ok3, detail3 = await _rejected(
            interview_service.start_session(db, user_id, sid_s), 400
        )
        n_after = len(await _stored(db, sid_s))
        _check("[8.1] 重复 start 被 400 拦下", ok3, detail3)
        _check("[8.2] 题数没有翻倍（仍是首次那批）", n_before == n_after == 3,
               f"{n_before}->{n_after}")

        ok4, detail4 = await _rejected(
            interview_service.end_session(db, user_id, sid_s), 400
        )
        _check("[8.3] 未作答直接 end 按既有口径 400（不是 500）",
               ok4 and "回答" in detail4, detail4)

        # ====================================================
        # [9] AI 失败 / 超时 ⇒ 规则兜底，不抛异常、不留半截数据
        # ====================================================
        print("\n[9] 模型失败 / 超时：规则兜底，不留半截数据")

        with _flag("1"):
            sid_f = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=3)
            await interview_service.start_session(db, user_id, sid_f)
            with _installed_spark(MockSpark(RuntimeError("模拟超时"))):
                out_f = await _submit(db, user_id, sid_f)
            _check("[9.1] 超时不抛异常、答题照常完成",
                   out_f["answer"]["score"] is not None, str(out_f)[:120])
            _check("[9.2] 下一题自动降级为规则兜底（不是 None）",
                   out_f["next_question"] is not None
                   and out_f["next_question"]["question_no"] == 2,
                   str(out_f.get("next_question"))[:100])
            _check("[9.3] 兜底题已落库（不会让题号悬空）",
                   len(await _stored(db, sid_f)) == 2)

            gen_f = await interview_service.generate_and_persist_next(db, user_id, sid_f)
            _check("[9.4] 再补同一题 ⇒ existing（兜底题也是幂等的）",
                   gen_f["source"] == interview_service.SOURCE_EXISTING,
                   str(gen_f["source"]))

            # 两条路都失败 ⇒ ok=False 且**不落库**
            sid_f2 = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=3)
            await interview_service.start_session(db, user_id, sid_f2)

            async def _no_item(db_, session_, question_no_):
                return None

            with _installed_spark(MockSpark(RuntimeError("模拟超时"))), _patched(
                interview_service, "_rule_plan_item", _no_item
            ):
                out_f2 = await _submit(db, user_id, sid_f2)
                _check("[9.5] 两条路都失败 ⇒ 答题仍成功，next_question=None",
                       out_f2["answer"]["score"] is not None
                       and out_f2["next_question"] is None,
                       str(out_f2.get("next_question")))
                gen_f2 = await interview_service.generate_and_persist_next(
                    db, user_id, sid_f2
                )
            _check("[9.6] 直接补题 ⇒ ok=False 且稳定错误码在最前",
                   gen_f2["ok"] is False
                   and gen_f2["errors"][0] == interview_service.ERROR_QUESTION_GENERATE_FAILED,
                   str(gen_f2["errors"]))
            _check("[9.7] 失败不留脏数据（库里仍只有第 1 题）",
                   len(await _stored(db, sid_f2)) == 1)

        # ====================================================
        # [10] 提交失败（模拟竞争失败者）⇒ IntegrityError 收敛为 existing
        # ====================================================
        print("\n[10] 提交冲突：IntegrityError 收敛为 existing，不插重复行")

        with _flag("1"):
            sid_c = await _make(db, user_id, job_id, mode=INTERVIEW_MODE_AVATAR, total=3)
            await interview_service.start_session(db, user_id, sid_c)

            # 先让第 2 题正常落库（模拟"另一个请求已经写好了"）
            with _installed_spark(_spark_ok("Q2：并发写入的那道")):
                await _submit(db, user_id, sid_c)
            rows_now = await _stored(db, sid_c)
            _check("[10.1] 第 2 题已在库里", len(rows_now) == 2, str(len(rows_now)))

            # 再模拟"竞争失败者"：它的幂等预检查看不到那一行（TOCTOU），
            # 于是走到 INSERT，被唯一约束挡下 ⇒ 必须收敛成 existing。
            real_load = interview_service._load_questions
            calls = {"n": 0}

            async def _blind_first(db_, session_id_):
                calls["n"] += 1
                if calls["n"] == 1:
                    return []
                return await real_load(db_, session_id_)

            with _installed_spark(_spark_ok("Q2：另一路也想写")):
                with _patched(interview_service, "_load_questions", _blind_first):
                    gen_c = await interview_service.generate_and_persist_next(
                        db, user_id, sid_c
                    )
            rows_after = await _stored(db, sid_c)
            _check("[10.2] 冲突被收敛成 existing（不抛 409、不插重复行）",
                   gen_c["source"] == interview_service.SOURCE_EXISTING,
                   str(gen_c["source"]))
            _check("[10.3] 行数不变（唯一约束真的挡住了）",
                   len(rows_after) == 2, str(len(rows_after)))
            _check("[10.4] 返回的是已存在的那一行",
                   gen_c["question"]["id"] == rows_now[1].id)

        # ====================================================
        # [11] 缺行自愈：失败窗口留下的"题号已推进、题目缺失"能自动恢复
        # ====================================================
        print("\n[11] 缺行自愈：会话推进了但题目缺失 ⇒ 自动补回")

        sid_h = await _make(db, user_id, job_id, total=3)
        await interview_service.start_session(db, user_id, sid_h)
        # 手工删掉第 1 题，模拟"模型成功、提交失败"留下的空洞
        # （text 模式开场整场落库 3 道，删掉第 1 道后剩 2 道）
        victim = (await _stored(db, sid_h))[0]
        await db.delete(victim)
        await db.commit()
        remaining = await _stored(db, sid_h)
        _check("[11.1] 空洞已制造（第 1 题缺失、其余仍在）",
               len(remaining) == 2 and all(r.question_no != 1 for r in remaining),
               str([r.question_no for r in remaining]))

        healed = await interview_service.get_current_question(db, user_id, sid_h)
        _check("[11.2] get_current_question 不再 500，而是自愈",
               healed["question"] is not None and healed["question_no"] == 1,
               str(healed.get("question"))[:100])
        rows_h = await _stored(db, sid_h)
        _check("[11.3] 自愈结果已落库（重新凑齐 3 道、题号连续）",
               [r.question_no for r in rows_h] == [1, 2, 3],
               str([r.question_no for r in rows_h]))
        _check("[11.4] 自愈出来的是**规则题**（与丢失前逐字段相同，不是另生成一道）",
               healed["question"]["question"]
               == (await _rule_plan(db, sid_h))[0]["question"],
               str(healed["question"]["question"])[:80])
        _check("[11.5] 自愈后仍可正常作答",
               (await _submit(db, user_id, sid_h))["answer"]["score"] is not None)

        # 自愈在 submit_answer 里也生效（它持行锁，必须先释放再补题）
        sid_h2 = await _make(db, user_id, job_id, total=3)
        await interview_service.start_session(db, user_id, sid_h2)
        victim2 = (await _stored(db, sid_h2))[0]
        await db.delete(victim2)
        await db.commit()
        out_h2 = await _submit(db, user_id, sid_h2)
        _check("[11.6] submit_answer 路径也能自愈（不是 500）",
               out_h2["answer"]["score"] is not None
               and len(await _stored(db, sid_h2)) == 3)

        # ====================================================
        # [12] 进程内结构不得承担正确性
        # ====================================================
        print("\n[12] 进程内并发结构不参与正确性判定")

        svc_imports = _imported_modules(SERVICE_SRC)
        _check("[12.1] Service 不 import interview_production（配额/限流不进业务层）",
               "services.interview_production" not in svc_imports,
               str(sorted(svc_imports)))
        _check("[12.2] Service 不 import interview_orchestrator（内存态不进权威路径）",
               "services.interview_orchestrator" not in svc_imports,
               str(sorted(svc_imports)))
        _check("[12.3] 正确性判据全部来自数据库（唯一约束 + 状态列）",
               "with_for_update" in SERVICE_SRC
               and "IntegrityError" in SERVICE_SRC
               and "current_question_no" in SERVICE_SRC)

        # ====================================================
        # [13] 上下文创建的 TOCTOU：并发补题的第二条收敛点
        # ====================================================
        print("\n[13] create_context：并发建上下文必须收敛（不能靠 rollback）")

        ctx_node = _func_node(CONTEXT_SRC, "create_context")
        ctx_calls = [n for _, n in _call_lines(ctx_node)] if ctx_node else []
        _check("[13.1] 写入不再走「SELECT 判存在 → db.add」两步（AST 取证）",
               ctx_node is not None
               and "add" not in ctx_calls
               and "_idempotent_insert_stmt" in ctx_calls,
               str(ctx_calls))

        # ★ 跨方言取证：SQLite 套件里也能验证 **MySQL 分支**编译出的 SQL 形态。
        #   "源码里有 on_duplicate_key_update 字样" 是弱判据；这里编译真语句。
        probe_values = {
            "session_id": 1, "current_stage": "introduction",
            "asked_questions": [], "covered_topics": [], "weak_topics": [],
            "follow_up_count": 0, "max_questions": 3,
        }
        from sqlalchemy.dialects import mysql as _mysql_dialect
        from sqlalchemy.dialects import sqlite as _sqlite_dialect

        # ★ 不能加 literal_binds：JSON 列没有字面量渲染器（CompileError）。
        #   这里只需要看 SQL 骨架里的收敛子句，参数用占位符即可。
        sqlite_sql = str(
            interview_context._idempotent_insert_stmt(_FakeDb("sqlite"), probe_values)
            .compile(dialect=_sqlite_dialect.dialect())
        )
        mysql_sql = str(
            interview_context._idempotent_insert_stmt(_FakeDb("mysql"), probe_values)
            .compile(dialect=_mysql_dialect.dialect())
        )
        _check("[13.2] SQLite 分支编译出 ON CONFLICT DO NOTHING",
               "ON CONFLICT" in sqlite_sql.upper()
               and "DO NOTHING" in sqlite_sql.upper(), sqlite_sql[:120])
        _check("[13.3] ★ MySQL 分支编译出 ON DUPLICATE KEY UPDATE（生产方言）",
               "ON DUPLICATE KEY UPDATE" in mysql_sql.upper(), mysql_sql[:120])

        try:
            interview_context._idempotent_insert_stmt(_FakeDb("oracle"), probe_values)
            unknown_raised = False
        except RuntimeError:
            unknown_raised = True
        _check("[13.4] 未知方言响亮失败（不静默退回「先查后插」）", unknown_raised)

        sid_ctx = await _make(db, user_id, job_id, total=2)
        # ★ 证明「提交后重读」读的是**库里那一行**，而不是自己构造的 values：
        #   先手工把 stage 改成 closing，再用默认 stage 调一次，期望拿到 closing。
        await interview_context.create_context(db, sid_ctx)
        await interview_context.update_context(db, sid_ctx, current_stage="closing")

        rollback_calls = []
        original_rollback = db.rollback

        async def _spy_rollback(*args, **kwargs):
            rollback_calls.append(1)
            return await original_rollback(*args, **kwargs)

        db.rollback = _spy_rollback
        try:
            outs = [await interview_context.create_context(db, sid_ctx) for _ in range(3)]
        finally:
            db.rollback = original_rollback

        _check("[13.5] 重复调用 3 次不抛异常、结果完全一致",
               all(o == outs[0] for o in outs), str([o.get("current_stage") for o in outs]))
        _check("[13.6] 返回的是库里既有行（stage=closing），不是本次传入的默认值",
               outs[0]["current_stage"] == "closing", str(outs[0]["current_stage"]))
        _check("[13.7] 全程没有调用 db.rollback()（不靠回滚打坏调用方 ORM 状态）",
               rollback_calls == [], str(rollback_calls))

        n_ctx = (await db.execute(
            select(InterviewContext).where(InterviewContext.session_id == sid_ctx)
        )).scalars().all()
        _check("[13.8] 库里仍然只有 1 行上下文（唯一索引兜住）", len(n_ctx) == 1, str(len(n_ctx)))

    await engine.dispose()

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


async def _raises_runtime_error(db: AsyncSession, user: User) -> bool:
    """[4.1]：脏会话调用 _end_read_transaction 必须抛 RuntimeError。"""
    probe = InterviewSession(
        user_id=user.id, status=SESSION_STATUS_CREATED,
        interview_type="technical", difficulty="mid",
        total_questions=1, current_question_no=0, duration=10,
    )
    db.add(probe)
    try:
        await interview_service._end_read_transaction(db)
        return False
    except RuntimeError:
        return True
    finally:
        await db.rollback()


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
