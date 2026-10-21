# -*- coding: utf-8 -*-
"""面试并发正确性 · **真实 MySQL** 运行器（不是回归套件）

为什么需要它
------------
项目硬约束：SQLite 只允许出现在 ``backend/tests/*.py``，而回归套件又**不能依赖
本机 MySQL**（否则换台机器就红）。于是并发验证被拆成两层：

======================  ====================================  ==========================
层                       文件                                  能证明什么
======================  ====================================  ==========================
逻辑收敛（SQLite）        ``tests/test_interview_concurrency.py``  守卫口径 / 幂等 / 收敛分支
真实并发（MySQL 8）      本文件（**不进回归循环**）             行锁真的串行化 / 唯一约束
                                                              真的挡重复 / 事务边界真的
                                                              不跨模型调用
======================  ====================================  ==========================

★ **不能只靠 SQLite 推断 MySQL 行为**——SQLite 上 ``SELECT ... FOR UPDATE``
是空操作，没有真正的多语句并发，也没有 ``REPEATABLE READ`` 快照语义。

安全性（不碰生产数据）
----------------------
本运行器会 **CREATE 一个独立 schema**（``career_conc_probe_<pid>_<时间戳>``），
在里面 ``create_all`` 建表、跑完全部场景，最后 **DROP**。生产库
（``DATABASE_URL`` 指向的那个）**只做一次只读检查**（约束是否存在）。

用法::

    cd backend
    python tests/interview_mysql_concurrency_run.py            # 跑完即清理
    python tests/interview_mysql_concurrency_run.py --keep     # 保留 probe 库供排查

退出码：0 全部通过 / 1 有失败 / 2 环境不满足（连不上 / 无建库权限）
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import pathlib
import sys
import time
import traceback

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
_TESTS = pathlib.Path(__file__).resolve().parent
for _p in (str(_BACKEND), str(_TESTS)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ★ 刻意**不** import regression_env：本运行器要的是真实配置。
#   但也不真的打网络——模型与检索都被替身替换。
#   ★ 数字人通道也必须钉成 Mock（空串 = mock），否则 avatar 会话的 `_deliver`
#     会去打真实讯飞（本运行器与"验证并发"无关，不该消耗配额）。
os.environ["AVATAR_PROVIDER"] = ""
from sqlalchemy import inspect as sa_inspect  # noqa: E402
from sqlalchemy import select, text  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool  # noqa: E402

from database import DATABASE_URL  # noqa: E402
import mysql_guard  # noqa: E402
from models import (  # noqa: E402
    INTERVIEW_MODE_AVATAR,
    SESSION_STATUS_FINISHED,
    InterviewAnswer,
    InterviewQuestion,
    InterviewReport,
    InterviewSession,
    Job,
    User,
)
from schemas.interview import AnswerSubmitRequest, SessionCreateRequest  # noqa: E402
from services import (  # noqa: E402
    interview_next_agent as na,
    interview_service,
)

_PASSED = 0
_FAILED = 0

ANSWER = (
    "我在上一家公司负责订单中台重构，把单体拆成 8 个微服务，"
    "用 Python + MySQL + Redis + Kafka，QPS 从 800 提到 5000。"
)

PROBE_PREFIX = "career_conc_probe_"


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
# 替身
# ============================================================
class MockSpark:
    """按顺序吐回复的假 Spark（线程/协程安全：asyncio 单线程，pop 即原子）。"""

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
        "reason": "probe",
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


# ============================================================
# 工具
# ============================================================
def _probe_url(base: str, schema: str) -> str:
    """把 ``DATABASE_URL`` 的库名换成 probe schema。"""
    head, _, _tail = base.rpartition("/")
    # 去掉可能存在的 ?charset=... 后再拼
    if "?" in head:
        head = head.split("?")[0]
    return f"{head}/{schema}?charset=utf8mb4"


def _redact(url: str) -> str:
    return url.rsplit("@", 1)[-1] if "@" in url else url


async def _count(db: AsyncSession, model, **filters) -> int:
    stmt = select(model)
    for k, v in filters.items():
        stmt = stmt.where(getattr(model, k) == v)
    return len((await db.execute(stmt)).scalars().all())


async def _answers_of(db: AsyncSession, session_id: int) -> list:
    rows = (await db.execute(
        select(InterviewAnswer)
        .join(InterviewQuestion, InterviewAnswer.question_id == InterviewQuestion.id)
        .where(InterviewQuestion.session_id == session_id)
        .order_by(InterviewQuestion.question_no)
    )).scalars().all()
    return list(rows)


# ============================================================
# 主流程
# ============================================================
async def run(keep: bool) -> int:
    print("=" * 74)
    print("面试并发正确性 · 真实 MySQL 运行器")
    print("=" * 74)
    print(f"  生产库（只读检查）：{_redact(DATABASE_URL)}")

    if not DATABASE_URL.startswith("mysql"):
        print("[SKIP] DATABASE_URL 不是 MySQL，本运行器不适用")
        return 2

    # ★ 目标库守卫：集成测试会建库建表并 DROP，绝不允许指向生产 / 开发库。
    _unsafe = mysql_guard.unsafe_reason(DATABASE_URL)
    if _unsafe:
        print(f"[SKIP] {_unsafe}")
        return 2

    schema = f"{PROBE_PREFIX}{os.getpid()}_{int(time.time())}"

    # ---------- 生产库：只读核对约束 ----------
    prod_engine = create_async_engine(DATABASE_URL, poolclass=NullPool)
    try:
        async with prod_engine.connect() as conn:
            def _prod_uq(sync_conn):
                insp = sa_inspect(sync_conn)
                if not insp.has_table("interview_question"):
                    return None
                names = {u["name"] for u in insp.get_unique_constraints("interview_question")}
                names |= {i["name"] for i in insp.get_indexes("interview_question")
                          if i.get("unique") and i.get("name")}
                return names

            prod_uqs = await conn.run_sync(_prod_uq)
    finally:
        await prod_engine.dispose()

    print("\n[1] 目标库结构：唯一约束必须真的存在（不是只写在模型里）")
    if prod_uqs is None:
        print("  [INFO] 目标库里还没有 interview_question 表 —— 先跑一次初始化：")
        print("         cd backend && python scripts/ci_bootstrap_db.py")
        print("         （CI 里这一步由 workflow 显式执行；本地手工跑运行器前也要跑）")
    _check("[1.1] interview_question 表存在", prod_uqs is not None, str(prod_uqs))
    _check("[1.2] 真实库含 uq_interview_question_session_no",
           bool(prod_uqs) and "uq_interview_question_session_no" in prod_uqs,
           str(prod_uqs))

    # ---------- 建 probe schema ----------
    admin_engine = create_async_engine(DATABASE_URL, poolclass=NullPool)
    try:
        async with admin_engine.begin() as conn:
            await conn.execute(text(
                f"CREATE DATABASE IF NOT EXISTS `{schema}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            ))
    except Exception as exc:  # noqa: BLE001
        print(f"[SKIP] 无法创建 probe 库（{type(exc).__name__}）：{exc}")
        await admin_engine.dispose()
        return 2
    finally:
        await admin_engine.dispose()

    probe_url = _probe_url(DATABASE_URL, schema)
    print(f"  probe 库：{schema}")

    # 引擎：NullPool ⇒ 每个会话一条独立物理连接，才谈得上"真并发"
    engine = create_async_engine(probe_url, poolclass=NullPool)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    from database import Base

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    try:
        async with factory() as db:
            user = User(username="conc_probe", email="conc_probe@example.com",
                        password_hash="x", role="user")
            job = Job(job_name="并发探针岗", city="北京", skills="Python,Redis",
                      salary="10-20K")
            db.add_all([user, job])
            await db.commit()
            await db.refresh(user)
            await db.refresh(job)
            user_id, job_id = user.id, job.id

        # ====================================================
        print("\n[2] 引擎与隔离级别")
        async with factory() as db:
            level = (await db.execute(
                text("SELECT @@transaction_isolation")
            )).scalar()
            ver = (await db.execute(text("SELECT VERSION()"))).scalar()
        _check("[2.1] 版本是 MySQL 8", str(ver).startswith("8."), str(ver))
        _check("[2.2] 默认隔离级别是 REPEATABLE-READ（长事务会保留快照）",
               level == "REPEATABLE-READ", str(level))

        # ====================================================
        print("\n[3] 唯一约束在真实库上真的挡重复（两条并发 INSERT）")

        async def _raw_insert_question(sid: int, qno: int, tag: str):
            async with factory() as db:
                db.add(InterviewQuestion(
                    session_id=sid, question_no=qno, question=f"probe-{tag}",
                    question_type="technical", topic="probe",
                    difficulty="mid", expected_points=[],
                ))
                try:
                    await db.commit()
                    return "ok"
                except Exception as exc:  # noqa: BLE001
                    await db.rollback()
                    return type(exc).__name__

        async with factory() as db:
            sid_dup = (await interview_service.create_session(
                db, user_id, SessionCreateRequest(job_id=job_id, total_questions=3)
            ))["session"]["id"]

        r1, r2 = await asyncio.gather(
            _raw_insert_question(sid_dup, 99, "a"),
            _raw_insert_question(sid_dup, 99, "b"),
        )
        outcomes = sorted([r1, r2])
        _check("[3.1] 两个并发 INSERT 只有 1 条成功",
               outcomes.count("ok") == 1, str(outcomes))
        _check("[3.2] 另一个以 IntegrityError 失败（不是静默成功）",
               any("Integrity" in o for o in outcomes), str(outcomes))
        async with factory() as db:
            n = await _count(db, InterviewQuestion, session_id=sid_dup, question_no=99)
        _check("[3.3] 库里只有 1 行 (session_id, question_no=99)", n == 1, str(n))

        # ====================================================
        print("\n[4] 并发提交作答：行锁必须把两条请求串行化")

        async with factory() as db:
            sid_ans = (await interview_service.create_session(
                db, user_id, SessionCreateRequest(job_id=job_id, total_questions=3)
            ))["session"]["id"]
            await interview_service.start_session(db, user_id, sid_ans)

        async def _submit(tag: str):
            async with factory() as db:
                try:
                    out = await interview_service.submit_answer(
                        db, user_id, sid_ans, AnswerSubmitRequest(answer_text=ANSWER + tag)
                    )
                    return ("ok", out["session"]["current_question_no"])
                except Exception as exc:  # noqa: BLE001
                    return (type(exc).__name__,
                            getattr(exc, "detail", str(exc))[:40])

        res_a, res_b = await asyncio.gather(_submit("A"), _submit("B"))
        _check("[4.1] 两条并发提交都成功（FOR UPDATE 让它们各答一题）",
               res_a[0] == "ok" and res_b[0] == "ok", f"{res_a} / {res_b}")
        async with factory() as db:
            n_ans = len(await _answers_of(db, sid_ans))
            sess = await db.get(InterviewSession, sid_ans)
            qno = sess.current_question_no
        _check("[4.2] 恰好 2 条作答（没有重复评分 / 重复落库）", n_ans == 2, str(n_ans))
        _check("[4.3] 题号推进到 3（两次各推进 1，没有跳号）", qno == 3, str(qno))

        # ====================================================
        print("\n[5] 并发补题（无行锁路径）：唯一约束必须兜住")

        # 开场用开关打开（只落第 1 题）；提交时**显式关闭**（否则 submit 自己会补题）
        async with factory() as db:
            with _flag("1"):
                sid_q = (await interview_service.create_session(
                    db, user_id,
                    SessionCreateRequest(job_id=job_id, total_questions=3,
                                         mode=INTERVIEW_MODE_AVATAR),
                ))["session"]["id"]
                await interview_service.start_session(db, user_id, sid_q)
            with _flag("0"):
                await interview_service.submit_answer(
                    db, user_id, sid_q, AnswerSubmitRequest(answer_text=ANSWER)
                )

        async with factory() as db:
            before = await _count(db, InterviewQuestion, session_id=sid_q)
        _check("[5.0] 前置：库里只有第 1 题、题号已推进到 2",
               before == 1, str(before))

        async def _gen(tag: str):
            async with factory() as db:
                try:
                    out = await interview_service.generate_and_persist_next(
                        db, user_id, sid_q, use_rag=False
                    )
                    return ("ok", out.get("source"),
                            (out.get("question") or {}).get("id"))
                except Exception as exc:  # noqa: BLE001
                    if os.environ.get("CONC_DEBUG"):
                        # ★ 只打印，不影响断言：并发失败时"异常名 + 40 字"往往
                        #   不足以定位（例如 StatementError 的真实 cause 在 __cause__）
                        print(f"\n--- [{tag}] 异常全文 ---")
                        traceback.print_exc()
                    return (type(exc).__name__,
                            getattr(exc, "detail", str(exc))[:40], None)

        # 两条请求各自做幂等预检查时都看不到对方 ⇒ 都会走到 INSERT，
        # 由唯一约束收敛（这正是改造前在生产库上会插出重复行的那条路径）。
        with _installed_spark(MockSpark(
            json.dumps({"question": "Q2-并发-A", "question_type": "technical",
                        "topic": "Redis", "difficulty": "mid",
                        "expected_points": [], "reason": "probe"}, ensure_ascii=False),
            json.dumps({"question": "Q2-并发-B", "question_type": "technical",
                        "topic": "Redis", "difficulty": "mid",
                        "expected_points": [], "reason": "probe"}, ensure_ascii=False),
        )):
            g1, g2 = await asyncio.gather(_gen("A"), _gen("B"))

        async with factory() as db:
            after = await _count(db, InterviewQuestion, session_id=sid_q)

        _check("[5.1] 两条并发补题都没有抛异常",
               g1[0] == "ok" and g2[0] == "ok", f"{g1} / {g2}")
        _check("[5.2] 只多出 1 行题目（唯一约束挡住了第二次 INSERT）",
               after == before + 1, f"{before}->{after}")
        _check("[5.3] 至少一条以 existing 收敛（另一条才是真正写入者）",
               "existing" in (g1[1], g2[1]), f"{g1[1]} / {g2[1]}")
        _check("[5.4] 两条拿到的是**同一行**（同一个 id）",
               g1[2] is not None and g1[2] == g2[2], f"{g1[2]} / {g2[2]}")

        # ====================================================
        print("\n[6] 并发结束：只出一份报告")

        async with factory() as db:
            sid_end = (await interview_service.create_session(
                db, user_id, SessionCreateRequest(job_id=job_id, total_questions=2)
            ))["session"]["id"]
            await interview_service.start_session(db, user_id, sid_end)
            await interview_service.submit_answer(
                db, user_id, sid_end, AnswerSubmitRequest(answer_text=ANSWER)
            )

        async def _end():
            async with factory() as db:
                try:
                    out = await interview_service.end_session(db, user_id, sid_end)
                    return ("ok", out["report"]["id"], out["session"]["status"])
                except Exception as exc:  # noqa: BLE001
                    return (type(exc).__name__,
                            getattr(exc, "detail", str(exc))[:40], None)

        e1, e2 = await asyncio.gather(_end(), _end())
        async with factory() as db:
            n_rep = await _count(db, InterviewReport, session_id=sid_end)
            st = (await db.get(InterviewSession, sid_end)).status
        _check("[6.1] 两条并发 end 都拿到报告", e1[0] == "ok" and e2[0] == "ok",
               f"{e1} / {e2}")
        _check("[6.2] 报告表只有 1 行（没有重复出报告）", n_rep == 1, str(n_rep))
        _check("[6.3] 两条拿到的是**同一份**报告", e1[1] == e2[1], f"{e1[1]} / {e2[1]}")
        _check("[6.4] 会话停在 finished（没有被回滚成 ongoing）",
               st == SESSION_STATUS_FINISHED, str(st))

        # ====================================================
        print("\n[7] 并发开始（start 重放）：题数不翻倍")

        async with factory() as db:
            sid_st = (await interview_service.create_session(
                db, user_id, SessionCreateRequest(job_id=job_id, total_questions=3)
            ))["session"]["id"]

        async def _start():
            async with factory() as db:
                try:
                    out = await interview_service.start_session(db, user_id, sid_st)
                    return ("ok", out["session"]["status"])
                except Exception as exc:  # noqa: BLE001
                    return (type(exc).__name__,
                            getattr(exc, "detail", str(exc))[:40])

        s1, s2 = await asyncio.gather(_start(), _start())
        async with factory() as db:
            n_q = await _count(db, InterviewQuestion, session_id=sid_st)
            st2 = (await db.get(InterviewSession, sid_st)).status
        _check("[7.1] 恰好一条成功、另一条被 400 拒绝",
               sorted([s1[0], s2[0]]) == ["HTTPException", "ok"], f"{s1} / {s2}")
        _check("[7.2] 题数没有翻倍（仍是 3 道）", n_q == 3, str(n_q))
        _check("[7.3] 状态是 ongoing", st2 == "ongoing", str(st2))

        # ====================================================
        print("\n[8] 模型超时：规则兜底 + 不留半截数据")

        with _flag("1"):
            async with factory() as db:
                sid_to = (await interview_service.create_session(
                    db, user_id,
                    SessionCreateRequest(job_id=job_id, total_questions=3,
                                         mode=INTERVIEW_MODE_AVATAR),
                ))["session"]["id"]
                await interview_service.start_session(db, user_id, sid_to)

            async with factory() as db:
                with _installed_spark(MockSpark(RuntimeError("模拟模型超时"))):
                    out_to = await interview_service.submit_answer(
                        db, user_id, sid_to, AnswerSubmitRequest(answer_text=ANSWER)
                    )
            async with factory() as db:
                n_to = await _count(db, InterviewQuestion, session_id=sid_to)
            _check("[8.1] 超时没有打断答题",
                   out_to["answer"]["score"] is not None, str(out_to)[:100])
            _check("[8.2] 下一题降级为规则兜底（不是 None）",
                   out_to["next_question"] is not None
                   and out_to["next_question"]["question_no"] == 2,
                   str(out_to.get("next_question"))[:100])
            _check("[8.3] 兜底题已落库、恰好 2 行（无半截数据）", n_to == 2, str(n_to))

        # ====================================================
        print("\n[9] 提交冲突（真实 IntegrityError）：收敛为 existing")

        with _flag("1"):
            async with factory() as db:
                sid_cf = (await interview_service.create_session(
                    db, user_id,
                    SessionCreateRequest(job_id=job_id, total_questions=3,
                                         mode=INTERVIEW_MODE_AVATAR),
                ))["session"]["id"]
                await interview_service.start_session(db, user_id, sid_cf)
                with _installed_spark(_spark_ok("Q2-正常写入")):
                    await interview_service.submit_answer(
                        db, user_id, sid_cf, AnswerSubmitRequest(answer_text=ANSWER)
                    )
            async with factory() as db:
                before_cf = await _count(db, InterviewQuestion, session_id=sid_cf)

            # 让幂等预检查"看不见"已有行 ⇒ 必然走到 INSERT ⇒ 真实 IntegrityError
            real_load = interview_service._load_questions
            state = {"n": 0}

            async def _blind(db_, session_id_):
                state["n"] += 1
                if state["n"] == 1:
                    return []
                return await real_load(db_, session_id_)

            async with factory() as db:
                with _installed_spark(_spark_ok("Q2-竞争失败者")):
                    with _patched(interview_service, "_load_questions", _blind):
                        out_cf = await interview_service.generate_and_persist_next(
                            db, user_id, sid_cf, use_rag=False
                        )
            async with factory() as db:
                after_cf = await _count(db, InterviewQuestion, session_id=sid_cf)
            _check("[9.1] 真实 IntegrityError 被收敛成 existing（不抛 409）",
                   out_cf["ok"] is True
                   and out_cf["source"] == interview_service.SOURCE_EXISTING,
                   str(out_cf.get("source")))
            _check("[9.2] 行数不变（唯一约束真的生效）",
                   after_cf == before_cf, f"{before_cf}->{after_cf}")

        # ====================================================
        print("\n[10] 缺行自愈：失败窗口可恢复（真实库）")

        async with factory() as db:
            sid_heal = (await interview_service.create_session(
                db, user_id, SessionCreateRequest(job_id=job_id, total_questions=3)
            ))["session"]["id"]
            await interview_service.start_session(db, user_id, sid_heal)
            victim = (await db.execute(
                select(InterviewQuestion)
                .where(InterviewQuestion.session_id == sid_heal,
                       InterviewQuestion.question_no == 1)
            )).scalars().first()
            await db.delete(victim)
            await db.commit()

        async with factory() as db:
            healed = await interview_service.get_current_question(db, user_id, sid_heal)
        async with factory() as db:
            rows_heal = (await db.execute(
                select(InterviewQuestion)
                .where(InterviewQuestion.session_id == sid_heal)
                .order_by(InterviewQuestion.question_no)
            )).scalars().all()
        _check("[10.1] get_current_question 自愈而不是 500",
               healed["question"] is not None and healed["question_no"] == 1,
               str(healed.get("question"))[:100])
        _check("[10.2] 题号重新连续 1..3",
               [r.question_no for r in rows_heal] == [1, 2, 3],
               str([r.question_no for r in rows_heal]))

        # ====================================================
        print("\n[11] 事务边界：模型调用期间业务会话没有活跃事务")

        # 取证方式：在**同一个业务会话**上检查 `in_transaction()`。
        # ① 进入 prepare_candidate 时（刚做完 `_end_read_transaction`）应为 False
        #    ⇒ 证明第一次"结束只读事务"真的生效；
        # ② 进入 generate_from_prepared（模型调用）时必须为 False
        #    ⇒ 证明 8~19 秒的外部调用期间没有事务开着。
        # 这比查 information_schema.INNODB_TRX 精确（后者会混入同机其它连接）。
        from services import interview_next_flow as _flow

        captured: dict = {}
        real_prep = _flow.prepare_candidate
        real_gen = _flow.generate_from_prepared

        async def _prep_spy(db_, *a, **kw):
            captured["db"] = db_
            captured["in_tx_at_prepare"] = db_.in_transaction()
            return await real_prep(db_, *a, **kw)

        async def _gen_spy(prepared, *, spark=None):
            d = captured.get("db")
            captured["in_tx_at_llm"] = None if d is None else d.in_transaction()
            return await real_gen(prepared, spark=spark)

        async with factory() as db:
            with _flag("1"):
                sid_tx = (await interview_service.create_session(
                    db, user_id,
                    SessionCreateRequest(job_id=job_id, total_questions=3,
                                         mode=INTERVIEW_MODE_AVATAR),
                ))["session"]["id"]
                await interview_service.start_session(db, user_id, sid_tx)
            with _patched(_flow, "prepare_candidate", _prep_spy), _patched(
                _flow, "generate_from_prepared", _gen_spy
            ):
                with _installed_spark(_spark_ok("Q2-事务边界探针")):
                    await interview_service.submit_answer(
                        db, user_id, sid_tx, AnswerSubmitRequest(answer_text=ANSWER)
                    )

        _check("[11.1] 探针确实跑过（不是恒真）",
               "in_tx_at_prepare" in captured and "in_tx_at_llm" in captured,
               str(captured.keys()))
        _check("[11.2] 取材料阶段开始时事务已关闭",
               captured.get("in_tx_at_prepare") is False,
               f"in_tx_at_prepare={captured.get('in_tx_at_prepare')}")
        _check("[11.3] ★ 模型调用期间业务会话**没有**活跃事务",
               captured.get("in_tx_at_llm") is False,
               f"in_tx_at_llm={captured.get('in_tx_at_llm')}")

    finally:
        await engine.dispose()
        if keep:
            print(f"\n[KEEP] probe 库保留：{schema}")
        else:
            admin = create_async_engine(DATABASE_URL, poolclass=NullPool)
            try:
                async with admin.begin() as conn:
                    await conn.execute(text(f"DROP DATABASE IF EXISTS `{schema}`"))
                print(f"\n[CLEAN] 已删除 probe 库：{schema}")
            finally:
                await admin.dispose()

    print("\n" + "=" * 74)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return 0 if _FAILED == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="面试并发正确性 · 真实 MySQL 运行器")
    parser.add_argument("--keep", action="store_true", help="保留 probe 库供排查")
    args = parser.parse_args()
    return asyncio.run(run(args.keep))


if __name__ == "__main__":
    sys.exit(main())
