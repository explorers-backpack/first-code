# -*- coding: utf-8 -*-
"""AI 模拟面试 · InterviewOrchestrator（数字人面试全生命周期编排层）。

分层定位
--------
::

    api/interview.py                    ← HTTP 契约
        └── interview_service           ← Service：会话管理 / **唯一**接触 DB
              ├── interview_core        ← Core：出题 / 评分 / 报告（本模块**不修改**它）
              └── avatar_interview      ← 通道：数字人（→ avatar_provider 层）

    services/interview_orchestrator     ← 【本模块】编排层：把上面这些能力
                                          按「数字人面试」的时间顺序串起来，
                                          并维护面试的**编排状态机**。

为什么需要这一层
----------------
``interview_service`` 提供的是**单步能力**（创建会话 / 开始 / 取题 / 提交 / 结束），
每一次调用都是**无状态**的——它只回答「这一步做完的结果是什么」，不关心
「上一步做过什么、下一步该做什么」。数字人面试需要的却是一条**有状态的时间线**：

    创建 Session → 初始化 Avatar → 生成问题 → 数字人播报
    → 等待回答 → 提交答案 → 评分 → 下一题（回到「生成问题」）→ … → 结束

本模块把这条时间线**显式化**：每一步都推进一个**显式状态**，顺序不对就报错，
而不是让前端自己去猜「现在该调哪个接口」。

三条不变量（务必保持）
----------------------
1. **不修改 ``interview_core``**：出题、评分、报告的实现一行都不动；
   本模块只**调用** ``interview_core`` / ``interview_service`` 的既有入口。
2. **不新增数据库列**：编排状态是**进程内**的运行态（``_STATES``），
   库表结构与 ``utils/schema_sync`` 都不动；进程重启后状态丢失可自愈
   （见 ``_advance`` 的「无记录即从初始态起算」）。
3. **依赖单向**：``interview_orchestrator → interview_service → (interview_core |
   avatar_interview)``。本模块**不得被** ``interview_core`` / ``interview_service`` /
   ``api/*`` 反向 import —— 否则会把「可选编排」变成「必经路径」，改变现有行为。

状态机（5 个状态）
------------------
======================  ==================================================
状态                     含义
======================  ==================================================
``waiting_question``     会话已建立，尚未拿到要播报的题目
``avatar_speaking``      题目已交付，正在播报（数字人模式下即「数字人说话中」）
``waiting_answer``       播报结束，等待用户作答
``processing_answer``    用户已提交，正在评分
``finished``             面试结束（**终态**，不可再离开）
======================  ==================================================

事件驱动的转移（``transition(state, event)`` 是**纯函数**，可脱离 DB 单测）::

    waiting_question   --question_ready--->  avatar_speaking
    avatar_speaking    --speech_done------>  waiting_answer
    waiting_answer     --answer_submitted->  processing_answer
    processing_answer  --scored---------->  avatar_speaking   （还有下一题，已交付播报）
    processing_answer  --all_answered---->  finished          （全部答完）
    <任意状态>          --finish---------->  finished          （提前结束 / 幂等）

``avatar_speaking`` 与 ``processing_answer`` 是**瞬时态**：进入后必然由**同一次**
编排调用（``start`` / ``submit_answer``）推进到下一个稳定态，不会长期停留。

两种交互模式都适用
------------------
本模块对 ``interview_mode`` **不做分支**：``text`` 会话下 ``avatar_speaking`` 只表示
「题目已交付」（``interview_service._deliver`` 对 text 原样返回），语义仍然成立。
数字人专属的播报由通道层 ``avatar_interview`` 负责，本模块不感知 provider。

数字人闭环（提问 → 播报 → 语音作答 → 评分 → 下一题）
-----------------------------------------------------
闭环在**本层**被显式串起来（两条新增入口，其余入口不变）::

    ① create_session   → 状态 waiting_question
    ② prepare_avatar   → 【Avatar 启动】预检 / 初始化数字人（幂等）
    ③ start            → 生成整场题目 + 播报第一题 → 状态 avatar_speaking
    ④ speech_done      → 播报结束 → 状态 waiting_answer
    ⑤ answer_from_asr  → 【ASR 取文本 → submit_answer()】评分 + 交付下一题
                         → 状态 avatar_speaking（还有题）/ finished（答完）
    ⑥ finish           → 结束 + 报告

其中 ``prepare_avatar`` / ``answer_from_asr`` 复用 ``interview_service`` 的
``ensure_avatar`` / ``capture_avatar_answer`` / ``submit_answer``——
**编排层不直接碰通道层**（``avatar_interview``），也不碰 DB、不碰 provider：
「取 session 行 / 归属校验 / 调 provider」全部留在 Service 与通道层。

异常处置（4 类，全部**如实上报而非静默**）
------------------------------------------
============================  ==================================================
故障                           表现与处置
============================  ==================================================
**Avatar 启动失败**            ``prepare_avatar()["ok"] is False``；``status`` 区分
                              ``unavailable`` / ``expired`` / ``disabled`` / ``failed``。
                              **不阻断**面试：``start`` 仍会出题（交付降级为文字）。
**ASR 失败**                   ``answer_from_asr()["ok"] is False`` 且 ``state`` 不变，
                              **绝不调用** ``submit_answer``（不会写入空答案）；
                              ``status="empty"`` 时 ``retryable=True``（让用户重说）。
**网络异常**                   provider 抛 ``AvatarProviderUnavailableError`` ⇒
                              ``status="unavailable"``、``retryable=True``（原地重试）。
**Session 失效**               （a）**数字人会话**失效 ⇒ ``status="expired"``、
                              ``retryable=False``（重试无用，需重建会话：
                              调用 ``avatar_interview.reset_provider()`` 或新建 provider）；
                              （b）**面试会话**不存在 / 非本人 ⇒ 沿用服务层既有口径，
                              抛 ``HTTPException`` 404（与其余编排入口一致，不吞异常）。
**非数字人会话**               ``interview_mode=text`` 的会话被误调 ⇒
                              ``status="unsupported"``（``ok=False``，不重试、不打 provider）。
============================  ==================================================

对外约定
--------
- 异步方法第一参数均为 ``db: AsyncSession``，返回**普通 dict**（在 ``interview_service``
  的返回值上**追加** ``state`` 键），不依赖 FastAPI 请求上下文。
- 状态表是**进程内**的，测试用 ``clear_states()`` / ``reset_state(id)`` 清理。
- 状态类错误抛 ``OrchestratorStateError``（**不是** ``HTTPException``）——
  API 层若接入本模块，自行决定映射成 400 还是 409。
"""

from __future__ import annotations

import contextlib
import threading
from typing import Any, Dict, Iterator, Mapping, Optional, Tuple

from services import interview_core, interview_service

__all__ = [
    # ---- 状态 ----
    "STATE_WAITING_QUESTION",
    "STATE_AVATAR_SPEAKING",
    "STATE_WAITING_ANSWER",
    "STATE_PROCESSING_ANSWER",
    "STATE_FINISHED",
    "STATES",
    "INITIAL_STATE",
    "TERMINAL_STATES",
    # ---- 事件 ----
    "EVENT_QUESTION_READY",
    "EVENT_SPEECH_DONE",
    "EVENT_ANSWER_SUBMITTED",
    "EVENT_SCORED",
    "EVENT_ALL_ANSWERED",
    "EVENT_FINISH",
    "EVENTS",
    # ---- 转移表与纯函数 ----
    "TRANSITIONS",
    "transition",
    "can_transition",
    "is_valid_state",
    "is_terminal",
    # ---- 进程内状态表 ----
    "get_state",
    "set_state",
    "apply_event",
    "reset_state",
    "clear_states",
    "states_snapshot",
    # ---- 编排（复用 interview_service / interview_core）----
    "create_session",
    "start",
    "speech_done",
    "generate_question",
    "submit_answer",
    "finish",
    "status",
    # ---- 顺序守卫（只校验不落位）----
    "assert_can",
    # ---- 数字人闭环：Avatar 预检 / 语音作答 ----
    "prepare_avatar",
    "answer_from_asr",
    "ASR_RESULT_FIELDS",
    "SOURCE_ASR",
    "SOURCE_FALLBACK",
    # ---- 评分 ----
    "score_answer",
    "preview_score",
    # ---- 异常 ----
    "OrchestratorError",
    "OrchestratorStateError",
]


# ============================================================
# 一、异常
# ============================================================
class OrchestratorError(RuntimeError):
    """编排层错误基类（继承 RuntimeError：属于「流程走不下去」，不是调用参数写错）。"""


class OrchestratorStateError(OrchestratorError):
    """状态机错误：未知状态 / 未知事件 / 非法转移 / 会话无状态记录。

    单独成类，便于调用方把「顺序错了」与「服务层拒绝」区分开：
    前者是**编排顺序**问题（前端该先播报再等回答），后者是**业务校验**问题。
    """


# ============================================================
# 二、状态与事件（常量 + 转移表）
# ============================================================
#: 会话已建立，尚未拿到要播报的题目。
STATE_WAITING_QUESTION = "waiting_question"
#: 题目已交付、正在播报（数字人模式下 = 数字人说话中）。
STATE_AVATAR_SPEAKING = "avatar_speaking"
#: 播报结束，等待用户作答。
STATE_WAITING_ANSWER = "waiting_answer"
#: 用户已提交，正在评分。
STATE_PROCESSING_ANSWER = "processing_answer"
#: 面试结束（终态）。
STATE_FINISHED = "finished"

#: 全部合法状态（顺序 = 生命周期顺序，便于前端按序展示）。
STATES: Tuple[str, ...] = (
    STATE_WAITING_QUESTION,
    STATE_AVATAR_SPEAKING,
    STATE_WAITING_ANSWER,
    STATE_PROCESSING_ANSWER,
    STATE_FINISHED,
)

#: 新会话的初始状态。
INITIAL_STATE = STATE_WAITING_QUESTION

#: 终态集合（进入后只能再收 ``finish`` 事件，且原地不动）。
TERMINAL_STATES: Tuple[str, ...] = (STATE_FINISHED,)

#: 题目已就绪、开始播报。
EVENT_QUESTION_READY = "question_ready"
#: 播报结束（数字人播完 / 前端播放完毕回调）。
EVENT_SPEECH_DONE = "speech_done"
#: 用户提交了答案。
EVENT_ANSWER_SUBMITTED = "answer_submitted"
#: 评分完成，且**还有下一题**（下一题已交付播报）。
EVENT_SCORED = "scored"
#: 评分完成，且**全部题目已答完**。
EVENT_ALL_ANSWERED = "all_answered"
#: 结束面试（任意状态均可；在终态上是幂等的）。
EVENT_FINISH = "finish"

#: 全部合法事件。
EVENTS: Tuple[str, ...] = (
    EVENT_QUESTION_READY,
    EVENT_SPEECH_DONE,
    EVENT_ANSWER_SUBMITTED,
    EVENT_SCORED,
    EVENT_ALL_ANSWERED,
    EVENT_FINISH,
)

#: 转移表：``(当前状态, 事件) -> 目标状态``。**这是状态机的唯一权威**。
TRANSITIONS: Dict[Tuple[str, str], str] = {
    (STATE_WAITING_QUESTION, EVENT_QUESTION_READY): STATE_AVATAR_SPEAKING,
    (STATE_AVATAR_SPEAKING, EVENT_SPEECH_DONE): STATE_WAITING_ANSWER,
    (STATE_WAITING_ANSWER, EVENT_ANSWER_SUBMITTED): STATE_PROCESSING_ANSWER,
    (STATE_PROCESSING_ANSWER, EVENT_SCORED): STATE_AVATAR_SPEAKING,
    (STATE_PROCESSING_ANSWER, EVENT_ALL_ANSWERED): STATE_FINISHED,
}
# ``finish`` 是「逃生舱」：任意状态都能提前结束，在终态上幂等。
for _state in STATES:
    TRANSITIONS[(_state, EVENT_FINISH)] = STATE_FINISHED


def is_valid_state(state: str) -> bool:
    """``state`` 是否为合法状态之一。"""
    return state in STATES


def is_terminal(state: str) -> bool:
    """``state`` 是否为终态。"""
    return state in TERMINAL_STATES


def can_transition(state: str, event: str) -> bool:
    """``state`` 上能否应用 ``event``（未知状态 / 未知事件一律 ``False``，不抛错）。"""
    return (state, event) in TRANSITIONS


def transition(state: str, event: str) -> str:
    """纯函数：由 ``state`` 经 ``event`` 得到目标状态。

    非法组合**抛错不静默**（``OrchestratorStateError``）——状态机的价值就在于
    「顺序不对时立刻报错」，静默返回原状态会让调用方误以为推进成功了。
    """
    if state not in STATES:
        raise OrchestratorStateError(
            f"未知状态：{state!r}；合法状态：{'/'.join(STATES)}"
        )
    if event not in EVENTS:
        raise OrchestratorStateError(
            f"未知事件：{event!r}；合法事件：{'/'.join(EVENTS)}"
        )
    target = TRANSITIONS.get((state, event))
    if target is None:
        allowed = [e for e in EVENTS if (state, e) in TRANSITIONS]
        raise OrchestratorStateError(
            f"非法状态转移：{state} --{event}--> ？；"
            f"该状态只接受：{'/'.join(allowed) or '（无）'}"
        )
    return target


# ============================================================
# 三、进程内状态表（编排状态是运行态，**不落库**）
# ============================================================
#: ``session_id -> state``。刻意只存进程内：库表不动，重启后自愈。
_STATES: Dict[int, str] = {}
_STATES_LOCK = threading.Lock()


def get_state(session_id: int, default: Optional[str] = None) -> Optional[str]:
    """取会话当前编排状态；**无记录返回 ``default``（默认 ``None``）**。

    返回 ``None`` 表示「本进程还没有这场的编排记录」（首次编排 / 进程重启），
    调用方据此决定是「从初始态起算」还是「报错」。
    """
    return _STATES.get(session_id, default)


def set_state(session_id: int, state: str) -> str:
    """直接置位（会校验状态合法性）。用于「按库重建状态」等场景。"""
    if state not in STATES:
        raise OrchestratorStateError(
            f"未知状态：{state!r}；合法状态：{'/'.join(STATES)}"
        )
    with _STATES_LOCK:
        _STATES[session_id] = state
    return state


def apply_event(session_id: int, event: str) -> str:
    """在**已记录**的当前状态上应用事件并落位，返回新状态。

    与 ``transition`` 的区别：本函数读状态表、会写回；**无记录直接报错**
    （而不是猜一个初始态），因为「没记录还要推事件」说明调用顺序本身就错了。
    """
    with _STATES_LOCK:
        current = _STATES.get(session_id)
        if current is None:
            raise OrchestratorStateError(
                f"会话 {session_id} 尚无编排状态记录，无法应用事件 {event!r}"
                "（请先经 create_session / start 建立编排）"
            )
        target = transition(current, event)
        _STATES[session_id] = target
        return target


def reset_state(session_id: int) -> None:
    """删除某会话的编排状态（不影响数据库）。"""
    with _STATES_LOCK:
        _STATES.pop(session_id, None)


def clear_states() -> None:
    """清空全部编排状态（测试用；不影响数据库）。"""
    with _STATES_LOCK:
        _STATES.clear()


def states_snapshot() -> Dict[int, str]:
    """当前进程内全部编排状态的**副本**（只读快照）。"""
    with _STATES_LOCK:
        return dict(_STATES)


def _advance(session_id: int, event: str) -> str:
    """内部：按事件推进状态。

    - 有记录 → 严格按转移表走（顺序不对即报错）；
    - **无记录** → 从 ``INITIAL_STATE`` 起算（进程重启后的自愈：只要事件本身
      在该状态上合法，就把它当成一次全新的编排）。

    自愈**不放松合法性**：例如无记录时应用 ``scored`` 仍会因
    ``waiting_question --scored--> ？`` 非法而报错。
    """
    current = get_state(session_id, INITIAL_STATE)
    target = transition(current, event)
    return set_state(session_id, target)


@contextlib.contextmanager
def _rollback_state_on_error(session_id: int) -> Iterator[None]:
    """本次编排调用抛错时，把状态表回滚到调用前（保证「状态与库一致」）。

    例：``submit_answer`` 已进入 ``processing_answer``，但服务层因「答案为空」
    拒绝 → 若不回滚，状态会永远卡在 ``processing_answer``，后续调用全部报错。
    """
    previous = get_state(session_id)
    try:
        yield
    except BaseException:
        if previous is None:
            reset_state(session_id)
        else:
            set_state(session_id, previous)
        raise


# ============================================================
# 四、编排：生命周期（每一步都复用 interview_service 的既有入口）
# ============================================================
async def create_session(
    db: Any, user_id: int, payload: Any
) -> Dict[str, Any]:
    """创建会话（状态 ``created``）并把编排状态置为 ``waiting_question``。

    纯透传 ``interview_service.create_session``（校验 / 落库都在那里），
    本层只多做一件事：**登记编排状态**。
    """
    result = await interview_service.create_session(db, user_id, payload)
    session_id = result["session"]["id"]
    set_state(session_id, INITIAL_STATE)
    return {**result, "state": INITIAL_STATE}


async def start(db: Any, user_id: int, session_id: int) -> Dict[str, Any]:
    """开始面试：**初始化 Avatar + 生成整场题目 + 播报第一题**。

    调用 ``interview_service.start_session`` —— 它内部已经完成
    「建题目计划 → 落库 → 经 ``_deliver`` 交给数字人通道（``create_session``
    + ``initialize`` + ``speak``）」。本层只是把编排状态推到 ``avatar_speaking``。

    前置状态：``waiting_question``（重复调用会因状态机报错，而不是打到服务层）。
    """
    with _rollback_state_on_error(session_id):
        _advance(session_id, EVENT_QUESTION_READY)
        result = await interview_service.start_session(db, user_id, session_id)
    return {**result, "state": get_state(session_id)}


def speech_done(session_id: int) -> str:
    """播报结束回调：``avatar_speaking`` → ``waiting_answer``。

    真实链路里由数字人前端在播放完成后调用；Mock 环境下由调用方显式调用。
    同步函数——它只推状态，不碰数据库。
    """
    return apply_event(session_id, EVENT_SPEECH_DONE)


async def generate_question(
    db: Any,
    user_id: int,
    session_id: int,
    *,
    spark: Any = None,
    retriever: Any = None,
    use_rag: bool = False,
    retriever_kwargs: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """**按需出题**（大模型通道）：复用 ``interview_service.generate_next_question``。

    与 ``start`` 的规则出题路径互不影响——本入口需要调用方显式触发，
    **不会自动接管**现有流程。

    - ``ok=True``  → 题目已就绪：``waiting_question`` → ``avatar_speaking``；
    - ``ok=False`` → 生成失败（无 LLM / 校验不过 / 会话已结束等）：**状态不变**
      （调用方通常正处于 ``waiting_question``，即「停在等待题目」），
      错误原样回传（不抛错，便于前端按字段分支）。

    刻意**不**在失败时把状态写回 ``waiting_question``：那样会把「会话已结束」
    （``finished``）这类终态**降级**，让编排状态与库里的真实状态脱节。

    前置状态：``waiting_question``。
    """
    with _rollback_state_on_error(session_id):
        result = await interview_service.generate_next_question(
            db,
            user_id,
            session_id,
            spark=spark,
            retriever=retriever,
            use_rag=use_rag,
            retriever_kwargs=retriever_kwargs,
        )
        if result.get("ok"):
            _advance(session_id, EVENT_QUESTION_READY)
    return {**result, "state": get_state(session_id)}


async def submit_answer(
    db: Any, user_id: int, session_id: int, payload: Any
) -> Dict[str, Any]:
    """提交答案：**评分 + 推进到下一题**。

    复用 ``interview_service.submit_answer``（它内部用 ``interview_core.score_answer``
    评分并落库、推进题号、经 ``_deliver`` 播报下一题）。本层负责把状态按
    「瞬时态 ``processing_answer``」走完::

        waiting_answer --answer_submitted--> processing_answer
        processing_answer --scored-------->  avatar_speaking   （还有下一题）
        processing_answer --all_answered-->  finished          （全部答完）

    服务层拒绝（空答案 / 重复作答 / 状态不符）时**状态回滚**，不会卡在瞬时态。
    """
    with _rollback_state_on_error(session_id):
        _advance(session_id, EVENT_ANSWER_SUBMITTED)
        result = await interview_service.submit_answer(
            db, user_id, session_id, payload
        )
        event = EVENT_ALL_ANSWERED if result.get("all_answered") else EVENT_SCORED
        apply_event(session_id, event)
    return {**result, "state": get_state(session_id)}


async def finish(db: Any, user_id: int, session_id: int) -> Dict[str, Any]:
    """结束面试并出报告：任意状态 → ``finished``（复用 ``interview_service.end_session``）。

    ``end_session`` 自身是幂等的（``finished`` 且有报告时返回既有报告），
    因此本方法重复调用安全，状态机侧 ``finish`` 也是幂等的。
    """
    with _rollback_state_on_error(session_id):
        _advance(session_id, EVENT_FINISH)
        result = await interview_service.end_session(db, user_id, session_id)
    return {**result, "state": get_state(session_id)}


def status(session_id: int) -> Dict[str, Any]:
    """编排视角的状态快照（**不查库**，只看进程内状态表）。

    返回::

        {
          "session_id": int,
          "state": str | None,          # None = 本进程无编排记录
          "tracked": bool,
          "terminal": bool,
          "allowed_events": [str, ...], # 当前状态下合法的事件（前端据此判断下一步）
        }
    """
    state = get_state(session_id)
    return {
        "session_id": session_id,
        "state": state,
        "tracked": state is not None,
        "terminal": bool(state is not None and is_terminal(state)),
        "allowed_events": (
            [e for e in EVENTS if can_transition(state, e)] if state else []
        ),
    }


# ============================================================
# 四·五、数字人闭环：Avatar 预检 / 语音作答
# ============================================================
#: ``answer_from_asr`` 返回值的**恒定键集**（成功与失败**同形状**）。
ASR_RESULT_FIELDS: Tuple[str, ...] = (
    "ok", "source", "text", "state", "error", "asr", "result",
)

#: ``source`` 取值：文本来自讯飞虚拟人 ASR。
SOURCE_ASR = "asr"
#: ``source`` 取值：ASR 不可用，文本来自调用方兜底（前端输入框）。
SOURCE_FALLBACK = "fallback"


def _assert_can(session_id: int, event: str) -> str:
    """**只校验不落位**（纯计算）——用于「先校验顺序，再调外部服务」的场景。

    与 ``_advance`` 共用同一套口径（无记录 ⇒ 从 ``INITIAL_STATE`` 起算），
    但**不写状态表**：真正的落位仍由后续的 ``submit_answer`` 完成。
    这样「顺序不对」会在**调用 ASR / 打到服务层之前**就报错。
    """
    return transition(get_state(session_id, INITIAL_STATE), event)


#: 公开别名（**同一个函数对象**，只做名字绑定）。
#: ``api/interview_ws.py`` 需要在「打 provider / 打服务层之前」验顺序——
#: 跨模块引用带下划线的私有名不合适，故对外暴露一个同义名。
assert_can = _assert_can


async def prepare_avatar(db: Any, user_id: int, session_id: int) -> Dict[str, Any]:
    """【闭环第 ② 步】**Avatar 启动**：初始化 / 预检数字人会话（幂等）。

    复用 ``interview_service.ensure_avatar``（它负责取 session 行、归属校验、
    调 ``avatar_interview.ensure_ready`` → provider ``create_session`` + ``initialize``）。

    本方法**不改编排状态**（``start`` 才推进状态），只在返回值上追加 ``state``。
    ``ok=False`` 表示数字人不可用（``status`` 区分 ``unavailable`` / ``expired`` /
    ``disabled`` / ``failed``）——**不阻断**面试：随后的 ``start`` 仍会正常出题。
    """
    result = await interview_service.ensure_avatar(db, user_id, session_id)
    return {**result, "state": get_state(session_id)}


def _answer_request(text: str) -> Any:
    """构造 ``AnswerSubmitRequest``。

    ★ **延迟导入** ``schemas``：编排层顶层只依赖 ``services``（见模块文档的
    依赖单向约定），因此这里在函数体内 import。
    """
    from schemas.interview import AnswerSubmitRequest  # noqa: PLC0415

    return AnswerSubmitRequest(answer_text=text)


async def answer_from_asr(
    db: Any,
    user_id: int,
    session_id: int,
    *,
    timeout_ms: int = 0,
    fallback_text: str = "",
) -> Dict[str, Any]:
    """【闭环第 ⑤ 步】**语音作答**：讯飞虚拟人 **ASR** → 取文本 → ``submit_answer()``。

    这是「用户回答」那一段的完整实现::

        讯飞虚拟人 ASR  →  interview_service.capture_avatar_answer（取文本）
                        →  interview_service.submit_answer（评分 + 落库 + 交付下一题）

    参数
    ----
    - ``timeout_ms``：ASR 等待时长（``0`` = 交给 provider 决定）。
    - ``fallback_text``：**兜底文本**。仅在 ASR **没取到**文本时使用
      （例如前端让用户改用输入框作答）。ASR 正常时**以语音识别结果为准**。

    返回（恒定键集 ``ASR_RESULT_FIELDS``，成功与失败**同形状**）::

        {
          "ok":     bool,            # 是否成功提交（评分 + 落库）
          "source": "asr"|"fallback"|None,
          "text":   str,             # 实际提交的文本（失败时为空串）
          "state":  str|None,        # 提交后的编排状态（失败时**不变**）
          "error":  str|None,
          "asr":    dict,            # 通道层恒定 8 键信封（含 status / retryable）
          "result": dict|None,       # submit_answer 的原始返回（成功时；未加工）
        }

    异常与红线
    ----------
    - **顺序不对**（当前不是 ``waiting_answer``）：**在调 ASR 之前**就抛
      ``OrchestratorStateError``——不浪费一次外部调用，也不打到服务层。
    - **ASR 失败**（网络 / 空结果 / 会话失效）：``ok=False`` 且 ``state`` 不变，
      **绝不调用** ``submit_answer``（不会写入空答案）。据 ``asr["status"]`` 处置：
      ``empty`` / ``unavailable`` → 可重试；``expired`` → 需**重建数字人会话**。
    - **面试会话不存在 / 非本人**：沿用服务层既有口径抛 ``HTTPException`` 404
      （与其余编排入口一致，**不吞异常**）。
    """
    # ① 顺序守卫：不合法就在**碰外部服务之前**报错
    _assert_can(session_id, EVENT_ANSWER_SUBMITTED)

    # ② ASR：取用户语音回答的文本（讯飞虚拟人内置能力，不新增独立 ASR Provider）
    asr = await interview_service.capture_avatar_answer(
        db, user_id, session_id, timeout_ms=timeout_ms
    )

    asr_text = (asr.get("text") or "").strip() if asr.get("ok") else ""
    fallback = (fallback_text or "").strip()
    if asr_text:
        text, source = asr_text, SOURCE_ASR
    elif fallback:
        text, source = fallback, SOURCE_FALLBACK
    else:
        # ASR 失败且无兜底 ⇒ 不提交（**绝不写入空答案**），状态保持不变
        return {
            "ok": False,
            "source": None,
            "text": "",
            "state": get_state(session_id),
            "error": asr.get("error") or "ASR 未取到回答",
            "asr": asr,
            "result": None,
        }

    # ③ 复用既有提交入口：评分 → 落库 → 经 _deliver 交付下一题（状态一并推进）
    submitted = await submit_answer(db, user_id, session_id, _answer_request(text))
    return {
        "ok": True,
        "source": source,
        "text": text,
        "state": submitted.get("state"),
        "error": None,
        "asr": asr,
        "result": submitted,
    }


# ============================================================
# 五、评分（**直通** interview_core，编排层不重造评分逻辑）
# ============================================================
#: 与 ``interview_core.score_answer`` 是**同一个对象**（只做名字绑定，不复制实现）。
score_answer = interview_core.score_answer


def preview_score(question: Dict[str, Any], answer_text: str) -> Dict[str, Any]:
    """**不落库的试评分**——直接复用 ``interview_core.score_answer``。

    数字人场景下可先算分再决定「是否落库 / 是否追问」；正式提交仍走
    ``submit_answer``（那条路径由 ``interview_service`` 负责落库）。

    入参 ``question`` 用 ``interview_service`` 序列化后的题目 dict
    （含 ``question`` / ``expected_points`` / ``question_type`` 等）。
    """
    return score_answer(question, answer_text)
