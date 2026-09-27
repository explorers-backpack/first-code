# -*- coding: utf-8 -*-
"""AI 模拟面试 · 题目生成器抽象层（QuestionGenerator）。

为什么需要这一层
----------------
``start_session`` 目前走 :func:`services.interview_core.build_question_plan`
（确定性规则），而数字人面试需要 :func:`services.interview_core.generate_next_question`
（大模型）。两者**产出形态不同**：

- 规则：**整场批量**——一次生成 N 道题，同输入同输出
- Agent：**单题**——每次一道，依赖模型输出

若直接在 ``start_session`` 里写 if/else 选路径，会把「用哪种生成方式」这个**策略**
散落进流程代码。本层把策略收敛为**可替换的生成器对象**，
让两种模式**共存、互不影响、可分别测试**。

分层位置
--------
::

    api → interview_service → interview_core（build_question_plan / generate_next_question）
                                      ▲
                                      │ 回边：Agent 策略复用 Core 的流程原语
        question_generator ───────────┘
          ├── RuleQuestionGenerator   → interview_core.build_question_plan
          └── AgentQuestionGenerator  → interview_core.generate_next_question
                                            └── KnowledgeRetriever（仅 agent 路径）

.. note::
   ``AgentQuestionGenerator`` 调用 Core 的 ``generate_next_question``，因此
   **Core 不得反向 import 本模块**（否则成环）。依赖方向是
   「上层策略 → Core 原语」，Core 只提供原语、不认识策略。

两种模式的区别
--------------
====================  ==============================  ==============================
维度                  ``mode="rule"``（默认）         ``mode="agent"``
====================  ==============================  ==============================
实现                  ``build_question_plan``         ``generate_next_question``
产出粒度              **整场**（N 道一次生成）        **单题**（每次一道）
是否调用大模型        **否**（纯规则）                **是**（Spark + Prompt）
是否调用知识检索      **否**（结构上不可能）          **是**（Core 调 ``retrieve_knowledge``）
是否读库              只读会话 / 岗位 / 简历          读会话，并**幂等创建 Context**
可复现                **是**（同输入同输出）          否（取决于模型输出）
失败模式              会话不存在                      ``session_not_found`` /
                                                      ``session_finished`` /
                                                      ``all_answered`` /
                                                      ``agent_failed`` /
                                                      ``validation_failed``
当前用途              ``start_session`` 默认路径      数字人面试（未来接入）
====================  ==============================  ==============================

**知识检索只走 agent 路径**：``retriever`` 是 ``generate`` 的 keyword-only 可选参数，
只有 :class:`AgentQuestionGenerator` 会把它透传给 Core；:class:`RuleQuestionGenerator`
**接收但绝不使用**（其方法体内没有任何检索调用），且规则出题走
``build_question_plan``、根本不经过 Core 的 ``generate_next_question``——
两道保险叠加，「rule 模式不触发检索」是结构性保证。
**缺省不接真实知识库**：不注入 ``retriever`` 时 Core 用空实现（恒返回 ``[]``），
出题行为与引入本能力之前完全一致。

统一契约
--------
两种模式返回**同一形状**的结果信封（:data:`GENERATED_SET_FIELDS`）::

    {
      "mode": "rule" | "agent",
      "ok": bool,
      "questions": [ {question_no, question, question_type,
                      topic, difficulty, expected_points}, ... ],
      "question": {...} | None,   # 便捷访问：本次的「当前题」
      "count": int,
      "reason": str,              # agent：模型给出的出题理由（rule 恒为 ""）
      "stage": str,               # agent：当前面试阶段（rule 恒为 ""）
      "errors": [...],            # 稳定错误码
      "error": str | None,
    }

``questions`` 中每一项**恒为 6 个字段**（与 ``build_question_plan`` 的产出一致），
因此 rule 模式的结果可与 ``build_question_plan`` 的输出**逐字段直接比对**。

接口
----
``await generator.generate(db, session_id, context=None, plan=None, *, spark=None)``

本项目约定 ``db`` 为第一参数（依赖注入）；业务参数顺序与需求一致：
``session_id`` → ``context`` → ``plan``。

不做
----
- **不改动** ``start_session`` 的默认行为（仍走规则出题）
- **不删除** ``build_question_plan``
- 不修改数据库结构
- 不接入数字人 / ASR / TTS / RAG
- ``mode`` 必须由调用方显式指定，默认 ``"rule"``
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Mapping, Optional, Tuple

from services import interview_core
from services.resume_scoring import extract_skills

__all__ = [
    "GENERATOR_MODES",
    "DEFAULT_MODE",
    "GENERATED_SET_FIELDS",
    "QUESTION_ITEM_FIELDS",
    "ERROR_UNKNOWN_MODE",
    "QuestionGeneratorError",
    "UnknownModeError",
    "QuestionGenerator",
    "RuleQuestionGenerator",
    "AgentQuestionGenerator",
    "get_generator",
    "generate_questions",
]

# 支持的生成模式（顺序即文档展示顺序，第一个为默认）
GENERATOR_MODES: Tuple[str, ...] = ("rule", "agent")
DEFAULT_MODE = "rule"

ERROR_UNKNOWN_MODE = "unknown_mode"

# 结果信封字段（**恒定**：成功与失败同形状，调用方无需分支取值）
GENERATED_SET_FIELDS: Tuple[str, ...] = (
    "mode",
    "ok",
    "questions",
    "question",
    "count",
    "reason",
    "stage",
    "errors",
    "error",
)

# 单题字段（与 ``interview_core.build_question_plan`` 的产出**完全一致**）
QUESTION_ITEM_FIELDS: Tuple[str, ...] = (
    "question_no",
    "question",
    "question_type",
    "topic",
    "difficulty",
    "expected_points",
)


# ============================================================
# 一、异常（沿用项目约定：领域基类 + 贴近的内建异常）
# ============================================================
class QuestionGeneratorError(Exception):
    """题目生成器领域基类。"""


class UnknownModeError(QuestionGeneratorError, ValueError):
    """``mode`` 取值非法。属于**编程错误**（不是数据错误），故直接抛。"""


# ============================================================
# 二、内部工具
# ============================================================
def _result(mode: str, **overrides: Any) -> Dict[str, Any]:
    """构造恒定字段集的结果信封。"""
    base: Dict[str, Any] = {
        "mode": mode,
        "ok": False,
        "questions": [],
        "question": None,
        "count": 0,
        "reason": "",
        "stage": "",
        "errors": [],
        "error": None,
    }
    base.update(overrides)
    return base


def _read_field(source: Any, name: str) -> Any:
    """从 dict / ORM 对象读字段（Context 两种形态都支持）。"""
    if source is None:
        return None
    if isinstance(source, Mapping):
        return source.get(name)
    return getattr(source, name, None)


def _current_question(
    questions: List[Dict[str, Any]], context: Any
) -> Optional[Dict[str, Any]]:
    """取「当前题」：按 ``context.current_question_no`` 匹配，取不到则退回第一道。

    规则模式一次产出整场题目，本函数让调用方可以直接拿到「现在该问的那一道」，
    与 Agent 模式的单题返回保持一致的使用体验。
    """
    if not questions:
        return None
    number = _read_field(context, "current_question_no")
    if isinstance(number, int) and number > 0:
        for item in questions:
            if item.get("question_no") == number:
                return item
    return questions[0]


def _project_question(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """把 Core 的单题结果投影成**统一 6 字段**的题目项。"""
    item = {key: payload.get(key) for key in QUESTION_ITEM_FIELDS}
    points = item.get("expected_points")
    item["expected_points"] = list(points) if isinstance(points, list) else []
    return item


# ============================================================
# 三、抽象接口
# ============================================================
class QuestionGenerator(ABC):
    """题目生成器抽象接口。

    子类只需实现 :meth:`generate`；``mode`` 是类属性，标识策略身份。
    """

    mode: str = ""

    @abstractmethod
    async def generate(
        self,
        db: Any,
        session_id: int,
        context: Any = None,
        plan: Any = None,
        *,
        retriever: Any = None,
        use_rag: bool = False,
        retriever_kwargs: Optional[Mapping[str, Any]] = None,
        spark: Any = None,
    ) -> Dict[str, Any]:
        """生成题目，返回 :data:`GENERATED_SET_FIELDS` 形状的结果信封。

        - ``db``：数据库会话（依赖注入；本层不建连接）
        - ``session_id``：面试会话 id
        - ``context`` / ``plan``：可选注入，便于脱离 DB 单测
        - ``retriever``：可选注入的知识检索器（**仅 agent 模式使用**；
          rule 模式接收但**绝不使用**，见 :class:`RuleQuestionGenerator`）
        - ``use_rag``：可选（默认 ``False``）。**仅 agent 模式使用**：
          置 ``True`` 时由 Core 组装真实 RAG 链路（Embedding + 向量库 + 检索器）。
          rule 模式接收但**绝不使用**。
        - ``retriever_kwargs``：可选（默认 ``None``）。**仅 agent 模式使用**：
          ``use_rag=True`` 时原样透传给 Core → ``build_vector_retriever``
          （``top_k`` / ``min_score`` 等），使检索参数真正生效。rule 模式接收但
          **绝不使用**。默认 ``None`` ⇒ 行为逐字节不变。
        - ``spark``：可选注入的 LLM 客户端（仅 agent 模式使用）
        """
        raise NotImplementedError

    def __repr__(self) -> str:  # pragma: no cover - 仅调试用
        return f"<{type(self).__name__} mode={self.mode!r}>"


# ============================================================
# 四、规则生成器（封装现有 build_question_plan，行为不变）
# ============================================================
class RuleQuestionGenerator(QuestionGenerator):
    """**规则生成器**——封装 :func:`interview_core.build_question_plan`。

    特性：整场批量、纯规则、确定性、**不调用大模型**。
    ``start_session`` 的现有行为即由本策略承载，因此结果必须与重构前**逐字段一致**。
    """

    mode = "rule"

    async def generate(
        self,
        db: Any,
        session_id: int,
        context: Any = None,
        plan: Any = None,
        *,
        retriever: Any = None,
        use_rag: bool = False,
        retriever_kwargs: Optional[Mapping[str, Any]] = None,
        spark: Any = None,
    ) -> Dict[str, Any]:
        """按会话配置生成**整场**题目（确定性规则）。

        ``context`` / ``plan`` / ``retriever`` / ``use_rag`` / ``retriever_kwargs``
        / ``spark`` 在本模式下**都不参与生成**——规则出题只需要
        「会话 + 岗位 + 简历技能」。
        刻意保留这些参数是为了接口统一；不读取它们（而非静默忽略某个已注入的依赖）
        是明确的行为声明。

        其中 ``retriever`` / ``use_rag`` / ``retriever_kwargs`` 是**关键**：
        规则模式**完全不调用知识检索**——既不调 ``retrieve_knowledge`` 接缝、
        也不碰 ``retriever.retrieve``、更不会组装真实 RAG 链路（本方法只调
        ``load_*`` 与 ``build_question_plan``）。于是「rule 不触发 RAG」是
        **结构性保证**：即使调用方误传 ``use_rag=True``、
        ``retriever_kwargs={"min_score": 0.9}`` 或注入一个会爆炸的检索器，
        规则出题也不受影响。
        """
        session = await interview_core.load_session_row(db, session_id)
        if session is None:
            return _result(
                self.mode,
                errors=[interview_core.ERROR_SESSION_NOT_FOUND],
                error=f"面试会话 {session_id} 不存在",
            )

        job = await interview_core.load_job_row(db, session.job_id)
        resume = await interview_core.load_resume_row(db, session.resume_id)
        resume_skills = (
            extract_skills(resume.content) if resume is not None and resume.content else []
        )

        questions = [
            dict(item)
            for item in interview_core.build_question_plan(session, job, resume_skills)
        ]
        return _result(
            self.mode,
            ok=True,
            questions=questions,
            question=_current_question(questions, context),
            count=len(questions),
        )


# ============================================================
# 五、Agent 生成器（调用 Core 的 generate_next_question）
# ============================================================
class AgentQuestionGenerator(QuestionGenerator):
    """**Agent 生成器**——封装 :func:`interview_core.generate_next_question`。

    特性：单题、依赖大模型、读库并幂等创建 Context。
    本层**不重复**任何流程与校验逻辑，全部委托 Core（Core 再委托 Agent / Validator）。
    """

    mode = "agent"

    async def generate(
        self,
        db: Any,
        session_id: int,
        context: Any = None,
        plan: Any = None,
        *,
        retriever: Any = None,
        use_rag: bool = False,
        retriever_kwargs: Optional[Mapping[str, Any]] = None,
        spark: Any = None,
    ) -> Dict[str, Any]:
        """生成**一道**题目（Core 的完整流程：session→context→plan→Retriever→Agent→Validator）。

        ``retriever`` / ``use_rag`` / ``retriever_kwargs`` 都原样透传给 Core：
        **注入的 ``retriever`` 优先；只给 ``use_rag=True`` 则由 Core 组装真实 RAG
        链路（``retriever_kwargs`` 在此分支生效，把 ``top_k`` / ``min_score``
        真正接上线）；都不给则用空实现**（无知识）。
        本层**不自己调用** ``retrieve``、也**不自己组装** RAG——
        「检索什么 topic、何时检索、用哪个 Embedding/向量库、用什么阈值」都是流程与组装，
        归 Core / ``knowledge_rag``；本层只负责「把 agent 模式该有的依赖接上」。
        """
        core_result = await interview_core.generate_next_question(
            db,
            session_id,
            spark=spark,
            context=context,
            plan=plan,
            retriever=retriever,
            use_rag=use_rag,
            retriever_kwargs=retriever_kwargs,
        )

        if not core_result.get("ok"):
            return _result(
                self.mode,
                errors=list(core_result.get("errors") or []),
                error=core_result.get("error"),
                stage=core_result.get("stage") or "",
            )

        item = _project_question(core_result)
        return _result(
            self.mode,
            ok=True,
            questions=[item],
            question=item,
            count=1,
            reason=core_result.get("reason") or "",
            stage=core_result.get("stage") or "",
        )


# ============================================================
# 六、工厂与便捷入口
# ============================================================
# 生成器**无状态**，可安全共享单例。
_GENERATORS: Dict[str, QuestionGenerator] = {
    "rule": RuleQuestionGenerator(),
    "agent": AgentQuestionGenerator(),
}


def get_generator(mode: str = DEFAULT_MODE) -> QuestionGenerator:
    """按 ``mode`` 取生成器。

    ``mode`` 非法时抛 :class:`UnknownModeError`——这是**编程错误**
    （调用方写错了字符串），不是运行期数据问题，因此直接失败而不是返回 ``ok=False``。
    """
    try:
        return _GENERATORS[mode]
    except KeyError:
        raise UnknownModeError(
            f"未知的生成模式：{mode!r}，允许：{'/'.join(GENERATOR_MODES)}"
        ) from None


async def generate_questions(
    db: Any,
    session_id: int,
    mode: str = DEFAULT_MODE,
    context: Any = None,
    plan: Any = None,
    *,
    retriever: Any = None,
    use_rag: bool = False,
    retriever_kwargs: Optional[Mapping[str, Any]] = None,
    spark: Any = None,
) -> Dict[str, Any]:
    """便捷入口：按 ``mode`` 选择生成器并生成题目。

    等价于
    ``await get_generator(mode).generate(db, session_id, context, plan,
    retriever=retriever, use_rag=use_rag, retriever_kwargs=retriever_kwargs,
    spark=spark)``。

    ``retriever`` / ``use_rag`` / ``retriever_kwargs`` 只在 ``mode="agent"`` 时起作用；
    ``mode="rule"`` 会**接收并忽略**它们——规则模式不调用任何检索，
    也不会组装真实 RAG 链路（「rule 不触发 RAG」是结构性保证，见
    :class:`RuleQuestionGenerator`）。
    """
    return await get_generator(mode).generate(
        db, session_id, context, plan,
        retriever=retriever, use_rag=use_rag,
        retriever_kwargs=retriever_kwargs, spark=spark,
    )
