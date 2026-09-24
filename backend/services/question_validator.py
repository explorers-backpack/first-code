# -*- coding: utf-8 -*-
"""AI 模拟面试 · QuestionValidator（问题校验）

当前阶段只实现**基础字段校验**：``question`` 与 ``topic`` 的存在性与非空性。
其余字段与规则一律未实现（见文末「后续实现计划」）。

设计约束（四条硬约束，均已在实现中满足）
----------------------------------------
1. **不依赖 FastAPI**（含 ``Request``）—— 可脱离 HTTP 层复用
2. **不依赖数据库 Session** —— 纯内存校验，不读库不写库
3. **不依赖 Spark** —— 不产生任何网络调用
4. **可被单元测试直接调用** —— 只需传入普通 dict / 普通对象即可
   （因此本模块**不 import** fastapi / sqlalchemy / main / models）

异常规范
--------
复用项目既有约定（见 ``prompts/loader.py``）：领域基类继承 ``Exception``，
具体异常再额外继承最贴近的内建异常，便于调用方按语义捕获。

**「校验不通过」不是异常**：它是数据，体现在 ``ValidationResult.valid=False``
与 ``errors`` 上。异常只用于「校验流程本身无法进行」。

错误码契约（重要）
------------------
``errors`` 中放的是**稳定的机器可读错误码**（ASCII 蛇形，如 ``question_empty``），
**不是**给人看的散文。理由：错误码可被前端做 i18n 映射、可被测试精确断言、
不会因为文案微调而破坏调用方逻辑。

已定义的错误码：

===========================  ==================================================
错误码                        含义
===========================  ==================================================
``question_empty``           ``question`` 缺失，或去空白后为空，或不是字符串
``topic_empty``              ``topic`` 缺失，或去空白后为空，或不是字符串
===========================  ==================================================

与 ``interview_agent.validate_question`` 的关系
----------------------------------------------
后者是 Interview Agent 内部的**内联规则集**（question 非空 / topic 非空 /
difficulty 合法 / 查重），当前**仍然生效**，本阶段刻意未改动它。
本类是那套规则的**架构化演进**：把校验从 Agent 中抽出，成为可独立演进的组件。
迁移完成前，两者**不得同时作为权威**（否则规则会分叉）。
注意两者的**失败表达方式不同**：Agent 内联规则返回散文式错误说明，
本类返回机器可读错误码。

后续实现计划
------------
TODO(rule-1) difficulty 合法性校验（对照 ``models.DIFFICULTIES``）
TODO(rule-2) 与 ``context.asked_questions`` 的重复检测
TODO(rule-3) 字段补全（填充 ``ValidationResult.normalized_question``）
TODO(rule-4) Interview Agent 集成（Agent 委托本类，并移除内联规则）
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Dict, List

__all__ = [
    "ERROR_QUESTION_EMPTY",
    "ERROR_TOPIC_EMPTY",
    "ValidationResult",
    "QuestionValidatorError",
    "ValidationError",
    "QuestionValidator",
]


# ============================================================
# 一、错误码
# ============================================================
#: ``question`` 缺失 / 空串 / 非字符串
ERROR_QUESTION_EMPTY = "question_empty"

#: ``topic`` 缺失 / 空串 / 非字符串
ERROR_TOPIC_EMPTY = "topic_empty"


# ============================================================
# 二、异常（复用 prompts/loader.py 的异常规范）
# ============================================================
class QuestionValidatorError(Exception):
    """QuestionValidator 相关错误的基类。

    需要捕获「本模块抛出的任何异常」时，捕获本类即可——
    这样后续新增具体异常时调用方无需改动。
    """


class ValidationError(QuestionValidatorError, ValueError):
    """**校验流程本身无法进行**时抛出。

    与「问题不合法」严格区分：

    - **问题不合法** → 不是异常。返回 ``ValidationResult(valid=False,
      errors=[...])``。校验结果是**数据**，不应拿异常做控制流。
    - **校验流程无法进行** → 抛本异常。典型场景：规则内部出现无法继续的状态。

    .. note::

        本类与 ``pydantic.ValidationError`` **无关**（项目测试中已使用后者）。
        需要消歧时，请用基类名 ``QuestionValidatorError`` 捕获，或显式
        ``from services.question_validator import ValidationError`` 导入。

    .. note::

        当前 ``validate()`` 对任何输入都返回结果、不抛异常（保持**全函数**性质，
        便于在重试循环中直接调用），因此本类**尚未被抛出**；
        其触发语义在此固定下来，供后续规则与调用方使用。
    """


# ============================================================
# 三、结果结构（独立定义）
# ============================================================
@dataclass
class ValidationResult:
    """校验结果。

    **独立定义**：不继承 FastAPI / Pydantic / ORM 的任何类型，可自由构造、
    可被单元测试直接断言、可 ``to_dict()`` 后直接作为 API 响应体。

    字段
    ----
    - ``valid``：是否通过。**不变量**：``errors`` 非空时必须为 ``False``，
      请通过 :meth:`add_error` 维护（它会自动置位）。
    - ``errors``：阻断性问题，元素为**稳定错误码**（见模块文档的错误码契约），
      非空即 ``valid=False``。
    - ``warnings``：非阻断提示，不影响 ``valid``（例如「难度与计划不一致，
      但仍在允许范围内」）。当前尚无规则产生提示。
    - ``normalized_question``：补全 / 归一后的问题数据；**尚未实现字段补全**，
      因此当前恒为空 dict。
    """

    valid: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    normalized_question: Dict[str, Any] = field(default_factory=dict)

    def add_error(self, code: str) -> None:
        """登记一条错误码，并**自动置 ``valid=False``**。

        后续规则请一律通过本方法登记错误，避免出现
        「``errors`` 非空但 ``valid`` 仍为 ``True``」的静默不一致。

        :raises ValueError: ``code`` 为空或仅空白（属调用方编程错误，
            用内建异常而非 :class:`ValidationError`——后者语义是校验流程无法进行）。
        """
        text = str(code).strip()
        if not text:
            raise ValueError("错误码不能为空")
        self.errors.append(text)
        self.valid = False

    def add_warning(self, message: str) -> None:
        """登记一条非阻断提示，**不影响 ``valid``**。"""
        text = str(message).strip()
        if not text:
            raise ValueError("提示说明不能为空")
        self.warnings.append(text)

    def to_dict(self) -> Dict[str, Any]:
        """转成 JSON 可序列化的普通 dict（返回副本，不暴露内部引用）。"""
        return {
            "valid": self.valid,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "normalized_question": dict(self.normalized_question),
        }


# ============================================================
# 四、字段判定工具
# ============================================================
def _is_present_text(value: Any) -> bool:
    """字段是否为「存在的非空文本」。

    只有**非空字符串**才算通过：

    - ``None`` / 键缺失 → 不通过
    - ``""`` / 纯空白（如 ``"   "``）→ 不通过（去空白后判空）
    - 非字符串（数字、列表、字典、布尔等）→ 不通过
      （问题文本与知识点必须是文字，其它类型无法作为面试题使用）

    刻意**不**在这里做 strip 后的写回——字段归一属于 rule-3（字段补全），
    本阶段只判断、不修改。
    """
    return isinstance(value, str) and bool(value.strip())


# ============================================================
# 五、校验器
# ============================================================
class QuestionValidator:
    """面试问题校验器。

    当前已实现：``question`` 与 ``topic`` 的基础字段校验。
    未实现：difficulty 校验、重复检测、字段补全、Agent 集成。

    无构造参数：后续规则的阈值（查重相似度、难度白名单等）按项目既有做法
    以模块级常量配置（参见 ``interview_agent.DUPLICATE_THRESHOLD``），
    避免把配置散进实例状态。

    典型用法::

        result = QuestionValidator().validate(question_data, context, plan)
        if not result.valid:
            ...  # 读 result.errors 中的错误码决定是重试还是上报
    """

    def validate(
        self,
        question_data: Mapping[str, Any],
        context: Any = None,
        plan: Any = None,
    ) -> ValidationResult:
        """校验一道待入库的问题。

        参数
        ----
        - ``question_data``：Interview Agent 产出的结构化问题
          （``interview_agent.generate_question`` 的返回值，含 ``question`` /
          ``question_type`` / ``topic`` / ``difficulty`` / ``expected_points`` /
          ``reason``）。**非 Mapping 的入参按「字段全部缺失」处理**，
          因此同样会得到 ``question_empty`` + ``topic_empty``，不会抛异常。
        - ``context``：InterviewContext（``interview_context.get_context`` 的
          dict，或 ORM 对象）。**当前规则尚不使用**，为后续重复检测预留。
        - ``plan``：InterviewPlan（``interview_planner.build_plan_for`` 的 dict，
          或 ORM 对象）。**当前规则尚不使用**，为后续难度 / 知识点约束预留。

        返回
        ----
        :class:`ValidationResult`。**校验不通过不抛异常**，
        而是 ``valid=False`` + ``errors`` 中的错误码。

        已实现的校验规则
        ----------------
        1. ``question`` 必须存在且为非空字符串，否则记 ``question_empty``
        2. ``topic`` 必须存在且为非空字符串，否则记 ``topic_empty``

        规则**不做短路**：两个字段都为空时，两条错误码都会登记，
        便于调用方一次性把问题反馈给模型修复。

        暂不校验
        --------
        ``difficulty`` / ``question_type`` / ``expected_points`` / ``reason``
        一律不校验，缺省或取值异常都不会导致 ``valid=False``。

        异常
        ----
        :class:`ValidationError`：保留给「校验流程本身无法进行」的场景。
        当前实现对所有输入都返回结果，不会抛出。

        实现状态
        --------
        ``normalized_question`` 恒为空 dict——字段补全属 rule-3，尚未实现。
        """
        # 非 Mapping 入参：视作「字段全部缺失」，保持 validate 为全函数
        data: Mapping[str, Any] = question_data if isinstance(question_data, Mapping) else {}

        result = ValidationResult()

        # 规则 1：question 必须存在且非空
        if not _is_present_text(data.get("question")):
            result.add_error(ERROR_QUESTION_EMPTY)

        # 规则 2：topic 必须存在且非空
        if not _is_present_text(data.get("topic")):
            result.add_error(ERROR_TOPIC_EMPTY)

        return result
