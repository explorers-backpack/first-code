# -*- coding: utf-8 -*-
"""AI 模拟面试 · QuestionValidator（问题校验 + 字段补全标准化）

当前阶段实现四件事：

1. **基础字段校验**：``question`` 与 ``topic`` 的存在性与非空性
2. **字段补全与标准化**：把入参补全成固定的六字段结构，
   存入 ``ValidationResult.normalized_question``
3. **difficulty 合法性校验**：取值必须属于 :data:`ALLOWED_DIFFICULTIES`
4. **问题重复检测**（本阶段新增）：对照 ``context.asked_questions``
   拒绝完全相同、对高度相似给出 warning

未实现：Agent 集成（见文末「后续实现计划」）。

设计约束（四条硬约束，均已在实现中满足）
----------------------------------------
1. **不依赖 FastAPI**（含 ``Request``）—— 可脱离 HTTP 层复用
2. **不依赖数据库 Session** —— 纯内存校验，不读库不写库
3. **不依赖 Spark** —— 不产生任何网络调用
4. **可被单元测试直接调用** —— 只需传入普通 dict / 普通对象即可
   （因此本模块**不 import** fastapi / sqlalchemy / main / models）

**纯函数设计**：``validate()`` 不修改入参、不持有跨调用状态、同输入同输出。
返回的 ``normalized_question`` 及其中的列表都是**新建对象**，
调用方改它不会影响入参，也不会影响下一次调用的结果。

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

===========================  ==================================================
错误码                        含义
===========================  ==================================================
``question_empty``           ``question`` 缺失，或去空白后为空，或不是字符串
``topic_empty``              ``topic`` 缺失，或去空白后为空，或不是字符串
``invalid_difficulty``       ``difficulty`` 解析后有值，但不属于
                             :data:`ALLOWED_DIFFICULTIES`
``duplicate_question``       ``question`` 与 ``context.asked_questions``
                             中的某条**完全相同**（归一化后逐字相同）
===========================  ==================================================

difficulty 处理流程
-------------------
difficulty 规则由**两个独立纯函数**实现，不与其他字段的校验逻辑纠缠：

1. :func:`resolve_difficulty` —— **补全**（不校验）：取 ``question_data.difficulty``；
   缺失 / 为空 / 非字符串时回落到 ``plan.difficulty``；再无则 ``""``。
2. :func:`is_valid_difficulty` —— **合法性**：取值是否属于
   :data:`ALLOWED_DIFFICULTIES`（``junior`` / ``mid`` / ``senior``）。

``validate()`` 只负责把两者串起来：**先补全，再校验**。因此：

- **情况1**：``question_data`` 无 ``difficulty`` → 用 ``plan.difficulty`` 补全；
  补全结果合法则 ``valid=True``，并写入 ``normalized_question``。
- **情况2**：``question_data`` 有 ``difficulty`` 且合法 → 通过（原样保留）。
- **情况3**：``difficulty`` 非法（如 ``god_mode``）→ ``valid=False``，
  ``errors=["invalid_difficulty"]``。

两条**刻意**的边界判定（用户规格未覆盖，此处显式固定，便于日后调整）：

- **补全后仍为空（既无 ``question_data`` 值也无 ``plan`` 值）→ 不算非法**。
  ``""`` 表示「未知」，与「取值非法」是两件事；阶段二已把 ``""`` 定为
  ``difficulty`` 的合法缺省值，若在此判错会自相矛盾。此时
  ``valid`` 只取决于其它规则。
- **枚举比较为精确相等，不做 trim 后比较**：``" senior "`` 判为非法。
  依据是本模块「不做文本改写」原则；且实际链路上
  ``prompts.parse_question_output`` 已用 ``_clean_str`` 去空白，
  模型输出的首尾空白在进入本模块前已被消除。

词汇表统一（已收口）
--------------------
``difficulty`` 的合法取值与**全项目标准一致**：``junior`` / ``mid`` / ``senior``。
已逐处核对，全链路同源同值：

- ``models.DIFFICULTIES``（唯一真源）
- ``schemas.interview.DifficultyLevel``（``Literal``）
- ``interview_service``（建会话时校验入参）
- ``interview_planner``（``_MINUTES_PER_QUESTION`` / ``_DIFFICULTY_LABELS`` / 默认值）
- ``interview_agent``（``DEFAULT_DIFFICULTY = "mid"``）
- ``prompts/interview/question.txt`` / ``question_repair.txt``（对模型的取值约束）

.. note::

   本模块**仍不 import** ``models``——这是「零第三方依赖」硬约束决定的
   （``models`` 会拉入 SQLAlchemy）。因此该元组在此**字面重复声明**，
   并由 ``tests/test_question_validator.py`` 用**子进程**断言
   ``ALLOWED_DIFFICULTIES == models.DIFFICULTIES``，防止日后漂移。

   **不设任何转换层**：不存在别名映射，``beginner`` / ``intermediate`` /
   ``advanced`` 一律判为非法，与全项目保持一致。

重复检测策略（本阶段新增）
--------------------------
输入是 ``context.asked_questions``——字符串列表（例如
``["介绍Spring Boot自动配置", "Redis为什么快"]``）；也兼容
``{"question_no": 3, "question": "..."}`` 这类对象形态，
与 ``interview_context.add_asked_question`` 接受的两种入参保持一致。
检测对象是 ``question_data.question``。规则由 :func:`find_asked_question_match`
实现（纯函数）：

1. **完全相同**（归一化后逐字相同）→ ``valid=False``，
   ``errors=["duplicate_question"]``——**直接拒绝**。
2. **高度相似**（相似度 ≥ :data:`SIMILAR_QUESTION_THRESHOLD`）→
   **只记 warning，不拒绝**。
3. 其余 → 不产生任何输出。

**归一化**（:func:`normalize_question_text`）：去空白（含全角空格）、
去中英文标点、转小写。因此 ``"Redis 为什么快？"`` 与 ``"redis为什么快"``
视为**完全相同**。

**相似度**（:func:`question_similarity`）：归一化后的字符二元组 Dice 系数，
另加**包含关系兜底**（较短一方 ≥ :data:`CONTAINMENT_MIN_LEN` 字且一方完整包含
另一方 → 1.0）。理由与 ``interview_agent.question_similarity`` 相同：中文无词边界，
二元组既能容忍虚词增删、又对改写敏感；纯规则、确定性、可复现。
**包含关系判为「高度相似」而非「完全相同」**，因此落在 warning 档，不会被拒绝。

.. note::

   **与 ``interview_agent.question_similarity`` 暂时重复**：本模块受
   「零第三方依赖 + 不 import 业务模块」约束（Agent 依赖本模块，
   反向 import 会成环），故该算法在此**独立实现**。两者目前逻辑一致，
   待 rule-4 集成时统一到一处。

**刻意不使用** embedding / 向量数据库 / RAG——纯字符规则足以满足
「防止 AI 连续生成高度重复的问题」，且确定性可复现、可申诉。

标准化契约
----------
校验通过（``valid is True``）时，``normalized_question`` 恒为**恰好六个键**的 dict，
顺序固定为 :data:`NORMALIZED_FIELDS`::

    {
        "question":        "",      # 原文，一字不改
        "question_type":   "",      # 缺失 → ""
        "topic":           "",      # 原文，一字不改
        "difficulty":      "",      # 补全后的值（见上「difficulty 处理流程」）
        "expected_points": [],      # 缺失 → []
        "reason":          "",      # 缺失 → ""
    }

补全规则：

1. ``difficulty``：见上「difficulty 处理流程」——先补全，再校验合法性。
2. ``expected_points``：缺失 → ``[]``。
3. ``reason``：缺失 → ``""``。
4. ``question_type``：缺失 → ``""``。**不做枚举校验**。

**刻意不做文本改写**：``question`` / ``topic`` / ``difficulty`` 的取值一字不改
（不做 ``strip`` 写回），避免悄悄改变语义。空白仅在**判空**时被忽略。

**``None`` 与键缺失同等对待**（都视为「未提供」），属正常补全路径。

warning 规则
------------
``warnings`` 是**非阻断提示**，不影响 ``valid``。共有两个来源：

1. **补全阶段**：字段**存在且非 ``None``**，但取值无法按上述契约使用
   （因此被丢弃或替换）。键缺失与 ``None`` 属正常补全，**不产生 warning**。
   当前情形：``difficulty`` 存在但为空 / 非字符串；``expected_points``
   存在但不是数组，或数组内含非字符串元素；``question_type`` / ``reason``
   存在但不是字符串。
2. **校验阶段**：问题与历史问题**高度相似**（但非完全相同）。

注意 ``difficulty`` 的「非法取值」（如 ``god_mode``）与「与历史问题完全相同」
记的都是 **error** 而非 warning——它们属于阻断性问题。

``normalized_question`` 与 ``valid`` 的关系
------------------------------------------
**校验不通过时 ``normalized_question`` 恒为 ``{}``**——半补全的数据没有意义，
也不该被当作权威使用。调用方应先看 ``valid``，再读 ``normalized_question``。

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
TODO(rule-4) Interview Agent 集成（Agent 委托本类，并移除内联规则）
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Dict, List, NamedTuple, Tuple

__all__ = [
    "ERROR_QUESTION_EMPTY",
    "ERROR_TOPIC_EMPTY",
    "ERROR_INVALID_DIFFICULTY",
    "ERROR_DUPLICATE_QUESTION",
    "ALLOWED_DIFFICULTIES",
    "NORMALIZED_FIELDS",
    "SIMILAR_QUESTION_THRESHOLD",
    "CONTAINMENT_MIN_LEN",
    "resolve_difficulty",
    "is_valid_difficulty",
    "normalize_question_text",
    "question_similarity",
    "find_asked_question_match",
    "AskedQuestionMatch",
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

#: ``difficulty`` 解析后有值，但不属于 :data:`ALLOWED_DIFFICULTIES`
ERROR_INVALID_DIFFICULTY = "invalid_difficulty"

#: ``question`` 与历史问题归一化后**完全相同**
ERROR_DUPLICATE_QUESTION = "duplicate_question"


# ============================================================
# 二、difficulty 允许集
# ============================================================
#: ``difficulty`` 的合法取值（闭集，精确相等比较）。
#:
#: 与全项目标准一致：``models.DIFFICULTIES`` / ``schemas.DifficultyLevel`` /
#: ``interview_planner`` / ``interview_agent`` / ``prompts/interview/*.txt``
#: 用的都是这一组取值。
#:
#: .. note::
#:
#:    本模块刻意**不 import** ``models``（零第三方依赖约束，``models`` 会拉入
#:    SQLAlchemy），因此在此**字面重复声明**。
#:    ``tests/test_question_validator.py`` 用子进程断言
#:    ``ALLOWED_DIFFICULTIES == models.DIFFICULTIES`` 防止漂移。
#:    **不设转换层**：旧词汇 ``beginner`` / ``intermediate`` / ``advanced``
#:    一律判为非法。
ALLOWED_DIFFICULTIES: Tuple[str, ...] = ("junior", "mid", "senior")


# ============================================================
# 三、标准化输出契约
# ============================================================
#: ``normalized_question`` 的**固定字段与顺序**。
#: 调用方（后续的 Agent / API 层）可直接依赖它做序列化或字段遍历。
NORMALIZED_FIELDS: Tuple[str, ...] = (
    "question",
    "question_type",
    "topic",
    "difficulty",
    "expected_points",
    "reason",
)

#: 内部哨兵：区分「键不存在」与「键存在但值为 None」
_MISSING = object()


# ============================================================
# 四、重复检测常量
# ============================================================
#: 判为「高度相似」（记 warning）的相似度阈值。
#: 与 ``interview_agent.DUPLICATE_THRESHOLD`` 取值一致，便于日后统一。
SIMILAR_QUESTION_THRESHOLD = 0.85

#: 包含关系兜底生效的最短长度：较短一方达到该长度且一方完整包含另一方时，
#: 直接判相似度为 1.0。设下限是为了避免「是吗」这类短串误命中长问题。
CONTAINMENT_MIN_LEN = 8

#: 归一化时剔除的字符：所有空白（含全角空格）与中英文标点。
_PUNCTUATION = re.compile(
    r"[\s\u3000，。、；：？！,.;:?!\"'“”‘’（）()\[\]【】《》<>—\-_/\\|~`*#]+"
)


# ============================================================
# 五、异常（复用 prompts/loader.py 的异常规范）
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
# 六、结果结构（独立定义）
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
    - ``warnings``：非阻断提示，不影响 ``valid``。两个来源见模块文档的
      warning 规则（字段补全 + 高度相似）。
    - ``normalized_question``：补全 / 标准化后的六字段 dict，键与顺序见
      :data:`NORMALIZED_FIELDS`。**``valid=False`` 时恒为 ``{}``**。
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
# 七、字段判定与读取工具（模块级纯函数）
# ============================================================
def _is_present_text(value: Any) -> bool:
    """字段是否为「存在的非空文本」。

    只有**非空字符串**才算通过：

    - ``None`` / 键缺失 → 不通过
    - ``""`` / 纯空白（如 ``"   "``、全角空格）→ 不通过（去空白后判空）
    - 非字符串（数字、列表、字典、布尔等）→ 不通过
      （问题文本与知识点必须是文字，其它类型无法作为面试题使用）

    刻意**不**在这里做 strip 后的写回——本模块不做文本改写，
    空白只在判空时被忽略。
    """
    return isinstance(value, str) and bool(value.strip())


def _as_text(value: Any) -> str:
    """把任意取值收窄成字符串：非字符串一律视为「无文本」（``""``）。"""
    return value if isinstance(value, str) else ""


def _read_field(source: Any, name: str) -> Any:
    """从 ``dict`` **或**普通对象上读一个字段。

    ``context`` / ``plan`` 既可能是 ``services.interview_planner`` 产出的 dict，
    也可能是 ORM 对象，因此两种取值方式都支持。

    返回值可能是内部哨兵 :data:`_MISSING`（键不存在 / ``source`` 为 ``None``），
    调用方应配合 :func:`_as_text` 或显式判 ``is _MISSING`` 使用。
    """
    if source is None:
        return _MISSING
    if isinstance(source, Mapping):
        return source.get(name, _MISSING)
    return getattr(source, name, _MISSING)


def _read_free_text(data: Mapping[str, Any], name: str, result: ValidationResult) -> str:
    """读取一个「自由文本」字段（``question_type`` / ``reason``）。

    - 字符串（含空串）→ 原样返回
    - 键缺失 / ``None`` → 返回 ``""``（正常补全，不告警）
    - 其它类型 → 返回 ``""`` 并告警（存在但不可用）
    """
    raw = data.get(name, _MISSING)
    if isinstance(raw, str):
        return raw
    if raw is not _MISSING and raw is not None:
        result.add_warning(f"{name} 不是字符串，已置空")
    return ""


def _read_expected_points(data: Mapping[str, Any], result: ValidationResult) -> List[str]:
    """读取 ``expected_points``，保证输出是**新的**字符串列表。

    - 数组（``list`` / ``tuple``）→ 返回新列表，**只保留字符串元素**
    - 键缺失 / ``None`` → ``[]``（正常补全，不告警）
    - 其它类型（字符串 / 数字 / 字典等）→ ``[]`` 并告警

    刻意**不做猜测性转换**：``"A,B"`` 这类字符串**不**按逗号切分——
    切分规则不唯一，猜错会污染知识点集合，不如明确按「未提供」处理。
    """
    raw = data.get("expected_points", _MISSING)
    if isinstance(raw, (list, tuple)):
        points = [item for item in raw if isinstance(item, str)]
        dropped = len(raw) - len(points)
        if dropped:
            result.add_warning(f"expected_points 中有 {dropped} 个非字符串元素，已丢弃")
        return points
    if raw is not _MISSING and raw is not None:
        result.add_warning("expected_points 不是数组，已按未提供处理（补 []）")
    return []


# ============================================================
# 八、difficulty 规则（独立纯函数）
# ============================================================
def resolve_difficulty(question_data: Any, plan: Any = None) -> str:
    """**补全**本道题的 ``difficulty``（只取值，不判断合法性）。

    取值优先级：

    1. ``question_data["difficulty"]``（必须是存在的非空字符串）
    2. ``plan.difficulty``（``plan`` 可为 dict 或 ORM 对象）
    3. ``""``（表示「未知」）

    ``question_data`` 不是 Mapping 时按「无该字段」处理。
    **不修改任何入参，也不产生 warning**（告警由 :meth:`QuestionValidator._normalize`
    负责，避免本函数产生副作用，便于单独测试与复用）。

    合法性判断请用 :func:`is_valid_difficulty`。
    """
    data: Mapping[str, Any] = question_data if isinstance(question_data, Mapping) else {}

    raw = data.get("difficulty", _MISSING)
    if _is_present_text(raw):
        # 原样返回（是否合法交给 is_valid_difficulty 判断，本函数不校验）
        return raw

    fallback = _as_text(_read_field(plan, "difficulty"))
    return fallback if _is_present_text(fallback) else ""


def is_valid_difficulty(
    value: Any, allowed: Any = ALLOWED_DIFFICULTIES
) -> bool:
    """``difficulty`` 取值是否合法（是否属于允许集）。

    **精确相等**比较：``" senior "`` 判为非法（本模块不做 trim 改写；
    实际链路上 ``prompts.parse_question_output`` 已先行去空白）。

    ``None`` / ``""`` / 非字符串一律返回 ``False``——本函数只回答
    「这个**值**合法吗」，**不判断「缺失是否算错」**：
    那是调用方的策略。``validate()`` 的策略是「补全后仍为空不算非法」，
    详见模块文档的「difficulty 处理流程」。
    """
    return _is_present_text(value) and value in tuple(allowed or ())


# ============================================================
# 九、重复检测规则（独立纯函数）
# ============================================================
class AskedQuestionMatch(NamedTuple):
    """一次重复检测的命中结果。

    ``kind`` 的三种取值（用类属性引用，避免调用方硬编码字符串）：

    - :attr:`NONE`：未命中
    - :attr:`EXACT`：归一化后完全相同 → 应**拒绝**
    - :attr:`SIMILAR`：高度相似 → 应**只记 warning**
    """

    #: 命中类型：``"none"`` / ``"exact"`` / ``"similar"``
    kind: str
    #: 命中的历史问题原文（``none`` 时为空串）
    matched: str
    #: 相似度，0.0-1.0（``none`` 时为 0.0）
    score: float

    #: 未命中
    NONE = "none"
    #: 归一化后完全相同
    EXACT = "exact"
    #: 高度相似（≥ 阈值但并非完全相同）
    SIMILAR = "similar"


def normalize_question_text(text: Any) -> str:
    """问题文本归一化：**去空白（含全角空格）、去中英文标点、转小写**。

    目的：让「同一道题的不同书写方式」能够被识别为同一道题。
    例如 ``"Redis 为什么快？"`` 与 ``"redis为什么快"`` 归一化后都是
    ``"redis为什么快"``。

    非字符串一律返回 ``""``（无法作为问题文本参与比较）。
    这是**纯函数**，且幂等（对已归一化的文本再调用结果不变）。
    """
    return _PUNCTUATION.sub("", _as_text(text).lower())


def _bigrams(text: str) -> set:
    """字符二元组集合；空串返回空集，单字符返回其本身。"""
    if not text:
        return set()
    if len(text) < 2:
        return {text}
    return {text[i:i + 2] for i in range(len(text) - 1)}


def question_similarity(left: Any, right: Any) -> float:
    """两道题的相似度，0.0-1.0（**先归一化再比较**）。

    算法与 ``interview_agent.question_similarity`` 一致（本模块独立实现，
    原因见模块文档的 note）：

    1. 归一化后**完全相同** → ``1.0``
    2. 一方**完整包含**另一方，且较短一方 ≥ :data:`CONTAINMENT_MIN_LEN` 字 → ``1.0``
       （「同一问题 + 附加从句」是最常见的重复形态，此时二元组系数会被长度差
       稀释——8 字的「请做一下自我介绍」对 19 字的扩写版只有 0.58，
       单靠阈值会漏判）
    3. 否则取字符二元组 Dice 系数：``2 * |A ∩ B| / (|A| + |B|)``

    任一为空串则返回 ``0.0``。**纯函数、确定性**。
    """
    a, b = normalize_question_text(left), normalize_question_text(right)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if min(len(a), len(b)) >= CONTAINMENT_MIN_LEN and (a in b or b in a):
        return 1.0
    grams_a, grams_b = _bigrams(a), _bigrams(b)
    if not grams_a or not grams_b:
        return 0.0
    return 2 * len(grams_a & grams_b) / (len(grams_a) + len(grams_b))


def _asked_question_texts(asked: Any) -> List[str]:
    """把 ``asked_questions`` 归一成问题文本列表。

    该字段可能同时存放两种形态（``interview_context.add_asked_question``
    两种都接受）：纯字符串，或 ``{"question_no": 3, "question": "..."}``
    这样的对象。非字符串元素与空白项一律跳过。
    """
    if not isinstance(asked, (list, tuple)):
        return []
    out: List[str] = []
    for item in asked:
        if isinstance(item, Mapping):
            text = _as_text(item.get("question")).strip()
        else:
            text = _as_text(item).strip()
        if text:
            out.append(text)
    return out


def find_asked_question_match(
    question: Any,
    asked_questions: Any,
    *,
    threshold: float = SIMILAR_QUESTION_THRESHOLD,
) -> AskedQuestionMatch:
    """在历史问题中找出与 ``question`` 最匹配的一条。

    返回 :class:`AskedQuestionMatch`：

    - ``EXACT``：归一化后完全相同（**最强匹配，命中即返回**，不再计算相似度）
    - ``SIMILAR``：相似度 ≥ ``threshold`` 但不是完全相同
    - ``NONE``：无历史、``question`` 为空、或全部低于阈值

    多条候选命中时取**相似度最高**者；相同分数取**先出现**者，
    保证结果确定（同输入同输出）。

    ``question`` 为空（或非字符串）时直接返回 ``NONE``——
    空问题不属于「重复」，应由 ``question_empty`` 规则处理，避免重复报错。

    **不修改任何入参。**
    """
    target = normalize_question_text(question)
    if not target:
        return AskedQuestionMatch(AskedQuestionMatch.NONE, "", 0.0)

    best = AskedQuestionMatch(AskedQuestionMatch.NONE, "", 0.0)
    for previous in _asked_question_texts(asked_questions):
        if normalize_question_text(previous) == target:
            # 完全相同：最强匹配，无需再算相似度
            return AskedQuestionMatch(AskedQuestionMatch.EXACT, previous, 1.0)
        score = question_similarity(target, previous)
        if score >= threshold and score > best.score:
            best = AskedQuestionMatch(AskedQuestionMatch.SIMILAR, previous, score)
    return best


# ============================================================
# 十、校验器
# ============================================================
class QuestionValidator:
    """面试问题校验器。

    已实现：``question`` / ``topic`` 基础字段校验、六字段补全标准化、
    ``difficulty`` 合法性校验、问题重复检测。
    未实现：Agent 集成。

    无构造参数：规则的阈值（相似度等）按项目既有做法以模块级常量配置
    （:data:`SIMILAR_QUESTION_THRESHOLD` / :data:`ALLOWED_DIFFICULTIES`），
    避免把配置散进实例状态。

    典型用法::

        result = QuestionValidator().validate(question_data, context, plan)
        if not result.valid:
            ...  # 读 result.errors 中的错误码决定是重试还是上报
        else:
            question = result.normalized_question  # 六字段齐全，可直接使用
    """

    def validate(
        self,
        question_data: Mapping[str, Any],
        context: Any = None,
        plan: Any = None,
    ) -> ValidationResult:
        """校验并标准化一道待入库的问题。

        参数
        ----
        - ``question_data``：Interview Agent 产出的结构化问题
          （``interview_agent.generate_question`` 的返回值，含 ``question`` /
          ``question_type`` / ``topic`` / ``difficulty`` / ``expected_points`` /
          ``reason``）。**非 Mapping 的入参按「字段全部缺失」处理**，
          因此同样会得到 ``question_empty`` + ``topic_empty``，不会抛异常。
        - ``context``：InterviewContext（``interview_context.get_context`` 的
          dict，或 ORM 对象）。读取 ``asked_questions`` 用于重复检测；
          其它字段不使用。
        - ``plan``：InterviewPlan（``interview_planner.build_plan_for`` 的 dict，
          或 ORM 对象）。**仅用于补全缺失的 ``difficulty``**（读 ``plan.difficulty``）。

        返回
        ----
        :class:`ValidationResult`。**校验不通过不抛异常**，
        而是 ``valid=False`` + ``errors`` 中的错误码；
        此时 ``normalized_question`` 为 ``{}``。

        已实现的校验规则
        ----------------
        1. ``question`` 必须存在且为非空字符串，否则记 ``question_empty``
        2. ``topic`` 必须存在且为非空字符串，否则记 ``topic_empty``
        3. ``difficulty`` 经 :func:`resolve_difficulty` 补全后若有值，
           必须属于 :data:`ALLOWED_DIFFICULTIES`，否则记 ``invalid_difficulty``
        4. ``question`` 不得与 ``context.asked_questions`` 中的某条
           **完全相同**，否则记 ``duplicate_question``；
           **高度相似**只记 warning，不拒绝

        规则**不做短路**：四条规则都会执行，错误码按上述顺序全部登记，
        便于调用方一次性把问题反馈给模型修复。

        已实现的标准化规则
        ------------------
        校验通过后按 :data:`NORMALIZED_FIELDS` 输出六字段结构，
        补全规则见模块文档。

        暂不校验
        --------
        ``question_type`` / ``expected_points`` / ``reason``
        的**取值合法性**一律不校验，只会被补全或收窄，不会导致 ``valid=False``。

        异常
        ----
        :class:`ValidationError`：保留给「校验流程本身无法进行」的场景。
        当前实现对所有输入都返回结果，不会抛出。
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

        # 规则 3：difficulty 先补全、再校验合法性
        #   补全后仍为空（既无 question_data 值也无 plan 值）→ 视为「未知」，
        #   不记错。理由见模块文档「difficulty 处理流程」。
        difficulty = resolve_difficulty(data, plan)
        if difficulty and not is_valid_difficulty(difficulty):
            result.add_error(ERROR_INVALID_DIFFICULTY)

        # 规则 4：与历史问题重复检测
        #   完全相同 → error（拒绝）；高度相似 → warning（不拒绝）
        match = find_asked_question_match(
            data.get("question"), _read_field(context, "asked_questions")
        )
        if match.kind == AskedQuestionMatch.EXACT:
            result.add_error(ERROR_DUPLICATE_QUESTION)
        elif match.kind == AskedQuestionMatch.SIMILAR:
            result.add_warning(
                f"与历史问题高度相似（相似度 {match.score:.0%}）：{match.matched[:60]}"
            )

        # 只有校验通过才产出标准化结果：半补全的数据不应被当作权威使用
        if result.valid:
            result.normalized_question = self._normalize(data, plan, result)

        return result

    # --------------------------------------------------------
    # 内部：字段补全与标准化
    # --------------------------------------------------------
    def _normalize(
        self,
        data: Mapping[str, Any],
        plan: Any,
        result: ValidationResult,
    ) -> Dict[str, Any]:
        """把已通过校验的入参补全成六字段结构。

        仅在 ``valid is True`` 时被调用，因此 ``question`` / ``topic``
        必定是非空字符串，``difficulty`` 也必定合法（或为空）。
        **不修改 ``data``，不修改 ``plan``。**

        warning 通过 ``result.add_warning`` 登记（不影响 ``valid``）。
        各字段按 :data:`NORMALIZED_FIELDS` 的顺序处理，
        使 warning 的出现顺序稳定可断言。
        """
        # ---- question_type：缺失 → ""（本阶段不做枚举校验） ----
        question_type = _read_free_text(data, "question_type", result)

        # ---- difficulty：缺失 / 为空 / 非字符串 → plan.difficulty → "" ----
        # 与 validate() 走的是同一个纯函数，因此「校验过的值」与
        # 「写进 normalized_question 的值」必然一致。
        difficulty = resolve_difficulty(data, plan)
        raw_difficulty = data.get("difficulty", _MISSING)
        if (
            raw_difficulty is not _MISSING
            and raw_difficulty is not None
            and not _is_present_text(raw_difficulty)
        ):
            # 字段存在但不可用 —— 本阶段的「检测是否为空」结果，非阻断
            result.add_warning(
                "difficulty 存在但为空或非字符串，已用 plan.difficulty 补全"
                if difficulty else
                "difficulty 存在但为空或非字符串，且 plan 未提供可用难度，已置空"
            )

        # ---- expected_points：缺失 → [] ----
        expected_points = _read_expected_points(data, result)

        # ---- reason：缺失 → "" ----
        reason = _read_free_text(data, "reason", result)

        # 固定键与顺序（NORMALIZED_FIELDS）；question / topic 取原文，一字不改
        return {
            "question": data.get("question"),
            "question_type": question_type,
            "topic": data.get("topic"),
            "difficulty": difficulty,
            "expected_points": expected_points,
            "reason": reason,
        }
