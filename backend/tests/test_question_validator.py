# -*- coding: utf-8 -*-
"""AI 模拟面试 · QuestionValidator 自检（基础校验 + 补全 + difficulty + 重复检测）

无需 pytest，直接运行：
    python backend/tests/test_question_validator.py

**本文件刻意不设置 DATABASE_URL**，以此证明被测模块不依赖数据库配置。
被测对象是 ``services.question_validator``（当前实现 question / topic 基础校验、
六字段补全标准化、difficulty 合法性校验、问题重复检测）。

覆盖范围：
1. 依赖约束：零第三方依赖、导入后 fastapi/sqlalchemy 未进入 sys.modules
2. 独立性：在无 DATABASE_URL 的子进程中也能导入（不依赖数据库）
3. ValidationResult：字段、默认值、to_dict、副本语义、不变量
4. ValidationError：继承关系、与 pydantic.ValidationError 的区别
5. QuestionValidator：可无参构造、validate 可调用（1/2/3 个参数）、返回类型
6. **question 校验**：正常 / 空串 / 纯空白 / 缺失 / None / 非字符串
7. **topic 校验**：同上
8. **question 与 topic 同时为空**：两条错误码都要登记（不短路）
9. 取值合法性边界：本阶段仅 difficulty 受校验
10. 错误码契约：常量取值、机器可读性、无重复、valid 不变量
11. 无副作用：不篡改入参、可重复调用、结果互不共享
12. **标准化契约**：六字段结构与顺序、字段齐全时逐字保留
13. **字段补全规则**：difficulty / expected_points / reason / question_type 四条
14. **difficulty 合法性校验**：允许集、情况1/2/3、两个独立纯函数、不影响其他字段
15. **问题重复检测**：归一化、相似度、完全重复拒绝 / 不重复通过 / 相似告警、
    asked_questions 两种形态、独立函数、与其他规则互不干扰
16. **标准化的纯函数性**：不改入参、不共享引用、确定性、失败不产出半补全数据
17. 待实现边界：rule-4 确实尚未实现
18. **词汇表一致性守卫**：子进程断言 ALLOWED_DIFFICULTIES == models.DIFFICULTIES
"""

import ast
import inspect
import json
import os
import pathlib
import re
import subprocess
import sys
from types import SimpleNamespace

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

# 注意：刻意不设置 DATABASE_URL —— 本模块必须与数据库解耦。
os.environ.pop("DATABASE_URL", None)

from services import question_validator as qv  # noqa: E402

_PASSED = 0
_FAILED = 0


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def _raises(fn, exc_type) -> bool:
    try:
        fn()
        return False
    except exc_type:
        return True
    except Exception:  # noqa: BLE001 - 抛了别的异常也算失败
        return False


VALIDATOR = qv.QuestionValidator()

VALID_DATA = {
    "question": "请说明 Redis 持久化中 RDB 与 AOF 的取舍。",
    "question_type": "technical",
    "topic": "Redis 持久化",
    "difficulty": "senior",
    "expected_points": ["RDB 快照", "AOF 追加"],
    "reason": "补强薄弱知识点",
}
SAMPLE_CONTEXT = {
    "session_id": 7,
    "current_stage": "technical",
    "asked_questions": ["请做一下自我介绍"],
    "covered_topics": ["Java"],
    "weak_topics": ["Redis 持久化"],
}
SAMPLE_PLAN = {
    "interview_type": "technical",
    "difficulty": "senior",
    "total_questions": 12,
    "priority_topics": ["Redis"],
}


def run() -> bool:
    print("=" * 70)
    print("AI 模拟面试 · QuestionValidator 基础字段校验自检")
    print("=" * 70)

    # ============================================================
    # [1] 依赖约束
    # ============================================================
    print("\n[1] 依赖约束")
    _check("导入后 fastapi 未进入 sys.modules", "fastapi" not in sys.modules)
    _check("导入后 sqlalchemy 未进入 sys.modules", "sqlalchemy" not in sys.modules)
    _check("导入后 pydantic 未进入 sys.modules", "pydantic" not in sys.modules)
    _check("导入后 main（Spark 宿主）未进入 sys.modules", "main" not in sys.modules)
    _check("导入后 database 未进入 sys.modules", "database" not in sys.modules)
    _check("导入后 interview_agent 未被牵连", "services.interview_agent" not in sys.modules)
    _check("导入后 interview_planner 未被牵连", "services.interview_planner" not in sys.modules)

    source = (BACKEND_DIR / "services" / "question_validator.py").read_text(encoding="utf-8")
    imported = {
        m.group(1).split(".")[0]
        for m in re.finditer(r"^\s*(?:from|import)\s+([A-Za-z_][\w.]*)", source, re.MULTILINE)
    }
    _check("源码只 import 标准库（零第三方依赖）",
           imported <= {"__future__", "collections", "dataclasses", "re", "typing"},
           str(sorted(imported)))
    for banned in ("fastapi", "sqlalchemy", "pydantic", "main", "models", "database"):
        _check(f"源码未 import {banned}", banned not in imported)

    # 用 AST 收集「代码里真正出现的标识符」——避免误伤 docstring 里的说明文字
    tree = ast.parse(source)
    code_identifiers = (
        {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
        | {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
    )
    for banned in ("Request", "HTTPException", "AsyncSession", "Session",
                   "SparkAPI", "spark_api", "engine", "select"):
        _check(f"源码未以代码形式引用 {banned}", banned not in code_identifiers)

    print("\n[2] 无数据库配置时仍可导入")
    env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
    probe = subprocess.run(
        [sys.executable, "-c", "import services.question_validator as m; print('IMPORT_OK', m.__name__)"],
        cwd=str(BACKEND_DIR), env=env, capture_output=True, text=True,
    )
    _check("子进程无 DATABASE_URL 时导入成功", probe.returncode == 0,
           (probe.stderr or "").strip()[-200:])
    _check("子进程输出确认模块名", "IMPORT_OK services.question_validator" in probe.stdout,
           probe.stdout.strip())

    # ============================================================
    # [3] ValidationResult
    # ============================================================
    print("\n[3] ValidationResult 结构")
    _check("是独立定义的类型（dataclass）", inspect.isclass(qv.ValidationResult))
    fields = {f.name for f in qv.ValidationResult.__dataclass_fields__.values()}
    _check("字段为 valid / errors / warnings / normalized_question",
           fields == {"valid", "errors", "warnings", "normalized_question"}, str(sorted(fields)))

    result = qv.ValidationResult()
    _check("默认 valid=True", result.valid is True)
    _check("默认 errors=[]", result.errors == [])
    _check("默认 warnings=[]", result.warnings == [])
    _check("默认 normalized_question={}", result.normalized_question == {})
    _check("错误列表互不共享（default_factory）",
           qv.ValidationResult().errors is not qv.ValidationResult().errors)

    payload = result.to_dict()
    _check("to_dict 键集与约定一致",
           set(payload) == {"valid", "errors", "warnings", "normalized_question"},
           str(sorted(payload)))
    _check("to_dict 可 JSON 序列化",
           json.loads(json.dumps(payload, ensure_ascii=False)) == payload)

    result.add_error("question_empty")
    _check("add_error 追加错误码", result.errors == ["question_empty"])
    _check("add_error 自动置 valid=False（结果类型的不变量）", result.valid is False)
    _check("空错误码被拒", _raises(lambda: result.add_error("   "), ValueError))

    warned = qv.ValidationResult()
    warned.add_warning("难度与计划不一致")
    _check("add_warning 不影响 valid", warned.valid is True and warned.warnings != [])

    result.normalized_question["difficulty"] = "mid"
    snapshot = result.to_dict()
    snapshot["errors"].append("外部篡改")
    snapshot["normalized_question"]["difficulty"] = "hacked"
    _check("to_dict 返回副本，不暴露内部引用",
           result.errors == ["question_empty"]
           and result.normalized_question == {"difficulty": "mid"})

    # ============================================================
    # [4] ValidationError
    # ============================================================
    print("\n[4] ValidationError 异常类型")
    _check("ValidationError 继承 QuestionValidatorError",
           issubclass(qv.ValidationError, qv.QuestionValidatorError))
    _check("QuestionValidatorError 继承 Exception",
           issubclass(qv.QuestionValidatorError, Exception))
    _check("ValidationError 同时继承 ValueError",
           issubclass(qv.ValidationError, ValueError))
    _check("可用基类捕获", _raises(
        lambda: (_ for _ in ()).throw(qv.ValidationError("x")), qv.QuestionValidatorError))

    from pydantic import ValidationError as PydanticValidationError  # noqa: PLC0415

    _check("与 pydantic.ValidationError 是不同类",
           qv.ValidationError is not PydanticValidationError)
    _check("不继承 pydantic.ValidationError",
           not issubclass(qv.ValidationError, PydanticValidationError))
    _check("异常语义已在 docstring 固定（区分「不合法」与「无法校验」）",
           "校验流程本身无法进行" in (qv.ValidationError.__doc__ or ""))

    # ============================================================
    # [5] QuestionValidator 接口
    # ============================================================
    print("\n[5] QuestionValidator 接口")
    _check("可无参构造", qv.QuestionValidator() is not None)
    signature = inspect.signature(qv.QuestionValidator.validate)
    params = list(signature.parameters)
    _check("validate 参数顺序为 self / question_data / context / plan",
           params == ["self", "question_data", "context", "plan"], str(params))
    _check("context 与 plan 可省略（默认 None）",
           signature.parameters["context"].default is None
           and signature.parameters["plan"].default is None)
    _check("validate 是普通实例方法（非 static/classmethod）",
           inspect.isfunction(qv.QuestionValidator.validate)
           and "self" in signature.parameters)

    _check("只传 question_data 即可调用",
           isinstance(VALIDATOR.validate(VALID_DATA), qv.ValidationResult))
    _check("传 question_data + context 可调用",
           isinstance(VALIDATOR.validate(VALID_DATA, SAMPLE_CONTEXT), qv.ValidationResult))
    _check("三参数位置调用符合给定签名",
           isinstance(VALIDATOR.validate(VALID_DATA, SAMPLE_CONTEXT, SAMPLE_PLAN),
                      qv.ValidationResult))
    _check("支持关键字调用",
           isinstance(VALIDATOR.validate(question_data=VALID_DATA, context=SAMPLE_CONTEXT,
                                         plan=SAMPLE_PLAN), qv.ValidationResult))
    _check("返回的是 ValidationResult 实例（而非裸 dict）",
           type(VALIDATOR.validate(VALID_DATA)) is qv.ValidationResult)

    # ============================================================
    # [6] question 校验
    # ============================================================
    print("\n[6] question 校验")
    ok = VALIDATOR.validate(VALID_DATA, SAMPLE_CONTEXT, SAMPLE_PLAN)
    _check("[用例1] question 正常 → valid=True", ok.valid is True)
    _check("[用例1] question 正常 → errors=[]", ok.errors == [], str(ok.errors))
    _check("[用例1] question 正常 → warnings 为空", ok.warnings == [])
    _check("[用例1] question 正常 → 产出六字段 normalized_question",
           tuple(ok.normalized_question) == tuple(qv.NORMALIZED_FIELDS),
           str(tuple(ok.normalized_question)))

    _check("[用例2] question 为空字符串 → valid=False",
           VALIDATOR.validate({**VALID_DATA, "question": ""}).valid is False)
    _check("[用例2] question 为空字符串 → errors=[question_empty]",
           VALIDATOR.validate({**VALID_DATA, "question": ""}).errors == [qv.ERROR_QUESTION_EMPTY],
           str(VALIDATOR.validate({**VALID_DATA, "question": ""}).errors))
    _check("[用例2] question 纯空白 → 视为空",
           VALIDATOR.validate({**VALID_DATA, "question": "   "}).errors
           == [qv.ERROR_QUESTION_EMPTY])
    _check("[用例2] question 为换行制表符 → 视为空",
           VALIDATOR.validate({**VALID_DATA, "question": "\n\t  "}).errors
           == [qv.ERROR_QUESTION_EMPTY])

    missing = {k: v for k, v in VALID_DATA.items() if k != "question"}
    _check("question 键缺失 → question_empty",
           VALIDATOR.validate(missing).errors == [qv.ERROR_QUESTION_EMPTY])
    _check("question 为 None → question_empty",
           VALIDATOR.validate({**VALID_DATA, "question": None}).errors
           == [qv.ERROR_QUESTION_EMPTY])
    for label, weird in (("数字", 123), ("布尔", True), ("列表", ["q"]), ("字典", {"a": 1})):
        _check(f"question 为非字符串（{label}）→ question_empty",
               VALIDATOR.validate({**VALID_DATA, "question": weird}).errors
               == [qv.ERROR_QUESTION_EMPTY],
               str(VALIDATOR.validate({**VALID_DATA, "question": weird}).errors))
    _check("question 前后有空白但非空 → 通过（本阶段不做 strip 写回）",
           VALIDATOR.validate({**VALID_DATA, "question": "  请介绍你自己  "}).valid is True)
    _check("question 为超长文本 → 通过（长度不校验）",
           VALIDATOR.validate({**VALID_DATA, "question": "问" * 5000}).valid is True)
    _check("question 为多行文本 → 通过",
           VALIDATOR.validate({**VALID_DATA, "question": "第一行\n第二行"}).valid is True)

    # ============================================================
    # [7] topic 校验
    # ============================================================
    print("\n[7] topic 校验")
    _check("[用例3] topic 为空字符串 → valid=False",
           VALIDATOR.validate({**VALID_DATA, "topic": ""}).valid is False)
    _check("[用例3] topic 为空字符串 → errors=[topic_empty]",
           VALIDATOR.validate({**VALID_DATA, "topic": ""}).errors == [qv.ERROR_TOPIC_EMPTY],
           str(VALIDATOR.validate({**VALID_DATA, "topic": ""}).errors))
    _check("[用例3] topic 纯空白 → 视为空",
           VALIDATOR.validate({**VALID_DATA, "topic": "  \u3000 "}).errors
           == [qv.ERROR_TOPIC_EMPTY])

    missing = {k: v for k, v in VALID_DATA.items() if k != "topic"}
    _check("topic 键缺失 → topic_empty",
           VALIDATOR.validate(missing).errors == [qv.ERROR_TOPIC_EMPTY])
    _check("topic 为 None → topic_empty",
           VALIDATOR.validate({**VALID_DATA, "topic": None}).errors == [qv.ERROR_TOPIC_EMPTY])
    for label, weird in (("数字", 1), ("列表", ["t"]), ("字典", {"t": 1})):
        _check(f"topic 为非字符串（{label}）→ topic_empty",
               VALIDATOR.validate({**VALID_DATA, "topic": weird}).errors
               == [qv.ERROR_TOPIC_EMPTY])
    _check("topic 前后有空白但非空 → 通过",
           VALIDATOR.validate({**VALID_DATA, "topic": "  Redis  "}).valid is True)

    # ============================================================
    # [8] question 与 topic 同时为空
    # ============================================================
    print("\n[8] question 与 topic 同时为空")
    both = VALIDATOR.validate({"question": "", "topic": ""})
    _check("[用例4] 两者同时为空 → valid=False", both.valid is False)
    _check("[用例4] 两者同时为空 → 登记两条错误码",
           both.errors == [qv.ERROR_QUESTION_EMPTY, qv.ERROR_TOPIC_EMPTY], str(both.errors))
    _check("[用例4] 规则不短路（不是只报第一条）", len(both.errors) == 2)
    _check("[用例4] 错误码顺序稳定（question 在前）",
           both.errors.index(qv.ERROR_QUESTION_EMPTY) < both.errors.index(qv.ERROR_TOPIC_EMPTY))

    _check("空 dict → 两条错误码",
           VALIDATOR.validate({}).errors == [qv.ERROR_QUESTION_EMPTY, qv.ERROR_TOPIC_EMPTY])
    _check("两键都缺失 → 两条错误码（difficulty 合法，不额外报错）",
           VALIDATOR.validate({"difficulty": "senior"}).errors
           == [qv.ERROR_QUESTION_EMPTY, qv.ERROR_TOPIC_EMPTY])
    _check("两键都为 None → 两条错误码",
           VALIDATOR.validate({"question": None, "topic": None}).errors
           == [qv.ERROR_QUESTION_EMPTY, qv.ERROR_TOPIC_EMPTY])
    _check("仅 question 为空 → 只报 question_empty",
           VALIDATOR.validate({"question": "", "topic": "T"}).errors
           == [qv.ERROR_QUESTION_EMPTY])
    _check("仅 topic 为空 → 只报 topic_empty",
           VALIDATOR.validate({"question": "Q", "topic": ""}).errors == [qv.ERROR_TOPIC_EMPTY])
    _check("两者都有值 → 无错误",
           VALIDATOR.validate({"question": "Q", "topic": "T"}).errors == [])
    _check("非 Mapping 入参（None）→ 两条错误码且不抛异常",
           VALIDATOR.validate(None).errors == [qv.ERROR_QUESTION_EMPTY, qv.ERROR_TOPIC_EMPTY])
    _check("非 Mapping 入参（字符串）→ 两条错误码",
           VALIDATOR.validate("not a mapping").errors
           == [qv.ERROR_QUESTION_EMPTY, qv.ERROR_TOPIC_EMPTY])
    _check("非 Mapping 入参（列表）→ 两条错误码",
           VALIDATOR.validate([1, 2]).errors
           == [qv.ERROR_QUESTION_EMPTY, qv.ERROR_TOPIC_EMPTY])

    # ============================================================
    # [9] 取值合法性边界（本阶段仅 difficulty 受校验）
    # ============================================================
    print("\n[9] 取值合法性边界（本阶段仅 difficulty 受校验）")
    _check("difficulty 缺失（且无 plan）→ 不影响 valid（缺失不算非法）",
           VALIDATOR.validate({"question": "Q", "topic": "T"}).valid is True)
    _check("difficulty 取值合法 → 不影响 valid",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": "senior"}).valid
           is True)
    _check("question_type 非法取值 → 不影响 valid",
           VALIDATOR.validate({"question": "Q", "topic": "T", "question_type": "weird"}).valid
           is True)
    _check("expected_points 非数组 → 不影响 valid",
           VALIDATOR.validate({"question": "Q", "topic": "T", "expected_points": "A,B"}).valid
           is True)
    _check("reason 缺失 → 不影响 valid",
           VALIDATOR.validate({"question": "Q", "topic": "T"}).valid is True)
    _check("reason 为非字符串 → 不影响 valid",
           VALIDATOR.validate({"question": "Q", "topic": "T", "reason": 42}).valid is True)
    _check("额外未知字段被忽略 → 不影响 valid",
           VALIDATOR.validate({"question": "Q", "topic": "T", "unexpected": object()}).valid
           is True)
    _check("只提供 question 与 topic 即通过（其余字段自动补全，不报错）",
           VALIDATOR.validate({"question": "Q", "topic": "T"}).to_dict()
           == {"valid": True, "errors": [], "warnings": [],
               "normalized_question": {"question": "Q", "question_type": "", "topic": "T",
                                       "difficulty": "", "expected_points": [], "reason": ""}},
           str(VALIDATOR.validate({"question": "Q", "topic": "T"}).to_dict()))
    _check("「不校验取值」不等于「不处理」：question_type 非法取值仍被标准化进六字段结构",
           VALIDATOR.validate({"question": "Q", "topic": "T", "question_type": "weird"}
                              ).normalized_question["question_type"] == "weird")

    # ============================================================
    # [10] 错误码契约
    # ============================================================
    print("\n[10] 错误码契约")
    _check("ERROR_QUESTION_EMPTY == 'question_empty'",
           qv.ERROR_QUESTION_EMPTY == "question_empty", qv.ERROR_QUESTION_EMPTY)
    _check("ERROR_TOPIC_EMPTY == 'topic_empty'",
           qv.ERROR_TOPIC_EMPTY == "topic_empty", qv.ERROR_TOPIC_EMPTY)
    _check("ERROR_INVALID_DIFFICULTY == 'invalid_difficulty'",
           qv.ERROR_INVALID_DIFFICULTY == "invalid_difficulty", qv.ERROR_INVALID_DIFFICULTY)
    for code in (qv.ERROR_QUESTION_EMPTY, qv.ERROR_TOPIC_EMPTY, qv.ERROR_INVALID_DIFFICULTY):
        _check(f"错误码 {code!r} 为机器可读蛇形（纯小写 ASCII + 下划线）",
               bool(re.fullmatch(r"[a-z]+(_[a-z]+)*", code)), code)
        _check(f"错误码 {code!r} 不含空格或中文",
               code.isascii() and " " not in code)
    _check("三个错误码互不相同",
           len({qv.ERROR_QUESTION_EMPTY, qv.ERROR_TOPIC_EMPTY,
                qv.ERROR_INVALID_DIFFICULTY}) == 3)
    # 注意：__all__ 里放的是**常量名**（字符串形式的标识符），不是常量值。
    # 因此这里断言标识符可见，再断言标识符能取回期望的错误码值。
    _check("错误码常量名出现在 __all__ 中（对外契约可见）",
           all(name in qv.__all__ for name in ("ERROR_QUESTION_EMPTY", "ERROR_TOPIC_EMPTY",
                                               "ERROR_INVALID_DIFFICULTY")),
           str(qv.__all__))
    _check("__all__ 中的错误码常量名可解析为预期错误码值",
           getattr(qv, "ERROR_QUESTION_EMPTY") == "question_empty"
           and getattr(qv, "ERROR_TOPIC_EMPTY") == "topic_empty"
           and getattr(qv, "ERROR_INVALID_DIFFICULTY") == "invalid_difficulty")
    _check("__all__ 导出 18 个公开名",
           qv.__all__ == ["ERROR_QUESTION_EMPTY", "ERROR_TOPIC_EMPTY",
                          "ERROR_INVALID_DIFFICULTY", "ERROR_DUPLICATE_QUESTION",
                          "ALLOWED_DIFFICULTIES", "NORMALIZED_FIELDS",
                          "SIMILAR_QUESTION_THRESHOLD", "CONTAINMENT_MIN_LEN",
                          "resolve_difficulty", "is_valid_difficulty",
                          "normalize_question_text", "question_similarity",
                          "find_asked_question_match", "AskedQuestionMatch",
                          "ValidationResult", "QuestionValidatorError", "ValidationError",
                          "QuestionValidator"],
           str(qv.__all__))
    _check("标准化契约常量名出现在 __all__ 中", "NORMALIZED_FIELDS" in qv.__all__)
    _check("difficulty 规则函数与允许集均对外可见",
           {"ALLOWED_DIFFICULTIES", "resolve_difficulty", "is_valid_difficulty"} <= set(qv.__all__))
    _check("重复检测规则函数与阈值均对外可见",
           {"ERROR_DUPLICATE_QUESTION", "SIMILAR_QUESTION_THRESHOLD",
            "normalize_question_text", "question_similarity",
            "find_asked_question_match", "AskedQuestionMatch"} <= set(qv.__all__))
    _check("错误码列表无重复",
           len(set(both.errors)) == len(both.errors))
    _check("valid 与 errors 的不变量：errors 非空 ⇒ valid=False",
           all(VALIDATOR.validate(d).valid is (not VALIDATOR.validate(d).errors)
               for d in ({"question": "Q", "topic": "T"}, {}, {"question": "", "topic": ""})))
    _check("errors 元素均为字符串",
           all(isinstance(e, str) for e in both.errors))

    # ============================================================
    # [11] 无副作用
    # ============================================================
    print("\n[11] 无副作用")
    question_copy = json.loads(json.dumps(VALID_DATA))
    context_copy = json.loads(json.dumps(SAMPLE_CONTEXT))
    plan_copy = json.loads(json.dumps(SAMPLE_PLAN))
    first = VALIDATOR.validate(VALID_DATA, SAMPLE_CONTEXT, SAMPLE_PLAN)
    _check("不篡改 question_data 入参", VALID_DATA == question_copy)
    _check("不篡改 context 入参", SAMPLE_CONTEXT == context_copy)
    _check("不篡改 plan 入参", SAMPLE_PLAN == plan_copy)
    _check("可重复调用且结果一致",
           VALIDATOR.validate(VALID_DATA, SAMPLE_CONTEXT, SAMPLE_PLAN).to_dict()
           == first.to_dict())
    _check("两次调用的 errors 不是同一列表对象",
           VALIDATOR.validate({}).errors is not VALIDATOR.validate({}).errors)
    _check("不同实例行为一致",
           qv.QuestionValidator().validate({}).to_dict() == VALIDATOR.validate({}).to_dict())
    _check("context 只被用于读 asked_questions（其它字段不参与校验）",
           VALIDATOR.validate({"question": "Q", "topic": "T"},
                              {"asked_questions": [], "current_stage": "technical"},
                              SAMPLE_PLAN).to_dict()
           == VALIDATOR.validate({"question": "Q", "topic": "T"}, None, SAMPLE_PLAN).to_dict())
    _check("plan 仅在 difficulty 缺失时被读取（已提供时不看 plan）",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": "junior"},
                              None, {"difficulty": "senior"}).to_dict()
           == VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": "junior"},
                                 object(), object()).to_dict())

    # ============================================================
    # [12] 标准化契约
    # ============================================================
    print("\n[12] 标准化契约")
    _check("模块导出 NORMALIZED_FIELDS 常量", hasattr(qv, "NORMALIZED_FIELDS"))
    _check("NORMALIZED_FIELDS 为约定的六字段且顺序固定",
           tuple(qv.NORMALIZED_FIELDS) == ("question", "question_type", "topic",
                                           "difficulty", "expected_points", "reason"),
           str(tuple(qv.NORMALIZED_FIELDS)))
    _check("NORMALIZED_FIELDS 无重复", len(set(qv.NORMALIZED_FIELDS)) == 6)

    full = VALIDATOR.validate(VALID_DATA, SAMPLE_CONTEXT, SAMPLE_PLAN)
    norm = full.normalized_question
    _check("校验通过 → normalized_question 非空", norm != {})
    _check("键与顺序与 NORMALIZED_FIELDS 完全一致",
           tuple(norm) == tuple(qv.NORMALIZED_FIELDS), str(tuple(norm)))
    _check("恰好六个键", len(norm) == 6)
    _check("question 逐字保留", norm["question"] == VALID_DATA["question"])
    _check("topic 逐字保留", norm["topic"] == VALID_DATA["topic"])
    _check("question_type 逐字保留", norm["question_type"] == VALID_DATA["question_type"])
    _check("difficulty 逐字保留", norm["difficulty"] == VALID_DATA["difficulty"])
    _check("expected_points 逐字保留", norm["expected_points"] == VALID_DATA["expected_points"])
    _check("reason 逐字保留", norm["reason"] == VALID_DATA["reason"])
    _check("字段齐全 → 不产生 warning（正常补全路径不告警）",
           full.warnings == [], str(full.warnings))
    _check("字段齐全且合法时 normalized_question 与原数据等值",
           norm == VALID_DATA)
    _check("文本不做 strip 改写（空白原样保留）",
           VALIDATOR.validate({"question": "  请介绍你自己  ", "topic": "  Redis  "}
                              ).normalized_question["question"] == "  请介绍你自己  "
           and VALIDATOR.validate({"question": "  请介绍你自己  ", "topic": "  Redis  "}
                                  ).normalized_question["topic"] == "  Redis  ")

    # ============================================================
    # [13] 字段补全规则（difficulty / expected_points / reason / question_type）
    # ============================================================
    print("\n[13] 字段补全规则")

    # ---- 用例1：difficulty 缺失 → 用 plan.difficulty 补全 ----
    case1 = VALIDATOR.validate({"question": "解释Redis", "topic": "Redis"},
                               plan={"difficulty": "senior"})
    _check("[用例1] 校验通过", case1.valid is True, str(case1.errors))
    _check("[用例1] difficulty 缺失 → 用 plan.difficulty 补全为 'senior'",
           case1.normalized_question["difficulty"] == "senior",
           repr(case1.normalized_question.get("difficulty")))
    _check("[用例1] 缺失补全属正常路径，不产生 warning", case1.warnings == [],
           str(case1.warnings))
    _check("[用例1] 补全结果与题目示例完全一致（六字段）",
           case1.normalized_question == {"question": "解释Redis", "question_type": "",
                                         "topic": "Redis", "difficulty": "senior",
                                         "expected_points": [], "reason": ""},
           str(case1.normalized_question))
    _check("[用例1] 'senior' 属于本模块允许集（与全项目标准一致）",
           "senior" in qv.ALLOWED_DIFFICULTIES)

    # difficulty 缺失且 plan 无可用值 → ""
    for label, plan_arg in (("未传 plan", None),
                            ("plan 无 difficulty 键", {"interview_type": "technical"}),
                            ("plan 为普通对象", object()),
                            ("plan.difficulty 为空串", {"difficulty": "   "}),
                            ("plan.difficulty 非字符串", {"difficulty": 3})):
        _check(f"difficulty 缺失且 {label} → 补 ''",
               VALIDATOR.validate({"question": "Q", "topic": "T"}, plan=plan_arg
                                  ).normalized_question["difficulty"] == "",
               repr(VALIDATOR.validate({"question": "Q", "topic": "T"}, plan=plan_arg
                                       ).normalized_question.get("difficulty")))
    _check("plan 为 ORM 风格对象时也能读到 difficulty（属性访问）",
           VALIDATOR.validate({"question": "Q", "topic": "T"},
                              plan=SimpleNamespace(difficulty="senior")
                              ).normalized_question["difficulty"] == "senior")

    # difficulty 已提供 → 只检测是否为空（合法性校验见 [14] 节）
    _check("difficulty 已提供且合法 → 原样保留",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": "junior"}
                              ).normalized_question["difficulty"] == "junior")
    _check("difficulty 已提供且非空 → 优先于 plan.difficulty",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": "junior"},
                              plan={"difficulty": "senior"}
                              ).normalized_question["difficulty"] == "junior")
    _check("difficulty 为空串 + plan 有值 → 用 plan 补全",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": ""},
                              plan={"difficulty": "senior"}
                              ).normalized_question["difficulty"] == "senior")
    _check("difficulty 纯空白 + plan 有值 → 用 plan 补全",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": "   "},
                              plan={"difficulty": "senior"}
                              ).normalized_question["difficulty"] == "senior")
    _check("difficulty 为 None + plan 有值 → 用 plan 补全",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": None},
                              plan={"difficulty": "senior"}
                              ).normalized_question["difficulty"] == "senior")
    _check("difficulty 存在但为空 → 产生非阻断 warning",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": ""},
                              plan={"difficulty": "senior"}).warnings != [])
    _check("difficulty 存在但为空 → 补全成功仍 valid=True",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": ""},
                              plan={"difficulty": "senior"}).valid is True)
    _check("difficulty 为 None → 视为「未提供」，不告警（与键缺失同等待遇）",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": None},
                              plan={"difficulty": "senior"}).warnings == [])
    _check("difficulty 非字符串（数字）→ 补全并告警",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": 5},
                              plan={"difficulty": "senior"}).warnings != [])
    _check("difficulty 不可用且 plan 也无 → 置空并告警",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": ""
                               }).warnings != [])

    # ---- 用例2：expected_points 缺失 → [] ----
    case2 = VALIDATOR.validate({"question": "Q", "topic": "T"})
    _check("[用例2] expected_points 缺失 → 补 []",
           case2.normalized_question["expected_points"] == [],
           repr(case2.normalized_question.get("expected_points")))
    _check("[用例2] 补出的值为 list 类型",
           isinstance(case2.normalized_question["expected_points"], list))
    _check("[用例2] 缺失补全不告警", case2.warnings == [])
    _check("expected_points 为空数组 → 保持 []",
           VALIDATOR.validate({"question": "Q", "topic": "T", "expected_points": []}
                              ).normalized_question["expected_points"] == [])
    _check("expected_points 为 None → 补 [] 且不告警",
           VALIDATOR.validate({"question": "Q", "topic": "T", "expected_points": None}
                              ).normalized_question["expected_points"] == []
           and VALIDATOR.validate({"question": "Q", "topic": "T", "expected_points": None}
                                  ).warnings == [])
    _check("expected_points 非数组（字符串）→ 补 [] 并告警",
           VALIDATOR.validate({"question": "Q", "topic": "T", "expected_points": "A,B"}
                              ).normalized_question["expected_points"] == []
           and VALIDATOR.validate({"question": "Q", "topic": "T", "expected_points": "A,B"}
                                  ).warnings != [])
    _check("expected_points 非数组（字典）→ 补 [] 并告警",
           VALIDATOR.validate({"question": "Q", "topic": "T", "expected_points": {"a": 1}}
                              ).normalized_question["expected_points"] == [])
    _check("expected_points 数组含非字符串元素 → 只保留字符串",
           VALIDATOR.validate({"question": "Q", "topic": "T",
                               "expected_points": ["A", 1, None, "B", {"c": 3}]}
                              ).normalized_question["expected_points"] == ["A", "B"])
    _check("expected_points 含非字符串元素 → 告警",
           VALIDATOR.validate({"question": "Q", "topic": "T",
                               "expected_points": ["A", 1]}
                              ).warnings != [])
    _check("expected_points 元素顺序保持不变",
           VALIDATOR.validate({"question": "Q", "topic": "T",
                               "expected_points": ["z", "a", "m"]}
                              ).normalized_question["expected_points"] == ["z", "a", "m"])
    _check("expected_points 全为非字符串 → 补 [] 并告警",
           VALIDATOR.validate({"question": "Q", "topic": "T", "expected_points": [1, 2]}
                              ).normalized_question["expected_points"] == [])

    # ---- 用例3：reason 缺失 → "" ----
    _check("[用例3] reason 缺失 → 补 ''",
           VALIDATOR.validate({"question": "Q", "topic": "T"}
                              ).normalized_question["reason"] == "")
    _check("[用例3] reason 为空串 → 保持 ''",
           VALIDATOR.validate({"question": "Q", "topic": "T", "reason": ""}
                              ).normalized_question["reason"] == "")
    _check("[用例3] reason 为 None → 补 '' 且不告警",
           VALIDATOR.validate({"question": "Q", "topic": "T", "reason": None}
                              ).normalized_question["reason"] == ""
           and VALIDATOR.validate({"question": "Q", "topic": "T", "reason": None}
                                  ).warnings == [])
    _check("[用例3] reason 为非字符串（数字）→ 补 '' 并告警",
           VALIDATOR.validate({"question": "Q", "topic": "T", "reason": 42}
                              ).normalized_question["reason"] == ""
           and VALIDATOR.validate({"question": "Q", "topic": "T", "reason": 42}
                                  ).warnings != [])
    _check("reason 已提供 → 逐字保留",
           VALIDATOR.validate({"question": "Q", "topic": "T", "reason": "补强薄弱点"}
                              ).normalized_question["reason"] == "补强薄弱点")

    # ---- 用例4：question_type 缺失 → "" ----
    _check("[用例4] question_type 缺失 → 补 ''",
           VALIDATOR.validate({"question": "Q", "topic": "T"}
                              ).normalized_question["question_type"] == "")
    _check("[用例4] question_type 为空串 → 保持 ''",
           VALIDATOR.validate({"question": "Q", "topic": "T", "question_type": ""}
                              ).normalized_question["question_type"] == "")
    _check("[用例4] question_type 为 None → 补 '' 且不告警",
           VALIDATOR.validate({"question": "Q", "topic": "T", "question_type": None}
                              ).normalized_question["question_type"] == ""
           and VALIDATOR.validate({"question": "Q", "topic": "T", "question_type": None}
                                  ).warnings == [])
    _check("[用例4] question_type 非字符串（列表）→ 补 '' 并告警",
           VALIDATOR.validate({"question": "Q", "topic": "T", "question_type": ["technical"]}
                              ).normalized_question["question_type"] == ""
           and VALIDATOR.validate({"question": "Q", "topic": "T",
                                   "question_type": ["technical"]}).warnings != [])
    _check("[用例4] question_type 非法枚举值 → 原样保留（本阶段不做枚举校验）",
           VALIDATOR.validate({"question": "Q", "topic": "T", "question_type": "weird"}
                              ).normalized_question["question_type"] == "weird")

    _check("四个字段同时缺失 → 一次性补全，全部走默认值",
           VALIDATOR.validate({"question": "Q", "topic": "T"}).normalized_question
           == {"question": "Q", "question_type": "", "topic": "T", "difficulty": "",
               "expected_points": [], "reason": ""})

    # ============================================================
    # [14] difficulty 合法性校验（本阶段 rule-1）
    # ============================================================
    print("\n[14] difficulty 合法性校验")

    _check("允许集为 junior / mid / senior",
           tuple(qv.ALLOWED_DIFFICULTIES) == ("junior", "mid", "senior"),
           str(tuple(qv.ALLOWED_DIFFICULTIES)))
    _check("允许集为闭集（恰好 3 个）且无重复",
           len(qv.ALLOWED_DIFFICULTIES) == 3 and len(set(qv.ALLOWED_DIFFICULTIES)) == 3)

    # ---- 情况2：question_data 提供且合法 → 通过 ----
    for legal in qv.ALLOWED_DIFFICULTIES:
        _check(f"[用例2] difficulty={legal!r} 合法 → valid=True",
               VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": legal}
                                  ).valid is True)
        _check(f"[用例2] difficulty={legal!r} 原样写入 normalized_question",
               VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": legal}
                                  ).normalized_question["difficulty"] == legal)

    # ---- 情况3：difficulty 非法 → valid=False + errors=[invalid_difficulty] ----
    for illegal in ("god_mode", "expert", "hard", "SENIOR", " senior ", "sen",
                    # 已废弃的旧词汇：词汇表统一后必须一律被拒（不设转换层）
                    "beginner", "intermediate", "advanced", "Beginner"):
        bad = VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": illegal})
        _check(f"[用例3] difficulty={illegal!r} 非法 → valid=False", bad.valid is False)
        _check(f"[用例3] difficulty={illegal!r} → errors=[invalid_difficulty]",
               bad.errors == [qv.ERROR_INVALID_DIFFICULTY], str(bad.errors))
        _check(f"[用例3] difficulty={illegal!r} → normalized_question 为空",
               bad.normalized_question == {})
    _check("[用例3] difficulty 非法记的是 error 而非 warning",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": "god_mode"}
                              ).warnings == [])
    _check("[用例3] 旧词汇 beginner/intermediate/advanced 一律被拒（无别名映射）",
           all(VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": v}).errors
               == [qv.ERROR_INVALID_DIFFICULTY]
               for v in ("beginner", "intermediate", "advanced")))
    _check("项目标准词汇 junior/mid/senior 全部合法（词汇表已统一）",
           all(VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": v}).valid
               is True for v in ("junior", "mid", "senior")))

    # ---- 情况1：缺失 → 用 plan.difficulty 补全，补全后同样受合法性约束 ----
    case1b = VALIDATOR.validate({"question": "解释Redis缓存机制", "topic": "Redis"},
                                plan={"difficulty": "senior"})
    _check("[用例1] 缺失 → plan.difficulty 补全，valid=True", case1b.valid is True)
    _check("[用例1] 输出 difficulty='senior'",
           case1b.normalized_question["difficulty"] == "senior")
    _check("[用例1] 补全后仍校验合法性：plan.difficulty 非法 → invalid_difficulty",
           VALIDATOR.validate({"question": "Q", "topic": "T"},
                              plan={"difficulty": "god_mode"}).errors
           == [qv.ERROR_INVALID_DIFFICULTY])
    _check("[用例1] 缺失且 plan 也无 → 视为「未知」，不记 invalid_difficulty",
           VALIDATOR.validate({"question": "Q", "topic": "T"}).errors == [])
    _check("difficulty 为非字符串（数字）→ 回落 plan，合法则通过",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": 5},
                              plan={"difficulty": "junior"}).valid is True)
    _check("difficulty 非字符串且 plan 也无 → 视为未知，不记 invalid_difficulty",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": 5}).errors == [])

    # ---- difficulty 规则由两个独立纯函数实现 ----
    _check("resolve_difficulty 优先取 question_data",
           qv.resolve_difficulty({"difficulty": "junior"}, {"difficulty": "senior"})
           == "junior")
    _check("resolve_difficulty 缺失时回落 plan",
           qv.resolve_difficulty({"question": "Q"}, {"difficulty": "senior"}) == "senior")
    _check("resolve_difficulty 都缺失 → ''",
           qv.resolve_difficulty({"question": "Q"}, None) == "")
    _check("resolve_difficulty 支持 ORM 风格 plan",
           qv.resolve_difficulty({"question": "Q"}, SimpleNamespace(difficulty="mid"))
           == "mid")
    _check("resolve_difficulty 非 Mapping 入参 → 回落 plan",
           qv.resolve_difficulty(None, {"difficulty": "senior"}) == "senior")
    resolve_input = {"question": "Q"}
    resolve_plan = {"difficulty": "senior"}
    qv.resolve_difficulty(resolve_input, resolve_plan)
    _check("resolve_difficulty 不修改入参", resolve_input == {"question": "Q"})
    _check("resolve_difficulty 不修改 plan", resolve_plan == {"difficulty": "senior"})

    _check("is_valid_difficulty 接受允许集内全部取值",
           all(qv.is_valid_difficulty(v) for v in qv.ALLOWED_DIFFICULTIES))
    _check("is_valid_difficulty 拒绝集合外取值", not qv.is_valid_difficulty("god_mode"))
    _check("is_valid_difficulty 拒绝空值 / None / 非字符串",
           not qv.is_valid_difficulty("") and not qv.is_valid_difficulty(None)
           and not qv.is_valid_difficulty(5) and not qv.is_valid_difficulty(["senior"]))
    _check("is_valid_difficulty 为精确相等比较（不做 trim）",
           not qv.is_valid_difficulty(" senior "))
    _check("is_valid_difficulty 支持自定义允许集",
           qv.is_valid_difficulty("mid", allowed=("junior", "mid", "senior")))
    _check("is_valid_difficulty 是纯函数（重复调用结果一致）",
           [qv.is_valid_difficulty("senior") for _ in range(3)] == [True] * 3)

    # ---- 不影响其他字段的校验与补全 ----
    _check("difficulty 非法不掩盖 question/topic 的错误码（规则不短路）",
           VALIDATOR.validate({"question": "", "topic": "", "difficulty": "god_mode"}).errors
           == [qv.ERROR_QUESTION_EMPTY, qv.ERROR_TOPIC_EMPTY, qv.ERROR_INVALID_DIFFICULTY],
           str(VALIDATOR.validate({"question": "", "topic": "",
                                   "difficulty": "god_mode"}).errors))
    _check("difficulty 合法时错误码列表不受影响",
           VALIDATOR.validate({"question": "", "topic": "", "difficulty": "senior"}).errors
           == [qv.ERROR_QUESTION_EMPTY, qv.ERROR_TOPIC_EMPTY])
    _check("difficulty 校验不改动其他字段的补全结果",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": "senior",
                               "expected_points": ["A"], "reason": "r",
                               "question_type": "technical"}).normalized_question
           == {"question": "Q", "question_type": "technical", "topic": "T",
               "difficulty": "senior", "expected_points": ["A"], "reason": "r"})
    _check("normalized_question 的 difficulty 与 resolve_difficulty 结果一致",
           VALIDATOR.validate(VALID_DATA, None, SAMPLE_PLAN).normalized_question["difficulty"]
           == qv.resolve_difficulty(VALID_DATA, SAMPLE_PLAN))
    _check("QuestionValidator 仍只暴露 validate 一个公开方法（职责单一）",
           [n for n in dir(qv.QuestionValidator) if not n.startswith("_")] == ["validate"],
           str([n for n in dir(qv.QuestionValidator) if not n.startswith("_")]))

    # ============================================================
    # [15] 问题重复检测（本阶段 rule-2）
    # ============================================================
    print("\n[15] 问题重复检测")

    _check("ERROR_DUPLICATE_QUESTION == 'duplicate_question'",
           qv.ERROR_DUPLICATE_QUESTION == "duplicate_question")
    _check("相似度阈值常量为 0.85（与 interview_agent 一致）",
           qv.SIMILAR_QUESTION_THRESHOLD == 0.85)
    _check("包含兜底最短长度常量为 8", qv.CONTAINMENT_MIN_LEN == 8)

    # ---- 文本归一化：去空白 / 去标点 / 小写 ----
    _check("归一化去空格",
           qv.normalize_question_text("Redis 为什么快") == "redis为什么快")
    _check("归一化去全角空格",
           qv.normalize_question_text("Redis\u3000为什么快") == "redis为什么快")
    _check("归一化去中英文标点",
           qv.normalize_question_text("Redis，为什么快？") == "redis为什么快")
    _check("归一化转小写",
           qv.normalize_question_text("REDIS Why FAST") == "rediswhyfast")
    _check("归一化后「Redis 为什么快？」与「redis为什么快」完全相同",
           qv.normalize_question_text("Redis 为什么快？")
           == qv.normalize_question_text("redis为什么快"))
    _check("归一化非字符串 → ''",
           qv.normalize_question_text(None) == "" and qv.normalize_question_text(5) == "")
    _check("归一化幂等",
           qv.normalize_question_text(qv.normalize_question_text("Redis，为什么快？"))
           == qv.normalize_question_text("Redis，为什么快？"))

    # ---- 相似度 ----
    _check("相似度：完全相同（含书写差异）→ 1.0",
           qv.question_similarity("Redis为什么快", "redis 为什么快？") == 1.0)
    _check("相似度：无关问题 → 接近 0",
           qv.question_similarity("介绍Spring Boot自动配置", "Redis为什么快") < 0.2)
    _check("相似度：包含关系 → 1.0（较短一方 ≥ 8 字）",
           qv.question_similarity("请做一下自我介绍",
                                  "请做一下自我介绍并说明你的职业规划") == 1.0)
    _check("相似度：短串包含不触发兜底（< 8 字）",
           qv.question_similarity("是吗", "是吗，那为什么呢") < 1.0)
    _check("相似度：任一为空 → 0.0",
           qv.question_similarity("", "Redis") == 0.0
           and qv.question_similarity("Redis", None) == 0.0)
    _check("相似度对称",
           qv.question_similarity("Redis为什么快", "Redis为什么这么快")
           == qv.question_similarity("Redis为什么这么快", "Redis为什么快"))
    _check("相似度落在 0-1 区间",
           0.0 <= qv.question_similarity("Redis为什么快", "Redis为什么这么快") <= 1.0)

    HISTORY = ["介绍Spring Boot自动配置", "Redis为什么快"]

    # ---- 用例1：完全相同 → 拒绝 ----
    dup = VALIDATOR.validate({"question": "Redis为什么快", "topic": "Redis",
                              "difficulty": "senior"}, {"asked_questions": HISTORY})
    _check("[用例1] 与历史完全相同 → valid=False", dup.valid is False)
    _check("[用例1] → errors=[duplicate_question]",
           dup.errors == [qv.ERROR_DUPLICATE_QUESTION], str(dup.errors))
    _check("[用例1] → normalized_question 为空", dup.normalized_question == {})
    _check("[用例1] → 不产生 warning（完全相同记 error 而非 warning）",
           dup.warnings == [])
    _check("[用例1] 归一化后相同即算完全相同（空格/标点/大小写差异不影响判定）",
           VALIDATOR.validate({"question": "  redis，为什么快？ ", "topic": "Redis",
                               "difficulty": "senior"},
                              {"asked_questions": HISTORY}).errors
           == [qv.ERROR_DUPLICATE_QUESTION])

    # ---- 用例2：不重复 → 通过 ----
    fresh = VALIDATOR.validate({"question": "请解释 Spring 的 IOC 容器", "topic": "Spring",
                                "difficulty": "senior"}, {"asked_questions": HISTORY})
    _check("[用例2] 与历史无关 → valid=True", fresh.valid is True, str(fresh.errors))
    _check("[用例2] → 无 error 也无 warning",
           fresh.errors == [] and fresh.warnings == [], str(fresh.warnings))
    _check("[用例2] → 正常产出六字段",
           tuple(fresh.normalized_question) == tuple(qv.NORMALIZED_FIELDS))
    _check("无历史（None / 空列表 / 缺字段 / 非列表）→ 不报重复",
           all(VALIDATOR.validate({"question": "Redis为什么快", "topic": "Redis",
                                   "difficulty": "senior"}, ctx).valid is True
               for ctx in (None, {}, {"asked_questions": []}, {"asked_questions": None},
                           {"asked_questions": "not a list"})))

    # ---- 用例3：高度相似 → warning，不拒绝 ----
    similar = VALIDATOR.validate({"question": "Redis为什么这么快", "topic": "Redis",
                                  "difficulty": "senior"}, {"asked_questions": HISTORY})
    _check("[用例3] 高度相似 → valid=True（不拒绝）", similar.valid is True, str(similar.errors))
    _check("[用例3] → errors 为空", similar.errors == [])
    _check("[用例3] → 恰好产生一条 warning",
           len(similar.warnings) == 1, str(similar.warnings))
    _check("[用例3] warning 含相似度与命中的历史问题",
           "高度相似" in similar.warnings[0] and "Redis为什么快" in similar.warnings[0],
           similar.warnings[0] if similar.warnings else "")
    _check("[用例3] 高度相似仍产出六字段",
           similar.normalized_question["question"] == "Redis为什么这么快")
    _check("包含关系（同一问题 + 附加从句）归入 warning 而非拒绝",
           VALIDATOR.validate({"question": "请做一下自我介绍并说明你的职业规划",
                               "topic": "自我介绍", "difficulty": "senior"},
                              {"asked_questions": ["请做一下自我介绍"]}).errors == []
           and VALIDATOR.validate({"question": "请做一下自我介绍并说明你的职业规划",
                                   "topic": "自我介绍", "difficulty": "senior"},
                                  {"asked_questions": ["请做一下自我介绍"]}).warnings != [])
    _check("阈值可调：调高到 0.99 后相似问题不再告警",
           qv.find_asked_question_match("Redis为什么这么快", HISTORY,
                                        threshold=0.99).kind == qv.AskedQuestionMatch.NONE)
    _check("阈值边界：恰好等于阈值算相似",
           qv.find_asked_question_match(
               "Redis为什么这么快", HISTORY,
               threshold=qv.question_similarity("Redis为什么这么快", "Redis为什么快")
           ).kind == qv.AskedQuestionMatch.SIMILAR)

    # ---- asked_questions 的两种形态 ----
    _check("支持对象形态（{\"question_no\": .., \"question\": ..}）",
           VALIDATOR.validate({"question": "Redis为什么快", "topic": "Redis",
                               "difficulty": "senior"},
                              {"asked_questions": [{"question_no": 1,
                                                    "question": "Redis为什么快"}]}
                              ).errors == [qv.ERROR_DUPLICATE_QUESTION])
    _check("混合形态（字符串 + 对象）都能识别",
           VALIDATOR.validate({"question": "Redis为什么快", "topic": "Redis",
                               "difficulty": "senior"},
                              {"asked_questions": ["介绍Spring Boot自动配置",
                                                   {"question": "Redis为什么快"}]}
                              ).errors == [qv.ERROR_DUPLICATE_QUESTION])
    _check("历史中的空项 / 非字符串项被跳过",
           VALIDATOR.validate({"question": "Redis为什么快", "topic": "Redis",
                               "difficulty": "senior"},
                              {"asked_questions": ["", "   ", None, 5, "Redis为什么快"]}
                              ).errors == [qv.ERROR_DUPLICATE_QUESTION])
    _check("ORM 风格 context 也能读到 asked_questions",
           VALIDATOR.validate({"question": "Redis为什么快", "topic": "Redis",
                               "difficulty": "senior"},
                              SimpleNamespace(asked_questions=["Redis为什么快"])
                              ).errors == [qv.ERROR_DUPLICATE_QUESTION])

    # ---- 独立函数 ----
    _check("find_asked_question_match：完全相同 → EXACT",
           qv.find_asked_question_match("Redis为什么快", HISTORY).kind
           == qv.AskedQuestionMatch.EXACT)
    _check("find_asked_question_match：相似 → SIMILAR",
           qv.find_asked_question_match("Redis为什么这么快", HISTORY).kind
           == qv.AskedQuestionMatch.SIMILAR)
    _check("find_asked_question_match：无关 → NONE",
           qv.find_asked_question_match("请解释 Spring 的 IOC 容器", HISTORY).kind
           == qv.AskedQuestionMatch.NONE)
    _check("find_asked_question_match：空问题 → NONE（交给 question_empty 处理）",
           qv.find_asked_question_match("", HISTORY).kind == qv.AskedQuestionMatch.NONE
           and qv.find_asked_question_match(None, HISTORY).kind == qv.AskedQuestionMatch.NONE)
    _check("命中时返回历史原文与相似度",
           qv.find_asked_question_match("Redis为什么这么快", HISTORY).matched
           == "Redis为什么快")
    _check("EXACT 优先于 SIMILAR（即使相似项先出现）",
           qv.find_asked_question_match("Redis为什么快",
                                        ["Redis为什么这么快", "Redis为什么快"]).kind
           == qv.AskedQuestionMatch.EXACT)
    _check("多条相似取相似度最高者",
           qv.find_asked_question_match("Redis为什么这么快",
                                        ["Redis为什么快", "Redis为什么这么慢"]).matched
           == "Redis为什么这么慢")
    match_input = list(HISTORY)
    qv.find_asked_question_match("Redis为什么快", match_input)
    _check("find_asked_question_match 不修改入参", match_input == HISTORY)
    _check("AskedQuestionMatch 是具名元组（字段 kind / matched / score）",
           qv.AskedQuestionMatch._fields == ("kind", "matched", "score"))
    _check("AskedQuestionMatch 的三个 kind 常量",
           (qv.AskedQuestionMatch.NONE, qv.AskedQuestionMatch.EXACT,
            qv.AskedQuestionMatch.SIMILAR) == ("none", "exact", "similar"))

    # ---- 与其他规则互不干扰 ----
    _check("重复检测不短路：与 topic 错误同时登记",
           VALIDATOR.validate({"question": "Redis为什么快", "topic": ""},
                              {"asked_questions": HISTORY}).errors
           == [qv.ERROR_TOPIC_EMPTY, qv.ERROR_DUPLICATE_QUESTION],
           str(VALIDATOR.validate({"question": "Redis为什么快", "topic": ""},
                                  {"asked_questions": HISTORY}).errors))
    _check("question 为空时不报 duplicate_question（避免重复报错）",
           VALIDATOR.validate({"question": "", "topic": "T"},
                              {"asked_questions": ["", "Redis为什么快"]}).errors
           == [qv.ERROR_QUESTION_EMPTY])
    _check("重复检测不影响其它字段的补全",
           VALIDATOR.validate({"question": "Redis为什么这么快", "topic": "Redis",
                               "difficulty": "senior", "expected_points": ["A"],
                               "reason": "r", "question_type": "technical"},
                              {"asked_questions": HISTORY}).normalized_question
           == {"question": "Redis为什么这么快", "question_type": "technical", "topic": "Redis",
               "difficulty": "senior", "expected_points": ["A"], "reason": "r"})
    _check("重复检测具备确定性",
           [VALIDATOR.validate({"question": "Redis为什么这么快", "topic": "Redis",
                                "difficulty": "senior"},
                               {"asked_questions": HISTORY}).to_dict() for _ in range(3)]
           == [VALIDATOR.validate({"question": "Redis为什么这么快", "topic": "Redis",
                                   "difficulty": "senior"},
                                  {"asked_questions": HISTORY}).to_dict()] * 3)

    # ============================================================
    # [16] 标准化的纯函数性
    # ============================================================
    print("\n[16] 标准化的纯函数性")
    data_before = json.loads(json.dumps(VALID_DATA))
    plan_before = json.loads(json.dumps(SAMPLE_PLAN))
    VALIDATOR.validate(VALID_DATA, SAMPLE_CONTEXT, SAMPLE_PLAN)
    _check("标准化不修改 question_data 入参", VALID_DATA == data_before)
    _check("标准化不修改 plan 入参", SAMPLE_PLAN == plan_before)

    first_call = VALIDATOR.validate(VALID_DATA)
    second_call = VALIDATOR.validate(VALID_DATA)
    _check("两次调用的 normalized_question 不是同一 dict",
           first_call.normalized_question is not second_call.normalized_question)
    _check("两次调用的 expected_points 不是同一 list",
           first_call.normalized_question["expected_points"]
           is not second_call.normalized_question["expected_points"])
    _check("normalized_question 的列表不是入参列表的别名",
           first_call.normalized_question["expected_points"] is not VALID_DATA["expected_points"])

    first_call.normalized_question["expected_points"].append("外部篡改")
    first_call.normalized_question["difficulty"] = "hacked"
    _check("篡改返回值不影响入参", VALID_DATA == data_before)
    _check("篡改返回值不影响后续调用",
           VALIDATOR.validate(VALID_DATA).normalized_question == VALID_DATA)

    _check("同输入同输出（确定性）",
           [VALIDATOR.validate(VALID_DATA).to_dict() for _ in range(3)]
           == [VALIDATOR.validate(VALID_DATA).to_dict()] * 3)
    _check("warning 也具备确定性",
           [VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": ""}).warnings
            for _ in range(3)]
           == [VALIDATOR.validate({"question": "Q", "topic": "T",
                                   "difficulty": ""}).warnings] * 3)
    _check("不同实例产出相同结果",
           qv.QuestionValidator().validate(VALID_DATA, None, SAMPLE_PLAN).to_dict()
           == VALIDATOR.validate(VALID_DATA, None, SAMPLE_PLAN).to_dict())

    _check("校验不通过 → normalized_question 保持 {}（不产出半补全数据）",
           VALIDATOR.validate({"question": "", "topic": ""}).normalized_question == {})
    _check("校验不通过 → 即使其余字段齐全也不补全",
           VALIDATOR.validate({"question": "", "topic": "T", "difficulty": "senior",
                               "expected_points": ["A"], "reason": "r",
                               "question_type": "technical"}).normalized_question == {})
    _check("校验不通过 → 不产生 warning（未进入补全阶段）",
           VALIDATOR.validate({"question": "", "topic": "T"}).warnings == [])
    _check("校验不通过 → 仍登记错误码",
           VALIDATOR.validate({"question": "", "topic": "T"}).errors
           == [qv.ERROR_QUESTION_EMPTY])

    # ============================================================
    # [17] 待实现边界
    # ============================================================
    print("\n[17] 待实现边界")
    _check("未集成 Interview Agent（rule-4）：未 import 任何业务模块",
           not any(m.startswith("services.interview_") for m in imported))
    _check("后续实现计划仅剩 1 条 TODO（rule-1 / rule-2 / rule-3 已完成）",
           "TODO(rule-4)" in (qv.__doc__ or "")
           and all(f"TODO(rule-{i})" not in (qv.__doc__ or "") for i in (1, 2, 3)))
    _check("rule-2 已从 TODO 中移除（重复检测已实现）",
           "TODO(rule-2)" not in (qv.__doc__ or ""))
    _check("与 interview_agent 相似度算法暂时重复已在 docstring 说明（待 rule-4 统一）",
           "interview_agent.question_similarity" in (qv.__doc__ or "")
           and "统一到一处" in (qv.__doc__ or ""))
    _check("docstring 明确声明不使用 embedding / 向量数据库 / RAG",
           "embedding" in (qv.__doc__ or "")
           and "向量数据库" in (qv.__doc__ or "")
           and "RAG" in (qv.__doc__ or ""))
    _check("docstring 已说明本阶段四项能力",
           "基础字段校验" in (qv.__doc__ or "")
           and "字段补全" in (qv.__doc__ or "")
           and "difficulty 合法性校验" in (qv.__doc__ or "")
           and "问题重复检测" in (qv.__doc__ or "")
           and "fail-open" not in (qv.__doc__ or ""))
    _check("docstring 已登记标准化契约（六字段 / warning 规则）",
           "NORMALIZED_FIELDS" in (qv.__doc__ or "")
           and "warning 规则" in (qv.__doc__ or ""))
    _check("docstring 已登记 difficulty 处理流程（三种情况）",
           "difficulty 处理流程" in (qv.__doc__ or "")
           and "情况3" in (qv.__doc__ or ""))
    _check("docstring 已登记重复检测策略（完全相同拒绝 / 高度相似告警）",
           "重复检测策略" in (qv.__doc__ or "")
           and "直接拒绝" in (qv.__doc__ or "")
           and "只记 warning，不拒绝" in (qv.__doc__ or ""))
    _check("docstring 已登记词汇表统一结论（与 models.DIFFICULTIES 同源同值）",
           "models.DIFFICULTIES" in (qv.__doc__ or "")
           and "词汇表统一" in (qv.__doc__ or "")
           and "不设任何转换层" in (qv.__doc__ or ""))
    _check("docstring 不再残留词汇表分歧告警", "词汇表分歧" not in (qv.__doc__ or ""))
    _check("源码未 import models（允许集在模块内独立声明）", "models" not in imported)
    _check("源码未 import interview_agent（相似度算法独立实现，避免成环）",
           not any(m.startswith("services.interview_") for m in imported))

    # ============================================================
    # [18] 词汇表一致性守卫（防止 ALLOWED_DIFFICULTIES 与 models 漂移）
    # ============================================================
    print("\n[18] 词汇表一致性守卫")
    # 注意：本模块刻意不 import models，因此该元组是「字面重复声明」。
    # 这里用**子进程**同时导入两侧并断言相等——子进程会拉入 SQLAlchemy，
    # 放在主进程会破坏 [1] 节的「sqlalchemy 未进入 sys.modules」断言。
    guard_env = dict(os.environ)
    guard_env["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
    guard = subprocess.run(
        [sys.executable, "-c",
         "from models import DIFFICULTIES;"
         "from services.question_validator import ALLOWED_DIFFICULTIES;"
         "print('GUARD', tuple(DIFFICULTIES) == tuple(ALLOWED_DIFFICULTIES),"
         " tuple(DIFFICULTIES), tuple(ALLOWED_DIFFICULTIES))"],
        cwd=str(BACKEND_DIR), env=guard_env, capture_output=True, text=True,
    )
    _check("子进程同时导入 models 与 validator 成功", guard.returncode == 0,
           (guard.stderr or "").strip()[-300:])
    _check("ALLOWED_DIFFICULTIES == models.DIFFICULTIES（词汇表同源同值）",
           "GUARD True" in guard.stdout, guard.stdout.strip()[:200])
    _check("子进程回显的取值就是 junior/mid/senior",
           "('junior', 'mid', 'senior') ('junior', 'mid', 'senior')" in guard.stdout,
           guard.stdout.strip()[:200])

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
