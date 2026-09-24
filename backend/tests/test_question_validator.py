# -*- coding: utf-8 -*-
"""AI 模拟面试 · QuestionValidator 基础字段校验自检

无需 pytest，直接运行：
    python backend/tests/test_question_validator.py

**本文件刻意不设置 DATABASE_URL**，以此证明被测模块不依赖数据库配置。
被测对象是 ``services.question_validator``（当前只实现 question / topic 基础校验）。

覆盖范围：
1. 依赖约束：零第三方依赖、导入后 fastapi/sqlalchemy 未进入 sys.modules
2. 独立性：在无 DATABASE_URL 的子进程中也能导入（不依赖数据库）
3. ValidationResult：字段、默认值、to_dict、副本语义、不变量
4. ValidationError：继承关系、与 pydantic.ValidationError 的区别
5. QuestionValidator：可无参构造、validate 可调用（1/2/3 个参数）、返回类型
6. **question 校验**：正常 / 空串 / 纯空白 / 缺失 / None / 非字符串
7. **topic 校验**：同上
8. **question 与 topic 同时为空**：两条错误码都要登记（不短路）
9. 明确不校验的字段：difficulty / question_type / expected_points / reason
10. 错误码契约：常量取值、机器可读性、无重复、valid 不变量
11. 无副作用：不篡改入参、可重复调用、结果互不共享
12. 待实现边界：rule-1..4 确实尚未实现
"""

import ast
import inspect
import json
import os
import pathlib
import re
import subprocess
import sys

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
    "difficulty": "mid",
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
    "difficulty": "mid",
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
           imported <= {"__future__", "collections", "dataclasses", "typing"},
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
    _check("[用例1] question 正常 → normalized_question 为空（未实现补全）",
           ok.normalized_question == {})

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
    _check("两键都缺失 → 两条错误码",
           VALIDATOR.validate({"difficulty": "mid"}).errors
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
    # [9] 明确不校验的字段
    # ============================================================
    print("\n[9] 本阶段明确不校验的字段")
    _check("difficulty 缺失 → 不影响 valid",
           VALIDATOR.validate({"question": "Q", "topic": "T"}).valid is True)
    _check("difficulty 非法取值 → 不影响 valid",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": "god_mode"}).valid
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
    _check("只提供 question 与 topic 即通过",
           VALIDATOR.validate({"question": "Q", "topic": "T"}).to_dict()
           == {"valid": True, "errors": [], "warnings": [], "normalized_question": {}})

    # ============================================================
    # [10] 错误码契约
    # ============================================================
    print("\n[10] 错误码契约")
    _check("ERROR_QUESTION_EMPTY == 'question_empty'",
           qv.ERROR_QUESTION_EMPTY == "question_empty", qv.ERROR_QUESTION_EMPTY)
    _check("ERROR_TOPIC_EMPTY == 'topic_empty'",
           qv.ERROR_TOPIC_EMPTY == "topic_empty", qv.ERROR_TOPIC_EMPTY)
    for code in (qv.ERROR_QUESTION_EMPTY, qv.ERROR_TOPIC_EMPTY):
        _check(f"错误码 {code!r} 为机器可读蛇形（纯小写 ASCII + 下划线）",
               bool(re.fullmatch(r"[a-z]+(_[a-z]+)*", code)), code)
        _check(f"错误码 {code!r} 不含空格或中文",
               code.isascii() and " " not in code)
    _check("两个错误码互不相同", qv.ERROR_QUESTION_EMPTY != qv.ERROR_TOPIC_EMPTY)
    # 注意：__all__ 里放的是**常量名**（字符串形式的标识符），不是常量值。
    # 因此这里断言标识符可见，再断言标识符能取回期望的错误码值。
    _check("错误码常量名出现在 __all__ 中（对外契约可见）",
           "ERROR_QUESTION_EMPTY" in qv.__all__ and "ERROR_TOPIC_EMPTY" in qv.__all__,
           str(qv.__all__))
    _check("__all__ 中的错误码常量名可解析为预期错误码值",
           getattr(qv, "ERROR_QUESTION_EMPTY") == "question_empty"
           and getattr(qv, "ERROR_TOPIC_EMPTY") == "topic_empty")
    _check("__all__ 导出 6 个公开名",
           qv.__all__ == ["ERROR_QUESTION_EMPTY", "ERROR_TOPIC_EMPTY", "ValidationResult",
                          "QuestionValidatorError", "ValidationError", "QuestionValidator"],
           str(qv.__all__))
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
    _check("context / plan 任意取值都不影响结果（当前规则不使用它们）",
           VALIDATOR.validate({"question": "Q", "topic": "T"}).to_dict()
           == VALIDATOR.validate({"question": "Q", "topic": "T"}, object(), object()).to_dict())

    # ============================================================
    # [12] 待实现边界
    # ============================================================
    print("\n[12] 待实现边界")
    _check("difficulty 校验尚未实现（rule-1）",
           VALIDATOR.validate({"question": "Q", "topic": "T", "difficulty": "nope"}).valid
           is True)
    _check("重复检测尚未实现（rule-2）：与历史重复也通过",
           VALIDATOR.validate({"question": "请做一下自我介绍", "topic": "自我介绍"},
                              SAMPLE_CONTEXT, SAMPLE_PLAN).valid is True)
    _check("字段补全尚未实现（rule-3）：normalized_question 仍为空",
           VALIDATOR.validate({"question": "Q", "topic": "T"}).normalized_question == {})
    _check("未集成 Interview Agent（rule-4）：未 import 任何业务模块",
           not any(m.startswith("services.interview_") for m in imported))
    _check("后续实现计划已登记 4 条 TODO",
           all(f"TODO(rule-{i})" in (qv.__doc__ or "") for i in (1, 2, 3, 4)))
    _check("已实现规则在 docstring 中说明（不再标注为 fail-open 骨架）",
           "基础字段校验" in (qv.__doc__ or "")
           and "fail-open" not in (qv.__doc__ or ""))

    print("\n" + "=" * 70)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 70)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
