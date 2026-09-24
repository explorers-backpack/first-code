# -*- coding: utf-8 -*-
"""岗位技能匹配（keyword_match 去重合并）—— 自检脚本

无需 pytest，直接运行：
    python backend/tests/test_job_matching.py

背景
----
``main.py`` 曾存在两份**逐字节相同**的 ``keyword_match`` 定义（后者覆盖前者）。
本次将其合并为唯一实现并迁移至 ``services/job_matching.py``。本脚本用于保证：

1. 合并后的唯一实现，其行为与旧实现**完全一致**（不破坏原有岗位匹配功能）；
2. 覆盖需求点名的 8 类用例：
   正常技能匹配 / 多技能匹配 / 空字符串 / None / 大小写不同 /
   技能不存在 / 完全匹配 / 部分匹配；
3. ``main.py`` 不再保留同名定义，而是引用同一份实现（无影子函数）。
"""

import asyncio
import json
import os
import pathlib
import sys

_BACKEND = str(pathlib.Path(__file__).resolve().parents[1])
sys.path.insert(0, _BACKEND)

# 必须先于 `import main` 设置：main.py 会在导入期用 DATABASE_URL 构造 engine。
# 用 setdefault 而非直接赋值，避免覆盖真实 .env 中的配置；
# load_dotenv 默认 override=False，不会反向覆盖此处的兜底值。
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from services.job_matching import (  # noqa: E402
    keyword_match,
    match_job,
    split_job_skills,
)
from services import job_matching  # noqa: E402

import main  # noqa: E402


# ============================================================
# 测试数据
# ============================================================
USER_SKILLS = ["Python", "MySQL", "Redis", "Docker"]

JOBS = [
    {
        "id": 1,
        "job_name": "后端开发工程师",
        "city": "深圳",
        "salary": "20-35K",
        "skills": "Python,Java,Go,MySQL",
    },
    {
        "id": 2,
        "job_name": "前端开发工程师",
        "city": "杭州",
        "salary": "15-25K",
        "skills": "Vue,React",
    },
    {
        "id": 3,
        "job_name": "数据开发工程师",
        "city": "北京",
        "salary": "25-40K",
        "skills": "Python,MySQL,Redis",
    },
]

ZERO = {
    "match_rate": 0,
    "matched": [],
    "missing": [],
    "total_required": 0,
    "matched_count": 0,
}


def _check(name: str, cond: bool, detail: str = "") -> bool:
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (f"  -> {detail}" if detail and not cond else ""))
    return cond


# ============================================================
# 1. 正常技能匹配（单技能全命中）
# ============================================================
def test_normal_match() -> bool:
    print("\n[1] 正常技能匹配（单技能全命中）")
    r = keyword_match(["Python"], "Python")
    ok = _check(f"命中率 100.0（实际 {r['match_rate']}）", r["match_rate"] == 100.0)
    ok &= _check(f"matched == ['python']（实际 {r['matched']}）", r["matched"] == ["python"])
    ok &= _check(f"missing 为空（实际 {r['missing']}）", r["missing"] == [])
    ok &= _check("total_required == 1 且 matched_count == 1",
                 r["total_required"] == 1 and r["matched_count"] == 1)
    return ok


# ============================================================
# 2. 多技能匹配（部分命中）
# ============================================================
def test_multi_skill_match() -> bool:
    print("\n[2] 多技能匹配（6 项要求命中 3 项）")
    r = keyword_match(USER_SKILLS, "Python,Java,Go,MySQL,Redis,Kafka")
    ok = _check(f"命中率 50.0（实际 {r['match_rate']}）", r["match_rate"] == 50.0)
    ok &= _check(f"matched 三项（实际 {r['matched']}）",
                 r["matched"] == ["python", "mysql", "redis"])
    ok &= _check(f"missing 三项（实际 {r['missing']}）",
                 r["missing"] == ["java", "go", "kafka"])
    ok &= _check(f"total_required == 6（实际 {r['total_required']}）", r["total_required"] == 6)
    ok &= _check(f"matched_count == 3（实际 {r['matched_count']}）", r["matched_count"] == 3)
    ok &= _check("matched_count + len(missing) == total_required",
                 r["matched_count"] + len(r["missing"]) == r["total_required"])
    return ok


# ============================================================
# 3. 完全匹配（exact match）
# ============================================================
def test_exact_match() -> bool:
    print("\n[3] 完全匹配（技能集合与要求逐项一致）")
    r = keyword_match(["Python", "MySQL", "Redis"], "Python,MySQL,Redis")
    ok = _check(f"命中率 100.0（实际 {r['match_rate']}）", r["match_rate"] == 100.0)
    ok &= _check("missing 为空", r["missing"] == [])
    ok &= _check(f"matched 顺序与岗位串一致（实际 {r['matched']}）",
                 r["matched"] == ["python", "mysql", "redis"])
    return ok


# ============================================================
# 4. 部分匹配（partial match）
# ============================================================
def test_partial_match() -> bool:
    print("\n[4] 部分匹配（2 项要求命中 1 项）")
    r = keyword_match(["Python"], "Python,Java")
    ok = _check(f"命中率 50.0（实际 {r['match_rate']}）", r["match_rate"] == 50.0)
    ok &= _check(f"matched == ['python']（实际 {r['matched']}）", r["matched"] == ["python"])
    ok &= _check(f"missing == ['java']（实际 {r['missing']}）", r["missing"] == ["java"])
    return ok


# ============================================================
# 5. 大小写不同（case-insensitive）
# ============================================================
def test_case_insensitive() -> bool:
    print("\n[5] 大小写不同（两侧均归一化为小写）")
    r1 = keyword_match(["PYTHON", "mysql"], "Python, MySQL")
    ok = _check(f"大小写混合仍全命中（实际 {r1['match_rate']}）", r1["match_rate"] == 100.0)
    ok &= _check(f"输出统一小写（实际 {r1['matched']}）", r1["matched"] == ["python", "mysql"])

    r2 = keyword_match(["PyThOn"], "python")
    ok &= _check("单侧大写亦可命中", r2["match_rate"] == 100.0)

    r3 = keyword_match(["python"], "PYTHON")
    ok &= _check("岗位侧大写亦可命中", r3["match_rate"] == 100.0)
    return ok


# ============================================================
# 6. 技能不存在（零命中）
# ============================================================
def test_no_skill_match() -> bool:
    print("\n[6] 技能不存在（0 命中）")
    r = keyword_match(["Cooking", "Painting"], "Python,Java")
    ok = _check(f"命中率 0（实际 {r['match_rate']}）", r["match_rate"] == 0)
    ok &= _check("matched 为空", r["matched"] == [])
    ok &= _check(f"missing 为全部要求（实际 {r['missing']}）", r["missing"] == ["python", "java"])
    ok &= _check(f"total_required == 2（实际 {r['total_required']}）", r["total_required"] == 2)
    ok &= _check(f"matched_count == 0（实际 {r['matched_count']}）", r["matched_count"] == 0)
    return ok


# ============================================================
# 7. 空字符串 / 空列表（零值早返回）
# ============================================================
def test_empty_inputs() -> bool:
    print("\n[7] 空字符串 / 空列表")
    cases = [
        ("岗位技能为空串", keyword_match(["Python"], "")),
        ("用户技能为空列表", keyword_match([], "Python")),
        ("两侧均为空", keyword_match([], "")),
        ("岗位技能为空串 + 用户技能为空", keyword_match([], "")),
    ]
    ok = True
    for name, r in cases:
        ok &= _check(f"{name} -> 零值结果", r == ZERO, str(r))
    return ok


# ============================================================
# 8. None（零值早返回，且不抛异常）
# ============================================================
def test_none_inputs() -> bool:
    print("\n[8] None 入参（不得抛异常）")
    ok = True
    try:
        r1 = keyword_match(["Python"], None)
        ok &= _check("job_skills_str=None -> 零值结果", r1 == ZERO, str(r1))
    except Exception as exc:  # noqa: BLE001
        ok &= _check("job_skills_str=None 不抛异常", False, repr(exc))

    try:
        r2 = keyword_match(None, "Python")
        ok &= _check("user_skills=None -> 零值结果", r2 == ZERO, str(r2))
    except Exception as exc:  # noqa: BLE001
        ok &= _check("user_skills=None 不抛异常", False, repr(exc))

    try:
        r3 = keyword_match(None, None)
        ok &= _check("两侧均为 None -> 零值结果", r3 == ZERO, str(r3))
    except Exception as exc:  # noqa: BLE001
        ok &= _check("两侧均为 None 不抛异常", False, repr(exc))
    return ok


# ============================================================
# 9. 空格与分隔符容错
# ============================================================
def test_whitespace() -> bool:
    print("\n[9] 空格与分隔符容错")
    r = keyword_match(["  Python  "], " Python , MySQL ")
    ok = _check(f"两侧空格被剥离（实际 {r['matched']}）", r["matched"] == ["python"])
    ok &= _check(f"未命中项亦去空格（实际 {r['missing']}）", r["missing"] == ["mysql"])
    ok &= _check(f"命中率 50.0（实际 {r['match_rate']}）", r["match_rate"] == 50.0)

    # 历史行为：仅由分隔符/空白构成时，会拆出空串，空串被任意技能包含 -> 100%
    # 本次刻意保留该行为（不改变既有业务表现），此处显式记录。
    r2 = keyword_match(["Python"], " , ")
    ok &= _check(f"仅空白分隔符 -> 保留历史 100% 行为（实际 {r2['match_rate']}）",
                 r2["match_rate"] == 100.0)
    r3 = keyword_match(["Python"], "Python,")
    ok &= _check(f"尾随逗号 -> 保留历史行为（total={r3['total_required']}）",
                 r3["total_required"] == 2 and r3["match_rate"] == 100.0)
    return ok


# ============================================================
# 10. 特殊字符与双向子串规则
# ============================================================
def test_special_chars() -> bool:
    print("\n[10] 特殊字符与双向子串规则")
    r = keyword_match(["C++", "Node.js"], "C++,Node.js")
    ok = _check(f"C++ / Node.js 正常匹配（实际 {r['match_rate']}）", r["match_rate"] == 100.0)

    r2 = keyword_match(["Node"], "Node.js")
    ok &= _check("用户技能为岗位技能子串 -> 命中（双向子串规则）", r2["match_rate"] == 100.0)

    r3 = keyword_match(["JavaScript"], "Java")
    ok &= _check("岗位技能为用户技能子串 -> 命中（历史行为，java ⊆ javascript）",
                 r3["match_rate"] == 100.0)
    return ok


# ============================================================
# 11. 返回契约与确定性
# ============================================================
def test_contract_and_determinism() -> bool:
    print("\n[11] 返回契约与确定性")
    r = keyword_match(USER_SKILLS, "Python,Java,Go,MySQL,Redis,Kafka")
    expected_keys = {"match_rate", "matched", "missing", "total_required", "matched_count"}
    ok = _check(f"键集合恰为 5 项（实际 {sorted(r)}）", set(r) == expected_keys)
    ok &= _check("match_rate 为数值", isinstance(r["match_rate"], (int, float)))
    ok &= _check("matched / missing 均为 list",
                 isinstance(r["matched"], list) and isinstance(r["missing"], list))

    a = json.dumps(keyword_match(USER_SKILLS, "Python,Java"), sort_keys=True)
    b = json.dumps(keyword_match(USER_SKILLS, "Python,Java"), sort_keys=True)
    ok &= _check("同输入两次调用结果逐字节一致", a == b)

    # 每次返回独立对象，互不影响（避免可变默认值被共享）
    r1 = keyword_match(["Python"], "")
    r2 = keyword_match(["Python"], "")
    r1["matched"].append("__mutated__")
    ok &= _check("返回值不共享可变对象", r2["matched"] == [])
    return ok


# ============================================================
# 12. split_job_skills 行为
# ============================================================
def test_split_job_skills() -> bool:
    print("\n[12] split_job_skills 拆分行为")
    ok = _check(f"常规拆分（实际 {split_job_skills('Python, MySQL')}）",
                split_job_skills("Python, MySQL") == ["python", "mysql"])
    ok &= _check("空串 -> []", split_job_skills("") == [])
    ok &= _check("None -> []", split_job_skills(None) == [])
    ok &= _check(f"不过滤空串（实际 {split_job_skills('python,,')}）",
                 split_job_skills("python,,") == ["python", "", ""])
    return ok


# ============================================================
# 13. match_job 岗位摘要包装
# ============================================================
def test_match_job() -> bool:
    print("\n[13] match_job 岗位摘要包装")
    r = match_job(["Python"], JOBS[0])
    ok = _check(f"job_id 透传（实际 {r['job_id']}）", r["job_id"] == 1)
    ok &= _check(f"job_name 透传（实际 {r['job_name']}）", r["job_name"] == "后端开发工程师")
    ok &= _check(f"city / salary 透传", r["city"] == "深圳" and r["salary"] == "20-35K")
    ok &= _check("keyword_match 为嵌套结果",
                 r["keyword_match"]["match_rate"] == 25.0 and
                 r["keyword_match"]["matched"] == ["python"])
    ok &= _check("字段集合与 filter_jobs_by_keywords 内联结构一致",
                 set(r) == {"job_id", "job_name", "city", "salary", "keyword_match"})

    # 岗位未标注 skills / job 为 None，均应安全降级
    r2 = match_job(["Python"], {"id": 9, "job_name": "无技能岗位"})
    ok &= _check("岗位无 skills 字段 -> 零值匹配", r2["keyword_match"] == ZERO)
    r3 = match_job(["Python"], None)
    ok &= _check("job=None -> 不抛异常且零值匹配",
                 r3["keyword_match"] == ZERO and r3["job_id"] is None)
    return ok


# ============================================================
# 14. 集成：main.py 使用唯一实现 + filter_jobs_by_keywords 行为不变
# ============================================================
def test_main_uses_single_implementation() -> bool:
    print("\n[14] 集成：main.py 引用唯一实现，且匹配/排序/阈值行为不变")
    ok = _check("main.keyword_match 即 services.job_matching.keyword_match",
                main.keyword_match is job_matching.keyword_match)

    # 统计 main 模块中名为 keyword_match 的定义数量（应为 0：已无本地定义）
    local_defs = [
        v for k, v in vars(main).items()
        if k == "keyword_match" and getattr(v, "__module__", None) == "main"
    ]
    ok &= _check(f"main 模块内无本地 keyword_match 定义（实际 {len(local_defs)}）",
                 len(local_defs) == 0)

    # 用假数据替换 get_all_jobs，验证过滤 + 降序排序
    async def _fake_get_all_jobs():
        return JOBS

    original = main.get_all_jobs
    main.get_all_jobs = _fake_get_all_jobs
    try:
        results = asyncio.run(main.filter_jobs_by_keywords(["Python", "MySQL"], min_match_rate=30))
    finally:
        main.get_all_jobs = original

    ok &= _check(f"低于阈值(30)的前端岗位被过滤（实际 {[r['job_id'] for r in results]}）",
                 [r["job_id"] for r in results] == [3, 1])
    ok &= _check(f"按命中率降序（实际 {[r['keyword_match']['match_rate'] for r in results]}）",
                 [r["keyword_match"]["match_rate"] for r in results] == [66.7, 50.0])
    ok &= _check("结果项结构完整",
                 all(set(r) == {"job_id", "job_name", "city", "salary", "keyword_match"}
                     for r in results))

    # clean_jobs_to_text 仍可消费该结构（沿用 keyword_match 键）
    text = main.clean_jobs_to_text(results)
    ok &= _check("clean_jobs_to_text 正常渲染", "数据开发工程师" in text and "66.7%" in text)
    return ok


def main_run() -> int:
    print("=" * 68)
    print("岗位技能匹配（keyword_match 合并）· 自检")
    print("=" * 68)

    results = [
        test_normal_match(),
        test_multi_skill_match(),
        test_exact_match(),
        test_partial_match(),
        test_case_insensitive(),
        test_no_skill_match(),
        test_empty_inputs(),
        test_none_inputs(),
        test_whitespace(),
        test_special_chars(),
        test_contract_and_determinism(),
        test_split_job_skills(),
        test_match_job(),
        test_main_uses_single_implementation(),
    ]

    passed = sum(1 for x in results if x)
    print("\n" + "=" * 68)
    print(f"结果: {passed}/{len(results)} 组通过")
    print("=" * 68)
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main_run())
