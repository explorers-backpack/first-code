# -*- coding: utf-8 -*-
"""简历 8 维可取证评分 —— 自检脚本

无需 pytest，直接运行：
    python backend/tests/test_resume_scoring.py

覆盖的验收口径：
1. 确定性：同一份简历两次评分结果完全一致（可复现）
2. 证据可回溯：每条 evidence 原句都能在简历原文中检索到（不编造证据）
3. 不再捏造技能：无技能简历返回空列表，不填充 Python/JavaScript/Git
4. 词边界正确：JavaScript 不产生 Java，Django 不产生 Go
5. 维度有效性：技能清单相同但内容不同的两份简历，维度分与雷达数据不同
6. 结构完整：8 个维度齐全、权重和为 1.0
"""

import json
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from services.resume_scoring import DIMENSION_META, evaluate_resume, extract_skills  # noqa: E402

# ============================================================
# 测试数据
# ============================================================
JOBS = [
    {
        "id": 1,
        "job_name": "后端开发工程师",
        "salary": "20-35K",
        "edu_require": "本科",
        "skills": "Python,Java,Go,MySQL,Redis,Kafka,Docker,Kubernetes,Spring Boot,微服务",
        "duty": "负责后端服务的设计与开发，参与需求分析、系统设计、接口设计与性能优化；"
                "负责线上问题排查与故障处理；参与代码评审，保障系统高可用。",
        "city": "深圳",
        "industry": "互联网",
    },
    {
        "id": 2,
        "job_name": "前端开发工程师",
        "salary": "15-25K",
        "edu_require": "本科",
        "skills": "JavaScript,TypeScript,Vue,React,CSS,HTML,Webpack",
        "duty": "负责前端页面开发与性能优化，参与需求评审与产品设计。",
        "city": "杭州",
        "industry": "互联网",
    },
]

RESUME_STRONG = """张伟
求职意向：后端开发工程师
手机：13800000000 邮箱：zhangwei@example.com

教育背景
2015.09-2019.06 某某大学 计算机科学与技术 本科（985）
主修课程：数据结构、操作系统、数据库原理

工作经历
2021.07-至今 某某科技有限公司 高级后端开发工程师
负责订单中台架构设计与重构，将单体服务拆分为 8 个微服务，QPS 从 800 提升到 5000
主导分库分表方案落地，订单查询响应时间从 1200ms 降低到 80ms
基于 Kafka 构建异步消息链路，峰值处理能力提升 3 倍
推动 Docker 与 Kubernetes 容器化改造，发布效率提升 60%
2019.07-2021.06 某某网络科技公司 后端开发工程师
参与支付系统开发，日均交易量 200 万笔，系统可用性达到 99.99%

项目经历
项目一：智能推荐平台
独立负责推荐系统召回层开发，使用 Python、Spark、Redis，服务 50 万日活用户，点击率提升 12%
项目二：监控告警平台
使用 Prometheus、Grafana 搭建监控体系，覆盖 200 个服务，故障发现时间从 30 分钟缩短到 2 分钟

技能
Python、Java、Go、MySQL、Redis、Kafka、Docker、Kubernetes、Spring Boot、Spark、Linux、Git

荣誉证书
2018 年 ACM-ICPC 区域赛银奖
AWS 认证解决方案架构师
开源项目 gitee.com/zhangwei/order-platform 获得 star 300

自我评价
具备从0到1搭建系统的经验，熟悉高并发场景下的性能优化与稳定性治理。
"""

# 与 STRONG 完全相同的技能清单，但没有任何项目/经历正文
RESUME_SKILLS_ONLY = """李娜
教育背景
2018.09-2022.06 某某学院 软件工程 本科

技能
Python、Java、Go、MySQL、Redis、Kafka、Docker、Kubernetes、Spring Boot、Spark、Linux、Git
"""

# 无任何技术技能的简历
RESUME_NO_SKILLS = """王芳
教育背景
2016.09-2020.06 某某大学 汉语言文学 本科

工作经历
2020.07-2023.06 某某文化传媒公司 文案策划
负责品牌公众号内容撰写，累计产出 120 篇推文，平均阅读量 8000
"""

# 词边界陷阱
RESUME_BOUNDARY = """赵强
技能
JavaScript、TypeScript、Django、MongoDB、Node.js
"""


def _norm(s: str) -> str:
    """去掉所有空白与标点，用于证据回溯比对。"""
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]", "", s)


def _check(name: str, cond: bool, detail: str = "") -> bool:
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (f"  -> {detail}" if detail and not cond else ""))
    return cond


def test_determinism() -> bool:
    print("\n[1] 确定性：同一输入两次评分结果完全一致")
    a = json.dumps(evaluate_resume(RESUME_STRONG, JOBS), ensure_ascii=False, sort_keys=True)
    b = json.dumps(evaluate_resume(RESUME_STRONG, JOBS), ensure_ascii=False, sort_keys=True)
    return _check("两次结果逐字节一致", a == b)


def test_evidence_traceable() -> bool:
    print("\n[2] 证据可回溯：每条 evidence 都能在原文中检索到")
    result = evaluate_resume(RESUME_STRONG, JOBS)
    source = _norm(RESUME_STRONG)
    bad = []
    total = 0
    for m in result["metrics"]:
        for e in m["evidence"]:
            total += 1
            if _norm(e["text"]) not in source:
                bad.append(f"{m['name']}: {e['text']}")
    ok = _check(f"{total} 条证据全部可在原文定位", not bad, "; ".join(bad[:3]))
    ok &= _check("每维均给出依据说明", all(m["rationale"] for m in result["metrics"]))
    return ok


def test_no_fabricated_skills() -> bool:
    print("\n[3] 不再捏造技能")
    skills = extract_skills(RESUME_NO_SKILLS)
    ok = _check(f"无技能简历返回空列表（实际 {skills}）", skills == [])
    result = evaluate_resume(RESUME_NO_SKILLS, JOBS)
    ok &= _check("技能匹配度得 0 分且说明原因", result["metrics"][0]["score"] == 0)
    return ok


def test_word_boundary() -> bool:
    print("\n[4] 词边界：子串不再产生假阳性")
    skills = extract_skills(RESUME_BOUNDARY)
    ok = _check(f"JavaScript 不产生 Java（实际 {skills}）", "Java" not in skills)
    ok &= _check("Django 不产生 Go", "Go" not in skills)
    ok &= _check("Node.js 被正确识别", "JavaScript" in skills and "TypeScript" in skills)
    return ok


def test_same_skills_different_content() -> bool:
    print("\n[5] 维度有效性：技能清单相同、内容不同 -> 分数必须不同")
    a = evaluate_resume(RESUME_STRONG, JOBS)
    b = evaluate_resume(RESUME_SKILLS_ONLY, JOBS)
    va = [m["score"] for m in a["metrics"]]
    vb = [m["score"] for m in b["metrics"]]
    ok = _check(f"维度向量不同\n       有内容: {va}\n       仅清单: {vb}", va != vb)
    ok &= _check(f"综合分不同（{a['score']} vs {b['score']}）", a["score"] != b["score"])
    ok &= _check("仅技能清单者：项目复杂度为 0", vb[2] == 0, f"实际 {vb[2]}")
    ok &= _check("仅技能清单者：量化成果密度为 0", vb[3] == 0, f"实际 {vb[3]}")
    ok &= _check("仅技能清单者：技术栈深度低于有内容者", vb[4] < va[4], f"{vb[4]} vs {va[4]}")
    return ok


def test_structure() -> bool:
    print("\n[6] 结构完整：8 维齐全、权重和为 1.0")
    result = evaluate_resume(RESUME_STRONG, JOBS)
    ok = _check(f"维度数为 8（实际 {len(result['metrics'])}）", len(result["metrics"]) == 8)
    names = [m["name"] for m in result["metrics"]]
    ok &= _check("维度名称符合约定", names == ["技能匹配度", "岗位相关性", "项目复杂度", "量化成果密度",
                                              "技术栈深度", "成果可验证性", "经历连续性", "教育背景匹配"],
                 str(names))
    wsum = round(sum(m["weight"] for m in result["metrics"]), 6)
    ok &= _check(f"权重和为 1.0（实际 {wsum}）", wsum == 1.0)
    ok &= _check("每维含 key/name/weight/score/rationale/evidence",
                 all({"key", "name", "weight", "score", "rationale", "evidence"} <= set(m) for m in result["metrics"]))
    ok &= _check("综合分等于加权和",
                 result["score"] == round(sum(m["score"] * m["weight"] for m in result["metrics"])),)
    return ok


def main() -> int:
    print("=" * 68)
    print("简历 8 维可取证评分 · 自检")
    print("=" * 68)

    results = [
        test_determinism(),
        test_evidence_traceable(),
        test_no_fabricated_skills(),
        test_word_boundary(),
        test_same_skills_different_content(),
        test_structure(),
    ]

    print("\n" + "-" * 68)
    print("示例输出（RESUME_STRONG）")
    print("-" * 68)
    r = evaluate_resume(RESUME_STRONG, JOBS)
    print(f"综合评分: {r['score']}")
    print(f"参照岗位: {r['reference_job']}")
    print(f"识别技能({len(r['skills'])}): {'、'.join(r['skills'])}")
    print(f"段落识别: {r['sections']}")
    for m in r["metrics"]:
        print(f"\n  {m['name']}（权重 {m['weight']}）: {m['score']} 分")
        print(f"    依据: {m['rationale']}")
        for e in m["evidence"][:2]:
            print(f"    证据[{e['source']}]: {e['text']}")

    passed = sum(1 for x in results if x)
    print("\n" + "=" * 68)
    print(f"结果: {passed}/{len(results)} 组通过")
    print("=" * 68)
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
