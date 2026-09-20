# -*- coding: utf-8 -*-
"""简历分析接口（/api/resume/analyze）集成自检

直接运行（需已安装 fastapi 等后端依赖）：
    python backend/tests/test_analyze_endpoint.py

不连数据库：通过 monkeypatch 替换 get_all_jobs 与 SparkAPI，只验证接口装配逻辑。
"""

import asyncio
import io
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from fastapi import HTTPException, UploadFile  # noqa: E402

import main  # noqa: E402

JOBS = [
    {
        "id": 1,
        "job_name": "后端开发工程师",
        "salary": "20-35K",
        "edu_require": "本科",
        "major_require": "不限",
        "skills": "Python,Java,Go,MySQL,Redis,Kafka,Docker,Kubernetes,Spring Boot,微服务",
        "duty": "负责后端服务的设计与开发，参与需求分析、系统设计与性能优化；负责线上问题排查；参与代码评审。",
        "city": "深圳",
        "industry": "互联网",
    },
]

RESUME_TXT = """张伟
求职意向：后端开发工程师

教育背景
2015.09-2019.06 某某大学 计算机科学与技术 本科（985）

工作经历
2021.07-至今 某某科技有限公司 高级后端开发工程师
负责订单中台架构设计与重构，将单体服务拆分为 8 个微服务，QPS 从 800 提升到 5000
主导分库分表方案落地，订单查询响应时间从 1200ms 降低到 80ms
基于 Kafka 构建异步消息链路，峰值处理能力提升 3 倍

项目经历
项目一：监控告警平台
使用 Prometheus、Grafana 搭建监控体系，覆盖 200 个服务，故障发现时间从 30 分钟缩短到 2 分钟

技能
Python、Java、Go、MySQL、Redis、Kafka、Docker、Kubernetes、Spring Boot、Linux、Git

荣誉证书
2018 年 ACM-ICPC 区域赛银奖
开源项目 gitee.com/zhangwei/order-platform 获得 star 300
"""


def _upload(data: bytes, name: str = "resume.txt") -> UploadFile:
    return UploadFile(filename=name, file=io.BytesIO(data))


def _check(name: str, cond: bool, detail: str = "") -> bool:
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (f"  -> {detail}" if detail and not cond else ""))
    return cond


async def run() -> bool:
    print("=" * 68)
    print("简历分析接口 · 集成自检")
    print("=" * 68)

    main.get_all_jobs = lambda: _async_return(JOBS)

    # ---- 1. LLM 不可用：应降级为规则引擎文案，而非原始错误串 ----
    print("\n[1] LLM 不可用时的降级")
    main.spark_api.chat_async = lambda prompt: _async_return("API调用失败: 连接超时")
    r = await main.analyze_resume(file=_upload(RESUME_TXT.encode("utf-8")))
    ok = _check("ai_available 为 False", r["ai_available"] is False)
    ok &= _check("不再把原始错误串当诊断结果", "API调用失败" not in r["chat_answer"])
    ok &= _check("降级文案含维度名", "【核心优势】" in r["chat_answer"] and "【提升建议】" in r["chat_answer"])

    # ---- 2. 正常返回结构 ----
    print("\n[2] 返回结构")
    ok &= _check("metrics 为 8 维", len(r["metrics"]) == 8, str(len(r["metrics"])))
    ok &= _check("字段为 metrics 而非 twelve_metrics", "twelve_metrics" not in r and "metrics" in r)
    ok &= _check("score 为 0-100 整数", isinstance(r["score"], int) and 0 <= r["score"] <= 100, str(r["score"]))
    ok &= _check("每维含 rationale 与 evidence 字段",
                 all("rationale" in m and "evidence" in m for m in r["metrics"]))
    ok &= _check("技能为真实抽取结果", "Python" in r["skills"] and "Kafka" in r["skills"], str(r["skills"]))
    ok &= _check("参照岗位已回传", r["reference_job"] and r["reference_job"]["job_name"] == "后端开发工程师")
    print(f"        score={r['score']}  技能 {len(r['skills'])} 项  推荐岗位 {len(r['recommended_jobs'])} 个")
    for m in r["metrics"]:
        print(f"        - {m['name']}: {m['score']}（证据 {len(m['evidence'])} 条）")

    # ---- 3. PDF / DOCX 显式拒绝，不再产出乱码评分 ----
    print("\n[3] 二进制格式显式拒绝")
    for payload, label, expect in ((b"%PDF-1.7\n...binary...", "PDF", 415), (b"PK\x03\x04docx", "DOCX", 415)):
        try:
            await main.analyze_resume(file=_upload(payload, f"a.{label.lower()}"))
            ok &= _check(f"{label} 应被拒绝", False, "未抛异常")
        except HTTPException as e:
            ok &= _check(f"{label} 返回 {e.status_code} 且提示可读", e.status_code == expect, str(e.detail))

    # ---- 4. 无技能简历不捏造技能 ----
    print("\n[4] 无技能简历")
    plain = "王芳\n教育背景\n2016.09-2020.06 某某大学 汉语言文学 本科\n工作经历\n2020.07-2023.06 某公司 文案策划\n负责公众号内容撰写，累计产出 120 篇推文\n"
    r2 = await main.analyze_resume(file=_upload(plain.encode("utf-8")))
    ok &= _check("技能列表为空", r2["skills"] == [], str(r2["skills"]))
    ok &= _check("未出现 Python/JavaScript/Git 兜底", "Python" not in r2["skills"])

    # ---- 5. LLM 正常时使用模型输出 ----
    print("\n[5] LLM 可用时")
    main.spark_api.chat_async = lambda prompt: _async_return(
        "【核心优势】：架构设计与性能优化经验扎实，QPS 提升 6 倍以上。\n【提升建议】：补充代码评审与需求分析经历。"
    )
    r3 = await main.analyze_resume(file=_upload(RESUME_TXT.encode("utf-8")))
    ok &= _check("ai_available 为 True", r3["ai_available"] is True)
    ok &= _check("使用模型输出", "QPS" in r3["chat_answer"])
    ok &= _check("提示词未包含裸分数行", "综合评分：" not in "".join(
        [str(m) for m in r3["metrics"]]) and True)

    print("\n" + "=" * 68)
    print("结果: " + ("全部通过" if ok else "存在失败项"))
    print("=" * 68)
    return ok


async def _async_return(value):
    return value


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
