# -*- coding: utf-8 -*-
"""补齐 jobs.duty 岗位职责数据

背景
----
`jobs.duty` 原为标签式短语（「后端API开发」7 字、「交互实现」4 字），不含任何职责
关键词，导致简历评分的「岗位相关性」维度无法比对、恒为 0 分。本脚本按岗位名称把
该字段替换为完整岗位职责描述。

数据来源说明
------------
以下职责文本是依据每个岗位已有的 `skills` 字段（技术栈）撰写的**通用岗位职责模板**，
不是抓取自真实招聘信息。如需真实 JD，请替换 `JOB_DUTIES` 后重新执行本脚本。

用法
----
    cd backend
    python scripts/update_job_duties.py --dry-run   # 仅预览，不写库
    python scripts/update_job_duties.py             # 实际写库

脚本幂等，可重复执行；`init_mysql.py` 重置数据后可再次运行恢复职责文本。
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select  # noqa: E402

import main  # noqa: E402

# 岗位名称 -> 完整岗位职责描述
JOB_DUTIES = {
    "Python开发工程师": (
        "负责后端 API 的设计与开发，参与需求分析与接口设计；"
        "负责数据库表结构设计与慢查询优化；"
        "参与代码评审与单元测试；"
        "负责线上问题排查与故障处理，保障服务高可用。"
    ),
    "Python后端开发": (
        "负责服务端接口与业务逻辑开发，参与系统设计与技术选型；"
        "负责缓存与消息队列的接入及性能调优；"
        "参与代码评审，编写接口文档；"
        "负责线上监控告警与故障排查。"
    ),
    "Django开发工程师": (
        "负责 Web 系统的后端开发与重构，参与需求评审与数据库设计；"
        "负责权限体系与后台管理模块开发；"
        "参与单元测试与代码评审；"
        "编写设计文档，保障线上运行稳定。"
    ),
    "前端开发工程师": (
        "负责前端页面与组件开发，参与需求评审与产品设计；"
        "负责前端性能优化与首屏加载优化；"
        "参与代码评审，推动通用组件库建设；"
        "配合移动端与小程序完成多端适配。"
    ),
    "Web前端开发": (
        "负责 Web 前端页面开发与交互实现，参与产品设计与需求评审；"
        "负责前端性能优化与浏览器兼容性处理；"
        "参与代码评审与技术文档编写；"
        "与后端协作完成接口联调。"
    ),
    "全栈开发工程师": (
        "负责前后端功能开发与系统设计，参与需求分析与技术选型；"
        "负责数据库设计与管理后台开发；"
        "负责性能优化与线上问题排查；"
        "参与代码评审与迭代交付。"
    ),
    "自动化测试工程师": (
        "负责自动化测试框架与测试用例开发，参与需求评审与测试方案设计；"
        "负责接口自动化与回归测试，提升代码覆盖率；"
        "参与代码评审，推动质量保障流程落地；"
        "负责线上问题的复现与跟踪，保障版本交付质量。"
    ),
    "Java开发工程师": (
        "负责微服务模块的设计与开发，参与需求分析与系统设计；"
        "负责接口性能优化与数据库调优；"
        "参与代码评审与技术攻关；"
        "负责线上问题排查，保障系统稳定性。"
    ),
    "大数据开发工程师": (
        "负责数据仓库建设与 ETL 开发，参与数据建模与指标体系建设；"
        "负责离线与实时任务的性能优化；"
        "参与数据治理与报表开发；"
        "负责数据任务的监控告警与故障处理。"
    ),
    "数据分析师": (
        "负责业务数据分析与报表开发，参与指标体系建设与埋点设计；"
        "负责用户行为分析与专题分析、AB 测试效果评估，输出数据结论；"
        "参与数据仓库建设与数据治理，保障数据口径一致；"
        "参与需求沟通，为业务增长提供数据支持。"
    ),
    "算法工程师": (
        "负责推荐与搜索算法的模型训练与调优，参与特征工程与算法方案设计；"
        "负责模型上线后的效果评估与迭代优化；"
        "参与技术攻关，跟进大模型等前沿方向。"
    ),
    "DevOps Engineer": (
        "负责云原生平台建设与容器化改造，参与系统设计与技术选型；"
        "负责 CI/CD 流水线设计，以及监控告警与链路追踪体系搭建；"
        "负责线上故障排查与容量规划，保障系统高可用；"
        "编写运维规范与技术文档；推动运维自动化建设。"
    ),
}


async def run(dry_run: bool) -> int:
    print("=" * 72)
    print("补齐 jobs.duty 岗位职责数据" + ("（DRY RUN，不写库）" if dry_run else ""))
    print("=" * 72)

    updated = 0
    skipped = []
    missing = []

    async with main.async_session() as session:
        rows = (await session.execute(select(main.Job).order_by(main.Job.id))).scalars().all()
        for job in rows:
            new_duty = JOB_DUTIES.get(job.job_name)
            if not new_duty:
                missing.append(job.job_name)
                continue
            old = (job.duty or "").strip()
            if old == new_duty:
                skipped.append(job.job_name)
                continue
            print(f"\nid={job.id}  {job.job_name}")
            print(f"  旧 ({len(old)} 字): {old!r}")
            print(f"  新 ({len(new_duty)} 字): {new_duty}")
            if not dry_run:
                job.duty = new_duty
            updated += 1

        if dry_run:
            await session.rollback()
        else:
            await session.commit()

    print("\n" + "-" * 72)
    print(f"待更新/已更新: {updated} 个")
    if skipped:
        print(f"内容已一致跳过: {len(skipped)} 个 -> {'、'.join(skipped)}")
    if missing:
        print(f"脚本未覆盖的岗位（保持原值）: {len(missing)} 个 -> {'、'.join(missing)}")
    print("-" * 72)

    await main.engine.dispose()
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="补齐 jobs.duty 岗位职责数据")
    parser.add_argument("--dry-run", action="store_true", help="仅预览差异，不写入数据库")
    args = parser.parse_args()
    sys.exit(asyncio.run(run(args.dry_run)))
