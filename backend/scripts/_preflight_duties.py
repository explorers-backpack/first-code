# -*- coding: utf-8 -*-
"""写库前预检：新 duty 文本能被解析出多少项「可比对要求」。

只读，不触碰数据库。目的：避免写入后发现职责词解析不出、维度仍为 0。
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.resume_scoring import DUTY_TERM_ALIASES  # noqa: E402
from scripts.update_job_duties import JOB_DUTIES  # noqa: E402


def required_terms(duty: str):
    duty_lower = duty.lower()
    return sorted(
        term for term, aliases in DUTY_TERM_ALIASES.items()
        if any(a.lower() in duty_lower for a in aliases)
    )


def main() -> int:
    print("=" * 72)
    print("写库前预检：新职责文本的职责词解析情况")
    print("=" * 72)

    bad = []
    for name, duty in JOB_DUTIES.items():
        terms = required_terms(duty)
        flag = "OK " if len(terms) >= 3 else "!! "
        if len(terms) < 3:
            bad.append(name)
        print(f"{flag}{name:<18} {len(duty):>3} 字  解析出 {len(terms):>2} 项: {'、'.join(terms)}")

    print("-" * 72)
    print(f"岗位总数 {len(JOB_DUTIES)}；解析项 <3 的岗位: {len(bad)}")
    if bad:
        print("需调整: " + "、".join(bad))
    else:
        print("全部岗位均可解析出 >=3 项可比对要求，可安全写库。")
    print("-" * 72)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
