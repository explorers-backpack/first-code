# -*- coding: utf-8 -*-
"""
简历可取证评分模块（路线 A · 8 维）

设计原则
--------
1. **纯规则、无随机**：同一份简历任意次评分结果完全一致（同输入同输出），
   可复现、可申诉。不依赖 LLM，避免不可复现的分数。
2. **证据优先**：每个维度的分数必须能追溯到简历原文片段（evidence）。
   取不到证据即 0 分并说明原因，绝不填充默认值、不凭空推算。
3. **不做无依据的维度**：删除「团队协作 / 安全意识 / 创新能力 / 学习能力」
   这类无法从简历正文取证的维度，只保留 8 个可取证维度。

维度与权重
----------
| 维度         | key                 | 权重 |
|--------------|---------------------|------|
| 技能匹配度   | skill_match         | 0.20 |
| 岗位相关性   | role_relevance      | 0.15 |
| 项目复杂度   | project_complexity  | 0.15 |
| 量化成果密度 | quantified_impact   | 0.15 |
| 技术栈深度   | stack_depth         | 0.10 |
| 成果可验证性 | verifiability       | 0.10 |
| 经历连续性   | continuity          | 0.08 |
| 教育背景匹配 | education           | 0.07 |

已知局限
--------
- 「技能匹配度」与「岗位相关性」都以库内最匹配岗位为参照，二者存在部分相关性。
  这是当前无 JD 输入下的折中；接入目标 JD 后应改为按目标 JD 分别计算。
- 技能词表为白名单匹配，未收录的新技术无法识别（会体现在证据里，不会编造）。
- 别名 "go" 与英文单词 "go" 存在理论上的误命中，中文技术简历场景下影响可忽略。
"""

from __future__ import annotations

import bisect
import re
from typing import Dict, List, Optional, Tuple

# ============================================================
# 一、技能词表：canonical -> 别名（含大小写变体与常见写法）
# ============================================================
SKILL_ALIASES: Dict[str, Tuple[str, ...]] = {
    # ---- 编程语言 ----
    "Python": ("python",),
    "Java": ("java", "javase", "javaee", "java8", "java11", "java17"),
    "JavaScript": ("javascript", "js", "es6", "ecmascript"),
    "TypeScript": ("typescript", "ts"),
    "Go": ("golang", "go语言", "go"),
    "C++": ("c++", "cpp", "c＋＋"),
    "C#": ("c#", "csharp", ".net", "dotnet"),
    "C语言": ("c语言",),
    "Rust": ("rust",),
    "PHP": ("php",),
    "Ruby": ("ruby",),
    "Swift": ("swift",),
    "Kotlin": ("kotlin",),
    "Scala": ("scala",),
    "Dart": ("dart",),
    "Lua": ("lua",),
    "MATLAB": ("matlab",),
    "R语言": ("r语言",),
    "Shell": ("shell", "bash", "shell脚本"),
    "SQL": ("sql",),
    # ---- 前端 ----
    "Vue": ("vue", "vue.js", "vue3", "vue2", "vuex", "pinia"),
    "React": ("react", "react.js", "redux", "react native"),
    "Angular": ("angular", "angularjs"),
    "Svelte": ("svelte",),
    "Next.js": ("next.js", "nextjs"),
    "Nuxt": ("nuxt",),
    "jQuery": ("jquery",),
    "HTML": ("html", "html5"),
    "CSS": ("css", "css3"),
    "Sass": ("sass", "scss", "less", "stylus"),
    "Tailwind": ("tailwind",),
    "Webpack": ("webpack",),
    "Vite": ("vite",),
    "Element UI": ("element ui", "element-plus", "elementui"),
    "Ant Design": ("ant design", "antd"),
    "ECharts": ("echarts",),
    "Three.js": ("three.js", "threejs"),
    "WebGL": ("webgl",),
    "微信小程序": ("微信小程序", "小程序"),
    "uni-app": ("uni-app", "uniapp"),
    "Flutter": ("flutter",),
    "Android": ("android",),
    "iOS": ("ios", "objective-c"),
    "Unity": ("unity", "unity3d"),
    # ---- 后端框架 ----
    "Flask": ("flask",),
    "Django": ("django",),
    "FastAPI": ("fastapi",),
    "Tornado": ("tornado",),
    "Spring": ("spring",),
    "Spring Boot": ("spring boot", "springboot"),
    "Spring Cloud": ("spring cloud", "springcloud"),
    "MyBatis": ("mybatis",),
    "Express": ("express",),
    "Koa": ("koa",),
    "Nest.js": ("nest.js", "nestjs"),
    "Gin": ("gin",),
    "Netty": ("netty",),
    "gRPC": ("grpc",),
    "GraphQL": ("graphql",),
    "RESTful": ("restful", "rest api"),
    "WebSocket": ("websocket",),
    "Nginx": ("nginx",),
    "Tomcat": ("tomcat",),
    # ---- 数据存储 ----
    "MySQL": ("mysql",),
    "PostgreSQL": ("postgresql", "postgres", "pgsql"),
    "MongoDB": ("mongodb", "mongo"),
    "Redis": ("redis",),
    "Elasticsearch": ("elasticsearch", "elastic search", "es集群"),
    "ClickHouse": ("clickhouse",),
    "HBase": ("hbase",),
    "Hive": ("hive",),
    "Oracle": ("oracle",),
    "SQL Server": ("sql server", "sqlserver"),
    "SQLite": ("sqlite",),
    "TiDB": ("tidb",),
    "Doris": ("doris", "apache doris"),
    "Neo4j": ("neo4j",),
    # ---- 消息与调度 ----
    "Kafka": ("kafka",),
    "RabbitMQ": ("rabbitmq",),
    "RocketMQ": ("rocketmq",),
    "ZooKeeper": ("zookeeper",),
    "Airflow": ("airflow",),
    "XXL-JOB": ("xxl-job", "xxljob"),
    # ---- 大数据与算法 ----
    "Hadoop": ("hadoop",),
    "Spark": ("spark", "sparksql"),
    "Flink": ("flink",),
    "Pandas": ("pandas",),
    "NumPy": ("numpy",),
    "PyTorch": ("pytorch",),
    "TensorFlow": ("tensorflow",),
    "Scikit-learn": ("scikit-learn", "sklearn"),
    "XGBoost": ("xgboost",),
    "LightGBM": ("lightgbm",),
    "OpenCV": ("opencv",),
    "HuggingFace": ("huggingface", "transformers"),
    "LangChain": ("langchain",),
    "Milvus": ("milvus", "faiss", "向量数据库"),
    # ---- DevOps 与云 ----
    "Docker": ("docker",),
    "Kubernetes": ("kubernetes", "k8s", "k8s集群"),
    "Jenkins": ("jenkins",),
    "GitLab CI": ("gitlab ci", "gitlab-ci"),
    "GitHub Actions": ("github actions",),
    "Ansible": ("ansible",),
    "Terraform": ("terraform",),
    "Prometheus": ("prometheus",),
    "Grafana": ("grafana",),
    "ELK": ("elk", "elastic stack", "logstash", "kibana"),
    "Linux": ("linux", "unix", "centos", "ubuntu"),
    "Git": ("git", "svn"),
    "Maven": ("maven", "gradle"),
    "CI/CD": ("ci/cd", "cicd", "持续集成", "持续交付"),
    "AWS": ("aws", "amazon web services"),
    "Azure": ("azure",),
    "GCP": ("gcp", "google cloud"),
    "阿里云": ("阿里云", "aliyun"),
    "腾讯云": ("腾讯云",),
    "华为云": ("华为云",),
    "Serverless": ("serverless", "faas", "云函数"),
    # ---- 测试与工具 ----
    "JUnit": ("junit",),
    "PyTest": ("pytest", "unittest"),
    "Selenium": ("selenium",),
    "Playwright": ("playwright",),
    "JMeter": ("jmeter", "locust"),
    "Postman": ("postman",),
    # ---- 中文技术概念（CJK，按子串匹配）----
    # 说明：此处只收「可独立掌握与考核的技术方法/工具」，业务域词汇
    # （订单/支付/搜索/风控/用户增长等）不属于技能，归入 DUTY_TERM_ALIASES
    # 用于「岗位相关性」维度，避免技能清单被业务名词灌水。
    "微服务": ("微服务", "服务治理", "服务拆分"),
    "分布式系统": ("分布式",),
    "高并发": ("高并发", "大流量", "海量请求"),
    "高可用": ("高可用", "容灾", "多活"),
    "性能优化": ("性能优化", "性能调优", "压测", "慢查询"),
    "架构设计": ("架构设计", "系统设计", "技术选型", "领域驱动"),
    "中台": ("中台", "平台化", "通用组件"),
    "机器学习": ("机器学习", "深度学习", "神经网络"),
    "推荐系统": ("推荐系统", "推荐算法"),
    "自然语言处理": ("自然语言处理", "nlp", "大模型", "llm"),
    "计算机视觉": ("计算机视觉", "图像识别"),
    "数据分析": ("数据分析", "数据挖掘", "指标体系", "ab测试", "埋点"),
    "数据仓库": ("数据仓库", "数仓", "数据建模", "数据治理"),
    "中间件": ("中间件", "消息队列", "消息中间件"),
    "容器化": ("容器化", "云原生"),
    "自动化测试": ("自动化测试", "单元测试", "测试用例", "代码覆盖率"),
    "敏捷开发": ("敏捷开发", "scrum", "迭代开发"),
    "代码评审": ("代码评审", "codereview", "code review"),
    "监控告警": ("监控告警", "链路追踪", "可观测"),
    "灰度发布": ("灰度发布", "灰度", "蓝绿部署", "滚动发布"),
}

# ============================================================
# 二、职责 / 业务域词表（用于「岗位相关性」）
# canonical 职责项 -> 同义写法。命中任一写法即视为该项被要求 / 被覆盖，
# 避免因用词不同（如「系统设计」vs「架构设计」）造成漏判。
# ============================================================
DUTY_TERM_ALIASES: Dict[str, Tuple[str, ...]] = {
    "需求分析": ("需求分析", "需求沟通", "需求评审", "需求梳理", "业务需求", "需求文档"),
    "系统设计": ("系统设计", "架构设计", "方案设计", "接口设计", "数据库设计", "技术选型", "领域驱动"),
    "性能优化": ("性能优化", "性能调优", "压测", "慢查询", "调优", "响应时间"),
    "代码评审": ("代码评审", "codereview", "code review", "代码审查", "代码质量"),
    "测试保障": ("单元测试", "自动化测试", "测试用例", "代码覆盖率", "测试框架", "质量保障"),
    "线上问题": ("线上问题", "故障排查", "故障处理", "问题排查", "生产问题", "线上故障", "故障"),
    "稳定性": ("稳定性", "高可用", "容灾", "熔断", "限流", "降级", "多活"),
    "运维部署": ("运维", "部署", "发布", "上线", "监控", "告警"),
    "文档输出": ("技术文档", "接口文档", "设计文档", "文档"),
    "技术分享": ("技术分享", "分享", "培训", "带教", "指导"),
    "团队协作": ("团队协作", "跨部门", "沟通协作", "协作"),
    "项目管理": ("项目管理", "进度管理", "需求排期", "排期"),
    "带团队": ("带团队", "团队管理", "技术负责人", "小组负责人", "mentor"),
    "业务增长": ("业务增长", "用户增长", "增长", "拉新", "留存", "转化"),
    "数据分析": ("数据分析", "数据驱动", "数据挖掘", "指标体系", "报表", "埋点", "ab测试"),
    "产品设计": ("产品设计", "产品需求", "产品方案"),
    "迭代交付": ("迭代", "敏捷", "scrum", "交付", "版本发布"),
    "重构": ("重构", "代码重构", "技术重构"),
    "技术攻关": ("技术攻关", "疑难问题", "技术难点", "攻坚"),
    "算法模型": ("算法", "模型", "建模", "特征工程"),
    "推荐": ("推荐", "推荐系统", "推荐算法"),
    "搜索": ("搜索", "检索", "召回", "排序"),
    "风控": ("风控", "反欺诈", "风险控制"),
    "支付": ("支付", "清结算", "交易", "对账"),
    "订单": ("订单", "库存", "供应链", "履约"),
    "营销": ("营销", "广告", "投放", "活动"),
    "内容": ("内容", "社区", "图文", "视频"),
    "电商": ("电商", "商城", "交易平台"),
    "金融": ("金融", "银行", "保险", "证券"),
    "物流": ("物流", "配送", "仓储"),
    "数据仓库": ("数据仓库", "数仓", "数据治理", "数据建模", "etl"),
    "移动端": ("移动端", "app", "android", "ios", "小程序"),
    "后台管理": ("后台管理", "管理后台", "b端", "crm", "erp"),
    "中台": ("中台", "平台化", "平台建设", "通用组件"),
    "开源": ("开源", "开源项目", "社区贡献"),
}

# ============================================================
# 三、段落识别
# ============================================================
SECTION_PATTERNS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("intent", ("求职意向", "期望职位", "目标岗位", "应聘岗位", "期望岗位")),
    ("education", ("教育背景", "教育经历", "学历背景", "学习经历", "毕业院校", "院校")),
    ("experience", ("工作经历", "工作经验", "实习经历", "实习经验", "职业经历", "任职经历", "工作履历")),
    ("project", ("项目经历", "项目经验", "项目介绍", "主要项目", "项目实践")),
    ("skill", ("专业技能", "技能清单", "掌握技能", "擅长技能", "技术能力", "技术栈", "技能")),
    ("honor", ("荣誉奖项", "获奖情况", "获奖经历", "证书", "认证", "专利", "论文", "荣誉", "奖项")),
    ("summary", ("自我评价", "个人简介", "自我介绍", "个人总结", "个人优势")),
)

SECTION_LABELS: Dict[str, str] = {
    "intent": "求职意向",
    "education": "教育背景",
    "experience": "工作/实习经历",
    "project": "项目经历",
    "skill": "技能",
    "honor": "荣誉证书",
    "summary": "自我评价",
    "other": "正文",
}

_SECTION_LOOKUP = dict(SECTION_PATTERNS)

# 正文类段落（用于「量化成果密度」「项目复杂度」等按内容判定的维度）
CONTENT_SECTIONS = ("experience", "project", "honor", "summary", "other")

# ============================================================
# 四、其它词表
# ============================================================
COMPLEXITY_SIGNALS: Tuple[Tuple[str, Tuple[str, ...], int], ...] = (
    ("架构设计", ("架构设计", "系统设计", "技术选型", "方案设计", "重构", "领域驱动"), 3),
    ("分布式/微服务", ("分布式", "微服务", "服务治理", "服务拆分", "soa", "rpc"), 3),
    ("高并发/高可用", ("高并发", "高可用", "高性能", "大流量", "qps", "tps", "百万级", "千万级", "海量"), 3),
    ("性能优化", ("性能优化", "性能调优", "压测", "慢查询", "调优", "响应时间"), 2),
    ("容器与编排", ("docker", "kubernetes", "k8s", "容器化", "helm", "istio"), 2),
    ("消息与中间件", ("消息队列", "kafka", "rabbitmq", "rocketmq", "中间件", "缓存"), 2),
    ("数据层设计", ("分库分表", "数据库设计", "索引优化", "读写分离", "主从", "分片", "数据建模"), 2),
    ("稳定性治理", ("容灾", "灰度发布", "多活", "熔断", "限流", "降级", "故障排查"), 2),
    ("平台化/中台", ("中台", "平台化", "通用组件", "sdk", "框架", "低代码"), 2),
    ("从0到1", ("从0到1", "从零到一", "独立负责", "主导", "owner", "搭建"), 2),
    ("可观测性", ("监控告警", "链路追踪", "可观测", "日志系统", "prometheus", "grafana", "elk"), 1),
    ("协作与带教", ("带团队", "技术负责人", "项目管理", "跨部门协作", "mentor", "指导"), 1),
)
COMPLEXITY_FULL_RAW = 20.0  # 加权信号累计到 20 分即满分

# 量化成果：只认「带度量单位」的指标，不认纯计数（如「8 个微服务」）
QUANT_UNIT_AFTER = re.compile(
    r"(\d+(?:\.\d+)?)\s*(%|％|倍|万|亿|ms|毫秒|GB|MB|TB|万次|万人次|万元|QPS|TPS)",
    re.IGNORECASE,
)
QUANT_UNIT_BEFORE = re.compile(
    r"(QPS|TPS|DAU|MAU|GMV|PV|UV|ROI|日活|月活|阅读量|播放量|转化率|留存率|准确率|召回率|可用性|响应时间|并发量)"
    r"[^0-9\n]{0,6}(\d+(?:\.\d+)?)",
    re.IGNORECASE,
)
QUANT_FULL_DENSITY = 0.30  # 量化句占比达到 30% 即满分
DATE_MASK_PATTERN = re.compile(
    r"(\d{4}\s*[年./\-]\s*\d{0,2}\s*月?\s*[-~—至到]\s*(?:\d{4}\s*[年./\-]\s*\d{0,2}\s*月?|至今|现在|今|present|now))"
    r"|(\d{4}\s*[年./\-]\s*\d{1,2}\s*月)"
    r"|(\d{4}\s*年)"
)

_ONGOING = 10 ** 9  # 「至今」哨兵值

DATE_RANGE_PATTERN = re.compile(
    r"(\d{4})\s*[年./\-]\s*(\d{1,2})?\s*月?\s*[-~—至到]+\s*"
    r"(\d{4}|至今|现在|今|present|now)\s*(?:[年./\-]\s*(\d{1,2}))?",
    re.IGNORECASE,
)

EDU_LEVELS: Tuple[Tuple[Tuple[str, ...], int], ...] = (
    (("博士", "phd", "doctor"), 100),
    (("硕士", "研究生", "master"), 85),
    (("本科", "学士", "bachelor"), 70),
    (("大专", "专科", "高职"), 50),
    (("高中", "中专", "职高", "技校"), 25),
)
EDU_PRESTIGE: Tuple[Tuple[str, int], ...] = (
    ("985", 8), ("211", 6), ("双一流", 6), ("qs", 5), ("海外", 4), ("留学", 4), ("重点大学", 4),
)

VERIFY_SIGNALS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("开源仓库", ("github", "gitee", "gitlab.com", "开源项目", "开源贡献", "star")),
    ("技术博客", ("博客", "技术文章", "掘金", "csdn", "专栏", "公众号", "知乎")),
    ("论文", ("论文", "sci", "ei检索", "期刊", "会议论文")),
    ("专利/软著", ("专利", "软件著作权", "软著")),
    ("竞赛奖项", ("acm", "挑战杯", "蓝桥杯", "数学建模", "kaggle", "天池", "竞赛", "一等奖", "二等奖", "三等奖", "金奖", "银奖")),
    ("职业认证", ("认证", "证书", "pmp", "软考", "cpa", "cfa", "cisco", "ocp", "中级职称")),
    ("线上作品", ("http://", "https://", "作品集", "线上地址", "demo地址", "预览地址")),
    ("技术分享", ("技术分享", "讲师", "分享嘉宾", "大会", "meetup", "出版", "著书", "专利授权")),
)

DIMENSION_META: Tuple[Tuple[str, str, float], ...] = (
    ("skill_match", "技能匹配度", 0.20),
    ("role_relevance", "岗位相关性", 0.15),
    ("project_complexity", "项目复杂度", 0.15),
    ("quantified_impact", "量化成果密度", 0.15),
    ("stack_depth", "技术栈深度", 0.10),
    ("verifiability", "成果可验证性", 0.10),
    ("continuity", "经历连续性", 0.08),
    ("education", "教育背景匹配", 0.07),
)


# ============================================================
# 五、内部工具
# ============================================================
def _has_cjk(s: str) -> bool:
    return re.search(r"[\u4e00-\u9fff]", s) is not None


def _build_skill_patterns() -> List[Tuple[str, str, "re.Pattern[str]"]]:
    """构建技能匹配正则。ASCII 词用词边界，CJK 词用子串。"""
    pats: List[Tuple[str, str, re.Pattern]] = []
    for canon, aliases in SKILL_ALIASES.items():
        for alias in {canon, *aliases}:
            if _has_cjk(alias):
                rx = re.compile(re.escape(alias))
            else:
                rx = re.compile(r"(?<![A-Za-z0-9])" + re.escape(alias) + r"(?![A-Za-z0-9])", re.IGNORECASE)
            pats.append((canon, alias, rx))
    # 长别名优先，先占位，避免 "Spring Boot" 被 "Spring" 抢先
    pats.sort(key=lambda t: (-len(t[1]), t[0]))
    return pats


SKILL_PATTERNS = _build_skill_patterns()

_ALIAS_TO_CANON: Dict[str, str] = {}
for _canon, _aliases in SKILL_ALIASES.items():
    for _a in {_canon, *_aliases}:
        _ALIAS_TO_CANON[_a.strip().lower()] = _canon


def _strip_heading_prefix(line: str, key: str) -> str:
    s = line
    for word in _SECTION_LOOKUP.get(key, ()):
        idx = s.find(word)
        if 0 <= idx <= 6:
            s = s[idx + len(word):]
            break
    return s.strip(" \t　:：-—–、,，|｜·•")


def _match_section(line: str) -> Optional[str]:
    """判断该行是否为段落标题。要求关键词出现在行首附近，且行内含年份则视为内容行。"""
    s = line.strip()
    if not s or len(s) > 24:
        return None
    s2 = re.sub(r"^[\s#*\-•·【\[\(（]*[0-9一二三四五六七八九十]{0,2}[\s、.．)）\]】]*", "", s)
    if not s2 or len(s2) > 20 or re.search(r"\d{4}", s2):
        return None
    head = s2[:6]
    for key, words in SECTION_PATTERNS:
        for word in words:
            if word in head:
                return key
    return None


def _split_sentences(line: str) -> List[str]:
    parts = re.split(r"[。；;!！?？，,]|(?<=[A-Za-z0-9%])\.\s+", line)
    out: List[str] = []
    for p in parts:
        p = p.strip(" \t　-—–·•|｜、")
        if len(p) >= 6:
            out.append(p)
    return out


def _mask_dates(s: str) -> str:
    return DATE_MASK_PATTERN.sub(" ", s)


def _clamp(v: float) -> int:
    return int(max(0, min(100, round(v))))


def _ev(text: str, section: str) -> Dict[str, str]:
    return {"text": text, "source": SECTION_LABELS.get(section, "正文")}


class _Resume:
    """解析后的简历结构，供各维度评分器共享。"""

    def __init__(self, text: str) -> None:
        self.text = text
        self.lines: List[Tuple[str, str, int]] = []  # (行文本, 段落, 行起始偏移)
        self.sections: Dict[str, List[str]] = {k: [] for k, _ in SECTION_PATTERNS}
        self.sections["other"] = []
        self.sentences: List[Dict[str, object]] = []

        pos = 0
        current = "other"
        for raw in text.splitlines():
            stripped = raw.strip()
            if stripped:
                key = _match_section(stripped)
                if key:
                    current = key
                self.lines.append((stripped, current, pos))

                pure_heading = key is not None and len(_strip_heading_prefix(stripped, key)) < 2
                if not pure_heading:
                    self.sections[current].append(stripped)
                    line_offset = pos + (len(raw) - len(raw.lstrip()))
                    for sent in _split_sentences(stripped):
                        idx = stripped.find(sent)
                        sent_start = line_offset + (idx if idx >= 0 else 0)
                        self.sentences.append({
                            "text": sent,
                            "section": current,
                            "start": sent_start,
                            "end": sent_start + len(sent),
                        })
            pos += len(raw) + 1

        self._starts = [ln[2] for ln in self.lines]

    # -- 段落工具 --
    def sentences_in(self, sections: Tuple[str, ...]) -> List[Dict[str, object]]:
        return [s for s in self.sentences if s["section"] in sections]

    def content_sentences(self) -> List[Dict[str, object]]:
        return self.sentences_in(CONTENT_SECTIONS)

    # -- 技能工具 --
    def skill_hits(self) -> Dict[str, Dict[str, object]]:
        """全文技能命中（带全局偏移，用于定位所属段落）。"""
        occupied: List[Tuple[int, int]] = []
        hits: Dict[str, Dict[str, object]] = {}
        for canon, alias, rx in SKILL_PATTERNS:
            for m in rx.finditer(self.text):
                s, e = m.span()
                if any(s < oe and os_ < e for os_, oe in occupied):
                    continue
                occupied.append((s, e))
                item = hits.setdefault(canon, {"canonical": canon, "alias": alias, "spans": []})
                item["spans"].append((s, e))
        return hits

    def section_of_offset(self, offset: int) -> str:
        i = bisect.bisect_right(self._starts, offset) - 1
        if 0 <= i < len(self.lines):
            return self.lines[i][1]
        return "other"

    def sentence_at(self, offset: int) -> Optional[Dict[str, object]]:
        """按精确字符区间定位句子（避免用模糊窗口造成证据错配）。"""
        for s in self.sentences:
            if int(s["start"]) <= offset < int(s["end"]):
                return s
        return None


# ============================================================
# 六、8 个维度评分器
# ============================================================
def _pick_reference_job(resume_skills: List[str], jobs: List[dict]) -> Optional[dict]:
    """按技能覆盖率选取参照岗位（覆盖率相同则取技能要求更少者，避免大而全岗位占优）。"""
    if not jobs or not resume_skills:
        return None
    have = {s.lower() for s in resume_skills}
    best = None
    best_key = (-1.0, 0)
    for job in jobs:
        raw = (job.get("skills") or "").strip()
        if not raw:
            continue
        required = {_ALIAS_TO_CANON.get(t.strip().lower(), t.strip().lower())
                    for t in re.split(r"[,，、/;；|]", raw) if t.strip()}
        if not required:
            continue
        hit = len({r for r in required if r.lower() in have})
        rate = hit / len(required)
        key = (rate, -len(required))
        if key > best_key:
            best_key = key
            best = {"job": job, "required": required, "hit": hit, "rate": rate}
    return best


def _dim_skill_match(resume: _Resume, skills: List[str], ref: Optional[dict]) -> Dict[str, object]:
    if ref is None:
        evidence = [_ev(f"识别到技能：{'、'.join(skills)}", "skill")] if skills else []
        return {
            "score": 0,
            "rationale": "岗位库为空或未识别到任何技能，无法计算匹配度（不填充默认分）。",
            "evidence": evidence,
        }
    job = ref["job"]
    required = sorted(ref["required"])
    hit = sorted({r for r in required if r.lower() in {s.lower() for s in skills}})
    missing = sorted({r for r in required if r.lower() not in {s.lower() for s in skills}})

    evidence: List[Dict[str, str]] = []
    all_hits = resume.skill_hits()
    for canon in hit[:5]:
        info = all_hits.get(canon)
        if not info:
            continue
        first_span = min(info["spans"], key=lambda t: t[0])
        sent = resume.sentence_at(first_span[0])
        if sent:
            evidence.append(_ev(str(sent["text"]), str(sent["section"])))
    if not evidence and hit:
        evidence.append(_ev("技能清单命中：" + "、".join(hit[:10]), "skill"))

    return {
        "score": _clamp(ref["rate"] * 100),
        "rationale": (
            f"参照岗位《{job.get('job_name', '未知')}》要求 {len(required)} 项技能，"
            f"命中 {len(hit)} 项（覆盖率 {round(ref['rate'] * 100, 1)}%）。"
            + (f"缺口：{'、'.join(missing[:6])}。" if missing else "无缺口。")
        ),
        "evidence": evidence,
    }


def _dim_role_relevance(resume: _Resume, ref: Optional[dict]) -> Dict[str, object]:
    if ref is None:
        return {"score": 0, "rationale": "岗位库为空，无法计算岗位相关性。", "evidence": []}
    job = ref["job"]
    duty = (job.get("duty") or "").strip()
    if not duty:
        return {
            "score": 0,
            "rationale": (
                f"岗位《{job.get('job_name', '未知')}》未填写职责描述，无法计算（不猜测）。"
                "建议在岗位库中补充该岗位的完整岗位职责。"
            ),
            "evidence": [],
        }

    duty_lower = duty.lower()
    required = {
        term for term, aliases in DUTY_TERM_ALIASES.items()
        if any(a.lower() in duty_lower for a in aliases)
    }
    if not required:
        return {
            "score": 0,
            "rationale": (
                f"岗位《{job.get('job_name', '未知')}》的职责字段仅 {len(duty)} 字（「{duty}」），"
                "属于标签式短语而非职责描述，无法作为比对依据（不猜测）。"
                "建议将该字段补充为完整岗位职责（如「负责系统设计与性能优化，参与需求分析与代码评审」）。"
            ),
            "evidence": [],
        }

    resume_lower = resume.text.lower()
    covered: List[str] = []
    alias_hit: Dict[str, str] = {}
    for term in sorted(required):
        for alias in DUTY_TERM_ALIASES[term]:
            if alias.lower() in resume_lower:
                covered.append(term)
                alias_hit[term] = alias
                break
    missing = sorted(set(required) - set(covered))

    evidence: List[Dict[str, str]] = []
    seen: set = set()
    for term in covered:
        alias = alias_hit[term].lower()
        for s in resume.sentences:
            text = str(s["text"])
            if alias in text.lower():
                if text not in seen:
                    seen.add(text)
                    evidence.append(_ev(text, str(s["section"])))
                break
        if len(evidence) >= 5:
            break

    return {
        "score": _clamp(len(covered) / len(required) * 100),
        "rationale": (
            f"参照岗位《{job.get('job_name', '未知')}》职责可归为 {len(required)} 项可比对要求，"
            f"简历覆盖 {len(covered)} 项（{round(len(covered) / len(required) * 100, 1)}%）。"
            + (f"未覆盖：{'、'.join(missing[:6])}。" if missing else "")
        ),
        "evidence": evidence,
    }


def _dim_project_complexity(resume: _Resume) -> Dict[str, object]:
    pool = resume.sentences_in(("experience", "project"))
    if not pool:
        return {"score": 0, "rationale": "未识别到工作/项目段落，无复杂度证据。", "evidence": []}

    raw = 0.0
    matched: List[Tuple[str, int]] = []
    evidence: List[Dict[str, str]] = []
    for name, words, weight in COMPLEXITY_SIGNALS:
        found = None
        for s in pool:
            text = str(s["text"])
            low = text.lower()
            if any(w.lower() in low for w in words):
                found = s
                break
        if found:
            raw += weight
            matched.append((name, weight))
            evidence.append(_ev(str(found["text"]), str(found["section"])))

    if not matched:
        return {"score": 0, "rationale": "工作/项目段落中未识别到复杂度信号，无证据即 0 分。", "evidence": []}

    return {
        "score": _clamp(raw / COMPLEXITY_FULL_RAW * 100),
        "rationale": (
            f"命中 {len(matched)} 类复杂度信号（加权 {raw:.0f} / {COMPLEXITY_FULL_RAW:.0f} 分即满分）："
            + "、".join(f"{n}({w})" for n, w in matched)
        ),
        "evidence": evidence[:6],
    }


def _dim_quantified_impact(resume: _Resume) -> Dict[str, object]:
    pool = resume.content_sentences()
    if not pool:
        return {"score": 0, "rationale": "无可分析正文，无法计算量化成果密度。", "evidence": []}

    quantified: List[Dict[str, object]] = []
    for s in pool:
        masked = _mask_dates(str(s["text"]))
        if QUANT_UNIT_AFTER.search(masked) or QUANT_UNIT_BEFORE.search(masked):
            quantified.append(s)

    density = len(quantified) / len(pool)
    return {
        "score": _clamp(density / QUANT_FULL_DENSITY * 100),
        "rationale": (
            f"正文共 {len(pool)} 句，其中 {len(quantified)} 句含带单位的量化指标"
            f"（密度 {round(density * 100, 1)}%，{int(QUANT_FULL_DENSITY * 100)}% 即满分）。"
            "仅统计带度量单位（%/倍/万/ms/QPS 等）的指标，纯计数不计入。"
        ),
        "evidence": [_ev(str(s["text"]), str(s["section"])) for s in quantified[:6]],
    }


def _dim_stack_depth(resume: _Resume, skills: List[str]) -> Dict[str, object]:
    if not skills:
        return {"score": 0, "rationale": "未识别到任何技术栈，无证据即 0 分。", "evidence": []}

    hits = resume.skill_hits()
    context_sections = {"experience", "project"}
    in_context: List[Tuple[str, Dict[str, object]]] = []
    for canon, info in hits.items():
        spans = info["spans"]
        first = min(spans, key=lambda t: t[0])
        if resume.section_of_offset(first[0]) in context_sections:
            sent = resume.sentence_at(first[0])
            in_context.append((canon, sent or {"text": canon, "section": "project"}))

    breadth_score = min(100.0, len(skills) / 15 * 100)
    depth_score = min(100.0, len(in_context) / 8 * 100)
    score = _clamp(depth_score * 0.6 + breadth_score * 0.4)

    evidence = [_ev(str(s["text"]), str(s["section"])) for _, s in in_context[:6]]
    if not evidence:
        evidence = [_ev("技能仅出现在技能清单，未在工作/项目描述中体现", "skill")]

    return {
        "score": score,
        "rationale": (
            f"广度：识别 {len(skills)} 项技术栈（15 项为满分）；"
            f"深度：其中 {len(in_context)} 项在工作/项目描述中被实际使用（8 项为满分）。"
            "深度权重 60%、广度 40%，避免堆砌技能清单拿高分。"
        ),
        "evidence": evidence,
    }


def _dim_verifiability(resume: _Resume) -> Dict[str, object]:
    text_lower = resume.text.lower()
    matched: List[str] = []
    evidence: List[Dict[str, str]] = []
    for name, words in VERIFY_SIGNALS:
        for s in resume.sentences:
            text = str(s["text"])
            if any(w in text.lower() for w in words):
                matched.append(name)
                evidence.append(_ev(text, str(s["section"])))
                break

    if not matched:
        return {
            "score": 0,
            "rationale": "未发现任何可第三方核验的成果载体（开源/论文/专利/认证/作品链接等）。",
            "evidence": [],
        }
    return {
        "score": _clamp(len(matched) * 20),
        "rationale": f"命中 {len(matched)} 类可核验成果（每类 20 分，5 类满分）：" + "、".join(matched),
        "evidence": evidence[:6],
    }


def _dim_continuity(resume: _Resume) -> Dict[str, object]:
    pool = resume.sentences_in(("experience", "project"))
    ranges: List[Tuple[int, int, Dict[str, object]]] = []
    for s in pool:
        text = str(s["text"])
        for m in DATE_RANGE_PATTERN.finditer(text):
            start_year = int(m.group(1))
            start_month = int(m.group(2)) if m.group(2) else 1
            end_raw = m.group(3)
            if end_raw.isdigit():
                end_year = int(end_raw)
                end_month = int(m.group(4)) if m.group(4) else 12
                end_abs = end_year * 12 + end_month
            else:
                end_abs = _ONGOING  # 至今
            ranges.append((start_year * 12 + start_month, end_abs, s))

    if not ranges:
        return {
            "score": 0,
            "rationale": "未解析到「起止时间」区间，无法判断连续性（不猜测）。",
            "evidence": [],
        }

    finished = [b for _, b, _ in ranges if b != _ONGOING]
    latest_end = max(finished) if finished else 0
    for a, b, _ in ranges:
        if b == _ONGOING:
            latest_end = max(latest_end, a + 12)  # 「至今」按起始后 12 个月估算
    if latest_end <= 0:
        return {
            "score": 0,
            "rationale": "时间区间均为「至今」且无法确定终点，不猜测连续性。",
            "evidence": [_ev(str(s["text"]), str(s["section"])) for _, _, s in ranges[:4]],
        }
    earliest_start = min(a for a, _, _ in ranges)
    span_months = max(0, latest_end - earliest_start)

    # 覆盖区间合并（允许 3 个月容差，视为连续）
    merged: List[List[int]] = []
    for a, b in sorted((a, min(b, latest_end)) for a, b, _ in ranges):
        if merged and a <= merged[-1][1] + 3:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    gaps = len(merged) - 1

    years = span_months / 12
    years_score = min(70.0, years * 14)
    gap_bonus = 30.0 if gaps == 0 else max(0.0, 30 - gaps * 10)

    evidence = [_ev(str(s["text"]), str(s["section"])) for _, _, s in ranges[:4]]
    return {
        "score": _clamp(years_score + gap_bonus),
        "rationale": (
            f"解析到 {len(ranges)} 段时间区间，跨度约 {years:.1f} 年，"
            f"合并后 {len(merged)} 段、空档 {gaps} 处（跨度分 {years_score:.0f}/70，连续分 {gap_bonus:.0f}/30）。"
        ),
        "evidence": evidence,
    }


def _dim_education(resume: _Resume, ref: Optional[dict]) -> Dict[str, object]:
    pool = resume.sentences_in(("education",)) or [s for s in resume.sentences if "学历" in str(s["text"])]
    if not pool:
        return {"score": 0, "rationale": "未识别到教育背景段落，无证据即 0 分。", "evidence": []}

    joined = " ".join(str(s["text"]) for s in pool).lower()
    level_score = 0
    level_name = "未识别学历层次"
    for words, val in EDU_LEVELS:
        if any(w in joined for w in words):
            level_score = val
            level_name = words[0]
            break

    bonus = 0
    prestige_hit: List[str] = []
    for word, val in EDU_PRESTIGE:
        if word in joined:
            bonus += val
            prestige_hit.append(word)
    bonus = min(bonus, 15)

    penalty = 0
    note = ""
    if ref is not None:
        require = (ref["job"].get("edu_require") or "").strip()
        if require:
            required_score = 0
            for words, val in EDU_LEVELS:
                if any(w in require for w in words):
                    required_score = val
                    break
            if required_score and level_score < required_score:
                penalty = 15
                note = f"低于岗位要求（{require}），扣 15 分。"

    evidence = [_ev(str(s["text"]), str(s["section"])) for s in pool[:4]]
    return {
        "score": _clamp(level_score + bonus - penalty),
        "rationale": (
            f"学历层次：{level_name}（{level_score} 分）"
            + (f"；院校/背景加分：{'、'.join(prestige_hit)}（+{bonus}）" if prestige_hit else "")
            + (f"；{note}" if note else "")
        ),
        "evidence": evidence,
    }


# ============================================================
# 七、对外入口
# ============================================================
def extract_skills(text: str) -> List[str]:
    """从文本抽取技能（canonical 名称，按首次出现位置排序）。取不到即返回空列表，不填充默认值。"""
    resume = _Resume(text)
    hits = resume.skill_hits()
    ordered = sorted(hits.values(), key=lambda h: min(t[0] for t in h["spans"]))
    return [str(h["canonical"]) for h in ordered]


def evaluate_resume(text: str, jobs: Optional[List[dict]] = None) -> Dict[str, object]:
    """对简历文本做 8 维可取证评分。纯规则、确定性输出。"""
    resume = _Resume(text)
    skills = extract_skills(text)
    ref = _pick_reference_job(skills, jobs or [])

    results: Dict[str, Dict[str, object]] = {
        "skill_match": _dim_skill_match(resume, skills, ref),
        "role_relevance": _dim_role_relevance(resume, ref),
        "project_complexity": _dim_project_complexity(resume),
        "quantified_impact": _dim_quantified_impact(resume),
        "stack_depth": _dim_stack_depth(resume, skills),
        "verifiability": _dim_verifiability(resume),
        "continuity": _dim_continuity(resume),
        "education": _dim_education(resume, ref),
    }

    metrics: List[Dict[str, object]] = []
    weighted = 0.0
    for key, name, weight in DIMENSION_META:
        r = results[key]
        score = int(r["score"])
        weighted += score * weight
        metrics.append({
            "key": key,
            "name": name,
            "weight": weight,
            "score": score,
            "rationale": r["rationale"],
            "evidence": r["evidence"],
        })

    return {
        "score": _clamp(weighted),
        "metrics": metrics,
        "skills": skills,
        "reference_job": (
            {"job_id": ref["job"].get("id"), "job_name": ref["job"].get("job_name")} if ref else None
        ),
        "sections": {SECTION_LABELS.get(k, k): len(v) for k, v in resume.sections.items() if v},
        "sentence_count": len(resume.sentences),
    }


def build_prompt_summary(result: Dict[str, object]) -> str:
    """把结构化维度与证据整理成给 LLM 的事实摘要（不再喂裸分数）。"""
    lines: List[str] = []
    ref = result.get("reference_job")
    if ref:
        lines.append(f"参照岗位：{ref.get('job_name')}")
    lines.append(f"识别技能：{'、'.join(result.get('skills') or []) or '未识别到'}")
    lines.append("维度评分与依据：")
    for m in result.get("metrics") or []:
        lines.append(f"- {m['name']}：{m['score']} 分。依据：{m['rationale']}")
        for e in (m.get("evidence") or [])[:2]:
            lines.append(f"    原文证据（{e['source']}）：{e['text']}")
    return "\n".join(lines)
