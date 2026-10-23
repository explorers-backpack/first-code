# AI 边界主流程接线报告

结论：**PASS WITH RISKS**。现有业务通过统一门面调用 Provider、策略和传输；新增实际业务路径测试通过，真实供应商与 MySQL 未验证。

## 调用链审计

- main 创建 LegacySparkFacade，注册 Spark transport，并将同一门面注册给 common.llm（面试）与 career.ports（对话、简历）。
- 面试 HTTP next-question → interview_service → interview_core → Agent → common.llm 默认门面 → Provider 工厂 → call_llm 策略 → SparkLlmProvider → 已注册传输。Planner、next/followup Agent 的默认依赖同样来自此门面。
- 对话 HTTP /api/chat → career.api → career.service → career.ports 注入门面 → 同一 Provider/策略/传输链。
- 简历分析与解析也使用 career.ports 门面。简历分析已有 ai_call_scope 与规则降级，继续保留。
- lifespan 原本已调用 common.health._report_ai_status → validate_startup；未新增平行启动校验入口，而是测试证明该检查早于数据库初始化执行。

原有缺口：PromptBoundary/build_guarded_prompt 未由门面调用；生产 Provider 缓存命中可能跳过凭据缺失检查；chat 只检查 Error 前缀，无法识别门面失败哨兵；离线 Provider 文档承诺单次调用但策略未落实；取消异常会被吞成失败字符串；非有限超时值或小数重试次数未严格拒绝。

## 修改内容

1. modules/ai/compat.py：门面在 Provider 选择和网络调用之前执行 build_guarded_prompt(message, PromptBoundary())；超过默认 20,000 字符拒绝，不截断。记录边界阻断计数和配置/边界失败事件（只含分类/追踪信息，不记录 Prompt）。离线 Provider 强制只尝试一次。取消异常向上传播。
2. modules/ai/factory.py：每次获取 Provider，包括缓存命中，先 validate_ai_config，防止显式 production 缺少凭据时继续使用缓存。注册传输仍沿用现有 factory 注册点及缓存失效机制。
3. modules/ai/policy.py：拒绝 NaN、Infinity 和非整数尝试次数，保证超时配置可用；CancelledError 不归为上游失败，不触发重试。
4. modules/career/service.py：chat 声明 career/chat@v1 作用域，识别结构化失败、旧哨兵和空输出，返回既有文案“抱歉，暂时无法获取分析结果。”，不再把供应商故障字符串显示为职业建议。
5. 新增 tests/test_ai_business_boundary.py，未删除、跳过或降低既有测试断言。

HTTP 路径、参数、状态声明和响应 schema 不变，完整 OpenAPI 比较相等。明确的行为变化是失败对话使用既有友好降级文案，以及超长输入在门面被拦截；正常成功回答不变。

## 配置与故障行为

| 场景 | 实际行为 |
|---|---|
| 显式 production 缺凭据/未注册 transport | lifespan 在访问数据库前报配置错误；调用期不访问传输，返回兼容失败，业务按原链路处理 |
| 已缓存 Provider 后移除生产凭据 | 下一次调用拒绝，不因缓存绕过检查 |
| 启动配置超时 NaN/Infinity 或小数尝试次数 | AiConfigError，启动失败 |
| 运行时策略配置非法 | 保留既有 get_call_policy 行为：ERROR 日志并使用默认策略，不新增静默策略 |
| 未显式选模式且凭据不完整 | 沿用原自动 offline 语义；不伪装为模型成功 |
| offline | 一次确定失败，不联网、不按生产重试配置等待 |
| 超时/限流 | 由实际 call_llm 策略有限重试；用尽后业务降级 |
| 上游鉴权失败 | auth 分类，不重试；面试 next-question 返回既有 ok=false/agent_failed 形状 |
| 简历分析上游不可用 | ai_available=false，ai_error.kind=unavailable，规则引擎诊断 |
| 超长 Prompt | boundary 分类，传输调用次数 0；记录 ai_boundary_blocks_total |
| 调用方取消 | CancelledError 传播，不伪造成功回答或发起重试 |

边界范围说明：本轮统一启用的是 Prompt 长度守卫，没有自动启用联系人脱敏、forbidden 标识符或 owner 校验。数据归属仍由现有服务鉴权负责，不能把空 owner 的默认 PromptBoundary 宣称为新增的所有权安全保护。显式依赖替身仍是允许的测试注入点；验收测试没有使用这些替身绕过统一门面。

## 测试与证据

新增测试只替换最底层 Spark transport；真实执行 FastAPI HTTP 路由、业务服务、Agent、SQLite、门面、Provider 工厂、策略与 Spark Provider。依赖覆盖只用于隔离数据库和已认证用户，不替换业务函数、Provider、策略或门面。

11 个新测试涵盖：

- 面试创建/start/next-question 成功和对话成功，核对传输实际收到 Prompt，以及策略调用计数。
- 面试上游 401 被分类为 auth，无重试，HTTP 返回既有失败结构。
- 对话实际一秒超时、两次尝试、最终友好降级。
- 对话 429 后恢复，实际策略重试一次并返回成功结果。
- HTTP 超长输入在传输前拒绝并递增边界指标。
- Provider 缓存预热后移除凭据，下一次请求不再调用传输。
- 实际 lifespan 在缺凭据、未注册传输和非法超时时失败，数据库 begin 未调用。
- 简历上传/分析上游 503，返回分类错误及规则诊断。
- 外部取消传播；offline 不联网且仅一次尝试；非法策略值拒绝。

实际运行（Python 为现有 C:\Users\DELL-PC\AppData\Local\Programs\Python\Python310\python.exe，未安装工具链）：

```powershell
# 仓库根目录；通过已有执行权限运行解释器
$env:PYTHONIOENCODING='utf-8'
python backend/tests/test_ai_business_boundary.py
python .ai-boundary-audit/run.py
# runner 清除子进程外部 DATABASE_URL，AI_RUN_MODE=offline；cwd=backend
# 定向：test_ai_business_boundary、test_ai_layer、test_analyze_endpoint、
# test_rag_production_route、test_module_boundaries
# 随后逐个 python tests/test_*.py
python .ai-boundary-audit/smoke.py
# cwd=frontend
npm test
npm run build
# 仓库根目录
git diff --check
```

结果：

| 项目 | 通过 | 失败/跳过 |
|---|---:|---:|
| 定向套件 | 5 | 0 / 0 |
| 完整后端 test_*.py 集合 | 80 套 | 0 / 0 |
| 其中新增实际业务链测试 | 11 个测试 | 0 / 0 |
| 前端自检 | 104 项 | 0 |
| 前端构建 | 成功 | 仍有 >500 KB chunk 告警 |
| Python 编译、导入、SQLite lifespan、health 200/auth 401 | 通过 | 0 |
| 完整 OpenAPI 与上一阶段快照比较 | 相等 | 0 |
| 依赖守卫 | 硬环 0，软环仍 1 | 无新增反向依赖 |
| git diff --check | 通过 | 0 |

初次新测试有一项测试实现错误：直接 patch SQLAlchemy AsyncEngine.begin 的只读实例属性失败；改为替换 main.engine 的测试替身，保留“数据库不能访问”的断言后通过。不是业务失败，也未跳过测试。完整后端最终运行退出码 0，各套件 JSON/log 保存于 .ai-boundary-audit；首次失败输出保留在 focused.log。

## 限制与风险

- 未调用真实 Spark、数字人/ASR、embedding 网络服务；没有验证外部凭证正确性或供应商可用性。
- 未执行真实 MySQL 的五个基础设施运行器，SQLite 不能证明 MySQL 并发语义。
- 没有实际 uvicorn 网络 E2E，使用进程内 ASGI；手工 RAG 标定与生成质量评测未运行。
- asyncio 超时/取消限制等待协程，不能保证立即终止原 Spark 同步 WebSocket 工作线程；原传输自身仍采用 60 秒超时。此次未重写传输层。
- 默认 Prompt 上限按字符数计算，不是 Token 预算；已有正常 Prompt 回归通过，极长输入将明确降级而不截断。

## Git 与完成边界

保留所有开始前的暂存/未暂存修改。本轮修改 4 个已有业务文件，新增 1 个测试文件和本报告；修改前副本位于 .ai-boundary-audit/before，专属已有文件 diff 位于 .ai-boundary-audit/task-existing.diff。整个 Git diff 包含前阶段和用户的其他改动，不能全部归于本轮。

未改 API schema、Prompt 模板、管理员接口、数据库数据或源码指纹；未 commit，完成本阶段后停止。
