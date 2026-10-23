# Career-ai 模块迁移收尾审计报告

日期：2026-10-23。范围：当前工作区；模块化单体迁移收尾，不改变业务行为。

## 执行前状态

分支 main，HEAD 0d0a03f（数据库事务与面试状态一致性），领先 origin/main 1 个提交；前序相关提交 e3de816（出题策略）、46e0558（RAG）。工作区已有大量暂存、未暂存及未跟踪修改，包括 main、模块迁移、security、AI 适配、测试和 CI。此次基于实际工作区检查，未重置、覆盖或提交这些修改。

原始 Git 状态及已跟踪 diff 保存在 `.migration-audit/status-before.txt`、`before.patch`；本次修改的 12 个测试均保存了操作前副本。对重叠文件按原内容做最小修改，无需覆盖用户改动。没有发现 AGENTS.md。

## 旧路径引用审计表

以下位置均相对仓库根目录。先完成并展示初步审计表后实施修复；回归发现的查询下沉守卫另行补充。

| 文件位置 | 旧引用/问题 | 当前目标 | 影响范围 | 处理 |
|---|---|---|---|---|
| backend/tests/test_interview_ws.py、test_interview_production.py、test_ws_revocation.py、test_ws_ticket.py | from api import interview_ws | modules.interview.ws | WS、鉴权、ticket、撤销测试无法导入 | 更新导入及源码读取路径 |
| backend/tests/test_rag_diagnostics.py | api.interview 导入、logger 名称、AST 文件路径 | modules.interview.api | 路由探针、日志捕获和静态检查 | 同步三类引用 |
| backend/tests/test_avatar_loop.py、test_interview_core.py、test_interview_orchestrator.py、test_rag_production_route.py、test_strategy_wiring.py | api/interview.py、api/interview_ws.py 文件读取 | modules/interview/api.py、ws.py | 文件不存在、接线断言失效 | 只更新迁移路径 |
| backend/tests/test_rag_retriever_config.py | 测试说明中的旧路径 | modules/interview/api.py | 测试说明 | 同步说明 |
| backend/tests/test_interview_ws.py、test_interview_production.py | 消费者扫描只含 services、api | 增加 modules 递归扫描 | 新位置可能漏检 | 扩大扫描，仍要求唯一消费者 |
| backend/tests/test_module_boundaries.py | ws 未识别为路由、common 未纳入包集合、过期 WS SQL 白名单 | modules.interview.ws、common、已下沉的 security 查询 | 分层与环检测基线 | 更新识别，移除过期白名单；软环上限 23 收紧至实际 21 |
| backend/tests/test_ws_revocation.py | 要求在 WS 中找到 UserSession SQL | WS 调用 security.load_session_by_id | 查询下沉后误报 | 同时检查 auth_session_id 参数传递、security 导入及目标函数主键查询；反向依赖检查增加新路径 |
| backend/services/interview_agent.py:239、interview_planner.py:568 | 延迟 from main import spark_api | main 中现存兼容门面 | 冻结业务源文件和调用契约 | 保留，不重构 |
| frontend/src/api/interviewSocket.js、后端路由 | /api/interview/... | HTTP/WS URL 不随 Python 包路径变化 | 前后端协议 | 保留 URL；协议测试验证 |
| 服务文档、历史报告及部分注释 | api/interview*.py 文本 | 历史描述 | 无执行影响，部分文件有冻结指纹 | 保留并登记，不为改注释触碰冻结代码 |

## 实际架构与引用核对

main 装配 modules.auth、modules.career、modules.interview 的路由，以及仍在 api 下的 knowledge；配置、数据库与认证仍通过 database、deps、security 等既有入口共享。没有创建新的兼容模块，没有删除旧模块。

Spark 仍由 main 注册传输并导出兼容门面；Agent/Planner 的延迟导入保留，不能据此宣称零循环依赖。边界扫描覆盖 92 个业务模块，硬环为 0、软环为 21，既有白名单没有扩大。

ASR 是面试 WS 的 answer(mode=asr) 经编排层调用数字人内置能力，不是待恢复的独立 ASR HTTP 路由。前端 speech_done、answer/asr 协议和后端对应处理保留；数字人/ASR 离线行为由现有套件验证。真实供应商连通性未验证。

检查了 import、源码文件读取、动态/字符串引用、路由注册、认证依赖、启动方式、CI、环境示例和前端地址。未发现剩余可执行的 api.interview/api.interview_ws 旧导入；搜索仍能命中历史注释、测试名称和正确的 /api/interview URL。当前仓库未找到 Docker/Compose 部署文件。uvicorn main:app 的入口仍有效。

兼容保留：main.spark_api 门面和 main 中现有 database/models/session 等导出供旧调用方及测试使用；本次未清理这些符号。main 的管理员端点及相应 SQL 白名单未改动。

## 验证方式与记录

使用现有 Python 3.10、已有依赖和逐文件自检方式，没有引入 pytest 或安装新工具链。Python 在当前沙箱内无法启动，经执行权限批准运行同一现有解释器。数据库测试使用内存或临时文件 SQLite；没有连接生产库。

实际命令：

```powershell
# 仓库根目录；现有解释器绝对路径
$env:PYTHONIOENCODING='utf-8'
& 'C:\Users\DELL-PC\AppData\Local\Programs\Python\Python310\python.exe' .migration-audit/run.py
# runner：子进程移除外部 DATABASE_URL，AI_RUN_MODE=offline；cwd=backend
# 先分别 python tests/test_module_boundaries.py、test_rag_diagnostics.py、
# test_interview_ws.py、test_strategy_wiring.py，再逐个 python tests/test_*.py

# 撤销守卫修改后的定向测试
Remove-Item Env:DATABASE_URL -ErrorAction SilentlyContinue
& 'C:\Users\DELL-PC\AppData\Local\Programs\Python\Python310\python.exe' backend/tests/test_ws_revocation.py

# 独立冒烟，脚本内指定内存 SQLite 并导入 regression_env
& 'C:\Users\DELL-PC\AppData\Local\Programs\Python\Python310\python.exe' .migration-audit/smoke.py

# cwd=frontend
npm test
npm run build

# 仓库根目录
git diff --check
```

最终结果：后端完整 CI 自检集合 78 套通过、0 套失败、0 套跳过，运行器退出码 0；先行定向 4 套通过、0 失败；撤销修正后额外定向 55 项通过、0 失败。后端按套件计数，不把各套件格式不同的断言输出混算为用例数。逐套件退出码记录在 .migration-audit/full-*.json，日志在同名 .log。

已确认：前端 104 项通过、0 失败；Vite 构建成功（2237 个模块），主 chunk 约 2255 KB，有超过 500 KB 的体积告警，本轮不做拆包优化。

冒烟通过：216 个 Python 文件内存编译，main/models 导入，TestClient 启动/关闭 lifespan、SQLite schema 初始化；/api/health 返回 200、未认证 /api/auth/me 返回 401；HTTP 面试创建、WS、ticket、知识库、chat 路由注册存在。此处是进程内 ASGI 启动验证，不等同于真实 uvicorn 监听端口、生产环境启动或真实网络服务验证。

过程失败如实记录：首次运行器全局强制内存 DATABASE_URL，覆盖了 3 个套件自行设置的临时文件库，导致所有权、撤销、ticket 套件失败；改为 CI 既有的清除外部 DATABASE_URL 方式后，所有权和 ticket 通过，撤销剩余 1 条旧源码位置断言失败。修复该守卫后定向撤销测试 55 通过、0 失败。未改变业务实现或跳过断言。首次冒烟受 Windows GBK 输出编码影响失败，指定 UTF-8 后成功。逐套件日志保存最终重跑结果，不保留每次覆盖前的完整日志。

## 未执行及风险

- 真实 MySQL 的 5 个运行器：engine_pool_mysql_run.py、ws_ticket_mysql_concurrency_run.py、interview_mysql_concurrency_run.py、ws_revocation_mysql_run.py、login_throttle_mysql_run.py。没有为此次任务建立隔离 MySQL 基础设施，不使用现有业务库验证。不能以 SQLite 结果证明 MySQL 并发/隔离语义。
- Spark LLM/embedding、真实数字人/ASR/TTS：未调用外部凭证或网络服务；离线适配与 mock 测试不代表真实接口通过。
- frontend/tests/interview_ws_integration_run.js：未启动专用 uvicorn 网络实例，未执行真实网络 E2E；已执行后端 ASGI WS 测试与前端协议自检。
- tests/*_run.py 手工 RAG 标定、迁移、基线及评测工具未纳入本轮 CI 自检，不视为通过；其中部分需要外部服务或数据库写入。CI 的 Linux/MySQL service job 未在本机复现。
- 启动日志中的 RAG 标定配置不一致、离线数字人、进程内限流、关闭撤销轮询提示属于当前回归配置/既有配置提示，未改参数或数据。没有解除单 worker 限制。
- 21 个既有软循环、main 中管理员逻辑和兼容导出仍存在，属于后续独立任务。

## Diff 与完成边界

本次相对操作前副本修改 12 个测试文件，共 51 行增加、54 行删除（含路径说明更新）。专属 diff：`.migration-audit/task.diff`。新增本报告及 `.migration-audit` 本地取证脚本、备份、日志；它们不是应用运行依赖。

整个 Git diff 中还包含执行前就存在的业务迁移、AI、认证、CI 等大量改动，不能归为此次工作；此次没有修改业务源代码或前端源代码。未删除测试、未降低断言、未修改生产数据、未提交 commit。完成报告后停止，不进入下一阶段重构。

## 最终结论

**PASS WITH RISKS**：迁移收尾的本地离线回归基线通过；真实 MySQL、外部供应商和真实网络 E2E 未验证，不宣称全部环境的全量回归通过。
