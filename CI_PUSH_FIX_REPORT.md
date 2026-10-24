# 推送后 CI 失败排查与修复报告

日期：2026-10-24。检查起点：`main`，HEAD `b389691ad52e28043dc3a282cf6d64007a6b1aba`，工作区原本干净。

远程 Actions 运行 `39963997801` 已收到提交：Git push 已成功，失败发生在后续 CI。截图中前端自检通过，后端 SQLite 回归和真实 MySQL 集成失败。此次仅修复依赖、测试隔离、测试执行及 CI 配置，不改业务接口或架构。

**结论：PASS WITH RISKS。** 本机最终一轮新依赖检查为后端 81 个套件通过、0 失败；前端 104 项通过、0 失败，构建与后端启动冒烟成功。真实 MySQL 与远端 Linux CI 未验证，且前一次检查出现过原因未明的 Chroma 偶发错误，不能据此声明全范围回归通过。后续用户提供的新日志指出远端失败步骤为 worker_guard 安全网；该步骤已改为按明确文件路径加载目标模块并打印实际模块路径和 worker 值，本机复核通过，但远端仍需推送后验证。

## 1. 问题审计与处理

| 文件位置 | 原问题 / 证据 | 当前处理 | 影响范围与兼容性 |
| --- | --- | --- | --- |
| `backend/requirements.txt` | `main` 导入 `modules.career.api` 注册 `File(...)` 路由时报缺少 `python-multipart`；用户日志与全新依赖环境均复现 | 新增直接依赖 `python-multipart>=0.0.18` | 恢复上传路由及所有导入应用入口的套件；不修改上传 API |
| `.github/workflows/ci.yml`，MySQL service options | 把 mysqld 的 `--character-set-server` / `--collation-server` 传给 `docker create`；公开 Actions 错误注释为退出 125 | 删除两个无效 Docker 参数 | 专用测试库仍由 `scripts/setup_test_mysql.py` 显式以 utf8mb4 / utf8mb4_unicode_ci 建立；未修改生产字符集 |
| 同文件，health-cmd | `$$MYSQL_ROOT_PASSWORD` 在容器 shell 中会展开为 PID 加字面后缀，原注释把它误称为 GitHub 转义 | 使用 `MYSQL_PWD=$MYSQL_ROOT_PASSWORD mysqladmin ping ...` | 仍保留后续 PyMySQL 真实连接与版本检查；没有降低数据库就绪门槛 |
| 同文件，always 清理步骤 | service 初始化失败可能早于 checkout，`working-directory: backend` 不存在导致二次报错 | 清理从默认目录执行，不依赖检出文件 | 清理范围与连接失败处理沿用原有逻辑 |
| `backend/tests/test_agent_knowledge.py`、`test_knowledge_retriever.py`、`test_analyze_endpoint.py` | 测试导入业务模块时会加载数据库配置；本机 `.env` 掩盖了 CI 无 `DATABASE_URL` 的问题 | 项目模块导入前按现有惯例设置 SQLite 内存连接串，并加载 `regression_env` | 仅补足离线测试环境，不给应用增加数据库回退 |
| `.gitignore`、`docs/AI面试问题评估基准.md` | 基准测试依赖现有本地文档，但整个 `docs/` 原被忽略；仅 Git 跟踪文件的检出副本缺失该文档 | 仅放行这一份既有基准文档，其他文档继续忽略 | 文档内容未改；后续提交时必须同时包含此文件 |
| `backend/tests/test_vector_store_chroma.py` | 用户日志显示 `_suite_metadata_codec` 协程未等待，原来的退出码 0 未覆盖其断言 | 补齐 `await` | 此套件由实际执行 123 项增至 136 项，新增执行的 13 项均通过；未跳过或删除测试 |
| `backend/tests/test_interview_ws.py`、`test_rag_production_route.py` | 新解析到的 FastAPI 0.161.1 使用延迟包含 router，依赖 `app.routes` 扁平列表的断言失败，实际 HTTP / WS 调用正常 | 分别用公开 `url_path_for` 与最终 OpenAPI 路径检查注册情况 | 原有实际请求、权限和响应断言保留；不改应用路由或外部契约 |
| `.github/workflows/ci.yml`，worker_guard 安全网步骤 | 用户后来提供的 Actions 摘要显示该步骤退出 1，但未包含其展开日志或 Traceback；原命令通过 `from utils import worker_guard` 导入整个工具包 | 改为从 `utils/worker_guard.py` 的明确路径加载，仅执行该守卫，并打印模块路径与 `WEB_CONCURRENCY` | 本机 `WEB_CONCURRENCY=2` 复核按预期捕获拒绝并退出 0；这减少包初始化/同名导入的不确定性，但在没有远端 Traceback 前不能确认其就是根因 |

用户最新补充日志中的 `test_vector_knowledge_retriever.py` 为 110 项通过、0 失败，`test_vector_store.py` 为 116 项通过、0 失败。该片段没有提供 `test_chroma_retriever_baseline.py` 的失败 Traceback，不能据此断言原 Linux CI 中该套件的根因。

## 2. 验证方式与执行命令

为避免开发机 `.env`、已有包、未跟踪文件掩盖问题，在忽略目录 `.ci-push-audit/` 中由 `git archive HEAD` 建立仅含 Git 跟踪文件的副本，再覆盖本次修改及必需基准文档。副本没有 `.env`；测试不连接真实业务库。为贴近 Linux 检出，副本的 prompt 文本统一为 LF，未改工作区源文件。

遵循已有执行方式逐个运行 `backend/tests/test_*.py`，不是 pytest；共 81 个独立 Python 进程。临时 `verify.py` 只负责顺序启动、收集退出码和保存日志，任何非零退出均算失败，无自动重试、无跳过、无断言降级。

实际使用的命令（均在项目根目录，前端命令在 `frontend` 目录）：

```powershell
# 全新 Python 3.10 虚拟环境，按项目 requirements 安装
.ci-push-audit/venv/Scripts/python.exe -m pip install -r backend/requirements.txt
.ci-push-audit/venv/Scripts/python.exe -m pip check

# 先跑针对性测试，再跑全部 81 个 test_*.py
# 下列 runner 内部实际执行：python tests/<套件>.py，cwd=临时检出副本/backend
python .ci-push-audit/verify.py test_interview_ws.py test_rag_production_route.py
python .ci-push-audit/verify.py

# 最终新依赖环境下的编译 / 冒烟（python 解释器选择说明见下文）
python -m compileall -q .ci-push-audit/checkout/backend
python .ci-push-audit/smoke.py
python .ci-push-audit/server_smoke.py

# 检索评分故障单独复测一次，再连续执行 5 次
python .ci-push-audit/verify.py test_retrieval_scoring_rules.py
python .ci-push-audit/verify.py test_retrieval_scoring_rules.py test_retrieval_scoring_rules.py test_retrieval_scoring_rules.py test_retrieval_scoring_rules.py test_retrieval_scoring_rules.py

# frontend，使用现有 node_modules，未重新 npm ci
npm test
npm run build

git diff --check
```

新安装版本：Python 3.10.11、FastAPI 0.161.1、Starlette 1.7.0、SQLAlchemy 2.0.55、bcrypt 5.0.0、Chroma 1.5.9、python-multipart 0.0.32。未为测试通过而回退或固定依赖版本。

Windows 本机存在额外运行环境限制：新 venv 的解释器启动 Chroma 持久化库时报 SQLite code 14（unable to open database file），而系统 Python 启动同一份新安装依赖可正常读写。交叉探针中“系统解释器 + 新包”成功，“venv 解释器 + 原有包”仍失败；Chroma 原生扩展文件相同。这里只定位到解释器运行环境差异，未确定底层原因，也未修改 Windows 安全设置。

因此，最后两轮新依赖全量检查使用系统 Python 3.10.11 加载隔离 venv 的包；临时 `sitecustomize.py` 删除全局 site-packages 搜索路径，子进程同样继承，避免落回全局旧包。实际 PowerShell 环境如下（不属于项目代码或新工具链）：

```powershell
$env:PYTHONPATH = "$PWD/.ci-push-audit/isolated;$PWD/.ci-push-audit/venv/Lib/site-packages"
$env:PYTHONNOUSERSITE = '1'
$env:PYTHONUTF8 = '1'
$env:AUDIT_RUN_LABEL = 'final-clean'
& 'C:/Users/DELL-PC/AppData/Local/Programs/Python/Python310/python.exe' .ci-push-audit/verify.py
```

Runner 为子进程设置 `AI_RUN_MODE=offline`、`PYTHON_DOTENV_DISABLED=1` 并移除外部 `DATABASE_URL`，各套件沿用自己的 SQLite / regression_env 初始化。启动冒烟显式设置 SQLite 内存库；不读取生产凭据。

## 3. 已得到的结果

| 检查 | 通过 | 失败 | 说明 |
| --- | ---: | ---: | --- |
| 原有本机依赖，全量自检 | 81 套件 | 0 | 不能单靠此结果代表全新 CI 环境 |
| 新 venv 解释器及新依赖，第一轮全量 | 76 套件 | 5 | 2 个路由枚举断言已修复；3 个持久化 Chroma 套件受上述本机 venv 运行环境影响，原始失败日志保留 |
| 修复后的两个路由套件，新 venv 针对性验证 | 2 套件（102 + 31 项） | 0 | HTTP / WS 真实测试及原断言继续执行 |
| 系统解释器 + 隔离的新依赖，第一轮全量 | 80 套件 | 1 | `test_retrieval_scoring_rules.py` 一次报 Chroma `InternalError: Error finding id`；不算通过 |
| 上述评分套件单独复测 / 连续 5 次复测 | 1 + 5 次 | 0 | 偶发错误原因未明，复测通过不抹去首次故障 |
| 系统解释器 + 隔离的新依赖，最终全量 | 81 套件 | 0 | 进程退出码 0；结果及逐套件日志保存在 `final-clean-results.json`；此前 Chroma 偶发失败仍按风险保留 |
| 前端 `npm test` | 104 项 | 0 | 退出码 0 |
| 前端 `npm run build` | 1 次 | 0 | Vite 8.0.6 构建成功；保留原有大于 500 kB chunk 警告（主 JS 约 2.26 MB），未顺带重构 |
| Python 编译、pip check、workflow 语法检查 | 已通过 | 0 | workflow YAML、4 个合法形状的 Docker options、3 段内联 Python 语法通过；未等同真实 MySQL 容器执行 |
| TestClient 启动 / 初始化 / 权限冒烟 | 已通过 | 0 | SQLite 内存库初始化 16 张模型表；27 个 HTTP OpenAPI 路径；健康检查 200；上传路由存在；未认证管理员 401；WS 未认证错误帧后关闭 4401；多 worker 仍被阻断 |
| 独立 Uvicorn 进程启动 | 1 次 | 0 | `python -m uvicorn main:app --host 127.0.0.1 --port <临时端口>`；健康检查及上传 OpenAPI 成功，进程已停止 |

原始失败与复测日志保留在忽略的 `.ci-push-audit/` 中，例如 `clean-results.json`、`clean-base-results.json`、`scoring-stability-results.json`，没有覆盖首次故障记录。评分套件连续复测结果数组保留全部 5 次退出码。

## 4. 未验证项目与风险

1. **真实 MySQL 集成未执行**：本机 Docker 引擎未启动（找不到 `docker_engine` 管道），不能把 workflow 静态修复视为 5 个数据库运行器通过。未运行 `engine_pool_mysql_run.py`、`ws_ticket_mysql_concurrency_run.py`、`interview_mysql_concurrency_run.py`、`ws_revocation_mysql_run.py`、`login_throttle_mysql_run.py`，及其真实 MySQL 建库、权限、迁移与并发检查。没有启动或修改业务数据库。
2. **Linux GitHub Actions 尚未重跑**：本次未 commit、未 push；公开错误注释可读，完整 Actions 日志下载要求登录。用户提供的 multipart Traceback 已复现并修复，但原 25 个失败套件的完整日志并不齐全，不能承诺 25 个失败的原因全部相同。需要后续提交本次文件并在 GitHub 上执行 CI，才能确认远端结果。
3. **Chroma 稳定性仍有风险**：本机 venv 持久化启动限制未解决；前一轮新依赖全量中出现 `Error finding id`，随后的单独、连续 5 次及最后一轮完整执行均通过，尚无充分证据判断为业务缺陷、第三方实现或本机运行环境。没有加重试、捕获后吞错、跳过测试或改用假实现。
4. **worker_guard 远端原因待确认**：用户提供的是失败步骤标题和退出码，没有步骤展开内容。改成显式路径加载后本机复核通过；是否解决 GitHub runner 上的实际故障，要等此变更推送后查看新运行日志。
5. **真实外部服务未验证**：Spark、ASR、Embedding、数字人等离线替身和错误处理的测试不等于真实上游集成通过。此次没有使用真实凭据或消耗生产配额。
6. requirements 现有宽版本范围仍会解析到不同版本，未擅自引入锁文件或更换依赖管理方式；Windows 验证也不能替代 Linux CI。

## 5. 变更边界与 Git 状态

- 修复前工作区干净，没有覆盖用户未提交修改。
- 原始修复提交涉及 9 个跟踪文件（31 行新增、16 行删除）及本报告、必需基准文档。本次后续另修改 `.github/workflows/ci.yml` 的 worker_guard 诊断步骤与本报告；当前确切状态以 Git 工作区为准。其他 docs 未放行；临时环境与日志保持忽略。
- 未改业务模块、API 地址、认证权限规则、数据库模型、Spark / AI / RAG 业务实现；兼容导入保持原状，未删除旧模块。此次失败根因没有要求删除或替换旧导入路径。
- 未删除测试、未降低断言、未修改真实业务数据，未新增第三方工具链，未进行 Git commit / push。无与本次 CI 修复无关的源代码变更。
- 最终结论为 **PASS WITH RISKS**：已确认的 CI 依赖和配置问题完成最小修复，本机末轮检查通过；worker_guard 改动也已本机复核。远端 Linux、真实 MySQL、worker_guard 远端故障原因和 Chroma 偶发错误仍需后续验证。到此停止，不自动执行后续架构重构。
