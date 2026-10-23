# Spark、管理员模块与入口反向依赖抽离报告

结论：**PASS WITH RISKS**。本地离线检查通过；没有验证真实 Spark、MySQL 或生产网络环境。

## 改动与边界

- `backend/modules/ai/spark_transport.py`：从 main 原样迁出 SparkAPI 与 _make_url，负责 HMAC URL 签名、WebSocket 建连、请求参数、分帧接收、content/reasoning_content 拼接和异步线程桥接。未改变超时、协议参数、哨兵串或异常处理策略。AI Provider、策略和 RAG 未重构。
- `backend/modules/admin/api.py`：保留三个管理员 URL、HTTP 方法、函数名、文档、请求 schema 和 Depends(get_current_admin)/Depends(get_db)。
- `backend/modules/admin/service.py`：承接日志查询、岗位写入和踢下线用例。保留登录会话撤销提交 → ticket 作废 → 广播记录的顺序，以及后两步失败时的原有降级逻辑。服务不 import FastAPI，UserNotFound 由路由映射为原 404/用户不存在。
- `backend/modules/admin/schemas.py`：原 JobCreate 字段及校验完整迁入；`__init__.py` 导出 router。
- `backend/common/llm.py`：无应用依赖的门面注册/获取端口。main 构造原有门面并显式注册；Agent/Planner 默认路径使用该端口，显式传 spark 的调用方式不变。
- `backend/main.py`：只装配 Spark 传输与门面、注册端口、挂载管理员路由；移除对应内联实现与已无用途的传输依赖。

依赖方向：main → Spark transport / LLM port / admin router；admin router → admin service → models/共享服务；Agent/Planner → common.llm。业务服务没有反向 import main。

## 兼容性

完整 OpenAPI JSON 与改动前完全相等，包括 operationId、路径、方法、参数、schema、响应声明、tags 和描述。数据库事务及副作用顺序也经源码 AST 比较确认，仅将用户不存在的 HTTPException 转成领域异常，再在路由层恢复同一 HTTP 错误。

main.spark_api、SparkAPI、_make_url、JobCreate、三个管理员端点函数仍保留兼容导出；数据库/模型兼容导出未删除。main.spark_api 仍是注册给业务的同一实例，但以后通过重新赋值 main.spark_api 来注入替身不再有效；改用 common.llm.configure(fake)，或者业务函数已有的 spark 参数。仓库内两处这样的测试/手工运行器已同步迁移。未承诺维持 main._ws 等私有实现细节的 monkeypatch 接口。

独立调用业务服务时，应显式注入 spark，或由调用方先配置 common.llm；服务不会再通过 import main 隐式启动应用装配。未配置端口仍走既有不可用/降级路径，保留原错误文本。

## 回归与取证

使用现有 Python 3.10 和项目逐文件自检命令，离线 Provider、隔离 SQLite，不接触真实业务数据。

实际命令（仓库根目录，Python 使用现有 `C:\Users\DELL-PC\AppData\Local\Programs\Python\Python310\python.exe`）：

```powershell
$env:PYTHONIOENCODING='utf-8'
python .extraction-audit/baseline.py  # 修改前执行，保存 OpenAPI
python .extraction-audit/contracts.py
python .extraction-audit/focused.py
python .extraction-audit/run.py
# 子进程 cwd=backend；移除外部 DATABASE_URL，先定向，再逐文件 python tests/test_*.py
python .extraction-audit/fingerprint.py  # 仅同步遗漏的同两处源码指纹
python backend/tests/test_interview_eval_benchmark.py
# cwd=frontend
npm test
npm run build
# 仓库根目录
git diff --check
```

| 检查 | 实际结果 |
|---|---|
| 最终定向 7 套 | 7 通过，0 失败 |
| 后端完整集合，79 套 | 首轮 78 通过、1 失败；失败套件更新指纹后重跑通过；79 套均取得通过结果，无跳过 |
| 新增 Spark/端口自检 | unittest 7 个测试通过；签名、请求帧、流拼接、推理回退、错误文案、异步桥接、无 main 导入的默认端口 |
| 管理员角色自检 | 46 项通过；补充三个接口缺失/伪造凭证 401、普通用户 403、管理员踢不存在用户 404 |
| WS 撤销自检 | 55 项通过，包括踢下线、已建立连接断开和广播故障路径 |
| 评测套件定向重跑 | 226 项通过、0 失败；不是生成质量评测 |
| 前端 | 104 项通过、0 失败；Vite 构建成功，仍有 chunk 大于 500 KB 告警 |
| 契约与结构 | 完整 OpenAPI 相等；Spark 类/签名函数 AST 相等；管理员业务体等价 |
| 编译/启动 | backend Python 编译，main 导入，TestClient lifespan/SQLite 建表，health 200、管理员匿名 401 通过 |
| 依赖扫描 | 硬环 0；软环 21 → 1；剩余为已有 embedding_provider ↔ embedding_provider_spark；无新增反向 main 导入 |
| Git 空白检查 | git diff --check 通过 |

过程失败没有隐藏：第一轮边界检查发现软环上限仍为 21、实际已降至 1，按既有棘轮规则收紧；第一次完整回归中 test_interview_eval_benchmark 因两处旧指纹失败。test_agent_full_input 和该评测套件只更新 Agent/Planner 默认 LLM 取值接线的指纹，未改 Prompt、评分、语料或任何业务断言。R1 反向 main 白名单和 R7 管理员 SQL 白名单均已清空，新增管理员模块迁移守卫。

完整回归运行器的首轮退出码为 1，原始 `full-*.json/log` 如实保留；不能称这一次运行退出码为 0。补跑日志为 `.extraction-audit/recheck-test_interview_eval_benchmark.py.log`，退出码 0。其他日志、前后 OpenAPI、取证脚本均在 `.extraction-audit/`。

## 未验证与已知限制

- 未调用真实 Spark/数字人/ASR 网络服务；协议测试使用假 WebSocket，证明协议与重构前一致，不证明供应商在线。
- 未执行真实 MySQL 五个并发/连接池运行器，SQLite 不能替代 MySQL 隔离语义验证。
- 未运行真实 uvicorn 网络 E2E；启动验证是进程内 ASGI TestClient。
- 手工 Spark/RAG 标定、迁移与两臂生成质量对比未执行；同步改动的 rag_production_chain_spark_run.py 已编译检查，但未联网运行。
- Spark 的原有异常清理、重试/取消等语义未调整；本轮是职责抽离，不是传输健壮性重写。
- 既有 embedding 软循环和前端 bundle 体积告警保留。

## 工作区保护与 Diff 范围

本轮开始时工作区已存在前一阶段及用户未提交修改。逐个保存修改前副本到 `.extraction-audit/before/`，未重置、暂存或提交。

修改 11 个已有文件：main、Agent/Planner；test_admin_role、test_agent_full_input、test_ai_layer、test_interview_eval_benchmark、test_module_boundaries、test_rag_production_route、test_ws_revocation；以及手工运行器 rag_production_chain_spark_run。

新增 7 个代码文件：admin 的 4 个文件、Spark transport、common.llm、test_spark_transport。另新增本报告及本地取证目录。前端源代码未改。已有文件的本轮专属 diff 在 `.extraction-audit/task-existing.diff`，完整 Git diff 仍包含本轮开始前的无关改动，不能全部归属本任务。

未创建 commit；完成本轮抽离与验证后停止。
