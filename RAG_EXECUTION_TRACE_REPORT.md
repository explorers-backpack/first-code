# RAG 真实执行诊断接线报告

日期：2026-10-23。结论：**PASS WITH RISKS**。真实本地执行链路验证通过；真实供应商、生产 MySQL 与语义检索质量未验证。

## 开始状态与范围

开始时 main 分支，HEAD fe9559a（将 AI 边界检查接入实际主流程），工作区干净。检查实际解析、分块、向量入库、SQL/Chroma 检索、出题、追问及 AI 门面代码后实施。原四态 rag_diagnostics 保留，不改变其响应口径。

新增 common/rag_trace.py，以 ContextVar 为一次调用串联 trace_id，记录真实调用开始/结束和返回数量。阶段通过明确装饰器或 with span 接在业务函数上，没有安装运行期 monkeypatch，也不替换业务返回结果。纯标准库，不读取密钥配置、不访问数据库。

## 实际观测点

| 阶段 | 接线位置 | 数据来源 |
|---|---|---|
| 请求与解析 | api/knowledge.py 上传端点 | 实际文件解析调用的耗时、输出字符数、异常类别 |
| 分块 | knowledge_import_pipeline.import_document | 实际 chunker.split 返回数量、耗时 |
| 文档向量化 | _embed_planned / _encode_batch | 实际批量尝试、已产向量数量、批量失败和逐片回退结果 |
| 导入结果 | import_document | 实际 document 引用、切片数、保存数、向量数、复用跳过数、业务失败 |
| 检索器装配 | knowledge_rag.build_vector_retriever | 实际配置/组装成功或失败 |
| 查询向量化 | VectorKnowledgeRetriever._embed | 实际向量维度、耗时、输入/配置或上游故障 |
| SQL 候选排序过滤 | SqlAlchemyVectorStore.search | 实际候选数，排序/阈值/top-k 后数量；耗时 |
| 向量搜索 | _search 与 SQL vector_search | 实际召回数量、结果引用、评分、耗时与错误分类 |
| 相对阈值过滤 | _apply_ratio_filter | 实际过滤后数量、结果与顺序 |
| 检索输出 | VectorKnowledgeRetriever.retrieve | 去空/去重后的实际输出条数、结果引用及评分 |
| 上下文组装 | 三个 Agent 的 render_*_prompt | 实际组装耗时、渲染字符数、是否含知识上下文 |
| 生成 | LegacySparkFacade.chat_async + SparkLlmProvider 的传输调用前 | 耗时、成功/失败、类型化错误，以及确切 Prompt 是否提交给传输 |
| 最终业务结果 | interview_core.generate_next_question / interview_followup.generate_followup | 校验后的 ok、上下文是否组装、至少一次实际提交、整体耗时 |

每条事件包含 event=rag_stage、trace_id、sequence、stage、status、elapsed_ms。阶段耗时可以嵌套，不能简单相加当总耗时。一个上传请求的 parse/import 共用 trace；一次出题或追问的检索、上下文、生成、outcome 共用 trace。上传与后续检索是不同调用，用 document_ref 在同一进程内关联。

项目没有独立 reranker。retrieval 事件明确写 rerank=not_supported；向量库相似度排序和阈值过滤不冒充“重排”。不新增重排算法、不改变召回顺序或阈值。

“使用上下文”的精确定义：context_rendered 是真实知识模板渲染；context_submitted 是该 Prompt 在实际 Spark Provider 调用传输前被核对。内部使用带随机进程密钥的 HMAC 指纹匹配，指纹不写日志，避免把 JSON 修复请求误报为携带上下文。它证明发送意图和传输调用发生，不证明远程模型已收到、更不证明模型在语义上采纳知识。generation_ok/outcome 则分别体现传输输出与业务校验结果；失败不能冒充成功。

## 隐私与故障处理

- 新诊断只输出白名单标量：阶段、数量、耗时、布尔值、固定错误码，以及有限结果评分。
- 不输出查询、Prompt、文件名、标题、来源 URL、用户 ID、邮箱、手机号、文档正文、向量、生成答案或异常原文。
- 文档/切片引用由原始整数主键经进程随机密钥 HMAC 得到；每次最多列出 20 个结果，并标记 results_truncated。实际总数量单独记录。
- 进程重启或不同 worker 的引用不能直接关联；没有持久化密钥或新配置，以免为追踪引入长期可关联的用户标识。
- 异常映射为有限错误码：超时、配置/输入错误、不可用、解析错误等；失败原因精度以安全分类为限，不记录原始异常消息。
- 同时去掉 AI policy 失败日志的 message=error.message，防止上游把密钥/Prompt 回显到已有生成日志；保留类型、重试性、请求号等诊断字段。
- 日志 handler 失败不会改变业务结果；并发请求的 ContextVar 追踪已验证隔离。
- HTTP 原有错误响应与既有历史日志不在本轮全面脱敏范围内。本轮新增诊断流保证不序列化上述内容，不声称全系统所有旧错误文案已经完成隐私治理。

日志名为 career_ai.rag.trace，正常事件为 INFO；需由运行环境启用该 logger 的 INFO 级别。未新增公开诊断 API 或向业务响应增加敏感字段。

## 验证

新增 tests/test_rag_execution_trace.py，使用真实 HTTP 上传、真实解析器、分块器、hash-local 向量计算、SQLite 持久化与 SQL 检索，再走实际 Agent/门面/Provider。只替换外部 LLM 传输；数据库和用户依赖覆盖用于隔离环境，不模拟召回结果。hash-local 是项目已有离线实现，不能据此评估语义检索质量。

9 个新增测试覆盖：

1. 上传解析/分块/向量化计数与数据库行数一致，真实检索数量与响应一致，Prompt 确含测试知识，document_ref 可关联，整个调用 trace 一致。
2. 零命中和关闭 RAG 不误报上下文已提交。
3. 存储故障被记录于实际搜索阶段，仍按原逻辑降级；含敏感哨兵的异常不会出现在诊断日志。
4. 解析失败与向量化失败分别定位到实际阶段；失败时已完成数量如实记录。
5. 上游鉴权失败：上下文可已提交，但生成及最终结果失败；AI 日志也不回显上游秘密。
6. 并发 trace 隔离、日志 handler 抛错不影响请求。
7. 真实追问检索与上下文提交路径。
8. 上下文已渲染但被 AI 长度边界拒绝时，context_submitted=false。
9. common.rag_trace 所有导入仅来自标准库。

执行命令（解释器为现有 C:\Users\DELL-PC\AppData\Local\Programs\Python\Python310\python.exe）：

```powershell
$env:PYTHONIOENCODING='utf-8'
python backend/tests/test_rag_execution_trace.py
python .rag-trace-audit/run.py
# 清除子进程外部 DATABASE_URL，默认 AI_RUN_MODE=offline
# cwd=backend，先 5 套定向，再逐文件 python tests/test_*.py
python .rag-trace-audit/smoke.py
# cwd=frontend
npm test
npm run build
# 根目录
git diff --check
```

最终完整集合 81 套：80 套通过，1 套新增诊断测试因测试阈值失败；修正后该套件单独重跑 9/9 通过。81 套均取得通过结果，无跳过，但完整运行器该次退出码为 1，未把它标成一次全绿。最终定向允许集合/诊断/边界四套通过，新增诊断套件的最终结果以 final-trace-tests.log 为准。前端 104/104 通过，构建成功（保留 >500 KB chunk 告警）。OpenAPI 与前阶段快照完全相等，Python 编译、导入、隔离 SQLite lifespan、health 200/auth 401 通过。依赖守卫通过，无新增循环。

过程失败：首次现有全量 80 套中两套的“精确允许导入集合”未包含 common.rag_trace；仅增加该精确标准库诊断模块，并增加其标准库依赖断言，原禁止数据库/框架检查保留。第二次新测试的追问检索在阈值 0 下实际零命中；hash-local 相似度可以为负，将测试专用 min_score 设为 -1 后保留全部召回和 Prompt 断言并通过。生产阈值未变，未伪造检索结果。最初日志与最终重跑日志保留在 .rag-trace-audit。

## 未验证与限制

- 真实 Spark/embedding/数字人/ASR 连通性、凭据和生成质量未验证。
- 未运行真实 MySQL 五套基础设施并发运行器或真实 uvicorn 网络 E2E。
- Chroma 已有离线套件执行，但此次新增端到端诊断证据针对 SQL。公共检索阶段对 Chroma 同样接线；其 ANN 内部候选数量不可见，不伪造该字段。
- 直接导入文本 JSON 不经过文件解析器，不能报告文件解析阶段；只有实际文件上传出现 parse 事件。
- 直接绕过既有入库/检索组装点调用第三方对象不属于统一追踪入口；独立函数调用有独立 trace，跨后台任务/进程需要调用方单独设计关联。
- 现有无效输入提前返回、不调用模型的路径不能标为模型生成成功；outcome 的 generation_ok 对应最终业务 ok，须结合 generation 事件是否存在判断。

## Diff 与结束边界

修改前副本位于 .rag-trace-audit/before；未改业务算法、提示词文本、schema、数据库事务、外部接口或默认检索参数。新增 common/rag_trace.py、test_rag_execution_trace.py 和本报告；对插入诊断的冻结文件同步精确源码指纹，保留全部原有行为断言。

开始时工作区干净，当前 diff 均为本轮诊断及相关验证工作；未 commit、未部署、未写真实业务数据。完成后停止。
