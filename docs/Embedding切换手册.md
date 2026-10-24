# Embedding 切换手册（Runbook）

> 适用场景：把 RAG 向量化从**讯飞星火**（当前生产配置）切到 **OpenAI 兼容端点**  
> （OpenAI / DeepSeek / 智谱 / 本地 vLLM 等任何暴露 `/embeddings` 的服务），或反向切回。
>
> ⚠️ **核心原则：阈值数字不可照抄。** 当前 `.env` 中的  
> `RAG_MIN_SCORE=0.7685` / `RAG_MIN_SCORE_RATIO=0.9947` 是**在  
> `xinghuo-embedding`（2560 维）+ 当前语料**上真实标定出来的，  
> 换任何模型后这两个数字立即失效，必须走第 4 步重新标定。

---

## 0. 切换前必读：模型隔离机制

检索器按 `KnowledgeChunk.embedding_model == embedder.name` **精确过滤**，  
跨模型的向量**不参与打分、也不会报错**。这意味着：

- ✅ 切换是**安全的**：旧向量不会被错误使用，随时可切回（旧向量还在库里）。
- ❌ 但也是**静默的**：切换后若不做迁移，知识库检索**永远返回空**，  
  面试链路只会降级到 `WARNING_KNOWLEDGE_FAILED`，不会有人收到报警。  
  所以第 2 步的健康检查和第 3 步的迁移**不可跳过**。

---

## 第 1 步：改 `.env`

当前值（讯飞星火，专用凭据组）：

```env
EMBEDDING_PROVIDER=spark
SPARK_EMBEDDING_APP_ID=...
SPARK_EMBEDDING_API_KEY=***
SPARK_EMBEDDING_API_SECRET=***
RAG_TOP_K=8
RAG_MIN_SCORE=0.7685          # ← 仅对 spark 模型有效
RAG_MIN_SCORE_RATIO=0.9947    # ← 仅对 spark 模型有效
```

切换到 OpenAI 兼容端点，改成：

```env
EMBEDDING_PROVIDER=openai
EMBEDDING_API_KEY=sk-...
EMBEDDING_BASE_URL=https://api.openai.com/v1   # 不含 /embeddings；其他厂商换对应地址
EMBEDDING_MODEL=text-embedding-3-small          # 会写进 embedding_model 列
EMBEDDING_DIMENSION=1536                        # 0 = 不校验（按上游返回）
EMBEDDING_TIMEOUT=30
EMBEDDING_BATCH_SIZE=16
```

配置键速查（两条真实协议的解析见 `services/embedding_provider.py` 文件头注释）：

| 键                      | 默认                          | 说明                                                                        |
| ---------------------- | --------------------------- | ------------------------------------------------------------------------- |
| `EMBEDDING_PROVIDER`   | 空→自动                        | `hash` / `mock` / `openai` / `spark`；留空时配了 `EMBEDDING_API_KEY` → `openai` |
| `EMBEDDING_API_KEY`    | 空                           | `openai` 必填，**禁止提交进仓库**                                                   |
| `EMBEDDING_BASE_URL`   | `https://api.openai.com/v1` | 不含 `/embeddings`                                                          |
| `EMBEDDING_MODEL`      | `text-embedding-3-small`    | 即 `embedder.name`，模型隔离的匹配键                                                |
| `EMBEDDING_DIMENSION`  | `0`                         | 只做**校验**（0 = 不校验）                                                         |
| `EMBEDDING_TIMEOUT`    | `30`                        | 单次请求超时（秒）                                                                 |
| `EMBEDDING_BATCH_SIZE` | `16`                        | 单次请求最多几条文本                                                                |

注意：`spark` 模式读**专用组** `SPARK_EMBEDDING_*`（未配置时回落文本模型凭据）；  
`BASE_URL / _MODEL / _DIMENSION / _TIMEOUT / _BATCH_SIZE` 五键两条协议共用。

## 第 2 步：重启并自证运行态

```bash
cd backend && python -m uvicorn main:app --reload   # 或你的启动方式
```

启动日志会打印 embedding 状态；再核对健康检查：

```bash
curl http://localhost:8000/api/health | python -m json.tool
# 关注 "embedding" 键，固定 4 项：
#   provider          → 应为新模型名（EMBEDDING_MODEL 的值）
#   dimension         → 应与 EMBEDDING_DIMENSION 一致
#   semantic_enabled  → 应为 true（false = 回退哈希占位，配置没生效！）
#   error             → 应为 null
```

`semantic_enabled=false` 说明凭据/网络有问题，服务静默回退到了离线哈希——  
**此时检索结果全部作废**，先排障再继续。

## 第 3 步：迁移存量向量（不重切片）

迁移工具 `scripts/migrate_embeddings.py` 只重算向量、不重新切分文档；  
幂等设计，中断后可复跑精确补漏。

```bash
cd backend

# 3a. 先看规模（不写库）
python scripts/migrate_embeddings.py --provider openai --dry-run

# 3b. 确认无误后真跑（--yes 与 --dry-run 必须二选一，同时给会退出码 2）
python scripts/migrate_embeddings.py --provider openai --yes \
    --batch-size 8 --pace 0.5        # 限速防上游限流（讯飞曾因并发撞 11202）
```

常用参数：

| 参数                                               | 说明                                                  |
| ------------------------------------------------ | --------------------------------------------------- |
| `--provider`                                     | 目标模型名（= 新 `EMBEDDING_MODEL`）                        |
| `--document-id` / `--limit`                      | 只迁某文档 / 限量试跑                                        |
| `--batch-size`                                   | 默认 8                                                |
| `--pace`                                         | 批间隔秒数（限流时加）                                         |
| `--retries` / `--retry-delay` / `--max-failures` | 三级失败恢复                                              |
| `--retry-failed STATS_JSON`                      | 按上次统计文件只补失败项                                        |
| `--stats`                                        | 统计输出路径（默认 `scripts/embedding_migration_stats.json`） |

退出码：`0` 成功｜`2` 参数问题｜`3` 安全闸拦截。  
真跑完成后**复跑一次 `--dry-run`**，应显示 0 待迁——即迁移完备的自证。

## 第 4 步：重标定双阈值（不可跳过）

新模型的分数分布和旧模型**没有任何可比性**。用现成标定脚本跑一遍：

```bash
cd backend
python scripts/analyze_min_score_sweep.py    # 阈值扫描
python scripts/analyze_rag_params.py         # 参数网格
```

产出 JSON（参考 `scripts/rag_min_score_calibration_spark.json` 的格式），  
选点标准沿用任务 74 的口径：

- `RAG_MIN_SCORE`（绝对下限）：回答"语料到底有没有这个主题"——取  
  **不相关 query 全灭、相关 query 保留**的分界；
- `RAG_MIN_SCORE_RATIO`（相对阈值 α）：要求 `score >= α × top1`，  
  回答"top1 命中时，尾部哪些是蹭分噪声"；
- 两者是 **AND**：`score >= max(RAG_MIN_SCORE, α × top1)`。

标定完写回 `.env` 并重启。

## 第 5 步：端到端验证

```bash
# 冒烟：真实链路检索
cd backend && python scripts/_probe_retrieval_smoke.py

# 回归：50 套测试（regression_env 会把 EMBEDDING_*/RAG_* 钉成空串，不受 .env 影响）
python tests/run_all.py    # 或逐套运行
```

业务侧正向信号：开启 RAG 的面试题返回结果中，  
`knowledge_chunks`（int，本次真正注入 Prompt 的知识切片条数）应 > 0；  
连续为 0 = 阈值过严或迁移缺失，回到第 3/4 步。

---

## 附录 A：切回讯飞

把 `.env` 改回 `EMBEDDING_PROVIDER=spark` + `SPARK_EMBEDDING_*`，重启即可——  
模型隔离保证旧 spark 向量仍然在库、立即可用（前提：当初迁移时用的是**重算**  
而非删除，`migrate_embeddings.py` 从不删旧向量）。阈值记得换回 spark 标定值。

## 附录 B：何时切 Chroma 向量库

当前 SQL 后端检索是**全表扫描 + 内存余弦**。经验阈值：

- 语料 < 1 万切片：SQL 后端足够（当前规模），无需动作；
- 语料 1~10 万切片：检索延迟开始可感知，规划切换；
- 语料 > 10 万切片：必须切。切换前先用 `scripts/sql_baseline_snapshot.json` /  
  `chroma_baseline_snapshot.json` 的基线数据做一致性对照。

## 附录 C：串行向量化（embed_batch）

`embedding_service.embed_batch` 已实现但**无消费者**——导入知识时仍是逐片串行  
调用。语料增长后若导入变慢，在 `import_knowledge.py` / `migrate_embeddings.py`  
中改为批量调用（注意各厂商单请求条数上限，用 `EMBEDDING_BATCH_SIZE` 控制）。
