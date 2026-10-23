# 知识库与 RAG 链路 · 端到端审计与渐进式优化报告

> 范围：`文档上传 → 解析 → 切片 → Embedding → 向量存储 → 检索 → 结果过滤 → Prompt 注入 → 面试题生成`
>
> 方法：**先建立基准，再改实现**。所有结论都指向可复现的产物：
> `backend/tests/rag_eval_run.py`（评估运行器）、`backend/tests/rag_eval_metrics.py`（指标定义）、
> `backend/scripts/rag_eval_report.json`（本轮实测数据）。
>
> 语料**未修改**：沿用仓库既有的 `scripts/rag_query_set.json`（16 篇受控语料 + 16 条查询）
> 与 `scripts/interview_knowledge.json`（22 篇真实风格文档）。金标准由
> `expected_keywords` 在**运行时推导**（命中 ≠ 1 篇直接抛错，不猜）。

---

## 0. 结论速览

| # | 审计项 | 结论 | 严重度 |
|---|--------|------|--------|
| 1 | 生产实际组件 | Retriever = `VectorKnowledgeRetriever`；Embedding = `xinghuo-embedding`（2560 维语义）；VectorStore = `SqlAlchemyVectorStore`（**全表扫描**）。`VECTOR_STORE` 未配置 ⇒ 默认 `sql` | — |
| 2 | 维度 / 模型 / 归一化 / 空向量 / 旧索引 | 维度与模型**逐行落库**并参与检索过滤；**向量不做归一化存储**，余弦每次现算 L2 模长；空向量被 `require_vector` 拒收；换模型必须重算（已有迁移器） | 中 |
| 3 | 切片重叠 / 元数据 / 来源 / 更新删除一致性 | 重叠与元数据齐备；**★ 文档改短后残留切片仍可被检索到**（已修） | **高** |
| 4 | Top-K / 阈值 / 无结果 / 去重 / 低相关过滤 | 去重与空正文过滤齐备；**默认无阈值 ⇒ 注入噪声率 80%**（实测）；相对阈值远优于绝对阈值 | 中 |
| 5 | 全表扫描是否要换 Chroma | **当前规模不需要换**（32 片 / 54ms，远低于 LLM 的 8~19s）；但 SQL 是 O(N)，**生产维度下每片约 1.4ms**，超 ~140 片即 >200ms。已给出量化触发条件 | 中 |
| 6 | 权限与范围过滤 | 知识库**全局共享、无归属列**，写操作仅 admin ⇒ 今天不存在串读；但**没有任何强制**。已加「半成品多租户」tripwire | 中（潜在） |
| 7 | 来源追踪 / 检索质量与生成质量分离 | Prompt 里每来源标一次 `（来源：…）`，**实测 5/5 查询全部可追**；生成质量**未评估**（需真实模型，见 §8） | — |
| 8 | 离线评估集 | Recall@K / Precision@K / MRR / nDCG@K / 噪声注入率 / 延迟分位，均有手算样例钉死定义 | — |
| 9 | 文档 / 模型 / 索引版本 | 有 `embedding_model` + `embedding_dim` + 阈值溯源产物；**无 `index_version` 列**（见 §9 迁移计划） | 低 |
| 10 | 故障 vs 无结果 + 告警 + 降级 | 降级有、错误码有、**告警此前完全没有**（无任何日志）⇒ 已新增四态诊断 + 分级日志 | **高**（已修） |

本轮改动**只动非冻结文件**。13 个 `FROZEN_SOURCES` 文件与 2 个 prompt 模板
`git diff` 为空（已核对）。

---

## 1. 真实运行路径（逐段取证）

### 1.1 写入链

```
POST /api/knowledge/documents        (JSON)      ┐
POST /api/knowledge/documents/upload (多部分文件) ├→ api/knowledge.py  ← 仅 admin
                                                    │
                            services/document_parser.py        二进制 → 纯文本
                            （pdf / docx / pptx / html / txt，编码探测 + 解析告警）
                                                    │
                            services/knowledge_import_pipeline.py  五阶段
                              ① document  自然键 (title, source, category) 命中则复用
                              ② chunk     DocumentChunker.split()  size=500 overlap=80
                              ③ embedding 分批 encode（批 16，失败回退逐片定位）
                              ④ persist   按 chunk_index upsert 切片行
                              ⑤ vector    store.add(VectorRecord) → 落 embedding 三列
                                                    │
                            services/vector_store_sql.py  ← 权威行（MySQL knowledge_chunk）
                            services/vector_store_chroma.py ← 仅当 VECTOR_STORE=chroma，镜像进 ANN
```

关键事实：

- **权威副本只有一份**：MySQL 的 `knowledge_document` / `knowledge_chunk`。
  `sql` 后端下「索引就是权威行本身」；`chroma` 后端下 ANN 是**派生索引**，
  `ChromaVectorStore.search()` 在索引为空时**返回 `[]` 而不回落 SQL**
  （这是刻意设计，避免「以为在用 ANN、其实在全表扫描」）。
- 切片幂等判据是**四条件同时成立**才跳过：有行 + 有向量 + `embedding_model` 相同 + 正文逐字节未变。
  只按「有没有向量」跳过会留下**指向旧文字的向量**（换模型 / 改文档都会踩）。

### 1.2 读取链

```
POST /api/interview/{id}/next-question   ┐  use_rag 默认 True（只翻 HTTP 契约层）
POST /api/interview/{id}/follow-up       ┘
        │
  services/interview_service.py  ──── 唯一接触 DB 的层
        │
  services/interview_core.py  ── resolve_retriever(db, retriever, use_rag, retriever_kwargs)
        │                          ├─ ① 显式注入的 retriever（测试替身 / 自定义）
        │                          ├─ ② use_rag=True → knowledge_rag.build_vector_retriever()
        │                          └─ ③ 都不是 → None（空实现，恒返回 []）
        │
  services/vector_knowledge_retriever.py
        ├─ build_query()   只取**一个**来源：topic → current_stage → job 的 name/title/position
        │                  （刻意不拼接：多来源拼在一个向量里会互相稀释）
        ├─ store.search()  → select_matches() 唯一排序口径
        │                  跳维度 → (-score, 位置) → min_score(含等号) → top_k
        ├─ _apply_ratio_filter()  score >= α × top1
        └─ _to_chunks()    丢空正文 + 按 "".join(content.split()) 去重
        │
  services/interview_agent.py  format_knowledge_context()
        ├─ 按 source 分组，同源多片合并，**每个来源只标一次**（来源：…）
        ├─ 剥掉相邻切片的重复重叠文本
        └─ 总量上限 KNOWLEDGE_CONTEXT_MAX_CHARS = 4000
        │
  prompts/interview/question_knowledge.txt  （空知识 ⇒ question.txt，逐字节不变）
```

### 1.3 「只被定义、没有生产调用方」的接口

| 组件 | 状态 | 说明 |
|------|------|------|
| `MemoryVectorStore`（若有） | **不在生产路径** | `resolve_vector_backend()` 只认 `sql` / `chroma`，未知值**启动即报错**（不静默退回） |
| `HashEmbeddingService` | **离线/测试专用** | `EMBEDDING_PROVIDER` 空串时才是默认；生产 `.env` 是 `spark` |
| `FakeLlmProvider` | **测试专用** | `AI_RUN_MODE=production` 下选 fake ⇒ 启动抛 `AiModeError` |
| `ChromaVectorStore` | **已实现但未启用** | 生产未配 `VECTOR_STORE` ⇒ 走 `sql`。本轮基准给出「何时该启用」的量化依据（§5） |

> 结论：**没有「定义了却假装在用」的组件**。风险不在 Mock 上，而在
> 「默认走全表扫描」这件事**没有任何可见性**——`/api/health` 的 `embedding`
> 字段能看出模型，但没有字段告诉你「当前向量后端是 sql」。

---

## 2. 向量维度 / 模型 / 归一化 / 空向量 / 旧索引兼容

### 2.1 维度与模型

- `knowledge_chunk` 逐行存 `embedding`（JSON）、`embedding_model`（str）、`embedding_dim`（int）。
- 检索时按 `embedding_model == embedder.name` 过滤（`vector_store_sql.search`），
  **不同模型的向量不会被混在一起比较**。
- 维度不符（同一模型改了维度、或行被改坏）⇒ `select_matches()` 跳过该候选；
  候选非空但**全部**维度不符 ⇒ 抛 `VectorStoreDimensionError`（不静默返回空）。

### 2.2 归一化与相似度

- **存储时不归一化**：`embedding` 原样落库。
- `cosine_similarity(a, b)` = `dot(a,b) / (|a|·|b|)`，**每次查询现算两个 L2 模长**。
  对全表扫描来说这意味着每个候选多两次平方和 —— 这是 §5 里 SQL 成本随维度
  线性放大的直接原因之一。
- 零向量 ⇒ 返回 `0.0`（不抛、不除零）；长度不等 ⇒ 抛错。

### 2.3 空向量

- 写入侧：`require_vector()` 拒绝空列表 / 字符串 / bool，脏数据进不了库。
- 读取侧：`search` 带 `WHERE embedding IS NOT NULL`；`rebuild_index` 对
  「不是非空 list/tuple」的行**跳过并计数**，不让一条坏数据打挂整次重建。

### 2.4 旧索引兼容

- 判据是**三个条件**（有向量 + 模型相同 + 维度相同），见
  `services/knowledge_embedding_migration.needs_migration()`。
- 已有专用迁移器 `EmbeddingMigration`：**只 UPDATE 向量三列**，不重切、不动正文/元数据。
  这一点是正确性要求而非效率要求——重切会改变 `chunk_index`，而向量是按行存的，
  错位之后「切片 7」在迁移前后不是同一段文字。
- 幂等：重复执行第二次是 `status="skipped"` 的成功空操作；中途失败可按
  `failures[].chunk_id` 精确重跑。

---

## 3. 切片重叠 / 元数据 / 来源引用 / 更新与删除一致性

### 3.1 实测切片质量

| 语料 | 文档 | 切片 | 去重后 | 重复率 | 平均长度 | 平均重叠 | 最大重叠 |
|------|------|------|--------|--------|----------|----------|----------|
| `rag_query_set`（受控） | 16 | 16 | 16 | 0.0 | 343.0 | 0.0 | 0 |
| `interview_knowledge`（真实风格） | 22 | 32 | 32 | 0.0 | 354.8 | 25.2 | 78 |

- **重复率 0**：没有两片正文完全相同（`_to_chunks` 的去重是第二道防线，不是唯一防线）。
- **重叠受控**：配置 `overlap=80`，实测最大 78、平均 25.2（短段落自然重叠更少）。
  重叠在 Prompt 侧还会被 `_strip_chunk_overlap` 再剥一次，避免同一句话进两次。
- 元数据必填字段：`document_id` / `category` / `source`，附加 `chunk_index`。

### 3.2 来源引用

- 切片阶段把文档的 `source` 搬进 `metadata["source"]`；检索时
  `_to_chunk()` 把它**提升**成 `KnowledgeChunk.source`（不留在 metadata 里等人翻）。
- Prompt 里按来源分组，每个来源只标一次 `（来源：…）`——不是每片标一次。
- **`score` / `chunk_id` / `embedding_model` 从来没有进过 Prompt**（`format_knowledge_context`
  明确不读 metadata）。这是对的：模型不需要知道相似度分数，但**审计需要**——
  所以它们留在 `KnowledgeChunk.metadata` 里供日志/回放使用。

### 3.3 ★ 文档更新后的一致性缺陷（已修）

**缺陷**：同一文档被改短后，`chunk_index >= 本次切片数` 的旧行**既不被复用、也不被删除**。
导入管线早期版本刻意「只 upsert、不删」，理由是「静默删数据风险更大」。但那条约定
留下的是**半替换语义**：

- `chunk_index` 在 `0..n-1` 的行 → 正文被覆盖成新内容 ✅
- `chunk_index >= n` 的行 → **原样保留** ❌
- 而检索**只看 `embedding`、不看 `chunk_index`** ⇒ 这些过期正文**仍会被检索到并注入 Prompt**

现象就是「文档已经改短了，模型还在引用被删掉的段落」。

**修法**（`KnowledgeImportPipeline._remove_stale_chunks`）：

1. 只在**本次零失败**时清理 —— 避免「新内容没写完、旧内容已删」；
2. 删权威行 → 同步派生索引（`store.delete` / `store.remove`，若后端提供）→ 索引同步失败
   写进 `errors`（稳定码 `stale_index_failed`），**绝不假装同步成功**；
3. **如实记账**：新增报告字段 `removed_stale_chunks`。用「报告字段」而不是「不删」
   来满足「不静默」。

**取证**（`tests/test_knowledge_import_pipeline.py` 第 [9] 节，13 项断言）：

- 对照组：缩容前「第39节」**能**被检索到（否则「检索不到」是恒真的）
- 缩容后：`removed_stale_chunks == 少掉的片数`，库里只剩新切片，`chunk_index` 连续 `0..n-1`
- 缩容后：「第39节」**检索不到**
- `store.delete` 确实收到了被删的 `chunk_id`（派生索引同步）
- 重复导入 ⇒ `status=skipped`、清理 0 条、完全不碰索引（幂等）
- 中途失败 ⇒ 报告失败、`removed_stale_chunks == 0`、**旧切片一条没少**（保守性）

> 契约变化：`IMPORT_RESULT_FIELDS` 12 → **13 键**（`schemas.KnowledgeImportReport` 同步）。
> 前端未消费该报告的任何计数字段（已 grep 确认），加键对前端无影响。

### 3.4 删除一致性

`DELETE /api/knowledge/documents/{id}` → `knowledge_maintenance.delete_document`：

- 顺序是**先删切片、后删文档**（反序会违反外键）
- 再调后端索引删除原语；后端「索引与权威行分离且不提供删除接口」⇒ 把这一点写进
  `index_error` 并提示重建，**不假装同步成功**
- `POST /api/knowledge/rebuild` 支持 `purge=True` 真正重建（`add` 是 upsert，清不掉残留）；
  `purge` 对「索引即权威行」的后端**直接拒绝**（清空等于删知识库正文）

---

## 4. Top-K / 阈值 / 无结果 / 重复 / 低相关过滤

### 4.1 参数网格实测（hash-local 256 维，16 条查询，每条 1 篇金标准）

| top_k | min_score | ratio | Recall@K | Precision | MRR | 噪声率 | 空结果 | p95(ms) |
|-------|-----------|-------|----------|-----------|-----|--------|--------|---------|
| 1 | | | 1.0 | 1.0 | 1.0 | **0.0** | 0 | 4.02 |
| 5 | | | 1.0 | 0.2 | 1.0 | **0.8** | 0 | 3.19 |
| 10 | | | 1.0 | 0.1 | 1.0 | 0.9 | 0 | 4.11 |
| 5 | 0.05 | | 1.0 | 0.2115 | 1.0 | 0.7885 | 0 | 3.34 |
| 5 | 0.15 | | 0.9375 | 0.7078 | 0.9375 | 0.2922 | 1 | 3.56 |
| 5 | 0.25 | | 0.6875 | 0.8939 | 0.6875 | 0.1061 | 5 | 4.62 |
| 5 | 0.30 | | 0.5 | 0.9375 | 0.5 | 0.0625 | 8 | 5.32 |
| 5 | | 0.7 | 1.0 | 0.875 | 1.0 | 0.125 | 0 | 3.85 |
| 5 | | 0.9 | 1.0 | 0.9688 | 1.0 | 0.0312 | 0 | 3.53 |
| 5 | | 0.99 | 1.0 | 1.0 | 1.0 | 0.0 | 0 | 4.36 |

**三条可行动的结论**：

1. **`top_k` 本身不是过滤器**。`top_k=5` 无阈值时，5 条里只有 1 条相关 ⇒
   **噪声注入率 80%**。把 `top_k` 调大只会让噪声更多（`top_k=10` → 90%）。
2. **绝对阈值 `min_score` 用错了会直接砍掉召回**：`min_score=0.30` 时 Recall 掉到 0.5，
   16 条查询里有 **8 条一条都检索不到**。绝对尺度与 Embedding 模型、query 形态强绑定，
   换模型就失效。
3. **相对阈值 `min_score_ratio` 是安全的**：`α=0.9` 时 Recall 仍 1.0，噪声降到 3.1%，
   **空结果 0 条**。原因在数学上：`score >= α × top1` 在候选非空时**必然保留 top1**，
   所以它**永远不会**把非空结果变成空。

> 因此：**优先用 `min_score_ratio`，不要用 `min_score`**。
> 但**本轮没有改默认值**——`knowledge_rag` 有一条刻意的设计约定：
> 「本项目不内置任何『推荐的魔法数字』，阈值必须由部署方按自己的库标定后显式配置」。
> 本轮提供的是**标定方法与实测区间**，不是新的魔法数字。

### 4.2 当前生产配置（`.env`）

```
EMBEDDING_PROVIDER=spark          # 讯飞，2560 维语义
RAG_TOP_K=8
RAG_MIN_SCORE=0.7685
RAG_MIN_SCORE_RATIO=0.9947
```

这组阈值来自 `scripts/rag_threshold_provenance.json`，是**对着同一套 2560 维模型
+ 同一份 22 篇语料**标定出来的（`corpus.sha256` 已记录）。三者的**配套关系**是可核对的：
`main.threshold_status()` 会在启动时把「产物里记的」与「现在实际的」逐项比对，
给出 `ok` / `stale` / `unknown` 三态。

⚠️ **重要限定**：本报告 §4.1 的表格是**256 维哈希占位**下测的（离线可复现），
**不能**用来评估生产检索质量——哈希是词面匹配，语义模型不是。生产阈值的可信度来自
`rag_threshold_provenance.json` 那次**真实模型标定**，不是这张表。这张表的用途是
**比较参数形态的相对优劣**（绝对 vs 相对阈值、top_k 的边际效应），这个结论与模型无关。

### 4.3 去重与低相关过滤

- **空正文**：`_to_chunks` 直接丢（不让它占掉一条 top_k 名额）。
- **重复切片**：按 `"".join(content.split())` 归一后去重（忽略空白差异）。
- **低相关**：靠 `min_score` / `min_score_ratio` 两层，二者可叠加。
- **无结果处理**：返回空列表 → Prompt 分流到 `question.txt`（**逐字节不变**），
  并在响应里 `knowledge_chunks=0`（**正向信号**，绝不把「命中 0 条」说成「已用到知识」）。

---

## 5. 全表扫描审计：到底要不要换 Chroma

### 5.1 `vector_store_sql.search()` 做了什么

```sql
SELECT * FROM knowledge_chunk
WHERE embedding IS NOT NULL
  [AND embedding_model = :model] [AND document_id = :doc]
ORDER BY id
```

然后**在 Python 侧**：逐行 `cosine_similarity(query, row.embedding)` → 排序 → 截断。
`category` 过滤也在 Python 侧（metadata 是 JSON，跨库语法不同）。没有 ANN 索引，
没有向量距离下推。模块文档自己也写着「只适合中小规模（几千条以内）」。

### 5.2 实测：规模 × 维度（同一台机器，单次查询，重复 7 次）

| 后端 | 维度 | 32 片 | 200 片 | 1000 片 | 3000 片 |
|------|------|-------|--------|---------|---------|
| sql | 256 | — | 26.1ms | 132.7ms | 459.9ms |
| chroma | 256 | — | 6.2ms | 10.1ms | 23.2ms |
| **sql** | **2560（生产）** | **53.9ms** | **320.2ms** | **1428.7ms** | **4267.3ms** |
| **chroma** | **2560（生产）** | **25.5ms** | **22.1ms** | **25.4ms** | **40.4ms** |

（维度非 0 的行用**合成随机向量**——只测后端成本，不测检索质量。成本正比于
「行数 × 维度」，与产生向量的模型无关，所以这个外推是成立的。生产是 2560 维。）

### 5.3 结论：**当前不换，但给出量化触发条件**

- **当前生产语料 22 篇 / 32 片** ⇒ SQL p50 ≈ **54ms**。
  而出题链路里 LLM 调用是 **8~19s**，54ms 占比 < 1%，**不是瓶颈**。
- **SQL 是 O(N)**：生产维度下约 **1.42ms/片**。线性外推：
  - ~140 片 ⇒ 超过 200ms
  - ~700 片 ⇒ 超过 1s
  - 3000 片 ⇒ **4.3s**（已经明显影响体验）
- **Chroma 基本是常数**（22~40ms），且排序结果与 SQL **逐条一致**
  （`backend_parity=True`：16 条查询的 top-1 与返回条数全等）⇒ ANN 只是加速，
  没有改变排序语义。

**建议的切换条件**（满足任一条即切）：

1. 切片总数 **> 300**；
2. 检索 p95 **> 200ms**（用 `/api/health` + 日志里的耗时观察）；
3. 需要按 `category` / `document_id` 收窄范围且规模继续增长（Python 侧过滤会更慢）。

**切换成本**：改一个环境变量 `VECTOR_STORE=chroma` + 执行一次
`POST /api/knowledge/rebuild`（`purge=true`），**零代码改动**。回滚 = 去掉该环境变量。

> 这正是「不要直接假定更换数据库一定更好」的答案：**它取决于规模**。
> 现在换是过度设计（还引入一个额外进程内索引要维护）；等到 300 片再换是数据驱动的决定。

---

## 6. 权限与范围过滤

### 6.1 现状（已核实）

| 能力 | 现状 |
|------|------|
| 知识库写操作（导入 / 上传 / 删除 / 重建） | `Depends(get_current_admin)` — **仅 admin** |
| 知识库读操作（列表 / 详情） | `Depends(get_current_user)` — 任意登录用户 |
| `knowledge_document` / `knowledge_chunk` 归属列 | **不存在**（无 `user_id` / `tenant_id` / `org_id`） |
| 面试链路的检索范围 | **不收窄**（全库召回） |
| 检索器是否支持收窄 | **支持**：`VectorKnowledgeRetriever(document_id=…, category=…)`，`retriever_kwargs` 可透传 |

### 6.2 判定

**今天不存在跨用户串读**：知识库是**全局共享**的单一知识库，只有 admin 能写，
所有用户检索的是同一份内容——这是产品形态，不是漏洞。

**但也没有任何强制**。真正的风险是「只加了一半」：给模型加 `user_id` 列，
却忘了让检索按 `user_id` 过滤 ⇒ 每个用户都能召回别人的知识，而且**静默**
（召回结果看起来完全正常）。

### 6.3 已加的守卫（`tests/test_rag_diagnostics.py` 第 [11] 节）

`check_scope_tripwire()` 是一个**半成品多租户 tripwire**：

- 扫描两个知识表是否出现 `user_id` / `owner_id` / `tenant_id` / `org_id` / `workspace_id`
- 扫描 `api/interview.py` 的 `retriever_kwargs` 是否带了 `document_id` / `category`
- **两者不一致 ⇒ 立刻变红**，并打印「需要什么、否则会怎样」

它**不阻止**引入多租户，只阻止**半引入**。同时它还守住两个前提：

- `api/knowledge.py` 的每个写路由都必须挂 `get_current_admin`
- `VectorKnowledgeRetriever` 必须接受 `document_id` / `category`
  （保证引入多租户时**不必改冻结文件**）

> 如果将来真要做多租户：`vector_knowledge_retriever.py` 在 `FROZEN_SOURCES` 里，
> 但**它已经支持 `document_id` / `category` 收窄**，所以只需在
> `retriever_kwargs` 里传租户范围（或新增等价的 scope 机制），不需要动冻结文件。

---

## 7. 来源追踪：面试题能否关联到检索证据

### 7.1 管道取证（`tests/rag_eval_run.py` 第 [6] 节）

| query | 模板 | 切片 | Prompt 字符 | 来源标记 | 来源可追 | 出题 ok |
|-------|------|------|-------------|----------|----------|---------|
| HashMap 的扩容机制… | `question_knowledge` | 3 | 3518 | 3 | **True** | True |
| Java 线程池的核心参数… | `question_knowledge` | 3 | 3558 | 3 | **True** | True |
| Redis 缓存穿透/击穿/雪崩… | `question_knowledge` | 3 | 3519 | 3 | **True** | True |
| Redis 的 RDB 和 AOF… | `question_knowledge` | 3 | 3470 | 3 | **True** | True |
| MySQL 索引优化 / EXPLAIN… | `question_knowledge` | 3 | 3527 | 3 | **True** | True |

- **来源全部可追：True**（`（来源：…）` 标记数 == 命中切片数 == 3，且每个来源字符串
  都逐字出现在 Prompt 里）
- 出题全部成功：True（Fake LLM 只验证**管道连通**，见 §8）

> ⚠️ 这个探针**此前有一个假红 bug**，本轮已修：`render_question_prompt()` 返回
> `(模板名, Prompt 正文)`，而探针把它写反成 `prompt, _ = ...`，于是「Prompt」其实是
> 18 字符的模板名 `question_knowledge` ⇒ 「来源可追」恒为 `False`。
> 这类 bug 的特征是**看起来像一个严重缺陷**（RAG 完全没生效），实际是测量错误。
> 修正后数字才可用。

### 7.2 检索质量与生成质量是**分开评估**的

| 维度 | 本轮是否评估 | 怎么评估 |
|------|--------------|----------|
| **检索质量** | ✅ 已评估 | Recall@K / Precision@K / MRR / nDCG@K / 噪声注入率 / 延迟分位（§4.1、§5.2） |
| **Prompt 注入** | ✅ 已评估 | 切片条数、Prompt 字符数、来源标记数、来源是否逐字可追（§7.1） |
| **生成质量** | ❌ **未评估** | 需要真实 LLM 调用 + 评判口径。离线套件用 Fake LLM，**只验管道不验质量** |

**为什么生成质量没做**：它需要（a）真实模型配额，（b）一个可复现的评判标准
（人工标注或 LLM-as-judge），（c）与 `no_rag_baseline` 的 A/B 对照。
仓库里已有 `scripts/interview_ab_comparison.json` / `no_rag_baseline.json`
这类产物，说明这条链有基础，但**本轮没有重跑**（会打真实模型、且结果不可离线复现）。
把它混进「检索质量」里报告，正是本项目一直在防的那种「因错误的原因通过」。

**建议的下一步（未做）**：用固定的 20 条查询 × 2 组（`use_rag=true/false`），
同一模型、同一 temperature，人工/评委双盲打分「题目是否引用了知识库中的具体事实」，
给出「知识引用率」与「事实一致性」两个指标。这需要一次显式的、有配额的评测窗口。

---

## 8. 离线评估集与指标定义

### 8.1 数据集

- `scripts/rag_query_set.json`：16 篇受控语料 + 16 条查询。
  每条查询 5 键：`query` / `expected_category` / `expected_keywords` / `top_k` / `min_score`。
- `scripts/interview_knowledge.json`：22 篇真实风格文档（job / technical / company / project），
  多段落、单篇远超 500 字符（因此会产生多切片）。
- **金标准是推导出来的，不是硬编码的**：`_gold_document_ids()` 按 `expected_keywords`
  在语料上**运行时匹配**，命中 ≠ 1 篇直接抛错。
  ⇒ 改语料或改关键词会**自动**让金标准跟着变，不存在「为了提分硬编码答案」的空间。

### 8.2 指标（`tests/rag_eval_metrics.py`，纯函数、零依赖）

| 指标 | 定义要点 |
|------|----------|
| `recall_at_k` | 相关集为空 ⇒ `None`（**不参与平均**，不是 0.0）；结果为空但有标准答案 ⇒ 0.0 |
| `precision_at_k` | **分母是实际返回条数**（阈值把结果砍到 1 条且相关 ⇒ 1.0，不是 1/K） |
| `reciprocal_rank` | 首个命中的倒数排名 |
| `ndcg_at_k` | 二值增益 + 对数折扣 |
| `noise_injection_rate` | `1 - precision`（**与顺序无关**，只看条数） |
| `percentile` | **最近秩法，不插值** ⇒ 结果一定是样本里出现过的值 |
| `summarize` | 显式报 `skipped`（有多少条没参与平均） |

`tests/test_rag_eval_metrics.py` 用 **8 节手算样例**钉死每个定义，并有一条
「指标必须能变差」的自检（第 [8] 节）——防止写出恒为 1 的废指标。

---

## 9. 版本记录 / 重建索引 / 安全迁移

### 9.1 现状

| 维度 | 记录在哪 | 说明 |
|------|----------|------|
| Embedding 模型 | `knowledge_chunk.embedding_model`（**逐行**） | 检索按它过滤；换模型必须重算 |
| 向量维度 | `knowledge_chunk.embedding_dim`（逐行） | 维度不符 ⇒ 跳过或抛 `VectorStoreDimensionError` |
| 阈值标定来源 | `scripts/rag_threshold_provenance.json` | 模型 + 语料 sha256 + 三个参数 + 可行区间；`main.threshold_status()` 启动时比对 |
| 在线判据 | `GET /api/health` 的 `embedding` 字段 | `provider` / `dimension` / `semantic_enabled` |
| **索引版本** | ❌ **无** | 见下 |

### 9.2 缺口：没有 `index_version`

现在能表达的是「**每行**的向量是哪来的」，但表达不了「**整个索引**是哪一次构建的」。
后果：`VECTOR_STORE` 从 `sql` 切到 `chroma` 后，如果只做了部分重建，**无法判断
ANN 索引是否完整**（只能靠 `count()` 对比权威行数）。

**建议的迁移计划（未实施，需要新列 + DDL）**：

1. 新表 `knowledge_index_version`：`id` / `backend`（sql|chroma）/ `model` / `dim`
   / `corpus_sha256` / `chunks` / `built_at` / `status`（building|ready|failed）。
   ——**新表** ⇒ `create_all` 自动建，零人工 DDL（与 `ws_ticket` 同一套路）。
2. `rebuild_index` 开始/结束各写一行；`search` 前可选校验「最新 ready 版本的
   backend/model/dim 与当前配置一致」，不一致 ⇒ 打 `WARNING` 并回落/报错（按配置）。
3. 回填：首次启动时按当前 `knowledge_chunk` 的 `(model, dim, count)` 生成一条 `ready` 版本。

**为什么这轮没做**：它需要新增表和跨模块接线，属于「新增能力」而非「修缺陷」；
本轮原则是「先建立基准，再渐进优化」，而且 §5 的结论是**当前不需要切后端**——
没有 `index_version` 的真实风险要等到真的切换时才会发生。把它写进计划比现在
半做一遍更安全。

### 9.3 重建与回滚（现在就能用的）

| 场景 | 动作 | 回滚 |
|------|------|------|
| 换 Embedding 模型 | `scripts/migrate_embeddings.py`（只改向量三列，幂等，可 `--retry-failed`） | 迁回旧模型再跑一次（旧向量已被覆盖，需重新编码） |
| ANN 索引与权威行不一致 | `POST /api/knowledge/rebuild`（`purge=true` 真正重建） | 无副作用（只重建派生索引，权威行不动） |
| 切向量后端 | 设 `VECTOR_STORE=chroma` + 一次 `rebuild` | 去掉环境变量即回 `sql`（权威行一直是对的） |
| 阈值失效 | 重跑标定 → `scripts/apply_threshold_calibration.py` 更新溯源产物 | 改回 `.env` 里的旧值 + 回退溯源产物 |

> 关键安全属性：**权威数据永远在 MySQL**。所有后端切换、索引重建都是「派生数据」的
> 操作，回滚成本是「再 rebuild 一次」，不存在数据丢失路径。

---

## 10. ★ RAG 故障与「无结果」的区分（本轮主要修复）

### 10.1 缺陷

既有降级约定是对的：**检索失败不算失败**，`ok` 不受影响，只在 `warnings` 里记
`knowledge_retrieval_failed`，返回 `knowledge_chunks=0`。但它把三种情况压成了
同一个可观测形态：

| 情况 | `knowledge_chunks` | `warnings` | 真实含义 |
|------|--------------------|------------|----------|
| 没开 RAG | 0 | 无 | Prompt 里本来就不该有知识 |
| 开了、真的零命中 | 0 | 无 | 链路正常，库里没有相关内容 |
| **开了、检索故障** | 0 | `knowledge_retrieval_failed` | **链路坏了，知识本该有却没进来** |

后果：**向量库挂了、面试题一路在无知识状态下生成，而日志里一条记录都没有**。
`api/interview.py` 的 `_run()` 只对**抛出的异常**落日志，而检索失败是被
`interview_core._gather_knowledge` 吞掉后当成**正常返回值**返回的 ⇒ `except` 永远碰不到。

### 10.2 修复：四态诊断 + 分级告警

新增 `services/rag_diagnostics.py`（**零项目依赖**，纯函数）：

```
use_rag=False ──────────────────────────────► off       （未启用，不算降级）
use_rag=True  ── 有失败告警 ───────────────► failed    （★ 降级，必须告警）
              └─ 无失败告警 ─┬─ chunks>0 ──► injected  （正常命中）
                             └─ chunks==0 ─► empty     （正常零命中）
```

**判据顺序是「先看失败告警，再看条数」**——写反就会把故障说成「零命中」，
而这正是要防的那个错误。

接入点（`api/interview.py` 的 `_observe_rag`，两个 RAG 入口都接）：

| 状态 | 日志级别 | 理由 |
|------|----------|------|
| `failed` | **WARNING** | 唯一的告警态：知识本该进来却没进来 |
| `empty` | INFO | 链路正常但零命中，值得知道，不是故障 |
| `off` / `injected` | DEBUG | 正常路径，不污染生产日志 |

日志带结构化字段：`rag_state` / `rag_degraded` / `rag_chunks` / `rag_failure_code`
/ `interview_session_id`（可定位到具体会话）。**只观察，不改任何返回值**——
响应字段集恒定的契约一字未动。可观测性自身失败也不抛异常（只记一条 `ERROR`，
说明我们自己的入参约定写错了）。

### 10.3 取证（`tests/test_rag_diagnostics.py`，11 节 60+ 项）

- 四态判定（含**优先级**：`chunks=5` + 失败码仍判 `failed`，不因条数好看就放行）
- `empty` **不是**降级（否则告警天天响，最终没人看）
- 入参严格性：`use_rag` 只收真 `bool`（`"false"` 在 Python 里是真值，转 bool 会**判反**）；
  `knowledge_chunks` 拒 `bool`（`True` 会被当成「命中 1 条」）
- **运行期接线取证**（不只 AST）：真调 `next_question` / `follow_up`（service 打桩），
  断言观测函数收到的 `use_rag` 与请求一致、`failed` 时真的打出 WARNING、
  响应体一字未改、`use_rag=false` 时判成 `off` 而不是 `failed`

---

## 11. 本轮改动清单

### 新增

| 文件 | 作用 |
|------|------|
| `backend/services/rag_diagnostics.py` | RAG 结果四态判定（纯函数，零项目依赖） |
| `backend/tests/rag_eval_metrics.py` | 检索评估指标（纯函数） |
| `backend/tests/rag_eval_run.py` | 端到端离线评估运行器（`*_run.py`，不进回归） |
| `backend/tests/test_rag_eval_metrics.py` | 指标定义守卫（8 节手算样例） |
| `backend/tests/test_rag_diagnostics.py` | 四态 + 告警接线 + 范围 tripwire |
| `backend/scripts/rag_eval_report.json` | 本轮实测产物（供复核） |

### 修改

| 文件 | 改动 |
|------|------|
| `backend/services/knowledge_import_pipeline.py` | 新增 `_remove_stale_chunks` + `removed_stale_chunks`（13 键）+ 两个稳定错误码 |
| `backend/api/interview.py` | 新增 `_observe_rag` 并接入 `/next-question`、`/follow-up` |
| `backend/schemas/knowledge.py` | `KnowledgeImportReport` 增 `removed_stale_chunks` |
| `backend/services/__init__.py` | 包文档同步 13 键与错误码 |
| `backend/tests/test_knowledge_import_pipeline.py` | 新增第 [9] 节（残留切片 13 项）+ 键数 12→13 |
| `backend/tests/test_interview_knowledge_dataset.py` / `test_knowledge_upload_api.py` | 键数文案与断言同步 |
| `.github/workflows/ci.yml` | 套件数注释 75 → **77** |

### 冻结文件

`git diff` 为空（已核对）：`interview_agent.py` / `interview_core.py` /
`interview_planner.py` / `interview_context.py` / `question_validator.py` /
`knowledge_retriever.py` / `vector_knowledge_retriever.py` / `vector_store.py` /
`vector_store_sql.py` / `vector_store_chroma.py` / `embedding_service.py` /
`embedding_provider.py` / `prompts/interview/*`。

---

## 12. 验证

| 项 | 结果 |
|----|------|
| 后端全量自检 | **77 套全部通过**（`tests/test_*.py`，SQLite） |
| 新增 `test_rag_diagnostics.py` | 全通过（11 节） |
| 新增 `test_rag_eval_metrics.py` | 全通过（8 节手算样例） |
| `test_knowledge_import_pipeline.py` | **122 项通过 / 0 失败**（原 109 + 新增 13） |
| 冻结文件指纹 | 未变动（`git diff` 空） |
| 评估运行器 | `EXIT=0`，产物已写入 `scripts/rag_eval_report.json` |

---

## 13. 未做 / 建议的下一步（按优先级）

1. **【建议做】生成质量评估**：真实模型 + 双盲评判，量化「知识引用率 / 事实一致性」。
   这是唯一还空着的一环（§7.2）。
2. **【建议做】`index_version` 表**：见 §9.2。等真的切后端时它是必需品。
3. **【可选】生产维度下的检索质量基线**：本轮质量指标跑在 256 维哈希上。
   若要评估**生产**检索质量，需用 `--respect-env` 走真实 2560 维模型
   （会消耗配额、且结果依赖模型版本，不能离线复现），建议放在一次显式评测窗口里做。
4. **【可选】`/api/health` 暴露 `vector_store` 字段**：现在只能看出「用的是哪个
   Embedding」，看不出「用的是 sql 还是 chroma」。加这个字段需要动 `metrics` 的
   12 键契约，本轮刻意没碰。
5. **【不做】把默认阈值改成 `min_score_ratio`**：违反项目「不内置魔法数字」的约定。
   正确做法是部署方按 §4.1 的方法在自己的库上标定。
