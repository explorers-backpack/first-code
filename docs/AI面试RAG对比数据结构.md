# 无 RAG / 有 RAG 实验对比 · 数据结构说明

> 对比文件：`backend/scripts/interview_ab_comparison.json`
> 生成脚本：`backend/scripts/build_interview_ab_comparison.py`
>
> 本文件是**说明**；字段与取值的权威定义在 JSON 里。本文件只讲**结构**与**口径**，
> **不评价任何生成结果的好坏**。

## 1. 这份对比解决什么问题

把两次实验（任务 66 的「无 RAG」、任务 67 的「有 RAG」）整理成**同一张表**：
**同一份输入**、**只差 RAG 条件**、**输出并排**。

因此本文件只做三件事：**对齐输入** → **标注 RAG 条件差异** → **并排输出**。
它**不**做评分、不做排序、不判断哪一臂更好。

## 2. 数据来源（全部只读，不重跑实验）

| 来源 | 文件 | 用途 |
|---|---|---|
| 无 RAG 臂 | `backend/scripts/no_rag_baseline.json` | 输入 + `rag_off` 输出 |
| 有 RAG 臂 | `backend/scripts/rag_baseline.json` | 输入 + `rag_on` 输出 + 检索内容 |
| 固定场景声明 | `backend/scripts/interview_eval_benchmark.json` | `job` / `resume` 的**原始**取值 |

脚本**只读**这三份文件，**不重跑任何实验**，也不修改 Agent / Retriever / Prompt。

## 3. 顶层结构

```
{
  name, version, purpose, scope,
  sources,        // 三份来源文件及其 name / version
  arms,           // 两个臂的定义（id / label / use_rag / 模板）
  conditions,     // 固定条件清单 + 唯一变化 + 模型配置
  comparability,  // 输入一致性的校验口径与逐场景结果
  schemas,        // input / rag / output 三段的自描述
  field_map,      // {"type": "question_type"}
  output_fields,  // 五个输出字段名
  scenarios: [...],
  summary,        // 汇总计数
  integrity       // {"checks_passed": true, "problems": []}
}
```

**两个臂**

| id | label | `use_rag` | Prompt 模板 |
|---|---|---|---|
| `rag_off` | 无 RAG | `false` | `prompts/interview/question.txt` |
| `rag_on` | 有 RAG | `true` | `prompts/interview/question_knowledge.txt` |

## 4. 每个场景的结构

```
scenarios[i] = {
  scenario_id, label,
  input,                            // ★ 只存一份，两臂共用
  input_identical_across_arms: true,// 两臂输入逐键相同的实测结论
  arms: {
    rag_off: { rag: {...}, output: {...} },
    rag_on:  { rag: {...}, output: {...} }
  }
}
```

**为什么 `input` 只存一份**：两臂输入**本来就完全相同**。存一份让「输入字段一致」
由**结构**保证——不存在「改了一边忘了另一边」的可能；再配一条
`input_identical_across_arms` 作为**实测证据**（脚本独立比对过，不依赖上游自述）。

## 5. 输入条件（`input`）

用户要的五项，外加 `current_stage`（同属固定条件）：

| 键 | 内容 |
|---|---|
| `interview_plan` | Planner（`use_llm=False`）产出的计划载荷，**7 键**：`interview_type` / `difficulty` / `total_questions` / `stages` / `target_topics` / `priority_topics` / `resume_focus_points` |
| `resume` | `content`=原始简历正文；`summary_rendered`=**实际渲染进 Prompt** 的值；两者的字符数 |
| `job` | `job_name` / `skills` / `duty`=原始岗位行；`description_rendered`=**实际渲染进 Prompt** 的值 |
| `difficulty` | 会话行声明的难度（同时也是 Agent 的期望难度） |
| `history_questions` | `context.asked_questions`（用于查重） |
| `current_stage` | `context.current_stage` |

**★ 为什么 `resume` / `job` 要同时给「原始值」和「渲染值」**：两者不是一回事——
渲染值会被**截断或拼接**（如简历摘要上限 1200 字；岗位描述由
`技能要求：{skills}；岗位职责：{duty}` 拼成）。只留渲染值会让人误判实验条件，
只留原始值又对不上 Prompt 实际内容。两个都留，差异一眼可见。

## 6. RAG 条件（`arms.*.rag`）

**无 RAG**

```json
{ "enabled": false, "prompt_template_key": "question", "knowledge_injected": false }
```

**有 RAG**

```json
{
  "enabled": true,
  "corpus_chunks_in_store": 32,
  "retrieved_chunks": [ { "rank", "score", "source", "document_id", "chunk_id",
                          "content_chars", "content_preview" }, ... ],
  "retrieved_count": 5,
  "retriever": { "class", "source_name", "model_name", "top_k", "min_score", "dedup" },
  "query_topic": "MySQL 索引",
  "knowledge_context_chars": 1981,
  "knowledge_context_lines": 5,
  "knowledge_injected": true,
  "prompt_template_key": "question_knowledge",
  "chunks_full_text_in": { "file": ".../rag_baseline.json", "path": "runs[...].rag.retrieved_chunks" }
}
```

| 键 | 含义 |
|---|---|
| `enabled` | 本臂是否注入检索知识 |
| `corpus_chunks_in_store` | **仅 `enabled=true`**：向量库里可检索的切片总数（22 篇 → 32 片） |
| `retrieved_chunks` | **仅 `enabled=true`**：Retriever 实际返回的片段，按 `rank` 排序 |
| `retrieved_chunks[].score` | 该片段的相似度分数（来自 `metadata.score`） |
| `retrieved_chunks[].content_preview` | 正文前 120 字；**全文**见 `rag_baseline.json` |
| `retriever.top_k` / `min_score` | 检索口径。本实验走 `use_rag=True` 默认组装 ⇒ `top_k=5` / `min_score=None` |
| `query_topic` | 实际用于检索的 query（即 `current_topic`） |

**★ 为什么 `retrieved_chunks` 只放预览**：全文已在 `rag_baseline.json`，
在对比文件里再存一份会让文件膨胀数倍且产生**两份可能漂移的副本**。
需要全文时按 `chunks_full_text_in` 取。

## 7. 输出（`arms.*.output`）

```json
{ "question": "", "question_type": "", "difficulty": "", "stage": "", "reason": "" }
```

| 字段 | 说明 |
|---|---|
| `question` | 生成的面试问题 |
| `question_type` | 题型。**用户口中的 `type` 就是它** ⇒ 见 `field_map` |
| `difficulty` | 题目难度 |
| `stage` | 面试阶段。**不是模型输出**，是 Core 从 `context.current_stage` 回填的 |
| `reason` | 出题理由 |

两臂的 `output` **键与键序完全一致**，可直接逐字段 diff。

## 8. 完整性校验（本次结果）

脚本每次运行都会校验并在 `integrity` 里留下结论：

| 校验 | 结果 |
|---|---|
| 两臂场景集合一致 | ✅ 3 个场景相同 |
| 每个场景 `input` 键与值逐项相同 | ✅ 3/3 `true` |
| 有 RAG 臂确实注入了知识（`knowledge_injected`） | ✅ 3/3 |
| 有 RAG 臂检索条数 > 0 | ✅ 各 5 条 |
| 各场景语料切片数一致 | ✅ 均 32 |
| 两臂模型配置一致 | ✅ 同一星火 X1 |

⇒ `integrity.checks_passed = true`、`problems = []`。

## 9. 已知口径差异（如实记录，不修改既有文件）

`no_rag_baseline.json` 的 `conditions.fixed` 写的是 **4 项**
（`interview_plan` / `user_profile` / `difficulty` / `history_questions`），
`rag_baseline.json` 写的是 **6 项**（多 `interview_session` / `current_stage`）。

这是**元数据口径不一致，不是数据不一致**——两臂的 `input` 实测**逐键相同**。
本对比文件统一采用 **6 项规范口径**（见 `conditions.fixed`），
并**刻意不去改**两份既有基线文件（它们是已交付的冻结产物）。

## 10. 怎么用 / 边界

1. **看差异**：对同一 `scenario_id`，比 `arms.rag_off.output` 与 `arms.rag_on.output`
   的五个字段；要解释差异就回看 `arms.rag_on.rag.retrieved_chunks`。
2. **重算**：`python backend/scripts/build_interview_ab_comparison.py`
   —— 幂等，只读三份输入、重写对比文件。
3. **不评价质量**：本对比**只整理数据**。两臂都用**真实 LLM**，单次结果
   **非确定性** ⇒ 不宜据单次结果下质量结论；要比较生成质量需重复采样或评委，
   **不在本文件范围内**。
4. **`min_score=None` 是条件事实**：有 RAG 臂走默认组装口径，`top_k=5` 的候选
   会全部注入（含低分无关片段）。这是**条件**，不是结论。
