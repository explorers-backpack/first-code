<script setup>
/**
 * AI 面试间 —— 完整面试闭环（作答 → 结束 → 八维报告）+ 大模型出题通道（可挂载 RAG）
 * ================================================================================
 * 本组件是面试功能在生产 UI 上的**唯一入口**，覆盖后端 `api/interview.py` 的 8 条路由：
 *
 * | 动作 | 路由 | 说明 |
 * |---|---|---|
 * | 建会话 | `POST /api/interview/create` | 仅落库会话配置，**不生成题目**，状态 `created` |
 * | 开始面试 | `POST /api/interview/{id}/start` | 一次性生成全部题目 → `ongoing`，返回第一题 |
 * | 会话详情 | `GET /api/interview/{id}` | 会话 + 全部题目 + 已提交作答（**断点恢复**用） |
 * | 当前题 | `GET /api/interview/{id}/question` | 当前待答题；`all_answered` 表示可结束 |
 * | 提交作答 | `POST /api/interview/{id}/answer` | 规则评分并落库，返回下一题 |
 * | 结束面试 | `POST /api/interview/{id}/end` | `ongoing` → `finished`，生成八维报告 |
 * | 获取报告 | `GET /api/interview/{id}/report` | 已结束会话恢复时取报告 |
 * | 大模型出题 | `POST /api/interview/{id}/next-question` | **试跑通道**，`use_rag` 是 RAG 的生产开关 |
 *
 * 注：后端没有 `POST /api/interview/start`（不带 id）这一条——`/start` 必须带 `session_id`，
 * 所以「建会话 + 开始」是两次调用，本组件把它们串成一步。
 *
 * 约定
 * ----
 * 1. **所有请求走 `apiClient`**（唯一请求层，自动带 `Authorization`），
 *    本组件不写 `fetch`、不手写 token；
 * 2. 401 / 403 **不静默、不跳转、不清 token**，只把后端给的说明展示出来；
 * 3. `/next-question` 的**成功与否看响应体的 `ok`，不看 HTTP 状态码**——
 *    它在失败时同样返回 200（字段集恒定，成功失败同形状）；
 * 4. **不宣称「已用到知识」**：接口只在**检索失败**时给 `knowledge_retrieval_failed`，
 *    **没有**「已注入知识」的正向信号，因此本页对检索状态只做「未启用 / 已请求未报失败 /
 *    检索失败」三种如实表述（见 `knowledgeStatus`）；
 * 5. **评分是纯规则评分，不是大模型评分**：`interview_core.score_answer` 按文本特征词表
 *    计分，确定性、可复现（见其 docstring）。本页如实标注为「规则评分」；
 * 6. **大模型出题通道与面试闭环是两条独立的线**：`/next-question` **不落库、不推进题号**
 *    （只产出**一道候选题**），题目持久化与状态机推进由 `/start` 与 `/answer` 负责。
 *    因此本页把「试跑通道」与「本场作答」分区展示，**不把候选题的作答混进本场记录**；
 * 7. **报告权重与后端同源**：`REPORT_DIMENSIONS` 的 7 个维度名与权重逐项对齐
 *    `interview_core.REPORT_DIMENSION_WEIGHTS`（仅用于**展示**，总分一律取后端返回的
 *    `total_score`，不在前端重算——后端在维度缺失时会重新归一化权重）。
 */

import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { apiClient } from '../api/apiClient'

const API = '/interview'

// ---------------- 枚举标签（与后端 schemas.interview 的 Literal 取值一一对应） ----------------
const TYPE_OPTIONS = [
  { value: 'comprehensive', label: '综合面试' },
  { value: 'technical', label: '技术面试' },
  { value: 'behavioral', label: '行为面试' },
]
const DIFFICULTY_OPTIONS = [
  { value: 'junior', label: '初级' },
  { value: 'mid', label: '中级' },
  { value: 'senior', label: '高级' },
]
const STATUS_LABELS = { created: '未开始', ongoing: '进行中', finished: '已结束' }

const TYPE_LABELS = TYPE_OPTIONS.reduce((acc, i) => ((acc[i.value] = i.label), acc), {})
const DIFFICULTY_LABELS = DIFFICULTY_OPTIONS.reduce((acc, i) => ((acc[i.value] = i.label), acc), {})

/**
 * 八维报告构成（**展示用**，与 `interview_core.REPORT_DIMENSION_WEIGHTS` 逐项对齐）。
 *
 * 七维加权得到总分，加上 `total_score` 本身即「八维报告」。
 * `job_match_score` 在**未指定岗位**时后端会返回 `null`，且总分按剩余维度**重新归一化**
 * ——所以本页遇到 `null` 时只标注「未计入」，绝不把它当 0 分参与任何前端计算。
 */
const REPORT_DIMENSIONS = [
  { key: 'technical_score', label: '技术能力', weight: 0.25 },
  { key: 'project_score', label: '项目经验', weight: 0.15 },
  { key: 'logic_score', label: '逻辑思维', weight: 0.15 },
  { key: 'adaptability_score', label: '应变能力', weight: 0.13 },
  { key: 'expression_score', label: '表达能力', weight: 0.12 },
  { key: 'communication_score', label: '沟通能力', weight: 0.1 },
  { key: 'job_match_score', label: '岗位匹配度', weight: 0.1 },
]

// 稳定错误码 → 可读说明（后端约定：errors 放稳定码，不放长句）
const ERROR_LABELS = {
  session_not_found: '会话不存在，或不属于当前登录用户',
  session_finished: '会话已结束，不能再出题',
  all_answered: '全部题目已作答完毕，请先结束面试并查看报告',
  agent_failed: '大模型出题失败：未产出合规题目（常见原因：文本模型未授权 / 输出无法解析）',
  validation_failed: '生成的题目未通过校验（与已问题目重复或质量不达标）',
}

// 稳定 warning 码 → 可读说明（warning 不是失败）
const WARNING_LABELS = {
  knowledge_retrieval_failed:
    '知识检索失败：已按约定静默降级为「无知识」，出题照常进行，不影响本次结果',
}

// ---------------- 岗位（可选） ----------------
const jobs = ref([])
const jobsLoading = ref(false)

// ---------------- 配置 ----------------
const form = reactive({
  job_id: null,
  interview_type: 'comprehensive',
  difficulty: 'mid',
  total_questions: 5,
  // ★ 本页存在的意义之一就是跑通 RAG 出题通道，因此**默认打开**知识库检索。
  //   关掉它即可对照「无知识」时的出题结果。
  use_rag: true,
})

// ---------------- 会话与面试闭环 ----------------
const session = ref(null)
const jobName = ref('')
const questions = ref([]) // /start 一次性生成的整场题目（用于把 question_id 映射回题号）
const answers = ref([]) // 已提交的作答（含规则评分与反馈）
const currentQuestion = ref(null) // 当前待答题；null = 无题可答
const allAnswered = ref(false) // 后端口径：可以结束并出报告
const statusMessage = ref('') // 后端给的状态提示（如「面试尚未开始，请先调用 start」）
const report = ref(null) // 八维报告

const resumeId = ref(null) // 断点恢复：输入会话 id
const starting = ref(false)
const loading = ref(false)
const submitting = ref(false)
const ending = ref(false)

// 作答草稿
const answerDraft = ref('')

// ---------------- 大模型出题通道（试跑，不进闭环） ----------------
const asking = ref(false)
const agentResult = ref(null)

const warnings = computed(() => (agentResult.value && agentResult.value.warnings) || [])
const errors = computed(() => (agentResult.value && agentResult.value.errors) || [])

const answeredCount = computed(() => answers.value.length)
const canSubmitAnswer = computed(
  () => !!currentQuestion.value && answerDraft.value.trim().length > 0 && !submitting.value
)
const isFinished = computed(() => !!session.value && session.value.status === 'finished')
const isCreated = computed(() => !!session.value && session.value.status === 'created')

/** 报告行：把 null 维度显式标出，避免「null 被当成 0 分」这类静默错误。 */
const reportRows = computed(() => {
  if (!report.value) return []
  return REPORT_DIMENSIONS.map((dim) => {
    const value = report.value[dim.key]
    return {
      ...dim,
      value,
      missing: value === null || value === undefined,
      percent: typeof value === 'number' ? Math.max(0, Math.min(100, value)) : 0,
    }
  })
})

/** 报告里「实际计入总分」的维度数（与后端 available 口径一致：非 null 即计入）。 */
const countedDimensions = computed(() => reportRows.value.filter((r) => !r.missing).length)

/**
 * 知识检索状态 —— **如实**表述。
 *
 * 后端只在**检索失败**时给 `knowledge_retrieval_failed`；**没有**「检索到几条 / 是否注入」
 * 的正向信号。所以这里刻意区分三态，且第三种不写「已用到知识」：
 * 无 warning 只能说明「没报失败」，不能推出「一定命中了语料」。
 */
const knowledgeStatus = computed(() => {
  if (!agentResult.value) return null
  if (!form.use_rag) {
    return { level: 'off', text: '本次未启用知识检索（未组装检索链路，与引入 RAG 前的行为一致）' }
  }
  if (warnings.value.includes('knowledge_retrieval_failed')) {
    return { level: 'failed', text: '知识检索失败 → 本次出题**未使用知识库**（已降级为无知识）' }
  }
  return {
    level: 'requested',
    text: '已请求知识检索，未报失败。接口不区分「命中 0 条」与「已注入知识」，故此处不宣称已用到知识。',
  }
})

function fail(e, fallback) {
  ElMessage.error((e && e.message) || fallback)
}

function labelOf(map, code) {
  return map[code] || `未收录的码：${code}`
}

/** 把 answer.question_id 映射回题号（answer 出参里没有 question_no）。 */
function questionNoOf(questionId) {
  const found = questions.value.find((q) => q.id === questionId)
  return found ? found.question_no : null
}

async function loadJobs() {
  jobsLoading.value = true
  try {
    const data = await apiClient.get('/jobs')
    jobs.value = Array.isArray(data && data.jobs) ? data.jobs : []
  } catch (e) {
    // 岗位列表只是「可选增强」，拿不到不该阻塞面试
    jobs.value = []
  } finally {
    jobsLoading.value = false
  }
}

/**
 * 载入/刷新一场面试的完整状态（**断点恢复**与流程推进共用同一个入口）。
 *
 * - `GET /{id}` 拿会话 + 全部题目 + 已作答（用于恢复与作答记录）
 * - `GET /{id}/question` 拿**权威的**当前题与 `all_answered`（不自己复算后端状态机）
 * - 已结束会话不再取当前题，改为取报告（`GET /{id}/report`）
 */
async function loadSession(sessionId) {
  loading.value = true
  try {
    const detail = await apiClient.get(`${API}/${sessionId}`)
    session.value = detail.session
    jobName.value = detail.job_name || ''
    questions.value = Array.isArray(detail.questions) ? detail.questions : []
    answers.value = Array.isArray(detail.answers) ? detail.answers : []

    if (detail.session.status === 'finished') {
      currentQuestion.value = null
      allAnswered.value = true
      statusMessage.value = '面试已结束'
      try {
        report.value = await apiClient.get(`${API}/${sessionId}/report`)
      } catch (e) {
        report.value = null
        statusMessage.value = `面试已结束，但报告获取失败：${e.message}`
      }
      return
    }

    const cur = await apiClient.get(`${API}/${sessionId}/question`)
    currentQuestion.value = cur.question || null
    allAnswered.value = !!cur.all_answered
    statusMessage.value = cur.message || ''
  } finally {
    loading.value = false
  }
}

/** 建会话 → 开始面试 → 载入状态。三步都成功才算成功。 */
async function startInterview() {
  starting.value = true
  agentResult.value = null
  report.value = null
  try {
    const created = await apiClient.post(`${API}/create`, {
      body: {
        job_id: form.job_id || undefined,
        interview_type: form.interview_type,
        difficulty: form.difficulty,
        total_questions: form.total_questions,
      },
    })
    const sessionId = created && created.session && created.session.id
    if (!sessionId) {
      ElMessage.error('创建会话未返回 session id，无法继续')
      return
    }

    await apiClient.post(`${API}/${sessionId}/start`)
    await loadSession(sessionId)
    ElMessage.success('面试已开始，题目已生成')
  } catch (e) {
    session.value = null
    fail(e, '开始面试失败')
  } finally {
    starting.value = false
  }
}

/** 恢复一场中断的面试（刷新页面 / 换设备后继续作答）。 */
async function restoreSession() {
  const id = Number(resumeId.value)
  if (!Number.isInteger(id) || id < 1) {
    ElMessage.warning('请输入有效的会话 id（正整数）')
    return
  }
  report.value = null
  agentResult.value = null
  try {
    await loadSession(id)
    ElMessage.success(`已恢复会话 #${id}`)
  } catch (e) {
    session.value = null
    fail(e, '恢复会话失败（会话不存在，或不属于当前登录用户）')
  }
}

/** 已恢复的会话若仍是 `created`，补一次 `/start`（后端允许 created → ongoing）。 */
async function beginCreatedSession() {
  if (!session.value) return
  starting.value = true
  try {
    await apiClient.post(`${API}/${session.value.id}/start`)
    await loadSession(session.value.id)
    ElMessage.success('面试已开始，题目已生成')
  } catch (e) {
    fail(e, '开始面试失败')
  } finally {
    starting.value = false
  }
}

/** 提交当前题作答 → 后端规则评分并落库 → 推进到下一题。 */
async function submitAnswer() {
  const text = answerDraft.value.trim()
  if (!text) {
    ElMessage.warning('请先输入回答内容')
    return
  }
  if (!session.value || !currentQuestion.value) return

  submitting.value = true
  try {
    const data = await apiClient.post(`${API}/${session.value.id}/answer`, {
      body: { answer_text: text },
    })
    session.value = data.session
    if (data.answer) answers.value = [...answers.value, data.answer]
    currentQuestion.value = data.next_question || null
    allAnswered.value = !!data.all_answered
    statusMessage.value = data.message || ''
    answerDraft.value = ''
    ElMessage.success(data.all_answered ? '全部题目已作答，可以生成报告了' : '回答已提交并完成规则评分')
  } catch (e) {
    fail(e, '提交回答失败')
  } finally {
    submitting.value = false
  }
}

/** 结束面试并生成八维报告（`ongoing → finished` 不可逆，故需二次确认）。 */
async function endInterview() {
  if (!session.value) return
  if (answers.value.length === 0) {
    // 后端 `build_report` 对「零作答」直接 400；这里先给出可读提示，避免无意义往返
    ElMessage.warning('尚未提交任何回答，无法生成报告（后端同样会拒绝）')
    return
  }

  try {
    await ElMessageBox.confirm(
      allAnswered.value
        ? '全部题目已作答完毕。生成报告后本场面试即结束，不可再作答。是否继续？'
        : `当前已作答 ${answers.value.length} 题，仍有题目未作答。提前结束会按已作答内容生成报告，且不可恢复。是否继续？`,
      '结束面试并生成报告',
      { type: 'warning', confirmButtonText: '结束并生成报告', cancelButtonText: '再想想' }
    )
  } catch (e) {
    return // 用户取消，不视为错误
  }

  ending.value = true
  try {
    const data = await apiClient.post(`${API}/${session.value.id}/end`)
    session.value = data.session
    report.value = data.report
    currentQuestion.value = null
    allAnswered.value = true
    statusMessage.value = data.message || '面试已结束'
    ElMessage.success('面试已结束，报告已生成')
  } catch (e) {
    fail(e, '结束面试失败')
  } finally {
    ending.value = false
  }
}

/** 大模型出题通道（试跑）。成功与否看 `ok`，不看 HTTP 200。 */
async function askNextQuestion() {
  if (!session.value) return
  asking.value = true
  try {
    const data = await apiClient.post(`${API}/${session.value.id}/next-question`, {
      body: { use_rag: form.use_rag },
    })
    agentResult.value = data
    if (data.ok) {
      ElMessage.success('已生成题目')
    } else {
      // HTTP 200 但 ok=false：这是「业务失败」，不是请求失败
      ElMessage.warning('本次未生成题目，原因见下方')
    }
  } catch (e) {
    agentResult.value = null
    fail(e, '生成题目失败')
  } finally {
    asking.value = false
  }
}

function reset() {
  session.value = null
  jobName.value = ''
  questions.value = []
  answers.value = []
  currentQuestion.value = null
  allAnswered.value = false
  statusMessage.value = ''
  report.value = null
  answerDraft.value = ''
  agentResult.value = null
  resumeId.value = null
}

onMounted(loadJobs)
</script>

<template>
  <div class="ir-page">
    <div class="ir-container glass-card">
      <!-- 头部 -->
      <div class="ir-header">
        <div class="ir-title">
          <h2>AI 面试间</h2>
          <p>作答 → 结束 → 八维报告（完整闭环）· 另含大模型出题试跑通道（可挂载 RAG 知识库）</p>
        </div>
        <div class="ir-header-actions">
          <button v-if="session" class="ghost-btn" @click="reset">重新开始</button>
        </div>
      </div>

      <!-- ============ 一、配置区（未开始） ============ -->
      <section v-if="!session" class="ir-section">
        <div class="ir-grid">
          <label class="ir-field">
            <span>目标岗位（可选）</span>
            <select v-model="form.job_id" :disabled="jobsLoading">
              <option :value="null">不指定岗位</option>
              <option v-for="job in jobs" :key="job.id" :value="job.id">{{ job.job_name }}</option>
            </select>
            <small>指定岗位后，出题会围绕该岗位的技能要求展开，报告会多出「岗位匹配度」一维</small>
          </label>

          <label class="ir-field">
            <span>面试类型</span>
            <select v-model="form.interview_type">
              <option v-for="opt in TYPE_OPTIONS" :key="opt.value" :value="opt.value">
                {{ opt.label }}
              </option>
            </select>
          </label>

          <label class="ir-field">
            <span>难度</span>
            <select v-model="form.difficulty">
              <option v-for="opt in DIFFICULTY_OPTIONS" :key="opt.value" :value="opt.value">
                {{ opt.label }}
              </option>
            </select>
          </label>

          <label class="ir-field">
            <span>计划题量</span>
            <input v-model.number="form.total_questions" type="number" min="1" max="20" />
            <small>1 ~ 20 题</small>
          </label>
        </div>

        <!-- RAG 开关（作用于下方「大模型出题试跑通道」） -->
        <div class="ir-rag-box" :class="{ on: form.use_rag }">
          <label class="ir-switch">
            <input v-model="form.use_rag" type="checkbox" />
            <span class="ir-switch-label">启用知识库检索（RAG）</span>
          </label>
          <p class="ir-rag-hint">
            打开后，每次「生成下一题」都会走
            <code>POST /api/interview/{id}/next-question</code> 且带 <code>use_rag: true</code>：
            组装 Embedding + 向量后端 + 检索器，把命中的语料注入出题 Prompt。<br />
            检索参数（<code>top_k</code> / <code>min_score</code>）不在此处传，由后端
            <code>.env</code> 的 <code>RAG_*</code> 决定。<br />
            <strong>检索失败不会让出题失败</strong>——会静默降级为「无知识」，并在下方
            <code>warnings</code> 里给出 <code>knowledge_retrieval_failed</code>。<br />
            注：本场面试的题目由 <code>/start</code> 一次性生成，<strong>RAG 试跑通道产出的候选
            题不落库、不进本场记录</strong>。
          </p>
        </div>

        <div class="ir-actions">
          <button class="primary-btn" :disabled="starting" @click="startInterview">
            {{ starting ? '正在创建并开始…' : '开始面试' }}
          </button>
        </div>

        <!-- 断点恢复 -->
        <div class="ir-resume">
          <span class="ir-resume-title">继续一场中断的面试</span>
          <div class="ir-resume-row">
            <input
              v-model.number="resumeId"
              type="number"
              min="1"
              placeholder="输入会话 id，例如 12"
              @keyup.enter="restoreSession"
            />
            <button class="ghost-btn" :disabled="loading" @click="restoreSession">
              {{ loading ? '载入中…' : '恢复会话' }}
            </button>
          </div>
          <small>
            走 <code>GET /api/interview/{id}</code> 拉回会话、全部题目与已提交作答；
            只接受<strong>属于当前登录用户</strong>的会话，他人的会话一律 404。
          </small>
        </div>
      </section>

      <!-- ============ 二、面试进行区 ============ -->
      <template v-else>
        <!-- 会话状态 -->
        <div class="ir-session-bar">
          <span class="chip">会话 #{{ session.id }}</span>
          <span class="chip">{{ jobName || '未指定岗位' }}</span>
          <span class="chip">{{ labelOf(TYPE_LABELS, session.interview_type) }}</span>
          <span class="chip">{{ labelOf(DIFFICULTY_LABELS, session.difficulty) }}</span>
          <span class="chip" :class="`status-${session.status}`">
            {{ labelOf(STATUS_LABELS, session.status) }}
          </span>
          <span class="chip">
            进度 {{ answeredCount }} / {{ session.total_questions }} 已作答
          </span>
        </div>

        <p v-if="statusMessage" class="ir-status-msg">{{ statusMessage }}</p>

        <!-- created：已恢复但尚未开始 -->
        <section v-if="isCreated" class="ir-section">
          <h3 class="ir-section-title">这场面试还未开始</h3>
          <p class="ir-empty">
            会话已存在但题目尚未生成。点下方按钮调用
            <code>POST /api/interview/{{ session.id }}/start</code> 一次性生成全部题目。
          </p>
          <div class="ir-actions left">
            <button class="primary-btn" :disabled="starting" @click="beginCreatedSession">
              {{ starting ? '正在生成题目…' : '开始这场面试' }}
            </button>
          </div>
        </section>

        <!-- ============ 二·一、作答闭环 ============ -->
        <section v-if="!isCreated" class="ir-section">
          <h3 class="ir-section-title">
            本场作答
            <span class="tag">/start 一次性生成全部题目 · 逐题作答</span>
          </h3>

          <!-- 当前题 -->
          <div v-if="currentQuestion" class="q-card plain">
            <div class="q-head">
              <span class="q-no">第 {{ currentQuestion.question_no }} / {{ session.total_questions }} 题</span>
              <span class="chip">{{ currentQuestion.question_type }}</span>
              <span v-if="currentQuestion.topic" class="chip">主题：{{ currentQuestion.topic }}</span>
              <span v-if="currentQuestion.difficulty" class="chip">
                难度：{{ currentQuestion.difficulty }}
              </span>
            </div>
            <p class="q-text">{{ currentQuestion.question }}</p>
            <div
              v-if="currentQuestion.expected_points && currentQuestion.expected_points.length"
              class="q-points"
            >
              <span class="q-points-title">考察点</span>
              <ul>
                <li v-for="(point, index) in currentQuestion.expected_points" :key="index">
                  {{ point }}
                </li>
              </ul>
            </div>
          </div>

          <!-- 作答输入 -->
          <div v-if="currentQuestion" class="ir-answer-box">
            <textarea
              v-model="answerDraft"
              rows="6"
              maxlength="10000"
              placeholder="在此输入你的回答。提交后由后端规则评分（确定性口径，非大模型评分）并落库，一题只能作答一次。"
            ></textarea>
            <div class="ir-answer-foot">
              <span class="ir-counter">{{ answerDraft.length }} / 10000</span>
              <button class="primary-btn" :disabled="!canSubmitAnswer" @click="submitAnswer">
                {{ submitting ? '提交并评分中…' : '提交回答' }}
              </button>
            </div>
          </div>

          <!-- 全部作答完毕 -->
          <div v-if="!currentQuestion && allAnswered" class="ir-done-box">
            <p class="ir-done-text">
              全部 {{ session.total_questions }} 题已作答完毕。点下方按钮调用
              <code>POST /api/interview/{{ session.id }}/end</code> 生成八维报告。
            </p>
            <div class="ir-actions left">
              <button class="primary-btn" :disabled="ending" @click="endInterview">
                {{ ending ? '正在生成报告…' : '结束面试并生成报告' }}
              </button>
            </div>
          </div>

          <!-- 进行中：提前结束 -->
          <div v-if="!isFinished && currentQuestion" class="ir-actions left ir-end-early">
            <button class="ghost-btn" :disabled="ending || answeredCount === 0" @click="endInterview">
              {{ ending ? '正在生成报告…' : '提前结束面试并生成报告' }}
            </button>
            <small v-if="answeredCount === 0">至少作答 1 题才能生成报告</small>
          </div>
        </section>

        <!-- ============ 二·二、已作答记录 ============ -->
        <section v-if="answers.length" class="ir-section">
          <h3 class="ir-section-title">
            已作答记录
            <span class="tag">{{ answers.length }} 题 · 规则评分</span>
          </h3>

          <div v-for="item in answers" :key="item.id" class="ir-record">
            <div class="ir-record-head">
              <span class="chip">第 {{ questionNoOf(item.question_id) ?? '?' }} 题</span>
              <span class="chip score">得分 {{ item.score ?? '—' }}</span>
              <span class="chip">技术 {{ item.technical_score ?? '—' }}</span>
              <span class="chip">逻辑 {{ item.logic_score ?? '—' }}</span>
              <span class="chip">表达 {{ item.expression_score ?? '—' }}</span>
              <span class="chip">应变 {{ item.adaptability_score ?? '—' }}</span>
            </div>
            <p class="ir-record-answer">{{ item.answer_text }}</p>
            <p v-if="item.feedback" class="ir-record-feedback">
              <strong>点评：</strong>{{ item.feedback }}
            </p>
          </div>
        </section>

        <!-- ============ 二·三、八维报告 ============ -->
        <section v-if="report" class="ir-section">
          <h3 class="ir-section-title">
            面试报告（八维）
            <span class="tag">{{ countedDimensions }} / 7 维计入加权</span>
          </h3>

          <div class="ir-report-total">
            <span class="ir-total-num">{{ report.total_score ?? '—' }}</span>
            <span class="ir-total-unit">总分</span>
          </div>

          <div class="ir-report-bars">
            <div v-for="row in reportRows" :key="row.key" class="ir-bar-row">
              <span class="ir-bar-label">
                {{ row.label }}
                <small>权重 {{ Math.round(row.weight * 100) }}%</small>
              </span>
              <span class="ir-bar-track">
                <span
                  v-if="!row.missing"
                  class="ir-bar-fill"
                  :style="{ width: row.percent + '%' }"
                ></span>
              </span>
              <span class="ir-bar-value">
                {{ row.missing ? '未计入' : row.value }}
              </span>
            </div>
          </div>

          <p class="ir-report-note">
            总分由后端按 <code>REPORT_DIMENSION_WEIGHTS</code> 加权得出；维度缺失时（如未指定岗位
            导致「岗位匹配度」为 null）后端会<strong>重新归一化权重</strong>，因此本页不自行重算总分。
          </p>

          <div class="ir-report-grid">
            <div class="ir-report-block">
              <h4>亮点</h4>
              <ul>
                <li v-for="(text, index) in report.strengths || []" :key="index">{{ text }}</li>
              </ul>
            </div>
            <div class="ir-report-block">
              <h4>不足</h4>
              <ul>
                <li v-for="(text, index) in report.weaknesses || []" :key="index">{{ text }}</li>
              </ul>
            </div>
          </div>

          <div v-if="report.suggestions" class="ir-report-block suggestions">
            <h4>改进建议</h4>
            <p class="ir-suggestions">{{ report.suggestions }}</p>
          </div>

          <details class="ir-raw">
            <summary>查看报告原始响应</summary>
            <pre>{{ JSON.stringify(report, null, 2) }}</pre>
          </details>
        </section>

        <!-- ============ 二·四、大模型出题试跑通道 ============ -->
        <section class="ir-section ir-trial">
          <h3 class="ir-section-title">
            大模型出题试跑通道
            <span class="tag">/next-question · 不落库 · 不影响本场面试</span>
          </h3>
          <p class="ir-empty">
            该通道走 Core 编排的「上下文 → 计划 → 知识检索 → Agent → Validator」生成
            <strong>一道候选题</strong>，用于验证 RAG 出题效果。
            <strong>它不落库、不推进题号</strong>，产出不会出现在上方的本场作答与报告里。
          </p>

          <div class="ir-actions left">
            <button class="primary-btn" :disabled="asking" @click="askNextQuestion">
              {{ asking ? '生成中…（真实模型约 10~30 秒）' : '生成一道候选题' }}
            </button>
            <label class="ir-switch inline">
              <input v-model="form.use_rag" type="checkbox" />
              <span class="ir-switch-label">启用知识库</span>
            </label>
          </div>

          <!-- 知识检索状态（如实表述，不宣称「已用到知识」） -->
          <div v-if="knowledgeStatus" class="ir-knowledge" :class="knowledgeStatus.level">
            <strong>知识检索：</strong>{{ knowledgeStatus.text }}
          </div>

          <!-- 结果 -->
          <template v-if="agentResult">
            <div class="q-card" :class="agentResult.ok ? 'ok' : 'bad'">
              <template v-if="agentResult.ok">
                <p class="q-text">{{ agentResult.question }}</p>
                <div class="q-meta">
                  <span v-if="agentResult.question_no" class="chip">
                    第 {{ agentResult.question_no }} 题
                  </span>
                  <span v-if="agentResult.question_type" class="chip">
                    {{ agentResult.question_type }}
                  </span>
                  <span v-if="agentResult.topic" class="chip">主题：{{ agentResult.topic }}</span>
                  <span v-if="agentResult.difficulty" class="chip">
                    难度：{{ agentResult.difficulty }}
                  </span>
                  <span v-if="agentResult.stage" class="chip">阶段：{{ agentResult.stage }}</span>
                </div>
                <div
                  v-if="agentResult.expected_points && agentResult.expected_points.length"
                  class="q-points"
                >
                  <span class="q-points-title">考察点</span>
                  <ul>
                    <li v-for="(point, index) in agentResult.expected_points" :key="index">
                      {{ point }}
                    </li>
                  </ul>
                </div>
                <p v-if="agentResult.reason" class="q-reason">
                  <strong>出题理由：</strong>{{ agentResult.reason }}
                </p>
              </template>
              <template v-else>
                <p class="q-fail">本次未生成题目（<code>ok = false</code>，但 HTTP 仍是 200）。</p>
                <p v-if="agentResult.error" class="q-fail-detail">{{ agentResult.error }}</p>
              </template>
            </div>

            <!-- warnings：不是失败，但要如实展示 -->
            <div v-if="warnings.length" class="ir-warnings">
              <h4>warnings（不影响出题成败）</h4>
              <ul>
                <li v-for="code in warnings" :key="code">
                  <code>{{ code }}</code> —— {{ labelOf(WARNING_LABELS, code) }}
                </li>
              </ul>
            </div>

            <!-- errors：稳定错误码 -->
            <div v-if="errors.length" class="ir-errors">
              <h4>errors（稳定错误码）</h4>
              <ul>
                <li v-for="code in errors" :key="code">
                  <code>{{ code }}</code> —— {{ labelOf(ERROR_LABELS, code) }}
                </li>
              </ul>
            </div>

            <details class="ir-raw">
              <summary>查看接口原始响应</summary>
              <pre>{{ JSON.stringify(agentResult, null, 2) }}</pre>
            </details>
          </template>

          <p v-else class="ir-empty">
            还没有生成候选题。点「生成一道候选题」走一次大模型出题通道。
          </p>
        </section>
      </template>
    </div>
  </div>
</template>

<style scoped>
.ir-page {
  position: relative;
  z-index: 1;
  /* 与 .kb-page / .resume-page 同款：自己当滚动容器。
     `.app-container` 是 height:100vh + overflow:hidden，不这样写内容会被裁掉且无法滚动。 */
  height: 100vh;
  overflow-y: auto;
  display: flex;
  align-items: flex-start;
  justify-content: center;
  padding: 100px 20px 40px;
}

.ir-container {
  width: 100%;
  max-width: 980px;
  padding: 28px 32px 32px;
  font-family: var(--font-body);
  color: var(--text-primary);
}

.ir-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  flex-wrap: wrap;
  padding-bottom: 18px;
  border-bottom: 1px solid var(--border-glass);
}

.ir-title h2 {
  margin: 0 0 6px;
  font-size: 22px;
  font-weight: 600;
}

.ir-title p {
  margin: 0;
  font-size: 13px;
  color: var(--text-secondary);
}

.ir-section {
  margin-top: 22px;
}

.ir-section-title {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  margin: 0 0 12px;
  font-size: 16px;
  font-weight: 600;
}

.tag {
  padding: 2px 10px;
  font-size: 12px;
  font-weight: 400;
  color: var(--text-secondary);
  background: rgba(201, 162, 39, 0.1);
  border-radius: 12px;
}

/* ---------------- 配置区 ---------------- */
.ir-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 16px;
}

.ir-field {
  display: flex;
  flex-direction: column;
  gap: 6px;
  font-size: 13px;
}

.ir-field > span {
  color: var(--text-secondary);
}

.ir-field select,
.ir-field input {
  padding: 9px 12px;
  font-family: inherit;
  font-size: 14px;
  color: var(--text-primary);
  background: rgba(255, 255, 255, 0.85);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-sm);
  outline: none;
}

.ir-field select:focus,
.ir-field input:focus {
  border-color: var(--border-glass-strong);
}

.ir-field small {
  font-size: 12px;
  color: var(--text-muted);
}

.ir-rag-box {
  margin-top: 18px;
  padding: 16px 18px;
  background: rgba(122, 117, 109, 0.05);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-md);
  transition: all 0.2s;
}

.ir-rag-box.on {
  background: rgba(0, 168, 150, 0.06);
  border-color: rgba(0, 168, 150, 0.3);
}

.ir-switch {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
}

.ir-switch input {
  width: 16px;
  height: 16px;
  accent-color: var(--accent-cyan);
  cursor: pointer;
}

.ir-switch-label {
  font-size: 14px;
  font-weight: 500;
}

.ir-switch.inline .ir-switch-label {
  font-size: 13px;
  font-weight: 400;
  color: var(--text-secondary);
}

.ir-rag-hint {
  margin: 10px 0 0;
  font-size: 12.5px;
  line-height: 1.7;
  color: var(--text-secondary);
}

.ir-rag-hint code,
.q-fail code,
.ir-warnings code,
.ir-errors code,
.ir-status-msg code,
.ir-empty code,
.ir-done-text code,
.ir-report-note code,
.ir-resume small code {
  padding: 1px 5px;
  font-size: 12px;
  background: rgba(45, 42, 38, 0.06);
  border-radius: 4px;
}

/* ---------------- 断点恢复 ---------------- */
.ir-resume {
  margin-top: 26px;
  padding-top: 20px;
  border-top: 1px dashed var(--border-glass);
  font-size: 13px;
}

.ir-resume-title {
  display: block;
  margin-bottom: 10px;
  font-weight: 600;
  color: var(--text-secondary);
}

.ir-resume-row {
  display: flex;
  gap: 10px;
  align-items: center;
  flex-wrap: wrap;
}

.ir-resume-row input {
  width: 220px;
  padding: 9px 12px;
  font-family: inherit;
  font-size: 14px;
  color: var(--text-primary);
  background: rgba(255, 255, 255, 0.85);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-sm);
  outline: none;
}

.ir-resume small {
  display: block;
  margin-top: 10px;
  font-size: 12px;
  line-height: 1.7;
  color: var(--text-muted);
}

/* ---------------- 会话状态条 ---------------- */
.ir-session-bar {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 18px;
}

.ir-status-msg {
  margin: 12px 0 0;
  font-size: 13px;
  color: var(--text-secondary);
}

.chip {
  padding: 4px 12px;
  font-size: 12.5px;
  color: var(--text-secondary);
  background: rgba(255, 255, 255, 0.7);
  border: 1px solid var(--border-glass);
  border-radius: 12px;
}

.chip.status-ongoing {
  color: var(--accent-cyan);
  background: rgba(0, 168, 150, 0.1);
  border-color: rgba(0, 168, 150, 0.3);
}

.chip.status-finished {
  color: var(--text-muted);
}

.chip.score {
  color: var(--accent-gold);
  background: rgba(201, 162, 39, 0.12);
  border-color: rgba(201, 162, 39, 0.3);
}

/* ---------------- 按钮 ---------------- */
.ir-actions {
  display: flex;
  align-items: center;
  gap: 14px;
  flex-wrap: wrap;
  margin-top: 22px;
}

.ir-actions.left {
  justify-content: flex-start;
  margin-top: 0;
  margin-bottom: 14px;
}

.ir-end-early {
  margin-top: 18px;
  margin-bottom: 0;
}

.ir-end-early small {
  font-size: 12px;
  color: var(--text-muted);
}

.primary-btn,
.ghost-btn {
  font-family: inherit;
  cursor: pointer;
  transition: all 0.2s;
}

.primary-btn {
  padding: 10px 22px;
  font-size: 14px;
  color: #fff;
  background: var(--accent-gold);
  border: 1px solid var(--accent-gold);
  border-radius: var(--radius-sm);
}

.primary-btn:hover:not(:disabled) {
  background: var(--accent-gold-light);
}

.ghost-btn {
  padding: 8px 16px;
  font-size: 14px;
  color: var(--text-secondary);
  background: rgba(255, 255, 255, 0.7);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-sm);
}

.ghost-btn:hover:not(:disabled) {
  color: var(--accent-gold);
  border-color: var(--border-glass-strong);
}

.primary-btn:disabled,
.ghost-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

/* ---------------- 题目卡片 ---------------- */
.q-card {
  padding: 18px 20px;
  background: rgba(255, 255, 255, 0.75);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-md);
}

.q-card.plain {
  background: rgba(201, 162, 39, 0.04);
}

.q-card.ok {
  background: rgba(0, 168, 150, 0.05);
  border-color: rgba(0, 168, 150, 0.28);
}

.q-card.bad {
  background: rgba(232, 93, 61, 0.06);
  border-color: rgba(232, 93, 61, 0.3);
}

.q-head {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
}

.q-no {
  font-size: 13px;
  font-weight: 600;
  color: var(--accent-gold);
}

.q-text {
  margin: 0 0 12px;
  font-size: 15px;
  line-height: 1.75;
}

.q-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.q-points {
  margin-top: 14px;
}

.q-points-title {
  font-size: 13px;
  font-weight: 600;
  color: var(--text-secondary);
}

.q-points ul {
  margin: 6px 0 0;
  padding-left: 20px;
  font-size: 13.5px;
  line-height: 1.8;
  color: var(--text-primary);
}

.q-reason {
  margin: 14px 0 0;
  font-size: 13px;
  line-height: 1.7;
  color: var(--text-secondary);
}

.q-fail {
  margin: 0;
  font-size: 14px;
}

.q-fail-detail {
  margin: 8px 0 0;
  font-size: 13px;
  color: var(--text-secondary);
}

/* ---------------- 作答区 ---------------- */
.ir-answer-box {
  margin-top: 14px;
}

.ir-answer-box textarea {
  width: 100%;
  box-sizing: border-box;
  padding: 14px 16px;
  font-family: inherit;
  font-size: 14px;
  line-height: 1.8;
  color: var(--text-primary);
  background: rgba(255, 255, 255, 0.85);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-md);
  outline: none;
  resize: vertical;
}

.ir-answer-box textarea:focus {
  border-color: var(--border-glass-strong);
}

.ir-answer-foot {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  margin-top: 10px;
}

.ir-counter {
  font-size: 12px;
  color: var(--text-muted);
}

.ir-done-box {
  margin-top: 14px;
  padding: 18px 20px;
  background: rgba(0, 168, 150, 0.06);
  border: 1px solid rgba(0, 168, 150, 0.28);
  border-radius: var(--radius-md);
}

.ir-done-text {
  margin: 0;
  font-size: 14px;
  line-height: 1.8;
}

/* ---------------- 作答记录 ---------------- */
.ir-record {
  margin-bottom: 14px;
  padding: 16px 18px;
  background: rgba(255, 255, 255, 0.7);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-md);
}

.ir-record-head {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 10px;
}

.ir-record-answer {
  margin: 0;
  font-size: 13.5px;
  line-height: 1.8;
  color: var(--text-primary);
  white-space: pre-wrap;
  word-break: break-word;
}

.ir-record-feedback {
  margin: 10px 0 0;
  font-size: 13px;
  line-height: 1.8;
  color: var(--text-secondary);
}

/* ---------------- 报告 ---------------- */
.ir-report-total {
  display: flex;
  align-items: baseline;
  gap: 8px;
  padding: 18px 20px;
  margin-bottom: 18px;
  background: rgba(201, 162, 39, 0.08);
  border: 1px solid rgba(201, 162, 39, 0.3);
  border-radius: var(--radius-md);
}

.ir-total-num {
  font-size: 40px;
  font-weight: 700;
  line-height: 1;
  color: var(--accent-gold);
}

.ir-total-unit {
  font-size: 14px;
  color: var(--text-secondary);
}

.ir-report-bars {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.ir-bar-row {
  display: grid;
  grid-template-columns: 160px 1fr 64px;
  align-items: center;
  gap: 12px;
  font-size: 13px;
}

.ir-bar-label {
  display: flex;
  flex-direction: column;
  color: var(--text-primary);
}

.ir-bar-label small {
  font-size: 11.5px;
  color: var(--text-muted);
}

.ir-bar-track {
  position: relative;
  height: 10px;
  overflow: hidden;
  background: rgba(45, 42, 38, 0.07);
  border-radius: 6px;
}

.ir-bar-fill {
  display: block;
  height: 100%;
  background: linear-gradient(90deg, var(--accent-cyan), var(--accent-gold));
  border-radius: 6px;
  transition: width 0.4s ease;
}

.ir-bar-value {
  text-align: right;
  font-weight: 600;
  color: var(--text-primary);
}

.ir-report-note {
  margin: 16px 0 0;
  font-size: 12.5px;
  line-height: 1.8;
  color: var(--text-muted);
}

.ir-report-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
  gap: 14px;
  margin-top: 18px;
}

.ir-report-block {
  padding: 16px 18px;
  background: rgba(255, 255, 255, 0.7);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-md);
}

.ir-report-block.suggestions {
  margin-top: 14px;
}

.ir-report-block h4 {
  margin: 0 0 10px;
  font-size: 13.5px;
  font-weight: 600;
}

.ir-report-block ul {
  margin: 0;
  padding-left: 20px;
  font-size: 13px;
  line-height: 1.9;
}

.ir-suggestions {
  margin: 0;
  font-size: 13px;
  line-height: 1.9;
  white-space: pre-line;
}

/* ---------------- 试跑通道 ---------------- */
.ir-trial {
  padding-top: 22px;
  border-top: 1px dashed var(--border-glass);
}

/* ---------------- 知识检索状态 ---------------- */
.ir-knowledge {
  margin-bottom: 14px;
  padding: 11px 14px;
  font-size: 13px;
  line-height: 1.7;
  border-radius: var(--radius-sm);
}

.ir-knowledge.requested {
  color: var(--accent-cyan);
  background: rgba(0, 168, 150, 0.08);
  border: 1px solid rgba(0, 168, 150, 0.25);
}

.ir-knowledge.failed {
  color: var(--warning);
  background: rgba(232, 93, 61, 0.08);
  border: 1px solid rgba(232, 93, 61, 0.28);
}

.ir-knowledge.off {
  color: var(--text-secondary);
  background: rgba(122, 117, 109, 0.07);
  border: 1px solid var(--border-glass);
}

/* ---------------- warnings / errors ---------------- */
.ir-warnings,
.ir-errors {
  margin-top: 14px;
  padding: 14px 16px;
  border-radius: var(--radius-sm);
}

.ir-warnings {
  background: rgba(201, 162, 39, 0.07);
  border: 1px solid var(--border-glass-strong);
}

.ir-errors {
  background: rgba(232, 93, 61, 0.07);
  border: 1px solid rgba(232, 93, 61, 0.3);
}

.ir-warnings h4,
.ir-errors h4 {
  margin: 0 0 8px;
  font-size: 13px;
  font-weight: 600;
}

.ir-warnings ul,
.ir-errors ul {
  margin: 0;
  padding-left: 20px;
  font-size: 13px;
  line-height: 1.8;
}

.ir-raw {
  margin-top: 16px;
  font-size: 13px;
  color: var(--text-secondary);
}

.ir-raw summary {
  cursor: pointer;
}

.ir-raw pre {
  max-height: 320px;
  margin: 10px 0 0;
  padding: 14px;
  overflow: auto;
  font-size: 12px;
  line-height: 1.6;
  color: var(--text-primary);
  background: rgba(45, 42, 38, 0.04);
  border-radius: var(--radius-sm);
}

.ir-empty {
  margin: 0;
  font-size: 13.5px;
  line-height: 1.8;
  color: var(--text-muted);
}
</style>
