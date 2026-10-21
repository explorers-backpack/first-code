<script setup>
/**
 * AI 面试间 —— 完整面试闭环（作答 → 结束 → 八维报告）+ 大模型出题通道（RAG 默认开启）
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
 * | 大模型出题 | `POST /api/interview/{id}/next-question` | **试跑通道**，RAG **默认开启**（后端契约层默认） |
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
 * 4. **知识检索状态如实表述**：后端除失败信号 `knowledge_retrieval_failed` 外，
 *    还会回传正向信号 `knowledge_chunks`（本次真正注入 Prompt 的切片条数）。
 *    RAG 已**默认开启**（本页不再提供开关），因此本页只区分**三态**——
 *    已注入 N 条 / 命中 0 条 / 检索失败（见 `knowledgeStatus`）。
 *    **不把「命中 0 条」说成「已用到知识」**；
 * 5. **评分是纯规则评分，不是大模型评分**：`interview_core.score_answer` 按文本特征词表
 *    计分，确定性、可复现（见其 docstring）。本页如实标注为「规则评分」；
 * 6. **大模型出题通道与面试闭环是两条独立的线**：`/next-question` **不落库、不推进题号**
 *    （只产出**一道候选题**），题目持久化与状态机推进由 `/start` 与 `/answer` 负责。
 *    因此本页把「试跑通道」与「本场作答」分区展示，**不把候选题的作答混进本场记录**；
 * 7. **报告权重与后端同源**：`REPORT_DIMENSIONS` 的 7 个维度名与权重逐项对齐
 *    `interview_core.REPORT_DIMENSION_WEIGHTS`（仅用于**展示**，总分一律取后端返回的
 *    `total_score`，不在前端重算——后端在维度缺失时会重新归一化权重）。
 *
 * 面试模式（本次新增）
 * -------------------
 * 配置区新增「面试模式」，`startInterview` 把它作为 `mode` 传给
 * `POST /api/interview/create`（后端字段名 `interview_mode`，两种键名都接受）。
 *
 * | 模式 | 值 | 走哪条通道 | 作答方式 |
 * |---|---|---|---|
 * | 文字面试 | `text` | HTTP（与改造前**完全一致**） | 打字 |
 * | 视频面试 | `avatar` | WebSocket `/api/interview/{id}/ws` | 数字人播报 + 语音(ASR) / 打字兜底 |
 *
 * 关键约束：**两种模式共用同一套面试核心**（出题 / 评分 / 报告 / 状态机全不分流），
 * 差别只在「怎么交付」。因此：
 * - `mode === 'text'` 时本组件的行为与改造前**逐字节相同**（多出来的分支都带
 *   `isAvatarMode` 条件，文字路径一个判断都不会命中）；
 * - `mode === 'avatar'` 时**不再调** HTTP `/start`（题目由实时通道的 `start` 消息
 *   触发，服务端会先做数字人预检再出题），作答、结束也都走通道。
 *
 * 视频模式额外渲染（用户要求的五项）：
 * - **数字人展示区域**：`avatar` 事件（`provider` / `configured` / `session_probed` /
 *   `status` / `stream_url`）。注意「未探测」（`session_probed === false`，`ok === null`）
 *   与「探测失败」（`ok === false`）是**两件事**，界面必须分开说；
 * - **视频流播放器**：`<video>` 绑定 `stream_url`（Mock 下是 `mock://stream`，
 *   浏览器放不出来属正常，因此播放器不可用时**降级为占位提示**而不是空白）；
 * - **当前问题**：`question` 事件的 `question`（与 HTTP 契约**同一形状**）；
 * - **回答状态**：`answer` 事件的 `submitted` / `rejected`（含 ASR 明细与拒收原因）；
 * - **面试状态**：`state` 事件的编排状态（5 态，`ORCH_STATE_LABELS` 给中文）。
 *
 * 通道的关闭码按语义分开提示（4401 未登录 / 4403 无权限 / 4404 会话不存在），
 * **不在本组件跳转、不清 token**——与全项目的登录态约定一致。
 */

import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { apiClient } from '../api/apiClient'
import {
  ANSWER_REJECTED,
  ANSWER_SUBMITTED,
  AVATAR_STATUS_LABELS,
  InterviewSocket,
  ORCH_STATE_LABELS,
  SocketAuthError,
  SocketTicketError,
  WS_CLOSE_BUSY,
  WS_CLOSE_FORBIDDEN,
  WS_CLOSE_IDLE,
  WS_CLOSE_NOT_FOUND,
  WS_CLOSE_UNAUTHORIZED,
} from '../api/interviewSocket'

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

// ---------------- 面试模式（与后端 schemas.interview.InterviewMode 取值一一对应） ----------------
// text   = 文字面试：打字作答，走 HTTP —— **现有 UI 与流程一行未改**
// avatar = 视频面试：数字人播报 + 实时通道（WebSocket），语音(ASR)作答 / 打字兜底
const MODE_TEXT = 'text'
const MODE_AVATAR = 'avatar'
const MODE_OPTIONS = [
  { value: MODE_TEXT, label: '文字面试' },
  { value: MODE_AVATAR, label: '视频面试' },
]

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
  // ★ 交互模式：默认 text ⇒ 不选就是改造前的行为，文字面试零变化。
  mode: MODE_TEXT,
  interview_type: 'comprehensive',
  difficulty: 'mid',
  total_questions: 5,
  // ★ 知识库检索（RAG）已由**后端契约层默认开启**，本页不再提供开关：
  //   请求体不带 `use_rag` ⇒ 后端 `NextQuestionRequest.use_rag` 的默认值（True）生效。
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
// 视频模式：作答请求已发出、还没等到 `answer` 事件（用于禁用按钮，避免重复提交）
const answerPending = ref(false)

// 作答草稿
const answerDraft = ref('')

// ---------------- 视频面试：实时通道状态（仅 avatar 模式会用到） ----------------
// ★ `activeSocket` 刻意是**普通变量**而不是 ref：Vue 的 ref 会用 Proxy 深包裹对象，
//   把 WebSocket 实例塞进去会在 readyState / send 等路径上多一层代理，得不偿失。
//   界面需要的是**派生出来的**几个响应式状态（下面这些 ref），而不是实例本身。
let activeSocket = null

const socketConnected = ref(false) // 通道是否已连上（用于按钮可用性）
const socketNotice = ref('') // 通道提示（连接中 / 断开原因 / 重连耗尽）
const helloInfo = ref(null) // hello 事件（服务端自报的协议能力）
const orchState = ref(null) // state 事件 → 面试状态
const avatarState = ref(null) // avatar 事件 → 数字人状态
const answerStatus = ref(null) // answer 事件 → 回答状态（submitted / rejected）
const lastScore = ref(null) // score 事件 → 最近一题的评分
const wsError = ref(null) // error 事件（稳定错误码 + 可读说明）

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

// ---------------- 视频面试的派生状态（模式判定 + 五项展示） ----------------
/**
 * 当前会话的交互模式。**以库中的 `interview_mode` 为准**（不是 `form.mode`）——
 * 恢复会话 / 刷新页面后 `form.mode` 已回到默认值，只有会话上的字段才是真相。
 * 老会话（改造前建的）该列为 `text`，因此 `isAvatarMode` 恒为 false ⇒ 文字路径不变。
 */
const sessionMode = computed(() => (session.value && session.value.interview_mode) || MODE_TEXT)
const isAvatarMode = computed(() => sessionMode.value === MODE_AVATAR)

/** 面试状态（编排层 5 态）。未收到 `state` 事件时给占位符，不假装是「准备中」。 */
const orchStateLabel = computed(() => {
  if (!orchState.value) return '—'
  return ORCH_STATE_LABELS[orchState.value] || `未收录的编排状态：${orchState.value}`
})

/** 视频流地址（数字人服务端下发；Mock provider 给的是 `mock://stream`）。 */
const avatarStreamUrl = computed(() => (avatarState.value && avatarState.value.stream_url) || '')

/**
 * 该流地址浏览器能否真的播。
 *
 * Mock provider 下发的是 `mock://stream` 这类**自定义协议**——`<video>` 挂上去只会
 * 得到一个破图。所以这里按协议白名单判断，不能播就渲染占位区并**说明原因**，
 * 而不是把破播放器摆在那儿让人以为是功能坏了。
 */
const avatarStreamPlayable = computed(() => /^(https?:|blob:|data:)/i.test(avatarStreamUrl.value))

/**
 * 数字人状态文案 —— 三种情况**必须分开说**：
 * - 没收到 `avatar` 事件：不知道；
 * - `session_probed === false`：**尚未预检**（不是失败！`ok` 为 null）；
 * - 有 `status`：按通道层的 7 档给中文。
 */
const avatarStatusText = computed(() => {
  const st = avatarState.value
  if (!st) return '尚未收到数字人状态'
  if (st.session_probed === false) return '数字人尚未预检（发出「开始」后由服务端探测）'
  if (st.status) return AVATAR_STATUS_LABELS[st.status] || `未收录的通道状态：${st.status}`
  return st.ok ? '数字人已就绪' : '数字人未就绪'
})

/** 回答状态文案（`answer` 事件的提交结果）。 */
const answerStatusText = computed(() => {
  const s = answerStatus.value
  if (!s) return '尚未提交回答'
  if (s.status === ANSWER_SUBMITTED) {
    return s.source === 'asr' ? '已收到语音作答并完成评分' : '已提交文字作答并完成评分'
  }
  return s.error || '作答未被接受'
})

/**
 * ★ 视频模式：**编排状态是否允许作答**。
 *
 * 编排层是严格顺序的：只有 `waiting_answer` 能提交作答。答完一题后状态会回到
 * `avatar_speaking`（数字人正在播报**下一题**）——此时提交只会拿到 `state_error`。
 * 这条闸门是**真机端到端跑出来的**：假 WebSocket 的单元自检里没有编排状态，
 * 界面照常「看起来能用」，一连真服务端就露馅。
 */
const canAnswerNow = computed(() => orchState.value === 'waiting_answer')

/** 视频模式：数字人正在播报（此时该点「数字人播报完毕」）。 */
const isAvatarSpeaking = computed(() => orchState.value === 'avatar_speaking')

/** 视频模式：当前该做什么（**把编排状态翻译成一句人话**，不让用户猜按钮为什么是灰的）。 */
const answerGateHint = computed(() => {
  if (!isAvatarMode.value) return ''
  if (!socketConnected.value) return '实时通道未连接，请先「重新连接」。'
  switch (orchState.value) {
    case 'avatar_speaking':
      return '数字人正在播报，播报结束后点「数字人播报完毕」即可作答。'
    case 'processing_answer':
      return '正在评分，请稍候…'
    case 'waiting_question':
      return '正在准备题目…'
    case 'finished':
      return '本场面试已结束。'
    case 'waiting_answer':
      return '轮到你了：可用「语音作答（ASR）」，也可在下方打字作答。'
    default:
      return '正在等待服务端状态…'
  }
})

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
 * 知识检索状态 —— **如实**表述（四态）。
 *
 * 两个信号配合才能说清楚：
 * - `warnings` 含 `knowledge_retrieval_failed` ⇒ 检索链路报错，降级为无知识；
 * - `knowledge_chunks`（**正向信号**）⇒ 本次真正注入 Prompt 的切片条数。
 *
 * 「命中 0 条」与「已注入 N 条」在只失败信号的时代**无法区分**，现在可以了。
 * RAG 已由后端契约层默认开启，本页**不再有「未启用」这一态**：
 * 走到「命中 0 条」分支时，0 一定是「检索了但真的没命中」。
 */
const knowledgeStatus = computed(() => {
  if (!agentResult.value) return null
  if (warnings.value.includes('knowledge_retrieval_failed')) {
    return { level: 'failed', text: '知识检索失败 → 本次出题**未使用知识库**（已降级为无知识）' }
  }
  const injected = Number(agentResult.value.knowledge_chunks) || 0
  if (injected > 0) {
    return {
      level: 'injected',
      text: `已注入 ${injected} 条知识切片到出题 Prompt（后端 knowledge_chunks 计数）`,
    }
  }
  return {
    level: 'requested',
    text: '已请求知识检索，但**命中 0 条** —— 本次出题未拿到任何知识（语料可能未覆盖该 topic）。',
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

// ============================================================
// 视频面试 · 实时通道（仅 avatar 模式使用）
// ============================================================
/** 清空通道派生状态（**不动** `session` / `answers` / `report` 这些业务数据）。 */
function resetSocketState() {
  socketConnected.value = false
  socketNotice.value = ''
  helloInfo.value = null
  orchState.value = null
  avatarState.value = null
  answerStatus.value = null
  lastScore.value = null
  wsError.value = null
  answerPending.value = false
}

/** 主动断开（组件卸载 / 重新开始 / 换会话时必须调，否则会触发自动重连）。 */
function disconnectSocket() {
  if (activeSocket) {
    activeSocket.close()
    activeSocket = null
  }
  socketConnected.value = false
}

/** 把关闭码翻译成人话（**不跳转、不清 token**，与全项目登录态约定一致）。 */
function describeClose(code, reason) {
  if (code === WS_CLOSE_UNAUTHORIZED) return '登录状态已失效，请重新登录后再试（实时通道已关闭）'
  if (code === WS_CLOSE_FORBIDDEN) return '没有权限访问该会话（实时通道已关闭）'
  if (code === WS_CLOSE_NOT_FOUND) return '会话不存在或不属于当前用户（实时通道已关闭）'
  if (code === WS_CLOSE_IDLE) return '长时间无操作，实时通道已自动关闭（重新进入面试间即可恢复进度）'
  if (code === WS_CLOSE_BUSY) return '同时打开的面试窗口过多，请关闭其他窗口后重试（实时通道已关闭）'
  if (code === 1000) return '实时通道已关闭'
  return `实时通道已断开（code=${code}）${reason ? `：${reason}` : ''}`
}

/** 把通道推来的题目累积进 `questions`，好让 `questionNoOf` 能映射题号。 */
function rememberQuestion(question) {
  if (!question || question.id === undefined || question.id === null) return
  if (questions.value.some((q) => q.id === question.id)) return
  questions.value = [...questions.value, question]
}

/** `answer` 事件 → 回答状态 + 作答记录（`answer` 与 HTTP 契约**同一形状**）。 */
function handleAnswerEvent(evt) {
  answerPending.value = false
  if (evt.status === ANSWER_SUBMITTED) {
    answerStatus.value = { status: ANSWER_SUBMITTED, text: evt.text || '', source: evt.source || '' }
    if (evt.answer) answers.value = [...answers.value, evt.answer]
    answerDraft.value = ''
  } else {
    answerStatus.value = {
      status: ANSWER_REJECTED,
      text: '',
      source: evt.source || '',
      error: evt.error || '作答未被接受',
      asr: evt.asr || null,
    }
  }
}

/** 服务端事件分发（9 种，`SERVER_EVENTS`）。未知类型**不静默丢弃**，挂到提示上。 */
function handleSocketEvent(evt) {
  if (!evt || typeof evt !== 'object') return
  switch (evt.type) {
    case 'hello':
      helloInfo.value = evt
      break
    case 'state':
      orchState.value = evt.state || null
      break
    case 'avatar':
      avatarState.value = evt
      break
    case 'question':
      if (evt.session) session.value = evt.session
      currentQuestion.value = evt.question || null
      allAnswered.value = !!evt.all_answered
      rememberQuestion(evt.question)
      if (!evt.question && evt.all_answered) {
        statusMessage.value = '全部题目已作答，请结束面试并生成报告'
      } else if (evt.question) {
        statusMessage.value = ''
      }
      break
    case 'answer':
      handleAnswerEvent(evt)
      break
    case 'score':
      lastScore.value = evt
      break
    case 'report':
      if (evt.session) session.value = evt.session
      report.value = evt.report || null
      currentQuestion.value = null
      allAnswered.value = true
      statusMessage.value = '面试已结束'
      break
    case 'error':
      wsError.value = evt
      answerPending.value = false
      break
    case 'pong':
      break
    default:
      socketNotice.value = `收到未收录的服务端事件：${evt.type}`
  }
}

/**
 * 建立实时通道。
 *
 * 服务端在**每次**连接建立后都会推一次快照（`hello` → `state` → `question` → `avatar`），
 * 因此重连本身即完成界面恢复，不需要额外再要一次 `snapshot`。
 *
 * @param {number} sessionId
 * @param {{autoStart?: boolean}} [options] `autoStart`：连上后立刻发 `start`
 *   （仅用于「会话还是 created，题目还没生成」这一种情形）。
 */
function connectSocket(sessionId, options = {}) {
  const autoStart = options.autoStart === true
  disconnectSocket()
  resetSocketState()

  let instance = null
  try {
    instance = new InterviewSocket(sessionId, {
      onEvent: handleSocketEvent,
      onOpen: () => {
        socketConnected.value = true
        socketNotice.value = '实时通道已连接'
        if (autoStart) instance.start()
      },
      onClose: (code, reason) => {
        socketConnected.value = false
        socketNotice.value = describeClose(code, reason)
      },
      onRetry: (attempt, delayMs) => {
        // 退避重试期间给出**可预期**的提示：让用户知道在自动重试、还要等多久，
        // 而不是盯着一个「未连接」猜是不是卡死了。
        socketNotice.value = `实时通道连接失败，正在自动重试（第 ${attempt} 次，约 ${Math.round(
          delayMs / 1000
        )} 秒后）`
      },
      onError: (err) => {
        socketNotice.value = (err && err.message) || '实时通道异常'
      },
    })
  } catch (e) {
    socketNotice.value = (e && e.message) || '实时通道初始化失败'
    return
  }

  activeSocket = instance
  // connectWithRetry() 内部已做「有上限的指数退避 + 抖动」，这里只需接住**最终**失败。
  // 拿不到一次性 ticket 时**不会**退回长期 token 建连（见 interviewSocket 的 fail closed），
  // 因此这里给出的是「连不上 + 可重试」，而不是偷偷降级后的「连上了」。
  instance.connectWithRetry().catch((e) => {
    activeSocket = null
    socketNotice.value = describeConnectError(e)
  })
}

/** 把「连不上」翻译成人话（区分登录问题 / 凭证服务问题 / 其它）。 */
function describeConnectError(e) {
  if (e instanceof SocketAuthError) return e.message
  if (e instanceof SocketTicketError) {
    return `${e.message}（实时通道未建立，可点击「重新连接」重试）`
  }
  return (e && e.message) || '实时通道连接失败'
}

/** 手动重连（通道断开后由用户触发；不自动无限重试，避免刷屏）。 */
function reconnectSocket() {
  if (!session.value) return
  connectSocket(session.value.id, { autoStart: session.value.status === 'created' })
}

/** `speech_done`：数字人播报完毕（真实接入时由音视频 `ended` 事件触发）。 */
function socketSpeechDone() {
  if (!activeSocket) {
    ElMessage.warning('实时通道未连接')
    return
  }
  activeSocket.speechDone()
}

/** `answer` + `mode=asr`：语音作答（服务端取讯飞虚拟人内置 ASR 的文本再评分）。 */
function socketAnswerByVoice() {
  if (!activeSocket) {
    ElMessage.warning('实时通道未连接')
    return
  }
  answerPending.value = true
  if (!activeSocket.answerByVoice()) {
    answerPending.value = false
    ElMessage.warning('实时通道未连接，语音作答未发出')
  }
}

/** `answer` + `mode=text`：打字作答（ASR 不可用时的兜底）。 */
function socketAnswerByText() {
  const text = answerDraft.value.trim()
  if (!text) {
    ElMessage.warning('请先输入回答内容')
    return
  }
  if (!activeSocket) {
    ElMessage.warning('实时通道未连接')
    return
  }
  answerPending.value = true
  if (!activeSocket.answerByText(text)) {
    answerPending.value = false
    ElMessage.warning('实时通道未连接，回答未发出')
  }
}

/** `finish`：结束面试并生成报告（与 HTTP 版同款二次确认）。 */
async function socketFinish() {
  if (!activeSocket) {
    ElMessage.warning('实时通道未连接')
    return
  }
  if (answers.value.length === 0) {
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
    if (!activeSocket.finish()) ElMessage.warning('实时通道未连接，结束请求未发出')
  } finally {
    ending.value = false
  }
}

/** 视频模式建会话后的落地：先把会话挂上（UI 切到视频面板），再连通道。 */
function beginAvatarSession(sessionDict, jobNameText) {
  session.value = sessionDict
  jobName.value = jobNameText || ''
  questions.value = []
  answers.value = []
  currentQuestion.value = null
  allAnswered.value = false
  report.value = null
  answerDraft.value = ''
  statusMessage.value = '正在建立实时通道…'
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
        // ★ 交互模式随建会话一起落库（后端字段 interview_mode，键名 mode 同样接受）。
        mode: form.mode,
      },
    })
    const sessionId = created && created.session && created.session.id
    if (!sessionId) {
      ElMessage.error('创建会话未返回 session id，无法继续')
      return
    }

    if (form.mode === MODE_AVATAR) {
      // 视频模式：**不调** HTTP /start —— 题目由实时通道的 start 消息触发
      // （服务端会先做数字人预检，再生成整场题目并播报第一题）。
      beginAvatarSession(created.session, created.job_name)
      connectSocket(sessionId, { autoStart: true })
      ElMessage.success('视频面试已创建，正在建立实时通道…')
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
    if (isAvatarMode.value) {
      // 视频会话：通道一连上服务端就会推快照（状态 / 当前题 / 数字人），界面即恢复；
      // 仅当会话还是 `created`（题目尚未生成）时才补发一次 start。
      connectSocket(id, { autoStart: session.value.status === 'created' })
    }
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
    // 不带 `use_rag`：由后端契约层默认值决定（默认开启知识检索）
    const data = await apiClient.post(`${API}/${session.value.id}/next-question`, {
      body: {},
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
  disconnectSocket()
  resetSocketState()
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
// ★ 组件卸载必须断开实时通道：否则客户端会自动重连，用户已经离开页面了还在打后端。
onUnmounted(disconnectSocket)
</script>

<template>
  <div class="ir-page">
    <div class="ir-container glass-card">
      <!-- 头部 -->
      <div class="ir-header">
        <div class="ir-title">
          <h2>AI 面试间</h2>
          <p>
            作答 → 结束 → 八维报告（完整闭环）· 另含大模型出题试跑通道（可挂载 RAG 知识库）·
            支持文字面试与数字人视频面试
          </p>
        </div>
        <div class="ir-header-actions">
          <button v-if="session" class="ghost-btn" @click="reset">重新开始</button>
        </div>
      </div>

      <!-- ============ 一、配置区（未开始） ============ -->
      <section v-if="!session" class="ir-section">
        <div class="ir-grid">
          <label class="ir-field">
            <span>面试模式</span>
            <select v-model="form.mode">
              <option v-for="opt in MODE_OPTIONS" :key="opt.value" :value="opt.value">
                {{ opt.label }}
              </option>
            </select>
            <small>
              文字面试：现有流程，打字作答。<br />
              视频面试：数字人播报 + 实时通道，支持语音（ASR）作答，也可打字兜底。
            </small>
          </label>

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

        <div class="ir-actions">
          <button class="primary-btn" :disabled="starting" @click="startInterview">
            {{
              starting
                ? '正在创建并开始…'
                : form.mode === MODE_AVATAR
                  ? '开始视频面试'
                  : '开始面试'
            }}
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

        <!-- created：已恢复但尚未开始（视频模式的「未开始」由实时通道的 start 触发，不走这里） -->
        <section v-if="isCreated && !isAvatarMode" class="ir-section">
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

        <!-- ============ 二·一、作答闭环（文字面试：与改造前逐字节一致） ============ -->
        <section v-if="!isCreated && !isAvatarMode" class="ir-section">
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

        <!-- ============ 二·一'、视频面试面板（仅 avatar 模式） ============ -->
        <section v-if="isAvatarMode" class="ir-section ir-video">
          <h3 class="ir-section-title">
            视频面试
            <span class="tag">数字人 + 实时通道（WebSocket）</span>
            <span class="tag">会话 #{{ session.id }}</span>
          </h3>

          <div class="ir-video-grid">
            <!-- ---- 数字人展示区域 + 视频流播放器 ---- -->
            <div class="ir-avatar-stage">
              <video
                v-if="avatarStreamPlayable"
                class="ir-video-stream"
                :src="avatarStreamUrl"
                autoplay
                playsinline
                controls
              ></video>
              <div v-else class="ir-avatar-placeholder">
                <span class="ir-avatar-face">数字人</span>
                <p class="ir-avatar-hint">{{ avatarStatusText }}</p>
                <p v-if="avatarStreamUrl" class="ir-avatar-hint small">
                  已下发流地址 <code>{{ avatarStreamUrl }}</code>，但它不是浏览器可直接播放的
                  协议（Mock provider 的占位地址），因此这里只显示占位区。
                </p>
              </div>

              <div class="ir-avatar-meta">
                <span class="chip">{{ avatarStatusText }}</span>
                <span v-if="avatarState" class="chip">
                  provider：{{ avatarState.provider || '（未配置）' }}
                </span>
                <span v-if="avatarState" class="chip">
                  configured：{{ avatarState.configured ? '是' : '否' }}
                </span>
                <span v-if="avatarState && avatarState.avatar_session_id" class="chip">
                  数字人会话：{{ avatarState.avatar_session_id }}
                </span>
              </div>
              <p v-if="avatarState && avatarState.error" class="ir-avatar-error">
                数字人明细：{{ avatarState.error }}
              </p>
            </div>

            <!-- ---- 面试状态 / 回答状态 / 通道状态 ---- -->
            <div class="ir-video-side">
              <div class="ir-state-box">
                <span class="ir-state-label">面试状态</span>
                <strong class="ir-state-value">{{ orchStateLabel }}</strong>
                <small v-if="orchState" class="ir-state-code">state = {{ orchState }}</small>
                <small v-else class="ir-state-code">尚未收到 state 事件</small>
              </div>

              <div class="ir-state-box" :class="answerStatus ? 'st-' + answerStatus.status : ''">
                <span class="ir-state-label">回答状态</span>
                <strong class="ir-state-value">{{ answerStatusText }}</strong>
                <small v-if="answerStatus && answerStatus.source" class="ir-state-code">
                  来源：{{ answerStatus.source === 'asr' ? '语音（ASR）' : '打字' }}
                </small>
                <small v-if="answerStatus && answerStatus.text" class="ir-state-code">
                  文本：{{ answerStatus.text }}
                </small>
              </div>

              <div class="ir-state-box">
                <span class="ir-state-label">通道状态</span>
                <strong class="ir-state-value">{{ socketConnected ? '已连接' : '未连接' }}</strong>
                <small v-if="socketNotice" class="ir-state-code">{{ socketNotice }}</small>
                <small v-if="helloInfo" class="ir-state-code">
                  服务端协议：{{ (helloInfo.server_events || []).length }} 类事件 ·
                  {{ (helloInfo.client_messages || []).length }} 类消息
                </small>
                <button
                  v-if="!socketConnected"
                  class="ghost-btn ir-mini-btn"
                  @click="reconnectSocket"
                >
                  重新连接
                </button>
              </div>
            </div>
          </div>

          <!-- ---- score 结果（最近一题） ---- -->
          <div v-if="lastScore" class="ir-score-strip">
            <span class="chip score">
              第 {{ questionNoOf(lastScore.question_id) ?? '?' }} 题得分
              {{ lastScore.score ?? '—' }}
            </span>
            <span class="chip">技术 {{ lastScore.technical_score ?? '—' }}</span>
            <span class="chip">逻辑 {{ lastScore.logic_score ?? '—' }}</span>
            <span class="chip">表达 {{ lastScore.expression_score ?? '—' }}</span>
            <span class="chip">应变 {{ lastScore.adaptability_score ?? '—' }}</span>
          </div>
          <p v-if="lastScore && lastScore.feedback" class="ir-score-feedback">
            <strong>点评：</strong>{{ lastScore.feedback }}
          </p>

          <!-- ---- 服务端 error 事件（稳定错误码） ---- -->
          <div v-if="wsError" class="ir-errors">
            <h4>实时通道报错（稳定错误码）</h4>
            <ul>
              <li><code>{{ wsError.code }}</code> —— {{ wsError.message }}</li>
            </ul>
          </div>

          <!-- ---- 当前问题显示 ---- -->
          <div v-if="currentQuestion" class="q-card plain">
            <div class="q-head">
              <span class="q-no">
                第 {{ currentQuestion.question_no }} / {{ session.total_questions }} 题
              </span>
              <span class="chip">{{ currentQuestion.question_type }}</span>
              <span v-if="currentQuestion.topic" class="chip">
                主题：{{ currentQuestion.topic }}
              </span>
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
          <p v-else class="ir-empty">
            {{ socketConnected ? '正在等待题目…' : '实时通道未连接，请先重新连接。' }}
          </p>

          <!-- ---- 作答操作 ---- -->
          <p v-if="currentQuestion || isFinished" class="ir-gate-hint">{{ answerGateHint }}</p>
          <div v-if="currentQuestion" class="ir-actions left">
            <button
              class="primary-btn"
              :disabled="answerPending || !canAnswerNow"
              @click="socketAnswerByVoice"
            >
              {{ answerPending ? '等待评分…' : '语音作答（ASR）' }}
            </button>
            <button
              class="ghost-btn"
              :disabled="!socketConnected || !isAvatarSpeaking"
              @click="socketSpeechDone"
            >
              数字人播报完毕
            </button>
            <small>
              语音作答走服务端讯飞虚拟人内置 ASR；取不到文本会被**拒收**且不落库，可用下方打字兜底。
            </small>
          </div>

          <!-- ---- 打字兜底作答 ---- -->
          <div v-if="currentQuestion" class="ir-answer-box">
            <textarea
              v-model="answerDraft"
              rows="5"
              maxlength="10000"
              :disabled="!canAnswerNow"
              placeholder="也可在此打字作答（ASR 不可用时的兜底）。提交后由后端规则评分并落库，一题只能作答一次。"
            ></textarea>
            <div class="ir-answer-foot">
              <span class="ir-counter">{{ answerDraft.length }} / 10000</span>
              <button
                class="primary-btn"
                :disabled="!canSubmitAnswer || answerPending || !socketConnected || !canAnswerNow"
                @click="socketAnswerByText"
              >
                {{ answerPending ? '提交并评分中…' : '提交文字回答' }}
              </button>
            </div>
          </div>

          <!-- ---- 全部作答完毕 → 结束 ---- -->
          <div v-if="!currentQuestion && allAnswered && !isFinished" class="ir-done-box">
            <p class="ir-done-text">
              全部 {{ session.total_questions }} 题已作答完毕。点下方按钮通过实时通道发
              <code>finish</code> 消息生成八维报告。
            </p>
            <div class="ir-actions left">
              <button
                class="primary-btn"
                :disabled="ending || !socketConnected"
                @click="socketFinish"
              >
                {{ ending ? '正在生成报告…' : '结束面试并生成报告' }}
              </button>
            </div>
          </div>

          <!-- ---- 进行中：提前结束 ---- -->
          <div v-if="!isFinished && currentQuestion" class="ir-actions left ir-end-early">
            <button
              class="ghost-btn"
              :disabled="ending || answeredCount === 0 || !socketConnected"
              @click="socketFinish"
            >
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
            <strong>它不落库、不推进题号</strong>，产出不会出现在上方的本场作答与报告里。<br />
            知识库检索（RAG）<strong>默认开启</strong>，检索参数由后端 <code>.env</code> 的
            <code>RAG_*</code> 决定；检索失败不会让出题失败——会静默降级为「无知识」。
          </p>

          <div class="ir-actions left">
            <button class="primary-btn" :disabled="asking" @click="askNextQuestion">
              {{ asking ? '生成中…（真实模型约 10~30 秒）' : '生成一道候选题' }}
            </button>
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

/* 正向信号：本次确实把 N 条知识注入了 Prompt */
.ir-knowledge.injected {
  color: var(--accent-gold);
  background: rgba(201, 162, 39, 0.10);
  border: 1px solid rgba(201, 162, 39, 0.30);
}

/* 已检索但命中 0 条 —— 不是失败，但也没拿到知识 */
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

/* ---------------- 视频面试面板（仅 avatar 模式） ---------------- */
.ir-video {
  padding-top: 22px;
  border-top: 1px dashed var(--border-glass);
}

.ir-video-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.6fr) minmax(0, 1fr);
  gap: 16px;
}

@media (max-width: 720px) {
  .ir-video-grid {
    grid-template-columns: 1fr;
  }
}

/* ---- 数字人展示区域 + 视频流播放器 ---- */
.ir-actions.left small {
  font-size: 12px;
  line-height: 1.7;
  color: var(--text-muted);
}

.ir-avatar-stage {
  padding: 14px;
  background: rgba(255, 255, 255, 0.75);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-md);
}

.ir-video-stream {
  display: block;
  width: 100%;
  aspect-ratio: 16 / 9;
  background: rgba(45, 42, 38, 0.06);
  border-radius: var(--radius-sm);
}

.ir-avatar-placeholder {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 8px;
  aspect-ratio: 16 / 9;
  padding: 16px;
  text-align: center;
  background: linear-gradient(135deg, rgba(201, 162, 39, 0.07), rgba(0, 168, 150, 0.07));
  border: 1px dashed var(--border-glass-strong);
  border-radius: var(--radius-sm);
}

.ir-avatar-face {
  font-size: 15px;
  font-weight: 600;
  color: var(--accent-gold);
  letter-spacing: 0.08em;
}

.ir-avatar-hint {
  margin: 0;
  font-size: 12.5px;
  line-height: 1.7;
  color: var(--text-secondary);
}

.ir-avatar-hint.small {
  font-size: 12px;
  color: var(--text-muted);
}

.ir-avatar-hint code,
.ir-avatar-error code {
  padding: 1px 5px;
  font-size: 12px;
  background: rgba(45, 42, 38, 0.06);
  border-radius: 4px;
}

.ir-avatar-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 12px;
}

.ir-avatar-error {
  margin: 10px 0 0;
  font-size: 12px;
  line-height: 1.7;
  color: var(--warning);
  word-break: break-word;
}

/* ---- 面试状态 / 回答状态 / 通道状态 ---- */
.ir-video-side {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.ir-state-box {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 13px 15px;
  background: rgba(255, 255, 255, 0.75);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-md);
}

.ir-state-label {
  font-size: 12px;
  color: var(--text-secondary);
}

.ir-state-value {
  font-size: 15px;
  font-weight: 600;
  color: var(--text-primary);
}

.ir-state-code {
  font-size: 11.5px;
  line-height: 1.6;
  color: var(--text-muted);
  word-break: break-word;
}

/* 回答状态：提交成功 / 被拒收（拒收不是错误，是「这一条没被接受」） */
.ir-state-box.st-submitted {
  background: rgba(0, 168, 150, 0.06);
  border-color: rgba(0, 168, 150, 0.28);
}

.ir-state-box.st-rejected {
  background: rgba(201, 162, 39, 0.08);
  border-color: rgba(201, 162, 39, 0.3);
}

.ir-mini-btn {
  align-self: flex-start;
  margin-top: 6px;
  padding: 5px 12px;
  font-size: 12.5px;
}

/* ---- score 结果 ---- */
.ir-score-strip {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 16px;
}

.ir-score-feedback {
  margin: 10px 0 0;
  font-size: 13px;
  line-height: 1.8;
  color: var(--text-secondary);
}

/* 当前该做什么（把编排状态翻译成人话） */
.ir-gate-hint {
  margin: 16px 0 0;
  padding: 10px 14px;
  font-size: 13px;
  line-height: 1.7;
  color: var(--accent-gold);
  background: rgba(201, 162, 39, 0.08);
  border: 1px solid rgba(201, 162, 39, 0.28);
  border-radius: var(--radius-sm);
}
</style>
