/**
 * CareerAI 前端 · AI 面试间「面试模式 + 实时通道」功能自检
 * ============================================================
 * 运行：node frontend/tests/test_interview_mode.js   （或 npm test）
 *
 * 为什么自建脚本而不是用测试框架
 * ------------------------------
 * `package.json` 里**没有任何测试框架**（无 vitest / @vue/test-utils / jsdom）。
 * 为了不为了「跑一个自检」而引入一整套依赖，这里用**零依赖**的 Node 脚本：
 *
 * - **运行期**：真的把 `src/api/interviewSocket.js` 加载起来，用假的 `WebSocket` /
 *   `localStorage` 驱动它，断言 URL 拼装、消息构造、事件派发、重连策略；
 * - **源码级**：`InterviewRoom.vue` 是 SFC，Node 无法直接渲染（没有 Vue 编译器），
 *   因此对它做**结构化字符串断言**——检查「模式选择 / mode 透传 / 视频面板五项 /
 *   事件分发 / 卸载清理 / 文字路径未被破坏」这些**可被字符串客观判定**的事实。
 *   这不是渲染测试，但对「接线是否真的接上了」是有效取证。
 *
 * ★ 关于 `import { ... } from './apiClient'`（无扩展名）
 * ------------------------------------------------------
 * Vite 能解析无扩展名相对导入，**Node ESM 不能**（ERR_MODULE_NOT_FOUND）。
 * 为了不为了测试而改动生产源码，这里把两个模块**复制到临时目录**，
 * 只把 `'./apiClient'` 补成 `'./apiClient.js'`，并补一个 `{"type":"module"}` 的
 * `package.json`，再从临时目录动态导入。生产文件一个字节都没动。
 */

import fs from 'node:fs'
import path from 'node:path'
import { COMPONENTS, FRONTEND, installLocalStorage, loadSocketModule } from './socket_loader.js'

const ROOM = path.join(COMPONENTS, 'InterviewRoom.vue')
const VITE_CONFIG = path.join(FRONTEND, 'vite.config.js')
const APP_VUE = path.join(FRONTEND, 'src', 'App.vue')

let _PASSED = 0
let _FAILED = 0

function _check(name, cond, detail = '') {
  if (cond) _PASSED += 1
  else _FAILED += 1
  console.log(
    (cond ? '  [PASS] ' : '  [FAIL] ') + name + (detail && !cond ? `  -> ${detail}` : '')
  )
  return cond
}

// ============================================================
// 二、运行期替身：localStorage / WebSocket
// ============================================================
/** 极简 WebSocket 替身：只实现被测代码真正用到的面。 */
class FakeWebSocket {
  static instances = []
  static reset() {
    FakeWebSocket.instances = []
  }

  constructor(url) {
    this.url = url
    this.readyState = 0 // CONNECTING
    this.sent = []
    this.onopen = null
    this.onmessage = null
    this.onerror = null
    this.onclose = null
    FakeWebSocket.instances.push(this)
  }

  send(data) {
    if (this.readyState !== 1) throw new Error('尚未连接')
    this.sent.push(data)
  }

  close(code = 1000, reason = '') {
    if (this.readyState === 3) return
    this.readyState = 3
    if (this.onclose) this.onclose({ code, reason })
  }

  // ---- 测试助手（模拟服务端 / 浏览器行为） ----
  _open() {
    this.readyState = 1
    if (this.onopen) this.onopen()
  }

  _emit(payload) {
    if (this.onmessage) {
      this.onmessage({ data: typeof payload === 'string' ? payload : JSON.stringify(payload) })
    }
  }

  _serverClose(code, reason = '') {
    this.readyState = 3
    if (this.onclose) this.onclose({ code, reason })
  }

  get lastSent() {
    return this.sent.length ? JSON.parse(this.sent[this.sent.length - 1]) : null
  }
}

// ============================================================
// 三、主流程
// ============================================================
async function run() {
  console.log('='.repeat(68))
  console.log('前端 · AI 面试间「面试模式 + 实时通道」自检')
  console.log('='.repeat(68))

  installLocalStorage('fe-test-token')
  globalThis.WebSocket = FakeWebSocket

  const { mod, tmp } = await loadSocketModule()
  const {
    SERVER_EVENTS,
    CLIENT_MESSAGES,
    ANSWER_MODES,
    ANSWER_MODE_ASR,
    ANSWER_MODE_TEXT,
    ANSWER_SUBMITTED,
    ANSWER_REJECTED,
    WS_CLOSE_UNAUTHORIZED,
    WS_CLOSE_FORBIDDEN,
    WS_CLOSE_NOT_FOUND,
    ORCH_STATES,
    ORCH_STATE_LABELS,
    AVATAR_STATUS_LABELS,
    MAX_RECONNECT_ATTEMPTS,
    buildSocketPath,
    buildSocketUrl,
    SocketAuthError,
    InterviewSocket,
  } = mod

  // ------------------------------------------------------------
  // [1] 协议常量（必须与后端 backend/api/interview_ws.py 逐字一致）
  // ------------------------------------------------------------
  console.log('\n[1] 协议常量')
  _check(
    '服务端事件集恒定（9 个，与后端同序）',
    Array.isArray(SERVER_EVENTS) &&
      SERVER_EVENTS.length === 9 &&
      SERVER_EVENTS.join(',') ===
        'hello,state,avatar,question,answer,score,report,error,pong',
    JSON.stringify(SERVER_EVENTS)
  )
  _check(
    '客户端消息集恒定（6 个）',
    CLIENT_MESSAGES.join(',') === 'start,speech_done,answer,finish,snapshot,ping',
    JSON.stringify(CLIENT_MESSAGES)
  )
  _check(
    '作答方式 = {asr, text}',
    ANSWER_MODES.length === 2 && ANSWER_MODE_ASR === 'asr' && ANSWER_MODE_TEXT === 'text',
    JSON.stringify(ANSWER_MODES)
  )
  _check(
    'answer 事件结果 = {submitted, rejected}',
    ANSWER_SUBMITTED === 'submitted' && ANSWER_REJECTED === 'rejected'
  )
  _check(
    '关闭码用 4xxx',
    WS_CLOSE_UNAUTHORIZED === 4401 && WS_CLOSE_FORBIDDEN === 4403 && WS_CLOSE_NOT_FOUND === 4404
  )
  _check('编排状态 5 态', ORCH_STATES.length === 5 && ORCH_STATES[0] === 'waiting_question')
  _check(
    '★ 5 个编排状态**每个**都有中文文案（漏一个界面就会出现原始英文码）',
    ORCH_STATES.every((s) => typeof ORCH_STATE_LABELS[s] === 'string' && ORCH_STATE_LABELS[s]),
    JSON.stringify(Object.keys(ORCH_STATE_LABELS))
  )
  _check(
    '★ 通道层 7 档 status **每个**都有中文文案',
    ['ok', 'empty', 'unavailable', 'expired', 'unsupported', 'disabled', 'failed'].every(
      (s) => typeof AVATAR_STATUS_LABELS[s] === 'string' && AVATAR_STATUS_LABELS[s]
    ),
    JSON.stringify(Object.keys(AVATAR_STATUS_LABELS))
  )
  _check('重连次数上限为正整数', Number.isInteger(MAX_RECONNECT_ATTEMPTS) && MAX_RECONNECT_ATTEMPTS > 0)

  // ------------------------------------------------------------
  // [2] URL 拼装
  // ------------------------------------------------------------
  console.log('\n[2] WebSocket URL 拼装')
  _check(
    '路径模板与后端 WS_PATH 一致',
    buildSocketPath(12) === '/api/interview/12/ws',
    buildSocketPath(12)
  )
  const url = buildSocketUrl(12, 'tok-abc')
  _check(
    'URL = ws(s)://host/api/interview/{id}/ws?token=…',
    url === 'ws://localhost/api/interview/12/ws?token=tok-abc',
    url
  )
  _check(
    'token 做了 URL 编码（否则含 + / = 的 token 会被截断）',
    buildSocketUrl(7, 'a+b/c=d e').endsWith('?token=' + encodeURIComponent('a+b/c=d e')),
    buildSocketUrl(7, 'a+b/c=d e')
  )
  _check(
    'token 缺省从 apiClient.getToken() 取（不硬编码）',
    buildSocketUrl(3).endsWith('token=fe-test-token'),
    buildSocketUrl(3)
  )
  _check('显式传空 token 时不抛异常（由后端给 4401）', buildSocketUrl(3, '').endsWith('token='))

  // ------------------------------------------------------------
  // [3] InterviewSocket 行为
  // ------------------------------------------------------------
  console.log('\n[3] InterviewSocket：连接 / 发消息 / 收事件')
  FakeWebSocket.reset()

  // 未登录：不发请求，直接抛 SocketAuthError
  installLocalStorage(null)
  let authErr = null
  const noToken = new InterviewSocket(1, {})
  try {
    noToken.connect()
  } catch (e) {
    authErr = e
  }
  _check(
    '★ 无 token 时**不发请求**，抛 SocketAuthError（区别于「连接失败」）',
    authErr instanceof SocketAuthError && FakeWebSocket.instances.length === 0,
    authErr ? authErr.name : 'no-throw'
  )

  // 正常连接
  installLocalStorage('fe-test-token')
  FakeWebSocket.reset()
  const events = []
  const closes = []
  const errors = []
  let opened = 0
  const sock = new InterviewSocket(42, {
    onEvent: (e) => events.push(e),
    onOpen: () => {
      opened += 1
    },
    onClose: (code, reason) => closes.push([code, reason]),
    onError: (e) => errors.push(e),
    autoReconnect: false,
  })
  sock.connect()
  const ws = FakeWebSocket.instances[0]
  _check('connect() 创建了一个 WebSocket', FakeWebSocket.instances.length === 1 && !!ws)
  _check('URL 带上了 session id 与 token', /\/api\/interview\/42\/ws\?token=fe-test-token$/.test(ws.url), ws.url)
  _check('未 open 前 connected=false', sock.connected === false)

  ws._open()
  _check('open 后 connected=true 且回调了 onOpen', sock.connected === true && opened === 1)

  // 发消息（逐条断言 JSON 形状 —— 这是与后端 normalize_client_message 的契约）
  sock.start()
  _check("start() 发 {'type':'start'}", JSON.stringify(ws.lastSent) === JSON.stringify({ type: 'start' }), ws.sent[ws.sent.length - 1])
  sock.speechDone()
  _check("speechDone() 发 {'type':'speech_done'}", ws.lastSent.type === 'speech_done')
  sock.answerByVoice()
  _check(
    "answerByVoice() 发 {'type':'answer','mode':'asr'}",
    ws.lastSent.type === 'answer' && ws.lastSent.mode === 'asr' && ws.lastSent.text === undefined,
    JSON.stringify(ws.lastSent)
  )
  sock.answerByText('我做过订单中台')
  _check(
    "answerByText(t) 发 {'type':'answer','mode':'text','text':t}",
    ws.lastSent.type === 'answer' && ws.lastSent.mode === 'text' && ws.lastSent.text === '我做过订单中台',
    JSON.stringify(ws.lastSent)
  )
  sock.answerByText(null)
  _check('answerByText(null) 归一成空串（不把 null 发出去）', ws.lastSent.text === '')
  sock.finish()
  _check("finish() 发 {'type':'finish'}", ws.lastSent.type === 'finish')
  sock.snapshot()
  _check("snapshot() 发 {'type':'snapshot'}", ws.lastSent.type === 'snapshot')
  sock.ping()
  _check("ping() 发 {'type':'ping'}", ws.lastSent.type === 'ping')

  // 收事件
  ws._emit({ type: 'hello', session_id: 42 })
  ws._emit({ type: 'state', state: 'avatar_speaking' })
  ws._emit({ type: 'avatar', ok: true, stream_url: 'mock://stream' })
  ws._emit({ type: 'question', question: { id: 1, question_no: 1 } })
  _check(
    'onEvent 逐条派发（4 条事件 → 4 次回调，顺序不变）',
    events.length === 4 && events.map((e) => e.type).join(',') === 'hello,state,avatar,question',
    JSON.stringify(events.map((e) => e.type))
  )
  _check('事件体原样透传（未被改写）', events[3].question.question_no === 1)

  // 非 JSON 消息 → onError（不静默丢弃）
  ws._emit('<html>502 Bad Gateway</html>')
  _check(
    '★ 非 JSON 消息 → onError（不静默丢弃，否则界面永远停在「等待」）',
    errors.length === 1 && /无法解析/.test(errors[0].message),
    errors.length ? errors[0].message : 'no-error'
  )
  _check('解析失败不污染事件流', events.length === 4)

  // 主动关闭
  sock.close()
  _check('close() 关闭底层连接并置 connected=false', sock.connected === false && ws.readyState === 3)
  _check('close() 被记为「用户主动关闭」（不再自动重连）', sock.closedByUser === true)
  _check('主动关闭后 close 回调仍被调用（含正常关闭码 1000）', closes.some((c) => c[0] === 1000))

  // 未连接时 send 不抛异常
  _check('未连接时 send() 返回 false 且**不抛异常**', sock.send({ type: 'ping' }) === false)

  // ------------------------------------------------------------
  // [4] 重连策略
  // ------------------------------------------------------------
  console.log('\n[4] 断线重连策略')
  const mkSock = () => {
    FakeWebSocket.reset()
    const s = new InterviewSocket(9, { onError: () => {}, onClose: () => {} })
    s.connect()
    return { s, w: FakeWebSocket.instances[0] }
  }

  const a = mkSock()
  a.w._serverClose(1006, 'abnormal')
  _check('★ 异常断开（1006）会安排重连', a.s.retryTimer !== null && a.s.attempts === 1)
  a.s.close()

  const b = mkSock()
  b.w._serverClose(WS_CLOSE_UNAUTHORIZED, '未登录')
  _check('★ 4401（未登录）**不重连** —— 重试多少次都不会成功', b.s.retryTimer === null)
  b.s.close()

  const c = mkSock()
  c.w._serverClose(WS_CLOSE_NOT_FOUND, '会话不存在')
  _check('★ 4404（会话不存在）**不重连**', c.s.retryTimer === null)
  c.s.close()

  const d = mkSock()
  d.w._serverClose(1006, 'x')
  d.s.close()
  _check('用户主动关闭会清掉待执行的重连定时器', d.s.retryTimer === null)

  const exhausted = []
  FakeWebSocket.reset()
  const e = new InterviewSocket(9, {
    onError: (err) => exhausted.push(err),
    autoReconnect: true,
    maxAttempts: 0,
  })
  e.connect()
  FakeWebSocket.instances[0]._serverClose(1006, 'x')
  _check(
    '重连次数耗尽 → 通过 onError 上报（不无限重试）',
    exhausted.length === 1 && /重连失败/.test(exhausted[0].message),
    exhausted.length ? exhausted[0].message : 'no-error'
  )
  e.close()

  // ------------------------------------------------------------
  // [5] InterviewRoom.vue：模式选择 + mode 透传
  // ------------------------------------------------------------
  console.log('\n[5] InterviewRoom.vue：面试模式与 mode 透传')
  const roomSrc = fs.readFileSync(ROOM, 'utf8')

  _check(
    '★ 未新建独立视频页面（用户明确要求复用 AI 面试间）',
    !fs.existsSync(path.join(COMPONENTS, 'VideoInterview.vue')),
    'components/VideoInterview.vue 竟然存在'
  )
  const appSrc = fs.readFileSync(APP_VUE, 'utf8')
  _check('App.vue 仍挂载 InterviewRoom（入口未变）', /<InterviewRoom\b/.test(appSrc))

  _check("模式常量 MODE_TEXT='text'", /const MODE_TEXT = 'text'/.test(roomSrc))
  _check("模式常量 MODE_AVATAR='avatar'", /const MODE_AVATAR = 'avatar'/.test(roomSrc))
  _check(
    '★ 配置区提供「文字面试 / 视频面试」两个选项',
    /label: '文字面试'/.test(roomSrc) && /label: '视频面试'/.test(roomSrc)
  )
  _check(
    '★ 模板里有模式下拉（v-model 绑到 form.mode）',
    /<select v-model="form\.mode">/.test(roomSrc) && /v-for="opt in MODE_OPTIONS"/.test(roomSrc)
  )
  _check(
    '★ 建会话时把模式传出去（body 里 mode: form.mode）',
    /mode: form\.mode,/.test(roomSrc)
  )
  _check(
    '默认模式是文字面试（不选即与改造前一致）',
    /mode: MODE_TEXT,/.test(roomSrc)
  )

  // ------------------------------------------------------------
  // [6] InterviewRoom.vue：实时通道接线
  // ------------------------------------------------------------
  console.log('\n[6] InterviewRoom.vue：实时通道接线')
  _check(
    '引入 InterviewSocket（唯一 WS 入口）',
    /from '\.\.\/api\/interviewSocket'/.test(roomSrc) && /\bInterviewSocket,/.test(roomSrc)
  )
  _check('模式判定以会话字段为准（不是 form.mode）', /session\.value\.interview_mode/.test(roomSrc))

  const caseNames = ['hello', 'state', 'avatar', 'question', 'answer', 'score', 'report', 'error', 'pong']
  const missing = caseNames.filter((n) => !new RegExp(`case '${n}':`).test(roomSrc))
  _check(
    '★ 9 类服务端事件**每一类**都有分支（含 pong）',
    missing.length === 0,
    `缺少：${missing.join(',')}`
  )
  _check(
    '★ 未知事件不静默丢弃（走 default 分支给出提示）',
    /default:\s*\n\s*socketNotice\.value = `收到未收录的服务端事件/.test(roomSrc)
  )

  _check(
    'avatar 状态：区分「未探测」与「探测失败」',
    /session_probed === false/.test(roomSrc) && /AVATAR_STATUS_LABELS\[st\.status\]/.test(roomSrc)
  )
  _check(
    '回答状态：submitted / rejected 都处理',
    /ANSWER_SUBMITTED/.test(roomSrc) && /ANSWER_REJECTED/.test(roomSrc)
  )
  _check('面试状态用编排层文案表', /ORCH_STATE_LABELS\[orchState\.value\]/.test(roomSrc))
  _check('score 结果被接住并展示', /lastScore\.value = evt/.test(roomSrc) && /ir-score-strip/.test(roomSrc))
  _check(
    '关闭码按语义分开提示（4401 / 4403 / 4404）',
    /WS_CLOSE_UNAUTHORIZED/.test(roomSrc) &&
      /WS_CLOSE_FORBIDDEN/.test(roomSrc) &&
      /WS_CLOSE_NOT_FOUND/.test(roomSrc)
  )
  _check(
    '★ 4401/4403/4404 不跳转、不清 token（只提示）',
    !/clearToken|location\.href|router\.push/.test(roomSrc)
  )

  // ------------------------------------------------------------
  // [7] InterviewRoom.vue：视频模式 UI 五项
  // ------------------------------------------------------------
  console.log('\n[7] InterviewRoom.vue：视频模式 UI（用户要求的五项）')
  _check('① 数字人展示区域', /class="ir-avatar-stage"/.test(roomSrc) && /ir-avatar-meta/.test(roomSrc))
  _check(
    '② 视频流播放器（<video> 绑 stream_url，不可播协议降级为占位区）',
    /class="ir-video-stream"/.test(roomSrc) &&
      /:src="avatarStreamUrl"/.test(roomSrc) &&
      /avatarStreamPlayable/.test(roomSrc)
  )
  _check(
    '③ 当前问题显示（复用同一 question 形状 + 考察点）',
    /currentQuestion\.question_no/.test(roomSrc) && /currentQuestion\.expected_points/.test(roomSrc)
  )
  _check('④ 回答状态区', /class="ir-state-box"/.test(roomSrc) && /answerStatusText/.test(roomSrc))
  _check('⑤ 面试状态区', /面试状态/.test(roomSrc) && /orchStateLabel/.test(roomSrc))

  _check(
    '作答操作齐全：语音(ASR) / 打字兜底 / 播报完毕 / 结束',
    /socketAnswerByVoice/.test(roomSrc) &&
      /socketAnswerByText/.test(roomSrc) &&
      /socketSpeechDone/.test(roomSrc) &&
      /socketFinish/.test(roomSrc)
  )
  _check(
    '★ 作答按钮受**编排状态闸门**约束（只有 waiting_answer 能提交）——' +
      '这是真机端到端跑出来的：答完一题状态回到 avatar_speaking，此时提交只会拿 state_error',
    /const canAnswerNow = computed\(\(\) => orchState\.value === 'waiting_answer'\)/.test(roomSrc) &&
      /:disabled="answerPending \|\| !canAnswerNow"/.test(roomSrc) &&
      /!canSubmitAnswer \|\| answerPending \|\| !socketConnected \|\| !canAnswerNow/.test(roomSrc),
    'canAnswerNow 未接线到作答按钮'
  )
  _check(
    '★「数字人播报完毕」只在 avatar_speaking 时可点（否则会推出非法顺序）',
    /:disabled="!socketConnected \|\| !isAvatarSpeaking"/.test(roomSrc)
  )
  _check(
    '界面把编排状态翻译成「当前该做什么」（不让用户猜按钮为什么是灰的）',
    /const answerGateHint = computed/.test(roomSrc) && /class="ir-gate-hint"/.test(roomSrc)
  )
  _check(
    '★ 视频模式**不调** HTTP /start（题目由通道的 start 触发）',
    /connectSocket\(sessionId, \{ autoStart: true \}\)/.test(roomSrc)
  )
  _check('通道断开时提供「重新连接」入口', /reconnectSocket/.test(roomSrc))

  // ------------------------------------------------------------
  // [8] 文字面试未被破坏
  // ------------------------------------------------------------
  console.log('\n[8] 文字面试路径未被破坏')
  _check(
    '★ 文字作答区仍以 `!isCreated` 为主条件（仅多一个 isAvatarMode 排除项）',
    /<section v-if="!isCreated && !isAvatarMode" class="ir-section">/.test(roomSrc)
  )
  _check(
    '文字模式的四个动作函数仍在（submitAnswer / endInterview / askNextQuestion / beginCreatedSession）',
    /async function submitAnswer\(\)/.test(roomSrc) &&
      /async function endInterview\(\)/.test(roomSrc) &&
      /async function askNextQuestion\(\)/.test(roomSrc) &&
      /async function beginCreatedSession\(\)/.test(roomSrc)
  )
  _check(
    '文字模式的 HTTP 调用未改（/answer 仍走 apiClient.post）',
    /apiClient\.post\(`\$\{API\}\/\$\{session\.value\.id\}\/answer`/.test(roomSrc)
  )
  _check(
    '文字模式仍传 interview_type / difficulty / total_questions',
    /interview_type: form\.interview_type/.test(roomSrc) &&
      /difficulty: form\.difficulty/.test(roomSrc) &&
      /total_questions: form\.total_questions/.test(roomSrc)
  )
  _check(
    '★ 组件卸载时断开通道（否则离开页面后客户端还在自动重连）',
    /onUnmounted\(disconnectSocket\)/.test(roomSrc)
  )
  _check(
    '「重新开始」也会断开通道',
    /function reset\(\) \{\s*\n\s*disconnectSocket\(\)/.test(roomSrc)
  )

  // ------------------------------------------------------------
  // [9] 开发环境代理必须转发 WebSocket 升级
  // ------------------------------------------------------------
  console.log('\n[9] 开发环境代理')
  const viteSrc = fs.readFileSync(VITE_CONFIG, 'utf8')
  _check(
    '★ vite 代理开了 ws: true（否则 /api 下的 WS 升级请求根本不会到后端）',
    /ws:\s*true/.test(viteSrc)
  )
  _check('HTTP 代理目标未变（仍指向 127.0.0.1:5000）', /target:\s*'http:\/\/127\.0\.0\.1:5000'/.test(viteSrc))

  // 清理临时目录
  try {
    fs.rmSync(tmp, { recursive: true, force: true })
  } catch (e) {
    console.log(`  [WARN] 临时目录未清理（不影响结果）：${tmp} -> ${e.message}`)
  }

  console.log('\n' + '='.repeat(68))
  console.log(`结果: 通过 ${_PASSED} 项，失败 ${_FAILED} 项`)
  console.log('='.repeat(68))
  return _FAILED === 0
}

run()
  .then((ok) => {
    process.exit(ok ? 0 : 1)
  })
  .catch((e) => {
    console.error('自检脚本自身异常：', e)
    process.exit(1)
  })
