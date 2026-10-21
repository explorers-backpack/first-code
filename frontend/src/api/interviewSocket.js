/**
 * CareerAI AI 模拟面试 · 实时通道（WebSocket）客户端
 * ============================================================
 * 对应后端 `backend/api/interview_ws.py`。数字人视频面试是一条**有状态的时间线**
 * （数字人播报中 → 等待作答 → 评分中 → 下一题），HTTP 的「请求-响应」表达不了
 * 「服务端主动推状态」，所以这里走 WebSocket。
 *
 * 职责
 * ----
 * 1. 拼 WebSocket URL（**只**拼一次性 ticket；浏览器 WebSocket 无法自定义请求头）；
 * 2. 提供 `InterviewSocket`：连接 / 重连 / 发消息 / 收事件（回调派发）；
 * 3. 把协议常量与「给人看的标签」集中一处，组件不写魔法字符串。
 *
 * ★ 与后端的一致性
 * ----------------
 * 下面的 `SERVER_EVENTS` / `CLIENT_MESSAGES` / `ANSWER_MODES` / 路径模板
 * 与后端**逐字一致**，`backend/tests/test_interview_ws.py` 会读本文件比对字面量，
 * 任何一侧改名都会立刻失败。**改协议必须同时改两边**。
 *
 * 安全约定
 * --------
 * - token 只从 `apiClient.getToken()` 读（不硬编码、不打印）；
 * - 本模块**不**改登录态：4401（未登录）只是把关闭码交给调用方，
 *   由页面决定「提示重新登录」，不在这里跳转、也不在这里清 token。
 * - ★ **凭证只有一种：一次性 ticket**。长期 Bearer token **绝不**进 URL。
 *   取不到 ticket 就**不建连**（fail closed）——不存在「用 token 顶一下」的降级路径，
 *   因为那等于把长期凭证明文写进网关 / 反向代理 / 访问日志，而这类日志的留存
 *   周期与访问控制通常远弱于 `localStorage`。宁可连不上、给用户一个明确的
 *   失败提示与重试按钮，也不要静默降级。
 *
 * 用法
 * ----
 * ```js
 * import { InterviewSocket } from '@/api/interviewSocket'
 *
 * const socket = new InterviewSocket(sessionId, {
 *   onEvent: evt => {
 *     if (evt.type === 'question') currentQuestion.value = evt.question
 *     if (evt.type === 'avatar') avatar.value = evt
 *   },
 *   onClose: code => { if (code === WS_CLOSE_UNAUTHORIZED) ElMessage.error('登录已过期') },
 * })
 * socket.connectWithRetry() // 连接（失败按指数退避 + 抖动重试；拿不到 ticket 就不连）
 * socket.start()          // 开始面试（数字人预检 + 播报第一题）
 * socket.speechDone()     // 播放完毕回调
 * socket.answerByVoice()  // 语音作答（服务端走讯飞虚拟人内置 ASR）
 * socket.answerByText('…')// 打字兜底作答
 * socket.finish()         // 结束并出报告
 * socket.close()          // 组件卸载时务必调用
 * ```
 */

import { apiClient, getToken } from './apiClient'

// ============================================================
// 一、协议常量（★ 必须与 backend/api/interview_ws.py 逐字一致）
// ============================================================
// ---- 服务端 → 客户端 ----
export const SERVER_EVENTS = [
  'hello',
  'state',
  'avatar',
  'question',
  'answer',
  'score',
  'report',
  'error',
  'pong',
]

// ---- 客户端 → 服务端 ----
export const CLIENT_MESSAGES = [
  'start',
  'speech_done',
  'answer',
  'finish',
  'snapshot',
  'ping',
]

// ---- answer 消息的两种提交方式 ----
/** 语音作答：服务端走编排层的 `answer_from_asr`（讯飞虚拟人内置 ASR 取文本）。 */
export const ANSWER_MODE_ASR = 'asr'
/** 打字作答（兜底）：直接提交文本。 */
export const ANSWER_MODE_TEXT = 'text'
export const ANSWER_MODES = [ANSWER_MODE_ASR, ANSWER_MODE_TEXT]

// ---- answer 事件的提交结果 ----
export const ANSWER_SUBMITTED = 'submitted'
export const ANSWER_REJECTED = 'rejected'

// ---- 关闭码（4xxx = 应用自定义，避开标准码） ----
export const WS_CLOSE_UNAUTHORIZED = 4401
export const WS_CLOSE_FORBIDDEN = 4403
export const WS_CLOSE_NOT_FOUND = 4404
/** 空闲超时：服务端长时间没收到任何客户端消息（含心跳）⇒ 主动释放资源。 */
export const WS_CLOSE_IDLE = 4408
/** 并发超限：同时打开的连接 / 会话数超过服务端配额。 */
export const WS_CLOSE_BUSY = 4409

// ---- 稳定错误码（error 事件的 code；据它分支，不解析长句） ----
export const ERROR_BAD_MESSAGE = 'bad_message'
export const ERROR_UNAUTHORIZED = 'unauthorized'
export const ERROR_SESSION_NOT_FOUND = 'session_not_found'
export const ERROR_STATE = 'state_error'
export const ERROR_EMPTY_ANSWER = 'empty_answer'
export const ERROR_SERVICE = 'service_error'
export const ERROR_INTERNAL = 'internal_error'
/** 并发闸门拒绝（同一时刻操作太多 / 上一条还在处理）。 */
export const ERROR_BUSY = 'busy'

// ---- 编排状态（与 services/interview_orchestrator.STATES 一致） ----
export const ORCH_STATES = [
  'waiting_question',
  'avatar_speaking',
  'waiting_answer',
  'processing_answer',
  'finished',
]

/** 编排状态 → 界面文案（视频模式顶部的「面试状态」）。 */
export const ORCH_STATE_LABELS = {
  waiting_question: '准备中',
  avatar_speaking: '数字人播报中',
  waiting_answer: '等待你回答',
  processing_answer: '评分中',
  finished: '面试已结束',
}

/** 通道层 status → 界面文案（数字人区域的状态提示）。 */
export const AVATAR_STATUS_LABELS = {
  ok: '数字人已就绪',
  empty: '未识别到语音，请再说一次',
  unavailable: '数字人服务暂时不可用，可重试',
  expired: '数字人会话已失效，需重建会话',
  unsupported: '当前会话不是视频面试',
  disabled: '数字人未启用（部署配置）',
  failed: '数字人异常',
}

/** 重连最多尝试次数（指数退避；`0` 表示不重连）。 */
export const MAX_RECONNECT_ATTEMPTS = 5

/**
 * 心跳间隔（毫秒）。
 *
 * ★ 为什么必须有：服务端有**空闲超时**（`INTERVIEW_WS_IDLE_TIMEOUT`，默认 300s）——
 * 超时就主动关连接、释放进程内状态。它要清掉的是「拔网线 / 进程被杀 / 浏览器崩溃」
 * 这类**不会发 FIN** 的半开连接；但如果客户端自己不吭声，一个**活着但用户暂时没操作**
 * 的连接也会被当成死的清掉。定期 `ping` 让服务端知道「我还在」，二者才能区分开。
 */
export const HEARTBEAT_INTERVAL_MS = 25000

// ============================================================
// 二、错误类型
// ============================================================
/** 需要登录但未持有 token、或凭证被服务端拒绝时抛出（区别于「连接失败」）。 */
export class SocketAuthError extends Error {
  constructor(message = '未登录或登录状态已失效，请重新登录') {
    super(message)
    this.name = 'SocketAuthError'
  }
}

/**
 * 获取一次性 ticket 失败时抛出。
 *
 * ★ 它是 **fail closed 的载体**：抛出即代表「本次连接被放弃」。
 * 调用方**不得**据此改用长期 token 建连——`retryable` 只表示
 * 「等一会儿用**同一种**凭证重试可能成功」（5xx / 超时 / 网络抖动），
 * 不表示「可以换一种凭证」。
 *
 * 消息里**只**出现状态码与人话，绝不回显 ticket / token / 响应体原文
 * （错误提示会进 UI、也可能进前端错误上报）。
 */
export class SocketTicketError extends Error {
  constructor(message, { status = 0, retryable = true } = {}) {
    super(message)
    this.name = 'SocketTicketError'
    this.status = status
    this.retryable = retryable
  }
}

// ============================================================
// 三、重试策略与错误翻译
// ============================================================
/** 退避基数（毫秒）：第 n 次重试的**名义**等待 = `BASE * 2^n`。 */
export const RECONNECT_BASE_DELAY_MS = 500
/** 退避上限（毫秒）：无论重试多少次，名义等待都不超过它。 */
export const RECONNECT_MAX_DELAY_MS = 15000
/** 抖动比例（equal jitter）：实际等待 = 名义×0.5 + random(0, 名义×0.5)。 */
export const RECONNECT_JITTER_RATIO = 0.5
/** 获取一次性 ticket 的 HTTP 超时（毫秒）。超时即放弃本次建连，不退化成 token。 */
export const TICKET_REQUEST_TIMEOUT_MS = 8000

/**
 * 计算第 `attempt` 次重试要等多久（毫秒）。
 *
 * ★ 为什么必须是**指数 + 抖动**而不是固定间隔：
 * - 纯固定间隔（或线性增长）在服务端整体故障时，会让**所有**客户端以同一节奏
 *   齐步重试，形成周期性尖峰，把刚恢复的服务再打垮一次；
 * - 纯指数没有抖动，同一时刻断线的客户端仍会同步重试；
 * - 抖动取 equal jitter（一半固定、一半随机）：既保留**下限**（不会 0ms 空转），
 *   又保留**上限**（不会因为随机而突破 `RECONNECT_MAX_DELAY_MS`）。
 *
 * @param {number} attempt 从 0 开始的重试序号
 * @param {() => number} [rand] 随机源（测试可注入确定值）
 * @returns {number} 落在 `[名义/2, 名义]` 区间内的整数毫秒
 */
export function computeReconnectDelay(attempt, rand = Math.random) {
  // 非数字 / 负数一律按 0 处理：算不出有限值就会把 setTimeout 的延时变成 NaN
  //（浏览器把 NaN 当 0 ⇒ 退化成**零间隔狂刷**，正是要避免的那种「快速重试」）。
  const n = Number.isFinite(attempt) ? Math.max(0, Math.floor(attempt)) : 0
  const nominal = Math.min(RECONNECT_BASE_DELAY_MS * 2 ** n, RECONNECT_MAX_DELAY_MS)
  const fixed = nominal * (1 - RECONNECT_JITTER_RATIO)
  const raw = typeof rand === 'function' ? Number(rand()) : 0
  const clamped = Number.isFinite(raw) ? Math.min(Math.max(raw, 0), 1) : 0
  return Math.round(fixed + nominal * RECONNECT_JITTER_RATIO * clamped)
}

/**
 * 把 `_fetchTicket` 抛出的异常翻译成「能给人看、且不含凭据」的错误。
 *
 * 分支口径：401/403 = 认证问题（**不重试**，交回登录流程）；
 * 其余 = 凭证服务问题（可重试，但**只能用 ticket 重试**）。
 */
function _toTicketError(e, timeoutMs = TICKET_REQUEST_TIMEOUT_MS) {
  if (e instanceof SocketAuthError || e instanceof SocketTicketError) return e
  const status = (e && typeof e.status === 'number' && e.status) || 0
  if (status === 401) return new SocketAuthError('登录状态已过期，请重新登录')
  if (status === 403) return new SocketAuthError('无权访问该面试会话')
  if (e && e.name === 'AbortError') {
    return new SocketTicketError(
      `获取实时通道凭证超时（${timeoutMs} 毫秒），请检查网络后重试`,
      { status: 0 }
    )
  }
  if (status === 0) {
    return new SocketTicketError('无法连接后端服务，未能获取实时通道凭证，请稍后重试', {
      status: 0,
    })
  }
  if (status >= 500) {
    return new SocketTicketError(
      `实时通道凭证服务暂时不可用（HTTP ${status}），请稍后重试`,
      { status }
    )
  }
  return new SocketTicketError(`获取实时通道凭证失败（HTTP ${status}），请稍后重试`, {
    status,
  })
}

// ============================================================
// 四、URL 拼装
// ============================================================
/**
 * WebSocket 路径模板（与后端 `WS_PATH` 一致）。
 * @param {number|string} sessionId
 * @returns {string}
 */
export function buildSocketPath(sessionId) {
  return `/api/interview/${sessionId}/ws`
}

/**
 * 拼出完整的 WebSocket URL（**只拼 ticket，永不拼长期 token**）。
 *
 * 浏览器 WebSocket 无法自定义请求头，凭证只能走 query ——
 * 因此必须用**短时（30 秒）、单次使用**的 ticket：即使被日志记录，
 * 也只在极短窗口内有效，且用一次即废。
 *
 * ★ 没有 ticket 时**抛错而不是退回 token**：这个函数是「凭证落到 URL 上」的
 * 唯一出口，把 fail-closed 放在这里，任何「忘了先取 ticket」的新调用点都会
 * 立刻炸掉，而不是安静地泄漏一个长期凭证。
 *
 * ★ 这里**不再**拼 `apiClient.API_BASE`：`buildSocketPath` 返回的
 * `/api/interview/{id}/ws` 已经是**根路径下的绝对路径**（与后端 `WS_PATH` 逐字一致），
 * 再前置一个 `/api` 会得到 `/api/api/interview/…` —— 双前缀，浏览器直接连不上。
 * （这个错误在源码里看不出来，只有把 URL 真的拼一遍才会暴露。）
 *
 * @param {number|string} sessionId 面试会话 id
 * @param {string} ticket 一次性 WS ticket（必填）
 * @returns {string} 例如 `ws://localhost:5173/api/interview/12/ws?ticket=xxx`
 * @throws {SocketTicketError} 未提供 ticket 时
 */
export function buildSocketUrl(sessionId, ticket) {
  if (typeof ticket !== 'string' || !ticket) {
    throw new SocketTicketError('缺少一次性连接凭证，已中止建立实时通道', {
      status: 0,
      retryable: true,
    })
  }

  const path = buildSocketPath(sessionId)

  const host =
    typeof window !== 'undefined' && window.location
      ? window.location.host
      : 'localhost'
  const protocol =
    typeof window !== 'undefined' &&
    window.location &&
    window.location.protocol === 'https:'
      ? 'wss:'
      : 'ws:'

  return `${protocol}//${host}${path}?ticket=${encodeURIComponent(ticket)}`
}

// ============================================================
// 五、客户端
// ============================================================

/**
 * 面试实时通道客户端。
 *
 * 事件回调（都是可选）：
 * - `onEvent(evt)`    每收到一条服务端事件调用一次（`evt.type` 见 `SERVER_EVENTS`）；
 * - `onOpen()`        连接建立（此时还没收到 `hello`）；
 * - `onClose(code, reason)` 连接关闭（**含**未登录的 4401/4403/4404 与正常关闭）；
 * - `onError(err)`    底层异常（含自动重连耗尽）；
 * - `onRetry(attempt, delayMs, err)` 第 `attempt` 次重试即将发生（UI 可据此显示倒计时）。
 */
export class InterviewSocket {
  /**
   * @param {number|string} sessionId
   * @param {object} [handlers]
   * @param {(evt: object) => void} [handlers.onEvent]
   * @param {() => void} [handlers.onOpen]
   * @param {(code: number, reason: string) => void} [handlers.onClose]
   * @param {(err: Error) => void} [handlers.onError]
   * @param {(attempt: number, delayMs: number, err: Error) => void} [handlers.onRetry]
   * @param {boolean} [handlers.autoReconnect=true]
   * @param {number}  [handlers.maxAttempts]
   * @param {number}  [handlers.ticketTimeoutMs] 取 ticket 的 HTTP 超时（默认 `TICKET_REQUEST_TIMEOUT_MS`）
   */
  constructor(sessionId, handlers = {}) {
    this.sessionId = sessionId
    this.onEvent = handlers.onEvent || (() => {})
    this.onOpen = handlers.onOpen || (() => {})
    this.onClose = handlers.onClose || (() => {})
    this.onError = handlers.onError || (() => {})
    this.onRetry = handlers.onRetry || (() => {})
    this.autoReconnect = handlers.autoReconnect !== false
    this.maxAttempts =
      handlers.maxAttempts === undefined ? MAX_RECONNECT_ATTEMPTS : handlers.maxAttempts
    this.ticketTimeoutMs =
      handlers.ticketTimeoutMs === undefined
        ? TICKET_REQUEST_TIMEOUT_MS
        : handlers.ticketTimeoutMs

    this.socket = null
    this.attempts = 0
    this.closedByUser = false
    this.retryTimer = null
    this.heartbeatTimer = null
    /** 退避等待中挂起的 resolve（`close()` 时唤醒，避免 Promise 永挂）。 */
    this._retryResolve = null
  }

  /** 是否已连接。 */
  get connected() {
    return !!this.socket && this.socket.readyState === 1 /* OPEN */
  }

  /**
   * 建立连接。未登录（无 token）时**不发请求**，直接抛 `SocketAuthError`
   * ——让调用方给出「请先登录」而不是一个含糊的连接失败。
   *
   * 流程：先 HTTP 获取一次性 ticket → 再用 ticket 连接 WS。
   * ticket 短时（30 秒）且单次使用，避免长期 Bearer Token 暴露在 URL query 中。
   *
   * ★ **fail closed**：ticket 拿不到（网络 / 5xx / 超时 / 响应体无效）就抛
   * `SocketTicketError` 并**放弃本次建连**——绝不退回 `?token=`。
   * 需要「失败后自动再试」的调用方请用 `connectWithRetry()`。
   *
   * @throws {SocketAuthError} 未登录 / 401 / 403
   * @throws {SocketTicketError} 凭证获取失败（可重试，但只重试 ticket 路径）
   */
  async connect() {
    if (!getToken()) throw new SocketAuthError()
    if (this.socket) this._disposeSocket()

    this.closedByUser = false

    const ticket = await this._fetchTicket()
    // ★ 走到这里才拼 URL：没有 ticket 时 `buildSocketUrl` 会抛，
    //   因此**不存在**「URL 里出现长期 token」的分支。
    const url = buildSocketUrl(this.sessionId, ticket)
    const socket = new WebSocket(url)
    this.socket = socket

    socket.onopen = () => {
      this.attempts = 0
      this._startHeartbeat()
      this.onOpen()
    }
    socket.onmessage = event => {
      let payload = null
      try {
        payload = JSON.parse(event.data)
      } catch (e) {
        // 协议只发 JSON；解析不了说明对端异常，如实上报而不是静默丢弃
        this.onError(new Error('收到无法解析的消息（非 JSON）'))
        return
      }
      this.onEvent(payload)
    }
    socket.onerror = () => {
      // 浏览器出于安全不给 WebSocket 错误细节，统一交给 onclose 处理
    }
    socket.onclose = event => {
      this._stopHeartbeat()
      this.onClose(event.code, event.reason || '')
      this._scheduleReconnect(event.code)
    }
    return this
  }

  /**
   * 取一次性 ticket（带超时），失败一律抛 `SocketAuthError` / `SocketTicketError`。
   *
   * ★ 超时用 `AbortController` 而不是「等 fetch 自己超时」：默认 fetch 没有超时，
   * 凭证端点一旦挂住（连接被中间设备静默丢弃），`connect()` 会永远 pending，
   * 界面既没连上也没报错——那是最糟的状态。
   */
  async _fetchTicket() {
    const controller =
      typeof AbortController === 'function' ? new AbortController() : null
    let timer = null
    if (controller) {
      // ★ 这个定时器**不能** unref：`await apiClient.post(...)` 的完成依赖它触发 abort。
      //   Node 里 unref 的定时器不阻止进程退出 ⇒ 事件循环一空就直接退出，
      //   `await` 永远等不到（脚本「跑完了但没跑完」，且退出码还是 0）。
      //   规则：**被 await 的 Promise 所依赖的定时器一律不 unref**。
      timer = setTimeout(() => controller.abort(), this.ticketTimeoutMs)
    }

    let data
    try {
      data = await apiClient.post(
        `/interview/${this.sessionId}/ws-ticket`,
        controller ? { signal: controller.signal } : {}
      )
    } catch (e) {
      throw _toTicketError(e, this.ticketTimeoutMs)
    } finally {
      if (timer) clearTimeout(timer)
    }

    const ticket = data && data.ticket
    if (typeof ticket !== 'string' || !ticket) {
      // 200 但没带 ticket / ticket 不是字符串 ⇒ 契约被破坏，按「凭证无效」处理。
      // 这时**不能**当作「服务端让我用 token」，那正是要根除的降级。
      throw new SocketTicketError('实时通道凭证响应无效，已中止建立连接', {
        status: 200,
      })
    }
    return ticket
  }

  /**
   * 建立连接，失败时按**有上限的指数退避 + 随机抖动**重试。
   *
   * 与 `connect()` 的分工：`connect()` 是「一次尝试，失败就抛」的原子操作；
   * 本方法负责「重试几次」，并且：
   * - `SocketAuthError`（未登录 / 401 / 403）**不重试**——重试只会重复被拒；
   * - 退避期间调用 `close()` 会立刻唤醒等待并终止重试链（不会留下孤儿定时器）；
   * - 用尽 `maxAttempts` 后把**最后一次**错误抛给调用方，由界面给出可理解的提示。
   *
   * @returns {Promise<InterviewSocket>}
   */
  async connectWithRetry() {
    let lastError = null
    for (let attempt = 0; ; attempt += 1) {
      try {
        await this.connect()
        return this
      } catch (e) {
        lastError = e
        if (e instanceof SocketAuthError) throw e
        const retryable = e instanceof SocketTicketError ? e.retryable !== false : true
        if (!retryable || !this.autoReconnect || this.closedByUser) throw e
        if (attempt >= this.maxAttempts) break
        const delay = computeReconnectDelay(attempt)
        this.onRetry(attempt + 1, delay, e)
        await this._sleep(delay)
        if (this.closedByUser) throw e
      }
    }
    throw lastError
  }

  /**
   * 发送一条客户端消息。
   * @param {object} message 例如 `{ type: 'answer', mode: 'asr' }`
   * @returns {boolean} 是否已发出（未连接时返回 false，**不抛异常**）
   */
  send(message) {
    if (!this.connected) return false
    this.socket.send(JSON.stringify(message))
    return true
  }

  /** `{type:'start'}` —— 数字人预检 + 生成整场题目 + 播报第一题。 */
  start() {
    return this.send({ type: 'start' })
  }

  /** `{type:'speech_done'}` —— 数字人播放完毕（前端在音频/视频结束后回调）。 */
  speechDone() {
    return this.send({ type: 'speech_done' })
  }

  /** `{type:'answer', mode:'asr'}` —— 语音作答（服务端取 ASR 文本并评分）。 */
  answerByVoice() {
    return this.send({ type: 'answer', mode: ANSWER_MODE_ASR })
  }

  /** `{type:'answer', mode:'text', text}` —— 打字作答（ASR 不可用时的兜底）。 */
  answerByText(text) {
    return this.send({ type: 'answer', mode: ANSWER_MODE_TEXT, text: String(text || '') })
  }

  /** `{type:'finish'}` —— 结束面试并生成报告。 */
  finish() {
    return this.send({ type: 'finish' })
  }

  /** `{type:'snapshot'}` —— 主动要一次完整状态（断线重连后恢复界面）。 */
  snapshot() {
    return this.send({ type: 'snapshot' })
  }

  /** `{type:'ping'}` —— 心跳。 */
  ping() {
    return this.send({ type: 'ping' })
  }

  /** 主动关闭（组件卸载时必须调用，否则会触发自动重连）。 */
  close() {
    this.closedByUser = true
    this._clearRetry()
    this._stopHeartbeat()
    if (this.socket) {
      try {
        this.socket.close(1000, 'client closed')
      } catch (e) {
        // 已断开时关闭会抛，忽略
      }
    }
    this.socket = null
  }

  // ---- 内部 ----
  _clearRetry() {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer)
      this.retryTimer = null
    }
    // 正在退避等待时被 close()：必须唤醒挂起的 Promise，
    // 否则 `connectWithRetry()` 会永远 pending（既不 resolve 也不 reject）。
    const pending = this._retryResolve
    this._retryResolve = null
    if (pending) pending()
  }

  /** 可被 `close()` 打断的等待（`connectWithRetry` 的退避）。 */
  _sleep(ms) {
    if (typeof setTimeout !== 'function') return Promise.resolve()
    return new Promise(resolve => {
      // ★ 不 unref：有调用方在 `await` 它。unref 会让 Node 在事件循环变空时直接退出，
      //   Promise 永远不 settle（详见 `_fetchTicket` 的说明）。
      this.retryTimer = setTimeout(() => {
        this.retryTimer = null
        this._retryResolve = null
        resolve()
      }, ms)
      this._retryResolve = resolve
    })
  }

  /**
   * 启动心跳（幂等：先停后启，避免重复连接时残留多个定时器）。
   *
   * 非浏览器环境（Node 自检脚本）里对定时器调 `unref()`——否则一个没被清掉的
   * 定时器会把 Node 进程一直吊着，自检脚本就「跑完了但不退出」。
   */
  _startHeartbeat() {
    this._stopHeartbeat()
    if (typeof setInterval !== 'function') return
    this.heartbeatTimer = setInterval(() => {
      if (!this.connected) return
      this.ping()
    }, HEARTBEAT_INTERVAL_MS)
    if (this.heartbeatTimer && typeof this.heartbeatTimer.unref === 'function') {
      this.heartbeatTimer.unref()
    }
  }

  _stopHeartbeat() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = null
    }
  }

  _disposeSocket() {
    const old = this.socket
    this.socket = null
    this._stopHeartbeat()
    if (!old) return
    old.onopen = null
    old.onmessage = null
    old.onerror = null
    old.onclose = null
    try {
      old.close()
    } catch (e) {
      // 忽略
    }
  }

  /**
   * 断线重连（**只在连接曾经建立过之后**由 `onclose` 触发）。
   *
   * **不重连**的情况：用户主动关闭、未登录（4401）、无权（4403）、会话不存在（4404）、
   * 空闲超时（4408）、并发超限（4409）——它们重连多少次都不会成功（或只会立刻再被拒），
   * 只会把「请重新登录」「窗口太多」这类提示淹没在重试里。
   */
  _scheduleReconnect(code) {
    if (this.closedByUser || !this.autoReconnect) return
    if (
      code === WS_CLOSE_UNAUTHORIZED ||
      code === WS_CLOSE_FORBIDDEN ||
      code === WS_CLOSE_NOT_FOUND ||
      code === WS_CLOSE_IDLE ||
      code === WS_CLOSE_BUSY
    ) {
      return
    }
    this._retry()
  }

  /**
   * 排一次重连（指数退避 + 抖动，次数有上限）。
   *
   * 与 `connectWithRetry()` 的区别：本方法只排**一次**，且不返回 Promise ——
   * 它服务的是「连接已建立过、随后断开」这条异步链路，没有调用方在等结果。
   */
  _retry() {
    if (this.closedByUser || !this.autoReconnect) return
    if (this.attempts >= this.maxAttempts) {
      this.onError(new Error(`实时通道重连失败（已尝试 ${this.attempts} 次）`))
      return
    }
    const delay = computeReconnectDelay(this.attempts)
    this.attempts += 1
    this.onRetry(this.attempts, delay, null)
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null
      if (this.closedByUser) return
      // connect() 是 async：**必须**接住 Promise 拒绝，否则重连时 401 会变成
      // unhandled rejection（try/catch 只能拦同步抛出）。
      try {
        // 服务端在**每次**连接建立后都会推一次快照（state + question + avatar），
        // 所以重连本身即完成界面恢复，不需要额外再要一次 snapshot。
        const p = this.connect()
        if (p && typeof p.catch === 'function') {
          p.catch(e => {
            this.onError(e)
            if (e instanceof SocketAuthError) return
            // ★ 凭证获取失败时**没有 socket**，因此不会有 onclose 来续链；
            //   必须在这里主动排下一次，否则一次 503 就把自动重连彻底停掉。
            this._retry()
          })
        }
      } catch (e) {
        this.onError(e)
      }
    }, delay)
    if (this.retryTimer && typeof this.retryTimer.unref === 'function') {
      this.retryTimer.unref()
    }
  }
}

export default InterviewSocket
