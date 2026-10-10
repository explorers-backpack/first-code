/**
 * CareerAI AI 模拟面试 · 实时通道（WebSocket）客户端
 * ============================================================
 * 对应后端 `backend/api/interview_ws.py`。数字人视频面试是一条**有状态的时间线**
 * （数字人播报中 → 等待作答 → 评分中 → 下一题），HTTP 的「请求-响应」表达不了
 * 「服务端主动推状态」，所以这里走 WebSocket。
 *
 * 职责
 * ----
 * 1. 拼 WebSocket URL（token 走 query，因为浏览器 WebSocket 无法自定义请求头）；
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
 * socket.connect()
 * socket.start()          // 开始面试（数字人预检 + 播报第一题）
 * socket.speechDone()     // 播放完毕回调
 * socket.answerByVoice()  // 语音作答（服务端走讯飞虚拟人内置 ASR）
 * socket.answerByText('…')// 打字兜底作答
 * socket.finish()         // 结束并出报告
 * socket.close()          // 组件卸载时务必调用
 * ```
 */

import { getToken } from './apiClient'

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
// 二、URL 拼装
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
 * 拼出完整的 WebSocket URL。
 *
 * token 走 query（`?token=…`）——浏览器 `WebSocket` **不能**自定义请求头，
 * 这是后端刻意支持的鉴权方式（口径与 `deps.get_current_user` 一致）。
 *
 * ★ 这里**不再**拼 `apiClient.API_BASE`：`buildSocketPath` 返回的
 * `/api/interview/{id}/ws` 已经是**根路径下的绝对路径**（与后端 `WS_PATH` 逐字一致），
 * 再前置一个 `/api` 会得到 `/api/api/interview/…` —— 双前缀，浏览器直接连不上。
 * （这个错误在源码里看不出来，只有把 URL 真的拼一遍才会暴露。）
 *
 * @param {number|string} sessionId 面试会话 id
 * @param {string} [token] 显式 token；缺省从 `apiClient.getToken()` 取
 * @returns {string} 例如 `ws://localhost:5173/api/interview/12/ws?token=xxx`
 */
export function buildSocketUrl(sessionId, token) {
  const value = token === undefined || token === null ? getToken() : token
  const safe = value || ''
  const path = buildSocketPath(sessionId)

  // 开发环境走 vite 代理（同源 /api → 后端 5000），生产同源部署 —— 都用 location.host。
  // 非浏览器环境（如单测）没有 location，退化为相对主机占位。
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

  return `${protocol}//${host}${path}?token=${encodeURIComponent(safe)}`
}

// ============================================================
// 三、客户端
// ============================================================
/** 需要登录但未持有 token 时抛出（区别于「连接失败」）。 */
export class SocketAuthError extends Error {
  constructor(message = '未登录或登录状态已失效，请重新登录') {
    super(message)
    this.name = 'SocketAuthError'
  }
}

/**
 * 面试实时通道客户端。
 *
 * 事件回调（都是可选）：
 * - `onEvent(evt)`    每收到一条服务端事件调用一次（`evt.type` 见 `SERVER_EVENTS`）；
 * - `onOpen()`        连接建立（此时还没收到 `hello`）；
 * - `onClose(code, reason)` 连接关闭（**含**未登录的 4401/4403/4404 与正常关闭）；
 * - `onError(err)`    底层异常（含自动重连耗尽）。
 */
export class InterviewSocket {
  /**
   * @param {number|string} sessionId
   * @param {object} [handlers]
   * @param {(evt: object) => void} [handlers.onEvent]
   * @param {() => void} [handlers.onOpen]
   * @param {(code: number, reason: string) => void} [handlers.onClose]
   * @param {(err: Error) => void} [handlers.onError]
   * @param {boolean} [handlers.autoReconnect=true]
   * @param {number}  [handlers.maxAttempts]
   */
  constructor(sessionId, handlers = {}) {
    this.sessionId = sessionId
    this.onEvent = handlers.onEvent || (() => {})
    this.onOpen = handlers.onOpen || (() => {})
    this.onClose = handlers.onClose || (() => {})
    this.onError = handlers.onError || (() => {})
    this.autoReconnect = handlers.autoReconnect !== false
    this.maxAttempts =
      handlers.maxAttempts === undefined ? MAX_RECONNECT_ATTEMPTS : handlers.maxAttempts

    this.socket = null
    this.attempts = 0
    this.closedByUser = false
    this.retryTimer = null
    this.heartbeatTimer = null
  }

  /** 是否已连接。 */
  get connected() {
    return !!this.socket && this.socket.readyState === 1 /* OPEN */
  }

  /**
   * 建立连接。未登录（无 token）时**不发请求**，直接抛 `SocketAuthError`
   * ——让调用方给出「请先登录」而不是一个含糊的连接失败。
   */
  connect() {
    if (!getToken()) throw new SocketAuthError()
    if (this.socket) this._disposeSocket()

    this.closedByUser = false
    const url = buildSocketUrl(this.sessionId)
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
   * 断线重连。
   *
   * **不重连**的情况：用户主动关闭、未登录（4401）、会话不存在（4404）、
   * 空闲超时（4408）、并发超限（4409）——它们重连多少次都不会成功（或只会立刻再被拒），
   * 只会把「请重新登录」「窗口太多」这类提示淹没在重试里。
   */
  _scheduleReconnect(code) {
    if (this.closedByUser || !this.autoReconnect) return
    if (
      code === WS_CLOSE_UNAUTHORIZED ||
      code === WS_CLOSE_NOT_FOUND ||
      code === WS_CLOSE_IDLE ||
      code === WS_CLOSE_BUSY
    ) {
      return
    }
    if (this.attempts >= this.maxAttempts) {
      this.onError(new Error(`实时通道重连失败（已尝试 ${this.attempts} 次）`))
      return
    }
    const delay = Math.min(1000 * 2 ** this.attempts, 8000)
    this.attempts += 1
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null
      if (this.closedByUser) return
      try {
        // 服务端在**每次**连接建立后都会推一次快照（state + question + avatar），
        // 所以重连本身即完成界面恢复，不需要额外再要一次 snapshot。
        this.connect()
      } catch (e) {
        this.onError(e)
      }
    }, delay)
  }
}

export default InterviewSocket
