/**
 * CareerAI 统一 API 客户端
 * ============================================================
 * 唯一请求层。业务模块（resume / jobs / chat / interview / admin…）
 * 一律通过本模块发起请求，**不得各自重复书写 `Authorization: Bearer ...`**。
 *
 * 职责
 * ----
 * 1. 自动从 localStorage 读取当前 token；
 * 2. 需要认证的请求自动注入请求头 `Authorization: Bearer <token>`；
 * 3. 提供 GET / POST / PUT / PATCH / DELETE 与底层 `request()`；
 * 4. 统一解析响应体（JSON 优先），非 2xx 一律抛出 `ApiError`；
 * 5. 提供 `publicApi` 供无需认证的公共接口使用（不携带 Authorization）；
 * 6. 401 / 403 等错误**明确抛出**，不静默吞掉、不做页面跳转、不改动登录状态。
 *
 * 安全约定
 * --------
 * - token 仅从 localStorage 读取，**不硬编码、不打印到 console**；
 * - 前端不存放任何 API Secret / 密钥（后端 Spark 密钥一律留在服务端）。
 *
 * 用法
 * ----
 * ```js
 * import { apiClient, publicApi } from '@/api/apiClient'
 *
 * // 需要认证（自动带 Bearer）
 * const data = await apiClient.get('/admin/user-logs')
 * await apiClient.post('/admin/add-job', { body: jobForm })
 *
 * // 无需认证的公共接口（不携带 Authorization）
 * const res = await publicApi.post('/auth/login', { body: { email, password } })
 *
 * // 上传文件：直接传 FormData，Content-Type 由浏览器自动带 boundary
 * await apiClient.post('/resume/analyze', { body: formData })
 * ```
 *
 * 错误处理
 * --------
 * ```js
 * try {
 *   await apiClient.get('/admin/user-logs')
 * } catch (e) {
 *   if (e.isUnauthorized) { /* 401：提示重新登录，但不在此处跳转 *\/ }
 *   else ElMessage.error(e.message)
 * }
 * ```
 */

// 后端统一前缀（与原 App.vue 中的 apiBase 一致；开发环境由 vite 代理到 FastAPI）
export const API_BASE = '/api'

// token / 用户信息的 localStorage 键。
// 写入统一走 TOKEN_KEY；读取时兼容旧键，避免历史写入位置不同导致取不到 token。
const TOKEN_KEY = 'career_ai_token'
const TOKEN_LEGACY_KEYS = ['token']
const USER_KEY = 'career_ai_user'

// ============================================================
// 一、token 与当前用户存取（业务代码请使用这些函数，勿直连 localStorage）
// ============================================================
export function getToken() {
  try {
    const primary = localStorage.getItem(TOKEN_KEY)
    if (primary) return primary
    for (const key of TOKEN_LEGACY_KEYS) {
      const value = localStorage.getItem(key)
      if (value) return value
    }
  } catch (e) {
    // 隐私模式 / localStorage 被禁用：视为未登录，不抛错
  }
  return null
}

export function setToken(token) {
  try {
    if (token) localStorage.setItem(TOKEN_KEY, token)
    else localStorage.removeItem(TOKEN_KEY)
  } catch (e) {
    // 写入失败不阻断主流程
  }
}

export function clearToken() {
  setToken(null)
}

export function hasToken() {
  return !!getToken()
}

export function getStoredUser() {
  try {
    const raw = localStorage.getItem(USER_KEY)
    return raw ? JSON.parse(raw) : null
  } catch (e) {
    return null
  }
}

export function setStoredUser(user) {
  try {
    if (user) localStorage.setItem(USER_KEY, JSON.stringify(user))
    else localStorage.removeItem(USER_KEY)
  } catch (e) {
    // 忽略
  }
}

// ============================================================
// 二、错误类型
// ============================================================
export class ApiError extends Error {
  constructor(message, { status = 0, detail = null, payload = null, url = '', method = '' } = {}) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.detail = detail
    this.payload = payload
    this.url = url
    this.method = method
  }

  get isUnauthorized() {
    return this.status === 401
  }

  get isForbidden() {
    return this.status === 403
  }

  get isNetworkError() {
    return this.status === 0
  }
}

const STATUS_MESSAGE = {
  0: '无法连接后端服务，请确认服务已启动',
  400: '请求参数有误',
  401: '登录状态无效或已过期，请重新登录',
  403: '没有权限执行该操作',
  404: '请求的接口不存在',
  405: '请求方法不被允许',
  415: '不支持的文件类型',
  422: '请求参数校验未通过',
  500: '服务器内部错误',
  502: '网关错误',
  503: '服务暂不可用',
}

function _extractDetail(data) {
  if (!data || typeof data !== 'object') return null
  const detail = data.detail ?? data.message ?? data.error
  if (typeof detail === 'string') return detail
  if (Array.isArray(detail)) {
    // FastAPI 422 校验错误为数组结构
    return detail
      .map(item => (item && (item.msg || item.message)) || JSON.stringify(item))
      .join('；')
  }
  return null
}

function _buildUrl(path, query) {
  const url = /^https?:\/\//i.test(path)
    ? path
    : `${API_BASE}${path.startsWith('/') ? path : `/${path}`}`

  if (!query || typeof query !== 'object') return url

  const params = new URLSearchParams()
  Object.entries(query).forEach(([key, value]) => {
    if (value !== undefined && value !== null) params.append(key, String(value))
  })
  const qs = params.toString()
  return qs ? `${url}${url.includes('?') ? '&' : '?'}${qs}` : url
}

// FormData / Blob 等应原样交给 fetch，由其自动生成 Content-Type（含 boundary）
function _isPlainBody(body) {
  return (
    body !== undefined &&
    body !== null &&
    !(body instanceof FormData) &&
    !(body instanceof Blob) &&
    !(body instanceof URLSearchParams) &&
    !(body instanceof ArrayBuffer)
  )
}

// ============================================================
// 三、核心请求方法
// ============================================================
/**
 * @param {string} method  HTTP 方法
 * @param {string} path    以 / 开头的后端路径（如 '/admin/user-logs'）或完整 URL
 * @param {object} [options]
 * @param {*}      [options.body]        请求体：普通对象自动 JSON 序列化；FormData/Blob 原样传递
 * @param {object} [options.headers]     附加请求头
 * @param {boolean}[options.auth=true]   是否需要认证；false 时不携带 Authorization
 * @param {object} [options.query]       查询参数对象
 * @param {AbortSignal} [options.signal] 中断信号
 * @param {RequestCredentials} [options.credentials='same-origin']
 * @returns {Promise<any>} 解析后的响应体（JSON 或文本；204 返回 null）
 * @throws {ApiError} 网络异常或非 2xx 响应
 */
export async function request(method, path, options = {}) {
  const {
    body,
    headers = {},
    auth = true,
    query,
    signal,
    credentials = 'same-origin',
  } = options

  const url = _buildUrl(path, query)
  const finalHeaders = { Accept: 'application/json', ...headers }
  let payload

  if (_isPlainBody(body)) {
    payload = JSON.stringify(body)
    const hasContentType = Object.keys(finalHeaders).some(
      key => key.toLowerCase() === 'content-type'
    )
    if (!hasContentType) finalHeaders['Content-Type'] = 'application/json'
  } else {
    payload = body
  }

  // —— 统一注入 Authorization：唯一注入点，业务代码无需重复书写 ——
  if (auth) {
    const token = getToken()
    if (token) finalHeaders['Authorization'] = `Bearer ${token}`
  }

  let response
  try {
    response = await fetch(url, {
      method,
      headers: finalHeaders,
      body: payload,
      signal,
      credentials,
    })
  } catch (e) {
    // 主动中断（AbortController）原样抛出，交由调用方处理
    if (e && e.name === 'AbortError') throw e
    throw new ApiError(STATUS_MESSAGE[0], { status: 0, url, method })
  }

  let data = null
  if (response.status !== 204 && response.status !== 205) {
    const text = await response.text()
    if (text) {
      try {
        data = JSON.parse(text)
      } catch (e) {
        data = text
      }
    }
  }

  if (!response.ok) {
    const detail = _extractDetail(data)
    // 401/403 等一律抛出明确错误：不静默吞掉、不跳转、不改动登录状态。
    // 需要清理登录态时由调用方显式调用 clearToken()。
    throw new ApiError(
      detail || STATUS_MESSAGE[response.status] || `请求失败（HTTP ${response.status}）`,
      { status: response.status, detail, payload: data, url, method }
    )
  }

  return data
}

// ============================================================
// 四、对外实例
// ============================================================
const bind = method => (path, options = {}) => request(method, path, options)

/** 需要认证的接口：自动携带 Authorization */
export const apiClient = {
  request,
  get: bind('GET'),
  post: bind('POST'),
  put: bind('PUT'),
  patch: bind('PATCH'),
  delete: bind('DELETE'),
}

const bindPublic = method => (path, options = {}) =>
  request(method, path, { ...options, auth: false })

/** 公共接口：明确不携带 Authorization（如 /auth/login、/auth/register、/health） */
export const publicApi = {
  get: bindPublic('GET'),
  post: bindPublic('POST'),
  put: bindPublic('PUT'),
  patch: bindPublic('PATCH'),
  delete: bindPublic('DELETE'),
}

export default apiClient
