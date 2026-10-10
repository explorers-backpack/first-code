/**
 * 前端自检共用：把 `src/api/*.js` 搬进临时目录后加载。
 * ============================================================
 * **不是回归套件**（无 `test_` 前缀，不进任何循环），只是被 `test_interview_mode.js`
 * 与 `interview_ws_integration_run.js` 复用的加载器。
 *
 * 为什么需要它
 * ------------
 * `interviewSocket.js` 写的是 `import { getToken } from './apiClient'`（**无扩展名**）。
 * Vite 能解析这种写法，**Node ESM 不能**（`ERR_MODULE_NOT_FOUND`）。
 *
 * 为了**不为了测试而改动生产源码**，这里把两个模块复制到系统临时目录：
 * 只把 `'./apiClient'` 补成 `'./apiClient.js'`，并补一个 `{"type":"module"}` 的
 * `package.json`（临时目录里没有 package.json，`.js` 会被当成 CommonJS 解析）。
 * 生产文件一个字节都没动。
 */

import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const HERE = path.dirname(fileURLToPath(import.meta.url))
export const FRONTEND = path.resolve(HERE, '..')
export const SRC_API = path.join(FRONTEND, 'src', 'api')
export const COMPONENTS = path.join(FRONTEND, 'src', 'components')

/**
 * 把 `apiClient.js` + `interviewSocket.js` 复制到临时目录并动态导入。
 * @returns {Promise<{mod: object, tmp: string}>} `tmp` 供调用方自行清理
 */
export async function loadSocketModule() {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'career-ai-fe-socket-'))
  fs.writeFileSync(path.join(tmp, 'package.json'), JSON.stringify({ type: 'module' }), 'utf8')
  fs.copyFileSync(path.join(SRC_API, 'apiClient.js'), path.join(tmp, 'apiClient.js'))
  const socketSrc = fs
    .readFileSync(path.join(SRC_API, 'interviewSocket.js'), 'utf8')
    // 只补扩展名，其余逐字节保持
    .replace("from './apiClient'", "from './apiClient.js'")
  fs.writeFileSync(path.join(tmp, 'interviewSocket.js'), socketSrc, 'utf8')

  const mod = await import(pathToFileURL(path.join(tmp, 'interviewSocket.js')).href)
  return { mod, tmp }
}

/** 极简 `localStorage` 替身（`apiClient.getToken()` 从它读 token）。 */
export function installLocalStorage(token) {
  const store = new Map()
  if (token) store.set('career_ai_token', token)
  globalThis.localStorage = {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => store.set(k, String(v)),
    removeItem: (k) => store.delete(k),
    clear: () => store.clear(),
  }
}

/**
 * 伪装 `window.location`，让 `buildSocketUrl` 走真实分支。
 *
 * 非浏览器环境下 `buildSocketUrl` 会退化为 `ws://localhost`——那**测不到**
 * 「协议 / 主机是否正确」。传一个 base（如 `http://127.0.0.1:5000`）就能让它
 * 真的拼出 `ws://127.0.0.1:5000/api/interview/{id}/ws?token=…`。
 *
 * @param {string} base 形如 `http://127.0.0.1:5000`
 */
export function installFakeLocation(base) {
  const url = new URL(base)
  globalThis.window = {
    location: {
      host: url.host,
      protocol: url.protocol,
      href: url.href,
    },
  }
}
