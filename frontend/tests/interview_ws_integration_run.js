/**
 * 前端 · 实时通道「真机端到端」运行器
 * ============================================================
 * **不是回归套件**（无 `test_` 前缀、不进任何循环、不进 `npm test`），
 * 与后端 `tests/rag_vector_backend_switch_run.py` 同一性质：**按需手动跑**。
 *
 * 为什么需要它
 * ------------
 * `test_interview_mode.js` 用的是**假 `WebSocket`**——它能验证「消息怎么拼、重连怎么退避」，
 * 但**验证不了「客户端与真实服务端之间那条缝」**。
 * 任务 94 恰恰在这条缝上踩了一个坑：`buildSocketUrl` 把 `API_BASE='/api'` 又拼了一遍，
 * 生成 `/api/api/interview/…` **双前缀**；而后端的字面量守卫**查不出**这种拼接错误
 * （它只断言源码里含那个字符串）。**只有真的连一次才会暴露。**
 *
 * 本运行器用 Node 22 内置的 `WebSocket`（undici）连**真实 uvicorn**，走完整链路：
 * 建会话（avatar 模式）→ 连接 → 快照 → start → 数字人播报 → speech_done →
 * ASR 作答 → 打字作答 → 下一题 → finish → 报告。
 *
 * 用法
 * ----
 * ```bash
 * # 1) 起一个后端（端口任意，默认 5000）
 * cd backend && python -m uvicorn main:app --port 5000
 * # 2) 取一个登录 token，然后：
 * cd frontend && CAREER_AI_TOKEN=<token> node tests/interview_ws_integration_run.js
 * ```
 *
 * 环境变量
 * --------
 * | 变量 | 默认 | 说明 |
 * |---|---|---|
 * | `CAREER_AI_TOKEN` | （必填） | 登录 token（`user_session.token`） |
 * | `CAREER_AI_BASE` | `http://127.0.0.1:5000` | 后端地址 |
 * | `CAREER_AI_JOB_ID` | 不带 | 指定岗位 id |
 * | `CAREER_AI_QUESTIONS` | `2` | 题量（跑完整闭环用 2 即可） |
 *
 * 退出码：`0` 全部通过；`1` 有失败或异常。
 */

import { installFakeLocation, installLocalStorage, loadSocketModule } from './socket_loader.js'

const TOKEN = process.env.CAREER_AI_TOKEN || ''
const BASE = (process.env.CAREER_AI_BASE || 'http://127.0.0.1:5000').replace(/\/+$/, '')
const JOB_ID = process.env.CAREER_AI_JOB_ID ? Number(process.env.CAREER_AI_JOB_ID) : undefined
const QUESTIONS = Number(process.env.CAREER_AI_QUESTIONS || 2)

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

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

async function main() {
  console.log('='.repeat(68))
  console.log('前端 · 实时通道真机端到端（对真实 uvicorn）')
  console.log('='.repeat(68))
  console.log(`后端：${BASE}`)
  if (!TOKEN) {
    console.error('缺少 CAREER_AI_TOKEN（登录 token）。用法见本文件头部。')
    return false
  }

  installFakeLocation(BASE) // 让 buildSocketUrl 走真实分支（否则退化成 ws://localhost）
  installLocalStorage(TOKEN)

  const { mod, tmp } = await loadSocketModule()
  const { InterviewSocket, buildSocketUrl, ORCH_STATE_LABELS } = mod

  try {
    // ------------------------------------------------------------
    // [1] 建会话（avatar 模式）—— 证明 mode 真的落库并回传
    // ------------------------------------------------------------
    console.log('\n[1] 建会话（mode=avatar）')
    const createResp = await fetch(`${BASE}/api/interview/create`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${TOKEN}` },
      body: JSON.stringify({
        mode: 'avatar',
        total_questions: QUESTIONS,
        ...(JOB_ID ? { job_id: JOB_ID } : {}),
      }),
    })
    const created = await createResp.json()
    if (!_check('POST /api/interview/create 成功', createResp.ok, `${createResp.status} ${JSON.stringify(created).slice(0, 200)}`)) {
      return false
    }
    const sessionId = created.session && created.session.id
    _check('会话已创建且 id 为正整数', Number.isInteger(sessionId) && sessionId > 0, String(sessionId))
    _check(
      '★ interview_mode 落库为 avatar（不是默认 text）',
      created.session.interview_mode === 'avatar',
      String(created.session.interview_mode)
    )
    _check('初始状态为 created', created.session.status === 'created', String(created.session.status))

    // ------------------------------------------------------------
    // [2] 连接实时通道（真实 WebSocket）
    // ------------------------------------------------------------
    console.log('\n[2] 连接实时通道')
    const events = []
    const closes = []
    const errors = []
    const sock = new InterviewSocket(sessionId, {
      onEvent: (e) => events.push(e),
      onClose: (code, reason) => closes.push([code, reason]),
      onError: (e) => errors.push(e),
      autoReconnect: false,
    })

    const url = buildSocketUrl(sessionId)
    console.log(`      URL = ${url.replace(/token=.*/, 'token=<redacted>')}`)
    _check(
      '★ URL 只有**一个** /api 前缀（双前缀 bug 的回归守卫）',
      (url.match(/\/api\//g) || []).length === 1 && url.includes(`/api/interview/${sessionId}/ws`),
      url
    )
    _check('URL 走 ws:（http base）', url.startsWith('ws://'), url)

    sock.connect()

    /** 等到「最新一条事件是指定类型」为止，返回这一步新收到的事件。 */
    async function waitUntil(type, from, timeoutMs = 45000) {
      const deadline = Date.now() + timeoutMs
      while (Date.now() < deadline) {
        const tail = events[events.length - 1]
        if (events.length > from && tail && tail.type === type) return events.slice(from)
        if (closes.length) throw new Error(`连接已关闭：${JSON.stringify(closes[0])}`)
        await sleep(25)
      }
      throw new Error(
        `等待 ${type} 超时（本步已收到：${events.slice(from).map((e) => e.type).join(',') || '无'}）`
      )
    }

    const snap = await waitUntil('avatar', 0)
    _check(
      '★ 连接后服务端推快照 hello → state → question → avatar（顺序恒定）',
      snap.map((e) => e.type).join(',') === 'hello,state,question,avatar',
      snap.map((e) => e.type).join(',')
    )
    _check('hello 自报协议能力（事件 / 消息 / 作答方式）', snap[0].session_id === sessionId && Array.isArray(snap[0].server_events) && snap[0].server_events.length === 9)
    _check(
      '★ 首帧 state 不是 None（adopt_state 按库中 created 登记为 waiting_question）',
      snap[1].state === 'waiting_question',
      String(snap[1].state)
    )
    _check('created 会话的 question 为 null（尚未出题）', snap[2].question === null)
    _check('avatar 事件带配置层信息（provider / configured）', 'provider' in snap[3] && 'configured' in snap[3], JSON.stringify(snap[3]).slice(0, 160))
    _check(
      '★ 快照的 avatar 是「未探测」（session_probed=false, ok=null），不是「探测失败」',
      snap[3].session_probed === false && snap[3].ok === null,
      JSON.stringify(snap[3]).slice(0, 160)
    )

    // ------------------------------------------------------------
    // [3] start：数字人预检 + 生成整场题目 + 播报第一题
    // ------------------------------------------------------------
    console.log('\n[3] start：预检 + 出题 + 播报第一题')
    let from = events.length
    sock.start()
    const started = await waitUntil('question', from)
    _check(
      'start 后首推 avatar（预检结果），末推 question（第一题），中间有 state',
      started[0].type === 'avatar' &&
        started[started.length - 1].type === 'question' &&
        started.some((e) => e.type === 'state'),
      started.map((e) => e.type).join(',')
    )
    _check('★ avatar 预检已执行（session_probed=true）', started[0].session_probed === true, JSON.stringify(started[0]).slice(0, 200))
    _check(
      `数字人已就绪（provider=${started[0].provider}，Mock 下 ok=true）`,
      started[0].ok === true && started[0].status === 'ok',
      `ok=${started[0].ok} status=${started[0].status} err=${started[0].error}`
    )
    _check(
      '★ 状态推进到 avatar_speaking（数字人播报中）',
      started.some((e) => e.type === 'state' && e.state === 'avatar_speaking'),
      started.filter((e) => e.type === 'state').map((e) => e.state).join(',')
    )
    const q1 = started.find((e) => e.type === 'question')
    _check('第一题已交付（question_no=1）', q1 && q1.question && q1.question.question_no === 1, JSON.stringify(q1 && q1.question).slice(0, 160))
    _check('会话状态变为 ongoing', q1 && q1.session && q1.session.status === 'ongoing', q1 && q1.session && q1.session.status)
    _check(
      '★ question 事件的 session/question 与 HTTP 契约**同一形状**（含 id / question_no / expected_points 键）',
      q1 && q1.question && 'id' in q1.question && 'question_no' in q1.question && 'expected_points' in q1.question
    )

    // ------------------------------------------------------------
    // [4] speech_done：播报结束 → 等待作答
    // ------------------------------------------------------------
    console.log('\n[4] speech_done')
    from = events.length
    sock.speechDone()
    const afterSpeech = await waitUntil('state', from)
    _check(
      'speech_done ⇒ state 推进到 waiting_answer',
      afterSpeech[0].state === 'waiting_answer',
      afterSpeech.map((e) => e.type + ':' + (e.state || '')).join(',')
    )
    _check('speech_done 只推一条 state（不重复推题）', afterSpeech.length === 1, afterSpeech.map((e) => e.type).join(','))

    // ------------------------------------------------------------
    // [5] 语音作答（ASR）—— 取不到文本必须**拒收且不落库**
    // ------------------------------------------------------------
    console.log('\n[5] 语音作答（ASR）')
    from = events.length
    sock.answerByVoice()
    const asrBatch = await waitUntil('state', from)
    _check('ASR 作答后收到 answer 事件', asrBatch[0] && asrBatch[0].type === 'answer', asrBatch.map((e) => e.type).join(','))
    _check(
      'answer 事件带恒定键集（status/answer/text/source/asr/error）',
      asrBatch[0] &&
        ['status', 'answer', 'text', 'source', 'asr', 'error'].every((k) => k in asrBatch[0]),
      JSON.stringify(asrBatch[0]).slice(0, 200)
    )
    _check(
      'status ∈ {submitted, rejected}',
      asrBatch[0] && ['submitted', 'rejected'].includes(asrBatch[0].status),
      String(asrBatch[0] && asrBatch[0].status)
    )
    if (asrBatch[0] && asrBatch[0].status === 'rejected') {
      _check(
        '★ ASR 取不到文本 ⇒ **拒收**且状态**不变**（仍在 waiting_answer，题号未推进）',
        asrBatch.some((e) => e.type === 'state' && e.state === 'waiting_answer') &&
          !asrBatch.some((e) => e.type === 'score'),
        asrBatch.map((e) => e.type + ':' + (e.state || '')).join(',')
      )
    } else {
      console.log('  [INFO] 本次 ASR 成功（provider 配了 answer_text）⇒ 跳过拒收断言')
    }

    // ------------------------------------------------------------
    // [6] 打字作答：answer → score → question → state
    // ★ 答完一题后状态回到 avatar_speaking（数字人在播报**下一题**）⇒
    //   必须再发一次 speech_done 才能作答下一题。这是编排层的**真实协议**，
    //   前端必须据此给「提交回答」设闸（否则只会拿到 state_error）。
    // ------------------------------------------------------------
    console.log('\n[6] 打字作答（逐题答完）')
    for (let i = 1; i <= QUESTIONS; i += 1) {
      from = events.length
      sock.answerByText(`第 ${i} 题的回答：我做过订单中台重构，用 Redis 做缓存，QPS 从 800 提升到 5000。`)
      const batch = await waitUntil('state', from)
      _check(
        `第 ${i} 题：answer → score → question → state 四连推（顺序恒定）`,
        batch.map((e) => e.type).join(',') === 'answer,score,question,state',
        batch.map((e) => e.type).join(',')
      )
      const ans = batch[0] || {}
      _check(`第 ${i} 题：作答被接受（status=submitted）`, ans.status === 'submitted', JSON.stringify(ans).slice(0, 160))
      _check(`第 ${i} 题：文本来源标记为 text`, ans.source === 'text', String(ans.source))
      _check(`第 ${i} 题：answer 与 HTTP 契约同形状（含 score / feedback）`, 'score' in (ans.answer || {}) && 'feedback' in (ans.answer || {}), JSON.stringify(ans.answer ?? null).slice(0, 160))
      const sc = batch[1] || {}
      _check(`第 ${i} 题：score 事件摊平评分字段`, typeof sc.score === 'number' && 'technical_score' in sc && 'feedback' in sc, JSON.stringify(sc).slice(0, 160))
      _check(`第 ${i} 题：score.question_id 与 answer.question_id 一致`, sc.question_id === (ans.answer || {}).question_id, `${sc.question_id} vs ${(ans.answer || {}).question_id}`)
      const qevt = batch[2] || {}
      const tail = batch[batch.length - 1] || {}
      if (i < QUESTIONS) {
        _check(`第 ${i} 题：交付下一题（question_no=${i + 1}）`, qevt.question && qevt.question.question_no === i + 1, JSON.stringify(qevt.question).slice(0, 120))
        _check(
          `第 ${i} 题：★ 状态回到 avatar_speaking（数字人正在播报下一题）`,
          tail.state === 'avatar_speaking',
          String(tail.state)
        )
        from = events.length
        sock.speechDone()
        const spoken = await waitUntil('state', from)
        _check(
          `第 ${i} 题：★ 播报完毕（speech_done）⇒ 才能作答下一题（waiting_answer）`,
          spoken[0].state === 'waiting_answer',
          String(spoken[0].state)
        )
      } else {
        _check(`第 ${i} 题：全部答完 ⇒ question=null + all_answered=true`, qevt.question === null && qevt.all_answered === true, `q=${qevt.question} all=${qevt.all_answered}`)
        _check(`第 ${i} 题：★ 全部答完 ⇒ 终态 finished（不再回到播报）`, tail.state === 'finished', String(tail.state))
      }
    }

    // ------------------------------------------------------------
    // [7] finish：报告
    // ------------------------------------------------------------
    console.log('\n[7] finish：结束并出报告')
    from = events.length
    sock.finish()
    const fin = await waitUntil('state', from)
    _check('finish ⇒ report → state 两连推', fin.map((e) => e.type).join(',') === 'report,state', fin.map((e) => e.type).join(','))
    _check('report 事件带会话与报告', !!fin[0].session && !!fin[0].report, JSON.stringify(fin[0]).slice(0, 120))
    _check('★ 报告有总分（八维报告已生成）', fin[0].report && typeof fin[0].report.total_score === 'number', JSON.stringify(fin[0].report && fin[0].report.total_score))
    _check('★ 终态 state=finished', fin[1].state === 'finished', String(fin[1].state))
    _check(
      '编排状态文案表能翻译成中文（界面直接用）',
      !!ORCH_STATE_LABELS[fin[1].state],
      String(ORCH_STATE_LABELS[fin[1].state])
    )

    // ------------------------------------------------------------
    // [8] 收尾：snapshot 幂等 + 主动关闭不重连
    // ------------------------------------------------------------
    console.log('\n[8] 收尾')
    from = events.length
    sock.snapshot()
    const snap2 = await waitUntil('avatar', from)
    _check('终态下 snapshot 仍推三连（state/question/avatar）', snap2.map((e) => e.type).join(',') === 'state,question,avatar', snap2.map((e) => e.type).join(','))
    _check('终态快照的 state 仍是 finished（不被降级）', snap2[0].state === 'finished', String(snap2[0].state))

    sock.close()
    await sleep(300)
    _check('主动关闭后不再自动重连（closedByUser）', sock.closedByUser === true)
    _check('底层连接已关闭', sock.connected === false)
    _check('无未预期的 onError（解析失败 / 重连耗尽）', errors.length === 0, errors.map((e) => e.message).join(' | '))
    _check('收到正常关闭码 1000', closes.some((c) => c[0] === 1000), JSON.stringify(closes))

    return _FAILED === 0
  } finally {
    try {
      const fs = await import('node:fs')
      fs.rmSync(tmp, { recursive: true, force: true })
    } catch (e) {
      console.log(`  [WARN] 临时目录未清理（不影响结果）：${tmp} -> ${e.message}`)
    }
  }
}

main()
  .then((ok) => {
    console.log('\n' + '='.repeat(68))
    console.log(`结果: 通过 ${_PASSED} 项，失败 ${_FAILED} 项`)
    console.log('='.repeat(68))
    process.exit(ok ? 0 : 1)
  })
  .catch((e) => {
    console.log('\n' + '='.repeat(68))
    console.log(`结果: 通过 ${_PASSED} 项，失败 ${_FAILED} 项（运行中断）`)
    console.log('='.repeat(68))
    console.error('运行器异常：', e && e.message ? e.message : e)
    process.exit(1)
  })
