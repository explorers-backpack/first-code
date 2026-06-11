<script setup>
import { ref, nextTick } from 'vue'
import { ElMessage } from 'element-plus'
import MarkdownIt from 'markdown-it'

const md = new MarkdownIt()

const inputText = ref('')
const inputWidth = ref(400)
const isLoading = ref(false)
const isChatMode = ref(false)
const chatHistory = ref([])
const showResult = ref(false)
const jobMatches = ref([])
const chatContainerRef = ref(null)

const apiBase = 'http://localhost:5000/api'

const updateInputWidth = () => {
  const len = inputText.value.length
  inputWidth.value = Math.max(400, Math.min(1000, len * 20 + 100))
}

const handleAsk = async () => {
  if (!inputText.value.trim() || isLoading.value) return

  const userMsg = inputText.value.trim()
  chatHistory.value = []
  chatHistory.value.push({ role: 'user', content: userMsg })
  inputText.value = ''
  inputWidth.value = 400
  isLoading.value = true

  if (!isChatMode.value) {
    isChatMode.value = true
    await nextTick()
  }

  try {
    const matchRes = await fetch(`${apiBase}/match`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ skills: userMsg.split(',').map(s => s.trim()) })
    })
    const matchData = await matchRes.json()

    const jobsData = matchData.results ? matchData.results.slice(0, 5) : []

    if (jobsData.length > 0) {
      jobMatches.value = jobsData
      showResult.value = true
    } else {
      jobMatches.value = []
      showResult.value = false
    }

    const chatRes = await fetch(`${apiBase}/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: userMsg,
        jobs: jobsData
      })
    })
    const chatData = await chatRes.json()

    chatHistory.value.push({
      role: 'assistant',
      content: chatData.chat_answer || '抱歉，我未能理解您的问题'
    })

    if (chatData.top_jobs && chatData.top_jobs.length > 0) {
      jobMatches.value = chatData.top_jobs
      showResult.value = true
    }

    await nextTick()
    scrollToBottom()
  } catch (error) {
    ElMessage.error('请求失败，请稍后重试')
    chatHistory.value.push({
      role: 'assistant',
      content: '抱歉，服务暂时不可用，请稍后重试。'
    })
  } finally {
    isLoading.value = false
  }
}

const sendMessage = async () => {
  if (!inputText.value.trim() || isLoading.value) return

  const userMsg = inputText.value.trim()
  chatHistory.value.push({ role: 'user', content: userMsg })
  inputText.value = ''
  inputWidth.value = 400
  isLoading.value = true

  try {
    const matchRes = await fetch(`${apiBase}/match`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ skills: userMsg.split(',').map(s => s.trim()) })
    })
    const matchData = await matchRes.json()

    const jobsData = matchData.results ? matchData.results.slice(0, 5) : []

    if (jobsData.length > 0) {
      jobMatches.value = jobsData
      showResult.value = true
    }

    const chatRes = await fetch(`${apiBase}/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: userMsg,
        jobs: jobsData
      })
    })
    const chatData = await chatRes.json()

    chatHistory.value.push({
      role: 'assistant',
      content: chatData.chat_answer || '抱歉，我未能理解您的问题'
    })

    if (chatData.top_jobs && chatData.top_jobs.length > 0) {
      jobMatches.value = chatData.top_jobs
      showResult.value = true
    }

    await nextTick()
    scrollToBottom()
  } catch (error) {
    ElMessage.error('请求失败，请稍后重试')
    chatHistory.value.push({
      role: 'assistant',
      content: '抱歉，服务暂时不可用，请稍后重试。'
    })
  } finally {
    isLoading.value = false
  }
}

const scrollToBottom = async () => {
  await nextTick()
  if (chatContainerRef.value) {
    chatContainerRef.value.scrollTop = chatContainerRef.value.scrollHeight
  }
}

const renderMarkdown = (text) => {
  return md.render(text)
}
</script>

<template>
  <div class="app-container">
    <!-- Ambient Background -->
    <div class="ambient-bg">
      <div class="orb orb-1"></div>
      <div class="orb orb-2"></div>
      <div class="grain"></div>
    </div>

    <!-- Home Mode -->
    <div v-if="!isChatMode" class="home-mode">
      <div class="home-content">
        <div class="brand-mark">
          <svg viewBox="0 0 60 60" class="logo-icon">
            <circle cx="30" cy="30" r="28" fill="none" stroke="currentColor" stroke-width="1.5"/>
            <path d="M30 10 L30 50 M15 25 L45 25 M15 35 L45 35" stroke="currentColor" stroke-width="1.5" fill="none"/>
          </svg>
          <span class="brand-text">Career.ai</span>
        </div>

        <h1 class="home-title">
          <span class="title-line">职业规划</span>
          <span class="title-line accent">智能助手</span>
        </h1>

        <p class="home-subtitle">
          输入您的技能与背景，AI 将为您分析最适合的职业方向
        </p>

        <div class="input-group">
          <div class="input-wrapper">
            <input
              v-model="inputText"
              type="text"
              class="skill-input"
              placeholder="例如：Python, Flask, MySQL, Vue.js"
              :disabled="isLoading"
              @keyup.enter="handleAsk"
            />
            <div class="input-line"></div>
          </div>
          <button
            class="submit-btn"
            :class="{ loading: isLoading }"
            :disabled="isLoading"
            @click="handleAsk"
          >
            <span class="btn-text">{{ isLoading ? '分析中' : '开始规划' }}</span>
            <span class="btn-icon">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M5 12h14M12 5l7 7-7 7"/>
              </svg>
            </span>
          </button>
        </div>

        <div class="suggestions">
          <span class="suggestion-label">试试：</span>
          <button class="suggestion-chip" @click="inputText = 'Python, 机器学习, 数据分析'">Python + 机器学习</button>
          <button class="suggestion-chip" @click="inputText = 'React, TypeScript, 前端开发'">React + 前端</button>
          <button class="suggestion-chip" @click="inputText = 'Java, Spring, 分布式系统'">Java 后端</button>
        </div>
      </div>

      <footer class="home-footer">
        <span class="footer-text">由 讯飞Spark 大模型驱动</span>
      </footer>
    </div>

    <!-- Chat Mode -->
    <div v-else class="chat-mode">
      <aside class="sidebar">
        <div class="sidebar-header">
          <button class="back-btn" @click="isChatMode = false; showResult = false;">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M19 12H5M12 19l-7-7 7-7"/>
            </svg>
            <span>返回</span>
          </button>
        </div>

        <div class="sidebar-content">
          <h2 class="sidebar-title">职业匹配</h2>

          <div v-if="jobMatches.length > 0" class="job-list">
            <div
              v-for="(job, idx) in jobMatches"
              :key="job.job_id"
              class="job-item"
              :style="{ animationDelay: idx * 0.1 + 's' }"
            >
              <div class="job-item-header">
                <span class="job-item-name">{{ job.job_name }}</span>
                <span class="job-item-rate">{{ job.keyword_match?.match_rate || 0 }}%</span>
              </div>
              <div class="job-item-meta">
                <span class="job-item-city">{{ job.city }}</span>
                <span class="job-item-salary">{{ job.salary }}</span>
              </div>
              <div class="job-item-skills">
                <span
                  v-for="skill in (job.keyword_match?.matched || []).slice(0, 3)"
                  :key="skill"
                  class="skill-pill matched"
                >{{ skill }}</span>
                <span
                  v-for="skill in (job.keyword_match?.missing || []).slice(0, 2)"
                  :key="skill"
                  class="skill-pill missing"
                >{{ skill }}</span>
              </div>
            </div>
          </div>

          <div v-else class="no-jobs">
            <span>暂无匹配的岗位</span>
          </div>
        </div>
      </aside>

      <main class="chat-main">
        <div class="chat-header">
          <div class="chat-header-brand">
            <svg viewBox="0 0 60 60" class="logo-icon small">
              <circle cx="30" cy="30" r="28" fill="none" stroke="currentColor" stroke-width="1.5"/>
              <path d="M30 10 L30 50 M15 25 L45 25 M15 35 L45 35" stroke="currentColor" stroke-width="1.5" fill="none"/>
            </svg>
            <span>Career.ai</span>
          </div>
        </div>

        <div class="chat-dialog" ref="chatContainerRef">
          <div
            v-for="(msg, index) in chatHistory"
            :key="index"
            :class="['message', msg.role]"
          >
            <div v-if="msg.role === 'assistant'" class="avatar assistant">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                <circle cx="12" cy="12" r="10"/>
                <path d="M8 14s1.5 2 4 2 4-2 4-2"/>
                <line x1="9" y1="9" x2="9.01" y2="9"/>
                <line x1="15" y1="9" x2="15.01" y2="9"/>
              </svg>
            </div>
            <div v-else class="avatar user">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                <circle cx="12" cy="8" r="5"/>
                <path d="M20 21a8 8 0 1 0-16 0"/>
              </svg>
            </div>

            <div class="message-content" v-html="renderMarkdown(msg.content)"></div>

            <div v-if="isLoading && index === chatHistory.length - 1" class="typing-indicator">
              <span></span><span></span><span></span>
            </div>
          </div>
        </div>

        <div class="chat-input-area">
          <div class="input-wrapper compact">
            <input
              v-model="inputText"
              type="text"
              class="skill-input"
              placeholder="继续提问..."
              :disabled="isLoading"
              @keyup.enter="sendMessage"
            />
            <div class="input-line"></div>
          </div>
          <button
            class="send-btn"
            :class="{ loading: isLoading }"
            :disabled="isLoading"
            @click="sendMessage"
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M22 2L11 13M22 2l-7 20-4-9-9-4 20-7z"/>
            </svg>
          </button>
        </div>
      </main>
    </div>
  </div>
</template>

<style>
@import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@400;500;600&family=Noto+Sans+SC:wght@300;400;500&display=swap');

:root {
  --bg: #faf8f5;
  --bg-warm: #f5f0e8;
  --surface: #ffffff;
  --surface-hover: #faf8f5;
  --text: #2d2a26;
  --text-secondary: #7a756d;
  --text-muted: #a39e94;
  --accent: #c9a227;
  --accent-hover: #b8922a;
  --accent-soft: rgba(201, 162, 39, 0.12);
  --border: #e8e4dd;
  --border-strong: #d4cfc5;
  --shadow: 0 2px 8px rgba(45, 42, 38, 0.06);
  --shadow-lg: 0 8px 32px rgba(45, 42, 38, 0.1);

  --font-display: 'Cormorant Garamond', 'Noto Serif SC', Georgia, serif;
  --font-body: 'Noto Sans SC', -apple-system, BlinkMacSystemFont, sans-serif;

  --radius-sm: 8px;
  --radius-md: 16px;
  --radius-lg: 24px;
}

* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

html, body {
  width: 100%;
  height: 100%;
  font-family: var(--font-body);
  background: var(--bg);
  color: var(--text);
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}

#app {
  width: 100%;
  height: 100%;
}

.app-container {
  width: 100%;
  height: 100vh;
  position: relative;
  overflow: hidden;
}

/* Ambient Background */
.ambient-bg {
  position: fixed;
  inset: 0;
  pointer-events: none;
  z-index: 0;
}

.orb {
  position: absolute;
  border-radius: 50%;
  filter: blur(80px);
  opacity: 0.4;
}

.orb-1 {
  width: 600px;
  height: 600px;
  background: radial-gradient(circle, rgba(201, 162, 39, 0.15), transparent 70%);
  top: -200px;
  right: -100px;
  animation: float 20s ease-in-out infinite;
}

.orb-2 {
  width: 400px;
  height: 400px;
  background: radial-gradient(circle, rgba(201, 162, 39, 0.1), transparent 70%);
  bottom: -100px;
  left: -50px;
  animation: float 15s ease-in-out infinite reverse;
}

.grain {
  position: absolute;
  inset: 0;
  background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)'/%3E%3C/svg%3E");
  opacity: 0.03;
}

@keyframes float {
  0%, 100% { transform: translate(0, 0); }
  33% { transform: translate(30px, -20px); }
  66% { transform: translate(-20px, 20px); }
}

/* Home Mode */
.home-mode {
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  position: relative;
  z-index: 1;
  padding: 40px;
}

.home-content {
  max-width: 640px;
  width: 100%;
  text-align: center;
  animation: fadeUp 0.8s ease-out;
}

@keyframes fadeUp {
  from {
    opacity: 0;
    transform: translateY(30px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.brand-mark {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 12px;
  margin-bottom: 48px;
}

.logo-icon {
  width: 40px;
  height: 40px;
  color: var(--accent);
}

.logo-icon.small {
  width: 28px;
  height: 28px;
}

.brand-text {
  font-family: var(--font-display);
  font-size: 20px;
  font-weight: 500;
  color: var(--text);
  letter-spacing: 0.5px;
}

.home-title {
  font-family: var(--font-display);
  font-size: 64px;
  font-weight: 500;
  line-height: 1.1;
  margin-bottom: 24px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.title-line {
  display: block;
}

.title-line.accent {
  color: var(--accent);
}

.home-subtitle {
  font-size: 16px;
  color: var(--text-secondary);
  line-height: 1.6;
  margin-bottom: 48px;
}

.input-group {
  display: flex;
  gap: 12px;
  margin-bottom: 32px;
}

.input-wrapper {
  flex: 1;
  position: relative;
}

.skill-input {
  width: 100%;
  padding: 16px 0;
  font-size: 16px;
  font-family: var(--font-body);
  color: var(--text);
  background: transparent;
  border: none;
  outline: none;
}

.skill-input::placeholder {
  color: var(--text-muted);
}

.skill-input:disabled {
  opacity: 0.6;
}

.input-line {
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  height: 2px;
  background: var(--border-strong);
  transition: background 0.3s;
}

.input-wrapper:focus-within .input-line {
  background: var(--accent);
}

.input-wrapper.compact .skill-input {
  padding: 12px 0;
  font-size: 15px;
}

.submit-btn {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 16px 28px;
  font-size: 15px;
  font-family: var(--font-body);
  font-weight: 500;
  color: var(--surface);
  background: var(--text);
  border: none;
  border-radius: var(--radius-md);
  cursor: pointer;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

.submit-btn:hover:not(:disabled) {
  background: var(--accent);
  transform: translateY(-2px);
  box-shadow: var(--shadow-lg);
}

.submit-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.btn-icon {
  width: 18px;
  height: 18px;
  transition: transform 0.3s;
}

.submit-btn:hover .btn-icon {
  transform: translateX(4px);
}

.suggestions {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 12px;
  flex-wrap: wrap;
}

.suggestion-label {
  font-size: 13px;
  color: var(--text-muted);
}

.suggestion-chip {
  padding: 8px 16px;
  font-size: 13px;
  font-family: var(--font-body);
  color: var(--text-secondary);
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 20px;
  cursor: pointer;
  transition: all 0.2s;
}

.suggestion-chip:hover {
  color: var(--accent);
  border-color: var(--accent);
  background: var(--accent-soft);
}

.home-footer {
  position: absolute;
  bottom: 40px;
  left: 50%;
  transform: translateX(-50%);
}

.footer-text {
  font-size: 12px;
  color: var(--text-muted);
}

/* Chat Mode */
.chat-mode {
  width: 100%;
  height: 100%;
  display: flex;
  position: relative;
  z-index: 1;
}

/* Sidebar */
.sidebar {
  width: 320px;
  height: 100%;
  background: var(--surface);
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
}

.sidebar-header {
  padding: 20px;
  border-bottom: 1px solid var(--border);
}

.back-btn {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  font-size: 14px;
  font-family: var(--font-body);
  color: var(--text-secondary);
  background: transparent;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: all 0.2s;
}

.back-btn:hover {
  color: var(--text);
  border-color: var(--border-strong);
  background: var(--surface-hover);
}

.back-btn svg {
  width: 16px;
  height: 16px;
}

.sidebar-content {
  flex: 1;
  overflow-y: auto;
  padding: 20px;
}

.sidebar-title {
  font-family: var(--font-display);
  font-size: 20px;
  font-weight: 500;
  color: var(--text);
  margin-bottom: 20px;
}

.job-list {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.job-item {
  padding: 16px;
  background: var(--bg);
  border-radius: var(--radius-md);
  animation: slideIn 0.4s ease-out both;
}

@keyframes slideIn {
  from {
    opacity: 0;
    transform: translateX(-20px);
  }
  to {
    opacity: 1;
    transform: translateX(0);
  }
}

.job-item-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 8px;
}

.job-item-name {
  font-size: 14px;
  font-weight: 500;
  color: var(--text);
  flex: 1;
}

.job-item-rate {
  font-size: 14px;
  font-weight: 600;
  color: var(--accent);
}

.job-item-meta {
  display: flex;
  gap: 12px;
  margin-bottom: 12px;
}

.job-item-city,
.job-item-salary {
  font-size: 12px;
  color: var(--text-muted);
}

.job-item-skills {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.skill-pill {
  padding: 4px 10px;
  font-size: 11px;
  border-radius: 12px;
}

.skill-pill.matched {
  color: var(--accent);
  background: var(--accent-soft);
}

.skill-pill.missing {
  color: var(--text-muted);
  background: var(--bg-warm);
}

.no-jobs {
  text-align: center;
  padding: 40px 20px;
  color: var(--text-muted);
  font-size: 14px;
}

/* Chat Main */
.chat-main {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.chat-header {
  padding: 20px 28px;
  border-bottom: 1px solid var(--border);
  background: var(--surface);
}

.chat-header-brand {
  display: flex;
  align-items: center;
  gap: 10px;
  font-family: var(--font-display);
  font-size: 18px;
  font-weight: 500;
  color: var(--text);
}

.chat-dialog {
  flex: 1;
  overflow-y: auto;
  padding: 28px;
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.message {
  display: flex;
  gap: 16px;
  max-width: 720px;
  animation: messageIn 0.4s ease-out;
}

@keyframes messageIn {
  from {
    opacity: 0;
    transform: translateY(10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.message.user {
  margin-left: auto;
  flex-direction: row-reverse;
}

.avatar {
  width: 36px;
  height: 36px;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}

.avatar svg {
  width: 20px;
  height: 20px;
}

.avatar.assistant {
  background: var(--accent-soft);
  color: var(--accent);
}

.avatar.user {
  background: var(--bg-warm);
  color: var(--text-secondary);
}

.message-content {
  padding: 16px 20px;
  border-radius: var(--radius-md);
  font-size: 15px;
  line-height: 1.7;
  color: var(--text);
}

.message.assistant .message-content {
  background: var(--surface);
  border: 1px solid var(--border);
  border-top-left-radius: 4px;
}

.message.user .message-content {
  background: var(--text);
  color: var(--surface);
  border-top-right-radius: 4px;
}

/* Markdown Styles in Messages */
.message-content h1,
.message-content h2,
.message-content h3 {
  font-family: var(--font-display);
  margin: 16px 0 8px;
  color: var(--text);
}

.message-content h1 { font-size: 22px; }
.message-content h2 { font-size: 18px; }
.message-content h3 { font-size: 16px; }

.message-content p {
  margin: 8px 0;
}

.message-content ul,
.message-content ol {
  margin: 8px 0;
  padding-left: 20px;
}

.message-content li {
  margin: 4px 0;
}

.message-content code {
  padding: 2px 6px;
  background: var(--bg-warm);
  border-radius: 4px;
  font-family: 'SF Mono', Monaco, monospace;
  font-size: 13px;
}

.message.user .message-content code {
  background: rgba(255, 255, 255, 0.1);
}

.message-content strong {
  font-weight: 600;
  color: var(--accent);
}

.typing-indicator {
  display: flex;
  gap: 4px;
  padding: 8px 0;
}

.typing-indicator span {
  width: 6px;
  height: 6px;
  background: var(--text-muted);
  border-radius: 50%;
  animation: typing 1.4s ease-in-out infinite;
}

.typing-indicator span:nth-child(2) {
  animation-delay: 0.2s;
}

.typing-indicator span:nth-child(3) {
  animation-delay: 0.4s;
}

@keyframes typing {
  0%, 60%, 100% {
    transform: translateY(0);
    opacity: 0.4;
  }
  30% {
    transform: translateY(-6px);
    opacity: 1;
  }
}

/* Chat Input */
.chat-input-area {
  display: flex;
  gap: 12px;
  padding: 20px 28px;
  background: var(--surface);
  border-top: 1px solid var(--border);
}

.chat-input-area .input-wrapper {
  flex: 1;
  padding: 12px 16px;
  background: var(--bg);
  border-radius: var(--radius-md);
  border: 1px solid var(--border);
  transition: border-color 0.2s;
}

.chat-input-area .input-wrapper:focus-within {
  border-color: var(--accent);
}

.chat-input-area .input-line {
  display: none;
}

.send-btn {
  width: 48px;
  height: 48px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--text);
  border: none;
  border-radius: var(--radius-md);
  cursor: pointer;
  transition: all 0.3s;
}

.send-btn svg {
  width: 20px;
  height: 20px;
  color: var(--surface);
  transition: transform 0.3s;
}

.send-btn:hover:not(:disabled) {
  background: var(--accent);
}

.send-btn:hover .send-icon {
  transform: translateX(2px) translateY(-2px);
}

.send-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

/* Scrollbar */
::-webkit-scrollbar {
  width: 6px;
}

::-webkit-scrollbar-track {
  background: transparent;
}

::-webkit-scrollbar-thumb {
  background: var(--border-strong);
  border-radius: 3px;
}

::-webkit-scrollbar-thumb:hover {
  background: var(--text-muted);
}
</style>
