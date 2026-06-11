<script setup>
import { ref, nextTick, computed, watch, onMounted } from 'vue'
import { ElMessage, ElCollapse, ElCollapseItem } from 'element-plus'
import MarkdownIt from 'markdown-it'
import AuthModal from './components/AuthModal.vue'

const md = new MarkdownIt()

const inputText = ref('')
const isLoading = ref(false)
const jobMatches = ref([])
const chatHistory = ref([])
const chatContainerRef = ref(null)
const hasResult = ref(false)
const userBadges = ref([])
const expandedTasks = ref([])

// Auth state
const isLoggedIn = ref(false)
const currentUser = ref({ email: '', role: 'user' })
const showAuthModal = ref(false)
const currentPage = ref('home')

const apiBase = 'http://localhost:5000/api'

// Mock beat rate based on match rate
const beatRate = computed(() => {
  if (jobMatches.value.length === 0) return 0
  const avgMatch = jobMatches.value.reduce((sum, j) => sum + (j.keyword_match?.match_rate || 0), 0) / jobMatches.value.length
  return Math.round(avgMatch + 15)
})

// Parse AI response for badges and tasks
const parseAIResponse = (content) => {
  const badges = []
  const tasks = []

  // Extract potential badges from content
  const badgeKeywords = ['潜力', '规范', '极客', '全栈', '专家', '精英', '领袖', '创新']
  badgeKeywords.forEach(keyword => {
    if (content.includes(keyword)) {
      badges.push({ label: keyword, type: 'gold' })
    }
  })

  if (badges.length === 0) {
    badges.push({ label: '技术人才', type: 'gold' }, { label: '工程规范', type: 'silver' })
  }

  // Parse P0/P1/P2 tasks
  const p0Match = content.match(/P0[:：]\s*([^P\n]+)/i)
  const p1Match = content.match(/P1[:：]\s*([^P\n]+)/i)
  const p2Match = content.match(/P2[:：]\s*([^。\n]+)/gi)

  if (p0Match) tasks.push({ priority: 'P0', label: p0Match[1].trim(), done: false })
  if (p1Match) tasks.push({ priority: 'P1', label: p1Match[1].trim(), done: false })
  if (p2Match) {
    p2Match.forEach(m => {
      const label = m.replace(/P2[:：]\s*/i, '').trim()
      if (label) tasks.push({ priority: 'P2', label, done: false })
    })
  }

  if (tasks.length === 0) {
    tasks.push({ priority: 'P0', label: '夯实核心技能基础', done: false })
    tasks.push({ priority: 'P1', label: '拓展项目实战经验', done: false })
    tasks.push({ priority: 'P2', label: '持续关注行业动态', done: false })
  }

  return { badges, tasks }
}

// Extract highlighted tech terms
const highlightTechTerms = (text) => {
  const techTerms = ['Python', 'JavaScript', 'TypeScript', 'React', 'Vue', 'Angular', 'Node.js', 'Flask', 'Django', 'Spring', 'MySQL', 'PostgreSQL', 'MongoDB', 'Redis', 'Docker', 'Kubernetes', 'AWS', 'Azure', 'GCP', 'Git', 'Linux', 'API', 'REST', 'GraphQL', 'TensorFlow', 'PyTorch', 'Scikit-learn', 'Pandas', 'NumPy']
  let result = text
  techTerms.forEach(term => {
    if (result.includes(term)) {
      result = result.replace(new RegExp(`(${term})`, 'gi'), '<span class="tech-highlight">$1</span>')
    }
  })
  return result
}

const handleAsk = async () => {
  if (!inputText.value.trim()) {
    ElMessage.warning('请输入您掌握的技能')
    return
  }

  if (!isLoggedIn.value) {
    ElMessage.warning('请先登录后再使用分析功能')
    showAuthModal.value = true
    return
  }

  if (isLoading.value) return

  const userMsg = inputText.value.trim()
  chatHistory.value = []
  jobMatches.value = []
  hasResult.value = false
  userBadges.value = []
  expandedTasks.value = []

  chatHistory.value.push({ role: 'user', content: userMsg })
  inputText.value = ''
  isLoading.value = true

  try {
    const matchRes = await fetch(`${apiBase}/match`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ skills: userMsg.split(',').map(s => s.trim()) })
    })
    const matchData = await matchRes.json()

    const jobsData = matchData.results ? matchData.results.slice(0, 5) : []
    jobMatches.value = jobsData

    const chatRes = await fetch(`${apiBase}/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: userMsg, jobs: jobsData })
    })
    const chatData = await chatRes.json()

    const { badges, tasks } = parseAIResponse(chatData.chat_answer || '')
    userBadges.value = badges

    chatHistory.value.push({
      role: 'assistant',
      content: chatData.chat_answer || '抱歉，我未能理解您的问题'
    })

    if (chatData.top_jobs && chatData.top_jobs.length > 0) {
      jobMatches.value = chatData.top_jobs
    }

    if (jobMatches.value.length > 0 || chatData.chat_answer) {
      hasResult.value = true
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

  if (!isLoggedIn.value) {
    ElMessage.warning('请先登录后再使用分析功能')
    showAuthModal.value = true
    return
  }

  const userMsg = inputText.value.trim()
  chatHistory.value.push({ role: 'user', content: userMsg })
  inputText.value = ''
  isLoading.value = true

  try {
    const matchRes = await fetch(`${apiBase}/match`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ skills: userMsg.split(',').map(s => s.trim()) })
    })
    const matchData = await matchRes.json()

    const jobsData = matchData.results ? matchData.results.slice(0, 5) : []
    jobMatches.value = jobsData

    const chatRes = await fetch(`${apiBase}/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: userMsg, jobs: jobsData })
    })
    const chatData = await chatRes.json()

    chatHistory.value.push({
      role: 'assistant',
      content: chatData.chat_answer || '抱歉，我未能理解您的问题'
    })

    if (chatData.top_jobs && chatData.top_jobs.length > 0) {
      jobMatches.value = chatData.top_jobs
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
  return highlightTechTerms(md.render(text))
}

const resetAnalysis = () => {
  hasResult.value = false
  jobMatches.value = []
  chatHistory.value = []
  userBadges.value = []
  expandedTasks.value = []
}

const toggleTask = (idx) => {
  if (expandedTasks.value.includes(idx)) {
    expandedTasks.value = expandedTasks.value.filter(i => i !== idx)
  } else {
    expandedTasks.value.push(idx)
  }
}

const getPriorityColor = (priority) => {
  switch (priority) {
    case 'P0': return '#FF4757'
    case 'P1': return '#FFA502'
    case 'P2': return '#2ED573'
    default: return '#747D8C'
  }
}

// Auth handlers
const handleLoginSuccess = (user) => {
  currentUser.value = user
  isLoggedIn.value = true
  showAuthModal.value = false
}

const handleLogout = () => {
  currentUser.value = { email: '', role: 'user' }
  isLoggedIn.value = false
  currentPage.value = 'home'
  hasResult.value = false
  jobMatches.value = []
  chatHistory.value = []
  userBadges.value = []
  ElMessage.success('已安全退出')
}

const openAuthModal = (mode = 'login') => {
  showAuthModal.value = true
}
</script>

<template>
  <div class="app-container">
    <!-- Top Navigation Header -->
    <header class="top-header glass-card">
      <div class="header-left">
        <h1 class="logo">Career.ai</h1>
        <nav class="nav-links">
          <button
            :class="{ active: currentPage === 'home' }"
            @click="currentPage = 'home'"
          >
            职业地图
          </button>
          <button
            v-if="currentUser.role === 'admin'"
            :class="{ active: currentPage === 'admin' }"
            @click="currentPage = 'admin'"
          >
            数据管理
          </button>
        </nav>
      </div>
      <div class="header-right">
        <!-- 未登录 -->
        <button v-if="!isLoggedIn" class="auth-btn" @click="openAuthModal('login')">
          登录/注册
        </button>
        <!-- 已登录 -->
        <div v-else class="user-menu">
          <span class="user-email">{{ currentUser.email.split('@')[0] }}</span>
          <button class="logout-btn" @click="handleLogout">退出</button>
        </div>
      </div>
    </header>

    <!-- Cosmic Background -->
    <div class="cosmic-bg">
      <div class="orb orb-1"></div>
      <div class="orb orb-2"></div>
      <div class="grid-overlay"></div>
    </div>

    <!-- Admin Page -->
    <div v-if="currentPage === 'admin'" class="admin-page">
      <div class="glass-card admin-panel">
        <svg class="admin-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
          <path d="M12 15V3m0 12l-4-4m4 4l4-4M2 17l.621 2.485A2 2 0 0 0 4.561 21h14.878a2 2 0 0 0 1.94-1.515L22 17"/>
        </svg>
        <h2>数据管理页面</h2>
        <p>暂无内容，开发中...</p>
      </div>
    </div>

    <!-- Search Center (Initial State) -->
    <div v-else-if="!hasResult" class="search-center">
      <div class="search-card glass-card">
        <div class="brand-mark">
          <svg viewBox="0 0 60 60" class="logo-icon">
            <circle cx="30" cy="30" r="28" fill="none" stroke="currentColor" stroke-width="1.5"/>
            <path d="M30 10 L30 50 M15 25 L45 25 M15 35 L45 35" stroke="currentColor" stroke-width="1.5" fill="none"/>
          </svg>
          <span class="brand-text">Career.ai</span>
        </div>

        <h1 class="search-title">职业智能规划系统</h1>
        <p class="search-subtitle">输入您的技能，开启职业发展诊断</p>

        <div class="search-input-group">
          <div class="search-input-wrapper">
            <svg class="search-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <circle cx="11" cy="11" r="8"/>
              <path d="M21 21l-4.35-4.35"/>
            </svg>
            <input
              v-model="inputText"
              type="text"
              class="search-input"
              placeholder="例如：Python, Vue, MySQL, 机器学习"
              :disabled="isLoading"
              @keyup.enter="handleAsk"
            />
          </div>
          <button class="search-btn" :class="{ loading: isLoading }" :disabled="isLoading" @click="handleAsk">
            <span>开始诊断</span>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M5 12h14M12 5l7 7-7 7"/>
            </svg>
          </button>
        </div>

        <div class="quick-suggestions">
          <button class="suggestion-pill" @click="isLoggedIn ? inputText = 'Python, 机器学习, 数据分析' : (ElMessage.warning('请先登录'), showAuthModal = true)">Python + ML</button>
          <button class="suggestion-pill" @click="isLoggedIn ? inputText = 'React, TypeScript, 前端' : (ElMessage.warning('请先登录'), showAuthModal = true)">React 前端</button>
          <button class="suggestion-pill" @click="isLoggedIn ? inputText = 'Java, Spring, 分布式' : (ElMessage.warning('请先登录'), showAuthModal = true)">Java 后端</button>
        </div>

        <div v-if="isLoading" class="loading-indicator">
          <div class="loading-dots">
            <span></span><span></span><span></span>
          </div>
          <p>AI 正在分析您的职业竞争力...</p>
        </div>
      </div>
    </div>

    <!-- Dashboard Layout (Results State) -->
    <div v-else class="dashboard-layout">
      <!-- Header -->
      <header class="dashboard-header">
        <div class="header-left">
          <button class="reset-btn" @click="resetAnalysis">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M19 12H5M12 19l-7-7 7-7"/>
            </svg>
            <span>重新分析</span>
          </button>
        </div>
        <div class="header-center">
          <h2>职业竞争力诊断报告</h2>
        </div>
        <div class="header-right">
          <span class="timestamp">诊断时间：{{ new Date().toLocaleTimeString() }}</span>
        </div>
      </header>

      <div class="dashboard-content">
        <!-- Left Panel: Personal Competitiveness (25%) -->
        <aside class="left-panel glass-card">
          <div class="panel-section">
            <h3 class="section-title">市场击败率</h3>
            <div class="beat-rate-gauge">
              <svg viewBox="0 0 120 120" class="gauge-svg">
                <circle cx="60" cy="60" r="50" class="gauge-bg"/>
                <circle
                  cx="60" cy="60" r="50"
                  class="gauge-fill"
                  :stroke-dasharray="beatRate * 3.14 + ' 314'"
                />
              </svg>
              <div class="gauge-value">
                <span class="gauge-number">{{ beatRate }}</span>
                <span class="gauge-percent">%</span>
              </div>
              <p class="gauge-label">击败同龄人</p>
            </div>
          </div>

          <div class="panel-section">
            <h3 class="section-title">AI 勋章墙</h3>
            <div class="badges-wall">
              <div
                v-for="(badge, idx) in userBadges"
                :key="idx"
                class="badge-card"
                :class="badge.type"
              >
                <svg class="badge-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                  <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/>
                </svg>
                <span>{{ badge.label }}</span>
              </div>
            </div>
          </div>
        </aside>

        <!-- Center Panel: AI Diagnostic Report (45%) -->
        <main class="center-panel">
          <div class="report-card glass-card">
            <div class="report-header">
              <h3>AI 核心诊断</h3>
              <div class="report-meta">
                <span class="meta-tag">实时分析</span>
              </div>
            </div>

            <div class="chat-messages" ref="chatContainerRef">
              <div
                v-for="(msg, index) in chatHistory"
                :key="index"
                :class="['message', msg.role]"
              >
                <div class="message-avatar">
                  <svg v-if="msg.role === 'assistant'" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                    <circle cx="12" cy="12" r="10"/>
                    <path d="M8 14s1.5 2 4 2 4-2 4-2"/>
                    <line x1="9" y1="9" x2="9.01" y2="9"/>
                    <line x1="15" y1="9" x2="15.01" y2="9"/>
                  </svg>
                  <svg v-else viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                    <circle cx="12" cy="8" r="5"/>
                    <path d="M20 21a8 8 0 1 0-16 0"/>
                  </svg>
                </div>
                <div class="message-body">
                  <div class="message-content" v-html="renderMarkdown(msg.content)"></div>
                </div>
              </div>

              <div v-if="isLoading" class="typing-indicator">
                <span></span><span></span><span></span>
              </div>
            </div>

            <!-- Task Priorities -->
            <div class="tasks-section">
              <h4>学习优先级</h4>
              <div class="tasks-list">
                <div
                  v-for="(task, idx) in [
                    { priority: 'P0', label: '夯实核心技能基础', done: false },
                    { priority: 'P1', label: '拓展项目实战经验', done: false },
                    { priority: 'P2', label: '持续关注行业动态', done: false }
                  ]"
                  :key="idx"
                  class="task-item"
                  :class="{ expanded: expandedTasks.includes(idx) }"
                  @click="toggleTask(idx)"
                >
                  <div class="task-header">
                    <span class="task-priority" :style="{ color: getPriorityColor(task.priority) }">
                      {{ task.priority }}
                    </span>
                    <span class="task-label">{{ task.label }}</span>
                    <svg class="task-arrow" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                      <path d="M6 9l6 6 6-6"/>
                    </svg>
                  </div>
                  <div v-if="expandedTasks.includes(idx)" class="task-detail">
                    <p>制定 30 天学习计划，每天投入 2 小时系统学习</p>
                    <div class="task-progress">
                      <div class="progress-bar" :style="{ width: task.done ? '100%' : '0%' }"></div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <!-- Chat Input -->
          <div class="chat-input-bar glass-card">
            <input
              v-model="inputText"
              type="text"
              class="chat-input"
              placeholder="继续提问..."
              :disabled="isLoading"
              @keyup.enter="sendMessage"
            />
            <button class="send-btn" :disabled="isLoading" @click="sendMessage">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M22 2L11 13M22 2l-7 20-4-9-9-4 20-7z"/>
              </svg>
            </button>
          </div>
        </main>

        <!-- Right Panel: Job Matrix (30%) -->
        <aside class="right-panel">
          <h3 class="panel-title">匹配岗位</h3>
          <div class="jobs-list">
            <div
              v-for="(job, idx) in jobMatches"
              :key="job.job_id"
              class="job-card glass-card"
              :style="{ animationDelay: idx * 0.1 + 's' }"
            >
              <div class="job-header">
                <h4 class="job-name">{{ job.job_name }}</h4>
                <div class="job-tags">
                  <span class="job-city">{{ job.city }}</span>
                  <span class="job-salary">{{ job.salary }}</span>
                </div>
              </div>

              <div class="job-skills">
                <span
                  v-for="skill in (job.keyword_match?.matched || []).slice(0, 4)"
                  :key="skill"
                  class="skill-tag matched"
                >{{ skill }}</span>
                <span
                  v-for="skill in (job.keyword_match?.missing || []).slice(0, 2)"
                  :key="skill"
                  class="skill-tag missing"
                >{{ skill }}</span>
              </div>

              <!-- Energy Bar -->
              <div class="energy-bar-container">
                <div
                  class="energy-bar"
                  :style="{
                    width: (job.keyword_match?.match_rate || 0) + '%',
                    background: `linear-gradient(90deg, #D4AF37, #F3E5AB)`
                  }"
                ></div>
              </div>
            </div>

            <div v-if="jobMatches.length === 0" class="no-jobs">
              <p>暂无精确匹配的岗位</p>
            </div>
          </div>
        </aside>
      </div>
    </div>

    <!-- Auth Modal -->
    <AuthModal
      v-if="showAuthModal"
      @close="showAuthModal = false"
      @loginSuccess="handleLoginSuccess"
    />
  </div>
</template>

<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;500;600;700&family=Noto+Sans+SC:wght@300;400;500&display=swap');

:root {
  --bg-deep: #faf8f5;
  --bg-darker: #f5f0e8;
  --surface: rgba(255, 255, 255, 0.8);
  --surface-hover: rgba(255, 255, 255, 0.95);
  --text-primary: #2d2a26;
  --text-secondary: #7a756d;
  --text-muted: #a39e94;
  --accent-gold: #c9a227;
  --accent-gold-light: #d4af37;
  --accent-cyan: #00a896;
  --warning: #e85d3d;
  --border-glass: rgba(201, 162, 39, 0.15);
  --border-glass-strong: rgba(201, 162, 39, 0.35);

  --font-display: 'Orbitron', 'Noto Sans SC', sans-serif;
  --font-body: 'Noto Sans SC', -apple-system, sans-serif;

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
  background: var(--bg-deep);
  color: var(--text-primary);
  -webkit-font-smoothing: antialiased;
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

/* Cosmic Background */
.cosmic-bg {
  position: fixed;
  inset: 0;
  background: #faf8f5;
  z-index: 0;
}

.grid-overlay {
  position: absolute;
  inset: 0;
  background-image:
    linear-gradient(rgba(201, 162, 39, 0.02) 1px, transparent 1px),
    linear-gradient(90deg, rgba(201, 162, 39, 0.02) 1px, transparent 1px);
  background-size: 50px 50px;
}

.stars {
  display: none;
}

.orb {
  position: absolute;
  border-radius: 50%;
  filter: blur(80px);
  opacity: 0.5;
}

.orb-1 {
  width: 600px;
  height: 600px;
  background: radial-gradient(circle, rgba(201, 162, 39, 0.2), transparent 70%);
  top: -200px;
  right: -100px;
  animation: float 20s ease-in-out infinite;
}

.orb-2 {
  width: 400px;
  height: 400px;
  background: radial-gradient(circle, rgba(201, 162, 39, 0.12), transparent 70%);
  bottom: -100px;
  left: -50px;
  animation: float 15s ease-in-out infinite reverse;
}

@keyframes float {
  0%, 100% { transform: translate(0, 0); }
  33% { transform: translate(30px, -20px); }
  66% { transform: translate(-20px, 20px); }
}

/* Glass Card */
.glass-card {
  background: rgba(255, 255, 255, 0.9);
  backdrop-filter: blur(12px);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-lg);
}

/* Search Center */
.search-center {
  position: relative;
  z-index: 1;
  width: 100%;
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 20px;
}

.search-card {
  max-width: 600px;
  width: 100%;
  padding: 48px;
  text-align: center;
  animation: fadeUp 0.8s ease-out;
}

@keyframes fadeUp {
  from { opacity: 0; transform: translateY(30px); }
  to { opacity: 1; transform: translateY(0); }
}

.brand-mark {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 12px;
  margin-bottom: 32px;
}

.logo-icon {
  width: 40px;
  height: 40px;
  color: var(--accent-gold);
}

.brand-text {
  font-family: var(--font-display);
  font-size: 18px;
  font-weight: 500;
  letter-spacing: 2px;
}

.search-title {
  font-family: var(--font-display);
  font-size: 32px;
  font-weight: 600;
  margin-bottom: 12px;
  background: linear-gradient(135deg, var(--accent-gold), var(--accent-gold-light));
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}

.search-subtitle {
  color: var(--text-secondary);
  margin-bottom: 40px;
}

.search-input-group {
  display: flex;
  gap: 12px;
  margin-bottom: 24px;
}

.search-input-wrapper {
  flex: 1;
  position: relative;
  display: flex;
  align-items: center;
}

.search-icon {
  position: absolute;
  left: 16px;
  width: 20px;
  height: 20px;
  color: var(--text-muted);
}

.search-input {
  width: 100%;
  padding: 16px 16px 16px 48px;
  font-size: 15px;
  font-family: var(--font-body);
  color: var(--text-primary);
  background: rgba(255, 255, 255, 0.9);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-md);
  outline: none;
  transition: border-color 0.3s;
}

.search-input::placeholder {
  color: var(--text-muted);
}

.search-input:focus {
  border-color: var(--accent-gold);
}

.search-btn {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 16px 24px;
  font-size: 15px;
  font-weight: 500;
  color: var(--bg-deep);
  background: linear-gradient(135deg, var(--accent-gold), var(--accent-gold-light));
  border: none;
  border-radius: var(--radius-md);
  cursor: pointer;
  transition: all 0.3s;
}

.search-btn:hover:not(:disabled) {
  transform: translateY(-2px);
  box-shadow: 0 4px 20px rgba(212, 175, 55, 0.4);
}

.search-btn svg {
  width: 18px;
  height: 18px;
}

.quick-suggestions {
  display: flex;
  justify-content: center;
  gap: 12px;
  flex-wrap: wrap;
}

.suggestion-pill {
  padding: 8px 16px;
  font-size: 13px;
  color: var(--text-secondary);
  background: transparent;
  border: 1px solid var(--border-glass);
  border-radius: 20px;
  cursor: pointer;
  transition: all 0.2s;
}

.suggestion-pill:hover {
  color: var(--accent-gold);
  border-color: var(--accent-gold);
}

.loading-indicator {
  margin-top: 32px;
}

.loading-dots {
  display: flex;
  justify-content: center;
  gap: 6px;
  margin-bottom: 12px;
}

.loading-dots span {
  width: 8px;
  height: 8px;
  background: var(--accent-gold);
  border-radius: 50%;
  animation: bounce 1.4s ease-in-out infinite;
}

.loading-dots span:nth-child(2) { animation-delay: 0.2s; }
.loading-dots span:nth-child(3) { animation-delay: 0.4s; }

@keyframes bounce {
  0%, 80%, 100% { transform: scale(0.6); opacity: 0.4; }
  40% { transform: scale(1); opacity: 1; }
}

.loading-indicator p {
  color: var(--text-muted);
  font-size: 14px;
}

/* Dashboard Layout */
.dashboard-layout {
  position: relative;
  z-index: 1;
  width: 100%;
  height: 100vh;
  display: flex;
  flex-direction: column;
  animation: spatialExpand 0.8s cubic-bezier(0.4, 0, 0.2, 1);
}

@keyframes spatialExpand {
  from { opacity: 0; transform: scale(0.95); }
  to { opacity: 1; transform: scale(1); }
}

.dashboard-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 16px 24px;
  border-bottom: 1px solid var(--border-glass);
}

.header-left, .header-right {
  flex: 1;
}

.header-right {
  text-align: right;
}

.header-center {
  text-align: center;
}

.header-center h2 {
  font-family: var(--font-display);
  font-size: 18px;
  font-weight: 500;
  letter-spacing: 2px;
}

.reset-btn {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 16px;
  font-size: 14px;
  color: var(--text-secondary);
  background: transparent;
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: all 0.2s;
}

.reset-btn:hover {
  color: var(--accent-gold);
  border-color: var(--accent-gold);
}

.reset-btn svg {
  width: 16px;
  height: 16px;
}

.timestamp {
  font-size: 12px;
  color: var(--text-muted);
}

.dashboard-content {
  flex: 1;
  display: flex;
  gap: 20px;
  padding: 20px;
  overflow: hidden;
}

/* Left Panel */
.left-panel {
  width: 25%;
  padding: 20px;
  display: flex;
  flex-direction: column;
  gap: 24px;
  overflow-y: auto;
}

.panel-section {
  animation: slideIn 0.5s ease-out both;
}

@keyframes slideIn {
  from { opacity: 0; transform: translateX(-20px); }
  to { opacity: 1; transform: translateX(0); }
}

.section-title {
  font-family: var(--font-display);
  font-size: 14px;
  font-weight: 500;
  color: var(--text-secondary);
  letter-spacing: 1px;
  margin-bottom: 16px;
}

/* Beat Rate Gauge */
.beat-rate-gauge {
  position: relative;
  width: 160px;
  margin: 0 auto;
  text-align: center;
}

.gauge-svg {
  width: 120px;
  height: 120px;
  transform: rotate(-90deg);
}

.gauge-bg {
  fill: none;
  stroke: rgba(212, 175, 55, 0.1);
  stroke-width: 8;
}

.gauge-fill {
  fill: none;
  stroke: url(#goldGradient);
  stroke-width: 8;
  stroke-linecap: round;
  transition: stroke-dasharray 1s ease-out;
}

.gauge-value {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}

.gauge-number {
  font-family: var(--font-display);
  font-size: 32px;
  font-weight: 700;
  background: linear-gradient(135deg, var(--accent-gold), var(--accent-gold-light));
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}

.gauge-percent {
  font-size: 14px;
  color: var(--accent-gold);
}

.gauge-label {
  font-size: 12px;
  color: var(--text-muted);
  margin-top: 4px;
}

/* Badges Wall */
.badges-wall {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.badge-card {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 12px;
  border-radius: var(--radius-sm);
  font-size: 12px;
  animation: glow 2s ease-in-out infinite;
}

.badge-card.gold {
  background: rgba(212, 175, 55, 0.15);
  border: 1px solid rgba(212, 175, 55, 0.4);
  color: var(--accent-gold);
}

.badge-card.silver {
  background: rgba(192, 192, 192, 0.1);
  border: 1px solid rgba(192, 192, 192, 0.3);
  color: #C0C0C0;
}

@keyframes glow {
  0%, 100% { box-shadow: 0 0 5px rgba(212, 175, 55, 0.2); }
  50% { box-shadow: 0 0 15px rgba(212, 175, 55, 0.4); }
}

.badge-icon {
  width: 14px;
  height: 14px;
}

/* Center Panel */
.center-panel {
  width: 45%;
  display: flex;
  flex-direction: column;
  gap: 16px;
  overflow: hidden;
}

.report-card {
  flex: 1;
  display: flex;
  flex-direction: column;
  padding: 20px;
  overflow: hidden;
}

.report-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.report-header h3 {
  font-family: var(--font-display);
  font-size: 16px;
  font-weight: 500;
}

.meta-tag {
  font-size: 11px;
  padding: 4px 8px;
  background: rgba(0, 210, 160, 0.15);
  color: var(--accent-cyan);
  border-radius: 4px;
}

/* Chat Messages */
.chat-messages {
  flex: 1;
  overflow-y: auto;
  padding-right: 8px;
  margin-bottom: 16px;
}

.message {
  display: flex;
  gap: 12px;
  margin-bottom: 16px;
}

.message.user {
  flex-direction: row-reverse;
}

.message-avatar {
  width: 32px;
  height: 32px;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}

.message.assistant .message-avatar {
  background: rgba(201, 162, 39, 0.2);
  color: var(--accent-gold);
}

.message.user .message-avatar {
  background: rgba(0, 168, 150, 0.2);
  color: var(--accent-cyan);
}

.message-avatar svg {
  width: 18px;
  height: 18px;
}

.message-body {
  max-width: 85%;
}

.message-content {
  padding: 12px 16px;
  border-radius: var(--radius-md);
  font-size: 14px;
  line-height: 1.7;
}

.message.assistant .message-content {
  background: rgba(255, 255, 255, 0.95);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-md) var(--radius-md) var(--radius-md) 4px;
}

.message.user .message-content {
  background: linear-gradient(135deg, rgba(201, 162, 39, 0.15), rgba(201, 162, 39, 0.08));
  border: 1px solid rgba(201, 162, 39, 0.3);
  border-radius: var(--radius-md) var(--radius-md) 4px var(--radius-md);
  color: var(--text-primary);
}

/* Tech Highlight */
.message-content :deep(.tech-highlight) {
  color: var(--accent-gold);
  font-weight: 500;
  padding: 0 2px;
}

.message-content :deep(h1),
.message-content :deep(h2),
.message-content :deep(h3) {
  font-family: var(--font-display);
  margin: 12px 0 8px;
}

.message-content :deep(p) {
  margin: 8px 0;
}

.message-content :deep(ul),
.message-content :deep(ol) {
  margin: 8px 0;
  padding-left: 20px;
}

.message-content :deep(strong) {
  color: var(--accent-gold);
}

.message-content :deep(code) {
  padding: 2px 6px;
  background: rgba(212, 175, 55, 0.1);
  border-radius: 4px;
  font-family: 'SF Mono', monospace;
  font-size: 13px;
}

/* Typing Indicator */
.typing-indicator {
  display: flex;
  gap: 4px;
  padding: 8px 0;
}

.typing-indicator span {
  width: 6px;
  height: 6px;
  background: var(--accent-gold);
  border-radius: 50%;
  animation: typing 1.4s ease-in-out infinite;
}

.typing-indicator span:nth-child(2) { animation-delay: 0.2s; }
.typing-indicator span:nth-child(3) { animation-delay: 0.4s; }

@keyframes typing {
  0%, 60%, 100% { transform: translateY(0); opacity: 0.4; }
  30% { transform: translateY(-6px); opacity: 1; }
}

/* Tasks Section */
.tasks-section {
  border-top: 1px solid var(--border-glass);
  padding-top: 16px;
}

.tasks-section h4 {
  font-family: var(--font-display);
  font-size: 14px;
  color: var(--text-secondary);
  margin-bottom: 12px;
}

.tasks-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.task-item {
  background: rgba(255, 255, 255, 0.9);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: all 0.2s;
  overflow: hidden;
}

.task-item:hover {
  border-color: var(--border-glass-strong);
}

.task-item.expanded {
  border-color: var(--accent-gold);
}

.task-header {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px;
}

.task-priority {
  font-family: var(--font-display);
  font-size: 12px;
  font-weight: 700;
  min-width: 32px;
}

.task-label {
  flex: 1;
  font-size: 14px;
}

.task-arrow {
  width: 16px;
  height: 16px;
  color: var(--text-muted);
  transition: transform 0.3s;
}

.task-item.expanded .task-arrow {
  transform: rotate(180deg);
}

.task-detail {
  padding: 0 12px 12px;
  background: rgba(201, 162, 39, 0.05);
  animation: slideDown 0.3s ease-out;
}

@keyframes slideDown {
  from { opacity: 0; transform: translateY(-10px); }
  to { opacity: 1; transform: translateY(0); }
}

.task-detail p {
  font-size: 13px;
  color: var(--text-secondary);
  margin-bottom: 8px;
}

.task-progress {
  height: 4px;
  background: rgba(255, 255, 255, 0.1);
  border-radius: 2px;
  overflow: hidden;
}

.progress-bar {
  height: 100%;
  background: linear-gradient(90deg, var(--accent-gold), var(--accent-cyan));
  border-radius: 2px;
  transition: width 0.5s ease-out;
}

/* Chat Input Bar */
.chat-input-bar {
  display: flex;
  gap: 12px;
  padding: 12px 16px;
}

.chat-input {
  flex: 1;
  padding: 12px 16px;
  font-size: 14px;
  font-family: var(--font-body);
  color: var(--text-primary);
  background: rgba(255, 255, 255, 0.9);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-md);
  outline: none;
  transition: border-color 0.2s;
}

.chat-input::placeholder {
  color: var(--text-muted);
}

.chat-input:focus {
  border-color: var(--accent-gold);
}

.send-btn {
  width: 44px;
  height: 44px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, var(--accent-gold), var(--accent-gold-light));
  border: none;
  border-radius: var(--radius-md);
  cursor: pointer;
  transition: all 0.3s;
}

.send-btn:hover:not(:disabled) {
  transform: scale(1.05);
  box-shadow: 0 0 20px rgba(212, 175, 55, 0.4);
}

.send-btn svg {
  width: 18px;
  height: 18px;
  color: var(--bg-deep);
}

.send-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

/* Right Panel */
.right-panel {
  width: 30%;
  display: flex;
  flex-direction: column;
  gap: 12px;
  overflow-y: auto;
}

.panel-title {
  font-family: var(--font-display);
  font-size: 14px;
  font-weight: 500;
  color: var(--text-secondary);
  letter-spacing: 1px;
  padding-bottom: 12px;
  border-bottom: 1px solid var(--border-glass);
}

.jobs-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.job-card {
  padding: 16px;
  cursor: pointer;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  animation: fadeIn 0.5s ease-out both;
}

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(10px); }
  to { opacity: 1; transform: translateY(0); }
}

.job-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 8px 30px rgba(201, 162, 39, 0.15);
  border-color: var(--accent-gold);
}

.job-header {
  margin-bottom: 12px;
}

.job-name {
  font-size: 15px;
  font-weight: 500;
  margin-bottom: 6px;
}

.job-tags {
  display: flex;
  gap: 8px;
}

.job-city, .job-salary {
  font-size: 12px;
  color: var(--text-muted);
}

.job-skills {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-bottom: 12px;
}

.skill-tag {
  padding: 4px 10px;
  font-size: 11px;
  border-radius: 4px;
  font-family: 'SF Mono', monospace;
}

.skill-tag.matched {
  background: rgba(0, 210, 160, 0.15);
  color: var(--accent-cyan);
  border: 1px solid rgba(0, 210, 160, 0.3);
}

.skill-tag.missing {
  background: rgba(255, 107, 74, 0.15);
  color: var(--warning);
  border: 1px solid rgba(255, 107, 74, 0.3);
  animation: breathe 2s ease-in-out infinite;
}

@keyframes breathe {
  0%, 100% { opacity: 0.7; }
  50% { opacity: 1; }
}

/* Energy Bar */
.energy-bar-container {
  height: 4px;
  background: rgba(255, 255, 255, 0.05);
  border-radius: 2px;
  overflow: hidden;
}

.energy-bar {
  height: 100%;
  border-radius: 2px;
  box-shadow: 0 0 10px rgba(212, 175, 55, 0.5);
  transition: width 1s ease-out;
}

.no-jobs {
  text-align: center;
  padding: 40px;
  color: var(--text-muted);
}

/* Scrollbar */
::-webkit-scrollbar {
  width: 4px;
}

::-webkit-scrollbar-track {
  background: transparent;
}

::-webkit-scrollbar-thumb {
  background: var(--border-glass-strong);
  border-radius: 2px;
}

::-webkit-scrollbar-thumb:hover {
  background: var(--accent-gold);
}

/* Top Navigation Header */
.top-header {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  height: 64px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 32px;
  z-index: 100;
  border-radius: 0;
  border-top: none;
  border-left: none;
  border-right: none;
}

.header-left {
  display: flex;
  align-items: center;
  gap: 32px;
  flex: 1;
}

.logo {
  font-family: var(--font-display);
  font-size: 20px;
  font-weight: 600;
  letter-spacing: 2px;
  color: var(--accent-gold);
}

.nav-links {
  display: flex;
  gap: 8px;
}

.nav-links button {
  padding: 10px 20px;
  font-size: 14px;
  font-family: var(--font-body);
  color: var(--text-secondary);
  background: transparent;
  border: none;
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: all 0.2s;
}

.nav-links button:hover {
  color: var(--text-primary);
  background: rgba(201, 162, 39, 0.1);
}

.nav-links button.active {
  color: var(--accent-gold);
  background: rgba(201, 162, 39, 0.15);
}

.header-right {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  flex: 1;
}

.auth-btn {
  padding: 10px 24px;
  font-size: 14px;
  font-weight: 500;
  font-family: var(--font-body);
  color: var(--accent-gold);
  background: transparent;
  border: 1px solid rgba(201, 162, 39, 0.4);
  border-radius: 24px;
  cursor: pointer;
  transition: all 0.3s;
}

.auth-btn:hover {
  background: rgba(201, 162, 39, 0.1);
  border-color: var(--accent-gold);
  box-shadow: 0 0 15px rgba(201, 162, 39, 0.2);
}

.user-menu {
  display: flex;
  align-items: center;
  gap: 16px;
}

.user-email {
  font-size: 14px;
  color: var(--text-secondary);
  padding: 8px 16px;
  background: rgba(201, 162, 39, 0.08);
  border-radius: 20px;
}

.logout-btn {
  padding: 8px 16px;
  font-size: 13px;
  font-family: var(--font-body);
  color: var(--text-muted);
  background: transparent;
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: all 0.2s;
}

.logout-btn:hover {
  color: var(--warning);
  border-color: var(--warning);
}

/* Admin Page */
.admin-page {
  position: relative;
  z-index: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 100vh;
  padding: 100px 20px 20px;
}

.admin-panel {
  padding: 80px 120px;
  text-align: center;
}

.admin-icon {
  width: 64px;
  height: 64px;
  color: var(--accent-gold);
  margin-bottom: 24px;
}

.admin-panel h2 {
  font-family: var(--font-display);
  font-size: 28px;
  font-weight: 600;
  color: var(--accent-gold);
  margin-bottom: 12px;
}

.admin-panel p {
  font-size: 16px;
  color: var(--text-muted);
}
</style>
