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
    <div v-if="!isChatMode" class="home-mode">
      <div class="home-content">
        <h1 class="home-title">AI 职业规划助手</h1>
        <p class="home-subtitle">输入您的技能，让我们为您匹配最适合的职业方向</p>
        <div class="home-input-wrapper">
          <el-input
            v-model="inputText"
            placeholder="请输入您的技能，多个技能用逗号分隔..."
            :disabled="isLoading"
            @keyup.enter="handleAsk"
          />
          <el-button type="primary" :disabled="isLoading" @click="handleAsk">
            {{ isLoading ? '处理中...' : '开始规划' }}
          </el-button>
        </div>
      </div>
    </div>

    <div v-else class="chat-mode">
      <div class="left-panel">
        <div class="chat-dialog" ref="chatContainerRef">
          <div
            v-for="(msg, index) in chatHistory"
            :key="index"
            :class="['bubble', msg.role]"
          >
            <template v-if="msg.role === 'assistant'">
              <div class="bubble-wrapper">
                <div class="ai-avatar">
                  <svg viewBox="0 0 24 24"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.95-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.54c-.26-.81-1-1.39-1.9-1.39h-1v-3c0-.55-.45-1-1-1H8v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.39z"/></svg>
                </div>
                <div class="bubble-content" v-html="renderMarkdown(msg.content)"></div>
              </div>
              <div v-if="isLoading && index === chatHistory.length - 1" class="ai-typing"></div>
            </template>
            <template v-else>
              <div class="bubble-content" v-html="renderMarkdown(msg.content)"></div>
            </template>
          </div>
        </div>

        <div class="input-area">
          <el-input
            v-model="inputText"
            type="textarea"
            :autosize="{ minRows: 1, maxRows: 8 }"
            :disabled="isLoading"
            placeholder="继续提问..."
            @keyup.enter="sendMessage"
          />
          <el-button type="primary" :disabled="isLoading" @click="sendMessage">
            {{ isLoading ? '发送中...' : '发送' }}
          </el-button>
        </div>
      </div>

      <div v-if="showResult" class="right-panel">
        <div class="job-cards-container">
          <el-card v-for="job in jobMatches" :key="job.job_id" class="job-card">
            <template #header>
              <div class="job-title">{{ job.job_name }}</div>
            </template>
            <div class="job-detail">
              <div class="job-info">
                <el-tag type="info">{{ job.city }}</el-tag>
                <el-tag type="info">{{ job.salary }}</el-tag>
              </div>
              <div class="job-skills">
                <span
                  v-for="(skill, idx) in job.keyword_match?.matched || []"
                  :key="'m-' + skill"
                  class="skill-tag matched"
                  :style="{ animationDelay: idx * 0.05 + 's' }"
                >
                  {{ skill }}
                </span>
                <span
                  v-for="(skill, idx) in job.keyword_match?.missing || []"
                  :key="'mi-' + skill"
                  class="skill-tag missing"
                  :style="{ animationDelay: ((job.keyword_match?.matched?.length || 0) + idx) * 0.05 + 's' }"
                >
                  {{ skill }}
                </span>
              </div>
              <div class="progress-wrapper">
                <el-progress
                  :percentage="job.keyword_match?.match_rate || 0"
                  :stroke-width="10"
                  :color="'#7B61FF'"
                />
                <span class="progress-text">
                  {{ job.keyword_match?.match_rate || 0 }}%
                </span>
              </div>
            </div>
          </el-card>
        </div>
      </div>
    </div>
  </div>
</template>

<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

html, body, #app {
  width: 100%;
  height: 100%;
  font-family: 'Inter', 'PingFang SC', -apple-system, sans-serif;
}

.app-container {
  width: 100%;
  height: 100vh;
  background: 
    linear-gradient(135deg, rgba(74, 144, 226, 0.03) 0%, rgba(123, 97, 255, 0.03) 100%),
    repeating-linear-gradient(0deg, transparent, transparent 19px, #e8e8e8 19px, #e8e8e8 20px),
    repeating-linear-gradient(90deg, transparent, transparent 19px, #e8e8e8 19px, #e8e8e8 20px),
    #FFFFFF;
  overflow: hidden;
}

.home-mode {
  width: 100%;
  height: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
  flex-direction: column;
  gap: 40px;
}

.home-title {
  font-size: 42px;
  font-weight: 700;
  background: linear-gradient(135deg, #4A90E2 0%, #7B61FF 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
  letter-spacing: -1px;
}

.home-subtitle {
  font-size: 16px;
  color: #8c8c8c;
  margin-top: 8px;
}

.home-input-wrapper {
  display: flex;
  gap: 12px;
  transition: all 0.3s ease;
  width: 100%;
  max-width: 600px;
}

.home-input-wrapper .el-input__wrapper {
  border-radius: 24px;
  box-shadow: 0 4px 20px rgba(74, 144, 226, 0.15);
  border: none;
  padding: 8px 20px;
}

.home-input-wrapper .el-input__inner {
  font-size: 16px;
}

.home-input-wrapper .el-button {
  border-radius: 24px;
  background: linear-gradient(135deg, #4A90E2 0%, #7B61FF 100%);
  border: none;
  padding: 0 28px;
  font-weight: 500;
  box-shadow: 0 4px 15px rgba(74, 144, 226, 0.3);
  transition: all 0.3s ease;
}

.home-input-wrapper .el-button:hover {
  transform: translateY(-2px);
  box-shadow: 0 6px 20px rgba(74, 144, 226, 0.4);
}

.chat-mode {
  width: 100%;
  height: 100%;
  display: flex;
  gap: 16px;
  padding: 20px;
}

.left-panel {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-width: 0;
  max-height: 100%;
}

.chat-dialog {
  flex: 1;
  overflow-y: auto;
  padding: 24px;
  border-radius: 20px;
  margin-bottom: 16px;
  background: rgba(255, 255, 255, 0.7);
  backdrop-filter: blur(10px);
  border: 1px solid rgba(74, 144, 226, 0.1);
  min-height: 200px;
}

.bubble {
  margin-bottom: 20px;
  display: flex;
  align-items: flex-start;
}

.bubble.user {
  justify-content: flex-end;
}

.bubble.assistant {
  justify-content: flex-start;
  flex-direction: column;
}

.ai-avatar {
  width: 36px;
  height: 36px;
  border-radius: 50%;
  background: linear-gradient(135deg, #4A90E2 0%, #7B61FF 100%);
  display: flex;
  align-items: center;
  justify-content: center;
  margin-right: 12px;
  flex-shrink: 0;
  box-shadow: 0 0 20px rgba(74, 144, 226, 0.5);
  animation: avatarGlow 2s ease-in-out infinite;
}

@keyframes avatarGlow {
  0%, 100% { box-shadow: 0 0 15px rgba(74, 144, 226, 0.4); }
  50% { box-shadow: 0 0 25px rgba(123, 97, 255, 0.6); }
}

.ai-avatar svg {
  width: 20px;
  height: 20px;
  fill: white;
}

.bubble-content {
  max-width: 70%;
  padding: 14px 20px;
  border-radius: 18px;
  word-wrap: break-word;
  line-height: 1.6;
}

.bubble.user .bubble-content {
  background: linear-gradient(135deg, #7B61FF 0%, #4A90E2 100%);
  color: #FFFFFF;
  border-bottom-right-radius: 4px;
}

.bubble.assistant .bubble-content {
  background: #FFFFFF;
  color: #1a1a1a;
  border: 1px solid rgba(74, 144, 226, 0.2);
  border-bottom-left-radius: 4px;
  box-shadow: 0 4px 15px rgba(0, 0, 0, 0.05);
}

.bubble.assistant .bubble-wrapper {
  display: flex;
  align-items: flex-start;
}

.ai-typing {
  height: 3px;
  width: 60px;
  background: linear-gradient(90deg, #4A90E2, #7B61FF, #4A90E2);
  background-size: 200% 100%;
  border-radius: 2px;
  margin-top: 8px;
  margin-left: 48px;
  animation: typingGlow 1.5s ease-in-out infinite;
}

@keyframes typingGlow {
  0% { background-position: 200% 0; opacity: 0.5; }
  50% { opacity: 1; }
  100% { background-position: -200% 0; opacity: 0.5; }
}

.input-area {
  display: flex;
  gap: 12px;
  align-items: flex-start;
  transition: all 0.3s ease;
  padding: 16px 20px;
  background: rgba(255, 255, 255, 0.8);
  backdrop-filter: blur(10px);
  border-top: 1px solid rgba(74, 144, 226, 0.1);
  border-radius: 20px;
  position: sticky;
  bottom: 0;
  z-index: 10;
}

.input-area .el-textarea {
  flex: 1;
}

.input-area .el-textarea .el-textarea__inner {
  border-radius: 16px;
  border: 1px solid rgba(74, 144, 226, 0.2);
  box-shadow: 0 2px 10px rgba(74, 144, 226, 0.1);
  padding: 12px 16px;
  font-size: 15px;
  transition: all 0.3s ease;
}

.input-area .el-textarea .el-textarea__inner:focus {
  border-color: #7B61FF;
  box-shadow: 0 4px 15px rgba(123, 97, 255, 0.2);
}

.input-area .el-textarea textarea {
  max-height: 200px;
  overflow-y: auto;
}

.input-area .el-button {
  border-radius: 16px;
  background: linear-gradient(135deg, #4A90E2 0%, #7B61FF 100%);
  border: none;
  padding: 0 24px;
  font-weight: 500;
  box-shadow: 0 4px 15px rgba(74, 144, 226, 0.3);
  transition: all 0.3s ease;
  height: 42px;
}

.input-area .el-button:hover {
  transform: translateY(-2px);
  box-shadow: 0 6px 20px rgba(74, 144, 226, 0.4);
}

.right-panel {
  width: 400px;
  min-width: 400px;
  height: 100%;
  display: flex;
  flex-direction: column;
}

.job-cards-container {
  flex: 1;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 16px;
  padding-right: 4px;
}

.job-card {
  flex-shrink: 0;
  border-radius: 16px;
  background: rgba(255, 255, 255, 0.6);
  backdrop-filter: blur(10px);
  border: 1px solid rgba(74, 144, 226, 0.15);
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.05);
  transition: all 0.3s ease;
}

.job-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 8px 30px rgba(74, 144, 226, 0.15);
}

.job-card .el-card__header {
  border-bottom: 1px solid rgba(74, 144, 226, 0.1);
  padding: 16px 20px;
}

.job-title {
  font-size: 18px;
  font-weight: 600;
  color: #1a1a1a;
  letter-spacing: -0.5px;
}

.job-detail {
  display: flex;
  flex-direction: column;
  gap: 12px;
  padding: 16px 20px;
}

.job-info {
  display: flex;
  gap: 8px;
}

.job-info .el-tag {
  border-radius: 20px;
  padding: 0 12px;
  height: 26px;
  border: none;
  background: rgba(74, 144, 226, 0.1);
  color: #4A90E2;
}

.job-skills {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.skill-tag {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 4px 12px;
  border-radius: 20px;
  font-size: 13px;
  animation: fadeIn 0.3s ease forwards;
  opacity: 0;
  transform: translateY(5px);
}

@keyframes fadeIn {
  to { opacity: 1; transform: translateY(0); }
}

.skill-tag.matched {
  background: rgba(87, 190, 106, 0.15);
  color: #57BE6A;
}

.skill-tag.matched::before {
  content: '✓';
  font-size: 11px;
}

.skill-tag.missing {
  background: rgba(255, 165, 0, 0.15);
  color: #ff9500;
}

.skill-tag.missing::before {
  content: '!';
  font-size: 11px;
  margin-right: 2px;
}

.progress-wrapper {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-top: 8px;
}

.progress-wrapper .el-progress {
  flex: 1;
}

.progress-wrapper .el-progress-bar__outer {
  border-radius: 10px;
  background: rgba(74, 144, 226, 0.1);
}

.progress-wrapper .el-progress-bar__inner {
  border-radius: 10px;
  background: linear-gradient(90deg, #4A90E2 0%, #7B61FF 100%);
}

.progress-text {
  font-weight: 600;
  background: linear-gradient(135deg, #4A90E2 0%, #7B61FF 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
  min-width: 45px;
}
</style>