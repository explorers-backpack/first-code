<script setup>
import { ref, reactive, defineEmits } from 'vue'
import { ElMessage } from 'element-plus'
import { publicApi, setToken, setStoredUser } from '../api/apiClient'

const emit = defineEmits(['close', 'loginSuccess'])

const isRegister = ref(false)
const error = ref('')
const isSubmitting = ref(false)

const form = reactive({
  email: '',
  username: '',
  password: ''
})

const validateEmailLimit = (email) => {
  const stored = localStorage.getItem('registered_emails') || '{}'
  const emails = JSON.parse(stored)
  const count = emails[email] || 0
  return count < 5
}

// 向后端换取并持久化 token —— 统一请求层（apiClient）的 token 唯一来源。
// 后端未启动 / 数据库不可用 / 后端校验失败时静默降级，不改变既有本地登录流程与提示文案。
// 返回后端权威 user（含 role）；失败返回 null。**前端不硬编码任何角色判定**，
// 管理员身份完全以后端返回为准。
const syncBackendToken = async (path) => {
  try {
    const body = { email: form.email, password: form.password }
    if (isRegister.value) body.username = form.username
    const data = await publicApi.post(path, { body })
    if (data && data.token) {
      setToken(data.token)
      setStoredUser(data.user || null)
      return data.user || { email: form.email, role: 'user' }
    }
  } catch (e) {
    // 有意静默：保持原有纯本地登录体验，token 缺失时后续请求按未登录处理
  }
  return null
}

const handleSubmit = async () => {
  error.value = ''

  if (!form.email || !form.password) {
    error.value = '请填写所有必填项'
    return
  }

  if (isRegister.value && !form.username) {
    error.value = '请输入用户名'
    return
  }

  if (!form.email.includes('@')) {
    error.value = '请输入有效的邮箱地址'
    return
  }

  if (form.password.length < 6) {
    error.value = '密码长度至少为6位'
    return
  }

  isSubmitting.value = true

  try {
    if (isRegister.value) {
      // 注册逻辑
      const users = JSON.parse(localStorage.getItem('users') || '{}')

      if (users[form.email]) {
        error.value = '该邮箱已注册，请直接登录'
        isSubmitting.value = false
        return
      }

      if (!validateEmailLimit(form.email)) {
        error.value = '该邮箱注册的账号已达上限（最大5个）'
        ElMessage.error('该邮箱注册的账号已达上限（最大5个）')
        isSubmitting.value = false
        return
      }

      users[form.email] = {
        username: form.username,
        password: form.password,
        role: 'user',
        createdAt: new Date().toISOString()
      }
      localStorage.setItem('users', JSON.stringify(users))

      // 更新邮箱计数
      const emails = JSON.parse(localStorage.getItem('registered_emails') || '{}')
      emails[form.email] = (emails[form.email] || 0) + 1
      localStorage.setItem('registered_emails', JSON.stringify(emails))

      ElMessage.success('注册成功！')
      const backendUser = await syncBackendToken('/auth/register')
      // 注册角色同样以后端返回为准（后端固定为 user），前端不自行决定。
      emit('loginSuccess', { email: form.email, role: (backendUser && backendUser.role) || 'user' })
    } else {
      // 登录逻辑
      // 角色一律由后端权威返回，前端**不硬编码任何管理员账号或权限判断**。
      const users = JSON.parse(localStorage.getItem('users') || '{}')
      const user = users[form.email]

      if (user) {
        if (user.password !== form.password) {
          error.value = '密码错误，请重试'
          isSubmitting.value = false
          return
        }
        ElMessage.success(`欢迎回来，${user.username}！`)
        await syncBackendToken('/auth/login')
        emit('loginSuccess', { email: form.email, role: user.role })
        return
      }

      // 本地无此账号（例如由数据库脚本创建的管理员）：交由后端判定角色。
      const backendUser = await syncBackendToken('/auth/login')
      if (backendUser) {
        ElMessage.success(`欢迎回来，${backendUser.username || form.email}！`)
        emit('loginSuccess', { email: form.email, role: backendUser.role || 'user' })
        return
      }

      error.value = '该邮箱尚未注册'
      isSubmitting.value = false
    }
  } catch (e) {
    error.value = '操作失败，请稍后重试'
  } finally {
    isSubmitting.value = false
  }
}

const switchMode = () => {
  isRegister.value = !isRegister.value
  error.value = ''
  form.password = ''
}

const handleClose = () => {
  emit('close')
}
</script>

<template>
  <div class="modal-overlay" @click.self="handleClose">
    <div class="auth-modal glass-card">
      <button class="close-btn" @click="handleClose">✕</button>

      <div class="auth-header">
        <svg class="auth-logo" viewBox="0 0 60 60">
          <circle cx="30" cy="30" r="28" fill="none" stroke="currentColor" stroke-width="1.5"/>
          <path d="M30 10 L30 50 M15 25 L45 25 M15 35 L45 35" stroke="currentColor" stroke-width="1.5" fill="none"/>
        </svg>
        <h2>{{ isRegister ? '创建账号' : '欢迎回来' }}</h2>
        <p>{{ isRegister ? '注册新账户，开始职业规划之旅' : '登录您的账户，继续探索职业发展' }}</p>
      </div>

      <form @submit.prevent="handleSubmit" class="auth-form">
        <div class="form-group">
          <label>邮箱地址</label>
          <input
            v-model="form.email"
            type="email"
            placeholder="your@email.com"
            :disabled="isSubmitting"
          />
        </div>

        <div v-if="isRegister" class="form-group">
          <label>用户名</label>
          <input
            v-model="form.username"
            type="text"
            placeholder="输入用户名"
            :disabled="isSubmitting"
          />
        </div>

        <div class="form-group">
          <label>密码</label>
          <input
            v-model="form.password"
            type="password"
            placeholder="••••••••"
            :disabled="isSubmitting"
          />
        </div>

        <p v-if="error" class="error-msg">{{ error }}</p>

        <button type="submit" class="submit-btn" :disabled="isSubmitting">
          <span v-if="isSubmitting">处理中...</span>
          <span v-else>{{ isRegister ? '注册' : '登录' }}</span>
        </button>
      </form>

      <div class="auth-footer">
        <p>
          {{ isRegister ? '已有账号？' : '还没有账号？' }}
          <a @click="switchMode">{{ isRegister ? '立即登录' : '免费注册' }}</a>
        </p>
      </div>

      <div class="admin-hint">
        <p>管理员账号请联系系统管理员获取。</p>
      </div>
    </div>
  </div>
</template>

<style scoped>
.modal-overlay {
  position: fixed;
  inset: 0;
  background: rgba(45, 42, 38, 0.5);
  backdrop-filter: blur(8px);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
  animation: fadeIn 0.3s ease-out;
}

@keyframes fadeIn {
  from { opacity: 0; }
  to { opacity: 1; }
}

.auth-modal {
  width: 420px;
  max-width: 95vw;
  padding: 40px;
  position: relative;
  animation: slideUp 0.3s ease-out;
}

@keyframes slideUp {
  from {
    opacity: 0;
    transform: translateY(20px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.close-btn {
  position: absolute;
  top: 16px;
  right: 16px;
  width: 32px;
  height: 32px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: transparent;
  border: 1px solid rgba(201, 162, 39, 0.3);
  border-radius: 50%;
  color: #7a756d;
  cursor: pointer;
  transition: all 0.2s;
}

.close-btn:hover {
  border-color: #c9a227;
  color: #c9a227;
}

.auth-header {
  text-align: center;
  margin-bottom: 32px;
}

.auth-logo {
  width: 48px;
  height: 48px;
  color: #c9a227;
  margin-bottom: 16px;
}

.auth-header h2 {
  font-family: 'Orbitron', 'Noto Sans SC', sans-serif;
  font-size: 24px;
  font-weight: 600;
  color: #2d2a26;
  margin-bottom: 8px;
}

.auth-header p {
  font-size: 14px;
  color: #7a756d;
}

.auth-form {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.form-group label {
  font-size: 13px;
  font-weight: 500;
  color: #2d2a26;
}

.form-group input {
  padding: 14px 16px;
  font-size: 15px;
  font-family: 'Noto Sans SC', sans-serif;
  color: #2d2a26;
  background: rgba(255, 255, 255, 0.9);
  border: 1px solid rgba(201, 162, 39, 0.2);
  border-radius: 12px;
  outline: none;
  transition: all 0.2s;
}

.form-group input::placeholder {
  color: #a39e94;
}

.form-group input:focus {
  border-color: #c9a227;
  box-shadow: 0 0 0 3px rgba(201, 162, 39, 0.1);
}

.form-group input:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.error-msg {
  padding: 12px;
  font-size: 13px;
  color: #e85d3d;
  background: rgba(232, 93, 61, 0.1);
  border-radius: 8px;
  text-align: center;
}

.submit-btn {
  padding: 14px 24px;
  font-size: 15px;
  font-weight: 500;
  font-family: 'Noto Sans SC', sans-serif;
  color: #faf8f5;
  background: linear-gradient(135deg, #c9a227, #d4af37);
  border: none;
  border-radius: 12px;
  cursor: pointer;
  transition: all 0.3s;
}

.submit-btn:hover:not(:disabled) {
  transform: translateY(-2px);
  box-shadow: 0 4px 20px rgba(201, 162, 39, 0.4);
}

.submit-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.auth-footer {
  margin-top: 24px;
  text-align: center;
}

.auth-footer p {
  font-size: 14px;
  color: #7a756d;
}

.auth-footer a {
  color: #c9a227;
  font-weight: 500;
  cursor: pointer;
  text-decoration: none;
  transition: color 0.2s;
}

.auth-footer a:hover {
  color: #b8922a;
  text-decoration: underline;
}

.admin-hint {
  margin-top: 20px;
  padding-top: 16px;
  border-top: 1px solid rgba(201, 162, 39, 0.15);
  text-align: center;
}

.admin-hint p {
  font-size: 12px;
  color: #a39e94;
}
</style>
