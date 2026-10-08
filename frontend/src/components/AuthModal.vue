<script setup>
import { ref, reactive } from 'vue'
import { ElMessage } from 'element-plus'
import { ApiError, publicApi, setToken, setStoredUser } from '../api/apiClient'

const emit = defineEmits(['close', 'loginSuccess'])

const isRegister = ref(false)
const error = ref('')
const isSubmitting = ref(false)

const form = reactive({
  email: '',
  username: '',
  password: ''
})

// 把后端 / 网络的失败原因翻成用户能看懂的一句话。
//
// ★ 为什么不再做本地校验：本组件原先在 localStorage 里存一份
//   ``{email: {username, password(明文), role}}`` 并自行比对密码，
//   比对通过就放行——**明文密码落盘**，且**后端返回被忽略**，
//   等于本地比对即可登录（后端不可用时也能进）。现已彻底移除：
//   注册/登录一律由 ``/api/auth/*`` 判定，前端只负责展示结论。
//
// 后端用 ``HTTPException(detail="…")`` 返回中文原因（「密码错误」「邮箱已被使用」等），
// apiClient 会把它原样放进 ``ApiError.detail`` ⇒ 优先展示服务端的真实判定，
// 前端**不自己编造**「该邮箱尚未注册」这类结论。
const describeAuthError = (e, fallback) => {
  if (e instanceof ApiError) {
    if (e.isNetworkError) return '无法连接服务器，请确认后端已启动后重试'
    if (typeof e.detail === 'string' && e.detail.trim()) return e.detail
  }
  return fallback
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
    const path = isRegister.value ? '/auth/register' : '/auth/login'
    const body = { email: form.email, password: form.password }
    if (isRegister.value) body.username = form.username

    const data = await publicApi.post(path, { body })

    // 没有 token 就不算登录成功——**绝不放行一个「看起来成功」的会话**。
    if (!data || !data.token) {
      error.value = '服务器未返回登录凭证，请稍后重试'
      return
    }

    setToken(data.token)
    setStoredUser(data.user || null)

    const name = (data.user && data.user.username) || form.email
    ElMessage.success(isRegister.value ? '注册成功！' : `欢迎回来，${name}！`)

    // 角色**一律以后端权威返回为准**：前端不硬编码任何角色判定，也不读本地副本。
    emit('loginSuccess', {
      email: form.email,
      role: (data.user && data.user.role) || 'user'
    })
  } catch (e) {
    error.value = describeAuthError(
      e,
      isRegister.value ? '注册失败，请稍后重试' : '登录失败，请稍后重试'
    )
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
