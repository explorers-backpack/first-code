<script setup>
/**
 * 知识库页面（RAG 检索的语料入口）
 * ============================================================
 * 对应后端 `api/knowledge.py` 的 5 条路由：
 *
 * | 动作 | 路由 | 权限 |
 * |---|---|---|
 * | 列表 | `GET  /api/knowledge/documents` | 任意登录用户 |
 * | 详情 | `GET  /api/knowledge/documents/{id}` | 任意登录用户 |
 * | 导入 | `POST /api/knowledge/documents` | **仅 admin** |
 * | 删除 | `DELETE /api/knowledge/documents/{id}` | **仅 admin** |
 * | 重建 | `POST /api/knowledge/rebuild`（可带 `purge`） | **仅 admin** |
 *
 * 约定
 * ----
 * 1. **所有请求走 `apiClient`**（唯一请求层，自动带 `Authorization`），
 *    本组件不写 `fetch`、不手写 token；
 * 2. 401 / 403 **不静默、不跳转、不清 token**，只把后端给的说明展示出来；
 * 3. 后端把「导入 / 删除 / 重建」都设计成**恒定形状的报告**（失败也返回 200），
 *    因此这里一律把报告**原样展示**，而不是把「HTTP 200」当成「操作成功」；
 * 4. `purge` 会清空**整个派生索引**，且对「索引即权威行」的后端会被后端
 *    **拒绝**（理由写在 `purge_note`）——这里如实展示，不替后端宣称成功。
 */

import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { apiClient } from '../api/apiClient'

const props = defineProps({
  /** 是否管理员：写操作（导入 / 删除 / 重建）仅 admin 可见 */
  isAdmin: { type: Boolean, default: false },
})

const API = '/knowledge'

// 与后端 models.knowledge.KNOWLEDGE_CATEGORY_LABELS 一一对应（顺序即展示顺序）
const CATEGORIES = [
  { value: 'job', label: '岗位知识' },
  { value: 'technical', label: '技术知识' },
  { value: 'company', label: '公司知识' },
  { value: 'project', label: '项目经验知识' },
]
const CATEGORY_LABELS = CATEGORIES.reduce((acc, item) => {
  acc[item.value] = item.label
  return acc
}, {})

// ---------------- 列表状态 ----------------
const documents = ref([])
const total = ref(0)
const limit = ref(10)
const offset = ref(0)
const loading = ref(false)
const filters = reactive({ category: '', keyword: '' })

const page = computed(() => Math.floor(offset.value / limit.value) + 1)
const pageCount = computed(() => Math.max(1, Math.ceil(total.value / limit.value)))
const canPrev = computed(() => offset.value > 0)
const canNext = computed(() => offset.value + limit.value < total.value)

// ---------------- 详情 ----------------
const detail = ref(null)
const detailLoading = ref(false)

// ---------------- 写操作 ----------------
const showImport = ref(false)
const importForm = reactive({ title: '', category: 'technical', source: '', content: '' })
const importing = ref(false)
const maintaining = ref(false)

/** 最近一次写操作的报告（导入 / 删除 / 重建共用一块展示区） */
const report = ref(null)

function fail(e, fallback) {
  ElMessage.error((e && e.message) || fallback)
}

function formatTime(value) {
  if (!value) return '—'
  const text = String(value).replace('T', ' ')
  return text.length >= 19 ? text.slice(0, 19) : text
}

function categoryLabel(value) {
  return CATEGORY_LABELS[value] || value || '—'
}

/** 把后端报告转成「键值行 + 备注 + 错误」的展示结构（形状恒定，故可通用渲染） */
function setReport(title, data, { notes = [], errors = [] } = {}) {
  const skip = new Set(['errors'])
  const rows = Object.entries(data || {})
    .filter(([key]) => !skip.has(key))
    .map(([key, value]) => ({ key, value: value === null || value === undefined ? '—' : String(value) }))
  report.value = { title, rows, notes, errors }
}

async function loadDocuments() {
  loading.value = true
  try {
    const data = await apiClient.get(`${API}/documents`, {
      query: {
        limit: limit.value,
        offset: offset.value,
        category: filters.category || undefined,
        keyword: filters.keyword.trim() || undefined,
      },
    })
    documents.value = Array.isArray(data.items) ? data.items : []
    total.value = Number(data.total) || 0
  } catch (e) {
    documents.value = []
    total.value = 0
    fail(e, '知识库列表加载失败')
  } finally {
    loading.value = false
  }
}

function applyFilters() {
  offset.value = 0
  loadDocuments()
}

function resetFilters() {
  filters.category = ''
  filters.keyword = ''
  applyFilters()
}

function goPrev() {
  if (!canPrev.value) return
  offset.value = Math.max(0, offset.value - limit.value)
  loadDocuments()
}

function goNext() {
  if (!canNext.value) return
  offset.value += limit.value
  loadDocuments()
}

async function openDetail(id) {
  detailLoading.value = true
  detail.value = { id, title: '', category: '', source: '', created_at: null, content: '' }
  try {
    detail.value = await apiClient.get(`${API}/documents/${id}`)
  } catch (e) {
    detail.value = null
    fail(e, '文档详情加载失败')
  } finally {
    detailLoading.value = false
  }
}

function closeDetail() {
  detail.value = null
}

async function submitImport() {
  const payload = {
    title: importForm.title.trim(),
    content: importForm.content.trim(),
    category: importForm.category,
    source: importForm.source.trim(),
  }
  // 只做「空」的前置提示，取值合法性由后端 service 判定（唯一校验口径）
  if (!payload.title || !payload.content) {
    ElMessage.warning('标题与正文都不能为空')
    return
  }
  importing.value = true
  try {
    const data = await apiClient.post(`${API}/documents`, { body: payload })
    setReport(
      data.ok ? '导入成功' : '导入未完成',
      data,
      {
        notes: [
          data.ok
            ? `切片 ${data.chunk_count} 片，已向量化 ${data.embedded_chunks} 片`
            : `停在阶段：${data.stage}${data.failed_index === null || data.failed_index === undefined ? '' : `（第 ${data.failed_index} 片）`}`,
        ],
        errors: data.errors || [],
      }
    )
    if (data.ok) {
      ElMessage.success('导入完成')
      importForm.title = ''
      importForm.content = ''
      importForm.source = ''
      showImport.value = false
      offset.value = 0
      await loadDocuments()
    } else {
      ElMessage.warning('导入未完成，详情见下方报告')
      await loadDocuments()
    }
  } catch (e) {
    fail(e, '导入失败')
  } finally {
    importing.value = false
  }
}

async function removeDocument(doc) {
  try {
    await ElMessageBox.confirm(
      `将删除《${doc.title}》及其全部切片，并同步派生索引。此操作不可撤销。`,
      '确认删除知识文档',
      { type: 'warning', confirmButtonText: '删除', cancelButtonText: '取消' }
    )
  } catch (e) {
    return // 用户取消
  }
  try {
    const data = await apiClient.delete(`${API}/documents/${doc.id}`)
    setReport('删除结果', data, {
      notes: [
        data.index_removed === null || data.index_removed === undefined
          ? '该后端的索引就是权威行本身（删行即同步），无需额外同步'
          : `已从派生索引移除 ${data.index_removed} 条`,
      ],
      errors: data.index_error ? [data.index_error] : [],
    })
    ElMessage.success(`已删除，切片 ${data.deleted_chunks} 条`)
    if (detail.value && detail.value.id === doc.id) detail.value = null
    await loadDocuments()
  } catch (e) {
    fail(e, '删除失败')
  }
}

async function runRebuild(purge) {
  if (purge) {
    try {
      await ElMessageBox.confirm(
        '「清空并重建」会先清空整个派生索引（不影响知识库正文），再按文档 id 从权威行重建。' +
          '若当前向量后端的索引就是权威行本身，后端会拒绝执行并在报告中说明。',
        '确认清空派生索引',
        { type: 'warning', confirmButtonText: '清空并重建', cancelButtonText: '取消' }
      )
    } catch (e) {
      return // 用户取消
    }
  }
  maintaining.value = true
  try {
    const data = await apiClient.post(`${API}/rebuild`, { query: purge ? { purge: true } : {} })
    const notes = []
    if (data.purge_note) notes.push(data.purge_note)
    notes.push(
      `扫描 ${data.scanned_chunks} 片，重建 ${data.rebuilt_chunks} 片，跳过 ${data.skipped_chunks} 片`
    )
    setReport(purge ? '清空并重建结果' : '重建结果', data, { notes, errors: data.errors || [] })
    if (data.errors && data.errors.length) ElMessage.warning('重建完成，但有错误，见报告')
    else ElMessage.success('重建完成')
  } catch (e) {
    fail(e, '索引重建失败')
  } finally {
    maintaining.value = false
  }
}

onMounted(loadDocuments)
</script>

<template>
  <div class="kb-page">
    <div class="kb-container glass-card">
      <!-- 头部 -->
      <div class="kb-header">
        <div class="kb-title">
          <h2>RAG 知识库</h2>
          <p>
            面试出题时按语义检索的语料。当前权限：
            <span class="role-chip" :class="isAdmin ? 'admin' : 'user'">
              {{ isAdmin ? '管理员（可导入 / 删除 / 重建）' : '只读' }}
            </span>
          </p>
        </div>
        <div class="kb-header-actions">
          <button class="ghost-btn" :disabled="loading" @click="loadDocuments">
            {{ loading ? '加载中…' : '刷新' }}
          </button>
          <template v-if="isAdmin">
            <button class="ghost-btn" :disabled="maintaining" @click="runRebuild(false)">
              重建索引
            </button>
            <button class="ghost-btn warn" :disabled="maintaining" @click="runRebuild(true)">
              清空并重建
            </button>
            <button class="primary-btn" @click="showImport = !showImport">
              {{ showImport ? '收起导入' : '导入知识' }}
            </button>
          </template>
        </div>
      </div>

      <!-- 导入表单（admin） -->
      <div v-if="isAdmin && showImport" class="kb-import">
        <div class="form-row">
          <div class="form-group grow">
            <label>标题</label>
            <input v-model="importForm.title" type="text" maxlength="300" placeholder="例如：Redis 持久化手册" />
          </div>
          <div class="form-group">
            <label>分类</label>
            <select v-model="importForm.category">
              <option v-for="item in CATEGORIES" :key="item.value" :value="item.value">
                {{ item.label }}
              </option>
            </select>
          </div>
          <div class="form-group grow">
            <label>来源（可选）</label>
            <input v-model="importForm.source" type="text" placeholder="例如：handbook://redis/persistence" />
          </div>
        </div>
        <div class="form-group">
          <label>正文（纯文本；切片与向量化由服务端完成）</label>
          <textarea v-model="importForm.content" rows="6" placeholder="粘贴已解析好的纯文本内容…"></textarea>
        </div>
        <div class="form-actions">
          <button class="primary-btn" :disabled="importing" @click="submitImport">
            {{ importing ? '导入中…' : '提交导入' }}
          </button>
        </div>
      </div>

      <!-- 筛选 -->
      <div class="kb-toolbar">
        <div class="toolbar-group">
          <label>分类</label>
          <select v-model="filters.category" @change="applyFilters">
            <option value="">全部</option>
            <option v-for="item in CATEGORIES" :key="item.value" :value="item.value">
              {{ item.label }}
            </option>
          </select>
        </div>
        <div class="toolbar-group grow">
          <label>标题关键词</label>
          <input
            v-model="filters.keyword"
            type="text"
            placeholder="按标题模糊匹配（正文全文检索走向量检索）"
            @keyup.enter="applyFilters"
          />
        </div>
        <div class="toolbar-actions">
          <button class="ghost-btn" @click="applyFilters">查询</button>
          <button class="ghost-btn" @click="resetFilters">重置</button>
        </div>
      </div>

      <!-- 列表 -->
      <div class="kb-table-wrap">
        <table class="kb-table">
          <thead>
            <tr>
              <th class="col-id">ID</th>
              <th>标题</th>
              <th class="col-cat">分类</th>
              <th class="col-source">来源</th>
              <th class="col-time">创建时间</th>
              <th class="col-ops">操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="doc in documents" :key="doc.id" :class="{ active: detail && detail.id === doc.id }">
              <td class="col-id">{{ doc.id }}</td>
              <td class="cell-title" @click="openDetail(doc.id)">{{ doc.title }}</td>
              <td class="col-cat">
                <span class="cat-tag">{{ categoryLabel(doc.category) }}</span>
              </td>
              <td class="col-source">{{ doc.source || '—' }}</td>
              <td class="col-time">{{ formatTime(doc.created_at) }}</td>
              <td class="col-ops">
                <button class="link-btn" @click="openDetail(doc.id)">查看</button>
                <button v-if="isAdmin" class="link-btn danger" @click="removeDocument(doc)">删除</button>
              </td>
            </tr>
            <tr v-if="!loading && documents.length === 0">
              <td colspan="6" class="empty-cell">
                暂无文档。{{ isAdmin ? '可用右上角「导入知识」添加语料。' : '请联系管理员导入语料。' }}
              </td>
            </tr>
            <tr v-if="loading">
              <td colspan="6" class="empty-cell">加载中…</td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- 分页 -->
      <div class="kb-pager">
        <span class="pager-info">
          共 {{ total }} 篇 · 第 {{ page }} / {{ pageCount }} 页 · 每页 {{ limit }} 条
        </span>
        <div class="pager-actions">
          <button class="ghost-btn" :disabled="!canPrev || loading" @click="goPrev">上一页</button>
          <button class="ghost-btn" :disabled="!canNext || loading" @click="goNext">下一页</button>
        </div>
      </div>

      <!-- 详情 -->
      <div v-if="detail" class="kb-detail">
        <div class="detail-header">
          <div>
            <h3>{{ detail.title }}</h3>
            <p class="detail-meta">
              #{{ detail.id }} · {{ categoryLabel(detail.category) }} ·
              来源：{{ detail.source || '—' }} · {{ formatTime(detail.created_at) }}
            </p>
          </div>
          <button class="ghost-btn" @click="closeDetail">关闭</button>
        </div>
        <pre class="detail-content">{{ detailLoading ? '加载中…' : detail.content }}</pre>
      </div>

      <!-- 最近一次操作报告 -->
      <div v-if="report" class="kb-report">
        <div class="report-header">
          <h3>{{ report.title }}</h3>
          <button class="ghost-btn" @click="report = null">清除</button>
        </div>
        <div class="report-rows">
          <span v-for="row in report.rows" :key="row.key" class="report-cell">
            <b>{{ row.key }}</b>
            <em>{{ row.value }}</em>
          </span>
        </div>
        <p v-for="(note, idx) in report.notes" :key="`n${idx}`" class="report-note">{{ note }}</p>
        <p v-for="(err, idx) in report.errors" :key="`e${idx}`" class="report-error">{{ err }}</p>
      </div>
    </div>
  </div>
</template>

<style scoped>
.kb-page {
  position: relative;
  z-index: 1;
  /* 自己当滚动容器：`.app-container` 是 height:100vh + overflow:hidden，
     不这样写「列表 + 详情 + 报告」超出视口的部分会被裁掉且无法滚动。
     同时 height:100vh 也让它盖住下面同级的首页内容（沿用 .resume-page 的做法）。 */
  height: 100vh;
  overflow-y: auto;
  display: flex;
  align-items: flex-start;
  justify-content: center;
  padding: 100px 20px 40px;
}

.kb-container {
  width: 100%;
  max-width: 1100px;
  padding: 28px 32px 32px;
  font-family: var(--font-body);
  color: var(--text-primary);
}

.kb-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  flex-wrap: wrap;
  padding-bottom: 18px;
  border-bottom: 1px solid var(--border-glass);
}

.kb-title h2 {
  margin: 0 0 6px;
  font-size: 22px;
  font-weight: 600;
}

.kb-title p {
  margin: 0;
  font-size: 13px;
  color: var(--text-secondary);
}

.role-chip {
  display: inline-block;
  margin-left: 4px;
  padding: 2px 10px;
  border-radius: 12px;
  font-size: 12px;
}

.role-chip.admin {
  background: rgba(201, 162, 39, 0.15);
  color: var(--accent-gold);
}

.role-chip.user {
  background: rgba(0, 168, 150, 0.1);
  color: var(--accent-cyan);
}

.kb-header-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.primary-btn,
.ghost-btn,
.link-btn {
  font-family: inherit;
  cursor: pointer;
  transition: all 0.2s;
}

.primary-btn {
  padding: 9px 18px;
  font-size: 14px;
  color: #fff;
  background: var(--accent-gold);
  border: 1px solid var(--accent-gold);
  border-radius: var(--radius-sm);
}

.primary-btn:hover:not(:disabled) {
  background: var(--accent-gold-light);
}

.ghost-btn {
  padding: 8px 16px;
  font-size: 14px;
  color: var(--text-secondary);
  background: rgba(255, 255, 255, 0.7);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-sm);
}

.ghost-btn:hover:not(:disabled) {
  color: var(--accent-gold);
  border-color: var(--border-glass-strong);
}

.ghost-btn.warn:hover:not(:disabled) {
  color: var(--warning);
  border-color: rgba(232, 93, 61, 0.4);
}

.primary-btn:disabled,
.ghost-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

/* ---------------- 导入表单 ---------------- */
.kb-import {
  margin-top: 18px;
  padding: 18px 20px;
  background: rgba(201, 162, 39, 0.04);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-md);
}

.form-row {
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: 6px;
  margin-bottom: 14px;
  min-width: 160px;
}

.form-group.grow {
  flex: 1;
}

.form-group label {
  font-size: 12px;
  color: var(--text-secondary);
}

.form-group input,
.form-group select,
.form-group textarea,
.toolbar-group input,
.toolbar-group select {
  padding: 9px 12px;
  font-family: inherit;
  font-size: 14px;
  color: var(--text-primary);
  background: rgba(255, 255, 255, 0.9);
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-sm);
  outline: none;
  transition: border-color 0.2s;
}

.form-group input:focus,
.form-group select:focus,
.form-group textarea:focus,
.toolbar-group input:focus,
.toolbar-group select:focus {
  border-color: var(--accent-gold);
}

.form-group textarea {
  resize: vertical;
  line-height: 1.6;
}

.form-actions {
  display: flex;
  justify-content: flex-end;
}

/* ---------------- 工具栏 ---------------- */
.kb-toolbar {
  display: flex;
  align-items: flex-end;
  gap: 16px;
  flex-wrap: wrap;
  padding: 18px 0;
}

.toolbar-group {
  display: flex;
  flex-direction: column;
  gap: 6px;
  min-width: 160px;
}

.toolbar-group.grow {
  flex: 1;
}

.toolbar-group label {
  font-size: 12px;
  color: var(--text-secondary);
}

.toolbar-actions {
  display: flex;
  gap: 8px;
}

/* ---------------- 表格 ---------------- */
.kb-table-wrap {
  border: 1px solid var(--border-glass);
  border-radius: var(--radius-md);
  overflow: hidden;
}

.kb-table {
  width: 100%;
  border-collapse: collapse;
}

.kb-table th {
  text-align: left;
  padding: 14px 16px;
  font-size: 13px;
  font-weight: 600;
  color: var(--text-secondary);
  background: rgba(201, 162, 39, 0.05);
  border-bottom: 1px solid var(--border-glass);
}

.kb-table td {
  padding: 14px 16px;
  font-size: 14px;
  border-bottom: 1px solid var(--border-glass);
  transition: background 0.2s;
}

.kb-table tr:last-child td {
  border-bottom: none;
}

.kb-table tr:hover td {
  background: linear-gradient(90deg, rgba(201, 162, 39, 0.03), rgba(201, 162, 39, 0.08));
}

.kb-table tr.active td {
  background: rgba(201, 162, 39, 0.1);
}

.col-id {
  width: 64px;
  color: var(--text-muted);
}

.col-cat,
.col-time {
  width: 120px;
}

.col-source {
  max-width: 220px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--text-secondary);
}

.col-ops {
  width: 120px;
}

.cell-title {
  cursor: pointer;
  font-weight: 500;
}

.cell-title:hover {
  color: var(--accent-gold);
}

.cat-tag {
  display: inline-block;
  padding: 3px 10px;
  font-size: 12px;
  color: var(--accent-gold);
  background: rgba(201, 162, 39, 0.12);
  border-radius: 12px;
}

.link-btn {
  padding: 0 10px 0 0;
  font-size: 13px;
  color: var(--accent-cyan);
  background: none;
  border: none;
}

.link-btn.danger {
  color: var(--warning);
}

.link-btn:hover {
  text-decoration: underline;
}

.empty-cell {
  text-align: center;
  padding: 40px 16px !important;
  color: var(--text-muted);
}

/* ---------------- 分页 ---------------- */
.kb-pager {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
  padding-top: 16px;
}

.pager-info {
  font-size: 13px;
  color: var(--text-secondary);
}

.pager-actions {
  display: flex;
  gap: 8px;
}

/* ---------------- 详情 ---------------- */
.kb-detail {
  margin-top: 20px;
  padding: 18px 20px;
  border: 1px solid var(--border-glass-strong);
  border-radius: var(--radius-md);
  background: rgba(255, 255, 255, 0.75);
}

.detail-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 12px;
}

.detail-header h3 {
  margin: 0 0 4px;
  font-size: 16px;
}

.detail-meta {
  margin: 0;
  font-size: 12px;
  color: var(--text-secondary);
}

.detail-content {
  margin: 0;
  max-height: 320px;
  overflow: auto;
  padding: 14px 16px;
  font-family: inherit;
  font-size: 13px;
  line-height: 1.7;
  white-space: pre-wrap;
  word-break: break-word;
  color: var(--text-primary);
  background: rgba(245, 240, 232, 0.6);
  border-radius: var(--radius-sm);
}

/* ---------------- 报告 ---------------- */
.kb-report {
  margin-top: 20px;
  padding: 16px 20px;
  border: 1px dashed var(--border-glass-strong);
  border-radius: var(--radius-md);
  background: rgba(0, 168, 150, 0.04);
}

.report-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 10px;
}

.report-header h3 {
  margin: 0;
  font-size: 15px;
}

.report-rows {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 20px;
}

.report-cell {
  font-size: 12px;
  color: var(--text-secondary);
}

.report-cell b {
  margin-right: 6px;
  font-weight: 500;
  color: var(--text-muted);
}

.report-cell em {
  font-style: normal;
  color: var(--text-primary);
}

.report-note {
  margin: 10px 0 0;
  font-size: 12px;
  line-height: 1.6;
  color: var(--text-secondary);
}

.report-error {
  margin: 6px 0 0;
  font-size: 12px;
  line-height: 1.6;
  color: var(--warning);
}
</style>
