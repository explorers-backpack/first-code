import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// https://vite.dev/config/
export default defineConfig({
  plugins: [vue()],
  server: {
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:5000',
        changeOrigin: true,
        // ★ 必须显式打开：WebSocket 升级请求（Upgrade: websocket）走的是
        //   http server 的 'upgrade' 事件，vite 只对 ws:true 的代理项注册该处理器。
        //   不打开的话 `/api/interview/{id}/ws` 会在开发环境被 dev server 直接丢弃，
        //   表现为「连接立刻失败」，而 HTTP 接口一切正常——极难排查。
        ws: true,
      },
    },
  },
})
