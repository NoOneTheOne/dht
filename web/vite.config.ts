import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/search': 'http://localhost:30080',
      '/health': 'http://localhost:30080'
    }
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true
  }
})
