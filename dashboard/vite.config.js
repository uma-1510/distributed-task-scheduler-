import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Dev server on 5173 (Vite's default) talks to the coordinator on :8000 —
// see coordinator/main.py's CORS middleware for the other half of that.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
  },
})
