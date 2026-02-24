import path from 'path'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react-swc'
import tailwindcss from '@tailwindcss/vite'
import { tanstackRouter } from '@tanstack/router-plugin/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    tanstackRouter({
      target: 'react',
      autoCodeSplitting: true,
    }),
    react(),
    tailwindcss(),
  ],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  build: {
    rollupOptions: {
      output: {
        manualChunks: {
          // TanStack libraries
          'tanstack-query': ['@tanstack/react-query'],
          'tanstack-router': ['@tanstack/react-router'],
          'tanstack-table': ['@tanstack/react-table'],
          
          // Visualization libraries
          'charts': ['recharts'],
          'flow-diagram': ['@xyflow/react'],
          
          // UI libraries
          'radix-ui': [
            '@radix-ui/react-accordion',
            '@radix-ui/react-alert-dialog',
            '@radix-ui/react-avatar',
            '@radix-ui/react-checkbox',
            '@radix-ui/react-collapsible',
            '@radix-ui/react-dialog',
            '@radix-ui/react-dropdown-menu',
            '@radix-ui/react-label',
            '@radix-ui/react-popover',
            '@radix-ui/react-radio-group',
            '@radix-ui/react-scroll-area',
            '@radix-ui/react-select',
            '@radix-ui/react-separator',
            '@radix-ui/react-slot',
            '@radix-ui/react-switch',
            '@radix-ui/react-tabs',
            '@radix-ui/react-tooltip',
          ],
          
          // Form libraries
          'forms': ['react-hook-form', '@hookform/resolvers', 'zod'],
          
          // Date utilities
          'date-utils': ['date-fns', 'react-day-picker'],
          
          // Code editor
          'code-editor': ['ace-builds', 'react-ace'],
          
          // Authentication
          'auth': ['@clerk/clerk-react'],
          
          // Icons
          'icons': ['lucide-react', '@radix-ui/react-icons'],
          
          // Other utilities
          'utils': [
            'axios',
            'clsx',
            'tailwind-merge',
            'class-variance-authority',
            'sonner',
            'zustand',
          ],
        },
      },
    },
    // Increase chunk size warning limit to 1000 kB
    chunkSizeWarningLimit: 1000,
  },
})
