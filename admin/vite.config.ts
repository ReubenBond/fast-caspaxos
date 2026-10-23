import { defineConfig } from 'vitest/config';

export default defineConfig({
  base: '/admin/',
  build: { outDir: 'dist', emptyOutDir: true },
  test: {
    environment: 'jsdom',
    include: ['src/**/*.test.{ts,tsx}'],
    setupFiles: ['src/test-setup.ts'],
    restoreMocks: true,
  },
});
