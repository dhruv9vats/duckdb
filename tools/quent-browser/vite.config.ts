import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import { TanStackRouterVite } from '@tanstack/router-vite-plugin';
import { defineConfig, type Plugin } from 'vite';
import { readFileSync } from 'node:fs';
import path from 'node:path';

const rootDir = import.meta.dirname;

function quentLogo(): Plugin {
  return {
    name: 'quent-logo',
    buildStart() {
      this.emitFile({
        type: 'asset',
        fileName: 'iframe/logo.svg',
        source: readFileSync(path.resolve(rootDir, '.quent/ui/public/logo.svg')),
      });
    },
  };
}

export default defineConfig({
  base: './',
  define: {
    'import.meta.env.VITE_EPHEMERAL_PROFILES': JSON.stringify('true'),
  },
  plugins: [
    react(),
    TanStackRouterVite({
      routesDirectory: path.resolve(rootDir, '.quent/ui/src/routes'),
      generatedRouteTree: path.resolve(rootDir, '.quent/ui/src/routeTree.gen.ts'),
      routeFileIgnorePattern: '.test.|.spec.',
    }),
    tailwindcss(),
    quentLogo(),
  ],
  resolve: {
    dedupe: ['react', 'react-dom', 'jotai', '@tanstack/react-query', '@tanstack/react-router'],
    alias: {
      '@': path.resolve(rootDir, '.quent/ui/src'),
      '~quent/types': path.resolve(rootDir, '.quent/ui/generated/ts-bindings'),
      elkjs: 'elkjs/lib/elk.bundled.js',
    },
  },
  build: {
    target: 'es2022',
    rollupOptions: {
      input: {
        app: path.resolve(rootDir, 'index.html'),
        iframe: path.resolve(rootDir, 'iframe/index.html'),
      },
    },
  },
  server: {
    headers: {
      'Cache-Control': 'no-store',
    },
  },
});
