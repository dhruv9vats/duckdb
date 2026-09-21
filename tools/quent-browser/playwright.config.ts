import { defineConfig, devices } from '@playwright/test';

const chromiumExecutable = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE;
const firefoxExecutable = process.env.PLAYWRIGHT_FIREFOX_EXECUTABLE;
const webkitExecutable = process.env.PLAYWRIGHT_WEBKIT_EXECUTABLE;
const port = Number(process.env.PLAYWRIGHT_PORT ?? 4173);
const basePath = process.env.QUENT_BASE_PATH ?? '/duckdb-quent/';

export default defineConfig({
  testDir: './tests/e2e',
  outputDir: 'test-results',
  fullyParallel: true,
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI ? 'github' : 'list',
  use: {
    baseURL: `http://127.0.0.1:${port}${basePath}`,
    screenshot: 'only-on-failure',
    trace: 'retain-on-failure',
  },
  projects: [
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        launchOptions: chromiumExecutable ? { executablePath: chromiumExecutable } : undefined,
      },
    },
    {
      name: 'firefox',
      use: {
        ...devices['Desktop Firefox'],
        launchOptions: firefoxExecutable ? { executablePath: firefoxExecutable } : undefined,
      },
    },
    {
      name: 'webkit',
      use: {
        ...devices['Desktop Safari'],
        launchOptions: webkitExecutable ? { executablePath: webkitExecutable } : undefined,
      },
    },
  ],
  webServer: {
    command: 'node tests/static-server.mjs',
    port,
    reuseExistingServer: !process.env.CI,
  },
});
