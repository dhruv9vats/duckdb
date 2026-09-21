import { setApiClient } from '@quent/client';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App';
import { FixtureSession } from './fixture-session';
import { loadManifest } from './protocol';
import type { BrowserSession } from './session';
import { WorkerSession } from './session';
import './styles.css';

declare global {
  interface Window {
    __DUCKDB_QUENT_TEST__?: {
      snapshot: () => ReturnType<BrowserSession['snapshot']>;
    };
  }
}

async function start(): Promise<void> {
  const root = document.getElementById('root');
  if (!root) {
    throw new Error('Missing root element');
  }
  root.innerHTML = '<div class="fatal"><h1>Starting DuckDB + Quent…</h1></div>';

  const fixtureMode = new URLSearchParams(location.search).get('fixture') === '1';
  const testMode = new URLSearchParams(location.search).get('test') === '1';
  const session: BrowserSession = fixtureMode
    ? new FixtureSession()
    : await WorkerSession.create(await loadManifest());
  setApiClient(session.apiClient);

  if (fixtureMode || testMode) {
    window.__DUCKDB_QUENT_TEST__ = { snapshot: () => session.snapshot() };
  }

  createRoot(root).render(
    <StrictMode>
      <App session={session} />
    </StrictMode>,
  );
}

void start().catch(error => {
  const root = document.getElementById('root');
  if (root) {
    root.innerHTML = `<div class="fatal"><h1>DuckDB Quent failed to start</h1><pre></pre></div>`;
    const pre = root.querySelector('pre');
    if (pre) {
      pre.textContent = error instanceof Error ? error.message : String(error);
    }
  }
});
