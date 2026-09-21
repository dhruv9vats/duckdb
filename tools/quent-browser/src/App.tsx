import { useSyncExternalStore, useState } from 'react';
import { QuentFrame } from './quent-frame';
import type { BrowserSession } from './session';

const DEFAULT_SQL = `SELECT count(*) AS answer\nFROM range(1000);`;

export function App({ session }: { session: BrowserSession }) {
  const snapshot = useSyncExternalStore(
    listener => session.subscribe(listener),
    () => session.snapshot(),
  );
  const [sql, setSql] = useState(DEFAULT_SQL);
  const [actionError, setActionError] = useState<string>();
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const activeCapture = snapshot.captures.find(capture => capture.revision === snapshot.revision);

  const act = async (action: () => Promise<void>): Promise<void> => {
    setActionError(undefined);
    try {
      await action();
    } catch (error) {
      setActionError(error instanceof Error ? error.message : String(error));
    }
  };

  return (
    <main>
      <header>
        <div>
          <span className="eyebrow">Local telemetry</span>
          <h1>DuckDB × Quent</h1>
        </div>
        <div className="status" data-state={snapshot.activeRunId ? 'running' : 'idle'}>
          <span className="status-dot" />
          <span data-testid="status">{snapshot.status}</span>
        </div>
        {snapshot.fixture && <div className="fixture-badge">Fixture transport — not DuckDB execution</div>}
      </header>

      <div className="workspace" data-sidebar-open={sidebarOpen}>
        <section className="sql-panel" aria-label="SQL panel" hidden={!sidebarOpen}>
          <div className="panel-title">
            <h2>SQL</h2>
            <span>{snapshot.capabilities?.threads ? 'Threaded' : 'Single worker'}</span>
          </div>
          <textarea aria-label="SQL query" value={sql} onChange={event => setSql(event.target.value)} spellCheck={false} />
          <div className="actions">
            <button className="run-button" disabled={Boolean(snapshot.activeRunId)} onClick={() => void act(() => session.run(sql))}>
              Run
            </button>
            <button disabled={!snapshot.activeRunId} onClick={() => void act(() => session.cancel())}>
              Cancel
            </button>
            <button disabled={Boolean(snapshot.activeRunId)} onClick={() => void act(() => session.reset())}>
              Reset database
            </button>
          </div>
          {actionError && <div className="error">{actionError}</div>}

          <h3>Result</h3>
          <ResultTable result={snapshot.result} />

          <h3>Capture history</h3>
          <div className="capture-list">
            {snapshot.captures.length === 0 && <div className="empty">Run a query to capture telemetry.</div>}
            {snapshot.captures.map(capture => (
              <button
                className="capture"
                data-selected={capture.revision === snapshot.revision}
                disabled={!capture.revision || capture.state === 'evicted'}
                key={capture.captureId}
                onClick={() => session.select(capture.captureId)}
              >
                <span className={`capture-state ${capture.state}`}>{capture.state}</span>
                <code>{capture.sql.split('\n')[0]}</code>
                <span>{formatBytes(capture.bytes)}</span>
              </button>
            ))}
          </div>
        </section>

        <section className="visual-panel" aria-label="Quent visualization">
          <div className="panel-title">
            <h2>Quent</h2>
            <div className="query-selection"><span>Revision {snapshot.revision || '—'}</span><button onClick={() => setSidebarOpen(open => !open)}>{sidebarOpen ? 'Hide SQL' : 'Show SQL'}</button></div>
          </div>
          <QuentFrame
            apiClient={session.apiClient}
            revision={snapshot.revision}
            getRevision={() => session.snapshot().revision}
            capture={activeCapture}
          />
        </section>
      </div>
    </main>
  );
}

function ResultTable({ result }: { result: ReturnType<BrowserSession['snapshot']>['result'] }) {
  if (!result) {
    return <div className="empty result-empty">No result yet.</div>;
  }

  return (
    <div className="result-wrap" data-testid="query-result" data-row-count={result.rowCount}>
      <table>
        <thead><tr>{result.columns.map(column => <th key={column}>{column}</th>)}</tr></thead>
        <tbody>
          {result.rows.map((row, rowIndex) => (
            <tr key={rowIndex}>{row.map((value, columnIndex) => <td key={columnIndex}>{String(value)}</td>)}</tr>
          ))}
        </tbody>
      </table>
      {result.truncated && <div className="notice">Preview truncated at the configured row limit.</div>}
    </div>
  );
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  return `${(bytes / 1024).toFixed(1)} KiB`;
}
