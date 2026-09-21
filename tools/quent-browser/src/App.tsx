import { useSyncExternalStore, useState } from 'react';
import { QuentFrame } from './quent-frame';
import type { BrowserSession } from './session';

const DEFAULT_SQL = `-- Quent showcase: joins, aggregation, windows, and Top-N
WITH customers AS (
    SELECT i AS customer_id, i % 8 AS region_id, i % 5 AS segment_id
    FROM range(5000) AS t(i)
),
products AS (
    SELECT i AS product_id, i % 12 AS category_id, 100 + (i * 37) % 9900 AS price_cents
    FROM range(500) AS t(i)
),
orders AS (
    SELECT i AS order_id,
           i % 5000 AS customer_id,
           DATE '2023-01-01' + CAST(i % 730 AS INTEGER) AS order_date,
           i % 4 AS channel_id,
           i % 5 AS status_id
    FROM range(50000) AS t(i)
),
line_items AS (
    SELECT i AS line_id,
           i % 50000 AS order_id,
           (i * 13) % 500 AS product_id,
           1 + i % 7 AS quantity,
           i % 16 AS discount_pct
    FROM range(150000) AS t(i)
),
sales AS (
    SELECT c.region_id,
           c.segment_id,
           p.category_id,
           o.channel_id,
           year(o.order_date) AS sales_year,
           count(*) AS line_count,
           count(DISTINCT o.order_id) AS order_count,
           sum(l.quantity) AS units,
           sum((p.price_cents * l.quantity * (100 - l.discount_pct)) // 100) AS revenue_cents,
           round(avg(p.price_cents), 2) AS average_price_cents,
           min(p.price_cents) AS lowest_price_cents,
           max(p.price_cents) AS highest_price_cents
    FROM line_items AS l
    JOIN orders AS o ON o.order_id = l.order_id
    JOIN customers AS c ON c.customer_id = o.customer_id
    JOIN products AS p ON p.product_id = l.product_id
    WHERE o.status_id <> 4 AND l.quantity >= 2
    GROUP BY c.region_id, c.segment_id, p.category_id, o.channel_id, sales_year
),
ranked AS (
    SELECT *,
           dense_rank() OVER (
               PARTITION BY region_id, sales_year
               ORDER BY revenue_cents DESC
           ) AS revenue_rank,
           sum(revenue_cents) OVER (
               PARTITION BY region_id, sales_year
               ORDER BY revenue_cents DESC, category_id, segment_id, channel_id
               ROWS UNBOUNDED PRECEDING
           ) AS running_revenue_cents
    FROM sales
)
SELECT region_id,
       segment_id,
       category_id,
       channel_id,
       sales_year,
       line_count,
       order_count,
       units,
       revenue_cents,
       average_price_cents,
       lowest_price_cents,
       highest_price_cents,
       revenue_rank,
       running_revenue_cents
FROM ranked
WHERE revenue_rank <= 3
ORDER BY revenue_cents DESC, region_id, sales_year, category_id
LIMIT 40;`;

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
