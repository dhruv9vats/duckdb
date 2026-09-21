import { expect, test } from '@playwright/test';

test('default query exercises the showcase operators and is rerunnable', async ({ page }) => {
  test.setTimeout(120_000);
  test.skip(process.env.QUENT_REAL_PRODUCER !== '1', 'real producer artifact is not enabled');

  await page.goto('?test=1');
  await expect(page.getByRole('textbox')).toContainText('Quent showcase: joins, aggregation, windows, and Top-N');

  const results: string[][][] = [];
  for (let run = 1; run <= 2; run += 1) {
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('status')).toHaveText('Telemetry ready', { timeout: 60_000 });
    await expect(page.getByTestId('query-result')).toHaveAttribute('data-row-count', '40');
    await expect(page.locator('.capture')).toHaveCount(run);

    const result = await page.evaluate(() => window.__DUCKDB_QUENT_TEST__?.snapshot().result);
    results.push(result!.rows.map(row => row.map(String)));
  }
  expect(results[0][0].slice(0, 4)).toEqual(['3', '3', '7', '3']);
  expect(results[0][0][8]).toBe('13269938');
  expect(results[1]).toEqual(results[0]);

  const quent = page.frameLocator('iframe[title="Quent"]');
  const planNodes = quent.locator('.react-flow__node');
  await expect.poll(() => planNodes.filter({ hasText: 'HASH_JOIN' }).count(), { timeout: 60_000 }).toBeGreaterThanOrEqual(3);
  await expect.poll(() => planNodes.filter({ hasText: 'HASH_GROUP_BY' }).count()).toBeGreaterThanOrEqual(1);
  await expect.poll(() => planNodes.filter({ hasText: 'WINDOW' }).count()).toBeGreaterThanOrEqual(1);
  await expect(quent.locator('.react-flow__node').filter({ hasText: 'TOP_N' })).toHaveCount(1);

  const frameBox = await page.locator('iframe[title="Quent"]').boundingBox();
  expect(frameBox).not.toBeNull();
  expect(Math.ceil(frameBox!.y + frameBox!.height)).toBeLessThanOrEqual(page.viewportSize()!.height);

  const snapshot = await page.evaluate(() => window.__DUCKDB_QUENT_TEST__?.snapshot());
  expect(snapshot?.captures).toHaveLength(2);
  expect(snapshot?.captures.every(capture => capture.state === 'sealed')).toBe(true);
  expect(snapshot?.captures.every(capture => capture.droppedEvents === 0)).toBe(true);
  expect(snapshot?.captures.every(capture => capture.bytes > 0)).toBe(true);
});

test('runs real DuckDB and publishes runtime telemetry', async ({ page }, testInfo) => {
  test.setTimeout(120_000);
  test.skip(process.env.QUENT_REAL_PRODUCER !== '1', 'real producer artifact is not enabled');

  const startedAt = performance.now();
  const apiRequests: string[] = [];
  const pageErrors: string[] = [];
  const consoleErrors: string[] = [];
  page.on('request', request => {
    if (new URL(request.url()).pathname.includes('/api/')) {
      apiRequests.push(request.url());
    }
  });
  page.on('pageerror', error => pageErrors.push(error.message));
  page.on('console', message => {
    if (message.type() === 'error') {
      consoleErrors.push(message.text());
    }
  });
  await page.goto('?test=1');
  await expect(page.getByText('Fixture transport — not DuckDB execution')).toHaveCount(0);
  await page.getByRole('textbox').fill('SELECT sum(i)::BIGINT AS answer FROM range(1000) t(i);');
  await page.getByRole('button', { name: 'Run', exact: true }).click();
  await expect(page.getByTestId('status')).toHaveText('Telemetry ready', { timeout: 60_000 });
  const telemetryReadyMs = Math.round(performance.now() - startedAt);
  await expect(page.getByTestId('query-result')).toContainText('499500');

  const snapshot = await page.evaluate(() => window.__DUCKDB_QUENT_TEST__?.snapshot());
  expect(snapshot?.result?.rowCount).toBe(1);
  expect(String(snapshot?.result?.rows[0]?.[0])).toBe('499500');
  expect(snapshot?.captures[0]?.state).toBe('sealed');
  expect(snapshot?.captures[0]?.queryIds.length).toBeGreaterThan(0);
  expect(snapshot?.captures[0]?.bytes).toBeGreaterThan(0);

  const quent = page.frameLocator('iframe[title="Quent"]');
  await expect(quent.getByRole('link', { name: 'Timeline', exact: true })).toBeVisible({ timeout: 60_000 });
  await expect(quent.locator('.react-flow__node').filter({ hasText: 'RANGE' })).toHaveCount(1);
  await quent.getByRole('link', { name: 'Timeline', exact: true }).click();
  await expect(quent.getByText('buffer-pool-memory', { exact: true })).toBeVisible();
  const bufferPoolValue = quent.getByText(/^[1-9]\d*(?:\.\d+)? (?:KiB|MiB)$/).first();
  await expect(bufferPoolValue).toBeVisible({ timeout: 30_000 });
  await page.screenshot({ path: testInfo.outputPath('real-plan.png'), fullPage: true });
  await expect(bufferPoolValue).toBeVisible({ timeout: 30_000 });
  await page.screenshot({ path: testInfo.outputPath('real-runtime-timeline.png'), fullPage: true });
  await quent.getByRole('link', { name: 'Operators', exact: true }).click();
  await expect(quent.locator('table').filter({ hasText: 'RANGE' })).toHaveCount(1);
  await quent.getByRole('link', { name: 'Entities', exact: true }).click();
  await expect(quent.locator('table').filter({ hasText: 'pipeline_task' })).toHaveCount(1);
  await expect(quent.locator('table').filter({ hasText: 'operator_invocation' })).toHaveCount(1);

  // Reloading only Quent reconnects and keeps its selected tab.
  const iframe = page.locator('iframe[title="Quent"]');
  await iframe.evaluate(element => (element as HTMLIFrameElement).contentWindow?.location.reload());
  await expect(quent.getByRole('link', { name: 'Entities', exact: true })).toHaveClass(/font-semibold/);
  await page.screenshot({ path: testInfo.outputPath('real-runtime-entities.png'), fullPage: true });
  console.log(JSON.stringify({
    browser: testInfo.project.name,
    query_to_telemetry_ms: telemetryReadyMs,
    captured_bytes: snapshot?.captures[0]?.bytes,
    query_ids: snapshot?.captures[0]?.queryIds.length,
    revision: snapshot?.revision,
  }));

  const firstRevision = snapshot?.revision;
  await page.getByRole('textbox').fill('SELECT count(*) AS answer FROM range(7);');
  await page.getByRole('button', { name: 'Run', exact: true }).click();
  await expect(page.getByTestId('status')).toHaveText('Telemetry ready', { timeout: 60_000 });
  await expect(page.getByTestId('query-result')).toContainText('7');
  const second = await page.evaluate(() => window.__DUCKDB_QUENT_TEST__?.snapshot());
  expect(second?.revision).not.toBe(firstRevision);
  expect(second?.captures).toHaveLength(2);

  await page.locator('.capture').nth(1).click();
  await expect.poll(() => page.evaluate(() => window.__DUCKDB_QUENT_TEST__?.snapshot().revision)).toBe(firstRevision);
  await expect(quent.getByRole('link', { name: 'Timeline', exact: true })).toBeVisible();
  await expect.poll(() => iframe.evaluate(element => (element as HTMLIFrameElement).contentWindow?.location.hash))
    .toContain(second?.captures[1]?.queryIds.at(-1));

  await page.getByRole('textbox').fill('SELECT * FROM definitely_missing_table;');
  await page.getByRole('button', { name: 'Run', exact: true }).click();
  await expect(page.getByTestId('status')).toContainText('SQL failed', { timeout: 60_000 });
  await expect.poll(() => page.evaluate(() => window.__DUCKDB_QUENT_TEST__?.snapshot().activeRunId)).toBeUndefined();

  await page.getByRole('textbox').fill('SELECT 6 * 7 AS recovered;');
  await page.getByRole('button', { name: 'Run', exact: true }).click();
  await expect(page.getByTestId('status')).toHaveText('Telemetry ready', { timeout: 60_000 });
  await expect(page.getByTestId('query-result')).toContainText('42');

  await page.getByRole('button', { name: 'Reset database' }).click();
  await expect(page.locator('.capture')).toHaveCount(0);
  await expect(page.getByTestId('status')).toHaveText('Ready');

  await page.getByRole('textbox').fill('SELECT sum(a.i * b.i) FROM range(1000000) a(i), range(1000000) b(i);');
  await page.getByRole('button', { name: 'Run', exact: true }).click();
  await page.getByRole('button', { name: 'Cancel', exact: true }).click();
  await expect(page.getByTestId('status')).toHaveText('Worker terminated; telemetry incomplete', { timeout: 10_000 });
  await expect(page.locator('.capture').first()).toContainText('incomplete');

  await page.getByRole('button', { name: 'Reset database' }).click();
  await expect(page.getByTestId('status')).toHaveText('Ready');
  await page.getByRole('textbox').fill('SELECT 6 * 7 AS recovered;');
  await page.getByRole('button', { name: 'Run', exact: true }).click();
  await expect(page.getByTestId('status')).toHaveText('Telemetry ready', { timeout: 60_000 });
  await expect(page.getByTestId('query-result')).toContainText('42');
  expect(apiRequests).toEqual([]);
  const knownWebKitResize = 'ResizeObserver loop completed with undelivered notifications.';
  const unexpectedErrors = pageErrors.filter(message => testInfo.project.name !== 'webkit' || message !== knownWebKitResize);
  expect(unexpectedErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
});
