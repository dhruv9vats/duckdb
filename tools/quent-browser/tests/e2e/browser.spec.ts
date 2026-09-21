import { expect, test } from '@playwright/test';

test('runs SQL and renders plan, timelines, and entities', async ({ page }, testInfo) => {
  const graphWarnings: string[] = [];
  const apiRequests: string[] = [];
  page.on('request', request => {
    if (new URL(request.url()).pathname.includes('/api/')) {
      apiRequests.push(request.url());
    }
  });
  page.on('console', message => {
    if (/node type|attribution/i.test(message.text())) {
      graphWarnings.push(message.text());
    }
  });
  await page.goto('?fixture=1');
  await expect(page.getByText('Fixture transport — not DuckDB execution')).toBeVisible();
  const contrast = await page.getByRole('button', { name: 'Run', exact: true }).evaluate(element => {
    const style = getComputedStyle(element);
    const parse = (value: string) => value.match(/[\d.]+/g)!.slice(0, 3).map(Number);
    const luminance = (value: string) => {
      const channels = parse(value).map(channel => channel / 255).map(channel =>
        channel <= 0.03928 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4,
      );
      return 0.2126 * channels[0]! + 0.7152 * channels[1]! + 0.0722 * channels[2]!;
    };
    const foreground = luminance(style.color);
    const background = luminance(style.backgroundColor);
    return (Math.max(foreground, background) + 0.05) / (Math.min(foreground, background) + 0.05);
  });
  expect(contrast).toBeGreaterThanOrEqual(4.5);
  await page.getByRole('button', { name: 'Run', exact: true }).click();
  await expect(page.getByTestId('status')).toHaveText('Telemetry ready');
  await expect(page.getByTestId('query-result')).toContainText('1000');
  const quent = page.frameLocator('iframe[title="Quent"]');
  await expect(quent.getByRole('link', { name: 'Timeline', exact: true })).toBeVisible();
  await quent.getByRole('link', { name: 'Timeline', exact: true }).click();
  await expect(quent.getByText('Buffer manager memory', { exact: true })).toBeVisible();
  expect(graphWarnings).toEqual([]);

  await quent.getByRole('link', { name: 'Operators', exact: true }).click();
  await expect(quent.getByRole('link', { name: 'Operators', exact: true })).toHaveClass(/font-semibold/);
  await expect(quent.getByText('duration_s', { exact: true }).first()).toBeVisible();

  await quent.getByRole('link', { name: 'Entities', exact: true }).click();
  await expect(quent.getByRole('link', { name: 'Entities', exact: true })).toHaveClass(/font-semibold/);
  await expect(quent.getByText('Pipeline task', { exact: true }).first()).toBeVisible();
  await page.locator('iframe[title="Quent"]').evaluate(element => {
    (element as HTMLIFrameElement).contentWindow?.location.reload();
  });
  await expect(quent.getByRole('link', { name: 'Entities', exact: true })).toHaveClass(/font-semibold/);

  const numbers = await page.evaluate(() => {
    const snapshot = window.__DUCKDB_QUENT_TEST__?.snapshot();
    return {
      rowCount: snapshot?.result?.rowCount,
      answer: snapshot?.result?.rows[0]?.[0],
      captureBytes: snapshot?.captures[0]?.bytes,
      revision: snapshot?.revision,
    };
  });
  expect(numbers).toEqual({ rowCount: 1, answer: 1000, captureBytes: 4096, revision: '1' });
  expect(apiRequests).toEqual([]);
  await page.screenshot({ path: testInfo.outputPath('runtime-entities.png'), fullPage: true });
});

test('keeps bounded history and resets the session', async ({ page }) => {
  await page.goto('?fixture=1');
  for (let index = 0; index < 3; index++) {
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('status')).toHaveText('Telemetry ready');
  }
  await expect(page.locator('.capture')).toHaveCount(3);
  await page.locator('.capture').nth(1).click();
  await expect(page.locator('.capture').nth(1)).toHaveAttribute('data-selected', 'true');
  await page.getByRole('button', { name: 'Reset database' }).click();
  await expect(page.locator('.capture')).toHaveCount(0);
});

test('gives Quent space when SQL is hidden', async ({ page }) => {
  await page.setViewportSize({ width: 1000, height: 700 });
  await page.goto('?fixture=1');
  const iframe = page.locator('iframe[title="Quent"]');
  const before = await iframe.boundingBox();
  await page.getByRole('button', { name: 'Hide SQL', exact: true }).click();
  const after = await iframe.boundingBox();

  expect(before?.width).toBeLessThan(after?.width ?? 0);
  await expect(iframe).toBeVisible();
});
