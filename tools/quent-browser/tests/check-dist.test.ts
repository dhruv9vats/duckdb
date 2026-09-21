import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { checkDist } from '../scripts/check-dist.mjs';

const MANIFEST_ASSETS = {
  producer_worker_url: 'workers/duckdb-browser.worker.js',
  duckdb_module_js_url: 'wasm/duckdb-browser.js',
  duckdb_wasm_url: 'wasm/duckdb-browser.wasm',
  analyzer_worker_url: 'workers/duckdb-telemetry.worker.js',
  analyzer_wasm_js_url: 'wasm/duckdb_telemetry_web.js',
  analyzer_wasm_url: 'wasm/duckdb_telemetry_web_bg.wasm',
} as const;

describe('distribution check', () => {
  it('accepts complete parent, iframe, and runtime assets', async () => {
    const root = await fixture();

    await expect(checkDist(root)).resolves.toBeUndefined();
  });

  it('rejects a runtime asset changed after stamping', async () => {
    const root = await fixture();
    await writeFile(join(root, MANIFEST_ASSETS.duckdb_wasm_url), 'changed');

    await expect(checkDist(root)).rejects.toThrow('hash mismatch');
  });

  it('rejects a missing iframe bundle', async () => {
    const root = await fixture();
    await writeFile(join(root, 'iframe/index.html'), '<html></html>');

    await expect(checkDist(root)).rejects.toThrow('does not reference a JavaScript entry');
  });
});

async function fixture(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'quent-dist-'));
  await mkdir(join(root, 'assets'));
  await mkdir(join(root, 'iframe'));
  await mkdir(join(root, 'workers'));
  await mkdir(join(root, 'wasm'));

  await writeFile(join(root, 'assets/parent.js'), 'parent');
  await writeFile(join(root, 'assets/parent.css'), 'parent-style');
  await writeFile(join(root, 'assets/quent.js'), 'quent');
  await writeFile(join(root, 'iframe/logo.svg'), '<svg/>');
  await writeFile(join(root, 'index.html'), html('./assets/parent.js', './assets/parent.css'));
  await writeFile(join(root, 'iframe/index.html'), html('../assets/quent.js'));

  const hashes: Record<string, string> = {};
  for (const path of Object.values(MANIFEST_ASSETS)) {
    const data = `runtime:${path}`;
    await writeFile(join(root, path), data);
    hashes[path] = createHash('sha256').update(data).digest('hex');
  }

  await writeFile(join(root, 'build-manifest.json'), JSON.stringify({
    ...MANIFEST_ASSETS,
    content_sha256: hashes,
  }));
  return root;
}

function html(script: string, stylesheet?: string): string {
  const style = stylesheet ? `<link rel="stylesheet" href="${stylesheet}">` : '';
  return `<html><head>${style}<script type="module" src="${script}"></script></head></html>`;
}
