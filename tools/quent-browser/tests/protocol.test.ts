import { afterEach, describe, expect, it, vi } from 'vitest';
import { manifestAsset, type BuildManifest } from '../src/protocol';

describe('manifest assets', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('keeps the project subpath and content version', () => {
    vi.stubGlobal('document', { baseURI: 'https://example.test/project/' });
    const path = 'workers/duckdb-browser.worker.js';
    const manifest = {
      protocol_version: 1,
      schema_hash: 'schema',
      build_id: 'build',
      producer_worker_url: path,
      duckdb_module_js_url: 'wasm/duckdb.js',
      duckdb_wasm_url: 'wasm/duckdb.wasm',
      analyzer_worker_url: 'workers/analyzer.js',
      analyzer_wasm_js_url: 'wasm/analyzer.js',
      analyzer_wasm_url: 'wasm/analyzer.wasm',
      content_sha256: { [path]: 'abc123' },
    } satisfies BuildManifest;

    expect(manifestAsset(manifest, 'producer_worker_url')).toBe(
      'https://example.test/project/workers/duckdb-browser.worker.js?v=abc123',
    );
  });
});
