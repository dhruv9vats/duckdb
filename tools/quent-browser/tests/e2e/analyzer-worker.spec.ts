import { expect, test } from '@playwright/test';

test('runs the real Rust analyzer worker with lossless counters', async ({ page }) => {
  test.skip(process.env.QUENT_REAL_ANALYZER !== '1', 'Build analyzer WASM and set QUENT_REAL_ANALYZER=1');
  await page.goto('?fixture=1');
  const result = await page.evaluate(async () => {
    const manifest = await fetch('build-manifest.json').then(response => response.json());
    const asset = (path: string) => {
      const url = new URL(path, document.baseURI);
      const digest = manifest.content_sha256?.[path];
      if (digest) {
        url.searchParams.set('v', digest);
      }
      return url.toString();
    };
    const worker = new Worker(asset(manifest.analyzer_worker_url), { type: 'module' });
    let id = 1;
    const request = (op: string, fields: Record<string, unknown>) => new Promise<any>((resolve, reject) => {
      const requestId = id++;
      const listener = (event: MessageEvent) => {
        if (event.data?.id !== requestId) {
          return;
        }
        worker.removeEventListener('message', listener);
        event.data.ok ? resolve(event.data.result) : reject(new Error(event.data.error.message));
      };
      worker.addEventListener('message', listener);
      worker.postMessage({ id: requestId, op, ...fields });
    });
    await request('init', {
      protocol_version: manifest.protocol_version,
      schema_hash: manifest.schema_hash,
      build_id: manifest.build_id,
      assets: {
        wasm_js_url: asset(manifest.analyzer_wasm_js_url),
        wasm_url: asset(manifest.analyzer_wasm_url),
      },
      limits: {
        max_batch_bytes: 4194304,
        max_capture_bytes: 67108864,
        max_session_bytes: 67108864,
        max_snapshot_bytes: 536870912,
        max_history: 8,
      },
    });
    const payloadHex = '0110000000000000000000000000000000018180808080808010000001064475636b4442000000';
    const payload = Uint8Array.from(payloadHex.match(/../g) ?? [], byte => Number.parseInt(byte, 16));
    await request('ingest', {
      header: {
        capture_id: '10000000-0000-0000-0000-000000000001',
        context_id: '30000000-0000-0000-0000-000000000001',
        event_count: 1,
        max_ts_ns: '9007199254740993',
        min_ts_ns: '9007199254740993',
        overflowed: false,
        payload_len: payload.byteLength,
        protocol_version: manifest.protocol_version,
        run_id: '20000000-0000-0000-0000-000000000001',
        schema_hash: manifest.schema_hash,
        seq: '0',
      },
      payload: payload.buffer,
    });
    const sealed = await request('seal', {
      capture_id: '10000000-0000-0000-0000-000000000001',
      final_seq: '0',
      watermark_ns: '9007199254741993',
      query_ids: [],
      outcome: 'success',
      overflowed: false,
    });
    const engines = await request('request', {
      revision: sealed.revision,
      method: 'GET',
      route: '/api/engines',
      params: { with_metadata: true },
    });
    worker.terminate();
    const start = engines.match(/"start_time_unix_ns":\s*(\d+)/)?.[1];
    return { sealed, start };
  });

  expect(result.sealed).toMatchObject({ revision: '1', state: 'sealed', acked_seq: '0' });
  expect(result.start).toBe('9007199254740993');
});
