import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { CONTROL_TIMEOUT_MS, MAX_CAPTURE_HISTORY, type BuildManifest } from '../src/protocol';
import { WorkerSession } from '../src/session';

const manifest: BuildManifest = {
  protocol_version: 1,
  schema_hash: 'fixture-schema',
  build_id: 'fixture-build',
  producer_worker_url: 'workers/producer.js',
  duckdb_module_js_url: 'wasm/duckdb.js',
  duckdb_wasm_url: 'wasm/duckdb.wasm',
  analyzer_worker_url: 'workers/analyzer.js',
  analyzer_wasm_js_url: 'wasm/analyzer.js',
  analyzer_wasm_url: 'wasm/analyzer.wasm',
};

class FakeWorker extends EventTarget {
  static instances: FakeWorker[] = [];
  static readyDelay = 0;
  readonly analyzer: boolean;
  terminated = false;
  requests: unknown[] = [];

  constructor(url: string | URL) {
    super();
    this.analyzer = String(url).includes('analyzer');
    FakeWorker.instances.push(this);
  }

  postMessage(message: Record<string, unknown>): void {
    this.requests.push(message);
    if (this.analyzer && typeof message.id === 'number') {
      const result = message.op === 'seal'
        ? message.final_seq === null
          ? { capture_id: message.capture_id, revision: null, state: 'failed', query_ids: [] }
          : { capture_id: message.capture_id, revision: '7', state: 'sealed', query_ids: ['query-1'] }
        : message.op === 'request'
          ? '[{"id":"duckdb-browser"}]'
          : {};
      queueMicrotask(() => this.emit({ id: message.id, ok: true, result }));
      return;
    }
    if (message.type === 'init') {
      const ready = () => this.emit({
        type: 'ready',
        capabilities: { telemetry: true, spill_io: false, threads: false },
      });
      if (FakeWorker.readyDelay > 0) {
        setTimeout(ready, FakeWorker.readyDelay);
      } else {
        queueMicrotask(ready);
      }
    }
    if (message.type === 'reset') {
      queueMicrotask(() => this.emit({ type: 'reset_complete' }));
    }
  }

  terminate(): void {
    this.terminated = true;
  }

  emit(data: unknown): void {
    this.dispatchEvent(new MessageEvent('message', { data }));
  }

  fail(message: string): void {
    this.dispatchEvent(new ErrorEvent('error', { message }));
  }
}

describe('WorkerSession terminal states', () => {
  let nextRun: number;

  beforeEach(() => {
    FakeWorker.instances = [];
    FakeWorker.readyDelay = 0;
    nextRun = 1;
    vi.stubGlobal('Worker', FakeWorker);
    vi.stubGlobal('crypto', { randomUUID: vi.fn(() => `run-${nextRun++}`) });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('ignores stale SQL results', async () => {
    const session = await WorkerSession.create(manifest);
    await session.run('select 42');
    producer().emit({
      type: 'query_result',
      run_id: 'stale-run',
      columns: ['stale'],
      rows: [[0]],
      rowCount: 1,
      truncated: false,
    });

    expect(session.snapshot().result).toBeUndefined();
  });

  it('allows producer startup beyond the control timeout', async () => {
    vi.useFakeTimers();
    FakeWorker.readyDelay = CONTROL_TIMEOUT_MS + 1_000;
    const creating = WorkerSession.create(manifest);
    await vi.advanceTimersByTimeAsync(FakeWorker.readyDelay);

    await expect(creating).resolves.toBeInstanceOf(WorkerSession);
    vi.useRealTimers();
  });

  it('does not publish a seal after capture failure', async () => {
    const session = await WorkerSession.create(manifest);
    await session.run('select 42');
    const captureId = session.snapshot().captures[0]!.captureId;
    producer().emit({
      type: 'telemetry_batch',
      protocol_version: 1,
      schema_hash: 'fixture-schema',
      capture_id: captureId,
      run_id: session.snapshot().activeRunId,
      sequence: '0',
      event_count: 1,
      payload: new ArrayBuffer(4 * 1024 * 1024 + 1),
      timestamp_start_ns: '9007199254740993',
      timestamp_end_ns: '9007199254740994',
      overflow: false,
    });
    await vi.waitFor(() => expect(session.snapshot().captures[0]!.state).toBe('incomplete'));
    producer().emit({
      type: 'telemetry_seal',
      capture_id: captureId,
      run_id: session.snapshot().captures[0]!.runId,
      final_sequence: '0',
      watermark_ns: '9007199254740994',
      query_ids: ['query-1'],
      outcome: 'success',
      dropped_events: 0,
      complete: true,
    });
    await Promise.resolve();

    expect(session.snapshot().captures[0]!.state).toBe('incomplete');
    expect(analyzer().requests).not.toContainEqual(expect.objectContaining({ op: 'seal' }));
  });

  it('bounds retained capture metadata', async () => {
    const session = await WorkerSession.create(manifest);
    for (let index = 0; index < MAX_CAPTURE_HISTORY + 2; index++) {
      await session.run(`select ${index}`);
      const capture = session.snapshot().captures[0]!;
      producer().emit({
        type: 'telemetry_seal',
        capture_id: capture.captureId,
        run_id: capture.runId,
        final_sequence: '0',
        watermark_ns: String(9007199254740993n + BigInt(index)),
        query_ids: ['query-1'],
        outcome: 'success',
        dropped_events: 0,
        complete: true,
      });
      await vi.waitFor(() => expect(session.snapshot().activeRunId).toBeUndefined());
    }

    expect(session.snapshot().captures).toHaveLength(MAX_CAPTURE_HISTORY);
  });

  it('keeps a failed preplan capture without publishing a revision', async () => {
    const session = await WorkerSession.create(manifest);
    await session.run('not sql');
    const capture = session.snapshot().captures[0]!;
    producer().emit({
      type: 'telemetry_seal',
      capture_id: capture.captureId,
      run_id: capture.runId,
      final_sequence: null,
      watermark_ns: '9007199254740993',
      query_ids: [],
      outcome: 'failed',
      dropped_events: 0,
      complete: true,
    });
    await vi.waitFor(() => expect(session.snapshot().activeRunId).toBeUndefined());

    expect(session.snapshot().captures[0]!.state).toBe('failed');
    expect(session.snapshot().captures[0]).not.toHaveProperty('revision');
    expect(session.snapshot().revision).toBe('0');
  });

  it('bounds failed preplan capture metadata', async () => {
    const session = await WorkerSession.create(manifest);
    for (let index = 0; index < MAX_CAPTURE_HISTORY + 2; index++) {
      await session.run('not sql');
      const capture = session.snapshot().captures[0]!;
      producer().emit({
        type: 'telemetry_seal',
        capture_id: capture.captureId,
        run_id: capture.runId,
        final_sequence: null,
        watermark_ns: String(9007199254740993n + BigInt(index)),
        query_ids: [],
        outcome: 'failed',
        dropped_events: 0,
        complete: true,
      });
      await vi.waitFor(() => expect(session.snapshot().activeRunId).toBeUndefined());
    }

    expect(session.snapshot().captures).toHaveLength(MAX_CAPTURE_HISTORY);
  });

  it('keeps the SQL error after telemetry publishes', async () => {
    const session = await WorkerSession.create(manifest);
    await session.run('select fail()');
    const capture = session.snapshot().captures[0]!;
    producer().emit({ type: 'query_error', run_id: capture.runId, message: 'query failed' });
    producer().emit({
      type: 'telemetry_seal',
      capture_id: capture.captureId,
      run_id: capture.runId,
      final_sequence: '0',
      watermark_ns: '9007199254740993',
      query_ids: ['query-1'],
      outcome: 'failed',
      dropped_events: 0,
      complete: true,
    });
    await vi.waitFor(() => expect(session.snapshot().activeRunId).toBeUndefined());

    expect(session.snapshot().status).toContain('SQL failed: query failed');
    expect(session.snapshot().captures[0]!.state).toBe('sealed');
  });

  it('recreates the producer on reset after cancellation timeout', async () => {
    vi.useFakeTimers();
    const session = await WorkerSession.create(manifest);
    await session.run('select * from range(1000000000)');
    await session.cancel();
    await vi.advanceTimersByTimeAsync(2_001);

    expect(producer().terminated).toBe(true);
    expect(session.snapshot().captures[0]!.state).toBe('incomplete');
    const workerCount = FakeWorker.instances.length;
    const reset = session.reset();
    await vi.runAllTimersAsync();
    await reset;

    expect(FakeWorker.instances).toHaveLength(workerCount + 1);
    expect(session.snapshot()).toMatchObject({ captures: [], status: 'Ready', revision: '0' });
    vi.useRealTimers();
  });

  it('ends an active capture after a fatal producer error', async () => {
    const session = await WorkerSession.create(manifest);
    await session.run('select 42');
    producer().fail('WASM trap');

    expect(session.snapshot().activeRunId).toBeUndefined();
    expect(session.snapshot().captures[0]).toMatchObject({ state: 'incomplete', error: 'WASM trap' });
    expect(producer().terminated).toBe(true);

    const reset = session.reset();
    await reset;
    expect(session.snapshot()).toMatchObject({ captures: [], status: 'Ready' });
  });
});

function producer(): FakeWorker {
  return FakeWorker.instances.find(worker => !worker.analyzer)!;
}

function analyzer(): FakeWorker {
  return FakeWorker.instances.find(worker => worker.analyzer)!;
}
