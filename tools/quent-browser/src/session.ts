import type { ApiClient } from '@quent/client';
import { AnalyzerClient } from './analyzer-client';
import {
  CANCEL_GRACE_MS,
  CONTROL_TIMEOUT_MS,
  MAX_BATCH_BYTES,
  MAX_CAPTURE_BYTES,
  MAX_CAPTURE_HISTORY,
  MAX_SNAPSHOT_BYTES,
  PROTOCOL_VERSION,
  RESULT_ROW_LIMIT,
  STARTUP_TIMEOUT_MS,
  manifestAsset,
  type BuildManifest,
  type CaptureState,
  type CaptureSummary,
  type QueryResult,
  type TelemetryBatch,
  type TelemetrySeal,
} from './protocol';
import { WorkerRpc } from './worker-rpc';

export interface SessionSnapshot {
  captures: CaptureSummary[];
  activeRunId?: string;
  result?: QueryResult;
  status: string;
  revision: string;
  capabilities?: {
    telemetry: boolean;
    spill_io: boolean;
    threads: boolean;
  };
  fixture?: boolean;
}

export interface BrowserSession {
  readonly apiClient: ApiClient;
  snapshot(): SessionSnapshot;
  subscribe(listener: () => void): () => void;
  run(sql: string): Promise<void>;
  cancel(): Promise<void>;
  reset(): Promise<void>;
  select(captureId: string): void;
  close(): void;
}

interface ProducerReady {
  type: 'ready';
  capabilities: SessionSnapshot['capabilities'];
}

interface QueryResultMessage extends QueryResult {
  type: 'query_result';
  run_id: string;
}

interface QueryErrorMessage {
  type: 'query_error';
  run_id: string;
  message: string;
}

interface PublishedResult {
  capture_id: string;
  revision: string | null;
  state: CaptureState;
  query_ids: string[];
}

type ProducerMessage = ProducerReady | QueryResultMessage | QueryErrorMessage | TelemetryBatch | TelemetrySeal;

export class WorkerSession implements BrowserSession {
  readonly apiClient: AnalyzerClient;

  private readonly listeners = new Set<() => void>();
  private readonly captures = new Map<string, CaptureSummary>();
  private readonly analyzerRpc: WorkerRpc;
  private transferChain = Promise.resolve();
  private current: SessionSnapshot = {
    captures: [],
    status: 'Starting workers',
    revision: '0',
  };
  private selectedCaptureId?: string;
  private cancelTimer?: ReturnType<typeof setTimeout>;
  private producerPoisoned = false;

  private constructor(
    private readonly manifest: BuildManifest,
    private producer: Worker,
    private readonly analyzer: Worker,
  ) {
    this.analyzerRpc = new WorkerRpc(analyzer);
    this.apiClient = new AnalyzerClient(this.analyzerRpc);
    producer.addEventListener('message', this.onProducerMessage);
    producer.addEventListener('error', this.onProducerError);
  }

  static async create(manifest: BuildManifest): Promise<WorkerSession> {
    const analyzer = new Worker(manifestAsset(manifest, 'analyzer_worker_url'), { type: 'module', name: 'duckdb-telemetry' });
    const producer = new Worker(manifestAsset(manifest, 'producer_worker_url'), { type: 'module', name: 'duckdb-engine' });
    const session = new WorkerSession(manifest, producer, analyzer);
    try {
      await session.initialize();
      return session;
    } catch (error) {
      session.close();
      throw error;
    }
  }

  snapshot(): SessionSnapshot {
    return this.current;
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  async run(sql: string): Promise<void> {
    if (this.producerPoisoned) {
      throw new Error('Reset the database before running another query');
    }
    if (this.current.activeRunId) {
      throw new Error('A query is already running');
    }

    const runId = crypto.randomUUID();
    const captureId = runId;
    this.captures.set(captureId, {
      captureId,
      runId,
      queryIds: [],
      state: 'running',
      sql,
      droppedEvents: 0,
      bytes: 0,
      startedAt: Date.now(),
    });
    this.current = {
      ...this.current,
      activeRunId: runId,
      result: undefined,
      status: 'Running SQL',
    };
    this.publish();

    this.producer.postMessage({
      type: 'run',
      run_id: runId,
      capture_id: captureId,
      sql,
      result_row_limit: RESULT_ROW_LIMIT,
    });
  }

  async cancel(): Promise<void> {
    const runId = this.current.activeRunId;
    if (!runId) {
      return;
    }

    this.current = { ...this.current, status: 'Cancellation requested' };
    this.publish();
    this.producer.postMessage({ type: 'cancel', run_id: runId });
    this.cancelTimer = setTimeout(() => this.cancelFallback(runId), CANCEL_GRACE_MS);
  }

  async reset(): Promise<void> {
    if (this.current.activeRunId) {
      throw new Error('Cancel the active query before reset');
    }

    await this.transferChain;
    await this.analyzerRpc.request('reset');
    if (this.producerPoisoned) {
      this.producer.removeEventListener('message', this.onProducerMessage);
      this.producer.removeEventListener('error', this.onProducerError);
      this.producer = new Worker(manifestAsset(this.manifest, 'producer_worker_url'), { type: 'module', name: 'duckdb-engine' });
      this.producer.addEventListener('message', this.onProducerMessage);
      this.producer.addEventListener('error', this.onProducerError);
      await this.initializeProducer();
      this.producerPoisoned = false;
    } else {
      await this.waitForProducer('reset_complete', () => this.producer.postMessage({ type: 'reset' }));
    }
    this.captures.clear();
    this.selectedCaptureId = undefined;
    this.apiClient.setRevision('0');
    this.current = {
      ...this.current,
      captures: [],
      result: undefined,
      revision: '0',
      status: 'Ready',
    };
    this.publish();
  }

  select(captureId: string): void {
    const capture = this.captures.get(captureId);
    if (capture?.revision === undefined) {
      return;
    }

    this.selectedCaptureId = captureId;
    this.apiClient.setRevision(capture.revision);
    this.current = { ...this.current, revision: capture.revision };
    this.publish();
  }

  close(): void {
    if (this.cancelTimer) {
      clearTimeout(this.cancelTimer);
    }
    this.producer.removeEventListener('message', this.onProducerMessage);
    this.producer.removeEventListener('error', this.onProducerError);
    this.analyzerRpc.close();
    this.producer.terminate();
    this.analyzer.terminate();
  }

  private async initialize(): Promise<void> {
    const absoluteManifest = {
      ...this.manifest,
      duckdb_module_js_url: manifestAsset(this.manifest, 'duckdb_module_js_url'),
      duckdb_wasm_url: manifestAsset(this.manifest, 'duckdb_wasm_url'),
      analyzer_wasm_js_url: manifestAsset(this.manifest, 'analyzer_wasm_js_url'),
      analyzer_wasm_url: manifestAsset(this.manifest, 'analyzer_wasm_url'),
    };
    await this.analyzerRpc.request(
      'init',
      {
        protocol_version: PROTOCOL_VERSION,
        schema_hash: this.manifest.schema_hash,
        build_id: this.manifest.build_id,
        assets: {
          wasm_js_url: absoluteManifest.analyzer_wasm_js_url,
          wasm_url: absoluteManifest.analyzer_wasm_url,
        },
        limits: this.limits(),
      },
      [],
      STARTUP_TIMEOUT_MS,
    );
    const producerReady = await this.initializeProducer();
    this.current = {
      ...this.current,
      capabilities: producerReady.capabilities,
      status: 'Ready',
    };
    this.publish();
  }

  private initializeProducer(): Promise<ProducerReady> {
    const absoluteManifest = {
      ...this.manifest,
      duckdb_module_js_url: manifestAsset(this.manifest, 'duckdb_module_js_url'),
      duckdb_wasm_url: manifestAsset(this.manifest, 'duckdb_wasm_url'),
      analyzer_wasm_js_url: manifestAsset(this.manifest, 'analyzer_wasm_js_url'),
      analyzer_wasm_url: manifestAsset(this.manifest, 'analyzer_wasm_url'),
    };
    return this.waitForProducer<ProducerReady>('ready', () => {
      this.producer.postMessage({
        type: 'init',
        manifest: absoluteManifest,
        limits: this.limits(),
      });
    }, STARTUP_TIMEOUT_MS);
  }

  private limits(): Record<string, number> {
    return {
      max_batch_bytes: MAX_BATCH_BYTES,
      max_capture_bytes: MAX_CAPTURE_BYTES,
      max_session_bytes: MAX_CAPTURE_BYTES,
      max_snapshot_bytes: MAX_SNAPSHOT_BYTES,
      max_history: MAX_CAPTURE_HISTORY,
    };
  }

  private readonly onProducerMessage = (event: MessageEvent<ProducerMessage>): void => {
    const message = event.data;
    if (message.type === 'telemetry_batch') {
      this.transferChain = this.transferChain.then(() => this.ingest(message)).catch(error => this.fail(message.capture_id, error));
      return;
    }
    if (message.type === 'telemetry_seal') {
      this.transferChain = this.transferChain.then(() => this.seal(message)).catch(error => this.fail(message.capture_id, error));
      return;
    }
    if (message.type === 'query_result') {
      if (message.run_id !== this.current.activeRunId) {
        return;
      }
      this.current = {
        ...this.current,
        result: {
          columns: message.columns,
          rows: message.rows,
          rowCount: message.rowCount,
          truncated: message.truncated,
        },
        status: 'SQL complete; publishing telemetry',
      };
      this.publish();
      return;
    }
    if (message.type === 'query_error') {
      if (message.run_id !== this.current.activeRunId) {
        return;
      }
      this.finishSqlError(message.run_id, message.message);
    }
  };

  private async ingest(batch: TelemetryBatch): Promise<void> {
    if (batch.payload.byteLength > MAX_BATCH_BYTES) {
      throw new Error(`Batch exceeds ${MAX_BATCH_BYTES} bytes`);
    }

    const capture = this.captures.get(batch.capture_id);
    if (!capture || capture.state !== 'running') {
      return;
    }
    if (batch.protocol_version !== PROTOCOL_VERSION || batch.schema_hash !== this.manifest.schema_hash) {
      throw new Error('Telemetry batch contract mismatch');
    }
    capture.bytes += batch.payload.byteLength;
    if (capture.bytes > MAX_CAPTURE_BYTES) {
      throw new Error(`Capture exceeds ${MAX_CAPTURE_BYTES} bytes`);
    }

    const payload = batch.payload;
    await this.analyzerRpc.request(
      'ingest',
      {
        header: {
          protocol_version: PROTOCOL_VERSION,
          schema_hash: this.manifest.schema_hash,
          capture_id: batch.capture_id,
          run_id: batch.run_id,
          context_id: batch.context_id ?? batch.capture_id,
          seq: batch.sequence,
          event_count: batch.event_count,
          payload_len: payload.byteLength,
          min_ts_ns: batch.timestamp_start_ns,
          max_ts_ns: batch.timestamp_end_ns,
          overflowed: batch.overflow,
        },
        payload,
      },
      [payload],
    );
    this.producer.postMessage({ type: 'telemetry_ack', capture_id: batch.capture_id, sequence: batch.sequence });
    this.publish();
  }

  private async seal(seal: TelemetrySeal): Promise<void> {
    const capture = this.captures.get(seal.capture_id);
    if (!capture || capture.state !== 'running') {
      return;
    }

    const result = await this.analyzerRpc.request<PublishedResult>('seal', {
      capture_id: seal.capture_id,
      final_seq: seal.final_sequence,
      watermark_ns: seal.watermark_ns,
      query_ids: seal.query_ids,
      outcome: seal.outcome,
      overflowed: seal.dropped_events > 0 || !seal.complete,
    });
    if (!seal.complete) {
      this.producer.terminate();
      this.producerPoisoned = true;
    }
    if (result.revision === null) {
      capture.queryIds = result.query_ids;
      capture.state = result.state;
      capture.droppedEvents = seal.dropped_events;
      this.clearCancelTimer();
      const status = capture.error
        ? `SQL failed: ${capture.error}; no analyzable telemetry`
        : 'No analyzable query';
      this.current = { ...this.current, activeRunId: undefined, status };
      this.evictOldCaptures();
      this.publish();
      return;
    }

    capture.revision = result.revision;
    capture.queryIds = result.query_ids;
    capture.state = result.state;
    capture.droppedEvents = seal.dropped_events;
    this.selectedCaptureId = seal.capture_id;
    this.apiClient.setRevision(result.revision);
    const engines = await this.apiClient.fetchListEngines();
    capture.engineId = engines[0]?.id;
    this.clearCancelTimer();
    this.current = {
      ...this.current,
      activeRunId: undefined,
      revision: result.revision,
      status: capture.error
        ? `SQL failed: ${capture.error}; telemetry ${capture.state}`
        : capture.state === 'sealed' ? 'Telemetry ready' : `Telemetry ${capture.state}`,
    };
    this.producer.postMessage({
      type: 'telemetry_published',
      capture_id: seal.capture_id,
      revision: result.revision,
      query_ids: result.query_ids,
    });
    this.evictOldCaptures();
    this.publish();
  }

  private finishSqlError(runId: string, message: string): void {
    const capture = [...this.captures.values()].find(item => item.runId === runId);
    if (capture) {
      capture.error = message;
    }
    if (this.current.activeRunId !== runId) {
      return;
    }
    this.current = { ...this.current, status: `SQL failed: ${message}` };
    this.publish();
  }

  private fail(captureId: string, reason: unknown): void {
    const error = reason instanceof Error ? reason : new Error(String(reason));
    const capture = this.captures.get(captureId);
    if (capture?.state === 'running') {
      capture.state = 'incomplete';
      capture.error = error.message;
    }
    if (capture && this.current.activeRunId !== capture.runId) {
      this.publish();
      return;
    }
    this.producerPoisoned = true;
    this.clearCancelTimer();
    this.producer.terminate();
    this.current = {
      ...this.current,
      activeRunId: undefined,
      status: `Telemetry incomplete: ${error.message}`,
    };
    this.publish();
  }

  private cancelFallback(runId: string): void {
    if (this.current.activeRunId !== runId) {
      return;
    }

    const capture = [...this.captures.values()].find(item => item.runId === runId);
    if (capture) {
      capture.state = 'incomplete';
      capture.error = 'DuckDB worker terminated after cancellation timeout; database reset required';
    }
    this.producer.terminate();
    this.producerPoisoned = true;
    this.current = {
      ...this.current,
      activeRunId: undefined,
      status: 'Worker terminated; telemetry incomplete',
    };
    this.publish();
  }

  private evictOldCaptures(): void {
    const retained = [...this.captures.values()];
    retained.sort((left, right) => left.startedAt - right.startedAt);
    while (retained.length > MAX_CAPTURE_HISTORY) {
      const capture = retained.shift();
      if (capture) {
        this.captures.delete(capture.captureId);
      }
    }
  }

  private waitForProducer<T extends { type: string }>(
    type: string,
    start: () => void,
    timeoutMs = CONTROL_TIMEOUT_MS,
  ): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.producer.removeEventListener('message', onMessage);
        this.producer.removeEventListener('error', onError);
        reject(new Error(`Producer timed out: ${type}`));
      }, timeoutMs);
      const onMessage = (event: MessageEvent<T>): void => {
        if (event.data?.type !== type) {
          return;
        }
        clearTimeout(timeout);
        this.producer.removeEventListener('message', onMessage);
        this.producer.removeEventListener('error', onError);
        resolve(event.data);
      };
      const onError = (event: ErrorEvent): void => {
        clearTimeout(timeout);
        this.producer.removeEventListener('message', onMessage);
        this.producer.removeEventListener('error', onError);
        reject(new Error(event.message || `Producer failed before ${type}`));
      };
      this.producer.addEventListener('message', onMessage);
      this.producer.addEventListener('error', onError);
      start();
    });
  }

  private readonly onProducerError = (event: ErrorEvent): void => {
    const runId = this.current.activeRunId;
    const capture = [...this.captures.values()].find(item => item.runId === runId);
    if (capture) {
      this.fail(capture.captureId, new Error(event.message || 'DuckDB worker failed'));
      return;
    }

    this.producerPoisoned = true;
    this.producer.terminate();
    this.current = { ...this.current, status: `DuckDB worker failed: ${event.message}` };
    this.publish();
  };

  private clearCancelTimer(): void {
    if (!this.cancelTimer) {
      return;
    }
    clearTimeout(this.cancelTimer);
    this.cancelTimer = undefined;
  }

  private publish(): void {
    this.current = {
      ...this.current,
      captures: [...this.captures.values()].sort((left, right) => right.startedAt - left.startedAt),
    };
    for (const listener of this.listeners) {
      listener();
    }
  }
}
