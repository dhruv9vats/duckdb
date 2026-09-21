export const PROTOCOL_VERSION = 1;
export const MAX_BATCH_BYTES = 4 * 1024 * 1024;
export const MAX_CAPTURE_BYTES = 64 * 1024 * 1024;
export const MAX_SNAPSHOT_BYTES = 512 * 1024 * 1024;
export const MAX_CAPTURE_HISTORY = 8;
export const RESULT_ROW_LIMIT = 200;
export const CONTROL_TIMEOUT_MS = 15_000;
export const STARTUP_TIMEOUT_MS = 120_000;
export const ANALYSIS_TIMEOUT_MS = 60_000;
export const CANCEL_GRACE_MS = 2_000;

export type CaptureState =
  | 'running'
  | 'sealed'
  | 'incomplete'
  | 'overflowed'
  | 'failed'
  | 'evicted';

export interface BuildManifest {
  protocol_version: number;
  schema_hash: string;
  build_id: string;
  producer_worker_url: string;
  duckdb_module_js_url: string;
  duckdb_wasm_url: string;
  analyzer_worker_url: string;
  analyzer_wasm_js_url: string;
  analyzer_wasm_url: string;
  content_sha256?: Record<string, string>;
}

export interface CaptureSummary {
  captureId: string;
  runId: string;
  queryIds: string[];
  engineId?: string;
  revision?: string;
  state: CaptureState;
  sql: string;
  error?: string;
  droppedEvents: number;
  bytes: number;
  startedAt: number;
}

export interface QueryResult {
  columns: string[];
  rows: unknown[][];
  rowCount: number;
  truncated: boolean;
}

export interface WorkerErrorShape {
  code: string;
  message: string;
}

export type RpcResponse<T> =
  | { id: number; ok: true; result: T }
  | { id: number; ok: false; error: WorkerErrorShape };

export interface TelemetryBatch {
  type: 'telemetry_batch';
  protocol_version: number;
  schema_hash: string;
  capture_id: string;
  context_id?: string;
  run_id: string;
  sequence: string;
  event_count: number;
  payload: ArrayBuffer;
  timestamp_start_ns: string;
  timestamp_end_ns: string;
  overflow: boolean;
}

export interface TelemetrySeal {
  type: 'telemetry_seal';
  capture_id: string;
  run_id: string;
  final_sequence: string | null;
  watermark_ns: string;
  query_ids: string[];
  outcome: 'success' | 'failed' | 'cancelled';
  dropped_events: number;
  complete: boolean;
}

export function assetUrl(path: string, digest?: string): string {
  const url = new URL(path, document.baseURI);
  if (digest) {
    url.searchParams.set('v', digest);
  }
  return url.toString();
}

export function manifestAsset(manifest: BuildManifest, key: keyof BuildManifest): string {
  const path = manifest[key];
  if (typeof path !== 'string') {
    throw new Error(`Invalid manifest asset ${key}`);
  }

  const digest = manifest.content_sha256?.[path];
  if (manifest.content_sha256 && !digest) {
    throw new Error(`Missing content digest for ${path}`);
  }

  return assetUrl(path, digest);
}

export async function loadManifest(): Promise<BuildManifest> {
  const response = await fetch(assetUrl('build-manifest.json'), { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(`Build manifest: ${response.status} ${response.statusText}`);
  }

  const manifest = (await response.json()) as BuildManifest;
  if (manifest.protocol_version !== PROTOCOL_VERSION) {
    throw new Error(`Unsupported protocol ${manifest.protocol_version}`);
  }
  if (!manifest.content_sha256) {
    throw new Error('Build manifest is not stamped');
  }

  return manifest;
}
