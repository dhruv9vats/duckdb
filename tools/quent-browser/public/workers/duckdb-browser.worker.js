const DRAIN_EMPTY = 0;
const DRAIN_READY = 1;
const DRAIN_FAILED = 2;
const STATUS_OK = 0;
const ACK_TIMEOUT_MS = 15_000;

let engine;
let manifest;
let limits;
let activeRun;
let cancelled = false;
let pendingAck;

self.addEventListener('message', event => {
  const message = event.data;
  if (message?.type === 'init') {
    void initialize(message).catch(reportWorkerError);
    return;
  }
  if (message?.type === 'run') {
    void run(message).catch(reason => failRun(message, reason));
    return;
  }
  if (message?.type === 'cancel') {
    if (activeRun?.run_id === message.run_id) {
      cancelled = true;
    }
    return;
  }
  if (message?.type === 'telemetry_ack') {
    acceptAck(message);
    return;
  }
  if (message?.type === 'reset') {
    reset();
  }
});

async function initialize(message) {
  if (engine) {
    throw new Error('DuckDB worker is already initialized');
  }

  manifest = message.manifest;
  limits = message.limits;
  const module = await import(manifest.duckdb_module_js_url);
  engine = await module.default({
    locateFile(path) {
      return path.endsWith('.wasm') ? manifest.duckdb_wasm_url : new URL(path, manifest.duckdb_module_js_url).toString();
    },
  });
  requireRuntime();
  validateManifest();
  checkStatus(engine._quent_browser_open(), 'open');
  self.postMessage({
    type: 'ready',
    capabilities: { telemetry: true, spill_io: false, threads: false },
  });
}

function validateManifest() {
  const compiled = parsePointer(engine._quent_browser_manifest_json(), 'build manifest');
  const keys = ['protocol_version', 'schema_hash', 'build_id'];
  for (const key of keys) {
    if (compiled[key] !== manifest[key]) {
      throw new Error(`DuckDB ${key} does not match the build manifest`);
    }
  }
}

async function run(message) {
  if (!engine) {
    throw new Error('DuckDB worker is not initialized');
  }
  if (activeRun) {
    throw new Error('A query is already running');
  }

  activeRun = message;
  cancelled = false;
  let outcome = 'success';
  let error;
  try {
    query(message.sql, message.result_row_limit);
  } catch (reason) {
    outcome = cancelled ? 'cancelled' : 'failed';
    error = asError(reason);
    self.postMessage({ type: 'query_error', run_id: message.run_id, message: error.message });
  }

  const capture = await drain(message);
  if (capture.failed) {
    outcome = 'failed';
    if (!error) {
      error = new Error('Telemetry drain failed');
      self.postMessage({ type: 'query_error', run_id: message.run_id, message: error.message });
    }
  }
  if (cancelled) {
    outcome = 'cancelled';
  }

  self.postMessage({
    type: 'telemetry_seal',
    capture_id: message.capture_id,
    run_id: message.run_id,
    final_sequence: capture.finalSequence,
    watermark_ns: capture.watermark,
    query_ids: capture.queryIds,
    outcome,
    dropped_events: capture.dropped,
    complete: !capture.failed && capture.dropped === 0,
  });
  activeRun = undefined;
}

function query(sql, rowLimit) {
  const sqlPointer = engine.stringToNewUTF8(sql);
  try {
    checkStatus(engine._quent_browser_query(sqlPointer, rowLimit), 'query');
  } finally {
    engine._free(sqlPointer);
  }

  const result = parsePointer(engine._quent_browser_result_json(), 'query result');
  self.postMessage({
    type: 'query_result',
    run_id: activeRun.run_id,
    columns: result.columns,
    rows: result.rows,
    rowCount: result.row_count,
    truncated: result.truncated,
  });
}

async function drain(message) {
  let sequence = 0n;
  let watermark = decimal(engine._quent_browser_watermark());
  let dropped = 0;
  let failed = false;
  const queryIds = new Set();
  const contextId = parsePointer(engine._quent_browser_context_id_json(), 'context ID');
  const maxBytes = limits?.max_batch_bytes;

  while (true) {
    engine._quent_browser_drain(BigInt(maxBytes));
    const status = engine._quent_browser_drain_status();
    if (status === DRAIN_EMPTY) {
      break;
    }
    if (status === DRAIN_FAILED) {
      failed = true;
      break;
    }
    if (status !== DRAIN_READY) {
      throw new Error(`Unknown telemetry drain status ${status}`);
    }

    const length = Number(engine._quent_browser_drain_len());
    const count = Number(engine._quent_browser_drain_count());
    const minTimestamp = decimal(engine._quent_browser_drain_min_ts());
    const maxTimestamp = decimal(engine._quent_browser_drain_max_ts());
    const batchDropped = Number(engine._quent_browser_drain_dropped());
    dropped += batchDropped;
    watermark = maxDecimal(watermark, maxTimestamp);
    if (length === 0) {
      continue;
    }
    if (!Number.isSafeInteger(length) || length > maxBytes) {
      throw new Error(`Telemetry batch length ${length} exceeds ${maxBytes}`);
    }

    const pointer = Number(engine._quent_browser_drain_ptr());
    if (!Number.isSafeInteger(pointer) || pointer < 0 || pointer + length > engine.HEAPU8.byteLength) {
      throw new Error(`Telemetry payload range ${pointer}+${length} exceeds the WASM heap`);
    }
    const payload = engine.HEAPU8.slice(pointer, pointer + length).buffer;
    const currentSequence = sequence.toString();
    self.postMessage({
      type: 'telemetry_batch',
      protocol_version: manifest.protocol_version,
      schema_hash: manifest.schema_hash,
      capture_id: message.capture_id,
      context_id: contextId,
      run_id: message.run_id,
      sequence: currentSequence,
      event_count: count,
      payload,
      timestamp_start_ns: minTimestamp,
      timestamp_end_ns: maxTimestamp,
      overflow: batchDropped > 0,
    }, [payload]);
    await waitForAck(message.capture_id, currentSequence);
    sequence += 1n;
  }

  const queryIdsJson = engine._quent_browser_query_ids_json?.();
  if (queryIdsJson) {
    for (const queryId of parsePointer(queryIdsJson, 'query IDs')) {
      queryIds.add(queryId);
    }
  }

  return {
    finalSequence: sequence === 0n ? null : (sequence - 1n).toString(),
    watermark,
    dropped,
    failed,
    queryIds: [...queryIds],
  };
}

function waitForAck(captureId, sequence) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      pendingAck = undefined;
      reject(new Error(`Telemetry acknowledgment timed out at sequence ${sequence}`));
    }, ACK_TIMEOUT_MS);
    pendingAck = { captureId, sequence, resolve, timeout };
  });
}

function acceptAck(message) {
  if (!pendingAck) {
    return;
  }
  if (pendingAck.captureId !== message.capture_id || pendingAck.sequence !== message.sequence) {
    return;
  }

  clearTimeout(pendingAck.timeout);
  const resolve = pendingAck.resolve;
  pendingAck = undefined;
  resolve();
}

function reset() {
  if (!engine || activeRun) {
    throw new Error('DuckDB cannot reset while a query is active');
  }
  checkStatus(engine._quent_browser_reset(), 'reset');
  self.postMessage({ type: 'reset_complete' });
}

function requireRuntime() {
  const required = ['stringToNewUTF8', 'UTF8ToString', '_free'];
  for (const name of required) {
    if (typeof engine[name] !== 'function') {
      throw new Error(`DuckDB module lacks ${name}`);
    }
  }
  if (!(engine.HEAPU8 instanceof Uint8Array)) {
    throw new Error('DuckDB module lacks HEAPU8');
  }
}

function checkStatus(status, operation) {
  if (status === STATUS_OK) {
    return;
  }
  const pointer = engine._quent_browser_error_json?.();
  const error = pointer ? parsePointer(pointer, `${operation} error`) : undefined;
  throw new Error(error?.message ?? `DuckDB ${operation} failed with status ${status}`);
}

function parsePointer(pointer, label) {
  if (!pointer) {
    throw new Error(`DuckDB returned no ${label}`);
  }
  return JSON.parse(engine.UTF8ToString(pointer));
}

function decimal(value) {
  if (typeof value === 'bigint') {
    return value.toString();
  }
  if (!Number.isSafeInteger(value)) {
    throw new Error('DuckDB timestamp requires WASM_BIGINT');
  }
  return String(value);
}

function maxDecimal(left, right) {
  return BigInt(left) >= BigInt(right) ? left : right;
}

function failRun(message, reason) {
  const error = asError(reason);
  self.postMessage({ type: 'query_error', run_id: message.run_id, message: error.message });
  self.postMessage({
    type: 'telemetry_seal',
    capture_id: message.capture_id,
    run_id: message.run_id,
    final_sequence: null,
    watermark_ns: '0',
    query_ids: [],
    outcome: cancelled ? 'cancelled' : 'failed',
    dropped_events: 0,
    complete: false,
  });
  activeRun = undefined;
}

function reportWorkerError(reason) {
  queueMicrotask(() => {
    throw asError(reason);
  });
}

function asError(reason) {
  return reason instanceof Error ? reason : new Error(String(reason));
}
