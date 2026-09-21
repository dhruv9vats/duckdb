let facade;
const cancelled = new Set();

self.addEventListener('message', event => {
  const message = event.data;
  if (typeof message?.id !== 'number' || typeof message?.op !== 'string') {
    return;
  }

  void dispatch(message).then(
    result => self.postMessage({ id: message.id, ok: true, result }),
    reason => self.postMessage({ id: message.id, ok: false, error: workerError(reason) }),
  );
});

async function dispatch(message) {
  if (message.op === 'init') {
    const module = await import(message.assets.wasm_js_url);
    await module.default(message.assets.wasm_url);
    facade = new module.AnalyzerFacade(JSON.stringify({
      manifest: {
        protocol_version: message.protocol_version,
        schema_hash: message.schema_hash,
        build_id: message.build_id,
      },
      limits: message.limits,
    }));
    return {
      protocol_version: message.protocol_version,
      schema_hash: message.schema_hash,
      build_id: message.build_id,
    };
  }

  if (!facade) {
    throw new Error('NOT_INITIALIZED: analyzer is not initialized');
  }
  if (message.op === 'ingest') {
    return JSON.parse(facade.ingest(JSON.stringify(message.header), new Uint8Array(message.payload)));
  }
  if (message.op === 'seal') {
    return JSON.parse(facade.seal(JSON.stringify({
      capture_id: message.capture_id,
      final_seq: message.final_seq,
      watermark_ns: message.watermark_ns,
      query_ids: message.query_ids,
      outcome: message.outcome,
      overflowed: message.overflowed,
    })));
  }
  if (message.op === 'request') {
    if (cancelled.delete(message.id)) {
      throw new Error('CANCELLED: analysis request cancelled');
    }
    return facade.request(JSON.stringify({
      revision: message.revision,
      method: message.method,
      route: message.route,
      params: message.params ?? {},
      body: message.body ?? null,
    }));
  }
  if (message.op === 'cancel') {
    cancelled.add(message.request_id);
    return {};
  }
  if (message.op === 'reset') {
    facade.reset();
    cancelled.clear();
    return { revision: '0' };
  }

  throw new Error(`UNKNOWN_OPERATION: ${message.op}`);
}

function workerError(reason) {
  const message = reason instanceof Error ? reason.message : String(reason);
  const separator = message.indexOf(':');
  if (separator < 0) {
    return { code: 'ANALYZER_ERROR', message };
  }
  return {
    code: message.slice(0, separator).trim(),
    message: message.slice(separator + 1).trim(),
  };
}

