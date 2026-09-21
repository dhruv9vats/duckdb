import type { ApiClient } from '@quent/client';
import { useEffect, useRef, useState, type MutableRefObject } from 'react';
import type { CaptureSnapshot, QuentApiMethod, QuentRpcRequest } from '../iframe/protocol';
import type { CaptureSummary } from './protocol';

type RpcMethod = QuentApiMethod;

enum SnapshotMode {
  Deduplicate,
  Force,
}

const METHODS: ReadonlySet<RpcMethod> = new Set([
  'fetchQueryBundle',
  'fetchListEngines',
  'fetchEngineContexts',
  'fetchNvtxCatalog',
  'fetchNvtxViewport',
  'fetchListCoordinators',
  'fetchListQueries',
  'fetchSingleTimeline',
  'fetchBulkTimelines',
  'fetchEntityList',
  'fetchDataFlow',
]);

interface QuentFrameProps {
  apiClient: ApiClient;
  revision: string;
  getRevision(): string;
  capture?: CaptureSummary;
}

// The iframe sees snapshots and a narrow API, never workers or network transport.
export function QuentFrame({ apiClient, revision, getRevision, capture }: QuentFrameProps) {
  const iframe = useRef<HTMLIFrameElement>(null);
  const port = useRef<MessagePort | undefined>(undefined);
  const sentSnapshot = useRef<string | undefined>(undefined);
  const latest = useRef({ apiClient, revision, getRevision, capture });
  const [ready, setReady] = useState(false);
  latest.current = { apiClient, revision, getRevision, capture };

  useEffect(() => {
    const frame = iframe.current;
    if (!frame) {
      return;
    }

    const receive = (event: MessageEvent): void => {
      if (event.origin !== location.origin || event.source !== frame.contentWindow || event.data?.type !== 'quent-connect') {
        return;
      }
      const channel = new MessageChannel();
      const nextPort = channel.port1;
      port.current?.close();
      port.current = nextPort;
      nextPort.onmessage = message => {
        if (message.data?.type === 'ready') {
          sendSnapshot(nextPort, latest.current, SnapshotMode.Force, sentSnapshot);
          setReady(true);
          return;
        }
        void handleRpc(nextPort, message.data, latest);
      };
      nextPort.start();
      frame.contentWindow?.postMessage({ type: 'quent-port' }, location.origin, [channel.port2]);
    };
    window.addEventListener('message', receive);
    return () => {
      window.removeEventListener('message', receive);
      port.current?.close();
      port.current = undefined;
    };
  }, []);

  useEffect(() => {
    if (!port.current) {
      return;
    }
    sendSnapshot(port.current, latest.current, SnapshotMode.Deduplicate, sentSnapshot);
  }, [revision, capture]);

  return (
    <div className="quent-frame" data-ready={ready}>
      {!ready && <div className="quent-loading">Loading Quent…</div>}
      <iframe ref={iframe} title="Quent" src={new URL('iframe/', document.baseURI).toString()} />
    </div>
  );
}

function sendSnapshot(
  port: MessagePort,
  state: { revision: string; capture?: CaptureSummary },
  mode: SnapshotMode,
  sent: MutableRefObject<string | undefined>,
): void {
  const next = snapshot(state);
  const identity = `${next.revision}:${next.captureId ?? ''}:${next.engineId ?? ''}:${next.lastQueryId ?? ''}`;
  if (mode === SnapshotMode.Deduplicate && sent.current === identity) {
    return;
  }
  sent.current = identity;
  port.postMessage(next);
}

function snapshot(state: { revision: string; capture?: CaptureSummary }): CaptureSnapshot {
  const lastQueryId = state.capture?.queryIds.at(-1);
  return {
    type: 'snapshot',
    revision: state.revision,
    captureId: state.capture?.captureId,
    engineId: state.capture?.engineId,
    lastQueryId,
  };
}

export async function handleRpc(
  port: MessagePort,
  value: unknown,
  latest: MutableRefObject<{ apiClient: ApiClient; revision: string; getRevision(): string; capture?: CaptureSummary }>,
): Promise<void> {
  if (!isRpc(value) || value.revision !== latest.current.getRevision() || !METHODS.has(value.method)) {
    returnRpcError(port, isRpc(value) ? value : undefined, 'Rejected stale or unsupported request');
    return;
  }
  if (!validArgs(value.method, value.args)) {
    returnRpcError(port, value, 'Invalid request arguments');
    return;
  }

  const requestRevision = latest.current.getRevision();
  try {
    const result = await invokeApi(latest.current.apiClient, value.method, value.args);
    if (latest.current.getRevision() !== requestRevision) {
      returnRpcError(port, value, 'Snapshot changed while handling request');
      return;
    }
    port.postMessage({ type: 'rpc-result', id: value.id, revision: requestRevision, result });
  } catch (error) {
    returnRpcError(port, value, error instanceof Error ? error.message : String(error));
  }
}

// The analyzer has no NVTX endpoints; Quent treats their absence as optional data.
export async function invokeApi(client: ApiClient, method: RpcMethod, args: unknown[]): Promise<unknown> {
  if (method === 'fetchNvtxCatalog' || method === 'fetchNvtxViewport') {
    return null;
  }
  const fn = client[method] as (...values: unknown[]) => Promise<unknown>;
  return fn.apply(client, args);
}

function isRpc(value: unknown): value is QuentRpcRequest {
  if (!value || typeof value !== 'object') {
    return false;
  }
  const rpc = value as Partial<QuentRpcRequest>;
  return rpc.type === 'rpc' && typeof rpc.id === 'string' && typeof rpc.revision === 'string'
    && typeof rpc.method === 'string' && Array.isArray(rpc.args);
}

function validArgs(method: RpcMethod, args: unknown[]): boolean {
  const text = (value: unknown): boolean => typeof value === 'string';
  const object = (value: unknown): boolean => value !== null && typeof value === 'object';
  const number = (value: unknown): boolean => typeof value === 'number';
  const bigint = (value: unknown): boolean => typeof value === 'bigint';
  const optionalTextList = (value: unknown): boolean => value === undefined || (Array.isArray(value) && value.every(text));
  const schemas: Record<RpcMethod, ((values: unknown[]) => boolean)> = {
    fetchQueryBundle: values => values.length === 2 && text(values[0]) && text(values[1]),
    fetchListEngines: values => values.length === 0,
    fetchEngineContexts: values => values.length === 1 && text(values[0]),
    fetchNvtxCatalog: values => values.length === 2 && text(values[0]) && bigint(values[1]),
    fetchNvtxViewport: values => values.length === 3 && text(values[0]) && bigint(values[1]) && object(values[2]),
    fetchListCoordinators: values => values.length === 1 && text(values[0]),
    fetchListQueries: values => values.length === 2 && text(values[0]) && text(values[1]),
    fetchSingleTimeline: values => values.length === 3 && text(values[0]) && object(values[1]) && number(values[2]),
    fetchBulkTimelines: values => values.length === 2 && text(values[0]) && object(values[1]),
    fetchEntityList: values => values.length === 2 && text(values[0]) && object(values[1]),
    fetchDataFlow: values => values.length >= 3 && values.length <= 4 && text(values[0]) && text(values[1])
      && object(values[2]) && optionalTextList(values[3]),
  };
  return schemas[method](args);
}

function returnRpcError(port: MessagePort, rpc: QuentRpcRequest | undefined, message: string): void {
  if (!rpc) {
    return;
  }
  port.postMessage({ type: 'rpc-error', id: rpc.id, revision: rpc.revision, error: message });
}
