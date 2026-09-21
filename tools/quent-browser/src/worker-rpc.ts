import { CONTROL_TIMEOUT_MS, type RpcResponse, type WorkerErrorShape } from './protocol';

interface PendingRequest {
  resolve(value: unknown): void;
  reject(reason: Error): void;
  timeout: ReturnType<typeof setTimeout>;
}

export class WorkerRpc {
  private nextId = 1;
  private readonly pending = new Map<number, PendingRequest>();

  constructor(private readonly worker: Worker) {
    worker.addEventListener('message', this.onMessage);
    worker.addEventListener('error', this.onError);
  }

  request<T>(
    op: string,
    fields: Record<string, unknown> = {},
    transfer: Transferable[] = [],
    timeoutMs = CONTROL_TIMEOUT_MS,
  ): Promise<T> {
    const id = this.nextId++;

    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Worker request timed out: ${op}`));
      }, timeoutMs);

      this.pending.set(id, {
        resolve: value => resolve(value as T),
        reject,
        timeout,
      });
      this.worker.postMessage({ id, op, ...fields }, transfer);
    });
  }

  cancel(requestId: number): void {
    this.worker.postMessage({ id: this.nextId++, op: 'cancel', request_id: requestId });
  }

  close(): void {
    this.worker.removeEventListener('message', this.onMessage);
    this.worker.removeEventListener('error', this.onError);

    for (const request of this.pending.values()) {
      clearTimeout(request.timeout);
      request.reject(new Error('Worker closed'));
    }
    this.pending.clear();
  }

  private readonly onMessage = (event: MessageEvent<RpcResponse<unknown>>): void => {
    const response = event.data;
    if (typeof response?.id !== 'number' || typeof response?.ok !== 'boolean') {
      return;
    }

    const request = this.pending.get(response.id);
    if (!request) {
      return;
    }

    clearTimeout(request.timeout);
    this.pending.delete(response.id);
    if (response.ok) {
      request.resolve(response.result);
      return;
    }

    request.reject(toError(response.error));
  };

  private readonly onError = (event: ErrorEvent): void => {
    const error = new Error(event.message || 'Worker failed');
    for (const request of this.pending.values()) {
      clearTimeout(request.timeout);
      request.reject(error);
    }
    this.pending.clear();
  };
}

function toError(error: WorkerErrorShape): Error {
  const result = new Error(`${error.code}: ${error.message}`);
  result.name = error.code;
  return result;
}
