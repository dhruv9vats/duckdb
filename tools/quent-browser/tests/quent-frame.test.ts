import { describe, expect, it, vi } from 'vitest';
import { handleRpc, invokeApi } from '../src/quent-frame';

describe('Quent iframe adapter', () => {
  it('returns no NVTX data without issuing an unsupported request', async () => {
    const client = { fetchNvtxCatalog: vi.fn() };

    await expect(invokeApi(client as never, 'fetchNvtxCatalog', ['context', 1n])).resolves.toBeNull();
    expect(client.fetchNvtxCatalog).not.toHaveBeenCalled();
  });

  it('keeps the API client receiver for stateful calls', async () => {
    class StatefulClient {
      readonly value = 'bundle';

      fetchQueryBundle(): Promise<string> {
        return Promise.resolve(this.value);
      }
    }

    await expect(invokeApi(new StatefulClient() as never, 'fetchQueryBundle', ['engine', 'query'])).resolves.toBe('bundle');
  });

  it('rejects a response when the session revision changes before a rerender', async () => {
    let revision = '1';
    let resolveRequest: (value: unknown) => void = () => undefined;
    const apiClient = {
      fetchQueryBundle: vi.fn(() => new Promise(resolve => {
        resolveRequest = resolve;
      })),
    };
    const port = { postMessage: vi.fn() };
    const latest = {
      current: { apiClient, revision: '1', getRevision: () => revision },
    };
    const request = handleRpc(port as never, {
      type: 'rpc', id: '1', revision: '1', method: 'fetchQueryBundle', args: ['engine', 'query'],
    }, latest as never);

    revision = '2';
    resolveRequest('old capture');
    await request;

    expect(port.postMessage).toHaveBeenCalledWith(expect.objectContaining({ type: 'rpc-error', revision: '1' }));
  });
});
