import { afterEach, describe, expect, it, vi } from 'vitest';
import { IframeApiClient } from './rpc-client';

class TestPort {
	readonly posted: unknown[] = [];
	onmessage: ((event: MessageEvent) => void) | null = null;

	postMessage(message: unknown): void {
		this.posted.push(message);
	}

	start(): void {}
	close(): void {}

	reply(message: unknown): void {
		this.onmessage?.({ data: message } as MessageEvent);
	}
}

describe('IframeApiClient', () => {
	afterEach(() => vi.useRealTimers());

	it('does not send discovery RPC before a capture exists', async () => {
		const port = new TestPort();
		const client = new IframeApiClient(port as unknown as MessagePort);

		expect(await client.fetchListEngines()).toEqual([]);
		expect(port.posted).toEqual([]);
	});

	it('preserves bigint values through the message port', async () => {
		const port = new TestPort();
		const client = new IframeApiClient(port as unknown as MessagePort);
		client.setSnapshot({ type: 'snapshot', revision: 'r1', captureId: 'c1', engineId: 'e1', lastQueryId: 'q1' });

		const request = client.fetchNvtxCatalog('ctx', 42n);
		const sent = port.posted[0] as { id: string; args: unknown[] };
		expect(sent.args[1]).toBe(42n);

		port.reply({
			type: 'rpc-result',
			id: sent.id,
			revision: 'r1',
			result: { count: 7n },
		});

		await expect(request).resolves.toEqual({ count: 7n });
	});

	it('rejects pending and stale results after a revision switch', async () => {
		const port = new TestPort();
		const client = new IframeApiClient(port as unknown as MessagePort);
		client.setSnapshot({ type: 'snapshot', revision: 'r1', captureId: 'c1', engineId: 'e1', lastQueryId: 'q1' });
		const request = client.fetchQueryBundle('e1', 'q1');
		const sent = port.posted[0] as { id: string };

		client.setSnapshot({ type: 'snapshot', revision: 'r2', captureId: 'c2', engineId: 'e2', lastQueryId: 'q2' });
		await expect(request).rejects.toThrow('Capture changed');

		port.reply({
			type: 'rpc-result',
			id: sent.id,
			revision: 'r1',
			result: {},
		});
		expect(client.pendingCount).toBe(0);
	});

	it('reports parent RPC errors', async () => {
		const port = new TestPort();
		const client = new IframeApiClient(port as unknown as MessagePort);
		client.setSnapshot({ type: 'snapshot', revision: 'r1', captureId: 'c1', engineId: 'e1', lastQueryId: 'q1' });
		const request = client.fetchEngineContexts('e1');
		const sent = port.posted[0] as { id: string };

		port.reply({
			type: 'rpc-error',
			id: sent.id,
			revision: 'r1',
			error: 'broken',
		});

		await expect(request).rejects.toThrow('broken');
	});

	it('times out unanswered RPCs', async () => {
		vi.useFakeTimers();
		const port = new TestPort();
		const client = new IframeApiClient(port as unknown as MessagePort, 10);
		client.setSnapshot({ type: 'snapshot', revision: 'r1', captureId: 'c1', engineId: 'e1', lastQueryId: 'q1' });
		const request = client.fetchEngineContexts('e1');
		const rejection = expect(request).rejects.toThrow('timed out');

		await vi.advanceTimersByTimeAsync(10);

		await rejection;
		expect(client.pendingCount).toBe(0);
	});

	it('rejects pending RPCs when closed', async () => {
		const port = new TestPort();
		const client = new IframeApiClient(port as unknown as MessagePort);
		client.setSnapshot({ type: 'snapshot', revision: 'r1', captureId: 'c1', engineId: 'e1', lastQueryId: 'q1' });
		const request = client.fetchEngineContexts('e1');

		client.close();

		await expect(request).rejects.toThrow('closed');
		expect(client.pendingCount).toBe(0);
	});
});
