import type { ApiClient } from '@quent/client';
import type {
	BulkTimelineRequest,
	BulkTimelinesResponse,
	DataFlowTimelineBinned,
	Engine,
	EngineContexts,
	EntityListRequest,
	EntityListResponse,
	EntityRef,
	NvtxCatalog,
	NvtxViewportRequest,
	NvtxViewportResponse,
	OperatorFilter,
	Query,
	QueryBundle,
	QueryFilter,
	QueryGroup,
	SingleTimelineRequest,
	SingleTimelineResponse,
	TimelineConfig,
} from '@quent/utils';
import {
	EMPTY_REVISION,
	isRpcReply,
	type CaptureSnapshot,
	type QuentApiMethod,
	type QuentRpcRequest,
} from './protocol';

interface PendingRequest {
	revision: string;
	timer: ReturnType<typeof setTimeout>;
	resolve(value: unknown): void;
	reject(error: Error): void;
}

const CAPTURE_CHANGED = 'Capture changed';
const CAPTURE_UNAVAILABLE = 'No capture selected';
const RPC_TIMEOUT_MS = 60_000;

export class IframeApiClient implements ApiClient {
	private revision = EMPTY_REVISION;
	private captureId?: string;
	private nextId = 0;
	private readonly pending = new Map<string, PendingRequest>();

	constructor(
		private readonly port: MessagePort,
		private readonly timeoutMs = RPC_TIMEOUT_MS,
	) {
		port.onmessage = event => this.onMessage(event.data);
		port.onmessageerror = () => this.rejectPending('Quent bridge message failed');
		port.start();
	}

	get pendingCount(): number {
		return this.pending.size;
	}

	setSnapshot(snapshot: CaptureSnapshot): void {
		if (snapshot.revision === this.revision && snapshot.captureId === this.captureId) {
			return;
		}

		this.rejectPending(CAPTURE_CHANGED);
		this.revision = snapshot.revision;
		this.captureId = snapshot.captureId;
	}

	close(): void {
		this.rejectPending('Quent bridge closed');
		this.port.onmessage = null;
		this.port.onmessageerror = null;
		this.port.close();
	}

	fetchQueryBundle(engineId: string, queryId: string): Promise<QueryBundle<EntityRef>> {
		return this.request('fetchQueryBundle', engineId, queryId);
	}

	fetchListEngines(): Promise<Engine[]> {
		if (!this.hasCapture()) {
			return Promise.resolve([]);
		}

		return this.request('fetchListEngines');
	}

	fetchEngineContexts(engineId: string): Promise<EngineContexts> {
		return this.request('fetchEngineContexts', engineId);
	}

	fetchNvtxCatalog(contextId: string, queryStartUnixNs: bigint): Promise<NvtxCatalog | null> {
		return this.request('fetchNvtxCatalog', contextId, queryStartUnixNs);
	}

	fetchNvtxViewport(
		contextId: string,
		queryStartUnixNs: bigint,
		request: NvtxViewportRequest,
	): Promise<NvtxViewportResponse | null> {
		return this.request('fetchNvtxViewport', contextId, queryStartUnixNs, request);
	}

	fetchListCoordinators(engineId: string): Promise<QueryGroup[]> {
		return this.request('fetchListCoordinators', engineId);
	}

	fetchListQueries(engineId: string, coordinatorId: string): Promise<Query[]> {
		return this.request('fetchListQueries', engineId, coordinatorId);
	}

	fetchSingleTimeline(
		engineId: string,
		request: SingleTimelineRequest<QueryFilter, OperatorFilter>,
		durationSeconds: number,
	): Promise<SingleTimelineResponse> {
		return this.request('fetchSingleTimeline', engineId, request, durationSeconds);
	}

	fetchBulkTimelines(
		engineId: string,
		request: BulkTimelineRequest<QueryFilter, OperatorFilter>,
	): Promise<BulkTimelinesResponse> {
		return this.request('fetchBulkTimelines', engineId, request);
	}

	fetchEntityList(
		engineId: string,
		request: EntityListRequest<QueryFilter, OperatorFilter>,
	): Promise<EntityListResponse> {
		return this.request('fetchEntityList', engineId, request);
	}

	fetchDataFlow(
		engineId: string,
		queryId: string,
		config: TimelineConfig,
		measures: string[] = [],
	): Promise<DataFlowTimelineBinned | null> {
		return this.request('fetchDataFlow', engineId, queryId, config, measures);
	}

	private hasCapture(): boolean {
		return this.revision !== EMPTY_REVISION && this.captureId !== undefined;
	}

	private request<T>(method: QuentApiMethod, ...args: unknown[]): Promise<T> {
		if (!this.hasCapture()) {
			return Promise.reject(new Error(CAPTURE_UNAVAILABLE));
		}

		const id = String(++this.nextId);
		const revision = this.revision;
		const message: QuentRpcRequest = {
			type: 'rpc',
			id,
			revision,
			method,
			args,
		};

		return new Promise<T>((resolve, reject) => {
			const timer = setTimeout(() => {
				const pending = this.pending.get(id);
				if (!pending) {
					return;
				}

				this.pending.delete(id);
				pending.reject(new Error('Quent API request timed out'));
			}, this.timeoutMs);
			this.pending.set(id, { revision, timer, resolve: value => resolve(value as T), reject });
			try {
				this.port.postMessage(message);
			} catch (error) {
				clearTimeout(timer);
				this.pending.delete(id);
				reject(error instanceof Error ? error : new Error(String(error)));
			}
		});
	}

	private onMessage(value: unknown): void {
		if (!isRpcReply(value)) {
			return;
		}

		const pending = this.pending.get(value.id);
		if (!pending) {
			return;
		}

		this.pending.delete(value.id);
		clearTimeout(pending.timer);
		if (value.revision !== this.revision || value.revision !== pending.revision) {
			pending.reject(new Error(CAPTURE_CHANGED));
			return;
		}
		if (value.type === 'rpc-error') {
			pending.reject(new Error(value.error));
			return;
		}

		pending.resolve(value.result);
	}

	private rejectPending(message: string): void {
		for (const pending of this.pending.values()) {
			clearTimeout(pending.timer);
			pending.reject(new Error(message));
		}
		this.pending.clear();
	}
}
