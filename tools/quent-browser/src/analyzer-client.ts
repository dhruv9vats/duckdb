import type { ApiClient } from '@quent/client';
import { parseJsonWithBigInt } from '@quent/utils';
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
import { WorkerRpc } from './worker-rpc';
import { ANALYSIS_TIMEOUT_MS } from './protocol';

type Method = 'GET' | 'POST';

export class AnalyzerClient implements ApiClient {
  private revision = '0';

  constructor(private readonly rpc: WorkerRpc) {}

  setRevision(revision: string): void {
    this.revision = revision;
  }

  fetchQueryBundle(engineId: string, queryId: string): Promise<QueryBundle<EntityRef>> {
    return this.request('GET', `/api/engines/${engineId}/query/${queryId}`);
  }

  fetchListEngines(): Promise<Engine[]> {
    return this.request('GET', '/api/engines', { with_metadata: true });
  }

  fetchEngineContexts(engineId: string): Promise<EngineContexts> {
    return this.request('GET', `/api/engines/${engineId}/contexts`);
  }

  fetchNvtxCatalog(contextId: string, queryStartUnixNs: bigint): Promise<NvtxCatalog | null> {
    return this.request('GET', `/api/nvtx/contexts/${contextId}/catalog`, {
      query_start: queryStartUnixNs.toString(),
    });
  }

  fetchNvtxViewport(
    contextId: string,
    queryStartUnixNs: bigint,
    request: NvtxViewportRequest,
  ): Promise<NvtxViewportResponse | null> {
    return this.request(
      'POST',
      `/api/nvtx/contexts/${contextId}/viewport`,
      { query_start: queryStartUnixNs.toString() },
      request,
    );
  }

  fetchListCoordinators(engineId: string): Promise<QueryGroup[]> {
    return this.request('GET', `/api/engines/${engineId}/query-groups`);
  }

  fetchListQueries(engineId: string, coordinatorId: string): Promise<Query[]> {
    return this.request('GET', `/api/engines/${engineId}/query_group/${coordinatorId}/queries`);
  }

  fetchSingleTimeline(
    engineId: string,
    request: SingleTimelineRequest<QueryFilter, OperatorFilter>,
    durationSeconds: number,
  ): Promise<SingleTimelineResponse> {
    return this.request('POST', `/api/engines/${engineId}/timeline/single`, { duration: durationSeconds }, request);
  }

  fetchBulkTimelines(
    engineId: string,
    request: BulkTimelineRequest<QueryFilter, OperatorFilter>,
  ): Promise<BulkTimelinesResponse> {
    return this.request('POST', `/api/engines/${engineId}/timeline/bulk`, undefined, request);
  }

  fetchEntityList(
    engineId: string,
    request: EntityListRequest<QueryFilter, OperatorFilter>,
  ): Promise<EntityListResponse> {
    return this.request('POST', `/api/engines/${engineId}/entities`, undefined, request);
  }

  fetchDataFlow(
    engineId: string,
    queryId: string,
    config: TimelineConfig,
    measures: string[] = [],
  ): Promise<DataFlowTimelineBinned | null> {
    return this.request('POST', `/api/engines/${engineId}/timeline/data-flow`, undefined, {
      measures,
      config,
      app_params: { query_id: queryId },
    });
  }

  private request<T>(method: Method, route: string, params?: unknown, body?: unknown): Promise<T> {
    return this.rpc.request<string>(
      'request',
      {
        revision: this.revision,
        method,
        route,
        params,
        body,
      },
      [],
      ANALYSIS_TIMEOUT_MS,
    ).then(response => parseJsonWithBigInt<T>(response));
  }
}
