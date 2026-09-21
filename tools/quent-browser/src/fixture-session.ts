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
import type { BrowserSession, SessionSnapshot } from './session';

const FIXTURE_ENGINE_ID = 'duckdb-browser';
const FIXTURE_GROUP_ID = 'browser-session';
const FIXTURE_QUERY_ID = 'query-1';

class FixtureApiClient implements ApiClient {
  fetchQueryBundle(): Promise<QueryBundle<EntityRef>> {
    return Promise.resolve(fixtureBundle());
  }

  fetchListEngines(): Promise<Engine[]> {
    return Promise.resolve([{ id: FIXTURE_ENGINE_ID, instance_name: 'DuckDB Browser' }] as Engine[]);
  }

  fetchEngineContexts(): Promise<EngineContexts> {
    return Promise.resolve({} as EngineContexts);
  }

  fetchNvtxCatalog(_contextId: string, _queryStartUnixNs: bigint): Promise<NvtxCatalog | null> {
    return Promise.resolve(null);
  }

  fetchNvtxViewport(
    _contextId: string,
    _queryStartUnixNs: bigint,
    _request: NvtxViewportRequest,
  ): Promise<NvtxViewportResponse | null> {
    return Promise.resolve(null);
  }

  fetchListCoordinators(): Promise<QueryGroup[]> {
    return Promise.resolve([{ id: FIXTURE_GROUP_ID }] as QueryGroup[]);
  }

  fetchListQueries(): Promise<Query[]> {
    return Promise.resolve([{ id: FIXTURE_QUERY_ID }] as Query[]);
  }

  fetchSingleTimeline(
    _engineId: string,
    _request: SingleTimelineRequest<QueryFilter, OperatorFilter>,
    _durationSeconds: number,
  ): Promise<SingleTimelineResponse> {
    const values = [0, 1, 3, 5, 4, 2, 1, 0];
    return Promise.resolve({
      config: { span: { start: 0, end: 1 }, bin_duration: 0.125, num_bins: BigInt(values.length) },
      data: {
        Binned: {
          config: { span: { start: 0, end: 1 }, bin_duration: 0.125, num_bins: BigInt(values.length) },
          capacities_values: { bytes: values.map(value => value * 1024) },
          long_fsms: [],
        },
      },
    } as SingleTimelineResponse);
  }

  fetchBulkTimelines(
    _engineId: string,
    _request: BulkTimelineRequest<QueryFilter, OperatorFilter>,
  ): Promise<BulkTimelinesResponse> {
    return Promise.resolve({ entries: {} } as BulkTimelinesResponse);
  }

  fetchEntityList(
    _engineId: string,
    _request: EntityListRequest<QueryFilter, OperatorFilter>,
  ): Promise<EntityListResponse> {
    return Promise.resolve({
      total: 2,
      items: [
        { entity: fixtureFsm('pipeline-task-1', 'Pipeline task', 0, 0.9), usage_duration_s: 0.9 },
        { entity: fixtureFsm('operator-call-1', 'Aggregate invocation', 0.18, 0.72), usage_duration_s: 0.54 },
      ],
    } as EntityListResponse);
  }

  fetchDataFlow(
    _engineId: string,
    _queryId: string,
    _config: TimelineConfig,
  ): Promise<DataFlowTimelineBinned | null> {
    return Promise.resolve(null);
  }
}

export class FixtureSession implements BrowserSession {
  readonly apiClient: ApiClient = new FixtureApiClient();
  private readonly listeners = new Set<() => void>();
  private current: SessionSnapshot = {
    captures: [],
    status: 'Ready (fixture transport)',
    revision: '0',
    capabilities: { telemetry: true, spill_io: false, threads: false },
    fixture: true,
  };

  snapshot(): SessionSnapshot {
    return this.current;
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  async run(sql: string): Promise<void> {
    const revision = String(Number(this.current.revision) + 1);
    const runId = `fixture-run-${revision}`;
    this.current = { ...this.current, activeRunId: runId, status: 'Running SQL' };
    this.publish();
    await Promise.resolve();
    const capture = {
      captureId: runId,
      runId,
      queryIds: [FIXTURE_QUERY_ID],
      engineId: FIXTURE_ENGINE_ID,
      revision,
      state: 'sealed' as const,
      sql,
      droppedEvents: 0,
      bytes: 4096,
      startedAt: Date.now(),
    };
    this.current = {
      ...this.current,
      activeRunId: undefined,
      captures: [capture, ...this.current.captures],
      result: {
        columns: ['answer', 'rows_scanned'],
        rows: [[1000, 1000]],
        rowCount: 1,
        truncated: false,
      },
      revision,
      status: 'Telemetry ready',
    };
    this.publish();
  }

  async cancel(): Promise<void> {}

  async reset(): Promise<void> {
    this.current = { ...this.current, captures: [], result: undefined, revision: '0', status: 'Ready' };
    this.publish();
  }

  select(captureId: string): void {
    const capture = this.current.captures.find(item => item.captureId === captureId);
    if (capture?.revision === undefined) {
      return;
    }
    this.current = { ...this.current, revision: capture.revision };
    this.publish();
  }

  close(): void {}

  private publish(): void {
    for (const listener of this.listeners) {
      listener();
    }
  }
}

function unsupported<T>(): Promise<T> {
  return Promise.reject(new Error('Fixture endpoint is not implemented'));
}

function fixtureBundle(): QueryBundle<EntityRef> {
  const scan = {
    id: 'operator-scan',
    plan_id: 'plan-1',
    parent_operator_ids: [],
    instance_name: 'range(1000)',
    operator_type_name: 'Table Scan',
    custom_attributes: {},
    statistics: null,
    active_span: null,
  };
  const aggregate = {
    id: 'operator-aggregate',
    plan_id: 'plan-1',
    parent_operator_ids: [],
    instance_name: 'count(*)',
    operator_type_name: 'Aggregate',
    custom_attributes: {},
    statistics: null,
    active_span: null,
  };

  return {
    query_id: FIXTURE_QUERY_ID,
    entities: {
      plans: {
        'plan-1': {
          id: 'plan-1',
          instance_name: 'Physical Plan',
          parent: null,
          worker_id: 'duckdb-worker',
          edges: [{ source: 'port-scan', target: 'port-aggregate' }],
        },
      },
      operators: {
        [scan.id]: scan,
        [aggregate.id]: aggregate,
      },
      ports: {
        'port-scan': { id: 'port-scan', operator_id: scan.id, instance_name: null, statistics: null },
        'port-aggregate': {
          id: 'port-aggregate',
          operator_id: aggregate.id,
          instance_name: null,
          statistics: null,
        },
      },
      workers: {
        'duckdb-worker': {
          id: 'duckdb-worker',
          parent_engine_id: FIXTURE_ENGINE_ID,
          instance_name: 'DuckDB worker',
          start_unix_ns: '9007199254740993',
          end_unix_ns: null,
        },
      },
      resource_types: {
        memory: {
          name: 'memory',
          capacities: [{ name: 'bytes', kind: 'Occupancy', quantity: 'bytes' }],
          used_by: ['Pipeline task', 'Operator invocation'],
        },
      },
      resource_group_types: {},
      resources: {
        'memory-account': {
          id: 'memory-account',
          instance_name: 'Buffer manager memory',
          type_name: 'memory',
          parent_group_id: FIXTURE_ENGINE_ID,
        },
      },
      resource_groups: {},
      fsm_types: {
        'Pipeline task': {
          name: 'Pipeline task',
          states: [{ name: 'queued', usages: [] }, { name: 'running', usages: ['memory'] }],
          transitions: [],
        },
        'Operator invocation': {
          name: 'Operator invocation',
          states: [{ name: 'running', usages: ['memory'] }, { name: 'complete', usages: [] }],
          transitions: [],
        },
      },
      engine: { id: FIXTURE_ENGINE_ID, instance_name: 'DuckDB Browser' },
      query_group: { id: FIXTURE_GROUP_ID },
      query: { id: FIXTURE_QUERY_ID, query_group_id: FIXTURE_GROUP_ID },
    },
    plan_tree: { id: 'plan-1', worker: 'duckdb-worker', children: [] },
    resource_tree: {
      ResourceGroup: {
        id: { QueryGroup: FIXTURE_GROUP_ID },
        children: [{ Resource: { Resource: 'memory-account' } }],
      },
    },
    unique_operator_names: ['Table Scan', 'Aggregate'],
    quantity_specs: {
      bytes: { symbol: 'B', singular: 'byte', plural: 'bytes', occupancy_prefix: 'Iec', rate_prefix: 'Si' },
    },
    start_time_unix_ns: BigInt('9007199254740993'),
    duration_s: 1,
  } as unknown as QueryBundle<EntityRef>;
}

function fixtureFsm(id: string, name: string, start: number, end: number) {
  const usage = { resource: 'memory-account', capacities: [['bytes', BigInt(4096)]] as Array<[string, bigint]> };
  return {
    id,
    type_name: name.includes('task') ? 'Pipeline task' : 'Operator invocation',
    instance_name: name,
    transitions: [
      { name: 'queued', timestamp: start, usages: [], attributes: [], derived_attributes: [] },
      { name: 'running', timestamp: start + 0.1, usages: [usage], attributes: [], derived_attributes: [] },
      { name: 'complete', timestamp: end, usages: [], attributes: [], derived_attributes: [] },
    ],
  };
}
