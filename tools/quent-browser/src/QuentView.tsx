import { useEntityList, useQueryBundle } from '@quent/client';
import {
  DAGChart,
  LongEntitiesGantt,
  ResourceTimeline,
  buildLongEntityEntries,
  getPlanDAG,
  getTreeData,
} from '@quent/components';
import { useHydrateTimelineAtoms, useSetBulkInitialized, useZoomRange } from '@quent/hooks';
import { useEffect, useMemo, useState, type ReactNode } from 'react';

interface QuentViewProps {
  engineId: string;
  queryId: string;
}

export function QuentView({ engineId, queryId }: QuentViewProps) {
  const [tab, setTab] = useState<'plan' | 'timelines' | 'entities'>('plan');
  const bundle = useQueryBundle({ engineId, queryId });
  const dag = useMemo(() => {
    if (!bundle.data) {
      return undefined;
    }
    const plan = getPlanDAG(bundle.data, bundle.data.plan_tree.id);
    return {
      ...plan,
      nodes: plan.nodes.map(node => ({ ...node, type: nodeType(node.type) })),
      queryData: getTreeData(bundle.data),
      quantitySpecs: bundle.data.quantity_specs,
    };
  }, [bundle.data]);

  if (bundle.isLoading) {
    return <div className="empty">Analyzing query…</div>;
  }
  if (bundle.error) {
    return <div className="error">{bundle.error.message}</div>;
  }
  if (!dag || !bundle.data) {
    return <div className="empty">No plan available</div>;
  }

  const operators = Object.values(bundle.data.entities.operators).filter(item => item !== undefined);

  return (
    <TimelineHydration bundle={bundle.data}>
      <div className="quent-view" data-testid="quent-view" data-node-count={dag.nodes.length}>
        <nav className="quent-tabs" aria-label="Telemetry views">
          {(['plan', 'timelines', 'entities'] as const).map(value => (
            <button key={value} data-active={tab === value} onClick={() => setTab(value)}>{value}</button>
          ))}
        </nav>
        <div className="quent-content">
          {tab === 'plan' && <DAGChart data={dag} height="100%" isDark={false} operators={operators} />}
          {tab === 'timelines' && <RuntimeTimelines engineId={engineId} queryId={queryId} bundle={bundle.data} />}
          {tab === 'entities' && <RuntimeEntities engineId={engineId} queryId={queryId} bundle={bundle.data} />}
        </div>
      </div>
    </TimelineHydration>
  );
}

function TimelineHydration({ bundle, children }: { bundle: QuentRuntimeProps['bundle']; children: ReactNode }) {
  const zoomRange = { start: 0, end: bundle.duration_s };
  const setBulkInitialized = useSetBulkInitialized();
  useHydrateTimelineAtoms({
    zoomRange,
    debouncedZoomRange: zoomRange,
    startTimeMs: nanosToMs(bundle.start_time_unix_ns),
  });
  useEffect(() => {
    setBulkInitialized(true);
  }, [setBulkInitialized]);
  return children;
}

function nanosToMs(value: bigint | number): number {
  if (typeof value === 'number') {
    return value / 1_000_000;
  }
  return Number(value / 1_000_000n) + Number(value % 1_000_000n) / 1_000_000;
}

function nodeType(value: string): string {
  const type = value.toLowerCase().replaceAll(/[^a-z]/g, '');
  const exact = new Set([
    'source', 'scan', 'join', 'joinlocal', 'joinpartition', 'filesystemscan',
    'aggregate', 'exchange', 'output', 'stage', 'local', 'project', 'filter',
    'sort', 'limit', 'union', 'other', 'default',
  ]);
  if (exact.has(type)) {
    return type;
  }
  if (type.includes('scan')) {
    return 'scan';
  }
  if (type.includes('join')) {
    return 'join';
  }
  if (type.includes('aggregate')) {
    return 'aggregate';
  }
  if (type.includes('project')) {
    return 'project';
  }
  if (type.includes('filter')) {
    return 'filter';
  }
  if (type.includes('sort') || type.includes('order')) {
    return 'sort';
  }
  if (type.includes('limit')) {
    return 'limit';
  }

  return 'other';
}

function RuntimeTimelines({ engineId, queryId, bundle }: QuentRuntimeProps) {
  const resources = Object.values(bundle.entities.resources).filter(item => item !== undefined);
  const [selectedId, setSelectedId] = useState(resources[0]?.id ?? '');
  const zoomRange = useZoomRange();
  const selected = resources.find(resource => resource.id === selectedId) ?? resources[0];
  if (resources.length === 0) {
    return <div className="empty">This build reported no timeline resources.</div>;
  }

  return (
    <div
      className="telemetry-stack"
      data-testid="runtime-timelines"
      data-resource-count={resources.length}
      data-zoom-span={zoomRange.end - zoomRange.start}
    >
      <label className="resource-picker">
        Resource
        <select value={selected?.id} onChange={event => setSelectedId(event.target.value)}>
          {resources.map(resource => <option key={resource.id} value={resource.id}>{resource.instance_name}</option>)}
        </select>
      </label>
      {selected && (
        <article className="timeline-card" key={selected.id}>
          <strong>{selected.instance_name}</strong>
          <span>{selected.type_name}</span>
          <ResourceTimeline
            engineId={engineId}
            queryId={queryId}
            resourceId={selected.id}
            resourceType="Resource"
            durationSeconds={bundle.duration_s}
            resourceTypeDecl={bundle.entities.resource_types[selected.type_name]}
            quantitySpecs={bundle.quantity_specs}
            fsmTypes={bundle.entities.fsm_types}
            isDark={false}
          />
        </article>
      )}
    </div>
  );
}

function RuntimeEntities({ engineId, queryId, bundle }: QuentRuntimeProps) {
  const response = useEntityList({
    engineId,
    queryId,
    window: { start: 0, end: bundle.duration_s },
    maxItems: 100,
  });
  const entries = useMemo(
    () => buildLongEntityEntries(response.data?.items.map(item => item.entity) ?? [], bundle.entities.fsm_types, 'light'),
    [bundle.entities.fsm_types, response.data],
  );

  if (response.isLoading) {
    return <div className="empty">Loading runtime entities…</div>;
  }
  if (response.error) {
    return <div className="error">{response.error.message}</div>;
  }

  return (
    <div className="entity-view" data-testid="runtime-entities" data-entity-count={response.data?.total ?? 0}>
      <div className="entity-summary">{response.data?.total ?? 0} runtime entities</div>
      <div className="entity-labels">
        {response.data?.items.map(item => (
          <span key={item.entity.id}>
            <strong>{item.entity.instance_name || item.entity.type_name}</strong>
            {' · '}{item.entity.type_name}{' · '}{item.entity.transitions.length} transitions
          </span>
        ))}
      </div>
      <LongEntitiesGantt
        entries={entries}
        durationSeconds={bundle.duration_s}
        minUsageSeconds={0}
        height={Math.max(180, entries.length * 26)}
        isDark={false}
        noUsagesInRange={entries.length === 0}
      />
    </div>
  );
}

interface QuentRuntimeProps {
  engineId: string;
  queryId: string;
  bundle: NonNullable<ReturnType<typeof useQueryBundle>['data']>;
}
