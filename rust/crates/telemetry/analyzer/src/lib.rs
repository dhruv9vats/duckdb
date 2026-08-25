use std::collections::{BTreeMap, HashMap, HashSet};

use duckdb_telemetry_model::{DuckDB, DuckDBEvent, chunk_transfer::ChunkTransferTransition};
use quent_analyzer::{
    AnalyzerError, AnalyzerResult, Entity, Span,
    fsm::{FsmTypeDeclaration, FsmUsages, collection::FsmCollection},
    resource::{Usage, Using, collection::ResourceCollection, tree::ResourceTreeNode},
    timeline::binned::resource::{
        ResourceTimeline, ResourceTimelineBuilder, ResourceTimelineByKey,
        ResourceTimelineByKeyBuilder,
    },
};
use quent_events::Event;
pub use quent_query_engine_analyzer::QueryEngineModel;
use quent_query_engine_analyzer::entities;
use quent_query_engine_analyzer::ui::{QuentViewer, UiAnalyzer, ViewerEventStream};
use quent_query_engine_analyzer::{
    EngineEntity, OperatorEntity, PlanEntity, PortEntity, QueryEntity, QueryGroupEntity,
    WorkerEntity,
};
use quent_query_engine_ui::{
    DataFlowTimelineBinned, OperatorFilter, QueryBundle, QueryEntities, QueryFilter,
};
use quent_simulator_ui::EntityRef;
use quent_time::{TimeUnixNanoSec, to_nanosecs, to_secs};
use quent_ui::{
    FiniteStateMachine, ResourceGroupNode, ResourceTree, convert_resource_tree,
    entities::{request::EntityListRequest, response::EntityListResponse},
    quantity::{CapacityKind, PrefixSystem, QuantitySpec},
    timeline::{
        categorical::{
            CategoricalDecl, CategoricalSeries, CategoricalTimelineRequest, DimensionKeyDecl,
            MeasureDecl,
        },
        request::{BulkTimelineRequest, SingleTimelineRequest, TimelineRequest},
        response::{
            BulkTimelinesResponse, BulkTimelinesResponseEntry,
            ResourceTimeline as UiResourceTimeline, ResourceTimelineBinned,
            ResourceTimelineBinnedByState, SingleTimelineResponse,
        },
    },
};
use uuid::Uuid;

use crate::{
    chunk_transfer::{ChunkTransfer, ChunkTransferExt},
    model::{DuckDbModel, DuckDbModelBuilder},
    operator_invocation::{OperatorInvocation, OperatorInvocationExt},
    pipeline_task::{PipelineTask, PipelineTaskExt},
};

pub mod chunk_transfer;
pub mod model;
pub mod operator_invocation;
pub mod pipeline_task;

const PIPELINE_TASK_TYPE_NAME: &str = "pipeline_task";
const CHUNK_TRANSFER_TYPE_NAME: &str = "chunk_transfer";
const OPERATOR_INVOCATION_TYPE_NAME: &str = "operator_invocation";
const MEASURE_CHUNKS: &str = "chunks";
const MEASURE_ROWS: &str = "rows";
const MEASURE_LOGICAL_BYTES: &str = "logical_bytes";
const DATA_FLOW_STATE: &str = "published";

/// `quent-open` entry point for DuckDB telemetry directories.
pub struct Viewer;

impl QuentViewer for Viewer {
    type Analyzer = DuckDbUiAnalyzer;

    fn import_events(
        dir: &std::path::Path,
    ) -> quent_model::io::ImporterResult<ViewerEventStream<Self::Analyzer>> {
        DuckDB::import_events(dir)
    }
}

/// Query-plan analyzer for the first DuckDB telemetry milestone.
pub struct DuckDbUiAnalyzer {
    pub model: DuckDbModel,
}

struct PipelineTaskCollection<'a>(&'a std::collections::HashMap<Uuid, PipelineTask>);

impl FsmCollection for PipelineTaskCollection<'_> {
    type Fsm = PipelineTask;

    fn fsms(&self) -> impl Iterator<Item = &PipelineTask> {
        self.0.values()
    }
}

struct ChunkTransferCollection<'a>(&'a std::collections::HashMap<Uuid, ChunkTransfer>);

impl FsmCollection for ChunkTransferCollection<'_> {
    type Fsm = ChunkTransfer;

    fn fsms(&self) -> impl Iterator<Item = &ChunkTransfer> {
        self.0.values()
    }
}

struct OperatorInvocationCollection<'a>(&'a std::collections::HashMap<Uuid, OperatorInvocation>);

impl FsmCollection for OperatorInvocationCollection<'_> {
    type Fsm = OperatorInvocation;

    fn fsms(&self) -> impl Iterator<Item = &OperatorInvocation> {
        self.0.values()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkEdgeSummary {
    pub source_operator_id: Uuid,
    pub source_port_id: Uuid,
    pub target_operator_id: Uuid,
    pub target_port_id: Uuid,
    pub transfers: u64,
    pub rows: u64,
    pub logical_bytes: u64,
}

struct QueryTopology {
    plan_ids: HashSet<Uuid>,
    plan_workers: HashMap<Uuid, Uuid>,
    operator_plans: HashMap<Uuid, Uuid>,
    port_operators: HashMap<Uuid, Uuid>,
    plan_edges: HashSet<(Uuid, Uuid)>,
}

impl UiAnalyzer for DuckDbUiAnalyzer {
    type Event = DuckDBEvent;
    type EntityRef = EntityRef;

    fn try_new(
        engine_id: Uuid,
        events: impl Iterator<Item = Event<DuckDBEvent>>,
    ) -> AnalyzerResult<Self> {
        let mut builder = DuckDbModelBuilder::try_new(engine_id)?;
        for event in events {
            builder.try_push(event)?;
        }
        let model = builder.try_build()?;
        tracing::info!(
            workers = model.query_engine.workers.len(),
            query_groups = model.query_engine.query_groups.len(),
            queries = model.query_engine.queries.len(),
            plans = model.query_engine.plans.len(),
            operators = model.query_engine.operators.len(),
            ports = model.query_engine.ports.len(),
            pipeline_tasks = model.pipeline_tasks.len(),
            chunk_transfers = model.chunk_transfers.len(),
            operator_invocations = model.operator_invocations.len(),
            resources = model.runtime_resources.resources.len(),
            "built DuckDB query-engine model"
        );
        Ok(Self { model })
    }

    fn extract_engine(
        engine_id: Uuid,
        events: impl Iterator<Item = Event<DuckDBEvent>>,
    ) -> AnalyzerResult<quent_query_engine_ui::Engine> {
        use quent_query_engine_model::engine::EngineEvent;
        for event in events {
            if let DuckDBEvent::Engine(EngineEvent::Init(init)) = event.data {
                return Ok(quent_query_engine_ui::Engine {
                    id: engine_id,
                    start_time_unix_ns: Some(event.timestamp),
                    duration_s: None,
                    instance_name: init.instance_name,
                    implementation: Some(
                        quent_query_engine_ui::EngineImplementationAttributes::from(
                            &init.implementation,
                        ),
                    ),
                });
            }
        }
        Ok(quent_query_engine_ui::Engine::new(engine_id))
    }

    fn query_bundle(&self, query_id: Uuid) -> AnalyzerResult<QueryBundle<EntityRef>> {
        let view = self.model.query_engine.query_view(query_id)?;
        let model_query = view.query(query_id)?;
        let epoch = view.query_epoch(query_id)?;
        let duration_s = to_secs(model_query.span()?.duration());

        let engine = view.engine()?.to_ui()?;
        let query_group_id = model_query.query_group_id().ok_or_else(|| {
            AnalyzerError::IncompleteEntity(format!("query {query_id} has no query group"))
        })?;
        let task_decl = PipelineTask::fsm_type_declaration();
        let transfer_decl = ChunkTransfer::fsm_type_declaration();
        let invocation_decl = OperatorInvocation::fsm_type_declaration();
        let fsm_types = [
            (task_decl.name.clone(), task_decl),
            (transfer_decl.name.clone(), transfer_decl),
            (invocation_decl.name.clone(), invocation_decl),
        ]
        .into_iter()
        .collect();
        let entities = QueryEntities {
            engine,
            query_group: view.query_group(query_group_id)?.to_ui(),
            query: model_query.to_ui()?,
            workers: view.workers().map(|v| (v.id(), v.to_ui(epoch))).collect(),
            plans: view.plans().map(|v| (v.id(), v.to_ui())).collect(),
            operators: view.operators().map(|v| (v.id(), v.to_ui(epoch))).collect(),
            ports: view.ports().map(|v| (v.id(), v.to_ui(epoch))).collect(),
            resource_types: self
                .model
                .runtime_resources
                .resource_types
                .iter()
                .map(|(name, resource_type)| (name.clone(), resource_type.into()))
                .collect(),
            resource_group_types: self
                .model
                .resource_group_types
                .iter()
                .map(|(name, group_type)| (name.clone(), group_type.into()))
                .collect(),
            resources: self
                .model
                .runtime_resources
                .resources
                .values()
                .map(|resource| (resource.id(), resource.into()))
                .collect(),
            resource_groups: self
                .model
                .runtime_resources
                .resource_groups
                .values()
                .map(|group| {
                    let group: &dyn quent_analyzer::resource::ResourceGroup = group;
                    (group.id(), group.into())
                })
                .collect(),
            fsm_types,
        };
        let unique_operator_names = view
            .operators()
            .filter_map(|operator| operator.operator_type_name().map(str::to_owned))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let resource_tree = convert_resource_tree(
            ResourceTreeNode::try_new(&self.model, view.engine()?.id())?,
            &self.model,
        )?
        .unwrap_or_else(|| {
            ResourceTree::ResourceGroup(ResourceGroupNode {
                id: EntityRef::Engine(view.engine().unwrap().id()),
                children: vec![],
            })
        });

        Ok(QueryBundle {
            query_id,
            entities,
            plan_tree: view.plan_tree(query_id)?.to_ui(),
            resource_tree,
            unique_operator_names,
            quantity_specs: [
                (
                    crate::model::QUEUE_ENTRIES_CAPACITY_NAME.to_owned(),
                    QuantitySpec::unit(),
                ),
                ("unit".to_owned(), QuantitySpec::unit()),
                (
                    MEASURE_CHUNKS.to_owned(),
                    QuantitySpec {
                        symbol: String::new(),
                        singular: "chunk".to_owned(),
                        plural: "chunks".to_owned(),
                        occupancy_prefix: PrefixSystem::None,
                        rate_prefix: PrefixSystem::Si,
                    },
                ),
                (
                    MEASURE_ROWS.to_owned(),
                    QuantitySpec {
                        symbol: String::new(),
                        singular: "row".to_owned(),
                        plural: "rows".to_owned(),
                        occupancy_prefix: PrefixSystem::None,
                        rate_prefix: PrefixSystem::Si,
                    },
                ),
                (MEASURE_LOGICAL_BYTES.to_owned(), QuantitySpec::bytes()),
            ]
            .into(),
            start_time_unix_ns: epoch,
            duration_s,
        })
    }

    fn query_engine_model(&self) -> &impl QueryEngineModel {
        &self.model
    }

    fn single_resource_timeline(
        &self,
        request: SingleTimelineRequest<QueryFilter, OperatorFilter>,
    ) -> AnalyzerResult<SingleTimelineResponse> {
        self.build_single_timeline(request)
    }

    fn list_entities(
        &self,
        request: EntityListRequest<QueryFilter, OperatorFilter>,
    ) -> AnalyzerResult<EntityListResponse> {
        let query_id = request.app_params.query_id;
        let epoch = self.model.query_epoch(query_id)?;
        let topology = self.query_topology(query_id)?;
        let task_operators = self.query_tasks(query_id, &topology);
        let task_ids = task_operators.keys().copied().collect();
        let entry = request.entry;
        let window = entry.window.try_into_span(epoch)?;
        let scope = entry
            .filter
            .scope
            .as_ref()
            .map(|scope| scope.resolve(&self.model))
            .transpose()?;
        let filter = entry.application;
        let query = entities::ListQuery {
            scope: scope.as_ref(),
            window,
            filter: &entry.filter,
            sort: entry.sort,
            page: entry.page,
            epoch,
        };

        match entry.filter.entity_type_name.as_deref() {
            None | Some(PIPELINE_TASK_TYPE_NAME) => entities::list_entities(
                &PipelineTaskCollection(&self.model.pipeline_tasks),
                |task| {
                    task.query_id() == Some(query_id)
                        && task.is_complete()
                        && task.belongs_to_query(
                            &topology.operator_plans,
                            &topology.plan_workers,
                            &topology.plan_ids,
                        )
                        && task.resources_are_valid(&self.model.runtime_resources)
                        && task.matches_operator(&filter)
                        && task.span().is_ok_and(|span| span.intersects(&window))
                },
                query,
            ),
            Some(CHUNK_TRANSFER_TYPE_NAME) => entities::list_entities(
                &ChunkTransferCollection(&self.model.chunk_transfers),
                |transfer| {
                    transfer.query_id() == Some(query_id)
                        && transfer.is_complete()
                        && transfer.belongs_to_query(
                            &topology.port_operators,
                            &topology.plan_edges,
                            &task_ids,
                        )
                        && transfer.matches_operator(&filter)
                        && transfer.span().is_ok_and(|span| span.intersects(&window))
                },
                query,
            ),
            Some(OPERATOR_INVOCATION_TYPE_NAME) => entities::list_entities(
                &OperatorInvocationCollection(&self.model.operator_invocations),
                |invocation| {
                    invocation.query_id() == Some(query_id)
                        && invocation.is_complete()
                        && invocation.belongs_to_query(
                            &topology.operator_plans,
                            &topology.plan_workers,
                            &topology.plan_ids,
                            &task_operators,
                        )
                        && invocation.resources_are_valid(
                            &topology.plan_workers,
                            &self.model.runtime_resources,
                        )
                        && invocation.matches_operator(&filter)
                        && invocation.span().is_ok_and(|span| span.intersects(&window))
                },
                query,
            ),
            Some(type_name) => Err(AnalyzerError::InvalidArgument(format!(
                "unknown DuckDB entity type {type_name:?}"
            ))),
        }
    }

    fn data_flow_timeline(
        &self,
        request: CategoricalTimelineRequest<QueryFilter>,
    ) -> AnalyzerResult<DataFlowTimelineBinned> {
        if let Some(unknown) = request.measures.iter().find(|measure| {
            !matches!(
                measure.as_str(),
                MEASURE_CHUNKS | MEASURE_ROWS | MEASURE_LOGICAL_BYTES
            )
        }) {
            return Err(AnalyzerError::InvalidArgument(format!(
                "unknown measure {unknown:?}; declared measures are {MEASURE_CHUNKS:?}, \
                 {MEASURE_ROWS:?}, and {MEASURE_LOGICAL_BYTES:?}"
            )));
        }

        let query_id = request.app_params.query_id;
        let epoch = self.model.query_epoch(query_id)?;
        let config = request.config.try_into_binned_span(epoch)?;
        let topology = self.query_topology(query_id)?;
        let task_ids = self
            .query_tasks(query_id, &topology)
            .into_keys()
            .collect::<HashSet<_>>();
        let operator_names = self
            .model
            .query_engine
            .query_view(query_id)?
            .operators()
            .map(|operator| (operator.id(), operator.instance_name().to_owned()))
            .collect::<HashMap<_, _>>();

        let wants = |name: &str| {
            request.measures.is_empty() || request.measures.iter().any(|value| value == name)
        };
        let num_bins = config.num_bins().get() as usize;
        let bin_duration_s = to_secs(config.bin_duration().get());
        let mut dimensions = BTreeMap::<String, String>::new();
        let mut operators = HashMap::<Uuid, CategoricalSeries>::new();

        for transfer in self.model.chunk_transfers.values() {
            if transfer.query_id() != Some(query_id)
                || !transfer.is_complete()
                || !transfer.belongs_to_query(
                    &topology.port_operators,
                    &topology.plan_edges,
                    &task_ids,
                )
            {
                continue;
            }
            let Some(publication) = transfer.publication() else {
                continue;
            };
            let Some(bin) = config.index_of(publication.timestamp) else {
                continue;
            };

            let dimension = publication.source_operator_id.to_string();
            dimensions.entry(dimension.clone()).or_insert_with(|| {
                operator_names
                    .get(&publication.source_operator_id)
                    .cloned()
                    .unwrap_or_else(|| publication.source_operator_id.to_string())
            });
            let series = operators.entry(publication.target_operator_id).or_default();
            for (measure, value) in [
                (MEASURE_CHUNKS, 1.0),
                (MEASURE_ROWS, publication.rows as f64),
                (MEASURE_LOGICAL_BYTES, publication.logical_bytes as f64),
            ] {
                if !wants(measure) {
                    continue;
                }
                let bins = series
                    .values
                    .entry(measure.to_owned())
                    .or_default()
                    .entry(DATA_FLOW_STATE.to_owned())
                    .or_default()
                    .entry(dimension.clone())
                    .or_insert_with(|| vec![0.0; num_bins]);
                bins[bin as usize] += value / bin_duration_s;
            }
        }

        let mut measures = Vec::new();
        if wants(MEASURE_CHUNKS) {
            measures.push(MeasureDecl {
                name: MEASURE_CHUNKS.to_owned(),
                display_name: "Chunk publications".to_owned(),
                quantity: MEASURE_CHUNKS.to_owned(),
                kind: CapacityKind::Rate,
            });
        }
        if wants(MEASURE_ROWS) {
            measures.push(MeasureDecl {
                name: MEASURE_ROWS.to_owned(),
                display_name: "Published rows".to_owned(),
                quantity: MEASURE_ROWS.to_owned(),
                kind: CapacityKind::Rate,
            });
        }
        if wants(MEASURE_LOGICAL_BYTES) {
            measures.push(MeasureDecl {
                name: MEASURE_LOGICAL_BYTES.to_owned(),
                display_name: "Published logical bytes".to_owned(),
                quantity: MEASURE_LOGICAL_BYTES.to_owned(),
                kind: CapacityKind::Rate,
            });
        }
        let default_measure = measures
            .iter()
            .find(|measure| measure.name == MEASURE_LOGICAL_BYTES)
            .or_else(|| measures.first())
            .map(|measure| measure.name.clone());

        Ok(DataFlowTimelineBinned {
            config: config.try_to_secs_relative(epoch)?,
            decl: CategoricalDecl {
                entity_type_name: ChunkTransfer::fsm_type_declaration().name,
                dimension_name: "Upstream operator".to_owned(),
                dimension_keys: dimensions
                    .into_iter()
                    .map(|(key, display_name)| DimensionKeyDecl { key, display_name })
                    .collect(),
                measures,
                default_measure,
            },
            operators,
        })
    }

    fn bulk_resource_timeline(
        &self,
        request: BulkTimelineRequest<QueryFilter, OperatorFilter>,
    ) -> AnalyzerResult<BulkTimelinesResponse> {
        let entries = request
            .entries
            .into_iter()
            .map(|(key, entry)| {
                let response = self.build_single_timeline(SingleTimelineRequest {
                    entry,
                    app_params: request.app_params.clone(),
                });
                let entry = match response {
                    Ok(response) => BulkTimelinesResponseEntry::Ok {
                        message: String::new(),
                        config: response.config,
                        data: response.data,
                    },
                    Err(error) => BulkTimelinesResponseEntry::Error {
                        message: error.to_string(),
                    },
                };
                (key, entry)
            })
            .collect();
        Ok(BulkTimelinesResponse { entries })
    }
}

impl DuckDbUiAnalyzer {
    fn build_single_timeline(
        &self,
        request: SingleTimelineRequest<QueryFilter, OperatorFilter>,
    ) -> AnalyzerResult<SingleTimelineResponse> {
        let query_id = request.app_params.query_id;
        let epoch = self.model.query_epoch(query_id)?;
        let topology = self.query_topology(query_id)?;
        let task_operators = self.query_tasks(query_id, &topology);
        let config = request.entry.config().try_into_binned_span(epoch)?;
        let config_secs = config.try_to_secs_relative(epoch)?;

        let (resource_type, resource_ids, entity_filter, operator_filter, threshold) =
            match request.entry {
                TimelineRequest::Resource(resource) => (
                    self.model.resource_type_of(resource.resource_id)?,
                    [resource.resource_id].into_iter().collect(),
                    resource.entity_filter,
                    resource.application,
                    resource.long_entities_threshold_s.map(to_nanosecs),
                ),
                TimelineRequest::ResourceGroup(group) => {
                    let tree = ResourceTreeNode::try_new(&self.model, group.resource_group_id)?;
                    let mut available_types = tree
                        .iter_leaf_ids()
                        .filter_map(|resource_id| {
                            self.model
                                .resource(resource_id)
                                .ok()
                                .map(|resource| resource.type_name().to_owned())
                        })
                        .collect::<Vec<_>>();
                    available_types.sort_unstable();
                    available_types.dedup();

                    // Some Quent UI paths omit the group type while their
                    // resource selector is initializing. Resolve that request
                    // deterministically from the group's actual resources.
                    let resource_type_name = if group.resource_type_name.is_empty() {
                        available_types.first().ok_or_else(|| {
                            AnalyzerError::InvalidArgument(format!(
                                "resource group {} contains no resources",
                                group.resource_group_id
                            ))
                        })?
                    } else {
                        &group.resource_type_name
                    };
                    let resource_type = self.model.resource_type(resource_type_name)?;
                    let resource_ids = tree
                        .iter_leaf_ids()
                        .filter(|resource_id| {
                            self.model
                                .resource(*resource_id)
                                .ok()
                                .is_some_and(|resource| resource.type_name() == resource_type_name)
                        })
                        .collect::<HashSet<_>>();
                    (
                        resource_type,
                        resource_ids,
                        group.entity_filter,
                        group.app_params,
                        group.long_entities_threshold_s.map(to_nanosecs),
                    )
                }
            };

        let data = if let Some(entity_type) = entity_filter.entity_type_name.as_deref() {
            let mut builder =
                ResourceTimelineByKeyBuilder::try_new(resource_type, config, threshold)?;
            match entity_type {
                PIPELINE_TASK_TYPE_NAME => {
                    for task in self.model.pipeline_tasks.values().filter(|task| {
                        self.task_matches(task, query_id, &topology, &operator_filter)
                    }) {
                        for (state, usage) in task.usages_with_state_names() {
                            if resource_ids.contains(&usage.resource_id()) {
                                builder.try_push(state, &usage)?;
                            }
                        }
                    }
                }
                OPERATOR_INVOCATION_TYPE_NAME => {
                    for invocation in
                        self.model
                            .operator_invocations
                            .values()
                            .filter(|invocation| {
                                self.invocation_matches(
                                    invocation,
                                    query_id,
                                    &topology,
                                    &task_operators,
                                    &operator_filter,
                                )
                            })
                    {
                        for (state, usage) in invocation.usages_with_state_names() {
                            if resource_ids.contains(&usage.resource_id()) {
                                builder.try_push(state, &usage)?;
                            }
                        }
                    }
                }
                // Chunk publications are point events, not resource occupancy.
                CHUNK_TRANSFER_TYPE_NAME => {}
                other => {
                    return Err(AnalyzerError::InvalidArgument(format!(
                        "unknown DuckDB entity type {other:?}"
                    )));
                }
            }
            self.timeline_to_ui_keyed(builder.build(), epoch)?
        } else {
            let mut builder = ResourceTimelineBuilder::try_new(resource_type, config, threshold)?;
            // Task and invocation usages are nested on execution threads. Use the
            // task span for unfiltered physical occupancy; callers can request
            // operator_invocation explicitly for the detailed breakdown.
            for task in self
                .model
                .pipeline_tasks
                .values()
                .filter(|task| self.task_matches(task, query_id, &topology, &operator_filter))
            {
                for usage in task.usages() {
                    if resource_ids.contains(&usage.resource_id()) {
                        builder.try_push(&usage)?;
                    }
                }
            }
            self.timeline_to_ui(builder.build(), epoch)?
        };

        Ok(SingleTimelineResponse {
            config: config_secs,
            data,
        })
    }

    fn query_tasks(
        &self,
        query_id: Uuid,
        topology: &QueryTopology,
    ) -> HashMap<Uuid, HashSet<Uuid>> {
        self.model
            .pipeline_tasks
            .iter()
            .filter(|(_, task)| {
                self.task_matches(
                    task,
                    query_id,
                    topology,
                    &OperatorFilter {
                        operator_ids: vec![],
                    },
                )
            })
            .filter_map(|(id, task)| {
                task.operator_ids()
                    .map(|operator_ids| (*id, operator_ids.iter().copied().collect()))
            })
            .collect()
    }

    fn task_matches(
        &self,
        task: &PipelineTask,
        query_id: Uuid,
        topology: &QueryTopology,
        filter: &OperatorFilter,
    ) -> bool {
        task.query_id() == Some(query_id)
            && task.is_complete()
            && task.belongs_to_query(
                &topology.operator_plans,
                &topology.plan_workers,
                &topology.plan_ids,
            )
            && task.resources_are_valid(&self.model.runtime_resources)
            && task.matches_operator(filter)
    }

    fn invocation_matches(
        &self,
        invocation: &OperatorInvocation,
        query_id: Uuid,
        topology: &QueryTopology,
        task_operators: &HashMap<Uuid, HashSet<Uuid>>,
        filter: &OperatorFilter,
    ) -> bool {
        invocation.query_id() == Some(query_id)
            && invocation.is_complete()
            && invocation.belongs_to_query(
                &topology.operator_plans,
                &topology.plan_workers,
                &topology.plan_ids,
                task_operators,
            )
            && invocation.resources_are_valid(&topology.plan_workers, &self.model.runtime_resources)
            && invocation.matches_operator(filter)
    }

    fn entities_to_ui(
        &self,
        entity_ids: &[Uuid],
        epoch: TimeUnixNanoSec,
    ) -> AnalyzerResult<Vec<FiniteStateMachine>> {
        entity_ids
            .iter()
            .filter_map(|id| {
                if let Some(task) = self.model.pipeline_tasks.get(id) {
                    return Some(FiniteStateMachine::try_from_fsm(task, epoch));
                }
                self.model
                    .operator_invocations
                    .get(id)
                    .map(|invocation| FiniteStateMachine::try_from_fsm(invocation, epoch))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn timeline_to_ui(
        &self,
        timeline: ResourceTimeline<'_>,
        epoch: TimeUnixNanoSec,
    ) -> AnalyzerResult<UiResourceTimeline> {
        Ok(UiResourceTimeline::Binned(ResourceTimelineBinned {
            config: timeline.config.try_to_secs_relative(epoch)?,
            capacities_values: timeline
                .data
                .into_iter()
                .map(|(name, values)| (name.to_owned(), values))
                .collect(),
            long_fsms: self.entities_to_ui(&timeline.long_entities, epoch)?,
        }))
    }

    fn timeline_to_ui_keyed(
        &self,
        timeline: ResourceTimelineByKey<'_, &str>,
        epoch: TimeUnixNanoSec,
    ) -> AnalyzerResult<UiResourceTimeline> {
        let mut capacities_states_values = HashMap::new();
        for ((state, capacity), values) in timeline.data {
            capacities_states_values
                .entry(capacity.to_owned())
                .or_insert_with(HashMap::new)
                .insert(state.to_owned(), values);
        }
        Ok(UiResourceTimeline::BinnedByState(
            ResourceTimelineBinnedByState {
                config: timeline.config.try_to_secs_relative(epoch)?,
                capacities_states_values,
                long_fsms: self.entities_to_ui(&timeline.long_entities, epoch)?,
            },
        ))
    }

    fn query_topology(&self, query_id: Uuid) -> AnalyzerResult<QueryTopology> {
        let view = self.model.query_engine.query_view(query_id)?;
        let worker_ids: HashSet<Uuid> = view.workers().map(|worker| worker.id()).collect();
        Ok(QueryTopology {
            plan_ids: view.plans().map(|plan| plan.id()).collect(),
            plan_workers: view
                .plans()
                .filter_map(|plan| {
                    plan.worker_id()
                        .filter(|worker_id| worker_ids.contains(worker_id))
                        .map(|worker_id| (plan.id(), worker_id))
                })
                .collect(),
            operator_plans: view
                .operators()
                .filter_map(|operator| operator.plan_id().map(|plan_id| (operator.id(), plan_id)))
                .collect(),
            port_operators: view
                .ports()
                .filter_map(|port| {
                    port.operator_id()
                        .map(|operator_id| (port.id(), operator_id))
                })
                .collect(),
            plan_edges: view
                .plans()
                .flat_map(|plan| {
                    plan.edges()
                        .iter()
                        .map(|edge| (edge.source.uuid(), edge.target.uuid()))
                })
                .collect(),
        })
    }

    /// Aggregate completed chunk-publication events by physical plan edge.
    pub fn chunk_summary(&self, query_id: Uuid) -> Vec<ChunkEdgeSummary> {
        type Edge = (Uuid, Uuid, Uuid, Uuid);
        type Totals = (u64, u64, u64);

        let Ok(topology) = self.query_topology(query_id) else {
            return vec![];
        };
        let task_ids: HashSet<Uuid> = self.query_tasks(query_id, &topology).into_keys().collect();
        let mut grouped = BTreeMap::<Edge, Totals>::new();
        for transfer in self.model.chunk_transfers.values() {
            if transfer.query_id() != Some(query_id)
                || !transfer.is_complete()
                || !transfer.belongs_to_query(
                    &topology.port_operators,
                    &topology.plan_edges,
                    &task_ids,
                )
            {
                continue;
            }
            let Some(ChunkTransferTransition::Produced(produced)) = transfer.first_data() else {
                continue;
            };
            let edge = (
                produced.source_operator_id,
                produced.source_port_id,
                produced.target_operator_id,
                produced.target_port_id,
            );
            let totals = grouped.entry(edge).or_default();
            totals.0 = totals.0.saturating_add(1);
            totals.1 = totals.1.saturating_add(produced.rows);
            totals.2 = totals.2.saturating_add(produced.logical_bytes);
        }

        grouped
            .into_iter()
            .map(
                |(
                    (source_operator_id, source_port_id, target_operator_id, target_port_id),
                    (transfers, rows, logical_bytes),
                )| ChunkEdgeSummary {
                    source_operator_id,
                    source_port_id,
                    target_operator_id,
                    target_port_id,
                    transfers,
                    rows,
                    logical_bytes,
                },
            )
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{
        EXECUTION_THREAD_TYPE_NAME, QUEUE_ENTRIES_CAPACITY_NAME, TASK_QUEUE_TYPE_NAME,
    };
    use duckdb_telemetry_model::{
        chunk_transfer, engine, operator, operator_invocation, pipeline_task, plan, port, query,
        query_group, runtime_resource, worker,
    };
    use quent_model::{Capacity, FsmEvent, Ref, Usage};
    use quent_ui::entities::request::{
        EntityListEntry, EntityListFilter, EntitySortKey, Sort, SortDir, TimeWindow,
    };
    use quent_ui::timeline::request::{
        EntityFilter, ResourceGroupTimelineRequest, ResourceTimelineRequest, TimelineConfig,
    };

    use super::*;

    const ENGINE_ID: Uuid = Uuid::from_u128(1);
    const QUERY_GROUP_ID: Uuid = Uuid::from_u128(2);
    const QUERY_ID: Uuid = Uuid::from_u128(3);
    const PLAN_ID: Uuid = Uuid::from_u128(4);
    const SOURCE_ID: Uuid = Uuid::from_u128(5);
    const TARGET_ID: Uuid = Uuid::from_u128(6);
    const SOURCE_PORT_ID: Uuid = Uuid::from_u128(7);
    const TARGET_PORT_ID: Uuid = Uuid::from_u128(8);
    const TASK_ID: Uuid = Uuid::from_u128(9);
    const TRANSFER_ID: Uuid = Uuid::from_u128(10);
    const SECOND_TRANSFER_ID: Uuid = Uuid::from_u128(11);
    const WORKER_ID: Uuid = Uuid::from_u128(12);
    const INCOMPLETE_TRANSFER_ID: Uuid = Uuid::from_u128(13);
    const MISWIRED_TRANSFER_ID: Uuid = Uuid::from_u128(14);
    const INCOMPLETE_TASK_ID: Uuid = Uuid::from_u128(16);
    const EXECUTION_THREAD_ID: Uuid = Uuid::from_u128(17);
    const SECOND_EXECUTION_THREAD_ID: Uuid = Uuid::from_u128(18);
    const TASK_QUEUE_ID: Uuid = Uuid::from_u128(19);
    const INVOCATION_ID: Uuid = Uuid::from_u128(20);
    const INVALID_WORKER_TASK_ID: Uuid = Uuid::from_u128(21);
    const INVALID_RESOURCE_TASK_ID: Uuid = Uuid::from_u128(22);
    const INVALID_INVOCATION_ID: Uuid = Uuid::from_u128(23);
    const UNKNOWN_WORKER_ID: Uuid = Uuid::from_u128(24);

    fn event(id: Uuid, timestamp: u64, data: DuckDBEvent) -> Event<DuckDBEvent> {
        Event::new(id, timestamp, data)
    }

    fn fsm<T>(seq: u64, state: T) -> FsmEvent<T> {
        FsmEvent { seq, state }
    }

    fn queue_usage() -> Usage<runtime_resource::TaskQueue> {
        queue_usage_for(TASK_QUEUE_ID)
    }

    fn queue_usage_for(id: Uuid) -> Usage<runtime_resource::TaskQueue> {
        Usage {
            resource_id: Ref::new(id),
            capacity: runtime_resource::TaskQueueOperating {
                capacity_entries: Capacity::new(Some(1)),
            },
        }
    }

    fn thread_usage(id: Uuid) -> Usage<runtime_resource::ExecutionThread> {
        Usage {
            resource_id: Ref::new(id),
            capacity: runtime_resource::ExecutionThreadOperating {},
        }
    }

    fn resource_events() -> Vec<Event<DuckDBEvent>> {
        let mut events = vec![];
        for (id, name) in [
            (EXECUTION_THREAD_ID, "thread-0"),
            (SECOND_EXECUTION_THREAD_ID, "thread-1"),
        ] {
            events.extend([
                event(
                    id,
                    4,
                    DuckDBEvent::ExecutionThread(fsm(
                        0,
                        runtime_resource::ExecutionThreadTransition::ExecutionThreadInitializing(
                            runtime_resource::ExecutionThreadInitializing {
                                instance_name: name.to_owned(),
                                parent_group_id: WORKER_ID,
                                resource_type_name: EXECUTION_THREAD_TYPE_NAME.to_owned(),
                            },
                        ),
                    )),
                ),
                event(
                    id,
                    5,
                    DuckDBEvent::ExecutionThread(fsm(
                        1,
                        runtime_resource::ExecutionThreadTransition::ExecutionThreadOperating(
                            runtime_resource::ExecutionThreadOperating {},
                        ),
                    )),
                ),
                event(
                    id,
                    202,
                    DuckDBEvent::ExecutionThread(fsm(
                        2,
                        runtime_resource::ExecutionThreadTransition::ExecutionThreadFinalizing(
                            runtime_resource::ExecutionThreadFinalizing,
                        ),
                    )),
                ),
                event(
                    id,
                    203,
                    DuckDBEvent::ExecutionThread(fsm(
                        3,
                        runtime_resource::ExecutionThreadTransition::Exit,
                    )),
                ),
            ]);
        }
        events.extend([
            event(
                TASK_QUEUE_ID,
                4,
                DuckDBEvent::TaskQueue(fsm(
                    0,
                    runtime_resource::TaskQueueTransition::TaskQueueInitializing(
                        runtime_resource::TaskQueueInitializing {
                            instance_name: "regular".to_owned(),
                            parent_group_id: WORKER_ID,
                            resource_type_name: TASK_QUEUE_TYPE_NAME.to_owned(),
                        },
                    ),
                )),
            ),
            event(
                TASK_QUEUE_ID,
                5,
                DuckDBEvent::TaskQueue(fsm(
                    1,
                    runtime_resource::TaskQueueTransition::TaskQueueOperating(
                        runtime_resource::TaskQueueOperating {
                            capacity_entries: Capacity::new(Some(64)),
                        },
                    ),
                )),
            ),
            event(
                TASK_QUEUE_ID,
                202,
                DuckDBEvent::TaskQueue(fsm(
                    2,
                    runtime_resource::TaskQueueTransition::TaskQueueFinalizing(
                        runtime_resource::TaskQueueFinalizing,
                    ),
                )),
            ),
            event(
                TASK_QUEUE_ID,
                203,
                DuckDBEvent::TaskQueue(fsm(3, runtime_resource::TaskQueueTransition::Exit)),
            ),
        ]);
        events
    }

    fn base_events() -> Vec<Event<DuckDBEvent>> {
        vec![
            event(
                ENGINE_ID,
                1,
                DuckDBEvent::Engine(engine::EngineEvent::Init(engine::Init {
                    implementation: engine::EngineImplementationAttributes {
                        name: Some("DuckDB".to_owned()),
                        ..Default::default()
                    },
                    instance_name: Some("test".to_owned()),
                })),
            ),
            event(
                WORKER_ID,
                2,
                DuckDBEvent::Worker(worker::WorkerEvent::Init(worker::Init {
                    parent_engine_id: Ref::new(ENGINE_ID),
                    instance_name: "local".to_owned(),
                })),
            ),
            event(
                QUERY_GROUP_ID,
                3,
                DuckDBEvent::QueryGroup(query_group::QueryGroupEvent::Declaration(
                    query_group::Declaration {
                        instance_name: "session".to_owned(),
                        engine_id: ENGINE_ID,
                    },
                )),
            ),
            event(
                QUERY_ID,
                100,
                DuckDBEvent::Query(fsm(
                    0,
                    query::QueryTransition::Init(query::Init {
                        instance_name: "select range".to_owned(),
                        query_group_id: Ref::new(QUERY_GROUP_ID),
                    }),
                )),
            ),
            event(
                QUERY_ID,
                105,
                DuckDBEvent::Query(fsm(1, query::QueryTransition::Planning(query::Planning {}))),
            ),
            event(
                PLAN_ID,
                106,
                DuckDBEvent::Plan(plan::PlanEvent::Declaration(plan::Declaration {
                    parent: plan::PlanParent {
                        query_id: Some(Ref::new(QUERY_ID)),
                        plan_id: None,
                    },
                    instance_name: "physical".to_owned(),
                    edges: vec![plan::Edge {
                        source: Ref::new(SOURCE_PORT_ID),
                        target: Ref::new(TARGET_PORT_ID),
                    }],
                    worker_id: Some(Ref::new(WORKER_ID)),
                })),
            ),
            operator_event(SOURCE_ID, "RANGE"),
            operator_event(TARGET_ID, "UNGROUPED_AGGREGATE"),
            port_event(SOURCE_PORT_ID, SOURCE_ID, "out"),
            port_event(TARGET_PORT_ID, TARGET_ID, "in"),
            event(
                QUERY_ID,
                110,
                DuckDBEvent::Query(fsm(
                    2,
                    query::QueryTransition::Executing(query::Executing {}),
                )),
            ),
            event(
                QUERY_ID,
                200,
                DuckDBEvent::Query(fsm(3, query::QueryTransition::Exit)),
            ),
            event(
                WORKER_ID,
                205,
                DuckDBEvent::Worker(worker::WorkerEvent::Exit(worker::Exit)),
            ),
            event(
                ENGINE_ID,
                210,
                DuckDBEvent::Engine(engine::EngineEvent::Exit(engine::Exit)),
            ),
        ]
    }

    fn operator_event(id: Uuid, type_name: &str) -> Event<DuckDBEvent> {
        event(
            id,
            107,
            DuckDBEvent::Operator(operator::OperatorEvent::Declaration(
                operator::Declaration {
                    plan_id: Ref::new(PLAN_ID),
                    parent_operator_ids: vec![],
                    instance_name: type_name.to_owned(),
                    type_name: type_name.to_owned(),
                    custom_attributes: Default::default(),
                },
            )),
        )
    }

    fn port_event(id: Uuid, operator_id: Uuid, name: &str) -> Event<DuckDBEvent> {
        event(
            id,
            108,
            DuckDBEvent::Port(port::PortEvent::Declaration(port::Declaration {
                operator_id: Ref::new(operator_id),
                instance_name: name.to_owned(),
            })),
        )
    }

    fn task_events() -> Vec<Event<DuckDBEvent>> {
        vec![
            event(
                TASK_ID,
                120,
                DuckDBEvent::PipelineTask(fsm(
                    0,
                    pipeline_task::PipelineTaskTransition::Created(pipeline_task::Created {
                        instance_name: "pipeline 0 task 0".to_owned(),
                        query_id: QUERY_ID,
                        plan_id: PLAN_ID,
                        worker_id: WORKER_ID,
                        operator_ids: vec![SOURCE_ID, TARGET_ID],
                        task_index: 0,
                        queue: Some(queue_usage()),
                    }),
                )),
            ),
            event(
                TASK_ID,
                125,
                DuckDBEvent::PipelineTask(fsm(
                    1,
                    pipeline_task::PipelineTaskTransition::Running(pipeline_task::Running {
                        instance_name: "pipeline 0 task 0".to_owned(),
                        mode: "all".to_owned(),
                        cpu_id: 0,
                        execution_thread: Some(thread_usage(EXECUTION_THREAD_ID)),
                    }),
                )),
            ),
            event(
                TASK_ID,
                130,
                DuckDBEvent::PipelineTask(fsm(
                    2,
                    pipeline_task::PipelineTaskTransition::Ready(pipeline_task::Ready {
                        queue: Some(queue_usage()),
                    }),
                )),
            ),
            event(
                TASK_ID,
                132,
                DuckDBEvent::PipelineTask(fsm(
                    3,
                    pipeline_task::PipelineTaskTransition::Running(pipeline_task::Running {
                        instance_name: "pipeline 0 task 0".to_owned(),
                        mode: "partial".to_owned(),
                        cpu_id: 1,
                        execution_thread: Some(thread_usage(SECOND_EXECUTION_THREAD_ID)),
                    }),
                )),
            ),
            event(
                TASK_ID,
                134,
                DuckDBEvent::PipelineTask(fsm(
                    4,
                    pipeline_task::PipelineTaskTransition::Blocked(pipeline_task::Blocked {}),
                )),
            ),
            event(
                TASK_ID,
                136,
                DuckDBEvent::PipelineTask(fsm(
                    5,
                    pipeline_task::PipelineTaskTransition::Running(pipeline_task::Running {
                        instance_name: "pipeline 0 task 0".to_owned(),
                        mode: "partial".to_owned(),
                        cpu_id: 0,
                        execution_thread: Some(thread_usage(EXECUTION_THREAD_ID)),
                    }),
                )),
            ),
            event(
                TASK_ID,
                140,
                DuckDBEvent::PipelineTask(fsm(
                    6,
                    pipeline_task::PipelineTaskTransition::Finalizing(pipeline_task::Finalizing {
                        instance_name: "pipeline 0 task 0".to_owned(),
                        success: true,
                    }),
                )),
            ),
            event(
                TASK_ID,
                141,
                DuckDBEvent::PipelineTask(fsm(7, pipeline_task::PipelineTaskTransition::Exit)),
            ),
        ]
    }

    fn simple_task_events(
        id: Uuid,
        worker_id: Uuid,
        operator_ids: Vec<Uuid>,
        queue_id: Uuid,
        thread_id: Uuid,
    ) -> Vec<Event<DuckDBEvent>> {
        vec![
            event(
                id,
                150,
                DuckDBEvent::PipelineTask(fsm(
                    0,
                    pipeline_task::PipelineTaskTransition::Created(pipeline_task::Created {
                        instance_name: format!("task-{id}"),
                        query_id: QUERY_ID,
                        plan_id: PLAN_ID,
                        worker_id,
                        operator_ids,
                        task_index: 2,
                        queue: Some(queue_usage_for(queue_id)),
                    }),
                )),
            ),
            event(
                id,
                151,
                DuckDBEvent::PipelineTask(fsm(
                    1,
                    pipeline_task::PipelineTaskTransition::Running(pipeline_task::Running {
                        instance_name: String::new(),
                        mode: "all".to_owned(),
                        cpu_id: 0,
                        execution_thread: Some(thread_usage(thread_id)),
                    }),
                )),
            ),
            event(
                id,
                152,
                DuckDBEvent::PipelineTask(fsm(
                    2,
                    pipeline_task::PipelineTaskTransition::Finalizing(pipeline_task::Finalizing {
                        instance_name: String::new(),
                        success: true,
                    }),
                )),
            ),
            event(
                id,
                153,
                DuckDBEvent::PipelineTask(fsm(3, pipeline_task::PipelineTaskTransition::Exit)),
            ),
        ]
    }

    fn transfer_events(
        id: Uuid,
        timestamp: u64,
        query_id: Uuid,
        rows: u64,
        logical_bytes: u64,
        miswired: bool,
    ) -> Vec<Event<DuckDBEvent>> {
        vec![
            event(
                id,
                timestamp,
                DuckDBEvent::ChunkTransfer(fsm(
                    0,
                    chunk_transfer::ChunkTransferTransition::Produced(chunk_transfer::Produced {
                        instance_name: "range -> aggregate".to_owned(),
                        query_id,
                        task_id: TASK_ID,
                        source_operator_id: SOURCE_ID,
                        source_port_id: if miswired {
                            TARGET_PORT_ID
                        } else {
                            SOURCE_PORT_ID
                        },
                        target_operator_id: TARGET_ID,
                        target_port_id: if miswired {
                            SOURCE_PORT_ID
                        } else {
                            TARGET_PORT_ID
                        },
                        rows,
                        logical_bytes,
                    }),
                )),
            ),
            event(
                id,
                timestamp + 1,
                DuckDBEvent::ChunkTransfer(fsm(
                    1,
                    chunk_transfer::ChunkTransferTransition::Published(
                        chunk_transfer::Published {},
                    ),
                )),
            ),
            event(
                id,
                timestamp + 2,
                DuckDBEvent::ChunkTransfer(fsm(2, chunk_transfer::ChunkTransferTransition::Exit)),
            ),
        ]
    }

    fn incomplete_task_events() -> Vec<Event<DuckDBEvent>> {
        vec![
            event(
                INCOMPLETE_TASK_ID,
                150,
                DuckDBEvent::PipelineTask(fsm(
                    0,
                    pipeline_task::PipelineTaskTransition::Created(pipeline_task::Created {
                        instance_name: "incomplete task".to_owned(),
                        query_id: QUERY_ID,
                        plan_id: PLAN_ID,
                        worker_id: WORKER_ID,
                        operator_ids: vec![SOURCE_ID],
                        task_index: 1,
                        queue: Some(queue_usage()),
                    }),
                )),
            ),
            event(
                INCOMPLETE_TASK_ID,
                151,
                DuckDBEvent::PipelineTask(fsm(
                    1,
                    pipeline_task::PipelineTaskTransition::Running(pipeline_task::Running {
                        instance_name: "incomplete task".to_owned(),
                        mode: "partial".to_owned(),
                        cpu_id: 0,
                        execution_thread: Some(thread_usage(EXECUTION_THREAD_ID)),
                    }),
                )),
            ),
        ]
    }

    fn invocation_events() -> Vec<Event<DuckDBEvent>> {
        invocation_events_for(INVOCATION_ID, TASK_ID, SOURCE_ID)
    }

    fn invocation_events_for(
        id: Uuid,
        task_id: Uuid,
        operator_id: Uuid,
    ) -> Vec<Event<DuckDBEvent>> {
        vec![
            event(
                id,
                125,
                DuckDBEvent::OperatorInvocation(fsm(
                    0,
                    operator_invocation::OperatorInvocationTransition::InvocationCreated(
                        operator_invocation::InvocationCreated {
                            instance_name: "RANGE/source".to_owned(),
                            query_id: QUERY_ID,
                            plan_id: PLAN_ID,
                            task_id,
                            operator_id,
                            phase: "source".to_owned(),
                        },
                    ),
                )),
            ),
            event(
                id,
                126,
                DuckDBEvent::OperatorInvocation(fsm(
                    1,
                    operator_invocation::OperatorInvocationTransition::InvocationRunning(
                        operator_invocation::InvocationRunning {
                            instance_name: String::new(),
                            input_rows: 0,
                            input_logical_bytes: 0,
                            execution_thread: Some(thread_usage(EXECUTION_THREAD_ID)),
                        },
                    ),
                )),
            ),
            event(
                id,
                129,
                DuckDBEvent::OperatorInvocation(fsm(
                    2,
                    operator_invocation::OperatorInvocationTransition::InvocationCompleted(
                        operator_invocation::InvocationCompleted {
                            instance_name: String::new(),
                            success: true,
                            output_rows: 10,
                            output_logical_bytes: 80,
                        },
                    ),
                )),
            ),
            event(
                id,
                130,
                DuckDBEvent::OperatorInvocation(fsm(
                    3,
                    operator_invocation::OperatorInvocationTransition::Exit,
                )),
            ),
        ]
    }

    fn analyzer() -> DuckDbUiAnalyzer {
        let events = base_events()
            .into_iter()
            .chain(resource_events())
            .chain(task_events())
            .chain(incomplete_task_events())
            .chain(invocation_events())
            .chain(transfer_events(TRANSFER_ID, 130, QUERY_ID, 10, 80, false))
            .chain(transfer_events(
                SECOND_TRANSFER_ID,
                135,
                QUERY_ID,
                5,
                40,
                false,
            ))
            .chain(
                transfer_events(INCOMPLETE_TRANSFER_ID, 145, QUERY_ID, 20, 160, false)
                    .into_iter()
                    .take(2),
            )
            .chain(transfer_events(
                MISWIRED_TRANSFER_ID,
                150,
                QUERY_ID,
                30,
                240,
                true,
            ));
        DuckDbUiAnalyzer::try_new(ENGINE_ID, events).unwrap()
    }

    fn list_request(
        entity_type_name: &str,
        operator_ids: Vec<Uuid>,
    ) -> EntityListRequest<QueryFilter, OperatorFilter> {
        EntityListRequest {
            app_params: QueryFilter { query_id: QUERY_ID },
            entry: EntityListEntry {
                window: TimeWindow {
                    start: 0.0,
                    end: 1.0,
                },
                filter: EntityListFilter {
                    entity_type_name: Some(entity_type_name.to_owned()),
                    ..Default::default()
                },
                sort: Sort {
                    key: EntitySortKey::UsageDuration,
                    dir: SortDir::Desc,
                },
                page: None,
                application: OperatorFilter { operator_ids },
            },
        }
    }

    fn single_resource_request(
        resource_id: Uuid,
        entity_type_name: Option<&str>,
        operator_ids: Vec<Uuid>,
    ) -> SingleTimelineRequest<QueryFilter, OperatorFilter> {
        SingleTimelineRequest {
            entry: TimelineRequest::Resource(ResourceTimelineRequest {
                resource_id,
                long_entities_threshold_s: None,
                entity_filter: EntityFilter {
                    entity_type_name: entity_type_name.map(str::to_owned),
                },
                application: OperatorFilter { operator_ids },
                config: TimelineConfig {
                    num_bins: 10,
                    start: 0.0,
                    end: 1e-7,
                },
            }),
            app_params: QueryFilter { query_id: QUERY_ID },
        }
    }

    fn worker_timeline_request(
        entity_type_name: Option<&str>,
    ) -> SingleTimelineRequest<QueryFilter, OperatorFilter> {
        SingleTimelineRequest {
            entry: TimelineRequest::ResourceGroup(ResourceGroupTimelineRequest {
                resource_group_id: WORKER_ID,
                resource_type_name: EXECUTION_THREAD_TYPE_NAME.to_owned(),
                long_entities_threshold_s: None,
                entity_filter: EntityFilter {
                    entity_type_name: entity_type_name.map(str::to_owned),
                },
                app_params: OperatorFilter {
                    operator_ids: vec![],
                },
                config: TimelineConfig {
                    num_bins: 10,
                    start: 0.0,
                    end: 1e-7,
                },
            }),
            app_params: QueryFilter { query_id: QUERY_ID },
        }
    }

    fn data_flow_request(measures: &[&str]) -> CategoricalTimelineRequest<QueryFilter> {
        CategoricalTimelineRequest {
            measures: measures
                .iter()
                .map(|measure| (*measure).to_owned())
                .collect(),
            config: TimelineConfig {
                num_bins: 10,
                start: 0.0,
                end: 1e-7,
            },
            app_params: QueryFilter { query_id: QUERY_ID },
        }
    }

    #[test]
    fn bundle_declares_runtime_fsms() {
        let bundle = analyzer().query_bundle(QUERY_ID).unwrap();

        assert!(
            bundle
                .entities
                .fsm_types
                .contains_key(PIPELINE_TASK_TYPE_NAME)
        );
        assert!(
            bundle
                .entities
                .fsm_types
                .contains_key(CHUNK_TRANSFER_TYPE_NAME)
        );
        assert!(
            bundle
                .entities
                .fsm_types
                .contains_key(OPERATOR_INVOCATION_TYPE_NAME)
        );
        assert_eq!(bundle.entities.resources.len(), 3);
        assert!(
            bundle
                .entities
                .resource_types
                .get(EXECUTION_THREAD_TYPE_NAME)
                .unwrap()
                .used_by
                .contains(&PIPELINE_TASK_TYPE_NAME.to_owned())
        );
        assert!(
            bundle
                .entities
                .resource_types
                .get(EXECUTION_THREAD_TYPE_NAME)
                .unwrap()
                .used_by
                .contains(&OPERATOR_INVOCATION_TYPE_NAME.to_owned())
        );
    }

    #[test]
    fn live_runtime_resources_are_closed_for_the_snapshot() {
        let events = base_events()
            .into_iter()
            .filter(|event| event.timestamp < 205)
            .chain(
                resource_events()
                    .into_iter()
                    .filter(|event| event.timestamp < 200),
            )
            .chain(task_events())
            .chain(invocation_events());
        let analyzer = DuckDbUiAnalyzer::try_new(ENGINE_ID, events).unwrap();

        assert!(analyzer.model.query_engine.engine().unwrap().span().is_ok());
        assert!(
            analyzer
                .model
                .query_engine
                .workers()
                .all(|worker| worker.span().is_ok())
        );

        assert_eq!(
            analyzer
                .query_bundle(QUERY_ID)
                .unwrap()
                .entities
                .resources
                .len(),
            3
        );
        assert!(
            analyzer
                .single_resource_timeline(single_resource_request(
                    EXECUTION_THREAD_ID,
                    Some(PIPELINE_TASK_TYPE_NAME),
                    vec![],
                ))
                .is_ok()
        );
    }

    #[test]
    fn finalizing_runtime_resources_get_a_snapshot_exit() {
        let events = base_events()
            .into_iter()
            .filter(|event| event.timestamp < 205)
            .chain(
                resource_events()
                    .into_iter()
                    .filter(|event| event.timestamp != 203),
            )
            .chain(task_events())
            .chain(invocation_events());

        assert!(DuckDbUiAnalyzer::try_new(ENGINE_ID, events).is_ok());
    }

    #[test]
    fn operator_active_span_comes_from_valid_running_invocation() {
        let bundle = analyzer().query_bundle(QUERY_ID).unwrap();
        let span = bundle.entities.operators[&SOURCE_ID]
            .active_span
            .expect("source invocation should activate its operator");

        assert!((span.start() - 26e-9).abs() < f64::EPSILON);
        assert!((span.end() - 29e-9).abs() < f64::EPSILON);
        assert!(bundle.entities.operators[&TARGET_ID].active_span.is_none());
    }

    #[test]
    fn tasks_with_invalid_worker_or_resource_links_are_filtered() {
        let events = base_events()
            .into_iter()
            .chain(resource_events())
            .chain(simple_task_events(
                INVALID_WORKER_TASK_ID,
                UNKNOWN_WORKER_ID,
                vec![SOURCE_ID],
                TASK_QUEUE_ID,
                EXECUTION_THREAD_ID,
            ))
            .chain(simple_task_events(
                INVALID_RESOURCE_TASK_ID,
                WORKER_ID,
                vec![SOURCE_ID],
                EXECUTION_THREAD_ID,
                TASK_QUEUE_ID,
            ));
        let analyzer = DuckDbUiAnalyzer::try_new(ENGINE_ID, events).unwrap();

        let tasks = analyzer
            .list_entities(list_request(PIPELINE_TASK_TYPE_NAME, vec![]))
            .unwrap();
        assert_eq!(tasks.total, 0);
    }

    #[test]
    fn invocation_operator_must_belong_to_its_task() {
        let events = base_events()
            .into_iter()
            .chain(resource_events())
            .chain(simple_task_events(
                TASK_ID,
                WORKER_ID,
                vec![SOURCE_ID],
                TASK_QUEUE_ID,
                EXECUTION_THREAD_ID,
            ))
            .chain(invocation_events_for(
                INVALID_INVOCATION_ID,
                TASK_ID,
                TARGET_ID,
            ));
        let analyzer = DuckDbUiAnalyzer::try_new(ENGINE_ID, events).unwrap();

        let invocations = analyzer
            .list_entities(list_request(OPERATOR_INVOCATION_TYPE_NAME, vec![]))
            .unwrap();
        assert_eq!(invocations.total, 0);
        assert!(
            analyzer.query_bundle(QUERY_ID).unwrap().entities.operators[&TARGET_ID]
                .active_span
                .is_none()
        );
    }

    #[test]
    fn chunk_summary_groups_plan_edges() {
        let summary = analyzer().chunk_summary(QUERY_ID);

        assert_eq!(summary.len(), 1);
        assert_eq!(summary[0].transfers, 2);
        assert_eq!(summary[0].rows, 15);
        assert_eq!(summary[0].logical_bytes, 120);
        assert_eq!(summary[0].source_operator_id, SOURCE_ID);
        assert_eq!(summary[0].target_operator_id, TARGET_ID);
    }

    #[test]
    fn lists_runtime_entities_by_type_and_operator() {
        let analyzer = analyzer();
        let transfers = analyzer
            .list_entities(list_request(CHUNK_TRANSFER_TYPE_NAME, vec![SOURCE_ID]))
            .unwrap();
        let tasks = analyzer
            .list_entities(list_request(PIPELINE_TASK_TYPE_NAME, vec![TARGET_ID]))
            .unwrap();
        let invocations = analyzer
            .list_entities(list_request(OPERATOR_INVOCATION_TYPE_NAME, vec![SOURCE_ID]))
            .unwrap();

        assert_eq!(transfers.total, 2);
        assert_eq!(transfers.items.len(), 2);
        assert_eq!(tasks.total, 1);
        assert_eq!(tasks.items.len(), 1);
        assert_eq!(invocations.total, 1);
        assert_eq!(invocations.items.len(), 1);
    }

    #[test]
    fn data_flow_timeline_reports_publication_rates_by_upstream_operator() {
        let response = analyzer()
            .data_flow_timeline(data_flow_request(&[]))
            .unwrap();
        let dimension = SOURCE_ID.to_string();
        let series = &response.operators[&TARGET_ID].values;

        assert_eq!(response.decl.entity_type_name, CHUNK_TRANSFER_TYPE_NAME);
        assert_eq!(response.decl.dimension_name, "Upstream operator");
        assert_eq!(
            response.decl.default_measure.as_deref(),
            Some(MEASURE_LOGICAL_BYTES)
        );
        assert_eq!(response.decl.dimension_keys[0].key, dimension);
        assert_eq!(response.decl.dimension_keys[0].display_name, "RANGE");
        assert_eq!(
            series[MEASURE_CHUNKS][DATA_FLOW_STATE][&dimension][3],
            200_000_000.0
        );
        assert_eq!(
            series[MEASURE_ROWS][DATA_FLOW_STATE][&dimension][3],
            1_500_000_000.0
        );
        assert_eq!(
            series[MEASURE_LOGICAL_BYTES][DATA_FLOW_STATE][&dimension][3],
            12_000_000_000.0
        );
    }

    #[test]
    fn data_flow_timeline_validates_measure_names() {
        assert!(
            analyzer()
                .data_flow_timeline(data_flow_request(&["physical_bytes"]))
                .is_err()
        );
    }

    #[test]
    fn execution_thread_timeline_uses_task_as_physical_occupancy() {
        let response = analyzer()
            .single_resource_timeline(single_resource_request(EXECUTION_THREAD_ID, None, vec![]))
            .unwrap();
        let UiResourceTimeline::Binned(timeline) = response.data else {
            panic!("expected plain resource timeline");
        };

        assert_eq!(
            timeline.capacities_values["unit"],
            [0.0, 0.0, 0.5, 0.4, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        );
    }

    #[test]
    fn resource_group_timeline_resolves_an_omitted_resource_type() {
        let mut request = worker_timeline_request(None);
        let TimelineRequest::ResourceGroup(group) = &mut request.entry else {
            unreachable!();
        };
        group.resource_type_name.clear();

        let response = analyzer().single_resource_timeline(request).unwrap();
        let UiResourceTimeline::Binned(timeline) = response.data else {
            panic!("expected plain resource timeline");
        };
        assert_eq!(
            timeline.capacities_values["unit"],
            [
                0.0,
                0.0,
                0.5,
                0.6000000000000001,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0
            ]
        );
    }

    #[test]
    fn worker_timeline_splits_execution_by_entity_state() {
        let task_response = analyzer()
            .single_resource_timeline(worker_timeline_request(Some(PIPELINE_TASK_TYPE_NAME)))
            .unwrap();
        let UiResourceTimeline::BinnedByState(task_timeline) = task_response.data else {
            panic!("expected keyed resource timeline");
        };
        assert_eq!(
            task_timeline.capacities_states_values["unit"]["running"],
            [
                0.0,
                0.0,
                0.5,
                0.6000000000000001,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0
            ]
        );

        let invocation_response = analyzer()
            .single_resource_timeline(worker_timeline_request(Some(OPERATOR_INVOCATION_TYPE_NAME)))
            .unwrap();
        let UiResourceTimeline::BinnedByState(invocation_timeline) = invocation_response.data
        else {
            panic!("expected keyed resource timeline");
        };
        assert_eq!(
            invocation_timeline.capacities_states_values["unit"]["invocation_running"],
            [0.0, 0.0, 0.3, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        );
    }

    #[test]
    fn queue_timeline_tracks_created_and_ready_tasks() {
        let response = analyzer()
            .single_resource_timeline(single_resource_request(
                TASK_QUEUE_ID,
                Some(PIPELINE_TASK_TYPE_NAME),
                vec![],
            ))
            .unwrap();
        let UiResourceTimeline::BinnedByState(timeline) = response.data else {
            panic!("expected keyed queue timeline");
        };

        assert_eq!(
            timeline.capacities_states_values[QUEUE_ENTRIES_CAPACITY_NAME]["created"],
            [0.0, 0.0, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        );
        assert_eq!(
            timeline.capacities_states_values[QUEUE_ENTRIES_CAPACITY_NAME]["ready"],
            [0.0, 0.0, 0.0, 0.2, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        );
    }

    #[test]
    fn bulk_timeline_returns_independent_entries() {
        let entries = [
            (
                "thread".to_owned(),
                single_resource_request(EXECUTION_THREAD_ID, None, vec![]).entry,
            ),
            (
                "queue".to_owned(),
                single_resource_request(TASK_QUEUE_ID, None, vec![]).entry,
            ),
        ]
        .into_iter()
        .collect();
        let response = analyzer()
            .bulk_resource_timeline(BulkTimelineRequest {
                entries,
                app_params: QueryFilter { query_id: QUERY_ID },
            })
            .unwrap();

        assert!(matches!(
            response.entries["thread"],
            BulkTimelinesResponseEntry::Ok { .. }
        ));
        assert!(matches!(
            response.entries["queue"],
            BulkTimelinesResponseEntry::Ok { .. }
        ));
    }

    #[test]
    fn chunk_publications_have_no_resource_usage() {
        let analyzer = analyzer();
        let transfer = analyzer.model.chunk_transfers.get(&TRANSFER_ID).unwrap();

        assert_eq!(transfer.usages().count(), 0);
    }
}
