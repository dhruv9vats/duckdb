use std::collections::{BTreeMap, HashMap, HashSet};

use duckdb_telemetry_model::{DuckDB, DuckDBEvent, chunk_transfer::ChunkTransferTransition};
use quent_analyzer::{
    AnalyzerError, AnalyzerResult, Entity, Span,
    fsm::{FsmTypeDeclaration, FsmUsages, collection::FsmCollection},
    resource::{
        CapacityValue, Usage, Using, collection::ResourceCollection, tree::ResourceTreeNode,
    },
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
use quent_time::{TimeUnixNanoSec, span::SpanUnixNanoSec, to_nanosecs, to_secs};
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
    memory_account::{MemoryAccount, MemoryAccountExt},
    model::{
        DuckDbModel, DuckDbModelBuilder, IO_BUFFER_BYTES_CAPACITY_NAME,
        IO_OPERATIONS_CAPACITY_NAME, MEMORY_BYTES_CAPACITY_NAME,
    },
    operator_invocation::{OperatorInvocation, OperatorInvocationExt},
    pipeline_task::{PipelineTask, PipelineTaskExt, task_plan_id},
    temporary_block_io::{TemporaryBlockIo, TemporaryBlockIoExt},
};

pub mod chunk_transfer;
mod memory_account;
pub mod model;
pub mod operator_invocation;
pub mod pipeline_task;
mod temporary_block_io;

const PIPELINE_TASK_TYPE_NAME: &str = "pipeline_task";
const CHUNK_TRANSFER_TYPE_NAME: &str = "chunk_transfer";
const OPERATOR_INVOCATION_TYPE_NAME: &str = "operator_invocation";
const TEMPORARY_BLOCK_IO_TYPE_NAME: &str = "temporary_block_io";
const MEMORY_ACCOUNT_TYPE_NAME: &str = "memory_account";
const MEASURE_CHUNKS: &str = "chunks";
const MEASURE_ROWS: &str = "rows";
const MEASURE_LOGICAL_BYTES: &str = "logical_bytes";
const DATA_FLOW_STATE: &str = "published";
const NANOS_PER_SECOND: f64 = 1_000_000_000.0;

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

struct TemporaryBlockIoCollection<'a>(&'a std::collections::HashMap<Uuid, TemporaryBlockIo>);

impl FsmCollection for TemporaryBlockIoCollection<'_> {
    type Fsm = TemporaryBlockIo;

    fn fsms(&self) -> impl Iterator<Item = &TemporaryBlockIo> {
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

struct QueryTasks {
    operators: HashMap<Uuid, HashSet<Uuid>>,
    plans: HashMap<Uuid, Uuid>,
}

#[derive(Clone, Copy)]
enum RateScale {
    Native,
    PerSecond,
}

struct ClippedUsage<U> {
    inner: U,
    span: SpanUnixNanoSec,
}

impl<'a, U: Usage<'a>> Usage<'a> for ClippedUsage<U> {
    fn entity_id(&self) -> Uuid {
        self.inner.entity_id()
    }

    fn resource_id(&self) -> Uuid {
        self.inner.resource_id()
    }

    fn capacities(&self) -> impl Iterator<Item = &'a CapacityValue> {
        self.inner.capacities()
    }

    fn span(&self) -> SpanUnixNanoSec {
        self.span
    }
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
            temporary_block_ios = model.temporary_block_ios.len(),
            memory_accounts = model.memory_accounts.len(),
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
        let temporary_io_decl = TemporaryBlockIo::fsm_type_declaration();
        let memory_account_decl = MemoryAccount::fsm_type_declaration();
        let fsm_types = [
            (task_decl.name.clone(), task_decl),
            (transfer_decl.name.clone(), transfer_decl),
            (invocation_decl.name.clone(), invocation_decl),
            (temporary_io_decl.name.clone(), temporary_io_decl),
            (memory_account_decl.name.clone(), memory_account_decl),
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
                (
                    IO_OPERATIONS_CAPACITY_NAME.to_owned(),
                    QuantitySpec {
                        symbol: String::new(),
                        singular: "operation".to_owned(),
                        plural: "operations".to_owned(),
                        occupancy_prefix: PrefixSystem::None,
                        rate_prefix: PrefixSystem::Si,
                    },
                ),
                (
                    IO_BUFFER_BYTES_CAPACITY_NAME.to_owned(),
                    QuantitySpec::bytes(),
                ),
                (MEMORY_BYTES_CAPACITY_NAME.to_owned(), QuantitySpec::bytes()),
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
        let tasks = self.query_tasks(query_id, &topology);
        let task_ids = tasks.operators.keys().copied().collect();
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
                            &tasks.operators,
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
            Some(TEMPORARY_BLOCK_IO_TYPE_NAME) => entities::list_entities(
                &TemporaryBlockIoCollection(&self.model.temporary_block_ios),
                |io| {
                    self.temporary_io_matches(io, query_id, &topology, &tasks, &filter)
                        && io.span().is_ok_and(|span| span.intersects(&window))
                },
                query,
            ),
            // Accounts are engine-lived and begin before the query epoch. They
            // power query-clipped timelines but cannot be rendered as query FSMs.
            Some(MEMORY_ACCOUNT_TYPE_NAME) => Ok(EntityListResponse {
                items: vec![],
                total: 0,
            }),
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
            .operators
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
        let query_span = self.model.query(query_id)?.span()?;
        let topology = self.query_topology(query_id)?;
        let tasks = self.query_tasks(query_id, &topology);
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
        let rate_scale = if resource_type.name == crate::model::TEMPORARY_IO_CHANNEL_TYPE_NAME {
            RateScale::PerSecond
        } else {
            RateScale::Native
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
                                    &tasks.operators,
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
                TEMPORARY_BLOCK_IO_TYPE_NAME => {
                    for io in self.model.temporary_block_ios.values().filter(|io| {
                        self.temporary_io_matches(io, query_id, &topology, &tasks, &operator_filter)
                    }) {
                        for (state, usage) in io.usages_with_state_names() {
                            if resource_ids.contains(&usage.resource_id()) {
                                builder.try_push(state, &usage)?;
                            }
                        }
                    }
                }
                MEMORY_ACCOUNT_TYPE_NAME => {
                    for account in
                        self.model.memory_accounts.values().filter(|account| {
                            self.memory_account_matches(account, &operator_filter)
                        })
                    {
                        let Some(memory_tag) = account.memory_tag() else {
                            continue;
                        };
                        for (_, usage) in account.usages_with_state_names() {
                            if resource_ids.contains(&usage.resource_id()) {
                                let Some(span) = usage.span().intersection(&query_span) else {
                                    continue;
                                };
                                builder
                                    .try_push(memory_tag, &ClippedUsage { inner: usage, span })?;
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
            self.timeline_to_ui_keyed(builder.build(), epoch, rate_scale)?
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
            for io in self.model.temporary_block_ios.values().filter(|io| {
                self.temporary_io_matches(io, query_id, &topology, &tasks, &operator_filter)
            }) {
                for usage in io.usages() {
                    if resource_ids.contains(&usage.resource_id()) {
                        builder.try_push(&usage)?;
                    }
                }
            }
            for account in self
                .model
                .memory_accounts
                .values()
                .filter(|account| self.memory_account_matches(account, &operator_filter))
            {
                for usage in account.usages() {
                    if resource_ids.contains(&usage.resource_id()) {
                        let Some(span) = usage.span().intersection(&query_span) else {
                            continue;
                        };
                        builder.try_push(&ClippedUsage { inner: usage, span })?;
                    }
                }
            }
            self.timeline_to_ui(builder.build(), epoch, rate_scale)?
        };

        Ok(SingleTimelineResponse {
            config: config_secs,
            data,
        })
    }

    fn query_tasks(&self, query_id: Uuid, topology: &QueryTopology) -> QueryTasks {
        let operators = self
            .model
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
            .collect::<HashMap<_, _>>();
        let plans = operators
            .keys()
            .filter_map(|id| {
                self.model
                    .pipeline_tasks
                    .get(id)
                    .and_then(task_plan_id)
                    .map(|plan_id| (*id, plan_id))
            })
            .collect();

        QueryTasks { operators, plans }
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

    fn temporary_io_matches(
        &self,
        io: &TemporaryBlockIo,
        query_id: Uuid,
        topology: &QueryTopology,
        tasks: &QueryTasks,
        filter: &OperatorFilter,
    ) -> bool {
        io.query_id() == Some(query_id)
            && io.is_complete()
            && io.belongs_to_query(
                &topology.operator_plans,
                &topology.plan_workers,
                &topology.plan_ids,
                &tasks.plans,
                &tasks.operators,
            )
            && io.resources_are_valid(&topology.plan_workers, &self.model.runtime_resources)
            && io.matches_operator(filter)
    }

    fn memory_account_matches(&self, account: &MemoryAccount, filter: &OperatorFilter) -> bool {
        let Ok(engine) = self.model.query_engine.engine() else {
            return false;
        };

        account.is_complete()
            && account.is_valid(engine.id(), &self.model.runtime_resources)
            && account.matches_operator(filter)
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
                if let Some(io) = self.model.temporary_block_ios.get(id) {
                    return Some(FiniteStateMachine::try_from_fsm(io, epoch));
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
        rate_scale: RateScale,
    ) -> AnalyzerResult<UiResourceTimeline> {
        Ok(UiResourceTimeline::Binned(ResourceTimelineBinned {
            config: timeline.config.try_to_secs_relative(epoch)?,
            capacities_values: timeline
                .data
                .into_iter()
                .map(|(name, mut values)| {
                    normalize_rate(rate_scale, name, &mut values);
                    (name.to_owned(), values)
                })
                .collect(),
            long_fsms: self.entities_to_ui(&timeline.long_entities, epoch)?,
        }))
    }

    fn timeline_to_ui_keyed(
        &self,
        timeline: ResourceTimelineByKey<'_, &str>,
        epoch: TimeUnixNanoSec,
        rate_scale: RateScale,
    ) -> AnalyzerResult<UiResourceTimeline> {
        let mut capacities_states_values = HashMap::new();
        for ((state, capacity), mut values) in timeline.data {
            normalize_rate(rate_scale, capacity, &mut values);
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
        let task_ids: HashSet<Uuid> = self
            .query_tasks(query_id, &topology)
            .operators
            .into_keys()
            .collect();
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

fn normalize_rate(rate_scale: RateScale, capacity: &str, values: &mut [f64]) {
    if !matches!(rate_scale, RateScale::PerSecond)
        || !matches!(
            capacity,
            IO_OPERATIONS_CAPACITY_NAME | IO_BUFFER_BYTES_CAPACITY_NAME
        )
    {
        return;
    }

    // Quent computes per-nanosecond rates; the UI displays per second.
    for value in values {
        *value *= NANOS_PER_SECOND;
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{
        BUFFER_POOL_MEMORY_TYPE_NAME, EXECUTION_THREAD_TYPE_NAME, MEMORY_BYTES_CAPACITY_NAME,
        QUEUE_ENTRIES_CAPACITY_NAME, TASK_QUEUE_TYPE_NAME, TEMPORARY_DIRECTORY_STORAGE_TYPE_NAME,
        TEMPORARY_IO_CHANNEL_TYPE_NAME, TEMPORARY_STORAGE_TYPE_NAME,
    };
    use duckdb_telemetry_model::{
        chunk_transfer, engine, memory_account, operator, operator_invocation, pipeline_task, plan,
        port, query, query_group, runtime_resource, temporary_block_io, worker,
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
    const TEMPORARY_IO_CHANNEL_ID: Uuid = Uuid::from_u128(25);
    const TEMPORARY_IO_ID: Uuid = Uuid::from_u128(26);
    const INVALID_TEMPORARY_IO_ID: Uuid = Uuid::from_u128(27);
    const NIL_TEMPORARY_IO_ID: Uuid = Uuid::from_u128(28);
    const BUFFER_POOL_MEMORY_ID: Uuid = Uuid::from_u128(29);
    const TEMPORARY_STORAGE_ID: Uuid = Uuid::from_u128(30);
    const TEMPORARY_DIRECTORY_ID: Uuid = Uuid::from_u128(31);
    const HASH_TABLE_MEMORY_ID: Uuid = Uuid::from_u128(32);
    const ORDER_BY_MEMORY_ID: Uuid = Uuid::from_u128(33);
    const TEMPORARY_MEMORY_ID: Uuid = Uuid::from_u128(34);
    const DIRECTORY_MEMORY_ID: Uuid = Uuid::from_u128(35);
    const INVALID_MEMORY_ID: Uuid = Uuid::from_u128(36);
    const ONE_SECOND_NS: u64 = 1_000_000_000;

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

    fn temporary_io_usage(
        operations: u64,
        buffer_bytes: u64,
    ) -> Usage<runtime_resource::TemporaryIoChannel> {
        Usage {
            resource_id: Ref::new(TEMPORARY_IO_CHANNEL_ID),
            capacity: runtime_resource::TemporaryIoChannelOperating {
                capacity_operations: Capacity::new(Some(operations)),
                capacity_buffer_bytes: Capacity::new(Some(buffer_bytes)),
            },
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

    fn temporary_io_channel_events(parent_id: Uuid) -> Vec<Event<DuckDBEvent>> {
        vec![
            event(
                TEMPORARY_IO_CHANNEL_ID,
                110,
                DuckDBEvent::TemporaryIoChannel(fsm(
                    0,
                    runtime_resource::TemporaryIoChannelTransition::TemporaryIoChannelInitializing(
                        runtime_resource::TemporaryIoChannelInitializing {
                            instance_name: "temporary-storage".to_owned(),
                            parent_group_id: parent_id,
                            resource_type_name: TEMPORARY_IO_CHANNEL_TYPE_NAME.to_owned(),
                        },
                    ),
                )),
            ),
            event(
                TEMPORARY_IO_CHANNEL_ID,
                111,
                DuckDBEvent::TemporaryIoChannel(fsm(
                    1,
                    runtime_resource::TemporaryIoChannelTransition::TemporaryIoChannelOperating(
                        runtime_resource::TemporaryIoChannelOperating {
                            capacity_operations: Capacity::new(None),
                            capacity_buffer_bytes: Capacity::new(None),
                        },
                    ),
                )),
            ),
            event(
                TEMPORARY_IO_CHANNEL_ID,
                ONE_SECOND_NS + 123,
                DuckDBEvent::TemporaryIoChannel(fsm(
                    2,
                    runtime_resource::TemporaryIoChannelTransition::TemporaryIoChannelFinalizing(
                        runtime_resource::TemporaryIoChannelFinalizing,
                    ),
                )),
            ),
            event(
                TEMPORARY_IO_CHANNEL_ID,
                ONE_SECOND_NS + 124,
                DuckDBEvent::TemporaryIoChannel(fsm(
                    3,
                    runtime_resource::TemporaryIoChannelTransition::Exit,
                )),
            ),
        ]
    }

    fn memory_resource_events(parent_id: Uuid) -> Vec<Event<DuckDBEvent>> {
        vec![
            event(
                BUFFER_POOL_MEMORY_ID,
                90,
                DuckDBEvent::BufferPoolMemory(fsm(
                    0,
                    memory_account::BufferPoolMemoryTransition::BufferPoolMemoryInitializing(
                        memory_account::BufferPoolMemoryInitializing {
                            instance_name: "buffer-pool-memory".to_owned(),
                            parent_group_id: parent_id,
                            resource_type_name: BUFFER_POOL_MEMORY_TYPE_NAME.to_owned(),
                        },
                    ),
                )),
            ),
            event(
                BUFFER_POOL_MEMORY_ID,
                91,
                DuckDBEvent::BufferPoolMemory(fsm(
                    1,
                    memory_account::BufferPoolMemoryTransition::BufferPoolMemoryOperating(
                        memory_account::BufferPoolMemoryOperating {
                            capacity_bytes: Capacity::new(Some(4096)),
                        },
                    ),
                )),
            ),
            event(
                TEMPORARY_STORAGE_ID,
                90,
                DuckDBEvent::TemporaryStorage(fsm(
                    0,
                    memory_account::TemporaryStorageTransition::TemporaryStorageInitializing(
                        memory_account::TemporaryStorageInitializing {
                            instance_name: "temporary-storage".to_owned(),
                            parent_group_id: parent_id,
                            resource_type_name: TEMPORARY_STORAGE_TYPE_NAME.to_owned(),
                        },
                    ),
                )),
            ),
            event(
                TEMPORARY_STORAGE_ID,
                91,
                DuckDBEvent::TemporaryStorage(fsm(
                    1,
                    memory_account::TemporaryStorageTransition::TemporaryStorageOperating(
                        memory_account::TemporaryStorageOperating {
                            capacity_bytes: Capacity::new(Some(2048)),
                        },
                    ),
                )),
            ),
            event(
                TEMPORARY_DIRECTORY_ID,
                90,
                DuckDBEvent::TemporaryDirectoryStorage(fsm(
                    0,
                    memory_account::TemporaryDirectoryStorageTransition::TemporaryDirectoryStorageInitializing(
                        memory_account::TemporaryDirectoryStorageInitializing {
                            instance_name: "temporary-directory-storage".to_owned(),
                            parent_group_id: parent_id,
                            resource_type_name: TEMPORARY_DIRECTORY_STORAGE_TYPE_NAME.to_owned(),
                        },
                    ),
                )),
            ),
            event(
                TEMPORARY_DIRECTORY_ID,
                91,
                DuckDBEvent::TemporaryDirectoryStorage(fsm(
                    1,
                    memory_account::TemporaryDirectoryStorageTransition::TemporaryDirectoryStorageOperating(
                        memory_account::TemporaryDirectoryStorageOperating {
                            capacity_bytes: Capacity::new(Some(8192)),
                        },
                    ),
                )),
            ),
        ]
    }

    #[derive(Clone, Copy)]
    enum MemoryDomain {
        BufferPool,
        TemporaryStorage,
        TemporaryDirectory,
    }

    fn memory_accounted(domain: MemoryDomain, bytes: u64) -> memory_account::Accounted {
        let mut accounted = memory_account::Accounted {
            buffer_pool: None,
            temporary_storage: None,
            temporary_directory: None,
        };
        match domain {
            MemoryDomain::BufferPool => {
                accounted.buffer_pool = Some(Usage {
                    resource_id: Ref::new(BUFFER_POOL_MEMORY_ID),
                    capacity: memory_account::BufferPoolMemoryOperating {
                        capacity_bytes: Capacity::new(Some(bytes)),
                    },
                });
            }
            MemoryDomain::TemporaryStorage => {
                accounted.temporary_storage = Some(Usage {
                    resource_id: Ref::new(TEMPORARY_STORAGE_ID),
                    capacity: memory_account::TemporaryStorageOperating {
                        capacity_bytes: Capacity::new(Some(bytes)),
                    },
                });
            }
            MemoryDomain::TemporaryDirectory => {
                accounted.temporary_directory = Some(Usage {
                    resource_id: Ref::new(TEMPORARY_DIRECTORY_ID),
                    capacity: memory_account::TemporaryDirectoryStorageOperating {
                        capacity_bytes: Capacity::new(Some(bytes)),
                    },
                });
            }
        }
        accounted
    }

    fn memory_account_events(
        id: Uuid,
        memory_tag: &str,
        domain: MemoryDomain,
        first_bytes: u64,
        updated_bytes: Option<u64>,
        exit: bool,
    ) -> Vec<Event<DuckDBEvent>> {
        let mut events = vec![
            event(
                id,
                92,
                DuckDBEvent::MemoryAccount(fsm(
                    0,
                    memory_account::MemoryAccountTransition::AccountRegistered(
                        memory_account::AccountRegistered {
                            instance_name: format!("memory-account/{memory_tag}"),
                            memory_tag: memory_tag.to_owned(),
                        },
                    ),
                )),
            ),
            event(
                id,
                93,
                DuckDBEvent::MemoryAccount(fsm(
                    1,
                    memory_account::MemoryAccountTransition::Accounted(memory_accounted(
                        domain,
                        first_bytes,
                    )),
                )),
            ),
        ];
        if let Some(bytes) = updated_bytes {
            events.push(event(
                id,
                150,
                DuckDBEvent::MemoryAccount(fsm(
                    2,
                    memory_account::MemoryAccountTransition::Accounted(memory_accounted(
                        domain, bytes,
                    )),
                )),
            ));
        }
        if exit {
            events.push(event(
                id,
                201,
                DuckDBEvent::MemoryAccount(fsm(3, memory_account::MemoryAccountTransition::Exit)),
            ));
        }
        events
    }

    fn memory_events(exit: bool) -> Vec<Event<DuckDBEvent>> {
        memory_resource_events(ENGINE_ID)
            .into_iter()
            .chain(memory_account_events(
                HASH_TABLE_MEMORY_ID,
                "HASH_TABLE",
                MemoryDomain::BufferPool,
                100,
                Some(40),
                exit,
            ))
            .chain(memory_account_events(
                ORDER_BY_MEMORY_ID,
                "ORDER_BY",
                MemoryDomain::BufferPool,
                50,
                None,
                exit,
            ))
            .chain(memory_account_events(
                TEMPORARY_MEMORY_ID,
                "HASH_TABLE",
                MemoryDomain::TemporaryStorage,
                80,
                None,
                exit,
            ))
            .chain(memory_account_events(
                DIRECTORY_MEMORY_ID,
                "UNKNOWN",
                MemoryDomain::TemporaryDirectory,
                256,
                None,
                exit,
            ))
            .collect()
    }

    fn memory_analyzer(exit: bool) -> DuckDbUiAnalyzer {
        DuckDbUiAnalyzer::try_new(
            ENGINE_ID,
            base_events().into_iter().chain(memory_events(exit)),
        )
        .unwrap()
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

    fn temporary_io_events(
        id: Uuid,
        task_id: Uuid,
        trigger_operator_id: Uuid,
    ) -> Vec<Event<DuckDBEvent>> {
        vec![
            event(
                id,
                115,
                DuckDBEvent::TemporaryBlockIo(fsm(
                    0,
                    temporary_block_io::TemporaryBlockIoTransition::IoRequested(
                        temporary_block_io::IoRequested {
                            instance_name: "write block 42".to_owned(),
                            query_id: QUERY_ID,
                            plan_id: PLAN_ID,
                            task_id,
                            trigger_operator_id,
                            block_id: 42,
                            memory_tag: "HASH_TABLE".to_owned(),
                            direction: "write".to_owned(),
                        },
                    ),
                )),
            ),
            event(
                id,
                120,
                DuckDBEvent::TemporaryBlockIo(fsm(
                    1,
                    temporary_block_io::TemporaryBlockIoTransition::IoActive(
                        temporary_block_io::IoActive {
                            channel: Some(temporary_io_usage(1, 100)),
                        },
                    ),
                )),
            ),
            event(
                id,
                ONE_SECOND_NS + 120,
                DuckDBEvent::TemporaryBlockIo(fsm(
                    2,
                    temporary_block_io::TemporaryBlockIoTransition::IoCompleted(
                        temporary_block_io::IoCompleted {
                            instance_name: String::new(),
                            success: true,
                            storage_bytes: 80,
                        },
                    ),
                )),
            ),
            event(
                id,
                ONE_SECOND_NS + 121,
                DuckDBEvent::TemporaryBlockIo(fsm(
                    3,
                    temporary_block_io::TemporaryBlockIoTransition::Exit,
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

    fn temporary_io_analyzer(
        id: Uuid,
        task_id: Uuid,
        trigger_operator_id: Uuid,
        parent_id: Uuid,
    ) -> DuckDbUiAnalyzer {
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
            .chain(temporary_io_channel_events(parent_id))
            .chain(temporary_io_events(id, task_id, trigger_operator_id));

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

    fn temporary_io_request(
        operator_ids: Vec<Uuid>,
    ) -> SingleTimelineRequest<QueryFilter, OperatorFilter> {
        SingleTimelineRequest {
            entry: TimelineRequest::Resource(ResourceTimelineRequest {
                resource_id: TEMPORARY_IO_CHANNEL_ID,
                long_entities_threshold_s: None,
                entity_filter: EntityFilter {
                    entity_type_name: Some(TEMPORARY_BLOCK_IO_TYPE_NAME.to_owned()),
                },
                application: OperatorFilter { operator_ids },
                config: TimelineConfig {
                    num_bins: 1,
                    start: to_secs(20),
                    end: to_secs(ONE_SECOND_NS + 20),
                },
            }),
            app_params: QueryFilter { query_id: QUERY_ID },
        }
    }

    fn memory_request(
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
                    num_bins: 2,
                    start: to_secs(20),
                    end: to_secs(100),
                },
            }),
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
        assert!(
            bundle
                .entities
                .fsm_types
                .contains_key(TEMPORARY_BLOCK_IO_TYPE_NAME)
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
    fn bundle_declares_memory_gauges() {
        let bundle = memory_analyzer(true).query_bundle(QUERY_ID).unwrap();

        assert!(
            bundle
                .entities
                .fsm_types
                .contains_key(MEMORY_ACCOUNT_TYPE_NAME)
        );
        assert!(
            bundle
                .quantity_specs
                .contains_key(MEMORY_BYTES_CAPACITY_NAME)
        );
        for type_name in [
            BUFFER_POOL_MEMORY_TYPE_NAME,
            TEMPORARY_STORAGE_TYPE_NAME,
            TEMPORARY_DIRECTORY_STORAGE_TYPE_NAME,
        ] {
            let resource_type = &bundle.entities.resource_types[type_name];
            assert_eq!(resource_type.capacities.len(), 1);
            assert!(matches!(
                resource_type.capacities[0].kind,
                CapacityKind::Occupancy
            ));
            assert!(
                resource_type
                    .used_by
                    .contains(&MEMORY_ACCOUNT_TYPE_NAME.to_owned())
            );
        }
    }

    #[test]
    fn prequery_memory_accounts_are_timeline_only() {
        let accounts = memory_analyzer(true)
            .list_entities(list_request(MEMORY_ACCOUNT_TYPE_NAME, vec![]))
            .unwrap();

        assert_eq!(accounts.total, 0);
        assert!(accounts.items.is_empty());
    }

    #[test]
    fn memory_timelines_report_totals_and_tags() {
        let analyzer = memory_analyzer(true);

        let response = analyzer
            .single_resource_timeline(memory_request(BUFFER_POOL_MEMORY_ID, None, vec![]))
            .unwrap();
        let UiResourceTimeline::Binned(timeline) = response.data else {
            panic!("expected plain memory timeline");
        };
        assert_eq!(
            timeline.capacities_values[MEMORY_BYTES_CAPACITY_NAME],
            [135.0, 90.0]
        );

        let response = analyzer
            .single_resource_timeline(memory_request(
                BUFFER_POOL_MEMORY_ID,
                Some(MEMORY_ACCOUNT_TYPE_NAME),
                vec![],
            ))
            .unwrap();
        let UiResourceTimeline::BinnedByState(timeline) = response.data else {
            panic!("expected memory-tag timeline");
        };
        assert_eq!(
            timeline.capacities_states_values[MEMORY_BYTES_CAPACITY_NAME]["HASH_TABLE"],
            [85.0, 40.0]
        );
        assert_eq!(
            timeline.capacities_states_values[MEMORY_BYTES_CAPACITY_NAME]["ORDER_BY"],
            [50.0, 50.0]
        );

        for (resource_id, bytes) in [
            (TEMPORARY_STORAGE_ID, 80.0),
            (TEMPORARY_DIRECTORY_ID, 256.0),
        ] {
            let response = analyzer
                .single_resource_timeline(memory_request(resource_id, None, vec![]))
                .unwrap();
            let UiResourceTimeline::Binned(timeline) = response.data else {
                panic!("expected plain memory timeline");
            };
            assert_eq!(
                timeline.capacities_values[MEMORY_BYTES_CAPACITY_NAME],
                [bytes, bytes]
            );
        }
    }

    #[test]
    fn memory_timeline_clips_to_query_span() {
        let analyzer = memory_analyzer(false);
        let request = |entity_type_name: Option<&str>| SingleTimelineRequest {
            entry: TimelineRequest::Resource(ResourceTimelineRequest {
                resource_id: BUFFER_POOL_MEMORY_ID,
                long_entities_threshold_s: None,
                entity_filter: EntityFilter {
                    entity_type_name: entity_type_name.map(str::to_owned),
                },
                application: OperatorFilter {
                    operator_ids: vec![],
                },
                config: TimelineConfig {
                    num_bins: 4,
                    start: to_secs(80),
                    end: to_secs(120),
                },
            }),
            app_params: QueryFilter { query_id: QUERY_ID },
        };

        let response = analyzer.single_resource_timeline(request(None)).unwrap();
        let UiResourceTimeline::Binned(timeline) = response.data else {
            panic!("expected plain memory timeline");
        };
        assert_eq!(
            timeline.capacities_values[MEMORY_BYTES_CAPACITY_NAME],
            [90.0, 90.0, 0.0, 0.0]
        );

        let response = analyzer
            .single_resource_timeline(request(Some(MEMORY_ACCOUNT_TYPE_NAME)))
            .unwrap();
        let UiResourceTimeline::BinnedByState(timeline) = response.data else {
            panic!("expected memory-tag timeline");
        };
        assert_eq!(
            timeline.capacities_states_values[MEMORY_BYTES_CAPACITY_NAME]["HASH_TABLE"],
            [40.0, 40.0, 0.0, 0.0]
        );
        assert_eq!(
            timeline.capacities_states_values[MEMORY_BYTES_CAPACITY_NAME]["ORDER_BY"],
            [50.0, 50.0, 0.0, 0.0]
        );
    }

    #[test]
    fn init_only_memory_resource_is_snapshot_safe() {
        let events = base_events()
            .into_iter()
            .chain(memory_resource_events(ENGINE_ID).into_iter().take(1));
        let analyzer = DuckDbUiAnalyzer::try_new(ENGINE_ID, events).unwrap();

        assert!(
            analyzer
                .query_bundle(QUERY_ID)
                .unwrap()
                .entities
                .resources
                .contains_key(&BUFFER_POOL_MEMORY_ID)
        );
    }

    #[test]
    fn memory_accounts_ignore_operator_selection() {
        let analyzer = memory_analyzer(true);
        let accounts = analyzer
            .list_entities(list_request(MEMORY_ACCOUNT_TYPE_NAME, vec![SOURCE_ID]))
            .unwrap();
        assert_eq!(accounts.total, 0);

        let response = analyzer
            .single_resource_timeline(memory_request(BUFFER_POOL_MEMORY_ID, None, vec![SOURCE_ID]))
            .unwrap();
        let UiResourceTimeline::Binned(timeline) = response.data else {
            panic!("expected plain memory timeline");
        };
        let total = timeline
            .capacities_values
            .get(MEMORY_BYTES_CAPACITY_NAME)
            .into_iter()
            .flatten()
            .sum::<f64>();
        assert_eq!(total, 0.0);

        let response = analyzer
            .single_resource_timeline(memory_request(
                BUFFER_POOL_MEMORY_ID,
                Some(MEMORY_ACCOUNT_TYPE_NAME),
                vec![SOURCE_ID],
            ))
            .unwrap();
        let UiResourceTimeline::BinnedByState(timeline) = response.data else {
            panic!("expected memory-tag timeline");
        };
        let total = timeline
            .capacities_states_values
            .get(MEMORY_BYTES_CAPACITY_NAME)
            .into_iter()
            .flat_map(|tags| tags.values())
            .flatten()
            .sum::<f64>();
        assert_eq!(total, 0.0);
    }

    #[test]
    fn live_memory_accounts_are_closed_for_the_snapshot() {
        let analyzer = memory_analyzer(false);

        assert!(
            analyzer
                .model
                .memory_accounts
                .values()
                .all(MemoryAccountExt::is_complete)
        );
        assert!(
            analyzer
                .single_resource_timeline(memory_request(BUFFER_POOL_MEMORY_ID, None, vec![]))
                .is_ok()
        );
    }

    #[test]
    fn memory_accounts_validate_domain_and_parent() {
        let mut malformed = memory_accounted(MemoryDomain::BufferPool, 10);
        malformed.temporary_storage =
            memory_accounted(MemoryDomain::TemporaryStorage, 20).temporary_storage;
        let malformed_events = [
            event(
                INVALID_MEMORY_ID,
                112,
                DuckDBEvent::MemoryAccount(fsm(
                    0,
                    memory_account::MemoryAccountTransition::AccountRegistered(
                        memory_account::AccountRegistered {
                            instance_name: "malformed".to_owned(),
                            memory_tag: "HASH_TABLE".to_owned(),
                        },
                    ),
                )),
            ),
            event(
                INVALID_MEMORY_ID,
                113,
                DuckDBEvent::MemoryAccount(fsm(
                    1,
                    memory_account::MemoryAccountTransition::Accounted(malformed),
                )),
            ),
            event(
                INVALID_MEMORY_ID,
                201,
                DuckDBEvent::MemoryAccount(fsm(2, memory_account::MemoryAccountTransition::Exit)),
            ),
        ];
        let events = base_events()
            .into_iter()
            .chain(memory_resource_events(WORKER_ID))
            .chain(memory_account_events(
                HASH_TABLE_MEMORY_ID,
                "HASH_TABLE",
                MemoryDomain::BufferPool,
                10,
                None,
                true,
            ))
            .chain(malformed_events);
        let analyzer = DuckDbUiAnalyzer::try_new(ENGINE_ID, events).unwrap();

        let response = analyzer
            .single_resource_timeline(memory_request(BUFFER_POOL_MEMORY_ID, None, vec![]))
            .unwrap();
        let UiResourceTimeline::Binned(timeline) = response.data else {
            panic!("expected plain memory timeline");
        };
        let total = timeline
            .capacities_values
            .get(MEMORY_BYTES_CAPACITY_NAME)
            .into_iter()
            .flatten()
            .sum::<f64>();
        assert_eq!(total, 0.0);
        assert!(
            !analyzer.model.runtime_resources.resource_types[BUFFER_POOL_MEMORY_TYPE_NAME]
                .used_by
                .contains(MEMORY_ACCOUNT_TYPE_NAME)
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
    fn operator_selection_filters_runtime_timelines() {
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
            .chain(invocation_events());
        let analyzer = DuckDbUiAnalyzer::try_new(ENGINE_ID, events).unwrap();

        for entity_type in [PIPELINE_TASK_TYPE_NAME, OPERATOR_INVOCATION_TYPE_NAME] {
            let selected = analyzer
                .single_resource_timeline(single_resource_request(
                    EXECUTION_THREAD_ID,
                    Some(entity_type),
                    vec![SOURCE_ID],
                ))
                .unwrap();
            let unselected = analyzer
                .single_resource_timeline(single_resource_request(
                    EXECUTION_THREAD_ID,
                    Some(entity_type),
                    vec![TARGET_ID],
                ))
                .unwrap();
            let UiResourceTimeline::BinnedByState(selected) = selected.data else {
                panic!("expected keyed resource timeline");
            };
            let UiResourceTimeline::BinnedByState(unselected) = unselected.data else {
                panic!("expected keyed resource timeline");
            };

            let selected_usage = selected.capacities_states_values["unit"]
                .values()
                .flatten()
                .sum::<f64>();
            let unselected_usage = unselected
                .capacities_states_values
                .get("unit")
                .into_iter()
                .flat_map(|states| states.values())
                .flatten()
                .sum::<f64>();

            assert!(selected_usage > 0.0);
            assert_eq!(unselected_usage, 0.0);
        }
    }

    #[test]
    fn temporary_io_reports_per_second_rates() {
        let analyzer = temporary_io_analyzer(TEMPORARY_IO_ID, TASK_ID, SOURCE_ID, WORKER_ID);
        let bundle = analyzer.query_bundle(QUERY_ID).unwrap();
        let resource_type = &bundle.entities.resource_types[TEMPORARY_IO_CHANNEL_TYPE_NAME];

        assert_eq!(resource_type.capacities.len(), 2);
        assert!(
            resource_type
                .capacities
                .iter()
                .all(|capacity| matches!(capacity.kind, CapacityKind::Rate))
        );
        assert!(
            bundle
                .quantity_specs
                .contains_key(IO_OPERATIONS_CAPACITY_NAME)
        );
        assert!(
            bundle
                .quantity_specs
                .contains_key(IO_BUFFER_BYTES_CAPACITY_NAME)
        );
        assert!(
            resource_type
                .used_by
                .contains(&TEMPORARY_BLOCK_IO_TYPE_NAME.to_owned())
        );

        let selected = analyzer
            .list_entities(list_request(TEMPORARY_BLOCK_IO_TYPE_NAME, vec![SOURCE_ID]))
            .unwrap();
        let unselected = analyzer
            .list_entities(list_request(TEMPORARY_BLOCK_IO_TYPE_NAME, vec![TARGET_ID]))
            .unwrap();
        assert_eq!(selected.total, 1);
        assert_eq!(unselected.total, 0);

        let response = analyzer
            .single_resource_timeline(temporary_io_request(vec![SOURCE_ID]))
            .unwrap();
        let UiResourceTimeline::BinnedByState(timeline) = response.data else {
            panic!("expected keyed resource timeline");
        };
        let operations =
            timeline.capacities_states_values[IO_OPERATIONS_CAPACITY_NAME]["io_active"][0];
        let buffer_bytes =
            timeline.capacities_states_values[IO_BUFFER_BYTES_CAPACITY_NAME]["io_active"][0];

        assert!((operations - 1.0).abs() < f64::EPSILON);
        assert!((buffer_bytes - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn temporary_io_validates_links_and_nil_trigger() {
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
            .chain(temporary_io_channel_events(WORKER_ID))
            .chain(temporary_io_events(
                INVALID_TEMPORARY_IO_ID,
                TASK_ID,
                TARGET_ID,
            ))
            .chain(temporary_io_events(
                NIL_TEMPORARY_IO_ID,
                Uuid::nil(),
                Uuid::nil(),
            ));
        let analyzer = DuckDbUiAnalyzer::try_new(ENGINE_ID, events).unwrap();

        let all = analyzer
            .list_entities(list_request(TEMPORARY_BLOCK_IO_TYPE_NAME, vec![]))
            .unwrap();
        let selected = analyzer
            .list_entities(list_request(TEMPORARY_BLOCK_IO_TYPE_NAME, vec![SOURCE_ID]))
            .unwrap();

        assert_eq!(all.total, 1);
        assert_eq!(selected.total, 0);
    }

    #[test]
    fn temporary_io_channel_gets_snapshot_exit() {
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
            .chain(temporary_io_channel_events(WORKER_ID).into_iter().take(2))
            .chain(temporary_io_events(TEMPORARY_IO_ID, TASK_ID, SOURCE_ID));

        assert!(DuckDbUiAnalyzer::try_new(ENGINE_ID, events).is_ok());
    }

    #[test]
    fn temporary_io_requires_worker_channel() {
        let analyzer =
            temporary_io_analyzer(TEMPORARY_IO_ID, TASK_ID, SOURCE_ID, UNKNOWN_WORKER_ID);
        let entities = analyzer
            .list_entities(list_request(TEMPORARY_BLOCK_IO_TYPE_NAME, vec![]))
            .unwrap();

        assert_eq!(entities.total, 0);
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
