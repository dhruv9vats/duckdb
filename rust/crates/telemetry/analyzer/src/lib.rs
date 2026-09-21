use std::collections::{BTreeMap, HashMap, HashSet};

use duckdb_telemetry_store::{
    ChunkTransferEvent, DuckDb, DuckDbEvent, MemoryAccountEvent, OperatorInvocationEvent,
    PipelineTaskEvent, TemporaryBlockIoEvent,
};
use quent_analyzer::{
    AnalyzerError, AnalyzerResult, Entity, RefTreeEntity, Span,
    context::ContextInventory,
    fsm::{
        FsmUsages, Transition,
        native::{AnalyzedTransition, TransitionEvent},
    },
    resource::{
        CapacityValue, Usage, Using, collection::ResourceCollection, tree::ResourceTreeNode,
    },
    timeline::binned::resource::{
        ResourceTimeline, ResourceTimelineBuilder, ResourceTimelineByKey,
        ResourceTimelineByKeyBuilder,
    },
};
use quent_dynamic_attributes::DynamicAttribute;
use quent_events::Event;
pub use quent_query_engine_analyzer::QueryEngineModel;
use quent_query_engine_analyzer::entities;
use quent_query_engine_analyzer::ui::{QuentViewer, UiAnalyzer, ViewerEventStream};
use quent_query_engine_analyzer::{
    EngineEntity, OperatorEntity, PlanEntity, PortEntity, QueryEntity, QueryGroupEntity,
    WorkerEntity,
};
use quent_query_engine_ui::{
    DataFlowTimelineBinned, EntityRef, OperatorFilter, QueryBundle, QueryEntities, QueryFilter,
};
use quent_store::event::{EntityEventStore, ModelEventStore, filesystem::Store};
use quent_time::{
    TimeNanoSec, TimeUnixNanoSec, Timestamp, span::SpanUnixNanoSec, to_nanosecs, to_secs,
    try_to_secs_relative,
};
use quent_ui::{
    FiniteStateMachine, FsmTransition, FsmUsage, Resource, ResourceGroupNode, ResourceTree,
    convert_resource_tree,
    entities::{
        request::{EntityListRequest, EntitySortKey, SortDir},
        response::{EntityListItem, EntityListResponse},
    },
    fsm::FsmTypeDeclaration,
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
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
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

#[path = "entities.rs"]
mod analyzed_entities;
pub mod chunk_transfer;
mod memory_account;
pub mod model;
pub mod operator_invocation;
pub mod pipeline_task;
mod temporary_block_io;
mod view;

const PIPELINE_TASK_TYPE_NAME: &str = "pipeline_task";
const CHUNK_TRANSFER_TYPE_NAME: &str = "chunk_transfer";
const OPERATOR_INVOCATION_TYPE_NAME: &str = "operator_invocation";
const TEMPORARY_BLOCK_IO_TYPE_NAME: &str = "temporary_block_io";
const MEMORY_ACCOUNT_TYPE_NAME: &str = "memory_account";
const MEASURE_CHUNKS: &str = "chunks";
const MEASURE_ROWS: &str = "rows";
const MEASURE_LOGICAL_BYTES: &str = "logical_bytes";
const DATA_FLOW_STATE: &str = "published";
const MEMORY_UPDATES_TOTAL_ATTR: &str = "accounted_updates_total";
const MEMORY_UPDATES_OMITTED_ATTR: &str = "accounted_updates_omitted";
const NANOS_PER_SECOND: f64 = 1_000_000_000.0;

/// `quent-open` entry point for DuckDB telemetry directories.
pub struct Viewer;

impl QuentViewer for Viewer {
    type Analyzer = DuckDbUiAnalyzer;

    fn context_inventory(dir: &std::path::Path) -> quent_io::ImporterResult<ContextInventory> {
        let (context_id, root) = context_location(dir)?;
        let store = Store::<DuckDb>::new(root);
        let engine_ids = store
            .entity_events::<duckdb_telemetry_store::Engine>(context_id)
            .map_err(quent_io::ImporterError::other)?
            .map(|event| event.map(|event| event.id))
            .collect::<Result<HashSet<_>, _>>()
            .map_err(quent_io::ImporterError::other)?;
        let worker_engine_ids = store
            .entity_events::<duckdb_telemetry_store::Worker>(context_id)
            .map_err(quent_io::ImporterError::other)?
            .filter_map(|event| match event {
                Ok(Event {
                    data:
                        duckdb_telemetry_store::WorkerEvent::Init {
                            parent_engine_id, ..
                        },
                    ..
                }) => Some(Ok(parent_engine_id.target)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<HashSet<_>, _>>()
            .map_err(quent_io::ImporterError::other)?;

        Ok(ContextInventory {
            analysis_target_ids: engine_ids.into_iter().chain(worker_engine_ids).collect(),
        })
    }

    fn import_events(
        dir: &std::path::Path,
    ) -> quent_io::ImporterResult<ViewerEventStream<Self::Analyzer>> {
        let (context_id, root) = context_location(dir)?;
        let events = Store::<DuckDb>::new(root)
            .events(context_id)
            .map_err(quent_io::ImporterError::other)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(quent_io::ImporterError::other)?;
        Ok(Box::new(events.into_iter()))
    }
}

fn context_location(dir: &std::path::Path) -> quent_io::ImporterResult<(Uuid, &std::path::Path)> {
    let invalid = || {
        quent_io::ImporterError::other(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("context directory must end in a UUID: {}", dir.display()),
        ))
    };
    let context_id = dir
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| Uuid::parse_str(name).ok())
        .ok_or_else(invalid)?;
    let root = dir.parent().ok_or_else(invalid)?;
    Ok((context_id, root))
}

/// Query-plan analyzer for the first DuckDB telemetry milestone.
pub struct DuckDbUiAnalyzer {
    pub model: DuckDbModel,
    query_topologies: FxHashMap<Uuid, QueryTopology>,
    query_tasks: FxHashMap<Uuid, QueryTasks>,
}

/// Convert a DuckDB FSM into a UI FSM, attaching each transition's own usages.
///
/// This intentionally does not reuse Quent's `try_from_fsm`, which groups usages
/// by state name onto the first transition of that name. DuckDB FSMs (e.g.
/// pipeline tasks) reuse state names as they oscillate between running/ready/
/// blocked, so name-based grouping collapses every interval's usages onto the
/// first occurrence and the UI drops the later intervals. When `clip` is set,
/// transition timestamps are clamped into it so engine-lived, pre-epoch FSMs
/// (memory accounts) stay non-negative relative to the epoch.
trait UiTransitionEvent: TransitionEvent {
    fn ui_attributes(&self) -> Vec<DynamicAttribute>;
}

#[derive(Clone, Copy)]
enum UsageDetail {
    Exact,
    ResourceOnly,
}

fn transition_to_ui<T: UiTransitionEvent>(
    transition: &AnalyzedTransition<T>,
    epoch: TimeUnixNanoSec,
    clip: Option<SpanUnixNanoSec>,
    usage_detail: UsageDetail,
) -> Result<FsmTransition, quent_time::TimeError> {
    let timestamp = match clip {
        Some(span) => transition.timestamp().clamp(span.start(), span.end()),
        None => transition.timestamp(),
    };
    Ok(FsmTransition {
        name: transition.name().to_owned(),
        usages: transition
            .usages()
            .iter()
            .map(|usage| FsmUsage {
                resource: usage.resource_id,
                capacities: match usage_detail {
                    UsageDetail::Exact => usage
                        .capacities
                        .iter()
                        .map(|capacity| (capacity.name.to_string(), capacity.value))
                        .collect(),
                    UsageDetail::ResourceOnly => vec![],
                },
            })
            .collect(),
        timestamp: try_to_secs_relative(timestamp, epoch)?,
        attributes: transition.data.ui_attributes(),
        derived_attributes: vec![],
    })
}

fn native_fsm_to_ui<T: UiTransitionEvent>(
    fsm: &impl Entity,
    transitions: &[AnalyzedTransition<T>],
    instance_name: String,
    epoch: TimeUnixNanoSec,
    clip: Option<SpanUnixNanoSec>,
) -> AnalyzerResult<FiniteStateMachine> {
    let transitions = transitions
        .iter()
        .map(|transition| transition_to_ui(transition, epoch, clip, UsageDetail::Exact))
        .collect::<Result<Vec<_>, quent_time::TimeError>>()?;

    Ok(FiniteStateMachine {
        id: fsm.id(),
        type_name: fsm.type_name().to_owned(),
        instance_name,
        transitions,
    })
}

impl UiTransitionEvent for PipelineTaskEvent {
    fn ui_attributes(&self) -> Vec<DynamicAttribute> {
        match self {
            Self::Created {
                instance_name,
                task_index,
                ..
            } => vec![
                DynamicAttribute::string("instance_name", instance_name),
                DynamicAttribute::u64("task_index", *task_index),
            ],
            Self::Running { mode, cpu_id, .. } => vec![
                DynamicAttribute::string("mode", mode),
                DynamicAttribute::u64("cpu_id", *cpu_id),
            ],
            Self::Finalizing { success, .. } => {
                vec![DynamicAttribute::u8("success", u8::from(*success))]
            }
            _ => vec![],
        }
    }
}

impl UiTransitionEvent for ChunkTransferEvent {
    fn ui_attributes(&self) -> Vec<DynamicAttribute> {
        match self {
            Self::Produced {
                instance_name,
                rows,
                logical_bytes,
                ..
            } => vec![
                DynamicAttribute::string("instance_name", instance_name),
                DynamicAttribute::u64("rows", *rows),
                DynamicAttribute::u64("logical_bytes", *logical_bytes),
            ],
            _ => vec![],
        }
    }
}

impl UiTransitionEvent for OperatorInvocationEvent {
    fn ui_attributes(&self) -> Vec<DynamicAttribute> {
        match self {
            Self::InvocationCreated {
                instance_name,
                phase,
                ..
            } => vec![
                DynamicAttribute::string("instance_name", instance_name),
                DynamicAttribute::string("phase", phase),
            ],
            Self::InvocationRunning {
                input_rows,
                input_logical_bytes,
                ..
            } => vec![
                DynamicAttribute::u64("input_rows", *input_rows),
                DynamicAttribute::u64("input_logical_bytes", *input_logical_bytes),
            ],
            Self::InvocationCompleted {
                success,
                output_rows,
                output_logical_bytes,
                ..
            } => vec![
                DynamicAttribute::u8("success", u8::from(*success)),
                DynamicAttribute::u64("output_rows", *output_rows),
                DynamicAttribute::u64("output_logical_bytes", *output_logical_bytes),
            ],
            _ => vec![],
        }
    }
}

impl UiTransitionEvent for TemporaryBlockIoEvent {
    fn ui_attributes(&self) -> Vec<DynamicAttribute> {
        match self {
            Self::IoRequested {
                instance_name,
                block_id,
                memory_tag,
                direction,
                ..
            } => vec![
                DynamicAttribute::string("instance_name", instance_name),
                DynamicAttribute::u64("block_id", *block_id),
                DynamicAttribute::string("memory_tag", memory_tag),
                DynamicAttribute::string("direction", direction),
            ],
            Self::IoCompleted {
                success,
                storage_bytes,
                ..
            } => vec![
                DynamicAttribute::u8("success", u8::from(*success)),
                DynamicAttribute::u64("storage_bytes", *storage_bytes),
            ],
            _ => vec![],
        }
    }
}

impl UiTransitionEvent for MemoryAccountEvent {
    fn ui_attributes(&self) -> Vec<DynamicAttribute> {
        match self {
            Self::AccountRegistered {
                instance_name,
                memory_tag,
                ..
            } => vec![
                DynamicAttribute::string("instance_name", instance_name),
                DynamicAttribute::string("memory_tag", memory_tag),
            ],
            _ => vec![],
        }
    }
}

trait ToUiFsm: for<'a> FsmUsages<'a> {
    fn to_ui_fsm(
        &self,
        epoch: TimeUnixNanoSec,
        clip: Option<SpanUnixNanoSec>,
    ) -> AnalyzerResult<FiniteStateMachine>;
}

macro_rules! impl_to_ui_fsm {
    ($ty:ty, $event:ty, $variant:path) => {
        impl ToUiFsm for $ty {
            fn to_ui_fsm(
                &self,
                epoch: TimeUnixNanoSec,
                clip: Option<SpanUnixNanoSec>,
            ) -> AnalyzerResult<FiniteStateMachine> {
                let instance_name = self
                    .transitions()
                    .first()
                    .and_then(|transition| match &transition.data {
                        $variant { instance_name, .. } => Some(instance_name.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();
                native_fsm_to_ui::<$event>(self, self.transitions(), instance_name, epoch, clip)
            }
        }
    };
}

impl_to_ui_fsm!(PipelineTask, PipelineTaskEvent, PipelineTaskEvent::Created);
impl_to_ui_fsm!(
    ChunkTransfer,
    ChunkTransferEvent,
    ChunkTransferEvent::Produced
);
impl_to_ui_fsm!(
    OperatorInvocation,
    OperatorInvocationEvent,
    OperatorInvocationEvent::InvocationCreated
);
impl_to_ui_fsm!(
    TemporaryBlockIo,
    TemporaryBlockIoEvent,
    TemporaryBlockIoEvent::IoRequested
);

impl ToUiFsm for MemoryAccount {
    fn to_ui_fsm(
        &self,
        epoch: TimeUnixNanoSec,
        clip: Option<SpanUnixNanoSec>,
    ) -> AnalyzerResult<FiniteStateMachine> {
        let transitions = self.transitions();
        let registered = transitions.first().ok_or_else(|| {
            AnalyzerError::IncompleteEntity(format!("memory account {} is empty", self.id()))
        })?;
        let exited = transitions.last().ok_or_else(|| {
            AnalyzerError::IncompleteEntity(format!("memory account {} is empty", self.id()))
        })?;
        let MemoryAccountEvent::AccountRegistered { instance_name, .. } = &registered.data else {
            return Err(AnalyzerError::Validation(format!(
                "memory account {} does not begin with registration",
                self.id()
            )));
        };
        if !matches!(exited.data, MemoryAccountEvent::Exit { .. }) {
            return Err(AnalyzerError::IncompleteEntity(format!(
                "memory account {} has no exit",
                self.id()
            )));
        }

        let mut updates = transitions
            .iter()
            .filter(|transition| matches!(transition.data, MemoryAccountEvent::Accounted { .. }));
        let accounted = updates.next().ok_or_else(|| {
            AnalyzerError::IncompleteEntity(format!(
                "memory account {} has no accounted state",
                self.id()
            ))
        })?;
        let total = 1 + updates.count();
        let mut accounted = transition_to_ui(
            accounted,
            epoch,
            clip,
            if total == 1 {
                UsageDetail::Exact
            } else {
                UsageDetail::ResourceOnly
            },
        )?;
        accounted.derived_attributes = vec![
            DynamicAttribute::u64(MEMORY_UPDATES_TOTAL_ATTR, total as u64),
            DynamicAttribute::u64(MEMORY_UPDATES_OMITTED_ATTR, (total - 1) as u64),
        ];

        Ok(FiniteStateMachine {
            id: self.id(),
            type_name: self.type_name().to_owned(),
            instance_name: instance_name.clone(),
            transitions: vec![
                transition_to_ui(registered, epoch, clip, UsageDetail::Exact)?,
                accounted,
                transition_to_ui(exited, epoch, clip, UsageDetail::Exact)?,
            ],
        })
    }
}

/// List the FSMs matching `keep`, scope, window, and filters, ranked by longest
/// in-window usage and paged. Mirrors Quent's generic `list_entities` but builds
/// UI FSMs via [`native_fsm_to_ui`] so per-transition usages survive (and optionally
/// clips engine-lived FSMs into `clip`).
fn list_fsms<'a, T, P>(
    fsms: impl Iterator<Item = &'a T>,
    keep: P,
    query: &entities::ListQuery<'_>,
    clip: Option<SpanUnixNanoSec>,
) -> AnalyzerResult<EntityListResponse>
where
    T: ToUiFsm + Entity + 'a,
    P: Fn(&T) -> bool,
{
    let min_usage = query.filter.min_usage_s.map(to_nanosecs);
    let mut ranked: Vec<(&'a T, TimeNanoSec)> = fsms
        .filter(|fsm| keep(fsm))
        .filter_map(|fsm| {
            let longest = fsm
                .usages_with_state_names()
                .filter(|(_, usage)| {
                    query
                        .scope
                        .is_none_or(|scope| scope.contains(&usage.resource_id()))
                })
                .filter_map(|(_, usage)| usage.span().intersection(&query.window))
                .map(|span| span.duration())
                .max();
            // With a scope an FSM must use a scoped resource; without one, keep
            // FSMs whose lifecycle overlaps the window.
            let metric = match query.scope {
                Some(_) => longest?,
                None => {
                    if !fsm.span().is_ok_and(|span| span.intersects(&query.window)) {
                        return None;
                    }
                    longest.unwrap_or(0)
                }
            };
            min_usage
                .is_none_or(|min| metric >= min)
                .then_some((fsm, metric))
        })
        .collect();

    ranked.sort_by(|(fsm_a, metric_a), (fsm_b, metric_b)| {
        let ordering = match query.sort.key {
            EntitySortKey::UsageDuration => metric_a.cmp(metric_b),
        };
        let ordering = match query.sort.dir {
            SortDir::Asc => ordering,
            SortDir::Desc => ordering.reverse(),
        };
        ordering.then_with(|| fsm_a.id().cmp(&fsm_b.id()))
    });

    let total = ranked.len() as u32;
    let page: Box<dyn Iterator<Item = (&T, TimeNanoSec)>> = match query.page {
        Some(page) => Box::new(
            ranked
                .into_iter()
                .skip(page.page.saturating_mul(page.max) as usize)
                .take(page.max as usize),
        ),
        None => Box::new(ranked.into_iter()),
    };

    let items = page
        .map(|(fsm, metric)| {
            fsm.to_ui_fsm(query.epoch, clip)
                .map(|entity| EntityListItem {
                    usage_duration_s: to_secs(metric),
                    entity,
                })
        })
        .collect::<AnalyzerResult<Vec<_>>>()?;

    Ok(EntityListResponse { items, total })
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
    plan_ids: FxHashSet<Uuid>,
    plan_workers: FxHashMap<Uuid, Uuid>,
    operator_plans: FxHashMap<Uuid, Uuid>,
    port_operators: FxHashMap<Uuid, Uuid>,
    plan_edges: FxHashSet<(Uuid, Uuid)>,
}

struct QueryTasks {
    operators: FxHashMap<Uuid, FxHashSet<Uuid>>,
    plans: FxHashMap<Uuid, Uuid>,
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
    type Event = DuckDbEvent;

    fn try_new(
        engine_id: Uuid,
        events: impl Iterator<Item = Event<DuckDbEvent>>,
    ) -> AnalyzerResult<Self> {
        let mut builder = DuckDbModelBuilder::try_new(engine_id)?;
        for event in events {
            builder.try_push(event)?;
        }
        let model = builder.try_build()?;
        tracing::info!(
            workers = model.workers.len(),
            query_groups = model.query_groups.len(),
            queries = model.queries.len(),
            plans = model.plans.len(),
            operators = model.operators.len(),
            ports = model.ports.len(),
            pipeline_tasks = model.pipeline_tasks.len(),
            chunk_transfers = model.chunk_transfers.len(),
            operator_invocations = model.operator_invocations.len(),
            temporary_block_ios = model.temporary_block_ios.len(),
            memory_accounts = model.memory_accounts.len(),
            resources = model.runtime_resources.len(),
            "built DuckDB query-engine model"
        );
        let mut analyzer = Self {
            model,
            query_topologies: FxHashMap::default(),
            query_tasks: FxHashMap::default(),
        };
        let query_ids = analyzer
            .model
            .queries()
            .map(Entity::id)
            .collect::<SmallVec<[Uuid; 4]>>();
        for query_id in query_ids {
            let topology = analyzer.build_query_topology(query_id)?;
            let tasks = analyzer.build_query_tasks(query_id, &topology);
            analyzer.query_topologies.insert(query_id, topology);
            analyzer.query_tasks.insert(query_id, tasks);
        }
        Ok(analyzer)
    }

    fn extract_engine(
        engine_id: Uuid,
        events: impl Iterator<Item = Event<DuckDbEvent>>,
    ) -> AnalyzerResult<quent_query_engine_ui::Engine> {
        for event in events {
            if let DuckDbEvent::Engine(duckdb_telemetry_store::EngineEvent::Init {
                implementation,
                instance_name,
            }) = event.data
            {
                return Ok(quent_query_engine_ui::Engine {
                    id: engine_id,
                    start_time_unix_ns: Some(event.timestamp),
                    duration_s: None,
                    instance_name,
                    implementation: Some(quent_query_engine_ui::EngineImplementationAttributes {
                        name: implementation.name,
                        version: implementation.version,
                        custom_attributes: implementation.custom_attributes.0,
                    }),
                });
            }
        }
        Ok(quent_query_engine_ui::Engine::new(engine_id))
    }

    fn query_bundle(&self, query_id: Uuid) -> AnalyzerResult<QueryBundle> {
        let view = self.model.query_view(query_id)?;
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
                .resource_types
                .iter()
                .map(|(name, resource_type)| (name.clone(), resource_type.into()))
                .collect(),
            resource_group_types: self
                .model
                .resource_group_types
                .iter()
                .map(|(name, group_type)| (name.clone(), group_type.clone()))
                .collect(),
            resources: self
                .model
                .query_view(query_id)?
                .resources
                .values()
                .map(|resource| {
                    let parent_id = resource.parent_id().ok_or_else(|| {
                        AnalyzerError::Validation(format!(
                            "resource {} has no parent",
                            resource.id()
                        ))
                    })?;
                    Ok((
                        resource.id(),
                        Resource::from_analyzed(*resource, resource.instance_name(), parent_id),
                    ))
                })
                .collect::<AnalyzerResult<_>>()?,
            resource_groups: HashMap::new(),
            fsm_types,
        };
        let unique_operator_names = view
            .operators()
            .filter_map(|operator| operator.operator_type_name().map(str::to_owned))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let resource_tree = convert_resource_tree(ResourceTreeNode::try_new(&view)?, &view)?
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
        let tasks = self.query_tasks(query_id)?;
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

        // The Quent long-entities row omits the entity type; default it from
        // the scoped resource so each resource lists its own entities (e.g.
        // memory resources list memory accounts).
        let entity_type = match entry.filter.entity_type_name.as_deref() {
            Some(name) => name.to_owned(),
            None => self.default_scope_entity_type(scope.as_ref()),
        };

        match entity_type.as_str() {
            PIPELINE_TASK_TYPE_NAME => list_fsms(
                self.model.pipeline_tasks_for(query_id),
                |task| {
                    task.query_id() == Some(query_id)
                        && task.belongs_to_query(
                            &topology.operator_plans,
                            &topology.plan_workers,
                            &topology.plan_ids,
                        )
                        && task.resources_are_valid(&self.model)
                        && task.matches_operator(&filter)
                        && task.span().is_ok_and(|span| span.intersects(&window))
                },
                &query,
                None,
            ),
            CHUNK_TRANSFER_TYPE_NAME => list_fsms(
                self.model.chunk_transfers_for(query_id),
                |transfer| {
                    transfer.query_id() == Some(query_id)
                        && transfer.belongs_to_query(
                            &topology.port_operators,
                            &topology.plan_edges,
                            &task_ids,
                        )
                        && transfer.matches_operator(&filter)
                        && transfer.span().is_ok_and(|span| span.intersects(&window))
                },
                &query,
                None,
            ),
            OPERATOR_INVOCATION_TYPE_NAME => list_fsms(
                self.model.operator_invocations_for(query_id),
                |invocation| {
                    invocation.query_id() == Some(query_id)
                        && invocation.belongs_to_query(
                            &topology.operator_plans,
                            &topology.plan_workers,
                            &topology.plan_ids,
                            &tasks.operators,
                        )
                        && invocation.resources_are_valid(&topology.plan_workers, &self.model)
                        && invocation.matches_operator(&filter)
                        && invocation.span().is_ok_and(|span| span.intersects(&window))
                },
                &query,
                None,
            ),
            TEMPORARY_BLOCK_IO_TYPE_NAME => list_fsms(
                self.model.temporary_block_ios_for(query_id),
                |io| {
                    self.temporary_io_matches(io, query_id, topology, tasks, &filter)
                        && io.span().is_ok_and(|span| span.intersects(&window))
                },
                &query,
                None,
            ),
            // Accounts are engine-lived and begin before the query epoch, so
            // their transitions are clipped into the query span to list them.
            MEMORY_ACCOUNT_TYPE_NAME => {
                let query_span = self.model.query(query_id)?.span()?;
                list_fsms(
                    self.model.memory_accounts.values(),
                    |account| self.memory_account_matches(account, &filter),
                    &query,
                    Some(query_span),
                )
            }
            other => Err(AnalyzerError::InvalidArgument(format!(
                "unknown DuckDB entity type {other:?}"
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
            .query_tasks(query_id)?
            .operators
            .keys()
            .copied()
            .collect::<FxHashSet<_>>();
        let operator_names = self
            .model
            .query_view(query_id)?
            .operators()
            .map(|operator| (operator.id(), operator.instance_name().to_owned()))
            .collect::<FxHashMap<_, _>>();

        let wants = |name: &str| {
            request.measures.is_empty() || request.measures.iter().any(|value| value == name)
        };
        let num_bins = config.num_bins().get() as usize;
        let bin_duration_s = to_secs(config.bin_duration().get());
        let mut dimensions = BTreeMap::<String, String>::new();
        let mut operators = HashMap::<Uuid, CategoricalSeries>::new();

        for transfer in self.model.chunk_transfers_for(query_id) {
            if transfer.query_id() != Some(query_id)
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
        let tasks = self.query_tasks(query_id)?;
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
                    let tree = ResourceTreeNode::try_new(&self.model)?;
                    let tree = tree
                        .find(group.resource_group_id)
                        .ok_or(AnalyzerError::InvalidId(group.resource_group_id))?;
                    let mut available_types = tree
                        .iter_resource_ids()
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
                        .iter_resource_ids()
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
                    for task in self.model.pipeline_tasks_for(query_id).filter(|task| {
                        self.task_matches(task, query_id, topology, &operator_filter)
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
                            .operator_invocations_for(query_id)
                            .filter(|invocation| {
                                self.invocation_matches(
                                    invocation,
                                    query_id,
                                    topology,
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
                    for io in self.model.temporary_block_ios_for(query_id).filter(|io| {
                        self.temporary_io_matches(io, query_id, topology, tasks, &operator_filter)
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
                .pipeline_tasks_for(query_id)
                .filter(|task| self.task_matches(task, query_id, topology, &operator_filter))
            {
                for usage in task.usages() {
                    if resource_ids.contains(&usage.resource_id()) {
                        builder.try_push(&usage)?;
                    }
                }
            }
            for io in self.model.temporary_block_ios_for(query_id).filter(|io| {
                self.temporary_io_matches(io, query_id, topology, tasks, &operator_filter)
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

    fn build_query_tasks(&self, query_id: Uuid, topology: &QueryTopology) -> QueryTasks {
        let operators = self
            .model
            .pipeline_tasks_for(query_id)
            .filter(|task| {
                self.task_matches(
                    task,
                    query_id,
                    topology,
                    &OperatorFilter {
                        operator_ids: vec![],
                    },
                )
            })
            .filter_map(|task| {
                task.operator_ids()
                    .map(|operator_ids| (task.id(), operator_ids.iter().copied().collect()))
            })
            .collect::<FxHashMap<_, _>>();
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

    fn query_tasks(&self, query_id: Uuid) -> AnalyzerResult<&QueryTasks> {
        self.query_tasks
            .get(&query_id)
            .ok_or(AnalyzerError::InvalidId(query_id))
    }

    fn task_matches(
        &self,
        task: &PipelineTask,
        query_id: Uuid,
        topology: &QueryTopology,
        filter: &OperatorFilter,
    ) -> bool {
        task.query_id() == Some(query_id)
            && task.belongs_to_query(
                &topology.operator_plans,
                &topology.plan_workers,
                &topology.plan_ids,
            )
            && task.resources_are_valid(&self.model)
            && task.matches_operator(filter)
    }

    fn invocation_matches(
        &self,
        invocation: &OperatorInvocation,
        query_id: Uuid,
        topology: &QueryTopology,
        task_operators: &FxHashMap<Uuid, FxHashSet<Uuid>>,
        filter: &OperatorFilter,
    ) -> bool {
        invocation.query_id() == Some(query_id)
            && invocation.belongs_to_query(
                &topology.operator_plans,
                &topology.plan_workers,
                &topology.plan_ids,
                task_operators,
            )
            && invocation.resources_are_valid(&topology.plan_workers, &self.model)
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
            && io.belongs_to_query(
                &topology.operator_plans,
                &topology.plan_workers,
                &topology.plan_ids,
                &tasks.plans,
                &tasks.operators,
            )
            && io.resources_are_valid(&topology.plan_workers, &self.model)
            && io.matches_operator(filter)
    }

    fn memory_account_matches(&self, account: &MemoryAccount, filter: &OperatorFilter) -> bool {
        account.is_valid(self.model.engine.id(), &self.model) && account.matches_operator(filter)
    }

    /// The default entity type to list for a resource scope: a resource type's
    /// sole `used_by` entity type, matching the Quent UI's per-resource default.
    /// Falls back to pipeline tasks for unscoped or multi-entity resources.
    fn default_scope_entity_type(&self, scope: Option<&HashSet<Uuid>>) -> String {
        let Some(resource_ids) = scope else {
            return PIPELINE_TASK_TYPE_NAME.to_owned();
        };
        let mut chosen: Option<String> = None;
        for resource_id in resource_ids {
            let Ok(resource_type) = self.model.resource_type_of(*resource_id) else {
                return PIPELINE_TASK_TYPE_NAME.to_owned();
            };
            if resource_type.used_by.len() != 1 {
                return PIPELINE_TASK_TYPE_NAME.to_owned();
            }
            let used_by = resource_type
                .used_by
                .iter()
                .next()
                .expect("len == 1")
                .clone();
            match &chosen {
                Some(existing) if *existing != used_by => {
                    return PIPELINE_TASK_TYPE_NAME.to_owned();
                }
                _ => chosen = Some(used_by),
            }
        }
        chosen.unwrap_or_else(|| PIPELINE_TASK_TYPE_NAME.to_owned())
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
                    return Some(task.to_ui_fsm(epoch, None));
                }
                if let Some(io) = self.model.temporary_block_ios.get(id) {
                    return Some(io.to_ui_fsm(epoch, None));
                }
                self.model
                    .operator_invocations
                    .get(id)
                    .map(|invocation| invocation.to_ui_fsm(epoch, None))
            })
            .collect::<AnalyzerResult<Vec<_>>>()
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

    fn build_query_topology(&self, query_id: Uuid) -> AnalyzerResult<QueryTopology> {
        let view = self.model.query_view(query_id)?;
        let worker_ids: FxHashSet<Uuid> = view.workers().map(|worker| worker.id()).collect();
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
            plan_edges: view.plans().flat_map(|plan| plan.edges()).collect(),
        })
    }

    fn query_topology(&self, query_id: Uuid) -> AnalyzerResult<&QueryTopology> {
        self.query_topologies
            .get(&query_id)
            .ok_or(AnalyzerError::InvalidId(query_id))
    }

    /// Aggregate completed chunk-publication events by physical plan edge.
    pub fn chunk_summary(&self, query_id: Uuid) -> Vec<ChunkEdgeSummary> {
        type Edge = (Uuid, Uuid, Uuid, Uuid);
        type Totals = (u64, u64, u64);

        let Ok(topology) = self.query_topology(query_id) else {
            return vec![];
        };
        let task_ids: FxHashSet<Uuid> = self
            .query_tasks(query_id)
            .into_iter()
            .flat_map(|tasks| tasks.operators.keys().copied())
            .collect();
        let mut grouped = BTreeMap::<Edge, Totals>::new();
        for transfer in self.model.chunk_transfers_for(query_id) {
            if transfer.query_id() != Some(query_id)
                || !transfer.belongs_to_query(
                    &topology.port_operators,
                    &topology.plan_edges,
                    &task_ids,
                )
            {
                continue;
            }
            let Some(produced) = transfer.publication() else {
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
mod schema_tests {
    use crate::model::EXECUTION_THREAD_TYPE_NAME;
    use duckdb_telemetry_store as schema;
    use quent_events::{DynamicAttributes, EntityRef, Event};
    use quent_ui::entities::request::{
        EntityListEntry, EntityListFilter, EntityScope, EntitySortKey, Sort, SortDir, TimeWindow,
    };
    use quent_ui::timeline::request::{
        EntityFilter, ResourceGroupTimelineRequest, ResourceTimelineRequest, TimelineConfig,
    };

    use super::*;

    const ENGINE_ID: Uuid = Uuid::from_u128(1);
    const OTHER_ENGINE_ID: Uuid = Uuid::from_u128(2);
    const RESOURCE_ID: Uuid = Uuid::from_u128(3);
    const ACCOUNT_ID: Uuid = Uuid::from_u128(4);
    const QUERY_GROUP_ID: Uuid = Uuid::from_u128(5);
    const QUERY_ID: Uuid = Uuid::from_u128(6);
    const PLAN_ID: Uuid = Uuid::from_u128(7);
    const WORKER_ID: Uuid = Uuid::from_u128(8);
    const QUEUE_ID: Uuid = Uuid::from_u128(9);
    const THREAD_ID: Uuid = Uuid::from_u128(10);
    const IO_CHANNEL_ID: Uuid = Uuid::from_u128(11);
    const SOURCE_ID: Uuid = Uuid::from_u128(12);
    const TARGET_ID: Uuid = Uuid::from_u128(13);
    const SOURCE_PORT_ID: Uuid = Uuid::from_u128(14);
    const TARGET_PORT_ID: Uuid = Uuid::from_u128(15);
    const TASK_ID: Uuid = Uuid::from_u128(16);
    const INVOCATION_ID: Uuid = Uuid::from_u128(17);
    const IO_ID: Uuid = Uuid::from_u128(18);
    const TRANSFER_ID: Uuid = Uuid::from_u128(19);
    const ONE_SECOND_NS: u64 = 1_000_000_000;

    fn engine_init() -> Event<schema::DuckDbEvent> {
        Event::new(
            ENGINE_ID,
            1,
            schema::EngineEvent::Init {
                implementation: schema::EngineImplementation {
                    name: Some("DuckDB".to_owned()),
                    version: None,
                    custom_attributes: DynamicAttributes::new(),
                },
                instance_name: None,
            }
            .into(),
        )
    }

    fn event(
        id: Uuid,
        timestamp: u64,
        data: impl Into<schema::DuckDbEvent>,
    ) -> Event<schema::DuckDbEvent> {
        Event::new(id, timestamp, data.into())
    }

    fn fixture() -> DuckDbUiAnalyzer {
        let unit = schema::ExecutionThreadUsage;
        let queue_usage = schema::TaskQueueUsage { entries: 1 };
        let mut events = vec![
            engine_init(),
            event(
                QUERY_GROUP_ID,
                2,
                schema::QueryGroupEvent::Declaration {
                    instance_name: "connection".to_owned(),
                    engine_id: EntityRef::new(ENGINE_ID, ()),
                },
            ),
            event(
                WORKER_ID,
                3,
                schema::WorkerEvent::Init {
                    parent_engine_id: EntityRef::new(ENGINE_ID, ()),
                    instance_name: "worker".to_owned(),
                },
            ),
            event(
                QUERY_ID,
                ONE_SECOND_NS,
                schema::QueryEvent::Init {
                    seq: 0,
                    instance_name: "select sum(i)".to_owned(),
                    query_group_id: EntityRef::new(QUERY_GROUP_ID, ()),
                },
            ),
            event(
                QUERY_ID,
                ONE_SECOND_NS + 1,
                schema::QueryEvent::Planning { seq: 1 },
            ),
            event(
                QUERY_ID,
                2 * ONE_SECOND_NS,
                schema::QueryEvent::Executing { seq: 2 },
            ),
            event(
                QUERY_ID,
                11 * ONE_SECOND_NS,
                schema::QueryEvent::Exit { seq: 3 },
            ),
            event(
                PLAN_ID,
                ONE_SECOND_NS,
                schema::PlanEvent::Declaration {
                    parent: schema::PlanParent {
                        query_id: EntityRef::new(QUERY_ID, ()),
                        plan_id: None,
                    },
                    instance_name: "root".to_owned(),
                    edges: vec![schema::Edge {
                        source: EntityRef::new(SOURCE_PORT_ID, ()),
                        target: EntityRef::new(TARGET_PORT_ID, ()),
                    }],
                    worker_id: Some(EntityRef::new(WORKER_ID, ())),
                },
            ),
            event(
                SOURCE_ID,
                ONE_SECOND_NS,
                schema::OperatorEvent::Declaration {
                    plan_id: EntityRef::new(PLAN_ID, ()),
                    parent_operator_ids: vec![EntityRef::new(TARGET_ID, ())],
                    instance_name: "RANGE".to_owned(),
                    type_name: "RANGE".to_owned(),
                    custom_attributes: DynamicAttributes::new(),
                },
            ),
            event(
                TARGET_ID,
                ONE_SECOND_NS,
                schema::OperatorEvent::Declaration {
                    plan_id: EntityRef::new(PLAN_ID, ()),
                    parent_operator_ids: vec![],
                    instance_name: "AGGREGATE".to_owned(),
                    type_name: "AGGREGATE".to_owned(),
                    custom_attributes: DynamicAttributes::new(),
                },
            ),
            event(
                SOURCE_PORT_ID,
                ONE_SECOND_NS,
                schema::PortEvent::Declaration {
                    operator_id: EntityRef::new(SOURCE_ID, ()),
                    instance_name: "output".to_owned(),
                },
            ),
            event(
                TARGET_PORT_ID,
                ONE_SECOND_NS,
                schema::PortEvent::Declaration {
                    operator_id: EntityRef::new(TARGET_ID, ()),
                    instance_name: "input".to_owned(),
                },
            ),
            event(
                QUEUE_ID,
                4,
                schema::TaskQueueEvent::Initializing {
                    seq: 0,
                    instance_name: "queue".to_owned(),
                    worker_id: EntityRef::new(WORKER_ID, ()),
                },
            ),
            event(QUEUE_ID, 5, schema::TaskQueueEvent::Operating { seq: 1 }),
            event(
                THREAD_ID,
                4,
                schema::ExecutionThreadEvent::Initializing {
                    seq: 0,
                    instance_name: "thread".to_owned(),
                    worker_id: EntityRef::new(WORKER_ID, ()),
                },
            ),
            event(
                THREAD_ID,
                5,
                schema::ExecutionThreadEvent::Operating { seq: 1 },
            ),
            event(
                IO_CHANNEL_ID,
                4,
                schema::TemporaryIoChannelEvent::Initializing {
                    seq: 0,
                    instance_name: "spill".to_owned(),
                    worker_id: EntityRef::new(WORKER_ID, ()),
                },
            ),
            event(
                IO_CHANNEL_ID,
                5,
                schema::TemporaryIoChannelEvent::Operating { seq: 1 },
            ),
            event(
                RESOURCE_ID,
                4,
                schema::BufferPoolMemoryEvent::Initializing {
                    seq: 0,
                    instance_name: "buffer pool".to_owned(),
                    engine_id: EntityRef::new(ENGINE_ID, ()),
                },
            ),
            event(
                RESOURCE_ID,
                5,
                schema::BufferPoolMemoryEvent::Operating {
                    seq: 1,
                    limits: schema::BufferPoolMemoryBounds { bytes: 4096 },
                },
            ),
            event(
                TASK_ID,
                2 * ONE_SECOND_NS,
                schema::PipelineTaskEvent::Created {
                    seq: 0,
                    instance_name: "task".to_owned(),
                    query_id: EntityRef::new(QUERY_ID, ()),
                    plan_id: EntityRef::new(PLAN_ID, ()),
                    worker_id: EntityRef::new(WORKER_ID, ()),
                    operator_ids: vec![
                        EntityRef::new(SOURCE_ID, ()),
                        EntityRef::new(TARGET_ID, ()),
                    ],
                    task_index: 0,
                    queue: EntityRef::new(QUEUE_ID, queue_usage),
                },
            ),
            event(
                TASK_ID,
                3 * ONE_SECOND_NS,
                schema::PipelineTaskEvent::Running {
                    seq: 1,
                    mode: "partial".to_owned(),
                    cpu_id: 1,
                    execution_thread: EntityRef::new(THREAD_ID, unit),
                },
            ),
            event(
                TASK_ID,
                4 * ONE_SECOND_NS,
                schema::PipelineTaskEvent::Ready {
                    seq: 2,
                    queue: EntityRef::new(QUEUE_ID, schema::TaskQueueUsage { entries: 1 }),
                },
            ),
            event(
                TASK_ID,
                5 * ONE_SECOND_NS,
                schema::PipelineTaskEvent::Running {
                    seq: 3,
                    mode: "partial".to_owned(),
                    cpu_id: 2,
                    execution_thread: EntityRef::new(THREAD_ID, schema::ExecutionThreadUsage),
                },
            ),
            event(
                TASK_ID,
                6 * ONE_SECOND_NS,
                schema::PipelineTaskEvent::Finalizing {
                    seq: 4,
                    success: true,
                },
            ),
            event(
                TASK_ID,
                7 * ONE_SECOND_NS,
                schema::PipelineTaskEvent::Exit { seq: 5 },
            ),
            event(
                INVOCATION_ID,
                3 * ONE_SECOND_NS,
                schema::OperatorInvocationEvent::InvocationCreated {
                    seq: 0,
                    instance_name: "source".to_owned(),
                    query_id: EntityRef::new(QUERY_ID, ()),
                    plan_id: EntityRef::new(PLAN_ID, ()),
                    task_id: Some(EntityRef::new(TASK_ID, ())),
                    operator_id: EntityRef::new(SOURCE_ID, ()),
                    phase: "source".to_owned(),
                },
            ),
            event(
                INVOCATION_ID,
                3 * ONE_SECOND_NS + 200_000_000,
                schema::OperatorInvocationEvent::InvocationRunning {
                    seq: 1,
                    input_rows: 0,
                    input_logical_bytes: 0,
                    execution_thread: EntityRef::new(THREAD_ID, schema::ExecutionThreadUsage),
                },
            ),
            event(
                INVOCATION_ID,
                3 * ONE_SECOND_NS + 800_000_000,
                schema::OperatorInvocationEvent::InvocationCompleted {
                    seq: 2,
                    success: true,
                    output_rows: 10,
                    output_logical_bytes: 80,
                },
            ),
            event(
                INVOCATION_ID,
                3 * ONE_SECOND_NS + 900_000_000,
                schema::OperatorInvocationEvent::Exit { seq: 3 },
            ),
            event(
                IO_ID,
                5 * ONE_SECOND_NS + 800_000_000,
                schema::TemporaryBlockIoEvent::IoRequested {
                    seq: 0,
                    instance_name: "spill block".to_owned(),
                    query_id: EntityRef::new(QUERY_ID, ()),
                    plan_id: EntityRef::new(PLAN_ID, ()),
                    task_id: Some(EntityRef::new(TASK_ID, ())),
                    trigger_operator_id: Some(EntityRef::new(SOURCE_ID, ())),
                    block_id: 42,
                    memory_tag: "HASH_TABLE".to_owned(),
                    direction: "write".to_owned(),
                },
            ),
            event(
                IO_ID,
                6 * ONE_SECOND_NS,
                schema::TemporaryBlockIoEvent::IoActive {
                    seq: 1,
                    channel: EntityRef::new(
                        IO_CHANNEL_ID,
                        schema::TemporaryIoChannelUsage {
                            operations: 1,
                            buffer_bytes: 100,
                        },
                    ),
                },
            ),
            event(
                IO_ID,
                7 * ONE_SECOND_NS,
                schema::TemporaryBlockIoEvent::IoCompleted {
                    seq: 2,
                    success: true,
                    storage_bytes: 80,
                },
            ),
            event(
                IO_ID,
                7 * ONE_SECOND_NS + 1,
                schema::TemporaryBlockIoEvent::Exit { seq: 3 },
            ),
            event(
                TRANSFER_ID,
                8 * ONE_SECOND_NS,
                schema::ChunkTransferEvent::Produced {
                    seq: 0,
                    instance_name: "range -> aggregate".to_owned(),
                    query_id: EntityRef::new(QUERY_ID, ()),
                    task_id: Some(EntityRef::new(TASK_ID, ())),
                    source_operator_id: EntityRef::new(SOURCE_ID, ()),
                    source_port_id: EntityRef::new(SOURCE_PORT_ID, ()),
                    target_operator_id: EntityRef::new(TARGET_ID, ()),
                    target_port_id: EntityRef::new(TARGET_PORT_ID, ()),
                    rows: 10,
                    logical_bytes: 80,
                },
            ),
            event(
                TRANSFER_ID,
                8 * ONE_SECOND_NS + 1,
                schema::ChunkTransferEvent::Published { seq: 1 },
            ),
            event(
                TRANSFER_ID,
                8 * ONE_SECOND_NS + 2,
                schema::ChunkTransferEvent::Exit { seq: 2 },
            ),
            event(
                ACCOUNT_ID,
                100,
                schema::MemoryAccountEvent::AccountRegistered {
                    seq: 0,
                    instance_name: "hash table".to_owned(),
                    engine_id: EntityRef::new(ENGINE_ID, ()),
                    memory_tag: "HASH_TABLE".to_owned(),
                },
            ),
            event(
                ACCOUNT_ID,
                500_000_000,
                schema::MemoryAccountEvent::Accounted {
                    seq: 1,
                    buffer_pool: Some(EntityRef::new(
                        RESOURCE_ID,
                        schema::BufferPoolMemoryUsage { bytes: 100 },
                    )),
                    temporary_storage: None,
                    temporary_directory: None,
                },
            ),
            event(
                ACCOUNT_ID,
                4 * ONE_SECOND_NS,
                schema::MemoryAccountEvent::Accounted {
                    seq: 2,
                    buffer_pool: Some(EntityRef::new(
                        RESOURCE_ID,
                        schema::BufferPoolMemoryUsage { bytes: 200 },
                    )),
                    temporary_storage: None,
                    temporary_directory: None,
                },
            ),
            event(
                ACCOUNT_ID,
                20 * ONE_SECOND_NS,
                schema::MemoryAccountEvent::Exit { seq: 9 },
            ),
        ];
        for (seq, second) in (3..9).zip(5..11) {
            events.push(event(
                ACCOUNT_ID,
                second * ONE_SECOND_NS,
                schema::MemoryAccountEvent::Accounted {
                    seq,
                    buffer_pool: Some(EntityRef::new(
                        RESOURCE_ID,
                        schema::BufferPoolMemoryUsage { bytes: 200 },
                    )),
                    temporary_storage: None,
                    temporary_directory: None,
                },
            ));
        }
        events.sort_by_key(|event| event.timestamp);

        DuckDbUiAnalyzer::try_new(ENGINE_ID, events.into_iter()).unwrap()
    }

    fn list_request(
        entity_type_name: Option<&str>,
        scope: Option<EntityScope>,
        operator_ids: Vec<Uuid>,
    ) -> EntityListRequest<QueryFilter, OperatorFilter> {
        EntityListRequest {
            entry: EntityListEntry {
                window: TimeWindow {
                    start: 0.0,
                    end: 10.0,
                },
                filter: EntityListFilter {
                    scope,
                    entity_type_name: entity_type_name.map(str::to_owned),
                    min_usage_s: None,
                },
                sort: Sort {
                    key: EntitySortKey::UsageDuration,
                    dir: SortDir::Desc,
                },
                page: None,
                application: OperatorFilter { operator_ids },
            },
            app_params: QueryFilter { query_id: QUERY_ID },
        }
    }

    fn timeline_request(
        resource_id: Uuid,
        entity_type_name: Option<&str>,
        operator_ids: Vec<Uuid>,
        config: TimelineConfig,
    ) -> SingleTimelineRequest<QueryFilter, OperatorFilter> {
        SingleTimelineRequest {
            entry: TimelineRequest::Resource(ResourceTimelineRequest {
                resource_id,
                long_entities_threshold_s: None,
                entity_filter: EntityFilter {
                    entity_type_name: entity_type_name.map(str::to_owned),
                },
                application: OperatorFilter { operator_ids },
                config,
            }),
            app_params: QueryFilter { query_id: QUERY_ID },
        }
    }

    fn full_config() -> TimelineConfig {
        TimelineConfig {
            num_bins: 10,
            start: 0.0,
            end: 10.0,
        }
    }

    fn keyed_sum(timeline: &ResourceTimelineBinnedByState, capacity: &str) -> f64 {
        timeline
            .capacities_states_values
            .get(capacity)
            .into_iter()
            .flat_map(|states| states.values())
            .flatten()
            .sum()
    }

    #[test]
    fn rejects_invalid_resource_lifecycle() {
        let mut builder = DuckDbModelBuilder::try_new(ENGINE_ID).unwrap();
        builder.try_push(engine_init()).unwrap();
        builder
            .try_push(Event::new(
                RESOURCE_ID,
                2,
                schema::TaskQueueEvent::Initializing {
                    seq: 0,
                    instance_name: "queue".to_owned(),
                    worker_id: EntityRef::<schema::Worker>::new(Uuid::from_u128(5), ()),
                }
                .into(),
            ))
            .unwrap();

        builder
            .try_push(Event::new(
                RESOURCE_ID,
                3,
                schema::TaskQueueEvent::Finalizing { seq: 1 }.into(),
            ))
            .unwrap();
        let error = builder.try_build().err().unwrap();
        assert!(matches!(error, AnalyzerError::Validation(_)));
    }

    #[test]
    fn orders_resource_events_before_validation() {
        let mut builder = DuckDbModelBuilder::try_new(ENGINE_ID).unwrap();
        builder.try_push(engine_init()).unwrap();
        builder
            .try_push(Event::new(
                RESOURCE_ID,
                3,
                schema::BufferPoolMemoryEvent::Operating {
                    seq: 1,
                    limits: schema::BufferPoolMemoryBounds { bytes: 1024 },
                }
                .into(),
            ))
            .unwrap();
        builder
            .try_push(Event::new(
                RESOURCE_ID,
                2,
                schema::BufferPoolMemoryEvent::Initializing {
                    seq: 0,
                    instance_name: "buffer pool".to_owned(),
                    engine_id: EntityRef::<schema::Engine>::new(ENGINE_ID, ()),
                }
                .into(),
            ))
            .unwrap();

        let model = builder.try_build().unwrap();
        let resource = model.runtime_resources.get(&RESOURCE_ID).unwrap();
        assert_eq!(resource.bounds[0].value, Some(1024));
    }

    #[test]
    fn rejects_cross_type_resource_transition() {
        let mut builder = DuckDbModelBuilder::try_new(ENGINE_ID).unwrap();
        builder.try_push(engine_init()).unwrap();
        builder
            .try_push(Event::new(
                RESOURCE_ID,
                2,
                schema::TaskQueueEvent::Initializing {
                    seq: 0,
                    instance_name: "queue".to_owned(),
                    worker_id: EntityRef::<schema::Worker>::new(Uuid::from_u128(5), ()),
                }
                .into(),
            ))
            .unwrap();

        builder
            .try_push(Event::new(
                RESOURCE_ID,
                3,
                schema::BufferPoolMemoryEvent::Operating {
                    seq: 1,
                    limits: schema::BufferPoolMemoryBounds { bytes: 1024 },
                }
                .into(),
            ))
            .unwrap();
        let error = builder.try_build().err().unwrap();
        assert!(matches!(error, AnalyzerError::Validation(_)));
    }

    #[test]
    fn rejects_cross_engine_memory_account() {
        let mut builder = DuckDbModelBuilder::try_new(ENGINE_ID).unwrap();
        builder.try_push(engine_init()).unwrap();
        builder
            .try_push(Event::new(
                RESOURCE_ID,
                2,
                schema::BufferPoolMemoryEvent::Initializing {
                    seq: 0,
                    instance_name: "buffer pool".to_owned(),
                    engine_id: EntityRef::<schema::Engine>::new(ENGINE_ID, ()),
                }
                .into(),
            ))
            .unwrap();
        builder
            .try_push(Event::new(
                RESOURCE_ID,
                3,
                schema::BufferPoolMemoryEvent::Operating {
                    seq: 1,
                    limits: schema::BufferPoolMemoryBounds { bytes: 1024 },
                }
                .into(),
            ))
            .unwrap();
        builder
            .try_push(Event::new(
                ACCOUNT_ID,
                4,
                schema::MemoryAccountEvent::AccountRegistered {
                    seq: 0,
                    instance_name: "hash table".to_owned(),
                    engine_id: EntityRef::<schema::Engine>::new(OTHER_ENGINE_ID, ()),
                    memory_tag: "HASH_TABLE".to_owned(),
                }
                .into(),
            ))
            .unwrap();
        builder
            .try_push(Event::new(
                ACCOUNT_ID,
                5,
                schema::MemoryAccountEvent::Accounted {
                    seq: 1,
                    buffer_pool: Some(EntityRef::<schema::BufferPoolMemory, _>::new(
                        RESOURCE_ID,
                        schema::BufferPoolMemoryUsage { bytes: 32 },
                    )),
                    temporary_storage: None,
                    temporary_directory: None,
                }
                .into(),
            ))
            .unwrap();
        builder
            .try_push(Event::new(
                ACCOUNT_ID,
                6,
                schema::MemoryAccountEvent::Exit { seq: 2 }.into(),
            ))
            .unwrap();

        let model = builder.try_build().unwrap();
        let account = model.memory_accounts.get(&ACCOUNT_ID).unwrap();
        assert!(!account.is_valid(ENGINE_ID, &model));
    }

    #[test]
    fn omits_incomplete_fsms() {
        let mut builder = DuckDbModelBuilder::try_new(ENGINE_ID).unwrap();
        builder.try_push(engine_init()).unwrap();
        builder
            .try_push(Event::new(
                ACCOUNT_ID,
                2,
                schema::MemoryAccountEvent::AccountRegistered {
                    seq: 0,
                    instance_name: "hash table".to_owned(),
                    engine_id: EntityRef::<schema::Engine>::new(ENGINE_ID, ()),
                    memory_tag: "HASH_TABLE".to_owned(),
                }
                .into(),
            ))
            .unwrap();

        assert!(builder.try_build().unwrap().memory_accounts.is_empty());
    }

    #[test]
    fn query_view_resolves_resources() {
        use quent_analyzer::Model as _;

        let events = [
            engine_init(),
            Event::new(
                QUERY_GROUP_ID,
                2,
                schema::QueryGroupEvent::Declaration {
                    instance_name: "connection".to_owned(),
                    engine_id: EntityRef::<schema::Engine>::new(ENGINE_ID, ()),
                }
                .into(),
            ),
            Event::new(
                QUERY_ID,
                3,
                schema::QueryEvent::Init {
                    seq: 0,
                    instance_name: "select 1".to_owned(),
                    query_group_id: EntityRef::<schema::QueryGroup>::new(QUERY_GROUP_ID, ()),
                }
                .into(),
            ),
            Event::new(QUERY_ID, 4, schema::QueryEvent::Planning { seq: 1 }.into()),
            Event::new(QUERY_ID, 5, schema::QueryEvent::Executing { seq: 2 }.into()),
            Event::new(QUERY_ID, 6, schema::QueryEvent::Exit { seq: 3 }.into()),
            Event::new(
                WORKER_ID,
                6,
                schema::WorkerEvent::Init {
                    parent_engine_id: EntityRef::<schema::Engine>::new(ENGINE_ID, ()),
                    instance_name: "worker".to_owned(),
                }
                .into(),
            ),
            Event::new(
                PLAN_ID,
                6,
                schema::PlanEvent::Declaration {
                    parent: schema::PlanParent {
                        query_id: EntityRef::<schema::Query>::new(QUERY_ID, ()),
                        plan_id: None,
                    },
                    instance_name: "root".to_owned(),
                    edges: vec![],
                    worker_id: Some(EntityRef::<schema::Worker>::new(WORKER_ID, ())),
                }
                .into(),
            ),
            Event::new(
                QUEUE_ID,
                7,
                schema::TaskQueueEvent::Initializing {
                    seq: 0,
                    instance_name: "queue".to_owned(),
                    worker_id: EntityRef::<schema::Worker>::new(WORKER_ID, ()),
                }
                .into(),
            ),
            Event::new(
                QUEUE_ID,
                8,
                schema::TaskQueueEvent::Operating { seq: 1 }.into(),
            ),
            Event::new(
                RESOURCE_ID,
                9,
                schema::BufferPoolMemoryEvent::Initializing {
                    seq: 0,
                    instance_name: "buffer pool".to_owned(),
                    engine_id: EntityRef::<schema::Engine>::new(ENGINE_ID, ()),
                }
                .into(),
            ),
            Event::new(
                RESOURCE_ID,
                10,
                schema::BufferPoolMemoryEvent::Operating {
                    seq: 1,
                    limits: schema::BufferPoolMemoryBounds { bytes: 1024 },
                }
                .into(),
            ),
            Event::new(
                RESOURCE_ID,
                11,
                schema::BufferPoolMemoryEvent::Resizing { seq: 2 }.into(),
            ),
            Event::new(
                RESOURCE_ID,
                12,
                schema::BufferPoolMemoryEvent::Operating {
                    seq: 3,
                    limits: schema::BufferPoolMemoryBounds { bytes: 2048 },
                }
                .into(),
            ),
        ];
        let mut builder = DuckDbModelBuilder::try_new(ENGINE_ID).unwrap();
        for event in events {
            builder.try_push(event).unwrap();
        }
        let model = builder.try_build().unwrap();
        let view = model.query_view(QUERY_ID).unwrap();

        assert!(model.runtime_resources[&QUEUE_ID].bounds.is_empty());
        assert_eq!(
            model.runtime_resources[&RESOURCE_ID].bounds.as_slice(),
            &[CapacityValue::new(MEMORY_BYTES_CAPACITY_NAME, 2048)]
        );
        assert_eq!(
            view.try_entity_ref(RESOURCE_ID).unwrap(),
            quent_query_engine_ui::EntityRef::Resource(RESOURCE_ID)
        );
    }

    #[test]
    fn bundle_and_entity_filters_are_consistent() {
        let analyzer = fixture();
        let bundle = analyzer.query_bundle(QUERY_ID).unwrap();

        assert_eq!(bundle.entities.resources.len(), 4);
        for type_name in [
            PIPELINE_TASK_TYPE_NAME,
            CHUNK_TRANSFER_TYPE_NAME,
            OPERATOR_INVOCATION_TYPE_NAME,
            TEMPORARY_BLOCK_IO_TYPE_NAME,
            MEMORY_ACCOUNT_TYPE_NAME,
        ] {
            assert!(bundle.entities.fsm_types.contains_key(type_name));
        }

        let source_tasks = analyzer
            .list_entities(list_request(
                Some(PIPELINE_TASK_TYPE_NAME),
                None,
                vec![SOURCE_ID],
            ))
            .unwrap();
        let target_invocations = analyzer
            .list_entities(list_request(
                Some(OPERATOR_INVOCATION_TYPE_NAME),
                None,
                vec![TARGET_ID],
            ))
            .unwrap();
        let memory_accounts = analyzer
            .list_entities(list_request(
                None,
                Some(EntityScope::Resource {
                    resource_id: RESOURCE_ID,
                }),
                vec![],
            ))
            .unwrap();

        assert_eq!(source_tasks.total, 1);
        assert_eq!(target_invocations.total, 0);
        assert_eq!(memory_accounts.total, 1);
        assert_eq!(
            memory_accounts.items[0].entity.type_name,
            MEMORY_ACCOUNT_TYPE_NAME
        );
        assert_eq!(
            memory_accounts.items[0].entity.transitions[0].timestamp,
            0.0
        );
    }

    #[test]
    fn task_and_invocation_timelines_match_observed_spans() {
        let analyzer = fixture();
        let task_response = analyzer
            .single_resource_timeline(timeline_request(
                THREAD_ID,
                Some(PIPELINE_TASK_TYPE_NAME),
                vec![],
                full_config(),
            ))
            .unwrap();
        let UiResourceTimeline::BinnedByState(task_timeline) = task_response.data else {
            panic!("expected task states");
        };
        let invocation_response = analyzer
            .single_resource_timeline(timeline_request(
                THREAD_ID,
                Some(OPERATOR_INVOCATION_TYPE_NAME),
                vec![SOURCE_ID],
                full_config(),
            ))
            .unwrap();
        let UiResourceTimeline::BinnedByState(invocation_timeline) = invocation_response.data
        else {
            panic!("expected invocation states");
        };
        let filtered_response = analyzer
            .single_resource_timeline(timeline_request(
                THREAD_ID,
                Some(OPERATOR_INVOCATION_TYPE_NAME),
                vec![TARGET_ID],
                full_config(),
            ))
            .unwrap();
        let UiResourceTimeline::BinnedByState(filtered_timeline) = filtered_response.data else {
            panic!("expected invocation states");
        };

        assert!((keyed_sum(&task_timeline, "unit") - 2.0).abs() < 1e-9);
        assert!((keyed_sum(&invocation_timeline, "unit") - 0.6).abs() < 1e-9);
        assert_eq!(keyed_sum(&filtered_timeline, "unit"), 0.0);
    }

    #[test]
    fn temporary_io_reports_per_second_rates() {
        let response = fixture()
            .single_resource_timeline(timeline_request(
                IO_CHANNEL_ID,
                Some(TEMPORARY_BLOCK_IO_TYPE_NAME),
                vec![SOURCE_ID],
                TimelineConfig {
                    num_bins: 1,
                    start: 5.0,
                    end: 6.0,
                },
            ))
            .unwrap();
        let UiResourceTimeline::BinnedByState(timeline) = response.data else {
            panic!("expected I/O states");
        };

        assert_eq!(
            timeline.capacities_states_values[IO_OPERATIONS_CAPACITY_NAME]["io_active"],
            [1.0]
        );
        assert_eq!(
            timeline.capacities_states_values[IO_BUFFER_BYTES_CAPACITY_NAME]["io_active"],
            [100.0]
        );
    }

    #[test]
    fn memory_timeline_clips_and_filters() {
        let analyzer = fixture();
        let response = analyzer
            .single_resource_timeline(timeline_request(RESOURCE_ID, None, vec![], full_config()))
            .unwrap();
        let UiResourceTimeline::Binned(timeline) = response.data else {
            panic!("expected memory totals");
        };
        let tagged_response = analyzer
            .single_resource_timeline(timeline_request(
                RESOURCE_ID,
                Some(MEMORY_ACCOUNT_TYPE_NAME),
                vec![],
                full_config(),
            ))
            .unwrap();
        let UiResourceTimeline::BinnedByState(tagged) = tagged_response.data else {
            panic!("expected memory tags");
        };
        let filtered_response = analyzer
            .single_resource_timeline(timeline_request(
                RESOURCE_ID,
                None,
                vec![SOURCE_ID],
                full_config(),
            ))
            .unwrap();
        let UiResourceTimeline::Binned(filtered) = filtered_response.data else {
            panic!("expected memory totals");
        };

        assert_eq!(
            timeline.capacities_values[MEMORY_BYTES_CAPACITY_NAME],
            [
                100.0, 100.0, 100.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0
            ]
        );
        assert_eq!(
            tagged.capacities_states_values[MEMORY_BYTES_CAPACITY_NAME]["HASH_TABLE"],
            timeline.capacities_values[MEMORY_BYTES_CAPACITY_NAME]
        );
        assert!(
            !filtered
                .capacities_values
                .contains_key(MEMORY_BYTES_CAPACITY_NAME)
        );
    }

    #[test]
    fn memory_entity_rows_summarize_updates() {
        let accounts = fixture()
            .list_entities(list_request(Some(MEMORY_ACCOUNT_TYPE_NAME), None, vec![]))
            .unwrap();
        let transitions = &accounts.items[0].entity.transitions;

        assert_eq!(
            transitions
                .iter()
                .map(|transition| transition.name.as_str())
                .collect::<Vec<_>>(),
            ["account_registered", "accounted", "exit"]
        );
        assert_eq!(transitions[0].timestamp, 0.0);
        assert_eq!(transitions[1].timestamp, 0.0);
        assert_eq!(transitions[2].timestamp, 10.0);
        assert!(
            transitions[0]
                .attributes
                .contains(&DynamicAttribute::string("memory_tag", "HASH_TABLE"))
        );
        assert_eq!(transitions[1].usages.len(), 1);
        assert_eq!(transitions[1].usages[0].resource, RESOURCE_ID);
        assert!(transitions[1].usages[0].capacities.is_empty());
        assert_eq!(
            transitions[1].derived_attributes,
            [
                DynamicAttribute::u64(MEMORY_UPDATES_TOTAL_ATTR, 8),
                DynamicAttribute::u64(MEMORY_UPDATES_OMITTED_ATTR, 7),
            ]
        );
    }

    #[test]
    fn data_flow_uses_valid_plan_edges() {
        let analyzer = fixture();
        let request = CategoricalTimelineRequest {
            measures: vec![],
            config: full_config(),
            app_params: QueryFilter { query_id: QUERY_ID },
        };
        let response = analyzer.data_flow_timeline(request).unwrap();
        let dimension = SOURCE_ID.to_string();
        let values = &response.operators[&TARGET_ID].values;
        let sum = |measure: &str| {
            values[measure][DATA_FLOW_STATE][&dimension]
                .iter()
                .sum::<f64>()
        };

        assert_eq!(sum(MEASURE_CHUNKS), 1.0);
        assert_eq!(sum(MEASURE_ROWS), 10.0);
        assert_eq!(sum(MEASURE_LOGICAL_BYTES), 80.0);
        assert_eq!(analyzer.chunk_summary(QUERY_ID)[0].rows, 10);
        assert!(
            analyzer
                .data_flow_timeline(CategoricalTimelineRequest {
                    measures: vec!["physical_bytes".to_owned()],
                    config: full_config(),
                    app_params: QueryFilter { query_id: QUERY_ID },
                })
                .is_err()
        );
    }

    #[test]
    fn bulk_and_group_timelines_are_independent() {
        let analyzer = fixture();
        let thread = timeline_request(THREAD_ID, None, vec![], full_config()).entry;
        let group = TimelineRequest::ResourceGroup(ResourceGroupTimelineRequest {
            resource_group_id: WORKER_ID,
            resource_type_name: EXECUTION_THREAD_TYPE_NAME.to_owned(),
            long_entities_threshold_s: None,
            entity_filter: EntityFilter {
                entity_type_name: Some(OPERATOR_INVOCATION_TYPE_NAME.to_owned()),
            },
            app_params: OperatorFilter {
                operator_ids: vec![],
            },
            config: full_config(),
        });
        let response = analyzer
            .bulk_resource_timeline(BulkTimelineRequest {
                entries: [("thread".to_owned(), thread), ("group".to_owned(), group)]
                    .into_iter()
                    .collect(),
                app_params: QueryFilter { query_id: QUERY_ID },
            })
            .unwrap();

        assert!(matches!(
            response.entries["thread"],
            BulkTimelinesResponseEntry::Ok { .. }
        ));
        assert!(matches!(
            response.entries["group"],
            BulkTimelinesResponseEntry::Ok { .. }
        ));
    }
}
