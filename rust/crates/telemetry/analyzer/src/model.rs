use std::collections::{BTreeSet, hash_map::Entry};

use duckdb_telemetry_store as schema;
use quent_analyzer::{
    AnalyzerError, AnalyzerResult, Entity, Model, RefTreeEntity,
    fsm::native::{AnalyzedFsmBuilder, TransitionEvent},
    ref_tree::RefTreeCollection,
    resource::{
        CapacityDecl, CapacityValue, Resource, ResourceTypeDecl, Usage, Using,
        collection::ResourceCollection,
    },
};
use quent_events::Event;
use quent_query_engine_analyzer::{
    OperatorEntityMut, QueryEngineModel, QueryEngineModelMut, plan_tree::PlanTree,
};
use quent_query_engine_ui::EntityRef;
use quent_time::TimeUnixNanoSec;
use quent_ui::ResourceGroupTypeDecl;
use rustc_hash::FxHashMap as HashMap;
use smallvec::SmallVec;
use uuid::Uuid;

use crate::{
    analyzed_entities::{Engine, Operator, Plan, Port, Query, QueryBuilder, QueryGroup, Worker},
    chunk_transfer::{ChunkTransfer, ChunkTransferBuilder, ChunkTransferExt},
    memory_account::{MemoryAccount, MemoryAccountBuilder},
    operator_invocation::{OperatorInvocation, OperatorInvocationBuilder, OperatorInvocationExt},
    pipeline_task::{PipelineTask, PipelineTaskBuilder, PipelineTaskExt},
    temporary_block_io::{TemporaryBlockIo, TemporaryBlockIoBuilder, TemporaryBlockIoExt},
};

pub(crate) const EXECUTION_THREAD_TYPE_NAME: &str = "execution_thread";
pub(crate) const TASK_QUEUE_TYPE_NAME: &str = "task_queue";
pub(crate) const QUEUE_ENTRIES_CAPACITY_NAME: &str = "entries";
pub(crate) const TEMPORARY_IO_CHANNEL_TYPE_NAME: &str = "temporary_io_channel";
pub(crate) const IO_OPERATIONS_CAPACITY_NAME: &str = "operations";
pub(crate) const IO_BUFFER_BYTES_CAPACITY_NAME: &str = "buffer_bytes";
pub(crate) const MEMORY_BYTES_CAPACITY_NAME: &str = "bytes";
pub(crate) const BUFFER_POOL_MEMORY_TYPE_NAME: &str = "buffer_pool_memory";
pub(crate) const TEMPORARY_STORAGE_TYPE_NAME: &str = "temporary_storage";
pub(crate) const TEMPORARY_DIRECTORY_STORAGE_TYPE_NAME: &str = "temporary_directory_storage";

#[derive(Debug)]
pub(crate) struct RuntimeResource {
    id: Uuid,
    type_name: &'static str,
    instance_name: String,
    parent_id: Uuid,
    earliest_timestamp: TimeUnixNanoSec,
    latest_timestamp: TimeUnixNanoSec,
    pub(crate) bounds: SmallVec<[CapacityValue; 2]>,
}

impl RuntimeResource {
    pub(crate) fn instance_name(&self) -> &str {
        &self.instance_name
    }
}

impl Entity for RuntimeResource {
    fn id(&self) -> Uuid {
        self.id
    }
    fn type_name(&self) -> &str {
        self.type_name
    }
    fn earliest_timestamp(&self) -> TimeUnixNanoSec {
        self.earliest_timestamp
    }
    fn latest_timestamp(&self) -> TimeUnixNanoSec {
        self.latest_timestamp
    }
}

impl Resource for RuntimeResource {}

impl RefTreeEntity for RuntimeResource {
    fn parent_id(&self) -> Option<Uuid> {
        Some(self.parent_id)
    }
}

struct RuntimeResourceBuilder {
    transitions: Vec<RuntimeResourceTransition>,
}

struct RuntimeResourceTransition {
    timestamp: TimeUnixNanoSec,
    event: RuntimeResourceEvent,
}

enum RuntimeResourceEvent {
    ExecutionThread(schema::ExecutionThreadEvent),
    TaskQueue(schema::TaskQueueEvent),
    TemporaryIoChannel(schema::TemporaryIoChannelEvent),
    BufferPoolMemory(schema::BufferPoolMemoryEvent),
    TemporaryStorage(schema::TemporaryStorageEvent),
    TemporaryDirectoryStorage(schema::TemporaryDirectoryStorageEvent),
}

enum ResourceEffect {
    Initializing(ResourceInit),
    Operating(SmallVec<[CapacityValue; 2]>),
    Other,
}

struct ResourceInit {
    type_name: &'static str,
    instance_name: String,
    parent_id: Uuid,
}

impl RuntimeResourceEvent {
    fn sequence(&self) -> u16 {
        match self {
            Self::ExecutionThread(event) => event.sequence(),
            Self::TaskQueue(event) => event.sequence(),
            Self::TemporaryIoChannel(event) => event.sequence(),
            Self::BufferPoolMemory(event) => event.sequence(),
            Self::TemporaryStorage(event) => event.sequence(),
            Self::TemporaryDirectoryStorage(event) => event.sequence(),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::ExecutionThread(event) => event.name(),
            Self::TaskQueue(event) => event.name(),
            Self::TemporaryIoChannel(event) => event.name(),
            Self::BufferPoolMemory(event) => event.name(),
            Self::TemporaryStorage(event) => event.name(),
            Self::TemporaryDirectoryStorage(event) => event.name(),
        }
    }

    fn is_valid_next(&self, next: &Self) -> bool {
        match self {
            Self::ExecutionThread(current) => {
                let Self::ExecutionThread(next) = next else {
                    return false;
                };
                current.is_valid_next(next)
            }
            Self::TaskQueue(current) => {
                let Self::TaskQueue(next) = next else {
                    return false;
                };
                current.is_valid_next(next)
            }
            Self::TemporaryIoChannel(current) => {
                let Self::TemporaryIoChannel(next) = next else {
                    return false;
                };
                current.is_valid_next(next)
            }
            Self::BufferPoolMemory(current) => {
                let Self::BufferPoolMemory(next) = next else {
                    return false;
                };
                current.is_valid_next(next)
            }
            Self::TemporaryStorage(current) => {
                let Self::TemporaryStorage(next) = next else {
                    return false;
                };
                current.is_valid_next(next)
            }
            Self::TemporaryDirectoryStorage(current) => {
                let Self::TemporaryDirectoryStorage(next) = next else {
                    return false;
                };
                current.is_valid_next(next)
            }
        }
    }

    fn effect(&self) -> ResourceEffect {
        match self {
            Self::ExecutionThread(event) => match event {
                schema::ExecutionThreadEvent::Initializing {
                    instance_name,
                    worker_id,
                    ..
                } => ResourceEffect::Initializing(ResourceInit {
                    type_name: EXECUTION_THREAD_TYPE_NAME,
                    instance_name: instance_name.clone(),
                    parent_id: worker_id.target,
                }),
                schema::ExecutionThreadEvent::Operating { .. } => {
                    ResourceEffect::Operating(SmallVec::new())
                }
                schema::ExecutionThreadEvent::Finalizing { .. }
                | schema::ExecutionThreadEvent::Exit { .. } => ResourceEffect::Other,
            },
            Self::TaskQueue(event) => match event {
                schema::TaskQueueEvent::Initializing {
                    instance_name,
                    worker_id,
                    ..
                } => ResourceEffect::Initializing(ResourceInit {
                    type_name: TASK_QUEUE_TYPE_NAME,
                    instance_name: instance_name.clone(),
                    parent_id: worker_id.target,
                }),
                schema::TaskQueueEvent::Operating { .. } => {
                    ResourceEffect::Operating(SmallVec::new())
                }
                schema::TaskQueueEvent::Finalizing { .. } | schema::TaskQueueEvent::Exit { .. } => {
                    ResourceEffect::Other
                }
            },
            Self::TemporaryIoChannel(event) => match event {
                schema::TemporaryIoChannelEvent::Initializing {
                    instance_name,
                    worker_id,
                    ..
                } => ResourceEffect::Initializing(ResourceInit {
                    type_name: TEMPORARY_IO_CHANNEL_TYPE_NAME,
                    instance_name: instance_name.clone(),
                    parent_id: worker_id.target,
                }),
                schema::TemporaryIoChannelEvent::Operating { .. } => {
                    ResourceEffect::Operating(SmallVec::new())
                }
                schema::TemporaryIoChannelEvent::Finalizing { .. }
                | schema::TemporaryIoChannelEvent::Exit { .. } => ResourceEffect::Other,
            },
            Self::BufferPoolMemory(event) => match event {
                schema::BufferPoolMemoryEvent::Initializing {
                    instance_name,
                    engine_id,
                    ..
                } => ResourceEffect::Initializing(ResourceInit {
                    type_name: BUFFER_POOL_MEMORY_TYPE_NAME,
                    instance_name: instance_name.clone(),
                    parent_id: engine_id.target,
                }),
                schema::BufferPoolMemoryEvent::Operating { limits, .. } => {
                    ResourceEffect::Operating(smallvec::smallvec![CapacityValue::new(
                        MEMORY_BYTES_CAPACITY_NAME,
                        limits.bytes
                    )])
                }
                schema::BufferPoolMemoryEvent::Resizing { .. }
                | schema::BufferPoolMemoryEvent::Finalizing { .. }
                | schema::BufferPoolMemoryEvent::Exit { .. } => ResourceEffect::Other,
            },
            Self::TemporaryStorage(event) => match event {
                schema::TemporaryStorageEvent::Initializing {
                    instance_name,
                    engine_id,
                    ..
                } => ResourceEffect::Initializing(ResourceInit {
                    type_name: TEMPORARY_STORAGE_TYPE_NAME,
                    instance_name: instance_name.clone(),
                    parent_id: engine_id.target,
                }),
                schema::TemporaryStorageEvent::Operating { limits, .. } => {
                    ResourceEffect::Operating(smallvec::smallvec![CapacityValue::new(
                        MEMORY_BYTES_CAPACITY_NAME,
                        limits.bytes
                    )])
                }
                schema::TemporaryStorageEvent::Resizing { .. }
                | schema::TemporaryStorageEvent::Finalizing { .. }
                | schema::TemporaryStorageEvent::Exit { .. } => ResourceEffect::Other,
            },
            Self::TemporaryDirectoryStorage(event) => match event {
                schema::TemporaryDirectoryStorageEvent::Initializing {
                    instance_name,
                    engine_id,
                    ..
                } => ResourceEffect::Initializing(ResourceInit {
                    type_name: TEMPORARY_DIRECTORY_STORAGE_TYPE_NAME,
                    instance_name: instance_name.clone(),
                    parent_id: engine_id.target,
                }),
                schema::TemporaryDirectoryStorageEvent::Operating { limits, .. } => {
                    ResourceEffect::Operating(smallvec::smallvec![CapacityValue::new(
                        MEMORY_BYTES_CAPACITY_NAME,
                        limits.bytes
                    )])
                }
                schema::TemporaryDirectoryStorageEvent::Resizing { .. }
                | schema::TemporaryDirectoryStorageEvent::Finalizing { .. }
                | schema::TemporaryDirectoryStorageEvent::Exit { .. } => ResourceEffect::Other,
            },
        }
    }
}

fn derive_resource_scope_types(
    model: &DuckDbModel,
) -> AnalyzerResult<HashMap<String, ResourceGroupTypeDecl>> {
    fn populate(
        node: &quent_analyzer::resource::tree::ResourceTreeNode,
        model: &DuckDbModel,
        declarations: &mut HashMap<String, (BTreeSet<String>, BTreeSet<String>)>,
    ) -> AnalyzerResult<()> {
        if !node.is_resource {
            let mut contained_types = Vec::new();
            for resource_id in node.iter_resource_ids() {
                contained_types.push(model.resource_type_of(resource_id)?);
            }
            if !contained_types.is_empty() {
                let type_name = model
                    .ref_tree_entity(node.entity_id)?
                    .type_name()
                    .to_owned();
                let (used_by, contains) = declarations.entry(type_name).or_default();
                for resource_type in contained_types {
                    contains.insert(resource_type.name.clone());
                    used_by.extend(resource_type.used_by.iter().cloned());
                }
            }
        }
        for child in &node.children {
            populate(child, model, declarations)?;
        }
        Ok(())
    }

    let tree = quent_analyzer::resource::tree::ResourceTreeNode::try_new(model)?;
    let mut declarations = HashMap::default();
    populate(&tree, model, &mut declarations)?;
    Ok(declarations
        .into_iter()
        .map(|(name, (used_by_entity_types, contains_resource_types))| {
            (
                name.clone(),
                ResourceGroupTypeDecl {
                    name,
                    used_by_entity_types: used_by_entity_types.into_iter().collect(),
                    contains_resource_types: contains_resource_types.into_iter().collect(),
                },
            )
        })
        .collect())
}

pub struct DuckDbModel {
    pub(crate) engine: Engine,
    pub(crate) workers: HashMap<Uuid, Worker>,
    pub(crate) query_groups: HashMap<Uuid, QueryGroup>,
    pub(crate) queries: HashMap<Uuid, Query>,
    pub(crate) plans: HashMap<Uuid, Plan>,
    pub(crate) operators: HashMap<Uuid, Operator>,
    pub(crate) ports: HashMap<Uuid, Port>,
    pub(crate) runtime_resources: HashMap<Uuid, RuntimeResource>,
    pub(crate) resource_types: HashMap<String, ResourceTypeDecl>,
    pub(crate) pipeline_tasks: HashMap<Uuid, PipelineTask>,
    pub(crate) chunk_transfers: HashMap<Uuid, ChunkTransfer>,
    pub(crate) operator_invocations: HashMap<Uuid, OperatorInvocation>,
    pub(crate) temporary_block_ios: HashMap<Uuid, TemporaryBlockIo>,
    pub(crate) memory_accounts: HashMap<Uuid, MemoryAccount>,
    pub(crate) resource_group_types: HashMap<String, ResourceGroupTypeDecl>,
    query_index: QueryIndex,
}

#[derive(Default)]
struct QueryIndex {
    pipeline_tasks: HashMap<Uuid, SmallVec<[Uuid; 8]>>,
    chunk_transfers: HashMap<Uuid, SmallVec<[Uuid; 8]>>,
    operator_invocations: HashMap<Uuid, SmallVec<[Uuid; 8]>>,
    temporary_block_ios: HashMap<Uuid, SmallVec<[Uuid; 8]>>,
}

impl DuckDbModel {
    pub(crate) fn query_view(
        &self,
        query_id: Uuid,
    ) -> AnalyzerResult<crate::view::DuckDbQueryView<'_>> {
        crate::view::DuckDbQueryView::try_new(self, query_id)
    }

    pub(crate) fn resource_matches(&self, id: Uuid, type_name: &str, parent_id: Uuid) -> bool {
        self.runtime_resources.get(&id).is_some_and(|resource| {
            resource.type_name == type_name && resource.parent_id == parent_id
        })
    }

    pub(crate) fn pipeline_tasks_for(&self, query_id: Uuid) -> impl Iterator<Item = &PipelineTask> {
        self.query_index
            .pipeline_tasks
            .get(&query_id)
            .into_iter()
            .flatten()
            .filter_map(|id| self.pipeline_tasks.get(id))
    }

    pub(crate) fn chunk_transfers_for(
        &self,
        query_id: Uuid,
    ) -> impl Iterator<Item = &ChunkTransfer> {
        self.query_index
            .chunk_transfers
            .get(&query_id)
            .into_iter()
            .flatten()
            .filter_map(|id| self.chunk_transfers.get(id))
    }

    pub(crate) fn operator_invocations_for(
        &self,
        query_id: Uuid,
    ) -> impl Iterator<Item = &OperatorInvocation> {
        self.query_index
            .operator_invocations
            .get(&query_id)
            .into_iter()
            .flatten()
            .filter_map(|id| self.operator_invocations.get(id))
    }

    pub(crate) fn temporary_block_ios_for(
        &self,
        query_id: Uuid,
    ) -> impl Iterator<Item = &TemporaryBlockIo> {
        self.query_index
            .temporary_block_ios
            .get(&query_id)
            .into_iter()
            .flatten()
            .filter_map(|id| self.temporary_block_ios.get(id))
    }
}

impl Model for DuckDbModel {
    type EntityIdType = EntityRef;

    fn try_entity_ref(&self, id: Uuid) -> AnalyzerResult<EntityRef> {
        if self.engine.id() == id {
            return Ok(EntityRef::Engine(id));
        }
        if self.workers.contains_key(&id) {
            return Ok(EntityRef::Worker(id));
        }
        if self.query_groups.contains_key(&id) {
            return Ok(EntityRef::QueryGroup(id));
        }
        if self.queries.contains_key(&id) {
            return Ok(EntityRef::Query(id));
        }
        if self.plans.contains_key(&id) {
            return Ok(EntityRef::Plan(id));
        }
        if self.operators.contains_key(&id) {
            return Ok(EntityRef::Operator(id));
        }
        if self.ports.contains_key(&id) {
            return Ok(EntityRef::Port(id));
        }
        if self.runtime_resources.contains_key(&id) {
            return Ok(EntityRef::Resource(id));
        }

        for (type_name, contains) in [
            ("pipeline_task", self.pipeline_tasks.contains_key(&id)),
            (
                "operator_invocation",
                self.operator_invocations.contains_key(&id),
            ),
            (
                "temporary_block_io",
                self.temporary_block_ios.contains_key(&id),
            ),
            ("memory_account", self.memory_accounts.contains_key(&id)),
        ] {
            if contains {
                return Ok(EntityRef::Application {
                    type_name: type_name.to_owned(),
                    id,
                });
            }
        }

        Err(AnalyzerError::InvalidId(id))
    }
}

impl QueryEngineModel for DuckDbModel {
    type Engine = Engine;
    type Query = Query;
    type QueryGroup = QueryGroup;
    type Worker = Worker;
    type Plan = Plan;
    type Operator = Operator;
    type Port = Port;

    fn engine(&self) -> AnalyzerResult<&Engine> {
        Ok(&self.engine)
    }
    fn query(&self, id: Uuid) -> AnalyzerResult<&Query> {
        self.queries.get(&id).ok_or(AnalyzerError::InvalidId(id))
    }
    fn query_group(&self, id: Uuid) -> AnalyzerResult<&QueryGroup> {
        self.query_groups
            .get(&id)
            .ok_or(AnalyzerError::InvalidId(id))
    }
    fn worker(&self, id: Uuid) -> AnalyzerResult<&Worker> {
        self.workers.get(&id).ok_or(AnalyzerError::InvalidId(id))
    }
    fn plan(&self, id: Uuid) -> AnalyzerResult<&Plan> {
        self.plans.get(&id).ok_or(AnalyzerError::InvalidId(id))
    }
    fn operator(&self, id: Uuid) -> AnalyzerResult<&Operator> {
        self.operators.get(&id).ok_or(AnalyzerError::InvalidId(id))
    }
    fn port(&self, id: Uuid) -> AnalyzerResult<&Port> {
        self.ports.get(&id).ok_or(AnalyzerError::InvalidId(id))
    }
    fn queries(&self) -> impl Iterator<Item = &Query> {
        self.queries.values()
    }
    fn query_groups(&self) -> impl Iterator<Item = &QueryGroup> {
        self.query_groups.values()
    }
    fn workers(&self) -> impl Iterator<Item = &Worker> {
        self.workers.values()
    }
    fn plans(&self) -> impl Iterator<Item = &Plan> {
        self.plans.values()
    }
    fn operators(&self) -> impl Iterator<Item = &Operator> {
        self.operators.values()
    }
    fn ports(&self) -> impl Iterator<Item = &Port> {
        self.ports.values()
    }
    fn plan_tree(&self, query_id: Uuid) -> AnalyzerResult<PlanTree> {
        PlanTree::try_new(self.plans.values(), query_id)
    }
}

impl QueryEngineModelMut for DuckDbModel {
    fn operator_mut(&mut self, id: Uuid) -> AnalyzerResult<&mut Operator> {
        self.operators
            .get_mut(&id)
            .ok_or(AnalyzerError::InvalidId(id))
    }
}

impl ResourceCollection for DuckDbModel {
    fn resources(&self) -> impl Iterator<Item = &dyn Resource> {
        self.runtime_resources
            .values()
            .map(|resource| resource as &dyn Resource)
    }
    fn resource(&self, id: Uuid) -> AnalyzerResult<&dyn Resource> {
        self.runtime_resources
            .get(&id)
            .map(|resource| resource as &dyn Resource)
            .ok_or(AnalyzerError::InvalidId(id))
    }
    fn resource_type(&self, name: &str) -> AnalyzerResult<&ResourceTypeDecl> {
        self.resource_types
            .get(name)
            .ok_or_else(|| AnalyzerError::InvalidTypeName(name.to_owned()))
    }
}

impl RefTreeCollection for DuckDbModel {
    fn ref_tree_entities(&self) -> impl Iterator<Item = &dyn RefTreeEntity> {
        std::iter::once(&self.engine as &dyn RefTreeEntity)
            .chain(
                self.workers
                    .values()
                    .map(|entity| entity as &dyn RefTreeEntity),
            )
            .chain(
                self.query_groups
                    .values()
                    .map(|entity| entity as &dyn RefTreeEntity),
            )
            .chain(
                self.queries
                    .values()
                    .map(|entity| entity as &dyn RefTreeEntity),
            )
            .chain(
                self.plans
                    .values()
                    .map(|entity| entity as &dyn RefTreeEntity),
            )
            .chain(
                self.operators
                    .values()
                    .map(|entity| entity as &dyn RefTreeEntity),
            )
            .chain(
                self.ports
                    .values()
                    .map(|entity| entity as &dyn RefTreeEntity),
            )
            .chain(
                self.runtime_resources
                    .values()
                    .map(|entity| entity as &dyn RefTreeEntity),
            )
    }

    fn ref_tree_entity(&self, id: Uuid) -> AnalyzerResult<&dyn RefTreeEntity> {
        if self.engine.id() == id {
            return Ok(&self.engine);
        }
        self.workers
            .get(&id)
            .map(|entity| entity as &dyn RefTreeEntity)
            .or_else(|| {
                self.query_groups
                    .get(&id)
                    .map(|entity| entity as &dyn RefTreeEntity)
            })
            .or_else(|| {
                self.queries
                    .get(&id)
                    .map(|entity| entity as &dyn RefTreeEntity)
            })
            .or_else(|| {
                self.plans
                    .get(&id)
                    .map(|entity| entity as &dyn RefTreeEntity)
            })
            .or_else(|| {
                self.operators
                    .get(&id)
                    .map(|entity| entity as &dyn RefTreeEntity)
            })
            .or_else(|| {
                self.ports
                    .get(&id)
                    .map(|entity| entity as &dyn RefTreeEntity)
            })
            .or_else(|| {
                self.runtime_resources
                    .get(&id)
                    .map(|entity| entity as &dyn RefTreeEntity)
            })
            .ok_or(AnalyzerError::InvalidId(id))
    }
}

impl Using for DuckDbModel {
    fn usages(&self) -> impl Iterator<Item = impl Usage<'_>> {
        self.pipeline_tasks.values().flat_map(|fsm| fsm.usages())
    }
}

pub(crate) struct DuckDbModelBuilder {
    engine_id: Uuid,
    engine: Option<Engine>,
    workers: HashMap<Uuid, Worker>,
    query_groups: HashMap<Uuid, QueryGroup>,
    queries: HashMap<Uuid, QueryBuilder>,
    plans: HashMap<Uuid, Plan>,
    operators: HashMap<Uuid, Operator>,
    ports: HashMap<Uuid, Port>,
    resources: HashMap<Uuid, RuntimeResourceBuilder>,
    pipeline_tasks: HashMap<Uuid, PipelineTaskBuilder>,
    chunk_transfers: HashMap<Uuid, ChunkTransferBuilder>,
    operator_invocations: HashMap<Uuid, OperatorInvocationBuilder>,
    temporary_block_ios: HashMap<Uuid, TemporaryBlockIoBuilder>,
    memory_accounts: HashMap<Uuid, MemoryAccountBuilder>,
}

impl DuckDbModelBuilder {
    pub(crate) fn try_new(engine_id: Uuid) -> AnalyzerResult<Self> {
        if engine_id.is_nil() {
            return Err(AnalyzerError::Validation(
                "engine id cannot be nil".to_owned(),
            ));
        }
        Ok(Self {
            engine_id,
            engine: None,
            workers: HashMap::default(),
            query_groups: HashMap::default(),
            queries: HashMap::default(),
            plans: HashMap::default(),
            operators: HashMap::default(),
            ports: HashMap::default(),
            resources: HashMap::default(),
            pipeline_tasks: HashMap::default(),
            chunk_transfers: HashMap::default(),
            operator_invocations: HashMap::default(),
            temporary_block_ios: HashMap::default(),
            memory_accounts: HashMap::default(),
        })
    }

    pub(crate) fn try_push(&mut self, event: Event<schema::DuckDbEvent>) -> AnalyzerResult<()> {
        let Event {
            id,
            timestamp,
            data,
        } = event;
        match data {
            schema::DuckDbEvent::Engine(data) => self.push_engine(Event::new(id, timestamp, data)),
            schema::DuckDbEvent::Worker(data) => Self::push_entity(
                &mut self.workers,
                Event::new(id, timestamp, data),
                Worker::try_from_event,
                Worker::push,
            ),
            schema::DuckDbEvent::QueryGroup(data) => Self::push_entity(
                &mut self.query_groups,
                Event::new(id, timestamp, data),
                QueryGroup::try_from_event,
                QueryGroup::push,
            ),
            schema::DuckDbEvent::Plan(data) => Self::push_entity(
                &mut self.plans,
                Event::new(id, timestamp, data),
                Plan::try_from_event,
                Plan::push,
            ),
            schema::DuckDbEvent::Operator(data) => Self::push_entity(
                &mut self.operators,
                Event::new(id, timestamp, data),
                Operator::try_from_event,
                Operator::push,
            ),
            schema::DuckDbEvent::Port(data) => Self::push_entity(
                &mut self.ports,
                Event::new(id, timestamp, data),
                Port::try_from_event,
                Port::push,
            ),
            schema::DuckDbEvent::Query(data) => {
                Self::push_fsm(&mut self.queries, Event::new(id, timestamp, data))?;
                Ok(())
            }
            schema::DuckDbEvent::PipelineTask(data) => {
                Self::push_fsm(&mut self.pipeline_tasks, Event::new(id, timestamp, data))?;
                Ok(())
            }
            schema::DuckDbEvent::ChunkTransfer(data) => {
                Self::push_fsm(&mut self.chunk_transfers, Event::new(id, timestamp, data))?;
                Ok(())
            }
            schema::DuckDbEvent::OperatorInvocation(data) => {
                Self::push_fsm(
                    &mut self.operator_invocations,
                    Event::new(id, timestamp, data),
                )?;
                Ok(())
            }
            schema::DuckDbEvent::TemporaryBlockIo(data) => {
                Self::push_fsm(
                    &mut self.temporary_block_ios,
                    Event::new(id, timestamp, data),
                )?;
                Ok(())
            }
            schema::DuckDbEvent::MemoryAccount(data) => {
                Self::push_fsm(&mut self.memory_accounts, Event::new(id, timestamp, data))?;
                Ok(())
            }
            schema::DuckDbEvent::ExecutionThread(data) => {
                self.push_execution_thread(id, timestamp, data)
            }
            schema::DuckDbEvent::TaskQueue(data) => self.push_task_queue(id, timestamp, data),
            schema::DuckDbEvent::TemporaryIoChannel(data) => {
                self.push_temporary_io_channel(id, timestamp, data)
            }
            schema::DuckDbEvent::BufferPoolMemory(data) => {
                self.push_buffer_pool_memory(id, timestamp, data)
            }
            schema::DuckDbEvent::TemporaryStorage(data) => {
                self.push_temporary_storage(id, timestamp, data)
            }
            schema::DuckDbEvent::TemporaryDirectoryStorage(data) => {
                self.push_temporary_directory_storage(id, timestamp, data)
            }
        }
    }

    fn push_engine(&mut self, event: Event<schema::EngineEvent>) -> AnalyzerResult<()> {
        if event.id != self.engine_id {
            return Err(AnalyzerError::Validation(format!(
                "multiple engine instances in one model: expected {}, found {}",
                self.engine_id, event.id
            )));
        }
        if let Some(engine) = &mut self.engine {
            engine.push(event)
        } else {
            self.engine = Some(Engine::try_from_event(event)?);
            Ok(())
        }
    }

    fn push_entity<T, E>(
        map: &mut HashMap<Uuid, T>,
        event: Event<E>,
        create: fn(Event<E>) -> AnalyzerResult<T>,
        push: fn(&mut T, Event<E>) -> AnalyzerResult<()>,
    ) -> AnalyzerResult<()> {
        match map.entry(event.id) {
            Entry::Occupied(entry) => push(entry.into_mut(), event),
            Entry::Vacant(entry) => {
                entry.insert(create(event)?);
                Ok(())
            }
        }
    }

    fn push_fsm<T: quent_analyzer::fsm::native::TransitionEvent>(
        map: &mut HashMap<Uuid, AnalyzedFsmBuilder<T>>,
        event: Event<T>,
    ) -> AnalyzerResult<()> {
        let builder = match map.entry(event.id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(AnalyzedFsmBuilder::try_new(event.id)?),
        };
        builder.push_transition(event);
        Ok(())
    }

    fn push_resource(
        &mut self,
        id: Uuid,
        timestamp: TimeUnixNanoSec,
        event: RuntimeResourceEvent,
    ) -> AnalyzerResult<()> {
        if id.is_nil() {
            return Err(AnalyzerError::Validation(
                "resource id cannot be nil".to_owned(),
            ));
        }

        self.resources
            .entry(id)
            .or_insert_with(|| RuntimeResourceBuilder {
                transitions: Vec::new(),
            })
            .transitions
            .push(RuntimeResourceTransition { timestamp, event });
        Ok(())
    }

    fn push_execution_thread(
        &mut self,
        id: Uuid,
        timestamp: TimeUnixNanoSec,
        event: schema::ExecutionThreadEvent,
    ) -> AnalyzerResult<()> {
        self.push_resource(id, timestamp, RuntimeResourceEvent::ExecutionThread(event))
    }

    fn push_task_queue(
        &mut self,
        id: Uuid,
        timestamp: TimeUnixNanoSec,
        event: schema::TaskQueueEvent,
    ) -> AnalyzerResult<()> {
        self.push_resource(id, timestamp, RuntimeResourceEvent::TaskQueue(event))
    }

    fn push_temporary_io_channel(
        &mut self,
        id: Uuid,
        timestamp: TimeUnixNanoSec,
        event: schema::TemporaryIoChannelEvent,
    ) -> AnalyzerResult<()> {
        self.push_resource(
            id,
            timestamp,
            RuntimeResourceEvent::TemporaryIoChannel(event),
        )
    }

    fn push_buffer_pool_memory(
        &mut self,
        id: Uuid,
        timestamp: TimeUnixNanoSec,
        event: schema::BufferPoolMemoryEvent,
    ) -> AnalyzerResult<()> {
        self.push_resource(id, timestamp, RuntimeResourceEvent::BufferPoolMemory(event))
    }

    fn push_temporary_storage(
        &mut self,
        id: Uuid,
        timestamp: TimeUnixNanoSec,
        event: schema::TemporaryStorageEvent,
    ) -> AnalyzerResult<()> {
        self.push_resource(id, timestamp, RuntimeResourceEvent::TemporaryStorage(event))
    }

    fn push_temporary_directory_storage(
        &mut self,
        id: Uuid,
        timestamp: TimeUnixNanoSec,
        event: schema::TemporaryDirectoryStorageEvent,
    ) -> AnalyzerResult<()> {
        self.push_resource(
            id,
            timestamp,
            RuntimeResourceEvent::TemporaryDirectoryStorage(event),
        )
    }

    pub(crate) fn try_build(self) -> AnalyzerResult<DuckDbModel> {
        let engine = self.engine.ok_or_else(|| {
            AnalyzerError::IncompleteEntity(format!("engine {} has no events", self.engine_id))
        })?;
        let queries = build_fsms(self.queries, Query::try_from_builder, "query")?;
        let pipeline_tasks = build_fsms(
            self.pipeline_tasks,
            PipelineTask::try_from_builder,
            "pipeline task",
        )?;
        let chunk_transfers = build_fsms(
            self.chunk_transfers,
            ChunkTransfer::try_from_builder,
            "chunk transfer",
        )?;
        let operator_invocations = build_fsms(
            self.operator_invocations,
            OperatorInvocation::try_from_builder,
            "operator invocation",
        )?;
        let temporary_block_ios = build_fsms(
            self.temporary_block_ios,
            TemporaryBlockIo::try_from_builder,
            "temporary block I/O",
        )?;
        let memory_accounts = build_fsms(
            self.memory_accounts,
            MemoryAccount::try_from_builder,
            "memory account",
        )?;

        let runtime_resources = build_resources(self.resources)?;
        let mut resource_types = resource_types();
        resource_types
            .get_mut(TASK_QUEUE_TYPE_NAME)
            .unwrap()
            .used_by
            .insert("pipeline_task".to_owned());
        resource_types
            .get_mut(EXECUTION_THREAD_TYPE_NAME)
            .unwrap()
            .used_by
            .extend(["pipeline_task".to_owned(), "operator_invocation".to_owned()]);
        resource_types
            .get_mut(TEMPORARY_IO_CHANNEL_TYPE_NAME)
            .unwrap()
            .used_by
            .insert("temporary_block_io".to_owned());
        for name in [
            BUFFER_POOL_MEMORY_TYPE_NAME,
            TEMPORARY_STORAGE_TYPE_NAME,
            TEMPORARY_DIRECTORY_STORAGE_TYPE_NAME,
        ] {
            resource_types
                .get_mut(name)
                .unwrap()
                .used_by
                .insert("memory_account".to_owned());
        }

        let mut model = DuckDbModel {
            engine,
            workers: self.workers,
            query_groups: self.query_groups,
            queries,
            plans: self.plans,
            operators: self.operators,
            ports: self.ports,
            runtime_resources,
            resource_types,
            pipeline_tasks,
            chunk_transfers,
            operator_invocations,
            temporary_block_ios,
            memory_accounts,
            resource_group_types: HashMap::default(),
            query_index: QueryIndex::default(),
        };

        model.query_index.pipeline_tasks =
            index_by_query(&model.pipeline_tasks, PipelineTaskExt::query_id);
        model.query_index.chunk_transfers =
            index_by_query(&model.chunk_transfers, ChunkTransferExt::query_id);
        model.query_index.operator_invocations =
            index_by_query(&model.operator_invocations, OperatorInvocationExt::query_id);
        model.query_index.temporary_block_ios =
            index_by_query(&model.temporary_block_ios, TemporaryBlockIoExt::query_id);

        let active_spans: Vec<(Uuid, quent_time::span::SpanUnixNanoSec)> = model
            .operator_invocations
            .values()
            .filter_map(|invocation| Some((invocation.operator_id()?, invocation.active_span()?)))
            .collect();
        for (operator_id, span) in active_spans {
            if let Ok(operator) = model.operator_mut(operator_id) {
                operator.extend_active_span(span);
            }
        }
        model.resource_group_types = derive_resource_scope_types(&model)?;
        Ok(model)
    }
}

fn index_by_query<T>(
    entities: &HashMap<Uuid, T>,
    query_id: fn(&T) -> Option<Uuid>,
) -> HashMap<Uuid, SmallVec<[Uuid; 8]>> {
    let mut index = HashMap::default();
    for (id, entity) in entities {
        if let Some(query_id) = query_id(entity) {
            index
                .entry(query_id)
                .or_insert_with(SmallVec::new)
                .push(*id);
        }
    }
    index
}

fn build_resources(
    builders: HashMap<Uuid, RuntimeResourceBuilder>,
) -> AnalyzerResult<HashMap<Uuid, RuntimeResource>> {
    let mut resources = HashMap::default();
    for (id, builder) in builders {
        if let Some(resource) = build_resource(id, builder)? {
            resources.insert(id, resource);
        }
    }
    Ok(resources)
}

fn build_resource(
    id: Uuid,
    mut builder: RuntimeResourceBuilder,
) -> AnalyzerResult<Option<RuntimeResource>> {
    builder
        .transitions
        .sort_unstable_by_key(|transition| (transition.timestamp, transition.event.sequence()));
    if builder.transitions.windows(2).any(|transitions| {
        (transitions[0].timestamp, transitions[0].event.sequence())
            == (transitions[1].timestamp, transitions[1].event.sequence())
    }) {
        return Err(AnalyzerError::Validation(format!(
            "resource {id} has duplicate transitions"
        )));
    }

    let mut transitions = builder.transitions.into_iter();
    let first = transitions.next().ok_or_else(|| {
        AnalyzerError::IncompleteEntity(format!("resource {id} has no transitions"))
    })?;
    let ResourceEffect::Initializing(init) = first.event.effect() else {
        return Err(AnalyzerError::IncompleteEntity(format!(
            "resource {id} has no initializing event"
        )));
    };
    if init.parent_id.is_nil() {
        return Err(AnalyzerError::Validation(format!(
            "resource {id} has a nil parent id"
        )));
    }

    let mut resource = RuntimeResource {
        id,
        type_name: init.type_name,
        instance_name: init.instance_name,
        parent_id: init.parent_id,
        earliest_timestamp: first.timestamp,
        latest_timestamp: first.timestamp,
        bounds: SmallVec::new(),
    };
    let mut operating = false;
    let mut last_event = first.event;
    for transition in transitions {
        if !last_event.is_valid_next(&transition.event) {
            return Err(AnalyzerError::Validation(format!(
                "resource {id} cannot transition from '{}' to '{}'",
                last_event.name(),
                transition.event.name()
            )));
        }

        resource.latest_timestamp = transition.timestamp;
        if let ResourceEffect::Operating(bounds) = transition.event.effect() {
            operating = true;
            resource.bounds = bounds;
        }
        last_event = transition.event;
    }

    if !operating {
        tracing::debug!(%id, "omit resource without an observed operating state");
        return Ok(None);
    }
    Ok(Some(resource))
}

fn build_fsms<T, U>(
    builders: HashMap<Uuid, AnalyzedFsmBuilder<T>>,
    build: fn(AnalyzedFsmBuilder<T>) -> AnalyzerResult<U>,
    name: &str,
) -> AnalyzerResult<HashMap<Uuid, U>>
where
    T: quent_analyzer::fsm::native::TransitionEvent,
{
    let mut fsms = HashMap::default();
    for (id, builder) in builders {
        match build(builder) {
            Ok(fsm) => {
                fsms.insert(id, fsm);
            }
            Err(AnalyzerError::IncompleteFsm(_)) => {
                tracing::debug!(%id, entity_type = name, "omit incomplete FSM")
            }
            Err(error) => return Err(error),
        }
    }
    Ok(fsms)
}

fn resource_types() -> HashMap<String, ResourceTypeDecl> {
    [
        ResourceTypeDecl::unit(EXECUTION_THREAD_TYPE_NAME),
        ResourceTypeDecl::new(
            TASK_QUEUE_TYPE_NAME,
            [CapacityDecl::new_occupancy(QUEUE_ENTRIES_CAPACITY_NAME)],
        ),
        ResourceTypeDecl::new(
            TEMPORARY_IO_CHANNEL_TYPE_NAME,
            [
                CapacityDecl::new_rate(IO_OPERATIONS_CAPACITY_NAME),
                CapacityDecl::new_rate(IO_BUFFER_BYTES_CAPACITY_NAME),
            ]
            .as_slice(),
        ),
        ResourceTypeDecl::new(
            BUFFER_POOL_MEMORY_TYPE_NAME,
            [CapacityDecl::new_occupancy(MEMORY_BYTES_CAPACITY_NAME)],
        ),
        ResourceTypeDecl::new(
            TEMPORARY_STORAGE_TYPE_NAME,
            [CapacityDecl::new_occupancy(MEMORY_BYTES_CAPACITY_NAME)],
        ),
        ResourceTypeDecl::new(
            TEMPORARY_DIRECTORY_STORAGE_TYPE_NAME,
            [CapacityDecl::new_occupancy(MEMORY_BYTES_CAPACITY_NAME)],
        ),
    ]
    .into_iter()
    .map(|declaration| (declaration.name.clone(), declaration))
    .collect()
}
