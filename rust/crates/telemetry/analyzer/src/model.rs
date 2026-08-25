use std::collections::{HashMap, HashSet};

use duckdb_telemetry_model::{DuckDBEvent, memory_account, runtime_resource};
use quent_analyzer::{
    AnalyzerError, AnalyzerResult, Entity, Model,
    resource::{
        CapacityDecl, CapacityValue, Resource, ResourceCapacities, ResourceGroup,
        ResourceGroupTypeDecl, ResourceTypeDecl, Usage, Using,
        collection::{
            InMemoryResources, InMemoryResourcesBuilder, ResourceCollection,
            derive_resource_group_types,
        },
        runtime::RtResourceTransition,
    },
};
use quent_events::Event;
use quent_model::FsmEvent;
use quent_query_engine_analyzer::{
    OperatorEntity, OperatorEntityMut, PlanEntity, QueryEngineModel,
    plain::legacy::{
        Engine, InMemoryQueryEngineModel, InMemoryQueryEngineModelBuilder, Operator, Plan, Port,
        Query, QueryEngineEntityId, QueryGroup, Worker,
    },
    plan_tree::PlanTree,
};
use quent_query_engine_model::{QueryEngineEvent, engine, worker};
use quent_simulator_ui::EntityRef;
use uuid::Uuid;

use crate::{
    chunk_transfer::{ChunkTransfer, ChunkTransferBuilder},
    memory_account::{MemoryAccount, MemoryAccountBuilder, MemoryAccountExt},
    operator_invocation::{OperatorInvocation, OperatorInvocationBuilder, OperatorInvocationExt},
    pipeline_task::{PipelineTask, PipelineTaskBuilder, PipelineTaskExt, task_plan_id},
    temporary_block_io::{TemporaryBlockIo, TemporaryBlockIoBuilder, TemporaryBlockIoExt},
};

pub(crate) const EXECUTION_THREAD_TYPE_NAME: &str = "execution_thread";
pub(crate) const TASK_QUEUE_TYPE_NAME: &str = "task_queue";
pub(crate) const QUEUE_ENTRIES_CAPACITY_NAME: &str = "capacity_entries";
pub(crate) const TEMPORARY_IO_CHANNEL_TYPE_NAME: &str = "temporary_io_channel";
pub(crate) const IO_OPERATIONS_CAPACITY_NAME: &str = "capacity_operations";
pub(crate) const IO_BUFFER_BYTES_CAPACITY_NAME: &str = "capacity_buffer_bytes";
pub(crate) const MEMORY_BYTES_CAPACITY_NAME: &str = "capacity_bytes";
pub(crate) const BUFFER_POOL_MEMORY_TYPE_NAME: &str = "buffer_pool_memory";
pub(crate) const TEMPORARY_STORAGE_TYPE_NAME: &str = "temporary_storage";
pub(crate) const TEMPORARY_DIRECTORY_STORAGE_TYPE_NAME: &str = "temporary_directory_storage";

fn validate_resource_type(actual: &str, expected: &str, id: Uuid) -> AnalyzerResult<()> {
    if actual == expected {
        return Ok(());
    }

    Err(AnalyzerError::InvalidArgument(format!(
        "resource {id} declared type {actual:?}, expected {expected:?}"
    )))
}

fn insert_resource_types(resources: &mut InMemoryResources) {
    resources.resource_types.insert(
        EXECUTION_THREAD_TYPE_NAME.to_owned(),
        ResourceTypeDecl::unit(EXECUTION_THREAD_TYPE_NAME),
    );
    resources.resource_types.insert(
        TASK_QUEUE_TYPE_NAME.to_owned(),
        ResourceTypeDecl::new(
            TASK_QUEUE_TYPE_NAME,
            [CapacityDecl::new_occupancy(QUEUE_ENTRIES_CAPACITY_NAME)],
        ),
    );
    resources.resource_types.insert(
        TEMPORARY_IO_CHANNEL_TYPE_NAME.to_owned(),
        ResourceTypeDecl::new(
            TEMPORARY_IO_CHANNEL_TYPE_NAME,
            vec![
                CapacityDecl::new_rate(IO_OPERATIONS_CAPACITY_NAME),
                CapacityDecl::new_rate(IO_BUFFER_BYTES_CAPACITY_NAME),
            ],
        ),
    );
    for type_name in [
        BUFFER_POOL_MEMORY_TYPE_NAME,
        TEMPORARY_STORAGE_TYPE_NAME,
        TEMPORARY_DIRECTORY_STORAGE_TYPE_NAME,
    ] {
        resources.resource_types.insert(
            type_name.to_owned(),
            ResourceTypeDecl::new(
                type_name,
                [CapacityDecl::new_occupancy(MEMORY_BYTES_CAPACITY_NAME)],
            ),
        );
    }
}

pub struct DuckDbModel {
    pub(crate) query_engine: InMemoryQueryEngineModel,
    pub(crate) runtime_resources: InMemoryResources,
    pub(crate) pipeline_tasks: HashMap<Uuid, PipelineTask>,
    pub(crate) chunk_transfers: HashMap<Uuid, ChunkTransfer>,
    pub(crate) operator_invocations: HashMap<Uuid, OperatorInvocation>,
    pub(crate) temporary_block_ios: HashMap<Uuid, TemporaryBlockIo>,
    pub(crate) memory_accounts: HashMap<Uuid, MemoryAccount>,
    pub(crate) resource_group_types: HashMap<String, ResourceGroupTypeDecl>,
}

impl Model for DuckDbModel {
    type EntityIdType = EntityRef;

    fn try_entity_ref(&self, entity_id: Uuid) -> AnalyzerResult<EntityRef> {
        if let Ok(entity) = self.query_engine.try_entity_ref(entity_id) {
            return Ok(match entity {
                QueryEngineEntityId::Engine(id) => EntityRef::Engine(id),
                QueryEngineEntityId::Worker(id) => EntityRef::Worker(id),
                QueryEngineEntityId::QueryGroup(id) => EntityRef::QueryGroup(id),
                QueryEngineEntityId::Query(id) => EntityRef::Query(id),
                QueryEngineEntityId::Plan(id) => EntityRef::Plan(id),
                QueryEngineEntityId::Operator(id) => EntityRef::Operator(id),
                QueryEngineEntityId::Port(id) => EntityRef::Port(id),
            });
        }

        if self.runtime_resources.resources.contains_key(&entity_id) {
            return Ok(EntityRef::Resource(entity_id));
        }
        if self
            .runtime_resources
            .resource_groups
            .contains_key(&entity_id)
        {
            return Ok(EntityRef::ResourceGroup(entity_id));
        }

        (self.pipeline_tasks.contains_key(&entity_id)
            || self.operator_invocations.contains_key(&entity_id)
            || self.temporary_block_ios.contains_key(&entity_id)
            || self.memory_accounts.contains_key(&entity_id))
        .then_some(EntityRef::Task(entity_id))
        .ok_or(AnalyzerError::InvalidId(entity_id))
    }

    fn root(&self) -> AnalyzerResult<&impl ResourceGroup> {
        self.query_engine.root()
    }
}

impl ResourceCollection for DuckDbModel {
    fn resources(&self) -> impl Iterator<Item = &dyn Resource> {
        self.runtime_resources
            .resources()
            .chain(self.query_engine.resources())
    }

    fn resource_groups(&self) -> impl Iterator<Item = &dyn ResourceGroup> {
        self.runtime_resources
            .resource_groups()
            .chain(self.query_engine.resource_groups())
    }

    fn resource(&self, resource_id: Uuid) -> AnalyzerResult<&dyn Resource> {
        self.runtime_resources
            .resource(resource_id)
            .or_else(|_| self.query_engine.resource(resource_id))
    }

    fn resource_type(&self, resource_type_name: &str) -> AnalyzerResult<&ResourceTypeDecl> {
        self.runtime_resources
            .resource_type(resource_type_name)
            .or_else(|_| self.query_engine.resource_type(resource_type_name))
    }

    fn resource_group(&self, resource_group_id: Uuid) -> AnalyzerResult<&dyn ResourceGroup> {
        self.query_engine
            .resource_group(resource_group_id)
            .or_else(|_| self.runtime_resources.resource_group(resource_group_id))
    }

    fn resource_group_child_groups(
        &self,
        resource_group_id: Uuid,
    ) -> AnalyzerResult<impl Iterator<Item = Uuid>> {
        self.resource_group(resource_group_id)?;

        let query_engine = self
            .query_engine
            .resource_group_child_groups(resource_group_id)
            .ok();
        let runtime = self
            .runtime_resources
            .resource_groups
            .values()
            .filter_map(move |group| {
                (group.parent_group_id == Some(resource_group_id)).then_some(group.id)
            });
        Ok(query_engine.into_iter().flatten().chain(runtime))
    }

    fn resource_group_child_resources(
        &self,
        resource_group_id: Uuid,
    ) -> AnalyzerResult<impl Iterator<Item = Uuid>> {
        self.resource_group(resource_group_id)?;

        let query_engine = self
            .query_engine
            .resource_group_child_resources(resource_group_id)
            .ok();
        let runtime = self
            .runtime_resources
            .resources
            .values()
            .filter_map(move |resource| {
                (resource.parent_group_id() == resource_group_id).then_some(resource.id)
            });
        Ok(query_engine.into_iter().flatten().chain(runtime))
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
        self.query_engine.engine()
    }

    fn query(&self, query_id: Uuid) -> AnalyzerResult<&Query> {
        self.query_engine.query(query_id)
    }

    fn query_group(&self, query_group_id: Uuid) -> AnalyzerResult<&QueryGroup> {
        self.query_engine.query_group(query_group_id)
    }

    fn worker(&self, worker_id: Uuid) -> AnalyzerResult<&Worker> {
        self.query_engine.worker(worker_id)
    }

    fn plan(&self, plan_id: Uuid) -> AnalyzerResult<&Plan> {
        self.query_engine.plan(plan_id)
    }

    fn operator(&self, operator_id: Uuid) -> AnalyzerResult<&Operator> {
        self.query_engine.operator(operator_id)
    }

    fn port(&self, port_id: Uuid) -> AnalyzerResult<&Port> {
        self.query_engine.port(port_id)
    }

    fn queries(&self) -> impl Iterator<Item = &Query> {
        self.query_engine.queries()
    }

    fn query_groups(&self) -> impl Iterator<Item = &QueryGroup> {
        self.query_engine.query_groups()
    }

    fn workers(&self) -> impl Iterator<Item = &Worker> {
        self.query_engine.workers()
    }

    fn plans(&self) -> impl Iterator<Item = &Plan> {
        self.query_engine.plans()
    }

    fn operators(&self) -> impl Iterator<Item = &Operator> {
        self.query_engine.operators()
    }

    fn ports(&self) -> impl Iterator<Item = &Port> {
        self.query_engine.ports()
    }

    fn plan_tree(&self, query_id: Uuid) -> AnalyzerResult<PlanTree> {
        self.query_engine.plan_tree(query_id)
    }
}

pub(crate) struct DuckDbModelBuilder {
    engine_id: Uuid,
    query_engine: InMemoryQueryEngineModelBuilder,
    runtime_resources: InMemoryResourcesBuilder,
    pipeline_tasks: HashMap<Uuid, PipelineTaskBuilder>,
    chunk_transfers: HashMap<Uuid, ChunkTransferBuilder>,
    operator_invocations: HashMap<Uuid, OperatorInvocationBuilder>,
    temporary_block_ios: HashMap<Uuid, TemporaryBlockIoBuilder>,
    memory_accounts: HashMap<Uuid, MemoryAccountBuilder>,
    active_memory_accounts: HashSet<Uuid>,
    initializing_runtime_resources: HashSet<Uuid>,
    active_runtime_resources: HashSet<Uuid>,
    finalizing_runtime_resources: HashSet<Uuid>,
    engine_active: bool,
    active_workers: HashSet<Uuid>,
    latest_event_timestamp: u64,
}

impl DuckDbModelBuilder {
    pub(crate) fn try_new(engine_id: Uuid) -> AnalyzerResult<Self> {
        Ok(Self {
            engine_id,
            query_engine: InMemoryQueryEngineModelBuilder::try_new(engine_id)?,
            runtime_resources: InMemoryResourcesBuilder::default(),
            pipeline_tasks: HashMap::new(),
            chunk_transfers: HashMap::new(),
            operator_invocations: HashMap::new(),
            temporary_block_ios: HashMap::new(),
            memory_accounts: HashMap::new(),
            active_memory_accounts: HashSet::new(),
            initializing_runtime_resources: HashSet::new(),
            active_runtime_resources: HashSet::new(),
            finalizing_runtime_resources: HashSet::new(),
            engine_active: false,
            active_workers: HashSet::new(),
            latest_event_timestamp: 0,
        })
    }

    pub(crate) fn try_push(&mut self, event: Event<DuckDBEvent>) -> AnalyzerResult<()> {
        let Event {
            id,
            timestamp,
            data,
        } = event;
        self.latest_event_timestamp = self.latest_event_timestamp.max(timestamp);
        match data {
            DuckDBEvent::PipelineTask(event) => {
                let builder = self
                    .pipeline_tasks
                    .entry(id)
                    .or_insert(PipelineTaskBuilder::try_new(id)?);
                builder.push(Event::new(id, timestamp, event));
                Ok(())
            }
            DuckDBEvent::ChunkTransfer(event) => {
                let builder = self
                    .chunk_transfers
                    .entry(id)
                    .or_insert(ChunkTransferBuilder::try_new(id)?);
                builder.push(Event::new(id, timestamp, event));
                Ok(())
            }
            DuckDBEvent::OperatorInvocation(event) => {
                let builder = self
                    .operator_invocations
                    .entry(id)
                    .or_insert(OperatorInvocationBuilder::try_new(id)?);
                builder.push(Event::new(id, timestamp, event));
                Ok(())
            }
            DuckDBEvent::TemporaryBlockIo(event) => {
                let builder = self
                    .temporary_block_ios
                    .entry(id)
                    .or_insert(TemporaryBlockIoBuilder::try_new(id)?);
                builder.push(Event::new(id, timestamp, event));
                Ok(())
            }
            DuckDBEvent::MemoryAccount(event) => {
                let is_exit = matches!(&event.state, memory_account::MemoryAccountTransition::Exit);
                let builder = self
                    .memory_accounts
                    .entry(id)
                    .or_insert(MemoryAccountBuilder::try_new(id)?);
                builder.push(Event::new(id, timestamp, event));
                if is_exit {
                    self.active_memory_accounts.remove(&id);
                } else {
                    self.active_memory_accounts.insert(id);
                }
                Ok(())
            }
            DuckDBEvent::ExecutionThread(event) => self.push_execution_thread(id, timestamp, event),
            DuckDBEvent::TaskQueue(event) => self.push_task_queue(id, timestamp, event),
            DuckDBEvent::TemporaryIoChannel(event) => {
                self.push_temporary_io_channel(id, timestamp, event)
            }
            DuckDBEvent::BufferPoolMemory(event) => {
                self.push_buffer_pool_memory(id, timestamp, event)
            }
            DuckDBEvent::TemporaryStorage(event) => {
                self.push_temporary_storage(id, timestamp, event)
            }
            DuckDBEvent::TemporaryDirectoryStorage(event) => {
                self.push_temporary_directory_storage(id, timestamp, event)
            }
            DuckDBEvent::Engine(event) => {
                self.push_query_engine(id, timestamp, QueryEngineEvent::Engine(event))
            }
            DuckDBEvent::Worker(event) => {
                self.push_query_engine(id, timestamp, QueryEngineEvent::Worker(event))
            }
            DuckDBEvent::QueryGroup(event) => {
                self.push_query_engine(id, timestamp, QueryEngineEvent::QueryGroup(event))
            }
            DuckDBEvent::Query(event) => {
                self.push_query_engine(id, timestamp, QueryEngineEvent::Query(event))
            }
            DuckDBEvent::Plan(event) => {
                self.push_query_engine(id, timestamp, QueryEngineEvent::Plan(event))
            }
            DuckDBEvent::Operator(event) => {
                self.push_query_engine(id, timestamp, QueryEngineEvent::Operator(event))
            }
            DuckDBEvent::Port(event) => {
                self.push_query_engine(id, timestamp, QueryEngineEvent::Port(event))
            }
        }
    }

    fn push_query_engine(
        &mut self,
        id: Uuid,
        timestamp: u64,
        event: QueryEngineEvent,
    ) -> AnalyzerResult<()> {
        match &event {
            QueryEngineEvent::Engine(engine::EngineEvent::Init(_)) => {
                self.engine_active = true;
            }
            QueryEngineEvent::Engine(engine::EngineEvent::Exit(_)) => {
                self.engine_active = false;
            }
            QueryEngineEvent::Worker(worker::WorkerEvent::Init(_)) => {
                self.active_workers.insert(id);
            }
            QueryEngineEvent::Worker(worker::WorkerEvent::Exit(_)) => {
                self.active_workers.remove(&id);
            }
            _ => {}
        }
        self.query_engine.try_push(Event::new(id, timestamp, event))
    }

    fn push_execution_thread(
        &mut self,
        id: Uuid,
        timestamp: u64,
        event: runtime_resource::ExecutionThreadEvent,
    ) -> AnalyzerResult<()> {
        use runtime_resource::ExecutionThreadTransition;

        let builder = self.runtime_resources.try_builder(id)?;
        match event.state {
            ExecutionThreadTransition::ExecutionThreadInitializing(init) => {
                validate_resource_type(&init.resource_type_name, EXECUTION_THREAD_TYPE_NAME, id)?;
                builder.push(RtResourceTransition::Init(timestamp));
                builder.set_type_name(init.resource_type_name);
                builder.set_instance_name(Some(init.instance_name));
                builder.set_parent_group_id(init.parent_group_id);
                self.initializing_runtime_resources.insert(id);
            }
            ExecutionThreadTransition::ExecutionThreadOperating(_) => {
                builder.push(RtResourceTransition::Operating(
                    timestamp,
                    ResourceCapacities(vec![]),
                ));
                self.initializing_runtime_resources.remove(&id);
                self.active_runtime_resources.insert(id);
                self.finalizing_runtime_resources.remove(&id);
            }
            ExecutionThreadTransition::ExecutionThreadFinalizing(_) => {
                builder.push(RtResourceTransition::Finalizing(timestamp));
                self.initializing_runtime_resources.remove(&id);
                self.finalizing_runtime_resources.insert(id);
            }
            ExecutionThreadTransition::Exit => {
                builder.push(RtResourceTransition::Exit(timestamp));
                self.initializing_runtime_resources.remove(&id);
                self.active_runtime_resources.remove(&id);
                self.finalizing_runtime_resources.remove(&id);
            }
        }
        Ok(())
    }

    fn push_task_queue(
        &mut self,
        id: Uuid,
        timestamp: u64,
        event: runtime_resource::TaskQueueEvent,
    ) -> AnalyzerResult<()> {
        use runtime_resource::TaskQueueTransition;

        let builder = self.runtime_resources.try_builder(id)?;
        match event.state {
            TaskQueueTransition::TaskQueueInitializing(init) => {
                validate_resource_type(&init.resource_type_name, TASK_QUEUE_TYPE_NAME, id)?;
                builder.push(RtResourceTransition::Init(timestamp));
                builder.set_type_name(init.resource_type_name);
                builder.set_instance_name(Some(init.instance_name));
                builder.set_parent_group_id(init.parent_group_id);
                self.initializing_runtime_resources.insert(id);
            }
            TaskQueueTransition::TaskQueueOperating(operating) => {
                builder.push(RtResourceTransition::Operating(
                    timestamp,
                    ResourceCapacities(vec![CapacityValue::new(
                        QUEUE_ENTRIES_CAPACITY_NAME,
                        operating.capacity_entries.value.unwrap_or(0),
                    )]),
                ));
                self.initializing_runtime_resources.remove(&id);
                self.active_runtime_resources.insert(id);
                self.finalizing_runtime_resources.remove(&id);
            }
            TaskQueueTransition::TaskQueueFinalizing(_) => {
                builder.push(RtResourceTransition::Finalizing(timestamp));
                self.initializing_runtime_resources.remove(&id);
                self.finalizing_runtime_resources.insert(id);
            }
            TaskQueueTransition::Exit => {
                builder.push(RtResourceTransition::Exit(timestamp));
                self.initializing_runtime_resources.remove(&id);
                self.active_runtime_resources.remove(&id);
                self.finalizing_runtime_resources.remove(&id);
            }
        }
        Ok(())
    }

    fn push_temporary_io_channel(
        &mut self,
        id: Uuid,
        timestamp: u64,
        event: runtime_resource::TemporaryIoChannelEvent,
    ) -> AnalyzerResult<()> {
        use runtime_resource::TemporaryIoChannelTransition;

        let builder = self.runtime_resources.try_builder(id)?;
        match event.state {
            TemporaryIoChannelTransition::TemporaryIoChannelInitializing(init) => {
                validate_resource_type(
                    &init.resource_type_name,
                    TEMPORARY_IO_CHANNEL_TYPE_NAME,
                    id,
                )?;
                builder.push(RtResourceTransition::Init(timestamp));
                builder.set_type_name(init.resource_type_name);
                builder.set_instance_name(Some(init.instance_name));
                builder.set_parent_group_id(init.parent_group_id);
                self.initializing_runtime_resources.insert(id);
            }
            TemporaryIoChannelTransition::TemporaryIoChannelOperating(operating) => {
                builder.push(RtResourceTransition::Operating(
                    timestamp,
                    ResourceCapacities(vec![
                        CapacityValue::new(
                            IO_OPERATIONS_CAPACITY_NAME,
                            operating.capacity_operations.value.unwrap_or(0),
                        ),
                        CapacityValue::new(
                            IO_BUFFER_BYTES_CAPACITY_NAME,
                            operating.capacity_buffer_bytes.value.unwrap_or(0),
                        ),
                    ]),
                ));
                self.initializing_runtime_resources.remove(&id);
                self.active_runtime_resources.insert(id);
                self.finalizing_runtime_resources.remove(&id);
            }
            TemporaryIoChannelTransition::TemporaryIoChannelFinalizing(_) => {
                builder.push(RtResourceTransition::Finalizing(timestamp));
                self.initializing_runtime_resources.remove(&id);
                self.finalizing_runtime_resources.insert(id);
            }
            TemporaryIoChannelTransition::Exit => {
                builder.push(RtResourceTransition::Exit(timestamp));
                self.initializing_runtime_resources.remove(&id);
                self.active_runtime_resources.remove(&id);
                self.finalizing_runtime_resources.remove(&id);
            }
        }
        Ok(())
    }

    fn push_buffer_pool_memory(
        &mut self,
        id: Uuid,
        timestamp: u64,
        event: memory_account::BufferPoolMemoryEvent,
    ) -> AnalyzerResult<()> {
        use memory_account::BufferPoolMemoryTransition as Transition;

        let event = match event.state {
            Transition::BufferPoolMemoryInitializing(state) => MemoryResourceEvent::Initializing {
                instance_name: state.instance_name,
                parent_group_id: state.parent_group_id,
                resource_type_name: state.resource_type_name,
            },
            Transition::BufferPoolMemoryOperating(state) => {
                MemoryResourceEvent::Operating(state.capacity_bytes.value)
            }
            Transition::BufferPoolMemoryResizing(_) => MemoryResourceEvent::Resizing,
            Transition::BufferPoolMemoryFinalizing(_) => MemoryResourceEvent::Finalizing,
            Transition::Exit => MemoryResourceEvent::Exit,
        };
        self.push_memory_resource(id, timestamp, event, BUFFER_POOL_MEMORY_TYPE_NAME)
    }

    fn push_temporary_storage(
        &mut self,
        id: Uuid,
        timestamp: u64,
        event: memory_account::TemporaryStorageEvent,
    ) -> AnalyzerResult<()> {
        use memory_account::TemporaryStorageTransition as Transition;

        let event = match event.state {
            Transition::TemporaryStorageInitializing(state) => MemoryResourceEvent::Initializing {
                instance_name: state.instance_name,
                parent_group_id: state.parent_group_id,
                resource_type_name: state.resource_type_name,
            },
            Transition::TemporaryStorageOperating(state) => {
                MemoryResourceEvent::Operating(state.capacity_bytes.value)
            }
            Transition::TemporaryStorageResizing(_) => MemoryResourceEvent::Resizing,
            Transition::TemporaryStorageFinalizing(_) => MemoryResourceEvent::Finalizing,
            Transition::Exit => MemoryResourceEvent::Exit,
        };
        self.push_memory_resource(id, timestamp, event, TEMPORARY_STORAGE_TYPE_NAME)
    }

    fn push_temporary_directory_storage(
        &mut self,
        id: Uuid,
        timestamp: u64,
        event: memory_account::TemporaryDirectoryStorageEvent,
    ) -> AnalyzerResult<()> {
        use memory_account::TemporaryDirectoryStorageTransition as Transition;

        let event = match event.state {
            Transition::TemporaryDirectoryStorageInitializing(state) => {
                MemoryResourceEvent::Initializing {
                    instance_name: state.instance_name,
                    parent_group_id: state.parent_group_id,
                    resource_type_name: state.resource_type_name,
                }
            }
            Transition::TemporaryDirectoryStorageOperating(state) => {
                MemoryResourceEvent::Operating(state.capacity_bytes.value)
            }
            Transition::TemporaryDirectoryStorageResizing(_) => MemoryResourceEvent::Resizing,
            Transition::TemporaryDirectoryStorageFinalizing(_) => MemoryResourceEvent::Finalizing,
            Transition::Exit => MemoryResourceEvent::Exit,
        };
        self.push_memory_resource(id, timestamp, event, TEMPORARY_DIRECTORY_STORAGE_TYPE_NAME)
    }

    fn push_memory_resource(
        &mut self,
        id: Uuid,
        timestamp: u64,
        event: MemoryResourceEvent,
        expected_type: &'static str,
    ) -> AnalyzerResult<()> {
        let builder = self.runtime_resources.try_builder(id)?;
        match event {
            MemoryResourceEvent::Initializing {
                instance_name,
                parent_group_id,
                resource_type_name,
            } => {
                validate_resource_type(&resource_type_name, expected_type, id)?;
                builder.push(RtResourceTransition::Init(timestamp));
                builder.set_type_name(resource_type_name);
                builder.set_instance_name(Some(instance_name));
                builder.set_parent_group_id(parent_group_id);
                self.initializing_runtime_resources.insert(id);
            }
            MemoryResourceEvent::Operating(bytes) => {
                let capacity = bytes.map_or_else(
                    || CapacityValue::new_null(MEMORY_BYTES_CAPACITY_NAME),
                    |bytes| CapacityValue::new(MEMORY_BYTES_CAPACITY_NAME, bytes),
                );
                builder.push(RtResourceTransition::Operating(
                    timestamp,
                    ResourceCapacities(vec![capacity]),
                ));
                self.initializing_runtime_resources.remove(&id);
                self.active_runtime_resources.insert(id);
                self.finalizing_runtime_resources.remove(&id);
            }
            MemoryResourceEvent::Resizing => {
                builder.push(RtResourceTransition::Resizing(timestamp));
                self.initializing_runtime_resources.remove(&id);
            }
            MemoryResourceEvent::Finalizing => {
                builder.push(RtResourceTransition::Finalizing(timestamp));
                self.initializing_runtime_resources.remove(&id);
                self.finalizing_runtime_resources.insert(id);
            }
            MemoryResourceEvent::Exit => {
                builder.push(RtResourceTransition::Exit(timestamp));
                self.initializing_runtime_resources.remove(&id);
                self.active_runtime_resources.remove(&id);
                self.finalizing_runtime_resources.remove(&id);
            }
        }
        Ok(())
    }

    pub(crate) fn try_build(mut self) -> AnalyzerResult<DuckDbModel> {
        // Engine and worker lifetimes outlive completed queries in collector
        // captures. Close them only in this immutable analyzer snapshot so
        // the server's timeline cache has a finite engine span.
        for worker_id in self.active_workers.drain() {
            self.query_engine.try_push(Event::new(
                worker_id,
                self.latest_event_timestamp,
                QueryEngineEvent::Worker(worker::WorkerEvent::Exit(worker::Exit)),
            ))?;
        }
        if self.engine_active {
            self.query_engine.try_push(Event::new(
                self.engine_id,
                self.latest_event_timestamp,
                QueryEngineEvent::Engine(engine::EngineEvent::Exit(engine::Exit)),
            ))?;
        }

        for id in self.active_memory_accounts.drain() {
            let builder = self
                .memory_accounts
                .get_mut(&id)
                .ok_or(AnalyzerError::InvalidId(id))?;
            builder.push(Event::new(
                id,
                self.latest_event_timestamp,
                FsmEvent {
                    seq: 0,
                    state: memory_account::MemoryAccountTransition::Exit,
                },
            ));
        }

        // Preserve an initializing resource with unknown capacity when a live
        // snapshot lands between its Init and Operating events.
        for id in self.initializing_runtime_resources.drain() {
            let builder = self.runtime_resources.try_builder(id)?;
            builder.push(RtResourceTransition::Operating(
                self.latest_event_timestamp,
                ResourceCapacities(vec![]),
            ));
            builder.push(RtResourceTransition::Finalizing(
                self.latest_event_timestamp,
            ));
            builder.push(RtResourceTransition::Exit(self.latest_event_timestamp));
        }

        // Runtime resources are engine-lived, while the analyzer may snapshot a
        // completed query before the DuckDB instance exits. Close resources that
        // reached Operating; this is analysis-local and does not emit events.
        for id in self.active_runtime_resources.drain() {
            let builder = self.runtime_resources.try_builder(id)?;
            if !self.finalizing_runtime_resources.contains(&id) {
                builder.push(RtResourceTransition::Finalizing(
                    self.latest_event_timestamp,
                ));
            }
            builder.push(RtResourceTransition::Exit(self.latest_event_timestamp));
        }
        let mut runtime_resources = self.runtime_resources.try_build()?;
        insert_resource_types(&mut runtime_resources);

        let pipeline_tasks = self
            .pipeline_tasks
            .into_iter()
            .map(|(id, builder)| builder.try_build().map(|task| (id, task)))
            .collect::<AnalyzerResult<HashMap<_, _>>>()?;
        let chunk_transfers = self
            .chunk_transfers
            .into_iter()
            .map(|(id, builder)| builder.try_build().map(|transfer| (id, transfer)))
            .collect::<AnalyzerResult<HashMap<_, _>>>()?;
        let operator_invocations = self
            .operator_invocations
            .into_iter()
            .map(|(id, builder)| builder.try_build().map(|invocation| (id, invocation)))
            .collect::<AnalyzerResult<HashMap<_, _>>>()?;
        let temporary_block_ios = self
            .temporary_block_ios
            .into_iter()
            .map(|(id, builder)| builder.try_build().map(|io| (id, io)))
            .collect::<AnalyzerResult<HashMap<_, _>>>()?;
        let memory_accounts = self
            .memory_accounts
            .into_iter()
            .map(|(id, builder)| builder.try_build().map(|account| (id, account)))
            .collect::<AnalyzerResult<HashMap<_, _>>>()?;

        let mut query_engine = self.query_engine.try_build()?;
        let valid_tasks: HashMap<Uuid, HashSet<Uuid>> = pipeline_tasks
            .iter()
            .filter(|(_, task)| task_is_valid(task, &query_engine, &runtime_resources))
            .filter_map(|(id, task)| {
                task.operator_ids()
                    .map(|operator_ids| (*id, operator_ids.iter().copied().collect()))
            })
            .collect();
        let valid_task_plans = pipeline_tasks
            .iter()
            .filter(|(id, _)| valid_tasks.contains_key(id))
            .filter_map(|(id, task)| task_plan_id(task).map(|plan_id| (*id, plan_id)))
            .collect::<HashMap<_, _>>();
        let valid_invocations: Vec<&OperatorInvocation> = operator_invocations
            .values()
            .filter(|invocation| {
                invocation_is_valid(invocation, &valid_tasks, &query_engine, &runtime_resources)
            })
            .collect();
        let valid_temporary_ios = temporary_block_ios
            .values()
            .filter(|io| {
                temporary_io_is_valid(
                    io,
                    &valid_task_plans,
                    &valid_tasks,
                    &query_engine,
                    &runtime_resources,
                )
            })
            .collect::<Vec<_>>();
        let valid_memory_accounts = memory_accounts
            .values()
            .filter(|account| {
                account.is_complete() && account.is_valid(self.engine_id, &runtime_resources)
            })
            .collect::<Vec<_>>();

        for entity in pipeline_tasks
            .iter()
            .filter(|(id, _)| valid_tasks.contains_key(id))
            .map(|(_, entity)| entity as &dyn EntityUsing)
            .chain(
                valid_invocations
                    .iter()
                    .map(|entity| *entity as &dyn EntityUsing),
            )
            .chain(
                valid_temporary_ios
                    .iter()
                    .map(|entity| *entity as &dyn EntityUsing),
            )
            .chain(
                valid_memory_accounts
                    .iter()
                    .map(|entity| *entity as &dyn EntityUsing),
            )
        {
            entity.populate_used_by(&mut runtime_resources)?;
        }

        for invocation in valid_invocations {
            if let (Some(operator_id), Some(span)) =
                (invocation.operator_id(), invocation.active_span())
                && let Some(operator) = query_engine.operators.get_mut(&operator_id)
            {
                operator.extend_active_span(span);
            }
        }

        let model = DuckDbModel {
            query_engine,
            runtime_resources,
            pipeline_tasks,
            chunk_transfers,
            operator_invocations,
            temporary_block_ios,
            memory_accounts,
            resource_group_types: HashMap::new(),
        };
        let mut resource_group_types = derive_resource_group_types(&model)?;
        for group_type in resource_group_types.values_mut() {
            for resource_type_name in &group_type.contains_resource_types {
                let resource_type = model.runtime_resources.resource_type(resource_type_name)?;
                group_type
                    .used_by_entity_types
                    .extend(resource_type.used_by.iter().cloned());
            }
        }

        Ok(DuckDbModel {
            resource_group_types: resource_group_types.into_iter().collect(),
            ..model
        })
    }
}

enum MemoryResourceEvent {
    Initializing {
        instance_name: String,
        parent_group_id: Uuid,
        resource_type_name: String,
    },
    Operating(Option<u64>),
    Resizing,
    Finalizing,
    Exit,
}

struct IntegrityTopology {
    plan_ids: HashSet<Uuid>,
    plan_workers: HashMap<Uuid, Uuid>,
    operator_plans: HashMap<Uuid, Uuid>,
}

fn integrity_topology(
    query_engine: &InMemoryQueryEngineModel,
    query_id: Uuid,
) -> Option<IntegrityTopology> {
    let view = query_engine.query_view(query_id).ok()?;
    let worker_ids: HashSet<Uuid> = view.workers().map(|worker| worker.id()).collect();
    Some(IntegrityTopology {
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
    })
}

fn task_is_valid(
    task: &PipelineTask,
    query_engine: &InMemoryQueryEngineModel,
    resources: &InMemoryResources,
) -> bool {
    let Some(topology) = task
        .query_id()
        .and_then(|query_id| integrity_topology(query_engine, query_id))
    else {
        return false;
    };
    task.is_complete()
        && task.belongs_to_query(
            &topology.operator_plans,
            &topology.plan_workers,
            &topology.plan_ids,
        )
        && task.resources_are_valid(resources)
}

fn invocation_is_valid(
    invocation: &OperatorInvocation,
    valid_tasks: &HashMap<Uuid, HashSet<Uuid>>,
    query_engine: &InMemoryQueryEngineModel,
    resources: &InMemoryResources,
) -> bool {
    let Some(topology) = invocation
        .query_id()
        .and_then(|query_id| integrity_topology(query_engine, query_id))
    else {
        return false;
    };
    invocation.is_complete()
        && invocation.belongs_to_query(
            &topology.operator_plans,
            &topology.plan_workers,
            &topology.plan_ids,
            valid_tasks,
        )
        && invocation.resources_are_valid(&topology.plan_workers, resources)
}

fn temporary_io_is_valid(
    io: &TemporaryBlockIo,
    task_plans: &HashMap<Uuid, Uuid>,
    task_operators: &HashMap<Uuid, HashSet<Uuid>>,
    query_engine: &InMemoryQueryEngineModel,
    resources: &InMemoryResources,
) -> bool {
    let Some(topology) = io
        .query_id()
        .and_then(|query_id| integrity_topology(query_engine, query_id))
    else {
        return false;
    };
    io.is_complete()
        && io.belongs_to_query(
            &topology.operator_plans,
            &topology.plan_workers,
            &topology.plan_ids,
            task_plans,
            task_operators,
        )
        && io.resources_are_valid(&topology.plan_workers, resources)
}

trait EntityUsing: Entity {
    fn populate_used_by(&self, resources: &mut InMemoryResources) -> AnalyzerResult<()>;
}

fn populate_used_by<'a>(
    entity: &impl Entity,
    usages: impl Iterator<Item = impl Usage<'a>>,
    resources: &mut InMemoryResources,
) -> AnalyzerResult<()> {
    for usage in usages {
        let resource_type_name = resources
            .resource(usage.resource_id())?
            .type_name()
            .to_owned();
        resources
            .resource_types
            .get_mut(&resource_type_name)
            .ok_or_else(|| AnalyzerError::InvalidTypeName(resource_type_name.clone()))?
            .used_by
            .insert(entity.type_name().to_owned());
    }
    Ok(())
}

impl EntityUsing for PipelineTask {
    fn populate_used_by(&self, resources: &mut InMemoryResources) -> AnalyzerResult<()> {
        populate_used_by(self, self.usages(), resources)
    }
}

impl EntityUsing for OperatorInvocation {
    fn populate_used_by(&self, resources: &mut InMemoryResources) -> AnalyzerResult<()> {
        populate_used_by(self, self.usages(), resources)
    }
}

impl EntityUsing for TemporaryBlockIo {
    fn populate_used_by(&self, resources: &mut InMemoryResources) -> AnalyzerResult<()> {
        populate_used_by(self, self.usages(), resources)
    }
}

impl EntityUsing for MemoryAccount {
    fn populate_used_by(&self, resources: &mut InMemoryResources) -> AnalyzerResult<()> {
        populate_used_by(self, self.usages(), resources)
    }
}
