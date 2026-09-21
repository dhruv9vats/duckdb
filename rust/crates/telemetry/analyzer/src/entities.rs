use duckdb_telemetry_store as schema;
use quent_analyzer::{
    AnalyzerResult, Entity, RefTreeEntity,
    entity::native::{AnalyzedEntity, EntityEventAccumulator},
    fsm::{
        Fsm, FsmUsages,
        native::{AnalyzedFsm, AnalyzedFsmBuilder, AnalyzedTransition},
    },
    resource::{Usage, Using},
};
use quent_events::Event;
use quent_query_engine_analyzer::{
    EngineEntity, OperatorEntity, OperatorEntityMut, PlanEntity, PortEntity, QueryEntity,
    QueryGroupEntity, WorkerEntity,
};
use quent_query_engine_ui as ui;
use quent_time::{TimeUnixNanoSec, Timestamp, span::SpanUnixNanoSec, try_to_secs_relative};
use uuid::Uuid;

#[derive(Default)]
struct EngineData {
    instance_name: Option<String>,
    implementation: Option<schema::EngineImplementation>,
    exited: bool,
}

impl EntityEventAccumulator for EngineData {
    type Event = schema::EngineEvent;

    fn push(&mut self, event: Self::Event) {
        match event {
            schema::EngineEvent::Init {
                implementation,
                instance_name,
            } => {
                self.instance_name = instance_name;
                self.implementation = Some(implementation);
            }
            schema::EngineEvent::Exit => self.exited = true,
        }
    }
}

#[derive(Debug)]
pub struct Engine(AnalyzedEntity<EngineData>);

impl Engine {
    pub(crate) fn try_from_event(event: Event<schema::EngineEvent>) -> AnalyzerResult<Self> {
        Ok(Self(AnalyzedEntity::try_from_event(event)?))
    }

    pub(crate) fn push(&mut self, event: Event<schema::EngineEvent>) -> AnalyzerResult<()> {
        self.0.push(event)
    }
}

impl Entity for Engine {
    fn id(&self) -> Uuid {
        self.0.id()
    }
    fn type_name(&self) -> &str {
        self.0.type_name()
    }
    fn earliest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.earliest_timestamp()
    }
    fn latest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.latest_timestamp()
    }
}

impl RefTreeEntity for Engine {
    fn parent_id(&self) -> Option<Uuid> {
        None
    }
}

impl EngineEntity for Engine {
    fn to_ui(&self) -> AnalyzerResult<ui::Engine> {
        let data = self.0.accumulator();
        let start = self.earliest_timestamp();
        let duration_s = data
            .exited
            .then(|| try_to_secs_relative(self.latest_timestamp(), start))
            .transpose()?;

        Ok(ui::Engine {
            id: self.id(),
            start_time_unix_ns: Some(start),
            duration_s,
            instance_name: data.instance_name.clone(),
            implementation: data.implementation.as_ref().map(|implementation| {
                ui::EngineImplementationAttributes {
                    name: implementation.name.clone(),
                    version: implementation.version.clone(),
                    custom_attributes: implementation.custom_attributes.0.clone(),
                }
            }),
        })
    }
}

#[derive(Default)]
struct WorkerData {
    parent_engine_id: Option<Uuid>,
    instance_name: Option<String>,
    exited: bool,
}

impl EntityEventAccumulator for WorkerData {
    type Event = schema::WorkerEvent;

    fn push(&mut self, event: Self::Event) {
        match event {
            schema::WorkerEvent::Init {
                parent_engine_id,
                instance_name,
            } => {
                self.parent_engine_id = Some(parent_engine_id.target);
                self.instance_name = Some(instance_name);
            }
            schema::WorkerEvent::Exit => self.exited = true,
        }
    }
}

#[derive(Debug)]
pub struct Worker(AnalyzedEntity<WorkerData>);

impl Worker {
    pub(crate) fn try_from_event(event: Event<schema::WorkerEvent>) -> AnalyzerResult<Self> {
        Ok(Self(AnalyzedEntity::try_from_event(event)?))
    }

    pub(crate) fn push(&mut self, event: Event<schema::WorkerEvent>) -> AnalyzerResult<()> {
        self.0.push(event)
    }
}

impl Entity for Worker {
    fn id(&self) -> Uuid {
        self.0.id()
    }
    fn type_name(&self) -> &str {
        self.0.type_name()
    }
    fn earliest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.earliest_timestamp()
    }
    fn latest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.latest_timestamp()
    }
}

impl RefTreeEntity for Worker {
    fn parent_id(&self) -> Option<Uuid> {
        self.0.accumulator().parent_engine_id
    }
}

impl WorkerEntity for Worker {
    fn to_ui(&self, _epoch: TimeUnixNanoSec) -> ui::Worker {
        let data = self.0.accumulator();
        ui::Worker {
            id: self.id(),
            parent_engine_id: data.parent_engine_id,
            instance_name: data.instance_name.clone(),
            start_unix_ns: Some(self.earliest_timestamp()),
            end_unix_ns: data.exited.then(|| self.latest_timestamp()),
        }
    }
}

#[derive(Default)]
struct QueryGroupData {
    instance_name: Option<String>,
    engine_id: Option<Uuid>,
}

impl EntityEventAccumulator for QueryGroupData {
    type Event = schema::QueryGroupEvent;

    fn push(&mut self, event: Self::Event) {
        let schema::QueryGroupEvent::Declaration {
            instance_name,
            engine_id,
        } = event;
        self.instance_name = Some(instance_name);
        self.engine_id = Some(engine_id.target);
    }
}

#[derive(Debug)]
pub struct QueryGroup(AnalyzedEntity<QueryGroupData>);

impl QueryGroup {
    pub(crate) fn try_from_event(event: Event<schema::QueryGroupEvent>) -> AnalyzerResult<Self> {
        Ok(Self(AnalyzedEntity::try_from_event(event)?))
    }

    pub(crate) fn push(&mut self, event: Event<schema::QueryGroupEvent>) -> AnalyzerResult<()> {
        self.0.push(event)
    }
}

impl Entity for QueryGroup {
    fn id(&self) -> Uuid {
        self.0.id()
    }
    fn type_name(&self) -> &str {
        self.0.type_name()
    }
    fn earliest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.earliest_timestamp()
    }
    fn latest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.latest_timestamp()
    }
}

impl RefTreeEntity for QueryGroup {
    fn parent_id(&self) -> Option<Uuid> {
        self.0.accumulator().engine_id
    }
}

impl QueryGroupEntity for QueryGroup {
    fn to_ui(&self) -> ui::QueryGroup {
        let data = self.0.accumulator();
        ui::QueryGroup {
            id: self.id(),
            instance_name: data.instance_name.clone(),
            engine_id: data.engine_id,
        }
    }
}

pub(crate) type QueryBuilder = AnalyzedFsmBuilder<schema::QueryEvent>;

#[derive(Debug)]
pub struct Query(AnalyzedFsm<schema::QueryEvent>);

impl Query {
    pub(crate) fn try_from_builder(builder: QueryBuilder) -> AnalyzerResult<Self> {
        Ok(Self(builder.try_build()?))
    }

    fn query_group(&self) -> Option<Uuid> {
        match &self.0.transition(0)?.data {
            schema::QueryEvent::Init { query_group_id, .. } => Some(query_group_id.target),
            _ => None,
        }
    }
}

impl Entity for Query {
    fn id(&self) -> Uuid {
        self.0.id()
    }
    fn type_name(&self) -> &str {
        self.0.type_name()
    }
    fn earliest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.earliest_timestamp()
    }
    fn latest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.latest_timestamp()
    }
}

impl Fsm for Query {
    type TransitionType = AnalyzedTransition<schema::QueryEvent>;
    fn len(&self) -> usize {
        self.0.len()
    }
    fn transition(&self, index: usize) -> Option<&Self::TransitionType> {
        self.0.transition(index)
    }
}

impl<'a> FsmUsages<'a> for Query {
    fn usages_with_state_names(&'a self) -> impl Iterator<Item = (&'a str, impl Usage<'a>)> {
        self.0.usages_with_state_names()
    }
}

impl Using for Query {
    fn usages(&self) -> impl Iterator<Item = impl Usage<'_>> {
        self.0.usages()
    }
}

impl RefTreeEntity for Query {
    fn parent_id(&self) -> Option<Uuid> {
        self.query_group()
    }
}

impl QueryEntity for Query {
    fn query_group_id(&self) -> Option<Uuid> {
        self.query_group()
    }

    fn to_ui(&self) -> AnalyzerResult<ui::Query> {
        let transitions = self.0.transitions();
        let epoch = transitions.first().map(Timestamp::timestamp);
        let mut planning_s = None;
        let mut executing_s = None;
        let mut completed_s = None;

        if let Some(epoch) = epoch {
            for (index, transition) in transitions.iter().enumerate() {
                match transition.data {
                    schema::QueryEvent::Planning { .. } => {
                        planning_s = Some(try_to_secs_relative(transition.timestamp(), epoch)?);
                    }
                    schema::QueryEvent::Executing { .. } => {
                        executing_s = Some(try_to_secs_relative(transition.timestamp(), epoch)?);
                        if let Some(next) = transitions.get(index + 1) {
                            completed_s = Some(try_to_secs_relative(next.timestamp(), epoch)?);
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(ui::Query {
            id: self.id(),
            query_group_id: self.query_group().unwrap_or_default(),
            instance_name: transitions
                .first()
                .and_then(|transition| match &transition.data {
                    schema::QueryEvent::Init { instance_name, .. } => Some(instance_name.clone()),
                    _ => None,
                }),
            start_unix_ns: epoch,
            planning_s,
            executing_s,
            completed_s,
        })
    }
}

#[derive(Default)]
struct PlanData {
    instance_name: Option<String>,
    query_id: Option<Uuid>,
    parent_plan_id: Option<Uuid>,
    worker_id: Option<Uuid>,
    edges: Vec<(Uuid, Uuid)>,
}

impl EntityEventAccumulator for PlanData {
    type Event = schema::PlanEvent;

    fn push(&mut self, event: Self::Event) {
        let schema::PlanEvent::Declaration {
            parent,
            instance_name,
            edges,
            worker_id,
        } = event;
        self.instance_name = Some(instance_name);
        self.query_id = Some(parent.query_id.target);
        self.parent_plan_id = parent.plan_id.map(|plan| plan.target);
        self.worker_id = worker_id.map(|worker| worker.target);
        self.edges = edges
            .into_iter()
            .map(|edge| (edge.source.target, edge.target.target))
            .collect();
    }
}

#[derive(Debug)]
pub struct Plan(AnalyzedEntity<PlanData>);

impl Plan {
    pub(crate) fn try_from_event(event: Event<schema::PlanEvent>) -> AnalyzerResult<Self> {
        Ok(Self(AnalyzedEntity::try_from_event(event)?))
    }

    pub(crate) fn push(&mut self, event: Event<schema::PlanEvent>) -> AnalyzerResult<()> {
        self.0.push(event)
    }
}

impl Entity for Plan {
    fn id(&self) -> Uuid {
        self.0.id()
    }
    fn type_name(&self) -> &str {
        self.0.type_name()
    }
    fn earliest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.earliest_timestamp()
    }
    fn latest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.latest_timestamp()
    }
}

impl RefTreeEntity for Plan {
    fn parent_id(&self) -> Option<Uuid> {
        self.0.accumulator().query_id
    }
}

impl PlanEntity for Plan {
    fn parent_query_id(&self) -> Option<Uuid> {
        let data = self.0.accumulator();
        data.parent_plan_id
            .is_none()
            .then_some(data.query_id)
            .flatten()
    }

    fn parent_plan_id(&self) -> Option<Uuid> {
        self.0.accumulator().parent_plan_id
    }
    fn worker_id(&self) -> Option<Uuid> {
        self.0.accumulator().worker_id
    }
    fn edges(&self) -> impl Iterator<Item = (Uuid, Uuid)> + '_ {
        self.0.accumulator().edges.iter().copied()
    }

    fn to_ui(&self) -> ui::Plan {
        let data = self.0.accumulator();
        ui::Plan {
            id: self.id(),
            instance_name: data.instance_name.clone(),
            parent: data.parent_plan_id.or(data.query_id),
            worker_id: data.worker_id,
            edges: data
                .edges
                .iter()
                .map(|&(source, target)| ui::Edge { source, target })
                .collect(),
        }
    }
}

#[derive(Default)]
struct OperatorData {
    plan_id: Option<Uuid>,
    parent_ids: Vec<Uuid>,
    instance_name: Option<String>,
    type_name: Option<String>,
    attributes: quent_events::DynamicAttributes,
    statistics: Option<quent_events::DynamicAttributes>,
}

impl EntityEventAccumulator for OperatorData {
    type Event = schema::OperatorEvent;

    fn push(&mut self, event: Self::Event) {
        match event {
            schema::OperatorEvent::Declaration {
                plan_id,
                parent_operator_ids,
                instance_name,
                type_name,
                custom_attributes,
            } => {
                self.plan_id = Some(plan_id.target);
                self.parent_ids = parent_operator_ids
                    .into_iter()
                    .map(|operator| operator.target)
                    .collect();
                self.instance_name = Some(instance_name);
                self.type_name = Some(type_name);
                self.attributes = custom_attributes;
            }
            schema::OperatorEvent::Statistics { custom_attributes } => {
                self.statistics = Some(custom_attributes);
            }
        }
    }
}

#[derive(Debug)]
pub struct Operator {
    entity: AnalyzedEntity<OperatorData>,
    active_span: Option<SpanUnixNanoSec>,
}

impl Operator {
    pub(crate) fn try_from_event(event: Event<schema::OperatorEvent>) -> AnalyzerResult<Self> {
        Ok(Self {
            entity: AnalyzedEntity::try_from_event(event)?,
            active_span: None,
        })
    }

    pub(crate) fn push(&mut self, event: Event<schema::OperatorEvent>) -> AnalyzerResult<()> {
        self.entity.push(event)
    }

    pub(crate) fn instance_name(&self) -> &str {
        self.entity
            .accumulator()
            .instance_name
            .as_deref()
            .unwrap_or_default()
    }
}

impl Entity for Operator {
    fn id(&self) -> Uuid {
        self.entity.id()
    }
    fn type_name(&self) -> &str {
        self.entity.type_name()
    }
    fn earliest_timestamp(&self) -> TimeUnixNanoSec {
        self.entity.earliest_timestamp()
    }
    fn latest_timestamp(&self) -> TimeUnixNanoSec {
        self.entity.latest_timestamp()
    }
}

impl RefTreeEntity for Operator {
    fn parent_id(&self) -> Option<Uuid> {
        self.entity.accumulator().plan_id
    }
}

impl OperatorEntity for Operator {
    fn plan_id(&self) -> Option<Uuid> {
        self.entity.accumulator().plan_id
    }
    fn parent_operator_ids(&self) -> impl ExactSizeIterator<Item = Uuid> + '_ {
        self.entity.accumulator().parent_ids.iter().copied()
    }
    fn active_span(&self) -> Option<SpanUnixNanoSec> {
        self.active_span
    }
    fn operator_type_name(&self) -> Option<&str> {
        self.entity.accumulator().type_name.as_deref()
    }

    fn to_ui(&self, epoch: TimeUnixNanoSec) -> ui::Operator {
        let data = self.entity.accumulator();
        ui::Operator {
            id: self.id(),
            plan_id: data.plan_id,
            parent_operator_ids: data.parent_ids.clone(),
            instance_name: data.instance_name.clone(),
            operator_type_name: data.type_name.clone(),
            custom_attributes: data
                .attributes
                .iter()
                .map(|a| (a.key.clone(), a.value.clone()))
                .collect(),
            statistics: data
                .statistics
                .as_ref()
                .map(|statistics| ui::OperatorStatistics {
                    custom_statistics: statistics
                        .iter()
                        .map(|attribute| {
                            (
                                attribute.key.clone(),
                                ui::OperatorStatistic {
                                    value: attribute.value.clone(),
                                    quantity: None,
                                },
                            )
                        })
                        .collect(),
                }),
            active_span: self
                .active_span
                .and_then(|span| span.try_to_secs_relative(epoch).ok()),
        }
    }
}

impl OperatorEntityMut for Operator {
    fn extend_active_span(&mut self, span: SpanUnixNanoSec) {
        self.active_span = Some(
            self.active_span
                .map_or(span, |current| current.extend(&span)),
        );
    }
}

#[derive(Default)]
struct PortData {
    operator_id: Option<Uuid>,
    instance_name: Option<String>,
    statistics: Option<quent_events::DynamicAttributes>,
}

impl EntityEventAccumulator for PortData {
    type Event = schema::PortEvent;

    fn push(&mut self, event: Self::Event) {
        match event {
            schema::PortEvent::Declaration {
                operator_id,
                instance_name,
            } => {
                self.operator_id = Some(operator_id.target);
                self.instance_name = Some(instance_name);
            }
            schema::PortEvent::Statistics { custom_attributes } => {
                self.statistics = Some(custom_attributes)
            }
        }
    }
}

#[derive(Debug)]
pub struct Port(AnalyzedEntity<PortData>);

impl Port {
    pub(crate) fn try_from_event(event: Event<schema::PortEvent>) -> AnalyzerResult<Self> {
        Ok(Self(AnalyzedEntity::try_from_event(event)?))
    }
    pub(crate) fn push(&mut self, event: Event<schema::PortEvent>) -> AnalyzerResult<()> {
        self.0.push(event)
    }
}

impl Entity for Port {
    fn id(&self) -> Uuid {
        self.0.id()
    }
    fn type_name(&self) -> &str {
        self.0.type_name()
    }
    fn earliest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.earliest_timestamp()
    }
    fn latest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.latest_timestamp()
    }
}

impl RefTreeEntity for Port {
    fn parent_id(&self) -> Option<Uuid> {
        self.0.accumulator().operator_id
    }
}

impl PortEntity for Port {
    fn operator_id(&self) -> Option<Uuid> {
        self.0.accumulator().operator_id
    }

    fn to_ui(&self, _epoch: TimeUnixNanoSec) -> ui::Port {
        let data = self.0.accumulator();
        ui::Port {
            id: self.id(),
            operator_id: data.operator_id,
            instance_name: data.instance_name.clone(),
            statistics: data
                .statistics
                .as_ref()
                .map(|statistics| ui::PortStatistics {
                    custom_statistics: statistics
                        .iter()
                        .map(|a| (a.key.clone(), a.value.clone()))
                        .collect(),
                }),
        }
    }
}
