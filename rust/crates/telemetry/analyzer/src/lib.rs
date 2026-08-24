use std::collections::HashSet;

use duckdb_telemetry_model::{DuckDB, DuckDBEvent};
use quent_analyzer::{AnalyzerError, AnalyzerResult, Entity, Span};
use quent_events::Event;
pub use quent_query_engine_analyzer::QueryEngineModel;
use quent_query_engine_analyzer::plain::legacy::{
    InMemoryQueryEngineModel, InMemoryQueryEngineModelBuilder,
};
use quent_query_engine_analyzer::ui::{QuentViewer, UiAnalyzer, ViewerEventStream};
use quent_query_engine_analyzer::{
    EngineEntity, OperatorEntity, PlanEntity, PortEntity, QueryEntity, QueryGroupEntity,
    WorkerEntity,
};
use quent_query_engine_model::QueryEngineEvent;
use quent_query_engine_ui::{OperatorFilter, QueryBundle, QueryEntities, QueryFilter};
use quent_simulator_ui::EntityRef;
use quent_time::to_secs;
use quent_ui::{ResourceGroupNode, ResourceTree};
use quent_ui::{
    entities::{request::EntityListRequest, response::EntityListResponse},
    timeline::{
        request::{BulkTimelineRequest, SingleTimelineRequest},
        response::{BulkTimelinesResponse, SingleTimelineResponse},
    },
};
use uuid::Uuid;

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
    pub model: InMemoryQueryEngineModel,
}

fn push_event(
    builder: &mut InMemoryQueryEngineModelBuilder,
    event: Event<DuckDBEvent>,
) -> AnalyzerResult<()> {
    let Event {
        id,
        timestamp,
        data,
    } = event;
    let data = match data {
        DuckDBEvent::Engine(event) => QueryEngineEvent::Engine(event),
        DuckDBEvent::Worker(event) => QueryEngineEvent::Worker(event),
        DuckDBEvent::QueryGroup(event) => QueryEngineEvent::QueryGroup(event),
        DuckDBEvent::Query(event) => QueryEngineEvent::Query(event),
        DuckDBEvent::Plan(event) => QueryEngineEvent::Plan(event),
        DuckDBEvent::Operator(event) => QueryEngineEvent::Operator(event),
        DuckDBEvent::Port(event) => QueryEngineEvent::Port(event),
    };
    builder.try_push(Event::new(id, timestamp, data))
}

impl UiAnalyzer for DuckDbUiAnalyzer {
    type Event = DuckDBEvent;
    type EntityRef = EntityRef;

    fn try_new(
        engine_id: Uuid,
        events: impl Iterator<Item = Event<DuckDBEvent>>,
    ) -> AnalyzerResult<Self> {
        let mut builder = InMemoryQueryEngineModelBuilder::try_new(engine_id)?;
        for event in events {
            push_event(&mut builder, event)?;
        }
        let model = builder.try_build()?;
        tracing::info!(
            workers = model.workers.len(),
            query_groups = model.query_groups.len(),
            queries = model.queries.len(),
            plans = model.plans.len(),
            operators = model.operators.len(),
            ports = model.ports.len(),
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
        let view = self.model.query_view(query_id)?;
        let model_query = view.query(query_id)?;
        let epoch = view.query_epoch(query_id)?;
        let duration_s = to_secs(model_query.span()?.duration());

        let engine = view.engine()?.to_ui()?;
        let query_group_id = model_query.query_group_id().ok_or_else(|| {
            AnalyzerError::IncompleteEntity(format!("query {query_id} has no query group"))
        })?;
        let entities = QueryEntities {
            engine,
            query_group: view.query_group(query_group_id)?.to_ui(),
            query: model_query.to_ui()?,
            workers: view.workers().map(|v| (v.id(), v.to_ui(epoch))).collect(),
            plans: view.plans().map(|v| (v.id(), v.to_ui())).collect(),
            operators: view.operators().map(|v| (v.id(), v.to_ui(epoch))).collect(),
            ports: view.ports().map(|v| (v.id(), v.to_ui(epoch))).collect(),
            resource_types: Default::default(),
            resource_group_types: Default::default(),
            resources: Default::default(),
            resource_groups: Default::default(),
            fsm_types: Default::default(),
        };
        let unique_operator_names = view
            .operators()
            .filter_map(|operator| operator.operator_type_name().map(str::to_owned))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let resource_tree = ResourceTree::ResourceGroup(ResourceGroupNode {
            id: EntityRef::Engine(view.engine()?.id()),
            children: vec![],
        });

        Ok(QueryBundle {
            query_id,
            entities,
            plan_tree: view.plan_tree(query_id)?.to_ui(),
            resource_tree,
            unique_operator_names,
            quantity_specs: Default::default(),
            start_time_unix_ns: epoch,
            duration_s,
        })
    }

    fn query_engine_model(&self) -> &impl QueryEngineModel {
        &self.model
    }

    fn single_resource_timeline(
        &self,
        _request: SingleTimelineRequest<QueryFilter, OperatorFilter>,
    ) -> AnalyzerResult<SingleTimelineResponse> {
        Err(AnalyzerError::Unsupported)
    }

    fn list_entities(
        &self,
        _request: EntityListRequest<QueryFilter, OperatorFilter>,
    ) -> AnalyzerResult<EntityListResponse> {
        Err(AnalyzerError::Unsupported)
    }

    fn bulk_resource_timeline(
        &self,
        _request: BulkTimelineRequest<QueryFilter, OperatorFilter>,
    ) -> AnalyzerResult<BulkTimelinesResponse> {
        Err(AnalyzerError::Unsupported)
    }
}
