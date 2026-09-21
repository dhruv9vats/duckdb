//! Emit a connected physical plan for end-to-end validation.

use clap::Parser;
use duckdb_telemetry_model::{
    Context, DuckDb, DynamicAttributes, Edge, Engine, EngineImplementation, Operator, Plan,
    PlanParent, Port, Query, QueryGroup, Uuid, Worker,
};
use quent_io::clap::ExporterArgs;

type DuckDbContext = Context<DuckDb>;

const ENGINE: Uuid = Uuid::from_u128(1);
const WORKER: Uuid = Uuid::from_u128(2);
const QUERY_GROUP: Uuid = Uuid::from_u128(3);
const QUERY: Uuid = Uuid::from_u128(4);
const PLAN: Uuid = Uuid::from_u128(5);
const TABLE_SCAN: Uuid = Uuid::from_u128(6);
const FILTER: Uuid = Uuid::from_u128(7);
const RESULT_COLLECTOR: Uuid = Uuid::from_u128(8);
const TABLE_SCAN_OUT: Uuid = Uuid::from_u128(9);
const FILTER_IN: Uuid = Uuid::from_u128(10);
const FILTER_OUT: Uuid = Uuid::from_u128(11);
const RESULT_COLLECTOR_IN: Uuid = Uuid::from_u128(12);

#[derive(Parser, Debug)]
#[command(about = "Emit a sample DuckDB query plan as Quent telemetry")]
struct Args {
    #[command(flatten)]
    exporter: ExporterArgs,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let context = match args.exporter.into_options() {
        Some(provider) => DuckDbContext::try_new(provider)?,
        None => DuckDbContext::try_new(duckdb_telemetry_model::Noop)?,
    };

    let mut engine = context.observer::<Engine>().handle_with_id(ENGINE);
    engine.init(
        EngineImplementation {
            name: Some("DuckDB".into()),
            version: None,
            custom_attributes: DynamicAttributes::default(),
        },
        Some("duckdb-sample".into()),
    )?;

    let mut worker = context.observer::<Worker>().handle_with_id(WORKER);
    worker.init(engine.as_entity_ref(), "local".into())?;

    let mut query_group = context.observer::<QueryGroup>().handle_with_id(QUERY_GROUP);
    query_group.declaration("sample-session".into(), engine.as_entity_ref())?;

    let query = context
        .observer::<Query>()
        .handle_with_id(QUERY)
        .init(
            "SELECT * FROM t WHERE i > 0".into(),
            query_group.as_entity_ref(),
        )
        .planning();

    let plan_ref = context
        .observer::<Plan>()
        .handle_with_id(PLAN)
        .as_entity_ref();
    let mut operator_refs = Vec::new();
    for (id, instance_name, type_name) in [
        (TABLE_SCAN, "t", "TABLE_SCAN"),
        (FILTER, "i > 0", "FILTER"),
        (RESULT_COLLECTOR, "result", "RESULT_COLLECTOR"),
    ] {
        let mut operator = context.observer::<Operator>().handle_with_id(id);
        operator.declaration(
            plan_ref.clone(),
            Vec::new(),
            instance_name.into(),
            type_name.into(),
            DynamicAttributes::default(),
        )?;
        operator_refs.push(operator);
    }

    let mut ports = Vec::new();
    for (id, operator_index, instance_name) in [
        (TABLE_SCAN_OUT, 0, "out"),
        (FILTER_IN, 1, "in"),
        (FILTER_OUT, 1, "out"),
        (RESULT_COLLECTOR_IN, 2, "in"),
    ] {
        let mut port = context.observer::<Port>().handle_with_id(id);
        port.declaration(
            operator_refs[operator_index].as_entity_ref(),
            instance_name.into(),
        )?;
        ports.push(port);
    }

    let mut plan = context.observer::<Plan>().handle_with_id(PLAN);
    plan.declaration(
        PlanParent {
            query_id: query.as_entity_ref(),
            plan_id: None,
        },
        "physical".into(),
        vec![
            Edge {
                source: ports[0].as_entity_ref(),
                target: ports[1].as_entity_ref(),
            },
            Edge {
                source: ports[2].as_entity_ref(),
                target: ports[3].as_entity_ref(),
            },
        ],
        Some(worker.as_entity_ref()),
    )?;

    query.executing().exit();
    worker.exit()?;
    engine.exit()?;
    Ok(())
}
