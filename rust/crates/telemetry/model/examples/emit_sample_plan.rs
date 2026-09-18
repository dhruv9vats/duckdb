//! Emit a minimal connected DuckDB physical plan for end-to-end UI validation.

use clap::Parser;
use duckdb_telemetry_model::DuckDBContext;
use duckdb_telemetry_model::{engine, operator, plan, port, query_group, worker};
use quent_io::clap::ExporterArgs;
use quent_model::Ref;
use uuid::{Uuid, uuid};

const ENGINE: Uuid = uuid!("00000000-0000-0000-0000-000000000001");
const WORKER: Uuid = uuid!("00000000-0000-0000-0000-000000000002");
const QUERY_GROUP: Uuid = uuid!("00000000-0000-0000-0000-000000000003");
const QUERY: Uuid = uuid!("00000000-0000-0000-0000-000000000004");
const PLAN: Uuid = uuid!("00000000-0000-0000-0000-000000000005");

const TABLE_SCAN: Uuid = uuid!("00000000-0000-0000-0000-000000000006");
const FILTER: Uuid = uuid!("00000000-0000-0000-0000-000000000007");
const RESULT_COLLECTOR: Uuid = uuid!("00000000-0000-0000-0000-000000000008");

const TABLE_SCAN_OUT: Uuid = uuid!("00000000-0000-0000-0000-000000000009");
const FILTER_IN: Uuid = uuid!("00000000-0000-0000-0000-00000000000a");
const FILTER_OUT: Uuid = uuid!("00000000-0000-0000-0000-00000000000b");
const RESULT_COLLECTOR_IN: Uuid = uuid!("00000000-0000-0000-0000-00000000000c");

#[derive(Parser, Debug)]
#[command(about = "Emit a sample DuckDB query plan as Quent telemetry")]
struct Args {
    #[command(flatten)]
    exporter: ExporterArgs,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let ctx = match args.exporter.into_options() {
        Some(provider) => DuckDBContext::try_new(provider)?,
        None => DuckDBContext::try_new(quent_model::Noop)?,
    };

    let engine_observer = ctx.engine_observer();
    let worker_observer = ctx.worker_observer();
    let query_group_observer = ctx.query_group_observer();
    let query_observer = ctx.query_observer();
    let plan_observer = ctx.plan_observer();
    let operator_observer = ctx.operator_observer();
    let port_observer = ctx.port_observer();

    engine_observer.create(ENGINE).init(engine::Init {
        instance_name: Some("duckdb-sample".into()),
        implementation: engine::EngineImplementationAttributes {
            name: Some("DuckDB".into()),
            version: None,
            custom_attributes: Default::default(),
        },
    });
    worker_observer.create(WORKER).init(worker::Init {
        parent_engine_id: Ref::new(ENGINE),
        instance_name: "local".into(),
    });
    query_group_observer.declaration(
        QUERY_GROUP,
        query_group::Declaration {
            engine_id: ENGINE,
            instance_name: "sample-session".into(),
        },
    );

    let mut query =
        query_observer.init(QUERY, "SELECT * FROM t WHERE i > 0", Ref::new(QUERY_GROUP));
    query.planning();

    plan_observer.declaration(
        PLAN,
        plan::Declaration {
            instance_name: "physical".into(),
            parent: plan::PlanParent {
                query_id: Some(Ref::new(QUERY)),
                plan_id: None,
            },
            worker_id: Some(Ref::new(WORKER)),
            edges: vec![
                plan::Edge {
                    source: Ref::new(TABLE_SCAN_OUT),
                    target: Ref::new(FILTER_IN),
                },
                plan::Edge {
                    source: Ref::new(FILTER_OUT),
                    target: Ref::new(RESULT_COLLECTOR_IN),
                },
            ],
        },
    );

    for (id, instance_name, type_name) in [
        (TABLE_SCAN, "t", "TABLE_SCAN"),
        (FILTER, "i > 0", "FILTER"),
        (RESULT_COLLECTOR, "result", "RESULT_COLLECTOR"),
    ] {
        operator_observer
            .create(id)
            .declaration(operator::Declaration {
                plan_id: Ref::new(PLAN),
                parent_operator_ids: vec![],
                instance_name: instance_name.into(),
                type_name: type_name.into(),
                custom_attributes: Default::default(),
            });
    }

    for (id, operator_id, instance_name) in [
        (TABLE_SCAN_OUT, TABLE_SCAN, "out"),
        (FILTER_IN, FILTER, "in"),
        (FILTER_OUT, FILTER, "out"),
        (RESULT_COLLECTOR_IN, RESULT_COLLECTOR, "in"),
    ] {
        port_observer.create(id).declaration(port::Declaration {
            operator_id: Ref::new(operator_id),
            instance_name: instance_name.into(),
        });
    }

    query.executing();
    query.exit();
    worker_observer.create(WORKER).exit(worker::Exit);
    engine_observer.create(ENGINE).exit(engine::Exit);
    Ok(())
}
