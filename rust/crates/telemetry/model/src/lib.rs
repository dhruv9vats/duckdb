use quent_model::{instrumentation, model};

model! {
    name: DuckDB,
    root: quent_query_engine_model::engine::Engine,
    entities: {
        quent_query_engine_model::worker::Worker,
        quent_query_engine_model::query_group::QueryGroup,
        quent_query_engine_model::query::Query,
        quent_query_engine_model::plan::Plan,
        quent_query_engine_model::operator::Operator,
        quent_query_engine_model::port::Port,
    },
    analyzer: "duckdb-telemetry-analyzer",
}

instrumentation!(DuckDB);

// These re-exports give the generated bridge stable model-module paths.
pub use quent_query_engine_model::{engine, operator, plan, port, query, query_group, worker};
