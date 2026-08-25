use quent_model::{instrumentation, model};

pub mod chunk_transfer;
pub mod operator_invocation;
pub mod pipeline_task;
pub mod runtime_resource;

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
        pipeline_task::PipelineTask,
        chunk_transfer::ChunkTransfer,
        operator_invocation::OperatorInvocation,
        runtime_resource::ExecutionThread,
        runtime_resource::TaskQueue,
    },
    analyzer: "duckdb-telemetry-analyzer",
}

instrumentation!(DuckDB);

// These re-exports give the generated bridge stable model-module paths.
pub use quent_query_engine_model::{engine, operator, plan, port, query, query_group, worker};
