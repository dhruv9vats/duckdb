use quent_model::resource;

resource! {
    /// A stable OS thread that executes DuckDB pipeline work.
    ExecutionThread
}

resource! {
    /// DuckDB pipeline tasks that are ready for scheduler dispatch.
    TaskQueue {
        capacity: { entries: Option<u64> },
    }
}
