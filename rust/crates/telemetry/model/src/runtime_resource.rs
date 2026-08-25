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

resource! {
    /// Temporary-storage I/O issued by DuckDB's buffer manager.
    TemporaryIoChannel {
        capacity: {
            rate,
            operations: Option<u64>,
            // Page-aligned FileBuffer bytes transferred.
            buffer_bytes: Option<u64>,
        },
    }
}
