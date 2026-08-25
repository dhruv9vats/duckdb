#![allow(clippy::too_many_arguments)]

use quent_model::{fsm, state};
use uuid::Uuid;

pub use crate::runtime_resource::{ExecutionThread, TaskQueue};

state! {
    Created {
        attributes: {
            query_id: Uuid,
            plan_id: Uuid,
            worker_id: Uuid,
            operator_ids: Vec<Uuid>,
            task_index: u64,
        },
        usages: {
            queue: TaskQueue,
        },
    }
}

state! {
    Running {
        attributes: {
            mode: String,
            cpu_id: u64,
        },
        usages: {
            execution_thread: ExecutionThread,
        },
    }
}

state! {
    Ready {
        usages: {
            queue: TaskQueue,
        },
    }
}

state! { Blocked {} }

state! {
    Finalizing {
        attributes: {
            success: bool,
        },
    }
}

fsm! {
    PipelineTask {
        states: {
            created: Created,
            running: Running,
            ready: Ready,
            blocked: Blocked,
            finalizing: Finalizing,
        },
        entry: created,
        exit_from: { finalizing },
        transitions: {
            created => running,
            created => finalizing,
            running => ready,
            running => blocked,
            running => finalizing,
            ready => running,
            ready => finalizing,
            blocked => running,
            blocked => finalizing,
        },
    }
}
