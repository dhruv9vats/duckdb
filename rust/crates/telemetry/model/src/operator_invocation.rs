#![allow(clippy::too_many_arguments)]

use quent_model::{fsm, state};
use uuid::Uuid;

pub use crate::runtime_resource::ExecutionThread;

state! {
    InvocationCreated {
        attributes: {
            query_id: Uuid,
            plan_id: Uuid,
            task_id: Uuid,
            operator_id: Uuid,
            phase: String,
        },
    }
}

state! {
    InvocationRunning {
        attributes: {
            input_rows: u64,
            input_logical_bytes: u64,
        },
        usages: {
            execution_thread: ExecutionThread,
        },
    }
}

state! {
    InvocationCompleted {
        attributes: {
            success: bool,
            output_rows: u64,
            output_logical_bytes: u64,
        },
    }
}

fsm! {
    OperatorInvocation {
        states: {
            invocation_created: InvocationCreated,
            invocation_running: InvocationRunning,
            invocation_completed: InvocationCompleted,
        },
        entry: invocation_created,
        exit_from: { invocation_completed },
        transitions: {
            invocation_created => invocation_running,
            invocation_created => invocation_completed,
            invocation_running => invocation_completed,
        },
    }
}
