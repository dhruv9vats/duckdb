#![allow(clippy::too_many_arguments)]

use quent_model::{fsm, state};
use uuid::Uuid;

pub use crate::runtime_resource::TemporaryIoChannel;

state! {
    IoRequested {
        attributes: {
            query_id: Uuid,
            plan_id: Uuid,
            task_id: Uuid,
            // Operator whose work caused this buffer-manager I/O.
            trigger_operator_id: Uuid,
            block_id: u64,
            memory_tag: String,
            direction: String,
        },
    }
}

state! {
    IoActive {
        usages: {
            channel: TemporaryIoChannel,
        },
    }
}

state! {
    IoCompleted {
        attributes: {
            success: bool,
            // Physical bytes in the temporary block.
            storage_bytes: u64,
        },
    }
}

fsm! {
    TemporaryBlockIo {
        states: {
            io_requested: IoRequested,
            io_active: IoActive,
            io_completed: IoCompleted,
        },
        entry: io_requested,
        exit_from: { io_completed },
        transitions: {
            io_requested => io_active,
            // I/O setup failed before channel activation.
            io_requested => io_completed,
            io_active => io_completed,
        },
    }
}
