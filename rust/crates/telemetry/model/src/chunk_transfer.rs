#![allow(clippy::too_many_arguments)]

use quent_model::{fsm, state};
use uuid::Uuid;

state! {
    Produced {
        attributes: {
            query_id: Uuid,
            task_id: Uuid,
            source_operator_id: Uuid,
            source_port_id: Uuid,
            target_operator_id: Uuid,
            target_port_id: Uuid,
            rows: u64,
            logical_bytes: u64,
        },
    }
}

state! { Published {} }

fsm! {
    ChunkTransfer {
        states: {
            produced: Produced,
            published: Published,
        },
        entry: produced,
        exit_from: { published },
        transitions: {
            produced => published,
        },
    }
}
