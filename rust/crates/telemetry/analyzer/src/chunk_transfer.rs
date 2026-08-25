use duckdb_telemetry_model::chunk_transfer::ChunkTransferTransition;
use quent_analyzer::fsm::events::{FsmEvents, FsmEventsBuilder};
use quent_query_engine_ui::OperatorFilter;
use quent_time::{TimeUnixNanoSec, Timestamp};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub type ChunkTransfer = FsmEvents<ChunkTransferTransition>;
pub type ChunkTransferBuilder = FsmEventsBuilder<ChunkTransferTransition>;

#[derive(Clone, Copy, Debug)]
pub struct ChunkPublication {
    pub timestamp: TimeUnixNanoSec,
    pub source_operator_id: Uuid,
    pub target_operator_id: Uuid,
    pub rows: u64,
    pub logical_bytes: u64,
}

pub trait ChunkTransferExt {
    fn query_id(&self) -> Option<Uuid>;
    fn is_complete(&self) -> bool;
    fn publication(&self) -> Option<ChunkPublication>;
    fn belongs_to_query(
        &self,
        port_operators: &HashMap<Uuid, Uuid>,
        plan_edges: &HashSet<(Uuid, Uuid)>,
        task_ids: &HashSet<Uuid>,
    ) -> bool;
    fn matches_operator(&self, filter: &OperatorFilter) -> bool;
}

impl ChunkTransferExt for ChunkTransfer {
    fn query_id(&self) -> Option<Uuid> {
        self.first_data().and_then(|transition| match transition {
            ChunkTransferTransition::Produced(produced) => Some(produced.query_id),
            _ => None,
        })
    }

    fn is_complete(&self) -> bool {
        self.transitions()
            .last()
            .is_some_and(|transition| matches!(&transition.data, ChunkTransferTransition::Exit))
    }

    fn publication(&self) -> Option<ChunkPublication> {
        let transition = self.transitions().first()?;
        let ChunkTransferTransition::Produced(produced) = &transition.data else {
            return None;
        };
        Some(ChunkPublication {
            timestamp: transition.timestamp(),
            source_operator_id: produced.source_operator_id,
            target_operator_id: produced.target_operator_id,
            rows: produced.rows,
            logical_bytes: produced.logical_bytes,
        })
    }

    fn belongs_to_query(
        &self,
        port_operators: &HashMap<Uuid, Uuid>,
        plan_edges: &HashSet<(Uuid, Uuid)>,
        task_ids: &HashSet<Uuid>,
    ) -> bool {
        self.first_data()
            .is_some_and(|transition| match transition {
                ChunkTransferTransition::Produced(produced) => {
                    port_operators.get(&produced.source_port_id)
                        == Some(&produced.source_operator_id)
                        && port_operators.get(&produced.target_port_id)
                            == Some(&produced.target_operator_id)
                        && plan_edges.contains(&(produced.source_port_id, produced.target_port_id))
                        && (produced.task_id.is_nil() || task_ids.contains(&produced.task_id))
                }
                _ => false,
            })
    }

    fn matches_operator(&self, filter: &OperatorFilter) -> bool {
        if filter.operator_ids.is_empty() {
            return true;
        }

        self.first_data()
            .is_some_and(|transition| match transition {
                ChunkTransferTransition::Produced(produced) => {
                    filter.operator_ids.contains(&produced.source_operator_id)
                        || filter.operator_ids.contains(&produced.target_operator_id)
                }
                _ => false,
            })
    }
}
