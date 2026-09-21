use duckdb_telemetry_store::ChunkTransferEvent;
use quent_analyzer::{
    AnalyzerResult, Entity,
    fsm::{
        Fsm, FsmUsages,
        native::{AnalyzedFsm, AnalyzedFsmBuilder, AnalyzedTransition},
    },
    resource::{Usage, Using},
};
use quent_query_engine_ui::OperatorFilter;
use quent_time::{TimeUnixNanoSec, Timestamp};
use quent_ui::fsm::{FsmStateTypeDecl, FsmTransitionDecl, FsmTypeDecl, FsmTypeDeclaration};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use uuid::Uuid;

pub type ChunkTransferBuilder = AnalyzedFsmBuilder<ChunkTransferEvent>;

#[derive(Debug)]
pub struct ChunkTransfer(AnalyzedFsm<ChunkTransferEvent>);

impl ChunkTransfer {
    pub(crate) fn try_from_builder(builder: ChunkTransferBuilder) -> AnalyzerResult<Self> {
        Ok(Self(builder.try_build()?))
    }

    pub(crate) fn transitions(&self) -> &[AnalyzedTransition<ChunkTransferEvent>] {
        self.0.transitions()
    }

    fn first_data(&self) -> Option<&ChunkTransferEvent> {
        self.0.transition(0).map(|transition| &transition.data)
    }
}

impl Entity for ChunkTransfer {
    fn id(&self) -> Uuid {
        self.0.id()
    }
    fn type_name(&self) -> &str {
        "chunk_transfer"
    }
    fn earliest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.earliest_timestamp()
    }
    fn latest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.latest_timestamp()
    }
}

impl Fsm for ChunkTransfer {
    type TransitionType = AnalyzedTransition<ChunkTransferEvent>;
    fn len(&self) -> usize {
        self.0.len()
    }
    fn transition(&self, index: usize) -> Option<&Self::TransitionType> {
        self.0.transition(index)
    }
}

impl<'a> FsmUsages<'a> for ChunkTransfer {
    fn usages_with_state_names(&'a self) -> impl Iterator<Item = (&'a str, impl Usage<'a>)> {
        self.0.usages_with_state_names()
    }
}

impl Using for ChunkTransfer {
    fn usages(&self) -> impl Iterator<Item = impl Usage<'_>> {
        self.0.usages()
    }
}

impl FsmTypeDeclaration for ChunkTransfer {
    fn fsm_type_declaration() -> FsmTypeDecl {
        FsmTypeDecl {
            name: "chunk_transfer".to_owned(),
            states: ["produced", "published", "exit"]
                .into_iter()
                .map(|name| FsmStateTypeDecl {
                    name: name.to_owned(),
                    usages: vec![],
                })
                .collect(),
            transitions: vec![
                FsmTransitionDecl::Entry("produced".to_owned()),
                FsmTransitionDecl::Transition("produced".to_owned(), "published".to_owned()),
                FsmTransitionDecl::Transition("published".to_owned(), "exit".to_owned()),
                FsmTransitionDecl::Exit("exit".to_owned()),
            ],
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ChunkPublication {
    pub timestamp: TimeUnixNanoSec,
    pub source_operator_id: Uuid,
    pub source_port_id: Uuid,
    pub target_operator_id: Uuid,
    pub target_port_id: Uuid,
    pub rows: u64,
    pub logical_bytes: u64,
}

pub trait ChunkTransferExt {
    fn query_id(&self) -> Option<Uuid>;
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
        match self.first_data()? {
            ChunkTransferEvent::Produced { query_id, .. } => Some(query_id.target),
            _ => None,
        }
    }

    fn publication(&self) -> Option<ChunkPublication> {
        let transition = self.transitions().first()?;
        let ChunkTransferEvent::Produced {
            source_operator_id,
            source_port_id,
            target_operator_id,
            target_port_id,
            rows,
            logical_bytes,
            ..
        } = &transition.data
        else {
            return None;
        };

        Some(ChunkPublication {
            timestamp: transition.timestamp(),
            source_operator_id: source_operator_id.target,
            source_port_id: source_port_id.target,
            target_operator_id: target_operator_id.target,
            target_port_id: target_port_id.target,
            rows: *rows,
            logical_bytes: *logical_bytes,
        })
    }

    fn belongs_to_query(
        &self,
        port_operators: &HashMap<Uuid, Uuid>,
        plan_edges: &HashSet<(Uuid, Uuid)>,
        task_ids: &HashSet<Uuid>,
    ) -> bool {
        let Some(ChunkTransferEvent::Produced {
            task_id,
            source_operator_id,
            source_port_id,
            target_operator_id,
            target_port_id,
            ..
        }) = self.first_data()
        else {
            return false;
        };

        port_operators.get(&source_port_id.target) == Some(&source_operator_id.target)
            && port_operators.get(&target_port_id.target) == Some(&target_operator_id.target)
            && plan_edges.contains(&(source_port_id.target, target_port_id.target))
            && task_id
                .as_ref()
                .is_none_or(|task| task_ids.contains(&task.target))
    }

    fn matches_operator(&self, filter: &OperatorFilter) -> bool {
        if filter.operator_ids.is_empty() {
            return true;
        }

        matches!(self.first_data(), Some(ChunkTransferEvent::Produced { source_operator_id, target_operator_id, .. })
			if filter.operator_ids.contains(&source_operator_id.target) || filter.operator_ids.contains(&target_operator_id.target))
    }
}
