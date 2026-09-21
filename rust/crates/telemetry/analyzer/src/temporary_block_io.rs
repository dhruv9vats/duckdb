use duckdb_telemetry_store::TemporaryBlockIoEvent;
use quent_analyzer::{
    AnalyzerResult, Entity,
    fsm::{
        Fsm, FsmUsages,
        native::{AnalyzedFsm, AnalyzedFsmBuilder, AnalyzedTransition},
    },
    resource::{Usage, Using},
};
use quent_query_engine_ui::OperatorFilter;
use quent_time::TimeUnixNanoSec;
use quent_ui::fsm::{FsmStateTypeDecl, FsmTransitionDecl, FsmTypeDecl, FsmTypeDeclaration};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use uuid::Uuid;

use crate::model::DuckDbModel;

pub(crate) type TemporaryBlockIoBuilder = AnalyzedFsmBuilder<TemporaryBlockIoEvent>;

#[derive(Debug)]
pub(crate) struct TemporaryBlockIo(AnalyzedFsm<TemporaryBlockIoEvent>);

impl TemporaryBlockIo {
    pub(crate) fn try_from_builder(builder: TemporaryBlockIoBuilder) -> AnalyzerResult<Self> {
        Ok(Self(builder.try_build()?))
    }
    pub(crate) fn transitions(&self) -> &[AnalyzedTransition<TemporaryBlockIoEvent>] {
        self.0.transitions()
    }
    fn first_data(&self) -> Option<&TemporaryBlockIoEvent> {
        self.0.transition(0).map(|transition| &transition.data)
    }
}

impl Entity for TemporaryBlockIo {
    fn id(&self) -> Uuid {
        self.0.id()
    }
    fn type_name(&self) -> &str {
        "temporary_block_io"
    }
    fn earliest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.earliest_timestamp()
    }
    fn latest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.latest_timestamp()
    }
}

impl Fsm for TemporaryBlockIo {
    type TransitionType = AnalyzedTransition<TemporaryBlockIoEvent>;
    fn len(&self) -> usize {
        self.0.len()
    }
    fn transition(&self, index: usize) -> Option<&Self::TransitionType> {
        self.0.transition(index)
    }
}

impl<'a> FsmUsages<'a> for TemporaryBlockIo {
    fn usages_with_state_names(&'a self) -> impl Iterator<Item = (&'a str, impl Usage<'a>)> {
        self.0.usages_with_state_names()
    }
}

impl Using for TemporaryBlockIo {
    fn usages(&self) -> impl Iterator<Item = impl Usage<'_>> {
        self.0.usages()
    }
}

impl FsmTypeDeclaration for TemporaryBlockIo {
    fn fsm_type_declaration() -> FsmTypeDecl {
        let state = |name: &str, usages: &[&str]| FsmStateTypeDecl {
            name: name.to_owned(),
            usages: usages.iter().map(|usage| (*usage).to_owned()).collect(),
        };
        FsmTypeDecl {
            name: "temporary_block_io".to_owned(),
            states: vec![
                state("io_requested", &[]),
                state("io_active", &["channel"]),
                state("io_completed", &[]),
                state("exit", &[]),
            ],
            transitions: vec![
                FsmTransitionDecl::Entry("io_requested".to_owned()),
                FsmTransitionDecl::Transition("io_requested".to_owned(), "io_active".to_owned()),
                FsmTransitionDecl::Transition("io_requested".to_owned(), "io_completed".to_owned()),
                FsmTransitionDecl::Transition("io_active".to_owned(), "io_completed".to_owned()),
                FsmTransitionDecl::Transition("io_completed".to_owned(), "exit".to_owned()),
                FsmTransitionDecl::Exit("exit".to_owned()),
            ],
        }
    }
}

pub(crate) trait TemporaryBlockIoExt {
    fn query_id(&self) -> Option<Uuid>;
    fn belongs_to_query(
        &self,
        operator_plans: &HashMap<Uuid, Uuid>,
        plan_workers: &HashMap<Uuid, Uuid>,
        plan_ids: &HashSet<Uuid>,
        task_plans: &HashMap<Uuid, Uuid>,
        task_operators: &HashMap<Uuid, HashSet<Uuid>>,
    ) -> bool;
    fn resources_are_valid(&self, plan_workers: &HashMap<Uuid, Uuid>, model: &DuckDbModel) -> bool;
    fn matches_operator(&self, filter: &OperatorFilter) -> bool;
}

impl TemporaryBlockIoExt for TemporaryBlockIo {
    fn query_id(&self) -> Option<Uuid> {
        match self.first_data()? {
            TemporaryBlockIoEvent::IoRequested { query_id, .. } => Some(query_id.target),
            _ => None,
        }
    }

    fn belongs_to_query(
        &self,
        operator_plans: &HashMap<Uuid, Uuid>,
        plan_workers: &HashMap<Uuid, Uuid>,
        plan_ids: &HashSet<Uuid>,
        task_plans: &HashMap<Uuid, Uuid>,
        task_operators: &HashMap<Uuid, HashSet<Uuid>>,
    ) -> bool {
        let Some(TemporaryBlockIoEvent::IoRequested {
            plan_id,
            task_id,
            trigger_operator_id,
            ..
        }) = self.first_data()
        else {
            return false;
        };
        if !plan_ids.contains(&plan_id.target) || !plan_workers.contains_key(&plan_id.target) {
            return false;
        }
        if task_id
            .as_ref()
            .is_some_and(|task| task_plans.get(&task.target) != Some(&plan_id.target))
        {
            return false;
        }
        if trigger_operator_id
            .as_ref()
            .is_some_and(|operator| operator_plans.get(&operator.target) != Some(&plan_id.target))
        {
            return false;
        }

        match (task_id, trigger_operator_id) {
            (Some(task), Some(operator)) => task_operators
                .get(&task.target)
                .is_some_and(|operators| operators.contains(&operator.target)),
            _ => true,
        }
    }

    fn resources_are_valid(&self, plan_workers: &HashMap<Uuid, Uuid>, model: &DuckDbModel) -> bool {
        let Some(TemporaryBlockIoEvent::IoRequested { plan_id, .. }) = self.first_data() else {
            return false;
        };
        let Some(worker_id) = plan_workers.get(&plan_id.target) else {
            return false;
        };
        self.transitions()
            .iter()
            .all(|transition| match &transition.data {
                TemporaryBlockIoEvent::IoActive { channel, .. } => model.resource_matches(
                    channel.target,
                    crate::model::TEMPORARY_IO_CHANNEL_TYPE_NAME,
                    *worker_id,
                ),
                _ => true,
            })
    }

    fn matches_operator(&self, filter: &OperatorFilter) -> bool {
        if filter.operator_ids.is_empty() {
            return true;
        }
        matches!(self.first_data(), Some(TemporaryBlockIoEvent::IoRequested { trigger_operator_id: Some(operator), .. }) if filter.operator_ids.contains(&operator.target))
    }
}
