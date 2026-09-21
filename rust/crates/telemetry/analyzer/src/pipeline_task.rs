use duckdb_telemetry_store::PipelineTaskEvent;
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

pub type PipelineTaskBuilder = AnalyzedFsmBuilder<PipelineTaskEvent>;

#[derive(Debug)]
pub struct PipelineTask(AnalyzedFsm<PipelineTaskEvent>);

impl PipelineTask {
    pub(crate) fn try_from_builder(builder: PipelineTaskBuilder) -> AnalyzerResult<Self> {
        Ok(Self(builder.try_build()?))
    }
    pub(crate) fn transitions(&self) -> &[AnalyzedTransition<PipelineTaskEvent>] {
        self.0.transitions()
    }
    fn first_data(&self) -> Option<&PipelineTaskEvent> {
        self.0.transition(0).map(|transition| &transition.data)
    }
}

impl Entity for PipelineTask {
    fn id(&self) -> Uuid {
        self.0.id()
    }
    fn type_name(&self) -> &str {
        "pipeline_task"
    }
    fn earliest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.earliest_timestamp()
    }
    fn latest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.latest_timestamp()
    }
}

impl Fsm for PipelineTask {
    type TransitionType = AnalyzedTransition<PipelineTaskEvent>;
    fn len(&self) -> usize {
        self.0.len()
    }
    fn transition(&self, index: usize) -> Option<&Self::TransitionType> {
        self.0.transition(index)
    }
}

impl<'a> FsmUsages<'a> for PipelineTask {
    fn usages_with_state_names(&'a self) -> impl Iterator<Item = (&'a str, impl Usage<'a>)> {
        self.0.usages_with_state_names()
    }
}

impl Using for PipelineTask {
    fn usages(&self) -> impl Iterator<Item = impl Usage<'_>> {
        self.0.usages()
    }
}

impl FsmTypeDeclaration for PipelineTask {
    fn fsm_type_declaration() -> FsmTypeDecl {
        let state = |name: &str, usages: &[&str]| FsmStateTypeDecl {
            name: name.to_owned(),
            usages: usages.iter().map(|usage| (*usage).to_owned()).collect(),
        };
        FsmTypeDecl {
            name: "pipeline_task".to_owned(),
            states: vec![
                state("created", &["queue"]),
                state("running", &["execution_thread"]),
                state("ready", &["queue"]),
                state("blocked", &[]),
                state("finalizing", &[]),
                state("exit", &[]),
            ],
            transitions: vec![
                FsmTransitionDecl::Entry("created".to_owned()),
                FsmTransitionDecl::Transition("created".to_owned(), "running".to_owned()),
                FsmTransitionDecl::Transition("created".to_owned(), "finalizing".to_owned()),
                FsmTransitionDecl::Transition("running".to_owned(), "ready".to_owned()),
                FsmTransitionDecl::Transition("running".to_owned(), "blocked".to_owned()),
                FsmTransitionDecl::Transition("running".to_owned(), "finalizing".to_owned()),
                FsmTransitionDecl::Transition("ready".to_owned(), "running".to_owned()),
                FsmTransitionDecl::Transition("ready".to_owned(), "finalizing".to_owned()),
                FsmTransitionDecl::Transition("blocked".to_owned(), "running".to_owned()),
                FsmTransitionDecl::Transition("blocked".to_owned(), "finalizing".to_owned()),
                FsmTransitionDecl::Transition("finalizing".to_owned(), "exit".to_owned()),
                FsmTransitionDecl::Exit("exit".to_owned()),
            ],
        }
    }
}

pub trait PipelineTaskExt {
    fn query_id(&self) -> Option<Uuid>;
    fn belongs_to_query(
        &self,
        operator_plans: &HashMap<Uuid, Uuid>,
        plan_workers: &HashMap<Uuid, Uuid>,
        plan_ids: &HashSet<Uuid>,
    ) -> bool;
    fn operator_ids(&self) -> Option<Vec<Uuid>>;
    fn resources_are_valid(&self, model: &DuckDbModel) -> bool;
    fn matches_operator(&self, filter: &OperatorFilter) -> bool;
}

impl PipelineTaskExt for PipelineTask {
    fn query_id(&self) -> Option<Uuid> {
        match self.first_data()? {
            PipelineTaskEvent::Created { query_id, .. } => Some(query_id.target),
            _ => None,
        }
    }

    fn belongs_to_query(
        &self,
        operator_plans: &HashMap<Uuid, Uuid>,
        plan_workers: &HashMap<Uuid, Uuid>,
        plan_ids: &HashSet<Uuid>,
    ) -> bool {
        let Some(PipelineTaskEvent::Created {
            plan_id,
            worker_id,
            operator_ids,
            ..
        }) = self.first_data()
        else {
            return false;
        };
        plan_ids.contains(&plan_id.target)
            && plan_workers.get(&plan_id.target) == Some(&worker_id.target)
            && !operator_ids.is_empty()
            && operator_ids
                .iter()
                .all(|operator| operator_plans.get(&operator.target) == Some(&plan_id.target))
    }

    fn operator_ids(&self) -> Option<Vec<Uuid>> {
        match self.first_data()? {
            PipelineTaskEvent::Created { operator_ids, .. } => Some(
                operator_ids
                    .iter()
                    .map(|operator| operator.target)
                    .collect(),
            ),
            _ => None,
        }
    }

    fn resources_are_valid(&self, model: &DuckDbModel) -> bool {
        let Some(PipelineTaskEvent::Created { worker_id, .. }) = self.first_data() else {
            return false;
        };
        self.transitions()
            .iter()
            .all(|transition| match &transition.data {
                PipelineTaskEvent::Created { queue, .. }
                | PipelineTaskEvent::Ready { queue, .. } => model.resource_matches(
                    queue.target,
                    crate::model::TASK_QUEUE_TYPE_NAME,
                    worker_id.target,
                ),
                PipelineTaskEvent::Running {
                    execution_thread, ..
                } => model.resource_matches(
                    execution_thread.target,
                    crate::model::EXECUTION_THREAD_TYPE_NAME,
                    worker_id.target,
                ),
                _ => true,
            })
    }

    fn matches_operator(&self, filter: &OperatorFilter) -> bool {
        if filter.operator_ids.is_empty() {
            return true;
        }
        matches!(self.first_data(), Some(PipelineTaskEvent::Created { operator_ids, .. }) if operator_ids.iter().any(|operator| filter.operator_ids.contains(&operator.target)))
    }
}

pub(crate) fn task_plan_id(task: &PipelineTask) -> Option<Uuid> {
    match task.first_data()? {
        PipelineTaskEvent::Created { plan_id, .. } => Some(plan_id.target),
        _ => None,
    }
}
