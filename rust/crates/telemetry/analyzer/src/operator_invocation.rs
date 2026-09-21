use duckdb_telemetry_store::OperatorInvocationEvent;
use quent_analyzer::{
    AnalyzerResult, Entity,
    fsm::{
        Fsm, FsmUsages,
        native::{AnalyzedFsm, AnalyzedFsmBuilder, AnalyzedTransition},
    },
    resource::{Usage, Using},
};
use quent_query_engine_ui::OperatorFilter;
use quent_time::{TimeUnixNanoSec, Timestamp, span::SpanUnixNanoSec};
use quent_ui::fsm::{FsmStateTypeDecl, FsmTransitionDecl, FsmTypeDecl, FsmTypeDeclaration};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use uuid::Uuid;

use crate::model::DuckDbModel;

pub type OperatorInvocationBuilder = AnalyzedFsmBuilder<OperatorInvocationEvent>;

#[derive(Debug)]
pub struct OperatorInvocation(AnalyzedFsm<OperatorInvocationEvent>);

impl OperatorInvocation {
    pub(crate) fn try_from_builder(builder: OperatorInvocationBuilder) -> AnalyzerResult<Self> {
        Ok(Self(builder.try_build()?))
    }
    pub(crate) fn transitions(&self) -> &[AnalyzedTransition<OperatorInvocationEvent>] {
        self.0.transitions()
    }
    fn first_data(&self) -> Option<&OperatorInvocationEvent> {
        self.0.transition(0).map(|transition| &transition.data)
    }
}

impl Entity for OperatorInvocation {
    fn id(&self) -> Uuid {
        self.0.id()
    }
    fn type_name(&self) -> &str {
        "operator_invocation"
    }
    fn earliest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.earliest_timestamp()
    }
    fn latest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.latest_timestamp()
    }
}

impl Fsm for OperatorInvocation {
    type TransitionType = AnalyzedTransition<OperatorInvocationEvent>;
    fn len(&self) -> usize {
        self.0.len()
    }
    fn transition(&self, index: usize) -> Option<&Self::TransitionType> {
        self.0.transition(index)
    }
}

impl<'a> FsmUsages<'a> for OperatorInvocation {
    fn usages_with_state_names(&'a self) -> impl Iterator<Item = (&'a str, impl Usage<'a>)> {
        self.0.usages_with_state_names()
    }
}

impl Using for OperatorInvocation {
    fn usages(&self) -> impl Iterator<Item = impl Usage<'_>> {
        self.0.usages()
    }
}

impl FsmTypeDeclaration for OperatorInvocation {
    fn fsm_type_declaration() -> FsmTypeDecl {
        let state = |name: &str, usages: &[&str]| FsmStateTypeDecl {
            name: name.to_owned(),
            usages: usages.iter().map(|usage| (*usage).to_owned()).collect(),
        };
        FsmTypeDecl {
            name: "operator_invocation".to_owned(),
            states: vec![
                state("invocation_created", &[]),
                state("invocation_running", &["execution_thread"]),
                state("invocation_completed", &[]),
                state("exit", &[]),
            ],
            transitions: vec![
                FsmTransitionDecl::Entry("invocation_created".to_owned()),
                FsmTransitionDecl::Transition(
                    "invocation_created".to_owned(),
                    "invocation_running".to_owned(),
                ),
                FsmTransitionDecl::Transition(
                    "invocation_created".to_owned(),
                    "invocation_completed".to_owned(),
                ),
                FsmTransitionDecl::Transition(
                    "invocation_running".to_owned(),
                    "invocation_completed".to_owned(),
                ),
                FsmTransitionDecl::Transition("invocation_completed".to_owned(), "exit".to_owned()),
                FsmTransitionDecl::Exit("exit".to_owned()),
            ],
        }
    }
}

pub trait OperatorInvocationExt {
    fn query_id(&self) -> Option<Uuid>;
    fn belongs_to_query(
        &self,
        operator_plans: &HashMap<Uuid, Uuid>,
        plan_workers: &HashMap<Uuid, Uuid>,
        plan_ids: &HashSet<Uuid>,
        task_operators: &HashMap<Uuid, HashSet<Uuid>>,
    ) -> bool;
    fn operator_id(&self) -> Option<Uuid>;
    fn active_span(&self) -> Option<SpanUnixNanoSec>;
    fn resources_are_valid(&self, plan_workers: &HashMap<Uuid, Uuid>, model: &DuckDbModel) -> bool;
    fn matches_operator(&self, filter: &OperatorFilter) -> bool;
}

impl OperatorInvocationExt for OperatorInvocation {
    fn query_id(&self) -> Option<Uuid> {
        match self.first_data()? {
            OperatorInvocationEvent::InvocationCreated { query_id, .. } => Some(query_id.target),
            _ => None,
        }
    }

    fn belongs_to_query(
        &self,
        operator_plans: &HashMap<Uuid, Uuid>,
        plan_workers: &HashMap<Uuid, Uuid>,
        plan_ids: &HashSet<Uuid>,
        task_operators: &HashMap<Uuid, HashSet<Uuid>>,
    ) -> bool {
        let Some(OperatorInvocationEvent::InvocationCreated {
            plan_id,
            task_id,
            operator_id,
            ..
        }) = self.first_data()
        else {
            return false;
        };
        plan_ids.contains(&plan_id.target)
            && plan_workers.contains_key(&plan_id.target)
            && operator_plans.get(&operator_id.target) == Some(&plan_id.target)
            && task_id.as_ref().is_none_or(|task| {
                task_operators
                    .get(&task.target)
                    .is_some_and(|operators| operators.contains(&operator_id.target))
            })
    }

    fn operator_id(&self) -> Option<Uuid> {
        match self.first_data()? {
            OperatorInvocationEvent::InvocationCreated { operator_id, .. } => {
                Some(operator_id.target)
            }
            _ => None,
        }
    }

    fn active_span(&self) -> Option<SpanUnixNanoSec> {
        self.transitions().windows(2).find_map(|window| {
            matches!(
                (&window[0].data, &window[1].data),
                (
                    OperatorInvocationEvent::InvocationRunning { .. },
                    OperatorInvocationEvent::InvocationCompleted { .. }
                )
            )
            .then(|| SpanUnixNanoSec::try_new(window[0].timestamp(), window[1].timestamp()).ok())
            .flatten()
        })
    }

    fn resources_are_valid(&self, plan_workers: &HashMap<Uuid, Uuid>, model: &DuckDbModel) -> bool {
        let Some(OperatorInvocationEvent::InvocationCreated { plan_id, .. }) = self.first_data()
        else {
            return false;
        };
        let Some(worker_id) = plan_workers.get(&plan_id.target) else {
            return false;
        };
        self.transitions()
            .iter()
            .all(|transition| match &transition.data {
                OperatorInvocationEvent::InvocationRunning {
                    execution_thread, ..
                } => model.resource_matches(
                    execution_thread.target,
                    crate::model::EXECUTION_THREAD_TYPE_NAME,
                    *worker_id,
                ),
                _ => true,
            })
    }

    fn matches_operator(&self, filter: &OperatorFilter) -> bool {
        if filter.operator_ids.is_empty() {
            return true;
        }
        matches!(self.first_data(), Some(OperatorInvocationEvent::InvocationCreated { operator_id, .. }) if filter.operator_ids.contains(&operator_id.target))
    }
}
