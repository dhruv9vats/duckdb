use std::collections::{HashMap, HashSet};

use duckdb_telemetry_model::operator_invocation::OperatorInvocationTransition;
use quent_analyzer::{
    fsm::events::{FsmEvents, FsmEventsBuilder},
    resource::collection::InMemoryResources,
};
use quent_query_engine_ui::OperatorFilter;
use quent_time::{Timestamp, span::SpanUnixNanoSec};
use uuid::Uuid;

pub type OperatorInvocation = FsmEvents<OperatorInvocationTransition>;
pub type OperatorInvocationBuilder = FsmEventsBuilder<OperatorInvocationTransition>;

pub trait OperatorInvocationExt {
    fn query_id(&self) -> Option<Uuid>;
    fn is_complete(&self) -> bool;
    fn belongs_to_query(
        &self,
        operator_plans: &HashMap<Uuid, Uuid>,
        plan_workers: &HashMap<Uuid, Uuid>,
        plan_ids: &HashSet<Uuid>,
        task_operators: &HashMap<Uuid, HashSet<Uuid>>,
    ) -> bool;
    fn operator_id(&self) -> Option<Uuid>;
    fn active_span(&self) -> Option<SpanUnixNanoSec>;
    fn resources_are_valid(
        &self,
        plan_workers: &HashMap<Uuid, Uuid>,
        resources: &InMemoryResources,
    ) -> bool;
    fn matches_operator(&self, filter: &OperatorFilter) -> bool;
}

impl OperatorInvocationExt for OperatorInvocation {
    fn query_id(&self) -> Option<Uuid> {
        self.first_data().and_then(|transition| match transition {
            OperatorInvocationTransition::InvocationCreated(created) => Some(created.query_id),
            _ => None,
        })
    }

    fn is_complete(&self) -> bool {
        self.transitions().last().is_some_and(|transition| {
            matches!(&transition.data, OperatorInvocationTransition::Exit)
        })
    }

    fn belongs_to_query(
        &self,
        operator_plans: &HashMap<Uuid, Uuid>,
        plan_workers: &HashMap<Uuid, Uuid>,
        plan_ids: &HashSet<Uuid>,
        task_operators: &HashMap<Uuid, HashSet<Uuid>>,
    ) -> bool {
        self.first_data()
            .is_some_and(|transition| match transition {
                OperatorInvocationTransition::InvocationCreated(created) => {
                    plan_ids.contains(&created.plan_id)
                        && plan_workers.contains_key(&created.plan_id)
                        && operator_plans.get(&created.operator_id) == Some(&created.plan_id)
                        && (created.task_id.is_nil()
                            || task_operators
                                .get(&created.task_id)
                                .is_some_and(|operators| operators.contains(&created.operator_id)))
                }
                _ => false,
            })
    }

    fn operator_id(&self) -> Option<Uuid> {
        self.first_data().and_then(|transition| match transition {
            OperatorInvocationTransition::InvocationCreated(created) => Some(created.operator_id),
            _ => None,
        })
    }

    fn active_span(&self) -> Option<SpanUnixNanoSec> {
        self.transitions().windows(2).find_map(|window| {
            matches!(
                (&window[0].data, &window[1].data),
                (
                    OperatorInvocationTransition::InvocationRunning(_),
                    OperatorInvocationTransition::InvocationCompleted(_)
                )
            )
            .then(|| SpanUnixNanoSec::try_new(window[0].timestamp(), window[1].timestamp()).ok())
            .flatten()
        })
    }

    fn resources_are_valid(
        &self,
        plan_workers: &HashMap<Uuid, Uuid>,
        resources: &InMemoryResources,
    ) -> bool {
        let Some(OperatorInvocationTransition::InvocationCreated(created)) = self.first_data()
        else {
            return false;
        };
        let Some(worker_id) = plan_workers.get(&created.plan_id) else {
            return false;
        };

        self.transitions()
            .iter()
            .all(|transition| match &transition.data {
                OperatorInvocationTransition::InvocationRunning(state) => {
                    state.execution_thread.as_ref().is_some_and(|usage| {
                        resources
                            .resources
                            .get(&usage.resource_id.uuid())
                            .is_some_and(|resource| {
                                resource.type_name == crate::model::EXECUTION_THREAD_TYPE_NAME
                                    && resource.parent_group_id == *worker_id
                            })
                    })
                }
                OperatorInvocationTransition::InvocationCreated(_)
                | OperatorInvocationTransition::InvocationCompleted(_)
                | OperatorInvocationTransition::Exit => true,
            })
    }

    fn matches_operator(&self, filter: &OperatorFilter) -> bool {
        if filter.operator_ids.is_empty() {
            return true;
        }

        self.first_data()
            .is_some_and(|transition| match transition {
                OperatorInvocationTransition::InvocationCreated(created) => {
                    filter.operator_ids.contains(&created.operator_id)
                }
                _ => false,
            })
    }
}
