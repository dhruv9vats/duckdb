use duckdb_telemetry_model::pipeline_task::PipelineTaskTransition;
use quent_analyzer::{
    fsm::events::{FsmEvents, FsmEventsBuilder},
    resource::collection::InMemoryResources,
};
use quent_query_engine_ui::OperatorFilter;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub type PipelineTask = FsmEvents<PipelineTaskTransition>;
pub type PipelineTaskBuilder = FsmEventsBuilder<PipelineTaskTransition>;

pub trait PipelineTaskExt {
    fn query_id(&self) -> Option<Uuid>;
    fn is_complete(&self) -> bool;
    fn belongs_to_query(
        &self,
        operator_plans: &HashMap<Uuid, Uuid>,
        plan_workers: &HashMap<Uuid, Uuid>,
        plan_ids: &HashSet<Uuid>,
    ) -> bool;
    fn operator_ids(&self) -> Option<&[Uuid]>;
    fn resources_are_valid(&self, resources: &InMemoryResources) -> bool;
    fn matches_operator(&self, filter: &OperatorFilter) -> bool;
}

impl PipelineTaskExt for PipelineTask {
    fn query_id(&self) -> Option<Uuid> {
        self.first_data().and_then(|transition| match transition {
            PipelineTaskTransition::Created(created) => Some(created.query_id),
            _ => None,
        })
    }

    fn is_complete(&self) -> bool {
        self.transitions()
            .last()
            .is_some_and(|transition| matches!(&transition.data, PipelineTaskTransition::Exit))
    }

    fn belongs_to_query(
        &self,
        operator_plans: &HashMap<Uuid, Uuid>,
        plan_workers: &HashMap<Uuid, Uuid>,
        plan_ids: &HashSet<Uuid>,
    ) -> bool {
        self.first_data()
            .is_some_and(|transition| match transition {
                PipelineTaskTransition::Created(created) => {
                    plan_ids.contains(&created.plan_id)
                        && plan_workers.get(&created.plan_id) == Some(&created.worker_id)
                        && !created.operator_ids.is_empty()
                        && created.operator_ids.iter().all(|operator_id| {
                            operator_plans.get(operator_id) == Some(&created.plan_id)
                        })
                }
                _ => false,
            })
    }

    fn operator_ids(&self) -> Option<&[Uuid]> {
        self.first_data().and_then(|transition| match transition {
            PipelineTaskTransition::Created(created) => Some(created.operator_ids.as_slice()),
            _ => None,
        })
    }

    fn resources_are_valid(&self, resources: &InMemoryResources) -> bool {
        let Some(PipelineTaskTransition::Created(created)) = self.first_data() else {
            return false;
        };
        let worker_id = created.worker_id;

        self.transitions()
            .iter()
            .all(|transition| match &transition.data {
                PipelineTaskTransition::Created(state) => {
                    state.queue.as_ref().is_some_and(|usage| {
                        resource_matches(
                            resources,
                            usage.resource_id.uuid(),
                            crate::model::TASK_QUEUE_TYPE_NAME,
                            worker_id,
                        )
                    })
                }
                PipelineTaskTransition::Ready(state) => state.queue.as_ref().is_some_and(|usage| {
                    resource_matches(
                        resources,
                        usage.resource_id.uuid(),
                        crate::model::TASK_QUEUE_TYPE_NAME,
                        worker_id,
                    )
                }),
                PipelineTaskTransition::Running(state) => {
                    state.execution_thread.as_ref().is_some_and(|usage| {
                        resource_matches(
                            resources,
                            usage.resource_id.uuid(),
                            crate::model::EXECUTION_THREAD_TYPE_NAME,
                            worker_id,
                        )
                    })
                }
                PipelineTaskTransition::Blocked(_)
                | PipelineTaskTransition::Finalizing(_)
                | PipelineTaskTransition::Exit => true,
            })
    }

    fn matches_operator(&self, filter: &OperatorFilter) -> bool {
        if filter.operator_ids.is_empty() {
            return true;
        }

        self.first_data()
            .is_some_and(|transition| match transition {
                PipelineTaskTransition::Created(created) => created
                    .operator_ids
                    .iter()
                    .any(|operator_id| filter.operator_ids.contains(operator_id)),
                _ => false,
            })
    }
}

fn resource_matches(
    resources: &InMemoryResources,
    resource_id: Uuid,
    type_name: &str,
    parent_id: Uuid,
) -> bool {
    resources
        .resources
        .get(&resource_id)
        .is_some_and(|resource| {
            resource.type_name == type_name && resource.parent_group_id == parent_id
        })
}
