use std::collections::{HashMap, HashSet};

use duckdb_telemetry_model::temporary_block_io::TemporaryBlockIoTransition;
use quent_analyzer::{
    fsm::events::{FsmEvents, FsmEventsBuilder},
    resource::collection::InMemoryResources,
};
use quent_query_engine_ui::OperatorFilter;
use uuid::Uuid;

pub(crate) type TemporaryBlockIo = FsmEvents<TemporaryBlockIoTransition>;
pub(crate) type TemporaryBlockIoBuilder = FsmEventsBuilder<TemporaryBlockIoTransition>;

pub(crate) trait TemporaryBlockIoExt {
    fn query_id(&self) -> Option<Uuid>;
    fn is_complete(&self) -> bool;
    fn belongs_to_query(
        &self,
        operator_plans: &HashMap<Uuid, Uuid>,
        plan_workers: &HashMap<Uuid, Uuid>,
        plan_ids: &HashSet<Uuid>,
        task_plans: &HashMap<Uuid, Uuid>,
        task_operators: &HashMap<Uuid, HashSet<Uuid>>,
    ) -> bool;
    fn resources_are_valid(
        &self,
        plan_workers: &HashMap<Uuid, Uuid>,
        resources: &InMemoryResources,
    ) -> bool;
    fn matches_operator(&self, filter: &OperatorFilter) -> bool;
}

impl TemporaryBlockIoExt for TemporaryBlockIo {
    fn query_id(&self) -> Option<Uuid> {
        requested(self).map(|requested| requested.query_id)
    }

    fn is_complete(&self) -> bool {
        self.transitions()
            .last()
            .is_some_and(|transition| matches!(&transition.data, TemporaryBlockIoTransition::Exit))
    }

    fn belongs_to_query(
        &self,
        operator_plans: &HashMap<Uuid, Uuid>,
        plan_workers: &HashMap<Uuid, Uuid>,
        plan_ids: &HashSet<Uuid>,
        task_plans: &HashMap<Uuid, Uuid>,
        task_operators: &HashMap<Uuid, HashSet<Uuid>>,
    ) -> bool {
        let Some(requested) = requested(self) else {
            return false;
        };
        if !plan_ids.contains(&requested.plan_id) || !plan_workers.contains_key(&requested.plan_id)
        {
            return false;
        }
        if !requested.task_id.is_nil()
            && task_plans.get(&requested.task_id) != Some(&requested.plan_id)
        {
            return false;
        }
        if !requested.trigger_operator_id.is_nil()
            && operator_plans.get(&requested.trigger_operator_id) != Some(&requested.plan_id)
        {
            return false;
        }

        requested.task_id.is_nil()
            || requested.trigger_operator_id.is_nil()
            || task_operators
                .get(&requested.task_id)
                .is_some_and(|operators| operators.contains(&requested.trigger_operator_id))
    }

    fn resources_are_valid(
        &self,
        plan_workers: &HashMap<Uuid, Uuid>,
        resources: &InMemoryResources,
    ) -> bool {
        let Some(requested) = requested(self) else {
            return false;
        };
        let Some(worker_id) = plan_workers.get(&requested.plan_id) else {
            return false;
        };

        self.transitions().iter().all(|transition| {
            let TemporaryBlockIoTransition::IoActive(active) = &transition.data else {
                return true;
            };

            active.channel.as_ref().is_some_and(|usage| {
                resources
                    .resources
                    .get(&usage.resource_id.uuid())
                    .is_some_and(|resource| {
                        resource.type_name == crate::model::TEMPORARY_IO_CHANNEL_TYPE_NAME
                            && resource.parent_group_id == *worker_id
                    })
            })
        })
    }

    fn matches_operator(&self, filter: &OperatorFilter) -> bool {
        if filter.operator_ids.is_empty() {
            return true;
        }

        requested(self).is_some_and(|requested| {
            !requested.trigger_operator_id.is_nil()
                && filter.operator_ids.contains(&requested.trigger_operator_id)
        })
    }
}

fn requested(
    io: &TemporaryBlockIo,
) -> Option<&duckdb_telemetry_model::temporary_block_io::IoRequested> {
    io.first_data().and_then(|transition| match transition {
        TemporaryBlockIoTransition::IoRequested(requested) => Some(requested),
        _ => None,
    })
}
