use duckdb_telemetry_model::memory_account::{
    AccountRegistered, Accounted, MemoryAccountTransition,
};
use quent_analyzer::{
    fsm::events::{FsmEvents, FsmEventsBuilder},
    resource::collection::InMemoryResources,
};
use quent_query_engine_ui::OperatorFilter;
use uuid::Uuid;

pub(crate) type MemoryAccount = FsmEvents<MemoryAccountTransition>;
pub(crate) type MemoryAccountBuilder = FsmEventsBuilder<MemoryAccountTransition>;

pub(crate) trait MemoryAccountExt {
    fn memory_tag(&self) -> Option<&str>;
    fn is_complete(&self) -> bool;
    fn is_valid(&self, engine_id: Uuid, resources: &InMemoryResources) -> bool;
    fn matches_operator(&self, filter: &OperatorFilter) -> bool;
}

impl MemoryAccountExt for MemoryAccount {
    fn memory_tag(&self) -> Option<&str> {
        registered(self).map(|state| state.memory_tag.as_str())
    }

    fn is_complete(&self) -> bool {
        self.transitions()
            .last()
            .is_some_and(|transition| matches!(&transition.data, MemoryAccountTransition::Exit))
    }

    fn is_valid(&self, engine_id: Uuid, resources: &InMemoryResources) -> bool {
        let Some(tag) = self.memory_tag() else {
            return false;
        };
        if tag.is_empty() {
            return false;
        }

        let mut account_resource = None;
        for transition in self.transitions() {
            let MemoryAccountTransition::Accounted(state) = &transition.data else {
                continue;
            };

            let Some((resource_id, expected_type)) = account_usage(state) else {
                return false;
            };
            if account_resource.is_some_and(|id| id != resource_id) {
                return false;
            }
            account_resource = Some(resource_id);

            let Some(resource) = resources.resources.get(&resource_id) else {
                return false;
            };
            if resource.type_name != expected_type || resource.parent_group_id != engine_id {
                return false;
            }
        }

        account_resource.is_some()
    }

    fn matches_operator(&self, filter: &OperatorFilter) -> bool {
        filter.operator_ids.is_empty()
    }
}

fn registered(account: &MemoryAccount) -> Option<&AccountRegistered> {
    account
        .first_data()
        .and_then(|transition| match transition {
            MemoryAccountTransition::AccountRegistered(state) => Some(state),
            _ => None,
        })
}

fn account_usage(state: &Accounted) -> Option<(Uuid, &'static str)> {
    let mut result = None;
    for usage in [
        state.buffer_pool.as_ref().map(|usage| {
            (
                usage.resource_id.uuid(),
                crate::model::BUFFER_POOL_MEMORY_TYPE_NAME,
            )
        }),
        state.temporary_storage.as_ref().map(|usage| {
            (
                usage.resource_id.uuid(),
                crate::model::TEMPORARY_STORAGE_TYPE_NAME,
            )
        }),
        state.temporary_directory.as_ref().map(|usage| {
            (
                usage.resource_id.uuid(),
                crate::model::TEMPORARY_DIRECTORY_STORAGE_TYPE_NAME,
            )
        }),
    ]
    .into_iter()
    .flatten()
    {
        if result.is_some() {
            return None;
        }
        result = Some(usage);
    }
    result
}
