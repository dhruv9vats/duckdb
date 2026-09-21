use duckdb_telemetry_store::MemoryAccountEvent;
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
use uuid::Uuid;

use crate::model::DuckDbModel;

pub(crate) type MemoryAccountBuilder = AnalyzedFsmBuilder<MemoryAccountEvent>;

#[derive(Debug)]
pub(crate) struct MemoryAccount(AnalyzedFsm<MemoryAccountEvent>);

impl MemoryAccount {
    pub(crate) fn try_from_builder(builder: MemoryAccountBuilder) -> AnalyzerResult<Self> {
        Ok(Self(builder.try_build()?))
    }
    pub(crate) fn transitions(&self) -> &[AnalyzedTransition<MemoryAccountEvent>] {
        self.0.transitions()
    }
    fn first_data(&self) -> Option<&MemoryAccountEvent> {
        self.0.transition(0).map(|transition| &transition.data)
    }
}

impl Entity for MemoryAccount {
    fn id(&self) -> Uuid {
        self.0.id()
    }
    fn type_name(&self) -> &str {
        "memory_account"
    }
    fn earliest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.earliest_timestamp()
    }
    fn latest_timestamp(&self) -> TimeUnixNanoSec {
        self.0.latest_timestamp()
    }
}

impl Fsm for MemoryAccount {
    type TransitionType = AnalyzedTransition<MemoryAccountEvent>;
    fn len(&self) -> usize {
        self.0.len()
    }
    fn transition(&self, index: usize) -> Option<&Self::TransitionType> {
        self.0.transition(index)
    }
}

impl<'a> FsmUsages<'a> for MemoryAccount {
    fn usages_with_state_names(&'a self) -> impl Iterator<Item = (&'a str, impl Usage<'a>)> {
        self.0.usages_with_state_names()
    }
}

impl Using for MemoryAccount {
    fn usages(&self) -> impl Iterator<Item = impl Usage<'_>> {
        self.0.usages()
    }
}

impl FsmTypeDeclaration for MemoryAccount {
    fn fsm_type_declaration() -> FsmTypeDecl {
        let state = |name: &str, usages: &[&str]| FsmStateTypeDecl {
            name: name.to_owned(),
            usages: usages.iter().map(|usage| (*usage).to_owned()).collect(),
        };
        FsmTypeDecl {
            name: "memory_account".to_owned(),
            states: vec![
                state("account_registered", &[]),
                state(
                    "accounted",
                    &["buffer_pool", "temporary_storage", "temporary_directory"],
                ),
                state("exit", &[]),
            ],
            transitions: vec![
                FsmTransitionDecl::Entry("account_registered".to_owned()),
                FsmTransitionDecl::Transition(
                    "account_registered".to_owned(),
                    "accounted".to_owned(),
                ),
                FsmTransitionDecl::Transition("accounted".to_owned(), "accounted".to_owned()),
                FsmTransitionDecl::Transition("accounted".to_owned(), "exit".to_owned()),
                FsmTransitionDecl::Exit("exit".to_owned()),
            ],
        }
    }
}

pub(crate) trait MemoryAccountExt {
    fn memory_tag(&self) -> Option<&str>;
    fn is_valid(&self, engine_id: Uuid, model: &DuckDbModel) -> bool;
    fn matches_operator(&self, filter: &OperatorFilter) -> bool;
}

impl MemoryAccountExt for MemoryAccount {
    fn memory_tag(&self) -> Option<&str> {
        match self.first_data()? {
            MemoryAccountEvent::AccountRegistered { memory_tag, .. } => Some(memory_tag),
            _ => None,
        }
    }

    fn is_valid(&self, engine_id: Uuid, model: &DuckDbModel) -> bool {
        if self.memory_tag().is_none_or(str::is_empty) {
            return false;
        }
        if !matches!(self.first_data(), Some(MemoryAccountEvent::AccountRegistered { engine_id: account_engine, .. }) if account_engine.target == engine_id)
        {
            return false;
        }
        let mut account_resource = None;
        for transition in self.transitions() {
            let MemoryAccountEvent::Accounted {
                buffer_pool,
                temporary_storage,
                temporary_directory,
                ..
            } = &transition.data
            else {
                continue;
            };
            let usages = [
                buffer_pool
                    .as_ref()
                    .map(|resource| (resource.target, crate::model::BUFFER_POOL_MEMORY_TYPE_NAME)),
                temporary_storage
                    .as_ref()
                    .map(|resource| (resource.target, crate::model::TEMPORARY_STORAGE_TYPE_NAME)),
                temporary_directory.as_ref().map(|resource| {
                    (
                        resource.target,
                        crate::model::TEMPORARY_DIRECTORY_STORAGE_TYPE_NAME,
                    )
                }),
            ];
            let mut usages = usages.into_iter().flatten();
            let Some((resource_id, type_name)) = usages.next() else {
                return false;
            };
            if usages.next().is_some()
                || account_resource.is_some_and(|id| id != resource_id)
                || !model.resource_matches(resource_id, type_name, engine_id)
            {
                return false;
            }
            account_resource = Some(resource_id);
        }

        account_resource.is_some()
    }

    fn matches_operator(&self, filter: &OperatorFilter) -> bool {
        filter.operator_ids.is_empty()
    }
}
