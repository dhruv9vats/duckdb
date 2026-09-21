use duckdb_telemetry_store::MemoryAccountEvent;
use quent_analyzer::{
    AnalyzerError, AnalyzerResult, Entity,
    fsm::{Fsm, FsmUsages, Transition, native::TransitionEvent},
    resource::{AnalyzedUsage, CapacityValue, Usage, Using},
};
use quent_events::Event;
use quent_query_engine_ui::OperatorFilter;
use quent_time::{TimeUnixNanoSec, Timestamp, span::SpanUnixNanoSec};
use quent_ui::fsm::{FsmStateTypeDecl, FsmTransitionDecl, FsmTypeDecl, FsmTypeDeclaration};
use smallvec::SmallVec;
use uuid::Uuid;

use crate::model::DuckDbModel;

pub(crate) struct MemoryAccountBuilder {
    id: Uuid,
    events: Vec<Event<MemoryAccountEvent>>,
}

impl MemoryAccountBuilder {
    pub(crate) fn try_new(id: Uuid) -> AnalyzerResult<Self> {
        if id.is_nil() {
            return Err(AnalyzerError::Validation(
                "memory account id cannot be nil".to_owned(),
            ));
        }

        Ok(Self {
            id,
            events: Vec::new(),
        })
    }

    pub(crate) fn push_transition(&mut self, event: Event<MemoryAccountEvent>) {
        self.events.push(event);
    }
}

#[derive(Debug)]
pub(crate) struct MemoryTransition {
    timestamp: TimeUnixNanoSec,
    usages: SmallVec<[AnalyzedUsage; 1]>,
    pub(crate) data: MemoryAccountEvent,
}

impl Timestamp for MemoryTransition {
    fn timestamp(&self) -> TimeUnixNanoSec {
        self.timestamp
    }
}

impl Transition for MemoryTransition {
    fn name(&self) -> &str {
        self.data.name()
    }

    fn sequence(&self) -> u16 {
        self.data.sequence()
    }

    fn is_final(&self) -> bool {
        self.data.is_final()
    }
}

#[derive(Debug)]
pub(crate) struct MemoryAccount {
    id: Uuid,
    transitions: Vec<MemoryTransition>,
    snapshot_bound: Option<TimeUnixNanoSec>,
}

impl MemoryAccount {
    pub(crate) fn try_from_builder(builder: MemoryAccountBuilder) -> AnalyzerResult<Self> {
        Self::build(builder, None)
    }

    pub(crate) fn try_from_snapshot(
        builder: MemoryAccountBuilder,
        watermark: TimeUnixNanoSec,
    ) -> AnalyzerResult<Self> {
        Self::build(builder, Some(watermark))
    }

    fn build(
        mut builder: MemoryAccountBuilder,
        watermark: Option<TimeUnixNanoSec>,
    ) -> AnalyzerResult<Self> {
        builder
            .events
            .sort_by_key(|event| (event.timestamp, event.data.sequence()));
        if let Some(events) = builder.events.windows(2).find(|events| {
            events[0].timestamp == events[1].timestamp
                && events[0].data.sequence() == events[1].data.sequence()
        }) {
            return Err(AnalyzerError::Validation(format!(
                "memory account {} has duplicate transition ({}, {})",
                builder.id,
                events[0].timestamp,
                events[0].data.sequence()
            )));
        }
        let Some(first) = builder.events.first() else {
            return Err(AnalyzerError::IncompleteFsm(format!(
                "memory account {} is empty",
                builder.id
            )));
        };
        if !first.data.is_initial() {
            return Err(AnalyzerError::Validation(format!(
                "memory account {} starts with {}",
                builder.id,
                first.data.name()
            )));
        }
        if let Some(events) = builder
            .events
            .windows(2)
            .find(|events| !events[0].data.is_valid_next(&events[1].data))
        {
            return Err(AnalyzerError::Validation(format!(
                "memory account {} cannot transition from {} to {}",
                builder.id,
                events[0].data.name(),
                events[1].data.name()
            )));
        }

        let last_timestamp = builder.events.last().unwrap().timestamp;
        let is_closed = builder
            .events
            .last()
            .is_some_and(|event| event.data.is_final());
        let snapshot_bound = if is_closed {
            None
        } else {
            let Some(watermark) = watermark else {
                return Err(AnalyzerError::IncompleteFsm(format!(
                    "memory account {} has no final transition",
                    builder.id
                )));
            };
            if watermark < last_timestamp {
                return Err(AnalyzerError::Validation(format!(
                    "memory account {} snapshot watermark {watermark} precedes event {last_timestamp}",
                    builder.id
                )));
            }
            Some(watermark)
        };
        let transitions = builder
            .events
            .into_iter()
            .map(|event| MemoryTransition {
                timestamp: event.timestamp,
                usages: event.data.usages(),
                data: event.data,
            })
            .collect();

        Ok(Self {
            id: builder.id,
            transitions,
            snapshot_bound,
        })
    }

    pub(crate) fn transitions(&self) -> &[MemoryTransition] {
        &self.transitions
    }

    #[cfg(test)]
    pub(crate) fn is_snapshot_bounded(&self) -> bool {
        self.snapshot_bound.is_some()
    }

    pub(crate) fn snapshot_bound(&self) -> Option<TimeUnixNanoSec> {
        self.snapshot_bound
    }

    fn first_data(&self) -> Option<&MemoryAccountEvent> {
        self.transitions.first().map(|transition| &transition.data)
    }
}

impl MemoryTransition {
    pub(crate) fn usages(&self) -> &[AnalyzedUsage] {
        &self.usages
    }
}

impl Entity for MemoryAccount {
    fn id(&self) -> Uuid {
        self.id
    }
    fn type_name(&self) -> &str {
        "memory_account"
    }
    fn earliest_timestamp(&self) -> TimeUnixNanoSec {
        self.transitions.first().unwrap().timestamp()
    }
    fn latest_timestamp(&self) -> TimeUnixNanoSec {
        self.snapshot_bound
            .unwrap_or_else(|| self.transitions.last().unwrap().timestamp())
    }
}

impl Fsm for MemoryAccount {
    type TransitionType = MemoryTransition;
    fn len(&self) -> usize {
        self.transitions.len().saturating_sub(1)
    }
    fn transition(&self, index: usize) -> Option<&Self::TransitionType> {
        self.transitions.get(index)
    }
}

struct MemoryUsage<'a> {
    account_id: Uuid,
    usage: &'a AnalyzedUsage,
    span: SpanUnixNanoSec,
}

impl<'a> Usage<'a> for MemoryUsage<'a> {
    fn entity_id(&self) -> Uuid {
        self.account_id
    }

    fn resource_id(&self) -> Uuid {
        self.usage.resource_id
    }

    fn capacities(&self) -> impl Iterator<Item = &'a CapacityValue> {
        self.usage.capacities.iter()
    }

    fn span(&self) -> SpanUnixNanoSec {
        self.span
    }
}

impl<'a> FsmUsages<'a> for MemoryAccount {
    fn usages_with_state_names(&'a self) -> impl Iterator<Item = (&'a str, impl Usage<'a>)> {
        self.transitions
            .iter()
            .zip(self.transition_ends())
            .flat_map(move |(transition, end)| {
                let span = SpanUnixNanoSec::try_new(transition.timestamp(), end).unwrap();
                transition.usages.iter().map(move |usage| {
                    (
                        transition.name(),
                        MemoryUsage {
                            account_id: self.id,
                            usage,
                            span,
                        },
                    )
                })
            })
    }
}

impl Using for MemoryAccount {
    fn usages(&self) -> impl Iterator<Item = impl Usage<'_>> {
        self.transitions
            .iter()
            .zip(self.transition_ends())
            .flat_map(move |(transition, end)| {
                let span = SpanUnixNanoSec::try_new(transition.timestamp(), end).unwrap();
                transition.usages.iter().map(move |usage| MemoryUsage {
                    account_id: self.id,
                    usage,
                    span,
                })
            })
    }
}

impl MemoryAccount {
    fn transition_ends(&self) -> impl Iterator<Item = TimeUnixNanoSec> + '_ {
        self.transitions
            .iter()
            .skip(1)
            .map(Timestamp::timestamp)
            .chain(self.snapshot_bound)
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
