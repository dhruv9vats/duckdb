// SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Backing types for generated FSM instrumentation handles.

use crate::{AnyEntity, EntityRef, HandleInner, InstrumentedEntity, Uuid};

/// Assigns the transition sequence carried by an FSM event.
#[doc(hidden)]
pub trait FsmEvent {
    /// Replaces the event's transition sequence.
    fn set_sequence(&mut self, sequence: u16);
}

#[derive(Debug, Default)]
struct SequenceCounter(u16);

impl SequenceCounter {
    fn advance(self) -> (u16, Self) {
        (self.0, Self(self.0.wrapping_add(1)))
    }
}

/// Owns the runtime state of a generated FSM handle.
#[doc(hidden)]
pub struct FsmHandleInner<E: InstrumentedEntity> {
    handle: HandleInner<E>,
    sequence: SequenceCounter,
}

impl<E: InstrumentedEntity> From<HandleInner<E>> for FsmHandleInner<E> {
    fn from(handle: HandleInner<E>) -> Self {
        Self {
            handle,
            sequence: SequenceCounter::default(),
        }
    }
}

impl<E> FsmHandleInner<E>
where
    E: InstrumentedEntity,
    E::Event: FsmEvent,
{
    /// Assigns the next sequence number, emits `event`, and advances the
    /// wrapping counter.
    pub fn transition(mut self, mut event: E::Event) -> Self {
        let (sequence, next) = self.sequence.advance();
        event.set_sequence(sequence);
        self.handle.emit(event);
        self.sequence = next;
        self
    }
}

impl<E: InstrumentedEntity> FsmHandleInner<E> {
    /// Returns the entity instance ID.
    pub fn uuid(&self) -> Uuid {
        self.handle.uuid()
    }

    /// Returns a typed reference to this instance carrying no data.
    pub fn as_entity_ref(&self) -> EntityRef<E> {
        self.handle.as_entity_ref()
    }

    /// Returns a typed reference to this instance carrying `data`.
    pub fn as_entity_ref_with<T>(&self, data: T) -> EntityRef<E, T> {
        self.handle.as_entity_ref_with(data)
    }

    /// Returns an untyped reference to this instance carrying no data.
    pub fn as_any_entity_ref(&self) -> EntityRef<AnyEntity> {
        self.handle.as_any_entity_ref()
    }

    /// Returns an untyped reference to this instance carrying `data`.
    pub fn as_any_entity_ref_with<T>(&self, data: T) -> EntityRef<AnyEntity, T> {
        self.handle.as_any_entity_ref_with(data)
    }
}

#[cfg(test)]
mod tests {
    use super::SequenceCounter;

    #[test]
    fn sequence_counter_starts_at_zero_and_advances() {
        let (sequence, counter) = SequenceCounter::default().advance();
        assert_eq!(sequence, 0);
        let (sequence, _) = counter.advance();
        assert_eq!(sequence, 1);
    }

    #[test]
    fn sequence_counter_wraps() {
        let (sequence, counter) = SequenceCounter(u16::MAX).advance();
        assert_eq!(sequence, u16::MAX);
        let (sequence, _) = counter.advance();
        assert_eq!(sequence, 0);
    }
}
