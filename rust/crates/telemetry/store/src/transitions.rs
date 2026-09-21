//! Analysis mappings for schema-generated FSM events.

use quent_analyzer::{
    fsm::native::TransitionEvent,
    resource::{AnalyzedUsage, CapacityValue},
};
use smallvec::{SmallVec, smallvec};

use crate::{
    BufferPoolMemoryEvent, ChunkTransferEvent, ExecutionThreadEvent, MemoryAccountEvent,
    OperatorInvocationEvent, PipelineTaskEvent, QueryEvent, TaskQueueEvent, TemporaryBlockIoEvent,
    TemporaryDirectoryStorageEvent, TemporaryIoChannelEvent, TemporaryStorageEvent, Uuid,
};

const UNIT_CAPACITY: &str = "unit";
const QUEUE_ENTRIES_CAPACITY: &str = "entries";
const IO_OPERATIONS_CAPACITY: &str = "operations";
const IO_BUFFER_BYTES_CAPACITY: &str = "buffer_bytes";
const MEMORY_BYTES_CAPACITY: &str = "bytes";

fn usage(resource_id: Uuid, capacities: SmallVec<[CapacityValue; 3]>) -> AnalyzedUsage {
    AnalyzedUsage {
        resource_id,
        capacities,
    }
}

impl TransitionEvent for QueryEvent {
    fn sequence(&self) -> u16 {
        match self {
            Self::Init { seq, .. }
            | Self::Planning { seq }
            | Self::Executing { seq }
            | Self::Exit { seq } => *seq,
        }
    }

    fn is_initial(&self) -> bool {
        matches!(self, Self::Init { .. })
    }

    fn is_final(&self) -> bool {
        matches!(self, Self::Exit { .. })
    }

    fn is_valid_next(&self, next: &Self) -> bool {
        match self {
            Self::Init { .. } => matches!(next, Self::Planning { .. }),
            Self::Planning { .. } => matches!(next, Self::Executing { .. }),
            Self::Executing { .. } => matches!(next, Self::Exit { .. }),
            Self::Exit { .. } => false,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Init { .. } => "init",
            Self::Planning { .. } => "planning",
            Self::Executing { .. } => "executing",
            Self::Exit { .. } => "exit",
        }
    }
}

impl TransitionEvent for PipelineTaskEvent {
    fn sequence(&self) -> u16 {
        match self {
            Self::Created { seq, .. }
            | Self::Running { seq, .. }
            | Self::Ready { seq, .. }
            | Self::Blocked { seq }
            | Self::Finalizing { seq, .. }
            | Self::Exit { seq } => *seq,
        }
    }

    fn is_initial(&self) -> bool {
        matches!(self, Self::Created { .. })
    }

    fn is_final(&self) -> bool {
        matches!(self, Self::Exit { .. })
    }

    fn is_valid_next(&self, next: &Self) -> bool {
        match self {
            Self::Created { .. } => {
                matches!(next, Self::Running { .. } | Self::Finalizing { .. })
            }
            Self::Running { .. } => matches!(
                next,
                Self::Ready { .. } | Self::Blocked { .. } | Self::Finalizing { .. }
            ),
            Self::Ready { .. } | Self::Blocked { .. } => {
                matches!(next, Self::Running { .. } | Self::Finalizing { .. })
            }
            Self::Finalizing { .. } => matches!(next, Self::Exit { .. }),
            Self::Exit { .. } => false,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Created { .. } => "created",
            Self::Running { .. } => "running",
            Self::Ready { .. } => "ready",
            Self::Blocked { .. } => "blocked",
            Self::Finalizing { .. } => "finalizing",
            Self::Exit { .. } => "exit",
        }
    }

    fn usages(&self) -> SmallVec<[AnalyzedUsage; 1]> {
        match self {
            Self::Created { queue, .. } | Self::Ready { queue, .. } => smallvec![usage(
                queue.target,
                smallvec![CapacityValue::new(
                    QUEUE_ENTRIES_CAPACITY,
                    queue.data.entries
                )],
            )],
            Self::Running {
                execution_thread, ..
            } => smallvec![usage(
                execution_thread.target,
                smallvec![CapacityValue::new(UNIT_CAPACITY, 1)],
            )],
            Self::Blocked { .. } | Self::Finalizing { .. } | Self::Exit { .. } => SmallVec::new(),
        }
    }
}

impl TransitionEvent for ChunkTransferEvent {
    fn sequence(&self) -> u16 {
        match self {
            Self::Produced { seq, .. } | Self::Published { seq } | Self::Exit { seq } => *seq,
        }
    }

    fn is_initial(&self) -> bool {
        matches!(self, Self::Produced { .. })
    }

    fn is_final(&self) -> bool {
        matches!(self, Self::Exit { .. })
    }

    fn is_valid_next(&self, next: &Self) -> bool {
        match self {
            Self::Produced { .. } => matches!(next, Self::Published { .. }),
            Self::Published { .. } => matches!(next, Self::Exit { .. }),
            Self::Exit { .. } => false,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Produced { .. } => "produced",
            Self::Published { .. } => "published",
            Self::Exit { .. } => "exit",
        }
    }
}

impl TransitionEvent for OperatorInvocationEvent {
    fn sequence(&self) -> u16 {
        match self {
            Self::InvocationCreated { seq, .. }
            | Self::InvocationRunning { seq, .. }
            | Self::InvocationCompleted { seq, .. }
            | Self::Exit { seq } => *seq,
        }
    }

    fn is_initial(&self) -> bool {
        matches!(self, Self::InvocationCreated { .. })
    }

    fn is_final(&self) -> bool {
        matches!(self, Self::Exit { .. })
    }

    fn is_valid_next(&self, next: &Self) -> bool {
        match self {
            Self::InvocationCreated { .. } => matches!(
                next,
                Self::InvocationRunning { .. } | Self::InvocationCompleted { .. }
            ),
            Self::InvocationRunning { .. } => {
                matches!(next, Self::InvocationCompleted { .. })
            }
            Self::InvocationCompleted { .. } => matches!(next, Self::Exit { .. }),
            Self::Exit { .. } => false,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::InvocationCreated { .. } => "invocation_created",
            Self::InvocationRunning { .. } => "invocation_running",
            Self::InvocationCompleted { .. } => "invocation_completed",
            Self::Exit { .. } => "exit",
        }
    }

    fn usages(&self) -> SmallVec<[AnalyzedUsage; 1]> {
        match self {
            Self::InvocationRunning {
                execution_thread, ..
            } => smallvec![usage(
                execution_thread.target,
                smallvec![CapacityValue::new(UNIT_CAPACITY, 1)],
            )],
            _ => SmallVec::new(),
        }
    }
}

impl TransitionEvent for TemporaryBlockIoEvent {
    fn sequence(&self) -> u16 {
        match self {
            Self::IoRequested { seq, .. }
            | Self::IoActive { seq, .. }
            | Self::IoCompleted { seq, .. }
            | Self::Exit { seq } => *seq,
        }
    }

    fn is_initial(&self) -> bool {
        matches!(self, Self::IoRequested { .. })
    }

    fn is_final(&self) -> bool {
        matches!(self, Self::Exit { .. })
    }

    fn is_valid_next(&self, next: &Self) -> bool {
        match self {
            Self::IoRequested { .. } => {
                matches!(next, Self::IoActive { .. } | Self::IoCompleted { .. })
            }
            Self::IoActive { .. } => matches!(next, Self::IoCompleted { .. }),
            Self::IoCompleted { .. } => matches!(next, Self::Exit { .. }),
            Self::Exit { .. } => false,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::IoRequested { .. } => "io_requested",
            Self::IoActive { .. } => "io_active",
            Self::IoCompleted { .. } => "io_completed",
            Self::Exit { .. } => "exit",
        }
    }

    fn usages(&self) -> SmallVec<[AnalyzedUsage; 1]> {
        match self {
            Self::IoActive { channel, .. } => smallvec![usage(
                channel.target,
                smallvec![
                    CapacityValue::new(IO_OPERATIONS_CAPACITY, channel.data.operations),
                    CapacityValue::new(IO_BUFFER_BYTES_CAPACITY, channel.data.buffer_bytes),
                ],
            )],
            _ => SmallVec::new(),
        }
    }
}

impl TransitionEvent for MemoryAccountEvent {
    fn sequence(&self) -> u16 {
        match self {
            Self::AccountRegistered { seq, .. }
            | Self::Accounted { seq, .. }
            | Self::Exit { seq } => *seq,
        }
    }

    fn is_initial(&self) -> bool {
        matches!(self, Self::AccountRegistered { .. })
    }

    fn is_final(&self) -> bool {
        matches!(self, Self::Exit { .. })
    }

    fn is_valid_next(&self, next: &Self) -> bool {
        match self {
            Self::AccountRegistered { .. } => matches!(next, Self::Accounted { .. }),
            Self::Accounted { .. } => {
                matches!(next, Self::Accounted { .. } | Self::Exit { .. })
            }
            Self::Exit { .. } => false,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::AccountRegistered { .. } => "account_registered",
            Self::Accounted { .. } => "accounted",
            Self::Exit { .. } => "exit",
        }
    }

    fn usages(&self) -> SmallVec<[AnalyzedUsage; 1]> {
        let Self::Accounted {
            buffer_pool,
            temporary_storage,
            temporary_directory,
            ..
        } = self
        else {
            return SmallVec::new();
        };

        let mut usages = SmallVec::new();
        usages.extend(buffer_pool.iter().map(|memory| {
            usage(
                memory.target,
                smallvec![CapacityValue::new(MEMORY_BYTES_CAPACITY, memory.data.bytes)],
            )
        }));
        usages.extend(temporary_storage.iter().map(|memory| {
            usage(
                memory.target,
                smallvec![CapacityValue::new(MEMORY_BYTES_CAPACITY, memory.data.bytes)],
            )
        }));
        usages.extend(temporary_directory.iter().map(|memory| {
            usage(
                memory.target,
                smallvec![CapacityValue::new(MEMORY_BYTES_CAPACITY, memory.data.bytes)],
            )
        }));
        usages
    }
}

macro_rules! impl_resource_transitions {
    ($event:ident) => {
        impl TransitionEvent for $event {
            fn sequence(&self) -> u16 {
                match self {
                    Self::Initializing { seq, .. }
                    | Self::Operating { seq, .. }
                    | Self::Resizing { seq }
                    | Self::Finalizing { seq }
                    | Self::Exit { seq } => *seq,
                }
            }

            fn is_initial(&self) -> bool {
                matches!(self, Self::Initializing { .. })
            }

            fn is_final(&self) -> bool {
                matches!(self, Self::Exit { .. })
            }

            fn is_valid_next(&self, next: &Self) -> bool {
                match self {
                    Self::Initializing { .. } => matches!(next, Self::Operating { .. }),
                    Self::Operating { .. } => {
                        matches!(next, Self::Resizing { .. } | Self::Finalizing { .. })
                    }
                    Self::Resizing { .. } => matches!(next, Self::Operating { .. }),
                    Self::Finalizing { .. } => matches!(next, Self::Exit { .. }),
                    Self::Exit { .. } => false,
                }
            }

            fn name(&self) -> &'static str {
                match self {
                    Self::Initializing { .. } => "initializing",
                    Self::Operating { .. } => "operating",
                    Self::Resizing { .. } => "resizing",
                    Self::Finalizing { .. } => "finalizing",
                    Self::Exit { .. } => "exit",
                }
            }
        }
    };
}

impl_resource_transitions!(BufferPoolMemoryEvent);
impl_resource_transitions!(TemporaryStorageEvent);
impl_resource_transitions!(TemporaryDirectoryStorageEvent);

macro_rules! impl_fixed_resource_transitions {
    ($event:ident) => {
        impl TransitionEvent for $event {
            fn sequence(&self) -> u16 {
                match self {
                    Self::Initializing { seq, .. }
                    | Self::Operating { seq }
                    | Self::Finalizing { seq }
                    | Self::Exit { seq } => *seq,
                }
            }

            fn is_initial(&self) -> bool {
                matches!(self, Self::Initializing { .. })
            }

            fn is_final(&self) -> bool {
                matches!(self, Self::Exit { .. })
            }

            fn is_valid_next(&self, next: &Self) -> bool {
                match self {
                    Self::Initializing { .. } => matches!(next, Self::Operating { .. }),
                    Self::Operating { .. } => matches!(next, Self::Finalizing { .. }),
                    Self::Finalizing { .. } => matches!(next, Self::Exit { .. }),
                    Self::Exit { .. } => false,
                }
            }

            fn name(&self) -> &'static str {
                match self {
                    Self::Initializing { .. } => "initializing",
                    Self::Operating { .. } => "operating",
                    Self::Finalizing { .. } => "finalizing",
                    Self::Exit { .. } => "exit",
                }
            }
        }
    };
}

impl_fixed_resource_transitions!(ExecutionThreadEvent);
impl_fixed_resource_transitions!(TaskQueueEvent);
impl_fixed_resource_transitions!(TemporaryIoChannelEvent);

#[cfg(test)]
mod tests {
    use quent_analyzer::fsm::native::TransitionEvent;
    use quent_events::EntityRef;

    use super::*;
    use crate::{
        BufferPoolMemory, BufferPoolMemoryBounds, BufferPoolMemoryUsage, ExecutionThread,
        ExecutionThreadUsage, PipelineTaskEvent, Plan, Query, TaskQueue, TaskQueueUsage,
        TemporaryDirectoryStorage, TemporaryDirectoryStorageUsage, TemporaryIoChannel,
        TemporaryIoChannelUsage, TemporaryStorage, TemporaryStorageUsage, Worker,
    };

    const QUERY_ID: Uuid = Uuid::from_u128(1);
    const PLAN_ID: Uuid = Uuid::from_u128(2);
    const WORKER_ID: Uuid = Uuid::from_u128(3);
    const QUEUE_ID: Uuid = Uuid::from_u128(4);
    const THREAD_ID: Uuid = Uuid::from_u128(5);
    const BUFFER_POOL_ID: Uuid = Uuid::from_u128(7);
    const TEMP_STORAGE_ID: Uuid = Uuid::from_u128(8);
    const TEMP_DIRECTORY_ID: Uuid = Uuid::from_u128(9);

    #[test]
    fn query_requires_complete_order() {
        let init = QueryEvent::Init {
            seq: 0,
            instance_name: "select 42".into(),
            query_group_id: EntityRef::new(Uuid::from_u128(6), ()),
        };
        let planning = QueryEvent::Planning { seq: 1 };
        let executing = QueryEvent::Executing { seq: 2 };
        let exit = QueryEvent::Exit { seq: 3 };

        assert!(init.is_initial());
        assert!(init.is_valid_next(&planning));
        assert!(!init.is_valid_next(&executing));
        assert!(planning.is_valid_next(&executing));
        assert!(executing.is_valid_next(&exit));
        assert!(exit.is_final());
        assert_eq!(exit.sequence(), 3);
    }

    #[test]
    fn task_cycles_preserve_resource_usage() {
        let created = PipelineTaskEvent::Created {
            seq: 0,
            instance_name: "task".into(),
            query_id: EntityRef::<Query>::new(QUERY_ID, ()),
            plan_id: EntityRef::<Plan>::new(PLAN_ID, ()),
            worker_id: EntityRef::<Worker>::new(WORKER_ID, ()),
            operator_ids: Vec::new(),
            task_index: 0,
            queue: EntityRef::<TaskQueue, _>::new(QUEUE_ID, TaskQueueUsage { entries: 1 }),
        };
        let running = PipelineTaskEvent::Running {
            seq: 1,
            mode: "all".into(),
            cpu_id: 0,
            execution_thread: EntityRef::<ExecutionThread, _>::new(THREAD_ID, ExecutionThreadUsage),
        };
        let ready = PipelineTaskEvent::Ready {
            seq: 2,
            queue: EntityRef::<TaskQueue, _>::new(QUEUE_ID, TaskQueueUsage { entries: 1 }),
        };
        let blocked = PipelineTaskEvent::Blocked { seq: 2 };
        let finalizing = PipelineTaskEvent::Finalizing {
            seq: 3,
            success: true,
        };
        let exit = PipelineTaskEvent::Exit { seq: 4 };

        assert!(created.is_valid_next(&running));
        assert!(running.is_valid_next(&ready));
        assert!(running.is_valid_next(&blocked));
        assert!(ready.is_valid_next(&running));
        assert!(blocked.is_valid_next(&running));
        assert!(running.is_valid_next(&finalizing));
        assert!(finalizing.is_valid_next(&exit));

        let queue_usage = created.usages();
        assert_eq!(queue_usage[0].resource_id, QUEUE_ID);
        assert_eq!(queue_usage[0].capacities[0].name, QUEUE_ENTRIES_CAPACITY);
        assert_eq!(queue_usage[0].capacities[0].value, Some(1));

        let thread_usage = running.usages();
        assert_eq!(thread_usage[0].resource_id, THREAD_ID);
        assert_eq!(thread_usage[0].capacities[0].name, UNIT_CAPACITY);
        assert_eq!(thread_usage[0].capacities[0].value, Some(1));
    }

    #[test]
    fn resource_resize_returns_to_operating() {
        let operating = BufferPoolMemoryEvent::Operating {
            seq: 1,
            limits: BufferPoolMemoryBounds { bytes: 1_024 },
        };
        let resizing = BufferPoolMemoryEvent::Resizing { seq: 2 };
        let finalizing = BufferPoolMemoryEvent::Finalizing { seq: 3 };
        let exit = BufferPoolMemoryEvent::Exit { seq: 4 };

        assert!(operating.is_valid_next(&resizing));
        assert!(resizing.is_valid_next(&operating));
        assert!(operating.is_valid_next(&finalizing));
        assert!(finalizing.is_valid_next(&exit));
        assert!(!exit.is_valid_next(&operating));
    }

    #[test]
    fn unbounded_resource_cannot_resize() {
        let operating = TaskQueueEvent::Operating { seq: 1 };
        let finalizing = TaskQueueEvent::Finalizing { seq: 2 };

        assert!(operating.is_valid_next(&finalizing));
        assert!(!finalizing.is_valid_next(&operating));
    }

    #[test]
    fn temporary_io_reports_both_rates() {
        let event = TemporaryBlockIoEvent::IoActive {
            seq: 1,
            channel: EntityRef::<TemporaryIoChannel, _>::new(
                QUEUE_ID,
                TemporaryIoChannelUsage {
                    operations: 1,
                    buffer_bytes: 4_096,
                },
            ),
        };

        let usages = event.usages();
        assert_eq!(usages[0].resource_id, QUEUE_ID);
        assert_eq!(
            usages[0].capacities.as_slice(),
            [
                CapacityValue::new(IO_OPERATIONS_CAPACITY, 1),
                CapacityValue::new(IO_BUFFER_BYTES_CAPACITY, 4_096),
            ]
        );
    }

    #[test]
    fn memory_account_reports_each_domain() {
        let event = MemoryAccountEvent::Accounted {
            seq: 1,
            buffer_pool: Some(EntityRef::<BufferPoolMemory, _>::new(
                BUFFER_POOL_ID,
                BufferPoolMemoryUsage { bytes: 11 },
            )),
            temporary_storage: Some(EntityRef::<TemporaryStorage, _>::new(
                TEMP_STORAGE_ID,
                TemporaryStorageUsage { bytes: 22 },
            )),
            temporary_directory: Some(EntityRef::<TemporaryDirectoryStorage, _>::new(
                TEMP_DIRECTORY_ID,
                TemporaryDirectoryStorageUsage { bytes: 33 },
            )),
        };

        let usages = event.usages();
        assert_eq!(usages.len(), 3);
        assert_eq!(usages[0].resource_id, BUFFER_POOL_ID);
        assert_eq!(usages[0].capacities[0].value, Some(11));
        assert_eq!(usages[1].resource_id, TEMP_STORAGE_ID);
        assert_eq!(usages[1].capacities[0].value, Some(22));
        assert_eq!(usages[2].resource_id, TEMP_DIRECTORY_ID);
        assert_eq!(usages[2].capacities[0].value, Some(33));
    }
}
