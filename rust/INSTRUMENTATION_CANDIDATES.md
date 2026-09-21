# Quent instrumentation candidates

These proposals extend the current model. They are not current telemetry.

## Evidence rule

- **Observed** states what the present DuckDB code exposes.
- **Proposed** defines the Quent meaning.
- **Do not infer** marks claims the hooks cannot support.

Add a candidate only when its identity, lifetime, hook, and cost are explicit.
Prefer one semantic hook over several lower-level hooks for the same work.

## Priority

| Priority | Candidate | Value | Event volume | Main risk |
|---|---|---|---|---|
| P0 | Pipeline and event graph | Critical path and barriers | Low | Stable execution IDs |
| P0 | Task wait reason | Explains blocked time | Medium | Missing reason propagation |
| P0 | Scheduler capacity | Saturation denominator | Low | Caller-thread semantics |
| P1 | Broadcast exchange | Backpressure and fan-out | Medium to high | Chunk identity |
| P1 | Temporary memory grant | Externalization pressure | Low | Grant is not residency |
| P1 | Persistent block I/O | Scan latency and throughput | Medium | Double counting |
| P2 | Block placement | RAM/spill residence | High | Ownership and overhead |
| P2 | Query preparation phases | Parse/bind/optimize latency | Low | Failed-query lifecycle |
| P2 | Checkpoint, WAL, transaction | Maintenance interference | Low to medium | Query attribution |
| P2 | Profile summaries | Cheap aggregate diagnosis | Low | Duplicate metrics |

## P0: pipeline and event graph

**Observed.** `BuildPipelineSchedule` creates initialize, execute,
prepare-finish, finish, and complete stages. `Event::AddDependency`,
`CompleteDependency`, `SetTasks`, `FinishTask`, and `Finish` expose dependency
and task barriers. Hook sites:

- `src/parallel/pipeline_schedule.cpp`
- `src/parallel/executor.cpp`
- `src/parallel/event.cpp`
- `src/parallel/pipeline_*.cpp`

**Proposed.** Declare one `Pipeline` per execution pipeline and one
`PipelineEvent` per scheduled stage. Record source, operator chain, sink,
maximum threads, input mode, dependencies, and external-input links.

```text
Declared → WaitingDependencies → Ready → Running → Complete → Exit
```

Link each `PipelineTask` to its pipeline and execute event. Derive dependency
wait, barrier wait, task fan-out, and the event critical path.

**Cost.** O(pipelines + schedule edges + events), normally far below task and
operator-call volume. Cache IDs on execution-owned pipeline and event objects.

**Do not infer.** A dependency interval is not CPU starvation. An event with
no tasks may still perform meaningful finalization work.

## P0: task wait reason

**Observed.** `PipelineTask::ExecuteTask` distinguishes blocked results, while
sources, sinks, broadcast exchanges, and result collection own the wake paths.
The current `PipelineTask.Blocked` state records no cause. Hook sites:

- `src/parallel/pipeline.cpp`
- `src/parallel/pipeline_executor.cpp`
- `src/parallel/interrupt.cpp`
- `src/parallel/pipeline_broadcast_exchange.cpp`
- `src/parallel/executor.cpp`

**Proposed.** Add a `TaskWait` FSM or typed usage on `Blocked`:

```text
Waiting(reason, resource, operator) → Woken(outcome) → Exit
```

Initial reasons should be explicit enums: source I/O, sink backpressure,
broadcast data, broadcast watermark, result consumer, batch-order dependency,
and unknown. The blocking component must provide the reason and wake outcome.

**Cost.** One entity per blocked interval. Avoid stack inspection and string
construction. Store enum codes and optional typed references.

**Do not infer.** Idle gaps do not reveal a wait reason. A callback means the
task became eligible, not that it ran immediately.

## P0: scheduler capacity

**Observed.** `TaskSchedulerPool::SetThreads` and `RelaunchThreads` manage
regular and external worker threads. Query work may also run on a caller
thread. Hook sites:

- `src/parallel/task_scheduler_pool.cpp`
- `src/parallel/task_scheduler.cpp`
- `src/parallel/task_executor.cpp`

**Proposed.** Model `SchedulerPool` as a resource with a `slots` occupancy
bound and declare scheduler-owned `ExecutionThread` lifetimes at thread start
and exit. Keep caller threads separate.

**Cost.** O(resizes + thread lifetimes). This is low-volume and should precede
CPU-utilization claims.

**Do not infer.** `SET threads=N` is not observed concurrency. Pool capacity
does not include every caller or asynchronous execution path unless each path
is modeled.

## P1: broadcast exchange

**Observed.** `PipelineBroadcastExchange` has direct, buffered, and
materialized consumers; a shared spool; append and read reservations;
high/low watermarks; blocked readers and writers; chunk retirement; and
consumer unregister. Hook sites:

- `src/include/duckdb/parallel/pipeline_broadcast_exchange.hpp`
- `src/parallel/pipeline_broadcast_exchange.cpp`

**Proposed.** Declare `BroadcastExchange`, `ExchangeConsumer`, and
`ExchangeChunk`. Model buffer occupancy as a resource and each consumer claim
as a lifecycle:

```text
Produced → Buffered → Claimed → Read → Retired → Exit
                    ↘ Spooling → SpoolResident ↗
```

Record mode, batch index, rows, logical bytes, active consumers, and watermark
transitions. Attach producer blocking to the exchange resource.

**Cost.** One chunk entity plus up to one claim per consumer. Sampling or
aggregate-only mode is required for large fan-out. Reuse exchange positions or
batch sequence as scoped identities; do not use `DataChunk *`.

**Do not infer.** Logical bytes are not copied bytes. Direct delivery and
buffered publication have different lifetimes. Spool residence is not general
buffer-pool ownership.

## P1: temporary memory grant

**Observed.** `TemporaryMemoryManager::Register`, `SetRemainingSize`, and
`TemporaryMemoryState::UpdateReservation` maintain requested remaining size,
minimum reservation, and granted reservation. Hook site:

- `src/storage/temporary_memory_manager.cpp`

**Proposed.** Model `OperatorMemoryGrant` with request, minimum, grant, resize,
and release states. Propagate query and physical-operator identity from sort,
hash join, aggregate, and window registrations.

**Cost.** O(grant updates), normally low. Coalesce unchanged grants and emit
integer bytes only.

**Do not infer.** A reservation is policy budget, not allocated, resident, or
operator-owned memory. Keep it separate from `MemoryAccount`.

## P1: persistent block I/O

**Observed.** `SingleFileBlockManager::Read`, `ReadBlocks`, and `Write` carry a
`QueryContext` into checksum and file work. Buffer-manager pinning may trigger
these reads. Hook sites:

- `src/storage/single_file_block_manager.cpp`
- `src/storage/standard_buffer_manager.cpp`
- `src/storage/buffer/block_handle.cpp`

**Proposed.** Add `PersistentBlockIo` with operation, block range, requested
bytes, completed bytes, database, query when known, checksum time, and success.
Add a separate prefetch entity only where a request has its own lifetime.

**Cost.** O(physical requests), lower than row or chunk instrumentation.
Instrument one semantic layer. A buffer miss may reference the storage request
but must not emit a second byte total.

**Do not infer.** Filesystem call duration is not device time. The kernel page
cache may satisfy a read. A `QueryContext` identifies the requester, not data
ownership.

## P2: block placement

**Observed.** `BlockHandle` has a stable block ID, load state, reader count,
memory tag, buffer, and allocation charge. `StandardBufferManager::Pin`,
`Unpin`, and eviction paths control residency. Temporary spill/reload events
already observe transfer operations. Hook sites:

- `src/storage/buffer/block_handle.cpp`
- `src/storage/buffer/buffer_handle.cpp`
- `src/storage/standard_buffer_manager.cpp`

**Proposed.** Model one placement per block generation:

```text
Registered → Loaded ↔ Pinned
                ↓       ↓
             Spilling  Unpinned
                ↓       ↓
              Spilled → Reloading → Loaded
                └──────────────────→ Destroyed → Exit
```

Use stationary RAM and temporary-tier byte resources. Reference existing
`TemporaryBlockIo` transitions for transfer time. Reconcile aggregate bytes
against `MemoryAccount`; never add both as independent totals.

**Cost.** Pin/unpin can be hotter than operator invocation. Default to
load/unload/spill/reload/destruction. Enable pin spans only under sampling or
a focused query filter. Define block-generation identity before implementation.

**Do not infer.** General query or operator ownership is unavailable. Eviction
context names the claimant that caused pressure, not the victim's owner.

## P2: query preparation phases

**Observed.** Current `Query` begins only after an executable physical plan is
available. DuckDB separately parses, binds, plans, optimizes, and constructs a
physical plan in `ClientContext` processing.

**Proposed.** Add `QueryAttempt`, separate from executable `Query`:

```text
Received → Parsing → Binding → Optimizing → PhysicalPlanning
         → Executable | Failed | Cancelled → Exit
```

Link successful attempts to `Query`. Record statement type, prepared/cached
status, and structured failure phase without SQL error text by default.

**Cost.** O(statements). This is low-volume.

**Do not infer.** A failed attempt is not an executed query. Planning wall
time is not optimizer CPU time when catalog or extension work occurs inside it.

## P2: checkpoint, WAL, and transaction

**Observed.** Transactions have begin, commit, rollback, cleanup, and WAL
paths. Checkpoint scheduling and storage work can overlap query execution.
Candidate hook sites:

- `src/transaction/`
- `src/transaction/wal_write_state.cpp`
- `src/storage/checkpoint_manager.cpp`
- `src/function/table/checkpoint.cpp`

**Proposed.** Model `Transaction`, `WalFlush`, and `Checkpoint` separately.
Use database-scoped storage-channel resources. Link a query only when DuckDB
provides the transaction or query context at the semantic boundary.

**Cost.** Low for lifecycles; medium if every WAL record is emitted. Prefer
flush batches and checkpoint phases over per-record events.

**Do not infer.** Temporal overlap is not causation. Automatic checkpoints may
serve many transactions.

## P2: profile summaries

**Observed.** DuckDB's profiler already aggregates operator timing,
cardinality, blocked time, peak buffer memory, and temporary size.

**Proposed.** Emit one query statistics event and one operator statistics event
after execution. Use them for reconciliation and low-overhead captures where
fine-grained FSMs are disabled.

**Cost.** O(operators). Avoid serializing duplicate textual profiles or dynamic
maps when fixed schema fields exist.

**Do not infer.** Aggregates cannot reconstruct concurrency, causal order, or
resource overlap. They complement rather than replace runtime entities.

## Admission checklist

Before implementation:

1. Define the entity's physical meaning and non-meaning.
2. Prove stable identity and terminal cleanup on failure.
3. Choose one authoritative hook per transition.
4. Propagate typed scope references without crossing storage-layer boundaries.
5. Estimate events per query and bytes per event.
6. Add analyzer join validation and incomplete-entity exclusion.
7. Benchmark disabled, enabled-empty, and enabled-capture overhead.
8. Reconcile counts or capacities with an existing DuckDB source of truth.
9. Exercise API and UI views on parallel, spilling, and failing queries.
