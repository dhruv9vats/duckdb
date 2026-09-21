# DuckDB Quent telemetry handoff

> Agent-only historical record. It predates the YAML migration and contains
> obsolete crate, revision, and generated-API details. Use
> [AGENT_CONTEXT.md](AGENT_CONTEXT.md) for the current architecture. Human
> documentation starts at [README.md](README.md).

Updated: 2026-08-25

This is the self-contained handoff for the DuckDB Quent work. It records the
design, implementation, semantics, evidence, rejected interpretations, known
limits, and next work established during the full investigation.

Read `/home/dvats/repos/duckdb/AGENTS.md` before changing code. Preserve the
dirty worktree. Do not regenerate, reset, or discard changes unless their
ownership is known.

## Current decision

`MemoryAccount` is the sole source of truth for byte occupancy.

- It defines how many bytes each modeled memory or storage resource holds.
- A future `BlockPlacement` may explain part of those totals.
- `BlockPlacement` must not replace, redefine, or be added to
  `MemoryAccount` totals.
- DuckDB cannot currently assign general memory ownership to a query, task, or
  physical operator. Causal attribution is possible at some transitions, but
  causality is not ownership.

This distinction is the starting constraint for all future memory work.

## Repository state

The telemetry changes are uncommitted. Relevant new or modified areas include:

```text
CMakeLists.txt
rust/
src/include/duckdb/main/telemetry_context.hpp
src/include/duckdb/storage/memory_usage_probe.hpp
src/include/duckdb/storage/buffer/buffer_pool.hpp
src/include/duckdb/storage/standard_buffer_manager.hpp
src/include/duckdb/storage/temporary_file_manager.hpp
src/main/database.cpp
src/main/telemetry_context.cpp
src/parallel/pipeline.cpp
src/parallel/pipeline_executor.cpp
src/storage/buffer/buffer_pool.cpp
src/storage/standard_buffer_manager.cpp
src/storage/temporary_file_manager.cpp
test/common/test_buffer_pool_reservation.cpp
```

The maintained user documents are:

- [README.md](README.md): build, capture, server, UI, and API overview.
- [INSTRUMENTATION.md](INSTRUMENTATION.md): entity and resource semantics.
- [QUERY_COOKBOOK.md](QUERY_COOKBOOK.md): copy-paste generated and TPC-H
  workloads.
- This file: architectural history and continuation context.

## Goal

The system maps DuckDB's query-engine domain into Quent, emits typed events
from C++, reconstructs them in Rust, and powers plan, entity, resource, and
data-flow views in the Quent UI.

```text
Rust Quent model
       |
       | bridge/build.rs code generation
       v
Generated CXX observers and handles
       |
       | DuckDB lifecycle hooks
       v
Quent event stream
       |
       | DuckDB analyzer reconstruction and validation
       v
Query bundle / entities / timelines / data flow
       |
       v
Quent UI
```

The model is the event schema. Native code owns lifecycle placement. The
analyzer is a separate semantic boundary: it validates joins, filters invalid
or incomplete entities, derives usages, and adapts results to UI APIs.

## Sirius and Quent findings

The initial reference was:

```text
/home/dvats/repos/sirius/rust/crates/telemetry
/home/dvats/repos/sirius/src/telemetry/telemetry_context.cpp
```

Sirius maps an execution pipeline to one Quent `Operator`; its label contains
the physical-operator chain. DuckDB instead maps one `PhysicalOperator` to one
Quent `Operator`. This gives DuckDB an actual operator-level physical plan.

The Quent UI graph contract is stricter than the declaration model:

- A `Plan` contains directed edges.
- An edge references source and target `Port` UUIDs, not operator UUIDs.
- Each `Port` references its owning `Operator`.
- The current UI creates visible nodes while walking plan edges.
- Operator declarations without connected edges do not render.

DuckDB therefore includes `RESULT_COLLECTOR`, traverses virtual
`PhysicalOperator::GetChildren()`, and emits:

```text
upstream child output port ---> downstream parent input port
```

One source output port can feed several edges. Every input relation receives a
distinct target port. `parent_operator_ids` remains empty for a single physical
plan because it describes cross-plan derivation, not adjacency.

Do not traverse only the public `children` member. Overrides expose children
for result collectors, executes, delimiter joins, and positional scans. MERGE
INTO action operators also need their existing special traversal because they
are outside ordinary `GetChildren()` relationships.

Prepared physical plans are reusable. Execution UUIDs must never be stored on
`PhysicalOperator` objects. Each execution receives fresh plan, operator, port,
task, invocation, and transfer UUIDs.

The Quent revision used by the harness is:

```text
2a5ca83442953cbea1c9c53560808104f6b59127
```

## Build and runtime switches

Three controls have different roles:

| Control | Meaning |
|---|---|
| `BUILD_QUENT_TELEMETRY` | CMake option that builds, generates, and links the Rust bridge. |
| `DUCKDB_QUENT_TELEMETRY` | C++ compile definition guarding native hooks. |
| `QUENT_EXPORTER` | Runtime exporter selection; no capture occurs without it. |

The current tree explicitly defaults `BUILD_QUENT_TELEMETRY` to `TRUE`.
`duckdb_main` and `duckdb_parallel` both require
`DUCKDB_QUENT_TELEMETRY`; missing it on `duckdb_parallel` silently compiles
runtime hot hooks as no-ops.

Telemetry-disabled runtime hooks are inline no-ops. This avoids out-of-line
calls in normal pipeline execution.

Generated bridge headers are build products, for example:

```cpp
#include "duckdb-telemetry-bridge/gen/engine.rs.h"
```

The bridge target generates and exports its include directory through CMake.
The generated `gen/` and `include/` directories are ignored. An absolute
`.clangd` include path was used as a local editor workaround; it is not a
portable build solution.

Runtime exporters:

```text
QUENT_EXPORTER=none|ndjson|msgpack|postcard|collector
QUENT_OUTPUT_DIR=<directory>
QUENT_COLLECTOR_ADDRESS=<HTTP collector endpoint>
QUENT_COLLECTOR_BIND_ADDRESS=<server bind address>
```

The collector endpoint and server bind address deliberately use separate
variables because one is an HTTP URI and the other is a socket address.

## Rust workspace

```text
rust/crates/telemetry/model     Quent model declarations
rust/crates/telemetry/bridge    CXX code generation and Rust/C++ bridge
rust/crates/telemetry/analyzer  event reconstruction and UI analyzer
rust/crates/telemetry/server    collector, analyzer HTTP server, embedded UI
```

Principal model files:

```text
model/src/lib.rs
model/src/runtime_resource.rs
model/src/pipeline_task.rs
model/src/operator_invocation.rs
model/src/chunk_transfer.rs
model/src/temporary_block_io.rs
model/src/memory_account.rs
```

The analyzer has corresponding entity modules plus `model.rs` and `lib.rs`.
`bridge/build.rs` derives generated native types from `DuckDbModel`.

The original harness design used four telemetry crates only: model, bridge,
analyzer, and server. Keep that separation unless a new responsibility cannot
fit one of them. The bridge uses Corrosion for CMake/Rust integration and CXX
for generated bindings. The local Sirius lock used Corrosion `v0.6.1` and CXX
`1.0.199`; inspect the current lock before changing either dependency.

DuckDB compiles as C++17. Do not copy Sirius's C++20 designated initializers.
Default-construct generated bridge structs, assign fields, then move them.
Pass `*context` to generated observer creation. Use UUIDv7 for entity IDs and a
nil UUID where the generated Quent model encodes an absent optional parent.

### Implementation map

| Path | Responsibility |
|---|---|
| `CMakeLists.txt` | bridge build, link, compile definitions |
| `rust/crates/telemetry/model/src/lib.rs` | complete DuckDB model registration |
| `rust/crates/telemetry/bridge/build.rs` | generated CXX bridge |
| `rust/crates/telemetry/analyzer/src/model.rs` | event ingestion, joins, resources, snapshots |
| `rust/crates/telemetry/analyzer/src/lib.rs` | query bundle and UI APIs |
| `rust/crates/telemetry/server` | event import, HTTP, embedded UI |
| `src/include/duckdb/main/telemetry_context.hpp` | narrow native facade and disabled no-ops |
| `src/main/telemetry_context.cpp` | Quent handles, lifecycle, registries, probe implementations |
| `src/main/client_context.cpp` | query/plan/execution lifecycle hooks |
| `src/parallel/pipeline.cpp` | PipelineTask lifecycle |
| `src/parallel/pipeline_executor.cpp` | invocations and chunk publications |
| `src/include/duckdb/storage/temporary_io_probe.hpp` | storage-layer temporary-I/O abstraction |
| `src/include/duckdb/storage/memory_usage_probe.hpp` | storage-layer occupancy abstraction |
| `src/storage/buffer/buffer_pool.cpp` | canonical buffer-pool charge and limit notifications |
| `src/storage/standard_buffer_manager.cpp` | temporary operations and live-representation accounting |
| `src/storage/temporary_file_manager.cpp` | directory extent and swap-limit accounting |
| `test/common/test_buffer_pool_reservation.cpp` | native accounting probe regression |

## Structural query-engine mapping

| Quent entity | DuckDB meaning | Lifetime |
|---|---|---|
| Engine | `DatabaseInstance` | database initialization to teardown |
| Worker | local execution backend | inside Engine lifetime |
| QueryGroup | `ClientContext`/connection | lazily declared per context |
| Query | one executable statement | execution start to drained completion |
| Plan | one execution-specific physical plan | within Query |
| Operator | one unique `PhysicalOperator` | within Plan |
| Port | one physical input/output attachment | within Operator |
| Edge | child output to parent input | declared by Plan |

One Worker represents the local execution backend, not one scheduler thread.
Physical execution can move across regular workers, async workers, and caller
threads. Those lanes are separate resources.

One QueryGroup represents a connection/session. It is declared lazily because
the `ClientContext` constructor runs before `ConnectionManager` assigns its
connection identifier.

Structural declaration fields follow these semantics:

- Plan instance: `physical`.
- Plan parent: current Query UUID; parent Plan is nil.
- Plan Worker: local Worker UUID and never nil.
- Operator type: stable `PhysicalOperatorToString(op.type)` taxonomy.
- Operator instance: `op.GetName()` for specialized scan/function names.
- Operator parent IDs: empty for this physical-only plan.
- Ports: operator UUID plus input/output instance name.
- Edges: source and target Port UUIDs.

The runtime plan registry interns each `PhysicalOperator *` before traversal so
shared subplans emit once. The pointer is only a live native lookup key. Quent
UUIDs are the exported identity.

Query creation is delayed until execution can start. Creating a Quent query at
DuckDB `QueryBegin` caused failed binding or planning to emit the invalid FSM:

```text
Init -> Planning -> Exit
```

The base Quent query model only permits Exit from Executing. The implemented
order is:

```text
successful statement
  -> Query Init
  -> Planning
  -> physical plan declarations
  -> Executing
  -> DuckDB task drain and transaction finalization
  -> Exit
```

Plan emission occurs in `ClientContext::PendingPreparedStatementInternal`
after the execution-specific result collector exists and before
`Executor::Initialize`. At that point the Query UUID exists, the actual root is
known, and no scheduled task can race plan declaration.

## Native architecture

`TelemetryContext` is a narrow DuckDB facade with its implementation in
`src/main/telemetry_context.cpp`. Generated Rust headers stay out of public
DuckDB structures and low-level storage.

Query-local telemetry lives in a private `ClientContextState`. It retains the
query handle, runtime plan registry, task handles, invocation handles, and
physical object-to-Quent UUID mappings until query completion.

Storage cannot call the main layer directly. Low-level accounting and I/O use
abstract probe interfaces defined in the storage layer. `DatabaseInstance` is
the composition root and injects the Quent implementation.

```text
storage code -> storage probe interface -> TelemetryContext implementation
```

No storage file includes generated Quent headers. Probe calls are fail-open and
must not change query or storage behavior.

## Runtime entities

### PipelineTask

One `PipelineTask` represents one scheduled executor of a DuckDB pipeline. It
does not represent a physical operator or a single data chunk. One partial
execution slice is bounded by DuckDB's execution budget; a task may yield and
resume several times, possibly on another thread.

```text
Created -> Running
Created -> Finalizing
Running -> Ready -> Running
Running -> Blocked -> Running
Running/Ready/Blocked -> Finalizing -> Exit
```

Fields include query, plan, worker, the pipeline's physical operator UUIDs,
task index, execution mode, outcome, and sampled CPU identifier.

Resources:

- `Created` and `Ready` use `TaskQueue` with one runnable entry.
- `Running` uses one `ExecutionThread` unit.
- `Blocked` uses neither because a typed wait resource is not modeled yet.

The queue lane is an approximation of runnable backlog. Creation precedes the
initial enqueue slightly. Ready precedes the yielded task's scheduler handoff,
and a caller may retain work locally depending on scheduler settings. Do not
call it exact queue occupancy.

`cpu_id` is sampled at dispatch. On Linux it is based on `sched_getcpu()`; a
thread can migrate during a long slice. It is not a CPU-core resource or a CPU
occupancy span.

Task success is emitted only after `event->FinishTask()` succeeds. Exceptions,
cancellation, and destruction use failure finalization. Registries are detached
before terminal bridge calls so telemetry errors cannot leave stale pointer
mappings.

Operator filtering includes a task when its pipeline contains the selected
operator. The whole task interval is then highlighted. It expresses pipeline
association, not exact time spent inside that operator.

### OperatorInvocation

One `OperatorInvocation` represents one physical operator call bracketed by
`PipelineExecutor` profiling hooks:

- source `GetData`
- intermediate `Execute`
- sink `Sink`
- caching operator `FinalExecute`

```text
InvocationCreated -> InvocationRunning -> InvocationCompleted -> Exit
```

It records query, plan, task, operator, phase, input/output rows, input/output
logical bytes, success, and execution thread usage.

Invocation duration is wall time, not CPU time. It includes synchronous work
inside the call. It does not currently cover `Combine`, `NextBatch`,
`UpdateMinBatchIndex`, every auxiliary executor task, or all source-finish
notifications.

Operator filtering is exact for this entity. Valid invocation spans also extend
the physical operator's active span in the query bundle.

An unfiltered execution-thread timeline must not sum nested task and invocation
usage. The analyzer uses `PipelineTask` as its canonical plain occupancy layer;
typed invocation lanes provide detailed work. This fixed an earlier 2x unit
overcount.

### ChunkTransfer

`ChunkTransfer` is historically named. Its precise meaning is a non-empty chunk
publication across a declared physical-plan edge.

```text
Produced -> Published -> Exit
```

It records query, optional task, source/target operator and port UUIDs, rows,
and logical bytes. A fresh UUID identifies each publication occurrence.

Emission occurs after producer output exists and before the downstream
operator or sink consumes it. It therefore does not mean:

- delivery or consumer acceptance
- a queue residence interval
- memcpy, DMA, PCIe, or storage traffic
- unique allocation or resident memory
- a stable data-batch lineage

The name `Published` replaced the incorrect `Delivered` claim.

`logical_bytes` comes from `DataChunk::GetDataSize()`. It estimates uncompressed
logical vector data. Shared or referenced buffers can be counted at multiple
edges. It is not allocation size, resident size, or physical bandwidth.

Current hooks cover source output, externally pushed source input, intermediate
output, and caching `FinalExecute` output. Sink retries do not duplicate the
publication. External-input execution can have a nil task ID.

Safe analysis includes per-edge chunk, row, and logical-byte totals/rates;
average chunk cardinality; approximate fill; bytes per row; operator
selectivity/amplification; task contribution; and burstiness. It cannot derive
copy behavior, unique bytes, storage I/O, consumer latency, lineage, or tier
residency.

### DataChunk versus Sirius DataBatch

DuckDB `DataChunk` is the closest execution-batch analog. It is a set of
same-cardinality column vectors, normally up to `STANDARD_VECTOR_SIZE` rows.
It is not a stable Sirius-style batch object:

- PipelineExecutor allocates reusable chunk slots.
- `Reset` retains allocations for the next occurrence.
- vectors can own, share, or reference upstream buffers.
- filter and projection paths may avoid copying.
- a pointer identifies a reusable slot, not logical contents.

A future lineage model must mint a semantic occurrence ID and define explicit
propagation for pass-through, transform, split, merge, fan-out, and
materialization. Never use a `DataChunk` address as a batch ID.

Sirius also has `BatchPlacement`: one physical batch multiplied by one consumer
input port. It tracks queued, packaged into a task, processing, consumed, and
memory-tier usage. DuckDB pipeline edges are usually synchronous calls through
reused `DataChunk` slots, not consumer repositories. Copying the Sirius entity
would invent queue residence and stable batch ownership. DuckDB's current
`ChunkTransfer` models the honest publication event; the proposed
`BlockPlacement` instead models stable storage-buffer location.

### TemporaryBlockIo

One `TemporaryBlockIo` represents one synchronous
`StandardBufferManager::WriteTemporaryBuffer` or `ReadTemporaryBuffer` call.

```text
IoRequested -> IoActive -> IoCompleted -> Exit
IoRequested -> IoCompleted -> Exit      start failure
```

It records:

- query, plan, optional task, and causal trigger operator
- DuckDB block identifier and `MemoryTag`
- spill or reload direction
- page-aligned buffer bytes presented to the service
- resulting stored representation bytes
- success

The trigger query/operator caused the operation. For a spill it may be
evicting a victim created or used elsewhere. Never call it block ownership.

`buffer_bytes` is the uncompressed page-aligned buffer representation, not
logical query payload. `storage_bytes` is the stored slot or variable-file
representation, not directory extent.

The active state uses one of two Worker-child rate resources:

```text
temporary-spill
temporary-reload
```

Each `TemporaryIoChannel` reports operations/s and buffer bytes/s. It is a
channel because it represents work moving a representation through a
synchronous service path. It is utilized like a service or bus, not occupied
like RAM. Duration includes compression, encryption, locks, file-system calls,
and management overhead. It is not raw device or PCIe bandwidth.

The occupancy counterparts are `TemporaryStorage` and
`TemporaryDirectoryStorage`, driven by `MemoryAccount`. Channel and tier answer
different questions:

```text
channel: how much spill/reload work happened per second?
tier:    how many bytes were held over time?
```

The pinned Quent rate builder produces per-nanosecond values while the UI labels
per-second. The DuckDB analyzer applies a scoped `1e9` conversion only to
`TemporaryIoChannel` rates. Do not apply it to occupancy or data-flow series.

## Runtime resources

```text
Engine
├─ buffer-pool-memory              occupancy
├─ temporary-storage               occupancy
├─ temporary-directory-storage     occupancy
└─ Worker: local
   ├─ runnable-pipeline-tasks       approximate runnable entries
   ├─ thread-*                      unit execution lanes
   ├─ temporary-spill              operations/s and buffer bytes/s
   └─ temporary-reload             operations/s and buffer bytes/s
```

`ExecutionThread` is an observed OS-thread lane. It is not a CPU core. A task
can resume on another lane. Current resources are created lazily when observed;
a future scheduler-owned lifecycle could predeclare idle workers and distinguish
regular, async, and external roles.

## DuckDB memory architecture

DuckDB has several related but non-equivalent measurements.

### Buffer-pool accounting

`BufferPoolReservation` owns an accounting charge. Resizing the reservation
calls `BufferPool::UpdateUsedMemory(MemoryTag, delta)`. That is the authoritative
memory-limit accounting boundary.

It includes more than loaded storage blocks:

- `BlockMemory` reservations
- allocator charges
- object cache reservations
- extension reservations
- other explicitly charged components

It excludes unmanaged allocation, thread stacks, Rust telemetry, kernel page
cache, and other process memory outside DuckDB accounting. It is not RSS.

### Block residency

`BlockMemory` is the stable buffer-manager object for a block representation.
It knows block ID, load state, readers/pins, memory tag, buffer type, allocation
charge, and destroy/persist policy.

`BufferHandle` is a move-only RAII pin. Pin loads an unloaded block if needed
and increments readers. Unpin decrements readers; reaching zero makes a block
evictable or releases it according to policy. Pinned blocks cannot be unloaded.

Eviction can:

- discard a destroyable intermediate
- drop a persistent block and reload it from the database later
- write a non-destroyable temporary representation, then unload RAM

This lifecycle can support diagnostic placement, but it covers only
buffer-managed blocks. It cannot reconstruct all buffer-pool charges.

### Live temporary representations

`StandardBufferManager` tracks live evicted representations per `MemoryTag`.
Fixed temporary blocks may be compressed into grouped temporary files.
Variable-size buffers use separate files.

These bytes mean currently live temporary representations. They are not bytes
transferred and are not necessarily equal to directory extent.

### Temporary-directory extent

`TemporaryFileManager` tracks DuckDB-accounted file extent charged to the swap
limit. Grouped files can retain and reuse interior slots. Variable files include
their own layout. Therefore:

```text
sum(live temporary representations) != directory extent
```

Directory extent is not filesystem allocated sectors, kernel page cache, or
device-resident bytes.

### Temporary-memory reservations

`TemporaryMemoryManager` grants budgets to blocking/external algorithms such
as sort and hash join. Desired, minimum, and granted reservation describe
admission policy and the choice between in-memory and external execution. They
are not actual resident bytes. This remains a useful future entity, but it must
not be combined with occupancy.

## MemoryAccount: canonical occupancy

`MemoryAccount` is a database-lived, absolute gauge FSM:

```text
AccountRegistered { memory_tag }
        |
        v
Accounted { exactly one resource usage }
        |
        +---- Accounted { new absolute bytes }
        |
        v
Exit
```

There are 33 accounts:

- 16 accounted `MemoryTag` values on `BufferPoolMemory`.
- 16 accounted `MemoryTag` values on `TemporaryStorage`.
- one `UNKNOWN` account on `TemporaryDirectoryStorage`.

`UNKNOWN` is deliberate for directory extent because the file-manager layer no
longer retains a per-block `MemoryTag` decomposition.

The three resources are resizable occupancy resources:

| Resource | Authoritative measurement | Capacity |
|---|---|---|
| `buffer-pool-memory` | bytes charged against DuckDB's memory limit, by tag | current memory limit |
| `temporary-storage` | live evicted representations, by tag | current maximum temporary size when known |
| `temporary-directory-storage` | DuckDB-accounted directory extent | current maximum temporary size when known |

The core invariant is:

```text
buffer-pool charge
    != live temporary representation bytes
    != temporary-directory extent
```

They can overlap during reload or spill transitions and answer different
questions. Do not force equality.

### Native source of truth

The storage abstraction is
`src/include/duckdb/storage/memory_usage_probe.hpp`:

```text
BufferPoolSnapshot(tag, bytes)
BufferPoolDelta(tag, signed bytes)
BufferPoolLimit(bytes)
TemporaryStorageDelta(tag, signed bytes)
TemporaryStorageLimit(optional bytes)
TemporaryDirectoryDelta(signed bytes)
```

`BufferPool::UpdateUsedMemory` drives the first domain. Existing manual release
paths, including `FreeReservedMemory`, route through it.

StandardBufferManager mutations drive live temporary bytes. Variable-size live
temporary accounting starts only after a successful write, preventing a failed
spill from creating false occupancy.

TemporaryFileManager mutations drive directory extent. Limit events are emitted
only after successful limit changes.

### Ordering and concurrency

BufferPool observers are weak because a pool may be shared. Registration and
updates are linearized:

- registration publishes the observer under the probe lock before snapshots
- a zero-observer update already in flight rechecks and reconciles an absolute
  post-update snapshot
- active mutations and delta delivery share the registration lock
- a second observer receives a consistent baseline, then ordered deltas

With no observer, the BufferPool hot path takes no mutex. Enabled accounting
serializes notification order, not DuckDB execution generally.

Live-temporary and directory-extent mutation/callback pairs have separate
probe-only locks so concurrent signed deltas cannot be observed out of order.

Probe entrypoints are `noexcept` and catch bridge failures. Telemetry must never
turn allocation, eviction, spill, reload, or teardown into a query failure.

### Lifetime

Memory resources are Engine children. Account handles exit before their
resources. Resources exit before Worker and Engine. Database teardown destroys
the standard buffer manager before TelemetryContext, so final decrements are
captured.

Resources are resizable. Successful `SET memory_limit` and temporary-limit
changes emit `Resizing -> Operating` transitions with the new capacity.

### Analyzer behavior

- Memory accounts are timeline-only.
- `/entities` returns an empty memory-account list.
- The reason is structural: accounts begin before a Query epoch, so generic
  query-relative entity conversion cannot safely represent their full FSM.
- Timelines clip each account usage to the selected `Query.span`.
- Even if an API caller requests a window past query completion, post-query
  bins are zero.
- Untyped views sum valid tags.
- Typed views key series by `memory_tag`.
- Unused tags can appear as zero series because every account starts at zero.
- Any non-empty physical-operator filter removes memory-account usage.

The last rule prevents false ownership. Query selection defines only the time
window over which the database-wide gauge is observed.

### Shared and custom managers

A shared BufferPool is one accounting domain. Each registered DatabaseInstance
currently observes the entire shared pool, not a database-owned fraction.
Temporary-storage and directory gauges remain per StandardBufferManager.

A custom `DBConfig.buffer_manager` does not expose the StandardBufferManager
probe contract. The current initialization skips these memory resources and
accounts rather than presenting false zero lanes.

### Meaning in the UI

Memory timelines answer:

- how close DuckDB-accounted memory came to its configured limit
- which `MemoryTag` categories dominated pressure
- when live temporary representations grew and shrank
- how directory extent evolved independently of live spill data
- how these curves align in time with task, operator, chunk, and I/O activity

They do not answer which query or operator owns a byte, how many physical pages
the process occupies, or which individual block contributed to a peak.

## Why BlockPlacement is still useful

Lack of ownership does not prevent placement modeling. Ownership asks:

```text
Who should be charged for this memory?
```

Placement asks:

```text
Where is this specific buffer-managed representation, and can it be evicted?
```

DuckDB has enough state for the second question. A `BlockPlacement` can reveal
pinned versus evictable memory, spill/reload churn, residence durations, and
hot repeatedly loaded blocks without claiming query ownership.

It remains secondary because:

- it covers `BlockMemory`, not allocator, extension, or object-cache charges
- temporary files and live representations are not one-to-one with directory
  extent
- a block can outlive a query and be reused by later queries
- an evicting query may not own the victim

## Proposed BlockPlacement design: start here

This is a design only. It has not been implemented.

### Identity

Create one entity per `BlockMemory` lifetime. Mint a Quent UUID at registration.
Do not export the C++ address as identity. A pointer may key the live native
registry only while the object exists; erase it before destruction so address
reuse cannot alias two placements.

Retain DuckDB's block ID as a diagnostic attribute. It is not a globally stable
telemetry identity across databases and lifetimes.

### Initial FSM

```text
Registered
   |
   +--> PersistentUnloaded --> Loading --> ResidentPinned
   |
   +--> ResidentPinned <--> ResidentEvictable
                              |
                              +--> Spilling --> TemporaryStored
                              |                    |
                              |                    +--> Reloading
                              |                           |
                              |                           v
                              |                     ResidentPinned
                              |
                              +--> Discarded

PersistentUnloaded <--> Loading/ResidentPinned

any live state --> Destroyed --> Exit
```

Refine names against actual DuckDB policies before modeling. Likely distinctions
are needed for:

- initially loaded transient blocks
- registered but unloaded persistent blocks
- temporary non-destroyable blocks
- destroyable intermediate blocks
- persistent blocks that can be dropped and reread
- conversion from temporary to persistent

Transitions must reflect completed storage state, not requested operations.
Failed write/read must leave the old placement state intact.

### Attributes

Entry or stable attributes:

- placement UUID
- DuckDB block ID
- `MemoryTag`
- `FileBufferType`
- allocation bytes
- destroy/persist policy
- initial load state

Transition attributes where available:

- pin-count boundary
- RAM allocation bytes
- temporary representation bytes
- compression/storage ratio inputs
- spill/reload reason
- prior and next persistence policy

Optional causal attributes:

- creation query UUID
- eviction-trigger query UUID
- trigger task UUID
- trigger operator UUID
- reload consumer query/task/operator

Name every such field `created_by`, `triggered_by`, or `consumed_by`. Never name
it `owner` unless DuckDB gains a durable ownership model.

### Placement resources

Possible usages:

| State | Diagnostic usage |
|---|---|
| `ResidentPinned` | bytes on buffer-pool RAM placement, pinned category |
| `ResidentEvictable` | bytes on buffer-pool RAM placement, evictable category |
| `Spilling` | RAM placement remains until successful write/unload |
| `TemporaryStored` | bytes in live temporary representation placement |
| `Reloading` | model the actual overlap if RAM and temporary representation coexist |
| `PersistentUnloaded` | no RAM or live-temporary occupancy |
| `Discarded` | no occupancy |

Do not attach placement to `TemporaryDirectoryStorage`. Grouped file extent,
holes, reused slots, and variable files prevent a stable one-block-to-extent
mapping.

Do not duplicate `TemporaryIoChannel` usage. `TemporaryBlockIo` remains the
operation/rate entity. Add `placement_id` to it so operation and placement can
be joined.

### Preventing double counting

The default memory-resource timeline must use only `MemoryAccount`.

Typed `block_placement` views may itemize blocks, but the analyzer must never
sum these usages with `memory_account`. Two safe approaches are:

1. Treat `MemoryAccount` as the canonical unfiltered layer and
   `BlockPlacement` as a typed detail layer, matching the existing task versus
   invocation treatment on execution threads.
2. Use distinct diagnostic placement resource types if Quent's generic
   aggregation cannot enforce consumer selection.

Never present:

```text
MemoryAccount bytes + BlockPlacement bytes
```

That counts the same buffer-managed bytes twice.

### Reconciliation

Use the canonical accounts to validate placement, not vice versa.

Expected relationships:

- Sum of resident placement bytes is a subset of `BufferPoolMemory`.
- The difference includes allocator, extension, object-cache, reservation, and
  any unmodeled buffer charges.
- Sum of `TemporaryStored` bytes should match `TemporaryStorage` only when all
  relevant block paths use identical stored-byte semantics.
- Directory extent is independent and must not be reconciled block-for-block.

Emit or derive a diagnostic residual:

```text
buffer_pool_accounted - modeled_resident_blocks
```

Call it `unmodeled_charged_bytes`, not leakage.

### Native hooks to audit

Start with these paths:

- `BlockMemory` constructors and destructor
- `StandardBufferManager::Pin` and `Unpin`
- batch load and pin paths
- `BlockMemory::UnloadAndTakeBlock`
- `BlockHandle::Load`
- temporary write completion and temporary deletion
- persistent drop and reload
- destroyable discard
- `ConvertToPersistent`
- block resize/reallocation

Emit placement state only after the underlying operation commits. For pin
noise, first model reader-count boundaries:

```text
0 -> 1  ResidentEvictable -> ResidentPinned
1 -> 0  ResidentPinned -> ResidentEvictable
```

Do not emit every intermediate reader increment/decrement unless per-reader
contention is a stated requirement.

### Layering

Add a storage-level `BlockPlacementProbe` abstraction near
`MemoryUsageProbe`. Low-level storage calls only that interface.
`TelemetryContext` implements the Quent bridge side. `DatabaseInstance`
injects it. Generated Rust headers remain in `src/main/telemetry_context.cpp`.

The probe must be fail-open. Destructors and cleanup paths must be `noexcept`.
Detach registry state before bridge calls that can fail. Probe callbacks must
not call back into storage while storage locks are held.

### Concurrency and volume

Audit:

- concurrent pins and unpins
- eviction racing a new pin
- shared BufferPool observers
- batch loading
- destruction during teardown
- C++ address reuse
- resource/account exit order

Per-block telemetry can be high volume. Measure event count and exporter memory
on a forced-spill workload before adding per-pin transitions. Quent currently
uses asynchronous unbounded exporter queues; large captures can perturb or
exhaust memory. Prefer semantic state boundaries, sampling, or aggregation over
raw access events.

### Analyzer contract

- Reconstruct only legal, complete placement FSMs.
- Snapshot-close live placements for immutable analysis without modifying raw
  events.
- Validate placement resource type, Engine parent, block identity, and
  `TemporaryBlockIo.placement_id` joins.
- Default memory views remain `MemoryAccount` only.
- Typed placement views expose pinned, evictable, temporary, unloaded, and
  discarded state.
- Operator selection must not show stationary placement as owned memory.
- Operator selection may show transition markers or I/O operations whose
  causal trigger matches the selected operator.
- Clip placement usages to Query span when viewed from a query.
- Keep database-wide placement visible as contextual data, explicitly labeled
  as global.

### Required tests

Write tests before fixes or behavioral changes.

Native tests:

- initially loaded construction
- persistent unloaded construction
- first pin and final unpin
- concurrent pins
- successful spill and reload
- failed write leaves resident state
- failed read leaves stored/unloaded state
- destroyable discard
- persistent drop and reload
- batch load
- conversion to persistent
- resize
- destruction from every legal state
- registry removal before address reuse
- telemetry failure cannot fail storage
- enabled and disabled builds

Analyzer tests:

- every legal lifecycle
- illegal transition rejection
- truncated live snapshot
- wrong resource or parent rejection
- nil and valid causal references
- no false operator ownership
- query-span clipping
- canonical account timeline excludes placement
- typed placement timeline excludes accounts
- temporary-I/O join by placement UUID
- residual and reconciliation behavior

Integration tests:

- forced spill and reload
- persistent table scan eviction/reload
- concurrent task execution
- both grouped fixed blocks and variable blocks
- final canonical temp occupancy returns to zero
- resident placement sum never replaces canonical account total
- directory extent is allowed to differ from live stored placements
- API and UI payloads render expected typed lanes
- capture volume and query slowdown are recorded

### Questions BlockPlacement cannot answer

Even a complete placement entity does not provide:

- general query/operator ownership
- itemization of allocator, extension, or object-cache charges
- process RSS or allocator fragmentation
- OS swap or kernel page cache
- filesystem allocated sectors
- physical DIMM, NUMA, GPU, or device placement
- stable DataChunk lineage

Do not infer these from placement UUIDs.

## Analyzer and UI contract

The server exposes the Quent query-engine APIs, including:

```text
GET  /api/engines
GET  /api/engines/{engine_id}/query-groups
GET  /api/engines/{engine_id}/query_group/{group_id}/queries
GET  /api/engines/{engine_id}/query/{query_id}
POST /api/engines/{engine_id}/entities
POST /api/engines/{engine_id}/timeline/single
POST /api/engines/{engine_id}/timeline/bulk
POST /api/engines/{engine_id}/timeline/data-flow
```

The query bundle contains the physical plan, entity FSM declarations, resource
tree, resource types, group types, `used_by` relations, quantity specifications,
and operator active spans.

Supported entity types:

```text
pipeline_task
operator_invocation
chunk_transfer
temporary_block_io
memory_account       timeline-only; entity list is empty
```

Data-flow measures are chunk publications, rows, and logical bytes. The
in-process `chunk_summary` helper is not exposed as a separate HTTP route.

A single-resource request has this shape:

```json
{
  "entry": {
    "Resource": {
      "resource_id": "RESOURCE_UUID",
      "long_entities_threshold_s": null,
      "entity_filter": {
        "entity_type_name": "memory_account"
      },
      "application": {
        "operator_ids": []
      },
      "config": {
        "num_bins": 100,
        "start": 0.0,
        "end": 1.0
      }
    }
  },
  "app_params": {
    "query_id": "QUERY_UUID"
  }
}
```

Use the query's actual duration for `end`. Resource-group requests replace the
resource ID with a group ID and type selection. The UI often sends an empty
resource type while initializing; the analyzer resolves leaf types.

A data-flow request uses the same time configuration and Query UUID:

```json
{
  "measures": ["chunks", "rows", "logical_bytes"],
  "config": {
    "num_bins": 100,
    "start": 0.0,
    "end": 1.0
  },
  "app_params": {
    "query_id": "QUERY_UUID"
  }
}
```

### Operator selection

- `PipelineTask`: any selected operator in the task's pipeline.
- `OperatorInvocation`: exact operator UUID.
- `ChunkTransfer`: declared source or target relationship as requested.
- `TemporaryBlockIo`: causal trigger operator.
- `MemoryAccount`: always empty for a non-empty operator filter.

### Historical analyzer failures and fixes

These are useful regression context:

- `resource type  is unknown`: the UI initializes a ResourceGroup request with
  an empty type. The analyzer now resolves actual leaf types.
- `engine does not have an exit timestamp`: live streams lacked terminal
  events. Snapshot construction now closes Engine, Worker, resources, and live
  runtime entities for analysis only.
- `data-flow timeline unsupported`: ChunkTransfer reconstruction now powers it.
- pre-query MemoryAccount conversion produced a negative query-relative epoch:
  accounts are now timeline-only and excluded from entity lists.
- Init-only truncated resources aborted analyzer construction: snapshots now
  synthesize their missing lifecycle with unknown capacity.
- unfiltered execution-thread occupancy counted nested tasks and invocations:
  PipelineTask is now the canonical plain layer.
- memory usages appeared after query completion when callers requested an
  oversized window: usages are now clipped to `Query.span`.
- malformed or cross-query task/invocation/I/O references could leak into a
  view: membership, worker, plan, operator, port, and resource validation were
  tightened.

The generic FSM builder remains permissive about some corrupted event sequence
numbers and illegal custom transitions. Generated instrumentation emits legal
sequences; stricter malformed-stream validation remains possible future work.

The QueryBundle UI representation currently omits resource capacity history.
The analyzer reconstructs resize transitions, but the UI shows absolute bytes,
not an overlaid limit or utilization percentage.

## Build, capture, and serve

Build:

```bash
cd /home/dvats/repos/duckdb
cmake -S . -B build/release \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_QUENT_TELEMETRY=ON
cmake --build build/release --target shell -j4
```

Capture:

```bash
events_dir=$(mktemp -d /tmp/duckdb-quent.XXXXXX)
QUENT_EXPORTER=ndjson QUENT_OUTPUT_DIR="$events_dir" \
    build/release/duckdb -c "SELECT 42;"
```

Serve after DuckDB exits:

```bash
cargo run --manifest-path rust/Cargo.toml \
    -p duckdb-telemetry-server --features ui -- \
    --output-dir "$events_dir" \
    --collector-address 127.0.0.1:7836 \
    --analyzer-address 127.0.0.1:8080
```

Open `http://127.0.0.1:8080`. Restart the server when capture files change; the
analyzer caches an Engine after first access.

Expand `local` for task, thread, and temporary-I/O resources. Memory resources
are direct children of the Engine root, not `local`.

## Representative memory workload

This workload exercises canonical memory accounts plus spill/reload operations:

```bash
cd /home/dvats/repos/duckdb
events_dir=$(mktemp -d /tmp/duckdb-quent-memory.XXXXXX)
spill_dir=$(mktemp -d /tmp/duckdb-spill.XXXXXX)

QUENT_EXPORTER=ndjson QUENT_OUTPUT_DIR="$events_dir" \
build/release/duckdb -c "
SET threads=4;
SET scheduler_process_partial=true;
SET memory_limit='128MB';
SET temp_directory='$spill_dir';
SET max_temp_directory_size='1GB';
SET preserve_insertion_order=false;
SET debug_force_external=true;
SELECT count(*)
FROM range(10000000) build_side(i)
JOIN range(10000000) probe_side(i) USING(i);
"
```

Direct `range()` is forced single-threaded as a source. This query is useful for
memory and spill behavior, not for proving four-way scan parallelism. Use a
materialized table or partitioned Parquet workload for execution-thread tests.

See [QUERY_COOKBOOK.md](QUERY_COOKBOOK.md) for parallel generated data, TPC-H
Q1, multiway joins, windows, and forced external execution.

Available data discovered during this work:

```text
/data/tpch/sf1/p16/snappy/<table>/*.parquet
/data/tpch/sf10/p16/snappy/<table>/*.parquet
```

Higher scale factors and compression variants also exist. SF1 is the default
for instrumented forced-spill examples because per-invocation and per-chunk
events can make SF10 captures hundreds of megabytes. No TPC-DS data was found.

`SET threads=4` is a maximum, not a guarantee. Parallelism depends on source,
pipeline, cardinality, and blocking operators. A stored 5-million-row table
scan produced tasks on four OS-thread resources; a direct range aggregation
used one. That earlier one-thread UI result was genuine scheduling, not lost
analyzer data.

## Observed validation results

A completed forced-spill capture at
`/tmp/duckdb-memory-spill.HJncrw` produced:

- target query duration: about 0.519 seconds
- event directory size: about 58 MB
- 24,960 MemoryAccount transitions
- 664 TemporaryBlockIo entities
- 2,656 TemporaryBlockIo lifecycle events

Observed keyed occupancy peaks:

| Resource/tag | Peak |
|---|---:|
| buffer pool / HASH_TABLE | 121.24 MiB |
| buffer pool / COLUMN_DATA | 102.27 MiB |
| buffer pool / ALLOCATOR | 9.40 MiB |
| live temporary / HASH_TABLE | 107.47 MiB |
| live temporary / COLUMN_DATA | 69.44 MiB |
| directory extent / UNKNOWN | 176.91 MiB |

These are independent series, not additive tiers of one ownership model.

API checks confirmed:

- three Engine-child memory resources
- all resources declare `memory_account` in `used_by`
- unfiltered and tag-keyed timelines are nonnegative
- operator-filtered memory timelines are empty
- memory `/entities` returns `{"items":[],"total":0}`
- oversized windows have zero post-query memory bins
- temporary accounts and directory extent return to zero after cleanup
- live Engine/resource snapshots work before process Exit

A concurrent four-thread randomized sort also returned all temporary gauges to
zero with 33 account exits and no deadlock or underflow.

## Tests already run

Rust:

```bash
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo check --manifest-path rust/Cargo.toml --workspace --all-targets
cargo test --manifest-path rust/Cargo.toml --workspace
cargo clippy --manifest-path rust/Cargo.toml \
    --workspace --all-targets -- -D warnings
```

The analyzer suite reached 29 passing tests. It covers entity reconstruction,
resource timelines, operator filters, live snapshot closure, memory sums and
tags, malformed memory account rejection, query-span clipping, and Init-only
resource recovery.

Native focused test:

```bash
build/release/test/unittest \
    "BufferPool memory probes preserve accounting order"
```

It reached 13 assertions covering tag snapshots, signed deltas, two observer
baselines, post-registration ordering, successful limits, and failed-limit
silence.

Telemetry-enabled shell/unittest builds and telemetry-disabled
`duckdb_main`, `duckdb_storage`, and `duckdb_storage_buffer` builds passed.
Formatting dry runs and `git diff --check` passed at the end of the memory work.

The bridge emits a known warning because the model component `operator` maps to
the generated C++ namespace `operator_`. It is expected.

## Rejected interpretations

| Claim | Why it is false |
|---|---|
| ChunkTransfer is physical memory transfer | It observes logical publication before consumption and may involve shared buffers. |
| Produced-to-Published is edge latency | Both transitions bracket telemetry emission, not consumer acceptance. |
| DataChunk pointer is a batch ID | Slots and vector buffers are reset, reused, moved, and referenced. |
| TemporaryIoChannel is a memory tier | It measures operations and bytes serviced over time, not stored occupancy. |
| TemporaryBlockIo operator owns the victim | The active operator may only trigger eviction of another block. |
| BufferPoolMemory is process RSS | It is DuckDB's configured-limit accounting. |
| TemporaryStorage equals directory size | Live representations and file extent have different allocation/reuse behavior. |
| Directory extent is disk sectors | It is DuckDB's accounted file extent/quota. |
| Sum of block placement can replace memory accounting | Non-block charges are omitted and some domains are not one-to-one. |
| `threads=4` requires four active lanes | It sets a ceiling; pipeline/source constraints determine achieved parallelism. |
| PipelineTask time is operator CPU | It spans the whole scheduled pipeline slice and measures wall time. |
| OperatorInvocation covers all operator work | Several control, combine, and auxiliary paths are not bracketed. |
| Selecting an operator gives owned memory | MemoryAccount is database-wide and deliberately filtered out. |

## Remaining diagnostic candidates

### OperatorMemoryGrant

Model `TemporaryMemoryManager` desired, minimum, and granted budgets plus the
in-memory/external decision. This can explain why sort, hash join, radix
aggregate, batch insert, or other consumers spill. It is policy, not residency.
The state currently lacks a generic physical operator identity, so attribution
requires explicit plumbing.

### Task wait reason and wait resource

`PipelineTask::Blocked` currently has no reason or resource. Adding scheduler,
I/O, backpressure, dependency, or exchange wait categories would explain idle
gaps. Avoid a generic string where a stable enum is available.

### Scheduler capacity

Thread timelines show active execution count, but not configured or actual
slot capacity. A resizable scheduler-pool resource could support saturation,
effective parallelism, and starvation analysis. `SET threads=N` includes an
external-thread allowance and remains only a ceiling.

### Scheduler-owned execution threads

Current thread resources are created lazily from observed OS thread IDs.
Instrument scheduler thread start/stop through a parallel-layer probe to model
idle capacity, resize, role, and lifetime. Keep caller/external threads
separate. Never equate thread identity with CPU identity.

### CPU migration insight

Existing dispatch samples can derive sampled CPU set and migration count.
They cannot define continuous CPU-core occupancy because a thread may migrate
between samples and non-Linux fallbacks are not physical CPU identifiers.

### Pipeline entity

Tasks contain an operator set but there is no explicit pipeline entity,
dependency graph, or maximum-thread decision. Modeling pipeline construction,
dependencies, events, and assigned task count would help explain critical paths
and serial bottlenecks.

### File and persistent I/O

Central file read/write paths accept `QueryContext` in some cases. Separate
persistent database, Parquet/object-store, WAL, and temporary traffic. Preserve
initiator versus owner semantics and avoid labeling kernel/page-cache behavior
as device latency without evidence.

### Query and operator statistics

Quent supports operator statistics, but this integration has focused on event
timelines. DuckDB already tracks operator timing, rows, bytes, row groups,
blocked time, bytes read/written, peak buffer memory, and temporary size. Export
only measures with defined aggregation and execution scope.

### Stable data lineage

A Sirius-style batch identity would require semantic IDs and propagation rules
across every DuckDB operator boundary. This is separate from `ChunkTransfer`.
Do not infer lineage from reusable DataChunk pointers or allocation buffers.

## Known limits and non-goals

- Telemetry is opt-in but per-invocation and per-chunk streams can be large.
- Quent exporters use asynchronous unbounded queues; high-volume captures can
  increase memory use and perturb execution.
- MemoryAccount is DuckDB accounting, not RSS.
- No general query, task, or operator memory ownership exists.
- TemporaryBlockIo attribution is causal.
- ChunkTransfer is logical publication, not memory movement.
- PipelineTask Running is scheduled wall time, not CPU time.
- OperatorInvocation is wall time for covered calls, not complete operator CPU.
- TaskQueue is runnable/backlog approximation.
- ExecutionThread is OS-thread occupancy, not core occupancy.
- Directory extent is DuckDB-accounted extent, not allocated disk sectors.
- Shared BufferPool gauges represent the shared pool in every observing Engine.
- Custom buffer managers bypass the current standard memory probes.
- Resource capacity history is not yet displayed by the QueryBundle UI.
- Typed memory timelines can include zero series for unused tags.
- Query parsing/binding failures are deliberately not modeled as executable
  Query FSMs under the current Quent query state machine.

## Continuation checklist

Before new work:

1. Read `/home/dvats/repos/duckdb/AGENTS.md`.
2. Read this file, [INSTRUMENTATION.md](INSTRUMENTATION.md), and
   [QUERY_COOKBOOK.md](QUERY_COOKBOOK.md).
3. Inspect `git status`; preserve all existing telemetry changes.
4. Run the Rust workspace tests and focused native accounting test.
5. Build telemetry-enabled and disabled targets touched by the change.
6. Use the actual server HTTP endpoints as the analyzer contract.
7. Validate one completed capture and one live/truncated snapshot.
8. Record semantics and limits before adding a new UI measure.

For `BlockPlacement`, begin with a read-only lifecycle trace of every
`BlockMemory` construction, pin boundary, unload outcome, reload, policy
conversion, resize, and destruction path. Write the native and analyzer tests
listed above before changing behavior. Keep `MemoryAccount` canonical
throughout.

## Glossary

| Term | Meaning here |
|---|---|
| occupancy | bytes held over an interval |
| rate | operations or bytes serviced per second |
| accounting | DuckDB's configured-limit charge, not OS memory |
| resident block | a loaded buffer-manager representation, not necessarily a physical RAM page |
| live temporary bytes | stored representations still needed for reload |
| directory extent | DuckDB-accounted temporary file extent |
| ownership | durable responsibility for bytes; generally unavailable |
| causal trigger | query/task/operator active when an action occurred |
| placement | location and evictability of a specific buffer-managed representation |
| publication | non-empty DataChunk occurrence exposed to the next plan edge |
| canonical layer | the only entity family used for the default aggregate |
