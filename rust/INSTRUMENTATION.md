# DuckDB Quent instrumentation model

This document explains what the DuckDB telemetry entities mean, where their
events come from, what the analyzer can conclude, and what it must not claim.
See [README.md](README.md) for build, capture, server, and UI commands.

## System map

```text
DatabaseInstance                         Quent Engine
└─ local execution backend               └─ Worker
   ├─ ClientContext                         └─ QueryGroup
   │  └─ one successful execution              └─ Query
   │     └─ physical operator DAG                    └─ Plan
   │        ├─ PhysicalOperator                          ├─ Operator
   │        └─ child-to-parent flow                      └─ Port ─ Edge ─ Port
   │
   ├─ PipelineTask              PipelineTask FSM
   ├─ physical operator call    OperatorInvocation FSM
   ├─ nonempty edge output      ChunkTransfer FSM
   └─ temporary buffer I/O      TemporaryBlockIo FSM
      ├─ spill                  temporary-spill resource
      └─ reload                 temporary-reload resource

BufferPool / temporary files    MemoryAccount FSM
├─ charged bytes by tag         buffer-pool-memory resource
├─ live evicted bytes by tag    temporary-storage resource
└─ accounted file extent        temporary-directory-storage resource
```

The model separates three concerns:

- Query-engine structure: query, plan, operator, port, and edge identity.
- Runtime work: tasks, operator calls, and chunk publications.
- Resources: runnable-task backlog, execution threads, and temporary-I/O
  service paths, plus database-wide memory and storage occupancy.

Every execution receives new UUIDs. Cached prepared plans are not assigned
persistent telemetry IDs because the same physical objects may be reused by
several executions.

### Event path

```text
Rust model declarations
        ↓ bridge/build.rs
Generated CXX observers and handles
        ↓
DuckDB lifecycle hooks
        ↓ exporter
Quent event stream
        ↓
DuckDB analyzer reconstruction and validation
        ↓
Quent query bundle, entity, timeline, and dataflow APIs
        ↓
Quent UI
```

The Rust model is the schema. Generated bridge types prevent C++ from emitting
attributes or transitions that are absent from that schema. The analyzer is a
separate semantic boundary: it validates cross-entity references, filters
incomplete data, derives timelines, and prepares UI-specific structures.

## Structural entities

### Engine

One `Engine` represents one `DatabaseInstance`. It is the root entity and root
resource group. Its lifetime starts during database initialization and ends
during database teardown.

The declaration records the DuckDB implementation name and version. It does
not represent a connection, query, scheduler thread, or database file.

### Worker

One `Worker` represents DuckDB's local execution backend. It is a child of the
engine and the parent of runtime resources.

It deliberately does not represent one scheduler thread. A physical plan may
run on the client thread, regular scheduler threads, and asynchronous threads,
while a Quent plan has one worker reference. Execution threads are separate
resources beneath this worker.

### QueryGroup

One `QueryGroup` represents one `ClientContext`, normally one connection or
session. It is declared lazily at the first query begin, after DuckDB has
assigned a connection ID.

Several queries on the same connection share the group. Concurrent queries use
separate `ClientContext` instances and therefore separate groups.

### Query

The standard Quent query lifecycle is:

```text
Init → Planning → Executing → Exit
```

DuckDB records the SQL text as the query instance name. A query handle is
created only when an executable physical plan is available. The telemetry then
emits Planning, declares the plan, enters Executing, and exits at query end.

This delayed creation is intentional. The standard Quent FSM cannot exit from
Planning, so emitting a binder or planner failure as `Planning → Exit` would be
model-invalid. Failed statements that never reach execution are currently
absent rather than mislabeled as executed queries.

### Plan

One `Plan` represents the physical plan for one query execution. It is attached
to the query and local worker. Prepared-plan reuse produces a new plan and new
operator IDs for each execution.

DuckDB's result collector is included as the root. This makes even a small
query normally contain at least one edge, which matters because the Quent UI
constructs plan nodes by walking edges.

The emitter traverses `PhysicalOperator::GetChildren()`, not the public child
container. Virtual traversal is required for operators such as result
collectors and prepared-statement execution wrappers.

### Operator

One Quent `Operator` represents one unique `PhysicalOperator` in the execution
graph.

- `operator_type_name` is DuckDB's stable physical operator category.
- `instance_name` is the operator's descriptive name.
- `plan_id` joins it to the execution-specific plan.
- `parent_operator_ids` is empty because physical adjacency is represented by
  edges, not by logical-to-physical lineage.

Shared physical subplans are interned by object identity within one emission so
they are declared once and may fan out to several edges.

### Port and edge

Ports make dataflow explicit. For every physical relation `parent ← child`:

```text
child output port ──────────────> parent input port
```

The child output port may feed several edges. Each parent input relation has a
separate input port. Plan edges always reference port IDs, never operator IDs.

This direction means production flows from an upstream child into its
downstream consumer. It does not mean that DuckDB copied bytes between two
independent buffers.

## Runtime entities

### PipelineTask

A `PipelineTask` is one scheduled executor of a DuckDB pipeline. It is not one
operator invocation and not one source chunk. One task may process many chunks
through every operator in its pipeline.

Its declaration contains:

- query, plan, and worker IDs;
- the physical operator IDs belonging to its pipeline;
- an execution-local task index.

Its lifecycle is:

```text
Created ────────────────> Running ───────────────> Finalizing → Exit
   │                        │  ▲                       ▲
   │                        │  └──── Ready ───────────┤
   │                        └─────── Blocked ─────────┤
   └─────────────────────────────────────────────────┘
```

State meaning:

| State | Meaning | Resource usage |
|---|---|---|
| `Created` | Task exists and awaits initial dispatch | Task queue, one entry |
| `Running` | DuckDB is executing a bounded or complete task slice | Execution thread, one unit |
| `Ready` | A partial slice yielded and can run again | Task queue, one entry |
| `Blocked` | Task cannot currently make progress | None |
| `Finalizing` | Terminal result is known | None |

`Running.mode` distinguishes partial from all-at-once execution. `cpu_id` is a
sample taken when the slice starts. On Linux it normally comes from
`sched_getcpu()`. It is not a CPU-core occupancy span: a thread may migrate
during one slice, and non-Linux fallbacks have different semantics.

The destructor provides failure/cancellation cleanup. Success is recorded only
after DuckDB's event completion succeeds.

#### Operator filtering

Selecting a physical operator includes every task whose pipeline contains that
operator. The entire task interval is shown because a task is a pipeline-level
unit. This answers, "which tasks could execute this operator?" It does not
claim that the selected operator consumed the task's entire running time.

Use `OperatorInvocation` for exact operator time.

### OperatorInvocation

An `OperatorInvocation` represents one call into a physical operator from
`PipelineExecutor`.

Covered phases are:

- source `GetData`;
- intermediate `Execute`;
- sink `Sink`;
- caching-operator `FinalExecute`.

Its lifecycle is:

```text
InvocationCreated → InvocationRunning → InvocationCompleted → Exit
```

The entity records query, plan, task, operator, phase, input rows and logical
bytes, output rows and logical bytes, terminal success, and execution thread.

This is intentionally fine-grained: a vectorized operator is normally invoked
once per chunk, and caching operators may be invoked repeatedly for pending
output.

The invocation interval is wall time on an execution thread. It includes
synchronous work performed inside the operator, including temporary I/O. It is
not CPU time.

Coverage does not yet include sink `Combine`, batch-index updates, every source
finalization callback, or auxiliary non-pipeline tasks. Consequently pipeline
task time may exceed the sum of operator invocation time.

### ChunkTransfer

`ChunkTransfer` records one nonempty `DataChunk` publication across a declared
physical-plan edge.

```text
Produced → Published → Exit
```

It records:

- query and task IDs;
- source and target operator IDs;
- source and target port IDs;
- row count;
- logical byte estimate.

Hooks cover source output, intermediate operator output, caching-operator final
output, and externally pushed pipeline input. A new UUID is created for every
publication.

Despite its historical name, this entity means publication, not delivery or
physical transfer:

- It is emitted after output exists and before the target consumes it.
- DuckDB calls adjacent operators synchronously; there may be no queue.
- The target may later block or fail.
- A `DataChunk` may reference upstream vectors without copying.
- The event does not measure edge latency or memory bandwidth.

`logical_bytes` comes from `DataChunk::GetDataSize()`. It is neither allocated
memory nor unique resident memory. Shared and referenced vector data may be
counted on several edges.

No resource usage is attached because the Produced-to-Published interval is an
instrumentation publication point, not resource occupancy.

#### Derived dataflow

The analyzer can aggregate complete, topology-valid publications by plan edge:

- chunk publications per second;
- rows per second;
- logical bytes per second;
- total chunks, rows, and logical bytes;
- average chunk cardinality and approximate bytes per row;
- operator expansion or reduction across ports.

It cannot derive copies, unique bytes, physical bandwidth, batch lineage,
consumer latency, or backpressure from these events.

### TemporaryBlockIo

A `TemporaryBlockIo` represents one synchronous buffer-manager spill or reload.
It is one operation, not the lifetime of a temporary block.

The exact hooks are:

- `StandardBufferManager::WriteTemporaryBuffer` for spill;
- `StandardBufferManager::ReadTemporaryBuffer` for reload.

Its lifecycle is:

```text
IoRequested → IoActive → IoCompleted → Exit
       └─────────────────────┘
          setup failure
```

`IoRequested` records:

- query and plan IDs;
- task ID when a pipeline task is known;
- `trigger_operator_id` when an operator invocation is active;
- block ID;
- DuckDB `MemoryTag`;
- `spill` or `reload` direction.

`IoActive` uses a temporary-I/O resource with one operation and the
page-aligned `FileBuffer` allocation size. `IoCompleted` records success and
the temporary representation's `storage_bytes`.

For grouped fixed-size blocks, `storage_bytes` is the compressed temporary slot
size. For variable-size blocks, it is the file size including metadata.

RAII closes failed operations. Probe and completion exceptions are suppressed
so observability cannot fail DuckDB storage work.

#### Attribution

The storage layer receives a `QueryContext`. The telemetry layer also tracks
the active operator invocation on the current thread.

- A reload is normally attributed to the consuming task and operator.
- A spill is attributed to the work that caused memory pressure.
- The evicted block may belong to another operator or query.

The field is therefore `trigger_operator_id`, not `owner_operator_id`. Nil task
or operator IDs are valid when I/O happens outside an instrumented invocation.

#### Interpretation

The measured interval includes compression, encryption, buffer-manager and
temporary-file locks, filesystem work, and deletion. It is end-to-end
temporary-storage service time, not raw disk or PCIe time.

The same block ID may appear in several spill and reload entities. Pairing
successive operations can suggest repeated eviction or approximate residence,
but residence is not explicitly modeled.

### MemoryAccount

A `MemoryAccount` is one database-lived absolute occupancy gauge. DuckDB emits:

- one buffer-pool account for each of its 16 accounted memory tags;
- one live-temporary-storage account for each tag;
- one `temporary-directory` account tagged `UNKNOWN`.

The 33 accounts follow:

```text
AccountRegistered → Accounted ↺ → Exit
```

Every `Accounted` state carries the current byte total and uses exactly one of
the three memory resources. Storage sends signed deltas; the bridge converts
them to absolute totals before emitting a state. Buffer-pool registration also
captures existing charges, so allocations made before probe attachment are not
lost.

Accounts and resources belong to the database, not a query, task, or operator.
The analyzer clips their database-wide spans to the selected query window.
Selecting any physical operator therefore removes these series. This avoids
inventing ownership that DuckDB does not track.

Memory accounts are timeline-only. They start before query-relative time zero,
which the current UI entity-list conversion cannot represent as a finite FSM.
The analyzer still uses them for untyped totals and `memory_account` series
split by `MemoryTag`.

## Resources

### ExecutionThread

An `ExecutionThread` is the OS-thread key observed while pipeline work runs. It
is parented under the local worker and has unit capacity.

PipelineTask `Running` and nested OperatorInvocation `InvocationRunning` both
refer to it. These spans overlap by design. To avoid double-counting physical
occupancy, an unfiltered execution-thread timeline uses task spans only.
Selecting `operator_invocation` requests the detailed operator breakdown.

Threads are created lazily in telemetry when first observed. They remain until
engine exit; a future scheduler-owned lifecycle should distinguish sequential
threads if the OS reuses an ID. A resource may exist because an earlier
statement used it even when the selected query did not. Query-filtered usage,
not resource existence, determines utilization.

`SET threads=N` is an upper bound, not a guarantee. Operator parallelism,
source parallelism, input size, and dependencies determine actual concurrency.
DuckDB's `range()` table function is forced single-threaded; a partitioned
Parquet or stored-table scan can produce several tasks.

### TaskQueue

The `runnable-pipeline-tasks` resource records task `Created` and `Ready`
intervals with one entry per task.

It is an approximation of runnable backlog, not an exact scheduler queue:

- `Created` begins just before initial scheduling.
- `Ready` begins before the yielded task is handed back.
- Some client execution paths may retain ready work locally.

Use it to diagnose dispatch delay and runnable pressure. Do not interpret it as
an exact count of nodes inside DuckDB's internal queue at every instant.

### TemporaryIoChannel

`TemporaryIoChannel` is a rate resource used only while a spill or reload is
active. The worker owns two instances:

- `temporary-spill` for RAM-to-temporary-storage service;
- `temporary-reload` for temporary-storage-to-RAM service.

Each usage contributes:

- `capacity_operations = 1`;
- `capacity_buffer_bytes = aligned in-memory buffer bytes`.

The analyzer reports operations per second and buffer bytes per second. These
are effective buffer-manager service rates, not fixed hardware capacity.

This is intentionally a channel rather than a memory tier. The current events
observe the transfer arrows, not the interval during which a block resides in
RAM or temporary storage:

```text
RAM tier ── spill channel ──> temporary-storage tier
RAM tier <─ reload channel ── temporary-storage tier
```

A truthful tier model requires a persistent block-placement lifecycle and
complete allocation, pin, eviction, reload, deletion, and destruction hooks.

### BufferPoolMemory

`buffer-pool-memory` is DuckDB's managed-memory charge, split by `MemoryTag`.
The bridge observes the authoritative `BufferPool::UpdateUsedMemory` boundary,
including reservations made before a physical block exists. Its resource bound
tracks successful `memory_limit` changes.

This is not process RSS. It excludes stacks, ordinary unmanaged allocations,
memory-mapped pages, the kernel page cache, and telemetry itself. A charge also
does not identify a unique buffer, query, or operator. Treat it as the byte
budget DuckDB's eviction policy sees. If databases share a buffer pool, each
registered database sees the shared total.

### TemporaryStorage

`temporary-storage` is the live evicted representation, split by the evicted
block's `MemoryTag`. It rises after a spill and falls on reload or deletion.
Grouped fixed-size blocks use their compressed temporary size; variable blocks
use their allocated buffer size.

This is stationary aggregate occupancy, unlike `temporary-spill` and
`temporary-reload`, which measure active operations and bytes per second. It is
not filesystem size, device bandwidth, or an ownership claim. Its displayed
bound follows `max_temp_directory_size` for comparison; DuckDB enforces that
setting against directory footprint.

### TemporaryDirectoryStorage

`temporary-directory-storage` is DuckDB's accounted temporary-directory file
extent. It has one `UNKNOWN` account because the file manager no longer retains
a tag breakdown at this layer. Its bound follows `max_temp_directory_size`.

This gauge can exceed or lag live temporary storage. Grouped files grow and
shrink in extents, while variable files include headers. Compression and file
reuse further separate accounted file extent from live logical representations.
Consequently these are three related, non-equivalent quantities:

```text
buffer-pool charge  ≠  live evicted representations  ≠  directory extent
```

None is RSS. Do not add temporary-storage tags and expect the directory gauge,
or subtract either temporary gauge from buffer-pool usage to infer a block's
placement.

## DuckDB memory semantics

### DataChunk is an execution batch, not a placement

`DataChunk` is DuckDB's closest execution-level analog to a Sirius data batch:
equal-length column vectors, normally up to 2,048 rows. However, it lacks the
stable identity and ownership required for a Sirius-style placement lifecycle.

- Pipeline executors reuse chunk objects and retained allocations.
- `Reset` prepares a slot for new contents without creating a new object.
- Vectors can own storage or reference other vectors.
- Filters may publish dictionary or selection references without copying.
- A pointer identifies a reusable executor slot, not one logical batch.

Therefore `ChunkTransfer` allocates an identity per publication and makes no
lineage or memory-residency claim.

### Buffer blocks are the residency candidates

DuckDB's managed-memory layer is block based:

- `BlockMemory` holds block ID, loaded state, reader count, tag, buffer, and
  allocation charge.
- `BufferHandle` is a move-only RAII pin.
- Unpinned blocks become eviction candidates.
- Persistent blocks may be discarded and reread.
- destroyable intermediates may be discarded permanently.
- non-destroyable temporary blocks are written before unload.

The buffer pool accounts managed charges by `MemoryTag`, but does not retain
universal query or operator ownership. A `QueryContext` passed to eviction
identifies the claimant that needed memory, not necessarily the victim's owner.

This distinction constrains every future ownership and tier claim.

## Relation to Sirius

The DuckDB model follows the same Quent contracts but not every Sirius mapping.

- Sirius currently represents an execution pipeline as one Quent Operator and
  concatenates its physical operations into the label. DuckDB declares one
  Quent Operator per `PhysicalOperator`, which gives operator-level plan and
  runtime joins.
- Sirius and DuckDB both model scheduler tasks as FSMs with queue and execution
  resources. DuckDB's task states follow its partial-yield and interrupt
  behavior rather than Sirius GPU-specific states.
- Sirius `DataBatch` has a persistent process-unique identity and explicit
  representation conversion. DuckDB `DataChunk` is reusable and referenceable,
  so the current model gives publications new identities instead.
- Sirius `BatchPlacement` represents one physical batch at one consumer port
  and records queued, packaged, processing, consumed, and memory-tier states.
  DuckDB's ordinary pipeline edges are synchronous calls without such a
  repository. Its broadcast exchange is the closest honest placement target.
- Sirius distinguishes stationary tier occupancy from in-transit channel
  usage. DuckDB currently implements the temporary-I/O channel half; a future
  block-placement FSM is required for the tier half.

## Analyzer contract

The analyzer reconstructs standard query-engine declarations, five runtime FSM
collections, and resources from one engine event stream.

Before an entity affects UI output, the analyzer validates relevant joins:

- query, plan, and worker membership;
- operator membership in the referenced plan;
- task membership and task-to-operator containment;
- port ownership and declared plan edges;
- resource type and worker parent;
- one stable Engine-parent memory resource per account;
- terminal FSM completion.

Invalid or incomplete runtime entities remain importable but are excluded from
query-scoped lists and timelines.

### Live snapshots

Engine, worker, and runtime resources may still be operating when the UI opens.
The analyzer creates analysis-only terminal events at the latest observed
timestamp. It never writes synthetic events back to the capture. Real terminal
events remain authoritative.

This permits completed queries to be inspected before the DuckDB process exits
and avoids the earlier "engine does not have an exit timestamp" failure.

### Query bundle

The query bundle supplies:

- plan and resource trees;
- operator, port, worker, and resource declarations;
- runtime FSM declarations;
- resource types and their `used_by` relationships;
- quantity specifications;
- query-relative start and duration.

If a resource-group request temporarily omits its resource type, the analyzer
resolves it from the group's actual leaf resources. This handles an
initializing Quent UI selector without accepting unknown named types.

### Entity lists

`POST /api/engines/{engine_id}/entities` supports:

- `pipeline_task`;
- `operator_invocation`;
- `chunk_transfer`;
- `temporary_block_io`.

Operator filters use pipeline containment for tasks, exact operator identity
for invocations, and causal trigger identity for temporary I/O.
`memory_account` is intentionally timeline-only because its Engine lifetime
starts before every query epoch.

### Resource timelines

Single and bulk timelines support:

- task queue by `pipeline_task` state;
- execution thread by `pipeline_task` or `operator_invocation` state;
- temporary spill/reload by `temporary_block_io` state;
- buffer-pool, live temporary, and directory bytes by `memory_account` tag;
- worker aggregation across resources of a selected type.

The analyzer reconstructs successful limit changes, but the current Quent
query bundle does not expose resource-capacity history. The UI shows absolute
bytes, not a limit line or utilization percentage. The tagged view also keeps
unused tags as zero-valued series.

Unfiltered execution-thread aggregation uses tasks as the canonical physical
occupancy layer so nested operator spans do not consume two units.

Quent's pinned generic rate builder currently computes per-nanosecond values
while the UI labels them per second. The DuckDB analyzer applies a scoped
`1,000,000,000` conversion only to temporary-I/O rate capacities. This should
eventually be fixed in Quent itself.

### Dataflow timeline

`POST /api/engines/{engine_id}/timeline/data-flow` is powered by
`ChunkTransfer`, not by temporary I/O. It exposes per-target-operator series
split by upstream operator for chunks, rows, and logical bytes per second.

`DuckDbUiAnalyzer::chunk_summary` also produces totals by physical edge, but is
currently a Rust library method rather than a separate HTTP endpoint.

## What the current model can answer

- Which physical plan and operators executed?
- Which pipelines became tasks, and where did each task run?
- How much runnable delay and yielding occurred?
- Which physical operator occupied an execution thread at a given time?
- How many chunks, rows, and logical bytes were published across each edge?
- Which operator triggered temporary spilling or reloading?
- How many temporary operations overlapped, and at what effective rate?
- Which memory tags and blocks generated temporary traffic?
- Which tags consumed DuckDB's managed-memory budget over time?
- How much live evicted data and temporary file extent existed concurrently?
- How do task, operator, dataflow, and temporary-I/O intervals correlate?

## What it cannot yet answer

- Exact CPU time or whole-system CPU utilization.
- Exact scheduler queue contents or configured-slot utilization percentage.
- Why a task blocked.
- Unique resident bytes owned by a query, operator, or block.
- Zero-copy versus copied chunk publications.
- Stable batch lineage through split, merge, reference, and fan-out.
- Downstream chunk acceptance or edge latency.
- Per-block RAM versus temporary-storage placement over time.
- Persistent-database, remote-object-store, or kernel-cache I/O.
- Complete pipeline dependency and barrier timing.

## Next candidates

### BlockPlacement and memory tiers

Model a stable block placement with states such as Loaded, Pinned, Unpinned,
Spilling, Spilled, Reloading, and Destroyed. Attach stationary states to RAM or
temporary-storage tier byte occupancy and transition states to I/O channels.

The aggregate gauges now provide reconciliation totals. They still cannot say
which block occupied either tier, how long it stayed there, or who owned it.

This is the natural complement to `TemporaryBlockIo`, but ownership must remain
unknown unless DuckDB adds explicit block provenance.

### Broadcast exchange placement

Instrument buffered exchange chunks, spool residency, consumer claims,
high-watermark blocking, and retirement. This is the closest DuckDB analog to
Sirius `BatchPlacement` because it has a real repository, fan-out, queue
residence, consumers, and backpressure.

### Operator memory grant

Instrument `TemporaryMemoryState` requested, minimum, and granted reservation,
plus the external-versus-in-memory decision. This describes policy budget, not
resident allocation. Sort, hash join, and aggregate call sites must propagate
physical operator identity.

### Task wait reason

Extend Blocked with an explicit wait resource and reason, such as asynchronous
I/O, exchange high watermark, result backpressure, or batch-order dependency.
The reason must come from DuckDB's interrupt/wait path; idle time alone is not
enough to infer it.

### Pipeline and event lifecycle

Declare pipelines, dependencies, maximum parallelism, event scheduling, task
barriers, and finalization. This would explain serial gaps and critical paths
that operator timelines alone cannot describe.

### Scheduler capacity

Track actual created scheduler slots and resize events. Existing thread-group
timelines already show active work, but Quent's current resource presentation
does not expose a clear configured-capacity denominator for saturation.

### Persistent and remote I/O

Instrument high-level scan, prefetch, and block-read paths with query context.
Keep temporary I/O separate and avoid counting the same request at file,
buffer-manager, and storage-driver layers.

### Operator and query statistics

Publish DuckDB profiler totals such as rows, bytes, timing, peak buffer memory,
temporary size, and blocked-thread time through Quent statistics events. These
are low-volume summaries that complement, rather than replace, runtime FSMs.

## Cost and safety

Telemetry is compile-time optional and runtime disabled unless an exporter is
selected. Disabled parallel hot-path hooks compile to inline no-ops.

Enabled telemetry is intentionally detailed:

- one operator invocation FSM per vectorized call;
- one chunk FSM per nonempty plan-edge publication;
- four events per temporary-I/O operation.

Large scans and spills can create millions of events. Quent currently uses an
unbounded asynchronous channel. Production deployment needs bounded loss
accounting, sampling, or windowed aggregation before enabling this detail by
default.

Storage telemetry is fail-open. The storage layer depends only on probe
abstractions; generated Quent types stay in the main telemetry layer. Custom
`DBConfig.buffer_manager` implementations bypass temporary I/O and all three
memory gauges.
