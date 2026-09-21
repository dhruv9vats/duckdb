# Browser instrumentation candidates

These are future hooks, not current browser telemetry. Each proposal states
what can be observed without turning implementation detail into a claim.

## Optimizer rewrites

- **Hook:** `src/optimizer/optimizer.cpp` and individual rule application.
- **Lifecycle:** declare a logical node, then emit `Before → Applied → After`
  with the rule and old/new lineage.
- **Cost:** one event per applied rule; omit unchanged attempts by default.
- **Do not infer:** object addresses are not stable node identities, and an
  applied rewrite does not prove that it improved runtime.

## Vector throughput

- **Hook:** `src/parallel/pipeline_executor.cpp` at `GetData`, `Execute`,
  `Sink`, and `FinalExecute` boundaries.
- **Lifecycle:** attach sampled input/output rows and logical bytes to the
  existing operator invocation.
- **Cost:** at most one update per `DataChunk`; never emit per row.
- **Do not infer:** logical bytes are not memory traffic, copied bytes, or
  storage bandwidth.

## Hash build, probe, and spill

- **Hook:** `JoinHashTable::Build` and `Finalize` in
  `src/execution/join_hashtable.cpp`; `RadixPartitionedHashTable::Sink`,
  `Combine`, `Finalize`, and `GetData` in
  `src/execution/radix_partitioned_hashtable.cpp`.
- **Lifecycle:** joins use `Created → Building → Finalized → Probing → Complete
  → Exit`. Grouped aggregates use `Created → Aggregating → Combining →
  Finalized → Scanning → Exit`. Repartition and spill are separate transitions
  with stable partition lineage.
- **Cost:** one event per phase or partition transition; aggregate tuple
  counts rather than tracing inserts and probes.
- **Do not infer:** hash-table size is not resident memory, and the query that
  triggers eviction does not own the evicted bytes.

## Scheduler waits

- **Hook:** task enqueue/dequeue/steal, `PipelineTask` blocked results, and the
  component-specific wake path.
- **Lifecycle:** `Eligible → Running → Waiting(reason) → Eligible`, preserving
  task, worker, and optional resource identity.
- **Cost:** one event per state change; use enum reasons and allocate nothing
  while scheduler locks are held.
- **Do not infer:** queue delay is not CPU starvation, and a wake means
  eligible rather than immediately running.

## Buffer pressure

- **Hook:** `src/storage/buffer/block_handle.cpp` and
  `src/storage/standard_buffer_manager.cpp` at load, eviction, spill, reload,
  and destruction boundaries.
- **Lifecycle:** `Registered → Loaded ↔ Spilled → Destroyed → Exit`, with
  sampled pin pressure and an explicit eviction cause.
- **Cost:** avoid per-pin events unless a focused sampling mode is active;
  emit after lock-protected accounting completes.
- **Do not infer:** buffer accounting is not RSS, filesystem duration is not
  device time, and an eviction cause is not block ownership.

## Persistent block I/O

- **Hook:** `src/storage/single_file_block_manager.cpp` read/write requests
  and completion.
- **Lifecycle:** `Requested → Active → Completed/Failed → Exit`, carrying the
  block range, bytes, database, and query context when present.
- **Cost:** one entity per physical request; reference it from a buffer miss
  instead of counting the same bytes twice.
- **Do not infer:** the kernel page cache may satisfy a request, and requester
  identity does not establish data ownership.

The current browser producer is single-threaded and has no spill I/O. Missing
scheduler, spill, or storage events therefore indicate unsupported coverage,
not proof that no such work exists in other DuckDB builds.
