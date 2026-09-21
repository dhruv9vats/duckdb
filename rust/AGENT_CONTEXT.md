# DuckDB Quent agent context

Audience: coding agents maintaining this integration. Human guidance is in
`README.md`, `INSTRUMENTATION.md`, and `QUERY_COOKBOOK.md`.

Updated: 2026-09-18.

## Authority

Read the repository `AGENTS.md` before edits. Preserve unrelated changes and
the existing untracked `telemetry_events/` capture.

Quent is pinned to commit:

```text
4a091722f1a5b7a93e5883b82b838eea43f28c3a
```

That is the head of Quent PR 699 used for this migration. Do not mix Quent
revisions across crates.

## Source of truth

`crates/telemetry/model/model.yaml` owns records, entities, FSMs, references,
resource capacities, and transitions.

`crates/telemetry/model/build.rs` must call
`quent_build_info::emit_source()` before generation. Without it, `model.qmi`
silently attributes the DuckDB schema to Quent.

```text
model/model.yaml
├─ model/build.rs  + quent-instrumentation-build → Rust emitters
├─ store/build.rs  + quent-store-build           → stored events
└─ bridge/build.rs + quent-schema-codegen-cpp    → C++ facade
```

Never edit Cargo `OUT_DIR` files or generated bridge headers. Schema changes
must be made in YAML, followed by changes to all semantic consumers.

## Crates

| Crate | Responsibility |
|---|---|
| `model` | Schema-generated instrumentation and exporters |
| `store` | Schema-generated stored events and `TransitionEvent` adapters |
| `bridge` | C++20 schema facade and staged headers |
| `analyzer` | Typed reconstruction, validation, indexes, UI analysis |
| `server` | Filesystem import, collector, analyzer API, embedded UI |

`store/src/transitions.rs` is handwritten. It must name each initial state,
legal successor, terminal state, and event sequence consistently with YAML.

The analyzer should use Quent's `AnalyzedEntity`, `AnalyzedFsmBuilder`, and
`TransitionEvent` APIs. Keep DuckDB-specific cross-entity validation and UI
adaptation outside generated code.

## Native facade

The generated public header is:

```text
duckdb-telemetry-bridge/gen/quent.hpp
```

C++ typestate transitions consume the prior handle and return the next state:

```cpp
auto created = std::move(initial).created(payload);
auto running = std::move(created).running(payload);
```

Long-lived cyclic FSMs require an explicit variant or state wrapper. Cache
observers in `TelemetryContext::Impl`; do not reconstruct them in hot hooks.
Convert raw DuckDB telemetry UUIDs to generated typed IDs only at the schema
boundary.

The bridge and telemetry-enabled `duckdb_main` compile as C++20. Telemetry-
disabled hot hooks remain inline no-ops.

## Semantic invariants

- One `Engine` means one `DatabaseInstance`.
- One `Worker` means the local backend, not one thread.
- One `QueryGroup` means one `ClientContext` connection.
- A `Query` begins only after an executable physical plan exists.
- Every execution receives fresh plan, operator, port, task, invocation, and
  transfer IDs. Never store execution IDs on reusable physical operators.
- Traverse `PhysicalOperator::GetChildren()`, including the result collector.
- Plan edges point from child output ports to parent input ports.
- `ChunkTransfer` means publication, not a copy or accepted delivery.
- `TemporaryBlockIo.trigger_operator_id` is causal, not ownership.
- `MemoryAccount` is the byte-occupancy source of truth. It is database-wide,
  not RSS and not query, task, operator, or block ownership.
- Unfiltered execution-thread occupancy uses task spans. Nested invocation
  spans must not double-count physical occupancy.
- Never synthesize FSM exits. Omit incomplete query/application FSMs. Reject
  invalid completed FSMs. Keep an Engine, Worker, or Operating resource open
  when its exit is absent; omit a resource that never reached Operating.

Quent's native `AnalyzedFsmBuilder::try_build` requires a final transition and
does not expose a validated partial FSM. Runtime resources therefore cannot use
that builder without dropping valid open resources or inventing exits. Their
wrapper delegates sequence and topology checks to each generated resource
event's `TransitionEvent` implementation. `ModelEventStore::events` does not
guarantee order, so buffer transitions per resource and sort by `(timestamp,
sequence)` before validation. Its exhaustive matches must fail to compile when
a resource event gains a state; keep ordering, invalid-edge, cross-type,
resize, bounds, and open-resource tests with it. Revisit this only if Quent
adds validated partial FSMs.

See `INSTRUMENTATION.md` before changing any meaning.

## Service contract

Read endpoints:

```text
GET /api/engines?with_metadata=true
GET /api/engines/{engine_id}
GET /api/engines/{engine_id}/contexts
GET /api/engines/{engine_id}/query-groups
GET /api/engines/{engine_id}/query_group/{group_id}/queries
GET /api/engines/{engine_id}/query/{query_id}
```

Analysis endpoints:

```text
POST /api/engines/{engine_id}/entities
POST /api/engines/{engine_id}/timeline/single
POST /api/engines/{engine_id}/timeline/bulk
POST /api/engines/{engine_id}/timeline/data-flow
```

`/contexts` returns `context_ids`. Stored schema types are PascalCase. The UI
analyzer deliberately preserves the existing snake_case API names:
`pipeline_task`, `chunk_transfer`, `operator_invocation`,
`temporary_block_io`, and `memory_account`.

`memory_account` entity rows are intentionally summaries, not memory-value
histories. Serialize at most `account_registered`, the first `accounted`, and
`exit`. Preserve the tag, clipped lifecycle timestamps, and memory-resource ID.
For multiple updates, omit capacity values and attach
`accounted_updates_total` and `accounted_updates_omitted` to the accounted
transition. Native FSMs, ranking, and timelines must continue to consume every
`Accounted` transition.

## Performance

The fine-grained streams dominate capture size:

- one `OperatorInvocation` per vectorized operator call;
- one `ChunkTransfer` per nonempty edge publication;
- one `TemporaryBlockIo` per spill or reload.

Use `FxHashMap`/`FxHashSet` for UUID-keyed analyzer indexes and `SmallVec` for
short local collections. Build joins and query indexes once during analysis.
Do not rescan all events per entity page or timeline request. Preserve bulk
timeline paths and cached query bundles.

Never serialize every `MemoryAccount::Accounted` self-transition into an
entity row. A single SF10 directory account can contain thousands of updates;
the exact history belongs in the timeline API. Keep the compact row transition
count bounded independently of update count, and expose the omitted count so
the summary cannot be mistaken for exact occupancy history.

A pre-migration 143 MB SF10 capture contained 33 tasks, 140,718 transfers,
215,988 invocations, 28,742 temporary-I/O operations, and 33 memory accounts.
Cold analysis took about 1.29 s and peaked near 1.37 GiB RSS. Treat those as a
historical regression baseline, not proof of migrated performance. The memory
ratio is a known pressure point.

The 2026-09-18 migrated SF10 validation produced 146 MB across 19 files:
31 tasks, 140,718 transfers, 217,766 invocations, 29,242 temporary-I/O
operations, 33 memory accounts, and 10 resources. Its query result matched the
uninstrumented run. Capture wall time was 6.211 s versus 5.796 s without
telemetry (7.2% overhead on that run). Release-service cold analysis took
1.15 s; cached query bundles took under 1 ms. Browser requests took 50 ms for
data flow, 80--295 ms for timelines, and 13--16 ms for memory entity pages.
Memory pages were at most 12.5 KB and three transitions per account. These are
machine- and workload-specific regression anchors.

The server importer currently collects one decoded context into a `Vec` because
the upstream analyzer-service importer requires an owned iterator. The buffer
is dropped after model construction, but cold analysis still needs O(context
events) transient memory. Fixing that requires an upstream streaming importer
contract; do not hide it behind a leaked or self-referential iterator.

## Validation order

```bash
cargo check --manifest-path rust/Cargo.toml --workspace --all-targets
cargo test --manifest-path rust/Cargo.toml --workspace
cmake -S . -B build/release \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_QUENT_TELEMETRY=ON
cmake --build build/release --target shell -j4
```

Then:

1. Capture the SF10 all-runtime workload from `QUERY_COOKBOOK.md` into a new
   temporary directory.
2. Start the server against the completed capture.
3. Call every service route above with real IDs.
4. Request entity pages, single and bulk timelines, and data flow.
5. Inspect the plan, timeline, and data-flow UI pages in a browser.
6. Record capture size, analysis latency, response size, and peak RSS.
7. Compare entity counts and resource semantics with the DuckDB hooks.

Use msgpack for the representative performance capture. Keep any failed or
partial capture separate from `telemetry_events/`.

## Change checklist

For every schema edit:

1. Update `model.yaml`.
2. Update store transition adapters.
3. Update C++ typed payloads and transitions.
4. Update analyzer builders, indexes, joins, and declarations.
5. Update server imports and context discovery.
6. Add transition, malformed-reference, and API tests.
7. Run native and Rust formatting.
8. Re-run the actual service and UI validation.
9. Update human semantics and this file separately.

## Browser maintenance

The browser application lives in `tools/quent-browser`. The worker wire
contract is `crates/telemetry/web/protocol-v1.json`; change that fixture, both
workers, and the TypeScript worker protocol together. All u64 values on that
wire are decimal strings. Analyzer responses use Quent's lossless JSON parser.

`tools/quent-browser/iframe/protocol.ts` separately owns the parent/iframe
contract. The child sends `quent-connect`; after source/origin validation the
parent sends a new port with `quent-port`. The child sends `ready` and receives
snapshots containing a string revision plus optional capture, engine, and
last-query IDs. RPC uses `rpc`, `rpc-result`, and `rpc-error`. The parent
allowlists `ApiClient` methods and rejects stale revisions before and after
dispatch. Structured clone preserves BigInt.
Do not add an HTTP API, global `fetch` patch, or duplicate protocol types.

Quent is pinned to `4a091722f1a5b7a93e5883b82b838eea43f28c3a` and built from
source by `scripts/prepare-quent.sh`. Do not replace its API transport by
patching global `fetch`. The build manifest binds protocol, schema, build ID,
and asset URLs. `scripts/stamp-manifest.mjs` derives schema/build identity and
hashes every worker and WASM asset from `build-manifest.template.json`. The
generated public manifest is ignored; real mode rejects an unstamped manifest.

Run the browser gates in this order:

```bash
cd tools/quent-browser
bash scripts/prepare-quent.sh
pnpm install --frozen-lockfile
pnpm analyzer:build
bash scripts/build-producer.sh
pnpm test
pnpm build
QUENT_REAL_ANALYZER=1 QUENT_REAL_PRODUCER=1 pnpm test:e2e
```

`pnpm build` must produce both `dist/index.html` and
`dist/iframe/index.html`; raw `vite build` is not a release build. Both Vite
bases are relative and Quent uses hash routes, so `dist` works at `/duckdb/`.
Pushes to `quent` deploy after all gates. Manual runs deploy only when the ref
is `quent` and `deploy=true`. Pages must use the GitHub Actions source, and the
`github-pages` environment must allow only `quent`.

Analyzer revisions replay all accepted session batches. Admission limits are
64 MiB encoded session bytes and 512 MiB estimated retained snapshots. The
snapshot estimate is ten times each revision's cumulative encoded source; it
is not an allocator or RSS bound. `max_history = 8` is only an upper bound.
`SESSION_LIMIT` or `SNAPSHOT_LIMIT` requires a user reset.

The producer is a custom Emscripten module with a narrow C ABI, not the
upstream `duckdb-wasm` JavaScript wrapper. The page relays transferred batches
between workers. The parent/iframe channel carries API calls and responses,
not captures. Captures and revisions remain memory-only; browser builds disable
Copy Link because it cannot reproduce them after a full-page reload.
NVTX calls return no data and browser spill telemetry remains unsupported.

On 2026-09-21 the latest native analyzer service replayed a fresh 250,000-row
group/window capture. Engines, contexts, query groups, queries, bundles,
entities, single and bulk timelines, and data flow returned HTTP 200; operator
and entity collections were nonempty. The entities response was 516,815 bytes.
