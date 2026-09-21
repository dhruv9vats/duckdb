# DuckDB and Quent in one browser page

Exploration date: 2026-09-21.

Implementation status on 2026-09-21: `tools/quent-browser` contains the pinned
Quent UI, injected transport, analyzer WASM, instrumented DuckDB Emscripten
module, bounded capture history, real browser tests, and manual Pages workflow.
Chromium, Firefox, and WebKit execute SQL and publish plan, timeline, and entity
data. Chromium also verifies an analyzer timestamp above JavaScript's safe
integer range. The implementation uses a narrow C ABI and relays batches
through the page; the proposed `duckdb-wasm` wrapper and direct
`MessageChannel` below remain design history.

## Feasibility

Yes. A static page can run an instrumented DuckDB-Wasm instance, retain its
telemetry in browser memory, analyze it locally, and display Quent beside a SQL
editor. GitHub Pages only serves the built assets. Query execution, analysis,
and visualization run on the visitor's machine.

This needs a custom DuckDB-Wasm build and changes to Quent's runtime and client
integration. The published DuckDB-Wasm binary does not contain this fork's
Quent hooks. A SQL extension cannot replace missing core execution hooks.

Recommended first release: one persistent database, one active query, a
single-threaded engine in a worker, and automatic visualization after each
query completes. Keep completed captures selectable. Add visualization during
execution and threaded execution after that path is correct.

## Evidence and limitations

The DuckDB checkout inspected was `f7fa1af315c37f2112c87d3cff7849889f835c37`.
Its telemetry dependencies pin Quent
`4a091722f1a5b7a93e5883b82b838eea43f28c3a`. The separate Quent checkout was
`a7b2f5681184a4d1e95cb4112817eec559f5db3b`. These are different revisions;
implementation must select and test one compatible dependency set.

| Component | Existing capability | Required work |
|---|---|---|
| DuckDB hooks | Typed events for plans, tasks, calls, chunks, I/O, memory | Compile this fork into DuckDB-Wasm; expose capture controls |
| YAML model | Generates Rust events and C++ facade | Retain one schema and version the capture protocol |
| Quent callback exporter | Accepts typed events in memory | Add a synchronous runtime path and bounded batching |
| Quent instrumentation runtime | Native asynchronous observers | Pinned `context.rs` explicitly rejects active `wasm32` contexts |
| DuckDB analyzer | Accepts an iterator of stored events | Remove native I/O dependencies from browser build; export worker API |
| Quent UI | Browser React components and typed client | Inject a worker-backed transport and update capture cache keys |
| Quent YAML WASM example | Parses schema YAML in the browser | Does not establish browser support for runtime telemetry analysis |
| Memory accounts | Engine-lived FSMs | Analyze valid open accounts at a snapshot watermark |
| Service | Axum/Tokio collector and HTTP endpoints | Reuse analyzer contracts without running the native service |

Relevant local sources:

- [Schema](crates/telemetry/model/model.yaml).
- [C++ hooks](../src/main/telemetry_context.cpp): `ClientState::QueryEnd`,
  `FinishQuery`, `Impl::Exit`, memory accounts, and thread attribution.
- [Analyzer](crates/telemetry/analyzer/src/lib.rs): `UiAnalyzer::try_new`,
  bundles, entity lists, timelines, and data flow.
- [Memory reconstruction](crates/telemetry/analyzer/src/memory_account.rs):
  currently requires a final FSM transition.
- [Build dependencies](crates/telemetry/bridge/Cargo.toml) and
  [bridge generation](crates/telemetry/bridge/build.rs): CXX, C++20, exporters.
- [Semantics](INSTRUMENTATION.md) and [prior measurements](AGENT_CONTEXT.md).

In Quent, inspect `crates/instrumentation/src/context.rs` and `observer.rs`,
`crates/io/callback/src/lib.rs`, and
`ui/packages/@quent/client/src/api.ts`. The pinned runtime's
`resolve_runtime()` returns `active instrumentation contexts are unsupported
on wasm32`. The callback exporter exists, but still goes through that runtime.
The client funnels requests through `fetch` and uses lossless integer parsing.
The newer checkout's `experimental/vibe/ui/yaml-wasm` exports a schema parser.

DuckDB currently neither enables the callback feature nor exposes it through
the generated C++ exporter options. Adding that bridge API is required alongside
the runtime change. Telemetry is enabled by default in this checkout's CMake
configuration, which adds the Corrosion bridge without an Emscripten guard.
The initial build gate must select the Rust cross-target explicitly and prevent
accidental linkage of a host static library into the WASM module.

The newer checkout also retains a multithreaded Tokio instrumentation runtime;
upgrading the pin alone does not establish browser support. Audit transitive
Cargo features: `quent-io` defaults include collector/network exporters, and
the current analyzer/store enable filesystem formats. Separate portable event
types and analysis from native discovery/import/export features, including
dependencies pulled in through `quent-query-engine-analyzer`. Build scripts
and YAML code generation run on the host and may still use its filesystem.

DuckDB-Wasm embeds DuckDB through a submodule and adds its own worker/binding
layer. Pin that wrapper, its patch set, this DuckDB fork, and all extensions
together; compatibility with this fork remains an implementation gate.
[DuckDB-Wasm repository](https://github.com/duckdb/duckdb-wasm),
[submodule configuration](https://github.com/duckdb/duckdb-wasm/blob/main/.gitmodules).

## Architecture

```text
Browser page
├── SQL panel: editor, Run/Cancel, result preview, capture status
└── Quent panel: plan, timelines, entities, query selector
        │ typed analysis requests / bounded responses
        ▼
Analysis worker
  Rust WASM analyzer + in-memory capture store + query caches
        ▲
        │ versioned binary batches + acknowledged watermark
        │ dedicated MessageChannel
        │
DuckDB worker
  custom DuckDB-Wasm
    → existing C++ hooks → generated Quent emitter
    → synchronous bounded capture sink → batch drain
```

Use two WASM modules with separate memories:

- Producer: DuckDB C++ plus the Rust CXX bridge, targeting
  `wasm32-unknown-emscripten`.
- Consumer: the Rust analyzer targeting `wasm32-unknown-unknown`, exported
  through `wasm-bindgen`.

This is a proposed build route, subject to the first build gate. Rust documents
Emscripten C/C++ interoperability and warns that compiler versions, exception
settings, and other flags must agree across Rust, its standard library, and
C++. Pin the toolchain and rebuild the Rust standard library if needed.
[Rust Emscripten target](https://doc.rust-lang.org/rustc/platform-support/wasm32-unknown-emscripten.html).

Do not attempt to link a standalone wasm-bindgen module into Emscripten as
though their runtimes and memories were interchangeable. If the CXX route is
unworkable, the explicit fallback is a YAML-generated C++ browser emitter with
the same wire contract, tested against Rust-generated fixtures. That is a
separate code-generation effort, not a handwritten second schema.

The UI queries an analysis facade; the facade communicates with the worker.
Components do not inspect DuckDB memory or raw event buffers. Preserve the
current logical API operations: engines, contexts, query groups, queries,
query bundles, entities, single/bulk timelines, and data flow.

Inject a transport into Quent's client boundary. Keep HTTP as the existing
default and supply a worker transport in this app. A fetch-shaped adapter can
preserve response/error semantics initially. Avoid a global `fetch` override.
Embed Quent's components and required providers in the page; its workspace
packages need a pinned source build. An iframe is unnecessary.

## Run-to-view protocol

1. Load both workers, validate schema/protocol/build identifiers, and connect
   their telemetry channel before opening the database.
2. Register capture configuration before DuckDB initialization so engine,
   worker, resource, and initial memory declarations are retained.
3. Assign an application `run_id`. Correlate it explicitly with connection,
   statement ordinal, and emitted Quent query IDs. SQL text is not an ID.
4. Run SQL in the DuckDB worker. Hot hooks append typed or encoded events to
   bounded buffers without making one JavaScript call per event.
5. On true execution completion, finish task/invocation cleanup and drain all
   event streams through a common boundary. Streaming results require a clear
   policy: finish consuming them, or keep the capture marked running until
   execution/finalization actually ends.
6. Send the remaining batches and a seal containing the final batch sequence,
   producer watermark, query IDs, outcome, and loss/completeness status.
7. The analyzer acknowledges every batch through the seal, validates the
   capture, and publishes a new immutable snapshot revision.
8. Select the corresponding query in Quent, invalidate affected caches, and
   render the plan and initial timelines. Return SQL results independently of
   rendering; show analysis progress when it takes longer.

A query result arriving on another channel does not prove telemetry has
arrived. Publication requires the analyzer's acknowledgement. Multi-statement
runs map to multiple query IDs. Parsing/binding failures may have no Quent
query because instrumentation begins at physical-plan availability; show the
SQL failure and that coverage limitation explicitly.

Proposed envelope fields: protocol version, schema hash, capture/context ID,
run ID, batch sequence, event count, payload length, timestamp range, and
overflow state. Use a framing codec shared by producer and consumer; raw
filesystem exporter buffers are not automatically a self-contained wire
protocol. Validate version, lengths, sequence gaps, and duplicate batches.

Keep timestamps and counters as Rust integers in the binary path. Preserve
`u64`/`i64` values as BigInt or the existing lossless JSON representation at the
UI boundary. Never round Unix nanoseconds through JavaScript `Number`.

Copy a completed batch from WASM memory into an owned JavaScript ArrayBuffer,
then transfer ownership across the channel. Decode it into the analyzer's
memory and release the transport buffer. This has copies at WASM boundaries;
ArrayBuffer transfer only avoids the inter-worker clone.
[Transferable objects](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Transferable_objects).

## Correctness requirements

### Open resources and memory

The persistent database stays open across queries. Engine, thread, and memory
lifetimes therefore extend beyond an individual query. Existing resource
handling preserves open resources, but `MemoryAccount` reconstruction requires
`Exit`, so simply loading a post-query capture currently omits memory gauges.

Add a validated snapshot representation for open memory-account FSMs. Retain
all observed transitions and bound the final known occupancy interval by the
acknowledged producer watermark. Mark it as snapshot-bounded; never insert a
synthetic `Exit` or zero-valued sample into the event stream. For completed
queries, clip that interval to the real query end. Validate that the snapshot
watermark is no earlier than the events it seals.

Preserve the last gauge observation at or before the query start, relevant
updates within the window, resource declarations, and capacity changes.
Without that carry-in value, a later query can incorrectly start at zero.
Retention must preserve this dependency closure before older captures are
evicted. Historical snapshots must remain stable while the database continues.

Memory account entity rows must remain compact. Display lifecycle/snapshot
metadata and omitted-update counts; keep exact values in timeline analysis.
For resource capacity changes, either expose the bound history or label a
scalar bound as the latest observation, rather than implying it held throughout.

### Time, threads, and telemetry meaning

Provide a browser clock adapter with one defined time origin, monotonic
ordering, and documented effective resolution. Browser timestamps can tie;
test sequence wrap at tied timestamps before choosing the ordering key. If
necessary, add a wider producer ordinal without claiming extra clock precision.
Use a browser-supported UUID/entropy path. Test initialization, reset, and
multiple workers for collisions.

Label the default execution lane as the DuckDB worker. Do not represent a
browser fallback thread identifier as an observed CPU core. Preserve current
semantics: operator wall time, task wall time, logical chunk publication,
database-wide charged memory, and causal spill attribution.

Browser file systems and buffer managers may differ from native DuckDB.
Confirm that the selected DuckDB-Wasm build reaches each hook. Show unavailable
spill/storage metrics as unsupported when their paths are absent. A virtual
memory filesystem does not establish physical disk I/O.

### Failure and retention

Capture states should include running, sealed, incomplete, overflowed, failed,
and evicted. SQL success and telemetry completeness are separate outcomes.
If a bounded buffer fills, continue the SQL query but mark capture truncation;
never silently drop events and publish an apparently exact timeline.

Do not block a full producer queue waiting for a drain scheduled on the same
busy worker. For the completion-only path, enforce a capture budget and report
overflow. Larger lossless captures require actual execution-time drain points.
In a threaded phase, sealing needs a barrier across every emitting thread and
stream; observing Query Exit on one stream is insufficient.

Prefer disabling fine-grained capture at a declared cutoff to arbitrary event
sampling that breaks FSMs. Any later sampling mode must preserve complete
entity lifecycles, references, and explicit sampling metadata.

Cancellation uses DuckDB-Wasm's pending-query cancellation path where available.
A synchronous call can prevent the worker from processing a cancel message.
Terminating that worker is a fallback that loses its database and leaves an
incomplete capture; the UI must state that outcome.

## Performance approach

The native SF10 measurements are warning signs for browser sizing, not browser
benchmarks: about 146 MB of encoded telemetry, and an earlier analysis peak
near 1.37 GiB for a comparable capture. Do not make SF10 the default web demo.

Start with deterministic generated tables and small aggregation, join, and
window examples. Bound input data, result preview, raw capture bytes, retained
history, and analyzer memory separately. Free raw batches after indexing when
export/replay is disabled. Keep structural declarations and baseline gauges.

Use named configuration values for batch bytes, capture budget, history count,
timeline bins, and result rows. Tune them from measurements. Batch serialization,
clock calls, memory copies, allocations, and result rendering each need a
separate timing measurement. Avoid repeated full-history reconstruction.

Proposed acceptance targets for a specified desktop browser and small demo
capture: p95 query-completion-to-first-view below 500 ms; cached interactions
below 100 ms; no telemetry-processing main-thread task above 50 ms. These are
targets to test, not promised results. Report engine overhead against the same
custom WASM build with capture disabled and use repeated warmed runs.

Progressive visualization requires producer drain points during execution.
A timer in a busy DuckDB worker cannot interrupt synchronous WASM. Use existing
pending-query execution slices if they provide adequate yield points; otherwise
add deliberate batch flush calls with measured cost. Initial delivery remains
after query completion, which already meets the rapid run-to-view workflow.

## GitHub Pages deployment

Deploy static HTML, JavaScript, CSS, WASM modules, worker scripts, and small
fixtures through GitHub Actions. Serve assets under the repository base path;
use hash navigation or a root-only route so refreshes do not require server
rewrites. Pin artifacts by content hash and include a build manifest.
[GitHub Pages overview](https://docs.github.com/en/pages/getting-started-with-github-pages/what-is-github-pages).

Use the single-threaded exception-handling DuckDB-Wasm variant first. It still
executes in a Web Worker and leaves the page responsive. Threaded `coi` builds
require cross-origin isolation, SharedArrayBuffer, and a pthread worker;
extensions must match the selected platform.
[DuckDB-Wasm deployment](https://duckdb.org/docs/stable/clients/wasm/deploying_duckdb_wasm).

GitHub Pages does not provide an ordinary per-site custom-header configuration.
A same-origin COI service worker is a possible later solution, with first-load
reload and browser/cache caveats. Enable threading only after checking
`crossOriginIsolated`; retain single-threaded fallback. Test the published site,
not just a development server that supplies headers.
[COI service worker](https://github.com/gzuidhof/coi-serviceworker).

Build in CI, then upload the finished Pages artifact. Keep the site below
GitHub's 1 GB published-site limit and account for its soft bandwidth quota.
Prefer bundled compatible Parquet/JSON support for initial examples; remote
extensions must match the custom DuckDB version and WASM ABI. Same-origin data
fixtures simplify CORS and optional isolation.
[Pages limits](https://docs.github.com/en/pages/getting-started-with-github-pages/github-pages-limits).

## Implementation sequence and gates

| Phase | Deliverable | Acceptance gate |
|---|---|---|
| 0: freeze contracts | Version manifest, dependency matrix, protocol and ownership spec | One chosen DuckDB-Wasm/DuckDB/Quent/toolchain combination; agreed snapshot semantics |
| 1A: producer spike | Tiny Emscripten C++ program using generated Quent emitter and memory sink | Real ordered events exported in a browser without native threads, filesystem, or collector |
| 1B: analyzer spike | `duckdb-telemetry-web` WASM crate using deterministic existing fixtures | Browser responses agree with native analyzer for bundles, entities, timelines, data flow |
| 1C: UI seam | Injectable client transport and split-panel shell using fixture responses | Quent renders through worker RPC, handles errors and selection correctly |
| 2: engine integration | Custom DuckDB-Wasm plus capture controls and query ID correlation | SELECT, aggregate, join, prepared query, and repeated runs produce valid telemetry |
| 3: persistent snapshots | Seal/ack protocol, open memory analysis, carry-in gauges, revision cache keys | Second query works without closing DB; no false exits or stale first-query data |
| 4: resource limits | Batching, retention, overflow status, cancellation/reset | Bounded memory across repeated runs; loss and termination never appear complete |
| 5: deployment | Reproducible release assets, Pages workflow, browser tests | Published project-subpath URL works with no analysis server |
| 6: optional live/threaded | Incremental analysis and validated COI build | Correct watermarks, thread identity, bounded queues, measured overhead |

Phase 1 is a feasibility gate. If the producer cannot link or run correctly,
resolve its toolchain/runtime design before building out the engine UI. The
analyzer and UI spikes can proceed independently using the same fixtures.

Expected change locations:

- DuckDB: telemetry initialization/configuration, capture boundary and drain
  APIs, bridge Cargo features, CMake cross-compilation settings, analyzer
  snapshot support, and a new browser facade crate.
- Quent: synchronous in-memory observer runtime, target-specific clock/UUID
  adapters where necessary, native-I/O feature separation, client transport
  injection, and possibly validated partial-FSM support.
- DuckDB-Wasm fork: DuckDB source pin, capture configuration/bindings, worker
  request/response extensions, query correlation, and artifact exports.
- Web app: a proposed `tools/quent-browser/` package, SQL panel, worker
  coordinator, Quent providers, capture state, fixture data, and Playwright tests.
- CI/docs: proposed Pages build workflow, browser setup instructions, protocol
  reference, and separate agent implementation notes.

## Parallel-agent execution plan

Implementation uses only `gpt-5.6-sol` and `gpt-5.6-terra`. Astra may perform
exploration or planning; it does not edit implementation or tests. A Sol agent
coordinates implementation, integration, and acceptance checks.

With four concurrent slots, use one Sol coordinator and three bounded workers:

| Wave | Terra worker A | Sol worker B | Sol/Terra worker C |
|---|---|---|---|
| Contract/build spike | Emscripten/CXX and synchronous sink | Analyzer WASM feature audit and fixture parity | Quent transport injection and fixture UI |
| Engine/snapshots | DuckDB-Wasm hooks, drain, correlation | Open-account snapshots, retention, watermarks | SQL panel, query selection, cache revisions |
| Validation/deployment | Producer performance and cancellation tests | Native/browser semantic parity and malformed captures | Pages packaging, browser matrix, screenshots |

Freeze protocol types before parallel integration. Give each worker explicit
file ownership; shared Cargo manifests, generated bindings, and protocol
changes go through the coordinator. Do not let agents regenerate into the same
build output directory. Use isolated build targets/worktrees when necessary.
Review each area with a different Sol/Terra agent before integration.

The coordinator owns dependency pins and a checklist of unresolved gates.
Agents return patches, test commands/results, performance measurements, and
remaining limitations. Human documentation describes operation and semantics;
agent notes record ownership, build pitfalls, and follow-up context separately.

## Verification matrix

- Native and browser analyzers consume identical deterministic event fixtures;
  compare IDs, reference integrity, state spans, memory values, rates, and data
  flow. Real native/WASM runs compare invariants and SQL results, not timings
  or task counts, which legitimately differ.
- Cover SELECT, aggregation, join, window, prepared reuse, multiple statements,
  syntax/binder errors, execution failure, empty output, cancellation, reset,
  memory-limit changes, capture overflow, and rapid successive runs.
- Cover out-of-order batches, duplicates, missing batches, sequence wrap,
  invalid references, truncated frames, schema mismatch, and stale revisions.
- Test open memory-account carry-in and final intervals without synthetic exits.
  Verify previous snapshots remain unchanged after another query executes.
- Test browser integer round trips beyond JavaScript's exact integer range.
- Run Chromium, Firefox, and WebKit tests against static release artifacts;
  inspect plan direction, resource labels, visible values, errors, zoom/filter
  behavior, and the run-to-view latency. Treat rendering screenshots as one
  check alongside numerical API assertions.
- Load the actual GitHub Pages URL, including a cold cache and project subpath.
  Verify worker/WASM content types, optional extension loading, asset versions,
  no localhost requests, and no telemetry upload.
- Record startup/download costs, engine slowdown, batch copies, analysis time,
  UI latency, capture bytes, and peak memory under bounded repeat-run workloads.

Completion means a user can open the static page, run several queries in one
database session, and immediately select trustworthy telemetry for each run,
with clear handling of unsupported metrics, failed queries, and truncated
captures.
