# DuckDB + Quent in the browser

This is the human-facing guide to the implemented browser application and its
extension points. The older [exploration](BROWSER_TELEMETRY_PLAN.md) includes
superseded proposals; the [implementation plan](FULL_QUENT_BROWSER_PLAN.md)
records delivery requirements. Neither replaces this description of the runtime.

## What runs where

GitHub Pages serves static files. The visitor's browser runs the database,
telemetry analysis, and full Quent UI. No native telemetry service is required.
SQL, results, and captures are not uploaded by the demo.

The engine is this fork's instrumented DuckDB compiled with Emscripten, exposed
through a small C ABI. It is **not** the standard DuckDB-Wasm JavaScript client:
its file-registration APIs and extension support are not automatically present.

```text
Browser tab
│
├─ Parent page: SQL editor, results, capture history, session coordinator
│    │
│    ├─ DuckDB worker
│    │    C++ DuckDB + Rust instrumentation → SQL result + event batches
│    │                                      │
│    │                         parent relays transferred buffers
│    │                                      ↓
│    ├─ Analyzer worker
│    │    Rust WASM → validated capture → immutable analysis revision
│    │                                      ↑
│    └─ Parent API adapter ──────────────────┘
│             ↑ requests / ↓ responses
│         MessagePort
└─ Same-origin iframe: full pinned Quent app, routes, charts, entity tables
```

The parent coordinates workers; the iframe does not access them directly.
The iframe uses an injected Quent `ApiClient`, not HTTP `/api` calls. The
analyzer facade accepts service-like routes internally, without opening a server.
The iframe isolates routing and styles, not hostile code: both documents share
an origin and trust boundary.

## One query, end to end

1. Startup loads a manifest and both WASM modules. Protocol version, schema
   hash, and build identity must agree; incompatible components are rejected.
2. Run assigns capture/run IDs and sends SQL to the persistent DuckDB worker.
   Only one run can be active. Several SQL statements may produce several
   query IDs within one capture.
3. C++ execution hooks emit typed events through the Rust browser collector.
   The query call is synchronous inside its worker; telemetry is drained after
   execution, not streamed to charts while the query runs.
4. The worker publishes the SQL result, copies encoded event batches out of
   WASM memory, and transfers their buffers through the parent to the analyzer.
   Acknowledgments bound outstanding transfers. This avoids additional message
   copies, but is not zero-copy from the engine heap.
5. A final seal declares the sequence, watermark, query IDs, and outcome.
   The analyzer validates ordering, completeness, compatibility, and budgets
   before publishing a revision. SQL success and capture completeness are
   separate facts; a result can appear before telemetry is ready.
6. The parent selects the revision and tells the iframe which engine/query to
   display. Quent requests plan, entities, and timelines through the bridge.
   Switching captures selects the corresponding immutable revision.

Each revision analyzes the bounded accumulated session, not just the latest
query. This retains engine-lived resource declarations and memory accounts.
The revision's watermark bounds interpretation of still-open lifetimes; it is
not evidence that the underlying resource was destroyed.

## Correctness boundaries

- **Schema:** `crates/telemetry/model/model.yaml` defines entities, resources,
  transitions, and attributes. Build-time generation produces Rust types and
  the C++ facade. Do not edit generated output.
- **Meaning:** hooks must describe actual engine operations. An operator call
  is not an entire operator lifetime; published chunk bytes are logical bytes,
  not network traffic. Query `Planning` is telemetry plan emission, not parser
  or optimizer time. See [INSTRUMENTATION.md](INSTRUMENTATION.md).
- **Integers:** sequence numbers, timestamps, and revisions use decimal strings
  at protocol boundaries where required. Analysis responses preserve large
  integers through lossless parsing and structured-clone `bigint`; converting
  everything to JavaScript `number` corrupts values above its safe integer range.
- **Revision isolation:** requests carry a revision. The bridge rejects stale
  calls/results; the iframe cancels requests and clears query caches when the
  revision changes. A delayed response must not repaint a newer capture.
- **Handshake:** the parent validates both origin and iframe window, then
  transfers a dedicated port. Dispatch is limited to declared API methods.
- **Failures:** SQL failure, incomplete telemetry, overflow, and forced
  cancellation remain distinguishable. Cancellation cannot interrupt a
  synchronous WASM call through an ordinary worker message; timeout fallback
  terminates the worker and requires reset, losing its database.
- **Lifetime:** full-page reload loses the database and captures. Reloading
  only Quent reconnects to the parent and preserves a matching query route.
  Copy Link is disabled because a URL cannot reconstruct an ephemeral capture.

## Source map

Paths below are relative to the repository root unless prefixed with `rust/`.

| Responsibility | Main source |
|---|---|
| Browser shell and session state | `tools/quent-browser/src/App.tsx`, `src/session.ts` |
| Worker protocol and API adapter | `tools/quent-browser/src/protocol.ts`, `src/analyzer-client.ts`, `src/worker-rpc.ts` |
| Parent iframe bridge | `tools/quent-browser/src/quent-frame.tsx` |
| Full Quent entry and transport | `tools/quent-browser/iframe/{main.tsx,rpc-client.ts,protocol.ts,reload-route.ts}` |
| Worker message handling | `tools/quent-browser/public/workers/` |
| DuckDB C ABI and WASM linkage | `tools/quent-browser/producer/` |
| Browser event collector | `rust/crates/telemetry/bridge/src/browser.rs` |
| Capture validation and revisions | `rust/crates/telemetry/web/src/lib.rs` |
| Shared native/browser analysis | `rust/crates/telemetry/analyzer/src/` |
| Native hooks and generated-facade integration | `src/main/telemetry_context.cpp`, `rust/crates/telemetry/bridge/` |
| WASM runtime adaptations | `rust/vendor/`; see its README |
| Quent checkout, patch, and bundle | `tools/quent-browser/scripts/prepare-quent.sh`, `patches/`, `vite.config.ts` |
| Publishing | `.github/workflows/QuentBrowserPages.yml` |

The actual frontend is imported from the prepared `.quent` checkout. Keep
upstream modifications in the tracked patch, not only in that ignored checkout.

## Adding features

### More instrumentation

Define the measurement and its attribution before adding hooks: what begins
and ends the entity, which resource it uses, and how failure/cancellation closes
it. Add YAML definitions, regenerate bindings, update C++ hook calls and Rust
transition/model analysis, then expose the relevant UI metadata and APIs.

Use the same semantics in native and browser builds. Rebuild producer and
analyzer together after schema changes. Test lifecycle balance, query/operator
attribution, empty and failed operations, filtering, and snapshot boundaries.
Prefer batch/operator-level events over per-row events. Candidate measurements
are listed in [BROWSER_INSTRUMENTATION_CANDIDATES.md](BROWSER_INSTRUMENTATION_CANDIDATES.md).

### A new Quent view or API method

First implement the domain result in the shared Rust analyzer. Expose it in the
browser facade, add the corresponding worker-backed `ApiClient` method, and
extend the iframe protocol allowlist and argument validation. If native service
support is intended, keep its response contract aligned too.

Update the upstream UI or tracked patch to consume that method. Do not make a
chart bypass the adapter to call workers or fetch a nonexistent HTTP service.
Test stale responses during capture switching, empty results, errors, reload,
and integers larger than JavaScript's safe range.

### Local files or remote datasets

Currently there is no upload/file-registration path, browser HTTP filesystem,
or dynamically loadable extension support. Small datasets can be pasted as
SQL inserts; see the [query walkthrough](../tools/quent-browser/QUERIES.md).

A file-loading feature needs these layers:

1. UI selects a file or URL and displays progress, limits, and errors.
2. A session-level import API coordinates bounded reads and transfers to the
   engine worker, without racing an active query or reset.
3. A worker-owned filesystem adapter registers bytes under controlled paths
   and releases them after import/reset. Do not expose arbitrary host paths.
4. SQL imports from those registered paths. CSV can use the core reader;
   Parquet requires its extension linked into this instrumented build.

Remote fetch also needs server CORS permission and explicit HTTP error handling.
Account for download buffers, engine copies, decoded tables, and telemetry—not
just compressed file size. Fetch timing is browser/network telemetry, not a
DuckDB scan measurement unless separately instrumented.

### Live charts or multiple execution threads

Live charts require a change to execution and publication, not merely faster
polling. The synchronous query currently prevents worker message processing.
Introduce safe execution yields or another producer path, then define partial
watermarks, open-state semantics, backpressure, and cancellation before allowing
analysis of an unfinished capture. Never label a partial revision complete.

Multithreading requires a threaded build, suitable browser shared-memory
deployment headers, thread-safe instrumentation, and tested event ordering.
Ordinary static Pages hosting is not sufficient evidence that these requirements
are met. Preserve the distinction between backend workers, execution threads,
and CPU cores in the model.

### Persistent or shareable captures

Add explicit export/import or persistent storage with schema/build metadata,
size limits, and compatibility validation. Database persistence and telemetry
persistence are separate features. Re-enable sharing only when the receiver
can obtain the capture; a route alone is insufficient. Treat SQL text and event
attributes as potentially sensitive before adding any upload service.

## Performance and limits

Current limits are 4 MiB per event batch, 64 MiB encoded capture/session, and at
most eight revisions. Retained snapshots have a 512 MiB estimated admission
budget based on ten times encoded bytes; this is not a hard browser-memory cap.
Session growth and replay can increase analysis cost even when the latest SQL
query is small. Raising a limit alone is not a scalability fix.

SQL's preview row limit applies after result materialization. Large results can
exhaust memory before the UI truncates them. Browser spill telemetry and NVTX
data are not supported. Memory charts describe DuckDB accounting, not total
JavaScript/WASM memory.

Keep entity pagination and timeline binning in the analyzer. The unscoped
All-types entity list merges five FSM types with global sorting/pagination;
paged conversion is bounded to candidates from each type. Preserve this when
adding types. Measure execution, event drain, replay/publication, and first
populated chart separately; “Telemetry ready” does not mean charts have rendered.

## Updating, testing, and publishing

Use the [browser README](../tools/quent-browser/README.md) for tool versions,
build commands, and local serving. A UI-only change needs a frontend rebuild;
producer, analyzer, or schema changes require the corresponding WASM rebuilds
before manifest stamping and frontend packaging.

When upgrading Quent, align the preparation-script revision, Cargo dependencies,
vendored adaptations, tracked patch, generated TypeScript bindings, and frontend
lockfile. Verify preparation from a clean checkout. Never fix compatibility by
disabling manifest checks. `pnpm build` generates bindings, stamps assets,
bundles both HTML entries, type-checks, and validates the output inventory.

Before publishing:

- Run Rust workspace tests and browser unit tests.
- Build real producer/analyzer artifacts; run the Chromium, Firefox, and WebKit
  tests with `QUENT_REAL_ANALYZER=1 QUENT_REAL_PRODUCER=1`.
- Check actual rows and populated charts, not just headings or loading skeletons.
  Exercise query failure, cancellation/reset, old captures, and iframe reload.
- Serve under a project subpath such as `/duckdb/` to catch absolute asset URLs.
  Fixture mode alone does not verify instrumented DuckDB execution.

The Pages workflow builds and deploys pushes to `quent`; manual deployment also
requires that branch and `deploy=true`. In repository settings, Pages **Source**
must be **GitHub Actions**, and the **github-pages** environment must allow the
`quent` branch. Legacy branch-root publishing can replace the demo with the
README. The demo URL is `https://dhruv9vats.github.io/duckdb/`.

Agent coordination notes remain separate in [AGENT_CONTEXT.md](AGENT_CONTEXT.md).
