# Full Quent browser demo plan

Status: implementation contract for the `quent` branch.

## Outcome

Publish one static, same-origin application at
`https://dhruv9vats.github.io/duckdb/`. The parent runs instrumented DuckDB,
captures telemetry, and owns immutable analyzer revisions. A same-origin iframe
renders the pinned Quent application through an allowlisted in-memory API.
Queries, captures, and API calls never leave the browser.

The shipped iframe is Quent's full route tree, navigation, theme, plan,
timeline, entity, and data-flow UI. A bespoke partial dashboard does not meet
this contract. Full UI coverage does not imply unsupported telemetry: NVTX
adapters return `null`, and browser spill views remain unavailable. Fixture
mode remains test-only and visibly labelled.

## Ownership

| Area | Owner | Files | Required handoff |
|---|---|---|---|
| DuckDB producer, telemetry worker, analyzer revisions | existing browser integration | `tools/quent-browser/{producer,public/workers,src/analyzer-client.ts,src/session.ts}` | revision snapshot and query completion events |
| Full pinned Quent build and iframe adapter | full-Quent app workstream | `.quent` preparation, Quent patch/package scripts, iframe entry and output | iframe URL, build command, ready/error state, bridge client |
| Parent shell and bridge | demo-shell workstream | parent React shell, protocol implementation, unit and browser tests | query controls, iframe lifecycle, revision switching |
| Release plan, workflow, operator docs | deployment workstream | this file, `.github/workflows/QuentBrowserPages.yml`, browser and Rust READMEs | branch/manual policy, Pages artifact, deployment URL |

No workstream may hand-edit generated schema or Quent output. Shared protocol
changes require synchronized parent, child, and tests in one changeset.

## Browser boundaries

```text
parent document
  SQL controls -> DuckDB worker -> transferred telemetry batches
                                   |
                                   v
                              analyzer worker
                                   |
                         immutable revision N
                                   |
             allowlisted postMessage request/response
                                   |
                                   v
                     same-origin iframe
                     pinned full Quent application
```

The parent is authoritative for the active query, capture state, revision list,
and iframe URL. The analyzer worker is authoritative for snapshot contents.
The iframe owns only Quent routing and presentation state.

There is no HTTP API, service worker proxy, global `fetch` replacement, or
iframe access to DuckDB/analyzer workers. Vite must emit relative asset URLs.
Quent uses hash routing so refreshes and direct navigation remain within the
GitHub Pages project path. The root and a representative query hash route must
work both at `/` in local tests and at the production `/duckdb/` subpath.

Captures and revisions are memory-only and disappear on full-page reload.
Browser builds disable Quent's Copy Link action because a URL cannot reproduce
an in-memory capture. An iframe-only reload preserves a matching hash route
while the parent session remains alive.

## Shared-port protocol

The iframe sends `{ type: 'quent-connect' }`. The parent accepts it only after
validating `event.origin === location.origin` and
`event.source === iframe.contentWindow`, creates one `MessageChannel`, retains
one port, and sends the other with `{ type: 'quent-port' }`. The child replies
with `{ type: 'ready' }`. All API traffic then uses that channel.

Requests have this logical shape:

```ts
type ApiRequest = {
  type: 'rpc';
  id: string;
  revision: string;
  method: ApiMethod;
  args: unknown[];
};
```

The other messages are:

```ts
type Snapshot = {
  type: 'snapshot';
  revision: string;
  captureId?: string;
  engineId?: string;
  lastQueryId?: string;
};
type RpcResult = { type: 'rpc-result'; id: string; revision: string; result: unknown };
type RpcError = { type: 'rpc-error'; id: string; revision: string; error: string };
```

`iframe/protocol.ts` owns these types. Parent and child import them; neither may
declare a parallel copy. The snapshot field is `lastQueryId`, not `queryId`.

`ApiMethod` is a closed string union checked against the methods required by
the pinned Quent `ApiClient`. The parent rejects unknown names, arguments that
fail the method-specific arity/type schema, and revisions other than the active
string revision. It calls the analyzer snapshot selected by `revision`; it
never silently substitutes the newest snapshot.

Responses contain the request ID and revision plus a result or error string.
Values cross the port with the structured-clone algorithm. `bigint` remains
`bigint`; JSON stringification is forbidden on this bridge.

Changing the active revision rejects pending child requests and the parent
rejects a response if its revision became stale during the call. Recreating or
navigating the iframe closes the prior port. Reset publishes revision `"0"`.
Evicted, overflowed, or unavailable capture states remain visible in the shell;
they are not replaced with newer or fixture data.

## Phases and acceptance gates

### 1. Pin and package the full Quent application

- Prepare the existing pinned Quent commit reproducibly; no network fetch is
  allowed after dependency installation.
- Export the real route tree, providers, navigation, theme, and feature pages
  from a dedicated iframe entry.
- Replace only Quent's API construction seam with the shared-port client.
- Emit the iframe beneath the same `dist` tree as the parent, with relative
  assets and hash routes.

Accepted when a production build contains separate parent and iframe HTML,
hashed Quent chunks, worker scripts, both WASM modules, and the stamped runtime
manifest. A test must fail if the iframe entry, a representative full-Quent
route chunk, or any runtime asset is absent.

### 2. Implement the parent shell and bridge

- Keep SQL editing, execution state, result preview, capture health, reset,
  revision selection, and limits visible beside the iframe.
- Allocate most viewport width and height to Quent while preserving usable SQL
  controls at desktop and narrow widths. The SQL panel can collapse; the Quent
  viewport remains usable without horizontal page overflow on narrow screens.
- Capture the revision returned by the engine/analyzer publication path and
  bind the iframe to that immutable revision.
- Validate the source/origin handshake and every allowlisted API call. The
  iframe uses same-origin embedding for routing and style isolation; it is not
  described as a security boundary.

Accepted when unit tests cover handshake rejection, method/argument rejection,
BigInt round-trip, errors, out-of-order responses, stale revision, iframe
reload, reset, and concurrent request IDs.

### 3. Prove real end-to-end behavior

- Build the real C++ Emscripten producer and Rust analyzer WASM.
- Run an aggregate query and a plan with several operators.
- Navigate the iframe through representative Quent routes and assert real
  engine/query IDs, plan nodes, entity rows, timeline data, and data flow.
  Chart acceptance waits for a rendered nonempty series/mark and stable layout,
  not merely an SVG or canvas element count.
- Change revisions while a request is in flight and prove stale data is not
  rendered. Reload a deep hash route and prove the app returns to that route.
- Run Chromium, Firefox, and WebKit. Fixture tests supplement but never replace
  the real producer/analyzer matrix.

Accepted when all browsers pass without console/page errors and retain traces
and screenshots on failure. At least one success screenshot must show the SQL
shell beside a populated full-Quent route. Narrow-viewport coverage collapses
and restores SQL controls without losing the selected revision or route.

### 4. Package and deploy

- One package command builds bindings, stamps runtime assets, type-checks, and
  builds both parent and iframe. CI invokes that command, not raw `vite build`.
- CI verifies the output inventory and runs unit plus real E2E gates before
  uploading `tools/quent-browser/dist` as the Pages artifact.
- A push to exactly `quent` builds and deploys. `workflow_dispatch` always
  builds, but deploys only when its explicit boolean input is true and the
  selected ref is exactly `refs/heads/quent`.
- Deployment uses the `github-pages` environment. Build permissions are
  `contents: read`; deployment adds only `pages: write` and `id-token: write`.
  Concurrency serializes deployments and cancels superseded runs.

Accepted when an artifact served under `/duckdb/` passes its real browser test,
the deployment job cannot run for another branch, and the environment URL is
reported by `actions/deploy-pages`.

## Failure behavior

Build fails for a dirty/missing Quent pin, generated-binding drift, unstamped
manifest, missing parent/iframe/runtime asset, type error, unit failure, real
browser failure, or Pages packaging failure. Deployment never runs after a
failed or skipped build.

Runtime errors remain specific: worker start, DuckDB query, telemetry seal,
analyzer revision, bridge protocol, unsupported API method, evicted revision,
and iframe render failures have distinct visible states. The shell preserves
the SQL text and last valid revision where safe. It never converts a real-mode
failure into fixture data.

Quent's unscoped Entities **All types** filter sends a null entity type. The
analyzer merges all five supported types before applying global sorting and
pagination. Scoped null requests retain the resource-specific default used by
long-entity rows.

## Security

- Same-origin iframe and exact source-window validation are mandatory. The
  iframe is not a hostile-content sandbox or separate trust boundary.
- The child receives a port, not worker references or unrestricted callbacks.
- API methods are a closed enum; property-path dispatch and arbitrary function
  invocation are forbidden.
- Error payloads omit stack traces and filesystem/build-runner paths.
- SQL executes only in local DuckDB WASM. No capture or result is uploaded.
- Pages dependencies and toolchain versions remain pinned by the workflow and
  lockfiles.

## Performance budgets

Preserve current limits: 4 MiB per telemetry batch, 64 MiB encoded session, a
512 MiB estimated snapshot admission budget, at most eight revisions, and one
active query. The snapshot estimate is not a hard allocator or RSS limit.

The iframe bridge must not copy whole captures. It sends API arguments and
responses only. Revision publication must not block on iframe rendering.
Virtualized Quent views stay enabled. E2E currently records
query-to-revision-ready as a diagnostic. Recording
revision-ready-to-first-populated-route remains planned. Proposed first CI
budgets are 30 seconds per smoke query and 15 seconds per first populated route
on each browser. Add enforcement only after repeated CI measurements; then
retain the trace, screenshot, and timing output on a breach.

The 2026-09-21 local production build emitted a 1.12 MB minified iframe entry
(349 KB gzip) plus a 638 KB shared query-bundle chunk (218 KB gzip). Vite warns
above 500 KB. Treat these as initial transfer baselines; split routes or shared
modules before accepting material growth, then set enforceable size budgets
from repeated release builds.

## Release operation

Repository settings require one manual action: **Settings → Pages → Build and
deployment → Source → GitHub Actions**. The `github-pages` environment must
allow the `quent` branch; required reviewers, if configured, intentionally
pause deployment.

Normal release: push to `quent`. Manual rebuild/deploy: run **Quent browser
Pages** on the `quent` branch with `deploy=true`. Manual runs on other refs may
verify an artifact but cannot deploy. Expected public URL:
`https://dhruv9vats.github.io/duckdb/`.

## Completion checklist

- [x] Full pinned route tree, providers, theme, and navigation load in iframe.
- [x] Parent/child protocol tests cover validation and revision isolation.
- [x] Real producer/analyzer E2E passes Chromium, Firefox, and WebKit.
- [x] Production artifact inventory proves parent, iframe, Quent chunks,
      workers, WASM, and stamped manifest are present.
- [x] `/duckdb/` test covers relative assets and deep hash navigation.
- [x] Static workflow validation restricts push deployment to `quent` and
      manual deployment to an explicit request from that branch.
- [ ] Production Pages deployment has been performed.
- [ ] Pages source and environment branch allowance are configured once.
- [x] README commands match the commands executed by CI.

Final validation passed 31 Rust tests, 25 UI/unit tests, and 15 browser tests.
The native HTTP suite passed 922 assertions, including globally sorted and
paged unscoped **All types** entities. An exact `/duckdb/` probe ran a real
`SELECT 42` and preserved the Entities hash across an iframe-only reload. The
success screenshot shows the full plan, task-queue timelines, and the 64 KiB
buffer-memory series. Production deployment has not been performed.
