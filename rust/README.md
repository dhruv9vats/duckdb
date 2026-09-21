# DuckDB Quent telemetry

This workspace captures DuckDB execution as Quent events, analyzes captures,
and serves the Quent UI.

Human documentation:

- [BROWSER_ARCHITECTURE.md](BROWSER_ARCHITECTURE.md): implemented browser stack,
  correctness boundaries, feature extensions, testing, and deployment.
- [INSTRUMENTATION.md](INSTRUMENTATION.md): event and resource semantics.
- [QUERY_COOKBOOK.md](QUERY_COOKBOOK.md): capture and validation workloads.
- [INSTRUMENTATION_CANDIDATES.md](INSTRUMENTATION_CANDIDATES.md): proposed
  coverage, cost, and priority.
- [BROWSER_INSTRUMENTATION_CANDIDATES.md](BROWSER_INSTRUMENTATION_CANDIDATES.md):
  browser-focused hook and inference constraints.
- [FULL_QUENT_BROWSER_PLAN.md](FULL_QUENT_BROWSER_PLAN.md): full iframe UI,
  bridge, verification, and Pages release contract.

Agent-only documentation:

- [AGENT_CONTEXT.md](AGENT_CONTEXT.md): generated-code boundaries, migration
  constraints, and validation state.
- [HANDOFF.md](HANDOFF.md): historical investigation notes.

## Architecture

`crates/telemetry/model/model.yaml` is the source of truth.

```text
model.yaml
├─ instrumentation build → Rust emitters
├─ store build           → serialized event types
└─ C++ schema codegen    → typed native handles
                              ↓
DuckDB hooks → exporter → store → analyzer → HTTP API → UI
```

Do not hand-edit generated types. Change the YAML, transition adapters, native
hooks, or analyzer as appropriate.

The model covers:

- engine, worker, connection, query, plan, operator, port, and edge structure;
- pipeline tasks, operator calls, and plan-edge chunk publications;
- temporary spill and reload operations;
- execution threads, runnable tasks, temporary-I/O rates, and memory gauges.

## Build

```bash
cd /path/to/duckdb
cmake -S . -B build/release \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_QUENT_TELEMETRY=ON
cmake --build build/release --target shell -j4

cargo check --manifest-path rust/Cargo.toml --workspace --all-targets
```

The bridge requires C++20. Telemetry is runtime-disabled unless
`QUENT_EXPORTER` is set.

Supported file exporters are `ndjson`, `msgpack`, and `postcard`. `collector`
sends events to `QUENT_COLLECTOR_ADDRESS`.

## Capture

```bash
events_dir=$(mktemp -d /tmp/duckdb-quent.XXXXXX)

QUENT_EXPORTER=msgpack \
QUENT_OUTPUT_DIR="$events_dir" \
build/release/duckdb -c \
    "SET threads=4;
     CREATE TABLE t AS SELECT i FROM range(5000000) r(i);
     SELECT sum(i * i) FROM t WHERE i % 7 = 0;"
```

Start the service after DuckDB exits:

```bash
cargo run --manifest-path rust/Cargo.toml \
    -p duckdb-telemetry-server --features ui -- \
    --output-dir "$events_dir" \
    --collector-address 127.0.0.1:7836 \
    --analyzer-address 127.0.0.1:8080
```

Open `http://127.0.0.1:8080`. Restart the service after replacing or extending
a capture because analyzed engines are cached.

## API smoke test

Discover IDs in this order:

```bash
curl -s 'http://127.0.0.1:8080/api/engines?with_metadata=true'
curl -s 'http://127.0.0.1:8080/api/engines/{engine_id}'
curl -s 'http://127.0.0.1:8080/api/engines/{engine_id}/contexts'
curl -s 'http://127.0.0.1:8080/api/engines/{engine_id}/query-groups'
curl -s 'http://127.0.0.1:8080/api/engines/{engine_id}/query_group/{group_id}/queries'
curl -s 'http://127.0.0.1:8080/api/engines/{engine_id}/query/{query_id}'
```

`/contexts` returns `context_ids`. The query response supplies resource IDs and
the query-relative duration needed by timeline requests.

Runtime instances use:

```text
POST /api/engines/{engine_id}/entities
POST /api/engines/{engine_id}/timeline/single
POST /api/engines/{engine_id}/timeline/bulk
POST /api/engines/{engine_id}/timeline/data-flow
```

Example execution-thread request:

```bash
curl -s -X POST \
    -H 'content-type: application/json' \
    'http://127.0.0.1:8080/api/engines/{engine_id}/timeline/single' \
    -d '{
      "entry": {"Resource": {
        "resource_id": "{execution_thread_id}",
        "long_entities_threshold_s": null,
        "entity_filter": {"entity_type_name": "operator_invocation"},
        "application": {"operator_ids": []},
        "config": {"num_bins": 100, "start": 0, "end": {duration_s}}
      }},
      "app_params": {"query_id": "{query_id}"}
    }'
```

## UI meanings

| Selection | Meaning |
|---|---|
| `runnable-pipeline-tasks` + `pipeline_task` | Runnable approximation |
| `thread-*` + `pipeline_task` | Task wall time |
| `thread-*` + `operator_invocation` | Physical-operator call wall time |
| `temporary-spill` or `temporary-reload` | Buffer-manager service rate |
| `buffer-pool-memory` | Managed-memory charge |
| `temporary-storage` | Live evicted representations |
| `temporary-directory-storage` | Accounted file extent |
| Plan data-flow overlay | Published chunks, rows, and logical bytes |

Execution-thread lanes are OS threads, not CPU cores. Task-queue occupancy is
an approximation. Chunk bytes are logical bytes, not copies or bandwidth.
Memory gauges are database-wide DuckDB accounting, not query ownership or RSS.

## Browser application

`tools/quent-browser` runs SQL and telemetry analysis in separate browser
workers. A same-origin iframe renders the pinned full Quent application. Its
allowlisted `ApiClient` uses a transferred port rather than HTTP or a global
`fetch` override. Captures are ephemeral; a full-page reload loses them.
Browser builds disable Copy Link rather than emit a URL that cannot reconstruct
an in-memory capture.

The initial browser capability set is intentionally narrow: one query at a
time, one persistent single-threaded database, no spill-I/O telemetry, bounded
4 MiB batches and a 64 MiB encoded session. SQL results may appear before
telemetry; only the telemetry view waits for an acknowledged capture seal.
Failed, cancelled, incomplete, and overflowed captures remain distinct.
The result row limit bounds only the rendered preview. DuckDB materializes the
full result before serialization, so it is not a query-memory limit; aggregate
queries are safer for demonstrations.

Each immutable revision replays the bounded session. Retained revisions use a
512 MiB estimated snapshot budget at ten times their encoded source bytes.
This is admission accounting, not an allocator or RSS cap. Eight revisions is
an upper bound, not a retention promise; the application requires reset when a
session or snapshot budget is exhausted.

See `tools/quent-browser/README.md` for reproducible builds and tests. Fixture
mode is visibly labelled and is not evidence of DuckDB execution.
Pushing `quent` publishes the verified static artifact to
`https://dhruv9vats.github.io/duckdb/` after Pages is set to GitHub Actions and
the `github-pages` environment permits that branch.
