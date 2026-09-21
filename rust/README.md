# DuckDB Quent telemetry

This workspace captures DuckDB execution as Quent events, analyzes captures,
and serves the Quent UI.

Human documentation:

- [INSTRUMENTATION.md](INSTRUMENTATION.md): event and resource semantics.
- [QUERY_COOKBOOK.md](QUERY_COOKBOOK.md): capture and validation workloads.
- [INSTRUMENTATION_CANDIDATES.md](INSTRUMENTATION_CANDIDATES.md): proposed
  coverage, cost, and priority.

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
