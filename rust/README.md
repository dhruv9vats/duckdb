# DuckDB Quent telemetry harness

This workspace defines DuckDB's Quent query-engine model, generates its C++
instrumentation bridge, analyzes captured events, and serves them to the Quent
UI. It mirrors the corresponding workspace in Sirius and is wired into DuckDB
through the `BUILD_QUENT_TELEMETRY` CMake option.

The model contains the standard query-engine entities plus three DuckDB runtime
FSMs:

- `pipeline_task` follows one scheduled pipeline task through running, partial
  yields, blocking, and finalization.
- `chunk_transfer` records every non-empty chunk published to a physical-plan
  edge, including its task, operator and port IDs, rows, and logical bytes.
- `operator_invocation` measures one source, execute, final-execute, or sink
  call, including its task, physical operator, row counts, and logical bytes.

Pipeline tasks use the worker's `task_queue` while queued or ready, and both
tasks and operator invocations use the stable `execution_thread` on which they
run. These usages power resource timelines.

The included sample emits a connected physical plan so it can be rendered by
the Quent UI.

## Validate the workspace

```bash
cd rust
cargo check --workspace --all-targets
```

## Generate sample plan telemetry

```bash
cd rust
cargo run -p duckdb-telemetry-model --example emit_sample_plan -- \
    --exporter ndjson --output-dir events
```

The exporter creates one context directory beneath `events/`. Start the analyzer
server against that directory:

```bash
cargo run -p duckdb-telemetry-server --features ui -- \
    --output-dir events
```

The server listens for collector traffic on port 7836 and serves the analyzer
and embedded UI on port 8080 by default.

## Generate the C++ bridge

Enable `BUILD_QUENT_TELEMETRY` to build and link the bridge with both DuckDB
library targets:

```bash
cmake -S . -B build/reldebug -DBUILD_QUENT_TELEMETRY=ON
cmake --build build/reldebug
```

The bridge build script writes generated C++ sources under `gen/` and headers
under `include/`; both directories are ignored because they are build products.

## Capture a DuckDB query

Telemetry is runtime-disabled unless `QUENT_EXPORTER` is set. Capture an actual
DuckDB query into a temporary directory:

```bash
events_dir=$(mktemp -d)
QUENT_EXPORTER=ndjson QUENT_OUTPUT_DIR="$events_dir" \
    build/reldebug/duckdb -c \
    "SET threads=4;
     SET scheduler_process_partial=true;
     SELECT sum(i * i) FROM range(1000000) t(i) WHERE i % 7 = 0;"
```

Then serve the completed event stream and open `http://127.0.0.1:8080`:

```bash
cd rust
cargo run -p duckdb-telemetry-server --features ui -- \
    --output-dir "$events_dir"
```

Opening the UI after DuckDB exits gives the most reliable stable snapshot;
collector streams may buffer smaller entity streams until shutdown. The
analyzer tolerates captures without Engine, Worker, or runtime-resource exit
events by closing those lifetimes only in its immutable snapshot. It also
caches an engine on first access, so restart the server if more events are
added afterward. Supported exporters are `none`, `ndjson`, `msgpack`,
`postcard`, and `collector`. The collector exporter reads its HTTP endpoint
from `QUENT_COLLECTOR_ADDRESS`; the server bind address uses
`QUENT_COLLECTOR_BIND_ADDRESS`.

Select the `SELECT sum(...)` query and open its **Timeline** tab, or navigate to
`/profile/engine/{engine_id}/query/{query_id}/timeline`. Expand the `local`
worker:

- `runnable-pipeline-tasks` with `pipeline_task` shows runnable backlog and
  dispatch delay (`Created` and `Ready`). It approximates scheduler occupancy:
  `Created` begins just before the initial enqueue and `Ready` just before a
  yielded task is handed back to the scheduler.
- `thread-*` with `pipeline_task` shows scheduled task execution (`Running`).
- `thread-*` with `operator_invocation` shows physical-operator calls
  (`Running`). Selecting plan operators filters these timelines.

The execution-thread lanes represent OS threads, not CPU cores. A task may use
different lanes after yielding. `Blocked` carries no resource usage because a
blocked task is neither runnable nor executing.

The query-plan view also shows a data-flow overlay derived from
`chunk_transfer` publications. It provides per-operator publication rates for
chunks, rows, and logical bytes, split by upstream operator. These are logical
flow rates; they do not represent physical copies or memory bandwidth.

The same timeline can be checked directly. Discover the engine, query group,
and query, then obtain resource IDs and the query duration from the bundle:

```bash
curl -s http://127.0.0.1:8080/api/engines
curl -s http://127.0.0.1:8080/api/engines/{engine_id}/query-groups
curl -s http://127.0.0.1:8080/api/engines/{engine_id}/query_group/{query_group_id}/queries
curl -s http://127.0.0.1:8080/api/engines/{engine_id}/query/{query_id}
```

Then request an execution-thread operator timeline, using the query duration as
`end`:

```bash
curl -s -X POST \
    -H 'content-type: application/json' \
    http://127.0.0.1:8080/api/engines/{engine_id}/timeline/single \
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

The query bundle advertises all runtime FSM declarations. Runtime instances
are available through `POST /api/engines/{engine_id}/entities` with
`filter.entity_type_name` set to `pipeline_task`, `chunk_transfer`, or
`operator_invocation`. The analyzer also exposes
`DuckDbUiAnalyzer::chunk_summary(query_id)` for totals by physical-plan edge.
