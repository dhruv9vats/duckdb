# DuckDB Quent telemetry harness

This workspace defines DuckDB's Quent query-engine model, generates its C++
instrumentation bridge, analyzes captured events, and serves them to the Quent
UI. It mirrors the corresponding workspace in Sirius and is wired into DuckDB
through the `BUILD_QUENT_TELEMETRY` CMake option.

The first model is intentionally limited to the standard query-engine entities:
engine, worker, query group, query, plan, operator, and port. The included sample
emits a connected physical plan so it can be rendered by the Quent UI.

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

## Capture a DuckDB query plan

Telemetry is runtime-disabled unless `QUENT_EXPORTER` is set. Capture an actual
DuckDB query into a temporary directory:

```bash
events_dir=$(mktemp -d)
QUENT_EXPORTER=ndjson QUENT_OUTPUT_DIR="$events_dir" \
    build/reldebug/duckdb -c \
    "SELECT sum(i) FROM range(1000) t(i) WHERE i % 2 = 0;"
```

Then serve the completed event stream and open `http://127.0.0.1:8080`:

```bash
cd rust
cargo run -p duckdb-telemetry-server --features ui -- \
    --output-dir "$events_dir"
```

Open the UI only after DuckDB exits. The analyzer caches an engine on first
access; restart the server if more events are added afterward. Supported
exporters are `none`, `ndjson`, `msgpack`, `postcard`, and `collector`. The
collector exporter reads its HTTP endpoint from `QUENT_COLLECTOR_ADDRESS`; the
server bind address uses `QUENT_COLLECTOR_BIND_ADDRESS`.
