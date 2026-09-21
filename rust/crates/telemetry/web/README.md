# Browser analyzer

`duckdb-telemetry-web` accepts bounded postcard batches and publishes immutable
analyzer revisions only after a validated seal. It never inserts synthetic FSM
events. Open memory accounts end at the acknowledged watermark.

Build:

```bash
WASM_LD=/path/to/wasm-ld \
WASM_BINDGEN=/path/to/wasm-bindgen \
rust/scripts/build-telemetry-web.sh
```

Use `wasm-bindgen-cli` 0.2.127. Set `DUCKDB_TELEMETRY_BUILD_ID` for release
builds in both producer and analyzer.

The first version retains encoded session batches, rebuilds each sealed
snapshot, and keeps at most eight decoded snapshots. Defaults bound a batch to
4 MiB, a capture to 32 MiB, and the encoded session to 64 MiB. Snapshot
admission applies a conservative 10x encoded-size weight against a 512 MiB
aggregate budget before decoding. Reset when either session or snapshot budget
is exhausted. Actual decoded memory remains workload-dependent.

The facade returns analysis responses as raw JSON. Parse them with Quent's
lossless parser; JavaScript `JSON.parse` rounds nanosecond timestamps.

Convert a native capture into the browser wire format:

```bash
cargo run --manifest-path rust/Cargo.toml -p duckdb-telemetry-web \
  --example convert_capture --features native-fixture -- \
  CAPTURE_DIR CONTEXT_ID OUTPUT_DIR
```

The output contains `header.json`, `batch.postcard`, and `seal.json`. The
converter preserves the native event union and exact integer timestamps.
