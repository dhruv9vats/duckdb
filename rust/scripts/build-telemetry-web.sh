#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
target_dir=${CARGO_TARGET_DIR:-"$root/rust/target"}
output_dir=${1:-"$root/tools/quent-browser/public/wasm"}
wasm_bindgen=${WASM_BINDGEN:-wasm-bindgen}

if [[ -z "${DUCKDB_TELEMETRY_BUILD_ID:-}" ]]; then
    revision=$(git -C "$root" rev-parse HEAD)
    status=$(git -C "$root" status --porcelain --untracked-files=normal)
    dirty=
    if [[ -n "$status" ]]; then
        dirty=.dirty
    fi
    export DUCKDB_TELEMETRY_BUILD_ID="${revision}${dirty}+browser-v1"
fi

mkdir -p "$output_dir"
if [[ -n "${WASM_LD:-}" ]]; then
    export CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_LINKER="$WASM_LD"
fi
CARGO_TARGET_DIR="$target_dir" \
    cargo build \
        --locked \
        --release \
        --manifest-path "$root/rust/Cargo.toml" \
        --package duckdb-telemetry-web \
        --target wasm32-unknown-unknown
"$wasm_bindgen" \
    "$target_dir/wasm32-unknown-unknown/release/duckdb_telemetry_web.wasm" \
    --target web \
    --out-dir "$output_dir" \
    --out-name duckdb_telemetry_web
