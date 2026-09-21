#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly APP_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly SOURCE_DIR="${APP_DIR}/producer"
readonly BUILD_DIR="${DUCKDB_BROWSER_BUILD_DIR:-${APP_DIR}/.build/producer}"
readonly OUTPUT_DIR="${1:-${APP_DIR}/public/wasm}"

command -v emcmake >/dev/null || {
  echo "emcmake is required" >&2
  exit 1
}

cmake_args=(
  -S "${SOURCE_DIR}"
  -B "${BUILD_DIR}"
  -DCMAKE_BUILD_TYPE=Release
  -DCMAKE_C_COMPILER_LAUNCHER=
  -DCMAKE_CXX_COMPILER_LAUNCHER=
)
if [[ -n "${CORROSION_SOURCE_DIR:-}" ]]; then
  cmake_args+=( -DFETCHCONTENT_SOURCE_DIR_CORROSION="${CORROSION_SOURCE_DIR}" )
fi
if [[ -n "${PYTHON_EXECUTABLE:-}" ]]; then
  cmake_args+=( -DPython3_EXECUTABLE="${PYTHON_EXECUTABLE}" )
fi
if [[ -n "${RUST_COMPILER:-}" ]]; then
  cmake_args+=( -DRust_COMPILER="${RUST_COMPILER}" )
fi
if [[ -n "${CMAKE_GENERATOR:-}" ]]; then
  cmake_args+=( -G "${CMAKE_GENERATOR}" )
fi

emcmake cmake "${cmake_args[@]}"
cmake --build "${BUILD_DIR}" --target duckdb-browser --parallel "${BUILD_JOBS:-2}"

mkdir -p "${OUTPUT_DIR}"
cp "${BUILD_DIR}/artifacts/duckdb-browser.js" "${OUTPUT_DIR}/duckdb-browser.js"
cp "${BUILD_DIR}/artifacts/duckdb-browser.wasm" "${OUTPUT_DIR}/duckdb-browser.wasm"

node "${SCRIPT_DIR}/test-producer.mjs" "${OUTPUT_DIR}/duckdb-browser.js"
