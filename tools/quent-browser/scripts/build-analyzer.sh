#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_DIR="$(cd -- "${SCRIPT_DIR}/../../.." && pwd)"

exec "${REPO_DIR}/rust/scripts/build-telemetry-web.sh" "$@"
