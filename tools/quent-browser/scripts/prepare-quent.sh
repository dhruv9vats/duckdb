#!/usr/bin/env bash
set -euo pipefail

readonly QUENT_REVISION='4a091722f1a5b7a93e5883b82b838eea43f28c3a'
readonly QUENT_REPOSITORY='https://github.com/rapidsai/quent.git'
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly APP_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly CHECKOUT_DIR="${APP_DIR}/.quent"
readonly PATCH_FILE="${APP_DIR}/patches/quent-attribution.patch"
readonly SOURCE_REPOSITORY="${QUENT_SOURCE_REPOSITORY:-${QUENT_REPOSITORY}}"

if [[ -d "${CHECKOUT_DIR}/.git" ]]; then
  actual_revision="$(git -C "${CHECKOUT_DIR}" rev-parse HEAD)"
  if [[ "${actual_revision}" == "${QUENT_REVISION}" ]]; then
    if git -C "${CHECKOUT_DIR}" apply --reverse --check "${PATCH_FILE}" 2>/dev/null; then
      exit 0
    fi
    git -C "${CHECKOUT_DIR}" apply --check "${PATCH_FILE}"
    git -C "${CHECKOUT_DIR}" apply "${PATCH_FILE}"
    exit 0
  fi

  echo "Unexpected Quent revision: ${actual_revision}" >&2
  exit 1
fi

git clone --filter=blob:none --no-checkout "${SOURCE_REPOSITORY}" "${CHECKOUT_DIR}"
git -C "${CHECKOUT_DIR}" checkout --detach "${QUENT_REVISION}"

actual_revision="$(git -C "${CHECKOUT_DIR}" rev-parse HEAD)"
if [[ "${actual_revision}" != "${QUENT_REVISION}" ]]; then
  echo "Quent revision check failed" >&2
  exit 1
fi

git -C "${CHECKOUT_DIR}" apply --check "${PATCH_FILE}"
git -C "${CHECKOUT_DIR}" apply "${PATCH_FILE}"
