#!/bin/bash
set -euo pipefail

if [[ "${CI_XCODEBUILD_ACTION:-}" != archive ]]; then
  exit 0
fi
if [[ "${CI_BRANCH:-}" != main || -n "${CI_PULL_REQUEST_NUMBER:-}" ]]; then
  printf 'error: TestFlight archives must come from main.\n' >&2
  exit 1
fi

native_dir="${CI_PRIMARY_REPOSITORY_PATH:?}/ios"
run_dir=$(mktemp -d "${TMPDIR:-/tmp}/agent-release.XXXXXX")
trap 'rm -rf "$run_dir"' EXIT
mkdir "$run_dir/rendered"

# Xcode Cloud's required Test action owns iOS UI tests and their artifacts.
# Run the package contracts and production performance gates before signing.
check() {
  local phase=$1
  shift
  local log="$run_dir/$phase.log"
  if "$@" 2>&1 | tee "$log"; then
    return 0
  else
    local status=$?
    # Cloud otherwise reports only this script's exit code in its build check.
    if ! python3 - "$phase" "$log" "$status" <<'PY'
import re
import sys
from pathlib import Path

phase, path, status = sys.argv[1:]
text = Path(path).read_text(errors="replace")
text = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", text)
lines = [line.strip() for line in text.splitlines() if line.strip()]
issues = [line for line in lines if any(marker in line for marker in
          ("error:", "recorded an issue", "Expectation failed", "failed -"))]
for issue in (issues or lines[-1:] or ["No diagnostic output"])[:3]:
    print(f"error: {phase} failed (exit {status}): {issue[:1000]}")
PY
    then
      printf 'error: %s failed (exit %s); diagnostic extraction failed.\n' "$phase" "$status"
    fi
    return "$status"
  fi
}

check contracts swift test --package-path "$native_dir" --scratch-path "$run_dir/build" \
  --attachments-path "$run_dir/rendered"
check history-performance swift test --configuration release --package-path "$native_dir" \
  --scratch-path "$run_dir/build" --attachments-path "$run_dir/rendered" \
  --filter pagedHistoryAndStreamingPreparationRemainBoundedAndKeepCanonicalIdentity
