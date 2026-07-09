#!/usr/bin/env bash
# Local-only quality gate for Weeks 1–2 (no Kafka replay by default, no cloud).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON="${PYTHON:-.venv/bin/python3}"
SKIP_DOCKER=false
SKIP_PIPELINE=false

usage() {
  cat <<'EOF'
Usage: scripts/run_local_quality_gate.sh [options]

Runs production-quality local checks for the Weeks 1–2 pipeline.
Does NOT replay Kafka, reset demo state, or touch cloud resources.

Options:
  --skip-docker     Skip Docker stack health polling
  --skip-pipeline   Skip make verify-1m (use existing dq_pipeline_summary.json)
  -h, --help        Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-docker) SKIP_DOCKER=true; shift ;;
    --skip-pipeline) SKIP_PIPELINE=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

echo "=== Step 1/4: Python syntax (compileall) ==="
if [[ -x "$PYTHON" ]]; then
  "$PYTHON" -m compileall -q src scripts
  echo "compileall: PASSED"
else
  echo "WARN: $PYTHON not found; using system python3"
  python3 -m compileall -q src scripts
  echo "compileall: PASSED"
fi

echo ""
echo "=== Step 2/4: Docker stack health (wait_for_stack.sh) ==="
if $SKIP_DOCKER; then
  echo "skipped (--skip-docker)"
else
  if ! command -v docker >/dev/null 2>&1; then
    echo "WARN: docker not available; skipping stack check"
  else
    chmod +x scripts/wait_for_stack.sh
    STACK_WAIT_TIMEOUT="${STACK_WAIT_TIMEOUT:-120}" \
      STACK_WAIT_INTERVAL="${STACK_WAIT_INTERVAL:-3}" \
      ./scripts/wait_for_stack.sh
  fi
fi

echo ""
echo "=== Step 3/4: Git data hygiene ==="
TRACKED_DATA="$(git ls-files '*.csv' '*.parquet' 'data/bronze/*' 'data/silver/*' 'data/gold/*' 2>/dev/null \
  | grep -v '\.gitkeep' || true)"
if [[ -n "$TRACKED_DATA" ]]; then
  echo "FAIL: large data files tracked in git:"
  echo "$TRACKED_DATA"
  exit 1
fi
echo "no large data files tracked (only .gitkeep allowed)"

echo ""
echo "=== Step 4/4: Python quality gate + pipeline validation ==="
if $SKIP_PIPELINE; then
  "$PYTHON" src/validation/run_quality_gate.py --skip-pipeline
else
  "$PYTHON" src/validation/run_quality_gate.py
fi
