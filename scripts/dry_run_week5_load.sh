#!/usr/bin/env bash
# Day 28: Dry-run Week 5 load plan — checks prerequisites, no COPY INTO, no dbt run.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PASS=0
WARN=0
FAIL=0

ok()   { echo "  [OK]   $*"; PASS=$((PASS + 1)); }
warn() { echo "  [WARN] $*"; WARN=$((WARN + 1)); }
bad()  { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); }

echo "=== Week 5 load dry-run (no data load) ==="
echo

# --- Local gold ---
echo "Local pipeline"
if [[ -d data/gold ]]; then
  ok "data/gold/ exists"
else
  bad "data/gold/ missing — run make verify-1m first"
fi

# --- .env ---
echo
echo "Environment (.env)"
if [[ -f .env ]]; then
  ok ".env present"
  set -a && # shellcheck disable=SC1091
  source ./.env && set +a
else
  bad ".env missing — cp .env.example .env"
fi

for var in SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_ROLE AWS_S3_BUCKET; do
  if [[ -n "${!var:-}" ]]; then
    ok "$var is set"
  else
    bad "$var is not set"
  fi
done

if [[ -n "${SNOWFLAKE_PASSWORD:-}" ]]; then
  ok "SNOWFLAKE_PASSWORD is set (value hidden)"
else
  warn "SNOWFLAKE_PASSWORD not set — SnowSQL/dbt will prompt or fail"
fi

if [[ -n "${SNOWFLAKE_S3_STORAGE_AWS_ROLE_ARN:-}" ]]; then
  ok "SNOWFLAKE_S3_STORAGE_AWS_ROLE_ARN is set"
else
  warn "SNOWFLAKE_S3_STORAGE_AWS_ROLE_ARN not set — run infra/snowflake/README.md IAM setup before stage"
fi

# --- SQL / dbt scaffold ---
echo
echo "Scaffold files"
for f in \
  sql/admin/run_guardrails_in_order.sql \
  sql/snowflake/run_stage_setup.sql \
  dbt/commercestream/dbt_project.yml \
  dbt/commercestream/models/staging/_sources.yml; do
  if [[ -f "$f" ]]; then
    ok "$f"
  else
    bad "missing $f"
  fi
done

# --- Planned Week 5 sequence ---
echo
echo "Planned Week 5 command sequence (not executed):"
echo "  1. make snowflake-check-guardrails"
echo "  2. make snowflake-stage-list         # expect ~225 gold files"
echo "  3. make upload-gold-s3              # if S3 gold stale"
echo "  4. make snowflake-load-gold         # COPY INTO + verify + suspend"
echo "  5. make dbt-build                   # .venv-dbt only + suspend"
echo "  6. make cloud-lite                  # or all-in-one: upload → load → dbt → suspend"
echo "  7. make snowflake-suspend           # mandatory after every session"
echo

echo "=== Summary: $PASS passed, $WARN warnings, $FAIL failed ==="
if [[ "$FAIL" -gt 0 ]]; then
  exit 1
fi
