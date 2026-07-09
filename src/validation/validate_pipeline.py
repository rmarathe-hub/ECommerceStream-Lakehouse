#!/usr/bin/env python3
"""Run end-to-end data quality checks across bronze, silver, and gold layers."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.dataset as ds

# Allow sibling imports when executed as a script.
_VALIDATION_DIR = Path(__file__).resolve().parent
if str(_VALIDATION_DIR) not in sys.path:
    sys.path.insert(0, str(_VALIDATION_DIR))

from validate_bronze import validate_bronze
from validate_funnel_marts import validate_agg_conversion_funnel, validate_fct_cart_abandonment
from validate_purchase_marts import validate_fct_purchases, validate_agg_product_performance
from validate_sessions import validate_session_events, validate_fct_sessions
from validate_silver import validate_silver


@dataclass
class CheckResult:
    name: str
    status: str
    details: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate the full local lakehouse pipeline.")
    parser.add_argument("--bronze-path", type=Path, default=Path("data/bronze/events"))
    parser.add_argument("--silver-path", type=Path, default=Path("data/silver/events"))
    parser.add_argument(
        "--session-events-path",
        type=Path,
        default=Path("data/silver/session_events"),
    )
    parser.add_argument("--sessions-path", type=Path, default=Path("data/gold/fct_sessions"))
    parser.add_argument("--purchases-path", type=Path, default=Path("data/gold/fct_purchases"))
    parser.add_argument(
        "--product-performance-path",
        type=Path,
        default=Path("data/gold/agg_product_performance"),
    )
    parser.add_argument("--funnel-path", type=Path, default=Path("data/gold/agg_conversion_funnel"))
    parser.add_argument(
        "--cart-abandonment-path",
        type=Path,
        default=Path("data/gold/fct_cart_abandonment"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/gold/dq_pipeline_summary.json"),
        help="Write JSON summary report to this path.",
    )
    parser.add_argument(
        "--skip-bronze",
        action="store_true",
        help="Skip bronze layer validation.",
    )
    parser.add_argument(
        "--gold-only",
        action="store_true",
        help="Validate gold marts and cross-layer checks only.",
    )
    parser.add_argument(
        "--min-bronze-rows",
        type=int,
        default=1,
        help="Minimum bronze rows required (default: 1).",
    )
    parser.add_argument(
        "--min-silver-rows",
        type=int,
        default=1,
        help="Minimum silver rows required (default: 1).",
    )
    parser.add_argument(
        "--min-sessions",
        type=int,
        default=1,
        help="Minimum fct_sessions rows required (default: 1).",
    )
    return parser.parse_args()


def load_dataset(path: Path) -> ds.Dataset:
    if not path.exists():
        raise FileNotFoundError(f"Path not found: {path}")
    parquet_files = list(path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found under {path}")
    return ds.dataset(str(path), format="parquet", partitioning="hive")


def row_count(path: Path) -> int:
    return load_dataset(path).to_table().num_rows


def sum_column(path: Path, column: str) -> float:
    table = load_dataset(path).to_table(columns=[column])
    return float(sum(float(value or 0) for value in table[column].to_pylist()))


def run_layer_validations(args: argparse.Namespace) -> list[CheckResult]:
    results: list[CheckResult] = []

    if not args.gold_only and not args.skip_bronze:
        code = validate_bronze(args.bronze_path, min_rows=args.min_bronze_rows)
        results.append(
            CheckResult(
                name="bronze",
                status="PASS" if code == 0 else "FAIL",
                details=[f"exit_code={code}"],
            )
        )

    if not args.gold_only:
        code = validate_silver(args.silver_path, args.bronze_path, min_rows=args.min_silver_rows)
        results.append(
            CheckResult(
                name="silver",
                status="PASS" if code == 0 else "FAIL",
                details=[f"exit_code={code}"],
            )
        )

        failures = validate_session_events(args.session_events_path, args.silver_path)
        results.append(
            CheckResult(
                name="session_events",
                status="PASS" if not failures else "FAIL",
                details=failures or ["ok"],
            )
        )

        failures = validate_fct_sessions(
            args.sessions_path, args.session_events_path, min_sessions=args.min_sessions
        )
        results.append(
            CheckResult(
                name="fct_sessions",
                status="PASS" if not failures else "FAIL",
                details=failures or ["ok"],
            )
        )

    failures = validate_fct_purchases(
        args.purchases_path, args.session_events_path, min_purchases=1
    )
    results.append(
        CheckResult(
            name="fct_purchases",
            status="PASS" if not failures else "FAIL",
            details=failures or ["ok"],
        )
    )

    failures = validate_agg_product_performance(
        args.product_performance_path, args.session_events_path
    )
    results.append(
        CheckResult(
            name="agg_product_performance",
            status="PASS" if not failures else "FAIL",
            details=failures or ["ok"],
        )
    )

    failures = validate_agg_conversion_funnel(args.funnel_path, args.sessions_path)
    results.append(
        CheckResult(
            name="agg_conversion_funnel",
            status="PASS" if not failures else "FAIL",
            details=failures or ["ok"],
        )
    )

    failures = validate_fct_cart_abandonment(
        args.cart_abandonment_path, args.sessions_path, min_abandonments=0
    )
    results.append(
        CheckResult(
            name="fct_cart_abandonment",
            status="PASS" if not failures else "FAIL",
            details=failures or ["ok"],
        )
    )

    return results


def run_cross_layer_checks(args: argparse.Namespace) -> list[CheckResult]:
    results: list[CheckResult] = []
    failures: list[str] = []

    bronze_rows = None
    if not args.skip_bronze and not args.gold_only and args.bronze_path.exists():
        bronze_rows = row_count(args.bronze_path)
    silver_rows = row_count(args.silver_path)
    session_event_rows = row_count(args.session_events_path)
    session_rows = row_count(args.sessions_path)
    purchase_rows = row_count(args.purchases_path)

    print("Cross-layer reconciliation:")
    print(f"  bronze rows: {bronze_rows:,}" if bronze_rows is not None else "  bronze rows: (skipped)")
    print(f"  silver rows: {silver_rows:,}")
    print(f"  session_events rows: {session_event_rows:,}")
    print(f"  fct_sessions rows: {session_rows:,}")
    print(f"  fct_purchases rows: {purchase_rows:,}")

    if bronze_rows is not None and silver_rows > bronze_rows:
        failures.append(f"silver rows {silver_rows:,} exceed bronze rows {bronze_rows:,}")
    if session_event_rows != silver_rows:
        failures.append(
            f"session_events rows {session_event_rows:,} != silver rows {silver_rows:,}"
        )

    session_events_table = load_dataset(args.session_events_path).to_table(
        columns=["session_id", "event_type"]
    )
    distinct_sessions = len(
        {
            str(value)
            for value in session_events_table["session_id"].to_pylist()
            if value is not None
        }
    )
    if distinct_sessions != session_rows:
        failures.append(
            f"distinct session_id in session_events {distinct_sessions:,} != "
            f"fct_sessions rows {session_rows:,}"
        )

    purchase_events = sum(
        1 for value in session_events_table["event_type"].to_pylist() if value == "purchase"
    )
    if purchase_rows != purchase_events:
        failures.append(
            f"fct_purchases rows {purchase_rows:,} != purchase events {purchase_events:,}"
        )

    sessions_revenue = sum_column(args.sessions_path, "session_revenue")
    purchases_revenue = sum_column(args.purchases_path, "purchase_amount")
    product_revenue = sum_column(args.product_performance_path, "total_revenue")

    print(f"  fct_sessions revenue: {sessions_revenue:,.2f}")
    print(f"  fct_purchases revenue: {purchases_revenue:,.2f}")
    print(f"  agg_product_performance revenue: {product_revenue:,.2f}")

    if abs(sessions_revenue - purchases_revenue) > 0.01:
        failures.append(
            f"fct_sessions revenue {sessions_revenue:,.2f} != "
            f"fct_purchases revenue {purchases_revenue:,.2f}"
        )
    if abs(purchases_revenue - product_revenue) > 0.01:
        failures.append(
            f"fct_purchases revenue {purchases_revenue:,.2f} != "
            f"agg_product_performance revenue {product_revenue:,.2f}"
        )

    funnel_table = load_dataset(args.funnel_path).to_table(columns=["total_sessions"])
    funnel_session_total = sum(int(value or 0) for value in funnel_table["total_sessions"].to_pylist())
    if funnel_session_total != session_rows:
        failures.append(
            f"funnel total_sessions sum {funnel_session_total:,} != "
            f"fct_sessions rows {session_rows:,}"
        )

    results.append(
        CheckResult(
            name="cross_layer_reconciliation",
            status="PASS" if not failures else "FAIL",
            details=failures or ["ok"],
        )
    )
    return results


def write_summary(path: Path, results: list[CheckResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall_status": "PASS" if all(r.status == "PASS" for r in results) else "FAIL",
        "checks": [asdict(result) for result in results],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"DQ summary written to {path}")


def print_report(results: list[CheckResult]) -> int:
    print("")
    print("=== Pipeline Data Quality Report ===")
    for result in results:
        print(f"[{result.status}] {result.name}")
        for detail in result.details:
            print(f"  - {detail}")

    overall = all(result.status == "PASS" for result in results)
    print("")
    print("OVERALL:", "PASSED" if overall else "FAILED")
    return 0 if overall else 1


def main() -> int:
    args = parse_args()

    try:
        print("Running layer validations...")
        layer_results = run_layer_validations(args)

        print("")
        cross_results = run_cross_layer_checks(args)

        all_results = layer_results + cross_results
        write_summary(args.output, all_results)
        return print_report(all_results)
    except FileNotFoundError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
