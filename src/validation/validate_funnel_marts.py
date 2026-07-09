#!/usr/bin/env python3
"""Validate agg_conversion_funnel and fct_cart_abandonment gold marts."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pyarrow.dataset as ds

FUNNEL_REQUIRED_COLUMNS = [
    "session_date",
    "total_sessions",
    "sessions_with_view",
    "sessions_with_cart",
    "sessions_with_purchase",
    "view_to_cart_sessions",
    "cart_to_purchase_sessions",
    "view_to_purchase_sessions",
    "abandoned_cart_sessions",
    "view_to_cart_rate",
    "cart_to_purchase_rate",
    "view_to_purchase_rate",
    "cart_abandonment_rate",
    "gold_processed_at",
]

ABANDONMENT_REQUIRED_COLUMNS = [
    "session_id",
    "user_id",
    "session_date",
    "session_start_ts",
    "session_end_ts",
    "session_duration_seconds",
    "view_count",
    "cart_count",
    "remove_from_cart_count",
    "cart_event_count",
    "distinct_products_carted",
    "first_cart_ts",
    "last_cart_ts",
    "abandoned",
    "gold_processed_at",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate funnel and cart abandonment gold marts.")
    parser.add_argument(
        "--sessions-path",
        type=Path,
        default=Path("data/gold/fct_sessions"),
        help="Session fact table for reconciliation.",
    )
    parser.add_argument(
        "--funnel-path",
        type=Path,
        default=Path("data/gold/agg_conversion_funnel"),
        help="Conversion funnel mart Parquet directory.",
    )
    parser.add_argument(
        "--cart-abandonment-path",
        type=Path,
        default=Path("data/gold/fct_cart_abandonment"),
        help="Cart abandonment fact table Parquet directory.",
    )
    parser.add_argument(
        "--min-abandonments",
        type=int,
        default=0,
        help="Minimum abandoned cart sessions required (default: 0).",
    )
    return parser.parse_args()


def load_dataset(path: Path) -> ds.Dataset:
    if not path.exists():
        raise FileNotFoundError(f"Path not found: {path}")

    parquet_files = list(path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found under {path}")

    return ds.dataset(str(path), format="parquet", partitioning="hive")


def validate_required_columns(column_names: set[str], required: list[str]) -> list[str]:
    missing = [col for col in required if col not in column_names]
    if missing:
        return [f"missing required columns: {', '.join(missing)}"]
    return []


def validate_agg_conversion_funnel(funnel_path: Path, sessions_path: Path) -> list[str]:
    failures: list[str] = []
    print(f"Validating agg_conversion_funnel at {funnel_path}")

    dataset = load_dataset(funnel_path)
    table = dataset.to_table()
    total_rows = table.num_rows
    print(f"agg_conversion_funnel rows: {total_rows:,}")

    failures.extend(validate_required_columns(set(table.column_names), FUNNEL_REQUIRED_COLUMNS))
    if failures:
        return failures

    data = table.to_pydict()

    rate_out_of_range = 0
    count_logic_errors = 0
    funnel_total_sessions = 0

    for idx in range(total_rows):
        rates = [
            float(data["view_to_cart_rate"][idx] or 0),
            float(data["cart_to_purchase_rate"][idx] or 0),
            float(data["view_to_purchase_rate"][idx] or 0),
            float(data["cart_abandonment_rate"][idx] or 0),
        ]
        if any(rate < 0 or rate > 1 for rate in rates):
            rate_out_of_range += 1

        total_sessions = int(data["total_sessions"][idx] or 0)
        sessions_with_view = int(data["sessions_with_view"][idx] or 0)
        sessions_with_cart = int(data["sessions_with_cart"][idx] or 0)
        sessions_with_purchase = int(data["sessions_with_purchase"][idx] or 0)
        abandoned_cart_sessions = int(data["abandoned_cart_sessions"][idx] or 0)

        funnel_total_sessions += total_sessions

        if sessions_with_view > total_sessions or sessions_with_cart > total_sessions:
            count_logic_errors += 1
        if sessions_with_purchase > sessions_with_cart and sessions_with_cart > 0:
            # purchases can exceed carts in edge cases, not an error
            pass
        if abandoned_cart_sessions > sessions_with_cart:
            count_logic_errors += 1

    print(f"rate out of range rows: {rate_out_of_range:,}")
    print(f"count logic error rows: {count_logic_errors:,}")
    print(f"sum total_sessions across funnel dates: {funnel_total_sessions:,}")

    if rate_out_of_range > 0:
        failures.append(f"found {rate_out_of_range:,} rows with rate outside [0, 1]")
    if count_logic_errors > 0:
        failures.append(f"found {count_logic_errors:,} rows with inconsistent session counts")

    sessions_dataset = load_dataset(sessions_path)
    expected_sessions = sessions_dataset.to_table(columns=["session_id"]).num_rows
    print(f"fct_sessions rows (for comparison): {expected_sessions:,}")
    if funnel_total_sessions != expected_sessions:
        failures.append(
            f"sum of funnel total_sessions {funnel_total_sessions:,} does not match "
            f"fct_sessions row count {expected_sessions:,}"
        )

    return failures


def is_parseable_timestamp(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, datetime):
        return True
    text = str(value)
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def validate_fct_cart_abandonment(
    cart_abandonment_path: Path,
    sessions_path: Path,
    min_abandonments: int,
) -> list[str]:
    failures: list[str] = []
    print(f"Validating fct_cart_abandonment at {cart_abandonment_path}")

    dataset = load_dataset(cart_abandonment_path)
    table = dataset.to_table()
    total_rows = table.num_rows
    print(f"fct_cart_abandonment rows: {total_rows:,}")

    if total_rows < min_abandonments:
        failures.append(
            f"abandoned cart session count {total_rows:,} is below minimum {min_abandonments:,}"
        )

    failures.extend(validate_required_columns(set(table.column_names), ABANDONMENT_REQUIRED_COLUMNS))
    if failures:
        return failures

    data = table.to_pydict()

    null_session_id = sum(1 for value in data["session_id"] if value is None or str(value).strip() == "")
    not_abandoned = sum(1 for value in data["abandoned"] if value is not True)
    zero_cart_count = sum(1 for value in data["cart_count"] if value is None or int(value) < 1)
    null_first_cart_ts = sum(1 for value in data["first_cart_ts"] if not is_parseable_timestamp(value))

    session_ids = [str(value) for value in data["session_id"] if value is not None]
    duplicate_session_ids = len(session_ids) - len(set(session_ids))

    print(f"null session_id rows: {null_session_id:,}")
    print(f"abandoned flag false rows: {not_abandoned:,}")
    print(f"zero cart_count rows: {zero_cart_count:,}")
    print(f"null first_cart_ts rows: {null_first_cart_ts:,}")
    print(f"duplicate session_id rows: {duplicate_session_ids:,}")

    if null_session_id > 0:
        failures.append(f"found {null_session_id:,} rows with null/blank session_id")
    if not_abandoned > 0:
        failures.append(f"found {not_abandoned:,} rows where abandoned is not true")
    if zero_cart_count > 0:
        failures.append(f"found {zero_cart_count:,} rows with cart_count < 1")
    if null_first_cart_ts > 0:
        failures.append(f"found {null_first_cart_ts:,} rows with null first_cart_ts")
    if duplicate_session_ids > 0:
        failures.append(f"found {duplicate_session_ids:,} duplicate session_id rows")

    sessions_table = load_dataset(sessions_path).to_table(
        columns=["session_id", "cart_count", "converted"]
    )
    expected_abandonments = sum(
        1
        for cart_count, converted in zip(
            sessions_table["cart_count"].to_pylist(),
            sessions_table["converted"].to_pylist(),
            strict=True,
        )
        if int(cart_count or 0) > 0 and converted is not True
    )
    print(f"expected abandoned cart sessions in fct_sessions: {expected_abandonments:,}")
    if total_rows != expected_abandonments:
        failures.append(
            f"fct_cart_abandonment row count {total_rows:,} does not match expected "
            f"abandoned cart sessions {expected_abandonments:,} in fct_sessions"
        )

    return failures


def main() -> int:
    args = parse_args()
    if args.min_abandonments < 0:
        print("error: --min-abandonments must be >= 0", file=sys.stderr)
        return 1

    try:
        failures = []
        failures.extend(validate_agg_conversion_funnel(args.funnel_path, args.sessions_path))
        failures.extend(
            validate_fct_cart_abandonment(
                args.cart_abandonment_path,
                args.sessions_path,
                args.min_abandonments,
            )
        )

        if failures:
            print("FAILED")
            for failure in failures:
                print(f"  - {failure}")
            return 1

        print("PASSED")
        return 0
    except FileNotFoundError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
