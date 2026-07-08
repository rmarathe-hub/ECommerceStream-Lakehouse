#!/usr/bin/env python3
"""Validate sessionized silver events and fct_sessions gold output."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pyarrow.dataset as ds

SESSION_EVENTS_REQUIRED_COLUMNS = [
    "event_id",
    "event_ts",
    "event_date",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "session_id",
    "event_seq_in_session",
    "session_start_ts",
    "session_end_ts",
    "seconds_from_session_start",
    "bronze_ingested_at",
    "silver_processed_at",
    "sessionized_at",
]

FCT_SESSIONS_REQUIRED_COLUMNS = [
    "session_id",
    "user_id",
    "session_date",
    "session_start_ts",
    "session_end_ts",
    "session_duration_seconds",
    "event_count",
    "view_count",
    "cart_count",
    "remove_from_cart_count",
    "purchase_count",
    "distinct_products_viewed",
    "distinct_products_purchased",
    "session_revenue",
    "converted",
    "gold_processed_at",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate sessionization outputs.")
    parser.add_argument(
        "--silver-path",
        type=Path,
        default=Path("data/silver/events"),
        help="Silver events path for row-count reconciliation.",
    )
    parser.add_argument(
        "--session-events-path",
        type=Path,
        default=Path("data/silver/session_events"),
        help="Sessionized events Parquet directory.",
    )
    parser.add_argument(
        "--sessions-path",
        type=Path,
        default=Path("data/gold/fct_sessions"),
        help="Session fact table Parquet directory.",
    )
    parser.add_argument(
        "--min-sessions",
        type=int,
        default=1,
        help="Minimum number of sessions required (default: 1).",
    )
    return parser.parse_args()


def load_dataset(path: Path) -> ds.Dataset:
    if not path.exists():
        raise FileNotFoundError(f"Path not found: {path}")

    parquet_files = list(path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found under {path}")

    return ds.dataset(str(path), format="parquet", partitioning="hive")


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


def validate_required_columns(column_names: set[str], required: list[str]) -> list[str]:
    missing = [col for col in required if col not in column_names]
    if missing:
        return [f"missing required columns: {', '.join(missing)}"]
    return []


def validate_session_events(
    session_events_path: Path,
    silver_path: Path,
) -> list[str]:
    failures: list[str] = []
    print(f"Validating session events at {session_events_path}")

    dataset = load_dataset(session_events_path)
    table = dataset.to_table()
    total_rows = table.num_rows
    print(f"session_events rows: {total_rows:,}")

    failures.extend(validate_required_columns(set(table.column_names), SESSION_EVENTS_REQUIRED_COLUMNS))
    if failures:
        return failures

    data = table.to_pydict()

    null_event_id = sum(1 for value in data["event_id"] if value is None or str(value).strip() == "")
    null_session_id = sum(1 for value in data["session_id"] if value is None or str(value).strip() == "")
    invalid_seq = sum(
        1 for value in data["event_seq_in_session"] if value is None or int(value) < 1
    )
    negative_seconds = sum(
        1 for value in data["seconds_from_session_start"] if value is not None and int(value) < 0
    )
    null_session_start = sum(
        1 for value in data["session_start_ts"] if not is_parseable_timestamp(value)
    )

    print(f"null event_id rows: {null_event_id:,}")
    print(f"null session_id rows: {null_session_id:,}")
    print(f"invalid event_seq_in_session rows: {invalid_seq:,}")
    print(f"negative seconds_from_session_start rows: {negative_seconds:,}")
    print(f"null session_start_ts rows: {null_session_start:,}")

    if null_event_id > 0:
        failures.append(f"found {null_event_id:,} rows with null/blank event_id")
    if null_session_id > 0:
        failures.append(f"found {null_session_id:,} rows with null/blank session_id")
    if invalid_seq > 0:
        failures.append(f"found {invalid_seq:,} rows with invalid event_seq_in_session")
    if negative_seconds > 0:
        failures.append(f"found {negative_seconds:,} rows with negative seconds_from_session_start")
    if null_session_start > 0:
        failures.append(f"found {null_session_start:,} rows with null session_start_ts")

    silver_dataset = load_dataset(silver_path)
    silver_rows = silver_dataset.to_table(columns=["event_id"]).num_rows
    print(f"silver rows (for comparison): {silver_rows:,}")
    if total_rows != silver_rows:
        failures.append(
            f"session_events row count {total_rows:,} does not match silver row count {silver_rows:,}"
        )

    return failures


def validate_fct_sessions(
    sessions_path: Path,
    session_events_path: Path,
    min_sessions: int,
) -> list[str]:
    failures: list[str] = []
    print(f"Validating fct_sessions at {sessions_path}")

    dataset = load_dataset(sessions_path)
    table = dataset.to_table()
    total_rows = table.num_rows
    print(f"fct_sessions rows: {total_rows:,}")

    if total_rows < min_sessions:
        failures.append(f"session count {total_rows:,} is below minimum {min_sessions:,}")

    failures.extend(validate_required_columns(set(table.column_names), FCT_SESSIONS_REQUIRED_COLUMNS))
    if failures:
        return failures

    data = table.to_pydict()

    null_session_id = sum(1 for value in data["session_id"] if value is None or str(value).strip() == "")
    zero_event_count = sum(1 for value in data["event_count"] if value is None or int(value) < 1)
    negative_duration = sum(
        1 for value in data["session_duration_seconds"] if value is not None and int(value) < 0
    )
    negative_revenue = sum(
        1 for value in data["session_revenue"] if value is not None and float(value) < 0
    )

    session_ids = [str(value) for value in data["session_id"] if value is not None]
    duplicate_session_ids = len(session_ids) - len(set(session_ids))

    event_count_mismatch = 0
    for idx in range(total_rows):
        counts = [
            int(data["view_count"][idx] or 0),
            int(data["cart_count"][idx] or 0),
            int(data["remove_from_cart_count"][idx] or 0),
            int(data["purchase_count"][idx] or 0),
        ]
        if int(data["event_count"][idx] or 0) != sum(counts):
            event_count_mismatch += 1

    converted_mismatch = sum(
        1
        for idx in range(total_rows)
        if bool(data["converted"][idx]) != (int(data["purchase_count"][idx] or 0) > 0)
    )

    print(f"null session_id rows: {null_session_id:,}")
    print(f"zero event_count rows: {zero_event_count:,}")
    print(f"negative session_duration_seconds rows: {negative_duration:,}")
    print(f"negative session_revenue rows: {negative_revenue:,}")
    print(f"duplicate session_id rows: {duplicate_session_ids:,}")
    print(f"event_count mismatch rows: {event_count_mismatch:,}")
    print(f"converted flag mismatch rows: {converted_mismatch:,}")

    if null_session_id > 0:
        failures.append(f"found {null_session_id:,} rows with null/blank session_id")
    if zero_event_count > 0:
        failures.append(f"found {zero_event_count:,} rows with event_count < 1")
    if negative_duration > 0:
        failures.append(f"found {negative_duration:,} rows with negative session_duration_seconds")
    if negative_revenue > 0:
        failures.append(f"found {negative_revenue:,} rows with negative session_revenue")
    if duplicate_session_ids > 0:
        failures.append(f"found {duplicate_session_ids:,} duplicate session_id rows")
    if event_count_mismatch > 0:
        failures.append(f"found {event_count_mismatch:,} rows where event_count != sum of event types")
    if converted_mismatch > 0:
        failures.append(f"found {converted_mismatch:,} rows with inconsistent converted flag")

    session_events_table = load_dataset(session_events_path).to_table(columns=["session_id"])
    distinct_sessions = len(
        {
            str(value)
            for value in session_events_table["session_id"].to_pylist()
            if value is not None and str(value).strip() != ""
        }
    )
    print(f"distinct sessions in session_events: {distinct_sessions:,}")
    if total_rows != distinct_sessions:
        failures.append(
            f"fct_sessions row count {total_rows:,} does not match distinct session_id count "
            f"{distinct_sessions:,} in session_events"
        )

    return failures


def main() -> int:
    args = parse_args()
    if args.min_sessions < 0:
        print("error: --min-sessions must be >= 0", file=sys.stderr)
        return 1

    try:
        failures = []
        failures.extend(validate_session_events(args.session_events_path, args.silver_path))
        failures.extend(
            validate_fct_sessions(args.sessions_path, args.session_events_path, args.min_sessions)
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
