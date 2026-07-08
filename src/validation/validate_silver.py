#!/usr/bin/env python3
"""Validate silver Parquet output from the bronze-to-silver transform."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pyarrow.dataset as ds

VALID_EVENT_TYPES = {"view", "cart", "remove_from_cart", "purchase"}

REQUIRED_COLUMNS = [
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
    "user_session",
    "bronze_ingested_at",
    "silver_processed_at",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate silver event Parquet files.")
    parser.add_argument(
        "--silver-path",
        type=Path,
        default=Path("data/silver/events"),
        help="Path to silver events Parquet directory.",
    )
    parser.add_argument(
        "--bronze-path",
        type=Path,
        default=None,
        help="Optional bronze path to compare row counts.",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=1,
        help="Minimum number of silver rows required (default: 1).",
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


def validate_silver(silver_path: Path, bronze_path: Path | None, min_rows: int) -> int:
    print(f"Validating silver dataset at {silver_path}")

    dataset = load_dataset(silver_path)
    table = dataset.to_table()
    total_rows = table.num_rows
    failures: list[str] = []

    print(f"rows: {total_rows:,}")

    if total_rows < min_rows:
        failures.append(f"row count {total_rows:,} is below minimum {min_rows:,}")

    column_names = set(table.column_names)
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in column_names]
    if missing_columns:
        failures.append(f"missing required columns: {', '.join(missing_columns)}")
        print("FAILED")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    data = table.to_pydict()

    null_event_id = sum(1 for value in data["event_id"] if value is None or str(value).strip() == "")
    invalid_event_type = sum(
        1 for value in data["event_type"] if value is None or str(value) not in VALID_EVENT_TYPES
    )
    null_event_ts = sum(1 for value in data["event_ts"] if not is_parseable_timestamp(value))
    null_event_date = sum(1 for value in data["event_date"] if value is None)
    null_product_id = sum(1 for value in data["product_id"] if value is None)
    null_user_id = sum(1 for value in data["user_id"] if value is None)
    null_user_session = sum(
        1 for value in data["user_session"] if value is None or str(value).strip() == ""
    )
    negative_price = sum(
        1 for value in data["price"] if value is not None and float(value) < 0
    )

    event_ids = [str(value) for value in data["event_id"] if value is not None]
    duplicate_event_ids = len(event_ids) - len(set(event_ids))

    event_type_counts: dict[str, int] = {}
    for value in data["event_type"]:
        key = str(value) if value is not None else "NULL"
        event_type_counts[key] = event_type_counts.get(key, 0) + 1

    print("event_type counts:")
    for event_type, count in sorted(event_type_counts.items()):
        print(f"  {event_type}: {count:,}")

    print(f"null event_id rows: {null_event_id:,}")
    print(f"invalid event_type rows: {invalid_event_type:,}")
    print(f"null event_ts rows: {null_event_ts:,}")
    print(f"null event_date rows: {null_event_date:,}")
    print(f"null product_id rows: {null_product_id:,}")
    print(f"null user_id rows: {null_user_id:,}")
    print(f"null user_session rows: {null_user_session:,}")
    print(f"negative price rows: {negative_price:,}")
    print(f"duplicate event_id rows: {duplicate_event_ids:,}")

    if null_event_id > 0:
        failures.append(f"found {null_event_id:,} rows with null/blank event_id")
    if invalid_event_type > 0:
        failures.append(f"found {invalid_event_type:,} rows with invalid event_type")
    if null_event_ts > 0:
        failures.append(f"found {null_event_ts:,} rows with null event_ts")
    if null_event_date > 0:
        failures.append(f"found {null_event_date:,} rows with null event_date")
    if null_product_id > 0:
        failures.append(f"found {null_product_id:,} rows with null product_id")
    if null_user_id > 0:
        failures.append(f"found {null_user_id:,} rows with null user_id")
    if null_user_session > 0:
        failures.append(f"found {null_user_session:,} rows with null user_session")
    if negative_price > 0:
        failures.append(f"found {negative_price:,} rows with negative price")
    if duplicate_event_ids > 0:
        failures.append(f"found {duplicate_event_ids:,} duplicate event_id rows")

    if bronze_path is not None:
        bronze_dataset = load_dataset(bronze_path)
        bronze_rows = bronze_dataset.to_table(columns=["event_id"]).num_rows
        print(f"bronze rows (for comparison): {bronze_rows:,}")
        if total_rows > bronze_rows:
            failures.append(
                f"silver row count {total_rows:,} exceeds bronze row count {bronze_rows:,}"
            )

    if failures:
        print("FAILED")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print("PASSED")
    return 0


def main() -> int:
    args = parse_args()
    if args.min_rows < 0:
        print("error: --min-rows must be >= 0", file=sys.stderr)
        return 1

    try:
        return validate_silver(args.silver_path, args.bronze_path, args.min_rows)
    except FileNotFoundError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
