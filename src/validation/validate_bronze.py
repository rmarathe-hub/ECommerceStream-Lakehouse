#!/usr/bin/env python3
"""Validate bronze Parquet output from the Kafka streaming ingestion job."""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

import pyarrow.dataset as ds

VALID_EVENT_TYPES = {"view", "cart", "remove_from_cart", "purchase"}
EVENT_TIME_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC$")

REQUIRED_COLUMNS = [
    "event_id",
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
    "event_date",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate bronze event Parquet files.")
    parser.add_argument(
        "--bronze-path",
        type=Path,
        default=Path("data/bronze/events"),
        help="Path to bronze events Parquet directory.",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=1,
        help="Minimum number of bronze rows required (default: 1).",
    )
    return parser.parse_args()


def load_bronze_dataset(bronze_path: Path) -> ds.Dataset:
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze path not found: {bronze_path}")

    parquet_files = list(bronze_path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found under {bronze_path}")

    return ds.dataset(str(bronze_path), format="parquet", partitioning="hive")


def is_parseable_event_time(value: object) -> bool:
    if value is None:
        return False
    text = str(value)
    if not EVENT_TIME_PATTERN.match(text):
        return False
    try:
        datetime.strptime(text.replace(" UTC", ""), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return False
    return True


def validate_bronze(bronze_path: Path, min_rows: int) -> int:
    print(f"Validating bronze dataset at {bronze_path}")

    dataset = load_bronze_dataset(bronze_path)
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
    unparseable_event_time = sum(
        1 for value in data["event_time"] if not is_parseable_event_time(value)
    )
    null_event_date = sum(1 for value in data["event_date"] if value is None)

    empty_records = 0
    for idx in range(total_rows):
        if all(data[column][idx] is None for column in REQUIRED_COLUMNS):
            empty_records += 1

    event_type_counts: dict[str, int] = {}
    for value in data["event_type"]:
        key = str(value) if value is not None else "NULL"
        event_type_counts[key] = event_type_counts.get(key, 0) + 1

    print("event_type counts:")
    for event_type, count in sorted(event_type_counts.items()):
        print(f"  {event_type}: {count:,}")

    print(f"null event_id rows: {null_event_id:,}")
    print(f"invalid event_type rows: {invalid_event_type:,}")
    print(f"unparseable event_time rows: {unparseable_event_time:,}")
    print(f"null event_date rows: {null_event_date:,}")
    print(f"completely empty rows: {empty_records:,}")

    if null_event_id > 0:
        failures.append(f"found {null_event_id:,} rows with null/blank event_id")
    if invalid_event_type > 0:
        failures.append(f"found {invalid_event_type:,} rows with invalid event_type")
    if unparseable_event_time > 0:
        failures.append(f"found {unparseable_event_time:,} rows with unparseable event_time")
    if null_event_date > 0:
        failures.append(f"found {null_event_date:,} rows with null event_date")
    if empty_records > 0:
        failures.append(f"found {empty_records:,} completely empty rows")

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
        return validate_bronze(args.bronze_path, args.min_rows)
    except FileNotFoundError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
