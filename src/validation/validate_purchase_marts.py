#!/usr/bin/env python3
"""Validate fct_purchases and agg_product_performance gold marts."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pyarrow.dataset as ds

FCT_PURCHASES_REQUIRED_COLUMNS = [
    "purchase_id",
    "purchase_ts",
    "purchase_date",
    "session_id",
    "user_id",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "purchase_amount",
    "event_seq_in_session",
    "seconds_from_session_start",
    "bronze_ingested_at",
    "silver_processed_at",
    "gold_processed_at",
]

AGG_PRODUCT_REQUIRED_COLUMNS = [
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "view_count",
    "cart_count",
    "remove_from_cart_count",
    "purchase_count",
    "unique_viewers",
    "unique_cart_adders",
    "unique_purchasers",
    "total_revenue",
    "last_event_date",
    "view_to_purchase_rate",
    "cart_to_purchase_rate",
    "gold_processed_at",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate purchase and product gold marts.")
    parser.add_argument(
        "--session-events-path",
        type=Path,
        default=Path("data/silver/session_events"),
        help="Session events path for reconciliation.",
    )
    parser.add_argument(
        "--purchases-path",
        type=Path,
        default=Path("data/gold/fct_purchases"),
        help="Purchase fact table Parquet directory.",
    )
    parser.add_argument(
        "--product-performance-path",
        type=Path,
        default=Path("data/gold/agg_product_performance"),
        help="Product performance mart Parquet directory.",
    )
    parser.add_argument(
        "--min-purchases",
        type=int,
        default=1,
        help="Minimum purchase rows required (default: 1).",
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


def validate_fct_purchases(
    purchases_path: Path,
    session_events_path: Path,
    min_purchases: int,
) -> list[str]:
    failures: list[str] = []
    print(f"Validating fct_purchases at {purchases_path}")

    dataset = load_dataset(purchases_path)
    table = dataset.to_table()
    total_rows = table.num_rows
    print(f"fct_purchases rows: {total_rows:,}")

    if total_rows < min_purchases:
        failures.append(f"purchase count {total_rows:,} is below minimum {min_purchases:,}")

    failures.extend(validate_required_columns(set(table.column_names), FCT_PURCHASES_REQUIRED_COLUMNS))
    if failures:
        return failures

    data = table.to_pydict()

    null_purchase_id = sum(
        1 for value in data["purchase_id"] if value is None or str(value).strip() == ""
    )
    null_session_id = sum(1 for value in data["session_id"] if value is None or str(value).strip() == "")
    null_product_id = sum(1 for value in data["product_id"] if value is None)
    null_purchase_ts = sum(1 for value in data["purchase_ts"] if not is_parseable_timestamp(value))
    negative_amount = sum(
        1 for value in data["purchase_amount"] if value is not None and float(value) < 0
    )

    purchase_ids = [str(value) for value in data["purchase_id"] if value is not None]
    duplicate_purchase_ids = len(purchase_ids) - len(set(purchase_ids))

    print(f"null purchase_id rows: {null_purchase_id:,}")
    print(f"null session_id rows: {null_session_id:,}")
    print(f"null product_id rows: {null_product_id:,}")
    print(f"null purchase_ts rows: {null_purchase_ts:,}")
    print(f"negative purchase_amount rows: {negative_amount:,}")
    print(f"duplicate purchase_id rows: {duplicate_purchase_ids:,}")

    if null_purchase_id > 0:
        failures.append(f"found {null_purchase_id:,} rows with null/blank purchase_id")
    if null_session_id > 0:
        failures.append(f"found {null_session_id:,} rows with null/blank session_id")
    if null_product_id > 0:
        failures.append(f"found {null_product_id:,} rows with null product_id")
    if null_purchase_ts > 0:
        failures.append(f"found {null_purchase_ts:,} rows with null purchase_ts")
    if negative_amount > 0:
        failures.append(f"found {negative_amount:,} rows with negative purchase_amount")
    if duplicate_purchase_ids > 0:
        failures.append(f"found {duplicate_purchase_ids:,} duplicate purchase_id rows")

    session_events_dataset = load_dataset(session_events_path)
    purchase_events = session_events_dataset.to_table(
        columns=["event_id", "event_type"]
    )
    expected_purchases = sum(
        1
        for event_type in purchase_events["event_type"].to_pylist()
        if event_type == "purchase"
    )
    print(f"purchase events in session_events (for comparison): {expected_purchases:,}")
    if total_rows != expected_purchases:
        failures.append(
            f"fct_purchases row count {total_rows:,} does not match purchase event count "
            f"{expected_purchases:,} in session_events"
        )

    return failures


def validate_agg_product_performance(
    product_performance_path: Path,
    session_events_path: Path,
) -> list[str]:
    failures: list[str] = []
    print(f"Validating agg_product_performance at {product_performance_path}")

    dataset = load_dataset(product_performance_path)
    table = dataset.to_table()
    total_rows = table.num_rows
    print(f"agg_product_performance rows: {total_rows:,}")

    failures.extend(validate_required_columns(set(table.column_names), AGG_PRODUCT_REQUIRED_COLUMNS))
    if failures:
        return failures

    data = table.to_pydict()

    null_product_id = sum(1 for value in data["product_id"] if value is None)
    negative_revenue = sum(
        1 for value in data["total_revenue"] if value is not None and float(value) < 0
    )
    negative_view_count = sum(
        1 for value in data["view_count"] if value is not None and int(value) < 0
    )

    product_ids = [value for value in data["product_id"] if value is not None]
    duplicate_product_ids = len(product_ids) - len(set(product_ids))

    rate_out_of_range = 0
    revenue_mismatch = 0
    for idx in range(total_rows):
        view_rate = float(data["view_to_purchase_rate"][idx] or 0)
        cart_rate = float(data["cart_to_purchase_rate"][idx] or 0)
        if view_rate < 0 or view_rate > 1 or cart_rate < 0 or cart_rate > 1:
            rate_out_of_range += 1

        purchase_count = int(data["purchase_count"][idx] or 0)
        total_revenue = float(data["total_revenue"][idx] or 0)
        if purchase_count == 0 and total_revenue != 0:
            revenue_mismatch += 1

    print(f"null product_id rows: {null_product_id:,}")
    print(f"negative total_revenue rows: {negative_revenue:,}")
    print(f"negative view_count rows: {negative_view_count:,}")
    print(f"duplicate product_id rows: {duplicate_product_ids:,}")
    print(f"rate out of range rows: {rate_out_of_range:,}")
    print(f"zero purchases with revenue rows: {revenue_mismatch:,}")

    if null_product_id > 0:
        failures.append(f"found {null_product_id:,} rows with null product_id")
    if negative_revenue > 0:
        failures.append(f"found {negative_revenue:,} rows with negative total_revenue")
    if negative_view_count > 0:
        failures.append(f"found {negative_view_count:,} rows with negative view_count")
    if duplicate_product_ids > 0:
        failures.append(f"found {duplicate_product_ids:,} duplicate product_id rows")
    if rate_out_of_range > 0:
        failures.append(f"found {rate_out_of_range:,} rows with conversion rate outside [0, 1]")
    if revenue_mismatch > 0:
        failures.append(
            f"found {revenue_mismatch:,} rows with purchase_count=0 but total_revenue != 0"
        )

    session_events_dataset = load_dataset(session_events_path)
    distinct_products = len(
        {
            value
            for value in session_events_dataset.to_table(columns=["product_id"])["product_id"].to_pylist()
            if value is not None
        }
    )
    print(f"distinct products in session_events: {distinct_products:,}")
    if total_rows != distinct_products:
        failures.append(
            f"agg_product_performance row count {total_rows:,} does not match distinct "
            f"product_id count {distinct_products:,} in session_events"
        )

    purchases_table = load_dataset(product_performance_path).to_table(columns=["total_revenue"])
    mart_revenue = sum(float(v or 0) for v in purchases_table["total_revenue"].to_pylist())

    session_purchase_table = session_events_dataset.to_table(columns=["event_type", "price"])
    event_types = session_purchase_table["event_type"].to_pylist()
    prices = session_purchase_table["price"].to_pylist()
    source_revenue = sum(
        float(price or 0)
        for event_type, price in zip(event_types, prices, strict=True)
        if event_type == "purchase"
    )
    print(f"total revenue in agg_product_performance: {mart_revenue:,.2f}")
    print(f"total purchase revenue in session_events: {source_revenue:,.2f}")
    if abs(mart_revenue - source_revenue) > 0.01:
        failures.append(
            f"agg_product_performance total_revenue {mart_revenue:,.2f} does not match "
            f"session_events purchase revenue {source_revenue:,.2f}"
        )

    return failures


def main() -> int:
    args = parse_args()
    if args.min_purchases < 0:
        print("error: --min-purchases must be >= 0", file=sys.stderr)
        return 1

    try:
        failures = []
        failures.extend(
            validate_fct_purchases(args.purchases_path, args.session_events_path, args.min_purchases)
        )
        failures.extend(
            validate_agg_product_performance(args.product_performance_path, args.session_events_path)
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
