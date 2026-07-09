#!/usr/bin/env python3
"""Local-only quality gate for Weeks 1–2 pipeline (no Kafka replay, no cloud)."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pyarrow.dataset as ds

VALID_EVENT_TYPES = {"view", "cart", "remove_from_cart", "purchase"}
SAMPLE_REQUIRED_COLUMNS = [
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
]
BRONZE_KAFKA_COLUMNS = [
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
]
GOLD_TABLES = [
    "fct_sessions",
    "fct_purchases",
    "agg_product_performance",
    "agg_conversion_funnel",
    "fct_cart_abandonment",
]
MILESTONE = {
    "bronze_rows": 1_000_000,
    "silver_rows": 1_000_000,
    "fct_sessions": 874_457,
    "fct_purchases": 17_405,
    "agg_product_performance": 83_600,
    "fct_cart_abandonment": 20_858,
    "total_revenue": 5_377_910.49,
}
MILESTONE_TOLERANCE = {
    "bronze_rows": 0,
    "silver_rows": 0,
    "fct_sessions": 5_000,
    "fct_purchases": 500,
    "agg_product_performance": 2_000,
    "fct_cart_abandonment": 1_000,
    "total_revenue": 1.0,
}


@dataclass
class SectionResult:
    name: str
    passed: bool
    details: list[str] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run local quality gate checks (no Kafka replay, no cloud)."
    )
    parser.add_argument(
        "--skip-pipeline",
        action="store_true",
        help="Skip make verify-1m (use existing dq_pipeline_summary.json).",
    )
    parser.add_argument(
        "--sample-1m",
        type=Path,
        default=Path("data/raw/events_1m.csv"),
    )
    parser.add_argument(
        "--sample-5m",
        type=Path,
        default=Path("data/raw/events_5m.csv"),
    )
    parser.add_argument(
        "--bronze-path",
        type=Path,
        default=Path("data/bronze/events"),
    )
    parser.add_argument(
        "--silver-path",
        type=Path,
        default=Path("data/silver/events"),
    )
    parser.add_argument(
        "--gold-dir",
        type=Path,
        default=Path("data/gold"),
    )
    parser.add_argument(
        "--dq-summary",
        type=Path,
        default=Path("data/gold/dq_pipeline_summary.json"),
    )
    return parser.parse_args()


def load_parquet_dataset(path: Path) -> ds.Dataset:
    if not path.exists():
        raise FileNotFoundError(f"Path not found: {path}")
    parquet_files = list(path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files under {path}")
    return ds.dataset(str(path), format="parquet", partitioning="hive")


def row_count(path: Path) -> int:
    return load_parquet_dataset(path).to_table().num_rows


def sum_column(path: Path, column: str) -> float:
    table = load_parquet_dataset(path).to_table(columns=[column])
    return float(sum(float(value or 0) for value in table[column].to_pylist()))


def check_environment() -> SectionResult:
    details: list[str] = []
    passed = True

    venv_python = Path(".venv/bin/python3")
    if venv_python.exists():
        details.append(f"venv: {venv_python} exists")
    else:
        passed = False
        details.append("venv: .venv/bin/python3 missing (run: make venv)")

    try:
        import pyarrow  # noqa: F401

        details.append("pyarrow: importable")
    except ImportError:
        passed = False
        details.append("pyarrow: not installed")

    try:
        import kafka  # noqa: F401

        details.append("kafka-python: importable")
    except ImportError:
        passed = False
        details.append("kafka-python: not installed")

    makefile = Path("Makefile")
    if makefile.exists() and "wait-for-stack" in makefile.read_text(encoding="utf-8"):
        details.append("Makefile uses wait-for-stack (not docker compose wait)")
    else:
        passed = False
        details.append("Makefile missing wait-for-stack target")

    if Path("scripts/wait_for_stack.sh").exists():
        details.append("scripts/wait_for_stack.sh: present")
    else:
        passed = False
        details.append("scripts/wait_for_stack.sh: missing")

    if Path(".env.example").exists():
        details.append(".env.example: present")
    else:
        passed = False
        details.append(".env.example: missing")

    gitignore = Path(".gitignore")
    if gitignore.exists():
        text = gitignore.read_text(encoding="utf-8")
        for pattern in (".env", "data/bronze/*", "data/silver/*", "data/gold/*", "*.csv"):
            if pattern in text:
                details.append(f".gitignore covers {pattern}")
            else:
                passed = False
                details.append(f".gitignore missing {pattern}")

    return SectionResult("environment", passed, details)


def _parse_event_time(value: str) -> bool:
    try:
        datetime.strptime(value.replace(" UTC", ""), "%Y-%m-%d %H:%M:%S")
        return True
    except (ValueError, AttributeError):
        return False


def check_sample_file(path: Path, expected_rows: int, label: str) -> SectionResult:
    details: list[str] = []
    passed = True

    if not path.exists():
        return SectionResult(
            f"sample_{label}",
            False,
            [f"{path}: missing"],
        )

    size_mb = path.stat().st_size / (1024 * 1024)
    details.append(f"{path}: {size_mb:.1f} MB")

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return SectionResult(f"sample_{label}", False, [f"{path}: no header"])

        missing_cols = [c for c in SAMPLE_REQUIRED_COLUMNS if c not in reader.fieldnames]
        if missing_cols:
            passed = False
            details.append(f"missing columns: {', '.join(missing_cols)}")
        else:
            details.append("required columns: ok")

        row_num = 0
        event_ids: set[str] = set()
        invalid_types = 0
        unparseable_time = 0
        duplicate_ids = 0
        negative_price = 0

        for row in reader:
            row_num += 1
            event_id = row.get("event_id", "")
            if event_id in event_ids:
                duplicate_ids += 1
            else:
                event_ids.add(event_id)

            event_type = row.get("event_type", "")
            if event_type not in VALID_EVENT_TYPES:
                invalid_types += 1

            if not _parse_event_time(row.get("event_time", "")):
                unparseable_time += 1

            price = row.get("price", "")
            if price:
                try:
                    if float(price) < 0:
                        negative_price += 1
                except ValueError:
                    pass

        details.append(f"data rows: {row_num:,} (expected {expected_rows:,})")
        if row_num != expected_rows:
            passed = False
            details.append(f"row count mismatch: got {row_num:,}, expected {expected_rows:,}")

        if duplicate_ids:
            passed = False
            details.append(f"duplicate event_id rows: {duplicate_ids:,}")
        else:
            details.append("event_id uniqueness: ok")

        if invalid_types:
            passed = False
            details.append(f"invalid event_type rows: {invalid_types:,}")
        if unparseable_time:
            passed = False
            details.append(f"unparseable event_time rows: {unparseable_time:,}")
        if negative_price:
            passed = False
            details.append(f"negative price rows: {negative_price:,}")

    return SectionResult(f"sample_{label}", passed, details)


def check_sample_files(args: argparse.Namespace) -> list[SectionResult]:
    results = [check_sample_file(args.sample_1m, 1_000_000, "1m")]
    if args.sample_5m.exists():
        results.append(check_sample_file(args.sample_5m, 5_000_000, "5m"))
    else:
        results.append(
            SectionResult(
                "sample_5m",
                True,
                [f"{args.sample_5m}: not present (optional local stress sample)"],
            )
        )
    return results


def check_bronze(bronze_path: Path) -> SectionResult:
    details: list[str] = []
    passed = True

    try:
        dataset = load_parquet_dataset(bronze_path)
        table = dataset.to_table()
        rows = table.num_rows
        details.append(f"rows: {rows:,}")

        if rows < 1_000_000:
            passed = False
            details.append(f"bronze rows {rows:,} below 1M demo minimum")

        columns = set(table.column_names)
        for col in BRONZE_KAFKA_COLUMNS:
            if col in columns:
                details.append(f"kafka metadata column present: {col}")
            else:
                passed = False
                details.append(f"missing kafka metadata column: {col}")

        if "event_date" in columns and "event_type" in columns:
            details.append("partition columns event_date, event_type: present")
        else:
            passed = False
            details.append("missing partition columns")

        quarantine = Path("data/bronze/quarantine")
        if quarantine.exists():
            quarantine_files = list(quarantine.rglob("*.parquet"))
            details.append(f"quarantine path: {len(quarantine_files)} parquet file(s)")
        else:
            details.append("quarantine path: absent (ok if no invalid rows)")

    except FileNotFoundError as exc:
        return SectionResult("bronze", False, [str(exc)])

    return SectionResult("bronze", passed, details)


def check_silver(silver_path: Path, bronze_path: Path) -> SectionResult:
    details: list[str] = []
    passed = True

    try:
        rows = row_count(silver_path)
        bronze_rows = row_count(bronze_path)
        details.append(f"rows: {rows:,}")
        details.append(f"bronze rows (comparison): {bronze_rows:,}")

        if rows < 1_000_000:
            passed = False
            details.append(f"silver rows {rows:,} below 1M demo minimum")
        if rows > bronze_rows:
            passed = False
            details.append(f"silver exceeds bronze ({rows:,} > {bronze_rows:,})")

        table = load_parquet_dataset(silver_path).to_table(columns=["event_id"])
        event_ids = [str(v) for v in table["event_id"].to_pylist() if v is not None]
        dupes = len(event_ids) - len(set(event_ids))
        if dupes:
            passed = False
            details.append(f"duplicate event_id rows: {dupes:,}")
        else:
            details.append("event_id uniqueness: ok")

        columns = set(load_parquet_dataset(silver_path).to_table().column_names)
        for col in ("event_ts", "category_code", "brand"):
            if col in columns:
                details.append(f"column present: {col}")
            else:
                passed = False
                details.append(f"missing column: {col}")

    except FileNotFoundError as exc:
        return SectionResult("silver", False, [str(exc)])

    return SectionResult("silver", passed, details)


def check_gold_tables(gold_dir: Path) -> SectionResult:
    details: list[str] = []
    passed = True

    for table in GOLD_TABLES:
        path = gold_dir / table
        try:
            count = row_count(path)
            details.append(f"{table}: {count:,} rows")
        except FileNotFoundError:
            passed = False
            details.append(f"{table}: missing or empty")

    summary = gold_dir / "dq_pipeline_summary.json"
    if summary.exists():
        details.append(f"dq_pipeline_summary.json: present ({summary.stat().st_size} bytes)")
    else:
        passed = False
        details.append("dq_pipeline_summary.json: missing")

    return SectionResult("gold_tables", passed, details)


def run_verify_1m() -> SectionResult:
    details: list[str] = []
    try:
        result = subprocess.run(
            ["make", "verify-1m"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            details.append("make verify-1m: PASSED")
            return SectionResult("cross_layer_reconciliation", True, details)
        details.append("make verify-1m: FAILED")
        for line in result.stdout.splitlines()[-20:]:
            details.append(line)
        for line in result.stderr.splitlines()[-10:]:
            details.append(line)
        return SectionResult("cross_layer_reconciliation", False, details)
    except OSError as exc:
        return SectionResult("cross_layer_reconciliation", False, [str(exc)])


def check_milestones(gold_dir: Path, dq_summary: Path) -> SectionResult:
    details: list[str] = []
    passed = True

    metrics = {
        "bronze_rows": row_count(Path("data/bronze/events")),
        "silver_rows": row_count(Path("data/silver/events")),
        "fct_sessions": row_count(gold_dir / "fct_sessions"),
        "fct_purchases": row_count(gold_dir / "fct_purchases"),
        "agg_product_performance": row_count(gold_dir / "agg_product_performance"),
        "fct_cart_abandonment": row_count(gold_dir / "fct_cart_abandonment"),
        "total_revenue": sum_column(gold_dir / "fct_purchases", "purchase_amount"),
    }

    for key, actual in metrics.items():
        expected = MILESTONE[key]
        tol = MILESTONE_TOLERANCE[key]
        delta = abs(actual - expected)
        ok = delta <= tol
        status = "ok" if ok else "DRIFT"
        if not ok:
            passed = False
        if key == "total_revenue":
            details.append(
                f"{key}: {actual:,.2f} (expected ~{expected:,.2f}, tol ±{tol:,.2f}) [{status}]"
            )
        else:
            details.append(
                f"{key}: {actual:,} (expected ~{expected:,}, tol ±{tol:,}) [{status}]"
            )

    if dq_summary.exists():
        payload = json.loads(dq_summary.read_text(encoding="utf-8"))
        overall = payload.get("overall_status", "UNKNOWN")
        details.append(f"dq_pipeline_summary overall_status: {overall}")
        if overall != "PASS":
            passed = False
    else:
        passed = False
        details.append("dq_pipeline_summary.json: missing")

    if metrics["bronze_rows"] > metrics["silver_rows"]:
        details.append(
            "note: bronze > silver indicates cumulative Kafka replay without reset-demo-state"
        )
    if metrics["bronze_rows"] == metrics["silver_rows"] == MILESTONE["bronze_rows"]:
        details.append("clean 1M state: bronze and silver match milestone exactly")

    return SectionResult("milestones", passed, details)


def print_report(sections: list[SectionResult]) -> int:
    print("")
    print("=" * 60)
    print("LOCAL QUALITY GATE REPORT (Weeks 1–2)")
    print("=" * 60)
    for section in sections:
        status = "PASS" if section.passed else "FAIL"
        print(f"\n[{status}] {section.name}")
        for detail in section.details:
            print(f"  {detail}")

    overall = all(s.passed for s in sections)
    print("")
    print("=" * 60)
    print("OVERALL:", "PASSED" if overall else "FAILED")
    print("=" * 60)
    return 0 if overall else 1


def main() -> int:
    args = parse_args()
    sections: list[SectionResult] = []

    sections.append(check_environment())
    sections.extend(check_sample_files(args))

    if args.bronze_path.exists():
        sections.append(check_bronze(args.bronze_path))
    else:
        sections.append(
            SectionResult("bronze", False, [f"{args.bronze_path}: not found (run pipeline first)"])
        )

    if args.silver_path.exists():
        sections.append(check_silver(args.silver_path, args.bronze_path))
    else:
        sections.append(
            SectionResult("silver", False, [f"{args.silver_path}: not found (run pipeline first)"])
        )

    sections.append(check_gold_tables(args.gold_dir))

    if args.skip_pipeline:
        if args.dq_summary.exists():
            payload = json.loads(args.dq_summary.read_text(encoding="utf-8"))
            ok = payload.get("overall_status") == "PASS"
            sections.append(
                SectionResult(
                    "cross_layer_reconciliation",
                    ok,
                    [f"skipped make verify-1m; existing summary status={payload.get('overall_status')}"],
                )
            )
        else:
            sections.append(
                SectionResult(
                    "cross_layer_reconciliation",
                    False,
                    ["skipped make verify-1m but dq_pipeline_summary.json missing"],
                )
            )
    else:
        sections.append(run_verify_1m())

    if args.gold_dir.exists():
        sections.append(check_milestones(args.gold_dir, args.dq_summary))

    return print_report(sections)


if __name__ == "__main__":
    raise SystemExit(main())
