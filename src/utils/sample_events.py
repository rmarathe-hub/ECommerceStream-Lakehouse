#!/usr/bin/env python3
"""Create cost-controlled event samples from the e-commerce behavior dataset.

Reads one or more source CSV files (plain or gzip) from data/raw/source/ and
writes a sampled output file with an added event_id column.

Source dataset (manual download):
  https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

Place monthly files locally, e.g.:
  data/raw/source/2019-Oct.csv
  data/raw/source/2019-Nov.csv.gz
"""

from __future__ import annotations

import argparse
import csv
import gzip
import random
import sys
from pathlib import Path
from typing import Iterable, TextIO

EXPECTED_COLUMNS = [
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

OUTPUT_COLUMNS = ["event_id", *EXPECTED_COLUMNS]

DEFAULT_SOURCE_DIR = Path("data/raw/source")
DEFAULT_SOURCE_CANDIDATES = [
    DEFAULT_SOURCE_DIR / "2019-Oct.csv",
    DEFAULT_SOURCE_DIR / "2019-Oct.csv.gz",
    Path("data/raw/2019-Oct.csv"),
    Path("data/raw/2019-Oct.csv.gz"),
    DEFAULT_SOURCE_DIR / "2019-Nov.csv",
    DEFAULT_SOURCE_DIR / "2019-Nov.csv.gz",
    DEFAULT_SOURCE_DIR / "2019-Dec.csv",
    DEFAULT_SOURCE_DIR / "2019-Dec.csv.gz",
    DEFAULT_SOURCE_DIR / "2020-Jan.csv",
    DEFAULT_SOURCE_DIR / "2020-Jan.csv.gz",
]


def open_text(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return path.open("r", encoding="utf-8", newline="")


def discover_source_files(input_path: Path | None, input_dir: Path | None) -> list[Path]:
    if input_path is not None:
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
        return [input_path]

    if input_dir is not None:
        if not input_dir.exists():
            raise FileNotFoundError(f"Input directory not found: {input_dir}")
        files = sorted(
            p
            for p in input_dir.iterdir()
            if p.is_file() and (p.suffix in {".csv", ".gz"} or p.name.endswith(".csv.gz"))
        )
        if not files:
            raise FileNotFoundError(f"No CSV files found in {input_dir}")
        return files

    for candidate in DEFAULT_SOURCE_CANDIDATES:
        if candidate.exists():
            return [candidate]

    if DEFAULT_SOURCE_DIR.exists():
        files = sorted(
            p
            for p in DEFAULT_SOURCE_DIR.iterdir()
            if p.is_file() and (p.suffix == ".csv" or p.name.endswith(".csv.gz"))
        )
        if files:
            return files

    raise FileNotFoundError(
        "No source dataset found. Download a monthly file from Kaggle and place it in "
        "data/raw/source/ (e.g. data/raw/source/2019-Oct.csv). "
        "Dataset: https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store"
    )


def validate_header(fieldnames: Iterable[str] | None) -> None:
    if not fieldnames:
        raise ValueError("Source CSV is missing a header row.")
    missing = [col for col in EXPECTED_COLUMNS if col not in fieldnames]
    if missing:
        raise ValueError(f"Source CSV missing expected columns: {', '.join(missing)}")


def reservoir_sample(files: list[Path], limit: int, seed: int) -> tuple[list[dict[str, str]], int]:
    rng = random.Random(seed)
    reservoir: list[dict[str, str]] = []
    seen = 0

    for path in files:
        with open_text(path) as handle:
            reader = csv.DictReader(handle)
            validate_header(reader.fieldnames)
            for row in reader:
                seen += 1
                if len(reservoir) < limit:
                    reservoir.append(row)
                else:
                    replace_idx = rng.randrange(seen)
                    if replace_idx < limit:
                        reservoir[replace_idx] = row

                if seen % 1_000_000 == 0:
                    print(f"  scanned {seen:,} rows...", file=sys.stderr)

    return reservoir, seen


def write_sample(rows: list[dict[str, str]], output_path: Path, seed: int) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for idx, row in enumerate(rows, start=1):
            enriched = {col: row.get(col, "") for col in EXPECTED_COLUMNS}
            enriched["event_id"] = f"evt_{seed}_{idx:010d}"
            writer.writerow(enriched)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sample e-commerce events into a smaller local CSV for pipeline demos."
    )
    parser.add_argument(
        "--limit",
        type=int,
        required=True,
        help="Maximum number of events to sample (e.g. 1000000 for 1M).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output CSV path (default: data/raw/events_<limit>.csv).",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Single source CSV or CSV.GZ file.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=None,
        help="Directory of monthly source CSV files (reads in sorted order until limit is reached).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible sampling (default: 42).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.limit <= 0:
        print("error: --limit must be a positive integer", file=sys.stderr)
        return 1

    output_path = args.output
    if output_path is None:
        if args.limit >= 1_000_000:
            millions = args.limit // 1_000_000
            output_path = Path(f"data/raw/events_{millions}m.csv")
        else:
            output_path = Path(f"data/raw/events_{args.limit}.csv")

    try:
        source_files = discover_source_files(args.input, args.input_dir)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"Source file(s): {', '.join(str(p) for p in source_files)}")
    print(f"Sampling up to {args.limit:,} events (seed={args.seed}) -> {output_path}")

    rows, scanned = reservoir_sample(source_files, args.limit, args.seed)
    if not rows:
        print("error: no rows found in source file(s)", file=sys.stderr)
        return 1

    write_sample(rows, output_path, args.seed)

    print(f"Scanned {scanned:,} source rows")
    print(f"Wrote {len(rows):,} rows to {output_path}")
    if scanned < args.limit:
        print(
            f"warning: source contained only {scanned:,} rows (less than requested {args.limit:,})",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
