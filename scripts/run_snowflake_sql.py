#!/usr/bin/env python3
"""CI-friendly Snowflake SQL runner (no SnowSQL required).

Skips SnowSQL meta-commands (!set, !source, …).

Usage:
  python scripts/run_snowflake_sql.py sql/admin/05_check_snowflake_guardrails.sql
  python scripts/run_snowflake_sql.py --ignore-errors sql/admin/04_suspend_warehouse.sql
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


def strip_snowsql(sql_text: str) -> str:
    kept: list[str] = []
    for line in sql_text.splitlines():
        if line.strip().startswith("!"):
            continue
        kept.append(line)
    return "\n".join(kept)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a Snowflake SQL file via connector")
    parser.add_argument("sql_file", type=Path)
    parser.add_argument(
        "--ignore-errors",
        action="store_true",
        help="Continue on statement errors (e.g. suspend when already suspended)",
    )
    parser.add_argument(
        "--warehouse",
        default=os.environ.get("SNOWFLAKE_WAREHOUSE", "DE_PROJECT_WH"),
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    os.chdir(repo_root)
    load_dotenv(repo_root / ".env")

    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ROLE"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        print(f"Missing env: {', '.join(missing)}", file=sys.stderr)
        return 1

    if not args.sql_file.exists():
        print(f"SQL file not found: {args.sql_file}", file=sys.stderr)
        return 1

    import snowflake.connector

    sql = strip_snowsql(args.sql_file.read_text())
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=args.warehouse,
        database=os.environ.get("SNOWFLAKE_DATABASE", "COMMERCESTREAM_DB"),
        client_session_keep_alive=False,
    )
    cur = conn.cursor()
    try:
        for stmt in statements:
            print(f"-- executing ({len(stmt)} chars)")
            try:
                cur.execute(stmt)
                # SHOW / SELECT may return rows; DDL often does not
                if cur.description:
                    rows = cur.fetchall()
                    for row in rows[:30]:
                        print(row)
                    if len(rows) > 30:
                        print(f"... ({len(rows)} rows total)")
            except Exception as exc:  # noqa: BLE001
                if args.ignore_errors:
                    print(f"ignored error: {exc}")
                else:
                    raise
    finally:
        cur.close()
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
