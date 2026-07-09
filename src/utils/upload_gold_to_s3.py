#!/usr/bin/env python3
"""Upload curated gold lakehouse outputs to S3 (gold/ prefix only).

Never uploads data/raw/, bronze, or silver. Intended for use with the dedicated
least-privilege IAM upload user credentials in local .env.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from boto3.exceptions import S3UploadFailedError

LOG = logging.getLogger("upload_gold_to_s3")

DEFAULT_GOLD_PATH = Path("data/gold")
DEFAULT_S3_PREFIX = "gold"
ALLOWED_SUFFIXES = {".parquet", ".json"}
FORBIDDEN_TOP_LEVEL_DIRS = {"raw", "bronze", "silver"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload curated gold Parquet/JSON outputs to S3 (gold/ prefix only)."
    )
    parser.add_argument(
        "--gold-path",
        type=Path,
        default=DEFAULT_GOLD_PATH,
        help=f"Local gold directory (default: {DEFAULT_GOLD_PATH})",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("AWS_S3_BUCKET"),
        help="S3 bucket name (default: AWS_S3_BUCKET env var)",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION", "us-east-1"),
        help="AWS region (default: AWS_REGION env var or us-east-1)",
    )
    parser.add_argument(
        "--prefix",
        default=os.getenv("AWS_S3_GOLD_PREFIX", DEFAULT_S3_PREFIX),
        help=f"S3 key prefix under the bucket (default: {DEFAULT_S3_PREFIX})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be uploaded without calling S3.",
    )
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def normalize_prefix(prefix: str) -> str:
    cleaned = prefix.strip().strip("/")
    if cleaned != "gold":
        raise ValueError(
            f"S3 prefix must be 'gold' for curated uploads (got: {prefix!r}). "
            "Raw, bronze, and silver must never be uploaded."
        )
    return cleaned


def resolve_gold_path(gold_path: Path) -> Path:
    if not gold_path.exists():
        raise FileNotFoundError(f"Gold path not found: {gold_path}")

    resolved = gold_path.resolve()
    parts = {part.lower() for part in resolved.parts}
    if parts & FORBIDDEN_TOP_LEVEL_DIRS and "gold" not in parts:
        raise ValueError(f"Refusing to upload from forbidden path: {resolved}")

    # Ensure path looks like .../data/gold (or ends with /gold)
    if resolved.name != "gold":
        raise ValueError(
            f"Gold path must point at the gold layer directory (…/data/gold), got: {resolved}"
        )

    return resolved


def collect_upload_files(gold_path: Path) -> list[Path]:
    files: list[Path] = []
    for path in sorted(gold_path.rglob("*")):
        if not path.is_file():
            continue
        if path.name == ".gitkeep":
            continue
        if path.name.startswith("."):
            continue
        if path.name in {"_SUCCESS"} or path.suffix == ".crc":
            continue
        if path.suffix not in ALLOWED_SUFFIXES:
            LOG.warning("skipping non-gold artifact: %s", path)
            continue
        files.append(path)
    return files


def build_s3_key(prefix: str, gold_root: Path, local_file: Path) -> str:
    relative = local_file.relative_to(gold_root).as_posix()
    return f"{prefix}/{relative}"


def create_s3_client(region: str) -> object:
    try:
        return boto3.client("s3", region_name=region)
    except NoCredentialsError as exc:
        raise RuntimeError(
            "AWS credentials not found. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY "
            "in .env (upload user keys after terraform apply)."
        ) from exc


def verify_upload_access(client: object, bucket: str, prefix: str, region: str) -> None:
    """Check bucket access using list_objects with prefix (matches upload IAM policy)."""
    list_prefix = f"{prefix}/"
    try:
        client.list_objects_v2(Bucket=bucket, Prefix=list_prefix, MaxKeys=1)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in {"404", "NoSuchBucket", "NotFound"}:
            raise RuntimeError(
                f"S3 bucket does not exist: s3://{bucket}/\n"
                "Create it first:\n"
                "  cd infra/aws\n"
                "  terraform apply -var-file=terraform.tfvars\n"
                "Then copy upload IAM keys to .env and retry make upload-gold-s3."
            ) from exc
        if error_code == "403":
            raise RuntimeError(
                f"Access denied listing s3://{bucket}/{list_prefix} in region {region}. "
                "Use the dedicated upload IAM keys from terraform output in .env "
                "(not admin credentials). Required: ListBucket on gold/* and PutObject on gold/*."
            ) from exc
        raise RuntimeError(f"Could not access s3://{bucket}/{list_prefix}: {exc}") from exc


def upload_files(
    client: object,
    bucket: str,
    prefix: str,
    gold_root: Path,
    files: list[Path],
    dry_run: bool,
) -> tuple[int, int]:
    total_bytes = 0
    uploaded = 0

    for local_file in files:
        key = build_s3_key(prefix, gold_root, local_file)
        size = local_file.stat().st_size
        total_bytes += size

        if dry_run:
            LOG.info("dry-run: would upload %s -> s3://%s/%s (%.2f MB)", local_file, bucket, key, size / (1024 * 1024))
            uploaded += 1
            continue

        try:
            client.upload_file(
                str(local_file),
                bucket,
                key,
                ExtraArgs={"ServerSideEncryption": "AES256"},
            )
            uploaded += 1
            if uploaded == 1 or uploaded % 50 == 0:
                LOG.info("uploaded %s files (latest: s3://%s/%s)", uploaded, bucket, key)
        except S3UploadFailedError as exc:
            message = str(exc)
            if "NoSuchBucket" in message:
                raise RuntimeError(
                    f"S3 bucket does not exist: s3://{bucket}/\n"
                    "Run: cd infra/aws && terraform apply -var-file=terraform.tfvars"
                ) from exc
            raise RuntimeError(f"Failed to upload {local_file} to s3://{bucket}/{key}: {exc}") from exc
        except (ClientError, BotoCoreError) as exc:
            raise RuntimeError(f"Failed to upload {local_file} to s3://{bucket}/{key}: {exc}") from exc

    return uploaded, total_bytes


def main() -> int:
    configure_logging()
    args = parse_args()

    if not args.bucket:
        LOG.error("Missing S3 bucket. Set AWS_S3_BUCKET in .env or pass --bucket.")
        return 1

    try:
        prefix = normalize_prefix(args.prefix)
        gold_root = resolve_gold_path(args.gold_path)
    except (FileNotFoundError, ValueError) as exc:
        LOG.error("%s", exc)
        return 1

    files = collect_upload_files(gold_root)
    if not files:
        LOG.error("No uploadable gold files found under %s (.parquet / .json)", gold_root)
        return 1

    LOG.info(
        "gold upload: path=%s bucket=%s prefix=%s region=%s dry_run=%s files=%s",
        gold_root,
        args.bucket,
        prefix,
        args.region,
        args.dry_run,
        len(files),
    )

    client = None if args.dry_run else create_s3_client(args.region)

    try:
        if not args.dry_run:
            verify_upload_access(client, args.bucket, prefix, args.region)
        uploaded, total_bytes = upload_files(client, args.bucket, prefix, gold_root, files, args.dry_run)
    except RuntimeError as exc:
        LOG.error("%s", exc)
        return 1

    total_mb = total_bytes / (1024 * 1024)
    LOG.info(
        "Finished: %s %s files (%.2f MB total) to s3://%s/%s/",
        "would upload" if args.dry_run else "uploaded",
        f"{uploaded:,}",
        total_mb,
        args.bucket,
        prefix,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
