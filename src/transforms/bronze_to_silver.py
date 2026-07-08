#!/usr/bin/env python3
"""Spark batch job: bronze Parquet -> cleaned silver events."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

VALID_EVENT_TYPES = ("view", "cart", "remove_from_cart", "purchase")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform bronze events into silver Parquet.")
    parser.add_argument("--bronze-path", default="/opt/data/bronze/events")
    parser.add_argument("--silver-path", default="/opt/data/silver/events")
    parser.add_argument(
        "--write-mode",
        default="overwrite",
        choices=("overwrite", "append"),
        help="Parquet write mode for silver output.",
    )
    return parser.parse_args()


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("bronze_to_silver")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, bronze_path: str) -> DataFrame:
    return spark.read.parquet(bronze_path)


def clean_bronze(bronze_df: DataFrame) -> DataFrame:
    return (
        bronze_df.withColumn(
            "event_ts",
            F.to_timestamp(
                F.regexp_replace(F.col("event_time"), " UTC$", ""),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
        .withColumn("category_id_clean", F.expr("try_cast(category_id AS BIGINT)"))
        .withColumn(
            "brand_clean",
            F.when(
                F.col("brand").isNull() | (F.length(F.trim(F.col("brand"))) == 0),
                F.lit(None),
            ).otherwise(F.lower(F.trim(F.col("brand")))),
        )
        .withColumn(
            "category_code_clean",
            F.when(
                F.col("category_code").isNull() | (F.length(F.trim(F.col("category_code"))) == 0),
                F.lit(None),
            ).otherwise(F.trim(F.col("category_code"))),
        )
        .withColumn(
            "is_silver_eligible",
            F.col("event_id").isNotNull()
            & (F.length(F.trim(F.col("event_id"))) > 0)
            & F.col("event_type").isin(*VALID_EVENT_TYPES)
            & F.col("event_ts").isNotNull()
            & F.col("event_date").isNotNull()
            & F.col("product_id").isNotNull()
            & F.col("user_id").isNotNull()
            & F.col("user_session").isNotNull()
            & (F.length(F.trim(F.col("user_session"))) > 0)
            & (F.col("price").isNull() | (F.col("price") >= 0)),
        )
    )


def deduplicate_events(cleaned_df: DataFrame) -> DataFrame:
    window = Window.partitionBy("event_id").orderBy(
        F.col("kafka_partition").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
    )
    return (
        cleaned_df.filter(F.col("is_silver_eligible"))
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "is_silver_eligible")
    )


def select_silver_columns(deduped_df: DataFrame) -> DataFrame:
    return deduped_df.select(
        F.col("event_id"),
        F.col("event_ts"),
        F.col("event_date"),
        F.col("event_type"),
        F.col("product_id"),
        F.col("category_id_clean").alias("category_id"),
        F.col("category_code_clean").alias("category_code"),
        F.col("brand_clean").alias("brand"),
        F.col("price"),
        F.col("user_id"),
        F.col("user_session"),
        F.col("ingested_at").alias("bronze_ingested_at"),
        F.current_timestamp().alias("silver_processed_at"),
    )


def write_silver(silver_df: DataFrame, silver_path: str, write_mode: str) -> None:
    (
        silver_df.write.mode(write_mode)
        .partitionBy("event_date")
        .parquet(silver_path)
    )


def main() -> None:
    args = parse_args()
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = read_bronze(spark, args.bronze_path)
    bronze_count = bronze_df.count()
    if bronze_count == 0:
        raise ValueError(f"No bronze rows found at {args.bronze_path}")

    cleaned_df = clean_bronze(bronze_df)
    silver_df = select_silver_columns(deduplicate_events(cleaned_df))
    silver_count = silver_df.count()

    write_silver(silver_df, args.silver_path, args.write_mode)

    dropped_count = bronze_count - silver_count
    print(f"bronze rows read: {bronze_count:,}")
    print(f"silver rows written: {silver_count:,}")
    print(f"rows dropped (invalid or duplicate): {dropped_count:,}")
    print(f"silver output: {args.silver_path}")

    spark.stop()


if __name__ == "__main__":
    main()
