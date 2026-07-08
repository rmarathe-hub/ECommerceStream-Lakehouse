#!/usr/bin/env python3
"""Spark batch job: silver events -> sessionized events + session fact table."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, Window, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sessionize silver events and build the fct_sessions gold mart."
    )
    parser.add_argument("--silver-path", default="/opt/data/silver/events")
    parser.add_argument(
        "--session-events-path",
        default="/opt/data/silver/session_events",
    )
    parser.add_argument("--sessions-path", default="/opt/data/gold/fct_sessions")
    parser.add_argument(
        "--write-mode",
        default="overwrite",
        choices=("overwrite", "append"),
        help="Parquet write mode for outputs.",
    )
    return parser.parse_args()


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("silver_sessionize")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_silver(spark: SparkSession, silver_path: str) -> DataFrame:
    return spark.read.parquet(silver_path)


def enrich_with_session_metrics(silver_df: DataFrame) -> DataFrame:
    session_window = Window.partitionBy("user_session")
    event_order_window = Window.partitionBy("user_session").orderBy(
        F.col("event_ts").asc_nulls_last(),
        F.col("event_id").asc_nulls_last(),
    )

    return (
        silver_df.withColumn("session_start_ts", F.min("event_ts").over(session_window))
        .withColumn("session_end_ts", F.max("event_ts").over(session_window))
        .withColumn("event_seq_in_session", F.row_number().over(event_order_window))
        .withColumn(
            "seconds_from_session_start",
            F.unix_timestamp("event_ts") - F.unix_timestamp("session_start_ts"),
        )
    )


def build_fct_sessions(sessionized_df: DataFrame) -> DataFrame:
    return (
        sessionized_df.groupBy("user_session", "user_id")
        .agg(
            F.min("event_ts").alias("session_start_ts"),
            F.max("event_ts").alias("session_end_ts"),
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
            F.sum(F.when(F.col("event_type") == "remove_from_cart", 1).otherwise(0)).alias(
                "remove_from_cart_count"
            ),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            F.countDistinct(
                F.when(F.col("event_type") == "view", F.col("product_id"))
            ).alias("distinct_products_viewed"),
            F.countDistinct(
                F.when(F.col("event_type") == "purchase", F.col("product_id"))
            ).alias("distinct_products_purchased"),
            F.sum(
                F.when(F.col("event_type") == "purchase", F.coalesce(F.col("price"), F.lit(0.0))).otherwise(
                    0.0
                )
            ).alias("session_revenue"),
        )
        .withColumn("session_id", F.col("user_session"))
        .withColumn("session_date", F.to_date("session_start_ts"))
        .withColumn(
            "session_duration_seconds",
            F.unix_timestamp("session_end_ts") - F.unix_timestamp("session_start_ts"),
        )
        .withColumn("converted", F.col("purchase_count") > 0)
        .withColumn("gold_processed_at", F.current_timestamp())
        .select(
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
        )
    )


def select_session_events(sessionized_df: DataFrame) -> DataFrame:
    return sessionized_df.select(
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
        F.col("user_session").alias("session_id"),
        "event_seq_in_session",
        "session_start_ts",
        "session_end_ts",
        "seconds_from_session_start",
        "bronze_ingested_at",
        "silver_processed_at",
        F.current_timestamp().alias("sessionized_at"),
    )


def write_parquet(df: DataFrame, path: str, write_mode: str, partition_col: str) -> None:
    df.write.mode(write_mode).partitionBy(partition_col).parquet(path)


def main() -> None:
    args = parse_args()
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    silver_df = read_silver(spark, args.silver_path)
    silver_count = silver_df.count()
    if silver_count == 0:
        raise ValueError(f"No silver rows found at {args.silver_path}")

    sessionized_df = enrich_with_session_metrics(silver_df)
    session_events_df = select_session_events(sessionized_df)
    fct_sessions_df = build_fct_sessions(sessionized_df)

    session_events_count = session_events_df.count()
    session_count = fct_sessions_df.count()

    write_parquet(session_events_df, args.session_events_path, args.write_mode, "event_date")
    write_parquet(fct_sessions_df, args.sessions_path, args.write_mode, "session_date")

    print(f"silver rows read: {silver_count:,}")
    print(f"session_events rows written: {session_events_count:,}")
    print(f"fct_sessions rows written: {session_count:,}")
    print(f"session_events output: {args.session_events_path}")
    print(f"fct_sessions output: {args.sessions_path}")

    spark.stop()


if __name__ == "__main__":
    main()
