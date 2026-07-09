#!/usr/bin/env python3
"""Spark batch job: session events -> purchase fact and product performance marts."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build fct_purchases and agg_product_performance gold marts."
    )
    parser.add_argument(
        "--session-events-path",
        default="/opt/data/silver/session_events",
    )
    parser.add_argument(
        "--purchases-path",
        default="/opt/data/gold/fct_purchases",
    )
    parser.add_argument(
        "--product-performance-path",
        default="/opt/data/gold/agg_product_performance",
    )
    parser.add_argument(
        "--write-mode",
        default="overwrite",
        choices=("overwrite", "append"),
        help="Parquet write mode for outputs.",
    )
    return parser.parse_args()


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("build_purchase_product_marts")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_session_events(spark: SparkSession, session_events_path: str) -> DataFrame:
    return spark.read.parquet(session_events_path)


def build_fct_purchases(session_events_df: DataFrame) -> DataFrame:
    return (
        session_events_df.filter(F.col("event_type") == "purchase")
        .select(
            F.col("event_id").alias("purchase_id"),
            F.col("event_ts").alias("purchase_ts"),
            F.col("event_date").alias("purchase_date"),
            "session_id",
            "user_id",
            "product_id",
            "category_id",
            "category_code",
            "brand",
            F.col("price").alias("purchase_amount"),
            "event_seq_in_session",
            "seconds_from_session_start",
            "bronze_ingested_at",
            "silver_processed_at",
            F.current_timestamp().alias("gold_processed_at"),
        )
    )


def build_agg_product_performance(session_events_df: DataFrame) -> DataFrame:
    product_metrics = (
        session_events_df.groupBy("product_id")
        .agg(
            F.max("category_id").alias("category_id"),
            F.max("category_code").alias("category_code"),
            F.max("brand").alias("brand"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
            F.sum(F.when(F.col("event_type") == "remove_from_cart", 1).otherwise(0)).alias(
                "remove_from_cart_count"
            ),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            F.countDistinct(
                F.when(F.col("event_type") == "view", F.col("user_id"))
            ).alias("unique_viewers"),
            F.countDistinct(
                F.when(F.col("event_type") == "cart", F.col("user_id"))
            ).alias("unique_cart_adders"),
            F.countDistinct(
                F.when(F.col("event_type") == "purchase", F.col("user_id"))
            ).alias("unique_purchasers"),
            F.sum(
                F.when(
                    F.col("event_type") == "purchase",
                    F.coalesce(F.col("price"), F.lit(0.0)),
                ).otherwise(0.0)
            ).alias("total_revenue"),
            F.max("event_date").alias("last_event_date"),
        )
        .withColumn(
            "view_to_purchase_rate",
            F.least(
                F.lit(1.0),
                F.when(
                    F.col("view_count") > 0,
                    F.col("purchase_count") / F.col("view_count"),
                ).otherwise(F.lit(0.0)),
            ),
        )
        .withColumn(
            "cart_to_purchase_rate",
            F.least(
                F.lit(1.0),
                F.when(
                    F.col("cart_count") > 0,
                    F.col("purchase_count") / F.col("cart_count"),
                ).otherwise(F.lit(0.0)),
            ),
        )
        .withColumn("gold_processed_at", F.current_timestamp())
    )

    return product_metrics


def write_parquet(
    df: DataFrame,
    path: str,
    write_mode: str,
    partition_col: str | None = None,
) -> None:
    writer = df.write.mode(write_mode)
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.parquet(path)


def main() -> None:
    args = parse_args()
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    session_events_df = read_session_events(spark, args.session_events_path)
    session_events_count = session_events_df.count()
    if session_events_count == 0:
        raise ValueError(f"No session events found at {args.session_events_path}")

    fct_purchases_df = build_fct_purchases(session_events_df)
    agg_product_df = build_agg_product_performance(session_events_df)

    purchase_count = fct_purchases_df.count()
    product_count = agg_product_df.count()

    write_parquet(fct_purchases_df, args.purchases_path, args.write_mode, "purchase_date")
    write_parquet(agg_product_df, args.product_performance_path, args.write_mode)

    print(f"session_events rows read: {session_events_count:,}")
    print(f"fct_purchases rows written: {purchase_count:,}")
    print(f"agg_product_performance rows written: {product_count:,}")
    print(f"fct_purchases output: {args.purchases_path}")
    print(f"agg_product_performance output: {args.product_performance_path}")

    spark.stop()


if __name__ == "__main__":
    main()
