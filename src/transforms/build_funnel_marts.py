#!/usr/bin/env python3
"""Spark batch job: build conversion funnel and cart abandonment gold marts."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build agg_conversion_funnel and fct_cart_abandonment gold marts."
    )
    parser.add_argument(
        "--session-events-path",
        default="/opt/data/silver/session_events",
    )
    parser.add_argument(
        "--sessions-path",
        default="/opt/data/gold/fct_sessions",
    )
    parser.add_argument(
        "--funnel-path",
        default="/opt/data/gold/agg_conversion_funnel",
    )
    parser.add_argument(
        "--cart-abandonment-path",
        default="/opt/data/gold/fct_cart_abandonment",
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
        SparkSession.builder.appName("build_funnel_marts")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def build_session_funnel_flags(session_events_df: DataFrame) -> DataFrame:
    return (
        session_events_df.groupBy("session_id")
        .agg(
            F.min("user_id").alias("user_id"),
            F.min("event_date").alias("session_date"),
            F.max(F.when(F.col("event_type") == "view", F.lit(1)).otherwise(F.lit(0))).alias("has_view"),
            F.max(F.when(F.col("event_type") == "cart", F.lit(1)).otherwise(F.lit(0))).alias("has_cart"),
            F.max(F.when(F.col("event_type") == "purchase", F.lit(1)).otherwise(F.lit(0))).alias(
                "has_purchase"
            ),
            F.min(F.when(F.col("event_type") == "view", F.col("event_seq_in_session"))).alias(
                "first_view_seq"
            ),
            F.min(F.when(F.col("event_type") == "cart", F.col("event_seq_in_session"))).alias(
                "first_cart_seq"
            ),
            F.min(F.when(F.col("event_type") == "purchase", F.col("event_seq_in_session"))).alias(
                "first_purchase_seq"
            ),
        )
        .withColumn(
            "has_strict_view_to_cart",
            F.col("first_view_seq").isNotNull()
            & F.col("first_cart_seq").isNotNull()
            & (F.col("first_view_seq") < F.col("first_cart_seq")),
        )
        .withColumn(
            "has_strict_cart_to_purchase",
            F.col("first_cart_seq").isNotNull()
            & F.col("first_purchase_seq").isNotNull()
            & (F.col("first_cart_seq") < F.col("first_purchase_seq")),
        )
        .withColumn(
            "has_strict_view_to_purchase",
            F.col("first_view_seq").isNotNull()
            & F.col("first_purchase_seq").isNotNull()
            & (F.col("first_view_seq") < F.col("first_purchase_seq")),
        )
        .withColumn("is_cart_abandoned", (F.col("has_cart") == 1) & (F.col("has_purchase") == 0))
    )


def build_agg_conversion_funnel(session_flags_df: DataFrame) -> DataFrame:
    return (
        session_flags_df.groupBy("session_date")
        .agg(
            F.count("*").alias("total_sessions"),
            F.sum("has_view").alias("sessions_with_view"),
            F.sum("has_cart").alias("sessions_with_cart"),
            F.sum("has_purchase").alias("sessions_with_purchase"),
            F.sum(F.col("has_strict_view_to_cart").cast("int")).alias("view_to_cart_sessions"),
            F.sum(F.col("has_strict_cart_to_purchase").cast("int")).alias("cart_to_purchase_sessions"),
            F.sum(F.col("has_strict_view_to_purchase").cast("int")).alias("view_to_purchase_sessions"),
            F.sum(F.col("is_cart_abandoned").cast("int")).alias("abandoned_cart_sessions"),
        )
        .withColumn(
            "view_to_cart_rate",
            F.least(
                F.lit(1.0),
                F.when(
                    F.col("sessions_with_view") > 0,
                    F.col("view_to_cart_sessions") / F.col("sessions_with_view"),
                ).otherwise(F.lit(0.0)),
            ),
        )
        .withColumn(
            "cart_to_purchase_rate",
            F.least(
                F.lit(1.0),
                F.when(
                    F.col("sessions_with_cart") > 0,
                    F.col("cart_to_purchase_sessions") / F.col("sessions_with_cart"),
                ).otherwise(F.lit(0.0)),
            ),
        )
        .withColumn(
            "view_to_purchase_rate",
            F.least(
                F.lit(1.0),
                F.when(
                    F.col("sessions_with_view") > 0,
                    F.col("view_to_purchase_sessions") / F.col("sessions_with_view"),
                ).otherwise(F.lit(0.0)),
            ),
        )
        .withColumn(
            "cart_abandonment_rate",
            F.least(
                F.lit(1.0),
                F.when(
                    F.col("sessions_with_cart") > 0,
                    F.col("abandoned_cart_sessions") / F.col("sessions_with_cart"),
                ).otherwise(F.lit(0.0)),
            ),
        )
        .withColumn("gold_processed_at", F.current_timestamp())
    )


def build_cart_event_details(session_events_df: DataFrame) -> DataFrame:
    return session_events_df.filter(F.col("event_type") == "cart").groupBy("session_id").agg(
        F.min("event_ts").alias("first_cart_ts"),
        F.max("event_ts").alias("last_cart_ts"),
        F.countDistinct("product_id").alias("distinct_products_carted"),
        F.count("*").alias("cart_event_count"),
    )


def build_fct_cart_abandonment(
    fct_sessions_df: DataFrame,
    cart_event_details_df: DataFrame,
) -> DataFrame:
    abandoned_sessions = fct_sessions_df.filter(
        (F.col("cart_count") > 0) & (~F.col("converted"))
    )

    return (
        abandoned_sessions.join(cart_event_details_df, on="session_id", how="left")
        .select(
            "session_id",
            "user_id",
            "session_date",
            "session_start_ts",
            "session_end_ts",
            "session_duration_seconds",
            "view_count",
            "cart_count",
            "remove_from_cart_count",
            F.coalesce(F.col("cart_event_count"), F.col("cart_count")).alias("cart_event_count"),
            F.col("distinct_products_carted"),
            "first_cart_ts",
            "last_cart_ts",
            F.lit(True).alias("abandoned"),
            F.current_timestamp().alias("gold_processed_at"),
        )
    )


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

    session_events_df = read_parquet(spark, args.session_events_path)
    fct_sessions_df = read_parquet(spark, args.sessions_path)

    session_events_count = session_events_df.count()
    if session_events_count == 0:
        raise ValueError(f"No session events found at {args.session_events_path}")

    session_flags_df = build_session_funnel_flags(session_events_df)
    funnel_df = build_agg_conversion_funnel(session_flags_df)
    cart_details_df = build_cart_event_details(session_events_df)
    abandonment_df = build_fct_cart_abandonment(fct_sessions_df, cart_details_df)

    funnel_count = funnel_df.count()
    abandonment_count = abandonment_df.count()

    write_parquet(funnel_df, args.funnel_path, args.write_mode, "session_date")
    write_parquet(abandonment_df, args.cart_abandonment_path, args.write_mode, "session_date")

    print(f"session_events rows read: {session_events_count:,}")
    print(f"agg_conversion_funnel rows written: {funnel_count:,}")
    print(f"fct_cart_abandonment rows written: {abandonment_count:,}")
    print(f"agg_conversion_funnel output: {args.funnel_path}")
    print(f"fct_cart_abandonment output: {args.cart_abandonment_path}")

    spark.stop()


if __name__ == "__main__":
    main()
