#!/usr/bin/env python3
"""Spark Structured Streaming job: Kafka/Redpanda -> bronze Parquet."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VALID_EVENT_TYPES = ("view", "cart", "remove_from_cart", "purchase")

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream Kafka events into bronze Parquet.")
    parser.add_argument("--kafka-bootstrap", default="redpanda:9092")
    parser.add_argument("--topic", default="ecommerce_events")
    parser.add_argument("--bronze-path", default="/opt/data/bronze/events")
    parser.add_argument("--quarantine-path", default="/opt/data/bronze/quarantine")
    parser.add_argument("--checkpoint-path", default="/opt/data/bronze/checkpoints/kafka_to_bronze")
    parser.add_argument(
        "--starting-offsets",
        default="earliest",
        choices=("earliest", "latest"),
        help="Kafka starting offsets when no checkpoint exists.",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Delete checkpoint before starting (reprocess from starting offsets).",
    )
    return parser.parse_args()


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("kafka_to_bronze")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession, bootstrap: str, topic: str, starting_offsets: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(kafka_df: DataFrame) -> DataFrame:
    parsed = (
        kafka_df.select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
        )
        .select("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "event.*")
        .withColumn(
            "event_ts",
            F.to_timestamp(F.regexp_replace(F.col("event_time"), " UTC$", ""), "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("ingested_at", F.current_timestamp())
    )

    return parsed.withColumn(
        "is_valid",
        F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("event_type").isin(*VALID_EVENT_TYPES)
        & F.col("event_date").isNotNull(),
    ).withColumn(
        "invalid_reason",
        F.when(F.col("event_id").isNull(), F.lit("missing_event_id"))
        .when(F.col("event_time").isNull(), F.lit("missing_event_time"))
        .when(~F.col("event_type").isin(*VALID_EVENT_TYPES), F.lit("invalid_event_type"))
        .when(F.col("event_date").isNull(), F.lit("unparseable_event_time"))
        .otherwise(F.lit(None)),
    )


def write_batch(
    batch_df: DataFrame,
    batch_id: int,
    bronze_path: str,
    quarantine_path: str,
) -> None:
    if batch_df.isEmpty():
        return

    bronze_cols = [
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
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingested_at",
    ]

    good_df = batch_df.filter(F.col("is_valid")).select(*bronze_cols)
    if not good_df.isEmpty():
        (
            good_df.write.mode("append")
            .partitionBy("event_date", "event_type")
            .parquet(bronze_path)
        )

    bad_df = batch_df.filter(~F.col("is_valid"))
    if not bad_df.isEmpty():
        (
            bad_df.select(
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
                "invalid_reason",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "ingested_at",
            )
            .write.mode("append")
            .parquet(quarantine_path)
        )


def maybe_reset_checkpoint(checkpoint_path: str, reset: bool) -> None:
    path = Path(checkpoint_path)
    if reset and path.exists():
        shutil.rmtree(path)


def main() -> None:
    args = parse_args()
    maybe_reset_checkpoint(args.checkpoint_path, args.reset_checkpoint)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_kafka(spark, args.kafka_bootstrap, args.topic, args.starting_offsets)
    parsed_df = parse_events(kafka_df)

    query = (
        parsed_df.writeStream.foreachBatch(
            lambda df, batch_id: write_batch(
                df, batch_id, args.bronze_path, args.quarantine_path
            )
        )
        .option("checkpointLocation", args.checkpoint_path)
        .trigger(availableNow=True)
        .queryName("kafka_to_bronze")
        .start()
    )

    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
