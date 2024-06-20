#!/usr/bin/env python3
"""Kafka clickstream.raw → validate/normalize → clickstream.cleaned + Delta bronze/silver."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DELTA_PATH = os.getenv("DELTA_PATH", "/opt/delta")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/opt/checkpoints")

RAW_TOPIC = "clickstream.raw"
CLEANED_TOPIC = "clickstream.cleaned"
DLQ_TOPIC = "clickstream.dlq"

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("ts", T.StringType()),
        T.StructField("user_id", T.StringType()),
        T.StructField("session_id", T.StringType()),
        T.StructField("page", T.StringType()),
        T.StructField("referrer", T.StringType()),
        T.StructField("device", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("utm_source", T.StringType()),
        T.StructField("latency_ms", T.IntegerType()),
        T.StructField("amount", T.DoubleType()),
    ]
)

VALID_TYPES = ["page_view", "click", "add_to_cart", "purchase", "search"]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("clickstream-clean")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("timestamp").alias("kafka_ts"),
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
    ).select("kafka_key", "raw_json", "kafka_ts", "e.*")

    with_ts = parsed.withColumn("event_ts", F.to_timestamp("ts"))

    valid = (
        with_ts.filter(F.col("event_id").isNotNull())
        .filter(F.col("event_type").isin(VALID_TYPES))
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("session_id").isNotNull())
        .filter(F.col("page").isNotNull())
        .filter(F.col("event_ts").isNotNull())
        .withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
        .withColumn("latency_ms", F.coalesce(F.col("latency_ms"), F.lit(0)))
        .withColumn("device", F.lower(F.coalesce(F.col("device"), F.lit("unknown"))))
        .withColumn("country", F.upper(F.coalesce(F.col("country"), F.lit("ZZ"))))
        .withColumn("cleaned_at", F.current_timestamp())
        .drop("ts")
    )

    invalid = with_ts.filter(
        F.col("event_id").isNull()
        | F.col("event_type").isNull()
        | (~F.col("event_type").isin(VALID_TYPES))
        | F.col("user_id").isNull()
        | F.col("session_id").isNull()
        | F.col("page").isNull()
        | F.col("event_ts").isNull()
    ).withColumn("reject_reason", F.lit("schema_or_null_violation")).withColumn(
        "rejected_at", F.current_timestamp()
    )

    cleaned_out = valid.select(
        F.col("session_id").alias("key"),
        F.to_json(
            F.struct(
                "event_id",
                "event_type",
                "event_ts",
                "user_id",
                "session_id",
                "page",
                "referrer",
                "device",
                "country",
                "utm_source",
                "latency_ms",
                "amount",
                "cleaned_at",
            )
        ).alias("value"),
    )

    (
        cleaned_out.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", CLEANED_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/clean_to_kafka")
        .outputMode("append")
        .start()
    )

    (
        valid.writeStream.format("delta")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/clean_to_delta")
        .outputMode("append")
        .start(f"{DELTA_PATH}/silver/events_cleaned")
    )

    dlq_out = invalid.select(
        F.lit("dlq").alias("key"),
        F.to_json(
            F.struct(
                "raw_json",
                "event_id",
                "event_type",
                "user_id",
                "reject_reason",
                "rejected_at",
            )
        ).alias("value"),
    )

    (
        dlq_out.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", DLQ_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/clean_dlq")
        .outputMode("append")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
