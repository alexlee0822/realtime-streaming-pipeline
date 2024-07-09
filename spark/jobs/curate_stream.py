#!/usr/bin/env python3
"""Kafka clickstream.cleaned → windowed metrics → curated topic + Delta gold + Postgres."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DELTA_PATH = os.getenv("DELTA_PATH", "/opt/delta")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/opt/checkpoints")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/clickstream")
POSTGRES_USER = os.getenv("POSTGRES_USER", "stream")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "stream")

CLEANED_TOPIC = "clickstream.cleaned"
CURATED_TOPIC = "clickstream.curated"

CLEANED_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("event_ts", T.StringType()),
        T.StructField("user_id", T.StringType()),
        T.StructField("session_id", T.StringType()),
        T.StructField("page", T.StringType()),
        T.StructField("referrer", T.StringType()),
        T.StructField("device", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("utm_source", T.StringType()),
        T.StructField("latency_ms", T.IntegerType()),
        T.StructField("amount", T.DoubleType()),
        T.StructField("cleaned_at", T.StringType()),
    ]
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("clickstream-curate")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def jdbc_opts() -> dict:
    return {
        "url": POSTGRES_URL,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }


def write_postgres(df: DataFrame, table: str, mode: str = "append") -> None:
    (
        df.write.format("jdbc")
        .options(**jdbc_opts())
        .option("dbtable", table)
        .mode(mode)
        .save()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    cleaned = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", CLEANED_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(F.from_json(F.col("value").cast("string"), CLEANED_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withWatermark("event_ts", "2 minutes")
    )

    # Persist individual cleaned events into Postgres for drill-down panels
    def foreach_events(batch_df: DataFrame, _batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        out = batch_df.select(
            F.col("event_id"),
            F.col("event_type"),
            F.col("event_ts"),
            F.col("user_id"),
            F.col("session_id"),
            F.col("page"),
            F.col("referrer"),
            F.col("device"),
            F.col("country"),
            F.col("utm_source"),
            F.col("latency_ms"),
            F.col("amount"),
        )
        write_postgres(out, "curated.events_cleaned")

    (
        cleaned.writeStream.foreachBatch(foreach_events)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/curate_events_pg")
        .outputMode("append")
        .start()
    )

    page_metrics = (
        cleaned.groupBy(
            F.window("event_ts", "1 minute"),
            F.col("page"),
            F.col("event_type"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("user_id").alias("unique_users"),
            F.approx_count_distinct("session_id").alias("unique_sessions"),
            F.avg("latency_ms").alias("avg_latency_ms"),
            F.sum("amount").alias("total_amount"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "page",
            "event_type",
            "event_count",
            "unique_users",
            "unique_sessions",
            "avg_latency_ms",
            F.coalesce(F.col("total_amount"), F.lit(0.0)).alias("total_amount"),
        )
    )

    def foreach_page_metrics(batch_df: DataFrame, _batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        write_postgres(batch_df, "curated.page_metrics_1m")
        batch_df.write.format("delta").mode("append").save(
            f"{DELTA_PATH}/gold/page_metrics_1m"
        )

    (
        page_metrics.writeStream.foreachBatch(foreach_page_metrics)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/curate_page_metrics")
        .outputMode("update")
        .start()
    )

    funnel = (
        cleaned.groupBy(F.window("event_ts", "1 minute"))
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias(
                "page_views"
            ),
            F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("clicks"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(
                "add_to_carts"
            ),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
                "purchases"
            ),
            F.sum(
                F.when(F.col("event_type") == "purchase", F.col("amount")).otherwise(0.0)
            ).alias("revenue"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "page_views",
            "clicks",
            "add_to_carts",
            "purchases",
            F.coalesce(F.col("revenue"), F.lit(0.0)).alias("revenue"),
        )
    )

    def foreach_funnel(batch_df: DataFrame, _batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        write_postgres(batch_df, "curated.funnel_1m")
        batch_df.write.format("delta").mode("append").save(f"{DELTA_PATH}/gold/funnel_1m")

        curated_kafka = batch_df.select(
            F.lit("funnel").alias("key"),
            F.to_json(F.struct(*batch_df.columns)).alias("value"),
        )
        (
            curated_kafka.write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", CURATED_TOPIC)
            .save()
        )

    (
        funnel.writeStream.foreachBatch(foreach_funnel)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/curate_funnel")
        .outputMode("update")
        .start()
    )

    device_country = (
        cleaned.groupBy(
            F.window("event_ts", "1 minute"),
            F.col("device"),
            F.col("country"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
                "purchases"
            ),
            F.sum(
                F.when(F.col("event_type") == "purchase", F.col("amount")).otherwise(0.0)
            ).alias("revenue"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "device",
            "country",
            "event_count",
            "purchases",
            F.coalesce(F.col("revenue"), F.lit(0.0)).alias("revenue"),
        )
    )

    def foreach_device_country(batch_df: DataFrame, _batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        write_postgres(batch_df, "curated.device_country_1m")
        batch_df.write.format("delta").mode("append").save(
            f"{DELTA_PATH}/gold/device_country_1m"
        )

    (
        device_country.writeStream.foreachBatch(foreach_device_country)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/curate_device_country")
        .outputMode("update")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
