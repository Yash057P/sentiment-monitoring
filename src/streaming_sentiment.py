"""Spark Structured Streaming application.

Subscribes to the Kafka topic, deserializes JSON messages, cleans the tweet
text, applies the SAVED Spark ML PipelineModel to classify sentiment, and:

1. writes every prediction to output/predictions (Parquet)
2. computes a 1-minute windowed sentiment trend (total / positive / negative /
   percentages) using streaming aggregation with watermarking and writes it to
   output/trends (Parquet)

The model is loaded from models/sentiment_pipeline and never retrained here.

Usage:
    python src/streaming_sentiment.py
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

import config
import dataset_utils
from spark_utils import build_spark_session, stop_spark_session

logger = logging.getLogger("sentiment.streaming")

MESSAGE_SCHEMA = StructType(
    [
        StructField("tweet_id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("user", StringType(), True),
        StructField("text", StringType(), True),
        StructField("actual_sentiment", LongType(), True),
        StructField("ingestion_timestamp", StringType(), True),
        StructField("ingestion_ts_ms", LongType(), True),
    ]
)


def check_model_exists() -> None:
    import os

    model_path = str(config.MODEL_DIR)
    if not os.path.exists(model_path):
        raise FileNotFoundError(
            f"The saved model was not found at {model_path}.\n"
            "Train it first with:  python src/train_model.py\n"
            "(use `--limit 50000` for a quick smoke run)."
        )


def load_model() -> PipelineModel:
    check_model_exists()
    model = PipelineModel.load(str(config.MODEL_DIR))
    logger.info("Loaded PipelineModel from %s", config.MODEL_DIR)
    return model


def read_from_kafka(spark) -> DataFrame:
    """Read a stream of raw Kafka records."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", config.STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_kafka_messages(raw: DataFrame) -> DataFrame:
    """Deserialize JSON payloads; rows with malformed JSON / null text are dropped."""
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("msg")
    ).select("msg.*")

    valid = parsed.filter(
        F.col("text").isNotNull()
        & (F.length(F.trim(F.col("text"))) > 0)
        & F.col("tweet_id").isNotNull()
    )
    return valid


def add_predictions(clean: DataFrame, model: PipelineModel) -> DataFrame:
    """Apply the ML pipeline and add labeled, human-readable sentiment."""
    predicted = model.transform(clean)
    result = predicted.select(
        F.col("tweet_id"),
        F.col("date"),
        F.col("user"),
        F.col("text"),
        F.col("actual_sentiment"),
        F.col("prediction"),
        F.when(F.col("prediction") >= dataset_utils.PREDICTION_THRESHOLD, "Positive")
        .otherwise("Negative")
        .alias("predicted_sentiment"),
        F.col("ingestion_timestamp"),
        F.col("ingestion_ts_ms"),
        F.current_timestamp().alias("processing_time"),
    )
    return result


def write_predictions_batch(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch sink: log progress and append predictions to Parquet."""
    count = batch_df.count()
    if count == 0:
        logger.info("Processed batch %d: 0 rows (empty)", batch_id)
        return
    logger.info("Processed batch %d: %d rows", batch_id, count)
    batch_df.write.mode("append").parquet(str(config.PREDICTIONS_DIR))


def build_trends(predictions: DataFrame) -> DataFrame:
    """Windowed sentiment aggregation (1-minute windows, watermark-based)."""
    with_event_time = predictions.withColumn(
        "event_time", F.timestamp_millis(F.col("ingestion_ts_ms"))
    )
    trends = (
        with_event_time.withWatermark("event_time", config.WATERMARK_DELAY)
        .groupBy(F.window(F.col("event_time"), config.WINDOW_DURATION))
        .agg(
            F.count(F.lit(1)).alias("total_messages"),
            F.sum(
                F.when(F.col("predicted_sentiment") == "Positive", 1).otherwise(0)
            ).alias("positive_messages"),
            F.sum(
                F.when(F.col("predicted_sentiment") == "Negative", 1).otherwise(0)
            ).alias("negative_messages"),
        )
        .withColumn(
            "positive_pct",
            F.round(F.col("positive_messages") / F.col("total_messages") * 100, 2),
        )
        .withColumn(
            "negative_pct",
            F.round(F.col("negative_messages") / F.col("total_messages") * 100, 2),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_messages",
            "positive_messages",
            "negative_messages",
            "positive_pct",
            "negative_pct",
        )
    )
    return trends


def ensure_kafka_topic(bootstrap_servers: str, topic: str) -> None:
    """Create the topic up-front (idempotent) via AdminClient.

    A Structured Streaming query fails at startup if the subscribed topic does
    not exist yet, so create it before the queries start. Requires Kafka to be
    reachable; raises a clear error otherwise.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    try:
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        metadata = admin.list_topics(timeout=10)
    except Exception as exc:
        raise ConnectionError(
            f"Cannot reach Kafka at {bootstrap_servers} to prepare topic "
            f"'{topic}'. Start the broker first:  docker compose up -d"
        ) from exc

    if topic in metadata.topics:
        logger.info(
            "Kafka topic '%s' already exists (%s)",
            topic,
            metadata.topics[topic].partitions,
        )
        return

    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)]
    )
    for result in futures.values():
        try:
            result.result(10)
            logger.info("Created Kafka topic '%s' (1 partition, replication=1)", topic)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                logger.info("Kafka topic '%s' already exists", topic)
                return
            raise ConnectionError(f"Failed to create Kafka topic '{topic}': {exc}") from exc


def run(args: argparse.Namespace) -> None:
    config.configure_logging()
    spark = build_spark_session(
        config.SPARK_APP_NAME_STREAM, include_kafka_connector=True
    )
    try:
        model = load_model()
        ensure_kafka_topic(config.KAFKA_BOOTSTRAP_SERVERS, config.KAFKA_TOPIC)

        raw = read_from_kafka(spark)
        parsed = parse_kafka_messages(raw)
        cleaned = dataset_utils.clean_tweets(parsed, "text", "clean_text")
        predictions = add_predictions(cleaned, model)

        # ---- Sink 1: individual predictions (Parquet, batch logging) ----
        query_predictions = (
            predictions.writeStream.foreachBatch(write_predictions_batch)
            .outputMode("append")
            .option("checkpointLocation", str(config.CHECKPOINT_PREDICTIONS_DIR))
            .queryName("prediction-sink")
            .start()
        )

        # ---- Sink 2: windowed trends (Parquet) ----
        trends = build_trends(predictions)
        query_trends = (
            trends.writeStream.format("parquet")
            .outputMode("append")
            .option("path", str(config.TRENDS_DIR))
            .option("checkpointLocation", str(config.CHECKPOINT_TRENDS_DIR))
            .queryName("trend-sink")
            .start()
        )

        logger.info(
            "Streaming queries started: predictions -> %s, trends -> %s. "
            "Waiting for data from topic '%s'...",
            config.PREDICTIONS_DIR,
            config.TRENDS_DIR,
            config.KAFKA_TOPIC,
        )
        spark.streams.awaitAnyTermination()
    finally:
        stop_spark_session(spark)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming sentiment")
    parser.add_argument("--topic", default=config.KAFKA_TOPIC)
    parser.add_argument("--bootstrap-servers", default=config.KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--model-dir", default=str(config.MODEL_DIR))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    # allow CLI overrides
    config.KAFKA_TOPIC = args.topic
    config.KAFKA_BOOTSTRAP_SERVERS = args.bootstrap_servers
    config.MODEL_DIR = Path(args.model_dir)
    run(args)


if __name__ == "__main__":
    main()