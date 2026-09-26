"""Shared Spark helper: builds a SparkSession with the project's local settings."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession

import config

logger = logging.getLogger("sentiment.spark_utils")


def build_spark_session(
    app_name: str,
    master: str | None = None,
    include_kafka_connector: bool = False,
) -> SparkSession:
    """Create a SparkSession tuned for a local, single-machine run.

    Args:
        app_name: name shown for the application.
        master: Spark master URL (defaults to config.SPARK_MASTER).
        include_kafka_connector: when True the Kafka connector jars are added so
            structured streaming can talk to Kafka (needs jars/ populated).
    """
    spark_dir = Path(__file__).resolve().parents[1]
    _ = spark_dir  # kept for future use / clarity

    config.ensure_java_and_hadoop_env()
    config.ensure_directories()

    extra_java_opts = None
    if sys.platform.startswith("win"):
        java_lib = str(config.HADOOP_HOME / "bin").replace("\\", "/")
        extra_java_opts = f"-Djava.library.path={java_lib}"

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master or config.SPARK_MASTER)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )
    if extra_java_opts is not None:
        builder = (
            builder.config("spark.driver.extraJavaOptions", extra_java_opts)
            .config("spark.executor.extraJavaOptions", extra_java_opts)
        )

    if include_kafka_connector:
        jars = config.kafka_connector_jars()
        if jars:
            logger.info("Using Kafka connector jars: %s", jars.replace("\\", "\\\\"))
            builder.config("spark.jars", jars)
        else:
            logger.warning(
                "No Kafka connector jars found in %s. The streaming query will fail "
                "unless the jars are provided (see README -> Windows setup).",
                config.JARS_DIR,
            )

    spark = builder.getOrCreate()
    logger.info("Spark %s started for %s", spark.version, app_name)
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """Stop the session (used in tests / CLI exits)."""
    if spark is not None:
        spark.stop()