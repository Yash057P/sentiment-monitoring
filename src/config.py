"""Centralized configuration for the Real-Time Social Media Sentiment Monitoring project.

Every important setting lives here. Most values can be overridden through
environment variables so the same project can run on a different machine or
cluster without code changes.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger("sentiment.config")

# ---------------------------------------------------------------------------
# Project layout
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
VENV_DIR = BASE_DIR / ".venv"

DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models"
ARTIFACTS_DIR = BASE_DIR / "artifacts"
OUTPUT_DIR = BASE_DIR / "output"
CHECKPOINT_DIR = BASE_DIR / "checkpoints"
JARS_DIR = BASE_DIR / "jars"
SCRIPTS_DIR = BASE_DIR / "scripts"

# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------
DATASET_PATH = Path(
    os.environ.get(
        "DATASET_PATH",
        str(DATA_DIR / "training.1600000.processed.noemoticon.csv"),
    )
)

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "social-media-stream")

# How many messages per second the producer tries to publish by default.
DEFAULT_MESSAGES_PER_SECOND = int(os.environ.get("DEFAULT_MESSAGES_PER_SECOND", "10"))

# ---------------------------------------------------------------------------
# Models / artifacts
# ---------------------------------------------------------------------------
MODEL_DIR = Path(
    os.environ.get("MODEL_DIR", str(MODELS_DIR / "sentiment_pipeline"))
)
METRICS_PATH = Path(
    os.environ.get("METRICS_PATH", str(ARTIFACTS_DIR / "model_metrics.json"))
)

# Reproducibility
RANDOM_SEED = int(os.environ.get("RANDOM_SEED", "42"))
EVALUATION_TEST_FRACTION = float(os.environ.get("TEST_FRACTION", "0.2"))

# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
SPARK_APP_NAME_TRAIN = os.environ.get("SPARK_APP_NAME_TRAIN", "SentimentModelTraining")
SPARK_APP_NAME_STREAM = os.environ.get("SPARK_APP_NAME_STREAM", "SentimentStreaming")

# Kafka connector for Structured Streaming, matched to the Spark version.
# Spark 4.x is built with Scala 2.13.
SPARK_KAFKA_PACKAGE = os.environ.get(
    "SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4"
)

# Local Spark wants a small number of shuffle partitions for good throughput.
SPARK_SHUFFLE_PARTITIONS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8")

# ---------------------------------------------------------------------------
# Streaming windows / output
# ---------------------------------------------------------------------------
WINDOW_DURATION = os.environ.get("WINDOW_DURATION", "1 minute")
SLIDE_DURATION = os.environ.get("SLIDE_DURATION", "1 minute")
WATERMARK_DELAY = os.environ.get("WATERMARK_DELAY", "2 minutes")

# 'earliest' for first run so nothing is missed, 'latest' to only see new data.
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "earliest")

PREDICTIONS_DIR = Path(
    os.environ.get("PREDICTIONS_DIR", str(OUTPUT_DIR / "predictions"))
)
TRENDS_DIR = Path(
    os.environ.get("TRENDS_DIR", str(OUTPUT_DIR / "trends"))
)
CHECKPOINT_PREDICTIONS_DIR = Path(
    os.environ.get("CHECKPOINT_PREDICTIONS_DIR", str(CHECKPOINT_DIR / "predictions"))
)
CHECKPOINT_TRENDS_DIR = Path(
    os.environ.get("CHECKPOINT_TRENDS_DIR", str(CHECKPOINT_DIR / "trends"))
)

# ---------------------------------------------------------------------------
# Windows local runtime (kept inside the venv so nothing touches the system)
# ---------------------------------------------------------------------------
JVM_DIR = Path(
    os.environ.get("JAVA_HOME", str(VENV_DIR / "jvm17"))
)
HADOOP_HOME = Path(
    os.environ.get("HADOOP_HOME", str(VENV_DIR / "hadoop_home"))
)


def ensure_java_and_hadoop_env() -> None:
    """Point JAVA_HOME / HADOOP_HOME at the portable runtimes bundled in .venv.

    Only applies when the appropriate env vars are not already set by the user.
    This must run before the SparkSession/JVM is created.
    """
    if not os.environ.get("JAVA_HOME"):
        if JVM_DIR.joinpath("bin", "java.exe").exists():
            os.environ["JAVA_HOME"] = str(JVM_DIR)
        else:
            logger.warning(
                "JAVA_HOME is not set and no portable JRE was found at %s. "
                "Install a JDK/JRE 17 and set JAVA_HOME manually.",
                JVM_DIR,
            )
    if not os.environ.get("HADOOP_HOME"):
        if HADOOP_HOME.joinpath("bin", "winutils.exe").exists():
            os.environ["HADOOP_HOME"] = str(HADOOP_HOME)
        else:
            logger.warning(
                "HADOOP_HOME is not set and no portable winutils was found at %s. "
                "On Windows, Spark needs winutils.exe (see README -> Windows setup).",
                HADOOP_HOME,
            )

    # Windows quirk: the Spark Python worker (spawned as a subprocess) must be
    # told exactly which interpreter to use and which loopback address to bind,
    # otherwise it can fail with "Python worker failed to connect back".
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


def kafka_connector_jars() -> str:
    """Comma separated path list for the Kafka connector jars (for spark.jars)."""
    if not JARS_DIR.exists():
        return ""
    paths = [str(p) for p in sorted(JARS_DIR.glob("*.jar"))]
    return ",".join(paths)


def ensure_directories() -> None:
    """Create every output / checkpoint directory the project writes to."""
    for target in (
        DATA_DIR,
        MODELS_DIR,
        ARTIFACTS_DIR,
        OUTPUT_DIR,
        CHECKPOINT_DIR,
        PREDICTIONS_DIR,
        TRENDS_DIR,
        CHECKPOINT_PREDICTIONS_DIR,
        CHECKPOINT_TRENDS_DIR,
    ):
        target.mkdir(parents=True, exist_ok=True)


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )