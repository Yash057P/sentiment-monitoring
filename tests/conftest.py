"""Shared pytest fixtures. Puts src/ on sys.path and provides a tiny SparkSession."""

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))
sys.path.insert(0, str(ROOT))

# Prefer the portable JRE bundled in .venv so tests run without a system JDK.
os.environ.setdefault("JAVA_HOME", str(ROOT / ".venv" / "jvm17"))
os.environ.setdefault("HADOOP_HOME", str(ROOT / ".venv" / "hadoop_home"))
os.environ.setdefault(
    "PYSPARK_PYTHON", str(ROOT / ".venv" / "Scripts" / "python.exe")
)
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


@pytest.fixture(scope="session")
def spark():
    """Minimal shared SparkSession for tests (no Kafka connector needed)."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("sentiment-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()