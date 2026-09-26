"""Dataset helpers for Sentiment140.

Handles loading the raw CSV, converting the 0/4 labels to 0/1, reporting basic
statistics and applying tweet text cleaning that is Spark-native (SQL column
expressions, no Python UDF).
"""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

logger = logging.getLogger("sentiment.dataset")

# Sentiment140 has no header. Column order defined by the dataset.
COLUMN_NAMES = ["target", "id", "date", "query", "user", "text"]

RAW_CSV_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in COLUMN_NAMES]
)

# Original labels -> internal numeric labels (0=Negative, 1=Positive)
NEGATIVE_LABEL = 0
POSITIVE_LABEL = 1
TARGET_TO_LABEL = {"0": NEGATIVE_LABEL, "4": POSITIVE_LABEL}

POSITIVE_STR = "Positive"
NEGATIVE_STR = "Negative"
PREDICTION_THRESHOLD = 0.5


def target_to_label(target: str | int | None) -> int | None:
    """Convert a raw Sentiment140 target (string '0' or '4') to 0/1.

    Returns None for anything unexpected so the row can be skipped.
    """
    if target is None:
        return None
    return TARGET_TO_LABEL.get(str(target).strip())


def prediction_to_label(prediction: str | int | float | None) -> str:
    """Convert a model prediction score (probability of Positive) to a label.

    Mirrors the classifier threshold used by LogisticRegression (0.5).
    """
    if prediction is None:
        return NEGATIVE_STR
    value = float(prediction)
    return POSITIVE_STR if value >= PREDICTION_THRESHOLD else NEGATIVE_STR


#: (regex, replacement) pairs applied in order for cleaning tweet text.
CLEANING_RULES: list[tuple[str, str]] = [
    (r"(?i)https?://\S+|www\.\S+", " "),      # URLs
    (r"@\w+", " "),                            # mentions @user
    (r"#\w+", " "),                            # hashtags (optional)
    (r"&amp;|&lt;|&gt;|&quot;|&apos;|&#\d+;", " "),  # HTML entities
    (r"[^A-Za-z0-9\s]", " "),                  # punctuation / special chars
    (r"\s+", " "),                             # collapse whitespace
]


def check_dataset(path: str | Path) -> Path:
    """Raise a clear error if the Sentiment140 CSV is missing."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(
            f"Dataset not found at: {p}\n"
            "Download Sentiment140 from "
            "https://www.kaggle.com/datasets/kazanova/sentiment140 and place the "
            "file 'training.1600000.processed.noemoticon.csv' inside the data/ "
            "directory. See data/README.md for exact steps."
        )
    if p.stat().st_size == 0:
        raise ValueError(f"Dataset at {p} is empty (0 bytes).")
    return p


def load_sentiment140(
    spark: SparkSession,
    path: str | Path,
    limit: int | None = None,
) -> DataFrame:
    """Load Sentiment140, convert labels and retain usable fields.

    Returns a DataFrame with columns:
        target  (raw string '0'/'4')
        label   (0 -> Negative, 1 -> Positive)
        id, date, user, text
    """
    p = check_dataset(path)

    df = (
        spark.read.schema(RAW_CSV_SCHEMA)
        .option("header", "false")
        .option("encoding", "iso-8859-1")
        .option("quote", '"')
        .option("escape", '"')
        .csv(str(p))
    )

    cleaned = df.filter(
        F.col("text").isNotNull()
        & (F.length(F.trim(F.col("text"))) > 0)
        & F.col("id").isNotNull()
    ).select(
        F.col("target"),
        F.col("id"),
        F.col("date"),
        F.col("user"),
        F.trim(F.col("text")).alias("text"),
    )

    labeled = cleaned.withColumn(
        "label",
        F.when(F.col("target") == "0", NEGATIVE_LABEL)
        .when(F.col("target") == "4", POSITIVE_LABEL)
        .otherwise(F.lit(None).cast("int")),
    ).filter(F.col("label").isNotNull())

    if limit is not None:
        labeled = labeled.limit(int(limit))

    return labeled


def dataset_statistics(df: DataFrame) -> dict:
    """Print and return basic dataset statistics + class distribution."""
    total = df.count()
    logger.info("Dataset loaded: %s rows", f"{total:,}")

    distribution = (
        df.groupBy("label")
        .agg(F.count(F.lit(1)).alias("count"))
        .orderBy("label")
        .collect()
    )
    dist = {int(row["label"]): int(row["count"]) for row in distribution}
    for label in sorted(dist):
        name = "Positive" if label == POSITIVE_LABEL else "Negative"
        pct = 100.0 * dist[label] / total if total else 0.0
        logger.info("  %s (%d): %s rows (%.2f%%)", name, label, f"{dist[label]:,}", pct)

    return {"total_rows": total, "distribution": dist}


def clean_tweets(
    df: DataFrame,
    input_col: str = "text",
    output_col: str = "clean_text",
) -> DataFrame:
    """Clean tweet text using Spark column expressions (regexp_replace chain).

    Removes URLs, mentions, hashtags, HTML entities and punctuation, then
    collapses whitespace and lowercases. Spark-native, no Python UDFs.
    """
    expr = F.col(input_col)
    for pattern, replacement in CLEANING_RULES:
        expr = F.regexp_replace(expr, pattern, replacement)
    expr = F.lower(F.trim(expr))
    return df.withColumn(output_col, expr)