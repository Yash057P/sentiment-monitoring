"""Train the Spark MLlib sentiment model and evaluate it.

Pipeline:
    RegexTokenizer -> StopWordsRemover -> HashingTF -> IDF -> LogisticRegression

Builds the complete PipelineModel, evaluates it on a held-out test set and saves
both the fitted model (models/sentiment_pipeline) and the metrics
(artifacts/model_metrics.json).

Usage:
    python src/train_model.py                 # train on the full dataset
    python src/train_model.py --limit 50000   # quick smoke training
"""

from __future__ import annotations

import argparse
import json
import logging
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StopWordsRemover
from pyspark.sql import functions as F

import config
import dataset_utils
from spark_utils import build_spark_session, stop_spark_session

logger = logging.getLogger("sentiment.train")

NUM_FEATURES = 2 ** 18  # HashingTF buckets -> 262,144 sparse features
LR_MAX_ITERATIONS = 50
LR_REG_PARAM = 0.1


def build_pipeline() -> Pipeline:
    """Create the ML pipeline used for training AND scoring."""
    tokenizer = RegexTokenizer(
        inputCol="clean_text",
        outputCol="words",
        pattern=r"\s+",
        gaps=True,
        toLowercase=False,
        minTokenLength=1,
    )
    remover = StopWordsRemover(
        inputCol="words", outputCol="filtered_words", caseSensitive=False
    )
    hashing = HashingTF(
        inputCol="filtered_words", outputCol="raw_features", numFeatures=NUM_FEATURES, binary=True
    )
    idf = IDF(inputCol="raw_features", outputCol="features")
    lr = LogisticRegression(
        labelCol="label",
        featuresCol="features",
        maxIter=LR_MAX_ITERATIONS,
        regParam=LR_REG_PARAM,
        standardization=True,
        threshold=0.5,
    )
    return Pipeline(stages=[tokenizer, remover, hashing, idf, lr])


def evaluate_model(model, test_df: "DataFrame") -> dict:
    """Compute accuracy / precision / recall / F1 / AUC / confusion matrix."""
    predictions = model.transform(test_df)

    counts = (
        predictions.select(
            F.sum(F.when(F.col("label") == 1, 1).otherwise(0)).alias("tp_total"),
            F.sum(F.when(F.col("label") == 0, 1).otherwise(0)).alias("total_actual_pos"),
        )
        .collect()[0]
    )
    total = predictions.count()
    if total == 0:
        raise ValueError("Test set is empty - cannot evaluate.")

    # Confusion matrix elements from (label, prediction) counts.
    cm = (
        predictions.groupBy("label", "prediction")
        .agg(F.count(F.lit(1)).alias("n"))
        .collect()
    )
    cell = {(int(r["label"]), int(r["prediction"])): int(r["n"]) for r in cm}
    tp = cell.get((1, 1.0), cell.get((1, 1), 0))
    fp = cell.get((0, 1.0), cell.get((0, 1), 0))
    fn = cell.get((1, 0.0), cell.get((1, 0), 0))
    tn = cell.get((0, 0.0), cell.get((0, 0), 0))

    correct = tp + tn
    accuracy = correct / total
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall)
        else 0.0
    )

    evaluator = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction"
    )
    auc = evaluator.evaluate(predictions)

    metrics = {
        "accuracy": round(float(accuracy), 4),
        "precision": round(float(precision), 4),
        "recall": round(float(recall), 4),
        "f1_score": round(float(f1), 4),
        "area_under_roc": round(float(auc), 4),
        "test_rows": int(total),
        "confusion_matrix": {
            "true_negative": int(tn),
            "false_positive": int(fp),
            "false_negative": int(fn),
            "true_positive": int(tp),
        },
    }

    logger.info("--- Test set evaluation (%s rows) ---", f"{total:,}")
    logger.info("Confusion matrix (rows=actual, cols=predicted):")
    logger.info("            pred=0   pred=1")
    logger.info("actual=0    %7d  %7d", tn, fp)
    logger.info("actual=1    %7d  %7d", fn, tp)
    logger.info("Accuracy=%.4f  Precision=%.4f  Recall=%.4f  F1=%.4f  AUC=%.4f",
                metrics["accuracy"], metrics["precision"],
                metrics["recall"], metrics["f1_score"], metrics["area_under_roc"])
    return metrics


def save_metrics(metrics: dict) -> None:
    config.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(config.METRICS_PATH, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    logger.info("Evaluation metrics saved to %s", config.METRICS_PATH)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train Spark MLlib sentiment model")
    parser.add_argument(
        "--dataset",
        default=str(config.DATASET_PATH),
        help="Path to Sentiment140 CSV",
    )
    parser.add_argument("--model-dir", default=str(config.MODEL_DIR))
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional: train only on the first N rows (quick smoke training)",
    )
    parser.add_argument(
        "--test-fraction",
        type=float,
        default=config.EVALUATION_TEST_FRACTION,
        help="Fraction of data held out for evaluation (default 0.2)",
    )
    parser.add_argument("--seed", type=int, default=config.RANDOM_SEED)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config.configure_logging()
    spark = build_spark_session(config.SPARK_APP_NAME_TRAIN, include_kafka_connector=False)
    try:
        start = time.time()

        data = dataset_utils.load_sentiment140(spark, args.dataset, limit=args.limit)
        dataset_utils.dataset_statistics(data)

        # Text cleaning is part of the preprocessing (before the pipeline).
        data = dataset_utils.clean_tweets(data, "text", "clean_text")

        logger.info("Splitting with seed=%s (test fraction=%.2f)...",
                    args.seed, args.test_fraction)
        train_df, test_df = data.randomSplit(
            [1.0 - args.test_fraction, args.test_fraction], seed=args.seed
        )

        logger.info("Training model...")
        pipeline = build_pipeline()
        model = pipeline.fit(train_df)
        logger.info("Training finished in %.1fs", time.time() - start)

        metrics = evaluate_model(model, test_df)
        metrics["trained_rows"] = int(train_df.count())
        metrics["model_dir"] = str(config.MODEL_DIR)
        save_metrics(metrics)

        model.write().overwrite().save(str(config.MODEL_DIR))
        logger.info("PipelineModel saved to %s", config.MODEL_DIR)

        logger.info("Done in %.1fs", time.time() - start)
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()