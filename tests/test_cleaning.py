"""Tests for Spark-native tweet cleaning."""

from pyspark.sql import functions as F

from dataset_utils import clean_tweets


def test_clean_tweets_removes_urls_mentions_punctuation(spark):
    df = spark.createDataFrame(
        [
            ("Check https://t.co/abc123 now!",),
            ("@elonmusk says hello!!!",),
            ("#breaking NEWS: wow...",),
        ],
        ["text"],
    )
    cleaned = clean_tweets(df)
    rows = cleaned.select(F.col("clean_text")).collect()
    texts = [r["clean_text"] for r in rows]

    assert texts[0] == "check now"
    assert texts[1] == "says hello"
    assert texts[2] == "news wow"


def test_clean_tweets_handles_html_entities_and_whitespace(spark):
    df = spark.createDataFrame([(" I &amp; you   love   it  ",)], ["text"])
    cleaned = clean_tweets(df)
    value = cleaned.select("clean_text").first()[0]
    assert value == "i you love it"


def test_clean_tweets_lowercases(spark):
    df = spark.createDataFrame([("LOVE This",)], ["text"])
    cleaned = clean_tweets(df)
    assert cleaned.select("clean_text").first()[0] == "love this"