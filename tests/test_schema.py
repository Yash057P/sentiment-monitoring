"""Tests for JSON message schema handling with Spark."""
from pyspark.sql import functions as F

from kafka_producer import build_message, message_to_bytes
from streaming_sentiment import MESSAGE_SCHEMA, parse_kafka_messages


def test_message_matches_schema(spark):
    """A producer message must parse cleanly with the streaming schema."""
    msg = build_message("4", "123", "some date", "user_x", "A wonderful day!")
    payload = message_to_bytes(msg)

    raw = spark.createDataFrame(
        [(payload.decode("utf-8"),)], ["value"]
    ).select(
        F.from_json(F.col("value"), MESSAGE_SCHEMA).alias("d"),
        F.col("value"),
    )

    parsed_row = raw.select("d").first()[0]
    assert parsed_row.tweet_id == "123"
    assert parsed_row.user == "user_x"
    assert parsed_row.text == "A wonderful day!"
    assert parsed_row.actual_sentiment == 1
    assert parsed_row.ingestion_ts_ms == msg["ingestion_ts_ms"]


def test_malformed_json_parses_to_blank_row(spark):
    """Garbage payloads produce an all-None row, filtered out downstream."""
    raw = spark.createDataFrame([("not json at all",)], ["value"]).select(
        F.from_json(F.col("value"), MESSAGE_SCHEMA).alias("d")
    )
    row = raw.select("d").first()[0]
    assert row is not None
    assert all(getattr(row, f.name) is None for f in MESSAGE_SCHEMA.fields)


def test_parse_kafka_messages_drops_invalid(spark):
    good = build_message("4", "1", "d", "u", "good text")
    raw = spark.createDataFrame(
        [
            (message_to_bytes(good),),
            (b"not-json",),
            (b"",),
        ],
        ["value"],
    )
    parsed = parse_kafka_messages(raw)
    rows = parsed.collect()
    assert len(rows) == 1
    assert rows[0]["tweet_id"] == "1"
    assert rows[0]["actual_sentiment"] == 1