"""Tests for Kafka message building + JSON serialization."""

import json

import kafka_producer


def test_build_message_valid_row():
    msg = kafka_producer.build_message("4", "1", "Mon Apr 06 22:19:45 PDT 2009", "user1", "I love it")
    assert msg["tweet_id"] == "1"
    assert msg["user"] == "user1"
    assert msg["text"] == "I love it"
    assert msg["actual_sentiment"] == 1  # 4 -> Positive
    assert msg["ingestion_ts_ms"] > 0
    assert msg["ingestion_timestamp"]


def test_build_message_negative_label():
    msg = kafka_producer.build_message("0", "2", "d", "u", "bad day")
    assert msg["actual_sentiment"] == 0  # 0 -> Negative


def test_build_message_skips_unknown_target():
    assert kafka_producer.build_message("2", "3", "d", "u", "text") is None


def test_message_to_bytes_is_serializable_json():
    msg = kafka_producer.build_message("4", "7", "d", "u", "yay 😀")
    raw = kafka_producer.message_to_bytes(msg)
    parsed = json.loads(raw.decode("utf-8"))
    assert parsed == msg


def test_serialized_message_round_trips_through_dict():
    msg = kafka_producer.build_message("0", "9", "d", "u", "who would do this")
    raw = kafka_producer.message_to_bytes(msg)
    assert json.loads(raw)["actual_sentiment"] == 0