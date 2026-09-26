"""Kafka producer that replays Sentiment140 tweets into a Kafka topic.

Each historical tweet is read from the CSV on disk incrementally (row by row,
no full dataset in memory) and published as a JSON message that simulates a
continuously arriving social-media message.

Message structure:
    {
      "tweet_id": "...",
      "date": "...",                 # original Sentiment140 date
      "user": "...",
      "text": "...",
      "actual_sentiment": 0,         # 0 = Negative, 1 = Positive
      "ingestion_timestamp": "...",  # ISO-8601 timestamp of publish time
      "ingestion_ts_ms": 123456789   # epoch milliseconds for windowing
    }

Usage:
    python src/kafka_producer.py --messages-per-second 10
    python src/kafka_producer.py --max-messages 1000 --messages-per-second 50
    python src/kafka_producer.py --repeat 0 --messages-per-second 10  # stream forever
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

import config
import dataset_utils

logger = logging.getLogger("sentiment.producer")

LOG_EVERY = 100


def build_message(
    target: str, tweet_id: str, date: str, user: str, text: str
) -> dict | None:
    """Build a Kafka JSON payload from a Sentiment140 CSV row.

    Returns None when the label cannot be converted so the row is skipped.
    """
    label = dataset_utils.target_to_label(target)
    if label is None:
        return None

    now = datetime.now(timezone.utc)
    return {
        "tweet_id": tweet_id,
        "date": date,
        "user": user,
        "text": text,
        "actual_sentiment": label,
        "ingestion_timestamp": now.isoformat(),
        "ingestion_ts_ms": int(now.timestamp() * 1000),
    }


def message_to_bytes(message: dict) -> bytes:
    """Serialize a message dict to the JSON bytes published to Kafka."""
    return json.dumps(message, ensure_ascii=False).encode("utf-8")


def check_kafka_available(bootstrap_servers: str) -> None:
    """Fail fast with a helpful message when Kafka is not reachable."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    try:
        metadata = admin.list_topics(timeout=5)
        _ = metadata
    except Exception as exc:  # noqa: BLE001 - report any connect failure clearly
        logger.error(
            "Kafka is not reachable at '%s'. "
            "Start it with `docker compose up -d` and wait until the container "
            "reports .. (kafka) started. Error: %s",
            bootstrap_servers,
            exc,
        )
        sys.exit(1)


def run(
    dataset_path: str | Path,
    bootstrap_servers: str,
    topic: str,
    messages_per_second: int,
    max_messages: int | None,
    repeat: int = 1,
) -> None:
    check_kafka_available(bootstrap_servers)
    dataset_utils.check_dataset(dataset_path)

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    delivered = {"count": 0}
    failures = {"count": 0}

    def on_delivery(err, msg):
        if err is not None:
            failures["count"] += 1
            logger.error("Delivery failed for message #%s: %s", msg.key(), err)
        else:
            delivered["count"] += 1

    logger.info(
        "Connected to Kafka at %s, publishing to topic '%s' "
        "(rate=%d msg/s, max_messages=%s, repeat=%s)",
        bootstrap_servers,
        topic,
        messages_per_second,
        max_messages if max_messages else "until end of dataset",
        repeat if repeat else "forever",
    )

    # Incremental streaming read - never loads the whole dataset into memory.
    start_wall = time.monotonic()
    published = 0
    interval = 1.0 / max(1, messages_per_second)

    pass_number = 0
    while True:
        if max_messages is not None and published >= max_messages:
            break
        if repeat != 0 and pass_number >= repeat:
            break
        pass_number += 1

        with open(dataset_path, "r", encoding="latin-1", newline="") as f:
            reader = csv.reader(f, quotechar='"', escapechar=None)
            for row in reader:
                if not row or len(row) < 6:
                    continue  # skip malformed lines
                target, tweet_id, date, query, user, text = row[0], row[1], row[2], row[3], row[4], row[5]

                message = build_message(target, tweet_id, date, user, text)
                if message is None or not message["text"].strip():
                    continue

                payload = message_to_bytes(message)
                producer.produce(
                    topic,
                    value=payload,
                    key=str(message["tweet_id"]),
                    callback=on_delivery,
                )
                published += 1
                if published % LOG_EVERY == 0:
                    logger.info("Published message #%d", published)

                # rate limiting
                producer.poll(0)
                target_time = start_wall + published * interval
                sleep_for = target_time - time.monotonic()
                if sleep_for > 0:
                    time.sleep(sleep_for)

                if max_messages is not None and published >= max_messages:
                    break

    producer.flush(30)

    if failures["count"]:
        logger.warning(
            "Finished with %d failed deliveries, %d successful.",
            failures["count"],
            delivered["count"],
        )
    else:
        logger.info(
            "Finished publishing %s messages to topic '%s'.", published, topic
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka producer for Sentiment140")
    parser.add_argument("--dataset", default=str(config.DATASET_PATH))
    parser.add_argument("--bootstrap-servers", default=config.KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=config.KAFKA_TOPIC)
    parser.add_argument(
        "--messages-per-second",
        type=int,
        default=config.DEFAULT_MESSAGES_PER_SECOND,
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Stop after this many messages (default: publish the whole dataset)",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="How many times to replay the dataset (default: 1, use 0 = loop forever)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config.configure_logging()
    run(
        dataset_path=args.dataset,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        messages_per_second=args.messages_per_second,
        max_messages=args.max_messages,
        repeat=args.repeat,
    )


if __name__ == "__main__":
    main()