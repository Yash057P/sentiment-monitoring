"""Tests for Sentiment140 CSV loading and label conversion with Spark."""

import csv

from dataset_utils import load_sentiment140, dataset_statistics

SAMPLE_ROWS = [
    ["0", "1", "Mon Apr 06 22:19:45 PDT 2009", "NO_QUERY", "_TheSpecialOne_", "@switchfoot http://twitpic.com/2y1zl - Awww, that's a bummer.  :("],
    ["4", "2", "Mon Apr 06 22:19:49 PDT 2009", "NO_QUERY", "_TheSpecialOne_", "is upset that he can't update his Facebook by texting it... and might cry as a result  School today also. Blah!"],
    ["4", "3", "Mon Apr 06 22:20:03 PDT 2009", "NO_QUERY", "darkop", "@Kenichan I dived many times for the ball. Managed to save 50%  The rest go out of bounds"],
    ["bad", "4", "some date", "NO_QUERY", "u", "row with invalid target"],
    ["", "", "", "", "", ""],
]


def test_load_sentiment140_converts_labels_and_skips_invalid(tmp_path, spark):
    csv_path = tmp_path / "tweets.csv"
    with open(csv_path, "w", encoding="latin-1", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(SAMPLE_ROWS)

    df = load_sentiment140(spark, str(csv_path))
    rows = df.select("id", "label", "text").orderBy("id").collect()

    # three valid rows, invalid target and empty row dropped
    assert len(rows) == 3
    assert [int(r["id"]) for r in rows] == [1, 2, 3]
    assert [int(r["label"]) for r in rows] == [0, 1, 1]
    assert all(r["text"].strip() for r in rows)


def test_load_sentiment140_missing_file_raises(tmp_path, spark):
    import pytest

    from dataset_utils import check_dataset

    with pytest.raises(FileNotFoundError):
        check_dataset(tmp_path / "nope.csv")


def test_dataset_statistics_reports_distribution(spark):
    df = spark.createDataFrame(
        [(0, "1"), (1, "2"), (1, "3")], ["label", "id"]
    )
    stats = dataset_statistics(df)
    assert stats["total_rows"] == 3
    assert stats["distribution"] == {0: 1, 1: 2}