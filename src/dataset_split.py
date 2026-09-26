"""Split the Sentiment140 dataset into four deterministic parts.

Parts produced (CSV format identical to the original, no header):

    data/split/train.csv       -> Part 1: model training
    data/split/test.csv        -> Part 2: testing (accuracy / precision / recall)
    data/split/validation.csv  -> Part 3: validation (extra metrics check)
    data/split/simulation.csv  -> Part 4: live simulation feed (the demo ONLY
                                           streams tweets from this hold-out part)

Rows are assigned to a part deterministically with ``crc32(tweet_id) % 10000``
so re-running this script always produces the exact same parts. The split is a
true hold-out design: the wide model never sees the simulation tweets during
training, which is exactly what makes the live demo a fair test.

Usage:
    python src/dataset_split.py --dataset data/training.1600000.processed.noemoticon.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import config  # noqa: E402

logger = logging.getLogger("sentiment.split")

PARTS = ("train", "test", "validation", "simulation")

# Fraction of rows per part (must sum to ~1.0).
DEFAULT_RATIOS = (0.60, 0.15, 0.15, 0.10)

# crc32 range used for assignment; boundaries are ratios * 10000.
BUCKET_COUNT = 10_000

# Interleaving granularity. During streaming, each part is written to
# ``SHARDS`` shard files keyed by ``crc32(tweet_id) % SHARDS``; the shards are
# merged back in order at the end. This shuffles the rows inside every part so
# each part is representative from the very first rows (the raw Sentiment140
# file starts with a long block of negative tweets and keeps the source order
# otherwise).
SHARDS = 16


def _boundaries(ratios: tuple[float, ...]) -> list[int]:
    cumulative = 0.0
    bounds: list[int] = []
    for ratio in ratios:
        cumulative += ratio
        bounds.append(int(round(cumulative * BUCKET_COUNT)))
    bounds[-1] = BUCKET_COUNT  # last part takes the remainder
    return bounds


def _assign(tweet_id: str, bounds: list[int]) -> tuple[int, int]:
    """Return ``(part_index, shard_index)`` for a tweet id (deterministic)."""
    digest = zlib.crc32(tweet_id.encode("utf-8", errors="replace")) & 0xFFFFFFFF
    bucket = digest % BUCKET_COUNT
    for index, bound in enumerate(bounds):
        if bucket < bound:
            return index, digest % SHARDS
    return len(bounds) - 1, digest % SHARDS


def split_dataset(
    dataset_path: str | Path,
    output_dir: str | Path = "data/split",
    ratios: tuple[float, ...] = DEFAULT_RATIOS,
) -> dict[str, Path]:
    """Split the CSV into four parts and write them under output_dir.

    Returns a mapping ``{part_name: path}``. Existing part files are
    overwritten, so the function is idempotent.
    """
    dataset = Path(dataset_path)
    if not dataset.exists():
        raise FileNotFoundError(
            f"Dataset not found at: {dataset}\n"
            "Download Sentiment140 first (run_demo.py does this automatically)."
        )

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    bounds = _boundaries(ratios)

    # Phase 1: stream the source into per-part/per-shard files.
    shard_dir = out / "_shards"
    shard_dir.mkdir(parents=True, exist_ok=True)
    writers: dict[tuple[str, int], object] = {}
    counters = {name: 0 for name in PARTS}

    def shard_path(name: str, index: int) -> Path:
        return shard_dir / f"{name}_{index:02d}.csv"

    try:
        for name in PARTS:
            for index in range(SHARDS):
                path = shard_path(name, index)
                handle = open(path, "w", encoding="latin-1", newline="")
                writers[(name, index)] = (handle, csv.writer(
                    handle, quotechar='"', lineterminator="\n"))
        with open(dataset, "r", encoding="latin-1", newline="") as source:
            reader = csv.reader(source, quotechar='"', escapechar=None)
            for row in reader:
                if not row or len(row) < 6:
                    continue  # skip malformed lines
                tweet_id = row[1] if len(row) > 1 else row[0]
                part_index, shard = _assign(tweet_id, bounds)
                name = PARTS[part_index]
                writers[(name, shard)][1].writerow(row)
                counters[name] += 1
    finally:
        for handle, _writer in writers.values():
            handle.close()

    # Phase 2: merge each part's shards in order -> interleaved, representative file.
    # Each shard is sorted by its crc32 digest (pseudorandom row order), so the
    # head of every part contains a representative mix of both sentiment labels.
    files = {}
    try:
        for name in PARTS:
            final_path = out / f"{name}.csv"
            with open(final_path, "w", encoding="latin-1", newline="") as target:
                writer = csv.writer(target, quotechar='"', lineterminator="\n")
                for index in range(SHARDS):
                    shard_rows: list[list[str]] = []
                    with open(shard_path(name, index), "r",
                              encoding="latin-1", newline="") as shard:
                        for row in csv.reader(shard, quotechar='"'):
                            if row:
                                shard_rows.append(row)
                    shard_rows.sort(
                        key=lambda r: zlib.crc32(
                            str(r[1] if len(r) > 1 else r[0]).encode(
                                "utf-8", errors="replace"
                            )
                        )
                    )
                    for row in shard_rows:
                        writer.writerow(row)
            files[name] = final_path
    finally:
        import shutil
        shutil.rmtree(shard_dir, ignore_errors=True)

    total = sum(counters.values())
    for name in PARTS:
        logger.info(
            "%-10s %8d rows (%5.1f%%) -> %s",
            name, counters[name],
            100.0 * counters[name] / total if total else 0.0,
            files[name],
        )

    ratio_text = "/".join(str(round(r * 100)) for r in ratios)
    logger.info("Split %d rows into train/test/validation/simulation = %s",
                total, ratio_text)
    return files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split Sentiment140 into train/test/validation/simulation"
    )
    parser.add_argument("--dataset", default=str(config.DATASET_PATH))
    parser.add_argument("--output-dir", default=str(config.BASE_DIR / "data" / "split"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config.configure_logging()
    split_dataset(args.dataset, args.output_dir)


if __name__ == "__main__":
    main()