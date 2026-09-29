"""Live analytics service for the Flask backend.

Reads the parquet predictions the Spark streaming app keeps writing into
``output/predictions`` (incrementally - only NEW files are loaded on each scan),
and computes everything the web app needs:

  * platform-wide totals and per-company KPIs
  * a time series of sentiment for charts
  * the positive/negative tweets per company (with the product keywords that
    drove each negative tweet - the "impact")
  * an outlook classification (Good / Warning / Bad) plus a short forecast of
    the positive percentage for the next window.

Pure python/pandas - no Spark here, the heavy lifting is the streaming side.
"""

from __future__ import annotations

import datetime as dt
import glob
import hashlib
import json
import logging
import os
from pathlib import Path

import pandas as pd

import companies

logger = logging.getLogger("sentiment.analytics")

PREDICTIONS_DIR = Path(__file__).resolve().parents[1] / "output" / "predictions"
MAX_CACHED_ROWS = 50_000
TIMELINE_BUCKET_SECONDS = 30
FORECAST_WINDOWS = 12
STATUS_GOOD = "good"
STATUS_WARNING = "warning"
STATUS_BAD = "bad"


class LiveStore:
    """Incremental parquet reader + in-memory row cache."""

    def __init__(self, predictions_dir: str | Path = PREDICTIONS_DIR,
                 max_rows: int = MAX_CACHED_ROWS) -> None:
        self.predictions_dir = Path(predictions_dir)
        self.max_rows = max_rows
        self._seen: set[str] = set()
        self._rows: list[dict] = []
        # Robust schema (older files may lack new company columns).
        self._columns = (
            "tweet_id", "text", "user", "date", "actual_sentiment",
            "predicted_sentiment", "company", "company_name", "sector",
            "source", "ingestion_ts_ms", "processing_time",
        )

    def refresh(self, force: bool = False) -> int:
        """Load any new parquet files; returns how many new rows were added."""
        if not self.predictions_dir.exists():
            return 0
        files = sorted(glob.glob(str(self.predictions_dir / "*.parquet")))
        new_files = [f for f in files if f not in self._seen]
        added = 0
        for path in new_files:
            self._seen.add(path)
            try:
                df = pd.read_parquet(path, engine="pyarrow")
            except Exception as exc:  # partial/corrupt file - skip
                logger.warning("Skipping unreadable parquet %s: %s", path, exc)
                continue
            if df.empty:
                continue
            for col in self._columns:
                if col not in df.columns:
                    df[col] = None
            df = df[list(self._columns)].where(pd.notnull(df[list(self._columns)]), None)
            self._rows.extend(df.to_dict(orient="records"))
            added += len(df)
        if self._rows:
            self._rows.sort(
                key=lambda r: r["ingestion_ts_ms"] or 0, reverse=True
            )
            if len(self._rows) > self.max_rows:
                self._rows = self._rows[: self.max_rows]
        return added

    def rows(self) -> list[dict]:
        self.refresh()
        return self._rows

    def reset(self, wipe_files: bool = True) -> dict:
        """Start counting again from zero.

        Clears the in-memory cache. With ``wipe_files`` the parquet files that
        were already produced are deleted too, so only tweets streamed from now
        on are counted. Any file that is left on disk is marked as already seen
        so it is not re-counted.
        """
        removed = 0
        if wipe_files and self.predictions_dir.exists():
            for path in self.predictions_dir.glob("*.parquet"):
                try:
                    path.unlink()
                    removed += 1
                except OSError:
                    pass
        self._rows.clear()
        self._seen = {
            str(p) for p in self.predictions_dir.glob("*.parquet")
        } if self.predictions_dir.exists() else set()
        logger.info("Live store reset (wipe_files=%s, removed=%d)", wipe_files, removed)
        return {"rows": 0, "removed_files": removed}

    def companies_df(self) -> pd.DataFrame:
        """Rows that carry a company attribution, as a DataFrame."""
        return pd.DataFrame(self.rows())


# ---------------------------------------------------------------------------
# Pure aggregation helpers (no state) -----------------------------------------
# ---------------------------------------------------------------------------

def _bucket(windows: list[dict]) -> list[dict]:
    out: list[dict] = []
    for i, curr in enumerate(windows):
        if i == 0:
            chg = None
        else:
            prev = out[-1]
            chg = round(curr["positive_pct"] - prev["positive_pct"], 2)
        out.append({
            "t": curr["t"],
            "total": curr["total"],
            "positive": curr["positive"],
            "negative": curr["negative"],
            "positive_pct": curr["positive_pct"],
            "change": chg,
        })
    return out


def sentiment_timeline(rows: list[dict], bucket_seconds: int = TIMELINE_BUCKET_SECONDS) -> list[dict]:
    """Bucketed positive percentage over time (for charts)."""
    buckets: dict[int, dict] = {}
    for row in rows:
        ms = row.get("ingestion_ts_ms")
        if not ms:
            continue
        bucket = int(int(ms) // (bucket_seconds * 1000))
        b = buckets.setdefault(bucket, {"total": 0, "positive": 0, "negative": 0})
        b["total"] += 1
        if row.get("predicted_sentiment") == "Positive":
            b["positive"] += 1
        else:
            b["negative"] += 1
    if not buckets:
        return []
    start_ms = min(buckets.keys()) * bucket_seconds * 1000
    windows: list[dict] = []
    for key in sorted(buckets):
        b = buckets[key]
        windows.append({
            "t": (start_ms + (key - min(buckets.keys())) * bucket_seconds * 1000),
            "total": b["total"],
            "positive": b["positive"],
            "negative": b["negative"],
            "positive_pct": round(100.0 * b["positive"] / b["total"], 2) if b["total"] else 0.0,
        })
    return _bucket(windows)


def forecast(windows: list[dict]) -> tuple[str, float, float] | None:
    """Return (status, projected_positive_pct, slope) from recent windows.

    Linear least-squares fit on positive_pct over time. Positive slope towards
    a healthy level = Good, negative = Bad, flat = Warning.
    """
    if len(windows) < 3:
        return None
    recent = windows[-FORECAST_WINDOWS:]
    xs = list(range(len(recent)))
    ys = [w["positive_pct"] for w in recent]
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den = sum((x - mean_x) ** 2 for x in xs)
    slope = num / den if den else 0.0
    last = ys[-1]
    projected = round(max(0.0, min(100.0, last + slope)), 2)
    if slope > 0.75 and projected >= 50:
        status = STATUS_GOOD
    elif slope < -0.75 or projected < 45:
        status = STATUS_BAD
    else:
        status = STATUS_WARNING
    return status, projected, round(slope, 3)


def impact_tags(rows: list[dict], company: companies.Company) -> list[dict]:
    """Which product keywords caused NEGATIVE tweets (the 'impact' view)."""
    counts: dict[str, int] = {}
    for row in rows:
        if row.get("predicted_sentiment") != "Negative":
            continue
        for kw in company.keywords:
            if companies.keyword_hits(row.get("text") or "", company) and any(
                w == kw or (len(kw) >= 4 and w.startswith(kw))
                for w in companies.tokens(row.get("text") or "")
            ):
                counts[kw] = counts.get(kw, 0) + 1
    return [
        {"keyword": kw, "count": c}
        for kw, c in sorted(counts.items(), key=lambda kv: -kv[1])
    ][:8]


# ---------------------------------------------------------------------------
# Company-level stats ----------------------------------------------------------
# ---------------------------------------------------------------------------

def company_stats(company: companies.Company, rows: pd.DataFrame) -> dict:
    mine = rows[rows["company"] == company.handle] if len(rows) else rows
    total = int(len(mine))
    positive = int((mine["predicted_sentiment"] == "Positive").sum())
    negative = total - positive
    pos_pct = round(100.0 * positive / total, 2) if total else 0.0
    timeline = sentiment_timeline(mine.to_dict(orient="records"))
    fc = forecast(timeline)
    return {
        "handle": company.handle,
        "name": company.name,
        "sector": company.sector,
        "logo": company.logo,
        "color": company.color,
        "tagline": company.tagline,
        "description": company.description,
        "tweets": total,
        "positive": positive,
        "negative": negative,
        "positive_pct": pos_pct,
        "negative_pct": round(100.0 - pos_pct, 2),
        "timeline": timeline,
        "outlook": fc[0] if fc else STATUS_WARNING,
        "forecast": fc[1] if fc else None,
        "slope": fc[2] if fc else None,
    }


def admin_overview() -> dict:
    df = store.companies_df()
    companies_stats = [company_stats(c, df) for c in companies.COMPANIES]
    total = int(len(df))
    positive = int((df["predicted_sentiment"] == "Positive").sum())
    overall_timeline = sentiment_timeline(df.to_dict(orient="records"))
    fc = forecast(overall_timeline)
    return {
        "generatedAt": dt.datetime.now(dt.timezone.utc).isoformat(),
        "totalTweets": total,
        "positive": positive,
        "negative": total - positive,
        "positivePct": round(100.0 * positive / total, 2) if total else 0.0,
        "outlook": fc[0] if fc else STATUS_WARNING,
        "forecast": fc[1] if fc else None,
        "timeline": overall_timeline,
        "companies": companies_stats,
        "generalTweets": int((df["company"].isnull() | (df["company"].astype(str) == "None")).sum()),
    }


def company_overview(handle: str) -> dict:
    company = companies.COMPANIES_BY_HANDLE.get(handle)
    if company is None:
        raise KeyError(handle)
    df = store.companies_df()
    mine = df[df["company"] == handle] if len(df) else df
    rows = mine.to_dict(orient="records")
    stats = company_stats(company, df)
    impacts = impact_tags(rows, company)
    details = {
        "positiveTweets": sorted(
            [r for r in rows if r.get("predicted_sentiment") == "Positive"],
            key=lambda r: r.get("ingestion_ts_ms") or 0, reverse=True,
        )[:100],
        "negativeTweets": sorted(
            [r for r in rows if r.get("predicted_sentiment") == "Negative"],
            key=lambda r: r.get("ingestion_ts_ms") or 0, reverse=True,
        )[:100],
    }
    stats["impacts"] = impacts
    stats.update(details)
    return stats


def tweet_rows(handle: str, sentiment: str, limit: int = 100) -> list[dict]:
    df = store.companies_df()
    mine = df[df["company"] == handle] if len(df) else df
    if sentiment == "positive":
        mine = mine[mine["predicted_sentiment"] == "Positive"]
    elif sentiment == "negative":
        mine = mine[mine["predicted_sentiment"] == "Negative"]
    out = mine.to_dict(orient="records")
    out.sort(key=lambda r: r.get("ingestion_ts_ms") or 0, reverse=True)
    return out[:limit]


store = LiveStore()