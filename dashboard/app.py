"""Streamlit dashboard for Real-Time Social Media Sentiment Monitoring.

Reads the Parquet output written by Spark Structured Streaming
(output/predictions, output/trends) and displays KPIs, charts and the latest
processed tweets. Auto-refreshes so new streaming results appear live.

Run:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Allow importing the shared config module.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import config  # noqa: E402

REFRESH_SECONDS = 5
MAX_LATEST_TWEETS = 10

# Cap in-memory rows so a long-running demo never grows unbounded.
MAX_CACHED_ROWS = 5_000


def _refresh_parquet_cache(state: dict, path: Path) -> pd.DataFrame:
    """Merge only the newly written Parquet files into the cached frame.

    Spark appends a new file per micro-batch, so during live streaming the
    directory grows by a few files every refresh. Re-reading ALL files each
    refresh gets slower forever; reading just the unseen ones keeps every
    refresh near-instant and the dashboard truly live.
    """
    files = sorted(path.glob("*.parquet"))
    new_files = [f for f in files if f.name not in state["seen"]]
    if not new_files:
        return state["df"] if state["df"] is not None else pd.DataFrame()

    frames = []
    for f in new_files:
        try:
            frames.append(pd.read_parquet(f))
        except Exception:  # noqa: BLE001 - skip a corrupt/partial file
            continue

    if frames:
        new_df = frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True)
        current = state["df"]
        if current is not None and not current.empty:
            current = pd.concat([current, new_df], ignore_index=True)
        else:
            current = new_df
        if len(current) > MAX_CACHED_ROWS:
            current = current.iloc[-MAX_CACHED_ROWS:]
        state["df"] = current

    state["seen"] |= {f.name for f in new_files}
    return state["df"] if state["df"] is not None else pd.DataFrame()


def read_parquet_dir(path: Path, key: str = "default") -> pd.DataFrame:
    """Read Parquet output, incrementally caching new files in the session."""
    state = st.session_state.setdefault(f"parquet_{key}", {"seen": set(), "df": None})
    if not path.exists():
        # directory was removed (fresh run) - forget previously seen files
        state["seen"] = set()
        state["df"] = None
        return pd.DataFrame()
    return _refresh_parquet_cache(state, path)


def load_metrics() -> dict | None:
    if not config.METRICS_PATH.exists():
        return None
    try:
        with open(config.METRICS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return None


@st.fragment(run_every=REFRESH_SECONDS)
def render_dashboard() -> None:
    trends = read_parquet_dir(config.TRENDS_DIR, key="trends")
    predictions = read_parquet_dir(config.PREDICTIONS_DIR, key="predictions")

    # ---------------- KPI cards (windowed trend totals written by Spark) ----
    if not trends.empty:
        total = int(trends["total_messages"].sum())
        positive = int(trends["positive_messages"].sum())
        negative = int(trends["negative_messages"].sum())
        pos_pct = (100.0 * positive / total) if total else 0.0
        neg_pct = 100.0 - pos_pct if total else 0.0
    else:
        total = positive = negative = 0
        pos_pct = neg_pct = 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Processed", f"{total:,}")
    c2.metric("Positive", f"{positive:,}", delta=None)
    c3.metric("Negative", f"{negative:,}")
    c4.metric("Positive %", f"{pos_pct:.1f}%")
    c5.metric("Negative %", f"{neg_pct:.1f}%")

    # ---------------- Sentiment distribution ---------------- 
    st.subheader("Sentiment Distribution")
    if total > 0:
        dist = pd.DataFrame(
            {"Sentiment": ["Positive", "Negative"], "Messages": [positive, negative]}
        ).set_index("Sentiment")
        st.bar_chart(dist)
    else:
        st.info("No windowed trend data yet. Waiting for Spark streaming output...")

    # ---------------- Sentiment trend over time ----------------
    st.subheader("Sentiment Trend Over Time")
    if not trends.empty:
        trend = trends.sort_values("window_start").copy()
        trend["window_start"] = pd.to_datetime(trend["window_start"])
        chart = trend.set_index("window_start")[
            ["positive_messages", "negative_messages", "total_messages"]
        ]
        st.line_chart(chart)
    else:
        st.info("No trend data yet.")

    # ---------------- Latest processed tweets ----------------
    st.subheader("Latest Processed Tweets")
    show_actual = (not predictions.empty) and "actual_sentiment" in predictions.columns
    if not predictions.empty:
        display_cols = [
            "processing_time",
            "date",
            "user",
            "text",
            "predicted_sentiment",
        ]
        if show_actual:
            display_cols.append("actual_sentiment")

        latest = predictions.sort_values(
            "processing_time", ascending=False
        ).head(MAX_LATEST_TWEETS)

        if "processing_time" in latest.columns:
            latest["processing_time"] = pd.to_datetime(
                latest["processing_time"], utc=True, errors="coerce"
            ).dt.strftime("%H:%M:%S")
        if show_actual:
            latest["actual_sentiment"] = latest["actual_sentiment"].map(
                {0: "Negative", 1: "Positive"}
            )
        st.dataframe(
            latest[display_cols].rename(
                columns={
                    "processing_time": "Timestamp (UTC)",
                    "text": "Tweet",
                    "predicted_sentiment": "Predicted Sentiment",
                    "actual_sentiment": "Actual Sentiment",
                    "user": "User",
                    "date": "Tweet Date",
                }
            ),
            use_container_width=True,
        )
    else:
        st.info("No predictions written yet. Start the producer and streaming app.")


def main() -> None:
    st.set_page_config(
        page_title="Real-Time Social Media Sentiment Monitoring",
        layout="wide",
    )
    st.title("Real-Time Social Media Sentiment Monitoring")
    st.caption(
        "Kafka → Spark Structured Streaming → Spark MLlib → Streamlit "
        f"(auto-refresh every {REFRESH_SECONDS}s)"
    )

    metrics = load_metrics()
    if metrics is not None:
        with st.expander("Model Evaluation (Spark MLlib pipeline)"):
            st.json(
                {
                    "accuracy": metrics.get("accuracy"),
                    "precision": metrics.get("precision"),
                    "recall": metrics.get("recall"),
                    "f1_score": metrics.get("f1_score"),
                    "area_under_roc": metrics.get("area_under_roc"),
                    "trained_rows": metrics.get("trained_rows"),
                    "test_rows": metrics.get("test_rows"),
                }
            )

    render_dashboard()

    st.divider()
    st.caption(
        "Trend/KPI totals are recomputed by Spark every minute (1-minute windows, "
        "watermark-based) - they are not hardcoded."
    )


if __name__ == "__main__":
    main()