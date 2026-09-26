# Real-Time Social Media Sentiment Monitoring

A real-time analytics MVP that consumes a stream of tweets via **Apache Kafka**,
classifies each tweet's sentiment with a scalable **Spark MLlib** model inside
**Spark Structured Streaming**, computes windowed sentiment trends, and shows
everything on a lightweight **Streamlit** dashboard.

```
                     Sentiment140 Dataset
                             |
                             v
                     Python Kafka Producer
                             |
                             v
                      Apache Kafka Topic
                             |
                             v
                  Spark Structured Streaming
                             |
                             v
                   Spark MLlib Pipeline (saved model)
                             |
                  +--------------------------+
                  |                          |
                  v                          v
         Sentiment Predictions      Windowed Trend Analysis
                  |                          |
                  +------------+-------------+
                               v
                     Streamlit Dashboard
```

---

## Problem Statement

Develop a real-time analytics system that processes continuously arriving
social media messages and identifies the sentiment or trend associated with the
incoming data, using a streaming framework (Kafka + Spark Structured Streaming)
and scalable machine learning (Spark MLlib).

## Objectives

1. Ingest synthetic streaming data using Apache Kafka.
2. Process the stream with Spark Structured Streaming.
3. Classify sentiment in-flight using a Spark MLlib model trained on the
   Sentiment140 dataset.
4. Aggregate sentiment into 1-minute windows to reveal live trends.
5. Visualize KPIs, charts and the latest tweets on a Streamlit dashboard.

## Technologies Used

| Layer         | Technology                                    |
|---------------|-----------------------------------------------|
| Streaming     | Apache Kafka 3.9 (single node, KRaft mode)    |
| Stream engine | Apache Spark 4.0 + Spark Structured Streaming |
| ML            | Spark MLlib (TF-IDF + Logistic Regression)    |
| Language      | Python 3.10 / PySpark                         |
| Dashboard     | Streamlit                                     |
| Infra         | Docker / Docker Compose                       |

> Spark **4.0.x** is intentionally chosen: older Spark 3.5.x throws
> `UnsupportedOperationException: getSubject is not supported` on JDK 17+.
> Spark 4.x officially runs on JDK 17. A portable JRE 17 is bundled inside the
> venv (`.venv/jvm17`) so no system-wide JDK is required.

---

## Dataset

**Sentiment140** — https://www.kaggle.com/datasets/kazanova/sentiment140

The main training file contains approx. **1.6 million labelled tweets**:

```
data/training.1600000.processed.noemoticon.csv
```

It has **no header row**. Columns: `target, id, date, query, user, text`.

Labels (binary sentiment classification):

| Raw target | Internal label | Meaning |
|------------|----------------|---------|
| 0          | 0              | Negative |
| 4          | 1              | Positive |

The project **only** does binary classification (Negative / Positive). There is
no artificial Neutral class. Original tweet text is preserved for display.

See `data/README.md` for exact download instructions.

---

## System Architecture

```
Sentiment140
    |
    v
Kafka Producer  (python src/kafka_producer.py)
    |
    v
Kafka Topic  (social-media-stream)
    |
    v
Spark Structured Streaming  (python src/streaming_sentiment.py)
    |
    v
Text Preprocessing  (URL/mention/HTML cleaning via Spark regexp_replace)
    |
    v
Spark MLlib Pipeline  (RegexTokenizer -> StopWordsRemover -> HashingTF -> IDF
                        -> LogisticRegression)  [saved PipelineModel]
    |
    v
Sentiment Predictions  (Negative / Positive)
    |                                  |
    v                                  v
output/predictions (Parquet)     output/trends (Parquet, 1-minute windows,
                                  watermark-based aggregation)
    |                                  |
    +------------------+---------------+
                       v
              Streamlit Dashboard (dashboard/app.py)
```

Two Spark streaming queries run off the same Kafka source:
- **prediction-sink** — writes every classified tweet to `output/predictions`.
- **trend-sink** — `groupBy(window(ingestion_time, "1 minute"))` aggregation
  (with a 2-minute watermark) writing totals + Positive/Negative percentages to
  `output/trends`.

The trend/count numbers shown on the dashboard are computed *by Spark*, not in
Python or Pandas.

---

## ML Pipeline

Text cleaning happens before the pipeline (Spark column expressions strip URLs,
`@mentions`, hashtags, HTML entities, punctuation, collapse whitespace,
lowercase). Then the Spark ML `Pipeline` applies:

```
RegexTokenizer  ->  StopWordsRemover  ->  HashingTF  ->  IDF  ->  LogisticRegression
```

- **RegexTokenizer** splits the cleaned tweet into word tokens.
- **StopWordsRemover** drops common, uninformative words.
- **HashingTF** maps tokens into 2^18 sparse hashed feature buckets.
- **IDF** down-weights terms that appear in many tweets.
- **LogisticRegression** trains a binary classifier (label 0/1) on the sparse
  TF-IDF vectors.

### Why Logistic Regression?

- Well suited to **large, sparse** TF-IDF text features.
- **Scales elegantly** in Spark MLlib across the ~1.28M-row training set.
- Fully **interpretable** (weights per feature, standard classification
  metrics).
- Appropriate for **binary** positive/negative sentiment, with a natural
  probability output (score >= 0.5 -> Positive).

No Hugging Face / BERT / Transformers / external sentiment APIs are used.

---

## Project Structure

```
SML_BDA/
|-- data/                     # place Sentiment140 CSV here (see data/README.md)
|-- models/                   # saved PipelineModel: models/sentiment_pipeline/
|-- artifacts/                # model_metrics.json
|-- output/
|   |-- predictions/          # Parquet written by Spark (per-tweet)
|   |-- trends/               # Parquet windowed trend aggregation
|-- checkpoints/              # Spark streaming checkpoints (recovery)
|-- jars/                     # Kafka connector jars for Spark Structured Streaming
|-- scripts/
|   |-- bootstrap.ps1         # one-command Windows setup (venv, JRE17, winutils, jars)
|-- src/
|   |-- config.py             # all configuration + env overrides
|   |-- spark_utils.py        # shared SparkSession builder
|   |-- dataset_utils.py      # Sentiment140 loader, cleaning, label conversion
|   |-- train_model.py        # trains + saves the Spark ML PipelineModel
|   |-- kafka_producer.py     # replays Sentiment140 into Kafka as JSON
|   |-- streaming_sentiment.py# Kafka -> ML predictions -> Parquet + trends
|-- dashboard/
|   |-- app.py                # Streamlit dashboard
|-- tests/                    # basic unit tests
|-- docker-compose.yml        # single-node Kafka (KRaft)
|-- requirements.txt
|-- .gitignore
|-- README.md
```

---

## Installation

Prerequisites:

- Windows/Linux/macOS
- Python 3.10+
- Docker / Docker Compose (only for Kafka)
- Internet access on first setup (downloads deps, JRE 17, winutils, Kafka jars)

### Windows (recommended path)

Everything is kept inside the project venv `.venv` (Python packages, a portable
JRE 17, winutils, Kafka connector jars) — nothing is installed system-wide:

```powershell
# 1) One-command bootstrap (venv + deps + JRE17 + winutils + Kafka jars)
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1

# 2) Start Kafka
docker compose up -d

# 3) Verify Kafka is healthy (wait ~10-30s for first start)
docker compose ps
```

### Linux / macOS / other setups

```bash
python -m venv .venv
source .venv/bin/activate           # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
# Provide a system JDK 17+ (JAVA_HOME) - Spark 4.x needs at least JDK 17.
# Download the Kafka connector jars (matched to your Spark version) into jars/
#   org.apache.spark:spark-sql-kafka-0-10_2.13:<spark-version>
#   ... and its transitive deps, OR use --packages.
```

---

## Dataset Setup

1. Download **Sentiment140** from
   https://www.kaggle.com/datasets/kazanova/sentiment140 (free Kaggle account).
2. Unzip `archive.zip`.
3. Copy `training.1600000.processed.noemoticon.csv` into:

   ```
   data/training.1600000.processed.noemoticon.csv
   ```

4. Optional quick test: any smaller CSV with the same schema works too,
   e.g. `python src/train_model.py --limit 50000`.

---

## Running the Project

Run every command from the project root. Use the venv's Python.

### STEP 1 — Start Kafka

```bash
docker compose up -d
docker compose ps          # wait until status is Running/healthy
docker compose logs -f kafka   # optional: watch for "(kafka) started"
```

Stop / clean up later with:

```bash
docker compose down        # stop (keeps broker data volume)
docker compose down -v     # stop and delete Kafka data
```

### STEP 2 — Train the ML model (once)

Train on the full dataset (a few minutes on a laptop):

```bash
.\.venv\Scripts\python.exe src\train_model.py
```

Quick smoke training on the first 50k rows instead:

```bash
.\.venv\Scripts\python.exe src\train_model.py --limit 50000
```

Outputs:

- `models/sentiment_pipeline/` — the complete fitted `PipelineModel`
- `artifacts/model_metrics.json` — accuracy/precision/recall/F1/AUC + confusion
  matrix, computed from real test-set predictions

The streaming app **does not retrain**; it loads the saved model.

### STEP 3 — Start Spark Structured Streaming

```bash
.\.venv\Scripts\python.exe src\streaming_sentiment.py
```

It subscribes to topic `social-media-stream`, applies the model, writes
predictions to `output/predictions` and trends to `output/trends`.

The app first creates the Kafka topic if it does not exist yet (idempotent), so
it can be started before the producer — only Kafka itself must already be up.

### STEP 4 — Start the Kafka producer (separate terminal)

```bash
.\.venv\Scripts\python.exe src\kafka_producer.py --messages-per-second 10
```

Options:

```bash
# Stop after 1000 messages, faster rate
.\.venv\Scripts\python.exe src\kafka_producer.py --messages-per-second 50 --max-messages 1000
```

### STEP 5 — Open the Streamlit dashboard (separate terminal)

```bash
.\.venv\Scripts\streamlit.exe run dashboard\app.py
```

Open http://localhost:8501 — it auto-refreshes every 5 seconds and will
start filling once messages flow.

### Typical flow

```
Kafka          docker compose up -d
Model          python src/train_model.py
Streaming      python src/streaming_sentiment.py   (terminal 1)
Producer       python src/kafka_producer.py          (terminal 2)
Dashboard      streamlit run dashboard/app.py        (terminal 3)
```

---

## Model Evaluation

`train_model.py` evaluates the held-out test set (20%) using real predictions:

- **Accuracy**, **Precision**, **Recall**, **F1-score**, **Area under ROC**
- **Confusion matrix** derived from `groupBy(label, prediction)` counts
- Metrics are written to `artifacts/model_metrics.json` and optionally shown on
  the dashboard.

No results are fabricated; all values come from actual model outputs.

## Streaming Analysis

- **Ingestion timestamp**: the producer stamps each message with `ingestion_ts_ms`
  (publish time) plus an ISO-8601 `ingestion_timestamp`. The original
  Sentiment140 `date` is kept separately.
- **Predictions**: every message is classified by the saved PipelineModel and
  appended to `output/predictions` (Parquet). Batch progress is logged.
- **Trends**: a separate streaming query applies
  `withWatermark("event_time", "2 minutes")` then
  `groupBy(window(event_time, "1 minute"))` and aggregates
  total / positive / negative / `positive_pct` / `negative_pct`, appended to
  `output/trends` (Parquet). Because of the watermark, an early window can be
  updated by late events — exactly what Spark Structured Streaming is for.

## Screenshots

> (Add screenshots here when you have them, e.g. `docs/screenshots/dashboard.png`)

---

## Limitations

- Sentiment140 is a **historical** dataset; tweets are **replayed through
  Kafka** to simulate a continuous social-media stream. The project does not
  depend on a live social-media API.
- The ML model is trained on 2009-era tweets, so performance on modern language
  is approximate (classic ML task, no transformer models).
- The dashboard and streaming app run on a single machine (`local[*]`), the
  default Spark deployment. The architecture (Kafka + Structured Streaming +
  MLlib) transfers unchanged to a small cluster.
- No authentication/authorization anywhere by design (academic MVP, local only).

## Future Scope

- Multi-class sentiment (plus Neutral) and emotion labels with the same MLlib
  pipeline.
- Additional window granularities (5m/15m/1h) and drill-down by hashtag/user.
- Cluster deployment (Spark on YARN/Kubernetes) and a message-oriented data lake
  (e.g. Delta Lake) instead of plain Parquet output.
- A dockerised Spark service alongside Kafka so the whole stack runs in compose.

---

## Testing

```bash
.\.venv\Scripts\python.exe -m pytest tests\ -q
```

Covers label conversion, tweet cleaning, Kafka message serialization, schema
handling and prediction-label conversion.