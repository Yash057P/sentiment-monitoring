# Real-Time Social Media Sentiment Monitoring

A real-time analytics MVP that consumes a stream of tweets via **Apache Kafka**,
classifies each tweet's sentiment with a scalable **Spark MLlib** model inside
**Spark Structured Streaming**, computes windowed sentiment trends, and serves
everything through a **Flask REST API** to a **React** web app with role-based
admin/company dashboards, light/dark mode and English/Hindi/Marathi.

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
             Parquet -> Flask API -> React
```

---

## Problem Statement

Develop a real-time analytics system that processes continuously arriving
social media messages and identifies the sentiment or trend associated with the
incoming data, using a streaming framework (Kafka + Spark Structured Streaming)
and scalable machine learning (Spark MLlib).

## Objectives

1. Ingest streaming data using Apache Kafka.
2. Process the stream with Spark Structured Streaming.
3. Classify sentiment in-flight using a Spark MLlib model trained on the
   Sentiment140 dataset, with the data split into 4 parts: **train** (60%),
   **test** (15%), **validation** (15%) and a hold-out **simulation** part (10%)
   used exclusively for the live demo.
4. Aggregate sentiment into 1-minute windows to reveal live trends.
5. Visualize KPIs, charts and the latest tweets on a role-based React web app
   served by the Flask API (admin sees every company; each company user only
   sees its own).
6. One command (`scripts/run_demo.py`) runs the entire project end to end.

## Technologies Used

| Layer         | Technology                                    |
|---------------|-----------------------------------------------|
| Streaming     | Apache Kafka 3.9 (single node, KRaft mode)    |
| Stream engine | Apache Spark 4.0 + Spark Structured Streaming |
| ML            | Spark MLlib (TF-IDF + Logistic Regression)    |
| Language      | Python 3.10 / PySpark                         |
| Web API       | Flask 3 (REST, JWT auth, CORS)               |
| Web UI        | React 18 + Vite + Recharts                    |
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

### 4-part split

The dataset is deterministically divided (hash of the tweet id, so re-running
always produces identical parts) into:

| Part | File | Share | Purpose |
|---|---|---|---|
| Part 1 | `data/split/train.csv` | 60% (~960k) | Model training |
| Part 2 | `data/split/test.csv` | 15% (~240k) | Testing (accuracy, precision, recall…) |
| Part 3 | `data/split/validation.csv` | 15% (~240k) | Validation (extra metric check) |
| Part 4 | `data/split/simulation.csv` | 10% (~160k) | **Live simulation feed** — the demo streams *only* this hold-out part, so the model never saw these tweets during training |

`scripts/run_demo.py` downloads the dataset (if missing), performs this split,
trains the model on Part 1 and evaluates it on Parts 2 & 3 automatically.

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
              Flask REST API (backend/app.py)  ->  React web app
```

Two Spark streaming queries run off the same Kafka source:
- **prediction-sink** — writes every classified tweet to `output/predictions`.
- **trend-sink** — `groupBy(window(ingestion_time, "1 minute"))` aggregation
  (with a 2-minute watermark) writing totals + Positive/Negative percentages to
  `output/trends`.

The trend/count numbers shown on screen are computed *by Spark*, not in
Python or Pandas; the Flask API only reads the Parquet that Spark wrote.

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
- **Scales elegantly** in Spark MLlib across the ~960k-row training part.
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
|   |-- run_demo.py           # ONE command: env -> dataset -> split -> train -> live demo
|-- src/
|   |-- config.py             # all configuration + env overrides
|   |-- spark_utils.py        # shared SparkSession builder
|   |-- dataset_utils.py      # Sentiment140 loader, cleaning, label conversion
|   |-- dataset_split.py      # 4-way split: train/test/validation/simulation
|   |-- train_model.py        # trains + evaluates (Parts 1/2/3) + saves the model
|   |-- kafka_producer.py     # replays the simulation part into Kafka as JSON
|   |-- streaming_sentiment.py# Kafka -> ML predictions -> Parquet + trends
|-- backend/
|   |-- app.py                # Flask REST API + serves the built React app
|   |-- analytics.py          # incremental Parquet reader + live KPI aggregation
|   |-- auth.py               # JWT login + admin/company roles
|-- frontend/                 # React 18 + Vite web app (npm run build -> dist/)
|-- tests/                    # backend/API + dataset unit tests
|-- docker-compose.yml        # single-node Kafka (KRaft)
|-- requirements.txt
|-- .gitignore
|-- README.md
```

---

## Web Application (Flask API + React UI)

The UI replaced the earlier Streamlit prototype. Spark still writes Parquet; the
Flask API reads it incrementally (only *new* files per scan) and the React app
polls the API every 5 seconds.

### Roles & login

| Role     | Username                              | Password       |
|----------|---------------------------------------|----------------|
| Admin    | `admin`                               | `admin`        |
| Company  | `technova`, `urbaneats`, `skyride`, `novapay`, `pacificair`, `zenwear` | `<handle>123` |

Login returns a 12-hour JWT. The admin sees every company, tweet stream and
outlook; a company user only ever receives its own data (enforced server-side
by `require_role`, not just hidden in the UI).

### API

| Method | Endpoint                        | Access | Purpose                                  |
|--------|---------------------------------|--------|------------------------------------------|
| POST   | `/api/auth/login`               | public | returns `{ token, profile }`             |
| GET    | `/api/me`                       | any    | current profile                          |
| GET    | `/api/health`                   | public | liveness + live row count                |
| GET    | `/api/companies/meta`           | any    | company list, sectors, keywords          |
| GET    | `/api/admin/overview`           | admin  | platform KPIs, per-company table, chart  |
| GET    | `/api/admin/tweets?company=`    | admin  | latest tweets + negative "impact" keywords |
| POST   | `/api/admin/reset`              | admin  | clear live data, counters restart from 0 |
| GET    | `/api/company/overview`         | company| that company's KPIs, chart, outlook, forecast |
| GET    | `/api/company/tweets?filter=`   | company| positive / negative / all tweets        |
| GET    | `/api/overall`                  | public | landing-page summary                    |

**Reset live data** (`POST /api/admin/reset`, body `{"wipe_files": true}`)
clears the API's in-memory cache and deletes the existing
`output/predictions` Parquet files, so every counter restarts at 0 and counts
only tweets streamed from that moment on. Spark checkpoints and the Kafka topic
are intentionally kept, so already-consumed messages are not replayed.

### Frontend features

- Interactive login with quick demo-account chips and password visibility toggle.
- Admin dashboard: platform KPIs, per-company comparison, tweet search, live feed.
- Company dashboard: KPIs, sentiment timeline chart, 1-minute outlook
  (Good / Warning / Bad), short forecast and the keywords behind negative tweets.
- Persistent light/dark theme, English / Hindi / Marathi switcher, About Us and
  Settings pages, desktop taskbar nav and a mobile drawer.
- `scripts/run_demo.py` supervises the streaming, producer and web processes and
  restarts any of them that dies.

---

## Quick Start (recommended)

> **One command runs the ENTIRE project.** Nothing else is required.

```powershell
python scripts\run_demo.py
```

`run_demo.py` does everything automatically:

1. Creates the virtual environment + installs required libraries (if missing,
   it runs `scripts/bootstrap.ps1` first).
2. Downloads the **Sentiment140** dataset from Stanford (if missing; a built-in
   demo set is used when offline).
3. Splits it into the **4 parts** (train / test / validation / simulation).
4. Trains the model on **Part 1**, evaluates **Part 2 (test)** and
   **Part 3 (validation)**.
5. Starts Docker Kafka → Spark streaming → the producer streaming **Part 4
   (simulation)** "as if live" → builds the React app and opens the Flask web
   app in the browser.

While it runs, the supervisor watches the streaming, producer and web processes
and restarts any of them that dies. Press `Ctrl+C` to stop everything (it also
cleans up Docker). See
[Running the Project](#running-the-project) for the manual steps and options.

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

1. **Automatic (recommended):** `python scripts\run_demo.py` downloads
   Sentiment140 for you (Stanford mirror, ~80 MB) and places it here:
   `data/training.1600000.processed.noemoticon.csv`
2. **Manual:** download from
   https://www.kaggle.com/datasets/kazanova/sentiment140 (free Kaggle account),
   unzip and copy the file into `data/`. If the network is unavailable,
   `run_demo.py` falls back to a small built-in demo dataset so the demo still
   runs offline.

---

## Running the Project

> **Recommended:** run the whole project with the single command
> `python scripts\run_demo.py` (see [Quick Start](#quick-start-recommended)).
> The steps below show how each part works individually.

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

Trains on **Part 1** (`data/split/train.csv`) and evaluates **Part 2 (test)**
and **Part 3 (validation)**:

```bash
.\.venv\Scripts\python.exe src\train_model.py`
    --train-dataset data\split\train.csv
    --test-dataset  data\split\test.csv
    --valid-dataset data\split\validation.csv
```

Quick smoke training on the first 50k rows of Part 1 instead:

```bash
.\.venv\Scripts\python.exe src\train_model.py`
    --train-dataset data\split\train.csv
    --test-dataset  data\split\test.csv
    --valid-dataset data\split\validation.csv
    --limit 50000
```

> If the parts don't exist yet, first run `python src\dataset_split.py`, or
> simply use `scripts\run_demo.py` which does all of this automatically.

Outputs:

- `models/sentiment_pipeline/` — the complete fitted `PipelineModel`
- `artifacts/model_metrics.json` — accuracy/precision/recall/F1/AUC + confusion
  matrix, computed from real predictions on the test AND validation parts

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

Streams **Part 4 (simulation)** — the demo's live feed:

```bash
.\.venv\Scripts\python.exe src\kafka_producer.py --dataset data\split\simulation.csv --messages-per-second 10
```

Options:

```bash
# Stop after 1000 messages, faster rate
.\.venv\Scripts\python.exe src\kafka_producer.py --dataset data\split\simulation.csv --messages-per-second 50 --max-messages 1000
```

### STEP 5 — Start the Flask API + React web app (separate terminal)

```bash
.\.venv\Scripts\python.exe backend\app.py
```

Open http://localhost:8000 — Flask serves the built React bundle and the JSON
API, the app auto-refreshes every 5 seconds and will start filling once
messages flow. The first run needs the frontend to be built once:

```bash
cd frontend
npm install
npm run build
```

### Typical flow

```bash
EVERYTHING:   python scripts\run_demo.py          # the whole project in one command
```
```bash
# ...or manually, step by step:
Kafka          docker compose up -d
Split          python src/dataset_split.py        # data/split/*.csv (4 parts)
Model          python src/train_model.py --train-dataset data\split\train.csv --test-dataset data\split\test.csv --valid-dataset data\split\validation.csv
Streaming      python src/streaming_sentiment.py   (terminal 1)
Producer       python src/kafka_producer.py --dataset data\split\simulation.csv   (terminal 2)
Web app        python backend/app.py                     (terminal 3)
```

---

## Model Evaluation

`train_model.py` trains on **Part 1** and evaluates on **Part 2 (test)** and
**Part 3 (validation)** using real predictions:

- **Accuracy**, **Precision**, **Recall**, **F1-score**, **Area under ROC**
- **Confusion matrix** derived from `groupBy(label, prediction)` counts
- Both test and validation metrics are written to
  `artifacts/model_metrics.json` and surfaced in the admin dashboard.

No results are fabricated; all values come from actual model outputs, measured
on data the model never trained on.

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
  depend on a live social-media API. The demo streams only the hold-out
  **simulation part** so the "live" tweets were never used for training.
- The ML model is trained on 2009-era tweets, so performance on modern language
  is approximate (classic ML task, no transformer models).
- The web app and streaming app run on a single machine (`local[*]`), the
  default Spark deployment. The architecture (Kafka + Structured Streaming +
  MLlib) transfers unchanged to a small cluster.
- Authentication is JWT-based with two roles (admin / company). Demo credentials are
  hard-coded in `backend/auth.py` for the academic MVP, so it is still local only
  and must not be exposed publicly as-is.

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
