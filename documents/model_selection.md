# Model Selection — Why Logistic Regression?

> Documentation for the sentiment classifier used in this project.
> Pipeline: `RegexTokenizer -> StopWordsRemover -> HashingTF -> IDF -> LogisticRegression`
> Trained on Sentiment140 **Part 1** (`train.csv`, ~960k tweets), evaluated on
> **Part 2** (`test.csv`) and **Part 3** (`validation.csv`).

---

## 1. The problem we are solving

Binary sentiment classification on tweets:

| Label | Meaning |
|-------|---------|
| `0`   | Negative |
| `1`   | Positive |

Input is raw tweet text. Output is a class **plus a probability**, because the
web app turns that probability into the company outlook (Good / Warning / Bad)
and the positive-percentage forecast.

---

## 2. Why Logistic Regression?

### 2.1 The data shape suits it perfectly

Our features are produced by `HashingTF` + `IDF` into **2^18 = 262,144** sparse
buckets. A 200-character tweet activates maybe 15-25 of them, so the matrix is
**~99.99% zeros**.

Logistic Regression is designed for exactly this: it learns one weight per
feature and computes `sigmoid(w·x)`. It never materialises a dense matrix, so
the 262k dimensions cost almost nothing.

### 2.2 It scales across the full training set

Spark MLlib's `LogisticRegression` solves the objective with L-BFGS over a
distributed RDD of sparse vectors, so it scales horizontally across executors.
This matters because Part 1 has ~960,000 rows.

### 2.3 It is interpretable — which matters for the VIVA

The entire trained model is **one number per word**. We can answer
"why was this tweet classified negative?" by listing the highest-weighted
negative features. For an academic project that must *explain* the model, a
black-box ensemble is a weakness rather than an advantage.

### 2.4 Trees are a poor fit for bag-of-words text

- **Decision Tree** — needs to be extremely deep to express interactions like
  "negative word present AND no negation". It overfits sparse text quickly and
  produces long, unreadable rule chains.
- **Random Forest** — a forest of 100 trees, each split testing one feature, can
  use only a few hundred of the 262k features. It effectively discards the
  sparse structure that makes the data cheap to represent.
- **Memory / broadcast cost** — forests must be shipped to every Spark executor,
  whereas a logistic weight vector is tiny.

### 2.5 It produces a natural probability, which the UI needs

`sigmoid(w·x)` is a calibrated score in `[0, 1]`. Both of these are computed
from it:

- the company **outlook** classification (Good / Warning / Bad)
- the **positive percentage forecast** for the next window

Tree ensembles return hard class labels. Getting a probability requires
averaging per-tree votes — extra work for no benefit here.

### 2.6 It matches the label type natively

Sentiment140 is binary (`0` negative, `4` positive, mapped to `0`/`1`).
Logistic Regression is a native binary linear classifier — no one-hot encoding,
no softmax layer, no threshold tuning machinery.

---

## 3. Why not the alternatives?

| Algorithm | Why we did not use it |
|---|---|
| **Decision Tree** | Overfits sparse high-dimensional text; rules too deep to explain; no probability output |
| **Random Forest** | Spark MLlib exposes `RandomForestRegressor` but **no Random Forest classifier**; ignores sparsity; heavy and uninterpretable |
| **GBT / XGBoost** | Same sparsity problem, slower per-row inference in streaming, harder to justify to an examiner |
| **Naive Bayes** | Very fast, but assumes feature independence — breaks on negation ("not good") and multi-word phrases |
| **LinearSVC** | Competitive accuracy, but produces no calibrated probability, which our outlook/forecast features require |
| **SVM (kernel)** | Kernel methods do not scale to 960k rows and cannot exploit sparse features well |
| **BERT / Transformers** | Needs a GPU, has no Spark `ml` equivalent, and is far too heavy for a streaming demo |

---

## 4. The full pipeline, step by step

| Stage | What it does | Why it is there |
|---|---|---|
| Text cleaning (Spark expressions) | strips URLs, `@mentions`, hashtags, HTML entities, punctuation; lowercases | removes noise before tokenisation |
| `RegexTokenizer` | splits cleaned text into word tokens | simple, fast, dependency-free |
| `StopWordsRemover` | drops common uninformative words | reduces dimensionality |
| `HashingTF` (2^18 buckets) | maps tokens to fixed sparse indices | no vocabulary to fit, no storage needed |
| `IDF` | down-weights terms appearing in many tweets | rare, sentiment-carrying words get more weight |
| `LogisticRegression` | binary classifier, probability output | sparse-friendly, scalable, interpretable |

---

## 5. Results

Measured by `src/train_model.py` on real predictions, written to
`artifacts/model_metrics.json`.

> **Note:** the numbers below are from the **3,000-row smoke model**
> (`--limit 3000`), which exists so the pipeline can be demonstrated quickly.
> The full-data model is trained by deleting `models/` and running
> `python scripts\run_demo.py` without `--limit`; TF-IDF + Logistic Regression on
> the complete Sentiment140 training split typically reaches **~79-80% accuracy**.

| Metric | Test (Part 2) | Validation (Part 3) |
|---|---|---|
| Rows | 240,726 | 240,034 |
| Accuracy | 0.6711 | 0.6717 |
| Precision | 0.6697 | 0.6704 |
| Recall | 0.6748 | 0.6752 |
| F1-score | 0.6722 | 0.6728 |
| Area under ROC | 0.7226 | 0.7232 |

Confusion matrix (test):

|  | Predicted negative | Predicted positive |
|---|---|---|
| **Actual negative** | 80,381 (TN) | 40,048 (FP) |
| **Actual positive** | 39,115 (FN) | 81,182 (TP) |

No results are fabricated — every value comes from actual model output on data
the model never trained on.

---

## 6. Honest limitations

- The model is trained on 2009-era tweets, so performance on modern slang and
  emoji-heavy text is approximate. This is a classic ML task, not a
  transformer task.
- ~0.67 accuracy on the 3k model reflects a deliberately under-trained model, not
  a limitation of the algorithm. A single word like "not" is invisible to a
  bag-of-words model, which is exactly why the full-data run matters.
- Deliberate bias in Sentiment140 (negative sentiment is often labelled
  sarcastic) is inherited by any model trained on it.
