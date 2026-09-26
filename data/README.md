# Dataset

Place the Sentiment140 CSV here:

```
data/training.1600000.processed.noemoticon.csv
```

## How to download it

1. Go to https://www.kaggle.com/datasets/kazanova/sentiment140
2. Click **Download** (requires a free Kaggle account).
3. Unzip the downloaded archive. The file you need is:

   ```
   training.1600000.processed.noemoticon.csv
   ```

4. Copy that file into this directory so it matches:

   ```
   data/training.1600000.processed.noemoticon.csv
   ```

## Format

The CSV has **no header row**. Columns (in order):

| # | Column  | Description                          |
|---|---------|--------------------------------------|
| 1 | target  | polarity: 0 = negative, 4 = positive |
| 2 | id      | tweet id                             |
| 3 | date    | tweet date                           |
| 4 | query   | query used to scrape                 |
| 5 | user    | username                             |
| 6 | text    | tweet text                           |

The project maps 0 -> Negative, 4 -> Positive (binary classification).