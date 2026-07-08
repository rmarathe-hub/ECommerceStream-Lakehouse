# Source dataset (not committed)

Download monthly CSV files from Kaggle and place them here:

https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

Recommended starter file:

- `2019-Oct.csv` or `2019-Oct.csv.gz` (~4M+ events; enough for 1M samples)

For 5M samples, October alone is sufficient (~42M source rows). Additional months are optional for variety.

**5M demo is local only** — do not upload to S3 or reload Snowflake. See [docs/demo_strategy.md](../../docs/demo_strategy.md).

Then run:

```bash
make sample-1m
make sample-5m
```

Sampled outputs are written to `data/raw/events_1m.csv` and `data/raw/events_5m.csv`.
Raw source files stay local and are never uploaded to S3 or Snowflake.
