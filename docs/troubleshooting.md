# Troubleshooting

Common issues when running the local streaming lakehouse stack.

## Docker stack

### `make up` hangs or never returns

Do **not** use `docker compose wait` on long-running services (Redpanda, Spark, MinIO) — it waits for containers to **exit**, which never happens.

This project uses `scripts/wait_for_stack.sh`, which polls healthchecks until ready (default timeout: 120s):

```bash
make up          # docker compose up -d + wait-for-stack
make wait-for-stack   # poll only (stack already running)
```

### `make up` fails or containers unhealthy

```bash
make down
make up
make ps
```

Check individual logs:

```bash
docker compose logs redpanda --tail 50
docker compose logs spark-master --tail 50
```

### Spark `spark-submit` Ivy / permission errors

Symptoms:

- `FileNotFoundException` under `/home/spark/.ivy2/cache`
- `Permission denied` on `/tmp/spark-ivy/cache`

Fix: Makefile sets `--conf spark.jars.ivy=/tmp/spark-ivy`, uses a persistent Docker volume, and fixes ownership before submit:

```bash
make up
make stream-bronze
```

If permissions are still broken after a volume recreate:

```bash
docker exec -u 0 spark-master sh -c 'mkdir -p /tmp/spark-ivy/cache && chown -R spark:spark /tmp/spark-ivy'
make stream-bronze
```

## Producer

### `Could not connect to Kafka at localhost:19092`

Stack is not running:

```bash
make up
make ps   # redpanda should be healthy
```

### `produce-100k` is slow (~25 min)

You're on the old per-message ack path. Ensure batch flush optimization is in `replay_events.py`. Expected: **~2–3 min** for 100k.

For quick iteration:

```bash
make produce-10k
```

## Bronze streaming

### `stream-bronze` processes no new rows

Checkpoint already consumed prior offsets. Either:

```bash
make produce-100k      # add new Kafka messages
make stream-bronze     # incremental
```

Or full reprocess:

```bash
make stream-bronze-reset
```

### Bronze row count much larger than expected

`stream-bronze` is incremental. Repeated `produce-100k` runs append to Kafka and bronze grows. For a clean 100k bronze test:

```bash
make stream-bronze-reset
make smoke-test-100k
```

## Silver / gold transforms

### Silver row count lower than bronze

Expected when bronze contains duplicate `event_id`s from repeated Kafka replays. Silver deduplicates by `event_id` (latest offset wins).

For a clean 1M run:

```bash
make reset-demo-state
make local-demo-1m
```

### Re-validate 1M outputs without replaying Kafka (~1 min)

If you already ran `local-demo-1m` successfully:

```bash
make verify-1m
```

## Validation

### `No module named 'pyarrow'`

```bash
make venv
.venv/bin/pip install -r requirements.txt
```

### `No Parquet files found under data/bronze/events`

Run streaming first:

```bash
make stream-bronze
make validate-bronze
```

## Command timing guide

| Goal | Command | Typical time |
|------|---------|--------------|
| Fast debug | `make quick-test` | ~30–60 sec |
| Week 1 demo | `make local-demo-100k` | ~3–5 min |
| Verify existing 1M data | `make verify-1m` | ~30–60 sec |
| Full 1M from scratch | `make local-demo-1m` | ~25–35 min |
| 5M stress test | `make produce-5m` + transforms | ~1.5+ hours |
| Gold S3 upload | `make upload-gold-s3` | ~30–60 sec (225 files) |

## S3 gold upload

### `SignatureDoesNotMatch`

Wrong or truncated `AWS_SECRET_ACCESS_KEY` in `.env`. Re-copy from Terraform:

```bash
cd infra/aws
terraform output upload_access_key_id
terraform output -raw upload_secret_access_key | pbcopy
```

Paste into repo root `.env` with **no quotes**, include every character (e.g. trailing `%` if present in raw output).

Verify from **repo root**:

```bash
set -a && . ./.env && set +a
aws sts get-caller-identity
# Arn should end with: commercestream-lakehouse-uploader
```

### `Access denied` on bucket check

The upload IAM user cannot use `HeadBucket`. The upload script uses `list_objects` with `gold/` prefix instead. Ensure you use upload user keys in `.env`, not admin `~/.aws/credentials`.

### `NoSuchBucket`

Run `terraform apply` in `infra/aws` before `make upload-gold-s3`.

### Wrong identity when testing

Source `.env` from **repo root**, not `infra/aws`:

```bash
cd /path/to/ECommerce_Stream_Lakehouse
set -a && . ./.env && set +a
```
