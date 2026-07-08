# Troubleshooting (Week 1)

Common issues when running the local streaming stack.

## Docker stack

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

Symptom: `FileNotFoundException` under `/home/spark/.ivy2/cache`

Fix: Makefile uses `--conf spark.jars.ivy=/tmp/spark-ivy` and a persistent Docker volume. Run:

```bash
make up
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

You're on the old per-message ack path. Ensure Day 3.5 optimization is in `replay_events.py` (batch flush). Expected: **~2–3 min** for 100k.

For quick iteration use:

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

## Quick dev loop vs full demo

| Goal | Command | Time |
|------|---------|------|
| Fast debug | `make quick-test` | ~30–60 sec |
| Full Week 1 demo | `make local-demo-100k` | ~3–5 min |
| 1M milestone | `make produce-1m` + pipeline | ~25+ min |
