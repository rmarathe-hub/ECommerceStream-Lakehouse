.PHONY: help up down logs ps sample-1m sample-5m produce-100k produce-1m produce-5m venv

PYTHON ?= .venv/bin/python3

help:
	@echo "ECommerceStream-Lakehouse — available commands"
	@echo ""
	@echo "  make venv            Create .venv and install Python dependencies"
	@echo "  make up            Start local Docker stack (Redpanda, Spark, MinIO)"
	@echo "  make down          Stop local Docker stack"
	@echo "  make logs          Tail Docker stack logs"
	@echo "  make ps            Show running stack containers"
	@echo "  make sample-1m     Sample 1M events -> data/raw/events_1m.csv"
	@echo "  make sample-5m     Sample 5M events -> data/raw/events_5m.csv (local demo only)"
	@echo "  make produce-100k  Replay 100k events to Kafka (requires: make up)"
	@echo "  make produce-1m    Replay 1M events to Kafka (local demo)"
	@echo "  make produce-5m    Replay 5M events to Kafka (local stress test)"
	@echo ""
	@echo "Optional Postgres (Airflow): docker compose --profile airflow up -d"

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

venv:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

sample-1m:
	$(PYTHON) src/utils/sample_events.py --limit 1000000 --output data/raw/events_1m.csv

sample-5m:
	$(PYTHON) src/utils/sample_events.py --limit 5000000 --output data/raw/events_5m.csv

produce-100k:
	$(PYTHON) src/producer/replay_events.py \
		--input data/raw/events_1m.csv \
		--topic ecommerce_events \
		--limit 100000 \
		--rate-per-second 1000

produce-1m:
	$(PYTHON) src/producer/replay_events.py \
		--input data/raw/events_1m.csv \
		--topic ecommerce_events \
		--limit 1000000 \
		--rate-per-second 1000

produce-5m:
	$(PYTHON) src/producer/replay_events.py \
		--input data/raw/events_5m.csv \
		--topic ecommerce_events \
		--limit 5000000 \
		--rate-per-second 1000
