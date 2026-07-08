.PHONY: help up down logs ps sample-1m sample-5m

help:
	@echo "ECommerceStream-Lakehouse — available commands"
	@echo ""
	@echo "  make up         Start local Docker stack (Redpanda, Spark, MinIO)"
	@echo "  make down       Stop local Docker stack"
	@echo "  make logs       Tail Docker stack logs"
	@echo "  make ps         Show running stack containers"
	@echo "  make sample-1m  Sample 1M events -> data/raw/events_1m.csv"
	@echo "  make sample-5m  Sample 5M events -> data/raw/events_5m.csv (local demo only)"
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

sample-1m:
	python3 src/utils/sample_events.py --limit 1000000 --output data/raw/events_1m.csv

sample-5m:
	python3 src/utils/sample_events.py --limit 5000000 --output data/raw/events_5m.csv
