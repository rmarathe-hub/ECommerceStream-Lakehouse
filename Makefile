.PHONY: help up down logs ps

help:
	@echo "ECommerceStream-Lakehouse — available commands"
	@echo ""
	@echo "  make up    Start local Docker stack (Redpanda, Spark, MinIO)"
	@echo "  make down  Stop local Docker stack"
	@echo "  make logs  Tail Docker stack logs"
	@echo "  make ps    Show running stack containers"
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
