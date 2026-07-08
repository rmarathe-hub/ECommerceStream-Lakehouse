.PHONY: help up down logs

help:
	@echo "ECommerceStream-Lakehouse — available commands"
	@echo ""
	@echo "  make up    Start local Docker stack (Redpanda, Spark, MinIO)"
	@echo "  make down  Stop local Docker stack"
	@echo "  make logs  Tail Docker stack logs"

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f
