.PHONY: help up down logs ps status sample-1m sample-5m \
	produce-10k produce-100k produce-1m produce-5m \
	stream-bronze stream-bronze-reset validate-bronze \
	transform-silver validate-silver smoke-test-silver \
	transform-sessions validate-sessions smoke-test-sessions \
	smoke-test-10k smoke-test-100k quick-test local-demo-100k venv

PYTHON ?= .venv/bin/python3
SPARK_MASTER_DOCKER ?= spark://spark-master:7077
SPARK_PACKAGES ?= org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3
KAFKA_BOOTSTRAP_DOCKER ?= redpanda:9092
BRONZE_PATH_DOCKER ?= /opt/data/bronze/events
QUARANTINE_PATH_DOCKER ?= /opt/data/bronze/quarantine
CHECKPOINT_PATH_DOCKER ?= /opt/data/bronze/checkpoints/kafka_to_bronze
SILVER_PATH_DOCKER ?= /opt/data/silver/events
SESSION_EVENTS_PATH_DOCKER ?= /opt/data/silver/session_events
FCT_SESSIONS_PATH_DOCKER ?= /opt/data/gold/fct_sessions

help:
	@echo "ECommerceStream-Lakehouse — available commands"
	@echo ""
	@echo "  make venv              Create .venv and install Python dependencies"
	@echo "  make up                Start local Docker stack (Redpanda, Spark, MinIO)"
	@echo "  make down              Stop local Docker stack"
	@echo "  make ps / make status  Show running stack containers"
	@echo "  make logs              Tail Docker stack logs"
	@echo ""
	@echo "  make sample-1m         Sample 1M events -> data/raw/events_1m.csv"
	@echo "  make sample-5m         Sample 5M events -> data/raw/events_5m.csv"
	@echo ""
	@echo "  make produce-10k       Replay 10k events (fast dev loop)"
	@echo "  make produce-100k      Replay 100k events to Kafka"
	@echo "  make produce-1m        Replay 1M events (local demo)"
	@echo "  make produce-5m        Replay 5M events (local stress test)"
	@echo ""
	@echo "  make stream-bronze       Stream new Kafka offsets to bronze"
	@echo "  make stream-bronze-reset Reset checkpoint and reprocess"
	@echo "  make validate-bronze     Validate bronze Parquet"
	@echo ""
	@echo "  make transform-silver    Bronze -> silver cleaning (Spark batch)"
	@echo "  make validate-silver     Validate silver Parquet"
	@echo "  make smoke-test-silver   transform-silver + validate-silver"
	@echo ""
	@echo "  make transform-sessions  Silver -> session events + fct_sessions"
	@echo "  make validate-sessions   Validate sessionization outputs"
	@echo "  make smoke-test-sessions transform-sessions + validate-sessions"
	@echo ""
	@echo "  make quick-test          produce-10k + stream + validate"
	@echo "  make smoke-test-100k     produce-100k + stream + validate"
	@echo "  make local-demo-100k     up + smoke-test-100k"
	@echo ""
	@echo "Optional Postgres (Airflow): docker compose --profile airflow up -d"

up:
	docker compose up -d --wait

down:
	docker compose down

logs:
	docker compose logs -f

ps status:
	docker compose ps

venv:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

sample-1m:
	$(PYTHON) src/utils/sample_events.py --limit 1000000 --output data/raw/events_1m.csv

sample-5m:
	$(PYTHON) src/utils/sample_events.py --limit 5000000 --output data/raw/events_5m.csv

produce-10k:
	$(PYTHON) src/producer/replay_events.py \
		--input data/raw/events_1m.csv \
		--topic ecommerce_events \
		--limit 10000 \
		--rate-per-second 1000 \
		--log-every 2000

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

stream-bronze:
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master $(SPARK_MASTER_DOCKER) \
		--conf spark.jars.ivy=/tmp/spark-ivy \
		--packages $(SPARK_PACKAGES) \
		/opt/src/streaming/kafka_to_bronze.py \
		--kafka-bootstrap $(KAFKA_BOOTSTRAP_DOCKER) \
		--topic ecommerce_events \
		--bronze-path $(BRONZE_PATH_DOCKER) \
		--quarantine-path $(QUARANTINE_PATH_DOCKER) \
		--checkpoint-path $(CHECKPOINT_PATH_DOCKER)

stream-bronze-reset:
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master $(SPARK_MASTER_DOCKER) \
		--conf spark.jars.ivy=/tmp/spark-ivy \
		--packages $(SPARK_PACKAGES) \
		/opt/src/streaming/kafka_to_bronze.py \
		--kafka-bootstrap $(KAFKA_BOOTSTRAP_DOCKER) \
		--topic ecommerce_events \
		--bronze-path $(BRONZE_PATH_DOCKER) \
		--quarantine-path $(QUARANTINE_PATH_DOCKER) \
		--checkpoint-path $(CHECKPOINT_PATH_DOCKER) \
		--reset-checkpoint

validate-bronze:
	$(PYTHON) src/validation/validate_bronze.py --bronze-path data/bronze/events

transform-silver:
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master $(SPARK_MASTER_DOCKER) \
		/opt/src/transforms/bronze_to_silver.py \
		--bronze-path $(BRONZE_PATH_DOCKER) \
		--silver-path $(SILVER_PATH_DOCKER)

validate-silver:
	$(PYTHON) src/validation/validate_silver.py \
		--silver-path data/silver/events \
		--bronze-path data/bronze/events

smoke-test-silver:
	$(MAKE) transform-silver
	$(MAKE) validate-silver

transform-sessions:
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master $(SPARK_MASTER_DOCKER) \
		/opt/src/transforms/silver_sessionize.py \
		--silver-path $(SILVER_PATH_DOCKER) \
		--session-events-path $(SESSION_EVENTS_PATH_DOCKER) \
		--sessions-path $(FCT_SESSIONS_PATH_DOCKER)

validate-sessions:
	$(PYTHON) src/validation/validate_sessions.py \
		--silver-path data/silver/events \
		--session-events-path data/silver/session_events \
		--sessions-path data/gold/fct_sessions

smoke-test-sessions:
	$(MAKE) transform-sessions
	$(MAKE) validate-sessions

smoke-test-10k:
	$(MAKE) produce-10k
	$(MAKE) stream-bronze
	$(MAKE) validate-bronze

smoke-test-100k:
	$(MAKE) produce-100k
	$(MAKE) stream-bronze
	$(MAKE) validate-bronze

quick-test: smoke-test-10k

local-demo-100k:
	@echo "Starting local 100k streaming demo..."
	$(MAKE) up
	$(MAKE) smoke-test-100k
	@echo ""
	@echo "Local 100k demo complete."
