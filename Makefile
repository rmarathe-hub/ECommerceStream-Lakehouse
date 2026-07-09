.PHONY: help up down logs ps status sample-1m sample-5m \
	produce-10k produce-100k produce-1m produce-5m \
	stream-bronze stream-bronze-reset validate-bronze \
	transform-silver validate-silver smoke-test-silver \
	transform-sessions validate-sessions smoke-test-sessions \
	transform-purchase-marts validate-purchase-marts smoke-test-purchase-marts \
	transform-funnel-marts validate-funnel-marts smoke-test-funnel-marts \
	validate-pipeline validate-gold transform-gold reset-demo-state wait-for-stack verify-1m quality-gate \
	smoke-test-10k smoke-test-100k quick-test local-demo-100k local-demo-1m venv

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
FCT_PURCHASES_PATH_DOCKER ?= /opt/data/gold/fct_purchases
AGG_PRODUCT_PATH_DOCKER ?= /opt/data/gold/agg_product_performance
AGG_FUNNEL_PATH_DOCKER ?= /opt/data/gold/agg_conversion_funnel
FCT_CART_ABANDONMENT_PATH_DOCKER ?= /opt/data/gold/fct_cart_abandonment
MIN_BRONZE_ROWS ?= 1
MIN_SILVER_ROWS ?= 1
MIN_SESSIONS ?= 1
STACK_WAIT_TIMEOUT ?= 120
STACK_WAIT_INTERVAL ?= 3

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
	@echo "  make transform-purchase-marts  Session events -> purchase + product marts"
	@echo "  make validate-purchase-marts Validate purchase and product gold tables"
	@echo "  make smoke-test-purchase-marts transform-purchase-marts + validate"
	@echo ""
	@echo "  make transform-funnel-marts   Build conversion funnel + cart abandonment marts"
	@echo "  make validate-funnel-marts    Validate funnel and abandonment outputs"
	@echo "  make smoke-test-funnel-marts  transform-funnel-marts + validate"
	@echo ""
	@echo "  make validate-pipeline   Full DQ across bronze/silver/gold"
	@echo "  make validate-gold       Gold-only DQ + cross-layer checks"
	@echo "  make transform-gold      Run all gold transforms"
	@echo ""
	@echo "  make quick-test          produce-10k + stream + validate"
	@echo "  make smoke-test-100k     produce-100k + stream + validate"
	@echo "  make local-demo-100k     up + smoke-test-100k"
	@echo "  make local-demo-1m       Full 1M medallion demo (local only)"
	@echo "  make verify-1m           DQ check on existing 1M pipeline (~1 min)"
	@echo "  make quality-gate        Full local Weeks 1–2 quality gate (~2–3 min)"
	@echo "  make reset-demo-state    Wipe pipeline outputs for a clean demo run"
	@echo ""
	@echo "Optional Postgres (Airflow): docker compose --profile airflow up -d"

up:
	docker compose up -d
	$(MAKE) wait-for-stack

wait-for-stack:
	@chmod +x scripts/wait_for_stack.sh
	@STACK_WAIT_TIMEOUT=$(STACK_WAIT_TIMEOUT) STACK_WAIT_INTERVAL=$(STACK_WAIT_INTERVAL) \
		./scripts/wait_for_stack.sh

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
	docker exec -u 0 spark-master sh -c 'mkdir -p /tmp/spark-ivy/cache && chown -R spark:spark /tmp/spark-ivy'
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
	docker exec -u 0 spark-master sh -c 'mkdir -p /tmp/spark-ivy/cache && chown -R spark:spark /tmp/spark-ivy'
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

transform-purchase-marts:
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master $(SPARK_MASTER_DOCKER) \
		/opt/src/transforms/build_purchase_product_marts.py \
		--session-events-path $(SESSION_EVENTS_PATH_DOCKER) \
		--purchases-path $(FCT_PURCHASES_PATH_DOCKER) \
		--product-performance-path $(AGG_PRODUCT_PATH_DOCKER)

validate-purchase-marts:
	$(PYTHON) src/validation/validate_purchase_marts.py \
		--session-events-path data/silver/session_events \
		--purchases-path data/gold/fct_purchases \
		--product-performance-path data/gold/agg_product_performance

smoke-test-purchase-marts:
	$(MAKE) transform-purchase-marts
	$(MAKE) validate-purchase-marts

transform-funnel-marts:
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master $(SPARK_MASTER_DOCKER) \
		/opt/src/transforms/build_funnel_marts.py \
		--session-events-path $(SESSION_EVENTS_PATH_DOCKER) \
		--sessions-path $(FCT_SESSIONS_PATH_DOCKER) \
		--funnel-path $(AGG_FUNNEL_PATH_DOCKER) \
		--cart-abandonment-path $(FCT_CART_ABANDONMENT_PATH_DOCKER)

validate-funnel-marts:
	$(PYTHON) src/validation/validate_funnel_marts.py \
		--sessions-path data/gold/fct_sessions \
		--funnel-path data/gold/agg_conversion_funnel \
		--cart-abandonment-path data/gold/fct_cart_abandonment

smoke-test-funnel-marts:
	$(MAKE) transform-funnel-marts
	$(MAKE) validate-funnel-marts

transform-gold:
	$(MAKE) transform-sessions
	$(MAKE) transform-purchase-marts
	$(MAKE) transform-funnel-marts

validate-pipeline:
	$(PYTHON) src/validation/validate_pipeline.py \
		--min-bronze-rows $(MIN_BRONZE_ROWS) \
		--min-silver-rows $(MIN_SILVER_ROWS) \
		--min-sessions $(MIN_SESSIONS)

validate-gold:
	$(PYTHON) src/validation/validate_pipeline.py --gold-only

reset-demo-state:
	@echo "Resetting Kafka topic and local pipeline outputs..."
	docker exec redpanda rpk topic delete ecommerce_events 2>/dev/null || true
	docker exec redpanda rpk topic create ecommerce_events
	rm -rf data/bronze/events data/bronze/quarantine data/bronze/checkpoints
	rm -rf data/silver/events data/silver/session_events
	rm -rf data/gold/fct_sessions data/gold/fct_purchases data/gold/agg_product_performance
	rm -rf data/gold/agg_conversion_funnel data/gold/fct_cart_abandonment data/gold/dq_pipeline_summary.json
	mkdir -p data/bronze/events data/bronze/quarantine data/bronze/checkpoints/kafka_to_bronze
	mkdir -p data/silver/events data/silver/session_events
	mkdir -p data/gold/fct_sessions data/gold/fct_purchases data/gold/agg_product_performance
	mkdir -p data/gold/agg_conversion_funnel data/gold/fct_cart_abandonment
	@echo "Demo state reset complete."

local-demo-1m:
	@echo "Starting local 1M medallion demo..."
	@test -f data/raw/events_1m.csv || (echo "Missing data/raw/events_1m.csv — run: make sample-1m" && exit 1)
	$(MAKE) up
	$(MAKE) reset-demo-state
	@echo ""
	@echo "Step 1/5: produce 1M events to Kafka (~15-20 min)..."
	$(MAKE) produce-1m
	@echo ""
	@echo "Step 2/5: stream bronze..."
	$(MAKE) stream-bronze
	@echo ""
	@echo "Step 3/5: transform silver..."
	$(MAKE) transform-silver
	@echo ""
	@echo "Step 4/5: transform gold marts..."
	$(MAKE) transform-gold
	@echo ""
	@echo "Step 5/5: validate full pipeline..."
	$(MAKE) validate-pipeline MIN_BRONZE_ROWS=1000000 MIN_SILVER_ROWS=1000000 MIN_SESSIONS=1
	@echo ""
	@echo "Local 1M demo complete."

verify-1m:
	@echo "Verifying existing 1M pipeline outputs (no Kafka replay)..."
	$(MAKE) validate-pipeline MIN_BRONZE_ROWS=1000000 MIN_SILVER_ROWS=1000000 MIN_SESSIONS=1

quality-gate:
	@chmod +x scripts/run_local_quality_gate.sh
	@PYTHON=$(PYTHON) ./scripts/run_local_quality_gate.sh

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
