# Real-Time Streaming Pipeline — Kafka + Spark Structured Streaming

High-throughput, low-latency ingestion pipeline: simulated IoT/sensor events are published to **Apache Kafka**, consumed by **Spark Structured Streaming**, validated, enriched, aggregated in event-time windows with watermarks, and written to a Delta/Parquet sink.

## Features
- Kafka producer with JSON schema, keyed partitioning and idempotent delivery
- Structured Streaming consumer with explicit schema, **event-time watermarking** and 5-minute tumbling windows
- Bad-record routing to a dead-letter sink
- Exactly-once file sink via checkpointing
- One-command local stack with `docker-compose` (Kafka KRaft + Kafka UI)

## Architecture
```
producer ──► Kafka topic `sensor-events` ──► Spark Structured Streaming
                                              ├─► valid  ──► windowed aggregates (Delta/Parquet)
                                              └─► invalid ──► dead-letter (JSON)
```

## Quick start
```bash
docker compose up -d
pip install -r requirements.txt
python producer/event_producer.py --rate 50
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 streaming/spark_consumer.py
pytest -q
```

## Tech
Apache Kafka · Spark Structured Streaming · PySpark · Python · Docker
