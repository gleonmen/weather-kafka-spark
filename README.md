# Kafka Producer (Python)

This repo includes a small Python script that produces messages to a Kafka topic running in Docker.

## Prerequisites

- Docker & Docker Compose installed
- Python 3.8+ installed

## Start Kafka

From the repo root:

```bash
docker compose up -d
```

Kafka will be available at `127.0.0.1:9092` (use this for both scripts and UI).

## Install Python dependencies

```bash
python -m pip install -r requirements.txt
```

## Run the producer

```bash
python producer.py --topic test-topic --count 10
```

You can override the Kafka broker list:

```bash
python producer.py --brokers 127.0.0.1:9092 --topic test-topic --count 20
```

## Run the consumer (for local testing)

Consume messages previously produced to a topic:

```bash
python consumer.py --topic test-topic --group my-group
```

You can override the Kafka broker list:

```bash
python consumer.py --brokers host.docker.internal:9092 --topic test-topic --group my-group
```

## Verify messages

You can use a Kafka client (e.g., `kafka-console-consumer`), or open the Kafka UI at `http://localhost:8090`.
- Select the `local` cluster.
- Go to **Topics** tab → select `test-topic`.
- Go to **Messages** to view the sent messages.
