"""Simple Kafka producer example.

This script connects to a local Kafka broker and publishes messages to a topic.

Usage:
  python producer.py [--topic TOPIC] [--brokers BROKER1,BROKER2] [--count N]

Example:
  python producer.py --topic test-topic --count 10
"""

import argparse
import json
import time

from kafka import KafkaProducer


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create a Kafka producer.

    Args:
        bootstrap_servers: Comma-separated list of broker addresses (e.g., localhost:9092).

    Returns:
        KafkaProducer instance.
    """

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        api_version=(3, 0, 0),
        linger_ms=10,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple Kafka producer")
    parser.add_argument(
        "--topic",
        default="test-topic",
        help="Kafka topic to produce to (default: test-topic)",
    )
    parser.add_argument(
        "--brokers",
        default="localhost:9092",
        help="Comma-separated list of Kafka bootstrap brokers (default: localhost:9092)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of messages to send (default: 10)",
    )
    args = parser.parse_args()

    producer = build_producer(args.brokers)

    try:
        for i in range(1, args.count + 1):
            payload = {
                "id": i,
                "timestamp": time.time(),
                "message": f"hello kafka {i}",
            }

            future = producer.send(
                args.topic,
                key=f"key-{i}",
                value=payload,
            )
            record_metadata = future.get(timeout=10)

            print(
                f"Sent message {i} to {record_metadata.topic} partition={record_metadata.partition} offset={record_metadata.offset}"
            )

        # Ensure all buffered messages are sent before exiting.
        producer.flush()
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
