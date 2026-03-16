"""Simple Kafka consumer example.

This script connects to a local Kafka broker and consumes messages from a topic.

Usage:
  python consumer.py [--topic TOPIC] [--brokers BROKER1,BROKER2] [--group GROUP_ID]

Example:
  python consumer.py --topic test-topic --group my-group
"""

import argparse
import json

from kafka import KafkaConsumer


def build_consumer(bootstrap_servers: str, topic: str, group_id: str) -> KafkaConsumer:
    """Create a Kafka consumer.

    Args:
        bootstrap_servers: Comma-separated list of broker addresses (e.g., localhost:9092).
        topic: Topic to subscribe to.
        group_id: Consumer group id.

    Returns:
        KafkaConsumer instance.
    """

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers.split(","),
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        api_version=(3, 0, 0),
    )

    return consumer


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple Kafka consumer")
    parser.add_argument(
        "--topic",
        default="test-topic",
        help="Kafka topic to consume from (default: test-topic)",
    )
    parser.add_argument(
        "--brokers",
        default="localhost:9092",
        help="Comma-separated list of Kafka bootstrap brokers (default: localhost:9092)",
    )
    parser.add_argument(
        "--group",
        default="python-consumer",
        help="Consumer group id (default: python-consumer)",
    )
    args = parser.parse_args()

    consumer = build_consumer(args.brokers, args.topic, args.group)

    try:
        print(f"Consuming from topic '{args.topic}' (group: {args.group})")
        for message in consumer:
            print(
                f"topic={message.topic} partition={message.partition} offset={message.offset} key={message.key} value={message.value}"
            )
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
