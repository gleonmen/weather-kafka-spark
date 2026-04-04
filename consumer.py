"""Simple Kafka consumer example.

This script connects to a local Kafka broker and consumes messages from a topic.

Usage:
  python consumer.py [--topic TOPIC] [--brokers BROKER1,BROKER2] [--group GROUP_ID]

Example:
  python consumer.py --topic test-topic --group my-group
"""

import argparse

from services.kafka_service import KafkaService
from services.weather_utils import WeatherUtils


def main() -> None:

     
    kafkaTopic = WeatherUtils.get_value_by_key(key="topic")
    brokers = WeatherUtils.get_value_by_key(key="brokers")
    group = WeatherUtils.get_value_by_key(key="group")

    parser = argparse.ArgumentParser(description="Simple Kafka consumer")
    parser.add_argument(
        "--topic",
        default=kafkaTopic,
        help="Kafka topic to consume from (default: weather-topic)",
    )
    parser.add_argument(
        "--brokers",
        default=brokers,
        help="Comma-separated list of Kafka bootstrap brokers (default: 127.0.0.1:9092)",
    )
    parser.add_argument(
        "--group",
        default=group,
        help="Consumer group id (default: weather-group)",
    )
    args = parser.parse_args()

    kafka_service = KafkaService(
        bootstrap_servers=args.brokers,
        topic=args.topic,
        group_id=args.group,
    )

    kafka_service.consume()


if __name__ == "__main__":
    main()