import json
from kafka import KafkaConsumer, KafkaProducer

class KafkaService:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str = "default-group"):
        self.bootstrap_servers = bootstrap_servers.split(",")
        self.topic = topic
        self.group_id = group_id
        self.producer = None
        self.consumer = None

    def produce(self, key: str, value: dict) -> None:
        """Produce a message to the Kafka topic."""
        if not self.producer:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                api_version='auto',
            )
        self.producer.send(self.topic, key=key, value=value)
        self.producer.flush()
        print(f"Produced: key={key}, value={value}")

    def consume(self) -> None:
        """Consume messages from the Kafka topic."""
        if not self.consumer:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                api_version='auto',
            )
        try:
            print(f"Consuming from topic '{self.topic}' (group: {self.group_id})")
            for message in self.consumer:
                print(
                    f"topic={message.topic} partition={message.partition} offset={message.offset} key={message.key} value={message.value}"
                )
        except KeyboardInterrupt:
            print("Interrupted by user")
        finally:
            if self.consumer:
                self.consumer.close()