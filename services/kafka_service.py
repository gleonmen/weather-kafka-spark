import json
from kafka import KafkaConsumer, KafkaProducer

class KafkaService:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str = "default-group"):
        self.bootstrap_servers = bootstrap_servers.split(",")
        self.topic = topic
        self.group_id = group_id
        self.producer = None
        self.consumer = None

    def _get_producer(self) -> KafkaProducer:
        if not self.producer:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                api_version=(3, 0, 0),
                linger_ms=10,
            )
        return self.producer

    def produce(self, key: str, value: dict) -> None:
        """Produce a message to the Kafka topic."""
        self.produce_to_topic(topic=self.topic, key=key, value=value)

    def produce_to_topic(self, topic: str, key: str, value: dict) -> None:
        producer = self._get_producer()
        producer.send(topic, key=key, value=value)
        producer.flush()
        print(f"Produced: topic={topic} key={key}, value={value}")

    def consume(self, forward_topic: str = None) -> None:
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
        if forward_topic and forward_topic == self.topic:
            print("forward_topic is the same as source topic. Forwarding disabled to avoid loops.")
            forward_topic = None
        try:
            print(f"Consuming from topic '{self.topic}' (group: {self.group_id})")
            for message in self.consumer:
                print(
                    f"topic={message.topic} partition={message.partition} offset={message.offset} key={message.key} value={message.value}"
                )
                if forward_topic and message.value is not None:
                    self.produce_to_topic(
                        topic=forward_topic,
                        key=message.key,
                        value=message.value,
                    )
        except KeyboardInterrupt:
            print("Interrupted by user")
        finally:
            if self.consumer:
                self.consumer.close()
