import json
from kafka import KafkaProducer


class Producer:

    def __init__(self, bootstrap_servers = 'localhost:9092', kafka_topic = 'transactions'):
        self.bootstrap_servers = bootstrap_servers
        self.kafka_topic = kafka_topic
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )


    def send_to_kafka(self, data):
        self.producer.send(self.kafka_topic, value=data)
        self.producer.flush()


    def close_producer(self):
        self.producer.close()
