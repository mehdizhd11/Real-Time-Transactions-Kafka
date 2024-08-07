from kafka import KafkaConsumer
import json


class Consumer:

    def __init__(self, bootstrap_servers = 'localhost:9092', group_id = 'my_group', kafka_topic = 'transactions',
                 client = None):
        self.kafka_topic = kafka_topic
        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',  # or 'latest' based on requirement
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.client = client


    def set_client(self, client):
        self.client = client


    def poll_messages(self, timeout = 1.0):
        while True:
            messages = self.consumer.poll(timeout_ms=timeout * 2000)
            records = []
            if messages:
                for topic_partition, msgs in messages.items():
                    records.extend(msg.value for msg in msgs)
            if records:
                self.client.insert_data(records)


    def close_consumer(self):
        self.consumer.close()
