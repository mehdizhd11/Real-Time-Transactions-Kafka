import json

from kafka import KafkaProducer


class Producer:

    def __init__(self, bootstrap_servers = 'localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))


    def close_producer(self):
        self.producer.close()


