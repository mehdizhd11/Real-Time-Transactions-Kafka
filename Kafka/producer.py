import json
from confluent_kafka import Producer as ConfluentProducer


class ProducerManager:

    def __init__(self, bootstrap_servers = 'localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = ConfluentProducer({'bootstrap.servers': self.bootstrap_servers})


    # def delivery_report(self, err):
    #     """Delivery report callback called (from flush()) on successful or failed delivery of message"""
    #     if err is not None:
    #         print(f"Message delivery failed: {err}")
    #     else:
    #         print(f"Message delivered")


    def send_to_kafka(self, data, kafka_topic = 'transactions'):
        self.producer.produce(kafka_topic, value=json.dumps(data).encode('utf-8'))
        self.producer.flush()


    def close_producer(self):
        self.producer.flush()
        self.producer = None


# Example usage
if __name__ == "__main__":
    producer = ProducerManager()
    data = {"key": "value"}
    producer.send_to_kafka(data)
    producer.close_producer()
