import json
import time

from confluent_kafka import Consumer as ConfluentConsumer, KafkaError, KafkaException


class ConsumerManager:

    def __init__(self, bootstrap_servers = 'localhost:9092', group_id = 'transactions_consumers', client = None):
        self.client = client
        self.consumer = ConfluentConsumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',  # or 'latest' based on requirement
            'enable.auto.commit': True
        })


    def set_client(self, client):
        self.client = client


    def poll_messages(self, timeout = 8.0, duration = 20, kafka_topic = 'transactions'):
        self.consumer.subscribe([kafka_topic])
        start_time = time.time()
        while time.time() - start_time < duration:
            try:
                msg = self.consumer.poll(timeout)
                if msg is None:
                    print('No data received')
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                record = json.loads(msg.value().decode('utf-8'))
                print(f'New data received : {[record]}')
                # self.client.insert_data([record])
            except Exception as e:
                print(f"Error consuming message: {e}")


    def close_consumer(self):
        self.consumer.close()


# Example usage
if __name__ == "__main__":
    class MockClient:

        def insert_data(self, data):
            print("Data inserted:", data)


    consumer = ConsumerManager(client=MockClient())
    try:
        consumer.poll_messages()
    except KeyboardInterrupt:
        print("ConsumerManager interrupted")
    finally:
        consumer.close_consumer()
