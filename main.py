from TransactionGenerator import transaction_generator

import threading
from Kafka import topic, producer, consumer
from Mongo import mongo_client


topic.create_topic(name='transaction-data', partitions=4, replication_factor=1)

transaction_generator.set_kafka_producer(producer.producer)

generator_thread = threading.Thread(target=transaction_generator.run(), daemon=True)
generator_thread.start()

consumer.set_client(client=mongo_client)
