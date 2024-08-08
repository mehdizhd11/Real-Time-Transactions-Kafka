from TransactionGenerator import transaction_generator

import threading
from Kafka import topic_manager, producer_manager, consumer_manager
from Databases import mongo_client


topic_manager.create_topic(name='transactions', partitions=1, replication_factor=1)

transaction_generator.set_kafka_producer(producer_manager)

generator_thread = threading.Thread(target=transaction_generator.run(), daemon=True)
generator_thread.start()

consumer_manager.set_client(client=mongo_client)
consumer_thread = threading.Thread(target=consumer_manager.poll_messages(), daemon=True)
consumer_thread.start()

producer_manager.close_producer()
consumer_manager.close_consumer()

print('Application finished')
