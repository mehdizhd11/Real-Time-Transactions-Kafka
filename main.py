from TransactionGenerator import transaction_generator

import threading
from Kafka import topic_manager, producer_manager, consumer_manager
from Databases import mongo_client


topic_manager.create_topic(name='transactions', partitions=10, replication_factor=1)

transaction_generator.set_kafka_producer(producer_manager)

generator_thread = threading.Thread(target=transaction_generator.run(duration=30), daemon=True)
generator_thread.start()

consumer_manager.set_client(client=mongo_client)
consumer_thread = threading.Thread(target=consumer_manager.poll_messages(duration=30), daemon=True)
consumer_thread.start()

producer_manager.close_producer()
consumer_manager.close_consumer()

print(f'Kafka process finished. Num aggregated data : {mongo_client.len_collection()}')

# test 1 : Kafka process finished. Num aggregated data : 66000

# change the Kafka config to handle more data

# test 2 : Kafka process finished. Num aggregated data : 600000

# test 3 : Kafka process finished. Num aggregated data : 1,400,000 ( in 30 seconds )
