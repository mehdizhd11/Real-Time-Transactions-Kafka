from kafka.admin import KafkaAdminClient, NewTopic


class Topic:

    def __init__(self, bootstrap_servers = 'localhost:9092', client_id = 'fraud_detection_admin'):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers, client_id=self.client_id)


    def close_admin_client(self):
        self.admin_client.close()


    def create_topic(self, name, partitions, replication_factor):
        try:
            self.start_admin_client()
            self.admin_client.create_topics(
                new_topics=[NewTopic(name=name, num_partitions=partitions, replication_factor=replication_factor)],
                validate_only=False)
            self.close_admin_client()
            return True
        except Exception as e:
            return False
