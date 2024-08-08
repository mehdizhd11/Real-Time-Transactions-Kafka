from confluent_kafka.admin import AdminClient, NewTopic
import logging


# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TopicManager:

    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})


    def create_topic(self, name: str, partitions: int, replication_factor: int) -> bool:
        new_topic = NewTopic(name, num_partitions=partitions, replication_factor=replication_factor)
        futures = self.admin_client.create_topics([new_topic])

        for topic, future in futures.items():
            try:
                future.result()  # The result itself is None
                logger.info(f"TopicManager '{name}' created successfully.")
                return True
            except Exception as e:
                logger.error(f"Failed to create topic '{name}': {e}")
                return False


    def delete_topic(self, name: str) -> bool:
        futures = self.admin_client.delete_topics([name])

        for topic, future in futures.items():
            try:
                future.result()  # The result itself is None
                logger.info(f"TopicManager '{name}' deleted successfully.")
                return True
            except Exception as e:
                logger.error(f"Failed to delete topic '{name}': {e}")
                return False


    def list_topics(self) -> list:
        # Fetch cluster metadata
        metadata = self.admin_client.list_topics(timeout=10)

        # Get all topics
        topics = metadata.topics

        return topics


# Example usage
if __name__ == "__main__":
    topic_manager = TopicManager()

    # Create a topic
    create_result = topic_manager.create_topic(name="transactions", partitions=3, replication_factor=1)
    if create_result:
        logger.info("TopicManager creation was successful.")
    else:
        logger.info("TopicManager creation failed.")

    # Delete a topic
    # delete_result = topic_manager.delete_topic(name="example_topic")
    # if delete_result:
    #     logger.info("TopicManager deletion was successful.")
    # else:
    #     logger.info("TopicManager deletion failed.")

    # for t in topic_manager.list_topics():
    #     topic_manager.delete_topic(name=t)

    print(topic_manager.list_topics())
