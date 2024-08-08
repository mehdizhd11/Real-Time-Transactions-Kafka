from pymongo import MongoClient
import pandas as pd
import logging


logging.basicConfig(level=logging.INFO)


class MongoDBClient:

    def __init__(self, uri = 'mongodb://localhost:27017/', database_name = 'kafka_data',
                 collection_name = 'transactions'):
        self.client = MongoClient(uri)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]
        logging.info("Connected to MongoDB")


    def ensure_string_keys(self, data):
        if isinstance(data, dict):
            return {str(k): v for k, v in data.items()}
        elif isinstance(data, list):
            return [self.ensure_string_keys(item) for item in data]
        else:
            return data


    def insert_data(self, data):
        try:
            data = self.ensure_string_keys(data)
            df = pd.DataFrame(data)
            if not df.empty:
                self.collection.insert_many(df.to_dict('records'))
                logging.info(f"Saved {len(df)} messages to MongoDB")
            else:
                logging.warning("Empty DataFrame, no data inserted")
        except Exception as e:
            logging.error(f"Error saving messages to MongoDB: {e}")


    def len_collection(self):
        return self.collection.count_documents({})


    def close_connection(self):
        self.client.close()
        logging.info("Closed MongoDB connection")


# Example usage
if __name__ == "__main__":
    class MockClient:

        def insert_data(self, data):
            print("Data inserted:", data)


    client = MongoDBClient()
    sample_data = [{'field1': 'value1', 'field2': 'value2'}, {'field1': 'value3', 'field2': 'value4'}]
    client.insert_data(sample_data)
    client.close_connection()
