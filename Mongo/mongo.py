from pymongo import MongoClient
import pandas as pd


class MongoDBClient:

    def __init__(self, uri = 'mongodb://localhost:27017/', database_name = 'kafka_data',
                 collection_name = 'transactions'):
        self.client = MongoClient(uri)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]


    def insert_data(self, data):
        try:
            df = pd.DataFrame(data)
            if not df.empty:
                self.collection.insert_many(df.to_dict('records'))
                print(f"Saved {len(df)} messages to MongoDB")
        except Exception as e:
            print(f"Error saving messages to MongoDB: {e}")


    def close_connection(self):
        self.client.close()
