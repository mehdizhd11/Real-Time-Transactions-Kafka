import pandas as pd
import random
import time
import os


class TransactionDataGenerator:

    def __init__(self, file_path = 'transaction_data.csv', chunk_size = 100, sleep_time = 5, kafka_producer = None):
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.sleep_time = sleep_time
        self.kafka_producer = kafka_producer
        self.last_sent_line = 0  # Track the last sent line


    def set_kafka_producer(self, kafka_producer):
        self.kafka_producer = kafka_producer


    def generate_transaction_data(self, num_rows):
        transaction_data = []
        for _ in range(num_rows):
            transaction = {
                'transaction_id': random.randint(1000, 9999),
                'account_id': random.randint(1, 100),
                'amount': round(random.uniform(1.0, 1000.0), 2),
                'timestamp': int(time.time()) - random.randint(0, 100000),
                'channel': random.choice(['ATM', 'Online', 'POS']),
                'location': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
                'merchant_id': random.randint(1, 1000),
                'currency': random.choice(['USD', 'EUR', 'GBP', 'JPY', 'AUD'])
            }
            transaction_data.append(transaction)
        return transaction_data


    def save_to_csv(self, data):
        df = pd.DataFrame(data)
        file_exists = os.path.isfile(self.file_path)
        df.to_csv(self.file_path, mode='a', index=False, header=not file_exists)


    def read_new_data_from_csv(self):
        if os.path.isfile(self.file_path):
            df = pd.read_csv(self.file_path, skiprows=lambda x: x <= self.last_sent_line and x != 0)
            self.last_sent_line += len(df)  # Update the last sent line
            return df.to_dict(orient='records')
        return []


    def run(self):
        while True:
            transaction_data = self.generate_transaction_data(self.chunk_size)
            self.save_to_csv(transaction_data)
            new_data = self.read_new_data_from_csv()
            if new_data and self.kafka_producer:
                self.kafka_producer.send_to_kafka(new_data)
            else:
                print('No new data or no kafka producer')
            time.sleep(self.sleep_time)
