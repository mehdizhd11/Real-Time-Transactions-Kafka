import pandas as pd
import random
import time
import os
import logging
from typing import List, Dict, Optional


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class TransactionDataGenerator:

    def __init__(self, file_paths: List[str], chunk_size: int = 1000, sleep_time: int = 10,
                 kafka_producer: Optional[object] = None):
        self.file_paths = file_paths
        self.chunk_size = chunk_size
        self.sleep_time = sleep_time
        self.kafka_producer = kafka_producer
        self.last_sent_lines = {file_path: 0 for file_path in file_paths}  # Track the last sent line for each file
        self._initialize_csvs()


    def set_kafka_producer(self, kafka_producer: object):
        self.kafka_producer = kafka_producer


    def get_kafka_producer(self):
        return self.kafka_producer


    def generate_transaction_data(self, num_rows: int) -> List[Dict[str, object]]:
        transaction_data = []
        for _ in range(num_rows):
            transaction = {
                'transaction_id': str(random.randint(1000, 9999)),
                'account_id': str(random.randint(1, 100)),
                'amount': str(round(random.uniform(1.0, 1000.0), 2)),
                'timestamp': str(int(time.time()) - random.randint(0, 100000)),
                'channel': random.choice(['ATM', 'Online', 'POS']),
                'location': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
                'merchant_id': str(random.randint(1, 1000)),
                'currency': random.choice(['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'IRR'])
            }
            transaction_data.append(transaction)
        return transaction_data


    def save_to_csv(self, data: List[Dict[str, object]], file_path: str):
        df = pd.DataFrame(data)
        file_exists = os.path.isfile(file_path)
        try:
            with open(file_path, mode='a', newline='') as f:
                df.to_csv(f, index=False, header=not file_exists)
                f.flush()  # Ensure the data is written to disk immediately
        except Exception as e:
            logging.error(f"Error saving to CSV: {e}")


    def read_new_data_from_csv(self, file_path: str) -> List[Dict[str, object]]:
        if os.path.isfile(file_path):
            try:
                df = pd.read_csv(file_path, skiprows=lambda x: x <= self.last_sent_lines[file_path] and x != 0)
                self.last_sent_lines[file_path] += len(df)
                return df.to_dict(orient='records')
            except Exception as e:
                logging.error(f"Error reading from CSV: {e}")
                return []
        return []


    def run(self, duration: int = 20):
        start_time = time.time()
        while time.time() - start_time < duration:
            for file_path in self.file_paths:
                transaction_data = self.generate_transaction_data(self.chunk_size)
                self.save_to_csv(transaction_data, file_path)
                new_data = self.read_new_data_from_csv(file_path)
                if new_data and self.kafka_producer:
                    try:
                        self.kafka_producer.send_to_kafka(new_data)
                        logging.info(f"Data from {file_path} sent to Kafka successfully")
                    except Exception as e:
                        logging.error(f"Error sending data to Kafka: {e}")
                else:
                    logging.info(f'No new data or no Kafka producer for {file_path}')
            time.sleep(self.sleep_time)


    def _initialize_csvs(self):
        for file_path in self.file_paths:
            if not os.path.isfile(file_path):
                try:
                    df = pd.DataFrame(
                        columns=['transaction_id', 'account_id', 'amount', 'timestamp', 'channel', 'location',
                                 'merchant_id', 'currency'])
                    df.to_csv(file_path, index=False)
                except Exception as e:
                    logging.error(f"Error initializing CSV {file_path}: {e}")


if __name__ == '__main__':
    file_paths = ['transaction_data_1.csv', 'transaction_data_2.csv', 'transaction_data_3.csv']
    generator = TransactionDataGenerator(file_paths)
    generator.run(4)
