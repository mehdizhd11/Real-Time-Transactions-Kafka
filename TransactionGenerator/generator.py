import pandas as pd
import random
import time
import os
import logging
from typing import List, Dict, Optional


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class TransactionDataGenerator:

    def __init__(self, file_path: str = 'transaction_data.csv', chunk_size: int = 100, sleep_time: int = 5,
                 kafka_producer: Optional[object] = None):
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.sleep_time = sleep_time
        self.kafka_producer = kafka_producer
        self.last_sent_line = 0  # Track the last sent line
        self._initialize_csv()


    def set_kafka_producer(self, kafka_producer: object):
        self.kafka_producer = kafka_producer


    def generate_transaction_data(self, num_rows: int) -> List[Dict[str, object]]:
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


    def save_to_csv(self, data: List[Dict[str, object]]):
        df = pd.DataFrame(data)
        file_exists = os.path.isfile(self.file_path)
        try:
            with open(self.file_path, mode='a', newline='') as f:
                df.to_csv(f, index=False, header=not file_exists)
                f.flush()  # Ensure the data is written to disk immediately
        except Exception as e:
            logging.error(f"Error saving to CSV: {e}")


    def read_new_data_from_csv(self) -> List[Dict[str, object]]:
        if os.path.isfile(self.file_path):
            try:
                df = pd.read_csv(self.file_path, skiprows=lambda x: x <= self.last_sent_line and x != 0)
                self.last_sent_line += len(df)  # Update the last sent line
                return df.to_dict(orient='records')
            except Exception as e:
                logging.error(f"Error reading from CSV: {e}")
                return []
        return []


    def run(self):
        while True:
            transaction_data = self.generate_transaction_data(self.chunk_size)
            self.save_to_csv(transaction_data)
            new_data = self.read_new_data_from_csv()
            if new_data and self.kafka_producer:
                try:
                    self.kafka_producer.send_to_kafka(new_data)
                    logging.info("Data sent to Kafka successfully")
                except Exception as e:
                    logging.error(f"Error sending data to Kafka: {e}")
            else:
                logging.info('No new data or no Kafka producer')
            time.sleep(self.sleep_time)


    def _initialize_csv(self):
        if not os.path.isfile(self.file_path):
            try:
                df = pd.DataFrame(columns=['transaction_id', 'account_id', 'amount', 'timestamp', 'channel', 'location',
                                           'merchant_id', 'currency'])
                df.to_csv(self.file_path, index=False)
            except Exception as e:
                logging.error(f"Error initializing CSV: {e}")


if __name__ == '__main__':
    generator = TransactionDataGenerator()
    generator.run()
