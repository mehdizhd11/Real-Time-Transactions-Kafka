import psycopg2
import pandas as pd
import logging


# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgreSQLClient:

    def __init__(self, uri = 'dbname=kafka_data user=mehdipsql password=mehdi1382 host=localhost port=5432'):
        try:
            self.connection = psycopg2.connect(uri)
            self.cursor = self.connection.cursor()
            logger.info("Connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise


    def insert_data(self, data, table_name = 'transactions'):
        try:
            df = pd.DataFrame(data)
            if not df.empty:
                cols = ','.join(list(df.columns))
                values = [tuple(x) for x in df.to_numpy()]
                placeholders = ','.join(['%s' for _ in df.columns])
                query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
                self.cursor.executemany(query, values)
                self.connection.commit()
                logger.info(f"Saved {len(df)} records to PostgreSQL table '{table_name}'")
            else:
                logger.warning("Empty DataFrame, no data inserted")
        except Exception as e:
            logger.error(f"Error saving records to PostgreSQL: {e}")
            self.connection.rollback()


    def close_connection(self):
        try:
            self.cursor.close()
            self.connection.close()
            logger.info("Closed PostgreSQL connection")
        except Exception as e:
            logger.error(f"Error closing PostgreSQL connection: {e}")


# Example usage
if __name__ == "__main__":
    client = PostgreSQLClient()
    sample_data = [{'field1': 'value1', 'field2': 'value2'}, {'field1': 'value3', 'field2': 'value4'}]
    client.insert_data(table_name='transactions', data=sample_data)
    client.close_connection()
