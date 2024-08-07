import pandas as pd
import random
import time


# Function to generate random transaction data
def generate_transaction_data(num_rows):
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


# Generate 100 rows of transaction data
num_rows = 2000001
transaction_data = generate_transaction_data(num_rows)

# Create a DataFrame
df = pd.DataFrame(transaction_data)

# Save to CSV
file_path = 'transaction_data.csv'
df.to_csv(file_path, index=True)
