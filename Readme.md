
---

# Real-Time Data Processing with Kafka

## Overview

This project demonstrates real-time data processing using Apache Kafka. The system generates data into CSV files, produces the data to Kafka, consumes the data from Kafka, and stores the aggregated data into MongoDB or PostgreSQL. The system could handle high throughput, processing up to 1,400,000 and more records in 30 seconds in last test.

## Project Structure

- **KafkaPyProject**: Contains the Kafka producer and consumer logic.
- **DatabasePyProject**: Handles connections and operations with MongoDB and PostgreSQL.
- **TransactionGeneratorPyProject**: Generates data in real-time and saves it to CSV files.

## Libraries Used

- `pandas~=2.2.2`
- `pymongo~=4.8.0`
- `psycopg2~=2.9.9`
- `confluent-kafka~=2.5.0`

## Setup

### Prerequisites

- Python 3.6 or above
- Apache Kafka
- MongoDB
- PostgreSQL (optional)

### Installation

1. **Clone the Repository**
   ```sh
   git clone https://github.com/your-repository-url.git
   cd your-repository-directory
   ```

2. **Install Dependencies**
   ```sh
   pip install -r requirements.txt
   ```

3. **Kafka Configuration**
   - Update Kafka broker configuration (`server.properties`):
     ```properties
     message.max.bytes=10485760  # 10 MB
     ```
   - Restart Kafka broker.

### Running the Project

1. **Generate Data**
   ```sh
   python TransactionGeneratorPyProject/generate_transactions.py
   ```

2. **Produce Data to Kafka**
   ```sh
   python KafkaPyProject/producer.py
   ```

3. **Consume Data from Kafka**
   ```sh
   python KafkaPyProject/consumer.py
   ```
4**Main project**
   ```sh
   python main.py
   ```
## Problems Addressed

- Handling large data for producer and consumer.
- Connecting and processing data from CSV files in real-time.

## Database Options

- MongoDB
- PostgreSQL

## Switching Between Databases

Modify the consumer configuration to choose between MongoDB and PostgreSQL.



---
