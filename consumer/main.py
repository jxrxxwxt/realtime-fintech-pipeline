import json
import logging
import psycopg2
from confluent_kafka import Consumer, KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fintech_transaction_group',
    'auto.offset.reset': 'earliest'
}
TOPIC_NAME = 'transactions'

# PostgreSQL Configuration
DB_CONFIG = {
    'dbname': 'transaction_db',
    'user': 'admin',
    'password': 'adminpassword',
    'host': 'localhost',
    'port': '5433'
}

def init_db(conn):
    """Initialize database and create the transactions table if it does not exist."""
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id VARCHAR(50) PRIMARY KEY,
        sender_id VARCHAR(50),
        receiver_id VARCHAR(50),
        amount NUMERIC(10, 2),
        status VARCHAR(20),
        timestamp TIMESTAMP
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    logger.info("Database initialized and table structure is verified.")

def insert_transaction(conn, data):
    """Insert a processed transaction record into the database."""
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO transactions (transaction_id, sender_id, receiver_id, amount, status, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (transaction_id) DO NOTHING;
    """
    cursor.execute(insert_query, (
        data.get('transaction_id'),
        data.get('sender_id'),
        data.get('receiver_id'),
        data.get('amount'),
        data.get('status'),
        data.get('timestamp')
    ))
    conn.commit()
    cursor.close()

def start_consumer():
    """Main loop to consume messages from Kafka and ingest them into PostgreSQL."""
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC_NAME])
    
    conn = None
    try:
        # Establish database connection
        conn = psycopg2.connect(**DB_CONFIG)
        init_db(conn)
        
        logger.info(f"Subscribed to Kafka topic: '{TOPIC_NAME}'. Awaiting messages...")
        
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event, not an error
                    continue
                else:
                    logger.error(f"Kafka error occurred: {msg.error()}")
                    break
            
            try:
                # Decode and parse the message
                record_value = msg.value().decode('utf-8')
                transaction_data = json.loads(record_value)
                
                # Insert into database
                insert_transaction(conn, transaction_data)
                logger.info(f"Successfully processed and saved TXN: {transaction_data.get('transaction_id')} | Amount: {transaction_data.get('amount')}")
                
            except json.JSONDecodeError:
                logger.error("Failed to decode JSON message payload.")
            except Exception as e:
                logger.error(f"Unexpected error during message processing: {e}")

    except KeyboardInterrupt:
        logger.info("Interrupt signal received. Initiating graceful shutdown sequence.")
    finally:
        # Resource cleanup
        if conn is not None:
            conn.close()
            logger.info("PostgreSQL connection closed.")
        consumer.close()
        logger.info("Kafka consumer instance terminated.")

if __name__ == "__main__":
    start_consumer()