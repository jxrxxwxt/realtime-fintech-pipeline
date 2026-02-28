import os
import json
import time
import random
import uuid
import logging
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks
from confluent_kafka import Producer

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Fintech Data Generator")

# Kafka configuration (Reads from environment variable, defaults to localhost)
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'transactions'

conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
producer = Producer(conf)

# Mock user data
USER_IDS = [f"U{str(i).zfill(3)}" for i in range(1, 101)]

# Global state for the generator
is_running = False

def delivery_report(err, msg):
    """Callback for Kafka producer to report message delivery status."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        try:
            payload = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Delivered to {msg.topic()} -> TXN: {payload.get('transaction_id')}")
        except json.JSONDecodeError:
            logger.info(f"Delivered to {msg.topic()}")

def generate_mock_data():
    """Background task to generate and publish mock transaction data."""
    global is_running
    logger.info("Started generating transactions.")
    
    while is_running:
        sender = random.choice(USER_IDS)
        receiver = random.choice(USER_IDS)
        while sender == receiver:
            receiver = random.choice(USER_IDS)
        
        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "sender_id": sender,
            "receiver_id": receiver,
            "amount": round(random.uniform(10.0, 5000.0), 2),
            "status": random.choices(["SUCCESS", "HOLD", "FAILED"], weights=[80, 15, 5])[0],
            "timestamp": datetime.utcnow().isoformat()
        }

        producer.produce(
            TOPIC_NAME, 
            key=transaction["transaction_id"], 
            value=json.dumps(transaction), 
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(random.uniform(0.2, 2.0))
        
    logger.info("Stopped generating transactions gracefully.")

@app.post("/start")
def start_generation(background_tasks: BackgroundTasks):
    """Endpoint to start the background data generation task."""
    global is_running
    if is_running:
        return {"status": "warning", "message": "System is already running."}
    
    is_running = True
    background_tasks.add_task(generate_mock_data)
    return {"status": "success", "message": "Data generation started."}

@app.post("/stop")
def stop_generation():
    """Endpoint to stop the background data generation task."""
    global is_running
    is_running = False
    return {"status": "success", "message": "Stopping data generation process."}

@app.get("/")
def read_root():
    """Health check endpoint."""
    return {"status": "success", "message": "API is ready. Use /start and /stop to control the generator."}