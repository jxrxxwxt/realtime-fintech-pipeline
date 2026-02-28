# Real-Time Fintech Data Pipeline

A fully containerized microservices architecture that simulates, streams, and ingests real-time financial transaction data. 

## Project Overview
This project demonstrates a robust, real-time data pipeline. It generates mock financial transactions using a **FastAPI** producer, streams them through **Apache Kafka**, and processes them via a dedicated **Python Consumer** before finally ingesting the records into a **PostgreSQL** database. The entire infrastructure is containerized using **Docker** and **Docker Compose** for seamless deployment and scalability.

## Tech Stack
- **Producer / Data Source:** FastAPI, Python
- **Message Broker:** Apache Kafka
- **Data Ingestion / Consumer:** Python, `confluent-kafka`, `psycopg2`
- **Database:** PostgreSQL
- **Infrastructure:** Docker, Docker Compose

## Architecture
1. **FastAPI Producer:** Exposes REST endpoints (`/start` and `/stop`) to trigger background tasks that generate random financial transactions (JSON payloads).
2. **Apache Kafka:** Acts as a high-throughput message broker. The producer publishes data to the `transactions` topic.
3. **Python Consumer:** Continuously listens to the Kafka topic, processes incoming messages, and handles database connections with a built-in retry mechanism.
4. **PostgreSQL:** Stores the final processed transaction records safely, avoiding duplicates using `ON CONFLICT DO NOTHING`.

## Project Structure
```text
realtime-fintech-pipeline/
├── consumer/
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
├── producer/
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
├── docker-compose.yml
└── README.md
```

## How to Run Locally
**Prerequisites:** Docker and Docker Compose installed on your machine.


**Step 1:** Start the Infrastructure

Clone the repository and spin up the entire microservices environment using Docker Compose:
```
docker-compose up -d --build
```
Wait a few moments for Kafka and PostgreSQL to initialize completely.


**Step 2:** Start Data Generation

Access the FastAPI Swagger UI to trigger the transaction generator:
1. Open your browser and navigate to: http://localhost:8000/docs
2. Execute the POST /start endpoint.


**Step 3:** Monitor the Pipeline

Verify the data flow by checking the consumer logs in real-time:
```
docker logs -f fintech_consumer
```


**Step 4:** Stop and Clean Up

To stop data generation, execute the POST /stop endpoint.
To shut down the entire infrastructure and remove volumes:
```
docker-compose down -v
```
