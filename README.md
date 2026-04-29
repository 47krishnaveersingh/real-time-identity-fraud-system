# 🚀 Real-Time Fraud Detection Pipeline (Kafka + Spark + PostgreSQL)

## 📌 Overview

This project implements a **real-time fraud detection system** using modern data engineering tools. It simulates transaction events, processes them in real time, detects anomalies, and stores flagged transactions in a database.

The pipeline is fully containerized using Docker and demonstrates **stream processing, event-driven architecture, and scalable system design**.

---

## 🏗️ Architecture

```
Producer (Python) 
      ↓
Kafka (Streaming Ingestion)
      ↓
Spark Structured Streaming (Processing Engine)
      ↓
PostgreSQL (Storage Layer)
```

---

## ⚙️ Tech Stack

* **Apache Kafka** → Event streaming platform
* **Apache Spark (Structured Streaming)** → Real-time processing
* **PostgreSQL** → Data storage
* **Docker & Docker Compose** → Containerized infrastructure
* **Python** → Producer + Spark jobs

---

## 🔥 Features

### ✅ Real-Time Streaming Pipeline

* Continuous ingestion of transaction data via Kafka
* Spark processes data in micro-batches

### ✅ Fraud Detection Logic

#### 1. High Amount Detection

* Flags transactions where:

  ```
  amount > 4000
  ```

#### 2. Velocity-Based Detection (Advanced)

* Detects multiple transactions in short time window:

  ```
  > 3 transactions in 30 seconds (per user)
  ```

* Uses:

  * Windowing
  * Watermarking
  * Stateful aggregation

---

## 📂 Project Structure

```
fraud_and_anomaly_detection/
│
├── docker/
│   ├── docker-compose.yml
│   └── kafka/
│       └── topics/
│
├── kafka/
│   └── producers/
│       └── transaction_producer.py
│
├── spark/
│   └── jobs/
│       └── streaming_job.py
│
└── airflow/ (optional)
```

---

## 🚀 How to Run

### 1️⃣ Start Infrastructure

```bash
docker-compose -f docker/docker-compose.yml up -d
```

---

### 2️⃣ Create Kafka Topics

```bash
docker exec -it docker-kafka-1 /scripts/create_topics.sh
```

---

### 3️⃣ Start Producer (Local Machine)

```bash
python kafka/producers/transaction_producer.py
```

---

### 4️⃣ Run Spark Job

```bash
docker exec -it spark /opt/spark/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0 \
/app/spark/jobs/streaming_job.py
```

---

### 5️⃣ Verify Data in PostgreSQL

```bash
docker exec -it docker-postgres-1 psql -U admin -d frauddb
```

```sql
SELECT * FROM fraud_transactions;
```

---

## 📊 Sample Output

| user_id | amount | txn_count | timestamp | fraud_type    |
| ------- | ------ | --------- | --------- | ------------- |
| user1   | 4500   | NULL      | ...       | HIGH_AMOUNT   |
| user2   | NULL   | 4         | ...       | HIGH_VELOCITY |

---

## 🧠 Key Concepts Demonstrated

* Event-driven architecture
* Distributed stream processing
* Window-based aggregations
* Watermarking for late data handling
* Micro-batch processing
* Docker-based system orchestration

---

## 👨‍💻 Author

**Krishnaveer Singh**
