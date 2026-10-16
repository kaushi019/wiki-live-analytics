# 📡 Wikipedia Live Stream AI Anomaly Monitor (Dockerized Kafka Stack)

A production-grade, multi-tier enterprise data engineering streaming pipeline. This application captures live global English Wikipedia article logs via Server-Sent Events (SSE), brokers data payloads asynchronously inside **Apache Kafka queues**, applies incremental online anomaly detection algorithms with **River ML**, aggregates metrics in real time via **Polars DataFrames**, stores structured results inside **PostgreSQL**, and serves an interactive web monitoring dashboard with **Streamlit**.

---

## 🏗️ Multi-Container Cluster Layout
```text
wiki-live-analytics/
│
├── app/
│   ├── dashboard.py             # Main Page: Live Anomaly Stream Monitor
│   └── pages/
│       ├── 1_Polars_Analytics.py # Page 2: Rolling metrics aggregates
│       └── 2_Advanced_Insights.py# Page 3: Search Engine & Bot Analytics
│
├── src/
│   ├── db_manager.py            # PostgreSQL schema integrations
│   ├── transformer.py           # Polars analytics calculations
│   ├── ml_model.py              # River streaming anomaly engine logic
│   ├── kafka_producer.py        # Event producer streaming pipeline nodes
│   └── kafka_consumer.py        # Kafka AI evaluation ingestion processors
│
├── Dockerfile                   # Python container compiler blueprints
├── docker-compose.yml           # Unified infrastructure cluster map configuration
├── requirements.txt             # Project library requirements sheet
├── run_cluster.py               # Parallel container initialization orchestrator
└── README.md                    # System execution documentation file
```

---

## ⚡ The Single-Command Deployment Guide

To initialize and deploy the entire multi-service container cluster architecture locally on your device, execute this command inside your terminal:

```bash
# Downloads images, builds the custom app container, and runs all 4 modules simultaneously
docker compose up --build
```

*Once the terminal finishes logging its warm-up parameters, open your browser and access your complete interactive analytics command dashboard at:*
👉 **`http://localhost:8501`**

---

## 🔍 Database Inspection & Queries

If you use **DBeaver** or a built-in IDE console connection panel, configure your link credentials to target `localhost` on Port `5432` with User `postgres` and Password `your_secure_password`. You can execute these test scripts to monitor the pipeline:

### 🚨 Query A: View the Latest Anomalies Caught by River AI
```sql
SELECT page_title, user_name, bytes_changed, anomaly_score, ingested_at
FROM wikipedia_raw
WHERE anomaly_score > 0.75
ORDER BY id DESC
LIMIT 10;
```

### ⚙️ Query B: Check Polars Real-Time Aggregated Insights
```sql
SELECT page_title, total_edits, unique_users, total_bytes_changed, avg_bytes_per_edit
FROM wikipedia_features
ORDER BY total_edits DESC
LIMIT 10;
```

---

## 🛠️ Infrastructure Operations & Housekeeping

* **To clear the python internal runtime cache folders:**
  ```cmd
  del /s /q __pycache__ *.pyc
  ```
* **To stop your database completely when you are done working:**
  ```bash
  docker compose down
  ```
