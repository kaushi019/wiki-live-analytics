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
│       ├── 1_Polars_Analytics.py # Page 2: Rolling metrics aggregates (NLP Intents)
│       └── 2_Advanced_Insights.py# Page 3: Search Engine & Volatility Analytics
│
├── src/
│   ├── db_manager.py            # PostgreSQL schema integrations
│   ├── transformer.py           # Polars business analytics calculations (Regex NLP)
│   ├── ml_model.py              # River streaming anomaly engine logic (Risk Profiling)
│   ├── kafka_producer.py        # Crash-proof event producer stream nodes
│   └── kafka_consumer.py        # Synchronized AI evaluation ingestion processors
│
├── Dockerfile                   # Python container compiler blueprints
├── docker-compose.yml           # Unified infrastructure cluster map configuration
├── requirements.txt             # Project library requirements sheet
├── run_cluster.py               # Parallel container initialization orchestrator
└── README.md                    # System execution documentation file
```

---

## 🎓 Stage-by-Stage Technical Breakdown

### Stage 1: Data Ingestion Edge & Reconnection Loop (`src/kafka_producer.py`)
*   **What it does:** Establishes a permanent, streaming chunked HTTP socket connection to the Wikimedia public EventStreams API (`/v2/stream/recentchange`).
*   **How it works:** It handles continuous server-sent events, parses incoming JSON payloads, filters out non-mainspace data, and isolates English domain articles (`en.wikipedia.org`).
*   **Self-Healing Resilience:** It features an infinite stream reconnection loop. If Wikipedia's public API prematurely drops the active chunk response (`Response ended prematurely`), the script flushes remaining buffers, waits 5 seconds, and automatically restores the tunnel without stopping the container.
*   **The Decoupling Pattern:** Clean raw records are published immediately into an **Apache Kafka topic (`wikipedia-edits`)** acting as an asynchronous message queue.

### Stage 2: Asynchronous Distributed Buffering (`docker-compose.yml`)
*   **What it does:** Orchestrates the infrastructure container nodes via **Apache Kafka** and **Zookeeper**.
*   **How it works:** Kafka holds an on-disk, fault-tolerant messaging queue using dual-network listener topologies (`localhost:9092` for external inspection tools like DBeaver and `kafka:29092` for internal docker container routing). This decouples the network ingestion layer from the downstream computing layer.

### Stage 3: Multi-Dimensional AI Inference Consumer (`src/kafka_consumer.py` & `src/ml_model.py`)
*   **What it does:** Consumes events from Kafka queues and computes real-time behavioral risk profiles.
*   **How it works:** Records are evaluated by an incremental machine learning pipeline (**River's HalfSpaceTrees**). It solves a critical **Trust & Safety business problem** by extracting four dimensions on the fly: content size deviations (`bytes_changed`), automated flags (`is_bot`), content revision classifications (`is_minor`), and author experience levels (`user_edit_count`). 
*   **Batching Optimization:** It buffers processed rows and uses transactional micro-batches (`execute_values`) to write clean arrays to **PostgreSQL** every 20 records or 5 seconds.

### Stage 4: NLP Intent Categorization via Polars (`src/transformer.py`)
*   **What it does:** Solves **Product Operations Use Cases** via automated change-intent text classification.
*   **How it works:** Immediately after a database flush, this layer spins up a multi-threaded **Polars DataFrame** execution tree. It utilizes regex parsing patterns over the `edit_comment` field to extract and categorize structural user intents (e.g., matching keywords to tally `typo_fixes_count` and `vandalism_reversals_count`), while tracking running averages of editor experience levels across active article targets.

---

## 🖥️ Streamlit Dashboard Page Explanations

When you deploy the container cluster, your business intelligence monitoring interface serves three optimized web layouts at **`http://localhost:8501`**:

### Page 1: Live Anomaly Stream Monitor (`app/dashboard.py`)
*   **Business Purpose:** Real-time Operational Incident Monitoring & Critical Alerts.
*   **Components:** High-level summary metric cards, a live timeline chart tracking River AI anomaly scores, and a high-alert datagrid displaying edits with an anomaly threshold score >0.75.
*   **Value:** Surfaces immediate platform risk. It scales dynamically to catch extreme size modifications or rogue bot behavior as edits flow from Kafka into PostgreSQL.

### Page 2: Polars Aggregations Analytics (`app/pages/1_Polars_Analytics.py`)
*   **Business Purpose:** Automated Content Integrity Auditing & Intent Profiling.
*   **Components:** An interactive data grid detailing the Top 50 most heavily modified articles across the entire streaming lifecycle duration.
*   **Value:** Evaluates platform moderation velocity. It exposes running counts of **✍️ Typo Fixes** and **🛡️ Vandalism Reversals** extracted by Polars NLP string matching, allowing teams to spot trending discussions and track editor experience histories.

### Page 3: Advanced Business Insights & Search Console (`app/pages/2_Advanced_Insights.py`)
*   **Business Purpose:** Granular Trust Profiling & Autocomplete Query Console.
*   **Components:** A searchable autocomplete dropdown menu listing unique article titles, account type filters, minimum editor experience sliders, layered Altair charts displaying absolute tallies with exact count placeholders, and a risk scatter plot.
*   **Value:** Pinpoints malicious behavior. Analysts can type keywords to autocomplete titles, audit bot-to-human activity volume, and visually analyze the risk hypothesis: *Are users with low system edit histories more likely to trigger high volatility anomaly flags?*

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

If you use **DBeaver** or a built-in IDE console connection panel, configure your link credentials to target `localhost` on Port `5432` with User `postgres` and Password `your_secure_password` to query the data:

```sql
-- View the latest anomalies caught by the River ML engine
SELECT page_title, user_name, bytes_changed, anomaly_score, edit_comment, ingested_at
FROM wikipedia_raw
WHERE anomaly_score > 0.75
ORDER BY id DESC LIMIT 10;
```

---

## 🛠️ Infrastructure Operations & Housekeeping

* **To clear the python internal runtime cache folders:**
  ```cmd
  del /s /q __pycache__ *.pyc
  ```
* **To stop your database and broker queues completely when you are done working:**
  ```bash
  docker compose down
  ```
