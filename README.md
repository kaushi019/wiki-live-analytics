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

## 🎓 Stage-by-Stage Technical Breakdown

### Stage 1: Data Ingestion Edge (`src/kafka_producer.py`)
*   **What it does:** Establishes a permanent, streaming chunked HTTP socket connection to the Wikimedia public EventStreams API (`/v2/stream/recentchange`).
*   **How it works:** It acts as a lightweight network thread. It polls the server-sent events continually, parses the incoming JSON, filters out non-mainspace data, and isolates articles specific to the English platform domain (`en.wikipedia.org`).
*   **The Decoupling Pattern:** Instead of writing directly to a database (which creates bottlenecks), it pushes the clean raw payloads straight into an **Apache Kafka topic (`wikipedia-edits`)** acting as a resilient distributed buffer.

### Stage 2: Asynchronous Distributed Buffering (`docker-compose.yml`)
*   **What it does:** Orchestrates the infrastructure layers via **Apache Kafka** and **Zookeeper**.
*   **How it works:** Kafka keeps an on-disk, highly fault-tolerant message queue. If the database crashes or undergoes maintenance, the producer keeps pouring logs safely into Kafka without dropping data. Once the downstream scripts recover, they pick up right where they left off.

### Stage 3: AI Inference & Streaming Consumer (`src/kafka_consumer.py` & `src/ml_model.py`)
*   **What it does:** Consumes events out of Kafka queues and applies live intelligence.
*   **How it works:** It processes records through an incremental machine learning engine (**River's HalfSpaceTrees**). Unlike Scikit-Learn (which requires loading giant historical tables into memory to retrain), River scores data and updates its internal weights mathematically row-by-row on the fly. 
*   **Batching Optimization:** It maps advanced dimensions (`edit_comment`, `is_minor`, `user_edit_count`), aggregates them into a local Python list buffer, and uses transactional micro-batches (`psycopg2.extras.execute_values`) to write to **PostgreSQL** every 20 records or 5 seconds.

### Stage 4: Downstream Metrics Aggregation (`src/transformer.py`)
*   **What it does:** Computes aggregated operational dimensions.
*   **How it works:** Immediately after a database flush, this layer triggers a **Polars DataFrame** execution tree. Polars handles multi-threaded group-by aggregations in memory to figure out running stats (like distinct user counts and average size variations per page), then refreshes the `wikipedia_features` summary table.

---

## 🖥️ Streamlit Dashboard Page Explanations

When you navigate to **`http://localhost:8501`**, your business intelligence monitoring interface serves three dedicated pages:

### Page 1: Live Anomaly Stream Monitor (`app/dashboard.py`)
*   **Business Purpose:** Real-time Threat Monitoring & System Alerts.
*   **Visual Components:** High-level KPI cards (Total processed metrics, flag count totals, peak byte variations), a live updating timeline chart plotting River AI anomaly scores, and an instantaneous high-alert data grid.
*   **Value:** Acts as an operational command center. Whenever an edit scores a high probability of vandalism or bot spam (>0.75 Anomaly Score), it surfaces immediately on the screen.

### Page 2: Polars Aggregations Analytics (`app/pages/1_Polars_Analytics.py`)
*   **Business Purpose:** Volatility Analytics & Trending Content Discovery.
*   **Visual Components:** An interactive data grid highlighting the Top 50 most heavily modified articles across the entire stream runtime duration.
*   **Value:** Summarizes high-volume platform activity. By ranking items by `total_edits` and `total_bytes_changed`, operations analysts can instantly spot breaking news events or pages currently under heavy discussion/edit wars.

### Page 3: Advanced Business Insights & Search Console (`app/pages/2_Advanced_Insights.py`)
*   **Business Purpose:** Contextual Search, Bot Auditing, and Trust Profiling.
*   **Visual Components:** A text-filtering input query bar, an account-type distribution dropdown, an experience threshold slider, structural volume allocation bar charts, and a scatter chart correlating account age against AI risk scores.
*   **Value:** Solves complex data sleuthing problems. Analysts can search for intent keywords (e.g., "typo", "vand") inside the `edit_comment` column, isolate impacts caused specifically by bot infrastructure vs. humans, and visually test the risk hypothesis: *Are users with low edit histories more likely to trigger high anomaly scores?*

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

## 🛠️ Infrastructure Operations & Housekeeping

* **To clear the python internal runtime cache folders:**
  ```cmd
  del /s /q __pycache__ *.pyc
  ```
* **To stop your database completely when you are done working:**
  ```bash
  docker compose down
  ```

💼 Future Scope : The Business Analytics Use Cases We Can Solve

By pulling in richer fields, our Polars analytics layer and River AI layer can solve three distinct enterprise problem domains:

1. Bad-Actor Risk Profiling (Trust & Safety Analytics)

• Business Problem: Attackers open sleeper accounts to systematically inject spam links, hidden advertising, or coordinate vandalism across a site.
• The Rich Dimensions: user_registration_dt (Account age) and user_edit_count (Historical account experience level).
• The Analytic Metric: New accounts (under 24 hours old) making significant edits have a 10x higher operational risk score.

2. Intent Classification via NLP (Product Operations Analytics)

• Business Problem: Understanding why users are modifying items in real time to categorize platform traffic variations.
• The Rich Dimension: edit_comment (The text summary explaining the change reason).
• The Analytic Metric: Polars will extract structural intent categories (e.g., matching keywords like "fixed typo", "reverted vandalism", "added reference") to profile total platform modification motivations.

3. Content Integrity Auditing (Version Control Analytics)

• Business Problem: Tracking sequence lineage to handle rollsbacks, data drift audits, and structural edit patterns.
• The Rich Dimensions: revision_id, parent_revision_id, and is_minor.
