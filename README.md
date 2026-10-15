# 📡 Wikipedia Live Stream AI Anomaly Monitor

An end-to-end real-time data streaming and machine learning pipeline. This architecture captures live global English Wikipedia article updates directly via Server-Sent Events (SSE), queues them into structured micro-batches, persists them inside a transactional relational database (**PostgreSQL**), applies lightning-fast structural aggregations on the fly using **Polars**, and deploys an incremental streaming AI model (**River ML**) to instantly isolate malicious vandalism actions or bot spikes without resource overload.

---

## 🏗️ Project Architecture Layout
```text
wiki-live-analytics/
│
├── app/
│   ├── dashboard.py             # Main Entry UI: Live Anomaly Stream Monitor
│   └── pages/
│       └── 1_Polars_Analytics.py # Page 2: Polars aggregated feature grids
│
├── src/
│   ├── db_manager.py            # PostgreSQL layout setup and batch insertions
│   ├── ingestor.py              # Live Event consumers and loop orchestration
│   ├── transformer.py           # Polars analytics calculations
│   └── ml_model.py              # River streaming anomaly logic
│
├── docker-compose.yml           # Database container configurations
├── requirements.txt             # Project environment manifest packages
└── run.py                       # Main pipeline orchestration launcher
```

---

## ⚡ Execution Order Blueprint

Follow these sequential steps in order to launch the full application workspace locally on your machine.

### Step 1: Environment Provisioning
Open a terminal window inside your project root folder (`wiki-live-analytics/`) and set up your virtual Python environment workspace:

```bash
# 1. Initialize an isolated virtual environment directory environment
python -m venv env

# 2. Activate your environment context rules
# On Windows Command Prompt:
env\Scripts\activate
# On Windows PowerShell:
.\env\Scripts\activate
# On macOS or Linux:
source env/bin/activate

# 3. Download and install all required open-source utility dependencies
pip install -r requirements.txt
```

### Step 2: Boot Storage Infrastructure
Ensure Docker Desktop is active in the background, then launch your database container using your Compose manifest:

```bash
# Start your isolated PostgreSQL database container quietly in the background
docker compose up -d

# Verify that the container is up and listening on port 5432
docker compose ps
```

### Step 3: Launch the Streaming Backend Pipeline
Keep your first terminal window open to display incoming events, and run your main orchestration file:

```bash
# Triggers system table initializations, mounts the API tunnel, and logs live inferences
python run.py
```
*(Leave this terminal running continually so data streams into storage rows.)*

### Step 4: Fire Up the Web Dashboard UI Panel
Open a **second, separate terminal window**, navigate back into your root folder directory, and run the frontend engine:

```bash
# 1. Securely re-activate your virtual environment block in terminal 2
env\Scripts\activate

# 2. Boot up the user interface layer using python module execution rules
python -m streamlit run app/dashboard.py
```
Your browser will automatically open up a dashboard control panel at **`http://localhost:8501`**.

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
