Steps To Run the project

1. py -m venv env
2. env\Scripts\activate
3. pip install -r requirements.txt



Docker Commands

# setup the Postgres SQL DB
docker compose up -d

# Check if DB is running smoothly
docker compose ps

# Stop running DB
docker compose down

3. Running Your Pipeline
    a. Run `docker compose up -d` to turn on your database.
    b. Ensure your `config/settings.py` file matches the password (your_secure_password) and dbname (`wiki_stream_db`) specified in your `docker-compose.yml` file.
    c. Execute your main Python file to start streaming:bash
        `python run.py`


Step 4:

🌲 Option A: Batch AI (Scikit-Learn - Isolation Forest)
• How it works: A separate script runs periodically (e.g., every 30 seconds). It loads the Polars features from your database, trains an Isolation Forest model to establish what "normal" editing activity looks like, and flags pages with extreme spikes in edits, unique users, or bytes changed.
• Best for: Learning standard, industry-standard ML patterns and scoring historical data.
🌊 Option B: Streaming AI (River - Incremental Hoeffding Tree)
• How it works: We plug the River library directly into your ingestion loop. The model learns incrementally from every single batch on the fly. It updates its internal mathematical thresholds continuously without needing a giant training dataset.
• Best for: Learning cutting-edge, real-time streaming data science principles.
