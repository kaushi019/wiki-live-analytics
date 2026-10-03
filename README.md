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
