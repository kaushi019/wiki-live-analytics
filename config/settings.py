import os

# 📡 Wikipedia Stream Configuration
WIKI_STREAM_URL = "https://wikimedia.org"
TARGET_SERVER = "en.wikipedia.org"

# ⚡ Processing Controls
BATCH_SIZE = 50           # Trigger database insert when this many events accumulate
BATCH_TIMEOUT = 5.0       # Or trigger insert every 5 seconds (prevents data getting stuck)

# 🐳 Dynamic Database Configuration
# os.getenv checks for system variables first (e.g. inside Docker), otherwise falls back to defaults.
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "wiki_stream_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "wiki_live_pass_019")  # Must match docker-compose.yml
}
