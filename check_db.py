# check_db.py
from src.db_manager import DBManager

db = DBManager()
conn = db.get_connection()
cursor = conn.cursor()

# Count records in both tables
cursor.execute("SELECT COUNT(*) FROM wikipedia_raw;")
raw_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM wikipedia_features;")
feature_count = cursor.fetchone()[0]

print(f"📊 Total Raw Ingested Rows: {raw_count}")
print(f"⚙️ Total Aggregated Feature Rows: {feature_count}")

# Print a tiny sample of raw data if it exists
if raw_count > 0:
    print("\n📝 Recent Raw Data Sample:")
    cursor.execute("SELECT page_title, user_name, bytes_changed FROM wikipedia_raw LIMIT 5;")
    for row in cursor.fetchall():
        print(f" - Page: {row[0]} | User: {row[1]} | Size Delta: {row[2]} bytes")

conn.close()
