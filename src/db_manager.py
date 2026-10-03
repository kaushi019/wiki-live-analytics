# src/db_manager.py
import os
import psycopg2
from psycopg2.extras import execute_values

class DBManager:
    def __init__(self):
        # Read from environment variables, or fallback to the standard Docker defaults
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")
        self.dbname = os.getenv("DB_NAME", "wiki_stream_db")
        self.user = os.getenv("DB_USER", "postgres")
        self.password = os.getenv("DB_PASSWORD", "wiki_live_pass_019")
        print(f"🔌 DBManager prepared configuration for {self.dbname} on port {self.port}")

    def get_connection(self):
        """Returns a clean connection instance to PostgreSQL."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )

    def initialize_database(self):
        """Creates the raw landing table if it doesn't exist."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS wikipedia_raw (
            id SERIAL PRIMARY KEY,
            timestamp VARCHAR(50),
            page_title TEXT,
            user_name TEXT,
            is_bot BOOLEAN,
            bytes_changed INT,
            edit_type VARCHAR(20),
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
            conn.commit()
            print("✅ Database initialized successfully: 'wikipedia_raw' table is ready.")
        except Exception as e:
            conn.rollback()
            print(f"❌ Failed to initialize database: {e}")
            raise e
        finally:
            conn.close()

    def insert_batch(self, batch_data):
        """Inserts data into PostgreSQL in a single transactional batch."""
        if not batch_data:
            return

        insert_query = """
        INSERT INTO wikipedia_raw (timestamp, page_title, user_name, is_bot, bytes_changed, edit_type)
        VALUES %s;
        """
        
        records_to_insert = [
            (row['timestamp'], row['page_title'], row['user_name'], row['is_bot'], row['bytes_changed'], row['edit_type']) 
            for row in batch_data
        ]

        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, records_to_insert)
            conn.commit()
            print(f"💾 Successfully flushed batch of {len(batch_data)} records to the database.")
        except Exception as e:
            conn.rollback()
            print(f"❌ Failed to insert batch into database: {e}")
        finally:
            conn.close()
