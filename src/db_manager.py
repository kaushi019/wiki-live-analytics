# src/db_manager.py
import os
import psycopg2
from psycopg2.extras import execute_values

class DBManager:
    def __init__(self):
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")
        self.dbname = os.getenv("DB_NAME", "wiki_stream_db")
        self.user = os.getenv("DB_USER", "postgres")
        self.password = os.getenv("DB_PASSWORD", "wiki_live_pass_019")
        print(f"🔌 DBManager prepared configuration for {self.dbname} on port {self.port}")

    def get_connection(self):
        return psycopg2.connect(
            host=self.host, port=self.port, dbname=self.dbname, user=self.user, password=self.password
        )

    def initialize_database(self):
        """Creates the raw table with expanded analytical business dimensions."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS wikipedia_raw (
            id SERIAL PRIMARY KEY,
            timestamp VARCHAR(50),
            page_title TEXT,
            user_name TEXT,
            is_bot BOOLEAN,
            bytes_changed INT,
            edit_type VARCHAR(20),
            anomaly_score FLOAT DEFAULT 0.0,
            
            -- 🧠 NEW BUSINESS DIMENSIONS
            edit_comment TEXT,
            is_minor BOOLEAN DEFAULT FALSE,
            revision_id BIGINT,
            parent_revision_id BIGINT,
            user_edit_count INT DEFAULT 0,
            
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                
                # Dynamic upgrade execution blocks in case the tables exist locally
                cursor.execute("ALTER TABLE wikipedia_raw ADD COLUMN IF NOT EXISTS edit_comment TEXT;")
                cursor.execute("ALTER TABLE wikipedia_raw ADD COLUMN IF NOT EXISTS is_minor BOOLEAN DEFAULT FALSE;")
                cursor.execute("ALTER TABLE wikipedia_raw ADD COLUMN IF NOT EXISTS revision_id BIGINT;")
                cursor.execute("ALTER TABLE wikipedia_raw ADD COLUMN IF NOT EXISTS parent_revision_id BIGINT;")
                cursor.execute("ALTER TABLE wikipedia_raw ADD COLUMN IF NOT EXISTS user_edit_count INT DEFAULT 0;")
            conn.commit()
            print("✅ Database initialized successfully: Expanded business metrics analytics schema is active.")
        except Exception as e:
            conn.rollback()
            print(f"❌ Failed to initialize database: {e}")
            raise e
        finally:
            conn.close()

    def insert_batch(self, batch_data):
        if not batch_data:
            return

        insert_query = """
        INSERT INTO wikipedia_raw (
            timestamp, page_title, user_name, is_bot, bytes_changed, 
            edit_type, anomaly_score, edit_comment, is_minor, revision_id, parent_revision_id, user_edit_count
        ) VALUES %s;
        """
        
        records_to_insert = [
            (
                row['timestamp'], row['page_title'], row['user_name'], row['is_bot'], row['bytes_changed'], 
                row['edit_type'], row['anomaly_score'], row['edit_comment'], row['is_minor'], 
                row['revision_id'], row['parent_revision_id'], row['user_edit_count']
            ) 
            for row in batch_data
        ]

        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, records_to_insert)
            conn.commit()
            print(f"💾 Successfully flushed batch of {len(batch_data)} rich analytics records to database.")
        except Exception as e:
            conn.rollback()
            print(f"❌ Failed to insert batch into database: {e}")
        finally:
            conn.close()
