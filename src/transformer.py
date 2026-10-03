# src/transformer.py
import polars as pl
from src.db_manager import DBManager

class WikiTransformer:
    def __init__(self):
        self.db = DBManager()

    def initialize_feature_table(self):
        """Creates an analytics table to save computed ML features."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS wikipedia_features (
            id SERIAL PRIMARY KEY,
            page_title TEXT,
            total_edits INT,
            unique_users INT,
            bot_edit_count INT,
            total_bytes_changed INT,
            avg_bytes_per_edit FLOAT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
            conn.commit()
            print("✅ Analytics database initialized: 'wikipedia_features' table is ready.")
        except Exception as e:
            conn.rollback()
            print(f"❌ Failed to initialize feature table: {e}")
        finally:
            conn.close()

    def process_and_store_features(self):
        """Extracts recent raw data, engineers metrics via Polars, and saves them."""
        query = "SELECT page_title, user_name, is_bot, bytes_changed FROM wikipedia_raw;"
        
        conn = self.db.get_connection()
        try:
            df = pl.read_database(query=query, connection=conn)
        except Exception as e:
            print(f"❌ Failed to fetch data for transformation: {e}")
            return
        finally:
            conn.close()

        if df.is_empty():
            print("📭 Feature table transformation skipped: Raw data is empty.")
            return

        transformed_df = (
            df.group_by("page_title")
            .agg([
                pl.len().alias("total_edits"),
                pl.col("user_name").n_unique().alias("unique_users"),
                pl.col("is_bot").filter(pl.col("is_bot") == True).len().alias("bot_edit_count"),
                pl.col("bytes_changed").sum().alias("total_bytes_changed"),
                pl.col("bytes_changed").mean().alias("avg_bytes_per_edit")
            ])
        )

        records_to_insert = [
            (
                row["page_title"],
                int(row["total_edits"]),
                int(row["unique_users"]),
                int(row["bot_edit_count"]),
                int(row["total_bytes_changed"]),
                float(row["avg_bytes_per_edit"] or 0.0)
            )
            for row in transformed_df.iter_rows(named=True)
        ]

        insert_query = """
        INSERT INTO wikipedia_features (page_title, total_edits, unique_users, bot_edit_count, total_bytes_changed, avg_bytes_per_edit)
        VALUES %s;
        """
        
        conn = self.db.get_connection()
        try:
            from psycopg2.extras import execute_values
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE wikipedia_features;") 
                execute_values(cursor, insert_query, records_to_insert)
            conn.commit()
            print(f"⚙️ Features calculated and saved for {transformed_df.height} unique wiki pages.")
        except Exception as e:
            conn.rollback()
            print(f"❌ Failed to save engineered features: {e}")
        finally:
            conn.close()
