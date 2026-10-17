# src/transformer.py
import polars as pl
from src.db_manager import DBManager

class WikiTransformer:
    def __init__(self):
        self.db = DBManager()

    def initialize_feature_table(self):
        """Creates an advanced analytics table to save computed business metrics."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS wikipedia_features (
            id SERIAL PRIMARY KEY,
            page_title TEXT,
            total_edits INT,
            unique_users INT,
            bot_edit_count INT,
            total_bytes_changed INT,
            avg_bytes_per_edit FLOAT,
            
            -- 📈 ENHANCED BUSINESS METRICS
            typo_fixes_count INT,
            vandalism_reversals_count INT,
            avg_editor_experience FLOAT,
            
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                cursor.execute("ALTER TABLE wikipedia_features ADD COLUMN IF NOT EXISTS typo_fixes_count INT DEFAULT 0;")
                cursor.execute("ALTER TABLE wikipedia_features ADD COLUMN IF NOT EXISTS vandalism_reversals_count INT DEFAULT 0;")
                cursor.execute("ALTER TABLE wikipedia_features ADD COLUMN IF NOT EXISTS avg_editor_experience FLOAT DEFAULT 0.0;")
            conn.commit()
            print("✅ Analytics database initialized: Upgraded business metrics schema ready.")
        except Exception as e:
            conn.rollback()
            print(f"❌ Failed to initialize feature table: {e}")
        finally:
            conn.close()

    def process_and_store_features(self):
        """Extracts rich raw data using COALESCE, executes NLP intent extraction via Polars, and saves."""
        
        # 🧠 FIX: COALESCE guarantees Polars reads text columns as String data types from row 1
        query = """
            SELECT 
                COALESCE(page_title, '') AS page_title, 
                COALESCE(user_name, '') AS user_name, 
                is_bot, 
                bytes_changed, 
                COALESCE(edit_comment, '') AS edit_comment, 
                user_edit_count 
            FROM wikipedia_raw;
        """
        
        conn = self.db.get_connection()
        try:
            df = pl.read_database(query=query, connection=conn)
        except Exception as e:
            print(f"❌ Failed to fetch data for transformation: {e}")
            return
        finally:
            conn.close()

        if df.is_empty():
            return

        # 🧠 INDUSTRIAL POLARS DATA NLP INTENT CLASSIFICATION
        transformed_df = (
            df.group_by("page_title")
            .agg([
                pl.len().alias("total_edits"),
                pl.col("user_name").n_unique().alias("unique_users"),
                pl.col("is_bot").filter(pl.col("is_bot") == True).len().alias("bot_edit_count"),
                pl.col("bytes_changed").sum().alias("total_bytes_changed"),
                pl.col("bytes_changed").mean().alias("avg_bytes_per_edit"),
                
                # 📝 NLP Intent Extractions via Regex matching patterns
                pl.col("edit_comment").str.contains("(?i)(typo|spell|grammar|fix)").filter(pl.col("edit_comment").str.contains("(?i)(typo|spell|grammar|fix)") == True).len().alias("typo_fixes_count"),
                pl.col("edit_comment").str.contains("(?i)(vandalism|rvv|revert|spam|rv)").filter(pl.col("edit_comment").str.contains("(?i)(vandalism|rvv|revert|spam|rv)") == True).len().alias("vandalism_reversals_count"),
                
                # 🛡️ Experience Metric Tally
                pl.col("user_edit_count").mean().alias("avg_editor_experience")
            ])
        )

        records_to_insert = [
            (
                row["page_title"], int(row["total_edits"]), int(row["unique_users"]), int(row["bot_edit_count"]),
                int(row["total_bytes_changed"]), float(row["avg_bytes_per_edit"] or 0.0),
                int(row["typo_fixes_count"]), int(row["vandalism_reversals_count"]), float(row["avg_editor_experience"] or 0.0)
            )
            for row in transformed_df.iter_rows(named=True)
        ]

        insert_query = """
        INSERT INTO wikipedia_features (
            page_title, total_edits, unique_users, bot_edit_count, total_bytes_changed, avg_bytes_per_edit,
            typo_fixes_count, vandalism_reversals_count, avg_editor_experience
        ) VALUES %s;
        """
        
        conn = self.db.get_connection()
        try:
            from psycopg2.extras import execute_values
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE wikipedia_features;") 
                execute_values(cursor, insert_query, records_to_insert)
            conn.commit()
            print(f"⚙️ Polars processed business metrics for {transformed_df.height} unique pages.")
        except Exception as e:
            conn.rollback()
            print(f"❌ Failed to save engineered features: {e}")
        finally:
            conn.close()
