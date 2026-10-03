from src.db_manager import DBManager
from src.ingestor import WikiIngestor
from src.transformer import WikiTransformer

def main():
    print("🚀 Initializing Wikipedia Live Stream Data Pipeline...")
    
    # 1. Initialize both tables inside PostgreSQL
    db_helper = DBManager()
    db_helper.initialize_database()
    
    transformer = WikiTransformer()
    transformer.initialize_feature_table()
    
    # 2. Launch the continuous ingestion & processing loop
    print("\n📈 Pipeline is live. Data is streaming and features are transforming in real-time!")
    pipeline_engine = WikiIngestor()
    pipeline_engine.process_stream()

if __name__ == "__main__":
    main()
