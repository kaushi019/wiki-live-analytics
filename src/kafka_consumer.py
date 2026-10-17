# src/kafka_consumer.py
import os
import sys
import json
import time

# PATH INJECTION: Forces Python to recognize the root project directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer
from src.db_manager import DBManager
from src.transformer import WikiTransformer
from src.ml_model import WikiAnomalyDetector

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "wikipedia-edits"
BATCH_SIZE = 20
BATCH_TIMEOUT = 5.0

def evaluate_sla_and_notify(record):
    """
    🏢 BUSINESS OPERATIONS ENGINE
    Evaluates streaming data against operational SLAs to classify incident severity.
    """
    score = record["anomaly_score"]
    exp = record["user_edit_count"]
    is_bot = record["is_bot"]
    is_minor = record["is_minor"]
    
    if score > 0.85 and exp < 10:
        print(f"\n🚨 [SLA BREACH - P1 CRITICAL] SECURITY INCIDENT TRIPPED!")
        print(f"   ↳ Target Page : '{record['page_title']}'")
        print(f"   ↳ Risk Vector : MALICIOUS SLEEPER ACCOUNT DETECTED (Experience: {exp} edits, Vol: {record['bytes_changed']} bytes)")
        print(f"   ↳ Action      : Initiating automated account suspension webhook routing...\n")
        
    elif score > 0.75 and is_bot:
        print(f"\n🤖 [SLA BREACH - P2 HIGH] SYSTEM UNSTABLE INCIENT!")
        print(f"   ↳ Target Page : '{record['page_title']}'")
        print(f"   ↳ Risk Vector : ROGUE BOT DISCOVERED (User: {record['user_name']}, Score: {score:.2f})")
        print(f"   ↳ Action      : Dispatching operational API rate-limiting throttling triggers...\n")
        
    elif score > 0.75 and not is_minor:
        print(f"\n📝 [SLA BREACH - P3 MEDIUM] CONTENT INTEGRITY ALERT!")
        print(f"   ↳ Target Page : '{record['page_title']}'")
        print(f"   ↳ Risk Vector : HIGH-VOLUME PROBABLE VANDALISM (Size Delta: {record['bytes_changed']} bytes)")
        print(f"   ↳ Action      : Routing payload directly to front-line moderation queue.\n")

def run_consumer():
    print("====================================================")
    print("🧠 LAUNCHING WIKIPEDIA KAFKA CONSUMER ENGINE")
    print("====================================================")
    
    db = DBManager()
    db.initialize_database()
    
    transformer = WikiTransformer()
    transformer.initialize_feature_table()
    
    detector = WikiAnomalyDetector()
    
    print(f"📥 Connecting Kafka Consumer to Topic: '{KAFKA_TOPIC}'...")
    
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='latest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                request_timeout_ms=5000
            )
            print("✅ Consumer linked to Kafka cluster broker successfully!")
            break
        except Exception as e:
            print(f"❌ Kafka Broker not fully ready yet ({e}). Re-trying in 3 seconds...")
            time.sleep(3)
            
    batch = []
    last_flush_time = time.time()
    print("🎬 Awaiting incoming stream data from Kafka queue message channels...\n")
    
    try:
        for message in consumer:
            change = message.value
            
            length_old = change.get("length", {}).get("old") or 0
            length_new = change.get("length", {}).get("new") or 0
            bytes_changed = abs(length_new - length_old)
            is_bot = change.get("bot", False)
            
            is_minor = change.get("minor", False)
            user_edit_count = int(change.get("user_edit_count") or 0)
            
            anomaly_score = detector.learn_and_score(bytes_changed, is_bot, user_edit_count, is_minor)
            revision = change.get("revision", {})
            
            record = {
                "timestamp": str(change.get("timestamp")),
                "page_title": change.get("title"),
                "user_name": change.get("user"),
                "is_bot": is_bot,
                "bytes_changed": int(bytes_changed),
                "edit_type": change.get("type", "unknown"),
                "anomaly_score": float(anomaly_score),
                "edit_comment": change.get("comment", ""),
                "is_minor": is_minor,
                "revision_id": revision.get("new"),
                "parent_revision_id": revision.get("old"),
                "user_edit_count": user_edit_count
            }
            
            batch.append(record)
            
            # 🧠 RUN THE REAL-TIME SLA RISK PROFILER FOR EVERY ALERT
            if anomaly_score > 0.75:
                evaluate_sla_and_notify(record)
            else:
                print(f"📥 Kafka Message Consumed: '{record['page_title']}' ({len(batch)}/{BATCH_SIZE})")
                
            if len(batch) >= BATCH_SIZE or (time.time() - last_flush_time >= BATCH_TIMEOUT):
                if batch:
                    print(f"💾 Kafka Consumer flushing batch of {len(batch)} rich records to PostgreSQL...")
                    db.insert_batch(batch)
                    batch = []
                    transformer.process_and_store_features()
                last_flush_time = time.time()
                
    except KeyboardInterrupt:
        print("\n🛑 Kafka Consumer processing paused by user.")

if __name__ == "__main__":
    run_consumer()
