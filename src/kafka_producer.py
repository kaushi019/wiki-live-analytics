# src/kafka_producer.py
import os
import sys
import json
import requests
import sseclient

# 🚀 PATH INJECTION: Forces Python to recognize the root project directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer

WIKI_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange" 
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "wikipedia-edits"

def run_producer():
    print("====================================================")
    print("🚀 LAUNCHING WIKIPEDIA KAFKA PRODUCER ENGINE")
    print("====================================================")
    
    print(f"📦 Connecting to Kafka Broker at: {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            connection_timeout_ms=10000
        )
        print("✅ Successfully connected to Kafka Broker!")
    except Exception as e:
        print(f"❌ KAFKA CONNECTION FAILED: {e}")
        return

    headers = {"User-Agent": "WikiKafkaProducerPipeline/2.0 (student@domain.com)"}
    print(f"\n📡 Establishing live API tunnel to: {WIKI_STREAM_URL}")
    
    try:
        response = requests.get(WIKI_STREAM_URL, stream=True, headers=headers, timeout=30)
        print(f"📡 Server Response Code: {response.status_code}")
        
        if response.status_code != 200:
            print("❌ Server rejected connection handshake. Exiting pipeline.")
            return
            
        client = sseclient.SSEClient(response)
        print("🚀 Tunnel opened! Actively listening for global Wikipedia changes...")
        print("📥 Broadcasting to Kafka topic. Press Ctrl+C to stop.\n")
        
        for event in client.events():
            if event.event == 'message' and event.data:
                try:
                    change = json.loads(event.data)
                    
                    if change.get('namespace') == 0 and change.get('server_name') == 'en.wikipedia.org':
                        page_title = change.get('title', 'Unknown Page')
                        
                        producer.send(KAFKA_TOPIC, value=change)
                        print(f"🔹 [KAFKA PUSHED] Published edit for: '{page_title}'")
                        
                except Exception as inner_err:
                    print(f"⚠️ Payload packet parse error: {inner_err}")
                    continue
                    
    except KeyboardInterrupt:
        print("\n🛑 Pipeline paused cleanly by user.")
    except Exception as e:
        print(f"\n❌ Streaming connection broke unexpectedly: {e}")
    finally:
        print("🧹 Flushing remaining message cues out to broker...")
        try:
            producer.flush()
        except Exception:
            pass
        print("🏁 Producer offline.")

if __name__ == "__main__":
    run_producer()
