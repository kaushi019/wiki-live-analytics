# src/kafka_producer.py
import os
import sys
import json
import time
import requests
import sseclient

# 🚀 PATH INJECTION: Forces Python to recognize the root project directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer

# 📡 Fixed Core Configurations
WIKI_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
KAFKA_TOPIC = "wikipedia-edits"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

def run_producer():
    print("====================================================")
    print("🚀 LAUNCHING WIKIPEDIA KAFKA PRODUCER ENGINE")
    print("====================================================")
    
    print(f"📦 Connecting to Kafka Broker at: {KAFKA_BROKER}...")
    
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                connection_timeout_ms=5000,
                request_timeout_ms=5000
            )
            print("✅ Successfully connected to Kafka Broker!")
            break
        except Exception as e:
            print(f"⚠️ Kafka Broker not fully ready yet ({e}). Re-trying in 3 seconds...")
            time.sleep(3)

    headers = {"User-Agent": "WikiKafkaProducerPipeline/2.0 (student@domain.com)"}
    
    # 🔄 1. INFINITE STREAM RECONNECTION LOOP
    while True:
        try:
            print(f"\n📡 Establishing live API tunnel to: {WIKI_STREAM_URL}")
            response = requests.get(WIKI_STREAM_URL, stream=True, headers=headers, timeout=30)
            print(f"📡 Server Response Code: {response.status_code}")
            
            if response.status_code != 200:
                print("❌ Server rejected connection handshake. Retrying in 5 seconds...")
                time.sleep(5)
                continue
                
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
                        continue
                        
        except KeyboardInterrupt:
            print("\n🛑 Pipeline paused cleanly by user.")
            break
        except Exception as e:
            # 🛠️ Catch network drops ("Response ended prematurely") and auto-reconnect
            print(f"\n⚠️ Wikipedia stream disconnected ({e}).")
            print("🧹 Flushing producer buffers and attempting auto-reconnection in 5 seconds...")
            try:
                producer.flush()
            except Exception:
                pass
            time.sleep(5)  # Safe cooldown before restarting the tunnel
            
    print("🏁 Producer offline.")

if __name__ == "__main__":
    run_producer()
