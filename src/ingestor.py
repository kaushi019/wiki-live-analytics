# src/ingestor.py
import json
import time
import requests
import sseclient
from src.db_manager import DBManager
from src.ml_model import WikiAnomalyDetector
from src.transformer import WikiTransformer

class WikiIngestor:
    def __init__(self):
        self.db = DBManager()
        self.transformer = WikiTransformer()
        self.detector = WikiAnomalyDetector()      # Initialize the River AI engine
        self.batch = []
        self.last_flush_time = time.time()

    def process_stream(self):
        """Connects to Wikipedia's live EventStreams server using explicit custom headers."""
        
        # 1. Force the correct, absolute streaming endpoint URL
        STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
        TARGET_SERVER = "en.wikipedia.org"
        BATCH_SIZE = 20  # Lowering batch size slightly so you see feedback faster!
        BATCH_TIMEOUT = 5.0
        
        # 2. Supply a clear User-Agent string to satisfy Wikimedia's api policy
        headers = {
            "User-Agent": "WikiLiveAnalyticsProject/1.0 (Local Dev Educational Pipeline; contact: student@domain.com)"
        }
        
        print(f"📡 Establishing tunnel to: {STREAM_URL}")
        
        try:
            # Open continuous chunked stream thread over HTTP with headers applied
            response = requests.get(STREAM_URL, stream=True, headers=headers, timeout=30)
            
            # Ensure the server returned a 200 OK before moving forward
            if response.status_code != 200:
                print(f"❌ Server rejected connection. Status Code: {response.status_code}")
                return
                
            client = sseclient.SSEClient(response)
            print("🚀 Connection opened! Listening for live Wikipedia edits...")
            
            # Continuous network polling loop
            for event in client.events():
                if event.event == 'message':
                    self._handle_event_message(event.data, TARGET_SERVER, BATCH_SIZE)
                    
                # Time-based trigger fallback
                if time.time() - self.last_flush_time >= BATCH_TIMEOUT:
                    self._flush_batch()

        except KeyboardInterrupt:
            print("\n🛑 Stream ingestion paused manually by user.")
            self._flush_batch()
        except Exception as e:
            print(f"❌ Error encountered in streaming thread: {e}")
            self._flush_batch()

    def _handle_event_message(self, raw_data, target_server, batch_size):
        """Parses individual live payloads into micro-batch lists."""
        try:
            if not raw_data:
                return
                
            change = json.loads(raw_data)
            
            # Filter for standard namespace main articles on the English domain
            if change.get('namespace') == 0 and change.get('server_name') == target_server:
                length_old = change.get("length", {}).get("old") or 0
                length_new = change.get("length", {}).get("new") or 0
                bytes_changed = abs(length_new - length_old)
                is_bot = change.get("bot", False)

                # 🤖 PASS TO STREAMING ML ENGINE
                # Calculate the anomaly score on the fly!
                anomaly_score = self.detector.learn_and_score(bytes_changed, is_bot)

                record = {
                    "timestamp": str(change.get("timestamp")),
                    "page_title": change.get("title"),
                    "user_name": change.get("user"),
                    "is_bot": is_bot,
                    "bytes_changed": int(bytes_changed),
                    "edit_type": change.get("type", "unknown"),
                    "anomaly_score": float(anomaly_score)
                }
                
                self.batch.append(record)

                # Visual flag in the logs if the edit looks suspicious
                if anomaly_score > 0.75:
                    print(f"⚠️ ANOMALY DETECTED [{anomaly_score:.2f}] | '{record['page_title']}' edited by {record['user_name']} ({bytes_changed} bytes)")
                else:
                    print(f"📥 Buffered edit: '{record['page_title']}' by {record['user_name']} ({len(self.batch)}/{batch_size})")
                
                if len(self.batch) >= batch_size:
                    self._flush_batch()

        except (ValueError, KeyError, TypeError, json.JSONDecodeError):
            pass

    def _flush_batch(self):
        """Saves batch records to Postgres and updates Polars features."""
        if self.batch:
            print(f"💾 Flushing {len(self.batch)} rows to database...")
            self.db.insert_batch(self.batch)
            self.batch = []
            
            print("⚡ Running Polars aggregations on your live data tables...")
            self.transformer.process_and_store_features()
            
        self.last_flush_time = time.time()
