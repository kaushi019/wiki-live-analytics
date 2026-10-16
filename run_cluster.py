# run_cluster.py
import subprocess
import time
import sys

def main():
    print("🚀 Container Cluster Orchestrator Active!")
    
    # Give Kafka a few seconds to warm up inside the Docker network environment
    print("⏳ Waiting 10 seconds for Kafka Broker initialization loops to settle...")
    time.sleep(10)

    # 1. Boot up the Consumer module script thread
    print("🧠 Starting Kafka Consumer AI engine...")
    consumer = subprocess.Popen([sys.executable, "-B", "-m", "src.kafka_consumer"])

    # 2. Boot up the Producer stream ingestor edge service thread
    print("📡 Starting Kafka Producer live API tunnel...")
    producer = subprocess.Popen([sys.executable, "-B", "-m", "src.kafka_producer"])

    # 3. Launch the Streamlit dashboard server view web client
    print("🖥️ Booting Streamlit Frontend Monitoring Interface panel...")
    try:
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", "app/dashboard.py",
            "--server.port=8501", "--server.address=0.0.0.0"
        ])
    except KeyboardInterrupt:
        print("\n🛑 Shutting down cluster services...")
    finally:
        consumer.terminate()
        producer.terminate()
        print("🏁 All internal pipeline application nodes halted.")

if __name__ == "__main__":
    main()
