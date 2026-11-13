import json
import time
import uuid
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from kafka import KafkaProducer

# Define the root path
ROOT = Path(__file__).resolve().parents[1]

# Path to your cleaned CSV file
DATA_PATH = ROOT / "data/processed/world_happiness_2015_2019_clean.csv"

def main():
    print("🚀 Starting Kafka Producer...")
    
    # 1. Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # 2. Load the dataset
    try:
        df = pd.read_csv(DATA_PATH)
        print(f"✅ Dataset loaded successfully: {len(df)} rows.")
    except FileNotFoundError:
        print(f"❌ Error: Could not find the dataset at {DATA_PATH}")
        return

    # 3. Stream ALL records one by one
    print("📤 Sending ALL events to Kafka topic 'happiness-events'...\n")
    
    # CAMBIO 1: Quitamos .head(50) para que recorra todo el DataFrame
    for index, row in df.iterrows():
        event_id = f"E{uuid.uuid4().hex[:8].upper()}"
        
        event = {
            "event_id": event_id,
            "country": row['country'],
            "year": int(row['year']),
            "gdp": float(row['gdp_per_capita']),
            "family": float(row['social_support']),
            "health": float(row['healthy_life_expectancy']),
            "freedom": float(row['freedom']),
            "generosity": float(row['generosity']),
            "corruption": float(row['perceptions_of_corruption']),
            "actual_happiness_score": float(row['happiness_score']),
            "event_time": datetime.now(timezone.utc).isoformat(timespec="seconds")
        }
        
        # Send to Kafka
        producer.send('happiness-events', key=event['country'].encode('utf-8'), value=event)
        
        print(f"✉️ Sent: {event['event_id']} | {event['country']} ({event['year']})")
        
        # CAMBIO 2: Bajamos el tiempo a 0.5 segundos para que no tarde 7 minutos
        time.sleep(0.5) 

    producer.flush()
    print("\n🏁 Producer finished. All events sent successfully.")

if __name__ == "__main__":
    main()