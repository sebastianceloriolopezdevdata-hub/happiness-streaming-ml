import json
import math
import os
import joblib  # <-- CAMBIO 1: Importar joblib en lugar de pickle
import hashlib
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import pymysql
from dotenv import load_dotenv
from kafka import KafkaConsumer

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

# Feature mapping: JSON keys to Model column names
FEATURES = {
    "gdp": "gdp_per_capita", "family": "social_support",
    "health": "healthy_life_expectancy", "freedom": "freedom",
    "generosity": "generosity", "corruption": "perceptions_of_corruption"
}
REQUIRED = {"event_id", "country", "year", *FEATURES, "actual_happiness_score", "event_time"}

def get_utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def validate_event(event):
    if not isinstance(event, dict): return "INVALID_SCHEMA", "Event must be a JSON object."
    missing = REQUIRED - event.keys()
    if missing: return "INVALID_SCHEMA", f"Missing fields: {', '.join(missing)}."
    for f in ["event_id", "country", "event_time"]:
        if not isinstance(event[f], str) or not event[f].strip(): return "INVALID_SCHEMA", f"Field '{f}' must be a non-empty string."
    if type(event["year"]) is not int or not 2015 <= event["year"] <= 2019:
        return "INVALID_VALUES", "Year must be an integer between 2015 and 2019."
    for f in [*FEATURES, "actual_happiness_score"]:
        if type(event[f]) not in (int, float) or not math.isfinite(event[f]):
            return "INVALID_VALUES", f"Field '{f}' must be a finite number."
    return "VALID", None

def main():
    # 1. Load the ML model
    print("Loading ML model...")
    model_path = ROOT / "models/model.pkl"
    
    with open(model_path, "rb") as f:
        model_bytes = f.read()
        model_hash = hashlib.sha256(model_bytes).hexdigest()
        
    # CAMBIO 2: Usar joblib.load() en lugar de pickle.loads()
    model = joblib.load(model_path) 

    # 2. Connect to MySQL
    conn = pymysql.connect(
        host='localhost',
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DATABASE'),
        autocommit=False
    )
    
    # 3. Connect to Kafka
    consumer = KafkaConsumer(
        'happiness-events',
        bootstrap_servers=['localhost:9092'],
        group_id='happiness-ml-team',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: x.decode('utf-8')
    )

    print("🎧 Listening to Kafka...")
    
    try:
        for message in consumer:
            topic, partition, offset = message.topic, message.partition, message.offset
            raw_value = message.value
            
            with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                try:
                    conn.begin()
                    
                    # Save Raw
                    cursor.execute("""
                        INSERT IGNORE INTO raw_happiness_events 
                        (topic, partition_id, offset_id, raw_key, raw_value, processing_status) 
                        VALUES (%s, %s, %s, %s, %s, 'RECEIVED')
                    """, (topic, partition, offset, str(message.key, 'utf-8') if message.key else None, raw_value))
                    
                    if cursor.rowcount == 0: 
                        conn.commit()
                        continue
                    
                    raw_id = cursor.lastrowid
                    event = json.loads(raw_value)
                    status, error = validate_event(event)
                    pred = None
                    
                    if status == "VALID":
                        try:
                            inputs = {col: float(event[k]) for k, col in FEATURES.items()}
                            df = pd.DataFrame([inputs], columns=list(FEATURES.values()))
                            pred = float(model.predict(df)[0])
                            
                            cursor.execute("INSERT IGNORE INTO dim_country (country_name) VALUES (%s)", (event['country'],))
                            cursor.execute("SELECT country_id FROM dim_country WHERE country_name=%s", (event['country'],))
                            country_id = cursor.fetchone()['country_id']
                            
                            event_time = datetime.fromisoformat(event['event_time'].replace("Z", "+00:00")).replace(tzinfo=None)
                            date_id = int(event_time.strftime("%Y%m%d"))
                            cursor.execute("INSERT IGNORE INTO dim_date (date_id, calendar_date) VALUES (%s, %s)", (date_id, event_time.date()))
                            
                            cursor.execute("""
                                INSERT INTO fact_predictions 
                                (raw_event_id, country_id, date_id, observation_year, actual_score, predicted_score, event_time, model_name, model_sha256)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (raw_id, country_id, date_id, event['year'], float(event['actual_happiness_score']), pred, event_time, "RandomForest", model_hash))
                            
                        except Exception as e:
                            status = "PREDICTION_ERROR"
                            error = str(e)
                    
                    cursor.execute("""
                        UPDATE raw_happiness_events SET processing_status=%s, error_message=%s, event_id=%s, processed_at=%s 
                        WHERE raw_event_id=%s
                    """, (status, error, event.get('event_id') if status == "VALID" else None, get_utc_now(), raw_id))
                    
                    conn.commit()
                    consumer.commit()
                    
                    if status == "VALID":
                        print(f"✅ {event['country']} | Real: {event['actual_happiness_score']:.2f} | Pred: {pred:.2f} | Error: {abs(event['actual_happiness_score'] - pred):.2f}")
                    else:
                        print(f"⚠️ {event.get('country', 'Unknown')} | Status: {status} | Error: {error}")
                    
                except Exception as e:
                    conn.rollback()
                    print(f"❌ Error processing message: {e}")
                    
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()