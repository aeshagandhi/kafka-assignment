import json
import psycopg2
from kafka import KafkaConsumer

def run_consumer():

    consumer = KafkaConsumer(
        "ride_trips",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="trips-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    print("[Consumer] ✓ Kafka ready")

    conn = psycopg2.connect(
        dbname="kafka_db",
        user="kafka_user",
        password="kafka_password",
        host="localhost",
        port="5432"
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ride_trips (
            trip_id VARCHAR(50) PRIMARY KEY,
            city VARCHAR(50),
            ride_type VARCHAR(50),
            status VARCHAR(30),
            distance_km NUMERIC(10,2),
            fare NUMERIC(10,2),
            timestamp TIMESTAMP
        );
    """)

    print("[Consumer] ✓ Table 'ride_trips' ready.")
    print("[Consumer] Listening...\n")

    for msg in consumer:
        trip = msg.value
        try:
            cur.execute("""
                INSERT INTO ride_trips (trip_id, city, ride_type, status, distance_km, fare, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trip_id) DO NOTHING;
            """, (
                trip["trip_id"],
                trip["city"],
                trip["ride_type"],
                trip["status"],
                trip["distance_km"],
                trip["fare"],
                trip["timestamp"]
            ))
            print(f"[Consumer] ✓ Inserted: {trip['trip_id']} | {trip['city']} | {trip['fare']}")
        except Exception as e:
            print("[Consumer ERROR]", e)

if __name__ == "__main__":
    run_consumer()
