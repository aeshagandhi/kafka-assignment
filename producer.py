import time
import json
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

def generate_trip_event():
    """Generate synthetic ride-sharing trip data."""
    
    cities = ["New York", "Chicago", "San Francisco", "Boston", "Seattle"]
    ride_types = ["UberX", "UberXL", "Comfort", "Lyft", "Lyft XL"]
    statuses = ["Requested", "Accepted", "In Progress", "Completed", "Cancelled"]

    distance_km = round(random.uniform(0.5, 25.0), 2)
    base_fare = round(2 + distance_km * random.uniform(1.0, 3.0), 2)

    return {
        "trip_id": str(uuid.uuid4())[:8],
        "city": random.choice(cities),
        "ride_type": random.choice(ride_types),
        "status": random.choice(statuses),
        "distance_km": distance_km,
        "fare": base_fare,
        "timestamp": datetime.utcnow().isoformat()
    }

def run_producer():
    print("[Producer] Connecting to Kafka...")
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("[Producer] ✓ Connected!")

    count = 0
    while True:
        trip = generate_trip_event()
        print(f"[Producer] Sending trip #{count}: {trip}")
        producer.send("ride_trips", value=trip)
        producer.flush()
        count += 1
        time.sleep(random.uniform(0.5, 2))
        
if __name__ == "__main__":
    run_producer()
