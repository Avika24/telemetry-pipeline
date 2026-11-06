import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer


conf = {
    "bootstrap.servers": "localhost:19092"
}
producer = Producer(conf)

TOPIC = "telemetry.raw"
DEVICE_IDS = ["sensor-01", "sensor-02", "sensor-03", "sensor-04"]


def delivery_report(err, msg):
    
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent to {msg.topic()} [partition {msg.partition()}]: {msg.value().decode('utf-8')}")


def generate_reading():
    
    device_id = random.choice(DEVICE_IDS)

    reading = {
        "device_id": device_id,
        "temperature": round(random.uniform(18.0, 28.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2),
        "cpu_usage": round(random.uniform(5.0, 85.0), 2),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Inject bad data ~10% of the time
    if random.random() < 0.10:
        bad_field = random.choice(["temperature", "humidity", "cpu_usage", "missing"])
        if bad_field == "temperature":
            reading["temperature"] = round(random.uniform(200.0, 500.0), 2)  
        elif bad_field == "humidity":
            reading["humidity"] = None  
        elif bad_field == "cpu_usage":
            reading["cpu_usage"] = -50.0  
        elif bad_field == "missing":
            del reading["device_id"] 

    return reading


if __name__ == "__main__":
    print(f"Producing messages to topic '{TOPIC}'. Press Ctrl+C to stop.\n")
    try:
        while True:
            event = generate_reading()
            producer.produce(
                TOPIC,
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)  
            time.sleep(0.5)   
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush() 