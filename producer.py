import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

# --- Connection setup (boilerplate — connects to our local Redpanda) ---
conf = {
    "bootstrap.servers": "localhost:19092"
}
producer = Producer(conf)

TOPIC = "telemetry.raw"
DEVICE_IDS = ["sensor-01", "sensor-02", "sensor-03", "sensor-04"]


def delivery_report(err, msg):
    """Called once Redpanda confirms it received the message (or failed to)."""
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent to {msg.topic()} [partition {msg.partition()}]: {msg.value().decode('utf-8')}")


def generate_reading():
    """Creates one fake telemetry event. Mostly good data, occasionally bad on purpose."""
    device_id = random.choice(DEVICE_IDS)

    reading = {
        "device_id": device_id,
        "temperature": round(random.uniform(18.0, 28.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2),
        "cpu_usage": round(random.uniform(5.0, 85.0), 2),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Inject bad data ~10% of the time, so we have something for validation to catch later
    if random.random() < 0.10:
        bad_field = random.choice(["temperature", "humidity", "cpu_usage", "missing"])
        if bad_field == "temperature":
            reading["temperature"] = round(random.uniform(200.0, 500.0), 2)  # impossible value
        elif bad_field == "humidity":
            reading["humidity"] = None  # missing value
        elif bad_field == "cpu_usage":
            reading["cpu_usage"] = -50.0  # impossible negative value
        elif bad_field == "missing":
            del reading["device_id"]  # missing required field

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
            producer.poll(0)  # triggers delivery_report callbacks
            time.sleep(0.5)   # one message every half second
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()  # make sure any in-flight messages are sent before exiting