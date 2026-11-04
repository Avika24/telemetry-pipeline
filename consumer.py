import json
from confluent_kafka import Consumer

# --- Connection setup ---
conf = {
    "bootstrap.servers": "localhost:19092",
    "group.id": "telemetry-consumer-group",   # identifies this consumer (or group of consumers)
    "auto.offset.reset": "earliest",          # if no saved position, start from the beginning of the topic
    "enable.auto.commit": False,              # WE control exactly when a message is marked "done" — see explanation below
}
consumer = Consumer(conf)

TOPIC = "telemetry.raw"
consumer.subscribe([TOPIC])

if __name__ == "__main__":
    print(f"Listening to topic '{TOPIC}'. Press Ctrl+C to stop.\n")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # wait up to 1 second for a new message

            if msg is None:
                continue  # no message arrived in this second, just loop again

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode("utf-8"))
            print(f"Received: {event}")

            # Mark this message as processed. We do this AFTER we've handled it,
            # never before — that's the core offset-safety rule.
            consumer.commit(msg)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()