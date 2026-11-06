import json
import time
import polars as pl
from confluent_kafka import Consumer

from validation import validate_batch, split_batch
from storage import get_connection, write_clean, write_rejected


conf = {
    "bootstrap.servers": "localhost:19092",
    "group.id": "telemetry-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
consumer = Consumer(conf)
TOPIC = "telemetry.raw"
consumer.subscribe([TOPIC])

BATCH_SIZE = 20         
BATCH_TIMEOUT = 5.0     

SCHEMA = {
    "device_id": pl.Utf8,
    "temperature": pl.Float64,
    "humidity": pl.Float64,
    "cpu_usage": pl.Float64,
    "event_timestamp": pl.Utf8,  
}


def process_batch(con, buffer, messages):
    
    if not buffer:
        return

    df = pl.DataFrame(buffer, schema=SCHEMA)
    df = df.with_columns(
    pl.col("event_timestamp").str.to_datetime(
        format="%+",
        time_zone="UTC",
        strict=False
    )
)

    validated_df = validate_batch(df)
    clean_df, rejected_df = split_batch(validated_df)

    if clean_df.height > 0:
        write_clean(con, clean_df)
    if rejected_df.height > 0:
        write_rejected(con, rejected_df)

    print(f"Flushed batch: {clean_df.height} clean, {rejected_df.height} rejected")

    
    for msg in messages:
        consumer.commit(msg)


if __name__ == "__main__":
    print(f"Listening to topic '{TOPIC}'. Press Ctrl+C to stop.\n")
    con = get_connection()

    buffer = []       
    messages = []      
    last_flush = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is not None:
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                else:
                    event = json.loads(msg.value().decode("utf-8"))
                    buffer.append(event)
                    messages.append(msg)

            time_elapsed = time.time() - last_flush
            should_flush = len(buffer) >= BATCH_SIZE or (buffer and time_elapsed >= BATCH_TIMEOUT)

            if should_flush:
                process_batch(con, buffer, messages)
                buffer = []
                messages = []
                last_flush = time.time()

    except KeyboardInterrupt:
        print("\nStopping consumer...")
        process_batch(con, buffer, messages)  
    finally:
        consumer.close()
        con.close()