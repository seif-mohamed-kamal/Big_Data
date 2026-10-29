import pandas as pd
import json
import time
from kafka import KafkaProducer

# -----------------------------
# Kafka Producer Configuration
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC_NAME = "weatherRaw"
RECORDS_PER_BATCH = 100
SEND_INTERVAL = 1  # seconds

# -----------------------------
# Load CSV
# -----------------------------
df = pd.read_csv("WeatherM.csv")
df = df.where(pd.notnull(df), None)
records = df.to_dict(orient="records")

# -----------------------------
# Send 100 records at a time (but NOT inside an array)
# Each message = ONE record
# -----------------------------

def send_records():
    total_records = len(records)
    batch_number = 1

    for i in range(0, total_records, RECORDS_PER_BATCH):
        batch = records[i:i + RECORDS_PER_BATCH]

        print(f"\n===== Sending Batch #{batch_number} =====")
        print(f"Records: {len(batch)}\n")

        for record in batch:
            producer.send(TOPIC_NAME, record)

        producer.flush()
        time.sleep(SEND_INTERVAL)

        batch_number += 1

    print("All records sent successfully.")


if __name__ == "__main__":
    send_records()
