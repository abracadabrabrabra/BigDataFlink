import csv
import json
import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

producer = None

while producer is None:

    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        print("Connected to Kafka.")

    except NoBrokersAvailable:

        print("Kafka not ready. Retry in 5 sec...")
        time.sleep(5)

DATA_DIR="/app/data"

for filename in os.listdir(DATA_DIR):

    if not filename.endswith(".csv"):
        continue

    filepath = os.path.join(DATA_DIR, filename)

    with open(filepath, newline='', encoding='utf-8') as csvfile:

        reader = csv.DictReader(csvfile)

        for row in reader:

            message = {
                "source_file": filename,
                "payload": row
            }

            producer.send(
                "csv-topic",
                value=message
            )

            print(message)

producer.flush()