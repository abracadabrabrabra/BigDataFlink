import csv
import json
import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from kafka.admin import KafkaAdminClient, NewTopic


def wait_for_kafka(max_retries=30, delay=5):
    for i in range(max_retries):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers='kafka:9092',
                client_id='producer-admin'
            )
            topics = admin.list_topics()
            print("Kafka is ready!")
            return True
        except Exception as e:
            print(f"Waiting for Kafka... (attempt {i + 1}/{max_retries})")
            time.sleep(delay)
    return False


def create_topic_if_not_exists():
    try:
        admin = KafkaAdminClient(
            bootstrap_servers='kafka:9092',
            client_id='producer-admin'
        )

        topics = admin.list_topics()
        if 'csv-topic' not in topics:
            topic = NewTopic(
                name='csv-topic',
                num_partitions=1,
                replication_factor=1
            )
            admin.create_topics([topic])
            print("Created topic 'csv-topic'")
    except Exception as e:
        print(f"Error creating topic: {e}")


def main():
    print("Waiting for Kafka...")
    if not wait_for_kafka():
        print("Kafka not available. Exiting.")
        return

    create_topic_if_not_exists()

    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )

    DATA_DIR = "/app/data"

    if not os.path.exists(DATA_DIR):
        print(f"Data directory {DATA_DIR} not found!")
        return

    files_processed = 0
    messages_sent = 0

    for filename in os.listdir(DATA_DIR):
        if not filename.endswith(".csv"):
            continue

        filepath = os.path.join(DATA_DIR, filename)
        print(f"Processing {filename}...")

        with open(filepath, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                message = {
                    "source_file": filename,
                    "payload": row,
                    "timestamp": time.time()
                }

                future = producer.send("csv-topic", value=message)
                messages_sent += 1

                if messages_sent % 100 == 0:
                    print(f"Sent {messages_sent} messages...")

        files_processed += 1

    producer.flush()
    producer.close()

    print(f"\nCompleted! Processed {files_processed} files, sent {messages_sent} messages")


if __name__ == "__main__":
    main()