import json
import uuid
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
import os
import config as cfg

print(cfg.KAFKA_TOPIC)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_topic(topic, bootstrap_servers):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    while topic not in admin.list_topics():
        time.sleep(10)

def safe_json_deserializer(v):
    if v is None or v == b'':
        return None
    try:
        return json.loads(v.decode("utf-8"))
    except Exception as e:
        logger.error("Skipping invalid JSON: %s | Error: %s", v, e)
        return None


def consume_kafka_to_file(
    topic="trade-events",
    bootstrap_servers="kafka:9092",
    batch_size=100,
    output_dir="/opt/airflow/raw/data"
) -> str:
    """
    Consumes Kafka messages and writes them to a batch JSON file.
    Returns the file path.
    """

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=safe_json_deserializer,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="trade-file-consumer",
        max_poll_records=50
    )

    logger.info("Kafka consumer started for topic: %s", topic)

    batch = []
    start_time = time.time()

    for msg in consumer:
        if msg.value is None:
            consumer.commit()
            print("value is coming null")
            continue

        batch.append(msg.value)

        if len(batch) >= batch_size:
            break

    if not batch:
        logger.info("No messages received, nothing to write.")
        return None

    # Create unique file name
    batch_id = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:6]}"
    file_path = f"{output_dir}/batch_{batch_id}.json"

    # Ensure directory exists
    
    os.makedirs(output_dir, exist_ok=True)

    with open(file_path, "w") as f:
        json.dump(batch, f, indent=2)

    consumer.commit()
    consumer.close()

    logger.info("Wrote %s messages to %s", len(batch), file_path)
    return file_path, batch_id
