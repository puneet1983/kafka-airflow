from kafka import KafkaConsumer
import json
from kafka.admin import KafkaAdminClient, NewTopic
import time
import datetime



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
        print("Skipping invalid JSON message:", v, "Error:", e)
        return None


def run_trade_consumer(max_messages=50):
    consumer = KafkaConsumer(
        "trade-events",
        bootstrap_servers="kafka:9092",
        value_deserializer=safe_json_deserializer,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="trade-debug-consumer-2",
        max_poll_records=10
    )

    print("🟢 Kafka Consumer started. Waiting for messages...")
    
    

    count = 0
    for msg in consumer:
        if msg.value is None:
            consumer.commit()
            continue

        try:
            print("📥 Received message:", msg.value)
            # print("📥 Received message keys is blank:",type(msg.value))
            trade = msg.value
            trade_id = trade["trade_id"]
            version = trade["version"]
            maturity_date = datetime.strptime(trade["maturity_date"], "%Y-%m-%d").date()
            # process_trade(msg.value)

            consumer.commit()
            count += 1

            if count >= max_messages:
                print(f"Processed {count} messages, exiting task.")
                break

        except Exception as e:
            print("Processing failed, not committing offset:", e)
            break

    consumer.close()