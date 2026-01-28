import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError,NoBrokersAvailable
import json

# Kafka broker
KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "trade-events"



def ensure_topic(topic_name="trade-events", bootstrap_servers="kafka:9092"):
    for _ in range(10):  # try 10 times
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            break
        except NoBrokersAvailable:
            print("Kafka broker not ready, retrying in 5 seconds...")
            time.sleep(10)
    else:
        raise Exception("Kafka broker not available after multiple retries")

    topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)

    try:
        admin.create_topics([topic])
        print(f"✅ Topic created: {topic_name}")
    except TopicAlreadyExistsError:
        print(f"ℹ️ Topic already exists: {topic_name}")
    finally:
        admin.close()

def generate_trade(no_of_trades, topic_name):
    i = 0
    today = datetime.utcnow().date()
    
    # Create the producer once
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks='all'
    )

    for _ in range(no_of_trades):
        maturity = today + timedelta(days=random.randint(-5, 30))
        trade = {
            "trade_id": f"T{random.randint(10000,99999)}",
            "instrument": random.choice(["BOND", "FX", "EQUITY"]),
            "version": random.randint(1, 3),
            "trade_date": str(today),
            "maturity_date": str(maturity),
            "price": round(random.uniform(95, 105), 2),
            "quantity": random.randint(100, 5000),
            "counterparty": random.choice(["CITI", "JPMC", "GS"])
        }

        producer.send(topic_name, value=trade)
        print(f"Published: {trade['trade_id']}")

    producer.flush()  # ensure all messages are sent
    producer.close()  # close connection