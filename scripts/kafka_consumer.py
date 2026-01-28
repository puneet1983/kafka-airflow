import json
import duckdb
# import duckdb_engine as duckdb
from kafka import KafkaConsumer
from datetime import datetime, date
from kafka.admin import KafkaAdminClient, NewTopic
import time
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def wait_for_topic(topic, bootstrap_servers):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    while topic not in admin.list_topics():
        time.sleep(10)

# DuckDB setup


def get_connection(db_path="/opt/airflow/duckdb/trades.db") -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection object."""
    return duckdb.connect(db_path)


def insert_trade(con: duckdb.DuckDBPyConnection, table: str, trade: dict, status_or_reason: str):
    """
    Insert a trade into valid_trades or rejected_trades.

    con: DuckDB connection object
    table: "valid_trades" or "rejected_trades"
    status_or_reason: status for valid trades or reason for rejected trades
    """
    if table == "valid_trades":
        # Remove existing same version
        con.execute("DELETE FROM valid_trades WHERE trade_id = ? AND version = ?", 
                    [trade["trade_id"], trade["version"]])

        con.execute("""
            INSERT INTO valid_trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade["trade_id"], trade["instrument"], trade["version"],
            trade["trade_date"], trade["maturity_date"],
            trade["price"], trade["quantity"], trade["counterparty"],
            status_or_reason, datetime.now()
        ))
        print(f"Accepted: {trade['trade_id']} v{trade['version']}")

    elif table == "rejected_trades":
        con.execute("""
            INSERT INTO rejected_trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade["trade_id"], trade["instrument"], trade["version"],
            trade["trade_date"], trade["maturity_date"],
            trade["price"], trade["quantity"], trade["counterparty"],
            status_or_reason, datetime.now()
        ))
        print(f"Rejected ({status_or_reason}): {trade['trade_id']}")
        logger.warning("Rejected (%s): %s", status_or_reason, trade["trade_id"])


def check_version(con: duckdb.DuckDBPyConnection, trade: dict) -> bool:
    """
    Returns True if trade passes version check.
    Inserts into rejected_trades if version is lower than existing.
    """
    existing = con.execute("""
        SELECT version FROM valid_trades
        WHERE trade_id = ?
        ORDER BY version DESC LIMIT 1
    """, [trade["trade_id"]]).fetchone()

    if existing and trade["version"] < existing[0]:
        insert_trade(con, "rejected_trades", trade, "Lower version than existing")
        return False

    return True

def check_maturity(con: duckdb.DuckDBPyConnection, trade: dict) -> bool:
    """
    Returns True if trade maturity date is valid.
    Inserts into rejected_trades if maturity date is earlier than today.
    """
    today = date.today()
    maturity_date = datetime.strptime(trade["maturity_date"], "%Y-%m-%d").date()

    if maturity_date < today:
        insert_trade(con, "rejected_trades", trade, "Maturity date earlier than today")
        return False

    return True

def validate_and_process_trade(con: duckdb.DuckDBPyConnection, trade: dict):
    """
    Validate a trade and insert into valid or rejected trades table.
    """
    # Step 1: Version check
    if not check_version(con, trade):
        return

    # Step 2: Maturity check
    if not check_maturity(con, trade):
        return

    # Step 3: All checks passed → insert as valid trade
    try:
        insert_trade(con, "valid_trades", trade, "VALID")
    except Exception as e:
        print("its failed with while instering valid trades")
        raise 

def createtable(con: duckdb.DuckDBPyConnection):
    con.execute("""
    CREATE TABLE IF NOT EXISTS valid_trades (
        trade_id TEXT,
        instrument TEXT,
        version INTEGER,
        trade_date DATE,
        maturity_date DATE,
        price DOUBLE,
        quantity INTEGER,
        counterparty TEXT,
        status TEXT,
        processed_at TIMESTAMP
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS rejected_trades (
        trade_id TEXT,
        instrument TEXT,
        version INTEGER,
        trade_date DATE,
        maturity_date DATE,
        price DOUBLE,
        quantity INTEGER,
        counterparty TEXT,
        reason TEXT,
        processed_at TIMESTAMP
    )
    """)


def safe_json_deserializer(v):
    if v is None or v == b'':
        return None
    try:
        return json.loads(v.decode("utf-8"))
    except Exception as e:
        print("Skipping invalid JSON message:", v, "Error:", e)
        logger.error("Skipping invalid JSON: %s | Error: %s", v, e)
        return None
    

def run_trade_consumer(max_messages=20):
    consumer = KafkaConsumer(
        "trade-events",
        bootstrap_servers="kafka:9092",
        value_deserializer=safe_json_deserializer,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="trade-debug-consumer-2",
        max_poll_records=10
    )

    # print("Kafka Consumer started. Waiting for messages...")
    logger.info("Kafka Consumer started. Waiting for messages...")
    

    count = 0
    for msg in consumer:
        if msg.value is None:
            consumer.commit()
            continue

        try:
            print("Received message:", msg.value)
            now = datetime.now()

            trade = msg.value
            trade_id = trade["trade_id"]
            version = trade["version"]
            maturity_date = datetime.strptime(trade["maturity_date"], "%Y-%m-%d").date()
            # process_trade(msg.value)
            con = get_connection()
            createtable(con)
            validate_and_process_trade(con,trade)
            consumer.commit()
            count += 1

            if count >= max_messages:
                # print(f"Processed {count} messages, exiting task.")
                logger.info("Processed %s messages, exiting task.", count)
                break

        except Exception as e:
            print("Processing failed, not committing offset:", e)
            logger.exception("Processing failed, not committing offset")
            break

    consumer.close()
   







