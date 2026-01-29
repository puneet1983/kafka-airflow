# /opt/airflow/config/config.py

# ---------------- Kafka Config ----------------
KAFKA_TOPIC = "trade-events"
KAFKA_BROKER = "kafka:9092"
KAFKA_BATCH_SIZE = 10

# ---------------- File Paths -----------------
RAW_DATA_PATH = "/opt/airflow/raw/data"

# ---------------- Email Config ----------------
EMAIL_LIST = ["puneet.jaiswal@gmail.com"]
EMAIL_ON_SUCCESS = True
EMAIL_ON_FAILURE = True
EMAIL_ON_RETRY = False

# ---------------- DAG Defaults ----------------
DEFAULT_RETRIES = 3
RETRY_DELAY_MINUTES = 2
RETRY_DELAY_SECONDS = 10

#--------------table config ------------

# Database config
DB_CONNECTION_STRING = "/opt/airflow/duckdb/trades.db"

# Staging table names
STAGING_VALID_TABLE = "staging_valid_trades"
STAGING_REJECTED_TABLE = "staging_rejected_trades"
FINAL_VALID_TABLE = "valid_trades"
FINAL_REJECTED_TABLE = "rejected_trades"
