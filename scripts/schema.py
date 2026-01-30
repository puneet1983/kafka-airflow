import duckdb
import json
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.append("/opt/airflow/scripts")
import config as cfg

def get_connection(db_path=cfg.DB_CONNECTION_STRING) -> duckdb.DuckDBPyConnection:
    print("*****db_path:", db_path)
    return duckdb.connect(db_path)


def create_all_tables(con: duckdb.DuckDBPyConnection):
    # Final tables
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

    is_current BOOLEAN,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    record_version INTEGER
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

    is_current BOOLEAN,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    record_version INTEGER
    )
    """)

    # Staging tables
    con.execute("""
    CREATE TABLE IF NOT EXISTS staging_valid_trades (
        batch_id TEXT,
        trade_id TEXT,
        instrument TEXT,
        version INTEGER,
        trade_date DATE,
        maturity_date DATE,
        price DOUBLE,
        quantity INTEGER,
        counterparty TEXT,
        status TEXT,
        validated_at TIMESTAMP
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS staging_rejected_trades (
        batch_id TEXT,
        trade_id TEXT,
        instrument TEXT,
        version INTEGER,
        trade_date DATE,
        maturity_date DATE,
        price DOUBLE,
        quantity INTEGER,
        counterparty TEXT,
        reason TEXT,
        validated_at TIMESTAMP
    )
    """)
