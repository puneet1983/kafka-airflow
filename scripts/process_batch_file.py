import json
import logging
from datetime import datetime, date
from schema import get_connection, create_all_tables

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def insert_staging_valid(con, trade, batch_id, status):
    con.execute("""
        INSERT INTO staging_valid_trades
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        batch_id,
        trade["trade_id"], trade["instrument"], trade["version"],
        trade["trade_date"], trade["maturity_date"],
        trade["price"], trade["quantity"], trade["counterparty"],
        status, datetime.now()
    ))


def insert_staging_rejected(con, trade, batch_id, reason):
    con.execute("""
        INSERT INTO staging_rejected_trades
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        batch_id,
        trade["trade_id"], trade["instrument"], trade["version"],
        trade["trade_date"], trade["maturity_date"],
        trade["price"], trade["quantity"], trade["counterparty"],
        reason, datetime.now()
    ))


def validate_trade_only(con, trade, batch_id):
    # Version check vs FINAL table
    existing = con.execute("""
        SELECT version FROM valid_trades
        WHERE trade_id = ?
        ORDER BY version DESC LIMIT 1
    """, [trade["trade_id"]]).fetchone()
    input_trade_id=trade["trade_id"]
    if existing and trade["version"] < existing[0]:
        insert_staging_rejected(con, trade, batch_id, "Lower version than existing")
        logger.info(f"TradeId : {input_trade_id} rejected due to version check ")
        return

    # Maturity check
    maturity_date = datetime.strptime(trade["maturity_date"], "%Y-%m-%d").date()
    if maturity_date < date.today():
        insert_staging_rejected(con, trade, batch_id, "Maturity date earlier than today")
        logger.info(f"TradeId : {input_trade_id} rejected due to Maturity date check ")
        
        return

    insert_staging_valid(con, trade, batch_id, "VALID")
    logger.info(f"TradeId : {input_trade_id}  valid trade id processed. ")
       

def process_staging_file(file_path: str, batch_id: str):
    logger.info("Processing file to staging | batch_id=%s | file=%s", batch_id, file_path)
    if not file_path:
        print("No file to process. Skipping.")
        return
    con = get_connection()
    create_all_tables(con)

    with open(file_path, "r") as f:
        trades = json.load(f)
        logger.info("loaded trade file")

    for trade in trades:
        try:
            validate_trade_only(con, trade, batch_id)
        except Exception:
            logger.exception("Failed staging trade | batch_id=%s | trade=%s", batch_id, trade)

    con.close()
    logger.info("Staging done | batch_id=%s", batch_id)
