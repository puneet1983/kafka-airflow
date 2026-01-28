import logging
from datetime import datetime
from schema import get_connection, create_all_tables

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def merge_staging_to_final(batch_id: str):
    """
    Moves data from staging_* tables into final tables for one batch_id.
    """
    logger.info("Merging staging to final | batch_id=%s", batch_id)

    con = get_connection()
    create_all_tables(con)

    # 1️ Merge VALID trades
    con.execute("""
        DELETE FROM valid_trades
        WHERE trade_id IN (
            SELECT trade_id FROM staging_valid_trades
            WHERE batch_id = ?
        )
    """, [batch_id])

    con.execute("""
        INSERT INTO valid_trades
        SELECT
            trade_id,
            instrument,
            version,
            trade_date,
            maturity_date,
            price,
            quantity,
            counterparty,
            status,
            validated_at
        FROM staging_valid_trades
        WHERE batch_id = ?
    """, [batch_id])

    # 2 Insert REJECTED trades
    con.execute("""
        INSERT INTO rejected_trades
        SELECT
            trade_id,
            instrument,
            version,
            trade_date,
            maturity_date,
            price,
            quantity,
            counterparty,
            reason,
            validated_at
        FROM staging_rejected_trades
        WHERE batch_id = ?
    """, [batch_id])

    #  Cleanup staging for that batch
    # con.execute("DELETE FROM staging_valid_trades WHERE batch_id = ?", [batch_id])
    # con.execute("DELETE FROM staging_rejected_trades WHERE batch_id = ?", [batch_id])

    con.close()
    logger.info("Merge complete | batch_id=%s", batch_id)
