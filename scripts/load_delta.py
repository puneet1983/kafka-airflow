import logging
from datetime import datetime
from schema import get_connection, create_all_tables

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scd2_expire_current(con, table_name: str, staging_table: str, batch_id: str):
    sql = f"""
    UPDATE {table_name}
    SET is_current = FALSE,
        effective_to = CURRENT_TIMESTAMP
    WHERE trade_id IN (
        SELECT trade_id FROM {staging_table} WHERE batch_id = ?
    )
    AND is_current = TRUE;
    """
    con.execute(sql, [batch_id])

def scd2_insert_new(con, table_name: str, staging_table: str, batch_id: str, is_valid: bool = True):
    if is_valid:
        extra_col = "status"
        staging_extra_col = "status"
    else:
        extra_col = "reason"
        staging_extra_col = "reason"

    sql = f"""
    INSERT INTO {table_name} (
      trade_id, instrument, version, trade_date, maturity_date,
      price, quantity, counterparty, {extra_col},
      is_current, effective_from, effective_to, record_version
    )
    SELECT
      s.trade_id, s.instrument, s.version, s.trade_date, s.maturity_date,
      s.price, s.quantity, s.counterparty, s.{staging_extra_col},
      TRUE,
      CURRENT_TIMESTAMP,
      NULL,
      COALESCE((
        SELECT MAX(t.record_version)
        FROM {table_name} t
        WHERE t.trade_id = s.trade_id
      ), 0) + 1
    FROM {staging_table} s
    WHERE s.batch_id = ?;
    """
    con.execute(sql, [batch_id])

def merge_staging_to_final(batch_id: str):
    """
    Moves data from staging_* tables into final tables for one batch_id.
    """
    logger.info("Merging staging to final | batch_id=%s", batch_id)
    con = get_connection()
    create_all_tables(con)
    logger.info("Merging staging to final | batch_id=%s Transaction started", batch_id)
   
    con.execute("BEGIN;")
    # VALID
    scd2_expire_current(con, "valid_trades", "staging_valid_trades", batch_id)
    scd2_insert_new(con, "valid_trades", "staging_valid_trades", batch_id, is_valid=True)

    # REJECTED
    scd2_expire_current(con, "rejected_trades", "staging_rejected_trades", batch_id)
    scd2_insert_new(con, "rejected_trades", "staging_rejected_trades", batch_id, is_valid=False)

    con.execute("COMMIT;")

# def merge_staging_to_final(batch_id: str):
#     """
#     Moves data from staging_* tables into final tables for one batch_id.
#     """
#     logger.info("Merging staging to final | batch_id=%s", batch_id)

#     con = get_connection()
#     create_all_tables(con)

#     # 1️ Merge VALID trades
#     con.execute("""
#         DELETE FROM valid_trades
#         WHERE trade_id IN (
#             SELECT trade_id FROM staging_valid_trades
#             WHERE batch_id = ?
#         )
#     """, [batch_id])

#     con.execute("""
#         INSERT INTO valid_trades
#         SELECT
#             trade_id,
#             instrument,
#             version,
#             trade_date,
#             maturity_date,
#             price,
#             quantity,
#             counterparty,
#             status,
#             validated_at
#         FROM staging_valid_trades
#         WHERE batch_id = ?
#     """, [batch_id])

#     # 2 Insert REJECTED trades
#     con.execute("""
#         INSERT INTO rejected_trades
#         SELECT
#             trade_id,
#             instrument,
#             version,
#             trade_date,
#             maturity_date,
#             price,
#             quantity,
#             counterparty,
#             reason,
#             validated_at
#         FROM staging_rejected_trades
#         WHERE batch_id = ?
#     """, [batch_id])

#     #  Cleanup staging for that batch
#     # con.execute("DELETE FROM staging_valid_trades WHERE batch_id = ?", [batch_id])
#     # con.execute("DELETE FROM staging_rejected_trades WHERE batch_id = ?", [batch_id])

#     con.close()
#     logger.info("Merge complete | batch_id=%s", batch_id)
