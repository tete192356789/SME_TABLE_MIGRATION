import datetime
import logging

from airflow.providers.mysql.hooks.mysql import MySqlHook

logger = logging.getLogger(__name__)


def _log_error_to_audit_table(audit_id: int, task_id: str):
    """Helper function to log errors to audit table"""
    sink_hook = MySqlHook(mysql_conn_id="sink_conn")
    sink_conn = sink_hook.get_conn()
    sink_cursor = sink_conn.cursor()

    try:
        # Get start time
        sink_cursor.execute(
            "SELECT start_date FROM migration_audit WHERE id = %s", (audit_id,)
        )
        result = sink_cursor.fetchone()
        start_time = result[0] if result else None

        end_time = datetime.datetime.now()
        duration = int((end_time - start_time).total_seconds()) if start_time else None

        update_query = """
        UPDATE migration_audit SET
            status = %s,
            err_task_id = %s,
            end_date = %s,
            duration_seconds = %s
        WHERE id = %s
        """

        sink_cursor.execute(
            update_query,
            (
                "FAILED",
                task_id,
                end_time,
                duration,
                audit_id,
            ),
        )

        sink_conn.commit()
        logger.info(f"Error logged to audit table - Task: {task_id}")
    except Exception as log_error:
        logger.error(f"Failed to log error to audit table: {log_error}")
    finally:
        sink_cursor.close()
        sink_conn.close()
