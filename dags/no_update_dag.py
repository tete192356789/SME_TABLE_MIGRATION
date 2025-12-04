import datetime
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, dag, task

logger = logging.getLogger(__name__)


@dag(dag_id="no_update", schedule=[Asset("not_update_asset")], tags=["sme"])
def no_update():
    @task
    def create_audit_table(**context):
        """Create audit table if not exists"""
        sink_hook = PostgresHook(postgres_conn_id="sink_postgres")
        sink_conn = sink_hook.get_conn()
        sink_cursor = sink_conn.cursor()
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS migration_audit (
                id SERIAL PRIMARY KEY,
                dag_id VARCHAR(255),
                run_id VARCHAR(255),
                status VARCHAR(255),
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                source_table VARCHAR(255),
                sink_table VARCHAR(255),
                source_count BIGINT,
                updated_count BIGINT,
                inserted_count BIGINT,
                sink_count_before BIGINT,
                sink_count_after BIGINT,
                duration_seconds INT,
                err_task_id VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            logger.info("Audit table created/verified")

        except Exception as e:
            logger.error(f"Failed to create audit table: {e}")
        finally:
            sink_cursor.execute(create_table_query)
            sink_conn.commit()
            sink_cursor.close()
            sink_conn.close()

    @task
    def init_audit_log(**context):
        """Initialize audit log entry"""
        table_config = context["ti"].xcom_pull(
            dag_id="main_migration_dag",
            task_ids="get_table_name_from_triggered_asset",
            key="table_config",
            include_prior_dates=True,
        )[-1][-1]

        execution_date = context["ti"].xcom_pull(
            dag_id="main_migration_dag",
            task_ids="get_table_name_from_triggered_asset",
            key="execution_date",
            include_prior_dates=True,
        )[-1]

        sink_hook = PostgresHook(postgres_conn_id="sink_postgres")
        sink_conn = sink_hook.get_conn()
        sink_cursor = sink_conn.cursor()

        insert_query = """
        INSERT INTO migration_audit (
            dag_id, run_id, start_date, source_table, sink_table,status
            
        ) VALUES (%s, %s, %s, %s, %s,%s)
        RETURNING id;
        """

        sink_cursor.execute(
            insert_query,
            (
                context["dag"].dag_id,
                context["run_id"],
                execution_date.isoformat(),
                table_config["name"],
                table_config["sink_table"],
                "NO UPDATE",
            ),
        )

        audit_id = sink_cursor.fetchone()[0]
        sink_conn.commit()
        sink_cursor.close()
        sink_conn.close()

        logger.info(f"Audit log initialized with ID: {audit_id}")
        return audit_id

    @task
    def insert_audit_log(**context):
        audit_id = context["ti"].xcom_pull(
            task_ids="init_audit_log", key="return_value", include_prior_dates=True
        )[-1]

        sink_hook = PostgresHook(postgres_conn_id="sink_postgres")
        sink_conn = sink_hook.get_conn()
        sink_cursor = sink_conn.cursor()

        sink_cursor.execute(
            "SELECT start_date FROM migration_audit WHERE id = %s", (audit_id,)
        )
        start_date = sink_cursor.fetchone()[0]
        end_date = datetime.datetime.now()
        duration = int((end_date - start_date).total_seconds())

        update_query = """
        UPDATE migration_audit SET
            end_date = %s,
            duration_seconds = %s
        WHERE id = %s
        """

        sink_cursor.execute(
            update_query,
            (
                end_date,
                duration,
                audit_id,
            ),
        )
        sink_conn.commit()
        sink_cursor.close()
        sink_conn.close()

        logger.info("Updated to audit log")

    create_audit_table() >> init_audit_log() >> insert_audit_log()


no_update()
