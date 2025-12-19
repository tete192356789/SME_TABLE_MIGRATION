import datetime
import importlib
import logging
import os

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sdk import Asset, dag, task

from config.env_var import conf_settings
from include.utilities.utils import (
    get_conn_string,
    get_log_table_name,
    get_table_name_for_query,
)

logger = logging.getLogger(__name__)

conn_str_data = get_conn_string(conf_settings=conf_settings)
os.environ["AIRFLOW_CONN_SOURCE_CONN"] = conn_str_data["source_conn_str"]
os.environ["AIRFLOW_CONN_SINK_CONN"] = conn_str_data["sink_conn_str"]
os.environ["AIRFLOW_CONN_LOG_TABLE_CONN"] = conn_str_data["log_table_conn_str"]


@dag(dag_id="no_update", schedule=[Asset("not_update_asset")], tags=["sme"])
def no_update():
    @task
    def create_audit_table(**context):
        """Create audit table if not exists"""
        sink_hook = MySqlHook(mysql_conn_id="log_table_conn")
        sink_conn = sink_hook.get_conn()
        sink_cursor = sink_conn.cursor()
        try:
            sql_ddl_path = conf_settings.general_config_sql_ddl_path
            if "/" in sql_ddl_path:
                sql_ddl_path = sql_ddl_path.strip("/").replace("/", ".")
            create_table_query = importlib.import_module(
                f"{sql_ddl_path}.{conf_settings.log_table_name}"
            ).QUERY

            logger.info("Audit table created/verified")
            logger.info(create_table_query)
        except Exception as e:
            logger.error(f"Failed to create audit table: {e}")
        finally:
            log_table_name = get_log_table_name(conf_settings=conf_settings)
            sink_cursor.execute(
                create_table_query.format(LOG_TABLE_NAME=log_table_name)
            )
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
        sink_hook = MySqlHook(mysql_conn_id="log_table_conn")
        sink_conn = sink_hook.get_conn()
        sink_cursor = sink_conn.cursor()

        log_table_name = get_log_table_name(conf_settings=conf_settings)
        insert_query = f"""
        INSERT INTO {log_table_name} (
            dag_id, run_id, start_date, source_table, sink_table, status
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """

        source_table_name = get_table_name_for_query(
            table_conf=table_config, type="source"
        )
        sink_table_name = get_table_name_for_query(table_conf=table_config, type="sink")
        sink_cursor.execute(
            insert_query,
            (
                context["dag"].dag_id,
                context["run_id"],
                execution_date,
                source_table_name,
                sink_table_name,
                "NO UPDATE",
            ),
        )
        audit_id = sink_cursor.lastrowid  # MySQL way to get last inserted ID
        sink_conn.commit()
        sink_cursor.close()
        sink_conn.close()
        logger.info(f"Audit log initialized with ID: {audit_id}")
        return audit_id

    @task
    def insert_audit_log(**context):
        audit_id = context["ti"].xcom_pull(
            task_ids="init_audit_log",
            key="audit_id_no_update",
            include_prior_dates=True,
        )[-1]
        logger.info(
            context["ti"].xcom_pull(
                task_ids="init_audit_log",
                key="audit_id_no_update",
                include_prior_dates=True,
            )
        )
        logger.info(audit_id)
        sink_hook = MySqlHook(mysql_conn_id="log_table_conn")
        sink_conn = sink_hook.get_conn()
        sink_cursor = sink_conn.cursor()
        log_table_name = get_log_table_name(conf_settings=conf_settings)
        sink_cursor.execute(
            f"SELECT start_date FROM {log_table_name} WHERE id = %s", (audit_id,)
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
