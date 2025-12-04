import datetime
import importlib
import logging

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, dag, task

from config.env_var import conf_settings

logger = logging.getLogger(__name__)


@dag(dag_id="incremental_update", schedule=[Asset("update_asset")], tags=["sme"])
def incremental_update():
    @task
    def create_audit_table(**context):
        """Create audit table if not exists"""
        sink_hook = PostgresHook(postgres_conn_id="sink_postgres")
        sink_conn = sink_hook.get_conn()
        sink_cursor = sink_conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS migration_audit (
            id SERIAL PRIMARY KEY,
            dag_id VARCHAR(255),
            run_id VARCHAR(255),
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        sink_cursor.execute(create_table_query)
        sink_conn.commit()
        sink_cursor.close()
        sink_conn.close()
        logger.info("Audit table created/verified")
        logger.info(context)

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
            dag_id, run_id, start_date, source_table, sink_table
            
        ) VALUES (%s, %s, %s, %s, %s)
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
            ),
        )

        audit_id = sink_cursor.fetchone()[0]
        sink_conn.commit()
        sink_cursor.close()
        sink_conn.close()

        logger.info(f"Audit log initialized with ID: {audit_id}")
        return audit_id

    @task
    def get_source_count(**context):
        """Get source row count for audit"""
        table_config = context["ti"].xcom_pull(
            dag_id="main_migration_dag",
            task_ids="get_table_name_from_triggered_asset",
            key="table_config",
            include_prior_dates=True,
        )[-1][-1]

        source_hook = PostgresHook(postgres_conn_id="source_postgres")
        count_query = f"SELECT COUNT(*) FROM {table_config['name']}"

        result = source_hook.get_records(count_query)
        source_count = result[0][0]

        logger.info(f"Source Table Rows Count: {source_count}")
        return source_count

    @task
    def read_sink_ddl_query(**context):
        table_config = context["ti"].xcom_pull(
            dag_id="main_migration_dag",
            task_ids="get_table_name_from_triggered_asset",
            key="table_config",
            include_prior_dates=True,
        )[-1][-1]

        sql_ddl_path = conf_settings.general_config_sql_ddl_path
        if "/" in sql_ddl_path:
            sql_ddl_path = sql_ddl_path.strip("/").replace("/", ".")
        query_module = importlib.import_module(
            f"{sql_ddl_path}.{table_config['sink_table']}"
        )
        return query_module.QUERY

    @task
    def check_sink_staging_exist(**context):
        sink_conn = PostgresHook(postgres_conn_id="sink_postgres").get_conn()
        sink_cursor = sink_conn.cursor()

        table_config = context["ti"].xcom_pull(
            dag_id="main_migration_dag",
            task_ids="get_table_name_from_triggered_asset",
            key="table_config",
            include_prior_dates=True,
        )[-1][-1]
        ddl_query = context["ti"].xcom_pull(
            dag_id="incremental_update",
            task_ids="read_sink_ddl_query",
            key="return_value",
            include_prior_dates=True,
        )[-1]

        ddl_query_format = ddl_query.format(
            FULL_TABLE_NAME=f"staging.{table_config['sink_table']}"
        )
        sink_cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")

        logger.info("CHECK TABLE EXIST OR NOT.")
        sink_cursor.execute(
            f"""    
        SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'staging'
        AND table_name = '{table_config["sink_table"]}'
        )   
        """,
        )
        table_exist = sink_cursor.fetchone()[-1]
        if not table_exist:
            logger.info("TABLE DID NOT EXIST, CREATING ...")
            sink_cursor.execute(ddl_query_format)

        sink_cursor.execute(ddl_query_format)
        sink_conn.commit()
        sink_cursor.close()
        sink_conn.close()

    @task
    def get_source_query(
        **context,
    ):
        date_data = context["ti"].xcom_pull(
            dag_id="main_migration_dag",
            task_ids="get_date_from_both",
            key="return_value",
            include_prior_dates=True,
        )

        table_config = context["ti"].xcom_pull(
            dag_id="main_migration_dag",
            task_ids="get_table_name_from_triggered_asset",
            key="table_config",
            include_prior_dates=True,
        )[-1][-1]
        logger.info(date_data)
        logger.info(table_config)
        # Build incremental query
        if date_data[-1]["sink_dt"]:
            query = f"""
            SELECT * FROM {table_config["name"]}
            WHERE {table_config["updated_column"]} > '{date_data[-1]["sink_dt"]}'
            ORDER BY {table_config["name"]};
            """
        else:
            # First run - full load
            query = f"SELECT * FROM {table_config['name']}"
        logger.info(f"QUERY: {query}")
        context["ti"].xcom_push(key="source_query", value=query)

    @task
    def load_source_to_staging(**context):
        table_config = context["ti"].xcom_pull(
            dag_id="main_migration_dag",
            task_ids="get_table_name_from_triggered_asset",
            key="table_config",
            include_prior_dates=True,
        )[-1][-1]

        query = context["ti"].xcom_pull(
            dag_id="incremental_update",
            task_ids="get_source_query",
            key="source_query",
            include_prior_dates=True,
        )[-1]
        logger.info(f"QUERY : {query}")
        source_hook = PostgresHook(postgres_conn_id="source_postgres")
        sink_hook = PostgresHook(postgres_conn_id="sink_postgres")

        # Get raw connection for cursor operations
        sink_conn = sink_hook.get_conn()
        sink_cursor = sink_conn.cursor()

        # Get SQLAlchemy engine for pandas
        source_engine = source_hook.get_sqlalchemy_engine()
        sink_engine = sink_hook.get_sqlalchemy_engine()

        total_rows = 0

        # truncate staging table
        try:
            sink_cursor.execute(f"TRUNCATE TABLE staging.{table_config['sink_table']}")
            sink_conn.commit()
            logger.info(
                f"Staging table truncated: staging.{table_config['sink_table']}"
            )
        except Exception as e:
            logger.error(f"Truncate Staging Table Failed: {e}")
            raise

        sink_cursor.execute(
            f"SELECT COUNT(*) FROM staging.{table_config['sink_table']}"
        )
        count_stg = sink_cursor.fetchone()[0]

        if count_stg != 0:
            raise ValueError("Staging Table Didn't Truncate Yet.")

        chunksize = (
            table_config["insert_chunk_size"]
            if table_config["insert_chunk_size"]
            else 10000
        )

        try:
            # Process in chunks
            for chunk in pd.read_sql(query, source_engine, chunksize=chunksize):
                if chunk.empty:
                    break

                total_rows += len(chunk)
                chunk.to_sql(
                    name=table_config["sink_table"],
                    schema="staging",
                    con=sink_engine,
                    if_exists="append",
                    method="multi",
                    index=False,
                )
                logger.info(
                    f"Upserting {total_rows} rows Into Staging Sink Table --> (staging.{table_config['sink_table']})!!!"
                )
        except Exception as e:
            logger.error(f"Failed to load data to staging: {e}")
            raise
        finally:
            sink_conn.commit()
            sink_cursor.close()
            sink_conn.close()
            source_engine.dispose()
            sink_engine.dispose()

        return total_rows

    @task
    def get_sink_count_before(**context):
        """Get sink table count before upsert"""
        table_config = context["ti"].xcom_pull(
            dag_id="main_migration_dag",
            task_ids="get_table_name_from_triggered_asset",
            key="table_config",
            include_prior_dates=True,
        )[-1][-1]

        sink_hook = PostgresHook(postgres_conn_id="sink_postgres")
        result = sink_hook.get_records(
            f"SELECT COUNT(*) FROM {table_config['sink_table']}"
        )
        sink_count_before = result[0][0]
        logger.info(f"Sink count before upsert: {sink_count_before}")
        return sink_count_before

    @task
    def get_sink_count_after(**context):
        """Get sink table count after upsert"""
        table_config = context["ti"].xcom_pull(
            dag_id="main_migration_dag",
            task_ids="get_table_name_from_triggered_asset",
            key="table_config",
            include_prior_dates=True,
        )[-1][-1]

        sink_hook = PostgresHook(postgres_conn_id="sink_postgres")
        result = sink_hook.get_records(
            f"SELECT COUNT(*) FROM {table_config['sink_table']}"
        )
        sink_count_after = result[0][0]
        logger.info(f"Sink count after upsert: {sink_count_after}")
        return sink_count_after

    @task
    def get_sink_staging_count(**context):
        """Get staging sink table count upsert"""
        table_config = context["ti"].xcom_pull(
            dag_id="main_migration_dag",
            task_ids="get_table_name_from_triggered_asset",
            key="table_config",
            include_prior_dates=True,
        )[-1][-1]

        sink_hook = PostgresHook(postgres_conn_id="sink_postgres")
        result = sink_hook.get_records(
            f"SELECT COUNT(*) FROM staging.{table_config['sink_table']}"
        )
        sink_staging_count = result[0][0]
        logger.info(f"Staging Sink count upsert: {sink_staging_count}")
        return sink_staging_count

    @task
    def load_staging_to_sink(**context):
        table_config = context["ti"].xcom_pull(
            dag_id="main_migration_dag",
            task_ids="get_table_name_from_triggered_asset",
            key="table_config",
            include_prior_dates=True,
        )[-1][-1]

        query = f"""SELECT * FROM staging.{table_config["sink_table"]}"""

        sink_conn = PostgresHook(postgres_conn_id="sink_postgres").get_conn()
        sink_cursor = sink_conn.cursor()
        total_rows = 0
        primary_key = table_config["primary_key"]

        chunksize = (
            table_config["insert_chunk_size"]
            if table_config["insert_chunk_size"]
            else 10000
        )

        try:
            # Process in chunks
            for chunk in pd.read_sql(query, sink_conn, chunksize=chunksize):
                if chunk.empty:
                    break

                total_rows += len(chunk)
                # Get column names
                columns = chunk.columns.tolist()
                placeholders = ", ".join(["%s"] * len(columns))
                columns_str = ", ".join([f'"{col}"' for col in columns])
                # Build upsert query (PostgreSQL syntax)
                update_clause = ", ".join(
                    [
                        f'"{col}" = EXCLUDED."{col}"'
                        for col in columns
                        if col != primary_key
                    ]
                )
                upsert_query = f"""
                INSERT INTO {table_config["sink_table"]} ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT ({primary_key}) DO UPDATE
                SET {update_clause}
                """
                # Execute batch upsert
                data = [tuple(row) for row in chunk.values]
                sink_cursor.executemany(upsert_query, data)
                sink_conn.commit()
            logger.info(
                f"Upserting {total_rows} rows Into Sink Table --> ({table_config['sink_table']})!!!"
            )
            logger.info(upsert_query)
        except Exception as e:
            logger.error(f"Failed to load data to staging: {e}")
            raise
        finally:
            sink_conn.commit()
            sink_cursor.close()
            sink_conn.close()

        return total_rows

    @task
    def finalize_audit_log(**context):
        """Update audit log with final results"""
        audit_id = context["ti"].xcom_pull(
            task_ids="init_audit_log", key="return_value", include_prior_dates=True
        )[-1]

        sink_before = context["ti"].xcom_pull(
            dag_id="incremental_update",
            task_ids="get_sink_count_before",
            key="return_value",
            include_prior_dates=True,
        )[-1]

        sink_after = context["ti"].xcom_pull(
            dag_id="incremental_update",
            task_ids="get_sink_count_after",
            key="return_value",
            include_prior_dates=True,
        )[-1]

        source_count = context["ti"].xcom_pull(
            dag_id="incremental_update",
            task_ids="get_source_count",
            key="return_value",
            include_prior_dates=True,
        )[-1]

        sink_staging_count = context["ti"].xcom_pull(
            dag_id="incremental_update",
            task_ids="get_sink_staging_count",
            key="return_value",
            include_prior_dates=True,
        )[-1]

        inserted_count = sink_after - sink_before
        updated_count = sink_staging_count - inserted_count

        # Get start time from audit table
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
            source_count = %s,
            updated_count = %s,
            inserted_count = %s,
            sink_count_before = %s,
            sink_count_after = %s,
            end_date = %s,
            duration_seconds = %s
        WHERE id = %s
        """

        sink_cursor.execute(
            update_query,
            (
                source_count,
                updated_count,
                inserted_count,
                sink_before,
                sink_after,
                end_date,
                duration,
                audit_id,
            ),
        )

        sink_conn.commit()
        sink_cursor.close()
        sink_conn.close()

        logger.info(f"Audit log finalized -  Duration: {duration}s")

    (
        create_audit_table()
        >> init_audit_log()
        >> get_source_count()
        >> get_sink_count_before()
        >> read_sink_ddl_query()
        >> check_sink_staging_exist()
        >> get_source_query()
        >> load_source_to_staging()
        >> get_sink_staging_count()
        >> load_staging_to_sink()
        >> get_sink_count_after()
        >> finalize_audit_log()
    )


incremental_update()
