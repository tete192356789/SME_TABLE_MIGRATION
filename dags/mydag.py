import datetime
import importlib
import logging
import os
from functools import reduce

import pandas as pd
import yaml
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, dag, task

from config.env_var import conf_settings

logger = logging.getLogger(__name__)

ENV = os.getenv("AIRFLOW_ENV")

base_path = os.getcwd()
tables_config_path = f"/opt/airflow/config/tables_config/tables_{ENV}.yaml"
with open(tables_config_path, "r") as f:
    tables_config = yaml.safe_load(f)

asset_names = [
    tables_config["tables"][table][0]["airflow_asset_name"]
    for table in tables_config["tables"]
]
assets = [Asset(asset) for asset in asset_names]
combined_schedule = reduce(lambda x, y: x | y, assets)


@dag(schedule=combined_schedule, tags=["sme"])
def main_migration_dag():
    @task
    def get_table_name_from_triggered_asset(**context):
        triggering_events = context.get("triggering_asset_events")
        logger.info(triggering_events._events)

        for asset_key, events in triggering_events._events.items():
            for event in events:
                triggered_asset_name = event.asset.name
        logger.info(f"Triggered Asset name: {triggered_asset_name}")
        logger.info(f"Triggered at Timestamp: {event.timestamp}")

        table_name = context["ti"].xcom_pull(
            dag_id=triggered_asset_name,
            task_ids=triggered_asset_name,
            key="return_value",
            include_prior_dates=True,
        )[-1]

        table_config = tables_config["tables"][table_name]

        logger.info(f"TABLE NAME: {table_name}")
        logger.info(f"TABLE CONFIG : {table_config}")

        context["ti"].xcom_push(key="table_config", value=table_config)

    @task
    def source_max_update_dt(**context):
        table_config = context["ti"].xcom_pull(
            task_ids="get_table_name_from_triggered_asset", key="table_config"
        )[-1]

        hook = PostgresHook(postgres_conn_id="source_postgres")
        logger.info("Getting Max Updated Date From Source Table.")
        logger.info(f"Source Table: {table_config['name']}")
        records = hook.get_records(
            f"SELECT MAX(updated_date) FROM {table_config['name']};"
        )
        logger.info(f"Source Table Max Updated Date: {records[0][0]}")

        return records

    @task
    def sink_max_update_dt(**context):
        table_config = context["ti"].xcom_pull(
            task_ids="get_table_name_from_triggered_asset", key="table_config"
        )[-1]

        hook = PostgresHook(postgres_conn_id="sink_postgres")
        logger.info("Getting Max Updated Date From Sink Table.")
        logger.info(f"Sink Table: {table_config['sink_table']}")
        records = hook.get_records(
            f"SELECT MAX(updated_date) FROM {table_config['sink_table']};"
        )
        logger.info(f"Sink Table Max Updated Date: {records[0][0]}")
        return records

    @task
    def get_date_from_both(**context):
        source_max_update_dt_data = context["ti"].xcom_pull(
            task_ids="source_max_update_dt",
            key="return_value",
            include_prior_dates=True,
        )

        sink_max_update_dt_data = context["ti"].xcom_pull(
            task_ids="sink_max_update_dt",
            key="return_value",
            include_prior_dates=True,
        )

        source_max_update_dt = (
            source_max_update_dt_data[-1][0][0].isoformat()
            if (source_max_update_dt_data[-1][0][0], datetime.datetime)
            else None
        )
        sink_max_update_dt = (
            sink_max_update_dt_data[-1][0][0].isoformat()
            if isinstance(sink_max_update_dt_data[-1][0][0], datetime.datetime)
            else None
        )
        logger.info(f"SOURCE UPDATED DT: {source_max_update_dt}")
        logger.info(f"SINK UPDATED DT: {sink_max_update_dt}")
        return {"source_dt": source_max_update_dt, "sink_dt": sink_max_update_dt}

    @task.branch
    def compare_date(**context):
        date_data = context["ti"].xcom_pull(
            task_ids="get_date_from_both",
            key="return_value",
            include_prior_dates=True,
        )

        source_date = (
            date_data[-1]["source_dt"]
            if isinstance(date_data, list)
            else date_data["source_dt"]
        )
        sink_date = (
            date_data[-1]["sink_dt"]
            if isinstance(date_data, list)
            else date_data["sink_dt"]
        )
        if not sink_date:
            logger.info("Sink date is None. Updating!!!")
            return "update_task"
        if not isinstance(
            datetime.datetime.fromisoformat(sink_date), datetime.datetime
        ):
            logger.info("Sink date is not type datetime. Updating!!!")
            return "update_task"
        if source_date > sink_date:
            logger.info("Source Date More Than Sink Date. Updating!!!")
            return "update_task"
        logger.info(date_data)
        logger.info(f"source dt : {source_date}")
        logger.info(f"sink dt : {sink_date}")
        return "not_update_task"

    @task(outlets=[Asset("update_asset")])
    def update_task(**context):
        logger.info("Updating update_asset !!!")

    @task(outlets=[Asset("not_update_asset")])
    def not_update_task(**context):
        logger.info("Updating not_update_asset !!!")

    (
        get_table_name_from_triggered_asset()
        >> source_max_update_dt()
        >> sink_max_update_dt()
        >> get_date_from_both()
        >> compare_date()
        >> [update_task(), not_update_task()]
    )


@dag(dag_id="incremental_update", schedule=[Asset("update_asset")], tags=["sme"])
def incremental_update():
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
        except Exception as e:
            raise Exception(f"Truncate Staging Table Failed: {e}")

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

        sink_conn.commit()
        sink_cursor.close()
        sink_conn.close()
        source_engine.dispose()
        sink_engine.dispose()

        logger.info(
            f"Upserting {total_rows} rows Into Staging Sink Table --> (staging.{table_config['sink_table']})!!!"
        )

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
                [f'"{col}" = EXCLUDED."{col}"' for col in columns if col != primary_key]
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
                f"Upserting {len(data)} rows Into Sink Table --> ({table_config['sink_table']})!!!"
            )
            logger.info(upsert_query)
            logger.info(data)

    (
        read_sink_ddl_query()
        >> check_sink_staging_exist()
        >> get_source_query()
        >> load_source_to_staging()
        >> load_staging_to_sink()
    )


main_migration_dag()
incremental_update()
