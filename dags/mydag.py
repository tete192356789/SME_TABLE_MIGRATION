import datetime
import logging
import os
from functools import reduce

import pandas as pd
import yaml
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, asset, dag, task

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


@asset(schedule=combined_schedule)
def source_max_update_dt(**context):
    triggering_events = context.get("triggering_asset_events")

    if triggering_events:
        for event in triggering_events.values():
            for asset_event in event:
                logger.info(f"Triggered by asset: {asset_event.source_asset_uri}")
                logger.info(f"Event timestamp: {asset_event.timestamp}")
    trigger_asset = triggering_events.values()[-1][-1].source_asset_uri
    table_name = context["ti"].xcom_pull(
        dag_id=trigger_asset,
        task_ids=trigger_asset,
        key="return_value",
        include_prior_dates=True,
    )

    logger.info(table_name)
    # hook = PostgresHook(postgres_conn_id="source_postgres")
    # logger.info("Getting Max Updated Date From Source Table.")
    # logger.info(f"Source Table: {tables_config['tables']['source_table']}")
    # records = hook.get_records(
    #     f"SELECT MAX(updated_date) FROM {tables_config['tables']['source_table']};"
    # )
    # logger.info(f"Source Table Max Updated Date: {records[0][0]}")

    # return records


@asset(schedule=[Asset("source_max_update_dt")])
def sink_max_update_dt():
    hook = PostgresHook(postgres_conn_id="sink_postgres")
    logger.info("Getting Max Updated Date From Sink Table.")
    records = hook.get_records("SELECT MAX(updated_date) FROM sink_table;")
    logger.info(f"Sink Table Max Updated Date: {records[0][0]}")
    return records


@dag(
    schedule=(Asset("source_max_update_dt") & Asset("sink_max_update_dt")), tags=["sme"]
)
def date_comparison_dag():
    @task
    def get_date_from_both(**context):
        triggering_events = context.get("triggering_asset_events")
        logger.info(triggering_events)

        source_max_update_dt_data = context["ti"].xcom_pull(
            dag_id="source_max_update_dt",
            task_ids="source_max_update_dt",
            key="return_value",
            include_prior_dates=True,
        )

        sink_max_update_dt_data = context["ti"].xcom_pull(
            dag_id="sink_max_update_dt",
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
            dag_id="date_comparison_dag",
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

    get_date_from_both() >> compare_date() >> [update_task(), not_update_task()]


@dag(dag_id="incremental_update", schedule=[Asset("update_asset")], tags=["sme"])
def incremental_update():
    @task
    def get_source_data(
        **context,
    ):
        date_data = context["ti"].xcom_pull(
            dag_id="date_comparison_dag",
            task_ids="get_date_from_both",
            key="return_value",
            include_prior_dates=True,
        )
        logger.info(date_data)

        # Build incremental query
        if date_data[-1]["sink_dt"]:
            query = f"""
            SELECT * FROM source_table
            WHERE updated_date > '{date_data[-1]["sink_dt"]}'
            ORDER BY source_table
            """
        else:
            # First run - full load
            query = "SELECT * FROM source_table"

        # source_conn = source_hook.get_conn()
        # sink_conn = sink_hook.get_conn()
        # sink_cursor = sink_conn.cursor()
        source_conn = PostgresHook(postgres_conn_id="source_postgres").get_conn()
        sink_conn = PostgresHook(postgres_conn_id="sink_postgres").get_conn()
        sink_cursor = sink_conn.cursor()
        total_rows = 0
        primary_key = "id"

        # max_tracking_value = last_sync

        # Process in chunks
        for chunk in pd.read_sql(query, source_conn, chunksize=10000):
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
            INSERT INTO sink_table ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT ({primary_key}) DO UPDATE
            SET {update_clause}
            """
            # Execute batch upsert
            data = [tuple(row) for row in chunk.values]
            sink_cursor.executemany(upsert_query, data)
            sink_conn.commit()
            logger.info(f"Upserting {len(data)} rows Into Sink Table!!!")
            logger.info(upsert_query)
            logger.info(data)

    get_source_data()


date_comparison_dag()
incremental_update()
