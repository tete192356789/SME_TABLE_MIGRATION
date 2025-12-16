import datetime
import logging
import os
from functools import reduce

import yaml
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sdk import Asset, dag, task

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

        for asset_key, events in triggering_events._events.items():
            for event in events:
                triggered_asset_name = event.asset.name
        logger.info(f"Triggered Asset name: {triggered_asset_name}")
        logger.info(event.timestamp)
        asset_info_pull = context["ti"].xcom_pull(
            dag_id=triggered_asset_name,
            task_ids=triggered_asset_name,
            key="return_value",
            include_prior_dates=True,
        )
        asset_info = (
            dict(asset_info_pull[-1])
            if isinstance(asset_info_pull, list)
            else dict(asset_info_pull)
        )

        logger.info(f"Triggered at Timestamp: {asset_info['timestamp']}")
        table_name = asset_info["name"]
        table_config = tables_config["tables"][table_name]

        logger.info(f"TABLE NAME: {table_name}")
        logger.info(f"TABLE CONFIG : {table_config}")

        context["ti"].xcom_push(key="table_config", value=table_config)
        context["ti"].xcom_push(key="execution_date", value=asset_info["timestamp"])

    @task
    def source_max_update_dt(**context):
        table_config_pull = context["ti"].xcom_pull(
            task_ids="get_table_name_from_triggered_asset", key="table_config"
        )[-1]
        table_config = (
            table_config_pull
            if isinstance(table_config_pull, list)
            else table_config_pull
        )

        hook = MySqlHook(mysql_conn_id="source_conn")
        logger.info("Getting Max Updated Date From Source Table.")
        logger.info(f"Source Table: {table_config['name']}")
        records = hook.get_records(
            f"SELECT MAX({table_config['updated_column']}) FROM {table_config['name']};"
        )
        logger.info(f"Source Table Max Updated Date: {records[0][0]}")

        return records

    @task
    def sink_max_update_dt(**context):
        table_config_pull = context["ti"].xcom_pull(
            task_ids="get_table_name_from_triggered_asset", key="table_config"
        )[-1]
        table_config = (
            table_config_pull
            if isinstance(table_config_pull, list)
            else table_config_pull
        )

        hook = MySqlHook(mysql_conn_id="sink_conn")
        logger.info("Getting Max Updated Date From Sink Table.")
        logger.info(f"Sink Table: {table_config['sink_table']}")
        records = hook.get_records(
            f"SELECT MAX({table_config['updated_column']}) FROM {table_config['sink_table']};"
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

        source_max_update_dt = source_max_update_dt_data[-1][0][0]
        sink_max_update_dt = sink_max_update_dt_data[-1][0][0]
        # source_max_update_dt = (
        #     source_max_update_dt_data[-1][0][0]
        #     if (source_max_update_dt_data[-1][0][0], datetime.datetime)
        #     else None
        # )
        # sink_max_update_dt = (
        #     sink_max_update_dt_data[-1][0][0]
        #     if (sink_max_update_dt_data[-1][0][0], datetime.datetime)
        #     else None
        # )
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


main_migration_dag()
