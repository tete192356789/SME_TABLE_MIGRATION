import logging

from airflow.sdk import asset

logger = logging.getLogger(__name__)


@asset(schedule="@daily")
def source_table_trigger_asset():
    return "source_table"


@asset(schedule="@daily")
def source_table2_trigger_asset():
    return "source_table2"


@asset(schedule="@daily")
def source_table3_trigger_asset():
    return "source_table3"
