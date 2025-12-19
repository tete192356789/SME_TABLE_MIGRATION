import datetime
import logging

from airflow.sdk import asset

logger = logging.getLogger(__name__)


@asset(schedule="@daily")
def CTBank_Temp_trigger_asset():
    return {"name": "CTBank_Temp", "timestamp": datetime.datetime.now().isoformat()}


@asset(schedule="@daily")
def ms_table_trigger_asset():
    return {"name": "ms_table", "timestamp": datetime.datetime.now().isoformat()}
