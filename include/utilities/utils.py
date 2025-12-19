from typing import Literal
from urllib.parse import quote_plus

from config.env_var import EnvVarsSettings


def get_conn_string(conf_settings: EnvVarsSettings):
    conn_str_data = {}
    source_pw_parsed = quote_plus(conf_settings.source_db_password)
    sink_pw_parsed = quote_plus(conf_settings.sink_db_password)
    log_table_pw_parsed = quote_plus(conf_settings.log_table_db_password)
    conn_str_data["source_conn_str"] = (
        f"{conf_settings.source_db_driver}://{conf_settings.source_db_username}:{source_pw_parsed}@{conf_settings.source_db_host}:{conf_settings.source_db_port}/{conf_settings.source_db_name}"
    )
    conn_str_data["sink_conn_str"] = (
        f"{conf_settings.sink_db_driver}://{conf_settings.sink_db_username}:{sink_pw_parsed}@{conf_settings.sink_db_host}:{conf_settings.sink_db_port}/{conf_settings.sink_db_name}"
    )
    conn_str_data["log_table_conn_str"] = (
        f"{conf_settings.log_table_db_driver}://{conf_settings.log_table_db_username}:{log_table_pw_parsed}@{conf_settings.log_table_db_host}:{conf_settings.log_table_db_port}/{conf_settings.log_table_db_name}"
    )

    return conn_str_data


def get_table_name_for_query(
    table_conf: dict, type: Literal["source", "sink"], is_staging: bool = False
):
    type_table = "name" if type == "source" else "sink_table"
    staging_str = "staging_" if is_staging else ""
    if not table_conf[f"{type}_database"]:
        if not table_conf[f"{type}_schema"]:
            print("No db & schema")
            return f"{staging_str}{table_conf[type_table]}"
        else:
            print("No db")
            return (
                f"{table_conf[f'{type}_schema']}.{staging_str}{table_conf[type_table]}"
            )
    else:
        if table_conf[f"{type}_schema"]:
            return f"{table_conf[f'{type}_database']}.{table_conf[f'{type}_schema']}.{staging_str}{table_conf[type_table]}"
        else:
            return f"{table_conf[f'{type}_database']}.{staging_str}{table_conf[type_table]}"


def get_log_table_name(conf_settings: EnvVarsSettings):
    if not conf_settings.log_table_database:
        if not conf_settings.log_table_schema:
            return conf_settings.log_table_name
        else:
            return f"{conf_settings.log_table_schema}.{conf_settings.log_table_name}"
    else:
        if conf_settings.log_table_schema:
            return f"{conf_settings.log_table_database}.{conf_settings.log_table_schema}.{conf_settings.log_table_name}"
        else:
            return f"{conf_settings.log_table_database}.{conf_settings.log_table_name}"
