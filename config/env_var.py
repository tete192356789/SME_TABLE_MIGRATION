import os

from dotenv import find_dotenv
from pydantic_settings import BaseSettings

airflow_env = os.getenv("AIRFLOW_ENV")


class EnvVarsSettings(BaseSettings):
    airflow_env: str
    general_config_sql_ddl_path: str

    # log table detail
    log_table_name: str
    log_table_database: str
    log_table_schema: str
    # source db conn
    source_db_driver: str
    source_db_host: str
    source_db_username: str
    source_db_password: str
    source_db_port: int
    source_db_name: str
    # sink db conn
    sink_db_driver: str
    sink_db_host: str
    sink_db_username: str
    sink_db_password: str
    sink_db_port: int
    sink_db_name: str

    # log table db conn
    log_table_db_driver: str
    log_table_db_host: str
    log_table_db_username: str
    log_table_db_password: str
    log_table_db_port: int
    log_table_db_name: str

    class Config:
        # env_prefix = "DB_"
        env_file = find_dotenv(f".env.{airflow_env}")
        env_file_encoding = "utf-8"
        extra = "ignore"


conf_settings = EnvVarsSettings()
