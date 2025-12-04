import os

from dotenv import find_dotenv
from pydantic_settings import BaseSettings

airflow_env = os.getenv("AIRFLOW_ENV")


class EnvVarsSettings(BaseSettings):
    airflow_conn_source_postgres: str
    airflow_conn_sink_postgres: str
    airflow_env: str
    general_config_sql_ddl_path: str

    class Config:
        # env_prefix = "DB_"
        env_file = find_dotenv(f".env.{airflow_env}")
        env_file_encoding = "utf-8"
        extra = "ignore"


conf_settings = EnvVarsSettings()
