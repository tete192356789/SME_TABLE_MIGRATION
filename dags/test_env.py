from airflow.sdk import dag, task

from config.env_var import conf_settings


@dag(dag_id="test_env", schedule="@daily", tags=["test_env"])
def test_env_var():
    @task
    def print_conf_settings():
        print(conf_settings)
        print("###########")
        print(conf_settings.general_config_sql_ddl_path)
        return conf_settings

    print_conf_settings()


test_env_var()
