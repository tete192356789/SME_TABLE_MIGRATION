import os

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.sdk import dag, task

# CRITICAL FIXES:
# 1. Add +pymssql driver
# 2. URL-encode # as %23
# 3. Add database name
os.environ["AIRFLOW_CONN_MSSQL_CUSTOM"] = (
    "mssql+pymssql://sa:Password_123%23@192.168.170.224:1433/"
)


@dag
def mssql_dag():
    @task
    def test_mssql_hook():
        hook = MsSqlHook(mssql_conn_id="mssql_custom")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM master.dbo.sample_table")
        results = cursor.fetchall()

        cursor.close()
        conn.close()
        return results

    test_mssql_hook()


mssql_dag()
