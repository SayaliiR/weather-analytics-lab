"""
Weather dbt DAG
Runs the dbt project (dbt run -> dbt test -> dbt snapshot) against Snowflake.
Has no schedule of its own: it is triggered by the weather_etl DAG after a successful load.

Snowflake credentials are read from the Airflow connection `snowflake_conn` at run
time and passed to dbt as environment variables used by dbt/profiles.yml, so no secrets are stored in the repository.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"
CONN = "conn.snowflake_conn"

dbt_env = {
    "DBT_ACCOUNT": "{{ " + CONN + ".extra_dejson.get('account') }}",
    "DBT_USER": "{{ " + CONN + ".login }}",
    "DBT_PRIVATE_KEY_PASSPHRASE": "{{ " + CONN + ".password }}",
    "DBT_ROLE": "{{ " + CONN + ".extra_dejson.get('role') }}",
    "DBT_WAREHOUSE": "{{ " + CONN + ".extra_dejson.get('warehouse') }}",
    "DBT_DATABASE": "{{ " + CONN + ".extra_dejson.get('database') }}",
    "DBT_PRIVATE_KEY_PATH": "{{ " + CONN + ".extra_dejson.get('private_key_file') }}",
}

DBT_FLAGS = f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"

with DAG(
    dag_id="weather_dbt",
    description="dbt run / test / snapshot for weather analytics",
    start_date=datetime(2026, 9, 27),
    schedule=None,                 
    catchup=False,
    tags=["ELT", "dbt", "weather"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run {DBT_FLAGS}",
        env=dbt_env,
        append_env=True,          
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test {DBT_FLAGS}",
        env=dbt_env,
        append_env=True,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"dbt snapshot {DBT_FLAGS}",
        env=dbt_env,
        append_env=True,
    )

    dbt_run >> dbt_test >> dbt_snapshot
