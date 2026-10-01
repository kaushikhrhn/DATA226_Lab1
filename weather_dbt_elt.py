"""
Lab 2 - Weather Prediction Analytics | DAG 2 of 2: ELT with dbt
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_BIN = "/home/airflow/.local/bin/dbt"
DBT_DIR = (
    "{{ var.value.get('weather_dbt_project_dir', '/opt/airflow/dbt/weather_analytics') }}"
)
DBT_ARGS = f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"

DBT_ENV = {
    "DBT_USER": "{{ conn.snowflake_conn.login }}",
    # Snowflake password, or the private-key passphrase when the connection uses key-pair auth
    "DBT_PASSWORD": "{{ conn.snowflake_conn.password or '' }}",
    "DBT_ACCOUNT": "{{ conn.snowflake_conn.extra_dejson.get('account', '') }}",
    "DBT_DATABASE": "{{ conn.snowflake_conn.extra_dejson.get('database') or 'DEMO_DB' }}",
    "DBT_WAREHOUSE": "{{ conn.snowflake_conn.extra_dejson.get('warehouse') or 'COMPUTE_WH' }}",
    "DBT_ROLE": "{{ conn.snowflake_conn.extra_dejson.get('role') or 'ACCOUNTADMIN' }}",
    "DBT_PRIVATE_KEY_PATH": "{{ conn.snowflake_conn.extra_dejson.get('private_key_file') or '' }}",
    "DBT_PRIVATE_KEY": "{{ conn.snowflake_conn.extra_dejson.get('private_key_content') or '' }}",
}

with DAG(
    dag_id="weather_dbt_elt",
    description="dbt: moving averages, temperature anomaly, rolling rainfall, dry spells",
    start_date=datetime(2026, 9, 20),
    schedule=None,  # triggered by weather_etl
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "kaushik"},
    tags=["lab2", "weather", "ELT", "dbt"],
) as dag:
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT_BIN} seed {DBT_ARGS}",
        env=DBT_ENV,
        append_env=True,  # keep PATH/HOME from the container
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_BIN} run {DBT_ARGS}",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_BIN} test {DBT_ARGS}",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_BIN} snapshot {DBT_ARGS}",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_seed >> dbt_run >> dbt_test >> dbt_snapshot
