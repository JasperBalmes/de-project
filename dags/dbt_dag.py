import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Path to your dbt project
DBT_PROJECT_PATH = Path("/usr/local/airflow/dags/dbt/snowflake_sample")

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
        profile_args={"database": "dbt_db", "schema": "dbt_schema"},
    )
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        # Standard path for dbt in an Astro venv setup
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
    ),
    operator_args={
        "install_deps": True,
        # Force Cosmos to use the CLI instead of the internal Python runner
        # This often ensures the manifest.json is written to disk in a standard way
        "use_dbt_runner": False, 
        # Tell OpenLineage exactly where the project lives
        "env": {
            "OPENLINEAGE_NAMESPACE": "snowflake_prod",
            "OPENLINEAGE_PARENT_RUN_ID": "{{ run_id }}",
            "DBT_MANIFEST_FILEPATH": f"{DBT_PROJECT_PATH}/target/manifest.json"
        }
        },
    # RenderConfig helps Airflow parse the project correctly without 'KeyErrors'
    render_config=RenderConfig(
        emit_datasets=True, 
    ),
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dag_id="dbt_dag_modern",
)