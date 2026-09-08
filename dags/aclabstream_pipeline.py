'''
This is the main ACLabStream pipeline controller.

It rusn every minute >> starts the JSON *AND* CSV ingestion in parallel >> waits for 
success on both >> runs dbt to clean, combine, test, and prepare data for downstream consumption

key settings:
schedule="*/1 * * * *" == run once per minute
max_active_runs=1 --> concurrency control, don't start the pipeline while another one is running
'''

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.bash import BashOperator


#This is the orchestration layer: it coordinates ingestion first, then lets dbt handle the cleanup and modeling work.
with DAG(
    dag_id="aclabstream_pipeline",
    description="End-to-end ACLabStream pipeline",
    
    # change timer here
    schedule="*/1 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    
    # catchup=False in an Apache Airflow DAG tells the scheduler to skip running any missed historical intervals between the DAG's start_date and the current time
    catchup=False,
    max_active_runs=1,
    tags=["aclabstream"],
) as dag:

    # ---------------------------------------------------------
    # BRONZE INGESTION
    #
    # Trigger our existing ingestion DAGs.
    # Both are independent, so Airflow can run them in parallel.
    # ---------------------------------------------------------

    ingest_json = TriggerDagRunOperator(
        task_id="ingest_json",
        trigger_dag_id="ingest_all_json",
        wait_for_completion=True,
    #check on the child DAG every 5 seconds.
        poke_interval=5,
    )

    ingest_csv = TriggerDagRunOperator(
        task_id="ingest_csv",
        trigger_dag_id="ingest_all_csv",
        # wait till (child) ingestion has completed
        # child DAG must be unpaused in order for run
        #i.e. If aclabstream_pipeline is unpaused but ingest_all_json or ingest_all_csv is paused, child remains queued and parent waits forever, dag never starts
        wait_for_completion=True,
        #check status every 5s while waiting
        poke_interval=5,
    )

    # ---------------------------------------------------------
    # DBT TRANSFORMATION + QUALITY
    #
    # dbt build:
    #   - builds staging models
    #   - builds Silver models
    #   - builds Gold models
    #   - runs dbt data tests
    #
    # This only starts after BOTH ingestion DAGs succeed.
    # ---------------------------------------------------------

    #dbt turns the raw bronze tables into cleaner analytics-ready models and runs quality checks along the way.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="""
            dbt build \
              --project-dir /opt/airflow/dbt \
              --profiles-dir /opt/airflow/dbt
        """,
    )

#don't start dbt until both JSON and CSV ingestion tasks are successfully completed
    [ingest_json, ingest_csv] >> dbt_build