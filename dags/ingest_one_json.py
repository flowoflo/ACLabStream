# Airflow DAG
#This is a small test DAG that finds and loads one valid JSON file at a time
# from the incoming folder into the raw Postgres table.


import json
from pathlib import Path

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id="ingest_one_json",
    schedule=None,
    catchup=False,
    tags=["aclabstream"],
)
def ingest_one_json():

    @task
    def ingest():

        incoming = Path("/opt/airflow/data/incoming")

        

        # Connect to our Postgres container using the
        # Airflow connection called "lab_postgres".
        hook = PostgresHook(postgres_conn_id="lab_postgres")
        conn = hook.get_conn()

        with conn.cursor() as cursor:

            # Create our raw landing table if it doesn't exist.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw_lab_json (
                    id BIGSERIAL PRIMARY KEY,
                    source_file TEXT UNIQUE NOT NULL,
                    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    payload JSONB NOT NULL
                );
            """)

            # Look through JSON files until we find a valid one.
            # Use the same RX filename convention as the full JSON ingestion DAG so the test behaves realistically.
            json_files = (    sorted(incoming.glob("RX*.json"))    + sorted(incoming.glob("rx*.json")))

            for file in json_files:         
                try:
                    payload = json.loads(file.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON: {file.name}")
                    continue

                cursor.execute("""
                    INSERT INTO raw_lab_json (source_file, payload)
                    VALUES (%s, %s::jsonb)
                    ON CONFLICT (source_file) DO NOTHING;
                """, (file.name, json.dumps(payload)))

                print(f"Ingested: {file.name}")
                #Stop after the first valid file ... this DAG is only meant to be a quick check/smoke test.
                break

    #Save the one-file test result only after the insert has completed.
        conn.commit()
        conn.close()

    ingest()


ingest_one_json()