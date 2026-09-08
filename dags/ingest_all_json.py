# Airflow DAG
# This DAG finds JSON files in the incoming folder and loads their original contents into the raw Postgres table.

import json
from pathlib import Path

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

#This DAG is triggered by the main pipeline instead of running on its own schedule.
@dag(
    dag_id="ingest_all_json",
    schedule=None,
    catchup=False,
    tags=["aclabstream"],
)
def ingest_all_json():

    @task
    def ingest():

        # This is the path INSIDE the Airflow container.
        # Our docker-compose.yml maps ./data -> /opt/airflow/data
        incoming = Path("/opt/airflow/data/incoming")

        # Only grab actual RX instrument JSON files.
        # This avoids files such as .labsim_state.json.
        #The simulator can create both upper- and lowercase RX filenames, so we check for both patterns.
        json_files = (
            sorted(incoming.glob("RX*.json"))
            + sorted(incoming.glob("rx*.json"))
        )

        print(f"Found {len(json_files)} candidate JSON files.")

        # Use the Postgres connection configured in the Airflow UI.
        hook = PostgresHook(postgres_conn_id="lab_postgres")
        conn = hook.get_conn()

        inserted = 0
        already_seen = 0
        invalid = 0

        try:
            with conn.cursor() as cursor:

                # Raw landing table.
                # We deliberately store the original JSON payload without
                # cleaning or transforming it yet.
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS raw_lab_json (
                        id BIGSERIAL PRIMARY KEY,
                        source_file TEXT UNIQUE NOT NULL,
                        ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        payload JSONB NOT NULL
                    );
                """)

                for file in json_files:

                    # Some files are intentionally corrupted by the generator.
                    try:
                        payload = json.loads(
                            file.read_text(encoding="utf-8")
                        )

                    except (json.JSONDecodeError, UnicodeDecodeError) as error:
                        print(f"INVALID JSON: {file.name}")
                        print(f"Reason: {error}")

                        invalid += 1
                        continue

                    # Insert the file.
                    #
                    # source_file is UNIQUE, so if we run this DAG again,
                    # files already loaded will simply be ignored.
                    # Using the filename as the unique key keeps this ingestion step idempotent when the pipeline runs again.
                    cursor.execute("""
                        INSERT INTO raw_lab_json (
                            source_file,
                            payload
                        )
                        VALUES (
                            %s,
                            %s::jsonb
                        )
                        ON CONFLICT (source_file)
                        DO NOTHING
                        RETURNING id;
                    """, (
                        file.name,
                        json.dumps(payload)
                    ))

                    result = cursor.fetchone()

                    if result:
                        inserted += 1
                        print(
                            f"INGESTED: {file.name} "
                            f"-> raw_lab_json.id={result[0]}"
                        )

                    else:
                        already_seen += 1
                        print(f"ALREADY INGESTED: {file.name}")

            conn.commit()

        except Exception:
            # If something unexpected blows up, undo the current transaction
            # instead of leaving the database half-committed.
            conn.rollback()
            raise

        finally:
            conn.close()

        print("")
        print("===== INGESTION SUMMARY =====")
        print(f"Candidate files: {len(json_files)}")
        print(f"Inserted:        {inserted}")
        print(f"Already seen:    {already_seen}")
        print(f"Invalid JSON:    {invalid}")
        print("=============================")

    #Return the run summary so it is visible in the Airflow task output as well as the logs.
        return {
            "candidate_files": len(json_files),
            "inserted": inserted,
            "already_seen": already_seen,
            "invalid": invalid,
        }

    ingest()


ingest_all_json()