# Airflow DAG
# This DAG looks through the incoming folder, reads all CSV files, and saves each row into the raw Postgres table.

import csv
import json
from io import StringIO
from pathlib import Path

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# This DAG is triggered by the main pipeline and loads CSV files from the incoming folder into Postgres.
@dag(
    dag_id="ingest_all_csv",
    schedule=None,
    catchup=False,
    tags=["aclabstream"],
)
def ingest_all_csv():

    @task
    def ingest():

#This is the shared incoming folder mounted into the Airflow container.
        incoming = Path("/opt/airflow/data/incoming")

#Only CSV files are picked up here so other incoming file types can be handled by their own ingestion DAGs.
        csv_files = sorted(
            file for file in incoming.iterdir()
            if file.is_file() and file.suffix.lower() == ".csv"
        )

        print(f"Found {len(csv_files)} candidate CSV files.")

        hook = PostgresHook(postgres_conn_id="lab_postgres")
        conn = hook.get_conn()

        inserted = 0
        already_seen = 0
        malformed_rows = 0
        unreadable_files = 0
        empty_files = 0

        try:
            with conn.cursor() as cursor:

                # Raw landing table.
                # Each original CSV row is stored as JSONB.
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS raw_lab_csv (
                        id BIGSERIAL PRIMARY KEY,
                        source_file TEXT NOT NULL,
                        row_number INTEGER NOT NULL,
                        ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        payload JSONB NOT NULL,

                        UNIQUE (source_file, row_number)
                    );
                """)

                for file in csv_files:

                    # ----------------------------------------
                    # 1. Read file, handling encoding variation
                    # ----------------------------------------
                    try:
                        try:
                            text = file.read_text(encoding="utf-8-sig")
                        except UnicodeDecodeError:
                            text = file.read_text(encoding="cp1252")

                    except Exception as error:
                        print(f"UNREADABLE FILE: {file.name}")
                        print(f"Reason: {error}")
                        unreadable_files += 1
                        continue

                    # ----------------------------------------
                    # 2. Remove LabTrak comment lines
                    # ----------------------------------------
                    lines = [
                        line
                        for line in text.splitlines()
                        if not line.startswith("#")
                    ]

                    if not lines:
                        print(f"EMPTY FILE: {file.name}")
                        empty_files += 1
                        continue

                    # ----------------------------------------
                    # 3. Detect delimiter from header
                    #
                    # Old instrument sometimes exports:
                    # comma-separated CSV
                    #
                    # or:
                    # semicolon-separated CSV
                    # ----------------------------------------
                    header_line = lines[0]

                    delimiter = (
                        ";"
                        if header_line.count(";") > header_line.count(",")
                        else ","
                    )

                    reader = csv.reader(
                        StringIO("\n".join(lines)),
                        delimiter=delimiter
                    )

                    try:
                        header = next(reader)
                    except StopIteration:
                        print(f"EMPTY FILE: {file.name}")
                        empty_files += 1
                        continue

                    header = [column.strip() for column in header]

                    file_rows = 0

                    # ----------------------------------------
                    # 4. Read each data row
                    # ----------------------------------------
                    for row_number, row in enumerate(reader, start=1):

                        # Truncated/broken exports can produce rows
                        # that don't match the header.
                        if len(row) != len(header):
                            print(
                                f"MALFORMED ROW: {file.name} "
                                f"row={row_number} "
                                f"expected={len(header)} "
                                f"actual={len(row)}"
                            )

                            malformed_rows += 1
                            continue

                        payload = {
                            column: value
                            for column, value in zip(header, row)
                        }

                        # ----------------------------------------
                        # 5. Store raw row in Postgres
                        # ON CONFLICT...DO NOTHING
                        #    This makes the load safe to rerun without duplicating rows that were already ingested.
                        # ----------------------------------------
                        cursor.execute("""
                            INSERT INTO raw_lab_csv (
                                source_file,
                                row_number,
                                payload
                            )
                            VALUES (
                                %s,
                                %s,
                                %s::jsonb
                            )
                        
                            ON CONFLICT (
                                source_file,
                                row_number
                            )
                            DO NOTHING
                            RETURNING id;
                        """, (
                            file.name,
                            row_number,
                            json.dumps(payload)
                        ))

                        result = cursor.fetchone()

                        if result:
                            inserted += 1
                            file_rows += 1
                        else:
                            already_seen += 1

                    if file_rows:
                        print(
                            f"INGESTED: {file.name} "
                            f"({file_rows} new rows)"
                        )
                    elif len(lines) == 1:
                        print(f"HEADER ONLY: {file.name}")

        #Commit everything together once all candidate files have been processed successfully.
            conn.commit()

        except Exception:
            conn.rollback()
            raise

        finally:
            conn.close()

        print("")
        print("===== CSV INGESTION SUMMARY =====")
        print(f"Candidate files: {len(csv_files)}")
        print(f"Rows inserted:   {inserted}")
        print(f"Already seen:    {already_seen}")
        print(f"Malformed rows:  {malformed_rows}")
        print(f"Unreadable:      {unreadable_files}")
        print(f"Empty files:     {empty_files}")
        print("=================================")

        return {
            "candidate_files": len(csv_files),
            "inserted": inserted,
            "already_seen": already_seen,
            "malformed_rows": malformed_rows,
            "unreadable_files": unreadable_files,
            "empty_files": empty_files,
        }

    ingest()


ingest_all_csv()