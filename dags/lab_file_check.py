# Airflow DAG
#This is a simple test DAG that checks whether Airflow can see files in the incoming 
# folder and prints a small sample in the task logs.
from pathlib import Path

from airflow.sdk import dag, task


@dag(
    dag_id="lab_file_check",
    schedule=None,
    catchup=False,
    tags=["aclabstream"],
)
def lab_file_check():

    @task
    def inspect_incoming_files():
        #This quick check confirms that the Docker volume is exposing the incoming lab files to Airflow.
        incoming = Path("/opt/airflow/data/incoming")

        files = list(incoming.iterdir())

        print(f"Found {len(files)} files.")

    # Show a small sample instead of dumping the whole folder into the task logs.
        for file in files[:10]:
            print(file.name)

        return len(files)

    inspect_incoming_files()


lab_file_check()