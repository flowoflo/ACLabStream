# Airflow DAG
# This is a simple connection chekc that prints which database and Postgres instance Airflow reached.

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="postgres_test",
    schedule=None,
    catchup=False,
    tags=["aclabstream"],
)
def postgres_test():

    @task
    def run_select():
        # Use the Airflow-managed connection so database credentials stay out of the DAG code.
        hook = PostgresHook(postgres_conn_id="lab_postgres")

        row = hook.get_first("""
            SELECT
                current_database(),
                current_user,
                version();
        """)

        print("Database:", row[0])
        print("User:", row[1])
        print("Postgres version:", row[2])

    run_select()


postgres_test()