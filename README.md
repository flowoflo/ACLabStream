# ACLabStream

ACLabStream is a local demo data pipeline for laboratory data. It simulates two lab instruments that create messy files, loads those files into PostgreSQL, cleans and combines the data with dbt, and shows the results in a Streamlit dashboard.

This is a portfolio/demo project, not a production deployment. The database username and password are intentionally  demo values.

## What the system does

Two simulated instruments write files into `data/incoming`:

- **RX-2100** creates JSON batch files.
- **LabTrak 3000** creates CSV files, with older and less consistent formatting.

The pipeline is orchestrated by Airflow. The main DAG `aclabstream_pipeline` runs every minute and:

1. Triggers the JSON and CSV ingestion DAGs at the same time.
2. Waits for both ingestion jobs to finish.
3. Runs `dbt build` to clean, test, and summarize the data.

The data then moves through these layers:

```text
RX JSON files                 LabTrak CSV files
      |                              |
raw_lab_json                  raw_lab_csv
      |                              |
stg_rx_batches                       |
      |                              |
stg_rx_samples --->------<----stg_labtrak_samples
                     |
                     +
                     |
            unified_lab_samples
                     |
            clean_lab_samples
                     |
            deduped_lab_samples
                     |
               batch_summary
               /           \
catalyst_performance  temperature_performance
```

The Streamlit dashboard reads the cleaned Silver and Gold tables from PostgreSQL. It shows pipeline counts, catalyst performance, temperature performance, low-performing batches, and basic data-quality metrics.

## Services

| Service | What it does | Address |
|---|---|---|
| PostgreSQL | Stores raw, cleaned, and summary data | `localhost:5432` |
| Airflow | Runs and monitors the pipeline | `http://localhost:8080` |
| Streamlit | Shows the dashboard | `http://localhost:8501` |
| dbt docs (optional) | Shows dbt lineage and model documentation | `http://localhost:8081` |

## Prerequisites

- Docker and Docker Compose
- Python 3.10+ only if you want to run `data_generator.py` directly on your computer. The Docker streaming option does not need a local Python install.

On Linux or WSL, the included `.env` file sets `AIRFLOW_UID` so files written by the Airflow container stay owned by your local user.

## First-time setup

From the project folder, build and start the normal stack to start `postgres`, `airflow`, `dashboard`, and `dbt_docs`:

```bash
docker compose up -d --build
```

Check that the containers are running:

```bash
docker compose ps
```

Airflow needs a connection called `lab_postgres` before its ingestion DAGs can load data. Run this once after Airflow is up:

```bash
docker compose exec airflow airflow connections add lab_postgres \
  --conn-uri 'postgresql://admin:password@postgres:5432/labdb'
```

If Airflow says the connection already exists, that is fine because the setup was already completed.

Open Airflow at `http://localhost:8080`. On the first start, Airflow prints its generated admin login details in the container logs:

```bash
docker compose logs airflow
```

## Create some demo data

Choose one of these options.

### Option 1: Generate a historical batch of files

This is the quickest way to get enough data for the dashboard:

```bash
python data_generator.py backfill --days 45
```

The files are written to `data/incoming`. By default, the generator creates realistic messy data, including occasional invalid files or unusual formats for the pipeline to handle.

For a clean happy-path demo, use:

```bash
python data_generator.py backfill --days 45 --chaos 0
```

### Option 2: Simulate a live instrument feed

Start the optional generator container:

```bash
docker compose --profile streaming up -d generator
```

It writes a new simulated batch about every eight seconds. Stop it when you are done:

```bash
docker compose stop generator
```

## Run the pipeline

1. In Airflow, find `aclabstream_pipeline`.
2. Unpause it if needed.
3. Click the play button to trigger it, or wait for its once-per-minute schedule.
4. Open the DAG graph to watch it trigger `ingest_all_json` and `ingest_all_csv`, then run `dbt_build`.
5. When the run succeeds, open `http://localhost:8501` to see the dashboard.

The main pipeline only includes the two full ingestion DAGs and dbt. These helper DAGs are manual checks and are not part of the normal pipeline:

- `ingest_one_json` loads one valid JSON file as a quick ingestion test.
- `lab_file_check` confirms Airflow can see the mounted incoming folder.
- `postgres_test` confirms Airflow can connect to PostgreSQL.

## Run dbt manually

The normal Airflow pipeline runs `dbt build` for you. To run dbt by hand for debugging or development:

```bash
docker compose --profile tools run --rm dbt debug
docker compose --profile tools run --rm dbt build
```

`dbt build` creates the models and runs the data tests. A test fails when its query returns problem rows, such as duplicate samples or missing required values.

## View dbt documentation

Start the optional documentation site:

```bash
docker compose up -d dbt_docs
```

Then open `http://localhost:8081`. It shows model descriptions, column descriptions, tests, and the model lineage graph.

Stop it with:

```bash
docker compose stop dbt_docs
```

## Useful commands

```bash
# Start a specific service
docker compose start (-d) (postgres) (airflow) (dashboard) (dbt_docs)

# Rebuild images after changing a Dockerfile or Python dependencies
docker compose up -d --build (postgres) (airflow) (dashboard) (dbt_docs)

# Because dbt is intentionally an on-demand tool rather than a persistent service, you run it separately with something like:
docker compose run --rm dbt build

# Run data generator stream (see above for more verbose documentation on generator backfill/vs streaming)
docker compose --profile streaming up -d generator

# Watch generator output
docker logs -f aclabstream_generator

# See running containers and their status
docker compose ps

# Follow Airflow logs while a DAG is running
docker compose logs -f airflow

# Stop the project without deleting database data
docker compose stop

# Start previously created containers again
docker compose start

# Validate the Compose file after editing it
docker compose config
```

## Demo database settings

These values are used by PostgreSQL, dbt, Airflow, and the dashboard in this local demo:

```text
Host: postgres inside Docker, localhost from your computer
Port: 5432
Database: labdb
Username: admin
Password: password
```

Do not use these credentials outside a local demo. A real deployment should use environment variables or a secret manager instead of committing credentials to the repository.

## Troubleshooting

- **The dashboard shows an error or no data:** generate files, run `aclabstream_pipeline`, and wait for `dbt_build` to succeed. The dashboard tables do not exist until dbt has run at least once.
- **An ingestion DAG cannot connect to Postgres:** run `postgres_test` in Airflow and confirm the `lab_postgres` connection was created with the first-time setup command.
- **Airflow does not show recent code changes:** wait a few seconds for DAG parsing, then refresh the UI. The project mounts `./dags` directly into the Airflow container.
- **A port is already in use:** stop the program using port `5432`, `8080`, `8081`, or `8501`, or change the host-side port in `docker-compose.yml`.
- **You want a fresh database:** this is destructive because it removes all local Postgres data. Stop the stack and remove the `postgres_data` Docker volume only if you intentionally want to start over.
