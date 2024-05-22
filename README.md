# Pipeline-Reddit-API
capstone project

These repositories will introduce building pipeline data from the Reddit API to data storage.

frist create folder airflow

next create dockerfile and docker-compose.yml

## Running Airflow in Docker

Before run Airflow, let's create these folders below first. Please note that if you're using Windows, you can skip this step.

```sh
mkdir -p mnt/dags mnt/logs mnt/plugins mnt/tests
```

On **Linux**, please make sure to configure the Airflow user for the docker-compose:

```sh
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

```sh
docker compose build
docker compose up
```

To stop Airflow, run:

```bash
docker compose down
```

## Create DAG

In folder Airflow, we have folder /mnt/dag for creating DAG with python.

## Reddit API

Reddit has an API for extracting data. Can follow this link [**praw**](https://praw.readthedocs.io/en/stable/getting_started/quick_start.html)

## Data Storage (Postgres)

Airflow must be connected with postgres, you can use airflow UI in topic admin, choose connections, then create connect list.

## Adminer

Port 8090 is the backend for managing the database. In this project, you use PostgreSQL for data storage. You can use PostgreSQL to query data from a database.

## dbt (Data Build Tool)

It is a tool used for managing and transforming data in a data warehouse. Emphasis is placed on using SQL to write code to model data.
The dbt implementation is run through Poetry, an environment management tool.

### run poetry

run this for install poetry

```bash
curl -sSL https://install.python-poetry.org | python3 -
```
create folder for install poetry in folder airflow and run poetry init

```bash
poetry init
```
### run dbt

add dbt and postgres in poetry

```bash
poetry add dbt-core dbt-postgres
```
run dbt and set up your dbt project

```bash
poetry run dbt init
``` 