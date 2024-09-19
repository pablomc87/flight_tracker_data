import datetime
import os
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

DAG_ID = str(os.path.basename(__file__).replace(".py", ""))
tag = DAG_ID.split("_zone")[0] + "_zone"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@hourly",
    catchup=False,
    tags=[tag],
) as dag:
    airline_with_most_flights_in_progress_task = PostgresOperator(
        task_id="airline_with_most_flights_in_progress",
        sql="sql/airline_with_most_flights_in_progress.sql",
        postgres_conn_id="postgres_conn",
    )
