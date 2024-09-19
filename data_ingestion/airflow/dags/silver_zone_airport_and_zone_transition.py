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
    airport_and_zone_transition_task = PostgresOperator(
        task_id="airport_and_zone_transition",
        sql="sql/airport_and_zone_transition.sql",
        postgres_conn_id="postgres_conn",
    )
