import datetime
import os
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DAG_ID = str(os.path.basename(__file__).replace(".py", ""))
tag = DAG_ID.split("_zone")[0] + "_zone"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@hourly",
    catchup=False,
    tags=[tag],
) as dag:
    trigger_another_dag_task = TriggerDagRunOperator(
        task_id="trigger_airport_and_zone_transition_dag",
        trigger_dag_id="silver_zone_airport_and_zone_transition",
        wait_for_completion=True,
        poke_interval=15,
    )

    current_longest_flight_task = PostgresOperator(
        task_id="current_longest_flight",
        sql="sql/current_longest_flight.sql",
        postgres_conn_id="postgres_conn",
    )

    trigger_another_dag_task >> current_longest_flight_task
