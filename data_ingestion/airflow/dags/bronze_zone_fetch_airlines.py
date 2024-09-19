import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from FlightRadar24 import FlightRadar24API
import json


DAG_ID = str(os.path.basename(__file__).replace(".py", ""))
tag = DAG_ID.split("_zone")[0] + "_zone"


def fetch_airlines():
    """
    Fetch airline data from the FlightRadar24 API, and cross-reference it with the data in openflights.org.
    Parameters
    ----------

    Returns
    -------

    """
    with open(
        os.path.join(os.path.dirname(__file__), "resources", "scraped_data.json")
    ) as f:
        avcodes_data = json.load(f)

    fr_api = FlightRadar24API()
    airline_data = []
    airlines = fr_api.get_airlines()
    for airline in airlines:
        data = {key.lower(): value for key, value in airline.items()}
        identifier = data.get("icao")
        data["country"] = avcodes_data.get(identifier, {}).get("country", "Unknown")
        airline_data.append(data)

    return airline_data


def insert_airline_data(ti):
    """
    Insert the shaped data into the Postgres database.
    """
    airline_data = ti.xcom_pull(task_ids="fetch_airlines")

    # Connect to Postgres
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    insert_query = (
        """
        INSERT INTO raw_data.airline (icao, code, name, country)
        VALUES (%(icao)s, %(code)s, %(name)s, %(country)s)
        ON CONFLICT (icao) DO UPDATE SET
            code = EXCLUDED.code,
            name = EXCLUDED.name,
            country = EXCLUDED.country;
        """,
    )

    # Insert each airline into the database
    for airline in airline_data:
        pg_hook.run(insert_query, parameters=airline)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2020, 2, 2),
    schedule_interval="@weekly",
    catchup=False,
    tags=[tag],
) as dag:
    fetch_airlines_task = PythonOperator(
        task_id="fetch_airlines",
        python_callable=fetch_airlines,
    )

    insert_airline_data = PythonOperator(
        task_id="insert_airline_data",
        python_callable=insert_airline_data,
    )

    fetch_airlines_task >> insert_airline_data
