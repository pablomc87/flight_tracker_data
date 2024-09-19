from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
from FlightRadar24 import FlightRadar24API
import pycountry_convert as pc
from resources.countries_not_in_pc import countries_not_in_pycountry_converter

DAG_ID = str(os.path.basename(__file__).replace(".py", ""))
tag = DAG_ID.split("_zone")[0] + "_zone"


def country_to_continent(country_name):
    country_alpha2 = pc.country_name_to_country_alpha2(country_name)
    country_continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
    country_continent_name = pc.convert_continent_code_to_continent_name(
        country_continent_code
    )
    return country_continent_name


def fetch_airports():
    """
    Fetch airport data from the FlightRadar24 API.
    Parameters
    ----------

    Returns
    -------

    """

    fr_api = FlightRadar24API()
    airport_data = []
    airports = fr_api.get_airports()
    for airport in airports:
        data = {
            attribute: getattr(airport, attribute)
            for attribute in dir(airport)
            if not attribute.startswith("_")
            and not callable(getattr(airport, attribute))
        }
        print(data)

        if data["country"] in countries_not_in_pycountry_converter:
            data["continent"] = countries_not_in_pycountry_converter[data["country"]]
        else:
            data["continent"] = country_to_continent(data["country"])
        airport_data.append(data)
    return airport_data


def insert_airport_data(ti):
    """
    Insert the shaped data into the Postgres database.
    """
    airport_data = ti.xcom_pull(task_ids="fetch_airports")

    # Connect to Postgres
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    insert_query = (
        """
        INSERT INTO raw_data.airport (icao, altitude, country, iata, latitude, longitude, name, continent)
        VALUES (%(icao)s, %(altitude)s, %(country)s, %(iata)s, %(latitude)s, %(longitude)s, %(name)s, %(continent)s)
        ON CONFLICT (icao) DO UPDATE SET
            altitude = EXCLUDED.altitude,
            country = EXCLUDED.country,
            iata = EXCLUDED.iata,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            name = EXCLUDED.name,
            continent = EXCLUDED.continent;
        """,
    )

    # Insert each airport into the database
    for airport in airport_data:
        pg_hook.run(insert_query, parameters=airport)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=[tag],
) as dag:

    fetch_airports_task = PythonOperator(
        task_id="fetch_airports",
        python_callable=fetch_airports,
    )

    insert_airport_data = PythonOperator(
        task_id="insert_airport_data",
        python_callable=insert_airport_data,
    )

    fetch_airports_task >> insert_airport_data
