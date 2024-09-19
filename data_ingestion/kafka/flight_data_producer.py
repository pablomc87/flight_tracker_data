import json

from FlightRadar24.errors import CloudflareError
from confluent_kafka import Producer, KafkaError, Message
import logging
from time import time
import os
from time import sleep
from FlightRadar24 import FlightRadar24API, Flight
from requests.exceptions import HTTPError

logging.basicConfig(level=logging.INFO)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POSTGRES_DB_PARAMS = {
    "dbname": os.getenv("POSTGRES_DB", "airflow"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "host": os.getenv("POSTGRES_HOST", "postgres"),
}

# Kafka Producer Configuration
producer_conf = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
producer = Producer(**producer_conf)


def delivery_report(err: KafkaError, msg: Message):
    """
    Callback function to report the success or failure of a message delivery.

    Parameters
    ----------
    err : KafkaError
        The error that occurred, or None if the message was successfully delivered.
    msg : Message
        The message that was produced.
    """
    if err is not None:
        logging.warning(f"Message delivery failed: {err}")


def send_to_kafka(topic: str, data: dict):
    """
    Send data to a Kafka topic.

    Parameters
    ----------
    topic : str
        The Kafka topic to send data to.
    data : dict
        The data to send.
    """
    producer.produce(topic, value=json.dumps(data), callback=delivery_report)
    producer.poll(0)
    producer.flush()


def get_flight_details(flight: Flight, fr_api: FlightRadar24API):
    """
    Fetch and send flight details to Kafka with retry logic.

    Parameters
    ----------
    flight: Flight instance
    fr_api: an instance of the FlightRadar API

    Returns
    -------

    """
    retries = 5
    backoff_factor = 2
    for attempt in range(retries):
        try:
            flight_details = fr_api.get_flight_details(flight)
            flight.set_flight_details(flight_details)
            flight_data = {
                attribute: getattr(flight, attribute)
                for attribute in dir(flight)
                if not attribute.startswith("_")
                and not callable(getattr(flight, attribute))
            }
            send_to_kafka(topic="raw_flight_data", data=flight_data)
            return  # Exit function if successful
        except HTTPError as e:
            logging.error(f"HTTPError occurred for flight {flight}: {e}")
            if e.response.status_code in [402]:
                wait_time = (backoff_factor**attempt) * 5  # Exponential backoff
                logging.warning(f"Retrying in {wait_time} seconds...")
                sleep(wait_time)
            else:
                raise e
        except Exception as e:
            logging.error(f"An unexpected error occurred for flight {flight}: {e}")
            wait_time = (backoff_factor**attempt) * 60
            logging.warning(f"Retrying in {wait_time} seconds...")
            sleep(wait_time)
    else:
        logging.error(f"Failed to get flight details after {retries} retries.")


def fetch_and_produce_data(fr_api: FlightRadar24API):
    """
    Fetch data from FlightRadar24 API and produce it to Kafka.
    Parameters
    ----------
    fr_api: FlightRadar24API instance

    Returns
    -------

    """
    # Fetch and produce flight data
    flights = fr_api.get_flights()
    for flight in flights:
        get_flight_details(flight, fr_api)


if __name__ == "__main__":
    fr_api = FlightRadar24API()
    last_log_time = time()
    log_interval = 300
    while True:
        fetch_and_produce_data(fr_api)
        current_time = time()
        if current_time - last_log_time >= log_interval:
            logging.info("Producer is working...")
            last_log_time = current_time
        sleep(300)
