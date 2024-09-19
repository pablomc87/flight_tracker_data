import json
import os
import logging
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
import psycopg2
from psycopg2 import sql
from time import sleep, time
from utils import wait_for_topic, get_table_columns

logging.basicConfig(level=logging.INFO)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POSTGRES_DB_PARAMS = {
    "dbname": os.getenv("POSTGRES_DB", "airflow"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "host": os.getenv("POSTGRES_HOST", "postgres"),
}

PRIMARY_KEY_COLUMNS = {
    "flight": "id",
}

# Kafka Consumer Configuration
consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "auto.offset.reset": "earliest",
}


def consume_kafka_messages(conf: dict, topic: str, db_params: dict):
    """
    Consume messages from Kafka and insert them into PostgreSQL.
    Parameters
    ----------
    conf: dict containing the Kafka consumer configuration
    topic: str containing the topic name
    db_params: dict containing the database connection parameters

    Returns
    -------

    """

    topic_group = topic.split("_")[1]
    admin_client = AdminClient(conf=conf)

    if not wait_for_topic(admin_client, topic):
        return

    conf["group.id"] = topic_group
    consumer = Consumer(**conf)
    consumer.subscribe([topic])
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cursor:
            attempts = 0
            max_attempts = 10
            last_log_time = time()
            log_interval = 60
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    sleep(10)
                    logging.warning("Message not found. Sleeping...")
                    attempts += 1
                    if attempts >= max_attempts:
                        logging.warning(
                            f"No messages after {max_attempts} attempts. Sleeping for 2 minutes"
                        )
                        sleep(120)
                    continue
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"End of partition reached {msg.partition()}")
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    data = json.loads(msg.value().decode("utf-8"))
                    columns = get_table_columns(conn, topic_group)
                    values = [data[column] for column in columns]
                    query = sql.SQL(
                        "INSERT INTO raw_data.{} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
                    ).format(
                        sql.Identifier(topic_group),
                        sql.SQL(", ").join(map(sql.Identifier, columns)),
                        sql.SQL(", ").join([sql.Placeholder()] * len(columns)),
                        sql.Identifier(PRIMARY_KEY_COLUMNS[topic_group]),
                        sql.SQL(", ").join(
                            sql.Composed(
                                [
                                    sql.Identifier(col),
                                    sql.SQL(" = EXCLUDED."),
                                    sql.Identifier(col),
                                ]
                            )
                            for col in columns
                            if col != PRIMARY_KEY_COLUMNS[topic_group]
                        ),
                    )
                    cursor.execute(query, values)
                    conn.commit()
                    attempts = 0
                    current_time = time()
                    if current_time - last_log_time >= log_interval:
                        logging.info(
                            "Messages found. Consumer is working... %s",
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        )
                        last_log_time = current_time
    consumer.close()


consume_kafka_messages(
    consumer_conf, topic="raw_flight_data", db_params=POSTGRES_DB_PARAMS
)
