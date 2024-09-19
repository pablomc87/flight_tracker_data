from psycopg2 import sql
import logging
from time import sleep

logging.basicConfig(level=logging.INFO)


def get_table_columns(conn, table_name):
    query = sql.SQL(
        """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'raw_data'
                  AND table_name = {};
            """
    ).format(
        sql.Literal(table_name),
    )
    with conn.cursor() as cursor:
        cursor.execute(query)
        columns = [row[0] for row in cursor.fetchall()]
    return columns


def wait_for_topic(admin_client, topic, timeout=300, interval=10):
    elapsed_time = 0
    while elapsed_time < timeout:
        topics = admin_client.list_topics(timeout=10).topics
        logging.info("Found topics: %s", topics)
        if topic in topics.keys():
            return True
        logging.info(f"Topic {topic} not found. Waiting...")
        sleep(interval)
        elapsed_time += interval
    logging.error(f"Topic {topic} did not appear within {timeout} seconds.")
    return False
