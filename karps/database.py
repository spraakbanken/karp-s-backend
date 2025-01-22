from typing import Iterator
import mysql.connector
from mysql.connector.connection import MySQLConnection

from karps.config import Config


def get_connection(config: Config) -> MySQLConnection:
    return mysql.connector.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        database=config.database,
    )


def run_queries(config, sql_queries: list[str]) -> Iterator[list[tuple]]:
    connection = get_connection(config)
    try:
        cursor = connection.cursor()
        for sql_query in sql_queries:
            cursor.execute(sql_query)
            results = cursor.fetchall()
            yield results
    finally:
        cursor.close()
        connection.close()
