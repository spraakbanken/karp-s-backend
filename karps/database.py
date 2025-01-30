from typing import Any, Iterator
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


def get_sql(resources: list[str], op: str = None, field: str = None, value: Any = None, selection: str = "*"):
    if op and field and value:
        if op == "equals":
            where_clause = f"WHERE {field} = '{value}'"
        elif op == "startswith":
            where_clause = f"WHERE {field} LIKE '{value}%'"
        else:
            raise NotImplementedError("Only equals and startswith are supported in queries right now")
    else:
        where_clause = ""

    return [f"SELECT {selection}, '{resource}' as resource_id FROM {resource} {where_clause}" for resource in resources]


def run_queries(config: Config, sql_queries: list[str]) -> Iterator[list[tuple]]:
    connection = get_connection(config)
    try:
        cursor = connection.cursor()
        for sql_query in sql_queries:
            cursor.execute(sql_query)
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            yield columns, results
    finally:
        cursor.close()
        connection.close()
