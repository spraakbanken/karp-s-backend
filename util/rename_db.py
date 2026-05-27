import sys


from karps.config import Env, get_env
from karps.database.database import get_cursor


def main(env, db_from, db_to):
    with get_cursor(env) as cursor:
        cursor.execute(f"use {db_from};")
        print(cursor.statement)
        cursor.execute("show tables;")
        print(cursor.statement)
        tables_res = cursor.fetchall()
        for table in tables_res:
            cursor.execute(f"RENAME TABLE {db_from}.`{table[0]}` TO {db_to}.`{table[0]}`;")
            print(cursor.statement)
        cursor.execute("COMMIT;")


if __name__ == "__main__":
    env: Env = get_env()
    db_from = sys.argv[1]
    db_to = sys.argv[2]
    main(env, db_from, db_to)
