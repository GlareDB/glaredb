import pathlib

import psycopg2


def test_pyiceberg(
    pyiceberg_table: pathlib.Path,
    glaredb_connection: psycopg2.extensions.connection,
):
    with glaredb_connection.cursor() as cur:
        cur.execute(f"select count(*) from read_iceberg('{pyiceberg_table}')")
        result = cur.fetchall()

    print(result)
