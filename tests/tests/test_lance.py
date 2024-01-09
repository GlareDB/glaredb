import os.path

import psycopg2.extensions
import psycopg2.extras
import pytest

from fixtures.glaredb import glaredb_connection, debug_path


def test_copy_to_round_trip(
    glaredb_connection: psycopg2.extensions.connection,
    tmp_path_factory: pytest.TempPathFactory,
):
    with glaredb_connection.cursor() as curr:
        curr.execute("create temp table lance_test (amount int)")

    with glaredb_connection.cursor() as curr:
        for i in range(10):
            curr.execute("insert into lance_test values (%s)", str(i))

    with glaredb_connection.cursor() as curr:
        curr.execute("select count(*) from lance_test;")
        res = curr.fetchone()

        assert res[0] == 10

    output_path = tmp_path_factory.mktemp("lance")

    with glaredb_connection.cursor() as curr:
        curr.execute(f"COPY lance_test TO 'file://{output_path}' FORMAT lance")

    with glaredb_connection.cursor() as curr:
        res = curr.execute(f"select count(*) from lance_scan('file://{output_path}')")
