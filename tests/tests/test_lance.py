import os.path

import lance
import pyarrow as pa

import psycopg2.extensions
import psycopg2.extras
import pytest


from fixtures.glaredb import glaredb_connection, debug_path


def test_sanity_check(
    tmp_path_factory: pytest.TempPathFactory,
):
    test_path = tmp_path_factory.mktemp("lance-sanity")

    table = pa.table(
        {
            "id": pa.array([1, 2, 4]),
            "values": pa.array([2, 4, 8]),
        }
    )

    assert test_path.exists(), test_path
    dataset = lance.write_dataset(table, test_path)

    print(dir(dataset))
    assert dataset.count_rows() == 3

    files = os.listdir(test_path)
    assert len(files) == 4
    assert "data" in files
    assert "_latest.manifest" in files
    assert "_transactions" in files
    assert "_versions" in files


def test_copy_to_round_trip(
    glaredb_connection: psycopg2.extensions.connection,
    tmp_path_factory: pytest.TempPathFactory,
):
    with glaredb_connection.cursor() as curr:
        curr.execute("create temp table lance_test (amount int)")

        for i in range(10):
            curr.execute("insert into lance_test values (%s)", str(i))

    output_path = tmp_path_factory.mktemp("lance")

    with glaredb_connection.cursor() as curr:
        curr.execute("select count(*) from lance_test;")
        res = curr.fetchone()
        assert res[0] == 10

        curr.execute(f"COPY lance_test TO '{output_path}' FORMAT lance")

    with glaredb_connection.cursor() as curr:
        curr.execute(f"select count(*) from lance_scan('{output_path}')")
        res = curr.fetchone()
        assert res[0] == 10
