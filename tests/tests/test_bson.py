import os.path

import bson
import psycopg2.extensions
import pytest

from fixtures.glaredb import glaredb_connection, debug_path


def test_copy_to(
    glaredb_connection: psycopg2.extensions.connection,
    tmp_path_factory: pytest.TempPathFactory,
):
    with glaredb_connection.cursor() as curr:
        curr.execute("create table bson_test (amount int)")

    with glaredb_connection.cursor() as curr:
        for i in range(10):
            curr.execute("insert into bson_test values (%s)", str(i))

    with glaredb_connection.cursor() as curr:
        curr.execute("select count(*) from bson_test;")
        res = curr.fetchone()

        assert res[0] == 10

    output_path = tmp_path_factory.mktemp("output").joinpath("copy_output.bson")

    assert not os.path.exists(output_path)

    with glaredb_connection.cursor() as curr:
        print(output_path)
        curr.execute(f"COPY( SELECT * FROM bson_test ) TO '{output_path}'")

    assert os.path.exists(output_path)

    with open(output_path, "rb") as f:
        for idx, doc in enumerate(bson.decode_file_iter(f)):
            print(doc)

            assert len(doc) == 1
            assert "amount" in doc
            assert doc["amount"] == idx
