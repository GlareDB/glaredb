import os.path
import random
import json
import subprocess
import pathlib

import bson
import psycopg2.extensions
import psycopg2.extras
import pytest

from tests.fixtures.glaredb import glaredb_connection, glaredb_path, binary_path
import tests.tools


def test_bson_copy_to(
    glaredb_connection: psycopg2.extensions.connection,
    binary_path: pathlib.Path,
    tmp_path_factory: pytest.TempPathFactory,
):
    curr = glaredb_connection.cursor()

    curr.execute("create table bson_test (amount int)")
    curr.execute(
        "INSERT INTO bson_test (amount) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)"
    )
    curr.execute("select count(*) from bson_test;")
    res = curr.fetchone()

    assert res[0] == 10

    output_dir = tmp_path_factory.mktemp("output")
    output_fn = "copy_output.bson"
    output_path = output_dir.joinpath(output_fn)

    assert not os.path.exists(output_path)

    curr.execute(f"COPY( SELECT * FROM bson_test ) TO '{output_path}'")

    assert os.path.exists(output_path)

    with open(output_path, "rb") as f:
        for idx, doc in enumerate(bson.decode_file_iter(f)):
            assert len(doc) == 1
            assert "amount" in doc
            assert doc["amount"] == idx

    with tests.tools.cd(output_dir):
        out = subprocess.run(
            [
                f"{binary_path}",
                "--query",
                f"select count(*) as count from '{output_fn}'",
                "--mode",
                "json",
            ],
            stdout=subprocess.PIPE,
        ).stdout
        assert str(out)[3:-4] == '{"count":10}'


def test_read_bson(
    glaredb_connection: psycopg2.extensions.connection,
    tmp_path_factory: pytest.TempPathFactory,
):
    beatles = ["john", "paul", "george", "ringo"]

    tmp_dir = tmp_path_factory.mktemp(basename="read-bson-beatles-", numbered=True)
    data_path = tmp_dir.joinpath("beatles.100.bson")

    with open(data_path, "wb") as f:
        for i in range(100):
            beatle_id = random.randrange(0, len(beatles))
            f.write(
                bson.encode(
                    {
                        "_id": bson.objectid.ObjectId(),
                        "beatle_idx": beatle_id + 1,
                        "beatle_name": beatles[beatle_id],
                        "case": i + 1,
                        "rand": random.random(),
                    }
                )
            )

    with glaredb_connection.cursor() as curr:
        curr.execute(
            f"create external table bson_beatles from bson options ( location='{data_path}', file_type='bson')"
        )

    for from_clause in ["bson_beatles", f"read_bson('{data_path}')", f"'{data_path}'"]:
        with glaredb_connection.cursor() as curr:
            curr.execute(f"select count(*) from {from_clause}")
            r = curr.fetchone()
            assert r[0] == 100

        with glaredb_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as curr:
            curr.execute(f"select * from {from_clause}")
            rows = curr.fetchall()
            assert len(rows) == 100
            for row in rows:
                assert len(row) == 5
                assert row["beatle_name"] in beatles
                assert beatles.index(row["beatle_name"]) == row["beatle_idx"] - 1


def test_null_handling(
    glaredb_connection: psycopg2.extensions.connection,
    tmp_path_factory: pytest.TempPathFactory,
):
    tmp_dir = tmp_path_factory.mktemp(basename="null_handling", numbered=True)
    data_path = tmp_dir.joinpath("mixed.bson")

    with open(data_path, "wb") as f:
        for i in range(100):
            f.write(bson.encode({"a": 1}))

        for i in range(10):
            f.write(bson.encode({"a": None}))

    with glaredb_connection.cursor() as curr:
        curr.execute(f"select count(*) from '{data_path}'")
        r = curr.fetchone()
        assert r[0] == 110
