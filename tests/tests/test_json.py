import os.path
import random
import json

import psycopg2
import psycopg2.extras
import pytest

from fixtures.glaredb import glaredb_connection, debug_path

@pytest.fixture
def beatle_mock_data():
    beatles = ["john", "paul", "george", "ringo"]

    arr = []
    for i in range(100):
        beatle_id = random.randrange(0, len(beatles))
        obj = {
            "beatle_idx": beatle_id + 1,
            "beatle_name": beatles[beatle_id],
            "case": i + 1,
            "rand": random.random(),
        }
        
        if beatle_id == 0:
            obj["house"] = "the dakota"
        if beatle_id == 1:
            obj["living"] = True
        if beatle_id == 2:
            obj["sitar"] = True
        if beatle_id == 3:
            obj["guitar"] = None

        arr.append(obj)

    return arr


def test_read_json_array(
    glaredb_connection: psycopg2.extensions.connection,
    tmp_path_factory: pytest.TempPathFactory,
    beatle_mock_data: list[dict], 
):
    tmp_dir = tmp_path_factory.mktemp(basename="read-json-array-", numbered=True)
    data_path = tmp_dir.joinpath("beatles.100.json")

    with open(data_path, "w") as f:
        json.dump(beatle_mock_data, f, indent="  ")
            
    with glaredb_connection.cursor() as curr:
        curr.execute(f"select count(*) from read_json('{data_path}');")
        r = curr.fetchone()
        assert r[0] == 100

    with glaredb_connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
    ) as curr:
        curr.execute(f"select * from read_json('{data_path}');")
        rows = curr.fetchall()
        assert len(rows) == 100
        for row in rows:
            assert len(row) == 8 # superset schema
            assert "house" in row
            assert "beatle_name" in row
            if row["beatle_name"] == "john":
                assert row["house"] == "the dakota"
            else:
                assert row["house"] is None


@pytest.mark.skip("globbing seems rather broken")
def test_read_json_glob(
    glaredb_connection: psycopg2.extensions.connection,
    tmp_path_factory: pytest.TempPathFactory,
    beatle_mock_data: list[dict], 
):
    beatles = ["john", "paul", "george", "ringo"]

    tmp_dir = tmp_path_factory.mktemp(basename="read-json-glob-", numbered=True)

    for idx, doc in enumerate(beatle_mock_data):
        with open(tmp_dir.joinpath(f"file-{idx}.json"), "w") as f:
            json.dump(doc, f, indent="  ")
            
    with glaredb_connection.cursor() as curr:
        curr.execute(f"select count(*) from read_json('{tmp_dir}/*.json')")
        r = curr.fetchone()
        assert r[0] == 100

    with glaredb_connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
    ) as curr:
        curr.execute(f"select * from read_json('{data_path}');")
        rows = curr.fetchall()
        assert len(rows) == 100
        for row in rows:
            assert len(row) == 8 # superset schema
            assert "house" in row
            assert "beatle_name" in row
            if row["beatle_name"] == "john":
                assert row["house"] == "the dakota"
            else:
                assert row["house"] is None
