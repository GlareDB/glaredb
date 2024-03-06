import os.path
import random
import json
import logging

import psycopg2
import psycopg2.extras
import pytest

from tests.fixtures.glaredb import glaredb_connection, glaredb_path, binary_path

logger = logging.getLogger("json")

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


@pytest.mark.timeout(5, method="thread")
def test_read_json_data(
    glaredb_connection: psycopg2.extensions.connection,
    tmp_path_factory: pytest.TempPathFactory,
    beatle_mock_data: list[dict],
):
    tmp_dir = tmp_path_factory.mktemp(basename="read-json-array-", numbered=True)
    paths: dict = {
        "array": tmp_dir.joinpath("beatles-array.100.json"),
        "newline": tmp_dir.joinpath("beatles-newline.100.json"),
        "spaced": tmp_dir.joinpath("beatles-spaced.100.json"),
        "oneline": tmp_dir.joinpath("beatles-one.100.json"),
        "multiconcat": tmp_dir.joinpath("beatles-multiconcat.100.json"),
        "multimulti": tmp_dir.joinpath("beatles-multimulti.100.json"),
    }

    with open(paths["array"], "w") as f:
        json.dump(beatle_mock_data, f, indent="  ")

    with open(paths["newline"], "w") as f:
        for doc in beatle_mock_data:
            json.dump(doc, f)
            f.write("\n")

    with open(paths["spaced"], "w") as f:
        for doc in beatle_mock_data:
            json.dump(doc, f)
            f.write(" ")

    with open(paths["oneline"], "w") as f:
        for doc in beatle_mock_data:
            json.dump(doc, f)

    with open(paths["multiconcat"], "w") as f:
        for doc in beatle_mock_data:
            json.dump(doc, f, indent="  ")

    with open(paths["multimulti"], "w") as f:
        for doc in beatle_mock_data:
            json.dump(doc, f, indent="  ")
            f.write("\n")

    for test_case, data_path in paths.items():
        logger.info(f"running format {test_case} count")
        with glaredb_connection.cursor() as curr:
            curr.execute(f"select count(*) from read_json('{data_path}');")
            r = curr.fetchone()
            assert r[0] == 100

        logger.info(f"running format {test_case} query")
        with glaredb_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as curr:
            curr.execute(f"select * from read_json('{data_path}');")
            rows = curr.fetchall()
            assert len(rows) == 100
            for row in rows:
                print(row)
                assert len(row) == 8
                assert "house" in row
                assert "beatle_name" in row
                if row["beatle_name"] == "john":
                    assert row["house"] == "the dakota"
                else:
                    assert row["house"] is None


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

    with glaredb_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as curr:
        curr.execute(f"select * from read_json('{tmp_dir}/*.json');")
        rows = curr.fetchall()
        assert len(rows) == 100
        for row in rows:
            assert len(row) == 5
            assert "beatle_name" in row
            assert row["beatle_name"] in ["john", "paul", "george", "ringo"]
            assert "house" in row or "living" in row or "sitar" in row or "guitar" in row
